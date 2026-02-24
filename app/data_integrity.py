"""
data_integrity.py — Historical data integrity layer + Pre-live safety gate + Stability monitor.

Sections:
  1. data_integrity_audit: monthly gap detection, auto-backfill enqueue
  5. pre_live_safety_gate: block LIVE_TRADING if data coverage insufficient
  6. system_stability: composite health scores

Usage:
    from data_integrity import (
        run_integrity_audit, check_pre_live_gate, compute_stability_scores
    )

    # Run audit (writes to data_integrity_audit table)
    report = run_integrity_audit(conn)

    # Pre-live gate (returns (status, blocks, warns))
    status, blocks, warns = check_pre_live_gate(conn)

    # Stability scores (returns dict)
    scores = compute_stability_scores(conn)
"""
import sys
import json
from datetime import datetime, timezone

sys.path.insert(0, '/root/trading-bot/app')

LOG_PREFIX = '[data_integrity]'
SYMBOL = 'BTC/USDT:USDT'

# Pre-live gate thresholds (day-based for LIVE_DB rolling window)
MIN_1M_DAYS = 7       # 1m coverage >= 7 days
MIN_5M_DAYS = 30      # 5m coverage >= 30 days
MIN_NEWS_PATH_ROWS = 500
MAX_MONTHLY_GAP = 2   # months
# Max gap (months) to attempt auto-backfill. Larger → ARCHIVE_REQUIRED
MAX_AUTO_BACKFILL_MONTHS = 3
# Anomalous ret_24h threshold (%)
MAX_RET_24H_ANOMALY = 20.0


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


# ============================================================
# Section 1: data_integrity_audit
# ============================================================
def ensure_integrity_audit_table(cur):
    """Create data_integrity_audit table if not exists."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.data_integrity_audit (
            id          BIGSERIAL PRIMARY KEY,
            table_name  TEXT NOT NULL,
            month       TEXT NOT NULL,
            row_count   BIGINT NOT NULL DEFAULT 0,
            gap_flag    BOOLEAN NOT NULL DEFAULT false,
            checked_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (table_name, month)
        );
    """)


def _scan_monthly_coverage(cur, table, ts_col, tf_filter=None):
    """Scan a table for monthly row counts. Returns list of (month_str, count)."""
    where = ""
    params = []
    if tf_filter:
        where = "WHERE tf = %s"
        params = [tf_filter]
    cur.execute(f"""
        SELECT to_char({ts_col}, 'YYYY-MM') AS month, COUNT(*) AS cnt
        FROM {table}
        {where}
        GROUP BY month
        ORDER BY month;
    """, params)
    return cur.fetchall()


def _detect_gaps(monthly_rows):
    """Detect gaps > MAX_MONTHLY_GAP months in monthly coverage.
    Returns list of (gap_start, gap_end, gap_months).
    """
    if not monthly_rows or len(monthly_rows) < 2:
        return []
    gaps = []
    months = [r[0] for r in monthly_rows]
    for i in range(1, len(months)):
        prev_dt = datetime.strptime(months[i - 1], '%Y-%m')
        curr_dt = datetime.strptime(months[i], '%Y-%m')
        diff_months = (curr_dt.year - prev_dt.year) * 12 + (curr_dt.month - prev_dt.month)
        if diff_months > MAX_MONTHLY_GAP:
            gaps.append((months[i - 1], months[i], diff_months))
    return gaps


def run_integrity_audit(conn):
    """Run full data integrity audit. Writes to data_integrity_audit table.
    Returns audit report dict.
    """
    report = {
        'candles_1m': {'months': 0, 'gaps': [], 'total_rows': 0},
        'market_ohlcv_5m': {'months': 0, 'gaps': [], 'total_rows': 0},
        'news': {'months': 0, 'total_rows': 0},
        'news_price_path': {'total_rows': 0},
        'gap_detected': False,
        'auto_backfill_enqueued': False,
    }

    try:
        with conn.cursor() as cur:
            ensure_integrity_audit_table(cur)
            conn.commit() if not conn.autocommit else None

            # 1m candles
            rows_1m = _scan_monthly_coverage(cur, 'candles', 'ts', '1m')
            gaps_1m = _detect_gaps(rows_1m)
            total_1m = sum(r[1] for r in rows_1m)
            report['candles_1m'] = {
                'months': len(rows_1m),
                'gaps': gaps_1m,
                'total_rows': total_1m,
            }

            # Write audit rows for 1m
            for month_str, count in rows_1m:
                cur.execute("""
                    INSERT INTO data_integrity_audit (table_name, month, row_count, gap_flag, checked_at)
                    VALUES ('candles_1m', %s, %s, false, now())
                    ON CONFLICT (table_name, month) DO UPDATE SET
                        row_count = EXCLUDED.row_count, checked_at = now();
                """, (month_str, count))

            # Mark gap_flag for missing months
            if rows_1m:
                first_month = datetime.strptime(rows_1m[0][0], '%Y-%m')
                last_month = datetime.strptime(rows_1m[-1][0], '%Y-%m')
                existing_months = {r[0] for r in rows_1m}
                current = first_month
                while current <= last_month:
                    m_str = current.strftime('%Y-%m')
                    if m_str not in existing_months:
                        cur.execute("""
                            INSERT INTO data_integrity_audit (table_name, month, row_count, gap_flag, checked_at)
                            VALUES ('candles_1m', %s, 0, true, now())
                            ON CONFLICT (table_name, month) DO UPDATE SET
                                row_count = 0, gap_flag = true, checked_at = now();
                        """, (m_str,))
                    if current.month == 12:
                        current = current.replace(year=current.year + 1, month=1)
                    else:
                        current = current.replace(month=current.month + 1)

            # 5m OHLCV
            rows_5m = _scan_monthly_coverage(cur, 'market_ohlcv', 'ts', '5m')
            gaps_5m = _detect_gaps(rows_5m)
            total_5m = sum(r[1] for r in rows_5m)
            report['market_ohlcv_5m'] = {
                'months': len(rows_5m),
                'gaps': gaps_5m,
                'total_rows': total_5m,
            }

            for month_str, count in rows_5m:
                cur.execute("""
                    INSERT INTO data_integrity_audit (table_name, month, row_count, gap_flag, checked_at)
                    VALUES ('market_ohlcv_5m', %s, %s, false, now())
                    ON CONFLICT (table_name, month) DO UPDATE SET
                        row_count = EXCLUDED.row_count, checked_at = now();
                """, (month_str, count))

            # News
            rows_news = _scan_monthly_coverage(cur, 'news', 'ts')
            report['news'] = {
                'months': len(rows_news),
                'total_rows': sum(r[1] for r in rows_news),
            }

            # News price path
            cur.execute("SELECT COUNT(*) FROM news_price_path;")
            npp_count = cur.fetchone()[0]
            report['news_price_path'] = {'total_rows': npp_count}

            conn.commit() if not conn.autocommit else None

            # Gap detection
            if gaps_1m or gaps_5m:
                report['gap_detected'] = True
                _log(f'GAPS DETECTED: 1m={gaps_1m}, 5m={gaps_5m}')

                # Auto-enqueue backfill for detected gaps
                try:
                    _enqueue_gap_backfills(cur, gaps_1m, gaps_5m)
                    conn.commit() if not conn.autocommit else None
                    report['auto_backfill_enqueued'] = True
                except Exception as e:
                    _log(f'auto backfill enqueue error: {e}')

    except Exception as e:
        _log(f'integrity audit error: {e}')
        report['error'] = str(e)

    return report


def _enqueue_gap_backfills(cur, gaps_1m, gaps_5m):
    """Insert pending backfill jobs for detected gaps into backfill_job_runs.
    Gaps > MAX_AUTO_BACKFILL_MONTHS → ARCHIVE_REQUIRED (no auto-backfill).
    """
    for gap_start, gap_end, gap_months in gaps_1m:
        if gap_months > MAX_AUTO_BACKFILL_MONTHS:
            _log(f'ARCHIVE_REQUIRED: 1m gap {gap_start} → {gap_end} '
                 f'({gap_months} months > {MAX_AUTO_BACKFILL_MONTHS}). '
                 f'Not auto-backfilling.')
            cur.execute("""
                INSERT INTO backfill_job_runs (job_name, status, metadata)
                VALUES ('candles_1m_gap', 'ARCHIVE_REQUIRED', %s::jsonb);
            """, (json.dumps({
                'gap_start': gap_start,
                'gap_end': gap_end,
                'gap_months': gap_months,
                'auto_enqueued': False,
                'reason': 'gap_too_large',
            }),))
            continue

        _log(f'Enqueuing 1m backfill: {gap_start} → {gap_end} ({gap_months} months)')
        cur.execute("""
            INSERT INTO backfill_job_runs (job_name, status, metadata)
            VALUES ('candles_1m_gap', 'QUEUED', %s::jsonb);
        """, (json.dumps({
            'gap_start': gap_start,
            'gap_end': gap_end,
            'gap_months': gap_months,
            'auto_enqueued': True,
        }),))

    for gap_start, gap_end, gap_months in gaps_5m:
        if gap_months > MAX_AUTO_BACKFILL_MONTHS:
            _log(f'ARCHIVE_REQUIRED: 5m gap {gap_start} → {gap_end} '
                 f'({gap_months} months > {MAX_AUTO_BACKFILL_MONTHS}). '
                 f'Not auto-backfilling.')
            cur.execute("""
                INSERT INTO backfill_job_runs (job_name, status, metadata)
                VALUES ('ohlcv_5m_gap', 'ARCHIVE_REQUIRED', %s::jsonb);
            """, (json.dumps({
                'gap_start': gap_start,
                'gap_end': gap_end,
                'gap_months': gap_months,
                'auto_enqueued': False,
                'reason': 'gap_too_large',
            }),))
            continue

        _log(f'Enqueuing 5m backfill: {gap_start} → {gap_end} ({gap_months} months)')
        cur.execute("""
            INSERT INTO backfill_job_runs (job_name, status, metadata)
            VALUES ('ohlcv_5m_gap', 'QUEUED', %s::jsonb);
        """, (json.dumps({
            'gap_start': gap_start,
            'gap_end': gap_end,
            'gap_months': gap_months,
            'auto_enqueued': True,
        }),))


# ============================================================
# Section 5: Pre-Live Safety Gate
# ============================================================
def check_pre_live_gate(conn):
    """Verify data prerequisites before allowing LIVE_TRADING.
    Returns (status: str, blocks: list[str], warns: list[str]).

    status: 'PASS' / 'WARN' / 'BLOCK'

    BLOCK criteria (실매매 진입 불가):
      - recent 24h data integrity score < 90
      - required services not OK (safety_manager.check_service_health)

    WARN criteria (reference only, 차단 아님):
      - 히스토리 커버리지 부족 (1m < 7d, 5m < 30d) (reference)
      - news_price_path < 500 (reference)
      - ret_24h anomaly (reference)
      - backfill FAILED in last 7d (WARN only)
      - backfill_runner active
    """
    blocks = []
    warns = []

    try:
        with conn.cursor() as cur:
            # ── BLOCK checks ──

            # 1) Recent 24h data integrity score >= 90
            recent_score = _compute_data_integrity_score_recent(cur)
            if recent_score < 90:
                blocks.append(f'data_integrity_recent_24h: {recent_score}/100 (<90)')

            # 2) Required services OK
            try:
                from safety_manager import check_service_health
                svc_ok, svc_reason = check_service_health()
                if not svc_ok:
                    blocks.append(f'required_services: {svc_reason}')
            except Exception as e:
                warns.append(f'service health check unavailable: {e}')

            # ── WARN checks ──

            # 2) 1m history coverage in days
            cur.execute("SELECT MIN(ts), MAX(ts) FROM candles WHERE tf='1m';")
            row = cur.fetchone()
            if row and row[0] and row[1]:
                min_ts, max_ts = row
                if min_ts.tzinfo is None:
                    min_ts = min_ts.replace(tzinfo=timezone.utc)
                if max_ts.tzinfo is None:
                    max_ts = max_ts.replace(tzinfo=timezone.utc)
                days_1m = (max_ts - min_ts).total_seconds() / 86400
                if days_1m < MIN_1M_DAYS:
                    warns.append(
                        f'(reference) 1m history coverage: {days_1m:.1f}d '
                        f'(need >= {MIN_1M_DAYS}d)')

            # 3) 5m history coverage in days
            cur.execute("SELECT MIN(ts), MAX(ts) FROM market_ohlcv WHERE tf='5m';")
            row = cur.fetchone()
            if row and row[0] and row[1]:
                min_ts, max_ts = row
                if min_ts.tzinfo is None:
                    min_ts = min_ts.replace(tzinfo=timezone.utc)
                if max_ts.tzinfo is None:
                    max_ts = max_ts.replace(tzinfo=timezone.utc)
                days_5m = (max_ts - min_ts).total_seconds() / 86400
                if days_5m < MIN_5M_DAYS:
                    warns.append(
                        f'(reference) 5m history coverage: {days_5m:.1f}d '
                        f'(need >= {MIN_5M_DAYS}d)')
            else:
                warns.append('(reference) 5m coverage: no data')

            # 4) news_price_path count
            cur.execute("SELECT COUNT(*) FROM news_price_path;")
            npp_count = cur.fetchone()[0]
            if npp_count < MIN_NEWS_PATH_ROWS:
                warns.append(
                    f'news_price_path: {npp_count} rows '
                    f'(need >= {MIN_NEWS_PATH_ROWS})')

            # 5) Anomalous ret_24h check
            cur.execute("""
                SELECT COUNT(*) FROM news_price_path
                WHERE ABS(end_ret_24h) > %s;
            """, (MAX_RET_24H_ANOMALY,))
            anomaly_count = cur.fetchone()[0]
            if anomaly_count > 0:
                warns.append(
                    f'ret_24h anomalies: {anomaly_count} rows with '
                    f'|ret_24h| > {MAX_RET_24H_ANOMALY}%')

            # 6) FAILED backfill jobs in last 7d (WARN only)
            cur.execute("""
                SELECT COUNT(*) FROM backfill_job_runs
                WHERE status = 'FAILED'
                  AND started_at >= now() - interval '7 days';
            """)
            failed_count = cur.fetchone()[0]
            if failed_count > 0:
                warns.append(f'(WARN only) FAILED backfill jobs in last 7d: {failed_count}')

            # 7) backfill_runner idle
            try:
                from backfill_utils import get_running_pid
                pid = get_running_pid()
                if pid:
                    warns.append(f'backfill_runner active (PID={pid})')
            except Exception:
                pass  # non-critical

    except Exception as e:
        blocks.append(f'gate check error: {e}')

    if blocks:
        status = 'BLOCK'
        _log(f'PRE-LIVE GATE BLOCK: {blocks}')
    elif warns:
        status = 'WARN'
        _log(f'PRE-LIVE GATE WARN: {warns}')
    else:
        status = 'PASS'
        _log('PRE-LIVE GATE: all checks passed')

    return status, blocks, warns


def _count_continuous_months(monthly_rows):
    """Count longest run of consecutive months (no gaps > 1 month).
    Used for audit/stability scoring (not for pre-live gate).
    """
    if not monthly_rows:
        return 0
    months = [r[0] for r in monthly_rows if r[1] > 0]
    if not months:
        return 0
    best = 1
    current_run = 1
    for i in range(1, len(months)):
        prev_dt = datetime.strptime(months[i - 1], '%Y-%m')
        curr_dt = datetime.strptime(months[i], '%Y-%m')
        diff = (curr_dt.year - prev_dt.year) * 12 + (curr_dt.month - prev_dt.month)
        if diff == 1:
            current_run += 1
            best = max(best, current_run)
        else:
            current_run = 1
    return best


# ============================================================
# Section 6: Stability Monitor
# ============================================================
def compute_stability_scores(conn):
    """Compute composite stability scores for /debug system_stability.

    Returns dict with:
      - data_integrity_score (0-100)
      - news_signal_stability_score (0-100)
      - backfill_health_score (0-100)
      - model_latency_avg (ms)
      - trade_execution_success_rate (0-100%)
    """
    scores = {
        'data_integrity_score': 0,
        'data_integrity_score_recent': 0,
        'news_signal_stability_score': 0,
        'backfill_health_score': 0,
        'model_latency_avg_ms': 0,
        'model_latency_is_na': False,
        'trade_execution_success_rate': 0,
    }

    try:
        with conn.cursor() as cur:
            # ── data_integrity_score (history) ──
            scores['data_integrity_score'] = _compute_data_integrity_score(cur)

            # ── data_integrity_score_recent (24h) ──
            scores['data_integrity_score_recent'] = _compute_data_integrity_score_recent(cur)

            # ── news_signal_stability_score ──
            scores['news_signal_stability_score'] = _compute_news_stability_score(cur)

            # ── backfill_health_score ──
            scores['backfill_health_score'] = _compute_backfill_health_score(cur)

            # ── model_latency_avg ──
            latency = _compute_model_latency(cur)
            scores['model_latency_avg_ms'] = latency
            scores['model_latency_is_na'] = (latency == 0)

            # ── trade_execution_success_rate ──
            scores['trade_execution_success_rate'] = _compute_execution_success_rate(cur)

    except Exception as e:
        _log(f'stability score error: {e}')
        scores['error'] = str(e)

    return scores


def _compute_data_integrity_score(cur):
    """Score 0-100 based on data coverage and gaps."""
    score = 100

    # 1m months with data
    rows_1m = _scan_monthly_coverage(cur, 'candles', 'ts', '1m')
    if not rows_1m:
        return 0
    gaps_1m = _detect_gaps(rows_1m)
    # Deduct 15 points per gap
    score -= len(gaps_1m) * 15
    # Deduct for total months of gaps
    total_gap_months = sum(g[2] for g in gaps_1m)
    score -= total_gap_months * 3

    # 5m coverage
    rows_5m = _scan_monthly_coverage(cur, 'market_ohlcv', 'ts', '5m')
    if not rows_5m:
        score -= 20
    else:
        gaps_5m = _detect_gaps(rows_5m)
        score -= len(gaps_5m) * 10

    return max(0, min(100, score))


def _compute_data_integrity_score_recent(cur):
    """Score 0-100 based on recent 24h candle continuity only.
    1m: expect 1440 candles, 5m: expect 288 candles.
    """
    score = 100

    # 1m recent 24h
    cur.execute("""
        SELECT COUNT(*) FROM candles
        WHERE tf='1m' AND ts >= now() - interval '24 hours';
    """)
    count_1m = cur.fetchone()[0]
    expected_1m = 1440
    coverage_1m = min(1.0, count_1m / expected_1m) if expected_1m > 0 else 0
    # 1m accounts for 60% of recent score
    score_1m = coverage_1m * 60

    # 5m recent 24h
    cur.execute("""
        SELECT COUNT(*) FROM market_ohlcv
        WHERE tf='5m' AND ts >= now() - interval '24 hours';
    """)
    count_5m = cur.fetchone()[0]
    expected_5m = 288
    coverage_5m = min(1.0, count_5m / expected_5m) if expected_5m > 0 else 0
    # 5m accounts for 40% of recent score
    score_5m = coverage_5m * 40

    return max(0, min(100, round(score_1m + score_5m)))


def _compute_news_stability_score(cur):
    """Score 0-100 based on news path coverage and class distribution."""
    score = 0

    # Total news_price_path rows
    cur.execute("SELECT COUNT(*) FROM news_price_path;")
    npp = cur.fetchone()[0]
    if npp >= MIN_NEWS_PATH_ROWS:
        score += 40
    elif npp > 0:
        score += int(40 * npp / MIN_NEWS_PATH_ROWS)

    # Path class distribution (want >= 500 per class for 7 classes)
    cur.execute("""
        SELECT path_class, COUNT(*) FROM news_price_path
        WHERE path_class IS NOT NULL
        GROUP BY path_class;
    """)
    class_counts = cur.fetchall()
    if class_counts:
        min_class = min(r[1] for r in class_counts)
        num_classes = len(class_counts)
        # 30 points for good distribution
        if num_classes >= 5 and min_class >= 100:
            score += 30
        elif num_classes >= 3:
            score += 15

    # Recent news flow (last 24h)
    cur.execute("""
        SELECT COUNT(*) FROM news
        WHERE ts >= now() - interval '24 hours';
    """)
    recent_news = cur.fetchone()[0]
    if recent_news >= 10:
        score += 30
    elif recent_news >= 5:
        score += 15
    elif recent_news > 0:
        score += 5

    return min(100, score)


def _compute_backfill_health_score(cur):
    """Score 0-100 based on recent job outcomes."""
    score = 100

    # Check last 7 days of jobs
    cur.execute("""
        SELECT status, COUNT(*) FROM backfill_job_runs
        WHERE started_at >= now() - interval '7 days'
        GROUP BY status;
    """)
    status_counts = {r[0]: r[1] for r in cur.fetchall()}
    total = sum(status_counts.values())
    if total == 0:
        return 50  # no jobs = neutral

    failed = status_counts.get('FAILED', 0)
    if failed > 0:
        score -= min(50, failed * 15)

    # Stale RUNNING jobs (> 2 hours)
    cur.execute("""
        SELECT COUNT(*) FROM backfill_job_runs
        WHERE status = 'RUNNING'
          AND started_at < now() - interval '2 hours';
    """)
    stale = cur.fetchone()[0]
    if stale > 0:
        score -= stale * 20

    return max(0, min(100, score))


def _compute_model_latency(cur):
    """Average model response latency proxy (ms). Uses trade_process_log timestamps."""
    try:
        cur.execute("""
            SELECT AVG(EXTRACT(EPOCH FROM (ts - LAG(ts) OVER (ORDER BY id))) * 1000)
            FROM (
                SELECT id, ts FROM trade_process_log
                WHERE source = 'autopilot'
                ORDER BY id DESC LIMIT 20
            ) sub;
        """)
        row = cur.fetchone()
        return round(float(row[0]), 0) if row and row[0] else 0
    except Exception:
        return 0


def _compute_execution_success_rate(cur):
    """Trade execution success rate (%) from execution_log last 30 days."""
    try:
        cur.execute("""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE status IN ('FILLED', 'VERIFIED', 'SENT')) AS success
            FROM execution_log
            WHERE ts >= now() - interval '30 days';
        """)
        row = cur.fetchone()
        if row and row[0] > 0:
            return round(row[1] / row[0] * 100, 1)
        return 0
    except Exception:
        return 0


def format_integrity_report(report):
    """Format integrity audit report for Telegram/debug output."""
    lines = [
        '🔍 데이터 무결성 감사 보고서',
        '━━━━━━━━━━━━━━━━━━━━━━━━',
    ]

    c1m = report.get('candles_1m', {})
    lines.append('[candles 1m]')
    lines.append(f'  months={c1m.get("months", 0)} total_rows={c1m.get("total_rows", 0):,}')
    if c1m.get('gaps'):
        for g in c1m['gaps']:
            lines.append(f'  GAP: {g[0]} → {g[1]} ({g[2]} months)')

    o5m = report.get('market_ohlcv_5m', {})
    lines.append('[market_ohlcv 5m]')
    lines.append(f'  months={o5m.get("months", 0)} total_rows={o5m.get("total_rows", 0):,}')
    if o5m.get('gaps'):
        for g in o5m['gaps']:
            lines.append(f'  GAP: {g[0]} → {g[1]} ({g[2]} months)')

    npp = report.get('news_price_path', {})
    lines.append(f'[news_price_path] rows={npp.get("total_rows", 0):,}')

    if report.get('gap_detected'):
        lines.append('')
        lines.append('⚠ 갭 감지됨 — 자동 백필 대기열 등록: '
                     + ('완료' if report.get('auto_backfill_enqueued') else '실패'))

    return '\n'.join(lines)


def format_stability_report(scores):
    """Format stability scores for /debug system_stability."""
    latency_str = (
        'N/A (no data)' if scores.get('model_latency_is_na')
        else f'{scores.get("model_latency_avg_ms", 0):.0f} ms'
    )
    lines = [
        '📊 시스템 안정성 모니터',
        '━━━━━━━━━━━━━━━━━━━━━━━━',
        f'  data_integrity (history):    {scores.get("data_integrity_score", 0)}/100',
        f'  data_integrity (recent 24h): {scores.get("data_integrity_score_recent", 0)}/100',
        f'  news_signal_stability:       {scores.get("news_signal_stability_score", 0)}/100',
        f'  backfill_health:             {scores.get("backfill_health_score", 0)}/100',
        f'  model_latency_avg:           {latency_str}',
        f'  trade_exec_success_rate:     {scores.get("trade_execution_success_rate", 0):.1f}%',
    ]

    if scores.get('error'):
        lines.append(f'\n(오류: {scores["error"]})')

    # Overall health rating — uses recent score (not history)
    avg = (scores.get('data_integrity_score_recent', 0)
           + scores.get('news_signal_stability_score', 0)
           + scores.get('backfill_health_score', 0)) / 3
    if avg >= 80:
        lines.append(f'\n종합: HEALTHY ({avg:.0f}/100)')
    elif avg >= 50:
        lines.append(f'\n종합: DEGRADED ({avg:.0f}/100)')
    else:
        lines.append(f'\n종합: CRITICAL ({avg:.0f}/100)')

    return '\n'.join(lines)


def format_pre_live_gate_report(status, blocks, warns):
    """Format pre-live gate check report (3-tier: PASS/WARN/BLOCK)."""
    if status == 'PASS':
        return '✅ Pre-Live Safety Gate: PASS'
    icon = '❌' if status == 'BLOCK' else '⚠'
    lines = [
        f'{icon} Pre-Live Safety Gate: {status}',
        '━━━━━━━━━━━━━━━━━━━━━━━━',
    ]
    if blocks:
        for b in blocks:
            lines.append(f'  ❌ {b}')
    if warns:
        for w in warns:
            lines.append(f'  ⚠ {w}')
    if status == 'BLOCK':
        lines.append('\nLIVE_TRADING entry 차단됨')
    return '\n'.join(lines)
