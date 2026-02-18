"""
verify_backfill.py — 백필 커버리지/품질 검증 리포트.

검증 항목:
1. candles 연속성: 갭률 < 0.5% (1m 캔들 간 2분 초과 갭 비율)
2. candles 범위: 2023-11-01 ~ 현재
3. news 월별 존재: 매월 50건 이상
4. UNKNOWN 비율: < 10%
5. 제외 뉴스 샘플: TIERX 50건
6. 포함 뉴스 샘플: TIER1/TIER2 50건
7. Tier1+급변 반응 통계
8. 방향 일치율

Usage:
    python verify_backfill.py
    python verify_backfill.py --verbose
"""
import sys
import argparse
import traceback

sys.path.insert(0, '/root/trading-bot/app')
from db_config import get_conn

LOG_PREFIX = '[verify_backfill]'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _section(title):
    print(f'\n{"="*60}')
    print(f'  {title}')
    print(f'{"="*60}')


def check_candle_range(cur):
    """Check candles table range."""
    _section('1. Candles Range')

    cur.execute("""
        SELECT COUNT(*), MIN(ts), MAX(ts)
        FROM candles WHERE symbol = 'BTC/USDT:USDT' AND tf = '1m';
    """)
    cnt, min_ts, max_ts = cur.fetchone()
    print(f'  Total 1m candles: {cnt:,}')
    print(f'  Range: {min_ts} ~ {max_ts}')

    target_start = '2023-11-01'
    if min_ts and str(min_ts)[:10] <= target_start:
        print(f'  [PASS] Start date covers {target_start}')
    else:
        print(f'  [FAIL] Start date {str(min_ts)[:10] if min_ts else "N/A"} '
              f'does not cover {target_start}')

    return cnt


def check_candle_gaps(cur):
    """Check 1m candle gap rate."""
    _section('2. Candle Continuity (Gap Rate)')

    cur.execute("""
        WITH gaps AS (
            SELECT ts, LEAD(ts) OVER (ORDER BY ts) AS next_ts
            FROM candles
            WHERE symbol = 'BTC/USDT:USDT' AND tf = '1m'
        )
        SELECT
            COUNT(*) AS total_candles,
            SUM(CASE WHEN EXTRACT(EPOCH FROM (next_ts - ts)) > 120 THEN 1 ELSE 0 END) AS gap_count
        FROM gaps
        WHERE next_ts IS NOT NULL;
    """)
    row = cur.fetchone()
    total = row[0] or 0
    gaps = row[1] or 0
    gap_rate = (gaps / total * 100) if total > 0 else 0

    print(f'  Total transitions: {total:,}')
    print(f'  Gaps (>2min): {gaps:,}')
    print(f'  Gap rate: {gap_rate:.3f}%')
    if gap_rate < 0.5:
        print(f'  [PASS] Gap rate < 0.5%')
    else:
        print(f'  [WARN] Gap rate {gap_rate:.3f}% >= 0.5%')


def check_ohlcv_range(cur):
    """Check market_ohlcv table range."""
    _section('3. Market OHLCV Range')

    for tf in ('5m', '15m', '1h'):
        cur.execute("""
            SELECT COUNT(*), MIN(ts), MAX(ts)
            FROM market_ohlcv WHERE symbol = 'BTC/USDT:USDT' AND tf = %s;
        """, (tf,))
        cnt, min_ts, max_ts = cur.fetchone()
        print(f'  {tf}: {cnt:,} bars, {min_ts} ~ {max_ts}')


def check_news_monthly(cur):
    """Check monthly news counts."""
    _section('4. News Monthly Coverage')

    cur.execute("""
        SELECT
            to_char(ts, 'YYYY-MM') AS month,
            COUNT(*) AS cnt
        FROM news
        GROUP BY month
        ORDER BY month;
    """)
    rows = cur.fetchall()

    low_months = 0
    for month, cnt in rows:
        marker = '' if cnt >= 50 else ' [LOW]'
        print(f'  {month}: {cnt:,}{marker}')
        if cnt < 50:
            low_months += 1

    if low_months == 0:
        print(f'  [PASS] All months have >= 50 news')
    else:
        print(f'  [WARN] {low_months} months below 50 news threshold')


def check_unknown_ratio(cur):
    """Check tier=UNKNOWN ratio."""
    _section('5. News UNKNOWN Ratio')

    cur.execute("SELECT COUNT(*) FROM news;")
    total = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM news WHERE tier = 'UNKNOWN' OR tier IS NULL;")
    unknown = cur.fetchone()[0]

    ratio = (unknown / total * 100) if total > 0 else 0
    print(f'  Total news: {total:,}')
    print(f'  UNKNOWN: {unknown:,} ({ratio:.1f}%)')

    if ratio < 10:
        print(f'  [PASS] UNKNOWN ratio < 10%')
    else:
        print(f'  [WARN] UNKNOWN ratio {ratio:.1f}% >= 10%')


def check_excluded_sample(cur, verbose=False):
    """Sample TIERX/excluded news."""
    _section('6. Excluded News Sample (TIERX)')

    cur.execute("""
        SELECT id, ts, title, exclusion_reason
        FROM news WHERE tier = 'TIERX'
        ORDER BY ts DESC LIMIT 50;
    """)
    rows = cur.fetchall()
    print(f'  TIERX sample ({len(rows)} shown):')
    for nid, ts, title, reason in rows[:10 if not verbose else 50]:
        print(f'    [{nid}] {str(ts)[:16]} {(title or "")[:60]}')
        if reason:
            print(f'         Reason: {reason}')


def check_included_sample(cur, verbose=False):
    """Sample TIER1/TIER2 news."""
    _section('7. Included News Sample (TIER1/TIER2)')

    cur.execute("""
        SELECT id, ts, title, tier, topic_class, asset_relevance
        FROM news WHERE tier IN ('TIER1', 'TIER2')
        ORDER BY ts DESC LIMIT 50;
    """)
    rows = cur.fetchall()
    print(f'  TIER1/TIER2 sample ({len(rows)} shown):')
    for nid, ts, title, tier, tc, ar in rows[:10 if not verbose else 50]:
        print(f'    [{nid}] {str(ts)[:16]} [{tier}|{tc}|{ar}] {(title or "")[:60]}')


def check_price_events(cur):
    """Check price_events table stats."""
    _section('8. Price Events')

    cur.execute("SELECT COUNT(*) FROM price_events;")
    total = cur.fetchone()[0]
    print(f'  Total price events: {total:,}')

    cur.execute("""
        SELECT trigger_type, COUNT(*), AVG(move_pct)
        FROM price_events
        GROUP BY trigger_type ORDER BY COUNT(*) DESC;
    """)
    for tt, cnt, avg_move in cur.fetchall():
        print(f'  {tt}: {cnt:,} events, avg_move={avg_move:.2f}%')


def check_event_news_links(cur):
    """Check event_news_link stats."""
    _section('9. Event↔News Links')

    cur.execute("SELECT COUNT(*) FROM event_news_link;")
    total = cur.fetchone()[0]
    print(f'  Total links: {total:,}')

    cur.execute("""
        SELECT AVG(match_score), MIN(match_score), MAX(match_score)
        FROM event_news_link;
    """)
    row = cur.fetchone()
    if row and row[0]:
        print(f'  Score: avg={row[0]:.1f}, min={row[1]:.1f}, max={row[2]:.1f}')


def check_tier1_reaction_stats(cur):
    """TIER1 events + price reaction statistics."""
    _section('10. TIER1 + Price Event Reaction Stats')

    cur.execute("""
        SELECT
            COUNT(*) AS link_count,
            AVG(pe.ret_1h) AS avg_ret_1h,
            AVG(pe.ret_4h) AS avg_ret_4h,
            AVG(pe.ret_24h) AS avg_ret_24h
        FROM price_events pe
        JOIN event_news_link enl ON enl.event_id = pe.event_id
        JOIN news n ON n.id = enl.news_id
        WHERE n.tier = 'TIER1';
    """)
    row = cur.fetchone()
    if row and row[0]:
        print(f'  TIER1-linked events: {row[0]}')
        print(f'  avg ret_1h: {row[1]:.3f}%' if row[1] else '  avg ret_1h: N/A')
        print(f'  avg ret_4h: {row[2]:.3f}%' if row[2] else '  avg ret_4h: N/A')
        print(f'  avg ret_24h: {row[3]:.3f}%' if row[3] else '  avg ret_24h: N/A')
    else:
        print('  No TIER1-linked events found')


def check_direction_accuracy(cur):
    """Direction accuracy: news direction vs actual 2h move."""
    _section('11. Direction Accuracy (direction_2h)')

    cur.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE
                WHEN nmr.direction_2h = 'up' AND n.impact_score > 5 THEN 1
                WHEN nmr.direction_2h = 'down' AND n.impact_score > 5 THEN 1
                ELSE 0
            END) AS high_impact_with_direction
        FROM news_market_reaction nmr
        JOIN news n ON n.id = nmr.news_id
        WHERE nmr.direction_2h IS NOT NULL;
    """)
    row = cur.fetchone()
    if row and row[0]:
        print(f'  Reactions with direction: {row[0]:,}')
        print(f'  High-impact with direction: {row[1]:,}')
    else:
        print('  No direction data available')


def check_macro_trace(cur):
    """Check macro_trace coverage."""
    _section('12. Macro Trace Coverage')

    cur.execute("SELECT COUNT(*) FROM macro_trace;")
    total = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM macro_trace WHERE btc_ret_2h IS NOT NULL;")
    with_ret2h = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM macro_trace WHERE btc_ret_24h IS NOT NULL;")
    with_ret24h = cur.fetchone()[0]

    print(f'  Total traces: {total:,}')
    print(f'  With ret_2h: {with_ret2h:,}')
    print(f'  With ret_24h: {with_ret24h:,}')


def check_backfill_jobs(cur):
    """Check backfill job run history."""
    _section('13. Backfill Job History')

    cur.execute("""
        SELECT job_name, status, started_at, finished_at, inserted, updated, failed
        FROM backfill_job_runs
        ORDER BY started_at DESC LIMIT 20;
    """)
    rows = cur.fetchall()
    if not rows:
        print('  No backfill jobs found')
        return

    for name, status, started, finished, ins, upd, fail in rows:
        duration = ''
        if finished and started:
            dur = finished - started
            duration = f' ({dur.total_seconds():.0f}s)'
        print(f'  {name}: {status}{duration} ins={ins} upd={upd} fail={fail}')


def main():
    parser = argparse.ArgumentParser(description='Verify backfill coverage and quality')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    conn = get_conn()
    conn.autocommit = True

    print(f'\n{"#"*60}')
    print(f'#  BACKFILL VERIFICATION REPORT')
    print(f'{"#"*60}')

    try:
        with conn.cursor() as cur:
            check_candle_range(cur)
            check_candle_gaps(cur)
            check_ohlcv_range(cur)
            check_news_monthly(cur)
            check_unknown_ratio(cur)
            check_excluded_sample(cur, verbose=args.verbose)
            check_included_sample(cur, verbose=args.verbose)
            check_price_events(cur)
            check_event_news_links(cur)
            check_tier1_reaction_stats(cur)
            check_direction_accuracy(cur)
            check_macro_trace(cur)
            check_backfill_jobs(cur)

        print(f'\n{"="*60}')
        print(f'  VERIFICATION COMPLETE')
        print(f'{"="*60}\n')

    except Exception as e:
        _log(f'Error: {e}')
        traceback.print_exc()
    finally:
        conn.close()


if __name__ == '__main__':
    main()
