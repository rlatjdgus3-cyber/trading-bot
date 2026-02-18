"""
backfill_news_path.py — 24h price path analysis after news events.

For each news item not yet in news_price_path:
  1. Get BTC price at news time (1m/5m hybrid)
  2. Get all candles in 24h window after news
  3. Compute: max_drawdown, max_runup, drawdown_ts, runup_ts
  4. Compute recovery_minutes: time from max drawdown to first candle >= entry
  5. Get end price at +24h
  6. Classify end_state_24h and path_shape

Usage:
    python backfill_news_path.py                    # full run
    python backfill_news_path.py --resume           # resume from last cursor
    python backfill_news_path.py --recompute        # recompute path_class where NULL
    python backfill_news_path.py --mode balanced     # stratified sampling rebuild
    python backfill_news_path.py --from 2024-01 --recompute  # rolling historical rebuild
"""
import sys
import argparse
import traceback
from datetime import datetime, timezone, timedelta

sys.path.insert(0, '/root/trading-bot/app')
from db_config import get_conn
from backfill_utils import (
    start_job, get_last_cursor, update_progress, finish_job,
    check_stop, check_pause,
)

LOG_PREFIX = '[backfill_news_path]'
JOB_NAME = 'backfill_news_path'
SYMBOL = 'BTC/USDT:USDT'
RETAIN_1M_DAYS = 180
BATCH_SIZE = 100
# Sanity clamp: ret_24h capped at +/- this value (%)
RET_24H_CLAMP = 25.0
# Minimum independent candles to confirm extreme ret_24h
RET_24H_EXTREME_CONFIRM_CANDLES = 3


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _choose_price_source(ts_news):
    """Auto-select 1m candles vs 5m ohlcv based on data age."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=RETAIN_1M_DAYS)
    if ts_news.tzinfo is None:
        ts_news = ts_news.replace(tzinfo=timezone.utc)
    return '1m' if ts_news >= cutoff else '5m'


def _get_price_at(cur, ts_news, price_source_tf):
    """Get BTC price at ts. Priority: 1m candles → 5m ohlcv → None.
    Searches both sources regardless of price_source_tf hint for maximum coverage.
    Returns None if no data within 1 hour of news time (prevents cross-period mismatch).
    """
    max_lookback = timedelta(hours=1)
    ts_floor = ts_news - max_lookback

    # Priority 1: 1m candles (any data_source: bybit or archive)
    cur.execute("""
        SELECT c FROM candles
        WHERE symbol=%s AND tf='1m' AND ts >= %s AND ts <= %s
        ORDER BY ts DESC LIMIT 1;
    """, (SYMBOL, ts_floor, ts_news))
    row = cur.fetchone()
    if row:
        return float(row[0])

    # Priority 2: 5m market_ohlcv
    cur.execute("""
        SELECT c FROM market_ohlcv
        WHERE symbol=%s AND tf='5m' AND ts >= %s AND ts <= %s
        ORDER BY ts DESC LIMIT 1;
    """, (SYMBOL, ts_floor, ts_news))
    row = cur.fetchone()
    return float(row[0]) if row else None


def _get_candles_24h(cur, ts_news, price_source_tf):
    """Get all candles in 24h window after news. Returns list of (ts, h, l, c)."""
    ts_end = ts_news + timedelta(hours=24)
    if price_source_tf == '1m':
        cur.execute("""
            SELECT ts, h, l, c FROM candles
            WHERE symbol=%s AND tf='1m'
              AND ts >= %s AND ts <= %s
            ORDER BY ts ASC;
        """, (SYMBOL, ts_news, ts_end))
        rows = cur.fetchall()
        if rows:
            return rows
    # Fallback to 5m
    cur.execute("""
        SELECT ts, h, l, c FROM market_ohlcv
        WHERE symbol=%s AND tf='5m'
          AND ts >= %s AND ts <= %s
        ORDER BY ts ASC;
    """, (SYMBOL, ts_news, ts_end))
    return cur.fetchall()


def _get_end_price_24h(cur, ts_news, price_source_tf):
    """Get price closest to +24h."""
    ts_target = ts_news + timedelta(hours=24)
    if price_source_tf == '1m':
        cur.execute("""
            SELECT c FROM candles
            WHERE symbol=%s AND tf='1m'
              AND ts >= %s - interval '5 minutes' AND ts <= %s + interval '5 minutes'
            ORDER BY ABS(EXTRACT(EPOCH FROM (ts - %s))) ASC LIMIT 1;
        """, (SYMBOL, ts_target, ts_target, ts_target))
        row = cur.fetchone()
        if row:
            return float(row[0])
    cur.execute("""
        SELECT c FROM market_ohlcv
        WHERE symbol=%s AND tf='5m'
          AND ts >= %s - interval '10 minutes' AND ts <= %s + interval '10 minutes'
        ORDER BY ABS(EXTRACT(EPOCH FROM (ts - %s))) ASC LIMIT 1;
    """, (SYMBOL, ts_target, ts_target, ts_target))
    row = cur.fetchone()
    return float(row[0]) if row else None


def _classify_end_state(ret_24h, max_drawdown):
    """Classify 24h end state."""
    if ret_24h is None:
        return None
    if ret_24h > 0.5 and max_drawdown > -0.3:
        return 'TREND_UP'
    if ret_24h > 0 and max_drawdown <= -0.3:
        return 'HELD'
    if ret_24h > -0.1 and max_drawdown < -0.5:
        return 'RECOVERED'
    if ret_24h < -0.5:
        return 'FURTHER_DROP'
    return 'MIXED'


def _classify_path_class(max_drawdown, max_runup, ret_24h, ret_30m):
    """7-class path classification.

    NO_MOVE / HOLD / RECOVER / CONTINUE_DOWN / CONTINUE_UP / MEAN_REVERT / CHOPPY
    """
    dd = abs(max_drawdown) if max_drawdown else 0
    ru = max_runup or 0
    r24 = ret_24h if ret_24h is not None else 0
    r30 = ret_30m if ret_30m is not None else 0

    # NO_MOVE: negligible price change
    if dd < 0.3 and ru < 0.3 and abs(r24) < 0.2:
        return 'NO_MOVE'
    # HOLD: small oscillation, flat end
    if abs(r24) < 0.3 and dd < 0.5 and ru < 0.5:
        return 'HOLD'
    # RECOVER: significant drawdown but positive 24h return
    if dd > 1.0 and r24 > 0:
        return 'RECOVER'
    # CONTINUE_DOWN: sustained downward movement
    if dd > 0.5 and r24 < -0.5:
        return 'CONTINUE_DOWN'
    # CONTINUE_UP: sustained upward movement
    if ru > 0.5 and r24 > 0.5:
        return 'CONTINUE_UP'
    # MEAN_REVERT: 30m direction opposite to 24h direction, 50%+ retracement
    if r30 != 0 and r24 != 0:
        if (r30 > 0.1 and r24 < -0.1) or (r30 < -0.1 and r24 > 0.1):
            return 'MEAN_REVERT'
    # CHOPPY: default
    return 'CHOPPY'


def _compute_direction(ret_val):
    """Classify direction based on return: UP / DOWN / FLAT."""
    if ret_val is None:
        return 'FLAT'
    if ret_val > 0.1:
        return 'UP'
    if ret_val < -0.1:
        return 'DOWN'
    return 'FLAT'


def _classify_path_shape(max_drawdown, max_runup, recovery_minutes, candles):
    """Classify path shape based on price behavior."""
    if max_drawdown is not None and max_drawdown < -0.5 and recovery_minutes is not None and recovery_minutes <= 720:
        return 'V_RECOVERY'
    # Spike fade: big runup in first 2h, then lost >50% of gain
    if candles and max_runup is not None and max_runup > 0.5:
        early_candles = [c for c in candles if (c[0] - candles[0][0]).total_seconds() <= 7200]
        if early_candles:
            early_max = max((float(c[1]) for c in early_candles), default=0)
            final_close = float(candles[-1][3])
            entry_price = float(candles[0][3])
            if entry_price > 0:
                early_gain = (early_max - entry_price) / entry_price * 100
                final_gain = (final_close - entry_price) / entry_price * 100
                if early_gain > 0.5 and final_gain < early_gain * 0.5:
                    return 'SPIKE_FADE'
    # Steady trend: check monotonicity within bands
    if candles and len(candles) >= 10:
        closes = [float(c[3]) for c in candles]
        n = len(closes)
        up_moves = sum(1 for i in range(1, n) if closes[i] >= closes[i - 1])
        if up_moves / (n - 1) > 0.7 or up_moves / (n - 1) < 0.3:
            return 'STEADY_TREND'
    return 'CHOP'


def _clamp_ret_24h(end_ret, candles, price_at):
    """Sanity clamp: cap ret_24h at +/- RET_24H_CLAMP (25%).
    Allow extreme values only if confirmed by >= RET_24H_EXTREME_CONFIRM_CANDLES
    independent candles closing beyond the threshold.
    """
    if end_ret is None or abs(end_ret) <= RET_24H_CLAMP:
        return end_ret

    # Count candles whose close price independently confirms the extreme return
    if candles and price_at and price_at > 0:
        confirm_count = 0
        threshold_price_up = price_at * (1 + RET_24H_CLAMP / 100)
        threshold_price_dn = price_at * (1 - RET_24H_CLAMP / 100)
        for _, _, _, c in candles:
            close = float(c)
            if end_ret > 0 and close >= threshold_price_up:
                confirm_count += 1
            elif end_ret < 0 and close <= threshold_price_dn:
                confirm_count += 1

        if confirm_count >= RET_24H_EXTREME_CONFIRM_CANDLES:
            return end_ret  # Confirmed extreme — keep raw value

    # Clamp to +/- RET_24H_CLAMP
    clamped = max(-RET_24H_CLAMP, min(RET_24H_CLAMP, end_ret))
    return round(clamped, 4)


def _compute_path(cur, news_id, ts_news):
    """Compute 24h price path for a single news item."""
    price_source_tf = _choose_price_source(ts_news)
    price_at = _get_price_at(cur, ts_news, price_source_tf)
    if price_at is None or price_at <= 0:
        return None

    candles = _get_candles_24h(cur, ts_news, price_source_tf)
    if not candles or len(candles) < 3:
        return None

    # Compute max drawdown and runup
    max_drawdown = 0.0
    max_runup = 0.0
    drawdown_ts = None
    runup_ts = None

    for ts, h, l, c in candles:
        low_ret = (float(l) - price_at) / price_at * 100
        high_ret = (float(h) - price_at) / price_at * 100
        if low_ret < max_drawdown:
            max_drawdown = low_ret
            drawdown_ts = ts
        if high_ret > max_runup:
            max_runup = high_ret
            runup_ts = ts

    # Recovery minutes: from max drawdown to first candle >= entry price
    recovery_minutes = None
    if drawdown_ts is not None and max_drawdown < -0.1:
        for ts, h, l, c in candles:
            if ts > drawdown_ts and float(c) >= price_at:
                recovery_minutes = int((ts - drawdown_ts).total_seconds() / 60)
                break

    # End price at +24h
    end_price = _get_end_price_24h(cur, ts_news, price_source_tf)
    end_ret = round((end_price - price_at) / price_at * 100, 4) if end_price and price_at > 0 else None

    # Sanity clamp: cap extreme ret_24h unless confirmed by multiple candles
    end_ret = _clamp_ret_24h(end_ret, candles, price_at)

    end_state = _classify_end_state(end_ret, max_drawdown)
    path_shape = _classify_path_shape(max_drawdown, max_runup, recovery_minutes, candles)

    # ── Compute ret_30m from candle data ──
    ret_30m = None
    ts_30m = ts_news + timedelta(minutes=30)
    for ts, h, l, c in candles:
        if ts >= ts_30m:
            ret_30m = round((float(c) - price_at) / price_at * 100, 4)
            break

    # ── 7-class path_class + direction columns ──
    path_class = _classify_path_class(max_drawdown, max_runup, end_ret, ret_30m)
    initial_move_dir = _compute_direction(ret_30m)
    follow_through_dir = _compute_direction(end_ret)
    recovered_flag = (max_drawdown is not None and max_drawdown < -1.0
                      and end_ret is not None and end_ret > -0.1)
    further_drop_flag = (max_drawdown is not None and max_drawdown < -0.5
                         and end_ret is not None
                         and end_ret < max_drawdown * 0.8)

    return {
        'news_id': news_id,
        'ts_news': ts_news,
        'btc_price_at': price_at,
        'price_source_tf': price_source_tf,
        'max_drawdown_24h': round(max_drawdown, 4),
        'max_runup_24h': round(max_runup, 4),
        'drawdown_ts': drawdown_ts,
        'runup_ts': runup_ts,
        'recovery_minutes': recovery_minutes,
        'end_price_24h': end_price,
        'end_ret_24h': end_ret,
        'end_state_24h': end_state,
        'path_shape': path_shape,
        'path_class': path_class,
        'initial_move_dir': initial_move_dir,
        'follow_through_dir': follow_through_dir,
        'recovered_flag': recovered_flag,
        'further_drop_flag': further_drop_flag,
    }


def _recompute_path_class(conn):
    """Recompute path_class for existing rows where path_class IS NULL."""
    _log('Recompute mode: updating rows with path_class IS NULL')
    total_updated = 0
    total_skipped = 0
    last_id = 0

    while True:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT npp.id, npp.news_id, npp.ts_news,
                       npp.max_drawdown_24h, npp.max_runup_24h,
                       npp.end_ret_24h, npp.btc_price_at
                FROM news_price_path npp
                WHERE npp.path_class IS NULL AND npp.id > %s
                ORDER BY npp.id ASC
                LIMIT %s;
            """, (last_id, BATCH_SIZE))
            rows = cur.fetchall()

        if not rows:
            break

        for row_id, news_id, ts_news, dd, ru, ret_24h, price_at in rows:
            last_id = row_id  # always advance cursor

            if price_at is None or float(price_at) <= 0:
                # Set path_class to CHOPPY to avoid re-processing
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE news_price_path SET path_class = 'CHOPPY'
                        WHERE id = %s;
                    """, (row_id,))
                conn.commit()  # commit immediately so rollback won't undo this
                total_skipped += 1
                continue

            # Compute ret_30m from candles
            ret_30m = None
            try:
                price_source_tf = _choose_price_source(ts_news)
                with conn.cursor() as cur:
                    candles = _get_candles_24h(cur, ts_news, price_source_tf)
                if candles and len(candles) >= 2:
                    ts_30m = ts_news + timedelta(minutes=30)
                    for ts, h, l, c in candles:
                        if ts >= ts_30m:
                            ret_30m = round((float(c) - float(price_at)) / float(price_at) * 100, 4)
                            break
            except Exception:
                pass

            dd_f = float(dd) if dd is not None else 0
            ru_f = float(ru) if ru is not None else 0
            r24_f = float(ret_24h) if ret_24h is not None else 0

            path_class = _classify_path_class(dd_f, ru_f, r24_f, ret_30m)
            initial_dir = _compute_direction(ret_30m)
            follow_dir = _compute_direction(r24_f)
            recovered = (dd_f < -1.0 and r24_f > -0.1)
            further_drop = (dd_f < -0.5 and r24_f < dd_f * 0.8)

            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE news_price_path SET
                        path_class = %s,
                        initial_move_dir = %s,
                        follow_through_dir = %s,
                        recovered_flag = %s,
                        further_drop_flag = %s
                    WHERE id = %s;
                """, (path_class, initial_dir, follow_dir,
                      recovered, further_drop, row_id))

            total_updated += 1

        conn.commit()
        _log(f'Recomputed {total_updated} rows, skipped {total_skipped} (last_id={last_id})')

    _log(f'Recompute DONE: {total_updated} updated, {total_skipped} skipped')


MIN_SAMPLE_THRESHOLD = 500  # minimum samples per path_class for stability


def check_path_class_stability(conn):
    """Check if all path_classes have >= MIN_SAMPLE_THRESHOLD samples.
    Returns (stable, report_dict).
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT path_class, COUNT(*) FROM news_price_path
            WHERE path_class IS NOT NULL
            GROUP BY path_class
            ORDER BY COUNT(*) ASC;
        """)
        rows = cur.fetchall()

    if not rows:
        return False, {'classes': {}, 'min_class': None, 'min_count': 0, 'stable': False}

    classes = {r[0]: r[1] for r in rows}
    min_class = min(classes, key=classes.get)
    min_count = classes[min_class]
    stable = min_count >= MIN_SAMPLE_THRESHOLD

    return stable, {
        'classes': classes,
        'min_class': min_class,
        'min_count': min_count,
        'stable': stable,
        'threshold': MIN_SAMPLE_THRESHOLD,
    }


def _rebuild_balanced(conn, from_month=None):
    """Stratified sampling rebuild: delete existing paths and recompute
    evenly across months to reduce recent-period bias.

    If from_month is given (YYYY-MM), only recompute from that month onward.
    """
    _log(f'BALANCED REBUILD: from={from_month or "all"}')

    with conn.cursor() as cur:
        # Get month distribution of news (source data)
        where_clause = ""
        params = []
        if from_month:
            where_clause = "AND to_char(n.ts, 'YYYY-MM') >= %s"
            params = [from_month]

        cur.execute(f"""
            SELECT to_char(n.ts, 'YYYY-MM') AS month, COUNT(*) AS cnt
            FROM news n
            WHERE n.ts < now() - interval '24 hours'
              {where_clause}
            GROUP BY month
            ORDER BY month;
        """, params)
        month_counts = cur.fetchall()

    if not month_counts:
        _log('No news data for balanced rebuild')
        return

    # Find the minimum month count to use as the per-month sample size
    min_per_month = min(r[1] for r in month_counts)
    # Cap at a reasonable max to avoid over-sampling sparse months
    sample_per_month = max(50, min(min_per_month, 500))
    _log(f'Months={len(month_counts)} min_per_month={min_per_month} '
         f'sample_per_month={sample_per_month}')

    # Delete existing paths for recompute range
    if from_month:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM news_price_path
                WHERE to_char(ts_news, 'YYYY-MM') >= %s;
            """, (from_month,))
            deleted = cur.rowcount
            conn.commit()
            _log(f'Deleted {deleted} existing paths from {from_month}')

    total_computed = 0
    total_skipped = 0

    for month_str, month_count in month_counts:
        # Sample evenly from each month
        with conn.cursor() as cur:
            cur.execute("""
                SELECT n.id, n.ts FROM news n
                LEFT JOIN news_price_path npp ON npp.news_id = n.id
                WHERE to_char(n.ts, 'YYYY-MM') = %s
                  AND npp.id IS NULL
                  AND n.ts < now() - interval '24 hours'
                ORDER BY RANDOM()
                LIMIT %s;
            """, (month_str, sample_per_month))
            rows = cur.fetchall()

        for news_id, ts_news in rows:
            if ts_news is None:
                total_skipped += 1
                continue
            try:
                with conn.cursor() as cur:
                    result = _compute_path(cur, news_id, ts_news)
                if result is None:
                    total_skipped += 1
                    continue
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO news_price_path
                            (news_id, ts_news, btc_price_at, price_source_tf,
                             max_drawdown_24h, max_runup_24h, drawdown_ts, runup_ts,
                             recovery_minutes, end_price_24h, end_ret_24h,
                             end_state_24h, path_shape,
                             path_class, initial_move_dir, follow_through_dir,
                             recovered_flag, further_drop_flag)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s)
                        ON CONFLICT (news_id) DO UPDATE SET
                            btc_price_at = EXCLUDED.btc_price_at,
                            price_source_tf = EXCLUDED.price_source_tf,
                            max_drawdown_24h = EXCLUDED.max_drawdown_24h,
                            max_runup_24h = EXCLUDED.max_runup_24h,
                            drawdown_ts = EXCLUDED.drawdown_ts,
                            runup_ts = EXCLUDED.runup_ts,
                            recovery_minutes = EXCLUDED.recovery_minutes,
                            end_price_24h = EXCLUDED.end_price_24h,
                            end_ret_24h = EXCLUDED.end_ret_24h,
                            end_state_24h = EXCLUDED.end_state_24h,
                            path_shape = EXCLUDED.path_shape,
                            path_class = EXCLUDED.path_class,
                            initial_move_dir = EXCLUDED.initial_move_dir,
                            follow_through_dir = EXCLUDED.follow_through_dir,
                            recovered_flag = EXCLUDED.recovered_flag,
                            further_drop_flag = EXCLUDED.further_drop_flag,
                            computed_at = now();
                    """, (
                        result['news_id'], result['ts_news'], result['btc_price_at'],
                        result['price_source_tf'],
                        result['max_drawdown_24h'], result['max_runup_24h'],
                        result['drawdown_ts'], result['runup_ts'],
                        result['recovery_minutes'],
                        result['end_price_24h'], result['end_ret_24h'],
                        result['end_state_24h'], result['path_shape'],
                        result['path_class'], result['initial_move_dir'],
                        result['follow_through_dir'],
                        result['recovered_flag'], result['further_drop_flag'],
                    ))
                conn.commit()
                total_computed += 1
            except Exception as e:
                conn.rollback()
                total_skipped += 1

        _log(f'Month {month_str}: processed, running total={total_computed}')

    _log(f'BALANCED REBUILD DONE: {total_computed} computed, {total_skipped} skipped')

    # Check stability
    stable, stability_report = check_path_class_stability(conn)
    if not stable:
        _log(f'UNSTABLE: {stability_report["min_class"]} has only '
             f'{stability_report["min_count"]} samples (need {MIN_SAMPLE_THRESHOLD})')
    else:
        _log(f'STABLE: all classes >= {MIN_SAMPLE_THRESHOLD} samples')


def main():
    parser = argparse.ArgumentParser(description='News 24h price path analysis')
    parser.add_argument('--resume', action='store_true')
    parser.add_argument('--recompute', action='store_true',
                        help='Recompute path_class for existing rows where NULL')
    parser.add_argument('--mode', choices=['normal', 'balanced'], default='normal',
                        help='Rebuild mode: normal (incremental) or balanced (stratified)')
    parser.add_argument('--from', dest='from_month', default=None,
                        help='Start month YYYY-MM for recompute/balanced mode')
    args = parser.parse_args()

    conn = get_conn()
    conn.autocommit = False

    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '300000';")
    conn.commit()

    # ── Balanced rebuild mode ──
    if args.mode == 'balanced':
        _rebuild_balanced(conn, from_month=args.from_month)
        conn.close()
        return

    # ── Recompute mode ──
    if args.recompute:
        _recompute_path_class(conn)
        conn.close()
        return

    last_id = 0
    if args.resume:
        cursor = get_last_cursor(conn, JOB_NAME)
        if cursor and 'last_news_id' in cursor:
            last_id = cursor['last_news_id']
            _log(f'Resuming from news_id > {last_id}')

    job_id = start_job(conn, JOB_NAME, metadata={'last_id': last_id})
    total_computed = 0
    total_skipped = 0
    batch_num = 0

    try:
        while True:
            if check_stop():
                _log('STOP signal received')
                update_progress(conn, job_id, {'last_news_id': last_id})
                finish_job(conn, job_id, status='PARTIAL', error='stopped_by_user')
                return
            if not check_pause():
                _log('STOP during pause')
                update_progress(conn, job_id, {'last_news_id': last_id})
                finish_job(conn, job_id, status='PARTIAL', error='stopped_during_pause')
                return

            with conn.cursor() as cur:
                cur.execute("""
                    SELECT n.id, n.ts FROM news n
                    LEFT JOIN news_price_path npp ON npp.news_id = n.id
                    WHERE n.id > %s AND npp.id IS NULL
                      AND n.ts < now() - interval '24 hours'
                    ORDER BY n.id ASC LIMIT %s;
                """, (last_id, BATCH_SIZE))
                rows = cur.fetchall()

            if not rows:
                _log('No more news needing path analysis')
                break

            batch_num += 1
            batch_ok = 0

            for news_id, ts_news in rows:
                if ts_news is None:
                    last_id = news_id
                    total_skipped += 1
                    continue

                try:
                    with conn.cursor() as cur:
                        result = _compute_path(cur, news_id, ts_news)

                    if result is None:
                        total_skipped += 1
                        last_id = news_id
                        continue

                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO news_price_path
                                (news_id, ts_news, btc_price_at, price_source_tf,
                                 max_drawdown_24h, max_runup_24h, drawdown_ts, runup_ts,
                                 recovery_minutes, end_price_24h, end_ret_24h,
                                 end_state_24h, path_shape,
                                 path_class, initial_move_dir, follow_through_dir,
                                 recovered_flag, further_drop_flag)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s)
                            ON CONFLICT (news_id) DO UPDATE SET
                                btc_price_at = EXCLUDED.btc_price_at,
                                price_source_tf = EXCLUDED.price_source_tf,
                                max_drawdown_24h = EXCLUDED.max_drawdown_24h,
                                max_runup_24h = EXCLUDED.max_runup_24h,
                                drawdown_ts = EXCLUDED.drawdown_ts,
                                runup_ts = EXCLUDED.runup_ts,
                                recovery_minutes = EXCLUDED.recovery_minutes,
                                end_price_24h = EXCLUDED.end_price_24h,
                                end_ret_24h = EXCLUDED.end_ret_24h,
                                end_state_24h = EXCLUDED.end_state_24h,
                                path_shape = EXCLUDED.path_shape,
                                path_class = EXCLUDED.path_class,
                                initial_move_dir = EXCLUDED.initial_move_dir,
                                follow_through_dir = EXCLUDED.follow_through_dir,
                                recovered_flag = EXCLUDED.recovered_flag,
                                further_drop_flag = EXCLUDED.further_drop_flag,
                                computed_at = now();
                        """, (
                            result['news_id'], result['ts_news'], result['btc_price_at'],
                            result['price_source_tf'],
                            result['max_drawdown_24h'], result['max_runup_24h'],
                            result['drawdown_ts'], result['runup_ts'],
                            result['recovery_minutes'],
                            result['end_price_24h'], result['end_ret_24h'],
                            result['end_state_24h'], result['path_shape'],
                            result['path_class'], result['initial_move_dir'],
                            result['follow_through_dir'],
                            result['recovered_flag'], result['further_drop_flag'],
                        ))

                    conn.commit()
                    batch_ok += 1
                    total_computed += 1

                except Exception as e:
                    conn.rollback()
                    _log(f'Path error news_id={news_id}: {e}')
                    total_skipped += 1

                last_id = news_id

            update_progress(conn, job_id, {'last_news_id': last_id},
                            inserted=total_computed)

            if batch_num % 5 == 0:
                _log(f'Batch {batch_num}: total={total_computed}, skipped={total_skipped}')

        finish_job(conn, job_id, status='COMPLETED')
        _log(f'DONE: {total_computed} computed, {total_skipped} skipped')

    except KeyboardInterrupt:
        _log('Interrupted')
        update_progress(conn, job_id, {'last_news_id': last_id})
        finish_job(conn, job_id, status='PARTIAL', error='KeyboardInterrupt')
    except Exception as e:
        _log(f'FATAL: {e}')
        traceback.print_exc()
        finish_job(conn, job_id, status='FAILED', error=str(e)[:500])
    finally:
        conn.close()


if __name__ == '__main__':
    main()
