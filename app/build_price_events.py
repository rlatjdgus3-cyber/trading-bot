"""
build_price_events.py — 가격 이벤트 감지.

market_ohlcv (5m) 데이터에서 유의미한 가격 변동 이벤트를 감지하여
price_events 테이블에 저장.

감지 규칙:
- 5m_ret:  |5분 수익률| >= 1.0%
- 15m_ret: |15분 수익률| >= 1.5%
- atr_z:   ATR z-score >= 2.0
- vol_z:   Volume z-score >= 2.5

5분 이내 중복 이벤트는 move_pct가 큰 쪽으로 병합.
event_id = sha256(symbol:start_ts_ms:direction)[:16] 으로 멱등성 보장.

Usage:
    python build_price_events.py
    python build_price_events.py --start 2024-01-01
"""
import sys
import hashlib
import argparse
import traceback
from datetime import datetime, timezone
from collections import deque

sys.path.insert(0, '/root/trading-bot/app')
from db_config import get_conn
from backfill_utils import start_job, finish_job, update_progress

LOG_PREFIX = '[build_price_events]'
JOB_NAME = 'build_price_events'
SYMBOL = 'BTC/USDT:USDT'

# Detection thresholds
THRESH_5M_RET = 1.0    # %
THRESH_15M_RET = 1.5   # %
THRESH_ATR_Z = 2.0
THRESH_VOL_Z = 2.5

# Dedup: merge events within 5 minutes
DEDUP_WINDOW_MIN = 5

# Rolling window for z-score calculation
ROLLING_WINDOW = 288  # 288 x 5min = 24h


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _make_event_id(symbol, start_ts, direction):
    """Generate deterministic event ID."""
    ts_ms = int(start_ts.timestamp() * 1000)
    raw = f"{symbol}:{ts_ms}:{direction}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _load_ohlcv(conn, start_date, end_date):
    """Load 5m OHLCV data into memory. Returns list of (ts, o, h, l, c, v)."""
    _log(f'Loading 5m OHLCV from {start_date} to {end_date}...')
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ts, o, h, l, c, v FROM market_ohlcv
            WHERE symbol = %s AND tf = '5m'
              AND ts >= %s AND ts <= %s
            ORDER BY ts ASC;
        """, (SYMBOL, start_date, end_date))
        rows = cur.fetchall()
    _log(f'Loaded {len(rows)} 5m bars')
    return rows


def _detect_events(bars):
    """Detect price events from 5m bars using rolling statistics (O(n))."""
    if len(bars) < ROLLING_WINDOW + 3:
        _log(f'Not enough bars ({len(bars)}), need at least {ROLLING_WINDOW + 3}')
        return []

    events = []

    # Convert to float arrays once
    closes = [float(b[4]) for b in bars]
    highs = [float(b[2]) for b in bars]
    lows = [float(b[3]) for b in bars]
    vols = [float(b[5]) for b in bars]
    timestamps = [b[0] for b in bars]

    # Pre-compute True Range for all bars
    trs = [highs[0] - lows[0]]  # first bar: just H-L
    for i in range(1, len(bars)):
        tr = max(highs[i] - lows[i],
                 abs(highs[i] - closes[i-1]),
                 abs(lows[i] - closes[i-1]))
        trs.append(tr)

    # Rolling ATR stats using deque (O(1) per step)
    tr_window = deque()
    tr_sum = 0.0
    tr_sq_sum = 0.0

    vol_window = deque()
    vol_sum = 0.0
    vol_sq_sum = 0.0

    # Initialize rolling windows
    for j in range(ROLLING_WINDOW):
        tr_val = trs[j]
        tr_window.append(tr_val)
        tr_sum += tr_val
        tr_sq_sum += tr_val * tr_val

        v_val = vols[j]
        vol_window.append(v_val)
        vol_sum += v_val
        vol_sq_sum += v_val * v_val

    for i in range(ROLLING_WINDOW, len(bars)):
        c_cur = closes[i]
        c_prev = closes[i-1]

        if c_prev <= 0:
            # Slide windows forward
            old_tr = tr_window.popleft()
            tr_sum -= old_tr
            tr_sq_sum -= old_tr * old_tr
            new_tr = trs[i]
            tr_window.append(new_tr)
            tr_sum += new_tr
            tr_sq_sum += new_tr * new_tr

            old_vol = vol_window.popleft()
            vol_sum -= old_vol
            vol_sq_sum -= old_vol * old_vol
            vol_window.append(vols[i])
            vol_sum += vols[i]
            vol_sq_sum += vols[i] * vols[i]
            continue

        ts = timestamps[i]
        ret_5m = ((c_cur - c_prev) / c_prev) * 100

        # --- 5m_ret trigger ---
        if abs(ret_5m) >= THRESH_5M_RET:
            direction = 1 if ret_5m > 0 else -1
            events.append({
                'start_ts': ts,
                'direction': direction,
                'move_pct': round(abs(ret_5m), 4),
                'trigger_type': '5m_ret',
                'btc_price_at': c_cur,
            })

        # --- ATR z-score (rolling) ---
        n = len(tr_window)
        mean_atr = tr_sum / n
        var_atr = (tr_sq_sum / n) - (mean_atr * mean_atr)
        std_atr = max(0, var_atr) ** 0.5  # guard against floating point negative

        current_tr = trs[i]
        if std_atr > 0:
            atr_z = (current_tr - mean_atr) / std_atr
            if atr_z >= THRESH_ATR_Z:
                direction = 1 if c_cur > c_prev else -1
                events.append({
                    'start_ts': ts,
                    'direction': direction,
                    'move_pct': round(abs(ret_5m), 4),
                    'trigger_type': 'atr_z',
                    'atr_z': round(atr_z, 2),
                    'btc_price_at': c_cur,
                })

        # --- Volume z-score (rolling) ---
        v_cur = vols[i]
        mean_vol = vol_sum / n
        var_vol = (vol_sq_sum / n) - (mean_vol * mean_vol)
        std_vol = max(0, var_vol) ** 0.5

        if std_vol > 0:
            vol_z = (v_cur - mean_vol) / std_vol
            if vol_z >= THRESH_VOL_Z:
                direction = 1 if c_cur > c_prev else -1
                events.append({
                    'start_ts': ts,
                    'direction': direction,
                    'move_pct': round(abs(ret_5m), 4),
                    'trigger_type': 'vol_z',
                    'vol_spike_z': round(vol_z, 2),
                    'btc_price_at': c_cur,
                })

        # Slide windows: remove oldest, add current
        old_tr = tr_window.popleft()
        tr_sum -= old_tr
        tr_sq_sum -= old_tr * old_tr
        tr_window.append(current_tr)
        tr_sum += current_tr
        tr_sq_sum += current_tr * current_tr

        old_vol = vol_window.popleft()
        vol_sum -= old_vol
        vol_sq_sum -= old_vol * old_vol
        vol_window.append(v_cur)
        vol_sum += v_cur
        vol_sq_sum += v_cur * v_cur

    # --- 15m return events (aggregate 3x5m) ---
    for i in range(3, len(bars), 3):
        if i + 2 >= len(bars):
            break
        ts = timestamps[i]
        c_cur = closes[min(i + 2, len(bars) - 1)]
        c_prev = closes[i - 1]
        if c_prev <= 0:
            continue

        ret_15m = ((c_cur - c_prev) / c_prev) * 100
        if abs(ret_15m) >= THRESH_15M_RET:
            direction = 1 if ret_15m > 0 else -1
            events.append({
                'start_ts': ts,
                'direction': direction,
                'move_pct': round(abs(ret_15m), 4),
                'trigger_type': '15m_ret',
                'btc_price_at': c_cur,
            })

    _log(f'Detected {len(events)} raw events before dedup')
    return events


def _dedup_events(events):
    """Merge events within DEDUP_WINDOW_MIN, keeping highest move_pct."""
    if not events:
        return []

    events.sort(key=lambda e: e['start_ts'])

    merged = []
    current = events[0]

    for evt in events[1:]:
        dt = evt['start_ts'] - current['start_ts']
        same_window = dt.total_seconds() <= DEDUP_WINDOW_MIN * 60
        same_dir = evt['direction'] == current['direction']

        if same_window and same_dir:
            if evt['move_pct'] > current['move_pct']:
                evt.setdefault('atr_z', current.get('atr_z'))
                evt.setdefault('vol_spike_z', current.get('vol_spike_z'))
                current = evt
            else:
                current.setdefault('atr_z', evt.get('atr_z'))
                current.setdefault('vol_spike_z', evt.get('vol_spike_z'))
        else:
            merged.append(current)
            current = evt

    merged.append(current)
    _log(f'After dedup: {len(merged)} events')
    return merged


def _compute_forward_returns(conn, events):
    """Compute ret_1h, ret_4h, ret_24h for each event using batch query."""
    if not events:
        return events

    _log(f'Computing forward returns for {len(events)} events...')

    # Load all 5m close prices into a sorted dict for O(log n) lookup
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ts, c FROM market_ohlcv
            WHERE symbol = %s AND tf = '5m'
            ORDER BY ts ASC;
        """, (SYMBOL,))
        all_prices = cur.fetchall()

    if not all_prices:
        return events

    # Build sorted list for bisect
    import bisect
    price_ts = [row[0] for row in all_prices]
    price_vals = [float(row[1]) for row in all_prices]

    for evt in events:
        ts = evt['start_ts']
        price_at = evt.get('btc_price_at')
        if not price_at or price_at <= 0:
            continue

        from datetime import timedelta
        for minutes, key in [(60, 'ret_1h'), (240, 'ret_4h'), (1440, 'ret_24h')]:
            target_ts = ts + timedelta(minutes=minutes)
            idx = bisect.bisect_left(price_ts, target_ts)
            if idx < len(price_vals):
                ret = round(((price_vals[idx] - price_at) / price_at) * 100, 4)
                evt[key] = ret

    return events


def _save_events(conn, events):
    """Save events to price_events table in batches."""
    inserted = 0
    updated = 0

    batch = []
    for evt in events:
        event_id = _make_event_id(SYMBOL, evt['start_ts'], evt['direction'])
        batch.append((
            event_id, SYMBOL, evt['start_ts'], evt['direction'],
            evt['move_pct'], evt['trigger_type'],
            evt.get('vol_spike_z'), evt.get('atr_z'),
            evt.get('btc_price_at'),
            evt.get('ret_1h'), evt.get('ret_4h'), evt.get('ret_24h'),
        ))

    # Batch insert with single commit
    try:
        with conn.cursor() as cur:
            for params in batch:
                cur.execute("""
                    INSERT INTO price_events
                        (event_id, symbol, start_ts, direction, move_pct,
                         trigger_type, vol_spike_z, atr_z, btc_price_at,
                         ret_1h, ret_4h, ret_24h)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (event_id) DO UPDATE SET
                        move_pct = GREATEST(price_events.move_pct, EXCLUDED.move_pct),
                        vol_spike_z = COALESCE(EXCLUDED.vol_spike_z, price_events.vol_spike_z),
                        atr_z = COALESCE(EXCLUDED.atr_z, price_events.atr_z),
                        ret_1h = COALESCE(EXCLUDED.ret_1h, price_events.ret_1h),
                        ret_4h = COALESCE(EXCLUDED.ret_4h, price_events.ret_4h),
                        ret_24h = COALESCE(EXCLUDED.ret_24h, price_events.ret_24h);
                """, params)
        conn.commit()
        inserted = len(batch)
    except Exception as e:
        conn.rollback()
        _log(f'Batch save error: {e}')
        # Fallback to individual inserts
        for params in batch:
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO price_events
                            (event_id, symbol, start_ts, direction, move_pct,
                             trigger_type, vol_spike_z, atr_z, btc_price_at,
                             ret_1h, ret_4h, ret_24h)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (event_id) DO UPDATE SET
                            move_pct = GREATEST(price_events.move_pct, EXCLUDED.move_pct),
                            vol_spike_z = COALESCE(EXCLUDED.vol_spike_z, price_events.vol_spike_z),
                            atr_z = COALESCE(EXCLUDED.atr_z, price_events.atr_z),
                            ret_1h = COALESCE(EXCLUDED.ret_1h, price_events.ret_1h),
                            ret_4h = COALESCE(EXCLUDED.ret_4h, price_events.ret_4h),
                            ret_24h = COALESCE(EXCLUDED.ret_24h, price_events.ret_24h);
                    """, params)
                conn.commit()
                updated += 1
            except Exception:
                conn.rollback()

    return inserted, updated


def main():
    parser = argparse.ArgumentParser(description='Build price events from OHLCV data')
    parser.add_argument('--start', default='2023-11-01', help='Start date YYYY-MM-DD')
    parser.add_argument('--end', default=None, help='End date YYYY-MM-DD (default=now)')
    args = parser.parse_args()

    start_dt = datetime.strptime(args.start, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    end_dt = (
        datetime.strptime(args.end, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        if args.end else datetime.now(timezone.utc)
    )

    conn = get_conn()
    conn.autocommit = False

    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '300000';")
    conn.commit()

    job_id = start_job(conn, JOB_NAME, metadata={
        'start': str(start_dt.date()), 'end': str(end_dt.date())
    })

    try:
        bars = _load_ohlcv(conn, start_dt, end_dt)
        if not bars:
            _log('No OHLCV data found')
            finish_job(conn, job_id, status='COMPLETED', error='No data')
            return

        events = _detect_events(bars)
        events = _dedup_events(events)
        events = _compute_forward_returns(conn, events)

        inserted, updated = _save_events(conn, events)

        finish_job(conn, job_id, status='COMPLETED')
        _log(f'DONE: {inserted} saved, {updated} fallback-saved out of {len(events)} events')

    except Exception as e:
        _log(f'FATAL: {e}')
        traceback.print_exc()
        finish_job(conn, job_id, status='FAILED', error=str(e)[:500])
    finally:
        conn.close()


if __name__ == '__main__':
    main()
