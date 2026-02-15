"""
vol_profile.py — Volume Profile calculator daemon.

Runs every 5 minutes:
  1. Fetch last 240 1m candles from DB
  2. Build volume profile (price buckets of $50)
  3. Calculate POC (Point of Control), VAH/VAL (Value Area 70%)
  4. Upsert to vol_profile table

Consumed by: market_snapshot.py, position_manager.py, event_trigger.py, telegram_cmd_poller.py
"""
import json
import time
import os
import traceback
import psycopg2
from db_config import get_conn

SYMBOL = os.getenv('SYMBOL', 'BTC/USDT:USDT')
TF = '1m'
LOOKBACK = 240       # 4 hours of 1m candles
BIN_SIZE = 50        # $50 price buckets
VALUE_AREA_PCT = 0.7  # 70% of total volume
LOOP_INTERVAL = 300   # 5 minutes

LOG_PREFIX = '[vol_profile]'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def bucket(px):
    """Round price down to nearest bin_size bucket."""
    return float(int(px // BIN_SIZE) * BIN_SIZE)


def _get_db():
    """Get DB connection with autocommit."""
    return get_conn(autocommit=True)


def fetch_candles(cur):
    """Fetch last LOOKBACK 1m candles from DB."""
    cur.execute("""
        SELECT ts, o, h, l, c, v FROM candles
        WHERE symbol = %s AND tf = %s
        ORDER BY ts DESC LIMIT %s;
    """, (SYMBOL, TF, LOOKBACK))
    rows = cur.fetchall()
    if not rows:
        return []
    return [{'ts': r[0], 'o': float(r[1]), 'h': float(r[2]),
             'l': float(r[3]), 'c': float(r[4]), 'v': float(r[5])}
            for r in rows]


def build_profile(candles):
    """Build volume profile from candles.

    For each candle, distribute volume across price buckets
    between low and high (inclusive).

    Returns: dict {bucket_price_str: total_volume, ...}
    """
    profile = {}
    for c in candles:
        low_bucket = bucket(c['l'])
        high_bucket = bucket(c['h'])
        # Number of buckets this candle spans
        n_buckets = max(1, int((high_bucket - low_bucket) / BIN_SIZE) + 1)
        vol_per_bucket = c['v'] / n_buckets
        px = low_bucket
        while px <= high_bucket:
            key = str(px)
            profile[key] = profile.get(key, 0) + vol_per_bucket
            px += BIN_SIZE
    return profile


def calc_poc(profile):
    """Find Point of Control — price bucket with highest volume."""
    if not profile:
        return None
    return float(max(profile, key=profile.get))


def calc_value_area(profile, poc_price):
    """Calculate Value Area High/Low (70% of total volume, centered on POC).

    Starting from POC, expand outward by adding the higher-volume
    adjacent bucket until VALUE_AREA_PCT of total volume is reached.

    Returns: (vah, val)
    """
    if not profile or poc_price is None:
        return None, None

    total_vol = sum(profile.values())
    if total_vol <= 0:
        return None, None

    target_vol = total_vol * VALUE_AREA_PCT
    sorted_buckets = sorted(profile.keys(), key=lambda k: float(k))

    poc_key = str(poc_price)
    if poc_key not in profile:
        return None, None

    poc_idx = sorted_buckets.index(poc_key)
    area_vol = profile[poc_key]
    lo_idx = poc_idx
    hi_idx = poc_idx

    while area_vol < target_vol:
        can_go_lo = lo_idx > 0
        can_go_hi = hi_idx < len(sorted_buckets) - 1

        if not can_go_lo and not can_go_hi:
            break

        lo_vol = profile.get(sorted_buckets[lo_idx - 1], 0) if can_go_lo else -1
        hi_vol = profile.get(sorted_buckets[hi_idx + 1], 0) if can_go_hi else -1

        if lo_vol >= hi_vol:
            lo_idx -= 1
            area_vol += lo_vol
        else:
            hi_idx += 1
            area_vol += hi_vol

    val = float(sorted_buckets[lo_idx])
    vah = float(sorted_buckets[hi_idx])
    return vah, val


def upsert_profile(cur, ts, profile, poc, vah, val):
    """Upsert volume profile to DB."""
    cur.execute("""
        INSERT INTO vol_profile (symbol, tf, ts, bin_size, profile, poc, vah, val)
        VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s)
        ON CONFLICT (symbol, tf, ts) DO UPDATE SET
            bin_size = EXCLUDED.bin_size,
            profile = EXCLUDED.profile,
            poc = EXCLUDED.poc,
            vah = EXCLUDED.vah,
            val = EXCLUDED.val;
    """, (SYMBOL, TF, ts, BIN_SIZE,
          json.dumps(profile, ensure_ascii=False),
          poc, vah, val))


def run_once(cur):
    """Single calculation cycle."""
    candles = fetch_candles(cur)
    if len(candles) < 10:
        _log(f'not enough candles: {len(candles)}')
        return

    profile = build_profile(candles)
    poc = calc_poc(profile)
    vah, val = calc_value_area(profile, poc)

    # Use the most recent candle's timestamp
    latest_ts = candles[0]['ts']
    upsert_profile(cur, latest_ts, profile, poc, vah, val)
    _log(f'updated: POC=${poc:,.0f} VAH=${vah:,.0f} VAL=${val:,.0f} '
         f'buckets={len(profile)} candles={len(candles)}')


def main():
    _log('=== VOLUME PROFILE STARTED ===')
    conn = _get_db()
    _log(f'symbol={SYMBOL} tf={TF} lookback={LOOKBACK} bin=${BIN_SIZE} interval={LOOP_INTERVAL}s')

    while True:
        try:
            with conn.cursor() as cur:
                run_once(cur)
        except psycopg2.OperationalError:
            _log('DB connection lost, reconnecting...')
            try:
                conn.close()
            except Exception:
                pass
            conn = _get_db()
        except Exception:
            traceback.print_exc()

        time.sleep(LOOP_INTERVAL)


if __name__ == '__main__':
    main()
