"""
backfill_macro_events.py — FRED API 등을 통해 CPI/NFP/FOMC 이력 데이터 수집.

Usage:
    python backfill_macro_events.py          # 전체 백필
    python backfill_macro_events.py --recent # 최근 90일만
"""
import os
import sys
import json
import time
import traceback
from datetime import datetime, timezone, timedelta

sys.path.insert(0, '/root/trading-bot/app')

LOG_PREFIX = '[backfill_macro]'
FRED_API_KEY = os.getenv('FRED_API_KEY', '')

# FRED series IDs for key macro indicators
FRED_SERIES = {
    'CPI': 'CPIAUCSL',          # Consumer Price Index
    'NFP': 'PAYEMS',            # Nonfarm Payrolls
    'UNRATE': 'UNRATE',         # Unemployment Rate
    'GDP': 'GDP',               # Gross Domestic Product
    'PPI': 'PPIFIS',            # Producer Price Index
    'FEDFUNDS': 'FEDFUNDS',     # Federal Funds Rate
}

# Known FOMC meeting dates (2023-2026) — manually maintained
FOMC_DATES = [
    # 2024
    '2024-01-31', '2024-03-20', '2024-05-01', '2024-06-12',
    '2024-07-31', '2024-09-18', '2024-11-07', '2024-12-18',
    # 2025
    '2025-01-29', '2025-03-19', '2025-05-07', '2025-06-18',
    '2025-07-30', '2025-09-17', '2025-10-29', '2025-12-17',
    # 2026
    '2026-01-28', '2026-03-18',
]


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _db_conn():
    from db_config import get_conn
    return get_conn()


def _fetch_fred_series(series_id, start_date='2023-11-01'):
    """Fetch data from FRED API."""
    if not FRED_API_KEY:
        _log(f'FRED_API_KEY not set, skipping {series_id}')
        return []

    try:
        import urllib.request
        url = (f'https://api.stlouisfed.org/fred/series/observations'
               f'?series_id={series_id}'
               f'&api_key={FRED_API_KEY}'
               f'&file_type=json'
               f'&observation_start={start_date}'
               f'&sort_order=desc')
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())

        observations = data.get('observations', [])
        _log(f'FRED {series_id}: {len(observations)} observations')
        return observations
    except Exception as e:
        _log(f'FRED {series_id} error: {e}')
        return []


def _fetch_btc_returns_at(conn, event_date):
    """Get BTC returns at 1h, 4h, 24h after event date.

    Uses market_ohlcv (1h preferred, 5m fallback) instead of candles.
    Fixed: candles only has 1m data, never 1h — was causing 0 results.
    """
    try:
        with conn.cursor() as cur:
            # Try market_ohlcv 1h first, fallback to 5m
            for tf in ('1h', '5m'):
                cur.execute(f"""
                    WITH event_price AS (
                        SELECT c AS price FROM market_ohlcv
                        WHERE symbol = 'BTC/USDT:USDT' AND tf = '{tf}'
                          AND ts >= %s::date AND ts < %s::date + interval '2 hours'
                        ORDER BY ts ASC LIMIT 1
                    ),
                    price_1h AS (
                        SELECT c AS price FROM market_ohlcv
                        WHERE symbol = 'BTC/USDT:USDT' AND tf = '{tf}'
                          AND ts >= %s::date + interval '1 hour'
                        ORDER BY ts ASC LIMIT 1
                    ),
                    price_4h AS (
                        SELECT c AS price FROM market_ohlcv
                        WHERE symbol = 'BTC/USDT:USDT' AND tf = '{tf}'
                          AND ts >= %s::date + interval '4 hours'
                        ORDER BY ts ASC LIMIT 1
                    ),
                    price_24h AS (
                        SELECT c AS price FROM market_ohlcv
                        WHERE symbol = 'BTC/USDT:USDT' AND tf = '{tf}'
                          AND ts >= %s::date + interval '24 hours'
                        ORDER BY ts ASC LIMIT 1
                    )
                    SELECT
                        (SELECT price FROM event_price) AS p0,
                        (SELECT price FROM price_1h) AS p1h,
                        (SELECT price FROM price_4h) AS p4h,
                        (SELECT price FROM price_24h) AS p24h;
                """, (event_date, event_date, event_date, event_date, event_date))
                row = cur.fetchone()
                if row and row[0]:
                    p0 = float(row[0])
                    ret_1h = round((float(row[1]) - p0) / p0 * 100, 4) if row[1] else None
                    ret_4h = round((float(row[2]) - p0) / p0 * 100, 4) if row[2] else None
                    ret_24h = round((float(row[3]) - p0) / p0 * 100, 4) if row[3] else None
                    return ret_1h, ret_4h, ret_24h
            return None, None, None
    except Exception:
        return None, None, None


def backfill_fred(conn, recent_only=False):
    """Backfill from FRED API."""
    start = '2024-01-01' if recent_only else '2023-11-01'
    inserted = 0

    for event_type, series_id in FRED_SERIES.items():
        observations = _fetch_fred_series(series_id, start)
        for obs in observations:
            try:
                date_str = obs.get('date', '')
                value = obs.get('value', '')
                if value == '.' or not value:
                    continue
                actual = float(value)

                ret_1h, ret_4h, ret_24h = _fetch_btc_returns_at(conn, date_str)

                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO macro_events (event_type, event_date, actual_value,
                                                  btc_ret_1h, btc_ret_4h, btc_ret_24h)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (event_type, event_date) DO UPDATE SET
                            actual_value = EXCLUDED.actual_value,
                            btc_ret_1h = COALESCE(EXCLUDED.btc_ret_1h, macro_events.btc_ret_1h),
                            btc_ret_4h = COALESCE(EXCLUDED.btc_ret_4h, macro_events.btc_ret_4h),
                            btc_ret_24h = COALESCE(EXCLUDED.btc_ret_24h, macro_events.btc_ret_24h);
                    """, (event_type, date_str, actual, ret_1h, ret_4h, ret_24h))
                conn.commit()
                inserted += 1
            except Exception as e:
                conn.rollback()
                _log(f'insert error {event_type}/{date_str}: {e}')

        time.sleep(1)  # FRED rate limit

    _log(f'FRED backfill complete: {inserted} records')
    return inserted


def backfill_fomc(conn):
    """Backfill FOMC meeting dates."""
    inserted = 0
    for date_str in FOMC_DATES:
        try:
            ret_1h, ret_4h, ret_24h = _fetch_btc_returns_at(conn, date_str)
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO macro_events (event_type, event_date,
                                              btc_ret_1h, btc_ret_4h, btc_ret_24h)
                    VALUES ('FOMC', %s, %s, %s, %s)
                    ON CONFLICT (event_type, event_date) DO UPDATE SET
                        btc_ret_1h = COALESCE(EXCLUDED.btc_ret_1h, macro_events.btc_ret_1h),
                        btc_ret_4h = COALESCE(EXCLUDED.btc_ret_4h, macro_events.btc_ret_4h),
                        btc_ret_24h = COALESCE(EXCLUDED.btc_ret_24h, macro_events.btc_ret_24h);
                """, (date_str, ret_1h, ret_4h, ret_24h))
            conn.commit()
            inserted += 1
        except Exception as e:
            conn.rollback()
            _log(f'FOMC insert error {date_str}: {e}')

    _log(f'FOMC backfill complete: {inserted} records')
    return inserted


def main():
    recent_only = '--recent' in sys.argv
    _log(f'START (recent_only={recent_only})')

    conn = _db_conn()
    try:
        # Ensure table exists
        from db_migrations import ensure_macro_events
        ensure_macro_events()

        total = 0
        total += backfill_fred(conn, recent_only=recent_only)
        total += backfill_fomc(conn)

        _log(f'DONE: total {total} records')
    except Exception as e:
        _log(f'ERROR: {e}')
        traceback.print_exc()
    finally:
        conn.close()


if __name__ == '__main__':
    main()
