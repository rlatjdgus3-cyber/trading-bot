"""
backfill_ohlcv.py — Fetch Bybit 5m OHLCV from 2024-01-01 -> now into market_ohlcv.

Uses same ccxt pattern as candles.py but for 5m timeframe.
~1,115 API calls (200 candles/batch), ~2 min runtime with 100ms sleep.

Usage: python3 backfill_ohlcv.py [--start 2024-01-01] [--end 2026-02-12]
"""
import os
import sys
import time
import argparse
from datetime import datetime, timezone

sys.path.insert(0, '/root/trading-bot/app')
import ccxt
from db_config import get_conn

SYMBOL = 'BTC/USDT:USDT'
TF = '5m'
LIMIT = 200
BATCH_SLEEP = 0.1
LOG_PREFIX = '[backfill_ohlcv]'


def _log(msg):
    print(f"{LOG_PREFIX} {msg}", flush=True)


def _db_conn():
    return get_conn()


def _make_exchange():
    return ccxt.bybit({
        'apiKey': os.getenv('BYBIT_API_KEY'),
        'secret': os.getenv('BYBIT_SECRET'),
        'enableRateLimit': True,
        'timeout': 20000,
        'options': {'defaultType': 'swap'},
    })


def main():
    parser = argparse.ArgumentParser(description='Backfill 5m OHLCV to market_ohlcv')
    parser.add_argument('--start', default='2024-01-01', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', default=None, help='End date (YYYY-MM-DD), default=now')
    args = parser.parse_args()

    start_dt = datetime.strptime(args.start, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    end_dt = (
        datetime.strptime(args.end, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        if args.end
        else datetime.now(timezone.utc)
    )
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    _log(f"Backfilling {TF} OHLCV from {start_dt.date()} to {end_dt.date()}")

    conn = _db_conn()
    conn.autocommit = True
    cur = conn.cursor()
    ex = _make_exchange()

    since_ms = start_ms
    total_rows = 0
    batch_num = 0

    try:
        while since_ms < end_ms:
            ohlcv = ex.fetch_ohlcv(SYMBOL, timeframe=TF, limit=LIMIT, params={'since': since_ms})
            if not ohlcv:
                _log("No data returned, stopping.")
                break

            for ms, o, h, l, c, v in ohlcv:
                cur.execute(
                    """
                    INSERT INTO market_ohlcv (symbol, tf, ts, o, h, l, c, v)
                    VALUES (%s, %s, to_timestamp(%s/1000.0), %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, tf, ts) DO UPDATE
                    SET o=EXCLUDED.o, h=EXCLUDED.h, l=EXCLUDED.l,
                        c=EXCLUDED.c, v=EXCLUDED.v;
                    """,
                    (SYMBOL, TF, ms, o, h, l, c, v),
                )

            batch_count = len(ohlcv)
            total_rows += batch_count
            batch_num += 1

            last_ms = ohlcv[-1][0]

            if batch_num % 100 == 0:
                _log(f"  batch {batch_num}: +{batch_count} rows, total={total_rows}")

            # Only stop on short batch if the last candle timestamp has reached end_ms
            if batch_count < LIMIT and last_ms >= end_ms:
                _log(f"Short batch with last_ms >= end_ms, done.")
                break

            # Advance past the last candle
            since_ms = last_ms + 1
            time.sleep(BATCH_SLEEP)

        _log(f"Done. Total rows upserted: {total_rows} in {batch_num} batches.")
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    main()
