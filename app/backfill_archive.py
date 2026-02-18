"""
backfill_archive.py — Bulk-load historical OHLCV from Binance data.vision CSVs.

Downloads and imports Binance public kline archives (ZIP files containing CSVs)
into the candles/market_ohlcv tables as cold storage for backtesting + news path analysis.

This is the "cold store" counterpart to real-time Bybit API backfill:
  - Hot store: Bybit API → candles (recent 180d, data_source='bybit')
  - Cold store: Binance archive → candles (historical, data_source='binance_archive')

Usage:
    python backfill_archive.py --tf 1m --start 2024-01 --end 2025-06
    python backfill_archive.py --tf 5m --start 2024-01 --end 2025-06
    python backfill_archive.py --tf 1m --start 2024-01 --end 2025-06 --dryrun
    python backfill_archive.py --file /path/to/BTCUSDT-1m-2024-01.csv  # local CSV
"""
import os
import sys
import csv
import io
import time
import zipfile
import argparse
import traceback
import urllib.request
from datetime import datetime, timezone

sys.path.insert(0, '/root/trading-bot/app')
from db_config import get_conn
from psycopg2.extras import execute_values
from psycopg2 import errors as pg_errors
from backfill_utils import start_job, update_progress, finish_job

LOG_PREFIX = '[backfill_archive]'
JOB_NAME = 'backfill_archive'
SYMBOL = 'BTC/USDT:USDT'
BINANCE_BASE_URL = 'https://data.binance.vision/data/spot/monthly/klines/BTCUSDT'

# Binance CSV columns: open_time, open, high, low, close, volume,
#   close_time, quote_volume, count, taker_buy_vol, taker_buy_quote_vol, ignore
BATCH_SIZE = 5000


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _download_zip(tf, year_month):
    """Download Binance archive ZIP for given month. Returns bytes or None."""
    url = f'{BINANCE_BASE_URL}/{tf}/BTCUSDT-{tf}-{year_month}.zip'
    _log(f'Downloading {url} ...')
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'backfill-archive/1.0'})
        resp = urllib.request.urlopen(req, timeout=60)
        data = resp.read()
        _log(f'Downloaded {len(data):,} bytes')
        return data
    except urllib.request.HTTPError as e:
        _log(f'HTTP {e.code}: {url} — skipping month')
        return None
    except Exception as e:
        _log(f'Download error: {e}')
        return None


def _parse_csv_from_zip(zip_bytes, tf, year_month):
    """Extract CSV from ZIP and parse OHLCV rows."""
    csv_name = f'BTCUSDT-{tf}-{year_month}.csv'
    rows = []
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            names = zf.namelist()
            target = csv_name if csv_name in names else names[0]
            with zf.open(target) as f:
                reader = csv.reader(io.TextIOWrapper(f, encoding='utf-8'))
                for line in reader:
                    if len(line) < 6:
                        continue
                    try:
                        ts_ms = int(line[0])
                        o = float(line[1])
                        h = float(line[2])
                        l = float(line[3])
                        c = float(line[4])
                        v = float(line[5])
                        rows.append((ts_ms, o, h, l, c, v))
                    except (ValueError, IndexError):
                        continue
    except Exception as e:
        _log(f'ZIP parse error: {e}')
    return rows


def _parse_csv_file(filepath):
    """Parse a local CSV file."""
    rows = []
    with open(filepath, 'r') as f:
        reader = csv.reader(f)
        for line in reader:
            if len(line) < 6:
                continue
            try:
                ts_ms = int(line[0])
                o = float(line[1])
                h = float(line[2])
                l = float(line[3])
                c = float(line[4])
                v = float(line[5])
                rows.append((ts_ms, o, h, l, c, v))
            except (ValueError, IndexError):
                continue
    return rows


def _upsert_batch(conn, rows, tf, data_source='binance_archive'):
    """Upsert a batch of rows into candles (1m) or market_ohlcv (5m+)."""
    table = 'candles' if tf == '1m' else 'market_ohlcv'
    values = [
        (SYMBOL, tf,
         datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc),
         o, h, l, c, v, data_source)
        for ts_ms, o, h, l, c, v in rows
    ]

    inserted = 0
    with conn.cursor() as cur:
        # Try with data_source column first; fall back only on UndefinedColumn
        try:
            execute_values(cur, f"""
                INSERT INTO {table} (symbol, tf, ts, o, h, l, c, v, data_source)
                VALUES %s
                ON CONFLICT (symbol, tf, ts) DO NOTHING;
            """, values)
            inserted = cur.rowcount
        except pg_errors.UndefinedColumn:
            conn.rollback()
            # Fallback: without data_source column
            values_no_ds = [
                (SYMBOL, tf,
                 datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc),
                 o, h, l, c, v)
                for ts_ms, o, h, l, c, v in rows
            ]
            execute_values(cur, f"""
                INSERT INTO {table} (symbol, tf, ts, o, h, l, c, v)
                VALUES %s
                ON CONFLICT (symbol, tf, ts) DO NOTHING;
            """, values_no_ds)
            inserted = cur.rowcount

    conn.commit()
    return inserted


def _generate_months(start_ym, end_ym):
    """Generate YYYY-MM strings between start and end (inclusive)."""
    start_y, start_m = int(start_ym[:4]), int(start_ym[5:7])
    end_y, end_m = int(end_ym[:4]), int(end_ym[5:7])
    y, m = start_y, start_m
    while (y, m) <= (end_y, end_m):
        yield f'{y:04d}-{m:02d}'
        m += 1
        if m > 12:
            m = 1
            y += 1


def main():
    parser = argparse.ArgumentParser(description='Bulk-load Binance archive data')
    parser.add_argument('--tf', default='1m', help='Timeframe: 1m, 5m, 15m, 1h')
    parser.add_argument('--start', default='2024-01', help='Start month YYYY-MM')
    parser.add_argument('--end', default=None, help='End month YYYY-MM (default=last month)')
    parser.add_argument('--file', default=None, help='Load from local CSV file')
    parser.add_argument('--dryrun', action='store_true', help='Download and count only')
    args = parser.parse_args()

    conn = get_conn()
    conn.autocommit = False

    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '600000';")
    conn.commit()

    # Run data_source migration
    try:
        with conn.cursor() as cur:
            table = 'candles' if args.tf == '1m' else 'market_ohlcv'
            cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS data_source TEXT DEFAULT 'bybit';")
        conn.commit()
    except Exception:
        conn.rollback()

    # ── Local file mode ──
    if args.file:
        _log(f'Loading local file: {args.file}')
        rows = _parse_csv_file(args.file)
        _log(f'Parsed {len(rows):,} rows')
        if args.dryrun:
            _log(f'DRYRUN: would insert {len(rows):,} rows')
            conn.close()
            return
        total = 0
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            _upsert_batch(conn, batch, args.tf)
            total += len(batch)
            _log(f'Inserted {total:,}/{len(rows):,}')
        _log(f'DONE: {total:,} rows from file')
        conn.close()
        return

    # ── Monthly download mode ──
    end_ym = args.end
    if not end_ym:
        now = datetime.now(timezone.utc)
        # Default to last completed month
        if now.month == 1:
            end_ym = f'{now.year - 1:04d}-12'
        else:
            end_ym = f'{now.year:04d}-{now.month - 1:02d}'

    months = list(_generate_months(args.start, end_ym))
    _log(f'Archive backfill: tf={args.tf}, months={args.start}~{end_ym} ({len(months)} months)')

    job_id = start_job(conn, JOB_NAME, metadata={
        'tf': args.tf, 'start': args.start, 'end': end_ym,
        'source': 'binance_archive',
    })

    total_rows = 0
    total_months = 0
    skipped_months = 0

    try:
        for ym in months:
            zip_bytes = _download_zip(args.tf, ym)
            if not zip_bytes:
                skipped_months += 1
                continue

            rows = _parse_csv_from_zip(zip_bytes, args.tf, ym)
            if not rows:
                _log(f'{ym}: empty CSV')
                skipped_months += 1
                continue

            _log(f'{ym}: {len(rows):,} rows parsed')

            if args.dryrun:
                total_rows += len(rows)
                total_months += 1
                continue

            month_inserted = 0
            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i:i + BATCH_SIZE]
                _upsert_batch(conn, batch, args.tf)
                month_inserted += len(batch)

            total_rows += month_inserted
            total_months += 1
            _log(f'{ym}: {month_inserted:,} upserted (total={total_rows:,})')

            update_progress(conn, job_id, {'last_month': ym},
                            inserted=total_rows)
            time.sleep(1)  # rate limit courtesy

        if args.dryrun:
            _log(f'DRYRUN: {total_rows:,} rows across {total_months} months '
                 f'(skipped={skipped_months})')
        else:
            _log(f'DONE: {total_rows:,} rows across {total_months} months '
                 f'(skipped={skipped_months})')

        finish_job(conn, job_id, status='COMPLETED')

    except KeyboardInterrupt:
        _log('Interrupted')
        finish_job(conn, job_id, status='PARTIAL', error='KeyboardInterrupt')
    except Exception as e:
        _log(f'FATAL: {e}')
        traceback.print_exc()
        finish_job(conn, job_id, status='FAILED', error=str(e)[:500])
    finally:
        conn.close()


if __name__ == '__main__':
    main()
