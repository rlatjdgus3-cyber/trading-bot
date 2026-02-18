"""
aggregate_candles.py — 1m 캔들 → 5m/15m/1h 집계.

candles (1m) 테이블의 데이터를 market_ohlcv에 5m, 15m, 1h 타임프레임으로 집계.
ON CONFLICT DO UPDATE로 멱등성 보장.

Usage:
    python aggregate_candles.py                       # 전체 집계
    python aggregate_candles.py --tf 15m              # 15m만
    python aggregate_candles.py --start 2024-01-01    # 특정 시작일
"""
import sys
import argparse
import traceback
from datetime import datetime, timezone

sys.path.insert(0, '/root/trading-bot/app')
from db_config import get_conn
from backfill_utils import start_job, finish_job, update_progress, check_stop, check_pause

LOG_PREFIX = '[aggregate_candles]'
JOB_NAME = 'aggregate_candles'
SYMBOL = 'BTC/USDT:USDT'

# Timeframe definitions: (name, interval_sql, minutes)
TIMEFRAMES = [
    ('5m', "5 minutes", 5),
    ('15m', "15 minutes", 15),
    ('1h', "1 hour", 60),
]


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _aggregate_tf(conn, tf_name, interval_sql, minutes, start_date, end_date):
    """Aggregate 1m candles into a specific timeframe."""
    _log(f'Aggregating to {tf_name} ({interval_sql})...')

    with conn.cursor() as cur:
        # Floor timestamps to interval boundaries, preserving timestamptz
        cur.execute(f"""
            INSERT INTO market_ohlcv (symbol, tf, ts, o, h, l, c, v)
            SELECT
                %s,
                %s,
                to_timestamp(
                    floor(extract(epoch from ts) / (%s * 60)) * (%s * 60)
                ) AT TIME ZONE 'UTC' AS bucket_ts,
                (array_agg(o ORDER BY ts ASC))[1] AS o,
                MAX(h) AS h,
                MIN(l) AS l,
                (array_agg(c ORDER BY ts DESC))[1] AS c,
                SUM(v) AS v
            FROM candles
            WHERE symbol = %s AND tf = '1m'
              AND ts >= %s AND ts <= %s
            GROUP BY bucket_ts
            HAVING COUNT(*) >= 1
            ON CONFLICT (symbol, tf, ts) DO UPDATE SET
                o = EXCLUDED.o,
                h = EXCLUDED.h,
                l = EXCLUDED.l,
                c = EXCLUDED.c,
                v = EXCLUDED.v;
        """, (SYMBOL, tf_name, minutes, minutes, SYMBOL, start_date, end_date))

        rows = cur.rowcount
        conn.commit()
        _log(f'{tf_name}: {rows} bars upserted')
        return rows


def main():
    parser = argparse.ArgumentParser(description='Aggregate 1m candles to higher timeframes')
    parser.add_argument('--start', default='2023-11-01', help='Start date YYYY-MM-DD')
    parser.add_argument('--end', default=None, help='End date YYYY-MM-DD (default=now)')
    parser.add_argument('--tf', default=None, help='Specific timeframe (5m/15m/1h)')
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
        'start': str(start_dt.date()), 'end': str(end_dt.date()),
        'tf': args.tf or 'all',
    })

    total_rows = 0

    try:
        for tf_name, interval_sql, minutes in TIMEFRAMES:
            if args.tf and args.tf != tf_name:
                continue

            if check_stop():
                _log('STOP signal received')
                finish_job(conn, job_id, status='PARTIAL', error='stopped_by_user')
                break
            if not check_pause():
                _log('STOP during pause')
                finish_job(conn, job_id, status='PARTIAL', error='stopped_during_pause')
                break

            rows = _aggregate_tf(conn, tf_name, interval_sql, minutes, start_dt, end_dt)
            total_rows += rows

            update_progress(conn, job_id, {'last_tf': tf_name}, inserted=rows)

        finish_job(conn, job_id, status='COMPLETED')
        _log(f'DONE: {total_rows} total bars aggregated')

    except Exception as e:
        _log(f'FATAL: {e}')
        traceback.print_exc()
        finish_job(conn, job_id, status='FAILED', error=str(e)[:500])
    finally:
        conn.close()


if __name__ == '__main__':
    main()
