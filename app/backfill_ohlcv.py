"""
backfill_ohlcv.py — 5m OHLCV 백필 (2023-11-01 ~ 현재).

Bybit V5 REST API 직접 호출로 5m OHLCV → market_ohlcv 테이블 저장.
1000개/배치 (Bybit max), ON CONFLICT DO UPDATE, backfill_job_runs에 커서 저장.

Usage:
    python3 backfill_ohlcv.py [--start 2023-11-01] [--end 2026-02-15]
    python3 backfill_ohlcv.py --resume
"""
import sys
import time
import argparse
import traceback
from datetime import datetime, timezone

sys.path.insert(0, '/root/trading-bot/app')
from db_config import get_conn
from psycopg2.extras import execute_values
from backfill_utils import (
    start_job, get_last_cursor, update_progress, finish_job,
    check_stop, check_pause,
    fetch_bybit_kline, normalize_symbol,
)

SYMBOL = 'BTC/USDT:USDT'
DB_SYMBOL = 'BTC/USDT:USDT'
TF = '5m'
LIMIT = 1000  # Bybit V5 max per request
BATCH_SLEEP = 0.15
LOG_PREFIX = '[backfill_ohlcv]'
JOB_NAME = 'backfill_ohlcv_5m'
DEFAULT_START = '2023-11-01'
MAX_EMPTY_STREAK = 5


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def main():
    parser = argparse.ArgumentParser(description='Backfill 5m OHLCV to market_ohlcv')
    parser.add_argument('--start', default=DEFAULT_START, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', default=None, help='End date (YYYY-MM-DD), default=now')
    parser.add_argument('--resume', action='store_true', help='Resume from last cursor')
    args = parser.parse_args()

    start_dt = datetime.strptime(args.start, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    end_dt = (
        datetime.strptime(args.end, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        if args.end
        else datetime.now(timezone.utc)
    )
    since_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    conn = get_conn()
    conn.autocommit = True

    if args.resume:
        cursor = get_last_cursor(conn, JOB_NAME)
        if cursor and 'since_ms' in cursor:
            since_ms = cursor['since_ms']
            _log(f'Resuming from cursor since_ms={since_ms}')

    _log(f'Backfilling {TF} OHLCV: '
         f'{datetime.fromtimestamp(since_ms/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")} → '
         f'{end_dt.strftime("%Y-%m-%d %H:%M")}')
    _log(f'API params: symbol={normalize_symbol(SYMBOL)} interval=5 limit={LIMIT}')

    job_id = start_job(conn, JOB_NAME, metadata={'start_ms': since_ms, 'end_ms': end_ms})
    cur = conn.cursor()

    total_rows = 0
    total_failed = 0
    batch_num = 0
    api_errors = 0
    empty_streak = 0
    backoff = 5

    try:
        while since_ms < end_ms:
            if check_stop():
                _log('STOP signal received')
                update_progress(conn, job_id, {'since_ms': since_ms},
                                inserted=total_rows, failed=total_failed)
                finish_job(conn, job_id, status='PARTIAL', error='stopped_by_user')
                break
            if not check_pause():
                _log('STOP during pause')
                update_progress(conn, job_id, {'since_ms': since_ms},
                                inserted=total_rows, failed=total_failed)
                finish_job(conn, job_id, status='PARTIAL', error='stopped_during_pause')
                break

            cursor_before = since_ms
            cursor_before_dt = datetime.fromtimestamp(since_ms / 1000, tz=timezone.utc)

            try:
                bars = fetch_bybit_kline(SYMBOL, TF, since_ms, limit=LIMIT, log_fn=_log)
            except Exception as e:
                api_errors += 1
                _log(f'API error (#{api_errors}): {e}, backoff {backoff}s')
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue

            backoff = 5

            if not bars:
                empty_streak += 1
                _log(f'EMPTY #{empty_streak}/{MAX_EMPTY_STREAK} '
                     f'cursor={cursor_before_dt.strftime("%Y-%m-%d %H:%M")}')
                if empty_streak >= MAX_EMPTY_STREAK:
                    _log(f'FATAL: {MAX_EMPTY_STREAK} consecutive empty — FAILED')
                    update_progress(conn, job_id, {'since_ms': since_ms},
                                    inserted=total_rows, failed=total_failed)
                    finish_job(conn, job_id, status='FAILED', error='empty_api_response')
                    break
                since_ms += 86400_000 * 5
                time.sleep(1)
                continue

            empty_streak = 0
            batch_count = len(bars)

            # Validate first bar on first batch
            if batch_num == 0:
                first_dt = datetime.fromtimestamp(bars[0][0] / 1000, tz=timezone.utc)
                drift_days = abs(bars[0][0] - since_ms) / 86400_000
                _log(f'FIRST BAR: {first_dt.strftime("%Y-%m-%d %H:%M")} '
                     f'(requested={cursor_before_dt.strftime("%Y-%m-%d %H:%M")}, '
                     f'drift={drift_days:.1f}d, bars={batch_count})')
                if drift_days > 30:
                    _log(f'FATAL: drift={drift_days:.0f} days — API parameter mismatch')
                    finish_job(conn, job_id, status='FAILED',
                               error=f'first_bar_drift_{drift_days:.0f}d')
                    break

            try:
                values = [
                    (DB_SYMBOL, TF,
                     datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc),
                     o, h, l, c, v)
                    for ts_ms, o, h, l, c, v in bars
                ]
                execute_values(cur, """
                    INSERT INTO market_ohlcv (symbol, tf, ts, o, h, l, c, v)
                    VALUES %s
                    ON CONFLICT (symbol, tf, ts) DO UPDATE
                    SET o=EXCLUDED.o, h=EXCLUDED.h, l=EXCLUDED.l,
                        c=EXCLUDED.c, v=EXCLUDED.v;
                """, values)
            except Exception as e:
                _log(f'DB error batch {batch_num}: {e}')
                total_failed += batch_count
                time.sleep(5)
                continue

            total_rows += batch_count
            batch_num += 1

            last_ms = bars[-1][0]
            since_ms = last_ms + 300_000  # +5 minutes for 5m candles
            cursor_after_dt = datetime.fromtimestamp(since_ms / 1000, tz=timezone.utc)

            # Per-batch log
            _log(f'B{batch_num}: {cursor_before_dt.strftime("%m-%d %H:%M")}→'
                 f'{cursor_after_dt.strftime("%m-%d %H:%M")} '
                 f'bars={batch_count} ins={batch_count} total={total_rows:,}')

            # Save cursor to DB every 3 batches (crash-safe ≈ 3000 bars ≈ 10d)
            if batch_num % 3 == 0:
                update_progress(conn, job_id, {'since_ms': since_ms},
                                inserted=total_rows, failed=total_failed)

            if batch_count < LIMIT and last_ms >= end_ms:
                _log(f'Short batch with last_ms >= end_ms, done.')
                break

            time.sleep(BATCH_SLEEP)

        if empty_streak < MAX_EMPTY_STREAK:
            update_progress(conn, job_id, {'since_ms': since_ms},
                            inserted=total_rows, failed=total_failed)
            finish_job(conn, job_id, status='COMPLETED')
        _log(f'Done. {total_rows:,} upserted, {total_failed:,} failed, '
             f'{api_errors} api_errors in {batch_num} batches.')

    except KeyboardInterrupt:
        _log('Interrupted by user')
        update_progress(conn, job_id, {'since_ms': since_ms},
                        inserted=total_rows, failed=total_failed)
        finish_job(conn, job_id, status='PARTIAL', error='KeyboardInterrupt')
    except Exception as e:
        _log(f'FATAL: {e}')
        traceback.print_exc()
        try:
            update_progress(conn, job_id, {'since_ms': since_ms},
                            inserted=total_rows, failed=total_failed)
            finish_job(conn, job_id, status='FAILED', error=str(e)[:500])
        except Exception:
            pass
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    main()
