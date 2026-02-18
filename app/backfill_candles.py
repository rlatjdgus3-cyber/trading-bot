"""
backfill_candles.py — 1m 캔들 백필 (2023-11-01 ~ 현재).

Bybit V5 REST API 직접 호출로 1m OHLCV → candles 테이블 저장.
Window-based cursor: start~end 윈도우로 순차 전진, reverse order 방어.
1000개/배치 (Bybit max), ON CONFLICT DO UPDATE, backfill_job_runs에 커서 저장.

Usage:
    python backfill_candles.py                    # 전체 백필
    python backfill_candles.py --resume           # 마지막 커서부터 재개
    python backfill_candles.py --start 2024-06-01 # 특정 시작일
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
TF = '1m'
INTERVAL_MS = 60_000  # 1 minute in milliseconds
LIMIT = 1000  # Bybit V5 max per request
WINDOW_MS = LIMIT * INTERVAL_MS  # 1000 minutes window per request
BATCH_SLEEP = 0.15  # rate limit safety
LOG_PREFIX = '[backfill_candles]'
JOB_NAME = 'backfill_candles_1m'
DEFAULT_START = '2023-11-01'
MAX_STALL_STREAK = 5  # consecutive zero-data responses before force-advance


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _ms_to_dt(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def _dt_fmt(ms):
    return _ms_to_dt(ms).strftime('%Y-%m-%d %H:%M')


def main():
    parser = argparse.ArgumentParser(description='Backfill 1m candles to candles table')
    parser.add_argument('--start', default=DEFAULT_START, help='Start date YYYY-MM-DD')
    parser.add_argument('--end', default=None, help='End date YYYY-MM-DD (default=now)')
    parser.add_argument('--resume', action='store_true', help='Resume from last cursor')
    args = parser.parse_args()

    conn = get_conn()
    conn.autocommit = False

    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '300000';")  # 300s
    conn.commit()

    # Determine start/end
    start_dt = datetime.strptime(args.start, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    end_dt = (
        datetime.strptime(args.end, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        if args.end else datetime.now(timezone.utc)
    )
    cursor_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    if args.resume:
        saved = get_last_cursor(conn, JOB_NAME)
        if saved and 'since_ms' in saved:
            cursor_ms = saved['since_ms']
            _log(f'Resuming from cursor since_ms={cursor_ms} ({_dt_fmt(cursor_ms)})')

    _log(f'Backfilling {TF} candles: {_dt_fmt(cursor_ms)} → {end_dt.strftime("%Y-%m-%d %H:%M")}')
    _log(f'API params: symbol={normalize_symbol(SYMBOL)} interval=1 limit={LIMIT}')

    job_id = start_job(conn, JOB_NAME, metadata={'start_ms': cursor_ms, 'end_ms': end_ms})

    # ── Metrics ──
    total_inserted = 0
    total_conflict = 0
    total_failed = 0
    batch_num = 0
    api_errors = 0
    empty_streak = 0      # consecutive batches with returned_rows==0
    conflict_streak = 0   # consecutive batches with returned_rows>0 but inserted==0
    backoff = 5
    last_stall_reason = None
    last_http_status = None
    last_api_latency_ms = 0
    last_returned_rows = 0
    last_first_ts = None
    last_last_ts = None
    last_error = None
    finished = False

    def _save_cursor(force=False):
        """Save cursor + detailed metrics to DB."""
        cursor_data = {
            'since_ms': cursor_ms,
            'current_cursor_ts': _dt_fmt(cursor_ms),
            'last_returned_rows': last_returned_rows,
            'last_first_ts': _dt_fmt(last_first_ts) if last_first_ts else None,
            'last_last_ts': _dt_fmt(last_last_ts) if last_last_ts else None,
            'last_api_latency_ms': last_api_latency_ms,
            'last_http_status': last_http_status,
            'inserted_count': total_inserted,
            'conflict_count': total_conflict,
            'error_count': api_errors,
            'last_error': str(last_error)[:200] if last_error else None,
            'last_stall_reason': last_stall_reason,
        }
        update_progress(conn, job_id, cursor_data,
                        inserted=total_inserted, failed=total_failed)

    try:
        while cursor_ms < end_ms:
            if check_stop():
                _log('STOP signal received')
                _save_cursor()
                finish_job(conn, job_id, status='PARTIAL', error='stopped_by_user')
                finished = True
                break
            if not check_pause():
                _log('STOP during pause')
                _save_cursor()
                finish_job(conn, job_id, status='PARTIAL', error='stopped_during_pause')
                finished = True
                break

            # ── Window-based request: [cursor_ms, chunk_end_ms) ──
            chunk_end_ms = min(cursor_ms + WINDOW_MS, end_ms)
            cursor_before = cursor_ms

            t0 = time.time()
            try:
                bars = fetch_bybit_kline(SYMBOL, TF, cursor_ms, limit=LIMIT, log_fn=_log)
                last_api_latency_ms = int((time.time() - t0) * 1000)
                last_http_status = 200
            except Exception as e:
                last_api_latency_ms = int((time.time() - t0) * 1000)
                last_http_status = 'ERROR'
                last_error = e
                api_errors += 1
                _log(f'API error (#{api_errors}): {e}, latency={last_api_latency_ms}ms, backoff {backoff}s')
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
                # Save metrics even on error
                if api_errors % 3 == 0:
                    _save_cursor()
                continue

            backoff = 5  # reset on successful HTTP call
            last_returned_rows = len(bars) if bars else 0

            # ── Sort ascending (defense against reverse order) ──
            if bars:
                bars.sort(key=lambda x: x[0])
                last_first_ts = bars[0][0]
                last_last_ts = bars[-1][0]
            else:
                last_first_ts = None
                last_last_ts = None

            # ── Handle empty response ──
            if not bars:
                empty_streak += 1
                conflict_streak = 0
                _log(f'EMPTY #{empty_streak}/{MAX_STALL_STREAK} '
                     f'cursor={_dt_fmt(cursor_before)} chunk_end={_dt_fmt(chunk_end_ms)}')

                if empty_streak >= MAX_STALL_STREAK:
                    # Force-advance cursor to chunk_end instead of ABORT
                    last_stall_reason = f'empty_streak={empty_streak}_force_advance'
                    _log(f'STALL: {MAX_STALL_STREAK} consecutive empty. '
                         f'Force-advancing cursor: {_dt_fmt(cursor_before)} → {_dt_fmt(chunk_end_ms)}')
                    cursor_ms = chunk_end_ms
                    empty_streak = 0
                    _save_cursor()
                else:
                    # Advance by 1 day on each empty (gap-skip)
                    cursor_ms = min(cursor_before + 86400_000, chunk_end_ms)
                    time.sleep(1)
                continue

            empty_streak = 0

            # Validate first bar on first batch
            if batch_num == 0:
                first_dt = _ms_to_dt(bars[0][0])
                drift_days = abs(bars[0][0] - cursor_before) / 86400_000
                _log(f'FIRST BAR: {first_dt.strftime("%Y-%m-%d %H:%M")} '
                     f'(requested={_dt_fmt(cursor_before)}, '
                     f'drift={drift_days:.1f}d, bars={last_returned_rows})')
                if drift_days > 30:
                    _log(f'FATAL: drift={drift_days:.0f} days — API parameter mismatch')
                    finish_job(conn, job_id, status='FAILED',
                               error=f'first_bar_drift_{drift_days:.0f}d')
                    finished = True
                    break

            # ── DB insert (batch upsert) ──
            batch_inserted = 0
            try:
                with conn.cursor() as cur:
                    values = [
                        (DB_SYMBOL, TF,
                         datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc),
                         o, h, l, c, v)
                        for ts_ms, o, h, l, c, v in bars
                    ]
                    # DO NOTHING: skip duplicates, rowcount = actual new inserts
                    execute_values(cur, """
                        INSERT INTO candles (symbol, tf, ts, o, h, l, c, v)
                        VALUES %s
                        ON CONFLICT (symbol, tf, ts) DO NOTHING;
                    """, values)
                    batch_inserted = cur.rowcount
                conn.commit()
            except Exception as e:
                conn.rollback()
                last_error = e
                _log(f'DB error batch {batch_num}: {e}')
                total_failed += last_returned_rows
                time.sleep(5)
                # Still advance cursor to prevent infinite loop on DB errors
                cursor_ms = bars[-1][0] + INTERVAL_MS
                continue

            # ── Conflict detection (DO NOTHING: rowcount = new inserts only) ──
            batch_conflict = max(0, last_returned_rows - batch_inserted)
            total_inserted += batch_inserted
            total_conflict += batch_conflict
            batch_num += 1

            # Track conflict-only streaks (returned rows > 0 but all conflicts)
            if batch_inserted == 0 and last_returned_rows > 0:
                conflict_streak += 1
                if conflict_streak >= MAX_STALL_STREAK:
                    last_stall_reason = f'conflict_streak={conflict_streak}_all_duplicates'
                    _log(f'WARN: {conflict_streak} consecutive conflict-only batches '
                         f'(all duplicates). cursor advancing normally.')
            else:
                conflict_streak = 0

            # ── Advance cursor ──
            cursor_ms = bars[-1][0] + INTERVAL_MS
            cursor_after_dt = _dt_fmt(cursor_ms)

            # Per-batch log
            _log(f'B{batch_num}: {_dt_fmt(cursor_before)}→{cursor_after_dt} '
                 f'bars={last_returned_rows} ins={batch_inserted} '
                 f'dup={batch_conflict} total={total_inserted:,} '
                 f'latency={last_api_latency_ms}ms')

            # Save cursor to DB every 3 batches (crash-safe)
            if batch_num % 3 == 0:
                _save_cursor()

            if last_returned_rows < LIMIT:
                _log(f'Short batch ({last_returned_rows}<{LIMIT}), reached end of data')
                break

            time.sleep(BATCH_SLEEP)

        # Final save
        if not finished:
            _save_cursor()
            finish_job(conn, job_id, status='COMPLETED')
        _log(f'DONE: {total_inserted:,} inserted, {total_conflict:,} conflicts, '
             f'{total_failed:,} failed, {api_errors} api_errors in {batch_num} batches')

    except KeyboardInterrupt:
        _log('Interrupted by user')
        _save_cursor()
        finish_job(conn, job_id, status='PARTIAL', error='KeyboardInterrupt')
    except Exception as e:
        _log(f'FATAL: {e}')
        traceback.print_exc()
        try:
            _save_cursor()
            finish_job(conn, job_id, status='FAILED', error=str(e)[:500])
        except Exception:
            pass
    finally:
        conn.close()


if __name__ == '__main__':
    main()
