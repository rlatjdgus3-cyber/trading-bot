"""
prune_candles_1m.py — Safe 1m candle pruning with 5m verification.

Structural rules:
  - NEVER hard abort. Degrade to partial prune with warning.
  - Auto-adjust prune start = max(5m_start_ts, retention_cutoff)
  - If 5m doesn't exist, attempt ONE aggregate (recent data only, NOT full historical)
  - Skip uncovered historical zones (mark ARCHIVE_REQUIRED)
  - Retain RETAIN_DAYS (30d) of 1m candles

Usage:
    python prune_candles_1m.py          # full run
    python prune_candles_1m.py --resume # resume
    python prune_candles_1m.py --dryrun # count only, no delete
"""
import os
import sys
import subprocess
import argparse
import traceback
import time
from datetime import datetime, timezone, timedelta

sys.path.insert(0, '/root/trading-bot/app')
from db_config import get_conn
from backfill_utils import (
    start_job, update_progress, finish_job,
    check_stop, check_pause,
)

LOG_PREFIX = '[prune_candles_1m]'
JOB_NAME = 'prune_candles_1m'
RETAIN_DAYS = 30  # LIVE_DB: 30 days only (down from 180)
BATCH_SIZE = 10000
AGGREGATE_LOCK_FILE = '/tmp/aggregate_5m.lock'
AGGREGATE_TIMEOUT_SEC = 300
# Max gap (months) to attempt auto-aggregate. Anything larger → ARCHIVE_REQUIRED
MAX_AUTO_AGGREGATE_MONTHS = 3


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _acquire_aggregate_lock():
    """Acquire idempotent lock for aggregate job. Returns True if acquired."""
    try:
        if os.path.exists(AGGREGATE_LOCK_FILE):
            mtime = os.path.getmtime(AGGREGATE_LOCK_FILE)
            if time.time() - mtime < 900:
                _log('Aggregate lock held by another process')
                return False
            _log('Removing stale aggregate lock')
        with open(AGGREGATE_LOCK_FILE, 'w') as f:
            f.write(str(os.getpid()))
        return True
    except Exception as e:
        _log(f'Lock acquire error: {e}')
        return False


def _release_aggregate_lock():
    try:
        if os.path.exists(AGGREGATE_LOCK_FILE):
            os.remove(AGGREGATE_LOCK_FILE)
    except Exception:
        pass


def _run_aggregate_recent():
    """Run aggregate_candles.py --tf 5m for RECENT data only (no historical rebuild).
    Single attempt, short timeout. Returns True on success.
    """
    if not _acquire_aggregate_lock():
        return False
    try:
        cmd = [sys.executable, '/root/trading-bot/app/aggregate_candles.py', '--tf', '5m']
        _log(f'Auto-aggregate (recent only): {" ".join(cmd)}')
        try:
            proc = subprocess.run(
                cmd, capture_output=True, text=True,
                timeout=AGGREGATE_TIMEOUT_SEC)
            if proc.stdout:
                for line in proc.stdout.strip().split('\n')[-3:]:
                    _log(f'  {line}')
            if proc.returncode == 0:
                _log('Auto-aggregate succeeded')
                return True
            _log(f'Auto-aggregate failed (rc={proc.returncode})')
            return False
        except subprocess.TimeoutExpired:
            _log(f'Auto-aggregate timeout ({AGGREGATE_TIMEOUT_SEC}s)')
            return False
        except Exception as e:
            _log(f'Auto-aggregate error: {e}')
            return False
    finally:
        _release_aggregate_lock()


def _get_5m_coverage(conn):
    """Get 5m coverage stats. Returns (min_ts, max_ts, count)."""
    with conn.cursor() as cur:
        cur.execute("SELECT MIN(ts), MAX(ts), COUNT(*) FROM market_ohlcv WHERE tf='5m';")
        return cur.fetchone()


def _ensure_tz(ts):
    """Ensure timestamp is tz-aware (UTC)."""
    if ts and ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts


def main():
    parser = argparse.ArgumentParser(description='Prune old 1m candles')
    parser.add_argument('--resume', action='store_true')
    parser.add_argument('--dryrun', action='store_true',
                        help='Count rows to delete without actually deleting')
    args = parser.parse_args()

    conn = get_conn()
    conn.autocommit = False

    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '600000';")
    conn.commit()

    cutoff = datetime.now(timezone.utc) - timedelta(days=RETAIN_DAYS)
    _log(f'Retention cutoff: {cutoff.strftime("%Y-%m-%d %H:%M")} ({RETAIN_DAYS}d)')

    job_id = start_job(conn, JOB_NAME, metadata={'retain_days': RETAIN_DAYS,
                                                   'cutoff': cutoff.isoformat()})
    total_deleted = 0

    try:
        # Step 1: Get 5m coverage
        ohlcv_min, ohlcv_max, ohlcv_count = _get_5m_coverage(conn)

        # If no 5m data at all, try ONE quick aggregate (recent data only)
        if not ohlcv_count or ohlcv_count == 0:
            _log('No 5m data. Attempting single aggregate (recent)...')
            _run_aggregate_recent()
            ohlcv_min, ohlcv_max, ohlcv_count = _get_5m_coverage(conn)

        if not ohlcv_count or ohlcv_count == 0:
            _log('WARN: No 5m data after aggregate attempt.')
            _log('  Completing without deletions.')
            finish_job(conn, job_id, status='COMPLETED',
                       error='no_5m_data_no_prune')
            return

        ohlcv_min = _ensure_tz(ohlcv_min)
        ohlcv_max = _ensure_tz(ohlcv_max)
        _log(f'5m coverage: {ohlcv_min} ~ {ohlcv_max} ({ohlcv_count:,} rows)')

        # Step 2: Auto-adjust prune boundary
        # prune_start = max(5m_min, cutoff_minus_extra) — only prune where 5m exists
        # prune_end = cutoff (retain_days boundary)
        prune_start = ohlcv_min
        prune_end = cutoff

        if prune_start >= prune_end:
            _log(f'5m starts at {prune_start.strftime("%Y-%m-%d")}, '
                 f'after cutoff {prune_end.strftime("%Y-%m-%d")}. '
                 f'No 1m data to prune in covered zone.')
            finish_job(conn, job_id, status='COMPLETED',
                       error='5m_starts_after_cutoff')
            return

        # Count archival zone (before 5m coverage — never touch)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM candles
                WHERE tf='1m' AND ts < %s;
            """, (prune_start,))
            archive_count = cur.fetchone()[0]

        if archive_count > 0:
            _log(f'ARCHIVE_REQUIRED: {archive_count:,} 1m rows before 5m start '
                 f'({prune_start.strftime("%Y-%m-%d")}). '
                 f'Skipping — not deleting uncovered data.')

        # Count rows safe to delete (within 5m coverage AND before cutoff)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM candles
                WHERE tf='1m' AND ts >= %s AND ts < %s;
            """, (prune_start, prune_end))
            safe_to_delete = cur.fetchone()[0]

        if safe_to_delete == 0:
            _log('No 1m candles in safe prune zone')
            finish_job(conn, job_id, status='COMPLETED')
            return

        _log(f'Safe to delete: {safe_to_delete:,} rows '
             f'(range={prune_start.strftime("%Y-%m-%d")} ~ '
             f'{prune_end.strftime("%Y-%m-%d")})')
        _log(f'  archive_preserved={archive_count:,}')

        # Dryrun mode
        if args.dryrun:
            _log(f'DRYRUN: would delete {safe_to_delete:,} rows')
            finish_job(conn, job_id, status='COMPLETED')
            return

        # Step 3: Delete in batches
        total_to_delete = safe_to_delete
        batch_num = 0
        while True:
            if check_stop():
                _log('STOP signal received')
                update_progress(conn, job_id, {'deleted': total_deleted})
                finish_job(conn, job_id, status='PARTIAL',
                           error='stopped_by_user')
                return
            if not check_pause():
                _log('STOP during pause')
                update_progress(conn, job_id, {'deleted': total_deleted})
                finish_job(conn, job_id, status='PARTIAL',
                           error='stopped_during_pause')
                return

            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM candles
                    WHERE ctid IN (
                        SELECT ctid FROM candles
                        WHERE tf='1m' AND ts >= %s AND ts < %s
                        LIMIT %s
                    );
                """, (prune_start, prune_end, BATCH_SIZE))
                deleted = cur.rowcount

            conn.commit()
            batch_num += 1
            total_deleted += deleted

            if batch_num % 10 == 0 or deleted < BATCH_SIZE:
                _log(f'Batch {batch_num}: deleted {deleted}, '
                     f'total={total_deleted:,}/{total_to_delete:,}')
                update_progress(conn, job_id, {'deleted': total_deleted},
                                inserted=total_deleted)

            if deleted < BATCH_SIZE:
                break

        finish_job(conn, job_id, status='COMPLETED')
        _log(f'DONE: {total_deleted:,} rows deleted')

        # VACUUM ANALYZE
        if total_deleted > 0:
            try:
                try:
                    conn.commit()
                except Exception:
                    pass
                conn.autocommit = True
                with conn.cursor() as cur:
                    _log('Running VACUUM ANALYZE candles...')
                    cur.execute('VACUUM ANALYZE candles;')
                _log('VACUUM ANALYZE complete')
            except Exception as ve:
                _log(f'VACUUM ANALYZE failed (non-critical): {ve}')

    except KeyboardInterrupt:
        _log('Interrupted')
        update_progress(conn, job_id, {'deleted': total_deleted})
        finish_job(conn, job_id, status='PARTIAL', error='KeyboardInterrupt')
    except Exception as e:
        _log(f'FATAL: {e}')
        traceback.print_exc()
        conn.rollback()
        finish_job(conn, job_id, status='FAILED', error=str(e)[:500])
    finally:
        _release_aggregate_lock()
        conn.close()


if __name__ == '__main__':
    main()
