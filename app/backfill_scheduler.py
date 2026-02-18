"""
backfill_scheduler.py — Autonomous data pipeline scheduler (LIVE_DB rolling window).

Runs as a systemd service, independent of telegram bot.
Periodically triggers backfill/aggregate/prune jobs based on data gaps.

LIVE_DB rolling retention:
  - 1m candles: 30 days
  - 5m OHLCV:   6 months
  - news:       6 months
  - Historical gaps (> 3 months) → ARCHIVE_REQUIRED, NOT auto-backfilled.

Pipeline order:
  1. candles_1m (Bybit API → recent data only, NOT full historical)
  2. aggregate_5m (1m → 5m/15m/1h)
  3. news_path (news → 24h price path)
  4. prune_1m (old 1m cleanup, safe against 5m coverage)

Each step only runs if the previous step has sufficient data.
Jobs are run via subprocess, respecting the same gates as manual runs.

Usage:
    python backfill_scheduler.py          # one-shot: run all needed jobs
    python backfill_scheduler.py --loop   # continuous: check every INTERVAL
"""
import os
import sys
import time
import subprocess
import argparse
from datetime import datetime, timezone

sys.path.insert(0, '/root/trading-bot/app')
from db_config import get_conn
from backfill_utils import get_running_pid, is_backfill_enabled, check_trade_switch_off

LOG_PREFIX = '[backfill_scheduler]'
APP_DIR = '/root/trading-bot/app'
CHECK_INTERVAL_SEC = 3600  # 1 hour between checks
MIN_GAP_MINUTES = 60  # minimum gap to trigger backfill

# Rolling retention limits — LIVE_DB only holds recent data
MAX_1M_BACKFILL_DAYS = 30   # Never backfill 1m beyond 30 days
MAX_5M_RETENTION_DAYS = 180  # 6 months
MAX_NEWS_RETENTION_DAYS = 180  # 6 months


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _check_candles_gap(conn):
    """Check if 1m candles have a gap > MIN_GAP_MINUTES from last row to now.
    Only backfills within MAX_1M_BACKFILL_DAYS rolling window.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(ts) FROM candles WHERE tf='1m';")
            row = cur.fetchone()
            if not row or not row[0]:
                return True, 'no_data'
            max_ts = row[0]
            if max_ts.tzinfo is None:
                max_ts = max_ts.replace(tzinfo=timezone.utc)
            gap_min = (datetime.now(timezone.utc) - max_ts).total_seconds() / 60
            gap_days = gap_min / 1440

            # If gap exceeds rolling window, only backfill recent portion
            if gap_days > MAX_1M_BACKFILL_DAYS:
                _log(f'1m gap={gap_days:.1f}d exceeds {MAX_1M_BACKFILL_DAYS}d limit. '
                     f'ARCHIVE_REQUIRED for historical portion. '
                     f'Will only backfill recent {MAX_1M_BACKFILL_DAYS}d.')
                return True, f'gap={gap_days:.1f}d (capped to {MAX_1M_BACKFILL_DAYS}d)'

            if gap_min > MIN_GAP_MINUTES:
                return True, f'gap={gap_min:.0f}min'
            return False, f'current (gap={gap_min:.0f}min)'
    except Exception as e:
        return False, f'check_error: {e}'


def _check_aggregate_needed(conn):
    """Check if 5m aggregate is behind 1m data."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(ts) FROM candles WHERE tf='1m';")
            c1m = cur.fetchone()
            cur.execute("SELECT MAX(ts) FROM market_ohlcv WHERE tf='5m';")
            o5m = cur.fetchone()

            if not c1m or not c1m[0]:
                return False, 'no_1m_data'
            if not o5m or not o5m[0]:
                return True, 'no_5m_data'

            c1m_ts = c1m[0]
            o5m_ts = o5m[0]
            if c1m_ts.tzinfo is None:
                c1m_ts = c1m_ts.replace(tzinfo=timezone.utc)
            if o5m_ts.tzinfo is None:
                o5m_ts = o5m_ts.replace(tzinfo=timezone.utc)

            gap_min = (c1m_ts - o5m_ts).total_seconds() / 60
            if gap_min > 30:  # 5m is 30+ minutes behind 1m
                return True, f'5m_behind={gap_min:.0f}min'
            return False, f'5m_current (gap={gap_min:.0f}min)'
    except Exception as e:
        return False, f'check_error: {e}'


def _run_job(script, args=None, timeout=1800):
    """Run a backfill script as subprocess. Returns (success, output_tail)."""
    cmd = [sys.executable, f'{APP_DIR}/{script}']
    if args:
        cmd.extend(args)
    _log(f'Running: {" ".join(cmd)}')
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        tail = (result.stdout or '')[-500:]
        if result.returncode == 0:
            _log(f'  OK (rc=0)')
            return True, tail
        else:
            _log(f'  FAILED (rc={result.returncode})')
            if result.stderr:
                _log(f'  stderr: {result.stderr[-200:]}')
            return False, tail
    except subprocess.TimeoutExpired:
        _log(f'  TIMEOUT ({timeout}s)')
        return False, 'timeout'
    except Exception as e:
        _log(f'  ERROR: {e}')
        return False, str(e)


def run_pipeline():
    """Run the data pipeline once."""
    # Gate 1: backfill must be enabled
    if not is_backfill_enabled():
        _log('Skipping: backfill not enabled')
        return

    # Gate 2: trade_switch must be OFF
    if not check_trade_switch_off():
        _log('Skipping: trade_switch is ON')
        return

    # Gate 3: concurrency — check for active runner
    pid = get_running_pid()
    if pid:
        _log(f'Another backfill runner is active (PID={pid}). Skipping.')
        return

    conn = get_conn(autocommit=True)

    try:
        # Step 1: Check and run candles_1m backfill (recent only)
        needed, reason = _check_candles_gap(conn)
        if needed:
            _log(f'candles_1m: {reason} — running backfill (recent only)')
            ok, _ = _run_job('backfill_candles.py', ['--resume'], timeout=1800)
            if not ok:
                _log('candles_1m failed, skipping remaining steps')
                return
        else:
            _log(f'candles_1m: {reason} — skipping')

        # Step 2: Check and run aggregate_5m
        needed, reason = _check_aggregate_needed(conn)
        if needed:
            _log(f'aggregate_5m: {reason} — running')
            _run_job('aggregate_candles.py', ['--tf', '5m'], timeout=600)
        else:
            _log(f'aggregate_5m: {reason} — skipping')

        # Step 3: Run news_path backfill (always incremental)
        _log('news_path: running incremental')
        _run_job('backfill_news_path.py', ['--resume'], timeout=600)

        # Step 4: Prune old 1m candles (safe, respects 5m coverage)
        _log('prune_1m: running')
        _run_job('prune_candles_1m.py', timeout=600)

        _log('Pipeline cycle complete')

    except Exception as e:
        _log(f'Pipeline error: {e}')
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description='Backfill data pipeline scheduler')
    parser.add_argument('--loop', action='store_true', help='Run continuously')
    args = parser.parse_args()

    if not args.loop:
        _log('One-shot pipeline run')
        run_pipeline()
        return

    _log(f'Starting continuous scheduler (interval={CHECK_INTERVAL_SEC}s)')
    consecutive_errors = 0
    while True:
        try:
            run_pipeline()
            consecutive_errors = 0
        except Exception as e:
            consecutive_errors += 1
            _log(f'Scheduler error (#{consecutive_errors}): {e}')
        # Exponential backoff on consecutive errors (max 4x normal interval)
        sleep_sec = CHECK_INTERVAL_SEC * min(2 ** consecutive_errors, 4) if consecutive_errors else CHECK_INTERVAL_SEC
        _log(f'Sleeping {sleep_sec}s until next check...')
        time.sleep(sleep_sec)


if __name__ == '__main__':
    main()
