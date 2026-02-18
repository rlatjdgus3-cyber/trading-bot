"""
backfill_runner.py — Background wrapper for backfill script execution.

Launched via subprocess.Popen from local_query_executor.
Responsibilities:
  1. Check backfill_enabled gate (explicit admin approval)
  2. Double-check trade_switch OFF (race protection)
  3. Write PID file (concurrency=1)
  4. Clear stale signal files
  5. Launch actual backfill script via subprocess.Popen
  6. Poll backfill_job_runs every 30s, send Telegram progress every 5min
  7. On child exit: write exit status, send notification, cleanup

Usage (called by local_query_executor, not directly):
    python3 backfill_runner.py <job_key> [--from YYYY-MM-DD] [--to YYYY-MM-DD]
"""
import os
import sys
import time
import json
import argparse
import subprocess
import traceback
import urllib.parse
import urllib.request

sys.path.insert(0, '/root/trading-bot/app')
from backfill_utils import (
    write_pid_file, remove_pid_file, clear_signals,
    check_trade_switch_off, is_backfill_enabled,
    write_exit_status,
)

LOG_PREFIX = '[backfill_runner]'
APP_DIR = '/root/trading-bot/app'
PROGRESS_INTERVAL_SEC = 300  # 5 min
POLL_INTERVAL_SEC = 30

# Job mapping: job_key -> script filename
JOB_MAP = {
    'candles_1m':    'backfill_candles.py',
    'ohlcv_5m':      'backfill_ohlcv.py',
    'aggregate_5m':  'aggregate_candles.py',
    'price_events':  'build_price_events.py',
    'macro_trace':   'backfill_macro_trace.py',
    'news_classify': 'backfill_news_classification_and_reaction.py',
    'news_reaction': 'backfill_news_classification_and_reaction.py',
    'link_events':   'link_event_to_news.py',
    'news_path':     'backfill_news_path.py',
    'prune_1m':      'prune_candles_1m.py',
    'archive':       'backfill_archive.py',
}

# Aliases: common alternative names → canonical job_key
JOB_ALIASES = {
    'prune_candles_1m': 'prune_1m',
    'backfill_candles_1m': 'candles_1m',
    'backfill_ohlcv_5m': 'ohlcv_5m',
    'backfill_news_path': 'news_path',
    'aggregate_candles': 'aggregate_5m',
}

JOB_EXTRA_ARGS = {
    'aggregate_5m':  lambda args: ['--tf', args['tf']] if args.get('tf') else ['--tf', '5m'],
    'macro_trace':   lambda args: ['--resume'],
    'news_classify': lambda args: ['--classify-only', '--resume'],
    'news_reaction': lambda args: ['--reaction-only', '--resume'],
    'link_events':   lambda args: [],
    'news_path':     lambda args: ['--resume'],
    'prune_1m':      lambda args: ['--resume'],
}


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _load_tg_env():
    """Load Telegram credentials from env file."""
    env_path = f'{APP_DIR}/telegram_cmd.env'
    token, chat_id = '', ''
    try:
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if '=' in line and not line.startswith('#'):
                    k, v = line.split('=', 1)
                    k, v = k.strip(), v.strip()
                    if k == 'TELEGRAM_BOT_TOKEN':
                        token = v
                    elif k == 'TELEGRAM_ALLOWED_CHAT_ID':
                        chat_id = v
    except Exception:
        pass
    return token, chat_id


def _notify_telegram(text):
    """Send Telegram notification."""
    token, chat_id = _load_tg_env()
    if not token or not chat_id:
        return
    try:
        url = f'https://api.telegram.org/bot{token}/sendMessage'
        data = urllib.parse.urlencode({
            'chat_id': chat_id,
            'text': text,
            'disable_web_page_preview': 'true',
        }).encode('utf-8')
        req = urllib.request.Request(url, data=data, method='POST')
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        _log(f'Telegram notify error: {e}')


def _is_valid_date(s):
    """Check if string is a valid YYYY-MM-DD date."""
    if not s:
        return False
    try:
        from datetime import datetime
        datetime.strptime(s, '%Y-%m-%d')
        return True
    except ValueError:
        return False


def _build_script_args(job_key, from_date=None, to_date=None, extra=None):
    """Build command-line args for the backfill script."""
    script = JOB_MAP[job_key]
    cmd = ['python3', f'{APP_DIR}/{script}']

    # Job-specific extra args
    extra_fn = JOB_EXTRA_ARGS.get(job_key)
    if extra_fn:
        cmd.extend(extra_fn(extra or {}))
    else:
        # Date-based scripts: --start/--end or --resume
        if from_date and _is_valid_date(from_date):
            cmd.extend(['--start', from_date])
        else:
            cmd.append('--resume')
        if to_date and _is_valid_date(to_date):
            cmd.extend(['--end', to_date])
        # Ignore invalid to_date (e.g. 'now') — scripts default to now

    return cmd


def _get_latest_job_progress(job_key):
    """Query latest RUNNING or recently finished job stats for this job."""
    try:
        from db_config import get_conn
        conn = get_conn(autocommit=True)
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, job_name, status, inserted, failed,
                           started_at, last_cursor, error
                    FROM backfill_job_runs
                    WHERE job_name = %s
                    ORDER BY started_at DESC LIMIT 1;
                """, (job_key,))
                row = cur.fetchone()
            if not row:
                return None
            return {
                'id': row[0], 'job_name': row[1], 'status': row[2],
                'inserted': row[3] or 0, 'failed': row[4] or 0,
                'started_at': row[5], 'last_cursor': row[6],
                'error': row[7],
            }
        finally:
            conn.close()
    except Exception as e:
        _log(f'progress query error: {e}')
        return None


def _smoke_test_fetch(job_key, from_date=None):
    """Smoke test: fetch 3 bars from Bybit V5 API directly.

    Two tests:
      1) Historical fetch (from_date or 7d ago) — validates start param works
      2) Recent fetch (7d ago) — validates API is reachable

    Returns (ok: bool, message: str).
    """
    from backfill_utils import smoke_test_bybit
    from datetime import datetime, timezone

    symbol = 'BTC/USDT:USDT'
    tf = '1m' if job_key == 'candles_1m' else '5m'

    # Test 1: Historical data (the actual backfill target)
    if from_date and _is_valid_date(from_date):
        hist_ms = int(datetime.strptime(from_date, '%Y-%m-%d')
                      .replace(tzinfo=timezone.utc).timestamp() * 1000)
    else:
        hist_ms = int((datetime.now(timezone.utc).timestamp() - 7 * 86400) * 1000)

    _log(f'Smoke test 1/2: historical fetch from {datetime.fromtimestamp(hist_ms/1000, tz=timezone.utc).strftime("%Y-%m-%d")}')
    ok1, msg1 = smoke_test_bybit(symbol, tf, hist_ms, log_fn=_log)
    _log(f'  result: {"OK" if ok1 else "FAIL"} — {msg1}')

    if not ok1:
        # Test 2: Recent data to distinguish "API down" from "param error"
        recent_ms = int((datetime.now(timezone.utc).timestamp() - 7 * 86400) * 1000)
        _log(f'Smoke test 2/2: recent fetch (last 7d) to check API reachability')
        ok2, msg2 = smoke_test_bybit(symbol, tf, recent_ms, log_fn=_log)
        _log(f'  result: {"OK" if ok2 else "FAIL"} — {msg2}')

        if ok2:
            return False, f'Historical fetch failed but recent works — param issue: {msg1}'
        else:
            return False, f'Both fetches failed — API may be down: {msg1}'

    return True, msg1


def _exit_with_reason(job_key, status, reason, rc=1):
    """Write exit status, log, notify, and exit."""
    _log(f'{status}: {reason}')
    write_exit_status(job_key, status, reason)
    _notify_telegram(f'[Backfill] {job_key} {status}: {reason}')
    remove_pid_file()
    sys.exit(rc)


def main():
    parser = argparse.ArgumentParser(description='Backfill runner wrapper')
    parser.add_argument('job_key', help='Job key from JOB_MAP')
    parser.add_argument('--from', dest='from_date', default=None)
    parser.add_argument('--to', dest='to_date', default=None)
    parser.add_argument('--tf', default=None, help='Timeframe for aggregate')
    args = parser.parse_args()

    job_key = args.job_key
    # Resolve aliases
    if job_key in JOB_ALIASES:
        canonical = JOB_ALIASES[job_key]
        _log(f'Alias resolved: {job_key} → {canonical}')
        job_key = canonical
    if job_key not in JOB_MAP:
        _log(f'Unknown job_key: {job_key}')
        write_exit_status(job_key, 'REJECTED', f'Unknown job_key: {job_key}')
        sys.exit(1)

    # Gate 1: backfill must be explicitly enabled
    if not is_backfill_enabled():
        write_exit_status(job_key, 'DENIED', 'backfill not enabled (run /debug backfill_enable on)')
        _log('DENIED: backfill not enabled')
        sys.exit(1)

    # Gate 2: trade_switch must be OFF
    if not check_trade_switch_off():
        write_exit_status(job_key, 'DENIED', 'trade_switch is ON')
        _log('DENIED: trade_switch is ON')
        sys.exit(1)

    # Gate 3: concurrency — write PID file
    if not write_pid_file(os.getpid()):
        write_exit_status(job_key, 'DENIED', 'Another backfill runner already running')
        _log('DENIED: another runner alive')
        sys.exit(1)

    # Clear stale signals
    clear_signals()

    # Validate dates
    if args.to_date and not _is_valid_date(args.to_date):
        _log(f'WARNING: invalid --to date "{args.to_date}", ignoring (will use now)')

    # Build command
    extra = {'tf': args.tf} if args.tf else {}
    cmd = _build_script_args(job_key, args.from_date, args.to_date, extra)
    _log(f'Launching: {" ".join(cmd)}')

    # Smoke test for API-fetching jobs: verify ccxt can fetch historical data
    if job_key in ('candles_1m', 'ohlcv_5m'):
        _log('Running smoke test: fetch 1 batch from API...')
        try:
            smoke_ok, smoke_msg = _smoke_test_fetch(job_key, args.from_date)
            if not smoke_ok:
                _exit_with_reason(job_key, 'SMOKE_FAIL', smoke_msg)
            _log(f'Smoke test OK: {smoke_msg}')
        except Exception as e:
            _exit_with_reason(job_key, 'SMOKE_FAIL', f'exception: {e}')

    # Notify start
    _notify_telegram(
        f'[Backfill] STARTED: {job_key}\n'
        f'cmd: {" ".join(cmd[1:])}\n'
        f'PID: {os.getpid()}'
    )

    # Launch child process (separate log file from runner)
    child_log_path = f'/tmp/backfill_{job_key}_child.log'
    child_log_file = open(child_log_path, 'w')
    child = subprocess.Popen(
        cmd,
        stdout=child_log_file,
        stderr=subprocess.STDOUT,
        cwd=APP_DIR,
    )

    _log(f'Child PID={child.pid}, child_log={child_log_path}')

    # Monitor loop
    last_notify_ts = time.time()
    try:
        while child.poll() is None:
            time.sleep(POLL_INTERVAL_SEC)

            now = time.time()
            if now - last_notify_ts >= PROGRESS_INTERVAL_SEC:
                last_notify_ts = now
                progress = _get_latest_job_progress(job_key)
                if progress:
                    elapsed = ''
                    if progress['started_at']:
                        elapsed_sec = now - progress['started_at'].timestamp()
                        elapsed = f' elapsed={elapsed_sec / 60:.0f}min'
                    msg = (
                        f'[Backfill] {job_key} progress\n'
                        f'  status={progress["status"]}\n'
                        f'  inserted={progress["inserted"]:,} failed={progress["failed"]}'
                        f'{elapsed}'
                    )
                    if progress.get('last_cursor'):
                        cursor_str = str(progress['last_cursor'])[:80]
                        msg += f'\n  cursor={cursor_str}'
                    _notify_telegram(msg)

        # Child exited
        rc = child.returncode
        _log(f'Child exited with rc={rc}')

        # Read child log tail for error context
        child_log_tail = ''
        try:
            child_log_file.close()
            with open(child_log_path) as f:
                lines = f.readlines()
                child_log_tail = ''.join(lines[-10:]).strip()
        except Exception:
            pass

        # Final status from DB
        progress = _get_latest_job_progress(job_key)
        status_str = progress['status'] if progress else 'NO_DB_RECORD'
        inserted = progress['inserted'] if progress else 0
        failed = progress['failed'] if progress else 0
        db_error = progress.get('error', '') if progress else ''

        if rc == 0:
            exit_status = 'COMPLETED'
            exit_reason = f'db_status={status_str} inserted={inserted}'
        else:
            exit_status = f'CHILD_EXIT(rc={rc})'
            exit_reason = f'db_status={status_str}'
            if db_error:
                exit_reason += f' error={str(db_error)[:200]}'
            elif child_log_tail:
                exit_reason += f' log_tail={child_log_tail[:300]}'

        write_exit_status(job_key, exit_status, exit_reason, pid=os.getpid())

        msg = (
            f'[Backfill] {job_key} FINISHED\n'
            f'  result={exit_status}\n'
            f'  inserted={inserted:,} failed={failed}'
        )
        if db_error:
            msg += f'\n  error={str(db_error)[:200]}'
        _notify_telegram(msg)

    except KeyboardInterrupt:
        _log('Runner interrupted, terminating child...')
        child.terminate()
        try:
            child.wait(timeout=30)
        except subprocess.TimeoutExpired:
            _log('Child did not exit in 30s, killing...')
            child.kill()
            child.wait(timeout=10)
        write_exit_status(job_key, 'INTERRUPTED', 'KeyboardInterrupt')
        _notify_telegram(f'[Backfill] {job_key} interrupted by runner')
    except Exception as e:
        _log(f'Runner error: {e}')
        traceback.print_exc()
        try:
            child.terminate()
            try:
                child.wait(timeout=30)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=10)
        except Exception:
            pass
        write_exit_status(job_key, 'RUNNER_ERROR', str(e)[:500])
        _notify_telegram(f'[Backfill] {job_key} runner error: {e}')
    finally:
        try:
            child_log_file.close()
        except Exception:
            pass
        remove_pid_file()
        clear_signals()
        _log('Cleanup done')


if __name__ == '__main__':
    main()
