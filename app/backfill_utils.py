"""
backfill_utils.py — 공용 백필 Job 추적 헬퍼 모듈.

모든 백필 스크립트가 이 모듈을 import하여 일관된 작업 이력 추적.
backfill_job_runs 테이블에 시작/종료/커서/카운터 저장.
Bybit V5 REST API 직접 호출 헬퍼 포함.

Usage:
    from backfill_utils import start_job, get_last_cursor, update_progress, finish_job
    from backfill_utils import fetch_bybit_kline, normalize_symbol, TF_TO_INTERVAL

    job_id = start_job(conn, 'backfill_candles')
    cursor = get_last_cursor(conn, 'backfill_candles')  # resume support
    update_progress(conn, job_id, '{"since_ms":123}', inserted=100)
    finish_job(conn, job_id, status='COMPLETED')
"""
import json
import os
import sys
import time
import urllib.parse
import urllib.request

sys.path.insert(0, '/root/trading-bot/app')

LOG_PREFIX = '[backfill_utils]'

# ── Signal file paths ─────────────────────────────────────
PID_FILE = '/tmp/backfill_runner.pid'
PAUSE_FILE = '/tmp/backfill_pause'
STOP_FILE = '/tmp/backfill_stop'
ENABLE_FILE = '/tmp/backfill_enabled'
EXIT_STATUS_FILE = '/tmp/backfill_last_exit.json'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _safe_commit(conn):
    """Commit if not autocommit. Handles both modes safely."""
    if not conn.autocommit:
        conn.commit()


def start_job(conn, job_name, metadata=None):
    """Start a new job run. Returns job_id (int)."""
    meta = json.dumps(metadata or {})
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO backfill_job_runs (job_name, status, metadata)
            VALUES (%s, 'RUNNING', %s::jsonb)
            RETURNING id;
        """, (job_name, meta))
        job_id = cur.fetchone()[0]
    _safe_commit(conn)
    _log(f'started job {job_name} id={job_id}')
    return job_id


def get_last_cursor(conn, job_name):
    """Get last cursor from most recent PARTIAL/COMPLETED run. Returns dict or None."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT last_cursor FROM backfill_job_runs
            WHERE job_name = %s AND last_cursor IS NOT NULL
              AND status IN ('PARTIAL', 'COMPLETED', 'RUNNING')
            ORDER BY started_at DESC LIMIT 1;
        """, (job_name,))
        row = cur.fetchone()
    if row and row[0]:
        try:
            return json.loads(row[0])
        except (json.JSONDecodeError, TypeError):
            return None
    return None


def update_progress(conn, job_id, cursor, inserted=0, updated=0, failed=0):
    """Update progress: cursor + absolute counters. Safe for both autocommit modes.
    NOTE: inserted/updated/failed are ABSOLUTE totals (SET), not deltas.
    """
    cursor_str = json.dumps(cursor) if isinstance(cursor, dict) else str(cursor)
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE backfill_job_runs
            SET last_cursor = %s,
                inserted = %s,
                updated  = %s,
                failed   = %s,
                status   = 'PARTIAL'
            WHERE id = %s;
        """, (cursor_str, inserted, updated, failed, job_id))
    _safe_commit(conn)


def finish_job(conn, job_id, status='COMPLETED', error=None):
    """Mark job as finished (COMPLETED/FAILED)."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE backfill_job_runs
            SET finished_at = now(), status = %s, error = %s
            WHERE id = %s;
        """, (status, error, job_id))
    _safe_commit(conn)
    _log(f'job {job_id} finished with status={status}')


def get_job_stats(conn, job_id):
    """Return current job stats as dict."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT inserted, updated, failed, status, last_cursor
            FROM backfill_job_runs WHERE id = %s;
        """, (job_id,))
        row = cur.fetchone()
    if not row:
        return None
    return {
        'inserted': row[0],
        'updated': row[1],
        'failed': row[2],
        'status': row[3],
        'last_cursor': row[4],
    }


# ── PID file helpers ─────────────────────────────────────

def write_pid_file(pid=None):
    """Write PID file. Returns False if another process is alive."""
    existing = get_running_pid()
    if existing is not None:
        if pid and existing == pid:
            return True  # same process
        return False
    try:
        with open(PID_FILE, 'w') as f:
            f.write(str(pid or os.getpid()))
        return True
    except Exception as e:
        _log(f'write_pid_file error: {e}')
        return False


def remove_pid_file():
    """Remove PID file."""
    try:
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
    except Exception:
        pass


def get_running_pid():
    """Return PID if alive, else None. Cleans stale PID file."""
    try:
        if not os.path.exists(PID_FILE):
            return None
        with open(PID_FILE) as f:
            pid = int(f.read().strip())
        os.kill(pid, 0)  # check alive
        return pid
    except (ProcessLookupError, ValueError):
        # stale PID file
        remove_pid_file()
        return None
    except PermissionError:
        # process exists but we can't signal it
        try:
            with open(PID_FILE) as f:
                return int(f.read().strip())
        except Exception:
            return None
    except Exception:
        return None


# ── Signal helpers ────────────────────────────────────────

def check_stop():
    """Return True if STOP signal file exists."""
    return os.path.exists(STOP_FILE)


def check_pause():
    """Blocking check. If PAUSE_FILE exists, sleep until resume or stop.
    Returns True to continue, False if stop signal received during pause."""
    if not os.path.exists(PAUSE_FILE):
        return True
    _log('PAUSE signal detected, waiting for resume or stop...')
    while os.path.exists(PAUSE_FILE):
        if os.path.exists(STOP_FILE):
            _log('STOP signal during pause')
            return False
        time.sleep(5)
    _log('RESUME — continuing')
    return True


def signal_pause():
    """Create pause signal file."""
    with open(PAUSE_FILE, 'w') as f:
        f.write(str(int(time.time())))


def signal_resume():
    """Remove pause signal file."""
    try:
        if os.path.exists(PAUSE_FILE):
            os.remove(PAUSE_FILE)
    except Exception:
        pass


def signal_stop():
    """Create stop signal file."""
    with open(STOP_FILE, 'w') as f:
        f.write(str(int(time.time())))


def clear_signals():
    """Remove all signal files."""
    for f in (PAUSE_FILE, STOP_FILE):
        try:
            if os.path.exists(f):
                os.remove(f)
        except Exception:
            pass


# ── Trade switch safety ──────────────────────────────────

def check_trade_switch_off():
    """Return True if trade_switch is OFF (safe for backfill).
    Returns False if ON or query fails."""
    try:
        from db_config import get_conn
        conn = get_conn(autocommit=True)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    'SELECT enabled FROM trade_switch ORDER BY id DESC LIMIT 1;')
                row = cur.fetchone()
            if row is None:
                return True  # no record = no trading
            return not row[0]  # True if enabled=False
        finally:
            conn.close()
    except Exception as e:
        _log(f'check_trade_switch_off error: {e}')
        return False  # fail-closed


# ── Backfill enable gate ─────────────────────────────────

def is_backfill_enabled():
    """Return True if backfill execution is enabled (gate file exists)."""
    return os.path.exists(ENABLE_FILE)


def set_backfill_enabled(enabled):
    """Enable or disable backfill execution."""
    if enabled:
        with open(ENABLE_FILE, 'w') as f:
            f.write(json.dumps({
                'enabled_at': time.strftime('%Y-%m-%d %H:%M:%S'),
                'pid': os.getpid(),
            }))
        _log('Backfill ENABLED')
    else:
        try:
            if os.path.exists(ENABLE_FILE):
                os.remove(ENABLE_FILE)
        except Exception:
            pass
        _log('Backfill DISABLED')


# ── Exit status tracking ─────────────────────────────────

def write_exit_status(job_key, status, reason, pid=None):
    """Write last exit status to file for status display."""
    try:
        data = {
            'job_key': job_key,
            'status': status,
            'reason': reason,
            'pid': pid or os.getpid(),
            'ts': time.strftime('%Y-%m-%d %H:%M:%S'),
        }
        with open(EXIT_STATUS_FILE, 'w') as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception:
        pass


def read_exit_status():
    """Read last exit status. Returns dict or None."""
    try:
        if not os.path.exists(EXIT_STATUS_FILE):
            return None
        with open(EXIT_STATUS_FILE) as f:
            return json.load(f)
    except Exception:
        return None


# ── Bybit V5 REST API helpers ────────────────────────────

BYBIT_BASE_URL = 'https://api.bybit.com'

# ccxt-style symbol → Bybit API symbol
def normalize_symbol(symbol):
    """Convert internal symbol format to Bybit API format.
    'BTC/USDT:USDT' -> 'BTCUSDT'
    'ETH/USDT:USDT' -> 'ETHUSDT'
    'BTCUSDT'       -> 'BTCUSDT' (passthrough)
    """
    s = symbol.split(':')[0]   # 'BTC/USDT:USDT' -> 'BTC/USDT'
    s = s.replace('/', '')     # 'BTC/USDT' -> 'BTCUSDT'
    return s


# tf string → Bybit V5 interval value
TF_TO_INTERVAL = {
    '1m': '1',
    '3m': '3',
    '5m': '5',
    '15m': '15',
    '30m': '30',
    '1h': '60',
    '2h': '120',
    '4h': '240',
    '6h': '360',
    '12h': '720',
    '1d': 'D',
    '1w': 'W',
    '1M': 'M',
}


def fetch_bybit_kline(symbol, tf, start_ms, limit=200, category='linear', log_fn=None):
    """Fetch kline (OHLCV) from Bybit V5 REST API directly.

    Args:
        symbol:   Internal format 'BTC/USDT:USDT' or Bybit format 'BTCUSDT'
        tf:       Timeframe string '1m', '5m', '1h', etc.
        start_ms: Start time in epoch milliseconds
        limit:    Number of bars (max 1000)
        category: 'linear' (USDT perp), 'inverse', 'spot'
        log_fn:   Optional logging function

    Returns:
        list of [ts_ms, open, high, low, close, volume] sorted ascending by ts.
        Each value is float except ts_ms (int).
        Returns empty list on error.

    Raises:
        Exception on HTTP/network errors (caller should handle).
    """
    _logf = log_fn or _log
    api_symbol = normalize_symbol(symbol)
    interval = TF_TO_INTERVAL.get(tf)
    if not interval:
        raise ValueError(f'Unknown timeframe: {tf} (valid: {list(TF_TO_INTERVAL.keys())})')

    params = {
        'category': category,
        'symbol': api_symbol,
        'interval': interval,
        'start': str(int(start_ms)),
        'limit': str(min(int(limit), 1000)),
    }

    url = f'{BYBIT_BASE_URL}/v5/market/kline?' + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={'User-Agent': 'backfill-bot/1.0'})
    resp = urllib.request.urlopen(req, timeout=20)
    body = json.loads(resp.read().decode())

    ret_code = body.get('retCode', -1)
    ret_msg = body.get('retMsg', '')
    result_list = body.get('result', {}).get('list', [])

    _logf(f'API resp: retCode={ret_code} retMsg={ret_msg} '
          f'bars={len(result_list)} '
          f'symbol={api_symbol} interval={interval} start={start_ms}')

    if ret_code != 0:
        _logf(f'API ERROR: retCode={ret_code} retMsg={ret_msg} '
              f'params={params}')
        return []

    if not result_list:
        _logf(f'API EMPTY: symbol={api_symbol} interval={interval} '
              f'start_ms={start_ms} start_dt={_ms_to_str(start_ms)} '
              f'limit={limit}')
        return []

    # Bybit returns rows in reverse chronological order: newest first.
    # Each row: [startTime, open, high, low, close, volume, turnover]
    # Convert to ascending order with numeric types.
    bars = []
    for row in result_list:
        ts_ms = int(row[0])
        o = float(row[1])
        h = float(row[2])
        l = float(row[3])
        c = float(row[4])
        v = float(row[5])
        bars.append([ts_ms, o, h, l, c, v])

    bars.sort(key=lambda x: x[0])  # ascending by timestamp
    return bars


def smoke_test_bybit(symbol, tf, start_ms, log_fn=None):
    """Run a quick smoke test: fetch 3 bars and validate timestamps.

    Returns (ok: bool, message: str).
    """
    _logf = log_fn or _log
    from datetime import datetime, timezone

    try:
        bars = fetch_bybit_kline(symbol, tf, start_ms, limit=3, log_fn=log_fn)
    except Exception as e:
        return False, f'HTTP error: {e}'

    if not bars:
        return False, (f'Empty response: symbol={normalize_symbol(symbol)} '
                       f'interval={TF_TO_INTERVAL.get(tf, "?")} '
                       f'start={start_ms} ({_ms_to_str(start_ms)})')

    first_ts = bars[0][0]
    first_dt = datetime.fromtimestamp(first_ts / 1000, tz=timezone.utc)
    req_dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
    drift_days = abs(first_ts - start_ms) / 86400_000

    if drift_days > 30:
        return False, (f'Drift too large: got {first_dt.strftime("%Y-%m-%d")} '
                       f'but requested {req_dt.strftime("%Y-%m-%d")} '
                       f'(drift={drift_days:.0f} days)')

    return True, (f'{len(bars)} bars, first={first_dt.strftime("%Y-%m-%d %H:%M")}, '
                  f'drift={drift_days:.1f}d')


def _ms_to_str(ms):
    """Convert epoch ms to human-readable UTC string."""
    from datetime import datetime, timezone
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')
    except Exception:
        return str(ms)
