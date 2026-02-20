"""
strategy.common.dedupe — Signal deduplication.

Signal key format: {symbol}:{mode}:{side}:{stage_bucket}:{ts_bucket_3m}
Uses DB table `signal_dedup_log` for persistence.
"""

import time

LOG_PREFIX = '[strategy.dedupe]'

DEFAULT_WINDOW_SEC = 180  # 3 minutes


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def make_signal_key(symbol, mode, side, stage=1):
    """Generate signal dedup key.

    Format: {symbol}:{mode}:{side}:{stage_bucket}:{ts_bucket_3m}
    ts_bucket_3m groups timestamps into 3-minute windows.
    """
    ts_bucket = int(time.time()) // DEFAULT_WINDOW_SEC
    stage_bucket = stage if stage else 1
    return f'{symbol}:{mode}:{side}:{stage_bucket}:{ts_bucket}'


def is_duplicate(cur, signal_key, window_sec=DEFAULT_WINDOW_SEC):
    """Check if this signal key was seen within the window.

    Returns True if duplicate (should be suppressed).
    FAIL-OPEN: returns False on any error.
    """
    try:
        cur.execute("""
            SELECT 1 FROM signal_dedup_log
            WHERE key = %s
              AND ts >= now() - make_interval(secs => %s)
              AND expired = false
            LIMIT 1
        """, (signal_key, window_sec))
        return cur.fetchone() is not None
    except Exception as e:
        _log(f'is_duplicate FAIL-OPEN: {e}')
        return False


def record_signal(cur, signal_key):
    """Record a signal emission for dedup tracking.

    Uses INSERT ON CONFLICT to update timestamp if key exists.
    FAIL-OPEN: silently ignores errors.
    """
    try:
        cur.execute("""
            INSERT INTO signal_dedup_log (key, ts, expired)
            VALUES (%s, now(), false)
            ON CONFLICT (key) DO UPDATE
                SET ts = now(), expired = false
        """, (signal_key,))
    except Exception as e:
        _log(f'record_signal FAIL-OPEN: {e}')


def cleanup_expired(cur, max_age_sec=600):
    """Mark old entries as expired. Called periodically.

    FAIL-OPEN: silently ignores errors.
    """
    try:
        cur.execute("""
            UPDATE signal_dedup_log
            SET expired = true
            WHERE ts < now() - make_interval(secs => %s)
              AND expired = false
        """, (max_age_sec,))
    except Exception as e:
        _log(f'cleanup_expired FAIL-OPEN: {e}')
