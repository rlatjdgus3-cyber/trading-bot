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

    Format: {symbol}:{mode}:{side}:{stage_bucket}
    Dedup time window is handled by DB query in is_duplicate(), not by key.
    """
    stage_bucket = stage if stage else 1
    return f'{symbol}:{mode}:{side}:{stage_bucket}'


def make_v3_signal_key(symbol, regime_class, side, level_bucket):
    """V3 extended debounce key: side+regime+level_bucket.

    Args:
        symbol: e.g. 'BTC/USDT:USDT'
        regime_class: 'STATIC_RANGE' | 'DRIFT_UP' | 'DRIFT_DOWN' | 'BREAKOUT'
        side: 'LONG' | 'SHORT'
        level_bucket: 'VAL' | 'POC' | 'VAH' | 'MID' | 'BREAKOUT_UP' | 'BREAKOUT_DOWN'

    Returns:
        str — dedup key for V3 signal debounce
    """
    return f'{symbol}:v3:{regime_class}:{side}:{level_bucket}'


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


def cleanup_expired(cur, max_age_sec=600, delete_after_sec=3600):
    """Mark old entries as expired, then DELETE very old rows.

    FAIL-OPEN: silently ignores errors.
    """
    try:
        cur.execute("""
            UPDATE signal_dedup_log
            SET expired = true
            WHERE ts < now() - make_interval(secs => %s)
              AND expired = false
        """, (max_age_sec,))
        # Hard-delete rows older than delete_after_sec to prevent table bloat
        cur.execute("""
            DELETE FROM signal_dedup_log
            WHERE expired = true
              AND ts < now() - make_interval(secs => %s)
        """, (delete_after_sec,))
    except Exception as e:
        _log(f'cleanup_expired FAIL-OPEN: {e}')
