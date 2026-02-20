"""
order_throttle.py — App-level order rate limiting & cooldown guard.

Prevents the bot from hitting Bybit's hourly trade limit (15/15) by enforcing
internal rate limits, per-action cooldowns, and rejection-based backoff.

Leaf module: does NOT import live_order_executor or position_manager.
Thread-safe via threading.Lock (position_manager uses threads).
"""
import time
import threading
import json
from datetime import datetime, timezone

LOG_PREFIX = '[order_throttle]'

DAILY_TRADE_LIMIT = 60
KST_UTC_OFFSET_SEC = 9 * 3600      # KST = UTC+9

# ── Constants ────────────────────────────────────────────
MAX_ATTEMPTS_PER_HOUR = 12          # Bybit 15 - 3 safety margin
MAX_ATTEMPTS_PER_10MIN = 4
COOLDOWN_AFTER_ANY_ORDER_SEC = 20
COOLDOWN_AFTER_ADD_SEC = 45
COOLDOWN_AFTER_REDUCE_SEC = 30
COOLDOWN_AFTER_REVERSE_SEC = 90
EVENT_DEBOUNCE_SEC = 120
RATE_LIMIT_LOCKOUT_SEC = 3600       # 15/15 hit -> 1 hour lockout
THROTTLE_BACKOFF_STAGES = [60, 120, 300, 600]  # 1m→2m→5m→10m exponential backoff
DB_ERROR_BACKOFF_SEC = 300
DB_ERROR_MAX_CONSECUTIVE = 3        # 3 consecutive -> trade OFF
NETWORK_BACKOFF_BASE = 5            # 5->10->20->40->300 max
NETWORK_BACKOFF_MAX = 300
ENTERING_TTL_SEC = 180
SIGNAL_DEDUP_SEC = 60              # same direction+action_type blocked for 60s
REJECTION_COOLDOWN_SEC = 900       # 15min cooldown after rejection (was 3min)

EXIT_ACTIONS = frozenset({'CLOSE', 'REDUCE', 'REVERSE_CLOSE', 'FULL_CLOSE'})

ACTION_COOLDOWNS = {
    'OPEN': COOLDOWN_AFTER_ANY_ORDER_SEC,
    'ADD': COOLDOWN_AFTER_ADD_SEC,
    'REDUCE': COOLDOWN_AFTER_REDUCE_SEC,
    'REVERSE_OPEN': COOLDOWN_AFTER_REVERSE_SEC,
    'REVERSE_CLOSE': 0,  # exit: no cooldown
    'CLOSE': 0,
    'FULL_CLOSE': 0,
}

# ── In-memory state ──────────────────────────────────────
_state = {
    'last_order_ts': 0.0,
    'last_action_ts': {},           # {action_type: float_ts}
    'entry_lock_until': 0.0,
    'entry_lock_reason': '',
    'network_consecutive': 0,
    'db_error_consecutive': 0,
    'last_reject_reason': '',
    'last_reject_ts': 0.0,
    'recent_attempts': [],          # list of float timestamps (sliding 1hr, ENTRY only)
    'last_signal_key': '',          # 'LONG:OPEN' format
    'last_signal_ts': 0.0,
    'throttle_backoff_stage': 0,    # exponential backoff stage index
}
_loaded = False
_lock = threading.Lock()


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


# ── DB load on startup ───────────────────────────────────

def _ensure_loaded(cur):
    """Load recent 1-hour attempts from DB to initialize in-memory sliding window.
    Safe to call repeatedly (idempotent after first load).
    """
    global _loaded
    # FIX #8: check _loaded under lock to prevent double-load race
    with _lock:
        if _loaded:
            return
    try:
        cur.execute("""
            SELECT extract(epoch FROM ts)
            FROM order_attempt_log
            WHERE symbol = 'BTC/USDT:USDT'
              AND action_type NOT IN ('CLOSE', 'REDUCE', 'REVERSE_CLOSE', 'FULL_CLOSE')
              AND ts >= now() - interval '1 hour'
            ORDER BY ts;
        """)
        rows = cur.fetchall()
        with _lock:
            _state['recent_attempts'] = [float(r[0]) for r in rows]
            _loaded = True
        _log(f'loaded {len(rows)} recent attempts from DB')
    except Exception as e:
        _log(f'_ensure_loaded error (non-fatal): {e}')
        # FIX #3: set _loaded under lock even on exception path
        with _lock:
            _loaded = True


# ── Sliding window helpers ───────────────────────────────

def _prune_old_attempts(now=None):
    """Remove attempts older than 1 hour from in-memory list.
    Must be called while _lock is held.
    """
    if now is None:
        now = time.time()
    cutoff = now - 3600
    _state['recent_attempts'] = [
        ts for ts in _state['recent_attempts'] if ts > cutoff
    ]


def _count_recent(window_sec, now=None):
    """Count attempts within the last window_sec seconds.
    Must be called while _lock is held.
    """
    if now is None:
        now = time.time()
    cutoff = now - window_sec
    return sum(1 for ts in _state['recent_attempts'] if ts > cutoff)


# ── Public API ───────────────────────────────────────────

def record_attempt(cur, action_type, direction, outcome,
                   reject_reason=None, error_code=None, detail=None):
    """Record every order attempt to DB + in-memory.
    Called after every place_open_order / place_close_order attempt.
    """
    now = time.time()

    # DB insert (autocommit-safe: no SAVEPOINT needed)
    try:
        cur.execute("""
            INSERT INTO order_attempt_log
                (symbol, action_type, direction, outcome,
                 reject_reason, error_code, source, detail)
            VALUES ('BTC/USDT:USDT', %s, %s, %s, %s, %s, 'executor', %s::jsonb);
        """, (
            action_type, direction, outcome,
            reject_reason, error_code,
            json.dumps(detail or {}, default=str),
        ))
    except Exception as e:
        _log(f'record_attempt DB error: {e}')

    # In-memory update — FIX #6: only count non-EXIT actions against rate limit
    with _lock:
        _state['last_order_ts'] = now
        _state['last_action_ts'][action_type] = now
        if action_type not in EXIT_ACTIONS:
            _state['recent_attempts'].append(now)
        # Only set signal dedup key on SUCCESS (DRY_RUN/ERROR should not block next real attempt)
        if outcome == 'SUCCESS':
            _state['last_signal_key'] = f'{direction}:{action_type}'
            _state['last_signal_ts'] = now
        _prune_old_attempts(now)


def check_signal_dedup(action_type, direction):
    """Block identical signal within SIGNAL_DEDUP_SEC.
    Also block any entry for REJECTION_COOLDOWN_SEC after last rejection."""
    with _lock:
        now = time.time()
        # 1) Rejection cooldown
        if _state['last_reject_ts'] > 0:
            elapsed = now - _state['last_reject_ts']
            if elapsed < REJECTION_COOLDOWN_SEC:
                return (False, f'rejection_cooldown ({REJECTION_COOLDOWN_SEC - elapsed:.0f}s remaining)')
        # 2) Same signal dedup
        key = f'{direction}:{action_type}'
        if key == _state['last_signal_key']:
            elapsed = now - _state['last_signal_ts']
            if elapsed < SIGNAL_DEDUP_SEC:
                return (False, f'signal_dedup ({key}, {SIGNAL_DEDUP_SEC - elapsed:.0f}s remaining)')
    return (True, '')


def check_rate_limit(cur=None, symbol='BTC/USDT:USDT'):
    """Check 12/hr and 4/10min rate limits.
    Returns (ok, reason, next_allowed_ts).
    """
    # FIX #1 + #10: snapshot list and compute everything inside _lock with consistent 'now'
    with _lock:
        now = time.time()
        _prune_old_attempts(now)
        hourly = _count_recent(3600, now)
        ten_min = _count_recent(600, now)
        attempts_snapshot = list(_state['recent_attempts'])

    if hourly >= MAX_ATTEMPTS_PER_HOUR:
        cutoff = now - 3600
        oldest_in_window = min(
            (ts for ts in attempts_snapshot if ts > cutoff),
            default=now)
        next_ts = oldest_in_window + 3600
        return (False,
                f'hourly_limit ({hourly}/{MAX_ATTEMPTS_PER_HOUR})',
                next_ts)

    if ten_min >= MAX_ATTEMPTS_PER_10MIN:
        cutoff = now - 600
        oldest_in_window = min(
            (ts for ts in attempts_snapshot if ts > cutoff),
            default=now)
        next_ts = oldest_in_window + 600
        return (False,
                f'10min_limit ({ten_min}/{MAX_ATTEMPTS_PER_10MIN})',
                next_ts)

    return (True, '', 0)


def check_cooldown(action_type):
    """Check per-action-type cooldown.
    Returns (ok, reason, remaining_sec).
    """
    cd_sec = ACTION_COOLDOWNS.get(action_type, COOLDOWN_AFTER_ANY_ORDER_SEC)
    if cd_sec <= 0:
        return (True, '', 0)

    with _lock:
        last_ts = _state['last_action_ts'].get(action_type, 0)
        last_any = _state['last_order_ts']

    now = time.time()

    # Action-specific cooldown
    elapsed_action = now - last_ts
    if last_ts > 0 and elapsed_action < cd_sec:
        remaining = cd_sec - elapsed_action
        return (False,
                f'cooldown_{action_type} ({remaining:.0f}s remaining)',
                remaining)

    # Base cooldown (any order)
    elapsed_any = now - last_any
    if last_any > 0 and elapsed_any < COOLDOWN_AFTER_ANY_ORDER_SEC:
        remaining = COOLDOWN_AFTER_ANY_ORDER_SEC - elapsed_any
        return (False,
                f'cooldown_any_order ({remaining:.0f}s remaining)',
                remaining)

    return (True, '', 0)


def check_entry_lock():
    """Check rejection-based entry lockout.
    Returns (ok, reason, lock_expires_ts).
    """
    with _lock:
        lock_until = _state['entry_lock_until']
        lock_reason = _state['entry_lock_reason']

    now = time.time()
    if lock_until > now:
        return (False,
                f'entry_locked: {lock_reason}',
                lock_until)

    return (True, '', 0)


def check_all(cur, action_type, symbol='BTC/USDT:USDT', direction=None, regime=None):
    """Unified throttle check. Combines entry_lock + signal_dedup + rate_limit + cooldown.
    EXIT actions (CLOSE, REDUCE, REVERSE_CLOSE, FULL_CLOSE) always bypass.
    Returns (ok, reason, meta_dict).
    """
    if action_type in EXIT_ACTIONS:
        return (True, '', {})

    # 1. Entry lock (rate limit hit -> 1h lockout)
    ok, reason, ts = check_entry_lock()
    if not ok:
        return (False, reason, {'next_allowed_ts': ts, 'lock_type': 'entry_lock'})

    # 1.5 Signal dedup (same direction+action within 60s, rejection cooldown 180s)
    if direction:
        ok, reason = check_signal_dedup(action_type, direction)
        if not ok:
            return (False, reason, {'lock_type': 'signal_dedup'})

    # 2. Rate limit (12/hr, 4/10min)
    ok, reason, ts = check_rate_limit(cur, symbol)
    if not ok:
        return (False, reason, {'next_allowed_ts': ts, 'lock_type': 'rate_limit'})

    # 2.5 RANGE mode stricter hourly rate limit
    if regime == 'RANGE':
        now = time.time()
        with _lock:
            attempts_1h = len([t for t in _state.get('recent_attempts', []) if now - t < 3600])
        if attempts_1h >= 3:
            return (False, f'RANGE 시간당 제한 ({attempts_1h}/3)', {'lock_type': 'range_rate_limit'})

    # 3. Cooldown (action-type specific)
    ok, reason, remaining = check_cooldown(action_type)
    if not ok:
        return (False, reason, {'cooldown_remaining': remaining, 'lock_type': 'cooldown'})

    return (True, '', {})


def handle_rejection(cur, error_code, reject_reason):
    """Handle order rejection: set backoff based on rejection reason.
    Called after exchange or compliance rejection.
    FIX #2: _auto_halt_trading called outside _lock to prevent stall.
    """
    reason_lower = (reject_reason or '').lower()
    should_halt = False
    halt_reason = ''

    with _lock:
        _state['last_reject_reason'] = (reject_reason or '')[:200]
        _state['last_reject_ts'] = time.time()

        # 1. Rate limit (Bybit 15/15 or error code 10006)
        if (error_code == 10006
                or '한도 초과' in (reject_reason or '')
                or 'rate limit' in reason_lower):
            # Exponential backoff: escalate stage
            stage = _state['throttle_backoff_stage']
            if stage < len(THROTTLE_BACKOFF_STAGES):
                lockout = THROTTLE_BACKOFF_STAGES[stage]
                _state['throttle_backoff_stage'] = stage + 1
            else:
                lockout = THROTTLE_BACKOFF_STAGES[-1]
            _state['entry_lock_until'] = time.time() + lockout
            _state['entry_lock_reason'] = f'RATE_LIMIT_HIT (stage {stage})'
            _state['network_consecutive'] = 0
            _log(f'RATE_LIMIT_HIT: locked for {lockout}s (backoff stage {stage})')
            return

        # 2. Min qty — FIX #5: proper parenthesization
        if (('최소' in (reject_reason or '') and '수량' in (reject_reason or ''))
                or ('min' in reason_lower and ('qty' in reason_lower or 'notional' in reason_lower))):
            _state['entry_lock_until'] = time.time() + 60
            _state['entry_lock_reason'] = 'MIN_QTY'
            _state['network_consecutive'] = 0
            _log(f'MIN_QTY: locked for 60s')
            return

        # 3. DB error (ON CONFLICT etc)
        if ('InvalidColumnReference' in (reject_reason or '')
                or 'ON CONFLICT' in (reject_reason or '')):
            _state['db_error_consecutive'] += 1
            if _state['db_error_consecutive'] >= DB_ERROR_MAX_CONSECUTIVE:
                should_halt = True
                halt_reason = 'db_error_auto_halt'
            _state['entry_lock_until'] = time.time() + DB_ERROR_BACKOFF_SEC
            _state['entry_lock_reason'] = 'DB_ERROR'
            _log(f'DB_ERROR: consecutive={_state["db_error_consecutive"]}, '
                 f'locked for {DB_ERROR_BACKOFF_SEC}s')
            # don't return yet — need to call _auto_halt outside lock
        else:
            # 4. Network / other errors
            _state['network_consecutive'] += 1
            backoff = min(
                NETWORK_BACKOFF_BASE * (2 ** (_state['network_consecutive'] - 1)),
                NETWORK_BACKOFF_MAX)
            _state['entry_lock_until'] = time.time() + backoff
            _state['entry_lock_reason'] = 'NETWORK_ERROR'
            _log(f'NETWORK_ERROR: consecutive={_state["network_consecutive"]}, '
                 f'backoff={backoff}s')

    # FIX #2: call _auto_halt_trading OUTSIDE _lock
    if should_halt:
        _auto_halt_trading(halt_reason)


def handle_success(action_type):
    """Reset error counters on successful order."""
    with _lock:
        _state['network_consecutive'] = 0
        _state['db_error_consecutive'] = 0
        _state['throttle_backoff_stage'] = 0  # reset exponential backoff
        # Clear entry lock if it was from a transient error
        if _state['entry_lock_reason'] in ('NETWORK_ERROR', 'MIN_QTY'):
            _state['entry_lock_until'] = 0
            _state['entry_lock_reason'] = ''


def get_throttle_status(cur=None):
    """Get full throttle status dict for /snapshot and /debug."""
    # FIX #12: extract scalars under lock, format datetimes outside
    with _lock:
        now = time.time()
        _prune_old_attempts(now)

        hourly_count = _count_recent(3600, now)
        ten_min_count = _count_recent(600, now)

        entry_locked = _state['entry_lock_until'] > now
        lock_expires = _state['entry_lock_until']
        lock_reason = _state['entry_lock_reason']

        cooldowns = {}
        for action, cd_sec in ACTION_COOLDOWNS.items():
            if cd_sec <= 0:
                continue
            last = _state['last_action_ts'].get(action, 0)
            if last > 0:
                cooldowns[action] = max(0, cd_sec - (now - last))
            else:
                cooldowns[action] = 0

        last_any = _state['last_order_ts']
        base_remaining = max(0, COOLDOWN_AFTER_ANY_ORDER_SEC - (now - last_any)) if last_any > 0 else 0

        last_reject = _state['last_reject_reason']
        last_reject_ts = _state['last_reject_ts']
        net_consecutive = _state['network_consecutive']
        db_consecutive = _state['db_error_consecutive']
        last_order_ts = _state['last_order_ts']

    # Format datetimes outside lock
    lock_expires_str = ''
    if entry_locked:
        try:
            lock_expires_str = datetime.fromtimestamp(
                lock_expires, tz=timezone.utc
            ).strftime('%H:%M:%S UTC')
        except Exception:
            lock_expires_str = '?'

    last_reject_ts_str = ''
    if last_reject_ts > 0:
        try:
            last_reject_ts_str = datetime.fromtimestamp(
                last_reject_ts, tz=timezone.utc
            ).strftime('%H:%M:%S UTC')
        except Exception:
            last_reject_ts_str = '?'

    return {
        'hourly_count': hourly_count,
        'hourly_limit': MAX_ATTEMPTS_PER_HOUR,
        '10min_count': ten_min_count,
        '10min_limit': MAX_ATTEMPTS_PER_10MIN,
        'entry_locked': entry_locked,
        'lock_reason': lock_reason,
        'lock_expires_ts': lock_expires,
        'lock_expires_str': lock_expires_str,
        'last_reject': last_reject,
        'last_reject_ts': last_reject_ts,
        'last_reject_ts_str': last_reject_ts_str,
        'cooldowns': cooldowns,
        'base_cooldown_remaining': base_remaining,
        'network_consecutive': net_consecutive,
        'db_error_consecutive': db_consecutive,
        'last_order_ts': last_order_ts,
    }


# ── Internal helpers ─────────────────────────────────────

def _auto_halt_trading(reason):
    """Auto-halt trading by setting trade_switch OFF with reason.
    FIX #7/#11: uses its own DB connection instead of passed cursor.
    """
    conn = None
    try:
        from db_config import get_conn
        conn = get_conn(autocommit=True)
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE trade_switch
                SET enabled = false, off_reason = %s, updated_at = now()
                WHERE id = (SELECT id FROM trade_switch ORDER BY id DESC LIMIT 1);
            """, (reason,))
        _log(f'AUTO HALT: trade_switch OFF, reason={reason}')
    except Exception as e:
        _log(f'_auto_halt_trading DB error: {e}')
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    # Send telegram notification
    try:
        import urllib.parse
        import urllib.request
        cfg = {}
        try:
            with open('/root/trading-bot/app/telegram_cmd.env') as f:
                for line in f:
                    line = line.strip()
                    if '=' in line and not line.startswith('#'):
                        k, v = line.split('=', 1)
                        cfg[k.strip()] = v.strip()
        except Exception:
            pass
        token = cfg.get('TELEGRAM_BOT_TOKEN', '')
        chat_id = cfg.get('TELEGRAM_ALLOWED_CHAT_ID', '')
        if token and chat_id:
            text = (f'🛑 AUTO HALT: trade_switch OFF\n'
                    f'사유: {reason}\n'
                    f'DB 오류 {DB_ERROR_MAX_CONSECUTIVE}회 연속 — 수동 확인 필요\n'
                    f'/trade on 으로 재개')
            url = f'https://api.telegram.org/bot{token}/sendMessage'
            data = urllib.parse.urlencode({
                'chat_id': chat_id,
                'text': text,
                'disable_web_page_preview': 'true'}).encode('utf-8')
            req = urllib.request.Request(url, data=data, method='POST')
            urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass


def get_next_try_ts():
    """v14: Compute the earliest timestamp when a new entry/ADD order can be attempted.
    Returns (next_ts_float, reason_str). If no block, returns (0, 'READY').
    """
    now = time.time()
    blockers = []

    with _lock:
        _prune_old_attempts(now)
        hourly = _count_recent(3600, now)
        ten_min = _count_recent(600, now)
        attempts_snapshot = list(_state['recent_attempts'])

        # Entry lock
        if _state['entry_lock_until'] > now:
            blockers.append((_state['entry_lock_until'], _state['entry_lock_reason']))

        # Rejection cooldown
        if _state['last_reject_ts'] > 0:
            reject_until = _state['last_reject_ts'] + REJECTION_COOLDOWN_SEC
            if reject_until > now:
                blockers.append((reject_until, 'rejection_cooldown'))

        # Base cooldown
        if _state['last_order_ts'] > 0:
            base_until = _state['last_order_ts'] + COOLDOWN_AFTER_ANY_ORDER_SEC
            if base_until > now:
                blockers.append((base_until, 'base_cooldown'))

        # ADD specific cooldown
        add_ts = _state['last_action_ts'].get('ADD', 0)
        if add_ts > 0:
            add_until = add_ts + COOLDOWN_AFTER_ADD_SEC
            if add_until > now:
                blockers.append((add_until, 'add_cooldown'))

    # Rate limit
    if hourly >= MAX_ATTEMPTS_PER_HOUR:
        cutoff = now - 3600
        oldest = min((ts for ts in attempts_snapshot if ts > cutoff), default=now)
        blockers.append((oldest + 3600, f'hourly_limit ({hourly}/{MAX_ATTEMPTS_PER_HOUR})'))

    if ten_min >= MAX_ATTEMPTS_PER_10MIN:
        cutoff = now - 600
        oldest = min((ts for ts in attempts_snapshot if ts > cutoff), default=now)
        blockers.append((oldest + 600, f'10min_limit ({ten_min}/{MAX_ATTEMPTS_PER_10MIN})'))

    if not blockers:
        return (0, 'READY')

    # Return the latest blocker (all must clear)
    max_ts, max_reason = max(blockers, key=lambda x: x[0])
    return (max_ts, max_reason)


def _get_kst_day_start():
    """Compute KST 00:00 as UTC timestamp.
    KST = UTC+9, so KST midnight = UTC 15:00 previous day.
    """
    import math
    now = time.time()
    kst_now = now + KST_UTC_OFFSET_SEC
    kst_day_start = math.floor(kst_now / 86400) * 86400
    return kst_day_start - KST_UTC_OFFSET_SEC  # convert back to UTC epoch


def _count_daily_orders_kst(cur):
    """Count filled/sent orders since KST 00:00.
    Uses execution_log (FILLED/SENT) — not attempts.
    """
    kst_start = _get_kst_day_start()
    try:
        cur.execute("""
            SELECT COUNT(*) FROM execution_log
            WHERE symbol = 'BTC/USDT:USDT'
              AND status IN ('FILLED', 'SENT')
              AND ts >= to_timestamp(%s)
        """, (kst_start,))
        row = cur.fetchone()
        return int(row[0]) if row else 0
    except Exception as e:
        _log(f'_count_daily_orders_kst error (FAIL-OPEN): {e}')
        return 0


def is_entry_blocked(cur):
    """Unified pre-signal gate check. Should be called BEFORE signal generation.

    Returns (blocked: bool, reason: str, cooldown_until: float).
    If blocked=True, caller should skip entire signal generation cycle.
    FAIL-OPEN: returns (False, '', 0) on any unexpected error.
    """
    try:
        _ensure_loaded(cur)
        now = time.time()

        # 1. Daily limit (KST boundary)
        daily_count = _count_daily_orders_kst(cur)
        if daily_count >= DAILY_TRADE_LIMIT:
            kst_start = _get_kst_day_start()
            next_day = kst_start + 86400
            return (True, f'daily_limit ({daily_count}/{DAILY_TRADE_LIMIT} KST)', next_day)

        # 2. Entry lock (rejection-based lockout)
        ok, reason, lock_ts = check_entry_lock()
        if not ok:
            return (True, reason, lock_ts)

        # 3. Rejection cooldown
        with _lock:
            if _state['last_reject_ts'] > 0:
                elapsed = now - _state['last_reject_ts']
                if elapsed < REJECTION_COOLDOWN_SEC:
                    return (True,
                            f'rejection_cooldown ({REJECTION_COOLDOWN_SEC - elapsed:.0f}s remaining)',
                            _state['last_reject_ts'] + REJECTION_COOLDOWN_SEC)

        # 4. Rate limits (hourly + 10min)
        ok, reason, next_ts = check_rate_limit(cur)
        if not ok:
            return (True, reason, next_ts)

        return (False, '', 0)

    except Exception as e:
        _log(f'is_entry_blocked FAIL-OPEN: {e}')
        return (False, '', 0)


def get_state_snapshot():
    """Return throttle state snapshot for debug display."""
    now = time.time()
    with _lock:
        attempts = _state.get('recent_attempts', [])
        attempts_1h = len([t for t in attempts if now - t < 3600])
        entry_locked = _state.get('entry_lock_until', 0) > now
        cooldown_remaining = max(0, _state.get('entry_lock_until', 0) - now)
        last_reject_reason = _state.get('last_reject_reason', '')
        last_reject_ts = _state.get('last_reject_ts', 0)
        rejection_cooldown_remaining = max(0, (last_reject_ts + REJECTION_COOLDOWN_SEC) - now) if last_reject_ts else 0
    # v14: next_try_ts
    next_ts, next_reason = get_next_try_ts()
    next_try_str = ''
    if next_ts > 0:
        try:
            next_try_str = datetime.fromtimestamp(next_ts, tz=timezone.utc).strftime('%H:%M:%S UTC')
        except Exception:
            next_try_str = '?'
    return {
        'attempts_1h': attempts_1h,
        'entry_locked': entry_locked,
        'cooldown_remaining': cooldown_remaining,
        'last_reject_reason': last_reject_reason,
        'rejection_cooldown_remaining': rejection_cooldown_remaining,
        'next_try_ts': next_ts,
        'next_try_str': next_try_str,
        'next_try_reason': next_reason,
    }
