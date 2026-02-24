"""
event_lock.py — DB-based event dedup lock.

Replaces all in-memory dedup (event_hash, HOLD repeat, consecutive HOLD)
with PostgreSQL-backed locks visible to ALL processes.

Lock types:
  event     — symbol:trigger_type:price_bucket  (TTL 10 min)
  hold_sup  — hold_suppress:symbol              (TTL 15 min)
  hash      — hash:event_hash                   (TTL 30 min)

Usage:
  from event_lock import acquire_event_lock, is_locked, check_hold_suppress
"""
import os
import sys
import time
import json

sys.path.insert(0, '/root/trading-bot/app')

LOG_PREFIX = '[event_lock]'

# TTL defaults
EVENT_LOCK_TTL_SEC = 600       # 10 min — per trigger type+price
HOLD_SUPPRESS_TTL_SEC = 900    # 15 min — after 2 consecutive HOLDs
HASH_LOCK_TTL_SEC = 1800       # 30 min — event hash dedup
CONSECUTIVE_HOLD_LIMIT = 2     # 2 consecutive HOLDs → suppress

# Telegram suppression notification dedup (in-memory, per-process)
_suppress_notify_ts = {}       # {lock_key: last_notify_timestamp}
SUPPRESS_NOTIFY_COOLDOWN_SEC = 900  # max 1 notification per trigger type per 15 min

# Suppression accumulator: count per trigger_type, flush every 15 min
_suppress_accumulator = {}     # {trigger_type: count}
_suppress_accumulator_ts = 0   # last flush timestamp
SUPPRESS_ACCUMULATOR_SEC = int(os.getenv('SUPPRESS_ACCUMULATOR_SEC', '900'))

# Emergency triggers that bypass accumulation and send immediately
IMMEDIATE_PASS_TRIGGERS = {
    'emergency_position_loss',
    'emergency_liquidation_near',
    'emergency_price_5m',
    'emergency_price_15m',
}

# Telegram env cache
_tg_env_cache = {}


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _db_conn():
    from db_config import get_conn
    return get_conn()


def _ensure_conn(conn):
    """Return (conn, should_close). Creates connection if None."""
    if conn is not None:
        return conn, False
    return _db_conn(), True


# ── Lock key builders ────────────────────────────────────

def make_event_key(symbol, trigger_types, price=None):
    """Build event lock key: symbol:sorted_triggers:price_bucket."""
    types_str = '|'.join(sorted(trigger_types)) if trigger_types else 'unknown'
    price_bucket = int(price / 500) * 500 if price else 0
    return f'{symbol}:{types_str}:{price_bucket}'


def make_hash_key(event_hash):
    """Build hash lock key."""
    return f'hash:{event_hash}'


def make_hold_suppress_key(symbol):
    """Build HOLD suppress lock key."""
    return f'hold_suppress:{symbol}'


# ── Core lock operations ─────────────────────────────────

def acquire_lock(lock_key, ttl_sec, caller='unknown', lock_type='event',
                 detail=None, conn=None):
    """Try to acquire a DB lock. Returns (acquired: bool, info: dict).

    If lock already exists and not expired → returns (False, {remaining_sec, ...}).
    If lock expired or doesn't exist → upserts new lock, returns (True, {}).
    """
    conn, close_conn = _ensure_conn(conn)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            # Check existing lock
            cur.execute("""
                SELECT lock_key, expires_at, caller,
                       EXTRACT(EPOCH FROM (expires_at - now())) AS remaining_sec
                FROM event_lock
                WHERE lock_key = %s AND expires_at > now();
            """, (lock_key,))
            row = cur.fetchone()
            if row:
                remaining = max(0, int(row[3]))
                _log(f'LOCK EXISTS: key={lock_key} caller={row[2]} '
                     f'remaining={remaining}s')
                return False, {
                    'lock_key': row[0],
                    'caller': row[2],
                    'remaining_sec': remaining,
                }

            # Upsert lock using make_interval for safe parameterization
            cur.execute("""
                INSERT INTO event_lock (lock_key, expires_at, caller, lock_type, detail)
                VALUES (%s, now() + make_interval(secs => %s), %s, %s, %s::jsonb)
                ON CONFLICT (lock_key) DO UPDATE SET
                    expires_at = now() + make_interval(secs => %s),
                    caller = EXCLUDED.caller,
                    lock_type = EXCLUDED.lock_type,
                    detail = EXCLUDED.detail,
                    created_at = now();
            """, (lock_key, ttl_sec, caller, lock_type,
                  json.dumps(detail or {}, default=str),
                  ttl_sec))
            _log(f'LOCK ACQUIRED: key={lock_key} ttl={ttl_sec}s caller={caller}')
            return True, {}

    except Exception as e:
        _log(f'acquire_lock error: {e}')
        return True, {}  # fail-open: allow on DB error
    finally:
        if close_conn and conn:
            try:
                conn.close()
            except Exception:
                pass


def is_locked(lock_key, conn=None):
    """Check if lock is active. Returns (locked: bool, info: dict)."""
    conn, close_conn = _ensure_conn(conn)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("""
                SELECT lock_key, expires_at, caller, lock_type,
                       EXTRACT(EPOCH FROM (expires_at - now())) AS remaining_sec
                FROM event_lock
                WHERE lock_key = %s AND expires_at > now();
            """, (lock_key,))
            row = cur.fetchone()
            if row:
                remaining = max(0, int(row[4]))
                return True, {
                    'lock_key': row[0],
                    'caller': row[2],
                    'lock_type': row[3],
                    'remaining_sec': remaining,
                }
            return False, {}
    except Exception as e:
        _log(f'is_locked error: {e}')
        return False, {}  # fail-open
    finally:
        if close_conn and conn:
            try:
                conn.close()
            except Exception:
                pass


def cleanup_expired(conn=None):
    """Delete expired locks."""
    conn, close_conn = _ensure_conn(conn)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("DELETE FROM event_lock WHERE expires_at <= now();")
            count = cur.rowcount
            if count > 0:
                _log(f'cleaned up {count} expired locks')
            return count
    except Exception as e:
        _log(f'cleanup error: {e}')
        return 0
    finally:
        if close_conn and conn:
            try:
                conn.close()
            except Exception:
                pass


# ── Event lock convenience ───────────────────────────────

def acquire_event_lock(symbol, trigger_types, price, caller='unknown', conn=None):
    """Acquire event dedup lock (10 min TTL).
    Returns (acquired, info).
    """
    key = make_event_key(symbol, trigger_types, price)
    return acquire_lock(key, EVENT_LOCK_TTL_SEC, caller=caller,
                        lock_type='event',
                        detail={'trigger_types': trigger_types,
                                'price': price},
                        conn=conn)


def check_event_lock(symbol, trigger_types, price, conn=None):
    """Check if event lock exists (without acquiring).
    Returns (locked, info).
    """
    key = make_event_key(symbol, trigger_types, price)
    return is_locked(key, conn=conn)


# ── Hash lock convenience ────────────────────────────────

def acquire_hash_lock(event_hash, caller='unknown', conn=None):
    """Acquire event hash dedup lock (30 min TTL).
    Returns (acquired, info).
    """
    key = make_hash_key(event_hash)
    return acquire_lock(key, HASH_LOCK_TTL_SEC, caller=caller,
                        lock_type='hash',
                        detail={'event_hash': event_hash},
                        conn=conn)


def check_hash_lock(event_hash, conn=None):
    """Check if event hash lock exists.
    Returns (locked, info).
    """
    key = make_hash_key(event_hash)
    return is_locked(key, conn=conn)


# ── HOLD suppress ────────────────────────────────────────

def record_hold_result(symbol, action, trigger_types, caller='unknown',
                       conn=None):
    """Record Claude action result for consecutive HOLD tracking.

    If action == 'HOLD' and same trigger_types:
      increment consecutive_count
      if count >= 2 → create hold_suppress lock (15 min)
    If action != 'HOLD':
      reset consecutive_count to 0

    Returns (suppressed: bool) — True if suppress_lock was just created.
    """
    conn, close_conn = _ensure_conn(conn)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            trigger_arr = sorted(trigger_types) if trigger_types else []

            if action == 'HOLD':
                # Upsert: increment if same triggers, reset if different
                cur.execute("""
                    INSERT INTO hold_consecutive
                        (symbol, consecutive_count, last_trigger_types,
                         last_caller, last_action, updated_at)
                    VALUES (%s, 1, %s, %s, %s, now())
                    ON CONFLICT (symbol) DO UPDATE SET
                        consecutive_count = CASE
                            WHEN hold_consecutive.last_trigger_types = EXCLUDED.last_trigger_types
                                 AND hold_consecutive.last_action = 'HOLD'
                            THEN hold_consecutive.consecutive_count + 1
                            ELSE 1
                        END,
                        last_trigger_types = EXCLUDED.last_trigger_types,
                        last_caller = EXCLUDED.last_caller,
                        last_action = EXCLUDED.last_action,
                        updated_at = now()
                    RETURNING consecutive_count;
                """, (symbol, trigger_arr, caller, action))
                row = cur.fetchone()
                count = row[0] if row else 0

                _log(f'HOLD recorded: symbol={symbol} count={count}/{CONSECUTIVE_HOLD_LIMIT} '
                     f'triggers={trigger_arr}')

                if count >= CONSECUTIVE_HOLD_LIMIT:
                    # Create hold_suppress lock
                    sup_key = make_hold_suppress_key(symbol)
                    acquired, _ = acquire_lock(
                        sup_key, HOLD_SUPPRESS_TTL_SEC,
                        caller=caller, lock_type='hold_sup',
                        detail={'consecutive_count': count,
                                'trigger_types': trigger_arr},
                        conn=conn)
                    if acquired:
                        _log(f'HOLD SUPPRESS LOCK created: symbol={symbol} '
                             f'count={count} ttl={HOLD_SUPPRESS_TTL_SEC}s')
                        _notify_hold_suppress(symbol, count,
                                              HOLD_SUPPRESS_TTL_SEC,
                                              trigger_arr)
                    return True
            else:
                # Non-HOLD action → reset counter
                cur.execute("""
                    INSERT INTO hold_consecutive
                        (symbol, consecutive_count, last_trigger_types,
                         last_caller, last_action, updated_at)
                    VALUES (%s, 0, %s, %s, %s, now())
                    ON CONFLICT (symbol) DO UPDATE SET
                        consecutive_count = 0,
                        last_trigger_types = EXCLUDED.last_trigger_types,
                        last_caller = EXCLUDED.last_caller,
                        last_action = EXCLUDED.last_action,
                        updated_at = now();
                """, (symbol, trigger_arr, caller, action))
                _log(f'HOLD counter RESET: symbol={symbol} action={action}')

            return False
    except Exception as e:
        _log(f'record_hold_result error: {e}')
        return False
    finally:
        if close_conn and conn:
            try:
                conn.close()
            except Exception:
                pass


def check_hold_suppress(symbol, conn=None):
    """Check if HOLD suppress lock is active.
    Returns (suppressed: bool, info: dict).
    """
    key = make_hold_suppress_key(symbol)
    return is_locked(key, conn=conn)


# ── Claude call logging ──────────────────────────────────

def log_claude_call(caller, gate_type, call_type='AUTO', model_used=None,
                    input_tokens=0, output_tokens=0, estimated_cost=0,
                    latency_ms=0, event_hash=None, trigger_types=None,
                    action_result=None, allowed=True, deny_reason=None,
                    conn=None):
    """Log a Claude API call/attempt for attribution tracking."""
    conn, close_conn = _ensure_conn(conn)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO claude_call_log
                    (caller, gate_type, call_type, model_used,
                     input_tokens, output_tokens, estimated_cost,
                     latency_ms, event_hash, trigger_types,
                     action_result, allowed, deny_reason)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (caller, gate_type, call_type, model_used,
                  input_tokens, output_tokens, estimated_cost,
                  latency_ms, event_hash,
                  sorted(trigger_types) if trigger_types else None,
                  action_result, allowed, deny_reason))
    except Exception as e:
        _log(f'log_claude_call error: {e}')
    finally:
        if close_conn and conn:
            try:
                conn.close()
            except Exception:
                pass


# ── Statistics ───────────────────────────────────────────

def get_lock_stats(conn=None):
    """Get active lock statistics."""
    conn, close_conn = _ensure_conn(conn)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("""
                SELECT lock_type, COUNT(*) as cnt
                FROM event_lock
                WHERE expires_at > now()
                GROUP BY lock_type;
            """)
            stats = {row[0]: row[1] for row in cur.fetchall()}

            cur.execute("""
                SELECT COUNT(*) FROM event_lock WHERE expires_at > now();
            """)
            stats['total_active'] = cur.fetchone()[0]

            return stats
    except Exception as e:
        _log(f'get_lock_stats error: {e}')
        return {'total_active': 0}
    finally:
        if close_conn and conn:
            try:
                conn.close()
            except Exception:
                pass


def get_caller_stats(hours=1, conn=None):
    """Get Claude call statistics by caller for the last N hours."""
    conn, close_conn = _ensure_conn(conn)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("""
                SELECT caller,
                       COUNT(*) as total_calls,
                       COUNT(*) FILTER (WHERE allowed) as allowed_calls,
                       COUNT(*) FILTER (WHERE NOT allowed) as denied_calls,
                       COALESCE(SUM(estimated_cost) FILTER (WHERE allowed), 0) as total_cost,
                       COALESCE(SUM(input_tokens) FILTER (WHERE allowed), 0) as total_input_tokens,
                       COALESCE(SUM(output_tokens) FILTER (WHERE allowed), 0) as total_output_tokens
                FROM claude_call_log
                WHERE ts >= now() - make_interval(hours => %s)
                GROUP BY caller
                ORDER BY total_calls DESC;
            """, (hours,))
            results = []
            for row in cur.fetchall():
                results.append({
                    'caller': row[0],
                    'total_calls': row[1],
                    'allowed_calls': row[2],
                    'denied_calls': row[3],
                    'total_cost': float(row[4]),
                    'total_input_tokens': int(row[5]),
                    'total_output_tokens': int(row[6]),
                })
            return results
    except Exception as e:
        _log(f'get_caller_stats error: {e}')
        return []
    finally:
        if close_conn and conn:
            try:
                conn.close()
            except Exception:
                pass


def format_stats_report(hours=1):
    """Generate a formatted stats report string."""
    import report_formatter
    caller_stats = get_caller_stats(hours)
    lock_stats = get_lock_stats()
    return report_formatter.format_lock_stats_report(hours, caller_stats, lock_stats)


# ── Telegram notification ────────────────────────────────

def _load_telegram_env():
    """Load telegram env with caching."""
    if _tg_env_cache:
        return _tg_env_cache
    env_path = '/root/trading-bot/app/telegram_cmd.env'
    try:
        with open(env_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                k, v = line.split('=', 1)
                _tg_env_cache[k.strip()] = v.strip()
    except Exception:
        pass
    return _tg_env_cache


def _send_tg(text):
    """Send telegram message (shared helper)."""
    import report_formatter
    env = _load_telegram_env()
    token = env.get('TELEGRAM_BOT_TOKEN', '')
    chat_id = env.get('TELEGRAM_ALLOWED_CHAT_ID', '')
    if not token or not chat_id:
        return
    import urllib.parse
    import urllib.request
    text = report_formatter.korean_output_guard(text)
    url = f'https://api.telegram.org/bot{token}/sendMessage'
    data = urllib.parse.urlencode({
        'chat_id': chat_id,
        'text': text,
    }).encode('utf-8')
    req = urllib.request.Request(url, data=data, method='POST')
    urllib.request.urlopen(req, timeout=5)


def _notify_hold_suppress(symbol, count, ttl_sec, trigger_types):
    """Send one-time telegram notification about HOLD suppress lock."""
    try:
        import report_formatter
        text = report_formatter.format_hold_suppress_notice(
            symbol, count, ttl_sec // 60, trigger_types)
        _send_tg(text)
    except Exception as e:
        _log(f'telegram notify error: {e}')


def notify_event_suppressed(symbol, lock_info, trigger_types, caller='unknown'):
    """Accumulate suppression events and send summary every window period.

    긴급 트리거는 즉시 전송, 나머지는 누적 후 배치 요약.
    """
    global _suppress_accumulator_ts
    now = time.time()
    trigger_types = trigger_types or ['unknown']

    # 9-1: 긴급 이벤트 즉시 전송
    immediate = [t for t in trigger_types if t in IMMEDIATE_PASS_TRIGGERS]
    if immediate:
        try:
            import report_formatter
            kr_types = [report_formatter.TRIGGER_KR.get(t, t) for t in immediate]
            remaining = lock_info.get('remaining_sec', 0) if isinstance(lock_info, dict) else 0
            text = '🚨 긴급 이벤트 억제 (즉시 알림)\n'
            text += f'- 트리거: {", ".join(kr_types)}\n'
            if remaining > 0:
                text += f'- 잔여 락: {remaining}초\n'
            text += f'- 종목: {symbol}'
            _send_tg(text)
        except Exception as e:
            _log(f'telegram immediate notify error: {e}')
        # Don't accumulate immediate triggers
        trigger_types = [t for t in trigger_types if t not in IMMEDIATE_PASS_TRIGGERS]
        if not trigger_types:
            return

    # Accumulate counts per trigger type
    for t in trigger_types:
        _suppress_accumulator[t] = _suppress_accumulator.get(t, 0) + 1

    # Initialize accumulator timestamp
    if _suppress_accumulator_ts == 0:
        _suppress_accumulator_ts = now

    # Check if window passed → flush summary
    window = SUPPRESS_ACCUMULATOR_SEC
    if now - _suppress_accumulator_ts < window:
        return  # still accumulating

    # 9-2: Flush with improved batch format
    try:
        import report_formatter
        if _suppress_accumulator:
            total = sum(_suppress_accumulator.values())
            # Sort by count descending, limit to top 5
            sorted_items = sorted(_suppress_accumulator.items(),
                                  key=lambda x: x[1], reverse=True)[:5]
            text = f'🚫 억제 요약 ({window // 60}분): 총 {total}건\n'
            for trigger_type, count in sorted_items:
                kr = report_formatter.TRIGGER_KR.get(trigger_type, trigger_type)
                text += f'  • {kr} {count}회\n'
            _send_tg(text)
    except Exception as e:
        _log(f'telegram notify error: {e}')
    finally:
        # Reset accumulator
        _suppress_accumulator.clear()
        _suppress_accumulator_ts = now
