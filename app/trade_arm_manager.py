"""
trade_arm_manager.py — 무장 상태 관리.

ARM 상태가 활성화되어야 Claude auto-apply가 작동.
TTL 기반 자동 만료, 명시적 disarm 지원.
"""
import sys
sys.path.insert(0, '/root/trading-bot/app')

LOG_PREFIX = '[trade_arm]'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _get_conn():
    from db_config import get_conn
    return get_conn()


def arm(chat_id, ttl_hours=12):
    """Arm the trading state. Returns status dict."""
    conn = None
    try:
        conn = _get_conn()
        with conn.cursor() as cur:
            # Auto-disarm expired first
            _auto_disarm_expired_inner(cur)
            cur.execute("""
                INSERT INTO trade_arm_state
                    (chat_id, armed, armed_at, expires_at)
                VALUES (%s, true, now(), now() + interval '%s hours')
                RETURNING id, armed_at, expires_at;
            """.replace('%s hours', f'{int(ttl_hours)} hours'),
                (chat_id,))
            row = cur.fetchone()
        conn.commit()
        result = {
            'armed': True,
            'id': row[0],
            'armed_at': str(row[1]),
            'expires_at': str(row[2]),
            'ttl_hours': ttl_hours,
        }
        _log(f'ARMED chat_id={chat_id} ttl={ttl_hours}h')
        return result
    except Exception as e:
        _log(f'arm error: {e}')
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return {'armed': False, 'error': str(e)}
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def disarm(chat_id):
    """Disarm all active states for chat. Returns status dict."""
    conn = None
    try:
        conn = _get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE trade_arm_state
                SET armed = false, disarmed_at = now()
                WHERE chat_id = %s AND armed = true;
            """, (chat_id,))
            updated = cur.rowcount
        conn.commit()
        _log(f'DISARMED chat_id={chat_id} updated={updated}')
        return {'armed': False, 'disarmed_count': updated}
    except Exception as e:
        _log(f'disarm error: {e}')
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return {'armed': False, 'error': str(e)}
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def is_armed(chat_id):
    """Check if armed and not expired. Returns (bool, dict)."""
    conn = None
    try:
        conn = _get_conn()
        with conn.cursor() as cur:
            _auto_disarm_expired_inner(cur)
            cur.execute("""
                SELECT id, armed, armed_at, expires_at
                FROM trade_arm_state
                WHERE chat_id = %s AND armed = true AND expires_at > now()
                ORDER BY armed_at DESC
                LIMIT 1;
            """, (chat_id,))
            row = cur.fetchone()
        conn.commit()
        if row:
            return (True, {
                'id': row[0],
                'armed': True,
                'armed_at': str(row[2]) if row[2] else '',
                'expires_at': str(row[3]) if row[3] else '',
            })
        return (False, {'armed': False})
    except Exception as e:
        _log(f'is_armed error: {e}')
        return (False, {'armed': False, 'error': str(e)})
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def get_status(chat_id):
    """Get full arm status including expired entries."""
    conn = None
    try:
        conn = _get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, armed, armed_at, expires_at, disarmed_at
                FROM trade_arm_state
                WHERE chat_id = %s
                ORDER BY id DESC
                LIMIT 5;
            """, (chat_id,))
            rows = cur.fetchall()
        entries = []
        for r in rows:
            entries.append({
                'id': r[0], 'armed': r[1],
                'armed_at': str(r[2]) if r[2] else None,
                'expires_at': str(r[3]) if r[3] else None,
                'disarmed_at': str(r[4]) if r[4] else None,
            })
        armed_now, current = is_armed(chat_id)
        return {
            'currently_armed': armed_now,
            'current': current,
            'recent_entries': entries,
        }
    except Exception as e:
        _log(f'get_status error: {e}')
        return {'currently_armed': False, 'error': str(e)}
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def auto_disarm_expired():
    """Disarm all expired arm states across all chats."""
    conn = None
    try:
        conn = _get_conn()
        with conn.cursor() as cur:
            count = _auto_disarm_expired_inner(cur)
        conn.commit()
        if count > 0:
            _log(f'auto_disarm_expired: disarmed {count} expired entries')
        return count
    except Exception as e:
        _log(f'auto_disarm_expired error: {e}')
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return 0
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _auto_disarm_expired_inner(cur):
    """Inner function: disarm expired (uses existing cursor)."""
    cur.execute("""
        UPDATE trade_arm_state
        SET armed = false, disarmed_at = now()
        WHERE armed = true AND expires_at <= now();
    """)
    return cur.rowcount
