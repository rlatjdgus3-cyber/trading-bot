"""
chat_memory.py — 대화 기록 CRUD for GPT ChatAgent.

DB table: chat_memory (chat_id, role, content, tool_name, metadata, ts)
"""
import json
import sys
sys.path.insert(0, '/root/trading-bot/app')

LOG_PREFIX = '[chat_memory]'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _get_conn():
    from db_config import get_conn
    return get_conn()


def save_turn(chat_id, role, content, metadata=None, tool_name=None):
    """INSERT a single conversation turn."""
    conn = None
    try:
        conn = _get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO chat_memory (chat_id, role, content, tool_name, metadata)
                VALUES (%s, %s, %s, %s, %s::jsonb);
            """, (chat_id, role, content, tool_name,
                  json.dumps(metadata or {}, default=str)))
        conn.commit()
    except Exception as e:
        _log(f'save_turn error: {e}')
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def load_history(chat_id, limit=30):
    """Load recent N turns (oldest first) as list of dicts."""
    conn = None
    try:
        conn = _get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT role, content, tool_name, metadata, ts
                FROM chat_memory
                WHERE chat_id = %s
                ORDER BY ts DESC
                LIMIT %s;
            """, (chat_id, limit))
            rows = cur.fetchall()
        # Reverse to chronological order
        rows.reverse()
        result = []
        for r in rows:
            entry = {
                'role': r[0],
                'content': r[1],
            }
            if r[2]:
                entry['tool_name'] = r[2]
            if r[3]:
                entry['metadata'] = r[3] if isinstance(r[3], dict) else json.loads(r[3])
            result.append(entry)
        return result
    except Exception as e:
        _log(f'load_history error: {e}')
        return []
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def clear_history(chat_id):
    """Delete all turns for a chat."""
    conn = None
    try:
        conn = _get_conn()
        with conn.cursor() as cur:
            cur.execute('DELETE FROM chat_memory WHERE chat_id = %s;', (chat_id,))
        conn.commit()
    except Exception as e:
        _log(f'clear_history error: {e}')
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def trim_old(chat_id, keep=30):
    """Delete turns beyond the most recent `keep` entries."""
    conn = None
    try:
        conn = _get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM chat_memory
                WHERE chat_id = %s
                  AND id NOT IN (
                    SELECT id FROM chat_memory
                    WHERE chat_id = %s
                    ORDER BY ts DESC
                    LIMIT %s
                  );
            """, (chat_id, chat_id, keep))
            deleted = cur.rowcount
        conn.commit()
        if deleted > 0:
            _log(f'trim_old chat_id={chat_id}: deleted {deleted} old turns')
    except Exception as e:
        _log(f'trim_old error: {e}')
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
