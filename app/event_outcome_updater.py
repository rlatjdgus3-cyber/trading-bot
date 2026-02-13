# Source Generated with Decompyle++
# File: event_outcome_updater.cpython-312.pyc (Python 3.12)

'''
event_outcome_updater.py — Outcome backfill daemon.

Runs every 30 minutes to:
  1. Fill btc_move_1h/4h/24h for event_history entries
  2. Fill btc_move_1h/4h for score_history entries
'''
import os
import sys
import time
import traceback
sys.path.insert(0, '/root/trading-bot/app')
LOG_PREFIX = '[outcome_updater]'
SYMBOL = 'BTC/USDT:USDT'
POLL_SEC = 1800
KILL_SWITCH_PATH = '/root/trading-bot/app/KILL_SWITCH'

def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _db_conn():
    import psycopg2
    return psycopg2.connect(host=os.getenv('DB_HOST', 'localhost'), port=int(os.getenv('DB_PORT', '5432')), dbname=os.getenv('DB_NAME', 'trading'), user=os.getenv('DB_USER', 'bot'), password=os.getenv('DB_PASS', 'botpass'), connect_timeout=10, options='-c statement_timeout=60000')


def _get_btc_price_at(cur, ts, offset_hours):
    '''Get BTC price at ts + offset_hours.'''
    cur.execute("""
        SELECT c FROM candles
        WHERE symbol = %s AND tf = '1m'
          AND ts >= %s + make_interval(hours => %s) - interval '2 minutes'
          AND ts <= %s + make_interval(hours => %s) + interval '2 minutes'
        ORDER BY ts DESC LIMIT 1;
    """, (SYMBOL, ts, offset_hours, ts, offset_hours))
    row = cur.fetchone()
    if row and row[0]:
        return float(row[0])


def _update_event_history(cur):
    '''Backfill btc_move columns for event_history entries.'''
    cur.execute("""
        SELECT id, ts, btc_price_at,
               btc_move_1h IS NULL AS need_1h,
               btc_move_4h IS NULL AS need_4h,
               btc_move_24h IS NULL AS need_24h
        FROM event_history
        WHERE outcome_filled = false
          AND btc_price_at IS NOT NULL
          AND ts < now() - interval '1 hour'
        ORDER BY ts ASC
        LIMIT 100;
    """)
    rows = cur.fetchall()
    updated = 0
    for eid, ts, btc_price_at, need_1h, need_4h, need_24h in rows:
        btc_price_at = float(btc_price_at)
        if btc_price_at <= 0:
            continue
        updates = []
        params = []
        if need_1h:
            price_1h = _get_btc_price_at(cur, ts, 1)
            if price_1h:
                move_1h = ((price_1h - btc_price_at) / btc_price_at) * 100
                updates.append('btc_move_1h = %s')
                params.append(round(move_1h, 4))
        if need_4h:
            price_4h = _get_btc_price_at(cur, ts, 4)
            if price_4h:
                move_4h = ((price_4h - btc_price_at) / btc_price_at) * 100
                updates.append('btc_move_4h = %s')
                params.append(round(move_4h, 4))
        if need_24h:
            price_24h = _get_btc_price_at(cur, ts, 24)
            if price_24h:
                move_24h = ((price_24h - btc_price_at) / btc_price_at) * 100
                updates.append('btc_move_24h = %s')
                params.append(round(move_24h, 4))
        if not updates:
            continue
        all_filled = (not need_1h or 'btc_move_1h' in ' '.join(updates)) and \
                     (not need_4h or 'btc_move_4h' in ' '.join(updates)) and \
                     (not need_24h or 'btc_move_24h' in ' '.join(updates))
        if all_filled:
            updates.append('outcome_filled = true')
        params.append(eid)
        cur.execute(f"UPDATE event_history SET {', '.join(updates)} WHERE id = %s;", params)
        updated += 1
    return updated


def _update_score_history(cur):
    '''Backfill btc_move columns for score_history entries.'''
    cur.execute("""
        SELECT id, ts, btc_price
        FROM score_history
        WHERE btc_price IS NOT NULL
          AND btc_move_1h IS NULL
          AND ts < now() - interval '1 hour'
        ORDER BY ts ASC
        LIMIT 200;
    """)
    rows = cur.fetchall()
    updated = 0
    for sid, ts, btc_price in rows:
        btc_price = float(btc_price)
        if btc_price <= 0:
            continue
        price_1h = _get_btc_price_at(cur, ts, 1)
        price_4h = _get_btc_price_at(cur, ts, 4)
        updates = []
        params = []
        if price_1h:
            move_1h = ((price_1h - btc_price) / btc_price) * 100
            updates.append('btc_move_1h = %s')
            params.append(round(move_1h, 4))
        if price_4h:
            move_4h = ((price_4h - btc_price) / btc_price) * 100
            updates.append('btc_move_4h = %s')
            params.append(round(move_4h, 4))
        if not updates:
            continue
        params.append(sid)
        cur.execute(f"UPDATE score_history SET {', '.join(updates)} WHERE id = %s;", params)
        updated += 1
    return updated


def _cycle():
    '''One update cycle.'''
    conn = None
    try:
        conn = _db_conn()
        conn.autocommit = True
        with conn.cursor() as cur:
            n_event = _update_event_history(cur)
            n_score = _update_score_history(cur)
        _log(f'cycle done: event_history={n_event}, score_history={n_score}')
    except Exception:
        _log(f'cycle error:\n{traceback.format_exc()}')
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def main():
    _log('=== EVENT OUTCOME UPDATER START ===')
    conn = _db_conn()
    conn.autocommit = True
    conn.close()
    _log('DB connection OK')
    while True:
        if os.path.exists(KILL_SWITCH_PATH):
            _log('KILL_SWITCH detected, exiting')
            break
        _cycle()
        time.sleep(POLL_SEC)

if __name__ == '__main__':
    if '--once' in sys.argv:
        _cycle()
    else:
        main()
