# Source Generated with Decompyle++
# File: position_watcher.cpython-312.pyc (Python 3.12)

import os
import json
import time
import sqlite3
import urllib.parse
import urllib.request
from typing import Dict, Any, Tuple
ENV_PATH = '/root/trading-bot/app/telegram_cmd.env'
DB_PATH = '/root/trading-bot/app/trading_bot.db'
STATE_FILE = '/root/trading-bot/app/.position_watcher_state.json'
POLL_SEC = 5

def load_env(path=None):
    env = {}
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, v = line.split('=', 1)
            env[k.strip()] = v.strip()
    return env


def tg_api_call(token=None, method=None, params=None):
    url = f'https://api.telegram.org/bot{token}/{method}'
    data = urllib.parse.urlencode(params).encode('utf-8')
    req = urllib.request.Request(url, data=data, method='POST')
    with urllib.request.urlopen(req, timeout=20) as resp:
        body = resp.read().decode('utf-8')
    return json.loads(body)


def send_message(token=None, chat_id=None, text=None):
    chunks = []
    s = text
    while len(s) > 3800:
        chunks.append(s[:3800])
        s = s[3800:]
    chunks.append(s)
    for c in chunks:
        tg_api_call(token, 'sendMessage', {
            'chat_id': str(chat_id),
            'text': c})


def read_state():
    try:
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def write_state(state=None):
    tmp = STATE_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_FILE)


def table_exists(conn=None, table=None):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return cur.fetchone() is not None


def fetch_positions(conn=None):
    '''
    open_positions 테이블에서 현재 포지션들을 가져와 dict로 반환.
    key는 안정적으로 만들기 위해 (symbol + side) 조합을 우선 사용.
    (실제 컬럼이 다를 수 있어 동적으로 컬럼을 읽음)
    '''
    cur = conn.cursor()
    cur.execute('SELECT * FROM open_positions')
    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]
    result = {}
    for row in rows:
        d = dict(zip(cols, row))
        sym = d.get('symbol', d.get('market', d.get('ticker', 'unknown')))
        side = d.get('side', d.get('position_side', d.get('dir', 'unknown')))
        key = f'{sym}:{side}'
        result[key] = d
    return result


def summarize_row(row=None):
    symbol = row.get('symbol', row.get('market', row.get('ticker', '')))
    side = row.get('side', row.get('position_side', row.get('dir', '')))
    qty = row.get('qty', row.get('size', row.get('amount', '')))
    entry = row.get('entry_price', row.get('avg_entry', row.get('price', '')))
    lev = row.get('leverage', row.get('lev', ''))
    ts = row.get('created_at', row.get('open_time', row.get('ts', '')))
    parts = []
    if symbol:
        parts.append(f'symbol={symbol}')
    if side:
        parts.append(f'side={side}')
    if qty != '':
        parts.append(f'qty={qty}')
    if entry != '':
        parts.append(f'entry={entry}')
    if lev != '':
        parts.append(f'lev={lev}')
    if ts != '':
        parts.append(f'ts={ts}')
    if parts:
        return ', '.join(parts)
    return json.dumps(row, ensure_ascii=False)


def main():
    env = load_env(ENV_PATH)
    token = env.get('TELEGRAM_BOT_TOKEN', '').strip()
    chat_id = int(env.get('TELEGRAM_ALLOWED_CHAT_ID', '0'))
    if not token or chat_id == 0:
        raise SystemExit('ENV missing: TELEGRAM_BOT_TOKEN / TELEGRAM_ALLOWED_CHAT_ID')
    if not os.path.exists(DB_PATH):
        raise SystemExit(f'DB not found: {DB_PATH}')
    state = read_state()
    known = state.get('known', {})
    print('=== POSITION WATCHER STARTED ===', flush=True)
    while True:
        try:
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            if not table_exists(conn, 'open_positions'):
                conn.close()
                time.sleep(POLL_SEC)
                continue
            current = fetch_positions(conn)
            conn.close()
            # Detect new positions
            for key, row in current.items():
                if key not in known:
                    summary = summarize_row(row)
                    msg = f'🟢 NEW POSITION\n{summary}'
                    print(msg, flush=True)
                    send_message(token, chat_id, msg)
            # Detect closed positions
            for key in list(known.keys()):
                if key not in current:
                    summary = summarize_row(known[key])
                    msg = f'🔴 POSITION CLOSED\n{summary}'
                    print(msg, flush=True)
                    send_message(token, chat_id, msg)
            known = {k: dict(v) if hasattr(v, 'keys') else v for k, v in current.items()}
            state['known'] = known
            write_state(state)
            time.sleep(POLL_SEC)
        except Exception as e:
            print(f'[position_watcher] error: {e}', flush=True)
            time.sleep(POLL_SEC)

if __name__ == '__main__':
    main()
