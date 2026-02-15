# Source Generated with Decompyle++
# File: stopped_watchdog.cpython-312.pyc (Python 3.12)

'''
STOPPED Watchdog — 5분마다 실행.
STOPPED 지속 감지 시 텔레그램에 인라인 버튼(자동 해결) 포함 경고 발송.
실주문 관련 변경 절대 금지.
'''
import os
import sys
import json
import time
import datetime
import urllib.parse
import urllib.request
sys.path.insert(0, '/root/trading-bot/app')
ENV_PATH = '/root/trading-bot/app/telegram_cmd.env'
STATE_FILE = '/root/trading-bot/app/.stopped_watchdog_state.json'
COOLDOWN_SEC = 600

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


def db_conn():
    from db_config import get_conn
    return get_conn()


def check():
    '''Returns alert dict if STOPPED persists, else None.'''
    conn = None
    try:
        conn = db_conn()
        conn.autocommit = True
        with conn.cursor() as cur:
            # Check trade_switch
            cur.execute("SELECT enabled FROM trade_switch LIMIT 1;")
            sw = cur.fetchone()
            trade_sw = bool(sw and sw[0])

            # Check executor state
            cur.execute("SELECT mode FROM executor_state ORDER BY id DESC LIMIT 1;")
            es = cur.fetchone()
            exec_mode = es[0] if es else 'unknown'

            # Check last trade time
            cur.execute("SELECT EXTRACT(EPOCH FROM (now() - MAX(ts))) / 60 FROM trade_process_log;")
            row = cur.fetchone()
            minutes_ago = round(row[0]) if row and row[0] else 9999

            # Check action distribution
            cur.execute("""
                SELECT chosen_side, COUNT(*) FROM trade_process_log
                WHERE ts > now() - interval '1 hour'
                GROUP BY chosen_side ORDER BY COUNT(*) DESC LIMIT 5;
            """)
            action_rows = cur.fetchall()
            action_dist_str = ', '.join([f'{a}={c}' for a, c in action_rows]) if action_rows else 'none'

            # Check indicators freshness
            cur.execute("""
                SELECT EXTRACT(EPOCH FROM (now() - MAX(ts))) / 60 FROM indicators;
            """)
            ind_row = cur.fetchone()
            ind_age = round(ind_row[0]) if ind_row and ind_row[0] else 9999
            ind_ok = ind_age < 10

        if exec_mode == 'STOPPED' and minutes_ago > 30:
            return {
                'minutes_ago': minutes_ago,
                'trade_sw': trade_sw,
                'exec_mode': exec_mode,
                'action_dist_str': action_dist_str,
                'ind_ok': ind_ok,
            }
        return None
    except Exception as e:
        print(f'[stopped_watchdog] check error: {e}', flush=True)
        return None
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def load_state():
    try:
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state=None):
    tmp = STATE_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_FILE)


def should_alert(state=None):
    last = state.get('last_alert_ts', 0)
    return time.time() - last > COOLDOWN_SEC


def send_alert(token=None, chat_id=None, info=None):
    from report_formatter import korean_output_guard
    text = korean_output_guard(f"⚠️ STOPPED 상태가 지속되고 있습니다.\n- 최근 거래: {info['minutes_ago']}분 전\n- trade_switch: {info['trade_sw']}\n- executor_state: {info['exec_mode']}\n- 최근 action 분포: {info['action_dist_str']}\n- 지표 업데이트: {'정상' if info['ind_ok'] else '지연/이상'}\n\n아래 버튼을 누르면 자동 해결을 실행합니다.")
    keyboard = {
        'inline_keyboard': [
            [
                {
                    'text': '🔧 자동 해결 실행',
                    'callback_data': 'AUTO_HEAL_STOPPED'}]]}
    params = {
        'chat_id': str(chat_id),
        'text': text,
        'reply_markup': json.dumps(keyboard),
        'disable_web_page_preview': 'true'}
    url = f'https://api.telegram.org/bot{token}/sendMessage'
    data = urllib.parse.urlencode(params).encode('utf-8')
    req = urllib.request.Request(url, data=data, method='POST')
    try:
        urllib.request.urlopen(req, timeout=20).read()
    except Exception as e:
        print(f'[stopped_watchdog] send_alert error: {e}', flush=True)


def main():
    env = load_env(ENV_PATH)
    token = env.get('TELEGRAM_BOT_TOKEN', '').strip()
    chat_id = int(env.get('TELEGRAM_ALLOWED_CHAT_ID', '0'))
    if not token or chat_id == 0:
        return None
    info = check()
    if not info:
        return None
    state = load_state()
    if not should_alert(state):
        return None
    send_alert(token, chat_id, info)
    state['last_alert_ts'] = time.time()
    save_state(state)

if __name__ == '__main__':
    main()
