# Source Generated with Decompyle++
# File: test_lifecycle.cpython-312.pyc (Python 3.12)

'''
test_lifecycle.py — Periodic test lifecycle event checker.

Runs every 5 minutes via systemd timer.
Uses state file to track which events have fired (avoids duplicate sends).

Events:
  T-60m  (pre_close_60m_sent):  Telegram warning + position/budget snapshot
  T-30m  (freeze_30m_sent):     Telegram "Freeze active — no new OPEN/ADD"
  T=0    (test_ended_sent):     Autopilot OFF, Telegram "Test ended", disable test
'''
import os
import sys
import json
import subprocess
import urllib.parse
import urllib.request
sys.path.insert(0, '/root/trading-bot/app')
from datetime import datetime, timezone, timedelta
import test_utils
from db_config import get_conn
from dotenv import load_dotenv
load_dotenv('/root/trading-bot/app/.env')
STATE_PATH = '/root/trading-bot/app/.test_lifecycle_state.json'
ENV_PATH = '/root/trading-bot/app/telegram_cmd.env'
LOG_PREFIX = '[test_lifecycle]'
KST = timezone(timedelta(hours=9))

def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _db_conn():
    return get_conn()


def _load_env(path=None):
    env = {}
    if path is None:
        return env
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, v = line.split('=', 1)
            env[k.strip()] = v.strip()
    return env


def _tg_send(text=None):
    if not text:
        return None
    env = _load_env(ENV_PATH)
    token = env.get('TELEGRAM_BOT_TOKEN', '').strip()
    chat_id = int(env.get('TELEGRAM_ALLOWED_CHAT_ID', '0'))
    if not token or chat_id == 0:
        return None
    if len(text) > 3500:
        text = text[:3500] + '\n…(truncated)'
    url = f'https://api.telegram.org/bot{token}/sendMessage'
    data = urllib.parse.urlencode({
        'chat_id': str(chat_id),
        'text': text,
        'disable_web_page_preview': 'true'}).encode('utf-8')
    req = urllib.request.Request(url, data=data, method='POST')
    try:
        urllib.request.urlopen(req, timeout=20).read()
    except Exception as e:
        _log(f'tg_send error: {e}')
    return None


def _load_state():
    try:
        with open(STATE_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def _save_state(state=None):
    tmp = STATE_PATH + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_PATH)


def _log_event(conn=None, event_type=None, detail=None):
    '''Insert into test_events_log (detail is JSONB).'''
    try:
        import json as _json
        detail_json = _json.dumps({'msg': str(detail or '')}, ensure_ascii=False)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO test_events_log (event_type, detail) VALUES (%s, %s::jsonb);",
                (event_type, detail_json)
            )
        conn.commit()
    except Exception as e:
        _log(f'_log_event error: {e}')


def _get_status_snapshot():
    '''Run status_full.py and return output.'''
    try:
        p = subprocess.run([
            'python3',
            '/root/trading-bot/app/status_full.py'], capture_output=True, text=True, timeout=25)
        return (p.stdout or '') + (p.stderr or '')
    except Exception as e:
        return f'status_full error: {e}'


def _set_autopilot_off(conn):
    '''Disable autopilot via DB.'''
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE autopilot_config SET enabled = false WHERE enabled = true;")
        conn.commit()
    except Exception as e:
        _log(f'_set_autopilot_off error: {e}')


def _disable_test_mode():
    '''Set test_mode.json enabled: false.'''
    test = test_utils.load_test_mode()
    test['enabled'] = False
    tmp = test_utils.TEST_MODE_PATH + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(test, f, ensure_ascii=False, indent=2)
    os.replace(tmp, test_utils.TEST_MODE_PATH)


def main():
    test = test_utils.load_test_mode()
    if not test.get('enabled'):
        _log('Test not enabled, skipping')
        return None
    end = test_utils.get_end_utc(test)
    if not end:
        _log('No end time parseable, skipping')
        return None
    now = datetime.now(timezone.utc)
    remaining = end - now
    remaining_min = remaining.total_seconds() / 60
    state = _load_state()
    _log(f'remaining={remaining_min:.1f}m')

    conn = _db_conn()
    conn.autocommit = True

    try:
        # T-60m event
        if remaining_min <= 60 and not state.get('pre_close_60m_sent'):
            snapshot = _get_status_snapshot()
            msg = f'⏰ 테스트 종료 60분 전\n남은 시간: {remaining_min:.0f}분\n\n{snapshot}'
            _tg_send(msg)
            _log_event(conn, 'pre_close_60m', msg[:500])
            state['pre_close_60m_sent'] = True
            _save_state(state)

        # T-30m event (freeze)
        if remaining_min <= 30 and not state.get('freeze_30m_sent'):
            msg = f'🧊 Freeze 활성화 — 신규 OPEN/ADD 차단\n남은 시간: {remaining_min:.0f}분'
            _tg_send(msg)
            _log_event(conn, 'freeze_30m', msg)
            state['freeze_30m_sent'] = True
            _save_state(state)

        # T=0 event (test ended)
        if remaining_min <= 0 and not state.get('test_ended_sent'):
            _set_autopilot_off(conn)
            _disable_test_mode()
            snapshot = _get_status_snapshot()
            msg = f'🏁 테스트 종료\nAutopilot OFF, test_mode disabled\n\n{snapshot}'
            _tg_send(msg)
            _log_event(conn, 'test_ended', msg[:500])
            state['test_ended_sent'] = True
            _save_state(state)
    finally:
        conn.close()

if __name__ == '__main__':
    main()
