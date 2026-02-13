# Source Generated with Decompyle++
# File: auto_mode_manager.cpython-312.pyc (Python 3.12)

import os
import json
import time
import subprocess
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
ENV_PATH = '/root/trading-bot/app/telegram_cmd.env'
TEST_PATH = '/root/trading-bot/app/test_mode.json'
GUIDE_PATH = '/root/trading-bot/app/operator_guidance.json'
AUDIT_LOG = '/root/trading-bot/app/audit_auto_mode.log'

def _now_utc():
    return datetime.now(timezone.utc)


def log(s):
    ts = _now_utc().strftime('%Y-%m-%d %H:%M:%S UTC')
    print(f'{ts} [auto_mode] {s}', flush=True)


def load_env(path):
    env = {}
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, v = line.split('=', 1)
            env[k.strip()] = v.strip()
    return env


def tg_send(text):
    env = load_env(ENV_PATH)
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
    urllib.request.urlopen(req, timeout=20).read()


def run(cmd, timeout=30):
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    out = ((p.stdout or '') + (p.stderr or '')).strip()
    return (p.returncode, out)


def read_json(path, default):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return default


def parse_status_full(text):
    d = {
        'live_trading': None,
        'trade_switch': None,
        'once_lock_rows': None,
        'position_none': None}
    for line in (text or '').splitlines():
        line = line.strip()
        if line.startswith('- LIVE_TRADING:'):
            d['live_trading'] = line.split(':', 1)[1].strip()
        if line.startswith('- trade_switch:'):
            d['trade_switch'] = line.split(':', 1)[1].strip()
        if line.startswith('- once_lock rows:'):
            d['once_lock_rows'] = int(line.split(':', 1)[1].strip().split()[0])
        if line.startswith('- position('):
            d['position_none'] = 'none' in line.lower()
    return d


def set_env_kv(key, value):
    env_file = '/root/trading-bot/app/.env'
    if not os.path.exists(env_file):
        open(env_file, 'a', encoding='utf-8').close()
    (rc, _) = run([
        'bash',
        '-lc',
        f"grep -q '^{key}=' {env_file}"])
    if rc == 0:
        run([
            'bash',
            '-lc',
            f"sed -i 's/^{key}=.*/{key}={value}/' {env_file}"])
        return None
    run([
        'bash',
        '-lc',
        f"echo '{key}={value}' >> {env_file}"])


def restart_core():
    run([
        'bash',
        '-lc',
        'systemctl restart executor'], timeout=40)
    run([
        'bash',
        '-lc',
        'systemctl restart order_executor 2>/dev/null || true'], timeout=40)


def main():
    import test_utils
    test = read_json(TEST_PATH, {
        'enabled': False})
    if not test.get('enabled'):
        return None
    end_dt = test_utils.get_end_utc(test)
    if not end_dt:
        log('No end_utc parseable, skip')
        return None
    now = _now_utc()
    if now >= end_dt:
        log('Test ended, skip')
        return None
    remaining = end_dt - now
    log(f'Test active, remaining={remaining}')
    # Check status and perform auto-mode actions
    (rc, out) = run(['python3', '/root/trading-bot/app/status_full.py'], timeout=25)
    if rc != 0:
        log(f'status_full failed rc={rc}')
        return None
    status = parse_status_full(out)
    log(f'status={status}')

if __name__ == '__main__':
    main()
