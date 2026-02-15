"""
error_watcher.py — Monitors systemd service logs for errors and sends Telegram alerts.
"""
import os
import re
import json
import time
import subprocess
import traceback
import urllib.parse
import urllib.request

ENV_PATH = '/root/trading-bot/app/telegram_cmd.env'
WATCH_UNITS = [
    'candles.service',
    'indicators.service',
    'vol_profile.service',
    'news_bot.service',
    'fill_watcher.service',
    'live_event_detector.service',
    'live_order_executor.service',
    'macro_collector.service',
    'order_executor.service',
    'pnl_watcher.service',
    'position_watcher.service',
    'position_manager.service',
    'dry_run_close_executor.service',
    'autopilot.service',
]
IGNORE_PATTERNS = [
    re.compile(r'executor\s+STOPPED', re.IGNORECASE),
    re.compile(r'empty-heartbeat-file', re.IGNORECASE)]
ERROR_PATTERNS = [
    re.compile(r'\bTraceback\b'),
    re.compile(r'\bException\b'),
    re.compile(r'\bERROR\b'),
    re.compile(r'\bCRITICAL\b'),
    re.compile(r'\bFATAL\b'),
    re.compile(r'\bfailed\b', re.IGNORECASE),
    re.compile(r'\bpanic\b', re.IGNORECASE)]
STATE_FILE = '/root/trading-bot/app/.error_watcher_state.json'
MIN_ALERT_INTERVAL_SEC = 60


def load_env(path=None):
    env = {}
    try:
        with open(path or ENV_PATH) as f:
            for line in f:
                line = line.strip()
                if '=' in line and not line.startswith('#'):
                    k, v = line.split('=', 1)
                    env[k.strip()] = v.strip()
    except Exception:
        pass
    return env


def tg_api_call(token=None, method=None, params=None):
    url = f'https://api.telegram.org/bot{token}/{method}'
    data = urllib.parse.urlencode(params).encode('utf-8')
    req = urllib.request.Request(url, data=data, method='POST')
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception:
        pass


def send_message(token=None, chat_id=None, text=None):
    try:
        from report_formatter import korean_output_guard
        text = korean_output_guard(text or '')
    except Exception:
        pass
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
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def write_state(state=None):
    tmp = STATE_FILE + '.tmp'
    try:
        with open(tmp, 'w') as f:
            json.dump(state, f)
        os.replace(tmp, STATE_FILE)
    except Exception:
        pass


def looks_like_error(line=None):
    for pat in IGNORE_PATTERNS:
        if pat.search(line):
            return False
    for pat in ERROR_PATTERNS:
        if pat.search(line):
            return True
    return False


def fingerprint(text=None):
    t = text.strip()
    if len(t) > 800:
        t = t[:400] + ' ... ' + t[-400:]
    return str(hash(t))


def main():
    env = load_env()
    token = env.get('TELEGRAM_BOT_TOKEN', '')
    chat_id = env.get('TELEGRAM_ALLOWED_CHAT_ID', '')
    if not token or not chat_id:
        print('[error_watcher] No telegram config, exiting', flush=True)
        return

    state = read_state()
    now = time.time()

    for unit in WATCH_UNITS:
        try:
            result = subprocess.run(
                ['journalctl', '-u', unit, '--since', '2 minutes ago', '--no-pager', '-q'],
                capture_output=True, text=True, timeout=10)
            lines = result.stdout.strip().split('\n')
        except Exception:
            continue

        errors = []
        for line in lines:
            if looks_like_error(line):
                fp = fingerprint(line)
                last_alert = state.get(fp, 0)
                if now - last_alert >= MIN_ALERT_INTERVAL_SEC:
                    errors.append(line.strip())
                    state[fp] = now

        if errors:
            msg = f'[error_watcher] {unit}\n' + '\n'.join(errors[:5])
            send_message(token, chat_id, msg)

    write_state(state)


if __name__ == '__main__':
    main()
