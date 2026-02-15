"""
error_watcher.py — Monitors systemd service logs for errors and sends Telegram alerts.
- 동일 에러 5분 dedup (fingerprint에서 타임스탬프 제거)
- 서비스별 에러 요약 1건으로 묶어서 발송
- traceback 전문은 로그에만, 텔레그램엔 핵심 원인 1줄만
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
    re.compile(r'empty-heartbeat-file', re.IGNORECASE),
    re.compile(r'DB 재연결 성공', re.IGNORECASE),
    re.compile(r'DB reconnected', re.IGNORECASE),
]
ERROR_PATTERNS = [
    re.compile(r'\bTraceback\b'),
    re.compile(r'\bException\b'),
    re.compile(r'\bERROR\b'),
    re.compile(r'\bCRITICAL\b'),
    re.compile(r'\bFATAL\b'),
    re.compile(r'\bfailed\b', re.IGNORECASE),
    re.compile(r'\bpanic\b', re.IGNORECASE)]
STATE_FILE = '/root/trading-bot/app/.error_watcher_state.json'
MIN_ALERT_INTERVAL_SEC = 300  # 5분 dedup (동일 에러 반복 스팸 방지)

# journalctl 타임스탬프 패턴 (Feb 15 03:10:04 hostname ...)
_TS_PREFIX_RE = re.compile(r'^[A-Z][a-z]{2}\s+\d+\s+\d+:\d+:\d+\s+\S+\s+')


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


def _clean_old_state(state):
    """7일 이상 된 fingerprint 제거 (state 비대화 방지)."""
    now = time.time()
    cutoff = now - 7 * 86400
    return {k: v for k, v in state.items() if isinstance(v, (int, float)) and v > cutoff}


def looks_like_error(line=None):
    for pat in IGNORE_PATTERNS:
        if pat.search(line):
            return False
    for pat in ERROR_PATTERNS:
        if pat.search(line):
            return True
    return False


def _strip_timestamp(line):
    """journalctl 타임스탬프 + hostname 접두사 제거 → 순수 내용만 추출."""
    return _TS_PREFIX_RE.sub('', line).strip()


def _extract_root_cause(line):
    """traceback/에러 라인에서 핵심 원인 1줄 추출."""
    stripped = _strip_timestamp(line)
    # "psycopg2.InterfaceError: connection already closed" 같은 형태
    if ':' in stripped:
        # 프로세스 ID 부분 제거 (python3[12345]: ...)
        m = re.match(r'\S+\[\d+\]:\s*(.*)', stripped)
        if m:
            return m.group(1).strip()
    return stripped


def fingerprint(text=None):
    """타임스탬프 제거 후 해시 → 동일 에러 올바르게 dedup."""
    t = _strip_timestamp(text or '')
    # 프로세스 ID도 제거 (python3[12345])
    t = re.sub(r'\[\d+\]', '[PID]', t)
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
    state = _clean_old_state(state)
    now = time.time()

    for unit in WATCH_UNITS:
        try:
            result = subprocess.run(
                ['journalctl', '-u', unit, '--since', '2 minutes ago', '--no-pager', '-q'],
                capture_output=True, text=True, timeout=10)
            lines = result.stdout.strip().split('\n')
        except Exception:
            continue

        error_causes = []
        seen_fps = set()
        for line in lines:
            if not looks_like_error(line):
                continue
            # Traceback 줄 자체는 건너뛰고, 실제 에러 메시지만 수집
            stripped = _strip_timestamp(line)
            if re.match(r'\S+\[\d+\]:\s*Traceback', stripped):
                continue
            if re.match(r'\S+\[\d+\]:\s*File\s+"', stripped):
                continue
            if re.match(r'\S+\[\d+\]:\s+\^', stripped):
                continue

            fp = fingerprint(line)
            if fp in seen_fps:
                continue
            seen_fps.add(fp)

            last_alert = state.get(fp, 0)
            if now - last_alert >= MIN_ALERT_INTERVAL_SEC:
                cause = _extract_root_cause(line)
                if cause:
                    error_causes.append(cause)
                    state[fp] = now

        if error_causes:
            svc_name = unit.replace('.service', '')
            # 핵심 원인만 최대 3줄, 중복 제거
            unique_causes = list(dict.fromkeys(error_causes))[:3]
            suppressed = len(error_causes) - len(unique_causes)
            cause_text = '\n'.join(f"  • {c[:200]}" for c in unique_causes)
            msg = f"🚨 {svc_name} 장애 감지\n{cause_text}"
            if suppressed > 0:
                msg += f"\n  (외 {suppressed}건 동일 에러 생략)"
            send_message(token, chat_id, msg)

    write_state(state)


if __name__ == '__main__':
    main()
