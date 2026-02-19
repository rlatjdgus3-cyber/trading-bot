"""
ctx_utils.py — Shared utilities for market_context service.

- Logging with [mctx] prefix
- Telegram messaging via raw urllib
- ExponentialBackoff for error handling
- Environment loading
"""
import os
import time
import urllib.parse
import urllib.request

LOG_PREFIX = '[mctx]'
_ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def load_env():
    """Load .env file into os.environ (setdefault, won't overwrite)."""
    if os.path.isfile(_ENV_PATH):
        with open(_ENV_PATH, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                k, v = line.split('=', 1)
                os.environ.setdefault(k.strip(), v.strip())


def send_telegram(text, prefix='[MCTX]'):
    """Send text to Telegram via raw urllib. Chunks at 3800 chars."""
    token = os.getenv('TELEGRAM_BOT_TOKEN', '')
    chat_id = os.getenv('TELEGRAM_ALLOWED_CHAT_ID', '')
    if not token or not chat_id:
        return
    full = f'{prefix} {text}' if prefix else text
    chunks = []
    while len(full) > 3800:
        chunks.append(full[:3800])
        full = full[3800:]
    chunks.append(full)
    for chunk in chunks:
        try:
            url = f'https://api.telegram.org/bot{token}/sendMessage'
            data = urllib.parse.urlencode({
                'chat_id': chat_id,
                'text': chunk,
                'disable_web_page_preview': 'true',
            }).encode('utf-8')
            req = urllib.request.Request(url, data=data, method='POST')
            with urllib.request.urlopen(req, timeout=15) as resp:
                resp.read()
        except Exception as e:
            _log(f'send_telegram error: {e}')


class ExponentialBackoff:
    """Exponential backoff: 5s -> 10s -> 20s -> 40s -> 120s max.
    Alerts via Telegram after 3 consecutive failures."""

    def __init__(self, base=5, max_delay=120, alert_after=3):
        self.base = base
        self.max_delay = max_delay
        self.alert_after = alert_after
        self.failures = 0
        self.last_alert_failures = 0

    def fail(self, error_msg=''):
        self.failures += 1
        delay = min(self.base * (2 ** (self.failures - 1)), self.max_delay)
        _log(f'backoff: failure #{self.failures}, sleeping {delay}s — {error_msg}')
        if self.failures >= self.alert_after and self.failures > self.last_alert_failures:
            send_telegram(
                f'Collector error #{self.failures}: {error_msg[:200]}',
                prefix='[MCTX ALERT]'
            )
            self.last_alert_failures = self.failures
        time.sleep(delay)

    def success(self):
        self.failures = 0
