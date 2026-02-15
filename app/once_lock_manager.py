# Source Generated with Decompyle++
# File: once_lock_manager.cpython-312.pyc (Python 3.12)

import json
import os
import time
import subprocess
from datetime import datetime, timezone, timedelta
from db_config import get_conn
TEST_PATH = '/root/trading-bot/app/test_mode.json'
SYMBOL = 'BTC/USDT:USDT'

def sh(cmd, timeout=30):
    p = subprocess.run([
        'bash',
        '-lc',
        cmd], capture_output=True, text=True, timeout=timeout)
    return (p.returncode, ((p.stdout or '') + (p.stderr or '')).strip())


def read_json(path, default):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return default


def in_test_window(test=None):
    import test_utils
    return test_utils.is_test_active(test)


def has_position():
    (rc, out) = sh('python3 /root/trading-bot/app/status_full.py', timeout=25)
    if rc != 0:
        return False
    if 'position(' in out:
        return 'none' not in out.lower()
    return False


def get_lock_opened_at():
    try:
        conn = get_conn(autocommit=True)
        with conn.cursor() as cur:
            cur.execute("SELECT opened_at FROM public.live_order_once_lock WHERE symbol=%s LIMIT 1;", (SYMBOL,))
            row = cur.fetchone()
        conn.close()
        if not row or not row[0]:
            return None
        return row[0]
    except Exception:
        return None


def delete_lock():
    try:
        conn = get_conn(autocommit=True)
        with conn.cursor() as cur:
            cur.execute("DELETE FROM public.live_order_once_lock WHERE symbol=%s;", (SYMBOL,))
        conn.close()
    except Exception:
        pass


def main():
    test = read_json(TEST_PATH, {
        'enabled': False})
    if not in_test_window(test):
        return None
    cooldown_min = int(os.getenv('ONCE_LOCK_COOLDOWN_MIN', '30'))
    if has_position():
        return None
    opened_at = get_lock_opened_at()
    if not opened_at:
        return None
    now = datetime.now(timezone.utc)
    age = now - opened_at
    age_min = age.total_seconds() / 60
    if age_min < cooldown_min:
        print(f'[once_lock_manager] Lock age={age_min:.1f}m < cooldown={cooldown_min}m, skip', flush=True)
        return None
    print(f'[once_lock_manager] Lock age={age_min:.1f}m >= cooldown={cooldown_min}m, deleting lock', flush=True)
    delete_lock()

if __name__ == '__main__':
    main()
