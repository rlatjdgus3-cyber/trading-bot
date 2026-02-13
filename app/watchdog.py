# Source Generated with Decompyle++
# File: watchdog.cpython-312.pyc (Python 3.12)

import time
import subprocess
import psycopg2
db = psycopg2.connect(host = 'localhost', dbname = 'trading', user = 'bot', password = 'botpass')
SERVICES = {
    'price_signals': 'python3 -u /root/trading-bot/app/main.py',
    'candles': 'python3 -u /root/trading-bot/app/candles.py',
    'indicators': 'python3 -u /root/trading-bot/app/indicators.py',
    'vol_profile': 'python3 -u /root/trading-bot/app/vol_profile.py',
    'news_bot': 'python3 -u /root/trading-bot/app/news_bot.py',
    'dispatcher': 'python3 -u /root/trading-bot/app/dispatcher.py' }
LOGS = {
    'price_signals': '/root/trading-bot/app/log.txt',
    'candles': '/root/trading-bot/app/candles.log',
    'indicators': '/root/trading-bot/app/indicators.log',
    'vol_profile': '/root/trading-bot/app/vol_profile.log',
    'news_bot': '/root/trading-bot/app/news.log',
    'dispatcher': '/root/trading-bot/app/dispatcher.log' }

def is_running(cmd = None):
    p = subprocess.run([
        'bash',
        '-lc',
        f'''ps -ef | grep -F \'{cmd}\' | grep -v grep'''], capture_output = True, text = True)
    return p.returncode == 0


def start_service(name = None, cmd = None):
    log = LOGS.get(name, f'''/root/trading-bot/app/{name}.log''')
    subprocess.run([
        'bash',
        '-lc',
        f'''nohup {cmd} > {log} 2>&1 &'''], check = False)


def record(service = None, status = None, detail = None):
    pass
# WARNING: Decompyle incomplete

print('=== WATCHDOG STARTED ===')
# WARNING: Decompyle incomplete
