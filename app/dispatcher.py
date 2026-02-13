# Source Generated with Decompyle++
# File: dispatcher.cpython-312.pyc (Python 3.12)

import json
import time
import psycopg2
db = psycopg2.connect(host = 'localhost', dbname = 'trading', user = 'bot', password = 'botpass')
symbol = 'BTC/USDT:USDT'
tf = '1m'
NEWS_THRESHOLD = 6
PRICE_MOVE_PCT = 0.8
COOLDOWN_SEC = 300
last_news_id = 0
last_alert_ts = {
    'news': 0,
    'move': 0 }

def now():
    return time.time()

print('=== DISPATCHER STARTED ===')
# WARNING: Decompyle incomplete
