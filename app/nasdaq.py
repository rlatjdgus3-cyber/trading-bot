# Source Generated with Decompyle++
# File: nasdaq.cpython-312.pyc (Python 3.12)

import os
import time
import requests
import psycopg2
from dotenv import load_dotenv
load_dotenv()
db = psycopg2.connect(host='localhost', dbname='trading', user='bot', password='botpass', connect_timeout=10, options='-c statement_timeout=30000')
KEY = os.getenv('ALPHAVANTAGE_API_KEY')
URL = 'https://www.alphavantage.co/query'
SYMBOL = 'QQQ'
PARAMS = {
    'function': 'TIME_SERIES_INTRADAY',
    'symbol': SYMBOL,
    'interval': '1min',
    'apikey': KEY,
    'outputsize': 'compact'}
print(f'=== NASDAQ PROXY LOGGER STARTED ({SYMBOL}) ===')
last_ts = None
if not KEY:
    raise RuntimeError('ALPHAVANTAGE_API_KEY is missing')
while True:
    try:
        r = requests.get(URL, params=PARAMS, timeout=15)
        r.raise_for_status()
        j = r.json()
        if 'Error Message' in j:
            raise RuntimeError(j['Error Message'])
        if 'Note' in j:
            print('Rate limited:', j['Note'][:120])
            time.sleep(70)
            continue
        series = j.get('Time Series (1min)')
        if not series:
            print('No series keys:', list(j.keys())[:10])
            time.sleep(60)
            continue
        ts = max(series.keys())
        close = float(series[ts]['4. close'])
        if ts != last_ts:
            with db.cursor() as cur:
                cur.execute(
                    "INSERT INTO nasdaq_proxy (ts, symbol, close) VALUES (%s, %s, %s) "
                    "ON CONFLICT (ts, symbol) DO NOTHING;",
                    (ts, SYMBOL, close)
                )
            db.commit()
            print(f'{ts} {SYMBOL} close={close}')
            last_ts = ts
        time.sleep(60)
    except Exception as e:
        print(f'Error: {e}')
        time.sleep(60)
