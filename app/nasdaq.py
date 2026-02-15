"""
nasdaq.py — NASDAQ proxy logger (QQQ via AlphaVantage).
Fetches QQQ 1min close prices and stores in nasdaq_proxy table.
"""
import os
import time
import requests
import psycopg2
from dotenv import load_dotenv
from db_config import get_conn

load_dotenv()

KEY = os.getenv('ALPHAVANTAGE_API_KEY')
URL = 'https://www.alphavantage.co/query'
SYMBOL = 'QQQ'
PARAMS = {
    'function': 'TIME_SERIES_INTRADAY',
    'symbol': SYMBOL,
    'interval': '1min',
    'apikey': KEY,
    'outputsize': 'compact'}

print(f'=== NASDAQ PROXY LOGGER STARTED ({SYMBOL}) ===', flush=True)

last_ts = None
if not KEY:
    raise RuntimeError('ALPHAVANTAGE_API_KEY is missing')

db = get_conn()

while True:
    try:
        r = requests.get(URL, params=PARAMS, timeout=15)
        r.raise_for_status()
        j = r.json()
        if 'Error Message' in j:
            raise RuntimeError(j['Error Message'])
        if 'Note' in j:
            print('Rate limited:', j['Note'][:120], flush=True)
            time.sleep(70)
            continue
        series = j.get('Time Series (1min)')
        if not series:
            print('No series keys:', list(j.keys())[:10], flush=True)
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
            print(f'{ts} {SYMBOL} close={close}', flush=True)
            last_ts = ts
        time.sleep(60)
    except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
        print(f'DB connection lost: {e}', flush=True)
        try:
            db.close()
        except Exception:
            pass
        try:
            db = get_conn()
            print('DB reconnected', flush=True)
        except Exception as re:
            print(f'DB reconnect failed: {re}', flush=True)
        time.sleep(10)
    except Exception as e:
        print(f'Error: {e}', flush=True)
        time.sleep(60)
