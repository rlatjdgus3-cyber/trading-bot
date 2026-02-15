# Source Generated with Decompyle++
# File: main.cpython-312.pyc (Python 3.12)

import os
import time
import ccxt
from dotenv import load_dotenv
from db_config import get_conn
load_dotenv()
exchange = ccxt.bybit({
    'apiKey': os.getenv('BYBIT_API_KEY'),
    'secret': os.getenv('BYBIT_SECRET'),
    'enableRateLimit': True,
    'options': {
        'defaultType': 'swap' } })
db = get_conn()
symbol = 'BTC/USDT:USDT'
print('=== SIGNAL LOGGER STARTED ===')
ticker = exchange.fetch_ticker(symbol)
price = float(ticker['last'])
note = 'price snapshot'
# WARNING: Decompyle incomplete
