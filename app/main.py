# Source Generated with Decompyle++
# File: main.cpython-312.pyc (Python 3.12)

import os
import time
import ccxt
import psycopg2
from dotenv import load_dotenv
load_dotenv()
exchange = ccxt.bybit({
    'apiKey': os.getenv('BYBIT_API_KEY'),
    'secret': os.getenv('BYBIT_SECRET'),
    'enableRateLimit': True,
    'options': {
        'defaultType': 'swap' } })
db = psycopg2.connect(host = 'localhost', dbname = 'trading', user = 'bot', password = 'botpass', connect_timeout = 10, options = '-c statement_timeout=30000')
symbol = 'BTC/USDT:USDT'
print('=== SIGNAL LOGGER STARTED ===')
ticker = exchange.fetch_ticker(symbol)
price = float(ticker['last'])
note = 'price snapshot'
# WARNING: Decompyle incomplete
