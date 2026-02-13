# Source Generated with Decompyle++
# File: bybit_test.cpython-312.pyc (Python 3.12)

import os
import ccxt
from dotenv import load_dotenv
load_dotenv('/root/trading-bot/app/.env')
API_KEY = os.getenv('BYBIT_API_KEY')
API_SECRET = os.getenv('BYBIT_SECRET')
exchange = ccxt.bybit({
    'apiKey': API_KEY,
    'secret': API_SECRET,
    'enableRateLimit': True })
balance = exchange.fetch_balance()
print(balance)
