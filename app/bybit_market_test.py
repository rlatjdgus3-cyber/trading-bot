# Source Generated with Decompyle++
# File: bybit_market_test.cpython-312.pyc (Python 3.12)

import os
import ccxt
from dotenv import load_dotenv
load_dotenv('/root/trading-bot/app/.env')
API_KEY = os.getenv('BYBIT_API_KEY')
API_SECRET = os.getenv('BYBIT_SECRET')
symbol = 'BTC/USDT:USDT'
exchange = ccxt.bybit({
    'apiKey': API_KEY,
    'secret': API_SECRET,
    'enableRateLimit': True,
    'options': {
        'defaultType': 'linear' } })
exchange.load_markets()
m = exchange.market(symbol)
print('symbol:', symbol)
print('type:', m.get('type'), 'spot/linear/inverse 등')
print('precision:', m.get('precision'))
print('limits.amount:', m.get('limits', { }).get('amount'))
print('limits.cost:', m.get('limits', { }).get('cost'))
print('contractSize:', m.get('contractSize'))
