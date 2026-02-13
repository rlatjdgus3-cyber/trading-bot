# Source Generated with Decompyle++
# File: bybit_position_test.cpython-312.pyc (Python 3.12)

import os
import ccxt
import json
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
positions = exchange.fetch_positions([
    symbol])
print('positions len:', len(positions))
for p in positions:
    print('------------------------------------------------------------')
    print('symbol:', p.get('symbol'))
    print('side:', p.get('side'))
    print('contracts:', p.get('contracts'))
    print('contractSize:', p.get('contractSize'))
    print('entryPrice:', p.get('entryPrice'))
    print('markPrice:', p.get('markPrice'))
    print('unrealizedPnl:', p.get('unrealizedPnl'))
    print('leverage:', p.get('leverage'))
