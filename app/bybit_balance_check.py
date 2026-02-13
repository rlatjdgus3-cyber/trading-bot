# Source Generated with Decompyle++
# File: bybit_balance_check.cpython-312.pyc (Python 3.12)

import os
import ccxt
from dotenv import load_dotenv
load_dotenv('/root/trading-bot/app/.env')
BYBIT_API_KEY = os.getenv('BYBIT_API_KEY')
BYBIT_SECRET = os.getenv('BYBIT_SECRET')
exchange = ccxt.bybit({
    'apiKey': BYBIT_API_KEY,
    'secret': BYBIT_SECRET,
    'enableRateLimit': True,
    'options': {
        'defaultType': 'swap' } })
print('=== BYBIT FREE USDT CHECK (NO ORDER) ===')
balance = exchange.fetch_balance()
if not balance.get('USDT'):
    balance.get('USDT')
usdt = { }
print('TOTAL USDT :', usdt.get('total'))
print('FREE  USDT :', usdt.get('free'))
print('USED  USDT :', usdt.get('used'))
print('✅ 주문 없음 / 조회만 수행 완료')
