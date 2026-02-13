# Source Generated with Decompyle++
# File: tpsl_suggest.cpython-312.pyc (Python 3.12)

import os
import sys
import ccxt
from dotenv import load_dotenv
load_dotenv('/root/trading-bot/app/.env')
SYMBOL = 'BTC/USDT:USDT'

def exchange():
    return ccxt.bybit({
        'apiKey': os.getenv('BYBIT_API_KEY'),
        'secret': os.getenv('BYBIT_SECRET'),
        'enableRateLimit': True,
        'options': {
            'defaultType': 'swap'}})


def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None


def main():
    if len(sys.argv) != 3:
        print('usage: python3 tpsl_suggest.py <SL% e.g. -1.0> <TP% e.g. 2.0>')
        return 2
    sl_pct = float(sys.argv[1])
    tp_pct = float(sys.argv[2])
    ex = exchange()
    positions = ex.fetch_positions([
        SYMBOL])
    p = None
    for it in positions:
        if it.get('symbol') == SYMBOL:
            p = it
            break
    if not p:
        print(f'position({SYMBOL}): not found')
        return 1
    side = p.get('side')
    contracts = safe_float(p.get('contracts') or 0)
    entry = safe_float(p.get('entryPrice') or p.get('entry_price') or None)
    if not side or not contracts or contracts == 0 or not entry:
        print(f'position({SYMBOL}): none')
        return 0
    if side == 'long':
        sl = entry * (1 + sl_pct / 100)
        tp = entry * (1 + tp_pct / 100)
    else:
        sl = entry * (1 - sl_pct / 100)
        tp = entry * (1 - tp_pct / 100)
    print('\n'.join([
        '🧭 TP/SL SUGGEST',
        f'- symbol: {SYMBOL}',
        f'- side: {side} qty={contracts}',
        f'- entry: {entry}',
        f'- input: SL%={sl_pct}, TP%={tp_pct}',
        f'- suggested SL price: {sl}',
        f'- suggested TP price: {tp}',
        '⚠️ 아직 자동 설정은 미적용(안전). 원하면 다음 단계에서 Bybit API로 실제 세팅 붙임.']))
    return 0

if __name__ == '__main__':
    sys.exit(main())
