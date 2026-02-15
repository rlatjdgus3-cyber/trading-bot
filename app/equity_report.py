#!/usr/bin/env python3
import os
import json
from decimal import Decimal
from dotenv import load_dotenv
import ccxt
from db_config import get_conn
SYMBOL = os.getenv("SYMBOL", "BTC/USDT:USDT")

def q1(cur, sql, params=()):
    cur.execute(sql, params)
    return cur.fetchone()

def get_cash(cur) -> Decimal:
    row = q1(cur, "SELECT capital_usdt FROM public.virtual_capital ORDER BY id DESC LIMIT 1;")
    return Decimal(str(row[0])) if row else Decimal('0')

def get_pos(cur):
    row = q1(cur, "SELECT side, qty, avg_entry, meta FROM public.dry_run_positions WHERE symbol=%s LIMIT 1;", (SYMBOL,))
    if not row:
        return None
    meta = row[3] or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except Exception:
            meta = {}
    return {"side": row[0], "qty": Decimal(str(row[1])), "avg_entry": Decimal(str(row[2])), "meta": meta}

def make_exchange():
    load_dotenv('/root/trading-bot/app/.env')
    key = os.getenv('BYBIT_API_KEY')
    sec = os.getenv('BYBIT_SECRET')
    if not key or not sec:
        raise RuntimeError('BYBIT_API_KEY/BYBIT_SECRET missing in /root/trading-bot/app/.env')
    ex = ccxt.bybit({
        "apiKey": key,
        "secret": sec,
        "enableRateLimit": True,
        "options": {"defaultType": "swap"},
    })
    ex.load_markets()
    return ex

def now_price(ex) -> Decimal:
    t = ex.fetch_ticker(SYMBOL)
    last = t.get('last') or t.get('close')
    if last:
        return Decimal(str(last))
    info = t.get('info') or {}
    for k in ['markPrice','mark_price','lastPrice','last_price','indexPrice']:
        v = info.get(k)
        if v:
            return Decimal(str(v))
    raise RuntimeError('ticker has no usable price')

def main():
    ex = make_exchange()
    conn = get_conn(autocommit=True)
    with conn.cursor() as cur:
        cash = get_cash(cur)
        pos = get_pos(cur)
    conn.close()

    px = now_price(ex)

    pos_qty = pos['qty'] if pos else Decimal('0')
    pos_val = (pos_qty * px).quantize(Decimal('0.000001'))
    equity = (cash + pos_val).quantize(Decimal('0.000001'))

    unreal = Decimal('0')
    if pos and pos_qty > 0:
        entry = pos['avg_entry']
        if pos['side'] == 'LONG':
            unreal = ((px - entry) * pos_qty).quantize(Decimal('0.000001'))
        else:
            unreal = ((entry - px) * pos_qty).quantize(Decimal('0.000001'))

    print('=== EQUITY REPORT (DRY_RUN, A-Model) ===')
    print('symbol:', SYMBOL)
    print('price_now:', px)
    print('cash_usdt:', cash)
    print('pos_side:', pos['side'] if pos else None)
    print('pos_qty:', pos_qty)
    print('avg_entry:', pos['avg_entry'] if pos else None)
    print('position_value_usdt:', pos_val)
    print('unrealized_pnl_usdt:', unreal)
    print('equity_usdt:', equity)

if __name__ == '__main__':
    main()
