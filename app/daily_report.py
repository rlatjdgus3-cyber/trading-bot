#!/usr/bin/env python3
import os
from decimal import Decimal
from dotenv import load_dotenv
import ccxt
from db_config import get_conn
SYMBOL = os.getenv("SYMBOL", "BTC/USDT:USDT")
DAY_DD = Decimal(os.getenv("EQUITY_GUARD_DAY_DD", "0.03"))
TOTAL_DD = Decimal(os.getenv("EQUITY_GUARD_TOTAL_DD", "0.06"))

def q1(cur, sql, params=()):
    cur.execute(sql, params)
    return cur.fetchone()

def qall(cur, sql, params=()):
    cur.execute(sql, params)
    return cur.fetchall()

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
    px = now_price(ex)

    conn = get_conn(autocommit=True)
    with conn.cursor() as cur:
        cash_row = q1(cur, "SELECT capital_usdt FROM public.virtual_capital ORDER BY id DESC LIMIT 1;")
        cash = Decimal(str(cash_row[0])) if cash_row else Decimal('0')

        pos_row = q1(cur, "SELECT side, qty, avg_entry FROM public.dry_run_positions WHERE symbol=%s LIMIT 1;", (SYMBOL,))
        if pos_row:
            side = pos_row[0]
            qty = Decimal(str(pos_row[1]))
            entry = Decimal(str(pos_row[2]))
        else:
            side = None
            qty = Decimal('0')
            entry = None

        pos_val = (qty * px).quantize(Decimal('0.000001'))
        equity = (cash + pos_val).quantize(Decimal('0.000001'))

        ts_row = q1(cur, "SELECT enabled, updated_at FROM public.trade_switch ORDER BY id DESC LIMIT 1;")
        trade_switch = bool(ts_row[0]) if ts_row else False
        ts_updated = str(ts_row[1]) if ts_row else None

        eg_row = q1(cur, "SELECT start_equity, updated_at FROM public.equity_guard_state WHERE symbol=%s;", (SYMBOL,))
        if eg_row:
            start_eq = Decimal(str(eg_row[0]))
            eg_updated = str(eg_row[1])
        else:
            start_eq = None
            eg_updated = None

        day_floor = (start_eq * (Decimal('1') - DAY_DD)).quantize(Decimal('0.000001')) if start_eq else None
        total_floor = (start_eq * (Decimal('1') - TOTAL_DD)).quantize(Decimal('0.000001')) if start_eq else None

        unreal = Decimal('0')
        if side and qty > 0 and entry is not None:
            unreal = ((px - entry) * qty) if side == 'LONG' else ((entry - px) * qty)
            unreal = unreal.quantize(Decimal('0.000001'))

        fills = qall(cur, """
            SELECT id, ts, action, side, price, usdt, qty, reason
            FROM public.dry_run_fills
            WHERE symbol=%s
            ORDER BY id DESC
            LIMIT 10;
        """, (SYMBOL,))

    conn.close()

    print('=== DAILY REPORT (DRY_RUN) ===')
    print('symbol:', SYMBOL)
    print('price_now:', px)
    print('trade_switch:', trade_switch, '| updated_at:', ts_updated)
    print('cash_usdt:', cash)
    print('position:', {'side': side, 'qty': str(qty), 'avg_entry': (str(entry) if entry is not None else None)})
    print('position_value_usdt:', pos_val)
    print('unrealized_pnl_usdt:', unreal)
    print('equity_usdt:', equity)
    print('equity_guard.start_equity:', (str(start_eq) if start_eq is not None else None), '| updated_at:', eg_updated)
    print('equity_guard.day_floor:', (str(day_floor) if day_floor is not None else None))
    print('equity_guard.total_floor:', (str(total_floor) if total_floor is not None else None))
    print('')
    print('--- recent fills (last 10) ---')
    for r in fills:
        rid, ts, act, s, price, usdt, q, reason = r
        print(f"#{rid} {ts} {act} {s} price={price} usdt={usdt} qty={q} reason={reason}")

if __name__ == '__main__':
    main()
