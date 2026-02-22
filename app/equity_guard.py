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

def ensure_tables(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS public.equity_guard_state (
      symbol TEXT PRIMARY KEY,
      start_equity NUMERIC NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """)
    # trade_switch: 구버전( reason 컬럼 없음 ) 기준
    cur.execute("""
    CREATE TABLE IF NOT EXISTS public.trade_switch (
      id BIGSERIAL PRIMARY KEY,
      enabled BOOLEAN NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """)
    row = q1(cur, "SELECT enabled FROM public.trade_switch ORDER BY id DESC LIMIT 1;")
    if row is None:
        cur.execute("INSERT INTO public.trade_switch(enabled) VALUES (true);")  # default ON

def get_trade_switch(cur) -> bool:
    row = q1(cur, "SELECT enabled FROM public.trade_switch ORDER BY id DESC LIMIT 1;")
    return bool(row[0]) if row else False

def set_trade_switch(cur, enabled: bool, off_reason=None):
    if not enabled and off_reason:
        import trade_switch_recovery
        trade_switch_recovery.set_off_with_reason(cur, off_reason, changed_by='equity_guard')
    elif enabled:
        import trade_switch_recovery
        trade_switch_recovery.set_on(cur, changed_by='equity_guard')
    else:
        cur.execute("INSERT INTO public.trade_switch(enabled) VALUES (%s);", (enabled,))

def get_cash(cur) -> Decimal:
    row = q1(cur, "SELECT capital_usdt FROM public.virtual_capital ORDER BY id DESC LIMIT 1;")
    return Decimal(str(row[0])) if row else Decimal('0')

def get_pos(cur):
    row = q1(cur, "SELECT qty FROM public.dry_run_positions WHERE symbol=%s LIMIT 1;", (SYMBOL,))
    if not row:
        return Decimal('0')
    return Decimal(str(row[0]))

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

def get_or_seed_start(cur, equity_now: Decimal) -> Decimal:
    row = q1(cur, "SELECT start_equity FROM public.equity_guard_state WHERE symbol=%s;", (SYMBOL,))
    if row:
        return Decimal(str(row[0]))
    cur.execute("INSERT INTO public.equity_guard_state(symbol, start_equity) VALUES (%s,%s);", (SYMBOL, str(equity_now)))
    return equity_now

def main():
    ex = make_exchange()
    conn = get_conn(autocommit=True)

    with conn.cursor() as cur:
        ensure_tables(cur)

        cash = get_cash(cur)
        qty = get_pos(cur)
        px = now_price(ex)

        pos_val = (qty * px).quantize(Decimal('0.000001'))
        equity = (cash + pos_val).quantize(Decimal('0.000001'))

        start_eq = get_or_seed_start(cur, equity)
        day_floor = (start_eq * (Decimal('1') - DAY_DD)).quantize(Decimal('0.000001'))
        total_floor = (start_eq * (Decimal('1') - TOTAL_DD)).quantize(Decimal('0.000001'))

        ts = get_trade_switch(cur)

        if ts and equity <= total_floor:
            set_trade_switch(cur, False, off_reason='equity_drawdown')
            print('[equity_guard] SWITCH OFF TOTAL_DD', 'equity=', equity, 'floor=', total_floor, flush=True)
        elif ts and equity <= day_floor:
            set_trade_switch(cur, False, off_reason='equity_drawdown')
            print('[equity_guard] SWITCH OFF DAY_DD', 'equity=', equity, 'floor=', day_floor, flush=True)
        else:
            print('[equity_guard] OK', 'equity=', equity, 'start=', start_eq, 'day_floor=', day_floor, 'total_floor=', total_floor, 'trade_switch=', ts, flush=True)

    conn.close()

if __name__ == '__main__':
    main()
