#!/usr/bin/env python3
import os
import time
import json
from decimal import Decimal
from db_config import get_conn

SYMBOL = os.getenv('SYMBOL', 'BTC/USDT:USDT')
DRY_RUN = os.getenv('DRY_RUN', '1') == '1'

BASE_RATIO = Decimal('0.70')
DCA_RATIO  = Decimal('0.30')

POLL_SEC = int(os.getenv('ORDER_EXECUTOR_POLL_SEC', '3'))

def q1(cur, sql, params=()):
    cur.execute(sql, params)
    return cur.fetchone()

def qall(cur, sql, params=()):
    cur.execute(sql, params)
    return cur.fetchall()

def now_price(_cur=None):
    # price source: Bybit CCXT (no DB candles required)
    from dotenv import load_dotenv
    import ccxt

    load_dotenv('/root/trading-bot/app/.env')

    key = os.getenv("BYBIT_API_KEY")
    sec = os.getenv("BYBIT_SECRET")
    if not key or not sec:
        raise RuntimeError("BYBIT_API_KEY/BYBIT_SECRET missing in /root/trading-bot/app/.env")

    ex = ccxt.bybit({
        "apiKey": key,
        "secret": sec,
        "enableRateLimit": True,
        "options": {"defaultType": "swap"},
    })

    t = ex.fetch_ticker(SYMBOL)
    last = t.get("last") or t.get("close")
    if last:
        return Decimal(str(last))

    info = t.get("info") or {}
    for k in ["markPrice","mark_price","lastPrice","last_price","indexPrice"]:
        v = info.get(k)
        if v:
            return Decimal(str(v))

    raise RuntimeError("ticker has no usable price: " + str(t))

def ensure_tables(cur):
    # 안전: 테이블 없으면 바로 만들기
    cur.execute('''
    CREATE TABLE IF NOT EXISTS public.dry_run_positions (
      id BIGSERIAL PRIMARY KEY,
      symbol TEXT NOT NULL,
      side TEXT NOT NULL CHECK (side IN ('LONG','SHORT')),
      qty NUMERIC NOT NULL DEFAULT 0,
      avg_entry NUMERIC,
      opened_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      meta JSONB NOT NULL DEFAULT '{}'::jsonb
    );
    ''')
    cur.execute('CREATE UNIQUE INDEX IF NOT EXISTS uq_dry_run_positions_symbol ON public.dry_run_positions(symbol);')

    cur.execute('''
    CREATE TABLE IF NOT EXISTS public.dry_run_fills (
      id BIGSERIAL PRIMARY KEY,
      ts TIMESTAMPTZ NOT NULL DEFAULT now(),
      symbol TEXT NOT NULL,
      action TEXT NOT NULL CHECK (action IN ('OPEN','DCA','CLOSE')),
      side TEXT NOT NULL CHECK (side IN ('LONG','SHORT')),
      price NUMERIC NOT NULL,
      usdt NUMERIC NOT NULL,
      qty NUMERIC NOT NULL,
      pool TEXT NOT NULL CHECK (pool IN ('base','dca')),
      step NUMERIC NOT NULL,
      reason TEXT NOT NULL DEFAULT '',
      meta JSONB NOT NULL DEFAULT '{}'::jsonb
    );
    ''')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_dry_run_fills_symbol_ts ON public.dry_run_fills(symbol, ts);')

def get_virtual_capital(cur):
    row = q1(cur, 'SELECT capital_usdt FROM public.virtual_capital ORDER BY id DESC LIMIT 1;')
    if not row:
        raise RuntimeError('virtual_capital empty')
    return Decimal(str(row[0]))

def append_virtual_capital(cur, new_capital, reason, meta=None):
    cur.execute('''
        INSERT INTO public.virtual_capital(capital_usdt, reason, meta)
        VALUES (%s,%s,%s::jsonb);
    ''', (str(new_capital), reason, json.dumps(meta or {})))

def latest_decision(cur):
    row = q1(cur, '''
        SELECT id, ts, pool, step, source, reason, meta
        FROM public.position_sizing_decision
        WHERE symbol=%s
        ORDER BY id DESC
        LIMIT 1;
    ''', (SYMBOL,))
    if not row:
        return None
    return {
        'id': row[0],
        'ts': row[1],
        'pool': row[2],
        'step': Decimal(str(row[3])),
        'source': row[4],
        'reason': row[5],
        'meta': row[6] or {}
    }

def get_position(cur):
    row = q1(cur, 'SELECT side, qty, avg_entry FROM public.dry_run_positions WHERE symbol=%s;', (SYMBOL,))
    if not row:
        return None
    return {'side': row[0], 'qty': Decimal(str(row[1])), 'avg_entry': (Decimal(str(row[2])) if row[2] is not None else None)}

def upsert_position(cur, side, qty, avg_entry, meta=None):
    cur.execute('''
        INSERT INTO public.dry_run_positions(symbol, side, qty, avg_entry, meta)
        VALUES (%s,%s,%s,%s,%s::jsonb)
        ON CONFLICT (symbol) DO UPDATE SET
          side=EXCLUDED.side,
          qty=EXCLUDED.qty,
          avg_entry=EXCLUDED.avg_entry,
          updated_at=now(),
          meta=EXCLUDED.meta;
    ''', (SYMBOL, side, str(qty), (str(avg_entry) if avg_entry is not None else None), json.dumps(meta or {})))

def delete_position(cur):
    cur.execute('DELETE FROM public.dry_run_positions WHERE symbol=%s;', (SYMBOL,))

def add_fill(cur, action, side, price, usdt, qty, pool, step, reason, meta=None):
    cur.execute('''
        INSERT INTO public.dry_run_fills(symbol, action, side, price, usdt, qty, pool, step, reason, meta)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb);
    ''', (SYMBOL, action, side, str(price), str(usdt), str(qty), pool, str(step), reason, json.dumps(meta or {})))

def calc_order_usdt(capital, pool, step):
    base_pool = capital * BASE_RATIO
    dca_pool  = capital * DCA_RATIO
    pool_amt = base_pool if pool == 'base' else dca_pool
    return pool_amt * step

def run_once(conn, last_decision_id):
    with conn.cursor() as cur:
        ensure_tables(cur)
        # trade_switch gate: OFF => do nothing
        try:
            row = q1(cur, "SELECT enabled FROM public.trade_switch ORDER BY id DESC LIMIT 1;")
            if row is not None and (row[0] is False):
                return last_decision_id
        except Exception:
            # if trade_switch table missing or query fails, default to ON
            pass


        dec = latest_decision(cur)
        if not dec or dec['id'] == last_decision_id:
            return last_decision_id

        # direction default LONG
        direction = (dec['meta'] or {}).get('direction', 'LONG')
        if direction not in ('LONG','SHORT'):
            direction = 'LONG'

        capital = get_virtual_capital(cur)
        price = now_price(cur)

        order_usdt = calc_order_usdt(capital, dec['pool'], dec['step'])
        if order_usdt <= 0:
            print('[DRY_RUN] skip: order_usdt<=0', order_usdt)
            return dec['id']

        qty = (order_usdt / price)
        qty = qty.quantize(Decimal('0.000001'))

        pos = get_position(cur)

        if not pos:
            # OPEN
            add_fill(cur, 'OPEN', direction, price, order_usdt, qty, dec['pool'], dec['step'], dec['reason'], meta={'decision_id': dec['id']})
            upsert_position(cur, direction, qty, price, meta={'last_action':'OPEN','decision_id': dec['id']})
            new_capital = (capital - order_usdt).quantize(Decimal('0.000001'))
            append_virtual_capital(cur, new_capital, 'dry_run_open_cost', meta={'usdt': str(order_usdt), 'price': str(price), 'qty': str(qty)})

            print(f'[DRY_RUN] OPEN {direction} usdt={order_usdt} qty={qty} price={price} capital->{new_capital}')
            return dec['id']

        # same direction DCA only (간단 정책)
        if pos['side'] != direction:
            print(f'[DRY_RUN] skip: existing side={pos['side']} != decision side={direction}')
            return dec['id']

        # DCA
        new_qty = (pos['qty'] + qty).quantize(Decimal('0.000001'))
        new_avg = ((pos['avg_entry'] * pos['qty'] + price * qty) / new_qty).quantize(Decimal('0.01'))

        add_fill(cur, 'DCA', direction, price, order_usdt, qty, dec['pool'], dec['step'], dec['reason'], meta={'decision_id': dec['id']})
        upsert_position(cur, direction, new_qty, new_avg, meta={'last_action':'DCA','decision_id': dec['id']})

        new_capital = (capital - order_usdt).quantize(Decimal('0.000001'))
        append_virtual_capital(cur, new_capital, 'dry_run_dca_cost', meta={'usdt': str(order_usdt), 'price': str(price), 'qty': str(qty)})

        print(f'[DRY_RUN] DCA {direction} usdt={order_usdt} add_qty={qty} price={price} new_qty={new_qty} new_avg={new_avg} capital->{new_capital}')
        return dec['id']

def main():
    if not DRY_RUN:
        raise SystemExit('This executor currently supports DRY_RUN only (set DRY_RUN=1).')

    last_decision_id = 0
    print('[order_executor] DRY_RUN=1 symbol=', SYMBOL)

    while True:
        try:
            conn = get_conn(autocommit=True)
            last_decision_id = run_once(conn, last_decision_id)
            conn.close()
        except Exception as e:
            print('[order_executor] error:', repr(e))
        time.sleep(POLL_SEC)

if __name__ == '__main__':
    main()
