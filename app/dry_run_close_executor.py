#!/usr/bin/env python3
import os, time, json
import psycopg2
from decimal import Decimal
from dotenv import load_dotenv
import ccxt

DB = dict(host="localhost", port=5433, dbname="trading", user="bot", password="botpass",
         connect_timeout=10, options="-c statement_timeout=30000")
SYMBOL = os.getenv("SYMBOL", "BTC/USDT:USDT")
POLL_SEC = int(os.getenv("DRY_RUN_CLOSE_POLL_SEC", "3"))

def q1(cur, sql, params=()):
    cur.execute(sql, params)
    return cur.fetchone()

def qall(cur, sql, params=()):
    cur.execute(sql, params)
    return cur.fetchall()

def ensure_state(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS public.dry_run_close_state (
      symbol TEXT PRIMARY KEY,
      last_trade_decision_id BIGINT NOT NULL DEFAULT 0,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """)
    cur.execute("""
    INSERT INTO public.dry_run_close_state(symbol, last_trade_decision_id)
    VALUES (%s, 0)
    ON CONFLICT (symbol) DO NOTHING;
    """, (SYMBOL,))

def get_last_id(cur) -> int:
    row = q1(cur, "SELECT last_trade_decision_id FROM public.dry_run_close_state WHERE symbol=%s;", (SYMBOL,))
    return int(row[0]) if row else 0

def set_last_id(cur, last_id: int):
    cur.execute("UPDATE public.dry_run_close_state SET last_trade_decision_id=%s, updated_at=now() WHERE symbol=%s;", (last_id, SYMBOL))

def fetch_close_decisions(cur, last_id: int):
    return qall(cur, """
        SELECT id, reason
        FROM public.trade_decision
        WHERE symbol=%s AND id > %s AND action='CLOSE'
        ORDER BY id ASC
        LIMIT 50;
    """, (SYMBOL, last_id))

def get_position(cur):
    row = q1(cur, "SELECT side, qty, avg_entry FROM public.dry_run_positions WHERE symbol=%s;", (SYMBOL,))
    if not row:
        return None
    return {"side": row[0], "qty": Decimal(str(row[1])), "avg_entry": Decimal(str(row[2]))}

def delete_position(cur):
    cur.execute("DELETE FROM public.dry_run_positions WHERE symbol=%s;", (SYMBOL,))

def get_capital(cur) -> Decimal:
    row = q1(cur, "SELECT capital_usdt FROM public.virtual_capital ORDER BY id DESC LIMIT 1;")
    if not row:
        raise RuntimeError("virtual_capital empty")
    return Decimal(str(row[0]))

def append_capital(cur, new_cap: Decimal, reason: str, meta: dict):
    cur.execute("INSERT INTO public.virtual_capital(capital_usdt, reason, meta) VALUES (%s,%s,%s::jsonb);", (str(new_cap), reason, json.dumps(meta)))

def add_close_fill(cur, exit_price: Decimal, pos: dict, decision_id: int, reason: str, pnl: Decimal):
    cur.execute("""
        INSERT INTO public.dry_run_fills(symbol, action, side, price, usdt, qty, pool, step, reason, meta)
        VALUES (%s,'CLOSE',%s,%s,0,%s,'base',0,%s,%s::jsonb);
    """, (SYMBOL, pos["side"], str(exit_price), str(pos["qty"]), reason,
            json.dumps({"decision_id": decision_id, "entry": str(pos["avg_entry"]), "exit": str(exit_price), "pnl": str(pnl)})))

def make_exchange():
    load_dotenv('/root/trading-bot/app/.env')
    key = os.getenv('BYBIT_API_KEY')
    sec = os.getenv('BYBIT_SECRET')
    if not key or not sec:
        raise RuntimeError('BYBIT_API_KEY/BYBIT_SECRET missing in /root/trading-bot/app/.env')
    ex = ccxt.bybit({"apiKey": key, "secret": sec, "enableRateLimit": True, "options": {"defaultType": "swap"}})
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
    print(f"[dry_run_close_executor] START symbol={SYMBOL} poll={POLL_SEC}", flush=True)

    while True:
        try:
            conn = psycopg2.connect(**DB)
            conn.autocommit = True
            with conn.cursor() as cur:
                ensure_state(cur)

                locked = q1(cur, "SELECT pg_try_advisory_lock(hashtext(%s));", ("dry_run_close:"+SYMBOL,))[0]
                if not locked:
                    conn.close()
                    time.sleep(POLL_SEC)
                    continue

                last_id = get_last_id(cur)
                decisions = fetch_close_decisions(cur, last_id)

                if decisions:
                    cap = get_capital(cur)
                    pos = get_position(cur)

                    for did, reason in decisions:
                        if pos is None:
                            set_last_id(cur, did)
                            continue

                        exit_price = now_price(ex)
                        entry = pos['avg_entry']
                        qty = pos['qty']

                        pnl = (exit_price - entry) * qty if pos['side'] == 'LONG' else (entry - exit_price) * qty
                        pnl = pnl.quantize(Decimal('0.000001'))

                        add_close_fill(cur, exit_price, pos, did, reason or 'close', pnl)
                        new_cap = (cap + pnl).quantize(Decimal('0.000001'))
                        append_capital(cur, new_cap, 'dry_run_close_pnl', {"decision_id": did, "pnl": str(pnl), "entry": str(entry), "exit": str(exit_price), "qty": str(qty)})

                        delete_position(cur)

                        print(f"[dry_run_close_executor] CLOSE id={did} entry={entry} exit={exit_price} qty={qty} pnl={pnl} cap->{new_cap}", flush=True)

                        cap = new_cap
                        pos = None
                        set_last_id(cur, did)

                q1(cur, "SELECT pg_advisory_unlock(hashtext(%s));", ("dry_run_close:"+SYMBOL,))

            conn.close()
        except Exception as e:
            print('[dry_run_close_executor] error:', repr(e), flush=True)

        time.sleep(POLL_SEC)

if __name__ == '__main__':
    main()
