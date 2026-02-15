#!/usr/bin/env python3
import os
from decimal import Decimal
from db_config import get_conn
SYMBOL = os.getenv("SYMBOL", "BTC/USDT:USDT")

def q1(cur, sql, params=()):
    cur.execute(sql, params)
    return cur.fetchone()

def main():
    conn = get_conn(autocommit=True)
    with conn.cursor() as cur:
        # 최신 cash
        row = q1(cur, "SELECT capital_usdt FROM public.virtual_capital ORDER BY id DESC LIMIT 1;")
        cash = Decimal(str(row[0])) if row else Decimal('0')

        # 현재 포지션 가치(포지션 있으면 equity=cash+pos_value, 없으면 cash)
        prow = q1(cur, "SELECT qty FROM public.dry_run_positions WHERE symbol=%s LIMIT 1;", (SYMBOL,))
        qty = Decimal(str(prow[0])) if prow else Decimal('0')

        # NOTE: 리셋은 보수적으로 cash만 기준으로 잡는다(포지션이 있으면 리셋 전에 CLOSE 권장)
        equity_now = cash

        cur.execute("""
        INSERT INTO public.equity_guard_state(symbol, start_equity)
        VALUES (%s,%s)
        ON CONFLICT (symbol)
        DO UPDATE SET start_equity=EXCLUDED.start_equity, updated_at=now();
        """, (SYMBOL, str(equity_now)))

        print('[equity_guard_reset] start_equity reset to', equity_now)

    conn.close()

if __name__ == '__main__':
    main()
