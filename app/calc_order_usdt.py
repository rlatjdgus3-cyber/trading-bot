#!/usr/bin/env python3
import psycopg2
from decimal import Decimal

DB = dict(host='localhost', port=5433, dbname='trading', user='bot', password='botpass',
         connect_timeout=10, options='-c statement_timeout=30000')
SYMBOL = 'BTC/USDT:USDT'

BASE_RATIO = Decimal('0.70')
DCA_RATIO  = Decimal('0.30')

def main():
    db = psycopg2.connect(**DB)
    db.autocommit = True
    with db.cursor() as cur:
        cur.execute('SELECT capital_usdt FROM public.virtual_capital ORDER BY id DESC LIMIT 1;')
        row = cur.fetchone()
        if not row:
            raise SystemExit('virtual_capital is empty')
        capital = Decimal(str(row[0]))

        cur.execute('''
            SELECT pool, step
            FROM public.position_sizing_decision
            WHERE symbol=%s
            ORDER BY id DESC
            LIMIT 1;
        ''', (SYMBOL,))
        dec = cur.fetchone()
        if not dec:
            raise SystemExit('position_sizing_decision is empty')

        pool = dec[0]
        step = Decimal(str(dec[1]))

        base_pool = capital * BASE_RATIO
        dca_pool  = capital * DCA_RATIO

        pool_amt = base_pool if pool == 'base' else dca_pool
        order_usdt = pool_amt * step

        print('=== ORDER SIZE CALC (NO ORDER) ===')
        print('db_port:', DB['port'])
        print('symbol:', SYMBOL)
        print('virtual_capital:', capital)
        print('base_pool(70%):', base_pool.quantize(Decimal('0.01')))
        print('dca_pool(30%):', dca_pool.quantize(Decimal('0.01')))
        print('decision.pool:', pool)
        print('decision.step:', step)
        print('order_usdt = pool_amt * step =>', order_usdt.quantize(Decimal('0.001')))

if __name__ == '__main__':
    main()
