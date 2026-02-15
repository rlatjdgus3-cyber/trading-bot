#!/usr/bin/env python3
import argparse
import json
from decimal import Decimal
from db_config import get_conn
SYMBOL = 'BTC/USDT:USDT'

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--pool', required=True, choices=['base','dca'])
    ap.add_argument('--step', required=True, type=str)  # Decimal로 받기
    ap.add_argument('--source', default='openclaw')
    ap.add_argument('--reason', default='')
    ap.add_argument('--meta', default='{}')
    args = ap.parse_args()

    step = Decimal(args.step)
    meta = json.loads(args.meta)

    db = get_conn(autocommit=True)
    with db.cursor() as cur:
        cur.execute('''
            INSERT INTO public.position_sizing_decision(symbol, pool, step, source, reason, meta)
            VALUES (%s,%s,%s,%s,%s,%s)
            RETURNING id, ts, symbol, pool, step, source, reason;
        ''', (SYMBOL, args.pool, step, args.source, args.reason, json.dumps(meta)))
        row = cur.fetchone()
        print('OK:', row)

if __name__ == '__main__':
    main()
