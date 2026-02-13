#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
panic_close.py
- Bybit USDT Perp: BTC/USDT:USDT 포지션이 있으면 reduceOnly 시장가로 전량 청산
- 청산 후 trade_switch OFF로 내려서 추가 주문을 원천 차단
- 실패해도 최대한 안전한 로그를 남김

전제:
- /root/trading-bot/app/.env 에 BYBIT_API_KEY, BYBIT_SECRET 존재
- Postgres(trading) 접근 가능: localhost:5432 bot/botpass
"""

import os
import sys
import time
import ccxt
import psycopg2
from dotenv import load_dotenv

load_dotenv("/root/trading-bot/app/.env")

SYMBOL = "BTC/USDT:USDT"

DB = dict(
    host="localhost",
    port=5433,
    dbname="trading",
    user="bot",
    password="botpass",
    connect_timeout=10,
    options="-c statement_timeout=30000",
)

def db_conn():
    conn = psycopg2.connect(**DB)
    conn.autocommit = True
    return conn

def set_trade_switch(enabled: bool):
    with db_conn() as db:
        with db.cursor() as cur:
            cur.execute("UPDATE trade_switch SET enabled=%s, updated_at=NOW() WHERE id=1;", (enabled,))

def exchange():
    return ccxt.bybit({
        "apiKey": os.getenv("BYBIT_API_KEY"),
        "secret": os.getenv("BYBIT_SECRET"),
        "enableRateLimit": True,
        "options": {"defaultType": "swap"},
    })

def get_position(ex):
    """
    Returns: (side, qty)
      side: 'long'/'short'/None
      qty: contracts amount (float)
    """
    positions = ex.fetch_positions([SYMBOL])
    for p in positions:
        if p.get("symbol") != SYMBOL:
            continue
        contracts = float(p.get("contracts") or 0.0)
        side = p.get("side")
        if contracts != 0.0 and side in ("long", "short"):
            return side, contracts
    return None, 0.0

def main():
    # 1) 먼저 스위치 OFF (추가 주문 차단)
    try:
        set_trade_switch(False)
        print("✅ trade_switch -> OFF")
    except Exception as e:
        print(f"⚠️ trade_switch OFF failed: {type(e).__name__}: {e}")

    # 2) 포지션 확인 후 전량 청산
    ex = exchange()

    try:
        side, qty = get_position(ex)
        if not side or qty == 0.0:
            print("✅ No position to close.")
            return 0

        print(f"⚠️ POSITION DETECTED: {side.upper()} qty={qty} symbol={SYMBOL}")
        params = {"reduceOnly": True}

        if side == "long":
            # long -> sell to close
            o = ex.create_market_sell_order(SYMBOL, qty, params)
            print(f"🧯 CLOSE SENT: SELL {qty} {SYMBOL}")
        else:
            # short -> buy to close
            o = ex.create_market_buy_order(SYMBOL, qty, params)
            print(f"🧯 CLOSE SENT: BUY {qty} {SYMBOL}")

        # 3) 약간 대기 후 재확인
        time.sleep(1.0)
        side2, qty2 = get_position(ex)
        if not side2 or qty2 == 0.0:
            print("✅ Close confirmed: position is now flat.")
        else:
            print(f"⚠️ Close not fully confirmed yet: still {side2} qty={qty2}")

        return 0

    except Exception as e:
        print(f"⛔ PANIC CLOSE ERROR: {type(e).__name__}: {e}")
        return 2

if __name__ == "__main__":
    sys.exit(main())
