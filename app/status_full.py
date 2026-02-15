#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import ccxt
from datetime import datetime, timezone
from dotenv import load_dotenv
from db_config import get_conn

load_dotenv("/root/trading-bot/app/.env")

SYMBOL = "BTC/USDT:USDT"

def db_conn():
    return get_conn(autocommit=True)

def q1(sql, params=None):
    conn = db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchone()
    finally:
        conn.close()

def exchange():
    return ccxt.bybit({
        "apiKey": os.getenv("BYBIT_API_KEY"),
        "secret": os.getenv("BYBIT_SECRET"),
        "enableRateLimit": True,
        "options": {"defaultType": "swap"},
    })

def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

def main():
    live = os.getenv("LIVE_TRADING", "") == "YES_I_UNDERSTAND"

    sw = q1("SELECT enabled, updated_at FROM trade_switch ORDER BY id DESC LIMIT 1;")
    sw_enabled = bool(sw and sw[0])
    sw_updated = str(sw[1]) if sw else "n/a"

    lock = q1("SELECT COUNT(*), MIN(opened_at), MAX(opened_at) FROM live_order_once_lock;")
    lock_rows = int(lock[0]) if lock else 0
    lock_first = str(lock[1]) if lock and lock[1] else "n/a"
    lock_last = str(lock[2]) if lock and lock[2] else "n/a"

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # Position / PnL
    pos_line = "position: unknown"
    try:
        ex = exchange()
        positions = ex.fetch_positions([SYMBOL])
        p = None
        for it in positions:
            if it.get("symbol") == SYMBOL:
                p = it
                break

        if not p:
            pos_line = f"position({SYMBOL}): not found"
        else:
            side = p.get("side")  # long/short/None
            contracts = safe_float(p.get("contracts") or 0)
            entry = safe_float(p.get("entryPrice") or p.get("entry_price") or None)
            mark = safe_float(p.get("markPrice") or p.get("mark_price") or None)
            upl = safe_float(p.get("unrealizedPnl") or p.get("unrealized_pnl") or None)

            if not side or not contracts or contracts == 0:
                pos_line = f"position({SYMBOL}): none"
            else:
                pos_line = f"position({SYMBOL}): {side} qty={contracts} entry={entry} mark={mark} uPnL={upl}"
    except Exception as e:
        pos_line = f"position({SYMBOL}): error {type(e).__name__}"

    print(
        "\n".join([
            "📌 STATUS (FULL)",
            f"- time: {now}",
            f"- LIVE_TRADING: {'ON' if live else 'OFF'}",
            f"- trade_switch: {'ON' if sw_enabled else 'OFF'} (updated_at={sw_updated})",
            f"- once_lock rows: {lock_rows} (first={lock_first}, last={lock_last})",
            f"- {pos_line}",
        ])
    )
    return 0

if __name__ == "__main__":
    sys.exit(main())
