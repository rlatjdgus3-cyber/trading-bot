#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
live_order_executor.py  --  300 USDT Live Trading Daemon

Polls signals_action_v3 (OPEN) and trade_decision (CLOSE) every 3 seconds.
Guard chain prevents unintended orders.  All decisions logged to live_executor_log.

Rollback:
  touch /root/trading-bot/app/KILL_SWITCH   # instant halt
  systemctl stop live_order_executor.service
  python3 /root/trading-bot/app/panic_close.py
"""

import os
import sys
import time
import json
import traceback
import ccxt
import psycopg2
from dotenv import load_dotenv

load_dotenv("/root/trading-bot/app/.env")

# ============================================================
# Constants
# ============================================================
SYMBOL = "BTC/USDT:USDT"
USDT_CAP = 300                     # hard cap per order
POLL_SEC = 3                       # main loop interval
MIN_ORDER_INTERVAL_SEC = 60        # rate-limit between orders
EMERGENCY_LOSS_PCT = -2.0          # unrealised PnL threshold for auto-close
KILL_SWITCH_PATH = "/root/trading-bot/app/KILL_SWITCH"

ACTION_TBL = "signals_action_v3"

DB = dict(
    host=os.getenv("DB_HOST", "localhost"),
    port=int(os.getenv("DB_PORT", "5432")),
    dbname=os.getenv("DB_NAME", "trading"),
    user=os.getenv("DB_USER", "bot"),
    password=os.getenv("DB_PASS", "botpass"),
    connect_timeout=10,
    options="-c statement_timeout=30000",
)

# ============================================================
# Guard 1: env var gate  (checked once at startup)
# ============================================================
LIVE_TRADING = os.getenv("LIVE_TRADING", "") == "YES_I_UNDERSTAND"
if not LIVE_TRADING:
    print("[LIVE_EXECUTOR] FATAL: LIVE_TRADING != YES_I_UNDERSTAND. Exiting.", flush=True)
    sys.exit(1)

# ============================================================
# Helpers
# ============================================================
def log(msg):
    print(f"[LIVE_EXECUTOR] {msg}", flush=True)


def db_conn():
    conn = psycopg2.connect(**DB)
    conn.autocommit = True
    return conn


def exchange():
    return ccxt.bybit({
        "apiKey": os.getenv("BYBIT_API_KEY"),
        "secret": os.getenv("BYBIT_SECRET"),
        "enableRateLimit": True,
        "options": {"defaultType": "swap"},
    })


# ============================================================
# DB utilities
# ============================================================
def ensure_log_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.live_executor_log (
            id          BIGSERIAL PRIMARY KEY,
            ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
            event       TEXT NOT NULL,
            symbol      TEXT,
            detail      JSONB NOT NULL DEFAULT '{}'::jsonb
        );
    """)


def audit(cur, event: str, symbol: str = None, detail: dict = None):
    cur.execute(
        "INSERT INTO live_executor_log(event, symbol, detail) VALUES (%s, %s, %s::jsonb);",
        (event, symbol, json.dumps(detail or {}, default=str)),
    )


def trade_switch_on(cur) -> bool:
    cur.execute("SELECT enabled FROM trade_switch ORDER BY id DESC LIMIT 1;")
    row = cur.fetchone()
    return bool(row and row[0])


def has_once_lock(cur, symbol: str) -> bool:
    cur.execute("SELECT 1 FROM live_order_once_lock WHERE symbol=%s LIMIT 1;", (symbol,))
    return cur.fetchone() is not None


def set_once_lock(cur, symbol: str):
    cur.execute("INSERT INTO live_order_once_lock(symbol) VALUES (%s);", (symbol,))


def clear_once_lock(cur, symbol: str):
    cur.execute("DELETE FROM live_order_once_lock WHERE symbol=%s;", (symbol,))


def ensure_close_state_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.live_close_state (
            symbol TEXT PRIMARY KEY,
            last_trade_decision_id BIGINT NOT NULL DEFAULT 0,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)
    cur.execute("""
        INSERT INTO public.live_close_state(symbol, last_trade_decision_id)
        VALUES (%s, 0) ON CONFLICT (symbol) DO NOTHING;
    """, (SYMBOL,))


def get_last_close_id(cur) -> int:
    cur.execute(
        "SELECT last_trade_decision_id FROM live_close_state WHERE symbol=%s;",
        (SYMBOL,),
    )
    row = cur.fetchone()
    return int(row[0]) if row else 0


def set_last_close_id(cur, last_id: int):
    cur.execute(
        "UPDATE live_close_state SET last_trade_decision_id=%s, updated_at=now() WHERE symbol=%s;",
        (last_id, SYMBOL),
    )


def fetch_close_decisions(cur, last_id: int):
    cur.execute("""
        SELECT id, reason, direction
        FROM public.trade_decision
        WHERE symbol=%s AND id > %s AND action='CLOSE'
        ORDER BY id ASC LIMIT 50;
    """, (SYMBOL, last_id))
    return cur.fetchall()


def fetch_unprocessed_open_signal(cur):
    """Oldest unprocessed OPEN signal for our symbol."""
    cur.execute(f"""
        SELECT id, action, signal, meta, price
        FROM {ACTION_TBL}
        WHERE processed = false AND action = 'OPEN' AND symbol = %s
        ORDER BY id ASC LIMIT 1;
    """, (SYMBOL,))
    return cur.fetchone()


def fetch_unprocessed_nonopen_signals(cur):
    """All unprocessed non-OPEN signals (STOPPED etc) — bulk mark them processed."""
    cur.execute(f"""
        UPDATE {ACTION_TBL}
        SET processed = true
        WHERE processed = false AND action <> 'OPEN' AND symbol = %s
        RETURNING id, action, signal;
    """, (SYMBOL,))
    return cur.fetchall()


def mark_processed(cur, action_id: int):
    cur.execute(
        f"UPDATE {ACTION_TBL} SET processed = true WHERE id = %s;",
        (action_id,),
    )


# ============================================================
# Exchange helpers  (panic_close.py patterns)
# ============================================================
def get_position(ex):
    """Returns (side, qty, unrealised_pnl, percentage).
    Calculates pct manually from entry/mark as ccxt percentage can be 0.
    """
    positions = ex.fetch_positions([SYMBOL])
    for p in positions:
        if p.get("symbol") != SYMBOL:
            continue
        contracts = float(p.get("contracts") or 0.0)
        side = p.get("side")
        upnl = float(p.get("unrealizedPnl") or 0.0)
        entry = float(p.get("entryPrice") or 0.0)
        mark = float(p.get("markPrice") or 0.0)
        # Manual pct calculation (ccxt 'percentage' may be 0 on Bybit)
        pct = 0.0
        if entry > 0 and mark > 0:
            if side == "long":
                pct = (mark - entry) / entry * 100
            elif side == "short":
                pct = (entry - mark) / entry * 100
        if contracts != 0.0 and side in ("long", "short"):
            return side, contracts, upnl, pct
    return None, 0.0, 0.0, 0.0


def place_close_order(ex, side: str, qty: float):
    """reduceOnly market close."""
    params = {"reduceOnly": True}
    if side == "long":
        order = ex.create_market_sell_order(SYMBOL, qty, params)
    else:
        order = ex.create_market_buy_order(SYMBOL, qty, params)
    return order


def place_open_order(ex, direction: str, usdt_size: float):
    """Market open order.  direction = 'LONG' or 'SHORT'."""
    ticker = ex.fetch_ticker(SYMBOL)
    price = float(ticker["last"])
    amount = usdt_size / price
    if direction == "LONG":
        order = ex.create_market_buy_order(SYMBOL, amount)
    else:
        order = ex.create_market_sell_order(SYMBOL, amount)
    return order, price, amount


# ============================================================
# Main daemon
# ============================================================
def main():
    log("=== DAEMON START ===")

    ex = exchange()
    last_order_ts = 0.0

    conn = db_conn()
    with conn.cursor() as cur:
        ensure_log_table(cur)
        ensure_close_state_table(cur)
        audit(cur, "DAEMON_START", SYMBOL, {"pid": os.getpid()})
    conn.close()

    while True:
        try:
            _cycle(ex, last_order_ts)
        except SystemExit:
            raise
        except Exception as e:
            log(f"CYCLE ERROR: {type(e).__name__}: {e}")
            traceback.print_exc()
            try:
                c = db_conn()
                with c.cursor() as cur:
                    audit(cur, "ERROR", SYMBOL, {"error": str(e)})
                c.close()
            except Exception:
                pass
        time.sleep(POLL_SEC)


def _cycle(ex, _last_order_ts_unused):
    """One poll cycle.  Uses module-level _state dict for mutable state."""
    global _state

    # --- Guard: KILL_SWITCH ---
    if os.path.exists(KILL_SWITCH_PATH):
        log("KILL_SWITCH detected. Exiting daemon.")
        conn = db_conn()
        with conn.cursor() as cur:
            audit(cur, "KILL_SWITCH", SYMBOL)
        conn.close()
        sys.exit(0)

    conn = db_conn()
    try:
        with conn.cursor() as cur:
            # --- Guard: trade_switch ---
            if not trade_switch_on(cur):
                return

            # --- Bybit position check + emergency stoploss ---
            side, qty, upnl, pct = get_position(ex)
            if side and qty > 0:
                if pct <= EMERGENCY_LOSS_PCT:
                    log(f"EMERGENCY STOPLOSS: pct={pct}% upnl={upnl}")
                    audit(cur, "STOPLOSS_TRIGGERED", SYMBOL, {
                        "side": side, "qty": qty, "pct": pct, "upnl": upnl,
                    })
                    order = place_close_order(ex, side, qty)
                    audit(cur, "CLOSE_SENT", SYMBOL, {
                        "reason": "emergency_stoploss",
                        "order_id": order.get("id"),
                        "side": side, "qty": qty,
                    })
                    clear_once_lock(cur, SYMBOL)
                    log(f"STOPLOSS CLOSE SENT: {side} qty={qty}")
                    _state["last_order_ts"] = time.time()
                    return

            # --- CLOSE from trade_decision ---
            last_close_id = get_last_close_id(cur)
            close_decisions = fetch_close_decisions(cur, last_close_id)
            for did, reason, direction in close_decisions:
                if side and qty > 0:
                    log(f"CLOSE decision id={did} reason={reason}")
                    audit(cur, "CLOSE_SENT", SYMBOL, {
                        "decision_id": did, "reason": reason,
                        "side": side, "qty": qty,
                    })
                    order = place_close_order(ex, side, qty)
                    clear_once_lock(cur, SYMBOL)
                    log(f"CLOSE ORDER SENT: {side} qty={qty} decision_id={did}")
                    _state["last_order_ts"] = time.time()
                    # refresh position after close
                    side, qty, upnl, pct = get_position(ex)
                else:
                    log(f"CLOSE decision id={did} but no position — skip")
                    audit(cur, "SKIP", SYMBOL, {
                        "decision_id": did, "reason": "no_position",
                    })
                set_last_close_id(cur, did)

            # --- Bulk-skip non-OPEN signals (STOPPED etc) ---
            skipped = fetch_unprocessed_nonopen_signals(cur)
            for sid, act, sig in skipped:
                audit(cur, "SKIP", SYMBOL, {
                    "signal_id": sid, "action": act, "signal": sig,
                })

            # --- OPEN signal ---
            row = fetch_unprocessed_open_signal(cur)
            if not row:
                return

            sig_id, action, signal, meta_raw, price = row

            # Parse meta
            meta = {}
            if meta_raw:
                if isinstance(meta_raw, str):
                    try:
                        meta = json.loads(meta_raw)
                    except Exception:
                        meta = {}
                elif isinstance(meta_raw, dict):
                    meta = meta_raw

            # Guard: dry_run flag in meta
            if meta.get("dry_run") is True:
                log(f"SKIP signal id={sig_id}: meta.dry_run=true")
                audit(cur, "GUARD_BLOCK", SYMBOL, {
                    "signal_id": sig_id, "guard": "dry_run_meta",
                })
                mark_processed(cur, sig_id)
                return

            direction = meta.get("direction", "LONG")

            # Guard: once_lock
            if has_once_lock(cur, SYMBOL):
                log(f"GUARD: once_lock exists for {SYMBOL} — skip signal id={sig_id}")
                audit(cur, "GUARD_BLOCK", SYMBOL, {
                    "signal_id": sig_id, "guard": "once_lock",
                })
                mark_processed(cur, sig_id)
                return

            # Guard: already has Bybit position
            if side and qty > 0:
                log(f"GUARD: Bybit position exists ({side} qty={qty}) — skip signal id={sig_id}")
                audit(cur, "GUARD_BLOCK", SYMBOL, {
                    "signal_id": sig_id, "guard": "bybit_position_exists",
                    "side": side, "qty": qty,
                })
                mark_processed(cur, sig_id)
                return

            # Guard: rate limit
            now = time.time()
            if now - _state["last_order_ts"] < MIN_ORDER_INTERVAL_SEC:
                log(f"GUARD: rate limit — {MIN_ORDER_INTERVAL_SEC}s not elapsed")
                audit(cur, "GUARD_BLOCK", SYMBOL, {
                    "signal_id": sig_id, "guard": "rate_limit",
                })
                # do NOT mark processed — retry next cycle
                return

            # --- Execute OPEN ---
            usdt_size = min(float(meta.get("qty", USDT_CAP)), USDT_CAP)
            if usdt_size <= 0:
                usdt_size = USDT_CAP

            log(f"OPEN {direction} signal_id={sig_id} usdt={usdt_size}")
            order, exec_price, amount = place_open_order(ex, direction, usdt_size)

            set_once_lock(cur, SYMBOL)
            mark_processed(cur, sig_id)
            _state["last_order_ts"] = time.time()

            audit(cur, "OPEN_SENT", SYMBOL, {
                "signal_id": sig_id, "direction": direction,
                "usdt": usdt_size, "price": exec_price, "amount": amount,
                "order_id": order.get("id"),
            })
            log(f"OPEN SENT: {direction} amount={amount} price={exec_price} order={order.get('id')}")

    finally:
        conn.close()


# mutable state shared across cycles
_state = {"last_order_ts": 0.0}

if __name__ == "__main__":
    main()
