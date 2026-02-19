#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
live_order_executor.py  --  Dynamic Capital Live Trading Daemon

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
import urllib.parse
import urllib.request
import ccxt
from dotenv import load_dotenv

load_dotenv("/root/trading-bot/app/.env")
from db_config import get_conn
import exchange_compliance as ecl
from trading_config import SYMBOL, ALLOWED_SYMBOLS
import order_throttle

# ============================================================
# Constants
# ============================================================
POLL_SEC = 3                       # main loop interval
MIN_ORDER_INTERVAL_SEC = 10        # was 60 — scalp-optimized
EMERGENCY_LOSS_PCT = -2.0          # unrealised PnL threshold for auto-close
KILL_SWITCH_PATH = "/root/trading-bot/app/KILL_SWITCH"
EQ_DRY_RUN = os.getenv("EQ_DRY_RUN", "1") != "0"  # default True = log-only

ACTION_TBL = "signals_action_v3"



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
    return get_conn(autocommit=True)


_tg_config = {}


def _load_tg_config():
    if _tg_config:
        return _tg_config
    try:
        with open('/root/trading-bot/app/telegram_cmd.env') as f:
            for line in f:
                line = line.strip()
                if '=' in line and not line.startswith('#'):
                    k, v = line.split('=', 1)
                    _tg_config[k.strip()] = v.strip()
    except Exception:
        pass
    return _tg_config


def _send_telegram(text):
    from report_formatter import korean_output_guard
    cfg = _load_tg_config()
    token = cfg.get('TELEGRAM_BOT_TOKEN', '')
    chat_id = cfg.get('TELEGRAM_ALLOWED_CHAT_ID', '')
    if not token or not chat_id:
        return
    try:
        text = korean_output_guard(text or '')
        url = f'https://api.telegram.org/bot{token}/sendMessage'
        data = urllib.parse.urlencode({
            'chat_id': chat_id,
            'text': text,
            'disable_web_page_preview': 'true'}).encode('utf-8')
        req = urllib.request.Request(url, data=data, method='POST')
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass


def exchange():
    return ccxt.bybit({
        "apiKey": os.getenv("BYBIT_API_KEY"),
        "secret": os.getenv("BYBIT_SECRET"),
        "enableRateLimit": True,
        "options": {"defaultType": "swap"},
    })


# ============================================================
# Dynamic capital helpers
# ============================================================
def _get_order_cap(cur=None):
    """Per-order USDT cap = 1 stage slice (equity * 10%). Min 5 USDT."""
    try:
        import safety_manager
        eq = safety_manager.get_equity_limits(cur)
        return max(5, min(eq['slice_usdt'], eq['operating_cap']))
    except Exception:
        return 300  # fallback


# ============================================================
# Symbol whitelist + Exposure cap
# ============================================================
def _check_symbol_allowed(symbol):
    """Hard block: only symbols in ALLOWED_SYMBOLS may trade."""
    if symbol not in ALLOWED_SYMBOLS:
        raise ValueError(f"SYMBOL_NOT_ALLOWED: {symbol}")


def get_btc_exposure_usdt(ex):
    """Current BTC position notional (USDT) from Bybit live."""
    side, qty, upnl, pct = get_position(ex)
    if not side or qty <= 0:
        return 0.0
    ticker = ex.fetch_ticker(SYMBOL)
    return qty * float(ticker["last"])


def enforce_exposure_cap(ex, requested_usdt, cur=None):
    """Check exposure cap (equity-based dynamic). Returns (allowed_usdt, reason_or_None).
    Shrinks requested_usdt if cap would be exceeded.
    Blocks (returns 0) if remaining < minNotional (~5 USDT).
    """
    import safety_manager
    eq = safety_manager.get_equity_limits(cur)
    cap = eq['operating_cap']

    exposure = get_btc_exposure_usdt(ex)
    remaining = max(0, cap - exposure)
    if requested_usdt <= remaining:
        return requested_usdt, None
    if remaining < 5:  # below minNotional
        reason = f"CAP_EXCEEDED: exp={exposure:.0f} cap={cap:.0f}"
        log(f"EXPOSURE CAP BLOCK: {reason}")
        if cur:
            audit(cur, "CAP_BLOCKED", SYMBOL, {
                "exposure": round(exposure, 2),
                "cap": round(cap, 2),
                "requested": round(requested_usdt, 2),
            })
        return 0, reason
    reason = f"CAP_SHRINK: {requested_usdt:.0f}->{remaining:.0f}"
    log(f"EXPOSURE CAP SHRINK: {reason}")
    if cur:
        audit(cur, "CAP_SHRINK", SYMBOL, {
            "exposure": round(exposure, 2),
            "cap": round(cap, 2),
            "requested": round(requested_usdt, 2),
            "allowed": round(remaining, 2),
        })
    return remaining, reason


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


ONCE_LOCK_TTL_MIN = 60  # was 1440 (24h→1h) — scalp re-entry


def has_once_lock(cur, symbol: str) -> bool:
    cur.execute("""
        SELECT 1 FROM live_order_once_lock
        WHERE symbol=%s AND (expires_at IS NULL OR expires_at > now())
        LIMIT 1;
    """, (symbol,))
    return cur.fetchone() is not None


def set_once_lock(cur, symbol: str):
    from datetime import timedelta
    ttl = timedelta(minutes=ONCE_LOCK_TTL_MIN)
    cur.execute("""
        INSERT INTO live_order_once_lock(symbol, opened_at, expires_at)
        VALUES (%s, now(), now() + %s)
        ON CONFLICT (symbol) DO UPDATE SET opened_at = now(), expires_at = now() + %s;
    """, (symbol, ttl, ttl))


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
# Execution Queue (EQ) consumer helpers
# ============================================================
def _fetch_pending_eq_items(cur):
    """Fetch up to 5 PENDING items ordered by priority (low=urgent), then id."""
    cur.execute("""
        SELECT id, action_type, direction, target_qty, target_usdt,
               reduce_pct, reason, meta, expire_at, depends_on
        FROM execution_queue
        WHERE symbol = %s AND status = 'PENDING'
        ORDER BY priority ASC, id ASC
        LIMIT 5;
    """, (SYMBOL,))
    return cur.fetchall()


def _expire_stale_eq_items(cur):
    """Mark items past expire_at as EXPIRED."""
    cur.execute("""
        UPDATE execution_queue
        SET status = 'EXPIRED'
        WHERE symbol = %s AND status = 'PENDING'
          AND expire_at IS NOT NULL AND expire_at < now()
        RETURNING id;
    """, (SYMBOL,))
    expired = cur.fetchall()
    for (eid,) in expired:
        log(f"EQ item id={eid} EXPIRED")
        audit(cur, "EQ_EXPIRED", SYMBOL, {"eq_id": eid})
    return len(expired)


def _update_eq_status(cur, eq_id, status):
    """Update execution_queue row status."""
    if not eq_id:
        return
    cur.execute(
        "UPDATE execution_queue SET status = %s WHERE id = %s;",
        (status, eq_id),
    )


def _insert_exec_log(cur, order, order_type, direction, qty, reason,
                     eq_id, pos_side=None, pos_qty=None, usdt=None, price=None):
    """Insert into execution_log so fill_watcher can track this order."""
    cur.execute("""
        INSERT INTO execution_log
            (order_id, symbol, order_type, direction,
             close_reason, requested_qty, requested_usdt, ticker_price,
             status, raw_order_response,
             source_queue, execution_queue_id,
             position_before_side, position_before_qty)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'SENT', %s::jsonb, %s, %s, %s, %s)
        RETURNING id;
    """, (
        order.get("id", ""),
        SYMBOL,
        order_type,
        direction,
        reason,
        qty,
        usdt,
        price,
        json.dumps(order, default=str),
        "execution_queue",
        eq_id,
        pos_side,
        pos_qty,
    ))
    row = cur.fetchone()
    return row[0] if row else None


def _process_execution_queue(ex, cur, pos_side, pos_qty, entry_enabled=True):
    """Main EQ consumer: expire stale, then process pending items."""
    _expire_stale_eq_items(cur)
    items = _fetch_pending_eq_items(cur)
    if not items:
        return
    for item in items:
        try:
            _process_eq_item(ex, cur, item, pos_side, pos_qty, entry_enabled=entry_enabled)
        except Exception as e:
            eq_id = item[0]
            log(f"EQ item id={eq_id} processing error: {type(e).__name__}: {e}")
            audit(cur, "EQ_ITEM_ERROR", SYMBOL, {"eq_id": eq_id, "error": str(e)})


def _update_position_order_state(cur, eq_id, action_type, direction, target_usdt):
    """Update position_state.order_state to SENT when EQ item is picked.
    Records planned_qty/planned_usdt for entry actions."""
    try:
        if action_type in ('ADD', 'REVERSE_OPEN'):
            usdt = float(target_usdt or 0)
            cur.execute("""
                UPDATE position_state SET
                    order_state = 'SENT',
                    planned_usdt = %s,
                    sent_usdt = %s,
                    last_order_ts = now(),
                    state_changed_at = now()
                WHERE symbol = %s;
            """, (usdt, usdt, SYMBOL))
        elif action_type in ('CLOSE', 'FULL_CLOSE', 'REDUCE', 'REVERSE_CLOSE'):
            cur.execute("""
                UPDATE position_state SET
                    order_state = 'SENT',
                    last_order_ts = now(),
                    state_changed_at = now()
                WHERE symbol = %s;
            """, (SYMBOL,))
    except Exception as e:
        log(f"_update_position_order_state error (eq_id={eq_id}): {e}")


def _process_eq_item(ex, cur, item, pos_side, pos_qty, entry_enabled=True):
    """Dispatch a single EQ item by action_type.
    EXIT actions (CLOSE/FULL_CLOSE/REDUCE/REVERSE_CLOSE) always allowed.
    ENTRY actions (ADD/REVERSE_OPEN) require entry_enabled=True.
    """
    eq_id, action_type, direction, target_qty, target_usdt, \
        reduce_pct, reason, meta_raw, expire_at, depends_on_col = item

    meta = {}
    if meta_raw:
        if isinstance(meta_raw, str):
            try:
                meta = json.loads(meta_raw)
            except Exception:
                meta = {}
        elif isinstance(meta_raw, dict):
            meta = meta_raw

    # Check depends_on (column or meta)
    dep_id = depends_on_col or meta.get("depends_on")
    if dep_id:
        cur.execute(
            "SELECT status FROM execution_queue WHERE id = %s;",
            (int(dep_id),),
        )
        dep_row = cur.fetchone()
        if dep_row and dep_row[0] not in ("FILLED", "DRY_RUN_LOGGED"):
            log(f"EQ id={eq_id} waiting on depends_on={dep_id} (status={dep_row[0]})")
            return  # stay PENDING, retry next cycle

    # THROTTLE gate: check order throttle before entry actions
    if action_type in ("ADD", "REVERSE_OPEN"):
        throttle_ok, throttle_reason, throttle_meta = order_throttle.check_all(cur, action_type)
        if not throttle_ok:
            log(f"EQ id={eq_id} {action_type} throttled: {throttle_reason}")
            # Keep PENDING (5-min expire_at will auto-GC)
            return

    # ENTRY gate: ADD/REVERSE_OPEN require entry_enabled
    if action_type in ("ADD", "REVERSE_OPEN") and not entry_enabled:
        log(f"EQ id={eq_id} {action_type} REJECTED — trade_switch OFF (entry disabled)")
        _update_eq_status(cur, eq_id, "REJECTED")
        audit(cur, "EQ_REJECTED", SYMBOL, {
            "eq_id": eq_id, "action_type": action_type,
            "reason": "trade_switch_off_entry_disabled"})
        return

    # Mark PICKED + update order_state to SENT
    _update_eq_status(cur, eq_id, "PICKED")
    _update_position_order_state(cur, eq_id, action_type, direction, target_usdt)

    if action_type in ("CLOSE", "FULL_CLOSE"):
        _eq_handle_close(ex, cur, eq_id, direction, pos_side, pos_qty, reason)
    elif action_type == "REDUCE":
        _eq_handle_reduce(ex, cur, eq_id, direction, reduce_pct, target_qty,
                          pos_side, pos_qty, reason)
    elif action_type == "ADD":
        _eq_handle_add(ex, cur, eq_id, direction, target_usdt, reason, meta=meta)
    elif action_type == "REVERSE_CLOSE":
        _eq_handle_reverse_close(ex, cur, eq_id, direction, pos_side, pos_qty, reason)
    elif action_type == "REVERSE_OPEN":
        _eq_handle_reverse_open(ex, cur, eq_id, direction, target_usdt, reason, meta=meta)
    else:
        log(f"EQ id={eq_id} unknown action_type={action_type} — REJECTED")
        _update_eq_status(cur, eq_id, "REJECTED")
        audit(cur, "EQ_REJECTED", SYMBOL, {"eq_id": eq_id, "reason": "unknown_action"})


def _eq_handle_close(ex, cur, eq_id, direction, pos_side, pos_qty, reason):
    """Handle CLOSE / FULL_CLOSE: close entire position."""
    if not pos_side or pos_qty <= 0:
        log(f"EQ id={eq_id} CLOSE — no position, REJECTED")
        _update_eq_status(cur, eq_id, "REJECTED")
        audit(cur, "EQ_REJECTED", SYMBOL, {"eq_id": eq_id, "reason": "no_position"})
        return

    if EQ_DRY_RUN:
        log(f"[EQ_DRY_RUN] CLOSE {pos_side} qty={pos_qty} reason={reason}"
            f" — WOULD CALL place_close_order()")
        _update_eq_status(cur, eq_id, "DRY_RUN_LOGGED")
        audit(cur, "EQ_DRY_RUN", SYMBOL, {
            "eq_id": eq_id, "action": "CLOSE", "side": pos_side,
            "qty": float(pos_qty), "reason": reason,
        })
        return

    try:
        order = place_close_order(ex, pos_side, pos_qty, cur=cur)
    except Exception as e:
        log(f"EQ id={eq_id} CLOSE failed: {e}")
        _update_eq_status(cur, eq_id, "REJECTED")
        audit(cur, "EQ_REJECTED", SYMBOL, {
            "eq_id": eq_id, "reason": f"compliance/exchange: {e}"})
        return
    _update_eq_status(cur, eq_id, "SENT")
    _insert_exec_log(cur, order, "CLOSE", pos_side.upper(), pos_qty, reason,
                     eq_id, pos_side, pos_qty)
    audit(cur, "EQ_CLOSE_SENT", SYMBOL, {
        "eq_id": eq_id, "side": pos_side, "qty": float(pos_qty),
        "order_id": order.get("id"), "reason": reason,
    })
    clear_once_lock(cur, SYMBOL)
    log(f"EQ CLOSE SENT: {pos_side} qty={pos_qty} order={order.get('id')}")


def _eq_handle_reduce(ex, cur, eq_id, direction, reduce_pct, target_qty,
                      pos_side, pos_qty, reason):
    """Handle REDUCE: partial position close."""
    if not pos_side or pos_qty <= 0:
        log(f"EQ id={eq_id} REDUCE — no position, REJECTED")
        _update_eq_status(cur, eq_id, "REJECTED")
        audit(cur, "EQ_REJECTED", SYMBOL, {"eq_id": eq_id, "reason": "no_position"})
        return

    if target_qty and float(target_qty) > 0:
        qty = min(float(target_qty), pos_qty)
    elif reduce_pct and float(reduce_pct) > 0:
        qty = pos_qty * float(reduce_pct) / 100.0
    else:
        qty = pos_qty * 0.3  # default 30%

    # ── min_qty enforcement for REDUCE ──
    try:
        info = ecl.get_market_info(ex, SYMBOL)
        min_qty = info['minQty']
        if qty < min_qty:
            log(f"REDUCE qty {qty:.6f} < min_qty {min_qty} — switching to FULL CLOSE "
                f"(pos_qty={pos_qty})")
            qty = pos_qty  # full position close instead of partial
    except Exception as e:
        log(f"REDUCE min_qty check warning: {e}")

    log(f"REDUCE ORDER_DETAIL: qty={qty:.6f} pos_qty={pos_qty:.6f} "
        f"reduce_pct={reduce_pct} reason={reason}")

    if EQ_DRY_RUN:
        log(f"[EQ_DRY_RUN] REDUCE {pos_side} qty={qty:.6f} (of {pos_qty}) reason={reason}"
            f" — WOULD CALL place_close_order()")
        _update_eq_status(cur, eq_id, "DRY_RUN_LOGGED")
        audit(cur, "EQ_DRY_RUN", SYMBOL, {
            "eq_id": eq_id, "action": "REDUCE", "side": pos_side,
            "qty": float(qty), "total_qty": float(pos_qty), "reason": reason,
        })
        return

    try:
        order = place_close_order(ex, pos_side, qty, cur=cur)
    except Exception as e:
        log(f"EQ id={eq_id} REDUCE failed: {e}")
        _update_eq_status(cur, eq_id, "REJECTED")
        audit(cur, "EQ_REJECTED", SYMBOL, {
            "eq_id": eq_id, "reason": f"compliance/exchange: {e}"})
        return
    _update_eq_status(cur, eq_id, "SENT")
    _insert_exec_log(cur, order, "REDUCE", pos_side.upper(), qty, reason,
                     eq_id, pos_side, pos_qty)
    audit(cur, "EQ_REDUCE_SENT", SYMBOL, {
        "eq_id": eq_id, "side": pos_side, "qty": float(qty),
        "order_id": order.get("id"), "reason": reason,
    })
    log(f"EQ REDUCE SENT: {pos_side} qty={qty:.6f} order={order.get('id')}")


def _eq_handle_add(ex, cur, eq_id, direction, target_usdt, reason, meta=None):
    """Handle ADD: pyramid add to position."""
    # Protection mode: block OPEN/ADD
    pm_ok, pm_reason = ecl.check_protection_mode_for_action('ADD')
    if not pm_ok:
        log(f"EQ id={eq_id} ADD blocked by protection mode: {pm_reason}")
        _update_eq_status(cur, eq_id, "REJECTED")
        audit(cur, "EQ_REJECTED", SYMBOL, {
            "eq_id": eq_id, "reason": f"protection_mode: {pm_reason}"})
        report = ecl.format_protection_mode_report()
        if report:
            _send_telegram(report)
        return

    order_cap = _get_order_cap(cur)
    usdt = float(target_usdt) if target_usdt else order_cap
    usdt = min(usdt, order_cap)
    if usdt <= 0:
        usdt = order_cap
    dir_upper = (direction or "LONG").upper()

    # Exposure cap enforcement
    usdt, cap_reason = enforce_exposure_cap(ex, usdt, cur=cur)
    if usdt <= 0:
        log(f"EQ id={eq_id} ADD blocked by exposure cap: {cap_reason}")
        _update_eq_status(cur, eq_id, "REJECTED")
        audit(cur, "EQ_REJECTED", SYMBOL, {
            "eq_id": eq_id, "reason": f"exposure_cap: {cap_reason}"})
        return

    if EQ_DRY_RUN:
        log(f"[EQ_DRY_RUN] ADD {dir_upper} usdt={usdt} reason={reason}"
            f" — WOULD CALL place_open_order()")
        _update_eq_status(cur, eq_id, "DRY_RUN_LOGGED")
        audit(cur, "EQ_DRY_RUN", SYMBOL, {
            "eq_id": eq_id, "action": "ADD", "direction": dir_upper,
            "usdt": usdt, "reason": reason,
        })
        order_throttle.record_attempt(cur, 'ADD', dir_upper, 'DRY_RUN')
        return

    try:
        order, exec_price, amount = place_open_order(ex, dir_upper, usdt, cur=cur, meta=meta or {})
    except Exception as e:
        log(f"EQ id={eq_id} ADD failed: {e}")
        _update_eq_status(cur, eq_id, "REJECTED")
        audit(cur, "EQ_REJECTED", SYMBOL, {
            "eq_id": eq_id, "reason": f"compliance/exchange: {e}"})
        # record_attempt already called inside place_open_order on failure
        return
    _update_eq_status(cur, eq_id, "SENT")
    _insert_exec_log(cur, order, "ADD", dir_upper, amount, reason,
                     eq_id, usdt=usdt, price=exec_price)
    # Record last_order_id in position_state
    try:
        cur.execute("UPDATE position_state SET last_order_id = %s WHERE symbol = %s;",
                    (order.get("id", ""), SYMBOL))
    except Exception:
        pass
    audit(cur, "EQ_ADD_SENT", SYMBOL, {
        "eq_id": eq_id, "direction": dir_upper, "usdt": usdt,
        "price": exec_price, "amount": amount,
        "order_id": order.get("id"), "reason": reason,
    })
    log(f"EQ ADD SENT: {dir_upper} usdt={usdt} amount={amount} order={order.get('id')}")


def _eq_handle_reverse_close(ex, cur, eq_id, direction, pos_side, pos_qty, reason):
    """Handle REVERSE_CLOSE: close current position as part of reversal."""
    if not pos_side or pos_qty <= 0:
        log(f"EQ id={eq_id} REVERSE_CLOSE — no position, REJECTED")
        _update_eq_status(cur, eq_id, "REJECTED")
        audit(cur, "EQ_REJECTED", SYMBOL, {"eq_id": eq_id, "reason": "no_position"})
        return

    if EQ_DRY_RUN:
        log(f"[EQ_DRY_RUN] REVERSE_CLOSE {pos_side} qty={pos_qty} reason={reason}"
            f" — WOULD CALL place_close_order()")
        _update_eq_status(cur, eq_id, "DRY_RUN_LOGGED")
        audit(cur, "EQ_DRY_RUN", SYMBOL, {
            "eq_id": eq_id, "action": "REVERSE_CLOSE", "side": pos_side,
            "qty": float(pos_qty), "reason": reason,
        })
        return

    try:
        order = place_close_order(ex, pos_side, pos_qty, cur=cur)
    except Exception as e:
        log(f"EQ id={eq_id} REVERSE_CLOSE failed: {e}")
        _update_eq_status(cur, eq_id, "REJECTED")
        audit(cur, "EQ_REJECTED", SYMBOL, {
            "eq_id": eq_id, "reason": f"compliance/exchange: {e}"})
        return
    _update_eq_status(cur, eq_id, "SENT")
    _insert_exec_log(cur, order, "REVERSE_CLOSE", pos_side.upper(), pos_qty, reason,
                     eq_id, pos_side, pos_qty)
    audit(cur, "EQ_REVERSE_CLOSE_SENT", SYMBOL, {
        "eq_id": eq_id, "side": pos_side, "qty": float(pos_qty),
        "order_id": order.get("id"), "reason": reason,
    })
    clear_once_lock(cur, SYMBOL)
    log(f"EQ REVERSE_CLOSE SENT: {pos_side} qty={pos_qty} order={order.get('id')}")


def _eq_handle_reverse_open(ex, cur, eq_id, direction, target_usdt, reason, meta=None):
    """Handle REVERSE_OPEN: open new position as part of reversal."""
    # Protection mode: block OPEN/ADD (REVERSE_OPEN = new position)
    pm_ok, pm_reason = ecl.check_protection_mode_for_action('REVERSE_OPEN')
    if not pm_ok:
        log(f"EQ id={eq_id} REVERSE_OPEN blocked by protection mode: {pm_reason}")
        _update_eq_status(cur, eq_id, "REJECTED")
        audit(cur, "EQ_REJECTED", SYMBOL, {
            "eq_id": eq_id, "reason": f"protection_mode: {pm_reason}"})
        report = ecl.format_protection_mode_report()
        if report:
            _send_telegram(report)
        return

    order_cap = _get_order_cap(cur)
    usdt = float(target_usdt) if target_usdt else order_cap
    usdt = min(usdt, order_cap)
    if usdt <= 0:
        usdt = order_cap
    dir_upper = (direction or "LONG").upper()

    # Exposure cap enforcement
    usdt, cap_reason = enforce_exposure_cap(ex, usdt, cur=cur)
    if usdt <= 0:
        log(f"EQ id={eq_id} REVERSE_OPEN blocked by exposure cap: {cap_reason}")
        _update_eq_status(cur, eq_id, "REJECTED")
        audit(cur, "EQ_REJECTED", SYMBOL, {
            "eq_id": eq_id, "reason": f"exposure_cap: {cap_reason}"})
        return

    if EQ_DRY_RUN:
        log(f"[EQ_DRY_RUN] REVERSE_OPEN {dir_upper} usdt={usdt} reason={reason}"
            f" — WOULD CALL place_open_order()")
        _update_eq_status(cur, eq_id, "DRY_RUN_LOGGED")
        audit(cur, "EQ_DRY_RUN", SYMBOL, {
            "eq_id": eq_id, "action": "REVERSE_OPEN", "direction": dir_upper,
            "usdt": usdt, "reason": reason,
        })
        order_throttle.record_attempt(cur, 'REVERSE_OPEN', dir_upper, 'DRY_RUN')
        return

    try:
        order, exec_price, amount = place_open_order(ex, dir_upper, usdt, cur=cur, meta=meta or {})
    except Exception as e:
        log(f"EQ id={eq_id} REVERSE_OPEN failed: {e}")
        _update_eq_status(cur, eq_id, "REJECTED")
        audit(cur, "EQ_REJECTED", SYMBOL, {
            "eq_id": eq_id, "reason": f"compliance/exchange: {e}"})
        return
    _update_eq_status(cur, eq_id, "SENT")
    _insert_exec_log(cur, order, "REVERSE_OPEN", dir_upper, amount, reason,
                     eq_id, usdt=usdt, price=exec_price)
    # Record last_order_id in position_state
    try:
        cur.execute("UPDATE position_state SET last_order_id = %s WHERE symbol = %s;",
                    (order.get("id", ""), SYMBOL))
    except Exception:
        pass
    set_once_lock(cur, SYMBOL)
    audit(cur, "EQ_REVERSE_OPEN_SENT", SYMBOL, {
        "eq_id": eq_id, "direction": dir_upper, "usdt": usdt,
        "price": exec_price, "amount": amount,
        "order_id": order.get("id"), "reason": reason,
    })
    log(f"EQ REVERSE_OPEN SENT: {dir_upper} usdt={usdt} amount={amount} order={order.get('id')}")


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


def place_close_order(ex, side: str, qty: float, cur=None):
    """reduceOnly market close with ECL compliance."""
    _check_symbol_allowed(SYMBOL)

    # ── min_qty enforcement for close ──
    try:
        info = ecl.get_market_info(ex, SYMBOL)
        min_qty = info['minQty']
        if qty < min_qty:
            log(f"CLOSE MIN_QTY ADJUST: {qty:.6f} -> {min_qty} (min_qty enforcement)")
            qty = min_qty
    except Exception as e:
        log(f"CLOSE min_qty check warning: {e}")

    # Compliance validation
    order_params = {
        'action': 'SELL' if side == 'long' else 'BUY',
        'qty': qty,
        'side': side,
        'reduce_only': True,
        'order_type': 'market',
        'position_qty': qty,
        'usdt_value': 0,  # close doesn't need notional check
    }
    comp = ecl.validate_bybit_compliance(ex, order_params, SYMBOL)

    if not comp.ok:
        log(f"ECL REJECT close: {comp.reason}")
        _send_telegram(ecl.format_compliance_rejection_telegram(comp))
        if cur:
            ecl.log_compliance_event(
                cur, 'PRE_ORDER_REJECT', SYMBOL, order_params,
                compliance_passed=False, reject_reason=comp.reject_reason,
                suggested_fix=comp.suggested_fix)
        raise ValueError(f"ECL reject: {comp.reject_reason or comp.reason}")

    final_qty = comp.corrected_qty if comp.corrected_qty is not None else qty

    params = {"reduceOnly": True}
    try:
        if side == "long":
            order = ex.create_market_sell_order(SYMBOL, final_qty, params)
        else:
            order = ex.create_market_buy_order(SYMBOL, final_qty, params)
        ecl.record_order_sent(SYMBOL, side=side)
        ecl.record_success(SYMBOL)
        if cur:
            ecl.log_compliance_event(
                cur, 'ORDER_SENT', SYMBOL, order_params,
                compliance_passed=True,
                detail={'corrected': comp.was_corrected, 'final_qty': final_qty})
            order_throttle.record_attempt(cur, 'CLOSE', side.upper(), 'SUCCESS')
        order_throttle.handle_success('CLOSE')
        return order
    except Exception as e:
        error_code, raw_msg = ecl.extract_bybit_error_code(e)
        error_info = ecl.map_bybit_error(error_code, raw_msg)
        log(f"BYBIT ERROR close: code={error_code} {error_info['korean_message']}")
        _send_telegram(ecl.format_rejection_telegram(error_info))
        ecl.record_error(SYMBOL, error_code=error_code)
        if cur:
            order_throttle.record_attempt(
                cur, 'CLOSE', side.upper(), 'ERROR',
                reject_reason=error_info.get('korean_message', str(e)),
                error_code=error_code)
        order_throttle.handle_rejection(cur, error_code, error_info.get('korean_message', str(e)))
        # Trigger market info refresh on specific errors
        if ecl.should_refresh_on_error(error_code):
            ecl.force_refresh_market_info(
                ex, SYMBOL, reason=f'close error {error_code}')
        if cur:
            ecl.log_compliance_event(
                cur, 'EXCHANGE_ERROR', SYMBOL, order_params,
                compliance_passed=False,
                reject_reason=error_info['korean_message'],
                exchange_error_code=error_code,
                suggested_fix=error_info['suggested_fix'],
                detail={'raw_message': raw_msg[:500]})
        raise


def place_open_order(ex, direction: str, usdt_size: float, cur=None, meta=None):
    """Market open order with ECL compliance.  direction = 'LONG' or 'SHORT'."""
    _check_symbol_allowed(SYMBOL)

    # Set leverage before order (from leverage_context in meta)
    try:
        import leverage_manager
        lev_ctx = (meta or {}).get('leverage_context', {})
        if lev_ctx:
            lev = leverage_manager.compute_leverage(
                atr_pct=lev_ctx.get('atr_pct', 1.0),
                regime_score=lev_ctx.get('regime_score', 0),
                news_shock=abs(lev_ctx.get('news_event_score', 0)) >= 60,
                confidence=lev_ctx.get('confidence', 0),
                stage=lev_ctx.get('stage', 0),
            )
            leverage_manager.set_exchange_leverage(ex, SYMBOL, lev)
    except Exception as e:
        log(f"leverage set warning: {e}")

    ticker = ex.fetch_ticker(SYMBOL)
    price = float(ticker["last"])
    raw_amount = usdt_size / price
    info = ecl.get_market_info(ex, SYMBOL)

    # ── min_qty enforcement ──
    min_qty = info['minQty']
    min_notional = info['minNotional']
    amount = raw_amount
    if amount < min_qty:
        amount = min_qty
        adjusted_usdt = amount * price
        if adjusted_usdt < min_notional:
            log(f"OPEN BLOCK: even min_qty={min_qty} fails minNotional "
                f"({adjusted_usdt:.2f} < {min_notional})")
            raise ValueError(
                f"min_qty adjusted amount still below minNotional "
                f"({adjusted_usdt:.2f} < {min_notional})")
        log(f"MIN_QTY ADJUST: {raw_amount:.6f} -> {amount} (min_qty={min_qty})")

    # ── Pre-order detail log ──
    try:
        import safety_manager as _sm
        _eq = _sm.get_equity_limits(cur)
        _slice = _eq.get('slice_usdt', 0)
    except Exception:
        _slice = 0
    try:
        positions = ex.fetch_positions([SYMBOL])
        _lev = 0
        for _p in positions:
            if _p.get('symbol') == SYMBOL:
                _lev = float(_p.get('leverage') or 0)
                break
    except Exception:
        _lev = 0
    log(f"ORDER_DETAIL: requested_qty={raw_amount:.6f} adjusted_qty={amount:.6f} "
        f"min_qty={min_qty} slice_usdt={_slice:.0f} leverage={_lev}")

    # Compliance validation
    order_params = {
        'action': 'BUY' if direction == 'LONG' else 'SELL',
        'qty': amount,
        'price': None,  # market order
        'side': direction.lower(),
        'reduce_only': False,
        'order_type': 'market',
        'position_qty': 0,
        'usdt_value': max(usdt_size, amount * price),
    }
    comp = ecl.validate_bybit_compliance(ex, order_params, SYMBOL)

    if not comp.ok:
        log(f"ECL REJECT open: {comp.reason}")
        _send_telegram(ecl.format_compliance_rejection_telegram(comp))
        if cur:
            ecl.log_compliance_event(
                cur, 'PRE_ORDER_REJECT', SYMBOL, order_params,
                compliance_passed=False, reject_reason=comp.reject_reason,
                suggested_fix=comp.suggested_fix)
        raise ValueError(f"ECL reject: {comp.reject_reason or comp.reason}")

    final_amount = comp.corrected_qty if comp.corrected_qty is not None else amount

    try:
        if direction == "LONG":
            order = ex.create_market_buy_order(SYMBOL, final_amount)
        else:
            order = ex.create_market_sell_order(SYMBOL, final_amount)
        ecl.record_order_sent(SYMBOL, price=price, side=direction.lower())
        ecl.record_success(SYMBOL)
        if cur:
            ecl.log_compliance_event(
                cur, 'ORDER_SENT', SYMBOL, order_params,
                compliance_passed=True,
                detail={'corrected': comp.was_corrected, 'final_amount': final_amount})
            order_throttle.record_attempt(cur, 'OPEN', direction, 'SUCCESS')
        order_throttle.handle_success('OPEN')
        return order, price, final_amount
    except Exception as e:
        error_code, raw_msg = ecl.extract_bybit_error_code(e)
        error_info = ecl.map_bybit_error(error_code, raw_msg)
        log(f"BYBIT ERROR open: code={error_code} {error_info['korean_message']}")
        _send_telegram(ecl.format_rejection_telegram(error_info))
        ecl.record_error(SYMBOL, error_code=error_code)
        if cur:
            order_throttle.record_attempt(
                cur, 'OPEN', direction, 'ERROR',
                reject_reason=error_info.get('korean_message', str(e)),
                error_code=error_code)
        order_throttle.handle_rejection(cur, error_code, error_info.get('korean_message', str(e)))
        # Trigger market info refresh on specific errors
        if ecl.should_refresh_on_error(error_code):
            ecl.force_refresh_market_info(
                ex, SYMBOL, reason=f'open error {error_code}')

        # Auto-correction retry for stepSize/tickSize errors
        if ecl.is_auto_correctable(error_code) and not comp.was_corrected:
            log(f"attempting auto-correction for error {error_code}")
            corrected_amount = ecl.align_qty(final_amount, info['stepSize'])
            if corrected_amount >= info['minQty'] and corrected_amount != final_amount:
                try:
                    if direction == "LONG":
                        order = ex.create_market_buy_order(SYMBOL, corrected_amount)
                    else:
                        order = ex.create_market_sell_order(SYMBOL, corrected_amount)
                    ecl.record_order_sent(SYMBOL, price=price, side=direction.lower())
                    ecl.record_success(SYMBOL)
                    log(f"auto-correction succeeded: {final_amount} -> {corrected_amount}")
                    if cur:
                        ecl.log_compliance_event(
                            cur, 'AUTO_CORRECTED', SYMBOL, order_params,
                            compliance_passed=True,
                            detail={'original_qty': final_amount,
                                    'corrected_qty': corrected_amount,
                                    'error_code': error_code})
                    return order, price, corrected_amount
                except Exception as e2:
                    log(f"auto-correction retry failed: {e2}")
                    ec2, _ = ecl.extract_bybit_error_code(e2)
                    ecl.record_error(SYMBOL, error_code=ec2)

        if cur:
            ecl.log_compliance_event(
                cur, 'EXCHANGE_ERROR', SYMBOL, order_params,
                compliance_passed=False,
                reject_reason=error_info['korean_message'],
                exchange_error_code=error_code,
                suggested_fix=error_info['suggested_fix'],
                detail={'raw_message': raw_msg[:500]})
        raise


# ============================================================
# Main daemon
# ============================================================
def main():
    log("=== DAEMON START ===")
    from watchdog_helper import init_watchdog
    init_watchdog(interval_sec=10)

    ex = exchange()
    last_order_ts = 0.0

    # Preload market info for compliance checks
    try:
        info = ecl.get_market_info(ex, SYMBOL)
        log(f"ECL market info: minQty={info['minQty']} stepSize={info['stepSize']} "
            f"tickSize={info['tickSize']} minNotional={info['minNotional']}")
    except Exception as e:
        log(f"ECL market info preload warning: {e}")

    conn = db_conn()
    with conn.cursor() as cur:
        ensure_log_table(cur)
        ensure_close_state_table(cur)
        # Ensure compliance_log + once_lock constraints exist
        try:
            import db_migrations
            db_migrations.ensure_compliance_log(cur)
            db_migrations.ensure_live_order_once_lock(cur)
            db_migrations.ensure_upsert_constraints(cur)
        except Exception:
            pass
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
            # --- Order throttle: load recent attempts from DB ---
            order_throttle._ensure_loaded(cur)

            # --- Guard: trade_switch (ENTRY only) ---
            # EXIT actions (CLOSE, stoploss, REDUCE) always run regardless of switch.
            entry_enabled = trade_switch_on(cur)

            # --- Cleanup expired once_locks ---
            if entry_enabled:
                cur.execute("DELETE FROM live_order_once_lock WHERE expires_at IS NOT NULL AND expires_at <= now();")

            # --- Guard: schedule expiry (auto entry block) ---
            try:
                import test_utils
                from datetime import datetime, timezone
                _test_mode = test_utils.load_test_mode()
                if _test_mode.get('enabled') and _test_mode.get('end_utc'):
                    _end_dt = datetime.fromisoformat(_test_mode['end_utc'])
                    if datetime.now(timezone.utc) >= _end_dt:
                        entry_enabled = False
            except Exception:
                pass

            # --- Bybit position check + emergency stoploss ---
            side, qty, upnl, pct = get_position(ex)
            if side and qty > 0:
                if pct <= EMERGENCY_LOSS_PCT:
                    log(f"EMERGENCY STOPLOSS: pct={pct}% upnl={upnl}")
                    audit(cur, "STOPLOSS_TRIGGERED", SYMBOL, {
                        "side": side, "qty": qty, "pct": pct, "upnl": upnl,
                    })
                    try:
                        order = place_close_order(ex, side, qty, cur=cur)
                    except Exception as e:
                        log(f"STOPLOSS CLOSE FAILED: {e}")
                        audit(cur, "STOPLOSS_FAILED", SYMBOL, {"error": str(e)})
                        return
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
                    try:
                        order = place_close_order(ex, side, qty, cur=cur)
                    except Exception as e:
                        log(f"CLOSE decision id={did} FAILED: {e}")
                        audit(cur, "CLOSE_FAILED", SYMBOL, {
                            "decision_id": did, "error": str(e)})
                        set_last_close_id(cur, did)
                        continue
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

            # --- Execution Queue (from position_manager / strategy) ---
            try:
                _process_execution_queue(ex, cur, side, qty, entry_enabled=entry_enabled)
            except Exception as eq_err:
                log(f"EQ ERROR: {type(eq_err).__name__}: {eq_err}")
                audit(cur, "EQ_ERROR", SYMBOL, {"error": str(eq_err)})

            # --- OPEN signal (entry_enabled gate) ---
            if not entry_enabled:
                return
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

            # Guard: order throttle (replaces simple rate limit)
            throttle_ok, throttle_reason, throttle_meta = order_throttle.check_all(cur, 'OPEN')
            if not throttle_ok:
                log(f"GUARD: throttle — {throttle_reason}")
                audit(cur, "GUARD_BLOCK", SYMBOL, {
                    "signal_id": sig_id, "guard": "order_throttle",
                    "reason": throttle_reason,
                })
                # do NOT mark processed — retry next cycle
                return

            # --- Protection mode check for OPEN ---
            pm_ok, pm_reason = ecl.check_protection_mode_for_action('OPEN')
            if not pm_ok:
                log(f"OPEN blocked by protection mode: {pm_reason}")
                audit(cur, "GUARD_BLOCK", SYMBOL, {
                    "signal_id": sig_id, "guard": "protection_mode",
                    "reason": pm_reason,
                })
                mark_processed(cur, sig_id)
                report = ecl.format_protection_mode_report()
                if report:
                    _send_telegram(report)
                return

            # --- Execute OPEN ---
            order_cap = _get_order_cap(cur)
            usdt_size = min(float(meta.get("qty", order_cap)), order_cap)
            if usdt_size <= 0:
                usdt_size = order_cap

            # Exposure cap enforcement
            usdt_size, cap_reason = enforce_exposure_cap(ex, usdt_size, cur=cur)
            if usdt_size <= 0:
                log(f"OPEN blocked by exposure cap: {cap_reason}")
                audit(cur, "GUARD_BLOCK", SYMBOL, {
                    "signal_id": sig_id, "guard": "exposure_cap",
                    "reason": cap_reason,
                })
                mark_processed(cur, sig_id)
                return

            log(f"OPEN {direction} signal_id={sig_id} usdt={usdt_size}")
            try:
                order, exec_price, amount = place_open_order(ex, direction, usdt_size, cur=cur)
            except Exception as e:
                log(f"OPEN signal_id={sig_id} FAILED: {e}")
                audit(cur, "OPEN_FAILED", SYMBOL, {
                    "signal_id": sig_id, "error": str(e)})
                mark_processed(cur, sig_id)
                return

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
