#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scheduled_liquidation.py — 17:30 KST 일일 강제 청산

standalone 스크립트 + systemd timer 방식.
autopilot OFF 상태에서도 동작. KILL_SWITCH도 무시.

Usage:
  python3 scheduled_liquidation.py --phase=close   # 17:30 KST
  python3 scheduled_liquidation.py --phase=retry   # 17:32 KST
  python3 scheduled_liquidation.py --phase=final   # 17:35 KST
"""

import argparse
import json
import os
import sys
import time
import traceback
import urllib.parse
import urllib.request

import ccxt
from dotenv import load_dotenv

load_dotenv("/root/trading-bot/app/.env")
from db_config import get_conn
import exchange_compliance as ecl

# ============================================================
# Constants
# ============================================================
SYMBOL = "BTC/USDT:USDT"
POLL_FILL_INTERVAL = 2        # seconds between fill polls
POLL_FILL_MAX = 30             # max poll attempts (60s total)
POSITION_VERIFY_DELAY = 2     # seconds before position re-check
DB_RETRY_MAX = 3
LOG_PREFIX = "[SCHED_LIQ]"


def log(msg):
    print(f"{LOG_PREFIX} {msg}", flush=True)


# ============================================================
# Telegram
# ============================================================
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
    try:
        from report_formatter import korean_output_guard
        text = korean_output_guard(text or '')
    except Exception:
        pass
    cfg = _load_tg_config()
    token = cfg.get('TELEGRAM_BOT_TOKEN', '')
    chat_id = cfg.get('TELEGRAM_ALLOWED_CHAT_ID', '')
    if not token or not chat_id:
        log("telegram config missing — skipped")
        return
    try:
        url = f'https://api.telegram.org/bot{token}/sendMessage'
        data = urllib.parse.urlencode({
            'chat_id': chat_id,
            'text': text,
            'disable_web_page_preview': 'true'}).encode('utf-8')
        req = urllib.request.Request(url, data=data, method='POST')
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        log(f"telegram send error: {e}")


# ============================================================
# Exchange
# ============================================================
def exchange():
    return ccxt.bybit({
        "apiKey": os.getenv("BYBIT_API_KEY"),
        "secret": os.getenv("BYBIT_SECRET"),
        "enableRateLimit": True,
        "options": {"defaultType": "swap"},
    })


# ============================================================
# DB helpers
# ============================================================
def _db_conn():
    return get_conn(autocommit=True)


def _db_with_retry(fn):
    """Execute fn(conn, cur) with up to DB_RETRY_MAX retries."""
    for attempt in range(1, DB_RETRY_MAX + 1):
        try:
            conn = _db_conn()
            try:
                with conn.cursor() as cur:
                    return fn(conn, cur)
            finally:
                conn.close()
        except Exception as e:
            log(f"DB error (attempt {attempt}/{DB_RETRY_MAX}): {e}")
            if attempt == DB_RETRY_MAX:
                raise
            time.sleep(1)


# ============================================================
# Position
# ============================================================
def get_position(ex):
    """Returns (side, qty, unrealised_pnl, entry_price, mark_price)."""
    positions = ex.fetch_positions([SYMBOL])
    for p in positions:
        if p.get("symbol") != SYMBOL:
            continue
        contracts = float(p.get("contracts") or 0.0)
        side = p.get("side")
        upnl = float(p.get("unrealizedPnl") or 0.0)
        entry = float(p.get("entryPrice") or 0.0)
        mark = float(p.get("markPrice") or 0.0)
        if contracts != 0.0 and side in ("long", "short"):
            return side, contracts, upnl, entry, mark
    return None, 0.0, 0.0, 0.0, 0.0


def verify_position_zero(ex):
    """Wait and verify position is flat. Returns True if flat."""
    time.sleep(POSITION_VERIFY_DELAY)
    side, qty, _, _, _ = get_position(ex)
    return side is None or qty == 0.0


# ============================================================
# Order placement (ECL compliance)
# ============================================================
def place_close_order(ex, side, qty, cur=None):
    """reduceOnly market close with ECL compliance. Returns order dict."""
    order_params = {
        'action': 'SELL' if side == 'long' else 'BUY',
        'qty': qty,
        'side': side,
        'reduce_only': True,
        'order_type': 'market',
        'position_qty': qty,
        'usdt_value': 0,
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
        return order
    except Exception as e:
        error_code, raw_msg = ecl.extract_bybit_error_code(e)
        error_info = ecl.map_bybit_error(error_code, raw_msg)
        log(f"BYBIT ERROR close: code={error_code} {error_info['korean_message']}")
        _send_telegram(ecl.format_rejection_telegram(error_info))
        ecl.record_error(SYMBOL, error_code=error_code)
        if ecl.should_refresh_on_error(error_code):
            ecl.force_refresh_market_info(ex, SYMBOL, reason=f'sched close error {error_code}')
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
# Fill polling
# ============================================================
def poll_fill(ex, order_id):
    """Poll order fill status. Returns (filled, avg_price, fee_cost, fee_currency) or None."""
    for i in range(POLL_FILL_MAX):
        time.sleep(POLL_FILL_INTERVAL)
        fetched = None
        try:
            fetched = ex.fetch_closed_order(order_id, SYMBOL)
        except Exception:
            try:
                fetched = ex.fetch_order(order_id, SYMBOL)
            except Exception:
                log(f"poll_fill attempt {i+1}: fetch error")
                continue

        if fetched is None:
            continue

        fx_status = fetched.get('status', '')
        filled_qty = float(fetched.get('filled', 0) or 0)
        avg_price = float(fetched.get('average', 0) or 0)
        fee_info = fetched.get('fee', {}) or {}
        fee_cost = float(fee_info.get('cost', 0) or 0)
        fee_currency = fee_info.get('currency', '')

        if fx_status in ('closed', 'filled') or (filled_qty > 0 and fx_status != 'open'):
            log(f"poll_fill: FILLED qty={filled_qty} price={avg_price}")
            return {
                'filled_qty': filled_qty,
                'avg_price': avg_price,
                'fee_cost': fee_cost,
                'fee_currency': fee_currency,
                'raw': fetched,
            }
        log(f"poll_fill attempt {i+1}/{POLL_FILL_MAX}: status={fx_status}")

    log("poll_fill: TIMEOUT after 60s")
    return None


# ============================================================
# DB logging
# ============================================================
def _insert_exec_log(cur, order, direction, qty, entry_price):
    """Insert execution_log with order_type=SCHEDULED_CLOSE."""
    cur.execute("""
        INSERT INTO execution_log
            (order_id, symbol, order_type, direction,
             close_reason, requested_qty, ticker_price,
             status, raw_order_response,
             source_queue)
        VALUES (%s, %s, 'SCHEDULED_CLOSE', %s, %s, %s, %s, 'SENT', %s::jsonb, %s)
        RETURNING id;
    """, (
        order.get("id", ""),
        SYMBOL,
        direction,
        "17:30 KST 일일 강제 청산",
        qty,
        entry_price,
        json.dumps(order, default=str),
        "scheduled_liquidation",
    ))
    row = cur.fetchone()
    return row[0] if row else None


def _update_exec_log_filled(cur, eid, fill_info, realized_pnl):
    """Update execution_log after fill verified."""
    cur.execute("""
        UPDATE execution_log SET
            status = 'VERIFIED',
            filled_qty = %s,
            avg_fill_price = %s,
            fee = %s,
            fee_currency = %s,
            first_fill_at = now(),
            last_fill_at = now(),
            position_after_side = NULL,
            position_after_qty = 0,
            position_verified = true,
            verified_at = now(),
            realized_pnl = %s,
            raw_fetch_response = %s::jsonb
        WHERE id = %s;
    """, (
        fill_info['filled_qty'],
        fill_info['avg_price'],
        fill_info['fee_cost'],
        fill_info['fee_currency'],
        realized_pnl,
        json.dumps(fill_info.get('raw', {}), default=str),
        eid,
    ))


def _update_exec_log_timeout(cur, eid):
    """Mark execution_log as TIMEOUT."""
    cur.execute("""
        UPDATE execution_log SET status = 'TIMEOUT', error_detail = 'scheduled_close_timeout'
        WHERE id = %s;
    """, (eid,))


def _insert_pm_decision_log(cur, side, qty, entry_price, current_price):
    """Insert pm_decision_log with chosen_action=SCHEDULED_CLOSE."""
    cur.execute("""
        INSERT INTO pm_decision_log
            (symbol, position_side, position_qty, avg_entry_price,
             current_price, chosen_action, action_reason,
             full_context, actor, candidate_action, final_action)
        VALUES (%s, %s, %s, %s, %s, 'SCHEDULED_CLOSE',
                '17:30 KST 일일 강제 청산',
                %s::jsonb, %s, %s, %s)
        RETURNING id;
    """, (
        SYMBOL, side, qty, entry_price, current_price,
        json.dumps({'trigger': 'daily_scheduled_close', 'time': '17:30_KST'}, default=str),
        'sched_liq',           # actor — VARCHAR(20) limit
        'SCHED_CLOSE',         # candidate_action — VARCHAR(20) limit
        'SCHED_CLOSE',         # final_action — VARCHAR(20) limit
    ))
    row = cur.fetchone()
    return row[0] if row else None


def _sync_position_state(cur):
    """Reset position_state to flat after close."""
    cur.execute("""
        UPDATE position_state SET
            side = NULL, total_qty = 0, stage = 0,
            capital_used_usdt = 0, trade_budget_used_pct = 0,
            stage_consumed_mask = 0, accumulated_entry_fee = 0,
            updated_at = now()
        WHERE symbol = %s;
    """, (SYMBOL,))


# ============================================================
# Trade switch
# ============================================================
def set_trade_switch(enabled):
    """Update trade_switch table."""
    def _do(conn, cur):
        cur.execute(
            "UPDATE trade_switch SET enabled=%s, updated_at=NOW() WHERE id=1;",
            (enabled,))
    _db_with_retry(_do)


# ============================================================
# Phase handlers
# ============================================================
def _phase_close(ex):
    """phase=close (17:30 KST): close position + record + report."""
    side, qty, upnl, entry_price, mark_price = get_position(ex)
    if not side or qty == 0.0:
        log("No position — nothing to close")
        _send_telegram("일일 정산 확인 — 포지션 없음\n신규 진입 차단 완료")
        set_trade_switch(False)
        return True

    log(f"Position detected: {side} qty={qty} entry={entry_price} mark={mark_price}")
    direction = side.upper()
    eid = None
    fill_info = None

    # Place close order (separate from DB to avoid double-ordering on DB error)
    order = None
    try:
        order = place_close_order(ex, side, qty)
        order_id = order.get("id", "")
        log(f"Close order sent: id={order_id}")
    except Exception as e:
        log(f"Close order failed: {e}")
        traceback.print_exc()
        _send_telegram(
            f"일일 정산 주문 실패\n"
            f"- 방향: {side}\n"
            f"- 수량: {qty} BTC\n"
            f"- 오류: {type(e).__name__}: {str(e)[:100]}\n"
            f"- retry phase(17:32)에서 재시도 예정")
        set_trade_switch(False)
        return False

    # DB logging (order already placed — safe to retry)
    try:
        def _do_db_log(conn, cur):
            log_id = _insert_exec_log(cur, order, direction, qty, entry_price)
            _insert_pm_decision_log(cur, side, qty, entry_price, mark_price)
            return log_id
        eid = _db_with_retry(_do_db_log)
    except Exception as e:
        log(f"DB logging failed (order already sent): {e}")

    # Poll fill
    fill_info = poll_fill(ex, order_id)

    if fill_info:
        # Verify position zero
        is_flat = verify_position_zero(ex)

        # Calculate PnL
        realized_pnl = None
        if entry_price and entry_price > 0:
            dir_sign = 1 if side == "long" else -1
            gross_pnl = (fill_info['avg_price'] - entry_price) * fill_info['filled_qty'] * dir_sign
            realized_pnl = gross_pnl - abs(fill_info['fee_cost'])
            # Include accumulated entry fee
            try:
                def _get_acc_fee(conn, cur):
                    cur.execute('SELECT accumulated_entry_fee FROM position_state WHERE symbol = %s;', (SYMBOL,))
                    row = cur.fetchone()
                    return float(row[0]) if row and row[0] else 0.0
                acc_fee = _db_with_retry(_get_acc_fee)
                realized_pnl -= acc_fee
            except Exception:
                pass

        # Update DB
        try:
            def _do_update(conn, cur):
                if eid:
                    _update_exec_log_filled(cur, eid, fill_info, realized_pnl)
                if is_flat:
                    _sync_position_state(cur)
            _db_with_retry(_do_update)
        except Exception as e:
            log(f"DB update error (fill recorded on exchange): {e}")

        # Telegram — CLOSE_FILLED 증빙
        pnl_str = f"{realized_pnl:+.4f}" if realized_pnl is not None else "N/A"
        flat_status = 'FLAT' if is_flat else 'NOT_FLAT'
        remaining_qty = 0 if is_flat else qty - fill_info['filled_qty']
        _send_telegram(
            f"일일 정산 완료 [CLOSE_FILLED]\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"- 주문ID: {order_id}\n"
            f"- 체결가: ${fill_info['avg_price']:,.2f}\n"
            f"- 체결수량: {fill_info['filled_qty']} BTC\n"
            f"- 수수료: {fill_info['fee_cost']} {fill_info['fee_currency']}\n"
            f"- 실현 손익: {pnl_str} USDT\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"- 잔여포지션: {remaining_qty} ({flat_status})\n"
            f"- 포지션검증: {flat_status}\n"
            f"- 신규 진입 차단 완료")
        log(f"Close VERIFIED: price={fill_info['avg_price']} pnl={pnl_str} flat={is_flat}")

        # Partial fill → retry remaining
        if not is_flat and fill_info['filled_qty'] < qty:
            log(f"Partial fill detected: filled={fill_info['filled_qty']} requested={qty}")
            try:
                remain = qty - fill_info['filled_qty']
                retry_order = place_close_order(ex, side, remain)
                retry_fill = poll_fill(ex, retry_order.get("id", ""))
                if retry_fill:
                    _send_telegram(
                        f"잔여 청산 완료\n"
                        f"- 추가 체결: {retry_fill['filled_qty']} BTC @ ${retry_fill['avg_price']:,.2f}")
            except Exception as e:
                log(f"Partial fill retry failed: {e}")
    else:
        # Timeout
        try:
            def _do_timeout(conn, cur):
                if eid:
                    _update_exec_log_timeout(cur, eid)
            _db_with_retry(_do_timeout)
        except Exception:
            pass
        _send_telegram(
            f"일일 정산 주문 체결 대기 시간 초과\n"
            f"- 주문ID: {order_id}\n"
            f"- retry phase(17:32)에서 재시도 예정")
        log("Close order TIMEOUT")

    # Always disable trade switch
    set_trade_switch(False)
    return fill_info is not None


def _phase_retry(ex):
    """phase=retry (17:32 KST): re-check and retry if position remains."""
    side, qty, upnl, entry_price, mark_price = get_position(ex)
    if not side or qty == 0.0:
        log("Retry check: position is flat")
        _send_telegram("일일 정산 재확인 — 포지션 없음 (정상)")
        set_trade_switch(False)
        return True

    log(f"Retry: position still open — {side} qty={qty}")
    _send_telegram(
        f"포지션 잔존 — 재청산 시도 중\n"
        f"- 방향: {side}\n"
        f"- 수량: {qty} BTC")

    # Same close flow
    return _phase_close(ex)


def _phase_final(ex):
    """phase=final (17:35 KST): final check, escalate if still open."""
    side, qty, upnl, entry_price, mark_price = get_position(ex)
    if not side or qty == 0.0:
        log("Final check: position is flat")
        _send_telegram("일일 정산 최종 확인 — 포지션 없음 (정상)")
        set_trade_switch(False)
        return True

    log(f"FINAL: position STILL open — {side} qty={qty} mark={mark_price}")
    _send_telegram(
        f"🚨 [CRITICAL] 일일 정산 최종 실패\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"- 3차 확인 후에도 포지션 잔존\n"
        f"- 방향: {side} / 수량: {qty} BTC\n"
        f"- 현재가: ${mark_price:,.2f}\n"
        f"- 미실현 손익: {upnl:+.4f} USDT\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"⚠ 수동 개입 필요\n"
        f"python3 /root/trading-bot/app/panic_close.py")

    # DB 기록: CRITICAL 이벤트
    try:
        def _do_critical_log(conn, cur):
            cur.execute("""
                INSERT INTO error_log (service, level, message)
                VALUES ('scheduled_liquidation', 'CRITICAL',
                        %s);
            """, (f'17:30 청산 최종 실패: {side} qty={qty} mark={mark_price}',))
        _db_with_retry(_do_critical_log)
    except Exception:
        pass

    # Last-ditch attempt
    try:
        return _phase_close(ex)
    except Exception as e:
        log(f"Final close attempt failed: {e}")
        return False


# ============================================================
# Main
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="Scheduled daily liquidation")
    parser.add_argument("--phase", required=True, choices=["close", "retry", "final"],
                        help="Execution phase: close/retry/final")
    args = parser.parse_args()

    log(f"=== Starting phase={args.phase} ===")

    try:
        ex = exchange()
    except Exception as e:
        log(f"Exchange init failed: {e}")
        _send_telegram(
            f"일일 정산 오류 — 거래소 연결 실패\n"
            f"- phase: {args.phase}\n"
            f"- 오류: {type(e).__name__}: {str(e)[:100]}")
        return 1

    try:
        if args.phase == "close":
            ok = _phase_close(ex)
        elif args.phase == "retry":
            ok = _phase_retry(ex)
        elif args.phase == "final":
            ok = _phase_final(ex)
        else:
            ok = False

        log(f"=== phase={args.phase} finished (ok={ok}) ===")
        return 0 if ok else 1

    except Exception as e:
        log(f"Unhandled error in phase={args.phase}: {e}")
        traceback.print_exc()
        _send_telegram(
            f"일일 정산 오류\n"
            f"- phase: {args.phase}\n"
            f"- 오류: {type(e).__name__}: {str(e)[:200]}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
