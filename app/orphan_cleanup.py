"""
orphan_cleanup.py — Orphan order cleanup module.

Detects and cancels orphan orders (active/conditional) when position is flat (qty=0).
Safety: NEVER cancels orders if position exists (hard guard).
All functions are FAIL-OPEN: errors never block trading.

Usage:
    from orphan_cleanup import cleanup_if_flat, pre_flight_cleanup
"""
import os
import time
import urllib.parse
import urllib.request

SYMBOL = 'BTC/USDT:USDT'
LOG_PREFIX = '[orphan_cleanup]'
_DEBOUNCE_SEC = 60
_last_cleanup_ts = 0


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _send_telegram(text):
    try:
        token = os.getenv('TELEGRAM_BOT_TOKEN', '')
        chat_id = os.getenv('TELEGRAM_ALLOWED_CHAT_ID', '')
        if not token or not chat_id:
            return
        url = f'https://api.telegram.org/bot{token}/sendMessage'
        data = urllib.parse.urlencode({
            'chat_id': chat_id,
            'text': text,
            'disable_web_page_preview': 'true'}).encode('utf-8')
        req = urllib.request.Request(url, data=data, method='POST')
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass


def _get_position_qty(ex, symbol):
    """Fetch current position qty from exchange. Returns abs qty (float)."""
    try:
        positions = ex.fetch_positions([symbol])
        for p in positions:
            qty = abs(float(p.get('contracts', 0) or 0))
            if qty > 1e-09:
                return qty
        return 0.0
    except Exception as e:
        _log(f'fetch_positions error: {e}')
        return -1.0  # negative = unknown, do NOT cleanup


def cancel_all_orders_for_symbol(ex, symbol, reason='orphan_cleanup'):
    """Cancel ALL active + conditional orders for symbol.

    Safety guards:
      - HARD GUARD: checks pos_qty first; if > 0 → immediate return (no cancel)
      - Debounce: skips if last cleanup was < 60s ago
      - Idempotent: already-canceled orders are silently OK, 1 retry on error

    Returns: (canceled_active: int, canceled_conditional: int)
    """
    global _last_cleanup_ts

    # Debounce: prevent rapid-fire cleanup
    now = time.time()
    if now - _last_cleanup_ts < _DEBOUNCE_SEC:
        _log(f'debounce: skipping cleanup (last={int(now - _last_cleanup_ts)}s ago)')
        return (0, 0)

    # A3: HARD GUARD — never cancel if position exists
    pos_qty = _get_position_qty(ex, symbol)
    if pos_qty < 0:
        _log('pos_qty unknown — aborting cleanup (FAIL-OPEN)')
        return (0, 0)
    if pos_qty > 1e-09:
        _log(f'pos_qty={pos_qty} > 0 — aborting cleanup (position exists)')
        return (0, 0)

    canceled_active = 0
    canceled_conditional = 0

    # 1. Cancel active orders (limit orders)
    try:
        active_orders = ex.fetch_open_orders(symbol)
        for order in active_orders:
            oid = order.get('id')
            if not oid:
                continue
            for attempt in range(2):
                try:
                    ex.cancel_order(oid, symbol)
                    canceled_active += 1
                    _log(f'canceled active order: {oid}')
                    break
                except Exception as e:
                    err_str = str(e)
                    # Already canceled/filled → OK
                    if '110001' in err_str or 'not found' in err_str.lower():
                        _log(f'active order {oid} already gone (OK)')
                        break
                    if attempt == 0:
                        time.sleep(0.5)
                    else:
                        _log(f'cancel active order {oid} failed (2 attempts): {e}')
    except Exception as e:
        _log(f'fetch_open_orders error: {e}')

    # 2. Cancel conditional orders (stop/TP/SL)
    try:
        cond_orders = ex.fetch_open_orders(symbol, params={'orderFilter': 'StopOrder'})
        for order in cond_orders:
            oid = order.get('id')
            if not oid:
                continue
            for attempt in range(2):
                try:
                    ex.cancel_order(oid, symbol, params={'orderFilter': 'StopOrder'})
                    canceled_conditional += 1
                    _log(f'canceled conditional order: {oid}')
                    break
                except Exception as e:
                    err_str = str(e)
                    if '110001' in err_str or 'not found' in err_str.lower():
                        _log(f'conditional order {oid} already gone (OK)')
                        break
                    if attempt == 0:
                        time.sleep(0.5)
                    else:
                        _log(f'cancel conditional order {oid} failed (2 attempts): {e}')
    except Exception as e:
        _log(f'fetch conditional orders error: {e}')

    _last_cleanup_ts = time.time()

    total = canceled_active + canceled_conditional
    if total > 0:
        msg = (f'[Orphan Cleanup] {symbol}\n'
               f'  active canceled: {canceled_active}\n'
               f'  conditional canceled: {canceled_conditional}\n'
               f'  reason: {reason}')
        _log(msg)
        _send_telegram(msg)

    return (canceled_active, canceled_conditional)


def cleanup_if_flat(ex, symbol, reason='post_exit'):
    """Cancel all orders only if position is flat (qty==0).

    Returns: (cleaned: bool, detail: str)
    """
    pos_qty = _get_position_qty(ex, symbol)
    if pos_qty < 0:
        return (False, 'pos_qty unknown — skipped')
    if pos_qty > 1e-09:
        return (False, f'pos_qty={pos_qty} — position exists, skipped')

    ca, cc = cancel_all_orders_for_symbol(ex, symbol, reason=reason)
    total = ca + cc
    if total > 0:
        return (True, f'canceled {ca} active + {cc} conditional orders')
    return (False, 'no orphan orders found')


def post_exit_cleanup(ex, symbol, caller='event_decision'):
    """Post-exit cleanup for Event Decision Mode.

    Waits 1s, checks if flat, cancels all orphan orders.
    FAIL-OPEN: errors never block trading.

    Returns: (cleaned: bool, detail: str)
    """
    try:
        time.sleep(1)
        pos_qty = _get_position_qty(ex, symbol)
        if pos_qty < 0:
            return (False, 'pos_qty unknown — skipped')
        if pos_qty > 1e-09:
            return (False, f'pos_qty={pos_qty} — position still exists')

        ca, cc = cancel_all_orders_for_symbol(ex, symbol, reason=caller)
        total = ca + cc
        if total > 0:
            msg = (f'[Event Decision Cleanup] {symbol}\n'
                   f'  caller: {caller}\n'
                   f'  canceled: {ca} active + {cc} conditional')
            _log(msg)
            _send_telegram(msg)
            return (True, f'canceled {ca}+{cc} orders')
        return (False, 'no orphan orders')
    except Exception as e:
        _log(f'post_exit_cleanup error (FAIL-OPEN): {e}')
        return (False, f'error: {e}')


def pre_flight_cleanup(ex, cur, symbol):
    """Pre-flight check before new entry. If pos=0 and pending orders exist, cleanup once.

    Called from live_order_executor before OPEN signal execution.
    FAIL-OPEN: any error is logged but does not block entry.
    """
    try:
        pos_qty = _get_position_qty(ex, symbol)
        if pos_qty < 0 or pos_qty > 1e-09:
            return  # position exists or unknown → skip

        # Check if there are any pending orders
        has_orders = False
        try:
            active = ex.fetch_open_orders(symbol)
            if active:
                has_orders = True
        except Exception:
            pass

        if not has_orders:
            try:
                cond = ex.fetch_open_orders(symbol, params={'orderFilter': 'StopOrder'})
                if cond:
                    has_orders = True
            except Exception:
                pass

        if has_orders:
            _log('pre-flight: pos=0 but orders exist — cleanup')
            cancel_all_orders_for_symbol(ex, symbol, reason='pre_flight')
    except Exception as e:
        _log(f'pre_flight_cleanup error (FAIL-OPEN): {e}')
