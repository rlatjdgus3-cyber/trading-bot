"""
ws_panic_guard.py — WebSocket-based real-time PanicGuard daemon (D1-1).

실시간(100-500ms) 급락 감지 + 단계적 포지션 보호.
websocket-client를 사용하여 Bybit linear WS에 직접 연결.

Actions (reduce_only ONLY):
  - TIGHTEN_STOP: SL 재조정 (server_stop_manager 호출)
  - REDUCE_HALF: 50% 감축 (ccxt reduce_only 시장가)
  - CLOSE_ALL: 전량 청산 (ccxt reduce_only 시장가)

안전 원칙:
  - reduce_only 주문만 실행 (신규 진입 절대 불가)
  - KILL_SWITCH 파일 감지 시 종료
  - WS 끊김 시 자동 재연결 + 텔레그램 경고
  - 60초 쿨다운 (중복 폭주 방지)
"""

import os
import sys
import time
import json
import signal
import threading
import traceback
import urllib.parse
import urllib.request

sys.path.insert(0, '/root/trading-bot/app')

import ccxt
import websocket

LOG_PREFIX = '[WS_PANIC]'
SYMBOL = os.getenv('SYMBOL', 'BTC/USDT:USDT')
# Derive BYBIT_SYMBOL from SYMBOL (BTC/USDT:USDT → BTCUSDT)
BYBIT_SYMBOL = SYMBOL.replace('/', '').replace(':USDT', '').replace(':USD', '')
KILL_SWITCH_PATH = '/root/trading-bot/app/KILL_SWITCH'

# WS endpoint
WS_URL = 'wss://stream.bybit.com/v5/public/linear'

# In-memory state (all protected by _tick_lock)
_last_action_ts = 0
_ws_connected = False
_rolling_prices = []  # (ts, price) pairs
_running = True
_tick_lock = threading.Lock()

# Track last known position entry to detect new position opens
_last_known_entry_price = 0.0
_exchange_position_zero = False


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _db_conn():
    from db_config import get_conn
    return get_conn()


def _load_tg_config():
    cfg = {}
    try:
        with open('/root/trading-bot/app/telegram_cmd.env') as f:
            for line in f:
                line = line.strip()
                if '=' in line and not line.startswith('#'):
                    k, v = line.split('=', 1)
                    cfg[k.strip()] = v.strip()
    except Exception:
        pass
    return cfg


def _send_telegram(text):
    try:
        from report_formatter import korean_output_guard
        text = korean_output_guard(text)
    except Exception:
        pass
    cfg = _load_tg_config()
    token = cfg.get('TELEGRAM_BOT_TOKEN', '')
    chat_id = cfg.get('TELEGRAM_ALLOWED_CHAT_ID', '')
    if not token or not chat_id:
        return
    try:
        url = f'https://api.telegram.org/bot{token}/sendMessage'
        data = urllib.parse.urlencode({
            'chat_id': chat_id,
            'text': text,
            'disable_web_page_preview': 'true'}).encode('utf-8')
        req = urllib.request.Request(url, data=data, method='POST')
        with urllib.request.urlopen(req, timeout=5) as resp:
            resp.read()
    except Exception as e:
        _log(f'telegram error: {e}')


def _get_exchange():
    ex = ccxt.bybit({
        'apiKey': os.getenv('BYBIT_API_KEY'),
        'secret': os.getenv('BYBIT_SECRET'),
        'enableRateLimit': True,
        'timeout': 20000,
        'options': {'defaultType': 'swap'}})
    ex.load_markets()
    return ex


def _get_config():
    """Load panic guard config from strategy_modes.yaml."""
    try:
        from strategy_v3 import config_v3
        cfg = config_v3.get_all()
    except Exception as e:
        _log(f'config load fallback to defaults: {e}')
        cfg = {}
    return {
        'tighten_pct': cfg.get('panic_guard_tighten_pct', 0.35),
        'reduce_pct': cfg.get('panic_guard_reduce_pct', 0.60),
        'close_pct': cfg.get('panic_guard_close_pct', 1.00),
        'cooldown_sec': cfg.get('panic_guard_cooldown_sec', 60),
    }


def _get_position(cur):
    """Read current position from DB."""
    cur.execute("""
        SELECT side, total_qty, avg_entry_price
        FROM position_state WHERE symbol = %s;
    """, (SYMBOL,))
    row = cur.fetchone()
    if not row or not row[0] or float(row[1] or 0) <= 0:
        return None
    return {
        'side': row[0],
        'qty': float(row[1]),
        'entry_price': float(row[2]) if row[2] else 0,
    }


def _compute_adverse_ret(price, position):
    """Compute adverse return % based on position side."""
    entry = position['entry_price']
    if entry <= 0:
        return 0
    side = position['side'].lower()
    ret = (price - entry) / entry * 100
    return -ret if side == 'long' else ret


def _compute_1m_rolling_ret(price):
    """Compute 1-minute rolling return from in-memory price buffer."""
    global _rolling_prices
    now = time.time()
    _rolling_prices.append((now, price))

    # Keep only last 90 seconds
    cutoff = now - 90
    _rolling_prices = [(t, p) for t, p in _rolling_prices if t >= cutoff]

    # Find price ~60 seconds ago
    target_ts = now - 60
    best = None
    best_diff = float('inf')
    for t, p in _rolling_prices:
        diff = abs(t - target_ts)
        if diff < best_diff:
            best_diff = diff
            best = p

    if best is None or best <= 0:
        return 0
    return (price - best) / best * 100


def _execute_reduce_only(ex, side, qty, reduce_type='CLOSE_ALL'):
    """Execute reduce_only market order. ONLY reduces position."""
    global _exchange_position_zero
    try:
        order_side = 'sell' if side.lower() == 'long' else 'buy'
        params = {
            'reduceOnly': True,
            'positionIdx': 0,
        }
        order = ex.create_order(
            symbol=SYMBOL,
            type='market',
            side=order_side,
            amount=qty,
            params=params,
        )
        _log(f'{reduce_type}: order placed — {order_side} {qty} '
             f'(id={order.get("id", "?")})')
        _exchange_position_zero = False
        return True
    except Exception as e:
        err_str = str(e)
        if '110017' in err_str:
            _log(f'{reduce_type}: exchange position is zero (110017) — skipping until DB updates')
            _exchange_position_zero = True
            return False
        _log(f'{reduce_type} ORDER ERROR: {e}')
        return False


def _handle_price_tick(price, ex):
    """Process each price tick: check position, compute adverse return, act."""
    global _last_action_ts, _exchange_position_zero, _last_known_entry_price

    cfg = _get_config()
    now = time.time()

    # Cooldown check
    if now - _last_action_ts < cfg['cooldown_sec']:
        return

    conn = None
    try:
        conn = _db_conn()
        conn.autocommit = True
        with conn.cursor() as cur:
            position = _get_position(cur)
            if not position:
                _exchange_position_zero = False
                _last_known_entry_price = 0.0
                return

            # FIX: Reset _exchange_position_zero when a NEW position is detected
            # (different entry_price = new or modified position)
            current_entry = position.get('entry_price', 0)
            if _exchange_position_zero:
                if abs(current_entry - _last_known_entry_price) > 0.01:
                    # New position opened — reset the flag
                    _log(f'New position detected (entry {_last_known_entry_price:.1f} → {current_entry:.1f}), re-enabling guard')
                    _exchange_position_zero = False
                else:
                    # Same stale position in DB, still skip
                    return
            _last_known_entry_price = current_entry

            # Compute adverse return from entry price
            adverse_ret = _compute_adverse_ret(price, position)

            # Also check 1m rolling return
            rolling_ret_1m = _compute_1m_rolling_ret(price)

            # Use the worse of the two for triggering
            side = position['side'].lower()
            rolling_adverse = -rolling_ret_1m if side == 'long' else rolling_ret_1m
            effective_adverse = max(adverse_ret, rolling_adverse)

            if effective_adverse < cfg['tighten_pct']:
                return

            # Determine action
            if effective_adverse >= cfg['close_pct']:
                action = 'CLOSE_ALL'
            elif effective_adverse >= cfg['reduce_pct']:
                action = 'REDUCE_HALF'
            else:
                action = 'TIGHTEN_STOP'

            _last_action_ts = now
            _log(f'ACTION: {action} | adverse={effective_adverse:.3f}% '
                 f'| entry_adverse={adverse_ret:.3f}% | rolling_1m={rolling_adverse:.3f}% '
                 f'| price={price}')

            # Execute action
            if action == 'CLOSE_ALL':
                success = _execute_reduce_only(
                    ex, position['side'], position['qty'], 'CLOSE_ALL')
                if success:
                    _send_telegram(
                        f'PANIC GUARD CLOSE_ALL\n'
                        f'Side: {position["side"]} | Adverse: {effective_adverse:.2f}%\n'
                        f'Price: {price:.1f} | Qty: {position["qty"]:.4f}')

            elif action == 'REDUCE_HALF':
                half_qty = round(position['qty'] * 0.5, 4)
                if half_qty > 0:
                    success = _execute_reduce_only(
                        ex, position['side'], half_qty, 'REDUCE_HALF')
                    if success:
                        _send_telegram(
                            f'PANIC GUARD REDUCE_HALF\n'
                            f'Side: {position["side"]} | Adverse: {effective_adverse:.2f}%\n'
                            f'Price: {price:.1f} | Reduced: {half_qty:.4f}')

            elif action == 'TIGHTEN_STOP':
                try:
                    import server_stop_manager
                    ssm = server_stop_manager.get_manager()
                    entry = position.get('entry_price', 0)
                    pos_side = position.get('side', '').lower()
                    tighten_sl_pct = cfg.get('tighten_pct', 0.35)
                    if entry > 0 and pos_side:
                        if pos_side == 'long':
                            sl_price = price * (1 - tighten_sl_pct / 100)
                        else:
                            sl_price = price * (1 + tighten_sl_pct / 100)
                        ssm.sync_stop_order(cur, position, sl_price=sl_price)
                        _log(f'TIGHTEN_STOP: SL re-synced @ {sl_price:.1f}')
                    else:
                        _log('TIGHTEN_STOP: skipped (no entry or side)')
                except Exception as e:
                    _log(f'TIGHTEN_STOP error: {e}')

            # Record event
            try:
                import shock_guard
                shock_guard.record_panic_event(
                    cur, action, effective_adverse, price, position['side'])
            except Exception as e:
                _log(f'record event error: {e}')

    except Exception as e:
        _log(f'handle_price_tick error: {e}')
        traceback.print_exc()
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _on_ws_message(ws, message):
    """Handle incoming WS message."""
    try:
        data = json.loads(message)
        topic = data.get('topic', '')
        if not topic.startswith('kline.'):
            return

        kline_data = data.get('data', [])
        if not kline_data:
            return

        for kline in kline_data:
            close_price = float(kline.get('close', 0))
            if close_price > 0:
                if _tick_lock.acquire(blocking=False):
                    try:
                        _handle_price_tick(close_price, _exchange_instance)
                    finally:
                        _tick_lock.release()

    except Exception as e:
        _log(f'ws message error: {e} | len={len(message) if message else 0}')


def _on_ws_error(ws, error):
    global _ws_connected
    _ws_connected = False
    _log(f'WS ERROR: {error}')


def _on_ws_close(ws, close_status_code, close_msg):
    global _ws_connected
    _ws_connected = False
    _log(f'WS CLOSED: code={close_status_code} msg={close_msg}')
    # Only send telegram if we're still running (not intentional shutdown)
    if _running:
        _send_telegram('WS PanicGuard: connection closed, reconnecting...')


def _on_ws_open(ws):
    global _ws_connected
    _ws_connected = True
    _log('WS CONNECTED')

    sub_msg = json.dumps({
        'op': 'subscribe',
        'args': [f'kline.1.{BYBIT_SYMBOL}']
    })
    ws.send(sub_msg)
    _log(f'Subscribed to kline.1.{BYBIT_SYMBOL}')


_exchange_instance = None
_ws_instance = None  # Track current WS for graceful shutdown


def _handle_signal(signum, frame):
    """Graceful shutdown on SIGTERM/SIGINT."""
    global _running
    _running = False
    _log(f'Signal {signum} received, shutting down...')
    # Close WS to unblock run_forever()
    if _ws_instance:
        try:
            _ws_instance.keep_running = False
            _ws_instance.close()
        except Exception:
            pass
        # Fallback: forcefully close underlying socket if close() doesn't work
        try:
            if _ws_instance.sock:
                _ws_instance.sock.close()
        except Exception:
            pass
    # Hard exit fallback — if run_forever() still blocks, force exit after 5s
    def _force_exit():
        time.sleep(5)
        _log('Force exit after 5s timeout')
        os._exit(0)
    t = threading.Thread(target=_force_exit, daemon=True)
    t.start()


def main():
    global _exchange_instance, _running, _ws_instance

    _log('=== WS PANIC GUARD START ===')

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    # Check feature flag
    try:
        import feature_flags
        if not feature_flags.is_enabled('ff_panic_guard_ws'):
            _log('ff_panic_guard_ws is OFF. Exiting.')
            return
    except Exception as e:
        _log(f'feature_flags check error: {e}')
        return

    # Initialize exchange
    try:
        _exchange_instance = _get_exchange()
        _log('Exchange connected')
    except Exception as e:
        _log(f'Exchange init error: {e}')
        return

    while _running:
        # KILL_SWITCH check
        if os.path.exists(KILL_SWITCH_PATH):
            _log('KILL_SWITCH detected. Exiting.')
            break

        try:
            _log('Connecting to Bybit WebSocket...')
            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=_on_ws_open,
                on_message=_on_ws_message,
                on_error=_on_ws_error,
                on_close=_on_ws_close,
            )
            _ws_instance = ws  # Store for signal handler

            # Rely on the outer while loop for reconnection.
            ws.run_forever(
                ping_interval=20,
                ping_timeout=10,
            )

        except Exception as e:
            _log(f'WS run error: {e}')
            traceback.print_exc()

        if not _running:
            break

        # KILL_SWITCH re-check before reconnect
        if os.path.exists(KILL_SWITCH_PATH):
            _log('KILL_SWITCH detected after disconnect. Exiting.')
            break

        _log('Reconnecting in 3 seconds...')
        time.sleep(3)

    _log('=== WS PANIC GUARD STOPPED ===')


if __name__ == '__main__':
    main()
