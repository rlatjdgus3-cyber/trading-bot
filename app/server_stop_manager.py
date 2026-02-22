"""
server_stop_manager.py — Server-side reduce-only STOP-MARKET order management.

P0-2: Places and maintains exchange-side conditional stop orders as a safety net.
The server stop is set 0.1% wider than the Python SL to prevent double execution.
All orders use reduce-only mode to prevent position reversal.

FAIL-OPEN: Errors in this module never block trading, only log warnings.
"""

import os
import sys
import time
import json
import traceback

sys.path.insert(0, '/root/trading-bot/app')

LOG_PREFIX = '[server_stop]'
SYMBOL = os.getenv('SYMBOL', 'BTC/USDT:USDT')
_VERIFY_INTERVAL_SEC = 30
_last_verify_ts = 0


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _get_exchange():
    """Create fresh ccxt.bybit instance for stop order management."""
    import ccxt
    return ccxt.bybit({
        'apiKey': os.getenv('BYBIT_API_KEY'),
        'secret': os.getenv('BYBIT_SECRET'),
        'enableRateLimit': True,
        'timeout': 15000,
        'options': {'defaultType': 'swap'},
    })


def _notify_telegram(text):
    """Send telegram notification."""
    try:
        import urllib.parse
        import urllib.request
        from dotenv import load_dotenv
        load_dotenv('/root/trading-bot/app/telegram_cmd.env')
        token = os.getenv('TELEGRAM_BOT_TOKEN')
        chat_id = os.getenv('TELEGRAM_ALLOWED_CHAT_ID')
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


class ServerStopManager:
    """Manages server-side conditional stop-market orders on Bybit."""

    def __init__(self, symbol=SYMBOL):
        self.symbol = symbol
        self._exchange = None
        self._version = 0
        self._last_order_id = None
        self._last_stop_price = None
        self._last_verify_ts = 0

    def _ex(self):
        if self._exchange is None:
            self._exchange = _get_exchange()
        return self._exchange

    def sync_stop_order(self, cur, position, sl_price):
        """Main entry point: ensure server-side stop exists and matches.

        Args:
            cur: DB cursor
            position: dict with {side, qty, entry_price}
            sl_price: Python-calculated stop-loss price

        Returns: (synced: bool, detail: str)
        """
        import feature_flags
        if not feature_flags.is_enabled('ff_server_stop_orders'):
            return (True, 'ff_server_stop_orders OFF')

        try:
            side = position.get('side', '').lower()
            qty = float(position.get('qty', 0))
            if not side or qty <= 0 or not sl_price or sl_price <= 0:
                return (True, 'no position or invalid params')

            # Server stop is 0.1% wider than Python SL
            from strategy_v3 import config_v3
            cfg = config_v3.get_all()
            offset_pct = cfg.get('server_stop_offset_pct', 0.1)

            if side == 'long':
                # For long: stop triggers below, so set lower
                server_stop_price = round(sl_price * (1 - offset_pct / 100), 2)
            else:
                # For short: stop triggers above, so set higher
                server_stop_price = round(sl_price * (1 + offset_pct / 100), 2)

            # Check if existing stop is close enough (within 0.05%)
            if (self._last_stop_price and self._last_order_id and
                    abs(server_stop_price - self._last_stop_price) / server_stop_price < 0.0005):
                # Periodic verify
                if time.time() - self._last_verify_ts >= _VERIFY_INTERVAL_SEC:
                    verified = self._verify_stop_exists()
                    if not verified:
                        _log('STOP MISSING on verify — recreating')
                        _notify_telegram('⚠ Server STOP missing — recreating')
                    else:
                        return (True, f'stop in sync @ {server_stop_price}')
                else:
                    return (True, f'stop in sync @ {server_stop_price}')

            # Cancel existing stop if price changed
            if self._last_order_id:
                self._cancel_stop(self._last_order_id)

            # Place new stop
            close_side = 'sell' if side == 'long' else 'buy'
            success, order_id = self._place_stop_market(
                close_side, qty, server_stop_price, side)

            if success:
                self._last_order_id = order_id
                self._last_stop_price = server_stop_price
                self._version += 1
                # DB record
                self._db_record(cur, order_id, close_side, qty,
                                server_stop_price, 'ACTIVE')
                return (True, f'stop placed @ {server_stop_price} (id={order_id})')
            else:
                _log(f'STOP placement failed: {order_id}')
                return (False, f'placement failed: {order_id}')

        except Exception as e:
            _log(f'sync_stop_order error (FAIL-OPEN): {e}')
            traceback.print_exc()
            return (False, f'error: {e}')

    def _place_stop_market(self, close_side, qty, stop_price, position_side):
        """Place reduce-only conditional stop-market order on Bybit.

        Returns: (success: bool, order_id_or_error: str)
        """
        try:
            self._version += 1
            client_id = f'STOP:{self.symbol}:{close_side}:{self._version}'

            # triggerDirection: 1=rise-above, 2=fall-below
            if position_side == 'long':
                trigger_direction = 2  # fall-below for long SL
            else:
                trigger_direction = 1  # rise-above for short SL

            order = self._ex().create_order(
                self.symbol, 'market', close_side, qty,
                params={
                    'triggerPrice': str(stop_price),
                    'triggerDirection': trigger_direction,
                    'reduceOnly': True,
                    'orderFilter': 'StopOrder',
                    'clientOrderId': client_id,
                })

            order_id = order.get('id', '') or order.get('info', {}).get('orderId', '')
            _log(f'STOP placed: side={close_side} qty={qty} '
                 f'trigger={stop_price} id={order_id}')
            return (True, order_id)

        except Exception as e:
            _log(f'_place_stop_market error: {e}')
            return (False, str(e))

    def _cancel_stop(self, order_id):
        """Cancel existing conditional stop order."""
        try:
            self._ex().cancel_order(order_id, self.symbol, params={
                'orderFilter': 'StopOrder',
            })
            _log(f'STOP cancelled: {order_id}')
            return True
        except Exception as e:
            _log(f'_cancel_stop error (non-fatal): {e}')
            return False

    def _verify_stop_exists(self):
        """Verify server-side stop still exists. Called every 30s."""
        self._last_verify_ts = time.time()
        try:
            orders = self._ex().fetch_open_orders(self.symbol, params={
                'orderFilter': 'StopOrder',
            })
            for o in orders:
                oid = o.get('id', '') or o.get('info', {}).get('orderId', '')
                if oid == self._last_order_id:
                    return True
            return False
        except Exception as e:
            _log(f'_verify_stop_exists error: {e}')
            return True  # FAIL-OPEN: assume exists on error

    def cancel_all_stops(self, cur=None):
        """Cancel all server-side stops for this symbol. Called on position close."""
        try:
            orders = self._ex().fetch_open_orders(self.symbol, params={
                'orderFilter': 'StopOrder',
            })
            cancelled = 0
            for o in orders:
                oid = o.get('id', '') or o.get('info', {}).get('orderId', '')
                if oid:
                    self._cancel_stop(oid)
                    if cur:
                        self._db_update_status(cur, oid, 'CANCELLED',
                                               'position_closed')
                    cancelled += 1
            self._last_order_id = None
            self._last_stop_price = None
            _log(f'cancel_all_stops: {cancelled} cancelled')
            return cancelled
        except Exception as e:
            _log(f'cancel_all_stops error: {e}')
            return 0

    def get_status(self):
        """Return status dict for /debug stop_orders."""
        return {
            'symbol': self.symbol,
            'last_order_id': self._last_order_id,
            'last_stop_price': self._last_stop_price,
            'version': self._version,
            'last_verify_ts': self._last_verify_ts,
            'ff_enabled': _is_ff_enabled(),
        }

    def _db_record(self, cur, order_id, side, qty, stop_price, status):
        """Record stop order to DB."""
        try:
            cur.execute("""
                INSERT INTO server_stop_orders
                    (symbol, order_id, client_order_id, side, qty, stop_price, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """, (self.symbol, order_id,
                  f'STOP:{self.symbol}:{side}:{self._version}',
                  side, qty, stop_price, status))
        except Exception as e:
            _log(f'_db_record error: {e}')

    def _db_update_status(self, cur, order_id, status, cancel_reason=None):
        """Update stop order status in DB."""
        try:
            cur.execute("""
                UPDATE server_stop_orders
                SET status = %s, cancel_reason = %s, updated_at = now()
                WHERE order_id = %s AND status = 'ACTIVE';
            """, (status, cancel_reason, order_id))
        except Exception as e:
            _log(f'_db_update_status error: {e}')


def _is_ff_enabled():
    try:
        import feature_flags
        return feature_flags.is_enabled('ff_server_stop_orders')
    except Exception:
        return False


# Module-level singleton
_manager = None


def get_manager(symbol=SYMBOL):
    """Get or create singleton ServerStopManager."""
    global _manager
    if _manager is None:
        _manager = ServerStopManager(symbol)
    return _manager
