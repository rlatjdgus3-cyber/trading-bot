"""
server_stop_manager.py — Server-side reduce-only STOP-MARKET order management.

P0-2: Places and maintains exchange-side conditional stop orders as a safety net.
The server stop is set 0.1% wider than the Python SL to prevent double execution.
All orders use reduce-only mode to prevent position reversal.

Key design (110072 fix):
  - clientOrderId uses timestamp+random for uniqueness (never reuses)
  - Pre-check: fetch existing stop orders before placing new ones (idempotent)
  - 110072 handler: re-query → if SL already set, treat as OK
  - Per-symbol mutex: prevents concurrent stop operations
  - Debounce: min 3s between stop operations
  - UNSET alert: if stop remains unset >60s, one-time Telegram warning

FAIL-OPEN: Errors in this module never block trading, only log warnings.
"""

import os
import sys
import time
import threading
import traceback

sys.path.insert(0, '/root/trading-bot/app')

LOG_PREFIX = '[server_stop]'
SYMBOL = os.getenv('SYMBOL', 'BTC/USDT:USDT')
_VERIFY_INTERVAL_SEC = 30
_DEBOUNCE_SEC = 3
_UNSET_ALERT_SEC = 60


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
        token = cfg.get('TELEGRAM_BOT_TOKEN', '')
        chat_id = cfg.get('TELEGRAM_ALLOWED_CHAT_ID', '')
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


def _make_unique_id(prefix='SL'):
    """Generate unique clientOrderId: SL_<uuid4_hex16>."""
    import uuid
    return f'{prefix}_{uuid.uuid4().hex[:16]}'


class ServerStopManager:
    """Manages server-side conditional stop-market orders on Bybit."""

    def __init__(self, symbol=SYMBOL):
        self.symbol = symbol
        self._exchange = _get_exchange()  # FIX: init eagerly (thread-safe)
        self._last_order_id = None
        self._last_stop_price = None
        self._last_verify_ts = 0
        self._last_sync_ts = 0
        self._last_set_result = None       # 'OK' | 'FAILED' | 'DUPLICATE_OK' etc
        self._last_set_ts = 0
        self._unset_since = 0              # 0 = not unset
        self._unset_alert_sent = False
        self._lock = threading.Lock()
        self._last_client_id = None        # Track actual clientOrderId for DB
        self._entry_blocked_until = 0    # [0-2] entry block on repeated SL failure

    def _ex(self):
        return self._exchange

    # ──────────────────────────────────────────────────────────
    # MAIN ENTRY: sync_stop_order
    # ──────────────────────────────────────────────────────────

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

        # Mutex: serialize per-symbol stop operations
        if not self._lock.acquire(blocking=False):
            return (True, 'lock held — skipping')

        try:
            # FIX: Debounce check INSIDE the lock to prevent race
            now = time.time()
            if now - self._last_sync_ts < _DEBOUNCE_SEC:
                return (True, f'debounce ({_DEBOUNCE_SEC}s)')
            self._last_sync_ts = now
            return self._sync_inner(cur, position, sl_price)
        finally:
            self._lock.release()

    def _sync_inner(self, cur, position, sl_price):
        """Core sync logic (called under lock)."""
        try:
            side = position.get('side', '').lower()
            qty = float(position.get('qty', 0))
            if not side or qty <= 0 or not sl_price or sl_price <= 0:
                return (True, 'no position or invalid params')

            # Compute server stop price (0.1% wider than Python SL)
            from strategy_v3 import config_v3
            cfg = config_v3.get_all()
            offset_pct = cfg.get('server_stop_offset_pct', 0.1)

            if side == 'long':
                server_stop_price = round(sl_price * (1 - offset_pct / 100), 2)
            else:
                server_stop_price = round(sl_price * (1 + offset_pct / 100), 2)

            if server_stop_price <= 0:
                return (True, 'computed stop price <= 0')

            # ── Step 1: Pre-check existing stop on exchange ──
            existing = self._fetch_existing_stops()

            if existing:
                for stop in existing:
                    ex_price = float(stop.get('trigger_price', 0))
                    if ex_price > 0 and abs(server_stop_price - ex_price) / server_stop_price < 0.001:
                        oid = stop.get('order_id', '')
                        self._last_order_id = oid
                        self._last_stop_price = ex_price
                        self._last_set_result = 'OK'
                        self._last_set_ts = time.time()
                        self._clear_unset()
                        self._last_verify_ts = time.time()
                        return (True, f'stop already set @ {ex_price} (id={oid})')

                # Existing stop at different price → cancel all and re-place
                for stop in existing:
                    self._cancel_stop(stop.get('order_id', ''))

            elif self._last_order_id:
                self._last_order_id = None
                self._last_stop_price = None

            # ── Step 2: Place new stop ──
            close_side = 'sell' if side == 'long' else 'buy'
            success, order_id, ret_code = self._place_stop_market(
                close_side, qty, server_stop_price, side)

            if success:
                self._last_order_id = order_id
                self._last_stop_price = server_stop_price
                self._last_set_result = 'OK'
                self._last_set_ts = time.time()
                self._last_verify_ts = time.time()
                self._clear_unset()
                self._db_record(cur, order_id, close_side, qty,
                                server_stop_price, 'ACTIVE')
                return (True, f'stop placed @ {server_stop_price} (id={order_id})')

            # ── Step 3: Handle 110072 (duplicate orderLinkId) ──
            if ret_code == 110072:
                return self._handle_duplicate(cur, close_side, qty,
                                              server_stop_price, side)

            # Other failure
            self._last_set_result = f'FAILED({ret_code})'
            self._last_set_ts = time.time()
            self._mark_unset()
            _log(f'STOP placement failed: ret={ret_code} detail={order_id}')
            return (False, f'placement failed: {order_id}')

        except Exception as e:
            _log(f'sync_stop_order error (FAIL-OPEN): {e}')
            traceback.print_exc()
            self._last_set_result = f'ERROR({e})'
            self._last_set_ts = time.time()
            return (False, f'error: {e}')

    # ──────────────────────────────────────────────────────────
    # 110072 HANDLER
    # ──────────────────────────────────────────────────────────

    def _handle_duplicate(self, cur, close_side, qty, desired_price, position_side):
        """Handle Bybit 110072 (OrderLinkedID duplicate).
        Re-query exchange → if SL is already set, treat as OK.
        Otherwise retry once with a fresh unique ID.
        On 2x failure → block new entries for 5 minutes."""
        _log('110072: duplicate orderLinkId — re-checking exchange')

        # Step 1: Check if stop already exists at target price
        existing = self._fetch_existing_stops()
        if existing:
            for stop in existing:
                ex_price = float(stop.get('trigger_price', 0))
                oid = stop.get('order_id', '')
                if ex_price > 0 and abs(desired_price - ex_price) / desired_price < 0.001:
                    self._last_order_id = oid
                    self._last_stop_price = ex_price
                    self._last_set_result = 'DUPLICATE_OK'
                    self._last_set_ts = time.time()
                    self._clear_unset()
                    _log(f'110072 resolved: stop exists @ {ex_price} (id={oid})')
                    return (True, f'stop already exists @ {ex_price} (110072 resolved)')

        # Step 2: Retry once with fresh uuid4-based ID
        _log('110072: no matching stop found — retrying with new ID')
        success, order_id, ret_code = self._place_stop_market(
            close_side, qty, desired_price, position_side)

        if success:
            self._last_order_id = order_id
            self._last_stop_price = desired_price
            self._last_set_result = 'OK(retry)'
            self._last_set_ts = time.time()
            self._clear_unset()
            self._db_record(cur, order_id, close_side, qty,
                            desired_price, 'ACTIVE')
            _log(f'110072 retry OK: stop placed @ {desired_price}')
            return (True, f'stop placed @ {desired_price} (retry after 110072)')

        # Step 3: 2x failure → CRITICAL + block new entries for 5 minutes
        self._last_set_result = f'FAILED(110072_retry:{ret_code})'
        self._last_set_ts = time.time()
        self._mark_unset()
        self._entry_blocked_until = time.time() + 300
        _notify_telegram(
            f'CRITICAL: SL 설정 2회 실패\n'
            f'retCode={ret_code}\n'
            f'Desired SL: {desired_price}\n'
            f'Entry 5분 차단 활성화')
        _log(f'110072 retry FAILED: ret={ret_code} — entry blocked for 300s')
        return (False, f'110072 retry failed: {ret_code}')

    def is_entry_blocked(self):
        """[0-2] Check if entry is blocked due to repeated SL failures."""
        if self._entry_blocked_until > time.time():
            remaining = int(self._entry_blocked_until - time.time())
            return (True, f'SL failure entry block ({remaining}s remaining)')
        return (False, '')

    # ──────────────────────────────────────────────────────────
    # PLACE / CANCEL / VERIFY
    # ──────────────────────────────────────────────────────────

    def _place_stop_market(self, close_side, qty, stop_price, position_side):
        """Place reduce-only conditional stop-market order on Bybit.

        Returns: (success: bool, order_id_or_error: str, ret_code: int)
        """
        try:
            client_id = _make_unique_id('SL')
            self._last_client_id = client_id

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
                 f'trigger={stop_price} id={order_id} clientId={client_id}')
            return (True, order_id, 0)

        except Exception as e:
            err_str = str(e)
            ret_code = self._extract_ret_code(err_str)
            _log(f'_place_stop_market error (ret={ret_code}): {e}')
            return (False, err_str, ret_code)

    def _extract_ret_code(self, error_str):
        """Extract Bybit retCode from error string."""
        try:
            if '"retCode":' in error_str:
                import re
                m = re.search(r'"retCode"\s*:\s*(\d+)', error_str)
                if m:
                    return int(m.group(1))
        except Exception:
            pass
        return -1

    def _cancel_stop(self, order_id):
        """Cancel existing conditional stop order."""
        if not order_id:
            return False
        try:
            self._ex().cancel_order(order_id, self.symbol, params={
                'orderFilter': 'StopOrder',
            })
            _log(f'STOP cancelled: {order_id}')
            return True
        except Exception as e:
            _log(f'_cancel_stop error (non-fatal): {e}')
            return False

    def _fetch_existing_stops(self):
        """Fetch all existing stop orders from exchange.

        Returns: list of {order_id, trigger_price, side, qty}
        """
        try:
            orders = self._ex().fetch_open_orders(self.symbol, params={
                'orderFilter': 'StopOrder',
            })
            result = []
            for o in orders:
                oid = o.get('id', '') or o.get('info', {}).get('orderId', '')
                trigger = (o.get('info', {}).get('triggerPrice') or
                           o.get('triggerPrice') or
                           o.get('stopPrice') or 0)
                result.append({
                    'order_id': oid,
                    'trigger_price': float(trigger) if trigger else 0,
                    'side': o.get('side', ''),
                    'qty': float(o.get('amount', 0)),
                })
            return result
        except Exception as e:
            _log(f'_fetch_existing_stops error: {e}')
            return []

    def cancel_all_stops(self, cur=None):
        """Cancel all server-side stops for this symbol. Called on position close."""
        # FIX: acquire lock to prevent race with sync_stop_order
        with self._lock:
            try:
                existing = self._fetch_existing_stops()
                cancelled = 0
                for stop in existing:
                    oid = stop.get('order_id', '')
                    if oid:
                        self._cancel_stop(oid)
                        if cur:
                            self._db_update_status(cur, oid, 'CANCELLED',
                                                   'position_closed')
                        cancelled += 1
                self._last_order_id = None
                self._last_stop_price = None
                self._last_set_result = None
                self._clear_unset()
                _log(f'cancel_all_stops: {cancelled} cancelled')
                return cancelled
            except Exception as e:
                _log(f'cancel_all_stops error: {e}')
                return 0

    # ──────────────────────────────────────────────────────────
    # UNSET ALERT TRACKING
    # ──────────────────────────────────────────────────────────

    def _mark_unset(self):
        """Mark stop as unset — starts the 60s alert timer."""
        if self._unset_since == 0:
            self._unset_since = time.time()
            self._unset_alert_sent = False

    def _clear_unset(self):
        """Clear unset state — stop is confirmed on exchange."""
        self._unset_since = 0
        self._unset_alert_sent = False

    def check_unset_alert(self):
        """Call periodically to send one-time alert if stop remains unset >60s."""
        if (self._unset_since > 0 and
                not self._unset_alert_sent and
                time.time() - self._unset_since >= _UNSET_ALERT_SEC):
            self._unset_alert_sent = True
            _notify_telegram(
                f'SERVER STOP UNSET > {_UNSET_ALERT_SEC}s\n'
                f'Last result: {self._last_set_result}\n'
                f'Symbol: {self.symbol}')
            _log(f'UNSET ALERT: stop unset for >{_UNSET_ALERT_SEC}s')

    # ──────────────────────────────────────────────────────────
    # STATUS / DB
    # ──────────────────────────────────────────────────────────

    def get_status(self):
        """Return status dict for /debug stop_orders and /bundle."""
        return {
            'symbol': self.symbol,
            'last_order_id': self._last_order_id,
            'last_stop_price': self._last_stop_price,
            'last_set_result': self._last_set_result,
            'last_set_ts': self._last_set_ts,
            'last_verify_ts': self._last_verify_ts,
            'unset_since': self._unset_since,
            'ff_enabled': _is_ff_enabled(),
            'entry_blocked_until': self._entry_blocked_until,
        }

    def _db_record(self, cur, order_id, side, qty, stop_price, status):
        """Record stop order to DB."""
        try:
            # FIX: use actual clientOrderId instead of generated REC_ prefix
            actual_client_id = self._last_client_id or _make_unique_id('REC')
            cur.execute("""
                INSERT INTO server_stop_orders
                    (symbol, order_id, client_order_id, side, qty, stop_price, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """, (self.symbol, order_id, actual_client_id,
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

    def sync_event_stop(self, cur, position, new_sl_pct=None, urgency='MEDIUM'):
        """Event Decision Mode stop sync — wraps sync_stop_order with urgency bypass.

        Args:
            cur: DB cursor
            position: dict with {side, qty, entry_price, mark_price}
            new_sl_pct: optional SL distance in % (e.g. 1.5 = 1.5% from mark)
            urgency: MEDIUM/HIGH/CRITICAL — HIGH+ bypasses debounce

        Returns: (synced: bool, detail: str)
        """
        import feature_flags
        if not feature_flags.is_enabled('ff_server_stop_orders'):
            return (True, 'ff_server_stop_orders OFF')

        side = (position.get('side') or '').lower()
        mark = float(position.get('mark_price', 0) or position.get('price', 0) or 0)
        qty = float(position.get('qty', 0))

        if not side or qty <= 0 or mark <= 0:
            return (True, 'no position for event stop')

        # Compute SL price from new_sl_pct or fallback to entry-based
        if new_sl_pct and new_sl_pct > 0:
            if side == 'long':
                sl_price = round(mark * (1 - new_sl_pct / 100), 2)
            else:
                sl_price = round(mark * (1 + new_sl_pct / 100), 2)
        else:
            # Fallback: use 2% from mark
            if side == 'long':
                sl_price = round(mark * 0.98, 2)
            else:
                sl_price = round(mark * 1.02, 2)

        # HIGH/CRITICAL urgency: bypass debounce
        if urgency in ('HIGH', 'CRITICAL'):
            if not self._lock.acquire(blocking=False):
                return (True, 'lock held — skipping event stop')
            try:
                self._last_sync_ts = 0  # reset debounce
                return self._sync_inner(cur, position, sl_price)
            finally:
                self._lock.release()
        else:
            return self.sync_stop_order(cur, position, sl_price)


def _is_ff_enabled():
    try:
        import feature_flags
        return feature_flags.is_enabled('ff_server_stop_orders')
    except Exception:
        return False


# Module-level singleton with thread-safe init
_manager = None
_manager_lock = threading.Lock()


def get_manager(symbol=SYMBOL):
    """Get or create singleton ServerStopManager (thread-safe)."""
    global _manager
    if _manager is None:
        with _manager_lock:
            if _manager is None:
                _manager = ServerStopManager(symbol)
    return _manager
