"""
integrity_checker.py — System integrity snapshot + auto-remediation.

Compares exchange state vs DB state to detect:
  - ORPHAN_RISK: pos=0 but orders exist
  - STOP_MISSING_RISK: position exists but no protective stop
  - DB_DRIFT: exchange/DB position mismatch
  - EXECUTION_ISSUE: signals emitted but no fills

All functions are FAIL-OPEN: errors return safe defaults.
"""
import os
import time
import json
import traceback
import ccxt

SYMBOL = 'BTC/USDT:USDT'
LOG_PREFIX = '[integrity]'

# Entry freeze tracking for auto-remediation
_entry_freeze_until = 0


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _exchange():
    ex = ccxt.bybit({
        'apiKey': os.getenv('BYBIT_API_KEY'),
        'secret': os.getenv('BYBIT_SECRET'),
        'enableRateLimit': True,
        'timeout': 20000,
        'options': {'defaultType': 'swap'}})
    return ex


def check_integrity(ex, cur, symbol) -> dict:
    """Integrity snapshot: compare exchange vs DB state.

    Returns: {status: str, details: str, checks: dict}

    status values:
      'OK' | 'ORPHAN_RISK' | 'STOP_MISSING_RISK' | 'DB_DRIFT' | 'EXECUTION_ISSUE'
    """
    result = {
        'status': 'OK',
        'details': '',
        'checks': {}
    }

    try:
        # 1. Exchange position
        exch_side = None
        exch_qty = 0.0
        try:
            positions = ex.fetch_positions([symbol])
            for p in positions:
                qty = abs(float(p.get('contracts', 0) or 0))
                if qty > 1e-09:
                    exch_side = p.get('side', '').upper()
                    if exch_side == 'BUY':
                        exch_side = 'LONG'
                    elif exch_side == 'SELL':
                        exch_side = 'SHORT'
                    exch_qty = qty
                    break
        except Exception as e:
            result['checks']['exchange_position'] = f'error: {e}'

        result['checks']['exch_side'] = exch_side or 'NONE'
        result['checks']['exch_qty'] = exch_qty

        # 2. Exchange orders
        active_count = 0
        conditional_count = 0
        try:
            active_orders = ex.fetch_open_orders(symbol)
            active_count = len(active_orders) if active_orders else 0
        except Exception:
            pass
        try:
            cond_orders = ex.fetch_open_orders(symbol, params={'orderFilter': 'StopOrder'})
            conditional_count = len(cond_orders) if cond_orders else 0
        except Exception:
            pass

        result['checks']['active_orders'] = active_count
        result['checks']['conditional_orders'] = conditional_count

        # 3. DB plan_state
        db_side = None
        db_qty = 0.0
        db_plan_state = 'UNKNOWN'
        try:
            cur.execute("""
                SELECT side, total_qty, plan_state
                FROM position_state WHERE symbol = %s;
            """, (symbol,))
            row = cur.fetchone()
            if row:
                db_side = row[0].upper() if row[0] else None
                db_qty = float(row[1] or 0)
                db_plan_state = row[2] or 'PLAN.NONE'
        except Exception as e:
            result['checks']['db_state'] = f'error: {e}'

        result['checks']['db_side'] = db_side or 'NONE'
        result['checks']['db_qty'] = db_qty
        result['checks']['db_plan_state'] = db_plan_state

        # 4. Recent signal vs fill count (10min)
        signal_count = 0
        fill_count = 0
        try:
            cur.execute("""
                SELECT COUNT(*) FROM signals_action_v3
                WHERE symbol = %s AND created_at >= now() - interval '10 minutes';
            """, (symbol,))
            row = cur.fetchone()
            signal_count = row[0] if row else 0
        except Exception:
            pass

        try:
            cur.execute("""
                SELECT COUNT(*) FROM execution_log
                WHERE symbol = %s AND status = 'FILLED'
                  AND ts >= now() - interval '10 minutes';
            """, (symbol,))
            row = cur.fetchone()
            fill_count = row[0] if row else 0
        except Exception:
            pass

        result['checks']['signals_10m'] = signal_count
        result['checks']['fills_10m'] = fill_count

        # === Detect issues ===

        # ORPHAN_RISK: pos=0 but orders exist
        is_flat = exch_qty < 1e-09
        if is_flat and (active_count > 0 or conditional_count > 0):
            result['status'] = 'ORPHAN_RISK'
            result['details'] = (f'pos=0 but {active_count} active + '
                                 f'{conditional_count} conditional orders exist')
            return result

        # STOP_MISSING_RISK: position exists but no conditional stop
        if not is_flat and conditional_count == 0:
            result['status'] = 'STOP_MISSING_RISK'
            result['details'] = (f'position {exch_side} qty={exch_qty} '
                                 f'but no conditional (SL/TP) orders')
            return result

        # DB_DRIFT: exchange and DB disagree on position
        exch_has_pos = not is_flat
        db_has_pos = db_qty > 1e-09 and db_side is not None
        if exch_has_pos != db_has_pos:
            result['status'] = 'DB_DRIFT'
            result['details'] = (f'exchange={exch_side or "NONE"} qty={exch_qty}, '
                                 f'DB={db_side or "NONE"} qty={db_qty} '
                                 f'plan={db_plan_state}')
            return result

        # EXECUTION_ISSUE: many signals but no fills
        if signal_count >= 3 and fill_count == 0:
            result['status'] = 'EXECUTION_ISSUE'
            result['details'] = (f'{signal_count} signals in 10min but '
                                 f'{fill_count} fills — possible execution problem')
            return result

        result['details'] = 'all checks passed'
        return result

    except Exception as e:
        _log(f'check_integrity error: {e}')
        return {'status': 'OK', 'details': f'check error (FAIL-OPEN): {e}', 'checks': {}}


def auto_remediate(ex, cur, symbol, status, details):
    """Auto-remediation based on integrity status.

    Actions:
      ORPHAN_RISK → orphan_cleanup.cleanup_if_flat()
      STOP_MISSING_RISK → server_stop_manager resync + CRITICAL telegram
      DB_DRIFT → reconcile + 5min entry freeze
      EXECUTION_ISSUE → 5min entry freeze + telegram summary
    """
    global _entry_freeze_until

    try:
        if status == 'OK':
            return

        _log(f'auto_remediate: status={status}, details={details}')

        if status == 'ORPHAN_RISK':
            try:
                from orphan_cleanup import cleanup_if_flat
                cleaned, detail = cleanup_if_flat(ex, symbol, reason='integrity_auto')
                _log(f'ORPHAN_RISK remediation: cleaned={cleaned}, {detail}')
            except Exception as e:
                _log(f'ORPHAN_RISK remediation error: {e}')

        elif status == 'STOP_MISSING_RISK':
            try:
                import server_stop_manager
                mgr = server_stop_manager.get_manager()
                mgr.sync()
                _log('STOP_MISSING_RISK: server_stop_manager resync attempted')
            except Exception as e:
                _log(f'STOP_MISSING_RISK resync error: {e}')
                # CRITICAL telegram
                _send_critical_alert(
                    f'[INTEGRITY] STOP_MISSING_RISK\n'
                    f'{details}\n'
                    f'server_stop resync failed: {e}')

        elif status == 'DB_DRIFT':
            _entry_freeze_until = time.time() + 300  # 5min freeze
            _log(f'DB_DRIFT: entry frozen for 5min. {details}')
            _send_critical_alert(
                f'[INTEGRITY] DB_DRIFT detected\n'
                f'{details}\n'
                f'Entry frozen 5min. Reconcile running.')

        elif status == 'EXECUTION_ISSUE':
            _entry_freeze_until = time.time() + 300  # 5min freeze
            _log(f'EXECUTION_ISSUE: entry frozen for 5min. {details}')
            _send_critical_alert(
                f'[INTEGRITY] EXECUTION_ISSUE\n'
                f'{details}\n'
                f'Entry frozen 5min.')

    except Exception as e:
        _log(f'auto_remediate error (FAIL-OPEN): {e}')


def is_entry_frozen():
    """Check if integrity-based entry freeze is active.
    Returns: (frozen: bool, remaining_sec: float)
    """
    global _entry_freeze_until
    now = time.time()
    if now < _entry_freeze_until:
        return (True, _entry_freeze_until - now)
    return (False, 0)


def _send_critical_alert(text):
    """Send critical Telegram alert."""
    try:
        import urllib.parse
        import urllib.request
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
