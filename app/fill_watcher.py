"""
fill_watcher.py — Bybit order fill verification daemon

Polls execution_log for SENT / PARTIALLY_FILLED orders every 5 seconds.
For each order:
  1. fetch_order() from Bybit -> verify actual fill status
  2. Update execution_log with fill details
  3. For EXIT/EMERGENCY: verify position=0 before declaring "정리 완료"
  4. Send Telegram notifications based on verified facts only

Statuses: SENT -> PARTIALLY_FILLED -> FILLED -> VERIFIED
          or CANCELED / REJECTED / TIMEOUT
"""
import os
import sys
import time
import json
import datetime
import traceback
import urllib.parse
import urllib.request
import ccxt
from db_config import get_conn
import report_formatter

POLL_SEC = 5
ORDER_TIMEOUT_SEC = 60
POSITION_VERIFY_DELAY_SEC = 2
MAX_POLLS_PER_ORDER = 30
SYMBOL = 'BTC/USDT:USDT'
KILL_SWITCH_PATH = '/root/trading-bot/app/KILL_SWITCH'
ACTION_TBL = 'signals_action_v3'
_TG_CONFIG = {}


def log(msg):
    print(f'[FILL_WATCHER] {msg}', flush=True)


def db_conn():
    return get_conn()


def _exchange():
    ex = ccxt.bybit({
        'apiKey': os.getenv('BYBIT_API_KEY'),
        'secret': os.getenv('BYBIT_SECRET'),
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'}})
    ex.load_markets()
    return ex


def _load_tg_config():
    if _TG_CONFIG:
        return _TG_CONFIG
    env_path = '/root/trading-bot/app/telegram_cmd.env'
    try:
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if '=' in line and not line.startswith('#'):
                    k, v = line.split('=', 1)
                    _TG_CONFIG[k.strip()] = v.strip()
    except Exception:
        pass
    return _TG_CONFIG


def _send_telegram(text=None):
    from report_formatter import korean_output_guard
    cfg = _load_tg_config()
    token = cfg.get('TELEGRAM_BOT_TOKEN', '')
    chat_id = cfg.get('TELEGRAM_ALLOWED_CHAT_ID', '')
    if not token or not chat_id:
        return None
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
    return None


def _fetch_position(ex):
    '''Returns (side, qty) for SYMBOL. side=None if no position.'''
    positions = ex.fetch_positions([SYMBOL])
    for p in positions:
        if p.get('symbol') != SYMBOL:
            continue
        contracts = float(p.get('contracts') or 0)
        side = p.get('side')
        if contracts == 0:
            continue
        if side not in ('long', 'short'):
            continue
        return (side, contracts)
    return (None, 0)


def _update_stage(cur, sig_id, stage):
    if not sig_id:
        return None
    cur.execute(f'UPDATE {ACTION_TBL} SET stage = %s WHERE id = %s;', (stage, sig_id))


def _update_trade_process_log(cur, sig_id, **kwargs):
    if not sig_id:
        return None
    sets = []
    vals = []
    for k, v in kwargs.items():
        sets.append(f'{k} = %s')
        vals.append(v)
    if not sets:
        return None
    vals.append(sig_id)
    cur.execute(f"UPDATE trade_process_log SET {', '.join(sets)} WHERE signal_id = %s;", vals)


def _poll_cycle(ex):
    conn = db_conn()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            # Reconcile + auto-heal every 5th cycle
            _reconcile_and_heal(ex, cur)

            cur.execute("""
                SELECT id, order_id, order_type, direction, signal_id, decision_id,
                       close_reason, requested_qty, ticker_price, status,
                       order_sent_at, poll_count, symbol, source_queue, execution_queue_id
                FROM execution_log
                WHERE status IN ('SENT', 'PARTIALLY_FILLED')
                ORDER BY id ASC;
            """)
            rows = cur.fetchall()
            if not rows:
                return
            for row in rows:
                try:
                    _process_order(ex, cur, row)
                except Exception:
                    traceback.print_exc()
    except Exception:
        traceback.print_exc()
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _process_order(ex, cur, row):
    (eid, order_id, order_type, direction, signal_id, decision_id,
     close_reason, requested_qty, ticker_price, status,
     order_sent_at, poll_count, symbol, source_queue, execution_queue_id) = row

    poll_count = (poll_count or 0) + 1
    cur.execute("""
        UPDATE execution_log SET poll_count = %s, last_poll_at = now() WHERE id = %s;
    """, (poll_count, eid))

    if poll_count > MAX_POLLS_PER_ORDER:
        _handle_timeout(cur, eid, order_id, order_type, direction, signal_id)
        return None

    sym = symbol or SYMBOL
    elapsed = time.time() - order_sent_at.timestamp() if order_sent_at else 999

    fetched = None
    try:
        fetched = ex.fetch_closed_order(order_id, sym)
    except Exception:
        try:
            fetched = ex.fetch_order(order_id, sym)
        except Exception:
            traceback.print_exc()
            return None

    if fetched is None:
        return None

    fx_status = fetched.get('status', '')
    filled_qty = float(fetched.get('filled', 0) or 0)
    avg_price = float(fetched.get('average', 0) or 0)
    fee_info = fetched.get('fee', {}) or {}
    fee_cost = float(fee_info.get('cost', 0) or 0)
    fee_currency = fee_info.get('currency', '')

    # Update raw response
    cur.execute("""
        UPDATE execution_log SET raw_fetch_response = %s::jsonb WHERE id = %s;
    """, (json.dumps(fetched, default=str), eid))

    if fx_status == 'canceled':
        _handle_canceled(cur, eid, order_id, order_type, direction, signal_id)
        if execution_queue_id:
            _update_eq_status(cur, execution_queue_id, 'CANCELED')
        return None

    if fx_status in ('closed', 'filled') or (filled_qty > 0 and fx_status != 'open'):
        # Order filled
        cur.execute("""
            UPDATE execution_log SET
                status = 'FILLED', filled_qty = %s, avg_fill_price = %s,
                fee = %s, fee_currency = %s,
                first_fill_at = COALESCE(first_fill_at, now()),
                last_fill_at = now()
            WHERE id = %s;
        """, (filled_qty, avg_price, fee_cost, fee_currency, eid))

        if execution_queue_id:
            _update_eq_status(cur, execution_queue_id, 'FILLED')

        # Route to appropriate handler
        if order_type in ('ENTRY', 'OPEN'):
            _handle_entry_filled(ex, cur, eid, order_id, direction, signal_id,
                                 filled_qty, avg_price, fee_cost, fee_currency)
        elif order_type in ('EXIT', 'CLOSE', 'EMERGENCY_CLOSE', 'STOP_LOSS', 'SCHEDULED_CLOSE'):
            _handle_exit_filled(ex, cur, eid, order_id, order_type, direction,
                                signal_id, decision_id, close_reason,
                                filled_qty, avg_price, fee_cost, fee_currency)
        elif order_type == 'ADD':
            _handle_add_filled(ex, cur, eid, order_id, direction,
                               filled_qty, avg_price, fee_cost, fee_currency,
                               execution_queue_id)
        elif order_type == 'REDUCE':
            _handle_reduce_filled(ex, cur, eid, order_id, direction,
                                  filled_qty, avg_price, fee_cost, fee_currency,
                                  close_reason, execution_queue_id)
        elif order_type == 'REVERSE_CLOSE':
            _handle_reverse_close_filled(ex, cur, eid, order_id, direction,
                                         filled_qty, avg_price, fee_cost, fee_currency,
                                         close_reason, signal_id, decision_id,
                                         execution_queue_id)
        elif order_type == 'REVERSE_OPEN':
            _handle_reverse_open_filled(ex, cur, eid, order_id, direction,
                                        filled_qty, avg_price, fee_cost, fee_currency,
                                        execution_queue_id)

    elif fx_status == 'open' and filled_qty > 0:
        cur.execute("""
            UPDATE execution_log SET
                status = 'PARTIALLY_FILLED', filled_qty = %s, avg_fill_price = %s,
                first_fill_at = COALESCE(first_fill_at, now())
            WHERE id = %s;
        """, (filled_qty, avg_price, eid))

    elif elapsed > ORDER_TIMEOUT_SEC:
        _handle_timeout(cur, eid, order_id, order_type, direction, signal_id)
        if execution_queue_id:
            _update_eq_status(cur, execution_queue_id, 'TIMEOUT')


def _update_position_order_state(cur, order_state, filled_qty=None, filled_usdt=None):
    """Update position_state.order_state + filled tracking columns."""
    try:
        sets = ['order_state = %s', 'state_changed_at = now()']
        vals = [order_state]
        if filled_qty is not None:
            sets.append('filled_qty = %s')
            vals.append(filled_qty)
        if filled_usdt is not None:
            sets.append('filled_usdt = %s')
            vals.append(filled_usdt)
        vals.append(SYMBOL)
        cur.execute(f"UPDATE position_state SET {', '.join(sets)} WHERE symbol = %s;", vals)
    except Exception as e:
        log(f'_update_position_order_state error: {e}')


def _handle_entry_filled(ex, cur, eid, order_id, direction, signal_id,
                          filled_qty, avg_price, fee_cost, fee_currency):
    '''Entry order filled -> update stage + verify position + set budget.'''
    _update_stage(cur, signal_id, 'ORDER_FILLED')
    _update_trade_process_log(cur, signal_id,
                               order_fill_time=datetime.datetime.now(datetime.timezone.utc),
                               fill_price=avg_price)
    time.sleep(POSITION_VERIFY_DELAY_SEC)
    (pos_side, pos_qty) = _fetch_position(ex)
    cur.execute("""
        UPDATE execution_log SET
            status = 'VERIFIED',
            position_after_side = %s, position_after_qty = %s,
            position_verified = true, verified_at = now()
        WHERE id = %s;
    """, (pos_side, pos_qty, eid))

    start_stage = 1
    entry_pct = 10
    if signal_id:
        cur.execute(f"""
                SELECT meta FROM {ACTION_TBL} WHERE id = %s;
            """, (signal_id,))
        meta_row = cur.fetchone()
        if meta_row and meta_row[0]:
            meta = meta_row[0] if isinstance(meta_row[0], dict) else json.loads(meta_row[0])
            start_stage = int(meta.get('start_stage', 0)) or 1
            entry_pct = float(meta.get('entry_pct', 0)) or start_stage * 10

    capital_used = avg_price * filled_qty
    consumed_mask = sum(1 << (s - 1) for s in range(1, start_stage + 1))
    next_stage = start_stage + 1 if start_stage < 7 else 7
    cur.execute("""
            INSERT INTO position_state
                (symbol, side, total_qty, avg_entry_price, stage, capital_used_usdt,
                 start_stage_used, trade_budget_used_pct, next_stage_available,
                 stage_consumed_mask, stages_detail, accumulated_entry_fee)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
            ON CONFLICT (symbol) DO UPDATE SET
                side = EXCLUDED.side, total_qty = EXCLUDED.total_qty,
                avg_entry_price = EXCLUDED.avg_entry_price,
                stage = EXCLUDED.stage, capital_used_usdt = EXCLUDED.capital_used_usdt,
                start_stage_used = EXCLUDED.start_stage_used,
                trade_budget_used_pct = EXCLUDED.trade_budget_used_pct,
                next_stage_available = EXCLUDED.next_stage_available,
                stage_consumed_mask = EXCLUDED.stage_consumed_mask,
                stages_detail = EXCLUDED.stages_detail,
                accumulated_entry_fee = EXCLUDED.accumulated_entry_fee,
                updated_at = now();
        """, (SYMBOL, pos_side, pos_qty, avg_price, start_stage, capital_used,
              start_stage, entry_pct, next_stage, consumed_mask,
              json.dumps([{
                  'stage': start_stage,
                  'price': avg_price,
                  'qty': filled_qty,
                  'pct': entry_pct,
                  'planned_usdt': capital_used,
                  'filled_usdt': capital_used,
                  'planned_qty': filled_qty,
                  'filled_qty': filled_qty}]),
              abs(fee_cost)))
    _update_trade_process_log(cur, signal_id, trade_budget_used_after=entry_pct)
    _update_position_order_state(cur, 'FILLED',
                                  filled_qty=filled_qty,
                                  filled_usdt=avg_price * filled_qty)
    # PLAN state: ENTERING → OPEN
    try:
        cur.execute("UPDATE position_state SET plan_state = 'PLAN.OPEN' WHERE symbol = %s;", (SYMBOL,))
    except Exception as e:
        log(f'plan_state PLAN.OPEN update error: {e}')

    msg = report_formatter.format_fill_notify('entry',
        direction=direction, avg_price=avg_price, filled_qty=filled_qty,
        fee_cost=fee_cost, fee_currency=fee_currency, signal_id=signal_id,
        start_stage=start_stage, entry_pct=entry_pct, next_stage=next_stage,
        pos_side=pos_side, pos_qty=pos_qty)
    _send_telegram(msg)
    log(f'ENTRY VERIFIED: {direction} qty={filled_qty} price={avg_price} signal={signal_id} start_stage={start_stage}')


def _handle_exit_filled(ex, cur, eid, order_id, order_type, direction,
                         signal_id, decision_id, close_reason,
                         filled_qty, avg_price, fee_cost, fee_currency):
    '''Exit/emergency order filled -> verify position=0 + PnL.'''
    time.sleep(POSITION_VERIFY_DELAY_SEC)
    (pos_side, pos_qty) = _fetch_position(ex)
    position_verified = pos_qty < 1e-09
    realized_pnl = None
    entry_price = None

    if signal_id:
        cur.execute("""
                SELECT fill_price FROM trade_process_log
                WHERE signal_id = %s AND fill_price IS NOT NULL
                ORDER BY id DESC LIMIT 1;
            """, (signal_id,))
        row = cur.fetchone()
        if row and row[0]:
            entry_price = float(row[0])

    if entry_price is None:
        cur.execute('SELECT avg_entry_price, accumulated_entry_fee FROM position_state WHERE symbol = %s;', (SYMBOL,))
        row = cur.fetchone()
        if row and row[0]:
            entry_price = float(row[0])
        acc_entry_fee = float(row[1]) if row and row[1] else 0.0
    else:
        cur.execute('SELECT accumulated_entry_fee FROM position_state WHERE symbol = %s;', (SYMBOL,))
        row = cur.fetchone()
        acc_entry_fee = float(row[0]) if row and row[0] else 0.0

    if entry_price and entry_price > 0:
        dir_sign = 1 if direction in ('LONG', 'long') else -1
        gross_pnl = (avg_price - entry_price) * filled_qty * dir_sign
        realized_pnl = gross_pnl - abs(fee_cost) - acc_entry_fee

    cur.execute("""
        UPDATE execution_log SET
            status = 'VERIFIED',
            position_after_side = %s, position_after_qty = %s,
            position_verified = %s, verified_at = now(),
            realized_pnl = %s
        WHERE id = %s;
    """, (pos_side, pos_qty, position_verified, realized_pnl, eid))

    if position_verified:
        _sync_position_state(cur, None, 0)
        _update_position_order_state(cur, 'NONE')
        # PLAN state: EXITING → NONE
        try:
            cur.execute("UPDATE position_state SET plan_state = 'PLAN.NONE' WHERE symbol = %s;", (SYMBOL,))
        except Exception as e:
            log(f'plan_state PLAN.NONE update error: {e}')

    # Backfill pnl_after_trade in trade_process_log for the originating signal
    if realized_pnl is not None and signal_id:
        _update_trade_process_log(cur, signal_id, pnl_after_trade=realized_pnl)

    msg = report_formatter.format_fill_notify('exit',
        order_type=order_type, direction=direction, avg_price=avg_price,
        filled_qty=filled_qty, fee_cost=fee_cost, fee_currency=fee_currency,
        realized_pnl=realized_pnl, pos_side=pos_side, pos_qty=pos_qty,
        close_reason=close_reason)
    _send_telegram(msg)
    log(f'EXIT VERIFIED: {order_type} {direction} qty={filled_qty} price={avg_price} pnl={realized_pnl}')


def _handle_timeout(cur, eid, order_id, order_type, direction, signal_id):
    '''Order not filled within timeout.'''
    cur.execute("""
        UPDATE execution_log SET status = 'TIMEOUT', error_detail = 'order_timeout'
        WHERE id = %s;
    """, (eid,))
    _update_stage(cur, signal_id, 'ORDER_TIMEOUT')
    _update_position_order_state(cur, 'TIMEOUT')
    # Reset plan_state based on whether position still exists
    try:
        cur.execute("SELECT side, total_qty FROM position_state WHERE symbol = %s;", (SYMBOL,))
        ps_row = cur.fetchone()
        if ps_row and ps_row[0] and float(ps_row[1] or 0) > 0:
            cur.execute("UPDATE position_state SET plan_state = 'PLAN.OPEN' WHERE symbol = %s;", (SYMBOL,))
        else:
            cur.execute("UPDATE position_state SET plan_state = 'PLAN.NONE' WHERE symbol = %s;", (SYMBOL,))
    except Exception as e:
        log(f'plan_state timeout reset error: {e}')
    msg = report_formatter.format_fill_notify('timeout',
        order_type=order_type, direction=direction, order_id=order_id,
        timeout_sec=ORDER_TIMEOUT_SEC)
    _send_telegram(msg)
    log(f'TIMEOUT: {order_type} {direction} order_id={order_id}')


def _handle_canceled(cur, eid, order_id, order_type, direction, signal_id):
    '''Order canceled by exchange.'''
    cur.execute("""
        UPDATE execution_log SET status = 'CANCELED', error_detail = 'exchange_canceled'
        WHERE id = %s;
    """, (eid,))
    _update_stage(cur, signal_id, 'ORDER_CANCELED')
    _update_position_order_state(cur, 'CANCELED')
    # Reset plan_state based on whether position still exists
    try:
        cur.execute("SELECT side, total_qty FROM position_state WHERE symbol = %s;", (SYMBOL,))
        ps_row = cur.fetchone()
        if ps_row and ps_row[0] and float(ps_row[1] or 0) > 0:
            cur.execute("UPDATE position_state SET plan_state = 'PLAN.OPEN' WHERE symbol = %s;", (SYMBOL,))
        else:
            cur.execute("UPDATE position_state SET plan_state = 'PLAN.NONE' WHERE symbol = %s;", (SYMBOL,))
    except Exception as e:
        log(f'plan_state cancel reset error: {e}')
    msg = report_formatter.format_fill_notify('canceled',
        order_type=order_type, direction=direction, order_id=order_id)
    _send_telegram(msg)
    log(f'CANCELED: {order_type} {direction} order_id={order_id}')


_reconcile_cycle_count = 0


def _reconcile_and_heal(ex, cur):
    """Called every 5th poll cycle (~25 seconds). Check exchange vs DB and auto-heal."""
    global _reconcile_cycle_count
    _reconcile_cycle_count += 1
    if _reconcile_cycle_count % 5 != 0:
        return

    try:
        import exchange_reader
        exch_data = exchange_reader.fetch_position()
        strat_data = exchange_reader.fetch_position_strat()
        recon_result = exchange_reader.reconcile(exch_data, strat_data)

        if recon_result['status'] != 'RECONCILE.MISMATCH' or not recon_result['needs_healing']:
            return

        exch_pos = exch_data.get('exchange_position', 'UNKNOWN')
        strat_state = strat_data.get('strategy_state', 'FLAT')
        detail = recon_result['detail']

        # Case A: Exchange=NONE, DB thinks position → reset DB to PLAN.NONE
        if exch_pos == 'NONE' and strat_state not in ('FLAT', 'PLAN.NONE'):
            # Check if this has persisted (wait at least 60s before healing)
            cur.execute("SELECT state_changed_at FROM position_state WHERE symbol = %s;", (SYMBOL,))
            row = cur.fetchone()
            if row and row[0]:
                from datetime import datetime, timezone
                age_sec = (datetime.now(timezone.utc) - row[0].replace(tzinfo=timezone.utc)
                           if row[0].tzinfo is None
                           else datetime.now(timezone.utc) - row[0]).total_seconds()
                if age_sec < 60:
                    return  # too fresh, wait
            cur.execute("""
                UPDATE position_state SET
                    side = NULL, total_qty = 0, avg_entry_price = 0,
                    stage = 0, capital_used_usdt = 0, trade_budget_used_pct = 0,
                    order_state = 'NONE', planned_qty = 0, filled_qty = 0,
                    planned_usdt = 0, sent_usdt = 0, filled_usdt = 0,
                    plan_state = 'PLAN.NONE',
                    state_changed_at = now(), updated_at = now()
                WHERE symbol = %s;
            """, (SYMBOL,))
            log(f'RECONCILE HEAL: DB reset to PLAN.NONE (exchange=NONE, {detail})')
            _send_telegram(f'⚠ RECONCILE 자동복구: DB→PLAN.NONE (거래소=NONE, {detail})')
            # Audit
            try:
                cur.execute("""
                    INSERT INTO live_executor_log (event, symbol, detail)
                    VALUES ('RECONCILE_HEAL', %s, %s::jsonb);
                """, (SYMBOL, json.dumps({'action': 'RESET_TO_FLAT', 'detail': detail})))
            except Exception:
                pass

        # Case B: Exchange has position, DB=FLAT → sync DB from exchange
        elif exch_pos in ('LONG', 'SHORT') and strat_state in ('FLAT', 'PLAN.NONE'):
            exch_qty = exch_data.get('exch_pos_qty', 0)
            exch_entry = exch_data.get('exch_entry_price', 0)
            cur.execute("""
                UPDATE position_state SET
                    side = %s, total_qty = %s, avg_entry_price = %s,
                    stage = 1, order_state = 'FILLED',
                    filled_qty = %s, filled_usdt = %s,
                    plan_state = 'PLAN.OPEN',
                    state_changed_at = now(), updated_at = now()
                WHERE symbol = %s;
            """, (exch_pos.lower(), exch_qty, exch_entry,
                  exch_qty, exch_qty * exch_entry, SYMBOL))
            log(f'RECONCILE HEAL: DB synced to {exch_pos} qty={exch_qty} ({detail})')
            _send_telegram(f'⚠ RECONCILE 자동복구: DB→{exch_pos} qty={exch_qty} (거래소 동기화)')
            try:
                cur.execute("""
                    INSERT INTO live_executor_log (event, symbol, detail)
                    VALUES ('RECONCILE_HEAL', %s, %s::jsonb);
                """, (SYMBOL, json.dumps({
                    'action': 'SYNC_TO_EXCHANGE', 'side': exch_pos,
                    'qty': exch_qty, 'entry': exch_entry, 'detail': detail})))
            except Exception:
                pass

        # Case C: Qty mismatch → update DB qty to match exchange
        elif exch_pos in ('LONG', 'SHORT') and strat_state not in ('FLAT', 'PLAN.NONE'):
            exch_qty = exch_data.get('exch_pos_qty', 0)
            cur.execute("""
                UPDATE position_state SET
                    total_qty = %s, filled_qty = %s,
                    updated_at = now()
                WHERE symbol = %s;
            """, (exch_qty, exch_qty, SYMBOL))
            log(f'RECONCILE HEAL: qty synced to {exch_qty} ({detail})')

    except Exception as e:
        log(f'_reconcile_and_heal error: {e}')


def _update_eq_status(cur, eq_id, status):
    '''Update execution_queue status.'''
    if not eq_id:
        return None
    cur.execute('UPDATE execution_queue SET status = %s WHERE id = %s;', (status, eq_id))


def _sync_position_state(cur, pos_side, pos_qty, avg_price=None, stage_delta=0, capital_delta=0):
    '''Sync position_state table after fill.'''
    symbol = SYMBOL
    cur.execute(
        'SELECT stage, capital_used_usdt, avg_entry_price, stages_detail FROM position_state WHERE symbol = %s;',
        (symbol,))
    row = cur.fetchone()
    if not row:
        if pos_side and pos_qty > 0:
            cur.execute("""
                INSERT INTO position_state (symbol, side, total_qty, avg_entry_price, stage, capital_used_usdt)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol) DO UPDATE SET
                    side = EXCLUDED.side, total_qty = EXCLUDED.total_qty,
                    avg_entry_price = EXCLUDED.avg_entry_price,
                    stage = EXCLUDED.stage, capital_used_usdt = EXCLUDED.capital_used_usdt,
                    updated_at = now();
            """, (symbol, pos_side, pos_qty, avg_price or 0, max(1, stage_delta), max(0, capital_delta)))
        return

    old_stage = int(row[0]) if row[0] else 0
    old_capital = float(row[1]) if row[1] else 0

    new_stage = max(0, old_stage + stage_delta)
    new_capital = max(0, old_capital + capital_delta)

    if pos_side is None or pos_qty == 0:
        # Position closed
        cur.execute("""
            UPDATE position_state SET
                side = NULL, total_qty = 0, stage = 0,
                capital_used_usdt = 0, trade_budget_used_pct = 0,
                stage_consumed_mask = 0, accumulated_entry_fee = 0,
                updated_at = now()
            WHERE symbol = %s;
        """, (symbol,))
    else:
        updates = ['side = %s', 'total_qty = %s', 'updated_at = now()']
        vals = [pos_side, pos_qty]
        if avg_price is not None:
            updates.append('avg_entry_price = %s')
            vals.append(avg_price)
        if stage_delta != 0:
            updates.append('stage = %s')
            vals.append(new_stage)
        if capital_delta != 0:
            updates.append('capital_used_usdt = %s')
            vals.append(new_capital)
        vals.append(symbol)
        cur.execute(f"UPDATE position_state SET {', '.join(updates)} WHERE symbol = %s;", vals)


def _handle_add_filled(ex, cur, eid, order_id, direction, filled_qty, avg_price,
                        fee_cost, fee_currency, execution_queue_id):
    '''ADD order filled -> update position_state with budget tracking + Telegram.'''
    time.sleep(POSITION_VERIFY_DELAY_SEC)
    (pos_side, pos_qty) = _fetch_position(ex)
    cur.execute("""
        UPDATE execution_log SET
            status = 'VERIFIED',
            position_after_side = %s, position_after_qty = %s,
            position_verified = true, verified_at = now()
        WHERE id = %s;
    """, (pos_side, pos_qty, eid))

    new_stage = '?'
    budget_used_pct = 0
    budget_remaining = 70

    cur.execute("""
            SELECT avg_entry_price, total_qty, stage,
                   trade_budget_used_pct, next_stage_available, stage_consumed_mask
            FROM position_state WHERE symbol = %s;
        """, (SYMBOL,))
    row = cur.fetchone()
    old_avg = float(row[0]) if row and row[0] else avg_price
    old_qty = float(row[1]) if row and row[1] else 0
    old_stage = int(row[2]) if row and row[2] else 0
    old_budget_pct = float(row[3]) if row and row[3] else 0
    old_next_stage = int(row[4]) if row and row[4] else old_stage + 1
    old_mask = int(row[5]) if row and row[5] else 0

    total_qty = old_qty + filled_qty
    new_avg = (old_avg * old_qty + avg_price * filled_qty) / total_qty if total_qty > 0 else avg_price
    capital_delta = avg_price * filled_qty
    new_stage = old_stage + 1
    add_slice_pct = 10
    budget_used_pct = min(old_budget_pct + add_slice_pct, 70)
    budget_remaining = 70 - budget_used_pct
    new_mask = old_mask | (1 << (new_stage - 1))
    next_avail = new_stage + 1 if new_stage < 7 else 7

    _sync_position_state(cur, pos_side, pos_qty, avg_price=new_avg,
                          stage_delta=1, capital_delta=capital_delta)
    cur.execute("""
            UPDATE position_state SET
                trade_budget_used_pct = %s,
                next_stage_available = %s,
                stage_consumed_mask = %s,
                accumulated_entry_fee = COALESCE(accumulated_entry_fee, 0) + %s
            WHERE symbol = %s;
        """, (budget_used_pct, next_avail, new_mask, abs(fee_cost), SYMBOL))

    msg = report_formatter.format_fill_notify('add',
        direction=direction, avg_price=avg_price, filled_qty=filled_qty,
        fee_cost=fee_cost, fee_currency=fee_currency, new_stage=new_stage,
        pos_side=pos_side, pos_qty=pos_qty, budget_used_pct=budget_used_pct,
        budget_remaining=budget_remaining)
    _send_telegram(msg)
    # PLAN state: stays OPEN after ADD
    try:
        cur.execute("UPDATE position_state SET plan_state = 'PLAN.OPEN' WHERE symbol = %s;", (SYMBOL,))
    except Exception as e:
        log(f'plan_state ADD update error: {e}')
    log(f'ADD VERIFIED: {direction} qty={filled_qty} price={avg_price} stage={new_stage} budget={budget_used_pct:.0f}%')


def _handle_reduce_filled(ex, cur, eid, order_id, direction, filled_qty, avg_price,
                           fee_cost, fee_currency, close_reason, execution_queue_id):
    '''REDUCE order filled -> calc partial PnL + Telegram.'''
    time.sleep(POSITION_VERIFY_DELAY_SEC)
    (pos_side, pos_qty) = _fetch_position(ex)
    realized_pnl = None
    cur.execute('SELECT avg_entry_price, total_qty, accumulated_entry_fee FROM position_state WHERE symbol = %s;', (SYMBOL,))
    row = cur.fetchone()
    entry_price = float(row[0]) if row and row[0] else None
    total_qty_before = float(row[1]) if row and row[1] else 0.0
    acc_entry_fee = float(row[2]) if row and row[2] else 0.0

    proportional_entry_fee = 0.0
    if entry_price and total_qty_before > 0:
        dir_sign = 1 if direction in ('LONG', 'long') else -1
        gross_pnl = (avg_price - entry_price) * filled_qty * dir_sign
        proportional_entry_fee = acc_entry_fee * (filled_qty / total_qty_before)
        realized_pnl = gross_pnl - abs(fee_cost) - proportional_entry_fee

    cur.execute("""
        UPDATE execution_log SET
            status = 'VERIFIED',
            position_after_side = %s, position_after_qty = %s,
            position_verified = true, verified_at = now(),
            realized_pnl = %s
        WHERE id = %s;
    """, (pos_side, pos_qty, realized_pnl, eid))

    capital_delta = -(avg_price * filled_qty)
    _sync_position_state(cur, pos_side, pos_qty, capital_delta=capital_delta)

    # Deduct proportional entry fee from accumulated total
    remaining_fee = max(0, acc_entry_fee - proportional_entry_fee)
    cur.execute('UPDATE position_state SET accumulated_entry_fee = %s WHERE symbol = %s;',
                (remaining_fee, SYMBOL))

    msg = report_formatter.format_fill_notify('reduce',
        direction=direction, avg_price=avg_price, filled_qty=filled_qty,
        fee_cost=fee_cost, fee_currency=fee_currency, realized_pnl=realized_pnl,
        pos_side=pos_side, pos_qty=pos_qty, close_reason=close_reason)
    _send_telegram(msg)
    log(f'REDUCE VERIFIED: {direction} qty={filled_qty} price={avg_price} pnl={realized_pnl}')


def _handle_reverse_close_filled(ex, cur, eid, order_id, direction, filled_qty, avg_price,
                                   fee_cost, fee_currency, close_reason, signal_id,
                                   decision_id, execution_queue_id):
    '''REVERSE_CLOSE filled -> verify position=0 + PnL.'''
    time.sleep(POSITION_VERIFY_DELAY_SEC)
    (pos_side, pos_qty) = _fetch_position(ex)
    position_verified = pos_qty < 1e-09
    realized_pnl = None

    cur.execute('SELECT avg_entry_price, accumulated_entry_fee FROM position_state WHERE symbol = %s;', (SYMBOL,))
    row = cur.fetchone()
    entry_price = float(row[0]) if row and row[0] else None
    acc_entry_fee = float(row[1]) if row and row[1] else 0.0

    if not entry_price and signal_id:
        cur.execute("""
                SELECT fill_price FROM trade_process_log
                WHERE signal_id = %s AND fill_price IS NOT NULL
                ORDER BY id DESC LIMIT 1;
            """, (signal_id,))
        row = cur.fetchone()
        if row and row[0]:
            entry_price = float(row[0])

    if entry_price and entry_price > 0:
        dir_sign = 1 if direction in ('LONG', 'long') else -1
        gross_pnl = (avg_price - entry_price) * filled_qty * dir_sign
        realized_pnl = gross_pnl - abs(fee_cost) - acc_entry_fee

    cur.execute("""
        UPDATE execution_log SET
            status = 'VERIFIED',
            position_after_side = %s, position_after_qty = %s,
            position_verified = %s, verified_at = now(),
            realized_pnl = %s
        WHERE id = %s;
    """, (pos_side, pos_qty, position_verified, realized_pnl, eid))

    _sync_position_state(cur, None, 0)
    # PLAN state: NONE after reverse_close
    try:
        cur.execute("UPDATE position_state SET plan_state = 'PLAN.NONE' WHERE symbol = %s;", (SYMBOL,))
    except Exception as e:
        log(f'plan_state REVERSE_CLOSE update error: {e}')

    msg = report_formatter.format_fill_notify('reverse_close',
        direction=direction, avg_price=avg_price, filled_qty=filled_qty,
        realized_pnl=realized_pnl, position_verified=position_verified,
        pos_side=pos_side, pos_qty=pos_qty)
    _send_telegram(msg)
    log(f'REVERSE_CLOSE VERIFIED: {direction} qty={filled_qty} price={avg_price} pnl={realized_pnl}')


def _handle_reverse_open_filled(ex, cur, eid, order_id, direction, filled_qty, avg_price,
                                  fee_cost, fee_currency, execution_queue_id):
    '''REVERSE_OPEN filled -> reset position_state with budget tracking + Telegram.'''
    time.sleep(POSITION_VERIFY_DELAY_SEC)
    (pos_side, pos_qty) = _fetch_position(ex)
    cur.execute("""
        UPDATE execution_log SET
            status = 'VERIFIED',
            position_after_side = %s, position_after_qty = %s,
            position_verified = true, verified_at = now()
        WHERE id = %s;
    """, (pos_side, pos_qty, eid))

    capital_used = avg_price * filled_qty
    start_stage = 1
    entry_pct = 10
    consumed_mask = 1
    next_stage = 2

    cur.execute("""
            INSERT INTO position_state
                (symbol, side, total_qty, avg_entry_price, stage, capital_used_usdt,
                 start_stage_used, trade_budget_used_pct, next_stage_available,
                 stage_consumed_mask, stages_detail, accumulated_entry_fee)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
            ON CONFLICT (symbol) DO UPDATE SET
                side = EXCLUDED.side, total_qty = EXCLUDED.total_qty,
                avg_entry_price = EXCLUDED.avg_entry_price,
                stage = EXCLUDED.stage, capital_used_usdt = EXCLUDED.capital_used_usdt,
                start_stage_used = EXCLUDED.start_stage_used,
                trade_budget_used_pct = EXCLUDED.trade_budget_used_pct,
                next_stage_available = EXCLUDED.next_stage_available,
                stage_consumed_mask = EXCLUDED.stage_consumed_mask,
                stages_detail = EXCLUDED.stages_detail,
                accumulated_entry_fee = EXCLUDED.accumulated_entry_fee,
                updated_at = now();
        """, (SYMBOL, pos_side, pos_qty, avg_price, start_stage, capital_used,
              start_stage, entry_pct, next_stage, consumed_mask,
              json.dumps([{
                  'stage': 1,
                  'price': avg_price,
                  'qty': filled_qty,
                  'pct': entry_pct}]),
              abs(fee_cost)))

    from_side = 'SHORT' if direction in ('LONG', 'long') else 'LONG'
    msg = report_formatter.format_fill_notify('reverse_open',
        direction=direction, avg_price=avg_price, filled_qty=filled_qty,
        from_side=from_side, pos_side=pos_side, pos_qty=pos_qty,
        entry_pct=entry_pct, start_stage=start_stage)
    _send_telegram(msg)
    # PLAN state: OPEN after reverse_open
    try:
        cur.execute("UPDATE position_state SET plan_state = 'PLAN.OPEN' WHERE symbol = %s;", (SYMBOL,))
    except Exception as e:
        log(f'plan_state REVERSE_OPEN update error: {e}')
    log(f'REVERSE_OPEN VERIFIED: {direction} qty={filled_qty} price={avg_price} stage={start_stage}')


def main():
    log('=== FILL WATCHER START ===')
    ex = _exchange()
    log(f'exchange connected, watching {SYMBOL}')

    # Ensure tables
    try:
        conn = db_conn()
        conn.autocommit = True
        with conn.cursor() as cur:
            import db_migrations
            db_migrations.ensure_execution_log(cur)
            db_migrations.ensure_execution_log_pm_columns(cur)
            db_migrations.ensure_position_state(cur)
            db_migrations.ensure_position_state_budget_columns(cur)
        conn.close()
    except Exception:
        traceback.print_exc()

    while True:
        try:
            if os.path.exists(KILL_SWITCH_PATH):
                log('KILL_SWITCH detected. Exiting.')
                sys.exit(0)
            _poll_cycle(ex)
        except Exception:
            traceback.print_exc()
        time.sleep(POLL_SEC)


if __name__ == '__main__':
    main()
