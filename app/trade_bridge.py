# Source Generated with Decompyle++
# File: trade_bridge.cpython-312.pyc (Python 3.12)

'''
trade_bridge.py — Bridge between Telegram NL trade commands and live_order_executor.

Inserts into signals_action_v3 (OPEN) so the existing live_order_executor daemon
picks them up and places Bybit orders. Also manages scheduled_orders table.

Supports:
  - side=None → auto direction via direction_scorer
  - capital_limit → per-order USDT cap
  - trade_process_log tracking
'''
import os
import sys
import json
import time
import datetime
sys.path.insert(0, '/root/trading-bot/app')
from trading_config import SYMBOL
LOG_PREFIX = '[trade_bridge]'
ACTION_TBL = 'signals_action_v3'


def _get_usdt_cap(cur=None):
    """Dynamic per-order USDT cap from safety_manager. Fallback 300."""
    try:
        import safety_manager
        eq = safety_manager.get_equity_limits(cur)
        return eq['slice_usdt']
    except Exception:
        return 300
SCHEDULED_EXPIRY_HOURS = 24

def _log(msg):
    print(f'''{LOG_PREFIX} {msg}''', flush = True)


def _db_conn():
    from db_config import get_conn
    return get_conn()


def _ensure_scheduled_orders_table(cur):
    '''Create scheduled_orders table if not exists.'''
    cur.execute("\n        CREATE TABLE IF NOT EXISTS public.scheduled_orders (\n            id           BIGSERIAL PRIMARY KEY,\n            created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),\n            execute_at   TIMESTAMPTZ NOT NULL,\n            symbol       TEXT NOT NULL DEFAULT 'BTC/USDT:USDT',\n            side         TEXT NOT NULL CHECK (side IN ('LONG', 'SHORT')),\n            size_percent INTEGER NOT NULL CHECK (size_percent BETWEEN 1 AND 30),\n            status       TEXT NOT NULL DEFAULT 'PENDING'\n                         CHECK (status IN ('PENDING', 'EXECUTED', 'CANCELLED', 'FAILED', 'EXPIRED')),\n            raw_text     TEXT,\n            result_msg   TEXT,\n            executed_at  TIMESTAMPTZ,\n            signal_id    BIGINT\n        );\n    ")


def _ensure_migrations(cur):
    '''Ensure trade_process_log + stage column exist.'''
    import db_migrations
    db_migrations.ensure_trade_process_log(cur)
    db_migrations.ensure_stage_column(cur)


def _check_safety(cur=None):
    '''Run safety checks. Returns (ok, reason, pos_ctx).
    pos_ctx contains position info if a position exists (non-blocking).'''
    pos_ctx = {}
    live = os.getenv('LIVE_TRADING', '')
    if live != 'YES_I_UNDERSTAND':
        return (False, 'LIVE_TRADING 미활성화 (현재: dry-run 모드)', pos_ctx)
    cur.execute('SELECT enabled FROM trade_switch ORDER BY id DESC LIMIT 1;')
    row = cur.fetchone()
    if not row or not row[0]:
        return (False, 'trade_switch OFF 상태. 주문 실행 불가.', pos_ctx)
    import ccxt
    ex = ccxt.bybit({
        'apiKey': os.getenv('BYBIT_API_KEY'),
        'secret': os.getenv('BYBIT_SECRET'),
        'enableRateLimit': True,
        'options': {
            'defaultType': 'swap'}})
    positions = ex.fetch_positions([SYMBOL])
    for p in positions:
        if p.get('symbol') != SYMBOL:
            continue
        contracts = float(p.get('contracts') or 0)
        s = p.get('side')
        if contracts == 0:
            continue
        if s not in ('long', 'short'):
            continue
        pos_ctx = {
            'position_exists': True,
            'current_side': s,
            'current_qty': contracts,
            'entry_price': float(p.get('entryPrice') or 0),
            'upnl': float(p.get('unrealizedPnl') or 0)}
        break
    return (True, '', pos_ctx)


def _fetch_available_balance():
    '''Fetch free USDT balance from Bybit.'''
    import ccxt
    ex = ccxt.bybit({
        'apiKey': os.getenv('BYBIT_API_KEY'),
        'secret': os.getenv('BYBIT_SECRET'),
        'enableRateLimit': True,
        'options': {
            'defaultType': 'swap' } })
    balance = ex.fetch_balance()
    return float(balance.get('USDT', {}).get('free', 0))


def _fetch_btc_price():
    '''Fetch current BTC price.'''
    import ccxt
    ex = ccxt.bybit({
        'enableRateLimit': True })
    t = ex.fetch_ticker('BTC/USDT')
    return float(t.get('last', 0))


def _log_trade_process(cur, **kwargs):
    '''Insert a row into trade_process_log.'''
    fields = [
        'signal_id',
        'decision_context',
        'long_score',
        'short_score',
        'chosen_side',
        'size_percent',
        'capital_limit',
        'risk_check_result',
        'order_sent_time',
        'order_fill_time',
        'fill_price',
        'pnl_after_trade',
        'rejection_reason',
        'source']
    cols = []
    vals = []
    placeholders = []
    for f in fields:
        if not f in kwargs:
            continue
        cols.append(f)
        val = kwargs[f]
        if f == 'decision_context' and isinstance(val, dict):
            vals.append(json.dumps(val, ensure_ascii = False, default = str))
            placeholders.append('%s::jsonb')
            continue
        vals.append(val)
        placeholders.append('%s')
    if not cols:
        return None
    sql = f'''INSERT INTO trade_process_log ({', '.join(cols)}) VALUES ({', '.join(placeholders)});'''
    cur.execute(sql, vals)


def _insert_signal(cur, direction, usdt_amount=None, raw_text=None, source='telegram_manual', start_stage=None, entry_pct=None):
    '''Insert OPEN signal into signals_action_v3 with stage. Returns signal id.'''
    now = datetime.datetime.now(datetime.timezone.utc)
    ts_text = now.strftime('%Y-%m-%d %H:%M:%S+00')
    now_unix = int(time.time())
    meta = {
        'direction': direction,
        'qty': min(usdt_amount, _get_usdt_cap(cur)) if usdt_amount else 0,
        'dry_run': False,
        'source': source,
        'reason': 'manual_trade_command' if source == 'telegram_manual' else source}
    if start_stage is not None:
        meta['start_stage'] = start_stage
    if entry_pct is not None:
        meta['entry_pct'] = entry_pct
    cur.execute(f'''
        INSERT INTO {ACTION_TBL}
            (action, direction, symbol, usdt_amount, meta, raw_text, source, ts, ts_text)
        VALUES ('OPEN', %s, %s, %s, %s::jsonb, %s, %s, %s, %s)
        RETURNING id;
    ''', (direction, SYMBOL, usdt_amount, json.dumps(meta, ensure_ascii=False, default=str),
          raw_text or '', source, now_unix, ts_text))
    row = cur.fetchone()
    return row[0] if row else None


def _insert_scheduled_order(cur = None, parsed = None, execute_at = None):
    '''Insert a scheduled order. Returns order id.'''
    _ensure_scheduled_orders_table(cur)
    side = parsed.get('side', 'LONG')
    cur.execute('\n        INSERT INTO scheduled_orders (execute_at, symbol, side, size_percent, raw_text)\n        VALUES (%s, %s, %s, %s, %s)\n        RETURNING id;\n    ', (execute_at, SYMBOL, side, parsed['size_percent'], parsed.get('raw_text', '')))
    row = cur.fetchone()
    if row:
        return row[0]


def cancel_scheduled_order(order_id = None):
    '''Cancel a scheduled order by ID.'''
    conn = _db_conn()
    conn.autocommit = True
    try:
        cur = conn.cursor()
        cur.execute("UPDATE scheduled_orders SET status='CANCELLED' WHERE id=%s AND status='PENDING'", (order_id,))
        return cur.rowcount > 0
    finally:
        conn.close()


def check_and_execute_scheduled_orders():
    '''Check PENDING scheduled orders that are due. Execute them.
    Also expire orders older than SCHEDULED_EXPIRY_HOURS.
    Returns list of Telegram messages to send.'''
    messages = []
    conn = _db_conn()
    conn.autocommit = True
    try:
        cur = conn.cursor()
        # Expire old orders
        cur.execute("""
            UPDATE scheduled_orders SET status='EXPIRED'
            WHERE status='PENDING'
            AND created_at < now() - interval '%s hours'
        """, (SCHEDULED_EXPIRY_HOURS,))
        # Fetch due orders
        cur.execute("""
            SELECT id, side, size_percent, raw_text
            FROM scheduled_orders
            WHERE status='PENDING' AND execute_at <= now()
            ORDER BY execute_at
        """)
        for row in cur.fetchall():
            oid, side, size_pct, raw_text = row
            _log(f"Executing scheduled order #{oid}: {side} {size_pct}%")
            try:
                result = execute_trade_command({
                    'side': side,
                    'size_percent': size_pct,
                    'immediate': True,
                    'raw_text': raw_text or '',
                })
                cur.execute("UPDATE scheduled_orders SET status='EXECUTED', executed_at=now(), result_msg=%s WHERE id=%s", (str(result)[:500], oid))
                messages.append(f"예약 주문 #{oid} 실행: {result}")
            except Exception as e:
                cur.execute("UPDATE scheduled_orders SET status='FAILED', result_msg=%s WHERE id=%s", (str(e)[:500], oid))
                messages.append(f"예약 주문 #{oid} 실패: {e}")
    finally:
        conn.close()
    return messages


def execute_trade_command(parsed = None):
    '''Main entry: execute a parsed trade command. Returns Telegram reply string.'''
    if parsed is None:
        return "파싱된 명령이 없습니다."
    side = parsed.get('side')
    size_pct = parsed.get('size_percent', 10)
    immediate = parsed.get('immediate', True)
    scheduled_hour = parsed.get('scheduled_hour')
    capital_limit = parsed.get('capital_limit')
    raw_text = parsed.get('raw_text', '')
    conn = _db_conn()
    conn.autocommit = True
    try:
        cur = conn.cursor()
        _ensure_migrations(cur)
        # Safety check
        ok, reason, pos_ctx = _check_safety(cur)
        if not ok:
            _log_trade_process(cur, rejection_reason=reason, source='telegram_manual')
            return f"주문 거부: {reason}"
        # Auto direction if not specified
        if not side:
            try:
                from direction_scorer import get_direction
                side = get_direction()
            except Exception:
                side = 'LONG'
        # Calculate USDT amount
        price = _fetch_btc_price()
        balance = _fetch_available_balance()
        usdt_amount = balance * (size_pct / 100.0)
        if capital_limit and usdt_amount > capital_limit:
            usdt_amount = capital_limit
        usdt_amount = min(usdt_amount, _get_usdt_cap(cur))
        if usdt_amount < 1:
            return "주문 가능 금액이 부족합니다."
        # Insert signal
        if not immediate and scheduled_hour is not None:
            execute_at = datetime.datetime.now(datetime.timezone.utc).replace(
                hour=int(scheduled_hour), minute=0, second=0, microsecond=0)
            if execute_at <= datetime.datetime.now(datetime.timezone.utc):
                execute_at += datetime.timedelta(days=1)
            oid = _insert_scheduled_order(cur, parsed={'side': side, 'size_percent': size_pct, 'raw_text': raw_text}, execute_at=execute_at)
            return f"예약 주문 등록 완료 (#{oid}, {execute_at.strftime('%H:%M')} UTC)"
        sig_id = _insert_signal(cur, direction=side, usdt_amount=usdt_amount, raw_text=raw_text, source='telegram_manual')
        _log_trade_process(cur, signal_id=sig_id, chosen_side=side, size_percent=size_pct,
                          capital_limit=capital_limit, source='telegram_manual',
                          decision_context=pos_ctx)
        return f"{side} {size_pct}% ({usdt_amount:.1f} USDT) 주문 신호 등록 완료 (#{sig_id})"
    except Exception as e:
        _log(f"execute_trade_command error: {e}")
        return f"주문 실행 오류: {e}"
    finally:
        conn.close()

