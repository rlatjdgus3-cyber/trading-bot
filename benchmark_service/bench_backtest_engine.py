"""
bench_backtest_engine.py — Virtual execution engine for benchmark strategies.

Simulates position management with realistic fees/slippage.
Tracks PnL in bench_executions, bench_virtual_positions, bench_equity_timeseries.

State machine per strategy:
  FLAT + LONG   → open LONG
  FLAT + SHORT  → open SHORT
  LONG + FLAT   → close LONG
  LONG + SHORT  → close + open SHORT (flip)
  SHORT + FLAT  → close SHORT
  SHORT + LONG  → close + open LONG (flip)
  Same signal   → HOLD
"""
import json
from datetime import datetime, timezone, timedelta
import threading

LOG_PREFIX = '[backtest_engine]'

# Fee/slippage model
TAKER_FEE = 0.00055      # 0.055%
SLIPPAGE_BPS = 2          # 0.02%
FIXED_NOTIONAL = 100.0    # $100/trade
INITIAL_EQUITY = 1000.0   # $1000 starting capital

# Funding: 8-hour intervals (Bybit perpetual standard)
FUNDING_INTERVAL_SEC = 8 * 3600

# Thread-safe counter for unique exec_id generation within same millisecond
_exec_counter = 0
_exec_lock = threading.Lock()


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def initialize_strategy_equity(bench_conn, source_id):
    """Set initial equity for a strategy source if no equity row exists."""
    with bench_conn.cursor() as cur:
        cur.execute("""
            SELECT count(*) FROM bench_equity_timeseries WHERE source_id = %s;
        """, (source_id,))
        if cur.fetchone()[0] == 0:
            cur.execute("""
                INSERT INTO bench_equity_timeseries
                    (source_id, equity, wallet_balance, available_balance)
                VALUES (%s, %s, %s, %s);
            """, (source_id, INITIAL_EQUITY, INITIAL_EQUITY, INITIAL_EQUITY))
            bench_conn.commit()
            _log(f'initialized equity ${INITIAL_EQUITY} for source {source_id}')


def process_signal(bench_conn, source_id, strategy_name, signal, confidence, price,
                   funding_rate=None):
    """Process a strategy signal through the virtual execution engine.

    Returns: {'action': str, 'details': dict}
    """
    pos = _get_virtual_position(bench_conn, source_id)
    current_side = pos.get('side') if pos else None
    price = float(price)

    # State machine
    if not current_side:
        # FLAT
        if signal == 'LONG':
            _open_position(bench_conn, source_id, strategy_name, 'LONG', price)
            return {'action': 'OPEN_LONG', 'details': {'price': price}}
        elif signal == 'SHORT':
            _open_position(bench_conn, source_id, strategy_name, 'SHORT', price)
            return {'action': 'OPEN_SHORT', 'details': {'price': price}}
        else:
            return {'action': 'HOLD', 'details': {'state': 'FLAT'}}

    elif current_side == 'LONG':
        if signal == 'FLAT':
            details = _close_position(bench_conn, source_id, strategy_name, price,
                                      funding_rate)
            return {'action': 'CLOSE_LONG', 'details': details}
        elif signal == 'SHORT':
            close_details = _close_position(bench_conn, source_id, strategy_name, price,
                                            funding_rate)
            _open_position(bench_conn, source_id, strategy_name, 'SHORT', price)
            return {'action': 'FLIP_TO_SHORT', 'details': close_details}
        else:
            # Same signal (LONG) → accumulate funding
            _accumulate_funding(bench_conn, source_id, strategy_name, pos,
                                funding_rate)
            return {'action': 'HOLD', 'details': {'state': 'LONG'}}

    elif current_side == 'SHORT':
        if signal == 'FLAT':
            details = _close_position(bench_conn, source_id, strategy_name, price,
                                      funding_rate)
            return {'action': 'CLOSE_SHORT', 'details': details}
        elif signal == 'LONG':
            close_details = _close_position(bench_conn, source_id, strategy_name, price,
                                            funding_rate)
            _open_position(bench_conn, source_id, strategy_name, 'LONG', price)
            return {'action': 'FLIP_TO_LONG', 'details': close_details}
        else:
            _accumulate_funding(bench_conn, source_id, strategy_name, pos,
                                funding_rate)
            return {'action': 'HOLD', 'details': {'state': 'SHORT'}}

    return {'action': 'HOLD', 'details': {}}


def _get_virtual_position(bench_conn, source_id):
    """Get current virtual position for a strategy source."""
    with bench_conn.cursor() as cur:
        cur.execute("""
            SELECT id, strategy_name, side, size, entry_price, entry_ts,
                   notional, accumulated_funding
            FROM bench_virtual_positions WHERE source_id = %s;
        """, (source_id,))
        row = cur.fetchone()
        if row and row[2]:  # has side
            return {
                'id': row[0], 'strategy_name': row[1], 'side': row[2],
                'size': float(row[3]), 'entry_price': float(row[4]),
                'entry_ts': row[5], 'notional': float(row[6] or 0),
                'accumulated_funding': float(row[7] or 0),
            }
    return None


def _apply_slippage(price, side):
    """Apply slippage: worse fill for taker."""
    slip = price * SLIPPAGE_BPS / 10000
    if side == 'LONG':
        return price + slip  # buy higher
    else:
        return price - slip  # sell lower


def _open_position(bench_conn, source_id, strategy_name, side, price):
    """Open a virtual position with slippage + taker fee."""
    fill_price = _apply_slippage(price, side)
    size = FIXED_NOTIONAL / fill_price
    fee = FIXED_NOTIONAL * TAKER_FEE
    now = datetime.now(timezone.utc)

    # Record execution
    exec_id = _make_exec_id(strategy_name)
    exec_side = 'BUY' if side == 'LONG' else 'SELL'
    _record_execution(bench_conn, source_id, exec_side, size, fill_price, fee,
                      exec_id, {'action': 'OPEN', 'virtual': True})

    # Upsert virtual position
    with bench_conn.cursor() as cur:
        cur.execute("""
            INSERT INTO bench_virtual_positions
                (source_id, strategy_name, side, size, entry_price, entry_ts,
                 notional, accumulated_funding, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 0, now())
            ON CONFLICT (strategy_name) DO UPDATE SET
                side = EXCLUDED.side, size = EXCLUDED.size,
                entry_price = EXCLUDED.entry_price, entry_ts = EXCLUDED.entry_ts,
                notional = EXCLUDED.notional, accumulated_funding = 0,
                updated_at = now();
        """, (source_id, strategy_name, side, size, fill_price, now,
              FIXED_NOTIONAL))
    bench_conn.commit()


def _close_position(bench_conn, source_id, strategy_name, price, funding_rate=None):
    """Close virtual position, compute PnL, update equity."""
    pos = _get_virtual_position(bench_conn, source_id)
    if not pos:
        return {'error': 'no position to close'}

    # Apply slippage (opposite direction for close)
    close_side = 'SHORT' if pos['side'] == 'LONG' else 'LONG'
    fill_price = _apply_slippage(price, close_side)
    fee = pos['notional'] * TAKER_FEE

    # PnL calculation
    if pos['side'] == 'LONG':
        raw_pnl = pos['size'] * (fill_price - pos['entry_price'])
    else:
        raw_pnl = pos['size'] * (pos['entry_price'] - fill_price)

    # Subtract opening + closing fees and accumulated funding
    open_fee = pos['notional'] * TAKER_FEE
    total_fees = open_fee + fee
    funding_cost = pos['accumulated_funding']
    net_pnl = raw_pnl - total_fees - funding_cost

    # Record close execution
    exec_id = _make_exec_id(strategy_name)
    exec_side = 'SELL' if pos['side'] == 'LONG' else 'BUY'
    _record_execution(bench_conn, source_id, exec_side, pos['size'], fill_price, fee,
                      exec_id, {
                          'action': 'CLOSE', 'virtual': True,
                          'pnl': round(net_pnl, 6),
                          'funding_cost': round(funding_cost, 6),
                          'entry_price': pos['entry_price'],
                      })

    # Update equity
    _update_equity(bench_conn, source_id, net_pnl)

    # Clear virtual position (set to flat)
    with bench_conn.cursor() as cur:
        cur.execute("""
            UPDATE bench_virtual_positions
            SET side = NULL, size = 0, entry_price = NULL, entry_ts = NULL,
                notional = NULL, accumulated_funding = 0, updated_at = now()
            WHERE source_id = %s;
        """, (source_id,))
    bench_conn.commit()

    return {
        'side': pos['side'], 'entry': pos['entry_price'], 'exit': fill_price,
        'raw_pnl': round(raw_pnl, 6), 'fees': round(total_fees, 6),
        'funding': round(funding_cost, 6), 'net_pnl': round(net_pnl, 6),
    }


def _accumulate_funding(bench_conn, source_id, strategy_name, pos, funding_rate):
    """Accumulate funding if 8h interval has passed since last update."""
    if funding_rate is None or not pos:
        return
    funding_rate = float(funding_rate)
    if funding_rate == 0:
        return

    entry_ts = pos.get('entry_ts')
    if not entry_ts:
        return

    now = datetime.now(timezone.utc)
    if entry_ts.tzinfo is None:
        entry_ts = entry_ts.replace(tzinfo=timezone.utc)

    # Check if a funding interval has passed since entry/last accumulation
    elapsed = (now - entry_ts).total_seconds()
    funding_periods = int(elapsed / FUNDING_INTERVAL_SEC)
    if funding_periods <= 0:
        return

    # Funding cost = notional * rate * periods (since entry)
    # We only add incremental funding (already accumulated is stored)
    notional = pos.get('notional', FIXED_NOTIONAL)
    total_expected = abs(notional * funding_rate * funding_periods)
    already = pos.get('accumulated_funding', 0)
    incremental = max(0, total_expected - already)

    if incremental > 0:
        with bench_conn.cursor() as cur:
            cur.execute("""
                UPDATE bench_virtual_positions
                SET accumulated_funding = accumulated_funding + %s, updated_at = now()
                WHERE source_id = %s;
            """, (incremental, source_id))
        bench_conn.commit()


def _make_exec_id(strategy_name):
    """Generate unique exec_id (safe for flip: close+open in same ms)."""
    global _exec_counter
    now = datetime.now(timezone.utc)
    with _exec_lock:
        _exec_counter += 1
        seq = _exec_counter
    return f'vm_{strategy_name}_{int(now.timestamp() * 1000)}_{seq}'


def _record_execution(bench_conn, source_id, side, qty, price, fee, exec_id, meta):
    """Record a virtual execution in bench_executions."""
    with bench_conn.cursor() as cur:
        cur.execute("""
            INSERT INTO bench_executions
                (source_id, symbol, side, qty, price, fee, exec_id, meta)
            VALUES (%s, 'BTC/USDT:USDT', %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_id, exec_id) DO NOTHING;
        """, (source_id, side, qty, price, fee, exec_id, json.dumps(meta)))
    bench_conn.commit()


def _update_equity(bench_conn, source_id, pnl_delta):
    """Get latest equity and record new equity after PnL."""
    with bench_conn.cursor() as cur:
        cur.execute("""
            SELECT equity FROM bench_equity_timeseries
            WHERE source_id = %s ORDER BY ts DESC LIMIT 1;
        """, (source_id,))
        row = cur.fetchone()
        prev_equity = float(row[0]) if row else INITIAL_EQUITY
        new_equity = prev_equity + pnl_delta

        cur.execute("""
            INSERT INTO bench_equity_timeseries
                (source_id, equity, wallet_balance, available_balance)
            VALUES (%s, %s, %s, %s);
        """, (source_id, new_equity, new_equity, new_equity))
    bench_conn.commit()
