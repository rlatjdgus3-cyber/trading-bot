"""
analysis_outcomes_writer.py — Fill PnL/outcome labels for analysis_outcomes.

Polls every 30 min:
  - Find analysis_outcomes with label='pending' and age > 1h
  - Fill btc_move_1h/4h/24h from market_ohlcv
  - Classify outcome: correct/incorrect/neutral based on action vs actual move
  - Also update events.btc_move_* for events missing outcome data
"""
import os
import sys
import time
import traceback

sys.path.insert(0, '/root/trading-bot/app')
from psycopg2 import OperationalError, InterfaceError
from db_config import get_conn

POLL_SEC = 1800
KILL_SWITCH_PATH = '/root/trading-bot/app/KILL_SWITCH'
LOG_PREFIX = '[outcomes_writer]'


def _log(msg):
    print(f"{LOG_PREFIX} {msg}", flush=True)


def _db_conn():
    return get_conn(autocommit=False)


def _get_price_at(cur, ts):
    """Get closest candle close at or before ts. Tries 5m ohlcv first, then 1m candles."""
    cur.execute("""
        SELECT c FROM market_ohlcv
        WHERE symbol = 'BTC/USDT:USDT' AND tf = '5m' AND ts <= %s
        ORDER BY ts DESC LIMIT 1;
    """, (ts,))
    row = cur.fetchone()
    if row:
        return float(row[0])
    # Fallback: 1m candles table
    cur.execute("""
        SELECT c FROM candles
        WHERE symbol = 'BTC/USDT:USDT' AND tf = '1m' AND ts <= %s
        ORDER BY ts DESC LIMIT 1;
    """, (ts,))
    row = cur.fetchone()
    if row:
        return float(row[0])


def _get_price_after(cur, ts, hours):
    """Get closest candle close at ts + hours. Tries 5m ohlcv first, then 1m candles."""
    cur.execute("""
        SELECT c FROM market_ohlcv
        WHERE symbol = 'BTC/USDT:USDT' AND tf = '5m'
          AND ts >= %s + %s * interval '1 hour'
        ORDER BY ts ASC LIMIT 1;
    """, (ts, hours))
    row = cur.fetchone()
    if row:
        return float(row[0])
    # Fallback: 1m candles table
    cur.execute("""
        SELECT c FROM candles
        WHERE symbol = 'BTC/USDT:USDT' AND tf = '1m'
          AND ts >= %s + %s * interval '1 hour'
        ORDER BY ts ASC LIMIT 1;
    """, (ts, hours))
    row = cur.fetchone()
    if row:
        return float(row[0])


def _classify_outcome(action, move_4h):
    """Classify whether the Claude recommendation was correct.

    - HOLD + small move -> correct
    - REDUCE/CLOSE + adverse move -> correct (avoided loss)
    - REDUCE/CLOSE + favorable move -> incorrect (missed gain)
    - REVERSE + move in new direction -> correct
    """
    if action is None or move_4h is None:
        return 'unknown'

    action_upper = action.upper()
    abs_move = abs(move_4h)

    if action_upper == 'HOLD':
        if abs_move < 1.0:
            return 'correct'
        elif abs_move < 2.0:
            return 'neutral'
        else:
            return 'incorrect'

    elif action_upper in ('REDUCE', 'CLOSE', 'CLOSE_ALL'):
        # These are defensive actions; correct if price moved adversely
        if move_4h < -1.0:
            return 'correct'
        elif move_4h > 1.0:
            return 'incorrect'
        else:
            return 'neutral'

    elif action_upper == 'REVERSE':
        # Correct if the market moved in the expected new direction
        if abs_move > 1.0:
            return 'correct'
        else:
            return 'neutral'

    elif action_upper == 'ADVISORY':
        # Advisory-only: no trade executed, record price movement as neutral
        return 'neutral'

    else:
        if abs_move < 0.5:
            return 'neutral'
        return 'unknown'


def _fill_analysis_outcomes(cur):
    """Fill pending analysis outcomes with price data."""
    cur.execute("""
        SELECT ao.id, ao.claude_analysis_id, ao.executed_action, ca.ts
        FROM analysis_outcomes ao
        JOIN claude_analyses ca ON ca.id = ao.claude_analysis_id
        WHERE ao.outcome_label = 'pending'
          AND ca.ts < now() - interval '1 hour';
    """)
    pending = cur.fetchall()
    filled = 0

    for ao_id, ca_id, action, ca_ts in pending:
        btc_price = _get_price_at(cur, ca_ts)
        if btc_price is None:
            continue

        price_1h = _get_price_after(cur, ca_ts, 1)
        price_4h = _get_price_after(cur, ca_ts, 4)
        price_24h = _get_price_after(cur, ca_ts, 24)

        move_1h = round(((price_1h - btc_price) / btc_price) * 100, 4) if price_1h else None
        move_4h = round(((price_4h - btc_price) / btc_price) * 100, 4) if price_4h else None
        move_24h = round(((price_24h - btc_price) / btc_price) * 100, 4) if price_24h else None

        outcome = _classify_outcome(action, move_4h)

        updates = []
        params = []

        if move_1h is not None:
            updates.append('btc_move_1h = %s')
            params.append(move_1h)
        if move_4h is not None:
            updates.append('btc_move_4h = %s')
            params.append(move_4h)
        if move_24h is not None:
            updates.append('btc_move_24h = %s')
            params.append(move_24h)

        if not updates:
            continue

        updates.append('outcome_label = %s')
        params.append(outcome)
        updates.append('filled_at = now()')
        params.append(ao_id)

        cur.execute(
            f"UPDATE analysis_outcomes SET {', '.join(updates)} WHERE id = %s;",
            params
        )
        filled += 1

    return filled


def _fill_event_moves(cur):
    """Fill missing btc_move_* for events."""
    cur.execute("""
        SELECT id, start_ts FROM events
        WHERE btc_move_1h IS NULL
          AND start_ts < now() - interval '1 hour';
    """)
    events = cur.fetchall()
    filled = 0

    for event_id, start_ts in events:
        btc_price = _get_price_at(cur, start_ts)
        if btc_price is None:
            continue

        price_1h = _get_price_after(cur, start_ts, 1)
        price_4h = _get_price_after(cur, start_ts, 4)
        price_24h = _get_price_after(cur, start_ts, 24)

        updates = []
        params = []

        if price_1h:
            move_1h = round(((price_1h - btc_price) / btc_price) * 100, 4)
            updates.append('btc_move_1h = %s')
            params.append(move_1h)
        if price_4h:
            move_4h = round(((price_4h - btc_price) / btc_price) * 100, 4)
            updates.append('btc_move_4h = %s')
            params.append(move_4h)
        if price_24h:
            move_24h = round(((price_24h - btc_price) / btc_price) * 100, 4)
            updates.append('btc_move_24h = %s')
            params.append(move_24h)

        if not updates:
            continue

        params.append(event_id)
        cur.execute(
            f"UPDATE events SET {', '.join(updates)} WHERE id = %s;",
            params
        )
        filled += 1

    return filled


def _cycle(conn):
    """One outcome-filling cycle. Uses persistent connection."""
    if os.path.exists(KILL_SWITCH_PATH):
        _log('KILL_SWITCH detected. Exiting.')
        sys.exit(0)

    # Check DB kill switch (bot_config may not exist)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT value FROM bot_config WHERE key='KILL_SWITCH';
            """)
            row = cur.fetchone()
            if row and row[0] == 'ON':
                _log('DB KILL_SWITCH=ON. Exiting.')
                sys.exit(0)
    except Exception:
        pass  # bot_config table may not exist — skip

    with conn.cursor() as cur:
        ao_filled = _fill_analysis_outcomes(cur)
        ev_filled = _fill_event_moves(cur)
        if ao_filled > 0 or ev_filled > 0:
            _log(f"Filled: outcomes={ao_filled} events={ev_filled}")


def main():
    _log('=== ANALYSIS OUTCOMES WRITER START ===')
    conn = _db_conn()
    conn.autocommit = True
    backoff = 5

    while True:
        try:
            # Auto-reconnect on DB errors
            if conn is None or conn.closed != 0:
                conn = _db_conn()
                conn.autocommit = True
                _log("DB reconnected.")

            _cycle(conn)
            backoff = 5
            time.sleep(POLL_SEC)

        except (OperationalError, InterfaceError) as e:
            _log(f"DB error: {repr(e)} | reconnect in {backoff}s")
            try:
                if conn:
                    conn.close()
            except Exception:
                pass
            conn = None
            time.sleep(backoff)
            backoff = min(backoff * 2, 120)

        except KeyboardInterrupt:
            _log("Interrupted. Shutting down.")
            break

        except Exception as e:
            _log(f"Unexpected error: {repr(e)}")
            _log(traceback.format_exc())
            time.sleep(backoff)
            backoff = min(backoff * 2, 120)


if __name__ == '__main__':
    main()
