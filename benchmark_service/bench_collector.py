#!/usr/bin/env python3
"""
bench_collector.py — Long-running data collection daemon.

Type=notify, WatchdogSec=60, POLL_SEC=30.

Collects:
  - Market snapshots (public, no auth)
  - OUR_STRATEGY: reads main DB execution_log/position_state → writes bench DB
  - BYBIT_ACCOUNT: ccxt.bybit with BENCH keys → fetch_my_trades/positions/balance

Dedup via ON CONFLICT (source_id, exec_id) DO NOTHING.
"""
import os
import sys
import time
import json
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import bench_migrations
from db_config_bench import get_bench_conn, get_main_conn_ro
from bench_utils import _log, send_telegram, ExponentialBackoff, load_env
from bench_strategies import STRATEGY_REGISTRY, STRATEGY_LABELS
import bench_backtest_engine

POLL_SEC = 30
SYMBOL = os.getenv('SYMBOL', 'BTC/USDT:USDT')
STALE_THRESHOLD_SEC = 120  # skip if indicators older than 2 minutes


def _sdnotify(state):
    """Send sd_notify state if NOTIFY_SOCKET is set."""
    addr = os.environ.get('NOTIFY_SOCKET')
    if not addr:
        return
    try:
        import socket
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        if addr.startswith('@'):
            addr = '\0' + addr[1:]
        sock.sendto(state.encode(), addr)
        sock.close()
    except Exception:
        pass


def _collect_market_snapshot(bench_conn):
    """Collect public market data (no auth needed)."""
    try:
        import ccxt
        ex = ccxt.bybit({'enableRateLimit': True})
        ticker = ex.fetch_ticker(SYMBOL)
        price = ticker.get('last', 0)
        funding_rate = None
        open_interest = None
        try:
            funding = ex.fetch_funding_rate(SYMBOL)
            funding_rate = funding.get('fundingRate')
        except Exception:
            pass
        try:
            oi = ex.fetch_open_interest(SYMBOL)
            open_interest = oi.get('openInterestValue') or oi.get('openInterestAmount')
        except Exception:
            pass

        with bench_conn.cursor() as cur:
            cur.execute("""
                INSERT INTO bench_market_snapshots (symbol, price, funding_rate, open_interest)
                VALUES (%s, %s, %s, %s);
            """, (SYMBOL, price, funding_rate, open_interest))
        bench_conn.commit()
        _log(f'market snapshot: price={price}')
    except Exception as e:
        bench_conn.rollback()
        _log(f'market snapshot error: {e}')
        raise


def _get_checkpoint(bench_conn, source_id):
    """Get collector state checkpoint for a source."""
    with bench_conn.cursor() as cur:
        cur.execute("""
            SELECT last_exec_ts, last_exec_id, last_position_ts, last_equity_ts
            FROM bench_collector_state WHERE source_id = %s;
        """, (source_id,))
        row = cur.fetchone()
        if row:
            return {
                'last_exec_ts': row[0],
                'last_exec_id': row[1],
                'last_position_ts': row[2],
                'last_equity_ts': row[3],
            }
    return {'last_exec_ts': None, 'last_exec_id': None,
            'last_position_ts': None, 'last_equity_ts': None}


def _update_checkpoint(bench_conn, source_id, **kwargs):
    """Update collector checkpoint."""
    with bench_conn.cursor() as cur:
        cur.execute("""
            INSERT INTO bench_collector_state (source_id, last_exec_ts, last_exec_id,
                                                last_position_ts, last_equity_ts)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (source_id) DO UPDATE SET
                last_exec_ts = COALESCE(EXCLUDED.last_exec_ts, bench_collector_state.last_exec_ts),
                last_exec_id = COALESCE(EXCLUDED.last_exec_id, bench_collector_state.last_exec_id),
                last_position_ts = COALESCE(EXCLUDED.last_position_ts, bench_collector_state.last_position_ts),
                last_equity_ts = COALESCE(EXCLUDED.last_equity_ts, bench_collector_state.last_equity_ts);
        """, (source_id,
              kwargs.get('last_exec_ts'), kwargs.get('last_exec_id'),
              kwargs.get('last_position_ts'), kwargs.get('last_equity_ts')))
    bench_conn.commit()


def _collect_our_strategy(bench_conn, source_id):
    """Collect OUR_STRATEGY data from main DB → bench DB."""
    cp = _get_checkpoint(bench_conn, source_id)
    last_exec_ts = cp['last_exec_ts']

    main_conn = None
    try:
        main_conn = get_main_conn_ro()
        with main_conn.cursor() as mcur:
            # 1. Executions
            if last_exec_ts:
                mcur.execute("""
                    SELECT id, ts, order_id, symbol, direction, filled_qty,
                           avg_fill_price, fee, status
                    FROM execution_log
                    WHERE status IN ('FILLED', 'VERIFIED') AND ts > %s
                    ORDER BY ts LIMIT 500;
                """, (last_exec_ts,))
            else:
                mcur.execute("""
                    SELECT id, ts, order_id, symbol, direction, filled_qty,
                           avg_fill_price, fee, status
                    FROM execution_log
                    WHERE status IN ('FILLED', 'VERIFIED')
                    ORDER BY ts LIMIT 500;
                """)
            exec_rows = mcur.fetchall()

            new_last_ts = last_exec_ts
            new_last_id = cp['last_exec_id']
            if exec_rows:
                with bench_conn.cursor() as bcur:
                    for row in exec_rows:
                        eid, ts, order_id, sym, direction, qty, price, fee, status = row
                        exec_id = f'our_{eid}'
                        qty = float(qty) if qty else 0
                        price = float(price) if price else 0
                        fee = float(fee) if fee else 0
                        bcur.execute("""
                            INSERT INTO bench_executions
                                (ts, source_id, symbol, side, qty, price, fee, order_id, exec_id, meta)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (source_id, exec_id) DO NOTHING;
                        """, (ts, source_id, sym or SYMBOL, direction or 'UNKNOWN',
                              qty, price, fee, order_id, exec_id,
                              json.dumps({'status': status})))
                        new_last_ts = ts
                        new_last_id = exec_id
                bench_conn.commit()
                _log(f'our_strategy: {len(exec_rows)} executions synced')

            # 2. Position snapshot
            mcur.execute("""
                SELECT symbol, side, total_qty, avg_entry_price, stage,
                       capital_used_usdt
                FROM position_state WHERE symbol = %s;
            """, (SYMBOL,))
            pos_row = mcur.fetchone()
            if pos_row:
                with bench_conn.cursor() as bcur:
                    bcur.execute("""
                        INSERT INTO bench_positions
                            (source_id, symbol, size, side, entry_price, meta)
                        VALUES (%s, %s, %s, %s, %s, %s);
                    """, (source_id, pos_row[0] or SYMBOL,
                          float(pos_row[2] or 0), pos_row[1],
                          float(pos_row[3] or 0),
                          json.dumps({'stage': pos_row[4], 'capital_used': float(pos_row[5] or 0)})))
                bench_conn.commit()

            # 3. Equity: read from main DB virtual_capital (latest row)
            try:
                mcur.execute("""
                    SELECT capital_usdt FROM virtual_capital
                    ORDER BY ts DESC LIMIT 1;
                """)
                vc_row = mcur.fetchone()
                if vc_row and vc_row[0]:
                    equity = float(vc_row[0])
                    with bench_conn.cursor() as bcur:
                        bcur.execute("""
                            INSERT INTO bench_equity_timeseries
                                (source_id, equity, wallet_balance, available_balance)
                            VALUES (%s, %s, %s, %s);
                        """, (source_id, equity, equity, equity))
                    bench_conn.commit()
            except Exception as e:
                _log(f'our_strategy equity fetch error: {e}')

            # Update checkpoint
            _update_checkpoint(bench_conn, source_id,
                             last_exec_ts=new_last_ts, last_exec_id=new_last_id,
                             last_position_ts=datetime.now(timezone.utc),
                             last_equity_ts=datetime.now(timezone.utc))
    except Exception as e:
        bench_conn.rollback()
        _log(f'our_strategy collection error: {e}')
        raise
    finally:
        if main_conn:
            try:
                main_conn.close()
            except Exception:
                pass


def _collect_bybit_account(bench_conn, source_id, source):
    """Collect BYBIT_ACCOUNT data via ccxt."""
    import ccxt

    api_key = os.getenv(source.get('api_key_env', ''), '')
    api_secret = os.getenv(source.get('api_secret_env', ''), '')
    if not api_key or not api_secret:
        _log(f'bybit_account: missing API keys for source {source_id}')
        return

    ex = ccxt.bybit({
        'apiKey': api_key,
        'secret': api_secret,
        'enableRateLimit': True,
        'options': {'defaultType': 'linear'},
    })

    cp = _get_checkpoint(bench_conn, source_id)

    # 1. Fetch trades
    since_ms = None
    if cp['last_exec_ts']:
        since_ms = int(cp['last_exec_ts'].timestamp() * 1000)
    else:
        since_ms = int((datetime.now(timezone.utc) - timedelta(days=30)).timestamp() * 1000)

    new_last_ts = cp['last_exec_ts']
    new_last_id = cp['last_exec_id']
    total_fetched = 0
    while True:
        trades = ex.fetch_my_trades(SYMBOL, since=since_ms, limit=100)
        if not trades:
            break
        with bench_conn.cursor() as bcur:
            for t in trades:
                exec_id = f"bybit_{t['id']}"
                ts = datetime.fromtimestamp(t['timestamp'] / 1000, tz=timezone.utc)
                bcur.execute("""
                    INSERT INTO bench_executions
                        (ts, source_id, symbol, side, qty, price, fee, order_id, exec_id, meta)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source_id, exec_id) DO NOTHING;
                """, (ts, source_id, SYMBOL, t['side'].upper(),
                      t['amount'], t['price'], t.get('fee', {}).get('cost', 0),
                      t.get('order'), exec_id,
                      json.dumps({'raw_id': t['id']})))
                new_last_ts = ts
                new_last_id = exec_id
        bench_conn.commit()
        total_fetched += len(trades)
        if len(trades) < 100:
            break
        since_ms = trades[-1]['timestamp'] + 1

    if total_fetched > 0:
        _log(f'bybit_account: {total_fetched} trades synced')

    # 2. Position snapshot
    try:
        positions = ex.fetch_positions([SYMBOL])
        for pos in positions:
            if float(pos.get('contracts', 0)) > 0:
                with bench_conn.cursor() as bcur:
                    bcur.execute("""
                        INSERT INTO bench_positions
                            (source_id, symbol, size, side, entry_price,
                             unrealized_pnl, leverage, liquidation_price)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                    """, (source_id, SYMBOL,
                          pos.get('contracts', 0), pos.get('side', '').upper(),
                          pos.get('entryPrice', 0), pos.get('unrealizedPnl', 0),
                          pos.get('leverage', 0), pos.get('liquidationPrice', 0)))
                bench_conn.commit()
    except Exception as e:
        bench_conn.rollback()
        _log(f'bybit position error: {e}')

    # 3. Balance snapshot
    try:
        balance = ex.fetch_balance()
        usdt = balance.get('USDT', {})
        equity = float(usdt.get('total', 0))
        wallet = float(usdt.get('total', 0))
        avail = float(usdt.get('free', 0))
        with bench_conn.cursor() as bcur:
            bcur.execute("""
                INSERT INTO bench_equity_timeseries
                    (source_id, equity, wallet_balance, available_balance)
                VALUES (%s, %s, %s, %s);
            """, (source_id, equity, wallet, avail))
        bench_conn.commit()
    except Exception as e:
        bench_conn.rollback()
        _log(f'bybit balance error: {e}')

    _update_checkpoint(bench_conn, source_id,
                     last_exec_ts=new_last_ts, last_exec_id=new_last_id,
                     last_position_ts=datetime.now(timezone.utc),
                     last_equity_ts=datetime.now(timezone.utc))


def _fetch_indicators_from_main(main_conn):
    """Fetch indicators, vol_profile, candles from main DB (read-only).

    Returns: (indicators, vol_profile, price, candles, historical_indicators) or None if stale.
    """
    with main_conn.cursor() as mcur:
        # Latest indicators row
        mcur.execute("""
            SELECT symbol, tf, ts, bb_mid, bb_up, bb_dn,
                   ich_tenkan, ich_kijun, ich_span_a, ich_span_b,
                   vol, vol_ma20, vol_spike, rsi_14, atr_14,
                   ma_50, ma_200, ema_9, ema_21, ema_50, vwap
            FROM indicators
            WHERE symbol = %s
            ORDER BY ts DESC LIMIT 1;
        """, (SYMBOL,))
        row = mcur.fetchone()
        if not row:
            _log('no indicators found in main DB')
            return None

        ts = row[2]
        now = datetime.now(timezone.utc)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        age_sec = (now - ts).total_seconds()
        if age_sec > STALE_THRESHOLD_SEC:
            _log(f'indicators stale ({age_sec:.0f}s old), skipping strategy signals')
            return None

        indicators = {
            'symbol': row[0], 'tf': row[1], 'ts': row[2],
            'bb_mid': row[3], 'bb_up': row[4], 'bb_dn': row[5],
            'ich_tenkan': row[6], 'ich_kijun': row[7],
            'ich_span_a': row[8], 'ich_span_b': row[9],
            'vol': row[10], 'vol_ma20': row[11], 'vol_spike': row[12],
            'rsi_14': row[13], 'atr_14': row[14],
            'ma_50': row[15], 'ma_200': row[16],
            'ema_9': row[17], 'ema_21': row[18], 'ema_50': row[19],
            'vwap': row[20],
        }

        # Historical indicators (20 rows for volatility_regime BBW MA)
        mcur.execute("""
            SELECT bb_mid, bb_up, bb_dn, ema_9, ema_21, rsi_14, atr_14
            FROM indicators
            WHERE symbol = %s
            ORDER BY ts DESC LIMIT 20;
        """, (SYMBOL,))
        hist_rows = mcur.fetchall()
        historical_indicators = [
            {'bb_mid': r[0], 'bb_up': r[1], 'bb_dn': r[2],
             'ema_9': r[3], 'ema_21': r[4], 'rsi_14': r[5], 'atr_14': r[6]}
            for r in hist_rows
        ]

        # Vol profile
        mcur.execute("""
            SELECT poc, vah, val FROM vol_profile
            WHERE symbol = %s
            ORDER BY ts DESC LIMIT 1;
        """, (SYMBOL,))
        vp_row = mcur.fetchone()
        vol_profile = {}
        if vp_row:
            vol_profile = {'poc': vp_row[0], 'vah': vp_row[1], 'val': vp_row[2]}

        # Candles: last 20 close prices
        mcur.execute("""
            SELECT c FROM candles
            WHERE symbol = %s
            ORDER BY ts DESC LIMIT 20;
        """, (SYMBOL,))
        candle_rows = mcur.fetchall()
        candles = [float(r[0]) for r in candle_rows if r[0]] if candle_rows else []

        # Current price = latest close or bb_mid fallback
        price = candles[0] if candles else float(indicators.get('bb_mid') or 0)

        return indicators, vol_profile, price, candles, historical_indicators


def _collect_strategy_signals(bench_conn):
    """Compute strategy signals and feed into virtual execution engine."""
    main_conn = None
    try:
        main_conn = get_main_conn_ro()
        result = _fetch_indicators_from_main(main_conn)
        if result is None:
            return

        indicators, vol_profile, price, candles, hist_ind = result

        # Get funding rate from latest market snapshot
        funding_rate = None
        with bench_conn.cursor() as cur:
            cur.execute("""
                SELECT funding_rate FROM bench_market_snapshots
                ORDER BY ts DESC LIMIT 1;
            """)
            fr_row = cur.fetchone()
            if fr_row and fr_row[0]:
                funding_rate = float(fr_row[0])

        # Load strategy source_ids
        with bench_conn.cursor() as cur:
            cur.execute("""
                SELECT id, account_tag FROM benchmark_sources
                WHERE kind = 'STRATEGY_SIGNAL' AND enabled = true;
            """)
            strategy_sources = {row[1]: row[0] for row in cur.fetchall()}

        computed = 0
        for name, compute_fn in STRATEGY_REGISTRY.items():
            source_id = strategy_sources.get(name)
            if not source_id:
                continue

            try:
                sig = compute_fn(indicators, vol_profile, price, candles,
                                 historical_indicators=hist_ind)

                signal = sig.get('signal', 'FLAT')
                confidence = sig.get('confidence', 0)
                rationale = sig.get('rationale', '')
                ind_snap = sig.get('indicators', {})

                # Insert signal record
                with bench_conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO bench_strategy_signals
                            (strategy_name, signal, confidence, rationale, indicators)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (strategy_name, ts) DO UPDATE SET
                            signal = EXCLUDED.signal,
                            confidence = EXCLUDED.confidence,
                            rationale = EXCLUDED.rationale,
                            indicators = EXCLUDED.indicators;
                    """, (name, signal, confidence, rationale,
                          json.dumps(ind_snap, default=str)))
                bench_conn.commit()

                # Initialize equity if needed
                bench_backtest_engine.initialize_strategy_equity(bench_conn, source_id)

                # Process through virtual execution engine
                bench_backtest_engine.process_signal(
                    bench_conn, source_id, name, signal, confidence, price,
                    funding_rate=funding_rate)

                computed += 1
            except Exception as e:
                bench_conn.rollback()
                _log(f'strategy {name} error: {e}')

        if computed > 0:
            _log(f'strategy signals: {computed} strategies computed at price=${price:.2f}')

    except Exception as e:
        _log(f'strategy signal collection error: {e}')
    finally:
        if main_conn:
            try:
                main_conn.close()
            except Exception:
                pass


def main():
    load_env()
    _log('starting bench_collector daemon')

    # Run migrations at startup
    bench_migrations.run_all()

    # Notify systemd ready
    _sdnotify('READY=1')
    send_telegram('Benchmark collector started', prefix='[BENCH]')

    backoff = ExponentialBackoff()

    while True:
        try:
            bench_conn = get_bench_conn()
            try:
                # Load enabled sources
                with bench_conn.cursor() as cur:
                    cur.execute("""
                        SELECT id, kind, label, api_key_env, api_secret_env
                        FROM benchmark_sources WHERE enabled = true;
                    """)
                    sources = cur.fetchall()

                # Collect market snapshot
                _collect_market_snapshot(bench_conn)

                # Collect per source
                for src in sources:
                    src_id, kind, label, api_key_env, api_secret_env = src
                    try:
                        if kind == 'OUR_STRATEGY':
                            _collect_our_strategy(bench_conn, src_id)
                        elif kind == 'BYBIT_ACCOUNT':
                            _collect_bybit_account(bench_conn, src_id, {
                                'api_key_env': api_key_env or '',
                                'api_secret_env': api_secret_env or '',
                            })
                        elif kind == 'STRATEGY_SIGNAL':
                            pass  # handled by _collect_strategy_signals
                        else:
                            _log(f'unknown source kind: {kind}')
                    except Exception as e:
                        _log(f'source {label} ({kind}) error: {e}')

                # Collect strategy signals (after per-source collection)
                try:
                    _collect_strategy_signals(bench_conn)
                except Exception as e:
                    _log(f'strategy signals error: {e}')

                backoff.success()
            finally:
                bench_conn.close()

            # Watchdog ping
            _sdnotify('WATCHDOG=1')

        except Exception as e:
            _log(f'main loop error: {e}')
            backoff.fail(str(e))

        time.sleep(POLL_SEC)


if __name__ == '__main__':
    main()
