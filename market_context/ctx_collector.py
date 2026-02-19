#!/usr/bin/env python3
"""
ctx_collector.py — Market context regime classification daemon.

Type=notify, WatchdogSec=60, POLL_SEC=30.

Reads candles/indicators/vol_profile/liquidity_snapshots from main DB (RO),
classifies regime (RANGE/BREAKOUT/SHOCK), writes to market_context table (RW).
"""
import os
import sys
import time
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import ctx_migrations
from db_config_ctx import get_main_conn_ro, get_main_conn_rw
from ctx_utils import _log, send_telegram, ExponentialBackoff, load_env
import regime_classifier

POLL_SEC = 30
SYMBOL = os.getenv('SYMBOL', 'BTC/USDT:USDT')


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


def _cycle(ro_conn, rw_conn):
    """One classification cycle:
    1. Read market data from main DB (RO)
    2. Classify regime (RANGE/BREAKOUT/SHOCK)
    3. Upsert to market_context table (RW)
    """
    with ro_conn.cursor() as cur:
        result = regime_classifier.classify(cur, SYMBOL)

    if not result:
        _log('classify returned None, skipping')
        return

    # Upsert to market_context
    with rw_conn.cursor() as cur:
        cur.execute("""
            INSERT INTO market_context (
                ts, symbol, timeframe,
                regime, regime_confidence,
                adx_14, plus_di, minus_di, bbw_ratio,
                poc, vah, val, price_vs_va,
                flow_bias, flow_shock,
                shock_type, shock_direction,
                breakout_confirmed, breakout_conditions,
                raw_inputs
            ) VALUES (
                now(), %s, '1m',
                %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s::jsonb,
                %s::jsonb
            )
            ON CONFLICT (symbol, timeframe, ts) DO UPDATE SET
                regime = EXCLUDED.regime,
                regime_confidence = EXCLUDED.regime_confidence,
                adx_14 = EXCLUDED.adx_14,
                plus_di = EXCLUDED.plus_di,
                minus_di = EXCLUDED.minus_di,
                bbw_ratio = EXCLUDED.bbw_ratio,
                poc = EXCLUDED.poc,
                vah = EXCLUDED.vah,
                val = EXCLUDED.val,
                price_vs_va = EXCLUDED.price_vs_va,
                flow_bias = EXCLUDED.flow_bias,
                flow_shock = EXCLUDED.flow_shock,
                shock_type = EXCLUDED.shock_type,
                shock_direction = EXCLUDED.shock_direction,
                breakout_confirmed = EXCLUDED.breakout_confirmed,
                breakout_conditions = EXCLUDED.breakout_conditions,
                raw_inputs = EXCLUDED.raw_inputs;
        """, (
            SYMBOL,
            result['regime'], result['confidence'],
            result.get('adx_14'), result.get('plus_di'), result.get('minus_di'),
            result.get('bbw_ratio'),
            result.get('poc'), result.get('vah'), result.get('val'),
            result.get('price_vs_va'),
            result.get('flow_bias'), result.get('flow_shock'),
            result.get('shock_type'), result.get('shock_direction'),
            result.get('breakout_confirmed'),
            json.dumps(result.get('breakout_conditions', {}), default=str),
            json.dumps(result.get('raw_inputs', {}), default=str),
        ))
    rw_conn.commit()

    _log(f'regime={result["regime"]} conf={result["confidence"]} '
         f'adx={result.get("adx_14")} flow={result.get("flow_bias")} '
         f'shock_type={result.get("shock_type")}')


def main():
    load_env()
    _log('starting ctx_collector daemon')

    # Run migrations at startup
    ctx_migrations.run_all()

    # Notify systemd ready
    _sdnotify('READY=1')
    send_telegram('Market context collector started')

    backoff = ExponentialBackoff()
    ro_conn = None
    rw_conn = None

    while True:
        try:
            # Ensure connections
            if ro_conn is None or ro_conn.closed:
                ro_conn = get_main_conn_ro()
            if rw_conn is None or rw_conn.closed:
                rw_conn = get_main_conn_rw(autocommit=False)

            _cycle(ro_conn, rw_conn)
            backoff.success()

            # Watchdog ping
            _sdnotify('WATCHDOG=1')

        except Exception as e:
            _log(f'cycle error: {e}')
            import traceback
            traceback.print_exc()
            backoff.fail(str(e))

            # Reconnect on DB errors
            for conn in (ro_conn, rw_conn):
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass
            ro_conn = None
            rw_conn = None

        time.sleep(POLL_SEC)


if __name__ == '__main__':
    main()
