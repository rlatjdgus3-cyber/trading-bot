"""
ctx_migrations.py — market_context table + view DDL.

Called at ctx_collector startup and from app/db_migrations.py.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ctx_utils import _log


def ensure_market_context(cur):
    """Create market_context table and latest view."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS market_context (
            id              BIGSERIAL PRIMARY KEY,
            ts              TIMESTAMPTZ NOT NULL DEFAULT now(),
            symbol          TEXT NOT NULL DEFAULT 'BTC/USDT:USDT',
            timeframe       TEXT NOT NULL DEFAULT '1m',
            -- mode classification
            regime          TEXT NOT NULL,
            regime_confidence NUMERIC DEFAULT 0,
            -- ADX
            adx_14          NUMERIC,
            plus_di         NUMERIC,
            minus_di        NUMERIC,
            -- BB width ratio
            bbw_ratio       NUMERIC,
            -- Volume Profile
            poc             NUMERIC,
            vah             NUMERIC,
            val             NUMERIC,
            price_vs_va     TEXT,
            -- Flow Proxy
            flow_bias       NUMERIC,
            flow_shock      BOOLEAN DEFAULT false,
            -- SHOCK detail
            shock_type      TEXT,
            shock_direction TEXT,
            -- Breakout confirmation
            breakout_confirmed BOOLEAN DEFAULT false,
            breakout_conditions JSONB DEFAULT '{}'::jsonb,
            -- debug
            raw_inputs      JSONB NOT NULL DEFAULT '{}'::jsonb,
            UNIQUE(symbol, timeframe, ts)
        );
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_mctx_symbol_ts
        ON market_context(symbol, ts DESC);
    """)

    cur.execute("""
        CREATE OR REPLACE VIEW market_context_latest AS
        SELECT DISTINCT ON (symbol)
            id, ts, symbol, timeframe,
            regime, regime_confidence,
            adx_14, plus_di, minus_di, bbw_ratio,
            poc, vah, val, price_vs_va,
            flow_bias, flow_shock,
            shock_type, shock_direction,
            breakout_confirmed, breakout_conditions,
            raw_inputs,
            EXTRACT(EPOCH FROM (now() - ts)) AS age_seconds
        FROM market_context
        ORDER BY symbol, ts DESC;
    """)

    _log('ensure_market_context done')


def run_all():
    """Standalone migration runner for ctx_collector startup."""
    from db_config_ctx import get_main_conn_rw
    conn = None
    try:
        conn = get_main_conn_rw(autocommit=True)
        with conn.cursor() as cur:
            ensure_market_context(cur)
        _log('ctx_migrations run_all complete')
    except Exception as e:
        _log(f'ctx_migrations run_all error: {e}')
        import traceback
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
