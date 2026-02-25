"""
bench_migrations.py — Schema setup for trading_benchmark DB.

8 tables for benchmark comparison service.
Safe to call repeatedly (IF NOT EXISTS / ADD COLUMN IF NOT EXISTS).
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

LOG_PREFIX = '[bench_migrations]'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _db_conn():
    from db_config_bench import get_bench_conn
    return get_bench_conn()


def ensure_benchmark_sources(cur):
    """Source registry: our strategy + external benchmarks."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS benchmark_sources (
            id              BIGSERIAL PRIMARY KEY,
            kind            TEXT NOT NULL,
            label           TEXT NOT NULL,
            account_tag     TEXT,
            enabled         BOOLEAN NOT NULL DEFAULT true,
            api_key_env     TEXT,
            api_secret_env  TEXT,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)
    # Seed OUR_STRATEGY row
    cur.execute("SELECT count(*) FROM benchmark_sources WHERE kind = 'OUR_STRATEGY';")
    if cur.fetchone()[0] == 0:
        cur.execute("""
            INSERT INTO benchmark_sources (kind, label, account_tag)
            VALUES ('OUR_STRATEGY', 'Our Trading Bot', 'main');
        """)
    # Seed BYBIT_ACCOUNT row
    cur.execute("SELECT count(*) FROM benchmark_sources WHERE kind = 'BYBIT_ACCOUNT';")
    if cur.fetchone()[0] == 0:
        cur.execute("""
            INSERT INTO benchmark_sources (kind, label, account_tag, api_key_env, api_secret_env, enabled)
            VALUES ('BYBIT_ACCOUNT', 'Bybit Benchmark', 'bench',
                    'BYBIT_BENCH_API_KEY', 'BYBIT_BENCH_API_SECRET', false);
        """)
    _log('ensure_benchmark_sources done')


def ensure_bench_executions(cur):
    """Trade execution records from all sources."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bench_executions (
            id          BIGSERIAL PRIMARY KEY,
            ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
            source_id   BIGINT NOT NULL REFERENCES benchmark_sources(id),
            symbol      TEXT NOT NULL DEFAULT 'BTC/USDT:USDT',
            side        TEXT NOT NULL,
            qty         NUMERIC NOT NULL,
            price       NUMERIC NOT NULL,
            fee         NUMERIC NOT NULL DEFAULT 0,
            order_id    TEXT,
            exec_id     TEXT NOT NULL,
            meta        JSONB NOT NULL DEFAULT '{}'::jsonb
        );
    """)
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_bench_exec_source_execid
        ON bench_executions (source_id, exec_id);
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_bench_exec_ts ON bench_executions(ts);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_bench_exec_source ON bench_executions(source_id);')
    _log('ensure_bench_executions done')


def ensure_bench_positions(cur):
    """Position snapshots per source."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bench_positions (
            id                BIGSERIAL PRIMARY KEY,
            ts                TIMESTAMPTZ NOT NULL DEFAULT now(),
            source_id         BIGINT NOT NULL REFERENCES benchmark_sources(id),
            symbol            TEXT NOT NULL DEFAULT 'BTC/USDT:USDT',
            size              NUMERIC NOT NULL DEFAULT 0,
            side              TEXT,
            entry_price       NUMERIC,
            unrealized_pnl    NUMERIC,
            leverage          NUMERIC,
            liquidation_price NUMERIC,
            meta              JSONB NOT NULL DEFAULT '{}'::jsonb
        );
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_bench_pos_ts ON bench_positions(ts);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_bench_pos_source ON bench_positions(source_id);')
    _log('ensure_bench_positions done')


def ensure_bench_equity_timeseries(cur):
    """Equity balance snapshots per source."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bench_equity_timeseries (
            id                 BIGSERIAL PRIMARY KEY,
            ts                 TIMESTAMPTZ NOT NULL DEFAULT now(),
            source_id          BIGINT NOT NULL REFERENCES benchmark_sources(id),
            equity             NUMERIC NOT NULL,
            wallet_balance     NUMERIC,
            available_balance  NUMERIC
        );
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_bench_eq_ts ON bench_equity_timeseries(ts);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_bench_eq_source ON bench_equity_timeseries(source_id);')
    _log('ensure_bench_equity_timeseries done')


def ensure_bench_market_snapshots(cur):
    """Market data snapshots (public, no auth)."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bench_market_snapshots (
            id            BIGSERIAL PRIMARY KEY,
            ts            TIMESTAMPTZ NOT NULL DEFAULT now(),
            symbol        TEXT NOT NULL DEFAULT 'BTC/USDT:USDT',
            price         NUMERIC,
            funding_rate  NUMERIC,
            open_interest NUMERIC,
            meta          JSONB NOT NULL DEFAULT '{}'::jsonb
        );
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_bench_mkt_ts ON bench_market_snapshots(ts);')
    _log('ensure_bench_market_snapshots done')


def ensure_bench_reports(cur):
    """Generated comparison reports."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bench_reports (
            id            BIGSERIAL PRIMARY KEY,
            period        TEXT NOT NULL,
            start_ts      TIMESTAMPTZ NOT NULL,
            end_ts        TIMESTAMPTZ NOT NULL,
            payload_md    TEXT,
            payload_json  JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_bench_reports_created ON bench_reports(created_at DESC);')
    _log('ensure_bench_reports done')


def ensure_bench_proposals(cur):
    """Parameter change proposals (human-approval only)."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bench_proposals (
            id                  BIGSERIAL PRIMARY KEY,
            created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
            based_on_report_id  BIGINT REFERENCES bench_reports(id),
            proposed_changes    JSONB NOT NULL DEFAULT '{}'::jsonb,
            status              TEXT NOT NULL DEFAULT 'DRAFT',
            applied_at          TIMESTAMPTZ,
            rejected_at         TIMESTAMPTZ,
            human_note          TEXT
        );
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_bench_proposals_status ON bench_proposals(status);')
    _log('ensure_bench_proposals done')


def ensure_bench_collector_state(cur):
    """Collector checkpoint state per source."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bench_collector_state (
            id              BIGSERIAL PRIMARY KEY,
            source_id       BIGINT NOT NULL UNIQUE REFERENCES benchmark_sources(id),
            last_exec_ts    TIMESTAMPTZ,
            last_exec_id    TEXT,
            last_position_ts TIMESTAMPTZ,
            last_equity_ts  TIMESTAMPTZ
        );
    """)
    _log('ensure_bench_collector_state done')


def ensure_bench_strategy_signals(cur):
    """Strategy signal history — one row per strategy per evaluation."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bench_strategy_signals (
            id            BIGSERIAL PRIMARY KEY,
            ts            TIMESTAMPTZ NOT NULL DEFAULT now(),
            strategy_name TEXT NOT NULL,
            signal        TEXT NOT NULL,
            confidence    NUMERIC,
            rationale     TEXT,
            indicators    JSONB,
            UNIQUE(strategy_name, ts)
        );
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_bench_sig_name_ts
        ON bench_strategy_signals(strategy_name, ts DESC);
    """)
    _log('ensure_bench_strategy_signals done')


def ensure_bench_virtual_positions(cur):
    """Virtual positions per strategy — max 1 row per strategy."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bench_virtual_positions (
            id            BIGSERIAL PRIMARY KEY,
            source_id     BIGINT NOT NULL REFERENCES benchmark_sources(id),
            strategy_name TEXT NOT NULL UNIQUE,
            side          TEXT,
            size          NUMERIC NOT NULL DEFAULT 0,
            entry_price   NUMERIC,
            entry_ts      TIMESTAMPTZ,
            notional      NUMERIC,
            accumulated_funding NUMERIC DEFAULT 0,
            updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)
    _log('ensure_bench_virtual_positions done')


def ensure_strategy_sources(cur):
    """Seed 4 strategy signal sources into benchmark_sources."""
    strategies = [
        ('STRATEGY_SIGNAL', 'Trend-Follow (EMA/VWAP)', 'trend_follow'),
        ('STRATEGY_SIGNAL', 'Mean-Reversion (BB/RSI)', 'mean_reversion'),
        ('STRATEGY_SIGNAL', 'Volume/VP (POC/VAH/VAL)', 'volume_vp'),
        ('STRATEGY_SIGNAL', 'Volatility/Regime (ATR/BBW)', 'volatility_regime'),
    ]
    for kind, label, tag in strategies:
        cur.execute(
            "SELECT count(*) FROM benchmark_sources WHERE account_tag = %s;", (tag,))
        if cur.fetchone()[0] == 0:
            cur.execute("""
                INSERT INTO benchmark_sources (kind, label, account_tag, enabled)
                VALUES (%s, %s, %s, true);
            """, (kind, label, tag))
    _log('ensure_strategy_sources done')


def migrate_fix_our_strategy_side_mapping(cur):
    """One-time fix: OUR_STRATEGY executions had direction (LONG/SHORT) as side
    instead of trade action (BUY/SELL). Clear stale data so collector re-syncs
    with correct BUY/SELL mapping."""
    cur.execute(
        "SELECT count(*) FROM bench_executions e "
        "JOIN benchmark_sources s ON s.id = e.source_id "
        "WHERE s.kind = 'OUR_STRATEGY' AND e.side IN ('LONG','SHORT');")
    bad_rows = cur.fetchone()[0]
    if bad_rows == 0:
        return  # already migrated or no data

    _log(f'fixing OUR_STRATEGY side mapping: {bad_rows} rows with LONG/SHORT side')

    # Get source_id
    cur.execute("SELECT id FROM benchmark_sources WHERE kind = 'OUR_STRATEGY' LIMIT 1;")
    row = cur.fetchone()
    if not row:
        return
    src_id = row[0]

    # Clear stale execution + equity data → collector will re-sync
    cur.execute("DELETE FROM bench_executions WHERE source_id = %s;", (src_id,))
    cur.execute("DELETE FROM bench_equity_timeseries WHERE source_id = %s;", (src_id,))
    cur.execute("DELETE FROM bench_collector_state WHERE source_id = %s;", (src_id,))
    _log(f'OUR_STRATEGY data cleared for source_id={src_id}, will re-sync on next poll')


def run_all():
    """Run all benchmark migrations. Safe to call multiple times."""
    conn = None
    try:
        conn = _db_conn()
        conn.autocommit = True
        with conn.cursor() as cur:
            ensure_benchmark_sources(cur)
            ensure_bench_executions(cur)
            ensure_bench_positions(cur)
            ensure_bench_equity_timeseries(cur)
            ensure_bench_market_snapshots(cur)
            ensure_bench_reports(cur)
            ensure_bench_proposals(cur)
            ensure_bench_collector_state(cur)
            ensure_bench_strategy_signals(cur)
            ensure_bench_virtual_positions(cur)
            ensure_strategy_sources(cur)
            migrate_fix_our_strategy_side_mapping(cur)
        _log('run_all complete')
    except Exception as e:
        _log(f'run_all error: {e}')
        import traceback
        traceback.print_exc()
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == '__main__':
    run_all()
