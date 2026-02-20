"""
Strategy v2 DB migrations — idempotent CREATE IF NOT EXISTS.
"""

LOG_PREFIX = '[strategy_migrations]'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def run_migrations(cur):
    """Run all strategy v2 schema migrations. Safe to call repeatedly."""

    # 1. strategy_decision_log — decision audit trail
    cur.execute("""
        CREATE TABLE IF NOT EXISTS strategy_decision_log (
            id          BIGSERIAL PRIMARY KEY,
            ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
            symbol      TEXT NOT NULL DEFAULT 'BTC/USDT:USDT',
            mode        TEXT,
            submode     TEXT,
            features    JSONB,
            action      TEXT,
            side        TEXT,
            qty         NUMERIC,
            tp          NUMERIC,
            sl          NUMERIC,
            gate_status TEXT,
            throttle_status TEXT,
            dedupe_hit  BOOLEAN DEFAULT false,
            chase_entry BOOLEAN DEFAULT false,
            reasons     TEXT[],
            signal_key  TEXT
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_sdl_ts
            ON strategy_decision_log (ts DESC)
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_sdl_symbol_mode
            ON strategy_decision_log (symbol, mode)
    """)

    # 2. signal_dedup_log — dedupe tracking
    cur.execute("""
        CREATE TABLE IF NOT EXISTS signal_dedup_log (
            key     TEXT PRIMARY KEY,
            ts      TIMESTAMPTZ NOT NULL DEFAULT now(),
            expired BOOLEAN NOT NULL DEFAULT false
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_signal_dedup_ts
            ON signal_dedup_log (ts DESC)
    """)

    # 3. Add column 'mode' to execution_log (nullable)
    cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'execution_log' AND column_name = 'mode'
            ) THEN
                ALTER TABLE execution_log ADD COLUMN mode TEXT;
            END IF;
        END $$
    """)

    # 4. Add column 'signal_key' to execution_queue (nullable)
    cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'execution_queue' AND column_name = 'signal_key'
            ) THEN
                ALTER TABLE execution_queue ADD COLUMN signal_key TEXT;
            END IF;
        END $$
    """)

    _log('all migrations applied successfully')
