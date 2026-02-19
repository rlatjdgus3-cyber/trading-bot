"""
db_migrations.py — DB schema ensure functions.

Each module imports the relevant ensure_* function to guarantee
its tables/columns exist before first use.  Safe to call repeatedly
(IF NOT EXISTS / ADD COLUMN IF NOT EXISTS).
"""
import os
import sys
sys.path.insert(0, '/root/trading-bot/app')
LOG_PREFIX = '[db_migrations]'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _db_conn():
    from db_config import get_conn
    return get_conn()


def ensure_trade_process_log(cur):
    '''Create trade_process_log table if not exists.'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.trade_process_log (
            id               BIGSERIAL PRIMARY KEY,
            ts               TIMESTAMPTZ NOT NULL DEFAULT now(),
            signal_id        BIGINT,
            decision_context JSONB NOT NULL DEFAULT '{}'::jsonb,
            long_score       INTEGER,
            short_score      INTEGER,
            chosen_side      TEXT,
            size_percent     INTEGER,
            capital_limit    NUMERIC,
            risk_check_result TEXT,
            order_sent_time  TIMESTAMPTZ,
            order_fill_time  TIMESTAMPTZ,
            fill_price       NUMERIC,
            pnl_after_trade  NUMERIC,
            rejection_reason TEXT,
            source           TEXT DEFAULT 'manual'
        );
    """)


def ensure_autopilot_config(cur):
    '''Create autopilot_config table if not exists.
    Inserts a default row (enabled=false) if table is empty.'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.autopilot_config (
            id         BIGSERIAL PRIMARY KEY,
            enabled    BOOLEAN NOT NULL DEFAULT false,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            config     JSONB NOT NULL DEFAULT '{}'::jsonb
        );
    """)
    cur.execute('SELECT count(*) FROM public.autopilot_config;')
    cnt = cur.fetchone()[0]
    if cnt == 0:
        cur.execute("""
            INSERT INTO public.autopilot_config (enabled) VALUES (false);
        """)


def ensure_stage_column(cur):
    '''Add stage column to signals_action_v3 if not exists.'''
    cur.execute("""
        ALTER TABLE signals_action_v3
            ADD COLUMN IF NOT EXISTS stage TEXT DEFAULT 'SIGNAL_CREATED';
    """)


def ensure_execution_log(cur):
    '''Create execution_log table for fill_watcher order tracking.'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.execution_log (
            id               BIGSERIAL PRIMARY KEY,
            ts               TIMESTAMPTZ NOT NULL DEFAULT now(),
            -- order identification
            order_id         TEXT NOT NULL,
            client_order_id  TEXT,
            symbol           TEXT NOT NULL DEFAULT 'BTC/USDT:USDT',
            -- order context
            order_type       TEXT NOT NULL,
            direction        TEXT NOT NULL,
            signal_id        BIGINT,
            decision_id      BIGINT,
            close_reason     TEXT,
            -- request details
            requested_qty    NUMERIC,
            requested_usdt   NUMERIC,
            ticker_price     NUMERIC,
            -- fill details (fill_watcher populates)
            status           TEXT NOT NULL DEFAULT 'SENT',
            filled_qty       NUMERIC,
            remaining_qty    NUMERIC,
            avg_fill_price   NUMERIC,
            fee              NUMERIC,
            fee_currency     TEXT,
            realized_pnl     NUMERIC,
            -- position verification (fill_watcher populates)
            position_after_side TEXT,
            position_after_qty  NUMERIC,
            position_verified   BOOLEAN DEFAULT false,
            -- timing
            order_sent_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
            first_fill_at    TIMESTAMPTZ,
            last_fill_at     TIMESTAMPTZ,
            verified_at      TIMESTAMPTZ,
            -- watch metadata
            poll_count       INTEGER DEFAULT 0,
            last_poll_at     TIMESTAMPTZ,
            raw_order_response JSONB,
            raw_fetch_response JSONB,
            error_detail     TEXT
        );
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_exec_log_status ON execution_log(status);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_exec_log_order_id ON execution_log(order_id);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_exec_log_signal_id ON execution_log(signal_id);')
    _log('ensure_execution_log done')


def ensure_execution_queue(cur):
    '''Commands from position_manager to executor.'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.execution_queue (
            id               BIGSERIAL PRIMARY KEY,
            ts               TIMESTAMPTZ NOT NULL DEFAULT now(),
            symbol           TEXT NOT NULL DEFAULT 'BTC/USDT:USDT',
            action_type      TEXT NOT NULL,
            direction        TEXT,
            target_qty       NUMERIC,
            target_usdt      NUMERIC,
            reduce_pct       NUMERIC,
            source           TEXT DEFAULT 'position_manager',
            pm_decision_id   BIGINT,
            emergency_id     BIGINT,
            reason           TEXT,
            status           TEXT NOT NULL DEFAULT 'PENDING',
            priority         INTEGER NOT NULL DEFAULT 5,
            expire_at        TIMESTAMPTZ,
            depends_on       BIGINT,
            execution_log_id BIGINT,
            meta             JSONB NOT NULL DEFAULT '{}'::jsonb
        );
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_eq_status ON execution_queue(status);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_eq_priority ON execution_queue(priority, id);')
    _log('ensure_execution_queue done')


def ensure_pm_decision_log(cur):
    '''Every position_manager decision with full context.'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.pm_decision_log (
            id               BIGSERIAL PRIMARY KEY,
            ts               TIMESTAMPTZ NOT NULL DEFAULT now(),
            symbol           TEXT NOT NULL DEFAULT 'BTC/USDT:USDT',
            position_side    TEXT,
            position_qty     NUMERIC,
            avg_entry_price  NUMERIC,
            stage            INTEGER,
            current_price    NUMERIC,
            long_score       INTEGER,
            short_score      INTEGER,
            atr_14           NUMERIC,
            rsi_14           NUMERIC,
            poc              NUMERIC,
            vah              NUMERIC,
            val              NUMERIC,
            chosen_action    TEXT NOT NULL,
            action_reason    TEXT,
            full_context     JSONB NOT NULL DEFAULT '{}'::jsonb
        );
    """)
    _log('ensure_pm_decision_log done')


def ensure_position_state(cur):
    '''Pyramid stage tracking and average entry.'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.position_state (
            symbol           TEXT PRIMARY KEY,
            side             TEXT,
            total_qty        NUMERIC NOT NULL DEFAULT 0,
            avg_entry_price  NUMERIC,
            stage            INTEGER NOT NULL DEFAULT 0,
            capital_used_usdt NUMERIC NOT NULL DEFAULT 0,
            stages_detail    JSONB NOT NULL DEFAULT '[]'::jsonb,
            updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)
    _log('ensure_position_state done')


def ensure_emergency_analysis_log(cur):
    '''Claude API call logging.'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.emergency_analysis_log (
            id               BIGSERIAL PRIMARY KEY,
            ts               TIMESTAMPTZ NOT NULL DEFAULT now(),
            symbol           TEXT NOT NULL DEFAULT 'BTC/USDT:USDT',
            trigger_type     TEXT NOT NULL,
            trigger_detail   JSONB NOT NULL DEFAULT '{}'::jsonb,
            context_packet   JSONB NOT NULL DEFAULT '{}'::jsonb,
            response_raw     TEXT,
            risk_level       TEXT,
            recommended_action TEXT,
            confidence       NUMERIC,
            reason_bullets   JSONB,
            ttl_seconds      INTEGER,
            applied          BOOLEAN DEFAULT false,
            api_latency_ms   INTEGER,
            fallback_used    BOOLEAN DEFAULT false
        );
    """)
    _log('ensure_emergency_analysis_log done')


def ensure_safety_limits(cur):
    '''Configurable safety parameters.'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.safety_limits (
            id                    BIGSERIAL PRIMARY KEY,
            capital_limit_usdt    NUMERIC NOT NULL DEFAULT 900,
            max_daily_trades      INTEGER NOT NULL DEFAULT 20,
            max_hourly_trades     INTEGER NOT NULL DEFAULT 8,
            daily_loss_limit_usdt NUMERIC NOT NULL DEFAULT -45,
            max_pyramid_stages    INTEGER NOT NULL DEFAULT 3,
            add_size_min_pct      NUMERIC NOT NULL DEFAULT 5,
            add_size_max_pct      NUMERIC NOT NULL DEFAULT 10,
            circuit_breaker_window_sec INTEGER NOT NULL DEFAULT 300,
            circuit_breaker_max_orders INTEGER NOT NULL DEFAULT 10,
            updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)
    cur.execute('SELECT count(*) FROM public.safety_limits;')
    cnt = cur.fetchone()[0]
    if cnt == 0:
        cur.execute('INSERT INTO public.safety_limits DEFAULT VALUES;')
    _log('ensure_safety_limits done')


def ensure_execution_log_pm_columns(cur):
    '''Add position_manager columns to execution_log.'''
    for col, dtype in (('source_queue', 'TEXT'), ('execution_queue_id', 'BIGINT'),
                        ('position_before_side', 'TEXT'), ('position_before_qty', 'NUMERIC')):
        cur.execute(f"""
            ALTER TABLE execution_log
                ADD COLUMN IF NOT EXISTS {col} {dtype};
        """)
    _log('ensure_execution_log_pm_columns done')


def ensure_indicator_columns(cur):
    '''Add RSI, ATR, MA columns to indicators.'''
    for col, dtype in (('rsi_14', 'NUMERIC'), ('atr_14', 'NUMERIC'),
                        ('ma_50', 'NUMERIC'), ('ma_200', 'NUMERIC')):
        cur.execute(f"""
            ALTER TABLE indicators
                ADD COLUMN IF NOT EXISTS {col} {dtype};
        """)
    _log('ensure_indicator_columns done')


def ensure_vol_profile_columns(cur):
    '''Add VAH/VAL columns to vol_profile.'''
    for col, dtype in (('vah', 'NUMERIC'), ('val', 'NUMERIC')):
        cur.execute(f"""
            ALTER TABLE vol_profile
                ADD COLUMN IF NOT EXISTS {col} {dtype};
        """)
    _log('ensure_vol_profile_columns done')


def ensure_trade_process_log_add_columns(cur):
    '''Add ADD-routing columns to trade_process_log.'''
    for col, dtype in (('position_exists', 'BOOLEAN'), ('add_decision', 'TEXT'),
                        ('add_reason', 'TEXT'), ('pyramiding_level', 'INTEGER')):
        cur.execute(f"""
            ALTER TABLE trade_process_log
                ADD COLUMN IF NOT EXISTS {col} {dtype};
        """)
    _log('ensure_trade_process_log_add_columns done')


def ensure_trade_process_log_budget_columns(cur):
    '''Add 7-stage budget columns to trade_process_log.'''
    for col, dtype in (('chosen_start_stage', 'INTEGER'), ('entry_policy', 'TEXT'),
                        ('entry_size_pct', 'NUMERIC'), ('add_slice_pct', 'NUMERIC'),
                        ('trade_budget_used_after', 'NUMERIC')):
        cur.execute(f"""
            ALTER TABLE trade_process_log
                ADD COLUMN IF NOT EXISTS {col} {dtype};
        """)
    _log('ensure_trade_process_log_budget_columns done')


def ensure_position_state_budget_columns(cur):
    '''Add 7-stage budget tracking columns to position_state.'''
    for col, dtype in (('start_stage_used', 'INTEGER'),
                        ('trade_budget_used_pct', 'NUMERIC NOT NULL DEFAULT 0'),
                        ('next_stage_available', 'INTEGER'),
                        ('stage_consumed_mask', 'INTEGER NOT NULL DEFAULT 0'),
                        ('last_reason', 'TEXT'), ('last_score', 'NUMERIC'),
                        ('accumulated_entry_fee', 'NUMERIC NOT NULL DEFAULT 0')):
        cur.execute(f"""
            ALTER TABLE position_state
                ADD COLUMN IF NOT EXISTS {col} {dtype};
        """)
    _log('ensure_position_state_budget_columns done')


def ensure_safety_limits_add_threshold(cur):
    '''Add add_score_threshold column to safety_limits.'''
    cur.execute("""
        ALTER TABLE safety_limits
            ADD COLUMN IF NOT EXISTS add_score_threshold INTEGER NOT NULL DEFAULT 45;
    """)
    _log('ensure_safety_limits_add_threshold done')


def ensure_safety_limits_budget_columns(cur):
    '''Add trade_budget_pct and stage_slice_pct to safety_limits.'''
    for col, dtype in (('trade_budget_pct', 'NUMERIC NOT NULL DEFAULT 70'),
                        ('stage_slice_pct', 'NUMERIC NOT NULL DEFAULT 10'),
                        ('max_stages', 'INTEGER NOT NULL DEFAULT 7')):
        cur.execute(f"""
            ALTER TABLE safety_limits
                ADD COLUMN IF NOT EXISTS {col} {dtype};
        """)
    cur.execute("""
        UPDATE safety_limits SET max_pyramid_stages = 7
        WHERE max_pyramid_stages = 3;
    """)
    _log('ensure_safety_limits_budget_columns done')


def ensure_safety_limits_stoploss_column(cur):
    '''Add stop_loss_pct column to safety_limits.'''
    cur.execute("""
        ALTER TABLE safety_limits
            ADD COLUMN IF NOT EXISTS stop_loss_pct NUMERIC NOT NULL DEFAULT 2.0;
    """)
    _log('ensure_safety_limits_stoploss_column done')


def ensure_pending_add_slices(cur):
    '''Track accumulated ADD slices that are below exchange minimum order qty.'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.pending_add_slices (
            symbol           TEXT PRIMARY KEY,
            direction        TEXT NOT NULL,
            accumulated_usdt NUMERIC NOT NULL DEFAULT 0,
            accumulated_pct  NUMERIC NOT NULL DEFAULT 0,
            slices_count     INTEGER NOT NULL DEFAULT 0,
            created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)
    _log('ensure_pending_add_slices done')


def ensure_test_events_log(cur):
    '''Test lifecycle event log (extension, freeze, end, etc.).'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.test_events_log (
            id         BIGSERIAL PRIMARY KEY,
            ts         TIMESTAMPTZ NOT NULL DEFAULT now(),
            event_type TEXT NOT NULL,
            detail     JSONB NOT NULL DEFAULT '{}'::jsonb
        );
    """)
    _log('ensure_test_events_log done')


def ensure_event_history(cur):
    '''Macro event database for historical pattern matching.'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.event_history (
            id               BIGSERIAL PRIMARY KEY,
            ts               TIMESTAMPTZ NOT NULL DEFAULT now(),
            event_type       TEXT NOT NULL,
            keywords         TEXT[] NOT NULL DEFAULT '{}',
            headline         TEXT,
            source           TEXT,
            news_id          BIGINT,
            impact_score     INTEGER,
            direction        TEXT,
            btc_price_at     NUMERIC,
            btc_move_1h      NUMERIC,
            btc_move_4h      NUMERIC,
            btc_move_24h     NUMERIC,
            outcome_filled   BOOLEAN NOT NULL DEFAULT false,
            metadata         JSONB NOT NULL DEFAULT '{}'::jsonb
        );
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_event_history_ts ON event_history(ts);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_event_history_type ON event_history(event_type);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_event_history_keywords ON event_history USING GIN(keywords);')
    _log('ensure_event_history done')


def ensure_liquidity_snapshots(cur):
    '''Liquidity flow snapshots (funding, OI, orderbook).'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.liquidity_snapshots (
            id                  BIGSERIAL PRIMARY KEY,
            ts                  TIMESTAMPTZ NOT NULL DEFAULT now(),
            symbol              TEXT NOT NULL DEFAULT 'BTC/USDT:USDT',
            funding_rate        NUMERIC,
            open_interest       NUMERIC,
            oi_value_usdt       NUMERIC,
            bid_depth_usdt      NUMERIC,
            ask_depth_usdt      NUMERIC,
            orderbook_imbalance NUMERIC,
            metadata            JSONB NOT NULL DEFAULT '{}'::jsonb
        );
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_liq_snap_ts ON liquidity_snapshots(ts);')
    _log('ensure_liquidity_snapshots done')


def ensure_macro_data(cur):
    '''Macro data store (QQQ, BTC vol, etc.).'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.macro_data (
            id      BIGSERIAL PRIMARY KEY,
            ts      TIMESTAMPTZ NOT NULL DEFAULT now(),
            source  TEXT NOT NULL,
            price   NUMERIC,
            metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
            UNIQUE(source, ts)
        );
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_macro_data_source ON macro_data(source, ts);')
    _log('ensure_macro_data done')


def ensure_score_history(cur):
    '''Unified score history for analysis and optimization.'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.score_history (
            id                      BIGSERIAL PRIMARY KEY,
            ts                      TIMESTAMPTZ NOT NULL DEFAULT now(),
            symbol                  TEXT NOT NULL DEFAULT 'BTC/USDT:USDT',
            total_score             NUMERIC,
            tech_score              NUMERIC,
            macro_event_score       NUMERIC,
            market_regime_score     NUMERIC,
            liquidity_flow_score    NUMERIC,
            position_context_score  NUMERIC,
            dominant_side           TEXT,
            computed_stage          INTEGER,
            dynamic_stop_loss_pct   NUMERIC,
            btc_price               NUMERIC,
            context                 JSONB NOT NULL DEFAULT '{}'::jsonb,
            btc_move_1h             NUMERIC,
            btc_move_4h             NUMERIC
        );
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_score_history_ts ON score_history(ts);')
    _log('ensure_score_history done')


def ensure_score_weights(cur):
    '''Configurable axis weights for unified scoring.'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.score_weights (
            id          BIGSERIAL PRIMARY KEY,
            ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
            tech_w      NUMERIC NOT NULL DEFAULT 0.40,
            macro_w     NUMERIC NOT NULL DEFAULT 0.30,
            regime_w    NUMERIC NOT NULL DEFAULT 0.15,
            liquidity_w NUMERIC NOT NULL DEFAULT 0.10,
            position_w  NUMERIC NOT NULL DEFAULT 0.05,
            source      TEXT DEFAULT 'manual',
            metadata    JSONB NOT NULL DEFAULT '{}'::jsonb
        );
    """)
    cur.execute('SELECT count(*) FROM public.score_weights;')
    cnt = cur.fetchone()[0]
    if cnt == 0:
        cur.execute('INSERT INTO public.score_weights DEFAULT VALUES;')
    _log('ensure_score_weights done')


def ensure_safety_limits_dynamic_sl_columns(cur):
    '''Add dynamic stop-loss columns to safety_limits.'''
    for col, dtype in (('dynamic_sl_enabled', 'BOOLEAN NOT NULL DEFAULT false'),
                        ('dynamic_sl_min_pct', 'NUMERIC NOT NULL DEFAULT 1.2'),
                        ('dynamic_sl_max_pct', 'NUMERIC NOT NULL DEFAULT 3.0'),
                        ('dynamic_sl_base_pct', 'NUMERIC NOT NULL DEFAULT 2.0')):
        cur.execute(f"""
            ALTER TABLE safety_limits
                ADD COLUMN IF NOT EXISTS {col} {dtype};
        """)
    _log('ensure_safety_limits_dynamic_sl_columns done')


def ensure_market_ohlcv(cur):
    '''5m OHLCV data for FACT pipeline analysis.'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.market_ohlcv (
            symbol TEXT NOT NULL DEFAULT 'BTC/USDT:USDT',
            tf     TEXT NOT NULL DEFAULT '5m',
            ts     TIMESTAMPTZ NOT NULL,
            o      NUMERIC NOT NULL,
            h      NUMERIC NOT NULL,
            l      NUMERIC NOT NULL,
            c      NUMERIC NOT NULL,
            v      NUMERIC NOT NULL DEFAULT 0,
            PRIMARY KEY (symbol, tf, ts)
        );
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_market_ohlcv_ts ON market_ohlcv(ts);')
    _log('ensure_market_ohlcv done')


def ensure_news_market_reaction(cur):
    '''Price reaction data per news item for FACT analysis.'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.news_market_reaction (
            id          BIGSERIAL PRIMARY KEY,
            news_id     BIGINT NOT NULL,
            ts_news     TIMESTAMPTZ NOT NULL,
            btc_price_at NUMERIC,
            ret_1h      NUMERIC,
            ret_4h      NUMERIC,
            ret_24h     NUMERIC,
            vol_1h      NUMERIC,
            vol_4h      NUMERIC,
            category    TEXT,
            computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE(news_id)
        );
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_nmr_ts ON news_market_reaction(ts_news);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_nmr_category ON news_market_reaction(category);')
    _log('ensure_news_market_reaction done')


def ensure_events(cur):
    '''Volatility spike event detection table.'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.events (
            id           BIGSERIAL PRIMARY KEY,
            kind         TEXT NOT NULL,
            start_ts     TIMESTAMPTZ NOT NULL,
            end_ts       TIMESTAMPTZ,
            symbol       TEXT NOT NULL DEFAULT 'BTC/USDT:USDT',
            vol_zscore   NUMERIC,
            vol_ratio    NUMERIC,
            vol_window   TEXT DEFAULT '1h',
            btc_price_at NUMERIC,
            btc_move_1h  NUMERIC,
            btc_move_4h  NUMERIC,
            btc_move_24h NUMERIC,
            direction    TEXT,
            confidence   NUMERIC,
            category     TEXT,
            keywords     TEXT[] NOT NULL DEFAULT '{}',
            metadata     JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_events_kind ON events(kind);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_events_start_ts ON events(start_ts DESC);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_events_category ON events(category);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_events_keywords ON events USING GIN(keywords);')
    _log('ensure_events done')


def ensure_event_news(cur):
    '''Junction table linking events to news items.'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.event_news (
            id       BIGSERIAL PRIMARY KEY,
            event_id BIGINT NOT NULL,
            news_id  BIGINT NOT NULL,
            relevance NUMERIC DEFAULT 1.0,
            UNIQUE(event_id, news_id)
        );
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_event_news_event ON event_news(event_id);')
    _log('ensure_event_news done')


def ensure_claude_analyses(cur):
    '''Store all Claude analysis inputs/outputs for feedback loop.'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.claude_analyses (
            id                  BIGSERIAL PRIMARY KEY,
            ts                  TIMESTAMPTZ NOT NULL DEFAULT now(),
            kind                TEXT NOT NULL,
            symbol              TEXT NOT NULL DEFAULT 'BTC/USDT:USDT',
            event_id            BIGINT,
            emergency_log_id    BIGINT,
            input_packet        JSONB NOT NULL DEFAULT '{}'::jsonb,
            output_packet       JSONB NOT NULL DEFAULT '{}'::jsonb,
            similar_events_used JSONB NOT NULL DEFAULT '[]'::jsonb,
            risk_level          TEXT,
            recommended_action  TEXT,
            confidence          NUMERIC,
            reason_bullets      JSONB,
            ttl_seconds         INTEGER,
            model_used          TEXT DEFAULT 'claude-sonnet-4-20250514',
            api_latency_ms      INTEGER,
            fallback_used       BOOLEAN DEFAULT false
        );
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_ca_ts ON claude_analyses(ts DESC);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_ca_kind ON claude_analyses(kind);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_ca_event ON claude_analyses(event_id);')
    _log('ensure_claude_analyses done')


def ensure_analysis_outcomes(cur):
    '''Track outcomes of Claude recommendations for feedback.'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.analysis_outcomes (
            id                  BIGSERIAL PRIMARY KEY,
            claude_analysis_id  BIGINT NOT NULL,
            ts                  TIMESTAMPTZ NOT NULL DEFAULT now(),
            executed_action     TEXT,
            execution_queue_id  BIGINT,
            btc_move_1h         NUMERIC,
            btc_move_4h         NUMERIC,
            btc_move_24h        NUMERIC,
            outcome_label       TEXT DEFAULT 'pending',
            filled_at           TIMESTAMPTZ,
            metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
            UNIQUE(claude_analysis_id)
        );
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_ao_label ON analysis_outcomes(outcome_label);')
    _log('ensure_analysis_outcomes done')


def ensure_price_event_stats(cur):
    '''Materialized view: aggregate price stats per event direction/zscore band.'''
    cur.execute("""
        SELECT 1 FROM pg_matviews WHERE matviewname = 'price_event_stats';
    """)
    if cur.fetchone():
        _log('ensure_price_event_stats: already exists')
        cur.execute("REFRESH MATERIALIZED VIEW public.price_event_stats;")
        _log('ensure_price_event_stats: refreshed')
        return
    cur.execute("""
        CREATE MATERIALIZED VIEW public.price_event_stats AS
        SELECT
            direction,
            CASE
                WHEN vol_zscore < 4 THEN 'low'
                WHEN vol_zscore < 6 THEN 'mid'
                ELSE 'high'
            END AS zscore_band,
            COUNT(*) AS event_count,
            AVG(btc_move_1h) AS avg_move_1h,
            AVG(btc_move_4h) AS avg_move_4h,
            AVG(btc_move_24h) AS avg_move_24h,
            STDDEV(btc_move_4h) AS std_move_4h,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY btc_move_4h) AS p25_move_4h,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY btc_move_4h) AS p75_move_4h,
            AVG(CASE WHEN btc_move_4h > 0 THEN 1.0 ELSE 0.0 END) AS continuation_rate
        FROM events
        WHERE btc_move_4h IS NOT NULL
        GROUP BY direction,
            CASE
                WHEN vol_zscore < 4 THEN 'low'
                WHEN vol_zscore < 6 THEN 'mid'
                ELSE 'high'
            END;
    """)
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_pes_dir_band
        ON public.price_event_stats (direction, zscore_band);
    """)
    _log('ensure_price_event_stats done')


def ensure_openclo_tables(cur):
    '''Create settings and command_queue tables used by openclo.py.'''
    cur.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS command_queue (
            id SERIAL PRIMARY KEY,
            ts TIMESTAMPTZ DEFAULT now(),
            cmd TEXT,
            args JSONB DEFAULT '{}',
            status TEXT DEFAULT 'pending',
            who TEXT DEFAULT 'unknown',
            result TEXT
        );
    ''')
    _log('ensure_openclo_tables done')


def ensure_openclaw_directives(cur):
    '''Create openclaw_directives table for directive logging.'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.openclaw_directives (
            id              BIGSERIAL PRIMARY KEY,
            ts              TIMESTAMPTZ NOT NULL DEFAULT now(),
            dtype           TEXT NOT NULL,
            params          JSONB NOT NULL DEFAULT '{}'::jsonb,
            source          TEXT DEFAULT 'telegram',
            status          TEXT NOT NULL DEFAULT 'PENDING',
            result          JSONB DEFAULT '{}'::jsonb,
            idempotency_key TEXT UNIQUE,
            applied_at      TIMESTAMPTZ,
            error           TEXT
        );
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_ocd_ts ON openclaw_directives(ts DESC);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_ocd_dtype ON openclaw_directives(dtype);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_ocd_status ON openclaw_directives(status);')
    _log('ensure_openclaw_directives done')


def ensure_openclaw_policies(cur):
    '''Create openclaw_policies table for active policy key-value store.'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.openclaw_policies (
            key             TEXT PRIMARY KEY,
            value           JSONB NOT NULL,
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            directive_id    BIGINT,
            description     TEXT
        );
    """)
    _log('ensure_openclaw_policies done')


def ensure_claude_gate_columns(cur):
    '''Add token/cost/gate columns to claude_analyses for gate tracking.'''
    for col, dtype in (('input_tokens', 'INTEGER'),
                        ('output_tokens', 'INTEGER'),
                        ('estimated_cost_usd', 'NUMERIC(8,4)'),
                        ('gate_type', 'TEXT')):
        cur.execute(f"""
            ALTER TABLE claude_analyses
                ADD COLUMN IF NOT EXISTS {col} {dtype};
        """)
    _log('ensure_claude_gate_columns done')


def ensure_score_weights_v2(cur):
    '''Add news_event_w column and update weights for price-event strategy.'''
    # Add news_event_w column if missing
    cur.execute("""
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'score_weights' AND column_name = 'news_event_w';
    """)
    if not cur.fetchone():
        cur.execute("""
            ALTER TABLE public.score_weights
            ADD COLUMN news_event_w NUMERIC NOT NULL DEFAULT 0.15;
        """)
        _log('ensure_score_weights_v2: added news_event_w column')
    # Update default row to new weights: regime=0.25, news_event=0.05
    cur.execute("""
        UPDATE public.score_weights
        SET regime_w = 0.25, news_event_w = 0.05
        WHERE id = (SELECT MIN(id) FROM public.score_weights)
          AND regime_w = 0.15 AND news_event_w = 0.15;
    """)
    _log('ensure_score_weights_v2 done')


def ensure_pm_decision_log_model_used(cur):
    '''Add model_used, model_provider, model_latency_ms columns to pm_decision_log.'''
    for col, dtype in (('model_used', 'TEXT'),
                        ('model_provider', 'TEXT'),
                        ('model_latency_ms', 'INTEGER')):
        cur.execute(f"""
            ALTER TABLE pm_decision_log
                ADD COLUMN IF NOT EXISTS {col} {dtype};
        """)
    _log('ensure_pm_decision_log_model_used done')


def ensure_claude_analyses_model_provider(cur):
    '''Add model_provider column to claude_analyses for provider tracking.'''
    cur.execute("""
        ALTER TABLE claude_analyses
            ADD COLUMN IF NOT EXISTS model_provider TEXT;
    """)
    _log('ensure_claude_analyses_model_provider done')


def ensure_macro_trace(cur):
    '''Macro trace: BTC reaction after news events for news→strategy pipeline.'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.macro_trace (
            id              BIGSERIAL PRIMARY KEY,
            news_id         BIGINT NOT NULL,
            ts_news         TIMESTAMPTZ NOT NULL,
            btc_price_at    NUMERIC,
            btc_ret_30m     NUMERIC,
            btc_ret_2h      NUMERIC,
            btc_ret_24h     NUMERIC,
            vol_2h          NUMERIC,
            vol_baseline    NUMERIC,
            spike_zscore    NUMERIC,
            regime_at_time  TEXT,
            label           TEXT,
            computed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE(news_id)
        );
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_macro_trace_ts ON macro_trace(ts_news DESC);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_macro_trace_label ON macro_trace(label);')
    _log('ensure_macro_trace done')


def ensure_event_trigger_log(cur):
    '''Event trigger evaluation log.'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.event_trigger_log (
            id              BIGSERIAL PRIMARY KEY,
            ts              TIMESTAMPTZ NOT NULL DEFAULT now(),
            symbol          TEXT NOT NULL DEFAULT 'BTC/USDT:USDT',
            mode            TEXT NOT NULL,
            triggers        JSONB NOT NULL DEFAULT '[]',
            event_hash      TEXT,
            snapshot_ts     TIMESTAMPTZ,
            snapshot_price  NUMERIC,
            claude_called   BOOLEAN DEFAULT false,
            claude_result   JSONB,
            call_type       TEXT,
            dedup_blocked   BOOLEAN DEFAULT false
        );
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_etl_ts ON event_trigger_log(ts);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_etl_mode ON event_trigger_log(mode);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_etl_hash ON event_trigger_log(event_hash);')
    _log('ensure_event_trigger_log done')


def ensure_compliance_log(cur):
    '''Exchange compliance event log — all order validation/rejection tracking.'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.compliance_log (
            id                  BIGSERIAL PRIMARY KEY,
            ts                  TIMESTAMPTZ NOT NULL DEFAULT now(),
            symbol              TEXT NOT NULL DEFAULT 'BTC/USDT:USDT',
            event_type          TEXT NOT NULL,
            order_params        JSONB NOT NULL DEFAULT '{}'::jsonb,
            compliance_passed   BOOLEAN NOT NULL DEFAULT false,
            reject_reason       TEXT,
            exchange_error_code INTEGER,
            suggested_fix       TEXT,
            emergency_flag      BOOLEAN NOT NULL DEFAULT false,
            detail              JSONB NOT NULL DEFAULT '{}'::jsonb
        );
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_compliance_ts ON compliance_log(ts);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_compliance_passed ON compliance_log(compliance_passed);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_compliance_error ON compliance_log(exchange_error_code);')
    _log('ensure_compliance_log done')


def ensure_execution_queue_compliance_columns(cur):
    '''Add compliance tracking columns to execution_queue.'''
    for col, dtype in (('compliance_passed', 'BOOLEAN'),
                       ('reject_reason', 'TEXT'),
                       ('exchange_error_code', 'INTEGER'),
                       ('suggested_fix', 'TEXT'),
                       ('emergency_flag', 'BOOLEAN DEFAULT false')):
        cur.execute(f"""
            ALTER TABLE execution_queue
                ADD COLUMN IF NOT EXISTS {col} {dtype};
        """)
    _log('ensure_execution_queue_compliance_columns done')


def ensure_event_lock(cur):
    '''DB-based event dedup lock — replaces in-memory dedup across all processes.
    key = symbol:trigger_type:price_bucket, ttl-based expiry.'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.event_lock (
            id          BIGSERIAL PRIMARY KEY,
            lock_key    TEXT NOT NULL UNIQUE,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            expires_at  TIMESTAMPTZ NOT NULL,
            caller      TEXT NOT NULL DEFAULT 'unknown',
            lock_type   TEXT NOT NULL DEFAULT 'event',
            detail      JSONB NOT NULL DEFAULT '{}'::jsonb
        );
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_event_lock_key ON event_lock(lock_key);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_event_lock_expires ON event_lock(expires_at);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_event_lock_type ON event_lock(lock_type);')
    _log('ensure_event_lock done')


def ensure_hold_consecutive(cur):
    '''Track consecutive HOLD results per symbol for suppress_lock generation.'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.hold_consecutive (
            symbol          TEXT PRIMARY KEY,
            consecutive_count INTEGER NOT NULL DEFAULT 0,
            last_trigger_types TEXT[] NOT NULL DEFAULT '{}',
            last_caller     TEXT,
            last_action     TEXT,
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)
    _log('ensure_hold_consecutive done')


def ensure_claude_call_log(cur):
    '''Per-call log with caller attribution for cost/frequency tracking.'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.claude_call_log (
            id              BIGSERIAL PRIMARY KEY,
            ts              TIMESTAMPTZ NOT NULL DEFAULT now(),
            caller          TEXT NOT NULL,
            gate_type       TEXT NOT NULL,
            call_type       TEXT NOT NULL DEFAULT 'AUTO',
            model_used      TEXT,
            input_tokens    INTEGER DEFAULT 0,
            output_tokens   INTEGER DEFAULT 0,
            estimated_cost  NUMERIC(8,6) DEFAULT 0,
            latency_ms      INTEGER DEFAULT 0,
            event_hash      TEXT,
            trigger_types   TEXT[],
            action_result   TEXT,
            allowed         BOOLEAN NOT NULL DEFAULT true,
            deny_reason     TEXT,
            detail          JSONB NOT NULL DEFAULT '{}'::jsonb
        );
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_ccl_ts ON claude_call_log(ts);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_ccl_caller ON claude_call_log(caller);')
    _log('ensure_claude_call_log done')


def ensure_news_title_ko(cur):
    '''Add title_ko column to news table for Korean translation.'''
    cur.execute("""
        ALTER TABLE public.news ADD COLUMN IF NOT EXISTS title_ko TEXT;
    """)
    _log('ensure_news_title_ko done')


def ensure_pm_decision_log_extended(cur):
    '''Add actor, candidate/final action, confidence, used_news_ids, claude_skipped columns.'''
    for col, dtype in (('actor', "VARCHAR(20) DEFAULT 'engine'"),
                        ('candidate_action', 'VARCHAR(20)'),
                        ('final_action', 'VARCHAR(20)'),
                        ('confidence', 'FLOAT'),
                        ('used_news_ids', 'TEXT'),
                        ('claude_skipped', 'BOOLEAN DEFAULT FALSE')):
        cur.execute(f"""
            ALTER TABLE pm_decision_log
                ADD COLUMN IF NOT EXISTS {col} {dtype};
        """)
    _log('ensure_pm_decision_log_extended done')


def ensure_safety_limits_hourly_15(cur):
    '''Update max_hourly_trades from 8 to 15.'''
    cur.execute("""
        UPDATE safety_limits SET max_hourly_trades = 15
        WHERE max_hourly_trades = 8;
    """)
    _log('ensure_safety_limits_hourly_15 done')


def ensure_news_impact_stats(cur):
    '''Aggregated news impact statistics per event_type/region/regime.'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.news_impact_stats (
            id SERIAL PRIMARY KEY,
            event_type VARCHAR(50) NOT NULL,
            region VARCHAR(20) DEFAULT 'GLOBAL',
            regime VARCHAR(20) DEFAULT 'NORMAL',
            avg_ret_2h FLOAT,
            med_ret_2h FLOAT,
            std_ret_2h FLOAT,
            avg_abs_ret_2h FLOAT,
            sample_count INT DEFAULT 0,
            last_updated TIMESTAMP DEFAULT NOW(),
            stats_version VARCHAR(30),
            UNIQUE(event_type, region, regime)
        );
    """)
    _log('ensure_news_impact_stats done')


def ensure_exchange_policy_audit(cur):
    '''Exchange policy audit results — 10-day compliance review.'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.exchange_policy_audit (
            id                          BIGSERIAL PRIMARY KEY,
            ts                          TIMESTAMPTZ NOT NULL DEFAULT now(),
            audit_period_start          TIMESTAMPTZ,
            audit_period_end            TIMESTAMPTZ,
            total_orders                INTEGER NOT NULL DEFAULT 0,
            total_rejections            INTEGER NOT NULL DEFAULT 0,
            rejection_rate              NUMERIC NOT NULL DEFAULT 0,
            top_errors                  JSONB NOT NULL DEFAULT '[]'::jsonb,
            rate_limit_events           INTEGER NOT NULL DEFAULT 0,
            mode_mismatch_events        INTEGER NOT NULL DEFAULT 0,
            protection_mode_activations INTEGER NOT NULL DEFAULT 0,
            report_text                 TEXT,
            detail                      JSONB NOT NULL DEFAULT '{}'::jsonb,
            symbol                      TEXT NOT NULL DEFAULT 'BTC/USDT:USDT',
            audit_type                  TEXT NOT NULL DEFAULT 'periodic_audit',
            metadata                    JSONB
        );
    """)
    # Add columns for exchange_audit.py (symbol, audit_type, metadata)
    for col, dtype in (('symbol', "TEXT NOT NULL DEFAULT 'BTC/USDT:USDT'"),
                       ('audit_type', "TEXT NOT NULL DEFAULT 'periodic_audit'"),
                       ('metadata', 'JSONB')):
        cur.execute(f"""
            ALTER TABLE exchange_policy_audit
                ADD COLUMN IF NOT EXISTS {col} {dtype};
        """)
    # Make audit_period_start/end nullable for exchange_audit.py rows
    cur.execute("""
        ALTER TABLE exchange_policy_audit
            ALTER COLUMN audit_period_start DROP NOT NULL;
    """)
    cur.execute("""
        ALTER TABLE exchange_policy_audit
            ALTER COLUMN audit_period_end DROP NOT NULL;
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_epa_ts ON exchange_policy_audit(ts);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_epa_symbol ON exchange_policy_audit(symbol);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_epa_audit_type ON exchange_policy_audit(audit_type);')
    _log('ensure_exchange_policy_audit done')


def ensure_execution_log_audit_columns(cur):
    '''Add error_code and error_message columns to execution_log for exchange_audit.py.'''
    for col, dtype in (('error_code', 'TEXT'),
                       ('error_message', 'TEXT')):
        cur.execute(f"""
            ALTER TABLE execution_log
                ADD COLUMN IF NOT EXISTS {col} {dtype};
        """)
    _log('ensure_execution_log_audit_columns done')


def cleanup_old_data(cur):
    '''Retention policy: prune old data from large tables.

    - pm_decision_log: keep 90 days
    - score_history: keep 60 days
    - event_trigger_log: keep 30 days
    - claude_call_log: keep 60 days

    NOTE: market_ohlcv and candles are EXCLUDED from cleanup.
    Historical price data is a core asset for backtest/indicators.
    Safe to call repeatedly. Uses DELETE with LIMIT to avoid long locks.
    '''
    policies = [
        ('pm_decision_log', 'ts', 90),
        # market_ohlcv removed — historical price data preserved for backtest
        ('score_history', 'ts', 60),
        ('event_trigger_log', 'ts', 30),
        ('claude_call_log', 'ts', 60),
    ]
    for table, ts_col, days in policies:
        try:
            total_deleted = 0
            while True:
                cur.execute(f"""
                    DELETE FROM {table} WHERE ctid IN (
                        SELECT ctid FROM {table}
                        WHERE {ts_col} < now() - interval '{days} days'
                        LIMIT 5000
                    );
                """)
                batch_del = cur.rowcount
                total_deleted += batch_del
                if batch_del < 5000:
                    break
            if total_deleted > 0:
                _log(f'cleanup {table}: deleted {total_deleted} rows (>{days}d)')
        except Exception as e:
            _log(f'cleanup {table} skip: {e}')


def ensure_macro_events():
    """macro_events 테이블 생성 — 거시경제 이벤트 이력."""
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS macro_events (
                    id SERIAL PRIMARY KEY,
                    event_type VARCHAR(20) NOT NULL,
                    event_date DATE NOT NULL,
                    actual_value NUMERIC,
                    expected_value NUMERIC,
                    previous_value NUMERIC,
                    impact_direction VARCHAR(10),
                    btc_ret_1h NUMERIC,
                    btc_ret_4h NUMERIC,
                    btc_ret_24h NUMERIC,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    UNIQUE(event_type, event_date)
                );
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_macro_events_type_date
                ON macro_events(event_type, event_date DESC);
            """)
            conn.commit()
            _log('ensure_macro_events: OK')
    except Exception as e:
        conn.rollback()
        _log(f'ensure_macro_events: {e}')
    finally:
        conn.close()


def ensure_news_relevance_verified():
    """news 테이블에 relevance_verified 컬럼 추가."""
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE news ADD COLUMN IF NOT EXISTS relevance_verified BOOLEAN DEFAULT NULL;
            """)
            conn.commit()
            _log('ensure_news_relevance_verified: OK')
    except Exception as e:
        conn.rollback()
        _log(f'ensure_news_relevance_verified: {e}')
    finally:
        conn.close()


def ensure_news_tier_columns(cur):
    """news 테이블에 tier/relevance_score/source_tier/exclusion_reason/direction_hit 컬럼 추가."""
    for col, dtype in (('tier', "TEXT DEFAULT 'UNKNOWN'"),
                       ('relevance_score', 'NUMERIC DEFAULT NULL'),
                       ('source_tier', 'TEXT DEFAULT NULL'),
                       ('exclusion_reason', 'TEXT DEFAULT NULL'),
                       ('direction_hit', 'BOOLEAN DEFAULT NULL')):
        cur.execute(f"""
            ALTER TABLE news ADD COLUMN IF NOT EXISTS {col} {dtype};
        """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_news_tier_ts ON news(tier, ts DESC);')
    _log('ensure_news_tier_columns done')


def ensure_news_source_accuracy(cur):
    """news_source_accuracy 테이블 — 소스별 방향 예측 적중률."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.news_source_accuracy (
            id SERIAL PRIMARY KEY,
            source TEXT NOT NULL,
            category TEXT DEFAULT 'ALL',
            total_predictions INT DEFAULT 0,
            correct_predictions INT DEFAULT 0,
            hit_rate NUMERIC DEFAULT 0.0,
            last_updated TIMESTAMP DEFAULT NOW(),
            UNIQUE(source, category)
        );
    """)
    _log('ensure_news_source_accuracy done')


def ensure_error_log(cur):
    """에러 로그 테이블 (scheduled_liquidation CRITICAL 등)."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS error_log (
            id SERIAL PRIMARY KEY,
            ts TIMESTAMPTZ NOT NULL DEFAULT now(),
            service VARCHAR(50),
            level VARCHAR(20) DEFAULT 'ERROR',
            message TEXT
        );
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_error_log_ts
        ON error_log(ts DESC);
    """)
    _log('ensure_error_log done')


def ensure_service_health_log(cur):
    """서비스 헬스체크 3-state 로깅 테이블."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS service_health_log (
            id SERIAL PRIMARY KEY,
            ts TIMESTAMPTZ NOT NULL DEFAULT now(),
            service VARCHAR(50) NOT NULL,
            state VARCHAR(10) NOT NULL CHECK (state IN ('OK','DOWN','UNKNOWN')),
            detail TEXT
        );
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_shl_svc_ts
        ON service_health_log(service, ts DESC);
    """)
    _log('ensure_service_health_log done')


def ensure_news_topic_columns(cur):
    """news 테이블에 topic_class, asset_relevance 컬럼 추가."""
    cur.execute("""
        ALTER TABLE news ADD COLUMN IF NOT EXISTS topic_class VARCHAR(20);
    """)
    cur.execute("""
        ALTER TABLE news ADD COLUMN IF NOT EXISTS asset_relevance VARCHAR(20);
    """)
    _log('ensure_news_topic_columns done')


def ensure_macro_trace_qqq_columns(cur):
    """macro_trace 테이블에 QQQ 수익률 컬럼 추가."""
    cur.execute("""
        ALTER TABLE macro_trace ADD COLUMN IF NOT EXISTS qqq_ret_2h FLOAT;
    """)
    cur.execute("""
        ALTER TABLE macro_trace ADD COLUMN IF NOT EXISTS qqq_ret_24h FLOAT;
    """)
    _log('ensure_macro_trace_qqq_columns done')


def ensure_backfill_job_runs(cur):
    """backfill_job_runs 테이블 — 백필 작업 이력 추적."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.backfill_job_runs (
            id           BIGSERIAL PRIMARY KEY,
            job_name     TEXT NOT NULL,
            started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
            finished_at  TIMESTAMPTZ,
            status       TEXT NOT NULL DEFAULT 'RUNNING',
            inserted     INTEGER NOT NULL DEFAULT 0,
            updated      INTEGER NOT NULL DEFAULT 0,
            failed       INTEGER NOT NULL DEFAULT 0,
            last_cursor  TEXT,
            error        TEXT,
            metadata     JSONB NOT NULL DEFAULT '{}'::jsonb
        );
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_bjr_job ON backfill_job_runs(job_name, started_at DESC);')
    _log('ensure_backfill_job_runs done')


def ensure_backfill_job_ack_columns(cur):
    """backfill_job_runs에 ack 컬럼 추가 — FAILED job acknowledge 지원."""
    cur.execute("ALTER TABLE backfill_job_runs ADD COLUMN IF NOT EXISTS acked_at TIMESTAMPTZ;")
    cur.execute("ALTER TABLE backfill_job_runs ADD COLUMN IF NOT EXISTS acked_by TEXT;")
    _log('ensure_backfill_job_ack_columns done')


def ensure_once_lock_ttl(cur):
    """once_lock에 expires_at 컬럼 추가 — TTL 기반 자동 만료."""
    cur.execute("ALTER TABLE live_order_once_lock ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;")
    _log('ensure_once_lock_ttl done')


def ensure_price_events(cur):
    """price_events 테이블 — 가격 이벤트 감지 결과."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.price_events (
            id               BIGSERIAL PRIMARY KEY,
            event_id         TEXT NOT NULL UNIQUE,
            symbol           TEXT NOT NULL DEFAULT 'BTC/USDT:USDT',
            start_ts         TIMESTAMPTZ NOT NULL,
            end_ts           TIMESTAMPTZ,
            direction        SMALLINT NOT NULL,
            move_pct         NUMERIC NOT NULL,
            trigger_type     TEXT NOT NULL,
            max_runup        NUMERIC,
            max_drawdown     NUMERIC,
            vol_spike_z      NUMERIC,
            atr_z            NUMERIC,
            regime_context   TEXT,
            btc_price_at     NUMERIC,
            ret_1h           NUMERIC,
            ret_4h           NUMERIC,
            ret_24h          NUMERIC,
            metadata         JSONB DEFAULT '{}'::jsonb,
            created_at       TIMESTAMPTZ DEFAULT now()
        );
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_pe_start_ts ON price_events(start_ts DESC);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_pe_trigger ON price_events(trigger_type);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_pe_direction ON price_events(direction);')
    _log('ensure_price_events done')


def ensure_event_news_link(cur):
    """event_news_link 테이블 — price_events↔news 연결."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.event_news_link (
            id               BIGSERIAL PRIMARY KEY,
            event_id         TEXT NOT NULL,
            news_id          BIGINT NOT NULL,
            time_lag_minutes NUMERIC NOT NULL,
            match_score      NUMERIC DEFAULT 0,
            reason           TEXT,
            UNIQUE(event_id, news_id)
        );
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_enl_event ON event_news_link(event_id);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_enl_news ON event_news_link(news_id);')
    _log('ensure_event_news_link done')


def ensure_alert_dedup_state(cur):
    '''DB-based cross-process alert dedup state.'''
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.alert_dedup_state (
            key              TEXT PRIMARY KEY,
            first_seen_ts    TIMESTAMPTZ NOT NULL DEFAULT now(),
            last_seen_ts     TIMESTAMPTZ NOT NULL DEFAULT now(),
            last_sent_ts     TIMESTAMPTZ,
            suppressed_count INTEGER NOT NULL DEFAULT 0,
            last_payload_hash TEXT,
            prev_state       TEXT
        );
    """)
    _log('ensure_alert_dedup_state done')


def ensure_candles_retention_policy(cur):
    """Index for efficient 1m candle pruning."""
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_candles_tf_ts
        ON candles(tf, ts);
    """)
    _log('ensure_candles_retention_policy done')


def ensure_news_reaction_direction_columns(cur):
    """Add price_source_tf, dir_30m, dir_24h columns to news_market_reaction."""
    for col, dtype in (
        ('price_source_tf', 'TEXT'),
        ('dir_30m', 'TEXT'),
        ('dir_24h', 'TEXT'),
    ):
        cur.execute(f"""
            ALTER TABLE news_market_reaction
                ADD COLUMN IF NOT EXISTS {col} {dtype};
        """)
    _log('ensure_news_reaction_direction_columns done')


def ensure_news_price_path(cur):
    """news_price_path 테이블 — 뉴스 후 24h 가격 경로 분석."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.news_price_path (
            id               BIGSERIAL PRIMARY KEY,
            news_id          BIGINT NOT NULL,
            ts_news          TIMESTAMPTZ NOT NULL,
            btc_price_at     NUMERIC,
            price_source_tf  TEXT,
            max_drawdown_24h NUMERIC,
            max_runup_24h    NUMERIC,
            drawdown_ts      TIMESTAMPTZ,
            runup_ts         TIMESTAMPTZ,
            recovery_minutes INTEGER,
            end_price_24h    NUMERIC,
            end_ret_24h      NUMERIC,
            end_state_24h    TEXT,
            path_shape       TEXT,
            computed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE(news_id)
        );
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_npp_ts ON news_price_path(ts_news DESC);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_npp_end_state ON news_price_path(end_state_24h);')
    _log('ensure_news_price_path done')


def ensure_news_price_path_v2(cur):
    """news_price_path v2 — 7분류 path_class + 방향 컬럼 추가."""
    cur.execute("ALTER TABLE news_price_path ADD COLUMN IF NOT EXISTS path_class TEXT;")
    cur.execute("ALTER TABLE news_price_path ADD COLUMN IF NOT EXISTS initial_move_dir TEXT;")
    cur.execute("ALTER TABLE news_price_path ADD COLUMN IF NOT EXISTS follow_through_dir TEXT;")
    cur.execute("ALTER TABLE news_price_path ADD COLUMN IF NOT EXISTS recovered_flag BOOLEAN;")
    cur.execute("ALTER TABLE news_price_path ADD COLUMN IF NOT EXISTS further_drop_flag BOOLEAN;")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_npp_path_class ON news_price_path(path_class);")
    _log('ensure_news_price_path_v2 done')


def ensure_chat_memory(cur):
    """대화 기록 테이블 — GPT ChatAgent 대화 히스토리."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.chat_memory (
            id        BIGSERIAL PRIMARY KEY,
            chat_id   BIGINT NOT NULL,
            role      TEXT NOT NULL,
            content   TEXT NOT NULL,
            tool_name TEXT,
            metadata  JSONB DEFAULT '{}',
            ts        TIMESTAMPTZ DEFAULT now()
        );
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_chatmem_chat_ts ON chat_memory(chat_id, ts DESC);')
    _log('ensure_chat_memory done')


def ensure_trade_arm_state(cur):
    """무장 상태 테이블 — 매매 활성화 전 확인."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.trade_arm_state (
            id          BIGSERIAL PRIMARY KEY,
            chat_id     BIGINT NOT NULL,
            armed       BOOLEAN NOT NULL DEFAULT false,
            armed_at    TIMESTAMPTZ,
            expires_at  TIMESTAMPTZ,
            disarmed_at TIMESTAMPTZ
        );
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_arm_chat ON trade_arm_state(chat_id);')
    _log('ensure_trade_arm_state done')


def ensure_auto_apply_config(cur):
    """AUTO APPLY 설정 + 리스크 정책."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.auto_apply_config (
            id                       BIGSERIAL PRIMARY KEY,
            auto_apply_on_claude     BOOLEAN DEFAULT false,
            auto_apply_on_emergency  BOOLEAN DEFAULT false,
            max_notional_usdt        NUMERIC DEFAULT 500,
            max_leverage             INTEGER DEFAULT 5,
            sl_min_pct               NUMERIC DEFAULT 1.0,
            sl_max_pct               NUMERIC DEFAULT 4.0,
            cooldown_sec             INTEGER DEFAULT 300,
            updated_at               TIMESTAMPTZ DEFAULT now()
        );
    """)
    cur.execute('SELECT count(*) FROM public.auto_apply_config;')
    cnt = cur.fetchone()[0]
    if cnt == 0:
        cur.execute('INSERT INTO public.auto_apply_config DEFAULT VALUES;')
    _log('ensure_auto_apply_config done')


def ensure_claude_trade_decision_log(cur):
    """Claude 매매 결정 감사 로그."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.claude_trade_decision_log (
            id                  BIGSERIAL PRIMARY KEY,
            ts                  TIMESTAMPTZ DEFAULT now(),
            trace_id            TEXT,
            provider            TEXT NOT NULL,
            model               TEXT,
            trade_action        JSONB DEFAULT '{}',
            applied             BOOLEAN DEFAULT false,
            blocked_reason      TEXT,
            execution_queue_id  BIGINT,
            arm_state           JSONB,
            risk_check_result   JSONB
        );
    """)
    cur.execute('CREATE INDEX IF NOT EXISTS idx_ctdl_ts ON claude_trade_decision_log(ts DESC);')
    _log('ensure_claude_trade_decision_log done')


def ensure_news_market_reaction_extended(cur):
    """news_market_reaction 확장 컬럼 추가 — ret_30m, ret_2h, vol 등."""
    for col, dtype in (('ret_30m', 'NUMERIC'),
                       ('ret_2h', 'NUMERIC'),
                       ('vol_30m', 'NUMERIC'),
                       ('vol_2h', 'NUMERIC'),
                       ('vol_24h', 'NUMERIC'),
                       ('direction_2h', 'TEXT'),
                       ('abs_move_2h', 'NUMERIC'),
                       ('status', "TEXT DEFAULT 'computed'")):
        cur.execute(f"""
            ALTER TABLE news_market_reaction
                ADD COLUMN IF NOT EXISTS {col} {dtype};
        """)
    _log('ensure_news_market_reaction_extended done')


def ensure_news_impact_stats_extended(cur):
    """news_impact_stats 확장 컬럼 추가."""
    cur.execute("""
        ALTER TABLE news_impact_stats ADD COLUMN IF NOT EXISTS avg_ret_30m FLOAT;
    """)
    cur.execute("""
        ALTER TABLE news_impact_stats ADD COLUMN IF NOT EXISTS avg_abs_ret_24h FLOAT;
    """)
    cur.execute("""
        ALTER TABLE news_impact_stats ADD COLUMN IF NOT EXISTS direction_accuracy FLOAT;
    """)
    _log('ensure_news_impact_stats_extended done')


def run_all():
    '''Run all migrations. Safe to call multiple times.'''
    conn = None
    try:
        conn = _db_conn()
        conn.autocommit = True
        with conn.cursor() as cur:
            ensure_trade_process_log(cur)
            ensure_autopilot_config(cur)
            ensure_stage_column(cur)
            ensure_execution_log(cur)
            ensure_execution_queue(cur)
            ensure_pm_decision_log(cur)
            ensure_position_state(cur)
            ensure_emergency_analysis_log(cur)
            ensure_safety_limits(cur)
            ensure_execution_log_pm_columns(cur)
            ensure_indicator_columns(cur)
            ensure_vol_profile_columns(cur)
            ensure_trade_process_log_add_columns(cur)
            ensure_trade_process_log_budget_columns(cur)
            ensure_position_state_budget_columns(cur)
            ensure_safety_limits_add_threshold(cur)
            ensure_safety_limits_budget_columns(cur)
            ensure_safety_limits_stoploss_column(cur)
            ensure_pending_add_slices(cur)
            ensure_test_events_log(cur)
            ensure_event_history(cur)
            ensure_liquidity_snapshots(cur)
            ensure_macro_data(cur)
            ensure_score_history(cur)
            ensure_score_weights(cur)
            ensure_safety_limits_dynamic_sl_columns(cur)
            # FACT pipeline tables
            ensure_market_ohlcv(cur)
            ensure_news_market_reaction(cur)
            ensure_events(cur)
            ensure_event_news(cur)
            ensure_claude_analyses(cur)
            ensure_analysis_outcomes(cur)
            # Price-event strategy
            ensure_price_event_stats(cur)
            ensure_score_weights_v2(cur)
            # OpenClo tables
            ensure_openclo_tables(cur)
            # Claude gate columns
            ensure_claude_gate_columns(cur)
            # OpenClaw directive tables
            ensure_openclaw_directives(cur)
            ensure_openclaw_policies(cur)
            # Provider tracking
            ensure_pm_decision_log_model_used(cur)
            ensure_claude_analyses_model_provider(cur)
            # Event trigger log
            ensure_event_trigger_log(cur)
            # Macro trace for news→strategy pipeline
            ensure_macro_trace(cur)
            # Exchange compliance layer
            ensure_compliance_log(cur)
            ensure_execution_queue_compliance_columns(cur)
            # News title_ko for Korean translation
            ensure_news_title_ko(cur)
            # Exchange policy audit + execution_log audit columns
            ensure_exchange_policy_audit(cur)
            ensure_execution_log_audit_columns(cur)
            # DB-based event dedup lock (replaces in-memory dedup)
            ensure_event_lock(cur)
            ensure_hold_consecutive(cur)
            ensure_claude_call_log(cur)
            # Extended pm_decision_log columns
            ensure_pm_decision_log_extended(cur)
            # News impact stats table
            ensure_news_impact_stats(cur)
            # Hourly trade limit 8→15
            ensure_safety_limits_hourly_15(cur)
            # Macro events table (FRED/FOMC history)
            ensure_macro_events()
            # News relevance_verified column
            ensure_news_relevance_verified()
            # News tier columns + source accuracy table
            ensure_news_tier_columns(cur)
            ensure_news_source_accuracy(cur)
            # Data retention cleanup
            cleanup_old_data(cur)
            # Error log table
            ensure_error_log(cur)
            # Phase 2: Service health log
            ensure_service_health_log(cur)
            # Phase 3: News topic classification columns
            ensure_news_topic_columns(cur)
            # Phase 4: Macro trace QQQ columns
            ensure_macro_trace_qqq_columns(cur)
            # Phase 5: News impact stats extended columns
            ensure_news_impact_stats_extended(cur)
            # Phase 6: Backfill infrastructure tables
            ensure_backfill_job_runs(cur)
            ensure_backfill_job_ack_columns(cur)
            ensure_price_events(cur)
            ensure_event_news_link(cur)
            ensure_news_market_reaction_extended(cur)
            # Alert dedup state (cross-process)
            ensure_alert_dedup_state(cur)
            # Hybrid data retention + news path analysis
            ensure_candles_retention_policy(cur)
            ensure_news_reaction_direction_columns(cur)
            ensure_news_price_path(cur)
            ensure_news_price_path_v2(cur)
            # ChatAgent + Auto-Apply tables
            ensure_chat_memory(cur)
            ensure_trade_arm_state(cur)
            ensure_auto_apply_config(cur)
            ensure_claude_trade_decision_log(cur)
            ensure_candles_data_source(cur)
            # Data integrity audit table (gap detection)
            ensure_data_integrity_audit(cur)
            # News v2 filter: allow_storage + allow_trading columns
            ensure_news_allow_columns(cur)
            # Once lock TTL column
            ensure_once_lock_ttl(cur)
            # Daily loss limit -150 → -45 (900 USDT * 5%)
            ensure_safety_limits_daily_loss_45(cur)
            # Position state v2: order_state + planned/filled tracking
            ensure_position_state_v2_columns(cur)
            # News trading_impact_weight column
            ensure_news_trading_impact_weight(cur)
            # Phase: EMA/VWAP indicators + score_weights v3
            ensure_indicator_ema_vwap_columns(cur)
            ensure_score_weights_v3(cur)
            ensure_score_weights_v4(cur)
            # Phase B: Dynamic leverage columns
            ensure_safety_limits_leverage_columns(cur)
            # trade_switch auto-recovery columns
            ensure_trade_switch_recovery_columns(cur)
            # trade_switch updated_at defensive migration
            ensure_trade_switch_updated_at(cur)
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


def ensure_position_state_v2_columns(cur):
    """Add order_state + planned/filled tracking columns to position_state."""
    for col, dtype in (
        ('order_state', "TEXT DEFAULT 'NONE'"),
        ('planned_qty', 'NUMERIC DEFAULT 0'),
        ('filled_qty', 'NUMERIC DEFAULT 0'),
        ('planned_usdt', 'NUMERIC DEFAULT 0'),
        ('sent_usdt', 'NUMERIC DEFAULT 0'),
        ('filled_usdt', 'NUMERIC DEFAULT 0'),
        ('last_order_id', 'TEXT'),
        ('last_order_ts', 'TIMESTAMPTZ'),
        ('state_changed_at', 'TIMESTAMPTZ DEFAULT now()'),
    ):
        cur.execute(f"""
            ALTER TABLE position_state
                ADD COLUMN IF NOT EXISTS {col} {dtype};
        """)
    _log('ensure_position_state_v2_columns done')


def ensure_news_trading_impact_weight(cur):
    """Add trading_impact_weight column to news table."""
    cur.execute("""
        ALTER TABLE news ADD COLUMN IF NOT EXISTS trading_impact_weight NUMERIC DEFAULT 0;
    """)
    _log('ensure_news_trading_impact_weight done')


def ensure_candles_data_source(cur):
    """Add data_source column to candles + market_ohlcv for multi-source tracking."""
    cur.execute("ALTER TABLE candles ADD COLUMN IF NOT EXISTS data_source TEXT DEFAULT 'bybit';")
    cur.execute("ALTER TABLE market_ohlcv ADD COLUMN IF NOT EXISTS data_source TEXT DEFAULT 'bybit';")
    _log('ensure_candles_data_source done')


def ensure_news_allow_columns(cur):
    """Add allow_storage, allow_trading boolean columns to news table (v2 filter)."""
    cur.execute("ALTER TABLE news ADD COLUMN IF NOT EXISTS allow_storage BOOLEAN;")
    cur.execute("ALTER TABLE news ADD COLUMN IF NOT EXISTS allow_trading BOOLEAN;")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_news_allow_trading ON news(allow_trading) WHERE allow_trading = true;")
    _log('ensure_news_allow_columns done')


def ensure_safety_limits_daily_loss_45(cur):
    """Update daily_loss_limit_usdt from -150 to -45 (900 USDT * 5%)."""
    cur.execute("""
        UPDATE safety_limits SET daily_loss_limit_usdt = -45
        WHERE daily_loss_limit_usdt = -150;
    """)
    _log('ensure_safety_limits_daily_loss_45 done')


def ensure_indicator_ema_vwap_columns(cur):
    """Add EMA(9/21/50) and VWAP columns to indicators table."""
    for col, dtype in (('ema_9', 'NUMERIC'), ('ema_21', 'NUMERIC'),
                       ('ema_50', 'NUMERIC'), ('vwap', 'NUMERIC')):
        cur.execute(f"""
            ALTER TABLE indicators
                ADD COLUMN IF NOT EXISTS {col} {dtype};
        """)
    _log('ensure_indicator_ema_vwap_columns done')


def ensure_score_weights_v3(cur):
    """Update score_weights to v3: tech=0.60, position=0.15, regime=0.20, news_event=0.05."""
    cur.execute("""
        UPDATE score_weights
        SET tech_w = 0.60, position_w = 0.15, regime_w = 0.20, news_event_w = 0.05
        WHERE id = (SELECT MAX(id) FROM score_weights) AND tech_w = 0.45;
    """)
    _log('ensure_score_weights_v3 done')


def ensure_score_weights_v4(cur):
    """Update score_weights to v4: tech=0.75, position=0.10, regime=0.10, news_event=0.05."""
    cur.execute("""
        UPDATE score_weights
        SET tech_w = 0.75, position_w = 0.10, regime_w = 0.10, news_event_w = 0.05
        WHERE id = (SELECT MAX(id) FROM score_weights) AND tech_w = 0.60;
    """)
    _log('ensure_score_weights_v4 done')


def ensure_trade_switch_recovery_columns(cur):
    """Add off_reason, manual_off_until columns to trade_switch for auto-recovery."""
    cur.execute("""
        ALTER TABLE trade_switch ADD COLUMN IF NOT EXISTS off_reason TEXT DEFAULT NULL;
    """)
    cur.execute("""
        ALTER TABLE trade_switch ADD COLUMN IF NOT EXISTS manual_off_until TIMESTAMPTZ DEFAULT NULL;
    """)
    _log('ensure_trade_switch_recovery_columns done')


def ensure_safety_limits_leverage_columns(cur):
    """Add leverage_min, leverage_max, leverage_high_stage_max columns to safety_limits."""
    for col, dtype in (('leverage_min', 'INT DEFAULT 3'),
                       ('leverage_max', 'INT DEFAULT 8'),
                       ('leverage_high_stage_max', 'INT DEFAULT 5')):
        cur.execute(f"""
            ALTER TABLE safety_limits
                ADD COLUMN IF NOT EXISTS {col} {dtype};
        """)
    _log('ensure_safety_limits_leverage_columns done')


def ensure_trade_switch_updated_at(cur):
    """Defensive: ensure trade_switch has updated_at column for existing tables."""
    cur.execute("""
        ALTER TABLE trade_switch ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();
    """)
    _log('ensure_trade_switch_updated_at done')


def ensure_data_integrity_audit(cur):
    """data_integrity_audit table — monthly data coverage tracking + gap detection."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.data_integrity_audit (
            id          BIGSERIAL PRIMARY KEY,
            table_name  TEXT NOT NULL,
            month       TEXT NOT NULL,
            row_count   BIGINT NOT NULL DEFAULT 0,
            gap_flag    BOOLEAN NOT NULL DEFAULT false,
            checked_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (table_name, month)
        );
    """)
    _log('ensure_data_integrity_audit done')


if __name__ == '__main__':
    run_all()
