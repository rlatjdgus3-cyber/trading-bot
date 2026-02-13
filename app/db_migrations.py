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
    import psycopg2
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=int(os.getenv('DB_PORT', '5432')),
        dbname=os.getenv('DB_NAME', 'trading'),
        user=os.getenv('DB_USER', 'bot'),
        password=os.getenv('DB_PASS', 'botpass'),
        connect_timeout=10,
        options='-c statement_timeout=30000')


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
            daily_loss_limit_usdt NUMERIC NOT NULL DEFAULT -150,
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
                        ('last_reason', 'TEXT'), ('last_score', 'NUMERIC')):
        cur.execute(f"""
            ALTER TABLE position_state
                ADD COLUMN IF NOT EXISTS {col} {dtype};
        """)
    _log('ensure_position_state_budget_columns done')


def ensure_safety_limits_add_threshold(cur):
    '''Add add_score_threshold column to safety_limits.'''
    cur.execute("""
        ALTER TABLE safety_limits
            ADD COLUMN IF NOT EXISTS add_score_threshold INTEGER NOT NULL DEFAULT 65;
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
