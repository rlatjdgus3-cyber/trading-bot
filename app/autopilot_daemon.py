"""
autopilot_daemon.py — Autonomous trading daemon.

20-second loop:
  1. Check autopilot_config.enabled
  2. Risk checks (trade_switch, LIVE_TRADING, once_lock, daily limit, cooldown, position)
  3. Run direction_scorer.compute_scores()
  4. If confidence >= MIN_CONFIDENCE (35), create OPEN signal (v3: +cooldown+stage1)
  5. Log to trade_process_log (source='autopilot')
  6. Telegram notification
"""
import os
import sys
import time
import json
import traceback
import urllib.parse
import urllib.request
sys.path.insert(0, '/root/trading-bot/app')

# ── Strategy v2 feature flag ──
# 'off': use only old logic
# 'shadow': run new logic in parallel, log decisions but don't execute
# 'on': use new logic for entry/add decisions
STRATEGY_V2_ENABLED = os.getenv('STRATEGY_V2_ENABLED', 'shadow')
_v2_migrations_done = False

LOG_PREFIX = '[autopilot]'
SYMBOL = 'BTC/USDT:USDT'
ALLOWED_SYMBOLS = frozenset({"BTC/USDT:USDT"})
POLL_SEC = 20
COOLDOWN_SEC = 30  # v2.1 중간 공격형
MAX_DAILY_TRADES = 60
MIN_CONFIDENCE = 35  # v3: conf>=35 진입 허용 (35-49: stage1 only, >=50: 기존 로직)
CONF_ADD_THRESHOLD = 50  # conf>=50 이어야 ADD 허용
DEFAULT_SIZE_PCT = 10
REPEAT_SIGNAL_COOLDOWN_SEC = 900  # [0-3] 동일 방향 재신호 쿨다운 15분
STOP_LOSS_COOLDOWN_WINDOW_SEC = 3600  # 손절 감시 윈도우 60분
STOP_LOSS_COOLDOWN_TRIGGER = 2  # 연속 손절 N회 이상이면 쿨다운
STOP_LOSS_COOLDOWN_PAUSE_SEC = 1800  # 손절 쿨다운 30분
KILL_SWITCH_PATH = '/root/trading-bot/app/KILL_SWITCH'
TRADE_SWITCH_DEDUP_KEY = 'autopilot:risk_check:trade_switch_off'
LOG_COOLDOWN_SEC = 1800  # 30min log dedup (non-trade_switch reasons)
_migrations_done = False
_exchange = None
_last_add_price = 0.0           # last ADD attempt price
_last_add_ts = 0.0
_add_dedup_initialized = False
ADD_PRICE_DEDUP_PCT = 0.1       # ±0.1% range
ADD_PRICE_DEDUP_SEC = 600       # 10min window

# [0-3] Price reentry validation state
_last_signal_price = {}  # (symbol, side) -> last trigger price

# ── Strategy V3 state ──
_v3_result = None  # last V3 cycle result (for signal metadata)
_v3_features = None  # last V3 feature snapshot (reused within cycle)


def _is_v3_enabled():
    """Check if Strategy V3 is enabled. FAIL-OPEN: returns False."""
    try:
        from strategy_v3.config_v3 import is_enabled
        return is_enabled()
    except Exception:
        return False


def _v3_check_sl_cooldown(cur, symbol, direction):
    """V3 SL cooldown: 1 SL → 10min same-direction ban (complementary to existing 2-SL cooldown).

    Returns (ok, reason).
    FAIL-OPEN: returns (True, '').
    """
    try:
        from strategy_v3.config_v3 import get as v3_get
        cooldown_sec = v3_get('cooldown_after_stop_sec', 600)

        from strategy_v3.regime_v3 import get_sl_cooldown_info
        last_sl_ts, last_sl_dir = get_sl_cooldown_info()

        if last_sl_ts > 0 and last_sl_dir:
            elapsed = time.time() - last_sl_ts
            if elapsed < cooldown_sec and last_sl_dir == direction:
                remaining = int(cooldown_sec - elapsed)
                return (False, f'V3 cooldown_after_stop: {direction} blocked for {remaining}s')
        # Also check DB for recent SL events (covers bot restarts)
        cur.execute("""
            SELECT direction, extract(epoch from last_fill_at) as ts
            FROM execution_log
            WHERE symbol = %s AND status = 'FILLED'
              AND order_type IN ('CLOSE', 'FULL_CLOSE')
              AND realized_pnl < 0
              AND last_fill_at >= now() - make_interval(secs => %s)
            ORDER BY last_fill_at DESC LIMIT 1;
        """, (symbol, cooldown_sec))
        row = cur.fetchone()
        if row and row[0]:
            sl_dir = 'LONG' if row[0].lower() == 'long' else 'SHORT'
            if sl_dir == direction:
                elapsed = time.time() - float(row[1])
                if elapsed < cooldown_sec:
                    remaining = int(cooldown_sec - elapsed)
                    return (False, f'V3 cooldown_after_stop(DB): {direction} blocked for {remaining}s')
    except Exception as e:
        _log(f'V3 SL cooldown check FAIL-OPEN: {e}')
    return (True, '')


def _v3_check_signal_debounce(cur, symbol, v3_regime, direction, features):
    """V3 signal debounce: side+regime+level_bucket → window check.

    Returns (ok, reason).
    FAIL-OPEN: returns (True, '').
    """
    try:
        from strategy_v3.config_v3 import get as v3_get
        from strategy.common.dedupe import is_duplicate, make_v3_signal_key

        debounce_sec = v3_get('signal_debounce_sec', 300)
        regime_class = v3_regime.get('regime_class', 'UNKNOWN')

        # Determine level bucket
        range_position = features.get('range_position') if features else None
        if regime_class == 'BREAKOUT':
            price = features.get('price', 0) if features else 0
            vah = features.get('vah', 0) if features else 0
            val = features.get('val', 0) if features else 0
            if price and vah and price > vah:
                level_bucket = 'BREAKOUT_UP'
            elif price and val and price < val:
                level_bucket = 'BREAKOUT_DOWN'
            else:
                level_bucket = 'MID'
        elif range_position is not None:
            if range_position <= 0.20:
                level_bucket = 'VAL'
            elif range_position >= 0.80:
                level_bucket = 'VAH'
            elif 0.40 <= range_position <= 0.60:
                level_bucket = 'POC'
            else:
                level_bucket = 'MID'
        else:
            level_bucket = 'MID'

        key = make_v3_signal_key(symbol, regime_class, direction, level_bucket)
        if is_duplicate(cur, key, window_sec=debounce_sec):
            return (False, f'V3 debounce: {key} within {debounce_sec}s')
        return (True, '')
    except Exception as e:
        _log(f'V3 debounce check FAIL-OPEN: {e}')
        return (True, '')


def _is_reentry_valid(symbol, side, current_price):
    """[0-3] Price reentry dedup: block re-entry if price is still near
    the last signal price (within 0.1%). Only allow if price moved
    significantly away, indicating a fresh signal.
    Returns (valid, reason)."""
    key = (symbol, side)
    if key not in _last_signal_price:
        return (True, 'first signal')
    last_price = _last_signal_price[key]
    if last_price <= 0:
        return (True, 'no previous price')
    # Calculate threshold: 0.1% from last signal price
    threshold = last_price * 0.001
    if side == 'LONG':
        # LONG: price must have moved above last signal + 0.1% (new breakout level)
        if current_price > last_price + threshold:
            return (True, f'price moved above {last_price:.1f}+{threshold:.1f}')
        return (False, f'reentry blocked: price {current_price:.1f} near last signal {last_price:.1f}')
    else:  # SHORT
        if current_price < last_price - threshold:
            return (True, f'price moved below {last_price:.1f}-{threshold:.1f}')
        return (False, f'reentry blocked: price {current_price:.1f} near last signal {last_price:.1f}')


def _record_signal_price(symbol, side, price):
    """[0-3] Record the price at which signal was generated."""
    _last_signal_price[(symbol, side)] = price


def _db_check_trade_switch_transition(cur):
    """Detect trade_switch ON→OFF transition via DB state.
    Returns True if transition detected (= immediate alert needed).
    Also updates prev_state to 'OFF'."""
    try:
        key = TRADE_SWITCH_DEDUP_KEY
        # Check existing state
        cur.execute("""
            SELECT prev_state FROM alert_dedup_state WHERE key = %s;
        """, (key,))
        row = cur.fetchone()

        if not row:
            # First time: no row → treat as transition (new OFF event)
            cur.execute("""
                INSERT INTO alert_dedup_state (key, prev_state, last_sent_ts)
                VALUES (%s, 'OFF', now())
                ON CONFLICT (key) DO NOTHING;
            """, (key,))
            return True

        prev = row[0]
        if prev != 'OFF':
            # Transition ON→OFF: mark state + reset send timer
            cur.execute("""
                UPDATE alert_dedup_state
                SET prev_state = 'OFF', last_sent_ts = now(),
                    last_seen_ts = now(), suppressed_count = 0
                WHERE key = %s;
            """, (key,))
            return True
        return False
    except Exception:
        return False


def _db_reset_trade_switch(cur):
    """Reset trade_switch dedup state to ON (called when switch is active)."""
    try:
        cur.execute("""
            UPDATE alert_dedup_state SET prev_state = 'ON'
            WHERE key = %s AND (prev_state IS NULL OR prev_state = 'OFF');
        """, (TRADE_SWITCH_DEDUP_KEY,))
    except Exception:
        pass


def _db_should_log_risk(cur, reason):
    """DB-based risk failure log dedup. Returns (should_log, summary).
    Uses alert_dedup_state for cross-process persistence."""
    key = f'autopilot:risk:{reason.replace(" ", "_").lower()}'
    try:
        cur.execute("""
            INSERT INTO alert_dedup_state (key, last_sent_ts, suppressed_count)
            VALUES (%s, NULL, 0)
            ON CONFLICT (key) DO NOTHING;
        """, (key,))
        cur.execute("""
            SELECT EXTRACT(EPOCH FROM (now() - last_sent_ts))::int,
                   suppressed_count
            FROM alert_dedup_state WHERE key = %s;
        """, (key,))
        row = cur.fetchone()
        if not row:
            return (True, '')
        elapsed = row[0]
        suppressed = row[1] or 0

        if elapsed is None or elapsed >= LOG_COOLDOWN_SEC:
            summary = ''
            if suppressed > 0:
                summary = f' (suppressed={suppressed} in last {LOG_COOLDOWN_SEC // 60}m)'
            cur.execute("""
                UPDATE alert_dedup_state
                SET last_sent_ts = now(), last_seen_ts = now(), suppressed_count = 0
                WHERE key = %s;
            """, (key,))
            return (True, summary)
        else:
            cur.execute("""
                UPDATE alert_dedup_state
                SET last_seen_ts = now(), suppressed_count = suppressed_count + 1
                WHERE key = %s;
            """, (key,))
            return (False, '')
    except Exception:
        return (True, '')


# ── Signal suppression state (snapshot용) ──
_last_suppress_reason = ''
_last_suppress_ts = 0


def _compute_signal_key(cur, symbol, direction, regime_ctx=None):
    """Compute composite signal key for dedup: symbol:regime:side:zone_anchor:time_bucket_5m."""
    regime = regime_ctx.get('regime', 'UNKNOWN') if regime_ctx else 'UNKNOWN'
    # Determine zone anchor based on current price vs VA/BB levels
    zone_anchor = 'UNKNOWN'
    try:
        cur.execute("SELECT mark_price FROM market_data_cache WHERE symbol = %s;", (symbol,))
        row = cur.fetchone()
        price = float(row[0]) if row and row[0] else 0
        if price > 0 and regime_ctx:
            vah = regime_ctx.get('vah')
            val = regime_ctx.get('val')
            poc = regime_ctx.get('poc')
            if val and price > 0 and abs(price - val) / val * 100 <= 0.3:
                zone_anchor = 'VAL'
            elif vah and price > 0 and abs(price - vah) / vah * 100 <= 0.3:
                zone_anchor = 'VAH'
            elif poc and price > 0 and abs(price - poc) / poc * 100 <= 0.2:
                zone_anchor = 'POC'
            else:
                zone_anchor = 'MID'
    except Exception:
        pass
    time_bucket = int(time.time()) // 300
    return f'{symbol}:{regime}:{direction}:{zone_anchor}:{time_bucket}'


def _record_signal_emission(cur, symbol, direction, regime_ctx=None):
    """Record signal emission timestamp in DB for cooldown tracking."""
    # Legacy direction-based key (backward compat)
    key = f'autopilot:signal:{symbol}:{direction}'
    cur.execute("""
        INSERT INTO alert_dedup_state (key, last_sent_ts, suppressed_count)
        VALUES (%s, now(), 0)
        ON CONFLICT (key) DO UPDATE
        SET last_sent_ts = now(), suppressed_count = 0;
    """, (key,))
    # Composite signal key (5m bucket dedup)
    if regime_ctx:
        sig_key = _compute_signal_key(cur, symbol, direction, regime_ctx)
        composite_db_key = f'autopilot:signal:{sig_key}'
        cur.execute("""
            INSERT INTO alert_dedup_state (key, last_sent_ts, suppressed_count)
            VALUES (%s, now(), 0)
            ON CONFLICT (key) DO UPDATE
            SET last_sent_ts = now(), suppressed_count = 0;
        """, (composite_db_key,))


def _is_in_repeat_cooldown(cur, symbol, direction, cooldown_sec=None, regime_ctx=None):
    """Check if same symbol+direction signal was emitted within cooldown window.
    Also checks composite signal_key for 5m bucket dedup.
    Returns (in_cooldown: bool, remaining_sec: int)."""
    if cooldown_sec is None:
        cooldown_sec = REPEAT_SIGNAL_COOLDOWN_SEC
    # 1. Legacy direction-based check (10min)
    key = f'autopilot:signal:{symbol}:{direction}'
    try:
        cur.execute("""
            SELECT EXTRACT(EPOCH FROM (now() - last_sent_ts))::int
            FROM alert_dedup_state WHERE key = %s;
        """, (key,))
        row = cur.fetchone()
        if row and row[0] is not None:
            elapsed = row[0]
            if elapsed < cooldown_sec:
                return (True, cooldown_sec - elapsed)
    except Exception as e:
        _log(f'repeat_cooldown legacy check error (FAIL-OPEN): {e}')
    # 2. Composite signal_key check (same zone+bucket within 5min)
    if regime_ctx:
        try:
            sig_key = _compute_signal_key(cur, symbol, direction, regime_ctx)
            composite_db_key = f'autopilot:signal:{sig_key}'
            cur.execute("""
                SELECT EXTRACT(EPOCH FROM (now() - last_sent_ts))::int
                FROM alert_dedup_state WHERE key = %s;
            """, (composite_db_key,))
            row = cur.fetchone()
            if row and row[0] is not None:
                elapsed = row[0]
                bucket_cooldown = 300  # 5min bucket
                if elapsed < bucket_cooldown:
                    return (True, bucket_cooldown - elapsed)
        except Exception as e:
            _log(f'repeat_cooldown composite check error (FAIL-OPEN): {e}')
    return (False, 0)


def _check_stop_loss_cooldown(cur, symbol):
    """Check consecutive stop-loss events. If >= TRIGGER within window, block for PAUSE duration.
    Returns (ok, reason, remaining_sec)."""
    try:
        cur.execute("""
            SELECT last_fill_at FROM execution_log
            WHERE symbol = %s AND status = 'FILLED'
              AND order_type IN ('CLOSE', 'FULL_CLOSE', 'REDUCE', 'REVERSE_CLOSE')
              AND realized_pnl < 0
              AND last_fill_at >= now() - make_interval(secs => %s)
            ORDER BY last_fill_at DESC;
        """, (symbol, STOP_LOSS_COOLDOWN_WINDOW_SEC))
        rows = cur.fetchall()
        stop_count = len(rows)
        if stop_count >= STOP_LOSS_COOLDOWN_TRIGGER and rows:
            from datetime import datetime, timezone
            last_stop_ts = rows[0][0]
            if last_stop_ts.tzinfo is None:
                last_stop_ts = last_stop_ts.replace(tzinfo=timezone.utc)
            elapsed = (datetime.now(timezone.utc) - last_stop_ts).total_seconds()
            if elapsed < STOP_LOSS_COOLDOWN_PAUSE_SEC:
                remaining = int(STOP_LOSS_COOLDOWN_PAUSE_SEC - elapsed)
                return (False,
                        f'COOLDOWN_AFTER_CONSECUTIVE_STOPS: stops={stop_count} '
                        f'window={STOP_LOSS_COOLDOWN_WINDOW_SEC // 60}m '
                        f'cooldown={STOP_LOSS_COOLDOWN_PAUSE_SEC // 60}m',
                        remaining)
    except Exception as e:
        _log(f'stop_loss_cooldown check error (FAIL-OPEN): {e}')
    return (True, '', 0)


def _check_loss_streak_cooldown(cur):
    """Check if loss streak cooldown blocks new entry.

    ff_loss_streak_cooldown=OFF → always (True, '').
    ON → check recent consecutive losses in execution_log.

    Returns (ok: bool, reason: str).
    """
    try:
        import feature_flags
        if not feature_flags.is_enabled('ff_loss_streak_cooldown'):
            return (True, '')
    except Exception:
        return (True, '')

    try:
        from strategy_v3 import config_v3
        trigger = config_v3.get('loss_streak_trigger', 3)
        cooldown_sec = config_v3.get('loss_streak_cooldown_sec', 1200)
        window_hours = config_v3.get('loss_streak_window_hours', 3)

        # Query recent closed trades within the window, ordered by time desc
        cur.execute("""
            SELECT realized_pnl FROM execution_log
            WHERE order_type IN ('CLOSE', 'FULL_CLOSE', 'REDUCE', 'REVERSE_CLOSE')
              AND status = 'FILLED'
              AND ts > now() - make_interval(hours => %s)
            ORDER BY ts DESC
            LIMIT %s
        """, (window_hours, trigger))
        rows = cur.fetchall()

        if not rows or len(rows) < trigger:
            return (True, '')

        # Count consecutive losses from most recent
        streak = 0
        for row in rows:
            if row[0] is not None and float(row[0]) < 0:
                streak += 1
            else:
                break

        if streak >= trigger:
            # Check if most recent loss is within cooldown window
            cur.execute("""
                SELECT ts FROM execution_log
                WHERE order_type IN ('CLOSE', 'FULL_CLOSE', 'REDUCE', 'REVERSE_CLOSE')
                  AND status = 'FILLED' AND realized_pnl < 0
                  AND ts > now() - make_interval(hours => %s)
                ORDER BY ts DESC LIMIT 1
            """, (window_hours,))
            last_loss_row = cur.fetchone()
            if last_loss_row and last_loss_row[0]:
                import datetime
                now = datetime.datetime.now(datetime.timezone.utc)
                last_loss_ts = last_loss_row[0]
                if last_loss_ts.tzinfo is None:
                    last_loss_ts = last_loss_ts.replace(tzinfo=datetime.timezone.utc)
                elapsed = (now - last_loss_ts).total_seconds()
                remaining = int(cooldown_sec - elapsed)
                if remaining > 0:
                    reason = f'LOSS_STREAK_COOLDOWN: {streak} consecutive losses, {remaining}s remaining'
                    return (False, reason)

        return (True, '')
    except Exception as e:
        _log(f'loss_streak_cooldown check error (FAIL-OPEN): {e}')
        return (True, '')


def _get_current_loss_streak(cur):
    """Return count of consecutive recent losses (0 if none)."""
    try:
        from strategy_v3 import config_v3
        window_hours = config_v3.get('loss_streak_window_hours', 3)
        cur.execute("""
            SELECT realized_pnl FROM execution_log
            WHERE order_type IN ('CLOSE', 'FULL_CLOSE', 'REDUCE', 'REVERSE_CLOSE')
              AND status = 'FILLED'
              AND ts > now() - make_interval(hours => %s)
            ORDER BY ts DESC LIMIT 10
        """, (window_hours,))
        streak = 0
        for row in cur.fetchall():
            if row[0] is not None and float(row[0]) < 0:
                streak += 1
            else:
                break
        return streak
    except Exception:
        return 0


# ── Mode cooloff state (in-memory) ──
_mode_cooloff_until = {}  # {'STATIC_RANGE': ts, ...}


def _check_mode_cooloff(cur, regime_class):
    """Block regime_class if its recent win rate is too low.
    Returns (ok: bool, reason: str).
    """
    try:
        from strategy_v3 import config_v3
        if not config_v3.get('mode_cooloff_enabled', False):
            return (True, '')

        cooloff_until = _mode_cooloff_until.get(regime_class, 0)
        now = time.time()
        if cooloff_until > now:
            remaining = int(cooloff_until - now)
            return (False, f'MODE_COOLOFF: {regime_class} blocked ({remaining}s remaining)')

        min_trades = config_v3.get('mode_cooloff_min_trades', 5)
        min_winrate = config_v3.get('mode_cooloff_min_winrate', 0.30)
        cooloff_hours = config_v3.get('mode_cooloff_hours', 4)

        cur.execute("""
            SELECT COUNT(*) as total,
                   COUNT(*) FILTER (WHERE realized_pnl > 0) as wins
            FROM execution_log
            WHERE order_type IN ('CLOSE', 'FULL_CLOSE', 'REDUCE', 'REVERSE_CLOSE')
              AND status = 'FILLED'
              AND regime_tag = %s
              AND ts > now() - interval '24 hours'
        """, (regime_class,))
        row = cur.fetchone()
        total, wins = row[0] or 0, row[1] or 0

        if total >= min_trades and total > 0:
            wr = wins / total
            if wr < min_winrate:
                _mode_cooloff_until[regime_class] = now + cooloff_hours * 3600
                return (False, f'MODE_COOLOFF: {regime_class} wr={wr:.0%} < {min_winrate:.0%} '
                        f'({total} trades) → blocked {cooloff_hours}h')

        return (True, '')
    except Exception as e:
        _log(f'mode_cooloff error (FAIL-OPEN): {e}')
        return (True, '')


def should_emit_signal(cur, symbol, direction, conf, regime_ctx=None):
    """Central gate: decide if a new signal should be emitted.
    Returns (ok, reason).
    """
    global _last_suppress_reason, _last_suppress_ts
    now_ts = int(time.time())

    # 1. Conf minimum cutoff
    if conf < MIN_CONFIDENCE:
        reason = f'CONF_TOO_LOW: conf={conf} < min={MIN_CONFIDENCE}'
        _last_suppress_reason = reason
        _last_suppress_ts = now_ts
        return (False, reason)

    # 2. Repeat signal cooldown (direction + composite key)
    (in_cd, remaining) = _is_in_repeat_cooldown(cur, symbol, direction, regime_ctx=regime_ctx)
    if in_cd:
        reason = f'REPEAT_SIGNAL_SUPPRESSED: dir={direction} cooldown_remaining={remaining}sec'
        _last_suppress_reason = reason
        _last_suppress_ts = now_ts
        return (False, reason)

    # 3. Stop-loss cooldown
    (sl_ok, sl_reason, sl_remaining) = _check_stop_loss_cooldown(cur, symbol)
    if not sl_ok:
        _last_suppress_reason = sl_reason
        _last_suppress_ts = now_ts
        return (False, sl_reason)

    # 4. Loss streak cooldown (ff_loss_streak_cooldown)
    (ls_ok, ls_reason) = _check_loss_streak_cooldown(cur)
    if not ls_ok:
        _last_suppress_reason = ls_reason
        _last_suppress_ts = now_ts
        return (False, ls_reason)

    # 5. Loss streak confidence escalation
    streak_count = _get_current_loss_streak(cur)
    if streak_count >= 2:
        from strategy_v3 import config_v3
        escalation = config_v3.get('loss_streak_conf_escalation', 5)
        escalated_min = MIN_CONFIDENCE + (streak_count * escalation)
        if conf < escalated_min:
            reason = f'LOSS_STREAK_ESCALATION: streak={streak_count}, conf={conf} < {escalated_min}'
            _last_suppress_reason = reason
            _last_suppress_ts = now_ts
            return (False, reason)

    return (True, 'OK')


def get_signal_policy_snapshot(cur):
    """Return signal suppression policy state for /snapshot display."""
    import regime_reader
    snapshot = {
        'start_stage_policy': 'FORCE_START_STAGE=1',
        'conf_thresholds': f'min_enter={MIN_CONFIDENCE}, add_only_conf>={CONF_ADD_THRESHOLD}',
        'repeat_cooldown_sec': REPEAT_SIGNAL_COOLDOWN_SEC,
        'last_suppress_reason': _last_suppress_reason or 'none',
        'last_suppress_ts': _last_suppress_ts,
    }
    # Per-direction cooldown remaining
    for direction in ('LONG', 'SHORT'):
        (in_cd, remaining) = _is_in_repeat_cooldown(cur, SYMBOL, direction)
        snapshot[f'cooldown_{direction}_remaining'] = remaining if in_cd else 0

    # Last signal info
    for direction in ('LONG', 'SHORT'):
        key = f'autopilot:signal:{SYMBOL}:{direction}'
        try:
            cur.execute("""
                SELECT last_sent_ts FROM alert_dedup_state WHERE key = %s;
            """, (key,))
            row = cur.fetchone()
            if row and row[0]:
                snapshot[f'last_signal_{direction}_ts'] = str(row[0])
        except Exception:
            pass

    # Stop-loss cooldown
    (sl_ok, sl_reason, sl_remaining) = _check_stop_loss_cooldown(cur, SYMBOL)
    snapshot['stop_cooldown_active'] = not sl_ok
    snapshot['stop_cooldown_remaining'] = sl_remaining
    if sl_reason:
        snapshot['stop_cooldown_reason'] = sl_reason

    # v14: Regime + ADD control info
    try:
        regime_ctx = regime_reader.get_current_regime(cur)
        regime = regime_ctx.get('regime', 'UNKNOWN')
        r_params = regime_reader.get_regime_params(regime, regime_ctx.get('shock_type'))
        snapshot['regime'] = regime
        snapshot['regime_confidence'] = regime_ctx.get('confidence', 0)
        snapshot['regime_bbw_ratio'] = regime_ctx.get('bbw_ratio')
        snapshot['regime_bb_width_pct'] = regime_ctx.get('bb_width_pct')
        snapshot['regime_adx'] = regime_ctx.get('adx_14')
        snapshot['max_stage'] = r_params.get('stage_max', 7)
        snapshot['add_min_interval_sec'] = r_params.get('add_min_interval_sec', 300)
        snapshot['max_adds_per_30m'] = r_params.get('max_adds_per_30m', 3)
        snapshot['add_retest_required'] = r_params.get('add_retest_required', False)
        snapshot['event_add_blocked'] = r_params.get('event_add_blocked', False)
        snapshot['same_dir_reentry_cooldown_sec'] = r_params.get('same_dir_reentry_cooldown_sec', 600)

        # ADD interval remaining
        (interval_ok, remaining) = _check_add_interval(cur, r_params)
        snapshot['add_interval_remaining'] = remaining if not interval_ok else 0

        # ADD count in 30m
        (count_ok, add_count, add_limit) = _check_adds_per_30m(cur, r_params)
        snapshot['adds_30m_count'] = add_count
        snapshot['adds_30m_limit'] = add_limit

        # Next entry/ADD earliest time
        import datetime as _dt
        now = _dt.datetime.now(_dt.timezone.utc)
        if not interval_ok:
            snapshot['next_add_earliest'] = str(now + _dt.timedelta(seconds=remaining))
        else:
            snapshot['next_add_earliest'] = 'NOW'

        # Regime transition
        snapshot['in_transition'] = regime_ctx.get('in_transition', False)
    except Exception as e:
        snapshot['regime_error'] = str(e)

    # Order throttle status
    try:
        import order_throttle
        ts = order_throttle.get_throttle_status()
        snapshot['throttle_hourly'] = f'{ts["hourly_count"]}/{ts["hourly_limit"]}'
        snapshot['throttle_10min'] = f'{ts["10min_count"]}/{ts["10min_limit"]}'
        snapshot['throttle_locked'] = ts.get('entry_locked', False)
        snapshot['throttle_lock_reason'] = ts.get('lock_reason', '')
    except Exception:
        pass

    return snapshot


def _get_exchange():
    '''Get or create a cached ccxt Bybit exchange instance.'''
    global _exchange
    if _exchange is not None:
        return _exchange
    import ccxt
    from dotenv import load_dotenv
    load_dotenv('/root/trading-bot/app/.env')
    _exchange = ccxt.bybit({
        'apiKey': os.getenv('BYBIT_API_KEY'),
        'secret': os.getenv('BYBIT_SECRET'),
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'}})
    _exchange.load_markets()
    return _exchange


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _db_conn():
    from db_config import get_conn
    return get_conn()


def _load_tg_env():
    env_path = '/root/trading-bot/app/telegram_cmd.env'
    (token, chat_id) = ('', '')
    try:
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if '=' in line and not line.startswith('#'):
                    k, v = line.split('=', 1)
                    k = k.strip()
                    v = v.strip()
                    if k == 'TELEGRAM_BOT_TOKEN':
                        token = v
                    elif k == 'TELEGRAM_ALLOWED_CHAT_ID':
                        chat_id = v
    except Exception:
        pass
    return (token, chat_id)


def _notify_telegram(text=None):
    (token, chat_id) = _load_tg_env()
    if not token or not chat_id:
        return None
    try:
        url = f'https://api.telegram.org/bot{token}/sendMessage'
        data = urllib.parse.urlencode({
            'chat_id': chat_id,
            'text': text,
            'disable_web_page_preview': 'true'}).encode('utf-8')
        req = urllib.request.Request(url, data=data, method='POST')
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass
    return None


def _check_autopilot_enabled(cur=None):
    '''Check if autopilot is enabled in DB.'''
    global _migrations_done
    if not _migrations_done:
        import db_migrations
        db_migrations.ensure_autopilot_config(cur)
        db_migrations.ensure_trade_process_log(cur)
        db_migrations.ensure_stage_column(cur)
        db_migrations.ensure_alert_dedup_state(cur)
        db_migrations.ensure_plan_state_column(cur)
        _migrations_done = True
    cur.execute('SELECT enabled FROM autopilot_config ORDER BY id DESC LIMIT 1;')
    row = cur.fetchone()
    return bool(row[0]) if row else False


def _risk_checks(cur=None):
    '''Run all risk checks. Returns (ok, reason).'''
    if os.getenv('LIVE_TRADING', '') != 'YES_I_UNDERSTAND':
        return (False, 'LIVE_TRADING not set')

    cur.execute('SELECT enabled FROM trade_switch ORDER BY id DESC LIMIT 1;')
    row = cur.fetchone()
    if not row or not row[0]:
        return (False, 'trade_switch OFF')

    # Proactive Manager: entry veto check
    try:
        import proactive_manager
        vetoed, veto_reason = proactive_manager.check_entry_veto(cur)
        if vetoed:
            return (False, f'proactive_veto: {veto_reason}')
    except Exception:
        pass  # FAIL-OPEN

    cur.execute("""
        SELECT count(*) FROM execution_log
        WHERE symbol = 'BTC/USDT:USDT' AND status = 'FILLED'
          AND order_type IN ('OPEN', 'ADD')
          AND last_fill_at >= (now() AT TIME ZONE 'Asia/Seoul')::date AT TIME ZONE 'Asia/Seoul';
    """)
    row = cur.fetchone()
    daily_count = int(row[0]) if row else 0
    if daily_count >= MAX_DAILY_TRADES:
        return (False, f'daily limit reached ({daily_count}/{MAX_DAILY_TRADES})')

    cur.execute("""
        SELECT ts FROM trade_process_log
        WHERE source = 'autopilot'
        ORDER BY id DESC LIMIT 1;
    """)
    row = cur.fetchone()
    if row and row[0]:
        import datetime
        ts = row[0] if row[0].tzinfo is not None else row[0].replace(
            tzinfo=datetime.timezone.utc)
        elapsed = (datetime.datetime.now(datetime.timezone.utc) - ts).total_seconds()
        if elapsed < COOLDOWN_SEC:
            return (False, f'cooldown ({int(elapsed)}/{COOLDOWN_SEC}s)')

    return (True, 'ok')


def _get_rsi_5m(cur):
    """Read latest RSI from indicators table (5m preferred, 1m fallback). Returns float or None."""
    try:
        for tf in ('5m', '1m'):
            cur.execute("""
                SELECT rsi_14 FROM indicators
                WHERE symbol = %s AND tf = %s
                ORDER BY ts DESC LIMIT 1;
            """, (SYMBOL, tf))
            row = cur.fetchone()
            if row and row[0] is not None:
                return float(row[0])
        return None
    except Exception:
        return None


def _get_recent_candles(cur, tf, count):
    """Read recent candles from candles table (fallback to 1m if requested tf empty)."""
    try:
        for try_tf in (tf, '1m') if tf != '1m' else (tf,):
            cur.execute("""
                SELECT h, l, c FROM candles
                WHERE symbol = %s AND tf = %s
                ORDER BY ts DESC LIMIT %s;
            """, (SYMBOL, try_tf, count))
            rows = cur.fetchall()
            result = [{'h': float(r[0]), 'l': float(r[1]), 'c': float(r[2])} for r in rows if r[0] is not None]
            if result:
                return result
        return []
    except Exception:
        return []


def _check_consec_loss_cooldown(cur, direction, regime_params):
    """Block entry if 2+ consecutive same-direction losses in last 2 hours."""
    cooldown_min = regime_params.get('consec_loss_cooldown_min', 45) if regime_params else 45
    try:
        cur.execute("""
            SELECT direction, realized_pnl, last_fill_at
            FROM execution_log
            WHERE symbol = %s AND status = 'FILLED'
              AND order_type IN ('CLOSE', 'FULL_CLOSE')
              AND last_fill_at >= now() - interval '2 hours'
            ORDER BY last_fill_at DESC LIMIT 3;
        """, (SYMBOL,))
        rows = cur.fetchall()
        consec_losses = 0
        for r in rows:
            if r[0] == direction and r[1] is not None and float(r[1]) < 0:
                consec_losses += 1
            else:
                break
        if consec_losses >= 2:
            from datetime import datetime, timezone
            last_loss_ts = rows[0][2]
            if last_loss_ts.tzinfo is None:
                last_loss_ts = last_loss_ts.replace(tzinfo=timezone.utc)
            elapsed_min = (datetime.now(timezone.utc) - last_loss_ts).total_seconds() / 60
            if elapsed_min < cooldown_min:
                return (False, f'연속 손절 쿨다운 ({consec_losses}회, {int(cooldown_min - elapsed_min)}분 남음)')
    except Exception as e:
        _log(f'consec_loss_cooldown check error (FAIL-OPEN): {e}')
    return (True, '')


def _compute_entry_zones(price, regime_ctx, bb_data=None):
    """Compute zone-based entry eligibility for RANGE mode.

    Returns dict with zone flags and distance metrics.
    FAIL-OPEN: returns all-False zones if data unavailable.
    """
    result = {
        'in_long_zone': False,
        'in_short_zone': False,
        'in_mid_zone_ban': False,
        'dist_to_val_pct': None,
        'dist_to_vah_pct': None,
        'dist_to_poc_pct': None,
        'zone_detail': '',
    }
    if not price or price <= 0:
        return result

    vah = regime_ctx.get('vah') if regime_ctx else None
    val = regime_ctx.get('val') if regime_ctx else None
    poc = regime_ctx.get('poc') if regime_ctx else None

    # Get regime params for margins
    import regime_reader
    regime = regime_ctx.get('regime', 'UNKNOWN') if regime_ctx else 'UNKNOWN'
    r_params = regime_reader.get_regime_params(regime, regime_ctx.get('shock_type') if regime_ctx else None)
    va_margin = r_params.get('zone_margin_va_pct', 0.12) / 100
    bb_margin = r_params.get('zone_margin_bb_pct', 0.08) / 100
    mid_ban_pct = r_params.get('mid_zone_ban_pct', 0.15) / 100

    bb_lower = bb_data.get('bb_lower') if bb_data else None
    bb_upper = bb_data.get('bb_upper') if bb_data else None
    bb_mid = bb_data.get('bb_mid') if bb_data else None

    # Distance calculations
    if val and val > 0:
        result['dist_to_val_pct'] = round((price - val) / val * 100, 3)
    if vah and vah > 0:
        result['dist_to_vah_pct'] = round((price - vah) / vah * 100, 3)
    if poc and poc > 0:
        result['dist_to_poc_pct'] = round((price - poc) / poc * 100, 3)

    # LONG_ZONE: price <= max(VAL*(1+va_margin), BB_lower*(1+bb_margin))
    long_boundaries = []
    if val and val > 0:
        long_boundaries.append(val * (1 + va_margin))
    if bb_lower and bb_lower > 0:
        long_boundaries.append(bb_lower * (1 + bb_margin))
    if long_boundaries and price <= max(long_boundaries):
        result['in_long_zone'] = True
        result['zone_detail'] += 'LONG_ZONE '

    # SHORT_ZONE: price >= min(VAH*(1-va_margin), BB_upper*(1-bb_margin))
    short_boundaries = []
    if vah and vah > 0:
        short_boundaries.append(vah * (1 - va_margin))
    if bb_upper and bb_upper > 0:
        short_boundaries.append(bb_upper * (1 - bb_margin))
    if short_boundaries and price >= min(short_boundaries):
        result['in_short_zone'] = True
        result['zone_detail'] += 'SHORT_ZONE '

    # MID_ZONE_BAN: |price - POC| <= mid_ban_pct OR |price - BB_mid| <= mid_ban_pct
    if poc and poc > 0 and abs(price - poc) / poc <= mid_ban_pct:
        result['in_mid_zone_ban'] = True
        result['zone_detail'] += 'MID_BAN(POC) '
    elif bb_mid and bb_mid > 0 and abs(price - bb_mid) / bb_mid <= mid_ban_pct:
        result['in_mid_zone_ban'] = True
        result['zone_detail'] += 'MID_BAN(BB_mid) '

    return result


# ── Anti-chase filter state ──
_anti_chase_ban_until = 0.0
_anti_chase_ban_reason = ''


def _check_anti_chase(cur, regime_ctx, dry_run=False):
    """Anti-chase filter for RANGE mode.
    Blocks entry for 3min after sharp 1m/5m price moves.
    dry_run=True: read-only check (no global state mutation), used by /snapshot.
    Returns (ok, reason)."""
    global _anti_chase_ban_until, _anti_chase_ban_reason

    now = time.time()
    if now < _anti_chase_ban_until:
        remaining = int(_anti_chase_ban_until - now)
        return (False, f'ANTI_CHASE active: {_anti_chase_ban_reason} ({remaining}s remaining)')

    regime = regime_ctx.get('regime', 'UNKNOWN') if regime_ctx else 'UNKNOWN'
    if regime != 'RANGE':
        return (True, 'not RANGE mode')

    import regime_reader
    r_params = regime_reader.get_regime_params(regime, regime_ctx.get('shock_type') if regime_ctx else None)
    ret_1m_threshold = r_params.get('anti_chase_ret_1m', 0.25)
    ret_5m_threshold = r_params.get('anti_chase_ret_5m', 0.60)
    ban_sec = r_params.get('anti_chase_ban_sec', 180)

    try:
        cur.execute("""
            SELECT c FROM candles
            WHERE symbol = %s AND tf = '1m'
            ORDER BY ts DESC LIMIT 6;
        """, (SYMBOL,))
        rows = cur.fetchall()
        if len(rows) >= 2:
            c_now = float(rows[0][0])
            c_1m_ago = float(rows[1][0])
            if c_1m_ago > 0:
                ret_1m = abs(c_now - c_1m_ago) / c_1m_ago * 100
                if ret_1m > ret_1m_threshold:
                    if not dry_run:
                        _anti_chase_ban_until = now + ban_sec
                        _anti_chase_ban_reason = f'ret_1m={ret_1m:.2f}% > {ret_1m_threshold}%'
                    return (False, f'ANTI_CHASE: ret_1m={ret_1m:.2f}% > {ret_1m_threshold}%')
        if len(rows) >= 6:
            c_now = float(rows[0][0])
            c_5m_ago = float(rows[5][0])
            if c_5m_ago > 0:
                ret_5m = abs(c_now - c_5m_ago) / c_5m_ago * 100
                if ret_5m > ret_5m_threshold:
                    if not dry_run:
                        _anti_chase_ban_until = now + ban_sec
                        _anti_chase_ban_reason = f'ret_5m={ret_5m:.2f}% > {ret_5m_threshold}%'
                    return (False, f'ANTI_CHASE: ret_5m={ret_5m:.2f}% > {ret_5m_threshold}%')
    except Exception as e:
        _log(f'anti_chase check error (FAIL-OPEN): {e}')

    return (True, '')


def _get_bb_data(cur):
    """Fetch BB band data from indicators table. Returns dict or empty."""
    try:
        cur.execute("""
            SELECT bb_up, bb_dn, bb_mid FROM indicators
            WHERE symbol = %s
            ORDER BY ts DESC LIMIT 1;
        """, (SYMBOL,))
        row = cur.fetchone()
        if row and row[0] and row[1] and row[2]:
            return {
                'bb_upper': float(row[0]),
                'bb_lower': float(row[1]),
                'bb_mid': float(row[2]),
            }
    except Exception as e:
        _log(f'BB data fetch error: {e}')
    return {}


def _check_reversal_confirmation(cur, direction):
    """Check reversal confirmation for RANGE entry.
    RSI(5m) turning or candle engulfing pattern.
    Returns (ok, reason)."""
    try:
        # RSI check: two recent 5m RSI values
        cur.execute("""
            SELECT rsi_14 FROM indicators
            WHERE symbol = %s AND tf = '5m' AND rsi_14 IS NOT NULL
            ORDER BY ts DESC LIMIT 2;
        """, (SYMBOL,))
        rsi_rows = cur.fetchall()
        if len(rsi_rows) >= 2:
            rsi_now = float(rsi_rows[0][0])
            rsi_prev = float(rsi_rows[1][0])
            if direction == 'LONG':
                # RSI was oversold and turning up
                if rsi_prev < 35 and rsi_now > rsi_prev:
                    return (True, f'RSI reversal LONG: {rsi_prev:.0f}→{rsi_now:.0f}')
            else:
                # RSI was overbought and turning down
                if rsi_prev > 65 and rsi_now < rsi_prev:
                    return (True, f'RSI reversal SHORT: {rsi_prev:.0f}→{rsi_now:.0f}')

        # Candle engulfing pattern check
        cur.execute("""
            SELECT o, c FROM candles
            WHERE symbol = %s AND tf = '5m'
            ORDER BY ts DESC LIMIT 2;
        """, (SYMBOL,))
        c_rows = cur.fetchall()
        if len(c_rows) >= 2:
            cur_o, cur_c = float(c_rows[0][0]), float(c_rows[0][1])
            prev_o, prev_c = float(c_rows[1][0]), float(c_rows[1][1])
            if direction == 'LONG':
                # Bullish engulfing: prev bearish, cur bullish and engulfs prev
                if prev_c < prev_o and cur_c > cur_o and cur_c > prev_o and cur_o < prev_c:
                    return (True, 'bullish engulfing')
            else:
                # Bearish engulfing: prev bullish, cur bearish and engulfs prev
                if prev_c > prev_o and cur_c < cur_o and cur_c < prev_o and cur_o > prev_c:
                    return (True, 'bearish engulfing')
    except Exception as e:
        _log(f'reversal_confirmation error (FAIL-OPEN): {e}')
        return (True, f'FAIL-OPEN: {e}')

    return (False, 'no reversal confirmation')


def _check_post_close_cooldown(cur, symbol):
    """Post-position-close cooldown: TP→3min, SL→10min.
    Returns (ok, reason, remaining_sec)."""
    try:
        cur.execute("""
            SELECT order_type, realized_pnl, last_fill_at
            FROM execution_log
            WHERE symbol = %s AND status = 'FILLED'
              AND order_type IN ('CLOSE', 'FULL_CLOSE')
              AND last_fill_at >= now() - interval '15 minutes'
            ORDER BY last_fill_at DESC LIMIT 1;
        """, (symbol,))
        row = cur.fetchone()
        if row and row[2]:
            from datetime import datetime, timezone
            pnl = float(row[1]) if row[1] is not None else 0
            fill_ts = row[2]
            if fill_ts.tzinfo is None:
                fill_ts = fill_ts.replace(tzinfo=timezone.utc)
            elapsed = (datetime.now(timezone.utc) - fill_ts).total_seconds()
            if pnl < 0:
                # SL → 10min cooldown
                cooldown = 600
                if elapsed < cooldown:
                    return (False, f'POST_SL_COOLDOWN: {int(cooldown - elapsed)}s remaining', int(cooldown - elapsed))
            else:
                # TP → 3min cooldown
                cooldown = 180
                if elapsed < cooldown:
                    return (False, f'POST_TP_COOLDOWN: {int(cooldown - elapsed)}s remaining', int(cooldown - elapsed))
    except Exception as e:
        _log(f'post_close_cooldown error (FAIL-OPEN): {e}')
    return (True, '', 0)


def _check_zone_reentry_ban(cur, symbol, direction, regime_ctx):
    """After SL at a zone, ban re-entry in same direction until price moves past POC/BB_mid.
    Returns (ok, reason)."""
    if not regime_ctx or regime_ctx.get('regime') != 'RANGE':
        return (True, '')
    try:
        # Find most recent SL in last 30 minutes
        cur.execute("""
            SELECT direction, realized_pnl, last_fill_at
            FROM execution_log
            WHERE symbol = %s AND status = 'FILLED'
              AND order_type IN ('CLOSE', 'FULL_CLOSE')
              AND realized_pnl < 0
              AND last_fill_at >= now() - interval '30 minutes'
            ORDER BY last_fill_at DESC LIMIT 1;
        """, (symbol,))
        row = cur.fetchone()
        if not row:
            return (True, '')
        sl_direction = row[0]
        if sl_direction != direction:
            return (True, '')
        # Same direction SL occurred — check if price has moved past POC/BB_mid
        cur.execute("SELECT mark_price FROM market_data_cache WHERE symbol = %s;", (symbol,))
        price_row = cur.fetchone()
        price = float(price_row[0]) if price_row and price_row[0] else 0
        if not price:
            return (True, 'FAIL-OPEN: no price')
        poc = regime_ctx.get('poc')
        bb_data = _get_bb_data(cur)
        bb_mid = bb_data.get('bb_mid')
        mid_ref = poc or bb_mid
        if not mid_ref:
            return (True, 'FAIL-OPEN: no POC/BB_mid')
        if direction == 'LONG' and price < mid_ref:
            return (False, f'ZONE_REENTRY_BAN: SL LONG → price {price:.0f} < mid {mid_ref:.0f}')
        elif direction == 'SHORT' and price > mid_ref:
            return (False, f'ZONE_REENTRY_BAN: SL SHORT → price {price:.0f} > mid {mid_ref:.0f}')
    except Exception as e:
        _log(f'zone_reentry_ban error (FAIL-OPEN): {e}')
    return (True, '')


def _check_post_sl_opposite_ban(cur, symbol):
    """After any SL, block ALL direction entries for 10 minutes.
    Returns (ok, reason)."""
    try:
        cur.execute("""
            SELECT last_fill_at FROM execution_log
            WHERE symbol = %s AND status = 'FILLED'
              AND order_type IN ('CLOSE', 'FULL_CLOSE')
              AND realized_pnl < 0
              AND last_fill_at >= now() - interval '10 minutes'
            ORDER BY last_fill_at DESC LIMIT 1;
        """, (symbol,))
        row = cur.fetchone()
        if row and row[0]:
            from datetime import datetime, timezone
            fill_ts = row[0]
            if fill_ts.tzinfo is None:
                fill_ts = fill_ts.replace(tzinfo=timezone.utc)
            elapsed = (datetime.now(timezone.utc) - fill_ts).total_seconds()
            remaining = int(600 - elapsed)
            if remaining > 0:
                return (False, f'POST_SL_BAN: all entries blocked ({remaining}s remaining)')
    except Exception as e:
        _log(f'post_sl_opposite_ban error (FAIL-OPEN): {e}')
    return (True, '')


def _check_regime_entry_filter(cur, regime_ctx, scores, price=None):
    """Check if entry is allowed based on regime's entry filter.

    Returns (ok, reason).
    FAIL-OPEN: if regime_ctx not available, always allow.
    """
    if not regime_ctx or not regime_ctx.get('available'):
        return (True, 'MCTX unavailable — FAIL-OPEN')

    import regime_reader
    params = regime_reader.get_regime_params(
        regime_ctx.get('regime', 'UNKNOWN'), regime_ctx.get('shock_type'))
    entry_filter = params.get('entry_filter', 'none')

    if entry_filter == 'none':
        return (True, 'no filter')

    if entry_filter == 'blocked':
        return (False, f'SHOCK 모드: 진입 차단 ({regime_ctx.get("shock_type", "")})')

    if regime_ctx.get('in_transition'):
        return (False, '레짐 전환 쿨다운 중')

    dominant = scores.get('dominant_side', 'LONG')

    if entry_filter == 'band_edge':
        # RANGE mode: zone-based entry with anti-chase, mid-zone ban, reversal confirmation
        if not price:
            try:
                ex = _get_exchange()
                ticker = ex.fetch_ticker(SYMBOL)
                price = float(ticker['last'])
            except Exception:
                return (True, 'price fetch failed — FAIL-OPEN')

        vah = regime_ctx.get('vah')
        val = regime_ctx.get('val')
        if not vah or not val:
            return (True, 'VAH/VAL unavailable — FAIL-OPEN')

        # 1. Fetch BB data
        bb_data = _get_bb_data(cur)

        # 2. Compute entry zones
        zones = _compute_entry_zones(price, regime_ctx, bb_data)

        # 3. MID_ZONE_BAN check
        if zones['in_mid_zone_ban']:
            return (False, f'RANGE MID_ZONE_BAN: 중앙 진입 금지 ({zones["zone_detail"].strip()})')

        # 4. BAND_OVERHEAT check
        bb_upper = bb_data.get('bb_upper')
        bb_lower = bb_data.get('bb_lower')
        if dominant == 'LONG' and bb_upper and price > bb_upper:
            return (False, f'RANGE BAND_OVERHEAT: LONG @ price {price:.0f} > BB_upper {bb_upper:.0f}')
        if dominant == 'SHORT' and bb_lower and price < bb_lower:
            return (False, f'RANGE BAND_OVERHEAT: SHORT @ price {price:.0f} < BB_lower {bb_lower:.0f}')

        # 5. ZONE_GATE: LONG needs LONG_ZONE, SHORT needs SHORT_ZONE
        if dominant == 'LONG' and not zones['in_long_zone']:
            return (False, f'RANGE ZONE_GATE: LONG 진입 불가 (not in LONG_ZONE, dist_val={zones["dist_to_val_pct"]}%)')
        if dominant == 'SHORT' and not zones['in_short_zone']:
            return (False, f'RANGE ZONE_GATE: SHORT 진입 불가 (not in SHORT_ZONE, dist_vah={zones["dist_to_vah_pct"]}%)')

        # 6. Reversal confirmation
        (rev_ok, rev_reason) = _check_reversal_confirmation(cur, dominant)
        if not rev_ok:
            return (False, f'RANGE NO_REVERSAL: {rev_reason}')

        return (True, f'RANGE {dominant}: zone OK ({zones["zone_detail"].strip()}) + {rev_reason}')

    if entry_filter == 'breakout_confirm':
        if not regime_ctx.get('breakout_confirmed'):
            return (False, 'BREAKOUT 모드: VA 돌파 확인 대기')

        # Fake breakout filter — check for rapid retracement
        retrace_pct = params.get('fake_breakout_retracement_pct', 60)
        candles_5m = _get_recent_candles(cur, '5m', 3)
        if len(candles_5m) >= 2:
            prev_range = abs(candles_5m[1]['h'] - candles_5m[1]['l'])
            if prev_range > 0:
                latest = candles_5m[0]
                if dominant == 'LONG':
                    retrace = (candles_5m[1]['h'] - latest['c']) / prev_range * 100
                else:
                    retrace = (latest['c'] - candles_5m[1]['l']) / prev_range * 100
                if retrace >= retrace_pct:
                    return (False, f'BREAKOUT 가짜돌파 의심: 되돌림 {retrace:.0f}% >= {retrace_pct}%')

        return (True, 'BREAKOUT 확인됨')

    return (True, f'unknown filter: {entry_filter}')


def _check_add_interval(cur, regime_params):
    """v14: Check minimum interval between ADD attempts.
    Returns (ok, remaining_sec)."""
    min_interval = regime_params.get('add_min_interval_sec', 300)
    try:
        cur.execute("""
            SELECT EXTRACT(EPOCH FROM (now() - MAX(ts)))::int
            FROM execution_queue
            WHERE symbol = %s AND action_type = 'ADD'
              AND status NOT IN ('FAILED', 'CANCELLED', 'EXPIRED')
              AND ts >= now() - interval '1 hour';
        """, (SYMBOL,))
        row = cur.fetchone()
        if row and row[0] is not None:
            elapsed = row[0]
            if elapsed < min_interval:
                return (False, min_interval - elapsed)
    except Exception:
        pass
    return (True, 0)


def _check_adds_per_30m(cur, regime_params):
    """v14: Check max ADD count in last 30 minutes.
    Returns (ok, count, limit)."""
    max_adds = regime_params.get('max_adds_per_30m', 3)
    try:
        cur.execute("""
            SELECT count(*) FROM execution_queue
            WHERE symbol = %s AND action_type = 'ADD'
              AND status NOT IN ('FAILED', 'CANCELLED', 'EXPIRED')
              AND ts >= now() - interval '30 minutes';
        """, (SYMBOL,))
        row = cur.fetchone()
        count = int(row[0]) if row else 0
        if count >= max_adds:
            return (False, count, max_adds)
        return (True, count, max_adds)
    except Exception:
        return (True, 0, max_adds)


def _check_add_retest(cur, pos_side, regime_params):
    """v14: Check retest/pullback condition for ADD.
    LONG ADD: price <= VWAP or near BB_mid (pullback confirmed)
    SHORT ADD: price >= VWAP or near BB_mid (pullback confirmed)
    Returns (ok, reason)."""
    if not regime_params.get('add_retest_required', False):
        return (True, 'retest not required')
    try:
        # Read 15m indicators (VWAP, BB_mid)
        cur.execute("""
            SELECT vwap, bb_mid FROM indicators
            WHERE symbol = %s AND tf = '15m'
            ORDER BY ts DESC LIMIT 1;
        """, (SYMBOL,))
        ind_row = cur.fetchone()
        if not ind_row or ind_row[0] is None:
            return (True, 'FAIL-OPEN: no 15m indicators')

        vwap = float(ind_row[0]) if ind_row[0] else None
        bb_mid = float(ind_row[1]) if ind_row[1] else None

        # Get current price
        cur.execute("SELECT mark_price FROM market_data_cache WHERE symbol = %s;", (SYMBOL,))
        price_row = cur.fetchone()
        cur_price = float(price_row[0]) if price_row and price_row[0] else None

        if not cur_price:
            return (True, 'FAIL-OPEN: no price')

        # Check retest condition
        retest_ok = False
        reason_parts = []
        proximity_pct = 0.15  # 0.15% proximity to VWAP/BB_mid

        if pos_side == 'long':
            # LONG ADD: price should be at or below VWAP/BB_mid (pullback)
            if vwap and vwap > 0:
                diff_pct = (cur_price - vwap) / vwap * 100
                if diff_pct <= proximity_pct:
                    retest_ok = True
                    reason_parts.append(f'price<=VWAP ({cur_price:.0f}<={vwap:.0f})')
                else:
                    reason_parts.append(f'price>VWAP+{proximity_pct}% ({diff_pct:.2f}%)')
            if bb_mid and bb_mid > 0 and not retest_ok:
                diff_pct = (cur_price - bb_mid) / bb_mid * 100
                if diff_pct <= proximity_pct:
                    retest_ok = True
                    reason_parts.append(f'price<=BB_mid ({cur_price:.0f}<={bb_mid:.0f})')
        else:
            # SHORT ADD: price should be at or above VWAP/BB_mid (pullback up)
            if vwap and vwap > 0:
                diff_pct = (cur_price - vwap) / vwap * 100
                if diff_pct >= -proximity_pct:
                    retest_ok = True
                    reason_parts.append(f'price near VWAP ({cur_price:.0f} vs {vwap:.0f}, diff={diff_pct:+.2f}%)')
                else:
                    reason_parts.append(f'price below VWAP ({diff_pct:+.2f}%)')
            if bb_mid and bb_mid > 0 and not retest_ok:
                diff_pct = (cur_price - bb_mid) / bb_mid * 100
                if diff_pct >= -proximity_pct:
                    retest_ok = True
                    reason_parts.append(f'price near BB_mid ({cur_price:.0f} vs {bb_mid:.0f}, diff={diff_pct:+.2f}%)')

        if retest_ok:
            return (True, f'retest OK: {", ".join(reason_parts)}')
        return (False, f'ADD_NO_RETEST: {", ".join(reason_parts)}')
    except Exception as e:
        return (True, f'FAIL-OPEN: retest check error ({e})')


# v14.1: Telegram throttled notification (dedup by msg_type, once per 10min)
_tg_throttle_cache = {}

def _notify_telegram_throttled(text, msg_type='info', cooldown_sec=600):
    """Send Telegram message with dedup by msg_type (default once per 10min)."""
    now = time.time()
    last = _tg_throttle_cache.get(msg_type, 0)
    if now - last < cooldown_sec:
        return
    _tg_throttle_cache[msg_type] = now
    _notify_telegram(text)


# B2: Signal spam guard — same direction signal dedup
_signal_emission_cache = {}  # {f'{symbol}_{direction}': [ts1, ts2, ...]}

def _check_signal_spam(symbol, direction):
    """Check if same direction signals exceeded 3/30min threshold.
    Returns (ok: bool, cooldown_remaining: int)
    """
    key = f'{symbol}_{direction}'
    now = time.time()
    window_sec = 1800   # 30min
    max_signals = 3
    cooldown_sec = 1800  # 30min cooldown

    history = _signal_emission_cache.get(key, [])
    recent = [t for t in history if now - t < window_sec]
    _signal_emission_cache[key] = recent

    if len(recent) >= max_signals:
        last = max(recent)
        remaining = cooldown_sec - (now - last)
        if remaining > 0:
            return (False, int(remaining))
    # Record this check as pending (actual record on signal creation)
    return (True, 0)

def _record_signal_spam(symbol, direction):
    """Record a signal emission for spam guard tracking."""
    key = f'{symbol}_{direction}'
    _signal_emission_cache.setdefault(key, []).append(time.time())


# B3: Post-impulse chasing ban
_impulse_ban_until = 0

def _check_impulse_ban(cur, direction):
    """5min abs(change) >= 1.0% → 10~20min no-entry.
    Especially: price_drop >= 1.5% AND direction==SHORT → 20min block.
    Returns (ok: bool, remaining: float)
    """
    global _impulse_ban_until
    now = time.time()
    if now < _impulse_ban_until:
        return (False, _impulse_ban_until - now)

    try:
        cur.execute("""
            SELECT c FROM candles WHERE symbol=%s AND tf='1m'
            ORDER BY ts DESC LIMIT 5;
        """, (SYMBOL,))
        rows = cur.fetchall()
        if len(rows) >= 2:
            latest = float(rows[0][0])
            oldest = float(rows[-1][0])
            if oldest > 0:
                change_pct = abs(latest - oldest) / oldest * 100

                if change_pct >= 1.0:
                    ban_sec = 600  # 10min default
                    # Crash + SHORT chasing = 20min
                    if oldest > latest and direction == 'SHORT' and change_pct >= 1.5:
                        ban_sec = 1200
                    _impulse_ban_until = now + ban_sec
                    return (False, ban_sec)
    except Exception:
        pass  # FAIL-OPEN
    return (True, 0)


def _check_same_dir_reentry_cooldown(cur, direction, regime_params):
    """v14.1: Check same-direction re-entry cooldown.
    After a position is closed, block same-direction re-entry for N seconds.
    Returns (ok, remaining_sec)."""
    cooldown_sec = regime_params.get('same_dir_reentry_cooldown_sec', 600)
    try:
        cur.execute("""
            SELECT EXTRACT(EPOCH FROM (now() - MAX(ts)))::int
            FROM execution_log
            WHERE symbol = %s AND direction = %s
              AND close_reason IS NOT NULL
              AND status = 'FILLED'
              AND ts >= now() - interval '1 hour';
        """, (SYMBOL, direction))
        row = cur.fetchone()
        if row and row[0] is not None:
            elapsed = row[0]
            if elapsed < cooldown_sec:
                return (False, cooldown_sec - elapsed)
    except Exception:
        pass
    return (True, 0)


def _check_breakout_structure_intact(cur, pos_side, regime_ctx):
    """Check if BREAKOUT structure is still intact for ADD.
    LONG: price must remain > VAH. SHORT: price must remain < VAL.
    Returns (ok, reason)."""
    try:
        vah = regime_ctx.get('vah')
        val = regime_ctx.get('val')
        cur.execute("SELECT mark_price FROM market_data_cache WHERE symbol = %s;", (SYMBOL,))
        row = cur.fetchone()
        price = float(row[0]) if row and row[0] else 0
        if not price:
            return (True, 'FAIL-OPEN: no price')
        if pos_side == 'long' and vah:
            if price <= vah:
                return (False, f'BREAKOUT LONG: price {price:.0f} <= VAH {vah:.0f} — structure broken')
        elif pos_side == 'short' and val:
            if price >= val:
                return (False, f'BREAKOUT SHORT: price {price:.0f} >= VAL {val:.0f} — structure broken')
    except Exception as e:
        return (True, f'FAIL-OPEN: structure check error ({e})')
    return (True, 'structure intact')


def _check_position_for_add(cur=None, pos_side=None, pos_qty=None, scores=None, regime_ctx=None):
    """Evaluate whether ADD is allowed for an existing position.
    Returns (decision, reason) -- 'ADD', 'HOLD', 'BLOCKED'."""
    # D0-1: 전역 ADD 차단 (ff_global_add_block)
    import feature_flags as _ff_add
    if _ff_add.is_enabled('ff_global_add_block'):
        return ('BLOCKED', 'GLOBAL_ADD_BLOCK')

    import safety_manager
    import regime_reader
    direction = 'LONG' if pos_side == 'long' else 'SHORT'
    dominant_side = scores.get('dominant_side', 'LONG')
    if dominant_side != direction:
        return ('HOLD', f'반대방향 신호 ({dominant_side} vs 현재 {direction})')

    # ── Regime params ──
    regime = regime_ctx.get('regime', 'UNKNOWN') if regime_ctx else 'UNKNOWN'
    r_params = regime_reader.get_regime_params(regime, regime_ctx.get('shock_type') if regime_ctx else None)

    # ── v14: ADD interval check ──
    (interval_ok, remaining) = _check_add_interval(cur, r_params)
    if not interval_ok:
        return ('HOLD', f'ADD_COOLDOWN: {remaining}s remaining (min={r_params.get("add_min_interval_sec")}s)')

    # ── v14: ADD count per 30m ──
    (count_ok, add_count, add_limit) = _check_adds_per_30m(cur, r_params)
    if not count_ok:
        return ('HOLD', f'ADD_MAX_30M: {add_count}/{add_limit} reached')

    # ── v14: ADD retest/pullback condition ──
    (retest_ok, retest_reason) = _check_add_retest(cur, pos_side, r_params)
    if not retest_ok:
        return ('HOLD', retest_reason)

    # ── BREAKOUT structure intact check ──
    if regime == 'BREAKOUT' and r_params.get('add_structure_intact_required'):
        (struct_ok, struct_reason) = _check_breakout_structure_intact(cur, pos_side, regime_ctx)
        if not struct_ok:
            return ('HOLD', struct_reason)

    # ── v14.1: same-direction re-entry cooldown ──
    (reentry_ok, reentry_remaining) = _check_same_dir_reentry_cooldown(cur, direction, r_params)
    if not reentry_ok:
        return ('HOLD', f'SAME_DIR_COOLDOWN: {reentry_remaining}s remaining')

    # ── Profit-zone-only ADD (loss-zone averaging forbidden) ──
    add_min_profit = r_params.get('add_min_profit_pct', 0.25)
    try:
        ex = _get_exchange()
        ticker = ex.fetch_ticker(SYMBOL)
        cur_price = float(ticker['last'])
        cur.execute('SELECT avg_entry_price FROM position_state WHERE symbol = %s;', (SYMBOL,))
        entry_row = cur.fetchone()
        avg_entry = float(entry_row[0]) if entry_row and entry_row[0] else None
        if avg_entry and avg_entry > 0 and cur_price:
            if pos_side == 'long':
                upnl_pct = (cur_price - avg_entry) / avg_entry * 100
            else:
                upnl_pct = (avg_entry - cur_price) / avg_entry * 100
            if upnl_pct < add_min_profit:
                return ('HOLD', f'ADD 금지: 수익구간 아님 (uPnL={upnl_pct:.2f}% < {add_min_profit}%)')
    except Exception as e:
        _log(f'ADD profit-zone check error (FAIL-OPEN): {e}')

    relevant_score = scores.get('long_score', 50) if direction == 'LONG' else scores.get('short_score', 50)
    add_threshold = safety_manager.get_add_score_threshold(cur)
    if relevant_score < add_threshold:
        return ('HOLD', f'방향 점수 부족 ({relevant_score} < {add_threshold})')

    cur.execute('SELECT stage FROM position_state WHERE symbol = %s;', (SYMBOL,))
    ps_row = cur.fetchone()
    pyramid_stage = int(ps_row[0]) if ps_row else 0

    # V3 max_stage override (ff_regime_risk_v3 ON → max_stage from risk_v3)
    if _v3_result and 'max_stage' in _v3_result:
        v3_max = _v3_result['max_stage']
        if pyramid_stage >= v3_max:
            return ('BLOCKED', f'V3 max_stage reached ({pyramid_stage}/{v3_max})')

    (ok, reason) = safety_manager.check_pyramid_allowed(cur, pyramid_stage, regime_ctx=regime_ctx)
    if not ok:
        return ('BLOCKED', reason)

    (ok, reason) = safety_manager.check_trade_budget(cur, safety_manager.get_add_slice_pct())
    if not ok:
        return ('BLOCKED', reason)

    add_usdt = safety_manager.get_add_slice_usdt(cur)
    (ok, reason) = safety_manager.check_total_exposure(cur, add_usdt)
    if not ok:
        return ('BLOCKED', reason)

    return ('ADD', '모든 조건 충족')


def _create_autopilot_signal(cur=None, side=None, scores=None, equity_limits=None, regime_ctx=None):
    '''Create OPEN signal for autopilot. Returns signal_id.'''
    if SYMBOL not in ALLOWED_SYMBOLS:
        _log(f'SYMBOL_NOT_ALLOWED: {SYMBOL} — signal skipped')
        return 0
    import datetime
    import safety_manager
    now = datetime.datetime.now(datetime.timezone.utc)
    ts_text = now.strftime('%Y-%m-%d %H:%M:%S+00')
    now_unix = int(time.time())
    # v3: FORCE start_stage=1 — 추격 시작 방지
    start_stage = 1
    # Regime-based stage cap (reuse regime_ctx from caller)
    import regime_reader
    if regime_ctx is None:
        regime_ctx = regime_reader.get_current_regime(cur)
    regime_stage_max = regime_reader.get_stage_limit(regime_ctx['regime'], regime_ctx.get('shock_type'))
    start_stage = min(start_stage, regime_stage_max)
    if start_stage <= 0:
        _log(f'REGIME 차단: stage_limit=0 ({regime_ctx["regime"]}/{regime_ctx.get("shock_type")})')
        return 0
    entry_pct = safety_manager.get_stage_entry_pct(start_stage)
    eq = equity_limits or safety_manager.get_equity_limits(cur)
    usdt_amount = eq['slice_usdt'] * start_stage
    usdt_amount = min(usdt_amount, eq['operating_cap'])
    if usdt_amount < 5:
        return 0
    # Determine regime_tag for execution tracking
    _regime_tag = None
    if _v3_result:
        _regime_tag = _v3_result.get('regime_class')
    elif regime_ctx and regime_ctx.get('available'):
        _regime_tag = regime_ctx.get('regime', 'UNKNOWN')
    meta = {
        'direction': side,
        'qty': usdt_amount,
        'dry_run': False,
        'source': 'autopilot',
        'reason': 'autopilot_auto_entry',
        'long_score': scores.get('long_score'),
        'short_score': scores.get('short_score'),
        'confidence': scores.get('confidence'),
        'start_stage': start_stage,
        'entry_pct': entry_pct,
        'regime_tag': _regime_tag}
    cur.execute("""
        INSERT INTO signals_action_v3 (
            ts, symbol, tf, strategy, signal, action, price, meta, created_at_unix, processed, stage
        ) VALUES (
            %s, %s, %s, %s, %s, %s, NULL, %s::jsonb, %s, false, 'SIGNAL_CREATED'
        ) RETURNING id;
    """, (ts_text, SYMBOL, 'auto', 'AUTOPILOT', 'AUTO_ENTRY', 'OPEN',
          json.dumps(meta, ensure_ascii=False, default=str), now_unix))
    row = cur.fetchone()
    signal_id = row[0] if row else 0
    # Set plan_state to PLAN.INTENT_ENTER
    if signal_id:
        try:
            cur.execute("""
                UPDATE position_state SET
                    plan_state = 'PLAN.INTENT_ENTER',
                    state_changed_at = now()
                WHERE symbol = %s;
            """, (SYMBOL,))
        except Exception as e:
            _log(f'plan_state INTENT_ENTER error: {e}')
    return signal_id


def _log_trade_process(cur, signal_id=None, scores=None, source='autopilot',
                        start_stage=None, entry_pct=None, equity_limits=None,
                        v3_result=None):
    '''Insert trade_process_log entry.'''
    if equity_limits is None:
        import safety_manager
        equity_limits = safety_manager.get_equity_limits(cur)
    eq = equity_limits
    context = dict(scores.get('context', {}))
    # V3: inject regime snapshot + comparison into decision_context
    if v3_result:
        context['v3_regime_class'] = v3_result.get('regime_class')
        context['v3_entry_mode'] = v3_result.get('entry_mode')
        context['v3_score_modifier'] = v3_result.get('score_modifier')
        context['v3_reasoning'] = v3_result.get('reasoning', [])[:3]
        context['v3_sl_pct'] = v3_result.get('sl_pct')
        context['v3_tp_pct'] = v3_result.get('tp_pct')
        context['v3_pre_total_score'] = v3_result.get('pre_v3_total_score')
        context['v3_post_total_score'] = v3_result.get('post_v3_total_score')
        context['v3_pre_dominant'] = v3_result.get('pre_v3_dominant')
    try:
        cur.execute("""
            INSERT INTO trade_process_log
                (signal_id, decision_context, long_score, short_score,
                 chosen_side, size_percent, capital_limit, source,
                 chosen_start_stage, entry_size_pct)
            VALUES (%s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            signal_id,
            json.dumps(context, default=str, ensure_ascii=False),
            scores.get('long_score'),
            scores.get('short_score'),
            scores.get('dominant_side'),
            DEFAULT_SIZE_PCT,
            eq['operating_cap'],
            source,
            start_stage,
            entry_pct,
        ))
    except Exception:
        traceback.print_exc()


def _ensure_v2_migrations(cur):
    """Run strategy v2 DB migrations once."""
    global _v2_migrations_done
    if _v2_migrations_done:
        return
    try:
        from strategy.migrations import run_migrations
        run_migrations(cur)
        _v2_migrations_done = True
    except Exception as e:
        _log(f'v2 migrations error (non-fatal): {e}')
        _v2_migrations_done = True  # don't retry every cycle


def _run_strategy_v2(cur, scores, regime_ctx):
    """Run strategy v2 routing and decision logic.

    In 'shadow' mode: log decision but don't execute.
    In 'on' mode: decision will be used by caller (future integration).
    """
    _ensure_v2_migrations(cur)

    from strategy.common.features import build_feature_snapshot
    from strategy.regime_router import route, get_mode_config
    from strategy.common.dedupe import is_duplicate, record_signal
    from strategy.modes.static_range import StaticRangeStrategy
    from strategy.modes.volatile_range import VolatileRangeStrategy
    from strategy.modes.shock_breakout import ShockBreakoutStrategy

    mode_strategies = {
        'A': StaticRangeStrategy(),
        'B': VolatileRangeStrategy(),
        'C': ShockBreakoutStrategy(),
    }

    # 1. Build feature snapshot
    features = build_feature_snapshot(cur, SYMBOL)

    # 2. Get gate/throttle status
    import order_throttle
    throttle_status = order_throttle.get_throttle_status(cur)
    gate_blocked = throttle_status.get('entry_locked', False)
    gate_status = 'BLOCKED' if gate_blocked else 'OPEN'

    # 3. Get current position
    cur.execute('SELECT side, total_qty, avg_entry_price, stage FROM position_state WHERE symbol = %s;', (SYMBOL,))
    ps_row = cur.fetchone()
    position = None
    if ps_row and ps_row[0] and float(ps_row[1] or 0) > 0:
        position = {
            'side': ps_row[0].upper(),
            'total_qty': float(ps_row[1]),
            'avg_entry_price': float(ps_row[2]) if ps_row[2] else 0,
            'stage': int(ps_row[3]) if ps_row[3] else 1,
        }

    # 4. Route to mode
    route_result = route(features, gate_status=gate_status, current_position=position)
    mode = route_result['mode']
    drift_submode = route_result.get('drift_submode')
    mode_config = get_mode_config(mode)

    # 5. Fetch candles and indicators for context
    candles = []
    indicators = {}
    try:
        cur.execute("""
            SELECT ts, o, h, l, c, v FROM candles
            WHERE symbol = %s AND tf = '1m'
            ORDER BY ts DESC LIMIT 10
        """, (SYMBOL,))
        for row in cur.fetchall():
            if any(v is None for v in row[1:6]):
                continue  # skip candles with NULL OHLCV
            candles.append({
                'ts': row[0], 'o': float(row[1]), 'h': float(row[2]),
                'l': float(row[3]), 'c': float(row[4]), 'v': float(row[5]),
            })
    except Exception as e:
        _log(f'candle fetch error (non-fatal): {e}')

    try:
        cur.execute("""
            SELECT atr_14, rsi_14, bb_mid, bb_up, bb_dn, vwap
            FROM indicators WHERE symbol = %s AND tf = '1m'
            ORDER BY ts DESC LIMIT 1
        """, (SYMBOL,))
        ind_row = cur.fetchone()
        if ind_row:
            indicators = {
                'atr_14': float(ind_row[0]) if ind_row[0] else None,
                'rsi_14': float(ind_row[1]) if ind_row[1] else None,
                'bb_mid': float(ind_row[2]) if ind_row[2] else None,
                'bb_up': float(ind_row[3]) if ind_row[3] else None,
                'bb_dn': float(ind_row[4]) if ind_row[4] else None,
                'vwap': float(ind_row[5]) if ind_row[5] else None,
            }
    except Exception as e:
        _log(f'indicator fetch error (non-fatal): {e}')

    # 6. Build context and call mode strategy
    ctx = {
        'features': features,
        'position': position,
        'regime_ctx': regime_ctx,
        'config': mode_config,
        'price': features.get('price'),
        'indicators': indicators,
        'vol_profile': {
            'poc': features.get('poc'),
            'vah': features.get('vah'),
            'val': features.get('val'),
        },
        'candles': candles,
        'drift_submode': drift_submode,
    }

    strategy = mode_strategies.get(mode)
    if not strategy:
        _log(f'v2: unknown mode {mode}')
        return

    decision = strategy.decide(ctx)

    # 7. Check dedupe
    dedupe_hit = False
    signal_key = decision.get('signal_key')
    if signal_key and decision['action'] in ('ENTER', 'ADD'):
        dedupe_hit = is_duplicate(cur, signal_key)
        if not dedupe_hit:
            record_signal(cur, signal_key)

    # 8. Detect chase entry
    chase_entry = decision.get('chase_entry', False)
    if not chase_entry and decision['action'] == 'ENTER':
        impulse = features.get('impulse')
        if impulse is not None and impulse > 0.8:
            chase_entry = True

    # 9. Log decision to strategy_decision_log
    try:
        cur.execute("""
            INSERT INTO strategy_decision_log
                (symbol, mode, submode, features, action, side, qty, tp, sl,
                 gate_status, throttle_status, dedupe_hit, chase_entry,
                 reasons, signal_key)
            VALUES (%s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s)
        """, (
            SYMBOL, mode, drift_submode or 'none',
            json.dumps(features, default=str),
            decision['action'], decision.get('side'),
            decision.get('qty'), decision.get('tp'), decision.get('sl'),
            gate_status, json.dumps(throttle_status, default=str),
            dedupe_hit, chase_entry,
            route_result.get('reasons', []),
            signal_key,
        ))
    except Exception as e:
        _log(f'v2 decision log error (non-fatal): {e}')

    action = decision['action']
    _log(f'v2[{STRATEGY_V2_ENABLED}]: mode={mode} action={action} '
         f'side={decision.get("side")} '
         f'reason={decision.get("reason", "")[:80]}')

    # In 'shadow' mode, don't execute — just logged above
    if STRATEGY_V2_ENABLED != 'on':
        return None

    # ── 'on' mode: execute v2 decisions ──
    if action == 'HOLD':
        return 'HOLD'  # Caller will skip old logic

    side = decision.get('side', '').upper()
    reason = decision.get('reason', '')
    meta = decision.get('meta') or {}
    meta['strategy_v2'] = True
    meta['mode'] = mode
    meta['drift_submode'] = drift_submode
    # regime_tag for execution_log tracking
    if _v3_result:
        meta['regime_tag'] = _v3_result.get('regime_class')
    elif regime_ctx and regime_ctx.get('available'):
        meta['regime_tag'] = regime_ctx.get('regime', 'UNKNOWN')

    if action == 'EXIT' and position:
        # Enqueue CLOSE via execution_queue
        direction = position['side'].upper()
        # Duplicate check: don't enqueue if CLOSE already pending
        cur.execute("""
            SELECT id FROM execution_queue
            WHERE symbol = %s AND action_type = 'CLOSE' AND direction = %s
              AND status IN ('PENDING', 'PICKED', 'SENT')
              AND ts >= now() - interval '5 minutes';
        """, (SYMBOL, direction))
        if cur.fetchone():
            _log('v2 EXIT skipped: CLOSE already pending in queue')
            return 'HOLD'
        cur.execute("""
            INSERT INTO execution_queue
                (symbol, action_type, direction, source, reason, priority,
                 expire_at, meta)
            VALUES (%s, 'CLOSE', %s, 'autopilot_v2', %s, 2,
                    now() + interval '5 minutes', %s::jsonb)
            RETURNING id;
        """, (SYMBOL, direction, reason, json.dumps(meta, default=str)))
        eq_row = cur.fetchone()
        eq_id = eq_row[0] if eq_row else None
        _log(f'v2 EXIT enqueued: CLOSE {direction} eq_id={eq_id}')
        _notify_telegram(
            f'🔴 V2 EXIT: {direction} → CLOSE\n'
            f'mode={mode} reason={reason[:60]}')
        return 'ACTED'

    if action == 'ENTER' and not position:
        # Enqueue via signals_action_v3 (same as old flow)
        if dedupe_hit:
            _log('v2 ENTER skipped: dedupe hit')
            return 'HOLD'
        if gate_status == 'BLOCKED':
            _log('v2 ENTER skipped: gate BLOCKED')
            return 'HOLD'

        import safety_manager
        eq = safety_manager.get_equity_limits(cur)
        usdt_amount = eq.get('slice_usdt', 0)
        usdt_amount = min(usdt_amount, eq.get('operating_cap', 0))
        if usdt_amount < 5:
            _log('v2 ENTER skipped: usdt_amount < 5')
            return 'HOLD'

        leverage = meta.get('leverage', 3)
        size_multiplier = meta.get('size_multiplier')
        if size_multiplier is not None and 0 < size_multiplier < 1:
            usdt_amount = usdt_amount * size_multiplier
            _log(f'v2 ENTER size_multiplier={size_multiplier} applied: usdt={usdt_amount:.0f}')
        entry_meta = {
            'direction': side,
            'qty': usdt_amount,
            'dry_run': False,
            'source': 'autopilot_v2',
            'reason': reason,
            'strategy_v2': True,
            'mode': mode,
            'leverage': leverage,
            'tp': decision.get('tp'),
            'sl': decision.get('sl'),
            'order_type': decision.get('order_type', 'market'),
            'size_multiplier': size_multiplier,
        }
        now_unix = int(time.time())
        ts_text = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(now_unix))
        cur.execute("""
            INSERT INTO signals_action_v3 (
                ts, symbol, tf, strategy, signal, action, price, meta, created_at_unix, processed, stage
            ) VALUES (
                %s, %s, %s, %s, %s, %s, NULL, %s::jsonb, %s, false, 'SIGNAL_CREATED'
            ) RETURNING id;
        """, (ts_text, SYMBOL, 'auto', 'AUTOPILOT_V2', 'AUTO_ENTRY', 'OPEN',
              json.dumps(entry_meta, ensure_ascii=False, default=str), now_unix))
        row = cur.fetchone()
        signal_id = row[0] if row else None
        # Set plan_state to PLAN.INTENT_ENTER
        if signal_id:
            try:
                cur.execute("""
                    UPDATE position_state SET
                        plan_state = 'PLAN.INTENT_ENTER',
                        state_changed_at = now()
                    WHERE symbol = %s;
                """, (SYMBOL,))
            except Exception as e:
                _log(f'plan_state INTENT_ENTER error: {e}')
        _log(f'v2 ENTER signal: {side} signal_id={signal_id} usdt={usdt_amount}')
        _notify_telegram(
            f'🟢 V2 ENTER: {side}\n'
            f'mode={mode} tp={decision.get("tp")} sl={decision.get("sl")}\n'
            f'reason={reason[:60]}')
        return 'ACTED'

    if action == 'ADD' and position:
        # D0-1: Global ADD block (v2 path) — fail-CLOSED on error
        try:
            import feature_flags as _ff_v2add
            if _ff_v2add.is_enabled('ff_global_add_block'):
                _log('v2 ADD blocked: GLOBAL_ADD_BLOCK')
                return 'HOLD'
        except Exception as _ff_err:
            _log(f'v2 ADD blocked: ff_global_add_block check error (fail-closed): {_ff_err}')
            return 'HOLD'
        if dedupe_hit:
            _log('v2 ADD skipped: dedupe hit')
            return 'HOLD'
        if gate_status == 'BLOCKED':
            _log('v2 ADD skipped: gate BLOCKED')
            return 'HOLD'

        direction = side or position['side'].upper()
        # Duplicate check
        cur.execute("""
            SELECT id FROM execution_queue
            WHERE symbol = %s AND action_type = 'ADD' AND direction = %s
              AND status IN ('PENDING', 'PICKED')
              AND ts >= now() - interval '5 minutes';
        """, (SYMBOL, direction))
        if cur.fetchone():
            _log('v2 ADD skipped: duplicate pending in queue')
            return 'HOLD'

        import safety_manager
        add_usdt = safety_manager.get_add_slice_usdt(cur)
        (ok, safety_reason) = safety_manager.run_all_checks(cur, add_usdt)
        if not ok:
            _log(f'v2 ADD safety block: {safety_reason}')
            return 'HOLD'

        cur.execute("""
            INSERT INTO execution_queue
                (symbol, action_type, direction, target_usdt,
                 source, reason, priority, expire_at, meta)
            VALUES (%s, 'ADD', %s, %s,
                    'autopilot_v2', %s, 4, now() + interval '5 minutes', %s::jsonb)
            RETURNING id;
        """, (SYMBOL, direction, add_usdt, reason, json.dumps(meta, default=str)))
        eq_row = cur.fetchone()
        eq_id = eq_row[0] if eq_row else None
        _log(f'v2 ADD enqueued: {direction} usdt={add_usdt} eq_id={eq_id}')
        return 'ACTED'

    return None


def _cycle():
    conn = None
    try:
        conn = _db_conn()
        conn.autocommit = True
        with conn.cursor() as cur:
            if not _check_autopilot_enabled(cur):
                return

            (ok, reason) = _risk_checks(cur)
            if not ok:
                if 'trade_switch' in reason.lower():
                    # Auto-recovery attempt
                    import trade_switch_recovery
                    recovered, rec_reason = trade_switch_recovery.try_auto_recover(cur)
                    if recovered:
                        _log(f'AUTO-RECOVERED: {rec_reason}')
                        _notify_telegram('✅ trade_switch 자동 복구 — gate PASS 확인')
                        # Skip this cycle, proceed normally next cycle
                    else:
                        is_transition = _db_check_trade_switch_transition(cur)
                        if is_transition:
                            _log('⚠ trade_switch OFF 전환 감지')
                            _notify_telegram(
                                f'⚠ trade_switch OFF 전환\n'
                                f'autopilot 매매 일시 중지됨\n'
                                f'복구 상태: {rec_reason}')
                else:
                    # Non-trade_switch reasons: DB-based log dedup
                    should_log, summary = _db_should_log_risk(cur, reason)
                    if should_log:
                        _log(f'INFO: risk check skipped: {reason}{summary}')
                return

            # Risk checks passed → trade_switch is ON: reset transition state
            _db_reset_trade_switch(cur)

            # WAIT_ORDER_FILL: block signal creation while pending orders exist
            try:
                import exchange_reader
                wr, wd = exchange_reader.compute_wait_reason(cur=cur)
                if wr == 'WAIT_ORDER_FILL':
                    _log(f'WAIT_ORDER_FILL: signal creation blocked ({wd})')
                    return
            except Exception as e:
                _log(f'WAIT_ORDER_FILL check failed (FAIL-OPEN): {e}')

            import direction_scorer
            scores = direction_scorer.compute_scores()
            confidence = scores.get('confidence', 0)
            dominant = scores.get('dominant_side', 'LONG')

            _log(f'scores: L={scores.get("long_score")} S={scores.get("short_score")} '
                 f'conf={confidence} side={dominant}')

            # Regime check: SHOCK/VETO blocks new entries
            import regime_reader
            regime_ctx = regime_reader.get_current_regime(cur)
            if regime_ctx['available'] and regime_ctx['regime'] == 'SHOCK' and regime_ctx.get('shock_type') == 'VETO':
                _log(f'REGIME VETO: 진입 차단 (flow_bias={regime_ctx.get("flow_bias")})')
                return

            # ── STRATEGY V3: regime switching + chase suppression ──
            global _v3_result, _v3_features
            _v3_result = None
            _v3_features = None
            _mtf_data = None  # [1-5] MTF data placeholder
            if _is_v3_enabled():
                try:
                    from strategy.common.features import build_feature_snapshot
                    from strategy_v3.regime_v3 import classify as v3_classify
                    from strategy_v3.score_v3 import compute_modifier as v3_score_mod
                    from strategy_v3.risk_v3 import compute_risk as v3_risk

                    v3_features = build_feature_snapshot(cur)
                    _v3_features = v3_features  # cache for reuse in debounce
                    v3_regime = v3_classify(v3_features, regime_ctx)

                    v3_price = v3_features.get('price', 0) if v3_features else 0
                    total_score = scores.get('unified', {}).get('total_score', 0) if scores.get('unified') else 0
                    # Fallback: derive total_score from long_score
                    if total_score == 0:
                        ls = scores.get('long_score', 50)
                        total_score = (ls - 50) * 2

                    # ── V3 comparison snapshot (pre-modification) ──
                    pre_v3_total_score = total_score
                    pre_v3_dominant = dominant

                    v3_mod = v3_score_mod(total_score, v3_features, v3_regime, v3_price,
                                          regime_ctx=regime_ctx)

                    if v3_mod.get('entry_blocked'):
                        _log(f'[V3] BLOCKED: {v3_mod.get("block_reason", "")} '
                             f'(regime={v3_regime.get("regime_class", "?")} mode={v3_regime.get("entry_mode", "?")})')
                        return

                    # [1-5] MTF Direction Gate (ff_unified_engine_v11)
                    try:
                        import feature_flags as _ff_mtf
                        if _ff_mtf.is_enabled('ff_unified_engine_v11'):
                            from mtf_direction import compute_mtf_direction, NO_TRADE, LONG_ONLY, SHORT_ONLY
                            _mtf_data = compute_mtf_direction(cur)

                            # TOP-LEVEL GATE
                            if _mtf_data['direction'] == NO_TRADE:
                                _log(f'[MTF] NO_TRADE: {_mtf_data["reasons"]}')
                                return

                            # Direction filter
                            if _mtf_data['direction'] == LONG_ONLY and dominant == 'SHORT':
                                _log('[MTF] LONG_ONLY but signal SHORT — blocked')
                                return
                            if _mtf_data['direction'] == SHORT_ONLY and dominant == 'LONG':
                                _log('[MTF] SHORT_ONLY but signal LONG — blocked')
                                return

                            # Inject MTF direction into regime result
                            if v3_regime:
                                v3_regime['mtf_direction'] = _mtf_data['direction']
                    except Exception as e:
                        _log(f'[MTF] error (FAIL-OPEN): {e}')

                    # Mode cooloff check
                    cooloff_ok, cooloff_reason = _check_mode_cooloff(cur, v3_regime.get('regime_class'))
                    if not cooloff_ok:
                        _log(f'[V3] {cooloff_reason}')
                        return

                    # Apply score modification
                    total_score = max(-100, min(100, total_score + v3_mod['modifier']))
                    new_long_score = int(max(0, min(100, 50 + total_score / 2)))
                    new_short_score = 100 - new_long_score
                    new_confidence = abs(new_long_score - new_short_score)
                    new_dominant = 'LONG' if new_long_score >= new_short_score else 'SHORT'

                    # Override scores
                    scores = dict(scores)
                    scores['long_score'] = new_long_score
                    scores['short_score'] = new_short_score
                    scores['confidence'] = new_confidence
                    scores['dominant_side'] = new_dominant
                    confidence = new_confidence
                    dominant = new_dominant

                    # Compute risk overrides
                    streak = _get_current_loss_streak(cur)
                    v3_risk_params = v3_risk(v3_features, v3_regime, loss_streak=streak)

                    # ── ADAPTIVE LAYERS: L1/L4/L5 (entry gate + penalty) ──
                    _adaptive_result = None
                    try:
                        import feature_flags
                        if feature_flags.is_enabled('ff_adaptive_layers'):
                            from strategy_v3.adaptive_v3 import apply_adaptive_layers
                            from strategy_v3 import compute_market_health
                            _health = compute_market_health(v3_features or {})
                            _entry_mode = v3_regime.get('entry_mode', 'MeanRev')
                            _adaptive_result = apply_adaptive_layers(
                                cur, _entry_mode, new_dominant,
                                v3_regime.get('regime_class', 'STATIC_RANGE'),
                                v3_features, regime_ctx, _health)

                            _is_dryrun = _adaptive_result.get('dryrun', True)

                            if not _is_dryrun:
                                # L4: WARN entry block
                                if _adaptive_result.get('l4_entry_blocked'):
                                    _log('[L4] health=WARN entry blocked')
                                    return

                                # L1: mode cooldown block
                                if _adaptive_result.get('l1_cooldown_active'):
                                    _log(f'[L1] {_entry_mode} cooldown '
                                         f'{_adaptive_result.get("l1_cooldown_remaining", 0)}s')
                                    return

                                # L1: global WR block
                                if _adaptive_result.get('l1_global_wr_block'):
                                    effective_min = MIN_CONFIDENCE + _adaptive_result.get('l1_effective_threshold_add', 0)
                                    if new_confidence < effective_min:
                                        _log(f'[L1] global WR block: conf={new_confidence} < {effective_min}')
                                        return

                                # Apply combined penalty to total_score
                                penalty = _adaptive_result.get('combined_penalty', 1.0)
                                if penalty < 1.0:
                                    total_score = total_score * penalty
                                    total_score = max(-100, min(100, total_score))
                                    new_long_score = int(max(0, min(100, 50 + total_score / 2)))
                                    new_short_score = 100 - new_long_score
                                    new_confidence = abs(new_long_score - new_short_score)
                                    new_dominant = 'LONG' if new_long_score >= new_short_score else 'SHORT'
                                    scores = dict(scores)
                                    scores['long_score'] = new_long_score
                                    scores['short_score'] = new_short_score
                                    scores['confidence'] = new_confidence
                                    scores['dominant_side'] = new_dominant
                                    confidence = new_confidence
                                    dominant = new_dominant
                                    _log(f'[ADAPTIVE] penalty={penalty:.2f} → score={total_score:+.0f} '
                                         f'conf={new_confidence}')

                                # Propagate L4 time_stop/trailing to _v3_result
                                if _adaptive_result.get('l4_time_stop_mult', 1.0) < 1.0:
                                    v3_risk_params['l4_time_stop_mult'] = _adaptive_result['l4_time_stop_mult']
                                if _adaptive_result.get('l4_trailing_sensitive'):
                                    v3_risk_params['l4_trailing_sensitive'] = True
                    except Exception as e:
                        _log(f'[ADAPTIVE] error (FAIL-OPEN): {e}')

                    # Store V3 result for signal metadata (including comparison fields)
                    _v3_result = {
                        'regime_class': v3_regime.get('regime_class', 'STATIC_RANGE'),
                        'entry_mode': v3_regime.get('entry_mode', 'MeanRev'),
                        'raw_class': v3_regime.get('raw_class'),
                        'v3_confidence': v3_regime.get('confidence', 0),
                        'score_modifier': v3_mod.get('modifier', 0),
                        'reasoning': v3_mod.get('reasoning', [])[:5],
                        'sl_pct': v3_risk_params.get('sl_pct', 0.006),
                        'tp_pct': v3_risk_params.get('tp_pct', 0.0072),
                        'stage_slice_mult': v3_risk_params.get('stage_slice_mult', 1.0),
                        'pre_v3_total_score': pre_v3_total_score,
                        'post_v3_total_score': total_score,
                        'pre_v3_dominant': pre_v3_dominant,
                    }
                    # Propagate max_stage from regime risk v3
                    if 'max_stage' in v3_risk_params:
                        _v3_result['max_stage'] = v3_risk_params['max_stage']
                    # Propagate L4 params
                    if 'l4_time_stop_mult' in v3_risk_params:
                        _v3_result['l4_time_stop_mult'] = v3_risk_params['l4_time_stop_mult']
                    if v3_risk_params.get('l4_trailing_sensitive'):
                        _v3_result['l4_trailing_sensitive'] = True
                    # Propagate adaptive debug
                    if _adaptive_result:
                        _v3_result['adaptive'] = _adaptive_result.get('debug', {})
                        _v3_result['adaptive_penalty'] = _adaptive_result.get('combined_penalty', 1.0)

                    _log(f'[V3] regime={v3_regime.get("regime_class", "?")} '
                         f'pre={pre_v3_total_score:+.0f} mod={v3_mod.get("modifier", 0):+.0f} '
                         f'post={total_score:+.0f} side={new_dominant}')
                except Exception as e:
                    _log(f'[V3] error (FAIL-OPEN, using original scores): {e}')
                    _v3_result = None

            # ── STRATEGY V2: early gate check + 3-mode routing ──
            if STRATEGY_V2_ENABLED != 'off':
                v2_result = None
                try:
                    v2_result = _run_strategy_v2(cur, scores, regime_ctx)
                except Exception as e:
                    _log(f'strategy_v2 error (non-fatal): {e}')

                # In 'on' mode: v2 decision replaces old logic
                # Exception: V3 DRIFT regimes override V2 HOLD (drift-following > range no-trade zone)
                if STRATEGY_V2_ENABLED == 'on' and v2_result in ('HOLD', 'ACTED'):
                    v3_drift_override = (
                        _v3_result
                        and _v3_result.get('regime_class') in ('DRIFT_UP', 'DRIFT_DOWN')
                        and v2_result == 'HOLD'
                    )
                    if v3_drift_override:
                        _log(f'[V3] V2 HOLD overridden: regime={_v3_result["regime_class"]} drift takes priority')
                    else:
                        return  # v2 handled this cycle

                # EARLY GATE CHECK — before any signal generation
                try:
                    import order_throttle
                    blocked, block_reason, _ = order_throttle.is_entry_blocked(cur)
                    if blocked:
                        _log(f'GATE_BLOCKED: skipping cycle — {block_reason}')
                        return  # No signal generation, no spam
                except Exception as e:
                    _log(f'early gate check FAIL-OPEN: {e}')

            # RECONCILE MISMATCH: block new entries when exchange/strategy disagree
            try:
                import exchange_reader
                exch_data = exchange_reader.fetch_position()
                strat_data = exchange_reader.fetch_position_strat()
                recon_result = exchange_reader.reconcile(exch_data, strat_data)
                recon_status = recon_result['legacy'] if isinstance(recon_result, dict) else recon_result
                if recon_status == 'MISMATCH':
                    _log(f'RECONCILE MISMATCH: entries blocked — {recon_result.get("detail", "") if isinstance(recon_result, dict) else ""}')
                    return
            except Exception as e:
                _log(f'RECONCILE check error (FAIL-OPEN): {e}')

            # Check existing position
            cur.execute('SELECT side, total_qty FROM position_state WHERE symbol = %s;', (SYMBOL,))
            ps_row = cur.fetchone()
            has_position = ps_row and ps_row[0] and float(ps_row[1] or 0) > 0

            # Regime entry filter (new entries only)
            # Skip V1 RANGE filters when V3 identifies DRIFT regime
            _v3_is_drift = (_v3_result and _v3_result.get('regime_class') in ('DRIFT_UP', 'DRIFT_DOWN'))
            if not has_position:
                if _v3_is_drift:
                    _log(f'[V3] V1 RANGE filters skipped: regime={_v3_result["regime_class"]}')
                else:
                    entry_ok, entry_reason = _check_regime_entry_filter(cur, regime_ctx, scores)
                    if not entry_ok:
                        _log(f'REGIME_FILTER: {entry_reason}')
                        return

                    # RANGE anti-chase filter
                    if regime_ctx.get('regime') == 'RANGE':
                        chase_ok, chase_reason = _check_anti_chase(cur, regime_ctx)
                        if not chase_ok:
                            _log(f'ANTI_CHASE: {chase_reason}')
                            return

                # Post-close cooldowns (TP→3min, SL→10min)
                pc_ok, pc_reason, _ = _check_post_close_cooldown(cur, SYMBOL)
                if not pc_ok:
                    _log(f'POST_CLOSE_COOLDOWN: {pc_reason}')
                    return

                # Zone re-entry ban (SL same zone)
                zr_ok, zr_reason = _check_zone_reentry_ban(cur, SYMBOL, dominant, regime_ctx)
                if not zr_ok:
                    _log(f'ZONE_REENTRY: {zr_reason}')
                    return

                # Post-SL opposite direction ban
                opp_ok, opp_reason = _check_post_sl_opposite_ban(cur, SYMBOL)
                if not opp_ok:
                    _log(f'POST_SL_BAN: {opp_reason}')
                    return

                # RANGE hourly entry cap
                r_params = regime_reader.get_regime_params(
                    regime_ctx.get('regime', 'UNKNOWN'), regime_ctx.get('shock_type'))
                if regime_ctx.get('regime') == 'RANGE':
                    max_per_hour = r_params.get('max_entries_per_hour', 3)
                    cur.execute("""
                        SELECT count(*) FROM execution_log
                        WHERE symbol = %s AND order_type = 'OPEN' AND status = 'FILLED'
                          AND last_fill_at >= now() - interval '1 hour';
                    """, (SYMBOL,))
                    hourly_count = cur.fetchone()[0] or 0
                    if hourly_count >= max_per_hour:
                        _log(f'RANGE 시간당 진입 제한 ({hourly_count}/{max_per_hour})')
                        return

                # Consecutive same-direction loss cooldown
                (cc_ok, cc_reason) = _check_consec_loss_cooldown(cur, dominant, r_params)
                if not cc_ok:
                    _log(f'CONSEC_LOSS: {cc_reason}')
                    return

            if has_position:
                # v3: conf < CONF_ADD_THRESHOLD → ADD 금지
                if confidence < CONF_ADD_THRESHOLD:
                    _log(f'ADD blocked: conf too low for ADD ({confidence} < {CONF_ADD_THRESHOLD})')
                    return

                # L3/L4: Adaptive ADD gate (before existing checks)
                try:
                    import feature_flags as _ff
                    if _ff.is_enabled('ff_adaptive_layers'):
                        from strategy_v3.adaptive_v3 import apply_adaptive_add_gate, _is_dryrun as _adp_dryrun
                        from strategy_v3 import compute_market_health as _cmh
                        _add_health = _cmh(_v3_features or {})
                        _add_gate = apply_adaptive_add_gate(
                            cur, ps_row[0], _v3_features or {}, _add_health)
                        if not _adp_dryrun():
                            if _add_gate.get('l4_add_blocked'):
                                _log('[L4] health=WARN ADD blocked')
                                return
                            if _add_gate.get('l3_add_blocked'):
                                _log(f'[L3] {_add_gate.get("l3_add_reason", "ADD blocked")}')
                                return
                        else:
                            if _add_gate.get('l4_add_blocked'):
                                _log('[L4_DRYRUN] health=WARN ADD would be blocked')
                            if _add_gate.get('l3_add_blocked'):
                                _log(f'[L3_DRYRUN] {_add_gate.get("l3_add_reason", "")}')
                except Exception as e:
                    _log(f'[ADAPTIVE] ADD gate FAIL-OPEN: {e}')

                # v14: order_throttle pre-gate — check BEFORE any ADD attempt
                try:
                    import order_throttle
                    regime = regime_ctx.get('regime', 'UNKNOWN') if regime_ctx else 'UNKNOWN'
                    direction = 'LONG' if ps_row[0] == 'long' else 'SHORT'
                    (throttle_ok, throttle_reason, throttle_meta) = order_throttle.check_all(
                        cur, 'ADD', SYMBOL, direction, regime)
                    if not throttle_ok:
                        _log(f'ADD_THROTTLE_BLOCKED: {throttle_reason}')
                        return
                except Exception as e:
                    _log(f'ADD throttle pre-gate error (FAIL-OPEN): {e}')

                # Evaluate ADD
                pos_side = ps_row[0]
                pos_qty = float(ps_row[1])
                (decision, add_reason) = _check_position_for_add(cur, pos_side, pos_qty, scores, regime_ctx=regime_ctx)
                if decision == 'ADD':
                    _log(f'ADD decision: {add_reason}')
                    import safety_manager
                    direction = 'LONG' if pos_side == 'long' else 'SHORT'
                    add_usdt = safety_manager.get_add_slice_usdt(cur)
                    # Duplicate check
                    cur.execute("""
                        SELECT id FROM execution_queue
                        WHERE symbol = %s AND action_type = 'ADD' AND direction = %s
                          AND status IN ('PENDING', 'PICKED')
                          AND ts >= now() - interval '5 minutes';
                    """, (SYMBOL, direction))
                    if cur.fetchone():
                        _log('ADD blocked: duplicate pending in queue')
                    else:
                        # ADD price dedup: block ±0.1% same price within 10min
                        global _last_add_price, _last_add_ts, _add_dedup_initialized
                        # Restore dedup state from last ADD in execution_queue on first use
                        if not _add_dedup_initialized:
                            _add_dedup_initialized = True
                            try:
                                cur.execute("""
                                    SELECT target_usdt, extract(epoch from ts) FROM execution_queue
                                    WHERE symbol = %s AND action_type = 'ADD'
                                      AND status NOT IN ('REJECTED', 'EXPIRED')
                                    ORDER BY ts DESC LIMIT 1;
                                """, (SYMBOL,))
                                _prev = cur.fetchone()
                                if _prev and _prev[1]:
                                    # Use mark_price at that time as proxy
                                    cur.execute("SELECT mark_price FROM market_data_cache WHERE symbol = %s;", (SYMBOL,))
                                    _mp = cur.fetchone()
                                    if _mp:
                                        _last_add_price = float(_mp[0])
                                        _last_add_ts = float(_prev[1])
                                        _log(f'ADD_PRICE_DEDUP restored from DB: price={_last_add_price}, ts={_last_add_ts:.0f}')
                            except Exception as _e:
                                _log(f'ADD_PRICE_DEDUP init error (non-fatal): {_e}')
                        try:
                            cur.execute("SELECT mark_price FROM market_data_cache WHERE symbol = %s;", (SYMBOL,))
                            price_row = cur.fetchone()
                            current_price = float(price_row[0]) if price_row else 0
                        except Exception:
                            current_price = 0
                        if current_price > 0 and _last_add_price > 0:
                            diff_pct = abs(current_price - _last_add_price) / _last_add_price * 100
                            elapsed = time.time() - _last_add_ts
                            if diff_pct <= ADD_PRICE_DEDUP_PCT and elapsed < ADD_PRICE_DEDUP_SEC:
                                _log(f'ADD_PRICE_DEDUP: blocked — price={current_price} vs last={_last_add_price} diff={diff_pct:.3f}%')
                                return

                        (ok, reason) = safety_manager.run_all_checks(cur, add_usdt)
                        if not ok:
                            _log(f'ADD safety block: {reason}')
                        else:
                            _add_meta = {}
                            if _v3_result:
                                _add_meta['regime_tag'] = _v3_result.get('regime_class')
                            elif regime_ctx and regime_ctx.get('available'):
                                _add_meta['regime_tag'] = regime_ctx.get('regime', 'UNKNOWN')
                            cur.execute("""
                                INSERT INTO execution_queue
                                    (symbol, action_type, direction, target_usdt,
                                     source, reason, priority, expire_at, meta)
                                VALUES (%s, 'ADD', %s, %s,
                                        'autopilot', %s, 4, now() + interval '5 minutes',
                                        %s::jsonb)
                                RETURNING id;
                            """, (SYMBOL, direction, add_usdt, add_reason,
                                  json.dumps(_add_meta, default=str)))
                            eq_row = cur.fetchone()
                            _log(f'ADD enqueued: eq_id={eq_row[0] if eq_row else None}')
                            _last_add_price = current_price if current_price > 0 else 0
                            _last_add_ts = time.time()
                else:
                    _log(f'position exists, {decision}: {add_reason}')
                    # v14.1: ADD blocked → throttled Telegram notification
                    _notify_telegram_throttled(
                        f'📊 ADD {decision}: {add_reason}', msg_type='add_blocked')
                return

            # D1-1: Shock guard freeze — 신규 진입 차단 (autopilot)
            try:
                import shock_guard as _sg_ap
                _sg_frozen, _sg_remaining = _sg_ap.is_entry_frozen()
                if _sg_frozen:
                    _log(f'SHOCK FREEZE: ENTRY blocked ({_sg_remaining:.0f}s remaining)')
                    return
            except Exception:
                pass  # FAIL-OPEN

            # D2-1: No-trade zone filter (ff_no_trade_zone 뒤에 숨김)
            try:
                import feature_flags as _ff_ntx
                if _ff_ntx.is_enabled('ff_no_trade_zone'):
                    from strategy_v3.regime_v3 import compute_trend_probability, is_no_trade_zone
                    _tp = compute_trend_probability(_v3_features or {})
                    _is_ntx, _ntx_reason = is_no_trade_zone(_tp)
                    if _is_ntx:
                        _log(f'NO_TRADE_ZONE: ENTRY blocked — {_ntx_reason}')
                        return
            except Exception:
                pass  # FAIL-OPEN

            # ── V3: SL cooldown + signal debounce (before emission gate) ──
            if _is_v3_enabled():
                v3_sl_ok, v3_sl_reason = _v3_check_sl_cooldown(cur, SYMBOL, dominant)
                if not v3_sl_ok:
                    _log(f'[V3] {v3_sl_reason}')
                    return

                if _v3_result:
                    v3_db_ok, v3_db_reason = _v3_check_signal_debounce(
                        cur, SYMBOL, _v3_result, dominant, _v3_features)
                    if not v3_db_ok:
                        _log(f'[V3] {v3_db_reason}')
                        return

            # ── v3: Central signal emission gate ──
            (emit_ok, emit_reason) = should_emit_signal(cur, SYMBOL, dominant, confidence, regime_ctx=regime_ctx)
            if not emit_ok:
                should_log, _ = _db_should_log_risk(cur, f'signal_suppress_{emit_reason[:30]}')
                if should_log:
                    _log(f'SIGNAL_SUPPRESSED: {emit_reason}')
                return

            # B1: FORCED hard block — autopilot FORCED entry prevention [0-3]
            try:
                import feature_flags as _ff_forced
                # [0-3] HARD BLOCK: conf<50 in autopilot → always rejected
                if confidence < 50:
                    _log(f'[FORCED_BLOCKED] autopilot에서 conf={confidence} < 50 진입 차단')
                    return
                if not _ff_forced.is_enabled('ff_allow_forced_entry'):
                    _notify_telegram_throttled(
                        f'[Autopilot] FORCED 진입 차단: {dominant} conf={confidence}\n'
                        f'- ff_allow_forced_entry=false',
                        msg_type='forced_rejected', cooldown_sec=600)
                    _log(f'FORCED entry REJECTED: {dominant} conf={confidence} (ff_allow_forced_entry=false)')
                    return
            except Exception as _e_forced:
                _log(f'[FORCED] check error (FAIL-OPEN): {_e_forced}')

            # B2: Signal spam guard (3 signals/30min same direction → 30min cooldown)
            try:
                import feature_flags as _ff_spam
                if _ff_spam.is_enabled('ff_signal_spam_guard'):
                    _spam_ok, _spam_remaining = _check_signal_spam(SYMBOL, dominant)
                    if not _spam_ok:
                        _notify_telegram_throttled(
                            f'[Autopilot] 신호 스팸 차단: {dominant} (cooldown {_spam_remaining}s)\n'
                            f'- 30분 내 3회 이상 동일 방향 신호',
                            msg_type='signal_spam', cooldown_sec=600)
                        _log(f'SIGNAL SPAM blocked: {dominant} (cooldown {_spam_remaining}s)')
                        return
            except Exception:
                pass  # FAIL-OPEN

            # B3: Post-impulse chasing ban
            try:
                _impulse_ok, _impulse_remaining = _check_impulse_ban(cur, dominant)
                if not _impulse_ok:
                    _notify_telegram_throttled(
                        f'[Autopilot] 급변 추격 차단: {dominant} ({_impulse_remaining:.0f}s)\n'
                        f'- 5분 내 1%+ 변동 감지',
                        msg_type='impulse_ban', cooldown_sec=600)
                    _log(f'IMPULSE BAN: {dominant} blocked ({_impulse_remaining:.0f}s remaining)')
                    return
            except Exception:
                pass  # FAIL-OPEN

            import safety_manager
            eq = safety_manager.get_equity_limits(cur)
            # v3: start_stage always 1
            start_stage = 1
            entry_pct = safety_manager.get_stage_entry_pct(start_stage)

            # [0-3] Price reentry validation
            try:
                _cur_price = 0
                cur.execute("SELECT mark_price FROM market_data_cache WHERE symbol = %s;", (SYMBOL,))
                _pr = cur.fetchone()
                if _pr:
                    _cur_price = float(_pr[0])
                if _cur_price > 0:
                    _reentry_ok, _reentry_reason = _is_reentry_valid(SYMBOL, dominant, _cur_price)
                    if not _reentry_ok:
                        _log(f'[REENTRY_BLOCKED] {_reentry_reason}')
                        return
            except Exception as e:
                _log(f'reentry check FAIL-OPEN: {e}')

            # [2-2] Trend-Follow Trigger Evaluation (ff_unified_engine_v11)
            _trigger_type = None
            try:
                import feature_flags as _ff_trend
                if _ff_trend.is_enabled('ff_unified_engine_v11') and _mtf_data:
                    from trend_triggers import evaluate_breakout, evaluate_pullback

                    _bo = evaluate_breakout(cur, SYMBOL, _mtf_data['direction'], _v3_features or {})
                    _pb = evaluate_pullback(cur, SYMBOL, _mtf_data['direction'], _v3_features or {})

                    if _bo['triggered']:
                        _trigger_type = 'DONCHIAN_BREAKOUT'
                        dominant = _bo['side']
                        _log(f'[TRIGGER] Donchian breakout: {_bo["side"]} @ {_bo["trigger_price"]:.1f}')
                    elif _pb['triggered']:
                        _trigger_type = 'EMA_PULLBACK'
                        dominant = _pb['side']
                        _log(f'[TRIGGER] EMA pullback: {_pb["side"]} @ {_pb["trigger_price"]:.1f}')
                    else:
                        _log(f'[TRIGGER] no trigger fired — skipping (bo={_bo["reasons"][:1]}, pb={_pb["reasons"][:1]})')
                        return
            except Exception as e:
                _log(f'[TRIGGER] error (FAIL-OPEN): {e}')

            signal_id = _create_autopilot_signal(cur, dominant, scores, equity_limits=eq, regime_ctx=regime_ctx)
            if signal_id:
                # Record emission for cooldown tracking
                _record_signal_emission(cur, SYMBOL, dominant, regime_ctx=regime_ctx)
                # [0-3] Record signal price for reentry validation
                try:
                    cur.execute("SELECT mark_price FROM market_data_cache WHERE symbol = %s;", (SYMBOL,))
                    _sp = cur.fetchone()
                    if _sp:
                        _record_signal_price(SYMBOL, dominant, float(_sp[0]))
                except Exception:
                    pass
                # B2: Record for spam guard
                try:
                    _record_signal_spam(SYMBOL, dominant)
                except Exception:
                    pass

                # V3: record debounce key after successful signal creation
                if _is_v3_enabled() and _v3_result:
                    try:
                        from strategy.common.dedupe import record_signal, make_v3_signal_key
                        rp = _v3_features.get('range_position') if _v3_features else None
                        rc = _v3_result.get('regime_class', 'UNKNOWN')
                        if rc == 'BREAKOUT':
                            lb = 'BREAKOUT_UP' if dominant == 'LONG' else 'BREAKOUT_DOWN'
                        elif rp is not None:
                            if rp <= 0.20:
                                lb = 'VAL'
                            elif rp >= 0.80:
                                lb = 'VAH'
                            elif 0.40 <= rp <= 0.60:
                                lb = 'POC'
                            else:
                                lb = 'MID'
                        else:
                            lb = 'MID'
                        v3_key = make_v3_signal_key(SYMBOL, rc, dominant, lb)
                        record_signal(cur, v3_key)
                    except Exception as e:
                        _log(f'[V3] debounce record FAIL-OPEN: {e}')

                _log_trade_process(cur, signal_id, scores, 'autopilot', start_stage, entry_pct,
                                    equity_limits=eq, v3_result=_v3_result)
                _notify_telegram(
                    f'[Autopilot] {dominant} 진입 신호 생성\n'
                    f'- L={scores.get("long_score")} S={scores.get("short_score")} conf={confidence}\n'
                    f'- start_stage={start_stage} (FORCED=1) entry={entry_pct}%\n'
                    f'- signal_id={signal_id}'
                    + (f'\n- [V3] {_v3_result["regime_class"]}/{_v3_result["entry_mode"]} '
                       f'mod={_v3_result["score_modifier"]:+.0f}' if _v3_result else ''))
                _log(f'signal created: {dominant} id={signal_id} stage={start_stage} (FORCED=1)')
    except Exception:
        traceback.print_exc()
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def main():
    _log('=== AUTOPILOT DAEMON START ===')
    from watchdog_helper import init_watchdog
    init_watchdog(interval_sec=10)
    _consecutive_errors = 0
    _MAX_CONSECUTIVE_ERRORS = 5

    while True:
        if os.path.exists(KILL_SWITCH_PATH):
            _log('KILL_SWITCH detected. Exiting.')
            sys.exit(0)
        try:
            _cycle()
            _consecutive_errors = 0  # reset on success

            # D3: heartbeat record
            try:
                from db_config import get_conn as _hb_get_conn
                _hb_conn = _hb_get_conn(autocommit=True)
                with _hb_conn.cursor() as _hb_cur:
                    _hb_cur.execute(
                        "INSERT INTO service_health_log (service, state) VALUES ('autopilot_daemon', 'OK');")
                _hb_conn.close()
            except Exception:
                pass
        except Exception:
            _consecutive_errors += 1
            traceback.print_exc()
            if _consecutive_errors >= _MAX_CONSECUTIVE_ERRORS:
                _notify_telegram(f'[autopilot] 연속 {_consecutive_errors}회 에러 — 자동 복구 시도 중')
                _consecutive_errors = 0
        time.sleep(POLL_SEC)


if __name__ == '__main__':
    main()
