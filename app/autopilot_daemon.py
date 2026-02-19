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
LOG_PREFIX = '[autopilot]'
SYMBOL = 'BTC/USDT:USDT'
ALLOWED_SYMBOLS = frozenset({"BTC/USDT:USDT"})
POLL_SEC = 20
COOLDOWN_SEC = 30  # v2.1 중간 공격형
MAX_DAILY_TRADES = 60
MIN_CONFIDENCE = 35  # v3: conf>=35 진입 허용 (35-49: stage1 only, >=50: 기존 로직)
CONF_ADD_THRESHOLD = 50  # conf>=50 이어야 ADD 허용
DEFAULT_SIZE_PCT = 10
REPEAT_SIGNAL_COOLDOWN_SEC = 600  # 동일 방향 재신호 쿨다운 10분
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
ADD_PRICE_DEDUP_PCT = 0.1       # ±0.1% range
ADD_PRICE_DEDUP_SEC = 600       # 10min window


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


def _record_signal_emission(cur, symbol, direction):
    """Record signal emission timestamp in DB for cooldown tracking."""
    key = f'autopilot:signal:{symbol}:{direction}'
    cur.execute("""
        INSERT INTO alert_dedup_state (key, last_sent_ts, suppressed_count)
        VALUES (%s, now(), 0)
        ON CONFLICT (key) DO UPDATE
        SET last_sent_ts = now(), suppressed_count = 0;
    """, (key,))


def _is_in_repeat_cooldown(cur, symbol, direction, cooldown_sec=None):
    """Check if same symbol+direction signal was emitted within cooldown window.
    Returns (in_cooldown: bool, remaining_sec: int)."""
    if cooldown_sec is None:
        cooldown_sec = REPEAT_SIGNAL_COOLDOWN_SEC
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
    except Exception:
        pass
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


def should_emit_signal(cur, symbol, direction, conf):
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

    # 2. Repeat signal cooldown
    (in_cd, remaining) = _is_in_repeat_cooldown(cur, symbol, direction)
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
        snapshot['regime_adx'] = regime_ctx.get('adx_14')
        snapshot['max_stage'] = r_params.get('stage_max', 7)
        snapshot['add_min_interval_sec'] = r_params.get('add_min_interval_sec', 300)
        snapshot['max_adds_per_30m'] = r_params.get('max_adds_per_30m', 3)
        snapshot['add_retest_required'] = r_params.get('add_retest_required', False)
        snapshot['event_add_blocked'] = r_params.get('event_add_blocked', False)

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

    cur.execute("""
        SELECT count(*) FROM trade_process_log
        WHERE source = 'autopilot'
          AND ts >= (now() AT TIME ZONE 'Asia/Seoul')::date AT TIME ZONE 'Asia/Seoul';
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
        # RANGE mode: price must be near VAL/VAH or BB boundary
        proximity = params.get('entry_proximity_pct', 0.3)
        vah = regime_ctx.get('vah')
        val = regime_ctx.get('val')

        if not price:
            try:
                ex = _get_exchange()
                ticker = ex.fetch_ticker(SYMBOL)
                price = float(ticker['last'])
            except Exception:
                return (True, 'price fetch failed — FAIL-OPEN')

        if not vah or not val:
            return (True, 'VAH/VAL unavailable — FAIL-OPEN')

        near_val = abs(price - val) / val * 100 <= proximity if val > 0 else False
        near_vah = abs(price - vah) / vah * 100 <= proximity if vah > 0 else False

        if dominant == 'LONG' and near_val:
            # RSI filter: LONG entry needs RSI oversold
            rsi_5m = _get_rsi_5m(cur)
            rsi_oversold = params.get('rsi_oversold', 30)
            if rsi_5m is not None and rsi_5m > rsi_oversold:
                return (False, f'RANGE LONG: RSI 과매도 미달 (RSI={rsi_5m:.0f} > {rsi_oversold})')
            return (True, f'RANGE LONG: 가격 VAL 근접 ({price:.0f} ≈ {val:.0f})')
        if dominant == 'SHORT' and near_vah:
            # RSI filter: SHORT entry needs RSI overbought
            rsi_5m = _get_rsi_5m(cur)
            rsi_overbought = params.get('rsi_overbought', 70)
            if rsi_5m is not None and rsi_5m < rsi_overbought:
                return (False, f'RANGE SHORT: RSI 과매수 미달 (RSI={rsi_5m:.0f} < {rsi_overbought})')
            return (True, f'RANGE SHORT: 가격 VAH 근접 ({price:.0f} ≈ {vah:.0f})')
        # Also allow if near opposite band (counter-trend at boundary)
        if near_val or near_vah:
            return (True, f'RANGE: 밴드 경계 근접')

        return (False, f'RANGE 모드: 밴드 경계 대기 (VAL={val:.0f} VAH={vah:.0f} price={price:.0f})')

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


def _check_position_for_add(cur=None, pos_side=None, pos_qty=None, scores=None, regime_ctx=None):
    """Evaluate whether ADD is allowed for an existing position.
    Returns (decision, reason) -- 'ADD', 'HOLD', 'BLOCKED'."""
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
    relevant_score = scores.get('long_score', 50) if side == 'LONG' else scores.get('short_score', 50)
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
        'entry_pct': entry_pct}
    cur.execute("""
        INSERT INTO signals_action_v3 (
            ts, symbol, tf, strategy, signal, action, price, meta, created_at_unix, processed, stage
        ) VALUES (
            %s, %s, %s, %s, %s, %s, NULL, %s::jsonb, %s, false, 'SIGNAL_CREATED'
        ) RETURNING id;
    """, (ts_text, SYMBOL, 'auto', 'AUTOPILOT', 'AUTO_ENTRY', 'OPEN',
          json.dumps(meta, ensure_ascii=False, default=str), now_unix))
    row = cur.fetchone()
    if row:
        return row[0]
    return 0


def _log_trade_process(cur, signal_id=None, scores=None, source='autopilot',
                        start_stage=None, entry_pct=None, equity_limits=None):
    '''Insert trade_process_log entry.'''
    if equity_limits is None:
        import safety_manager
        equity_limits = safety_manager.get_equity_limits(cur)
    eq = equity_limits
    context = scores.get('context', {})
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

            # RECONCILE MISMATCH: block new entries when exchange/strategy disagree
            try:
                import exchange_reader
                exch_data = exchange_reader.fetch_position()
                strat_data = exchange_reader.fetch_position_strat()
                if exchange_reader.reconcile(exch_data, strat_data) == 'MISMATCH':
                    _log('RECONCILE MISMATCH: entries blocked')
                    return
            except Exception:
                pass  # FAIL-OPEN: reconcile failure does not block

            # Check existing position
            cur.execute('SELECT side, total_qty FROM position_state WHERE symbol = %s;', (SYMBOL,))
            ps_row = cur.fetchone()
            has_position = ps_row and ps_row[0] and float(ps_row[1] or 0) > 0

            # Regime entry filter (new entries only)
            if not has_position:
                entry_ok, entry_reason = _check_regime_entry_filter(cur, regime_ctx, scores)
                if not entry_ok:
                    _log(f'REGIME_FILTER: {entry_reason}')
                    return

                # RANGE hourly entry cap
                r_params = regime_reader.get_regime_params(
                    regime_ctx.get('regime', 'UNKNOWN'), regime_ctx.get('shock_type'))
                if regime_ctx.get('regime') == 'RANGE':
                    max_per_hour = r_params.get('max_entries_per_hour', 3)
                    cur.execute("""
                        SELECT count(*) FROM trade_process_log
                        WHERE source = 'autopilot' AND ts >= now() - interval '1 hour';
                    """)
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
                        global _last_add_price, _last_add_ts
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
                            cur.execute("""
                                INSERT INTO execution_queue
                                    (symbol, action_type, direction, target_usdt,
                                     source, reason, priority, expire_at)
                                VALUES (%s, 'ADD', %s, %s,
                                        'autopilot', %s, 4, now() + interval '5 minutes')
                                RETURNING id;
                            """, (SYMBOL, direction, add_usdt, add_reason))
                            eq_row = cur.fetchone()
                            _log(f'ADD enqueued: eq_id={eq_row[0] if eq_row else None}')
                            _last_add_price = current_price if current_price > 0 else 0
                            _last_add_ts = time.time()
                else:
                    _log(f'position exists, {decision}: {add_reason}')
                return

            # ── v3: Central signal emission gate ──
            (emit_ok, emit_reason) = should_emit_signal(cur, SYMBOL, dominant, confidence)
            if not emit_ok:
                should_log, _ = _db_should_log_risk(cur, f'signal_suppress_{emit_reason[:30]}')
                if should_log:
                    _log(f'SIGNAL_SUPPRESSED: {emit_reason}')
                return

            import safety_manager
            eq = safety_manager.get_equity_limits(cur)
            # v3: start_stage always 1
            start_stage = 1
            entry_pct = safety_manager.get_stage_entry_pct(start_stage)

            signal_id = _create_autopilot_signal(cur, dominant, scores, equity_limits=eq, regime_ctx=regime_ctx)
            if signal_id:
                # Record emission for cooldown tracking
                _record_signal_emission(cur, SYMBOL, dominant)
                _log_trade_process(cur, signal_id, scores, 'autopilot', start_stage, entry_pct,
                                    equity_limits=eq)
                _notify_telegram(
                    f'[Autopilot] {dominant} 진입 신호 생성\n'
                    f'- L={scores.get("long_score")} S={scores.get("short_score")} conf={confidence}\n'
                    f'- start_stage={start_stage} (FORCED=1) entry={entry_pct}%\n'
                    f'- signal_id={signal_id}')
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
    while True:
        if os.path.exists(KILL_SWITCH_PATH):
            _log('KILL_SWITCH detected. Exiting.')
            sys.exit(0)
        try:
            _cycle()
        except Exception:
            traceback.print_exc()
        time.sleep(POLL_SEC)


if __name__ == '__main__':
    main()
