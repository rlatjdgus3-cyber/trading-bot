"""
autopilot_daemon.py — Autonomous trading daemon.

20-second loop:
  1. Check autopilot_config.enabled
  2. Risk checks (trade_switch, LIVE_TRADING, once_lock, daily limit, cooldown, position)
  3. Run direction_scorer.compute_scores()
  4. If confidence >= MIN_CONFIDENCE (10), create OPEN signal
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
MAX_DAILY_TRADES = 10
MIN_CONFIDENCE = 10  # abs(total_score) >= 10 for entry
DEFAULT_SIZE_PCT = 10
KILL_SWITCH_PATH = '/root/trading-bot/app/KILL_SWITCH'
TRADE_SWITCH_DEDUP_KEY = 'autopilot:risk_check:trade_switch_off'
LOG_COOLDOWN_SEC = 1800  # 30min log dedup (non-trade_switch reasons)
_migrations_done = False
_exchange = None


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


def _check_position_for_add(cur=None, pos_side=None, pos_qty=None, scores=None):
    """Evaluate whether ADD is allowed for an existing position.
    Returns (decision, reason) -- 'ADD', 'HOLD', 'BLOCKED'."""
    import safety_manager
    direction = 'LONG' if pos_side == 'long' else 'SHORT'
    dominant_side = scores.get('dominant_side', 'LONG')
    if dominant_side != direction:
        return ('HOLD', f'반대방향 신호 ({dominant_side} vs 현재 {direction})')

    relevant_score = scores.get('long_score', 50) if direction == 'LONG' else scores.get('short_score', 50)
    add_threshold = safety_manager.get_add_score_threshold(cur)
    if relevant_score < add_threshold:
        return ('HOLD', f'방향 점수 부족 ({relevant_score} < {add_threshold})')

    cur.execute('SELECT stage FROM position_state WHERE symbol = %s;', (SYMBOL,))
    ps_row = cur.fetchone()
    pyramid_stage = int(ps_row[0]) if ps_row else 0
    (ok, reason) = safety_manager.check_pyramid_allowed(cur, pyramid_stage)
    if not ok:
        return ('BLOCKED', reason)

    (ok, reason) = safety_manager.check_trade_budget(cur, safety_manager.get_add_slice_pct())
    if not ok:
        return ('BLOCKED', reason)

    add_usdt = safety_manager.get_add_slice_usdt(cur)
    (ok, reason) = safety_manager.check_total_exposure(cur, add_usdt)
    if not ok:
        return ('BLOCKED', reason)

    # News does not block chart-based entry (NEWS_CAN_BLOCK_ENTRY = False)

    return ('ADD', '모든 조건 충족')


def _create_autopilot_signal(cur=None, side=None, scores=None, equity_limits=None):
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
    start_stage = safety_manager.compute_start_stage(relevant_score)
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

            import direction_scorer
            scores = direction_scorer.compute_scores()
            confidence = scores.get('confidence', 0)
            dominant = scores.get('dominant_side', 'LONG')

            _log(f'scores: L={scores.get("long_score")} S={scores.get("short_score")} '
                 f'conf={confidence} side={dominant}')

            # Check existing position
            cur.execute('SELECT side, total_qty FROM position_state WHERE symbol = %s;', (SYMBOL,))
            ps_row = cur.fetchone()
            has_position = ps_row and ps_row[0] and float(ps_row[1] or 0) > 0

            if has_position:
                # Evaluate ADD
                pos_side = ps_row[0]
                pos_qty = float(ps_row[1])
                (decision, add_reason) = _check_position_for_add(cur, pos_side, pos_qty, scores)
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
                else:
                    _log(f'position exists, {decision}: {add_reason}')
                return

            if confidence < MIN_CONFIDENCE:
                _log(f'confidence too low ({confidence} < {MIN_CONFIDENCE})')
                return

            import safety_manager
            eq = safety_manager.get_equity_limits(cur)
            start_stage = safety_manager.compute_start_stage(
                scores.get('long_score', 50) if dominant == 'LONG' else scores.get('short_score', 50))
            entry_pct = safety_manager.get_stage_entry_pct(start_stage)

            signal_id = _create_autopilot_signal(cur, dominant, scores, equity_limits=eq)
            if signal_id:
                _log_trade_process(cur, signal_id, scores, 'autopilot', start_stage, entry_pct,
                                    equity_limits=eq)
                _notify_telegram(
                    f'[Autopilot] {dominant} 진입 신호 생성\n'
                    f'- L={scores.get("long_score")} S={scores.get("short_score")} conf={confidence}\n'
                    f'- start_stage={start_stage} entry={entry_pct}%\n'
                    f'- signal_id={signal_id}')
                _log(f'signal created: {dominant} id={signal_id} stage={start_stage}')
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
