"""
autopilot_daemon.py — Autonomous trading daemon.

60-second loop:
  1. Check autopilot_config.enabled
  2. Risk checks (trade_switch, LIVE_TRADING, once_lock, daily limit, cooldown, position)
  3. Run direction_scorer.compute_scores()
  4. If confidence >= 15, create OPEN signal
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
USDT_CAP = 900
POLL_SEC = 60
COOLDOWN_SEC = 300
MAX_DAILY_TRADES = 10
MIN_CONFIDENCE = 15
DEFAULT_SIZE_PCT = 10
KILL_SWITCH_PATH = '/root/trading-bot/app/KILL_SWITCH'
_migrations_done = False
_exchange = None


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
        elapsed = (datetime.datetime.now(datetime.timezone.utc) - row[0].replace(
            tzinfo=datetime.timezone.utc)).total_seconds()
        if elapsed < COOLDOWN_SEC:
            return (False, f'cooldown ({int(elapsed)}/{COOLDOWN_SEC}s)')

    # Check if position already exists
    cur.execute('SELECT side, total_qty FROM position_state WHERE symbol = %s;', (SYMBOL,))
    ps_row = cur.fetchone()
    has_position = ps_row and ps_row[0] and float(ps_row[1] or 0) > 0

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

    cur.execute("""
            SELECT COUNT(*) FROM news
            WHERE ts >= now() - interval '1 hour'
              AND impact_score >= 7;
        """)
    if int(cur.fetchone()[0]) > 0:
        return ('HOLD', '고영향 뉴스 존재')

    return ('ADD', '모든 조건 충족')


def _create_autopilot_signal(cur=None, side=None, scores=None):
    '''Create OPEN signal for autopilot. Returns signal_id.'''
    import datetime
    import safety_manager
    now = datetime.datetime.now(datetime.timezone.utc)
    ts_text = now.strftime('%Y-%m-%d %H:%M:%S+00')
    now_unix = int(time.time())
    relevant_score = scores.get('long_score', 50) if side == 'LONG' else scores.get('short_score', 50)
    start_stage = safety_manager.compute_start_stage(relevant_score)
    entry_pct = safety_manager.get_stage_entry_pct(start_stage)
    usdt_amount = safety_manager.get_entry_usdt(cur, start_stage)
    usdt_amount = min(usdt_amount, USDT_CAP)
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
                        start_stage=None, entry_pct=None):
    '''Insert trade_process_log entry.'''
    import safety_manager
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
            USDT_CAP,
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
                _log(f'risk check failed: {reason}')
                return

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
                    # Handle ADD via position_manager
                else:
                    _log(f'position exists, {decision}: {add_reason}')
                return

            if confidence < MIN_CONFIDENCE:
                _log(f'confidence too low ({confidence} < {MIN_CONFIDENCE})')
                return

            import safety_manager
            start_stage = safety_manager.compute_start_stage(
                scores.get('long_score', 50) if dominant == 'LONG' else scores.get('short_score', 50))
            entry_pct = safety_manager.get_stage_entry_pct(start_stage)

            signal_id = _create_autopilot_signal(cur, dominant, scores)
            if signal_id:
                _log_trade_process(cur, signal_id, scores, 'autopilot', start_stage, entry_pct)
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
