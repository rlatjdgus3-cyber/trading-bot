"""
position_manager.py — Core position management daemon.

10-30s adaptive loop:
  1. Check autopilot enabled
  2. Fetch Bybit position (source of truth)
  3. Build market context
  4. Check emergency conditions -> Claude API if triggered
  5. Run decision engine: HOLD / ADD / REDUCE / CLOSE / REVERSE
  6. Log to pm_decision_log
  7. If action != HOLD, insert into execution_queue

Never places orders directly — all actions go through execution_queue.
"""
import os
import sys
import time
import json
import traceback
import urllib.parse
import urllib.request
sys.path.insert(0, '/root/trading-bot/app')
import ccxt
import psycopg2
from dotenv import load_dotenv
import test_utils
import report_formatter
load_dotenv('/root/trading-bot/app/.env')

SYMBOL = 'BTC/USDT:USDT'
KILL_SWITCH_PATH = '/root/trading-bot/app/KILL_SWITCH'
LOOP_FAST_SEC = 10
LOOP_NORMAL_SEC = 15
LOOP_SLOW_SEC = 30
LOG_PREFIX = '[pos_mgr]'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _db_conn():
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=int(os.getenv('DB_PORT', '5432')),
        dbname=os.getenv('DB_NAME', 'trading'),
        user=os.getenv('DB_USER', 'bot'),
        password=os.getenv('DB_PASS', 'botpass'),
        connect_timeout=10,
        options='-c statement_timeout=30000')


_exchange = None
_tables_ensured = False
_prev_scores = {}  # Previous cycle scores for regime change detection

# ── HOLD repeat suppression ──────────────────────────────
_recent_claude_actions = []
CONSECUTIVE_HOLD_LIMIT = 3
_prev_position_side = None


def _get_exchange():
    global _exchange
    if _exchange is not None:
        return _exchange
    _exchange = ccxt.bybit({
        'apiKey': os.getenv('BYBIT_API_KEY'),
        'secret': os.getenv('BYBIT_SECRET'),
        'enableRateLimit': True,
        'timeout': 15000,
        'options': {
            'defaultType': 'swap',
            'recvWindow': 10000,
        }})
    _exchange.load_markets()
    return _exchange


_tg_config = {}


def _load_tg_config():
    if _tg_config:
        return _tg_config
    env_path = '/root/trading-bot/app/telegram_cmd.env'
    try:
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if '=' in line and not line.startswith('#'):
                    k, v = line.split('=', 1)
                    _tg_config[k.strip()] = v.strip()
    except Exception:
        pass
    return _tg_config


def _send_telegram(text=None):
    cfg = _load_tg_config()
    token = cfg.get('TELEGRAM_BOT_TOKEN', '')
    chat_id = cfg.get('TELEGRAM_ALLOWED_CHAT_ID', '')
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


def _fetch_position(ex=None):
    '''Fetch current Bybit position. Returns dict or None.'''
    positions = ex.fetch_positions([SYMBOL])
    for p in positions:
        if p.get('symbol') != SYMBOL:
            continue
        contracts = float(p.get('contracts') or 0)
        side = p.get('side')
        if not contracts > 0:
            continue
        if side not in ('long', 'short'):
            continue
        entry_price = float(p.get('entryPrice') or 0)
        mark_price = float(p.get('markPrice') or 0)
        upnl = float(p.get('unrealizedPnl') or 0)
        liq_price = float(p.get('liquidationPrice') or 0)
        return {
            'side': side,
            'qty': contracts,
            'entry_price': entry_price,
            'mark_price': mark_price,
            'upnl': upnl,
            'leverage': p.get('leverage'),
            'liquidation_price': liq_price}
    return None


def _build_context(cur=None, pos=None, snapshot=None):
    '''Build full market context for decision engine.'''
    ctx = {
        'position': pos,
        'price': pos.get('mark_price', 0) if pos else 0}

    # Use snapshot for indicators if available
    if snapshot:
        ctx['price'] = snapshot.get('price', ctx['price'])
        ctx['indicators'] = {
            'kijun': snapshot.get('kijun'),
            'rsi': snapshot.get('rsi_14'),
            'atr': snapshot.get('atr_14'),
            'vol': snapshot.get('vol_last'),
            'vol_ma20': snapshot.get('vol_ma20'),
            'vol_spike': (snapshot.get('vol_ratio', 0) >= 2.0),
            'bb_mid': snapshot.get('bb_mid'),
            'bb_up': snapshot.get('bb_upper'),
            'bb_dn': snapshot.get('bb_lower'),
            'tenkan': snapshot.get('tenkan'),
            'ma_50': snapshot.get('ma_50'),
            'ma_200': snapshot.get('ma_200'),
        }
    else:
        # Fallback: DB indicators
        cur.execute("""
                SELECT ich_kijun, rsi_14, atr_14, vol, vol_ma20, vol_spike,
                       bb_mid, bb_up, bb_dn, ich_tenkan, ma_50, ma_200
                FROM indicators
                WHERE symbol = %s AND tf = '1m'
                ORDER BY ts DESC LIMIT 1;
            """, (SYMBOL,))
        row = cur.fetchone()
        if row:
            ctx['indicators'] = {
                'kijun': float(row[0]) if row[0] else None,
                'rsi': float(row[1]) if row[1] else None,
                'atr': float(row[2]) if row[2] else None,
                'vol': float(row[3]) if row[3] else None,
                'vol_ma20': float(row[4]) if row[4] else None,
                'vol_spike': bool(row[5]) if row[5] is not None else False,
                'bb_mid': float(row[6]) if row[6] else None,
                'bb_up': float(row[7]) if row[7] else None,
                'bb_dn': float(row[8]) if row[8] else None,
                'tenkan': float(row[9]) if row[9] else None,
                'ma_50': float(row[10]) if row[10] else None,
                'ma_200': float(row[11]) if row[11] else None,
            }
        else:
            ctx['indicators'] = {}

    # Vol profile
    cur.execute("""
            SELECT poc, vah, val FROM vol_profile
            WHERE symbol = %s
            ORDER BY ts DESC LIMIT 1;
        """, (SYMBOL,))
    vp_row = cur.fetchone()
    if vp_row:
        ctx['vol_profile'] = {
            'poc': float(vp_row[0]) if vp_row[0] else None,
            'vah': float(vp_row[1]) if vp_row[1] else None,
            'val': float(vp_row[2]) if vp_row[2] else None,
        }
    else:
        ctx['vol_profile'] = {}

    # Recent candles
    cur.execute("""
            SELECT ts, o, h, l, c, v FROM candles
            WHERE symbol = %s AND tf = '1m'
            ORDER BY ts DESC LIMIT 10;
        """, (SYMBOL,))
    ctx['candles_1m'] = [
        {'ts': str(r[0]), 'o': float(r[1]), 'h': float(r[2]),
         'l': float(r[3]), 'c': float(r[4]), 'v': float(r[5])}
        for r in cur.fetchall()
    ]

    # Scores
    try:
        import score_engine
        scores = score_engine.compute_total(cur=cur, exchange=_get_exchange())
        ctx['scores'] = scores
        ctx['unified_score'] = scores
    except Exception:
        ctx['scores'] = {}
        ctx['unified_score'] = {}

    # Position state
    cur.execute("""
            SELECT side, total_qty, avg_entry_price, stage,
                   trade_budget_used_pct, next_stage_available
            FROM position_state WHERE symbol = %s;
        """, (SYMBOL,))
    ps_row = cur.fetchone()
    if ps_row:
        ctx['pos_state'] = {
            'side': ps_row[0],
            'total_qty': float(ps_row[1]) if ps_row[1] else 0,
            'avg_entry': float(ps_row[2]) if ps_row[2] else 0,
            'stage': int(ps_row[3]) if ps_row[3] else 0,
            'budget_used_pct': float(ps_row[4]) if ps_row[4] else 0,
            'next_stage': int(ps_row[5]) if ps_row[5] else 0,
        }
    else:
        ctx['pos_state'] = {}

    # News
    cur.execute("""
            SELECT title, summary, impact_score, ts FROM news
            WHERE ts >= now() - interval '2 hours'
            ORDER BY ts DESC LIMIT 10;
        """)
    ctx['news'] = [
        {'title': r[0], 'summary': r[1], 'impact_score': r[2], 'ts': str(r[3])}
        for r in cur.fetchall()
    ]

    # Funding rate
    try:
        funding_data = _get_exchange().fetch_funding_rate(SYMBOL)
        ctx['funding_rate'] = float(funding_data.get('fundingRate', 0))
    except Exception:
        ctx['funding_rate'] = 0

    return ctx


def _check_emergency(ctx=None):
    '''Check for emergency conditions. Returns trigger dict or None.'''
    ind = ctx.get('indicators', {})
    atr = ind.get('atr')
    price = ctx.get('price', 0)
    candles = ctx.get('candles_1m', [])
    funding = ctx.get('funding_rate', 0)

    if not atr or atr <= 0 or not candles or len(candles) < 2:
        return None

    if len(candles) >= 2:
        latest_c = candles[0].get('c', 0)
        prev_c = candles[1].get('c', 0)
        if prev_c > 0:
            move = abs(latest_c - prev_c)
            if move > 2.5 * atr:
                return {
                    'type': 'rapid_price_move',
                    'detail': {
                        'move': move,
                        'atr': atr,
                        'atr_multiple': round(move / atr, 2),
                        'direction': 'up' if latest_c > prev_c else 'down'}}

    if abs(funding) > 0.001:
        return {
            'type': 'extreme_funding',
            'detail': {
                'funding_rate': funding}}

    vol = ind.get('vol', 0)
    vol_ma = ind.get('vol_ma20', 0)
    if vol_ma > 0 and vol > 3 * vol_ma:
        return {
            'type': 'volume_spike',
            'detail': {
                'vol': vol,
                'vol_ma': vol_ma,
                'ratio': round(vol / vol_ma, 2)}}

    unified = ctx.get('unified_score') or ctx.get('scores', {}).get('unified') or {}
    current_total = unified.get('total_score')
    if current_total is not None and abs(current_total) > 80:
        return {
            'type': 'extreme_score',
            'detail': {
                'total_score': current_total,
                'dominant_side': unified.get('dominant_side')}}

    return None


def _handle_emergency(cur=None, ctx=None, trigger=None):
    '''Handle emergency via Claude API. Returns action taken.'''
    _send_telegram(report_formatter.format_emergency_pre_alert(
        trigger['type'], trigger.get('detail')))

    import attach_similar_events
    import save_claude_analysis
    from fact_categories import classify_news, extract_macro_keywords

    # Find similar FACT events
    news_text = ' '.join(n.get('summary', '') or '' for n in ctx.get('news', []))
    fact_category = classify_news(news_text)
    fact_keywords = extract_macro_keywords(news_text)
    similar = attach_similar_events.find_similar(cur, category=fact_category, keywords=fact_keywords)
    perf_summary = attach_similar_events.build_performance_summary(similar)

    import claude_api

    ctx['trigger'] = trigger
    ctx['fact_similar_events'] = similar
    ctx['fact_performance_summary'] = perf_summary

    try:
        result = claude_api.emergency_analysis(ctx)
    except Exception:
        traceback.print_exc()
        result = claude_api.FALLBACK_RESPONSE.copy()
        result['fallback_used'] = True

    # Log to emergency_analysis_log
    cur.execute("""
        INSERT INTO emergency_analysis_log
            (symbol, trigger_type, trigger_detail, context_packet,
             response_raw, risk_level, recommended_action, confidence,
             reason_bullets, ttl_seconds, api_latency_ms, fallback_used)
        VALUES (%s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s::jsonb, %s, %s, %s)
        RETURNING id;
    """, (
        SYMBOL,
        trigger['type'],
        json.dumps(trigger.get('detail', {}), default=str),
        json.dumps(ctx, default=str, ensure_ascii=False)[:10000],
        json.dumps(result, default=str, ensure_ascii=False),
        result.get('risk_level'),
        result.get('recommended_action'),
        result.get('confidence'),
        json.dumps(result.get('reason_bullets', []), ensure_ascii=False),
        result.get('ttl_seconds'),
        result.get('api_latency_ms'),
        result.get('fallback_used', False),
    ))
    eid_row = cur.fetchone()
    eid = eid_row[0] if eid_row else None

    # Save claude analysis for feedback loop
    try:
        ca_id = save_claude_analysis.save_analysis(
            cur, kind='emergency',
            input_packet=ctx,
            output=result,
            event_id=None,
            similar_events=similar,
            emergency_log_id=eid)
    except Exception:
        traceback.print_exc()
        ca_id = None

    action = result.get('action') or result.get('recommended_action', 'HOLD')

    if action == 'HOLD':
        _send_telegram(report_formatter.format_emergency_post_alert(
            trigger['type'], 'HOLD', result))
        return 'HOLD'

    pos = ctx.get('position', {})

    if action == 'REDUCE':
        reduce_pct = result.get('reduce_pct', 50)
        eq_id = _enqueue_action(
            cur, 'REDUCE', pos.get('side', '').upper(),
            reduce_pct=reduce_pct,
            reason=f'emergency_{trigger["type"]}',
            emergency_id=eid,
            priority=2)
        if ca_id and eq_id:
            try:
                save_claude_analysis.create_pending_outcome(cur, ca_id, 'REDUCE', execution_queue_id=eq_id)
            except Exception:
                traceback.print_exc()
        _send_telegram(report_formatter.format_emergency_post_alert(
            trigger['type'], 'REDUCE', result))
        return 'REDUCE'

    if action == 'CLOSE':
        eq_id = _enqueue_action(
            cur, 'CLOSE', pos.get('side', '').upper(),
            target_qty=pos.get('qty'),
            reason=f'emergency_{trigger["type"]}',
            emergency_id=eid,
            priority=1)
        if ca_id and eq_id:
            try:
                save_claude_analysis.create_pending_outcome(cur, ca_id, 'CLOSE', execution_queue_id=eq_id)
            except Exception:
                traceback.print_exc()
        _send_telegram(report_formatter.format_emergency_post_alert(
            trigger['type'], 'CLOSE', result))
        return 'CLOSE'

    if action == 'REVERSE':
        eq_id = _enqueue_reverse(
            cur, pos,
            reason=f'emergency_{trigger["type"]}',
            emergency_id=eid,
            priority=1)
        if ca_id and eq_id:
            try:
                save_claude_analysis.create_pending_outcome(cur, ca_id, 'REVERSE', execution_queue_id=eq_id)
            except Exception:
                traceback.print_exc()
        _send_telegram(report_formatter.format_emergency_post_alert(
            trigger['type'], 'REVERSE', result))
        return 'REVERSE'

    if action in ('OPEN_LONG', 'OPEN_SHORT'):
        pos_side = (pos.get('side') or '').upper()
        direction = 'LONG' if action == 'OPEN_LONG' else 'SHORT'
        if pos_side and pos_side != direction:
            _log(f'{action} conflicts with {pos_side} in emergency — HOLD')
            return 'HOLD'
        import safety_manager
        target_stage = result.get('target_stage', 1)
        target_usdt = safety_manager.get_add_slice_usdt(cur) * target_stage
        eq_id = _enqueue_action(
            cur, 'ADD', direction,
            target_usdt=target_usdt,
            reason=f'emergency_{trigger["type"]}',
            emergency_id=eid,
            priority=2)
        if ca_id and eq_id:
            try:
                save_claude_analysis.create_pending_outcome(cur, ca_id, action, execution_queue_id=eq_id)
            except Exception:
                traceback.print_exc()
        _send_telegram(report_formatter.format_emergency_post_alert(
            trigger['type'], action, result))
        return action

    return 'HOLD'


def _handle_event_trigger(cur=None, ctx=None, event_result=None, snapshot=None):
    """Handle event trigger → Claude analysis → action execution."""
    import claude_api
    import save_claude_analysis
    import event_trigger as _et

    trigger_types = [t.get('type', '?') for t in event_result.triggers]
    if _et.should_send_telegram_event(trigger_types):
        _send_telegram(report_formatter.format_event_pre_alert(
            trigger_types, event_result.mode))

    try:
        result = claude_api.event_trigger_analysis(ctx, snapshot, event_result)
    except Exception:
        traceback.print_exc()
        result = claude_api.ABORT_RESPONSE.copy()
        result['fallback_used'] = True

    # Log to event_trigger_log
    try:
        cur.execute("""
            INSERT INTO event_trigger_log
                (symbol, mode, triggers, event_hash, snapshot_ts, snapshot_price,
                 claude_called, claude_result, call_type, dedup_blocked)
            VALUES (%s, %s, %s::jsonb, %s, to_timestamp(%s), %s, %s, %s::jsonb, %s, %s)
        """, (
            SYMBOL,
            event_result.mode,
            json.dumps(event_result.triggers, default=str),
            event_result.event_hash,
            snapshot.get('snapshot_ts') if snapshot else None,
            snapshot.get('price') if snapshot else None,
            True,
            json.dumps(result, default=str, ensure_ascii=False)[:5000],
            event_result.call_type,
            False,
        ))
    except Exception:
        traceback.print_exc()

    if result.get('aborted') or result.get('fallback_used'):
        _log(f'event analysis skipped: aborted={result.get("aborted")} '
             f'fallback={result.get("fallback_used")} '
             f'gate_reason={result.get("gate_reason", "")}')
        return 'ABORT'

    # ── Price context validation ──
    import market_snapshot as _ms
    mentioned_price = result.get('price') or result.get('entry_price') or result.get('target_price')
    if mentioned_price and snapshot:
        price_ok, price_reason = _ms.validate_price_mention(mentioned_price, snapshot)
        if not price_ok:
            _log(f'INVALID PRICE CONTEXT – STRATEGY REJECTED: {price_reason}')
            trigger_types = [t.get('type', '?') for t in event_result.triggers]
            _send_telegram(report_formatter.format_event_post_alert(
                trigger_types, 'HOLD (price rejected)', result))
            return 'HOLD'

    # Save claude analysis
    try:
        ca_id = save_claude_analysis.save_analysis(
            cur, kind='event_trigger',
            input_packet=ctx,
            output=result,
            event_id=None,
            similar_events=[])
    except Exception:
        traceback.print_exc()
        ca_id = None

    action = result.get('action') or result.get('recommended_action', 'HOLD')
    pos = ctx.get('position', {})
    reason_info = ', '.join(result.get('reason_bullets', [])[:2]) or result.get('reason_code', '')

    # ── Event stabilization guards ──
    import event_trigger as _et
    pos_side = (pos.get('side') or '').upper()

    if action == 'REDUCE':
        pos_qty = pos.get('qty', 0)
        reduce_pct = result.get('reduce_pct', 50)
        reduce_qty = pos_qty * reduce_pct / 100
        if reduce_qty < _et.MIN_ORDER_QTY_BTC:
            _log(f'REDUCE blocked: qty {reduce_qty:.4f} < min {_et.MIN_ORDER_QTY_BTC}')
            action = 'HOLD'

    if action == 'HOLD':
        _send_telegram(report_formatter.format_event_post_alert(
            trigger_types, 'HOLD', result))
        return 'HOLD'

    if action == 'REDUCE':
        reduce_pct = result.get('reduce_pct', 50)
        eq_id = _enqueue_action(
            cur, 'REDUCE', pos_side,
            reduce_pct=reduce_pct,
            reason=f'event_trigger_{trigger_types[0] if trigger_types else "unknown"}',
            priority=3)
        if ca_id and eq_id:
            try:
                save_claude_analysis.create_pending_outcome(cur, ca_id, 'REDUCE', execution_queue_id=eq_id)
            except Exception:
                traceback.print_exc()
        _send_telegram(report_formatter.format_event_post_alert(
            trigger_types, 'REDUCE', result))
        return 'REDUCE'

    if action == 'CLOSE':
        eq_id = _enqueue_action(
            cur, 'CLOSE', pos_side,
            target_qty=pos.get('qty'),
            reason=f'event_trigger_{trigger_types[0] if trigger_types else "unknown"}',
            priority=2)
        if ca_id and eq_id:
            try:
                save_claude_analysis.create_pending_outcome(cur, ca_id, 'CLOSE', execution_queue_id=eq_id)
            except Exception:
                traceback.print_exc()
        _send_telegram(report_formatter.format_event_post_alert(
            trigger_types, 'CLOSE', result))
        return 'CLOSE'

    if action in ('OPEN_LONG', 'OPEN_SHORT'):
        direction = 'LONG' if action == 'OPEN_LONG' else 'SHORT'
        if pos_side and pos_side != direction:
            _log(f'{action} conflicts with {pos_side} — skipped')
            return 'HOLD'
        import safety_manager
        target_stage = result.get('target_stage', 1)
        target_usdt = safety_manager.get_add_slice_usdt(cur) * target_stage
        eq_id = _enqueue_action(
            cur, 'ADD', direction,
            target_usdt=target_usdt,
            reason=f'event_trigger_{trigger_types[0] if trigger_types else "unknown"}',
            priority=3)
        if ca_id and eq_id:
            try:
                save_claude_analysis.create_pending_outcome(cur, ca_id, action, execution_queue_id=eq_id)
            except Exception:
                traceback.print_exc()
        _send_telegram(report_formatter.format_event_post_alert(
            trigger_types, action, result))
        return 'OPEN'

    if action == 'REVERSE':
        eq_id = _enqueue_reverse(
            cur, pos,
            reason=f'event_trigger_{trigger_types[0] if trigger_types else "unknown"}',
            priority=2)
        if ca_id and eq_id:
            try:
                save_claude_analysis.create_pending_outcome(cur, ca_id, 'REVERSE', execution_queue_id=eq_id)
            except Exception:
                traceback.print_exc()
        _send_telegram(report_formatter.format_event_post_alert(
            trigger_types, 'REVERSE', result))
        return 'REVERSE'

    return 'HOLD'


def _handle_event_trigger_mini(cur=None, ctx=None, event_result=None, snapshot=None):
    """Handle event trigger via GPT-4o-mini (cost fallback). Returns action taken."""
    import claude_api
    import save_claude_analysis
    import event_trigger as _et

    trigger_types = [t.get('type', '?') for t in event_result.triggers]

    # Telegram throttle: only send pre-alert if not throttled
    if _et.should_send_telegram_event(trigger_types):
        _send_telegram(report_formatter.format_event_pre_alert(
            trigger_types, event_result.mode) + '\n(GPT-mini)')

    try:
        result = claude_api.event_trigger_analysis_mini(ctx, snapshot, event_result)
    except Exception:
        traceback.print_exc()
        result = claude_api.ABORT_RESPONSE.copy()
        result['fallback_used'] = True

    # Log to event_trigger_log
    try:
        cur.execute("""
            INSERT INTO event_trigger_log
                (symbol, mode, triggers, event_hash, snapshot_ts, snapshot_price,
                 claude_called, claude_result, call_type, dedup_blocked)
            VALUES (%s, %s, %s::jsonb, %s, to_timestamp(%s), %s, %s, %s::jsonb, %s, %s)
        """, (
            SYMBOL,
            event_result.mode,
            json.dumps(event_result.triggers, default=str),
            event_result.event_hash,
            snapshot.get('snapshot_ts') if snapshot else None,
            snapshot.get('price') if snapshot else None,
            True,
            json.dumps(result, default=str, ensure_ascii=False)[:5000],
            'AUTO_MINI',
            False,
        ))
    except Exception:
        traceback.print_exc()

    if result.get('aborted') or result.get('fallback_used'):
        _log(f'event mini analysis skipped: aborted={result.get("aborted")} '
             f'fallback={result.get("fallback_used")}')
        return 'ABORT'

    # Save analysis
    try:
        save_claude_analysis.save_analysis(
            cur, kind='event_trigger_mini',
            input_packet=ctx,
            output=result,
            event_id=None,
            similar_events=[])
    except Exception:
        traceback.print_exc()

    action = result.get('action') or result.get('recommended_action', 'HOLD')

    # GPT-mini safety: only allow HOLD and REDUCE (conservative)
    if action not in ('HOLD', 'REDUCE'):
        _log(f'GPT-mini action {action} downgraded to HOLD (safety)')
        action = 'HOLD'

    if action == 'HOLD':
        return 'HOLD'

    if action == 'REDUCE':
        pos = ctx.get('position', {})
        pos_side = (pos.get('side') or '').upper()
        pos_qty = pos.get('qty', 0)
        reduce_pct = result.get('reduce_pct', 25)  # conservative default
        reduce_qty = pos_qty * reduce_pct / 100
        if reduce_qty < _et.MIN_ORDER_QTY_BTC:
            _log(f'GPT-mini REDUCE blocked: qty {reduce_qty:.4f} < min')
            return 'HOLD'
        _enqueue_action(
            cur, 'REDUCE', pos_side,
            reduce_pct=reduce_pct,
            reason=f'event_mini_{trigger_types[0] if trigger_types else "unknown"}',
            priority=4)
        if _et.should_send_telegram_event(trigger_types):
            _send_telegram(report_formatter.format_event_post_alert(
                trigger_types, 'REDUCE (mini)', result))
        return 'REDUCE'

    return 'HOLD'


def _handle_emergency_v2(cur=None, ctx=None, event_result=None, snapshot=None):
    """Handle emergency via Claude API (snapshot-based). Returns action taken."""
    trigger_types = [t.get('type', '?') for t in event_result.triggers]
    _send_telegram(report_formatter.format_emergency_pre_alert(
        trigger_types[0] if trigger_types else 'event_emergency',
        event_result.triggers[0] if event_result.triggers else {}))

    import attach_similar_events
    import save_claude_analysis
    from fact_categories import classify_news, extract_macro_keywords

    # Find similar FACT events
    news_text = ' '.join(n.get('summary', '') or '' for n in ctx.get('news', []))
    fact_category = classify_news(news_text)
    fact_keywords = extract_macro_keywords(news_text)
    similar = attach_similar_events.find_similar(cur, category=fact_category, keywords=fact_keywords)
    perf_summary = attach_similar_events.build_performance_summary(similar)

    # Build trigger info for context
    primary_trigger = event_result.triggers[0] if event_result.triggers else {}
    ctx['trigger'] = {
        'type': primary_trigger.get('type', 'event_emergency'),
        'detail': primary_trigger,
    }
    ctx['fact_similar_events'] = similar
    ctx['fact_performance_summary'] = perf_summary

    import claude_api
    try:
        result = claude_api.event_trigger_analysis(ctx, snapshot, event_result)
    except Exception:
        traceback.print_exc()
        result = claude_api.ABORT_RESPONSE.copy()
        result['fallback_used'] = True

    # Log to emergency_analysis_log
    cur.execute("""
        INSERT INTO emergency_analysis_log
            (symbol, trigger_type, trigger_detail, context_packet,
             response_raw, risk_level, recommended_action, confidence,
             reason_bullets, ttl_seconds, api_latency_ms, fallback_used)
        VALUES (%s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s::jsonb, %s, %s, %s)
        RETURNING id;
    """, (
        SYMBOL,
        primary_trigger.get('type', 'event_emergency'),
        json.dumps(primary_trigger, default=str),
        json.dumps(ctx, default=str, ensure_ascii=False)[:10000],
        json.dumps(result, default=str, ensure_ascii=False),
        result.get('risk_level'),
        result.get('recommended_action'),
        result.get('confidence'),
        json.dumps(result.get('reason_bullets', []), ensure_ascii=False),
        result.get('ttl_seconds'),
        result.get('api_latency_ms'),
        result.get('fallback_used', False),
    ))
    eid_row = cur.fetchone()
    eid = eid_row[0] if eid_row else None

    # Log to event_trigger_log
    try:
        cur.execute("""
            INSERT INTO event_trigger_log
                (symbol, mode, triggers, event_hash, snapshot_ts, snapshot_price,
                 claude_called, claude_result, call_type, dedup_blocked)
            VALUES (%s, %s, %s::jsonb, %s, to_timestamp(%s), %s, %s, %s::jsonb, %s, %s)
        """, (
            SYMBOL,
            event_result.mode,
            json.dumps(event_result.triggers, default=str),
            event_result.event_hash,
            snapshot.get('snapshot_ts') if snapshot else None,
            snapshot.get('price') if snapshot else None,
            True,
            json.dumps(result, default=str, ensure_ascii=False)[:5000],
            event_result.call_type,
            False,
        ))
    except Exception:
        traceback.print_exc()

    # Save claude analysis
    try:
        ca_id = save_claude_analysis.save_analysis(
            cur, kind='emergency',
            input_packet=ctx,
            output=result,
            event_id=None,
            similar_events=similar,
            emergency_log_id=eid)
    except Exception:
        traceback.print_exc()
        ca_id = None

    if result.get('aborted') or result.get('fallback_used'):
        _log(f'emergency analysis skipped: aborted={result.get("aborted")} '
             f'fallback={result.get("fallback_used")}')
        return 'ABORT'

    # ── Price context validation ──
    import market_snapshot as _ms
    mentioned_price = result.get('price') or result.get('entry_price') or result.get('target_price')
    if mentioned_price and snapshot:
        price_ok, price_reason = _ms.validate_price_mention(mentioned_price, snapshot)
        if not price_ok:
            _log(f'INVALID PRICE CONTEXT – STRATEGY REJECTED: {price_reason}')
            action = 'HOLD'
            result['price_validation_failed'] = True
            result['price_validation_reason'] = price_reason
            _send_telegram(report_formatter.format_emergency_post_alert(
                primary_trigger.get('type', 'event_emergency'),
                'HOLD (price rejected)', result))
            return 'HOLD'

    action = result.get('action') or result.get('recommended_action', 'HOLD')
    pos = ctx.get('position', {})
    reason_info = ', '.join(result.get('reason_bullets', [])[:2]) or result.get('reason_code', '')

    # ── Emergency stabilization guards ──
    import event_trigger as _et
    pos_side = (pos.get('side') or '').upper()

    if action == 'REDUCE':
        pos_qty = pos.get('qty', 0)
        reduce_pct = result.get('reduce_pct', 50)
        reduce_qty = pos_qty * reduce_pct / 100
        if reduce_qty < _et.MIN_ORDER_QTY_BTC:
            _log(f'REDUCE blocked: qty {reduce_qty:.4f} < min {_et.MIN_ORDER_QTY_BTC}')
            action = 'HOLD'
        elif _et.is_duplicate_emergency_action(SYMBOL, 'REDUCE', pos_side):
            action = 'HOLD'

    em_trigger_type = primary_trigger.get('type', 'event_emergency')

    if action == 'HOLD':
        _send_telegram(report_formatter.format_emergency_post_alert(
            em_trigger_type, 'HOLD', result))
        return 'HOLD'

    if action == 'REDUCE':
        reduce_pct = result.get('reduce_pct', 50)
        eq_id = _enqueue_action(
            cur, 'REDUCE', pos_side,
            reduce_pct=reduce_pct,
            reason=f'emergency_{primary_trigger.get("type", "unknown")}',
            emergency_id=eid,
            emergency_mode=True,
            priority=2)
        if eq_id:
            _et.set_emergency_lock(SYMBOL)
            _et.record_emergency_action(SYMBOL, 'REDUCE', pos_side)
        if ca_id and eq_id:
            try:
                save_claude_analysis.create_pending_outcome(cur, ca_id, 'REDUCE', execution_queue_id=eq_id)
            except Exception:
                traceback.print_exc()
        _send_telegram(report_formatter.format_emergency_post_alert(
            em_trigger_type, 'REDUCE', result))
        return 'REDUCE'

    if action == 'CLOSE':
        eq_id = _enqueue_action(
            cur, 'CLOSE', pos_side,
            target_qty=pos.get('qty'),
            reason=f'emergency_{primary_trigger.get("type", "unknown")}',
            emergency_id=eid,
            emergency_mode=True,
            priority=1)
        if eq_id:
            _et.set_emergency_lock(SYMBOL)
            _et.record_emergency_action(SYMBOL, 'CLOSE', pos_side)
        if ca_id and eq_id:
            try:
                save_claude_analysis.create_pending_outcome(cur, ca_id, 'CLOSE', execution_queue_id=eq_id)
            except Exception:
                traceback.print_exc()
        _send_telegram(report_formatter.format_emergency_post_alert(
            em_trigger_type, 'CLOSE', result))
        return 'CLOSE'

    if action in ('OPEN_LONG', 'OPEN_SHORT'):
        direction = 'LONG' if action == 'OPEN_LONG' else 'SHORT'
        if pos_side and pos_side != direction:
            _log(f'{action} conflicts with {pos_side} — skipped')
            return 'HOLD'
        import safety_manager
        target_stage = result.get('target_stage', 1)
        target_usdt = safety_manager.get_add_slice_usdt(cur) * target_stage
        eq_id = _enqueue_action(
            cur, 'ADD', direction,
            target_usdt=target_usdt,
            reason=f'emergency_{primary_trigger.get("type", "unknown")}',
            emergency_id=eid,
            emergency_mode=True,
            priority=2)
        if eq_id:
            _et.set_emergency_lock(SYMBOL)
            _et.record_emergency_action(SYMBOL, action, pos_side or direction)
        if ca_id and eq_id:
            try:
                save_claude_analysis.create_pending_outcome(cur, ca_id, action, execution_queue_id=eq_id)
            except Exception:
                traceback.print_exc()
        _send_telegram(report_formatter.format_emergency_post_alert(
            em_trigger_type, action, result))
        return 'OPEN'

    if action == 'REVERSE':
        eq_id = _enqueue_reverse(
            cur, pos,
            reason=f'emergency_{primary_trigger.get("type", "unknown")}',
            emergency_id=eid,
            emergency_mode=True,
            priority=1)
        if eq_id:
            _et.set_emergency_lock(SYMBOL)
            _et.record_emergency_action(SYMBOL, 'REVERSE', pos_side)
        if ca_id and eq_id:
            try:
                save_claude_analysis.create_pending_outcome(cur, ca_id, 'REVERSE', execution_queue_id=eq_id)
            except Exception:
                traceback.print_exc()
        _send_telegram(report_formatter.format_emergency_post_alert(
            em_trigger_type, 'REVERSE', result))
        return 'REVERSE'

    return 'HOLD'


def _decide(ctx=None):
    '''Run decision engine. Returns (action, reason).'''
    pos = ctx.get('position', {})
    ind = ctx.get('indicators', {})
    vp = ctx.get('vol_profile', {})
    scores = ctx.get('scores', {})
    ps = ctx.get('pos_state', {})
    price = ctx.get('price', 0)
    side = pos.get('side', '')
    entry = pos.get('entry_price', 0)
    atr = ind.get('atr')
    rsi = ind.get('rsi')
    kijun = ind.get('kijun')
    poc = vp.get('poc')
    vah = vp.get('vah')
    val = vp.get('val')
    vol = ind.get('vol', 0)
    vol_ma = ind.get('vol_ma20', 0)
    vol_ratio = vol / vol_ma if vol_ma and vol_ma > 0 else 1
    long_score = scores.get('long_score', 50)
    short_score = scores.get('short_score', 50)
    stage = ps.get('stage', 0)
    news = ctx.get('news', [])

    # No position -> HOLD (autopilot_daemon handles entries)
    if not pos or not side:
        return ('HOLD', 'no position')

    dominant = scores.get('dominant_side', 'LONG')

    # Stop-loss check
    if atr and atr > 0 and entry and entry > 0:
        if side == 'long':
            sl_dist = (price - entry) / entry * 100
        else:
            sl_dist = (entry - price) / entry * 100

        sl_pct = scores.get('dynamic_stop_loss_pct', 2.0)
        if sl_dist <= -sl_pct:
            return ('CLOSE', f'stop_loss hit ({sl_dist:.2f}% vs -{sl_pct}%)')

    # Reversal check
    if side == 'long' and dominant == 'SHORT' and short_score >= 70:
        confirms = _structure_confirms(ctx, 'SHORT')
        if confirms >= 3:
            return ('REVERSE', f'strong SHORT reversal (score={short_score}, confirms={confirms})')

    if side == 'short' and dominant == 'LONG' and long_score >= 70:
        confirms = _structure_confirms(ctx, 'LONG')
        if confirms >= 3:
            return ('REVERSE', f'strong LONG reversal (score={long_score}, confirms={confirms})')

    # ADD check
    if stage < 7 and ps.get('budget_used_pct', 0) < 70:
        direction = 'LONG' if side == 'long' else 'SHORT'
        if dominant == direction:
            relevant = long_score if direction == 'LONG' else short_score
            if relevant >= 65:
                return ('ADD', f'score {relevant} favors {direction}, stage={stage}')

    # Reduce on strong counter signal
    if side == 'long' and short_score >= 65 and long_score <= 40:
        return ('REDUCE', f'counter signal (long={long_score}, short={short_score})')
    if side == 'short' and long_score >= 65 and short_score <= 40:
        return ('REDUCE', f'counter signal (long={long_score}, short={short_score})')

    return ('HOLD', 'no action needed')


def _structure_confirms(ctx=None, target_side=None):
    '''Count structure confirmations for reversal. Returns 0-4.'''
    confirms = 0
    ind = ctx.get('indicators', {})
    price = ctx.get('price', 0)

    tenkan = ind.get('tenkan')
    kijun = ind.get('kijun')
    rsi = ind.get('rsi')
    ma50 = ind.get('ma_50')
    ma200 = ind.get('ma_200')

    if target_side == 'LONG':
        if tenkan is not None and kijun is not None and tenkan > kijun:
            confirms += 1
        if rsi is not None and rsi < 40:
            confirms += 1
        if ma50 is not None and ma200 is not None and ma50 > ma200:
            confirms += 1
        if price and kijun and price > kijun:
            confirms += 1
    else:  # SHORT
        if tenkan is not None and kijun is not None and tenkan < kijun:
            confirms += 1
        if rsi is not None and rsi > 60:
            confirms += 1
        if ma50 is not None and ma200 is not None and ma50 < ma200:
            confirms += 1
        if price and kijun and price < kijun:
            confirms += 1

    return confirms


def _enqueue_action(cur=None, action_type=None, direction=None, **kwargs):
    '''Insert action into execution_queue.'''
    import safety_manager

    # Duplicate action check: block if same action_type already PENDING/PICKED
    if action_type in ('REDUCE', 'CLOSE', 'ADD', 'REVERSE_CLOSE', 'REVERSE_OPEN') and direction:
        cur.execute("""
            SELECT id FROM execution_queue
            WHERE symbol = %s AND action_type = %s AND direction = %s
              AND status IN ('PENDING', 'PICKED')
              AND ts >= now() - interval '5 minutes';
        """, (SYMBOL, action_type, direction))
        if cur.fetchone():
            _log(f'duplicate {action_type} {direction} blocked (already pending in queue)')
            return None

    emergency = kwargs.get('emergency_mode', False)
    (ok, reason) = safety_manager.run_all_checks(
        cur, kwargs.get('target_usdt', 0), emergency=emergency)
    if not ok and action_type == 'ADD':
        _log(f'safety block: {reason}')
        _send_telegram(f'[주문 거부] - 사유: {reason}')
        return None
    cur.execute("""
        INSERT INTO execution_queue
            (symbol, action_type, direction, target_qty, target_usdt,
             reduce_pct, source, pm_decision_id, emergency_id,
             reason, priority, expire_at, meta)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now() + interval '5 minutes',%s::jsonb)
        RETURNING id;
    """, (SYMBOL, action_type, direction,
          kwargs.get('target_qty'), kwargs.get('target_usdt'),
          kwargs.get('reduce_pct'), kwargs.get('source', 'position_manager'),
          kwargs.get('pm_decision_id'), kwargs.get('emergency_id'),
          kwargs.get('reason', ''), kwargs.get('priority', 5),
          json.dumps(kwargs.get('meta', {}), default=str)))
    row = cur.fetchone()
    eq_id = row[0] if row else None
    _log(f'enqueued: {action_type} {direction} eq_id={eq_id}')
    return eq_id


def _enqueue_reverse(cur=None, pos=None, **kwargs):
    '''Enqueue a 2-step reverse: REVERSE_CLOSE then REVERSE_OPEN.'''
    current_side = pos.get('side', '').upper()
    new_side = 'SHORT' if current_side == 'LONG' else 'LONG'
    em = kwargs.get('emergency_mode', False)
    close_id = _enqueue_action(
        cur, 'REVERSE_CLOSE', current_side,
        target_qty=pos.get('qty'),
        priority=kwargs.get('priority', 2),
        reason=kwargs.get('reason', 'reverse'),
        emergency_id=kwargs.get('emergency_id'),
        pm_decision_id=kwargs.get('pm_decision_id'),
        emergency_mode=em)
    if close_id:
        _enqueue_action(
            cur, 'REVERSE_OPEN', new_side,
            priority=kwargs.get('priority', 2),
            reason=kwargs.get('reason', 'reverse'),
            emergency_id=kwargs.get('emergency_id'),
            pm_decision_id=kwargs.get('pm_decision_id'),
            emergency_mode=em,
            meta={'depends_on': close_id})
    return close_id


def _sync_position_state(cur=None, pos=None):
    '''Sync position_state with Bybit reality.'''
    if pos is None:
        cur.execute("""
            UPDATE position_state SET
                side = NULL, total_qty = 0, stage = 0,
                capital_used_usdt = 0, trade_budget_used_pct = 0,
                updated_at = now()
            WHERE symbol = %s;
        """, (SYMBOL,))
    else:
        cur.execute("""
            UPDATE position_state SET
                side = %s, total_qty = %s, avg_entry_price = %s,
                updated_at = now()
            WHERE symbol = %s;
        """, (pos.get('side'), pos.get('qty'), pos.get('entry_price'), SYMBOL))


def _log_decision(cur=None, ctx=None, action=None, reason=None,
                   model_used=None, model_provider=None, model_latency_ms=None):
    '''Log decision to pm_decision_log. Returns id.'''
    pos = ctx.get('position', {})
    ind = ctx.get('indicators', {})
    vp = ctx.get('vol_profile', {})
    scores = ctx.get('scores', {})
    ps = ctx.get('pos_state', {})
    try:
        cur.execute("""
            INSERT INTO pm_decision_log
                (symbol, position_side, position_qty, avg_entry_price, stage,
                 current_price, long_score, short_score, atr_14, rsi_14,
                 poc, vah, val, chosen_action, action_reason, full_context,
                 model_used, model_provider, model_latency_ms)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb,
                    %s, %s, %s)
            RETURNING id;
        """, (
            SYMBOL,
            pos.get('side'),
            pos.get('qty'),
            pos.get('entry_price'),
            ps.get('stage'),
            ctx.get('price'),
            scores.get('long_score'),
            scores.get('short_score'),
            ind.get('atr'),
            ind.get('rsi'),
            vp.get('poc'),
            vp.get('vah'),
            vp.get('val'),
            action,
            reason,
            json.dumps(ctx, default=str, ensure_ascii=False)[:10000],
            model_used,
            model_provider,
            model_latency_ms,
        ))
        row = cur.fetchone()
        return row[0] if row else None
    except Exception:
        traceback.print_exc()
        return None


def _record_claude_action(action):
    """Record recent Claude action for HOLD repeat detection."""
    _recent_claude_actions.append(action)
    if len(_recent_claude_actions) > CONSECUTIVE_HOLD_LIMIT + 1:
        _recent_claude_actions.pop(0)


def _should_skip_claude_call(position):
    """Return True if last N actions were all HOLD and position unchanged."""
    if len(_recent_claude_actions) < CONSECUTIVE_HOLD_LIMIT:
        return False
    recent = _recent_claude_actions[-CONSECUTIVE_HOLD_LIMIT:]
    return all(a == 'HOLD' for a in recent)


def _reset_hold_tracker(reason=''):
    """Clear HOLD tracker (called on position change or non-HOLD action)."""
    _recent_claude_actions.clear()
    if reason:
        _log(f'hold tracker RESET ({reason})')


def _cycle():
    '''One position management cycle. Returns sleep seconds.'''
    global _prev_scores

    if os.path.exists(KILL_SWITCH_PATH):
        _log('KILL_SWITCH detected. Exiting.')
        sys.exit(0)

    conn = None
    try:
        conn = _db_conn()
        conn.autocommit = True
        with conn.cursor() as cur:
            # Check test lifecycle
            test = test_utils.load_test_mode()
            if not test_utils.is_test_active(test):
                _log('test period ended, sleeping')
                return LOOP_SLOW_SEC

            # Fetch Bybit position
            ex = _get_exchange()
            pos = _fetch_position(ex)
            if pos is None:
                _log('no position, sleeping')
                return LOOP_SLOW_SEC

            # Position change detection → reset edge + hold tracker
            global _prev_position_side
            current_side = pos.get('side') if pos else None
            if _prev_position_side is not None and current_side != _prev_position_side:
                import event_trigger as _et_reset
                _et_reset.reset_edge_state(f'position: {_prev_position_side}->{current_side}')
                _reset_hold_tracker(f'position: {_prev_position_side}->{current_side}')
            _prev_position_side = current_side

            # Phase 1: Real-time snapshot build
            import market_snapshot
            import event_trigger
            snapshot = None
            try:
                snapshot = market_snapshot.build_and_validate(ex, cur, SYMBOL)
            except market_snapshot.SnapshotError as e:
                _log(f'snapshot failed: {e}')
                # Fallback: continue with DB-only context
                pass

            # Phase 2: Build context (using snapshot if available)
            ctx = _build_context(cur, pos, snapshot=snapshot)

            # Phase 3: Event trigger evaluation
            event_result = event_trigger.evaluate(
                snapshot=snapshot, prev_scores=_prev_scores,
                position=pos, cur=cur, symbol=SYMBOL)

            # Phase 4: Mode-based handling
            if event_result.mode == event_trigger.MODE_EMERGENCY:
                _log(f'EMERGENCY: triggers={[t["type"] for t in event_result.triggers]}')
                em_action = _handle_emergency_v2(cur, ctx, event_result, snapshot)
                _record_claude_action(em_action)
                em_types = [t['type'] for t in event_result.triggers]
                event_trigger.record_claude_result(em_action, em_types, pos)
                if em_action and em_action != 'HOLD':
                    _reset_hold_tracker(f'emergency action: {em_action}')
                _prev_scores = ctx.get('scores', {})
                if snapshot and snapshot.get('atr_14') is not None:
                    _prev_scores['atr_14'] = snapshot.get('atr_14')
                return LOOP_FAST_SEC
            elif event_result.mode == event_trigger.MODE_EVENT:
                _trigger_types = [t['type'] for t in event_result.triggers]
                stats = event_trigger.get_event_claude_stats()
                _log(f'EVENT: triggers={_trigger_types} '
                     f'claude_budget={stats["daily_count"]}/{stats["daily_cap"]}')

                # ── Pre-filters (skip entirely) ──
                _suppress_reason = None

                # event_hash 30-min dedup
                if event_trigger.check_event_hash_dedup(event_result.event_hash):
                    _suppress_reason = 'dedupe'
                # §3: HOLD repeat prevention
                elif event_trigger.is_hold_repeat(_trigger_types, pos):
                    _suppress_reason = 'hold_repeat'
                # Consecutive HOLD limit
                elif _should_skip_claude_call(pos):
                    _suppress_reason = 'consecutive_hold'

                if _suppress_reason:
                    _log(f'EVENT suppressed: reason={_suppress_reason} '
                         f'triggers={_trigger_types}')
                    if event_trigger.should_send_telegram_event(
                            _trigger_types + ['_suppress']):
                        _send_telegram(
                            f'EVENT suppressed: reason={_suppress_reason} '
                            f'triggers={_trigger_types}')
                    _prev_scores = ctx.get('scores', {})
                    if snapshot and snapshot.get('atr_14') is not None:
                        _prev_scores['atr_14'] = snapshot.get('atr_14')
                    return LOOP_FAST_SEC

                # Record event_hash for dedup
                event_trigger.record_event_hash(event_result.event_hash)

                # ── Claude vs GPT-mini routing ──
                use_claude, gate_reason = event_trigger.should_use_claude_for_event(
                    snapshot, event_result.triggers)

                if use_claude:
                    _log(f'EVENT → Claude (gate passed: {gate_reason})')
                    ev_action = _handle_event_trigger(cur, ctx, event_result, snapshot)
                    event_trigger.record_event_claude_call()
                else:
                    _log(f'EVENT → GPT-mini (gate denied: {gate_reason})')
                    if event_trigger.should_send_telegram_event(
                            _trigger_types + ['_gate']):
                        _send_telegram(
                            f'EVENT suppressed: reason=cooldown '
                            f'triggers={_trigger_types} → GPT-mini')
                    # Daily cap notification (once per day)
                    if 'daily_cap' in gate_reason and not event_trigger.is_cap_notified():
                        _send_telegram(
                            '[EVENT] Claude 일일 상한 초과 → GPT-mini로 대체')
                        event_trigger.mark_cap_notified()
                    ev_action = _handle_event_trigger_mini(
                        cur, ctx, event_result, snapshot)

                _record_claude_action(ev_action)
                event_trigger.record_claude_result(ev_action, _trigger_types, pos)
                if ev_action and ev_action != 'HOLD':
                    _reset_hold_tracker(f'event action: {ev_action}')

                # Telegram throttle for event notifications
                if not event_trigger.should_send_telegram_event(_trigger_types):
                    _log(f'EVENT telegram throttled (10min)')

                _prev_scores = ctx.get('scores', {})
                if snapshot and snapshot.get('atr_14') is not None:
                    _prev_scores['atr_14'] = snapshot.get('atr_14')
                return LOOP_FAST_SEC
            else:
                # DEFAULT: score_engine only, no Claude call
                _log(f'DEFAULT mode, score_engine only')

            # Run decision engine (DEFAULT mode)
            (action, reason) = _decide(ctx)
            _log(f'decision: {action} - {reason}')

            # Non-HOLD score_engine action → reset hold tracker
            if action != 'HOLD':
                _reset_hold_tracker(f'default action: {action}')

            # Log decision
            dec_id = _log_decision(cur, ctx, action, reason,
                                   model_used='local_score_engine',
                                   model_provider='local',
                                   model_latency_ms=0)

            # Skip if strategy_intent already queued same action
            if action != 'HOLD':
                cur.execute("""
                    SELECT action_type FROM execution_queue
                    WHERE symbol = %s AND source = 'strategy_intent'
                      AND status = 'PENDING' AND ts >= now() - interval '5 minutes';
                """, (SYMBOL,))
                pending_row = cur.fetchone()
                if pending_row:
                    pending_action = pending_row[0]
                    # REVERSE_CLOSE in queue corresponds to REVERSE decision
                    if pending_action == 'REVERSE_CLOSE':
                        pending_action = 'REVERSE'
                    if pending_action == action:
                        _log(f'skip {action}: strategy_intent already pending')
                        action = 'HOLD'
                        reason = 'deferred to strategy_intent'

            # Execute non-HOLD actions
            if action == 'ADD':
                import safety_manager
                add_usdt = safety_manager.get_add_slice_usdt(cur)
                direction = pos.get('side', '').upper()
                _enqueue_action(
                    cur, 'ADD', direction,
                    target_usdt=add_usdt,
                    reason=reason,
                    pm_decision_id=dec_id,
                    priority=5)
            elif action == 'REDUCE':
                _enqueue_action(
                    cur, 'REDUCE', pos.get('side', '').upper(),
                    reduce_pct=30,
                    reason=reason,
                    pm_decision_id=dec_id,
                    priority=3)
            elif action == 'CLOSE':
                _enqueue_action(
                    cur, 'CLOSE', pos.get('side', '').upper(),
                    target_qty=pos.get('qty'),
                    reason=reason,
                    pm_decision_id=dec_id,
                    priority=2)
            elif action == 'REVERSE':
                _enqueue_reverse(
                    cur, pos,
                    reason=reason,
                    pm_decision_id=dec_id,
                    priority=2)

            # Sync position state
            _sync_position_state(cur, pos)
            _prev_scores = ctx.get('scores', {})
            if snapshot and snapshot.get('atr_14') is not None:
                _prev_scores['atr_14'] = snapshot.get('atr_14')

        return LOOP_NORMAL_SEC

    except Exception:
        traceback.print_exc()
        return LOOP_SLOW_SEC
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def main():
    _log('=== POSITION MANAGER START ===')
    import db_migrations
    db_migrations.run_all()
    while True:
        try:
            sleep_sec = _cycle()
            time.sleep(sleep_sec)
        except Exception:
            traceback.print_exc()
            time.sleep(LOOP_SLOW_SEC)


if __name__ == '__main__':
    main()
