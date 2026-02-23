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
import copy
import os
import sys
import time
import json
import threading
import traceback
import urllib.parse
import urllib.request
sys.path.insert(0, '/root/trading-bot/app')
import ccxt
from db_config import get_conn
from dotenv import load_dotenv
import test_utils
import report_formatter
import event_lock
load_dotenv('/root/trading-bot/app/.env')

SYMBOL = 'BTC/USDT:USDT'
KILL_SWITCH_PATH = '/root/trading-bot/app/KILL_SWITCH'

# ── Strategy v2 feature flag (mirrors autopilot_daemon) ──
STRATEGY_V2_ENABLED = os.getenv('STRATEGY_V2_ENABLED', 'shadow')
LOOP_FAST_SEC = 10
LOOP_NORMAL_SEC = 15
LOOP_SLOW_SEC = 30
LOG_PREFIX = '[pos_mgr]'
CALLER = 'position_manager'
_EXIT_ACTIONS = frozenset({'CLOSE', 'REDUCE', 'REVERSE_CLOSE', 'FULL_CLOSE'})

# Build identification — logged on startup for deployment verification
def _get_build_sha():
    try:
        import subprocess
        result = subprocess.run(
            ['git', 'rev-parse', '--short', 'HEAD'],
            capture_output=True, text=True, timeout=5,
            cwd='/root/trading-bot/app')
        return result.stdout.strip() if result.returncode == 0 else 'unknown'
    except Exception:
        return 'unknown'

BUILD_SHA = _get_build_sha()
CONFIG_VERSION = '2026.02.14-db-ctx-relevance-v2'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _db_conn():
    return get_conn()


_exchange = None
_tables_ensured = False
_prev_scores = {}  # Previous cycle scores for regime change detection

# ── HOLD repeat suppression ──────────────────────────────
_recent_claude_actions = []
CONSECUTIVE_HOLD_LIMIT = 3
_prev_position_side = None
_reconcile_cycle_count = 0
_last_cleanup_ts = 0
CLEANUP_INTERVAL_SEC = 300  # cleanup expired locks every 5 min
_prev_had_position = False

# ── Async Claude state ──────────────────────────────────
_claude_thread = None
_claude_thread_lock = threading.Lock()
_claude_result = None
_claude_result_ts = 0
_claude_result_consumed = True
ASYNC_CLAUDE_RESULT_MAX_AGE_SEC = 60  # 결과 유효 시간


def _compute_sl_check(side, entry, price, sl_pct, leverage=5):
    """Compute SL distance with basis conversion support.

    When ff_sl_basis_conversion is ON:
      - PRICE_PCT (legacy): sl_dist = (price - entry) / entry * 100
      - UPNL_PCT: sl_dist = price_move_pct * leverage (margin-based loss %)
      - Threshold is compared as-is against sl_pct

    Returns: (sl_dist: float, hit: bool, basis: str, shadow_log: dict|None)
    """
    import feature_flags as _ff
    from strategy_v3 import config_v3

    if side == 'long':
        price_move_pct = (price - entry) / entry * 100
    else:
        price_move_pct = (entry - price) / entry * 100

    # Legacy (PRICE_PCT) — always computed
    legacy_dist = price_move_pct
    legacy_hit = legacy_dist <= -sl_pct

    if not _ff.is_enabled('ff_sl_basis_conversion'):
        return (legacy_dist, legacy_hit, 'PRICE_PCT', None)

    cfg = config_v3.get_all()
    sl_basis = cfg.get('sl_basis', 'PRICE_PCT').upper()

    if sl_basis == 'UPNL_PCT':
        # Margin-based: price_move * leverage = actual margin loss %
        margin_dist = price_move_pct * leverage
        # Convert sl_pct to UPNL threshold: sl_pct * leverage
        upnl_threshold = sl_pct * leverage
        hit = margin_dist <= -upnl_threshold
        shadow_log = {
            'basis': 'UPNL_PCT',
            'price_move_pct': round(price_move_pct, 4),
            'margin_dist': round(margin_dist, 4),
            'upnl_threshold': round(upnl_threshold, 4),
            'legacy_dist': round(legacy_dist, 4),
            'legacy_hit': legacy_hit,
            'upnl_hit': hit,
            'leverage': leverage,
        }
        _log(f'SL[UPNL_PCT] margin={margin_dist:.2f}% vs -{upnl_threshold:.2f}% '
             f'| legacy={legacy_dist:.2f}% vs -{sl_pct:.2f}% '
             f'| hit={hit} legacy_hit={legacy_hit}')
        return (margin_dist, hit, 'UPNL_PCT', shadow_log)
    elif sl_basis == 'EQUITY_PCT':
        # Future: same as UPNL_PCT for now
        margin_dist = price_move_pct * leverage
        upnl_threshold = sl_pct * leverage
        hit = margin_dist <= -upnl_threshold
        return (margin_dist, hit, 'EQUITY_PCT', None)
    else:
        # Default: PRICE_PCT
        return (legacy_dist, legacy_hit, 'PRICE_PCT', None)


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
        text = report_formatter.korean_output_guard(text)
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


# ── Telegram throttle (non-urgent messages: 2min per msg_type) ──
_tg_throttle_ts = {}  # {msg_type: last_send_timestamp}
_TG_THROTTLE_SEC = 120  # 2 min
_URGENT_MSG_TYPES = {'emergency', 'trade_execution', 'error', 'fill', 'close'}


def _send_telegram_throttled(text, msg_type='info'):
    """Throttled telegram wrapper. Non-urgent: max 1 per msg_type per 2 min."""
    import time as _time
    if msg_type in _URGENT_MSG_TYPES:
        return _send_telegram(text)
    now = _time.time()
    last = _tg_throttle_ts.get(msg_type, 0)
    if now - last < _TG_THROTTLE_SEC:
        return None  # throttled
    _tg_throttle_ts[msg_type] = now
    return _send_telegram(text)


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
        'price': pos.get('mark_price', 0) if pos else 0,
        'cur': cur}

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
            'ema_9': snapshot.get('ema_9'),
            'ema_21': snapshot.get('ema_21'),
            'ema_50': snapshot.get('ema_50'),
            'vwap': snapshot.get('vwap'),
        }
    else:
        # Fallback: DB indicators
        cur.execute("""
                SELECT ich_kijun, rsi_14, atr_14, vol, vol_ma20, vol_spike,
                       bb_mid, bb_up, bb_dn, ich_tenkan, ma_50, ma_200,
                       ema_9, ema_21, ema_50, vwap
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
                'ema_9': float(row[12]) if row[12] else None,
                'ema_21': float(row[13]) if row[13] else None,
                'ema_50': float(row[14]) if row[14] else None,
                'vwap': float(row[15]) if row[15] else None,
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
                   trade_budget_used_pct, next_stage_available,
                   peak_upnl_pct, order_state
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
            'peak_upnl_pct': float(ps_row[6]) if ps_row[6] is not None else 0,
            'order_state': ps_row[7] if ps_row[7] else 'IDLE',
        }
    else:
        ctx['pos_state'] = {}

    # Position entry timestamp (for TIME STOP)
    try:
        cur.execute("""
            SELECT last_fill_at FROM execution_log
            WHERE symbol = %s AND status = 'FILLED' AND order_type IN ('OPEN', 'ADD')
            ORDER BY last_fill_at DESC LIMIT 1;
        """, (SYMBOL,))
        ts_row = cur.fetchone()
        ctx['position_entry_ts'] = ts_row[0].timestamp() if ts_row and ts_row[0] else None
    except Exception:
        ctx['position_entry_ts'] = None

    # News (filter out noise/TIERX/excluded items for cleaner AI context)
    cur.execute("""
            SELECT title, summary, impact_score, ts, title_ko FROM news
            WHERE ts >= now() - interval '2 hours'
              AND (tier IS NULL OR tier NOT IN ('TIERX'))
              AND exclusion_reason IS NULL
            ORDER BY impact_score DESC, ts DESC LIMIT 10;
        """)
    ctx['news'] = [
        {'title': r[0], 'summary': r[1], 'impact_score': r[2], 'ts': str(r[3]),
         'title_ko': r[4] if len(r) > 4 else None}
        for r in cur.fetchall()
    ]

    # Funding rate
    try:
        funding_data = _get_exchange().fetch_funding_rate(SYMBOL)
        ctx['funding_rate'] = float(funding_data.get('fundingRate', 0))
    except Exception:
        ctx['funding_rate'] = 0

    # ── 시장 조건 컨텍스트 (claude_gate 시장 바이패스용) ──
    if snapshot:
        returns = snapshot.get('returns', {})
        ctx['returns'] = returns
        ctx['bar_15m_returns'] = snapshot.get('bar_15m_returns', [])

        # 포지션 손실률 / 청산거리
        ps = ctx.get('pos_state', {})
        price = ctx.get('price', 0)
        entry = ps.get('avg_entry', 0)
        side = ps.get('side', '')
        if entry and entry > 0 and price and price > 0 and side:
            if side.lower() == 'long':
                ctx['position_loss_pct'] = round((entry - price) / entry * 100, 2)
            else:
                ctx['position_loss_pct'] = round((price - entry) / entry * 100, 2)
        else:
            ctx['position_loss_pct'] = 0

        # 청산거리 계산
        bybit_pos = pos or {}
        liq_price = float(bybit_pos.get('liquidationPrice', 0) or 0)
        if liq_price > 0 and price > 0:
            if side and side.lower() == 'long':
                ctx['liq_dist_pct'] = round((price - liq_price) / price * 100, 2)
            elif side and side.lower() == 'short':
                ctx['liq_dist_pct'] = round((liq_price - price) / price * 100, 2)
            else:
                ctx['liq_dist_pct'] = 999
        else:
            ctx['liq_dist_pct'] = 999

    # Decision history for GPT-mini context
    try:
        ctx['decision_history'] = _build_decision_history(cur)
    except Exception:
        traceback.print_exc()
        ctx['decision_history'] = {}

    # Market regime context (FAIL-OPEN: UNKNOWN if unavailable)
    try:
        import regime_reader
        ctx['regime_ctx'] = regime_reader.get_current_regime(cur)
    except Exception:
        ctx['regime_ctx'] = {'regime': 'UNKNOWN', 'available': False}

    return ctx


def _build_decision_history(cur):
    """Build decision history from DB for GPT-mini context injection."""
    history = {}
    # 1) pm_decision_log: 최근 3건
    cur.execute("""
        SELECT chosen_action, action_reason, ts, position_side, model_used
        FROM pm_decision_log WHERE symbol = %s
        ORDER BY ts DESC LIMIT 3;
    """, (SYMBOL,))
    rows = cur.fetchall()
    if rows:
        history['recent_decisions'] = [
            {'action': r[0], 'reason': (r[1] or '')[:100],
             'ts': str(r[2])[:19], 'position_side': r[3], 'model': r[4]}
            for r in rows]
        history['last_action'] = rows[0][0]
        history['last_position_side'] = rows[0][3]

    # 2) event_trigger_log: 최근 1건
    cur.execute("""
        SELECT mode, call_type, claude_result->>'action',
               claude_result->>'reason_code', ts
        FROM event_trigger_log WHERE symbol = %s AND claude_called = true
        ORDER BY ts DESC LIMIT 1;
    """, (SYMBOL,))
    row = cur.fetchone()
    if row:
        history['last_event'] = {'mode': row[0], 'call_type': row[1],
            'action': row[2], 'reason_code': (row[3] or '')[:80],
            'ts': str(row[4])[:19]}

    # 3) execution_queue: 최근 1건
    cur.execute("""
        SELECT action_type, direction, reason, status, ts
        FROM execution_queue WHERE symbol = %s
        ORDER BY ts DESC LIMIT 1;
    """, (SYMBOL,))
    row = cur.fetchone()
    if row:
        history['last_execution'] = {'action_type': row[0], 'direction': row[1],
            'reason': (row[2] or '')[:80], 'status': row[3],
            'ts': str(row[4])[:19]}

    # 4) 파생 플래그: just_closed (30분 이내 청산), hold_suppress
    last_exec = history.get('last_execution', {})
    history['just_closed'] = False
    if last_exec.get('action_type') in ('CLOSE', 'REVERSE_CLOSE'):
        from datetime import datetime, timezone, timedelta
        try:
            exec_ts = datetime.fromisoformat(last_exec['ts'].replace(' ', 'T'))
            if not exec_ts.tzinfo:
                exec_ts = exec_ts.replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) - exec_ts < timedelta(minutes=30):
                history['just_closed'] = True
                history['closed_direction'] = last_exec.get('direction')
        except Exception as e:
            _log(f'just_closed detection error: {e}')

    try:
        locked, _ = event_lock.check_hold_suppress(SYMBOL)
        history['hold_suppress_active'] = locked
    except Exception:
        history['hold_suppress_active'] = False

    return history


def get_db_context_for_prompt(cur=None):
    """Build DB context dict for GPT-mini prompt injection.

    Returns dict with:
      last_position: {side, qty, entry_price, unrealized_pnl}
      last_trade: {action, timestamp, pnl}
      last_reason: str (최근 결정 근거)
      cooldown_active: bool
      recent_decisions: list of last 3 pm_decision_log entries
    """
    ctx = {}
    try:
        # 1) Position state
        cur.execute("""
            SELECT side, total_qty, avg_entry_price, stage,
                   trade_budget_used_pct, last_reason
            FROM position_state WHERE symbol = %s;
        """, (SYMBOL,))
        row = cur.fetchone()
        if row and row[0]:
            ctx['last_position'] = {
                'side': row[0], 'qty': float(row[1] or 0),
                'entry_price': float(row[2] or 0), 'stage': int(row[3] or 0),
                'budget_used_pct': float(row[4] or 0),
            }
            ctx['last_reason'] = (row[5] or '')[:100]
        else:
            ctx['last_position'] = {'side': 'NONE', 'qty': 0}
            ctx['last_reason'] = ''

        # 2) Last trade from execution_log
        cur.execute("""
            SELECT order_type, direction, avg_fill_price, realized_pnl,
                   to_char(last_fill_at AT TIME ZONE 'Asia/Seoul', 'MM-DD HH24:MI') as ts
            FROM execution_log
            WHERE symbol = %s AND status = 'FILLED'
            ORDER BY last_fill_at DESC LIMIT 1;
        """, (SYMBOL,))
        row = cur.fetchone()
        if row:
            ctx['last_trade'] = {
                'action': row[0], 'direction': row[1],
                'price': float(row[2] or 0),
                'pnl': float(row[3]) if row[3] is not None else None,
                'ts': row[4] or '',
            }
        else:
            ctx['last_trade'] = None

        # 3) Recent decisions (last 3)
        cur.execute("""
            SELECT chosen_action, action_reason, actor, confidence,
                   to_char(ts AT TIME ZONE 'Asia/Seoul', 'MM-DD HH24:MI') as ts,
                   position_side, claude_skipped
            FROM pm_decision_log WHERE symbol = %s
            ORDER BY ts DESC LIMIT 3;
        """, (SYMBOL,))
        rows = cur.fetchall()
        ctx['recent_decisions'] = [
            {'action': r[0], 'reason': (r[1] or '')[:80], 'actor': r[2] or 'engine',
             'confidence': r[3], 'ts': r[4], 'position_side': r[5],
             'claude_skipped': r[6]}
            for r in rows
        ]

        # 4) Cooldown/lock state
        try:
            locked, _ = event_lock.check_hold_suppress(SYMBOL)
            ctx['cooldown_active'] = locked
        except Exception:
            ctx['cooldown_active'] = False

    except Exception:
        traceback.print_exc()

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
        json.dumps(ctx, default=str, ensure_ascii=False),
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

    if action == 'SKIP' or result.get('fallback_used'):
        _log(f'CLAUDE_SKIP: API fail → action forced to SKIP (original={action})')
        return 'HOLD'

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
        _send_telegram_throttled(report_formatter.format_event_pre_alert(
            trigger_types, event_result.mode, model='claude',
            snapshot=snapshot), msg_type='event_pre_alert')

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
            json.dumps(result, default=str, ensure_ascii=False),
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

    if action == 'SKIP':
        _log(f'CLAUDE_SKIP: event_trigger API fail → SKIP')
        return 'HOLD'

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
    """Handle event trigger via GPT-4o-mini (1차 결정자).

    Returns (action, result) — action taken + raw result dict for confidence.
    GPT-mini has full action authority (HOLD/REDUCE/CLOSE/OPEN/REVERSE).
    """
    import claude_api
    import save_claude_analysis
    import event_trigger as _et

    trigger_types = [t.get('type', '?') for t in event_result.triggers]

    # Telegram throttle: only send pre-alert if not throttled
    if _et.should_send_telegram_event(trigger_types):
        _send_telegram_throttled(report_formatter.format_event_pre_alert(
            trigger_types, event_result.mode, model='gpt-mini',
            snapshot=snapshot), msg_type='event_pre_alert')

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
            json.dumps(result, default=str, ensure_ascii=False),
            'AUTO_MINI',
            False,
        ))
    except Exception:
        traceback.print_exc()

    if result.get('aborted') or result.get('fallback_used'):
        _log(f'event mini analysis skipped: aborted={result.get("aborted")} '
             f'fallback={result.get("fallback_used")}')
        return ('ABORT', result)

    # Save analysis
    ca_id = None
    try:
        ca_id = save_claude_analysis.save_analysis(
            cur, kind='event_trigger_mini',
            input_packet=ctx,
            output=result,
            event_id=None,
            similar_events=[])
    except Exception:
        traceback.print_exc()

    action = result.get('action') or result.get('recommended_action', 'HOLD')

    if action == 'SKIP':
        _log(f'CLAUDE_SKIP: GPT-mini API fail → SKIP')
        return ('HOLD', result)

    pos = ctx.get('position', {})
    pos_side = (pos.get('side') or '').upper()

    # ── HOLD ──
    if action == 'HOLD':
        pos_label = (pos.get('side') or 'NONE').upper()
        if not pos.get('side'):
            _log('position=NONE → HOLD(대기)')
        return ('HOLD', result)

    # ── REDUCE ──
    if action == 'REDUCE':
        pos_qty = pos.get('qty', 0)
        reduce_pct = result.get('reduce_pct', 25)
        reduce_qty = pos_qty * reduce_pct / 100
        if reduce_qty < _et.MIN_ORDER_QTY_BTC:
            _log(f'GPT-mini REDUCE blocked: qty {reduce_qty:.4f} < min')
            return ('HOLD', result)
        eq_id = _enqueue_action(
            cur, 'REDUCE', pos_side,
            reduce_pct=reduce_pct,
            reason=f'event_mini_{trigger_types[0] if trigger_types else "unknown"}',
            priority=4)
        if ca_id and eq_id:
            try:
                save_claude_analysis.create_pending_outcome(
                    cur, ca_id, 'REDUCE', execution_queue_id=eq_id)
            except Exception:
                traceback.print_exc()
        _send_telegram(report_formatter.format_event_post_alert(
            trigger_types, 'REDUCE (mini)', result))
        return ('REDUCE', result)

    # ── CLOSE ──
    if action == 'CLOSE':
        if not pos_side:
            _log('GPT-mini CLOSE skipped: no position')
            return ('HOLD', result)
        eq_id = _enqueue_action(
            cur, 'CLOSE', pos_side,
            target_qty=pos.get('qty'),
            reason=f'event_mini_{trigger_types[0] if trigger_types else "unknown"}',
            priority=3)
        if ca_id and eq_id:
            try:
                save_claude_analysis.create_pending_outcome(
                    cur, ca_id, 'CLOSE', execution_queue_id=eq_id)
            except Exception:
                traceback.print_exc()
        _send_telegram(report_formatter.format_event_post_alert(
            trigger_types, 'CLOSE (mini)', result))
        return ('CLOSE', result)

    # ── OPEN_LONG / OPEN_SHORT ──
    if action in ('OPEN_LONG', 'OPEN_SHORT'):
        direction = 'LONG' if action == 'OPEN_LONG' else 'SHORT'

        # EVENT GUARD 1: FLAT + EVENT -> chart signal required
        if not pos_side:
            scores = ctx.get('scores', {})
            abs_score = abs(scores.get('total_score', 0))
            if abs_score < 20:
                _log(f'EVENT OPEN blocked: FLAT, abs_score={abs_score} < 20')
                return ('HOLD', result)

        # EVENT GUARD 2 + 2.5 (v14): regime-based EVENT ADD control
        import event_trigger as _et_guard
        regime_ctx = ctx.get('regime_ctx', {})
        regime = regime_ctx.get('regime', 'UNKNOWN') if regime_ctx else 'UNKNOWN'
        if pos_side and pos_side == direction:
            # Same-direction ADD from EVENT: only allow in BREAKOUT
            if not _et_guard.is_event_add_allowed(regime):
                _log(f'EVENT ADD same-dir blocked: {pos_side}+{direction} regime={regime}')
                return ('HOLD', result)
        elif not _et_guard.is_event_add_allowed(regime):
            _log(f'EVENT ADD blocked by regime: {regime} (event_add_blocked=True)')
            return ('HOLD', result)

        # EVENT GUARD 3: cooldown check
        import order_throttle
        action_for_cd = 'ADD' if pos_side else 'OPEN'
        cd_ok, cd_reason, _ = order_throttle.check_cooldown(action_type=action_for_cd)
        if not cd_ok:
            _log(f'EVENT OPEN cooldown: {cd_reason}')
            return ('HOLD', result)

        if pos_side and pos_side != direction:
            _log(f'GPT-mini {action} conflicts with {pos_side} — skipped')
            return ('HOLD', result)
        import safety_manager
        target_stage = result.get('target_stage', 1)
        target_usdt = safety_manager.get_add_slice_usdt(cur) * target_stage
        eq_id = _enqueue_action(
            cur, 'ADD', direction,
            target_usdt=target_usdt,
            reason=f'event_mini_{trigger_types[0] if trigger_types else "unknown"}',
            priority=4)
        if ca_id and eq_id:
            try:
                save_claude_analysis.create_pending_outcome(
                    cur, ca_id, action, execution_queue_id=eq_id)
            except Exception:
                traceback.print_exc()
        _send_telegram(report_formatter.format_event_post_alert(
            trigger_types, f'{action} (mini)', result))
        return ('OPEN', result)

    # ── REVERSE ──
    if action == 'REVERSE':
        if not pos_side:
            _log('GPT-mini REVERSE skipped: no position')
            return ('HOLD', result)

        # EVENT GUARD: REVERSE -> chart confirmation + cooldown
        scores = ctx.get('scores', {})
        total_score = scores.get('total_score', 0)
        if pos_side == 'LONG' and total_score > -20:
            _log(f'EVENT REVERSE blocked: LONG->SHORT but total_score={total_score} > -20')
            return ('HOLD', result)
        if pos_side == 'SHORT' and total_score < 20:
            _log(f'EVENT REVERSE blocked: SHORT->LONG but total_score={total_score} < 20')
            return ('HOLD', result)

        import order_throttle
        cd_ok, cd_reason, _ = order_throttle.check_cooldown(action_type='REVERSE_OPEN')
        if not cd_ok:
            _log(f'EVENT REVERSE cooldown: {cd_reason}')
            return ('HOLD', result)

        eq_id = _enqueue_reverse(
            cur, pos,
            reason=f'event_mini_{trigger_types[0] if trigger_types else "unknown"}',
            priority=3)
        if ca_id and eq_id:
            try:
                save_claude_analysis.create_pending_outcome(
                    cur, ca_id, 'REVERSE', execution_queue_id=eq_id)
            except Exception:
                traceback.print_exc()
        _send_telegram(report_formatter.format_event_post_alert(
            trigger_types, 'REVERSE (mini)', result))
        return ('REVERSE', result)

    return ('HOLD', result)


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
        json.dumps(ctx, default=str, ensure_ascii=False),
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
            json.dumps(result, default=str, ensure_ascii=False),
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

    if action == 'SKIP':
        _log(f'CLAUDE_SKIP: emergency_v2 API fail → SKIP')
        return 'HOLD'

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


def _check_range_split_tp(ctx, regime_params, regime_ctx, upnl_pct, side, price):
    """RANGE split TP: TP1 at POC/BB_mid (50% reduce), TP2 at opposite band (full close).

    Returns (action, reason) or None if no TP triggered.
    """
    try:
        poc = regime_ctx.get('poc')
        vah = regime_ctx.get('vah')
        val = regime_ctx.get('val')

        # Get BB data from ctx indicators (already fetched in _build_context)
        ind = ctx.get('indicators', {})
        bb_mid = ind.get('bb_mid')
        bb_upper = ind.get('bb_up')
        bb_lower = ind.get('bb_dn')
        if bb_mid is not None:
            bb_mid = float(bb_mid)
        if bb_upper is not None:
            bb_upper = float(bb_upper)
        if bb_lower is not None:
            bb_lower = float(bb_lower)

        tp1_close_pct = regime_params.get('tp1_close_pct', 50)

        # Check if TP1 already taken: any FILLED REDUCE since last OPEN
        tp1_taken = False
        conn_tp = None
        try:
            conn_tp = get_conn()
            with conn_tp.cursor() as cur_tp:
                # Find the most recent OPEN fill time
                cur_tp.execute("""
                    SELECT last_fill_at FROM execution_log
                    WHERE symbol = %s AND order_type = 'OPEN' AND status = 'FILLED'
                    ORDER BY last_fill_at DESC LIMIT 1;
                """, (SYMBOL,))
                open_row = cur_tp.fetchone()
                if open_row and open_row[0]:
                    cur_tp.execute("""
                        SELECT 1 FROM execution_log
                        WHERE symbol = %s AND order_type = 'REDUCE' AND status = 'FILLED'
                          AND last_fill_at >= %s
                        LIMIT 1;
                    """, (SYMBOL, open_row[0]))
                    tp1_taken = cur_tp.fetchone() is not None
        except Exception as e:
            _log(f'TP1 duplicate check error (FAIL-OPEN): {e}')
        finally:
            if conn_tp:
                try:
                    conn_tp.close()
                except Exception:
                    pass

        if not tp1_taken:
            # TP1: price reaches POC or BB_mid
            tp1_hit = False
            tp1_ref = ''
            if side == 'long':
                if poc and price >= poc:
                    tp1_hit = True
                    tp1_ref = f'POC={poc:.0f}'
                elif bb_mid and price >= bb_mid:
                    tp1_hit = True
                    tp1_ref = f'BB_mid={bb_mid:.0f}'
            else:  # short
                if poc and price <= poc:
                    tp1_hit = True
                    tp1_ref = f'POC={poc:.0f}'
                elif bb_mid and price <= bb_mid:
                    tp1_hit = True
                    tp1_ref = f'BB_mid={bb_mid:.0f}'

            if tp1_hit:
                return ('REDUCE',
                        f'RANGE TP1: {tp1_ref} 도달 → {tp1_close_pct}% 청산 (uPnL={upnl_pct:.2f}%)')

        # TP2: price reaches opposite band (VAH/BB_upper for LONG, VAL/BB_lower for SHORT)
        tp2_hit = False
        tp2_ref = ''
        if side == 'long':
            if vah and price >= vah:
                tp2_hit = True
                tp2_ref = f'VAH={vah:.0f}'
            elif bb_upper and price >= bb_upper:
                tp2_hit = True
                tp2_ref = f'BB_upper={bb_upper:.0f}'
        else:  # short
            if val and price <= val:
                tp2_hit = True
                tp2_ref = f'VAL={val:.0f}'
            elif bb_lower and price <= bb_lower:
                tp2_hit = True
                tp2_ref = f'BB_lower={bb_lower:.0f}'

        if tp2_hit:
            return ('CLOSE',
                    f'RANGE TP2: {tp2_ref} 도달 → 전량 청산 (uPnL={upnl_pct:.2f}%)')

        # Fallback: tp_pct_max
        tp_max = regime_params.get('tp_pct_max', 0.90)
        if tp_max > 0 and upnl_pct >= tp_max:
            return ('CLOSE', f'RANGE TP (fallback): uPnL {upnl_pct:.2f}% >= {tp_max}%')

    except Exception as e:
        import traceback
        traceback.print_exc()

    return None


def _check_v3_take_profit(ctx, regime_ctx):
    """V3 dynamic TP: ATR-based target + trailing from peak.

    Two TP triggers (whichever fires first):
      1. Fixed: uPnL% >= v3_tp_pct (ATR × R-ratio based)
      2. Trailing: peak drawdown >= v3_tp_pct when peak >= v3_tp_pct

    Returns (action, reason) or None.
    FAIL-OPEN: returns None on any error.
    """
    try:
        from strategy_v3.config_v3 import is_enabled
        if not is_enabled():
            return None

        pos = ctx.get('position', {})
        side = pos.get('side', '')
        entry = pos.get('entry_price', 0)
        price = ctx.get('price', 0)
        if not side or not entry or entry <= 0 or not price:
            return None

        # Calculate uPnL%
        if side == 'long':
            upnl_pct = (price - entry) / entry * 100
        else:
            upnl_pct = (entry - price) / entry * 100

        # Get V3 risk params
        from strategy.common.features import build_feature_snapshot
        cur = ctx.get('cur')
        if not cur:
            return None
        features = build_feature_snapshot(cur)
        if not features:
            return None

        from strategy_v3.regime_v3 import classify
        from strategy_v3.risk_v3 import compute_risk
        v3_regime = classify(features, regime_ctx or {})
        v3_risk = compute_risk(features, v3_regime)

        tp_pct = v3_risk['tp_pct'] * 100  # convert to percentage

        # 1. Fixed TP: uPnL >= v3_tp_pct
        if upnl_pct >= tp_pct:
            return ('CLOSE',
                    f'V3 TP (fixed): uPnL {upnl_pct:.2f}% >= {tp_pct:.2f}% '
                    f'(ATR-based, regime={v3_regime["regime_class"]})')

        # 2. Trailing TP: if peak was above tp_pct, close on drawdown
        peak = ctx.get('pos_state', {}).get('peak_upnl_pct', 0) or 0
        if peak >= tp_pct:
            drawdown = peak - upnl_pct
            # Trail distance = same as tp_pct (1R trailing)
            trail = tp_pct * 0.5  # trail at 50% of TP distance
            if drawdown >= trail:
                return ('CLOSE',
                        f'V3 TP (trailing): peak={peak:.2f}% now={upnl_pct:.2f}% '
                        f'drawdown={drawdown:.2f}% >= trail={trail:.2f}% '
                        f'(regime={v3_regime["regime_class"]})')

        return None
    except Exception as e:
        _log(f'V3 TP check FAIL-OPEN: {e}')
        return None


def _check_take_profit(ctx, regime_params, regime_ctx):
    """Check regime-based take-profit conditions.

    Returns (action, reason) or None if no TP triggered.

    tp_mode='fixed': close when uPnL% >= tp_pct_max
    tp_mode='trailing': track peak uPnL, close when drawdown from peak >= trail_pct
    """
    if not regime_params or not regime_ctx or not regime_ctx.get('available'):
        return None

    pos = ctx.get('position', {})
    side = pos.get('side', '')
    entry = pos.get('entry_price', 0)
    price = ctx.get('price', 0)
    if not side or not entry or entry <= 0 or not price:
        return None

    # Calculate uPnL%
    if side == 'long':
        upnl_pct = (price - entry) / entry * 100
    else:
        upnl_pct = (entry - price) / entry * 100

    tp_mode = regime_params.get('tp_mode', 'fixed')

    if tp_mode == 'fixed':
        # Check split TP for RANGE mode
        if regime_params.get('tp_split_enabled') and regime_ctx.get('regime') == 'RANGE':
            split_result = _check_range_split_tp(ctx, regime_params, regime_ctx, upnl_pct, side, price)
            if split_result:
                return split_result

        tp_max = regime_params.get('tp_pct_max', 1.5)
        if tp_max > 0 and upnl_pct >= tp_max:
            return ('CLOSE', f'REGIME TP (fixed): uPnL {upnl_pct:.2f}% >= {tp_max}%')
        return None

    if tp_mode == 'trailing':
        trail_activate = regime_params.get('trail_activate_pct', 0.5)
        trail_pct = regime_params.get('trail_pct', 0.8)
        peak = ctx.get('pos_state', {}).get('peak_upnl_pct', 0) or 0

        # Update peak if current is higher
        if upnl_pct > peak:
            peak = upnl_pct

        if peak >= trail_activate:
            drawdown = peak - upnl_pct
            if drawdown >= trail_pct:
                return ('CLOSE',
                        f'REGIME TP (trailing): peak={peak:.2f}% drawdown={drawdown:.2f}% >= {trail_pct}%')
        return None

    return None


def _v2_position_check(ctx, side, stage, price):
    """Strategy v2 mode-aware position check.

    Returns (action, reason) tuple if v2 has a recommendation, or None.
    Only handles EXIT-type decisions from v2 modes. ADD is handled by autopilot.
    FAIL-OPEN: returns None on any error.
    """
    try:
        from strategy.common.features import build_feature_snapshot
        from strategy.regime_router import route, get_mode_config
        from strategy.modes.static_range import StaticRangeStrategy
        from strategy.modes.volatile_range import VolatileRangeStrategy
        from strategy.modes.shock_breakout import ShockBreakoutStrategy

        # Need a DB cursor — borrow from ctx if available
        regime_ctx = ctx.get('regime_ctx', {})
        vp = ctx.get('vol_profile', {})
        ind = ctx.get('indicators', {})

        # Build minimal feature snapshot from existing ctx data
        features = {
            'symbol': SYMBOL,
            'price': price,
            'adx': regime_ctx.get('adx_14'),
            'bb_width': regime_ctx.get('bb_width_pct') / 100 if regime_ctx.get('bb_width_pct') is not None else None,
            'poc': vp.get('poc'),
            'vah': vp.get('vah'),
            'val': vp.get('val'),
            'range_position': None,
            'drift_score': 0,
            'drift_direction': 'NONE',
            'poc_slope': None,
            'atr_pct': None,
            'volume_z': None,
            'impulse': None,
        }

        # Compute range_position if we have price/vah/val
        if price and vp.get('vah') and vp.get('val'):
            vah_f, val_f = float(vp['vah']), float(vp['val'])
            if vah_f != val_f:
                features['range_position'] = (float(price) - val_f) / (vah_f - val_f)

        position = {
            'side': side.upper() if side else '',
            'total_qty': float(ctx.get('position', {}).get('qty', 0) or 0),
            'avg_entry_price': float(ctx.get('position', {}).get('entry_price', 0) or 0),
            'stage': stage,
        }

        route_result = route(features, current_position=position)
        mode = route_result['mode']

        mode_config = get_mode_config(mode)
        strategies = {'A': StaticRangeStrategy(), 'B': VolatileRangeStrategy(), 'C': ShockBreakoutStrategy()}
        strategy = strategies.get(mode)
        if not strategy:
            return None

        decision_ctx = {
            'features': features,
            'position': position,
            'regime_ctx': regime_ctx,
            'config': mode_config,
            'price': price,
            'indicators': ind,
            'vol_profile': vp,
            'candles': ctx.get('candles', []),
            'drift_submode': route_result.get('drift_submode'),
        }

        decision = strategy.decide(decision_ctx)
        if decision['action'] == 'EXIT':
            return (decision['action'], decision.get('reason', 'v2 exit'))
        return None

    except Exception as e:
        _log(f'v2_position_check FAIL-OPEN: {e}')
        return None


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
    total_score = scores.get('total_score', 0)
    stage = ps.get('stage', 0)
    news = ctx.get('news', [])

    # No position -> HOLD (autopilot_daemon handles entries)
    if not pos or not side:
        return ('HOLD', 'no position')

    # Order in flight — wait for fill confirmation
    order_state = ps.get('order_state', 'IDLE')
    if order_state == 'SENT':
        return ('HOLD', 'order in flight — waiting for fill confirmation')

    # [4-2] Chandelier Exit check (ff_unified_engine_v11)
    try:
        import feature_flags as _ff_ch
        if _ff_ch.is_enabled('ff_unified_engine_v11'):
            if pos and pos.get('side') and pos.get('qty', 0) > 0:
                from chandelier_exit import get_tracker
                from mtf_direction import compute_mtf_direction
                _ch = get_tracker()
                _price = price

                # Initialize tracker if needed
                if _ch.entry_price is None and pos.get('entry_price', 0) > 0:
                    _ch.on_entry(pos['side'], pos['entry_price'])

                if _ch.entry_price is not None and _price > 0:
                    _ch.update(_price)

                    # Get ATR_15m
                    _atr_15m = 0
                    try:
                        cur = ctx.get('cur')
                        if cur:
                            cur.execute("SELECT atr_15m FROM mtf_indicators WHERE symbol = %s;", (SYMBOL,))
                            _r = cur.fetchone()
                            if _r and _r[0]:
                                _atr_15m = float(_r[0])
                    except Exception:
                        pass

                    if _atr_15m > 0:
                        exit_signal = _ch.check_exit(_price, _atr_15m)
                        if exit_signal:
                            _log(f'[CHANDELIER] {exit_signal[1]}')
                            return exit_signal

                    # [4-3] Regime transition: MTF→NO_TRADE tightens time_stop
                    try:
                        cur = ctx.get('cur')
                        if cur:
                            mtf = compute_mtf_direction(cur)
                            if mtf['direction'] == 'NO_TRADE':
                                _ch.time_stop_hours = min(_ch.time_stop_hours, 6)
                    except Exception:
                        pass
    except Exception as e:
        _log(f'[CHANDELIER] error (FAIL-OPEN): {e}')

    dominant = scores.get('dominant_side', 'LONG')

    # ── Strategy v2: mode-aware exit/add check ──
    if STRATEGY_V2_ENABLED != 'off':
        v2_result = _v2_position_check(ctx, side, stage, price)
        if v2_result:
            v2_action, v2_reason = v2_result
            if STRATEGY_V2_ENABLED == 'on' and v2_action in ('CLOSE', 'REDUCE'):
                return (v2_action, f'v2: {v2_reason}')
            elif STRATEGY_V2_ENABLED == 'shadow':
                _log(f'v2[shadow] would: {v2_action} — {v2_reason}')

    # ── Regime-based TP check (before SL) ──
    regime_ctx = ctx.get('regime_ctx', {})
    regime_params = None
    if regime_ctx.get('available'):
        try:
            import regime_reader
            regime_params = regime_reader.get_regime_params(
                regime_ctx.get('regime', 'UNKNOWN'), regime_ctx.get('shock_type'))
        except Exception as e:
            _log(f'regime_params load error: {e}')
    # ── V3 dynamic TP check (ATR-based, runs before regime TP) ──
    v3_tp_result = _check_v3_take_profit(ctx, regime_ctx)
    if v3_tp_result:
        return v3_tp_result

    if regime_params:
        tp_result = _check_take_profit(ctx, regime_params, regime_ctx)
        if tp_result:
            return tp_result

    # ── TIME STOP (RANGE mode only) ──
    if regime_ctx.get('regime') == 'RANGE' and regime_params:
        time_stop_min = regime_params.get('time_stop_min', 25)
        entry_ts = ctx.get('position_entry_ts')
        if entry_ts:
            import time as _time
            elapsed_min = (_time.time() - entry_ts) / 60
            if elapsed_min >= time_stop_min:
                return ('CLOSE', f'TIME STOP: {elapsed_min:.0f}min >= {time_stop_min}min (RANGE)')

    # ── Trailing SL to breakeven (BREAKOUT mode) ──
    be_sl_active = False
    if regime_params and regime_ctx.get('regime') == 'BREAKOUT' and entry and entry > 0:
        be_threshold = regime_params.get('trailing_be_threshold_pct', 0.8)
        if side == 'long':
            upnl_for_be = (price - entry) / entry * 100
        else:
            upnl_for_be = (entry - price) / entry * 100
        if upnl_for_be >= be_threshold:
            be_sl_active = True
            fee_buffer_pct = 0.12  # estimated round-trip fees

    # Stop-loss check
    if atr and atr > 0 and entry and entry > 0:
        # Stage-based stop-loss tightening (v2.1)
        sl_base = scores.get('dynamic_stop_loss_pct', 2.0)
        # Regime SL clamping: use the tighter of dynamic SL and regime SL
        if regime_params:
            regime_sl = regime_params.get('sl_pct', 2.0)
            sl_base = min(sl_base, regime_sl)
        if stage >= 3:
            sl_pct = min(sl_base, 1.6)
        elif stage >= 2:
            sl_pct = min(sl_base, 1.8)
        else:
            sl_pct = sl_base

        # Trailing BE: tighten SL to breakeven + fees when in profit
        if be_sl_active:
            sl_pct = min(sl_pct, fee_buffer_pct)

        # P0-1: SL basis conversion (PRICE_PCT / UPNL_PCT)
        _leverage = ctx.get('leverage', 5)
        sl_dist, sl_hit, sl_basis, sl_shadow = _compute_sl_check(
            side, entry, price, sl_pct, leverage=_leverage)

        if sl_hit:
            be_tag = ' [BE trail]' if be_sl_active else ''
            basis_tag = f' [{sl_basis}]' if sl_basis != 'PRICE_PCT' else ''
            return ('CLOSE', f'stop_loss hit ({sl_dist:.2f}% vs -{sl_pct}%){be_tag}{basis_tag}')

    # Reversal / Close check (v3.0: total_score based)
    if side == 'long' and total_score <= -25:
        confirms = _structure_confirms(ctx, 'SHORT')
        if confirms >= 2:
            return ('REVERSE', f'strong SHORT reversal (total_score={total_score}, confirms={confirms})')
        return ('CLOSE', f'SHORT signal without structure (total_score={total_score}, confirms={confirms})')

    if side == 'short' and total_score >= 25:
        confirms = _structure_confirms(ctx, 'LONG')
        if confirms >= 2:
            return ('REVERSE', f'strong LONG reversal (total_score={total_score}, confirms={confirms})')
        return ('CLOSE', f'LONG signal without structure (total_score={total_score}, confirms={confirms})')

    # Reduce on counter signal (v3.0: total_score based)
    if side == 'long' and total_score <= -15:
        return ('REDUCE', f'counter signal (total_score={total_score})')
    if side == 'short' and total_score >= 15:
        return ('REDUCE', f'counter signal (total_score={total_score})')

    # ADD check (v3.0: profit-zone gate + regime-unified threshold)
    regime_ctx = ctx.get('regime_ctx', {})
    regime_stage_max = 7  # FAIL-OPEN default
    regime_params = None
    if regime_ctx.get('available'):
        import regime_reader
        regime_stage_max = regime_reader.get_stage_limit(
            regime_ctx.get('regime', 'UNKNOWN'), regime_ctx.get('shock_type'))
        regime_params = regime_reader.get_regime_params(
            regime_ctx.get('regime', 'UNKNOWN'), regime_ctx.get('shock_type'))
    if regime_stage_max == 0:
        return ('HOLD', f'REGIME VETO: ADD 차단 ({regime_ctx.get("regime")})')

    # D0-1: 전역 ADD 차단 (ff_global_add_block) — 손익 무관 전면 차단
    import feature_flags as _ff
    if _ff.is_enabled('ff_global_add_block'):
        return ('HOLD', 'GLOBAL_ADD_BLOCK: 전역 ADD 차단 활성')

    # P0-3: Hard ADD block in loss zone (unconditional when ff enabled)
    if _ff.is_enabled('ff_loss_zone_add_block'):
        if entry and entry > 0:
            if side == 'long':
                _upnl = (price - entry) / entry * 100
            else:
                _upnl = (entry - price) / entry * 100
            if _upnl < 0:
                return ('HOLD', f'ADD HARD-BLOCK: 손실구간 ({_upnl:.2f}%)')

    # Profit-zone-only ADD (loss-zone averaging forbidden)
    if entry and entry > 0:
        if side == 'long':
            upnl_pct = (price - entry) / entry * 100
        else:
            upnl_pct = (entry - price) / entry * 100
        add_min_profit = 0.25
        if regime_params:
            add_min_profit = regime_params.get('add_min_profit_pct', 0.25)
        if upnl_pct < add_min_profit:
            return ('HOLD', f'ADD 차단: 손실구간 물타기 금지 (uPnL={upnl_pct:.2f}%)')

    # Unified ADD threshold from regime params
    add_threshold = 45
    if regime_params:
        add_threshold = regime_params.get('add_score_threshold', 45)

    if stage < regime_stage_max and ps.get('budget_used_pct', 0) < 70:
        direction = 'LONG' if side == 'long' else 'SHORT'
        if dominant == direction:
            relevant = long_score if direction == 'LONG' else short_score
            if relevant >= add_threshold:
                # [3-3] ADD +1R condition (ff_unified_engine_v11)
                try:
                    import feature_flags as _ff_add
                    if _ff_add.is_enabled('ff_unified_engine_v11'):
                        from chandelier_exit import get_tracker
                        _ch_add = get_tracker()
                        if _ch_add.sl_distance and _ch_add.sl_distance > 0:
                            _unrealized_r = _ch_add._compute_unrealized_r(price)
                            if _unrealized_r < 1.0:
                                return ('HOLD', f'[v1.1] ADD blocked: {_unrealized_r:.2f}R < 1.0R')
                except Exception as _e_add:
                    _log(f'[v1.1] ADD +1R check error (FAIL-OPEN): {_e_add}')
                return ('ADD', f'score {relevant} favors {direction}, stage={stage}/{regime_stage_max}')

    return ('HOLD', 'no action needed')


def _structure_confirms(ctx=None, target_side=None):
    '''Count structure confirmations for reversal. Returns 0-5.
    Threshold: 2. Added EMA(9/21) confirmation.'''
    confirms = 0
    ind = ctx.get('indicators', {})
    price = ctx.get('price', 0)

    tenkan = ind.get('tenkan')
    kijun = ind.get('kijun')
    rsi = ind.get('rsi')
    ma50 = ind.get('ma_50')
    ma200 = ind.get('ma_200')
    ema_9 = ind.get('ema_9')
    ema_21 = ind.get('ema_21')

    if target_side == 'LONG':
        if tenkan is not None and kijun is not None and tenkan > kijun:
            confirms += 1
        if rsi is not None and rsi < 40:
            confirms += 1
        if ma50 is not None and ma200 is not None and ma50 > ma200:
            confirms += 1
        if price and kijun and price > kijun:
            confirms += 1
        if ema_9 is not None and ema_21 is not None and ema_9 > ema_21:
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
        if ema_9 is not None and ema_21 is not None and ema_9 < ema_21:
            confirms += 1

    return confirms


def _build_leverage_context(ctx):
    """Build leverage context dict for executor from market context."""
    ind = ctx.get('indicators', {})
    scores = ctx.get('scores', {})
    price = ctx.get('price', 0)
    atr = ind.get('atr', 0)
    atr_pct = (atr / price * 100) if price and atr else 1.0
    regime_ctx = ctx.get('regime_ctx', {})
    return {
        'atr_pct': round(atr_pct, 3),
        'regime_score': scores.get('regime_score', 0),
        'news_event_score': scores.get('news_event_score', 0),
        'confidence': scores.get('confidence', 0),
        'stage': ctx.get('pos_state', {}).get('stage', 0),
        'regime': regime_ctx.get('regime'),
        'shock_type': regime_ctx.get('shock_type'),
    }


def _enqueue_action(cur=None, action_type=None, direction=None, **kwargs):
    '''Insert action into execution_queue.'''
    import safety_manager

    # D0-1: Global ADD block — covers ALL ADD paths (event, emergency, score engine)
    if action_type == 'ADD':
        try:
            import feature_flags as _ff_eq
            if _ff_eq.is_enabled('ff_global_add_block'):
                _log(f'ENQUEUE BLOCKED: ADD {direction} — ff_global_add_block')
                return None
        except Exception:
            pass

    # v14.1: Throttle pre-gate — block ENTRY actions at enqueue time
    # This prevents execution_queue flooding when rate limits are active
    if action_type not in _EXIT_ACTIONS and direction:
        try:
            import order_throttle
            import regime_reader
            regime_ctx = regime_reader.get_current_regime(cur)
            regime = regime_ctx.get('regime') if regime_ctx.get('available') else None
            throttle_ok, throttle_reason, _ = order_throttle.check_all(
                cur, action_type, direction=direction, regime=regime)
            if not throttle_ok:
                _log(f'ENQUEUE_THROTTLE_BLOCKED: {action_type} {direction} — {throttle_reason}')
                _send_telegram_throttled(
                    f'⛔ {action_type} BLOCKED: {throttle_reason}',
                    msg_type='enqueue_throttle')
                return None
        except Exception as e:
            _log(f'enqueue throttle pre-gate error (FAIL-OPEN): {e}')

    # Duplicate action check: block if same action_type already PENDING/PICKED
    # CLOSE 계열은 SENT도 차단 → 첫 CLOSE가 SENT인 동안 재enqueue 방지
    if action_type in ('REDUCE', 'CLOSE', 'ADD', 'REVERSE_CLOSE', 'REVERSE_OPEN') and direction:
        if action_type in ('CLOSE', 'FULL_CLOSE', 'REDUCE', 'REVERSE_CLOSE'):
            dedup_statuses = ('PENDING', 'PICKED', 'SENT')
        else:
            dedup_statuses = ('PENDING', 'PICKED')
        cur.execute("""
            SELECT id FROM execution_queue
            WHERE symbol = %s AND action_type = %s AND direction = %s
              AND status IN %s
              AND ts >= now() - interval '5 minutes';
        """, (SYMBOL, action_type, direction, dedup_statuses))
        if cur.fetchone():
            _log(f'duplicate {action_type} {direction} blocked (already pending in queue)')
            return None

    emergency = kwargs.get('emergency_mode', False)
    (ok, reason) = safety_manager.run_all_checks(
        cur, kwargs.get('target_usdt', 0), emergency=emergency)
    if not ok and action_type == 'ADD':
        _log(f'safety block: {reason}')
        _send_telegram_throttled(
            f'🚫 주문 거부 — 사유: {report_formatter._kr_safety_reason(reason)}',
            msg_type='order_reject')
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


def _sync_position_state(cur=None, pos=None, upnl_pct=None):
    '''Sync position_state with Bybit reality.'''
    if pos is None:
        cur.execute("""
            UPDATE position_state SET
                side = NULL, total_qty = 0, stage = 0,
                capital_used_usdt = 0, trade_budget_used_pct = 0,
                peak_upnl_pct = 0,
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
        # Track peak uPnL% for trailing TP
        if upnl_pct is not None:
            try:
                cur.execute("""
                    UPDATE position_state
                    SET peak_upnl_pct = GREATEST(COALESCE(peak_upnl_pct, 0), %s)
                    WHERE symbol = %s;
                """, (upnl_pct, SYMBOL))
            except Exception as e:
                _log(f'peak_upnl_pct update error: {e}')


def _log_decision(cur=None, ctx=None, action=None, reason=None,
                   model_used=None, model_provider=None, model_latency_ms=None,
                   actor='engine', candidate_action=None, final_action=None,
                   confidence=None, claude_skipped=False):
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
                 model_used, model_provider, model_latency_ms,
                 actor, candidate_action, final_action, confidence, claude_skipped)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb,
                    %s, %s, %s, %s, %s, %s, %s, %s)
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
            json.dumps(ctx, default=str, ensure_ascii=False),
            model_used,
            model_provider,
            model_latency_ms,
            actor,
            candidate_action,
            final_action or action,
            confidence,
            claude_skipped,
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


def _run_async_claude(ctx, event_result, snapshot, reason):
    """Background thread: run Claude analysis asynchronously.

    1. Own DB connection
    2. claude_api.event_trigger_analysis() (blocking in this thread, non-blocking main)
    3. Filter: OPEN_LONG/OPEN_SHORT/REVERSE → HOLD (risk management only)
    4. Store result in _claude_result (lock-protected)
    5. DB logging
    6. Cleanup
    """
    global _claude_result, _claude_result_ts, _claude_result_consumed
    thread_conn = None
    try:
        import claude_api
        import event_trigger as _et

        _log(f'[async_claude] thread started: reason={reason}')

        # Own DB connection for thread safety
        thread_conn = _db_conn()
        thread_conn.autocommit = True
        thread_cur = thread_conn.cursor()

        result = claude_api.event_trigger_analysis(ctx, snapshot, event_result)

        action = result.get('action') or result.get('recommended_action', 'HOLD')

        # Filter: only risk-management actions allowed from async Claude
        original_action = action
        if action in ('OPEN_LONG', 'OPEN_SHORT', 'REVERSE'):
            _log(f'[async_claude] {action} → HOLD (async only allows risk mgmt)')
            action = 'HOLD'
            result['async_downgraded_from'] = original_action

        result['action'] = action
        result['async_claude'] = True
        result['async_reason'] = reason

        # Store result (lock-protected)
        with _claude_thread_lock:
            _claude_result = result
            _claude_result_ts = time.time()
            _claude_result_consumed = False

        # DB logging: event_trigger_log
        try:
            trigger_types = [t.get('type', '?') for t in event_result.triggers]
            thread_cur.execute("""
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
                json.dumps(result, default=str, ensure_ascii=False),
                'ASYNC_CLAUDE',
                False,
            ))
        except Exception:
            traceback.print_exc()

        # Record Claude call for cooldown tracking
        _et.record_event_claude_call()

        _log(f'[async_claude] done: action={action} '
             f'(original={original_action}) confidence={result.get("confidence")}')

    except Exception:
        _log('[async_claude] ERROR in background thread:')
        traceback.print_exc()
    finally:
        if thread_conn:
            try:
                thread_conn.close()
            except Exception:
                pass


def _spawn_async_claude(ctx, event_result, snapshot, reason):
    """Spawn background thread for async Claude analysis.

    1. Lock check: skip if thread already alive
    2. Deep copy ctx/snapshot for thread safety
    3. Start daemon thread
    """
    global _claude_thread

    with _claude_thread_lock:
        if _claude_thread is not None and _claude_thread.is_alive():
            _log('[async_claude] skipped: thread already running')
            return False

        # Deep copy for thread safety — exclude non-picklable objects (cursor, conn)
        _skip_keys = {'cur', 'conn', 'cursor', 'connection'}
        ctx_safe = {k: v for k, v in ctx.items() if k not in _skip_keys}
        ctx_copy = copy.deepcopy(ctx_safe)
        snapshot_copy = copy.deepcopy(snapshot)

        _claude_thread = threading.Thread(
            target=_run_async_claude,
            args=(ctx_copy, event_result, snapshot_copy, reason),
            daemon=True,
            name='async_claude')
        _claude_thread.start()
        _log(f'[async_claude] spawned: reason={reason}')
        return True


def _check_async_claude_result(cur):
    """Check and consume async Claude result if available.

    Called at the start of each _cycle().
    Returns action string or None.
    """
    global _claude_result, _claude_result_ts, _claude_result_consumed

    with _claude_thread_lock:
        if _claude_result_consumed or _claude_result is None:
            return None

        age = time.time() - _claude_result_ts
        if age > ASYNC_CLAUDE_RESULT_MAX_AGE_SEC:
            _log(f'[async_claude] result expired: age={age:.1f}s > {ASYNC_CLAUDE_RESULT_MAX_AGE_SEC}s')
            _claude_result_consumed = True
            return None

        result = _claude_result
        _claude_result_consumed = True

    action = result.get('action', 'HOLD')
    reason = result.get('async_reason', '?')
    _log(f'[async_claude] processing result: action={action} age={age:.1f}s '
         f'claude_waited=false reason={reason}')

    if action == 'REDUCE':
        pos_side = ''
        reduce_pct = result.get('reduce_pct', 50)
        # Need current position info
        try:
            ex = _get_exchange()
            pos = _fetch_position(ex)
            if pos:
                pos_side = (pos.get('side') or '').upper()
                pos_qty = pos.get('qty', 0)
                import event_trigger as _et
                reduce_qty = pos_qty * reduce_pct / 100
                if reduce_qty < _et.MIN_ORDER_QTY_BTC:
                    _log(f'[async_claude] REDUCE blocked: qty {reduce_qty:.4f} < min')
                    _send_telegram(report_formatter.format_async_claude_result(
                        'HOLD (수량 부족)', result, reason))
                    return 'HOLD'
            else:
                _log('[async_claude] REDUCE skipped: no position')
                return None
        except Exception:
            traceback.print_exc()
            return None

        _enqueue_action(
            cur, 'REDUCE', pos_side,
            reduce_pct=reduce_pct,
            reason=f'async_claude_{reason}',
            priority=3)
        _send_telegram(report_formatter.format_async_claude_result(
            'REDUCE', result, reason))
        return 'REDUCE'

    if action == 'CLOSE':
        try:
            ex = _get_exchange()
            pos = _fetch_position(ex)
            if pos:
                pos_side = (pos.get('side') or '').upper()
                _enqueue_action(
                    cur, 'CLOSE', pos_side,
                    target_qty=pos.get('qty'),
                    reason=f'async_claude_{reason}',
                    priority=2)
                _send_telegram(report_formatter.format_async_claude_result(
                    'CLOSE', result, reason))
                return 'CLOSE'
            else:
                _log('[async_claude] CLOSE skipped: no position')
                return None
        except Exception:
            traceback.print_exc()
            return None

    # HOLD — just send confirmation
    if action == 'HOLD':
        _send_telegram(report_formatter.format_async_claude_result(
            'HOLD', result, reason))
        return 'HOLD'

    return None


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

            # Phase 0: Check async Claude result from previous cycle
            async_action = _check_async_claude_result(cur)
            if async_action and async_action not in ('HOLD', None):
                _reset_hold_tracker(f'async_claude action: {async_action}')

            # Fetch Bybit position
            ex = _get_exchange()
            pos = _fetch_position(ex)

            # [0-1] Orphan cleanup: detect pos→0 transition
            global _prev_had_position
            if pos is None or pos.get('qty', 0) < 1e-09:
                if _prev_had_position:
                    try:
                        from orphan_cleanup import cleanup_if_flat
                        cleaned, detail = cleanup_if_flat(ex, SYMBOL, reason='pm_pos_zero_detected')
                        if cleaned:
                            _log(f'[PM_CLEANUP] pos→0 detected: {detail}')
                    except Exception as e:
                        _log(f'[PM_CLEANUP] error (FAIL-OPEN): {e}')
                    _prev_had_position = False
            else:
                _prev_had_position = True

            if pos is None:
                _log('position=NONE → HOLD(대기)')
                return LOOP_SLOW_SEC

            # Position change detection → reset edge + hold tracker
            global _prev_position_side
            current_side = pos.get('side') if pos else None
            if _prev_position_side is not None and current_side != _prev_position_side:
                import event_trigger as _et_reset
                _et_reset.reset_edge_state(f'position: {_prev_position_side}->{current_side}')
                _reset_hold_tracker(f'position: {_prev_position_side}->{current_side}')
            # [4-2] Initialize Chandelier tracker on new position
            try:
                import feature_flags as _ff_ch_init
                if _ff_ch_init.is_enabled('ff_unified_engine_v11') and pos and pos.get('side'):
                    from chandelier_exit import get_tracker
                    _ch_t = get_tracker()
                    if _prev_position_side is None and current_side:
                        # New position opened
                        _ch_t.on_entry(current_side, pos.get('entry_price', 0))
                    elif current_side is None and _prev_position_side:
                        # Position closed
                        _ch_t.on_close()
            except Exception as _e_ch_init:
                _log(f'[CHANDELIER] init error (FAIL-OPEN): {_e_ch_init}')

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

            # Phase 4: Mode-based handling (DB-lock dedup)
            if event_result.mode == event_trigger.MODE_EMERGENCY:
                em_types = [t['type'] for t in event_result.triggers]
                price = snapshot.get('price', 0) if snapshot else 0
                _log(f'EMERGENCY: caller={CALLER} triggers={em_types}')

                # DB lock check for EMERGENCY (still allow, but track)
                ev_locked, ev_lock_info = event_lock.check_event_lock(
                    SYMBOL, em_types, price, conn=conn)
                if ev_locked:
                    _log(f'EMERGENCY event_lock exists but proceeding '
                         f'(emergency override, remaining={ev_lock_info.get("remaining_sec")}s)')

                em_action = _handle_emergency_v2(cur, ctx, event_result, snapshot)
                _record_claude_action(em_action)
                event_trigger.record_claude_result(em_action, em_types, pos)

                # DB: record hold result + acquire event lock
                event_lock.record_hold_result(
                    SYMBOL, em_action, em_types, caller=CALLER, conn=conn)
                event_lock.acquire_event_lock(
                    SYMBOL, em_types, price, caller=CALLER, conn=conn)
                # Log Claude call
                event_lock.log_claude_call(
                    caller=CALLER, gate_type='emergency',
                    call_type='AUTO_EMERGENCY', trigger_types=em_types,
                    action_result=em_action, allowed=True, conn=conn)

                if em_action and em_action != 'HOLD':
                    _reset_hold_tracker(f'emergency action: {em_action}')
                _prev_scores = ctx.get('scores', {})
                if snapshot and snapshot.get('atr_14') is not None:
                    _prev_scores['atr_14'] = snapshot.get('atr_14')
                return LOOP_FAST_SEC

            elif event_result.mode == event_trigger.MODE_EVENT:
                _trigger_types = [t['type'] for t in event_result.triggers]
                price = snapshot.get('price', 0) if snapshot else 0
                stats = event_trigger.get_event_claude_stats()
                _log(f'EVENT: caller={CALLER} triggers={_trigger_types} '
                     f'claude_budget={stats["daily_count"]}/{stats["daily_cap"]}')

                # ── DB-based pre-filters (replace in-memory dedup) ──
                _suppress_reason = None
                _lock_info = {}

                # 1) DB event_lock: symbol + trigger_type + price_bucket (10 min)
                ev_locked, ev_lock_info = event_lock.check_event_lock(
                    SYMBOL, _trigger_types, price, conn=conn)
                if ev_locked:
                    _suppress_reason = 'db_event_lock'
                    _lock_info = ev_lock_info

                # 2) DB hash_lock: event_hash (30 min)
                if not _suppress_reason and event_result.event_hash:
                    h_locked, h_lock_info = event_lock.check_hash_lock(
                        event_result.event_hash, conn=conn)
                    if h_locked:
                        _suppress_reason = 'db_hash_lock'
                        _lock_info = h_lock_info

                # 3) DB hold_suppress: consecutive HOLD (15 min)
                if not _suppress_reason:
                    hs_locked, hs_lock_info = event_lock.check_hold_suppress(
                        SYMBOL, conn=conn)
                    if hs_locked:
                        _suppress_reason = 'db_hold_suppress'
                        _lock_info = hs_lock_info

                # 4) Legacy in-memory checks (kept as secondary layer)
                if not _suppress_reason:
                    if event_trigger.check_event_hash_dedup(event_result.event_hash):
                        _suppress_reason = 'local_dedupe'
                    elif event_trigger.is_hold_repeat(_trigger_types, pos):
                        _suppress_reason = 'local_hold_repeat'
                    elif _should_skip_claude_call(pos):
                        _suppress_reason = 'local_consecutive_hold'

                if _suppress_reason:
                    remaining = _lock_info.get('remaining_sec', 0)
                    _log(f'EVENT suppressed: reason={_suppress_reason} '
                         f'triggers={_trigger_types} remaining={remaining}s')
                    # Log denied call
                    event_lock.log_claude_call(
                        caller=CALLER, gate_type='event_trigger',
                        call_type='AUTO', trigger_types=_trigger_types,
                        action_result='SUPPRESSED', allowed=False,
                        deny_reason=_suppress_reason, conn=conn)
                    # Telegram: one-time suppression notice
                    if _suppress_reason.startswith('db_') and remaining > 0:
                        event_lock.notify_event_suppressed(
                            SYMBOL, _lock_info, _trigger_types, caller=CALLER)
                    elif event_trigger.should_send_telegram_event(
                            _trigger_types + ['_suppress']):
                        _send_telegram(
                            report_formatter.format_event_suppressed(
                                _trigger_types, _suppress_reason,
                                detail={'caller': CALLER}))
                    _prev_scores = ctx.get('scores', {})
                    if snapshot and snapshot.get('atr_14') is not None:
                        _prev_scores['atr_14'] = snapshot.get('atr_14')
                    return LOOP_FAST_SEC

                # ── Acquire DB locks before proceeding ──
                # Event lock (10 min)
                event_lock.acquire_event_lock(
                    SYMBOL, _trigger_types, price, caller=CALLER, conn=conn)
                # Hash lock (30 min)
                if event_result.event_hash:
                    event_lock.acquire_hash_lock(
                        event_result.event_hash, caller=CALLER, conn=conn)
                # Legacy: record event_hash for in-memory dedup too
                event_trigger.record_event_hash(event_result.event_hash)

                # ── GPT-mini 1차 결정 (항상 실행) ──
                _log(f'EVENT → GPT-mini 1차 (caller={CALLER})')
                ev_action, mini_result = _handle_event_trigger_mini(
                    cur, ctx, event_result, snapshot)
                # Log GPT-mini call
                event_lock.log_claude_call(
                    caller=CALLER, gate_type='event_trigger_mini',
                    call_type='AUTO_MINI', trigger_types=_trigger_types,
                    action_result=ev_action, allowed=True, conn=conn)

                # ── need_claude → 비동기 스폰 ──
                need, nc_reason = event_trigger.need_claude(
                    snapshot, mini_result, ctx.get('scores', {}))
                if need:
                    _log(f'EVENT → async Claude TRIGGERED: {nc_reason}')
                    _spawn_async_claude(ctx, event_result, snapshot, nc_reason)
                else:
                    _log(f'EVENT → Claude skipped: {nc_reason}')

                _record_claude_action(ev_action)
                event_trigger.record_claude_result(ev_action, _trigger_types, pos)

                # DB: record hold result (creates suppress_lock after 2 HOLDs)
                event_lock.record_hold_result(
                    SYMBOL, ev_action, _trigger_types, caller=CALLER,
                    conn=conn)

                if ev_action and ev_action != 'HOLD':
                    _reset_hold_tracker(f'event action: {ev_action}')

                # Periodic lock cleanup (throttled to every 5 min)
                global _last_cleanup_ts
                _now = time.time()
                if _now - _last_cleanup_ts > CLEANUP_INTERVAL_SEC:
                    event_lock.cleanup_expired(conn=conn)
                    _last_cleanup_ts = _now

                _prev_scores = ctx.get('scores', {})
                if snapshot and snapshot.get('atr_14') is not None:
                    _prev_scores['atr_14'] = snapshot.get('atr_14')
                return LOOP_FAST_SEC
            else:
                # DEFAULT: score_engine only, no Claude call
                _log(f'DEFAULT mode, score_engine only')

            # P0-4: Shock guard check (1m rapid move defense)
            try:
                import shock_guard
                _shock_detected, _shock_detail = shock_guard.check_shock(cur)
                if _shock_detected:
                    _log(f'SHOCK GUARD: freeze active — {_shock_detail.get("trigger_type", "?")}')
            except Exception as _sg_err:
                _log(f'shock_guard check error (FAIL-OPEN): {_sg_err}')

            # Run decision engine (DEFAULT mode)
            (action, reason) = _decide(ctx)
            _log(f'decision: {action} - {reason}')

            # P0-4: Shock guard freeze — block ENTRY/ADD actions, allow EXIT
            try:
                import shock_guard
                frozen, remaining = shock_guard.is_entry_frozen()
                if frozen and action in ('ADD',):
                    # FIX: build reason BEFORE overwriting action
                    _orig_action = action
                    reason = f'SHOCK FREEZE: {_orig_action} blocked ({remaining:.0f}s)'
                    _log(f'SHOCK FREEZE: {_orig_action} blocked ({remaining:.0f}s remaining)')
                    action = 'HOLD'
            except Exception:
                pass

            # D1-1: PanicGuard graduated response fallback (WS daemon backup)
            # FIX: Only ESCALATE — never downgrade a stronger action to weaker
            _action_priority = {'HOLD': 0, 'ADD': 0, 'TIGHTEN_STOP': 1,
                                'REDUCE': 2, 'CLOSE': 3, 'REVERSE': 4}
            try:
                import shock_guard as _sg_grad
                if pos and pos.get('side') and pos.get('qty') and float(pos.get('qty', 0)) > 0:
                    _grad_action, _grad_detail = _sg_grad.get_graduated_action(cur, pos)
                    _mapped = {'CLOSE_ALL': 'CLOSE', 'REDUCE_HALF': 'REDUCE',
                               'TIGHTEN_STOP': 'TIGHTEN_STOP'}.get(_grad_action)
                    if _mapped and _action_priority.get(_mapped, 0) > _action_priority.get(action, 0):
                        if _mapped == 'CLOSE':
                            _log(f'PANIC GUARD FALLBACK: CLOSE_ALL — {_grad_detail}')
                            action = 'CLOSE'
                            reason = f'PANIC GUARD: {_grad_detail}'
                        elif _mapped == 'REDUCE':
                            _log(f'PANIC GUARD FALLBACK: REDUCE_HALF — {_grad_detail}')
                            action = 'REDUCE'
                            reason = f'PANIC GUARD REDUCE_HALF: {_grad_detail}'
                    elif _grad_action == 'TIGHTEN_STOP':
                        _log(f'PANIC GUARD FALLBACK: TIGHTEN_STOP — {_grad_detail}')
                        ctx['panic_tighten_stop'] = True
            except Exception as _pg_err:
                _log(f'panic guard fallback error (FAIL-OPEN): {_pg_err}')

            # P0-2: Sync server-side stop order (after _decide, before action execution)
            try:
                import server_stop_manager
                ssm = server_stop_manager.get_manager()
                _scores = ctx.get('scores', {})
                if pos and pos.get('side') and pos.get('entry_price'):
                    _sl_base = _scores.get('dynamic_stop_loss_pct', 2.0)
                    # TIGHTEN_STOP: SL tightened (50% reduction)
                    if ctx.get('panic_tighten_stop'):
                        _sl_base = _sl_base * 0.5
                        _log(f'PANIC TIGHTEN: SL pct {_sl_base * 2:.2f}% -> {_sl_base:.2f}%')
                    _entry = pos.get('entry_price', 0)
                    if _entry > 0 and _sl_base > 0:
                        if pos['side'] == 'long':
                            _sl_price = _entry * (1 - _sl_base / 100)
                        else:
                            _sl_price = _entry * (1 + _sl_base / 100)
                        ssm.sync_stop_order(cur, pos, _sl_price)
                elif action in ('CLOSE', 'REVERSE'):
                    ssm.cancel_all_stops(cur)
            except Exception as _e:
                _log(f'server_stop sync error (FAIL-OPEN): {_e}')

            # FIX: check_unset_alert in separate try block (runs even if sync fails)
            try:
                import server_stop_manager as _ssm_alert
                _ssm_alert.get_manager().check_unset_alert()
            except Exception:
                pass

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
                lev_ctx = _build_leverage_context(ctx)
                _enqueue_action(
                    cur, 'ADD', direction,
                    target_usdt=add_usdt,
                    reason=reason,
                    pm_decision_id=dec_id,
                    priority=5,
                    meta={'leverage_context': lev_ctx})
            elif action == 'REDUCE':
                _enqueue_action(
                    cur, 'REDUCE', pos.get('side', '').upper(),
                    reduce_pct=50,
                    reason=reason,
                    pm_decision_id=dec_id,
                    priority=3)
            elif action == 'CLOSE':
                eq_id = _enqueue_action(
                    cur, 'CLOSE', pos.get('side', '').upper(),
                    target_qty=pos.get('qty'),
                    reason=reason,
                    pm_decision_id=dec_id,
                    priority=2)
                # P0-2: Cancel server stops on Python SL trigger
                try:
                    import server_stop_manager
                    server_stop_manager.get_manager().cancel_all_stops(cur)
                except Exception:
                    pass
                # TP CLOSE 시 peak 리셋 → trailing TP 재트리거 방지
                if eq_id and reason and 'TP' in reason:
                    try:
                        cur.execute("""
                            UPDATE position_state SET peak_upnl_pct = 0
                            WHERE symbol = %s;
                        """, (SYMBOL,))
                    except Exception as e:
                        _log(f'peak_upnl_pct reset error: {e}')
            elif action == 'REVERSE':
                _enqueue_reverse(
                    cur, pos,
                    reason=reason,
                    pm_decision_id=dec_id,
                    priority=2)

            # Sync position state (with peak uPnL tracking)
            _upnl_pct = None
            if pos and pos.get('side') and pos.get('entry_price') and ctx.get('price'):
                _entry = pos.get('entry_price', 0)
                _price = ctx.get('price', 0)
                if _entry > 0:
                    if pos['side'] == 'long':
                        _upnl_pct = (_price - _entry) / _entry * 100
                    else:
                        _upnl_pct = (_entry - _price) / _entry * 100
            _sync_position_state(cur, pos, upnl_pct=_upnl_pct)

            # RECONCILE auto-recovery (every 5th cycle ≈ 50-75s)
            global _reconcile_cycle_count
            _reconcile_cycle_count += 1
            if _reconcile_cycle_count % 5 == 0:
                try:
                    import exchange_reader
                    exch_data = exchange_reader.fetch_position()
                    strat_data = exchange_reader.fetch_position_strat()
                    recovered, rec_action, rec_detail = exchange_reader.check_and_recover_mismatch(
                        cur, exch_data, strat_data, ttl_minutes=10)
                    if recovered:
                        _log(f'RECONCILE recovery: {rec_action} — {rec_detail}')
                        _send_telegram_throttled(
                            f'⚠ MISMATCH 자동복구: {rec_action}\n{rec_detail}',
                            msg_type='warn')
                except Exception as e:
                    _log(f'RECONCILE check error: {e}')

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
    _log(f'=== POSITION MANAGER START ===')
    from watchdog_helper import init_watchdog
    init_watchdog(interval_sec=10)
    _log(f'BUILD_SHA={BUILD_SHA} CONFIG_VERSION={CONFIG_VERSION} CALLER={CALLER}')
    _send_telegram(report_formatter.format_service_start(
        BUILD_SHA, CONFIG_VERSION, {
            'DB 락 중복제거': '활성화',
            'GPT-mini 1차 결정': '활성화',
            '비동기 Claude': '활성화',
        }))
    import db_migrations
    db_migrations.run_all()
    # Cleanup expired locks on startup
    event_lock.cleanup_expired()
    global _last_cleanup_ts
    _last_cleanup_ts = time.time()
    while True:
        try:
            sleep_sec = _cycle()
            time.sleep(sleep_sec)
        except Exception:
            traceback.print_exc()
            time.sleep(LOOP_SLOW_SEC)


if __name__ == '__main__':
    main()
