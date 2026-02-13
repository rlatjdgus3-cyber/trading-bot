#!/usr/bin/env python3
"""
Telegram Command Poller — GPT Router edition.
Receives natural language → GPT Router → local/claude/none → response.
"""
import os
import sys
import json
import time
import urllib.parse
import urllib.request

sys.path.insert(0, "/root/trading-bot/app")
import gpt_router
import local_query_executor
import emergency_detector

ENV_PATH = "/root/trading-bot/app/telegram_cmd.env"
ENV_FALLBACKS = [
    "/root/trading-bot/app/.backup_20260211/telegram_cmd.env",
    "/root/trading-bot/app/_recovered/telegram_cmd.env",
]
LOG_PREFIX = "[tg_poller]"
ERR_LOG = "/root/trading-bot/app/telegram_cmd_poller.err"

# ── telegram plumbing ─────────────────────────────────────

def _log_err(msg: str):
    try:
        with open(ERR_LOG, "a", encoding="utf-8") as f:
            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}\n")
    except Exception:
        pass

def load_env(path: str) -> dict:
    candidates = [path] + ENV_FALLBACKS
    for p in candidates:
        if os.path.isfile(p):
            env = {}
            with open(p, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip()
            if p != path:
                _log(f"WARNING: primary env missing ({path}), loaded fallback: {p}")
                _log_err(f"WARNING: loaded fallback env from {p}")
            return env
    raise FileNotFoundError(
        f"telegram_cmd.env not found in any of: {candidates}"
    )

def read_offset(path: str) -> int:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return int(data.get("lastUpdateId", 0))
    except Exception:
        return 0

def write_offset(path: str, last_update_id: int) -> None:
    data = {"version": 1, "lastUpdateId": int(last_update_id)}
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def tg_api_call(token: str, method: str, params: dict) -> dict:
    url = f"https://api.telegram.org/bot{token}/{method}"
    data = urllib.parse.urlencode(params).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=20) as resp:
        body = resp.read().decode("utf-8")
    return json.loads(body)

def send_message(token: str, chat_id: int, text: str) -> None:
    chunks = []
    s = text or ""
    while len(s) > 3800:
        chunks.append(s[:3800])
        s = s[3800:]
    chunks.append(s)
    for c in chunks:
        tg_api_call(token, "sendMessage", {
            "chat_id": str(chat_id), "text": c,
            "disable_web_page_preview": "true",
        })

def _log(msg):
    print(f"{LOG_PREFIX} {msg}", flush=True)

# ── help text ────────────────────────────────────────────

HELP_TEXT = (
    "🦅 OpenClaw 콘솔 (GPT Router)\n"
    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "📌 명령어\n"
    "  /help      도움말\n"
    "  /status    봇 상태\n"
    "  /health    서비스 상태\n"
    "  /audit     시스템 감사\n"
    "  /risk MODE 리스크 모드 (conservative/normal/aggressive)\n"
    "  /keywords  워치 키워드 목록/관리\n"
    "\n"
    "💬 자연어 예시\n"
    "  상태 보여줘\n"
    "  BTC 지금 얼마야?\n"
    "  RSI랑 포지션 보여줘\n"
    "  최근 30분 뉴스\n"
    "  오늘 매매전략 잡아줘\n"
    "  급변 후 방향성 분석해줘\n"
    "  손절 원인 분석해줘\n"
    "  키워드에 trump 추가해\n"
    "  리스크 보수적으로 바꿔\n"
    "  시스템 점검해줘\n"
)

# ── news importance check & AI news advisory ─────────────

def _check_news_importance():
    """DB에서 최근 1시간 고영향 뉴스 확인. impact_score >= 7 뉴스 반환."""
    try:
        import psycopg2
        db_cfg = dict(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            dbname=os.getenv("DB_NAME", "trading"),
            user=os.getenv("DB_USER", "bot"),
            password=os.getenv("DB_PASS", "botpass"),
            connect_timeout=10,
            options="-c statement_timeout=30000",
        )
        conn = psycopg2.connect(**db_cfg)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, title, impact_score, summary
                FROM public.news
                WHERE ts >= now() - interval '1 hour'
                  AND impact_score >= 7
                ORDER BY impact_score DESC, id DESC
                LIMIT 5
            """)
            rows = cur.fetchall()
        conn.close()
        if rows:
            return [
                {"id": r[0], "title": r[1], "impact_score": r[2], "summary": r[3]}
                for r in rows
            ]
        return None
    except Exception as e:
        _log(f"_check_news_importance error: {e}")
        return None


def _ai_news_advisory(text: str, high_news: list) -> tuple:
    """고영향 뉴스에 대한 AI 분석 (Claude OK — emergency-adjacent). Returns (text, provider)."""
    news_lines = []
    for n in high_news[:3]:
        news_lines.append(
            f"- [{n['impact_score']}/10] {n['title']}\n  {n.get('summary', '')}"
        )
    news_block = "\n".join(news_lines)

    ind = local_query_executor.execute("indicator_snapshot")

    prompt = (
        f"사용자 요청: {text}\n\n"
        f"고영향 뉴스 (최근 1시간):\n{news_block}\n\n"
        f"현재 지표:\n{ind}\n\n"
        "분석 요청:\n"
        "1. 각 뉴스의 BTC 선물 영향 방향/크기\n"
        "2. 종합 시나리오 (상승/하락/횡보)\n"
        "3. 대응 포인트\n"
        "※ 매매 실행 권한 없음. 분석/권고만. 600자 이내."
    )
    gate_ctx = {
        'intent': 'news',
        'high_news': True,
        'impact_score': max((n.get('impact_score', 0) for n in high_news), default=0),
    }
    result, meta = _call_claude_advisory(
        prompt, gate='high_news', cooldown_key='tg_news_high',
        context=gate_ctx)
    _save_advisory('news_advisory',
                   {'user_text': text, 'high_news': high_news, 'indicators': ind},
                   result, meta)
    if meta.get('fallback_used'):
        return (result, 'gpt-4o-mini')
    cost = meta.get('estimated_cost_usd', 0)
    return (result, f'anthropic (${cost:.4f})')


# ── AI advisory (route=claude) ───────────────────────────

def _ai_advisory(intent: dict, text: str) -> tuple:
    """Generate AI advisory. Returns (response_text, provider_label).
    Advisory only — never executes trades."""
    intent_type = intent.get("intent", "other")
    claude_prompt = intent.get("claude_prompt", "") or text

    # budget gate
    state = gpt_router._load_state()
    allowed, is_gear2 = gpt_router._check_budget(state)
    if not allowed:
        return ("⚠️ AI 예산 한도 도달. 로컬 조회는 가능합니다: /status, /health, 뉴스 요약",
                "budget_exceeded")

    if intent_type == "emergency":
        return _ai_emergency_advisory(claude_prompt)
    elif intent_type == "strategy":
        return _ai_strategy_advisory(claude_prompt)
    elif intent_type == "news":
        return _ai_news_claude_advisory(claude_prompt)
    else:
        return (_ai_general_advisory(claude_prompt), "gpt-4o-mini")


def _ai_news_claude_advisory(text: str) -> tuple:
    """News analysis. Claude only for high-impact news. Returns (text, provider)."""
    parts = []

    # Recent news (broader window)
    news = local_query_executor.execute("news_summary", "최근 6시간 뉴스 10개")
    parts.append(f"최근 뉴스:\n{news[:800]}")

    # High impact news if any
    high = _check_news_importance()
    if high:
        high_lines = []
        for n in high[:3]:
            high_lines.append(
                f"- [{n['impact_score']}/10] {n['title']}\n  {n.get('summary', '')}")
        parts.append(f"고영향 뉴스:\n" + "\n".join(high_lines))

    # Indicators + price
    ind = local_query_executor.execute("indicator_snapshot")
    parts.append(f"지표:\n{ind}")

    # Score
    score = local_query_executor.execute("score_summary")
    parts.append(f"스코어:\n{score}")

    # Position
    pos = local_query_executor.execute("position_info")
    parts.append(f"포지션:\n{pos}")

    prompt = (
        f"당신은 비트코인 선물 트레이딩 뉴스 분석가입니다.\n"
        f"아래 제공된 실시간 데이터만 사용하여 분석하세요.\n\n"
        f"사용자 요청: {text}\n\n"
        f"=== 실시간 데이터 ===\n" + "\n\n".join(parts) + "\n\n"
        "=== 분석 요청 ===\n"
        "1. 각 뉴스의 BTC 선물 영향 방향/크기 평가\n"
        "2. 종합 시나리오 (상승/하락/횡보)\n"
        "3. 현재 포지션 기준 대응 포인트\n"
        "※ 매매 실행 권한 없음. 분석/권고만. 800자 이내."
    )

    # Claude only for high-impact news (emergency-adjacent); GPT-mini otherwise
    if high:
        gate_ctx = {
            'intent': 'news',
            'high_news': True,
            'impact_score': max((n.get('impact_score', 0) for n in high), default=0),
        }
        result, meta = _call_claude_advisory(
            prompt, gate='high_news', cooldown_key='tg_news_claude',
            context=gate_ctx)
        if meta.get('fallback_used'):
            provider = 'gpt-4o-mini'
        else:
            cost = meta.get('estimated_cost_usd', 0)
            provider = f'anthropic (${cost:.4f})'
    else:
        _log('news: no high-impact → GPT-mini (Claude skipped)')
        start_ms = int(time.time() * 1000)
        result = _call_gpt_advisory(prompt)
        elapsed = int(time.time() * 1000) - start_ms
        meta = {'model': 'gpt-4o-mini', 'api_latency_ms': elapsed, 'fallback_used': False}
        provider = 'gpt-4o-mini'

    _save_advisory('news_advisory',
                   {'user_text': text, 'news': news[:800], 'indicators': ind,
                    'score': score, 'position': pos},
                   result, meta)
    return (result, provider)


def _ai_emergency_advisory(text: str) -> tuple:
    """Emergency: gather detector data + AI analysis. Returns (text, provider)."""
    # run emergency checks
    alert_data = emergency_detector.run_check()
    alert_summary = emergency_detector.format_alerts(alert_data) if alert_data else "현재 긴급 감지 없음"

    context_str = ""
    if alert_data and alert_data.get("context"):
        context_str = json.dumps(alert_data["context"], ensure_ascii=False, default=str)

    prompt = (
        f"사용자 요청: {text}\n\n"
        f"긴급 감지 결과:\n{alert_summary}\n\n"
        f"시장 컨텍스트:\n{context_str}\n\n"
        "분석 요청:\n"
        "1. 급변 원인 분류 (매크로/기술적/뉴스)\n"
        "2. 시나리오 3개: 회복 / 추세전환 / 추세지속\n"
        "3. 10~30분 체크포인트 3개\n"
        "4. 리스크 모드 권고\n"
        "※ 매매 실행 권한 없음. 분석/권고만."
    )
    gate_ctx = {
        'is_emergency': True,
        'trigger_type': 'telegram_emergency',
        'alert_data': alert_data,
    }
    result, meta = _call_claude_advisory(
        prompt, gate='emergency', cooldown_key='tg_emergency', context=gate_ctx)
    _save_advisory('emergency_advisory',
                   {'user_text': text, 'alert_summary': alert_summary, 'context': context_str},
                   result, meta)
    if meta.get('fallback_used'):
        return (result, 'gpt-4o-mini')
    cost = meta.get('estimated_cost_usd', 0)
    return (result, f'anthropic (${cost:.4f})')


# ── strategy pipeline helpers ─────────────────────────────

STRATEGY_SYMBOL = 'BTC/USDT:USDT'

# Claude cost gate: only for high-stakes strategy actions
CLAUDE_STRATEGY_ACTIONS = {'CLOSE', 'REVERSE', 'REDUCE'}
SL_PROXIMITY_PCT = 0.3  # Call Claude when remaining SL distance < this %


def _check_auto_trading_active(cur=None):
    """Check if auto-trading is fully active (3 gates).
    Returns (bool, reason_str).
    Accepts optional DB cursor to avoid extra connection.
    """
    # Gate 1: test_mode active
    import test_utils
    test = test_utils.load_test_mode()
    if not test_utils.is_test_active(test):
        return (False, 'test mode inactive')

    # Gate 2: LIVE_TRADING env
    if os.getenv('LIVE_TRADING') != 'YES_I_UNDERSTAND':
        return (False, 'LIVE_TRADING not set')

    # Gate 3: trade_switch DB
    try:
        if cur:
            cur.execute('SELECT enabled FROM trade_switch ORDER BY id DESC LIMIT 1;')
            row = cur.fetchone()
        else:
            import psycopg2
            conn = None
            try:
                conn = psycopg2.connect(
                    host=os.getenv('DB_HOST', 'localhost'),
                    port=int(os.getenv('DB_PORT', '5432')),
                    dbname=os.getenv('DB_NAME', 'trading'),
                    user=os.getenv('DB_USER', 'bot'),
                    password=os.getenv('DB_PASS', 'botpass'),
                    connect_timeout=10,
                    options='-c statement_timeout=10000',
                )
                conn.autocommit = True
                with conn.cursor() as c:
                    c.execute('SELECT enabled FROM trade_switch ORDER BY id DESC LIMIT 1;')
                    row = c.fetchone()
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass
        if not row or not row[0]:
            return (False, 'trade_switch disabled')
    except Exception as e:
        return (False, f'trade_switch check error: {e}')

    return (True, 'auto-trading active')


def _fetch_position_state(cur):
    """Fetch current position state from DB. Returns dict or empty dict."""
    try:
        cur.execute("""
            SELECT side, total_qty, avg_entry_price, stage, trade_budget_used_pct
            FROM position_state WHERE symbol = %s;
        """, (STRATEGY_SYMBOL,))
        row = cur.fetchone()
        if row and row[0]:
            return {
                'side': row[0],
                'total_qty': float(row[1]) if row[1] is not None else 0,
                'avg_entry_price': float(row[2]) if row[2] is not None else 0,
                'stage': int(row[3]) if row[3] is not None else 0,
                'budget_used_pct': float(row[4]) if row[4] is not None else 0,
            }
    except Exception as e:
        _log(f'_fetch_position_state error: {e}')
    return {}


def _evaluate_strategy_action(scores, pos_state):
    """Evaluate strategy action. Mirrors position_manager._decide() logic.
    Returns (action, reason, details).
    """
    side = pos_state.get('side', '')
    stage = pos_state.get('stage', 0)
    avg_entry = pos_state.get('avg_entry_price', 0)
    budget_pct = pos_state.get('budget_used_pct', 0)

    total_score = scores.get('total_score', 0)
    long_score = scores.get('long_score', 50)
    short_score = scores.get('short_score', 50)
    dominant = scores.get('dominant_side', 'LONG')
    sl_pct = scores.get('dynamic_stop_loss_pct', 2.0)
    price = scores.get('price') or 0

    details = {
        'total_score': total_score,
        'long_score': long_score,
        'short_score': short_score,
        'dominant_side': dominant,
        'stop_loss_pct': sl_pct,
        'price': price,
        'tech_score': scores.get('tech_score', 0),
        'position_score': scores.get('position_score', 0),
        'regime_score': scores.get('regime_score', 0),
        'news_event_score': scores.get('news_event_score', 0),
    }

    # No position
    if not side:
        st = scores.get('stage', 1)
        if st >= 3:
            return ('ENTRY_POSSIBLE',
                    f'{dominant} stage {st} (score={total_score})', details)
        return ('HOLD', 'no position', details)

    # Stop-loss check
    if avg_entry > 0 and price > 0:
        if side == 'long':
            sl_dist = (price - avg_entry) / avg_entry * 100
        else:
            sl_dist = (avg_entry - price) / avg_entry * 100
        details['sl_dist_pct'] = round(sl_dist, 2)
        if sl_dist <= -sl_pct:
            return ('CLOSE', f'stop_loss ({sl_dist:.2f}% vs -{sl_pct}%)', details)

    # Reversal: strong opposite signal (score >= 70)
    if side == 'long' and dominant == 'SHORT' and short_score >= 70:
        return ('REVERSE', f'strong SHORT (score={short_score})', details)
    if side == 'short' and dominant == 'LONG' and long_score >= 70:
        return ('REVERSE', f'strong LONG (score={long_score})', details)

    # ADD: trend direction match + score >= 65
    if stage < 7 and budget_pct < 70:
        direction = 'LONG' if side == 'long' else 'SHORT'
        if dominant == direction:
            relevant = long_score if direction == 'LONG' else short_score
            if relevant >= 65:
                return ('ADD', f'score {relevant} favors {direction} (stage={stage})',
                        details)

    # REDUCE: counter signal strong (counter >= 65, side <= 40)
    if side == 'long' and short_score >= 65 and long_score <= 40:
        return ('REDUCE',
                f'counter signal (long={long_score}, short={short_score})', details)
    if side == 'short' and long_score >= 65 and short_score <= 40:
        return ('REDUCE',
                f'counter signal (long={long_score}, short={short_score})', details)

    return ('HOLD', 'no action needed', details)


def _enqueue_strategy_action(cur, action, pos_state, scores, reason):
    """Insert action into execution_queue. source='strategy_intent'.
    Returns eq_id or None (safety block).
    """
    import safety_manager

    side = (pos_state.get('side', '') or '').upper()
    meta = json.dumps({
        'total_score': scores.get('total_score'),
        'long_score': scores.get('long_score'),
        'short_score': scores.get('short_score'),
    }, default=str)

    if action == 'REDUCE':
        (safe, safe_reason) = safety_manager.run_all_checks(cur, 0)
        if not safe:
            _log(f'strategy safety block: {safe_reason}')
            return None
        cur.execute("""
            INSERT INTO execution_queue
                (symbol, action_type, direction, reduce_pct,
                 source, reason, priority, expire_at, meta)
            VALUES (%s, %s, %s, %s, %s, %s, %s,
                    now() + interval '5 minutes', %s::jsonb)
            RETURNING id;
        """, (STRATEGY_SYMBOL, 'REDUCE', side, 30,
              'strategy_intent', reason, 3, meta))
        row = cur.fetchone()
        return row[0] if row else None

    elif action == 'CLOSE':
        (safe, safe_reason) = safety_manager.run_all_checks(cur, 0)
        if not safe:
            _log(f'strategy safety block: {safe_reason}')
            return None
        cur.execute("""
            INSERT INTO execution_queue
                (symbol, action_type, direction, target_qty,
                 source, reason, priority, expire_at, meta)
            VALUES (%s, %s, %s, %s, %s, %s, %s,
                    now() + interval '5 minutes', %s::jsonb)
            RETURNING id;
        """, (STRATEGY_SYMBOL, 'CLOSE', side,
              pos_state.get('total_qty'),
              'strategy_intent', reason, 2, meta))
        row = cur.fetchone()
        return row[0] if row else None

    elif action == 'ADD':
        add_usdt = safety_manager.get_add_slice_usdt(cur)
        (safe, safe_reason) = safety_manager.run_all_checks(cur, add_usdt)
        if not safe:
            _log(f'strategy safety block: {safe_reason}')
            return None
        cur.execute("""
            INSERT INTO execution_queue
                (symbol, action_type, direction, target_usdt,
                 source, reason, priority, expire_at, meta)
            VALUES (%s, %s, %s, %s, %s, %s, %s,
                    now() + interval '5 minutes', %s::jsonb)
            RETURNING id;
        """, (STRATEGY_SYMBOL, 'ADD', side, add_usdt,
              'strategy_intent', reason, 5, meta))
        row = cur.fetchone()
        return row[0] if row else None

    elif action == 'REVERSE':
        (safe, safe_reason) = safety_manager.run_all_checks(cur, 0)
        if not safe:
            _log(f'strategy safety block: {safe_reason}')
            return None
        new_side = 'SHORT' if side == 'LONG' else 'LONG'
        # Step 1: REVERSE_CLOSE
        cur.execute("""
            INSERT INTO execution_queue
                (symbol, action_type, direction, target_qty,
                 source, reason, priority, expire_at, meta)
            VALUES (%s, %s, %s, %s, %s, %s, %s,
                    now() + interval '5 minutes', %s::jsonb)
            RETURNING id;
        """, (STRATEGY_SYMBOL, 'REVERSE_CLOSE', side,
              pos_state.get('total_qty'),
              'strategy_intent', reason, 2, meta))
        close_row = cur.fetchone()
        close_id = close_row[0] if close_row else None
        if close_id:
            # Step 2: REVERSE_OPEN
            open_meta = json.dumps({
                'total_score': scores.get('total_score'),
                'depends_on': close_id,
            }, default=str)
            cur.execute("""
                INSERT INTO execution_queue
                    (symbol, action_type, direction,
                     source, reason, priority, expire_at, meta)
                VALUES (%s, %s, %s, %s, %s, %s,
                        now() + interval '5 minutes', %s::jsonb)
                RETURNING id;
            """, (STRATEGY_SYMBOL, 'REVERSE_OPEN', new_side,
                  'strategy_intent', reason, 2, open_meta))
        return close_id

    return None


def _ai_strategy_advisory(text: str) -> tuple:
    """Strategy pipeline: Score → Action → Execute → AI verify.
    Claude only for CLOSE/REVERSE/REDUCE or SL proximity. Returns (text, provider)."""
    import psycopg2
    import score_engine

    conn = None
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            dbname=os.getenv("DB_NAME", "trading"),
            user=os.getenv("DB_USER", "bot"),
            password=os.getenv("DB_PASS", "botpass"),
            connect_timeout=10,
            options="-c statement_timeout=30000",
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            # Phase 1: Score evaluation
            scores = score_engine.compute_total(cur=cur)
            pos_state = _fetch_position_state(cur)

            # Phase 2: Action decision
            (action, reason, details) = _evaluate_strategy_action(scores, pos_state)

            # Phase 3: Execute (auto-trading mode only)
            eq_id = None
            execute_status = 'NO'
            (auto_ok, auto_reason) = _check_auto_trading_active(cur=cur)
            if auto_ok and action not in ('HOLD', 'ENTRY_POSSIBLE'):
                eq_id = _enqueue_strategy_action(cur, action, pos_state, scores, reason)
                if eq_id:
                    execute_status = f'YES (eq_id={eq_id})'
                else:
                    execute_status = 'NO (safety block)'
            elif not auto_ok:
                execute_status = f'NO ({auto_reason})'
            else:
                execute_status = 'NO (action not required)'

            # Build output header
            total = scores.get('total_score', 0)
            dominant = scores.get('dominant_side', 'LONG')
            stage = scores.get('stage', 1)
            tech = scores.get('tech_score', 0)
            pos_score = scores.get('position_score', 0)
            regime = scores.get('regime_score', 0)
            news_s = scores.get('news_event_score', 0)
            price = scores.get('price') or 0

            lines = [
                '=== 전략 평가 ===',
                f'ACTION: {action}',
                f'SCORE: {total:+.1f} ({dominant} stage {stage})',
                f'  TECH: {tech:+.0f} | POS: {pos_score:+.0f} | REGIME: {regime:+.0f} | NEWS: {news_s:+.0f}',
            ]

            if pos_state:
                ps_side = pos_state.get('side', '?').upper()
                qty = pos_state.get('total_qty', 0)
                entry = pos_state.get('avg_entry_price', 0)
                lines.append(f'POSITION: {ps_side} {qty} BTC @ ${entry:,.1f}')
                sl_dist = details.get('sl_dist_pct')
                sl_pct = details.get('stop_loss_pct', 2.0)
                if sl_dist is not None:
                    lines.append(f'STOP-LOSS: {sl_dist:+.1f}% (limit -{sl_pct}%)')
            else:
                lines.append('POSITION: none')

            lines.append(f'REASON: {reason}')
            lines.append(f'EXECUTE: {execute_status}')

            header = '\n'.join(lines)

            # Phase 4: AI analysis (Claude only for high-stakes actions)
            sl_dist = details.get('sl_dist_pct')
            sl_pct = details.get('stop_loss_pct', 2.0)
            sl_remaining = (sl_pct + sl_dist) if sl_dist is not None else 999
            needs_claude = (
                action in CLAUDE_STRATEGY_ACTIONS
                or sl_remaining < SL_PROXIMITY_PCT
            )

            ai_text = ''
            ai_meta = {}
            ai_label = ''

            if needs_claude:
                try:
                    claude_prompt = (
                        f"시스템이 아래와 같이 판단했습니다. 타당성을 검증하세요.\n\n"
                        f"ACTION: {action}\n"
                        f"SCORE: {total:+.1f} ({dominant} stage {stage})\n"
                        f"  TECH={tech:+.0f} POS={pos_score:+.0f} REGIME={regime:+.0f} NEWS={news_s:+.0f}\n"
                        f"POSITION: {pos_state.get('side', 'none')} qty={pos_state.get('total_qty', 0)} "
                        f"entry=${pos_state.get('avg_entry_price', 0):,.0f}\n"
                        f"PRICE: ${price:,.1f}\n"
                        f"REASON: {reason}\n\n"
                        "검증 요청 (300자 이내):\n"
                        "1. 이 판단의 타당성 (동의/주의/반대)\n"
                        "2. 핵심 리스크 1개\n"
                        "3. 관찰 포인트 1개"
                    )
                    gate_ctx = {
                        'intent': 'strategy',
                        'candidate_action': action,
                        'sl_dist_pct': sl_dist,
                    }
                    (ai_text, ai_meta) = _call_claude_advisory(
                        claude_prompt, gate='pre_action', cooldown_key='tg_strategy',
                        context=gate_ctx)
                    if ai_meta.get('fallback_used'):
                        ai_label = 'GPT-mini (fallback)'
                    else:
                        cost = ai_meta.get('estimated_cost_usd', 0)
                        ai_label = f'Claude (${cost:.4f})'
                except Exception as e:
                    ai_text = f'(AI 분석 실패: {e})'
                    ai_meta = {'model': 'error', 'api_latency_ms': 0, 'fallback_used': True}
                    ai_label = 'error'
            else:
                # HOLD/ADD: GPT-mini for brief summary (no Claude cost)
                _log(f'strategy: action={action} → GPT-mini (Claude skipped)')
                try:
                    gpt_prompt = (
                        f"트레이딩봇 전략 판단 요약 (200자 이내 한국어):\n"
                        f"ACTION={action}, SCORE={total:+.1f} ({dominant}), "
                        f"TECH={tech:+.0f} POS={pos_score:+.0f} REGIME={regime:+.0f}\n"
                        f"이유: {reason}\n"
                        f"간결한 시장 코멘트 1줄 추가."
                    )
                    start_ms = int(time.time() * 1000)
                    ai_text = _call_gpt_advisory(gpt_prompt)
                    elapsed = int(time.time() * 1000) - start_ms
                    ai_meta = {'model': 'gpt-4o-mini', 'api_latency_ms': elapsed, 'fallback_used': False}
                    ai_label = 'GPT-mini'
                except Exception as e:
                    ai_text = f'(GPT 분석 실패: {e})'
                    ai_meta = {'model': 'error', 'api_latency_ms': 0, 'fallback_used': True}
                    ai_label = 'error'

            # Compose final output
            result = header
            if ai_text:
                result += f'\n\n--- AI 분석 ({ai_label}) ---\n{ai_text}'

            # Provider label for footer
            if 'Claude' in ai_label:
                provider = f'anthropic ({ai_label.split("(")[1]}'  # "anthropic ($X.XXXX)"
            else:
                provider = ai_meta.get('model', 'gpt-4o-mini')

            # Save advisory with real action
            scores_summary = {
                'total_score': scores.get('total_score'),
                'dominant_side': scores.get('dominant_side'),
                'stage': scores.get('stage'),
                'long_score': scores.get('long_score'),
                'short_score': scores.get('short_score'),
                'tech_score': scores.get('tech_score'),
                'position_score': scores.get('position_score'),
                'regime_score': scores.get('regime_score'),
                'news_event_score': scores.get('news_event_score'),
                'dynamic_stop_loss_pct': scores.get('dynamic_stop_loss_pct'),
                'price': scores.get('price'),
            }
            _save_advisory('strategy',
                           {'user_text': text, 'scores': scores_summary,
                            'pos_state': pos_state,
                            'action': action, 'reason': reason},
                           result,
                           {**ai_meta,
                            'recommended_action': action,
                            'confidence': scores.get('confidence'),
                            'reason_bullets': [reason],
                            'execution_queue_id': eq_id})

            return (result, provider)

    except Exception as e:
        _log(f'_ai_strategy_advisory error: {e}')
        import traceback
        traceback.print_exc()
        return (f'⚠️ 전략 평가 오류: {e}', 'error')
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _fetch_vol_profile() -> str:
    """Fetch latest volume profile (POC/VAH/VAL) from DB."""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            dbname=os.getenv("DB_NAME", "trading"),
            user=os.getenv("DB_USER", "bot"),
            password=os.getenv("DB_PASS", "botpass"),
            connect_timeout=10,
            options="-c statement_timeout=10000",
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("""
                SELECT poc, vah, val, ts FROM vol_profile
                WHERE symbol = 'BTC/USDT:USDT'
                ORDER BY ts DESC LIMIT 1;
            """)
            row = cur.fetchone()
        conn.close()
        if row:
            return (f"  POC(주요가격대): ${float(row[0]):,.1f}\n"
                    f"  VAH(상단): ${float(row[1]):,.1f}\n"
                    f"  VAL(하단): ${float(row[2]):,.1f}\n"
                    f"  기준시점: {row[3]}")
        return ""
    except Exception:
        return ""


def _ai_general_advisory(text: str) -> str:
    """General AI query."""
    prompt = (
        f"사용자 질문: {text}\n\n"
        "비트코인 선물 트레이딩봇 운영자에게 간결하게 답변해줘.\n"
        "※ 매매 실행 권한 없음. 분석/권고만. 500자 이내."
    )
    start_ms = int(time.time() * 1000)
    result = _call_gpt_advisory(prompt)
    elapsed = int(time.time() * 1000) - start_ms
    gpt_model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    _save_advisory('general_gpt',
                   {'user_text': text},
                   result,
                   {'model': gpt_model, 'api_latency_ms': elapsed, 'fallback_used': False})
    return result


def _call_claude_advisory(prompt: str, gate: str = 'telegram',
                          cooldown_key: str = '', context: dict = None) -> tuple:
    """Call Claude via gate. Falls back to GPT on denial.
    Returns (text_response, metadata_dict).
    """
    import claude_gate
    if context is None:
        context = {}

    result = claude_gate.call_claude(
        gate=gate, prompt=prompt, cooldown_key=cooldown_key,
        context=context, max_tokens=800)

    if result.get('fallback_used'):
        _log(f"Claude gate denied ({result.get('gate_reason', '?')}) — fallback to GPT")
        start_ms = int(time.time() * 1000)
        gpt_text = _call_gpt_advisory(prompt)
        elapsed = int(time.time() * 1000) - start_ms
        return (gpt_text, {
            'model': 'gpt-4o-mini(claude-fallback)',
            'api_latency_ms': elapsed,
            'fallback_used': True,
            'gate_reason': result.get('gate_reason', ''),
        })

    return (result.get('text', ''), {
        'model': result.get('model', 'claude'),
        'api_latency_ms': result.get('api_latency_ms', 0),
        'fallback_used': False,
        'input_tokens': result.get('input_tokens', 0),
        'output_tokens': result.get('output_tokens', 0),
        'estimated_cost_usd': result.get('estimated_cost_usd', 0),
        'gate_type': result.get('gate_type', gate),
    })

def _call_gpt_advisory(prompt: str, provider_override: str = "") -> str:
    """Single GPT call for advisory. Never trades."""
    try:
        from openai import OpenAI
        key = os.getenv("OPENAI_API_KEY", "")
        if not key:
            return "⚠️ OPENAI_API_KEY 미설정. 로컬 조회만 가능합니다."
        client = OpenAI(api_key=key, timeout=15)
        resp = client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
            messages=[{"role": "user", "content": prompt}],
            max_tokens=600,
            temperature=0.3,
        )
        return resp.choices[0].message.content.strip()[:3500]
    except Exception as e:
        return f"⚠️ AI 분석 실패: {e}\n로컬 조회는 정상 작동합니다."

# ── DB save helper ────────────────────────────────────────

def _save_advisory(kind, input_packet, response_text, metadata):
    """Save Claude/GPT advisory to DB. Silent on error."""
    try:
        import psycopg2
        import save_claude_analysis
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            dbname=os.getenv("DB_NAME", "trading"),
            user=os.getenv("DB_USER", "bot"),
            password=os.getenv("DB_PASS", "botpass"),
            connect_timeout=10,
            options="-c statement_timeout=30000",
        )
        conn.autocommit = True
        rec_action = metadata.get('recommended_action', 'ADVISORY')
        output = {
            'recommended_action': rec_action,
            'risk_level': None,
            'confidence': metadata.get('confidence'),
            'reason_bullets': metadata.get('reason_bullets', []),
            'ttl_seconds': None,
            'api_latency_ms': metadata.get('api_latency_ms'),
            'fallback_used': metadata.get('fallback_used', False),
            'response_text': response_text,
            'input_tokens': metadata.get('input_tokens'),
            'output_tokens': metadata.get('output_tokens'),
            'estimated_cost_usd': metadata.get('estimated_cost_usd'),
            'gate_type': metadata.get('gate_type'),
        }
        with conn.cursor() as cur:
            ca_id = save_claude_analysis.save_analysis(
                cur, kind=kind, input_packet=input_packet, output=output,
                model_used=metadata.get('model', 'unknown'))
            if ca_id:
                eq_id = metadata.get('execution_queue_id')
                save_claude_analysis.create_pending_outcome(
                    cur, ca_id, rec_action, execution_queue_id=eq_id)
        conn.close()
    except Exception as e:
        _log(f"_save_advisory silent error: {e}")

# ── directive helpers ──────────────────────────────────────

def _get_directive_conn():
    """Get a DB connection for directive execution."""
    import psycopg2
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=int(os.getenv('DB_PORT', '5432')),
        dbname=os.getenv('DB_NAME', 'trading'),
        user=os.getenv('DB_USER', 'bot'),
        password=os.getenv('DB_PASS', 'botpass'),
        connect_timeout=10,
        options='-c statement_timeout=30000',
    )
    conn.autocommit = True
    return conn


def _handle_directive_command(dtype, params):
    """Execute a directive via openclaw_engine."""
    import openclaw_engine
    conn = _get_directive_conn()
    try:
        result = openclaw_engine.execute_directive(conn, dtype, params, source='telegram')
        return result.get('message', 'Directive processed')
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _parse_kw_args(text):
    """Parse keyword arguments from command text."""
    t = (text or '').strip().lower()
    if not t:
        return {'action': 'list', 'keywords': []}
    parts = t.split()
    action = 'list'
    keywords = []
    if parts[0] in ('add', 'remove', 'set', 'list'):
        action = parts[0]
        keywords = parts[1:]
    else:
        action = 'add'
        keywords = parts
    return {'action': action, 'keywords': keywords}


def _handle_directive_intent(intent, text):
    """Handle directive intent from GPT router. Returns (text, provider)."""
    import openclaw_engine
    conn = _get_directive_conn()
    try:
        parsed = openclaw_engine.parse_directive(text)
        if parsed:
            result = openclaw_engine.execute_directive(
                conn, parsed['dtype'], parsed['params'], source='telegram')
            return (result.get('message', 'Directive processed'), 'local')
        return ('Could not parse directive. Try: /audit, /risk <mode>, /keywords', 'local')
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ── main command handler ─────────────────────────────────

def _footer(intent_name: str, route: str, provider: str) -> str:
    if provider.startswith('anthropic'):
        return f"\n─\n[{intent_name}] 🤖 Claude used | {provider}"
    return f"\n─\n[{intent_name}] route={route} provider={provider}"

def handle_command(text: str) -> str:
    t = (text or "").strip()

    # 1. Direct commands — zero GPT cost
    if t in ("/help", "help"):
        return HELP_TEXT + _footer("help", "direct", "local")
    if t in ("/health", "health"):
        return local_query_executor.execute("health_check") + _footer("health", "local", "local")
    if t in ("/status", "status"):
        return local_query_executor.execute("status_full") + _footer("status", "local", "local")

    # 1b. Directive commands
    if t == '/audit' or t == 'audit':
        return _handle_directive_command('AUDIT', {}) + _footer('audit', 'local', 'local')
    if t.startswith('/risk '):
        mode = t.split(' ', 1)[1].strip()
        return _handle_directive_command('RISK_MODE', {'mode': mode}) + _footer('risk', 'local', 'local')
    if t.startswith('/keywords'):
        args_text = t[len('/keywords'):].strip()
        return _handle_directive_command('WATCH_KEYWORDS', _parse_kw_args(args_text)) + _footer('keywords', 'local', 'local')

    # 2. GPT Router — classify intent
    try:
        intent = gpt_router.classify_intent(t)
    except Exception:
        intent = gpt_router._keyword_fallback(t)

    route = intent.get("route", "none")
    intent_name = intent.get("intent", "other")
    _log(f"intent={intent_name} route={route} "
         f"local_qtype={intent.get('local_query_type','')} "
         f"fallback={intent.get('_fallback', False)} "
         f"budget_exceeded={intent.get('_budget_exceeded', False)}")

    # 3. Cooldown hit
    if intent.get("_cooldown_hit"):
        return "⏳ 동일 요청이 최근에 처리되었습니다. 잠시 후 다시 시도해주세요."

    # 4. Route: local (NO AI) — but news may upgrade to claude
    if route == "local":
        qtype = intent.get("local_query_type", "status_full")

        if intent.get("intent") == "news":
            high = _check_news_importance()
            if high:
                _log("news upgrade → claude gate (high impact detected)")
                news_result, news_provider = _ai_news_advisory(t, high)
                return news_result + _footer(intent_name, "claude", news_provider)

        return local_query_executor.execute(qtype, original_text=t) + _footer(intent_name, "local", "local")

    # 4b. Route: directive
    if intent_name == "directive":
        if route == "local" and intent.get("local_query_type") == "audit":
            return _handle_directive_command('AUDIT', {}) + _footer('directive', 'local', 'local')
        dir_result, dir_provider = _handle_directive_intent(intent, t)
        return dir_result + _footer('directive', 'local', dir_provider)

    # 5. Route: claude → gate-controlled (Claude only when conditions met)
    if route == "claude":
        ai_result, ai_provider = _ai_advisory(intent, t)
        return ai_result + _footer(intent_name, "claude", ai_provider)

    # 6. Route: none / other
    return (
        "무엇을 도와드릴까요?\n"
        "예시: 상태, 뉴스, 포지션, BTC 가격, 전략 분석, 에러 확인\n"
        "/help 로 전체 목록을 볼 수 있습니다."
    ) + _footer("none", "none", "local")

# ── main loop (unchanged) ────────────────────────────────

def main():
    env = load_env(ENV_PATH)
    token = env.get("TELEGRAM_BOT_TOKEN", "").strip()
    allowed_chat_id = int(env.get("TELEGRAM_ALLOWED_CHAT_ID", "0"))
    offset_file = env.get("TELEGRAM_OFFSET_FILE",
                          "/root/.openclaw/telegram/update-offset-default.json")

    if not token or allowed_chat_id == 0:
        raise SystemExit("ENV missing: TELEGRAM_BOT_TOKEN / TELEGRAM_ALLOWED_CHAT_ID")

    last_id = read_offset(offset_file)

    resp = tg_api_call(token, "getUpdates", {
        "offset": str(last_id + 1),
        "timeout": "0",
    })

    if not resp.get("ok"):
        raise SystemExit(f"getUpdates failed: {resp}")

    results = resp.get("result", [])
    if not results:
        return

    max_update_id = last_id
    for u in results:
        update_id = int(u.get("update_id", 0))
        max_update_id = max(max_update_id, update_id)

        msg = u.get("message") or u.get("edited_message") or {}
        chat = msg.get("chat") or {}
        chat_id = int(chat.get("id", 0))
        text = (msg.get("text") or "").strip()

        if chat_id != allowed_chat_id:
            continue
        if not text:
            continue

        try:
            reply = handle_command(text)
        except Exception as e:
            _log(f"handle_command error: {e}")
            _log_err(f"handle_command error: {e}")
            reply = f"⚠️ 명령 처리 중 오류: {e}"
        try:
            send_message(token, chat_id, reply)
        except Exception as e:
            _log(f"send_message error: {e}")
            _log_err(f"send_message error: {e}")

    write_offset(offset_file, max_update_id)

if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as e:
        print(f"{LOG_PREFIX} FATAL: {e}", flush=True)
        _log_err(f"FATAL: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"{LOG_PREFIX} ERROR: {e}", flush=True)
        _log_err(f"ERROR: {e}")
        sys.exit(1)
