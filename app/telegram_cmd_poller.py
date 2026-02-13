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
    "  /help   도움말\n"
    "  /status 봇 상태\n"
    "  /health 서비스 상태\n"
    "\n"
    "💬 자연어 예시\n"
    "  상태 보여줘\n"
    "  BTC 지금 얼마야?\n"
    "  RSI랑 포지션 보여줘\n"
    "  최근 30분 뉴스\n"
    "  오늘 매매전략 잡아줘\n"
    "  급변 후 방향성 분석해줘\n"
    "  손절 원인 분석해줘\n"
    "  최근 에러 뭐야?\n"
    "  리포트 보여줘\n"
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


def _ai_news_advisory(text: str, high_news: list) -> str:
    """고영향 뉴스에 대한 AI 분석. Advisory only."""
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
    result, meta = _call_claude_advisory(prompt)
    _save_advisory('news_advisory',
                   {'user_text': text, 'high_news': high_news, 'indicators': ind},
                   result, meta)
    return result


# ── AI advisory (route=claude) ───────────────────────────

def _ai_advisory(intent: dict, text: str) -> str:
    """Generate AI advisory. Advisory only — never executes trades."""
    intent_type = intent.get("intent", "other")
    claude_prompt = intent.get("claude_prompt", "") or text

    # budget gate
    state = gpt_router._load_state()
    allowed, is_gear2 = gpt_router._check_budget(state)
    if not allowed:
        return "⚠️ AI 예산 한도 도달. 로컬 조회는 가능합니다: /status, /health, 뉴스 요약"

    if intent_type == "emergency":
        return _ai_emergency_advisory(claude_prompt)
    elif intent_type == "strategy":
        return _ai_strategy_advisory(claude_prompt)
    elif intent_type == "news":
        return _ai_news_claude_advisory(claude_prompt)
    else:
        return _ai_general_advisory(claude_prompt)


def _ai_news_claude_advisory(text: str) -> str:
    """News analysis via Claude. Fetches recent news + indicators."""
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
    result, meta = _call_claude_advisory(prompt)
    _save_advisory('news_advisory',
                   {'user_text': text, 'news': news[:800], 'indicators': ind,
                    'score': score, 'position': pos},
                   result, meta)
    return result


def _ai_emergency_advisory(text: str) -> str:
    """Emergency: gather detector data + AI analysis."""
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
    result, meta = _call_claude_advisory(prompt)
    _save_advisory('emergency_advisory',
                   {'user_text': text, 'alert_summary': alert_summary, 'context': context_str},
                   result, meta)
    return result


def _ai_strategy_advisory(text: str) -> str:
    """Strategy: gather indicators + news + score engine + AI scenario."""
    parts = []

    # indicator snapshot (includes BTC current price)
    ind = local_query_executor.execute("indicator_snapshot")
    parts.append(f"지표:\n{ind}")

    # position info
    pos = local_query_executor.execute("position_info")
    parts.append(f"포지션:\n{pos}")

    # score engine (includes NEWS_EVENT)
    score = local_query_executor.execute("score_summary")
    parts.append(f"스코어:\n{score}")

    # vol summary
    vol = local_query_executor.execute("volatility_summary")
    parts.append(f"변동성:\n{vol}")

    # vol profile (POC/VAH/VAL)
    vp = _fetch_vol_profile()
    if vp:
        parts.append(f"볼륨 프로파일:\n{vp}")

    # news
    news = local_query_executor.execute("news_summary", "최근 6시간 뉴스 5개")
    parts.append(f"뉴스:\n{news[:600]}")

    prompt = (
        f"당신은 비트코인 선물 트레이딩 분석가입니다.\n"
        f"아래 제공된 실시간 데이터만 사용하여 분석하세요.\n"
        f"지지/저항 레벨은 반드시 아래 Bollinger Band, Ichimoku, MA, Volume Profile 값에서 도출하세요.\n"
        f"절대로 일반 지식이나 과거 학습 데이터의 가격 레벨을 사용하지 마세요.\n\n"
        f"사용자 요청: {text}\n\n"
        f"=== 실시간 시장 데이터 ===\n" + "\n\n".join(parts) + "\n\n"
        "=== 분석 요청 ===\n"
        "1. 현재 추세/국면 판단 (스코어 엔진 4축 종합)\n"
        "2. 뉴스 이벤트 스코어가 시장에 미치는 영향\n"
        "3. 전략 시나리오 2~3개\n"
        "4. 핵심 지지/저항 레벨 (위 BB/Ichimoku/MA/POC에서 도출) + 대응 포인트\n"
        "※ 매매 실행 권한 없음. 분석/권고만. 800자 이내."
    )
    result, meta = _call_claude_advisory(prompt)
    _save_advisory('strategy',
                   {'user_text': text, 'indicators': ind, 'position': pos,
                    'score': score, 'volatility': vol, 'vol_profile': vp,
                    'news': news[:600]},
                   result, meta)
    return result


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


def _call_claude_advisory(prompt: str) -> tuple:
    """Claude (Anthropic) call for complex analysis. Never trades.
    Returns (text_response, metadata_dict).
    """
    start_ms = int(time.time() * 1000)
    try:
        import anthropic
        from dotenv import load_dotenv
        load_dotenv("/root/trading-bot/app/.env")
        key = os.getenv("ANTHROPIC_API_KEY", "")
        if not key:
            _log("ANTHROPIC_API_KEY missing, falling back to GPT")
            elapsed = int(time.time() * 1000) - start_ms
            gpt_text = _call_gpt_advisory(prompt, provider_override="gpt-mini(fallback)")
            return (gpt_text, {'model': 'gpt-4o-mini(fallback)',
                               'api_latency_ms': elapsed, 'fallback_used': True})
        client = anthropic.Anthropic(api_key=key)
        resp = client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=800,
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text.strip()[:3500]
        elapsed = int(time.time() * 1000) - start_ms
        return (text, {'model': 'claude-sonnet-4-5-20250929',
                       'api_latency_ms': elapsed, 'fallback_used': False})
    except Exception as e:
        _log(f"Claude error: {e}, falling back to GPT")
        elapsed = int(time.time() * 1000) - start_ms
        gpt_text = _call_gpt_advisory(prompt, provider_override="gpt-mini(fallback)")
        return (gpt_text, {'model': 'gpt-4o-mini(fallback)',
                           'api_latency_ms': elapsed, 'fallback_used': True})

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
        output = {
            'recommended_action': 'ADVISORY',
            'risk_level': None,
            'confidence': None,
            'reason_bullets': [],
            'ttl_seconds': None,
            'api_latency_ms': metadata.get('api_latency_ms'),
            'fallback_used': metadata.get('fallback_used', False),
            'response_text': response_text,
        }
        with conn.cursor() as cur:
            ca_id = save_claude_analysis.save_analysis(
                cur, kind=kind, input_packet=input_packet, output=output,
                model_used=metadata.get('model', 'unknown'))
            if ca_id:
                save_claude_analysis.create_pending_outcome(cur, ca_id, 'ADVISORY')
        conn.close()
    except Exception as e:
        _log(f"_save_advisory silent error: {e}")

# ── main command handler ─────────────────────────────────

def _footer(intent_name: str, route: str, provider: str) -> str:
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
                _log("news upgrade → claude (high impact detected)")
                return _ai_news_advisory(t, high) + _footer(intent_name, "claude", "anthropic")

        return local_query_executor.execute(qtype, original_text=t) + _footer(intent_name, "local", "local")

    # 5. Route: claude (AI advisory)
    if route == "claude":
        provider = "anthropic"
        if intent_name == "other":
            provider = "gpt-mini"
        return _ai_advisory(intent, t) + _footer(intent_name, "claude", provider)

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
