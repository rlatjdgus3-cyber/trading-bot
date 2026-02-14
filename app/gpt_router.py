#!/usr/bin/env python3
"""
GPT Intent Router for Telegram messages.
Classifies natural language into structured JSON intents.
Falls back to keyword matching if GPT fails or budget exceeded.
"""

import os
import json
import time
from datetime import datetime, timezone

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
MAX_TOKENS = 300
TEMPERATURE = 0.0

STATE_FILE = "/root/trading-bot/app/.gpt_router_state.json"
DEFAULT_COOLDOWN_SEC = 300      # 5 min (general queries)
EMERGENCY_COOLDOWN_SEC = 60     # 1 min for emergency
MARKET_COOLDOWN_SEC = 120       # 2 min for strategy/market analysis
DAILY_BUDGET_LIMIT = 200
BUDGET_GEAR2_THRESHOLD = 150    # 75% → stronger cooldown

# Market-sensitive intents: shorter cooldown, gear2 does not multiply
MARKET_COOLDOWN_INTENTS = {"strategy", "emergency"}

VALID_INTENTS = ("status", "chart", "news", "strategy", "emergency",
                 "debug", "report", "directive", "other")
VALID_ROUTES = ("local", "claude", "none")
VALID_PRIORITIES = ("low", "normal", "high")

# ── NL-first type/intent constants ────────────────────────
VALID_TYPES = ("COMMAND", "QUESTION")

COMMAND_INTENTS = (
    "close_position", "reduce_position", "open_long", "open_short",
    "reverse_position", "set_risk_mode", "add_keywords", "remove_keywords",
    "list_keywords", "toggle_trading", "run_audit")

QUESTION_INTENTS = (
    "status", "price", "indicators", "news_analysis", "strategy",
    "emergency", "score", "report", "health", "errors",
    "volatility", "db_health", "claude_audit", "general")

# Legacy mapping: new QUESTION intent → (route, local_query_type)
QUESTION_ROUTE_MAP = {
    'status': ('local', 'status_full'),
    'price': ('local', 'btc_price'),
    'indicators': ('local', 'indicator_snapshot'),
    'news_analysis': ('claude', ''),
    'strategy': ('claude', ''),
    'emergency': ('claude', ''),
    'score': ('local', 'score_summary'),
    'report': ('local', 'daily_report'),
    'health': ('local', 'health_check'),
    'errors': ('local', 'recent_errors'),
    'volatility': ('local', 'volatility_summary'),
    'db_health': ('local', 'db_health'),
    'claude_audit': ('local', 'claude_audit'),
    'general': ('none', ''),
}

SYSTEM_PROMPT = """You are a trading bot NL parser. Parse the user's Korean/English message into structured JSON.

## type classification
- COMMAND: User wants to EXECUTE an action (trade, config change, toggle)
- QUESTION: User wants INFORMATION (analysis, status, price, news)

## COMMAND intents
- close_position: 포지션 전체 청산 ("롱 청산해", "포지션 닫아")
- reduce_position: 포지션 일부 축소 ("25% 줄여", "반만 정리")
- open_long: 롱 진입 ("롱 들어가", "매수 진입")
- open_short: 숏 진입 ("숏 들어가", "매도 진입")
- reverse_position: 포지션 반전 ("롱에서 숏으로 전환")
- set_risk_mode: 리스크 모드 변경 ("보수적으로", "공격적으로")
- add_keywords: 감시 키워드 추가 ("트럼프 감시 추가")
- remove_keywords: 감시 키워드 삭제 ("트럼프 감시 해제")
- list_keywords: 감시 키워드 목록 ("키워드 뭐 있어?")
- toggle_trading: 자동매매 ON/OFF ("트레이딩 일시정지", "매매 재개")
- run_audit: 시스템 감사 ("시스템 점검해줘")

## QUESTION intents
- status: 봇/포지션 상태
- price: BTC 가격 조회
- indicators: RSI, ATR, BB, Ichimoku 등 지표
- news_analysis: 뉴스 분석/해석
- strategy: 매매 전략/시나리오
- emergency: 급변/급락/손절 분석
- score: 스코어 엔진 결과
- report: 일간/자본 리포트
- health: 서비스 상태
- errors: 에러 로그
- volatility: 변동성 요약
- db_health: DB 상태/테이블 점검
- claude_audit: Claude API 사용량/비용 감사
- general: 기타 질문

## Output JSON (ONLY valid JSON, no text)
{
  "type": "COMMAND" or "QUESTION",
  "intent": "one of the intents above",
  "symbol": "BTC",
  "percent": null or number,
  "mode": null or "conservative"/"normal"/"aggressive",
  "keywords": null or ["keyword1", "keyword2"],
  "urgency": "normal" or "high",
  "use_claude": true/false,
  "needs_confirmation": true/false,
  "test_mode": false,
  "confidence": 0.0-1.0,
  "reason": "brief reason for classification"
}

## Rules
- ALWAYS classify. Never return empty or error.
- 거래 요청은 반드시 type=COMMAND.
- "오픈클로우에게" or "Claude에게" → use_claude=true.
- 퍼센트 미지정 시 percent=null (시스템이 기본값 사용).
- close/reverse/open은 needs_confirmation=true.
- "테스트"/"시뮬" 포함 시 test_mode=true.
"""


def _load_state() -> dict:
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {"version": 1, "daily_calls": {}, "cooldowns": {}}


def _save_state(state: dict):
    state["last_updated"] = time.time()
    tmp = STATE_FILE + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(state, f, indent=2)
        os.replace(tmp, STATE_FILE)
    except Exception:
        pass


def _check_cooldown(cooldown_key: str, state: dict, gear2: bool = False,
                    intent: str = "") -> bool:
    """Returns True if still in cooldown (should block).

    Market-sensitive intents (strategy, emergency) use shorter cooldowns
    and are not affected by gear2 multiplier.
    """
    if not cooldown_key:
        return False
    cooldowns = state.get("cooldowns", {})
    last_time = cooldowns.get(cooldown_key, 0)
    now = time.time()

    # Market-sensitive: shorter cooldown, gear2 ignored
    if (intent in MARKET_COOLDOWN_INTENTS
            or cooldown_key.startswith(("strategy", "emergency"))):
        if cooldown_key.startswith("emergency") or intent == "emergency":
            cd = EMERGENCY_COOLDOWN_SEC
        else:
            cd = MARKET_COOLDOWN_SEC
    else:
        cd = DEFAULT_COOLDOWN_SEC * (2 if gear2 else 1)

    if now - last_time < cd:
        return True

    cooldowns[cooldown_key] = now
    state["cooldowns"] = cooldowns
    return False


def _check_budget(state: dict) -> tuple[bool, bool]:
    """Returns (allowed, is_gear2)."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    daily = state.get("daily_calls", {})
    count = daily.get(today, 0)

    if count >= DAILY_BUDGET_LIMIT:
        return False, True
    if count >= BUDGET_GEAR2_THRESHOLD:
        return True, True
    return True, False


def _increment_budget(state: dict):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    daily = state.get("daily_calls", {})
    state["daily_calls"] = {today: daily.get(today, 0) + 1}


def _add_legacy_fields(result: dict) -> dict:
    """Add backward-compatible route/local_query_type fields from new type/intent."""
    msg_type = result.get('type', 'QUESTION')
    intent = result.get('intent', 'general')

    if msg_type == 'COMMAND':
        # COMMAND intents route to 'local' for directive-style, 'claude' for trades
        if intent in ('close_position', 'reduce_position', 'open_long',
                       'open_short', 'reverse_position'):
            result.setdefault('route', 'claude')
        else:
            result.setdefault('route', 'local')
        result.setdefault('local_query_type', '')
        # Map old intent for backward compat
        if intent in ('set_risk_mode', 'add_keywords', 'remove_keywords',
                       'list_keywords', 'run_audit'):
            result.setdefault('_legacy_intent', 'directive')
        else:
            result.setdefault('_legacy_intent', 'strategy')
    else:
        route, lqt = QUESTION_ROUTE_MAP.get(intent, ('none', ''))
        result.setdefault('route', route)
        result.setdefault('local_query_type', lqt)
        result.setdefault('_legacy_intent', intent)

    result.setdefault('priority', 'high' if result.get('urgency') == 'high' else 'normal')
    result.setdefault('cooldown_key', intent)
    result.setdefault('claude_prompt', '')
    return result


def classify_intent(text: str) -> dict:
    """Main entry. Returns intent dict with type/intent fields.
    Falls back to keywords on any failure."""
    state = _load_state()

    allowed, is_gear2 = _check_budget(state)
    if not allowed:
        result = _keyword_fallback(text)
        result["_budget_exceeded"] = True
        return result

    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY, timeout=10)
        resp = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": text},
            ],
            max_tokens=MAX_TOKENS,
            temperature=TEMPERATURE,
        )
        raw = resp.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
        result = json.loads(raw)

        # Validate type
        result.setdefault("type", "QUESTION")
        if result["type"] not in VALID_TYPES:
            result["type"] = "QUESTION"

        # Validate intent against type
        result.setdefault("intent", "general")
        msg_type = result["type"]
        if msg_type == "COMMAND":
            if result["intent"] not in COMMAND_INTENTS:
                result["intent"] = "general"
                result["type"] = "QUESTION"
        else:
            if result["intent"] not in QUESTION_INTENTS:
                result["intent"] = "general"

        # Ensure defaults for new fields
        result.setdefault("symbol", "BTC")
        result.setdefault("percent", None)
        result.setdefault("mode", None)
        result.setdefault("keywords", None)
        result.setdefault("urgency", "normal")
        result.setdefault("use_claude", False)
        result.setdefault("needs_confirmation", False)
        result.setdefault("test_mode", False)
        result.setdefault("confidence", 0.8)
        result.setdefault("reason", "")

        # Add legacy fields for backward compatibility
        _add_legacy_fields(result)

        ck = result.get("cooldown_key", result["intent"])
        route = result.get("route", "none")
        if route == "claude" and _check_cooldown(
                ck, state, gear2=is_gear2, intent=result["intent"]):
            result["_cooldown_hit"] = True

        _increment_budget(state)
        _save_state(state)
        return result

    except Exception:
        return _keyword_fallback(text)


def _keyword_fallback(text: str) -> dict:
    """Simplified keyword matching as fallback. Never calls any API.
    Returns NL-first format (type/intent) with legacy fields."""
    t = (text or "").strip().lower()

    # ── COMMAND: trade intents ──────────────────────────────
    if any(x in t for x in ["청산", "닫아", "close"]) and any(x in t for x in [
            "포지션", "롱", "숏", "전체", "position", "long", "short", "해", "해줘"]):
        return _add_legacy_fields({"type": "COMMAND", "intent": "close_position",
                "needs_confirmation": True, "confidence": 0.7, "_fallback": True})

    if any(x in t for x in ["줄여", "축소", "reduce", "정리"]) and not any(
            x in t for x in ["분석", "뉴스"]):
        import re
        m = re.search(r'(\d+)\s*%', t)
        pct = int(m.group(1)) if m else None
        return _add_legacy_fields({"type": "COMMAND", "intent": "reduce_position",
                "percent": pct, "needs_confirmation": True, "confidence": 0.7,
                "_fallback": True})

    has_entry = any(x in t for x in ["진입", "들어가", "열어", "open", "entry"])
    if any(x in t for x in ["숏", "short", "매도"]) and has_entry:
        test = "테스트" in t or "시뮬" in t or "dry" in t
        return _add_legacy_fields({"type": "COMMAND", "intent": "open_short",
                "needs_confirmation": True, "test_mode": test, "confidence": 0.7,
                "_fallback": True})
    if any(x in t for x in ["롱", "long", "매수"]) and has_entry:
        test = "테스트" in t or "시뮬" in t or "dry" in t
        return _add_legacy_fields({"type": "COMMAND", "intent": "open_long",
                "needs_confirmation": True, "test_mode": test, "confidence": 0.7,
                "_fallback": True})

    if any(x in t for x in ["반전", "전환", "reverse", "뒤집"]):
        return _add_legacy_fields({"type": "COMMAND", "intent": "reverse_position",
                "needs_confirmation": True, "confidence": 0.7, "_fallback": True})

    if any(x in t for x in ["일시정지", "멈춰", "pause"]) and any(
            x in t for x in ["트레이딩", "매매", "자동", "trading", "봇"]):
        return _add_legacy_fields({"type": "COMMAND", "intent": "toggle_trading",
                "confidence": 0.7, "_fallback": True})
    if any(x in t for x in ["트레이딩 정지", "매매 정지", "trading stop", "자동매매 stop"]):
        return _add_legacy_fields({"type": "COMMAND", "intent": "toggle_trading",
                "confidence": 0.7, "_fallback": True})
    if any(x in t for x in ["재개", "resume"]) and any(
            x in t for x in ["트레이딩", "매매", "자동", "trading", "봇"]):
        return _add_legacy_fields({"type": "COMMAND", "intent": "toggle_trading",
                "confidence": 0.7, "_fallback": True})
    if any(x in t for x in ["매매 시작", "트레이딩 시작", "자동매매 켜", "trading on"]):
        return _add_legacy_fields({"type": "COMMAND", "intent": "toggle_trading",
                "confidence": 0.7, "_fallback": True})

    # ── COMMAND: config/directive intents ────────────────────
    if any(x in t for x in ["키워드", "워치", "감시", "keyword", "watch"]):
        if any(x in t for x in ["삭제", "해제", "제거", "remove"]):
            return _add_legacy_fields({"type": "COMMAND", "intent": "remove_keywords",
                    "confidence": 0.7, "_fallback": True})
        if any(x in t for x in ["추가", "add", "등록", "강화"]):
            return _add_legacy_fields({"type": "COMMAND", "intent": "add_keywords",
                    "confidence": 0.7, "_fallback": True})
        return _add_legacy_fields({"type": "COMMAND", "intent": "list_keywords",
                "confidence": 0.7, "_fallback": True})

    if any(x in t for x in ["리스크", "risk", "위험모드"]):
        mode = None
        if any(x in t for x in ["보수", "conservative"]):
            mode = "conservative"
        elif any(x in t for x in ["공격", "aggressive"]):
            mode = "aggressive"
        elif any(x in t for x in ["노멀", "normal", "보통"]):
            mode = "normal"
        return _add_legacy_fields({"type": "COMMAND", "intent": "set_risk_mode",
                "mode": mode, "confidence": 0.7, "_fallback": True})

    if any(x in t for x in ["감사", "audit", "오딧", "시스템점검", "점검"]):
        return _add_legacy_fields({"type": "COMMAND", "intent": "run_audit",
                "confidence": 0.7, "_fallback": True})

    # ── QUESTION intents ────────────────────────────────────
    # claude_audit must be checked BEFORE db_health/status
    if any(x in t for x in ["claude_audit", "claude 사용량", "클로드 사용량", "클로드 비용",
                             "claude 비용", "api 사용량", "ai 비용", "ai 사용"]):
        return _add_legacy_fields({"type": "QUESTION", "intent": "claude_audit",
                "confidence": 0.8, "_fallback": True})

    # db_health must be checked BEFORE status (because "디비상태" contains "상태")
    if any(x in t for x in ["db_health", "디비상태", "db상태", "데이터베이스", "테이블 점검",
                             "디비 점검", "db 점검", "db health"]):
        return _add_legacy_fields({"type": "QUESTION", "intent": "db_health",
                "confidence": 0.8, "_fallback": True})

    if any(x in t for x in ["상태", "status", "스테이터스", "잘 돌아", "돌아가"]):
        return _add_legacy_fields({"type": "QUESTION", "intent": "status",
                "confidence": 0.8, "_fallback": True})

    if any(x in t for x in ["헬스", "health", "건강", "서비스 상태"]):
        return _add_legacy_fields({"type": "QUESTION", "intent": "health",
                "confidence": 0.8, "_fallback": True})

    if ("비트코인" in t or "btc" in t) and any(x in t for x in ["얼마", "가격", "시세", "현재가"]):
        return _add_legacy_fields({"type": "QUESTION", "intent": "price",
                "confidence": 0.8, "_fallback": True})

    if any(x in t for x in ["스코어", "score", "점수", "종합점수", "뉴스점수", "뉴스스코어"]):
        return _add_legacy_fields({"type": "QUESTION", "intent": "score",
                "confidence": 0.8, "_fallback": True})

    if any(x in t for x in ["rsi", "atr", "ma", "볼린저", "이치모쿠", "지표", "indicator"]):
        return _add_legacy_fields({"type": "QUESTION", "intent": "indicators",
                "confidence": 0.8, "_fallback": True})

    if any(x in t for x in ["긴급", "급변", "급락", "급등", "손절", "stop loss",
                             "stoploss", "stop-loss"]):
        return _add_legacy_fields({"type": "QUESTION", "intent": "emergency",
                "urgency": "high", "confidence": 0.8, "_fallback": True})

    if any(x in t for x in ["전략", "strategy", "대응", "시나리오"]):
        return _add_legacy_fields({"type": "QUESTION", "intent": "strategy",
                "confidence": 0.8, "_fallback": True})

    if ("뉴스" in t or "news" in t):
        return _add_legacy_fields({"type": "QUESTION", "intent": "news_analysis",
                "confidence": 0.8, "_fallback": True})

    if any(x in t for x in ["포지션", "position", "포지"]):
        return _add_legacy_fields({"type": "QUESTION", "intent": "status",
                "confidence": 0.7, "_fallback": True})

    if any(x in t for x in ["에러", "error", "오류", "장애"]):
        return _add_legacy_fields({"type": "QUESTION", "intent": "errors",
                "confidence": 0.8, "_fallback": True})

    if any(x in t for x in ["리포트", "report", "보고", "일간"]):
        return _add_legacy_fields({"type": "QUESTION", "intent": "report",
                "confidence": 0.8, "_fallback": True})

    if any(x in t for x in ["변동", "volatil", "볼라"]):
        return _add_legacy_fields({"type": "QUESTION", "intent": "volatility",
                "confidence": 0.8, "_fallback": True})

    if any(x in t for x in ["equity", "자본", "잔고"]):
        return _add_legacy_fields({"type": "QUESTION", "intent": "report",
                "confidence": 0.7, "_fallback": True})

    if any(x in t for x in ["매매"]):
        return _add_legacy_fields({"type": "QUESTION", "intent": "strategy",
                "confidence": 0.6, "_fallback": True})

    return _add_legacy_fields({"type": "QUESTION", "intent": "general",
            "confidence": 0.3, "_fallback": True})
