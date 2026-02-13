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
MAX_TOKENS = 150
TEMPERATURE = 0.0

STATE_FILE = "/root/trading-bot/app/.gpt_router_state.json"
DEFAULT_COOLDOWN_SEC = 300      # 5 min (general queries)
EMERGENCY_COOLDOWN_SEC = 60     # 1 min for emergency
MARKET_COOLDOWN_SEC = 120       # 2 min for strategy/market analysis
DAILY_BUDGET_LIMIT = 200
BUDGET_GEAR2_THRESHOLD = 150    # 75% → stronger cooldown

# Market-sensitive intents: shorter cooldown, gear2 does not multiply
MARKET_COOLDOWN_INTENTS = {"strategy", "emergency"}

VALID_INTENTS = ("status", "chart", "news", "strategy", "emergency", "debug", "report", "other")
VALID_ROUTES = ("local", "claude", "none")
VALID_PRIORITIES = ("low", "normal", "high")

SYSTEM_PROMPT = """You are a trading bot command router. Classify the user's Korean or English message into structured JSON.

Available intents:
- status: Bot status, position info, PnL, trade switch state
- chart: Price queries, technical indicator queries (RSI, ATR, MA, BB, Ichimoku, volume)
- news: News summary, recent news, news list
- strategy: Strategy recommendations, market analysis, scenario planning
- emergency: Crash analysis, stop-loss post-mortem, extreme volatility response
- debug: Error logs, service health, system diagnostics
- report: Daily/equity reports, performance summary
- other: Unrecognized or general conversation

Routing rules:
- "local": Data fetchable from DB/API without AI. Use for: status, positions, prices, indicators, health, news list, errors, reports, volatility stats.
- "claude": Complex analysis requiring AI reasoning. Use for: strategy analysis, crash direction analysis, stop-loss post-mortem, high-impact news interpretation.
- "none": Greetings, irrelevant, or blocked requests.

local_query_type (only when route=local):
  status_full, health_check, btc_price, news_summary, equity_report, daily_report,
  recent_errors, indicator_snapshot, volatility_summary, position_info, score_summary

CRITICAL RULES:
- Trading is done by local strategy only. Telegram NEVER executes trades.
- AI only advises/analyzes.
- If user asks for multiple things (e.g. "RSI and position"), pick the most relevant local_query_type.

Respond ONLY with valid JSON (no markdown, no explanation):
{"intent":"...","route":"...","local_query_type":"...","claude_prompt":"...","cooldown_key":"...","priority":"..."}"""


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


def classify_intent(text: str) -> dict:
    """Main entry. Returns intent dict. Falls back to keywords on any failure."""
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

        result.setdefault("intent", "other")
        if result["intent"] not in VALID_INTENTS:
            result["intent"] = "other"
        result.setdefault("route", "none")
        if result["route"] not in VALID_ROUTES:
            result["route"] = "none"
        result.setdefault("priority", "normal")
        if result["priority"] not in VALID_PRIORITIES:
            result["priority"] = "normal"
        result.setdefault("local_query_type", "")
        result.setdefault("claude_prompt", "")
        result.setdefault("cooldown_key", result["intent"])

        ck = result.get("cooldown_key", result["intent"])
        if result["route"] == "claude" and _check_cooldown(
                ck, state, gear2=is_gear2, intent=result["intent"]):
            result["_cooldown_hit"] = True

        _increment_budget(state)
        _save_state(state)
        return result

    except Exception:
        return _keyword_fallback(text)


def _keyword_fallback(text: str) -> dict:
    """Simplified keyword matching as fallback. Never calls any API."""
    t = (text or "").strip().lower()

    if any(x in t for x in ["상태", "status", "스테이터스", "잘 돌아", "돌아가"]):
        return {"intent": "status", "route": "local", "local_query_type": "status_full",
                "cooldown_key": "status", "priority": "normal", "_fallback": True}

    if any(x in t for x in ["헬스", "health", "건강", "서비스 상태"]):
        return {"intent": "debug", "route": "local", "local_query_type": "health_check",
                "cooldown_key": "health", "priority": "normal", "_fallback": True}

    if ("비트코인" in t or "btc" in t) and any(x in t for x in ["얼마", "가격", "시세", "현재가"]):
        return {"intent": "chart", "route": "local", "local_query_type": "btc_price",
                "cooldown_key": "btc_price", "priority": "normal", "_fallback": True}

    if any(x in t for x in ["스코어", "score", "점수", "종합점수", "뉴스점수", "뉴스스코어"]):
        return {"intent": "chart", "route": "local", "local_query_type": "score_summary",
                "cooldown_key": "score", "priority": "normal", "_fallback": True}

    if any(x in t for x in ["rsi", "atr", "ma", "볼린저", "이치모쿠", "지표", "indicator"]):
        return {"intent": "chart", "route": "local", "local_query_type": "indicator_snapshot",
                "cooldown_key": "indicator", "priority": "normal", "_fallback": True}

    # emergency/strategy BEFORE news — "긴급뉴스" should route to claude, not local news
    if any(x in t for x in ["긴급", "급변", "급락", "급등", "손절", "stop.?loss"]):
        return {"intent": "emergency", "route": "claude", "claude_prompt": text,
                "cooldown_key": "emergency", "priority": "high", "_fallback": True}

    if any(x in t for x in ["전략", "strategy", "대응", "시나리오", "매매"]):
        return {"intent": "strategy", "route": "claude", "claude_prompt": text,
                "cooldown_key": "strategy", "priority": "normal", "_fallback": True}

    # 뉴스 + 분석 키워드 → claude로 라우팅
    if ("뉴스" in t or "news" in t) and any(x in t for x in ["분석", "해석", "영향", "중요", "analysis"]):
        return {"intent": "news", "route": "claude", "claude_prompt": text,
                "cooldown_key": "news_analysis", "priority": "normal", "_fallback": True}

    if "뉴스" in t or "news" in t:
        return {"intent": "news", "route": "local", "local_query_type": "news_summary",
                "cooldown_key": "news", "priority": "normal", "_fallback": True}

    if any(x in t for x in ["포지션", "position", "포지"]):
        return {"intent": "status", "route": "local", "local_query_type": "position_info",
                "cooldown_key": "position", "priority": "normal", "_fallback": True}

    if any(x in t for x in ["에러", "error", "오류", "장애"]):
        return {"intent": "debug", "route": "local", "local_query_type": "recent_errors",
                "cooldown_key": "errors", "priority": "normal", "_fallback": True}

    if any(x in t for x in ["리포트", "report", "보고", "일간"]):
        return {"intent": "report", "route": "local", "local_query_type": "daily_report",
                "cooldown_key": "report", "priority": "normal", "_fallback": True}

    if any(x in t for x in ["변동", "volatil", "볼라"]):
        return {"intent": "chart", "route": "local", "local_query_type": "volatility_summary",
                "cooldown_key": "volatility", "priority": "normal", "_fallback": True}

    if any(x in t for x in ["equity", "자본", "잔고"]):
        return {"intent": "report", "route": "local", "local_query_type": "equity_report",
                "cooldown_key": "equity", "priority": "normal", "_fallback": True}

    return {"intent": "other", "route": "none", "cooldown_key": "",
            "priority": "low", "_fallback": True}
