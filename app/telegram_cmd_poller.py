#!/usr/bin/env python3
"""
Telegram Command Poller — GPT Router edition.
Receives natural language → GPT Router → local/claude/none → response.
"""
import os
import sys
import json
import re
import time
import urllib.parse
import urllib.request

sys.path.insert(0, "/root/trading-bot/app")
import gpt_router
import local_query_executor
import emergency_detector
import report_formatter
import event_lock

CALLER = 'telegram_cmd_poller'
CONFIG_VERSION = '2026.02.14-db-ctx-relevance-v2'

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
    s = report_formatter.sanitize_telegram_text(text or "")
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
    "🦅 OpenClaw 콘솔\n"
    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "💬 자연어로 무엇이든 말씀하세요!\n\n"
    "📌 거래 명령 예시\n"
    "  롱 포지션 청산해\n"
    "  롱 25% 줄여\n"
    "  숏으로 테스트 들어가\n"
    "  트레이딩 일시정지\n\n"
    "📊 조회/분석 예시\n"
    "  지금 BTC 어떤 상태야?\n"
    "  최근 뉴스 영향 분석해줘\n"
    "  RSI랑 이치모쿠 보여줘\n"
    "  오늘 매매 전략 추천\n\n"
    "⚙ 설정 명령 예시\n"
    "  리스크 보수적으로 바꿔\n"
    "  트럼프 감시 키워드 추가해\n"
    "  시스템 점검해줘\n\n"
    "🔧 백업 슬래시 명령\n"
    "  /help /status /health /db_health /claude_audit\n"
    "  /force /detail /debug\n"
)

# ── news importance check & AI news advisory ─────────────

def _check_news_importance():
    """DB에서 최근 1시간 고영향 뉴스 확인. impact_score >= 7 뉴스 반환."""
    conn = None
    try:
        from db_config import get_conn
        conn = get_conn(autocommit=True)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, title, impact_score, summary, title_ko
                FROM public.news
                WHERE ts >= now() - interval '1 hour'
                  AND impact_score >= 7
                ORDER BY impact_score DESC, id DESC
                LIMIT 5
            """)
            rows = cur.fetchall()
        if rows:
            return [
                {"id": r[0], "title": r[1], "impact_score": r[2], "summary": r[3],
                 "title_ko": r[4] if len(r) > 4 else None}
                for r in rows
            ]
        return None
    except Exception as e:
        _log(f"_check_news_importance error: {e}")
        return None
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _ai_news_advisory(text: str, high_news: list) -> tuple:
    """고영향 뉴스에 대한 AI 분석 (Claude OK — emergency-adjacent). Returns (text, provider)."""
    news_lines = []
    for n in high_news[:3]:
        display_title = n.get('title_ko') or n['title']
        news_lines.append(
            f"- [{n['impact_score']}/10] {display_title}\n  {n.get('summary', '')}"
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
        "600자 이내."
    )
    gate_ctx = {
        'intent': 'news',
        'high_news': True,
        'impact_score': max((n.get('impact_score', 0) for n in high_news), default=0),
    }
    result, meta = _call_claude_advisory(
        prompt, gate='high_news', cooldown_key='tg_news_high',
        context=gate_ctx, call_type='AUTO')
    _save_advisory('news_advisory',
                   {'user_text': text, 'high_news': high_news, 'indicators': ind},
                   result, meta)
    if meta.get('fallback_used'):
        return (result, 'gpt-4o-mini')
    cost = meta.get('estimated_cost_usd', 0)
    return (result, f'anthropic (${cost:.4f})')


# ── AI advisory (route=claude) ───────────────────────────

def _ai_advisory(intent: dict, text: str, no_fallback: bool = False,
                  force: bool = False) -> tuple:
    """Generate AI advisory. Returns (response_text, provider_label).
    force=True → USER call_type (bypass cooldown + Claude forced + no fallback).
    Advisory only — never executes trades."""
    intent_type = intent.get("intent", "other")
    claude_prompt = intent.get("claude_prompt", "") or text

    # Derive call_type from flags
    call_type = 'USER' if (force or no_fallback) else 'AUTO'

    # budget gate
    state = gpt_router._load_state()
    allowed, is_gear2 = gpt_router._check_budget(state)
    if not allowed:
        return ("⚠️ AI 예산 한도 도달. 로컬 조회는 가능합니다: /status, /health, 뉴스 요약",
                "budget_exceeded")

    if intent_type == "emergency":
        return _ai_emergency_advisory(claude_prompt, call_type=call_type)
    elif intent_type == "strategy":
        return _ai_strategy_advisory(claude_prompt, call_type=call_type)
    elif intent_type == "news":
        return _ai_news_claude_advisory(claude_prompt, call_type=call_type)
    else:
        return (_ai_general_advisory(claude_prompt), "gpt-4o-mini")


def _fetch_categorized_news():
    """DB에서 최근 6시간 enriched 뉴스를 카테고리별로 분리하여 반환.
    Returns (macro_news, crypto_news, stats) — stats는 집계 통계 dict.
    """
    macro_news = []
    crypto_news = []
    stats = {'total': 0, 'enriched': 0, 'high_impact': 0,
             'bullish': 0, 'bearish': 0, 'neutral': 0,
             'categories': {}}
    conn = None
    try:
        from db_config import get_conn
        conn = get_conn(autocommit=True)
        with conn.cursor() as cur:
            # Aggregate stats
            cur.execute("""
                SELECT count(*) AS total,
                       count(*) FILTER (WHERE impact_score > 0) AS enriched,
                       count(*) FILTER (WHERE impact_score >= 7) AS high_impact,
                       count(*) FILTER (WHERE summary ILIKE '[up]%%') AS bullish,
                       count(*) FILTER (WHERE summary ILIKE '[down]%%') AS bearish,
                       count(*) FILTER (WHERE summary ILIKE '[neutral]%%') AS neutral_cnt
                FROM news
                WHERE ts >= now() - interval '6 hours';
            """)
            sr = cur.fetchone()
            if sr:
                stats['total'] = sr[0] or 0
                stats['enriched'] = sr[1] or 0
                stats['high_impact'] = sr[2] or 0
                stats['bullish'] = sr[3] or 0
                stats['bearish'] = sr[4] or 0
                stats['neutral'] = sr[5] or 0

            # Top news per category (with id for macro_trace)
            cur.execute("""
                SELECT id, title, source, impact_score, summary,
                       to_char(ts AT TIME ZONE 'Asia/Seoul', 'MM-DD HH24:MI') as ts_kr,
                       keywords, url, title_ko
                FROM news
                WHERE ts >= now() - interval '6 hours'
                  AND impact_score > 0
                ORDER BY impact_score DESC, ts DESC
                LIMIT 50;
            """)
            rows = cur.fetchall()

        for r in rows:
            summary_raw = r[4] or ''
            cat = report_formatter._parse_news_category(summary_raw)
            direction = report_formatter._parse_news_direction(summary_raw)
            impact_path = report_formatter._parse_impact_path(summary_raw)
            # Extract Korean summary (strip tags)
    
            summary_kr = re.sub(r'^\[.*?\]\s*', '', summary_raw)
            summary_kr = re.sub(r'^\[.*?\]\s*', '', summary_kr)  # second tag
            if '|' in summary_kr:
                summary_kr = summary_kr.split('|', 1)[0].strip()

            item = {
                'id': r[0],
                'title': r[1] or '',
                'title_ko': r[8] if len(r) > 8 and r[8] else None,
                'source': r[2] or '',
                'impact_score': int(r[3]) if r[3] else 0,
                'summary': summary_raw,
                'summary_kr': summary_kr,
                'direction': direction,
                'category': cat,
                'category_kr': report_formatter.CATEGORY_KR.get(cat, cat),
                'impact_path': impact_path,
                'ts': r[5] or '',
                'keywords': list(r[6]) if r[6] else [],
            }
            # Category count
            stats['categories'][cat] = stats['categories'].get(cat, 0) + 1

            if cat in report_formatter.CRYPTO_CATEGORIES:
                crypto_news.append(item)
            elif cat in report_formatter.MACRO_CATEGORIES:
                macro_news.append(item)
            else:
                if item['source'] in ('coindesk', 'cointelegraph'):
                    crypto_news.append(item)
                else:
                    macro_news.append(item)
    except Exception as e:
        _log(f'_fetch_categorized_news error: {e}')
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    return (macro_news[:7], crypto_news[:7], stats)


def _ai_news_claude_advisory(text: str, call_type: str = 'AUTO',
                             detail: bool = False) -> tuple:
    """News→strategy integrated report. AI = 1-line summary only. Returns (text, provider)."""
    no_fallback = call_type in ('USER', 'EMERGENCY')
    conn = None
    try:
        import news_strategy_report
        import macro_trace_computer

        from db_config import get_conn
        conn = get_conn(autocommit=True)
        with conn.cursor() as cur:
            # 1. Compute pending macro_traces
            macro_trace_computer.compute_pending_traces(cur)

            # 2. Build report data
            data = news_strategy_report.build_report_data(
                cur, max_news=5 if detail else 3, detail=detail)

        # 3. AI: 1-line summary + risk only (500 tokens)
        stats = data.get('stats', {})
        bull = stats.get('bullish', 0)
        bear = stats.get('bearish', 0)
        high = stats.get('high_impact', 0)
        macro_titles = [(n.get('title_ko') or n.get('title', ''))[:60] for n in data.get('macro_news', [])[:3]]
        crypto_titles = [(n.get('title_ko') or n.get('title', ''))[:60] for n in data.get('crypto_news', [])[:3]]
        scores = data.get('scores', {})

        summary_prompt = (
            f"BTC 선물 뉴스 요약 JSON을 생성하세요.\n"
            f"뉴스: 상승 {bull}건, 하락 {bear}건, 고영향 {high}건\n"
            f"거시 뉴스: {'; '.join(macro_titles)}\n"
            f"크립토: {'; '.join(crypto_titles)}\n"
            f"총점: {scores.get('total', 0):+.1f}, regime: {scores.get('regime', 0):+.0f}\n\n"
            "JSON만 출력 (다른 텍스트 없이):\n"
            '{"one_liner": "결론 1줄 (한국어, 40자 이내)",'
            ' "risk_level": "낮음/보통/높음/심각 중 1개",'
            ' "watch_items": ["모니터링 항목 2-3개"],'
            ' "next_check": "다음 체크 시점"}'
        )

        ck = 'user_tg_news_summary' if no_fallback else 'auto_tg_news_summary'
        all_news = data.get('macro_news', []) + data.get('crypto_news', [])
        gate_ctx = {
            'intent': 'news',
            'high_news': bool(all_news),
            'impact_score': max(
                (n.get('impact_score', 0) for n in all_news), default=0),
        }
        gate = 'high_news' if all_news else 'telegram'
        ai_result, meta = _call_claude_advisory(
            summary_prompt, gate=gate, cooldown_key=ck,
            context=gate_ctx, call_type=call_type, max_tokens=500)
        meta['call_type'] = call_type

        if meta.get('fallback_used'):
            _log('news summary: Claude denied — GPT fallback BLOCKED, strategy aborted')
            ai_result = ''
            provider = 'claude(denied)'
        else:
            cost = meta.get('estimated_cost_usd', 0)
            provider = f'anthropic (${cost:.4f})'

        # 4. Parse AI JSON result into data
        ai_summary = _parse_ai_summary_json(ai_result)
        data['ai_summary'] = ai_summary

        # 5. Format with fixed template
        result = report_formatter.format_news_strategy_report(data, detail=detail)

        _save_advisory('news_advisory',
                       {'user_text': text,
                        'macro_news': [n['title'] for n in data.get('macro_news', [])[:5]],
                        'crypto_news': [n['title'] for n in data.get('crypto_news', [])[:5]],
                        'stats': stats},
                       result, meta)
        return (result, provider)

    except Exception as e:
        _log(f'_ai_news_claude_advisory error: {e}')
        import traceback
        traceback.print_exc()
        return (f'⚠️ 뉴스 분석 오류: {e}', 'error')
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _parse_ai_summary_json(text):
    """Parse AI JSON response for 1-line summary. Graceful fallback."""
    if not text:
        return {}
    try:
        # Try to extract JSON from response

        # Find JSON block
        match = re.search(r'\{[^{}]*\}', text, re.DOTALL)
        if match:
            return json.loads(match.group())
        return json.loads(text)
    except Exception:
        # Fallback: use raw text as one_liner
        clean = text.strip()[:100]
        return {'one_liner': clean} if clean else {}


def _ai_emergency_advisory(text: str, call_type: str = 'USER') -> tuple:
    """Emergency: gather detector data + AI analysis. Returns (text, provider)."""
    no_fallback = call_type in ('USER', 'EMERGENCY')
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
    )
    ck = 'user_tg_emergency' if no_fallback else 'auto_tg_emergency'
    gate_ctx = {
        'is_emergency': True,
        'trigger_type': 'telegram_emergency',
        'alert_data': alert_data,
    }
    result, meta = _call_claude_advisory(
        prompt, gate='emergency', cooldown_key=ck, context=gate_ctx,
        call_type=call_type)
    meta['call_type'] = call_type
    _save_advisory('emergency_advisory',
                   {'user_text': text, 'alert_summary': alert_summary, 'context': context_str},
                   result, meta)
    if meta.get('fallback_used'):
        return (result, 'claude(denied)' if no_fallback else 'gpt-4o-mini')
    cost = meta.get('estimated_cost_usd', 0)
    return (result, f'anthropic (${cost:.4f})')


# ── strategy pipeline helpers ─────────────────────────────

STRATEGY_SYMBOL = 'BTC/USDT:USDT'

# Claude cost gate: only for high-stakes strategy actions
CLAUDE_STRATEGY_ACTIONS = {'CLOSE', 'REVERSE', 'REDUCE'}
SL_PROXIMITY_PCT = 0.3  # Call Claude when remaining SL distance < this %

# ── 3-stage strategy analysis template ────────────────────
STRATEGY_ANALYSIS_TEMPLATE = (
    "=== 분석 구조 (반드시 아래 순서로 작성) ===\n\n"
    "1️⃣ 박스권 vs 추세 판정\n"
    "- 최근 24~72h 고점/저점 범위(%)와 현재가 레인지 내 위치\n"
    "- BB bandwidth + mid 기울기 해석\n"
    "- Kijun/Cloud 대비 현재가 위치\n"
    "- POC/VAH/VAL 대비 현재가 위치\n"
    "- 최근 돌파 시도 성공/실패 여부 추정\n\n"
    "2️⃣ REGIME 해석\n"
    "현재 시장 상태를 아래 중 하나로 명확히 분류:\n"
    "  A) 고변동 하락 추세\n"
    "  B) 고변동 박스권\n"
    "  C) 단순 노이즈\n\n"
    "3️⃣ 최종 결론 (반드시 하나 선택):\n"
    "  A) 박스권 반등\n"
    "  B) 추세 전환 진행\n"
    "  C) 아직 불명확 — 확정 트리거 가격 반드시 제시\n\n"
    "마지막 줄에 반드시: 최종 ACTION: HOLD/ADD/REDUCE/CLOSE/REVERSE"
)


def _check_auto_trading_active(cur=None):
    """Check if auto-trading is fully active (3 gates).
    Returns (bool, reason_str).
    Accepts optional DB cursor to avoid extra connection.
    """
    # Gate 1: test_mode active
    import test_utils
    test = test_utils.load_test_mode()
    if not test_utils.is_test_active(test):
        return (False, '테스트 모드 비활성')

    # Gate 2: LIVE_TRADING env
    if os.getenv('LIVE_TRADING') != 'YES_I_UNDERSTAND':
        return (False, 'LIVE_TRADING 미설정')

    # Gate 3: trade_switch DB
    try:
        if cur:
            cur.execute('SELECT enabled FROM trade_switch ORDER BY id DESC LIMIT 1;')
            row = cur.fetchone()
        else:
            from db_config import get_conn
            conn = None
            try:
                conn = get_conn(autocommit=True)
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
            return (False, '매매 스위치 비활성')
    except Exception as e:
        return (False, f'매매 스위치 확인 오류: {e}')

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


# Watch keywords for news matching
WATCH_KEYWORDS_DEFAULT = [
    'trump', 'fed', 'war', 'boj', 'sec', 'etf', 'nasdaq', 'china',
    'tariff', 'cpi', 'fomc', 'powell', 'rate', 'inflation', 'hack',
    'liquidation', 'ban', 'approval']


def _load_watch_keywords(cur):
    """Load watch keywords from openclaw_policies, fallback to default."""
    try:
        cur.execute("""
            SELECT value FROM openclaw_policies WHERE key = 'watch_keywords';
        """)
        row = cur.fetchone()
        if row and row[0]:
            import json as _json
            val = row[0] if isinstance(row[0], list) else _json.loads(row[0])
            if isinstance(val, list) and val:
                return [str(k).lower() for k in val]
    except Exception:
        pass
    return WATCH_KEYWORDS_DEFAULT


def _fetch_news_summary(cur):
    """Fetch recent news for strategy output. Returns list of dicts with relevance tag."""
    try:
        import news_event_scorer
        cur.execute("""
            SELECT title, source, impact_score, summary, ts,
                   keywords, title_ko
            FROM news
            WHERE ts >= now() - interval '6 hours'
              AND impact_score IS NOT NULL
            ORDER BY impact_score DESC, ts DESC
            LIMIT 10;
        """)
        rows = cur.fetchall()
        items = []
        for r in rows:
            title = r[0] or ''
            summary = r[3] or ''
            impact = int(r[2]) if r[2] else 0
            cat = news_event_scorer._parse_category_tag(summary)
            relevance = news_event_scorer._classify_relevance(title, cat, impact, summary)
            items.append({
                'title': title,
                'source': r[1] or '',
                'impact_score': impact,
                'summary': summary,
                'ts': str(r[4]) if r[4] else '',
                'keywords': list(r[5]) if r[5] else [],
                'title_ko': r[6] if len(r) > 6 and r[6] else None,
                'relevance': relevance,
            })
        return items
    except Exception as e:
        _log(f'_fetch_news_summary error: {e}')
        return []



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


def _enqueue_strategy_action(cur, action, pos_state, scores, reason, snapshot=None):
    """Insert action into execution_queue. source='strategy_intent'.
    Returns eq_id or None (safety block / snapshot validation fail).
    """
    import safety_manager

    if snapshot:
        import market_snapshot as _ms
        ok, reason_msg = _ms.validate_execution_ready(
            snapshot, scores.get('price', 0))
        if not ok:
            _log(f'execution validation failed: {reason_msg}')
            return None

    side = (pos_state.get('side', '') or '').upper()

    # 10-min same intent dedup: block if same action_type+direction queued recently
    action_type_map = {
        'REDUCE': 'REDUCE', 'CLOSE': 'CLOSE', 'ADD': 'ADD',
        'REVERSE': 'REVERSE_CLOSE',
    }
    eq_action = action_type_map.get(action, action)
    eq_direction = side
    cur.execute("""
        SELECT id FROM execution_queue
        WHERE symbol = %s AND action_type = %s AND direction = %s
          AND status IN ('PENDING', 'PICKED')
          AND ts >= now() - interval '10 minutes';
    """, (STRATEGY_SYMBOL, eq_action, eq_direction))
    if cur.fetchone():
        _log(f'duplicate intent {eq_action} {eq_direction} blocked (same intent within 10 min)')
        return None

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


def _fetch_strategy_context(cur):
    """Fetch enriched market context for 3-stage strategy analysis."""
    ctx = {}
    sym = STRATEGY_SYMBOL

    # Current indicators (BB, Ichimoku, RSI, ATR, MA)
    try:
        cur.execute("""
            SELECT rsi_14, atr_14, bb_up, bb_mid, bb_dn,
                   ich_tenkan, ich_kijun, ich_span_a, ich_span_b,
                   ma_50, ma_200, vol_spike
            FROM indicators
            WHERE symbol = %s AND tf = '1m'
            ORDER BY ts DESC LIMIT 1;
        """, (sym,))
        row = cur.fetchone()
        if row:
            bb_up = float(row[2] or 0)
            bb_mid = float(row[3] or 0)
            bb_dn = float(row[4] or 0)
            ctx['ind'] = {
                'rsi': round(float(row[0] or 0), 1),
                'atr': round(float(row[1] or 0), 1),
                'bb_up': bb_up, 'bb_mid': bb_mid, 'bb_dn': bb_dn,
                'bb_bw': round((bb_up - bb_dn) / bb_mid * 100, 2) if bb_mid else 0,
                'tenkan': float(row[5] or 0),
                'kijun': float(row[6] or 0),
                'span_a': float(row[7] or 0),
                'span_b': float(row[8] or 0),
                'ma50': float(row[9] or 0),
                'ma200': float(row[10] or 0),
                'vol_spike': bool(row[11]),
            }
    except Exception as e:
        _log(f'strategy_ctx indicators error: {e}')

    # BB mid slope (last 20 readings ≈ 20 min on 1m tf)
    try:
        cur.execute("""
            SELECT bb_mid FROM indicators
            WHERE symbol = %s AND tf = '1m' AND bb_mid IS NOT NULL
            ORDER BY ts DESC LIMIT 20;
        """, (sym,))
        rows = cur.fetchall()
        if len(rows) >= 5:
            newest = float(rows[0][0])
            oldest = float(rows[-1][0])
            diff = newest - oldest
            if abs(diff) < 5:
                slope = 'flat'
            elif diff > 0:
                slope = 'rising'
            else:
                slope = 'falling'
            ctx.setdefault('ind', {})['bb_mid_slope'] = slope
    except Exception:
        pass

    # 24h / 72h high-low range
    try:
        cur.execute("""
            SELECT
                MIN(l) FILTER (WHERE ts > now() - interval '24 hours'),
                MAX(h) FILTER (WHERE ts > now() - interval '24 hours'),
                MIN(l) FILTER (WHERE ts > now() - interval '72 hours'),
                MAX(h) FILTER (WHERE ts > now() - interval '72 hours')
            FROM market_ohlcv
            WHERE symbol = %s AND tf = '5m'
              AND ts > now() - interval '72 hours';
        """, (sym,))
        row = cur.fetchone()
        if row and row[0]:
            ctx['range'] = {
                'low_24h': float(row[0]), 'high_24h': float(row[1]),
                'low_72h': float(row[2]), 'high_72h': float(row[3]),
            }
    except Exception as e:
        _log(f'strategy_ctx range error: {e}')

    # Volume profile (POC/VAH/VAL)
    try:
        cur.execute("""
            SELECT poc, vah, val FROM vol_profile
            WHERE symbol = %s ORDER BY ts DESC LIMIT 1;
        """, (sym,))
        row = cur.fetchone()
        if row:
            ctx['vp'] = {
                'poc': float(row[0] or 0),
                'vah': float(row[1] or 0),
                'val': float(row[2] or 0),
            }
    except Exception as e:
        _log(f'strategy_ctx vol_profile error: {e}')

    return ctx


_exchange_cache = None

def _get_exchange():
    """Get cached ccxt Bybit exchange instance."""
    global _exchange_cache
    if _exchange_cache is not None:
        return _exchange_cache
    import ccxt
    _exchange_cache = ccxt.bybit({
        'apiKey': os.getenv('BYBIT_API_KEY'),
        'secret': os.getenv('BYBIT_SECRET'),
        'enableRateLimit': True,
        'timeout': 20000,
        'options': {'defaultType': 'swap'},
    })
    return _exchange_cache


def _refresh_market_snapshot(cur):
    """Thin wrapper around market_snapshot.build_snapshot() for backward compat.
    Returns latest price (float) or None on error.
    """
    import market_snapshot as _ms
    try:
        ex = _get_exchange()
        snap = _ms.build_snapshot(ex, cur, STRATEGY_SYMBOL)
        return snap.get('price')
    except Exception as e:
        _log(f'snapshot: refresh error: {e}')
        return None


def _format_market_data(price, ctx):
    """Format enriched market data block for Claude strategy prompt."""
    lines = [f'BTC 현재가: ${price:,.1f}']

    # Price range
    rng = ctx.get('range', {})
    if rng:
        h24, l24 = rng.get('high_24h', 0), rng.get('low_24h', 0)
        h72, l72 = rng.get('high_72h', 0), rng.get('low_72h', 0)
        r24_pct = (h24 - l24) / l24 * 100 if l24 else 0
        r72_pct = (h72 - l72) / l72 * 100 if l72 else 0
        pos_24 = (price - l24) / (h24 - l24) * 100 if (h24 - l24) > 0 else 50
        lines.append(f'\n[가격 범위]')
        lines.append(f'24h: ${l24:,.0f} ~ ${h24:,.0f} (범위 {r24_pct:.1f}%)')
        lines.append(f'72h: ${l72:,.0f} ~ ${h72:,.0f} (범위 {r72_pct:.1f}%)')
        lines.append(f'현재가 위치: 24h 레인지 {pos_24:.0f}% 지점')

    # Indicators
    ind = ctx.get('ind', {})
    if ind:
        lines.append(f'\n[Bollinger Bands]')
        lines.append(f'Upper: ${ind.get("bb_up", 0):,.0f} | Mid: ${ind.get("bb_mid", 0):,.0f} | Lower: ${ind.get("bb_dn", 0):,.0f}')
        lines.append(f'Bandwidth: {ind.get("bb_bw", 0):.2f}% | Mid 기울기: {ind.get("bb_mid_slope", "n/a")}')

        lines.append(f'\n[Ichimoku]')
        lines.append(f'Tenkan: ${ind.get("tenkan", 0):,.0f} | Kijun: ${ind.get("kijun", 0):,.0f}')
        lines.append(f'Cloud: Span A=${ind.get("span_a", 0):,.0f} Span B=${ind.get("span_b", 0):,.0f}')
        cloud_top = max(ind.get('span_a', 0), ind.get('span_b', 0))
        cloud_bot = min(ind.get('span_a', 0), ind.get('span_b', 0))
        if price > cloud_top:
            cloud_pos = '가격 > Cloud (위)'
        elif price < cloud_bot:
            cloud_pos = '가격 < Cloud (아래)'
        else:
            cloud_pos = '가격 ∈ Cloud (내부)'
        lines.append(cloud_pos)

        lines.append(f'\n[이동평균 & 기타]')
        lines.append(f'MA50: ${ind.get("ma50", 0):,.0f} | MA200: ${ind.get("ma200", 0):,.0f}')
        lines.append(f'RSI(14): {ind.get("rsi", 0)} | ATR(14): {ind.get("atr", 0)}')
        if ind.get('vol_spike'):
            lines.append('Volume Spike 감지')

    # Volume profile
    vp = ctx.get('vp', {})
    if vp:
        lines.append(f'\n[Volume Profile]')
        lines.append(f'POC: ${vp.get("poc", 0):,.0f} | VAH: ${vp.get("vah", 0):,.0f} | VAL: ${vp.get("val", 0):,.0f}')

    return '\n'.join(lines)


def _parse_claude_action(ai_text: str) -> str:
    """Claude 응답에서 '최종 ACTION: XXX' 패턴을 파싱. 없으면 빈 문자열."""
    m = re.search(r'최종\s*ACTION\s*[:\s]\s*(HOLD|ADD|REDUCE|CLOSE|REVERSE)', ai_text, re.IGNORECASE)
    if m:
        return m.group(1).upper()
    # fallback: "**최종 ACTION: REDUCE**" 마크다운 패턴
    m = re.search(r'\*\*최종\s*ACTION\s*[:\s]\s*(HOLD|ADD|REDUCE|CLOSE|REVERSE)\*\*', ai_text, re.IGNORECASE)
    if m:
        return m.group(1).upper()
    return ''


def _build_db_context_section():
    """Build DB context section for GPT-mini prompt."""
    conn = None
    try:
        import position_manager as _pm
        from db_config import get_conn
        conn = get_conn(autocommit=True)
        with conn.cursor() as cur:
            db_ctx = _pm.get_db_context_for_prompt(cur)

        lines = ['=== DB 컨텍스트 ===']
        lp = db_ctx.get('last_position', {})
        if lp.get('side') and lp['side'] != 'NONE':
            lines.append(f"포지션: {lp['side']} qty={lp.get('qty', 0)} "
                         f"entry=${lp.get('entry_price', 0):,.0f} stage={lp.get('stage', 0)}")
        else:
            lines.append('포지션: 없음')

        lt = db_ctx.get('last_trade')
        if lt:
            pnl_str = f" pnl={lt['pnl']:+.4f}" if lt.get('pnl') is not None else ''
            lines.append(f"최근 거래: {lt['action']} {lt['direction']} "
                         f"${lt.get('price', 0):,.0f}{pnl_str} ({lt.get('ts', '')})")

        if db_ctx.get('last_reason'):
            lines.append(f"최근 결정 근거: {db_ctx['last_reason']}")

        if db_ctx.get('cooldown_active'):
            lines.append('쿨다운: 활성화')

        rd = db_ctx.get('recent_decisions', [])
        if rd:
            lines.append('최근 결정 3건:')
            for d in rd[:3]:
                skip_tag = ' [SKIP]' if d.get('claude_skipped') else ''
                conf_str = f" conf={d['confidence']:.2f}" if d.get('confidence') is not None else ''
                lines.append(f"  {d['ts']}: {d['action']} ({d.get('actor', '?')}"
                             f"{conf_str}{skip_tag}) {d.get('reason', '')[:50]}")

        return '\n'.join(lines) + '\n\n'
    except Exception as e:
        _log(f'_build_db_context_section error: {e}')
        return ''
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _build_execution_prompt(scores, pos_state, strategy_ctx, snapshot, user_text,
                            engine_action, engine_reason, news_items=None):
    """Claude risk advisor prompt. Engine action is final; Claude evaluates risk/confidence. Forces JSON-only output."""
    price = scores.get('price') or (snapshot.get('price') if snapshot else 0) or 0
    market_data = _format_market_data(price, strategy_ctx)

    # DB context injection for GPT-mini
    db_ctx_section = _build_db_context_section()

    side = pos_state.get('side', 'none') or 'none'
    qty = pos_state.get('total_qty', 0)
    entry = pos_state.get('avg_entry_price', 0)
    stg = pos_state.get('stage', 0)
    budget_pct = pos_state.get('budget_used_pct', 0)
    pos_line = (f"side={side} qty={qty} entry=${entry:,.0f} "
                f"stage={stg} budget_used={budget_pct:.0f}%")

    tech = scores.get('tech_score', 0)
    pos_s = scores.get('position_score', 0)
    regime = scores.get('regime_score', 0)
    news = scores.get('news_event_score', 0)
    total = scores.get('total_score', 0)
    dominant = scores.get('dominant_side', 'LONG')

    snap_section = ''
    if snapshot:
        returns = snapshot.get('returns', {})
        snap_section = (
            f"=== 스냅샷 ===\n"
            f"BB: {snapshot.get('bb_lower', 0):.0f}/{snapshot.get('bb_mid', 0):.0f}/{snapshot.get('bb_upper', 0):.0f}\n"
            f"Cloud: {snapshot.get('cloud_position', '?')} | RSI: {snapshot.get('rsi_14', '?')}\n"
            f"ATR: {snapshot.get('atr_14', '?')} | Vol ratio: {snapshot.get('vol_ratio', '?')}\n"
            f"ret_1m={returns.get('ret_1m', '?')}% ret_5m={returns.get('ret_5m', '?')}%\n\n"
        )

    # Build news section for prompt (LOW 뉴스 제외)
    news_section = ''
    if news_items:
        prompt_news = [n for n in news_items if n.get('relevance', 'MED') != 'LOW']
        if prompt_news:
            news_lines = ['=== 최근 뉴스 ===']
            for i, n in enumerate(prompt_news[:5], 1):
                imp = n.get('impact_score', 0)
                title = (n.get('title_ko') or n.get('title', '?'))[:80]
                summary = n.get('summary', '')[:100]
                news_lines.append(f'{i}. [{imp}/10] {title}')
                if summary:
                    news_lines.append(f'   {summary}')
            news_section = '\n'.join(news_lines) + '\n\n'

    return (
        f"당신은 BTC 선물 트레이딩 리스크 파라미터 조언자입니다.\n"
        f"아래 실시간 데이터를 분석하고 리스크 평가 JSON을 출력하세요.\n\n"
        f"사용자 요청: {user_text}\n\n"
        f"{db_ctx_section}"
        f"=== 실시간 시장 데이터 ===\n{market_data}\n\n"
        f"=== 포지션 ===\n{pos_line}\n\n"
        f"{news_section}"
        f"=== 스코어 엔진(최종) ===\n"
        f"판단: {engine_action} | 이유: {engine_reason}\n"
        f"TOTAL={total:+.1f} ({dominant}) TECH={tech:+.0f} POS={pos_s:+.0f} "
        f"REGIME={regime:+.0f} NEWS={news:+.0f}\n"
        f"※ 이 판단이 최종 action입니다. 리스크/확신도/근거를 평가하세요.\n\n"
        f"{snap_section}"
        f"## JSON 출력 (이것만 출력, 텍스트 금지)\n"
        f'{{"action":"HOLD|OPEN_LONG|OPEN_SHORT|REDUCE|CLOSE|REVERSE",'
        f'"reduce_pct":0,"target_stage":1,"reason_code":"...","confidence":0.0,"ttl_seconds":60}}\n'
    )


def _enqueue_claude_action(cur, parsed, pos_state, scores, snapshot):
    """Claude JSON -> execution_queue. Returns eq_id or None."""
    import safety_manager
    import market_snapshot as _ms

    action = parsed['action']
    side = (pos_state.get('side', '') or '').upper()

    # Snapshot validation
    if snapshot:
        ok, reason = _ms.validate_execution_ready(snapshot, scores.get('price', 0))
        if not ok:
            _log(f'execution validation failed: {reason}')
            return None

    # Price context validation: Claude-mentioned price vs snapshot
    mentioned_price = parsed.get('price') or parsed.get('entry_price') or parsed.get('target_price')
    if mentioned_price and snapshot:
        price_ok, price_reason = _ms.validate_price_mention(mentioned_price, snapshot)
        if not price_ok:
            _log(f'INVALID PRICE CONTEXT – STRATEGY REJECTED: {price_reason}')
            return None

    # Duplicate check: same action+direction PENDING/PICKED within 10 min
    action_type_map = {
        'REDUCE': 'REDUCE', 'CLOSE': 'CLOSE',
        'OPEN_LONG': 'ADD', 'OPEN_SHORT': 'ADD',
        'REVERSE': 'REVERSE_CLOSE',
    }
    eq_action = action_type_map.get(action, action)
    direction = side
    if action == 'OPEN_LONG':
        direction = 'LONG'
    elif action == 'OPEN_SHORT':
        direction = 'SHORT'
    cur.execute("""
        SELECT id FROM execution_queue
        WHERE symbol = %s AND action_type = %s AND direction = %s
          AND status IN ('PENDING', 'PICKED')
          AND ts >= now() - interval '10 minutes';
    """, (STRATEGY_SYMBOL, eq_action, direction))
    if cur.fetchone():
        _log(f'duplicate intent {eq_action} {direction} blocked (same intent within 10 min)')
        return None

    meta = json.dumps({
        'total_score': scores.get('total_score'),
        'long_score': scores.get('long_score'),
        'short_score': scores.get('short_score'),
        'claude_action': action,
        'reason_code': parsed.get('reason_code', ''),
        'confidence': parsed.get('confidence', 0),
    }, default=str)

    if action == 'REDUCE':
        (safe, r) = safety_manager.run_all_checks(cur, 0)
        if not safe:
            _log(f'claude safety block: {r}')
            return None
        reduce_pct = parsed.get('reduce_pct', 30)
        if not side:
            _log('REDUCE: no position side')
            return None
        # Min qty check: reduce amount must be >= Bybit min (0.001 BTC)
        total_qty = float(pos_state.get('total_qty', 0))
        reduce_qty = total_qty * reduce_pct / 100.0
        MIN_ORDER_QTY = 0.001
        if reduce_qty < MIN_ORDER_QTY:
            if total_qty >= MIN_ORDER_QTY:
                _log(f'REDUCE {reduce_pct}% = {reduce_qty:.6f} < min {MIN_ORDER_QTY}. Upgrading to CLOSE.')
                # Position is at minimum unit — partial reduce impossible, do full CLOSE
                cur.execute("""
                    INSERT INTO execution_queue
                        (symbol, action_type, direction, target_qty,
                         source, reason, priority, expire_at, meta)
                    VALUES (%s, %s, %s, %s, %s, %s, %s,
                            now() + interval '5 minutes', %s::jsonb)
                    RETURNING id;
                """, (STRATEGY_SYMBOL, 'CLOSE', side,
                      pos_state.get('total_qty'),
                      'claude_execution', parsed.get('reason_code', 'reduce_upgraded_to_close'), 2, meta))
                row = cur.fetchone()
                return row[0] if row else None
            else:
                _log(f'REDUCE: position {total_qty} too small to reduce')
                return None
        cur.execute("""
            INSERT INTO execution_queue
                (symbol, action_type, direction, reduce_pct,
                 source, reason, priority, expire_at, meta)
            VALUES (%s, %s, %s, %s, %s, %s, %s,
                    now() + interval '5 minutes', %s::jsonb)
            RETURNING id;
        """, (STRATEGY_SYMBOL, 'REDUCE', side, reduce_pct,
              'claude_execution', parsed.get('reason_code', 'claude_reduce'), 3, meta))
        row = cur.fetchone()
        return row[0] if row else None

    elif action == 'CLOSE':
        (safe, r) = safety_manager.run_all_checks(cur, 0)
        if not safe:
            _log(f'claude safety block: {r}')
            return None
        if not side:
            _log('CLOSE: no position side')
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
              'claude_execution', parsed.get('reason_code', 'claude_close'), 2, meta))
        row = cur.fetchone()
        return row[0] if row else None

    elif action in ('OPEN_LONG', 'OPEN_SHORT'):
        direction = 'LONG' if action == 'OPEN_LONG' else 'SHORT'
        # Position conflict check
        if side and side != direction:
            _log(f'{action} conflicts with existing {side} position')
            return None
        target_stage = parsed.get('target_stage', 1)
        add_usdt = safety_manager.get_add_slice_usdt(cur)
        target_usdt = add_usdt * target_stage
        (safe, r) = safety_manager.run_all_checks(cur, target_usdt)
        if not safe:
            _log(f'claude safety block: {r}')
            return None
        cur.execute("""
            INSERT INTO execution_queue
                (symbol, action_type, direction, target_usdt,
                 source, reason, priority, expire_at, meta)
            VALUES (%s, %s, %s, %s, %s, %s, %s,
                    now() + interval '5 minutes', %s::jsonb)
            RETURNING id;
        """, (STRATEGY_SYMBOL, 'ADD', direction, target_usdt,
              'claude_execution', parsed.get('reason_code', f'claude_{action.lower()}'), 4, meta))
        row = cur.fetchone()
        return row[0] if row else None

    elif action == 'REVERSE':
        (safe, r) = safety_manager.run_all_checks(cur, 0)
        if not safe:
            _log(f'claude safety block: {r}')
            return None
        if not side:
            _log('REVERSE: no position to reverse')
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
              'claude_execution', parsed.get('reason_code', 'claude_reverse'), 2, meta))
        close_row = cur.fetchone()
        close_id = close_row[0] if close_row else None
        if close_id:
            # Step 2: REVERSE_OPEN
            open_meta = json.dumps({
                'total_score': scores.get('total_score'),
                'depends_on': close_id,
                'claude_action': action,
            }, default=str)
            cur.execute("""
                INSERT INTO execution_queue
                    (symbol, action_type, direction,
                     source, reason, priority, expire_at, meta)
                VALUES (%s, %s, %s, %s, %s, %s,
                        now() + interval '5 minutes', %s::jsonb)
                RETURNING id;
            """, (STRATEGY_SYMBOL, 'REVERSE_OPEN', new_side,
                  'claude_execution', parsed.get('reason_code', 'claude_reverse'), 2, open_meta))
        return close_id

    return None


def _send_decision_alert(action, parsed, engine_action, scores, pos_state,
                         claude_failed=False):
    """[DECISION] Final action summary via Telegram."""
    try:
        msg = report_formatter.format_decision_alert(
            action, parsed, engine_action, scores, pos_state,
            claude_failed=claude_failed)
        send_message(_load_tg_token(), _load_tg_chat_id(), msg)
    except Exception as e:
        _log(f'_send_decision_alert error: {e}')


def _send_enqueue_alert(eq_id, action, parsed, pos_state):
    """[ENQUEUE] Execution queue push alert via Telegram."""
    try:
        msg = report_formatter.format_enqueue_alert(
            eq_id, action, parsed, pos_state)
        send_message(_load_tg_token(), _load_tg_chat_id(), msg)
    except Exception as e:
        _log(f'_send_enqueue_alert error: {e}')


def _load_tg_token():
    """Load Telegram bot token from env cache."""
    cfg = {}
    env_path = '/root/trading-bot/app/telegram_cmd.env'
    try:
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if '=' in line and not line.startswith('#'):
                    k, v = line.split('=', 1)
                    cfg[k.strip()] = v.strip()
    except Exception:
        pass
    return cfg.get('TELEGRAM_BOT_TOKEN', '')


def _load_tg_chat_id():
    """Load Telegram chat ID from env cache."""
    cfg = {}
    env_path = '/root/trading-bot/app/telegram_cmd.env'
    try:
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if '=' in line and not line.startswith('#'):
                    k, v = line.split('=', 1)
                    cfg[k.strip()] = v.strip()
    except Exception:
        pass
    return int(cfg.get('TELEGRAM_ALLOWED_CHAT_ID', '0'))


def _ai_strategy_advisory(text: str, call_type: str = 'AUTO') -> tuple:
    """Engine-first strategy pipeline: Score → Engine final → Claude risk advice.
    Engine determines final action. Claude provides risk parameters only. Returns (text, provider)."""
    no_fallback = call_type in ('USER', 'EMERGENCY')
    import score_engine
    import claude_api

    conn = None
    try:
        from db_config import get_conn
        conn = get_conn(autocommit=True)
        with conn.cursor() as cur:
            # Phase 0: Real-time market snapshot
            import market_snapshot as _ms
            snapshot = None
            try:
                _ex = _get_exchange()
                snapshot = _ms.build_and_validate(_ex, cur, STRATEGY_SYMBOL)
            except _ms.SnapshotError as e:
                return (f'⚠️ 실시간 데이터 불가 — 전략 중단\n{e}', 'error')

            # Phase 1: Score + position + context + news (Claude input)
            scores = score_engine.compute_total(cur=cur)
            pos_state = _fetch_position_state(cur)
            strategy_ctx = _fetch_strategy_context(cur)
            news_items = _fetch_news_summary(cur)
            watch_kw = _load_watch_keywords(cur)

            # Score engine reference judgment (included in Claude input)
            (engine_action, engine_reason, details) = _evaluate_strategy_action(scores, pos_state)

            # Phase 2: Claude call (JSON-only prompt)
            prompt = _build_execution_prompt(
                scores, pos_state, strategy_ctx, snapshot, text,
                engine_action, engine_reason, news_items=news_items)
            gate = 'pre_action'
            ck = 'user_strategy' if call_type in ('USER', 'EMERGENCY') else 'auto_strategy'
            (ai_text, ai_meta) = _call_claude_advisory(
                prompt, gate=gate, cooldown_key=ck,
                context={'intent': 'strategy', 'candidate_action': engine_action},
                call_type=call_type, max_tokens=500)

            # Phase 3: JSON parsing + claude_failed 감지
            if ai_meta.get('fallback_used'):
                # Strategy route: Claude 거부 시 GPT fallback 차단 → ABORT
                _log('strategy: Claude denied → ABORT (GPT fallback blocked)')
                parsed = dict(claude_api.ABORT_RESPONSE)
                parsed['fallback_used'] = True
                claude_failed = True
                claude_action = 'ABORT'
                final_action = 'HOLD'  # ABORT = 실행 없음

                result = ('⚠️ 전략 분석 불가 — Claude 미사용\n'
                          f'사유: {ai_meta.get("gate_reason", "쿨다운/예산")}\n'
                          f'엔진 판단: {engine_action}\n'
                          '※ 전략 분석은 Claude 전용입니다. 잠시 후 재시도하세요.')
                _save_advisory('strategy_advisory',
                               {'user_text': text, 'abort': True,
                                'gate_reason': ai_meta.get('gate_reason', '')},
                               result, ai_meta)
                return (result, 'claude(denied)')
            else:
                parsed = claude_api._parse_response(ai_text)

            claude_action = parsed.get('action', 'HOLD')
            claude_failed = parsed.get('reason_code') == 'API_CALL_FAILED'

            # Engine이 항상 최종 action. Claude는 리스크 파라미터만 참고.
            final_action = engine_action
            if final_action == 'ENTRY_POSSIBLE':
                final_action = 'HOLD'

            if claude_failed:
                _log(f'CLAUDE_FALLBACK: claude_used=false, '
                     f'claude_error={parsed.get("reason_code", "unknown")}, '
                     f'final_action={final_action} (engine_only)')

            # [DECISION] Telegram alert
            _send_decision_alert(final_action, parsed, engine_action, scores, pos_state,
                                 claude_failed=claude_failed)

            # Phase 4: Safety guard -> enqueue (final_action 기반)
            # ENTRY_POSSIBLE은 이미 위에서 HOLD로 변환됨
            eq_id = None
            execute_status = 'NO'
            if final_action in ('HOLD', 'ABORT') or claude_failed:
                execute_status = f'HOLD (final={final_action})'
            else:
                # final_action 실행을 위해 parsed에 action 덮어쓰기
                exec_parsed = dict(parsed)
                exec_parsed['action'] = final_action
                (auto_ok, auto_reason) = _check_auto_trading_active(cur=cur)
                if not auto_ok:
                    execute_status = f'BLOCKED ({auto_reason})'
                else:
                    eq_id = _enqueue_claude_action(cur, exec_parsed, pos_state, scores, snapshot)
                    if eq_id:
                        execute_status = f'YES (eq_id={eq_id})'
                        # [ENQUEUE] Telegram alert
                        _send_enqueue_alert(eq_id, final_action, exec_parsed, pos_state)
                    else:
                        execute_status = f'BLOCKED (safety)'

            # Phase 5: Build output + DB save
            total = scores.get('total_score', 0)
            dominant = scores.get('dominant_side', 'LONG')
            stage = scores.get('stage', 1)
            tech = scores.get('tech_score', 0)
            pos_score = scores.get('position_score', 0)
            regime = scores.get('regime_score', 0)
            news_s = scores.get('news_event_score', 0)
            price = scores.get('price') or 0

            result = report_formatter.format_strategy_report(
                final_action, parsed, engine_action, engine_reason,
                scores, pos_state, details, news_items,
                watch_kw, execute_status, ai_meta,
                claude_failed=claude_failed)

            # Provider label for return tuple
            if ai_meta.get('fallback_used'):
                provider = 'claude(denied)'
            else:
                cost = ai_meta.get('estimated_cost_usd', 0)
                provider = f'anthropic (${cost:.4f})'

            # Save advisory
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
            save_meta = {
                **ai_meta,
                'recommended_action': final_action,
                'claude_action': claude_action,
                'claude_failed': claude_failed,
                'confidence': parsed.get('confidence'),
                'reason_bullets': [parsed.get('reason_code', '')],
                'execution_queue_id': eq_id,
                'engine_action': engine_action,
            }
            _save_advisory('strategy',
                           {'user_text': text, 'scores': scores_summary,
                            'pos_state': pos_state,
                            'final_action': final_action,
                            'claude_action': claude_action,
                            'engine_action': engine_action,
                            'reason': engine_reason,
                            'news_top3': [{'title': n['title'], 'impact': n['impact_score']}
                                          for n in news_items[:3]]},
                           result, save_meta)

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
    conn = None
    try:
        from db_config import get_conn
        conn = get_conn(autocommit=True)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT poc, vah, val, ts FROM vol_profile
                WHERE symbol = 'BTC/USDT:USDT'
                ORDER BY ts DESC LIMIT 1;
            """)
            row = cur.fetchone()
        if row:
            return (f"  POC(주요가격대): ${float(row[0]):,.1f}\n"
                    f"  VAH(상단): ${float(row[1]):,.1f}\n"
                    f"  VAL(하단): ${float(row[2]):,.1f}\n"
                    f"  기준시점: {row[3]}")
        return ""
    except Exception:
        return ""
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _ai_general_advisory(text: str) -> str:
    """General AI query."""
    prompt = (
        f"사용자 질문: {text}\n\n"
        "비트코인 선물 트레이딩봇 운영자에게 간결하게 답변해줘.\n"
        "500자 이내."
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
                          cooldown_key: str = '', context: dict = None,
                          call_type: str = 'AUTO',
                          max_tokens: int = 2500) -> tuple:
    """Call Claude via gate. Falls back to GPT on denial (unless USER/EMERGENCY).
    call_type: AUTO/USER/EMERGENCY controls cooldown/budget bypass and fallback.
    Returns (text_response, metadata_dict).
    """
    import claude_gate
    if context is None:
        context = {}
    no_fallback = call_type in ('USER', 'EMERGENCY')

    result = claude_gate.call_claude(
        gate=gate, prompt=prompt, cooldown_key=cooldown_key,
        context=context, max_tokens=max_tokens, call_type=call_type)

    # Log Claude call to DB for caller attribution
    try:
        event_lock.log_claude_call(
            caller=CALLER, gate_type=gate, call_type=call_type,
            model_used=result.get('model'),
            input_tokens=result.get('input_tokens', 0),
            output_tokens=result.get('output_tokens', 0),
            estimated_cost=result.get('estimated_cost_usd', 0),
            latency_ms=result.get('api_latency_ms', 0),
            allowed=not result.get('fallback_used', False),
            deny_reason=result.get('gate_reason') if result.get('fallback_used') else None)
    except Exception:
        pass

    if result.get('fallback_used'):
        if no_fallback:
            reason = result.get('gate_reason', 'unknown')
            _log(f"Claude gate denied ({reason}) — call_type={call_type}, no fallback")
            return (f'⚠️ Claude 게이트 거부 (GPT fallback 차단): {reason}', {
                'model': 'claude(denied)',
                'model_provider': 'anthropic(denied)',
                'api_latency_ms': 0,
                'fallback_used': True,
                'gate_reason': reason,
                'call_type': call_type,
            })
        # Block GPT fallback for strategy/event_trigger/emergency routes
        if gate in ('pre_action', 'event_trigger', 'emergency'):
            reason = result.get('gate_reason', 'unknown')
            _log(f"CLAUDE UNAVAILABLE – STRATEGY ABORTED ({reason})")
            return ('⚠️ Claude 미응답 — 전략 중단', {
                'model': 'claude(denied)',
                'model_provider': 'anthropic(denied)',
                'api_latency_ms': 0,
                'fallback_used': True,
                'gate_reason': reason,
                'call_type': call_type,
            })
        _log(f"Claude gate denied ({result.get('gate_reason', '?')}) — fallback to GPT")
        start_ms = int(time.time() * 1000)
        gpt_text = _call_gpt_advisory(prompt, max_tokens=max_tokens)
        elapsed = int(time.time() * 1000) - start_ms
        return (gpt_text, {
            'model': 'gpt-4o-mini(claude-fallback)',
            'model_provider': 'openai(fallback)',
            'api_latency_ms': elapsed,
            'fallback_used': True,
            'gate_reason': result.get('gate_reason', ''),
            'call_type': call_type,
        })

    return (result.get('text', ''), {
        'model': result.get('model', 'claude'),
        'model_provider': 'anthropic',
        'api_latency_ms': result.get('api_latency_ms', 0),
        'fallback_used': False,
        'input_tokens': result.get('input_tokens', 0),
        'output_tokens': result.get('output_tokens', 0),
        'estimated_cost_usd': result.get('estimated_cost_usd', 0),
        'gate_type': result.get('gate_type', gate),
        'call_type': call_type,
    })

def _call_gpt_advisory(prompt: str, provider_override: str = "",
                       max_tokens: int = 1500) -> str:
    """Single GPT call for advisory. Never trades."""
    try:
        from openai import OpenAI
        key = os.getenv("OPENAI_API_KEY", "")
        if not key:
            return "⚠️ OPENAI_API_KEY 미설정. 로컬 조회만 가능합니다."
        client = OpenAI(api_key=key, timeout=30)
        resp = client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens,
            temperature=0.3,
        )
        return resp.choices[0].message.content.strip()[:4500]
    except Exception as e:
        return f"⚠️ AI 분석 실패: {e}\n로컬 조회는 정상 작동합니다."

# ── DB save helper ────────────────────────────────────────

def _save_advisory(kind, input_packet, response_text, metadata):
    """Save Claude/GPT advisory to DB. Silent on error."""
    conn = None
    try:
        import save_claude_analysis
        from db_config import get_conn
        conn = get_conn(autocommit=True)
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
                model_used=metadata.get('model', 'unknown'),
                model_provider=metadata.get('model_provider'))
            if ca_id:
                eq_id = metadata.get('execution_queue_id')
                save_claude_analysis.create_pending_outcome(
                    cur, ca_id, rec_action, execution_queue_id=eq_id)
    except Exception as e:
        _log(f"_save_advisory silent error: {e}")
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

# ── directive helpers ──────────────────────────────────────

def _get_db_conn():
    """Get a DB connection. Unified helper for all DB operations."""
    from db_config import get_conn
    return get_conn(autocommit=True)


def _get_directive_conn():
    """Get a DB connection for directive execution. (Legacy alias)"""
    return _get_db_conn()


def _handle_directive_command(dtype, params):
    """Execute a directive via openclaw_engine."""
    import openclaw_engine
    conn = _get_db_conn()
    try:
        result = openclaw_engine.execute_directive(conn, dtype, params, source='telegram')
        return result.get('message', '지시 처리 완료')
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
            return (result.get('message', '지시 처리 완료'), 'local')
        return ('무엇을 변경하시겠어요?\n'
                '예시: "리스크 보수적으로", "trump 감시 추가", "시스템 점검"', 'local')
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ── NL-first handler functions ────────────────────────────

TRADE_INTENTS = frozenset({
    'close_position', 'reduce_position', 'open_long',
    'open_short', 'reverse_position',
})

# QUESTION intent → local_query_executor query type
NL_LOCAL_MAP = {
    'status': 'status_full',
    'price': 'btc_price',
    'indicators': 'indicator_snapshot',
    'score': 'score_summary',
    'health': 'health_check',
    'errors': 'recent_errors',
    'report': 'daily_report',
    'volatility': 'volatility_summary',
    'db_health': 'db_health',
    'claude_audit': 'claude_audit',
    'macro_summary': 'macro_summary',
    'db_monthly_stats': 'db_monthly_stats',
}


def _format_command_result(action, eq_id, parsed, pos, scores):
    """Format trade command execution result."""
    lines = [f"✅ {action} 명령 접수 (eq#{eq_id})"]
    if pos.get('side'):
        lines.append(f"포지션: {pos['side']} qty={pos.get('total_qty', 0)}")
    if action == 'REDUCE' and parsed.get('percent'):
        lines.append(f"축소: {parsed['percent']}%")
    lines.append(f"스코어: {scores.get('total_score', 0):+.1f}")
    return '\n'.join(lines)


def _execute_trade_command(parsed, text):
    """Execute trade COMMAND intent. Returns response string."""
    import score_engine

    intent = parsed.get('intent')
    test_mode = parsed.get('test_mode', False)
    percent = parsed.get('percent')
    use_claude = parsed.get('use_claude', False)

    # 1. use_claude → delegate to Claude strategy pipeline
    if use_claude:
        result, provider = _ai_strategy_advisory(text, call_type='USER_MANUAL')
        return result + _footer('strategy', 'claude', provider, call_type='USER_MANUAL')

    # 2. Safety check: auto-trading active?
    conn = _get_db_conn()
    try:
        with conn.cursor() as cur:
            (auto_ok, auto_reason) = _check_auto_trading_active(cur=cur)
            if not auto_ok and not test_mode:
                return (
                    f"⚠️ 자동매매 비활성: {auto_reason}\n"
                    f"💡 테스트 모드로 실행하려면: \"{text} 테스트\"\n"
                    f"💡 또는: /force {text}"
                ) + _footer(intent, 'blocked', 'local')

            # 3. Position + scores
            pos = _fetch_position_state(cur)
            scores = score_engine.compute_total(cur=cur)

            # 4. Map intent to action
            action_map = {
                'close_position': 'CLOSE',
                'reduce_position': 'REDUCE',
                'open_long': 'OPEN_LONG',
                'open_short': 'OPEN_SHORT',
                'reverse_position': 'REVERSE',
            }
            action = action_map[intent]

            # 5. Validation
            side = pos.get('side', '')
            if action == 'CLOSE' and not side:
                return "포지션이 없어 청산할 수 없습니다." + _footer(intent, 'local', 'local')
            if action == 'REDUCE' and not side:
                return "포지션이 없어 축소할 수 없습니다." + _footer(intent, 'local', 'local')
            if action == 'REVERSE' and not side:
                return "포지션이 없어 반전할 수 없습니다." + _footer(intent, 'local', 'local')

            # 6. Direct execution
            parsed_action = {
                'action': action,
                'reduce_pct': percent or 30,
                'reason_code': f'user_nl_{intent}',
                'confidence': parsed.get('confidence', 0.8),
            }

            # Build snapshot
            snapshot = None
            try:
                import market_snapshot as _ms
                _ex = _get_exchange()
                snapshot = _ms.build_and_validate(_ex, cur, STRATEGY_SYMBOL)
            except Exception:
                pass

            eq_id = _enqueue_claude_action(cur, parsed_action, pos, scores, snapshot)
            if eq_id:
                _send_enqueue_alert(eq_id, action, parsed_action, pos)
                return _format_command_result(action, eq_id, parsed, pos, scores) + \
                    _footer(intent, 'execute', 'local')
            else:
                return (
                    f"⚠️ {action} 안전 체크 차단\n"
                    f"💡 /force 로 강제 실행 가능"
                ) + _footer(intent, 'blocked', 'local')
    finally:
        conn.close()


def _toggle_trading(parsed, text):
    """Toggle auto-trading on/off via trade_switch DB."""
    t_lower = text.lower()
    if any(x in t_lower for x in ['정지', '멈춰', 'stop', 'pause', '끄', 'off']):
        enable = False
    elif any(x in t_lower for x in ['재개', '시작', 'start', 'resume', '켜', 'on']):
        enable = True
    else:
        enable = False  # default: pause

    conn = _get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE trade_switch SET enabled = %s "
                "WHERE id = (SELECT id FROM trade_switch ORDER BY id DESC LIMIT 1);",
                (enable,))
            if cur.rowcount == 0:
                return "⚠️ trade_switch 레코드가 없습니다." + \
                    _footer('toggle_trading', 'error', 'local')
        state_str = "재개" if enable else "일시정지"
        return f"자동매매 {state_str} 완료" + _footer('toggle_trading', 'local', 'local')
    finally:
        conn.close()


def _handle_nl_command(parsed, text):
    """Handle NL COMMAND type. Dispatches by intent."""
    intent = parsed.get('intent', '')

    # Trade commands
    if intent in TRADE_INTENTS:
        return _execute_trade_command(parsed, text)

    # Config commands
    if intent == 'set_risk_mode':
        mode = parsed.get('mode') or 'normal'
        return _handle_directive_command('RISK_MODE', {'mode': mode}) + \
            _footer('set_risk_mode', 'local', 'local')

    if intent == 'add_keywords':
        kws = parsed.get('keywords') or []
        if not kws:
            # Try to extract keywords from text
            kws = _extract_keywords_from_text(text)
        return _handle_directive_command('WATCH_KEYWORDS',
            {'action': 'add', 'keywords': kws}) + \
            _footer('add_keywords', 'local', 'local')

    if intent == 'remove_keywords':
        kws = parsed.get('keywords') or []
        if not kws:
            kws = _extract_keywords_from_text(text)
        return _handle_directive_command('WATCH_KEYWORDS',
            {'action': 'remove', 'keywords': kws}) + \
            _footer('remove_keywords', 'local', 'local')

    if intent == 'list_keywords':
        return _handle_directive_command('WATCH_KEYWORDS',
            {'action': 'list', 'keywords': []}) + \
            _footer('list_keywords', 'local', 'local')

    if intent == 'toggle_trading':
        return _toggle_trading(parsed, text)

    if intent == 'run_audit':
        return _handle_directive_command('AUDIT', {}) + \
            _footer('run_audit', 'local', 'local')

    # Fallback: treat as QUESTION
    return _handle_nl_question(parsed, text)


def _extract_keywords_from_text(text):
    """Extract potential keywords from natural language text."""
    t = text.lower()
    # Remove common verb/particle phrases (longer first to avoid partial matches)
    for w in ['추가해줘', '삭제해줘', '해제해줘', '등록해줘', '제거해줘',
              '추가해', '삭제해', '해제해', '등록해', '제거해',
              '추가', '삭제', '해제', '등록', '제거',
              '감시', '키워드', '워치', '해줘', '강화',
              '좀', '뉴스']:
        t = t.replace(w, ' ')
    parts = [p.strip() for p in re.split(r'[\s/,]+', t) if p.strip() and len(p.strip()) >= 2]
    return parts


def _handle_nl_question(parsed, text):
    """Handle NL QUESTION type. Dispatches by intent."""
    intent = parsed.get('intent', 'general')
    use_claude = parsed.get('use_claude', False)

    # 1. News → news report pipeline
    if intent == 'news_analysis':
        result, provider = _ai_news_claude_advisory(text, call_type='AUTO')
        return result + _footer('news_analysis', 'claude', provider)

    # 2. Strategy or use_claude → Claude strategy pipeline
    if intent == 'strategy' or use_claude:
        call_type = 'USER' if use_claude else 'AUTO'
        result, provider = _ai_strategy_advisory(text, call_type=call_type)
        return result + _footer('strategy', 'claude', provider, call_type=call_type)

    # 3. Emergency
    if intent == 'emergency':
        result, provider = _ai_emergency_advisory(text, call_type='USER_MANUAL')
        return result + _footer('emergency', 'claude', provider, call_type='USER_MANUAL')

    # 4. Local queries
    if intent in NL_LOCAL_MAP:
        qtype = NL_LOCAL_MAP[intent]
        return local_query_executor.execute(qtype, original_text=text) + \
            _footer(intent, 'local', 'local')

    # 5. General → GPT-mini
    result = _ai_general_advisory(text)
    return result + _footer('general', 'gpt', 'gpt-4o-mini')


# ── main command handler ─────────────────────────────────

def _footer(intent_name: str, route: str, provider: str,
            call_type: str = '', bypass: bool = False, cost: float = 0.0) -> str:
    return report_formatter._debug_line({
        'intent_name': intent_name,
        'route': route,
        'provider': provider,
        'call_type': call_type,
        'cost': cost,
    })

def handle_command(text: str) -> str:
    t = (text or "").strip()

    # Phase 0: Minimal direct commands (no GPT cost)
    if t in ("/help", "help"):
        return HELP_TEXT + _footer("help", "direct", "local")

    # /debug — toggle debug mode (no GPT cost)
    if t.startswith('/debug'):
        if t == '/debug on':
            return report_formatter.set_debug_mode(True)
        if t == '/debug off':
            return report_formatter.set_debug_mode(False)
        state = 'ON' if report_formatter.is_debug_on() else 'OFF'
        return f'디버그 모드: {state}\n사용법: /debug on 또는 /debug off'

    # /db_health — direct DB health check (no GPT cost)
    if t in ('/db_health', '/dbhealth', 'db_health'):
        return local_query_executor.execute('db_health') + \
            _footer('db_health', 'local', 'local')

    # /claude_audit — Claude API usage audit (no GPT cost)
    if t in ('/claude_audit', '/claude', '/ai_cost', 'claude_audit'):
        return local_query_executor.execute('claude_audit') + \
            _footer('claude_audit', 'local', 'local')

    # /force — cooldown bypass, Claude forced
    if t == '/force' or t.startswith('/force '):
        force_text = t[len('/force'):].strip() or '지금 BTC 전략 분석해줘'
        _log(f'/force command: call_type=USER, text={force_text[:50]}')
        force_intent = {'intent': 'strategy', 'claude_prompt': force_text}
        ai_result, ai_provider = _ai_advisory(force_intent, force_text,
                                               no_fallback=True, force=True)
        return ai_result + _footer('force_strategy', 'claude', ai_provider,
                                   call_type='USER_MANUAL', bypass=True)

    # /detail — expanded news report
    if t == '/detail' or t.startswith('/detail '):
        detail_text = t[len('/detail'):].strip() or '뉴스 상세 분석'
        _log(f'/detail command: detail=True')
        detail_result, detail_provider = _ai_news_claude_advisory(
            detail_text, call_type='AUTO', detail=True)
        return detail_result + _footer('detail', 'claude', detail_provider)

    # Phase 1: NL parser (always runs)
    try:
        parsed = gpt_router.classify_intent(t)
    except Exception:
        parsed = gpt_router._keyword_fallback(t)

    msg_type = parsed.get("type", "QUESTION")
    intent = parsed.get("intent", "general")
    _log(f"type={msg_type} intent={intent} "
         f"confidence={parsed.get('confidence', '?')} "
         f"fallback={parsed.get('_fallback', False)} "
         f"budget_exceeded={parsed.get('_budget_exceeded', False)}")

    # Phase 2: COMMAND → execution flow
    if msg_type == "COMMAND":
        return _handle_nl_command(parsed, t)

    # Phase 3: QUESTION → information/analysis flow
    return _handle_nl_question(parsed, t)

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
