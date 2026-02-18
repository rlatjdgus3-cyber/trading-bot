"""
chat_agent.py — GPT ChatAgent with function calling.

Main entry: process_message(chat_id, text) -> (response_str, metadata_dict)
Uses OpenAI function calling to query system state, then produces
natural language responses. Claude triggered on explicit request or GPT failure.
"""
import os
import sys
import json
import time
import uuid

sys.path.insert(0, '/root/trading-bot/app')

import chat_memory
import local_query_executor

LOG_PREFIX = '[chat_agent]'
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '')
MODEL = os.getenv('OPENAI_MODEL', 'gpt-4o-mini')
MAX_TOOL_ROUNDS = 3
GPT_FAIL_FILE = '/root/trading-bot/app/.chat_agent_gpt_fails.json'

SYSTEM_PROMPT = (
    "당신은 'OpenClaw'라는 BTC 선물 트레이딩 어시스턴트입니다.\n"
    "전문 트레이더와 대화하듯 자연스럽게 한국어(존댓말)로 답변하세요.\n\n"
    "## 답변 스타일\n"
    "- 데이터 나열 대신 맥락 있는 해석 제공 (예: 'RSI 72로 과매수 진입 구간이에요').\n"
    "- 핵심 인사이트 먼저, 보조 데이터는 뒤에 간략히.\n"
    "- 기본 300자 내외. 상세 요청 시 700자까지 확장.\n\n"
    "## 도구 사용\n"
    "- 질문에 필요한 도구만 최소 호출. 도구 결과를 그대로 붙여넣지 말고 자연어로 재구성.\n"
    "- 가격 질문 → get_price 하나, 종합 질문 → get_state 하나.\n\n"
    "## 금지사항 (절대 규칙)\n"
    "- 매매 권유/추천 금지. 분석과 정보 제공에 집중.\n"
    "- 추측 금지. 데이터가 없으면 '현재 데이터 없음'으로 명시.\n"
    "- 포지션 수량, 진입가, 잔고, 미체결 주문 등 수치를 직접 생성/답변 절대 금지.\n"
    "- '롱 보유중', '숏 보유중', '체결 완료' 같은 단정 표현은 EXCHANGE 데이터가 확인된 경우에만 사용.\n"
    "- STRATEGY_DB 상태(INTENT_ENTER/IN_POSITION)만으로 '보유중/체결됨'을 단정하면 절대 안 됨.\n"
    "- EXCHANGE=NONE인데 DB=INTENT_ENTER/OPEN이면 반드시 '체결 미확인(전략DB 기록상 진입 시도)'으로 표현.\n"
    "- '진입가'는 반드시 출처를 구분: '체결가(EXCHANGE)' vs '기준가(DB)'.\n"
    "- 포지션/잔고/주문/체결 관련 질문이 오면 시스템이 자동 조회하므로 직접 답하지 마세요.\n"
)

CLAUDE_TRIGGERS = ['클로드', 'claude', '@claude', 'force_claude']

_EMERGENCY_KEYWORDS = frozenset({
    '급락', '폭락', '급등', '비상', '긴급', 'crash', 'dump',
    'liquidation', 'emergency', '청산', '위험',
})

# Fact keyword → internal command routing (priority order: specific first)
# More specific routes first; general fact queries last → full snapshot
# Priority: "왜" 질문 → 잔고 → 손익/리스크 → 주문 → 전략포지션 → 일반포지션(fact_snapshot)
_FACT_ROUTES = [
    # "Why" / complaint queries → full snapshot (must be before simple '주문')
    (['왜 주문', '왜 안', '안 나가', '안나가', '왜 안사', '왜 안팔아'], 'fact_snapshot'),
    # Balance queries
    (['잔고', 'balance', '자산', 'account', '계좌'], 'account_exch'),
    # PnL / risk queries → full snapshot
    (['손익', 'pnl', '리스크', 'risk'], 'fact_snapshot'),
    # Order queries
    (['주문', 'order', '미체결', '주문나갔어', '주문 나갔어'], 'orders_exch'),
    # Strategy position queries (before general position)
    (['전략 포지션', '전략포지션', 'position_strat'], 'position_strat'),
    # General fact queries → full 4-section snapshot
    (['포지션', 'position', '포지션어때', '포지션 어때',
      '들어갔어', '평단', '평균단가',
      '진입가', 'entry price', '체결가',
      '레버리지', 'leverage', '마크가', 'mark',
      '청산가', 'liquidation', '수량', 'qty',
      '체결', '보유', '매매 상태', '매매상태', '매매중', '매매됐',
      '거래켜', '거래꺼',
      '게이트', '차단', '대기',
      'gate', 'blocked', 'wait',
      '현재 상태', '테스트 동작',
      '스위치', 'switch', '중지', 'paused',
      '불일치', 'mismatch', 'reconcile'], 'fact_snapshot'),
]
# Pre-computed flat set for fast detection
_FACT_KEYWORDS = frozenset(
    kw for keywords, _ in _FACT_ROUTES for kw in keywords
)


def _resolve_fact_command(text_lower):
    """Match fact keywords and return the internal command to execute.
    Returns command string or None."""
    for keywords, cmd in _FACT_ROUTES:
        if any(kw in text_lower for kw in keywords):
            return cmd
    return None

# ── OpenAI Function Calling Tool Definitions ──

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_state",
            "description": "시스템 전체 현황 조회 (포지션, 스코어, 가격, 서비스 상태 등)",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_health",
            "description": "서비스 상태 확인 (OK/DOWN)",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_price",
            "description": "BTC 현재가 조회",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_indicators",
            "description": "기술지표 스냅샷 (RSI, ATR, MA 등)",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_score",
            "description": "스코어 엔진 결과 (TECH, MACRO, REGIME, NEWS 축)",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_position",
            "description": "현재 포지션 정보 (사이드, 수량, 진입가, 스테이지)",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_news_summary",
            "description": "최근 뉴스 요약 (전략 반영 뉴스)",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_news_filter_stats",
            "description": "뉴스 필터 통계 (TIER별, 카테고리별)",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_db_coverage",
            "description": "DB 데이터 커버리지 (테이블별 기간, 행 수)",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_backfill_status",
            "description": "백필 작업 상태 (진행률, 마지막 실행)",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_volatility",
            "description": "변동성 요약 (ATR, 볼린저, 체제 분석)",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_macro",
            "description": "매크로 지표 요약 (QQQ, 금리, 펀딩비 등)",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_errors",
            "description": "최근 에러 로그",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_evidence",
            "description": "보조지표 근거 데이터",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },
]

TOOL_DISPATCH = {
    'get_state':            'status_full',
    'get_health':           'health_check',
    'get_price':            'btc_price',
    'get_indicators':       'indicator_snapshot',
    'get_score':            'score_summary',
    'get_position':         'position_exch',
    'get_news_summary':     'news_applied',
    'get_news_filter_stats':'debug_news_filter_stats',
    'get_db_coverage':      'db_coverage',
    'get_backfill_status':  'debug_backfill_status',
    'get_volatility':       'volatility_summary',
    'get_macro':            'macro_summary',
    'get_errors':           'recent_errors',
    'get_evidence':         'evidence',
}


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _execute_tool(name, args_str=''):
    """Execute a tool by name. Returns result string."""
    query_type = TOOL_DISPATCH.get(name)
    if not query_type:
        return f'Unknown tool: {name}'
    try:
        result = local_query_executor.execute(query_type)
        # Truncate long results to avoid token bloat
        if len(result) > 3000:
            result = result[:3000] + '\n...(truncated)'
        return result
    except Exception as e:
        return f'Tool error ({name}): {e}'


def _detect_claude_trigger(text):
    """Check if text contains Claude trigger keywords."""
    t = text.lower()
    for trigger in CLAUDE_TRIGGERS:
        if trigger in t:
            return True
    return False


def _get_gpt_fail_count():
    """Load consecutive GPT failure count."""
    try:
        with open(GPT_FAIL_FILE, 'r') as f:
            data = json.load(f)
        return data.get('consecutive_fails', 0)
    except Exception:
        return 0


def _set_gpt_fail_count(count):
    """Save GPT failure count."""
    try:
        with open(GPT_FAIL_FILE, 'w') as f:
            json.dump({'consecutive_fails': count,
                       'updated_at': time.strftime('%Y-%m-%d %H:%M:%S')}, f)
    except Exception:
        pass


def _build_messages(history, user_text):
    """Convert chat_memory history + current message into OpenAI messages format."""
    messages = [{'role': 'system', 'content': SYSTEM_PROMPT}]
    for entry in history:
        role = entry.get('role', 'user')
        content = entry.get('content', '')
        if role == 'tool_result':
            continue  # already reflected in assistant response, skip to avoid duplication
        elif role in ('user', 'assistant'):
            messages.append({'role': role, 'content': content})
    messages.append({'role': 'user', 'content': user_text})
    return messages


def process_message(chat_id, text):
    """Main entry point. Returns (response_str, metadata_dict).

    Flow:
    1. Check Claude trigger → delegate to claude_analysis_pipeline
    2. Check emergency → summarize via GPT
    3. Load chat history → GPT with function calling → response
    4. Save conversation turns
    5. On GPT failure → fallback to gpt_router._keyword_fallback
    """
    trace_id = uuid.uuid4().hex[:12]
    meta = {
        'trace_id': trace_id,
        'provider': MODEL,
        'route': 'chat_agent',
        'intent': 'chat',
        'call_type': '',
    }

    t = (text or '').strip()
    if not t:
        return ('메시지가 비어 있습니다.', meta)

    # ── Fact keyword intercept: auto-execute internal command ──
    t_lower = t.lower()
    fact_cmd = _resolve_fact_command(t_lower)
    if fact_cmd:
        meta['route'] = 'fact_auto_fetch'
        meta['intent'] = fact_cmd
        try:
            result = local_query_executor.execute(fact_cmd)
            return (result, meta)
        except Exception as e:
            _log(f'fact_auto_fetch error ({fact_cmd}): {e}')
            return (
                f'DATA_STATUS: UNKNOWN\nSOURCE: EXCHANGE\nREASON: {e}',
                meta,
            )

    # ── Claude trigger detection ──
    if _detect_claude_trigger(t):
        _log(f'Claude trigger detected: trace={trace_id}')
        meta['route'] = 'claude_analysis'
        meta['intent'] = 'claude_analysis'
        try:
            import claude_analysis_pipeline as cap
            # Build context
            from db_config import get_conn
            conn = get_conn(autocommit=True)
            try:
                with conn.cursor() as cur:
                    history = chat_memory.load_history(chat_id, limit=5)
                    packet = cap.build_context_packet(cur, chat_history=history)
            finally:
                conn.close()

            # Run Claude analysis
            analysis = cap.run_analysis(packet, analysis_type='strategy')
            meta['provider'] = analysis.get('provider', 'claude')
            meta['model'] = analysis.get('model', '')
            meta['call_type'] = 'USER_MANUAL'

            # Format response
            summary = analysis.get('summary', '')
            risk_notes = analysis.get('risk_notes', '')
            trade_action = analysis.get('trade_action', {})

            import report_formatter
            response = report_formatter.format_claude_analysis(analysis)

            # Try auto-apply if trade_action exists
            if trade_action and trade_action.get('should_trade'):
                try:
                    conn2 = get_conn(autocommit=False)
                    try:
                        with conn2.cursor() as cur2:
                            apply_result = cap.try_auto_apply(
                                cur2, trade_action, chat_id, trace_id)
                            conn2.commit()
                        if apply_result.get('applied'):
                            response += (
                                f"\n\n✅ 자동 매매 적용됨 "
                                f"(eq_id={apply_result.get('execution_queue_id')})")
                        elif apply_result.get('blocked_reason'):
                            response += (
                                f"\n\n⛔ 자동 매매 차단: "
                                f"{apply_result['blocked_reason']}")
                    finally:
                        conn2.close()
                except Exception as e:
                    _log(f'auto_apply error: {e}')

            # Save conversation
            chat_memory.save_turn(chat_id, 'user', t,
                                  metadata={'trace_id': trace_id})
            chat_memory.save_turn(chat_id, 'assistant', response,
                                  metadata={'trace_id': trace_id,
                                            'provider': 'claude'})
            return (response, meta)

        except Exception as e:
            _log(f'Claude analysis error: {e}')
            import traceback
            traceback.print_exc()
            meta['fallback_reason'] = f'claude_error: {e}'
            # Fall through to GPT

    # ── Emergency detection (only when keywords present) ──
    _has_emergency_kw = any(kw in t.lower() for kw in _EMERGENCY_KEYWORDS)
    if _has_emergency_kw:
        try:
            import emergency_detector
            sev = emergency_detector.run_check()
            if sev and sev.get('alerts'):
                _log(f'Emergency detected: trace={trace_id}')
                meta['intent'] = 'emergency'
                emergency_summary = emergency_detector.format_alerts(sev)

                # Check auto_apply_on_emergency
                try:
                    from db_config import get_conn
                    conn = get_conn(autocommit=True)
                    try:
                        with conn.cursor() as cur:
                            cur.execute("""
                                SELECT auto_apply_on_emergency
                                FROM auto_apply_config
                                ORDER BY id DESC LIMIT 1;
                            """)
                            row = cur.fetchone()
                            auto_emergency = row[0] if row else False
                    finally:
                        conn.close()

                    import trade_arm_manager
                    armed, arm_info = trade_arm_manager.is_armed(chat_id)

                    if auto_emergency and armed:
                        _log('Emergency auto-apply: armed + auto_emergency=true')
                        import claude_analysis_pipeline as cap
                        conn2 = get_conn(autocommit=True)
                        try:
                            with conn2.cursor() as cur2:
                                packet = cap.build_context_packet(cur2)
                                packet['emergency'] = sev
                            analysis = cap.run_analysis(packet,
                                                        analysis_type='emergency')
                            trade_action = analysis.get('trade_action', {})
                            if trade_action and trade_action.get('should_trade'):
                                conn3 = get_conn(autocommit=False)
                                try:
                                    with conn3.cursor() as cur3:
                                        apply_result = cap.try_auto_apply(
                                            cur3, trade_action, chat_id, trace_id)
                                        conn3.commit()
                                    if apply_result.get('applied'):
                                        emergency_summary += (
                                            f"\n\n✅ 긴급 자동 매매 적용 "
                                            f"(eq_id={apply_result.get('execution_queue_id')})")
                                finally:
                                    conn3.close()
                        finally:
                            conn2.close()
                        meta['provider'] = 'claude'
                        meta['call_type'] = 'AUTO_EMERGENCY'
                except Exception as e:
                    _log(f'Emergency auto-apply check error: {e}')

                # Still provide GPT summary of the emergency context
                response = emergency_summary
                chat_memory.save_turn(chat_id, 'user', t,
                                      metadata={'trace_id': trace_id})
                chat_memory.save_turn(chat_id, 'assistant', response,
                                      metadata={'trace_id': trace_id,
                                                'intent': 'emergency'})
                return (response, meta)
        except Exception as e:
            _log(f'emergency_detector error: {e}')

    # ── GPT ChatAgent with function calling ──
    try:
        history = chat_memory.load_history(chat_id, limit=30)
        messages = _build_messages(history, t)

        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY, timeout=15)

        response_text = None
        tool_results_summary = []

        for round_i in range(MAX_TOOL_ROUNDS):
            resp = client.chat.completions.create(
                model=MODEL,
                messages=messages,
                tools=TOOLS,
                tool_choice='auto',
                max_tokens=800,
                temperature=0.3,
            )

            choice = resp.choices[0]

            # No tool calls → final response
            if not choice.message.tool_calls:
                response_text = choice.message.content or ''
                break

            # Process tool calls
            messages.append(choice.message)
            for tc in choice.message.tool_calls:
                tool_name = tc.function.name
                tool_args = tc.function.arguments or '{}'
                _log(f'tool_call: {tool_name} (round {round_i+1})')

                result = _execute_tool(tool_name, tool_args)
                messages.append({
                    'role': 'tool',
                    'tool_call_id': tc.id,
                    'content': result,
                })
                tool_results_summary.append(
                    f'[{tool_name}] {result[:200]}')

        if response_text is None:
            # Max rounds reached, get final response without tools
            resp = client.chat.completions.create(
                model=MODEL,
                messages=messages,
                max_tokens=800,
                temperature=0.3,
            )
            response_text = resp.choices[0].message.content or ''

        # Success — reset GPT fail counter
        _set_gpt_fail_count(0)

        # Save conversation turns
        chat_memory.save_turn(chat_id, 'user', t,
                              metadata={'trace_id': trace_id})
        chat_memory.save_turn(chat_id, 'assistant', response_text,
                              metadata={'trace_id': trace_id,
                                        'provider': MODEL})
        # Save tool results as context
        if tool_results_summary:
            summary = '\n'.join(tool_results_summary)
            if len(summary) > 2000:
                summary = summary[:2000] + '...'
            chat_memory.save_turn(chat_id, 'tool_result', summary,
                                  metadata={'trace_id': trace_id})

        # Trim old history periodically
        chat_memory.trim_old(chat_id, keep=30)

        return (response_text, meta)

    except Exception as e:
        _log(f'GPT ChatAgent error: {e}')
        gpt_error = str(e)  # preserve before inner except blocks clear `e`

        # Track consecutive failures
        fails = _get_gpt_fail_count() + 1
        _set_gpt_fail_count(fails)
        meta['fallback_reason'] = f'gpt_error: {gpt_error}'

        # 2+ consecutive GPT failures → try Claude fallback
        if fails >= 2:
            _log(f'GPT failed {fails}x consecutively → Claude fallback')
            try:
                import claude_analysis_pipeline as cap
                from db_config import get_conn
                conn = get_conn(autocommit=True)
                try:
                    with conn.cursor() as cur:
                        packet = cap.build_context_packet(cur)
                    analysis = cap.run_analysis(packet,
                                                analysis_type='strategy')
                    meta['provider'] = 'claude(gpt_fallback)'
                    meta['route'] = 'claude_fallback'
                    import report_formatter
                    response = report_formatter.format_claude_analysis(analysis)
                    chat_memory.save_turn(chat_id, 'user', t,
                                          metadata={'trace_id': trace_id})
                    chat_memory.save_turn(chat_id, 'assistant', response,
                                          metadata={'trace_id': trace_id,
                                                    'provider': 'claude',
                                                    'fallback': True})
                    _set_gpt_fail_count(0)
                    return (response, meta)
                finally:
                    conn.close()
            except Exception as ce:
                _log(f'Claude fallback also failed: {ce}')

        # Final fallback: gpt_router._keyword_fallback + local_query_executor
        meta['route'] = 'keyword_fallback'
        meta['provider'] = 'keyword_fallback'
        try:
            import gpt_router
            parsed = gpt_router._keyword_fallback(t)
            query_type = parsed.get('local_query_type')
            if query_type:
                result = local_query_executor.execute(query_type)
                return (result, meta)
            else:
                return (f'시스템 일시 오류. 잠시 후 다시 시도해주세요.\n(오류: {gpt_error})', meta)
        except Exception:
            return (f'시스템 일시 오류. 잠시 후 다시 시도해주세요.\n(오류: {gpt_error})', meta)
