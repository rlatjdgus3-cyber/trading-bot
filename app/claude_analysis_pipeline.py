"""
claude_analysis_pipeline.py — Claude 분석 파이프라인.

ChatAgent에서 Claude 분석이 필요할 때 호출.
- build_context_packet(): 시스템 상태 수집
- run_analysis(): Claude API 호출 + JSON 파싱
- try_auto_apply(): 안전장치 게이트 체인 → 자동 매매 적용
"""
import os
import sys
import json

sys.path.insert(0, '/root/trading-bot/app')

LOG_PREFIX = '[claude_pipeline]'
STRATEGY_SYMBOL = os.getenv('STRATEGY_SYMBOL', 'BTC/USDT:USDT')

ANALYSIS_PROMPT_TEMPLATE = (
    "당신은 BTC 선물 트레이딩봇의 전략 분석 AI입니다.\n"
    "아래 시장 데이터를 분석하고, 반드시 JSON으로만 응답하세요.\n"
    "텍스트 설명은 JSON 내부 필드에 포함합니다.\n\n"
    "=== 컨텍스트 ===\n{context}\n\n"
    "=== 응답 형식 (JSON만 출력) ===\n"
    '{{\n'
    '  "summary": "시장 상황 요약 (한국어, 100자 이내)",\n'
    '  "risk_notes": "리스크 요인 (한국어, 100자 이내)",\n'
    '  "trade_action": {{\n'
    '    "should_trade": true/false,\n'
    '    "symbol": "BTC/USDT:USDT",\n'
    '    "side": "BUY|SELL",\n'
    '    "order_type": "MARKET|LIMIT",\n'
    '    "qty_usd": 200,\n'
    '    "leverage": 3,\n'
    '    "stop_loss_pct": 2.0,\n'
    '    "take_profit_pct": 3.0,\n'
    '    "confidence": 0.75,\n'
    '    "reason": "근거 설명"\n'
    '  }}\n'
    '}}\n'
)

EMERGENCY_PROMPT_TEMPLATE = (
    "긴급 상황이 감지되었습니다. BTC 선물 포지션 전략 분석가로서\n"
    "아래 데이터를 기반으로 즉시 대응 방안을 JSON으로 제시하세요.\n\n"
    "=== 긴급 컨텍스트 ===\n{context}\n\n"
    "=== 응답 형식 (JSON만 출력) ===\n"
    '{{\n'
    '  "summary": "긴급 상황 요약",\n'
    '  "risk_notes": "리스크 평가",\n'
    '  "trade_action": {{\n'
    '    "should_trade": true/false,\n'
    '    "symbol": "BTC/USDT:USDT",\n'
    '    "side": "BUY|SELL",\n'
    '    "order_type": "MARKET",\n'
    '    "qty_usd": 100,\n'
    '    "leverage": 1,\n'
    '    "stop_loss_pct": 3.0,\n'
    '    "take_profit_pct": 5.0,\n'
    '    "confidence": 0.5,\n'
    '    "reason": "긴급 대응 근거"\n'
    '  }}\n'
    '}}\n'
)


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def build_context_packet(cur, chat_history=None):
    """Assemble context dict from various system sources.
    Uses existing cursor for DB queries."""
    packet = {}

    # 1. Chat history summary
    if chat_history:
        recent = chat_history[-5:]
        conv_lines = []
        for entry in recent:
            role = entry.get('role', '')
            content = entry.get('content', '')[:200]
            conv_lines.append(f'{role}: {content}')
        packet['conversation_summary'] = '\n'.join(conv_lines)

    # 2. Position state
    try:
        cur.execute("""
            SELECT side, total_qty, avg_entry_price, stage, trade_budget_used_pct
            FROM position_state WHERE symbol = %s;
        """, (STRATEGY_SYMBOL,))
        row = cur.fetchone()
        if row and row[0]:
            packet['position'] = {
                'side': row[0],
                'total_qty': float(row[1]) if row[1] else 0,
                'avg_entry_price': float(row[2]) if row[2] else 0,
                'stage': int(row[3]) if row[3] else 0,
                'budget_used_pct': float(row[4]) if row[4] else 0,
            }
        else:
            packet['position'] = {'side': 'NONE', 'total_qty': 0}
    except Exception as e:
        packet['position'] = {'error': str(e)}

    # 3. Score engine
    try:
        import score_engine
        scores = score_engine.compute_total(cur=cur)
        packet['scores'] = {
            'total_score': scores.get('total_score'),
            'dominant_side': scores.get('dominant_side'),
            'stage': scores.get('stage'),
            'tech_score': scores.get('tech_score'),
            'regime_score': scores.get('regime_score'),
            'news_event_score': scores.get('news_event_score'),
            'price': scores.get('price'),
        }
    except Exception as e:
        packet['scores'] = {'error': str(e)}

    # 4. Health check
    try:
        import local_query_executor
        health = local_query_executor.execute('health_check')
        packet['health'] = health[:500]
    except Exception as e:
        packet['health'] = f'error: {e}'

    # 5. Recent indicators
    try:
        cur.execute("""
            SELECT rsi_14, atr_14, ma_50, ma_200
            FROM indicators
            WHERE symbol = %s
            ORDER BY ts DESC LIMIT 1;
        """, (STRATEGY_SYMBOL,))
        row = cur.fetchone()
        if row:
            packet['indicators'] = {
                'rsi_14': float(row[0]) if row[0] else None,
                'atr_14': float(row[1]) if row[1] else None,
                'ma_50': float(row[2]) if row[2] else None,
                'ma_200': float(row[3]) if row[3] else None,
            }
    except Exception as e:
        packet['indicators'] = {'error': str(e)}

    # 6. Risk policy
    try:
        cur.execute("""
            SELECT auto_apply_on_claude, auto_apply_on_emergency,
                   max_notional_usdt, max_leverage,
                   sl_min_pct, sl_max_pct, cooldown_sec
            FROM auto_apply_config
            ORDER BY id DESC LIMIT 1;
        """)
        row = cur.fetchone()
        if row:
            packet['risk_policy'] = {
                'auto_apply_on_claude': row[0],
                'auto_apply_on_emergency': row[1],
                'max_notional_usdt': float(row[2]) if row[2] else 500,
                'max_leverage': int(row[3]) if row[3] else 5,
                'sl_min_pct': float(row[4]) if row[4] else 1.0,
                'sl_max_pct': float(row[5]) if row[5] else 4.0,
                'cooldown_sec': int(row[6]) if row[6] else 300,
            }
    except Exception as e:
        packet['risk_policy'] = {'error': str(e)}

    # 7. News summary (compact)
    try:
        cur.execute("""
            SELECT title, impact_score, summary,
                   to_char(ts AT TIME ZONE 'Asia/Seoul', 'MM-DD HH24:MI')
            FROM news
            WHERE ts >= now() - interval '6 hours' AND impact_score >= 5
            ORDER BY impact_score DESC
            LIMIT 5;
        """)
        rows = cur.fetchall()
        news = []
        for r in rows:
            news.append({
                'title': r[0] or '',
                'impact': int(r[1]) if r[1] else 0,
                'summary': (r[2] or '')[:100],
                'ts': r[3] or '',
            })
        packet['top_news'] = news
    except Exception as e:
        packet['top_news'] = []

    return packet


def run_analysis(packet, analysis_type='strategy'):
    """Call Claude with context packet. Returns structured dict.

    analysis_type: 'strategy' or 'emergency'
    """
    import claude_gate

    context_str = json.dumps(packet, ensure_ascii=False, default=str,
                             indent=2)
    if len(context_str) > 6000:
        context_str = context_str[:6000] + '\n...(truncated)'

    if analysis_type == 'emergency':
        prompt = EMERGENCY_PROMPT_TEMPLATE.format(context=context_str)
        gate = 'auto_apply'
        call_type = 'AUTO_EMERGENCY'
    else:
        prompt = ANALYSIS_PROMPT_TEMPLATE.format(context=context_str)
        gate = 'chat_claude'
        call_type = 'USER_MANUAL'

    _log(f'run_analysis: type={analysis_type} gate={gate}')

    result = claude_gate.call_claude(
        gate=gate,
        prompt=prompt,
        cooldown_key=f'chat_analysis_{analysis_type}',
        context={'caller': 'claude_analysis_pipeline',
                 'intent': analysis_type},
        max_tokens=800,
        call_type=call_type,
    )

    if result.get('fallback_used'):
        _log(f'Claude denied: {result.get("gate_reason")}')
        return {
            'summary': 'Claude 분석 불가 (게이트 차단)',
            'risk_notes': result.get('gate_reason', ''),
            'trade_action': {'should_trade': False},
            'provider': 'claude(denied)',
            'model': result.get('model', ''),
            'fallback_used': True,
        }

    # Parse JSON response
    raw_text = result.get('text', '')
    parsed = _parse_claude_json(raw_text)

    return {
        'summary': parsed.get('summary', '분석 결과 없음'),
        'risk_notes': parsed.get('risk_notes', ''),
        'trade_action': parsed.get('trade_action', {'should_trade': False}),
        'provider': 'anthropic',
        'model': result.get('model', ''),
        'estimated_cost_usd': result.get('estimated_cost_usd', 0),
        'input_tokens': result.get('input_tokens', 0),
        'output_tokens': result.get('output_tokens', 0),
        'raw_text': raw_text,
    }


def _parse_claude_json(text):
    """Parse JSON from Claude response text."""
    if not text:
        return {}
    # Try direct parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # Try extracting JSON block
    import re
    match = re.search(r'\{[\s\S]*\}', text)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    _log(f'JSON parse failed, raw={text[:200]}')
    return {'summary': text[:300], 'trade_action': {'should_trade': False}}


def try_auto_apply(cur, trade_action, chat_id, trace_id):
    """Gate chain for auto-applying Claude trade decision.

    All gates must pass:
    1. auto_apply_config.auto_apply_on_claude == true
    2. trade_arm_manager.is_armed(chat_id) == true
    3. _check_auto_trading_active (3-gate: test_mode, LIVE_TRADING, trade_switch)
    4. Risk validation (notional, leverage, stop-loss)
    5. Cooldown check
    6. Duplicate prevention

    Returns dict with 'applied', 'blocked_reason', 'execution_queue_id'.
    """
    result = {'applied': False, 'blocked_reason': None,
              'execution_queue_id': None}

    # Gate 1: auto_apply_on_claude
    try:
        cur.execute("""
            SELECT auto_apply_on_claude, max_notional_usdt, max_leverage,
                   sl_min_pct, sl_max_pct, cooldown_sec
            FROM auto_apply_config
            ORDER BY id DESC LIMIT 1;
        """)
        row = cur.fetchone()
        if not row or not row[0]:
            result['blocked_reason'] = 'auto_apply_on_claude 비활성'
            _log_decision(cur, trace_id, trade_action, result)
            return result
        config = {
            'max_notional_usdt': float(row[1]) if row[1] else 500,
            'max_leverage': int(row[2]) if row[2] else 5,
            'sl_min_pct': float(row[3]) if row[3] else 1.0,
            'sl_max_pct': float(row[4]) if row[4] else 4.0,
            'cooldown_sec': int(row[5]) if row[5] else 300,
        }
    except Exception as e:
        result['blocked_reason'] = f'config 조회 오류: {e}'
        _log_decision(cur, trace_id, trade_action, result)
        return result

    # Gate 2: Armed state
    import trade_arm_manager
    armed, arm_info = trade_arm_manager.is_armed(chat_id)
    if not armed:
        result['blocked_reason'] = '무장 상태 비활성 (/trade arm 필요)'
        _log_decision(cur, trace_id, trade_action, result, arm_state=arm_info)
        return result

    # Gate 3: Auto-trading active (3-gate check)
    try:
        # Import from telegram_cmd_poller
        sys.path.insert(0, '/root/trading-bot/app')
        import test_utils
        test = test_utils.load_test_mode()
        if not test_utils.is_test_active(test):
            result['blocked_reason'] = '테스트 모드 비활성'
            _log_decision(cur, trace_id, trade_action, result, arm_state=arm_info)
            return result

        if os.getenv('LIVE_TRADING') != 'YES_I_UNDERSTAND':
            result['blocked_reason'] = 'LIVE_TRADING 미설정'
            _log_decision(cur, trace_id, trade_action, result, arm_state=arm_info)
            return result

        cur.execute('SELECT enabled FROM trade_switch ORDER BY id DESC LIMIT 1;')
        sw_row = cur.fetchone()
        if not sw_row or not sw_row[0]:
            result['blocked_reason'] = '매매 스위치 비활성'
            _log_decision(cur, trace_id, trade_action, result, arm_state=arm_info)
            return result
    except Exception as e:
        result['blocked_reason'] = f'auto_trading 확인 오류: {e}'
        _log_decision(cur, trace_id, trade_action, result, arm_state=arm_info)
        return result

    # Gate 4: Risk validation
    qty_usd = trade_action.get('qty_usd', 0)
    leverage = trade_action.get('leverage', 1)
    sl_pct = trade_action.get('stop_loss_pct', 0)

    risk_check = {}
    if qty_usd > config['max_notional_usdt']:
        result['blocked_reason'] = (
            f"금액 초과: {qty_usd} > {config['max_notional_usdt']} USDT")
        risk_check['qty_usd'] = 'FAIL'
        _log_decision(cur, trace_id, trade_action, result,
                      arm_state=arm_info, risk_check=risk_check)
        return result
    risk_check['qty_usd'] = 'OK'

    if leverage > config['max_leverage']:
        result['blocked_reason'] = (
            f"레버리지 초과: {leverage} > {config['max_leverage']}")
        risk_check['leverage'] = 'FAIL'
        _log_decision(cur, trace_id, trade_action, result,
                      arm_state=arm_info, risk_check=risk_check)
        return result
    risk_check['leverage'] = 'OK'

    if sl_pct < config['sl_min_pct'] or sl_pct > config['sl_max_pct']:
        result['blocked_reason'] = (
            f"손절폭 범위 벗어남: {sl_pct}% "
            f"(허용: {config['sl_min_pct']}~{config['sl_max_pct']}%)")
        risk_check['stop_loss'] = 'FAIL'
        _log_decision(cur, trace_id, trade_action, result,
                      arm_state=arm_info, risk_check=risk_check)
        return result
    risk_check['stop_loss'] = 'OK'

    # Gate 5: Cooldown — recent applied decision
    try:
        cur.execute("""
            SELECT ts FROM claude_trade_decision_log
            WHERE applied = true
            ORDER BY ts DESC LIMIT 1;
        """)
        last_applied = cur.fetchone()
        if last_applied:
            cur.execute("""
                SELECT EXTRACT(EPOCH FROM (now() - %s));
            """, (last_applied[0],))
            elapsed = cur.fetchone()[0]
            if elapsed < config['cooldown_sec']:
                result['blocked_reason'] = (
                    f"쿨다운 중: {int(elapsed)}초 경과 "
                    f"(최소 {config['cooldown_sec']}초)")
                _log_decision(cur, trace_id, trade_action, result,
                              arm_state=arm_info, risk_check=risk_check)
                return result
    except Exception as e:
        _log(f'cooldown check error: {e}')

    # Gate 6: Duplicate prevention
    side = trade_action.get('side', '')
    try:
        cur.execute("""
            SELECT id FROM execution_queue
            WHERE symbol = %s
              AND direction = %s
              AND status IN ('PENDING', 'PICKED')
              AND ts >= now() - interval '10 minutes';
        """, (STRATEGY_SYMBOL, side))
        if cur.fetchone():
            result['blocked_reason'] = f'중복 주문 감지 ({side} PENDING 10분 이내)'
            _log_decision(cur, trace_id, trade_action, result,
                          arm_state=arm_info, risk_check=risk_check)
            return result
    except Exception as e:
        _log(f'duplicate check error: {e}')

    # All gates passed — enqueue
    try:
        direction = 'LONG' if side == 'BUY' else 'SHORT'
        action_type = 'ADD'
        meta = json.dumps({
            'source': 'claude_auto_apply',
            'trace_id': trace_id,
            'confidence': trade_action.get('confidence', 0),
            'reason': trade_action.get('reason', ''),
            'leverage': leverage,
            'stop_loss_pct': sl_pct,
            'take_profit_pct': trade_action.get('take_profit_pct', 0),
        }, default=str)

        cur.execute("""
            INSERT INTO execution_queue
                (symbol, action_type, direction, target_usdt,
                 source, reason, priority, expire_at, meta)
            VALUES (%s, %s, %s, %s, %s, %s, %s,
                    now() + interval '5 minutes', %s::jsonb)
            RETURNING id;
        """, (STRATEGY_SYMBOL, action_type, direction, qty_usd,
              'claude_auto_apply', trade_action.get('reason', 'claude_analysis'),
              2, meta))
        eq_row = cur.fetchone()
        eq_id = eq_row[0] if eq_row else None

        result['applied'] = True
        result['execution_queue_id'] = eq_id
        _log(f'AUTO_APPLY: eq_id={eq_id} {direction} {qty_usd} USDT')

        _log_decision(cur, trace_id, trade_action, result,
                      arm_state=arm_info, risk_check=risk_check)
        return result

    except Exception as e:
        result['blocked_reason'] = f'enqueue 오류: {e}'
        _log(f'enqueue error: {e}')
        _log_decision(cur, trace_id, trade_action, result,
                      arm_state=arm_info, risk_check=risk_check)
        return result


def _log_decision(cur, trace_id, trade_action, result,
                  arm_state=None, risk_check=None):
    """Insert into claude_trade_decision_log."""
    try:
        cur.execute("""
            INSERT INTO claude_trade_decision_log
                (trace_id, provider, model, trade_action, applied,
                 blocked_reason, execution_queue_id, arm_state, risk_check_result)
            VALUES (%s, %s, %s, %s::jsonb, %s, %s, %s, %s::jsonb, %s::jsonb);
        """, (
            trace_id, 'anthropic', 'claude-sonnet',
            json.dumps(trade_action or {}, default=str),
            result.get('applied', False),
            result.get('blocked_reason'),
            result.get('execution_queue_id'),
            json.dumps(arm_state or {}, default=str),
            json.dumps(risk_check or {}, default=str),
        ))
    except Exception as e:
        _log(f'_log_decision error: {e}')
