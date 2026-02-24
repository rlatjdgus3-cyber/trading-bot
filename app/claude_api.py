"""
claude_api.py — Claude AI emergency analysis wrapper.

Uses Anthropic API to analyze sudden market changes and recommend actions.
Model: claude-sonnet-4-20250514 (~$0.01/call)
Never raises exceptions — always returns a valid response dict.
"""
import re
import sys
import json
sys.path.insert(0, '/root/trading-bot/app')
LOG_PREFIX = '[claude_api]'
FALLBACK_RESPONSE = {
    'action': 'SKIP',
    'recommended_action': 'SKIP',  # backward compat
    'reduce_pct': 0,
    'target_stage': 0,
    'reason_code': 'API_CALL_FAILED',
    'reason_bullets': ['API_CALL_FAILED'],  # backward compat
    'confidence': None,
    'ttl_seconds': 0,
    'fallback_used': True,
}

ABORT_RESPONSE = {
    'action': 'ABORT',
    'recommended_action': 'ABORT',  # backward compat
    'reduce_pct': 0,
    'target_stage': 0,
    'reason_code': 'REALTIME_DATA_UNAVAILABLE',
    'reason_bullets': ['REALTIME_DATA_UNAVAILABLE'],  # backward compat
    'confidence': 0,
    'ttl_seconds': 0,
    'aborted': True,
}


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def emergency_analysis(context_packet=None, snapshot=None):
    """Analyze emergency via Claude gate. Falls back on denial."""
    if snapshot:
        import market_snapshot
        valid, reason = market_snapshot.validate_snapshot(snapshot)
        if not valid:
            return {**ABORT_RESPONSE, 'abort_reason': reason}

    ctx = context_packet or {}
    import claude_gate
    ctx_compact = claude_gate.compact_context(ctx)
    prompt = _build_prompt(ctx_compact, snapshot=snapshot)

    trigger = ctx.get('trigger', {})
    cooldown_key = trigger.get('type', 'unknown')
    gate_context = {
        'trigger_type': trigger.get('type', ''),
        'zscore_band': ctx.get('zscore_band', ''),
        'is_emergency': True,
    }

    result = claude_gate.call_claude(
        gate='emergency', prompt=prompt, cooldown_key=cooldown_key,
        context=gate_context, max_tokens=600, call_type='EMERGENCY')

    if result.get('fallback_used'):
        _log(f'gate denied: {result.get("gate_reason", "unknown")}')
        return {**FALLBACK_RESPONSE, 'fallback_used': True, 'api_latency_ms': 0,
                'gate_reason': result.get('gate_reason', ''),
                'call_type': 'EMERGENCY'}

    parsed = _parse_response(result.get('text', ''))
    parsed['fallback_used'] = False
    parsed['api_latency_ms'] = result.get('api_latency_ms', 0)
    parsed['input_tokens'] = result.get('input_tokens', 0)
    parsed['output_tokens'] = result.get('output_tokens', 0)
    parsed['estimated_cost_usd'] = result.get('estimated_cost_usd', 0)
    parsed['gate_type'] = result.get('gate_type', 'emergency')
    parsed['model'] = result.get('model', '')
    return parsed


def _build_prompt(ctx=None, snapshot=None):
    if ctx is None:
        ctx = {}
    pos = ctx.get('position', {})
    ind = ctx.get('indicators', {})
    trigger = ctx.get('trigger', {})
    scores = ctx.get('scores', {})
    unified = ctx.get('unified_score') or scores.get('unified') or {}

    # Override indicators from snapshot if available
    if snapshot:
        ind = {
            'atr': snapshot.get('atr_14', ind.get('atr')),
            'rsi': snapshot.get('rsi_14', ind.get('rsi')),
            'kijun': snapshot.get('kijun', ind.get('kijun')),
            'poc': snapshot.get('poc', ind.get('poc')),
            'vah': snapshot.get('vah', ind.get('vah')),
            'val': snapshot.get('val', ind.get('val')),
        }
        ctx['price'] = snapshot.get('price', ctx.get('price', 0))

    score_section = ''
    if unified:
        score_section = (
            f"## Unified Score (5-Axis)\n"
            f"- Total: {unified.get('total_score', 0):+.1f} (abs={unified.get('abs_score', 0):.1f}, stage={unified.get('stage', '?')})\n"
            f"- Tech: {unified.get('tech_score', 0):+d}\n"
            f"- Macro: {unified.get('macro_score', 0):+d}\n"
            f"- Regime: {unified.get('regime_score', 0):+d}\n"
            f"- Liquidity: {unified.get('liquidity_score', 0):+d}\n"
            f"- Position: {unified.get('position_score', 0):+d}\n"
            f"- Dynamic SL: {unified.get('dynamic_stop_loss_pct', 2):.1f}%"
        )

        similar = unified.get('axis_details', {}).get('macro', {}).get('similar_events', [])
        if similar:
            events_text = '\n'.join(
                f"  - {e.get('headline', '?')}: move_4h={e.get('btc_move_4h', '?')}%"
                for e in similar[:3]
            )
            score_section += f'\n\n## Similar Historical Events\n{events_text}'


    prompt = (
        f"You are a crypto trading risk analyst. A sudden market event was detected.\n"
        f"Use ONLY the market data provided below. Do NOT use general knowledge for price levels.\n"
        f"Analyze and recommend an action. Respond ONLY in valid JSON.\n\n"
        f"## Current Position\n"
        f"- Side: {pos.get('side', 'none')}\n"
        f"- Quantity: {pos.get('qty', 0)} BTC\n"
        f"- Entry Price: ${pos.get('entry_price', 0):,.1f}\n"
        f"- Unrealized PnL: {pos.get('upnl', 0):+.2f} USDT\n\n"
        f"## Trigger Event\n"
        f"- Type: {trigger.get('type', 'unknown')}\n"
        f"- Detail: {json.dumps(trigger.get('detail', {}), default=str)}\n\n"
        f"## Market Data\n"
        f"- Current Price: ${ctx.get('price', 0):,.1f}\n"
        f"- ATR-14: {ind.get('atr', 'N/A')}\n"
        f"- RSI-14: {ind.get('rsi', 'N/A')}\n"
        f"- Kijun: {ind.get('kijun', 'N/A')}\n"
        f"- POC: {ind.get('poc', 'N/A')}\n"
        f"- VAH: {ind.get('vah', 'N/A')} / VAL: {ind.get('val', 'N/A')}\n"
        f"- Funding Rate: {ctx.get('funding_rate', 'N/A')}\n\n"
        f"{score_section}\n\n"
        f"## Recent News\n"
        f"{json.dumps(ctx.get('news', []), ensure_ascii=False)[:500]}\n\n"
        f"{_build_fact_section(ctx)}\n\n"
        f"{_build_decision_history_section(ctx)}"
        f"{_build_snapshot_section(snapshot)}"
        f"## Instructions\n"
        f"You are the EXECUTION AUTHORITY. Your decision will be executed immediately.\n"
        f"Respond with ONLY this JSON (no markdown, no explanation):\n"
        f'{{\n'
        f'  "action": "HOLD|OPEN_LONG|OPEN_SHORT|REDUCE|CLOSE|REVERSE",\n'
        f'  "reduce_pct": 0,\n'
        f'  "target_stage": 1,\n'
        f'  "reason_code": "short_description",\n'
        f'  "confidence": 0.0,\n'
        f'  "ttl_seconds": 60\n'
        f'}}\n\n'
        f"Rules:\n"
        f"- HOLD: no action needed\n"
        f"- OPEN_LONG: open/add long position (target_stage=1~7, each stage=10% budget)\n"
        f"- OPEN_SHORT: open/add short position (target_stage=1~7)\n"
        f"- REDUCE: reduce position by reduce_pct% (10-75%)\n"
        f"- CLOSE: fully close position\n"
        f"- REVERSE: close and open opposite (only if very high confidence)\n"
        f"- confidence: 0.0 to 1.0\n"
        f"- ttl_seconds: recommendation validity (30-300)\n"
        f"- reason_code: brief English reason (max 200 chars)\n"
        f"- Use Decision History: do not claim signal reversal unless history shows prior signal\n"
        f"- If position is NONE and just_closed, consider re-entry cooldown\n"
    )
    return prompt


def _build_fact_section(ctx=None):
    '''Build FACT similar events section for the prompt.'''
    fact_events = ctx.get('fact_similar_events', [])
    perf = ctx.get('fact_performance_summary', {})
    if not fact_events:
        return ''
    lines = ['## FACT: Similar Historical Events']
    for evt in fact_events[:5]:
        lines.append(
            f"- [{evt.get('kind', '?')}] {evt.get('category', '?')}: "
            f"move_1h={evt.get('btc_move_1h', '?')}%, "
            f"move_4h={evt.get('btc_move_4h', '?')}%, "
            f"zscore={evt.get('vol_zscore', '?')}")
    if perf:
        lines.append("\nPerformance Summary:")
        lines.append(f"- Avg move 1h: {perf.get('avg_move_1h', 'N/A')}%")
        lines.append(f"- Avg move 4h: {perf.get('avg_move_4h', 'N/A')}%")
        lines.append(f"- Up/Down: {perf.get('up_count', 0)}/{perf.get('down_count', 0)}")
        lines.append(f"- Best 4h: {perf.get('best_4h', 'N/A')}%, Worst 4h: {perf.get('worst_4h', 'N/A')}%")
    return '\n'.join(lines)


def _build_decision_history_section(ctx=None):
    """Build Decision History section for the prompt (~300-400 chars)."""
    if not ctx:
        return ''
    dh = ctx.get('decision_history', {})
    if not dh:
        return ''
    _log(f'decision_history injected: keys={sorted(dh.keys())}')
    lines = ['## Decision History']
    recent = dh.get('recent_decisions', [])
    if recent:
        for d in recent[:3]:
            lines.append(f"- {d.get('ts','?')}: {d.get('action','?')} "
                         f"(pos={d.get('position_side','none')}, "
                         f"reason={d.get('reason','?')[:60]})")
    else:
        lines.append('- No prior decisions')
    last_ev = dh.get('last_event', {})
    if last_ev:
        lines.append(f"- Last event: {last_ev.get('action','?')} "
                     f"({last_ev.get('call_type','?')}, {last_ev.get('ts','?')})")
    if dh.get('just_closed'):
        lines.append(f"- JUST CLOSED (was {dh.get('closed_direction','?')})")
    if dh.get('hold_suppress_active'):
        lines.append('- HOLD suppress lock active')
    return '\n'.join(lines) + '\n\n'


def _build_snapshot_section(snapshot=None):
    """Build snapshot data section for the prompt."""
    if not snapshot:
        return ''
    returns = snapshot.get('returns', {})
    lines = [
        '## Real-time Snapshot',
        f"- Snapshot TS: {snapshot.get('snapshot_ts', 'N/A')}",
        f"- Price: ${snapshot.get('price', 0):,.1f}",
        f"- BB: upper=${snapshot.get('bb_upper', 0):,.0f} mid=${snapshot.get('bb_mid', 0):,.0f} lower=${snapshot.get('bb_lower', 0):,.0f}",
        f"- Ichimoku: kijun=${snapshot.get('kijun', 0):,.0f} cloud={snapshot.get('cloud_position', 'N/A')}",
        f"- RSI(14): {snapshot.get('rsi_14', 'N/A')}",
        f"- ATR(14): {snapshot.get('atr_14', 'N/A')}",
        f"- Volume ratio: {snapshot.get('vol_ratio', 'N/A')}",
        f"- Returns: 1m={returns.get('ret_1m', 'N/A')}% 5m={returns.get('ret_5m', 'N/A')}% 15m={returns.get('ret_15m', 'N/A')}%",
    ]
    return '\n'.join(lines) + '\n\n'


def event_trigger_analysis(context_packet=None, snapshot=None, event_result=None):
    """Event-trigger Claude analysis. gate=event_trigger (AUTO) or emergency (EMERGENCY)."""
    # 1. Snapshot validation
    import market_snapshot
    valid, reason = market_snapshot.validate_snapshot(snapshot)
    if not valid:
        return {**ABORT_RESPONSE, 'abort_reason': reason}

    ctx = context_packet or {}
    import claude_gate

    # 2. Build prompt with snapshot data
    ctx_compact = claude_gate.compact_context(ctx)
    prompt = _build_prompt(ctx_compact, snapshot=snapshot)

    # Add event trigger info to prompt
    if event_result:
        trigger_lines = []
        for t in (event_result.triggers if hasattr(event_result, 'triggers') else []):
            trigger_lines.append(f"  - {t.get('type', '?')}: value={t.get('value', '?')} "
                                 f"threshold={t.get('threshold', '?')}")
        if trigger_lines:
            prompt += "\n## Event Triggers\n" + '\n'.join(trigger_lines) + '\n'

    # 3. Gate selection: EMERGENCY → 'emergency', EVENT → 'event_trigger'
    er_mode = getattr(event_result, 'mode', 'EVENT') if event_result else 'EVENT'
    if er_mode == 'EMERGENCY':
        gate = 'emergency'
        call_type = 'EMERGENCY'
    else:
        gate = 'event_trigger'
        call_type = 'AUTO'

    cooldown_key = f'event_{er_mode.lower()}'
    gate_context = {
        'event_mode': er_mode,
        'event_hash': getattr(event_result, 'event_hash', None) if event_result else None,
        'trigger_type': ((getattr(event_result, 'triggers', None) or [])[0].get('type', '')
                         if event_result and getattr(event_result, 'triggers', None) else ''),
        'is_emergency': er_mode == 'EMERGENCY',
    }

    # 4. Call Claude
    result = claude_gate.call_claude(
        gate=gate, prompt=prompt, cooldown_key=cooldown_key,
        context=gate_context, max_tokens=800, call_type=call_type)

    if result.get('fallback_used'):
        _log(f'gate denied (event_trigger): {result.get("gate_reason", "unknown")}')
        return {**ABORT_RESPONSE, 'fallback_used': True, 'aborted': True,
                'api_latency_ms': 0,
                'gate_reason': result.get('gate_reason', ''),
                'call_type': call_type}

    parsed = _parse_response(result.get('text', ''))
    parsed['fallback_used'] = False
    parsed['api_latency_ms'] = result.get('api_latency_ms', 0)
    parsed['input_tokens'] = result.get('input_tokens', 0)
    parsed['output_tokens'] = result.get('output_tokens', 0)
    parsed['estimated_cost_usd'] = result.get('estimated_cost_usd', 0)
    parsed['gate_type'] = result.get('gate_type', gate)
    parsed['model'] = result.get('model', '')
    parsed['call_type'] = call_type
    return parsed


def event_trigger_analysis_mini(context_packet=None, snapshot=None, event_result=None):
    """EVENT analysis via GPT-4o-mini (cost fallback when Claude gate denied).

    Same prompt structure as event_trigger_analysis, same response parsing.
    Cost: ~$0.0003/call vs ~$0.01/call for Claude.
    """
    import market_snapshot
    valid, reason = market_snapshot.validate_snapshot(snapshot)
    if not valid:
        return {**ABORT_RESPONSE, 'abort_reason': reason}

    ctx = context_packet or {}
    import claude_gate

    ctx_compact = claude_gate.compact_context(ctx)
    prompt = _build_prompt(ctx_compact, snapshot=snapshot)

    if event_result:
        trigger_lines = []
        for t in (event_result.triggers if hasattr(event_result, 'triggers') else []):
            trigger_lines.append(f"  - {t.get('type', '?')}: value={t.get('value', '?')} "
                                 f"threshold={t.get('threshold', '?')}")
        if trigger_lines:
            prompt += "\n## Event Triggers\n" + '\n'.join(trigger_lines) + '\n'

    er_mode = getattr(event_result, 'mode', 'EVENT') if event_result else 'EVENT'

    import os
    import time as _time
    api_key = os.getenv('OPENAI_API_KEY', '')
    if not api_key:
        _log('GPT-mini fallback: no OPENAI_API_KEY')
        return {**FALLBACK_RESPONSE, 'fallback_used': True, 'model': 'gpt-4o-mini',
                'call_type': 'AUTO_MINI'}

    start_ms = int(_time.time() * 1000)
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key, timeout=15)
        resp = client.chat.completions.create(
            model='gpt-4o-mini',
            messages=[{'role': 'user', 'content': prompt}],
            max_tokens=500,
            temperature=0.0,
        )
        elapsed_ms = int(_time.time() * 1000) - start_ms
        text = resp.choices[0].message.content.strip() if resp.choices else ''
        input_tokens = resp.usage.prompt_tokens if resp.usage else 0
        output_tokens = resp.usage.completion_tokens if resp.usage else 0

        _log(f'GPT-mini OK: in={input_tokens} out={output_tokens} '
             f'latency={elapsed_ms}ms')

        parsed = _parse_response(text)
        parsed['fallback_used'] = False
        parsed['api_latency_ms'] = elapsed_ms
        parsed['input_tokens'] = input_tokens
        parsed['output_tokens'] = output_tokens
        parsed['estimated_cost_usd'] = (input_tokens * 0.15 + output_tokens * 0.6) / 1_000_000
        parsed['gate_type'] = 'event_trigger_mini'
        parsed['model'] = 'gpt-4o-mini'
        parsed['call_type'] = 'AUTO_MINI'
        return parsed

    except Exception as e:
        elapsed_ms = int(_time.time() * 1000) - start_ms
        _log(f'GPT-mini error: {e} latency={elapsed_ms}ms')
        return {**FALLBACK_RESPONSE, 'fallback_used': True,
                'model': 'gpt-4o-mini', 'api_latency_ms': elapsed_ms,
                'call_type': 'AUTO_MINI'}


VALID_ACTIONS = {'HOLD', 'OPEN_LONG', 'OPEN_SHORT', 'REDUCE', 'CLOSE', 'REVERSE'}

# ── Event Decision Mode ──────────────────────────────────

VALID_EVENT_ACTIONS = frozenset({
    'HOLD', 'RISK_OFF_REDUCE', 'HARD_EXIT', 'REVERSE', 'HEDGE', 'FREEZE_NEW_ENTRY',
})

VALID_EVENT_CLASSES = frozenset({
    'FLASH_DROP', 'FLASH_PUMP', 'BREAKOUT', 'FAKEOUT', 'LIQUIDITY_STRESS',
})

EVENT_DECISION_FALLBACK = {
    'event_class': 'FAKEOUT',
    'confidence': 0.0,
    'action': 'HOLD',
    'params': {},
    'reasoning_short': 'FALLBACK — Claude parse failure',
    'safety_checks': {
        'orphan_orders_cleanup_required': False,
        'stop_order_required': False,
        'reverse_allowed': False,
    },
    'fallback_used': True,
}


def _parse_response(raw=None):
    """Parse Claude's JSON response. Falls back on parse failure."""
    if not raw:
        return dict(FALLBACK_RESPONSE)
    text = raw.strip()
    # Strip markdown backticks (handles ```json with/without newline)
    text = re.sub(r'^\s*```(?:json)?\s*\n?', '', text)
    text = re.sub(r'\n?\s*```\s*$', '', text)
    text = text.strip()
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        _log(f'JSON parse failed: {text[:200]}')
        return dict(FALLBACK_RESPONSE)

    action = data.get('action', '').upper()
    if action not in VALID_ACTIONS:
        # backward compat: try recommended_action
        action = data.get('recommended_action', '').upper()
        if action not in VALID_ACTIONS:
            _log(f'invalid action: {action}')
            return dict(FALLBACK_RESPONSE)

    try:
        reduce_pct = max(0, min(100, int(data.get('reduce_pct', 0))))
        target_stage = max(0, min(7, int(data.get('target_stage', 1))))
        confidence = max(0.0, min(1.0, float(data.get('confidence', 0.5))))
        ttl_seconds = max(30, min(300, int(data.get('ttl_seconds', 60))))
    except (ValueError, TypeError):
        _log('field parse error, using defaults')
        reduce_pct, target_stage, confidence, ttl_seconds = 0, 1, 0.5, 60

    return {
        'action': action,
        'recommended_action': action,  # backward compat
        'reduce_pct': reduce_pct,
        'target_stage': target_stage,
        'reason_code': str(data.get('reason_code', ''))[:200],
        'reason_bullets': [str(data.get('reason_code', '') or data.get('reason_bullets', [''])[0] if isinstance(data.get('reason_bullets'), list) else data.get('reason_code', ''))],  # backward compat
        'confidence': confidence,
        'ttl_seconds': ttl_seconds,
    }


def score_change_analysis(context_packet=None):
    """Analyze score change via Claude gate (pre_action). Falls back on denial."""
    ctx = context_packet or {}
    import claude_gate
    ctx_compact = claude_gate.compact_context(ctx)
    prompt = _build_prompt(ctx_compact)

    action = ctx.get('candidate_action', '')
    sl_dist = ctx.get('sl_dist_pct')
    cooldown_key = f'score_{action or "change"}'
    gate_context = {
        'candidate_action': action,
        'sl_dist_pct': sl_dist,
    }

    result = claude_gate.call_claude(
        gate='pre_action', prompt=prompt, cooldown_key=cooldown_key,
        context=gate_context, max_tokens=600, call_type='AUTO')

    if result.get('fallback_used'):
        _log(f'gate denied (score_change): {result.get("gate_reason", "unknown")}')
        return {**FALLBACK_RESPONSE, 'fallback_used': True, 'api_latency_ms': 0,
                'gate_reason': result.get('gate_reason', ''),
                'call_type': 'AUTO'}

    parsed = _parse_response(result.get('text', ''))
    parsed['fallback_used'] = False
    parsed['api_latency_ms'] = result.get('api_latency_ms', 0)
    parsed['input_tokens'] = result.get('input_tokens', 0)
    parsed['output_tokens'] = result.get('output_tokens', 0)
    parsed['estimated_cost_usd'] = result.get('estimated_cost_usd', 0)
    parsed['gate_type'] = result.get('gate_type', 'pre_action')
    parsed['model'] = result.get('model', '')
    return parsed


# ── Event Decision Mode: Claude direct decision ─────────

def _build_event_decision_prompt(bundle, snapshot):
    """Build Claude prompt for EVENT_DECISION mode."""
    pos = bundle.get('position', {})
    orders = bundle.get('orders', {})
    mctx = bundle.get('mctx', {})
    micro = bundle.get('microstructure', {})
    recent = bundle.get('recent_execution', [])
    health = bundle.get('system_health', {})
    risk_cfg = bundle.get('risk_config', {})
    triggers = bundle.get('triggers', [])

    lines = [
        'Role: Crypto trading risk analyst with EXECUTION AUTHORITY.',
        'You must respond with STRICT JSON only — no markdown, no explanation.',
        '',
        '── Current Position ──',
        f'  side: {pos.get("side", "NONE")}',
        f'  qty: {pos.get("qty", 0)}',
        f'  entry_price: {pos.get("entry_price", 0)}',
        f'  mark_price: {pos.get("mark_price", 0)}',
        f'  uPnL: {pos.get("upnl", 0)}',
        f'  uPnL%: {pos.get("upnl_pct", 0)}',
        f'  leverage: {pos.get("leverage", 0)}',
        f'  liq_price: {pos.get("liq_price", 0)}',
        '',
        '── Active Orders ──',
        f'  active_count: {orders.get("active_count", 0)}',
        f'  conditional_count: {orders.get("conditional_count", 0)}',
        f'  has_orphan_risk: {orders.get("has_orphan_risk", False)}',
        '',
        '── Trigger Events ──',
    ]
    for t in triggers:
        lines.append(f'  - {t.get("type", "?")}: value={t.get("value", "?")} '
                      f'threshold={t.get("threshold", "?")}')

    lines += [
        '',
        '── Market Context ──',
        f'  regime: {mctx.get("regime", "?")}',
        f'  drift: {mctx.get("drift", 0)}',
        f'  adx: {mctx.get("adx", 0)}',
        f'  atr_pct: {mctx.get("atr_pct", 0)}',
        f'  impulse: {mctx.get("impulse", 0)}',
        f'  volume_z: {mctx.get("volume_z", 0)}',
        f'  range_pos: {mctx.get("range_pos", 0)}',
        f'  breakout: {mctx.get("breakout", False)}',
        f'  ret_1m: {mctx.get("ret_1m", 0)}%',
        f'  ret_5m: {mctx.get("ret_5m", 0)}%',
        f'  ret_15m: {mctx.get("ret_15m", 0)}%',
        '',
        '── Microstructure ──',
        f'  spread_ok: {micro.get("spread_ok", True)}',
        f'  liquidity_ok: {micro.get("liquidity_ok", True)}',
        f'  orderbook_imbalance: {micro.get("orderbook_imbalance", 0)}',
        f'  slippage_est: {micro.get("slippage_est", 0)}',
        '',
        '── Recent Execution (last 5) ──',
    ]
    for trade in recent[:5]:
        lines.append(f'  - {trade.get("action", "?")} {trade.get("side", "?")} '
                      f'pnl={trade.get("pnl", "?")} reason={trade.get("reason", "?")}')

    lines += [
        '',
        '── System Health ──',
        f'  gate_status: {health.get("gate_status", "OK")}',
        f'  down_services: {health.get("down_services", [])}',
        f'  latency_ms: {health.get("latency_ms", 0)}',
        '',
        '── Risk Config ──',
        f'  allowed_actions: {list(VALID_EVENT_ACTIONS)}',
        f'  max_reduce_ratio: 0.70',
        f'  max_reverse_ratio: 0.30',
        f'  max_hedge_ratio: 0.30',
        f'  max_freeze_minutes: 60',
        '',
        '── Instructions ──',
        'Analyze the trigger events and decide the optimal action.',
        'HOLD = do nothing, keep position as-is',
        'RISK_OFF_REDUCE = reduce position by reduce_ratio (0.0-1.0)',
        'HARD_EXIT = close entire position immediately',
        'REVERSE = close + open opposite direction (only if reverse_allowed)',
        'HEDGE = open hedge position (opposite direction, hedge_size_ratio)',
        'FREEZE_NEW_ENTRY = block new entries for freeze_minutes',
        '',
        'Respond with STRICT JSON only:',
        '{',
        '  "event_class": "FLASH_DROP|FLASH_PUMP|BREAKOUT|FAKEOUT|LIQUIDITY_STRESS",',
        '  "confidence": 0.0-1.0,',
        '  "action": "HOLD|RISK_OFF_REDUCE|HARD_EXIT|REVERSE|HEDGE|FREEZE_NEW_ENTRY",',
        '  "params": {',
        '    "reduce_ratio": 0.0-1.0,',
        '    "freeze_minutes": 0-60,',
        '    "new_sl_type": "EXCHANGE_HARD_SL|ATR_TRAIL|BREAKEVEN",',
        '    "new_sl_value": 0,',
        '    "reverse_size_ratio": 0.0-1.0,',
        '    "hedge_size_ratio": 0.0-1.0',
        '  },',
        '  "reasoning_short": "...",',
        '  "safety_checks": {',
        '    "orphan_orders_cleanup_required": true/false,',
        '    "stop_order_required": true/false,',
        '    "reverse_allowed": true/false',
        '  }',
        '}',
    ]
    return '\n'.join(lines)


def _parse_event_decision_response(text):
    """Parse Claude EVENT_DECISION JSON response. Returns HOLD fallback on failure."""
    if not text:
        return dict(EVENT_DECISION_FALLBACK)

    raw = text.strip()
    # Strip markdown
    raw = re.sub(r'^\s*```(?:json)?\s*\n?', '', raw)
    raw = re.sub(r'\n?\s*```\s*$', '', raw)
    raw = raw.strip()

    data = None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        # regex fallback: find first { ... }
        m = re.search(r'\{[\s\S]*\}', raw)
        if m:
            try:
                data = json.loads(m.group())
            except json.JSONDecodeError:
                pass

    if not data or not isinstance(data, dict):
        _log(f'event_decision parse failed: {raw[:200]}')
        return dict(EVENT_DECISION_FALLBACK)

    # Validate action
    action = (data.get('action') or '').upper()
    if action not in VALID_EVENT_ACTIONS:
        _log(f'event_decision invalid action: {action}')
        return dict(EVENT_DECISION_FALLBACK)

    # Validate event_class
    event_class = (data.get('event_class') or '').upper()
    if event_class not in VALID_EVENT_CLASSES:
        event_class = 'FAKEOUT'

    # Clamp params
    params = data.get('params', {})
    if not isinstance(params, dict):
        params = {}
    try:
        params['reduce_ratio'] = max(0.0, min(1.0, float(params.get('reduce_ratio', 0))))
        params['freeze_minutes'] = max(0, min(60, int(params.get('freeze_minutes', 0))))
        params['reverse_size_ratio'] = max(0.0, min(1.0, float(params.get('reverse_size_ratio', 0))))
        params['hedge_size_ratio'] = max(0.0, min(1.0, float(params.get('hedge_size_ratio', 0))))
        params['new_sl_value'] = float(params.get('new_sl_value', 0))
        params['new_sl_type'] = str(params.get('new_sl_type', ''))
    except (ValueError, TypeError):
        params = {'reduce_ratio': 0, 'freeze_minutes': 0,
                  'reverse_size_ratio': 0, 'hedge_size_ratio': 0,
                  'new_sl_value': 0, 'new_sl_type': ''}

    # Confidence
    try:
        confidence = max(0.0, min(1.0, float(data.get('confidence', 0.5))))
    except (ValueError, TypeError):
        confidence = 0.5

    # Safety checks
    safety = data.get('safety_checks', {})
    if not isinstance(safety, dict):
        safety = {}

    return {
        'event_class': event_class,
        'confidence': confidence,
        'action': action,
        'params': params,
        'reasoning_short': str(data.get('reasoning_short', ''))[:500],
        'safety_checks': {
            'orphan_orders_cleanup_required': bool(safety.get('orphan_orders_cleanup_required', False)),
            'stop_order_required': bool(safety.get('stop_order_required', False)),
            'reverse_allowed': bool(safety.get('reverse_allowed', False)),
        },
        'fallback_used': False,
    }


def event_decision_analysis(context_bundle, snapshot, event_result):
    """EVENT_DECISION Claude analysis — Claude decides action directly.

    gate: event_trigger, call_type: AUTO_EMERGENCY
    Returns parsed decision dict (action=HOLD on failure).
    """
    import time as _time

    # Snapshot validation
    import market_snapshot
    valid, reason = market_snapshot.validate_snapshot(snapshot)
    if not valid:
        _log(f'event_decision aborted: {reason}')
        return {**EVENT_DECISION_FALLBACK, 'abort_reason': reason}

    import claude_gate

    # Build enriched prompt
    prompt = _build_event_decision_prompt(context_bundle, snapshot)

    er_mode = getattr(event_result, 'mode', 'EVENT_DECISION') if event_result else 'EVENT_DECISION'
    cooldown_key = 'event_decision'
    gate_context = {
        'event_mode': er_mode,
        'event_hash': getattr(event_result, 'event_hash', None) if event_result else None,
        'trigger_type': ((getattr(event_result, 'triggers', None) or [])[0].get('type', '')
                         if event_result and getattr(event_result, 'triggers', None) else ''),
        'is_emergency': False,
    }

    start_ms = int(_time.time() * 1000)
    result = claude_gate.call_claude(
        gate='event_trigger', prompt=prompt, cooldown_key=cooldown_key,
        context=gate_context, max_tokens=800, call_type='AUTO_EMERGENCY')
    elapsed_ms = int(_time.time() * 1000) - start_ms

    if result.get('fallback_used'):
        _log(f'event_decision gate denied: {result.get("gate_reason", "unknown")}')
        return {**EVENT_DECISION_FALLBACK, 'fallback_used': True,
                'gate_reason': result.get('gate_reason', ''),
                'api_latency_ms': elapsed_ms,
                'call_type': 'AUTO_EMERGENCY'}

    parsed = _parse_event_decision_response(result.get('text', ''))
    parsed['api_latency_ms'] = elapsed_ms
    parsed['input_tokens'] = result.get('input_tokens', 0)
    parsed['output_tokens'] = result.get('output_tokens', 0)
    parsed['estimated_cost_usd'] = result.get('estimated_cost_usd', 0)
    parsed['gate_type'] = result.get('gate_type', 'event_trigger')
    parsed['model'] = result.get('model', '')
    parsed['call_type'] = 'AUTO_EMERGENCY'
    return parsed


if __name__ == '__main__':
    test_ctx = {
        'position': {
            'side': 'long',
            'qty': 0.003,
            'entry_price': 97000,
            'upnl': -5},
        'price': 96500,
        'trigger': {
            'type': 'price_move',
            'detail': {
                'move_pct': -0.5,
                'atr_multiple': 2.8}},
        'indicators': {
            'atr': 150,
            'rsi': 35,
            'kijun': 96800,
            'poc': 97000,
            'vah': 97200,
            'val': 96600},
        'scores': {
            'long_score': 45,
            'short_score': 62},
        'news': [],
        'funding_rate': 0.0001}
    result = emergency_analysis(test_ctx)
    print(json.dumps(result, ensure_ascii=False, indent=2))
