"""
claude_api.py — Claude AI emergency analysis wrapper.

Uses Anthropic API to analyze sudden market changes and recommend actions.
Model: claude-sonnet-4-20250514 (~$0.01/call)
Never raises exceptions — always returns a valid response dict.
"""
import os
import sys
import time
import json
sys.path.insert(0, '/root/trading-bot/app')
LOG_PREFIX = '[claude_api]'
FALLBACK_RESPONSE = {
    'risk_level': 'high',
    'recommended_action': 'REDUCE',
    'reduce_pct': 50,
    'confidence': 0.5,
    'reason_bullets': ['API 호출 실패 — 보수적 축소 적용'],
    'ttl_seconds': 120}


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def emergency_analysis(context_packet=None):
    """Analyze emergency via Claude gate. Falls back on denial."""
    ctx = context_packet or {}
    import claude_gate
    ctx_compact = claude_gate.compact_context(ctx)
    prompt = _build_prompt(ctx_compact)

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
                'gate_reason': result.get('gate_reason', '')}

    parsed = _parse_response(result.get('text', ''))
    parsed['fallback_used'] = False
    parsed['api_latency_ms'] = result.get('api_latency_ms', 0)
    parsed['input_tokens'] = result.get('input_tokens', 0)
    parsed['output_tokens'] = result.get('output_tokens', 0)
    parsed['estimated_cost_usd'] = result.get('estimated_cost_usd', 0)
    parsed['gate_type'] = result.get('gate_type', 'emergency')
    parsed['model'] = result.get('model', '')
    return parsed


def _build_prompt(ctx=None):
    pos = ctx.get('position', {})
    ind = ctx.get('indicators', {})
    trigger = ctx.get('trigger', {})
    scores = ctx.get('scores', {})
    unified = ctx.get('unified_score') or scores.get('unified') or {}

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
        f"## Instructions\n"
        f"Respond with ONLY this JSON (no markdown, no explanation):\n"
        f'{{\n'
        f'  "risk_level": "low|medium|high|critical",\n'
        f'  "recommended_action": "HOLD|REDUCE|CLOSE|REVERSE",\n'
        f'  "reduce_pct": 0,\n'
        f'  "confidence": 0.0,\n'
        f'  "reason_bullets": ["reason1", "reason2"],\n'
        f'  "ttl_seconds": 120\n'
        f'}}\n\n'
        f"Rules:\n"
        f"- HOLD: situation manageable, no action needed\n"
        f"- REDUCE: reduce position by reduce_pct% (10-75%)\n"
        f"- CLOSE: fully close position\n"
        f"- REVERSE: close and open opposite (only if very high confidence reversal)\n"
        f"- ttl_seconds: how long this recommendation is valid (60-300)\n"
        f"- confidence: 0.0 to 1.0\n"
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
        lines.append(f"\nPerformance Summary:")
        lines.append(f"- Avg move 1h: {perf.get('avg_move_1h', 'N/A')}%")
        lines.append(f"- Avg move 4h: {perf.get('avg_move_4h', 'N/A')}%")
        lines.append(f"- Up/Down: {perf.get('up_count', 0)}/{perf.get('down_count', 0)}")
        lines.append(f"- Best 4h: {perf.get('best_4h', 'N/A')}%, Worst 4h: {perf.get('worst_4h', 'N/A')}%")
    return '\n'.join(lines)


def _parse_response(raw=None):
    """Parse Claude's JSON response. Falls back on parse failure."""
    try:
        text = raw.strip()
        if text.startswith('```'):
            lines = text.split('\n')
            text = '\n'.join(lines[1:])
            if text.endswith('```'):
                text = text[:-3]
            text = text.strip()
        data = json.loads(text)
        valid_risk = ('low', 'medium', 'high', 'critical')
        valid_action = ('HOLD', 'REDUCE', 'CLOSE', 'REVERSE')
        risk = data.get('risk_level', 'high')
        if risk not in valid_risk:
            risk = 'high'
        action = data.get('recommended_action', 'REDUCE')
        if action not in valid_action:
            action = 'REDUCE'
        confidence = float(data.get('confidence', 0.5))
        confidence = max(0, min(1, confidence))
        reduce_pct = int(data.get('reduce_pct', 50))
        reduce_pct = max(0, min(100, reduce_pct))
        ttl = int(data.get('ttl_seconds', 120))
        ttl = max(60, min(300, ttl))
        bullets = data.get('reason_bullets', [])
        if not isinstance(bullets, list):
            bullets = [str(bullets)]
        return {
            'risk_level': risk,
            'recommended_action': action,
            'reduce_pct': reduce_pct,
            'confidence': confidence,
            'reason_bullets': bullets[:5],
            'ttl_seconds': ttl}
    except Exception as e:
        _log(f'parse error: {e}')
        return dict(FALLBACK_RESPONSE)


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
                'gate_reason': result.get('gate_reason', '')}

    parsed = _parse_response(result.get('text', ''))
    parsed['fallback_used'] = False
    parsed['api_latency_ms'] = result.get('api_latency_ms', 0)
    parsed['input_tokens'] = result.get('input_tokens', 0)
    parsed['output_tokens'] = result.get('output_tokens', 0)
    parsed['estimated_cost_usd'] = result.get('estimated_cost_usd', 0)
    parsed['gate_type'] = result.get('gate_type', 'pre_action')
    parsed['model'] = result.get('model', '')
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
