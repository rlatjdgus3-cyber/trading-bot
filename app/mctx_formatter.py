"""
mctx_formatter.py — Format mctx (market context) data for /mctx command.

Provides one-line summary + detail block from feature snapshot + regime context.
"""


def format_mctx(features, regime_ctx):
    """Format mctx data for display.

    Args:
        features: dict from build_feature_snapshot()
        regime_ctx: dict from regime_reader.get_current_regime()

    Returns: formatted string
    """
    regime = regime_ctx.get('regime', 'UNKNOWN') if regime_ctx.get('available') else 'UNKNOWN'
    conf = regime_ctx.get('confidence', 0)
    drift_dir = features.get('drift_direction', 'NONE')

    # One-line summary
    spread_ok = features.get('spread_ok', True)
    liquidity_ok = features.get('liquidity_ok', True)
    health = 'OK' if (spread_ok and liquidity_ok) else 'WARN'
    summary = f'{regime} | drift={drift_dir} | health={health}'

    lines = ['[MCTX] 시장 환경 상태']
    lines.append('━━━━━━━━━━━━━━━━━━━━━━━━')
    lines.append(f'  요약: {summary}')
    lines.append('')

    # Regime section
    lines.append(f'  레짐: {regime} (confidence={conf})')
    adx = regime_ctx.get('adx_14') or features.get('adx')
    lines.append(f'  ADX: {adx:.1f}' if adx is not None else '  ADX: N/A')
    flow = regime_ctx.get('flow_bias', 0)
    lines.append(f'  flow_bias: {flow:+.1f}')
    pvs = regime_ctx.get('price_vs_va', '?')
    lines.append(f'  price_vs_VA: {pvs}')
    shock = regime_ctx.get('shock_type')
    if shock:
        lines.append(f'  shock: {shock}')
    bo = regime_ctx.get('breakout_confirmed', False)
    lines.append(f'  breakout_confirmed: {"YES" if bo else "NO"}')
    trans = regime_ctx.get('in_transition', False)
    if trans:
        lines.append(f'  전환 상태: 쿨다운 중')
    stale = regime_ctx.get('stale', False)
    if stale:
        lines.append(f'  !! 데이터 stale (>5분)')
    lines.append('')

    # VA/POC section
    vah = features.get('vah') or regime_ctx.get('vah')
    val = features.get('val') or regime_ctx.get('val')
    poc = features.get('poc') or regime_ctx.get('poc')
    if vah and val:
        va_line = f'  VAH: ${vah:,.0f} / VAL: ${val:,.0f}'
        if poc:
            va_line += f' / POC: ${poc:,.0f}'
        lines.append(va_line)

    # New feature fields
    lines.append('')
    lines.append('  [Feature Snapshot]')
    vol_pct = features.get('vol_pct')
    lines.append(f'  vol_pct: {vol_pct:.4f}%' if vol_pct is not None else '  vol_pct: N/A')
    ts = features.get('trend_strength')
    lines.append(f'  trend_strength: {ts:.2f}' if ts is not None else '  trend_strength: N/A')
    rq = features.get('range_quality')
    lines.append(f'  range_quality: {rq:.2f}' if rq is not None else '  range_quality: N/A')
    lines.append(f'  spread_ok: {"YES" if spread_ok else "NO"}')
    lines.append(f'  liquidity_ok: {"YES" if liquidity_ok else "NO"}')
    lines.append(f'  drift: {drift_dir} (score={features.get("drift_score", 0):.4f})')

    rp = features.get('range_position')
    if rp is not None:
        lines.append(f'  range_position: {rp:.3f}')
    atr_pct = features.get('atr_pct')
    if atr_pct is not None:
        lines.append(f'  atr_pct: {atr_pct:.4f}')
    impulse = features.get('impulse')
    if impulse is not None:
        lines.append(f'  impulse: {impulse:.2f}')
    volume_z = features.get('volume_z')
    if volume_z is not None:
        lines.append(f'  volume_z: {volume_z:.2f}')

    return '\n'.join(lines)
