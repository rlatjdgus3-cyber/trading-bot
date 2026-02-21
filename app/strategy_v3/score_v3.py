"""
strategy_v3.score_v3 — Regime-based score modifier.

Applies POC penalty, edge bonus, drift alignment, chase blocking
on top of the existing score_engine total_score.

FAIL-OPEN: any error → modifier=0, entry_blocked=False.
"""

from strategy_v3 import config_v3, safe_float

LOG_PREFIX = '[score_v3]'

MIN_CONFIDENCE = 35  # match autopilot_daemon


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _clamp(val, lo, hi):
    return max(lo, min(hi, val))


def _compute_static_range_modifier(total_score, features, price, cfg):
    """Compute score modifier for STATIC_RANGE regime."""
    modifier = 0.0
    blocked = False
    block_reason = ''
    reasoning = []

    atr_pct = safe_float(features.get('atr_pct'), 0.005)
    atr_val = atr_pct * price

    vah = safe_float(features.get('vah'))
    val = safe_float(features.get('val'))
    poc = safe_float(features.get('poc'))
    range_position = features.get('range_position')

    if not vah or not val or vah <= val:
        reasoning.append('VA data unavailable — no modifier')
        return modifier, blocked, block_reason, reasoning

    va_width = vah - val

    # 1. POC noise band
    if poc > 0:
        poc_band = max(
            cfg['poc_band_atr_mult'] * atr_val,
            cfg['poc_band_va_mult'] * va_width,
        )
        dist_from_poc = abs(price - poc)
        if dist_from_poc <= poc_band:
            modifier -= cfg['poc_zone_penalty']
            reasoning.append(f'POC noise zone (±{poc_band:.0f}): -{cfg["poc_zone_penalty"]}')
            # Check if total would fall below confidence after penalty
            adjusted = abs(total_score + modifier)
            if adjusted < MIN_CONFIDENCE:
                blocked = True
                block_reason = f'POC noise zone — conf {adjusted:.0f} < {MIN_CONFIDENCE}'

    # 2. Edge bonuses (VAL / VAH proximity)
    if range_position is not None:
        if range_position <= 0.20:
            # Near VAL → LONG bonus (positive modifier)
            modifier += cfg['edge_score_bonus']
            reasoning.append(f'VAL edge LONG bonus: +{cfg["edge_score_bonus"]}')
        elif range_position >= 0.80:
            # Near VAH → SHORT bonus (negative modifier pushes score toward SHORT)
            modifier -= cfg['edge_score_bonus']
            reasoning.append(f'VAH edge SHORT bonus: -{cfg["edge_score_bonus"]}')

    # 3. Edge overshoot detection (outside VA)
    if range_position is not None and (range_position < 0 or range_position > 1.0):
        if range_position < 0:
            nearest_edge = val
        else:
            nearest_edge = vah
        overshoot_dist = abs(price - nearest_edge) / atr_val if atr_val > 0 else 0

        if overshoot_dist > cfg['edge_overshoot_atr_mult']:
            blocked = True
            block_reason = f'edge overshoot chase block ({overshoot_dist:.1f} ATR)'
            reasoning.append(f'edge overshoot {overshoot_dist:.1f} ATR > {cfg["edge_overshoot_atr_mult"]}')
        elif overshoot_dist > 0:
            penalty = cfg['edge_overshoot_penalty']
            # Apply penalty in the dominant direction (reduce conviction)
            if total_score >= 0:
                modifier -= penalty
            else:
                modifier += penalty
            reasoning.append(f'edge overshoot penalty: {penalty} ({overshoot_dist:.1f} ATR)')

    return modifier, blocked, block_reason, reasoning


def _compute_drift_modifier(total_score, features, price, regime_class, cfg):
    """Compute score modifier for DRIFT_UP / DRIFT_DOWN regime."""
    modifier = 0.0
    blocked = False
    block_reason = ''
    reasoning = []

    dominant = 'LONG' if total_score >= 0 else 'SHORT'
    atr_pct = safe_float(features.get('atr_pct'), 0.005)
    atr_val = atr_pct * price
    poc = safe_float(features.get('poc'))

    # 1. Drift alignment check
    if regime_class == 'DRIFT_UP' and dominant == 'LONG':
        modifier += cfg['drift_aligned_bonus']
        reasoning.append(f'drift UP aligned LONG: +{cfg["drift_aligned_bonus"]}')
    elif regime_class == 'DRIFT_DOWN' and dominant == 'SHORT':
        modifier -= cfg['drift_aligned_bonus']  # negative = stronger SHORT
        reasoning.append(f'drift DOWN aligned SHORT: -{cfg["drift_aligned_bonus"]}')

    # 2. Drift counter-direction penalty
    if regime_class == 'DRIFT_UP' and dominant == 'SHORT':
        # Push score toward LONG (reduce SHORT conviction)
        modifier += cfg['drift_counter_penalty']
        reasoning.append(f'drift UP vs SHORT — counter penalty: +{cfg["drift_counter_penalty"]}')
    elif regime_class == 'DRIFT_DOWN' and dominant == 'LONG':
        # Push score toward SHORT (reduce LONG conviction)
        modifier -= cfg['drift_counter_penalty']
        reasoning.append(f'drift DOWN vs LONG — counter penalty: -{cfg["drift_counter_penalty"]}')

    # 3. POC retest bonus (optional)
    if poc > 0 and atr_val > 0:
        tolerance = cfg['poc_support_tolerance_atr'] * atr_val
        if abs(price - poc) <= tolerance:
            # Small bonus for entering near POC (pullback entry)
            if regime_class == 'DRIFT_UP':
                modifier += 5
            else:
                modifier -= 5
            reasoning.append('POC level retest bonus: ±5')

    return modifier, blocked, block_reason, reasoning


def _check_breakout_retest(features, breakout_dir, breakout_level, cfg):
    """Check if a breakout retest has occurred.

    Simplified check: price must have returned close to breakout_level
    after the initial breakout (within 1 ATR of the level).
    """
    atr_pct = safe_float(features.get('atr_pct'), 0.005)
    price = safe_float(features.get('price'))
    if not price or not breakout_level:
        return True  # FAIL-OPEN

    atr_val = atr_pct * price
    if atr_val <= 0:
        return True  # FAIL-OPEN

    dist = abs(price - breakout_level)
    return dist <= atr_val


def _is_regime_risk_v3_enabled():
    """Check if ff_regime_risk_v3 flag is ON."""
    try:
        import feature_flags
        return feature_flags.is_enabled('ff_regime_risk_v3')
    except Exception:
        return False


def _compute_breakout_modifier(total_score, features, price, cfg, v3_regime=None):
    """Compute score modifier for BREAKOUT regime."""
    modifier = 0.0
    blocked = False
    block_reason = ''
    reasoning = []

    atr_pct = safe_float(features.get('atr_pct'), 0.005)
    atr_val = atr_pct * price
    vah = safe_float(features.get('vah'))
    val = safe_float(features.get('val'))

    if not vah or not val or vah <= val or atr_val <= 0:
        reasoning.append('VA/ATR data unavailable — no modifier')
        return modifier, blocked, block_reason, reasoning

    pad = cfg['breakout_pad_atr_mult'] * atr_val

    # 1. Determine breakout direction
    breakout_dir = None
    if price > vah + pad:
        breakout_dir = 'UP'
    elif price < val - pad:
        breakout_dir = 'DOWN'

    if breakout_dir is None:
        reasoning.append('no breakout confirmed (price within VA+pad)')
        return modifier, blocked, block_reason, reasoning

    # 2. Breakout trend bonus
    if breakout_dir == 'UP':
        modifier += cfg['breakout_trend_bonus']
        reasoning.append(f'breakout UP trend bonus: +{cfg["breakout_trend_bonus"]}')
        breakout_level = vah
    else:
        modifier -= cfg['breakout_trend_bonus']
        reasoning.append(f'breakout DOWN trend bonus: -{cfg["breakout_trend_bonus"]}')
        breakout_level = val

    # 2b. Reverse direction hard block (ff_regime_risk_v3 only)
    if _is_regime_risk_v3_enabled() and cfg.get('breakout_reverse_block', True):
        dominant = 'LONG' if total_score >= 0 else 'SHORT'
        if breakout_dir == 'UP' and dominant == 'SHORT':
            blocked = True
            block_reason = 'BREAKOUT UP + SHORT reverse block'
            reasoning.append('reverse block: BREAKOUT UP vs SHORT')
        elif breakout_dir == 'DOWN' and dominant == 'LONG':
            blocked = True
            block_reason = 'BREAKOUT DOWN + LONG reverse block'
            reasoning.append('reverse block: BREAKOUT DOWN vs LONG')

    # 2c. Impulse chase threshold block (ff_regime_risk_v3 only)
    if _is_regime_risk_v3_enabled() and not blocked:
        impulse = safe_float(features.get('impulse'))
        impulse_threshold = cfg.get('impulse_chase_threshold', 1.5)
        if impulse > impulse_threshold:
            blocked = True
            block_reason = f'impulse chase block: {impulse:.2f} > {impulse_threshold}'
            reasoning.append(f'impulse chase: {impulse:.2f} > {impulse_threshold}')

    # 3. Retest entry mode
    if cfg['retest_entry_enabled'] and not blocked:
        if not _check_breakout_retest(features, breakout_dir, breakout_level, cfg):
            blocked = True
            block_reason = 'BREAKOUT retest pending'
            reasoning.append('retest not confirmed — entry blocked')

    # 4. Chase distance penalty
    dist_from_level = abs(price - breakout_level)
    dist_atr = dist_from_level / atr_val if atr_val > 0 else 0

    if dist_atr > cfg['chase_ban_atr_mult']:
        blocked = True
        block_reason = f'chase ban: {dist_atr:.1f} ATR from breakout level'
        reasoning.append(f'chase ban: {dist_atr:.1f} ATR > {cfg["chase_ban_atr_mult"]}')
    elif dist_atr > cfg['chase_ban_atr_mult'] * 0.5:
        # Proportional penalty
        ratio = dist_atr / cfg['chase_ban_atr_mult']
        penalty = cfg['chase_score_penalty'] * ratio
        # Apply in direction that reduces conviction
        if total_score >= 0:
            modifier -= penalty
        else:
            modifier += penalty
        reasoning.append(f'chase distance penalty: {penalty:.0f} ({dist_atr:.1f} ATR)')

    return modifier, blocked, block_reason, reasoning


def compute_modifier(total_score, features, v3_regime, price):
    """Compute V3 score modifier based on regime classification.

    Args:
        total_score: float (-100 to +100) from score_engine
        features: dict from build_feature_snapshot()
        v3_regime: dict from regime_v3.classify()
        price: current price

    Returns:
        dict with keys:
            modifier: float (-50 to +50)
            entry_blocked: bool
            block_reason: str
            reasoning: list[str]
    """
    try:
        if not features or not v3_regime or not price or price <= 0:
            return {
                'modifier': 0,
                'entry_blocked': False,
                'block_reason': '',
                'reasoning': ['no data — passthrough'],
            }

        cfg = config_v3.get_all()
        regime_class = v3_regime.get('regime_class', 'STATIC_RANGE')

        if regime_class == 'STATIC_RANGE':
            modifier, blocked, block_reason, reasoning = _compute_static_range_modifier(
                total_score, features, price, cfg)
        elif regime_class in ('DRIFT_UP', 'DRIFT_DOWN'):
            modifier, blocked, block_reason, reasoning = _compute_drift_modifier(
                total_score, features, price, regime_class, cfg)
        elif regime_class == 'BREAKOUT':
            modifier, blocked, block_reason, reasoning = _compute_breakout_modifier(
                total_score, features, price, cfg, v3_regime=v3_regime)
        else:
            modifier, blocked, block_reason, reasoning = 0, False, '', ['unknown regime — passthrough']

        # Clamp modifier
        modifier = _clamp(modifier, -50, 50)

        return {
            'modifier': modifier,
            'entry_blocked': blocked,
            'block_reason': block_reason,
            'reasoning': reasoning,
        }
    except Exception as e:
        _log(f'compute_modifier FAIL-OPEN: {e}')
        return {
            'modifier': 0,
            'entry_blocked': False,
            'block_reason': '',
            'reasoning': [f'FAIL-OPEN: {e}'],
        }
