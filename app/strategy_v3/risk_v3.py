"""
strategy_v3.risk_v3 — Dynamic ATR-based SL, R-based TP, stage slice adjustment.

FAIL-OPEN: any error → returns default conservative params.
"""

from strategy_v3 import config_v3

LOG_PREFIX = '[risk_v3]'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _safe_float(val, default=0.0):
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _clamp(val, lo, hi):
    return max(lo, min(hi, val))


def _compute_health(features):
    spread_ok = features.get('spread_ok', True)
    liquidity_ok = features.get('liquidity_ok', True)
    if spread_ok is None:
        spread_ok = True
    if liquidity_ok is None:
        liquidity_ok = True
    return 'OK' if (spread_ok and liquidity_ok) else 'WARN'


def compute_risk(features, v3_regime):
    """Compute dynamic risk parameters based on V3 regime.

    Args:
        features: dict from build_feature_snapshot()
        v3_regime: dict from regime_v3.classify()

    Returns:
        dict with keys:
            sl_pct: float (dynamic SL percentage)
            tp_pct: float (dynamic TP percentage)
            stage_slice_mult: float (1.0 = normal, 0.5 = half)
            leverage_max: int
            reasoning: list[str]
    """
    try:
        cfg = config_v3.get_all()
        regime_class = v3_regime.get('regime_class', 'STATIC_RANGE') if v3_regime else 'STATIC_RANGE'
        reasoning = []

        # SL calculation
        atr_pct = _safe_float(features.get('atr_pct') if features else None, 0.005)
        raw_sl = cfg['atr_sl_mult'] * atr_pct
        sl_pct = _clamp(raw_sl, cfg['min_sl_pct'], cfg['max_sl_pct'])
        reasoning.append(f'SL: {cfg["atr_sl_mult"]}×ATR({atr_pct:.4f})={raw_sl:.4f} → clamped {sl_pct:.4f}')

        # TP calculation (R-based)
        tp_pct = sl_pct * cfg['tp_r_ratio']
        reasoning.append(f'TP: SL({sl_pct:.4f})×{cfg["tp_r_ratio"]}R = {tp_pct:.4f}')

        # Stage slice adjustment
        health = _compute_health(features) if features else 'OK'
        if regime_class == 'BREAKOUT' or health == 'WARN':
            stage_slice_mult = cfg['breakout_stage_slice_mult']
            reasoning.append(f'stage_slice: {stage_slice_mult} (BREAKOUT/WARN)')
        else:
            stage_slice_mult = 1.0

        # Leverage
        if regime_class == 'BREAKOUT':
            leverage_max = 8
        elif regime_class in ('STATIC_RANGE', 'DRIFT_UP', 'DRIFT_DOWN'):
            leverage_max = 5
        else:
            leverage_max = 5
        reasoning.append(f'leverage_max: {leverage_max}')

        return {
            'sl_pct': sl_pct,
            'tp_pct': tp_pct,
            'stage_slice_mult': stage_slice_mult,
            'leverage_max': leverage_max,
            'reasoning': reasoning,
        }
    except Exception as e:
        _log(f'compute_risk FAIL-OPEN: {e}')
        return {
            'sl_pct': 0.006,
            'tp_pct': 0.0072,
            'stage_slice_mult': 1.0,
            'leverage_max': 5,
            'reasoning': [f'FAIL-OPEN: {e}'],
        }
