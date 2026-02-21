"""
strategy_v3.risk_v3 — Dynamic ATR-based SL, R-based TP, stage slice adjustment.

FAIL-OPEN: any error → returns default conservative params.
"""

from strategy_v3 import config_v3, safe_float, compute_health

LOG_PREFIX = '[risk_v3]'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _clamp(val, lo, hi):
    return max(lo, min(hi, val))


def _is_regime_risk_v3_enabled():
    """Check if ff_regime_risk_v3 flag is ON."""
    try:
        import feature_flags
        return feature_flags.is_enabled('ff_regime_risk_v3')
    except Exception:
        return False


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
            max_stage: int (pyramid cap; only present when ff_regime_risk_v3 ON)
            reasoning: list[str]
    """
    try:
        cfg = config_v3.get_all()
        regime_class = v3_regime.get('regime_class', 'STATIC_RANGE') if v3_regime else 'STATIC_RANGE'
        reasoning = []

        atr_pct = safe_float(features.get('atr_pct') if features else None, 0.005)

        if _is_regime_risk_v3_enabled():
            # ── Regime-specific risk (ff_regime_risk_v3=ON) ──
            if regime_class == 'STATIC_RANGE':
                atr_mult = 1.5
                min_sl = cfg.get('static_range_min_sl', 0.005)
                max_sl = cfg.get('static_range_max_sl', 0.009)
                stage_slice_mult = 1.0
                leverage_max = 5
            elif regime_class in ('DRIFT_UP', 'DRIFT_DOWN'):
                atr_mult = 1.5
                min_sl = cfg.get('drift_min_sl', 0.006)
                max_sl = cfg.get('drift_max_sl', 0.012)
                stage_slice_mult = 1.0
                leverage_max = 5
            elif regime_class == 'BREAKOUT':
                atr_mult = cfg.get('breakout_atr_sl_mult', 2.0)
                min_sl = cfg.get('breakout_min_sl', 0.008)
                max_sl = cfg.get('breakout_max_sl', 0.02)
                stage_slice_mult = cfg['breakout_stage_slice_mult']
                leverage_max = 8
            else:
                atr_mult = 1.5
                min_sl = cfg['min_sl_pct']
                max_sl = cfg['max_sl_pct']
                stage_slice_mult = 1.0
                leverage_max = 5

            raw_sl = atr_mult * atr_pct
            sl_pct = _clamp(raw_sl, min_sl, max_sl)
            reasoning.append(f'SL[{regime_class}]: {atr_mult}×ATR({atr_pct:.4f})={raw_sl:.4f} → [{min_sl},{max_sl}] → {sl_pct:.4f}')

            # Impulse → extra stage_slice reduction
            impulse = safe_float(features.get('impulse') if features else None)
            impulse_threshold = cfg.get('impulse_chase_threshold', 1.5)
            if impulse > impulse_threshold:
                stage_slice_mult *= 0.5
                reasoning.append(f'impulse={impulse:.2f} > {impulse_threshold} → stage_slice ×0.5')

            # Health WARN → additional slice reduction
            health = compute_health(features) if features else 'OK'
            if health == 'WARN':
                stage_slice_mult = min(stage_slice_mult, cfg['breakout_stage_slice_mult'])
                reasoning.append('health=WARN → stage_slice capped')

            max_stage = cfg.get('max_stage_v3', 3)
            reasoning.append(f'max_stage: {max_stage}')
        else:
            # ── Legacy risk (ff_regime_risk_v3=OFF) ──
            raw_sl = cfg['atr_sl_mult'] * atr_pct
            sl_pct = _clamp(raw_sl, cfg['min_sl_pct'], cfg['max_sl_pct'])
            reasoning.append(f'SL: {cfg["atr_sl_mult"]}×ATR({atr_pct:.4f})={raw_sl:.4f} → clamped {sl_pct:.4f}')

            # Stage slice adjustment
            health = compute_health(features) if features else 'OK'
            if regime_class == 'BREAKOUT' or health == 'WARN':
                stage_slice_mult = cfg['breakout_stage_slice_mult']
                reasoning.append(f'stage_slice: {stage_slice_mult} (BREAKOUT/WARN)')
            else:
                stage_slice_mult = 1.0

            # Leverage
            if regime_class == 'BREAKOUT':
                leverage_max = 8
            else:
                leverage_max = 5

            max_stage = None  # not set in legacy mode

        # TP calculation (R-based)
        tp_pct = sl_pct * cfg['tp_r_ratio']
        reasoning.append(f'TP: SL({sl_pct:.4f})×{cfg["tp_r_ratio"]}R = {tp_pct:.4f}')
        reasoning.append(f'leverage_max: {leverage_max}')

        result = {
            'sl_pct': sl_pct,
            'tp_pct': tp_pct,
            'stage_slice_mult': stage_slice_mult,
            'leverage_max': leverage_max,
            'reasoning': reasoning,
        }
        if max_stage is not None:
            result['max_stage'] = max_stage
        return result
    except Exception as e:
        _log(f'compute_risk FAIL-OPEN: {e}')
        return {
            'sl_pct': 0.006,
            'tp_pct': 0.0072,
            'stage_slice_mult': 1.0,
            'leverage_max': 5,
            'reasoning': [f'FAIL-OPEN: {e}'],
        }
