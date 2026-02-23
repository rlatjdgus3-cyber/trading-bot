"""
strategy_v3.risk_v3 — Dynamic ATR-based SL, R-based TP, stage slice adjustment.

FAIL-OPEN: any error → returns default conservative params.
"""

from strategy_v3 import config_v3, safe_float, compute_market_health

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


def compute_risk(features, v3_regime, loss_streak=0):
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
            health = compute_market_health(features) if features else 'OK'
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
            health = compute_market_health(features) if features else 'OK'
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

        # Loss streak → stage_slice reduction
        if loss_streak >= 3:
            slice_mult_3 = cfg.get('loss_streak_slice_mult_3', 0.5)
            stage_slice_mult *= slice_mult_3
            reasoning.append(f'loss_streak={loss_streak} → slice×{slice_mult_3}')
        elif loss_streak >= 2:
            slice_mult_2 = cfg.get('loss_streak_slice_mult_2', 0.7)
            stage_slice_mult *= slice_mult_2
            reasoning.append(f'loss_streak={loss_streak} → slice×{slice_mult_2}')

        # TP calculation (R-based)
        tp_pct = sl_pct * cfg['tp_r_ratio']
        reasoning.append(f'TP: SL({sl_pct:.4f})×{cfg["tp_r_ratio"]}R = {tp_pct:.4f}')
        reasoning.append(f'leverage_max: {leverage_max}')

        # P0-1: sl_basis info for downstream consumers
        sl_basis = 'PRICE_PCT'
        try:
            sl_basis = cfg.get('sl_basis', 'PRICE_PCT')
        except Exception:
            pass

        result = {
            'sl_pct': sl_pct,
            'tp_pct': tp_pct,
            'stage_slice_mult': stage_slice_mult,
            'leverage_max': leverage_max,
            'sl_basis': sl_basis,
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


def compute_risk_v11(features, v3_regime, equity, mtf_data=None):
    """[3-1] v1.1 ATR-based risk management.

    Uses 15m ATR for SL, fixed risk% per trade, position sizing from equity.

    Args:
        features: dict from build_feature_snapshot()
        v3_regime: dict from regime_v3.classify()
        equity: float, current equity in USDT
        mtf_data: dict from mtf_direction (with atr_15m)

    Returns:
        dict with sl_distance, sl_pct, position_size_usdt, risk_pct, cap_used_pct
    """
    try:
        # Load unified_v11 from top-level YAML (not strategy_v3 section)
        import yaml, os
        _yaml_path = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                                  'config', 'strategy_modes.yaml')
        try:
            with open(_yaml_path, 'r') as _f:
                _full_cfg = yaml.safe_load(_f) or {}
            v11 = _full_cfg.get('unified_v11', {})
            if not isinstance(v11, dict):
                v11 = {}
        except Exception:
            v11 = {}

        risk_pct = v11.get('risk_pct', 0.0075)  # 0.75% per trade
        sl_atr_mult = v11.get('sl_atr_mult', 2.0)
        cap_max_pct = v11.get('cap_max_pct', 0.20)

        # Get ATR_15m from mtf_data or features
        atr_15m = 0
        if mtf_data and mtf_data.get('atr_15m'):
            atr_15m = float(mtf_data['atr_15m'])
        if atr_15m <= 0:
            atr_val = safe_float(features.get('atr_val') if features else None)
            if atr_val > 0:
                atr_15m = atr_val
            else:
                atr_pct = safe_float(features.get('atr_pct') if features else None, 0.005)
                price = safe_float(features.get('price') if features else None, 50000)
                atr_15m = atr_pct * price

        price = safe_float(features.get('price') if features else None, 50000)
        if price <= 0:
            price = 50000  # fallback

        sl_distance = sl_atr_mult * atr_15m
        sl_pct = sl_distance / price if price > 0 else 0.01

        if sl_pct <= 0:
            sl_pct = 0.01  # safety floor
            sl_distance = price * sl_pct

        position_size_usdt = (equity * risk_pct) / sl_pct if sl_pct > 0 else 0

        # Cap: total capital used <= cap_max_pct
        max_usdt = equity * cap_max_pct
        position_size_usdt = min(position_size_usdt, max_usdt)
        position_size_usdt = max(5, position_size_usdt)  # minimum 5 USDT

        cap_used_pct = (position_size_usdt / equity * 100) if equity > 0 else 0

        _log(f'[v1.1] risk: {risk_pct*100:.2f}% equity={equity:.0f} '
             f'SL={sl_distance:.1f} ({sl_pct*100:.3f}%) '
             f'size={position_size_usdt:.0f} cap={cap_used_pct:.1f}%')

        return {
            'sl_distance': sl_distance,
            'sl_pct': sl_pct,
            'tp_pct': sl_pct * 3.0,  # 1:3 R:R target
            'position_size_usdt': position_size_usdt,
            'risk_pct': risk_pct,
            'cap_used_pct': cap_used_pct,
            'atr_15m': atr_15m,
            'reasoning': [f'v1.1 ATR risk: {sl_atr_mult}×ATR15m({atr_15m:.1f})={sl_distance:.1f}'],
            'stage_slice_mult': 1.0,
            'leverage_max': 5,
        }
    except Exception as e:
        _log(f'compute_risk_v11 FAIL-OPEN: {e}')
        return {
            'sl_distance': 0, 'sl_pct': 0.01, 'tp_pct': 0.03,
            'position_size_usdt': min(equity * 0.10, 100) if equity else 50,
            'risk_pct': 0.0075, 'cap_used_pct': 10,
            'reasoning': [f'FAIL-OPEN: {e}'],
            'stage_slice_mult': 1.0, 'leverage_max': 5,
        }
