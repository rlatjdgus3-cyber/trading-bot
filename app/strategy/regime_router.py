"""
strategy.regime_router — Core routing logic for MODE_A / MODE_B / MODE_C.

Rule-based + score-based routing.  Stateless per call.
All thresholds read from config YAML.

Inputs:  feature_snapshot, gate_status, current_position (optional)
Outputs: {mode, confidence, reasons, drift_submode}
"""

import os
import yaml

LOG_PREFIX = '[regime_router]'

_config = None
_CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                            'config', 'strategy_modes.yaml')


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _load_config():
    """Load and cache YAML config. FAIL-OPEN with defaults on error."""
    global _config
    if _config is not None:
        return _config
    try:
        with open(_CONFIG_PATH, 'r') as f:
            _config = yaml.safe_load(f)
        _log(f'config loaded from {_CONFIG_PATH}')
        return _config
    except Exception as e:
        _log(f'config load FAIL-OPEN: {e}')
        _config = {}
        return _config


def reload_config():
    """Force-reload config (for testing / hot reload)."""
    global _config
    _config = None
    return _load_config()


def get_config():
    """Get current config dict."""
    return _load_config()


def get_mode_config(mode):
    """Get config section for a specific mode.

    Args:
        mode: 'A', 'B', or 'C'

    Returns:
        dict — mode-specific config parameters
    """
    cfg = _load_config()
    key_map = {
        'A': 'mode_a_static_range',
        'B': 'mode_b_volatile_range',
        'C': 'mode_c_shock_breakout',
    }
    return cfg.get(key_map.get(mode, ''), {})


def route(features, gate_status=None, current_position=None):
    """Route to MODE_A, MODE_B, or MODE_C based on features.

    Args:
        features: dict from build_feature_snapshot()
        gate_status: str or None — if 'BLOCKED', still route but note it
        current_position: dict or None — {side, qty, stage, ...}

    Returns:
        {
            'mode': 'A' | 'B' | 'C',
            'confidence': 0-100,
            'reasons': list[str],
            'drift_submode': 'UP' | 'DOWN' | None,
        }
    """
    cfg = _load_config()
    cfg_a = cfg.get('mode_a_static_range', {})
    cfg_b = cfg.get('mode_b_volatile_range', {})
    cfg_c = cfg.get('mode_c_shock_breakout', {})

    reasons = []
    confidence = 50

    # Extract features (with safe defaults)
    volume_z = features.get('volume_z')
    impulse = features.get('impulse')
    adx = features.get('adx')
    bb_width = features.get('bb_width')
    atr_pct = features.get('atr_pct')
    poc_slope = features.get('poc_slope')
    drift_score = features.get('drift_score', 0)
    drift_dir = features.get('drift_direction', 'NONE')
    price = features.get('price')
    vah = features.get('vah')
    val = features.get('val')

    # ── Priority 1: Check MODE_C (Shock/Breakout) ──
    vol_z_min = cfg_c.get('volume_z_min', 2.0)
    impulse_min = cfg_c.get('impulse_min', 1.5)
    buffer_pct = cfg_c.get('breakout_buffer_pct', 0.002)

    is_c_volume = volume_z is not None and volume_z >= vol_z_min
    is_c_impulse = impulse is not None and impulse >= impulse_min
    is_c_breakout = False
    if price is not None and vah is not None and val is not None:
        upper_bound = vah * (1 + buffer_pct)
        lower_bound = val * (1 - buffer_pct)
        is_c_breakout = price > upper_bound or price < lower_bound

    if (is_c_volume or is_c_impulse) and is_c_breakout:
        reasons.append('MODE_C: volume/impulse spike + price outside VA')
        if is_c_volume:
            reasons.append(f'volume_z={volume_z:.2f} >= {vol_z_min}')
            confidence += 15
        if is_c_impulse:
            reasons.append(f'impulse={impulse:.2f} >= {impulse_min}')
            confidence += 10
        reasons.append(f'price_outside_VA (buffer={buffer_pct})')
        confidence = min(confidence, 95)
        return {
            'mode': 'C',
            'confidence': confidence,
            'reasons': reasons,
            'drift_submode': None,
        }

    # ── Priority 2: Check MODE_A (Static Range) ──
    adx_max = cfg_a.get('adx_max', 20)
    bb_max = cfg_a.get('bb_width_max', 0.012)
    atr_max = cfg_a.get('atr_pct_max', 0.009)
    poc_slope_max = cfg_a.get('poc_slope_max', 0.001)

    is_a_adx = adx is not None and adx <= adx_max
    is_a_bb = bb_width is not None and bb_width <= bb_max
    is_a_atr = atr_pct is not None and atr_pct <= atr_max
    is_a_poc = poc_slope is not None and poc_slope <= poc_slope_max

    # Require at least 3 of 4 conditions (allow 1 missing)
    a_checks = [is_a_adx, is_a_bb, is_a_atr, is_a_poc]
    a_none_count = sum(1 for x in [adx, bb_width, atr_pct, poc_slope] if x is None)
    a_pass_count = sum(1 for x in a_checks if x)

    if a_pass_count >= 3 or (a_pass_count >= 2 and a_none_count >= 1):
        reasons.append('MODE_A: static range conditions met')
        if is_a_adx:
            reasons.append(f'adx={adx:.1f} <= {adx_max}')
        if is_a_bb:
            reasons.append(f'bb_width={bb_width:.4f} <= {bb_max}')
        if is_a_atr:
            reasons.append(f'atr_pct={atr_pct:.4f} <= {atr_max}')
        if is_a_poc:
            reasons.append(f'poc_slope={poc_slope:.5f} <= {poc_slope_max}')
        confidence = 50 + a_pass_count * 10
        confidence = min(confidence, 90)
        return {
            'mode': 'A',
            'confidence': confidence,
            'reasons': reasons,
            'drift_submode': None,
        }

    # ── Priority 3: Default to MODE_B (Volatile Range) ──
    reasons.append('MODE_B: volatile range (default)')
    drift_submode = None
    drift_min = cfg_b.get('drift_poc_slope_min', 0.0015)
    if drift_score > drift_min and drift_dir in ('UP', 'DOWN'):
        drift_submode = drift_dir
        reasons.append(f'drift={drift_dir} (score={drift_score:.4f})')
        confidence += 5

    if adx is not None:
        reasons.append(f'adx={adx:.1f}')
    if bb_width is not None:
        reasons.append(f'bb_width={bb_width:.4f}')

    confidence = min(confidence, 80)
    return {
        'mode': 'B',
        'confidence': confidence,
        'reasons': reasons,
        'drift_submode': drift_submode,
    }
