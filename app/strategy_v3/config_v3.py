"""
strategy_v3.config_v3 — V3 parameter loader with defaults.

All V3 parameters live in config/strategy_modes.yaml → strategy_v3 section.
This module provides fallback defaults and a typed getter.
"""

import os
import threading
import yaml

LOG_PREFIX = '[config_v3]'

_config_cache = None
_config_lock = threading.Lock()
_CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                            'config', 'strategy_modes.yaml')

# ── Default values (fallback when YAML key is missing) ──

DEFAULTS = {
    # Feature flag
    'enabled': False,

    # Regime classification
    'drift_static_max': 0.0003,
    'drift_trend_min': 0.0008,
    'adx_range_max': 20,
    'adx_breakout_min': 28,
    'regime_confirm_bars': 3,
    'regime_min_dwell_sec': 300,
    'cooldown_after_stop_sec': 600,
    'breakout_bb_expand_min': 1.3,
    'breakout_volume_z_min': 1.5,

    # Static Range (Mean Reversion)
    'poc_band_atr_mult': 0.5,
    'poc_band_va_mult': 0.15,
    'edge_score_bonus': 25,
    'poc_zone_penalty': 40,
    'edge_overshoot_atr_mult': 1.5,
    'edge_overshoot_penalty': 30,

    # Drifting Range
    'drift_aligned_bonus': 15,
    'drift_counter_penalty': 50,
    'retest_confirm_bars': 3,
    'poc_support_tolerance_atr': 0.3,

    # Breakout
    'breakout_pad_atr_mult': 0.3,
    'retest_entry_enabled': True,
    'chase_ban_atr_mult': 2.0,
    'chase_score_penalty': 40,
    'breakout_trend_bonus': 25,

    # Dynamic Risk
    'atr_sl_mult': 1.5,
    'min_sl_pct': 0.003,
    'max_sl_pct': 0.02,
    'tp_r_ratio': 1.2,
    'breakout_stage_slice_mult': 0.5,
    'sl_update_min_interval_sec': 300,
    'sl_update_min_change_pct': 0.1,

    # Signal Debounce
    'signal_debounce_sec': 300,
    'post_sl_same_dir_cooldown_sec': 600,

    # Strict Breakout 3중 확인
    'strict_breakout_n_candles': 3,
    'strict_breakout_m_outside': 2,
    'strict_breakout_k_dist': 0.25,
    'strict_breakout_pct_dist': 0.0015,
    'strict_breakout_volume_z_min': 1.0,
    'strict_breakout_atr_ratio_min': 1.5,

    # 레짐별 리스크
    'static_range_min_sl': 0.005,
    'static_range_max_sl': 0.009,
    'drift_min_sl': 0.006,
    'drift_max_sl': 0.012,
    'breakout_min_sl': 0.008,
    'breakout_max_sl': 0.02,
    'breakout_atr_sl_mult': 2.0,
    'impulse_chase_threshold': 1.5,
    'max_stage_v3': 3,
    'breakout_reverse_block': True,

    # Scoring guards
    'liquidity_hard_block': True,
    'spread_hard_block': True,
    'vol_pct_na_penalty': 10,
    'non_breakout_impulse_penalty': 20,
    'non_breakout_impulse_block_mult': 1.5,

    # 연속손실 쿨다운
    'loss_streak_trigger': 3,
    'loss_streak_cooldown_sec': 1200,
    'loss_streak_window_hours': 3,

    # Performance guardrails
    'mode_cooloff_enabled': False,
    'mode_cooloff_min_trades': 5,
    'mode_cooloff_min_winrate': 0.30,
    'mode_cooloff_hours': 4,
    'loss_streak_conf_escalation': 5,
    'loss_streak_slice_mult_2': 0.7,
    'loss_streak_slice_mult_3': 0.5,

    # Adaptive Layers v2.1
    'adaptive_dryrun': True,
    'adaptive_l1_streak_penalty_3': 0.70,
    'adaptive_l1_cooldown_5_sec': 7200,
    'adaptive_l1_global_wr_trades': 20,
    'adaptive_l1_global_wr_low': 0.35,
    'adaptive_l1_global_wr_recovery': 0.40,
    'adaptive_l1_global_wr_threshold_add': 10,
    'adaptive_l1_global_wr_add_conf_min': 60,
    'adaptive_l2_range_pos_short_min': 0.85,
    'adaptive_l2_impulse_hard_block': 1.5,
    'adaptive_l3_peak_upnl_threshold': 0.4,
    'adaptive_l4_warn_tighten_sec': 120,
    'adaptive_l4_time_stop_mult': 0.5,
    'adaptive_l5_trades': 50,
    'adaptive_l5_min_sample': 10,
    'adaptive_l5_wr_low': 0.35,
    'adaptive_l5_wr_recovery': 0.40,
    'adaptive_l5_penalty': 0.75,
    'adaptive_combined_penalty_floor': 0.55,
    'adaptive_anti_paralysis_hours_1': 24,
    'adaptive_anti_paralysis_hours_2': 36,
}


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _load_v3_section():
    """Load strategy_v3 section from YAML. Returns dict (empty on error)."""
    global _config_cache
    if _config_cache is not None:
        return _config_cache
    with _config_lock:
        if _config_cache is not None:
            return _config_cache
        try:
            with open(_CONFIG_PATH, 'r') as f:
                full = yaml.safe_load(f) or {}
            _config_cache = full.get('strategy_v3', {})
            return _config_cache
        except Exception as e:
            _log(f'load FAIL-OPEN: {e}')
            _config_cache = {}
            return _config_cache


def reload():
    """Force-reload config (for hot reload / testing)."""
    global _config_cache
    with _config_lock:
        _config_cache = None


def get(key, default=None):
    """Get a V3 config value with fallback chain: YAML → DEFAULTS → default arg."""
    section = _load_v3_section()
    if key in section:
        return section[key]
    if key in DEFAULTS:
        return DEFAULTS[key]
    return default


def is_enabled():
    """Check if V3 is enabled. FAIL-OPEN: returns False."""
    try:
        val = get('enabled', False)
        return str(val).lower() in ('true', '1', 'on')
    except Exception:
        return False


def get_all():
    """Return merged config (DEFAULTS overridden by YAML values)."""
    section = _load_v3_section()
    merged = dict(DEFAULTS)
    merged.update(section)
    return merged
