"""
regime_reader.py — Read-only helper for live trading bot to read market regime.

FAIL-OPEN guarantee:
  - Table missing / empty / stale (>5 min) → returns UNKNOWN → existing behavior preserved.
  - Never blocks, never raises.
"""

import time

STALE_THRESHOLD_SEC = 300
LOG_PREFIX = '[regime_reader]'

# ── Regime transition cooldown state ──
_prev_regime = None
_prev_regime_ts = 0
_prev_regime_held_since = 0          # timestamp when current regime was first accepted
_consecutive_same = 0
TRANSITION_COOLDOWN_SEC = 60
TRANSITION_CONFIRM_COUNT = 2
# Direction-specific minimum hold times (seconds)
RANGE_TO_BREAKOUT_MIN_HOLD_SEC = 20 * 60     # 20 minutes
BREAKOUT_TO_RANGE_MIN_HOLD_SEC = 30 * 60     # 30 minutes


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _default():
    """FAIL-OPEN default: UNKNOWN regime, no restrictions."""
    return {
        'regime': 'UNKNOWN',
        'confidence': 0,
        'shock_type': None,
        'flow_bias': 0,
        'breakout_confirmed': False,
        'stale': False,
        'available': False,
        'in_transition': False,
        'adx_14': None,
        'vah': None,
        'val': None,
        'poc': None,
        'price_vs_va': None,
        'bbw_ratio': None,
        'bb_width_pct': None,
    }


def _apply_transition_cooldown(new_regime):
    """Apply transition cooldown with direction-specific minimum hold times.

    RANGE→BREAKOUT: require 2 consecutive confirms + 20 min minimum hold
    BREAKOUT→RANGE: require 2 consecutive confirms + 30 min minimum hold

    Returns the effective regime (may be the previous one during cooldown).
    """
    global _prev_regime, _prev_regime_ts, _consecutive_same, _prev_regime_held_since

    now = time.time()

    if _prev_regime is None:
        # First call ever — accept immediately
        _prev_regime = new_regime
        _prev_regime_ts = now
        _prev_regime_held_since = now
        _consecutive_same = 1
        return new_regime

    if new_regime == _prev_regime:
        # Same regime — reset counter
        _consecutive_same += 1
        return new_regime

    # Different regime detected — check direction-specific minimum hold
    held_sec = now - _prev_regime_held_since
    if _prev_regime == 'RANGE' and new_regime == 'BREAKOUT':
        min_hold = RANGE_TO_BREAKOUT_MIN_HOLD_SEC
    elif _prev_regime == 'BREAKOUT' and new_regime == 'RANGE':
        min_hold = BREAKOUT_TO_RANGE_MIN_HOLD_SEC
    else:
        min_hold = TRANSITION_COOLDOWN_SEC

    if held_sec < min_hold:
        # Minimum hold not met — keep previous regime
        _log(f'transition hold: {_prev_regime}→{new_regime} blocked '
             f'(held {held_sec:.0f}s < {min_hold}s)')
        return _prev_regime

    # Track pending regime for consecutive confirmation (single increment per call)
    pending = getattr(_apply_transition_cooldown, '_pending', None)
    if pending == new_regime:
        _consecutive_same += 1
    else:
        _apply_transition_cooldown._pending = new_regime
        _consecutive_same = 1

    elapsed = now - _prev_regime_ts
    if _consecutive_same >= TRANSITION_CONFIRM_COUNT or elapsed >= TRANSITION_COOLDOWN_SEC:
        # Confirmed by consecutive readings or cooldown expired
        _prev_regime = new_regime
        _prev_regime_ts = now
        _prev_regime_held_since = now
        _consecutive_same = 1
        _apply_transition_cooldown._pending = None
        return new_regime

    # Still waiting for confirmation — keep previous regime
    return _prev_regime


def get_current_regime(cur, symbol='BTC/USDT:USDT'):
    """Read latest regime from market_context_latest view.

    Returns:
    {
        'regime': 'RANGE'|'BREAKOUT'|'SHOCK'|'UNKNOWN',
        'confidence': 0-100,
        'shock_type': str|None,
        'flow_bias': float,
        'breakout_confirmed': bool,
        'stale': bool,
        'available': bool,  # True if data is valid and fresh
    }
    """
    try:
        cur.execute("""
            SELECT regime, regime_confidence, shock_type, flow_bias,
                   breakout_confirmed, age_seconds,
                   adx_14, vah, val, poc, price_vs_va, bbw_ratio
            FROM market_context_latest
            WHERE symbol = %s;
        """, (symbol,))
        row = cur.fetchone()
        if not row:
            return _default()

        age = float(row[5]) if row[5] else 9999
        stale = age > STALE_THRESHOLD_SEC

        raw_regime = row[0] if not stale else 'UNKNOWN'

        # ── Transition cooldown filter ──
        in_transition = False
        regime = _apply_transition_cooldown(raw_regime)
        if regime != raw_regime:
            in_transition = True

        # v14.1: Compute absolute bb_width_pct from indicators table
        bb_width_pct = None
        try:
            cur.execute("""
                SELECT bb_up, bb_dn, bb_mid FROM indicators
                WHERE symbol = %s
                ORDER BY ts DESC LIMIT 1;
            """, (symbol,))
            bb_row = cur.fetchone()
            if bb_row and bb_row[0] and bb_row[1] and bb_row[2]:
                bb_mid_val = float(bb_row[2])
                if bb_mid_val > 0:
                    bb_width_pct = (float(bb_row[0]) - float(bb_row[1])) / bb_mid_val * 100
        except Exception:
            pass

        return {
            'regime': regime,
            'confidence': int(row[1]) if row[1] and not stale else 0,
            'shock_type': row[2] if not stale else None,
            'flow_bias': float(row[3]) if row[3] else 0,
            'breakout_confirmed': bool(row[4]) if row[4] is not None and not stale else False,
            'stale': stale,
            'available': not stale,
            'in_transition': in_transition,
            # Extended fields for entry filters
            'adx_14': float(row[6]) if row[6] is not None else None,
            'vah': float(row[7]) if row[7] is not None else None,
            'val': float(row[8]) if row[8] is not None else None,
            'poc': float(row[9]) if row[9] is not None else None,
            'price_vs_va': row[10],
            'bbw_ratio': float(row[11]) if row[11] is not None else None,
            'bb_width_pct': bb_width_pct,
        }
    except Exception as e:
        # FAIL-OPEN: table doesn't exist, DB error, etc.
        _log(f'FAIL-OPEN: {e}')
        return _default()


def get_stage_limit(regime, shock_type=None):
    """Get maximum stage count for a given regime.

    RANGE:          3
    BREAKOUT:       7
    SHOCK/VETO:     0  (block all entries)
    SHOCK/RISK_DOWN: 2
    SHOCK/ACCEL:    5
    UNKNOWN:        7  (FAIL-OPEN, no restriction)
    """
    if regime == 'RANGE':
        return 3
    if regime == 'BREAKOUT':
        return 7
    if regime == 'SHOCK':
        if shock_type == 'VETO':
            return 0
        if shock_type == 'RISK_DOWN':
            return 2
        if shock_type == 'ACCEL':
            return 5
        return 2  # default SHOCK without sub-type
    # UNKNOWN or anything else: FAIL-OPEN
    return 7


# ── REGIME_PARAMS: single source of truth for mode-based parameters ──

REGIME_PARAMS = {
    'RANGE': {
        'tp_mode': 'fixed',  'tp_pct_min': 0.35, 'tp_pct_max': 0.90,
        'sl_pct': 0.60,
        'sl_pct_min': 0.40, 'sl_pct_max': 0.70,
        'leverage_min': 3, 'leverage_max': 5,
        'stage_max': 3,
        'stage_allocation': [0.4, 0.35, 0.25],  # stage 1/2/3 capital split
        'entry_filter': 'band_edge',   # BB/VA boundary within 0.3%
        'entry_proximity_pct': 0.3,
        'add_score_threshold': 70,
        # TP split (RANGE only)
        'tp_split_enabled': True,
        'tp1_target': 'POC_or_BB_mid',   # TP1: POC/BB_mid 도달 → 50% 청산
        'tp1_close_pct': 50,
        'tp2_target': 'opposite_band',    # TP2: 반대 밴드 도달 → 전량 청산
        'tp2_close_pct': 100,
        # Zone margin
        'zone_margin_va_pct': 0.12,
        'zone_margin_bb_pct': 0.08,
        'mid_zone_ban_pct': 0.15,
        # Anti-chase filter
        'anti_chase_ret_1m': 0.25,
        'anti_chase_ret_5m': 0.60,
        'anti_chase_ban_sec': 180,
        # TIME STOP & entry caps
        'time_stop_min': 25,
        'max_entries_per_hour': 3,
        'consec_loss_cooldown_min': 45,
        'add_min_profit_pct': 0.25,
        # RSI filter
        'rsi_oversold': 30,
        'rsi_overbought': 70,
        # v14: ADD 속도 제어
        'add_min_interval_sec': 480,     # 8분 간격
        'max_adds_per_30m': 2,
        'same_dir_reentry_cooldown_sec': 600,  # 10분
        'add_retest_required': True,     # VWAP/BB_mid 되돌림 필요
        # v14: EVENT 제한 — RANGE에서 EVENT→즉시ADD 금지
        'event_add_blocked': True,
    },
    'BREAKOUT': {
        'tp_mode': 'trailing',  'trail_pct': 0.8, 'trail_activate_pct': 0.5,
        'tp_pct_min': 0.8, 'tp_pct_max': 2.0,
        'sl_pct': 1.2,
        'leverage_min': 3, 'leverage_max': 8,
        'stage_max': 7,
        'entry_filter': 'breakout_confirm',  # VA breakout confirmed
        'confirm_candles': 2,
        'add_score_threshold': 50,
        # Fake breakout & ADD gate
        'fake_breakout_retracement_pct': 60,
        'add_min_profit_pct': 0.4,
        # Trailing SL to breakeven
        'trailing_be_threshold_pct': 0.8,
        # ADD structure integrity check
        'add_structure_intact_required': True,
        # Stage-based leverage ranges
        'stage_leverage': {1: (3, 5), 2: (4, 6), 3: (4, 6), 4: (4, 6),
                           5: (5, 8), 6: (5, 8), 7: (5, 8)},
        # v14: ADD 속도 제어
        'add_min_interval_sec': 120,     # 2분 간격
        'max_adds_per_30m': 4,
        'same_dir_reentry_cooldown_sec': 300,  # 5분
        'add_retest_required': False,    # 돌파 모드: 리테스트 불필요
        'event_add_blocked': False,
    },
    'SHOCK': {
        'tp_mode': 'fixed',  'tp_pct_min': 0.5, 'tp_pct_max': 1.0,
        'sl_pct': 1.5,
        'leverage_min': 3, 'leverage_max': 3,
        'stage_max': 0,
        'entry_filter': 'blocked',
        'add_score_threshold': 999,
        'add_min_interval_sec': 9999,
        'max_adds_per_30m': 0,
        'add_retest_required': False,
        'add_min_profit_pct': 999,
        'event_add_blocked': True,
    },
    'UNKNOWN': {  # FAIL-OPEN: identical to existing behavior
        'tp_mode': 'fixed',  'tp_pct_min': 0.5, 'tp_pct_max': 1.5,
        'sl_pct': 2.0,
        'leverage_min': 3, 'leverage_max': 8,
        'stage_max': 7,
        'entry_filter': 'none',
        'add_score_threshold': 45,
        'add_min_interval_sec': 300,
        'max_adds_per_30m': 3,
        'add_retest_required': False,
        'event_add_blocked': False,
    },
}


def get_regime_params(regime, shock_type=None):
    """Get mode-specific parameters for a given regime.

    SHOCK/ACCEL is treated as a modified BREAKOUT (reduced leverage/stage).
    FAIL-OPEN: unknown regime → UNKNOWN params (no restrictions).
    """
    if regime == 'SHOCK' and shock_type == 'ACCEL':
        params = dict(REGIME_PARAMS['BREAKOUT'])
        params['stage_max'] = 5
        params['leverage_max'] = 6
        return params
    return dict(REGIME_PARAMS.get(regime, REGIME_PARAMS['UNKNOWN']))


# ── Strategy v2: YAML-based mode params ──

def get_mode_params(mode):
    """Get mode-specific parameters from YAML config (strategy v2).

    Args:
        mode: 'A', 'B', or 'C'

    Returns:
        dict — mode config from strategy_modes.yaml, or empty dict on error.
    FAIL-OPEN: returns {} if config unavailable.
    """
    try:
        from strategy.regime_router import get_mode_config
        return get_mode_config(mode)
    except Exception as e:
        _log(f'get_mode_params FAIL-OPEN: {e}')
        return {}
