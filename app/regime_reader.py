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
_consecutive_same = 0
TRANSITION_COOLDOWN_SEC = 60
TRANSITION_CONFIRM_COUNT = 2


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
    }


def _apply_transition_cooldown(new_regime):
    """Apply transition cooldown: require 2 consecutive same classifications
    or 60 seconds elapsed before accepting a regime change.

    Returns the effective regime (may be the previous one during cooldown).
    """
    global _prev_regime, _prev_regime_ts, _consecutive_same

    now = time.time()

    if _prev_regime is None:
        # First call ever — accept immediately
        _prev_regime = new_regime
        _prev_regime_ts = now
        _consecutive_same = 1
        return new_regime

    if new_regime == _prev_regime:
        # Same regime — reset counter
        _consecutive_same += 1
        return new_regime

    # Different regime detected
    _consecutive_same += 1 if new_regime == getattr(_apply_transition_cooldown, '_pending', None) else 0

    elapsed = now - _prev_regime_ts
    if elapsed >= TRANSITION_COOLDOWN_SEC:
        # Cooldown expired — accept transition
        _prev_regime = new_regime
        _prev_regime_ts = now
        _consecutive_same = 1
        _apply_transition_cooldown._pending = None
        return new_regime

    # Track pending regime for consecutive counting
    if getattr(_apply_transition_cooldown, '_pending', None) == new_regime:
        _consecutive_same += 1
    else:
        _apply_transition_cooldown._pending = new_regime
        _consecutive_same = 1

    if _consecutive_same >= TRANSITION_CONFIRM_COUNT:
        # Confirmed by consecutive readings
        _prev_regime = new_regime
        _prev_regime_ts = now
        _consecutive_same = 1
        _apply_transition_cooldown._pending = None
        return new_regime

    # Still in cooldown — keep previous regime
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
        'tp_mode': 'fixed',  'tp_pct_min': 0.3, 'tp_pct_max': 0.8,
        'sl_pct': 1.0,
        'leverage_min': 3, 'leverage_max': 5,
        'stage_max': 2,
        'entry_filter': 'band_edge',   # BB/VA boundary within 0.3%
        'entry_proximity_pct': 0.3,
        'add_score_threshold': 70,
    },
    'BREAKOUT': {
        'tp_mode': 'trailing',  'trail_pct': 0.8, 'trail_activate_pct': 0.5,
        'sl_pct': 2.0,
        'leverage_min': 5, 'leverage_max': 8,
        'stage_max': 6,
        'entry_filter': 'breakout_confirm',  # VA breakout confirmed
        'confirm_candles': 2,
        'add_score_threshold': 55,
    },
    'SHOCK': {
        'tp_mode': 'fixed',  'tp_pct_min': 0.0, 'tp_pct_max': 0.0,
        'sl_pct': 1.5,
        'leverage_min': 3, 'leverage_max': 3,
        'stage_max': 0,
        'entry_filter': 'blocked',
        'add_score_threshold': 999,
    },
    'UNKNOWN': {  # FAIL-OPEN: identical to existing behavior
        'tp_mode': 'fixed',  'tp_pct_min': 0.5, 'tp_pct_max': 1.5,
        'sl_pct': 2.0,
        'leverage_min': 3, 'leverage_max': 8,
        'stage_max': 7,
        'entry_filter': 'none',
        'add_score_threshold': 45,
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
