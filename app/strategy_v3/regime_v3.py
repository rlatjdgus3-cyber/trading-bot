"""
strategy_v3.regime_v3 — 4-state regime classification with hysteresis.

Regime classes:
  STATIC_RANGE  — low drift, low ADX → mean reversion
  DRIFT_UP      — upward drift → follow drift long
  DRIFT_DOWN    — downward drift → follow drift short
  BREAKOUT      — high ADX / volume spike / health WARN → trend following

Entry modes:
  MeanRev       — fade edges in static range
  DriftFollow   — trade with drift direction
  BreakoutTrend — follow breakout direction

FAIL-OPEN: any error → STATIC_RANGE (most conservative).
"""

import time
from strategy_v3 import config_v3

LOG_PREFIX = '[regime_v3]'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


# ── Hysteresis state ──
_v3_state = {
    'current_class': None,
    'held_since': 0,
    'pending_class': None,
    'pending_count': 0,
    'last_sl_ts': 0,
    'last_sl_direction': None,
}


def reset_state():
    """Reset hysteresis state (for testing)."""
    _v3_state.update({
        'current_class': None,
        'held_since': 0,
        'pending_class': None,
        'pending_count': 0,
        'last_sl_ts': 0,
        'last_sl_direction': None,
    })


def record_stop_loss(direction):
    """Record a stop-loss event for cooldown tracking."""
    _v3_state['last_sl_ts'] = time.time()
    _v3_state['last_sl_direction'] = direction


def get_sl_cooldown_info():
    """Return (last_sl_ts, last_sl_direction) for cooldown checks."""
    return _v3_state['last_sl_ts'], _v3_state['last_sl_direction']


def _safe_float(val, default=0.0):
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _safe_bool(val, default=False):
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    return default


def _compute_health(features):
    """Compute health status from spread_ok and liquidity_ok."""
    spread_ok = features.get('spread_ok', True)
    liquidity_ok = features.get('liquidity_ok', True)
    if spread_ok is None:
        spread_ok = True
    if liquidity_ok is None:
        liquidity_ok = True
    return 'OK' if (spread_ok and liquidity_ok) else 'WARN'


def _classify_raw(features, regime_ctx):
    """Classify regime without hysteresis. Returns (class, entry_mode, confidence, reasons)."""
    cfg = config_v3.get_all()

    drift_score = _safe_float(features.get('drift_score'))
    drift_direction = features.get('drift_direction', 'NONE')
    adx = _safe_float(features.get('adx') or (regime_ctx.get('adx_14') if regime_ctx else None))
    health = _compute_health(features)
    breakout_confirmed = _safe_bool(regime_ctx.get('breakout_confirmed') if regime_ctx else None)
    bbw_ratio = _safe_float(regime_ctx.get('bbw_ratio') if regime_ctx else None)
    volume_z = _safe_float(features.get('volume_z'))

    reasons = []

    # ── Priority 1: BREAKOUT ──
    is_breakout = False

    if health == 'WARN':
        is_breakout = True
        reasons.append(f'health=WARN (spread/liquidity)')

    if breakout_confirmed:
        is_breakout = True
        reasons.append('breakout_confirmed=True')

    if adx >= cfg['adx_breakout_min']:
        # Need at least 1 auxiliary confirmation
        aux_count = 0
        if bbw_ratio >= cfg['breakout_bb_expand_min']:
            aux_count += 1
            reasons.append(f'bbw_ratio={bbw_ratio:.2f} >= {cfg["breakout_bb_expand_min"]}')
        if volume_z >= cfg['breakout_volume_z_min']:
            aux_count += 1
            reasons.append(f'volume_z={volume_z:.2f} >= {cfg["breakout_volume_z_min"]}')
        if aux_count >= 1:
            is_breakout = True
            reasons.append(f'ADX={adx:.1f} >= {cfg["adx_breakout_min"]}')

    if is_breakout:
        conf = min(90, 50 + int(adx))
        return ('BREAKOUT', 'BreakoutTrend', conf, reasons)

    # ── Priority 2: DRIFT_UP / DRIFT_DOWN ──
    if drift_score >= cfg['drift_trend_min'] and drift_direction in ('UP', 'DOWN'):
        if adx < cfg['adx_breakout_min']:  # not breakout
            regime_class = 'DRIFT_UP' if drift_direction == 'UP' else 'DRIFT_DOWN'
            conf = min(80, 50 + int(drift_score * 10000))
            reasons.append(f'drift={drift_score:.4f} >= {cfg["drift_trend_min"]}')
            reasons.append(f'drift_dir={drift_direction}')
            return (regime_class, 'DriftFollow', conf, reasons)

    # ── Priority 3: STATIC_RANGE ──
    if (drift_score <= cfg['drift_static_max']
            and adx <= cfg['adx_range_max']
            and health == 'OK'):
        conf = min(85, 60 + int((cfg['adx_range_max'] - adx) * 2))
        reasons.append(f'drift={drift_score:.4f} <= {cfg["drift_static_max"]}')
        reasons.append(f'ADX={adx:.1f} <= {cfg["adx_range_max"]}')
        return ('STATIC_RANGE', 'MeanRev', conf, reasons)

    # ── Fallback ──
    if drift_score > cfg['drift_static_max'] and drift_direction in ('UP', 'DOWN'):
        regime_class = 'DRIFT_UP' if drift_direction == 'UP' else 'DRIFT_DOWN'
        reasons.append(f'fallback: drift={drift_score:.4f} > static_max')
        return (regime_class, 'DriftFollow', 45, reasons)

    reasons.append('fallback: STATIC_RANGE (conservative default)')
    return ('STATIC_RANGE', 'MeanRev', 40, reasons)


def _apply_hysteresis(raw_class):
    """Apply hysteresis: require N consecutive confirmations and minimum dwell time.

    Returns effective regime class (may differ from raw_class).
    """
    cfg = config_v3.get_all()
    now = time.time()

    # First call ever — accept immediately
    if _v3_state['current_class'] is None:
        _v3_state['current_class'] = raw_class
        _v3_state['held_since'] = now
        _v3_state['pending_class'] = None
        _v3_state['pending_count'] = 0
        return raw_class

    # Same as current — reset pending
    if raw_class == _v3_state['current_class']:
        _v3_state['pending_class'] = None
        _v3_state['pending_count'] = 0
        return raw_class

    # Different regime detected — check minimum dwell time
    held_sec = now - _v3_state['held_since']
    if held_sec < cfg['regime_min_dwell_sec']:
        return _v3_state['current_class']

    # Track pending for consecutive confirmation
    if raw_class == _v3_state['pending_class']:
        _v3_state['pending_count'] += 1
    else:
        _v3_state['pending_class'] = raw_class
        _v3_state['pending_count'] = 1

    # Check confirmation threshold
    if _v3_state['pending_count'] >= cfg['regime_confirm_bars']:
        _v3_state['current_class'] = raw_class
        _v3_state['held_since'] = now
        _v3_state['pending_class'] = None
        _v3_state['pending_count'] = 0
        _log(f'regime transition: → {raw_class} (after {cfg["regime_confirm_bars"]} confirms)')
        return raw_class

    # Not yet confirmed — hold current
    return _v3_state['current_class']


def classify(features, regime_ctx, prev_state=None):
    """Classify market regime into V3 4-state model.

    Args:
        features: dict from build_feature_snapshot()
        regime_ctx: dict from regime_reader.get_current_regime()
        prev_state: unused (reserved for future)

    Returns:
        dict with keys:
            regime_class: STATIC_RANGE | DRIFT_UP | DRIFT_DOWN | BREAKOUT
            entry_mode: MeanRev | DriftFollow | BreakoutTrend
            confidence: 0-100
            reasons: list[str]
            raw_class: str (pre-hysteresis classification)
    """
    try:
        if not features:
            features = {}
        if not regime_ctx:
            regime_ctx = {}

        raw_class, entry_mode, confidence, reasons = _classify_raw(features, regime_ctx)

        # Apply hysteresis
        effective_class = _apply_hysteresis(raw_class)

        # If hysteresis changed the class, update entry_mode accordingly
        if effective_class != raw_class:
            if effective_class == 'BREAKOUT':
                entry_mode = 'BreakoutTrend'
            elif effective_class in ('DRIFT_UP', 'DRIFT_DOWN'):
                entry_mode = 'DriftFollow'
            elif effective_class == 'STATIC_RANGE':
                entry_mode = 'MeanRev'
            reasons.append(f'hysteresis hold: raw={raw_class} → effective={effective_class}')

        return {
            'regime_class': effective_class,
            'entry_mode': entry_mode,
            'confidence': confidence,
            'reasons': reasons,
            'raw_class': raw_class,
        }
    except Exception as e:
        _log(f'classify FAIL-OPEN: {e}')
        return {
            'regime_class': 'STATIC_RANGE',
            'entry_mode': 'MeanRev',
            'confidence': 30,
            'reasons': [f'FAIL-OPEN: {e}'],
            'raw_class': 'STATIC_RANGE',
        }
