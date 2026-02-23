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
from strategy_v3 import config_v3, safe_float, compute_market_health

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


def _safe_bool(val, default=False):
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    return default


def _is_strict_breakout_enabled():
    """Check if ff_strict_breakout flag is ON."""
    try:
        import feature_flags
        return feature_flags.is_enabled('ff_strict_breakout')
    except Exception:
        return False


def _check_strict_breakout(features, cfg):
    """Evaluate 3-way strict breakout gate.

    Returns (passed, strict_reasons_dict) where strict_reasons_dict has:
        structure_pass, volume_pass, atr_pass, fail_reasons list.
    """
    structure_pass = _safe_bool(features.get('structure_breakout_pass'))
    struct_dir = features.get('structure_breakout_dir')
    volume_z = safe_float(features.get('volume_z'))
    atr_ratio = safe_float(features.get('atr_ratio'))

    vol_min = cfg.get('strict_breakout_volume_z_min', 1.0)
    atr_min = cfg.get('strict_breakout_atr_ratio_min', 1.5)

    volume_pass = volume_z >= vol_min
    atr_pass = atr_ratio >= atr_min

    fail_reasons = []
    if not structure_pass:
        fail_reasons.append(f'structure_breakout=False')
    if not volume_pass:
        fail_reasons.append(f'volume_z={volume_z:.2f} < {vol_min}')
    if not atr_pass:
        fail_reasons.append(f'atr_ratio={atr_ratio:.2f} < {atr_min}')

    passed = structure_pass and volume_pass and atr_pass

    return passed, {
        'structure_pass': structure_pass,
        'volume_pass': volume_pass,
        'atr_pass': atr_pass,
        'struct_dir': struct_dir,
        'fail_reasons': fail_reasons,
    }


def _classify_raw(features, regime_ctx):
    """Classify regime without hysteresis. Returns (class, entry_mode, confidence, reasons, extra)."""
    cfg = config_v3.get_all()

    drift_score = safe_float(features.get('drift_score'))
    drift_direction = features.get('drift_direction', 'NONE')
    adx = safe_float(features.get('adx') or (regime_ctx.get('adx_14') if regime_ctx else None))
    health = compute_market_health(features)
    breakout_confirmed = _safe_bool(regime_ctx.get('breakout_confirmed') if regime_ctx else None)
    bbw_ratio = safe_float(regime_ctx.get('bbw_ratio') if regime_ctx else None)
    volume_z = safe_float(features.get('volume_z'))

    reasons = []
    extra = {}  # strict breakout metadata

    strict_enabled = _is_strict_breakout_enabled()

    # ── Priority 1: BREAKOUT ──
    is_breakout = False

    # health=WARN alone does NOT trigger BREAKOUT — low liquidity ≠ breakout.
    # WARN is handled separately via stage_slice reduction in risk_v3.

    if strict_enabled:
        # ── Strict Breakout: 3-way gate ──
        strict_passed, strict_info = _check_strict_breakout(features, cfg)
        extra['breakout_strict'] = strict_passed
        extra['structure_pass'] = strict_info['structure_pass']
        extra['volume_pass'] = strict_info['volume_pass']
        extra['atr_pass'] = strict_info['atr_pass']
        extra['strict_reasons'] = strict_info['fail_reasons']

        if strict_passed:
            is_breakout = True
            reasons.append('strict_breakout: structure+volume+atr triple pass')
        elif breakout_confirmed or drift_direction in ('UP', 'DOWN'):
            # Demote to DRIFT — absorb false breakout
            demote_dir = strict_info.get('struct_dir') or drift_direction
            if demote_dir in ('UP', 'DOWN'):
                regime_class = 'DRIFT_UP' if demote_dir == 'UP' else 'DRIFT_DOWN'
            else:
                regime_class = 'DRIFT_UP'  # default to UP if ambiguous
            conf = min(65, 50 + int(adx * 0.5))
            reasons.append(f'strict_breakout DEMOTE: {strict_info["fail_reasons"]}')
            if breakout_confirmed:
                reasons.append('breakout_confirmed=True but strict gate failed')
            extra['breakout_strict'] = False
            return (regime_class, 'DriftFollow', conf, reasons, extra)

        # ADX+auxiliary path still available (already 2-way confirmed)
        if not is_breakout and adx >= cfg['adx_breakout_min']:
            aux_count = 0
            if bbw_ratio >= cfg['breakout_bb_expand_min']:
                aux_count += 1
                reasons.append(f'bbw_ratio={bbw_ratio:.2f} >= {cfg["breakout_bb_expand_min"]}')
            if volume_z >= cfg['breakout_volume_z_min']:
                aux_count += 1
                reasons.append(f'volume_z={volume_z:.2f} >= {cfg["breakout_volume_z_min"]}')
            if aux_count >= 1:
                is_breakout = True
                reasons.append(f'ADX={adx:.1f} >= {cfg["adx_breakout_min"]} (ADX+aux path)')
    else:
        # ── Legacy mode: breakout_confirmed → immediate BREAKOUT ──
        extra['breakout_strict'] = False
        extra['strict_reasons'] = []

        if breakout_confirmed:
            is_breakout = True
            reasons.append('breakout_confirmed=True')

        if adx >= cfg['adx_breakout_min']:
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
        return ('BREAKOUT', 'BreakoutTrend', conf, reasons, extra)

    # ── Priority 2: DRIFT_UP / DRIFT_DOWN ──
    if drift_score >= cfg['drift_trend_min'] and drift_direction in ('UP', 'DOWN'):
        if adx < cfg['adx_breakout_min']:  # not breakout
            regime_class = 'DRIFT_UP' if drift_direction == 'UP' else 'DRIFT_DOWN'
            conf = min(80, 50 + int(drift_score * 10000))
            reasons.append(f'drift={drift_score:.4f} >= {cfg["drift_trend_min"]}')
            reasons.append(f'drift_dir={drift_direction}')
            return (regime_class, 'DriftFollow', conf, reasons, extra)

    # ── Priority 3: STATIC_RANGE ──
    # health=WARN does not disqualify STATIC_RANGE — low liquidity alone
    # is not a regime change; risk_v3 handles it via stage_slice reduction.
    if (drift_score <= cfg['drift_static_max']
            and adx <= cfg['adx_range_max']):
        conf = min(85, 60 + int((cfg['adx_range_max'] - adx) * 2))
        reasons.append(f'drift={drift_score:.4f} <= {cfg["drift_static_max"]}')
        reasons.append(f'ADX={adx:.1f} <= {cfg["adx_range_max"]}')
        if health == 'WARN':
            reasons.append('health=WARN (risk_v3 will reduce slice)')
        return ('STATIC_RANGE', 'MeanRev', conf, reasons, extra)

    # ── Fallback ──
    if drift_score > cfg['drift_static_max'] and drift_direction in ('UP', 'DOWN'):
        regime_class = 'DRIFT_UP' if drift_direction == 'UP' else 'DRIFT_DOWN'
        reasons.append(f'fallback: drift={drift_score:.4f} > static_max')
        return (regime_class, 'DriftFollow', 45, reasons, extra)

    reasons.append('fallback: STATIC_RANGE (conservative default)')
    return ('STATIC_RANGE', 'MeanRev', 40, reasons, extra)


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


def compute_trend_probability(features):
    """D2-1: 연속 확률 0.0~1.0 — 트렌드 존재 확률.

    Components:
      ADX component (0~0.4): ADX 25+ → strong trend signal
      Drift component (0~0.3): drift magnitude → directional momentum
      Volume_z component (0~0.3): volume above average → conviction

    Returns:
        float 0.0~1.0 (0 = no trend, 1 = strong trend)
    """
    try:
        if not features:
            return 0.0  # no data = no trend signal (FAIL-OPEN: won't trigger no-trade zone)

        # ADX component (weight: 0.4)
        # FIX: check both 'adx_14' and 'adx' keys for compatibility
        adx = safe_float(features.get('adx_14') or features.get('adx'), 20)
        if adx >= 40:
            adx_score = 0.4
        elif adx >= 25:
            adx_score = 0.2 + 0.2 * (adx - 25) / 15
        elif adx >= 15:
            adx_score = 0.1 + 0.1 * (adx - 15) / 10
        else:
            adx_score = adx / 15 * 0.1

        # Drift component (weight: 0.3)
        drift = abs(safe_float(features.get('drift_score'), 0))
        if drift >= 0.002:
            drift_score = 0.3
        elif drift >= 0.001:
            drift_score = 0.15 + 0.15 * (drift - 0.001) / 0.001
        else:
            drift_score = drift / 0.001 * 0.15

        # Volume_z component (weight: 0.3)
        vol_z = safe_float(features.get('volume_z'), 0)
        if vol_z >= 2.0:
            vol_score = 0.3
        elif vol_z >= 1.0:
            vol_score = 0.15 + 0.15 * (vol_z - 1.0) / 1.0
        elif vol_z >= 0:
            vol_score = vol_z / 1.0 * 0.15
        else:
            vol_score = 0.0

        return round(min(1.0, adx_score + drift_score + vol_score), 4)

    except Exception as e:
        _log(f'compute_trend_probability error: {e}')
        return 0.5


def is_no_trade_zone(trend_prob, cfg=None):
    """D2-1: no-trade zone 판별 (0.30~0.70 구간 = 방향성 불확실).

    Returns:
        (is_ntx: bool, reason: str)
    """
    try:
        if cfg is None:
            cfg = config_v3.get_all()
        ntx_low = cfg.get('no_trade_zone_low', 0.30)
        ntx_high = cfg.get('no_trade_zone_high', 0.70)
        if ntx_low <= trend_prob <= ntx_high:
            return (True, f'NO_TRADE_ZONE: trend_prob={trend_prob:.3f} in [{ntx_low},{ntx_high}]')
        return (False, f'trend_prob={trend_prob:.3f} outside no-trade zone')
    except Exception as e:
        return (False, f'ntx check error: {e}')


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

        raw_class, entry_mode, confidence, reasons, extra = _classify_raw(features, regime_ctx)

        # [1-3] range_pos > 1.0 → BREAKOUT override
        try:
            import feature_flags
            if feature_flags.is_enabled('ff_unified_engine_v11'):
                range_pos = features.get('range_position')
                if range_pos is not None and range_pos > 1.0:
                    raw_class = 'BREAKOUT'
                    entry_mode = 'BreakoutTrend'
                    reasons.append(f'[v1.1] range_pos={range_pos:.2f} > 1.0 → BREAKOUT')
                    extra['range_pos_raw'] = range_pos
                    extra['range_pos_clamped'] = min(1.0, max(0.0, range_pos))
        except Exception:
            pass  # FAIL-OPEN

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

        result = {
            'regime_class': effective_class,
            'entry_mode': entry_mode,
            'confidence': confidence,
            'reasons': reasons,
            'raw_class': raw_class,
        }
        # Merge strict breakout metadata
        if extra:
            result.update(extra)
        # [1-2] MTF direction field (populated by caller if v1.1 active)
        result['mtf_direction'] = None
        return result
    except Exception as e:
        _log(f'classify FAIL-OPEN: {e}')
        return {
            'regime_class': 'STATIC_RANGE',
            'entry_mode': 'MeanRev',
            'confidence': 30,
            'reasons': [f'FAIL-OPEN: {e}'],
            'raw_class': 'STATIC_RANGE',
            'breakout_strict': False,
            'strict_reasons': [],
        }
