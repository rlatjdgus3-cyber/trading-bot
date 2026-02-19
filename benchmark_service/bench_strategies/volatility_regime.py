"""
Volatility Regime strategy — ATR/BBW expansion/contraction.

Regime detection via BBW (Bollinger Band Width) moving average:
  EXPANSION:    bbw > bbw_ma * 1.2 → trend-follow (EMA direction)
  CONTRACTION:  bbw < bbw_ma * 0.8 → mean-reversion (BB position)
  TRANSITIONAL: → FLAT

Requires historical_indicators (20 rows) for BBW moving average.
"""


def _compute_bbw(ind):
    """Compute BBW = (bb_up - bb_dn) / bb_mid from an indicator row."""
    bb_up = float(ind.get('bb_up') or 0)
    bb_dn = float(ind.get('bb_dn') or 0)
    bb_mid = float(ind.get('bb_mid') or 0)
    if bb_mid <= 0:
        return 0
    return (bb_up - bb_dn) / bb_mid


def compute_signal(indicators, vol_profile, price, candles, historical_indicators=None):
    """Compute volatility-regime signal from indicators + history."""
    if not indicators or not price:
        return {'signal': 'FLAT', 'confidence': 0,
                'rationale': 'no data', 'indicators': {}}

    price = float(price)
    bb_up = float(indicators.get('bb_up') or 0)
    bb_dn = float(indicators.get('bb_dn') or 0)
    bb_mid = float(indicators.get('bb_mid') or 0)
    ema_9 = float(indicators.get('ema_9') or 0)
    ema_21 = float(indicators.get('ema_21') or 0)
    rsi = float(indicators.get('rsi_14') or 50)
    atr = float(indicators.get('atr_14') or 0)

    snap = {
        'bb_up': bb_up, 'bb_dn': bb_dn, 'bb_mid': bb_mid,
        'ema_9': ema_9, 'ema_21': ema_21, 'rsi_14': rsi,
        'atr_14': atr, 'price': price,
    }

    if not bb_mid or not bb_up or not bb_dn:
        return {'signal': 'FLAT', 'confidence': 0,
                'rationale': 'BB indicators incomplete', 'indicators': snap}

    # Compute current BBW
    bbw = _compute_bbw(indicators)
    snap['bbw'] = round(bbw, 6)

    # Compute BBW moving average from history
    hist = historical_indicators or []
    if len(hist) < 5:
        return {'signal': 'FLAT', 'confidence': 20,
                'rationale': f'insufficient history ({len(hist)} rows)',
                'indicators': snap}

    bbw_values = [_compute_bbw(h) for h in hist if _compute_bbw(h) > 0]
    if not bbw_values:
        return {'signal': 'FLAT', 'confidence': 0,
                'rationale': 'no valid BBW in history', 'indicators': snap}

    bbw_ma = sum(bbw_values) / len(bbw_values)
    snap['bbw_ma'] = round(bbw_ma, 6)
    snap['regime_ratio'] = round(bbw / bbw_ma, 3) if bbw_ma > 0 else 0

    if bbw_ma <= 0:
        return {'signal': 'FLAT', 'confidence': 0,
                'rationale': 'BBW MA zero', 'indicators': snap}

    ratio = bbw / bbw_ma

    # EXPANSION: trend-following mode
    if ratio > 1.2:
        snap['regime'] = 'EXPANSION'
        return _expansion_signal(price, ema_9, ema_21, atr, ratio, snap)

    # CONTRACTION: mean-reversion mode
    if ratio < 0.8:
        snap['regime'] = 'CONTRACTION'
        return _contraction_signal(price, bb_up, bb_dn, bb_mid, rsi, ratio, snap)

    # TRANSITIONAL
    snap['regime'] = 'TRANSITIONAL'
    return {'signal': 'FLAT', 'confidence': 25,
            'rationale': f'transitional regime (ratio={ratio:.2f})',
            'indicators': snap}


def _expansion_signal(price, ema_9, ema_21, atr, ratio, snap):
    """Expansion regime → follow EMA trend direction."""
    if not ema_9 or not ema_21:
        return {'signal': 'FLAT', 'confidence': 30,
                'rationale': 'EXPANSION but EMA incomplete', 'indicators': snap}

    ema_bull = ema_9 > ema_21
    ema_dist = abs(ema_9 - ema_21) / ema_21 * 100 if ema_21 else 0

    # Confidence: base 55, +expansion strength(max 20), +EMA distance(max 25)
    expansion_bonus = min((ratio - 1.2) * 50, 20)
    ema_bonus = min(ema_dist * 25, 25)
    conf = max(55, min(100, int(55 + expansion_bonus + ema_bonus)))

    if ema_bull:
        return {'signal': 'LONG', 'confidence': conf,
                'rationale': f'EXPANSION regime, following EMA trend (ratio={ratio:.2f})',
                'indicators': snap}
    else:
        return {'signal': 'SHORT', 'confidence': conf,
                'rationale': f'EXPANSION regime, following EMA downtrend (ratio={ratio:.2f})',
                'indicators': snap}


def _contraction_signal(price, bb_up, bb_dn, bb_mid, rsi, ratio, snap):
    """Contraction regime → mean-reversion at BB extremes."""
    bb_width = bb_up - bb_dn
    if bb_width <= 0:
        return {'signal': 'FLAT', 'confidence': 25,
                'rationale': 'CONTRACTION but BB width zero', 'indicators': snap}

    bb_pos = (price - bb_dn) / bb_width  # 0=lower, 1=upper

    # Confidence: base 50, +contraction depth(max 20), +BB extreme(max 30)
    contraction_bonus = min((0.8 - ratio) * 50, 20)

    # Near lower band + RSI low → LONG
    if bb_pos < 0.2 and rsi < 45:
        extreme_bonus = min((0.2 - bb_pos) * 60, 30)
        conf = max(50, min(95, int(50 + contraction_bonus + extreme_bonus)))
        return {'signal': 'LONG', 'confidence': conf,
                'rationale': f'CONTRACTION regime, BB lower zone (pos={bb_pos:.2f}, rsi={rsi:.1f})',
                'indicators': snap}

    # Near upper band + RSI high → SHORT
    if bb_pos > 0.8 and rsi > 55:
        extreme_bonus = min((bb_pos - 0.8) * 60, 30)
        conf = max(50, min(95, int(50 + contraction_bonus + extreme_bonus)))
        return {'signal': 'SHORT', 'confidence': conf,
                'rationale': f'CONTRACTION regime, BB upper zone (pos={bb_pos:.2f}, rsi={rsi:.1f})',
                'indicators': snap}

    # Inside BB middle during contraction
    return {'signal': 'FLAT', 'confidence': 30,
            'rationale': f'CONTRACTION regime, BB middle (pos={bb_pos:.2f})',
            'indicators': snap}
