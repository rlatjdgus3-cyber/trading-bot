"""
Mean-Reversion strategy — Bollinger Band + RSI.

LONG:  price at/below BB lower + RSI oversold (<35)
SHORT: price at/above BB upper + RSI overbought (>65)
FLAT:  middle zone
"""


def compute_signal(indicators, vol_profile, price, candles, historical_indicators=None):
    """Compute mean-reversion signal from indicator snapshot."""
    if not indicators or not price:
        return {'signal': 'FLAT', 'confidence': 0,
                'rationale': 'no data', 'indicators': {}}

    bb_up = float(indicators.get('bb_up') or 0)
    bb_dn = float(indicators.get('bb_dn') or 0)
    bb_mid = float(indicators.get('bb_mid') or 0)
    rsi = float(indicators.get('rsi_14') or 50)
    price = float(price)

    snap = {
        'bb_up': bb_up, 'bb_dn': bb_dn, 'bb_mid': bb_mid,
        'rsi_14': rsi, 'price': price,
    }

    if not bb_up or not bb_dn or not bb_mid:
        return {'signal': 'FLAT', 'confidence': 0,
                'rationale': 'BB indicators incomplete', 'indicators': snap}

    bb_width = bb_up - bb_dn
    if bb_width <= 0:
        return {'signal': 'FLAT', 'confidence': 0,
                'rationale': 'BB width zero', 'indicators': snap}

    # LONG: price near/below BB lower + RSI oversold
    near_lower = price <= bb_dn * 1.002
    rsi_oversold = rsi < 35

    # SHORT: price near/above BB upper + RSI overbought
    near_upper = price >= bb_up * 0.998
    rsi_overbought = rsi > 65

    if near_lower and rsi_oversold:
        # Confidence: base 50, +RSI extremity(max 25), +BB penetration depth(max 25)
        rsi_extreme = min((35 - rsi) / 35 * 25, 25)
        bb_depth = min((bb_dn - price) / bb_width * 50, 25) if price < bb_dn else 0
        conf = max(50, min(100, int(50 + rsi_extreme + bb_depth)))
        return {'signal': 'LONG', 'confidence': conf,
                'rationale': f'RSI={rsi:.1f} oversold, price near BB lower ${bb_dn:.0f}',
                'indicators': snap}

    if near_upper and rsi_overbought:
        rsi_extreme = min((rsi - 65) / 35 * 25, 25)
        bb_depth = min((price - bb_up) / bb_width * 50, 25) if price > bb_up else 0
        conf = max(50, min(100, int(50 + rsi_extreme + bb_depth)))
        return {'signal': 'SHORT', 'confidence': conf,
                'rationale': f'RSI={rsi:.1f} overbought, price near BB upper ${bb_up:.0f}',
                'indicators': snap}

    # FLAT: middle zone
    bb_pos = (price - bb_dn) / bb_width * 100  # 0=lower, 100=upper
    return {'signal': 'FLAT', 'confidence': 30,
            'rationale': f'RSI={rsi:.1f}, price in BB middle zone ({bb_pos:.0f}%)',
            'indicators': snap}
