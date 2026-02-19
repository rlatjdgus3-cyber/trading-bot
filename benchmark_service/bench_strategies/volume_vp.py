"""
Volume Profile strategy — POC/VAH/VAL + volume spike.

LONG:  price near VAL + volume spike (institutional buying)
SHORT: price near VAH + volume spike (distribution selling)
FLAT:  inside Value Area, near POC, or no volume confirmation
"""


def compute_signal(indicators, vol_profile, price, candles, historical_indicators=None):
    """Compute volume/VP signal from indicator + vol_profile snapshot."""
    if not indicators or not vol_profile or not price:
        return {'signal': 'FLAT', 'confidence': 0,
                'rationale': 'no data', 'indicators': {}}

    poc = float(vol_profile.get('poc') or 0)
    vah = float(vol_profile.get('vah') or 0)
    val = float(vol_profile.get('val') or 0)
    vol_spike = bool(indicators.get('vol_spike'))
    price = float(price)

    snap = {
        'poc': poc, 'vah': vah, 'val': val,
        'vol_spike': vol_spike, 'price': price,
    }

    if not poc or not vah or not val:
        return {'signal': 'FLAT', 'confidence': 0,
                'rationale': 'VP data incomplete', 'indicators': snap}

    vp_range = vah - val
    if vp_range <= 0:
        return {'signal': 'FLAT', 'confidence': 0,
                'rationale': 'VP range zero', 'indicators': snap}

    margin = vp_range * 0.1

    # Near POC = FLAT (high liquidity, no edge)
    poc_zone = vp_range * 0.05
    if abs(price - poc) <= poc_zone:
        return {'signal': 'FLAT', 'confidence': 20,
                'rationale': f'price near POC ${poc:.0f} (no edge)',
                'indicators': snap}

    # LONG: price near VAL support + volume spike
    near_val = price <= val + margin
    # SHORT: price near VAH resistance + volume spike
    near_vah = price >= vah - margin

    if near_val and vol_spike:
        # Confidence based on distance from VAL and spike strength
        dist_pct = (val - price) / vp_range if price < val else 0
        conf = max(55, min(90, int(55 + dist_pct * 70)))
        return {'signal': 'LONG', 'confidence': conf,
                'rationale': f'price near VAL ${val:.0f}, vol spike detected',
                'indicators': snap}

    if near_vah and vol_spike:
        dist_pct = (price - vah) / vp_range if price > vah else 0
        conf = max(55, min(90, int(55 + dist_pct * 70)))
        return {'signal': 'SHORT', 'confidence': conf,
                'rationale': f'price near VAH ${vah:.0f}, vol spike detected',
                'indicators': snap}

    # Near VAL/VAH but no volume spike
    if near_val:
        return {'signal': 'FLAT', 'confidence': 35,
                'rationale': f'price near VAL ${val:.0f}, no vol confirmation',
                'indicators': snap}
    if near_vah:
        return {'signal': 'FLAT', 'confidence': 35,
                'rationale': f'price near VAH ${vah:.0f}, no vol confirmation',
                'indicators': snap}

    # Inside Value Area
    return {'signal': 'FLAT', 'confidence': 25,
            'rationale': f'inside VA [{val:.0f}-{vah:.0f}]',
            'indicators': snap}
