"""
Trend-Follow strategy — EMA cross + VWAP position + Ichimoku cloud.

LONG:  all 3 bullish  → confidence 60-100
SHORT: all 3 bearish  → confidence 60-100
FLAT:  mixed signals or inside cloud
"""


def compute_signal(indicators, vol_profile, price, candles, historical_indicators=None):
    """Compute trend-follow signal from indicator snapshot."""
    if not indicators or not price:
        return {'signal': 'FLAT', 'confidence': 0,
                'rationale': 'no data', 'indicators': {}}

    ema_9 = float(indicators.get('ema_9') or 0)
    ema_21 = float(indicators.get('ema_21') or 0)
    vwap = float(indicators.get('vwap') or 0)
    cloud_top = max(float(indicators.get('ich_span_a') or 0),
                    float(indicators.get('ich_span_b') or 0))
    cloud_bot = min(float(indicators.get('ich_span_a') or 0),
                    float(indicators.get('ich_span_b') or 0))
    price = float(price)

    snap = {
        'ema_9': ema_9, 'ema_21': ema_21, 'vwap': vwap,
        'cloud_top': cloud_top, 'cloud_bot': cloud_bot, 'price': price,
    }

    if not ema_9 or not ema_21 or not vwap or not cloud_top:
        return {'signal': 'FLAT', 'confidence': 0,
                'rationale': 'indicators incomplete', 'indicators': snap}

    # Sub-signals
    ema_bull = ema_9 > ema_21
    vwap_bull = price > vwap
    cloud_bull = price > cloud_top

    ema_bear = ema_9 < ema_21
    vwap_bear = price < vwap
    cloud_bear = price < cloud_bot

    # Inside cloud → FLAT
    if cloud_bot <= price <= cloud_top:
        return {'signal': 'FLAT', 'confidence': 25,
                'rationale': f'price inside cloud [{cloud_bot:.0f}-{cloud_top:.0f}]',
                'indicators': snap}

    if ema_bull and vwap_bull and cloud_bull:
        # Confidence: base 60, +EMA distance(max 20), +cloud clearance(max 20)
        ema_dist_pct = (ema_9 - ema_21) / ema_21 * 100 if ema_21 else 0
        cloud_gap_pct = (price - cloud_top) / cloud_top * 100 if cloud_top else 0
        conf = 60 + min(ema_dist_pct * 40, 20) + min(cloud_gap_pct * 20, 20)
        conf = max(60, min(100, int(conf)))
        parts = []
        parts.append(f'EMA9>EMA21 +{ema_dist_pct:.2f}%')
        parts.append('above VWAP')
        parts.append('above cloud')
        return {'signal': 'LONG', 'confidence': conf,
                'rationale': ', '.join(parts), 'indicators': snap}

    if ema_bear and vwap_bear and cloud_bear:
        ema_dist_pct = (ema_21 - ema_9) / ema_21 * 100 if ema_21 else 0
        cloud_gap_pct = (cloud_bot - price) / cloud_bot * 100 if cloud_bot else 0
        conf = 60 + min(ema_dist_pct * 40, 20) + min(cloud_gap_pct * 20, 20)
        conf = max(60, min(100, int(conf)))
        parts = []
        parts.append(f'EMA9<EMA21 -{ema_dist_pct:.2f}%')
        parts.append('below VWAP')
        parts.append('below cloud')
        return {'signal': 'SHORT', 'confidence': conf,
                'rationale': ', '.join(parts), 'indicators': snap}

    # Mixed signals
    bulls = sum([ema_bull, vwap_bull, cloud_bull])
    bears = sum([ema_bear, vwap_bear, cloud_bear])
    return {'signal': 'FLAT', 'confidence': 30,
            'rationale': f'mixed signals ({bulls} bull / {bears} bear)',
            'indicators': snap}
