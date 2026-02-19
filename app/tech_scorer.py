"""
tech_scorer.py — Technical analysis scorer (scalp-optimized v2).

Single-axis score: -100 (strong short) to +100 (strong long).

Components (weighted to sum ~100 max per side):
  - EMA cross (9/21/50):          +/-25  (PRIMARY)
  - VWAP position:                +/-15  (PRIMARY)
  - Ichimoku tenkan/kijun cross:  +/-15  (Secondary)
  - Cloud position:               +/-10  (Secondary)
  - Bollinger Band position:      +/-10  (Secondary)
  - RSI (14):                     +/-10  (Secondary)
  - Volume spike:                 +/-5   (Amplifier)
  - Momentum (5-min):             +/-5   (Secondary)
  - Structure (MA-50 vs MA-200):  +/-5   (Secondary)
"""
import os
import sys
import traceback
sys.path.insert(0, '/root/trading-bot/app')
LOG_PREFIX = '[tech_scorer]'
SYMBOL = 'BTC/USDT:USDT'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _fetch_latest_indicator(cur=None):
    '''Fetch latest 1m indicator row.'''
    cur.execute("""
        SELECT ich_tenkan, ich_kijun, bb_mid, bb_up, bb_dn, vol_spike,
               rsi_14, ma_50, ma_200, vol, vol_ma20, atr_14,
               ema_9, ema_21, ema_50, vwap, ich_span_a, ich_span_b
        FROM indicators
        WHERE symbol = %s AND tf = '1m'
        ORDER BY ts DESC LIMIT 1;
    """, (SYMBOL,))
    row = cur.fetchone()
    if not row:
        return None
    return {
        'ich_tenkan': float(row[0]) if row[0] else None,
        'ich_kijun': float(row[1]) if row[1] else None,
        'bb_mid': float(row[2]) if row[2] else None,
        'bb_up': float(row[3]) if row[3] else None,
        'bb_dn': float(row[4]) if row[4] else None,
        'vol_spike': bool(row[5]) if row[5] is not None else False,
        'rsi_14': float(row[6]) if row[6] else None,
        'ma_50': float(row[7]) if row[7] else None,
        'ma_200': float(row[8]) if row[8] else None,
        'vol': float(row[9]) if row[9] else None,
        'vol_ma20': float(row[10]) if row[10] else None,
        'atr_14': float(row[11]) if row[11] else None,
        'ema_9': float(row[12]) if row[12] else None,
        'ema_21': float(row[13]) if row[13] else None,
        'ema_50': float(row[14]) if row[14] else None,
        'vwap': float(row[15]) if row[15] else None,
        'ich_span_a': float(row[16]) if row[16] else None,
        'ich_span_b': float(row[17]) if row[17] else None,
    }


def _fetch_latest_price(cur=None):
    '''Fetch latest close price from candles.'''
    cur.execute("""
        SELECT c FROM candles
        WHERE symbol = %s AND tf = '1m'
        ORDER BY ts DESC LIMIT 1;
    """, (SYMBOL,))
    row = cur.fetchone()
    if row and row[0]:
        return float(row[0])
    return None


def _fetch_recent_closes(cur=None, count=None):
    '''Fetch last N close prices from candles (oldest first).'''
    cur.execute("""
        SELECT c FROM (
            SELECT c, ts FROM candles
            WHERE symbol = %s AND tf = '1m'
            ORDER BY ts DESC LIMIT %s
        ) sub ORDER BY ts ASC;
    """, (SYMBOL, count))
    return [float(r[0]) for r in cur.fetchall() if r[0]]


def _score_ichimoku(ind=None):
    '''Ichimoku tenkan/kijun cross: -15 to +15 (was ±30).'''
    tenkan = ind.get('ich_tenkan')
    kijun = ind.get('ich_kijun')
    if tenkan is None or kijun is None:
        return 0
    if kijun == 0:
        return 0
    diff_pct = (tenkan - kijun) / kijun * 100
    # Scale: +-0.5% -> +-15
    score = diff_pct / 0.5 * 15
    return max(-15, min(15, int(round(score))))


def _score_bollinger(ind=None, price=None):
    '''Bollinger Band position: -10 to +10 (was ±20).
    Price below mid -> long favored (positive).
    Price above mid -> mean-reversion short bias (negative).'''
    bb_mid = ind.get('bb_mid')
    bb_up = ind.get('bb_up')
    bb_dn = ind.get('bb_dn')
    if bb_mid is None or bb_up is None or bb_dn is None or price is None:
        return 0
    band_width = bb_up - bb_dn
    if band_width <= 0:
        return 0
    position = (price - bb_mid) / (band_width / 2)
    score = -position * 10
    return max(-10, min(10, int(round(score))))


def _score_rsi(ind=None):
    '''RSI-14 score: -10 to +10 (was ±15).
    RSI < 30 -> oversold -> long.
    RSI > 70 -> overbought -> short.
    40-60 -> neutral.'''
    rsi = ind.get('rsi_14')
    if rsi is None:
        return 0
    if rsi < 30:
        return int(round((30 - rsi) / 30 * 10))
    if rsi > 70:
        return -int(round((rsi - 70) / 30 * 10))
    if rsi < 40:
        return int(round((40 - rsi) / 10 * 3))
    if rsi > 60:
        return -int(round((rsi - 60) / 10 * 3))
    return 0


def _score_volume(ind=None):
    '''Volume spike: -5 to +5 (was ±10).
    Volume spike amplifies the existing trend direction.'''
    if not ind.get('vol_spike'):
        return 0
    return 3


def _score_momentum(closes=None):
    '''5-min momentum: -5 to +5 (was ±10).'''
    if len(closes) < 2:
        return 0
    first = closes[0]
    last = closes[-1]
    if first == 0:
        return 0
    change_pct = ((last - first) / first) * 100
    strength = min(abs(change_pct) / 0.5, 1) * 5
    if change_pct > 0:
        return int(round(strength))
    return -int(round(strength))


def _score_structure(ind=None, price=None):
    '''MA-50 vs MA-200 structure: -5 to +5 (was ±15).
    Golden cross (MA50 > MA200 + price above both) -> long.
    Death cross (MA50 < MA200 + price below both) -> short.'''
    ma50 = ind.get('ma_50')
    ma200 = ind.get('ma_200')
    if ma50 is None or ma200 is None:
        return 0
    if price is None:
        price = 0

    score = 0
    if ma50 > ma200:
        score += 3
        if price and price > ma50:
            score += 2
    elif ma50 < ma200:
        score -= 3
        if price and price < ma50:
            score -= 2

    return max(-5, min(5, score))


def _score_ema_cross(ind=None):
    '''EMA cross (9/21/50): -25 to +25 (PRIMARY signal).
    EMA9 vs EMA21 distance → 0.3% = ±20.
    EMA50 alignment → additional ±5.'''
    ema9 = ind.get('ema_9')
    ema21 = ind.get('ema_21')
    if ema9 is None or ema21 is None:
        return 0
    if ema21 == 0:
        return 0
    diff_pct = (ema9 - ema21) / ema21 * 100
    # Scale: +-0.3% -> +-20
    score = diff_pct / 0.3 * 20
    score = max(-20, min(20, score))

    # EMA9 vs EMA21 alignment bonus
    if ema9 > ema21:
        score += 5
    elif ema9 < ema21:
        score -= 5

    return max(-25, min(25, int(round(score))))


def _score_vwap(ind=None, price=None):
    '''VWAP position: -15 to +15.
    Price above VWAP -> long bias, below -> short bias.
    0.5% distance = ±15.'''
    vwap = ind.get('vwap')
    if vwap is None or price is None or vwap == 0:
        return 0
    diff_pct = (price - vwap) / vwap * 100
    score = diff_pct / 0.5 * 15
    return max(-15, min(15, int(round(score))))


def _score_cloud_position(ind=None, price=None):
    '''Ichimoku cloud position: -10 to +10.
    Price above cloud -> +10, below -> -10, inside -> 0.'''
    span_a = ind.get('ich_span_a')
    span_b = ind.get('ich_span_b')
    if span_a is None or span_b is None or price is None:
        return 0
    cloud_top = max(span_a, span_b)
    cloud_bot = min(span_a, span_b)
    if price > cloud_top:
        return 10
    elif price < cloud_bot:
        return -10
    return 0


def compute(cur=None):
    '''Compute technical score (scalp-optimized v2).

    Returns:
        {
            "score": int (-100 to +100),
            "components": {
                "ema_cross": int,
                "vwap": int,
                "ichimoku": int,
                "cloud_position": int,
                "bollinger": int,
                "rsi": int,
                "volume": int,
                "momentum": int,
                "structure": int,
            },
            "price": float | None,
            "indicators": dict,
        }
    '''
    ind = _fetch_latest_indicator(cur)
    if not ind:
        return {
            'score': 0,
            'components': {},
            'price': None,
            'indicators': {},
            'error': 'no indicator data'}

    price = _fetch_latest_price(cur)
    closes = _fetch_recent_closes(cur, 5)

    ema_c = _score_ema_cross(ind)
    vwap_s = _score_vwap(ind, price)
    ich = _score_ichimoku(ind)
    cloud = _score_cloud_position(ind, price)
    bb = _score_bollinger(ind, price)
    rsi = _score_rsi(ind)
    vol = _score_volume(ind)
    mom = _score_momentum(closes)
    struct = _score_structure(ind, price)

    raw_total = ema_c + vwap_s + ich + cloud + bb + rsi + vol + mom + struct
    if ind.get('vol_spike') and raw_total != 0:
        boost = 3 if raw_total > 0 else -3
        raw_total += boost

    total = max(-100, min(100, raw_total))

    return {
        'score': total,
        'components': {
            'ema_cross': ema_c,
            'vwap': vwap_s,
            'ichimoku': ich,
            'cloud_position': cloud,
            'bollinger': bb,
            'rsi': rsi,
            'volume': vol,
            'momentum': mom,
            'structure': struct},
        'price': price,
        'indicators': ind}


if __name__ == '__main__':
    import json
    from db_config import get_conn
    conn = None
    try:
        conn = get_conn(autocommit=True)
        with conn.cursor() as cur:
            result = compute(cur)
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    except Exception:
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
