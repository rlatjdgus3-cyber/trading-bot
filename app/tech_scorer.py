"""
tech_scorer.py — Technical analysis scorer.

Single-axis score: -100 (strong short) to +100 (strong long).

Components (weighted to sum ~100 max per side):
  - Ichimoku tenkan/kijun cross:  +/-30
  - Bollinger Band position:      +/-20
  - RSI (14):                     +/-15
  - Volume spike:                 +/-10
  - Momentum (5-min):             +/-10
  - Structure (MA-50 vs MA-200):  +/-15
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
               rsi_14, ma_50, ma_200, vol, vol_ma20, atr_14
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
    '''Ichimoku tenkan/kijun cross: -30 to +30.'''
    tenkan = ind.get('ich_tenkan')
    kijun = ind.get('ich_kijun')
    if tenkan is None or kijun is None:
        return 0
    if kijun == 0:
        return 0
    diff_pct = (tenkan - kijun) / kijun * 100
    # Scale: +-0.5% -> +-30
    score = diff_pct / 0.5 * 30
    return max(-30, min(30, int(round(score))))


def _score_bollinger(ind=None, price=None):
    '''Bollinger Band position: -20 to +20.
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
    # Position: -1 (at lower) to +1 (at upper)
    position = (price - bb_mid) / (band_width / 2)
    # Below mid = positive (long, oversold), above mid = negative (short, overbought)
    score = -position * 20
    return max(-20, min(20, int(round(score))))


def _score_rsi(ind=None):
    '''RSI-14 score: -15 to +15.
    RSI < 30 -> oversold -> long.
    RSI > 70 -> overbought -> short.
    40-60 -> neutral.'''
    rsi = ind.get('rsi_14')
    if rsi is None:
        return 0
    if rsi < 30:
        return int(round((30 - rsi) / 30 * 15))
    if rsi > 70:
        return -int(round((rsi - 70) / 30 * 15))
    if rsi < 40:
        return int(round((40 - rsi) / 10 * 5))
    if rsi > 60:
        return -int(round((rsi - 60) / 10 * 5))
    return 0


def _score_volume(ind=None):
    '''Volume spike: -10 to +10.
    Volume spike amplifies the existing trend direction.'''
    if not ind.get('vol_spike'):
        return 0
    # Volume spike gives a small boost (direction determined by other indicators)
    return 5


def _score_momentum(closes=None):
    '''5-min momentum: -10 to +10.'''
    if len(closes) < 2:
        return 0
    first = closes[0]
    last = closes[-1]
    if first == 0:
        return 0
    change_pct = ((last - first) / first) * 100
    strength = min(abs(change_pct) / 0.5, 1) * 10
    if change_pct > 0:
        return int(round(strength))
    return -int(round(strength))


def _score_structure(ind=None, price=None):
    '''MA-50 vs MA-200 structure: -15 to +15.
    Golden cross (MA50 > MA200 + price above both) -> long.
    Death cross (MA50 < MA200 + price below both) -> short.'''
    ma50 = ind.get('ma_50')
    ma200 = ind.get('ma_200')
    if ma50 is None or ma200 is None:
        return 0
    if price is None:
        price = 0

    score = 0
    # MA cross
    if ma50 > ma200:
        score += 8  # golden cross
        if price and price > ma50:
            score += 7  # price confirms above both
    elif ma50 < ma200:
        score -= 8  # death cross
        if price and price < ma50:
            score -= 7  # price confirms below both

    return max(-15, min(15, score))


def compute(cur=None):
    '''Compute technical score.

    Returns:
        {
            "score": int (-100 to +100),
            "components": {
                "ichimoku": int,
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

    ich = _score_ichimoku(ind)
    bb = _score_bollinger(ind, price)
    rsi = _score_rsi(ind)
    vol = _score_volume(ind)
    mom = _score_momentum(closes)
    struct = _score_structure(ind, price)

    raw_total = ich + bb + rsi + vol + mom + struct
    if ind.get('vol_spike') and raw_total != 0:
        boost = 5 if raw_total > 0 else -5
        raw_total += boost

    total = max(-100, min(100, raw_total))

    return {
        'score': total,
        'components': {
            'ichimoku': ich,
            'bollinger': bb,
            'rsi': rsi,
            'volume': vol,
            'momentum': mom,
            'structure': struct},
        'price': price,
        'indicators': ind}


if __name__ == '__main__':
    import json
    conn = None
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', '5432')),
            dbname=os.getenv('DB_NAME', 'trading'),
            user=os.getenv('DB_USER', 'bot'),
            password=os.getenv('DB_PASS', 'botpass'),
            connect_timeout=10,
            options='-c statement_timeout=30000')
        conn.autocommit = True
        with conn.cursor() as cur:
            result = compute(cur)
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    except Exception:
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
