"""
direction_scorer.py — Direction scoring engine (backward-compatible wrapper).

Now delegates to score_engine.compute_total() for unified 5-axis scoring.
Falls back to legacy logic (_compute_scores_legacy) if score_engine fails.

compute_scores() -> {long_score, short_score, dominant_side, confidence, context}
"""
import os
import sys
import traceback
sys.path.insert(0, '/root/trading-bot/app')
LOG_PREFIX = '[direction_scorer]'
SYMBOL = 'BTC/USDT:USDT'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _db_conn():
    from db_config import get_conn
    return get_conn()


def compute_scores():
    '''Compute directional scores via unified score engine.

    Returns legacy-compatible format:
        {
            "long_score": int (0-100),
            "short_score": int (0-100),
            "dominant_side": "LONG" | "SHORT",
            "confidence": int (absolute diff),
            "context": dict (breakdown details),
            "unified": dict (full score_engine result, if available),
        }
    '''
    try:
        import score_engine
        result = score_engine.compute_total()
        return {
            'long_score': result['long_score'],
            'short_score': result['short_score'],
            'dominant_side': result['dominant_side'],
            'confidence': result['confidence'],
            'context': result.get('context', {}),
            'unified': result}
    except Exception as e:
        _log(f'score_engine failed, using legacy: {e}')
        traceback.print_exc()
        return _compute_scores_legacy()


def _fetch_latest_indicator(cur=None):
    '''Fetch latest 1m indicator row.'''
    cur.execute("""
        SELECT ich_tenkan, ich_kijun, bb_mid, bb_up, bb_dn, vol_spike
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


def _fetch_news_sentiment(cur=None):
    '''Fetch recent news sentiment from news table (last 1 hour).'''
    try:
        cur.execute("""
            SELECT impact_score, summary FROM news
            WHERE ts >= now() - interval '1 hour'
              AND impact_score IS NOT NULL
            ORDER BY ts DESC LIMIT 20;
        """)
        rows = cur.fetchall()
        bullish = 0
        bearish = 0
        for score, summary in rows:
            s = int(score) if score else 0
            text = (summary or '').lower()
            if any(k in text for k in ('상승', '강세', 'bull', 'pump', 'rally', 'surge')):
                bullish += s
            elif any(k in text for k in ('하락', '약세', 'bear', 'crash', 'drop', 'fear')):
                bearish += s
            else:
                bullish += s // 2
                bearish += s // 2
        return {'bullish_score': bullish, 'bearish_score': bearish}
    except Exception:
        return {'bullish_score': 0, 'bearish_score': 0}


def _score_ichimoku(ind=None):
    '''Ichimoku tenkan/kijun cross: 0-30 each side.'''
    tenkan = ind.get('ich_tenkan')
    kijun = ind.get('ich_kijun')
    if tenkan is None or kijun is None:
        return (15, 15)
    if tenkan > kijun:
        diff = min(abs(tenkan - kijun) / kijun * 100, 1) if kijun else 0
        boost = int(round(diff * 15))
        return (min(15 + boost, 30), max(15 - boost, 0))
    elif tenkan < kijun:
        diff = min(abs(kijun - tenkan) / kijun * 100, 1) if kijun else 0
        boost = int(round(diff * 15))
        return (max(15 - boost, 0), min(15 + boost, 30))
    return (15, 15)


def _score_bollinger(ind=None, price=None):
    '''Bollinger Band position: 0-25 each side.'''
    bb_mid = ind.get('bb_mid')
    bb_up = ind.get('bb_up')
    bb_dn = ind.get('bb_dn')
    if bb_mid is None or bb_up is None or bb_dn is None or price is None:
        return (12, 12)
    band_width = bb_up - bb_dn
    if band_width <= 0:
        return (12, 12)
    position = (price - bb_dn) / band_width  # 0 = at lower, 1 = at upper
    if position < 0.3:
        boost = int(round((0.3 - position) / 0.3 * 13))
        return (min(12 + boost, 25), max(12 - boost, 0))
    elif position > 0.7:
        boost = int(round((position - 0.7) / 0.3 * 13))
        return (max(12 - boost, 0), min(12 + boost, 25))
    return (12, 12)


def _score_volume(ind=None):
    '''Volume spike: 0-15 each side.'''
    if not ind.get('vol_spike'):
        return (7, 7)
    return (11, 11)


def _score_news(sentiment=None):
    '''News sentiment: 0-20 each side.'''
    bull = sentiment.get('bullish_score', 0)
    bear = sentiment.get('bearish_score', 0)
    total = bull + bear
    if total == 0:
        return (10, 10)
    bull_ratio = bull / total
    bear_ratio = bear / total
    long_score = int(round(bull_ratio * 20))
    short_score = int(round(bear_ratio * 20))
    return (min(long_score, 20), min(short_score, 20))


def _score_momentum(closes=None):
    '''5-min momentum: 0-10 each side.'''
    if len(closes) < 2:
        return (5, 5)
    first = closes[0]
    last = closes[-1]
    if first == 0:
        return (5, 5)
    change_pct = ((last - first) / first) * 100
    strength = min(abs(change_pct) / 0.5, 1) * 5
    strength = int(round(strength))
    if change_pct > 0:
        return (min(5 + strength, 10), max(5 - strength, 0))
    return (max(5 - strength, 0), min(5 + strength, 10))


def _compute_scores_legacy():
    '''Legacy scoring logic (fallback when score_engine unavailable).'''
    conn = None
    try:
        conn = _db_conn()
        conn.autocommit = True
        with conn.cursor() as cur:
            ind = _fetch_latest_indicator(cur)
            if not ind:
                return {
                    'long_score': 50, 'short_score': 50,
                    'dominant_side': 'LONG', 'confidence': 0,
                    'context': {'error': 'no indicators'}}

            price = _fetch_latest_price(cur)
            closes = _fetch_recent_closes(cur, 5)

            (ich_l, ich_s) = _score_ichimoku(ind)
            (bb_l, bb_s) = _score_bollinger(ind, price)
            (vol_l, vol_s) = _score_volume(ind)
            (mom_l, mom_s) = _score_momentum(closes)

            long_score = ich_l + bb_l + vol_l + mom_l
            short_score = ich_s + bb_s + vol_s + mom_s

            long_score = max(0, min(100, long_score))
            short_score = max(0, min(100, short_score))

            dominant = 'LONG' if long_score >= short_score else 'SHORT'
            confidence = abs(long_score - short_score)

            return {
                'long_score': long_score,
                'short_score': short_score,
                'dominant_side': dominant,
                'confidence': confidence,
                'context': {
                    'ichimoku': (ich_l, ich_s),
                    'bollinger': (bb_l, bb_s),
                    'volume': (vol_l, vol_s),
                    'momentum': (mom_l, mom_s),
                }}

    except Exception as e:
        _log(f'legacy scoring error: {e}')
        traceback.print_exc()
        return {
            'long_score': 50, 'short_score': 50,
            'dominant_side': 'LONG', 'confidence': 0,
            'context': {'error': str(e)}}
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == '__main__':
    result = compute_scores()
    import json
    print(json.dumps(result, ensure_ascii=False, indent=2))
