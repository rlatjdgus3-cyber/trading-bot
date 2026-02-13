"""
liquidity_scorer.py — Liquidity flow scorer.

Score: -100 (bearish liquidity) to +100 (bullish liquidity).

Components:
  - funding_rate:        Extreme positive -> shorts squeezed -> short-term bearish bias
  - oi_change:           OI increasing + price up -> bullish conviction
  - orderbook_imbalance: More bids than asks -> bullish pressure
"""
import os
import sys
import time
import traceback
sys.path.insert(0, '/root/trading-bot/app')
LOG_PREFIX = '[liq_scorer]'
SYMBOL = 'BTC/USDT:USDT'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _score_funding_rate(rate=None):
    '''Score funding rate: -40 to +40.
    Positive funding -> longs pay shorts -> crowded long -> slight bearish.
    Negative funding -> shorts pay longs -> crowded short -> slight bullish.
    Extreme funding -> stronger signal.'''
    if rate is None:
        return 0
    # Funding rate is typically very small (e.g., 0.0001)
    # Scale: 0.001 -> ~20 points
    score = -rate * 20000  # negative because positive funding = bearish signal
    return max(-40, min(40, int(round(score))))


def _fetch_oi_change(cur=None, exchange=None):
    '''Fetch OI change from liquidity_snapshots (1h comparison).
    Returns {oi_change_pct, current_oi}.'''
    try:
        cur.execute("""
            SELECT oi_value_usdt, ts FROM liquidity_snapshots
            WHERE symbol = %s
            ORDER BY ts DESC LIMIT 2;
        """, (SYMBOL,))
        rows = cur.fetchall()
        if len(rows) >= 2 and rows[0][0] and rows[1][0]:
            current = float(rows[0][0])
            prev = float(rows[1][0])
            if prev > 0:
                change_pct = ((current - prev) / prev) * 100
                return {
                    'oi_change_pct': change_pct,
                    'current_oi': current}
    except Exception:
        pass

    # Try live from exchange
    if exchange is not None:
        try:
            oi_data = exchange.fetch_open_interest(SYMBOL)
            current_oi = float(oi_data.get('openInterestValue', 0) or oi_data.get('openInterestAmount', 0) or 0)
            if current_oi > 0:
                cur.execute("""
                        INSERT INTO liquidity_snapshots (symbol, open_interest, oi_value_usdt)
                        VALUES (%s, %s, %s);
                    """, (SYMBOL, current_oi, current_oi))
                return {
                    'oi_change_pct': 0,
                    'current_oi': current_oi}
        except Exception:
            pass

    return {
        'oi_change_pct': 0,
        'current_oi': 0}


def _score_oi_change(oi_change_pct=None, price_change_pct=None):
    '''Score OI change: -30 to +30.
    OI up + price up -> bullish conviction -> positive.
    OI up + price down -> bearish conviction -> negative.
    OI down -> liquidation/capitulation -> reduce magnitude.'''
    if abs(oi_change_pct) < 0.5:
        return 0
    if oi_change_pct > 0:
        magnitude = min(abs(oi_change_pct) / 5, 1)
        if price_change_pct > 0:
            return int(round(magnitude * 30))
        if price_change_pct < 0:
            return -int(round(magnitude * 30))
        return int(round(magnitude * 10))
    else:
        magnitude = min(abs(oi_change_pct) / 5, 1)
        if price_change_pct > 0:
            return int(round(magnitude * 15))
        if price_change_pct < 0:
            return -int(round(magnitude * 15))
    return 0


def _fetch_orderbook_imbalance(exchange=None, depth=20):
    '''Fetch orderbook bid/ask imbalance from exchange.
    Returns imbalance ratio: >1 = more bids, <1 = more asks.'''
    if not exchange:
        return None
    try:
        ob = exchange.fetch_order_book(SYMBOL, limit=depth)
        bids = ob.get('bids', [])
        asks = ob.get('asks', [])
        if not bids or not asks:
            return None
        bid_depth = sum(b[0] * b[1] for b in bids)
        ask_depth = sum(a[0] * a[1] for a in asks)
        if ask_depth <= 0:
            return None
        return bid_depth / ask_depth
    except Exception:
        return None


def _score_orderbook(imbalance=None):
    '''Score orderbook imbalance: -30 to +30.
    imbalance > 1 -> more bids -> bullish.
    imbalance < 1 -> more asks -> bearish.'''
    if imbalance is None:
        return 0
    # Scale: 1.3 -> +15, 0.7 -> -15
    deviation = imbalance - 1.0
    score = deviation * 50  # 0.3 deviation -> 15 points
    return max(-30, min(30, int(round(score))))


def _get_price_change_1h(cur=None):
    '''Get BTC price change % over last hour.'''
    try:
        cur.execute("""
            SELECT c FROM candles
            WHERE symbol = %s AND tf = '1m'
            ORDER BY ts DESC LIMIT 1;
        """, (SYMBOL,))
        latest = cur.fetchone()
        cur.execute("""
            SELECT c FROM candles
            WHERE symbol = %s AND tf = '1m'
              AND ts <= now() - interval '1 hour'
            ORDER BY ts DESC LIMIT 1;
        """, (SYMBOL,))
        hour_ago = cur.fetchone()
        if latest and hour_ago and latest[0] and hour_ago[0]:
            return ((float(latest[0]) - float(hour_ago[0])) / float(hour_ago[0])) * 100
    except Exception:
        pass
    return 0


def compute(cur=None, exchange=None):
    '''Compute liquidity flow score.

    Args:
        cur: Database cursor
        exchange: ccxt exchange instance (optional, for live data)

    Returns:
        {
            "score": int (-100 to +100),
            "components": {
                "funding_rate": int,
                "oi_change": int,
                "orderbook": int,
            },
            "raw": {
                "funding_rate": float,
                "oi_change_pct": float,
                "orderbook_imbalance": float,
            },
        }
    '''
    funding_rate = None
    if exchange:
        try:
            funding_data = exchange.fetch_funding_rate(SYMBOL)
            funding_rate = float(funding_data.get('fundingRate', 0))
        except Exception:
            pass

    if funding_rate is None:
        try:
            cur.execute("""
                SELECT funding_rate FROM liquidity_snapshots
                WHERE symbol = %s AND funding_rate IS NOT NULL
                ORDER BY ts DESC LIMIT 1;
            """, (SYMBOL,))
            row = cur.fetchone()
            if row and row[0]:
                funding_rate = float(row[0])
        except Exception:
            pass

    funding_score = _score_funding_rate(funding_rate)

    oi_data = _fetch_oi_change(cur, exchange)
    price_change = _get_price_change_1h(cur)
    oi_score = _score_oi_change(oi_data['oi_change_pct'], price_change)

    imbalance = _fetch_orderbook_imbalance(exchange)
    ob_score = _score_orderbook(imbalance)

    total = funding_score + oi_score + ob_score
    total = max(-100, min(100, total))

    return {
        'score': total,
        'components': {
            'funding_rate': funding_score,
            'oi_change': oi_score,
            'orderbook': ob_score,
        },
        'raw': {
            'funding_rate': funding_rate,
            'oi_change_pct': oi_data['oi_change_pct'],
            'orderbook_imbalance': imbalance,
        }}


if __name__ == '__main__':
    import json
    import psycopg2
    conn = None
    try:
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
