"""
macro_trace_computer.py — BTC reaction trace for news events.

Computes price reaction (ret_30m, ret_2h, ret_24h), volume metrics,
and risk label for each news item. Stores in macro_trace table.

Candles (1m) preferred → market_ohlcv (5m) fallback → None.
Partial traces allowed: ret_30m first, ret_2h/24h filled later.
"""
import os
import sys
import traceback

sys.path.insert(0, '/root/trading-bot/app')

LOG_PREFIX = '[macro_trace]'
SYMBOL = 'BTC/USDT:USDT'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _get_price_at(cur, ts):
    """Get BTC price at ts. candles(1m) first, market_ohlcv(5m) fallback."""
    # Try 1m candles first
    try:
        cur.execute("""
            SELECT c FROM candles
            WHERE symbol = %s AND tf = '1m' AND ts <= %s
            ORDER BY ts DESC LIMIT 1;
        """, (SYMBOL, ts))
        row = cur.fetchone()
        if row:
            return float(row[0])
    except Exception:
        pass
    # Fallback to 5m market_ohlcv
    try:
        cur.execute("""
            SELECT c FROM market_ohlcv
            WHERE symbol = %s AND tf = '5m' AND ts <= %s
            ORDER BY ts DESC LIMIT 1;
        """, (SYMBOL, ts))
        row = cur.fetchone()
        if row:
            return float(row[0])
    except Exception:
        pass
    return None


def _get_price_after(cur, ts, minutes):
    """Get BTC price at ts + N minutes. candles(1m) first, market_ohlcv(5m) fallback."""
    interval_str = f'{minutes} minutes'
    # Try 1m candles
    try:
        cur.execute("""
            SELECT c FROM candles
            WHERE symbol = %s AND tf = '1m'
              AND ts >= %s + %s::interval
            ORDER BY ts ASC LIMIT 1;
        """, (SYMBOL, ts, interval_str))
        row = cur.fetchone()
        if row:
            return float(row[0])
    except Exception:
        pass
    # Fallback to 5m
    try:
        cur.execute("""
            SELECT c FROM market_ohlcv
            WHERE symbol = %s AND tf = '5m'
              AND ts >= %s + %s::interval
            ORDER BY ts ASC LIMIT 1;
        """, (SYMBOL, ts, interval_str))
        row = cur.fetchone()
        if row:
            return float(row[0])
    except Exception:
        pass
    return None


def _compute_vol_metrics(cur, ts_news):
    """Compute volume metrics around news event.

    vol_2h: avg volume in 2h after news
    vol_baseline: avg volume in 24h before news
    spike_zscore: (vol_2h - vol_baseline) / stddev(24h volume)

    Returns (vol_2h, vol_baseline, spike_zscore).
    """
    vol_2h = None
    vol_baseline = None
    spike_zscore = None

    # vol_2h: avg volume 2h after news (try candles first, then market_ohlcv)
    for table, col in [('candles', 'v'), ('market_ohlcv', 'v')]:
        try:
            tf = '1m' if table == 'candles' else '5m'
            cur.execute(f"""
                SELECT AVG({col}) FROM {table}
                WHERE symbol = %s AND tf = %s
                  AND ts >= %s AND ts < %s + interval '2 hours';
            """, (SYMBOL, tf, ts_news, ts_news))
            row = cur.fetchone()
            if row and row[0]:
                vol_2h = float(row[0])
                break
        except Exception:
            continue

    # vol_baseline: avg + stddev of volume 24h before news
    vol_std = None
    for table, col in [('candles', 'v'), ('market_ohlcv', 'v')]:
        try:
            tf = '1m' if table == 'candles' else '5m'
            cur.execute(f"""
                SELECT AVG({col}), STDDEV({col}) FROM {table}
                WHERE symbol = %s AND tf = %s
                  AND ts >= %s - interval '24 hours' AND ts < %s;
            """, (SYMBOL, tf, ts_news, ts_news))
            row = cur.fetchone()
            if row and row[0]:
                vol_baseline = float(row[0])
                vol_std = float(row[1]) if row[1] else None
                break
        except Exception:
            continue

    # spike_zscore
    if vol_2h is not None and vol_baseline is not None and vol_std and vol_std > 0:
        spike_zscore = round((vol_2h - vol_baseline) / vol_std, 2)

    return vol_2h, vol_baseline, spike_zscore


def _classify_regime(cur, ts_news):
    """Get regime state at ts_news from score_history."""
    try:
        cur.execute("""
            SELECT market_regime_score FROM score_history
            WHERE ts <= %s
            ORDER BY ts DESC LIMIT 1;
        """, (ts_news,))
        row = cur.fetchone()
        if row and row[0] is not None:
            score = float(row[0])
            if score > 30:
                return 'bullish'
            elif score < -30:
                return 'bearish'
            return 'neutral'
    except Exception:
        pass
    return None


def _classify_label(btc_ret_2h, spike_zscore):
    """Classify as risk-on / risk-off / mixed."""
    if btc_ret_2h is None:
        return None
    ret = float(btc_ret_2h)
    z = float(spike_zscore) if spike_zscore is not None else 0
    if ret > 0 and z < 2.0:
        return 'risk-on'
    if ret < 0 and z >= 2.0:
        return 'risk-off'
    return 'mixed'


def compute_trace_for_news(cur, news_id, ts_news):
    """Compute macro_trace for a single news item. Partial results OK."""
    try:
        btc_price = _get_price_at(cur, ts_news)
        if btc_price is None:
            return None

        # Check how much time has passed
        cur.execute("SELECT EXTRACT(EPOCH FROM (now() - %s::timestamptz)) / 60", (ts_news,))
        age_min = float(cur.fetchone()[0])

        ret_30m = None
        ret_2h = None
        ret_24h = None

        if age_min >= 30:
            p30 = _get_price_after(cur, ts_news, 30)
            if p30 and btc_price:
                ret_30m = round(((p30 - btc_price) / btc_price) * 100, 4)

        if age_min >= 120:
            p2h = _get_price_after(cur, ts_news, 120)
            if p2h and btc_price:
                ret_2h = round(((p2h - btc_price) / btc_price) * 100, 4)

        if age_min >= 1440:
            p24h = _get_price_after(cur, ts_news, 1440)
            if p24h and btc_price:
                ret_24h = round(((p24h - btc_price) / btc_price) * 100, 4)

        vol_2h, vol_baseline, spike_zscore = (None, None, None)
        if age_min >= 120:
            vol_2h, vol_baseline, spike_zscore = _compute_vol_metrics(cur, ts_news)

        regime = _classify_regime(cur, ts_news)
        label = _classify_label(ret_2h, spike_zscore)

        cur.execute("""
            INSERT INTO macro_trace
                (news_id, ts_news, btc_price_at,
                 btc_ret_30m, btc_ret_2h, btc_ret_24h,
                 vol_2h, vol_baseline, spike_zscore,
                 regime_at_time, label, computed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (news_id) DO UPDATE SET
                btc_price_at = COALESCE(EXCLUDED.btc_price_at, macro_trace.btc_price_at),
                btc_ret_30m  = COALESCE(EXCLUDED.btc_ret_30m, macro_trace.btc_ret_30m),
                btc_ret_2h   = COALESCE(EXCLUDED.btc_ret_2h, macro_trace.btc_ret_2h),
                btc_ret_24h  = COALESCE(EXCLUDED.btc_ret_24h, macro_trace.btc_ret_24h),
                vol_2h       = COALESCE(EXCLUDED.vol_2h, macro_trace.vol_2h),
                vol_baseline = COALESCE(EXCLUDED.vol_baseline, macro_trace.vol_baseline),
                spike_zscore = COALESCE(EXCLUDED.spike_zscore, macro_trace.spike_zscore),
                regime_at_time = COALESCE(EXCLUDED.regime_at_time, macro_trace.regime_at_time),
                label        = COALESCE(EXCLUDED.label, macro_trace.label),
                computed_at  = now();
        """, (news_id, ts_news, btc_price,
              ret_30m, ret_2h, ret_24h,
              vol_2h, vol_baseline, spike_zscore,
              regime, label))

        return {
            'news_id': news_id,
            'btc_price_at': btc_price,
            'btc_ret_30m': ret_30m,
            'btc_ret_2h': ret_2h,
            'btc_ret_24h': ret_24h,
            'vol_2h': vol_2h,
            'vol_baseline': vol_baseline,
            'spike_zscore': spike_zscore,
            'regime_at_time': regime,
            'label': label,
        }
    except Exception as e:
        _log(f'compute_trace error news_id={news_id}: {e}')
        traceback.print_exc()
        return None


def compute_pending_traces(cur, lookback_hours=6):
    """Compute traces for recent news not yet fully computed.

    Processes news in 3 tiers:
    - 30min+ elapsed: compute ret_30m
    - 2h+ elapsed: compute ret_2h + vol metrics
    - 24h+ elapsed: compute ret_24h
    """
    computed = 0
    try:
        # Find news items needing trace computation
        cur.execute("""
            SELECT n.id, n.ts
            FROM news n
            LEFT JOIN macro_trace mt ON mt.news_id = n.id
            WHERE n.ts >= now() - %s * interval '1 hour'
              AND n.ts <= now() - interval '30 minutes'
              AND n.impact_score > 0
              AND (mt.id IS NULL
                   OR (mt.btc_ret_2h IS NULL AND n.ts <= now() - interval '2 hours')
                   OR (mt.btc_ret_24h IS NULL AND n.ts <= now() - interval '24 hours'))
            ORDER BY n.ts DESC
            LIMIT 50;
        """, (lookback_hours,))
        rows = cur.fetchall()

        for news_id, ts_news in rows:
            result = compute_trace_for_news(cur, news_id, ts_news)
            if result:
                computed += 1

    except Exception as e:
        _log(f'compute_pending_traces error: {e}')
        traceback.print_exc()

    if computed > 0:
        _log(f'computed {computed} traces')
    return computed


def get_traces_for_report(cur, news_ids):
    """Batch fetch traces for report. Returns {news_id: trace_dict}."""
    if not news_ids:
        return {}
    traces = {}
    try:
        cur.execute("""
            SELECT news_id, btc_price_at, btc_ret_30m, btc_ret_2h, btc_ret_24h,
                   vol_2h, vol_baseline, spike_zscore, regime_at_time, label
            FROM macro_trace
            WHERE news_id = ANY(%s);
        """, (list(news_ids),))
        for row in cur.fetchall():
            traces[row[0]] = {
                'btc_price_at': float(row[1]) if row[1] else None,
                'btc_ret_30m': float(row[2]) if row[2] else None,
                'btc_ret_2h': float(row[3]) if row[3] else None,
                'btc_ret_24h': float(row[4]) if row[4] else None,
                'vol_2h': float(row[5]) if row[5] else None,
                'vol_baseline': float(row[6]) if row[6] else None,
                'spike_zscore': float(row[7]) if row[7] else None,
                'regime_at_time': row[8],
                'label': row[9],
            }
    except Exception as e:
        _log(f'get_traces_for_report error: {e}')
    return traces


if __name__ == '__main__':
    from db_config import get_conn
    conn = get_conn(autocommit=True)
    with conn.cursor() as cur:
        n = compute_pending_traces(cur, lookback_hours=24)
        print(f'Computed {n} traces')
    conn.close()
