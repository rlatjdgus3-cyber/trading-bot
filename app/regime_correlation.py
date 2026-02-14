"""
regime_correlation.py — BTC-QQQ 상관관계 기반 regime 분류.

30일 rolling correlation BTC-QQQ 계산:
  COUPLED_RISK:  corr > 0.5  (매크로 뉴스 영향 증가)
  NEUTRAL:       0.2 ~ 0.5
  DECOUPLED:     corr < 0.2  (매크로 뉴스 영향 감소)

캐시: 1시간 TTL
"""
import os
import sys
import time
import traceback

sys.path.insert(0, '/root/trading-bot/app')

LOG_PREFIX = '[regime_corr]'
CACHE_TTL_SEC = 3600  # 1 hour

# Module-level cache
_cached_regime = None
_cached_corr = None
_cache_ts = 0


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _db_conn():
    import psycopg2
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=int(os.getenv('DB_PORT', '5432')),
        dbname=os.getenv('DB_NAME', 'trading'),
        user=os.getenv('DB_USER', 'bot'),
        password=os.getenv('DB_PASS', 'botpass'),
        connect_timeout=10,
        options='-c statement_timeout=30000')


def _compute_correlation(cur):
    """Compute 30-day rolling correlation between BTC and QQQ daily returns.

    Uses candles (1m, aggregated to daily) for BTC and macro_data for QQQ.
    Returns (correlation_float, sample_count) or (None, 0) on error.
    """
    try:
        # BTC daily returns from candles (1m aggregated to daily)
        cur.execute("""
            SELECT date_trunc('day', ts) AS day,
                   (MAX(c) - MIN(o)) / NULLIF(MIN(o), 0) * 100 AS daily_ret
            FROM candles
            WHERE symbol = 'BTC/USDT:USDT' AND ts >= now() - interval '30 days'
            GROUP BY day
            ORDER BY day;
        """)
        btc_rows = cur.fetchall()

        # QQQ daily returns from macro_data
        cur.execute("""
            SELECT date_trunc('day', ts) AS day,
                   price
            FROM macro_data
            WHERE source = 'QQQ' AND ts >= now() - interval '30 days'
            ORDER BY ts;
        """)
        qqq_rows = cur.fetchall()

        if not btc_rows or not qqq_rows:
            _log(f'insufficient data: btc={len(btc_rows or [])} qqq={len(qqq_rows or [])}')
            return (None, 0)

        # Build daily return maps
        btc_daily = {}
        for day, ret in btc_rows:
            if ret is not None:
                btc_daily[day.date() if hasattr(day, 'date') else day] = float(ret)

        # QQQ: compute returns from prices
        qqq_prices = []
        for day, price in qqq_rows:
            if price is not None:
                qqq_prices.append((day.date() if hasattr(day, 'date') else day, float(price)))

        qqq_daily = {}
        for i in range(1, len(qqq_prices)):
            prev_p = qqq_prices[i - 1][1]
            cur_p = qqq_prices[i][1]
            if prev_p > 0:
                qqq_daily[qqq_prices[i][0]] = (cur_p - prev_p) / prev_p * 100

        # Find common days
        common_days = sorted(set(btc_daily.keys()) & set(qqq_daily.keys()))
        if len(common_days) < 10:
            _log(f'too few common days: {len(common_days)}')
            return (None, len(common_days))

        btc_rets = [btc_daily[d] for d in common_days]
        qqq_rets = [qqq_daily[d] for d in common_days]

        # Pearson correlation
        n = len(btc_rets)
        mean_btc = sum(btc_rets) / n
        mean_qqq = sum(qqq_rets) / n

        cov = sum((b - mean_btc) * (q - mean_qqq) for b, q in zip(btc_rets, qqq_rets)) / n
        std_btc = (sum((b - mean_btc) ** 2 for b in btc_rets) / n) ** 0.5
        std_qqq = (sum((q - mean_qqq) ** 2 for q in qqq_rets) / n) ** 0.5

        if std_btc == 0 or std_qqq == 0:
            return (0.0, n)

        corr = cov / (std_btc * std_qqq)
        corr = max(-1.0, min(1.0, corr))
        return (round(corr, 4), n)

    except Exception as e:
        _log(f'correlation compute error: {e}')
        traceback.print_exc()
        return (None, 0)


def _classify_regime(corr):
    """Classify regime from correlation value."""
    if corr is None:
        return 'NEUTRAL'
    if corr > 0.5:
        return 'COUPLED_RISK'
    elif corr < 0.2:
        return 'DECOUPLED'
    else:
        return 'NEUTRAL'


def get_current_regime(cur=None):
    """Get current BTC-QQQ regime. Cached for 1 hour.

    Returns: str ('COUPLED_RISK', 'NEUTRAL', 'DECOUPLED')
    """
    global _cached_regime, _cached_corr, _cache_ts

    now = time.time()
    if _cached_regime and (now - _cache_ts) < CACHE_TTL_SEC:
        return _cached_regime

    own_conn = False
    conn = None
    try:
        if cur is None:
            conn = _db_conn()
            conn.autocommit = True
            cur = conn.cursor()
            own_conn = True

        corr, n = _compute_correlation(cur)
        regime = _classify_regime(corr)

        _cached_regime = regime
        _cached_corr = corr
        _cache_ts = now

        _log(f'regime={regime} corr={corr} samples={n}')
        return regime

    except Exception as e:
        _log(f'get_current_regime error: {e}')
        return _cached_regime or 'NEUTRAL'
    finally:
        if own_conn and conn:
            try:
                conn.close()
            except Exception:
                pass


def get_correlation_info(cur=None):
    """Get detailed correlation info. Returns dict."""
    global _cached_corr, _cached_regime
    regime = get_current_regime(cur)
    return {
        'regime': regime,
        'correlation': _cached_corr,
        'cache_age_sec': int(time.time() - _cache_ts) if _cache_ts else None,
    }


if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv('/root/trading-bot/app/.env')
    info = get_correlation_info()
    import json
    print(json.dumps(info, indent=2, default=str))
