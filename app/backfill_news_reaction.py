"""
backfill_news_reaction.py — Compute price reaction per news item.

For each news row not yet in news_market_reaction:
  - Find closest 5m candle at/before news.ts -> btc_price_at
  - Find price at +1h/+4h/+24h -> compute ret_*
  - Compute avg volume in 1h/4h windows -> vol_*
  - Compute max_drawdown_24h / max_runup_24h / trend_after_24h
  - Classify category via fact_categories

Usage:
  python3 backfill_news_reaction.py            # new rows only
  python3 backfill_news_reaction.py --recompute # update existing rows with new fields
"""
import os
import sys

sys.path.insert(0, '/root/trading-bot/app')
from db_config import get_conn
import fact_categories

LOG_PREFIX = '[backfill_news_reaction]'
BATCH_SIZE = 100


def _log(msg):
    print(f"{LOG_PREFIX} {msg}", flush=True)


def _db_conn():
    return get_conn()


def _get_price_at(cur, ts):
    """Get the closest 5m candle close at or before ts."""
    cur.execute("""
        SELECT c FROM market_ohlcv
        WHERE symbol = 'BTC/USDT:USDT' AND tf = '5m' AND ts <= %s
        ORDER BY ts DESC LIMIT 1;
    """, (ts,))
    row = cur.fetchone()
    if row:
        return float(row[0])


def _get_price_after(cur, ts, hours):
    """Get the closest 5m candle close at ts + hours."""
    cur.execute("""
        SELECT c FROM market_ohlcv
        WHERE symbol = 'BTC/USDT:USDT' AND tf = '5m'
          AND ts >= %s + %s * interval '1 hour'
        ORDER BY ts ASC LIMIT 1;
    """, (ts, hours))
    row = cur.fetchone()
    if row:
        return float(row[0])


def _get_avg_volume(cur, ts, hours):
    """Get average volume in the window [ts, ts + hours]."""
    cur.execute("""
        SELECT AVG(v) FROM market_ohlcv
        WHERE symbol = 'BTC/USDT:USDT' AND tf = '5m'
          AND ts >= %s AND ts < %s + %s * interval '1 hour';
    """, (ts, ts, hours))
    row = cur.fetchone()
    if row and row[0]:
        return float(row[0])


def _get_24h_extremes(cur, ts, btc_price):
    """Get max drawdown and max runup in the 24h window after ts.

    Returns (max_drawdown_pct, max_runup_pct, trend_label).
    drawdown is negative (worst dip), runup is positive (best peak).
    """
    if btc_price is None or btc_price <= 0:
        return None, None, None
    cur.execute("""
        SELECT MIN(l), MAX(h) FROM market_ohlcv
        WHERE symbol = 'BTC/USDT:USDT' AND tf = '5m'
          AND ts > %s AND ts <= %s + interval '24 hours';
    """, (ts, ts))
    row = cur.fetchone()
    if not row or row[0] is None:
        return None, None, None
    min_low = float(row[0])
    max_high = float(row[1])
    drawdown = round(((min_low - btc_price) / btc_price) * 100, 4)
    runup = round(((max_high - btc_price) / btc_price) * 100, 4)
    # trend: bullish if runup > |drawdown|, bearish if |drawdown| > runup, else neutral
    if runup > abs(drawdown) * 1.5:
        trend = 'bullish'
    elif abs(drawdown) > runup * 1.5:
        trend = 'bearish'
    else:
        trend = 'neutral'
    return drawdown, runup, trend


def _compute_reaction(cur, ts_news, title, summary):
    """Compute all reaction fields for a single news item."""
    btc_price = _get_price_at(cur, ts_news)
    if btc_price is None:
        return None

    price_1h = _get_price_after(cur, ts_news, 1)
    price_4h = _get_price_after(cur, ts_news, 4)
    price_24h = _get_price_after(cur, ts_news, 24)

    ret_1h = round(((price_1h - btc_price) / btc_price) * 100, 6) if price_1h else None
    ret_4h = round(((price_4h - btc_price) / btc_price) * 100, 6) if price_4h else None
    ret_24h = round(((price_24h - btc_price) / btc_price) * 100, 6) if price_24h else None

    vol_1h = _get_avg_volume(cur, ts_news, 1)
    vol_4h = _get_avg_volume(cur, ts_news, 4)

    max_dd, max_ru, trend = _get_24h_extremes(cur, ts_news, btc_price)

    category = fact_categories.classify_news(title, summary)

    return {
        'btc_price_at': btc_price,
        'ret_1h': ret_1h, 'ret_4h': ret_4h, 'ret_24h': ret_24h,
        'vol_1h': vol_1h, 'vol_4h': vol_4h,
        'max_drawdown_24h': max_dd, 'max_runup_24h': max_ru,
        'trend_after_24h': trend, 'category': category,
    }


def main():
    recompute = '--recompute' in sys.argv
    conn = _db_conn()
    conn.autocommit = True
    cur = conn.cursor()

    _log("Starting news reaction backfill..." + (" (recompute mode)" if recompute else ""))

    total_processed = 0
    total_inserted = 0
    total_updated = 0

    try:
        if recompute:
            # Update existing rows that are missing the new fields
            while True:
                cur.execute("""
                    SELECT nmr.id, nmr.news_id, nmr.ts_news, n.title, n.summary
                    FROM news_market_reaction nmr
                    JOIN news n ON n.id = nmr.news_id
                    WHERE nmr.max_drawdown_24h IS NULL
                    ORDER BY nmr.id ASC
                    LIMIT %s;
                """, (BATCH_SIZE,))
                rows = cur.fetchall()
                if not rows:
                    break
                for nmr_id, news_id, ts_news, title, summary in rows:
                    total_processed += 1
                    r = _compute_reaction(cur, ts_news, title, summary)
                    if r is None:
                        continue
                    cur.execute("""
                        UPDATE news_market_reaction SET
                            max_drawdown_24h = %s,
                            max_runup_24h = %s,
                            trend_after_24h = %s,
                            category = COALESCE(category, %s),
                            computed_at = now()
                        WHERE id = %s;
                    """, (r['max_drawdown_24h'], r['max_runup_24h'],
                          r['trend_after_24h'], r['category'], nmr_id))
                    total_updated += 1
                _log(f"  recompute batch: processed={total_processed} updated={total_updated}")
            _log(f"Recompute done. processed={total_processed} updated={total_updated}")

        # Insert new rows
        total_processed = 0
        while True:
            cur.execute("""
                SELECT n.id, n.ts, n.title, n.summary
                FROM news n
                LEFT JOIN news_market_reaction nmr ON nmr.news_id = n.id
                WHERE nmr.id IS NULL
                ORDER BY n.ts ASC
                LIMIT %s;
            """, (BATCH_SIZE,))
            rows = cur.fetchall()

            if not rows:
                break

            for news_id, ts_news, title, summary in rows:
                total_processed += 1
                r = _compute_reaction(cur, ts_news, title, summary)
                if r is None:
                    continue

                cur.execute("""
                    INSERT INTO news_market_reaction
                        (news_id, ts_news, btc_price_at,
                         ret_1h, ret_4h, ret_24h,
                         vol_1h, vol_4h, category,
                         max_drawdown_24h, max_runup_24h, trend_after_24h)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (news_id) DO NOTHING;
                """, (news_id, ts_news, r['btc_price_at'],
                      r['ret_1h'], r['ret_4h'], r['ret_24h'],
                      r['vol_1h'], r['vol_4h'], r['category'],
                      r['max_drawdown_24h'], r['max_runup_24h'], r['trend_after_24h']))
                total_inserted += 1

            _log(f"  batch done: processed={total_processed} inserted={total_inserted}")

        _log(f"Done. processed={total_processed} inserted={total_inserted}")

        # Refresh category_stats materialized view
        try:
            cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY public.category_stats;")
            _log("category_stats refreshed.")
        except Exception as e:
            _log(f"category_stats refresh error (non-fatal): {e}")

    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    main()
