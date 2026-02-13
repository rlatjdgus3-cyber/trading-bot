"""
backfill_events.py — Detect historical volatility spike events from market_ohlcv.

- Compute 5m log-returns from market_ohlcv
- Rolling 1h window (12 bars) for mean/stddev
- Event starts when |z-score| > 3.0, ends when < 2.0 for 3 bars
- Links news within +/-30min window -> event_news
- Kind = 'BACKFILL_VOL_SPIKE'

Usage: python3 backfill_events.py
"""
import os
import sys
import math
import json

sys.path.insert(0, '/root/trading-bot/app')
import psycopg2
from dotenv import load_dotenv

load_dotenv('/root/trading-bot/app/.env')
import fact_categories

LOG_PREFIX = '[backfill_events]'
ZSCORE_START = 3.0
ZSCORE_END = 2.0
COOLDOWN_BARS = 3
WINDOW = 12


def _log(msg):
    print(f"{LOG_PREFIX} {msg}", flush=True)


def _db_conn():
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=int(os.getenv('DB_PORT', '5432')),
        dbname=os.getenv('DB_NAME', 'trading'),
        user=os.getenv('DB_USER', 'bot'),
        password=os.getenv('DB_PASS', 'botpass'),
        connect_timeout=10,
        options='-c statement_timeout=120000',
    )


def _get_price_after(cur, ts, hours):
    """Get closest 5m candle close at ts + hours."""
    cur.execute("""
        SELECT c FROM market_ohlcv
        WHERE symbol = 'BTC/USDT:USDT' AND tf = '5m'
          AND ts >= %s + %s * interval '1 hour'
        ORDER BY ts ASC LIMIT 1;
    """, (ts, hours))
    row = cur.fetchone()
    if row:
        return float(row[0])


def main():
    conn = _db_conn()
    conn.autocommit = True
    cur = conn.cursor()

    _log("Loading 5m OHLCV data...")
    cur.execute("""
        SELECT ts, c FROM market_ohlcv
        WHERE symbol = 'BTC/USDT:USDT' AND tf = '5m'
        ORDER BY ts ASC;
    """)
    rows = cur.fetchall()
    _log(f"Loaded {len(rows)} candles.")

    if len(rows) < WINDOW + 2:
        _log("Not enough data for z-score computation.")
        cur.close()
        conn.close()
        return

    # Compute log returns
    timestamps = []
    closes = []
    log_returns = []
    for i, (ts, c) in enumerate(rows):
        closes.append(float(c))
        timestamps.append(ts)
        if i == 0:
            log_returns.append(0.0)
        else:
            prev_c = closes[i - 1]
            if prev_c > 0:
                log_returns.append(math.log(closes[i] / prev_c))
            else:
                log_returns.append(0.0)

    _log("Detecting volatility spike events...")

    # Delete existing backfill events to allow re-run
    cur.execute("DELETE FROM event_news WHERE event_id IN (SELECT id FROM events WHERE kind = 'BACKFILL_VOL_SPIKE');")
    cur.execute("DELETE FROM events WHERE kind = 'BACKFILL_VOL_SPIKE';")
    _log("Cleared existing BACKFILL_VOL_SPIKE events.")

    events_created = 0
    in_event = False
    event_start_idx = None
    below_count = 0
    peak_zscore = 0.0
    event_direction = None

    for i in range(WINDOW + 1, len(log_returns)):
        # Rolling window: log_returns[i-WINDOW:i] (excludes current bar i)
        window = log_returns[i - WINDOW:i]
        n = len(window)
        mean = sum(window) / n
        variance = sum((x - mean) ** 2 for x in window) / n
        std = math.sqrt(variance) if variance > 0 else 0.0

        if std == 0:
            zscore = 0.0
        else:
            zscore = (log_returns[i] - mean) / std

        abs_z = abs(zscore)

        if not in_event:
            if abs_z > ZSCORE_START:
                in_event = True
                event_start_idx = i
                below_count = 0
                peak_zscore = abs_z
                event_direction = 'UP' if log_returns[i] > 0 else 'DOWN'
        else:
            # Track peak z-score during the event
            peak_zscore = max(peak_zscore, abs_z)

            if abs_z < ZSCORE_END:
                below_count += 1
            else:
                below_count = 0

            if below_count >= COOLDOWN_BARS:
                # Event ended
                event_end_idx = i
                start_ts = timestamps[event_start_idx]
                end_ts = timestamps[event_end_idx]
                btc_price = closes[event_start_idx]

                # Compute moves
                price_1h = _get_price_after(cur, start_ts, 1)
                price_4h = _get_price_after(cur, start_ts, 4)
                price_24h = _get_price_after(cur, start_ts, 24)

                move_1h = round(((price_1h - btc_price) / btc_price) * 100, 4) if price_1h and btc_price else None
                move_4h = round(((price_4h - btc_price) / btc_price) * 100, 4) if price_4h and btc_price else None
                move_24h = round(((price_24h - btc_price) / btc_price) * 100, 4) if price_24h and btc_price else None

                # Find nearby news (+/-30 min)
                cur.execute("""
                    SELECT id, title, summary FROM news
                    WHERE ts >= %s - interval '30 minutes'
                      AND ts <= %s + interval '30 minutes'
                    ORDER BY ts;
                """, (start_ts, start_ts))
                related_news = cur.fetchall()

                # Classify from linked news
                combined_text = ' '.join(
                    f"{t or ''} {s or ''}" for _, t, s in related_news
                )
                category = fact_categories.classify_news(combined_text)
                keywords = fact_categories.extract_macro_keywords(combined_text)

                cur.execute("""
                    INSERT INTO events
                        (kind, start_ts, end_ts, symbol, vol_zscore,
                         btc_price_at, btc_move_1h, btc_move_4h, btc_move_24h,
                         direction, category, keywords, metadata)
                    VALUES ('BACKFILL_VOL_SPIKE', %s, %s, 'BTC/USDT:USDT', %s,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s::jsonb)
                    RETURNING id;
                """, (start_ts, end_ts, round(peak_zscore, 4),
                      btc_price, move_1h, move_4h, move_24h,
                      event_direction, category, keywords,
                      json.dumps({
                          'duration_bars': event_end_idx - event_start_idx,
                          'news_count': len(related_news),
                      }, default=str)))

                event_id = cur.fetchone()[0]

                # Link news
                for news_id, _, _ in related_news:
                    cur.execute("""
                        INSERT INTO event_news (event_id, news_id, relevance)
                        VALUES (%s, %s, 1.0)
                        ON CONFLICT (event_id, news_id) DO NOTHING;
                    """, (event_id, news_id))

                events_created += 1
                if events_created % 50 == 0:
                    _log(f"  events created: {events_created}")

                # Reset state
                in_event = False
                event_start_idx = None
                below_count = 0
                peak_zscore = 0.0
                event_direction = None

    _log(f"Done. Total events created: {events_created}")

    # Refresh price_event_stats materialized view
    if events_created > 0:
        try:
            cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY public.price_event_stats;")
            _log("price_event_stats refreshed.")
        except Exception as e:
            _log(f"price_event_stats refresh error (non-fatal): {e}")

    cur.close()
    conn.close()


if __name__ == '__main__':
    main()
