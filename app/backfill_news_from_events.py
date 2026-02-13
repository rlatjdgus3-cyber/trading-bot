"""
backfill_news_from_events.py — Fetch GDELT news for historical volatility spike events.

For each BACKFILL_VOL_SPIKE event in the events table:
  - Query GDELT DOC 2.0 API for news articles within +/-1 day of start_ts
  - Store articles in the news table (ON CONFLICT url DO NOTHING)
  - Compute market reactions (same logic as backfill_news_reaction.py)
  - Link news to events via event_news junction table
  - Refresh category_stats materialized view

Usage:
  python3 backfill_news_from_events.py
  python3 backfill_news_from_events.py --start 2025-11-01
  python3 backfill_news_from_events.py --start 2025-11-01 --end 2025-12-31
  python3 backfill_news_from_events.py --dry-run
"""
import os
import sys
import time
import json
import argparse
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timedelta, timezone

sys.path.insert(0, '/root/trading-bot/app')
import psycopg2
from dotenv import load_dotenv

load_dotenv('/root/trading-bot/app/.env')
import fact_categories

LOG_PREFIX = '[backfill_news_from_events]'
GDELT_BASE_URL = 'https://api.gdeltproject.org/api/v2/doc/doc'
GDELT_KEYWORDS = '(bitcoin OR crypto OR fed OR cpi OR nfp OR trump OR war OR sec OR etf OR boj OR china OR nasdaq)'
GDELT_MAX_RECORDS = 250
API_DELAY_SEC = 1.5
EVENT_WINDOW_HOURS = 24
LINK_WINDOW_MINUTES = 30


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


# ── Event fetching ───────────────────────────────────────

def _fetch_events(cur, start_date=None, end_date=None):
    """Fetch BACKFILL_VOL_SPIKE events, optionally filtered by date range.

    Returns: list of (id, start_ts, btc_price_at, direction, category, keywords)
    """
    query = """
        SELECT id, start_ts, btc_price_at, direction, category, keywords
        FROM events
        WHERE kind = 'BACKFILL_VOL_SPIKE'
    """
    params = []
    if start_date:
        query += " AND start_ts >= %s"
        params.append(start_date)
    if end_date:
        query += " AND start_ts <= %s"
        params.append(end_date)
    query += " ORDER BY start_ts ASC;"
    cur.execute(query, params)
    return cur.fetchall()


def _merge_windows(events):
    """Group events into calendar-day windows for GDELT queries.

    With thousands of events over 2+ years, ±24h merging collapses into
    one giant window. Instead, bucket by calendar day (UTC) and query
    each day that has events.

    Returns: list of (day_start_utc, day_end_utc, [event_ids_on_day])
    """
    if not events:
        return []

    # Bucket events by calendar day (UTC)
    day_buckets = {}
    for event_id, start_ts, *_ in events:
        day_key = start_ts.date()
        if day_key not in day_buckets:
            day_buckets[day_key] = []
        day_buckets[day_key].append(event_id)

    # Convert to sorted list of (day_start, day_end, event_ids)
    windows = []
    for day_key in sorted(day_buckets.keys()):
        day_start = datetime(day_key.year, day_key.month, day_key.day,
                             tzinfo=timezone.utc)
        day_end = day_start + timedelta(days=1)
        windows.append((day_start, day_end, day_buckets[day_key]))

    return windows


# ── GDELT API ────────────────────────────────────────────

def _gdelt_fetch(window_start, window_end):
    """Query GDELT DOC 2.0 API for news articles in the given time window.

    Returns list of article dicts. Returns [] on any failure.
    """
    start_str = window_start.strftime('%Y%m%d%H%M%S')
    end_str = window_end.strftime('%Y%m%d%H%M%S')

    params = urllib.parse.urlencode({
        'query': GDELT_KEYWORDS,
        'mode': 'artlist',
        'format': 'json',
        'STARTDATETIME': start_str,
        'ENDDATETIME': end_str,
        'maxrecords': GDELT_MAX_RECORDS,
    })
    url = f"{GDELT_BASE_URL}?{params}"

    try:
        req = urllib.request.Request(url)
        req.add_header('User-Agent', 'trading-bot-backfill/1.0')
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode('utf-8')

        if not body or not body.strip():
            return []

        data = json.loads(body)
        articles = data.get('articles', [])
        return articles if articles else []

    except json.JSONDecodeError:
        _log(f"  GDELT non-JSON response for {window_start.date()} (likely no data)")
        return []
    except urllib.error.HTTPError as e:
        _log(f"  GDELT HTTP error {e.code} for {window_start.date()}: {e.reason}")
        return []
    except urllib.error.URLError as e:
        _log(f"  GDELT URL error for {window_start.date()}: {e.reason}")
        return []
    except Exception as e:
        _log(f"  GDELT unexpected error for {window_start.date()}: {e}")
        return []


def _parse_seendate(seendate_str):
    """Parse GDELT seendate 'YYYYMMDDTHHMMSSz' to timezone-aware datetime."""
    if not seendate_str:
        return None
    try:
        clean = seendate_str.rstrip('Zz')
        dt = datetime.strptime(clean, '%Y%m%dT%H%M%S')
        return dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


# ── News insertion ───────────────────────────────────────

def _insert_news_article(cur, article):
    """Insert a GDELT article into the news table.

    Returns (news_id, was_new) or (None, False).
    """
    url = (article.get('url') or '').strip()
    title = (article.get('title') or '').strip()
    domain = (article.get('domain') or '').strip()
    seendate = _parse_seendate(article.get('seendate'))
    lang = (article.get('language') or '').strip()
    country = (article.get('sourcecountry') or '').strip()

    if not url or not title:
        return None, False

    # English only
    if lang and lang.lower() not in ('english', ''):
        return None, False

    keywords = fact_categories.extract_macro_keywords(title)
    summary = f"[GDELT] domain={domain} country={country}"

    cur.execute("""
        INSERT INTO news (ts, source, title, url, summary, impact_score, keywords)
        VALUES (%s, 'gdelt', %s, %s, %s, 0, %s)
        ON CONFLICT (url) DO NOTHING
        RETURNING id;
    """, (seendate, title, url, summary, keywords if keywords else []))

    row = cur.fetchone()
    if row:
        return row[0], True

    # Already existed
    cur.execute("SELECT id FROM news WHERE url = %s;", (url,))
    row = cur.fetchone()
    return (row[0], False) if row else (None, False)


# ── Market reaction (same logic as backfill_news_reaction.py) ──

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
    """Get max drawdown and max runup in the 24h window after ts."""
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


def _insert_reaction(cur, news_id, ts_news, title, summary):
    """Compute and insert market reaction for a news item. Returns True if inserted."""
    r = _compute_reaction(cur, ts_news, title, summary)
    if r is None:
        return False

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
    return cur.rowcount > 0


# ── Event-news linking ───────────────────────────────────

def _link_news_to_events(cur, news_id, news_ts, events_in_window):
    """Link a news item to nearby events via event_news (±30 min window).

    Returns count of links created.
    """
    if news_ts is None:
        return 0

    links = 0
    link_delta = timedelta(minutes=LINK_WINDOW_MINUTES)

    for event_id, start_ts, *_ in events_in_window:
        if abs((news_ts - start_ts).total_seconds()) <= link_delta.total_seconds():
            cur.execute("""
                INSERT INTO event_news (event_id, news_id, relevance)
                VALUES (%s, %s, 1.0)
                ON CONFLICT (event_id, news_id) DO NOTHING;
            """, (event_id, news_id))
            if cur.rowcount > 0:
                links += 1

    return links


# ── Main ─────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Backfill news from GDELT for BACKFILL_VOL_SPIKE events')
    parser.add_argument('--start', default=None,
                        help='Start date filter (YYYY-MM-DD)')
    parser.add_argument('--end', default=None,
                        help='End date filter (YYYY-MM-DD)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be done without writing to DB')
    args = parser.parse_args()

    start_date = (datetime.strptime(args.start, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                  if args.start else None)
    end_date = (datetime.strptime(args.end, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                + timedelta(days=1) if args.end else None)  # inclusive of end day

    _log(f"Starting. start={args.start or 'all'} end={args.end or 'all'} dry_run={args.dry_run}")

    conn = _db_conn()
    conn.autocommit = True
    cur = conn.cursor()

    stats = {
        'events_total': 0,
        'windows_merged': 0,
        'api_calls': 0,
        'api_empty': 0,
        'articles_fetched': 0,
        'news_inserted': 0,
        'news_skipped_dup': 0,
        'reactions_created': 0,
        'links_created': 0,
    }

    try:
        # Step 1: Fetch all matching events
        events = _fetch_events(cur, start_date, end_date)
        stats['events_total'] = len(events)
        _log(f"Found {len(events)} BACKFILL_VOL_SPIKE events.")

        if not events:
            _log("No events found. Nothing to do.")
            return

        # Build event lookup dict for linking phase
        events_by_id = {e[0]: e for e in events}

        # Step 2: Merge overlapping windows
        merged_windows = _merge_windows(events)
        stats['windows_merged'] = len(merged_windows)
        _log(f"Merged into {len(merged_windows)} API windows "
             f"(saved {len(events) - len(merged_windows)} API calls).")

        # Step 3: Process each merged window
        for wi, (w_start, w_end, event_ids) in enumerate(merged_windows):
            _log(f"Window {wi+1}/{len(merged_windows)}: "
                 f"{w_start.strftime('%Y-%m-%d %H:%M')} to {w_end.strftime('%Y-%m-%d %H:%M')} "
                 f"({len(event_ids)} events)")

            if args.dry_run:
                _log(f"  [DRY RUN] Would call GDELT API for this window")
                stats['api_calls'] += 1
                continue

            # Fetch from GDELT
            articles = _gdelt_fetch(w_start, w_end)
            stats['api_calls'] += 1

            if not articles:
                stats['api_empty'] += 1
                _log(f"  0 articles returned")
                time.sleep(API_DELAY_SEC)
                continue

            stats['articles_fetched'] += len(articles)
            _log(f"  {len(articles)} articles fetched")

            # Gather the event tuples for this window's event_ids
            window_events = [events_by_id[eid] for eid in event_ids
                             if eid in events_by_id]

            # Step 4: Insert articles, compute reactions, link
            for article in articles:
                news_id, was_new = _insert_news_article(cur, article)
                if news_id is None:
                    continue

                if was_new:
                    stats['news_inserted'] += 1
                else:
                    stats['news_skipped_dup'] += 1

                seendate = _parse_seendate(article.get('seendate'))
                title = (article.get('title') or '').strip()
                domain = (article.get('domain') or '').strip()
                country = (article.get('sourcecountry') or '').strip()
                summary = f"[GDELT] domain={domain} country={country}"

                # Compute market reaction
                if seendate:
                    if _insert_reaction(cur, news_id, seendate, title, summary):
                        stats['reactions_created'] += 1

                # Link to nearby events
                if seendate:
                    links = _link_news_to_events(cur, news_id, seendate, window_events)
                    stats['links_created'] += links

            # Polite delay between API calls
            time.sleep(API_DELAY_SEC)

        # Step 5: Refresh materialized views
        if not args.dry_run and stats['reactions_created'] > 0:
            for mv in ('category_stats', 'price_event_stats'):
                try:
                    cur.execute(
                        f"REFRESH MATERIALIZED VIEW CONCURRENTLY public.{mv};")
                    _log(f"{mv} materialized view refreshed.")
                except Exception as e:
                    _log(f"{mv} refresh error (non-fatal): {e}")

    finally:
        cur.close()
        conn.close()

    # Print summary
    _log("=" * 60)
    _log("SUMMARY")
    _log(f"  Events processed:      {stats['events_total']}")
    _log(f"  Merged API windows:    {stats['windows_merged']}")
    _log(f"  API calls made:        {stats['api_calls']}")
    _log(f"  API calls empty:       {stats['api_empty']}")
    _log(f"  Articles fetched:      {stats['articles_fetched']}")
    _log(f"  News inserted (new):   {stats['news_inserted']}")
    _log(f"  News skipped (dup):    {stats['news_skipped_dup']}")
    _log(f"  Reactions created:     {stats['reactions_created']}")
    _log(f"  Event-news links:      {stats['links_created']}")
    _log("=" * 60)


if __name__ == '__main__':
    main()
