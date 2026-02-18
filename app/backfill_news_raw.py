"""
backfill_news_raw.py — GDELT DOC 2.0 API로 뉴스 빈 구간 채움.

2024-02 ~ 2025-10 뉴스 공백 구간을 일별로 조회하여 news 테이블에 저장.
ON CONFLICT (url) DO NOTHING으로 중복 방지.
일별 커서 저장으로 재개 지원.

Usage:
    python backfill_news_raw.py                     # 전체 백필
    python backfill_news_raw.py --resume            # 마지막 커서부터 재개
    python backfill_news_raw.py --start 2024-06-01  # 특정 시작일
"""
import os
import sys
import json
import time
import urllib.parse
import urllib.request
import traceback
import argparse
from datetime import datetime, timezone, timedelta

sys.path.insert(0, '/root/trading-bot/app')
from db_config import get_conn
from backfill_utils import start_job, get_last_cursor, update_progress, finish_job

LOG_PREFIX = '[backfill_news_raw]'
JOB_NAME = 'backfill_news_raw'

GDELT_BASE_URL = 'https://api.gdeltproject.org/api/v2/doc/doc'
GDELT_KEYWORDS = '(bitcoin OR crypto OR fed OR cpi OR nfp OR trump OR war OR sec OR etf OR boj OR china OR nasdaq)'
GDELT_MAX_RECORDS = 250

DEFAULT_START = '2024-02-01'
DEFAULT_END = '2025-11-01'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _gdelt_fetch(window_start, window_end):
    """Query GDELT DOC 2.0 API for news articles in the given time window."""
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
        return data.get('articles', []) or []

    except json.JSONDecodeError:
        return []
    except urllib.error.HTTPError as e:
        _log(f'GDELT HTTP error {e.code} for {window_start.date()}: {e.reason}')
        return []
    except urllib.error.URLError as e:
        _log(f'GDELT URL error for {window_start.date()}: {e.reason}')
        return []
    except Exception as e:
        _log(f'GDELT error for {window_start.date()}: {e}')
        return []


def _extract_keywords(title):
    """Extract macro keywords from title."""
    kw_list = [
        'bitcoin', 'btc', 'crypto', 'fed', 'fomc', 'powell', 'cpi', 'ppi',
        'nfp', 'jobs', 'inflation', 'rate', 'etf', 'sec', 'regulation',
        'trump', 'war', 'china', 'boj', 'japan', 'nasdaq', 'recession',
        'bank', 'dollar', 'treasury', 'bond', 'gdp', 'unemployment',
    ]
    title_lower = (title or '').lower()
    return [kw for kw in kw_list if kw in title_lower]


def _insert_articles(conn, articles):
    """Insert articles into news table. Returns (inserted, skipped)."""
    inserted = 0
    skipped = 0

    # Collect valid rows first
    rows = []
    for art in articles:
        title = (art.get('title') or '').strip()
        url = (art.get('url') or '').strip()
        seendate = art.get('seendate', '')
        domain = art.get('domain', '')
        country = art.get('sourcecountry', '')

        if not title or not url:
            skipped += 1
            continue

        # Parse seendate (YYYYMMDDTHHMMSSZ format)
        try:
            if 'T' in seendate:
                ts = datetime.strptime(seendate, '%Y%m%dT%H%M%SZ').replace(tzinfo=timezone.utc)
            else:
                ts = datetime.strptime(seendate[:14], '%Y%m%d%H%M%S').replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            skipped += 1
            continue

        summary = f"[GDELT] domain={domain} country={country}"
        keywords = _extract_keywords(title)
        rows.append((ts, title, url, summary, keywords if keywords else []))

    # Batch insert in single transaction
    if rows:
        try:
            with conn.cursor() as cur:
                for params in rows:
                    cur.execute("""
                        INSERT INTO news (ts, source, title, url, summary, impact_score, keywords)
                        VALUES (%s, 'gdelt', %s, %s, %s, 0, %s)
                        ON CONFLICT (url) DO NOTHING;
                    """, params)
                    if cur.rowcount > 0:
                        inserted += 1
                    else:
                        skipped += 1
            conn.commit()
        except Exception as e:
            conn.rollback()
            _log(f'Batch insert error: {e}')
            skipped += len(rows)

    return inserted, skipped


def main():
    parser = argparse.ArgumentParser(description='Backfill news from GDELT')
    parser.add_argument('--start', default=DEFAULT_START, help='Start date YYYY-MM-DD')
    parser.add_argument('--end', default=DEFAULT_END, help='End date YYYY-MM-DD')
    parser.add_argument('--resume', action='store_true', help='Resume from last cursor')
    args = parser.parse_args()

    conn = get_conn()
    conn.autocommit = False

    start_dt = datetime.strptime(args.start, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(args.end, '%Y-%m-%d').replace(tzinfo=timezone.utc)

    if args.resume:
        cursor = get_last_cursor(conn, JOB_NAME)
        if cursor and 'current_date' in cursor:
            start_dt = datetime.strptime(cursor['current_date'], '%Y-%m-%d').replace(tzinfo=timezone.utc)
            _log(f'Resuming from {start_dt.date()}')

    _log(f'Backfilling GDELT news from {start_dt.date()} to {end_dt.date()}')

    job_id = start_job(conn, JOB_NAME, metadata={
        'start': str(start_dt.date()), 'end': str(end_dt.date())
    })

    total_inserted = 0
    total_failed = 0
    current = start_dt
    day_count = 0
    backoff = 5

    try:
        while current < end_dt:
            next_day = current + timedelta(days=1)
            day_count += 1

            articles = _gdelt_fetch(current, next_day)

            if articles:
                ins, skip = _insert_articles(conn, articles)
                total_inserted += ins
            else:
                ins = 0

            # Save cursor every day
            update_progress(conn, job_id,
                            {'current_date': next_day.strftime('%Y-%m-%d')},
                            inserted=ins)

            if day_count % 30 == 0:
                _log(f'Day {day_count}: {current.date()}, total inserted={total_inserted}')

            current = next_day

            # GDELT rate limit: ~1 req/s
            time.sleep(1.0)

        finish_job(conn, job_id, status='COMPLETED')
        _log(f'DONE: {total_inserted} articles inserted over {day_count} days')

    except KeyboardInterrupt:
        _log('Interrupted by user')
        update_progress(conn, job_id, {'current_date': current.strftime('%Y-%m-%d')})
        finish_job(conn, job_id, status='PARTIAL', error='KeyboardInterrupt')
    except Exception as e:
        _log(f'FATAL: {e}')
        traceback.print_exc()
        try:
            finish_job(conn, job_id, status='FAILED', error=str(e)[:500])
        except Exception:
            pass
    finally:
        conn.close()


if __name__ == '__main__':
    main()
