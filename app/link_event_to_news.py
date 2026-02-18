"""
link_event_to_news.py — 이벤트↔뉴스 연결.

price_events의 각 이벤트에 대해 시간 창 [-240분, +60분] 내 뉴스를 찾아
match_score(0~100)를 계산하여 event_news_link에 저장.

match_score 구성:
- 시간 근접도 (0~40): 가까울수록 높음, 선행 뉴스 보너스
- 티어 점수 (0~30): TIER1=30, TIER2=21, TIER3=9
- 토픽 점수 (0~20): macro+BTC_DIRECT=20, crypto=12, noise=0
- 영향도 보너스 (0~10): min(10, impact_score)

TIERX 뉴스 제외, min_score=10 미만 링크 제외.

Usage:
    python link_event_to_news.py
    python link_event_to_news.py --min-score 15
"""
import sys
import argparse
import traceback

sys.path.insert(0, '/root/trading-bot/app')
from db_config import get_conn
from backfill_utils import start_job, finish_job, update_progress, check_stop, check_pause

LOG_PREFIX = '[link_event_news]'
JOB_NAME = 'link_event_to_news'

# Time window: -240min (news before event) to +60min (news after event)
WINDOW_BEFORE_MIN = 240
WINDOW_AFTER_MIN = 60

# Scoring weights
TIER_SCORES = {'TIER1': 30, 'TIER2': 21, 'TIER3': 9}
TOPIC_SCORES = {
    ('macro', 'BTC_DIRECT'): 20,
    ('macro', 'BTC_INDIRECT'): 16,
    ('crypto', 'BTC_DIRECT'): 18,
    ('crypto', 'BTC_INDIRECT'): 12,
    ('macro', 'NONE'): 10,
    ('crypto', 'NONE'): 6,
}
MIN_SCORE_DEFAULT = 10


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _time_proximity_score(time_lag_minutes):
    """Calculate time proximity score (0~40). Closer = higher, pre-event bonus."""
    abs_lag = abs(time_lag_minutes)

    # Base proximity: 40 at 0min, linearly decaying to 0 at 240min
    if abs_lag > 240:
        return 0
    base = 40 * (1 - abs_lag / 240)

    # Pre-event bonus: news that precedes the price event
    if time_lag_minutes < 0:  # news before event
        base = min(40, base * 1.3)

    return round(base, 1)


def _tier_score(tier):
    """Tier score (0~30)."""
    return TIER_SCORES.get(tier, 0)


def _topic_score(topic_class, asset_relevance):
    """Topic + asset relevance score (0~20)."""
    tc = (topic_class or 'noise').lower()
    ar = (asset_relevance or 'NONE').upper()
    return TOPIC_SCORES.get((tc, ar), 0)


def _impact_bonus(impact_score):
    """Impact bonus (0~10)."""
    if impact_score is None:
        return 0
    return min(10, max(0, int(impact_score)))


def _compute_match_score(time_lag_minutes, tier, topic_class, asset_relevance, impact_score):
    """Compute total match score (0~100)."""
    score = (
        _time_proximity_score(time_lag_minutes)
        + _tier_score(tier)
        + _topic_score(topic_class, asset_relevance)
        + _impact_bonus(impact_score)
    )
    return round(min(100, max(0, score)), 1)


def main():
    parser = argparse.ArgumentParser(description='Link price events to news')
    parser.add_argument('--min-score', type=float, default=MIN_SCORE_DEFAULT)
    parser.add_argument('--batch-size', type=int, default=500)
    args = parser.parse_args()

    conn = get_conn()
    conn.autocommit = False

    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '300000';")
    conn.commit()

    job_id = start_job(conn, JOB_NAME, metadata={'min_score': args.min_score})

    total_links = 0
    total_events = 0
    total_skipped = 0

    try:
        # Get all price events
        with conn.cursor() as cur:
            cur.execute("""
                SELECT event_id, start_ts FROM price_events
                ORDER BY start_ts ASC;
            """)
            events = cur.fetchall()

        _log(f'Processing {len(events)} price events')

        for idx, (event_id, start_ts) in enumerate(events):
            # Signal check every 100 events
            if idx % 100 == 0 and idx > 0:
                if check_stop():
                    _log('STOP signal received')
                    update_progress(conn, job_id, {'events_processed': idx},
                                    inserted=total_links)
                    finish_job(conn, job_id, status='PARTIAL', error='stopped_by_user')
                    return
                if not check_pause():
                    _log('STOP during pause')
                    update_progress(conn, job_id, {'events_processed': idx},
                                    inserted=total_links)
                    finish_job(conn, job_id, status='PARTIAL', error='stopped_during_pause')
                    return

            total_events += 1

            # Find nearby news within time window
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT n.id, n.ts, n.tier, n.topic_class, n.asset_relevance, n.impact_score
                    FROM news n
                    WHERE n.ts >= %s - %s * interval '1 minute'
                      AND n.ts <= %s + %s * interval '1 minute'
                      AND n.tier IS DISTINCT FROM 'TIERX'
                    ORDER BY n.ts ASC;
                """, (start_ts, WINDOW_BEFORE_MIN, start_ts, WINDOW_AFTER_MIN))
                nearby_news = cur.fetchall()

            if not nearby_news:
                total_skipped += 1
                continue

            links_batch = []
            for news_id, ts_news, tier, topic_class, asset_relevance, impact_score in nearby_news:
                # Time lag in minutes (negative = news before event)
                time_lag = (ts_news - start_ts).total_seconds() / 60.0

                score = _compute_match_score(time_lag, tier, topic_class, asset_relevance, impact_score)

                if score < args.min_score:
                    continue

                reason_parts = []
                if tier in TIER_SCORES:
                    reason_parts.append(f'tier={tier}')
                if topic_class:
                    reason_parts.append(f'topic={topic_class}')
                if time_lag < 0:
                    reason_parts.append(f'pre-event {abs(time_lag):.0f}m')
                else:
                    reason_parts.append(f'post-event {time_lag:.0f}m')
                reason = ', '.join(reason_parts)

                links_batch.append((event_id, news_id, round(time_lag, 1), score, reason))

            # Insert links
            if links_batch:
                try:
                    with conn.cursor() as cur:
                        for event_id, news_id, time_lag, score, reason in links_batch:
                            cur.execute("""
                                INSERT INTO event_news_link
                                    (event_id, news_id, time_lag_minutes, match_score, reason)
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (event_id, news_id) DO UPDATE SET
                                    match_score = GREATEST(event_news_link.match_score, EXCLUDED.match_score),
                                    time_lag_minutes = EXCLUDED.time_lag_minutes,
                                    reason = EXCLUDED.reason;
                            """, (event_id, news_id, time_lag, score, reason))
                    conn.commit()
                    total_links += len(links_batch)
                except Exception as e:
                    conn.rollback()
                    _log(f'Insert error event_id={event_id}: {e}')

            if (idx + 1) % 1000 == 0:
                update_progress(conn, job_id, {'events_processed': idx + 1},
                                inserted=total_links)
                _log(f'Processed {idx + 1}/{len(events)} events, {total_links} links')

        finish_job(conn, job_id, status='COMPLETED')
        _log(f'DONE: {total_events} events processed, {total_links} links created, '
             f'{total_skipped} events with no nearby news')

    except Exception as e:
        _log(f'FATAL: {e}')
        traceback.print_exc()
        finish_job(conn, job_id, status='FAILED', error=str(e)[:500])
    finally:
        conn.close()


if __name__ == '__main__':
    main()
