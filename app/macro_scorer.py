"""
macro_scorer.py — News/event-based macro scorer.

Score: -100 (strong bearish macro) to +100 (strong bullish macro).

Components:
  - news_sentiment:    Time-weighted sentiment from news table
  - event_similarity:  Historical pattern matching from event_history

Also provides:
  - record_event():      Store new event to event_history
  - backfill_from_news(): Backfill event_history from existing news table
"""
import os
import sys
import json
import math
import traceback
sys.path.insert(0, '/root/trading-bot/app')
LOG_PREFIX = '[macro_scorer]'
SYMBOL = 'BTC/USDT:USDT'

BEARISH_KEYWORDS = {
    '매도', '약세', '하락', '하방', '리스크', 'ban', 'sec', 'down', 'drop',
    'fear', 'hack', 'sell', 'crash', 'bearish', 'decline', 'exploit',
    'lawsuit', 'liquidation', '규제', '급락', '폭락', '해킹'}

BULLISH_KEYWORDS = {
    '강세', '돌파', '매수', '반등', '상방', '상승', '채택', 'up', 'buy',
    'etf', 'pump', 'rise', 'rally', 'surge', 'bullish', 'upgrade',
    'adoption', 'approval', 'partnership', '급등', '승인'}

EVENT_TYPES = {
    'fed_rate': ['fomc', '금리', '연준', 'fed', 'rate', 'powell'],
    'regulation': ['규제', 'sec', 'ban', 'regulation', 'lawsuit', '제재'],
    'etf': ['etf', '승인', 'approval', 'spot'],
    'hack': ['해킹', 'hack', 'exploit', 'drain', 'breach'],
    'macro_crash': ['폭락', 'crash', 'black swan', '급락'],
    'rally': ['급등', 'rally', 'surge', 'pump', 'ath'],
    'liquidation': ['청산', 'liquidation', 'cascade', 'long squeeze', 'short squeeze']}


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _compute_news_sentiment(cur=None):
    '''Compute time-weighted news sentiment from news table.

    Recent news (< 30min) weighted 1.0x.
    Older news (30min-2h) weighted 0.5x.
    Old news (2h-6h) weighted 0.2x.

    Returns: {score: int (-60 to +60), details: dict}
    '''
    try:
        cur.execute("""
            SELECT summary, impact_score, ts,
                   EXTRACT(EPOCH FROM (now() - ts)) / 60 as age_min
            FROM news
            WHERE ts >= now() - interval '6 hours'
              AND summary IS NOT NULL
            ORDER BY ts DESC LIMIT 50;
        """)
        rows = cur.fetchall()
        if not rows:
            return {'score': 0, 'details': {'count': 0}}

        bullish_total = 0
        bearish_total = 0
        count = 0
        for summary, impact, ts, age_min in rows:
            text = (summary or '').lower()
            impact_val = float(impact) if impact else 3

            # Time weight
            if age_min < 30:
                weight = 1.0
            elif age_min < 120:
                weight = 0.5
            else:
                weight = 0.2

            # Classify
            bull_hits = sum(1 for k in BULLISH_KEYWORDS if k in text)
            bear_hits = sum(1 for k in BEARISH_KEYWORDS if k in text)

            if bull_hits > bear_hits:
                bullish_total += impact_val * weight
            elif bear_hits > bull_hits:
                bearish_total += impact_val * weight
            count += 1

        # Normalize to -60 to +60
        net = bullish_total - bearish_total
        total = bullish_total + bearish_total
        if total == 0:
            score = 0
        else:
            score = int(round((net / total) * 60))

        return {
            'score': max(-60, min(60, score)),
            'details': {
                'count': count,
                'bullish_total': round(bullish_total, 1),
                'bearish_total': round(bearish_total, 1),
            }}
    except Exception:
        traceback.print_exc()
        return {'score': 0, 'details': {'error': 'query failed'}}


def _classify_event_type(text=None):
    '''Classify text into event type based on keywords.'''
    if not text:
        return 'general'
    text_lower = text.lower()
    best_type = 'general'
    best_count = 0
    for etype, keywords in EVENT_TYPES.items():
        hits = sum(1 for k in keywords if k in text_lower)
        if hits > best_count:
            best_count = hits
            best_type = etype
    return best_type


def _find_similar_events(cur=None, event_type=None, keywords=None, limit=5):
    '''Find similar historical events from event_history.
    Returns list of {headline, direction, btc_move_1h, btc_move_4h, btc_move_24h, impact_score}.'''
    if event_type:
        cur.execute("""
                SELECT headline, direction, btc_move_1h, btc_move_4h, btc_move_24h,
                       impact_score, ts
                FROM event_history
                WHERE event_type = %s AND outcome_filled = true
                ORDER BY ts DESC LIMIT %s;
            """, (event_type, limit))
    elif keywords:
        cur.execute("""
                SELECT headline, direction, btc_move_1h, btc_move_4h, btc_move_24h,
                       impact_score, ts
                FROM event_history
                WHERE keywords && %s AND outcome_filled = true
                ORDER BY ts DESC LIMIT %s;
            """, (keywords, limit))
    else:
        return []

    results = []
    for row in cur.fetchall():
        results.append({
            'headline': row[0],
            'direction': row[1],
            'btc_move_1h': float(row[2]) if row[2] else None,
            'btc_move_4h': float(row[3]) if row[3] else None,
            'btc_move_24h': float(row[4]) if row[4] else None,
            'impact_score': int(row[5]) if row[5] else None,
            'ts': str(row[6])})
    return results


def _score_event_similarity(similar_events=None):
    '''Score based on historical similar event outcomes: -40 to +40.'''
    if not similar_events:
        return 0
    weighted_move = 0
    total_weight = 0
    for evt in similar_events:
        move = evt.get('btc_move_4h') or evt.get('btc_move_1h') or 0
        impact = evt.get('impact_score') or 5
        weight = impact / 10
        weighted_move += move * weight
        total_weight += weight
    if total_weight == 0:
        return 0
    avg_move = weighted_move / total_weight
    score = (avg_move / 5) * 40
    return max(-40, min(40, int(round(score))))


def compute(cur=None):
    '''Compute macro event score.

    Returns:
        {
            "score": int (-100 to +100),
            "components": {
                "news_sentiment": int,
                "event_similarity": int,
            },
            "similar_events": list[dict],
            "news_detail": dict,
        }
    '''
    news = _compute_news_sentiment(cur)
    news_score = news['score']
    similar_events = []
    event_sim_score = 0

    try:
        cur.execute("""
                SELECT summary FROM news
                WHERE ts >= now() - interval '1 hour'
                  AND impact_score IS NOT NULL AND impact_score >= 5
                ORDER BY ts DESC LIMIT 5;
            """)
        high_impact = cur.fetchall()
        if high_impact:
            combined_text = ' '.join(r[0] for r in high_impact if r[0])
            event_type = _classify_event_type(combined_text)
            if event_type != 'general':
                similar_events = _find_similar_events(cur, event_type=event_type)
                event_sim_score = _score_event_similarity(similar_events)
    except Exception:
        traceback.print_exc()

    total = news_score + event_sim_score
    total = max(-100, min(100, total))

    return {
        'score': total,
        'components': {
            'news_sentiment': news_score,
            'event_similarity': event_sim_score},
        'similar_events': similar_events[:3],
        'news_detail': news}


def record_event(cur, headline, event_type=None, keywords=None,
                  source='manual', impact_score=5, direction=None,
                  news_id=None, btc_price=None):
    '''Record a new event to event_history. Returns event id.'''
    if not event_type:
        event_type = _classify_event_type(headline)
    if not keywords:
        text_lower = headline.lower()
        keywords = []
        for kw_list in EVENT_TYPES.values():
            for kw in kw_list:
                if kw in text_lower:
                    keywords.append(kw)
    try:
        cur.execute("""
            INSERT INTO event_history
                (event_type, keywords, headline, source, news_id,
                 impact_score, direction, btc_price_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
        """, (event_type, keywords, headline, source, news_id,
              impact_score, direction, btc_price))
        row = cur.fetchone()
        return row[0] if row else None
    except Exception:
        traceback.print_exc()
        return None


def backfill_from_news(cur=None, days=None):
    '''Backfill event_history from existing news table.
    Only processes high-impact news (impact_score >= 5).'''
    cur.execute("""
            SELECT id, ts, summary, impact_score
            FROM news
            WHERE ts >= now() - make_interval(days => %s)
              AND impact_score IS NOT NULL AND impact_score >= 5
              AND id NOT IN (SELECT news_id FROM event_history WHERE news_id IS NOT NULL)
            ORDER BY ts ASC;
        """, (days,))
    rows = cur.fetchall()
    count = 0
    for news_id, ts, summary, impact in rows:
        if not summary:
            continue
        cur.execute("""
                SELECT c FROM candles
                WHERE symbol = %s AND tf = '1m' AND ts <= %s
                ORDER BY ts DESC LIMIT 1;
            """, (SYMBOL, ts))
        price_row = cur.fetchone()
        btc_price = float(price_row[0]) if price_row and price_row[0] else None
        record_event(cur, headline=summary, source='backfill_news',
                      impact_score=int(impact), news_id=news_id, btc_price=btc_price)
        count += 1
    _log(f'backfill_from_news: processed {count}/{len(rows)} news items')
    return count


if __name__ == '__main__':
    from db_config import get_conn
    conn = None
    try:
        conn = get_conn(autocommit=True)
        with conn.cursor() as cur:
            result = compute(cur)
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    except Exception:
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
