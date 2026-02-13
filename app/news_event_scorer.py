"""
news_event_scorer.py — Supplementary news/event scoring axis.

Score: -100 (strong bearish news signal) to +100 (strong bullish news signal).

GUARD: This score is a SUPPLEMENTARY indicator only.
       It must NEVER trigger trades alone (enforced in score_engine.py).

Components:
  1. recent_sentiment  (-40 to +40): Time-weighted keyword sentiment from news (last 6h)
  2. category_bias     (-30 to +30): Historical category return bias from category_stats
  3. event_similarity  (-30 to +30): Historical event outcome pattern matching
"""
import os
import sys
import traceback

sys.path.insert(0, '/root/trading-bot/app')
import fact_categories

LOG_PREFIX = '[news_event_scorer]'
SYMBOL = 'BTC/USDT:USDT'

BEARISH_KEYWORDS = {
    '매도', '약세', '하락', '하방', '리스크', 'ban', 'sec', 'down', 'drop',
    'fear', 'hack', 'sell', 'crash', 'bearish', 'decline', 'exploit',
    'lawsuit', 'liquidation', '규제', '급락', '폭락', '해킹'}

BULLISH_KEYWORDS = {
    '강세', '돌파', '매수', '반등', '상방', '상승', '채택', 'up', 'buy',
    'etf', 'pump', 'rise', 'rally', 'surge', 'bullish', 'upgrade',
    'adoption', 'approval', 'partnership', '급등', '승인'}


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _compute_recent_sentiment(cur):
    """Time-weighted sentiment from recent news. Returns score -40 to +40."""
    try:
        cur.execute("""
            SELECT summary, impact_score,
                   EXTRACT(EPOCH FROM (now() - ts)) / 60 as age_min
            FROM news
            WHERE ts >= now() - interval '6 hours'
              AND summary IS NOT NULL
            ORDER BY ts DESC LIMIT 50;
        """)
        rows = cur.fetchall()
        if not rows:
            return 0, {'count': 0}

        bullish_total = 0.0
        bearish_total = 0.0
        count = 0
        for summary, impact, age_min in rows:
            text = (summary or '').lower()
            impact_val = float(impact) if impact else 3.0

            if age_min < 30:
                weight = 1.0
            elif age_min < 120:
                weight = 0.5
            else:
                weight = 0.2

            bull_hits = sum(1 for k in BULLISH_KEYWORDS if k in text)
            bear_hits = sum(1 for k in BEARISH_KEYWORDS if k in text)

            if bull_hits > bear_hits:
                bullish_total += impact_val * weight
            elif bear_hits > bull_hits:
                bearish_total += impact_val * weight
            count += 1

        net = bullish_total - bearish_total
        total = bullish_total + bearish_total
        if total == 0:
            score = 0
        else:
            score = int(round((net / total) * 40))

        return max(-40, min(40, score)), {
            'count': count,
            'bullish': round(bullish_total, 1),
            'bearish': round(bearish_total, 1),
        }
    except Exception:
        traceback.print_exc()
        return 0, {'error': 'query_failed'}


def _compute_category_bias(cur):
    """Historical category return bias from category_stats + recent news categories.

    Looks at what categories are active in recent news, then checks their
    historical avg returns to derive a directional bias.
    Returns score -30 to +30.
    """
    try:
        # Aggregate per-category to avoid double-counting when multiple
        # news items share the same category in the recent window
        cur.execute("""
            SELECT nmr.category,
                   cs.avg_ret_4h, cs.sample_count, cs.hit_rate_4h,
                   COUNT(*) AS recent_count
            FROM news_market_reaction nmr
            JOIN category_stats cs ON cs.category = nmr.category
            WHERE nmr.ts_news >= now() - interval '6 hours'
              AND nmr.category IS NOT NULL
              AND cs.sample_count >= 5
            GROUP BY nmr.category, cs.avg_ret_4h, cs.sample_count, cs.hit_rate_4h;
        """)
        rows = cur.fetchall()
        if not rows:
            return 0, {'active_categories': []}

        weighted_bias = 0.0
        total_weight = 0.0
        active_cats = []
        for category, avg_ret_4h, sample_count, hit_rate_4h, recent_count in rows:
            active_cats.append(category)
            avg_ret = float(avg_ret_4h) if avg_ret_4h else 0
            samples = int(sample_count) if sample_count else 0
            confidence = min(1.0, samples / 50)
            # Weight slightly more for categories with more recent news
            recency_boost = min(1.5, 1.0 + (int(recent_count) - 1) * 0.1)
            weighted_bias += avg_ret * confidence * recency_boost
            total_weight += confidence * recency_boost

        if total_weight == 0:
            return 0, {'active_categories': active_cats}

        avg_bias = weighted_bias / total_weight
        # Normalize: 1% avg ret = 30 points
        score = int(round(avg_bias * 30 / 1.0))
        return max(-30, min(30, score)), {
            'active_categories': active_cats,
            'avg_bias_pct': round(avg_bias, 4),
        }
    except Exception:
        traceback.print_exc()
        return 0, {'error': 'query_failed'}


def _compute_event_similarity(cur):
    """Historical event outcome matching. Returns score -30 to +30."""
    try:
        # Find recent high-impact news
        cur.execute("""
            SELECT summary FROM news
            WHERE ts >= now() - interval '2 hours'
              AND impact_score IS NOT NULL AND impact_score >= 5
            ORDER BY ts DESC LIMIT 5;
        """)
        high_impact = cur.fetchall()
        if not high_impact:
            return 0, {'similar_count': 0}

        combined_text = ' '.join(r[0] for r in high_impact if r[0])
        if not combined_text:
            return 0, {'similar_count': 0}

        # Classify and find similar events
        category = fact_categories.classify_news(combined_text, '')
        keywords = fact_categories.extract_macro_keywords(combined_text)

        similar = []
        if category:
            cur.execute("""
                SELECT direction, btc_move_1h, btc_move_4h, btc_move_24h,
                       impact_score, event_type
                FROM event_history
                WHERE event_type = %s AND outcome_filled = true
                ORDER BY ts DESC LIMIT 10;
            """, (category,))
            similar = cur.fetchall()

        if not similar and keywords:
            cur.execute("""
                SELECT direction, btc_move_1h, btc_move_4h, btc_move_24h,
                       impact_score, event_type
                FROM event_history
                WHERE keywords && %s AND outcome_filled = true
                ORDER BY ts DESC LIMIT 10;
            """, (keywords[:5],))
            similar = cur.fetchall()

        if not similar:
            return 0, {'similar_count': 0, 'category': category}

        # Weighted average of historical moves
        weighted_move = 0.0
        total_weight = 0.0
        for direction, m1h, m4h, m24h, impact, etype in similar:
            move = float(m4h) if m4h else (float(m1h) if m1h else 0)
            w = (float(impact) if impact else 5) / 10.0
            weighted_move += move * w
            total_weight += w

        if total_weight == 0:
            return 0, {'similar_count': len(similar), 'category': category}

        avg_move = weighted_move / total_weight
        # Normalize: 5% avg move = 30 points
        score = int(round((avg_move / 5.0) * 30))
        return max(-30, min(30, score)), {
            'similar_count': len(similar),
            'category': category,
            'avg_move_pct': round(avg_move, 4),
        }
    except Exception:
        traceback.print_exc()
        return 0, {'error': 'query_failed'}


def compute(cur):
    """Compute NEWS_EVENT_SCORE.

    Returns:
        {
            "score": int (-100 to +100),
            "is_supplementary": True,  # GUARD FLAG
            "components": {
                "recent_sentiment": int,
                "category_bias": int,
                "event_similarity": int,
            },
            "details": dict,
        }
    """
    sent_score, sent_detail = _compute_recent_sentiment(cur)
    cat_score, cat_detail = _compute_category_bias(cur)
    evt_score, evt_detail = _compute_event_similarity(cur)

    total = sent_score + cat_score + evt_score
    total = max(-100, min(100, total))

    return {
        'score': total,
        'is_supplementary': True,  # MUST remain True — guard for score_engine
        'components': {
            'recent_sentiment': sent_score,
            'category_bias': cat_score,
            'event_similarity': evt_score,
        },
        'details': {
            'sentiment': sent_detail,
            'category': cat_detail,
            'event': evt_detail,
        },
    }


if __name__ == '__main__':
    import json
    import psycopg2
    from dotenv import load_dotenv
    load_dotenv('/root/trading-bot/app/.env')
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
    conn.close()
