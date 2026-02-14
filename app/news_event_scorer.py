"""
news_event_scorer.py — 5-factor composite news/event scoring axis.

Score: -100 (strong bearish) to +100 (strong bullish).
Magnitude (0-100) = source_quality + category_weight + recency + market_reaction + watchlist
Direction = majority vote of GPT tag, macro_trace ret_2h sign, keyword sentiment
Final score = magnitude * direction_sign

GUARD: This score is a SUPPLEMENTARY indicator only.
       It must NEVER trigger trades alone (enforced in score_engine.py).

Components (5 factors):
  1. source_quality   (0-20): Fixed score per news source
  2. category_weight  (0-25): Fixed score per news category
  3. recency_weight   (0-15): Time decay since publication
  4. market_reaction  (0-25): macro_trace abs(btc_ret_2h) based
  5. watchlist_bonus  (0-15): watch_keywords match count
"""
import os
import sys
import json
import traceback

sys.path.insert(0, '/root/trading-bot/app')

LOG_PREFIX = '[news_event_scorer]'
SYMBOL = 'BTC/USDT:USDT'

# ── Source quality scores (0-20) ──────────────────────────

SOURCE_QUALITY = {
    'bloomberg': 18, 'reuters': 18, 'wsj': 17,
    'cnbc': 16, 'coindesk': 16, 'ft': 16,
    'marketwatch': 15, 'cointelegraph': 15,
    'yahoo': 12, 'decrypt': 12, 'theblock': 14,
    'coinglass': 10, 'cryptopanic': 8,
}
SOURCE_QUALITY_DEFAULT = 8

# ── Category weight scores (0-25) ────────────────────────

CATEGORY_WEIGHT = {
    'FED_RATES': 25, 'CPI_JOBS': 23,
    'REGULATION_SEC_ETF': 22, 'WAR': 20,
    'US_POLITICS': 18, 'NASDAQ_EQUITIES': 17,
    'JAPAN_BOJ': 15, 'CHINA': 15,
    'FIN_STRESS': 18, 'CRYPTO_SPECIFIC': 14,
    'OTHER': 5,
}

# ── Recency weight (0-15) ────────────────────────────────

RECENCY_BRACKETS = [
    (30, 15),     # 0-30 min
    (60, 12),     # 30-60 min
    (120, 8),     # 1-2 hours
    (240, 4),     # 2-4 hours
    (360, 1),     # 4-6 hours
]

# ── Sentiment keywords ───────────────────────────────────

BEARISH_KEYWORDS = {
    '매도', '약세', '하락', '하방', '리스크', 'ban', 'sec', 'down', 'drop',
    'fear', 'hack', 'sell', 'crash', 'bearish', 'decline', 'exploit',
    'lawsuit', 'liquidation', '규제', '급락', '폭락', '해킹'}

BULLISH_KEYWORDS = {
    '강세', '돌파', '매수', '반등', '상방', '상승', '채택', 'up', 'buy',
    'etf', 'pump', 'rise', 'rally', 'surge', 'bullish', 'upgrade',
    'adoption', 'approval', 'partnership', '급등', '승인'}

# ── Watch keywords default ────────────────────────────────

WATCH_KEYWORDS_DEFAULT = [
    'trump', 'fed', 'war', 'boj', 'sec', 'etf', 'nasdaq', 'china',
    'tariff', 'cpi', 'fomc', 'powell', 'rate', 'inflation', 'hack',
    'liquidation', 'ban', 'approval']


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _load_watch_keywords(cur):
    """Load watch keywords from openclaw_policies."""
    try:
        cur.execute("SELECT value FROM openclaw_policies WHERE key = 'watch_keywords';")
        row = cur.fetchone()
        if row and row[0]:
            val = row[0] if isinstance(row[0], list) else json.loads(row[0])
            if isinstance(val, list) and val:
                return [str(k).lower() for k in val]
    except Exception:
        pass
    return WATCH_KEYWORDS_DEFAULT


def _get_recency_score(age_min):
    """Time decay score (0-15)."""
    for bracket_min, score in RECENCY_BRACKETS:
        if age_min < bracket_min:
            return score
    return 0


def _get_source_quality(source):
    """Source quality score (0-20)."""
    if not source:
        return SOURCE_QUALITY_DEFAULT
    s = source.lower().strip()
    for key, score in SOURCE_QUALITY.items():
        if key in s:
            return score
    return SOURCE_QUALITY_DEFAULT


def _parse_direction_tag(summary):
    """Extract direction from summary tag like '[up]...' or '[down]...'."""
    if not summary:
        return 'neutral'
    sl = summary.lower().strip()
    if sl.startswith('[up]') or sl.startswith('[bullish]'):
        return 'up'
    if sl.startswith('[down]') or sl.startswith('[bearish]'):
        return 'down'
    return 'neutral'


def _parse_category_tag(summary):
    """Extract category from summary like '[up] [FED_RATES] ...'."""
    if not summary:
        return 'OTHER'
    import re
    tags = re.findall(r'\[([A-Za-z_]+)\]', summary)
    direction_tags = {'up', 'down', 'neutral', 'bullish', 'bearish'}
    for tag in tags:
        if tag.lower() in direction_tags:
            continue
        if tag in CATEGORY_WEIGHT:
            return tag
    return 'OTHER'


def _keyword_sentiment(text):
    """Keyword-based sentiment: +1 (bullish), -1 (bearish), 0 (neutral)."""
    if not text:
        return 0
    tl = text.lower()
    bull = sum(1 for k in BULLISH_KEYWORDS if k in tl)
    bear = sum(1 for k in BEARISH_KEYWORDS if k in tl)
    if bull > bear:
        return 1
    if bear > bull:
        return -1
    return 0


def _majority_direction(gpt_dir, trace_ret_2h, kw_sentiment):
    """3-way majority vote for direction. Returns +1 or -1."""
    votes = []

    # GPT tag
    if gpt_dir == 'up':
        votes.append(1)
    elif gpt_dir == 'down':
        votes.append(-1)
    else:
        votes.append(0)

    # macro_trace ret_2h sign
    if trace_ret_2h is not None:
        votes.append(1 if trace_ret_2h > 0 else (-1 if trace_ret_2h < 0 else 0))
    else:
        votes.append(0)

    # keyword sentiment
    votes.append(kw_sentiment)

    total = sum(votes)
    return 1 if total >= 0 else -1


def compute(cur):
    """Compute NEWS_EVENT_SCORE with 5-factor composite scoring.

    Returns:
        {
            "score": int (-100 to +100),
            "is_supplementary": True,
            "components": {
                "source_quality": float,
                "category_weight": float,
                "recency": float,
                "market_reaction": float,
                "watchlist": float,
            },
            "details": dict,
            "action_constraints": dict,
        }
    """
    try:
        watch_kw = _load_watch_keywords(cur)

        # Fetch recent news (6h, impact > 0)
        cur.execute("""
            SELECT id, title, source, impact_score, summary,
                   EXTRACT(EPOCH FROM (now() - ts)) / 60 AS age_min,
                   ts, keywords
            FROM news
            WHERE ts >= now() - interval '6 hours'
              AND impact_score > 0
            ORDER BY impact_score DESC, ts DESC
            LIMIT 30;
        """)
        rows = cur.fetchall()

        if not rows:
            return _empty_result()

        # Dedup: same category within 5 min → keep highest impact only
        seen_cat_window = {}  # (category, 5min_bucket) → best_row
        deduped = []
        for r in rows:
            news_id, title, source, impact, summary, age_min, ts, keywords = r
            cat = _parse_category_tag(summary)
            # 5-minute bucket
            bucket = int(age_min // 5)
            key = (cat, bucket)
            if key in seen_cat_window:
                existing_impact = seen_cat_window[key][3]
                if impact > existing_impact:
                    seen_cat_window[key] = r
                    # Replace in deduped
                    deduped = [x for x in deduped if (
                        _parse_category_tag(x[4]), int(x[5] // 5)) != key]
                    deduped.append(r)
            else:
                seen_cat_window[key] = r
                deduped.append(r)

        if not deduped:
            return _empty_result()

        # Fetch macro_trace data for all news_ids
        news_ids = [r[0] for r in deduped]
        traces = {}
        try:
            import macro_trace_computer
            traces = macro_trace_computer.get_traces_for_report(cur, news_ids)
        except Exception:
            pass

        # Compute 5 factors for top news item (highest composite)
        best_magnitude = 0
        best_direction = 0
        best_components = {}
        best_detail = {}
        total_bull = 0
        total_bear = 0
        total_neutral = 0

        # Aggregate across all deduped news
        agg_source = 0.0
        agg_category = 0.0
        agg_recency = 0.0
        agg_reaction = 0.0
        agg_watchlist = 0.0
        agg_weight = 0.0
        direction_votes_sum = 0
        watch_matched = set()

        for r in deduped[:10]:  # top 10 for scoring
            news_id, title, source, impact, summary, age_min, ts, keywords = r
            cat = _parse_category_tag(summary)
            gpt_dir = _parse_direction_tag(summary)
            text = f'{title or ""} {summary or ""}'
            kw_sent = _keyword_sentiment(text)

            # Track sentiment counts
            if gpt_dir == 'up':
                total_bull += 1
            elif gpt_dir == 'down':
                total_bear += 1
            else:
                total_neutral += 1

            # Factor 1: source_quality (0-20)
            f_source = _get_source_quality(source)

            # Factor 2: category_weight (0-25)
            f_category = CATEGORY_WEIGHT.get(cat, 5)

            # Factor 3: recency (0-15)
            f_recency = _get_recency_score(age_min)

            # Factor 4: market_reaction (0-25)
            f_reaction = 0
            trace = traces.get(news_id, {})
            ret_2h = trace.get('btc_ret_2h')
            if ret_2h is not None:
                # 1% abs ret → ~12.5 points, capped at 25
                f_reaction = min(25, round(abs(ret_2h) * 12.5))

            # Factor 5: watchlist_bonus (0-15)
            f_watchlist = 0
            text_lower = text.lower()
            kw_list = list(keywords) if keywords else []
            combined = text_lower + ' ' + ' '.join(str(k).lower() for k in kw_list)
            matched_here = set()
            for wk in watch_kw:
                if wk in combined:
                    matched_here.add(wk)
                    watch_matched.add(wk)
            f_watchlist = min(15, len(matched_here) * 5)

            # Item magnitude
            item_mag = f_source + f_category + f_recency + f_reaction + f_watchlist

            # Direction for this item
            item_dir = _majority_direction(gpt_dir, ret_2h, kw_sent)

            # Weight by impact_score for aggregation
            w = float(impact) if impact else 3.0
            agg_source += f_source * w
            agg_category += f_category * w
            agg_recency += f_recency * w
            agg_reaction += f_reaction * w
            agg_watchlist += f_watchlist * w
            agg_weight += w
            direction_votes_sum += item_dir * w

        # Weighted average factors
        if agg_weight > 0:
            comp_source = round(agg_source / agg_weight, 1)
            comp_category = round(agg_category / agg_weight, 1)
            comp_recency = round(agg_recency / agg_weight, 1)
            comp_reaction = round(agg_reaction / agg_weight, 1)
            comp_watchlist = round(agg_watchlist / agg_weight, 1)
        else:
            comp_source = comp_category = comp_recency = comp_reaction = comp_watchlist = 0

        magnitude = round(comp_source + comp_category + comp_recency +
                          comp_reaction + comp_watchlist)
        magnitude = max(0, min(100, magnitude))

        direction_sign = 1 if direction_votes_sum >= 0 else -1
        score = magnitude * direction_sign
        score = max(-100, min(100, score))

        components = {
            'source_quality': comp_source,
            'category_weight': comp_category,
            'recency': comp_recency,
            'market_reaction': comp_reaction,
            'watchlist': comp_watchlist,
        }

        # Score trace string for report
        dir_char = '+' if direction_sign > 0 else '-'
        score_trace = (f"source:{comp_source:.0f} + cat:{comp_category:.0f} + "
                       f"recency:{comp_recency:.0f} + reaction:{comp_reaction:.0f} + "
                       f"watch:{comp_watchlist:.0f} = {magnitude} -> "
                       f"방향:{dir_char}")

        return {
            'score': score,
            'is_supplementary': True,
            'components': components,
            'details': {
                'news_count': len(deduped),
                'deduped_from': len(rows),
                'bullish': total_bull,
                'bearish': total_bear,
                'neutral': total_neutral,
                'watch_matched': sorted(watch_matched),
                'score_trace': score_trace,
            },
            'action_constraints': {
                'can_open': False,
                'can_reverse': False,
                'can_reduce': True,
                'can_tighten_sl': True,
            },
        }

    except Exception as e:
        _log(f'compute error: {e}')
        traceback.print_exc()
        return _empty_result()


def _empty_result():
    """Return empty/zero result."""
    return {
        'score': 0,
        'is_supplementary': True,
        'components': {
            'source_quality': 0,
            'category_weight': 0,
            'recency': 0,
            'market_reaction': 0,
            'watchlist': 0,
        },
        'details': {
            'news_count': 0,
            'deduped_from': 0,
            'bullish': 0, 'bearish': 0, 'neutral': 0,
            'watch_matched': [],
            'score_trace': '',
        },
        'action_constraints': {
            'can_open': False,
            'can_reverse': False,
            'can_reduce': True,
            'can_tighten_sl': True,
        },
    }


if __name__ == '__main__':
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
