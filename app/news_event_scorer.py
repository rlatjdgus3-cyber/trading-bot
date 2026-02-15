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

CATEGORY_WEIGHT_DEFAULT = {
    'FED_RATES': 25, 'CPI_JOBS': 23,
    'REGULATION_SEC_ETF': 22, 'WAR': 20,
    'US_POLITICS': 18, 'NASDAQ_EQUITIES': 17,
    'JAPAN_BOJ': 15, 'CHINA': 15,
    'FIN_STRESS': 18, 'CRYPTO_SPECIFIC': 14,
    'OTHER': 5,
}
# Will be populated from DB if available
CATEGORY_WEIGHT = dict(CATEGORY_WEIGHT_DEFAULT)

# Cache for DB-loaded weights
_db_weights_loaded = False
_db_weights_version = None

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

# ── Relevance filter ──────────────────────────────────────

LIST_ARTICLE_KEYWORDS = {'5선', 'top', 'best', 'list', '추천', '정리',
                         'roundup', 'picks', 'ranking', '순위'}
RELEVANCE_MULTIPLIERS = {'HIGH': 1.0, 'MED': 0.5, 'LOW': 0.0}

CRYPTO_DIRECT_KEYWORDS = {
    'bitcoin', 'btc', 'ethereum', 'eth', 'crypto', 'blockchain',
    'defi', 'nft', 'altcoin', 'stablecoin', 'usdt', 'usdc',
    'binance', 'coinbase', 'bybit', 'okx', 'kraken',
    'halving', 'mining', 'hash rate',
    'etf', 'fed', 'cpi', 'inflation', 'sec', 'fomc',
    '비트코인', '이더리움', '가상자산', '암호화폐', '코인',
}


def _classify_relevance(title, category, impact_score, summary=''):
    """HIGH / MED / LOW 분류. OTHER 카테고리에서 crypto 키워드 2개 미만 → LOW."""
    title_lower = (title or '').lower()
    impact = int(impact_score or 0)
    if any(kw in title_lower for kw in LIST_ARTICLE_KEYWORDS):
        return 'LOW'
    if category == 'OTHER' and impact <= 3:
        return 'LOW'
    # OTHER 카테고리: crypto 키워드 2개 미만이면 LOW
    # (FED_RATES, CPI_JOBS 등 매크로 카테고리는 OTHER가 아니므로 자연 면제)
    if category == 'OTHER':
        combined = title_lower + ' ' + (summary or '').lower()
        crypto_hits = sum(1 for kw in CRYPTO_DIRECT_KEYWORDS if kw in combined)
        if crypto_hits < 2:
            return 'LOW'
    if impact >= 7:
        return 'HIGH'
    if category == 'OTHER':
        return 'MED'
    if impact >= 5:
        return 'HIGH'
    return 'MED'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _load_db_category_weights(cur):
    """Load category weights from news_impact_stats (avg_abs_ret_2h based).

    Maps avg_abs_ret_2h to 0-25 score: higher abs return = higher weight.
    Falls back to hardcoded CATEGORY_WEIGHT_DEFAULT if DB unavailable.
    Returns (weights_dict, stats_version, total_samples).
    """
    global _db_weights_loaded, _db_weights_version, CATEGORY_WEIGHT
    try:
        cur.execute("""
            SELECT event_type, avg_abs_ret_2h, sample_count, stats_version
            FROM news_impact_stats
            WHERE region = 'GLOBAL'
            ORDER BY avg_abs_ret_2h DESC;
        """)
        rows = cur.fetchall()
        if not rows:
            return (dict(CATEGORY_WEIGHT_DEFAULT), None, 0)

        # Scale: max avg_abs_ret_2h → 25, min → 5
        abs_rets = [float(r[1]) for r in rows if r[1] is not None]
        if not abs_rets:
            return (dict(CATEGORY_WEIGHT_DEFAULT), None, 0)

        max_ret = max(abs_rets) if abs_rets else 1.0
        min_ret = min(abs_rets) if abs_rets else 0.0
        range_ret = max_ret - min_ret if max_ret > min_ret else 1.0

        db_weights = {}
        total_samples = 0
        version = None
        for event_type, avg_abs, sample_count, sv in rows:
            if avg_abs is None:
                continue
            # Scale to 5-25 range
            normalized = (float(avg_abs) - min_ret) / range_ret
            weight = int(round(5 + normalized * 20))
            weight = max(5, min(25, weight))
            # Only use if enough samples
            if sample_count and int(sample_count) >= 5:
                db_weights[event_type] = weight
            total_samples += int(sample_count or 0)
            if sv:
                version = sv

        # Merge: DB weights override defaults, keep defaults for missing categories
        merged = dict(CATEGORY_WEIGHT_DEFAULT)
        merged.update(db_weights)
        CATEGORY_WEIGHT = merged
        _db_weights_loaded = True
        _db_weights_version = version

        _log(f'DB weights loaded: {len(db_weights)} categories, '
             f'version={version}, total_samples={total_samples}')
        return (merged, version, total_samples)

    except Exception as e:
        _log(f'DB weights load failed (using defaults): {e}')
        return (dict(CATEGORY_WEIGHT_DEFAULT), None, 0)


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
        # Load DB-based category weights (with fallback)
        db_cat_weights, stats_version, total_samples = _load_db_category_weights(cur)

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

            # Relevance filter: LOW=0.0, MED=0.5
            relevance = _classify_relevance(title, cat, impact, summary)
            relevance_mult = RELEVANCE_MULTIPLIERS.get(relevance, 1.0)
            effective_w = w * relevance_mult
            _log(f'relevance: "{(title or "")[:40]}" cat={cat} impact={impact} '
                 f'level={relevance} mag={item_mag} eff_w={effective_w:.1f}')

            agg_source += f_source * effective_w
            agg_category += f_category * effective_w
            agg_recency += f_recency * effective_w
            agg_reaction += f_reaction * effective_w
            agg_watchlist += f_watchlist * effective_w
            agg_weight += effective_w
            direction_votes_sum += item_dir * effective_w

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
                'stats_version': stats_version,
                'stats_total_samples': total_samples,
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
    from db_config import get_conn
    conn = get_conn(autocommit=True)
    with conn.cursor() as cur:
        result = compute(cur)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    conn.close()
