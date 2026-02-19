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
import re
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
    'yahoo': 8, 'decrypt': 12, 'theblock': 14,
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
                         'roundup', 'picks', 'ranking', '순위',
                         'quiz', 'recipe', 'travel', 'lifestyle',
                         'celebrity', 'entertainment', 'sports', 'dating'}
MINIMUM_IMPACT_FOR_SCORING = 3
RELEVANCE_MULTIPLIERS = {'HIGH': 1.0, 'MED': 0.5, 'LOW': 0.0}

# ── Tier multipliers ─────────────────────────────────────
TIER_MULTIPLIERS = {
    'TIER1': 1.0,
    'TIER2': 0.7,
    'TIER3': 0.1,
    'TIERX': 0.0,
    'UNKNOWN': 0.5,  # 기존 데이터 호환: tier 미분류 → 50% 반영
}

# ── 전략 반영 카테고리 ────────────────────────────────────
# STRATEGY_CATEGORIES에 포함된 카테고리만 전략 점수에 의미있게 반영
STRATEGY_TIER1 = {'FED_RATES', 'CPI_JOBS', 'REGULATION_SEC_ETF', 'WAR'}
STRATEGY_TIER2 = {'NASDAQ_EQUITIES', 'US_POLITICS', 'FIN_STRESS', 'CRYPTO_SPECIFIC'}
STRATEGY_CATEGORIES = STRATEGY_TIER1 | STRATEGY_TIER2
# 카테고리별 전략 가중치
STRATEGY_CATEGORY_MULT = {}
for _c in STRATEGY_TIER1:
    STRATEGY_CATEGORY_MULT[_c] = 1.0
for _c in STRATEGY_TIER2:
    STRATEGY_CATEGORY_MULT[_c] = 0.7
MINIMUM_RELEVANCE_FOR_SCORING = 0.6

CRYPTO_DIRECT_KEYWORDS = {
    'bitcoin', 'btc', 'ethereum', 'eth', 'crypto', 'blockchain',
    'defi', 'nft', 'altcoin', 'stablecoin', 'usdt', 'usdc',
    'binance', 'coinbase', 'bybit', 'okx', 'kraken',
    'halving', 'mining', 'hash rate',
    'etf', 'fed', 'cpi', 'inflation', 'sec', 'fomc',
    '비트코인', '이더리움', '가상자산', '암호화폐', '코인',
}


def _classify_relevance(title, category, impact_score, summary='',
                        tier=None, relevance_score=None):
    """HIGH / MED / LOW 분류.

    DB relevance_score 우선 사용. TIERX → 항상 LOW.
    """
    # TIERX → 항상 LOW
    if tier == 'TIERX':
        return 'LOW'

    # DB relevance_score가 있으면 우선 사용
    if relevance_score is not None:
        if relevance_score >= 0.8:
            return 'HIGH'
        elif relevance_score >= 0.6:
            return 'MED'
        else:
            return 'LOW'

    # 기존 휴리스틱 fallback
    title_lower = (title or '').lower()
    impact = int(impact_score or 0)
    if any(kw in title_lower for kw in LIST_ARTICLE_KEYWORDS):
        return 'LOW'
    if category == 'OTHER' and impact <= 3:
        return 'LOW'
    if category == 'OTHER':
        combined = title_lower + ' ' + (summary or '').lower()
        crypto_hits = sum(1 for kw in CRYPTO_DIRECT_KEYWORDS if kw in combined)
        if crypto_hits < 3:
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
            if sample_count and int(sample_count) >= 10:
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


MACRO_CATEGORIES = {
    'FED_RATES', 'CPI_JOBS', 'NASDAQ_EQUITIES', 'US_POLITICS',
    'WAR', 'JAPAN_BOJ', 'CHINA', 'FIN_STRESS',
}
CRYPTO_CATEGORIES = {'CRYPTO_SPECIFIC', 'REGULATION_SEC_ETF'}

# Macro/crypto score blend weights
MACRO_WEIGHT = 0.6
CRYPTO_WEIGHT = 0.4


def _macro_corroboration(cur, direction_sign):
    """QQQ 2시간 변동과 뉴스 방향 일치 여부 확인.

    일치 시 보너스 (+5점), 불일치 시 감점 (-3점).
    """
    try:
        cur.execute("""
            WITH latest AS (SELECT price FROM macro_data WHERE source='QQQ' ORDER BY ts DESC LIMIT 1),
                 past AS (SELECT price FROM macro_data WHERE source='QQQ' AND ts <= now()-interval '2h' ORDER BY ts DESC LIMIT 1)
            SELECT (SELECT price FROM latest) - (SELECT price FROM past);
        """)
        row = cur.fetchone()
        if row and row[0]:
            qqq_change = float(row[0])
            if (qqq_change > 0 and direction_sign > 0) or (qqq_change < 0 and direction_sign < 0):
                return 5   # 일치 보너스
            elif abs(qqq_change) > 1.0:
                return -3  # 불일치 감점
    except Exception:
        pass
    return 0


def _get_macro_context(cur):
    """Get current QQQ/VIX values for score_trace context."""
    try:
        cur.execute("""
            SELECT DISTINCT ON (source) source, price
            FROM macro_data
            WHERE source IN ('QQQ', 'VIX')
            ORDER BY source, ts DESC;
        """)
        return {r[0]: float(r[1]) for r in cur.fetchall() if r[1]}
    except Exception:
        return {}


def _score_group(items, traces, watch_kw, row_tiers=None, row_rel_scores=None,
                  source_accuracy=None):
    """Score a group of news items (macro or crypto). Returns (magnitude, direction_sign, components, stats)."""
    agg_source = 0.0
    agg_category = 0.0
    agg_recency = 0.0
    agg_reaction = 0.0
    agg_watchlist = 0.0
    agg_weight = 0.0
    direction_votes_sum = 0
    watch_matched = set()
    total_bull = 0
    total_bear = 0
    total_neutral = 0
    row_tiers = row_tiers or {}
    row_rel_scores = row_rel_scores or {}
    source_accuracy = source_accuracy or {}

    for r in items[:10]:
        news_id = r[0]
        title, source, impact, summary, age_min, ts, keywords = r[1], r[2], r[3], r[4], r[5], r[6], r[7]
        cat = _parse_category_tag(summary)
        gpt_dir = _parse_direction_tag(summary)
        text = f'{title or ""} {summary or ""}'
        kw_sent = _keyword_sentiment(text)

        if gpt_dir == 'up':
            total_bull += 1
        elif gpt_dir == 'down':
            total_bear += 1
        else:
            total_neutral += 1

        f_source = _get_source_quality(source)
        f_category = CATEGORY_WEIGHT.get(cat, 5)
        f_recency = _get_recency_score(age_min)

        f_reaction = 0
        trace = traces.get(news_id, {})
        ret_2h = trace.get('btc_ret_2h')
        ret_24h = trace.get('btc_ret_24h')
        spike_z = trace.get('spike_zscore')
        if ret_2h is not None:
            base = min(25, abs(ret_2h) * 12.5)
            confirm_24h = min(5, abs(ret_24h) * 2.5) if ret_24h else 0
            vol_bonus = min(5, spike_z * 1.5) if spike_z and spike_z > 1.0 else 0
            f_reaction = min(25, round(base + confirm_24h * 0.3 + vol_bonus * 0.2))

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

        item_mag = f_source + f_category + f_recency + f_reaction + f_watchlist
        item_dir = _majority_direction(gpt_dir, ret_2h, kw_sent)

        # 티어/relevance_score 조회 (classify 전에 먼저 로드)
        item_tier = row_tiers.get(news_id, 'UNKNOWN')
        item_rel_score = row_rel_scores.get(news_id)

        w = float(impact) if impact else 3.0
        relevance = _classify_relevance(title, cat, impact, summary,
                                        tier=item_tier,
                                        relevance_score=item_rel_score)
        relevance_mult = RELEVANCE_MULTIPLIERS.get(relevance, 1.0)
        effective_w = w * relevance_mult

        # 티어 승수 적용
        tier_mult = TIER_MULTIPLIERS.get(item_tier, 0.0)
        effective_w *= tier_mult

        # 전략 카테고리 승수: STRATEGY_CATEGORIES 외 → 0.05 (사실상 무시)
        strat_mult = STRATEGY_CATEGORY_MULT.get(cat, 0.05)
        effective_w *= strat_mult

        # relevance_score < 0.6 → 승수 0 (전략 반영 차단)
        if item_rel_score is not None and item_rel_score < MINIMUM_RELEVANCE_FOR_SCORING:
            effective_w = 0.0

        # 소스 신뢰도 감소: hit_rate < 0.4 (20건+) → 50% 감소
        sa_key = ((source or '').lower(), 'ALL')
        sa_info = source_accuracy.get(sa_key, {})
        if sa_info.get('total', 0) >= 20 and sa_info.get('hit_rate', 1.0) < 0.4:
            effective_w *= 0.5

        agg_source += f_source * effective_w
        agg_category += f_category * effective_w
        agg_recency += f_recency * effective_w
        agg_reaction += f_reaction * effective_w
        agg_watchlist += f_watchlist * effective_w
        agg_weight += effective_w
        direction_votes_sum += item_dir * effective_w

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

    components = {
        'source_quality': comp_source,
        'category_weight': comp_category,
        'recency': comp_recency,
        'market_reaction': comp_reaction,
        'watchlist': comp_watchlist,
    }
    stats = {
        'bullish': total_bull,
        'bearish': total_bear,
        'neutral': total_neutral,
        'watch_matched': watch_matched,
        'count': len(items),
    }
    return magnitude, direction_sign, components, stats


def compute(cur):
    """Compute NEWS_EVENT_SCORE with 5-factor composite scoring.

    매크로 vs 크립토 분리 스코어링 후 가중 합산.
    macro_data 연동으로 방향 일치/불일치 보정.

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

        # Load source accuracy for trust weighting
        source_accuracy = {}
        try:
            cur.execute("""
                SELECT source, category, hit_rate, total_predictions
                FROM news_source_accuracy
                WHERE total_predictions >= 5;
            """)
            for sa_row in cur.fetchall():
                source_accuracy[(sa_row[0], sa_row[1])] = {
                    'hit_rate': float(sa_row[2]) if sa_row[2] else 0.0,
                    'total': int(sa_row[3]) if sa_row[3] else 0,
                }
        except Exception:
            pass

        # Fetch recent news (6h, impact >= MINIMUM_IMPACT_FOR_SCORING, exclude TIERX)
        cur.execute("""
            SELECT id, title, source, impact_score, summary,
                   EXTRACT(EPOCH FROM (now() - ts)) / 60 AS age_min,
                   ts, keywords,
                   COALESCE(tier, 'UNKNOWN') AS tier,
                   relevance_score
            FROM news
            WHERE ts >= now() - interval '6 hours'
              AND impact_score >= %(min_impact)s
              AND COALESCE(tier, 'UNKNOWN') NOT IN ('TIERX')
              AND exclusion_reason IS NULL
            ORDER BY impact_score DESC, ts DESC
            LIMIT 30;
        """, {'min_impact': MINIMUM_IMPACT_FOR_SCORING})
        rows = cur.fetchall()

        if not rows:
            return _empty_result()

        # Precompute category and tier for each row
        row_cats = {r[0]: _parse_category_tag(r[4]) for r in rows}
        row_tiers = {r[0]: r[8] for r in rows}
        row_rel_scores = {r[0]: float(r[9]) if r[9] is not None else None for r in rows}

        # Dedup: same category within 5 min → keep highest source_quality item
        seen_cat_window = {}  # (category, 5min_bucket) → news_id
        deduped = []
        deduped_ids = set()
        for r in rows:
            news_id, title, source, impact, summary, age_min, ts, keywords, tier, rel_score = r
            cat = row_cats[news_id]
            bucket = int(age_min // 5)
            key = (cat, bucket)
            if key in seen_cat_window:
                existing_id = seen_cat_window[key]
                existing_r = next(x for x in deduped if x[0] == existing_id)
                existing_quality = _get_source_quality(existing_r[2])
                new_quality = _get_source_quality(source)
                # Prefer higher source quality, then higher impact
                if new_quality > existing_quality or (new_quality == existing_quality and impact > existing_r[3]):
                    seen_cat_window[key] = news_id
                    deduped = [x for x in deduped if x[0] != existing_id]
                    deduped_ids.discard(existing_id)
                    deduped.append(r)
                    deduped_ids.add(news_id)
            else:
                seen_cat_window[key] = news_id
                deduped.append(r)
                deduped_ids.add(news_id)

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

        # 6-1: Separate macro vs crypto items using precomputed categories
        macro_items = []
        crypto_items = []
        for r in deduped:
            cat = row_cats[r[0]]
            if cat in CRYPTO_CATEGORIES:
                crypto_items.append(r)
            else:
                # MACRO_CATEGORIES + OTHER → macro (broader market impact)
                macro_items.append(r)

        # Score each group separately (with tier/relevance data)
        macro_mag, macro_dir, macro_comp, macro_stats = _score_group(
            macro_items, traces, watch_kw, row_tiers, row_rel_scores, source_accuracy)
        crypto_mag, crypto_dir, crypto_comp, crypto_stats = _score_group(
            crypto_items, traces, watch_kw, row_tiers, row_rel_scores, source_accuracy)

        # Log group details
        for r in deduped[:10]:
            news_id = r[0]
            title, source, impact, summary = r[1], r[2], r[3], r[4]
            cat = row_cats[news_id]
            tier = row_tiers.get(news_id, 'UNKNOWN')
            rel_s = row_rel_scores.get(news_id)
            group = 'macro' if cat not in CRYPTO_CATEGORIES else 'crypto'
            relevance = _classify_relevance(title, cat, impact, summary,
                                            tier=tier, relevance_score=rel_s)
            _log(f'relevance: "{(title or "")[:40]}" cat={cat} impact={impact} '
                 f'tier={tier} rel={rel_s} level={relevance} group={group}')

        # BTC-QQQ 30일 상관관계 기반 동적 가중치
        dyn_macro_w = MACRO_WEIGHT
        dyn_crypto_w = CRYPTO_WEIGHT
        btc_qqq_corr = None
        try:
            import regime_correlation
            corr_data = regime_correlation.get_correlation_info(cur)
            btc_qqq_corr = corr_data.get('correlation') if corr_data else None
            if btc_qqq_corr is not None:
                if btc_qqq_corr > 0.5:
                    dyn_macro_w = 0.75
                    dyn_crypto_w = 0.25
                elif btc_qqq_corr < 0.3:
                    dyn_macro_w = 0.4
                    dyn_crypto_w = 0.6
        except Exception:
            pass

        # Weighted blend: macro * dyn_w + crypto * dyn_w
        macro_score = macro_mag * macro_dir
        crypto_score = crypto_mag * crypto_dir

        if macro_items and crypto_items:
            blended_score = round(macro_score * dyn_macro_w + crypto_score * dyn_crypto_w)
        elif macro_items:
            blended_score = macro_score
        elif crypto_items:
            blended_score = crypto_score
        else:
            blended_score = 0

        # 6-3: macro_data corroboration
        direction_sign = 1 if blended_score >= 0 else -1
        macro_bonus = _macro_corroboration(cur, direction_sign)
        blended_score = max(-100, min(100, blended_score + macro_bonus))

        # Blended components (weighted average with dynamic weights)
        components = {}
        for key in ('source_quality', 'category_weight', 'recency', 'market_reaction', 'watchlist'):
            mc = macro_comp.get(key, 0)
            cc = crypto_comp.get(key, 0)
            if macro_items and crypto_items:
                components[key] = round(mc * dyn_macro_w + cc * dyn_crypto_w, 1)
            elif macro_items:
                components[key] = mc
            else:
                components[key] = cc

        magnitude = abs(blended_score)
        total_bull = macro_stats.get('bullish', 0) + crypto_stats.get('bullish', 0)
        total_bear = macro_stats.get('bearish', 0) + crypto_stats.get('bearish', 0)
        total_neutral = macro_stats.get('neutral', 0) + crypto_stats.get('neutral', 0)
        watch_matched = macro_stats.get('watch_matched', set()) | crypto_stats.get('watch_matched', set())

        # 6-4: Score trace with macro_data context
        dir_char = '+' if direction_sign > 0 else '-'
        macro_ctx = _get_macro_context(cur)
        ctx_parts = [f'{k}:{v:.1f}' for k, v in macro_ctx.items()]
        ctx_str = f' [{", ".join(ctx_parts)}]' if ctx_parts else ''

        score_trace = (f"macro:{macro_score:+d}*{dyn_macro_w:.2f} + "
                       f"crypto:{crypto_score:+d}*{dyn_crypto_w:.2f} "
                       f"= {blended_score - macro_bonus:+d}")
        if macro_bonus != 0:
            score_trace += f" QQQ보정:{macro_bonus:+d}"
        if btc_qqq_corr is not None:
            score_trace += f" corr:{btc_qqq_corr:.2f}"
        score_trace += f" -> {blended_score:+d} 방향:{dir_char}{ctx_str}"

        return {
            'score': blended_score,
            'is_supplementary': True,
            'components': components,
            'details': {
                'news_count': len(deduped),
                'deduped_from': len(rows),
                'macro_count': len(macro_items),
                'crypto_count': len(crypto_items),
                'macro_score': macro_score,
                'crypto_score': crypto_score,
                'macro_bonus': macro_bonus,
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
