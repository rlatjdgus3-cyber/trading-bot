"""
News Classification Config v2 — weighted source scoring + two-tier allow.

Key changes from v1:
  - Blacklist replaced by SOURCE_WEIGHTS (soft penalty, never hard deny)
  - Two-tier allow: allow_for_storage (broad) vs allow_for_trading (narrow)
  - Keyword-based topic estimation for unclassified items
  - RELEVANCE_BOOST for US macro / Nasdaq keywords
  - Deny only if: (source_weight < 0.6) AND (relevance < 0.55) AND (tier==TIER3)
"""
import re

APPROVAL_REQUIRED = False  # Activated: shadow classifier now supplements GPT

# ── News Trading Invariants ──────────────────────────────
# News NEVER blocks entry or exit. It only adjusts position sizing.
NEWS_CAN_BLOCK_ENTRY = False  # INVARIANT — never set to True
NEWS_CAN_BLOCK_EXIT = False   # INVARIANT — never set to True
NEWS_MAX_SIZE_SCALE = 1.2     # max position size adjustment from news
NEWS_MIN_SIZE_SCALE = 0.8     # min position size adjustment from news

SOURCE_WHITELIST = [
    'bloomberg', 'reuters', 'wsj', 'ft',
    'fed.gov', 'sec.gov', 'us_treasury',
    'ecb', 'boj', 'bis',
]

SOURCE_TIER2 = [
    'coindesk', 'cointelegraph', 'theblock', 'decrypt',
    'cnbc', 'marketwatch', 'barrons',
]

SOURCE_BLACKLIST_PATTERNS = [
    # DEPRECATED — kept for backward compat reference only.
    r'(?i)^benzinga$',
    r'(?i)^seekingalpha$',
]

# ── Weighted source scoring (v2) ─────────────────────────
SOURCE_WEIGHTS = {
    # TIER1 — highest trust
    'bloomberg': 1.00, 'reuters': 1.00, 'wsj': 0.95, 'ft': 0.95,
    'fed.gov': 1.00, 'sec.gov': 1.00, 'us_treasury': 1.00,
    'ecb': 1.00, 'boj': 1.00, 'bis': 1.00, 'ap': 0.95,
    # Broadcast / editorial
    'bbc_business': 0.85, 'bbc_world': 0.85, 'bbc': 0.85,
    'cnbc': 0.80,
    # TIER2 — crypto-specific / financial media
    'coindesk': 0.75, 'cointelegraph': 0.65, 'theblock': 0.70,
    'decrypt': 0.65, 'marketwatch': 0.70, 'barrons': 0.75,
    # Reference — usable, lower weight (NOT blocked)
    'investing': 0.70, 'investing.com': 0.70,
    'yahoo_finance': 0.55, 'yahoo': 0.55,
    # Low-quality — soft penalty, NOT hard deny
    'benzinga': 0.30, 'seekingalpha': 0.30,
}
DEFAULT_SOURCE_WEIGHT = 0.40
# Deny threshold: source_weight < this (combined with low_relevance + low_tier)
DENY_SOURCE_WEIGHT_THRESHOLD = 0.60
# Trading threshold: source_weight >= this for allow_for_trading
TRADING_SOURCE_WEIGHT_THRESHOLD = 0.65

# ── In-scope topics (deny하지 않을 전략 범주) ─────────────
IN_SCOPE_TOPICS = frozenset({
    'FED_FOMC', 'MACRO_INDICATORS', 'EQUITY_MOVES', 'ETF_FLOWS',
    'SEC_REGULATION', 'INSTITUTIONAL_BTC', 'GEOPOLITICAL',
    'CRYPTO_GENERAL',
    # P1: US politics / macro expansion
    'US_POLITICS_ELECTION', 'US_FISCAL_DEBT', 'US_SCANDAL_LEGAL',
    'WALLSTREET_SIGNAL', 'IMMIGRATION_POLICY', 'TECH_NASDAQ', 'MACRO_RATES',
})

# ── Strategy category keyword patterns (A~G) ────────────
STRATEGY_CATEGORIES_V2 = {
    'FED_FOMC': re.compile(
        r'(?i)\b(fed(eral reserve)?|fomc|powell|rate (cut|hike|decision)|'
        r'monetary policy|fed fund|jackson hole|dot plot)\b'),
    'MACRO_INDICATORS': re.compile(
        r'(?i)\b(cpi|ppi|nfp|non.?farm|unemployment|gdp|pce|'
        r'retail sales|jobless claims|ism|consumer (confidence|sentiment)|'
        r'pmi|jobs report|payroll)\b'),
    'ETF_FLOWS': re.compile(
        r'(?i)\b(etf (inflow|outflow|flow|approval)|spot etf|'
        r'gbtc|ibit|fbtc|bitb|arkb|btco)\b'),
    'SEC_REGULATION': re.compile(
        r'(?i)\b(sec |regulation|regulatory|enforcement|lawsuit|'
        r'compliance|stablecoin bill|crypto ban|cbdc)\b'),
    'EQUITY_MOVES': re.compile(
        r'(?i)\b(nasdaq|qqq|s&p.?500|spy|spx|dow jones|megacap|'
        r'nvidia|nvda|magnificent.?seven|'
        r'stock (crash|surge|rally|selloff)|market (crash|correction|rally))\b'),
    'INSTITUTIONAL_BTC': re.compile(
        r'(?i)\b(microstrategy|saylor|tesla|galaxy digital|'
        r'institutional (buy|sell|accumulation)|whale|btc (purchase|sale))\b'),
    'GEOPOLITICAL': re.compile(
        r'(?i)(?<!\bstreaming )(?<!\bprice )(?<!\btalent )(?<!\bbidding )'
        r'(?<!\bbrowser )(?<!\bformat )(?<!\bconsole )'
        r'\b(war|sanction|tariff|embargo|missile|invasion|'
        r'geopolitic|trump|biden|china.?us|russia|iran|nato|'
        r'white house|congress)\b'),
    # P1: 7 new categories
    'US_POLITICS_ELECTION': re.compile(
        r'(?i)\b(election|midterm|caucus|ballot|polling|executive order|'
        r'presidential|gubernatorial|swing state|electoral college|'
        r'voter|campaign|nominee|primary|inauguration)\b'),
    'US_FISCAL_DEBT': re.compile(
        r'(?i)\b(debt ceiling|deficit|fiscal (policy|cliff)|government shutdown|'
        r'treasury auction|budget (deal|bill|crisis)|sequester|'
        r'federal (spending|budget)|continuing resolution|appropriation|'
        r'national debt|fiscal stimulus)\b'),
    'US_SCANDAL_LEGAL': re.compile(
        r'(?i)\b(indictment|impeach|special counsel|grand jury|arraign|'
        r'subpoena|criminal charges|plea (deal|guilty)|felony|'
        r'obstruction|contempt of congress|ethics (probe|violation)|'
        r'classified documents|hush money)\b'),
    'WALLSTREET_SIGNAL': re.compile(
        r'(?i)\b(goldman sachs|jpmorgan|morgan stanley|analyst (upgrade|downgrade)|'
        r'price target|strategist|wall street (forecast|outlook|consensus)|'
        r'bank of america|citigroup|wells fargo|blackrock|'
        r'institutional (forecast|call)|equity research)\b'),
    'IMMIGRATION_POLICY': re.compile(
        r'(?i)\b(immigration|border (wall|security|crisis)|deportation|'
        r'asylum|visa (ban|policy)|migrant|refugee|'
        r'ice raid|dhs|cbp|undocumented|h-1b|green card)\b'),
    'TECH_NASDAQ': re.compile(
        r'(?i)\b(tech stocks|semiconductor|chip (ban|export|shortage|act)|'
        r'ai regulation|big tech|antitrust|'
        r'artificial intelligence|ai (boom|bubble|arms race)|'
        r'chip war|fab|foundry|tsmc|asml)\b'),
    'MACRO_RATES': re.compile(
        r'(?i)\b(treasury yield|yield curve|credit spread|us10y|us2y|'
        r'term premium|bond (auction|selloff|sell-off|rally)|tlt|'
        r'real (rate|yield)|duration risk|swap spread|'
        r'sovereign debt|bills? auction|coupon auction)\b'),
}

# ── Strategy category Korean patterns (A~G) ──────────────
STRATEGY_CATEGORIES_KO = {
    'FED_FOMC': re.compile(
        r'연준|금리\s*(인상|인하|동결|결정)|FOMC|파월|통화\s*정책|기준금리'),
    'MACRO_INDICATORS': re.compile(
        r'소비자\s*물가|고용\s*(지표|보고서|률)|실업|GDP|CPI|PPI|'
        r'비농업|소매\s*판매|경기|PMI'),
    'ETF_FLOWS': re.compile(
        r'ETF\s*(유입|유출|승인|흐름)|현물\s*ETF|비트코인\s*ETF'),
    'SEC_REGULATION': re.compile(
        r'SEC|규제|증권\s*거래\s*위원회|단속|스테이블코인|CBDC|'
        r'암호화폐\s*(금지|규제|법안)'),
    'EQUITY_MOVES': re.compile(
        r'나스닥|S&P|다우|주가\s*(폭락|급등|급락|랠리)|증시\s*(폭락|급등|급락)|'
        r'주식\s*시장\s*(폭락|급등)'),
    'INSTITUTIONAL_BTC': re.compile(
        r'기관\s*(매수|매도|축적)|고래|대량\s*(매수|매도)|마이크로스트래티지|'
        r'비트코인\s*(매입|매각)'),
    'GEOPOLITICAL': re.compile(
        r'(?<!스트리밍 )(?<!가격 )전쟁|제재|관세|금수|미사일|침공|지정학|트럼프|바이든|'
        r'미중|러시아|이란|NATO|나토'),
    # P1: 7 new categories (Korean)
    'US_POLITICS_ELECTION': re.compile(
        r'선거|중간선거|대선|대통령\s*선거|행정명령|예비선거|투표|후보|취임'),
    'US_FISCAL_DEBT': re.compile(
        r'부채\s*한도|재정\s*적자|재정\s*정책|정부\s*셧다운|국채\s*입찰|'
        r'예산\s*(안|위기)|국가\s*부채|재정\s*절벽'),
    'US_SCANDAL_LEGAL': re.compile(
        r'기소|탄핵|특별\s*검사|대배심|소환장|형사\s*고발|유죄|윤리\s*조사'),
    'WALLSTREET_SIGNAL': re.compile(
        r'골드만\s*삭스|JP모건|모건\s*스탠리|애널리스트|목표\s*주가|'
        r'투자\s*의견|월가\s*(전망|컨센서스)|블랙록'),
    'IMMIGRATION_POLICY': re.compile(
        r'이민\s*(정책|개혁)|국경\s*(장벽|위기)|추방|망명|'
        r'비자\s*(금지|정책)|난민|이주민|불법\s*체류'),
    'TECH_NASDAQ': re.compile(
        r'기술주|반도체|칩\s*(수출|규제|부족)|AI\s*규제|빅테크|반독점|'
        r'인공지능|AI\s*(붐|버블)|칩\s*전쟁|TSMC|ASML'),
    'MACRO_RATES': re.compile(
        r'국채\s*수익률|수익률\s*곡선|신용\s*스프레드|'
        r'기간\s*프리미엄|채권\s*(입찰|매도|매각)|TLT|실질\s*금리|듀레이션'),
}

# ── Keyword-based topic estimation (for unclassified) ────
# Simple lowercase keyword → topic mapping (checked when regex fails)
TOPIC_KEYWORD_MAP = {
    'FED_FOMC': {'fed', 'fomc', 'powell', 'rate hike', 'rate cut',
                 'monetary policy', 'fed funds', 'federal reserve',
                 'interest rate', 'central bank', 'dot plot', 'jackson hole',
                 'quantitative', 'tightening', 'easing', 'dovish', 'hawkish'},
    'MACRO_INDICATORS': {'cpi', 'ppi', 'pce', 'nfp', 'jobs', 'unemployment',
                         'gdp', 'retail sales', 'jobless', 'payroll',
                         'ism', 'pmi', 'consumer confidence', 'inflation',
                         'housing', 'trade deficit', 'trade balance',
                         'economic growth', 'recession', 'soft landing',
                         'labor market', 'wage growth', 'core inflation',
                         'yield', 'treasury', 'bond', '10-year', '2-year',
                         'durable goods', 'industrial production'},
    'EQUITY_MOVES': {'nasdaq', 'qqq', 'spy', 'spx', 's&p 500', 's&p500',
                     'megacap', 'nvidia', 'nvda', 'dow jones', 'stock market',
                     'wall street', 'magnificent seven', 'tech stocks',
                     'earnings', 'profit', 'revenue', 'market cap',
                     'bull market', 'bear market', 'correction',
                     'rally', 'selloff', 'sell-off', 'crash',
                     'apple', 'aapl', 'tesla', 'tsla', 'microsoft', 'msft',
                     'meta', 'amazon', 'amzn', 'google', 'alphabet',
                     'semiconductor', 'ai stocks', 'risk-on', 'risk-off'},
    'GEOPOLITICAL': {'tariff', 'sanction', 'election', 'trump', 'white house',
                     'war', 'missile', 'nato', 'invasion', 'embargo',
                     'biden', 'congress', 'gop', 'democrat', 'republican',
                     'geopolitical', 'iran', 'russia', 'ukraine', 'china',
                     'taiwan', 'middle east', 'oil embargo', 'trade war',
                     'executive order', 'legislation', 'debt ceiling',
                     'government shutdown', 'fiscal policy'},
    'ETF_FLOWS': {'etf', 'gbtc', 'ibit', 'spot etf', 'bitcoin etf',
                  'etf inflow', 'etf outflow', 'fbtc', 'arkb', 'bitb',
                  'etf approval', 'fund flow'},
    'SEC_REGULATION': {'sec', 'regulation', 'regulatory', 'crypto ban', 'cbdc',
                       'compliance', 'enforcement', 'lawsuit', 'stablecoin',
                       'defi regulation', 'gensler', 'crypto regulation',
                       'securities', 'exchange commission'},
    'INSTITUTIONAL_BTC': {'microstrategy', 'saylor', 'whale', 'institutional',
                          'bitcoin purchase', 'btc purchase', 'galaxy digital',
                          'grayscale', 'fidelity crypto', 'blackrock bitcoin',
                          'corporate treasury', 'bitcoin adoption'},
    # P1: 7 new categories
    'US_POLITICS_ELECTION': {'election', 'presidential', 'midterm', 'caucus',
                             'ballot', 'polling', 'executive order', 'electoral',
                             'swing state', 'campaign', 'nominee', 'primary',
                             'inauguration', 'gubernatorial'},
    'US_FISCAL_DEBT': {'debt ceiling', 'deficit', 'fiscal policy', 'fiscal cliff',
                       'government shutdown', 'treasury auction', 'budget',
                       'sequester', 'federal spending', 'national debt',
                       'continuing resolution', 'appropriation', 'fiscal stimulus'},
    'US_SCANDAL_LEGAL': {'indictment', 'impeach', 'impeachment', 'special counsel',
                         'grand jury', 'arraign', 'subpoena', 'criminal charges',
                         'plea deal', 'felony', 'obstruction', 'contempt',
                         'ethics probe', 'classified documents', 'hush money'},
    'WALLSTREET_SIGNAL': {'goldman sachs', 'jpmorgan', 'morgan stanley',
                          'analyst upgrade', 'analyst downgrade', 'price target',
                          'strategist', 'wall street forecast', 'bank of america',
                          'citigroup', 'wells fargo', 'blackrock',
                          'equity research'},
    'IMMIGRATION_POLICY': {'immigration', 'border wall', 'border security',
                           'deportation', 'asylum', 'visa ban', 'migrant',
                           'refugee', 'ice raid', 'dhs', 'undocumented',
                           'h-1b', 'green card', 'border crisis'},
    'TECH_NASDAQ': {'tech stocks', 'semiconductor', 'chip ban', 'chip export',
                    'ai regulation', 'big tech', 'antitrust',
                    'artificial intelligence', 'ai boom', 'chip war',
                    'tsmc', 'asml', 'foundry', 'chip shortage', 'chip act'},
    'MACRO_RATES': {'treasury yield', 'yield curve', 'credit spread', 'us10y',
                    'us2y', 'term premium', 'bond auction', 'bond selloff',
                    'tlt', 'real rate', 'real yield', 'duration risk',
                    'swap spread', 'sovereign debt', 'coupon auction'},
}

# ── Relevance boost keywords (US macro / Nasdaq) ────────
RELEVANCE_BOOST_KEYWORDS = {
    # FED / rates
    'fed', 'fomc', 'powell', 'rate hike', 'rate cut',
    'interest rate', 'hawkish', 'dovish',
    # Macro indicators
    'cpi', 'ppi', 'pce', 'nfp', 'jobs', 'unemployment',
    'inflation', 'gdp', 'recession', 'labor market',
    # Treasury / dollar / liquidity
    'treasury', 'yield', 'yields', '2y', '10y', 'us10y', 'dxy', 'liquidity',
    'yield curve', 'credit spread', 'bond',
    # Equities / Nasdaq
    'nasdaq', 'qqq', 'spy', 'spx', 's&p', 'megacap', 'nvidia',
    'earnings', 'tech stocks', 'magnificent seven', 'wall street',
    'bull market', 'bear market', 'risk-on', 'risk-off',
    # Politics / geopolitical
    'tariff', 'sanction', 'election', 'trump', 'white house',
    'debt ceiling', 'government shutdown', 'geopolitical',
    # P1: US politics / macro expansion
    'presidential', 'midterm', 'executive order',
    'indictment', 'impeachment', 'special counsel',
    'goldman sachs', 'jpmorgan', 'morgan stanley',
    'semiconductor', 'chip ban', 'ai regulation', 'big tech',
    'bond auction', 'credit spread', 'tlt', 'term premium',
    'immigration', 'deportation', 'border',
}
RELEVANCE_BOOST_AMOUNT = 0.15

# ── Personal story / gossip exclusion patterns ───────────
PERSONAL_STORY_EXCLUDE_PATTERNS = [
    re.compile(r'(?i)\bgossip\b'),
    re.compile(r'(?i)\bpersonal (story|finance|opinion)\b'),
    re.compile(r'(?i)\bstock.?pick(ing|s)?\b'),
    re.compile(r'(?i)\brumor\b'),
    re.compile(r'(?i)\b셀럽\b'),
    re.compile(r'(?i)\b가십\b'),
    re.compile(r'(?i)\b루머\b'),
    re.compile(r'(?i)\b(best|top|worst)\s+\d+\s+(stock|crypto|coin)s?\b'),
    re.compile(r'(?i)\b(should you buy|is it time to|could make you)\b'),
    re.compile(r'(?i)\b(dividend|passive income|retire|portfolio)\b.*\b(stock|etf)\b'),
    re.compile(r'(?i)\b(meme stock|penny stock|small.?cap)\b'),
]

TIER_DEFINITIONS_V2 = {
    'TIER1': {
        'description': 'High-confidence: source_whitelist + strategy_categories match',
        'sources': SOURCE_WHITELIST,
        'weight': 1.0,
    },
    'TIER2': {
        'description': 'Medium-confidence: tier2 sources + crypto-specific news',
        'sources': SOURCE_TIER2,
        'weight': 0.5,
    },
    'TIER3': {
        'description': 'Low-confidence: everything else (weight <= 0.1)',
        'sources': [],
        'weight': 0.1,
    },
}


# ── Core functions ────────────────────────────────────────

_SOURCE_NORMALIZE = {
    'investing.com': 'investing',
    'yahoo finance': 'yahoo_finance',
    'yahoo!': 'yahoo_finance',
    'bbc business': 'bbc_business',
    'bbc world': 'bbc_world',
    'wall street journal': 'wsj',
    'financial times': 'ft',
    'the block': 'theblock',
    'seeking alpha': 'seekingalpha',
}


def get_source_weight(source: str) -> float:
    """Return source weight from SOURCE_WEIGHTS. Unknown -> DEFAULT_SOURCE_WEIGHT.
    Normalizes common source name variants before lookup.
    """
    if not source:
        return DEFAULT_SOURCE_WEIGHT
    s = source.lower().strip()
    # Normalize common variants
    if s in _SOURCE_NORMALIZE:
        s = _SOURCE_NORMALIZE[s]
    # Exact match first
    if s in SOURCE_WEIGHTS:
        return SOURCE_WEIGHTS[s]
    # Substring match
    for key, weight in SOURCE_WEIGHTS.items():
        if key in s or s in key:
            return weight
    return DEFAULT_SOURCE_WEIGHT


def _is_blacklisted_source(source: str) -> bool:
    """Soft check: True if source weight < DENY threshold.
    Note: This alone does NOT deny — needs triple-condition check."""
    return get_source_weight(source) < DENY_SOURCE_WEIGHT_THRESHOLD


def _is_personal_story(title: str, summary: str = '') -> bool:
    text = (title or '') + ' ' + (summary or '')
    for pat in PERSONAL_STORY_EXCLUDE_PATTERNS:
        if pat.search(text):
            return True
    return False


def _detect_topic_class(title: str, summary: str = '',
                        title_ko: str = '') -> str:
    """Detect strategy category from title/summary text.
    Tries EN patterns on title+summary, then KO patterns on title_ko."""
    # EN patterns first
    text_en = (title or '') + ' ' + (summary or '')
    for cat_name, pattern in STRATEGY_CATEGORIES_V2.items():
        if pattern.search(text_en):
            return cat_name
    # KO patterns on title_ko
    if title_ko:
        for cat_name, pattern in STRATEGY_CATEGORIES_KO.items():
            if pattern.search(title_ko):
                return cat_name
    return ''


def _estimate_topic_from_keywords(title: str, summary: str = '') -> str:
    """Keyword-based topic estimation for unclassified items.
    Fallback when regex-based _detect_topic_class returns empty."""
    text = ((title or '') + ' ' + (summary or '')).lower()
    best_topic = ''
    best_hits = 0
    for topic, keywords in TOPIC_KEYWORD_MAP.items():
        hits = sum(1 for kw in keywords if kw in text)
        if hits > best_hits:
            best_hits = hits
            best_topic = topic
    return best_topic if best_hits >= 1 else ''


def _compute_source_quality(source: str) -> float:
    """Source quality score 0.0~1.0 based on SOURCE_WEIGHTS."""
    return get_source_weight(source)


def _has_relevance_boost(title: str, summary: str = '') -> bool:
    """Check if title/summary contains macro/Nasdaq boost keywords."""
    text = ((title or '') + ' ' + (summary or '')).lower()
    return any(kw in text for kw in RELEVANCE_BOOST_KEYWORDS)


def _compute_relevance_preview(title: str, source: str, impact_score,
                                summary: str = '', title_ko: str = '',
                                topic: str = '') -> float:
    """Relevance score 0.0~1.0 for preview."""
    score = 0.0
    # Source quality contributes 30%
    score += _compute_source_quality(source) * 0.30
    # Topic match contributes 35%
    if topic:
        score += 0.35
    # Impact score contributes 15%
    imp = float(impact_score or 0)
    score += min(imp / 10.0, 1.0) * 0.15
    # Keyword boost contributes up to 20%
    if _has_relevance_boost(title, summary):
        score += RELEVANCE_BOOST_AMOUNT
    return round(min(1.0, score), 2)


def preview_classify(title: str, source: str, impact_score=0,
                     summary: str = '', title_ko: str = '') -> dict:
    """
    Classify a news item. Returns dict with:
      - tier_preview, topic_class_preview, relevance_score_preview
      - source_quality_preview, source_weight
      - allow_for_storage, allow_for_trading (two-tier allow)
      - deny_reasons
    """
    display_title = title_ko or title or ''
    personal = _is_personal_story(display_title, summary)

    # Topic detection — regex first, then keyword fallback
    topic = _detect_topic_class(title, summary, title_ko=title_ko)
    if not topic:
        topic = _estimate_topic_from_keywords(title, summary)

    # Source quality & weight
    src_weight = get_source_weight(source)
    src_quality = src_weight  # same value

    # Tier assignment
    if personal:
        tier = 'TIER3'
    elif src_quality >= 0.90 and topic:
        tier = 'TIER1'
    elif src_quality >= 0.60 or topic:
        tier = 'TIER2'
    else:
        tier = 'TIER3'

    # Relevance (pass topic for scoring)
    relevance = _compute_relevance_preview(title, source, impact_score,
                                            summary=summary, title_ko=title_ko,
                                            topic=topic)

    # ── Deny logic: (src_weight < 0.6) AND (relevance < 0.55) AND (tier==TIER3)
    deny_reasons = []
    if personal:
        deny_reasons.append('personal_story')

    low_src = src_weight < DENY_SOURCE_WEIGHT_THRESHOLD   # < 0.6
    low_rel = relevance < 0.55
    low_tier = tier == 'TIER3'
    if low_src and low_rel and low_tier and not personal:
        deny_reasons.append(f'triple_low(w={src_weight:.2f},r={relevance:.2f})')

    # ── Two-tier allow ──
    topic_in_scope = topic in IN_SCOPE_TOPICS

    # allow_for_storage: broad — data accumulation
    allow_storage = (
        not personal
        and not deny_reasons
        and (relevance >= 0.35 or topic_in_scope or src_weight >= 0.85)
    )

    # allow_for_trading: narrow — actual trade signals
    allow_trading = (
        not personal
        and not deny_reasons
        and tier in ('TIER1', 'TIER2')
        and topic_in_scope
        and relevance >= 0.50
        and src_weight >= TRADING_SOURCE_WEIGHT_THRESHOLD  # >= 0.65
    )

    return {
        'tier_preview': tier,
        'topic_class_preview': topic or 'unclassified',
        'relevance_score_preview': relevance,
        'source_quality_preview': src_quality,
        'source_weight': src_weight,
        'allow_for_storage': allow_storage,
        'allow_for_trading': allow_trading,
        # Legacy compat
        'allow_decision_preview': allow_storage,
        'deny_reasons': deny_reasons,
        'APPLIED': not APPROVAL_REQUIRED,
    }


# ── Scandal Confirmation Gate (P2) ──────────────────────────
SCANDAL_CONFIRMATION_CONFIG = {
    'min_source_quality': 0.85,
    'allowed_tiers': ('TIER1', 'TIER2'),
    'confirmation_window_min': 120,
    'min_independent_sources': 2,
    'rumor_keywords': re.compile(
        r'(?i)\b(claim|rumor|alleged|reportedly|unconfirmed|sources say|speculation)\b'),
    'rumor_impact_cap': 4,
}


def scandal_confirmation_check(topic, title, source, impact_score,
                                db_cursor=None) -> dict:
    """Check if a scandal/legal news item is sufficiently confirmed.

    Only applies to US_SCANDAL_LEGAL topic.
    Returns:
        {confirmed, reason, impact_cap, allow_storage(always True), allow_trading}
    """
    cfg = SCANDAL_CONFIRMATION_CONFIG
    result = {
        'confirmed': True,
        'reason': '',
        'impact_cap': None,
        'allow_storage': True,
        'allow_trading': True,
    }

    # Only gate US_SCANDAL_LEGAL
    if topic != 'US_SCANDAL_LEGAL':
        return result

    # Check rumor keywords → cap impact
    title_text = title or ''
    if cfg['rumor_keywords'].search(title_text):
        result['impact_cap'] = cfg['rumor_impact_cap']
        result['reason'] = 'rumor_keyword_detected'

    # Check source quality
    src_quality = get_source_weight(source)
    if src_quality < cfg['min_source_quality']:
        result['confirmed'] = False
        result['allow_trading'] = False
        result['reason'] = f'source_quality_low({src_quality:.2f}<{cfg["min_source_quality"]})'
        return result

    # Check independent source corroboration from DB
    if db_cursor:
        try:
            db_cursor.execute("""
                SELECT COUNT(DISTINCT source)
                FROM news
                WHERE topic_class = 'US_SCANDAL_LEGAL'
                  AND ts >= now() - interval '%s minutes'
                  AND source != %%s
                  AND exclusion_reason IS NULL
            """ % cfg['confirmation_window_min'], (source,))
            row = db_cursor.fetchone()
            independent_count = (row[0] if row and row[0] else 0)
            if independent_count < cfg['min_independent_sources']:
                result['confirmed'] = False
                result['allow_trading'] = False
                result['reason'] = (
                    f'insufficient_sources({independent_count}'
                    f'<{cfg["min_independent_sources"]})')
                return result
        except Exception:
            # DB check failure → unconfirmed (conservative)
            result['confirmed'] = False
            result['allow_trading'] = False
            result['reason'] = 'db_check_failed'
            return result

    return result
