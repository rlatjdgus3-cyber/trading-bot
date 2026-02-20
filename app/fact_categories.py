"""
fact_categories.py — Keyword-based news classification for FACT pipeline.

8 macro categories with keyword lists.
Pure-function library, no DB or LLM dependency.
"""
import re

CATEGORIES = {
    'war_geopolitics': [
        'war', 'missile', 'invasion', 'military', 'nato', 'ukraine',
        'russia', 'israel', 'iran', 'hamas', 'hezbollah', 'ceasefire',
        'sanction', 'conflict', 'attack',
        '전쟁', '미사일', '침공', '군사', '제재', '지정학',
        'geopolitical', 'tension', 'escalation',
    ],
    'us_politics': [
        'trump', 'biden', 'white house', 'congress', 'senate',
        'republican', 'democrat', 'election', 'impeach', 'tariff',
        'executive order',
        '대통령', '백악관', '의회', '관세', '행정명령',
    ],
    'fed_rates': [
        'fed ', 'fomc', 'powell', 'rate cut', 'rate hike',
        'interest rate', 'federal reserve', 'hawkish', 'dovish',
        'taper', 'quantitative',
        '금리', '연준', '파월', '기준금리', '금리인상', '금리인하',
        'cpi', 'inflation', 'ppi', 'nonfarm', 'payroll', 'unemployment',
        '인플레이션', '물가', '고용',
    ],
    'regulation': [
        ' sec ', 'regulation', 'lawsuit', 'compliance', 'etf',
        'spot etf', 'gensler', 'crypto regulation', 'defi regulation',
        '규제', '금지', '소송', '승인', '거래소 폐쇄',
        'stablecoin', 'cbdc', 'aml', 'kyc',
    ],
    'bigtech': [
        'apple', 'google', 'microsoft', 'amazon', 'nvidia',
        'meta ', 'tesla', 'artificial intelligence', 'earnings', 'layoff',
        '빅테크', '실적', '엔비디아', '테슬라', '반도체', 'semiconductor',
    ],
    'japan_boj': [
        'boj', 'bank of japan', 'yen ', 'jpy', 'ueda', 'kuroda',
        'yield curve',
        '일본은행', '엔화', '일본 금리',
    ],
    'china': [
        'china', 'pboc', 'xi jinping', 'yuan', 'cny', 'evergrande',
        '중국', '인민은행', '위안', '시진핑', '항셍', 'hang seng',
    ],
    'europe_ecb': [
        'ecb', 'lagarde', 'euro ', 'european central bank',
        'eu regulation',
        '유럽중앙은행', '유로', '라가르드',
    ],
    # P1: US politics / macro expansion
    'us_fiscal': [
        'debt ceiling', 'deficit', 'fiscal policy', 'fiscal cliff',
        'government shutdown', 'treasury auction', 'budget',
        'sequester', 'national debt', 'continuing resolution',
        '부채 한도', '재정 적자', '정부 셧다운', '예산', '국가 부채',
    ],
    'us_scandal_legal': [
        'indictment', 'impeach', 'special counsel', 'grand jury',
        'arraign', 'subpoena', 'criminal charges', 'plea deal',
        'felony', 'obstruction', 'ethics probe',
        '기소', '탄핵', '특별 검사', '대배심', '소환장',
    ],
    'wallstreet_signal': [
        'goldman sachs', 'jpmorgan', 'morgan stanley',
        'analyst upgrade', 'analyst downgrade', 'price target',
        'strategist', 'wall street forecast',
        'bank of america', 'citigroup', 'blackrock',
        '골드만', 'JP모건', '모건 스탠리', '목표 주가', '월가',
    ],
    'tech_sector': [
        'semiconductor', 'chip ban', 'chip export', 'chip war',
        'ai regulation', 'big tech', 'antitrust',
        'tsmc', 'asml', 'foundry', 'chip shortage', 'chip act',
        '반도체', '칩 수출', '칩 전쟁', 'AI 규제', '빅테크', '반독점',
    ],
    'immigration': [
        'immigration', 'border wall', 'border security', 'deportation',
        'asylum', 'visa ban', 'migrant', 'refugee',
        'ice raid', 'dhs', 'undocumented',
        '이민', '국경', '추방', '망명', '난민',
    ],
    'macro_rates': [
        'treasury yield', 'yield curve', 'credit spread', 'us10y',
        'term premium', 'bond auction', 'bond selloff', 'tlt',
        'real rate', 'real yield', 'duration risk', 'swap spread',
        '국채 수익률', '수익률 곡선', '신용 스프레드', '채권 입찰',
    ],
}

# Short keywords that need word-boundary regex to avoid false positives
_SHORT_WORDS = {
    'ai', 'aml', 'ban', 'boj', 'cny', 'cpi', 'ecb', 'etf',
    'eur', 'fed', 'jpy', 'kyc', 'ppi', 'sec', 'war', 'yen',
    'euro', 'meta',
}

_BOUNDARY_PATTERNS: dict[str, re.Pattern] = {}
for _kw in _SHORT_WORDS:
    _BOUNDARY_PATTERNS[_kw] = re.compile(r'\b' + re.escape(_kw) + r'\b', re.IGNORECASE)


def _kw_in_text(kw, text):
    """Check if keyword matches in text.

    Short words use word-boundary regex to avoid substring false positives.
    Multi-word phrases and Korean keywords use simple 'in' check.
    """
    kw_stripped = kw.strip()
    if kw_stripped in _BOUNDARY_PATTERNS:
        return bool(_BOUNDARY_PATTERNS[kw_stripped].search(text))
    return kw in text


def classify_news(title=None, summary=None):
    """Return the best-match macro category, or None if no match.

    Checks title first (higher weight), then summary.
    Returns the category with the most keyword hits.
    """
    if not title and not summary:
        return None

    title_lower = f" {title or ''} ".lower()
    summary_lower = f" {summary or ''} ".lower()

    best_cat = None
    best_score = 0

    for cat, keywords in CATEGORIES.items():
        score = 0
        for kw in keywords:
            if _kw_in_text(kw, title_lower):
                score += 2  # title matches weighted higher
            elif _kw_in_text(kw, summary_lower):
                score += 1
        if score > best_score:
            best_score = score
            best_cat = cat

    return best_cat


def extract_macro_keywords(text=None):
    """Return all matched keywords from any category."""
    if not text:
        return []
    text_lower = f" {text} ".lower()
    if not text_lower.strip():
        return []
    matched = []
    for keywords in CATEGORIES.values():
        for kw in keywords:
            kw_stripped = kw.strip()
            if _kw_in_text(kw, text_lower):
                if kw_stripped not in matched:
                    matched.append(kw_stripped)
    return matched
