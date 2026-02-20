import os, time, json, traceback, sys, re
import feedparser
import psycopg2
from openai import OpenAI
from db_config import get_conn
import news_classifier_config as _ncc
NEWS_POLL_SEC = int(os.getenv("NEWS_POLL_SEC", "300"))
FEED_AGENT = os.getenv("NEWS_FEED_AGENT", "Mozilla/5.0 trading-bot-news/1.0")

# Circuit breaker constants
MAX_CONSECUTIVE_DB_ERRORS = 10
BACKOFF_BASE_SEC = 5
MAX_BACKOFF_SEC = 120

FEEDS = [
    # 암호화폐
    ("coindesk", "https://www.coindesk.com/arc/outboundfeeds/rss/"),
    ("cointelegraph", "https://cointelegraph.com/rss"),
    # 미국/거시/비즈니스
    ("bloomberg", "https://feeds.bloomberg.com/markets/news.rss"),
    ("bbc_business", "http://feeds.bbci.co.uk/news/business/rss.xml"),
    ("bbc_world", "http://feeds.bbci.co.uk/news/world/rss.xml"),
    # 미국 주식/경제 (신규)
    ("cnbc", "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10001147"),
    ("yahoo_finance", "https://finance.yahoo.com/news/rssconf"),
    ("investing", "https://www.investing.com/rss/news.rss"),
    ("marketwatch", "https://feeds.content.dowjones.io/public/rss/mw_topstories"),
]

KEYWORDS = [
    # crypto
    "bitcoin","btc","crypto","etf","sec",
    # fed/rates
    "fed","fomc","cpi","inflation","rate","powell","nfp","pce","unemployment","jobs report",
    # treasury/dollar
    "treasury","bond","dollar","yields","us10y","dxy",
    # equities
    "nasdaq","qqq","spx","sp500","equit","risk-off","risk off",
    # politics
    "election","trump","tariff","white house","congress","gop",
    # geopolitics
    "war","missile","sanction","china","boj",
    # crypto-specific
    "hack","exploit","liquidation","bank","regulation",
    # P1: US politics / macro expansion
    "presidential","midterm","executive order","debt ceiling","government shutdown",
    "indictment","impeachment","special counsel",
    "goldman sachs","jpmorgan","morgan stanley",
    "semiconductor","chip ban","ai regulation",
    "immigration","deportation","border",
    "treasury yield","credit spread","bond auction","term premium","tlt",
]

# ── 가십/노이즈 하드 필터 ──
GOSSIP_BLOCKLIST = {
    'celebrity', 'divorce', 'lifestyle', 'entertainment', 'sports', 'fashion',
    'recipe', 'dating', 'top 10', '5 things', 'quiz', 'roundup', 'picks',
    'best movies', 'tv show', 'oscars', 'grammy', 'wedding', 'vacation',
    'horoscope', 'zodiac', 'beauty', 'fitness', 'weight loss', 'diet',
    'real estate tips', 'home decor', 'travel guide', 'pet', 'puppy',
    'tiktok', 'instagram', 'influencer', 'viral video', 'meme',
}

LOW_VALUE_SOURCES = {'yahoo_finance'}  # insert에 impact_score >= 6 조건 적용


# ── Word-boundary keyword matching ──
# Short keywords (≤4 chars) that need \b to avoid substring false positives
_SHORT_KW_THRESHOLD = 4

def _build_kw_regex(keywords):
    """Build a single compiled regex from a keyword set/list for word-boundary matching.
    ASCII keywords use \\b word boundaries to prevent substring false positives.
    Korean/CJK keywords use simple alternation (\\b doesn't work with CJK).
    """
    parts = []
    for kw in sorted(keywords, key=len, reverse=True):  # longer first for greedy match
        if kw.isascii():
            parts.append(r'\b' + re.escape(kw) + r'\b')
        else:
            parts.append(re.escape(kw))
    return re.compile('|'.join(parts), re.IGNORECASE) if parts else re.compile(r'(?!)')

def _kw_match(text, compiled_re):
    """Return True if any keyword matches in text."""
    return bool(compiled_re.search(text))

# ── 소스 티어 분류 (v2: bbc/investing → TIER2로 승격) ──
SOURCE_TIERS = {
    'TIER1_SOURCE': {'reuters', 'bloomberg', 'wsj', 'ft', 'ap'},
    'TIER2_SOURCE': {'cnbc', 'coindesk', 'marketwatch', 'cointelegraph',
                     'bbc_business', 'bbc_world', 'investing'},
    'REFERENCE_ONLY': {'yahoo_finance'},
}

CRYPTO_FEEDS = {'coindesk', 'cointelegraph'}

# ── Shadow → GPT category normalization ──
# Shadow classifier enum names differ from GPT enum; normalize to GPT names
# so topic_class column consistently uses the GPT enum (which the scorer reads).
_SHADOW_TO_GPT_CATEGORY = {
    'FED_FOMC': 'FED_RATES',
    'MACRO_INDICATORS': 'CPI_JOBS',
    'ETF_FLOWS': 'CRYPTO_SPECIFIC',
    'SEC_REGULATION': 'REGULATION_SEC_ETF',
    'EQUITY_MOVES': 'NASDAQ_EQUITIES',
    'INSTITUTIONAL_BTC': 'CRYPTO_SPECIFIC',
    'GEOPOLITICAL': 'WAR',
    'US_FISCAL_DEBT': 'US_FISCAL',
}


def _get_source_tier(source: str) -> str:
    """소스명 → 티어 반환. 미등록 → REFERENCE_ONLY."""
    s = (source or '').lower().strip()
    for tier, sources in SOURCE_TIERS.items():
        if s in sources:
            return tier
    return 'REFERENCE_ONLY'


# ── 하드 제외 패턴 (정규식) ──
HARD_EXCLUDE_PATTERNS = [
    r'is\s+\w+\s+(stock\s+)?a\s+good\s+buy',
    r'\d+\s+(best|top)\s+(stocks?|picks?|buys?)',
    r'(should\s+you|is\s+it\s+time\s+to)\s+(buy|sell|invest)',
    r'stock(s)?\s+(to|you\s+should)\s+(buy|sell|watch)',
    r'(my|his|her|their)\s+(husband|wife|journey|story|experience|portfolio)',
    r'how\s+i\s+(made|lost|earned|invest)',
    r'(personal\s+finance|retirement\s+plan|mortgage|student\s+loan)',
    r'^\d+\s+(things|ways|tips|reasons|steps)',
]
_HARD_EXCLUDE_RE = [re.compile(p, re.IGNORECASE) for p in HARD_EXCLUDE_PATTERNS]


def _is_hard_excluded(title: str) -> bool:
    """하드 제외 패턴 매치 시 True."""
    t = (title or '').strip()
    return any(pat.search(t) for pat in _HARD_EXCLUDE_RE)


# ── AND-기반 키워드 매칭 ──
CRYPTO_CORE_KEYWORDS = {
    'bitcoin', 'btc', 'crypto', 'ethereum', 'blockchain',
    'defi', 'stablecoin', 'halving', 'mining',
}
IMPACT_KEYWORDS = {
    'fed', 'fomc', 'cpi', 'inflation', 'rate', 'powell', 'nfp', 'pce',
    'treasury', 'bond', 'yields', 'dxy',
    'nasdaq', 'qqq', 'sp500', 'risk-off', 'risk off', 'risk-on', 'risk on',
    'war', 'missile', 'sanction', 'tariff',
    'sec', 'etf', 'regulation', 'ban',
    'liquidation', 'hack', 'exploit',
    # macro indicators
    'pmi', 'ism', 'gdp', 'retail sales', 'jobless',
    'credit spread', 'yield curve', 'us10y', 'earnings',
    # central banks
    'boj', 'ecb', 'rate decision', 'rate cut', 'rate hike',
    # P1: US politics / macro expansion
    'election', 'presidential', 'impeachment', 'indictment',
    'debt ceiling', 'government shutdown',
    'immigration', 'deportation',
    'semiconductor', 'credit spread', 'bond auction', 'term premium',
}
MACRO_STANDALONE_KEYWORDS = {
    # existing
    'fed', 'fomc', 'cpi', 'nfp', 'ppi', 'pce', 'powell',
    'war', 'missile', 'tariff', 'sanction',
    # macro indicators
    'dxy', 'pmi', 'ism', 'gdp', 'retail sales', 'jobless claims',
    'consumer confidence', 'consumer sentiment',
    # treasury/credit
    'treasury yields', 'yield curve', 'credit spread', 'us10y',
    # central banks
    'boj', 'ecb', 'rate decision', 'rate cut', 'rate hike',
    # geopolitical
    'nato', 'invasion', 'embargo',
    # mega-cap earnings (high BTC correlation)
    'earnings',
    # P1: US politics / macro expansion — pass worth_llm() without crypto keywords
    'election', 'presidential', 'midterm', 'executive order',
    'debt ceiling', 'government shutdown', 'shutdown', 'fiscal cliff',
    'indictment', 'impeachment', 'special counsel',
    'goldman sachs', 'jpmorgan', 'morgan stanley',
    'immigration', 'deportation',
    'semiconductor', 'chip ban', 'ai regulation',
    'treasury yield', 'credit spread', 'bond auction', 'term premium', 'tlt',
}

# Pre-compiled regexes for word-boundary safe matching
_RE_KEYWORDS = _build_kw_regex(KEYWORDS)
_RE_CRYPTO_CORE = _build_kw_regex(CRYPTO_CORE_KEYWORDS)
_RE_IMPACT = _build_kw_regex(IMPACT_KEYWORDS)
_RE_MACRO_STANDALONE = _build_kw_regex(MACRO_STANDALONE_KEYWORDS)
_RE_GOSSIP = _build_kw_regex(GOSSIP_BLOCKLIST)


def _is_gossip(title: str) -> bool:
    """가십/노이즈 키워드 매칭 시 True → GPT 호출 없이 스킵."""
    t = (title or '').lower()
    return _kw_match(t, _RE_GOSSIP)

# ── DB 에러 알림 ──
_error_alert_cache = {}  # {dedup_key: (last_ts, count)}
_ERROR_ALERT_COOLDOWN_SEC = 1800  # 30분
_ERROR_CACHE_MAX_SIZE = 200  # max entries before pruning

def _prune_error_cache():
    """Remove expired entries from _error_alert_cache to prevent unbounded growth."""
    if len(_error_alert_cache) <= _ERROR_CACHE_MAX_SIZE:
        return
    now = time.time()
    expired = [k for k, (ts, _) in _error_alert_cache.items()
               if now - ts > _ERROR_ALERT_COOLDOWN_SEC * 2]
    for k in expired:
        del _error_alert_cache[k]

def _send_error_alert(source, title, exception, dedup_key=None):
    """DB 에러 발생 시 Telegram 알림. 30분 쿨다운으로 스팸 방지."""
    import urllib.parse, urllib.request
    now = time.time()
    if dedup_key is None:
        dedup_key = f"{type(exception).__name__}:{source}"

    # Prune old entries periodically
    _prune_error_cache()

    cached = _error_alert_cache.get(dedup_key)
    if cached:
        last_ts, count = cached
        if now - last_ts < _ERROR_ALERT_COOLDOWN_SEC:
            _error_alert_cache[dedup_key] = (last_ts, count + 1)
            return  # 스팸 방지

    count = (cached[1] + 1) if cached else 1
    _error_alert_cache[dedup_key] = (now, 0)

    err_type = type(exception).__name__
    err_msg = str(exception)[:200]
    text = (
        f"[news_bot] DB 오류\n"
        f"- 소스: {source}\n"
        f"- 제목: {(title or '')[:60]}\n"
        f"- 오류: {err_type}: {err_msg}\n"
        f"- 연속: {count}회"
    )

    try:
        env_path = '/root/trading-bot/app/telegram_cmd.env'
        token, chat_id = '', ''
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                k, v = line.split('=', 1)
                if k.strip() == 'TELEGRAM_BOT_TOKEN':
                    token = v.strip()
                elif k.strip() == 'TELEGRAM_ALLOWED_CHAT_ID':
                    chat_id = v.strip()
        if token and chat_id:
            url = f'https://api.telegram.org/bot{token}/sendMessage'
            data = urllib.parse.urlencode({'chat_id': chat_id, 'text': text}).encode('utf-8')
            req = urllib.request.Request(url, data=data, method='POST')
            urllib.request.urlopen(req, timeout=10)
    except Exception:
        pass

def log(msg):
    print(msg, flush=True)

def _load_watch_keywords(db):
    """Load keywords from openclaw_policies. Falls back to hardcoded KEYWORDS."""
    try:
        with db.cursor() as cur:
            cur.execute("""
                SELECT value FROM openclaw_policies WHERE key = 'watch_keywords';
            """)
            row = cur.fetchone()
            if row and row[0]:
                custom = row[0] if isinstance(row[0], list) else json.loads(row[0])
                if isinstance(custom, list) and len(custom) > 0:
                    return custom
    except Exception:
        pass
    return list(KEYWORDS)


def worth_llm(title: str, active_keywords=None, source: str = '') -> bool:
    """AND-기반 키워드 매칭. crypto+impact 동시 충족 또는 macro standalone."""
    t = (title or "").lower()
    # 크립토 전용 피드는 바이패스
    if (source or '').lower() in CRYPTO_FEEDS:
        return True
    # 매크로 스탠드얼론: 단독 통과
    if _kw_match(t, _RE_MACRO_STANDALONE):
        return True
    # AND 조건: crypto + impact 동시
    has_crypto = _kw_match(t, _RE_CRYPTO_CORE)
    has_impact = _kw_match(t, _RE_IMPACT)
    if has_crypto and has_impact:
        return True
    # 기존 KEYWORDS fallback (하위호환)
    if active_keywords is not None:
        _re_active = _build_kw_regex(active_keywords)
        return _kw_match(t, _re_active)
    return _kw_match(t, _RE_KEYWORDS)

def extract_keywords(title: str, active_keywords=None):
    t = (title or "").lower()
    kw_list = active_keywords if active_keywords is not None else KEYWORDS
    hits = [k for k in kw_list if re.search(
        r'\b' + re.escape(k) + r'\b' if len(k) <= _SHORT_KW_THRESHOLD and k.isascii()
        else re.escape(k), t, re.IGNORECASE)]
    return hits

def get_openai():
    key = os.getenv("OPENAI_API_KEY", "")
    if not key:
        return None
    return OpenAI(api_key=key)

def llm_analyze(client, title):
    prompt = {
        "title": title,
        "task": "비트코인 선물에 미칠 영향 평가",
        "schema": {
            "impact_score": "0~10 (0=무관, 5=보통, 8+=높음)",
            "direction": "up/down/neutral",
            "category": "WAR/US_POLITICS/US_POLITICS_ELECTION/US_FISCAL/US_SCANDAL_LEGAL/FED_RATES/CPI_JOBS/NASDAQ_EQUITIES/TECH_NASDAQ/REGULATION_SEC_ETF/JAPAN_BOJ/CHINA/EUROPE_ECB/FIN_STRESS/WALLSTREET_SIGNAL/IMMIGRATION_POLICY/MACRO_RATES/CRYPTO_SPECIFIC/OTHER",
            "relevance": "HIGH/MED/LOW/GOSSIP — 암호화폐/거시경제 무관이면 GOSSIP",
            "impact_path": "예: 금리인상→달러강세→BTC하락",
            "summary_kr": "한국어 1~2문장",
            "title_ko": "뉴스 제목 한국어 번역",
            "tier": "TIER1/TIER2/TIER3/TIERX 분류",
            "relevance_score": "0.0~1.0 BTC 선물 실질 연관도",
            "topic_class": "macro/crypto/noise — 3-way 대분류",
            "asset_relevance": "BTC_DIRECT/BTC_INDIRECT/NONE",
        },
        "tier_guide": {
            "TIER1": "연준/FOMC/Powell, CPI/PPI/NFP 핵심지표, BTC ETF 대규모 자금흐름, SEC/규제, 지정학(전쟁급), 대형기관 BTC 매수/매도, 국채 금리 급변(MACRO_RATES), 부채한도 위기(US_FISCAL)",
            "TIER2": "나스닥/QQQ 1%+ 변동 원인, 금융시스템 리스크(은행/채권 급변), 주요국 정책, 대선/중간선거(US_POLITICS_ELECTION), 기술/반도체(TECH_NASDAQ), 기소/탄핵(US_SCANDAL_LEGAL), 월가 시그널(WALLSTREET_SIGNAL), 이민정책(IMMIGRATION_POLICY)",
            "TIER3": "일반 크립토 시황, BTC 직접 연결 약한 개별 기업/이슈",
            "TIERX": "개인사연, 주식추천, 칼럼, 클릭유도, 노이즈, 암호화폐/거시경제 무관",
        },
        "classification_rules": (
            "AI 주식 추천, 중국 기업 비교, 개인 투자 스토리는 topic_class=noise로 분류. "
            "bitcoin/btc/crypto 단어 포함만으로 asset_relevance=BTC_DIRECT 분류 금지. "
            "BTC ETF, BTC 반감기, BTC 직접 규제만 CRYPTO_SPECIFIC+BTC_DIRECT. "
            "일반 AI/기술주 뉴스는 asset_relevance=NONE."
        ),
    }
    resp = client.responses.create(
        model="gpt-4o-mini",
        input=json.dumps(prompt, ensure_ascii=False)
    )
    text = resp.output_text or ""
    try:
        data = json.loads(text)
        impact = int(data.get("impact_score", 0) or 0)
        direction = (data.get("direction", "neutral") or "neutral").strip()
        category = (data.get("category", "OTHER") or "OTHER").strip()
        relevance = (data.get("relevance", "MED") or "MED").strip().upper()
        impact_path = (data.get("impact_path", "") or "").strip()
        summary_kr = (data.get("summary_kr", "") or "").strip()
        title_ko = (data.get("title_ko", "") or "").strip()
        tier = (data.get("tier", "UNKNOWN") or "UNKNOWN").strip().upper()
        try:
            rel_score = float(data.get("relevance_score", 0) or 0)
            rel_score = max(0.0, min(1.0, rel_score))
        except (TypeError, ValueError):
            rel_score = 0.0
        topic_class = (data.get("topic_class", "noise") or "noise").strip().lower()
        if topic_class not in ('macro', 'crypto', 'noise'):
            topic_class = 'noise'
        asset_relevance = (data.get("asset_relevance", "NONE") or "NONE").strip().upper()
        if asset_relevance not in ('BTC_DIRECT', 'BTC_INDIRECT', 'NONE'):
            asset_relevance = 'NONE'
        if not title_ko:
            title_ko = summary_kr.split('.')[0] if summary_kr else ""
        # Encode category + impact_path into summary field
        summary = f"[{direction}] [{category}] {summary_kr}"
        if impact_path:
            summary += f" | {impact_path}"
        return impact, direction, summary, title_ko, relevance, tier, rel_score, topic_class, asset_relevance
    except Exception:
        return 0, "neutral", text[:200], "", "MED", "UNKNOWN", 0.0, "noise", "NONE"

def ensure_table(db):
    """
    현재 실DB에 존재하는 스키마와 맞춤:
      id, ts(timestamptz), source, title, url, summary, impact_score, keywords(text[])
    (테이블이 없을 때만 생성)
    """
    with db.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS public.news (
          id BIGSERIAL PRIMARY KEY,
          ts TIMESTAMPTZ NOT NULL DEFAULT now(),
          source TEXT,
          title TEXT,
          url TEXT UNIQUE,
          summary TEXT,
          impact_score INT,
          keywords TEXT[],
          title_ko TEXT
        );
        """)
        db.commit()

def db_news_summary(db, minutes=60, limit=20) -> str:
    with db.cursor() as cur:
        cur.execute("""
            WITH recent AS (
              SELECT id, ts, source, COALESCE(impact_score,0) AS impact_score,
                     COALESCE(title_ko, title, '(제목 없음)') AS display_title, url
              FROM public.news
              WHERE ts >= now() - (%s || ' minutes')::interval
              ORDER BY id DESC
              LIMIT %s
            )
            SELECT
              '📰 DB 뉴스 (최근 ' || %s || '분) 건수=' || (SELECT count(*) FROM recent) || E'\n'
              || COALESCE(string_agg(
                  '• [' || to_char(ts AT TIME ZONE 'Asia/Seoul', 'MM-DD HH24:MI') || ' KST] '
                  || '(' || impact_score || ') '
                  || display_title || E'\n  ' || COALESCE(url,'')
                , E'\n'), '• (없음)')
            FROM recent;
        """, (str(minutes), int(limit), str(minutes)))
        row = cur.fetchone()
        return (row[0] if row and row[0] else "📰 DB 뉴스\n• (없음)")

def run_summary_once():
    db = get_conn(autocommit=True)
    ensure_table(db)
    minutes = int(os.getenv("NEWS_SUMMARY_MINUTES", "60"))
    limit = int(os.getenv("NEWS_SUMMARY_LIMIT", "20"))
    print(db_news_summary(db, minutes=minutes, limit=limit))
    db.close()


def _ensure_conn(db):
    """DB 커넥션 상태 확인 후 끊겼으면 재연결."""
    if db is None or db.closed:
        log("[news_bot] DB 재연결 시도...")
        return get_conn(autocommit=True)
    try:
        with db.cursor() as cur:
            cur.execute("SELECT 1")
        return db
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        log("[news_bot] DB 커넥션 끊김, 재연결...")
        try:
            db.close()
        except Exception:
            pass
        return get_conn(autocommit=True)


def main():
    # 요약만 출력하고 종료 (텔레그램 "DB 뉴스 요약"용)
    if len(sys.argv) > 1 and sys.argv[1] == "--summary":
        run_summary_once()
        return

    log(f"[news_bot] START poll={NEWS_POLL_SEC}s")
    client = get_openai()
    if client is None:
        log("[news_bot] NOTE: OPENAI_API_KEY 없음/자리표시자 -> LLM 없이 저장만 진행")

    db = get_conn(autocommit=True)
    ensure_table(db)

    while True:
        try:
            # 매 TICK마다 커넥션 상태 확인
            db = _ensure_conn(db)
        except Exception as e:
            log(f"[news_bot] DB 재연결 실패: {e}")
            time.sleep(30)
            continue

        log("[news_bot] TICK")
        inserted = 0
        db_errors = 0
        circuit_break = False
        skipped_gossip = 0
        skipped_low_relevance = 0
        skipped_hard_exclude = 0
        skipped_keyword_and = 0
        active_keywords = _load_watch_keywords(db)
        log(f"[news_bot] active keywords: {len(active_keywords)}")

        for source, url in FEEDS:
            if circuit_break:
                break
            try:
                feed = feedparser.parse(url, agent=FEED_AGENT)
                entries = getattr(feed, "entries", []) or []
                log(f"[news_bot] feed={source} entries={len(entries)}")
            except Exception:
                log("[news_bot] ERROR feed parse")
                log(traceback.format_exc())
                continue

            # Batch URL dedup: collect all URLs first, then check DB in one query
            _feed_entries = []
            for e in entries[:20]:
                title = getattr(e, "title", "") or ""
                link = getattr(e, "link", "") or ""
                if title and link:
                    _feed_entries.append((title, link))
            _existing_urls = set()
            if _feed_entries:
                try:
                    _all_urls = [link for _, link in _feed_entries]
                    with db.cursor() as cur:
                        cur.execute("SELECT url FROM public.news WHERE url = ANY(%s)", (_all_urls,))
                        _existing_urls = {r[0] for r in cur.fetchall()}
                except Exception:
                    pass  # fallback: treat all as new

            for title, link in _feed_entries:
                if link in _existing_urls:
                    continue

                try:

                    # 2) 하드 제외 패턴 (GPT 호출 전)
                    if _is_hard_excluded(title):
                        skipped_hard_exclude += 1
                        continue

                    # 3) 가십/노이즈 하드 필터 (GPT 호출 전)
                    if _is_gossip(title):
                        skipped_gossip += 1
                        continue

                    # 4) 소스 티어
                    source_tier = _get_source_tier(source)

                    # 5) AND-기반 키워드 체크 (crypto 피드는 바이패스)
                    if not worth_llm(title, active_keywords, source):
                        skipped_keyword_and += 1
                        continue

                    # 6) GPT 분류
                    impact, direction, summary, title_ko, relevance = 0, "neutral", "", "", "MED"
                    tier, rel_score = "UNKNOWN", 0.0
                    topic_class, asset_relevance = "noise", "NONE"
                    if client:
                        try:
                            impact, direction, summary, title_ko, relevance, tier, rel_score, topic_class, asset_relevance = llm_analyze(client, title)
                        except Exception as llm_err:
                            log(f"[news_bot] LLM error: {llm_err}")

                    # 7) 소스 가중치 기반 티어 캡 (weight < 0.6 → TIER3)
                    _DENY_THRESHOLD = 0.60
                    try:
                        _sw = _ncc.get_source_weight(source)
                        _DENY_THRESHOLD = _ncc.DENY_SOURCE_WEIGHT_THRESHOLD
                    except Exception:
                        _sw = 0.55 if source_tier == 'REFERENCE_ONLY' else 0.70
                    if _sw < _DENY_THRESHOLD and tier in ('TIER1', 'TIER2'):
                        tier = 'TIER3'

                    # 8) 유효 티어 가드
                    valid_tiers = {'TIER1', 'TIER2', 'TIER3', 'TIERX'}
                    if tier not in valid_tiers:
                        tier = 'UNKNOWN'

                    # 9) exclusion_reason — v2 three-condition deny
                    exclusion_reason = None
                    _low_tier = tier in ('TIER3', 'TIERX')
                    _low_rel = (rel_score > 0 and rel_score < 0.55) or relevance in ('GOSSIP', 'LOW')
                    _low_src = _sw < _DENY_THRESHOLD  # from step 7

                    if tier == 'TIERX':
                        exclusion_reason = 'TIERX: noise/column/stock_pick'
                    elif _low_tier and _low_rel and _low_src:
                        exclusion_reason = f'triple_low: tier={tier} rel={rel_score:.2f} w={_sw:.2f}'

                    # 제외된 뉴스도 DB에 저장 (추적용), 카운터 증가
                    if exclusion_reason:
                        skipped_low_relevance += 1

                    kw = extract_keywords(title, active_keywords)

                    # 9.5) Shadow classifier — preview_classify (two-tier allow)
                    allow_storage = False
                    allow_trading = False
                    _shadow = {}
                    try:
                        _shadow = _ncc.preview_classify(
                            title, source, impact,
                            summary=summary or '', title_ko=title_ko or '')
                        # If GPT returned generic/noise topic, use shadow's detailed classification
                        _shadow_topic_preview = _shadow.get('topic_class_preview', 'unclassified')
                        if _shadow_topic_preview not in ('unclassified', '', None):
                            if topic_class in ('noise', 'macro', '', None):
                                # Normalize shadow enum → GPT enum for consistency
                                topic_class = _SHADOW_TO_GPT_CATEGORY.get(
                                    _shadow_topic_preview, _shadow_topic_preview)
                        # If GPT didn't assign tier, use shadow
                        if tier in ('UNKNOWN', None) and _shadow.get('tier_preview'):
                            tier = _shadow['tier_preview']
                        # If relevance_score is missing, use shadow
                        if (not rel_score or rel_score <= 0) and _shadow.get('relevance_score_preview'):
                            rel_score = _shadow['relevance_score_preview']
                        # Two-tier allow decisions
                        allow_storage = _shadow.get('allow_for_storage', False)
                        allow_trading = _shadow.get('allow_for_trading', False)
                    except Exception:
                        pass  # shadow classifier failure should never block insertion

                    # 9.6) Scandal confirmation gate
                    _shadow_topic = _shadow.get('topic_class_preview', '')
                    _scandal_topic = _shadow_topic if _shadow_topic == 'US_SCANDAL_LEGAL' else (
                        topic_class if topic_class == 'US_SCANDAL_LEGAL' else '')
                    if _scandal_topic == 'US_SCANDAL_LEGAL':
                        _scandal_cur = None
                        try:
                            try:
                                _scandal_cur = db.cursor()
                            except Exception:
                                pass
                            _scandal = _ncc.scandal_confirmation_check(
                                'US_SCANDAL_LEGAL', title, source, impact,
                                db_cursor=_scandal_cur)
                            if not _scandal['confirmed']:
                                allow_trading = False
                                exclusion_reason = exclusion_reason or f"scandal_unconfirmed: {_scandal['reason']}"
                            if _scandal.get('impact_cap') and impact > _scandal['impact_cap']:
                                impact = _scandal['impact_cap']
                        except Exception:
                            pass
                        finally:
                            if _scandal_cur:
                                try:
                                    _scandal_cur.close()
                                except Exception:
                                    pass

                    # 9.7) BTC keyword fallback for still-unclassified items
                    if topic_class in ('unclassified', 'noise', '', None):
                        _btc_kws = {'bitcoin', 'btc', 'crypto', 'cryptocurrency',
                                    'blockchain', 'halving', 'mining', 'defi', 'exchange'}
                        title_lower = (title or '').lower()
                        if any(kw in title_lower for kw in _btc_kws):
                            topic_class = 'CRYPTO_GENERAL'
                            if not allow_storage:
                                allow_storage = True

                    # 10) Compute trading_impact_weight = source_weight * relevance_score
                    try:
                        _src_w = _ncc.get_source_weight(source)
                        _trading_impact_weight = round(_src_w * (rel_score or 0), 4)
                    except Exception:
                        _trading_impact_weight = 0

                    # 11) DB INSERT (tier, relevance_score, source_tier, exclusion_reason, topic_class, asset_relevance, allow_storage, allow_trading, trading_impact_weight)
                    with db.cursor() as cur:
                        cur.execute("""
                            INSERT INTO public.news(source, title, url, summary, impact_score,
                                                    keywords, title_ko, tier, relevance_score,
                                                    source_tier, exclusion_reason,
                                                    topic_class, asset_relevance,
                                                    allow_storage, allow_trading,
                                                    trading_impact_weight)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (url) DO UPDATE SET
                                summary = EXCLUDED.summary,
                                impact_score = EXCLUDED.impact_score,
                                keywords = EXCLUDED.keywords,
                                title_ko = EXCLUDED.title_ko,
                                tier = EXCLUDED.tier,
                                relevance_score = EXCLUDED.relevance_score,
                                source_tier = EXCLUDED.source_tier,
                                exclusion_reason = EXCLUDED.exclusion_reason,
                                topic_class = EXCLUDED.topic_class,
                                asset_relevance = EXCLUDED.asset_relevance,
                                allow_storage = EXCLUDED.allow_storage,
                                allow_trading = EXCLUDED.allow_trading,
                                trading_impact_weight = EXCLUDED.trading_impact_weight
                            WHERE EXCLUDED.impact_score > COALESCE(news.impact_score, 0)
                               OR news.tier IS NULL
                        """, (
                            source,
                            title,
                            link,
                            summary if summary else f"[{direction}]",
                            int(impact),
                            kw if kw else None,
                            title_ko if title_ko else None,
                            tier,
                            rel_score if rel_score > 0 else None,
                            source_tier,
                            exclusion_reason,
                            topic_class,
                            asset_relevance,
                            allow_storage,
                            allow_trading,
                            _trading_impact_weight,
                        ))
                    inserted += 1

                except (psycopg2.OperationalError, psycopg2.InterfaceError) as db_err:
                    db_errors += 1
                    if db_errors <= 1:
                        log(f"[news_bot] DB 커넥션 오류: {db_err}")
                        _send_error_alert(source, title, db_err)
                    try:
                        db.close()
                    except Exception:
                        pass
                    try:
                        db = get_conn(autocommit=True)
                        log("[news_bot] DB 재연결 성공")
                    except Exception as re_err:
                        log(f"[news_bot] DB 재연결 실패: {re_err}")
                        break
                    # Circuit breaker: too many consecutive DB errors
                    if db_errors >= MAX_CONSECUTIVE_DB_ERRORS:
                        backoff = min(MAX_BACKOFF_SEC, BACKOFF_BASE_SEC * (2 ** min(db_errors - MAX_CONSECUTIVE_DB_ERRORS, 5)))
                        log(f"[news_bot] 서킷브레이커 발동: 연속 DB 오류 {db_errors}회, {backoff}초 대기")
                        time.sleep(backoff)
                        circuit_break = True
                        break
                    continue

                except psycopg2.DataError as de:
                    log(f"[news_bot] DataError (skip): {de}")
                    _send_error_alert(source, title, de)
                    db_errors += 1
                    continue

                except psycopg2.IntegrityError as ie:
                    # URL duplicate → normal (ON CONFLICT handles), non-URL → alert
                    if 'url' not in str(ie).lower():
                        log(f"[news_bot] IntegrityError (skip): {ie}")
                        _send_error_alert(source, title, ie)
                    db_errors += 1
                    continue

                except Exception as ex:
                    log(f"[news_bot] ERROR insert: {ex}")
                    log(traceback.format_exc())
                    db_errors += 1
                    continue

        log(f"[news_bot] DONE inserted={inserted}, skipped_hard_exclude={skipped_hard_exclude}, skipped_gossip={skipped_gossip}, skipped_keyword_and={skipped_keyword_and}, skipped_low_relevance={skipped_low_relevance}, db_errors={db_errors}, sleep={NEWS_POLL_SEC}s")
        time.sleep(NEWS_POLL_SEC)

if __name__ == "__main__":
    main()
