import os, time, json, traceback, sys
import feedparser
from openai import OpenAI
from db_config import get_conn
NEWS_POLL_SEC = int(os.getenv("NEWS_POLL_SEC", "300"))
FEED_AGENT = os.getenv("NEWS_FEED_AGENT", "Mozilla/5.0 trading-bot-news/1.0")

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
]

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


def worth_llm(title: str, active_keywords=None) -> bool:
    t = (title or "").lower()
    kw_list = active_keywords if active_keywords else KEYWORDS
    return any(k in t for k in kw_list)

def extract_keywords(title: str, active_keywords=None):
    t = (title or "").lower()
    kw_list = active_keywords if active_keywords else KEYWORDS
    hits = [k for k in kw_list if k in t]
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
            "category": "WAR/US_POLITICS/FED_RATES/CPI_JOBS/NASDAQ_EQUITIES/REGULATION_SEC_ETF/JAPAN_BOJ/CHINA/FIN_STRESS/CRYPTO_SPECIFIC/OTHER",
            "impact_path": "예: 금리인상→달러강세→BTC하락",
            "summary_kr": "한국어 1~2문장",
            "title_ko": "뉴스 제목 한국어 번역",
        }
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
        impact_path = (data.get("impact_path", "") or "").strip()
        summary_kr = (data.get("summary_kr", "") or "").strip()
        title_ko = (data.get("title_ko", "") or "").strip()
        if not title_ko:
            # fallback: use first sentence of summary_kr
            title_ko = summary_kr.split('.')[0] if summary_kr else ""
        # Encode category + impact_path into summary field
        summary = f"[{direction}] [{category}] {summary_kr}"
        if impact_path:
            summary += f" | {impact_path}"
        return impact, direction, summary, title_ko
    except Exception:
        return 0, "neutral", text[:200], ""

def ensure_table(db):
    """
    ✅ 현재 실DB에 존재하는 스키마와 맞춤:
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

def main():
    # ✅ 요약만 출력하고 종료 (텔레그램 “DB 뉴스 요약”용)
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
        log("[news_bot] TICK")
        inserted = 0
        active_keywords = _load_watch_keywords(db)
        log(f"[news_bot] active keywords: {len(active_keywords)}")

        for source, url in FEEDS:
            try:
                feed = feedparser.parse(url, agent=FEED_AGENT)
                entries = getattr(feed, "entries", []) or []
                log(f"[news_bot] feed={source} entries={len(entries)}")
            except Exception:
                log("[news_bot] ERROR feed parse")
                log(traceback.format_exc())
                continue

            for e in entries[:20]:
                title = getattr(e, "title", "") or ""
                link = getattr(e, "link", "") or ""
                if not title or not link:
                    continue

                try:
                    with db.cursor() as cur:
                        cur.execute("SELECT 1 FROM public.news WHERE url=%s", (link,))
                        if cur.fetchone():
                            continue

                    impact, direction, summary, title_ko = 0, "neutral", "", ""
                    # All news get GPT-mini classification. Dedup via URL UNIQUE.
                    if client:
                        try:
                            impact, direction, summary, title_ko = llm_analyze(client, title)
                        except Exception as llm_err:
                            log(f"[news_bot] LLM error: {llm_err}")
                            impact, direction, summary, title_ko = 0, "neutral", "", ""

                    kw = extract_keywords(title, active_keywords)

                    with db.cursor() as cur:
                        cur.execute("""
                            INSERT INTO public.news(source, title, url, summary, impact_score, keywords, title_ko)
                            VALUES (%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT DO NOTHING
                        """, (
                            source,
                            title,
                            link,
                            summary if summary else f"[{direction}]",
                            int(impact),
                            kw if kw else None,
                            title_ko if title_ko else None,
                        ))
                    db.commit()
                    inserted += 1

                except Exception as ex:
                    try:
                        db.rollback()
                    except Exception:
                        pass
                    log("[news_bot] ERROR entry process/insert (rolled back)")
                    log(str(ex))
                    log(traceback.format_exc())
                    continue

        log(f"[news_bot] DONE inserted={inserted}, sleep={NEWS_POLL_SEC}s")
        time.sleep(NEWS_POLL_SEC)

if __name__ == "__main__":
    main()
