"""
backfill_news_classification_and_reaction.py — GPT 배치 분류 + 시장반응 계산.

Part A: tier=UNKNOWN 뉴스를 GPT-4o-mini로 분류 (tier, topic_class, asset_relevance 등)
Part B: 뉴스별 시장반응 계산 (ret_30m, ret_2h, ret_24h, vol 등)

Usage:
    python backfill_news_classification_and_reaction.py                  # 전체
    python backfill_news_classification_and_reaction.py --classify-only  # 분류만
    python backfill_news_classification_and_reaction.py --reaction-only  # 반응만
    python backfill_news_classification_and_reaction.py --resume         # 재개
    python backfill_news_classification_and_reaction.py --batch-size 100 # 배치 크기
"""
import os
import sys
import json
import time
import traceback
import argparse
from datetime import datetime, timezone

sys.path.insert(0, '/root/trading-bot/app')
from db_config import get_conn
from backfill_utils import start_job, get_last_cursor, update_progress, finish_job, check_stop, check_pause

LOG_PREFIX = '[backfill_news_class]'
JOB_NAME_CLASSIFY = 'backfill_news_classify'
JOB_NAME_REACTION = 'backfill_news_reaction'
SYMBOL = 'BTC/USDT:USDT'
BATCH_SIZE = 50
RETAIN_1M_DAYS = 180


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _get_openai_client():
    """Get OpenAI client. Returns None if OPENAI_API_KEY not set."""
    api_key = os.getenv('OPENAI_API_KEY', '')
    if not api_key:
        _log('OPENAI_API_KEY not set, classification skipped')
        return None
    try:
        from openai import OpenAI
        return OpenAI(api_key=api_key)
    except ImportError:
        _log('openai package not installed')
        return None


def _llm_classify(client, title):
    """Classify a single news title using GPT-4o-mini. Reuses news_bot.llm_analyze logic."""
    prompt = {
        "title": title,
        "task": "비트코인 선물에 미칠 영향 평가",
        "schema": {
            "impact_score": "0~10 (0=무관, 5=보통, 8+=높음)",
            "direction": "up/down/neutral",
            "category": "WAR/US_POLITICS/FED_RATES/CPI_JOBS/NASDAQ_EQUITIES/REGULATION_SEC_ETF/JAPAN_BOJ/CHINA/FIN_STRESS/CRYPTO_SPECIFIC/OTHER",
            "relevance": "HIGH/MED/LOW/GOSSIP",
            "impact_path": "예: 금리인상→달러강세→BTC하락",
            "summary_kr": "한국어 1~2문장",
            "title_ko": "뉴스 제목 한국어 번역",
            "tier": "TIER1/TIER2/TIER3/TIERX 분류",
            "relevance_score": "0.0~1.0 BTC 선물 실질 연관도",
            "topic_class": "macro/crypto/noise — 3-way 대분류",
            "asset_relevance": "BTC_DIRECT/BTC_INDIRECT/NONE",
        },
        "tier_guide": {
            "TIER1": "연준/FOMC/Powell, CPI/PPI/NFP 핵심지표, BTC ETF 대규모 자금흐름, SEC/규제, 지정학(전쟁급), 대형기관 BTC 매수/매도",
            "TIER2": "나스닥/QQQ 1%+ 변동 원인, 금융시스템 리스크(은행/채권 급변), 주요국 정책",
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

    try:
        resp = client.responses.create(
            model="gpt-4o-mini",
            input=json.dumps(prompt, ensure_ascii=False)
        )
        text = resp.output_text or ""
        data = json.loads(text)

        impact = int(data.get("impact_score", 0) or 0)
        direction = (data.get("direction", "neutral") or "neutral").strip()
        category = (data.get("category", "OTHER") or "OTHER").strip()
        summary_kr = (data.get("summary_kr", "") or "").strip()
        title_ko = (data.get("title_ko", "") or "").strip()
        tier = (data.get("tier", "UNKNOWN") or "UNKNOWN").strip().upper()
        impact_path = (data.get("impact_path", "") or "").strip()

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

        summary = f"[{direction}] [{category}] {summary_kr}"
        if impact_path:
            summary += f" | {impact_path}"

        return {
            'impact_score': impact,
            'direction': direction,
            'summary': summary,
            'title_ko': title_ko,
            'tier': tier,
            'relevance_score': rel_score,
            'topic_class': topic_class,
            'asset_relevance': asset_relevance,
        }
    except Exception as e:
        _log(f'LLM error: {e}')
        return None


def run_classification(conn, resume=False, batch_size=BATCH_SIZE):
    """Part A: Classify UNKNOWN news with GPT-4o-mini."""
    client = _get_openai_client()
    if not client:
        return

    last_id = 0
    if resume:
        cursor = get_last_cursor(conn, JOB_NAME_CLASSIFY)
        if cursor and 'last_news_id' in cursor:
            last_id = cursor['last_news_id']
            _log(f'Resuming classification from news_id > {last_id}')

    job_id = start_job(conn, JOB_NAME_CLASSIFY, metadata={'last_id': last_id})

    total_classified = 0
    total_failed = 0
    batch_num = 0

    try:
        while True:
            if check_stop():
                _log('Classification STOP signal received')
                update_progress(conn, job_id, {'last_news_id': last_id})
                finish_job(conn, job_id, status='PARTIAL', error='stopped_by_user')
                return
            if not check_pause():
                _log('Classification STOP during pause')
                update_progress(conn, job_id, {'last_news_id': last_id})
                finish_job(conn, job_id, status='PARTIAL', error='stopped_during_pause')
                return

            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, title FROM news
                    WHERE tier = 'UNKNOWN' AND id > %s
                    ORDER BY id ASC LIMIT %s;
                """, (last_id, batch_size))
                rows = cur.fetchall()

            if not rows:
                _log('No more UNKNOWN news to classify')
                break

            batch_num += 1
            batch_ok = 0
            batch_fail = 0

            for news_id, title in rows:
                if not title or not title.strip():
                    last_id = news_id
                    continue

                result = _llm_classify(client, title)
                if result is None:
                    batch_fail += 1
                    total_failed += 1
                    last_id = news_id
                    continue

                try:
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE news SET
                                impact_score = %s,
                                summary = %s,
                                title_ko = %s,
                                tier = %s,
                                relevance_score = %s,
                                topic_class = %s,
                                asset_relevance = %s
                            WHERE id = %s AND tier = 'UNKNOWN';
                        """, (
                            result['impact_score'],
                            result['summary'],
                            result['title_ko'],
                            result['tier'],
                            result['relevance_score'],
                            result['topic_class'],
                            result['asset_relevance'],
                            news_id,
                        ))
                    conn.commit()
                    batch_ok += 1
                    total_classified += 1
                except Exception as e:
                    conn.rollback()
                    _log(f'DB update error news_id={news_id}: {e}')
                    batch_fail += 1
                    total_failed += 1

                last_id = news_id

                # GPT rate limit: ~60 RPM for gpt-4o-mini
                time.sleep(0.3)

            update_progress(conn, job_id, {'last_news_id': last_id},
                            inserted=batch_ok, failed=batch_fail)

            _log(f'Classify batch {batch_num}: {batch_ok} ok, {batch_fail} fail, '
                 f'total={total_classified}, last_id={last_id}')

        finish_job(conn, job_id, status='COMPLETED')
        _log(f'Classification DONE: {total_classified} classified, {total_failed} failed')

    except KeyboardInterrupt:
        _log('Classification interrupted')
        update_progress(conn, job_id, {'last_news_id': last_id})
        finish_job(conn, job_id, status='PARTIAL', error='KeyboardInterrupt')
    except Exception as e:
        _log(f'Classification FATAL: {e}')
        traceback.print_exc()
        finish_job(conn, job_id, status='FAILED', error=str(e)[:500])


def _choose_price_source(ts_news):
    """Auto-select 1m candles vs 5m ohlcv based on data age."""
    from datetime import timedelta
    cutoff = datetime.now(timezone.utc) - timedelta(days=RETAIN_1M_DAYS)
    if ts_news.tzinfo is None:
        ts_news = ts_news.replace(tzinfo=timezone.utc)
    if ts_news >= cutoff:
        return '1m'
    return '5m'


def _get_price_at_hybrid(cur, ts_news, price_source_tf):
    """Get price at ts. Uses chosen source, with fallback."""
    if price_source_tf == '1m':
        cur.execute("""
            SELECT c FROM candles
            WHERE symbol=%s AND tf='1m' AND ts <= %s
            ORDER BY ts DESC LIMIT 1;
        """, (SYMBOL, ts_news))
        row = cur.fetchone()
        if row:
            return float(row[0])
    # Fallback to 5m
    cur.execute("""
        SELECT c FROM market_ohlcv
        WHERE symbol=%s AND tf='5m' AND ts <= %s
        ORDER BY ts DESC LIMIT 1;
    """, (SYMBOL, ts_news))
    row = cur.fetchone()
    return float(row[0]) if row else None


def _get_return_hybrid(cur, ts_news, minutes, price_at, price_source_tf):
    """Get return % at ts_news + N minutes. Chosen source first, then fallback."""
    if price_source_tf == '1m':
        try:
            cur.execute("""
                SELECT c FROM candles
                WHERE symbol = %s AND tf = '1m'
                  AND ts >= %s + %s * interval '1 minute'
                ORDER BY ts ASC LIMIT 1;
            """, (SYMBOL, ts_news, minutes))
            row = cur.fetchone()
            if row and price_at > 0:
                return round(((float(row[0]) - price_at) / price_at) * 100, 4)
        except Exception:
            pass
    # Fallback to 5m
    try:
        cur.execute("""
            SELECT c FROM market_ohlcv
            WHERE symbol = %s AND tf = '5m'
              AND ts >= %s + %s * interval '1 minute'
            ORDER BY ts ASC LIMIT 1;
        """, (SYMBOL, ts_news, minutes))
        row = cur.fetchone()
        if row and price_at > 0:
            return round(((float(row[0]) - price_at) / price_at) * 100, 4)
    except Exception:
        pass
    return None


def _get_avg_vol_hybrid(cur, ts_news, minutes, price_source_tf):
    """Get average volume over N minutes after ts_news. Chosen source first, then fallback."""
    if price_source_tf == '1m':
        try:
            cur.execute("""
                SELECT AVG(v) FROM candles
                WHERE symbol = %s AND tf = '1m'
                  AND ts >= %s AND ts < %s + %s * interval '1 minute';
            """, (SYMBOL, ts_news, ts_news, minutes))
            row = cur.fetchone()
            if row and row[0]:
                return round(float(row[0]), 2)
        except Exception:
            pass
    # Fallback to 5m
    try:
        cur.execute("""
            SELECT AVG(v) FROM market_ohlcv
            WHERE symbol = %s AND tf = '5m'
              AND ts >= %s AND ts < %s + %s * interval '1 minute';
        """, (SYMBOL, ts_news, ts_news, minutes))
        row = cur.fetchone()
        if row and row[0]:
            return round(float(row[0]), 2)
    except Exception:
        pass
    return None


def _classify_direction(ret_pct):
    """Classify return as UP/DOWN/FLAT."""
    if ret_pct is None:
        return None
    if ret_pct > 0.1:
        return 'UP'
    if ret_pct < -0.1:
        return 'DOWN'
    return 'FLAT'


def run_reaction(conn, resume=False, batch_size=500):
    """Part B: Compute market reaction for news items."""
    last_id = 0
    if resume:
        cursor = get_last_cursor(conn, JOB_NAME_REACTION)
        if cursor and 'last_news_id' in cursor:
            last_id = cursor['last_news_id']
            _log(f'Resuming reaction from news_id > {last_id}')

    job_id = start_job(conn, JOB_NAME_REACTION, metadata={'last_id': last_id})

    total_computed = 0
    total_skipped = 0
    batch_num = 0

    try:
        while True:
            if check_stop():
                _log('Reaction STOP signal received')
                update_progress(conn, job_id, {'last_news_id': last_id})
                finish_job(conn, job_id, status='PARTIAL', error='stopped_by_user')
                return
            if not check_pause():
                _log('Reaction STOP during pause')
                update_progress(conn, job_id, {'last_news_id': last_id})
                finish_job(conn, job_id, status='PARTIAL', error='stopped_during_pause')
                return

            with conn.cursor() as cur:
                # Find news items needing reaction computation
                cur.execute("""
                    SELECT n.id, n.ts FROM news n
                    LEFT JOIN news_market_reaction nmr ON nmr.news_id = n.id
                    WHERE n.id > %s
                      AND (nmr.id IS NULL OR nmr.ret_2h IS NULL)
                    ORDER BY n.id ASC LIMIT %s;
                """, (last_id, batch_size))
                rows = cur.fetchall()

            if not rows:
                _log('No more news needing reaction computation')
                break

            batch_num += 1
            batch_ok = 0

            for news_id, ts_news in rows:
                if ts_news is None:
                    last_id = news_id
                    total_skipped += 1
                    continue

                try:
                    with conn.cursor() as cur:
                        # Hybrid price source: 1m for recent, 5m for old
                        price_source_tf = _choose_price_source(ts_news)
                        price_at = _get_price_at_hybrid(cur, ts_news, price_source_tf)
                        if price_at is None:
                            last_id = news_id
                            total_skipped += 1
                            continue

                        # Get prices at various offsets (hybrid)
                        ret_30m = _get_return_hybrid(cur, ts_news, 30, price_at, price_source_tf)
                        ret_1h = _get_return_hybrid(cur, ts_news, 60, price_at, price_source_tf)
                        ret_2h = _get_return_hybrid(cur, ts_news, 120, price_at, price_source_tf)
                        ret_4h = _get_return_hybrid(cur, ts_news, 240, price_at, price_source_tf)
                        ret_24h = _get_return_hybrid(cur, ts_news, 1440, price_at, price_source_tf)

                        # Volume metrics (hybrid)
                        vol_30m = _get_avg_vol_hybrid(cur, ts_news, 30, price_source_tf)
                        vol_1h = _get_avg_vol_hybrid(cur, ts_news, 60, price_source_tf)
                        vol_2h = _get_avg_vol_hybrid(cur, ts_news, 120, price_source_tf)
                        vol_4h = _get_avg_vol_hybrid(cur, ts_news, 240, price_source_tf)
                        vol_24h = _get_avg_vol_hybrid(cur, ts_news, 1440, price_source_tf)

                        # Direction classification
                        direction_2h = 'neutral'
                        abs_move_2h = None
                        if ret_2h is not None:
                            abs_move_2h = abs(ret_2h)
                            if ret_2h > 0.1:
                                direction_2h = 'up'
                            elif ret_2h < -0.1:
                                direction_2h = 'down'

                        # Direction labels (30m, 24h)
                        dir_30m = _classify_direction(ret_30m)
                        dir_24h = _classify_direction(ret_24h)

                        cur.execute("""
                            INSERT INTO news_market_reaction
                                (news_id, ts_news, btc_price_at,
                                 ret_30m, ret_1h, ret_2h, ret_4h, ret_24h,
                                 vol_30m, vol_1h, vol_2h, vol_4h, vol_24h,
                                 direction_2h, abs_move_2h,
                                 price_source_tf, dir_30m, dir_24h, status)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'computed')
                            ON CONFLICT (news_id) DO UPDATE SET
                                btc_price_at = EXCLUDED.btc_price_at,
                                ret_30m = COALESCE(EXCLUDED.ret_30m, news_market_reaction.ret_30m),
                                ret_1h = COALESCE(EXCLUDED.ret_1h, news_market_reaction.ret_1h),
                                ret_2h = COALESCE(EXCLUDED.ret_2h, news_market_reaction.ret_2h),
                                ret_4h = COALESCE(EXCLUDED.ret_4h, news_market_reaction.ret_4h),
                                ret_24h = COALESCE(EXCLUDED.ret_24h, news_market_reaction.ret_24h),
                                vol_30m = COALESCE(EXCLUDED.vol_30m, news_market_reaction.vol_30m),
                                vol_1h = COALESCE(EXCLUDED.vol_1h, news_market_reaction.vol_1h),
                                vol_2h = COALESCE(EXCLUDED.vol_2h, news_market_reaction.vol_2h),
                                vol_4h = COALESCE(EXCLUDED.vol_4h, news_market_reaction.vol_4h),
                                vol_24h = COALESCE(EXCLUDED.vol_24h, news_market_reaction.vol_24h),
                                direction_2h = COALESCE(EXCLUDED.direction_2h, news_market_reaction.direction_2h),
                                abs_move_2h = COALESCE(EXCLUDED.abs_move_2h, news_market_reaction.abs_move_2h),
                                price_source_tf = EXCLUDED.price_source_tf,
                                dir_30m = COALESCE(EXCLUDED.dir_30m, news_market_reaction.dir_30m),
                                dir_24h = COALESCE(EXCLUDED.dir_24h, news_market_reaction.dir_24h),
                                status = 'computed';
                        """, (news_id, ts_news, price_at,
                              ret_30m, ret_1h, ret_2h, ret_4h, ret_24h,
                              vol_30m, vol_1h, vol_2h, vol_4h, vol_24h,
                              direction_2h, abs_move_2h,
                              price_source_tf, dir_30m, dir_24h))

                    conn.commit()
                    batch_ok += 1
                    total_computed += 1

                except Exception as e:
                    conn.rollback()
                    _log(f'Reaction error news_id={news_id}: {e}')
                    total_skipped += 1

                last_id = news_id

            update_progress(conn, job_id, {'last_news_id': last_id},
                            inserted=batch_ok)

            if batch_num % 5 == 0:
                _log(f'Reaction batch {batch_num}: total={total_computed}, skipped={total_skipped}')

        finish_job(conn, job_id, status='COMPLETED')
        _log(f'Reaction DONE: {total_computed} computed, {total_skipped} skipped')

    except KeyboardInterrupt:
        _log('Reaction interrupted')
        finish_job(conn, job_id, status='PARTIAL', error='KeyboardInterrupt')
    except Exception as e:
        _log(f'Reaction FATAL: {e}')
        traceback.print_exc()
        finish_job(conn, job_id, status='FAILED', error=str(e)[:500])


def _get_return(cur, ts_news, minutes, price_at):
    """Get return % at ts_news + N minutes from market_ohlcv (5m bars)."""
    try:
        cur.execute("""
            SELECT c FROM market_ohlcv
            WHERE symbol = %s AND tf = '5m'
              AND ts >= %s + %s * interval '1 minute'
            ORDER BY ts ASC LIMIT 1;
        """, (SYMBOL, ts_news, minutes))
        row = cur.fetchone()
        if row and price_at > 0:
            return round(((float(row[0]) - price_at) / price_at) * 100, 4)
    except Exception:
        pass
    return None


def _get_avg_vol(cur, ts_news, minutes):
    """Get average volume over N minutes after ts_news from market_ohlcv (5m bars)."""
    try:
        cur.execute("""
            SELECT AVG(v) FROM market_ohlcv
            WHERE symbol = %s AND tf = '5m'
              AND ts >= %s AND ts < %s + %s * interval '1 minute';
        """, (SYMBOL, ts_news, ts_news, minutes))
        row = cur.fetchone()
        if row and row[0]:
            return round(float(row[0]), 2)
    except Exception:
        pass
    return None


def main():
    parser = argparse.ArgumentParser(description='News classification + market reaction')
    parser.add_argument('--classify-only', action='store_true')
    parser.add_argument('--reaction-only', action='store_true')
    parser.add_argument('--resume', action='store_true')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE)
    args = parser.parse_args()

    conn = get_conn()
    conn.autocommit = False

    # Set statement_timeout to 300s
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '300000';")
    conn.commit()

    try:
        if not args.reaction_only:
            _log('=== Part A: News Classification ===')
            run_classification(conn, resume=args.resume, batch_size=args.batch_size)

        if not args.classify_only:
            _log('=== Part B: Market Reaction Computation ===')
            run_reaction(conn, resume=args.resume)

        _log('ALL DONE')
    finally:
        conn.close()


if __name__ == '__main__':
    main()
