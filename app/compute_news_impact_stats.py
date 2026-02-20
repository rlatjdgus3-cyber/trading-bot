"""
compute_news_impact_stats.py — Backfill news_impact_stats table.

Joins news_events (news table) + macro_trace to compute per-category
(event_type, region, regime) aggregate statistics.

Usage: python3 compute_news_impact_stats.py
"""
import os
import sys
import json
sys.path.insert(0, '/root/trading-bot/app')

LOG_PREFIX = '[compute_news_impact_stats]'
STATS_VERSION = '2026.02.14'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _db_conn():
    from db_config import get_conn
    return get_conn()


def _parse_category(summary):
    """Extract category from summary like '[up] [FED_RATES] ...'."""
    import re
    if not summary:
        return 'OTHER'
    tags = re.findall(r'\[([A-Za-z_]+)\]', summary)
    direction_tags = {'up', 'down', 'neutral', 'bullish', 'bearish'}
    known_categories = {
        'FED_RATES', 'CPI_JOBS', 'REGULATION_SEC_ETF', 'WAR',
        'US_POLITICS', 'NASDAQ_EQUITIES', 'JAPAN_BOJ', 'CHINA',
        'FIN_STRESS', 'CRYPTO_SPECIFIC', 'EUROPE_ECB', 'OTHER',
        # P1: US politics / macro expansion
        'US_POLITICS_ELECTION', 'US_FISCAL', 'US_SCANDAL_LEGAL',
        'WALLSTREET_SIGNAL', 'IMMIGRATION_POLICY', 'TECH_NASDAQ', 'MACRO_RATES',
    }
    for tag in tags:
        if tag.lower() in direction_tags:
            continue
        if tag in known_categories:
            return tag
    return 'OTHER'


def compute_stats(conn):
    """Compute and upsert news_impact_stats from news + macro_trace."""
    _log('Starting stats computation...')
    cur = conn.cursor()

    # Ensure table exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.news_impact_stats (
            id SERIAL PRIMARY KEY,
            event_type VARCHAR(50) NOT NULL,
            region VARCHAR(20) DEFAULT 'GLOBAL',
            regime VARCHAR(20) DEFAULT 'NORMAL',
            avg_ret_2h FLOAT,
            med_ret_2h FLOAT,
            std_ret_2h FLOAT,
            avg_abs_ret_2h FLOAT,
            sample_count INT DEFAULT 0,
            last_updated TIMESTAMP DEFAULT NOW(),
            stats_version VARCHAR(30),
            UNIQUE(event_type, region, regime)
        );
    """)
    conn.commit()

    # Join news + macro_trace: get (category, btc_ret_2h) pairs
    cur.execute("""
        SELECT n.summary, mt.btc_ret_2h, mt.regime_at_time
        FROM news n
        JOIN macro_trace mt ON mt.news_id = n.id
        WHERE n.ts >= '2024-01-01'
          AND mt.btc_ret_2h IS NOT NULL
          AND n.impact_score > 0
        ORDER BY n.ts;
    """)
    rows = cur.fetchall()
    _log(f'Found {len(rows)} news+trace pairs')

    if not rows:
        _log('No data to compute. Exiting.')
        return

    # Aggregate by (category, regime)
    from collections import defaultdict
    import statistics

    buckets = defaultdict(list)  # (category, regime) -> [ret_2h, ...]
    for summary, ret_2h, regime in rows:
        cat = _parse_category(summary)
        regime_label = regime or 'NORMAL'
        buckets[(cat, regime_label)].append(float(ret_2h))

    _log(f'Computed {len(buckets)} category-regime buckets')

    # Upsert stats
    upserted = 0
    for (cat, regime), rets in buckets.items():
        if len(rets) < 2:
            continue
        avg_ret = statistics.mean(rets)
        med_ret = statistics.median(rets)
        std_ret = statistics.stdev(rets) if len(rets) >= 2 else 0
        avg_abs = statistics.mean([abs(r) for r in rets])
        sample_n = len(rets)

        cur.execute("""
            INSERT INTO news_impact_stats
                (event_type, region, regime, avg_ret_2h, med_ret_2h,
                 std_ret_2h, avg_abs_ret_2h, sample_count,
                 last_updated, stats_version)
            VALUES (%s, 'GLOBAL', %s, %s, %s, %s, %s, %s, NOW(), %s)
            ON CONFLICT (event_type, region, regime)
            DO UPDATE SET
                avg_ret_2h = EXCLUDED.avg_ret_2h,
                med_ret_2h = EXCLUDED.med_ret_2h,
                std_ret_2h = EXCLUDED.std_ret_2h,
                avg_abs_ret_2h = EXCLUDED.avg_abs_ret_2h,
                sample_count = EXCLUDED.sample_count,
                last_updated = NOW(),
                stats_version = EXCLUDED.stats_version;
        """, (cat, regime, avg_ret, med_ret, std_ret, avg_abs,
              sample_n, STATS_VERSION))
        upserted += 1

    conn.commit()
    _log(f'Upserted {upserted} stats rows (version={STATS_VERSION})')

    # Print summary
    cur.execute("""
        SELECT event_type, regime, avg_abs_ret_2h, sample_count
        FROM news_impact_stats
        WHERE stats_version = %s
        ORDER BY avg_abs_ret_2h DESC;
    """, (STATS_VERSION,))
    for r in cur.fetchall():
        _log(f'  {r[0]:25s} regime={r[1]:8s} avg_abs_ret={r[2]:.4f}% n={r[3]}')


def compute_direction_accuracy(conn):
    """소스×카테고리별 방향 예측 적중률 계산.

    news.summary에서 방향(up/down) 파싱 → macro_trace.btc_ret_2h 부호와 비교.
    news_source_accuracy 테이블에 UPSERT + news.direction_hit 개별 업데이트.
    """
    import re
    _log('Starting direction accuracy computation...')
    cur = conn.cursor()

    # Ensure target table exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.news_source_accuracy (
            id SERIAL PRIMARY KEY,
            source TEXT NOT NULL,
            category TEXT DEFAULT 'ALL',
            total_predictions INT DEFAULT 0,
            correct_predictions INT DEFAULT 0,
            hit_rate NUMERIC DEFAULT 0.0,
            last_updated TIMESTAMP DEFAULT NOW(),
            UNIQUE(source, category)
        );
    """)
    conn.commit()

    # Join news + macro_trace for direction comparison
    cur.execute("""
        SELECT n.id, n.source, n.summary, mt.btc_ret_2h
        FROM news n
        JOIN macro_trace mt ON mt.news_id = n.id
        WHERE n.ts >= now() - interval '90 days'
          AND mt.btc_ret_2h IS NOT NULL
          AND n.impact_score > 0
          AND n.summary IS NOT NULL
        ORDER BY n.ts;
    """)
    rows = cur.fetchall()
    _log(f'Found {len(rows)} news+trace pairs for direction accuracy')

    if not rows:
        _log('No data for direction accuracy. Exiting.')
        return

    from collections import defaultdict

    # Parse direction from summary and compare with actual ret_2h
    source_stats = defaultdict(lambda: {'total': 0, 'correct': 0})
    updates = []  # (direction_hit, news_id) pairs

    for news_id, source, summary, ret_2h in rows:
        # Parse direction from summary: [up] or [down]
        if not summary:
            continue
        sl = summary.lower().strip()
        if sl.startswith('[up]'):
            predicted_dir = 1
        elif sl.startswith('[down]'):
            predicted_dir = -1
        else:
            continue  # neutral or unparseable → skip

        actual_dir = 1 if float(ret_2h) > 0 else -1
        hit = (predicted_dir == actual_dir)

        # Aggregate by (source, 'ALL')
        key = (source or 'unknown', 'ALL')
        source_stats[key]['total'] += 1
        if hit:
            source_stats[key]['correct'] += 1

        # Per-category aggregation
        cat = _parse_category(summary)
        cat_key = (source or 'unknown', cat)
        source_stats[cat_key]['total'] += 1
        if hit:
            source_stats[cat_key]['correct'] += 1

        updates.append((hit, news_id))

    # Update news.direction_hit for individual news items
    updated_hits = 0
    for hit, news_id in updates:
        try:
            cur.execute("""
                UPDATE news SET direction_hit = %s WHERE id = %s AND direction_hit IS NULL;
            """, (hit, news_id))
            if cur.rowcount > 0:
                updated_hits += 1
        except Exception:
            pass
    conn.commit()
    _log(f'Updated {updated_hits} news.direction_hit values')

    # UPSERT news_source_accuracy
    upserted = 0
    for (source, category), stats in source_stats.items():
        total = stats['total']
        correct = stats['correct']
        if total < 3:
            continue  # 최소 3건 이상만
        hit_rate = round(correct / total, 4) if total > 0 else 0.0

        cur.execute("""
            INSERT INTO news_source_accuracy (source, category, total_predictions,
                                              correct_predictions, hit_rate, last_updated)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (source, category)
            DO UPDATE SET
                total_predictions = EXCLUDED.total_predictions,
                correct_predictions = EXCLUDED.correct_predictions,
                hit_rate = EXCLUDED.hit_rate,
                last_updated = NOW();
        """, (source, category, total, correct, hit_rate))
        upserted += 1

    conn.commit()
    _log(f'Upserted {upserted} source accuracy rows')

    # Print summary
    cur.execute("""
        SELECT source, category, hit_rate, total_predictions
        FROM news_source_accuracy
        WHERE category = 'ALL'
        ORDER BY total_predictions DESC;
    """)
    for r in cur.fetchall():
        _log(f'  {r[0]:20s} cat={r[1]:5s} hit_rate={r[2]:.2f} n={r[3]}')


def check_data_coverage(conn):
    """2023-11 이후 월별 뉴스 커버리지 점검 리포트."""
    _log('Starting data coverage check...')
    cur = conn.cursor()
    cur.execute("""
        SELECT to_char(ts, 'YYYY-MM') AS month,
               count(*) AS news_count,
               count(DISTINCT source) AS source_count,
               count(*) FILTER (WHERE impact_score >= 7) AS high_impact
        FROM news
        WHERE ts >= '2023-11-01'
        GROUP BY 1 ORDER BY 1;
    """)
    rows = cur.fetchall()
    lines = ['월별 뉴스 커버리지:']
    lines.append(f'{"월":>8} | {"건수":>6} | {"소스수":>5} | {"고영향":>5}')
    lines.append('-' * 35)
    low_months = []
    for month, cnt, src, high in rows:
        lines.append(f'{month:>8} | {cnt:>6} | {src:>5} | {high:>5}')
        if cnt < 50:
            low_months.append(month)
    if low_months:
        lines.append(f'\n⚠ 데이터 부족 월: {", ".join(low_months)}')
        lines.append('  → python3 backfill_macro_events.py 실행 권장')
    for line in lines:
        _log(line)
    return lines


if __name__ == '__main__':
    conn = _db_conn()
    conn.autocommit = True
    try:
        compute_stats(conn)
        compute_direction_accuracy(conn)
        check_data_coverage(conn)
    finally:
        conn.close()
