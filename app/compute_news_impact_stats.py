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
        'FIN_STRESS', 'CRYPTO_SPECIFIC', 'OTHER',
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


if __name__ == '__main__':
    conn = _db_conn()
    conn.autocommit = True
    try:
        compute_stats(conn)
    finally:
        conn.close()
