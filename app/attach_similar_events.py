"""
attach_similar_events.py — TopK similar event lookup for FACT pipeline.

Library module imported by position_manager.py.
Queries `events` table for similar historical events and builds
a performance summary for Claude prompt enrichment.
"""

LOG_PREFIX = '[similar_events]'


def _log(msg):
    print(f"{LOG_PREFIX} {msg}", flush=True)


def find_similar(cur, category=None, keywords=None, limit=5):
    """Find similar historical events from the events table.

    Strategy 1: Same category match.
    Strategy 2: Keyword overlap via GIN index.
    Returns events with btc_move data, deduplicated and sorted by start_ts DESC.
    """
    results = []
    seen_ids = set()

    # Strategy 1: category match
    if category:
        cur.execute("""
                SELECT id, kind, start_ts, vol_zscore, btc_price_at,
                       btc_move_1h, btc_move_4h, btc_move_24h,
                       direction, category, keywords
                FROM events
                WHERE category = %s
                ORDER BY start_ts DESC
                LIMIT %s;
            """, (category, limit))
        for row in cur.fetchall():
            if row[0] not in seen_ids:
                seen_ids.add(row[0])
                results.append(_row_to_dict(row))

    # Strategy 2: keyword overlap (if still need more)
    if keywords and len(results) < limit:
        remaining = limit - len(results)
        cur.execute("""
                SELECT id, kind, start_ts, vol_zscore, btc_price_at,
                       btc_move_1h, btc_move_4h, btc_move_24h,
                       direction, category, keywords
                FROM events
                WHERE keywords && %s
                ORDER BY start_ts DESC
                LIMIT %s;
            """, (keywords, remaining + len(results)))
        for row in cur.fetchall():
            if row[0] not in seen_ids and len(results) < limit:
                seen_ids.add(row[0])
                results.append(_row_to_dict(row))

    return results[:limit]


def _row_to_dict(row):
    """Convert a DB row tuple to a dict."""
    return {
        'id': row[0],
        'kind': row[1],
        'start_ts': str(row[2]) if row[2] else None,
        'vol_zscore': float(row[3]) if row[3] is not None else None,
        'btc_price_at': float(row[4]) if row[4] is not None else None,
        'btc_move_1h': float(row[5]) if row[5] is not None else None,
        'btc_move_4h': float(row[6]) if row[6] is not None else None,
        'btc_move_24h': float(row[7]) if row[7] is not None else None,
        'direction': row[8],
        'category': row[9],
        'keywords': list(row[10]) if row[10] else [],
    }


def build_performance_summary(events):
    """Build aggregate performance summary from similar events.

    Returns: {avg_move_1h, avg_move_4h, up_count, down_count,
              worst_4h, best_4h, event_count}
    """
    if not events:
        return {}

    moves_1h = [e['btc_move_1h'] for e in events if e.get('btc_move_1h') is not None]
    moves_4h = [e['btc_move_4h'] for e in events if e.get('btc_move_4h') is not None]

    up_count = sum(1 for e in events if e.get('direction') == 'UP')
    down_count = sum(1 for e in events if e.get('direction') == 'DOWN')

    summary = {
        'event_count': len(events),
        'up_count': up_count,
        'down_count': down_count,
        'avg_move_1h': round(sum(moves_1h) / len(moves_1h), 4) if moves_1h else None,
        'avg_move_4h': round(sum(moves_4h) / len(moves_4h), 4) if moves_4h else None,
        'worst_4h': round(min(moves_4h), 4) if moves_4h else None,
        'best_4h': round(max(moves_4h), 4) if moves_4h else None,
    }

    _log(f"summary: {len(events)} events, avg_4h={summary.get('avg_move_4h')}")
    return summary
