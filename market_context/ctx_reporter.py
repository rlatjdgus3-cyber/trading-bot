#!/usr/bin/env python3
"""
ctx_reporter.py — Regime-based performance analysis reporter.

Joins execution_log with market_context to analyze mode-specific performance.
Outputs recommendations (never auto-applies).
Run via systemd timer daily at 06:00 UTC.
"""
import os
import sys
import json
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db_config_ctx import get_main_conn_ro
from ctx_utils import _log, send_telegram, load_env


def generate_report(days=7):
    """Generate regime-mode performance report.

    - execution_log + market_context JOIN for per-mode trade stats
    - RANGE/BREAKOUT/SHOCK: trades, winrate, avg_pnl, max_dd
    - flow_bias bucket analysis (<-50, -50~0, 0~50, >50)
    - Top 3 policy change candidates (recommendations only)

    Returns: report text (str).
    """
    conn = None
    try:
        conn = get_main_conn_ro()
        with conn.cursor() as cur:
            # Check if both tables exist
            cur.execute("""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name IN ('market_context', 'execution_log');
            """)
            if cur.fetchone()[0] < 2:
                return 'Tables not ready (market_context or execution_log missing)'

            # Mode-specific performance
            cur.execute("""
                SELECT
                    mc.regime,
                    COUNT(el.id) AS trades,
                    COUNT(CASE WHEN el.realized_pnl > 0 THEN 1 END) AS wins,
                    ROUND(AVG(el.realized_pnl)::numeric, 2) AS avg_pnl,
                    ROUND(MIN(el.realized_pnl)::numeric, 2) AS max_dd,
                    ROUND(SUM(el.realized_pnl)::numeric, 2) AS total_pnl
                FROM execution_log el
                JOIN LATERAL (
                    SELECT regime, flow_bias FROM market_context
                    WHERE symbol = el.symbol AND ts <= el.ts
                    ORDER BY ts DESC LIMIT 1
                ) mc ON true
                WHERE el.ts >= now() - interval '%s days'
                  AND el.status IN ('FILLED', 'VERIFIED')
                  AND el.realized_pnl IS NOT NULL
                GROUP BY mc.regime
                ORDER BY mc.regime;
            """ % int(days))
            regime_rows = cur.fetchall()

            # Flow bias bucket analysis
            cur.execute("""
                SELECT
                    CASE
                        WHEN mc.flow_bias < -50 THEN 'strong_bear'
                        WHEN mc.flow_bias < 0 THEN 'mild_bear'
                        WHEN mc.flow_bias < 50 THEN 'mild_bull'
                        ELSE 'strong_bull'
                    END AS flow_bucket,
                    COUNT(el.id) AS trades,
                    ROUND(AVG(el.realized_pnl)::numeric, 2) AS avg_pnl,
                    COUNT(CASE WHEN el.realized_pnl > 0 THEN 1 END) AS wins
                FROM execution_log el
                JOIN LATERAL (
                    SELECT flow_bias FROM market_context
                    WHERE symbol = el.symbol AND ts <= el.ts
                    ORDER BY ts DESC LIMIT 1
                ) mc ON true
                WHERE el.ts >= now() - interval '%s days'
                  AND el.status IN ('FILLED', 'VERIFIED')
                  AND el.realized_pnl IS NOT NULL
                GROUP BY flow_bucket
                ORDER BY flow_bucket;
            """ % int(days))
            flow_rows = cur.fetchall()

            # Regime distribution
            cur.execute("""
                SELECT regime, COUNT(*) as cnt,
                       ROUND(AVG(regime_confidence)::numeric, 1) as avg_conf
                FROM market_context
                WHERE ts >= now() - interval '%s days'
                GROUP BY regime ORDER BY cnt DESC;
            """ % int(days))
            dist_rows = cur.fetchall()

        # Format report
        lines = [
            f'=== Market Context Report ({days}d) ===',
            f'Generated: {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}',
            '',
        ]

        # Regime distribution
        lines.append('-- Regime Distribution --')
        for r in dist_rows:
            lines.append(f'  {r[0]}: {r[1]} samples (avg conf={r[2]})')
        lines.append('')

        # Per-mode performance
        lines.append('-- Mode Performance --')
        if regime_rows:
            for r in regime_rows:
                regime, trades, wins, avg_pnl, max_dd, total_pnl = r
                wr = round(wins / trades * 100, 1) if trades > 0 else 0
                lines.append(
                    f'  {regime}: {trades} trades, WR={wr}%, '
                    f'avg_pnl={avg_pnl}, total={total_pnl}, max_dd={max_dd}')
        else:
            lines.append('  No trade data with regime context yet')
        lines.append('')

        # Flow bias buckets
        lines.append('-- Flow Bias Buckets --')
        if flow_rows:
            for r in flow_rows:
                bucket, trades, avg_pnl, wins = r
                wr = round(wins / trades * 100, 1) if trades > 0 else 0
                lines.append(f'  {bucket}: {trades} trades, WR={wr}%, avg_pnl={avg_pnl}')
        else:
            lines.append('  No data')
        lines.append('')

        # Recommendations
        lines.append('-- Recommendations --')
        recs = _generate_recommendations(regime_rows, flow_rows)
        for i, rec in enumerate(recs[:3], 1):
            lines.append(f'  {i}. {rec}')
        if not recs:
            lines.append('  Insufficient data for recommendations')

        report = '\n'.join(lines)
        return report

    except Exception as e:
        _log(f'report generation error: {e}')
        import traceback
        traceback.print_exc()
        return f'Report error: {e}'
    finally:
        if conn:
            conn.close()


def _generate_recommendations(regime_rows, flow_rows):
    """Generate top 3 policy change candidates based on performance data."""
    recs = []

    if not regime_rows:
        return ['Collect more data before making recommendations']

    regime_map = {}
    for r in regime_rows:
        regime, trades, wins, avg_pnl, max_dd, total_pnl = r
        wr = wins / trades * 100 if trades > 0 else 0
        regime_map[regime] = {
            'trades': trades, 'wr': wr, 'avg_pnl': float(avg_pnl or 0),
            'total_pnl': float(total_pnl or 0), 'max_dd': float(max_dd or 0)}

    # Check RANGE performance
    rng = regime_map.get('RANGE', {})
    if rng.get('trades', 0) >= 5 and rng.get('wr', 50) < 40:
        recs.append(f'RANGE WR={rng["wr"]:.0f}% is poor — consider reducing stage_limit from 3 to 2')
    if rng.get('trades', 0) >= 5 and rng.get('avg_pnl', 0) > 0:
        recs.append(f'RANGE avg_pnl=${rng["avg_pnl"]:.2f} is positive — current limits working well')

    # Check BREAKOUT performance
    bo = regime_map.get('BREAKOUT', {})
    if bo.get('trades', 0) >= 5 and bo.get('wr', 50) > 60:
        recs.append(f'BREAKOUT WR={bo["wr"]:.0f}% — consider increasing confidence threshold for more quality entries')

    # Check SHOCK performance
    shock = regime_map.get('SHOCK', {})
    if shock.get('trades', 0) >= 3 and shock.get('total_pnl', 0) < -10:
        recs.append(f'SHOCK total_pnl=${shock["total_pnl"]:.2f} — VETO threshold may need lowering (currently 3%)')

    # Flow bucket recommendations
    if flow_rows:
        for r in flow_rows:
            bucket, trades, avg_pnl, wins = r
            if trades >= 5 and float(avg_pnl or 0) < -5:
                recs.append(f'Flow bucket "{bucket}" has negative avg_pnl=${avg_pnl} — consider blocking entries')

    return recs


def main():
    """Run report and send via Telegram."""
    load_env()

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--days', type=int, default=7)
    args = parser.parse_args()

    report = generate_report(days=args.days)
    print(report)
    send_telegram(report, prefix='[MCTX REPORT]')


if __name__ == '__main__':
    main()
