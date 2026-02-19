#!/usr/bin/env python3
"""
bench_reporter.py — Oneshot report generation.

Run by timer daily at 06:00 UTC, or manually:
  python3 bench_reporter.py 7d
  python3 bench_reporter.py 30d
"""
import os
import sys
import json
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db_config_bench import get_bench_conn
from bench_utils import _log, send_telegram, load_env
import bench_metrics


SHORT_LABELS = {
    'Trend-Follow (EMA/VWAP)': 'Trend-Fol',
    'Mean-Reversion (BB/RSI)': 'MeanRev',
    'Volume/VP (POC/VAH/VAL)': 'Vol/VP',
    'Volatility/Regime (ATR/BBW)': 'Regime',
    'Bybit Benchmark': 'Bybit',
    'Our Trading Bot': 'Ours',
}

COMPARISON_ROWS = [
    ('Return %', 'total_return_pct', '.2f'),
    ('Win Rate %', 'win_rate', '.1f'),
    ('Max DD %', 'max_drawdown_pct', '.2f'),
    ('Trades', 'trade_count', 'd'),
    ('EV/Trade', 'ev_per_trade', '.4f'),
    ('Profit Fac', 'profit_factor', '.2f'),
    ('Fee Ratio %', 'fee_ratio_pct', '.4f'),
]


def _format_multi_strategy_table(our_metrics, our_label, bench_list, period):
    """Format N-column strategy comparison table."""
    # Column headers
    cols = [(our_label, our_metrics)] + bench_list
    headers = [SHORT_LABELS.get(lbl, lbl[:9]) for lbl, _ in cols]

    col_w = 10
    metric_w = 18
    header_line = f'{"Metric":<{metric_w}}'
    for h in headers:
        header_line += f' {h:>{col_w}}'

    sep = '\u2500' * (metric_w + (col_w + 1) * len(headers))
    lines = [
        f'Strategy Comparison ({period})',
        '=' * len(sep),
        header_line,
        sep,
    ]

    for row_label, key, fmt in COMPARISON_ROWS:
        line = f'{row_label:<{metric_w}}'
        for _, m in cols:
            val = m.get(key, 0)
            if isinstance(val, float) and val == float('inf'):
                val_str = 'inf'
            else:
                try:
                    val_str = f'{val:{fmt}}'
                except (ValueError, TypeError):
                    val_str = str(val)
            line += f' {val_str:>{col_w}}'
        lines.append(line)

    lines.append('')
    for lbl, m in cols:
        short = SHORT_LABELS.get(lbl, lbl[:9])
        w = m.get('winning_trades', 0)
        l = m.get('losing_trades', 0)
        lines.append(f'{short}: {m.get("trade_count", 0)} trades ({w}W/{l}L)')

    return '\n'.join(lines)


def generate_report(period_days):
    """Generate comparison report for given period.

    Returns (report_id, markdown_text) or (None, error_text).
    """
    period_label = f'{period_days}d'
    end_ts = datetime.now(timezone.utc)
    start_ts = end_ts - timedelta(days=period_days)

    conn = None
    try:
        conn = get_bench_conn()

        # Load sources
        with conn.cursor() as cur:
            cur.execute("SELECT id, kind, label FROM benchmark_sources WHERE enabled = true;")
            sources = cur.fetchall()

        if not sources:
            return None, 'No enabled benchmark sources found.'

        all_metrics = {}
        our_metrics = None
        our_label = 'Our Strategy'
        bench_metrics_list = []

        for src_id, kind, label in sources:
            # Fetch executions
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT ts, side, qty, price, fee
                    FROM bench_executions
                    WHERE source_id = %s AND ts >= %s AND ts <= %s
                    ORDER BY ts;
                """, (src_id, start_ts, end_ts))
                exec_rows = cur.fetchall()

            executions = [
                {'ts': r[0], 'side': r[1], 'qty': r[2], 'price': r[3], 'fee': r[4]}
                for r in exec_rows
            ]

            # Fetch equity series
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT ts, equity
                    FROM bench_equity_timeseries
                    WHERE source_id = %s AND ts >= %s AND ts <= %s
                    ORDER BY ts;
                """, (src_id, start_ts, end_ts))
                eq_rows = cur.fetchall()

            equity_series = [{'ts': r[0], 'equity': r[1]} for r in eq_rows]

            metrics = bench_metrics.compute_metrics(executions, equity_series)
            metrics['source_label'] = label
            metrics['source_kind'] = kind
            all_metrics[src_id] = metrics

            if kind == 'OUR_STRATEGY':
                our_metrics = metrics
                our_label = label
            else:
                bench_metrics_list.append((label, metrics))

        # Build report text
        report_parts = []
        report_parts.append(f'Benchmark Report \u2014 {period_label}')
        report_parts.append(f'Period: {start_ts.strftime("%Y-%m-%d")} ~ {end_ts.strftime("%Y-%m-%d")}')
        report_parts.append('')

        if our_metrics and bench_metrics_list:
            # Multi-strategy comparison table
            multi_table = _format_multi_strategy_table(
                our_metrics, our_label, bench_metrics_list, period_label)
            report_parts.append(multi_table)
            report_parts.append('')
        elif our_metrics:
            # No bench sources, show our metrics only
            report_parts.append(f'{our_label} Metrics ({period_label}):')
            for key, val in our_metrics.items():
                if key not in ('source_label', 'source_kind'):
                    report_parts.append(f'  {key}: {val}')
        else:
            report_parts.append('No OUR_STRATEGY data found for this period.')

        report_md = '\n'.join(report_parts)

        # Save to DB
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO bench_reports (period, start_ts, end_ts, payload_md, payload_json)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id;
            """, (period_label, start_ts, end_ts, report_md, json.dumps(all_metrics, default=str)))
            report_id = cur.fetchone()[0]
        conn.commit()

        _log(f'report generated: id={report_id}, period={period_label}')
        return report_id, report_md

    except Exception as e:
        if conn:
            conn.rollback()
        _log(f'generate_report error: {e}')
        import traceback
        traceback.print_exc()
        return None, f'Report generation failed: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def main():
    load_env()

    # Parse CLI args
    period = '7d'
    if len(sys.argv) > 1:
        period = sys.argv[1].lower()

    if period in ('7d', '7'):
        days = 7
    elif period in ('30d', '30'):
        days = 30
    else:
        try:
            days = int(period.replace('d', ''))
        except ValueError:
            _log(f'invalid period: {period}, using 7d')
            days = 7

    _log(f'generating {days}d report...')
    report_id, report_md = generate_report(days)

    if report_id:
        send_telegram(report_md, prefix='')
        _log(f'report {report_id} sent to telegram')
    else:
        _log(f'report failed: {report_md}')
        send_telegram(f'Report generation failed: {report_md}', prefix='[BENCH ERROR]')

    # If no CLI args (timer run), generate both 7d and 30d
    if len(sys.argv) <= 1:
        _log('generating 30d report...')
        report_id_30, report_md_30 = generate_report(30)
        if report_id_30:
            send_telegram(report_md_30, prefix='')
            _log(f'report {report_id_30} sent to telegram')


if __name__ == '__main__':
    main()
