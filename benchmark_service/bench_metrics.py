"""
bench_metrics.py — Pure calculation functions for benchmark comparison.

No DB or network access. All functions take data lists and return metrics.

Metrics computed:
  - Total Return %
  - Cumulative PnL
  - Win Rate
  - Profit Factor
  - Max Drawdown %
  - Avg Hold Time
  - EV per Trade
  - Fee Ratio %
  - Long/Short Ratio
"""
from collections import deque
from datetime import datetime, timezone


def compute_metrics(executions, equity_series):
    """Compute all metrics from execution list and equity time series.

    Args:
        executions: list of dicts with keys: ts, side, qty, price, fee
        equity_series: list of dicts with keys: ts, equity

    Returns: dict of all metrics
    """
    trades = _pair_trades(executions)
    metrics = {}

    # Total Return % from equity series
    if equity_series and len(equity_series) >= 2:
        start_eq = float(equity_series[0]['equity'])
        end_eq = float(equity_series[-1]['equity'])
        if start_eq > 0:
            metrics['total_return_pct'] = round((end_eq - start_eq) / start_eq * 100, 2)
        else:
            metrics['total_return_pct'] = 0.0
        metrics['start_equity'] = round(start_eq, 2)
        metrics['end_equity'] = round(end_eq, 2)
    elif trades:
        # Fallback: estimate return from cumulative trade PnL
        cumulative_pnl = sum(t['pnl'] for t in trades)
        # Estimate starting capital from first trade notional (conservative)
        first_notional = trades[0]['qty'] * trades[0]['open_price']
        est_capital = max(first_notional * 10, 1000.0)  # assume ~10x notional
        metrics['total_return_pct'] = round(cumulative_pnl / est_capital * 100, 2)
        metrics['start_equity'] = round(est_capital, 2)
        metrics['end_equity'] = round(est_capital + cumulative_pnl, 2)
    else:
        metrics['total_return_pct'] = 0.0
        metrics['start_equity'] = 0.0
        metrics['end_equity'] = 0.0

    # Trade-based metrics
    metrics['trade_count'] = len(trades)
    metrics['cumulative_pnl'] = round(sum(t['pnl'] for t in trades), 4) if trades else 0.0
    metrics['win_rate'] = _compute_win_rate(trades)
    metrics['profit_factor'] = _compute_profit_factor(trades)
    metrics['max_drawdown_pct'] = _compute_max_drawdown(equity_series)
    metrics['avg_hold_time_min'] = _compute_avg_hold_time(trades)
    metrics['ev_per_trade'] = _compute_ev(trades)
    metrics['fee_ratio_pct'] = _compute_fee_ratio(executions)
    metrics['long_short_ratio'] = _compute_long_short_ratio(trades)

    # Raw trade counts
    winning = sum(1 for t in trades if t['pnl'] > 0)
    losing = sum(1 for t in trades if t['pnl'] <= 0)
    metrics['winning_trades'] = winning
    metrics['losing_trades'] = losing

    return metrics


def _pair_trades(executions):
    """FIFO round-trip trade matching. Handles partial fills.

    Returns list of completed trades with:
        side, open_ts, close_ts, open_price, close_price, qty, pnl, fees
    """
    if not executions:
        return []

    sorted_execs = sorted(executions, key=lambda x: x['ts'])
    trades = []
    open_positions = deque()  # FIFO queue: (side, qty, price, ts, fee)

    for ex in sorted_execs:
        side = ex['side'].upper()
        qty = abs(float(ex['qty']))
        price = float(ex['price'])
        fee = float(ex.get('fee', 0))
        ts = ex['ts']

        if not open_positions or open_positions[0][0] == side:
            open_positions.append((side, qty, price, ts, fee))
        else:
            remaining = qty
            close_fees = fee
            while remaining > 0 and open_positions:
                op_side, op_qty, op_price, op_ts, op_fee = open_positions[0]
                fill_qty = min(remaining, op_qty)

                if op_side in ('BUY', 'LONG'):
                    pnl = fill_qty * (price - op_price)
                else:
                    pnl = fill_qty * (op_price - price)

                entry_fee = op_fee * (fill_qty / op_qty) if op_qty > 0 else 0
                exit_fee = close_fees * (fill_qty / qty) if qty > 0 else 0
                total_fee = entry_fee + exit_fee
                pnl -= total_fee

                trades.append({
                    'side': op_side,
                    'open_ts': op_ts,
                    'close_ts': ts,
                    'open_price': op_price,
                    'close_price': price,
                    'qty': fill_qty,
                    'pnl': pnl,
                    'fees': total_fee,
                })

                remaining -= fill_qty
                leftover = op_qty - fill_qty
                if leftover > 0:
                    open_positions[0] = (op_side, leftover, op_price, op_ts,
                                         op_fee * (leftover / op_qty))
                else:
                    open_positions.popleft()

            if remaining > 0:
                open_positions.append((side, remaining, price, ts,
                                       close_fees * (remaining / qty) if qty > 0 else 0))

    return trades


def _compute_win_rate(trades):
    """Win rate = winning_trades / total_trades."""
    if not trades:
        return 0.0
    winning = sum(1 for t in trades if t['pnl'] > 0)
    return round(winning / len(trades) * 100, 1)


def _compute_profit_factor(trades):
    """Profit factor = gross_profit / gross_loss."""
    if not trades:
        return 0.0
    gross_profit = sum(t['pnl'] for t in trades if t['pnl'] > 0)
    gross_loss = abs(sum(t['pnl'] for t in trades if t['pnl'] < 0))
    if gross_loss == 0:
        return float('inf') if gross_profit > 0 else 0.0
    return round(gross_profit / gross_loss, 2)


def _compute_max_drawdown(equity_series):
    """Max drawdown % from equity time series (peak-to-trough)."""
    if not equity_series or len(equity_series) < 2:
        return 0.0
    peak = float(equity_series[0]['equity'])
    max_dd = 0.0
    for point in equity_series:
        eq = float(point['equity'])
        if eq > peak:
            peak = eq
        if peak > 0:
            dd = (peak - eq) / peak * 100
            if dd > max_dd:
                max_dd = dd
    return round(max_dd, 2)


def _compute_avg_hold_time(trades):
    """Average hold time in minutes."""
    if not trades:
        return 0.0
    total_minutes = 0
    count = 0
    for t in trades:
        open_ts = t['open_ts']
        close_ts = t['close_ts']
        if isinstance(open_ts, str):
            open_ts = datetime.fromisoformat(open_ts.replace('Z', '+00:00'))
        if isinstance(close_ts, str):
            close_ts = datetime.fromisoformat(close_ts.replace('Z', '+00:00'))
        if hasattr(open_ts, 'timestamp') and hasattr(close_ts, 'timestamp'):
            delta = (close_ts - open_ts).total_seconds() / 60
            total_minutes += delta
            count += 1
    return round(total_minutes / count, 1) if count > 0 else 0.0


def _compute_ev(trades):
    """Expected value per trade = total_pnl / trade_count."""
    if not trades:
        return 0.0
    total_pnl = sum(t['pnl'] for t in trades)
    return round(total_pnl / len(trades), 4)


def _compute_fee_ratio(executions):
    """Fee ratio % = total_fees / total_volume * 100."""
    if not executions:
        return 0.0
    total_fees = sum(abs(float(e.get('fee', 0))) for e in executions)
    total_volume = sum(abs(float(e['qty']) * float(e['price'])) for e in executions)
    if total_volume == 0:
        return 0.0
    return round(total_fees / total_volume * 100, 4)


def _compute_long_short_ratio(trades):
    """Long/Short ratio = long_count / short_count."""
    if not trades:
        return 0.0
    longs = sum(1 for t in trades if t['side'] in ('BUY', 'LONG'))
    shorts = sum(1 for t in trades if t['side'] in ('SELL', 'SHORT'))
    if shorts == 0:
        return float('inf') if longs > 0 else 0.0
    return round(longs / shorts, 2)


def format_comparison_report(our_metrics, bench_metrics, our_label, bench_label, period):
    """Format side-by-side comparison report as text.

    Returns markdown-formatted text suitable for Telegram.
    """
    lines = [
        f'Benchmark Comparison Report ({period})',
        '=' * 40,
        '',
        f'{"Metric":<22} {"Ours":>12} {"Bench":>12} {"Delta":>10}',
        '-' * 58,
    ]

    comparisons = [
        ('Total Return %', 'total_return_pct', '{:+.2f}'),
        ('Cumulative PnL', 'cumulative_pnl', '{:+.4f}'),
        ('Win Rate %', 'win_rate', '{:.1f}'),
        ('Profit Factor', 'profit_factor', '{:.2f}'),
        ('Max Drawdown %', 'max_drawdown_pct', '{:.2f}'),
        ('Avg Hold (min)', 'avg_hold_time_min', '{:.1f}'),
        ('EV/Trade', 'ev_per_trade', '{:+.4f}'),
        ('Fee Ratio %', 'fee_ratio_pct', '{:.4f}'),
        ('L/S Ratio', 'long_short_ratio', '{:.2f}'),
        ('Trade Count', 'trade_count', '{:d}'),
    ]

    for label, key, fmt in comparisons:
        ours = our_metrics.get(key, 0)
        bench = bench_metrics.get(key, 0)
        if isinstance(ours, float) and ours == float('inf'):
            ours_str = 'inf'
        else:
            ours_str = fmt.format(ours) if not isinstance(ours, float) or key == 'trade_count' else fmt.format(ours)
        if isinstance(bench, float) and bench == float('inf'):
            bench_str = 'inf'
        else:
            bench_str = fmt.format(bench) if not isinstance(bench, float) or key == 'trade_count' else fmt.format(bench)

        # Delta
        try:
            if isinstance(ours, (int, float)) and isinstance(bench, (int, float)) and \
               ours != float('inf') and bench != float('inf'):
                delta = ours - bench
                delta_str = f'{delta:+.2f}'
            else:
                delta_str = 'N/A'
        except Exception:
            delta_str = 'N/A'

        lines.append(f'{label:<22} {ours_str:>12} {bench_str:>12} {delta_str:>10}')

    lines.append('')
    lines.append(f'{our_label}: trades={our_metrics.get("trade_count", 0)} '
                 f'({our_metrics.get("winning_trades", 0)}W/{our_metrics.get("losing_trades", 0)}L)')
    lines.append(f'{bench_label}: trades={bench_metrics.get("trade_count", 0)} '
                 f'({bench_metrics.get("winning_trades", 0)}W/{bench_metrics.get("losing_trades", 0)}L)')

    return '\n'.join(lines)
