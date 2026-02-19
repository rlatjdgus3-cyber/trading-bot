#!/usr/bin/env python3
"""
bench_telegram.py — CLI-invokable command handler for benchmark service.

Called from main bot's telegram_cmd_poller.py via subprocess:
  python3 bench_telegram.py --handle "/bench status"

Commands:
  /bench status      — collector health, last collection times, 24h exec counts
  /bench report7d    — generate/show 7d report
  /bench report30d   — generate/show 30d report
  /bench propose     — generate proposal from latest report
  /bench proposals   — list recent proposals
  /apply_proposal <id>  — show diff (DRAFT→PENDING)
  /apply_confirm <id>   — THE ONLY WRITE TO MAIN DB (PENDING→APPLIED)
"""
import os
import sys
import json
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db_config_bench import get_bench_conn, get_main_conn_ro, get_main_conn_rw
from bench_utils import _log, load_env


def handle_command(text):
    """Route command and return response string."""
    text = (text or '').strip()

    if text.startswith('/bench'):
        parts = text.split(None, 2)
        sub = parts[1].lower() if len(parts) > 1 else 'status'

        if sub == 'status':
            return _cmd_status()
        elif sub in ('report7d', 'report_7d', '7d'):
            return _cmd_report(7)
        elif sub in ('report30d', 'report_30d', '30d'):
            return _cmd_report(30)
        elif sub == 'snapshot':
            return _cmd_snapshot()
        elif sub == 'propose':
            return _cmd_propose()
        elif sub == 'proposals':
            return _cmd_proposals()
        else:
            return (f'Unknown /bench subcommand: {sub}\n'
                    f'Available: status, snapshot, report7d, report30d, propose, proposals')

    elif text.startswith('/apply_proposal'):
        parts = text.split()
        if len(parts) < 2:
            return 'Usage: /apply_proposal <id>'
        try:
            proposal_id = int(parts[1])
        except ValueError:
            return 'Invalid proposal ID. Usage: /apply_proposal <id>'
        return _cmd_apply_proposal(proposal_id)

    elif text.startswith('/apply_confirm'):
        parts = text.split()
        if len(parts) < 2:
            return 'Usage: /apply_confirm <id>'
        try:
            proposal_id = int(parts[1])
        except ValueError:
            return 'Invalid proposal ID. Usage: /apply_confirm <id>'
        return _cmd_apply_confirm(proposal_id)

    else:
        return f'Unknown benchmark command: {text}'


def _cmd_status():
    """Collector health, last collection times, 24h exec counts."""
    conn = None
    try:
        conn = get_bench_conn(autocommit=True)
        lines = ['[BENCH] Collector Status', '━━━━━━━━━━━━━━━━━━']

        # Sources and their states
        with conn.cursor() as cur:
            cur.execute("""
                SELECT s.id, s.kind, s.label, s.enabled,
                       cs.last_exec_ts, cs.last_position_ts, cs.last_equity_ts
                FROM benchmark_sources s
                LEFT JOIN bench_collector_state cs ON cs.source_id = s.id
                ORDER BY s.id;
            """)
            rows = cur.fetchall()

        for row in rows:
            src_id, kind, label, enabled, last_exec, last_pos, last_eq = row
            status = 'ON' if enabled else 'OFF'
            lines.append(f'\n{label} ({kind}) [{status}]')
            if last_exec:
                age = datetime.now(timezone.utc) - last_exec.replace(tzinfo=timezone.utc) \
                    if last_exec.tzinfo is None else datetime.now(timezone.utc) - last_exec
                lines.append(f'  Last exec: {last_exec.strftime("%m-%d %H:%M")} ({age.seconds // 60}m ago)')
            else:
                lines.append('  Last exec: never')
            if last_pos:
                lines.append(f'  Last pos: {last_pos.strftime("%m-%d %H:%M")}')
            if last_eq:
                lines.append(f'  Last equity: {last_eq.strftime("%m-%d %H:%M")}')

        # 24h execution counts
        with conn.cursor() as cur:
            cur.execute("""
                SELECT s.label, count(e.id)
                FROM benchmark_sources s
                LEFT JOIN bench_executions e ON e.source_id = s.id
                    AND e.ts >= now() - interval '24 hours'
                GROUP BY s.label ORDER BY s.label;
            """)
            counts = cur.fetchall()

        lines.append('\n24h Execution Counts:')
        for label, cnt in counts:
            lines.append(f'  {label}: {cnt}')

        # Last market snapshot
        with conn.cursor() as cur:
            cur.execute("""
                SELECT price, funding_rate, ts
                FROM bench_market_snapshots ORDER BY ts DESC LIMIT 1;
            """)
            mkt = cur.fetchone()
        if mkt:
            lines.append(f'\nMarket: ${mkt[0]:,.2f} | FR={mkt[1] or "N/A"} | {mkt[2].strftime("%H:%M")}')

        return '\n'.join(lines)
    except Exception as e:
        return f'Status error: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _cmd_snapshot():
    """Show current strategy signals + virtual positions."""
    conn = None
    try:
        conn = get_bench_conn(autocommit=True)
        lines = ['[BENCH] Strategy Snapshot', '━━━━━━━━━━━━━━━━━━']

        # Market data
        with conn.cursor() as cur:
            cur.execute("""
                SELECT price, funding_rate, ts
                FROM bench_market_snapshots ORDER BY ts DESC LIMIT 1;
            """)
            mkt = cur.fetchone()
        if mkt:
            fr_str = f'{float(mkt[1]) * 100:.4f}%' if mkt[1] else 'N/A'
            lines.append(
                f'BTC: ${float(mkt[0]):,.2f}  FR: {fr_str}'
                f'  @ {mkt[2].strftime("%H:%M")} UTC')
        lines.append('')

        # Strategy labels
        strategy_labels = {
            'trend_follow': 'Trend-Follow (EMA/VWAP)',
            'mean_reversion': 'Mean-Reversion (BB/RSI)',
            'volume_vp': 'Volume/VP (POC/VAH/VAL)',
            'volatility_regime': 'Volatility/Regime (ATR/BBW)',
        }

        # Latest signal per strategy
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (strategy_name)
                    strategy_name, signal, confidence, rationale, ts
                FROM bench_strategy_signals
                ORDER BY strategy_name, ts DESC;
            """)
            signals = cur.fetchall()

        now = datetime.now(timezone.utc)
        if signals:
            for sname, signal, conf, rationale, ts in signals:
                label = strategy_labels.get(sname, sname)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                age = now - ts
                age_str = f'{age.seconds // 60}m ago' if age.seconds < 3600 else f'{age.seconds // 3600}h ago'
                lines.append(f'{label}')
                lines.append(f'  {signal} ({conf}%) \u2014 {rationale}')
                lines.append(f'  Updated: {age_str}')
                lines.append('')
        else:
            lines.append('No strategy signals yet.')
            lines.append('')

        # Virtual positions
        with conn.cursor() as cur:
            cur.execute("""
                SELECT strategy_name, side, size, entry_price
                FROM bench_virtual_positions
                WHERE side IS NOT NULL AND size > 0;
            """)
            positions = cur.fetchall()

        if positions:
            lines.append('Active Virtual Positions:')
            for sname, side, size, entry in positions:
                lines.append(
                    f'  {sname}: {side} {float(size):.6f} BTC @ ${float(entry):,.2f}')
        else:
            lines.append('Active Virtual Positions: none')

        return '\n'.join(lines)
    except Exception as e:
        return f'Snapshot error: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _cmd_report(days):
    """Generate and show report."""
    import bench_reporter
    report_id, report_md = bench_reporter.generate_report(days)
    if report_id:
        return report_md
    return f'Report generation failed: {report_md}'


def _cmd_propose():
    """Generate proposal from latest report."""
    import bench_proposal_engine
    pid, text = bench_proposal_engine.generate_proposal()
    return text


def _cmd_proposals():
    """List recent proposals."""
    conn = None
    try:
        conn = get_bench_conn(autocommit=True)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, created_at, based_on_report_id, status,
                       proposed_changes
                FROM bench_proposals
                ORDER BY id DESC LIMIT 10;
            """)
            rows = cur.fetchall()

        if not rows:
            return 'No proposals found.'

        lines = ['[BENCH] Recent Proposals', '━━━━━━━━━━━━━━━━━━']
        for row in rows:
            pid, created, report_id, status, changes = row
            changes_dict = changes if isinstance(changes, dict) else json.loads(changes)
            proposed = changes_dict.get('proposed', {})
            change_summary = ', '.join(f'{k}→{v}' for k, v in proposed.items())
            lines.append(
                f'#{pid} [{status}] report#{report_id} '
                f'{created.strftime("%m-%d %H:%M")}\n'
                f'  Changes: {change_summary or "none"}'
            )
        return '\n'.join(lines)
    except Exception as e:
        return f'Proposals list error: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _cmd_apply_proposal(proposal_id):
    """Show diff and set DRAFT → PENDING. Step 1 of 2."""
    bench_conn = None
    main_conn = None
    try:
        bench_conn = get_bench_conn()
        with bench_conn.cursor() as cur:
            cur.execute("""
                SELECT id, status, proposed_changes
                FROM bench_proposals WHERE id = %s;
            """, (proposal_id,))
            row = cur.fetchone()

        if not row:
            return f'Proposal #{proposal_id} not found.'

        pid, status, changes = row
        if status not in ('DRAFT',):
            return f'Proposal #{pid} is {status}, can only apply DRAFT proposals.'

        changes_dict = changes if isinstance(changes, dict) else json.loads(changes)
        current = changes_dict.get('current', {})
        proposed = changes_dict.get('proposed', {})
        reasons = changes_dict.get('reasons', [])

        # Read live values from main DB
        main_conn = get_main_conn_ro()
        with main_conn.cursor() as mcur:
            mcur.execute("""
                SELECT add_score_threshold, stop_loss_pct, trade_budget_pct
                FROM safety_limits ORDER BY id DESC LIMIT 1;
            """)
            live_row = mcur.fetchone()
            live = {
                'add_score_threshold': int(live_row[0]) if live_row else 45,
                'stop_loss_pct': float(live_row[1]) if live_row else 2.0,
                'trade_budget_pct': float(live_row[2]) if live_row else 70,
            }

        # Build diff
        lines = [
            f'Proposal #{pid} — Review',
            '━━━━━━━━━━━━━━━━━━',
            '',
            'Reasons:',
        ]
        for r in reasons:
            lines.append(f'  - {r}')
        lines.append('')
        lines.append('Before → After:')
        for key, new_val in proposed.items():
            old_val = live.get(key, current.get(key, '?'))
            lines.append(f'  {key}: {old_val} → {new_val}')
        lines.append('')
        lines.append(f'Status: DRAFT → PENDING')
        lines.append(f'To confirm: /apply_confirm {pid}')

        # Set status to PENDING
        with bench_conn.cursor() as cur:
            cur.execute("""
                UPDATE bench_proposals SET status = 'PENDING'
                WHERE id = %s AND status = 'DRAFT';
            """, (pid,))
        bench_conn.commit()

        return '\n'.join(lines)
    except Exception as e:
        if bench_conn:
            bench_conn.rollback()
        return f'Apply proposal error: {e}'
    finally:
        if bench_conn:
            try:
                bench_conn.close()
            except Exception:
                pass
        if main_conn:
            try:
                main_conn.close()
            except Exception:
                pass


def _cmd_apply_confirm(proposal_id):
    """THE ONLY WRITE TO MAIN DB. PENDING → APPLIED. Step 2 of 2."""
    bench_conn = None
    main_conn = None
    try:
        bench_conn = get_bench_conn()
        with bench_conn.cursor() as cur:
            cur.execute("""
                SELECT id, status, proposed_changes
                FROM bench_proposals WHERE id = %s;
            """, (proposal_id,))
            row = cur.fetchone()

        if not row:
            return f'Proposal #{proposal_id} not found.'

        pid, status, changes = row
        if status != 'PENDING':
            return f'Proposal #{pid} is {status}, can only confirm PENDING proposals.'

        changes_dict = changes if isinstance(changes, dict) else json.loads(changes)
        proposed = changes_dict.get('proposed', {})

        if not proposed:
            return f'Proposal #{pid} has no changes to apply.'

        # Write to main DB
        main_conn = get_main_conn_rw()
        with main_conn.cursor() as mcur:
            for key, val in proposed.items():
                if key == 'add_score_threshold':
                    mcur.execute("""
                        UPDATE safety_limits SET add_score_threshold = %s,
                                                  updated_at = now();
                    """, (val,))
                elif key == 'stop_loss_pct':
                    mcur.execute("""
                        UPDATE safety_limits SET stop_loss_pct = %s,
                                                  updated_at = now();
                    """, (val,))
                elif key == 'trade_budget_pct':
                    mcur.execute("""
                        UPDATE safety_limits SET trade_budget_pct = %s,
                                                  updated_at = now();
                    """, (val,))
        main_conn.commit()

        # Mark proposal as APPLIED
        with bench_conn.cursor() as cur:
            cur.execute("""
                UPDATE bench_proposals
                SET status = 'APPLIED', applied_at = now()
                WHERE id = %s;
            """, (pid,))
        bench_conn.commit()

        lines = [
            f'Proposal #{pid} APPLIED',
            '━━━━━━━━━━━━━━━━━━',
            'Changes written to main DB:',
        ]
        for key, val in proposed.items():
            lines.append(f'  {key} = {val}')
        lines.append('')
        lines.append('Main bot will pick up changes on next cycle.')

        return '\n'.join(lines)
    except Exception as e:
        if main_conn:
            main_conn.rollback()
        if bench_conn:
            bench_conn.rollback()
        return f'Apply confirm error: {e}'
    finally:
        if bench_conn:
            try:
                bench_conn.close()
            except Exception:
                pass
        if main_conn:
            try:
                main_conn.close()
            except Exception:
                pass


def main():
    load_env()
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--handle', type=str, required=True,
                        help='Command text to handle')
    args = parser.parse_args()
    result = handle_command(args.handle)
    print(result)


if __name__ == '__main__':
    main()
