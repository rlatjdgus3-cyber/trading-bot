"""
bench_proposal_engine.py — Generate parameter adjustment proposals.

Reads latest report metrics, compares ours vs benchmark, applies rules,
and saves DRAFT proposals. NEVER auto-applies.
"""
import os
import sys
import json
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db_config_bench import get_bench_conn, get_main_conn_ro
from bench_utils import _log


def generate_proposal(report_id=None):
    """Generate a parameter change proposal based on benchmark comparison.

    Rules:
      1. If bench win_rate > ours by 10%+: raise add_score_threshold (+5, max 70)
      2. If our MDD > bench MDD by 20%+: tighten stop_loss_pct (-0.2, min 1.0)
      3. If bench EV > ours with fewer trades: reduce trade_budget_pct (-5, min 40)

    Returns (proposal_id, proposal_text) or (None, error_text).
    """
    bench_conn = None
    main_conn = None
    try:
        bench_conn = get_bench_conn()

        # Load report
        with bench_conn.cursor() as cur:
            if report_id:
                cur.execute("""
                    SELECT id, payload_json FROM bench_reports WHERE id = %s;
                """, (report_id,))
            else:
                cur.execute("""
                    SELECT id, payload_json FROM bench_reports
                    ORDER BY created_at DESC LIMIT 1;
                """)
            row = cur.fetchone()
            if not row:
                return None, 'No reports found.'
            report_id = row[0]
            payload = row[1] if isinstance(row[1], dict) else json.loads(row[1])

        # Find our metrics and bench metrics
        our_metrics = None
        bench_metrics = None
        for src_id, metrics in payload.items():
            if isinstance(metrics, dict):
                if metrics.get('source_kind') == 'OUR_STRATEGY':
                    our_metrics = metrics
                elif metrics.get('source_kind') in ('BYBIT_ACCOUNT', 'EXTERNAL'):
                    bench_metrics = metrics

        if not our_metrics:
            return None, 'No OUR_STRATEGY metrics in report.'
        if not bench_metrics:
            return None, 'No benchmark metrics in report for comparison.'

        # Read current settings from main DB
        main_conn = get_main_conn_ro()
        with main_conn.cursor() as mcur:
            mcur.execute("""
                SELECT add_score_threshold, stop_loss_pct, trade_budget_pct
                FROM safety_limits ORDER BY id DESC LIMIT 1;
            """)
            sl_row = mcur.fetchone()
            current = {
                'add_score_threshold': int(sl_row[0]) if sl_row else 45,
                'stop_loss_pct': float(sl_row[1]) if sl_row else 2.0,
                'trade_budget_pct': float(sl_row[2]) if sl_row else 70,
            }

        # Apply rules
        changes = {}
        reasons = []

        # Rule 1: Win rate comparison
        our_wr = our_metrics.get('win_rate', 0)
        bench_wr = bench_metrics.get('win_rate', 0)
        if bench_wr > our_wr + 10:
            new_threshold = min(current['add_score_threshold'] + 5, 70)
            if new_threshold != current['add_score_threshold']:
                changes['add_score_threshold'] = new_threshold
                reasons.append(
                    f'Bench win rate ({bench_wr:.1f}%) > ours ({our_wr:.1f}%) by 10%+: '
                    f'raise add_score_threshold {current["add_score_threshold"]} → {new_threshold}')

        # Rule 2: Max drawdown comparison
        our_mdd = our_metrics.get('max_drawdown_pct', 0)
        bench_mdd = bench_metrics.get('max_drawdown_pct', 0)
        if our_mdd > bench_mdd + 20:
            new_sl = max(round(current['stop_loss_pct'] - 0.2, 1), 1.0)
            if new_sl != current['stop_loss_pct']:
                changes['stop_loss_pct'] = new_sl
                reasons.append(
                    f'Our MDD ({our_mdd:.1f}%) > bench ({bench_mdd:.1f}%) by 20%+: '
                    f'tighten stop_loss_pct {current["stop_loss_pct"]} → {new_sl}')

        # Rule 3: EV with fewer trades
        our_ev = our_metrics.get('ev_per_trade', 0)
        bench_ev = bench_metrics.get('ev_per_trade', 0)
        our_trades = our_metrics.get('trade_count', 0)
        bench_trades = bench_metrics.get('trade_count', 0)
        if bench_ev > our_ev and bench_trades < our_trades:
            new_budget = max(current['trade_budget_pct'] - 5, 40)
            if new_budget != current['trade_budget_pct']:
                changes['trade_budget_pct'] = new_budget
                reasons.append(
                    f'Bench EV ({bench_ev:+.4f}) > ours ({our_ev:+.4f}) with fewer trades '
                    f'({bench_trades} vs {our_trades}): '
                    f'reduce trade_budget_pct {current["trade_budget_pct"]} → {new_budget}')

        if not changes:
            return None, 'No parameter changes recommended based on current comparison.'

        # Save proposal
        proposed = {
            'current': current,
            'proposed': changes,
            'reasons': reasons,
            'report_metrics': {
                'our_win_rate': our_wr,
                'bench_win_rate': bench_wr,
                'our_mdd': our_mdd,
                'bench_mdd': bench_mdd,
                'our_ev': our_ev,
                'bench_ev': bench_ev,
            }
        }

        with bench_conn.cursor() as cur:
            cur.execute("""
                INSERT INTO bench_proposals
                    (based_on_report_id, proposed_changes, status)
                VALUES (%s, %s, 'DRAFT')
                RETURNING id;
            """, (report_id, json.dumps(proposed, default=str)))
            proposal_id = cur.fetchone()[0]
        bench_conn.commit()

        # Format response
        lines = [
            f'Proposal #{proposal_id} (DRAFT)',
            '━━━━━━━━━━━━━━━━━━',
            f'Based on report #{report_id}',
            '',
        ]
        for reason in reasons:
            lines.append(f'  - {reason}')
        lines.append('')
        lines.append('Changes:')
        for key, val in changes.items():
            lines.append(f'  {key}: {current[key]} → {val}')
        lines.append('')
        lines.append('Use /apply_proposal <id> to review, then /apply_confirm <id> to apply.')

        _log(f'proposal generated: id={proposal_id}')
        return proposal_id, '\n'.join(lines)

    except Exception as e:
        if bench_conn:
            bench_conn.rollback()
        _log(f'generate_proposal error: {e}')
        import traceback
        traceback.print_exc()
        return None, f'Proposal generation failed: {e}'
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


if __name__ == '__main__':
    from bench_utils import load_env
    load_env()
    rid = int(sys.argv[1]) if len(sys.argv) > 1 else None
    pid, text = generate_proposal(rid)
    print(text)
