"""
strategy_supervisor.py — 6-hour periodic strategy supervision service (P3).

Generates reports analyzing recent trading performance, identifies issues,
and proposes conservative config adjustments. All proposals require manual
approval via Telegram commands unless ff_auto_apply_conservative is enabled.

FAIL-OPEN: Errors here never affect trading. Reports are advisory only.

Feature flags:
  - ff_strategy_supervisor: Enable 6h reporting loop
  - ff_config_versioning: Enable config snapshot tracking
  - ff_auto_apply_conservative: Enable auto-apply of safe changes
"""

import os
import sys
import time
import json
import hashlib
import traceback

sys.path.insert(0, '/root/trading-bot/app')

LOG_PREFIX = '[supervisor]'
SYMBOL = os.getenv('SYMBOL', 'BTC/USDT:USDT')
REPORT_INTERVAL_SEC = 6 * 3600  # 6 hours
_last_report_ts = 0


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _db_conn():
    from db_config import get_conn
    return get_conn()


def _notify_telegram(text):
    try:
        import urllib.parse
        import urllib.request
        from dotenv import load_dotenv
        load_dotenv('/root/trading-bot/app/telegram_cmd.env')
        token = os.getenv('TELEGRAM_BOT_TOKEN')
        chat_id = os.getenv('TELEGRAM_ALLOWED_CHAT_ID')
        if not token or not chat_id:
            return
        url = f'https://api.telegram.org/bot{token}/sendMessage'
        data = urllib.parse.urlencode({
            'chat_id': chat_id,
            'text': text[:4000],
            'disable_web_page_preview': 'true'}).encode('utf-8')
        req = urllib.request.Request(url, data=data, method='POST')
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════
# REPORT GENERATION
# ══════════════════════════════════════════════════════════════

def generate_report(cur, hours=6):
    """Generate a 6-hour strategy supervision report.

    Returns: dict with report sections
    """
    report = {
        'period_hours': hours,
        'trades': [],
        'summary': {},
        'mode_breakdown': {},
        'worst_trades': [],
        'recommendations': [],
        'patch_proposals': [],
    }

    try:
        # 1. Recent trades summary
        cur.execute("""
            SELECT id, ts, action_type, direction, realized_pnl,
                   close_reason, entry_mode, regime_tag
            FROM execution_log
            WHERE ts >= now() - make_interval(hours => %s)
              AND realized_pnl IS NOT NULL
            ORDER BY ts DESC;
        """, (hours,))
        rows = cur.fetchall()

        trades = []
        for r in rows:
            trades.append({
                'id': r[0], 'ts': str(r[1]), 'action': r[2],
                'direction': r[3], 'pnl': float(r[4]) if r[4] else 0,
                'close_reason': r[5], 'entry_mode': r[6], 'regime': r[7],
            })
        report['trades'] = trades

        # 2. Summary stats
        pnls = [t['pnl'] for t in trades]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p < 0]
        total_pnl = sum(pnls)
        wr = len(wins) / len(pnls) * 100 if pnls else 0

        # Max drawdown (running)
        running = 0
        max_dd = 0
        for p in pnls:
            running += p
            if running < max_dd:
                max_dd = running

        report['summary'] = {
            'trades_count': len(trades),
            'win_rate': round(wr, 1),
            'total_pnl': round(total_pnl, 2),
            'avg_win': round(sum(wins) / len(wins), 2) if wins else 0,
            'avg_loss': round(sum(losses) / len(losses), 2) if losses else 0,
            'max_drawdown': round(max_dd, 2),
        }

        # 3. Mode breakdown
        mode_stats = {}
        for t in trades:
            mode = t.get('entry_mode', 'unknown') or 'unknown'
            if mode not in mode_stats:
                mode_stats[mode] = {'count': 0, 'wins': 0, 'pnl': 0}
            mode_stats[mode]['count'] += 1
            mode_stats[mode]['pnl'] += t['pnl']
            if t['pnl'] > 0:
                mode_stats[mode]['wins'] += 1
        for mode, stats in mode_stats.items():
            stats['wr'] = round(stats['wins'] / stats['count'] * 100, 1) if stats['count'] > 0 else 0
            stats['pnl'] = round(stats['pnl'], 2)
        report['mode_breakdown'] = mode_stats

        # 4. Worst 3 trades
        sorted_trades = sorted(trades, key=lambda t: t['pnl'])
        report['worst_trades'] = sorted_trades[:3]

        # 5. Generate recommendations
        recs = []
        if wr < 35 and len(trades) >= 5:
            recs.append({
                'type': 'WARNING',
                'msg': f'WR {wr:.0f}% < 35% over {len(trades)} trades — consider threshold increase',
            })
        if max_dd < -20:
            recs.append({
                'type': 'CRITICAL',
                'msg': f'Max DD {max_dd:.1f} USDT — consider reducing position size',
            })
        for mode, stats in mode_stats.items():
            if stats['count'] >= 3 and stats['wr'] < 30:
                recs.append({
                    'type': 'MODE_WARNING',
                    'msg': f'{mode}: WR {stats["wr"]:.0f}% over {stats["count"]} trades — '
                           f'consider cooldown or disable',
                })
        report['recommendations'] = recs

        # 6. Generate conservative patch proposals
        proposals = []
        if wr < 30 and len(trades) >= 5:
            proposals.append({
                'change_type': 'threshold_increase',
                'param': 'add_score_threshold',
                'current': 45,
                'proposed': 55,
                'reasoning': f'Low WR ({wr:.0f}%) → increase ADD threshold for selectivity',
            })
        for mode, stats in mode_stats.items():
            if stats['count'] >= 3 and stats['wr'] < 25:
                proposals.append({
                    'change_type': 'mode_cooldown',
                    'param': f'adaptive_l1_cooldown_5_sec_{mode}',
                    'current': 7200,
                    'proposed': 14400,
                    'reasoning': f'{mode} WR={stats["wr"]:.0f}% → double cooldown time',
                })
        report['patch_proposals'] = proposals

    except Exception as e:
        _log(f'generate_report error: {e}')
        traceback.print_exc()
        report['error'] = str(e)

    return report


def format_report(report):
    """Format report dict into telegram-friendly text."""
    lines = ['=== STRATEGY SUPERVISOR REPORT ===']

    s = report.get('summary', {})
    lines.append(f"\nPeriod: {report.get('period_hours', 6)}h")
    lines.append(f"Trades: {s.get('trades_count', 0)}")
    lines.append(f"WR: {s.get('win_rate', 0):.1f}%")
    lines.append(f"PnL: {s.get('total_pnl', 0):.2f} USDT")
    lines.append(f"Avg Win: {s.get('avg_win', 0):.2f} | Avg Loss: {s.get('avg_loss', 0):.2f}")
    lines.append(f"Max DD: {s.get('max_drawdown', 0):.2f}")

    # Mode breakdown
    modes = report.get('mode_breakdown', {})
    if modes:
        lines.append("\n--- Mode Breakdown ---")
        for mode, stats in modes.items():
            lines.append(f"  {mode}: {stats['count']} trades, "
                         f"WR={stats['wr']:.0f}%, PnL={stats['pnl']:.2f}")

    # Worst trades
    worst = report.get('worst_trades', [])
    if worst:
        lines.append("\n--- Worst Trades ---")
        for t in worst:
            lines.append(f"  {t['ts'][:16]} {t['direction']} "
                         f"PnL={t['pnl']:.2f} — {t.get('close_reason', '?')}")

    # Recommendations
    recs = report.get('recommendations', [])
    if recs:
        lines.append("\n--- Recommendations ---")
        for r in recs:
            lines.append(f"  [{r['type']}] {r['msg']}")

    # Proposals
    proposals = report.get('patch_proposals', [])
    if proposals:
        lines.append(f"\n--- Patch Proposals ({len(proposals)}) ---")
        for p in proposals:
            lines.append(f"  {p['change_type']}: {p['param']} "
                         f"{p.get('current')} → {p.get('proposed')}")
            lines.append(f"    Reason: {p.get('reasoning', '')}")

    return '\n'.join(lines)


def save_report(cur, report):
    """Save report to supervisor_reports table."""
    try:
        s = report.get('summary', {})
        cur.execute("""
            INSERT INTO supervisor_reports
                (report_type, trades_count, win_rate, total_pnl, max_drawdown,
                 mode_breakdown, worst_trades, recommendations, patch_proposals,
                 full_report)
            VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s);
        """, ('6h', s.get('trades_count', 0), s.get('win_rate', 0),
              s.get('total_pnl', 0), s.get('max_drawdown', 0),
              json.dumps(report.get('mode_breakdown', {}), default=str),
              json.dumps(report.get('worst_trades', []), default=str),
              json.dumps(report.get('recommendations', []), default=str),
              json.dumps(report.get('patch_proposals', []), default=str),
              format_report(report)))
    except Exception as e:
        _log(f'save_report error: {e}')


# ══════════════════════════════════════════════════════════════
# CONFIG VERSIONING
# ══════════════════════════════════════════════════════════════

def snapshot_config(cur, changed_by='system', change_reason=None):
    """Save current config snapshot for versioning (P3-2)."""
    try:
        import feature_flags
        if not feature_flags.is_enabled('ff_config_versioning'):
            return None

        from strategy_v3 import config_v3
        cfg = config_v3.get_all()
        cfg_json = json.dumps(cfg, sort_keys=True, default=str)
        cfg_hash = hashlib.md5(cfg_json.encode()).hexdigest()

        # Check if config changed since last snapshot
        cur.execute("""
            SELECT config_hash FROM strategy_config_versions
            ORDER BY ts DESC LIMIT 1;
        """)
        last = cur.fetchone()
        if last and last[0] == cfg_hash:
            return None  # No change

        cur.execute("""
            INSERT INTO strategy_config_versions
                (config_hash, config_snapshot, changed_by, change_reason)
            VALUES (%s, %s::jsonb, %s, %s)
            RETURNING id;
        """, (cfg_hash, cfg_json, changed_by, change_reason))
        row = cur.fetchone()
        _log(f'config snapshot saved: hash={cfg_hash[:12]} id={row[0] if row else "?"}')
        return row[0] if row else None
    except Exception as e:
        _log(f'snapshot_config error: {e}')
        return None


def create_proposal(cur, change_type, param, current_val, proposed_val,
                    reasoning, proposed_by='supervisor'):
    """Create a config change proposal (P3-2)."""
    try:
        cur.execute("""
            INSERT INTO strategy_change_proposals
                (proposed_by, change_type, proposal, reasoning)
            VALUES (%s, %s, %s::jsonb, %s)
            RETURNING id;
        """, (proposed_by, change_type,
              json.dumps({
                  'param': param,
                  'current': current_val,
                  'proposed': proposed_val,
              }, default=str), reasoning))
        row = cur.fetchone()
        return row[0] if row else None
    except Exception as e:
        _log(f'create_proposal error: {e}')
        return None


def apply_proposal(cur, proposal_id):
    """Apply a pending proposal (P3-3). Only conservative changes allowed.

    Conservative = threshold increase, cooldown increase, max_loss decrease,
                   entry size decrease.
    Forbidden = leverage increase, SL loosening, aggressive entry.
    """
    try:
        import feature_flags
        if not feature_flags.is_enabled('ff_auto_apply_conservative'):
            return (False, 'ff_auto_apply_conservative OFF')

        cur.execute("""
            SELECT proposal, change_type, status FROM strategy_change_proposals
            WHERE id = %s;
        """, (proposal_id,))
        row = cur.fetchone()
        if not row:
            return (False, 'proposal not found')
        if row[2] != 'PENDING':
            return (False, f'proposal status={row[2]}, not PENDING')

        proposal = row[0] if isinstance(row[0], dict) else json.loads(row[0])
        change_type = row[1]

        # Safety check: only allow conservative changes
        ALLOWED_TYPES = {'threshold_increase', 'cooldown_increase',
                         'max_loss_decrease', 'size_decrease', 'mode_cooldown'}
        if change_type not in ALLOWED_TYPES:
            return (False, f'change_type={change_type} not in allowed set')

        param = proposal.get('param')
        proposed = proposal.get('proposed')
        if not param or proposed is None:
            return (False, 'invalid proposal data')

        # Snapshot before
        snapshot_config(cur, changed_by='supervisor_apply',
                        change_reason=f'proposal_{proposal_id}')

        # Apply to YAML config
        import yaml
        config_path = os.path.join(os.path.dirname(__file__),
                                   'config', 'strategy_modes.yaml')
        with open(config_path, 'r') as f:
            full_cfg = yaml.safe_load(f) or {}

        # Find and update param in strategy_v3 section
        v3 = full_cfg.get('strategy_v3', {})
        if param in v3:
            v3[param] = proposed
        else:
            v3[param] = proposed
        full_cfg['strategy_v3'] = v3

        with open(config_path, 'w') as f:
            yaml.dump(full_cfg, f, default_flow_style=False, allow_unicode=True)

        # Force feature_flags reload
        try:
            import feature_flags as ff
            ff.reload()
            from strategy_v3 import config_v3
            config_v3._cache = None
        except Exception:
            pass

        # Mark proposal as applied
        cur.execute("""
            UPDATE strategy_change_proposals
            SET status = 'APPLIED', applied_at = now()
            WHERE id = %s;
        """, (proposal_id,))

        # Snapshot after
        snapshot_config(cur, changed_by='supervisor_apply',
                        change_reason=f'applied_proposal_{proposal_id}')

        _log(f'proposal {proposal_id} applied: {param}={proposed}')
        return (True, f'applied: {param}={proposed}')

    except Exception as e:
        _log(f'apply_proposal error: {e}')
        return (False, str(e))


def rollback_proposal(cur, proposal_id):
    """Rollback a previously applied proposal."""
    try:
        cur.execute("""
            SELECT proposal, status FROM strategy_change_proposals
            WHERE id = %s;
        """, (proposal_id,))
        row = cur.fetchone()
        if not row:
            return (False, 'proposal not found')
        if row[1] != 'APPLIED':
            return (False, f'proposal status={row[1]}, not APPLIED')

        proposal = row[0] if isinstance(row[0], dict) else json.loads(row[0])
        param = proposal.get('param')
        original = proposal.get('current')

        if not param or original is None:
            return (False, 'no original value to rollback')

        import yaml
        config_path = os.path.join(os.path.dirname(__file__),
                                   'config', 'strategy_modes.yaml')
        with open(config_path, 'r') as f:
            full_cfg = yaml.safe_load(f) or {}

        v3 = full_cfg.get('strategy_v3', {})
        v3[param] = original
        full_cfg['strategy_v3'] = v3

        with open(config_path, 'w') as f:
            yaml.dump(full_cfg, f, default_flow_style=False, allow_unicode=True)

        cur.execute("""
            UPDATE strategy_change_proposals
            SET status = 'ROLLED_BACK', rollback_at = now()
            WHERE id = %s;
        """, (proposal_id,))

        snapshot_config(cur, changed_by='supervisor_rollback',
                        change_reason=f'rollback_proposal_{proposal_id}')

        _log(f'proposal {proposal_id} rolled back: {param}={original}')
        return (True, f'rolled back: {param}={original}')

    except Exception as e:
        _log(f'rollback_proposal error: {e}')
        return (False, str(e))


# ══════════════════════════════════════════════════════════════
# TELEGRAM COMMAND HANDLERS
# ══════════════════════════════════════════════════════════════

def handle_supervisor_command(text):
    """Handle /supervisor subcommands.

    Subcommands:
      /supervisor report — latest or generate new report
      /supervisor propose — list pending proposals
      /supervisor apply <id> — apply a proposal
      /supervisor rollback <id> — rollback a proposal
      /supervisor diff <from_id> <to_id> — compare config versions
    """
    parts = text.strip().split()
    subcmd = parts[1] if len(parts) > 1 else 'report'

    conn = None
    try:
        conn = _db_conn()
        conn.autocommit = True
        with conn.cursor() as cur:
            if subcmd == 'report':
                report = generate_report(cur)
                save_report(cur, report)
                return format_report(report)

            elif subcmd == 'propose':
                cur.execute("""
                    SELECT id, ts, change_type, proposal, reasoning, status
                    FROM strategy_change_proposals
                    WHERE status = 'PENDING'
                    ORDER BY ts DESC LIMIT 10;
                """)
                rows = cur.fetchall()
                if not rows:
                    return 'No pending proposals.'
                lines = ['=== PENDING PROPOSALS ===']
                for r in rows:
                    proposal = r[3] if isinstance(r[3], dict) else json.loads(r[3])
                    lines.append(f"\n#{r[0]} [{r[2]}] {str(r[1])[:16]}")
                    lines.append(f"  {proposal.get('param')}: "
                                 f"{proposal.get('current')} → {proposal.get('proposed')}")
                    lines.append(f"  Reason: {r[4]}")
                return '\n'.join(lines)

            elif subcmd == 'apply' and len(parts) > 2:
                try:
                    pid = int(parts[2])
                except ValueError:
                    return 'Usage: /supervisor apply <id>'
                ok, msg = apply_proposal(cur, pid)
                return f'Apply #{pid}: {"SUCCESS" if ok else "FAILED"} — {msg}'

            elif subcmd == 'rollback' and len(parts) > 2:
                try:
                    pid = int(parts[2])
                except ValueError:
                    return 'Usage: /supervisor rollback <id>'
                ok, msg = rollback_proposal(cur, pid)
                return f'Rollback #{pid}: {"SUCCESS" if ok else "FAILED"} — {msg}'

            elif subcmd == 'diff' and len(parts) > 3:
                try:
                    from_id = int(parts[2])
                    to_id = int(parts[3])
                except ValueError:
                    return 'Usage: /supervisor diff <from_id> <to_id>'
                return _diff_configs(cur, from_id, to_id)

            else:
                return (
                    '=== /supervisor 서브커맨드 ===\n'
                    '  /supervisor report — 최신 리포트 생성\n'
                    '  /supervisor propose — 보류 중 제안 목록\n'
                    '  /supervisor apply <id> — 제안 적용\n'
                    '  /supervisor rollback <id> — 적용 롤백\n'
                    '  /supervisor diff <from> <to> — 설정 비교')

    except Exception as e:
        return f'⚠ supervisor error: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _diff_configs(cur, from_id, to_id):
    """Compare two config versions."""
    try:
        cur.execute("""
            SELECT id, config_snapshot, ts FROM strategy_config_versions
            WHERE id IN (%s, %s) ORDER BY id;
        """, (from_id, to_id))
        rows = cur.fetchall()
        if len(rows) < 2:
            return f'Need 2 versions, found {len(rows)}'

        cfg_a = rows[0][1] if isinstance(rows[0][1], dict) else json.loads(rows[0][1])
        cfg_b = rows[1][1] if isinstance(rows[1][1], dict) else json.loads(rows[1][1])

        all_keys = sorted(set(list(cfg_a.keys()) + list(cfg_b.keys())))
        diffs = []
        for k in all_keys:
            va = cfg_a.get(k)
            vb = cfg_b.get(k)
            if va != vb:
                diffs.append(f'  {k}: {va} → {vb}')

        if not diffs:
            return f'Config #{from_id} and #{to_id}: identical'

        header = (f'=== DIFF #{from_id} ({str(rows[0][2])[:16]}) '
                  f'→ #{to_id} ({str(rows[1][2])[:16]}) ===')
        return header + '\n' + '\n'.join(diffs)
    except Exception as e:
        return f'diff error: {e}'


# ══════════════════════════════════════════════════════════════
# SERVICE LOOP (optional standalone daemon)
# ══════════════════════════════════════════════════════════════

def run_cycle():
    """Run one supervision cycle. Called periodically (6h)."""
    global _last_report_ts

    try:
        import feature_flags
        if not feature_flags.is_enabled('ff_strategy_supervisor'):
            return

        now = time.time()
        if now - _last_report_ts < REPORT_INTERVAL_SEC:
            return

        _last_report_ts = now

        conn = _db_conn()
        conn.autocommit = True
        with conn.cursor() as cur:
            report = generate_report(cur)
            save_report(cur, report)

            # Config snapshot
            snapshot_config(cur, changed_by='supervisor_periodic')

            # Create proposals from report recommendations
            for p in report.get('patch_proposals', []):
                create_proposal(cur, p['change_type'], p['param'],
                                p.get('current'), p.get('proposed'),
                                p.get('reasoning', ''))

            # Send to telegram
            text = format_report(report)
            _notify_telegram(text)

            _log(f'6h report generated: {report["summary"].get("trades_count", 0)} trades')

        conn.close()

    except Exception as e:
        _log(f'run_cycle error: {e}')
        traceback.print_exc()


def main():
    """Standalone daemon loop."""
    _log('=== STRATEGY SUPERVISOR START ===')
    import db_migrations
    conn = _db_conn()
    conn.autocommit = True
    with conn.cursor() as cur:
        db_migrations.ensure_supervisor_reports(cur)
        db_migrations.ensure_strategy_config_versions(cur)
        db_migrations.ensure_strategy_change_proposals(cur)
    conn.close()

    while True:
        try:
            run_cycle()
        except Exception:
            traceback.print_exc()
        time.sleep(300)  # Check every 5 min, but only generate every 6h


if __name__ == '__main__':
    main()
