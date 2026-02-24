"""
event_decision_engine.py — Claude Event Decision Mode core orchestrator.

When ff_event_decision_mode is ON and event_trigger returns MODE_EVENT_DECISION,
this module:
  1. Acquires entry lock (60-180s)
  2. Builds rich snapshot bundle for Claude
  3. Calls Claude for direct action decision
  4. Applies safety guards (clamping, liquidity checks)
  5. Maps action to execution_queue entries
  6. Enforces server-side stop-loss
  7. Post-execution cleanup (orphan orders)
  8. Logs to event_decision_log + Telegram

FAIL-OPEN: All errors fall through to DEFAULT mode.
"""
import json
import os
import time
import traceback
import urllib.parse
import urllib.request

LOG_PREFIX = '[event_decision]'
SYMBOL = 'BTC/USDT:USDT'

# ── Safety guard constants ───────────────────────────────
GUARD_MAX_REDUCE_RATIO = 0.70
GUARD_MAX_REVERSE_SIZE_RATIO = 0.30
GUARD_MAX_HEDGE_SIZE_RATIO = 0.30
GUARD_MAX_FREEZE_MINUTES = 60
LIQUIDITY_STRESS_ALLOWED = frozenset({
    'HARD_EXIT', 'RISK_OFF_REDUCE', 'HOLD', 'FREEZE_NEW_ENTRY',
})

# Minimum order qty (Bybit BTC/USDT:USDT)
MIN_ORDER_QTY_BTC = 0.001


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


_tg_config = {}

def _load_tg_config():
    if _tg_config:
        return _tg_config
    env_path = '/root/trading-bot/app/telegram_cmd.env'
    try:
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if '=' in line and not line.startswith('#'):
                    k, v = line.split('=', 1)
                    _tg_config[k.strip()] = v.strip()
    except Exception:
        pass
    return _tg_config


def _send_telegram(text):
    try:
        cfg = _load_tg_config()
        token = cfg.get('TELEGRAM_BOT_TOKEN', '')
        chat_id = cfg.get('TELEGRAM_ALLOWED_CHAT_ID', '')
        if not token or not chat_id:
            return
        import report_formatter as _rf
        text = _rf.korean_output_guard(text)
        url = f'https://api.telegram.org/bot{token}/sendMessage'
        data = urllib.parse.urlencode({
            'chat_id': chat_id,
            'text': text,
            'disable_web_page_preview': 'true'}).encode('utf-8')
        req = urllib.request.Request(url, data=data, method='POST')
        with urllib.request.urlopen(req, timeout=5):
            pass
    except Exception:
        pass


# ─────────────────────────────────────────────────────────
# MAIN ORCHESTRATOR
# ─────────────────────────────────────────────────────────

def handle_event_decision(cur, ctx, event_result, snapshot, position, conn=None):
    """Main orchestrator for EVENT_DECISION mode.

    Args:
        cur: DB cursor
        ctx: full context dict (position, scores, indicators, etc.)
        event_result: EventResult from event_trigger
        snapshot: market snapshot dict
        position: exchange position dict {side, qty, entry_price, mark_price, ...}
        conn: DB connection (optional)

    Returns: (action: str, detail: dict)
    """
    import report_formatter
    start_ms = int(time.time() * 1000)
    trigger_types = [t.get('type', '?') for t in event_result.triggers]
    price = snapshot.get('price', 0) if snapshot else 0

    _log(f'EVENT_DECISION start: triggers={trigger_types} price={price}')

    # Kill switch check
    if os.path.exists('/root/trading-bot/app/KILL_SWITCH'):
        _log('KILL_SWITCH active — aborting event decision')
        return ('HOLD', {'reason': 'KILL_SWITCH'})

    try:
        # 1. Acquire entry lock
        lock_key, lock_ttl = _acquire_entry_lock(
            event_result, trigger_types, price, conn=conn)

        # 2. Send pre-alert
        try:
            _send_telegram(report_formatter.format_event_decision_pre_alert(
                event_result.triggers, event_result.mode, snapshot=snapshot))
        except Exception:
            pass

        # 3. Build snapshot bundle
        bundle = _build_snapshot_bundle(cur, ctx, snapshot, position, event_result)

        # 4. Call Claude
        import claude_api
        claude_result = claude_api.event_decision_analysis(
            bundle, snapshot, event_result)

        claude_called = not claude_result.get('fallback_used', False)
        original_action = claude_result.get('action', 'HOLD')
        params = claude_result.get('params', {})

        _log(f'Claude result: action={original_action} '
             f'class={claude_result.get("event_class")} '
             f'conf={claude_result.get("confidence")} '
             f'fallback={claude_result.get("fallback_used")}')

        # 5. Apply safety guards
        final_action, guarded_params, guard_reasons = _apply_safety_guards(
            original_action, params, position, snapshot)

        guard_applied = bool(guard_reasons)
        if guard_applied:
            _log(f'Guard applied: {original_action} → {final_action} '
                 f'reasons={guard_reasons}')

        # 6. Map action to execution
        eq_ids = _map_action_to_execution(
            cur, final_action, guarded_params, position, SYMBOL)

        # 7. Enforce server stop
        _enforce_server_stop(cur, position, guarded_params)

        # 8. Post-execution cleanup
        safety_checks = claude_result.get('safety_checks', {})
        if final_action in ('HARD_EXIT',) or safety_checks.get('orphan_orders_cleanup_required'):
            _post_execution_cleanup(SYMBOL, final_action)

        # 9. DB logging
        elapsed_ms = int(time.time() * 1000) - start_ms
        _log_event_decision(
            cur, event_result, snapshot, claude_result,
            original_action, final_action, guard_applied, guard_reasons,
            eq_ids, lock_key, lock_ttl, elapsed_ms)

        # 10. Telegram post-alert
        try:
            _send_telegram(report_formatter.format_event_decision_post_alert(
                event_result.triggers, final_action, claude_result,
                guards=guard_reasons))
        except Exception:
            pass

        detail = {
            'action': final_action,
            'original_action': original_action,
            'event_class': claude_result.get('event_class'),
            'confidence': claude_result.get('confidence'),
            'params': guarded_params,
            'guard_applied': guard_applied,
            'guard_reasons': guard_reasons,
            'eq_ids': eq_ids,
            'latency_ms': elapsed_ms,
            'reasoning_short': claude_result.get('reasoning_short', ''),
        }

        _log(f'EVENT_DECISION done: action={final_action} eq_ids={eq_ids} '
             f'latency={elapsed_ms}ms')
        return (final_action, detail)

    except Exception as e:
        elapsed_ms = int(time.time() * 1000) - start_ms
        _log(f'EVENT_DECISION error (FAIL-OPEN): {e}')
        traceback.print_exc()
        return ('HOLD', {'reason': f'error: {e}', 'latency_ms': elapsed_ms})


# ─────────────────────────────────────────────────────────
# BUNDLE BUILDER
# ─────────────────────────────────────────────────────────

def _build_snapshot_bundle(cur, ctx, snapshot, position, event_result):
    """Build rich context bundle for Claude EVENT_DECISION prompt."""
    pos = ctx.get('position', {})
    if position:
        pos = {**pos, **{k: v for k, v in position.items() if v is not None}}

    # Position info
    pos_bundle = {
        'side': (pos.get('side') or 'NONE').upper(),
        'qty': pos.get('qty', 0),
        'entry_price': pos.get('entry_price', 0),
        'mark_price': pos.get('mark_price', snapshot.get('price', 0)),
        'upnl': pos.get('upnl', 0),
        'upnl_pct': pos.get('upnl_pct', 0),
        'leverage': pos.get('leverage', 0),
        'liq_price': pos.get('liq_price', 0),
    }

    # Orders info
    orders = {'active_count': 0, 'conditional_count': 0, 'has_orphan_risk': False}
    try:
        from live_order_executor import _get_exchange
        ex = _get_exchange()
        active = ex.fetch_open_orders(SYMBOL) or []
        cond = ex.fetch_open_orders(SYMBOL, params={'orderFilter': 'StopOrder'}) or []
        orders['active_count'] = len(active)
        orders['conditional_count'] = len(cond)
        # Orphan risk: orders exist but no position
        if (active or cond) and not pos.get('side'):
            orders['has_orphan_risk'] = True
    except Exception:
        pass

    # Market context
    returns = snapshot.get('returns', {}) if snapshot else {}
    mctx = {
        'regime': snapshot.get('regime', '?') if snapshot else '?',
        'drift': snapshot.get('drift', 0) if snapshot else 0,
        'adx': snapshot.get('adx', 0) if snapshot else 0,
        'atr_pct': snapshot.get('atr_pct', 0) if snapshot else 0,
        'impulse': snapshot.get('impulse', 0) if snapshot else 0,
        'volume_z': snapshot.get('volume_z', 0) if snapshot else 0,
        'range_pos': snapshot.get('range_pos', 0) if snapshot else 0,
        'breakout': snapshot.get('breakout', False) if snapshot else False,
        'ret_1m': returns.get('ret_1m', 0),
        'ret_5m': returns.get('ret_5m', 0),
        'ret_15m': returns.get('ret_15m', 0),
    }

    # Try to compute impulse from features module
    try:
        from strategy.common import features
        impulse = features.compute_impulse(cur)
        if impulse is not None:
            mctx['impulse'] = impulse
    except Exception:
        pass

    # Try to compute range_pos
    try:
        from strategy.common import features
        rp = features.compute_range_position(cur)
        if rp is not None:
            mctx['range_pos'] = rp
    except Exception:
        pass

    # Microstructure
    micro = {
        'spread_ok': snapshot.get('spread_ok', True) if snapshot else True,
        'liquidity_ok': snapshot.get('liquidity_ok', True) if snapshot else True,
        'orderbook_imbalance': snapshot.get('orderbook_imbalance', 0) if snapshot else 0,
        'slippage_est': snapshot.get('slippage_est', 0) if snapshot else 0,
    }

    # Recent execution
    recent = []
    try:
        cur.execute("""
            SELECT action_type, direction, reason, ts
            FROM execution_queue
            WHERE symbol = %s AND status = 'DONE'
            ORDER BY ts DESC LIMIT 20
        """, (SYMBOL,))
        for row in cur.fetchall():
            recent.append({
                'action': row[0],
                'side': row[1],
                'reason': row[2],
                'ts': str(row[3]),
            })
    except Exception:
        pass

    # System health
    health = {'gate_status': 'OK', 'down_services': [], 'latency_ms': 0}
    try:
        from system_watchdog import get_health_summary
        h = get_health_summary()
        health['down_services'] = h.get('down_services', [])
        health['latency_ms'] = h.get('latency_ms', 0)
    except Exception:
        pass

    return {
        'position': pos_bundle,
        'orders': orders,
        'mctx': mctx,
        'microstructure': micro,
        'recent_execution': recent,
        'system_health': health,
        'triggers': event_result.triggers if event_result else [],
        'risk_config': {
            'max_reduce_ratio': GUARD_MAX_REDUCE_RATIO,
            'max_reverse_ratio': GUARD_MAX_REVERSE_SIZE_RATIO,
            'max_hedge_ratio': GUARD_MAX_HEDGE_SIZE_RATIO,
            'max_freeze_minutes': GUARD_MAX_FREEZE_MINUTES,
        },
    }


# ─────────────────────────────────────────────────────────
# SAFETY GUARDS
# ─────────────────────────────────────────────────────────

def _apply_safety_guards(action, params, position, snapshot):
    """Apply safety guards to Claude's decision.

    Returns: (guarded_action, guarded_params, guard_reasons: list)
    """
    guard_reasons = []
    params = dict(params) if params else {}

    # 1. Clamp reduce_ratio
    if params.get('reduce_ratio', 0) > GUARD_MAX_REDUCE_RATIO:
        params['reduce_ratio'] = GUARD_MAX_REDUCE_RATIO
        guard_reasons.append(f'reduce_ratio clamped to {GUARD_MAX_REDUCE_RATIO}')

    # 2. Clamp reverse_size_ratio
    if params.get('reverse_size_ratio', 0) > GUARD_MAX_REVERSE_SIZE_RATIO:
        params['reverse_size_ratio'] = GUARD_MAX_REVERSE_SIZE_RATIO
        guard_reasons.append(f'reverse_size_ratio clamped to {GUARD_MAX_REVERSE_SIZE_RATIO}')

    # 3. Clamp hedge_size_ratio
    if params.get('hedge_size_ratio', 0) > GUARD_MAX_HEDGE_SIZE_RATIO:
        params['hedge_size_ratio'] = GUARD_MAX_HEDGE_SIZE_RATIO
        guard_reasons.append(f'hedge_size_ratio clamped to {GUARD_MAX_HEDGE_SIZE_RATIO}')

    # 4. Clamp freeze_minutes
    if params.get('freeze_minutes', 0) > GUARD_MAX_FREEZE_MINUTES:
        params['freeze_minutes'] = GUARD_MAX_FREEZE_MINUTES
        guard_reasons.append(f'freeze_minutes clamped to {GUARD_MAX_FREEZE_MINUTES}')

    # 5. Liquidity stress check: REVERSE/HEDGE blocked → HARD_EXIT
    spread_ok = snapshot.get('spread_ok', True) if snapshot else True
    liquidity_ok = snapshot.get('liquidity_ok', True) if snapshot else True
    if not spread_ok or not liquidity_ok:
        if action not in LIQUIDITY_STRESS_ALLOWED:
            guard_reasons.append(
                f'liquidity stress: {action} → HARD_EXIT '
                f'(spread_ok={spread_ok}, liquidity_ok={liquidity_ok})')
            action = 'HARD_EXIT'

    # 6. No position + exit actions → HOLD
    pos_side = ''
    pos_qty = 0
    if position:
        pos_side = (position.get('side') or '').upper()
        try:
            pos_qty = float(position.get('qty', 0) or 0)
        except (ValueError, TypeError):
            pos_qty = 0
    if pos_qty <= 0 and action in ('RISK_OFF_REDUCE', 'HARD_EXIT', 'REVERSE', 'HEDGE'):
        guard_reasons.append(f'no position: {action} → HOLD')
        action = 'HOLD'

    # 7. Unrecognized action → HOLD
    from claude_api import VALID_EVENT_ACTIONS
    if action not in VALID_EVENT_ACTIONS:
        guard_reasons.append(f'unrecognized action: {action} → HOLD')
        action = 'HOLD'

    return (action, params, guard_reasons)


# ─────────────────────────────────────────────────────────
# ACTION → EXECUTION QUEUE MAPPING
# ─────────────────────────────────────────────────────────

def _map_action_to_execution(cur, action, params, position, symbol):
    """Map Claude action to execution_queue entries.

    Returns: list of eq_ids
    """
    eq_ids = []
    pos_side = (position.get('side') or '').upper() if position else ''
    try:
        pos_qty = float(position.get('qty', 0) or 0) if position else 0
    except (ValueError, TypeError):
        pos_qty = 0

    if action == 'HOLD':
        # No-op: server SL check only (done separately)
        return eq_ids

    if action == 'RISK_OFF_REDUCE':
        if pos_qty <= 0:
            return eq_ids
        reduce_ratio = params.get('reduce_ratio', 0.5)
        reduce_pct = int(reduce_ratio * 100)
        reduce_qty = pos_qty * reduce_ratio
        if reduce_qty < MIN_ORDER_QTY_BTC:
            _log(f'REDUCE qty {reduce_qty:.4f} < min {MIN_ORDER_QTY_BTC} — skipped')
            return eq_ids
        eq_id = _enqueue(cur, 'REDUCE', pos_side, symbol,
                         reduce_pct=reduce_pct,
                         reason='event_decision_reduce',
                         priority=2)
        if eq_id:
            eq_ids.append(eq_id)

    elif action == 'HARD_EXIT':
        if pos_qty <= 0:
            return eq_ids
        eq_id = _enqueue(cur, 'FULL_CLOSE', pos_side, symbol,
                         target_qty=pos_qty,
                         reason='event_decision_hard_exit',
                         priority=1)
        if eq_id:
            eq_ids.append(eq_id)

    elif action == 'REVERSE':
        if pos_qty <= 0:
            return eq_ids
        new_side = 'SHORT' if pos_side == 'LONG' else 'LONG'
        # Close current
        close_id = _enqueue(cur, 'REVERSE_CLOSE', pos_side, symbol,
                            target_qty=pos_qty,
                            reason='event_decision_reverse',
                            priority=1)
        if close_id:
            eq_ids.append(close_id)
            # Open opposite
            open_id = _enqueue(cur, 'REVERSE_OPEN', new_side, symbol,
                               reason='event_decision_reverse',
                               priority=1,
                               meta={'depends_on': close_id})
            if open_id:
                eq_ids.append(open_id)

    elif action == 'HEDGE':
        if pos_qty <= 0:
            return eq_ids
        hedge_side = 'SHORT' if pos_side == 'LONG' else 'LONG'
        hedge_ratio = params.get('hedge_size_ratio', 0.20)
        # Estimate hedge USDT from position
        try:
            mark = float((position or {}).get('mark_price', 0) or
                         (position or {}).get('entry_price', 0) or 0)
        except (ValueError, TypeError):
            mark = 0
        hedge_usdt = pos_qty * mark * hedge_ratio if mark > 0 else 0
        if hedge_usdt <= 0:
            return eq_ids
        eq_id = _enqueue(cur, 'ADD', hedge_side, symbol,
                         target_usdt=hedge_usdt,
                         reason='event_decision_hedge',
                         priority=2)
        if eq_id:
            eq_ids.append(eq_id)

    elif action == 'FREEZE_NEW_ENTRY':
        # Set entry lock only — no execution_queue
        freeze_minutes = params.get('freeze_minutes', 15)
        try:
            import event_lock
            lock_key = f'freeze_entry:{symbol}'
            event_lock.acquire_lock(
                lock_key, ttl_sec=freeze_minutes * 60,
                caller='event_decision', lock_type='freeze',
                conn=None)
            _log(f'FREEZE_NEW_ENTRY: {freeze_minutes}min lock set')
        except Exception as e:
            _log(f'FREEZE lock error: {e}')

    return eq_ids


def _enqueue(cur, action_type, direction, symbol, **kwargs):
    """Insert into execution_queue. Thin wrapper matching position_manager pattern."""
    try:
        # Duplicate check
        if action_type in ('REDUCE', 'FULL_CLOSE', 'REVERSE_CLOSE'):
            dedup_statuses = ('PENDING', 'PICKED', 'SENT')
        else:
            dedup_statuses = ('PENDING', 'PICKED')

        cur.execute("""
            SELECT id FROM execution_queue
            WHERE symbol = %s AND action_type = %s AND direction = %s
              AND status IN %s
              AND ts >= now() - interval '5 minutes';
        """, (symbol, action_type, direction, dedup_statuses))
        if cur.fetchone():
            _log(f'duplicate {action_type} {direction} blocked')
            return None

        meta = kwargs.get('meta', {})
        cur.execute("""
            INSERT INTO execution_queue
                (symbol, action_type, direction, target_qty, target_usdt,
                 reduce_pct, source, reason, priority,
                 expire_at, meta)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    now() + interval '5 minutes', %s::jsonb)
            RETURNING id;
        """, (symbol, action_type, direction,
              kwargs.get('target_qty'), kwargs.get('target_usdt'),
              kwargs.get('reduce_pct'), 'event_decision',
              kwargs.get('reason', 'event_decision'), kwargs.get('priority', 3),
              json.dumps(meta, default=str)))
        row = cur.fetchone()
        eq_id = row[0] if row else None
        _log(f'enqueued: {action_type} {direction} eq_id={eq_id}')
        return eq_id
    except Exception as e:
        _log(f'enqueue error: {e}')
        traceback.print_exc()
        return None


# ─────────────────────────────────────────────────────────
# SERVER STOP ENFORCEMENT
# ─────────────────────────────────────────────────────────

def _enforce_server_stop(cur, position, params):
    """Ensure server-side stop-loss is set after Claude decision."""
    if not position:
        _log('enforce_server_stop: no position, skipping')
        return
    try:
        import server_stop_manager
        mgr = server_stop_manager.get_manager()
        new_sl_value = params.get('new_sl_value', 0)

        if new_sl_value and float(new_sl_value) > 0:
            # Direct SL price
            synced, detail = mgr.sync_stop_order(cur, position, float(new_sl_value))
        else:
            # Use sync_event_stop for percentage-based or default
            sl_type = params.get('new_sl_type', '')
            if sl_type == 'BREAKEVEN':
                try:
                    entry = float(position.get('entry_price', 0) or 0)
                except (ValueError, TypeError):
                    entry = 0
                if entry > 0:
                    synced, detail = mgr.sync_stop_order(cur, position, entry)
                else:
                    synced, detail = mgr.sync_event_stop(
                        cur, position, urgency='HIGH')
            else:
                synced, detail = mgr.sync_event_stop(
                    cur, position, urgency='HIGH')

        if not synced:
            _log(f'HARD STOP SET FAILED: {detail}')
            _send_telegram(
                f'⚠️ HARD STOP SET FAILED\n'
                f'- 상세: {detail}\n'
                f'- 수동 확인 필요')
    except Exception as e:
        _log(f'enforce_server_stop error: {e}')
        _send_telegram(
            f'⚠️ HARD STOP SET FAILED\n'
            f'- 에러: {e}\n'
            f'- 수동 확인 필요')


# ─────────────────────────────────────────────────────────
# POST-EXECUTION CLEANUP
# ─────────────────────────────────────────────────────────

def _post_execution_cleanup(symbol, action):
    """Post-execution orphan cleanup. FAIL-OPEN."""
    if action not in ('HARD_EXIT', 'FULL_CLOSE'):
        return
    try:
        from live_order_executor import _get_exchange
        import orphan_cleanup
        ex = _get_exchange()
        orphan_cleanup.post_exit_cleanup(ex, symbol, caller='event_decision')
    except Exception as e:
        _log(f'post_execution_cleanup error (FAIL-OPEN): {e}')


# ─────────────────────────────────────────────────────────
# ENTRY LOCK
# ─────────────────────────────────────────────────────────

def _acquire_entry_lock(event_result, trigger_types, price, conn=None):
    """Acquire soft entry lock for event decision processing (60-180s)."""
    try:
        import event_lock
        lock_key = f'event_decision:{SYMBOL}:{"|".join(sorted(trigger_types))}'
        ttl = 120  # default 120s
        # Emergency-like triggers get longer TTL
        if any('emergency' in t for t in trigger_types):
            ttl = 180
        event_lock.acquire_lock(
            lock_key, ttl_sec=ttl,
            caller='event_decision', lock_type='event_decision',
            conn=conn)
        return (lock_key, ttl)
    except Exception as e:
        _log(f'entry lock error (FAIL-OPEN): {e}')
        return ('', 0)


# ─────────────────────────────────────────────────────────
# DB LOGGING
# ─────────────────────────────────────────────────────────

def _log_event_decision(cur, event_result, snapshot, claude_result,
                        original_action, final_action, guard_applied,
                        guard_reasons, eq_ids, lock_key, lock_ttl,
                        elapsed_ms):
    """Log event decision to event_decision_log table."""
    try:
        cur.execute("""
            INSERT INTO event_decision_log
                (symbol, mode, triggers, event_hash, snapshot_price,
                 claude_called, claude_raw, claude_parsed,
                 guard_applied, guard_reasons, original_action, final_action,
                 eq_ids, entry_lock_key, entry_lock_ttl,
                 latency_ms, input_tokens, output_tokens,
                 estimated_cost, model_used)
            VALUES (%s, %s, %s::jsonb, %s, %s,
                    %s, %s::jsonb, %s::jsonb,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s, %s)
        """, (
            SYMBOL,
            event_result.mode,
            json.dumps(event_result.triggers, default=str),
            event_result.event_hash,
            snapshot.get('price') if snapshot else None,
            not claude_result.get('fallback_used', True),
            json.dumps(claude_result, default=str, ensure_ascii=False),
            json.dumps({
                'action': final_action,
                'event_class': claude_result.get('event_class'),
                'confidence': claude_result.get('confidence'),
                'params': claude_result.get('params', {}),
            }, default=str),
            guard_applied,
            guard_reasons or [],
            original_action,
            final_action,
            eq_ids or [],
            lock_key,
            lock_ttl,
            elapsed_ms,
            claude_result.get('input_tokens', 0),
            claude_result.get('output_tokens', 0),
            claude_result.get('estimated_cost_usd', 0),
            claude_result.get('model', ''),
        ))
    except Exception as e:
        _log(f'DB log error: {e}')
        traceback.print_exc()
