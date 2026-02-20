"""
exchange_reader.py — Read-only exchange API + strategy DB reader.

All functions return dicts with SOURCE / DATA_STATUS fields.
No writes, no order placement.
"""
import os
import sys

sys.path.insert(0, '/root/trading-bot/app')

LOG_PREFIX = '[exchange_reader]'
SYMBOL = os.getenv('SYMBOL', 'BTC/USDT:USDT')

_exchange_cache = None


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _get_exchange():
    """Cached ccxt.bybit read-only singleton."""
    global _exchange_cache
    if _exchange_cache is not None:
        return _exchange_cache
    import ccxt
    _exchange_cache = ccxt.bybit({
        'apiKey': os.getenv('BYBIT_API_KEY'),
        'secret': os.getenv('BYBIT_SECRET'),
        'enableRateLimit': True,
        'timeout': 20000,
        'options': {'defaultType': 'swap'},
    })
    return _exchange_cache


def _db():
    from db_config import get_conn
    return get_conn(autocommit=True)


# ── Exchange API readers ─────────────────────────────────


def fetch_position(symbol=None):
    """Fetch live exchange position. Returns dict with standardised labels."""
    sym = symbol or SYMBOL
    try:
        ex = _get_exchange()
        positions = ex.fetch_positions([sym])
        for p in positions:
            if p.get('symbol') != sym:
                continue
            contracts = float(p.get('contracts') or 0.0)
            side = p.get('side')
            upnl = float(p.get('unrealizedPnl') or 0.0)
            entry = float(p.get('entryPrice') or 0.0)
            mark = float(p.get('markPrice') or 0.0)
            leverage = float(p.get('leverage') or 0.0)
            liq = float(p.get('liquidationPrice') or 0.0)
            if contracts != 0.0 and side in ('long', 'short'):
                return {
                    'source': 'EXCHANGE',
                    'data_status': 'OK',
                    'exchange_position': side.upper(),
                    'exch_pos_qty': contracts,
                    'exch_entry_price': entry,
                    'exch_mark_price': mark,
                    'upnl': upnl,
                    'leverage': leverage,
                    'liq_price': liq,
                }
        return {
            'source': 'EXCHANGE',
            'data_status': 'OK',
            'exchange_position': 'NONE',
            'exch_pos_qty': 0.0,
            'exch_entry_price': 0.0,
            'exch_mark_price': 0.0,
            'upnl': 0.0,
            'leverage': 0.0,
            'liq_price': 0.0,
        }
    except Exception as e:
        _log(f'fetch_position error: {e}')
        return {
            'source': 'EXCHANGE',
            'data_status': 'ERROR',
            'exchange_position': 'UNKNOWN',
            'error': str(e),
        }


def fetch_open_orders(symbol=None):
    """Fetch open orders from exchange."""
    sym = symbol or SYMBOL
    try:
        ex = _get_exchange()
        raw = ex.fetch_open_orders(sym)
        orders = []
        for o in raw:
            orders.append({
                'id': o.get('id'),
                'side': o.get('side'),
                'type': o.get('type'),
                'price': float(o.get('price') or 0),
                'amount': float(o.get('amount') or 0),
                'filled': float(o.get('filled') or 0),
                'status': o.get('status'),
                'timestamp': o.get('datetime'),
            })
        return {
            'source': 'EXCHANGE',
            'data_status': 'OK',
            'orders': orders,
        }
    except Exception as e:
        _log(f'fetch_open_orders error: {e}')
        return {
            'source': 'EXCHANGE',
            'data_status': 'ERROR',
            'orders': [],
            'error': str(e),
        }


def fetch_balance():
    """Fetch USDT balance from exchange."""
    try:
        ex = _get_exchange()
        bal = ex.fetch_balance()
        usdt = bal.get('USDT', {})
        return {
            'source': 'EXCHANGE',
            'data_status': 'OK',
            'total': float(usdt.get('total') or 0),
            'free': float(usdt.get('free') or 0),
            'used': float(usdt.get('used') or 0),
        }
    except Exception as e:
        _log(f'fetch_balance error: {e}')
        return {
            'source': 'EXCHANGE',
            'data_status': 'ERROR',
            'total': 0,
            'free': 0,
            'used': 0,
            'error': str(e),
        }


# ── Strategy DB reader ───────────────────────────────────


def fetch_position_strat(symbol=None):
    """Read position_state from strategy DB.
    Attempts v2 query (with order_state, planned/filled cols) first,
    falls back to v1 query if migration not yet applied.
    """
    sym = symbol or SYMBOL
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            # Try v3 query with plan_state column
            try:
                cur.execute("""
                    SELECT side, total_qty, avg_entry_price, stage,
                           capital_used_usdt, trade_budget_used_pct,
                           order_state, planned_qty, filled_qty,
                           planned_usdt, filled_usdt, last_order_id,
                           plan_state
                    FROM position_state WHERE symbol = %s;
                """, (sym,))
                row = cur.fetchone()
                has_v2 = True
                has_v3 = True if row and len(row) > 12 else False
            except Exception:
                # v3 plan_state not present, try v2
                conn.rollback() if hasattr(conn, 'rollback') else None
                try:
                    cur.execute("""
                        SELECT side, total_qty, avg_entry_price, stage,
                               capital_used_usdt, trade_budget_used_pct,
                               order_state, planned_qty, filled_qty,
                               planned_usdt, filled_usdt, last_order_id
                        FROM position_state WHERE symbol = %s;
                    """, (sym,))
                    row = cur.fetchone()
                    has_v2 = True
                    has_v3 = False
                except Exception:
                    # v2 columns not present, fallback to v1
                    conn.rollback() if hasattr(conn, 'rollback') else None
                    cur.execute("""
                        SELECT side, total_qty, avg_entry_price, stage,
                               capital_used_usdt, trade_budget_used_pct
                        FROM position_state WHERE symbol = %s;
                    """, (sym,))
                    row = cur.fetchone()
                    has_v2 = False
                    has_v3 = False

        if row and row[0]:
            order_state = row[6] if has_v2 and len(row) > 6 else None
            plan_state_col = row[12] if has_v3 and len(row) > 12 else None
            result = {
                'source': 'STRATEGY_DB',
                'data_status': 'OK',
                'strategy_state': _map_strategy_state(row[0], row[3], order_state, plan_state_col),
                'side': row[0],
                'planned_stage_qty': float(row[1] or 0),
                'avg_entry_price': float(row[2] or 0),
                'stage': int(row[3] or 0),
                'capital_used_usdt': float(row[4] or 0),
                'trade_budget_used_pct': float(row[5] or 0),
            }
            if has_v2 and len(row) > 6:
                result['order_state'] = row[6] or 'NONE'
                result['planned_qty'] = float(row[7] or 0)
                result['filled_qty'] = float(row[8] or 0)
                result['planned_usdt'] = float(row[9] or 0)
                result['filled_usdt'] = float(row[10] or 0)
                result['last_order_id'] = row[11]
            if has_v3 and len(row) > 12:
                result['plan_state'] = row[12] or 'PLAN.NONE'
            return result
        return {
            'source': 'STRATEGY_DB',
            'data_status': 'OK',
            'strategy_state': 'FLAT',
            'side': None,
            'planned_stage_qty': 0,
            'avg_entry_price': 0,
            'stage': 0,
            'capital_used_usdt': 0,
            'trade_budget_used_pct': 0,
            'order_state': 'NONE',
            'planned_qty': 0,
            'filled_qty': 0,
            'planned_usdt': 0,
            'filled_usdt': 0,
            'last_order_id': None,
        }
    except Exception as e:
        _log(f'fetch_position_strat error: {e}')
        return {
            'source': 'STRATEGY_DB',
            'data_status': 'ERROR',
            'strategy_state': 'UNKNOWN',
            'error': str(e),
        }
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _map_strategy_state(side, stage, order_state=None, plan_state_col=None):
    """Map DB side+stage+order_state into PLAN.* state using plan_state module.
    Falls back to legacy strings for callers not yet migrated.
    """
    import plan_state as ps
    return ps.map_db_to_plan(side, stage, order_state, plan_state_col)


def _map_strategy_state_legacy(side, stage, order_state=None):
    """Legacy wrapper returning old-style strings for gradual migration.
    Returns: FLAT, INTENT_ENTER, IN_POSITION, ORDER_SENT, etc."""
    import plan_state as ps
    return ps.map_legacy(side, stage, order_state)


# ── Reconcile ────────────────────────────────────────────


def reconcile(exch, strat):
    """Compare exchange position vs strategy DB.

    Returns structured dict:
        {
            'status': 'RECONCILE.OK' | 'RECONCILE.MISMATCH' | 'RECONCILE.UNKNOWN',
            'detail': str,
            'needs_healing': bool,
            'legacy': 'MATCH' | 'MISMATCH' | 'UNKNOWN',  # backward compat
        }
    """
    if exch.get('data_status') == 'ERROR' or strat.get('data_status') == 'ERROR':
        return {
            'status': 'RECONCILE.UNKNOWN',
            'detail': 'data_status ERROR',
            'needs_healing': False,
            'legacy': 'UNKNOWN',
        }
    exch_pos = exch.get('exchange_position', 'UNKNOWN')
    strat_side = (strat.get('side') or '').upper()
    strat_state = strat.get('strategy_state', 'FLAT')
    plan_state = strat.get('plan_state', '')

    # Both flat
    if exch_pos == 'NONE' and strat_state in ('FLAT', 'PLAN.NONE'):
        return {
            'status': 'RECONCILE.OK',
            'detail': 'both_flat',
            'needs_healing': False,
            'legacy': 'MATCH',
        }

    # Both have position — compare side and rough qty
    if exch_pos in ('LONG', 'SHORT') and strat_side == exch_pos:
        exch_qty = exch.get('exch_pos_qty', 0)
        strat_qty = strat.get('planned_stage_qty', 0)
        if strat_qty > 0 and abs(exch_qty - strat_qty) / strat_qty < 0.05:
            return {
                'status': 'RECONCILE.OK',
                'detail': f'side_match qty_match: exch={exch_qty} strat={strat_qty}',
                'needs_healing': False,
                'legacy': 'MATCH',
            }
        return {
            'status': 'RECONCILE.MISMATCH',
            'detail': f'qty_diff: exch={exch_qty} strat={strat_qty}',
            'needs_healing': True,
            'legacy': 'MISMATCH',
        }

    # Exchange NONE but DB thinks position exists
    if exch_pos == 'NONE' and strat_state not in ('FLAT', 'PLAN.NONE'):
        # Check if it's an intent state — may not need healing yet
        import plan_state as ps
        is_intent = ps.is_intent_state(plan_state) if plan_state else False
        return {
            'status': 'RECONCILE.MISMATCH',
            'detail': f'exch_none_plan_{plan_state or strat_state}',
            'needs_healing': not is_intent,  # intent states might resolve naturally
            'legacy': 'MISMATCH',
        }

    # Exchange has position but DB says flat
    if exch_pos != 'NONE' and strat_state in ('FLAT', 'PLAN.NONE'):
        return {
            'status': 'RECONCILE.MISMATCH',
            'detail': f'exch_{exch_pos}_db_flat',
            'needs_healing': True,
            'legacy': 'MISMATCH',
        }

    # Side mismatch
    return {
        'status': 'RECONCILE.MISMATCH',
        'detail': f'side_mismatch: exch={exch_pos} strat={strat_side}',
        'needs_healing': True,
        'legacy': 'MISMATCH',
    }


def reconcile_status_str(exch, strat):
    """Legacy wrapper returning 'MATCH'/'MISMATCH'/'UNKNOWN' string."""
    result = reconcile(exch, strat)
    return result['legacy']


# ── Wait Reason ──────────────────────────────────────────


def compute_wait_reason(cur=None, gate_status=None):
    """Determine why the bot is not trading right now.
    Returns (reason, detail) tuple.
    Priority: WAIT_SWITCH > WAIT_GATED > WAIT_RISK_LOCK > WAIT_ORDER_FILL > WAIT_SIGNAL
    gate_status: (ok, reason) tuple from safety_manager.run_all_checks()
    """
    conn = None
    close_conn = False
    try:
        if cur is None:
            conn = _db()
            cur = conn.cursor()
            close_conn = True

        # trade_switch OFF → WAIT_SWITCH
        cur.execute(
            "SELECT enabled FROM trade_switch ORDER BY id DESC LIMIT 1;")
        row = cur.fetchone()
        if row and not row[0]:
            return ('WAIT_SWITCH', 'trade_switch OFF → 신규 진입 불가')

        # Order throttle check → WAIT_RATE_LIMIT
        try:
            import order_throttle
            t_ok, t_reason, t_meta = order_throttle.check_all(cur, 'OPEN')
            if not t_ok:
                next_ts = t_meta.get('next_allowed_ts')
                if next_ts:
                    from datetime import datetime as _dt, timezone as _tz
                    next_str = _dt.fromtimestamp(next_ts, tz=_tz.utc).strftime('%H:%M:%S UTC')
                    return ('WAIT_RATE_LIMIT', f'주문 속도 제한: {t_reason} (해제: {next_str})')
                cd_rem = t_meta.get('cooldown_remaining')
                if cd_rem:
                    return ('WAIT_RATE_LIMIT', f'쿨다운 대기: {t_reason} ({cd_rem:.0f}초)')
                return ('WAIT_RATE_LIMIT', f'주문 속도 제한: {t_reason}')
        except ImportError:
            pass

        # 1. gate BLOCKED → WAIT_GATED
        if gate_status is not None:
            gate_ok, _gate_reason = gate_status
            if gate_ok is False:
                return ('WAIT_GATED', f'safety gate 차단: {_gate_reason}')

        # 2. once_lock → WAIT_RISK_LOCK
        cur.execute(
            "SELECT count(*) FROM live_order_once_lock WHERE symbol = %s;",
            (SYMBOL,))
        if int(cur.fetchone()[0]) > 0:
            return ('WAIT_RISK_LOCK', 'once_lock 활성 (주문 중복 방지)')

        # 3. execution_queue PENDING/SUBMITTED → WAIT_ORDER_FILL
        cur.execute("""
            SELECT count(*) FROM execution_queue
            WHERE symbol = %s AND status IN ('PENDING', 'SUBMITTED')
        """, (SYMBOL,))
        if int(cur.fetchone()[0]) > 0:
            return ('WAIT_ORDER_FILL', '실행큐 대기/전송 중')

        # 4. Capital cap exceeded -> WAIT_CAP
        try:
            import safety_manager
            eq = safety_manager.get_equity_limits(cur)
            cur.execute('SELECT capital_used_usdt FROM position_state WHERE symbol = %s;', (SYMBOL,))
            row = cur.fetchone()
            used = float(row[0]) if row and row[0] else 0
            if used >= eq['operating_cap']:
                return ('WAIT_CAP', f'자본 한도 도달: {used:.0f}/{eq["operating_cap"]:.0f} USDT')
        except Exception:
            pass

        # Regime-specific wait detail
        try:
            import regime_reader
            _rctx = regime_reader.get_current_regime(cur)
            if _rctx.get('available'):
                _regime = _rctx.get('regime', 'UNKNOWN')
                if _rctx.get('in_transition'):
                    return ('WAIT_SIGNAL', '레짐 전환 쿨다운 중')
                if _regime == 'RANGE':
                    return ('WAIT_SIGNAL', 'RANGE 모드: 밴드 경계 대기 (BB/VA 0.3% 이내 필요)')
                if _regime == 'BREAKOUT':
                    return ('WAIT_SIGNAL', 'BREAKOUT 모드: VA 돌파 확인 대기')
                if _regime == 'SHOCK':
                    return ('WAIT_SIGNAL', 'SHOCK 모드: 진입 차단')
        except Exception:
            pass  # FAIL-OPEN: default message

        # 5. Liquidity/spread check
        try:
            from strategy.common.features import build_feature_snapshot
            feat = build_feature_snapshot(cur)
            if feat.get('spread_ok') is False:
                return ('WAIT_LIQUIDITY', '슬리피지/스프레드 불량 — 주문 보류')
            if feat.get('liquidity_ok') is False:
                return ('WAIT_LIQUIDITY', '유동성 부족 — 주문 보류')
        except Exception:
            pass  # FAIL-OPEN: feature check failure does not block

        return ('WAIT_SIGNAL', '모든 조건 통과, 신호 대기 중')
    except Exception as e:
        _log(f'compute_wait_reason error: {e}')
        return ('WAIT_SIGNAL', f'error: {e}')
    finally:
        if close_conn and conn:
            try:
                conn.close()
            except Exception:
                pass


# ── Execution Context (comprehensive pipeline status) ────


def fetch_execution_context(cur=None):
    """Comprehensive execution pipeline status for fact snapshot.
    Returns dict with gate, switch, once_lock, test_mode, wait_reason,
    recent exec queue, capital limits.
    """
    conn = None
    close_conn = False
    try:
        if cur is None:
            conn = _db()
            cur = conn.cursor()
            close_conn = True
        ctx = {}

        # 1. trade_switch
        cur.execute(
            "SELECT enabled, updated_at FROM trade_switch ORDER BY id DESC LIMIT 1;")
        row = cur.fetchone()
        ctx['entry_enabled'] = row[0] if row else None
        ctx['entry_updated'] = str(row[1])[:19] if row and row[1] else ''
        ctx['exit_enabled'] = True  # always ON

        # 2. once_lock
        cur.execute("""
            SELECT opened_at, expires_at
            FROM live_order_once_lock WHERE symbol = %s LIMIT 1;
        """, (SYMBOL,))
        lock_row = cur.fetchone()
        if lock_row:
            ctx['once_lock'] = True
            ctx['once_lock_opened'] = str(lock_row[0])[:19] if lock_row[0] else '?'
            if lock_row[1]:
                from datetime import datetime, timezone
                try:
                    exp = lock_row[1]
                    now = datetime.now(timezone.utc)
                    if hasattr(exp, 'tzinfo') and exp.tzinfo is None:
                        import pytz
                        exp = pytz.utc.localize(exp)
                    remaining = exp - now
                    mins = max(0, int(remaining.total_seconds() / 60))
                    ctx['once_lock_ttl'] = f'{mins}분'
                except Exception:
                    ctx['once_lock_ttl'] = str(lock_row[1])[:19]
            else:
                ctx['once_lock_ttl'] = '무기한'
        else:
            ctx['once_lock'] = False
            ctx['once_lock_ttl'] = None

        # 3. test_mode
        try:
            import test_utils
            test = test_utils.load_test_mode()
            ctx['test_mode'] = test_utils.is_test_active(test)
            ctx['test_mode_end'] = test.get('end_utc', '')
        except Exception:
            ctx['test_mode'] = False
            ctx['test_mode_end'] = ''

        # 4. gate (safety_manager checks)
        try:
            import safety_manager
            gate_ok, gate_reason = safety_manager.run_all_checks(cur)
            ctx['gate_ok'] = gate_ok
            ctx['gate_reason'] = gate_reason
        except Exception as e:
            ctx['gate_ok'] = None
            ctx['gate_reason'] = str(e)

        # 5. wait_reason (gate_status 전달)
        wr = compute_wait_reason(cur, gate_status=(ctx.get('gate_ok'), ctx.get('gate_reason')))
        if isinstance(wr, tuple):
            ctx['wait_reason'] = wr[0]
            ctx['wait_detail'] = wr[1]
        else:
            ctx['wait_reason'] = wr
            ctx['wait_detail'] = ''

        # 6. execution_queue recent (last 3)
        try:
            cur.execute("""
                SELECT id, action_type, direction, status, target_usdt,
                       to_char(ts, 'MM-DD HH24:MI') as ts_str
                FROM execution_queue
                WHERE symbol = %s
                ORDER BY id DESC LIMIT 3;
            """, (SYMBOL,))
            eq_rows = cur.fetchall()
            ctx['recent_exec_queue'] = [
                {'id': r[0], 'action': r[1], 'direction': r[2],
                 'status': r[3], 'usdt': float(r[4] or 0), 'ts': r[5]}
                for r in eq_rows
            ]
        except Exception:
            ctx['recent_exec_queue'] = []

        # 7. last fill from execution_log
        try:
            cur.execute("""
                SELECT id, order_type, direction, status, requested_qty,
                       to_char(order_sent_at, 'MM-DD HH24:MI') as ts_str
                FROM execution_log
                WHERE status IN ('FILLED', 'PARTIAL')
                ORDER BY id DESC LIMIT 1;
            """)
            fill_row = cur.fetchone()
            if fill_row:
                ctx['last_fill'] = {
                    'id': fill_row[0], 'type': fill_row[1],
                    'direction': fill_row[2], 'status': fill_row[3],
                    'qty': float(fill_row[4] or 0), 'ts': fill_row[5],
                }
            else:
                ctx['last_fill'] = None
        except Exception:
            ctx['last_fill'] = None

        # 8. capital limits (dynamic equity-based)
        try:
            import safety_manager
            eq = safety_manager.get_equity_limits(cur)
            ctx['equity_limits'] = eq
            ctx['capital_limit'] = eq.get('operating_cap', 900)
            ctx['trade_budget_pct'] = eq.get('operating_ratio', 0.70) * 100
            ctx['max_stages'] = eq.get('max_stages', 7)
        except Exception:
            ctx['equity_limits'] = None
            ctx['capital_limit'] = 900
            ctx['trade_budget_pct'] = 70.0
            ctx['max_stages'] = 7

        # 9. LIVE_TRADING env
        live_env = os.getenv('LIVE_TRADING', '')
        ctx['live_trading'] = live_env == 'YES_I_UNDERSTAND'

        return ctx
    except Exception as e:
        _log(f'fetch_execution_context error: {e}')
        return {'error': str(e), 'entry_enabled': None, 'exit_enabled': True,
                'once_lock': False, 'test_mode': False, 'gate_ok': None,
                'gate_reason': 'context fetch failed', 'wait_reason': 'UNKNOWN',
                'wait_detail': '', 'recent_exec_queue': [], 'last_fill': None,
                'equity_limits': None,
                'capital_limit': 900, 'trade_budget_pct': 70.0, 'max_stages': 7,
                'live_trading': False}
    finally:
        if close_conn and conn:
            try:
                conn.close()
            except Exception:
                pass


# ── Trade Switch Status ──────────────────────────────────


def fetch_trade_switch_status():
    """Return structured trade_switch status dict."""
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT enabled, updated_at FROM trade_switch ORDER BY id DESC LIMIT 1;")
            row = cur.fetchone()
            if row:
                return {
                    'entry_enabled': bool(row[0]),
                    'exit_enabled': True,
                    'updated_at': str(row[1])[:19] if row[1] else '',
                    'reason': '' if row[0] else 'trade_switch OFF',
                }
            return {
                'entry_enabled': None,
                'exit_enabled': True,
                'updated_at': '',
                'reason': 'trade_switch 레코드 없음',
            }
    except Exception as e:
        _log(f'fetch_trade_switch_status error: {e}')
        return {
            'entry_enabled': None,
            'exit_enabled': True,
            'updated_at': '',
            'reason': f'error: {e}',
        }
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ── Report Exchange Block (fact packet) ──────────────────


def build_report_exchange_block():
    """Build the reusable fact packet for reports/snapshots/chat.

    Calls fetch_position, fetch_position_strat, fetch_open_orders,
    reconcile, fetch_trade_switch_status, and compute_wait_reason.
    Returns a flat dict with all fields needed by report formatters.
    """
    exch = fetch_position()
    strat = fetch_position_strat()
    orders_data = fetch_open_orders()
    recon_result = reconcile(exch, strat)
    recon = recon_result['legacy']  # backward compat for report block
    switch = fetch_trade_switch_status()

    conn = None
    gate_ok = None
    gate_reason = ''
    wait_reason = 'UNKNOWN'
    wait_detail = ''
    last_fill = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            try:
                import safety_manager
                gate_ok, gate_reason = safety_manager.run_all_checks(cur)
            except Exception as e:
                gate_reason = str(e)

            wr = compute_wait_reason(cur, gate_status=(gate_ok, gate_reason))
            if isinstance(wr, tuple):
                wait_reason, wait_detail = wr
            else:
                wait_reason = wr

            # last fill
            try:
                cur.execute("""
                    SELECT id, order_type, direction, status, requested_qty,
                           to_char(order_sent_at, 'MM-DD HH24:MI') as ts_str
                    FROM execution_log
                    WHERE status IN ('FILLED', 'PARTIAL')
                    ORDER BY id DESC LIMIT 1;
                """)
                fill_row = cur.fetchone()
                if fill_row:
                    last_fill = {
                        'id': fill_row[0], 'type': fill_row[1],
                        'direction': fill_row[2], 'status': fill_row[3],
                        'qty': float(fill_row[4] or 0), 'ts': fill_row[5],
                    }
            except Exception:
                pass
    except Exception as e:
        _log(f'build_report_exchange_block DB error: {e}')
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    exch_pos = exch.get('exchange_position', 'UNKNOWN')
    strat_state = strat.get('strategy_state', 'FLAT')
    orders_list = orders_data.get('orders', [])

    return {
        'exch_position': exch_pos,
        'exch_qty': exch.get('exch_pos_qty', 0.0),
        'exch_entry': exch.get('exch_entry_price', 0.0),
        'exch_mark': exch.get('exch_mark_price', 0.0),
        'exch_upnl': exch.get('upnl', 0.0),
        'strat_state': strat_state,
        'strat_side': (strat.get('side') or '').upper(),
        'strat_qty': strat.get('planned_stage_qty', 0.0),
        'strat_stage': strat.get('stage', 0),
        'reconcile': recon,
        'entry_enabled': switch.get('entry_enabled'),
        'trade_switch_updated': switch.get('updated_at', ''),
        'gate_ok': gate_ok,
        'gate_reason': gate_reason,
        'wait_reason': wait_reason,
        'wait_detail': wait_detail,
        'orders_count': len(orders_list),
        'last_fill': last_fill,
    }


# ── RECONCILE Auto-Recovery ──────────────────────────────


_reconcile_cycle_count = 0


def check_and_recover_mismatch(cur, exch_pos, strat_pos, ttl_minutes=10):
    """Auto-recover MISMATCH states.

    Case A: EXCHANGE=NONE + DB=INTENT_ENTER for >ttl_minutes → reset DB to FLAT
    Case B: EXCHANGE=OPEN + DB=FLAT → force sync DB to match exchange

    Returns: (recovered: bool, action: str, detail: str)
    """
    exch_position = exch_pos.get('exchange_position', 'UNKNOWN')
    strat_state = strat_pos.get('strategy_state', 'FLAT')

    if exch_position == 'UNKNOWN' or strat_state == 'UNKNOWN':
        return (False, 'SKIP', 'data incomplete')

    recon_result = reconcile(exch_pos, strat_pos)
    if recon_result['legacy'] != 'MISMATCH':
        return (False, 'OK', 'no mismatch')

    # Case A: Exchange=NONE but DB thinks we have a position intent
    if exch_position == 'NONE' and strat_state in ('INTENT_ENTER', 'IN_POSITION'):
        try:
            cur.execute("""
                SELECT updated_at FROM position_state
                WHERE symbol = %s LIMIT 1;
            """, (SYMBOL,))
            row = cur.fetchone()
            if row and row[0]:
                from datetime import datetime, timezone
                updated = row[0]
                now = datetime.now(timezone.utc)
                if hasattr(updated, 'tzinfo') and updated.tzinfo is None:
                    import pytz
                    updated = pytz.utc.localize(updated)
                age_min = (now - updated).total_seconds() / 60
                if age_min < ttl_minutes:
                    return (False, 'WAIT', f'mismatch age={age_min:.0f}m < ttl={ttl_minutes}m')

                # TTL exceeded → reset to FLAT (including v2/v3 columns)
                cur.execute("""
                    UPDATE position_state
                    SET side = NULL, total_qty = 0, avg_entry_price = 0,
                        stage = 0, capital_used_usdt = 0, trade_budget_used_pct = 0,
                        order_state = 'NONE', planned_qty = 0, filled_qty = 0,
                        planned_usdt = 0, sent_usdt = 0, filled_usdt = 0,
                        last_order_id = NULL, last_order_ts = NULL,
                        plan_state = 'PLAN.NONE',
                        state_changed_at = now(),
                        updated_at = now()
                    WHERE symbol = %s;
                """, (SYMBOL,))
                detail = (f'INTENT_ENTER → FLAT (거래소=NONE, '
                          f'DB 갱신 {age_min:.0f}분 경과, ttl={ttl_minutes}분)')
                _log(f'RECONCILE recovery Case A: {detail}')
                return (True, 'RESET_TO_FLAT', detail)
        except Exception as e:
            _log(f'RECONCILE Case A error: {e}')
            return (False, 'ERROR', str(e))

    # Case B: Exchange has position but DB says FLAT
    if exch_position in ('LONG', 'SHORT') and strat_state == 'FLAT':
        try:
            exch_qty = exch_pos.get('exch_pos_qty', 0)
            exch_entry = exch_pos.get('exch_entry_price', 0)
            cur.execute("""
                UPDATE position_state
                SET side = %s, total_qty = %s, avg_entry_price = %s,
                    stage = 1, order_state = 'FILLED',
                    filled_qty = %s, filled_usdt = %s,
                    plan_state = 'PLAN.OPEN',
                    state_changed_at = now(), updated_at = now()
                WHERE symbol = %s;
            """, (exch_position.lower(), exch_qty, exch_entry,
                  exch_qty, exch_qty * exch_entry, SYMBOL))
            detail = (f'FLAT → {exch_position} (거래소 실포지션에 DB 동기화, '
                      f'qty={exch_qty}, entry={exch_entry})')
            _log(f'RECONCILE recovery Case B: {detail}')
            return (True, 'SYNC_TO_EXCHANGE', detail)
        except Exception as e:
            _log(f'RECONCILE Case B error: {e}')
            return (False, 'ERROR', str(e))

    return (False, 'UNHANDLED', f'exch={exch_position} strat={strat_state}')
