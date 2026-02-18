"""
safety_manager.py — Centralized safety checks for position_manager and executor.

Checks:
  - Total capital exposure (trade budget 70% cap)
  - Daily/hourly trade counts
  - Daily loss limit (auto-OFF)
  - Circuit breaker (order flood)

7-Stage Budget System:
  - 7 stages, each 10% of capital_limit (cumulative: 10->20->30->40->50->60->70%)
  - Dynamic start_stage based on direction score
  - Policy A (cumulative): start_stage=k -> initial entry = k*10%
  - ADD = one slice (10%), total never exceeds 70%
"""
import os
import sys
sys.path.insert(0, '/root/trading-bot/app')
LOG_PREFIX = '[safety_mgr]'
SYMBOL = 'BTC/USDT:USDT'
TRADE_BUDGET_PCT = 70
STAGE_SLICE_PCT = 10
MAX_STAGES = 7


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _db_conn():
    from db_config import get_conn
    return get_conn()


def _load_safety_limits(cur):
    '''Load safety_limits from DB. Returns dict.'''
    cur.execute("""
        SELECT capital_limit_usdt, max_daily_trades, max_hourly_trades,
               daily_loss_limit_usdt, max_pyramid_stages, add_size_min_pct,
               add_size_max_pct, circuit_breaker_window_sec, circuit_breaker_max_orders,
               add_score_threshold, trade_budget_pct, stage_slice_pct, max_stages,
               stop_loss_pct
        FROM safety_limits ORDER BY id DESC LIMIT 1;
    """)
    row = cur.fetchone()
    if not row:
        return {
            'capital_limit_usdt': 900,
            'max_daily_trades': 20,
            'max_hourly_trades': 15,
            'daily_loss_limit_usdt': -45,
            'max_pyramid_stages': 7,
            'add_score_threshold': 65,
            'trade_budget_pct': 70,
            'stage_slice_pct': 10,
            'max_stages': 7,
            'stop_loss_pct': 2.0,
            'circuit_breaker_window_sec': 300,
            'circuit_breaker_max_orders': 10,
        }
    return {
        'capital_limit_usdt': float(row[0]) if row[0] is not None else 900,
        'max_daily_trades': int(row[1]) if row[1] is not None else 20,
        'max_hourly_trades': int(row[2]) if row[2] is not None else 15,
        'daily_loss_limit_usdt': float(row[3]) if row[3] is not None else -45,
        'max_pyramid_stages': int(row[4]) if row[4] is not None else 7,
        'add_size_min_pct': float(row[5]) if row[5] is not None else 5,
        'add_size_max_pct': float(row[6]) if row[6] is not None else 10,
        'circuit_breaker_window_sec': int(row[7]) if row[7] is not None else 300,
        'circuit_breaker_max_orders': int(row[8]) if row[8] is not None else 10,
        'add_score_threshold': int(row[9]) if row[9] is not None else 65,
        'trade_budget_pct': float(row[10]) if row[10] is not None else 70,
        'stage_slice_pct': float(row[11]) if row[11] is not None else 10,
        'max_stages': int(row[12]) if row[12] is not None else 7,
        'stop_loss_pct': float(row[13]) if row[13] is not None else 2.0,
    }


def check_service_health():
    '''Check service health state. Returns (can_open, reason).
    DOWN >= 1 → 차단. UNKNOWN → WARN 로그만 (차단 안 함).
    Uses dual-source (systemctl --all + heartbeat DB) via get_service_health_snapshot().
    '''
    try:
        from local_query_executor import get_service_health_snapshot
        health = get_service_health_snapshot()
        req_down = health.get('required_down', [])
        req_unknown = health.get('required_unknown', [])
        if req_down:
            return (False, f'필수 서비스 중지: {", ".join(req_down)}')
        if req_unknown:
            _log(f'WARN: 필수 서비스 미확인 {len(req_unknown)}개: {", ".join(req_unknown)} (차단 안 함)')
            return (True, f'WARN: 필수 서비스 미확인 {len(req_unknown)}개: {", ".join(req_unknown)}')
        return (True, 'ok')
    except Exception as e:
        _log(f'check_service_health error: {e}')
        return (True, 'health check unavailable')


def run_all_checks(cur, target_usdt=0, limits=None, emergency=False, manual_override=False):
    '''Run all safety checks. Returns (ok, reason).
    When emergency=True or manual_override=True, daily/hourly trade limits are BYPASSED.
    Circuit breaker, daily loss limit, and 70% exposure cap remain active.

    INVARIANT: 뉴스는 절대 gate를 차단하지 않음.
    뉴스 점수(news_event_w=0.05)는 스코어 가중치에 보조 역할만 하며,
    이 함수에서 news 테이블을 참조하거나 뉴스를 이유로 차단하는 로직은 없음.
    '''
    if limits is None:
        limits = _load_safety_limits(cur)

    # Service health check (신규 포지션 차단)
    if not emergency and not manual_override:
        svc_ok, svc_reason = check_service_health()
        if not svc_ok:
            _log(f'Service health block: {svc_reason}')
            return (False, f'서비스 상태 이상: {svc_reason}')

    if emergency or manual_override:
        bypass_reason = 'MANUAL OVERRIDE' if manual_override else 'EMERGENCY'
        _log(f'{bypass_reason}: hourly/daily limits BYPASSED')
    else:
        # Daily trade count
        cur.execute("""
            SELECT count(*) FROM execution_queue
            WHERE ts >= (now() AT TIME ZONE 'Asia/Seoul')::date AT TIME ZONE 'Asia/Seoul'
              AND status != 'REJECTED';
        """)
        daily_count = int(cur.fetchone()[0])
        if daily_count >= limits['max_daily_trades']:
            return (False, f"daily trade limit ({daily_count}/{limits['max_daily_trades']})")

        # Hourly trade count
        cur.execute("""
            SELECT count(*) FROM execution_queue
            WHERE ts >= now() - interval '1 hour'
              AND status != 'REJECTED';
        """)
        hourly_count = int(cur.fetchone()[0])
        if hourly_count >= limits['max_hourly_trades']:
            return (False, f"hourly trade limit ({hourly_count}/{limits['max_hourly_trades']})")

    # Circuit breaker (always active)
    window = limits['circuit_breaker_window_sec']
    cur.execute("""
        SELECT count(*) FROM execution_queue
        WHERE ts >= now() - make_interval(secs => %s);
    """, (window,))
    recent_count = int(cur.fetchone()[0])
    if recent_count >= limits['circuit_breaker_max_orders']:
        return (False, f"circuit breaker ({recent_count} orders in {window}s)")

    # Daily loss limit (always active)
    cur.execute("""
        SELECT COALESCE(sum(realized_pnl), 0) FROM execution_log
        WHERE ts >= (now() AT TIME ZONE 'Asia/Seoul')::date AT TIME ZONE 'Asia/Seoul'
          AND realized_pnl IS NOT NULL;
    """)
    daily_pnl = float(cur.fetchone()[0])
    if daily_pnl <= limits['daily_loss_limit_usdt']:
        return (False, f"daily loss limit ({daily_pnl:.1f} <= {limits['daily_loss_limit_usdt']})")

    # Consecutive stop-loss auto-halt (always active)
    cur.execute("""
        SELECT close_reason FROM execution_log
        WHERE ts >= (now() AT TIME ZONE 'Asia/Seoul')::date AT TIME ZONE 'Asia/Seoul'
          AND close_reason IS NOT NULL
        ORDER BY ts DESC LIMIT 3;
    """)
    recent_reasons = [r[0] for r in cur.fetchall()]
    consec_stops = 0
    for r in recent_reasons:
        if 'stop' in r.lower():
            consec_stops += 1
        else:
            break
    if consec_stops >= 3:
        try:
            cur.execute("""UPDATE trade_switch SET enabled = false
                           WHERE id = (SELECT id FROM trade_switch ORDER BY id DESC LIMIT 1);""")
        except Exception as e:
            _log(f'CONSECUTIVE STOPS: trade_switch UPDATE failed: {e}')
        _log(f'CONSECUTIVE STOPS AUTO-HALT: {consec_stops} stops today → trade_switch OFF')
        return (False, f"consecutive stop-loss auto-halt ({consec_stops} stops)")

    return (True, 'all checks passed')


def get_block_reason_code(cur, limits=None):
    """Map run_all_checks() result to a structured BLOCK_REASON_CODE.
    Returns (code, korean_description) tuple.
    Does NOT change run_all_checks() signature.
    """
    ok, reason = run_all_checks(cur, limits=limits)
    if ok:
        return ('NONE', '차단 없음 — 정상')
    r = reason.lower()
    if '서비스' in r or 'service' in r:
        return ('SERVICE_STALE', '필수 서비스 이상 — 주문 발행 금지')
    if 'daily_loss' in r or '손실' in r or 'daily loss' in r:
        return ('DAILY_LOSS_LIMIT', '일일 손실 한도 도달 — 매매 자동 중지')
    if 'circuit' in r:
        return ('CIRCUIT_BREAKER', '서킷 브레이커 발동 — 주문 과다')
    if 'daily trade' in r:
        return ('RISK_LIMIT', '일일 거래 횟수 초과 — 주문 발행 금지')
    if 'hourly trade' in r:
        return ('RISK_LIMIT', '시간당 거래 횟수 초과 — 주문 발행 금지')
    if 'consecutive' in r or 'stop' in r:
        return ('DAILY_LOSS_LIMIT', '연속 손절 자동 중지')
    if 'budget' in r or 'exposure' in r or 'cap' in r:
        return ('CAP_LIMIT', '자본 노출 한도 초과 — 추가 진입 불가')
    return ('UNKNOWN', f'차단 사유: {reason}')


def check_total_exposure(cur, add_usdt, limits=None):
    '''Check if adding add_usdt exceeds capital limit.'''
    if limits is None:
        limits = _load_safety_limits(cur)
    cur.execute('SELECT capital_used_usdt FROM position_state WHERE symbol = %s;', (SYMBOL,))
    row = cur.fetchone()
    current = float(row[0]) if row and row[0] else 0
    cap = limits['capital_limit_usdt'] * limits['trade_budget_pct'] / 100
    if current + add_usdt > cap:
        return (False, f'total exposure would exceed budget ({current + add_usdt:.0f} > {cap:.0f})')
    return (True, 'ok')


def check_pyramid_allowed(cur, current_stage, limits=None):
    '''Check if pyramiding (ADD) is allowed at current stage.'''
    if limits is None:
        limits = _load_safety_limits(cur)
    if current_stage >= limits['max_stages']:
        return (False, f'max stages reached ({current_stage}/{limits["max_stages"]})')
    return (True, 'ok')


def check_trade_budget(cur, add_pct):
    '''Check if adding add_pct exceeds trade budget.'''
    cur.execute('SELECT trade_budget_used_pct FROM position_state WHERE symbol = %s;', (SYMBOL,))
    row = cur.fetchone()
    current_pct = float(row[0]) if row and row[0] else 0
    if current_pct + add_pct > TRADE_BUDGET_PCT:
        return (False, f'budget would exceed {TRADE_BUDGET_PCT}% ({current_pct + add_pct:.0f}%)')
    return (True, 'ok')


def get_add_score_threshold(cur, limits=None):
    '''Get minimum score for ADD orders.'''
    if limits is None:
        limits = _load_safety_limits(cur)
    return limits['add_score_threshold']


def get_add_slice_pct():
    '''Get ADD slice percentage (fixed 10%).'''
    return STAGE_SLICE_PCT


def get_add_slice_usdt(cur, limits=None):
    '''Get ADD slice amount in USDT.'''
    if limits is None:
        limits = _load_safety_limits(cur)
    return limits['capital_limit_usdt'] * STAGE_SLICE_PCT / 100


def compute_start_stage(score):
    '''Compute start stage based on direction score (0-100).
    Higher score -> higher start stage -> larger initial entry.'''
    if score >= 85:
        return 4
    if score >= 75:
        return 3
    if score >= 65:
        return 2
    return 1


def get_stage_entry_pct(start_stage):
    '''Get entry percentage for a given start stage.'''
    return start_stage * STAGE_SLICE_PCT


def get_entry_usdt(cur, start_stage, limits=None):
    '''Get entry USDT amount for a given start stage.'''
    if limits is None:
        limits = _load_safety_limits(cur)
    pct = get_stage_entry_pct(start_stage)
    return limits['capital_limit_usdt'] * pct / 100
