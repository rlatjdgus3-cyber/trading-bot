"""
response_envelope.py — Standardised response formatting for trading facts.

Every fact response includes:
  SOURCE: where the data came from
  DATA_STATUS: OK / ERROR
  RECONCILE: MATCH / MISMATCH / UNKNOWN (when applicable)

Labels follow strict naming to prevent confusion between exchange vs strategy values.
"""
import os

# ── Block Reason Codes (structured, single code per state) ──
BLOCK_REASON_MAP = {
    'SERVICE_STALE': '필수 서비스 이상 — 주문 발행 금지',
    'SWITCH_OFF': '매매 스위치 OFF — 진입 불가',
    'COOLDOWN': '쿨다운 대기 — 주문 간격 미달',
    'ONCE_LOCK': '중복방지 잠금(once_lock) — 주문 발행 금지',
    'WAIT_SIGNAL': '진입 신호 대기 중 — 아직 주문을 만들지 않음',
    'RISK_LIMIT': '리스크 한도 초과 — 주문 발행 금지',
    'DAILY_LOSS_LIMIT': '일일 손실 한도 도달 — 매매 자동 중지',
    'CAP_LIMIT': '자본 노출 한도 초과 — 추가 진입 불가',
    'CIRCUIT_BREAKER': '서킷 브레이커 발동 — 주문 과다',
    'UNKNOWN': '원인 불명 — /debug gate_details 로 확인',
}

# ── Wait Reason Korean mapping ──
WAIT_REASON_KR = {
    'WAIT_SIGNAL': '신호 부족(진입 조건 미충족) — 아직 주문을 만들지 않음',
    'WAIT_GATED': '안전 게이트 차단 — 주문 발행 금지',
    'WAIT_SWITCH': '매매 스위치 OFF — 주문 발행 금지',
    'WAIT_RISK_LOCK': '쿨다운/중복방지 잠금 — 주문 발행 금지',
    'WAIT_ORDER_FILL': '거래소 응답/동기화 대기(주문/포지션 확인중)',
    'WAIT_EXCHANGE_SYNC': '거래소 응답/동기화 대기(주문/포지션 확인중)',
    'WAIT_CAP': '자본 한도 도달 — 추가 진입 불가',
    'WAIT_RATE_LIMIT': '주문 속도 제한 — 시간당/10분 주문 한도 도달',
    'WAIT_MIN_QTY': '최소 주문 수량 미달 — 자본 부족',
    'WAIT_DB_ERROR': 'DB 오류 백오프 — 자동 복구 대기 중',
    'WAIT_REGIME_RANGE': 'RANGE 모드 — 밴드 경계 대기',
    'WAIT_REGIME_BREAKOUT': 'BREAKOUT 모드 — VA 돌파 확인 대기',
    'WAIT_REGIME_SHOCK': 'SHOCK 모드 — 진입 차단',
    'WAIT_REGIME_TRANSITION': '레짐 전환 쿨다운 중',
    'WAIT_COOLDOWN': '포지션 종료 후 쿨다운 대기 (TP/SL)',
    'WAIT_DEDUPED': '동일 존/방향 신호 중복 차단 (5분 버킷)',
    'WAIT_DAILY_LIMIT': '일일 체결 한도 도달',
    'WAIT_LIQUIDITY': '슬리피지/스프레드 불량 — 주문 보류',
    'WAIT_RETRY': 'API 오류 후 재시도 대기',
    'WAIT_COOLDOWN': '최근 손절/청산 이후 쿨다운',
}


def derive_block_reason_code(exec_ctx):
    """Derive a single BLOCK_REASON_CODE from execution context.
    Returns (code, korean_description) tuple."""
    if not exec_ctx:
        return ('UNKNOWN', BLOCK_REASON_MAP['UNKNOWN'])

    # Priority order: switch > gate > once_lock > wait_reason
    if exec_ctx.get('entry_enabled') is False:
        return ('SWITCH_OFF', BLOCK_REASON_MAP['SWITCH_OFF'])

    gate_ok = exec_ctx.get('gate_ok')
    gate_reason = exec_ctx.get('gate_reason', '')
    if gate_ok is False and gate_reason:
        gr = gate_reason.lower()
        if '서비스' in gr or 'service' in gr:
            return ('SERVICE_STALE', BLOCK_REASON_MAP['SERVICE_STALE'])
        if 'daily_loss' in gr or '손실' in gr:
            return ('DAILY_LOSS_LIMIT', BLOCK_REASON_MAP['DAILY_LOSS_LIMIT'])
        if 'circuit' in gr:
            return ('CIRCUIT_BREAKER', BLOCK_REASON_MAP['CIRCUIT_BREAKER'])
        if 'daily trade' in gr or 'hourly trade' in gr:
            return ('RISK_LIMIT', BLOCK_REASON_MAP['RISK_LIMIT'])
        if 'exposure' in gr or 'budget' in gr or 'cap' in gr:
            return ('CAP_LIMIT', BLOCK_REASON_MAP['CAP_LIMIT'])
        if 'consecutive' in gr:
            return ('DAILY_LOSS_LIMIT', BLOCK_REASON_MAP['DAILY_LOSS_LIMIT'])
        return ('RISK_LIMIT', BLOCK_REASON_MAP['RISK_LIMIT'])

    if exec_ctx.get('once_lock'):
        return ('ONCE_LOCK', BLOCK_REASON_MAP['ONCE_LOCK'])

    wait_reason = exec_ctx.get('wait_reason', '')
    if wait_reason == 'WAIT_SIGNAL':
        return ('WAIT_SIGNAL', BLOCK_REASON_MAP['WAIT_SIGNAL'])
    if wait_reason == 'WAIT_RISK_LOCK':
        return ('COOLDOWN', BLOCK_REASON_MAP['COOLDOWN'])
    if wait_reason in ('WAIT_ORDER_FILL', 'WAIT_EXCHANGE_SYNC'):
        return ('ONCE_LOCK', '거래소 응답/동기화 대기(주문/포지션 확인중)')
    if wait_reason == 'WAIT_CAP':
        return ('CAP_LIMIT', BLOCK_REASON_MAP['CAP_LIMIT'])

    return ('WAIT_SIGNAL', BLOCK_REASON_MAP['WAIT_SIGNAL'])

# ── Price Labels ──
EXCH_ENTRY_PRICE = 'exch_entry_price'
EXCH_MARK_PRICE = 'exch_mark_price'
PLANNED_ENTRY_PRICE = 'planned_entry_price'
SIGNAL_TRIGGER_PRICE = 'signal_trigger_price'
ORDER_LIMIT_PRICE = 'order_limit_price'

# ── Quantity Labels ──
EXCH_POS_QTY = 'exch_pos_qty'
PLANNED_STAGE_QTY = 'planned_stage_qty'
ORDER_QTY = 'order_qty'

# ── Status Values ──
# EXCHANGE_POSITION: NONE / LONG / SHORT
# STRATEGY_STATE: FLAT / INTENT_ENTER / IN_POSITION / ADDING / EXITING / BLOCKED
# ORDER_STATE: OPEN / PARTIALLY_FILLED / FILLED / CANCELLED


def format_fact_header(source, data_status, reconcile=None):
    """3-line header for every fact response."""
    lines = [
        f'SOURCE: {source}',
        f'DATA_STATUS: {data_status}',
    ]
    if reconcile is not None:
        lines.append(f'RECONCILE: {reconcile}')
    return '\n'.join(lines)


def format_position_exch(data):
    """Format exchange position card.
    INVARIANT: exchange_position == NONE -> only '포지션 없음', never '보유중'.
    INVARIANT: data_status == ERROR -> no numbers, only UNKNOWN.
    """
    status = data.get('data_status', 'ERROR')
    header = format_fact_header('EXCHANGE', status)

    if status == 'ERROR':
        error = data.get('error', '')
        return (
            f'{header}\n'
            f'━━━━━━━━━━━━━━━━━━\n'
            f'[EXCHANGE] Position\n'
            f'EXCHANGE_POSITION: UNKNOWN\n'
            f'조회 실패: {error}'
        )

    pos = data.get('exchange_position', 'NONE')
    if pos == 'NONE':
        return (
            f'{header}\n'
            f'━━━━━━━━━━━━━━━━━━\n'
            f'[EXCHANGE] Position\n'
            f'EXCHANGE_POSITION: NONE\n'
            f'포지션 없음'
        )

    entry = data.get('exch_entry_price', 0)
    mark = data.get('exch_mark_price', 0)
    qty = data.get('exch_pos_qty', 0)
    upnl = data.get('upnl', 0)
    leverage = data.get('leverage', 0)
    liq = data.get('liq_price', 0)
    pct = 0.0
    if entry > 0 and mark > 0:
        if pos == 'LONG':
            pct = (mark - entry) / entry * 100
        elif pos == 'SHORT':
            pct = (entry - mark) / entry * 100

    return (
        f'{header}\n'
        f'━━━━━━━━━━━━━━━━━━\n'
        f'[EXCHANGE] Position\n'
        f'EXCHANGE_POSITION: {pos}\n'
        f'EXCH_POS_QTY: {qty}\n'
        f'EXCH_ENTRY_PRICE: ${entry:,.2f}\n'
        f'EXCH_MARK_PRICE: ${mark:,.2f}\n'
        f'uPnL: {upnl:+.4f} USDT ({pct:+.2f}%)\n'
        f'leverage: x{leverage:.0f}\n'
        f'liq_price: ${liq:,.2f}'
    )


def format_orders_exch(data):
    """Format open orders card."""
    status = data.get('data_status', 'ERROR')
    header = format_fact_header('EXCHANGE', status)

    if status == 'ERROR':
        error = data.get('error', '')
        return (
            f'{header}\n'
            f'━━━━━━━━━━━━━━━━━━\n'
            f'[EXCHANGE] Open Orders\n'
            f'조회 실패: {error}'
        )

    orders = data.get('orders', [])
    if not orders:
        return (
            f'{header}\n'
            f'━━━━━━━━━━━━━━━━━━\n'
            f'[EXCHANGE] Open Orders\n'
            f'미체결 주문 없음'
        )

    lines = [
        header,
        '━━━━━━━━━━━━━━━━━━',
        f'[EXCHANGE] Open Orders ({len(orders)}건)',
    ]
    for o in orders:
        price = o.get('price', 0)
        amt = o.get('amount', 0)
        filled = o.get('filled', 0)
        lines.append(
            f"  {o.get('side', '?')} {o.get('type', '?')} "
            f"price=${price:,.2f} qty={amt} filled={filled} "
            f"[{o.get('id', '?')}]"
        )
    return '\n'.join(lines)


def format_account_exch(data):
    """Format balance card."""
    status = data.get('data_status', 'ERROR')
    header = format_fact_header('EXCHANGE', status)

    if status == 'ERROR':
        error = data.get('error', '')
        return (
            f'{header}\n'
            f'━━━━━━━━━━━━━━━━━━\n'
            f'[EXCHANGE] Balance\n'
            f'조회 실패: {error}'
        )

    total = data.get('total', 0)
    free = data.get('free', 0)
    used = data.get('used', 0)
    return (
        f'{header}\n'
        f'━━━━━━━━━━━━━━━━━━\n'
        f'[EXCHANGE] Balance (USDT)\n'
        f'total: {total:,.4f}\n'
        f'free:  {free:,.4f}\n'
        f'used:  {used:,.4f}'
    )


def format_position_strat(data):
    """Format strategy DB position card. All values prefixed with '계획/전략'."""
    status = data.get('data_status', 'ERROR')
    header = format_fact_header('STRATEGY_DB', status)

    if status == 'ERROR':
        error = data.get('error', '')
        return (
            f'{header}\n'
            f'━━━━━━━━━━━━━━━━━━\n'
            f'[STRATEGY_DB] Strategy Position\n'
            f'STRATEGY_STATE: UNKNOWN\n'
            f'조회 실패: {error}'
        )

    state = data.get('strategy_state', 'FLAT')
    if state == 'FLAT':
        return (
            f'{header}\n'
            f'━━━━━━━━━━━━━━━━━━\n'
            f'[STRATEGY_DB] Strategy Position\n'
            f'STRATEGY_STATE: FLAT\n'
            f'전략 포지션 없음'
        )

    side = data.get('side', '?')
    qty = data.get('planned_stage_qty', 0)
    avg = data.get('avg_entry_price', 0)
    stage = data.get('stage', 0)
    cap_used = data.get('capital_used_usdt', 0)
    budget_pct = data.get('trade_budget_used_pct', 0)

    lines = [
        f'{header}',
        '━━━━━━━━━━━━━━━━━━',
        '[STRATEGY_DB] Strategy Position',
        f'STRATEGY_STATE: {state}',
        f'(전략) side: {side}',
        f'PLANNED_STAGE_QTY: {qty}',
        f'PLANNED_ENTRY_PRICE: ${avg:,.2f}',
        f'stage: {stage}',
        f'capital_used: {cap_used:,.2f} USDT ({budget_pct:.1f}%)',
    ]

    # P4: planned/sent/filled 3-tier display when order_state != FILLED
    order_state = data.get('order_state', '')
    if order_state and order_state not in ('FILLED', 'NONE', ''):
        planned_qty = data.get('planned_qty', 0)
        filled_qty = data.get('filled_qty', 0)
        planned_usdt = data.get('planned_usdt', 0)
        filled_usdt = data.get('filled_usdt', 0)
        lines.append(f'order_state: {order_state}')
        lines.append(f'  계획: {planned_qty} BTC / {planned_usdt:,.2f} USDT')
        lines.append(f'  체결: {filled_qty} BTC / {filled_usdt:,.2f} USDT')
    if stage == 0:
        lines.append('※ stage=0: 계획 단계 — 실제 수량 아님')

    return '\n'.join(lines)


def format_risk_config(limits):
    """Format safety limits / risk config card."""
    if not limits:
        return (
            'SOURCE: SAFETY_DB\n'
            'DATA_STATUS: ERROR\n'
            '━━━━━━━━━━━━━━━━━━\n'
            '[RISK] Caps & Limits\n'
            '조회 실패'
        )
    return (
        'SOURCE: SAFETY_DB\n'
        'DATA_STATUS: OK\n'
        '━━━━━━━━━━━━━━━━━━\n'
        '[RISK] Caps & Limits\n'
        f"capital_limit: {limits.get('capital_limit_usdt', 0):,.0f} USDT\n"
        f"trade_budget: {limits.get('trade_budget_pct', 70)}%\n"
        f"stage_slice: {limits.get('stage_slice_pct', 10)}%\n"
        f"max_stages: {limits.get('max_stages', 7)}\n"
        f"max_daily_trades: {limits.get('max_daily_trades', 20)}\n"
        f"max_hourly_trades: {limits.get('max_hourly_trades', 15)}\n"
        f"daily_loss_limit: {limits.get('daily_loss_limit_usdt', -45)} USDT\n"
        f"stop_loss: {limits.get('stop_loss_pct', 2.0)}%\n"
        f"circuit_breaker: {limits.get('circuit_breaker_max_orders', 10)} orders / "
        f"{limits.get('circuit_breaker_window_sec', 300)}s"
    )


def format_fact_snapshot(exch_pos, strat_pos, orders, exec_ctx=None):
    """Korean 4-section fact snapshot for natural-language auto-fetch.

    Sections:
      [요약] — MISMATCH warning + 1-line cause (if applicable)
      1) 거래소(EXCHANGE, 체결 기준)
      2) 주문 상태(ORDER)
      3) 전략(DB, 내부 기록)
      4) 실행상태(GATE/WAIT)

    INVARIANT: data_status == ERROR → UNKNOWN only, no fabricated numbers.
    INVARIANT: EXCHANGE and DB data never mixed in single section.
    INVARIANT: EXCHANGE_POSITION == NONE → never '보유중' or '체결 완료' 단정.
    INVARIANT: "체결가" = EXCHANGE only. "기준가(DB)" = STRATEGY_DB only.
    """
    if exec_ctx is None:
        exec_ctx = {}

    import exchange_reader
    recon = 'UNKNOWN'
    recon_result = None
    if exch_pos and strat_pos:
        recon_result = exchange_reader.reconcile(exch_pos, strat_pos)
        recon = recon_result['legacy'] if isinstance(recon_result, dict) else recon_result

    exch_ok = exch_pos and exch_pos.get('data_status') == 'OK'
    exch_position = exch_pos.get('exchange_position', 'UNKNOWN') if exch_ok else 'UNKNOWN'
    strat_ok = strat_pos and strat_pos.get('data_status') == 'OK'
    strat_state = strat_pos.get('strategy_state', 'FLAT') if strat_ok else 'UNKNOWN'
    strat_side = (strat_pos.get('side') or '').upper() if strat_ok else ''

    orders_ok = orders and orders.get('data_status') == 'OK'
    open_orders = orders.get('orders', []) if orders_ok else []
    has_partial = any(float(o.get('filled', 0)) > 0 for o in open_orders)

    sections = []

    # ── Top badge: ENTRY/GATE/WAIT status ──
    entry_enabled = exec_ctx.get('entry_enabled')
    gate_ok_val = exec_ctx.get('gate_ok')
    wait_reason_val = exec_ctx.get('wait_reason', '')
    if entry_enabled is False:
        sections.append('🔴 EXECUTION DISABLED (trade_switch OFF) — 권고는 참고용, 실행 안 함')
    elif gate_ok_val is False:
        sections.append('🟡 GATE BLOCKED')
    elif wait_reason_val and wait_reason_val not in ('', 'N/A'):
        sections.append(f'🟢 {wait_reason_val}')
    else:
        sections.append('🟢 READY')

    sections.append('[요약]')

    # ── MISMATCH Warning (first line) ──
    if recon == 'MISMATCH':
        if exch_position == 'NONE' and strat_state not in ('FLAT', 'UNKNOWN'):
            cause_parts = []
            if len(open_orders) > 0:
                cause_parts.append('미체결 주문 대기중')
            else:
                cause_parts.append('주문 미발행(게이트/로직 차단)')
            gate_reason = exec_ctx.get('gate_reason', '')
            wait_reason = exec_ctx.get('wait_reason', '')
            if gate_reason and gate_reason != 'all checks passed':
                cause_parts.append(f'gate={gate_reason}')
            if wait_reason:
                cause_parts.append(f'wait={wait_reason}')
            cause = ' / '.join(cause_parts)
            sections.append(
                f'⚠ 체결 미확인 — 거래소=NONE / 전략DB={strat_state} {strat_side}'
            )
            sections.append(f'→ 원인: {cause}')
        elif exch_position != 'NONE' and strat_state == 'FLAT':
            sections.append(
                f'⚠ 불일치 — 거래소={exch_position} / 전략DB=FLAT'
            )
            sections.append('→ DB가 포지션을 인식 못함 (fill_watcher 확인 필요)')
        else:
            sections.append(
                f'⚠ RECONCILE=MISMATCH (거래소={exch_position} ≠ 전략DB={strat_state} {strat_side})'
            )
    elif recon == 'UNKNOWN':
        sections.append('⚠ 조회 일부 실패 — 데이터 불완전')

    # Trade switch OFF banner
    if exec_ctx.get('entry_enabled') is False:
        sections.append('⚠ 매매 중지: trade_switch OFF → 진입 불가')

    sections.append('')

    # ── 1) 거래소(EXCHANGE, 체결 기준) ──
    sections.append('1) 거래소(EXCHANGE, 체결 기준)')
    if not exch_ok:
        error = (exch_pos or {}).get('error', 'exchange_timeout')
        sections.append('   - 포지션: UNKNOWN')
        sections.append(f'   - DATA_STATUS: ERROR ({error})')
    elif exch_position == 'NONE':
        sections.append('   - 포지션: NONE (실포지션 없음)')
        sections.append(f'   - 미체결주문: {len(open_orders)}건')
    else:
        qty = exch_pos.get('exch_pos_qty', 0)
        entry = exch_pos.get('exch_entry_price', 0)
        mark = exch_pos.get('exch_mark_price', 0)
        upnl = exch_pos.get('upnl', 0)
        leverage = exch_pos.get('leverage', 0)
        liq = exch_pos.get('liq_price', 0)
        sections.append(f'   - 포지션: {exch_position} (체결 확인됨)')
        sections.append(f'   - 수량: {qty}')
        sections.append(f'   - 체결가(EXCHANGE): ${entry:,.2f}')
        sections.append(f'   - 마크가: ${mark:,.2f}')
        sections.append(f'   - uPnL: {upnl:+.4f} USDT')
        sections.append(f'   - 레버리지: x{leverage:.0f}')
        sections.append(f'   - 청산가: ${liq:,.2f}')
        sections.append(f'   - 미체결주문: {len(open_orders)}건')

    sections.append('')

    # ── 2) 주문 상태(ORDER) ──
    sections.append('2) 주문 상태(ORDER)')
    if not orders_ok:
        error = (orders or {}).get('error', 'exchange_timeout')
        sections.append(f'   - 상태: UNKNOWN ({error})')
    elif not open_orders:
        # No open orders — check exec_queue for recent pending
        eq = exec_ctx.get('recent_exec_queue', [])
        pending = [e for e in eq if e.get('status') in ('PENDING', 'SUBMITTED')]
        if pending:
            p = pending[0]
            sections.append(
                f"   - 상태: ORDER_PLACED (실행큐 대기)")
            sections.append(
                f"   - #{p['id']} {p.get('action','')} {p.get('direction','')} "
                f"${p.get('usdt',0):.0f} ({p.get('ts','')})")
            sections.append('   ※ 거래소 미전송 또는 미체결')
        else:
            sections.append('   - 상태: 없음 (활성 주문 없음)')
    else:
        if has_partial:
            sections.append(f'   - 상태: PARTIAL_FILL ({len(open_orders)}건 부분체결)')
            sections.append('   ※ 부분체결 — 체결 완료 아님')
        else:
            sections.append(f'   - 상태: OPEN_ORDER ({len(open_orders)}건 미체결 대기)')
        for o in open_orders[:3]:
            price = o.get('price', 0)
            amt = o.get('amount', 0)
            filled = o.get('filled', 0)
            sections.append(
                f"   - {o.get('side','?')} {o.get('type','?')} "
                f"${price:,.2f} qty={amt} filled={filled} [{o.get('id','?')}]"
            )

    sections.append('')

    # ── 3) PLAN(DB, 내부 기록) ──
    sections.append('3) PLAN(DB, 내부 기록 — 체결 확정 아님)')
    if not strat_ok:
        error = (strat_pos or {}).get('error', 'db_error')
        sections.append(f'   - 상태: UNKNOWN ({error})')
    elif strat_state in ('FLAT', 'PLAN.NONE'):
        sections.append('   - 상태: PLAN.NONE (전략 포지션 없음)')
    else:
        side = strat_pos.get('side', '?')
        qty = strat_pos.get('planned_stage_qty', 0)
        avg = strat_pos.get('avg_entry_price', 0)
        stage = strat_pos.get('stage', 0)
        cap_used = strat_pos.get('capital_used_usdt', 0)
        budget_pct = strat_pos.get('trade_budget_used_pct', 0)
        # Show PLAN.* state with intent/filled hint
        display_state = strat_state
        intent_hint = ''
        if strat_state.startswith('PLAN.INTENT_') and exch_position == 'NONE':
            intent_hint = ' <- 전략 신호 (미체결)'
        elif strat_state.startswith('PLAN.OPEN') and exch_position in ('LONG', 'SHORT'):
            intent_hint = ' <- 체결 확인(FILLED)'
        elif strat_state.startswith('PLAN.OPEN') and exch_position == 'NONE':
            intent_hint = ' <- DB 기록만 (거래소 미확인)'
        sections.append(f'   - 상태: {display_state}{intent_hint}')
        sections.append(f'   - 방향: {side.upper() if side else "?"}')
        sections.append(f'   - 수량: {qty}')
        sections.append(f'   - 기준가(DB): ${avg:,.2f}')
        sections.append(f'   - stage: {stage}')
        sections.append(f'   - 자본사용: {cap_used:,.2f} USDT ({budget_pct:.1f}%)')
        # P4: planned vs filled display
        order_state_val = strat_pos.get('order_state', '')
        if order_state_val and order_state_val not in ('FILLED', 'NONE', ''):
            planned_qty = strat_pos.get('planned_qty', 0)
            filled_qty_val = strat_pos.get('filled_qty', 0)
            sections.append(f'   - 계획: {planned_qty} BTC / 체결: {filled_qty_val} BTC')
        if stage == 0:
            sections.append('   ※ stage=0: 계획 단계 — 실제 수량 아님')
        sections.append('   - SOURCE: STRATEGY_DB')
        if recon == 'MISMATCH' and exch_position == 'NONE':
            sections.append('   ※ 전략 내부 기록일 뿐 — 실제 체결/보유 아님')

    sections.append('')

    # ── 4) 실행상태(GATE/WAIT) ──
    sections.append('4) 실행상태(GATE/WAIT)')
    gate_ok = exec_ctx.get('gate_ok')
    gate_reason = exec_ctx.get('gate_reason', 'N/A')
    wait_reason = exec_ctx.get('wait_reason', 'N/A')
    entry_enabled = exec_ctx.get('entry_enabled')
    once_lock = exec_ctx.get('once_lock', False)
    once_lock_ttl = exec_ctx.get('once_lock_ttl')
    test_mode = exec_ctx.get('test_mode', False)
    test_mode_end = exec_ctx.get('test_mode_end', '')
    eq_limits = exec_ctx.get('equity_limits')
    if eq_limits:
        cap_limit = eq_limits.get('operating_cap', 900)
        equity_val = eq_limits.get('equity', 0)
        budget_pct = eq_limits.get('operating_ratio', 0.70) * 100
        max_stages = eq_limits.get('max_stages', 7)
        cap_source = eq_limits.get('source', 'exchange').upper()
    else:
        cap_limit = exec_ctx.get('capital_limit', 900)
        equity_val = 0
        budget_pct = exec_ctx.get('trade_budget_pct', 70)
        max_stages = exec_ctx.get('max_stages', 7)
        cap_source = 'FALLBACK'
    live_trading = exec_ctx.get('live_trading', False)

    if gate_ok is True:
        sections.append('   - gate: PASS')
    elif gate_ok is False:
        sections.append(f'   - gate: BLOCKED ({gate_reason})')
    else:
        sections.append(f'   - gate: UNKNOWN ({gate_reason})')
    # Wait reason with Korean mapping
    wait_kr = WAIT_REASON_KR.get(wait_reason, wait_reason)
    sections.append(f'   - wait_reason: {wait_reason}')
    if wait_kr != wait_reason:
        sections.append(f'     → {wait_kr}')

    entry_str = 'ON' if entry_enabled else ('OFF' if entry_enabled is not None else 'UNKNOWN')
    sections.append(f'   - entry_enabled: {entry_str}')
    sections.append(f'   - exit_enabled: 항상ON')

    if once_lock:
        sections.append(f'   - once_lock: 있음 (TTL: {once_lock_ttl or "?"})')
    else:
        sections.append('   - once_lock: 없음')

    if test_mode:
        end_str = f' (종료: {test_mode_end})' if test_mode_end else ''
        sections.append(f'   - test_mode: ON{end_str}')
    else:
        sections.append('   - test_mode: OFF')

    sections.append(f'   - LIVE_TRADING: {"YES" if live_trading else "NO"}')
    if eq_limits and equity_val > 0:
        sections.append(f'   - 자본한도: {cap_limit:,.0f} USDT (= equity {equity_val:,.0f} x {budget_pct:.0f}%) / {max_stages}단계 [{cap_source}]')
    else:
        sections.append(f'   - 자본한도: {cap_limit:,.0f} USDT / {budget_pct:.0f}% / {max_stages}단계 [{cap_source}]')

    # Last fill info
    last_fill = exec_ctx.get('last_fill')
    if last_fill:
        sections.append(
            f"   - 마지막체결: #{last_fill['id']} "
            f"{last_fill.get('direction','')} {last_fill.get('status','')} "
            f"qty={last_fill.get('qty',0)} ({last_fill.get('ts','')})"
        )

    sections.append('')

    # ── 5) 시장 환경(MCTX) ──
    try:
        from db_config import get_conn as _get_conn
        _mctx_conn = _get_conn(autocommit=True)
        with _mctx_conn.cursor() as _mctx_cur:
            import regime_reader
            _rctx = regime_reader.get_current_regime(_mctx_cur)
        _mctx_conn.close()
        if _rctx.get('available'):
            _regime = _rctx.get('regime', 'UNKNOWN')
            _rparams = regime_reader.get_regime_params(_regime, _rctx.get('shock_type'))
            _tp_mode = _rparams.get('tp_mode', 'fixed')
            _sl = _rparams.get('sl_pct', 2.0)
            _lmin = _rparams.get('leverage_min', 3)
            _lmax = _rparams.get('leverage_max', 8)
            sections.append('5) 시장 환경(MCTX)')
            sections.append(f'   - 레짐: {_regime} (confidence={_rctx.get("confidence", 0)})')
            sections.append(f'   - flow: {_rctx.get("flow_bias", 0):+.1f} | '
                           f'ADX: {_rctx["adx_14"]:.1f}' if _rctx.get("adx_14") is not None
                           else f'   - flow: {_rctx.get("flow_bias", 0):+.1f} | ADX: N/A')
            sections.append(f'   - 모드: {_tp_mode} TP / {_sl}% SL / {_lmin}-{_lmax}x')
            if _rctx.get('in_transition'):
                sections.append('   - ⚠ 레짐 전환 쿨다운 중')
            sections.append('')
    except Exception:
        pass  # MCTX section is optional

    # ── Conclusion with BLOCK_REASON_CODE ──
    sections.append('')
    entry_enabled = exec_ctx.get('entry_enabled')
    if exch_position not in ('NONE', 'UNKNOWN'):
        # EXCHANGE position confirmed
        sections.append(f'결론: {exch_position} 포지션 보유 중 — 체결 확인됨')
    elif open_orders:
        sections.append('결론: 주문 발행됨 — 미체결 대기 중')
    elif exch_position == 'NONE' and strat_state not in ('FLAT', 'UNKNOWN'):
        # Exchange NONE but DB has intent → show block reason
        block_code, block_kr = derive_block_reason_code(exec_ctx)
        sections.append(f'결론: BLOCK_REASON_CODE: {block_code} — {block_kr}')
    elif exch_position == 'NONE' and not open_orders:
        if entry_enabled is False:
            sections.append('결론: 실포지션 없음 + 주문 없음 → 매매 중지 상태 (trade_switch OFF)')
        elif wait_reason == 'WAIT_SIGNAL':
            sections.append('결론: 대기 중 — 신호 부족')
        else:
            wait_kr = WAIT_REASON_KR.get(wait_reason, wait_reason)
            sections.append(f'결론: 대기 중 — {wait_kr}')

    return '\n'.join(sections)


def format_snapshot(exch_pos, strat_pos, orders, gate_status, switch_status, wait_reason,
                    capital_info=None, zone_check=None):
    """Format composite snapshot card with optional capital/leverage/zone info."""
    recon = None
    recon_legacy = None
    if exch_pos and strat_pos:
        import exchange_reader
        recon_result = exchange_reader.reconcile(exch_pos, strat_pos)
        recon = recon_result['legacy'] if isinstance(recon_result, dict) else recon_result
        recon_legacy = recon

    sections = []

    # ── Top badge: ENTRY/GATE/WAIT status ──
    if switch_status is False:
        sections.append('🔴 EXECUTION DISABLED (trade_switch OFF) — 권고는 참고용, 실행 안 함')
    elif gate_status and not gate_status[0]:
        sections.append('🟡 GATE BLOCKED')
    elif wait_reason and wait_reason not in ('', 'N/A'):
        sections.append(f'🟢 {wait_reason}')
    else:
        sections.append('🟢 READY')

    # Header
    exch_status = exch_pos.get('data_status', 'ERROR') if exch_pos else 'ERROR'
    strat_status = strat_pos.get('data_status', 'ERROR') if strat_pos else 'ERROR'
    overall = 'OK' if exch_status == 'OK' and strat_status == 'OK' else 'PARTIAL'
    header = format_fact_header('COMPOSITE', overall, recon)
    sections.append(header)
    sections.append('━━━━━━━━━━━━━━━━━━')

    # Trade switch OFF banner
    if switch_status is False:
        sections.append('⚠ 매매 중지: trade_switch OFF → 진입 불가')

    # 1. [EXCHANGE] Position
    if exch_pos:
        pos = exch_pos.get('exchange_position', 'UNKNOWN')
        if exch_pos.get('data_status') == 'ERROR':
            sections.append('[EXCHANGE] Position: UNKNOWN (ERROR)')
        elif pos == 'NONE':
            sections.append('[EXCHANGE] Position: NONE')
        else:
            qty = exch_pos.get('exch_pos_qty', 0)
            entry = exch_pos.get('exch_entry_price', 0)
            mark = exch_pos.get('exch_mark_price', 0)
            upnl = exch_pos.get('upnl', 0)
            sections.append(
                f'[EXCHANGE] Position: {pos} qty={qty} '
                f'entry=${entry:,.2f} mark=${mark:,.2f} uPnL={upnl:+.4f}'
            )
    else:
        sections.append('[EXCHANGE] Position: N/A')

    # 2. [EXCHANGE] Open Orders
    if orders:
        ol = orders.get('orders', [])
        if orders.get('data_status') == 'ERROR':
            sections.append('[EXCHANGE] Orders: ERROR')
        elif not ol:
            sections.append('[EXCHANGE] Orders: none')
        else:
            sections.append(f'[EXCHANGE] Orders: {len(ol)}건')
            for o in ol[:5]:
                sections.append(
                    f"  {o.get('side')} {o.get('type')} "
                    f"${o.get('price', 0):,.2f} qty={o.get('amount', 0)}"
                )
    else:
        sections.append('[EXCHANGE] Orders: N/A')

    # 3. [STRATEGY_DB] Strategy Position
    if strat_pos:
        state = strat_pos.get('strategy_state', 'UNKNOWN')
        if strat_pos.get('data_status') == 'ERROR':
            sections.append('[STRATEGY_DB] State: UNKNOWN (ERROR)')
        elif state == 'FLAT':
            sections.append('[STRATEGY_DB] State: FLAT')
        else:
            side = strat_pos.get('side', '?')
            qty = strat_pos.get('planned_stage_qty', 0)
            stage = strat_pos.get('stage', 0)
            # INTENT_ENTER + exchange=NONE → 체결 확정 아님 표기
            exch_position = exch_pos.get('exchange_position', 'UNKNOWN') if exch_pos else 'UNKNOWN'
            note = ''
            if state == 'INTENT_ENTER' and exch_position == 'NONE':
                note = ' <- 전략 신호 (미체결)'
            elif state == 'OPEN' and exch_position in ('LONG', 'SHORT'):
                note = ' <- 체결 확인(FILLED)'
            elif state == 'OPEN' and exch_position == 'NONE':
                note = ' <- DB 기록만 (거래소 미확인)'
            sections.append(
                f'[STRATEGY_DB] State: {state} {side} qty={qty} stage={stage}{note}'
            )
    else:
        sections.append('[STRATEGY_DB] State: N/A')

    # 4. [CAPITAL] Equity & Budget
    if capital_info:
        eq = capital_info.get('equity', 0)
        op = capital_info.get('operating_cap', 0)
        res = capital_info.get('reserve', 0)
        ratio = capital_info.get('operating_ratio', 0.70)
        max_stg = capital_info.get('max_stages', 7)
        slc = capital_info.get('slice_usdt', 0)
        used = capital_info.get('used_usdt', 0)
        remaining = capital_info.get('remaining_usdt', 0)
        source = capital_info.get('source', '?').upper()
        pct_int = int(ratio * 100)
        sections.append(
            f'[CAPITAL]\n'
            f'  - equity_total: {eq:,.2f} USDT ({source})\n'
            f'  - Operating Cap: {op:,.2f} USDT (equity {eq:,.0f} * {pct_int}%)\n'
            f'  - Stage Slice: {slc:,.2f} USDT (cap / {max_stg} stages)\n'
            f'  - reserve_cap: {res:,.2f} USDT\n'
            f'  - used_cap: {used:,.2f} USDT\n'
            f'  - remaining_cap: {remaining:,.2f} USDT'
        )
        # Leverage
        lev = capital_info.get('leverage_current', 0)
        lev_rule = capital_info.get('leverage_rule', '?')
        sections.append(f'[LEVERAGE] current={lev}x | rule={lev_rule}')
        # Stage
        stage = capital_info.get('stage', 0)
        sections.append(f'[STAGE] Position: {stage}/{max_stg} (capital used: {used:,.2f}/{op:,.2f} USDT)')
    else:
        sections.append(f'[RISK] (use /risk_config for details)')

    # 5. [GATE] Pre-Live Safety
    if gate_status:
        ok, reason = gate_status
        if ok and reason and 'WARN' in str(reason):
            gate_str = f'WARN ({reason})'
        elif ok:
            gate_str = 'PASS'
        else:
            gate_str = f'BLOCKED ({reason})'
    else:
        gate_str = 'N/A'
    sections.append(f'[GATE] Safety: {gate_str}')

    # 5.5 [ZONE_CHECK] Zone & Filter Status
    if zone_check:
        zc_lines = ['[ZONE_CHECK]']
        zc_price = zone_check.get('current_price', 0)
        zc_lines.append(f'  current_price: ${zc_price:,.1f}')
        dist_val = zone_check.get('dist_to_val_pct')
        dist_vah = zone_check.get('dist_to_vah_pct')
        dist_poc = zone_check.get('dist_to_poc_pct')
        if dist_val is not None:
            zc_lines.append(f'  dist_to_VAL: {dist_val:+.2f}%')
        if dist_vah is not None:
            zc_lines.append(f'  dist_to_VAH: {dist_vah:+.2f}%')
        if dist_poc is not None:
            zc_lines.append(f'  dist_to_POC: {dist_poc:+.2f}%')
        zc_lines.append(f'  in_long_zone: {"YES" if zone_check.get("in_long_zone") else "NO"}')
        zc_lines.append(f'  in_short_zone: {"YES" if zone_check.get("in_short_zone") else "NO"}')
        if zone_check.get('in_mid_zone_ban'):
            zc_lines.append(f'  MID_ZONE_BAN: YES ({zone_check.get("zone_detail", "").strip()})')
        if zone_check.get('chase_block'):
            zc_lines.append(f'  CHASE_BLOCK: {zone_check["chase_block"]}')
        sections.append('\n'.join(zc_lines))

    # 5.6 [THROTTLE] Order Throttle (renamed from GUARD)
    if capital_info:
        try:
            import order_throttle
            ts = order_throttle.get_throttle_status()
            guard_lines = ['[THROTTLE] Order Throttle']
            guard_lines.append(f'  attempts: {ts["hourly_count"]}/{ts["hourly_limit"]} (1h) | '
                              f'{ts["10min_count"]}/{ts["10min_limit"]} (10m)')
            # Daily trade count (FILLED)
            if zone_check and zone_check.get('daily_trade_count') is not None:
                guard_lines.append(f'  daily_trade_count(FILLED): {zone_check["daily_trade_count"]}/60')
            if ts.get('entry_locked'):
                guard_lines.append(f'  ENTRY LOCKED: {ts["lock_reason"]} -> {ts["lock_expires_str"]}')
            if ts.get('last_reject'):
                guard_lines.append(f'  last_reject: {ts["last_reject"][:80]}')
                guard_lines.append(f'  last_reject_ts: {ts.get("last_reject_ts_str", "?")}')
            cd_active = [f'{k}={v:.0f}s' for k, v in ts.get('cooldowns', {}).items() if v > 0]
            if cd_active:
                guard_lines.append(f'  cooldowns: {", ".join(cd_active)}')
            # WAIT_REASON detail
            if wait_reason and wait_reason not in ('', 'N/A'):
                wait_kr = WAIT_REASON_KR.get(wait_reason, wait_reason)
                guard_lines.append(f'  WAIT_REASON: {wait_reason} — {wait_kr}')
            # Next order allowed
            try:
                next_ts, next_reason = order_throttle.get_next_try_ts()
                if next_ts > 0:
                    from datetime import datetime, timezone
                    next_str = datetime.fromtimestamp(next_ts, tz=timezone.utc).strftime('%H:%M:%S UTC')
                    guard_lines.append(f'  next_order_allowed_at: {next_str} ({next_reason})')
                else:
                    guard_lines.append(f'  next_order_allowed_at: NOW')
            except Exception:
                pass
            sections.append('\n'.join(guard_lines))
        except Exception:
            pass

    # 5.7 [LAST ACTION] Signal suppression info
    if zone_check and zone_check.get('signal_policy'):
        sp = zone_check['signal_policy']
        la_lines = ['[LAST ACTION]']
        suppress = sp.get('last_suppress_reason', 'none')
        suppress_ts = sp.get('last_suppress_ts', 0)
        la_lines.append(f'  last_suppress_reason: {suppress}')
        if suppress_ts:
            try:
                from datetime import datetime, timezone
                ts_str = datetime.fromtimestamp(suppress_ts, tz=timezone.utc).strftime('%H:%M:%S UTC')
                la_lines.append(f'  last_suppress_ts: {ts_str}')
            except Exception:
                la_lines.append(f'  last_suppress_ts: {suppress_ts}')
        for d in ('LONG', 'SHORT'):
            cd_rem = sp.get(f'cooldown_{d}_remaining', 0)
            if cd_rem > 0:
                la_lines.append(f'  cooldown_{d}: {cd_rem}s remaining')
        sections.append('\n'.join(la_lines))

    # 6. [SWITCH] Entry/Exit + WAIT_REASON
    if switch_status is not None:
        entry_str = 'ON' if switch_status else 'OFF'
    else:
        entry_str = 'UNKNOWN'
    live_env = os.getenv('LIVE_TRADING', '')
    live_str = 'YES' if live_env == 'YES_I_UNDERSTAND' else 'NO'
    sections.append(
        f'[SWITCH] entry_enabled={entry_str} | exit_enabled=ON | '
        f'LIVE_TRADING={live_str}'
    )
    wait_kr = WAIT_REASON_KR.get(wait_reason, wait_reason) if wait_reason else 'N/A'
    sections.append(f'WAIT_REASON: {wait_reason or "N/A"}')
    if wait_kr != (wait_reason or 'N/A'):
        sections.append(f'  → {wait_kr}')

    # 1-line conclusion
    exch_position = exch_pos.get('exchange_position', 'UNKNOWN') if exch_pos else 'UNKNOWN'
    open_orders = orders.get('orders', []) if orders and orders.get('data_status') == 'OK' else []
    if exch_position not in ('NONE', 'UNKNOWN'):
        sections.append(f'결론: {exch_position} 포지션 보유 중 — 체결 확인됨')
    elif open_orders:
        sections.append('결론: 주문 발행됨 — 미체결 대기 중')
    elif exch_position == 'NONE' and not open_orders:
        if switch_status is False:
            sections.append('결론: 실포지션 없음 + 주문 없음 → 매매 중지 상태 (trade_switch OFF)')
        elif wait_reason == 'WAIT_SIGNAL':
            sections.append('결론: 대기 중 — 신호 부족')
        else:
            sections.append(f'결론: 대기 중 — {wait_kr}')

    return '\n'.join(sections)
