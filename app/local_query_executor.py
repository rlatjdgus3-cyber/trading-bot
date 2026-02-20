# Source Generated with Decompyle++
# File: local_query_executor.cpython-312.pyc (Python 3.12)

'''
Execute local queries that require NO LLM calls.
All data comes from DB, ccxt API, or systemd.
'''
import os
import re
import subprocess
import time as _time
from db_config import get_conn, DB_CONFIG
import exchange_reader
import response_envelope

def _log(msg):
    print(f'[local_query] {msg}', flush=True)

_process_start_time = _time.time()

SYMBOL = os.getenv('SYMBOL', 'BTC/USDT:USDT')
APP_DIR = '/root/trading-bot/app'
REQUIRED_SERVICES = [
    'candles',
    'executor',
    'indicators',
    'news_bot',
    'pnl_watcher']
OPTIONAL_SERVICES = [
    'signal_logger',
    'vol_profile',
    'error_watcher']
WATCHED_SERVICES = REQUIRED_SERVICES + OPTIONAL_SERVICES
SERVICE_NAMES_KO = {
    'candles': '캔들 수집',
    'executor': '실행기(컨트롤러)',
    'indicators': '지표 계산',
    'news_bot': '뉴스 수집',
    'signal_logger': '시그널 기록',
    'vol_profile': '볼륨 프로파일',
    'error_watcher': '에러 감시',
    'pnl_watcher': '손익 감시'}

def _db():
    return get_conn(autocommit=True)


def _run(cmd, timeout=25):
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    out = (p.stdout or '') + (p.stderr or '')
    return (p.returncode, out.strip())


def execute(query_type=None, original_text=None):
    handlers = {
        'status_full': _status_full,
        'health_check': _health_check,
        'btc_price': _btc_price,
        'news_summary': _news_summary,
        'equity_report': _equity_report,
        'daily_report': _daily_report,
        'recent_errors': _recent_errors,
        'indicator_snapshot': _indicator_snapshot,
        'volatility_summary': _volatility_summary,
        'position_info': _position_exch,
        'position_exch': _position_exch,
        'orders_exch': _orders_exch,
        'account_exch': _account_exch,
        'position_strat': _position_strat,
        'risk_config': _risk_config,
        'snapshot': _snapshot,
        'fact_snapshot': _fact_snapshot,
        'score_summary': _score_summary,
        'db_health': _db_health,
        'claude_audit': _claude_audit,
        'macro_summary': _macro_summary,
        'db_monthly_stats': _db_monthly_stats,
        'audit_report': _audit_report,
        'news_applied': _news_applied,
        'news_ignored': _news_ignored,
        'db_coverage': _db_coverage,
        'evidence': _evidence,
        'test_report_full': _test_report_full,
        'debug_version': _debug_version,
        'debug_router': _debug_router,
        'debug_health': _debug_health,
        'debug_db_coverage': _debug_db_coverage,
        'debug_news_sample': _debug_news_sample,
        'debug_news_reaction_sample': _debug_news_reaction_sample,
        'debug_backfill_status': _debug_backfill_status,
        'debug_backfill_dryrun': _debug_backfill_dryrun,
        'debug_state': _debug_state,
        'debug_news_filter_stats': _debug_news_filter_stats,
        'debug_backfill_enable': _debug_backfill_enable,
        'debug_backfill_start': _debug_backfill_start,
        'debug_backfill_pause': _debug_backfill_pause,
        'debug_backfill_resume': _debug_backfill_resume,
        'debug_backfill_stop': _debug_backfill_stop,
        'debug_backfill_log': _debug_backfill_log,
        'debug_news_gap_diagnosis': _debug_news_gap_diagnosis,
        'debug_storage': _debug_storage,
        'debug_news_path_sample': _debug_news_path_sample,
        'debug_news_path_stats': _debug_news_path_stats,
        'debug_system_stability': _debug_system_stability,
        'debug_once_lock_status': _debug_once_lock_status,
        'debug_once_lock_clear': _debug_once_lock_clear,
        'debug_backfill_ack': _debug_backfill_ack,
        'debug_gate_details': _debug_gate_details,
        'debug_order_throttle': _debug_order_throttle,
        'reconcile': _reconcile,
        'mctx_status': _mctx_status,
        'mode_params': _mode_params,
        'combined_snapshot': _combined_snapshot,
        'mode_performance': _mode_performance}
    handler = handlers.get(query_type, _unknown)
    return handler(original_text)


def _status_full(_text=None):
    (rc, out) = _run([
        'python3',
        f'{APP_DIR}/status_full.py'], timeout=35)
    if rc != 0:
        return f'⚠ status_full 실패(rc={rc})\n{out[-3500:]}'
    if len(out) > 3500:
        return out[-3500:]
    return out


def _classify_service_state(line, found):
    """Classify service state to OK/DOWN/UNKNOWN."""
    if not found:
        return 'UNKNOWN'
    ll = line.lower()
    if 'active running' in ll:
        return 'OK'
    if 'failed' in ll or 'dead' in ll:
        return 'DOWN'
    if 'inactive' in ll:
        return 'DOWN'
    if 'masked' in ll or 'activating' in ll:
        return 'UNKNOWN'
    return 'UNKNOWN'

STATE_ICONS = {'OK': '✔', 'DOWN': '❌', 'UNKNOWN': '❓'}
STATE_KR = {'OK': '정상', 'DOWN': '중지', 'UNKNOWN': '미확인'}


def _health_check(_text=None):
    (rc, out) = _run([
        'systemctl',
        'list-units',
        '--type=service'])
    if rc != 0:
        return '⚠ 서비스 상태 조회 실패'
    status_lines = []
    states = {}  # svc -> state
    ok_count = 0
    down_count = 0
    unknown_count = 0
    for svc in WATCHED_SERVICES:
        ko = SERVICE_NAMES_KO.get(svc, svc)
        found = False
        matched_line = ''
        for line in out.splitlines():
            if f'{svc}.service' not in line:
                continue
            found = True
            matched_line = line
            break
        state = _classify_service_state(matched_line, found)
        states[svc] = state
        icon = STATE_ICONS[state]
        state_kr = STATE_KR[state]
        is_required = svc in REQUIRED_SERVICES
        req_tag = '' if is_required else ' (선택)'
        if state == 'OK':
            status_lines.append(f'  {icon} {svc} ({ko}) — {state_kr}{req_tag}')
            ok_count += 1
        elif state == 'DOWN':
            detail = '오류' if found and 'failed' in matched_line.lower() else '중지'
            status_lines.append(f'  {icon} {svc} ({ko}) — {detail}{req_tag}')
            down_count += 1
        else:
            detail = '미등록' if not found else '미확인'
            status_lines.append(f'  {icon} {svc} ({ko}) — {detail}{req_tag}')
            unknown_count += 1
    total = len(WATCHED_SERVICES)
    header = [
        '🩺 서비스 상태 요약',
        f'  전체: {total}개 | 정상: {ok_count} | 중지: {down_count} | 미확인: {unknown_count}',
        '']
    # 경고 메시지
    req_down = [s for s in REQUIRED_SERVICES if states.get(s) == 'DOWN']
    req_unknown = [s for s in REQUIRED_SERVICES if states.get(s) == 'UNKNOWN']
    warnings = []
    if req_down:
        warnings.append(f'⚠ 필수 서비스 중지: {", ".join(req_down)}')
    if len(req_unknown) >= 2:
        warnings.append(f'⚠ 필수 서비스 미확인 {len(req_unknown)}개: {", ".join(req_unknown)}')
    if warnings:
        status_lines.append('')
        status_lines.extend(warnings)
    # DB 기록
    _log_service_health(states)
    return '\n'.join(header + status_lines)


def _log_service_health(states):
    """service_health_log 테이블에 현재 상태 기록."""
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            for svc, state in states.items():
                cur.execute("""
                    INSERT INTO service_health_log (service, state)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING;
                """, (svc, state))
    except Exception:
        pass  # DB 미생성 시 무시
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def get_service_health_snapshot():
    """Dual-source health check (systemctl --all + heartbeat DB).
    Same logic as _debug_health() but returns structured dict.
    Returns dict with per-service detail + aggregate counts.
    """
    from datetime import datetime, timezone

    # systemctl --all
    rc, sctl_out = _run(['systemctl', 'list-units', '--type=service', '--all'])

    # heartbeat DB
    heartbeats = {}
    hb_counts = {}
    hb_error = None
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT service, MAX(ts) AS last_ts, COUNT(*)
                FROM service_health_log
                GROUP BY service;
            """)
            for svc, last_ts, cnt in cur.fetchall():
                heartbeats[svc] = last_ts
                hb_counts[svc] = cnt
    except Exception as e:
        hb_error = str(e)
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    now_utc = datetime.now(timezone.utc)
    services = {}

    for svc in WATCHED_SERVICES:
        reg = SERVICE_REGISTRY.get(svc, {})
        expected_interval = reg.get('expected_interval_sec', 0)
        unit_name = reg.get('systemctl_unit', f'{svc}.service')

        # ── Check 1: Heartbeat from DB ──
        hb_ts = heartbeats.get(svc)
        hb_state = None
        hb_reason = None
        age_sec = None
        hb_cnt = hb_counts.get(svc, 0)
        if hb_error:
            hb_reason = f'db_error:{hb_error[:40]}'
        elif hb_ts:
            if hb_ts.tzinfo is None:
                hb_ts = hb_ts.replace(tzinfo=timezone.utc)
            age_sec = int((now_utc - hb_ts).total_seconds())
            if expected_interval > 0:
                stale_threshold = 3 * expected_interval
                if age_sec < stale_threshold:
                    hb_state = 'OK'
                    hb_reason = f'heartbeat_fresh (age={age_sec}s)'
                elif hb_cnt <= 1 and age_sec < stale_threshold:
                    # Warmup: only 1 heartbeat row and within threshold → UNKNOWN
                    hb_state = 'UNKNOWN'
                    hb_reason = f'warmup (age={age_sec}s, rows={hb_cnt})'
                else:
                    hb_state = 'DOWN'
                    hb_reason = f'heartbeat_stale (age={age_sec}s, threshold={stale_threshold}s)'
            else:
                hb_reason = 'missing_expected_interval'
        else:
            if hb_cnt <= 1:
                hb_state = 'UNKNOWN'
                hb_reason = 'warmup (no_heartbeat_rows)'
            else:
                hb_reason = 'no_heartbeat_rows'

        # ── Check 2: systemctl process ──
        proc_state = None
        proc_reason = None
        if rc != 0:
            proc_reason = 'systemctl_failed'
        else:
            found = False
            matched_line = ''
            for line in sctl_out.splitlines():
                if unit_name in line:
                    found = True
                    matched_line = line
                    break
            if not found:
                proc_reason = f'unit_not_found ({unit_name})'
            else:
                ll = matched_line.lower()
                if 'active' in ll and 'running' in ll:
                    proc_state = 'OK'
                    proc_reason = 'active_running'
                elif 'failed' in ll:
                    proc_state = 'DOWN'
                    proc_reason = 'systemctl_failed'
                elif 'inactive' in ll and 'dead' in ll:
                    proc_state = 'DOWN'
                    proc_reason = 'inactive_dead'
                elif 'activating' in ll:
                    proc_reason = 'activating'
                elif 'masked' in ll:
                    proc_reason = 'masked'
                else:
                    proc_reason = f'parse_unknown'

        # ── Final verdict: process alive + hb stale → OK+WARN (not DOWN) ──
        if hb_state == 'OK':
            state = 'OK'
            reason = hb_reason
        elif proc_state == 'OK' and hb_state == 'DOWN':
            # Process is alive but heartbeat stale → trust process, WARN only
            state = 'OK'
            reason = f'WARN: {hb_reason} (process alive)'
        elif proc_state == 'OK':
            state = 'OK'
            reason = proc_reason
        elif hb_state == 'DOWN':
            state = 'DOWN'
            reason = hb_reason
        elif proc_state == 'DOWN':
            state = 'DOWN'
            reason = proc_reason
        else:
            state = 'UNKNOWN'
            reasons = []
            if hb_reason:
                reasons.append(f'hb:{hb_reason}')
            if proc_reason:
                reasons.append(f'proc:{proc_reason}')
            reason = '; '.join(reasons) if reasons else 'no_check_source'

        services[svc] = {
            'state': state,
            'reason': reason,
            'age_sec': age_sec,
        }

    ok = [s for s, d in services.items() if d['state'] == 'OK']
    down = [s for s, d in services.items() if d['state'] == 'DOWN']
    unknown = [s for s, d in services.items() if d['state'] == 'UNKNOWN']
    req_down = [s for s in REQUIRED_SERVICES if services.get(s, {}).get('state') == 'DOWN']
    req_unknown = [s for s in REQUIRED_SERVICES if services.get(s, {}).get('state') == 'UNKNOWN']

    return {
        'ok': len(ok),
        'down': down,
        'unknown': unknown,
        'required_down': req_down,
        'required_unknown': req_unknown,
        'services': services,
        'health_check_ts': now_utc.isoformat(),
    }


def get_service_health_summary():
    """safety_manager 하위 호환 wrapper → get_service_health_snapshot() 위임."""
    return get_service_health_snapshot()


def _btc_price(_text=None):
    import ccxt
    ex = ccxt.bybit({
        'enableRateLimit': True})
    t = ex.fetch_ticker('BTC/USDT')
    last = t.get('last')
    ts = t.get('datetime', '')
    return f'BTC/USDT 현재가: {last} (bybit) {ts}'


def _news_summary(text=None):
    (minutes, limit) = _parse_minutes_and_limit(text)
    env = os.environ.copy()
    env['DATABASE_URL'] = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    env['NEWS_SUMMARY_MINUTES'] = str(minutes)
    env['NEWS_SUMMARY_LIMIT'] = str(limit)
    p = subprocess.run([
        'python3',
        f'{APP_DIR}/news_bot.py',
        '--summary'], capture_output=True, text=True, timeout=25, env=env)
    out = (p.stdout or '').strip()
    err = (p.stderr or '').strip()
    if p.returncode != 0:
        return f'DB 뉴스 요약 실패(rc={p.returncode})\n{(err or out)[-3500:]}'
    if out:
        return out[:3500]
    return '뉴스 데이터 없음'


def _equity_report(_text=None):
    (rc, out) = _run([
        'python3',
        f'{APP_DIR}/equity_report.py'], timeout=35)
    if rc != 0:
        return f'equity_report 실패(rc={rc})\n{out[-3500:]}'
    if len(out) > 3500:
        return out[-3500:]
    return out


def _daily_report(_text=None):
    (rc, out) = _run([
        'python3',
        f'{APP_DIR}/daily_report.py'], timeout=35)
    if rc != 0:
        return f'daily_report 실패(rc={rc})\n{out[-3500:]}'
    if len(out) > 3500:
        return out[-3500:]
    return out


def _recent_errors(_text=None):
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ts, service, message
                FROM error_log
                ORDER BY ts DESC
                LIMIT 10;
            """)
            rows = cur.fetchall()
        if not rows:
            return '최근 에러 없음'
        lines = ['🚨 최근 에러 목록']
        for ts, svc, msg in rows:
            lines.append(f'  [{ts}] {svc}: {msg[:200]}')
        return '\n'.join(lines)
    except Exception as e:
        return f'에러 조회 실패: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _indicator_snapshot(_text=None):
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            # BTC current price from latest candle
            cur.execute("""
                SELECT c FROM candles
                WHERE symbol = %s AND tf = '1m'
                ORDER BY ts DESC LIMIT 1;
            """, (SYMBOL,))
            price_row = cur.fetchone()
            btc_price = float(price_row[0]) if price_row else None

            cur.execute("""
                SELECT ts, rsi_14, atr_14, bb_up, bb_mid, bb_dn,
                       ich_tenkan, ich_kijun, vol_spike, ma_50, ma_200
                FROM indicators
                WHERE symbol = %s AND tf = '1m'
                ORDER BY ts DESC LIMIT 1;
            """, (SYMBOL,))
            row = cur.fetchone()
        if not row:
            return '지표 데이터 없음'
        (ts, rsi, atr, bb_up, bb_mid, bb_dn, ich_t, ich_k, vol_spike,
         ma_50, ma_200) = row
        lines = [
            f'📊 지표 스냅샷 ({ts})',
            f'  BTC 현재가: ${btc_price:,.1f}' if btc_price else '  BTC 현재가: N/A',
            f'  RSI(14): {rsi}',
            f'  ATR(14): {atr}',
            f'  BB: upper={bb_up} mid={bb_mid} lower={bb_dn}',
            f'  Ichimoku: tenkan={ich_t} kijun={ich_k}',
            f'  MA: 50={ma_50} 200={ma_200}',
            f'  Volume spike: {"YES" if vol_spike else "NO"}',
        ]
        return '\n'.join(lines)
    except Exception as e:
        return f'지표 조회 실패: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _volatility_summary(_text=None):
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ts, atr_14, bb_up - bb_dn AS bb_width,
                       vol_spike
                FROM indicators
                WHERE symbol = %s AND tf = '1m'
                ORDER BY ts DESC LIMIT 5;
            """, (SYMBOL,))
            rows = cur.fetchall()
        if not rows:
            return '변동성 데이터 없음'
        lines = ['📈 변동성 요약 (최근 5건)']
        for ts, atr, bb_w, vs in rows:
            lines.append(f'  [{ts}] ATR={atr} BB폭={bb_w} spike={"Y" if vs else "N"}')
        return '\n'.join(lines)
    except Exception as e:
        return f'변동성 조회 실패: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _position_info(_text=None):
    try:
        data = exchange_reader.fetch_position()
        pos = data.get('exchange_position', 'NONE')
        if pos == 'NONE':
            return '📍 포지션(거래소): 없음'
        qty = data.get('exch_pos_qty', 0)
        entry = data.get('exch_entry_price', 0)
        return f'📍 포지션(거래소): {pos} qty={qty} entry=${entry:,.2f}'
    except Exception as e:
        return f'포지션 조회 실패: {e}'


# ── OpenClaw v3: Fact handlers (exchange_reader + response_envelope) ──


def _position_exch(_text=None):
    try:
        data = exchange_reader.fetch_position()
    except Exception as e:
        _log(f'_position_exch error: {e}')
        data = {'data_status': 'ERROR', 'exchange_position': 'UNKNOWN', 'error': str(e)}
    return response_envelope.format_position_exch(data)


def _orders_exch(_text=None):
    try:
        data = exchange_reader.fetch_open_orders()
    except Exception as e:
        _log(f'_orders_exch error: {e}')
        data = {'data_status': 'ERROR', 'orders': [], 'error': str(e)}
    return response_envelope.format_orders_exch(data)


def _account_exch(_text=None):
    try:
        data = exchange_reader.fetch_balance()
    except Exception as e:
        _log(f'_account_exch error: {e}')
        data = {'data_status': 'ERROR', 'total': 0, 'free': 0, 'used': 0, 'error': str(e)}
    return response_envelope.format_account_exch(data)


def _position_strat(_text=None):
    try:
        data = exchange_reader.fetch_position_strat()
    except Exception as e:
        _log(f'_position_strat error: {e}')
        data = {'data_status': 'ERROR', 'strategy_state': 'UNKNOWN', 'error': str(e)}
    return response_envelope.format_position_strat(data)


def _risk_config(_text=None):
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            import safety_manager
            limits = safety_manager._load_safety_limits(cur)
        return response_envelope.format_risk_config(limits)
    except Exception as e:
        _log(f'_risk_config error: {e}')
        return response_envelope.format_risk_config(None)
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _combined_snapshot(_text=None):
    """Combined status: regime + position + score + wait reason."""
    lines = []
    conn = None
    scores = {}
    regime_ctx = {}
    try:
        conn = _db()
        with conn.cursor() as cur:
            # 1. Current regime
            try:
                import regime_reader
                regime_ctx = regime_reader.get_current_regime(cur)
                regime = regime_ctx.get('regime', 'UNKNOWN')
                conf = regime_ctx.get('confidence', 0)
                lines.append(f'모드: {regime} (신뢰도: {conf}%)')
                if regime_ctx.get('in_transition'):
                    lines.append('  (레짐 전환 쿨다운 중)')
            except Exception as e:
                lines.append(f'모드: 조회 실패 ({e})')

            # 2. Exchange position (4-block)
            try:
                fact_text = _fact_snapshot(_text)
                lines.append('')
                lines.append(fact_text)
            except Exception as e:
                lines.append(f'스냅샷 조회 실패: {e}')

            # 3. Score summary
            try:
                import score_engine
                scores = score_engine.compute_total()
                total = scores.get('total_score', 0)
                dominant = scores.get('dominant_side', '?')
                sig_stage = scores.get('signal_stage', '?')
                stage = scores.get('stage', 0)
                abs_score = scores.get('abs_score', 0)
                lines.append('')
                lines.append(f'스코어: total={total:+.1f}, 권고강도: {sig_stage} (abs={abs_score:.0f})')
                lines.append(f'방향: {dominant}, 분할단계: stage {stage}/7')
            except Exception as e:
                lines.append(f'스코어 조회 실패: {e}')

            # 4. WAIT reason (what conditions are missing)
            _scores = scores
            _regime_ctx = regime_ctx
            try:
                wr, wd = exchange_reader.compute_wait_reason(cur=cur)
                lines.append('')
                if wr != 'READY':
                    lines.append(f'대기 사유: {wr}')
                    lines.append(f'  부족 조건: {wd}')
                    # Enumerate specific missing conditions
                    missing = _compute_missing_conditions(cur, _scores, _regime_ctx)
                    if missing:
                        for m in missing:
                            lines.append(f'  - {m}')
                else:
                    lines.append('상태: 진입 대기 중 (조건 충족 시 주문)')
            except Exception as e:
                lines.append(f'대기 사유 조회 실패: {e}')

            # 5. Signal suppression + Regime + ADD control (v14)
            try:
                import autopilot_daemon
                sp = autopilot_daemon.get_signal_policy_snapshot(cur)

                # 5a. Regime info
                lines.append('')
                lines.append('── REGIME ──')
                regime = sp.get('regime', 'UNKNOWN')
                regime_conf = sp.get('regime_confidence', 0)
                bbw = sp.get('regime_bbw_ratio')
                adx = sp.get('regime_adx')
                lines.append(f'REGIME: {regime} (conf={regime_conf}%)')
                bbw_str = f'{bbw:.2f}' if bbw is not None else 'N/A'
                adx_str = f'{adx:.1f}' if adx is not None else 'N/A'
                bbw_pct = sp.get('regime_bb_width_pct')
                bbw_pct_str = f'{bbw_pct:.2f}%' if bbw_pct is not None else 'N/A'
                lines.append(f'  근거: BBW_ratio={bbw_str}, BB_WIDTH={bbw_pct_str}, ADX={adx_str}')
                lines.append(f'max_stage: {sp.get("max_stage", "?")}')
                if sp.get('in_transition'):
                    lines.append('  (레짐 전환 쿨다운 중)')

                # 5b. Signal suppression
                lines.append('')
                lines.append('── 신호 억제 ──')
                lines.append(f'start_stage: {sp["start_stage_policy"]}')
                lines.append(f'conf: {sp["conf_thresholds"]}')
                cd_long = sp.get('cooldown_LONG_remaining', 0)
                cd_short = sp.get('cooldown_SHORT_remaining', 0)
                lines.append(f'재신호 쿨다운: {sp["repeat_cooldown_sec"]}s (L={cd_long}s, S={cd_short}s)')
                for d in ('LONG', 'SHORT'):
                    ts_key = f'last_signal_{d}_ts'
                    if ts_key in sp:
                        lines.append(f'최근 {d}: {sp[ts_key]}')
                lines.append(f'억제 사유: {sp["last_suppress_reason"]}')
                if sp.get('stop_cooldown_active'):
                    lines.append(f'손절 쿨다운: 활성 ({sp["stop_cooldown_remaining"]}s)')
                else:
                    lines.append('손절 쿨다운: 비활성')

                # 5c. ADD control
                lines.append('')
                lines.append('── ADD 제어 ──')
                lines.append(f'ADD 간격: {sp.get("add_min_interval_sec", "?")}s '
                             f'(잔여: {sp.get("add_interval_remaining", 0)}s)')
                lines.append(f'ADD 30분: {sp.get("adds_30m_count", 0)}/{sp.get("adds_30m_limit", "?")}')
                lines.append(f'리테스트 필수: {"YES" if sp.get("add_retest_required") else "NO"}')
                lines.append(f'EVENT→ADD: {"차단" if sp.get("event_add_blocked") else "허용"}')
                lines.append(f'동일방향 재진입 쿨다운: {sp.get("same_dir_reentry_cooldown_sec", "?")}s')
                next_add = sp.get('next_add_earliest', 'NOW')
                lines.append(f'다음 ADD 가능: {next_add}')

                # 5d. Order throttle
                lines.append('')
                lines.append('── ORDER THROTTLE ──')
                lines.append(f'시간당: {sp.get("throttle_hourly", "?")}')
                lines.append(f'10분당: {sp.get("throttle_10min", "?")}')
                if sp.get('throttle_locked'):
                    lines.append(f'잠금: {sp.get("throttle_lock_reason", "")}')
                try:
                    import order_throttle
                    ots = order_throttle.get_state_snapshot()
                    if ots.get('next_try_str'):
                        lines.append(f'next_try: {ots["next_try_str"]} ({ots.get("next_try_reason", "")})')
                    else:
                        lines.append('next_try: READY')
                    if ots.get('last_reject_reason'):
                        lines.append(f'마지막 거부: {ots["last_reject_reason"]}')
                except Exception:
                    pass

            except Exception as e:
                lines.append(f'정책 조회 실패: {e}')

    except Exception as e:
        lines.append(f'combined_snapshot 오류: {e}')
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
    return '\n'.join(lines)


def _compute_missing_conditions(cur, scores, regime_ctx):
    """Enumerate specific unmet conditions for entry."""
    missing = []
    try:
        abs_score = scores.get('abs_score', 0) if scores else 0
        regime = regime_ctx.get('regime', 'UNKNOWN') if regime_ctx else 'UNKNOWN'

        if abs_score < 45:
            missing.append(f'total_score 부족 ({abs_score:.0f} < 45)')

        if regime == 'RANGE':
            # Check band proximity
            vah = regime_ctx.get('vah')
            val = regime_ctx.get('val')
            price = scores.get('price') if scores else None
            if vah and val and price:
                try:
                    near_val = abs(price - val) / val * 100 <= 0.3 if val > 0 else False
                    near_vah = abs(price - vah) / vah * 100 <= 0.3 if vah > 0 else False
                    if not near_val and not near_vah:
                        missing.append(f'밴드 경계 미도달 (VAL={val:.0f} VAH={vah:.0f} price={price:.0f}, 0.3% 이내 필요)')
                except Exception:
                    pass

            # Check RSI (5m preferred, 1m fallback)
            rsi_5m = None
            try:
                for _tf in ('5m', '1m'):
                    cur.execute("""
                        SELECT rsi_14 FROM indicators
                        WHERE symbol = 'BTC/USDT:USDT' AND tf = %s
                        ORDER BY ts DESC LIMIT 1;
                    """, (_tf,))
                    rsi_row = cur.fetchone()
                    if rsi_row and rsi_row[0] is not None:
                        rsi_5m = float(rsi_row[0])
                        break
                if rsi_5m is not None and not (rsi_5m <= 30 or rsi_5m >= 70):
                    missing.append(f'RSI 조건 미달 (현재 {rsi_5m:.0f}, 30 이하 또는 70 이상 필요)')
            except Exception:
                pass

        if regime == 'BREAKOUT':
            if not regime_ctx.get('breakout_confirmed'):
                missing.append('5m close-confirm 부족 (2캔들 필요)')
    except Exception:
        pass
    return missing


def _fact_snapshot(_text=None):
    """Comprehensive fact snapshot: EXCHANGE + ORDER + STRATEGY_DB + GATE/WAIT.
    Gathers all execution context for full pipeline visibility."""
    try:
        exch_pos = exchange_reader.fetch_position()
    except Exception as e:
        _log(f'_fact_snapshot fetch_position error: {e}')
        exch_pos = {'data_status': 'ERROR', 'exchange_position': 'UNKNOWN', 'error': str(e)}
    try:
        strat_pos = exchange_reader.fetch_position_strat()
    except Exception as e:
        _log(f'_fact_snapshot fetch_position_strat error: {e}')
        strat_pos = {'data_status': 'ERROR', 'strategy_state': 'UNKNOWN', 'error': str(e)}
    try:
        orders = exchange_reader.fetch_open_orders()
    except Exception as e:
        _log(f'_fact_snapshot fetch_open_orders error: {e}')
        orders = {'data_status': 'ERROR', 'orders': [], 'error': str(e)}

    exec_ctx = {}
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            exec_ctx = exchange_reader.fetch_execution_context(cur)
    except Exception as e:
        _log(f'_fact_snapshot exec_ctx error: {e}')
        exec_ctx = {'error': str(e), 'wait_reason': 'UNKNOWN'}
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    return response_envelope.format_fact_snapshot(
        exch_pos, strat_pos, orders, exec_ctx)


def _snapshot(_text=None):
    try:
        exch_pos = exchange_reader.fetch_position()
    except Exception as e:
        _log(f'_snapshot fetch_position error: {e}')
        exch_pos = {'data_status': 'ERROR', 'exchange_position': 'UNKNOWN', 'error': str(e)}
    try:
        strat_pos = exchange_reader.fetch_position_strat()
    except Exception as e:
        _log(f'_snapshot fetch_position_strat error: {e}')
        strat_pos = {'data_status': 'ERROR', 'strategy_state': 'UNKNOWN', 'error': str(e)}
    try:
        orders = exchange_reader.fetch_open_orders()
    except Exception as e:
        _log(f'_snapshot fetch_open_orders error: {e}')
        orders = {'data_status': 'ERROR', 'orders': [], 'error': str(e)}

    conn = None
    gate_status = None
    switch_status = None
    wait_reason = None
    capital_info = None
    zone_check = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            import safety_manager
            gate_status = safety_manager.run_all_checks(cur)
            cur.execute(
                "SELECT enabled FROM trade_switch ORDER BY id DESC LIMIT 1;")
            row = cur.fetchone()
            switch_status = row[0] if row else None
            wr = exchange_reader.compute_wait_reason(cur, gate_status=gate_status)
            wait_reason = wr[0] if isinstance(wr, tuple) else wr

            # Capital info for display
            try:
                eq = safety_manager.get_equity_limits(cur)
                cur.execute('SELECT capital_used_usdt, stage FROM position_state WHERE symbol = %s;',
                            ('BTC/USDT:USDT',))
                ps_row = cur.fetchone()
                used = float(ps_row[0]) if ps_row and ps_row[0] else 0
                stage = int(ps_row[1]) if ps_row and ps_row[1] else 0
                # leverage from already-fetched exchange position (avoid redundant API call)
                lev = exch_pos.get('leverage', 0) if exch_pos.get('data_status') == 'OK' else 0
                lev_min = eq.get('leverage_min', 3)
                lev_max = eq.get('leverage_max', 8)
                capital_info = {
                    **eq,
                    'used_usdt': used,
                    'remaining_usdt': round(max(0, eq['operating_cap'] - used), 2),
                    'stage': stage,
                    'leverage_current': lev,
                    'leverage_rule': f'{lev_min}-{lev_max}x',
                }
            except Exception:
                pass

            # Zone check data for ZONE_CHECK section
            try:
                import regime_reader
                import autopilot_daemon
                import order_throttle

                regime_ctx = regime_reader.get_current_regime(cur)
                regime = regime_ctx.get('regime', 'UNKNOWN')

                # Current price
                cur.execute("SELECT mark_price FROM market_data_cache WHERE symbol = %s;",
                            ('BTC/USDT:USDT',))
                zc_price_row = cur.fetchone()
                zc_price = float(zc_price_row[0]) if zc_price_row and zc_price_row[0] else 0

                # BB data
                bb_data = autopilot_daemon._get_bb_data(cur)

                # Compute zones
                zones = autopilot_daemon._compute_entry_zones(zc_price, regime_ctx, bb_data)

                zone_check = {
                    'current_price': zc_price,
                    'regime': regime,
                    **zones,
                }

                # Anti-chase status (RANGE only)
                if regime == 'RANGE':
                    chase_ok, chase_reason = autopilot_daemon._check_anti_chase(cur, regime_ctx, dry_run=True)
                    if not chase_ok:
                        zone_check['chase_block'] = chase_reason

                # Daily trade count (FILLED basis)
                cur.execute("""
                    SELECT count(*) FROM execution_log
                    WHERE symbol = 'BTC/USDT:USDT' AND status = 'FILLED'
                      AND order_type IN ('OPEN', 'ADD')
                      AND last_fill_at >= (now() AT TIME ZONE 'Asia/Seoul')::date AT TIME ZONE 'Asia/Seoul';
                """)
                dtc_row = cur.fetchone()
                zone_check['daily_trade_count'] = int(dtc_row[0]) if dtc_row else 0

                # Throttle attempt count
                ts = order_throttle.get_throttle_status()
                zone_check['throttle_attempts_1h'] = ts.get('hourly_count', 0)
                zone_check['throttle_limit_1h'] = ts.get('hourly_limit', 12)

                # Signal policy snapshot
                try:
                    zone_check['signal_policy'] = autopilot_daemon.get_signal_policy_snapshot(cur)
                except Exception:
                    pass

            except Exception as e:
                _log(f'_snapshot zone_check error: {e}')

    except Exception as e:
        _log(f'_snapshot gate/switch error: {e}')
        if gate_status is None:
            gate_status = (False, f'조회 실패: {e}')
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    return response_envelope.format_snapshot(
        exch_pos, strat_pos, orders, gate_status, switch_status, wait_reason,
        capital_info=capital_info, zone_check=zone_check)


def _score_summary(_text=None):
    try:
        import score_engine
        r = score_engine.compute_total()
        ne = r.get('news_event_score', 0)
        guarded = r.get('news_event_guarded', False)
        ne_detail = r.get('axis_details', {}).get('news_event', {})
        ne_comp = ne_detail.get('components', {})
        ne_details = ne_detail.get('details', {})
        weights = r.get('weights', {})
        total = r.get('total_score', 0)
        tech = r.get('tech_score', 0)
        pos = r.get('position_score', 0)
        regime = r.get('regime_score', 0)
        dominant = r.get('dominant_side', '?')
        stage = r.get('stage', '?')
        tech_w = weights.get('tech_w', 0.45)
        pos_w = weights.get('position_w', 0.25)
        regime_w = weights.get('regime_w', 0.25)
        news_w = weights.get('news_event_w', 0.05)
        # 축별 가중 기여도
        tech_c = tech * tech_w
        pos_c = pos * pos_w
        regime_c = regime * regime_w
        news_c = ne * news_w
        # Signal stage (from score engine: stg0/stg1/stg2/stg3)
        abs_score = r.get('abs_score', abs(total))
        signal_stage_label = r.get('signal_stage', f'stg{stage}')
        # Position stage (from position_state DB: 0-7 pyramid)
        pos_stage = 0
        pos_capital_pct = 0
        try:
            conn_ps = _db()
            with conn_ps.cursor() as cur_ps:
                cur_ps.execute(
                    "SELECT stage, capital_used_usdt FROM position_state WHERE symbol = %s;",
                    ('BTC/USDT:USDT',))
                ps_row = cur_ps.fetchone()
                if ps_row:
                    pos_stage = int(ps_row[0]) if ps_row[0] else 0
                    used_usdt = float(ps_row[1]) if ps_row[1] else 0
                    import safety_manager
                    eq = safety_manager.get_equity_limits(cur_ps)
                    op_cap = eq.get('operating_cap', 1)
                    pos_capital_pct = round(used_usdt / op_cap * 100, 0) if op_cap > 0 else 0
            conn_ps.close()
        except Exception:
            pass

        lines = [
            f"📊 스코어 엔진 (4축)",
            f"━━━━━━━━━━━━━━━━━━",
            f"총점: {total:+.1f} → {dominant}",
            f"",
            f"기술(TECH):   {tech:+.0f} × {tech_w} = {tech_c:+.1f}",
            f"포지션(POS):  {pos:+.0f} × {pos_w} = {pos_c:+.1f}",
            f"레짐(REG):    {regime:+.0f} × {regime_w} = {regime_c:+.1f}",
            f"뉴스(NEWS):   {ne:+.0f} × {news_w} = {news_c:+.1f}{' [차단됨]' if guarded else ''}",
            f"",
            f"권고강도: {signal_stage_label} (score={abs_score:.0f}, stg1>=10, stg2>=45, stg3>=65)",
            f"분할단계: stage {pos_stage}/7 (capital used: {pos_capital_pct:.0f}%)",
            f"",
            f"엔진권고: {dominant} {signal_stage_label} (총점 {total:+.1f})",
        ]
        # 현재 포지션 정보
        try:
            pos_info = _position_info()
            lines.append(f"현재포지션: {pos_info.replace('📍 ', '')}")
        except Exception:
            pass
        if guarded:
            lines.append(f"  ⚠ {dominant} 권고이나, TECH/POS 중립으로 뉴스 단독 차단")
        lines.append(f"")
        lines.append(f"뉴스 이벤트 내역:")
        lines.append(f"  소스품질: {ne_comp.get('source_quality', 0):.1f}/20")
        lines.append(f"  카테고리: {ne_comp.get('category_weight', 0):.1f}/25")
        lines.append(f"  최신성: {ne_comp.get('recency', 0):.1f}/15")
        lines.append(f"  시장반응: {ne_comp.get('market_reaction', 0):.1f}/25")
        lines.append(f"  키워드: {ne_comp.get('watchlist', 0):.1f}/15")
        score_trace = ne_details.get('score_trace', '')
        if score_trace:
            lines.append(f"  추적: {score_trace}")
        lines.append(f"")
        lines.append(f"손절: {r.get('dynamic_stop_loss_pct', 2.0)}%")
        lines.append(f"BTC: {r.get('price', '?')}")
        return '\n'.join(lines)
    except Exception as e:
        return f'스코어 조회 실패: {e}'


def _db_health(_text=None):
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            tables = [
                ('candles', 'ts'),
                ('news', 'ts'),
                ('indicators', 'ts'),
                ('events', 'start_ts'),
                ('pm_decision_log', 'ts'),
                ('execution_log', 'ts'),
                ('score_history', 'ts'),
                ('macro_data', 'ts'),
            ]
            lines = ['🗄 DB 상태 점검']
            lines.append('━━━━━━━━━━━━━━━━━━')
            for tbl, ts_col in tables:
                try:
                    cur.execute(f"""
                        SELECT count(*),
                               min({ts_col})::text,
                               max({ts_col})::text,
                               count(*) FILTER (WHERE {ts_col} >= now() - interval '24 hours')
                        FROM {tbl};
                    """)
                    row = cur.fetchone()
                    total, min_ts, max_ts, recent = row
                    min_ts = (min_ts or '')[:16]
                    max_ts = (max_ts or '')[:16]
                    lines.append(f'  {tbl}: {total:,}건 (24h: {recent:,}건)')
                    lines.append(f'    범위: {min_ts} ~ {max_ts}')
                except Exception as e:
                    lines.append(f'  {tbl}: 조회 실패 ({e})')

            # news_impact_stats
            lines.append('')
            lines.append('[뉴스 영향 통계]')
            try:
                cur.execute("""
                    SELECT stats_version, count(*), sum(sample_count)
                    FROM news_impact_stats
                    GROUP BY stats_version
                    ORDER BY stats_version DESC LIMIT 1;
                """)
                row = cur.fetchone()
                if row:
                    lines.append(f'  버전: {row[0]} | 카테고리: {row[1]}개 | 샘플: {row[2]:,}건')
                else:
                    lines.append('  데이터 없음 (compute_news_impact_stats.py 실행 필요)')
            except Exception:
                lines.append('  테이블 미생성')

            # regime_correlation
            lines.append('')
            lines.append('[BTC-QQQ 상관관계]')
            try:
                import regime_correlation
                info = regime_correlation.get_correlation_info(cur)
                regime = info.get('regime', '?')
                corr = info.get('correlation')
                age = info.get('cache_age_sec')
                corr_str = f'{corr:.4f}' if corr is not None else 'N/A'
                age_str = f'{age}초 전' if age is not None else 'N/A'
                lines.append(f'  레짐: {regime} | 상관계수: {corr_str} | 캐시: {age_str}')
            except Exception as e:
                lines.append(f'  조회 실패 ({e})')

        return '\n'.join(lines)
    except Exception as e:
        return f'DB 상태 조회 실패: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _claude_audit(_text=None):
    """Claude API 사용량 감사 리포트."""
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            lines = ['🧠 Claude 사용량 감사']
            lines.append('━━━━━━━━━━━━━━━━━━')

            # Today's stats from claude_call_log
            cur.execute("""
                SELECT count(*),
                       coalesce(sum(estimated_cost), 0),
                       coalesce(sum(input_tokens), 0),
                       coalesce(sum(output_tokens), 0)
                FROM claude_call_log
                WHERE ts >= date_trunc('day', now() AT TIME ZONE 'Asia/Seoul');
            """)
            row = cur.fetchone()
            today_calls = row[0] or 0
            today_cost = float(row[1] or 0)
            today_input = row[2] or 0
            today_output = row[3] or 0
            lines.append(f'\n[오늘 사용량]')
            lines.append(f'  호출: {today_calls}건 | 비용: ${today_cost:.4f}')
            lines.append(f'  입력 토큰: {today_input:,} | 출력 토큰: {today_output:,}')

            # By gate_type today
            cur.execute("""
                SELECT gate_type, count(*), coalesce(sum(estimated_cost), 0)
                FROM claude_call_log
                WHERE ts >= date_trunc('day', now() AT TIME ZONE 'Asia/Seoul')
                GROUP BY gate_type ORDER BY count(*) DESC;
            """)
            gate_rows = cur.fetchall()
            if gate_rows:
                lines.append(f'\n[게이트별 분류]')
                for gr in gate_rows:
                    lines.append(f'  {gr[0] or "?"}: {gr[1]}건 (${float(gr[2]):.4f})')

            # Monthly stats
            cur.execute("""
                SELECT count(*),
                       coalesce(sum(estimated_cost), 0)
                FROM claude_call_log
                WHERE ts >= date_trunc('month', now());
            """)
            row = cur.fetchone()
            month_calls = row[0] or 0
            month_cost = float(row[1] or 0)
            lines.append(f'\n[이번 달 누적]')
            lines.append(f'  호출: {month_calls}건 | 비용: ${month_cost:.4f}')

            # Budget remaining (from claude_gate)
            try:
                import claude_gate
                lines.append(f'\n[예산 한도]')
                lines.append(f'  일일 호출 한도: {claude_gate.DAILY_CALL_LIMIT}')
                lines.append(f'  일일 비용 한도: ${claude_gate.DAILY_COST_LIMIT}')
                lines.append(f'  월간 비용 한도: ${claude_gate.MONTHLY_COST_LIMIT}')
                remaining_calls = max(0, claude_gate.DAILY_CALL_LIMIT - today_calls)
                remaining_cost = max(0, claude_gate.DAILY_COST_LIMIT - today_cost)
                lines.append(f'  남은 호출: {remaining_calls}건 | 남은 비용: ${remaining_cost:.2f}')
            except Exception:
                pass

            # Recent 5 calls
            cur.execute("""
                SELECT to_char(ts AT TIME ZONE 'Asia/Seoul', 'HH24:MI'),
                       gate_type, call_type,
                       action_result, estimated_cost
                FROM claude_call_log
                ORDER BY ts DESC LIMIT 5;
            """)
            recent = cur.fetchall()
            if recent:
                lines.append(f'\n[최근 호출 5건]')
                for r in recent:
                    cost_str = f'${float(r[4]):.4f}' if r[4] else '$0'
                    lines.append(f'  {r[0]} {r[1] or "?"}/{r[2] or "?"} '
                                 f'→ {r[3] or "?"} ({cost_str})')

        return '\n'.join(lines)
    except Exception as e:
        return f'Claude 감사 조회 실패: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _audit_report(_text=None):
    """종합 감사 리포트: 차트흐름 + 이벤트 + 결정 + 실행 + 뉴스기여 + 규칙위반."""
    conn = None
    try:
        conn = _db()
        lines = ['📋 종합 감사 리포트']
        lines.append('━━━━━━━━━━━━━━━━━━')
        lines.append('⚠ 즉시 적용 금지 — 분석 자료')
        lines.append('')

        with conn.cursor() as cur:
            # 1. 차트 흐름
            lines.append('[1. 차트 흐름]')
            try:
                from news_strategy_report import _fetch_chart_flow
                chart = _fetch_chart_flow(cur)
                lines.append(f'  4h 추세: {chart.get("trend_4h", "?")}({chart.get("trend_4h_pct", 0):+.1f}%)')
                lines.append(f'  12h 추세: {chart.get("trend_12h", "?")}({chart.get("trend_12h_pct", 0):+.1f}%)')
                lines.append(f'  BB: {chart.get("bb_position", "?")} | Ichimoku: {chart.get("ichimoku_cloud", "?")}')
            except Exception as e:
                lines.append(f'  조회 실패: {e}')

            # 2. 이벤트 타임라인 (최근 24h)
            lines.append('')
            lines.append('[2. 이벤트 타임라인 (24h)]')
            try:
                cur.execute("""
                    SELECT to_char(ts AT TIME ZONE 'Asia/Seoul', 'MM-DD HH24:MI'),
                           mode, triggers, snapshot_price
                    FROM event_trigger_log
                    WHERE ts >= now() - interval '24 hours'
                    ORDER BY ts DESC LIMIT 10;
                """)
                evt_rows = cur.fetchall()
                if evt_rows:
                    for r in evt_rows:
                        triggers = r[2] or ''
                        if isinstance(triggers, (list, dict)):
                            import json
                            triggers = json.dumps(triggers, ensure_ascii=False)[:80]
                        lines.append(f'  {r[0]} [{r[1]}] {str(triggers)[:60]} price={r[3] or "?"}')
                else:
                    lines.append('  이벤트 없음')
            except Exception:
                lines.append('  조회 실패')

            # 3. 결정 로그 (최근 24h)
            lines.append('')
            lines.append('[3. 결정 로그 (24h)]')
            try:
                cur.execute("""
                    SELECT to_char(ts AT TIME ZONE 'Asia/Seoul', 'MM-DD HH24:MI'),
                           final_action, position_side, action_reason
                    FROM pm_decision_log
                    WHERE ts >= now() - interval '24 hours'
                    ORDER BY ts DESC LIMIT 10;
                """)
                dec_rows = cur.fetchall()
                if dec_rows:
                    for r in dec_rows:
                        lines.append(f'  {r[0]} {r[1] or "?"} {r[2] or ""} — {(r[3] or "")[:60]}')
                else:
                    lines.append('  결정 없음')
            except Exception:
                lines.append('  조회 실패')

            # 4. 실행 로그 (최근 24h)
            lines.append('')
            lines.append('[4. 실행 로그 (24h)]')
            try:
                cur.execute("""
                    SELECT to_char(ts AT TIME ZONE 'Asia/Seoul', 'MM-DD HH24:MI'),
                           order_type, direction, status,
                           filled_qty, avg_fill_price, realized_pnl
                    FROM execution_log
                    WHERE ts >= now() - interval '24 hours'
                    ORDER BY ts DESC LIMIT 10;
                """)
                exec_rows = cur.fetchall()
                if exec_rows:
                    for r in exec_rows:
                        pnl_str = f' PnL={float(r[6]):+.4f}' if r[6] else ''
                        lines.append(f'  {r[0]} {r[1]} {r[2] or ""} [{r[3]}]{pnl_str}')
                else:
                    lines.append('  실행 없음')
            except Exception:
                lines.append('  조회 실패')

            # 5. 뉴스 기여 (당일 전략 반영된 뉴스)
            lines.append('')
            lines.append('[5. 전략 반영 뉴스 (당일)]')
            try:
                cur.execute("""
                    SELECT title_ko, tier, impact_score, relevance_score, source
                    FROM news
                    WHERE ts >= date_trunc('day', now() AT TIME ZONE 'Asia/Seoul')
                      AND exclusion_reason IS NULL
                      AND COALESCE(tier, 'UNKNOWN') NOT IN ('TIERX')
                      AND impact_score >= 3
                    ORDER BY impact_score DESC
                    LIMIT 5;
                """)
                news_rows = cur.fetchall()
                if news_rows:
                    for i, r in enumerate(news_rows, 1):
                        title = (r[0] or '?')[:50]
                        lines.append(f'  {i}) [{r[1]}] ({r[2]}/10) {title} rel={r[3] or "?"}')
                else:
                    lines.append('  반영 뉴스 없음')
            except Exception:
                lines.append('  조회 실패')

            # 6. 규칙 위반 / 안전 차단 이력
            lines.append('')
            lines.append('[6. 안전 차단 이력 (24h)]')
            try:
                # error_log가 없을 수 있으므로 테이블 존재 확인
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_name = 'error_log' AND table_schema = 'public'
                    );
                """)
                has_error_log = cur.fetchone()[0]
                if has_error_log:
                    cur.execute("""
                        SELECT to_char(ts AT TIME ZONE 'Asia/Seoul', 'MM-DD HH24:MI'),
                               service, level, message
                        FROM error_log
                        WHERE ts >= now() - interval '24 hours'
                          AND (level IN ('CRITICAL', 'WARNING')
                               OR message ILIKE '%%block%%'
                               OR message ILIKE '%%차단%%')
                        ORDER BY ts DESC LIMIT 5;
                    """)
                    err_rows = cur.fetchall()
                    if err_rows:
                        for r in err_rows:
                            lines.append(f'  {r[0]} [{r[2]}] {r[1]}: {(r[3] or "")[:80]}')
                    else:
                        lines.append('  차단/경고 이력 없음')
                else:
                    # error_log 테이블 미생성 → safety_manager 차단 이력으로 대체
                    cur.execute("""
                        SELECT to_char(ts AT TIME ZONE 'Asia/Seoul', 'MM-DD HH24:MI'),
                               final_action, action_reason
                        FROM pm_decision_log
                        WHERE ts >= now() - interval '24 hours'
                          AND final_action IN ('HOLD', 'ABORT', 'SKIP')
                        ORDER BY ts DESC LIMIT 5;
                    """)
                    hold_rows = cur.fetchall()
                    if hold_rows:
                        for r in hold_rows:
                            lines.append(f'  {r[0]} {r[1]} — {(r[2] or "")[:60]}')
                    else:
                        lines.append('  차단/HOLD 이력 없음')
            except Exception:
                lines.append('  조회 실패')

            # 7. 17:30 청산 상태
            lines.append('')
            lines.append('[7. 17:30 청산 상태]')
            try:
                cur.execute("""
                    SELECT to_char(ts AT TIME ZONE 'Asia/Seoul', 'MM-DD HH24:MI'),
                           status, close_reason
                    FROM execution_log
                    WHERE order_type = 'SCHEDULED_CLOSE'
                      AND ts >= now() - interval '48 hours'
                    ORDER BY ts DESC LIMIT 3;
                """)
                sched_rows = cur.fetchall()
                if sched_rows:
                    for r in sched_rows:
                        lines.append(f'  {r[0]} [{r[1]}] {r[2] or ""}')
                else:
                    lines.append('  최근 48시간 예약 청산 기록 없음')
            except Exception:
                lines.append('  조회 실패')

        lines.append('')
        lines.append('⚠ 즉시 적용 금지 — 분석 자료')
        return '\n'.join(lines)
    except Exception as e:
        return f'감사 리포트 조회 실패: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _news_applied(_text=None):
    """전략 반영 뉴스 TOP5: tier/topic_class/relevance_score/source_tier/30m/2h 반응."""
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT n.id, n.title_ko, n.tier, n.topic_class,
                       n.relevance_score, n.source_tier, n.impact_score, n.source,
                       mt.btc_ret_30m, mt.btc_ret_2h,
                       to_char(n.ts AT TIME ZONE 'Asia/Seoul', 'MM-DD HH24:MI') as ts_kr
                FROM news n
                LEFT JOIN macro_trace mt ON mt.news_id = n.id
                WHERE n.ts >= now() - interval '6 hours'
                  AND n.exclusion_reason IS NULL
                  AND COALESCE(n.tier, 'UNKNOWN') NOT IN ('TIERX')
                  AND n.impact_score >= 3
                ORDER BY n.impact_score DESC, n.ts DESC
                LIMIT 5;
            """)
            rows = cur.fetchall()

        if not rows:
            return '📰 전략 반영 뉴스 TOP5\n━━━━━━━━━━━━━━━━━━\n최근 6시간 반영 뉴스 없음'

        lines = ['📰 전략 반영 뉴스 TOP5 (최근 6시간)', '━━━━━━━━━━━━━━━━━━']
        for i, r in enumerate(rows, 1):
            nid, title, tier, topic, rel, src_tier, impact, source, ret30, ret2h, ts = r
            title = (title or '?')[:60]
            tier = tier or 'UNKNOWN'
            topic = topic or '-'
            rel = f'{rel:.1f}' if rel is not None else '-'
            src_tier = src_tier or '-'
            ret30_str = f'{ret30:+.2f}%' if ret30 is not None else '-'
            ret2h_str = f'{ret2h:+.2f}%' if ret2h is not None else '-'
            lines.append(f'\n{i}) [{tier}] {title}')
            lines.append(f'   topic={topic} | rel={rel} | src={src_tier} | impact={impact}/10')
            lines.append(f'   반응: 30m={ret30_str} 2h={ret2h_str} | {ts} ({source})')
        return '\n'.join(lines)
    except Exception as e:
        return f'⚠ 전략 반영 뉴스 조회 실패: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _news_ignored(_text=None):
    """무시된 뉴스 10개 + 무시 사유."""
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT title_ko, tier, impact_score, exclusion_reason, source,
                       to_char(ts AT TIME ZONE 'Asia/Seoul', 'MM-DD HH24:MI') as ts_kr
                FROM news
                WHERE ts >= now() - interval '6 hours'
                  AND (exclusion_reason IS NOT NULL
                       OR tier = 'TIERX'
                       OR COALESCE(impact_score, 0) < 3)
                ORDER BY ts DESC
                LIMIT 10;
            """)
            rows = cur.fetchall()

        if not rows:
            return '🚫 무시된 뉴스 (최근 6시간)\n━━━━━━━━━━━━━━━━━━\n무시된 뉴스 없음'

        lines = ['🚫 무시된 뉴스 10개 (최근 6시간)', '━━━━━━━━━━━━━━━━━━']
        for i, r in enumerate(rows, 1):
            title, tier, impact, reason, source, ts = r
            title = (title or '?')[:55]
            tier = tier or 'UNKNOWN'
            impact = impact if impact is not None else 0
            # Determine reason
            if reason:
                reason_str = reason[:40]
            elif tier == 'TIERX':
                reason_str = '관련도 최하(TIERX)'
            elif impact < 3:
                reason_str = f'낮은 영향도({impact}/10)'
            else:
                reason_str = '미분류'
            lines.append(f'{i}) [{tier}] {title}')
            lines.append(f'   사유: {reason_str} | {ts} ({source})')
        return '\n'.join(lines)
    except Exception as e:
        return f'⚠ 무시된 뉴스 조회 실패: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _db_coverage(_text=None):
    """DB 커버리지: 2023-11부터 월별 candles/events/news 건수 + news tier=UNKNOWN 비율."""
    conn = None
    try:
        conn = _db()
        lines = ['📊 DB 커버리지 (2023-11~현재)', '━━━━━━━━━━━━━━━━━━']
        with conn.cursor() as cur:
            tables_config = [
                ('candles', 'ts'),
                ('events', 'start_ts'),
                ('news', 'ts'),
            ]
            for tbl, ts_col in tables_config:
                try:
                    cur.execute(f"""
                        SELECT to_char(date_trunc('month', {ts_col}), 'YYYY-MM') AS month,
                               count(*) AS cnt
                        FROM {tbl}
                        WHERE {ts_col} >= '2023-11-01'
                        GROUP BY month
                        ORDER BY month;
                    """)
                    rows = cur.fetchall()
                    lines.append(f'\n[{tbl}]')
                    if not rows:
                        lines.append('  데이터 없음')
                    else:
                        for month, cnt in rows:
                            lines.append(f'  {month}: {cnt:,}건')
                except Exception as e:
                    lines.append(f'\n[{tbl}] 조회 실패: {e}')

            # news tier=UNKNOWN 비율
            lines.append('\n[뉴스 tier 분포]')
            try:
                cur.execute("""
                    SELECT COALESCE(tier, 'NULL') AS t,
                           count(*) AS cnt
                    FROM news
                    WHERE ts >= '2023-11-01'
                    GROUP BY t
                    ORDER BY cnt DESC;
                """)
                tier_rows = cur.fetchall()
                total = sum(r[1] for r in tier_rows) if tier_rows else 0
                for t, cnt in tier_rows:
                    pct = cnt / total * 100 if total > 0 else 0
                    lines.append(f'  {t}: {cnt:,}건 ({pct:.1f}%)')
                unknown_cnt = sum(r[1] for r in tier_rows if r[0] in ('UNKNOWN', 'NULL'))
                if total > 0:
                    lines.append(f'  → UNKNOWN+NULL 비율: {unknown_cnt/total*100:.1f}%')
            except Exception as e:
                lines.append(f'  tier 분포 조회 실패: {e}')

        return '\n'.join(lines)
    except Exception as e:
        return f'⚠ DB 커버리지 조회 실패: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _evidence(_text=None):
    """보조지표 근거: price_events 요약 + tier1/2 impact 합 + 유사 이벤트 Top3."""
    conn = None
    try:
        conn = _db()
        lines = ['📈 보조지표 근거 섹션', '━━━━━━━━━━━━━━━━━━']
        with conn.cursor() as cur:
            # 1. 최근 price_events (급등/급락, ATR 급증, 15m 방향)
            lines.append('\n[최근 price_events (24h)]')
            try:
                cur.execute("""
                    SELECT trigger_type, direction, move_pct, atr_z,
                           btc_price_at,
                           to_char(start_ts AT TIME ZONE 'Asia/Seoul', 'MM-DD HH24:MI') as ts_kr
                    FROM price_events
                    WHERE start_ts >= now() - interval '24 hours'
                    ORDER BY start_ts DESC
                    LIMIT 8;
                """)
                pe_rows = cur.fetchall()
                if pe_rows:
                    for r in pe_rows:
                        trigger, dirn, move, atr_z, price, ts = r
                        move_str = f'{move:+.2f}%' if move is not None else '-'
                        atr_str = f'ATR_z={atr_z:.1f}' if atr_z is not None else ''
                        lines.append(f'  [{ts}] {trigger} {dirn} {move_str} {atr_str} @${price:,.0f}' if price else f'  [{ts}] {trigger} {dirn} {move_str} {atr_str}')
                else:
                    lines.append('  최근 24시간 price_events 없음')
            except Exception as e:
                lines.append(f'  조회 실패: {e}')

            # 2. tier1/2 뉴스 impact 합 (실제 30m/2h/24h 반응 기반)
            lines.append('\n[tier1/tier2 뉴스 반응 합산 (24h)]')
            try:
                cur.execute("""
                    SELECT n.tier,
                           count(*) AS cnt,
                           avg(mt.btc_ret_30m) AS avg_30m,
                           avg(mt.btc_ret_2h) AS avg_2h,
                           avg(mt.btc_ret_24h) AS avg_24h
                    FROM news n
                    JOIN macro_trace mt ON mt.news_id = n.id
                    WHERE n.ts >= now() - interval '24 hours'
                      AND n.tier IN ('TIER1', 'TIER2')
                    GROUP BY n.tier
                    ORDER BY n.tier;
                """)
                tier_rows = cur.fetchall()
                if tier_rows:
                    for t, cnt, a30, a2h, a24h in tier_rows:
                        a30s = f'{a30:+.3f}%' if a30 is not None else '-'
                        a2hs = f'{a2h:+.3f}%' if a2h is not None else '-'
                        a24hs = f'{a24h:+.3f}%' if a24h is not None else '-'
                        lines.append(f'  {t}: {cnt}건 | 평균30m={a30s} 2h={a2hs} 24h={a24hs}')
                else:
                    lines.append('  TIER1/2 뉴스 반응 데이터 없음')
            except Exception as e:
                lines.append(f'  조회 실패: {e}')

            # 3. 유사 이벤트 Top3 + 과거 평균 반응 (news_impact_stats)
            lines.append('\n[유사 이벤트 Top3 (과거 평균 반응)]')
            try:
                cur.execute("""
                    SELECT event_type, regime,
                           avg_ret_30m, avg_ret_2h, avg_abs_ret_2h,
                           sample_count, direction_accuracy
                    FROM news_impact_stats
                    WHERE sample_count >= 3
                    ORDER BY avg_abs_ret_2h DESC
                    LIMIT 3;
                """)
                stat_rows = cur.fetchall()
                if stat_rows:
                    for r in stat_rows:
                        etype, regime, a30, a2h, abs2h, cnt, acc = r
                        a30s = f'{a30:+.3f}%' if a30 is not None else '-'
                        a2hs = f'{a2h:+.3f}%' if a2h is not None else '-'
                        acc_s = f'{acc:.0f}%' if acc is not None else '-'
                        lines.append(f'  {etype} ({regime}): N={cnt}')
                        lines.append(f'    평균30m={a30s} 2h={a2hs} |abs2h|={abs2h:.3f}% 방향적중={acc_s}')
                else:
                    lines.append('  통계 데이터 없음 (compute_news_impact_stats.py 실행 필요)')
            except Exception as e:
                lines.append(f'  조회 실패: {e}')

        return '\n'.join(lines)
    except Exception as e:
        return f'⚠ 보조지표 근거 조회 실패: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _test_report_full(_text=None):
    """테스트 종합 보고: 이벤트/체결/오판/개선점 — 적용 금지."""
    conn = None
    try:
        conn = _db()
        lines = ['🧪 테스트 종합 보고 (적용 금지)', '━━━━━━━━━━━━━━━━━━']
        with conn.cursor() as cur:
            # 1. 오늘 이벤트
            lines.append('\n[1. 오늘 발생 이벤트]')
            try:
                cur.execute("""
                    SELECT to_char(start_ts AT TIME ZONE 'Asia/Seoul', 'HH24:MI'),
                           kind, direction, confidence, btc_price_at
                    FROM events
                    WHERE start_ts >= date_trunc('day', now() AT TIME ZONE 'Asia/Seoul')
                    ORDER BY start_ts DESC
                    LIMIT 10;
                """)
                evt_rows = cur.fetchall()
                if evt_rows:
                    for ts, kind, dirn, conf, price in evt_rows:
                        conf_s = f'{conf:.0f}%' if conf is not None else '-'
                        price_s = f'${price:,.0f}' if price else '-'
                        lines.append(f'  {ts} {kind} {dirn or ""} conf={conf_s} {price_s}')
                else:
                    lines.append('  오늘 이벤트 없음')
            except Exception as e:
                lines.append(f'  조회 실패: {e}')

            # 2. 오늘 체결
            lines.append('\n[2. 오늘 체결]')
            try:
                cur.execute("""
                    SELECT to_char(ts AT TIME ZONE 'Asia/Seoul', 'HH24:MI'),
                           order_type, direction, status,
                           filled_qty, avg_fill_price, realized_pnl
                    FROM execution_log
                    WHERE ts >= date_trunc('day', now() AT TIME ZONE 'Asia/Seoul')
                    ORDER BY ts DESC
                    LIMIT 10;
                """)
                exec_rows = cur.fetchall()
                if exec_rows:
                    for ts, otype, dirn, status, qty, price, pnl in exec_rows:
                        pnl_s = f' PnL={float(pnl):+.4f}' if pnl else ''
                        lines.append(f'  {ts} {otype} {dirn or ""} [{status}]{pnl_s}')
                else:
                    lines.append('  오늘 체결 없음')
            except Exception as e:
                lines.append(f'  조회 실패: {e}')

            # 3. 오판 분석 (권고 vs 실제 포지션 불일치)
            lines.append('\n[3. 오판 분석 (권고 vs 실제)]')
            try:
                cur.execute("""
                    SELECT to_char(d.ts AT TIME ZONE 'Asia/Seoul', 'HH24:MI'),
                           d.final_action, d.position_side, d.action_reason
                    FROM pm_decision_log d
                    WHERE d.ts >= date_trunc('day', now() AT TIME ZONE 'Asia/Seoul')
                    ORDER BY d.ts DESC
                    LIMIT 15;
                """)
                dec_rows = cur.fetchall()
                # Get current position
                cur.execute("""
                    SELECT side FROM position_state
                    WHERE symbol = 'BTC/USDT:USDT';
                """)
                pos_row = cur.fetchone()
                current_side = (pos_row[0] or '').upper() if pos_row else 'NONE'
                mismatches = []
                for ts, action, pos_side, reason in dec_rows:
                    pos_side_up = (pos_side or '').upper()
                    # Detect mismatch: engine says SHORT but position is LONG, etc.
                    if action in ('CLOSE', 'REVERSE') and pos_side_up and current_side != 'NONE':
                        if action == 'REVERSE' or (action == 'CLOSE' and pos_side_up == current_side):
                            mismatches.append(
                                f'  ⚠ {ts} 권고={action} (당시={pos_side_up}) 현재={current_side}\n'
                                f'    사유: {(reason or "")[:60]}')
                if mismatches:
                    lines.extend(mismatches[:5])
                else:
                    lines.append('  권고-실행 불일치 없음')
                lines.append(f'  현재 포지션: {current_side}')
            except Exception as e:
                lines.append(f'  조회 실패: {e}')

            # 4. 개선점/리스크 요약
            lines.append('\n[4. 개선점/리스크]')
            try:
                # Recent safety blocks
                cur.execute("""
                    SELECT count(*) FROM pm_decision_log
                    WHERE ts >= date_trunc('day', now() AT TIME ZONE 'Asia/Seoul')
                      AND final_action IN ('HOLD', 'ABORT', 'SKIP');
                """)
                hold_cnt = cur.fetchone()[0] or 0
                cur.execute("""
                    SELECT count(*) FROM pm_decision_log
                    WHERE ts >= date_trunc('day', now() AT TIME ZONE 'Asia/Seoul');
                """)
                total_dec = cur.fetchone()[0] or 0
                if total_dec > 0:
                    hold_pct = hold_cnt / total_dec * 100
                    lines.append(f'  결정 {total_dec}건 중 HOLD/SKIP {hold_cnt}건 ({hold_pct:.0f}%)')
                else:
                    lines.append('  오늘 결정 없음')
                # Claude denied count
                cur.execute("""
                    SELECT count(*) FROM claude_call_log
                    WHERE ts >= date_trunc('day', now() AT TIME ZONE 'Asia/Seoul')
                      AND NOT allowed;
                """)
                denied = cur.fetchone()[0] or 0
                if denied:
                    lines.append(f'  Claude 거부: {denied}건 (예산/쿨다운)')
            except Exception:
                pass

        lines.append('\n⚠ 적용 금지 — 분석 자료')
        return '\n'.join(lines)
    except Exception as e:
        return f'⚠ 테스트 보고 조회 실패: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ── /debug handler functions ─────────────────────────────

MAX_N_CAP = 200  # absolute max for --n parameter


_META_PARAM_KEYS = {'nonce', 'force_refresh', 'trace_id', 'cache', 'debug'}


def _parse_debug_args(text):
    """Unified debug argument parser.
    Returns dict with: n, mode, allow_only, window, from_month, extra_kv,
                       meta_args (known system params), ignored (truly unknown).
    """
    args = {
        'n': 20,
        'mode': 'latest',
        'allow_only': False,
        'window': '24h',
        'from_month': None,
        'extra_kv': {},
        'meta_args': {},
        'ignored': [],
        'n_capped': False,
    }
    if not text:
        return args

    # --n=<int>
    m = re.search(r'--n=(\d+)', text)
    if m:
        raw_n = int(m.group(1))
        if raw_n > MAX_N_CAP:
            args['n'] = MAX_N_CAP
            args['n_capped'] = True
        else:
            args['n'] = raw_n

    # allow_only=true/false
    m = re.search(r'allow_only\s*=\s*(true|false|1|0)', text, re.IGNORECASE)
    if m:
        args['allow_only'] = m.group(1).lower() in ('true', '1')

    # mode=<value>
    m = re.search(r'mode\s*=\s*(\S+)', text, re.IGNORECASE)
    if m:
        args['mode'] = m.group(1).lower()

    # window=<value>
    m = re.search(r'window\s*=\s*(\S+)', text, re.IGNORECASE)
    if m:
        args['window'] = m.group(1).lower()

    # --from=YYYY-MM
    m = re.search(r'--from=(\d{4}-\d{2})', text)
    if m:
        args['from_month'] = m.group(1)

    # Classify extra params: meta (system) vs ignored (truly unknown)
    known_keys = {'n', 'allow_only', 'mode', 'window', 'from'}
    for pm in re.finditer(r'(?:--|)(\w+)\s*=\s*(\S+)', text):
        pk = pm.group(1).lower()
        pv = pm.group(2)
        if pk in known_keys:
            continue
        if pk in _META_PARAM_KEYS:
            args['meta_args'][pk] = pv
        else:
            args['ignored'].append(pm.group(0))

    return args


# ── Service registry (Item 2) ────────────────────────────
# expected_interval_sec: how often the service should heartbeat
# systemctl_unit: the actual systemd unit name
PRICE_TABLE_CANONICAL = {
    '1m': {'table': 'candles', 'ts_col': 'ts', 'source': 'bybit kline (live collector)'},
    '5m': {'table': 'market_ohlcv', 'ts_col': 'ts', 'source': 'aggregated from candles_1m'},
}

SERVICE_REGISTRY = {
    'candles':        {'expected_interval_sec': 60,  'systemctl_unit': 'candles.service'},
    'executor':       {'expected_interval_sec': 60,  'systemctl_unit': 'dry_run_close_executor.service'},
    'indicators':     {'expected_interval_sec': 60,  'systemctl_unit': 'indicators.service'},
    'news_bot':       {'expected_interval_sec': 300, 'systemctl_unit': 'news_bot.service'},
    'pnl_watcher':    {'expected_interval_sec': 60,  'systemctl_unit': 'pnl_watcher.service'},
    'signal_logger':  {'expected_interval_sec': 120, 'systemctl_unit': 'signal_logger.service'},
    'vol_profile':    {'expected_interval_sec': 300, 'systemctl_unit': 'vol_profile.service'},
    'error_watcher':  {'expected_interval_sec': 300, 'systemctl_unit': 'error_watcher.service'},
}


def _debug_version(_text=None):
    """Build/version/environment info (Item 0: enhanced with timezone + db schema)."""
    import hashlib
    from datetime import datetime, timezone, timedelta
    lines = ['🔧 버전 정보', '━━━━━━━━━━━━━━━━━━']
    # git sha
    try:
        rc, sha = _run(['git', 'rev-parse', '--short', 'HEAD'], timeout=5)
        lines.append(f'git_sha: {sha if rc == 0 else "unknown"}')
    except Exception:
        lines.append('git_sha: unknown')
    # build_time = mtime of telegram_cmd_poller.py
    try:
        mtime = os.path.getmtime(f'{APP_DIR}/telegram_cmd_poller.py')
        bt = datetime.fromtimestamp(mtime, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        lines.append(f'build_time: {bt} UTC')
    except Exception:
        lines.append('build_time: unknown')
    # CONFIG_VERSION
    try:
        from telegram_cmd_poller import CONFIG_VERSION
        lines.append(f'config_version: {CONFIG_VERSION}')
        config_hash = hashlib.md5(CONFIG_VERSION.encode()).hexdigest()[:12]
        lines.append(f'config_hash: {config_hash}')
    except Exception:
        lines.append('config_version: unknown')
    # DB DSN masked — Item 0: host/db/schema explicit
    try:
        lines.append(f'db_dsn: host={DB_CONFIG.get("host", "?")} '
                     f'port={DB_CONFIG.get("port", "?")} '
                     f'db={DB_CONFIG.get("dbname", "?")} '
                     f'user={DB_CONFIG.get("user", "?")} schema=public')
    except Exception:
        lines.append('db_dsn: unknown')
    # process uptime
    uptime = _time.time() - _process_start_time
    lines.append(f'process_uptime_sec: {uptime:.1f}')
    # env
    env = os.getenv('ENV', 'production')
    lines.append(f'env: {env}')
    # query_ts with UTC+KST (Item 0)
    now_utc = datetime.now(timezone.utc)
    kst = timezone(timedelta(hours=9))
    now_kst = now_utc.astimezone(kst)
    lines.append(f'server_time: {now_utc.strftime("%Y-%m-%d %H:%M:%S")}UTC '
                 f'/ {now_kst.strftime("%Y-%m-%d %H:%M:%S")}KST')
    return '\n'.join(lines)


def _debug_router(_text=None):
    """Routing debug state (Item 1: always-populated fields)."""
    from datetime import datetime, timezone
    lines = ['🔀 라우터 디버그', '━━━━━━━━━━━━━━━━━━']
    # Lazy import to avoid circular import
    try:
        import telegram_cmd_poller as _tcp
        ds = _tcp._last_debug_state
        # Item 1: these must be non-empty — self-routing fills them
        detected = ds.get('detected_intent') or 'debug_router'
        handler = ds.get('selected_handler') or '_dispatch_debug(router)'
        lines.append(f'detected_intent: {detected}')
        lines.append(f'selected_handler: {handler}')
        lines.append(f'model_used: {ds.get("model_used", "none")}')
        lines.append(f'decision_ts: {ds.get("decision_ts") or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")}')
        lines.append(f'last_response_hash: {ds.get("last_response_hash") or "null"}')
        # last_llm_error: show null if empty
        llm_err = ds.get('last_llm_error', '')
        lines.append(f'last_llm_error: {llm_err if llm_err else "null"}')
        # Derive fallback_reason
        model = ds.get('model_used', 'none')
        if model == 'none':
            lines.append('fallback_reason: no_llm_call (direct handler)')
        elif 'gpt' in str(model).lower():
            lines.append('fallback_reason: claude_unavailable_or_budget')
        else:
            lines.append('fallback_reason: none')
        lines.append(f'cache_hit: {ds.get("cache_hit", "N/A")}')
    except Exception as e:
        lines.append(f'router_state: load failed ({e})')
    # Parse nonce from text
    nonce = ''
    if _text:
        m = re.search(r'nonce=(\S+)', _text)
        if m:
            nonce = m.group(1)
    lines.append(f'nonce: {nonce if nonce else "null"}')
    # GPT budget
    try:
        import gpt_router
        gpt_state = gpt_router._load_state()
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        gpt_calls = gpt_state.get('daily_calls', {}).get(today, 0)
        lines.append(f'gpt_budget: {gpt_calls}/{gpt_router.DAILY_BUDGET_LIMIT}')
    except Exception:
        pass
    # Claude gate budget
    try:
        import claude_gate
        gate_state = claude_gate._load_state()
        lines.append(f'claude_gate: calls={gate_state.get("daily_calls", 0)} '
                     f'cost=${gate_state.get("daily_cost", 0):.4f}')
    except Exception:
        pass
    return '\n'.join(lines)


def _debug_health(_text=None):
    """Service health (Item 2: registry-based, multi-source, specific reasons)."""
    from datetime import datetime, timezone
    lines = ['🩺 서비스 상태 (상세)', '━━━━━━━━━━━━━━━━━━']

    # Get systemctl --all output with timing
    t0 = _time.time()
    rc, sctl_out = _run(['systemctl', 'list-units', '--type=service', '--all'])
    sctl_ms = (_time.time() - t0) * 1000

    # Get heartbeat data from DB + observed intervals
    heartbeats = {}
    observed_intervals = {}
    hb_error = None
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT service, MAX(ts) AS last_ts
                FROM service_health_log
                GROUP BY service;
            """)
            for svc, last_ts in cur.fetchall():
                heartbeats[svc] = last_ts
            # Observed median interval per service over last 2h
            try:
                cur.execute("""
                    SELECT service,
                           PERCENTILE_CONT(0.5) WITHIN GROUP (
                               ORDER BY gap_sec
                           ) AS median_interval
                    FROM (
                        SELECT service,
                               EXTRACT(EPOCH FROM (ts - LAG(ts) OVER (
                                   PARTITION BY service ORDER BY ts
                               ))) AS gap_sec
                        FROM service_health_log
                        WHERE ts >= now() - interval '2 hours'
                    ) sub
                    WHERE gap_sec IS NOT NULL AND gap_sec > 0
                    GROUP BY service;
                """)
                for svc, median in cur.fetchall():
                    observed_intervals[svc] = round(float(median), 1) if median else None
            except Exception:
                pass
    except Exception as e:
        hb_error = str(e)
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    ok_count = 0
    down_count = 0
    unknown_count = 0
    now_utc = datetime.now(timezone.utc)
    states_for_log = {}

    for svc in WATCHED_SERVICES:
        reg = SERVICE_REGISTRY.get(svc, {})
        expected_interval = reg.get('expected_interval_sec', 0)
        unit_name = reg.get('systemctl_unit', f'{svc}.service')

        t1 = _time.time()

        # ── Check 1: Heartbeat from DB ──
        hb_ts = heartbeats.get(svc)
        hb_state = None
        hb_reason = None
        age_sec = None
        if hb_error:
            hb_reason = f'db_error:{hb_error[:40]}'
        elif hb_ts:
            if hb_ts.tzinfo is None:
                hb_ts = hb_ts.replace(tzinfo=timezone.utc)
            age_sec = int((now_utc - hb_ts).total_seconds())
            if expected_interval > 0:
                stale_threshold = 3 * expected_interval
                if age_sec < stale_threshold:
                    hb_state = 'OK'
                    hb_reason = f'heartbeat_fresh (age={age_sec}s < {stale_threshold}s)'
                else:
                    hb_state = 'DOWN'
                    hb_reason = f'heartbeat_stale (age={age_sec}s >= {stale_threshold}s)'
            else:
                hb_reason = 'missing_expected_interval'
        else:
            hb_reason = 'no_heartbeat_rows'

        # ── Check 2: systemctl process ──
        proc_state = None
        proc_reason = None
        if rc != 0:
            proc_reason = 'systemctl_failed'
        else:
            found = False
            matched_line = ''
            for line in sctl_out.splitlines():
                if unit_name in line:
                    found = True
                    matched_line = line
                    break
            if not found:
                proc_reason = f'unit_not_found ({unit_name})'
            else:
                ll = matched_line.lower()
                if 'active' in ll and 'running' in ll:
                    proc_state = 'OK'
                    proc_reason = 'active_running'
                elif 'failed' in ll:
                    proc_state = 'DOWN'
                    proc_reason = 'systemctl_failed'
                elif 'inactive' in ll and 'dead' in ll:
                    proc_state = 'DOWN'
                    proc_reason = 'inactive_dead'
                elif 'activating' in ll:
                    proc_reason = 'activating'
                elif 'masked' in ll:
                    proc_reason = 'masked'
                else:
                    proc_reason = f'parse_unknown ({matched_line.strip()[:60]})'

        check_ms = (_time.time() - t1) * 1000

        # ── Final verdict: process alive + hb stale → OK+WARN (not DOWN) ──
        if hb_state == 'OK':
            state = 'OK'
            reason = hb_reason
        elif proc_state == 'OK' and hb_state == 'DOWN':
            state = 'OK'
            reason = f'WARN: {hb_reason} (process alive)'
        elif proc_state == 'OK':
            state = 'OK'
            reason = proc_reason
        elif hb_state == 'DOWN':
            state = 'DOWN'
            reason = hb_reason
        elif proc_state == 'DOWN':
            state = 'DOWN'
            reason = proc_reason
        else:
            state = 'UNKNOWN'
            reasons = []
            if hb_reason:
                reasons.append(f'hb:{hb_reason}')
            if proc_reason:
                reasons.append(f'proc:{proc_reason}')
            reason = '; '.join(reasons) if reasons else 'no_check_source'

        states_for_log[svc] = state
        icon = STATE_ICONS[state]

        # Heartbeat display
        if hb_ts and age_sec is not None:
            hb_display = f'{hb_ts.strftime("%m-%d %H:%M")} ({age_sec}s ago)'
        else:
            hb_display = 'null'

        is_required = svc in REQUIRED_SERVICES

        lines.append(f'{icon} {svc}: {state}')
        lines.append(f'  reason={reason}')
        if expected_interval > 0:
            threshold = 2 * expected_interval
            obs = observed_intervals.get(svc)
            obs_str = f'{obs:.0f}s' if obs is not None else 'N/A'
            interval_line = (f'  expected={expected_interval}s '
                             f'threshold={threshold}s '
                             f'observed_interval={obs_str}')
            if obs is not None and obs > threshold:
                interval_line += ' ⚠ interval_mismatch'
            lines.append(interval_line)
        lines.append(f'  heartbeat={hb_display} | check={check_ms:.0f}ms')

        if state == 'OK':
            ok_count += 1
        elif state == 'DOWN':
            down_count += 1
        else:
            unknown_count += 1

    # Split summary: required vs optional
    req_ok = sum(1 for s in REQUIRED_SERVICES
                 if states_for_log.get(s) == 'OK')
    req_down = sum(1 for s in REQUIRED_SERVICES
                   if states_for_log.get(s) == 'DOWN')
    opt_ok = sum(1 for s in OPTIONAL_SERVICES
                 if states_for_log.get(s) == 'OK')
    opt_down = sum(1 for s in OPTIONAL_SERVICES
                   if states_for_log.get(s) == 'DOWN')

    total = len(WATCHED_SERVICES)
    lines.insert(2, f'전체: {total} | OK: {ok_count} | DOWN: {down_count} | UNKNOWN: {unknown_count}')
    lines.insert(3, f'required: {req_ok} OK {req_down} DOWN | '
                    f'optional: {opt_ok} OK {opt_down} DOWN')
    lines.insert(4, f'systemctl_latency: {sctl_ms:.0f}ms')
    lines.insert(5, '')

    _log_service_health(states_for_log)
    return '\n'.join(lines)


def _debug_gate_details(_text=None):
    """Gate details: per-service OK/DOWN/UNKNOWN + reason + age_sec + gate verdict."""
    snapshot = get_service_health_snapshot()
    services = snapshot.get('services', {})

    lines = ['🔒 Gate 상세 (dual-source)', '━━━━━━━━━━━━━━━━━━']

    for svc in WATCHED_SERVICES:
        info = services.get(svc, {})
        state = info.get('state', 'UNKNOWN')
        reason = info.get('reason', '?')
        age = info.get('age_sec')
        icon = STATE_ICONS.get(state, '❓')
        is_req = svc in REQUIRED_SERVICES
        tag = '' if is_req else ' (선택)'
        age_str = f' age={age}s' if age is not None else ''
        lines.append(f'{icon} {svc}: {state}{tag}{age_str}')
        lines.append(f'  reason={reason}')

    req_down = snapshot.get('required_down', [])
    req_unknown = snapshot.get('required_unknown', [])

    lines.append('')
    lines.append(f'OK: {snapshot.get("ok", 0)} | DOWN: {len(snapshot.get("down", []))} | UNKNOWN: {len(snapshot.get("unknown", []))}')
    lines.append(f'required_down: {req_down or "없음"}')
    lines.append(f'required_unknown: {req_unknown or "없음"}')

    # Gate verdict
    if req_down:
        verdict = f'BLOCKED (필수 서비스 중지: {", ".join(req_down)})'
    elif req_unknown:
        verdict = f'WARN (필수 서비스 미확인: {", ".join(req_unknown)} — 차단 안 함)'
    else:
        verdict = 'PASS'
    lines.append(f'gate_verdict: {verdict}')
    lines.append(f'health_check_ts: {snapshot.get("health_check_ts", "?")}')

    # ── Extended: Regime + Entry Filter + Throttle + Rejection ──
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            # Current regime + confidence
            try:
                import regime_reader
                rc = regime_reader.get_current_regime(cur)
                lines.append('')
                lines.append(f'regime: {rc.get("regime")} (confidence={rc.get("confidence")}%, '
                             f'transition={rc.get("in_transition")}, stale={rc.get("stale")})')
            except Exception as e:
                lines.append(f'regime: error ({e})')

            # Trade switch status
            try:
                sw = exchange_reader.fetch_trade_switch_status()
                lines.append(f'trade_switch: {sw}')
            except Exception as e:
                lines.append(f'trade_switch: error ({e})')

            # Throttle status
            try:
                import order_throttle
                state = order_throttle.get_state_snapshot()
                lines.append(f'throttle: attempts_1h={state.get("attempts_1h", "?")} '
                             f'cooldown_remaining={state.get("cooldown_remaining", 0):.0f}s '
                             f'entry_locked={state.get("entry_locked", False)}')
                if state.get('last_reject_reason'):
                    lines.append(f'  last_reject: {state.get("last_reject_reason")} '
                                 f'(cooldown={state.get("rejection_cooldown_remaining", 0):.0f}s)')
            except Exception as e:
                lines.append(f'throttle: error ({e})')
    except Exception:
        pass
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    return '\n'.join(lines)


def _debug_db_coverage(_text=None):
    """DB coverage (Item 3: gap diagnosis + alt table discovery + filter transparency)."""
    from_month = '2023-11'
    if _text:
        m = re.search(r'--from=(\d{4}-\d{2})', _text)
        if m:
            from_month = m.group(1)

    conn = None
    try:
        conn = _db()
        lines = [f'📊 DB 커버리지 ({from_month}~현재)', '━━━━━━━━━━━━━━━━━━']
        with conn.cursor() as cur:
            tables_config = [
                ('candles', 'ts', 'no filter (all symbols/tf)'),
                ('events', 'start_ts', 'no filter'),
                ('news', 'ts', 'no filter (all sources)'),
            ]
            gap_info = {}  # tbl -> [(gap_start, gap_end)]

            for tbl, ts_col, filter_desc in tables_config:
                try:
                    cur.execute(f"""
                        SELECT MIN({ts_col})::text, MAX({ts_col})::text
                        FROM {tbl};
                    """)
                    rng = cur.fetchone()
                    earliest = (rng[0] or '-')[:16] if rng else '-'
                    latest = (rng[1] or '-')[:16] if rng else '-'

                    cur.execute(f"""
                        SELECT to_char(m, 'YYYY-MM') AS month,
                               COALESCE(c.cnt, 0) AS cnt
                        FROM generate_series(
                            %s::date,
                            date_trunc('month', now())::date,
                            '1 month'::interval
                        ) AS m
                        LEFT JOIN (
                            SELECT date_trunc('month', {ts_col}) AS mo,
                                   count(*) AS cnt
                            FROM {tbl}
                            WHERE {ts_col} >= %s::date
                            GROUP BY mo
                        ) c ON c.mo = m
                        ORDER BY m;
                    """, (f'{from_month}-01', f'{from_month}-01'))
                    rows = cur.fetchall()

                    lines.append(f'\n[{tbl}] ts_col={ts_col} | filter: {filter_desc}')
                    lines.append(f'  range: {earliest} ~ {latest}')
                    gap_count = 0
                    gaps = []
                    gap_start = None
                    prev_month = None
                    for month, cnt in rows:
                        gap_tag = ' <<< GAP' if cnt == 0 else ''
                        if cnt == 0:
                            gap_count += 1
                            if gap_start is None:
                                gap_start = month
                        else:
                            if gap_start is not None and prev_month is not None:
                                gaps.append((gap_start, prev_month))
                                gap_start = None
                        prev_month = month
                        lines.append(f'  {month}: {cnt:,}건{gap_tag}')
                    if gap_start is not None and prev_month is not None:
                        gaps.append((gap_start, prev_month))
                    if gap_count > 0:
                        lines.append(f'  GAPS: {gap_count} months with 0 rows')
                    gap_info[tbl] = gaps
                except Exception as e:
                    lines.append(f'\n[{tbl}] 조회 실패: {e}')

            # ── Item 3B: Alternative table discovery ──
            lines.append('\n[대체 테이블 탐색]')
            try:
                cur.execute("""
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND (table_name ILIKE '%%candle%%'
                           OR table_name ILIKE '%%ohlcv%%'
                           OR table_name ILIKE '%%news%%'
                           OR table_name ILIKE '%%article%%')
                    ORDER BY table_name;
                """)
                candidates = [r[0] for r in cur.fetchall()]
                if candidates:
                    for cand in candidates:
                        try:
                            # Find a ts column
                            cur.execute("""
                                SELECT column_name FROM information_schema.columns
                                WHERE table_name = %s AND table_schema = 'public'
                                  AND (column_name ILIKE '%%ts%%'
                                       OR column_name ILIKE '%%date%%'
                                       OR column_name ILIKE '%%time%%'
                                       OR column_name ILIKE '%%created%%')
                                ORDER BY ordinal_position LIMIT 1;
                            """, (cand,))
                            ts_r = cur.fetchone()
                            ts_c = ts_r[0] if ts_r else None
                            cur.execute(f"SELECT count(*) FROM {cand};")
                            cnt = cur.fetchone()[0] or 0
                            if ts_c:
                                cur.execute(f"SELECT MIN({ts_c})::text, MAX({ts_c})::text FROM {cand};")
                                rng = cur.fetchone()
                                e = (rng[0] or '-')[:16] if rng else '-'
                                l = (rng[1] or '-')[:16] if rng else '-'
                                lines.append(f'  {cand}: {cnt:,}건 ({e} ~ {l}) ts_col={ts_c}')
                            else:
                                lines.append(f'  {cand}: {cnt:,}건 (no ts column found)')
                        except Exception:
                            lines.append(f'  {cand}: 조회 실패')
                else:
                    lines.append('  none found')
            except Exception as e:
                lines.append(f'  탐색 실패: {e}')

            # ── Item 3C: Gap cause analysis ──
            lines.append('\n[GAP 원인 진단]')
            for tbl, gaps in gap_info.items():
                if not gaps:
                    lines.append(f'  [{tbl}] no gaps')
                    continue
                for gs, ge in gaps:
                    lines.append(f'  [{tbl}] gap={gs}..{ge}')
                    # Determine primary cause with evidence
                    if tbl == 'candles':
                        lines.append(f'    primary: collector started recently '
                                     f'(evidence: data only from latest period)')
                        lines.append(f'    alt1: historical backfill not yet run')
                        lines.append(f'    alt2: data in market_ohlcv table instead')
                    elif tbl == 'news':
                        # Check if events has data in same period (evidence of system running)
                        try:
                            cur.execute("""
                                SELECT count(*) FROM events
                                WHERE start_ts >= %s::date AND start_ts < %s::date + interval '1 month';
                            """, (f'{gs}-01', f'{ge}-01'))
                            evt_cnt = cur.fetchone()[0] or 0
                        except Exception:
                            evt_cnt = -1
                        if evt_cnt > 0:
                            lines.append(f'    primary: news collector stopped or switched table '
                                         f'(evidence: events has {evt_cnt} rows in same period)')
                        else:
                            lines.append(f'    primary: system may not have been running')
                        lines.append(f'    alt1: rows in news_raw/news_market_reaction tables')
                        lines.append(f'    alt2: ts column mismatch (published_at vs ingested_at)')
                    else:
                        lines.append(f'    primary: data collection not active in this period')

            # ── News tier UNKNOWN monthly (Item 3D) ──
            lines.append('\n[뉴스 tier=UNKNOWN 월별]')
            try:
                cur.execute("""
                    SELECT to_char(m, 'YYYY-MM') AS month,
                           COALESCE(u.unknown_cnt, 0) AS unknown_cnt,
                           COALESCE(t.total_cnt, 0) AS total_cnt
                    FROM generate_series(
                        %s::date,
                        date_trunc('month', now())::date,
                        '1 month'::interval
                    ) AS m
                    LEFT JOIN (
                        SELECT date_trunc('month', ts) AS mo,
                               count(*) AS unknown_cnt
                        FROM news
                        WHERE ts >= %s::date
                          AND (tier IS NULL OR tier = 'UNKNOWN')
                        GROUP BY mo
                    ) u ON u.mo = m
                    LEFT JOIN (
                        SELECT date_trunc('month', ts) AS mo,
                               count(*) AS total_cnt
                        FROM news
                        WHERE ts >= %s::date
                        GROUP BY mo
                    ) t ON t.mo = m
                    ORDER BY m;
                """, (f'{from_month}-01', f'{from_month}-01', f'{from_month}-01'))
                rows = cur.fetchall()
                total_unknown = 0
                total_all = 0
                for month, unk, tot in rows:
                    if tot == 0:
                        lines.append(f'  {month}: N/A (no data)')
                    else:
                        pct = unk / tot * 100
                        lines.append(f'  {month}: {unk}/{tot} ({pct:.1f}%)')
                    total_unknown += unk
                    total_all += tot
                if total_all > 0:
                    overall_pct = total_unknown / total_all * 100
                    lines.append(f'  전체: {total_unknown}/{total_all} ({overall_pct:.1f}%)')
                    if overall_pct >= 99:
                        lines.append(f'  diagnosis: classification gated or not executed '
                                     f'(APPROVAL_REQUIRED=True in news_classifier_config)')
            except Exception as e:
                lines.append(f'  조회 실패: {e}')

            # ── Item 6: Canonical price tables ──
            lines.append('\n[canonical price tables]')
            for tf, cfg in PRICE_TABLE_CANONICAL.items():
                tbl = cfg['table']
                ts_c = cfg['ts_col']
                src = cfg['source']
                try:
                    cur.execute(f"""
                        SELECT count(*),
                               MIN({ts_c})::text,
                               MAX({ts_c})::text,
                               count(*) FILTER (WHERE {ts_c} >= now() - interval '24 hours')
                        FROM {tbl};
                    """)
                    r = cur.fetchone()
                    total, mn, mx, recent = r
                    mn = (mn or '-')[:16]
                    mx = (mx or '-')[:16]
                    lines.append(f'  {tf}: {tbl} | {total:,}건 (24h: {recent:,}) | {mn} ~ {mx}')
                    lines.append(f'    source={src}')
                except Exception as e:
                    lines.append(f'  {tf}: {tbl} 조회 실패 ({e})')

            # ── Item 6: Symbol/tf distribution (top 5) ──
            lines.append('\n[symbol/tf distribution (top 5)]')
            try:
                cur.execute("""
                    SELECT symbol, tf, count(*) AS cnt
                    FROM candles
                    GROUP BY symbol, tf
                    ORDER BY cnt DESC
                    LIMIT 5;
                """)
                dist_rows = cur.fetchall()
                if dist_rows:
                    for sym, tf, cnt in dist_rows:
                        lines.append(f'  {sym} / {tf}: {cnt:,}건')
                else:
                    lines.append('  데이터 없음')
            except Exception as e:
                lines.append(f'  조회 실패: {e}')

        return '\n'.join(lines)
    except Exception as e:
        return f'⚠ DB 커버리지 조회 실패: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _debug_news_sample(_text=None):
    """News sample with modes: latest (default), allow (allow_only), deny_reason:<reason>.
    Unified param parsing via _parse_debug_args."""
    args = _parse_debug_args(_text)
    n = args['n']
    mode = args['mode']
    allow_only = args['allow_only']

    # allow_only=true overrides mode
    if allow_only:
        mode = 'allow'

    # Determine scan window — allow/deny modes need wider scan
    max_scan = n if mode == 'latest' else 1000

    # deny_reason filter
    deny_filter = None
    if mode.startswith('deny_reason:'):
        deny_filter = mode.split(':', 1)[1]
        mode = 'deny_reason'

    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            # Always fetch 24h summary stats first
            cur.execute("""
                SELECT count(*),
                       count(*) FILTER (WHERE source ILIKE 'yahoo_finance') AS yahoo_cnt
                FROM news
                WHERE ts >= now() - interval '24 hours';
            """)
            sr = cur.fetchone()
            total_24h = sr[0] or 0
            yahoo_24h = sr[1] or 0

            # Fetch rows for scan
            cur.execute("""
                SELECT id, title_ko, tier, topic_class, relevance_score,
                       source, impact_score, title, summary,
                       to_char(ts AT TIME ZONE 'Asia/Seoul', 'MM-DD HH24:MI') AS ts_kr
                FROM news
                WHERE ts >= now() - interval '24 hours'
                ORDER BY ts DESC
                LIMIT %s;
            """, (max_scan,))
            rows = cur.fetchall()

        if not rows:
            return f'📰 뉴스 샘플\n━━━━━━━━━━━━━━━━━━\n데이터 없음 (최근 24시간)'

        # Import preview classifier
        import news_classifier_config as ncc

        # Classify all scanned rows
        classified = []
        deny_stats = {}
        source_stats = {}
        storage_count_24h = 0
        trading_count_24h = 0
        last_trading_ts = None

        for r in rows:
            nid, title_ko, tier, topic, rel, source, impact, title_en, summary, ts = r
            pv = ncc.preview_classify(
                title=title_en or '', source=source or '',
                impact_score=impact, summary=summary or '',
                title_ko=title_ko or '')
            if pv.get('allow_for_storage', False):
                storage_count_24h += 1
            if pv.get('allow_for_trading', False):
                trading_count_24h += 1
                if last_trading_ts is None:
                    last_trading_ts = ts
            for dr in pv.get('deny_reasons', []):
                deny_stats[dr] = deny_stats.get(dr, 0) + 1
            src = (source or 'unknown').lower()
            source_stats[src] = source_stats.get(src, 0) + 1
            classified.append((r, pv))

        # Filter by mode
        if mode == 'allow':
            # allow_only=true → show allow_for_trading items
            filtered = [(r, pv) for r, pv in classified if pv.get('allow_for_trading', False)]
        elif mode == 'deny_reason' and deny_filter:
            filtered = [(r, pv) for r, pv in classified
                        if deny_filter in pv.get('deny_reasons', [])]
        else:
            # latest mode
            filtered = classified

        # Apply N limit
        display_items = filtered[:n]

        # Build 24h summary line
        top_deny = sorted(deny_stats.items(), key=lambda x: -x[1])[:1]
        top_deny_str = f'{top_deny[0][0]}({top_deny[0][1]})' if top_deny else 'none'
        top_src = sorted(source_stats.items(), key=lambda x: -x[1])[:1]
        top_src_str = f'{top_src[0][0]}({top_src[0][1]})' if top_src else 'none'

        # Header
        scanned = len(classified)
        mode_label = mode
        if mode == 'allow':
            mode_label = 'allow_only=true'
        elif mode == 'deny_reason':
            mode_label = f'deny_reason:{deny_filter}'

        lines = [
            f'📰 뉴스 샘플 v2 [APPLIED={str(not ncc.APPROVAL_REQUIRED).lower()}]',
            '━━━━━━━━━━━━━━━━━━',
            f'last_24h_total={total_24h}',
            f'  allow_storage={storage_count_24h} | allow_trading={trading_count_24h}',
            f'  top_deny={top_deny_str} | top_src={top_src_str}',
        ]
        if args['n_capped']:
            lines.append(f'mode={mode_label} | --n={n} (capped_to={MAX_N_CAP}) | '
                         f'window=24h | scanned={scanned}')
        else:
            lines.append(f'mode={mode_label} | --n={n} | '
                         f'window=24h | scanned={scanned}')
        if args['meta_args']:
            meta_str = ', '.join(f'{k}={v}' for k, v in args['meta_args'].items())
            lines.append(f'meta_args: {meta_str}')
        if args['ignored']:
            lines.append(f'ignored_args={args["ignored"]}')

        if mode == 'allow':
            lines.append(f'allow_trading_found={len(filtered)}')

        lines.append('')

        # If allow mode and nothing found, show cause summary
        if mode == 'allow' and not display_items:
            lines.append('allow_trading_found=0 — 원인 요약:')
            top3_deny = sorted(deny_stats.items(), key=lambda x: -x[1])[:3]
            for dr, cnt in top3_deny:
                lines.append(f'  deny: {dr} = {cnt}건')
            top3_src = sorted(source_stats.items(), key=lambda x: -x[1])[:3]
            for s, cnt in top3_src:
                lines.append(f'  source: {s} = {cnt}건')
            if last_trading_ts:
                lines.append(f'  last_trading_ts={last_trading_ts}')
            else:
                lines.append(f'  last_trading_ts=none (24h 내 trading 후보 없음)')
            return '\n'.join(lines)

        # Display items
        for r, pv in display_items:
            nid, title_ko, tier, topic, rel, source, impact, title_en, summary, ts = r
            display = (title_ko or title_en or '?')[:50]
            impact_s = f'{impact}/10' if impact is not None else '-'
            s_icon = 'S' if pv.get('allow_for_storage', False) else '-'
            t_icon = 'T' if pv.get('allow_for_trading', False) else '-'
            deny = ','.join(pv['deny_reasons']) if pv['deny_reasons'] else '-'
            sw = pv.get('source_weight', pv.get('source_quality_preview', 0))
            lines.append(f'[{pv["tier_preview"]}] {display}')
            lines.append(f'  topic={pv["topic_class_preview"]} '
                         f'rel={pv["relevance_score_preview"]:.2f} '
                         f'w={sw:.2f} impact={impact_s}')
            lines.append(f'  S={s_icon} T={t_icon} deny={deny} '
                         f'src={source or "-"} {ts}')
        return '\n'.join(lines)
    except Exception as e:
        return f'⚠ 뉴스 샘플 조회 실패: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _debug_news_reaction_sample(_text=None):
    """News reaction sample with raw/eligible coverage split."""
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            # Horizon-specific coverage
            cur.execute("""
                SELECT
                    count(*) AS total_traced,
                    count(*) FILTER (WHERE btc_ret_30m IS NOT NULL) AS has_30m,
                    count(*) FILTER (WHERE btc_ret_2h IS NOT NULL) AS has_2h,
                    count(*) FILTER (WHERE btc_ret_24h IS NOT NULL) AS has_24h
                FROM macro_trace;
            """)
            cov = cur.fetchone()
            total_traced = cov[0] or 0
            has_30m = cov[1] or 0
            has_2h = cov[2] or 0
            has_24h = cov[3] or 0

            cur.execute("SELECT count(*) FROM news;")
            total_news = cur.fetchone()[0] or 0
            raw_pct = (total_traced / total_news * 100) if total_news > 0 else 0

            # Eligible news: news within candle coverage range + 24h lookahead
            cur.execute("""
                SELECT count(*) FROM news n
                WHERE n.ts >= (SELECT MIN(ts) FROM candles WHERE tf='1m')
                  AND n.ts + interval '24 hours' <= (SELECT MAX(ts) FROM candles WHERE tf='1m')
                  AND n.ts < now() - interval '24 hours';
            """)
            eligible_news = cur.fetchone()[0] or 0

            # Eligible traced
            cur.execute("""
                SELECT count(*) FROM macro_trace mt
                JOIN news n ON n.id = mt.news_id
                WHERE n.ts >= (SELECT MIN(ts) FROM candles WHERE tf='1m')
                  AND n.ts + interval '24 hours' <= (SELECT MAX(ts) FROM candles WHERE tf='1m');
            """)
            eligible_traced = cur.fetchone()[0] or 0
            eligible_pct = (eligible_traced / eligible_news * 100) if eligible_news > 0 else 0
            traced_pct = raw_pct

            # Prefer rows with most complete data (ORDER BY completeness)
            cur.execute("""
                SELECT n.tier, n.title_ko, n.impact_score,
                       mt.btc_ret_30m, mt.btc_ret_2h, mt.btc_ret_24h,
                       mt.label, mt.regime_at_time,
                       to_char(n.ts AT TIME ZONE 'Asia/Seoul', 'MM-DD HH24:MI') AS ts_kr,
                       n.ts AS raw_ts,
                       mt.computed_at
                FROM news n
                JOIN macro_trace mt ON mt.news_id = n.id
                WHERE mt.btc_ret_30m IS NOT NULL
                ORDER BY
                    (CASE WHEN mt.btc_ret_24h IS NOT NULL THEN 3
                          WHEN mt.btc_ret_2h IS NOT NULL THEN 2
                          ELSE 1 END) DESC,
                    n.ts DESC
                LIMIT 10;
            """)
            rows = cur.fetchall()

        lines = [
            '📰 뉴스 반응 샘플',
            '━━━━━━━━━━━━━━━━━━',
            f'raw_coverage: {total_traced}/{total_news} ({raw_pct:.1f}%)',
            f'eligible_coverage: {eligible_traced}/{eligible_news} ({eligible_pct:.1f}%)',
            f'  (eligible=캔들 커버리지 범위 내 + 24h 룩어헤드 존재)',
            f'  30m: {has_30m}/{total_traced} | '
            f'2h: {has_2h}/{total_traced} | '
            f'24h: {has_24h}/{total_traced}',
            '',
        ]
        if not rows:
            lines.append('반응 데이터 없음')
        else:
            from datetime import datetime, timezone, timedelta
            now_utc = datetime.now(timezone.utc)
            for r in rows:
                tier, title, impact, r30, r2h, r24h, label, regime, ts, raw_ts, computed_at = r
                title = (title or '?')[:45]
                tier = tier or 'UNKNOWN'
                r30s = f'{r30:+.2f}%' if r30 is not None else '-'
                # For 2h/24h: show pending reason if null
                if r2h is not None:
                    r2hs = f'{r2h:+.2f}%'
                elif raw_ts:
                    if raw_ts.tzinfo is None:
                        raw_ts = raw_ts.replace(tzinfo=timezone.utc)
                    age_h = (now_utc - raw_ts).total_seconds() / 3600
                    if age_h < 2:
                        ready = raw_ts + timedelta(hours=2)
                        r2hs = f'pending(ready={ready.strftime("%H:%M")}UTC)'
                    else:
                        r2hs = 'missing(horizon_passed)'
                else:
                    r2hs = '-'

                if r24h is not None:
                    r24hs = f'{r24h:+.2f}%'
                elif raw_ts:
                    if raw_ts.tzinfo is None:
                        raw_ts = raw_ts.replace(tzinfo=timezone.utc)
                    age_h = (now_utc - raw_ts).total_seconds() / 3600
                    if age_h < 24:
                        ready = raw_ts + timedelta(hours=24)
                        r24hs = f'pending(ready={ready.strftime("%m-%d %H:%M")}UTC)'
                    else:
                        r24hs = 'missing(horizon_passed)'
                else:
                    r24hs = '-'

                impact_s = f'{impact}/10' if impact is not None else '-'
                lines.append(f'[{tier}] {title}')
                lines.append(f'  30m={r30s} 2h={r2hs} 24h={r24hs}')
                lines.append(f'  label={label or "-"} regime={regime or "-"} '
                             f'impact={impact_s} {ts}')
        return '\n'.join(lines)
    except Exception as e:
        return f'⚠ 뉴스 반응 샘플 조회 실패: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _backfill_dryrun_summary(cur):
    """Compact 1-line-per-job dryrun summary for embedding in backfill_status.
    Returns list of summary lines."""
    summary = []
    try:
        cur.execute("""
            SELECT EXTRACT(EPOCH FROM (now() - '2023-11-01'::timestamp)) / 60 AS expected,
                   (SELECT count(*) FROM candles WHERE tf = '1m') AS actual;
        """)
        r = cur.fetchone()
        exp, act = int(r[0] or 0), int(r[1] or 0)
        summary.append(f'  candles_1m: remaining={max(0, exp - act):,}')
    except Exception:
        summary.append(f'  candles_1m: 조회 실패')

    try:
        cur.execute("""
            SELECT EXTRACT(EPOCH FROM (now() - '2023-11-01'::timestamp)) / 300 AS expected,
                   (SELECT count(*) FROM market_ohlcv) AS actual;
        """)
        r = cur.fetchone()
        exp, act = int(r[0] or 0), int(r[1] or 0)
        summary.append(f'  ohlcv_5m: remaining={max(0, exp - act):,}')
    except Exception:
        summary.append(f'  ohlcv_5m: 조회 실패')

    try:
        cur.execute("""
            SELECT count(*) FILTER (WHERE tier IS NULL OR tier = 'UNKNOWN'),
                   count(*)
            FROM news;
        """)
        r = cur.fetchone()
        summary.append(f'  news_classify: remaining={r[0] or 0:,}')
    except Exception:
        summary.append(f'  news_classify: 조회 실패')

    try:
        cur.execute("""
            SELECT (SELECT count(*) FROM news) - (SELECT count(DISTINCT news_id) FROM macro_trace);
        """)
        r = cur.fetchone()
        summary.append(f'  macro_trace: remaining={max(0, r[0] or 0):,}')
    except Exception:
        summary.append(f'  macro_trace: 조회 실패')

    try:
        cur.execute("SELECT count(*) FROM price_events;")
        cnt = cur.fetchone()[0] or 0
        if cnt == 0:
            summary.append(f'  ⚠ price_events: never_run (0건) — 백필 필수')
        else:
            summary.append(f'  price_events: {cnt:,}건')
    except Exception:
        summary.append(f'  ⚠ price_events: 테이블 미생성 — 백필 필수')

    return summary


def _debug_backfill_status(_text=None):
    """Backfill status (Item 6: progress % + ETA + gated note)."""
    from backfill_utils import (
        get_running_pid, is_backfill_enabled, read_exit_status,
        PAUSE_FILE, STOP_FILE,
    )

    conn = None
    try:
        conn = _db()

        # Runner status header
        runner_pid = get_running_pid()
        enabled = is_backfill_enabled()
        gate_str = '✅ enabled' if enabled else '🔒 disabled'

        runner_lines = []
        if runner_pid:
            state = 'RUNNING'
            if os.path.exists(PAUSE_FILE):
                state = 'PAUSED'
            elif os.path.exists(STOP_FILE):
                state = 'STOPPING'
            runner_lines.append(f'[Runner] PID={runner_pid} state={state} gate={gate_str}')
        else:
            runner_lines.append(f'[Runner] 미실행 (gate={gate_str})')
            # Show last exit reason if runner is dead
            exit_info = read_exit_status()
            if exit_info:
                runner_lines.append(
                    f'[Last Exit] {exit_info.get("status", "?")} '
                    f'@ {exit_info.get("ts", "?")} '
                    f'job={exit_info.get("job_key", "?")}'
                )
                reason = exit_info.get('reason', '')
                if reason:
                    runner_lines.append(f'  reason: {reason[:200]}')

        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (job_name)
                       job_name, status, inserted, updated, failed,
                       started_at, finished_at, last_cursor, error, metadata
                FROM backfill_job_runs
                ORDER BY job_name, started_at DESC;
            """)
            rows = cur.fetchall()

        if not rows:
            lines = ['📦 백필 작업 현황', '━━━━━━━━━━━━━━━━━━']
            lines.extend(runner_lines)
            lines.extend(['', 'backfill_job_runs 데이터 없음 (아직 실행한 적 없음)', ''])
            lines.append('[잔여 작업 요약 (dryrun)]')
            try:
                with conn.cursor() as cur2:
                    lines.extend(_backfill_dryrun_summary(cur2))
            except Exception:
                lines.append('  dryrun 요약 조회 실패')
            lines.append('')
            if not enabled:
                lines.append('⚠ /debug backfill_enable on → /debug backfill_start job=... write=true')
            return '\n'.join(lines)

        status_icons = {'SUCCESS': '✅', 'RUNNING': '🔄', 'FAILED': '❌',
                        'COMPLETED': '✅', 'PARTIAL': '⏸'}
        lines = ['📦 백필 작업 현황', '━━━━━━━━━━━━━━━━━━']
        lines.extend(runner_lines)
        lines.append('')
        # Build reverse alias map: canonical -> [aliases]
        _reverse_aliases = {}
        for alias, canonical in _BACKFILL_JOB_ALIASES.items():
            _reverse_aliases.setdefault(canonical, []).append(alias)

        for r in rows:
            job, status, ins, upd, fail, started, finished, cursor, err, meta = r
            icon = status_icons.get(status, '❓')
            ins = ins or 0
            upd = upd or 0
            fail = fail or 0

            # Elapsed
            if started and finished:
                elapsed_sec = (finished - started).total_seconds()
                elapsed_str = f'{elapsed_sec / 60:.1f}min'
            elif started:
                elapsed_sec = _time.time() - started.timestamp()
                elapsed_str = f'{elapsed_sec / 60:.1f}min (running)'
            else:
                elapsed_sec = 0
                elapsed_str = '-'

            # Progress % from metadata if available
            remaining = None
            if meta and isinstance(meta, dict):
                remaining = meta.get('remaining')
            total_est = (ins + (remaining or 0)) if remaining else None
            alias_list = _reverse_aliases.get(job, [])
            alias_tag = f' (alias: {", ".join(alias_list)})' if alias_list else ''
            if total_est and total_est > 0:
                pct = ins / total_est * 100
                lines.append(f'{icon} {job}{alias_tag}: {status} ({pct:.1f}%)')
            else:
                lines.append(f'{icon} {job}{alias_tag}: {status}')

            lines.append(f'  inserted={ins:,} updated={upd:,} failed={fail:,} '
                         f'elapsed={elapsed_str}')
            lines.append(f'  started={str(started)[:19] if started else "-"}')

            # Show start/end range from metadata
            if meta and isinstance(meta, dict):
                start_ms = meta.get('start_ms')
                end_ms = meta.get('end_ms')
                if start_ms:
                    from datetime import datetime as _dt, timezone as _tz
                    try:
                        s_dt = _dt.fromtimestamp(start_ms / 1000, tz=_tz.utc)
                        range_str = f'  range: {s_dt.strftime("%Y-%m-%d")}'
                        if end_ms:
                            e_dt = _dt.fromtimestamp(end_ms / 1000, tz=_tz.utc)
                            range_str += f' → {e_dt.strftime("%Y-%m-%d")}'
                        lines.append(range_str)
                    except Exception:
                        pass

            # Rate + ETA for RUNNING jobs
            if status in ('RUNNING', 'PARTIAL') and started and elapsed_sec > 0 and ins > 0:
                rate = ins / elapsed_sec
                lines.append(f'  rate={rate:.1f} rows/sec')
                if remaining and remaining > 0:
                    eta_sec = remaining / rate
                    lines.append(f'  eta={eta_sec / 60:.0f}min ({remaining:,} remaining)')

            # Cursor-based progress % for RUNNING jobs
            if status in ('RUNNING', 'PARTIAL') and meta and isinstance(meta, dict) and cursor:
                try:
                    import json as _json_p
                    cursor_dict = cursor if isinstance(cursor, dict) else _json_p.loads(cursor)
                    start_ms = meta.get('start_ms')
                    end_ms = meta.get('end_ms')
                    since_val = cursor_dict.get('since_ms')
                    if start_ms and end_ms and since_val and end_ms > start_ms:
                        pct = (since_val - start_ms) / (end_ms - start_ms) * 100
                        pct = max(0, min(100, pct))
                        lines.append(f'  progress={pct:.1f}%')
                except Exception:
                    pass

            # Cursor: parse since_ms + detailed metrics
            if cursor:
                cursor_display = str(cursor)[:80]
                try:
                    import json as _json
                    cursor_dict = cursor if isinstance(cursor, dict) else _json.loads(cursor)
                    since_val = cursor_dict.get('since_ms')
                    if since_val:
                        from datetime import datetime as _dt2, timezone as _tz2
                        c_dt = _dt2.fromtimestamp(since_val / 1000, tz=_tz2.utc)
                        cursor_display = f'at {c_dt.strftime("%Y-%m-%d %H:%M")}'

                    # Show detailed metrics if available (from new backfill_candles)
                    conflict_cnt = cursor_dict.get('conflict_count')
                    error_cnt = cursor_dict.get('error_count')
                    latency = cursor_dict.get('last_api_latency_ms')
                    returned = cursor_dict.get('last_returned_rows')
                    stall = cursor_dict.get('last_stall_reason')
                    last_err = cursor_dict.get('last_error')

                    if conflict_cnt is not None or error_cnt is not None:
                        detail_parts = []
                        if conflict_cnt is not None:
                            detail_parts.append(f'dup={conflict_cnt}')
                        if error_cnt is not None:
                            detail_parts.append(f'err={error_cnt}')
                        if latency is not None:
                            detail_parts.append(f'latency={latency}ms')
                        if returned is not None:
                            detail_parts.append(f'last_rows={returned}')
                        lines.append(f'  cursor={cursor_display}')
                        lines.append(f'  metrics: {" ".join(detail_parts)}')
                        if stall:
                            lines.append(f'  stall: {stall}')
                        if last_err:
                            lines.append(f'  last_error: {str(last_err)[:100]}')
                    else:
                        lines.append(f'  cursor={cursor_display}')
                except Exception:
                    lines.append(f'  cursor={cursor_display}')
            if err:
                lines.append(f'  error={str(err)[:200]}')

        # Data range display
        lines.append('')
        lines.append('[데이터 범위]')
        try:
            with conn.cursor() as cur_range:
                cur_range.execute("""
                    SELECT MIN(ts), MAX(ts), COUNT(*) FROM candles WHERE tf='1m';
                """)
                c1m = cur_range.fetchone()
                cur_range.execute("""
                    SELECT MIN(ts), MAX(ts), COUNT(*) FROM market_ohlcv WHERE tf='5m';
                """)
                o5m = cur_range.fetchone()
            if c1m and c1m[2]:
                lines.append(f'  candles(1m):      {str(c1m[0])[:10]} ~ {str(c1m[1])[:10]} ({c1m[2]:,} rows)')
            else:
                lines.append('  candles(1m):      데이터 없음')
            if o5m and o5m[2]:
                lines.append(f'  market_ohlcv(5m): {str(o5m[0])[:10]} ~ {str(o5m[1])[:10]} ({o5m[2]:,} rows)')
            else:
                lines.append('  market_ohlcv(5m): 데이터 없음')
        except Exception:
            lines.append('  데이터 범위 조회 실패')

        lines.append('')
        lines.append('[잔여 작업 요약 (dryrun)]')
        try:
            with conn.cursor() as cur2:
                lines.extend(_backfill_dryrun_summary(cur2))
        except Exception:
            lines.append('  dryrun 요약 조회 실패')
        lines.append('')
        if not enabled:
            lines.append('⚠ /debug backfill_enable on → /debug backfill_start job=... write=true')
        return '\n'.join(lines)
    except Exception as e:
        return f'⚠ 백필 현황 조회 실패: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


BACKFILL_JOB_DEPS = {
    'candles_1m': {
        'source': 'bybit REST API (kline)',
        'target': 'candles (tf=1m)',
        'script': 'backfill_ohlcv.py',
        'depends_on': [],
    },
    'ohlcv_5m': {
        'source': 'candles (tf=1m) aggregate',
        'target': 'market_ohlcv',
        'script': 'aggregate_candles.py',
        'depends_on': ['candles_1m'],
    },
    'news_classify': {
        'source': 'news (raw rows)',
        'target': 'news (tier/topic_class columns)',
        'script': 'backfill_news_classification_and_reaction.py',
        'depends_on': [],
    },
    'macro_trace': {
        'source': 'news + candles',
        'target': 'macro_trace (btc_ret_30m/2h/24h)',
        'script': 'backfill_macro_trace.py',
        'depends_on': ['candles_1m'],
    },
    'price_events': {
        'source': 'candles + indicators',
        'target': 'price_events',
        'script': 'build_price_events.py',
        'depends_on': ['candles_1m'],
    },
}


def _debug_backfill_dryrun(_text=None):
    """Backfill dryrun (Item 6: per-table, per-field estimates + gated)."""
    conn = None
    try:
        conn = _db()
        lines = ['📦 백필 잔여량 추정 (dryrun)', '━━━━━━━━━━━━━━━━━━']
        with conn.cursor() as cur:
            def _append_dep_info(job_key):
                dep = BACKFILL_JOB_DEPS.get(job_key, {})
                if dep:
                    lines.append(f'  source={dep["source"]} → target={dep["target"]}')
                    lines.append(f'  script={dep["script"]}')
                    if dep.get('depends_on'):
                        lines.append(f'  depends_on: {", ".join(dep["depends_on"])}')

            # candles_1m
            try:
                cur.execute("""
                    SELECT EXTRACT(EPOCH FROM (now() - '2023-11-01'::timestamp)) / 60 AS expected,
                           (SELECT count(*) FROM candles WHERE tf = '1m') AS actual;
                """)
                r = cur.fetchone()
                expected = int(r[0]) if r[0] else 0
                actual = int(r[1]) if r[1] else 0
                remaining = max(0, expected - actual)
                pct = (actual / expected * 100) if expected > 0 else 0
                lines.append(f'candles_1m: {actual:,}/{expected:,} ({pct:.1f}%) '
                             f'remaining={remaining:,}')
                _append_dep_info('candles_1m')
            except Exception as e:
                lines.append(f'candles_1m: 조회 실패 ({e})')

            # ohlcv_5m
            try:
                cur.execute("""
                    SELECT EXTRACT(EPOCH FROM (now() - '2023-11-01'::timestamp)) / 300 AS expected,
                           (SELECT count(*) FROM market_ohlcv) AS actual;
                """)
                r = cur.fetchone()
                expected = int(r[0]) if r[0] else 0
                actual = int(r[1]) if r[1] else 0
                remaining = max(0, expected - actual)
                pct = (actual / expected * 100) if expected > 0 else 0
                lines.append(f'ohlcv_5m: {actual:,}/{expected:,} ({pct:.1f}%) '
                             f'remaining={remaining:,}')
                _append_dep_info('ohlcv_5m')
            except Exception as e:
                lines.append(f'ohlcv_5m: 조회 실패 ({e})')

            # news_classify
            try:
                cur.execute("""
                    SELECT count(*) FILTER (WHERE tier IS NULL OR tier = 'UNKNOWN') AS unclassified,
                           count(*) AS total
                    FROM news;
                """)
                r = cur.fetchone()
                unc = r[0] or 0
                total = r[1] or 0
                pct = ((total - unc) / total * 100) if total > 0 else 0
                lines.append(f'news_classify: classified={total - unc}/{total} ({pct:.1f}%) '
                             f'remaining={unc:,}')
                _append_dep_info('news_classify')
            except Exception as e:
                lines.append(f'news_classify: 조회 실패 ({e})')

            # macro_trace
            try:
                cur.execute("""
                    SELECT (SELECT count(*) FROM news) AS total_news,
                           (SELECT count(DISTINCT news_id) FROM macro_trace) AS traced;
                """)
                r = cur.fetchone()
                total = r[0] or 0
                traced = r[1] or 0
                missing = max(0, total - traced)
                pct = (traced / total * 100) if total > 0 else 0
                lines.append(f'macro_trace: traced={traced}/{total} ({pct:.1f}%) '
                             f'remaining={missing:,}')
                # Per-horizon completion
                cur.execute("""
                    SELECT count(*) FILTER (WHERE btc_ret_2h IS NULL AND btc_ret_30m IS NOT NULL) AS need_2h,
                           count(*) FILTER (WHERE btc_ret_24h IS NULL AND btc_ret_30m IS NOT NULL) AS need_24h
                    FROM macro_trace;
                """)
                hr = cur.fetchone()
                lines.append(f'  need_2h_update={hr[0] or 0} need_24h_update={hr[1] or 0}')
                _append_dep_info('macro_trace')
            except Exception as e:
                lines.append(f'macro_trace: 조회 실패 ({e})')

            # price_events
            try:
                cur.execute("SELECT count(*) FROM price_events;")
                cnt = cur.fetchone()[0] or 0
                status = f'{cnt:,}건' if cnt > 0 else 'Never run (0건)'
                lines.append(f'price_events: {status}')
                _append_dep_info('price_events')
            except Exception as e:
                lines.append(f'price_events: 조회 실패 ({e})')

        lines.append('')
        lines.append('⚠ dryrun only — 실행은 승인 후 enable (gated)')
        return '\n'.join(lines)
    except Exception as e:
        return f'⚠ 백필 잔여량 추정 실패: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _debug_news_filter_stats(_text=None):
    """24h news v2 filter stats (two-tier allow: storage vs trading)."""
    conn = None
    try:
        conn = _db()
        lines = ['📰 뉴스 필터 통계 v2 (24h)', '━━━━━━━━━━━━━━━━━━']
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, title, source, impact_score, summary, title_ko
                FROM news
                WHERE ts >= now() - interval '24 hours'
                ORDER BY ts DESC;
            """)
            rows = cur.fetchall()

        if not rows:
            return '\n'.join(lines + ['데이터 없음 (최근 24시간)'])

        import news_classifier_config as ncc
        total = len(rows)
        tier_counts = {}
        topic_counts = {}
        deny_counts = {}
        source_counts = {}
        allow_storage_count = 0
        allow_trading_count = 0

        for r in rows:
            nid, title, source, impact, summary, title_ko = r
            result = ncc.preview_classify(
                title or '', source or '', impact or 0,
                summary=summary or '', title_ko=title_ko or '')
            tier = result.get('tier_preview', 'UNKNOWN')
            topic = result.get('topic_class_preview', 'unclassified')
            deny_reasons = result.get('deny_reasons', [])

            tier_counts[tier] = tier_counts.get(tier, 0) + 1
            topic_counts[topic] = topic_counts.get(topic, 0) + 1
            src = (source or 'unknown').lower()
            source_counts[src] = source_counts.get(src, 0) + 1
            if result.get('allow_for_storage', False):
                allow_storage_count += 1
            if result.get('allow_for_trading', False):
                allow_trading_count += 1
            for dr in deny_reasons:
                deny_counts[dr] = deny_counts.get(dr, 0) + 1

        deny_count = total - allow_storage_count
        storage_pct = allow_storage_count / total * 100 if total else 0
        trading_pct = allow_trading_count / total * 100 if total else 0
        lines.append(f'total={total}')
        lines.append(f'  allow_storage={allow_storage_count} ({storage_pct:.1f}%)')
        lines.append(f'  allow_trading={allow_trading_count} ({trading_pct:.1f}%)')
        lines.append(f'  deny={deny_count}')
        lines.append('')

        # Tier distribution
        lines.append('[tier distribution]')
        for t in sorted(tier_counts, key=tier_counts.get, reverse=True):
            pct = tier_counts[t] / total * 100
            lines.append(f'  {t}: {tier_counts[t]} ({pct:.1f}%)')

        # Topic distribution
        lines.append('\n[topic distribution]')
        for t in sorted(topic_counts, key=topic_counts.get, reverse=True):
            pct = topic_counts[t] / total * 100
            lines.append(f'  {t}: {topic_counts[t]} ({pct:.1f}%)')

        # Deny reason distribution
        lines.append('\n[deny_reason distribution]')
        if deny_counts:
            for dr in sorted(deny_counts, key=deny_counts.get, reverse=True):
                lines.append(f'  {dr}: {deny_counts[dr]}')
        else:
            lines.append('  none')

        # Source distribution (top 10)
        lines.append('\n[source distribution (top 10)]')
        for s in sorted(source_counts, key=source_counts.get, reverse=True)[:10]:
            lines.append(f'  {s}: {source_counts[s]}')

        return '\n'.join(lines)
    except Exception as e:
        return f'⚠ 뉴스 필터 통계 조회 실패: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _debug_state(_text=None):
    """System state (Item 7: feature flags, directives, backfill summary)."""
    lines = ['🔧 시스템 상태 변수', '━━━━━━━━━━━━━━━━━━']
    conn = None

    try:
        conn = _db()
        with conn.cursor() as cur:
            # trade_switch
            try:
                cur.execute("""
                    SELECT enabled, updated_at
                    FROM trade_switch
                    ORDER BY id DESC LIMIT 1;
                """)
                row = cur.fetchone()
                if row:
                    lines.append(f'trade_switch: enabled={row[0]} '
                                 f'updated_at={str(row[1])[:19] if row[1] else "-"}')
                else:
                    lines.append('trade_switch: 레코드 없음')
            except Exception as e:
                lines.append(f'trade_switch: 조회 실패 ({e})')

            # ── openclaw_policies + directives ──
            try:
                cur.execute("""
                    SELECT key, value, updated_at, description
                    FROM openclaw_policies
                    ORDER BY key;
                """)
                rows = cur.fetchall()
                if rows:
                    lines.append('')
                    lines.append('[openclaw_policies / directives]')
                    for k, v, updated_at, desc in rows:
                        v_str = str(v)[:60] if v else '-'
                        updated = str(updated_at)[:19] if updated_at else '-'
                        lines.append(f'  {k}: {v_str}')
                        lines.append(f'    set_at={updated} desc={str(desc)[:40] if desc else "-"}')
                else:
                    lines.append('openclaw_policies: 데이터 없음')
            except Exception:
                lines.append('openclaw_policies: 테이블 미존재')

            # ── Backfill summary ──
            lines.append('')
            lines.append('[backfill_summary]')
            try:
                cur.execute("""
                    SELECT DISTINCT ON (job_name) job_name, status, finished_at
                    FROM backfill_job_runs
                    ORDER BY job_name, started_at DESC;
                """)
                bf_rows = cur.fetchall()
                if bf_rows:
                    for job, status, finished in bf_rows:
                        fin = str(finished)[:19] if finished else '-'
                        lines.append(f'  {job}: {status} (finished={fin})')
                else:
                    lines.append('  no backfill jobs recorded')
            except Exception:
                lines.append('  backfill_job_runs: 조회 실패')
    except Exception as e:
        lines.append(f'DB 연결 실패: {e}')
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    # ── Exposure cap / symbol whitelist ──
    lines.append('')
    lines.append('[exposure_cap]')
    _cap_source = 'dynamic'
    _cap = 900
    try:
        from trading_config import ALLOWED_SYMBOLS
        import safety_manager
        eq = safety_manager.get_equity_limits()
        _cap = eq['operating_cap']
        lines.append(f'  ALLOWED_SYMBOLS: {", ".join(sorted(ALLOWED_SYMBOLS))}')
        lines.append(f'  operating_cap: {eq["operating_cap"]} (equity={eq["equity"]}, src={eq["source"]})')
    except Exception:
        _cap_source = 'FALLBACK'
        lines.append('  (equity_limits 조회 실패)')
    conn2 = None
    try:
        conn2 = _db()
        with conn2.cursor() as cur2:
            cur2.execute("""
                SELECT side, total_qty, capital_used_usdt
                FROM position_state WHERE symbol = %s;
            """, ('BTC/USDT:USDT',))
            ps = cur2.fetchone()
            if ps and ps[0]:
                side_str = ps[0]
                qty_val = float(ps[1] or 0)
                cap_used = float(ps[2] or 0)
                remaining = max(0, _cap - cap_used)
                lines.append(f'  position: {side_str} qty={qty_val} capital_used={cap_used:.1f}')
                lines.append(f'  remaining_cap: {remaining:.1f} USDT [{_cap_source}]')
            else:
                lines.append('  position: FLAT')
                lines.append(f'  remaining_cap: {_cap} USDT [{_cap_source}]')
            # Cap block/shrink counts
            cur2.execute("""
                SELECT event, count(*)
                FROM live_executor_log
                WHERE event IN ('CAP_BLOCKED', 'CAP_SHRINK')
                  AND ts >= now() - interval '24 hours'
                GROUP BY event;
            """)
            cap_rows = cur2.fetchall()
            if cap_rows:
                for ev, cnt in cap_rows:
                    lines.append(f'  {ev}_24h: {cnt}건')
            else:
                lines.append('  cap_events_24h: 0건')
    except Exception as e:
        lines.append(f'  exposure 조회 실패: {e}')
    finally:
        if conn2:
            try:
                conn2.close()
            except Exception:
                pass

    # test_mode (no DB needed)
    try:
        import test_utils
        test_mode = test_utils.load_test_mode()
        is_active = test_utils.is_test_active()
        lines.append(f'test_mode: loaded={test_mode is not None} active={is_active}')
    except Exception as e:
        lines.append(f'test_mode: 로드 실패 ({e})')

    # LIVE_TRADING env var
    live = os.getenv('LIVE_TRADING', 'unset')
    lines.append(f'LIVE_TRADING: {live}')

    # ── Feature flags / approval pending ──
    lines.append('')
    lines.append('[feature_flags]')
    try:
        from backfill_utils import is_backfill_enabled
        bf_enabled = is_backfill_enabled()
        lines.append(f'  backfill: ENABLED={bf_enabled} '
                     f'(/debug backfill_enable on|off)')
    except Exception:
        lines.append(f'  backfill: check failed')
    try:
        import news_classifier_config as ncc
        _applied = not ncc.APPROVAL_REQUIRED
        lines.append(f'  news_classifier: APPLIED={_applied} '
                     f'(APPROVAL_REQUIRED={ncc.APPROVAL_REQUIRED})')
    except Exception:
        lines.append(f'  news_classifier: import failed')

    # state_mode from telegram_cmd_poller
    lines.append('')
    try:
        import telegram_cmd_poller as _tcp
        ds = _tcp._last_debug_state
        lines.append(f'state_mode: {ds.get("state_mode", "chat")}')
        lines.append(f'last_detected_intent: {ds.get("detected_intent") or "null"}')
        lines.append(f'last_decision_ts: {ds.get("decision_ts") or "null"}')
    except Exception:
        lines.append('state_mode: unknown')

    # ── LLM routing policy ──
    lines.append('')
    lines.append('[LLM 라우팅 정책]')
    lines.append('  메인: GPT (gpt-4o-mini) — 모든 자연어 대화')
    lines.append('  보조: Claude (sonnet) — "클로드/claude/긴급" 키워드 시만')
    lines.append('  Fallback: keyword_fallback → local_query_executor')

    # Analysis-only reminder
    lines.append('')
    lines.append('⚠ 적용 금지 (analysis only)')

    return '\n'.join(lines)


# ── Backfill control handlers ────────────────────────────

# Available backfill jobs
_BACKFILL_JOBS = {
    'candles_1m':    'backfill_candles.py — 1m 캔들 (Bybit → candles)',
    'ohlcv_5m':      'backfill_ohlcv.py — 5m OHLCV (Bybit → market_ohlcv)',
    'aggregate_5m':  'aggregate_candles.py — 1m→5m/15m/1h 집계',
    'price_events':  'build_price_events.py — 가격 이벤트 탐지',
    'macro_trace':   'backfill_macro_trace.py — 뉴스 BTC 반응',
    'news_classify': 'backfill_news_classification_and_reaction.py — 뉴스 분류',
    'news_reaction': 'backfill_news_classification_and_reaction.py — 시장 반응',
    'link_events':   'link_event_to_news.py — 이벤트↔뉴스 연결',
    'news_path':     'backfill_news_path.py — 뉴스 24h 경로 분석',
    'prune_1m':      'prune_candles_1m.py — 오래된 1m 캔들 정리 (>180d)',
    'archive':       'backfill_archive.py — Binance 아카이브 벌크 적재 (cold store)',
}

# Aliases for common alternative job names
_BACKFILL_JOB_ALIASES = {
    'prune_candles_1m': 'prune_1m',
    'backfill_candles_1m': 'candles_1m',
    'backfill_ohlcv_5m': 'ohlcv_5m',
    'backfill_news_path': 'news_path',
    'aggregate_candles': 'aggregate_5m',
}


def _parse_backfill_args(text):
    """Parse backfill args: job=X from=Y to=Z tf=W write=true"""
    args = {}
    if not text:
        return args
    # Strip the command prefix
    t = text.strip()
    for part in t.split():
        if '=' in part:
            k, v = part.split('=', 1)
            args[k.lower()] = v
    return args


def _debug_backfill_enable(_text=None):
    """Enable or disable backfill execution."""
    from backfill_utils import is_backfill_enabled, set_backfill_enabled

    t = (_text or '').strip().lower()
    # Parse on/off from text
    if 'on' in t or 'true' in t or 'enable' in t:
        set_backfill_enabled(True)
        return ('✅ 백필 실행 ENABLED\n'
                'backfill_start write=true 실행 가능\n'
                '비활성화: /debug backfill_enable off')
    elif 'off' in t or 'false' in t or 'disable' in t:
        set_backfill_enabled(False)
        return ('🔒 백필 실행 DISABLED\n'
                'backfill_start write=true 차단됨')
    else:
        enabled = is_backfill_enabled()
        state = 'ON (실행 가능)' if enabled else 'OFF (차단 중)'
        return (f'🔧 백필 실행 게이트: {state}\n\n'
                f'/debug backfill_enable on — 활성화\n'
                f'/debug backfill_enable off — 비활성화')


def _debug_backfill_start(_text=None):
    """Start a backfill job via backfill_runner.py subprocess."""
    from backfill_utils import get_running_pid, check_trade_switch_off, is_backfill_enabled

    args = _parse_backfill_args(_text)
    job = args.get('job', '')

    # Resolve aliases
    if job in _BACKFILL_JOB_ALIASES:
        job = _BACKFILL_JOB_ALIASES[job]

    # No job specified: show usage
    if not job:
        enabled = is_backfill_enabled()
        gate_str = '✅ ENABLED' if enabled else '🔒 DISABLED (/debug backfill_enable on 필요)'
        lines = ['📦 백필 시작 (backfill_start)', '━━━━━━━━━━━━━━━━━━']
        lines.append(f'gate: {gate_str}')
        lines.append('')
        lines.append('Usage: /debug backfill_start job=<name> [from=YYYY-MM-DD] [to=YYYY-MM-DD] [write=true]')
        lines.append('')
        lines.append('[사용 가능한 job]')
        for k, desc in _BACKFILL_JOBS.items():
            lines.append(f'  {k} — {desc}')
        lines.append('')
        lines.append('write=true 없으면 dryrun (미리보기만)')
        return '\n'.join(lines)

    # Unknown job
    if job not in _BACKFILL_JOBS:
        return f'⚠ 알 수 없는 job: {job}\n사용 가능: {", ".join(_BACKFILL_JOBS.keys())}'

    from_date = args.get('from', '')
    to_date = args.get('to', '')
    tf = args.get('tf', '')
    write = args.get('write', 'false').lower() == 'true'

    # Dryrun preview (default) — no gate checks needed for preview
    if not write:
        enabled = is_backfill_enabled()
        gate_str = '✅ ENABLED' if enabled else '🔒 DISABLED'
        lines = ['📦 백필 Dryrun (미리보기)', '━━━━━━━━━━━━━━━━━━']
        lines.append(f'gate: {gate_str}')
        lines.append(f'job: {job}')
        lines.append(f'script: {_BACKFILL_JOBS[job]}')
        if from_date:
            lines.append(f'from: {from_date}')
        else:
            lines.append('from: --resume (마지막 커서부터)')
        if to_date:
            lines.append(f'to: {to_date}')
        else:
            lines.append('to: now')
        if tf:
            lines.append(f'tf: {tf}')
        lines.append('')
        if not enabled:
            lines.append('⚠ 실행 전 /debug backfill_enable on 필요')
        lines.append('실행하려면: /debug backfill_start job={} {}{}{}write=true'.format(
            job,
            f'from={from_date} ' if from_date else '',
            f'to={to_date} ' if to_date else '',
            f'tf={tf} ' if tf else '',
        ))
        return '\n'.join(lines)

    # === write=true: all gates checked BEFORE launching ===

    # Gate 1: backfill must be enabled
    if not is_backfill_enabled():
        return ('⚠ DENIED: 백필 실행이 비활성화 상태입니다.\n\n'
                '활성화: /debug backfill_enable on\n'
                '그 다음: /debug backfill_start job={} {}{}{}write=true'.format(
                    job,
                    f'from={from_date} ' if from_date else '',
                    f'to={to_date} ' if to_date else '',
                    f'tf={tf} ' if tf else '',
                ))

    # Gate 2: trade_switch must be OFF
    if not check_trade_switch_off():
        return '⚠ DENIED: trade_switch가 ON 상태입니다.\n백필은 trade_switch OFF일 때만 실행 가능합니다.'

    # Gate 3: concurrency check
    running_pid = get_running_pid()
    if running_pid:
        return f'⚠ DENIED: 다른 백필이 실행 중 (PID={running_pid})\n먼저 /debug backfill_stop 으로 종료하세요.'

    # Build runner command
    cmd = ['python3', f'{APP_DIR}/backfill_runner.py', job]
    if from_date:
        cmd.extend(['--from', from_date])
    if to_date and to_date.lower() != 'now':
        cmd.extend(['--to', to_date])
    if tf:
        cmd.extend(['--tf', tf])

    # Launch background process
    log_path = f'/tmp/backfill_{job}.log'
    log_file = open(log_path, 'w')
    proc = subprocess.Popen(
        cmd,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        cwd=APP_DIR,
        start_new_session=True,
    )
    # Close parent's copy of the fd — child process keeps its own copy
    log_file.close()

    # Wait briefly to check if runner dies immediately
    _time.sleep(1.5)
    try:
        os.kill(proc.pid, 0)
    except ProcessLookupError:
        # Runner already dead — read exit status
        from backfill_utils import read_exit_status
        exit_info = read_exit_status()
        if exit_info:
            return (f'⚠ FAILED: runner 즉시 종료됨\n'
                    f'  status: {exit_info.get("status", "?")}\n'
                    f'  reason: {exit_info.get("reason", "?")}\n'
                    f'  ts: {exit_info.get("ts", "?")}')
        # Fallback: read log
        try:
            with open(log_path) as f:
                log_tail = f.read().strip()[-500:]
        except Exception:
            log_tail = ''
        return f'⚠ FAILED: runner 즉시 종료됨 (PID={proc.pid})\nlog: {log_tail or "empty"}'
    except PermissionError:
        pass  # process exists

    lines = ['📦 백필 STARTED', '━━━━━━━━━━━━━━━━━━']
    lines.append(f'job: {job}')
    lines.append(f'runner PID: {proc.pid}')
    lines.append(f'log: {log_path}')
    lines.append(f'cmd: {" ".join(cmd)}')
    lines.append('')
    lines.append('제어 명령:')
    lines.append('  /debug backfill_status — 진행 상황')
    lines.append('  /debug backfill_pause — 일시정지')
    lines.append('  /debug backfill_resume — 재개')
    lines.append('  /debug backfill_stop — 안전 종료')
    return '\n'.join(lines)


def _debug_backfill_pause(_text=None):
    """Pause running backfill."""
    from backfill_utils import get_running_pid, signal_pause, PAUSE_FILE

    pid = get_running_pid()
    if not pid:
        return '⚠ 실행 중인 백필이 없습니다.'

    if os.path.exists(PAUSE_FILE):
        return '⚠ 이미 일시정지 상태입니다. /debug backfill_resume 으로 재개하세요.'

    signal_pause()
    return f'⏸ PAUSE 신호 전송 (PID={pid})\n현재 배치 완료 후 일시정지됩니다.'


def _debug_backfill_resume(_text=None):
    """Resume paused backfill."""
    from backfill_utils import get_running_pid, signal_resume, PAUSE_FILE

    pid = get_running_pid()
    if not pid:
        return '⚠ 실행 중인 백필이 없습니다.'

    if not os.path.exists(PAUSE_FILE):
        return '⚠ 일시정지 상태가 아닙니다.'

    signal_resume()
    return f'▶ RESUME 신호 전송 (PID={pid})\n백필이 재개됩니다.'


def _debug_backfill_stop(_text=None):
    """Stop running backfill gracefully."""
    from backfill_utils import get_running_pid, signal_stop

    pid = get_running_pid()
    if not pid:
        return '⚠ 실행 중인 백필이 없습니다.'

    signal_stop()
    return (f'⏹ STOP 신호 전송 (PID={pid})\n'
            f'현재 배치 커밋 후 안전 종료됩니다.\n'
            f'/debug backfill_status 로 종료 확인하세요.')


def _debug_backfill_log(_text=None):
    """Show last lines of backfill child log."""
    from backfill_utils import get_running_pid

    # Parse optional args: job=candles_1m lines=30
    args = _parse_backfill_args(_text or '')
    job_key = args.get('job', '')
    n_lines = 30
    try:
        n_lines = int(args.get('lines', '30'))
    except ValueError:
        pass
    n_lines = min(n_lines, 100)

    # Find log file
    if job_key:
        log_path = f'/tmp/backfill_{job_key}_child.log'
        runner_log = f'/tmp/backfill_{job_key}.log'
    else:
        # Try to find most recent log
        import glob as _glob
        child_logs = sorted(_glob.glob('/tmp/backfill_*_child.log'),
                            key=lambda f: os.path.getmtime(f) if os.path.exists(f) else 0,
                            reverse=True)
        if child_logs:
            log_path = child_logs[0]
            # Extract job_key from filename
            base = os.path.basename(log_path)
            job_key = base.replace('backfill_', '').replace('_child.log', '')
            runner_log = f'/tmp/backfill_{job_key}.log'
        else:
            return '로그 파일 없음. job= 지정 또는 백필 실행 후 다시 시도하세요.'

    lines = []
    pid = get_running_pid()
    lines.append(f'📋 백필 로그 (job={job_key}, runner={"PID=" + str(pid) if pid else "미실행"})')
    lines.append('━━━━━━━━━━━━━━━━━━')

    # Child log (main output)
    if os.path.exists(log_path):
        try:
            with open(log_path) as f:
                all_lines = f.readlines()
            total = len(all_lines)
            tail = all_lines[-n_lines:]
            lines.append(f'\n[Child log] {log_path} ({total} lines, last {len(tail)}):')
            for l in tail:
                lines.append(l.rstrip())
        except Exception as e:
            lines.append(f'Child log 읽기 실패: {e}')
    else:
        lines.append(f'Child log 없음: {log_path}')

    # Runner log (brief)
    if os.path.exists(runner_log):
        try:
            with open(runner_log) as f:
                r_lines = f.readlines()
            r_tail = r_lines[-5:]
            lines.append(f'\n[Runner log] {runner_log} (last {len(r_tail)}):')
            for l in r_tail:
                lines.append(l.rstrip())
        except Exception:
            pass

    return '\n'.join(lines)


def _debug_storage(_text=None):
    """Show DB table sizes and row counts."""
    conn = None
    try:
        conn = _db()
        lines = ['💾 DB 스토리지 현황', '━━━━━━━━━━━━━━━━━━']

        with conn.cursor() as cur:
            # Total DB size
            cur.execute("SELECT pg_size_pretty(pg_database_size(current_database()));")
            db_size = cur.fetchone()[0]
            lines.append(f'총 DB 크기: {db_size}')
            lines.append('')

            # Top tables by size
            cur.execute("""
                SELECT relname,
                       pg_size_pretty(pg_total_relation_size(relid)) AS size,
                       pg_total_relation_size(relid) AS size_bytes,
                       n_live_tup AS row_count
                FROM pg_stat_user_tables
                ORDER BY pg_total_relation_size(relid) DESC
                LIMIT 15;
            """)
            table_rows = cur.fetchall()

            if table_rows:
                lines.append(f'{"테이블":<28} {"크기":>10} {"행수":>12}')
                for name, size, _, rows in table_rows:
                    lines.append(f'  {name:<26} {size:>10} {rows:>10,}')

            # Data ranges
            lines.append('')
            lines.append('[데이터 범위]')

            cur.execute("SELECT MIN(ts), MAX(ts) FROM candles WHERE tf='1m';")
            c1m = cur.fetchone()
            if c1m and c1m[0]:
                lines.append(f'  candles(1m): {str(c1m[0])[:10]} ~ {str(c1m[1])[:10]}')
            else:
                lines.append('  candles(1m): 데이터 없음')

            cur.execute("SELECT MIN(ts), MAX(ts) FROM market_ohlcv WHERE tf='5m';")
            o5m = cur.fetchone()
            if o5m and o5m[0]:
                lines.append(f'  market_ohlcv(5m): {str(o5m[0])[:10]} ~ {str(o5m[1])[:10]}')
            else:
                lines.append('  market_ohlcv(5m): 데이터 없음')

            # Retention policy section
            lines.append('')
            lines.append('[보존 정책]')
            lines.append('  candles(1m): 180일 (prune_candles_1m.py)')
            lines.append('  pm_decision_log: 90일 (cleanup_old_data)')
            lines.append('  score_history: 60일 / event_trigger_log: 30일 / claude_call_log: 60일')

            # Count prunable 1m candles
            try:
                from datetime import timedelta
                cur.execute("""
                    SELECT COUNT(*) FROM candles
                    WHERE tf='1m' AND ts < now() - interval '180 days';
                """)
                prune_count = cur.fetchone()[0]
                lines.append(f'  -> candles(1m) 프루닝 대상: {prune_count:,}행')
            except Exception:
                pass

        return '\n'.join(lines)
    except Exception as e:
        return f'⚠ 스토리지 조회 실패: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _debug_news_path_sample(_text=None):
    """Show recent news price path analysis samples."""
    import re as _re
    conn = None
    try:
        # Parse --n=N from text
        n_limit = 10
        if _text:
            m = _re.search(r'--n=(\d+)', _text)
            if m:
                n_limit = min(int(m.group(1)), 50)

        conn = _db()
        lines = ['📊 뉴스 경로 분석 (최근 {}건)'.format(n_limit), '━━━━━━━━━━━━━━━━━━']

        with conn.cursor() as cur:
            cur.execute("""
                SELECT npp.news_id,
                       COALESCE(n.title_ko, LEFT(n.title, 40)) AS display_title,
                       COALESCE(n.topic_class, n.tier, '-') AS topic,
                       npp.ts_news, npp.path_class,
                       npp.initial_move_dir, npp.follow_through_dir,
                       npp.recovered_flag,
                       npp.max_drawdown_24h, npp.max_runup_24h,
                       npp.end_ret_24h, npp.recovery_minutes,
                       npp.path_shape, npp.end_state_24h
                FROM news_price_path npp
                JOIN news n ON n.id = npp.news_id
                ORDER BY npp.ts_news DESC
                LIMIT %s;
            """, (n_limit,))
            rows = cur.fetchall()

        if not rows:
            lines.append('데이터 없음')
            return '\n'.join(lines)

        for i, row in enumerate(rows, 1):
            (nid, title, topic, ts, pc, imd, ftd, rec,
             dd, ru, r24, rec_min, ps, es) = row
            title_short = (title or '?')[:40]
            ts_str = str(ts)[:16] if ts else '?'
            rec_icon = '✅' if rec else '❌'
            pc_str = pc or ps or '?'
            lines.append(
                f'{i}) [{topic or "?"}] {title_short} ({ts_str})')
            lines.append(
                f'   path_class={pc_str} | initial={imd or "?"} '
                f'| follow={ftd or "?"} | recovered={rec_icon}')
            dd_s = f'{dd:+.1f}%' if dd is not None else '?'
            ru_s = f'{ru:+.1f}%' if ru is not None else '?'
            r24_s = f'{r24:+.1f}%' if r24 is not None else '?'
            rec_s = f'{rec_min}min' if rec_min is not None else '-'
            lines.append(
                f'   DD={dd_s} | RU={ru_s} | ret_24h={r24_s} | recovery={rec_s}')

        # Coverage stats: raw + eligible
        with conn.cursor() as cur2:
            cur2.execute("""
                SELECT
                    count(*) AS total_path,
                    count(*) FILTER (WHERE path_class IS NOT NULL) AS classified
                FROM news_price_path;
            """)
            tot_row = cur2.fetchone()
            total_c, classified_c = (tot_row or (0, 0))
            pending = total_c - classified_c

            cur2.execute("SELECT count(*) FROM news WHERE ts < now() - interval '24 hours';")
            total_news = cur2.fetchone()[0] or 0
            raw_pct = (total_c / total_news * 100) if total_news > 0 else 0

            # Eligible: news within candle coverage + 24h lookahead
            cur2.execute("""
                SELECT count(*) FROM news n
                WHERE n.ts >= (SELECT COALESCE(MIN(ts), now()) FROM candles WHERE tf='1m')
                  AND n.ts + interval '24 hours' <= (SELECT COALESCE(MAX(ts), now()) FROM candles WHERE tf='1m')
                  AND n.ts < now() - interval '24 hours';
            """)
            eligible = cur2.fetchone()[0] or 0

            # Eligible traced
            cur2.execute("""
                SELECT count(*) FROM news_price_path npp
                JOIN news n ON n.id = npp.news_id
                WHERE n.ts >= (SELECT COALESCE(MIN(ts), now()) FROM candles WHERE tf='1m')
                  AND n.ts + interval '24 hours' <= (SELECT COALESCE(MAX(ts), now()) FROM candles WHERE tf='1m');
            """)
            eligible_traced = cur2.fetchone()[0] or 0
            elig_pct = (eligible_traced / eligible * 100) if eligible > 0 else 0

            lines.append('')
            lines.append(f'[커버리지]')
            lines.append(f'  raw: {total_c}/{total_news} ({raw_pct:.1f}%) | '
                         f'eligible: {eligible_traced}/{eligible} ({elig_pct:.1f}%)')
            lines.append(f'  분류완료={classified_c} | 미분류={pending}')

        return '\n'.join(lines)
    except Exception as e:
        return f'⚠ 뉴스 경로 샘플 조회 실패: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _debug_news_path_stats(_text=None):
    """Show news price path 7-class distribution statistics."""
    conn = None
    try:
        conn = _db()
        lines = ['📊 뉴스 경로 통계', '━━━━━━━━━━━━━━━━━━']

        with conn.cursor() as cur:
            # Overall distribution
            cur.execute("""
                SELECT path_class,
                       count(*) AS cnt,
                       round(avg(end_ret_24h)::numeric, 2) AS avg_ret,
                       round(avg(max_drawdown_24h)::numeric, 2) AS avg_dd
                FROM news_price_path
                WHERE path_class IS NOT NULL
                GROUP BY path_class
                ORDER BY cnt DESC;
            """)
            rows = cur.fetchall()

        if not rows:
            lines.append('path_class 데이터 없음 (--recompute 실행 필요)')
            return '\n'.join(lines)

        lines.append(f'{"path_class":<16} {"건수":>5} {"avg_ret_24h":>12} {"avg_dd":>10}')
        for pc, cnt, avg_ret, avg_dd in rows:
            r_s = f'{avg_ret:+.2f}%' if avg_ret is not None else '?'
            d_s = f'{avg_dd:+.2f}%' if avg_dd is not None else '?'
            lines.append(f'  {pc or "?":<14} {cnt:>5} {r_s:>12} {d_s:>10}')

        # Category breakdown
        with conn.cursor() as cur:
            cur.execute("""
                SELECT n.topic_class, npp.path_class, count(*) AS cnt
                FROM news_price_path npp
                JOIN news n ON n.id = npp.news_id
                WHERE npp.path_class IS NOT NULL AND n.topic_class IS NOT NULL
                GROUP BY n.topic_class, npp.path_class
                ORDER BY n.topic_class, cnt DESC;
            """)
            cat_rows = cur.fetchall()

        if cat_rows:
            lines.append('')
            lines.append('[카테고리별 경로 분포]')
            # Group by topic_class
            current_topic = None
            for topic, pc, cnt in cat_rows:
                if topic != current_topic:
                    current_topic = topic
                    lines.append(f'  {topic}:')
                lines.append(f'    {pc}: {cnt}건')

        return '\n'.join(lines)
    except Exception as e:
        return f'⚠ 뉴스 경로 통계 조회 실패: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _debug_news_gap_diagnosis(_text=None):
    """News monthly gap diagnosis: counts, classification rate, reaction coverage."""
    conn = None
    try:
        conn = _db()
        lines = ['📰 뉴스 월별 갭 진단', '━━━━━━━━━━━━━━━━━━']

        with conn.cursor() as cur:
            # Monthly news counts + classification status
            cur.execute("""
                SELECT to_char(ts, 'YYYY-MM') AS month,
                       count(*) AS total,
                       count(*) FILTER (WHERE tier IS NULL OR tier = 'UNKNOWN') AS unclassified,
                       count(*) FILTER (WHERE tier = 'TIERX') AS tierx,
                       count(*) FILTER (WHERE impact_score IS NULL OR impact_score = 0) AS no_impact
                FROM news
                GROUP BY month
                ORDER BY month;
            """)
            news_rows = cur.fetchall()

            # Monthly reaction coverage
            cur.execute("""
                SELECT to_char(n.ts, 'YYYY-MM') AS month,
                       count(DISTINCT nmr.news_id) AS reaction_count
                FROM news n
                LEFT JOIN news_market_reaction nmr ON nmr.news_id = n.id
                WHERE nmr.id IS NOT NULL
                GROUP BY month
                ORDER BY month;
            """)
            reaction_map = {}
            for r in cur.fetchall():
                reaction_map[r[0]] = r[1]

            # Monthly event_news_link coverage
            link_map = {}
            try:
                cur.execute("""
                    SELECT to_char(n.ts, 'YYYY-MM') AS month,
                           count(DISTINCT enl.news_id) AS linked_count
                    FROM news n
                    JOIN event_news_link enl ON enl.news_id = n.id
                    GROUP BY month
                    ORDER BY month;
                """)
                for r in cur.fetchall():
                    link_map[r[0]] = r[1]
            except Exception:
                pass  # table may not exist

        if not news_rows:
            return '\n'.join(lines + ['뉴스 데이터 없음'])

        # Header
        lines.append(f'{"월":>8} {"전체":>6} {"미분류":>6} {"TIERX":>6} '
                     f'{"no_imp":>6} {"분류%":>6} {"반응":>6} {"링크":>6} {"FLAG":>6}')
        lines.append('-' * 65)

        flagged_months = []
        for month, total, unclassified, tierx, no_impact in news_rows:
            classified_pct = ((total - unclassified) / total * 100) if total > 0 else 0
            reaction = reaction_map.get(month, 0)
            linked = link_map.get(month, 0)

            # Flag conditions
            flags = []
            if total < 100:
                flags.append('LOW')
            if classified_pct < 50:
                flags.append('UNCLASS')

            flag_str = ','.join(flags) if flags else '-'
            if flags:
                flagged_months.append(month)

            lines.append(
                f'{month:>8} {total:>6} {unclassified:>6} {tierx:>6} '
                f'{no_impact:>6} {classified_pct:>5.0f}% {reaction:>6} {linked:>6} {flag_str:>6}'
            )

        # Summary
        lines.append('')
        if flagged_months:
            lines.append(f'⚠ 주의 월: {", ".join(flagged_months)}')
            lines.append('  LOW = 뉴스 <100건 | UNCLASS = 분류 <50%')
        else:
            lines.append('모든 월 정상')

        return '\n'.join(lines)
    except Exception as e:
        return f'⚠ 뉴스 갭 진단 실패: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _debug_system_stability(_text=None):
    """System stability composite scores — data integrity, news, backfill, execution."""
    conn = None
    try:
        conn = _db()
        from data_integrity import compute_stability_scores, format_stability_report
        from data_integrity import check_pre_live_gate, format_pre_live_gate_report
        scores = compute_stability_scores(conn)
        lines = [format_stability_report(scores)]

        # Pre-live gate status
        status, blocks, warns = check_pre_live_gate(conn)
        lines.append('')
        lines.append(format_pre_live_gate_report(status, blocks, warns))
        return '\n'.join(lines)
    except Exception as e:
        return f'⚠ system_stability 조회 실패: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _debug_once_lock_status(_text=None):
    """once_lock 상태 + TTL 잔여시간 표시."""
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT symbol, opened_at, expires_at,
                       CASE WHEN expires_at IS NOT NULL
                            THEN EXTRACT(EPOCH FROM (expires_at - now())) / 60
                            ELSE NULL END AS ttl_min
                FROM live_order_once_lock
                ORDER BY opened_at DESC;
            """)
            rows = cur.fetchall()
        if not rows:
            return '🔓 once_lock: 없음 (empty)'
        lines = ['🔒 once_lock 상태', '━━━━━━━━━━━━━━━━━━']
        for symbol, opened, expires, ttl_min in rows:
            ttl_str = f'{ttl_min:.1f}min left' if ttl_min is not None else 'no TTL'
            expired = ttl_min is not None and ttl_min <= 0
            status = '(EXPIRED)' if expired else ''
            lines.append(f'  {symbol}: opened={opened} | {ttl_str} {status}')
        lines.append(f'\ntotal: {len(rows)}')
        return '\n'.join(lines)
    except Exception as e:
        return f'⚠ once_lock_status 조회 실패: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _debug_once_lock_clear(_text=None):
    """수동 once_lock 전체 삭제."""
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM live_order_once_lock;")
            deleted = cur.rowcount
        return f'🔓 once_lock 수동 삭제 완료: {deleted}건'
    except Exception as e:
        return f'⚠ once_lock_clear 실패: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _debug_backfill_ack(_text=None):
    """/debug backfill_ack <job_id> — FAILED job acknowledged 처리."""
    job_id = None
    if _text:
        import re as _re
        m = _re.search(r'(\d+)', _text)
        if m:
            job_id = int(m.group(1))
    if not job_id:
        return '⚠ 사용법: /debug backfill_ack <job_id>'
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE backfill_job_runs
                SET acked_at = now(), acked_by = 'operator'
                WHERE id = %s AND status = 'FAILED' AND acked_at IS NULL
                RETURNING id, job_name, started_at;
            """, (job_id,))
            row = cur.fetchone()
        if not row:
            return f'⚠ job_id={job_id}: FAILED+unacked 상태가 아니거나 존재하지 않음'
        return f'✅ backfill job acked: id={row[0]} name={row[1]} started={row[2]}'
    except Exception as e:
        return f'⚠ backfill_ack 실패: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _mode_performance(_text=None):
    """Strategy v2 mode performance report: per-mode (A/B/C) win rate, avg PnL, etc."""
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            # Check if table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = 'strategy_decision_log'
                )
            """)
            if not cur.fetchone()[0]:
                return '📊 Strategy v2 모드 성능\n\n테이블 없음 — strategy v2 미활성화 상태'

            # Decision counts per mode
            cur.execute("""
                SELECT mode,
                       COUNT(*) AS total_decisions,
                       COUNT(*) FILTER (WHERE action = 'ENTER') AS enters,
                       COUNT(*) FILTER (WHERE action = 'ADD') AS adds,
                       COUNT(*) FILTER (WHERE action = 'EXIT') AS exits,
                       COUNT(*) FILTER (WHERE action = 'HOLD') AS holds,
                       COUNT(*) FILTER (WHERE dedupe_hit = true) AS deduped,
                       COUNT(*) FILTER (WHERE chase_entry = true) AS chased
                FROM strategy_decision_log
                WHERE ts >= now() - interval '24 hours'
                GROUP BY mode
                ORDER BY mode
            """)
            rows = cur.fetchall()

            if not rows:
                return '📊 Strategy v2 모드 성능\n\n최근 24시간 결정 없음'

            lines = ['📊 Strategy v2 모드 성능 (24h)\n']
            for r in rows:
                mode, total, enters, adds, exits, holds, deduped, chased = r
                lines.append(f'MODE_{mode}: {total}건')
                lines.append(f'  ENTER={enters} ADD={adds} EXIT={exits} HOLD={holds}')
                lines.append(f'  dedupe={deduped} chase_blocked={chased}')
                lines.append('')

            # PnL by mode (join with execution_log if mode column exists)
            try:
                cur.execute("""
                    SELECT mode,
                           COUNT(*) AS filled,
                           ROUND(AVG(realized_pnl)::numeric, 4) AS avg_pnl,
                           ROUND(SUM(realized_pnl)::numeric, 4) AS total_pnl,
                           COUNT(*) FILTER (WHERE realized_pnl > 0) AS wins,
                           COUNT(*) FILTER (WHERE realized_pnl <= 0) AS losses
                    FROM execution_log
                    WHERE mode IS NOT NULL
                      AND status = 'FILLED'
                      AND realized_pnl IS NOT NULL
                      AND COALESCE(last_fill_at, ts) >= now() - interval '24 hours'
                    GROUP BY mode
                    ORDER BY mode
                """)
                pnl_rows = cur.fetchall()
                if pnl_rows:
                    lines.append('─── PnL by Mode ───')
                    for r in pnl_rows:
                        mode, filled, avg_pnl, total_pnl, wins, losses = r
                        wr = wins / filled * 100 if filled > 0 else 0
                        lines.append(f'MODE_{mode}: {filled}건 WR={wr:.0f}% '
                                     f'avg={avg_pnl} total={total_pnl}')
            except Exception:
                pass  # mode column may not exist yet

            # Gate block stats
            try:
                cur.execute("""
                    SELECT gate_status, COUNT(*)
                    FROM strategy_decision_log
                    WHERE ts >= now() - interval '24 hours'
                    GROUP BY gate_status
                    ORDER BY gate_status
                """)
                gate_rows = cur.fetchall()
                if gate_rows:
                    lines.append('')
                    lines.append('─── Gate Status ───')
                    for gs, cnt in gate_rows:
                        lines.append(f'  {gs}: {cnt}건')
            except Exception:
                pass

            return '\n'.join(lines)

    except Exception as e:
        return f'조회 실패: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _unknown(_text=None):
    return '알 수 없는 조회 유형입니다. /help 을 참고하세요.'


def _parse_minutes_and_limit(text=None):
    '''Migrated from telegram_cmd_poller.py.'''
    t = (text or '').strip().lower()
    minutes = 1440
    limit = 20
    if '오늘' in t:
        minutes = 1440
    if '최근' not in t and re.search('\\d+\\s*(분|min|minute|시간|h|hour)', t):
        minutes = 60
    m = re.search('(\\d+)\\s*(분|min|minute)', t)
    if m:
        minutes = int(m.group(1))
    h = re.search('(\\d+)\\s*(시간|h|hour)', t)
    if h:
        minutes = int(h.group(1)) * 60
    n = re.search('(\\d+)\\s*(개|건)', t)
    if n:
        limit = int(n.group(1))
    minutes = max(5, min(minutes, 10080))
    limit = max(1, min(limit, 50))
    return (minutes, limit)


def _macro_summary(_text=None):
    """매크로/거시경제 지표 최신 값 조회."""
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (source) source, price, ts
                FROM macro_data
                ORDER BY source, ts DESC;
            """)
            rows = cur.fetchall()
        if not rows:
            return '📊 매크로 데이터 없음'
        lines = ['📊 거시경제 지표 현황']
        source_kr = {
            'QQQ': 'QQQ(나스닥 추종)',
            'SPY': 'SPY(S&P500)',
            'DXY': 'DXY(달러 인덱스)',
            'US10Y': 'US10Y(미국 10년물)',
            'VIX': 'VIX(공포 지수)',
        }
        for row in rows:
            src = row[0]
            price = float(row[1]) if row[1] else 0
            ts = str(row[2])[:16] if row[2] else '?'
            label = source_kr.get(src, src)
            lines.append(f'- {label}: {price:,.2f} ({ts})')
        return '\n'.join(lines)
    except Exception as e:
        return f'⚠ 매크로 조회 오류: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _db_monthly_stats(_text=None):
    """월별 데이터 저장량 리포트."""
    conn = None
    try:
        conn = _db()
        lines = ['📊 DB 월별 통계']
        lines.append('━━━━━━━━━━━━━━━━━━')

        # Section 1: Cross-table summary (total, 24h, date range)
        with conn.cursor() as cur:
            summary_query = """
                SELECT 'news' as tbl, COUNT(*) as total,
                       COUNT(*) FILTER (WHERE ts >= now() - interval '24 hours') as last_24h,
                       MIN(ts) as earliest, MAX(ts) as latest
                FROM news
                UNION ALL
                SELECT 'candles', COUNT(*),
                       COUNT(*) FILTER (WHERE ts >= now() - interval '24 hours'),
                       MIN(ts), MAX(ts)
                FROM candles
                UNION ALL
                SELECT 'macro_data', COUNT(*),
                       COUNT(*) FILTER (WHERE ts >= now() - interval '24 hours'),
                       MIN(ts), MAX(ts)
                FROM macro_data
                UNION ALL
                SELECT 'execution_log', COUNT(*),
                       COUNT(*) FILTER (WHERE ts >= now() - interval '24 hours'),
                       MIN(ts), MAX(ts)
                FROM execution_log
                UNION ALL
                SELECT 'score_history', COUNT(*),
                       COUNT(*) FILTER (WHERE ts >= now() - interval '24 hours'),
                       MIN(ts), MAX(ts)
                FROM score_history;
            """
            try:
                cur.execute(summary_query)
                rows = cur.fetchall()
                lines.append('\n[테이블 요약]')
                for row in rows:
                    tbl, total, last_24h, earliest, latest = row
                    earliest_str = str(earliest)[:16] if earliest else '-'
                    latest_str = str(latest)[:16] if latest else '-'
                    lines.append(f'  {tbl}: {total:,}건 (24h: {last_24h:,}건)')
                    lines.append(f'    {earliest_str} ~ {latest_str}')
            except Exception as e:
                lines.append(f'\n[테이블 요약] 조회 실패: {e}')

            # macro_events summary (new table)
            try:
                cur.execute("""
                    SELECT COUNT(*),
                           MIN(event_date)::text,
                           MAX(event_date)::text
                    FROM macro_events;
                """)
                row = cur.fetchone()
                if row and row[0]:
                    lines.append(f'  macro_events: {row[0]:,}건')
                    lines.append(f'    {(row[1] or "-")} ~ {(row[2] or "-")}')
                else:
                    lines.append('  macro_events: 0건')
            except Exception:
                lines.append('  macro_events: 테이블 미생성')

        # Section 2: Monthly breakdown per table
        tables = [
            ('candles', 'ts'),
            ('news', 'ts'),
            ('indicators', 'ts'),
            ('events', 'start_ts'),
            ('pm_decision_log', 'ts'),
            ('macro_data', 'ts'),
            ('score_history', 'ts'),
            ('macro_events', 'event_date'),
        ]
        lines.append('\n[월별 데이터량]')
        with conn.cursor() as cur:
            for table, ts_col in tables:
                try:
                    cur.execute(f"""
                        SELECT date_trunc('month', {ts_col}) AS month,
                               count(*) AS cnt
                        FROM {table}
                        GROUP BY month
                        ORDER BY month DESC
                        LIMIT 6;
                    """)
                    rows = cur.fetchall()
                    lines.append(f'\n  [{table}]')
                    if not rows:
                        lines.append('    데이터 없음')
                    else:
                        for row in rows:
                            month_str = str(row[0])[:7] if row[0] else '?'
                            lines.append(f'    {month_str}: {row[1]:,}건')
                except Exception:
                    lines.append(f'\n  [{table}] 조회 실패')
        return '\n'.join(lines)
    except Exception as e:
        return f'⚠ 월별 통계 오류: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _debug_order_throttle(_text=None):
    """주문 속도 제한 상태 + 60분 타임라인."""
    import order_throttle
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            status = order_throttle.get_throttle_status(cur)
            lines = ['🚦 Order Throttle Guard', '━━━━━━━━━━━━━━━━━━']
            # Rate limits
            lines.append(f'📊 Rate Limits')
            lines.append(f'  1h: {status["hourly_count"]}/{status["hourly_limit"]}')
            lines.append(f'  10m: {status["10min_count"]}/{status["10min_limit"]}')
            # Entry lock
            if status.get('entry_locked'):
                lines.append(f'🔒 ENTRY LOCKED: {status["lock_reason"]}')
                lines.append(f'  expires: {status["lock_expires_str"]}')
            else:
                lines.append('🔓 Entry: UNLOCKED')
            # Cooldowns
            lines.append(f'\n⏱ Cooldowns')
            for action, remaining in status.get('cooldowns', {}).items():
                icon = '⏳' if remaining > 0 else '✅'
                lines.append(f'  {icon} {action}: {remaining:.0f}s' if remaining > 0 else f'  {icon} {action}: ready')
            # Last reject
            if status.get('last_reject'):
                lines.append(f'\n❌ Last Reject')
                lines.append(f'  {status["last_reject"][:100]}')
                lines.append(f'  at: {status.get("last_reject_ts_str", "?")}')
            # Backoff state
            if status.get('network_consecutive', 0) > 0:
                lines.append(f'🌐 Network errors: {status["network_consecutive"]} consecutive')
            if status.get('db_error_consecutive', 0) > 0:
                lines.append(f'💾 DB errors: {status["db_error_consecutive"]} consecutive')
            # 60-min timeline from DB
            cur.execute("""
                SELECT date_trunc('minute', ts) AS m, count(*),
                       count(*) FILTER (WHERE outcome='SUCCESS'),
                       count(*) FILTER (WHERE outcome IN ('REJECTED','ERROR','BLOCKED'))
                FROM order_attempt_log
                WHERE symbol='BTC/USDT:USDT' AND ts >= now()-interval '60 minutes'
                GROUP BY m ORDER BY m;
            """)
            rows = cur.fetchall()
            if rows:
                lines.append(f'\n📈 60-min Timeline ({len(rows)} active minutes)')
                for m, cnt, ok, fail in rows[-20:]:
                    bar = '█' * min(ok, 10) + '░' * min(fail, 10)
                    lines.append(f'  {m.strftime("%H:%M")} {bar} ({ok}/{fail})')
            return '\n'.join(lines)
    except Exception as e:
        return f'⚠ order_throttle error: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _reconcile(_text=None):
    """Compare exchange position vs strategy DB state and report mismatches."""
    try:
        exch = exchange_reader.fetch_position(SYMBOL)
        strat = exchange_reader.fetch_position_strat(SYMBOL)
        orders = exchange_reader.fetch_open_orders(SYMBOL)

        exch_pos = exch.get('exchange_position', 'UNKNOWN')
        exch_qty = exch.get('exch_pos_qty', 0)
        exch_status = exch.get('data_status', 'ERROR')

        strat_side = (strat.get('strat_side') or '').upper()
        strat_state = strat.get('strat_state', 'UNKNOWN')
        strat_qty = float(strat.get('strat_qty') or 0)
        order_state = strat.get('order_state', '')

        open_count = len(orders.get('orders', []))

        lines = ['[RECONCILE] 거래소 vs 전략DB 대조']
        lines.append('━━━━━━━━━━━━━━━━━━━━━━━━')

        # Section 1: Exchange
        lines.append(f'\n[거래소(Bybit)]')
        if exch_status != 'OK':
            lines.append(f'  상태: ERROR — {exch.get("error", "API 호출 실패")}')
        else:
            lines.append(f'  포지션: {exch_pos}')
            lines.append(f'  수량: {exch_qty}')
            if exch_pos != 'NONE':
                lines.append(f'  진입가: {exch.get("exch_entry_price", 0)}')
                lines.append(f'  미실현PnL: {exch.get("upnl", 0):.2f} USDT')
        lines.append(f'  미체결주문: {open_count}건')

        # Section 2: Strategy DB
        lines.append(f'\n[전략DB]')
        lines.append(f'  상태: {strat_state}')
        lines.append(f'  방향: {strat_side or "NONE"}')
        lines.append(f'  수량: {strat_qty}')
        if order_state:
            lines.append(f'  order_state: {order_state}')

        # Section 3: Comparison
        lines.append(f'\n[대조 결과]')

        exch_dir = exch_pos if exch_pos != 'NONE' else 'NONE'
        strat_dir = strat_side if strat_side else 'NONE'

        if exch_status != 'OK':
            verdict = 'UNKNOWN — 거래소 API 오류로 비교 불가'
        elif exch_dir == 'NONE' and strat_dir == 'NONE':
            verdict = 'MATCH — 양쪽 모두 포지션 없음'
        elif exch_dir == 'NONE' and strat_dir != 'NONE':
            if order_state in ('SENT', 'PENDING', 'ACKED'):
                verdict = f'PENDING — 전략DB {strat_dir} 의도, 주문 미체결 대기 중 (order_state={order_state})'
            else:
                verdict = f'MISMATCH — 거래소 NONE, 전략DB {strat_dir} ({strat_state})'
        elif exch_dir != 'NONE' and strat_dir == 'NONE':
            verdict = f'MISMATCH — 거래소 {exch_dir}, 전략DB NONE'
        elif exch_dir == strat_dir:
            verdict = f'MATCH — 양쪽 모두 {exch_dir}'
        else:
            verdict = f'MISMATCH — 거래소 {exch_dir}, 전략DB {strat_dir}'

        lines.append(f'  {verdict}')

        if 'MISMATCH' in verdict:
            lines.append(f'\n⚠ 불일치 감지 — exchange_reader 자동 복구 대기 중')
            lines.append(f'  수동 복구: /debug gate_details 에서 상태 확인')

        return '\n'.join(lines)
    except Exception as e:
        return f'⚠ reconcile 오류: {e}'


def _mctx_status(_text=None):
    """MCTX status: regime, features, vol_pct, spread_ok, liquidity_ok, drift."""
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            import regime_reader
            ctx = regime_reader.get_current_regime(cur)

            from strategy.common.features import build_feature_snapshot
            features = build_feature_snapshot(cur, SYMBOL)

        if not ctx.get('available'):
            return '[MCTX] 데이터 없음 (FAIL-OPEN: UNKNOWN 모드)'

        import mctx_formatter
        return mctx_formatter.format_mctx(features, ctx)
    except Exception as e:
        return f'MCTX error: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _mode_params(_text=None):
    """Current regime mode parameters: TP/SL/leverage/stage/entry filter."""
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            import regime_reader
            ctx = regime_reader.get_current_regime(cur)

        regime = ctx.get('regime', 'UNKNOWN')
        shock_type = ctx.get('shock_type')
        params = regime_reader.get_regime_params(regime, shock_type)

        lines = [f'[MODE] {regime} 모드 파라미터']
        lines.append('━━━━━━━━━━━━━━━━━━━━━━━━')
        tp_mode = params.get('tp_mode', 'fixed')
        if tp_mode == 'fixed':
            lines.append(f'  TP: fixed {params.get("tp_pct_min", 0)}-{params.get("tp_pct_max", 0)}%')
        elif tp_mode == 'trailing':
            lines.append(f'  TP: trailing (activate={params.get("trail_activate_pct", 0)}%, '
                         f'trail={params.get("trail_pct", 0)}%)')
        lines.append(f'  SL: {params.get("sl_pct", 2.0)}%')
        lines.append(f'  레버리지: {params.get("leverage_min", 3)}-{params.get("leverage_max", 8)}x')
        lines.append(f'  최대 스테이지: {params.get("stage_max", 7)}')
        lines.append(f'  진입 필터: {params.get("entry_filter", "none")}')
        lines.append(f'  ADD 점수 기준: {params.get("add_score_threshold", 45)}')

        if not ctx.get('available'):
            lines.append('\n  ※ MCTX 미가용 — UNKNOWN(FAIL-OPEN) 적용 중')

        return '\n'.join(lines)
    except Exception as e:
        return f'⚠ MODE 오류: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
