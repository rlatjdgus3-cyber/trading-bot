"""
proactive_manager.py — Active Risk Orchestrator (Claude-powered middle manager).

Passive → Active 전환: 기존 시스템이 "위험 임계값 도달 후 반응"이었다면,
이 모듈은 "위험을 예측하고 선제 대응"합니다.

기능:
  1. Macro Risk Monitor  — QQQ/VIX 실시간 변동 감시 → SL 조임, 포지션 축소
  2. Pre-Event Guard     — FOMC/CPI/NFP 전 선제 방어
  3. Score Trend Tracker — 점수 추세 악화 감지 → 경고/조치
  4. Stop-Loss ETA      — SL 도달 예상 시간 계산 → 선제 알림
  5. Entry Veto          — autopilot 진입 차단 (고변동/매크로/연패)

설계 원칙:
  - FAIL-OPEN: 오류 시 기존 동작 방해 안 함
  - 비파괴적: 절대 직접 주문하지 않음 (execution_queue 통해서만)
  - 로깅: 모든 판단을 proactive_log 테이블에 기록
  - 쓰로틀: 같은 조치 30분 이내 중복 방지
"""
import os
import sys
import time
import json
import traceback

sys.path.insert(0, '/root/trading-bot/app')

LOG_PREFIX = '[proactive]'
SYMBOL = 'BTC/USDT:USDT'

# ── Macro risk thresholds ──────────────────────────────────
# QQQ 변동률 (%)
QQQ_WARN_30M_PCT = -0.5       # 30분 -0.5% → SL 조임 경고
QQQ_REDUCE_1H_PCT = -1.0      # 1시간 -1.0% → 포지션 30% 축소
QQQ_EMERGENCY_2H_PCT = -1.5   # 2시간 -1.5% → Claude 강제 분석
# VIX 변동률 (%)
VIX_SPIKE_1H_PCT = 15.0       # VIX 1시간 +15% → 진입 차단
VIX_HIGH_ABSOLUTE = 30.0      # VIX 절대값 30 이상 → 경고

# ── Pre-event guard ────────────────────────────────────────
PRE_EVENT_REDUCE_PCT = 30     # 이벤트 전 포지션 축소 비율
PRE_EVENT_BLOCK_ENTRY = True  # 이벤트 윈도우 중 신규 진입 차단

# ── Score trend ────────────────────────────────────────────
SCORE_DECLINE_WINDOW = 5      # 최근 N 사이클 점수 추적
SCORE_RAPID_DROP_THRESHOLD = 20  # 10분 내 점수 20 이상 하락 → 경고

# ── SL ETA ─────────────────────────────────────────────────
SL_ETA_WARN_MINUTES = 5       # SL 도달 예상 5분 이내 → 경고

# ── Action cooldowns (초) ──────────────────────────────────
ACTION_COOLDOWN = {
    'macro_warn': 1800,       # 30분
    'macro_reduce': 3600,     # 1시간
    'macro_emergency': 1800,  # 30분
    'pre_event_reduce': 7200, # 2시간
    'pre_event_block': 600,   # 10분 (로깅용)
    'score_warn': 1200,       # 20분
    'sl_eta_warn': 600,       # 10분
    'vix_block': 1800,        # 30분
    'entry_veto': 300,        # 5분
}

# ── State tracking ──────────────────────────────────────────
_score_history = []           # [(timestamp, total_score), ...]
_last_action_ts = {}          # {action_type: timestamp}
_macro_cache = {}             # {source: {price, ts}}
_macro_cache_ts = 0
MACRO_CACHE_TTL_SEC = 30      # 매크로 데이터 캐시 30초


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _is_cooled_down(action_type):
    """Check if action is within cooldown."""
    last = _last_action_ts.get(action_type, 0)
    cooldown = ACTION_COOLDOWN.get(action_type, 600)
    return (time.time() - last) >= cooldown


def _record_action(action_type):
    """Record action timestamp for cooldown."""
    _last_action_ts[action_type] = time.time()


# ── 1. Macro Risk Monitor ──────────────────────────────────

def _fetch_macro_prices(cur):
    """Fetch latest QQQ, VIX prices + time-series from macro_data.
    Returns {source: {latest, ago_30m, ago_1h, ago_2h}} or empty on failure.
    """
    global _macro_cache, _macro_cache_ts
    now = time.time()
    if _macro_cache and (now - _macro_cache_ts) < MACRO_CACHE_TTL_SEC:
        return _macro_cache

    result = {}
    try:
        for source in ('QQQ', 'VIX'):
            # Latest price
            cur.execute("""
                SELECT price, ts FROM macro_data
                WHERE source = %s ORDER BY ts DESC LIMIT 1;
            """, (source,))
            row = cur.fetchone()
            if not row or not row[0]:
                continue
            latest = float(row[0])
            latest_ts = row[1]

            # Price 30m ago
            cur.execute("""
                SELECT price FROM macro_data
                WHERE source = %s AND ts <= now() - interval '30 minutes'
                ORDER BY ts DESC LIMIT 1;
            """, (source,))
            row_30m = cur.fetchone()
            ago_30m = float(row_30m[0]) if row_30m and row_30m[0] else None

            # Price 1h ago
            cur.execute("""
                SELECT price FROM macro_data
                WHERE source = %s AND ts <= now() - interval '1 hour'
                ORDER BY ts DESC LIMIT 1;
            """, (source,))
            row_1h = cur.fetchone()
            ago_1h = float(row_1h[0]) if row_1h and row_1h[0] else None

            # Price 2h ago
            cur.execute("""
                SELECT price FROM macro_data
                WHERE source = %s AND ts <= now() - interval '2 hours'
                ORDER BY ts DESC LIMIT 1;
            """, (source,))
            row_2h = cur.fetchone()
            ago_2h = float(row_2h[0]) if row_2h and row_2h[0] else None

            result[source] = {
                'latest': latest,
                'latest_ts': latest_ts,
                'ago_30m': ago_30m,
                'ago_1h': ago_1h,
                'ago_2h': ago_2h,
            }

        _macro_cache = result
        _macro_cache_ts = now
    except Exception as e:
        _log(f'macro fetch error (FAIL-OPEN): {e}')

    return result


def _compute_change_pct(latest, ago):
    """Compute percentage change. Returns None if data missing."""
    if latest is None or ago is None or ago == 0:
        return None
    return ((latest - ago) / ago) * 100


def check_macro_risk(cur, pos=None):
    """Check macro risk conditions and return action recommendations.

    Returns list of {action, severity, reason, detail} dicts.
    Severity: 'info', 'warn', 'action', 'emergency'
    """
    actions = []
    macro = _fetch_macro_prices(cur)
    if not macro:
        return actions

    # ── QQQ analysis ──
    qqq = macro.get('QQQ', {})
    if qqq:
        chg_30m = _compute_change_pct(qqq.get('latest'), qqq.get('ago_30m'))
        chg_1h = _compute_change_pct(qqq.get('latest'), qqq.get('ago_1h'))
        chg_2h = _compute_change_pct(qqq.get('latest'), qqq.get('ago_2h'))

        # Check BTC-QQQ correlation regime
        is_coupled = _check_coupled_risk(cur)

        # Level 1: 30분 -0.5% → SL 조임 경고
        if chg_30m is not None and chg_30m <= QQQ_WARN_30M_PCT:
            if _is_cooled_down('macro_warn'):
                actions.append({
                    'action': 'TIGHTEN_SL',
                    'severity': 'warn',
                    'reason': f'QQQ 30분 {chg_30m:+.2f}% 하락 — SL 30% 조임',
                    'detail': {'qqq_chg_30m': chg_30m, 'coupled': is_coupled},
                    'sl_tighten_pct': 30,
                })

        # Level 2: 1시간 -1.0% → 포지션 30% 축소 (COUPLED_RISK일 때만)
        if chg_1h is not None and chg_1h <= QQQ_REDUCE_1H_PCT:
            if is_coupled and _is_cooled_down('macro_reduce'):
                has_pos = pos and pos.get('side') and float(pos.get('qty', 0)) > 0
                if has_pos:
                    actions.append({
                        'action': 'REDUCE',
                        'severity': 'action',
                        'reason': f'QQQ 1시간 {chg_1h:+.2f}% (COUPLED_RISK) — 포지션 30% 축소',
                        'detail': {'qqq_chg_1h': chg_1h, 'coupled': True},
                        'reduce_pct': 30,
                    })

        # Level 3: 2시간 -1.5% → Claude 강제 분석
        if chg_2h is not None and chg_2h <= QQQ_EMERGENCY_2H_PCT:
            if _is_cooled_down('macro_emergency'):
                actions.append({
                    'action': 'CLAUDE_ANALYSIS',
                    'severity': 'emergency',
                    'reason': f'QQQ 2시간 {chg_2h:+.2f}% 급락 — Claude 긴급 분석',
                    'detail': {'qqq_chg_2h': chg_2h, 'coupled': is_coupled},
                })

    # ── VIX analysis ──
    vix = macro.get('VIX', {})
    if vix and vix.get('latest'):
        vix_latest = vix['latest']
        vix_chg_1h = _compute_change_pct(vix_latest, vix.get('ago_1h'))

        # VIX 급등 → 진입 차단
        if vix_chg_1h is not None and vix_chg_1h >= VIX_SPIKE_1H_PCT:
            if _is_cooled_down('vix_block'):
                actions.append({
                    'action': 'BLOCK_ENTRY',
                    'severity': 'warn',
                    'reason': f'VIX 1시간 +{vix_chg_1h:.1f}% 급등 — 신규 진입 차단',
                    'detail': {'vix_latest': vix_latest, 'vix_chg_1h': vix_chg_1h},
                    'block_duration_sec': 1800,
                })

        # VIX 절대값 높음
        if vix_latest >= VIX_HIGH_ABSOLUTE:
            if _is_cooled_down('macro_warn'):
                actions.append({
                    'action': 'WARN',
                    'severity': 'warn',
                    'reason': f'VIX {vix_latest:.1f} 고위험 구간 — 주의',
                    'detail': {'vix_latest': vix_latest},
                })

    return actions


def _check_coupled_risk(cur):
    """Check if BTC-QQQ are in COUPLED_RISK regime."""
    try:
        import regime_reader
        regime = regime_reader.get_current_regime(cur)
        # Check btc_qqq_regime from scores
        cur.execute("""
            SELECT details->>'btc_qqq_regime'
            FROM score_history
            WHERE details IS NOT NULL
            ORDER BY ts DESC LIMIT 1;
        """)
        row = cur.fetchone()
        if row and row[0] == 'COUPLED_RISK':
            return True
        # Also check via score_engine if available
        return False
    except Exception:
        return False


# ── 2. Pre-Event Guard ─────────────────────────────────────

def check_pre_event(cur, pos=None):
    """Check if macro event is imminent and recommend preemptive actions.

    Returns list of {action, severity, reason, detail} dicts.
    """
    actions = []
    try:
        from macro_collector import is_macro_event_window, get_upcoming_macro_events
    except ImportError:
        return actions

    try:
        event_status = is_macro_event_window()
        if not event_status.get('active'):
            return actions

        events = event_status.get('events', [])
        event_names = ', '.join(events)

        # Event window active → block entries
        if PRE_EVENT_BLOCK_ENTRY and _is_cooled_down('pre_event_block'):
            actions.append({
                'action': 'BLOCK_ENTRY',
                'severity': 'warn',
                'reason': f'매크로 이벤트 ({event_names}) 윈도우 — 신규 진입 차단',
                'detail': {'events': events, 'window': event_status},
                'block_duration_sec': 3600,
            })

        # If position exists, recommend reduce
        has_pos = pos and pos.get('side') and float(pos.get('qty', 0)) > 0
        if has_pos and _is_cooled_down('pre_event_reduce'):
            actions.append({
                'action': 'REDUCE',
                'severity': 'action',
                'reason': f'매크로 이벤트 ({event_names}) 전 — 포지션 {PRE_EVENT_REDUCE_PCT}% 선제 축소',
                'detail': {'events': events, 'reduce_pct': PRE_EVENT_REDUCE_PCT},
                'reduce_pct': PRE_EVENT_REDUCE_PCT,
            })

    except Exception as e:
        _log(f'pre_event check error (FAIL-OPEN): {e}')

    return actions


# ── 3. Score Trend Tracker ──────────────────────────────────

def track_score(total_score):
    """Record score for trend analysis."""
    global _score_history
    now = time.time()
    _score_history.append((now, total_score))
    # Keep only last 10 minutes
    cutoff = now - 600
    _score_history = [(ts, s) for ts, s in _score_history if ts >= cutoff]


def check_score_trend():
    """Detect deteriorating score trends.

    Returns list of {action, severity, reason, detail} dicts.
    """
    actions = []
    if len(_score_history) < 3:
        return actions

    try:
        latest_score = _score_history[-1][1]
        oldest_score = _score_history[0][1]
        time_span = _score_history[-1][0] - _score_history[0][0]

        # Rapid decline: abs_score dropped by 20+ in < 10 minutes
        score_drop = abs(oldest_score) - abs(latest_score)
        if score_drop >= SCORE_RAPID_DROP_THRESHOLD and time_span <= 600:
            if _is_cooled_down('score_warn'):
                actions.append({
                    'action': 'TIGHTEN_SL',
                    'severity': 'warn',
                    'reason': f'점수 급락 ({oldest_score:.0f} → {latest_score:.0f}, {time_span:.0f}s) — SL 조임',
                    'detail': {
                        'score_drop': score_drop,
                        'from_score': oldest_score,
                        'to_score': latest_score,
                        'time_span_sec': time_span,
                    },
                    'sl_tighten_pct': 20,
                })

        # Consecutive declines: last 3 cycles all declining
        if len(_score_history) >= 3:
            recent = _score_history[-3:]
            scores = [abs(s) for _, s in recent]
            if scores[0] > scores[1] > scores[2]:
                decline_total = scores[0] - scores[2]
                if decline_total >= 10 and _is_cooled_down('score_warn'):
                    actions.append({
                        'action': 'WARN',
                        'severity': 'info',
                        'reason': f'점수 3연속 하락 ({scores[0]:.0f}→{scores[1]:.0f}→{scores[2]:.0f})',
                        'detail': {'scores': scores, 'decline': decline_total},
                    })
    except Exception as e:
        _log(f'score trend check error (FAIL-OPEN): {e}')

    return actions


# ── 4. Stop-Loss ETA ────────────────────────────────────────

def check_sl_eta(pos, snapshot, dynamic_sl_pct=2.0):
    """Estimate time to stop-loss hit based on current momentum.

    Returns action dict or None.
    """
    if not pos or not pos.get('side') or not snapshot:
        return None

    try:
        entry = pos.get('entry_price', 0)
        price = snapshot.get('price', 0)
        if entry <= 0 or price <= 0:
            return None

        side = pos['side'].lower()

        # Current distance to SL
        if side == 'long':
            sl_price = entry * (1 - dynamic_sl_pct / 100)
            dist_to_sl = price - sl_price
            # Momentum: use 5-min return rate
            returns = snapshot.get('returns', {})
            ret_5m = returns.get('ret_5m')
            if ret_5m is not None and ret_5m < 0:
                # Price falling towards SL
                price_move_per_min = abs(ret_5m / 100 * price) / 5
                if price_move_per_min > 0:
                    eta_min = dist_to_sl / price_move_per_min
                else:
                    return None
            else:
                return None  # Price moving away from SL
        else:  # short
            sl_price = entry * (1 + dynamic_sl_pct / 100)
            dist_to_sl = sl_price - price
            returns = snapshot.get('returns', {})
            ret_5m = returns.get('ret_5m')
            if ret_5m is not None and ret_5m > 0:
                price_move_per_min = abs(ret_5m / 100 * price) / 5
                if price_move_per_min > 0:
                    eta_min = dist_to_sl / price_move_per_min
                else:
                    return None
            else:
                return None

        if eta_min <= SL_ETA_WARN_MINUTES and _is_cooled_down('sl_eta_warn'):
            return {
                'action': 'WARN',
                'severity': 'warn',
                'reason': f'SL 도달 예상 {eta_min:.1f}분 — 현재 모멘텀 지속 시',
                'detail': {
                    'sl_price': round(sl_price, 1),
                    'current_price': round(price, 1),
                    'dist_to_sl': round(dist_to_sl, 1),
                    'eta_minutes': round(eta_min, 1),
                    'momentum_per_min': round(price_move_per_min, 1),
                },
            }
    except Exception as e:
        _log(f'SL ETA check error (FAIL-OPEN): {e}')

    return None


# ── 5. Entry Veto ───────────────────────────────────────────

# In-memory veto state
_entry_veto = {
    'active': False,
    'reason': '',
    'until': 0,
}


def check_entry_veto(cur):
    """Check if entry should be vetoed. Called by autopilot_daemon.

    Returns (vetoed: bool, reason: str).
    FAIL-OPEN: Returns (False, '') on any error.
    """
    now = time.time()

    # Check existing veto
    if _entry_veto['active'] and _entry_veto['until'] > now:
        remaining = int(_entry_veto['until'] - now)
        return (True, f'{_entry_veto["reason"]} ({remaining}s)')

    # Clear expired veto
    if _entry_veto['active'] and _entry_veto['until'] <= now:
        _entry_veto['active'] = False
        _entry_veto['reason'] = ''
        _entry_veto['until'] = 0

    # Check DB-persisted veto
    try:
        cur.execute("""
            SELECT value FROM openclaw_policies
            WHERE key = 'proactive_entry_veto';
        """)
        row = cur.fetchone()
        if row and row[0]:
            veto_data = row[0] if isinstance(row[0], dict) else json.loads(row[0])
            if veto_data.get('active'):
                until = veto_data.get('until', 0)
                if until > now:
                    return (True, veto_data.get('reason', 'proactive veto'))
                else:
                    # Expired, clean up
                    cur.execute("""
                        UPDATE openclaw_policies
                        SET value = '{"active": false}'::jsonb, updated_at = now()
                        WHERE key = 'proactive_entry_veto';
                    """)
    except Exception:
        pass  # FAIL-OPEN

    return (False, '')


def set_entry_veto(cur, reason, duration_sec=1800):
    """Set entry veto (blocks autopilot entries)."""
    until = time.time() + duration_sec
    _entry_veto['active'] = True
    _entry_veto['reason'] = reason
    _entry_veto['until'] = until

    # Persist to DB for cross-process visibility
    try:
        veto_data = json.dumps({
            'active': True,
            'reason': reason,
            'until': until,
            'set_at': time.time(),
        })
        cur.execute("""
            INSERT INTO openclaw_policies (key, value, updated_at, description)
            VALUES ('proactive_entry_veto', %s::jsonb, now(), 'proactive manager entry veto')
            ON CONFLICT (key) DO UPDATE
            SET value = EXCLUDED.value, updated_at = now();
        """, (veto_data,))
    except Exception as e:
        _log(f'set_entry_veto DB error (memory-only): {e}')

    _log(f'ENTRY VETO SET: {reason} (duration={duration_sec}s)')


def clear_entry_veto(cur):
    """Clear entry veto."""
    _entry_veto['active'] = False
    _entry_veto['reason'] = ''
    _entry_veto['until'] = 0
    try:
        cur.execute("""
            UPDATE openclaw_policies
            SET value = '{"active": false}'::jsonb, updated_at = now()
            WHERE key = 'proactive_entry_veto';
        """)
    except Exception:
        pass


# ── Logging ─────────────────────────────────────────────────

def _log_proactive_action(cur, action_type, severity, reason, detail=None):
    """Log proactive action to DB."""
    try:
        cur.execute("""
            INSERT INTO proactive_log
                (symbol, action_type, severity, reason, detail)
            VALUES (%s, %s, %s, %s, %s::jsonb);
        """, (
            SYMBOL,
            action_type,
            severity,
            reason,
            json.dumps(detail or {}, default=str, ensure_ascii=False),
        ))
    except Exception as e:
        # Table might not exist yet — just log to stdout
        _log(f'proactive_log insert error (non-fatal): {e}')


# ── Telegram ────────────────────────────────────────────────

def _send_telegram(text):
    """Send Telegram notification."""
    try:
        import urllib.parse
        import urllib.request
        import report_formatter

        env_path = '/root/trading-bot/app/telegram_cmd.env'
        cfg = {}
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if '=' in line and not line.startswith('#'):
                    k, v = line.split('=', 1)
                    cfg[k.strip()] = v.strip()

        token = cfg.get('TELEGRAM_BOT_TOKEN', '')
        chat_id = cfg.get('TELEGRAM_ALLOWED_CHAT_ID', '')
        if not token or not chat_id:
            return

        text = report_formatter.korean_output_guard(text)
        url = f'https://api.telegram.org/bot{token}/sendMessage'
        data = urllib.parse.urlencode({
            'chat_id': chat_id,
            'text': text,
            'disable_web_page_preview': 'true',
        }).encode('utf-8')
        req = urllib.request.Request(url, data=data, method='POST')
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass


# ── Main Orchestrator ───────────────────────────────────────

def evaluate(cur, pos=None, snapshot=None, scores=None, dynamic_sl_pct=2.0):
    """Run all proactive checks and return combined action list.

    Called from position_manager._cycle() on every loop iteration.

    Args:
        cur: DB cursor
        pos: Current Bybit position dict
        snapshot: Market snapshot from market_snapshot.build_snapshot()
        scores: Score engine result dict
        dynamic_sl_pct: Current dynamic stop loss percentage

    Returns:
        list of action dicts: [{action, severity, reason, detail, ...}]
    """
    all_actions = []

    # 1. Macro risk
    try:
        macro_actions = check_macro_risk(cur, pos)
        all_actions.extend(macro_actions)
    except Exception as e:
        _log(f'macro risk check error (FAIL-OPEN): {e}')

    # 2. Pre-event guard
    try:
        event_actions = check_pre_event(cur, pos)
        all_actions.extend(event_actions)
    except Exception as e:
        _log(f'pre-event check error (FAIL-OPEN): {e}')

    # 3. Score trend
    if scores:
        total_score = scores.get('total_score', 0)
        track_score(total_score)
    try:
        score_actions = check_score_trend()
        all_actions.extend(score_actions)
    except Exception as e:
        _log(f'score trend check error (FAIL-OPEN): {e}')

    # 4. SL ETA
    try:
        sl_action = check_sl_eta(pos, snapshot, dynamic_sl_pct)
        if sl_action:
            all_actions.append(sl_action)
    except Exception as e:
        _log(f'SL ETA check error (FAIL-OPEN): {e}')

    return all_actions


def execute_actions(cur, actions, pos=None, send_telegram=True):
    """Execute proactive actions and return summary.

    Respects cooldowns: same action type not repeated within cooldown window.

    Args:
        cur: DB cursor
        actions: List from evaluate()
        pos: Current position dict
        send_telegram: Whether to send Telegram notifications

    Returns:
        dict: {executed: [...], skipped: [...], entry_veto_set: bool}
    """
    executed = []
    skipped = []
    entry_veto_set = False

    for act in actions:
        action_type = act.get('action', 'WARN')
        severity = act.get('severity', 'info')
        reason = act.get('reason', '')

        # Cooldown check
        cooldown_key = _get_cooldown_key(act)
        if not _is_cooled_down(cooldown_key):
            skipped.append({'action': action_type, 'reason': 'cooldown'})
            continue

        # Execute based on action type
        if action_type == 'REDUCE' and pos and pos.get('side'):
            reduce_pct = act.get('reduce_pct', 30)
            _enqueue_reduce(cur, pos, reduce_pct, reason)
            _record_action(cooldown_key)
            _log_proactive_action(cur, 'REDUCE', severity, reason, act.get('detail'))
            executed.append(act)

            if send_telegram:
                _send_telegram(
                    f'[Proactive Manager] REDUCE {reduce_pct}%\n'
                    f'{reason}')

        elif action_type == 'TIGHTEN_SL':
            tighten_pct = act.get('sl_tighten_pct', 20)
            _apply_sl_tighten(cur, tighten_pct, reason)
            _record_action(cooldown_key)
            _log_proactive_action(cur, 'TIGHTEN_SL', severity, reason, act.get('detail'))
            executed.append(act)

            if send_telegram:
                _send_telegram(
                    f'[Proactive Manager] SL {tighten_pct}% 조임\n'
                    f'{reason}')

        elif action_type == 'BLOCK_ENTRY':
            duration = act.get('block_duration_sec', 1800)
            set_entry_veto(cur, reason, duration)
            _record_action(cooldown_key)
            _log_proactive_action(cur, 'BLOCK_ENTRY', severity, reason, act.get('detail'))
            executed.append(act)
            entry_veto_set = True

            if send_telegram:
                _send_telegram(
                    f'[Proactive Manager] 진입 차단 ({duration // 60}분)\n'
                    f'{reason}')

        elif action_type == 'CLAUDE_ANALYSIS':
            _trigger_claude_analysis(cur, pos, act)
            _record_action(cooldown_key)
            _log_proactive_action(cur, 'CLAUDE_ANALYSIS', severity, reason, act.get('detail'))
            executed.append(act)

            if send_telegram:
                _send_telegram(
                    f'[Proactive Manager] Claude 긴급 분석 요청\n'
                    f'{reason}')

        elif action_type == 'WARN':
            _record_action(cooldown_key)
            _log_proactive_action(cur, 'WARN', severity, reason, act.get('detail'))
            executed.append(act)

            if send_telegram and severity in ('warn', 'emergency'):
                _send_telegram(
                    f'[Proactive Manager] {reason}')

        else:
            skipped.append({'action': action_type, 'reason': 'unknown action'})

    return {
        'executed': executed,
        'skipped': skipped,
        'entry_veto_set': entry_veto_set,
    }


def _get_cooldown_key(act):
    """Generate cooldown key from action."""
    action_type = act.get('action', 'WARN')
    severity = act.get('severity', 'info')
    if action_type == 'REDUCE' and severity == 'action':
        detail = act.get('detail', {})
        if detail.get('coupled'):
            return 'macro_reduce'
        if 'events' in detail:
            return 'pre_event_reduce'
        return 'macro_reduce'
    if action_type == 'TIGHTEN_SL':
        return 'macro_warn' if 'qqq' in act.get('reason', '').lower() else 'score_warn'
    if action_type == 'BLOCK_ENTRY':
        if 'VIX' in act.get('reason', ''):
            return 'vix_block'
        return 'pre_event_block'
    if action_type == 'CLAUDE_ANALYSIS':
        return 'macro_emergency'
    if action_type == 'WARN':
        if 'SL' in act.get('reason', ''):
            return 'sl_eta_warn'
        return 'macro_warn'
    return 'entry_veto'


# ── Action implementations ──────────────────────────────────

def _enqueue_reduce(cur, pos, reduce_pct, reason):
    """Enqueue REDUCE action to execution_queue."""
    try:
        pos_side = (pos.get('side') or '').upper()
        pos_qty = float(pos.get('qty', 0))
        reduce_qty = pos_qty * reduce_pct / 100

        MIN_ORDER_QTY = 0.001
        if reduce_qty < MIN_ORDER_QTY:
            _log(f'REDUCE skipped: qty {reduce_qty:.4f} < min {MIN_ORDER_QTY}')
            return

        cur.execute("""
            INSERT INTO execution_queue
                (symbol, action_type, direction, qty, reason, source, priority, status)
            VALUES (%s, 'REDUCE', %s, %s, %s, 'proactive_manager', 3, 'PENDING');
        """, (SYMBOL, pos_side, reduce_qty, f'proactive: {reason}'))
        _log(f'ENQUEUED REDUCE {reduce_pct}% ({reduce_qty:.4f} BTC): {reason}')
    except Exception as e:
        _log(f'enqueue REDUCE error: {e}')


def _apply_sl_tighten(cur, tighten_pct, reason):
    """Tighten SL by reducing dynamic_sl range.

    Uses server_stop_manager to sync tighter SL if available.
    """
    try:
        # Store tighten state in openclaw_policies for position_manager to read
        sl_override = json.dumps({
            'tighten_pct': tighten_pct,
            'reason': reason,
            'set_at': time.time(),
            'expires_at': time.time() + 1800,  # 30분 후 만료
        })
        cur.execute("""
            INSERT INTO openclaw_policies (key, value, updated_at, description)
            VALUES ('proactive_sl_tighten', %s::jsonb, now(), 'proactive SL tightening')
            ON CONFLICT (key) DO UPDATE
            SET value = EXCLUDED.value, updated_at = now();
        """, (sl_override,))
        _log(f'SL TIGHTEN {tighten_pct}%: {reason}')
    except Exception as e:
        _log(f'SL tighten error: {e}')


def _trigger_claude_analysis(cur, pos, act):
    """Trigger Claude emergency analysis for macro risk."""
    try:
        import claude_gate
        detail = act.get('detail', {})
        reason = act.get('reason', '')

        prompt = (
            f"[Proactive Manager 긴급 분석 요청]\n\n"
            f"상황: {reason}\n"
            f"매크로 데이터: {json.dumps(detail, ensure_ascii=False)}\n\n"
            f"현재 포지션: {json.dumps(pos, default=str, ensure_ascii=False) if pos else 'NONE'}\n\n"
            "=== 요청 사항 ===\n"
            "1. 현재 매크로 리스크 수준 평가 (1-10)\n"
            "2. BTC 영향 분석 (NASDAQ 연관성, 시간차 등)\n"
            "3. 권장 액션: HOLD / REDUCE [%] / CLOSE\n"
            "4. 대기 시 재평가 시점\n\n"
            "JSON 형식: {action, reduce_pct, risk_level, reason}\n"
            "500자 이내."
        )

        result = claude_gate.call_claude(
            gate='openclaw',
            prompt=prompt,
            cooldown_key='proactive_macro_analysis',
            context={
                'intent': 'proactive_macro_risk',
                'source': 'proactive_manager',
                'is_emergency': True,
            },
            max_tokens=300,
            call_type='AUTO_EMERGENCY',
        )

        if result and not result.get('fallback_used'):
            text = result.get('text', '')
            _log(f'Claude macro analysis: {text[:200]}')
            # Parse action from Claude response
            _apply_claude_recommendation(cur, pos, result)
        else:
            _log(f'Claude analysis gated: {result.get("gate_reason", "unknown")}')

    except Exception as e:
        _log(f'Claude analysis trigger error: {e}')
        traceback.print_exc()


def _apply_claude_recommendation(cur, pos, result):
    """Parse and apply Claude's proactive recommendation."""
    try:
        text = result.get('text', '')
        # Try to extract JSON from response
        import re
        json_match = re.search(r'\{[^}]+\}', text)
        if not json_match:
            _log(f'Claude response has no JSON — skipping auto-apply')
            return

        rec = json.loads(json_match.group())
        action = rec.get('action', 'HOLD').upper()

        if action == 'REDUCE' and pos and pos.get('side'):
            reduce_pct = rec.get('reduce_pct', 30)
            _enqueue_reduce(cur, pos, reduce_pct, f'claude_proactive: {rec.get("reason", "")}')
            _send_telegram(
                f'[Proactive] Claude 분석 → REDUCE {reduce_pct}%\n'
                f'Risk: {rec.get("risk_level", "?")} | {rec.get("reason", "")}')
        elif action == 'CLOSE' and pos and pos.get('side'):
            pos_side = (pos.get('side') or '').upper()
            cur.execute("""
                INSERT INTO execution_queue
                    (symbol, action_type, direction, qty, reason, source, priority, status)
                VALUES (%s, 'CLOSE', %s, %s, %s, 'proactive_manager', 2, 'PENDING');
            """, (SYMBOL, pos_side, pos.get('qty'),
                  f'proactive_claude: {rec.get("reason", "")}'))
            _send_telegram(
                f'[Proactive] Claude 분석 → CLOSE\n'
                f'Risk: {rec.get("risk_level", "?")} | {rec.get("reason", "")}')
        else:
            _log(f'Claude recommendation: {action} (no action taken)')
    except Exception as e:
        _log(f'Claude recommendation parse error: {e}')


# ── SL Tighten Reader (for position_manager) ────────────────

def get_sl_tighten_factor(cur):
    """Read current SL tighten override.

    Returns: float multiplier (e.g., 0.7 = 30% tighter) or 1.0 if none.
    Called from position_manager to adjust SL calculation.
    """
    try:
        cur.execute("""
            SELECT value FROM openclaw_policies
            WHERE key = 'proactive_sl_tighten';
        """)
        row = cur.fetchone()
        if not row or not row[0]:
            return 1.0

        data = row[0] if isinstance(row[0], dict) else json.loads(row[0])
        expires = data.get('expires_at', 0)
        if time.time() > expires:
            return 1.0

        tighten_pct = data.get('tighten_pct', 0)
        return max(0.3, 1.0 - tighten_pct / 100)
    except Exception:
        return 1.0  # FAIL-OPEN


# ── Macro Summary (for Telegram /status) ─────────────────────

def get_macro_summary(cur):
    """Get current macro risk summary for display.

    Returns: dict with human-readable macro state.
    """
    result = {
        'qqq': None,
        'vix': None,
        'macro_event': None,
        'entry_veto': None,
        'sl_tighten': None,
        'risk_level': 'NORMAL',
    }

    try:
        macro = _fetch_macro_prices(cur)

        # QQQ
        qqq = macro.get('QQQ', {})
        if qqq and qqq.get('latest'):
            chg_1h = _compute_change_pct(qqq['latest'], qqq.get('ago_1h'))
            chg_2h = _compute_change_pct(qqq['latest'], qqq.get('ago_2h'))
            result['qqq'] = {
                'price': qqq['latest'],
                'chg_1h': round(chg_1h, 2) if chg_1h else None,
                'chg_2h': round(chg_2h, 2) if chg_2h else None,
            }

        # VIX
        vix = macro.get('VIX', {})
        if vix and vix.get('latest'):
            result['vix'] = {
                'price': vix['latest'],
            }

        # Event window
        try:
            from macro_collector import is_macro_event_window
            event_status = is_macro_event_window()
            if event_status.get('active'):
                result['macro_event'] = event_status.get('events', [])
        except ImportError:
            pass

        # Entry veto
        vetoed, reason = check_entry_veto(cur)
        if vetoed:
            result['entry_veto'] = reason

        # SL tighten
        factor = get_sl_tighten_factor(cur)
        if factor < 1.0:
            result['sl_tighten'] = f'{int((1 - factor) * 100)}%'

        # Risk level
        if result.get('entry_veto') or (result.get('qqq', {}).get('chg_1h') or 0) <= -1.0:
            result['risk_level'] = 'HIGH'
        elif result.get('macro_event') or (result.get('qqq', {}).get('chg_1h') or 0) <= -0.5:
            result['risk_level'] = 'ELEVATED'
        else:
            result['risk_level'] = 'NORMAL'

    except Exception as e:
        _log(f'macro summary error: {e}')

    return result


# ── 6. Periodic Review (6h Claude Review) ──────────────────

REVIEW_INTERVAL_SEC = 6 * 3600  # 6시간
_last_review_ts = 0


def _build_review_prompt(metrics):
    """Build Claude prompt for periodic strategy review."""
    lines = [
        'Role: Crypto trading strategy reviewer.',
        'Analyze the recent trading performance and suggest improvements.',
        'Respond with STRICT JSON array of proposals.',
        '',
        '── Performance Metrics ──',
        f'  period: {metrics.get("period", "?")}',
        f'  total_trades: {metrics.get("total_trades", 0)}',
        f'  win_rate: {metrics.get("win_rate", 0):.1f}%',
        f'  profit_factor: {metrics.get("profit_factor", 0):.2f}',
        f'  avg_win: {metrics.get("avg_win", 0):.2f} USDT',
        f'  avg_loss: {metrics.get("avg_loss", 0):.2f} USDT',
        f'  max_drawdown: {metrics.get("max_drawdown", 0):.2f}%',
        f'  total_pnl: {metrics.get("total_pnl", 0):.2f} USDT',
        f'  sharpe_ratio: {metrics.get("sharpe_ratio", 0):.2f}',
        '',
        '── Loss Cluster Analysis ──',
        f'  consecutive_losses_max: {metrics.get("consecutive_losses_max", 0)}',
        f'  loss_hours_cluster: {metrics.get("loss_hours_cluster", [])}',
        f'  worst_regime: {metrics.get("worst_regime", "?")}',
        '',
        '── Current Config ──',
        f'  sl_atr_mult: {metrics.get("sl_atr_mult", 0)}',
        f'  risk_pct: {metrics.get("risk_pct", 0)}',
        f'  adx_enter_threshold: {metrics.get("adx_enter_threshold", 0)}',
        f'  daily_loss_limit_pct: {metrics.get("daily_loss_limit_pct", 0)}',
        '',
        '── Instructions ──',
        'Suggest 1-3 config-level improvements. DO NOT suggest code changes.',
        'Each proposal should be actionable with a specific config_key and value.',
        '',
        'Respond with JSON:',
        '[{',
        '  "category": "RISK|ENTRY|EXIT|TIMING|POSITION_SIZE",',
        '  "title": "short title",',
        '  "description": "what and why",',
        '  "config_key": "config key to change (e.g. sl_atr_mult)",',
        '  "current_value": "current value",',
        '  "proposed_value": "proposed value",',
        '  "confidence": 0.0-1.0,',
        '  "reasoning": "detailed reasoning"',
        '}]',
    ]
    return '\n'.join(lines)


def _fetch_review_metrics(cur):
    """Fetch trading performance metrics for Claude review."""
    metrics = {
        'period': '7d',
        'total_trades': 0,
        'win_rate': 0,
        'profit_factor': 0,
        'avg_win': 0,
        'avg_loss': 0,
        'max_drawdown': 0,
        'total_pnl': 0,
        'sharpe_ratio': 0,
        'consecutive_losses_max': 0,
        'loss_hours_cluster': [],
        'worst_regime': 'unknown',
        'sl_atr_mult': 2.0,
        'risk_pct': 0.0075,
        'adx_enter_threshold': 27,
        'daily_loss_limit_pct': -0.025,
    }

    try:
        # Trade stats (last 7 days)
        cur.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE realized_pnl > 0) as wins,
                COUNT(*) FILTER (WHERE realized_pnl <= 0) as losses,
                COALESCE(SUM(realized_pnl), 0) as total_pnl,
                COALESCE(AVG(realized_pnl) FILTER (WHERE realized_pnl > 0), 0) as avg_win,
                COALESCE(AVG(realized_pnl) FILTER (WHERE realized_pnl <= 0), 0) as avg_loss,
                COALESCE(SUM(realized_pnl) FILTER (WHERE realized_pnl > 0), 0) as gross_profit,
                COALESCE(ABS(SUM(realized_pnl) FILTER (WHERE realized_pnl <= 0)), 0.01) as gross_loss
            FROM execution_log
            WHERE ts >= now() - interval '7 days'
              AND action_type IN ('CLOSE', 'FULL_CLOSE', 'REDUCE', 'REVERSE_CLOSE');
        """)
        row = cur.fetchone()
        if row and row[0] > 0:
            total, wins, losses = row[0], row[1], row[2]
            metrics['total_trades'] = total
            metrics['win_rate'] = (wins / total * 100) if total > 0 else 0
            metrics['total_pnl'] = float(row[3])
            metrics['avg_win'] = float(row[4])
            metrics['avg_loss'] = float(row[5])
            gross_profit = float(row[6])
            gross_loss = float(row[7])
            metrics['profit_factor'] = gross_profit / gross_loss if gross_loss > 0 else 0

        # Consecutive losses
        cur.execute("""
            WITH ordered AS (
                SELECT realized_pnl,
                       ROW_NUMBER() OVER (ORDER BY ts) as rn,
                       SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END)
                           OVER (ORDER BY ts) as grp
                FROM execution_log
                WHERE ts >= now() - interval '7 days'
                  AND action_type IN ('CLOSE', 'FULL_CLOSE', 'REDUCE', 'REVERSE_CLOSE')
            )
            SELECT MAX(streak) FROM (
                SELECT grp, COUNT(*) as streak
                FROM ordered WHERE realized_pnl <= 0
                GROUP BY grp
            ) sub;
        """)
        row = cur.fetchone()
        if row and row[0]:
            metrics['consecutive_losses_max'] = int(row[0])

        # Loss hours cluster
        cur.execute("""
            SELECT EXTRACT(HOUR FROM ts)::int as hr, COUNT(*) as cnt
            FROM execution_log
            WHERE ts >= now() - interval '7 days'
              AND realized_pnl <= 0
              AND action_type IN ('CLOSE', 'FULL_CLOSE', 'REDUCE', 'REVERSE_CLOSE')
            GROUP BY hr ORDER BY cnt DESC LIMIT 3;
        """)
        metrics['loss_hours_cluster'] = [
            {'hour': row[0], 'count': row[1]} for row in cur.fetchall()
        ]

        # Worst regime
        cur.execute("""
            SELECT regime_tag, SUM(realized_pnl) as total_loss
            FROM execution_log
            WHERE ts >= now() - interval '7 days'
              AND realized_pnl <= 0
              AND regime_tag IS NOT NULL
            GROUP BY regime_tag ORDER BY total_loss ASC LIMIT 1;
        """)
        row = cur.fetchone()
        if row:
            metrics['worst_regime'] = row[0]

        # Current config
        try:
            from strategy_v3 import config_v3
            cfg = config_v3.get_all()
            metrics['sl_atr_mult'] = cfg.get('sl_atr_mult', 2.0)
            metrics['risk_pct'] = cfg.get('risk_pct', 0.0075)
            metrics['adx_enter_threshold'] = cfg.get('adx_enter_threshold', 27)
            metrics['daily_loss_limit_pct'] = cfg.get('daily_loss_limit_pct', -0.025)
        except Exception:
            pass

    except Exception as e:
        _log(f'fetch review metrics error: {e}')

    return metrics


def _parse_review_proposals(text):
    """Parse Claude review response into proposal list."""
    import re
    if not text:
        return []

    raw = text.strip()
    raw = re.sub(r'^\s*```(?:json)?\s*\n?', '', raw)
    raw = re.sub(r'\n?\s*```\s*$', '', raw)
    raw = raw.strip()

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r'\[[\s\S]*\]', raw)
        if m:
            try:
                data = json.loads(m.group())
            except json.JSONDecodeError:
                _log(f'review proposals parse failed: {raw[:200]}')
                return []
        else:
            _log(f'review proposals no JSON array found: {raw[:200]}')
            return []

    if not isinstance(data, list):
        data = [data]

    proposals = []
    for item in data[:5]:  # Max 5 proposals
        if not isinstance(item, dict):
            continue
        proposals.append({
            'category': str(item.get('category', 'RISK'))[:30],
            'title': str(item.get('title', ''))[:200],
            'description': str(item.get('description', ''))[:500],
            'config_key': str(item.get('config_key', ''))[:100],
            'current_value': str(item.get('current_value', ''))[:100],
            'proposed_value': str(item.get('proposed_value', ''))[:100],
            'confidence': max(0.0, min(1.0, float(item.get('confidence', 0.5)))),
            'reasoning': str(item.get('reasoning', ''))[:500],
        })

    return proposals


def run_periodic_review(cur, force=False):
    """Run 6-hourly Claude strategy review.

    Fetches trading metrics, sends to Claude, stores proposals in DB.
    Called from position_manager or autopilot_daemon on schedule.

    Args:
        cur: DB cursor
        force: If True, bypass interval check (for /review_now command)

    Returns: dict with {review_done, proposals_count, proposals}
    """
    global _last_review_ts

    # Interval check
    if not force:
        now = time.time()
        if now - _last_review_ts < REVIEW_INTERVAL_SEC:
            return {'review_done': False, 'reason': 'interval'}

    _log('periodic review starting')

    try:
        # 1. Fetch metrics
        metrics = _fetch_review_metrics(cur)
        if metrics['total_trades'] < 3:
            _log('periodic review skipped: insufficient trades')
            _last_review_ts = time.time()
            return {'review_done': False, 'reason': 'insufficient_trades'}

        # 2. Build prompt and call Claude
        prompt = _build_review_prompt(metrics)

        import claude_gate
        result = claude_gate.call_claude(
            gate='openclaw',
            prompt=prompt,
            cooldown_key='periodic_review',
            context={'intent': 'periodic_review', 'source': 'proactive_manager'},
            max_tokens=800,
            call_type='AUTO',
        )

        if result.get('fallback_used'):
            _log(f'periodic review Claude denied: {result.get("gate_reason", "?")}')
            return {'review_done': False, 'reason': f'gate_denied: {result.get("gate_reason")}'}

        # 3. Parse proposals
        text = result.get('text', '')
        proposals = _parse_review_proposals(text)
        _log(f'periodic review: {len(proposals)} proposals parsed')

        # 4. Store in DB
        stored_ids = []
        for p in proposals:
            try:
                cur.execute("""
                    INSERT INTO proposals
                        (review_type, category, title, description,
                         current_value, proposed_value, config_key,
                         confidence, reasoning, metrics_snapshot, claude_raw)
                    VALUES ('periodic_6h', %s, %s, %s, %s, %s, %s, %s, %s,
                            %s::jsonb, %s::jsonb)
                    RETURNING id;
                """, (
                    p['category'], p['title'], p['description'],
                    p['current_value'], p['proposed_value'], p['config_key'],
                    p['confidence'], p['reasoning'],
                    json.dumps(metrics, default=str, ensure_ascii=False),
                    json.dumps({'raw_text': text}, ensure_ascii=False),
                ))
                row = cur.fetchone()
                if row:
                    stored_ids.append(row[0])
            except Exception as e:
                _log(f'proposal store error: {e}')

        # 5. Telegram notification
        if proposals:
            lines = [f'📋 전략 리뷰 완료 ({len(proposals)}건 제안)']
            for i, p in enumerate(proposals[:3], 1):
                lines.append(f'{i}. [{p["category"]}] {p["title"]}')
                if p.get('config_key'):
                    lines.append(f'   {p["config_key"]}: {p["current_value"]} → {p["proposed_value"]}')
            lines.append(f'\n/proposals 로 전체 조회 | /apply_proposal <id> 로 적용')
            _send_telegram('\n'.join(lines))

        _last_review_ts = time.time()
        _log(f'periodic review done: {len(proposals)} proposals stored, ids={stored_ids}')

        return {
            'review_done': True,
            'proposals_count': len(proposals),
            'proposals': proposals,
            'stored_ids': stored_ids,
        }

    except Exception as e:
        _log(f'periodic review error: {e}')
        traceback.print_exc()
        _last_review_ts = time.time()  # Don't retry immediately
        return {'review_done': False, 'reason': f'error: {e}'}


def get_pending_proposals(cur, limit=10):
    """Fetch pending proposals from DB.

    Returns list of proposal dicts.
    """
    try:
        cur.execute("""
            SELECT id, ts, category, title, description,
                   config_key, current_value, proposed_value,
                   confidence, reasoning
            FROM proposals
            WHERE status = 'pending'
            ORDER BY ts DESC
            LIMIT %s;
        """, (limit,))
        proposals = []
        for row in cur.fetchall():
            proposals.append({
                'id': row[0],
                'ts': str(row[1]),
                'category': row[2],
                'title': row[3],
                'description': row[4],
                'config_key': row[5],
                'current_value': row[6],
                'proposed_value': row[7],
                'confidence': float(row[8]) if row[8] else 0,
                'reasoning': row[9],
            })
        return proposals
    except Exception as e:
        _log(f'get_pending_proposals error: {e}')
        return []


def apply_proposal(cur, proposal_id, applied_by='telegram'):
    """Apply a proposal by updating the referenced config key.

    SAFETY: Only updates config via strategy_v3.config_v3 — no direct code changes.

    Returns: (success: bool, message: str)
    """
    try:
        cur.execute("""
            SELECT id, config_key, proposed_value, status, title
            FROM proposals WHERE id = %s;
        """, (proposal_id,))
        row = cur.fetchone()
        if not row:
            return (False, f'제안 #{proposal_id} 없음')
        if row[3] != 'pending':
            return (False, f'제안 #{proposal_id} 이미 처리됨 (status={row[3]})')

        config_key = row[1]
        proposed_value = row[2]
        title = row[4]

        if not config_key or not proposed_value:
            return (False, f'제안 #{proposal_id}: config_key 또는 proposed_value 누락')

        # Apply via config_v3
        from strategy_v3 import config_v3
        try:
            # Try numeric conversion
            try:
                val = float(proposed_value)
                if val == int(val):
                    val = int(val)
            except (ValueError, TypeError):
                val = proposed_value

            config_v3.set_value(config_key, val)
        except Exception as e:
            return (False, f'config 적용 실패: {e}')

        # Mark as applied
        cur.execute("""
            UPDATE proposals
            SET status = 'applied', applied_at = now(), applied_by = %s
            WHERE id = %s;
        """, (applied_by, proposal_id))

        msg = f'✅ 제안 #{proposal_id} 적용 완료\n- {title}\n- {config_key}: → {proposed_value}'
        _send_telegram(msg)
        _log(f'proposal #{proposal_id} applied: {config_key}={proposed_value} by {applied_by}')
        return (True, msg)

    except Exception as e:
        _log(f'apply_proposal error: {e}')
        return (False, f'에러: {e}')


# ── DB Migration ─────────────────────────────────────────────

def ensure_tables(cur):
    """Create proactive_log table if not exists."""
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS proactive_log (
                id BIGSERIAL PRIMARY KEY,
                ts TIMESTAMPTZ NOT NULL DEFAULT now(),
                symbol TEXT NOT NULL DEFAULT 'BTC/USDT:USDT',
                action_type TEXT NOT NULL,
                severity TEXT NOT NULL DEFAULT 'info',
                reason TEXT,
                detail JSONB DEFAULT '{}'::jsonb
            );
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_proactive_log_ts
            ON proactive_log(ts DESC);
        """)
    except Exception as e:
        _log(f'ensure_tables error: {e}')
