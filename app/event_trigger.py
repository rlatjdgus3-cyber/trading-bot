"""
event_trigger.py — Event detection engine.

Snapshot-based event evaluation to decide if/how Claude should be called.

Modes:
  DEFAULT    — score_engine only, no AI call
  EVENT      — anomaly detected → GPT-mini default, Claude only if high-risk gate passes
  EMERGENCY  — critical condition → Claude forced (no budget/cooldown limit)
  USER       — /force command → Claude forced (no budget/cooldown limit)

Cost control:
  - EVENT Claude gated by 4 conditions (confidence, ret_5m, vol_spike, level break)
  - EVENT Claude cooldown: 15 min minimum
  - EVENT Claude daily cap: 20 calls/day
  - event_hash dedupe: 15 min window
  - Telegram throttle: same event type max once per 10 min
"""
import hashlib
import json
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, '/root/trading-bot/app')

LOG_PREFIX = '[event_trigger]'

# ── modes ──────────────────────────────────────────────────
MODE_DEFAULT = 'DEFAULT'
MODE_EVENT = 'EVENT'
MODE_EMERGENCY = 'EMERGENCY'
MODE_USER = 'USER'

# ── price spike thresholds (return %) ──────────────────────
PRICE_SPIKE_1M_PCT = 1.0
PRICE_SPIKE_5M_PCT = 1.8
PRICE_SPIKE_15M_PCT = 3.0

# ── volume spike ───────────────────────────────────────────
VOL_SPIKE_RATIO = 2.0  # vol_last >= 2.0 * vol_ma20

# ── level breaks (POC/VAH/VAL) ────────────────────────────
POC_SHIFT_MIN_PCT = 0.3
VAH_VAL_SUSTAIN_CANDLES = 3

# ── regime change ──────────────────────────────────────────
REGIME_SCORE_CHANGE_MIN = 15
ATR_INCREASE_PCT = 30

# ── emergency escalation ──────────────────────────────────
EMERGENCY_5M_RET_PCT = 1.0  # 요구: 5분 ≥ 1.0% → AUTO_EMERGENCY
EMERGENCY_15M_RET_PCT = 3.5
EMERGENCY_LOSS_PCT = 2.0
EMERGENCY_LIQ_DIST_PCT = 3.0
EMERGENCY_ATR_SURGE_PCT = 40
EMERGENCY_VOL_SPIKE_RATIO = 2.5
EMERGENCY_VOL_ZSCORE = 3.0

# ── box range filter (EMERGENCY suppression) ─────────────
BOX_BB_BANDWIDTH_PCT = 0.6
BOX_RET_5M_SUPPRESS_PCT = 1.0

# ── dedup ──────────────────────────────────────────────────
AUTO_DEDUP_WINDOW_SEC = 300    # 5 min (per-type trigger dedup)
EVENT_HASH_DEDUP_SEC = 900     # 15 min (same event_hash dedup)
EMERGENCY_LOCK_SEC = 180       # 3 min lock after emergency execution
MIN_ORDER_QTY_BTC = 0.001     # Bybit BTC/USDT:USDT minimum

# ── EVENT Claude escalation gate ─────────────────────────
EVENT_CLAUDE_COOLDOWN_SEC = 300   # 5 min minimum between EVENT Claude calls
# EVENT_CLAUDE_DAILY_CAP: 삭제됨 (claude_gate의 비용 기반 제어에 위임)
EVENT_CLAUDE_MIN_RET_5M = 1.2     # abs(ret_5m) >= 1.2%
EVENT_CLAUDE_MIN_VOL_RATIO = 2.5  # vol_spike >= 2.5x
EVENT_CLAUDE_MIN_CONFIDENCE = 0.75  # trigger confidence threshold

# ── Async Claude escalation (need_claude) ───────────────
ASYNC_CLAUDE_COOLDOWN_SEC = 600        # 10분 쿨다운
ASYNC_CLAUDE_RET_5M_THRESHOLD = 1.0    # 조건A: |ret_5m| >= 1.0%
ASYNC_CLAUDE_CONFIDENCE_THRESHOLD = 70  # 조건A: GPT confidence < 70
ASYNC_CLAUDE_BAR_15M_CUMULATIVE = 1.5  # 조건B: 누적 >=1.5% (같은 방향)
ASYNC_CLAUDE_NEWS_SCORE_THRESHOLD = 40  # 조건C: |news_event_score| >= 40

# ── Telegram throttle ────────────────────────────────────
TELEGRAM_EVENT_THROTTLE_SEC = 600  # same event type: max once per 10 min

# ── Whipsaw guard (vol_spike + level_break) ──────────────
WHIPSAW_GUARD_SEC = 120            # 120s wait when vol_spike + level_break coincide

# ── Event bundling (30s window) ──────────────────────────
BUNDLE_WINDOW_SEC = 30
_event_bundle = {
    'triggers': [],
    'first_ts': 0,
    'whipsaw_first_ts': 0,
}

# ── emergency lock state ──────────────────────────────────
_emergency_lock = {}           # {symbol: expire_timestamp}
_last_emergency_action = {}    # {symbol: {'action': str, 'direction': str, 'ts': float}}

# ── per-type EVENT dedup (5 min) ─────────────────────────
_last_trigger_ts = {}          # {trigger_type: timestamp}

# ── EVENT Claude budget tracking ─────────────────────────
_event_claude_state = {
    'last_call_ts': 0,         # last EVENT Claude call timestamp
    'daily_date': '',          # YYYY-MM-DD for daily cap reset
    'daily_count': 0,          # EVENT Claude calls today
    'cap_notified': False,     # Telegram cap notification sent today
}

# ── event_hash 15-min dedup ──────────────────────────────
_event_hash_history = {}       # {event_hash: timestamp}

# ── Telegram event throttle ──────────────────────────────
_telegram_event_ts = {}        # {trigger_type_key: timestamp}

# ── HOLD repeat suppression (§3) — progressive locking ───
HOLD_REPEAT_LIMIT = 2             # first lock at 2 consecutive HOLDs
# Progressive lock durations (seconds)
HOLD_LOCK_PROGRESSIVE = {
    2: 600,    # 2회: 10분
    3: 1200,   # 3회: 20분
    4: 1800,   # 4회+: 30분
}

_last_hold_state = {
    'action': None,            # last Claude action ('HOLD', 'CLOSE', etc.)
    'trigger_types': [],       # sorted event types that triggered the call
    'position_side': None,     # position side at time of action
    'position_qty': 0,         # position qty at time of action
    'consecutive_holds': 0,    # count of consecutive HOLD results
    'lock_until': 0,           # timestamp when hold lock expires
}

# ── edge detection state ──────────────────────────────────
_prev_edge_state = {
    'price_spike_1m': False,
    'price_spike_5m': False,
    'price_spike_15m': False,
    'volume_spike': False,
    'atr_increase': False,
}
_last_evaluate_ts = 0
EDGE_STALE_SEC = 600  # 10분 이상 gap → 엣지 상태 리셋

# ── priority ───────────────────────────────────────────────
PRIORITY_EMERGENCY = 1
PRIORITY_USER = 2
PRIORITY_EVENT = 3
PRIORITY_DEFAULT = 10


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def reset_edge_state(reason=''):
    """Reset edge detection state + level state.

    Called on position change or staleness to allow re-triggering.
    """
    global _last_evaluate_ts
    for k in _prev_edge_state:
        _prev_edge_state[k] = False
    _prev_level_state['above_vah'] = False
    _prev_level_state['below_val'] = False
    _prev_level_state['poc_zone'] = None
    _last_evaluate_ts = 0
    reset_hold_state()
    _log(f'edge state RESET ({reason})')


def record_claude_result(action, trigger_types, position):
    """Record Claude action result for HOLD repeat detection (§3).

    Called after every Claude call (EVENT mode) to track
    whether the same HOLD + event combo should be suppressed.
    After N consecutive HOLDs (same triggers+position), enters 15-min lock.
    """
    sorted_types = sorted(trigger_types) if trigger_types else []
    cur_side = position.get('side') if position else None
    cur_qty = position.get('qty', 0) if position else 0

    if action == 'HOLD':
        # 컨텍스트 무관하게 모든 HOLD를 카운트 (트리거 변경 시에도 누적)
        _last_hold_state['consecutive_holds'] += 1
        count = _last_hold_state['consecutive_holds']

        # Progressive locking: 2→10m, 3→20m, 4+→30m
        if count >= HOLD_REPEAT_LIMIT:
            lock_sec = HOLD_LOCK_PROGRESSIVE.get(
                min(count, max(HOLD_LOCK_PROGRESSIVE.keys())),
                HOLD_LOCK_PROGRESSIVE[max(HOLD_LOCK_PROGRESSIVE.keys())])
            _last_hold_state['lock_until'] = time.time() + lock_sec
            _log(f'HOLD loop lock ACTIVATED: {count} '
                 f'consecutive HOLDs → {lock_sec}s lock (progressive)')
    else:
        # 비-HOLD 액션(REDUCE/CLOSE 등)이면 카운터 리셋
        _last_hold_state['consecutive_holds'] = 0

    _last_hold_state['action'] = action
    _last_hold_state['trigger_types'] = sorted_types
    _last_hold_state['position_side'] = cur_side
    _last_hold_state['position_qty'] = cur_qty
    _log(f'recorded claude result: action={action} '
         f'types={sorted_types} side={cur_side} '
         f'consecutive_holds={_last_hold_state["consecutive_holds"]}')


def is_hold_repeat(trigger_types, position) -> bool:
    """Check if this would be a duplicate HOLD call (§3).

    Skip if:
      1. HOLD loop lock active (N consecutive HOLDs → 15min lock)
      2. last_claude_action == "HOLD" AND same triggers+position
    """
    # Check HOLD loop lock first
    now = time.time()
    lock_until = _last_hold_state.get('lock_until', 0)
    if lock_until > now:
        remaining = int(lock_until - now)
        _log(f'HOLD loop LOCKED: {remaining}s remaining '
             f'(consecutive={_last_hold_state["consecutive_holds"]})')
        return True

    if _last_hold_state['action'] != 'HOLD':
        return False
    # Position changed?
    cur_side = position.get('side') if position else None
    cur_qty = position.get('qty', 0) if position else 0
    if cur_side != _last_hold_state['position_side']:
        return False
    if cur_qty != _last_hold_state['position_qty']:
        return False
    # Same event type?
    cur_types = sorted(trigger_types) if trigger_types else []
    if cur_types != _last_hold_state['trigger_types']:
        return False
    _log(f'HOLD repeat detected: types={cur_types} side={cur_side}')
    return True


def reset_hold_state():
    """Clear HOLD repeat state (called on position change or edge reset)."""
    _last_hold_state['action'] = None
    _last_hold_state['trigger_types'] = []
    _last_hold_state['position_side'] = None
    _last_hold_state['position_qty'] = 0
    _last_hold_state['consecutive_holds'] = 0
    _last_hold_state['lock_until'] = 0


# ── EVENT Claude escalation gate ─────────────────────────

def _check_daily_reset():
    """Reset daily counter if date changed."""
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    if _event_claude_state['daily_date'] != today:
        _event_claude_state['daily_date'] = today
        _event_claude_state['daily_count'] = 0
        _event_claude_state['cap_notified'] = False
        _log(f'EVENT Claude daily counter reset ({today})')


def should_use_claude_for_event(snapshot, triggers) -> tuple:
    """Decide if EVENT should escalate to Claude or use GPT-mini.

    Returns (use_claude: bool, reason: str).

    시장 조건 A/B/C 충족 시 → 무조건 True
    미충족이지만 abs(ret_5m) >= 0.8% AND vol_ratio >= 2.0 → True
    최종 승인은 claude_gate.request()에 위임.
    """
    _check_daily_reset()
    now = time.time()

    # 쿨다운 체크 (5분)
    elapsed = now - _event_claude_state['last_call_ts']
    if _event_claude_state['last_call_ts'] > 0 and elapsed < EVENT_CLAUDE_COOLDOWN_SEC:
        return False, f'cooldown ({int(elapsed)}s/{EVENT_CLAUDE_COOLDOWN_SEC}s)'

    returns = snapshot.get('returns', {}) if snapshot else {}
    ret_5m = returns.get('ret_5m')
    ret_10m = returns.get('ret_10m')
    vol_ratio = snapshot.get('vol_ratio', 0) if snapshot else 0

    # 시장 조건 바이패스 체크
    # 조건 A: abs(ret_5m) >= 1.0% 또는 abs(ret_10m) >= 1.0%
    if (ret_5m is not None and abs(ret_5m) >= 1.0) or \
       (ret_10m is not None and abs(ret_10m) >= 1.0):
        return True, 'market_condition_bypass (ret >= 1.0%)'

    # 조건 B: 15분봉 3연속 + 누적 1.5%
    bar_15m = snapshot.get('bar_15m_returns', []) if snapshot else []
    if len(bar_15m) >= 3:
        last3 = bar_15m[:3]
        all_pos = all(b > 0 for b in last3)
        all_neg = all(b < 0 for b in last3)
        if all_pos or all_neg:
            cumulative = abs(sum(last3))
            if cumulative >= 1.5:
                return True, f'market_condition_bypass (3bar cumulative={cumulative:.2f}%)'

    # 조건 C: 포지션 위험
    position_loss = snapshot.get('position_loss_pct') if snapshot else None
    liq_dist = snapshot.get('liq_dist_pct') if snapshot else None
    if position_loss is not None and abs(position_loss) >= 2.0:
        return True, f'market_condition_bypass (loss={position_loss:.2f}%)'
    if liq_dist is not None and liq_dist <= 3.0:
        return True, f'market_condition_bypass (liq_dist={liq_dist:.2f}%)'

    # 일반 이벤트: ret_5m >= 0.8% AND vol_ratio >= 2.0
    if ret_5m is not None and abs(ret_5m) >= 0.8 and vol_ratio >= 2.0:
        return True, f'high_event (ret_5m={ret_5m:.2f}%, vol={vol_ratio:.2f}x)'

    return False, f'below threshold (ret_5m={ret_5m}, vol={vol_ratio:.2f}x)'


def need_claude(snapshot, mini_result, scores) -> tuple:
    """Decide if async Claude should be spawned after GPT-mini 1차 결정.

    Returns (need: bool, reason: str).

    Checks: 쿨다운 30분 → 일일 캡 →
      조건A: abs(ret_5m) >= 1.0% AND confidence < 70
      조건B: 15분봉 3개 연속 ±1.0% 이상
      조건C: abs(news_event_score) >= 40
    → 하나라도 충족 시 True
    """
    _check_daily_reset()
    now = time.time()

    # ── 쿨다운 30분 ──
    elapsed = now - _event_claude_state['last_call_ts']
    if _event_claude_state['last_call_ts'] > 0 and elapsed < ASYNC_CLAUDE_COOLDOWN_SEC:
        return False, f'async_cooldown ({int(elapsed)}s/{ASYNC_CLAUDE_COOLDOWN_SEC}s)'

    # 일일 캡: claude_gate 비용 기반 제어에 위임

    # ── 조건A: |ret_5m| >= 1.0% AND confidence < 70 ──
    returns = snapshot.get('returns', {}) if snapshot else {}
    ret_5m = returns.get('ret_5m')
    confidence = None
    if isinstance(mini_result, dict):
        confidence = mini_result.get('confidence')
    if (ret_5m is not None and abs(ret_5m) >= ASYNC_CLAUDE_RET_5M_THRESHOLD
            and confidence is not None and confidence < ASYNC_CLAUDE_CONFIDENCE_THRESHOLD):
        return True, f'condition_A (ret_5m={ret_5m:.2f}%, confidence={confidence})'

    # ── 조건B: 15분봉 3개 같은 방향 + 누적 >=1.5% ──
    bar_15m = snapshot.get('bar_15m_returns', []) if snapshot else []
    if len(bar_15m) >= 3:
        last3 = bar_15m[:3]
        all_positive = all(b > 0 for b in last3)
        all_negative = all(b < 0 for b in last3)
        if all_positive or all_negative:
            cumulative = abs(sum(last3))
            if cumulative >= ASYNC_CLAUDE_BAR_15M_CUMULATIVE:
                direction = 'up' if all_positive else 'down'
                return True, f'condition_B ({direction} cumulative={cumulative:.2f}%, bar_15m={last3})'

    # ── 조건C: |news_event_score| >= 40 ──
    news_score = scores.get('news_event_score', 0) if scores else 0
    if abs(news_score) >= ASYNC_CLAUDE_NEWS_SCORE_THRESHOLD:
        return True, f'condition_C (news_score={news_score})'

    # ── 조건D: NASDAQ 연관 뉴스 + COUPLED_RISK regime ──
    btc_qqq_regime = scores.get('btc_qqq_regime') if scores else None
    if btc_qqq_regime == 'COUPLED_RISK' and abs(news_score) >= 20:
        return True, f'condition_D (coupled_risk + news_score={news_score})'

    return False, 'no condition met'


def record_event_claude_call():
    """Record an EVENT Claude call for cooldown + daily cap tracking."""
    _check_daily_reset()
    _event_claude_state['last_call_ts'] = time.time()
    _event_claude_state['daily_count'] += 1
    _log(f'EVENT Claude call recorded: '
         f'{_event_claude_state["daily_count"]} today')


def get_event_claude_stats() -> dict:
    """Return current EVENT Claude budget stats for logging."""
    _check_daily_reset()
    return {
        'daily_count': _event_claude_state['daily_count'],
        'daily_cap': 999,  # claude_gate 비용 기반 제어에 위임
        'daily_remaining': max(0, 999 - _event_claude_state['daily_count']),
        'cap_notified': _event_claude_state['cap_notified'],
        'last_call_ts': _event_claude_state['last_call_ts'],
        'cooldown_remaining': max(0, int(
            EVENT_CLAUDE_COOLDOWN_SEC - (time.time() - _event_claude_state['last_call_ts'])
        )) if _event_claude_state['last_call_ts'] > 0 else 0,
    }


def mark_cap_notified():
    """Mark that Telegram cap notification was sent today."""
    _event_claude_state['cap_notified'] = True


def is_cap_notified() -> bool:
    """Check if cap notification was already sent today."""
    _check_daily_reset()
    return _event_claude_state['cap_notified']


# ── event_hash 15-min dedup ──────────────────────────────

def check_event_hash_dedup(event_hash) -> bool:
    """Check if event_hash was seen within 30-min window.
    Returns True if duplicate (should skip).
    """
    if not event_hash:
        return False
    now = time.time()
    last_seen = _event_hash_history.get(event_hash, 0)
    if (now - last_seen) < EVENT_HASH_DEDUP_SEC:
        _log(f'event_hash dedup: {event_hash[:12]} seen {int(now - last_seen)}s ago')
        return True
    return False


def record_event_hash(event_hash):
    """Record event_hash timestamp + purge expired entries."""
    if not event_hash:
        return
    now = time.time()
    _event_hash_history[event_hash] = now
    # Purge expired
    expired = [k for k, v in _event_hash_history.items()
               if now - v > EVENT_HASH_DEDUP_SEC * 2]
    for k in expired:
        del _event_hash_history[k]


# ── Telegram event throttle ──────────────────────────────

def bundle_triggers(new_triggers) -> list:
    """Bundle triggers within 30s window into a single event.

    Returns combined triggers if window expired, else empty list (still accumulating).
    """
    now = time.time()
    if not new_triggers:
        # Check if bundle has accumulated triggers past window
        if _event_bundle['triggers'] and now - _event_bundle['first_ts'] >= BUNDLE_WINDOW_SEC:
            result = list(_event_bundle['triggers'])
            _event_bundle['triggers'] = []
            _event_bundle['first_ts'] = 0
            return result
        return []

    if not _event_bundle['triggers']:
        _event_bundle['first_ts'] = now

    # Merge new triggers (avoid dups by type)
    existing_types = {t.get('type') for t in _event_bundle['triggers']}
    for t in new_triggers:
        if t.get('type') not in existing_types:
            _event_bundle['triggers'].append(t)
            existing_types.add(t.get('type'))

    # If window expired, flush
    if now - _event_bundle['first_ts'] >= BUNDLE_WINDOW_SEC:
        result = list(_event_bundle['triggers'])
        _event_bundle['triggers'] = []
        _event_bundle['first_ts'] = 0
        if len(result) > 1:
            _log(f'event bundle flushed: {len(result)} triggers bundled')
        return result

    return []  # still accumulating


def should_send_telegram_event(trigger_types) -> bool:
    """Check if Telegram event notification should be sent.
    Same event type(s) limited to once per 10 min.
    """
    key = '|'.join(sorted(trigger_types)) if trigger_types else 'unknown'
    now = time.time()
    last_ts = _telegram_event_ts.get(key, 0)
    if (now - last_ts) < TELEGRAM_EVENT_THROTTLE_SEC:
        return False
    _telegram_event_ts[key] = now
    # Purge expired
    expired = [k for k, v in _telegram_event_ts.items()
               if now - v > TELEGRAM_EVENT_THROTTLE_SEC * 2]
    for k in expired:
        del _telegram_event_ts[k]
    return True


class EventResult:
    """Result of event evaluation."""

    __slots__ = ('mode', 'triggers', 'event_hash', 'priority',
                 'call_type', 'should_call_claude', 'position_critical')

    def __init__(self, mode=MODE_DEFAULT, triggers=None, event_hash=None,
                 priority=PRIORITY_DEFAULT, call_type='AUTO',
                 should_call_claude=False, position_critical=False):
        self.mode = mode
        self.triggers = triggers or []
        self.event_hash = event_hash
        self.priority = priority
        self.call_type = call_type
        self.should_call_claude = should_call_claude
        self.position_critical = position_critical

    def to_dict(self):
        return {
            'mode': self.mode,
            'triggers': self.triggers,
            'event_hash': self.event_hash,
            'priority': self.priority,
            'call_type': self.call_type,
            'should_call_claude': self.should_call_claude,
            'position_critical': self.position_critical,
        }


def evaluate(snapshot, prev_scores=None, position=None, cur=None,
             symbol='BTC/USDT:USDT') -> EventResult:
    """Main evaluation entry point.

    0. Check emergency lock → DEFAULT if active
    1. _check_emergency_escalation() → EMERGENCY → immediate return
    2. _check_price_spikes()
    3. _check_volume_spike()
    4. _check_level_breaks()
    5. _check_regime_change()
    6. No triggers → DEFAULT
    7. Has triggers → compute hash → EVENT
    """
    if not snapshot:
        return EventResult()

    # Emergency lock: suppress all triggers during cooldown
    if is_emergency_locked(symbol):
        return EventResult()

    # Edge staleness check: reset if >10min gap between evaluations
    global _last_evaluate_ts
    now_ts = time.time()
    if _last_evaluate_ts > 0 and (now_ts - _last_evaluate_ts) > EDGE_STALE_SEC:
        reset_edge_state(f'stale {int(now_ts - _last_evaluate_ts)}s')
    _last_evaluate_ts = now_ts

    price = snapshot.get('price', 0)

    # Phase 1: emergency escalation (highest priority)
    emergency_triggers = _check_emergency_escalation(snapshot, position, prev_scores)
    if emergency_triggers:
        # Section 5: box range filter — suppress EMERGENCY in tight ranges
        bb_upper = snapshot.get('bb_upper', 0)
        bb_lower = snapshot.get('bb_lower', 0)
        bb_mid = snapshot.get('bb_mid', 0)
        ret_5m = abs(snapshot.get('returns', {}).get('ret_5m') or 0)
        if bb_mid and bb_mid > 0:
            bb_bandwidth = (bb_upper - bb_lower) / bb_mid * 100
            if bb_bandwidth < BOX_BB_BANDWIDTH_PCT and ret_5m < BOX_RET_5M_SUPPRESS_PCT:
                _log(f'EMERGENCY suppressed by box range filter: '
                     f'bb_bandwidth={bb_bandwidth:.3f}% < {BOX_BB_BANDWIDTH_PCT}%, '
                     f'ret_5m={ret_5m:.2f}% < {BOX_RET_5M_SUPPRESS_PCT}%')
                return EventResult()

        has_pos_critical = any(t.get('position_critical') for t in emergency_triggers)
        eh = compute_event_hash(emergency_triggers, symbol=symbol, price=price)
        _log(f'EMERGENCY: triggers={[t["type"] for t in emergency_triggers]} '
             f'position_critical={has_pos_critical} hash={eh[:12]}')
        return EventResult(
            mode=MODE_EMERGENCY,
            triggers=emergency_triggers,
            event_hash=eh,
            priority=PRIORITY_EMERGENCY,
            call_type='AUTO_EMERGENCY',
            should_call_claude=True,
            position_critical=has_pos_critical,
        )

    # Phase 2-5: event triggers
    all_triggers = []
    all_triggers.extend(_check_price_spikes(snapshot))
    all_triggers.extend(_check_volume_spike(snapshot))
    all_triggers.extend(_check_level_breaks(snapshot, cur))
    all_triggers.extend(_check_regime_change(snapshot, prev_scores))

    if all_triggers:
        _log(f'edge triggers fired: {[t["type"] for t in all_triggers]} '
             f'state={_prev_edge_state}')

    if not all_triggers:
        return EventResult()  # DEFAULT

    # Section 2: per-type dedup (5 min) — filter triggers seen within 300s
    now = time.time()
    surviving = []
    for t in all_triggers:
        ttype = t.get('type', '')
        last_ts = _last_trigger_ts.get(ttype, 0)
        if now - last_ts < AUTO_DEDUP_WINDOW_SEC:
            _log(f'trigger {ttype} deduped ({int(now - last_ts)}s since last)')
        else:
            surviving.append(t)

    if not surviving:
        _log(f'all triggers deduped, returning DEFAULT')
        return EventResult()  # DEFAULT

    # Update timestamps for surviving triggers only + purge expired entries
    for t in surviving:
        _last_trigger_ts[t.get('type', '')] = now
    expired = [k for k, v in _last_trigger_ts.items() if now - v > AUTO_DEDUP_WINDOW_SEC * 2]
    for k in expired:
        del _last_trigger_ts[k]

    # Whipsaw guard: check BEFORE bundle flush so triggers are not lost.
    # Requires a separate volume_spike trigger alongside a level break trigger.
    accumulated = list(_event_bundle.get('triggers', [])) + surviving
    has_vol_spike_trigger = any(t.get('type') == 'volume_spike' for t in accumulated)
    has_level_break = any(t.get('type') in ('vah_break', 'val_break') for t in accumulated)
    if has_vol_spike_trigger and has_level_break:
        ws_first = _event_bundle.get('whipsaw_first_ts', 0)
        if ws_first == 0:
            _event_bundle['whipsaw_first_ts'] = time.time()
            _log('WHIPSAW_GUARD: vol_spike+level_break detected, starting 120s wait')
            # Don't flush yet — keep triggers in bundle for re-evaluation
            bundle_triggers(surviving)  # accumulate without consuming
            return EventResult()
        elapsed_ws = time.time() - ws_first
        if elapsed_ws < WHIPSAW_GUARD_SEC:
            _log(f'WHIPSAW_GUARD: vol_spike+level_break, {WHIPSAW_GUARD_SEC - elapsed_ws:.0f}s remaining')
            bundle_triggers(surviving)
            return EventResult()
        # 120s elapsed → pass through, reset timer
        _event_bundle['whipsaw_first_ts'] = 0

    # Section 6: bundle triggers within 30s window
    bundled = bundle_triggers(surviving)
    if not bundled:
        _log(f'triggers buffered for bundling: {[t["type"] for t in surviving]}')
        return EventResult()  # still accumulating, return DEFAULT

    eh = compute_event_hash(bundled, symbol=symbol, price=price)
    _log(f'EVENT: triggers={[t["type"] for t in bundled]} hash={eh[:12]}')
    return EventResult(
        mode=MODE_EVENT,
        triggers=bundled,
        event_hash=eh,
        priority=PRIORITY_EVENT,
        call_type='AUTO',
        should_call_claude=True,
    )


def _check_price_spikes(snapshot) -> list:
    """Check ret_1m/5m/15m against thresholds — edge-based (False→True only)."""
    triggers = []
    returns = snapshot.get('returns', {})

    for key, ret_key, threshold in [
        ('price_spike_1m', 'ret_1m', PRICE_SPIKE_1M_PCT),
        ('price_spike_5m', 'ret_5m', PRICE_SPIKE_5M_PCT),
        ('price_spike_15m', 'ret_15m', PRICE_SPIKE_15M_PCT),
    ]:
        ret = returns.get(ret_key)
        now_active = ret is not None and abs(ret) >= threshold
        was_active = _prev_edge_state[key]
        if now_active and not was_active:
            triggers.append({
                'type': key,
                'value': ret,
                'threshold': threshold,
                'direction': 'up' if ret > 0 else 'down',
            })
        _prev_edge_state[key] = now_active

    return triggers


def _check_volume_spike(snapshot) -> list:
    """Check vol_ratio against threshold — edge-based (False→True only)."""
    triggers = []
    vol_ratio = snapshot.get('vol_ratio', 0)
    now_active = vol_ratio >= VOL_SPIKE_RATIO
    was_active = _prev_edge_state['volume_spike']
    if now_active and not was_active:
        triggers.append({
            'type': 'volume_spike',
            'value': round(vol_ratio, 2),
            'threshold': VOL_SPIKE_RATIO,
        })
    _prev_edge_state['volume_spike'] = now_active
    return triggers


VOL_PROFILE_MAX_AGE_SEC = 3600  # 1 hour — skip level_breaks if older

# Module-level state to detect *transitions* (not static positions)
_prev_level_state = {'above_vah': False, 'below_val': False, 'poc_zone': None}


def _check_level_breaks(snapshot, cur=None) -> list:
    """Check POC shift, VAH/VAL breach — only on *transitions*, not static state."""
    global _prev_level_state
    triggers = []
    price = snapshot.get('price', 0)
    if not price:
        return triggers

    # Skip if vol_profile data is stale (>1h)
    vp_ts = snapshot.get('vol_profile_ts')
    if vp_ts:
        age = time.time() - vp_ts
        if age > VOL_PROFILE_MAX_AGE_SEC:
            return triggers
    else:
        return triggers  # no timestamp = unknown freshness, skip

    poc = snapshot.get('poc')
    vah = snapshot.get('vah')
    val = snapshot.get('val')

    # POC zone: detect transition across POC_SHIFT_MIN_PCT boundary
    if poc and poc > 0:
        poc_dist_pct = abs(price - poc) / poc * 100
        current_zone = 'far_above' if price > poc and poc_dist_pct >= POC_SHIFT_MIN_PCT else \
                        'far_below' if price < poc and poc_dist_pct >= POC_SHIFT_MIN_PCT else 'near'
        prev_zone = _prev_level_state.get('poc_zone')
        if current_zone != 'near' and current_zone != prev_zone:
            triggers.append({
                'type': 'poc_shift',
                'value': round(poc_dist_pct, 2),
                'threshold': POC_SHIFT_MIN_PCT,
                'poc': poc,
                'price': price,
                'direction': 'above' if price > poc else 'below',
            })
        _prev_level_state['poc_zone'] = current_zone

    # VAH break: trigger only on first sustained break (transition from below to above)
    candles = snapshot.get('candles_1m', [])
    if vah and len(candles) >= VAH_VAL_SUSTAIN_CANDLES:
        recent = candles[:VAH_VAL_SUSTAIN_CANDLES]
        now_above_vah = all(c.get('c', 0) > vah for c in recent)
        was_above_vah = _prev_level_state.get('above_vah', False)
        if now_above_vah and not was_above_vah:
            triggers.append({
                'type': 'vah_break',
                'value': price,
                'vah': vah,
                'sustained_candles': VAH_VAL_SUSTAIN_CANDLES,
                'vol_spike': snapshot.get('vol_ratio', 0) >= VOL_SPIKE_RATIO,
            })
        _prev_level_state['above_vah'] = now_above_vah

    # VAL break: trigger only on first sustained break (transition from above to below)
    if val and len(candles) >= VAH_VAL_SUSTAIN_CANDLES:
        recent = candles[:VAH_VAL_SUSTAIN_CANDLES]
        now_below_val = all(c.get('c', 0) < val for c in recent)
        was_below_val = _prev_level_state.get('below_val', False)
        if now_below_val and not was_below_val:
            triggers.append({
                'type': 'val_break',
                'value': price,
                'val': val,
                'sustained_candles': VAH_VAL_SUSTAIN_CANDLES,
                'vol_spike': snapshot.get('vol_ratio', 0) >= VOL_SPIKE_RATIO,
            })
        _prev_level_state['below_val'] = now_below_val

    return triggers


def _check_regime_change(snapshot, prev_scores=None) -> list:
    """Check regime score change and ATR increase — edge-based (False→True only)."""
    triggers = []

    now_active = False
    if prev_scores:
        prev_atr = prev_scores.get('atr_14') or prev_scores.get('atr')
        curr_atr = snapshot.get('atr_14')
        if prev_atr and curr_atr and prev_atr > 0:
            atr_change_pct = (curr_atr - prev_atr) / prev_atr * 100
            now_active = atr_change_pct >= ATR_INCREASE_PCT
            was_active = _prev_edge_state['atr_increase']
            if now_active and not was_active:
                triggers.append({
                    'type': 'atr_increase',
                    'value': round(atr_change_pct, 1),
                    'threshold': ATR_INCREASE_PCT,
                    'prev_atr': prev_atr,
                    'curr_atr': curr_atr,
                })
    _prev_edge_state['atr_increase'] = now_active

    return triggers


def _check_3bar_directional(snapshot) -> list:
    """Check if last 3 15m bars are in same direction with cumulative >= 1.2%.
    If True, returns an emergency trigger for AUTO_EMERGENCY."""
    bar_15m = snapshot.get('bar_15m_returns', []) if snapshot else []
    if len(bar_15m) < 3:
        return []
    last3 = bar_15m[:3]
    all_positive = all(b > 0 for b in last3)
    all_negative = all(b < 0 for b in last3)
    if not (all_positive or all_negative):
        return []
    cumulative = abs(sum(last3))
    if cumulative < 1.2:
        return []
    direction = 'up' if all_positive else 'down'
    _log(f'3-bar directional trigger: {direction} cumulative={cumulative:.2f}% bars={last3}')
    return [{
        'type': 'emergency_3bar_directional',
        'value': round(cumulative, 2),
        'threshold': 1.2,
        'direction': direction,
        'bars': last3,
        'position_critical': False,
    }]


def _check_emergency_escalation(snapshot, position=None, prev_scores=None) -> list:
    """Check for emergency-level conditions.

    Signals are split into two groups:
      - Position-critical (loss, liquidation): trigger independently, NO vol_ratio gate
      - Market-critical (price_5m, price_15m, ATR_surge): require vol_ratio >= 2.5x gate

    Additionally, volatility_zscore >= 3.0 triggers emergency independently.
    """
    position_critical_signals = []
    market_critical_signals = []
    returns = snapshot.get('returns', {})
    vol_ratio = snapshot.get('vol_ratio', 0)

    # ── Market-critical signals (need vol_ratio gate) ──

    # Signal 1: 5m return >= 2%
    ret_5m = returns.get('ret_5m')
    if ret_5m is not None and abs(ret_5m) >= EMERGENCY_5M_RET_PCT:
        market_critical_signals.append({
            'type': 'emergency_price_5m',
            'value': ret_5m,
            'threshold': EMERGENCY_5M_RET_PCT,
            'direction': 'up' if ret_5m > 0 else 'down',
            'position_critical': False,
        })

    # Signal 2: 15m return >= 3.5%
    ret_15m = returns.get('ret_15m')
    if ret_15m is not None and abs(ret_15m) >= EMERGENCY_15M_RET_PCT:
        market_critical_signals.append({
            'type': 'emergency_price_15m',
            'value': ret_15m,
            'threshold': EMERGENCY_15M_RET_PCT,
            'direction': 'up' if ret_15m > 0 else 'down',
            'position_critical': False,
        })

    # Signal 5: ATR surge >= 40% vs previous cycle
    if prev_scores:
        prev_atr = prev_scores.get('atr_14') or prev_scores.get('atr')
        curr_atr = snapshot.get('atr_14')
        if prev_atr and curr_atr and prev_atr > 0:
            atr_surge_pct = (curr_atr - prev_atr) / prev_atr * 100
            if atr_surge_pct >= EMERGENCY_ATR_SURGE_PCT:
                market_critical_signals.append({
                    'type': 'emergency_atr_surge',
                    'value': round(atr_surge_pct, 1),
                    'threshold': EMERGENCY_ATR_SURGE_PCT,
                    'prev_atr': prev_atr,
                    'curr_atr': curr_atr,
                    'position_critical': False,
                })

    # ── Position-critical signals (NO vol_ratio gate) ──

    # Signal 3: position unrealized loss >= 2%
    if position:
        entry = position.get('entry_price', 0)
        side = position.get('side', '')
        price = snapshot.get('price', 0)
        if entry and entry > 0 and price > 0 and side:
            if side == 'long':
                loss_pct = (entry - price) / entry * 100
            else:
                loss_pct = (price - entry) / entry * 100
            if loss_pct >= EMERGENCY_LOSS_PCT:
                position_critical_signals.append({
                    'type': 'emergency_position_loss',
                    'value': round(loss_pct, 2),
                    'threshold': EMERGENCY_LOSS_PCT,
                    'side': side,
                    'position_critical': True,
                })

    # Signal 4: liquidation distance <= 3%
    if position:
        liq_price = position.get('liquidation_price', 0)
        price = snapshot.get('price', 0)
        side = position.get('side', '')
        if liq_price and liq_price > 0 and price and price > 0 and side:
            if side == 'long':
                liq_dist_pct = (price - liq_price) / price * 100
            else:
                liq_dist_pct = (liq_price - price) / price * 100
            if liq_dist_pct <= EMERGENCY_LIQ_DIST_PCT:
                position_critical_signals.append({
                    'type': 'emergency_liquidation_near',
                    'value': round(liq_dist_pct, 2),
                    'threshold': EMERGENCY_LIQ_DIST_PCT,
                    'side': side,
                    'position_critical': True,
                })

    # ── 3-bar directional trigger (independent) ──
    bar3_triggers = _check_3bar_directional(snapshot)
    if bar3_triggers:
        market_critical_signals.extend(bar3_triggers)

    # ── Volatility zscore trigger (independent) ──
    vol_zscore_triggered = False
    if prev_scores:
        prev_atr = prev_scores.get('atr_14') or prev_scores.get('atr')
        curr_atr = snapshot.get('atr_14')
        if prev_atr and curr_atr and prev_atr > 0:
            atr_change_ratio = abs(curr_atr - prev_atr) / prev_atr
            if atr_change_ratio >= EMERGENCY_VOL_ZSCORE:
                vol_zscore_triggered = True
                market_critical_signals.append({
                    'type': 'emergency_volatility_zscore',
                    'value': round(atr_change_ratio, 2),
                    'threshold': EMERGENCY_VOL_ZSCORE,
                    'prev_atr': prev_atr,
                    'curr_atr': curr_atr,
                    'position_critical': False,
                })

    # ── Assemble results ──

    # Position-critical signals fire independently (no vol gate)
    if position_critical_signals:
        _log(f'position-critical emergency: signals={[s["type"] for s in position_critical_signals]}')
        return position_critical_signals

    if not market_critical_signals:
        return []

    # Volatility zscore trigger bypasses vol_ratio gate
    if vol_zscore_triggered:
        _log(f'volatility zscore emergency: bypassing vol_ratio gate')
        return market_critical_signals

    # Market-critical signals require vol_ratio >= 2.5x gate
    # During macro event windows (FOMC/CPI/PPI/NFP), lower the gate to 1.5x
    effective_vol_gate = EMERGENCY_VOL_SPIKE_RATIO
    try:
        from macro_collector import is_macro_event_window
        macro_window = is_macro_event_window()
        if macro_window.get('active'):
            effective_vol_gate = 1.5
            _log(f'macro event window active ({macro_window["events"]}) — vol gate lowered to {effective_vol_gate}')
    except Exception:
        pass

    if vol_ratio < effective_vol_gate:
        _log(f'emergency signals found but vol_ratio={vol_ratio:.2f} < {effective_vol_gate} — suppressed '
             f'(signals={[s["type"] for s in market_critical_signals]})')
        return []

    # Add vol confirmation to trigger list
    market_critical_signals.append({
        'type': 'emergency_volume_confirmed',
        'value': round(vol_ratio, 2),
        'threshold': EMERGENCY_VOL_SPIKE_RATIO,
        'position_critical': False,
    })

    return market_critical_signals


def compute_event_hash(triggers, symbol='BTC/USDT:USDT', price=None) -> str:
    """Compute context-aware event hash for dedup.

    hash = f"{symbol}:{trigger_types}:{price_band}:{minute_bucket}"
    - price_band: price rounded to nearest $500
    - minute_bucket: current 5-min bucket
    """
    key_parts = []
    for t in sorted(triggers, key=lambda x: x.get('type', '')):
        part = t.get('type', '')
        direction = t.get('direction', '')
        if direction:
            part += f':{direction}'
        key_parts.append(part)
    types_str = '|'.join(key_parts)
    price_band = int(price / 500) * 500 if price else 0
    minute_bucket = int(time.time() / 900)  # 15-min bucket for hash dedup
    raw = f'{symbol}:{types_str}:{price_band}:{minute_bucket}'
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def set_emergency_lock(symbol='BTC/USDT:USDT'):
    """Set emergency lock for symbol (180s cooldown)."""
    _emergency_lock[symbol] = time.time() + EMERGENCY_LOCK_SEC
    _log(f'emergency lock SET for {symbol} ({EMERGENCY_LOCK_SEC}s)')


def is_emergency_locked(symbol='BTC/USDT:USDT') -> bool:
    """Check if emergency lock is active."""
    now = time.time()
    expire = _emergency_lock.get(symbol, 0)
    if now < expire:
        remaining = int(expire - now)
        _log(f'emergency lock ACTIVE ({remaining}s remaining)')
        return True
    # Purge expired locks
    expired = [k for k, v in _emergency_lock.items() if now >= v]
    for k in expired:
        del _emergency_lock[k]
    return False


def record_emergency_action(symbol, action, direction):
    """Record last emergency action for duplicate prevention."""
    _last_emergency_action[symbol] = {
        'action': action,
        'direction': direction,
        'ts': time.time(),
    }


def is_duplicate_emergency_action(symbol, action, direction) -> bool:
    """Check if same action+direction was already executed within lock window."""
    last = _last_emergency_action.get(symbol)
    if not last:
        return False
    if last['action'] == action and last['direction'] == direction:
        elapsed = time.time() - last['ts']
        if elapsed < EMERGENCY_LOCK_SEC:
            _log(f'duplicate {action} {direction} blocked ({int(EMERGENCY_LOCK_SEC - elapsed)}s remaining)')
            return True
    return False


def check_dedup(event_hash, state) -> bool:
    """Check if event_hash was seen within 15-min dedup window.
    Returns True if duplicate (should skip), False if new.
    """
    if not event_hash:
        return False
    dedup_hashes = state.get('event_hashes', {})
    last_seen = dedup_hashes.get(event_hash, 0)
    now = time.time()
    return (now - last_seen) < EVENT_HASH_DEDUP_SEC
