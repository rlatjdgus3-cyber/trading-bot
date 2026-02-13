"""
event_trigger.py — Event detection engine.

Snapshot-based event evaluation to decide if/how Claude should be called.
Replaces the old stage-based routing (HOLD/ADD → GPT, CLOSE/REVERSE → Claude).

Modes:
  DEFAULT    — score_engine only, no AI call
  EVENT      — anomaly detected → Claude (AUTO)
  EMERGENCY  — critical condition → Claude forced
  USER       — /force command → Claude forced
"""
import hashlib
import json
import sys
import time

sys.path.insert(0, '/root/trading-bot/app')

LOG_PREFIX = '[event_trigger]'

# ── modes ──────────────────────────────────────────────────
MODE_DEFAULT = 'DEFAULT'
MODE_EVENT = 'EVENT'
MODE_EMERGENCY = 'EMERGENCY'
MODE_USER = 'USER'

# ── price spike thresholds (return %) ──────────────────────
PRICE_SPIKE_1M_PCT = 0.8
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
EMERGENCY_5M_RET_PCT = 2.0
EMERGENCY_15M_RET_PCT = 3.5
EMERGENCY_LOSS_PCT = 2.0
EMERGENCY_LIQ_DIST_PCT = 3.0
EMERGENCY_ATR_SURGE_PCT = 40
EMERGENCY_VOL_SPIKE_RATIO = 2.5

# ── box range filter (EMERGENCY suppression) ─────────────
BOX_BB_BANDWIDTH_PCT = 0.6
BOX_RET_5M_SUPPRESS_PCT = 1.0

# ── dedup ──────────────────────────────────────────────────
AUTO_DEDUP_WINDOW_SEC = 300    # 5 min
EMERGENCY_LOCK_SEC = 180       # 3 min lock after emergency execution
MIN_ORDER_QTY_BTC = 0.001     # Bybit BTC/USDT:USDT minimum

# ── emergency lock state ──────────────────────────────────
_emergency_lock = {}           # {symbol: expire_timestamp}
_last_emergency_action = {}    # {symbol: {'action': str, 'direction': str, 'ts': float}}

# ── per-type EVENT dedup (5 min) ─────────────────────────
_last_trigger_ts = {}          # {trigger_type: timestamp}

# ── priority ───────────────────────────────────────────────
PRIORITY_EMERGENCY = 1
PRIORITY_USER = 2
PRIORITY_EVENT = 3
PRIORITY_DEFAULT = 10


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


class EventResult:
    """Result of event evaluation."""

    __slots__ = ('mode', 'triggers', 'event_hash', 'priority',
                 'call_type', 'should_call_claude')

    def __init__(self, mode=MODE_DEFAULT, triggers=None, event_hash=None,
                 priority=PRIORITY_DEFAULT, call_type='AUTO',
                 should_call_claude=False):
        self.mode = mode
        self.triggers = triggers or []
        self.event_hash = event_hash
        self.priority = priority
        self.call_type = call_type
        self.should_call_claude = should_call_claude

    def to_dict(self):
        return {
            'mode': self.mode,
            'triggers': self.triggers,
            'event_hash': self.event_hash,
            'priority': self.priority,
            'call_type': self.call_type,
            'should_call_claude': self.should_call_claude,
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

        eh = compute_event_hash(emergency_triggers, symbol=symbol, price=price)
        _log(f'EMERGENCY: triggers={[t["type"] for t in emergency_triggers]} hash={eh[:12]}')
        return EventResult(
            mode=MODE_EMERGENCY,
            triggers=emergency_triggers,
            event_hash=eh,
            priority=PRIORITY_EMERGENCY,
            call_type='EMERGENCY',
            should_call_claude=True,
        )

    # Phase 2-5: event triggers
    all_triggers = []
    all_triggers.extend(_check_price_spikes(snapshot))
    all_triggers.extend(_check_volume_spike(snapshot))
    all_triggers.extend(_check_level_breaks(snapshot, cur))
    all_triggers.extend(_check_regime_change(snapshot, prev_scores))

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

    # Update timestamps for surviving triggers only
    for t in surviving:
        _last_trigger_ts[t.get('type', '')] = now

    eh = compute_event_hash(surviving, symbol=symbol, price=price)
    _log(f'EVENT: triggers={[t["type"] for t in surviving]} hash={eh[:12]}')
    return EventResult(
        mode=MODE_EVENT,
        triggers=surviving,
        event_hash=eh,
        priority=PRIORITY_EVENT,
        call_type='AUTO',
        should_call_claude=True,
    )


def _check_price_spikes(snapshot) -> list:
    """Check ret_1m/5m/15m against thresholds."""
    triggers = []
    returns = snapshot.get('returns', {})

    ret_1m = returns.get('ret_1m')
    if ret_1m is not None and abs(ret_1m) >= PRICE_SPIKE_1M_PCT:
        triggers.append({
            'type': 'price_spike_1m',
            'value': ret_1m,
            'threshold': PRICE_SPIKE_1M_PCT,
            'direction': 'up' if ret_1m > 0 else 'down',
        })

    ret_5m = returns.get('ret_5m')
    if ret_5m is not None and abs(ret_5m) >= PRICE_SPIKE_5M_PCT:
        triggers.append({
            'type': 'price_spike_5m',
            'value': ret_5m,
            'threshold': PRICE_SPIKE_5M_PCT,
            'direction': 'up' if ret_5m > 0 else 'down',
        })

    ret_15m = returns.get('ret_15m')
    if ret_15m is not None and abs(ret_15m) >= PRICE_SPIKE_15M_PCT:
        triggers.append({
            'type': 'price_spike_15m',
            'value': ret_15m,
            'threshold': PRICE_SPIKE_15M_PCT,
            'direction': 'up' if ret_15m > 0 else 'down',
        })

    return triggers


def _check_volume_spike(snapshot) -> list:
    """Check vol_ratio against threshold."""
    triggers = []
    vol_ratio = snapshot.get('vol_ratio', 0)
    if vol_ratio >= VOL_SPIKE_RATIO:
        triggers.append({
            'type': 'volume_spike',
            'value': round(vol_ratio, 2),
            'threshold': VOL_SPIKE_RATIO,
        })
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
            })
        _prev_level_state['below_val'] = now_below_val

    return triggers


def _check_regime_change(snapshot, prev_scores=None) -> list:
    """Check regime score change and ATR increase."""
    triggers = []

    if prev_scores:
        prev_regime = prev_scores.get('regime_score', 0)
        # We can't get current regime from snapshot alone; caller passes prev_scores
        # The actual current scores will be available in context
        prev_total = prev_scores.get('total_score', 0)

        # ATR increase check
        prev_atr = prev_scores.get('atr_14') or prev_scores.get('atr')
        curr_atr = snapshot.get('atr_14')
        if prev_atr and curr_atr and prev_atr > 0:
            atr_change_pct = (curr_atr - prev_atr) / prev_atr * 100
            if atr_change_pct >= ATR_INCREASE_PCT:
                triggers.append({
                    'type': 'atr_increase',
                    'value': round(atr_change_pct, 1),
                    'threshold': ATR_INCREASE_PCT,
                    'prev_atr': prev_atr,
                    'curr_atr': curr_atr,
                })

    return triggers


def _check_emergency_escalation(snapshot, position=None, prev_scores=None) -> list:
    """Check for emergency-level conditions.

    Logic: at least 1 signal condition must be met AND vol_ratio >= 2.5x (mandatory gate).
    If signals exist but vol_ratio is insufficient, suppress and return empty.
    """
    signals = []
    returns = snapshot.get('returns', {})
    vol_ratio = snapshot.get('vol_ratio', 0)

    # Signal 1: 5m return >= 2%
    ret_5m = returns.get('ret_5m')
    if ret_5m is not None and abs(ret_5m) >= EMERGENCY_5M_RET_PCT:
        signals.append({
            'type': 'emergency_price_5m',
            'value': ret_5m,
            'threshold': EMERGENCY_5M_RET_PCT,
            'direction': 'up' if ret_5m > 0 else 'down',
        })

    # Signal 2: 15m return >= 3.5%
    ret_15m = returns.get('ret_15m')
    if ret_15m is not None and abs(ret_15m) >= EMERGENCY_15M_RET_PCT:
        signals.append({
            'type': 'emergency_price_15m',
            'value': ret_15m,
            'threshold': EMERGENCY_15M_RET_PCT,
            'direction': 'up' if ret_15m > 0 else 'down',
        })

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
                signals.append({
                    'type': 'emergency_position_loss',
                    'value': round(loss_pct, 2),
                    'threshold': EMERGENCY_LOSS_PCT,
                    'side': side,
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
                signals.append({
                    'type': 'emergency_liquidation_near',
                    'value': round(liq_dist_pct, 2),
                    'threshold': EMERGENCY_LIQ_DIST_PCT,
                    'side': side,
                })

    # Signal 5: ATR surge >= 40% vs previous cycle
    if prev_scores:
        prev_atr = prev_scores.get('atr_14') or prev_scores.get('atr')
        curr_atr = snapshot.get('atr_14')
        if prev_atr and curr_atr and prev_atr > 0:
            atr_surge_pct = (curr_atr - prev_atr) / prev_atr * 100
            if atr_surge_pct >= EMERGENCY_ATR_SURGE_PCT:
                signals.append({
                    'type': 'emergency_atr_surge',
                    'value': round(atr_surge_pct, 1),
                    'threshold': EMERGENCY_ATR_SURGE_PCT,
                    'prev_atr': prev_atr,
                    'curr_atr': curr_atr,
                })

    if not signals:
        return []

    # Mandatory gate: vol_ratio must be >= 2.5x
    if vol_ratio < EMERGENCY_VOL_SPIKE_RATIO:
        _log(f'emergency signals found but vol_ratio={vol_ratio:.2f} < {EMERGENCY_VOL_SPIKE_RATIO} — suppressed '
             f'(signals={[s["type"] for s in signals]})')
        return []

    # Add vol confirmation to trigger list
    signals.append({
        'type': 'emergency_volume_confirmed',
        'value': round(vol_ratio, 2),
        'threshold': EMERGENCY_VOL_SPIKE_RATIO,
    })

    return signals


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
    minute_bucket = int(time.time() / 300)
    raw = f'{symbol}:{types_str}:{price_band}:{minute_bucket}'
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def set_emergency_lock(symbol='BTC/USDT:USDT'):
    """Set emergency lock for symbol (180s cooldown)."""
    _emergency_lock[symbol] = time.time() + EMERGENCY_LOCK_SEC
    _log(f'emergency lock SET for {symbol} ({EMERGENCY_LOCK_SEC}s)')


def is_emergency_locked(symbol='BTC/USDT:USDT') -> bool:
    """Check if emergency lock is active."""
    expire = _emergency_lock.get(symbol, 0)
    if time.time() < expire:
        remaining = int(expire - time.time())
        _log(f'emergency lock ACTIVE ({remaining}s remaining)')
        return True
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
    """Check if event_hash was seen within dedup window.
    Returns True if duplicate (should skip), False if new.
    """
    if not event_hash:
        return False
    dedup_hashes = state.get('event_hashes', {})
    last_seen = dedup_hashes.get(event_hash, 0)
    now = time.time()
    return (now - last_seen) < AUTO_DEDUP_WINDOW_SEC
