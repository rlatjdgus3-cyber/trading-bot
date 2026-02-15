"""
claude_gate.py — Single gate for all Claude API calls.

Controls: cooldown, daily/monthly budget, error backoff, prompt compaction.
State persisted in .claude_gate_state.json (same pattern as gpt_router).
Never raises exceptions — always returns a safe response.
"""
import os
import sys
import time
import json

sys.path.insert(0, '/root/trading-bot/app')
LOG_PREFIX = '[claude_gate]'

# ── constants ────────────────────────────────────────────
COOLDOWN_GENERAL_SEC = 1800       # 30 min
COOLDOWN_EMERGENCY_SEC = 600      # 10 min
COOLDOWN_ERROR_SEC = 3600         # 5xx → 1 hour block
DAILY_CALL_LIMIT = 50
DAILY_COST_LIMIT = 5.0            # $5/day
MONTHLY_COST_LIMIT = 50.0         # $50/month
MAX_PROMPT_CHARS = 12000
MAX_SIMILAR_EVENTS = 3
MAX_SCHEDULED_PER_DAY = 2
CLAUDE_MODEL = 'claude-sonnet-4-20250514'

# Call type constants — 3-class system
CALL_TYPE_NORMAL = 'NORMAL'
CALL_TYPE_USER_MANUAL = 'USER_MANUAL'
CALL_TYPE_AUTO_EMERGENCY = 'AUTO_EMERGENCY'
# Backward compatibility aliases
CALL_TYPE_AUTO = CALL_TYPE_NORMAL
CALL_TYPE_USER = CALL_TYPE_USER_MANUAL
CALL_TYPE_EMERGENCY = CALL_TYPE_AUTO_EMERGENCY
VALID_CALL_TYPES = {CALL_TYPE_NORMAL, CALL_TYPE_USER_MANUAL, CALL_TYPE_AUTO_EMERGENCY,
                    'AUTO', 'USER', 'EMERGENCY'}  # accept old names too
EMERGENCY_SPAM_SEC = 60
AUTO_EMERGENCY_DAILY_CAP = 30  # 폭주 방지 (AUTO_EMERGENCY)
AUTO_EMERGENCY_DEDUP_SEC = 60  # 동일 이벤트 60초 dedup

# Sonnet pricing (per token)
INPUT_COST_PER_MTOK = 3.0         # $3 / 1M input tokens
OUTPUT_COST_PER_MTOK = 15.0       # $15 / 1M output tokens

STATE_FILE = '/root/trading-bot/app/.claude_gate_state.json'

# High-impact news keywords
HIGH_NEWS_KEYWORDS = {'SEC', 'ETF', 'Fed', 'FOMC', 'CPI', 'War', 'Trump',
                      'Hack', 'Ban', 'Regulation', 'Binance', 'Tether',
                      'Mt.Gox', 'Halving'}

# Gate definitions
GATE_COOLDOWNS = {
    'emergency': COOLDOWN_EMERGENCY_SEC,
    'pre_action': COOLDOWN_GENERAL_SEC,
    'high_news': COOLDOWN_GENERAL_SEC,
    'scheduled': COOLDOWN_GENERAL_SEC,
    'telegram': COOLDOWN_GENERAL_SEC,
    'openclaw': 120,                      # 2 min — control tower, permissive
    'event_trigger': 180,                 # 3 min — event cooldown (dedup is separate)
}

EVENT_DEDUP_WINDOW_SEC = 300              # 5 min — event hash dedup


def _normalize_call_type(call_type: str) -> str:
    """Normalize legacy call_type names to new 3-class names."""
    mapping = {
        'AUTO': CALL_TYPE_NORMAL,
        'USER': CALL_TYPE_USER_MANUAL,
        'EMERGENCY': CALL_TYPE_AUTO_EMERGENCY,
    }
    return mapping.get(call_type, call_type)


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


# ── state management ─────────────────────────────────────

def _load_state() -> dict:
    try:
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    except Exception:
        return {
            'daily_calls': {},
            'daily_cost': {},
            'monthly_cost': {},
            'cooldowns': {},
            'error_block_until': 0,
            'scheduled_today': {},
            'budget_notified': {},
        }


def _save_state(state: dict):
    state['last_updated'] = time.time()
    tmp = STATE_FILE + '.tmp'
    try:
        with open(tmp, 'w') as f:
            json.dump(state, f, indent=2)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        _log(f'state save error: {e}')


def _today() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime('%Y-%m-%d')


def _this_month() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime('%Y-%m')


# ── gate condition checks ────────────────────────────────

def _check_gate_condition(gate: str, context: dict) -> tuple:
    """Check if the gate condition is met. Returns (ok, reason)."""
    if gate == 'emergency':
        trigger_type = context.get('trigger_type', '')
        zscore_band = context.get('zscore_band', '')
        valid_triggers = ('rapid_price_move', 'volume_spike',
                          'extreme_funding', 'extreme_score')
        if trigger_type in valid_triggers or zscore_band == 'high':
            return (True, 'emergency condition met')
        # Also allow if explicitly flagged
        if context.get('is_emergency'):
            return (True, 'emergency flag set')
        return (True, 'emergency gate (permissive)')

    if gate == 'pre_action':
        action = context.get('candidate_action', '')
        sl_dist = context.get('sl_dist_pct')
        if action in ('CLOSE', 'REVERSE'):
            return (True, f'pre_action: {action}')
        if sl_dist is not None and abs(float(sl_dist)) < 0.3:
            return (True, f'pre_action: SL close ({sl_dist}%)')
        return (True, 'pre_action gate (permissive)')

    if gate == 'high_news':
        impact = context.get('impact_score', 0)
        title = context.get('title', '') or context.get('headline', '')
        has_keyword = any(kw.lower() in title.lower() for kw in HIGH_NEWS_KEYWORDS)
        if impact >= 7 and has_keyword:
            return (True, f'high_news: impact={impact}, keyword match')
        if impact >= 7:
            return (True, f'high_news: impact={impact}')
        if context.get('high_news'):
            return (True, 'high_news: flagged')
        return (True, 'high_news gate (permissive)')

    if gate == 'scheduled':
        return (True, 'scheduled gate')

    if gate == 'telegram':
        intent = context.get('intent', '')
        if intent in ('emergency', 'strategy', 'news'):
            return (True, f'telegram: intent={intent}')
        return (True, 'telegram gate (permissive)')

    if gate == 'openclaw':
        return (True, 'openclaw gate (control tower)')

    if gate == 'event_trigger':
        event_mode = context.get('event_mode', '')
        if event_mode in ('EVENT', 'EMERGENCY'):
            return (True, f'event_trigger: mode={event_mode}')
        return (False, 'event_trigger: no qualifying event')

    return (True, f'unknown gate {gate} (permissive)')


# ── core request/call functions ──────────────────────────

def request(gate: str, cooldown_key: str = '', context: dict = None,
            call_type: str = 'AUTO') -> dict:
    """7-stage gate check. Returns {allowed, reason, gate, budget_remaining, call_type}.
    call_type: AUTO (full control), USER (bypass cooldown+budget), EMERGENCY (bypass + 60s spam).
    Never raises exceptions."""
    if context is None:
        context = {}
    if call_type not in VALID_CALL_TYPES:
        _log(f'invalid call_type={call_type!r}, falling back to AUTO')
        call_type = CALL_TYPE_AUTO
    try:
        return _request_inner(gate, cooldown_key, context, call_type)
    except Exception as e:
        _log(f'request error (call_type={call_type}): {e}')
        return {'allowed': False, 'reason': f'gate error: {e}',
                'gate': gate, 'budget_remaining': {}, 'call_type': call_type}


def _request_inner(gate: str, cooldown_key: str, context: dict,
                   call_type: str = 'AUTO') -> dict:
    call_type = _normalize_call_type(call_type)
    state = _load_state()
    today = _today()
    month = _this_month()
    now = time.time()
    bypass = call_type in (CALL_TYPE_USER_MANUAL, CALL_TYPE_AUTO_EMERGENCY)

    budget_remaining = {}

    # Stage 1: API key check
    api_key = os.getenv('ANTHROPIC_API_KEY', '')
    if not api_key or api_key == 'DISABLED':
        return {'allowed': False, 'reason': 'API key disabled',
                'gate': gate, 'budget_remaining': budget_remaining,
                'call_type': call_type}

    # Stage 2: Error block check
    error_until = state.get('error_block_until', 0)
    if now < error_until:
        remaining = int(error_until - now)
        return {'allowed': False,
                'reason': f'error block active ({remaining}s remaining)',
                'gate': gate, 'budget_remaining': budget_remaining,
                'call_type': call_type}

    # Stage 3: Gate condition check
    ok, reason = _check_gate_condition(gate, context)
    if not ok:
        return {'allowed': False, 'reason': f'gate condition failed: {reason}',
                'gate': gate, 'budget_remaining': budget_remaining,
                'call_type': call_type}

    # Stage 3.5: Event hash dedup (NORMAL only; AUTO_EMERGENCY uses shorter dedup)
    if call_type == CALL_TYPE_NORMAL:
        event_hash = context.get('event_hash')
        if event_hash:
            dedup_hashes = state.get('event_hashes', {})
            last_seen = dedup_hashes.get(event_hash, 0)
            if now - last_seen < EVENT_DEDUP_WINDOW_SEC:
                remaining = int(EVENT_DEDUP_WINDOW_SEC - (now - last_seen))
                return {'allowed': False,
                        'reason': f'event dedup ({remaining}s remaining)',
                        'gate': gate, 'budget_remaining': budget_remaining,
                        'call_type': call_type}

    # Stage 4: Cooldown check — 3-class call_type branching
    if cooldown_key:
        if call_type == CALL_TYPE_NORMAL:
            # Full cooldown for NORMAL
            cooldowns = state.get('cooldowns', {})
            ck = f'{gate}:{cooldown_key}'
            last_call = cooldowns.get(ck, 0)
            cd_sec = GATE_COOLDOWNS.get(gate, COOLDOWN_GENERAL_SEC)
            if now - last_call < cd_sec:
                remaining = int(cd_sec - (now - last_call))
                return {'allowed': False,
                        'reason': f'cooldown active: {ck} ({remaining}s remaining)',
                        'gate': gate, 'budget_remaining': budget_remaining,
                        'call_type': call_type}
        elif call_type == CALL_TYPE_USER_MANUAL:
            # USER_MANUAL: skip cooldown, 2min request cache dedup only
            cooldowns = state.get('cooldowns', {})
            dedup_key = f'user_dedup:{cooldown_key}'
            last_call = cooldowns.get(dedup_key, 0)
            if now - last_call < 120:  # 2min dedup
                remaining = int(120 - (now - last_call))
                _log(f'USER_MANUAL dedup: {dedup_key} ({remaining}s remaining)')
            else:
                _log(f'cooldown SKIPPED (call_type=USER_MANUAL): {gate}:{cooldown_key}')
        elif call_type == CALL_TYPE_AUTO_EMERGENCY:
            # AUTO_EMERGENCY: 60-second spam guard only
            cooldowns = state.get('cooldowns', {})
            spam_key = f'emergency_spam:{cooldown_key}'
            last_call = cooldowns.get(spam_key, 0)
            if now - last_call < AUTO_EMERGENCY_DEDUP_SEC:
                remaining = int(AUTO_EMERGENCY_DEDUP_SEC - (now - last_call))
                return {'allowed': False,
                        'reason': f'emergency spam guard: {spam_key} ({remaining}s remaining)',
                        'gate': gate, 'budget_remaining': budget_remaining,
                        'call_type': call_type}
            _log(f'cooldown REDUCED (call_type=AUTO_EMERGENCY, {AUTO_EMERGENCY_DEDUP_SEC}s spam only): {gate}:{cooldown_key}')

    # Stage 5: Daily budget check
    daily_calls = state.get('daily_calls', {}).get(today, 0)
    daily_cost = state.get('daily_cost', {}).get(today, 0.0)
    budget_remaining['daily_calls'] = DAILY_CALL_LIMIT - daily_calls
    budget_remaining['daily_cost_usd'] = round(DAILY_COST_LIMIT - daily_cost, 4)

    if call_type == CALL_TYPE_NORMAL:
        # NORMAL: strict daily cap + cost limit
        if daily_calls >= DAILY_CALL_LIMIT:
            _notify_budget_exceeded(state, f'daily call limit ({daily_calls}/{DAILY_CALL_LIMIT})')
            _save_state(state)
            return {'allowed': False,
                    'reason': f'daily call limit ({daily_calls}/{DAILY_CALL_LIMIT})',
                    'gate': gate, 'budget_remaining': budget_remaining,
                    'call_type': call_type}
        if daily_cost >= DAILY_COST_LIMIT:
            _notify_budget_exceeded(state, f'daily cost limit (${daily_cost:.2f}/${DAILY_COST_LIMIT})')
            _save_state(state)
            return {'allowed': False,
                    'reason': f'daily cost limit (${daily_cost:.2f}/${DAILY_COST_LIMIT})',
                    'gate': gate, 'budget_remaining': budget_remaining,
                    'call_type': call_type}
    elif call_type == CALL_TYPE_AUTO_EMERGENCY:
        # AUTO_EMERGENCY: bypass daily 50 cap, but has own 30/day cap
        emerg_daily = state.get('call_type_counts', {}).get(today, {}).get(CALL_TYPE_AUTO_EMERGENCY, 0)
        if emerg_daily >= AUTO_EMERGENCY_DAILY_CAP:
            _log(f'AUTO_EMERGENCY daily cap reached ({emerg_daily}/{AUTO_EMERGENCY_DAILY_CAP})')
            return {'allowed': False,
                    'reason': f'AUTO_EMERGENCY daily cap ({emerg_daily}/{AUTO_EMERGENCY_DAILY_CAP})',
                    'gate': gate, 'budget_remaining': budget_remaining,
                    'call_type': call_type}
        _log(f'daily budget BYPASSED (call_type=AUTO_EMERGENCY): daily_calls={daily_calls} emerg={emerg_daily}')
    elif call_type == CALL_TYPE_USER_MANUAL:
        # USER_MANUAL: no cap
        _log(f'budget BYPASSED (call_type=USER_MANUAL): daily_calls={daily_calls} daily_cost=${daily_cost:.4f}')

    # Stage 6: Monthly budget check
    monthly_cost = state.get('monthly_cost', {}).get(month, 0.0)
    budget_remaining['monthly_cost_usd'] = round(MONTHLY_COST_LIMIT - monthly_cost, 4)

    # Monthly budget: enforced for ALL types (including AUTO_EMERGENCY)
    if monthly_cost >= MONTHLY_COST_LIMIT:
        if call_type == CALL_TYPE_USER_MANUAL:
            _log(f'monthly budget WARNING (USER_MANUAL bypass): ${monthly_cost:.4f}/${MONTHLY_COST_LIMIT}')
        else:
            _notify_budget_exceeded(state, f'monthly cost limit (${monthly_cost:.2f}/${MONTHLY_COST_LIMIT})')
            _save_state(state)
            return {'allowed': False,
                    'reason': f'monthly cost limit (${monthly_cost:.2f}/${MONTHLY_COST_LIMIT})',
                    'gate': gate, 'budget_remaining': budget_remaining,
                    'call_type': call_type}

    # Stage 7: Scheduled limit check (only for 'scheduled' gate)
    if gate == 'scheduled':
        if call_type == CALL_TYPE_NORMAL:
            scheduled = state.get('scheduled_today', {}).get(today, 0)
            if scheduled >= MAX_SCHEDULED_PER_DAY:
                return {'allowed': False,
                        'reason': f'scheduled limit ({scheduled}/{MAX_SCHEDULED_PER_DAY})',
                        'gate': gate, 'budget_remaining': budget_remaining,
                        'call_type': call_type}
        else:
            _log(f'scheduled limit BYPASSED (call_type={call_type})')

    return {'allowed': True, 'reason': 'approved',
            'gate': gate, 'budget_remaining': budget_remaining,
            'call_type': call_type}


def call_claude(gate: str, prompt: str, cooldown_key: str = '',
                context: dict = None, max_tokens: int = 800,
                call_type: str = 'AUTO') -> dict:
    """Gate check → API call → record. Returns response dict.
    call_type: AUTO/USER/EMERGENCY controls cooldown/budget bypass.
    On deny: {fallback_used: True, gate_reason: ...}
    Never raises exceptions."""
    if context is None:
        context = {}
    if call_type not in VALID_CALL_TYPES:
        _log(f'call_claude: invalid call_type={call_type!r}, falling back to AUTO')
        call_type = CALL_TYPE_AUTO
    try:
        return _call_claude_inner(gate, prompt, cooldown_key, context, max_tokens,
                                  call_type)
    except Exception as e:
        _log(f'call_claude error (call_type={call_type}): {e}')
        return {'fallback_used': True, 'gate_reason': f'error: {e}',
                'text': '', 'model': CLAUDE_MODEL, 'call_type': call_type}


def _call_claude_inner(gate, prompt, cooldown_key, context, max_tokens,
                       call_type='AUTO'):
    # Gate check
    check = request(gate, cooldown_key, context, call_type=call_type)
    if not check['allowed']:
        caller = (context or {}).get('caller', 'unknown')
        _log(f'DENIED gate={gate} key={cooldown_key} call_type={call_type} caller={caller} reason={check["reason"]}')
        return {'fallback_used': True, 'gate_reason': check['reason'],
                'text': '', 'model': CLAUDE_MODEL,
                'budget_remaining': check.get('budget_remaining', {}),
                'call_type': call_type}

    # Compact prompt
    prompt = compact_prompt(prompt)

    # API call
    api_key = os.getenv('ANTHROPIC_API_KEY', '')
    start_ms = int(time.time() * 1000)

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=max_tokens,
            messages=[{'role': 'user', 'content': prompt}],
        )
        elapsed_ms = int(time.time() * 1000) - start_ms
        text = response.content[0].text if response.content else ''
        input_tokens = response.usage.input_tokens
        output_tokens = response.usage.output_tokens

        # Record successful call
        state = _load_state()
        _record_call_to_state(state, gate, input_tokens, output_tokens,
                              cooldown_key, call_type=call_type, context=context)
        _save_state(state)

        caller = (context or {}).get('caller', 'unknown')
        _log(f'OK gate={gate} key={cooldown_key} call_type={call_type} '
             f'caller={caller} in={input_tokens} out={output_tokens} '
             f'latency={elapsed_ms}ms')

        return {
            'fallback_used': False,
            'text': text,
            'model': CLAUDE_MODEL,
            'input_tokens': input_tokens,
            'output_tokens': output_tokens,
            'estimated_cost_usd': _estimate_cost(input_tokens, output_tokens),
            'api_latency_ms': elapsed_ms,
            'gate_type': gate,
            'call_type': call_type,
        }

    except Exception as e:
        elapsed_ms = int(time.time() * 1000) - start_ms
        _log(f'API error gate={gate} call_type={call_type}: {e}')

        # Check for 5xx / overloaded
        status_code = getattr(e, 'status_code', 0)
        record_error(status_code, str(e))

        return {
            'fallback_used': True,
            'gate_reason': f'api_error: {e}',
            'text': '',
            'model': CLAUDE_MODEL,
            'api_latency_ms': elapsed_ms,
            'gate_type': gate,
            'call_type': call_type,
        }


# ── record keeping ───────────────────────────────────────

def _estimate_cost(input_tokens: int, output_tokens: int) -> float:
    cost = (input_tokens * INPUT_COST_PER_MTOK / 1_000_000
            + output_tokens * OUTPUT_COST_PER_MTOK / 1_000_000)
    return round(cost, 6)


def record_call(gate: str, input_tokens: int, output_tokens: int,
                cooldown_key: str = '', call_type: str = 'AUTO',
                context: dict = None):
    """Public interface. Records a call to state."""
    state = _load_state()
    _record_call_to_state(state, gate, input_tokens, output_tokens,
                          cooldown_key, call_type=call_type, context=context)
    _save_state(state)


def _record_call_to_state(state, gate, input_tokens, output_tokens,
                          cooldown_key, call_type='AUTO', context=None):
    today = _today()
    month = _this_month()
    now = time.time()
    cost = _estimate_cost(input_tokens, output_tokens)

    # Daily calls
    dc = state.setdefault('daily_calls', {})
    dc[today] = dc.get(today, 0) + 1
    # Keep only today
    state['daily_calls'] = {today: dc[today]}

    # Daily cost
    dco = state.setdefault('daily_cost', {})
    dco[today] = round(dco.get(today, 0.0) + cost, 6)
    state['daily_cost'] = {today: dco[today]}

    # Monthly cost
    mc = state.setdefault('monthly_cost', {})
    mc[month] = round(mc.get(month, 0.0) + cost, 6)
    # Keep only current month
    state['monthly_cost'] = {month: mc[month]}

    # Cooldown stamp
    if cooldown_key:
        ck = f'{gate}:{cooldown_key}'
        cds = state.setdefault('cooldowns', {})
        cds[ck] = now
        # EMERGENCY: also record spam guard key
        if call_type == CALL_TYPE_EMERGENCY:
            spam_key = f'emergency_spam:{cooldown_key}'
            cds[spam_key] = now

    # Scheduled count
    if gate == 'scheduled':
        sc = state.setdefault('scheduled_today', {})
        sc[today] = sc.get(today, 0) + 1
        state['scheduled_today'] = {today: sc[today]}

    # Call type daily counter
    ctc = state.setdefault('call_type_counts', {})
    day_ctc = ctc.get(today, {})
    day_ctc[call_type] = day_ctc.get(call_type, 0) + 1
    state['call_type_counts'] = {today: day_ctc}

    # Event hash recording
    event_hash = (context or {}).get('event_hash')
    if event_hash:
        eh = state.setdefault('event_hashes', {})
        eh[event_hash] = now
        # Purge hashes older than 10 minutes
        state['event_hashes'] = {k: v for k, v in eh.items() if now - v < 600}


def record_error(status_code, error_msg: str = ''):
    """Record API error. 5xx/overloaded → 1 hour block."""
    state = _load_state()
    status_code = int(status_code) if status_code else 0
    if status_code >= 500 or 'overloaded' in error_msg.lower():
        block_until = time.time() + COOLDOWN_ERROR_SEC
        state['error_block_until'] = block_until
        _log(f'ERROR BLOCK set: status={status_code} until={block_until}')
    _save_state(state)


# ── prompt compaction ────────────────────────────────────

def compact_prompt(prompt: str) -> str:
    """Trim prompt to MAX_PROMPT_CHARS."""
    if not prompt:
        return ''
    if len(prompt) <= MAX_PROMPT_CHARS:
        return prompt
    _log(f'prompt compacted: {len(prompt)} -> {MAX_PROMPT_CHARS}')
    return prompt[:MAX_PROMPT_CHARS]


def compact_context(context: dict) -> dict:
    """Trim context fields to reduce token usage."""
    if not context:
        return {}
    result = dict(context)

    # Limit similar_events
    for key in ('similar_events', 'fact_similar_events'):
        if key in result and isinstance(result[key], list):
            result[key] = result[key][:MAX_SIMILAR_EVENTS]

    # Limit candles
    if 'candles' in result and isinstance(result['candles'], list):
        result['candles'] = result['candles'][:3]

    # Limit news
    if 'news' in result and isinstance(result['news'], list):
        result['news'] = result['news'][:5]

    return result


# ── daily report ─────────────────────────────────────────

def get_daily_cost_report() -> str:
    """Generate daily cost report string for telegram."""
    import report_formatter
    state = _load_state()
    today = _today()
    month = _this_month()

    daily_calls = state.get('daily_calls', {}).get(today, 0)
    daily_cost = state.get('daily_cost', {}).get(today, 0.0)
    monthly_cost = state.get('monthly_cost', {}).get(month, 0.0)
    error_until = state.get('error_block_until', 0)
    now = time.time()

    # Call type counts
    ctc = state.get('call_type_counts', {}).get(today, {})
    auto_c = ctc.get(CALL_TYPE_AUTO, 0)
    user_c = ctc.get(CALL_TYPE_USER, 0)
    emerg_c = ctc.get(CALL_TYPE_EMERGENCY, 0)

    error_remaining = int(error_until - now) if now < error_until else 0

    return report_formatter.format_daily_cost_report(
        today=today,
        daily_calls=daily_calls, daily_limit=DAILY_CALL_LIMIT,
        daily_cost=daily_cost, daily_cost_limit=DAILY_COST_LIMIT,
        monthly_cost=monthly_cost, monthly_cost_limit=MONTHLY_COST_LIMIT,
        auto_c=auto_c, user_c=user_c, emerg_c=emerg_c,
        error_remaining=error_remaining)


# ── budget notification ──────────────────────────────────

def _notify_budget_exceeded(state: dict, reason: str):
    """Send telegram notification once per day when budget exceeded."""
    today = _today()
    notified = state.get('budget_notified', {})
    if notified.get(today):
        return  # already notified today

    try:
        env = _load_telegram_env()
        token = env.get('TELEGRAM_BOT_TOKEN', '')
        chat_id = env.get('TELEGRAM_ALLOWED_CHAT_ID', '')
        if not token or not chat_id:
            return

        import urllib.parse
        import urllib.request
        import report_formatter
        text = report_formatter.sanitize_telegram_text(
            report_formatter.format_budget_exceeded(reason, get_daily_cost_report()))
        url = f'https://api.telegram.org/bot{token}/sendMessage'
        data = urllib.parse.urlencode({
            'chat_id': chat_id,
            'text': text,
        }).encode('utf-8')
        req = urllib.request.Request(url, data=data, method='POST')
        urllib.request.urlopen(req, timeout=10)

        state.setdefault('budget_notified', {})[today] = True
        _log(f'budget notification sent: {reason}')
    except Exception as e:
        _log(f'budget notification error: {e}')


def _load_telegram_env() -> dict:
    """Load telegram credentials from telegram_cmd.env."""
    env_path = '/root/trading-bot/app/telegram_cmd.env'
    env = {}
    try:
        with open(env_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                k, v = line.split('=', 1)
                env[k.strip()] = v.strip()
    except Exception:
        pass
    return env
