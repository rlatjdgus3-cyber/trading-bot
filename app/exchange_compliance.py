"""
exchange_compliance.py — Exchange Compliance Layer (ECL).

Pre-order validation + Bybit error mapping.

Principle: Exchange rules > Risk rules > Strategy logic
Even EMERGENCY mode must comply with exchange rules.

Checks:
  A) minQty / minNotional
  B) stepSize alignment
  C) tickSize alignment
  D) leverage / margin mode consistency
  E) rate limit protection
  F) reduce-only logic validation

Post-order:
  - Bybit error code -> structured Korean message mapping
  - Auto-correction (stepSize/tickSize alignment) with 1 retry
  - Consecutive error tracking -> 5 min auto-block
"""
import hashlib
import math
import sys
import time
import json

sys.path.insert(0, '/root/trading-bot/app')

LOG_PREFIX = '[ecl]'
SYMBOL = 'BTC/USDT:USDT'

# Rate limit: minimum seconds between orders to same symbol
RATE_LIMIT_SEC = 1.0
# Consecutive error threshold for auto-block
CONSECUTIVE_ERROR_THRESHOLD = 3
CONSECUTIVE_ERROR_BLOCK_SEC = 300  # 5 min


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


# ── Market info cache ─────────────────────────────────────

_market_info_cache = {}
_market_info_ts = 0
MARKET_INFO_TTL_SEC = 600  # refresh every 10 min
_markets_version = 0       # incremented on each refresh
_markets_hash = ''         # hash of key market params for change detection


# Error codes that trigger immediate market info refresh
REFRESH_TRIGGER_CODES = {
    10001,   # minQty
    10004,   # stepSize
    10003,   # tickSize
    130021,  # position mode mismatch
    130074,  # leverage limit
    10006,   # rate limit (too many requests)
}


def _compute_markets_hash(info):
    """Compute a hash of key market parameters for change detection."""
    raw = f"{info['minQty']}|{info['stepSize']}|{info['tickSize']}|{info['minNotional']}|{info['maxQty']}"
    return hashlib.md5(raw.encode()).hexdigest()[:12]


def _load_market_info(exchange, symbol=None):
    """Load and cache market info (minQty, stepSize, tickSize, etc.) from exchange."""
    global _market_info_cache, _market_info_ts, _markets_version, _markets_hash
    sym = symbol or SYMBOL
    now = time.time()

    if sym in _market_info_cache and (now - _market_info_ts) < MARKET_INFO_TTL_SEC:
        return _market_info_cache[sym]

    try:
        if not exchange.markets:
            exchange.load_markets()
        market = exchange.market(sym)
        limits = market.get('limits', {})
        precision = market.get('precision', {})

        # Extract from ccxt market structure
        amount_limits = limits.get('amount', {})
        price_limits = limits.get('price', {})
        cost_limits = limits.get('cost', {})

        info = {
            'minQty': float(amount_limits.get('min') or 0.001),
            'maxQty': float(amount_limits.get('max') or 100),
            'stepSize': float(precision.get('amount') or 0.001),
            'minPrice': float(price_limits.get('min') or 0.5),
            'maxPrice': float(price_limits.get('max') or 999999),
            'tickSize': float(precision.get('price') or 0.1),
            'minNotional': float(cost_limits.get('min') or 5),
            'contractSize': float(market.get('contractSize') or 1),
        }

        new_hash = _compute_markets_hash(info)
        if _markets_hash and new_hash != _markets_hash:
            _log(f'MARKET RULES CHANGED: hash {_markets_hash} -> {new_hash}')
        _markets_hash = new_hash
        _markets_version += 1

        _market_info_cache[sym] = info
        _market_info_ts = now
        _log(f'market info loaded (v{_markets_version}): minQty={info["minQty"]} '
             f'stepSize={info["stepSize"]} tickSize={info["tickSize"]} '
             f'minNotional={info["minNotional"]} hash={_markets_hash}')
        return info

    except Exception as e:
        _log(f'market info load error: {e}')
        # Fallback defaults for BTC/USDT:USDT on Bybit
        fallback = {
            'minQty': 0.001,
            'maxQty': 100,
            'stepSize': 0.001,
            'minPrice': 0.5,
            'maxPrice': 999999,
            'tickSize': 0.1,
            'minNotional': 5.0,
            'contractSize': 1,
        }
        _market_info_cache[sym] = fallback
        _market_info_ts = now
        return fallback


def force_refresh_market_info(exchange, symbol=None, reason=''):
    """Force immediate market info refresh (e.g. after specific errors).
    Returns updated market info dict.
    """
    global _market_info_cache, _market_info_ts
    sym = symbol or SYMBOL
    _market_info_cache.pop(sym, None)
    _market_info_ts = 0
    _log(f'FORCE REFRESH market info: {reason or "manual"}')
    # Reload markets from exchange
    try:
        exchange.load_markets(True)  # reload=True
    except Exception as e:
        _log(f'force reload markets error: {e}')
    return _load_market_info(exchange, sym)


def should_refresh_on_error(error_code):
    """Check if this error code should trigger immediate market info refresh."""
    return error_code in REFRESH_TRIGGER_CODES


def get_markets_version():
    """Return current markets version and hash."""
    return _markets_version, _markets_hash


def get_market_info(exchange, symbol=None):
    """Public accessor for market info."""
    return _load_market_info(exchange, symbol)


# ── Alignment helpers ─────────────────────────────────────

def align_qty(qty, step_size):
    """Align quantity to stepSize (round DOWN to nearest step)."""
    if step_size <= 0:
        return qty
    # Use decimal precision to avoid float artifacts
    decimals = _step_decimals(step_size)
    aligned = math.floor(qty / step_size) * step_size
    return round(aligned, decimals)


def align_price(price, tick_size):
    """Align price to tickSize (round to nearest tick)."""
    if tick_size <= 0:
        return price
    decimals = _step_decimals(tick_size)
    aligned = round(price / tick_size) * tick_size
    return round(aligned, decimals)


def _step_decimals(step):
    """Count decimal places in a step value."""
    s = f'{step:.10f}'.rstrip('0')
    if '.' in s:
        return len(s.split('.')[1])
    return 0


# ── Rate limit tracking ──────────────────────────────────

_last_order_ts = {}  # {symbol: timestamp}
_last_order_price = {}  # {symbol: (price, side, ts)} for cancel/reorder detection


def check_rate_limit(symbol=None):
    """Check if rate limit allows a new order. Returns (ok, reason)."""
    sym = symbol or SYMBOL
    now = time.time()
    last = _last_order_ts.get(sym, 0)
    elapsed = now - last
    if elapsed < RATE_LIMIT_SEC:
        return (False, f'rate limit: {elapsed:.1f}s < {RATE_LIMIT_SEC}s minimum')
    return (True, 'ok')


def record_order_sent(symbol=None, price=None, side=None):
    """Record that an order was sent (for rate limit tracking)."""
    sym = symbol or SYMBOL
    now = time.time()
    _last_order_ts[sym] = now
    if price and side:
        _last_order_price[sym] = (price, side, now)


def check_duplicate_price_order(symbol, price, side):
    """Check if same price+side was just sent (cancel/reorder detection).
    Returns (ok, reason).
    """
    sym = symbol or SYMBOL
    last = _last_order_price.get(sym)
    if not last:
        return (True, 'ok')
    last_price, last_side, last_ts = last
    if last_price == price and last_side == side:
        elapsed = time.time() - last_ts
        if elapsed < 5.0:  # within 5 seconds
            return (False, f'duplicate price order: {side} @ {price} sent {elapsed:.1f}s ago')
    return (True, 'ok')


# ── Consecutive error tracking ────────────────────────────

_consecutive_errors = {}  # {symbol: {'count': int, 'last_ts': float, 'blocked_until': float}}


def _check_error_block(symbol=None):
    """Check if symbol is blocked due to consecutive errors. Returns (ok, reason)."""
    sym = symbol or SYMBOL
    state = _consecutive_errors.get(sym)
    if not state:
        return (True, 'ok')
    blocked_until = state.get('blocked_until', 0)
    now = time.time()
    if now < blocked_until:
        remaining = int(blocked_until - now)
        return (False, f'auto-blocked: {state["count"]} consecutive errors '
                       f'({remaining}s remaining)')
    # Block expired, reset
    if blocked_until > 0 and now >= blocked_until:
        _consecutive_errors.pop(sym, None)
    return (True, 'ok')


def record_error(symbol=None, error_code=0):
    """Record an exchange error for consecutive error tracking + protection mode."""
    sym = symbol or SYMBOL
    state = _consecutive_errors.get(sym, {'count': 0, 'last_ts': 0, 'blocked_until': 0})
    state['count'] += 1
    state['last_ts'] = time.time()
    if state['count'] >= CONSECUTIVE_ERROR_THRESHOLD:
        state['blocked_until'] = time.time() + CONSECUTIVE_ERROR_BLOCK_SEC
        _log(f'AUTO-BLOCK: {sym} blocked for {CONSECUTIVE_ERROR_BLOCK_SEC}s '
             f'after {state["count"]} consecutive errors')
    _consecutive_errors[sym] = state
    # Feed protection mode pattern detector
    if error_code:
        _record_protection_error(error_code)


def record_success(symbol=None):
    """Reset consecutive error count on success."""
    sym = symbol or SYMBOL
    _consecutive_errors.pop(sym, None)


# ── Protection Mode ──────────────────────────────────────
# When activated: block OPEN/ADD, allow REDUCE/CLOSE

PROTECTION_MODE_WINDOW_SEC = 120   # look-back window for error pattern
PROTECTION_MODE_THRESHOLD = 3     # errors in window → activate
PROTECTION_MODE_DURATION_SEC = 300  # 5 min block

_protection_mode = {
    'active': False,
    'activated_at': 0,
    'expires_at': 0,
    'reason': '',
    'error_history': [],  # list of (timestamp, error_code) tuples
}


def _check_protection_mode():
    """Check if protection mode is currently active. Returns (active, reason)."""
    now = time.time()
    if _protection_mode['active']:
        if now >= _protection_mode['expires_at']:
            _protection_mode['active'] = False
            _protection_mode['reason'] = ''
            _log('PROTECTION MODE expired — normal operations resumed')
            return (False, '')
        remaining = int(_protection_mode['expires_at'] - now)
        return (True, f"보호 모드 활성: {_protection_mode['reason']} ({remaining}s 남음)")
    return (False, '')


def _record_protection_error(error_code):
    """Record an error for protection mode pattern detection."""
    now = time.time()
    _protection_mode['error_history'].append((now, error_code))
    # Clean old entries
    cutoff = now - PROTECTION_MODE_WINDOW_SEC
    _protection_mode['error_history'] = [
        (ts, code) for ts, code in _protection_mode['error_history']
        if ts > cutoff
    ]
    # Check if threshold exceeded
    if len(_protection_mode['error_history']) >= PROTECTION_MODE_THRESHOLD:
        _activate_protection_mode()


def _activate_protection_mode():
    """Activate protection mode — block OPEN/ADD, allow REDUCE/CLOSE."""
    now = time.time()
    recent_codes = [code for _, code in _protection_mode['error_history'][-5:]]
    reason = f"연속 에러 감지: {recent_codes}"
    _protection_mode['active'] = True
    _protection_mode['activated_at'] = now
    _protection_mode['expires_at'] = now + PROTECTION_MODE_DURATION_SEC
    _protection_mode['reason'] = reason
    _log(f'PROTECTION MODE ACTIVATED: {reason} — OPEN/ADD 차단, REDUCE/CLOSE 허용')


def check_protection_mode_for_action(action_type):
    """Check if protection mode blocks this action type.
    OPEN/ADD → blocked; REDUCE/CLOSE/FULL_CLOSE/REVERSE_CLOSE → allowed.
    Returns (allowed, reason).
    """
    active, reason = _check_protection_mode()
    if not active:
        return (True, 'ok')
    # Allow risk-reducing actions
    if action_type in ('CLOSE', 'FULL_CLOSE', 'REDUCE', 'REVERSE_CLOSE'):
        _log(f'protection mode: {action_type} ALLOWED (risk-reducing)')
        return (True, 'ok')
    # Block risk-increasing actions
    return (False, f'보호 모드 — {action_type} 차단: {reason}')


def get_protection_mode_status():
    """Get current protection mode status for reporting."""
    active, reason = _check_protection_mode()
    return {
        'active': active,
        'reason': reason,
        'activated_at': _protection_mode['activated_at'],
        'expires_at': _protection_mode['expires_at'],
        'recent_errors': _protection_mode['error_history'][-10:],
    }


def format_protection_mode_report():
    """Format protection mode report for Telegram/OpenClaw."""
    status = get_protection_mode_status()
    if not status['active']:
        return None
    recent = status['recent_errors']
    error_summary = {}
    for _, code in recent:
        mapped = BYBIT_ERROR_MAP.get(code, _DEFAULT_ERROR)
        key = mapped['korean_message']
        error_summary[key] = error_summary.get(key, 0) + 1

    lines = [
        '🛡️ 보호 모드 자동 보고',
        f'원인: {status["reason"]}',
        '',
        '최근 오류:',
    ]
    for msg, cnt in error_summary.items():
        lines.append(f'  • {msg}: {cnt}건')
    lines.append(f'\n⏱️ 자동 해제까지 {int(status["expires_at"] - time.time())}초')
    lines.append('ℹ️ REDUCE/CLOSE는 계속 허용됩니다.')
    return '\n'.join(lines)


# ── Pre-order compliance validation ──────────────────────

class ComplianceResult:
    """Result of compliance validation."""
    __slots__ = ('ok', 'reason', 'corrected_qty', 'corrected_price',
                 'was_corrected', 'reject_reason', 'suggested_fix')

    def __init__(self, ok=True, reason='ok', corrected_qty=None,
                 corrected_price=None, was_corrected=False,
                 reject_reason=None, suggested_fix=None):
        self.ok = ok
        self.reason = reason
        self.corrected_qty = corrected_qty
        self.corrected_price = corrected_price
        self.was_corrected = was_corrected
        self.reject_reason = reject_reason
        self.suggested_fix = suggested_fix

    def to_dict(self):
        return {
            'ok': self.ok,
            'reason': self.reason,
            'corrected_qty': self.corrected_qty,
            'corrected_price': self.corrected_price,
            'was_corrected': self.was_corrected,
            'reject_reason': self.reject_reason,
            'suggested_fix': self.suggested_fix,
        }


def validate_bybit_compliance(exchange, order_params, symbol=None):
    """Main compliance validation entry point.

    order_params: {
        'action': 'BUY'|'SELL',
        'qty': float,            # in BTC (or contracts)
        'price': float|None,     # None for market orders
        'side': 'long'|'short',
        'reduce_only': bool,
        'order_type': 'market'|'limit',
        'position_qty': float,   # current position qty (for reduce-only check)
        'usdt_value': float,     # notional value
    }

    Returns ComplianceResult.
    """
    sym = symbol or SYMBOL
    info = _load_market_info(exchange, sym)

    qty = order_params.get('qty', 0)
    price = order_params.get('price')
    reduce_only = order_params.get('reduce_only', False)
    position_qty = order_params.get('position_qty', 0)
    usdt_value = order_params.get('usdt_value', 0)

    corrected_qty = qty
    corrected_price = price
    was_corrected = False

    # ── E) Rate limit check ──
    ok, reason = check_rate_limit(sym)
    if not ok:
        return ComplianceResult(
            ok=False, reason=reason,
            reject_reason='API 호출 과다 (Rate Limit)',
            suggested_fix='1초 이상 간격을 두고 재시도')

    # ── Error block check ──
    ok, reason = _check_error_block(sym)
    if not ok:
        return ComplianceResult(
            ok=False, reason=reason,
            reject_reason='연속 에러로 자동 차단',
            suggested_fix=f'{CONSECUTIVE_ERROR_BLOCK_SEC}초 후 자동 해제')

    # ── B) stepSize alignment ──
    step_size = info['stepSize']
    aligned_qty = align_qty(corrected_qty, step_size)
    if aligned_qty != corrected_qty:
        _log(f'qty aligned: {corrected_qty} -> {aligned_qty} (stepSize={step_size})')
        corrected_qty = aligned_qty
        was_corrected = True

    # ── A) minQty check ──
    min_qty = info['minQty']
    if corrected_qty < min_qty:
        return ComplianceResult(
            ok=False,
            reason=f'qty {corrected_qty} < minQty {min_qty}',
            reject_reason='최소 주문 수량 미달',
            suggested_fix=f'최소 {min_qty} BTC 이상으로 조정 필요')

    # ── A) minNotional check ──
    min_notional = info['minNotional']
    if usdt_value > 0 and usdt_value < min_notional:
        return ComplianceResult(
            ok=False,
            reason=f'notional {usdt_value:.2f} < minNotional {min_notional}',
            reject_reason='주문 금액(minNotional) 미달',
            suggested_fix=f'최소 {min_notional} USDT 이상으로 조정 필요')

    # ── C) tickSize alignment (for limit orders) ──
    if corrected_price is not None and corrected_price > 0:
        tick_size = info['tickSize']
        aligned_price = align_price(corrected_price, tick_size)
        if aligned_price != corrected_price:
            _log(f'price aligned: {corrected_price} -> {aligned_price} (tickSize={tick_size})')
            corrected_price = aligned_price
            was_corrected = True

    # ── F) reduce-only logic ──
    if reduce_only and position_qty > 0:
        if corrected_qty > position_qty:
            # Try to auto-correct to position qty
            corrected_qty = align_qty(position_qty, step_size)
            was_corrected = True
            _log(f'reduce-only qty capped to position qty: {corrected_qty}')
            if corrected_qty < min_qty:
                return ComplianceResult(
                    ok=False,
                    reason=f'reduce qty {corrected_qty} < minQty after capping to position',
                    reject_reason='리듀스 전용 조건 위반',
                    suggested_fix='현재 보유 수량이 최소 주문 단위 미만')

    if was_corrected:
        _log(f'compliance corrected: qty={corrected_qty} price={corrected_price}')

    return ComplianceResult(
        ok=True, reason='compliance passed',
        corrected_qty=corrected_qty,
        corrected_price=corrected_price,
        was_corrected=was_corrected)


def validate_leverage_margin(exchange, symbol=None, required_margin=0):
    """Check leverage/margin mode consistency.
    Returns (ok, reason, details).
    """
    sym = symbol or SYMBOL
    try:
        positions = exchange.fetch_positions([sym])
        for p in positions:
            if p.get('symbol') != sym:
                continue
            margin_mode = p.get('marginMode', 'cross')
            leverage = float(p.get('leverage') or 1)
            # Check available margin if we know
            initial_margin = float(p.get('initialMargin') or 0)
            maintenance_margin = float(p.get('maintenanceMargin') or 0)
            return (True, 'ok', {
                'margin_mode': margin_mode,
                'leverage': leverage,
                'initial_margin': initial_margin,
                'maintenance_margin': maintenance_margin,
            })
        return (True, 'ok', {'margin_mode': 'unknown', 'leverage': 0})
    except Exception as e:
        return (False, f'leverage/margin check failed: {e}', {})


# ── Bybit error code mapping ─────────────────────────────

BYBIT_ERROR_MAP = {
    10001: {
        'category': 'ORDER_SIZE',
        'severity': 'HIGH',
        'korean_message': '최소 주문 수량 미달',
        'reason_detail': '주문 수량이 거래소 최소 요구량보다 작음',
        'suggested_fix': '최소 0.001 BTC 이상으로 조정 필요',
    },
    10002: {
        'category': 'ORDER_SIZE',
        'severity': 'HIGH',
        'korean_message': '주문 금액(minNotional) 미달',
        'reason_detail': '주문의 총 가치가 최소 요구 금액 미만',
        'suggested_fix': '주문 금액을 최소 5 USDT 이상으로 조정',
    },
    10003: {
        'category': 'PRICE_FORMAT',
        'severity': 'MEDIUM',
        'korean_message': '가격 단위(tickSize) 오류',
        'reason_detail': '주문 가격이 tickSize 배수가 아님',
        'suggested_fix': '가격을 tickSize 단위로 정렬 후 재시도',
    },
    10004: {
        'category': 'QTY_FORMAT',
        'severity': 'MEDIUM',
        'korean_message': '수량 단위(stepSize) 오류',
        'reason_detail': '주문 수량이 stepSize 배수가 아님',
        'suggested_fix': '수량을 stepSize 단위로 정렬 후 재시도',
    },
    10006: {
        'category': 'RATE_LIMIT',
        'severity': 'HIGH',
        'korean_message': 'API 호출 과다 (Rate Limit)',
        'reason_detail': '초당 API 호출 한도 초과',
        'suggested_fix': '잠시 대기 후 재시도 (최소 1초 간격)',
    },
    110001: {
        'category': 'MARGIN',
        'severity': 'CRITICAL',
        'korean_message': '증거금 부족',
        'reason_detail': '주문 실행에 필요한 증거금이 부족',
        'suggested_fix': '포지션 크기 축소 또는 추가 증거금 입금 필요',
    },
    110043: {
        'category': 'POSITION_LOGIC',
        'severity': 'HIGH',
        'korean_message': '리듀스 전용 조건 위반',
        'reason_detail': '현재 보유 수량보다 많은 reduce 주문',
        'suggested_fix': 'reduce 수량을 보유 수량 이하로 조정',
    },
    130021: {
        'category': 'POSITION_MODE',
        'severity': 'HIGH',
        'korean_message': '포지션 모드 불일치',
        'reason_detail': 'isolated/cross 모드 설정이 현재 포지션과 충돌',
        'suggested_fix': '포지션 모드 확인 후 일치시킨 뒤 재시도',
    },
    130074: {
        'category': 'LEVERAGE',
        'severity': 'HIGH',
        'korean_message': '레버리지 한도 초과',
        'reason_detail': '설정 가능한 최대 레버리지를 초과',
        'suggested_fix': '레버리지를 허용 범위 이내로 조정',
    },
    110006: {
        'category': 'POSITION_LOGIC',
        'severity': 'MEDIUM',
        'korean_message': '포지션 없음',
        'reason_detail': '청산/축소할 포지션이 존재하지 않음',
        'suggested_fix': '현재 포지션 상태 확인 후 재시도',
    },
    20001: {
        'category': 'PARAM_ERROR',
        'severity': 'MEDIUM',
        'korean_message': '주문 파라미터 오류',
        'reason_detail': '잘못된 주문 파라미터가 포함됨',
        'suggested_fix': '주문 파라미터 (수량, 가격, 방향) 확인',
    },
}

# Default mapping for unknown error codes
_DEFAULT_ERROR = {
    'category': 'UNKNOWN',
    'severity': 'MEDIUM',
    'korean_message': '거래소 주문 오류',
    'reason_detail': '알 수 없는 거래소 오류',
    'suggested_fix': '로그 확인 후 수동 조치 필요',
}

# Auto-correctable error codes (retry with correction)
AUTO_CORRECTABLE_CODES = {10003, 10004}


def map_bybit_error(error_code, raw_message=''):
    """Map Bybit error code to structured Korean error info.
    Returns dict with: error_code, category, severity, korean_message,
    reason_detail, suggested_fix, raw_message.
    """
    mapping = BYBIT_ERROR_MAP.get(error_code, _DEFAULT_ERROR)
    return {
        'error_code': error_code,
        'category': mapping['category'],
        'severity': mapping['severity'],
        'korean_message': mapping['korean_message'],
        'reason_detail': mapping['reason_detail'],
        'suggested_fix': mapping['suggested_fix'],
        'raw_message': raw_message,
    }


def extract_bybit_error_code(exception):
    """Extract error code from a ccxt exception or Bybit error response.
    Returns (error_code: int, raw_message: str) or (0, str).
    """
    msg = str(exception)

    # ccxt exceptions often have the format: "bybit {...}" or contain retCode
    import re

    # Try to extract retCode from JSON in the exception message
    code_match = re.search(r'"retCode"\s*:\s*(\d+)', msg)
    if code_match:
        return (int(code_match.group(1)), msg)

    # Try to extract from ccxt error format: "bybit 10001"
    code_match2 = re.search(r'bybit\s+(\d+)', msg, re.IGNORECASE)
    if code_match2:
        return (int(code_match2.group(1)), msg)

    # ccxt exception types mapping
    import ccxt
    code = 0
    if isinstance(exception, ccxt.InsufficientFunds):
        code = 110001
    elif isinstance(exception, ccxt.InvalidOrder):
        # Could be many things, try to guess from message
        if 'reduce' in msg.lower() or 'reduceOnly' in msg.lower():
            code = 110043
        elif 'qty' in msg.lower() or 'quantity' in msg.lower():
            code = 10001
        elif 'price' in msg.lower():
            code = 10003
        else:
            code = 20001
    elif isinstance(exception, ccxt.RateLimitExceeded):
        code = 10006
    elif isinstance(exception, ccxt.ExchangeError):
        if 'leverage' in msg.lower():
            code = 130074
        elif 'margin' in msg.lower() or 'mode' in msg.lower():
            code = 130021
        elif 'position' in msg.lower() and 'not' in msg.lower():
            code = 110006

    return (code, msg)


def is_auto_correctable(error_code):
    """Check if an error can be auto-corrected (stepSize/tickSize alignment)."""
    return error_code in AUTO_CORRECTABLE_CODES


# ── Telegram formatting ──────────────────────────────────

def format_rejection_telegram(error_info, debug=False):
    """Format rejection message for Telegram in Korean.
    error_info: dict from map_bybit_error() or ComplianceResult.
    """
    korean_msg = error_info.get('korean_message', '알 수 없는 오류')
    suggested = error_info.get('suggested_fix', '')
    error_code = error_info.get('error_code', '')

    lines = [
        '❌ 주문 거부',
        f'사유: {korean_msg}',
    ]
    if suggested:
        lines.append(f'해결: {suggested}')
    if error_code:
        lines.append(f'(에러코드: {error_code})')

    if debug:
        raw = error_info.get('raw_message', '')
        if raw:
            lines.append(f'\n[DEBUG] {raw[:200]}')
        category = error_info.get('category', '')
        severity = error_info.get('severity', '')
        if category:
            lines.append(f'[DEBUG] category={category} severity={severity}')

    return '\n'.join(lines)


def format_compliance_rejection_telegram(result):
    """Format ComplianceResult rejection for Telegram."""
    lines = [
        '❌ 주문 거부',
        f'사유: {result.reject_reason or result.reason}',
    ]
    if result.suggested_fix:
        lines.append(f'해결: {result.suggested_fix}')
    return '\n'.join(lines)


# ── DB logging helper ────────────────────────────────────

def log_compliance_event(cur, event_type, symbol, order_params,
                         compliance_passed, reject_reason=None,
                         exchange_error_code=None, suggested_fix=None,
                         emergency_flag=False, detail=None):
    """Log compliance event to compliance_log table.
    Automatically includes markets_version and markets_hash in detail.
    """
    detail_with_version = dict(detail or {})
    detail_with_version['markets_version'] = _markets_version
    detail_with_version['markets_hash'] = _markets_hash
    prot_status = _check_protection_mode()
    if prot_status[0]:
        detail_with_version['protection_mode_active'] = True
    try:
        cur.execute("""
            INSERT INTO compliance_log
                (symbol, event_type, order_params, compliance_passed,
                 reject_reason, exchange_error_code, suggested_fix,
                 emergency_flag, detail)
            VALUES (%s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s::jsonb)
            RETURNING id;
        """, (
            symbol or SYMBOL,
            event_type,
            json.dumps(order_params or {}, default=str),
            compliance_passed,
            reject_reason,
            exchange_error_code,
            suggested_fix,
            emergency_flag,
            json.dumps(detail_with_version, default=str),
        ))
        row = cur.fetchone()
        return row[0] if row else None
    except Exception as e:
        _log(f'compliance log error: {e}')
        return None


# ── 10-day policy audit (OpenClaw) ────────────────────────

def run_10day_audit(cur):
    """Analyze last 10 days of compliance_log data and generate Korean report.
    Returns dict with audit results and report text.
    """
    try:
        # ── Top 20 error codes ──
        cur.execute("""
            SELECT exchange_error_code, count(*) AS cnt,
                   array_agg(DISTINCT reject_reason) AS reasons
            FROM compliance_log
            WHERE ts >= now() - interval '10 days'
              AND exchange_error_code IS NOT NULL
              AND exchange_error_code > 0
            GROUP BY exchange_error_code
            ORDER BY cnt DESC
            LIMIT 20;
        """)
        top_errors = []
        for row in cur.fetchall():
            code, cnt, reasons = row
            mapped = BYBIT_ERROR_MAP.get(code, _DEFAULT_ERROR)
            top_errors.append({
                'code': code,
                'count': cnt,
                'korean_message': mapped['korean_message'],
                'category': mapped['category'],
                'reasons': reasons,
            })

        # ── Overall counts ──
        cur.execute("""
            SELECT count(*) AS total,
                   count(*) FILTER (WHERE NOT compliance_passed) AS rejections,
                   count(*) FILTER (WHERE event_type = 'EXCHANGE_ERROR') AS exchange_errors,
                   count(*) FILTER (WHERE event_type = 'PRE_ORDER_REJECT') AS pre_rejects,
                   count(*) FILTER (WHERE event_type = 'AUTO_CORRECTED') AS auto_corrected
            FROM compliance_log
            WHERE ts >= now() - interval '10 days';
        """)
        row = cur.fetchone()
        total, rejections, exchange_errors, pre_rejects, auto_corrected = row or (0, 0, 0, 0, 0)

        # ── Rejection rate ──
        rejection_rate = (rejections / total * 100) if total > 0 else 0

        # ── Rate limit events ──
        cur.execute("""
            SELECT count(*)
            FROM compliance_log
            WHERE ts >= now() - interval '10 days'
              AND exchange_error_code = 10006;
        """)
        rate_limit_events = cur.fetchone()[0] or 0

        # ── Position mode mismatch ──
        cur.execute("""
            SELECT count(*)
            FROM compliance_log
            WHERE ts >= now() - interval '10 days'
              AND exchange_error_code = 130021;
        """)
        mode_mismatch = cur.fetchone()[0] or 0

        # ── Protection mode activations (from detail JSONB) ──
        cur.execute("""
            SELECT count(*)
            FROM compliance_log
            WHERE ts >= now() - interval '10 days'
              AND detail->>'protection_mode_active' = 'true';
        """)
        protection_activations = cur.fetchone()[0] or 0

        # ── Daily trend ──
        cur.execute("""
            SELECT date_trunc('day', ts)::date AS day,
                   count(*) AS total,
                   count(*) FILTER (WHERE NOT compliance_passed) AS rejects
            FROM compliance_log
            WHERE ts >= now() - interval '10 days'
            GROUP BY day ORDER BY day;
        """)
        daily_trend = [(str(r[0]), r[1], r[2]) for r in cur.fetchall()]

        # ── Generate Korean report ──
        lines = [
            '📋 거래소 규정 준수 감사 보고서 (최근 10일)',
            '━' * 30,
            '',
            f'📊 전체 통계',
            f'  • 총 주문 시도: {total}건',
            f'  • 총 거부: {rejections}건 (거부율: {rejection_rate:.1f}%)',
            f'  • 거래소 오류: {exchange_errors}건',
            f'  • 사전 차단: {pre_rejects}건',
            f'  • 자동 보정: {auto_corrected}건',
            '',
            f'⚠️ 특이 지표',
            f'  • Rate Limit 이벤트: {rate_limit_events}건',
            f'  • 포지션 모드 불일치: {mode_mismatch}건',
            f'  • 보호 모드 발동: {protection_activations}건',
        ]

        if top_errors:
            lines.append('')
            lines.append('🔝 주요 에러 코드 (Top 20)')
            for e in top_errors:
                lines.append(f'  [{e["code"]}] {e["korean_message"]}: {e["count"]}건')

        if daily_trend:
            lines.append('')
            lines.append('📅 일별 추이')
            for day, t, r in daily_trend:
                bar = '█' * min(r, 20)
                lines.append(f'  {day}: {t}건 중 {r}건 거부 {bar}')

        lines.append('')
        lines.append(f'🔖 현재 마켓 규정 버전: v{_markets_version} (hash: {_markets_hash})')
        lines.append('━' * 30)

        report_text = '\n'.join(lines)

        audit_result = {
            'total_orders': total,
            'total_rejections': rejections,
            'rejection_rate': round(rejection_rate, 2),
            'exchange_errors': exchange_errors,
            'pre_rejects': pre_rejects,
            'auto_corrected': auto_corrected,
            'rate_limit_events': rate_limit_events,
            'mode_mismatch_events': mode_mismatch,
            'protection_mode_activations': protection_activations,
            'top_errors': top_errors,
            'daily_trend': daily_trend,
            'markets_version': _markets_version,
            'markets_hash': _markets_hash,
            'report_text': report_text,
        }

        # ── Store in exchange_policy_audit table ──
        try:
            cur.execute("""
                INSERT INTO exchange_policy_audit
                    (audit_period_start, audit_period_end,
                     total_orders, total_rejections, rejection_rate,
                     top_errors, rate_limit_events, mode_mismatch_events,
                     protection_mode_activations, report_text, detail)
                VALUES (now() - interval '10 days', now(),
                        %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s::jsonb)
                RETURNING id;
            """, (
                total, rejections, round(rejection_rate, 2),
                json.dumps(top_errors, default=str),
                rate_limit_events, mode_mismatch,
                protection_activations, report_text,
                json.dumps(audit_result, default=str),
            ))
            audit_id = cur.fetchone()
            if audit_id:
                _log(f'10-day audit saved: id={audit_id[0]}')
        except Exception as ae:
            _log(f'audit save error (table may not exist): {ae}')

        _log(f'10-day audit complete: {total} orders, {rejections} rejections '
             f'({rejection_rate:.1f}%)')
        return audit_result

    except Exception as e:
        _log(f'10-day audit error: {e}')
        return {'error': str(e), 'report_text': f'감사 오류: {e}'}
