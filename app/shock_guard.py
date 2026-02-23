"""
shock_guard.py — 1-minute rapid move defense (P0-4).

Detects sudden price shocks on 1m candles and freezes new entries/ADDs.
EXIT/CLOSE/REDUCE always permitted. FAIL-OPEN on error.

Shock conditions (OR):
  1) abs(1m_return) >= shock_threshold_pct (default 1.5%)
  2) abs(1m_return) >= k * atr_1m (k=2.5, ATR-based dynamic threshold)

Freeze: No new entries or ADDs for shock_freeze_sec (default 300s).
"""

import os
import sys
import time
import json
import traceback

sys.path.insert(0, '/root/trading-bot/app')

LOG_PREFIX = '[shock_guard]'
SYMBOL = os.getenv('SYMBOL', 'BTC/USDT:USDT')

# In-memory freeze state (fast check, no DB required)
_freeze_until = 0
_last_shock_pct = 0.0
_last_shock_ts = 0


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def check_shock(cur):
    """Check recent 1m candles for price shock.

    Args:
        cur: DB cursor (for reading candles and writing events)

    Returns:
        (shocked: bool, detail: dict)
    """
    global _freeze_until, _last_shock_pct, _last_shock_ts

    try:
        import feature_flags
        if not feature_flags.is_enabled('ff_shock_guard'):
            return (False, {'reason': 'ff_shock_guard OFF'})

        from strategy_v3 import config_v3
        cfg = config_v3.get_all()
        threshold_pct = cfg.get('shock_guard_threshold_pct', 1.5)
        atr_mult = cfg.get('shock_guard_atr_mult', 2.5)
        freeze_sec = cfg.get('shock_guard_freeze_sec', 300)
        lookback = cfg.get('shock_guard_lookback_candles', 3)

        # Fetch recent 1m candles from market_ohlcv
        cur.execute("""
            SELECT ts, o, h, l, c
            FROM candles
            WHERE symbol = %s AND tf = '1m'
            ORDER BY ts DESC
            LIMIT %s;
        """, (SYMBOL, lookback + 10))  # extra for ATR calc
        rows = cur.fetchall()
        if not rows or len(rows) < 2:
            return (False, {'reason': 'insufficient 1m data'})

        # Compute 1m returns and ATR
        candles = []
        for r in rows:
            candles.append({
                'open_time': r[0],
                'open': float(r[1]) if r[1] else 0,
                'high': float(r[2]) if r[2] else 0,
                'low': float(r[3]) if r[3] else 0,
                'close': float(r[4]) if r[4] else 0,
            })

        # Most recent candle return
        latest = candles[0]
        if latest['open'] <= 0:
            return (False, {'reason': 'invalid candle'})

        ret_pct = (latest['close'] - latest['open']) / latest['open'] * 100

        # ATR_1m: average of high-low ranges for recent candles
        ranges = []
        for c in candles[:lookback + 5]:
            if c['high'] > 0 and c['low'] > 0:
                ranges.append((c['high'] - c['low']) / c['open'] * 100
                              if c['open'] > 0 else 0)
        atr_1m = sum(ranges) / len(ranges) if ranges else 0

        # Check shock conditions
        shocked = False
        trigger_type = None

        # Condition 1: absolute threshold
        if abs(ret_pct) >= threshold_pct:
            shocked = True
            trigger_type = f'ABS_THRESHOLD ({abs(ret_pct):.2f}% >= {threshold_pct}%)'

        # Condition 2: ATR-based dynamic
        if not shocked and atr_1m > 0 and abs(ret_pct) >= atr_mult * atr_1m:
            shocked = True
            trigger_type = (f'ATR_MULT ({abs(ret_pct):.2f}% >= '
                            f'{atr_mult}x{atr_1m:.3f}%={atr_mult * atr_1m:.3f}%)')

        # Also check lookback candles (cumulative shock)
        if not shocked and len(candles) >= lookback:
            cumulative_ret = 0
            for c in candles[:lookback]:
                if c['open'] > 0:
                    cumulative_ret += (c['close'] - c['open']) / c['open'] * 100
            if abs(cumulative_ret) >= threshold_pct * 1.2:
                shocked = True
                trigger_type = (f'CUMULATIVE_{lookback}m '
                                f'({abs(cumulative_ret):.2f}% >= '
                                f'{threshold_pct * 1.2:.2f}%)')

        if shocked:
            _freeze_until = time.time() + freeze_sec
            _last_shock_pct = ret_pct
            _last_shock_ts = time.time()

            _log(f'SHOCK DETECTED: {trigger_type} | ret={ret_pct:.2f}% '
                 f'| atr_1m={atr_1m:.4f}% | freeze={freeze_sec}s')

            # Record to DB
            try:
                cur.execute("""
                    INSERT INTO shock_guard_events
                        (symbol, price_change_pct, atr_1m, atr_mult_used,
                         freeze_until, trigger_type, candle_data)
                    VALUES (%s, %s, %s, %s,
                            now() + make_interval(secs => %s), %s, %s::jsonb);
                """, (SYMBOL, ret_pct, atr_1m, atr_mult,
                      freeze_sec, trigger_type,
                      json.dumps({'latest': latest,
                                  'lookback': lookback}, default=str)))
            except Exception as e:
                _log(f'DB record error: {e}')

            # Trigger server stop reconcile (P0-2 linkage)
            try:
                import server_stop_manager
                ssm = server_stop_manager.get_manager()
                if ssm._last_order_id:
                    ssm._verify_stop_exists()
            except Exception:
                pass

            return (True, {
                'trigger_type': trigger_type,
                'ret_pct': ret_pct,
                'atr_1m': atr_1m,
                'freeze_sec': freeze_sec,
                'freeze_until': _freeze_until,
            })

        return (False, {
            'ret_pct': ret_pct,
            'atr_1m': atr_1m,
            'threshold_pct': threshold_pct,
        })

    except Exception as e:
        _log(f'check_shock FAIL-OPEN: {e}')
        traceback.print_exc()
        return (False, {'error': str(e)})


def is_entry_frozen():
    """Fast in-memory check: is entry/ADD frozen due to shock?

    Returns: (frozen: bool, remaining_sec: float)
    """
    now = time.time()
    if now < _freeze_until:
        return (True, _freeze_until - now)
    return (False, 0)


# D1-1: PanicGuard 단계적 대응 — 쿨다운 상태
_panic_last_action_ts = 0


def get_graduated_action(cur, position):
    """D1-1: 단계적 급락 대응 판정.

    Args:
        cur: DB cursor
        position: dict {side, qty, entry_price}

    Returns:
        (action, detail) where action = None | 'TIGHTEN_STOP' | 'REDUCE_HALF' | 'CLOSE_ALL'
    """
    global _panic_last_action_ts

    try:
        import feature_flags
        if not feature_flags.is_enabled('ff_shock_guard'):
            return (None, 'ff_shock_guard OFF')

        from strategy_v3 import config_v3
        cfg = config_v3.get_all()
        tighten_pct = cfg.get('panic_guard_tighten_pct', 0.35)
        reduce_pct = cfg.get('panic_guard_reduce_pct', 0.60)
        close_pct = cfg.get('panic_guard_close_pct', 1.00)
        cooldown_sec = cfg.get('panic_guard_cooldown_sec', 60)

        side = position.get('side', '').lower()
        entry_price = float(position.get('entry_price', 0))
        if not side or entry_price <= 0:
            return (None, 'no position')

        # 쿨다운 체크
        now = time.time()
        if now - _panic_last_action_ts < cooldown_sec:
            remaining = cooldown_sec - (now - _panic_last_action_ts)
            return (None, f'COOLDOWN ({remaining:.0f}s remaining)')

        # 최신 1분봉 수익률 계산
        cur.execute("""
            SELECT o, c FROM candles
            WHERE symbol = %s AND tf = '1m'
            ORDER BY ts DESC LIMIT 1;
        """, (SYMBOL,))
        row = cur.fetchone()
        if not row or not row[0] or float(row[0]) <= 0:
            return (None, 'no candle data')

        candle_open = float(row[0])
        candle_close = float(row[1])
        ret_1m = (candle_close - candle_open) / candle_open * 100

        # 불리한 방향 판별 (long이면 하락이 불리, short이면 상승이 불리)
        adverse_ret = -ret_1m if side == 'long' else ret_1m

        if adverse_ret < tighten_pct:
            return (None, f'no action (adverse={adverse_ret:.3f}% < tighten={tighten_pct}%)')

        # 단계적 대응 결정
        if adverse_ret >= close_pct:
            action = 'CLOSE_ALL'
        elif adverse_ret >= reduce_pct:
            action = 'REDUCE_HALF'
        else:
            action = 'TIGHTEN_STOP'

        _panic_last_action_ts = now
        detail = f'{action}: adverse_ret={adverse_ret:.3f}% (thresholds: T={tighten_pct} R={reduce_pct} C={close_pct})'

        # DB 기록
        record_panic_event(cur, action, adverse_ret, candle_close, side)

        _log(f'PANIC GUARD: {detail}')
        return (action, detail)

    except Exception as e:
        _log(f'get_graduated_action FAIL-OPEN: {e}')
        return (None, f'error: {e}')


def record_panic_event(cur, action, ret_pct, price, side='unknown'):
    """D1-1: panic_guard_events 테이블에 이벤트 기록."""
    try:
        cur.execute("""
            INSERT INTO panic_guard_events (symbol, side, action, ret_1m, price, meta)
            VALUES (%s, %s, %s, %s, %s, %s::jsonb);
        """, (SYMBOL, side, action, ret_pct, price, json.dumps({'ts': time.time()})))
    except Exception as e:
        _log(f'record_panic_event error: {e}')


def get_shock_status():
    """Return full shock guard status for /debug."""
    frozen, remaining = is_entry_frozen()
    try:
        import feature_flags
        ff_on = feature_flags.is_enabled('ff_shock_guard')
    except Exception:
        ff_on = False

    # Read cooldown from config (not hardcoded)
    try:
        from strategy_v3 import config_v3
        _cfg = config_v3.get_all()
        _cooldown = _cfg.get('panic_guard_cooldown_sec', 60)
    except Exception:
        _cooldown = 60

    return {
        'ff_enabled': ff_on,
        'frozen': frozen,
        'remaining_sec': round(remaining, 1),
        'freeze_until': _freeze_until,
        'last_shock_pct': round(_last_shock_pct, 4),
        'last_shock_ts': _last_shock_ts,
        'panic_cooldown_remaining': max(0, round(_cooldown - (time.time() - _panic_last_action_ts), 1)),
    }
