"""
leverage_manager.py — Dynamic leverage selection (3-8x), score-based (v2.1).

Inputs: ATR%, regime_score, news_shock, confidence (abs(total_score)), current_stage
Output: recommended leverage (int 3-8)

Rules (v2.1, score-based):
  - abs_score >= 25: base 7x (8x if ATR < 1%)
  - abs_score >= 15: base 5x (6x if ATR < 1%)
  - abs_score < 15:  base 3x (4x if ATR < 1%)
  - news >= 80 -> cap at 4x; news >= 60 -> cap at 5x; daily_loss -> cap at 3x
  - Stage >= 5 -> cap at 5x; Stage >= 3 -> cap at 6x
"""
import sys
sys.path.insert(0, '/root/trading-bot/app')
LOG_PREFIX = '[leverage_mgr]'

# Cache: avoid redundant set_leverage API calls
_last_leverage = {}  # {symbol: int}


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def compute_leverage(atr_pct, regime_score, news_shock, confidence, stage,
                     daily_loss=False, regime=None, shock_type=None):
    """Compute recommended leverage (3-8x), score-based (v2.1).

    Args:
        atr_pct: ATR_14 / price * 100
        regime_score: score_engine regime axis (-100~100)
        news_shock: True if abs(news_event_score) >= 60
        confidence: abs(total_score) from score_engine
        stage: current pyramid stage (0-7)
        daily_loss: True if daily loss limit approached
        regime: MCTX regime string (optional, for clamping)
        shock_type: MCTX shock sub-type (optional)

    Returns:
        int: recommended leverage (3-8)
    """
    abs_score = confidence  # confidence = abs(total_score)

    # Score-based leverage (v2.1)
    if abs_score >= 25:
        base = 7       # 7-8x range
        if atr_pct < 1.0:
            base = 8
    elif abs_score >= 15:
        base = 5       # 5-6x range
        if atr_pct < 1.0:
            base = 6
    else:
        base = 3       # 3-4x range
        if atr_pct < 1.0:
            base = 4

    # Safety overrides: news-adjusted leverage reduction
    if daily_loss:
        base = min(base, 3)
    # Graduated news impact on leverage (news_shock: bool or abs(news_score))
    _news_abs = 0
    if news_shock is True:
        _news_abs = 60
    elif isinstance(news_shock, (int, float)):
        _news_abs = abs(news_shock)
    if _news_abs >= 80:
        base = min(base, 4)
    elif _news_abs >= 60:
        base = min(base, 5)

    # Stage cap: stages 5-7 max 5x, stages 3-4 max 6x
    if stage >= 5:
        base = min(base, 5)
    elif stage >= 3:
        base = min(base, 6)

    # Regime-based clamping (MCTX Phase 2)
    if regime:
        try:
            import regime_reader
            params = regime_reader.get_regime_params(regime, shock_type)
            base = max(params['leverage_min'], min(params['leverage_max'], base))

            # BREAKOUT stage-specific leverage ranges
            if regime == 'BREAKOUT':
                stage_lev = params.get('stage_leverage', {})
                if stage in stage_lev:
                    lev_min, lev_max = stage_lev[stage]
                    base = max(lev_min, min(lev_max, base))
        except Exception:
            pass  # FAIL-OPEN: skip clamping on error

    return max(3, min(8, base))


def set_exchange_leverage(exchange, symbol, leverage):
    """Set leverage on exchange. Skips if already same value (cached).

    Args:
        exchange: ccxt exchange instance
        symbol: trading symbol
        leverage: int leverage to set

    Returns:
        bool: True if set successfully or skipped (same value)
    """
    # Skip if already set to same value
    if _last_leverage.get(symbol) == leverage:
        return True

    try:
        exchange.set_leverage(leverage, symbol)
        _last_leverage[symbol] = leverage
        _log(f'leverage set: {symbol} -> {leverage}x')
        return True
    except Exception as e:
        _log(f'set_leverage error ({symbol}, {leverage}x): {e}')
        return False


def get_current_leverage_info(cur, exchange, symbol):
    """Get current leverage info from exchange + DB limits.

    Returns:
        dict: {current, min, max, stage_cap}
    """
    current = 0
    try:
        positions = exchange.fetch_positions([symbol])
        for p in positions:
            if p.get('symbol') == symbol:
                current = int(float(p.get('leverage') or 0))
                break
    except Exception as e:
        _log(f'get_current_leverage_info exchange error: {e}')

    # Read limits from DB
    lev_min = 3
    lev_max = 8
    high_stage_max = 5
    try:
        cur.execute("""
            SELECT leverage_min, leverage_max, leverage_high_stage_max
            FROM safety_limits ORDER BY id DESC LIMIT 1;
        """)
        row = cur.fetchone()
        if row:
            lev_min = int(row[0]) if row[0] is not None else 3
            lev_max = int(row[1]) if row[1] is not None else 8
            high_stage_max = int(row[2]) if row[2] is not None else 5
    except Exception as e:
        _log(f'get_current_leverage_info DB error: {e}')

    return {
        'current': current,
        'min': lev_min,
        'max': lev_max,
        'stage_cap': high_stage_max,
    }
