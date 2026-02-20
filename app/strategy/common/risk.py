"""
strategy.common.risk — Risk computation: TP/SL, leverage, qty, stage allocation.

All functions are stateless. Config values come from the loaded YAML config dict.
"""

LOG_PREFIX = '[strategy.risk]'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def validate_min_qty(qty, min_qty=0.001):
    """Check if qty meets minimum order size.
    Returns True if valid, False + log if not."""
    if qty is None or qty < min_qty:
        _log(f'qty {qty} below min {min_qty}')
        return False
    return True


def compute_sl(mode, entry_price, side, atr, level_price, config):
    """Compute stop-loss price.

    Args:
        mode: 'A', 'B', or 'C'
        entry_price: float
        side: 'LONG' or 'SHORT'
        atr: float (ATR value, not percentage)
        level_price: float (VAH/VAL boundary for level-based SL)
        config: mode config dict from YAML

    Returns:
        float — SL price
    """
    if entry_price is None or entry_price == 0:
        return 0.0

    if mode == 'A':
        sl_pct = config.get('sl_pct', [0.005, 0.009])
        sl_frac = sl_pct[1] if isinstance(sl_pct, list) else sl_pct
        if side == 'LONG':
            # Use VA boundary if available, otherwise percentage
            if level_price and level_price < entry_price:
                return min(level_price, entry_price * (1 - sl_frac))
            return entry_price * (1 - sl_frac)
        else:
            if level_price and level_price > entry_price:
                return max(level_price, entry_price * (1 + sl_frac))
            return entry_price * (1 + sl_frac)

    elif mode == 'B':
        sl_pct = config.get('sl_pct', [0.007, 0.012])
        sl_frac = sl_pct[1] if isinstance(sl_pct, list) else sl_pct
        if side == 'LONG':
            return entry_price * (1 - sl_frac)
        else:
            return entry_price * (1 + sl_frac)

    elif mode == 'C':
        # Hybrid: level-based + ATR multiplier
        atr_mult = config.get('sl_atr_multiplier', 1.5)
        atr_sl = atr * atr_mult if atr else entry_price * 0.015
        if side == 'LONG':
            level_sl = level_price if level_price and level_price < entry_price else entry_price * 0.99
            return min(level_sl, entry_price - atr_sl)
        else:
            level_sl = level_price if level_price and level_price > entry_price else entry_price * 1.01
            return max(level_sl, entry_price + atr_sl)

    # Fallback
    fallback_pct = 0.01
    if side == 'LONG':
        return entry_price * (1 - fallback_pct)
    return entry_price * (1 + fallback_pct)


def compute_tp(mode, entry_price, side, poc, config):
    """Compute take-profit price.

    Args:
        mode: 'A', 'B', or 'C'
        entry_price: float
        side: 'LONG' or 'SHORT'
        poc: float (point of control) — used as TP target for MODE_A
        config: mode config dict from YAML

    Returns:
        float — TP price
    """
    if entry_price is None or entry_price == 0:
        return 0.0

    if mode == 'A':
        tp_pct = config.get('tp_pct', [0.004, 0.008])
        tp_frac = tp_pct[0] if isinstance(tp_pct, list) else tp_pct
        # Use POC as conservative TP if closer
        if side == 'LONG':
            pct_tp = entry_price * (1 + tp_frac)
            if poc and poc > entry_price:
                return min(poc, pct_tp)
            return pct_tp
        else:
            pct_tp = entry_price * (1 - tp_frac)
            if poc and poc < entry_price:
                return max(poc, pct_tp)
            return pct_tp

    elif mode == 'B':
        tp_pct = config.get('tp_pct', [0.003, 0.006])
        tp_frac = tp_pct[0] if isinstance(tp_pct, list) else tp_pct
        if side == 'LONG':
            return entry_price * (1 + tp_frac)
        return entry_price * (1 - tp_frac)

    elif mode == 'C':
        tp1_pct = config.get('tp1_pct', [0.006, 0.010])
        tp_frac = tp1_pct[0] if isinstance(tp1_pct, list) else tp1_pct
        if side == 'LONG':
            return entry_price * (1 + tp_frac)
        return entry_price * (1 - tp_frac)

    # Fallback
    fallback_pct = 0.005
    if side == 'LONG':
        return entry_price * (1 + fallback_pct)
    return entry_price * (1 - fallback_pct)


def clamp_leverage(mode, base_leverage, config):
    """Clamp leverage to mode-allowed range.

    Args:
        mode: 'A', 'B', or 'C'
        base_leverage: int — desired leverage
        config: mode config dict from YAML

    Returns:
        int — clamped leverage
    """
    lev_range = config.get('leverage', [3, 5])
    lev_min = lev_range[0] if isinstance(lev_range, list) else 3
    lev_max = lev_range[1] if isinstance(lev_range, list) else 5
    return max(lev_min, min(lev_max, int(base_leverage)))


def compute_stage_qty(capital, stage, max_stages, config):
    """Compute qty allocation for a given stage.

    Args:
        capital: float — total available capital in USDT
        stage: int — current stage (1-based)
        max_stages: int — max stages for current mode
        config: mode config dict from YAML

    Returns:
        float — USDT allocation for this stage
    """
    allocation = config.get('stage_allocation', None)
    if allocation and stage <= len(allocation):
        return capital * allocation[stage - 1]
    # Equal allocation fallback
    if max_stages <= 0:
        return 0.0
    return capital / max_stages


def validate_stage_min_qty(stage_qty, min_qty=0.001):
    """Check if stage qty is usable. If not, return adjusted values.

    Returns:
        (usable: bool, adjusted_qty: float)
    """
    if stage_qty >= min_qty:
        return (True, stage_qty)
    return (False, 0.0)
