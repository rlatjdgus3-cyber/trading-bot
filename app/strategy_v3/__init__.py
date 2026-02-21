"""strategy_v3 — Regime switching + chase entry suppression layer."""


def safe_float(val, default=0.0):
    """Safely convert value to float. Returns default on failure."""
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def compute_health(features):
    """Compute market health from spread_ok and liquidity_ok flags."""
    spread_ok = features.get('spread_ok', True)
    liquidity_ok = features.get('liquidity_ok', True)
    if spread_ok is None:
        spread_ok = True
    if liquidity_ok is None:
        liquidity_ok = True
    return 'OK' if (spread_ok and liquidity_ok) else 'WARN'
