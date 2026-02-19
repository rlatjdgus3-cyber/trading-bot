"""
regime_reader.py — Read-only helper for live trading bot to read market regime.

FAIL-OPEN guarantee:
  - Table missing / empty / stale (>5 min) → returns UNKNOWN → existing behavior preserved.
  - Never blocks, never raises.
"""

STALE_THRESHOLD_SEC = 300
LOG_PREFIX = '[regime_reader]'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _default():
    """FAIL-OPEN default: UNKNOWN regime, no restrictions."""
    return {
        'regime': 'UNKNOWN',
        'confidence': 0,
        'shock_type': None,
        'flow_bias': 0,
        'breakout_confirmed': False,
        'stale': False,
        'available': False,
    }


def get_current_regime(cur, symbol='BTC/USDT:USDT'):
    """Read latest regime from market_context_latest view.

    Returns:
    {
        'regime': 'RANGE'|'BREAKOUT'|'SHOCK'|'UNKNOWN',
        'confidence': 0-100,
        'shock_type': str|None,
        'flow_bias': float,
        'breakout_confirmed': bool,
        'stale': bool,
        'available': bool,  # True if data is valid and fresh
    }
    """
    try:
        cur.execute("""
            SELECT regime, regime_confidence, shock_type, flow_bias,
                   breakout_confirmed, age_seconds
            FROM market_context_latest
            WHERE symbol = %s;
        """, (symbol,))
        row = cur.fetchone()
        if not row:
            return _default()

        age = float(row[5]) if row[5] else 9999
        stale = age > STALE_THRESHOLD_SEC

        return {
            'regime': row[0] if not stale else 'UNKNOWN',
            'confidence': int(row[1]) if row[1] else 0,
            'shock_type': row[2],
            'flow_bias': float(row[3]) if row[3] else 0,
            'breakout_confirmed': bool(row[4]) if row[4] is not None else False,
            'stale': stale,
            'available': not stale,
        }
    except Exception as e:
        # FAIL-OPEN: table doesn't exist, DB error, etc.
        _log(f'FAIL-OPEN: {e}')
        return _default()


def get_stage_limit(regime, shock_type=None):
    """Get maximum stage count for a given regime.

    RANGE:          3
    BREAKOUT:       7
    SHOCK/VETO:     0  (block all entries)
    SHOCK/RISK_DOWN: 2
    SHOCK/ACCEL:    5
    UNKNOWN:        7  (FAIL-OPEN, no restriction)
    """
    if regime == 'RANGE':
        return 3
    if regime == 'BREAKOUT':
        return 7
    if regime == 'SHOCK':
        if shock_type == 'VETO':
            return 0
        if shock_type == 'RISK_DOWN':
            return 2
        if shock_type == 'ACCEL':
            return 5
        return 2  # default SHOCK without sub-type
    # UNKNOWN or anything else: FAIL-OPEN
    return 7
