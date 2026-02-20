"""
strategy.common.features — Feature computation from existing DB data.

Uses existing `indicators`, `vol_profile`, and `candles` tables.
No new indicators are computed; this module extracts and packages
existing data into a feature snapshot for the regime router.
"""

import math

LOG_PREFIX = '[strategy.features]'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def compute_atr_pct(cur, symbol='BTC/USDT:USDT'):
    """ATR as percentage of price.  Returns float or None."""
    try:
        cur.execute("""
            SELECT atr_14 FROM indicators
            WHERE symbol = %s AND tf = '1m' ORDER BY ts DESC LIMIT 1
        """, (symbol,))
        row = cur.fetchone()
        if not row or row[0] is None:
            return None
        atr = float(row[0])

        cur.execute("""
            SELECT c FROM candles
            WHERE symbol = %s AND tf = '1m'
            ORDER BY ts DESC LIMIT 1
        """, (symbol,))
        c_row = cur.fetchone()
        if not c_row or c_row[0] is None or float(c_row[0]) == 0:
            return None
        return atr / float(c_row[0])
    except Exception as e:
        _log(f'compute_atr_pct error: {e}')
        return None


def compute_bb_width(cur, symbol='BTC/USDT:USDT'):
    """Bollinger Band width as fraction: (upper - lower) / mid.  Returns float or None."""
    try:
        cur.execute("""
            SELECT bb_up, bb_dn, bb_mid FROM indicators
            WHERE symbol = %s AND tf = '1m' ORDER BY ts DESC LIMIT 1
        """, (symbol,))
        row = cur.fetchone()
        if not row or any(v is None for v in row):
            return None
        bb_up, bb_dn, bb_mid = float(row[0]), float(row[1]), float(row[2])
        if bb_mid == 0:
            return None
        return (bb_up - bb_dn) / bb_mid
    except Exception as e:
        _log(f'compute_bb_width error: {e}')
        return None


def compute_volume_z(cur, symbol='BTC/USDT:USDT', window=50):
    """Volume Z-score: (current_vol - mean) / stdev over `window` candles.
    Returns float or None."""
    try:
        cur.execute("""
            SELECT v FROM candles
            WHERE symbol = %s AND tf = '1m'
            ORDER BY ts DESC LIMIT %s
        """, (symbol, window + 1))
        rows = cur.fetchall()
        if not rows or len(rows) < 10:
            return None
        volumes = [float(r[0]) for r in rows if r[0] is not None]
        if len(volumes) < 10:
            return None
        current = volumes[0]
        hist = volumes[1:]
        mean = sum(hist) / len(hist)
        variance = sum((v - mean) ** 2 for v in hist) / len(hist)
        stdev = math.sqrt(variance) if variance > 0 else 0
        if stdev == 0:
            return 0.0
        return (current - mean) / stdev
    except Exception as e:
        _log(f'compute_volume_z error: {e}')
        return None


def compute_impulse(cur, symbol='BTC/USDT:USDT'):
    """Impulse: abs(close - open) / ATR for latest candle.  Returns float or None."""
    try:
        cur.execute("""
            SELECT o, c FROM candles
            WHERE symbol = %s AND tf = '1m'
            ORDER BY ts DESC LIMIT 1
        """, (symbol,))
        candle = cur.fetchone()
        if not candle or candle[0] is None or candle[1] is None:
            return None

        cur.execute("""
            SELECT atr_14 FROM indicators
            WHERE symbol = %s AND tf = '1m' ORDER BY ts DESC LIMIT 1
        """, (symbol,))
        atr_row = cur.fetchone()
        if not atr_row or atr_row[0] is None or float(atr_row[0]) == 0:
            return None

        return abs(float(candle[1]) - float(candle[0])) / float(atr_row[0])
    except Exception as e:
        _log(f'compute_impulse error: {e}')
        return None


def compute_range_position(price, vah, val):
    """Position within value area: 0.0 (at VAL) to 1.0 (at VAH).
    Returns float or None if inputs invalid."""
    if price is None or vah is None or val is None:
        return None
    price, vah, val = float(price), float(vah), float(val)
    if vah == val:
        return 0.5
    return (price - val) / (vah - val)


def compute_drift_score(cur, symbol='BTC/USDT:USDT', lookback=10):
    """Drift score based on POC movement over `lookback` entries.
    Returns (score: float, direction: 'UP'|'DOWN'|'NONE')."""
    try:
        cur.execute("""
            SELECT poc FROM vol_profile
            WHERE symbol = %s
            ORDER BY ts DESC LIMIT %s
        """, (symbol, lookback))
        rows = cur.fetchall()
        if not rows or len(rows) < 3:
            return (0.0, 'NONE')
        pocs = [float(r[0]) for r in rows if r[0] is not None]
        if len(pocs) < 3:
            return (0.0, 'NONE')
        # Compute linear drift: (newest - oldest) / oldest
        newest, oldest = pocs[0], pocs[-1]
        if oldest == 0:
            return (0.0, 'NONE')
        drift = (newest - oldest) / oldest
        direction = 'UP' if drift > 0.0005 else ('DOWN' if drift < -0.0005 else 'NONE')
        return (abs(drift), direction)
    except Exception as e:
        _log(f'compute_drift_score error: {e}')
        return (0.0, 'NONE')


def compute_poc_slope(cur, symbol='BTC/USDT:USDT', lookback=5):
    """POC slope: average per-step change / price over `lookback` entries.
    Returns float or None."""
    try:
        cur.execute("""
            SELECT poc FROM vol_profile
            WHERE symbol = %s
            ORDER BY ts DESC LIMIT %s
        """, (symbol, lookback))
        rows = cur.fetchall()
        if not rows or len(rows) < 2:
            return None
        pocs = [float(r[0]) for r in rows if r[0] is not None]
        if len(pocs) < 2 or pocs[-1] == 0:
            return None
        # Average step change as fraction of price
        steps = [abs(pocs[i] - pocs[i + 1]) for i in range(len(pocs) - 1)]
        avg_step = sum(steps) / len(steps)
        return avg_step / pocs[-1]
    except Exception as e:
        _log(f'compute_poc_slope error: {e}')
        return None


def _get_current_price(cur, symbol='BTC/USDT:USDT'):
    """Get current price from latest 1m candle close."""
    try:
        cur.execute("""
            SELECT c FROM candles
            WHERE symbol = %s AND tf = '1m'
            ORDER BY ts DESC LIMIT 1
        """, (symbol,))
        row = cur.fetchone()
        if row and row[0] is not None:
            return float(row[0])
    except Exception:
        pass
    return None


def _get_adx(cur, symbol='BTC/USDT:USDT'):
    """Get ADX from market_context_latest view."""
    try:
        cur.execute("""
            SELECT adx_14 FROM market_context_latest
            WHERE symbol = %s
        """, (symbol,))
        row = cur.fetchone()
        if row and row[0] is not None:
            return float(row[0])
    except Exception:
        pass
    return None


def _get_vol_profile(cur, symbol='BTC/USDT:USDT'):
    """Get latest POC/VAH/VAL."""
    try:
        cur.execute("""
            SELECT poc, vah, val FROM vol_profile
            WHERE symbol = %s
            ORDER BY ts DESC LIMIT 1
        """, (symbol,))
        row = cur.fetchone()
        if row:
            return {
                'poc': float(row[0]) if row[0] is not None else None,
                'vah': float(row[1]) if row[1] is not None else None,
                'val': float(row[2]) if row[2] is not None else None,
            }
    except Exception:
        pass
    return {'poc': None, 'vah': None, 'val': None}


def build_feature_snapshot(cur, symbol='BTC/USDT:USDT'):
    """Build complete feature snapshot for regime routing.

    Returns dict with all features. Missing features are None.
    Never raises — returns partial snapshot on error.
    """
    vp = _get_vol_profile(cur, symbol)
    price = _get_current_price(cur, symbol)
    drift_score, drift_dir = compute_drift_score(cur, symbol)

    return {
        'symbol': symbol,
        'price': price,
        'atr_pct': compute_atr_pct(cur, symbol),
        'bb_width': compute_bb_width(cur, symbol),
        'volume_z': compute_volume_z(cur, symbol),
        'impulse': compute_impulse(cur, symbol),
        'adx': _get_adx(cur, symbol),
        'poc': vp['poc'],
        'vah': vp['vah'],
        'val': vp['val'],
        'range_position': compute_range_position(price, vp['vah'], vp['val']),
        'drift_score': drift_score,
        'drift_direction': drift_dir,
        'poc_slope': compute_poc_slope(cur, symbol),
    }
