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


def compute_vol_pct(cur, symbol='BTC/USDT:USDT', window=20):
    """Realized volatility: std of log-returns over `window` 5m candles.
    Returns float (as percentage) or None."""
    try:
        cur.execute("""
            SELECT c FROM candles
            WHERE symbol = %s AND tf = '5m'
            ORDER BY ts DESC LIMIT %s
        """, (symbol, window + 1))
        rows = cur.fetchall()
        if not rows or len(rows) < 5:
            return None
        closes = [float(r[0]) for r in rows if r[0] is not None]
        if len(closes) < 5:
            return None
        returns = []
        for i in range(len(closes) - 1):
            if closes[i + 1] > 0:
                returns.append(math.log(closes[i] / closes[i + 1]))
        if len(returns) < 3:
            return None
        mean_r = sum(returns) / len(returns)
        variance = sum((r - mean_r) ** 2 for r in returns) / len(returns)
        return math.sqrt(variance) * 100  # as percentage
    except Exception as e:
        _log(f'compute_vol_pct error: {e}')
        return None


def compute_trend_strength(adx):
    """ADX normalized to 0-1 (ADX/50 capped at 1.0). Returns float or None."""
    if adx is None:
        return None
    return min(adx / 50.0, 1.0)


def compute_range_quality(cur, symbol='BTC/USDT:USDT', lookback=10):
    """VA width consistency score 0-1.
    Measures how stable the VA range has been over recent entries.
    Returns float or None."""
    try:
        cur.execute("""
            SELECT vah, val FROM vol_profile
            WHERE symbol = %s
            ORDER BY ts DESC LIMIT %s
        """, (symbol, lookback))
        rows = cur.fetchall()
        if not rows or len(rows) < 3:
            return None
        widths = []
        for r in rows:
            if r[0] is not None and r[1] is not None:
                vah, val = float(r[0]), float(r[1])
                if val > 0:
                    widths.append((vah - val) / val)
        if len(widths) < 3:
            return None
        mean_w = sum(widths) / len(widths)
        if mean_w == 0:
            return 1.0
        variance = sum((w - mean_w) ** 2 for w in widths) / len(widths)
        cv = math.sqrt(variance) / mean_w if mean_w > 0 else 0
        # Lower CV = more consistent = higher quality
        return max(0.0, min(1.0, 1.0 - cv))
    except Exception as e:
        _log(f'compute_range_quality error: {e}')
        return None


def compute_atr_ratio(cur, symbol='BTC/USDT:USDT', window=20):
    """ATR expansion ratio: current ATR / SMA of recent ATR values.
    Returns float (>1.0 = expanding) or None.  FAIL-OPEN."""
    try:
        cur.execute("""
            SELECT atr_14 FROM indicators
            WHERE symbol = %s AND tf = '1m'
            ORDER BY ts DESC LIMIT %s
        """, (symbol, window + 1))
        rows = cur.fetchall()
        if not rows or len(rows) < 5:
            return None
        atrs = [float(r[0]) for r in rows if r[0] is not None]
        if len(atrs) < 5:
            return None
        current = atrs[0]
        hist = atrs[1:]
        mean_atr = sum(hist) / len(hist)
        if mean_atr <= 0:
            return None
        return current / mean_atr
    except Exception as e:
        _log(f'compute_atr_ratio error: {e}')
        return None


def compute_structure_breakout(cur, vah, val, atr_val, price, symbol='BTC/USDT:USDT',
                               n=3, m=2, k_dist=0.25, pct_dist=0.0015):
    """Structure breakout check: M of N recent candle closes outside VA ± min_dist.

    Returns (passed: bool, direction: 'UP'|'DOWN'|None, detail: dict).
    FAIL-OPEN: (False, None, {}) on error.
    """
    try:
        if vah is None or val is None or atr_val is None or price is None:
            return (False, None, {})
        if vah <= val or atr_val <= 0:
            return (False, None, {})

        min_dist = max(atr_val * k_dist, price * pct_dist)

        cur.execute("""
            SELECT c FROM candles
            WHERE symbol = %s AND tf = '1m'
            ORDER BY ts DESC LIMIT %s
        """, (symbol, n))
        rows = cur.fetchall()
        if not rows or len(rows) < n:
            return (False, None, {})

        closes = [float(r[0]) for r in rows if r[0] is not None]
        if len(closes) < n:
            return (False, None, {})

        above_count = sum(1 for c in closes if c > vah + min_dist)
        below_count = sum(1 for c in closes if c < val - min_dist)

        detail = {
            'above_count': above_count,
            'below_count': below_count,
            'min_dist': min_dist,
            'n': n,
            'm': m,
        }

        if above_count >= m:
            return (True, 'UP', detail)
        if below_count >= m:
            return (True, 'DOWN', detail)
        return (False, None, detail)
    except Exception as e:
        _log(f'compute_structure_breakout error: {e}')
        return (False, None, {})


def compute_spread_ok(cur, symbol='BTC/USDT:USDT', max_spread_pct=0.05):
    """Check if bid-ask spread < max_spread_pct%.
    FAIL-OPEN: returns True if data unavailable."""
    try:
        cur.execute("""
            SELECT bid, ask FROM market_data_cache
            WHERE symbol = %s
            ORDER BY ts DESC LIMIT 1
        """, (symbol,))
        row = cur.fetchone()
        if not row or row[0] is None or row[1] is None:
            return True  # FAIL-OPEN
        bid, ask = float(row[0]), float(row[1])
        if bid <= 0:
            return True
        spread_pct = ((ask - bid) / bid) * 100
        return spread_pct < max_spread_pct
    except Exception:
        return True  # FAIL-OPEN


def compute_liquidity_ok(cur, symbol='BTC/USDT:USDT', window=50, min_ratio=0.5):
    """Check if current volume >= min_ratio of median last `window` bars.
    FAIL-OPEN: returns True if data unavailable."""
    try:
        cur.execute("""
            SELECT v FROM candles
            WHERE symbol = %s AND tf = '1m'
            ORDER BY ts DESC LIMIT %s
        """, (symbol, window + 1))
        rows = cur.fetchall()
        if not rows or len(rows) < 10:
            return True  # FAIL-OPEN
        volumes = [float(r[0]) for r in rows if r[0] is not None]
        if len(volumes) < 10:
            return True
        current = volumes[0]
        hist = sorted(volumes[1:])
        median = hist[len(hist) // 2]
        if median <= 0:
            return True
        return current >= median * min_ratio
    except Exception:
        return True  # FAIL-OPEN


def build_feature_snapshot(cur, symbol='BTC/USDT:USDT'):
    """Build complete feature snapshot for regime routing.

    Returns dict with all features. Missing features are None.
    Never raises — returns partial snapshot on error.
    """
    vp = _get_vol_profile(cur, symbol)
    price = _get_current_price(cur, symbol)
    drift_score, drift_dir = compute_drift_score(cur, symbol)
    adx = _get_adx(cur, symbol)
    atr_pct = compute_atr_pct(cur, symbol)

    # Structure breakout computation (reuse already-fetched vah/val/atr/price)
    atr_val = (atr_pct * price) if (atr_pct is not None and price is not None) else None
    try:
        from strategy_v3 import config_v3
        sb_n = config_v3.get('strict_breakout_n_candles', 3)
        sb_m = config_v3.get('strict_breakout_m_outside', 2)
        sb_k = config_v3.get('strict_breakout_k_dist', 0.25)
        sb_pct = config_v3.get('strict_breakout_pct_dist', 0.0015)
    except Exception:
        sb_n, sb_m, sb_k, sb_pct = 3, 2, 0.25, 0.0015

    struct_pass, struct_dir, _struct_detail = compute_structure_breakout(
        cur, vp['vah'], vp['val'], atr_val, price, symbol,
        n=sb_n, m=sb_m, k_dist=sb_k, pct_dist=sb_pct)

    return {
        'symbol': symbol,
        'price': price,
        'atr_pct': atr_pct,
        'bb_width': compute_bb_width(cur, symbol),
        'volume_z': compute_volume_z(cur, symbol),
        'impulse': compute_impulse(cur, symbol),
        'adx': adx,
        'poc': vp['poc'],
        'vah': vp['vah'],
        'val': vp['val'],
        'range_position': compute_range_position(price, vp['vah'], vp['val']),
        'drift_score': drift_score,
        'drift_direction': drift_dir,
        'poc_slope': compute_poc_slope(cur, symbol),
        # New mctx fields (Step 9)
        'vol_pct': compute_vol_pct(cur, symbol),
        'trend_strength': compute_trend_strength(adx),
        'range_quality': compute_range_quality(cur, symbol),
        'spread_ok': compute_spread_ok(cur, symbol),
        'liquidity_ok': compute_liquidity_ok(cur, symbol),
        # Strict breakout fields
        'atr_ratio': compute_atr_ratio(cur, symbol),
        'structure_breakout_pass': struct_pass,
        'structure_breakout_dir': struct_dir,
    }
