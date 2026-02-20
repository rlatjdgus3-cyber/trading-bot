"""
regime_classifier.py — RANGE / BREAKOUT / SHOCK regime classification.

Decision tree (priority order):
  1. SHOCK  — 5m price move >= 2% OR vol_ratio_5m >= 3.0 OR flow_shock
  2. BREAKOUT — 2/3 conditions met (VA breakout + vol + ADX)
  3. RANGE  — default

Reads from: candles, indicators, vol_profile, liquidity_snapshots (via sub-modules).
"""
import json
from ctx_utils import _log
import adx_calculator
import flow_inference


def classify(cur, symbol='BTC/USDT:USDT'):
    """Classify current market regime.

    Returns: {
        'regime': 'RANGE'|'BREAKOUT'|'SHOCK',
        'confidence': 0-100,
        'adx_14', 'plus_di', 'minus_di': float|None,
        'bbw_ratio': float|None,
        'poc', 'vah', 'val': float|None,
        'price_vs_va': str,
        'flow_bias': int, 'flow_shock': bool,
        'shock_type', 'shock_direction': str|None,
        'breakout_confirmed': bool,
        'breakout_conditions': dict,
        'raw_inputs': dict,
    }
    """
    raw = {}

    # --- Current price ---
    cur.execute("""
        SELECT c FROM candles
        WHERE symbol = %s AND tf = '1m'
        ORDER BY ts DESC LIMIT 1;
    """, (symbol,))
    row = cur.fetchone()
    price = float(row[0]) if row and row[0] else 0
    raw['price'] = price

    # --- ADX ---
    candles = adx_calculator.fetch_candles_for_adx(cur, symbol, limit=60)
    adx_result = adx_calculator.compute_adx(candles, period=14)
    adx_14 = adx_result['adx'] if adx_result else None
    plus_di = adx_result['plus_di'] if adx_result else None
    minus_di = adx_result['minus_di'] if adx_result else None
    raw['adx'] = adx_result

    # --- BB width ratio ---
    bbw_ratio = None
    bb_mid = None
    bb_up = None
    bb_dn = None
    try:
        cur.execute("""
            SELECT bb_up, bb_dn, bb_mid FROM indicators
            WHERE symbol = %s
            ORDER BY ts DESC LIMIT 1;
        """, (symbol,))
        bb_row = cur.fetchone()
        if bb_row and bb_row[0] and bb_row[1] and bb_row[2]:
            bb_up = float(bb_row[0])
            bb_dn = float(bb_row[1])
            bb_mid = float(bb_row[2])
            if bb_mid > 0:
                current_bbw = (bb_up - bb_dn) / bb_mid

                # Get historical BBW average (20 periods)
                cur.execute("""
                    SELECT bb_up, bb_dn, bb_mid FROM indicators
                    WHERE symbol = %s AND bb_up IS NOT NULL
                    ORDER BY ts DESC LIMIT 20;
                """, (symbol,))
                hist_rows = cur.fetchall()
                if len(hist_rows) >= 5:
                    bbw_vals = []
                    for hr in hist_rows:
                        if hr[0] and hr[1] and hr[2] and float(hr[2]) > 0:
                            bbw_vals.append((float(hr[0]) - float(hr[1])) / float(hr[2]))
                    avg_bbw = sum(bbw_vals) / len(bbw_vals) if bbw_vals else current_bbw
                    bbw_ratio = current_bbw / avg_bbw if avg_bbw > 0 else 1.0
                else:
                    bbw_ratio = 1.0
        raw['bbw_ratio'] = bbw_ratio
    except Exception as e:
        _log(f'BBW error: {e}')

    # v14.1: compute absolute bb_width_pct once (used in all regime returns)
    bb_width_pct = ((bb_up - bb_dn) / bb_mid * 100) if (bb_mid and bb_mid > 0 and bb_up is not None and bb_dn is not None) else None

    # --- Volume Profile ---
    poc = vah = val = None
    price_vs_va = 'INSIDE'
    try:
        cur.execute("""
            SELECT poc, vah, val FROM vol_profile
            WHERE symbol = %s
            ORDER BY ts DESC LIMIT 1;
        """, (symbol,))
        vp_row = cur.fetchone()
        if vp_row:
            poc = float(vp_row[0]) if vp_row[0] else None
            vah = float(vp_row[1]) if vp_row[1] else None
            val = float(vp_row[2]) if vp_row[2] else None
            if price and vah and val:
                if price > vah:
                    price_vs_va = 'ABOVE_VAH'
                elif price < val:
                    price_vs_va = 'BELOW_VAL'
                else:
                    price_vs_va = 'INSIDE'
        raw['vol_profile'] = {'poc': poc, 'vah': vah, 'val': val, 'price_vs_va': price_vs_va}
    except Exception as e:
        _log(f'VP error: {e}')

    # --- Flow ---
    flow = flow_inference.compute_flow(cur, symbol)
    flow_bias = flow['flow_bias']
    flow_shock = flow['flow_shock']
    raw['flow'] = flow

    # --- 5m price move ---
    ret_5m = 0
    try:
        cur.execute("""
            SELECT c FROM candles
            WHERE symbol = %s AND tf = '1m'
            ORDER BY ts DESC LIMIT 5;
        """, (symbol,))
        c5_rows = cur.fetchall()
        if len(c5_rows) >= 5:
            now_px = float(c5_rows[0][0])
            ago_px = float(c5_rows[4][0])
            if ago_px > 0 and now_px > 0:
                ret_5m = (now_px - ago_px) / ago_px * 100
                # Sanity cap: bad data can't produce absurd returns
                ret_5m = max(-50, min(50, ret_5m))
        raw['ret_5m'] = round(ret_5m, 4)
    except Exception as e:
        _log(f'ret_5m error: {e}')

    # --- Volume ratio (5m) ---
    vol_ratio_5m = 1.0
    try:
        cur.execute("""
            SELECT vol, vol_ma20 FROM indicators
            WHERE symbol = %s
            ORDER BY ts DESC LIMIT 1;
        """, (symbol,))
        vr_row = cur.fetchone()
        if vr_row and vr_row[0] and vr_row[1] and float(vr_row[1]) > 0:
            vol_ratio_5m = float(vr_row[0]) / float(vr_row[1])
        raw['vol_ratio_5m'] = round(vol_ratio_5m, 2)
    except Exception as e:
        _log(f'vol_ratio error: {e}')

    # --- 5m candle check for breakout (2 consecutive 5m closes outside VA) ---
    breakout_va_confirmed = False
    breakout_direction = None
    try:
        cur.execute("""
            SELECT c FROM candles
            WHERE symbol = %s AND tf = '5m'
            ORDER BY ts DESC LIMIT 3;
        """, (symbol,))
        c5m_rows = cur.fetchall()
        if len(c5m_rows) >= 2 and vah and val:
            close_0 = float(c5m_rows[0][0])
            close_1 = float(c5m_rows[1][0])
            # 2 consecutive 5m closes above VAH or below VAL
            if close_0 > vah and close_1 > vah:
                breakout_va_confirmed = True
                breakout_direction = 'UP'
            elif close_0 < val and close_1 < val:
                breakout_va_confirmed = True
                breakout_direction = 'DOWN'
        raw['breakout_va_confirmed'] = breakout_va_confirmed
        raw['breakout_direction'] = breakout_direction
    except Exception as e:
        _log(f'breakout VA check error: {e}')

    # ========== CLASSIFICATION ==========

    # 1. SHOCK (highest priority)
    if abs(ret_5m) >= 2.0 or vol_ratio_5m >= 3.0 or flow_shock:
        # Direction: use price move if significant, else flow_bias, else None
        if abs(ret_5m) >= 0.5:
            shock_direction = 'UP' if ret_5m > 0 else 'DOWN'
        elif flow_bias > 20:
            shock_direction = 'UP'
        elif flow_bias < -20:
            shock_direction = 'DOWN'
        else:
            shock_direction = None
        shock_type = _classify_shock_type(ret_5m, flow_bias, breakout_direction)
        confidence = min(100, int(abs(ret_5m) * 20 + vol_ratio_5m * 10))
        confidence = max(confidence, 60)

        return {
            'regime': 'SHOCK',
            'confidence': confidence,
            'adx_14': adx_14, 'plus_di': plus_di, 'minus_di': minus_di,
            'bbw_ratio': bbw_ratio,
            'bb_width_pct': bb_width_pct,
            'poc': poc, 'vah': vah, 'val': val,
            'price_vs_va': price_vs_va,
            'flow_bias': flow_bias, 'flow_shock': flow_shock,
            'shock_type': shock_type,
            'shock_direction': shock_direction,
            'breakout_confirmed': breakout_va_confirmed,
            'breakout_conditions': {},
            'raw_inputs': raw,
        }

    # 2. BREAKOUT (2/3 conditions)
    bo_conditions = {}
    bo_count = 0

    # Condition A: VA breakout confirmed (3 x 5m blocks)
    cond_a = breakout_va_confirmed
    bo_conditions['va_breakout'] = cond_a
    if cond_a:
        bo_count += 1

    # Condition B: vol_ratio >= 1.8
    cond_b = vol_ratio_5m >= 1.8
    bo_conditions['vol_ratio'] = {'value': round(vol_ratio_5m, 2), 'met': cond_b}
    if cond_b:
        bo_count += 1

    # Condition C: BB width expanding (bbw_ratio > 1.2) OR EMA alignment (9>21>50 or 9<21<50)
    bbw_expanding = bbw_ratio is not None and bbw_ratio > 1.2
    ema_aligned = False
    try:
        cur.execute("""
            SELECT ema_9, ema_21, ema_50 FROM indicators
            WHERE symbol = %s AND ema_9 IS NOT NULL
            ORDER BY ts DESC LIMIT 1;
        """, (symbol,))
        ema_row = cur.fetchone()
        if ema_row and ema_row[0] is not None and ema_row[1] is not None and ema_row[2] is not None:
            e9, e21, e50 = float(ema_row[0]), float(ema_row[1]), float(ema_row[2])
            ema_aligned = (e9 > e21 > e50) or (e9 < e21 < e50)
    except Exception:
        pass
    cond_c = bbw_expanding or ema_aligned
    bo_conditions['bbw_or_ema'] = {
        'bbw_expanding': bbw_expanding,
        'ema_aligned': ema_aligned,
        'met': cond_c,
    }
    if cond_c:
        bo_count += 1

    if bo_count >= 2:
        confidence = min(100, 50 + bo_count * 15)
        if ema_aligned and bbw_expanding:
            confidence = min(100, confidence + 10)

        return {
            'regime': 'BREAKOUT',
            'confidence': confidence,
            'adx_14': adx_14, 'plus_di': plus_di, 'minus_di': minus_di,
            'bbw_ratio': bbw_ratio,
            'bb_width_pct': bb_width_pct,
            'poc': poc, 'vah': vah, 'val': val,
            'price_vs_va': price_vs_va,
            'flow_bias': flow_bias, 'flow_shock': flow_shock,
            'shock_type': None,
            'shock_direction': None,
            'breakout_confirmed': True,
            'breakout_conditions': bo_conditions,
            'raw_inputs': raw,
        }

    # 3. RANGE (default)
    confidence = 50
    # v14.1: tightened RANGE thresholds — ADX < 18 + bb_width_pct < 1.2 → high confidence
    if adx_14 is not None and adx_14 < 18 and bb_width_pct is not None and bb_width_pct < 1.2:
        if price_vs_va == 'INSIDE':
            confidence = 90
        else:
            confidence = 75
    elif adx_14 is not None and adx_14 < 20 and bbw_ratio is not None and bbw_ratio < 1.0:
        if price_vs_va == 'INSIDE':
            confidence = 85
        else:
            confidence = 65
    elif adx_14 is not None and adx_14 < 25:
        confidence = 55

    return {
        'regime': 'RANGE',
        'confidence': confidence,
        'adx_14': adx_14, 'plus_di': plus_di, 'minus_di': minus_di,
        'bbw_ratio': bbw_ratio,
        'bb_width_pct': bb_width_pct,
        'poc': poc, 'vah': vah, 'val': val,
        'price_vs_va': price_vs_va,
        'flow_bias': flow_bias, 'flow_shock': flow_shock,
        'shock_type': None,
        'shock_direction': None,
        'breakout_confirmed': False,
        'breakout_conditions': bo_conditions,
        'raw_inputs': raw,
    }


def _classify_shock_type(ret_5m, flow_bias, breakout_direction):
    """Determine SHOCK sub-type.

    VETO:      extreme move (>= 3%), all entries blocked
    ACCEL:     trend-aligned shock, allow continuation
    RISK_DOWN: moderate shock, reduce risk
    """
    if abs(ret_5m) >= 3.0:
        return 'VETO'

    # Check if flow aligns with breakout direction
    if breakout_direction:
        if breakout_direction == 'UP' and flow_bias > 20:
            return 'ACCEL'
        if breakout_direction == 'DOWN' and flow_bias < -20:
            return 'ACCEL'

    return 'RISK_DOWN'
