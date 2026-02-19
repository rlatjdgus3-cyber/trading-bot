"""
adx_calculator.py — ADX / +DI / -DI calculator using Wilder's smoothing.

Input: list of candles {'h': float, 'l': float, 'c': float} (oldest first).
Output: {'adx': float, 'plus_di': float, 'minus_di': float} or None.
"""


def compute_adx(candles, period=14):
    """Compute ADX, +DI, -DI from OHLCV candles.

    candles: list of dicts with keys 'h', 'l', 'c' (oldest first, min period*3 rows).
    Returns: {'adx': float, 'plus_di': float, 'minus_di': float} or None if insufficient data.
    """
    n = len(candles)
    if n < period + 1:
        return None

    # Step 1: Compute +DM, -DM, TR for each bar
    plus_dm = []
    minus_dm = []
    tr_list = []

    for i in range(1, n):
        high = float(candles[i]['h'])
        low = float(candles[i]['l'])
        prev_high = float(candles[i - 1]['h'])
        prev_low = float(candles[i - 1]['l'])
        prev_close = float(candles[i - 1]['c'])

        up_move = high - prev_high
        down_move = prev_low - low

        pdm = up_move if up_move > down_move and up_move > 0 else 0
        mdm = down_move if down_move > up_move and down_move > 0 else 0

        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))

        plus_dm.append(pdm)
        minus_dm.append(mdm)
        tr_list.append(tr)

    if len(tr_list) < period:
        return None

    # Step 2: Wilder's smoothing (first value = sum of first period)
    smoothed_pdm = sum(plus_dm[:period])
    smoothed_mdm = sum(minus_dm[:period])
    smoothed_tr = sum(tr_list[:period])

    # First +DI / -DI
    plus_di_list = []
    minus_di_list = []
    dx_list = []

    if smoothed_tr > 0:
        pdi = 100 * smoothed_pdm / smoothed_tr
        mdi = 100 * smoothed_mdm / smoothed_tr
    else:
        pdi = 0
        mdi = 0
    plus_di_list.append(pdi)
    minus_di_list.append(mdi)

    di_sum = pdi + mdi
    dx = 100 * abs(pdi - mdi) / di_sum if di_sum > 0 else 0
    dx_list.append(dx)

    # Step 3: Continue Wilder's smoothing for remaining bars
    for i in range(period, len(tr_list)):
        smoothed_pdm = smoothed_pdm - (smoothed_pdm / period) + plus_dm[i]
        smoothed_mdm = smoothed_mdm - (smoothed_mdm / period) + minus_dm[i]
        smoothed_tr = smoothed_tr - (smoothed_tr / period) + tr_list[i]

        if smoothed_tr > 0:
            pdi = 100 * smoothed_pdm / smoothed_tr
            mdi = 100 * smoothed_mdm / smoothed_tr
        else:
            pdi = 0
            mdi = 0
        plus_di_list.append(pdi)
        minus_di_list.append(mdi)

        di_sum = pdi + mdi
        dx = 100 * abs(pdi - mdi) / di_sum if di_sum > 0 else 0
        dx_list.append(dx)

    # Step 4: ADX = Wilder's smoothed DX over period
    if len(dx_list) < period:
        return None

    adx = sum(dx_list[:period]) / period

    for i in range(period, len(dx_list)):
        adx = (adx * (period - 1) + dx_list[i]) / period

    return {
        'adx': round(adx, 2),
        'plus_di': round(plus_di_list[-1], 2),
        'minus_di': round(minus_di_list[-1], 2),
    }


def fetch_candles_for_adx(cur, symbol='BTC/USDT:USDT', limit=60):
    """Fetch recent candles from DB for ADX computation.

    Returns list of {'h', 'l', 'c'} oldest first, or empty list.
    """
    cur.execute("""
        SELECT h, l, c FROM candles
        WHERE symbol = %s AND tf = '1m'
        ORDER BY ts DESC LIMIT %s;
    """, (symbol, limit))
    rows = cur.fetchall()
    if not rows:
        return []
    # Reverse to oldest-first
    return [{'h': float(r[0]), 'l': float(r[1]), 'c': float(r[2])} for r in reversed(rows)]
