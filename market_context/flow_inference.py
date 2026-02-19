"""
flow_inference.py — Flow bias and flow shock inference from liquidity data.

Reads liquidity_snapshots + candles from main DB to compute:
  - flow_bias: -100 to +100 weighted composite
  - flow_shock: bool (extreme OI change + vol spike, or extreme funding)
"""
from ctx_utils import _log


def compute_flow(cur, symbol='BTC/USDT:USDT'):
    """Compute flow_bias (-100 to +100) and flow_shock (bool).

    Sources (all from main DB):
      - liquidity_snapshots.oi_value_usdt  -> OI delta  (weight 0.5)
      - liquidity_snapshots.funding_rate   -> Funding bias (weight 0.3)
      - liquidity_snapshots.orderbook_imbalance -> Orderbook bias (weight 0.2)

    Returns:
      {'flow_bias': int, 'flow_shock': bool, 'components': dict}
    """
    components = {}
    oi_score = 0
    funding_score = 0
    ob_score = 0
    flow_shock = False

    # --- OI delta ---
    oi_change_pct = 0
    try:
        cur.execute("""
            SELECT oi_value_usdt FROM liquidity_snapshots
            WHERE symbol = %s
            ORDER BY ts DESC LIMIT 2;
        """, (symbol,))
        rows = cur.fetchall()
        if len(rows) == 2 and rows[1][0] and rows[0][0]:
            oi_now = float(rows[0][0])
            oi_prev = float(rows[1][0])
            if oi_prev > 0:
                oi_change_pct = (oi_now - oi_prev) / oi_prev * 100
                # OI increasing -> bullish bias, decreasing -> bearish
                oi_score = max(-100, min(100, oi_change_pct * 10))
        components['oi_change_pct'] = round(oi_change_pct, 3)
        components['oi_score'] = round(oi_score, 1)
    except Exception as e:
        _log(f'flow OI error: {e}')

    # --- Funding rate ---
    funding_rate = 0
    try:
        cur.execute("""
            SELECT funding_rate FROM liquidity_snapshots
            WHERE symbol = %s AND funding_rate IS NOT NULL
            ORDER BY ts DESC LIMIT 1;
        """, (symbol,))
        row = cur.fetchone()
        if row and row[0]:
            funding_rate = float(row[0])
            # Positive funding -> longs pay shorts -> bearish pressure
            # Negative funding -> shorts pay longs -> bullish pressure
            funding_score = max(-100, min(100, funding_rate * -100000))
        components['funding_rate'] = funding_rate
        components['funding_score'] = round(funding_score, 1)
    except Exception as e:
        _log(f'flow funding error: {e}')

    # --- Orderbook imbalance ---
    ob_imbalance = 0
    try:
        cur.execute("""
            SELECT orderbook_imbalance FROM liquidity_snapshots
            WHERE symbol = %s AND orderbook_imbalance IS NOT NULL
            ORDER BY ts DESC LIMIT 1;
        """, (symbol,))
        row = cur.fetchone()
        if row and row[0]:
            ob_imbalance = float(row[0])
            # imbalance > 0 -> more bids -> bullish, < 0 -> more asks -> bearish
            ob_score = max(-100, min(100, ob_imbalance * 100))
        components['ob_imbalance'] = ob_imbalance
        components['ob_score'] = round(ob_score, 1)
    except Exception as e:
        _log(f'flow orderbook error: {e}')

    # --- Weighted composite ---
    flow_bias = int(round(oi_score * 0.5 + funding_score * 0.3 + ob_score * 0.2))
    flow_bias = max(-100, min(100, flow_bias))

    # --- Shock detection ---
    # Check vol_spike from indicators
    vol_spike = False
    try:
        cur.execute("""
            SELECT vol_spike FROM indicators
            WHERE symbol = %s
            ORDER BY ts DESC LIMIT 1;
        """, (symbol,))
        row = cur.fetchone()
        if row and row[0]:
            vol_spike = bool(row[0])
    except Exception:
        pass

    if abs(oi_change_pct) > 5 and vol_spike:
        flow_shock = True
    # Extreme funding: 0.03% per 8h = 1.37% annualized — genuinely unusual
    if abs(funding_rate) > 0.0003:
        flow_shock = True

    return {
        'flow_bias': flow_bias,
        'flow_shock': flow_shock,
        'components': components,
    }
