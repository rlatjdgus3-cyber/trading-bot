"""
position_scorer.py — Position context scorer.

Score: -100 (strongly suggests reducing/closing) to +100 (strongly confirms direction).
Returns 0 when no position exists.

Components:
  - pnl_distance:    How far in profit/loss relative to ATR
  - stage_util:      How much of the 7-stage budget is used
  - stop_distance:   Proximity to stop-loss level
  - side_alignment:  Whether tech score aligns with position side
"""
import os
import sys
import traceback
sys.path.insert(0, '/root/trading-bot/app')
LOG_PREFIX = '[pos_scorer]'
SYMBOL = 'BTC/USDT:USDT'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _score_pnl_distance(pos=None, atr=None):
    '''Score based on PnL relative to ATR: -30 to +30.
    In profit -> positive (confirms direction).
    In loss -> negative (suggests risk).'''
    upnl = pos.get('upnl', 0)
    entry = pos.get('entry_price', 0)
    price = pos.get('mark_price', 0)
    if not atr or atr <= 0 or not entry:
        return 0
    if pos.get('side') == 'long':
        move = price - entry
    else:
        move = entry - price
    atr_multiple = move / atr
    clamped = max(-3, min(3, atr_multiple))
    return int(round((clamped / 3) * 30))


def _score_stage_util(stage=None, budget_used_pct=None):
    '''Score based on stage utilization: -20 to +20.
    Low utilization with room to grow -> positive (can add).
    High utilization -> negative (near capacity, more risk).'''
    if stage == 0:
        return 0
    util = budget_used_pct / 70
    util = max(0, min(1, util))
    if util < 0.3:
        return 15
    if util < 0.5:
        return 8
    if util < 0.7:
        return 0
    if util < 0.9:
        return -10
    return -20


def _score_stop_distance(pos=None, atr=None, stop_loss_pct=None):
    '''Score based on distance to stop-loss: -25 to +25.
    Far from SL -> positive.
    Close to SL -> negative.'''
    entry = pos.get('entry_price', 0)
    price = pos.get('mark_price', 0)
    if not entry or not price or entry <= 0:
        return 0
    if pos.get('side') == 'long':
        distance_pct = ((price - entry) / entry) * 100
    else:
        distance_pct = ((entry - price) / entry) * 100
    sl_proximity = (distance_pct + stop_loss_pct) / stop_loss_pct
    clamped = max(0, min(2, sl_proximity))
    return int(round((clamped - 1) * 25))


def _score_side_alignment(pos_side=None, tech_score=None):
    '''Score based on alignment with tech score: -25 to +25.
    Tech agrees with position -> positive.
    Tech disagrees -> negative.'''
    if tech_score == 0:
        return 0
    if pos_side == 'long':
        alignment = tech_score / 100
    else:
        alignment = -tech_score / 100
    return int(round(alignment * 25))


def compute(cur=None, tech_score=None):
    '''Compute position context score.

    Args:
        cur: Database cursor
        tech_score: Current technical score for alignment check

    Returns:
        {
            "score": int (-100 to +100),
            "components": {
                "pnl_distance": int,
                "stage_util": int,
                "stop_distance": int,
                "side_alignment": int,
            },
            "has_position": bool,
        }
    '''
    no_position_result = {
        'score': 0,
        'components': {
            'pnl_distance': 0,
            'stage_util': 0,
            'stop_distance': 0,
            'side_alignment': 0},
        'has_position': False}

    try:
        cur.execute("""
            SELECT side, total_qty, avg_entry_price, stage,
                   trade_budget_used_pct
            FROM position_state
            WHERE symbol = %s;
        """, (SYMBOL,))
        row = cur.fetchone()

        if not row or not row[0] or float(row[1] or 0) == 0:
            return no_position_result

        side = row[0]
        qty = float(row[1])
        avg_entry = float(row[2]) if row[2] else 0
        stage = int(row[3]) if row[3] else 0
        budget_pct = float(row[4]) if row[4] else 0

        # Fetch ATR
        cur.execute("""
            SELECT atr_14 FROM indicators
            WHERE symbol = %s AND tf = '1m'
            ORDER BY ts DESC LIMIT 1;
        """, (SYMBOL,))
        ind_row = cur.fetchone()
        atr = float(ind_row[0]) if ind_row and ind_row[0] else None

        # Fetch current price
        cur.execute("""
            SELECT c FROM candles
            WHERE symbol = %s AND tf = '1m'
            ORDER BY ts DESC LIMIT 1;
        """, (SYMBOL,))
        price_row = cur.fetchone()
        mark_price = float(price_row[0]) if price_row and price_row[0] else 0

        # Fetch stop-loss pct
        cur.execute('SELECT stop_loss_pct FROM safety_limits ORDER BY id DESC LIMIT 1;')
        sl_row = cur.fetchone()
        sl_pct = float(sl_row[0]) if sl_row and sl_row[0] else 2.0

        pos = {
            'side': side,
            'qty': qty,
            'entry_price': avg_entry,
            'mark_price': mark_price,
            'upnl': 0}

        pnl_d = _score_pnl_distance(pos, atr)
        stage_u = _score_stage_util(stage, budget_pct)
        stop_d = _score_stop_distance(pos, atr, sl_pct)
        align = _score_side_alignment(side, tech_score)

        total = pnl_d + stage_u + stop_d + align
        total = max(-100, min(100, total))

        return {
            'score': total,
            'components': {
                'pnl_distance': pnl_d,
                'stage_util': stage_u,
                'stop_distance': stop_d,
                'side_alignment': align},
            'has_position': True}

    except Exception:
        traceback.print_exc()
        return no_position_result


if __name__ == '__main__':
    import json
    import psycopg2
    conn = None
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', '5432')),
            dbname=os.getenv('DB_NAME', 'trading'),
            user=os.getenv('DB_USER', 'bot'),
            password=os.getenv('DB_PASS', 'botpass'),
            connect_timeout=10,
            options='-c statement_timeout=30000')
        conn.autocommit = True
        with conn.cursor() as cur:
            result = compute(cur, tech_score=0)
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    except Exception:
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
