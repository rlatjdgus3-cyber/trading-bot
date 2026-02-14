"""
market_snapshot.py — Real-time market snapshot build + validation.

Extracted from telegram_cmd_poller._refresh_market_snapshot() and made
into a standalone module used by event_trigger, position_manager, and
telegram_cmd_poller.
"""
import os
import sys
import time

sys.path.insert(0, '/root/trading-bot/app')

LOG_PREFIX = '[market_snapshot]'

SYMBOL = os.getenv('SYMBOL', 'BTC/USDT:USDT')
TF = '1m'

# ── validation constants ───────────────────────────────────
SNAPSHOT_MAX_AGE_SEC = 60
MIN_CANDLE_COUNT = 100
PRICE_DEVIATION_PCT = 40
EXECUTION_PRICE_TOLERANCE_PCT = 0.3


class SnapshotError(Exception):
    pass


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _sma(xs):
    return sum(xs) / len(xs) if xs else 0


def build_snapshot(exchange, cur, symbol=None) -> dict:
    """Build real-time market snapshot.

    1. fetch_ticker() → current price
    2. fetch_ohlcv(limit=120) → 1m candles → DB upsert
    3. Indicator calculations: BB(20,2), Ichimoku(9/26/52), RSI(14), ATR(14), MA50/200
    4. DB: 24h high/low, vol_profile(POC/VAH/VAL)
    5. ret_1m, ret_5m, ret_15m return calculations
    6. snapshot_ts = time.time()
    """
    sym = symbol or SYMBOL

    # 1. Realtime ticker
    ticker = exchange.fetch_ticker(sym)
    price = float(ticker.get('last') or ticker.get('close') or 0)
    if price <= 0:
        raise SnapshotError('ticker price <= 0')

    # 2. Fetch recent 1m candles and upsert
    ohlcv = exchange.fetch_ohlcv(sym, TF, limit=120)
    if ohlcv:
        for ms, o, h, l, c, v in ohlcv:
            cur.execute("""
                INSERT INTO candles(symbol, tf, ts, o, h, l, c, v)
                VALUES (%s, %s, to_timestamp(%s/1000.0), %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, tf, ts) DO UPDATE
                SET o=EXCLUDED.o, h=EXCLUDED.h, l=EXCLUDED.l,
                    c=EXCLUDED.c, v=EXCLUDED.v
            """, (sym, TF, ms, o, h, l, c, v))

    # 3. Read last 300 candles from DB
    cur.execute("""
        SELECT ts, o, h, l, c, v
        FROM candles
        WHERE symbol = %s AND tf = %s
        ORDER BY ts DESC LIMIT 300
    """, (sym, TF))
    rows = cur.fetchall()
    candle_count = len(rows)

    if candle_count < 52:
        raise SnapshotError(f'not enough candles ({candle_count})')

    rows = list(reversed(rows))  # ASC order
    closes = [float(r[4]) for r in rows]
    highs = [float(r[2]) for r in rows]
    lows = [float(r[3]) for r in rows]
    vols = [float(r[5]) for r in rows]

    # ── Indicator calculations ─────────────────────────────
    # BB (20, 2σ)
    bb_win = closes[-20:]
    bb_mid = _sma(bb_win)
    bb_var = sum((x - bb_mid) ** 2 for x in bb_win) / 20
    bb_sd = bb_var ** 0.5
    bb_upper = bb_mid + 2 * bb_sd
    bb_lower = bb_mid - 2 * bb_sd

    # Ichimoku (9/26/52)
    tenkan = (max(highs[-9:]) + min(lows[-9:])) / 2
    kijun = (max(highs[-26:]) + min(lows[-26:])) / 2
    span_a = (tenkan + kijun) / 2
    span_b = (max(highs[-52:]) + min(lows[-52:])) / 2
    cloud_top = max(span_a, span_b)
    cloud_bot = min(span_a, span_b)
    if price > cloud_top:
        cloud_position = 'above'
    elif price < cloud_bot:
        cloud_position = 'below'
    else:
        cloud_position = 'inside'

    # Volume
    vol_last = vols[-1]
    vol_ma20 = _sma(vols[-20:])
    vol_ratio = vol_last / vol_ma20 if vol_ma20 > 0 else 1.0

    # RSI (14)
    rsi_14 = None
    if len(closes) >= 15:
        deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
        recent = deltas[-14:]
        gains = [d if d > 0 else 0 for d in recent]
        losses = [-d if d < 0 else 0 for d in recent]
        avg_gain = sum(gains) / 14
        avg_loss = sum(losses) / 14
        if avg_loss > 0:
            rs = avg_gain / avg_loss
            rsi_14 = 100 - 100 / (1 + rs)
        else:
            rsi_14 = 100.0

    # ATR (14)
    atr_14 = None
    if len(closes) >= 15:
        trs = []
        for i in range(-14, 0):
            hi = highs[i]
            lo = lows[i]
            prev_c = closes[i - 1]
            tr = max(hi - lo, abs(hi - prev_c), abs(lo - prev_c))
            trs.append(tr)
        atr_14 = sum(trs) / 14

    # MA 50 / 200
    ma_50 = _sma(closes[-50:]) if len(closes) >= 50 else None
    ma_200 = _sma(closes[-200:]) if len(closes) >= 200 else None

    # Upsert indicators
    ts_last = rows[-1][0]
    cur.execute("""
        INSERT INTO indicators (
            symbol, tf, ts,
            bb_mid, bb_up, bb_dn,
            ich_tenkan, ich_kijun, ich_span_a, ich_span_b,
            vol, vol_ma20, vol_spike,
            rsi_14, atr_14, ma_50, ma_200
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (symbol, tf, ts) DO UPDATE SET
            bb_mid=EXCLUDED.bb_mid, bb_up=EXCLUDED.bb_up, bb_dn=EXCLUDED.bb_dn,
            ich_tenkan=EXCLUDED.ich_tenkan, ich_kijun=EXCLUDED.ich_kijun,
            ich_span_a=EXCLUDED.ich_span_a, ich_span_b=EXCLUDED.ich_span_b,
            vol=EXCLUDED.vol, vol_ma20=EXCLUDED.vol_ma20, vol_spike=EXCLUDED.vol_spike,
            rsi_14=EXCLUDED.rsi_14, atr_14=EXCLUDED.atr_14,
            ma_50=EXCLUDED.ma_50, ma_200=EXCLUDED.ma_200
    """, (
        sym, TF, ts_last,
        bb_mid, bb_upper, bb_lower,
        tenkan, kijun, span_a, span_b,
        vol_last, vol_ma20, vol_last > vol_ma20 * 2,
        rsi_14, atr_14, ma_50, ma_200,
    ))

    # 4. 24h high/low from DB
    high_24h = None
    low_24h = None
    try:
        cur.execute("""
            SELECT MIN(l), MAX(h)
            FROM candles
            WHERE symbol = %s AND tf = '1m'
              AND ts > now() - interval '24 hours';
        """, (sym,))
        hl_row = cur.fetchone()
        if hl_row and hl_row[0]:
            low_24h = float(hl_row[0])
            high_24h = float(hl_row[1])
    except Exception:
        pass

    # Vol profile (POC/VAH/VAL)
    poc, vah, val, vol_profile_ts = None, None, None, None
    try:
        cur.execute("""
            SELECT poc, vah, val, ts FROM vol_profile
            WHERE symbol = %s ORDER BY ts DESC LIMIT 1;
        """, (sym,))
        vp_row = cur.fetchone()
        if vp_row:
            poc = float(vp_row[0]) if vp_row[0] else None
            vah = float(vp_row[1]) if vp_row[1] else None
            val = float(vp_row[2]) if vp_row[2] else None
            vol_profile_ts = vp_row[3].timestamp() if vp_row[3] else None
    except Exception:
        pass

    # 5. Return calculations (ret_1m, ret_5m, ret_15m)
    ret_1m = None
    ret_5m = None
    ret_15m = None
    if len(closes) >= 2:
        ret_1m = (closes[-1] - closes[-2]) / closes[-2] * 100
    if len(closes) >= 6:
        ret_5m = (closes[-1] - closes[-6]) / closes[-6] * 100
    if len(closes) >= 16:
        ret_15m = (closes[-1] - closes[-16]) / closes[-16] * 100

    # 15m bar individual returns (3 bars)
    bar_15m_returns = []
    if len(closes) >= 46:
        bar1 = (closes[-1] - closes[-16]) / closes[-16] * 100    # 최근 15분
        bar2 = (closes[-16] - closes[-31]) / closes[-31] * 100   # 직전 15분
        bar3 = (closes[-31] - closes[-46]) / closes[-46] * 100   # 그 전 15분
        bar_15m_returns = [round(bar1, 4), round(bar2, 4), round(bar3, 4)]

    # Recent candles for context (last 10)
    candles_1m = [
        {'ts': str(r[0]), 'o': float(r[1]), 'h': float(r[2]),
         'l': float(r[3]), 'c': float(r[4]), 'v': float(r[5])}
        for r in reversed(rows[-10:])
    ]

    snapshot_ts = time.time()

    _log(f'built: price=${price:,.1f} rsi={rsi_14} atr={atr_14} '
         f'candles={candle_count} ret_1m={ret_1m}')

    return {
        'price': price,
        'snapshot_ts': snapshot_ts,
        'candle_count': candle_count,
        'high_24h': high_24h,
        'low_24h': low_24h,
        'bb_mid': bb_mid,
        'bb_upper': bb_upper,
        'bb_lower': bb_lower,
        'kijun': kijun,
        'tenkan': tenkan,
        'cloud_top': cloud_top,
        'cloud_bot': cloud_bot,
        'cloud_position': cloud_position,
        'atr_14': atr_14,
        'rsi_14': rsi_14,
        'ma_50': ma_50,
        'ma_200': ma_200,
        'vol_last': vol_last,
        'vol_ma20': vol_ma20,
        'vol_ratio': vol_ratio,
        'poc': poc,
        'vah': vah,
        'val': val,
        'vol_profile_ts': vol_profile_ts,
        'candles_1m': candles_1m,
        'returns': {
            'ret_1m': round(ret_1m, 4) if ret_1m is not None else None,
            'ret_5m': round(ret_5m, 4) if ret_5m is not None else None,
            'ret_15m': round(ret_15m, 4) if ret_15m is not None else None,
        },
        'bar_15m_returns': bar_15m_returns,
    }


def validate_snapshot(snapshot) -> tuple:
    """Validate snapshot: ts within 60s, price not None, candle_count >= 100.
    Returns (ok: bool, reason: str).
    """
    if not snapshot:
        return (False, 'snapshot is None')

    price = snapshot.get('price')
    if not price or price <= 0:
        return (False, 'price is None or <= 0')

    ts = snapshot.get('snapshot_ts', 0)
    age = time.time() - ts
    if age > SNAPSHOT_MAX_AGE_SEC:
        return (False, f'snapshot too old ({age:.0f}s > {SNAPSHOT_MAX_AGE_SEC}s)')

    count = snapshot.get('candle_count', 0)
    if count < MIN_CANDLE_COUNT:
        return (False, f'not enough candles ({count} < {MIN_CANDLE_COUNT})')

    return (True, 'ok')


def build_and_validate(exchange, cur, symbol=None) -> dict:
    """Build + validate. Raises SnapshotError on failure."""
    snapshot = build_snapshot(exchange, cur, symbol)
    ok, reason = validate_snapshot(snapshot)
    if not ok:
        raise SnapshotError(reason)
    return snapshot


def validate_price_mention(mentioned_price, snapshot) -> tuple:
    """Check if a price mentioned in Claude's response is within ±PRICE_DEVIATION_PCT
    of the current snapshot price. Returns (ok: bool, reason: str).
    """
    current = snapshot.get('price', 0)
    if not current or current <= 0:
        return (False, 'no current price in snapshot')
    if not mentioned_price or mentioned_price <= 0:
        return (True, 'no price mentioned')  # no price to validate

    deviation = abs(mentioned_price - current) / current * 100
    if deviation > PRICE_DEVIATION_PCT:
        return (False, f'price ${mentioned_price:,.1f} deviates {deviation:.1f}% '
                       f'from current ${current:,.1f} (limit {PRICE_DEVIATION_PCT}%)')

    return (True, 'ok')


def validate_execution_ready(snapshot, ticker_price) -> tuple:
    """Pre-execution validation: snapshot within 60s + price within ±0.3% of ticker.
    Returns (ok: bool, reason: str).
    """
    ok, reason = validate_snapshot(snapshot)
    if not ok:
        return (False, reason)

    if not ticker_price or ticker_price <= 0:
        return (True, 'no ticker price to compare')

    snap_price = snapshot.get('price', 0)
    if snap_price <= 0:
        return (False, 'snapshot price <= 0')

    deviation = abs(snap_price - ticker_price) / ticker_price * 100
    if deviation > EXECUTION_PRICE_TOLERANCE_PCT:
        return (False, f'snapshot price ${snap_price:,.1f} deviates {deviation:.2f}% '
                       f'from ticker ${ticker_price:,.1f} (limit {EXECUTION_PRICE_TOLERANCE_PCT}%)')

    return (True, 'ok')
