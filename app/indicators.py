"""
indicators.py — Technical indicator calculator daemon.
Runs every ~15s: fetches latest 300 1m candles, calculates indicators, upserts to DB.
"""
import os
import time
import psycopg2
from db_config import get_conn

# =========================
# 기본 설정
# =========================
symbol = os.getenv("SYMBOL", "BTC/USDT:USDT")
tf = os.getenv("TF", "1m")

def sma(xs):
    return sum(xs) / len(xs)

def hh(xs):
    return max(xs)

def ll(xs):
    return min(xs)

def ema(xs, period):
    if len(xs) < period:
        return None
    multiplier = 2 / (period + 1)
    ema_val = sma(xs[:period])
    for price in xs[period:]:
        ema_val = (price - ema_val) * multiplier + ema_val
    return ema_val


def _resample_candles(candles_1m, target_tf_minutes):
    """Resample 1m candles to target timeframe.
    candles_1m: list of (ts, o, h, l, c, v) sorted ASC by ts.
    Returns list of (ts, o, h, l, c, v) for target tf."""
    if not candles_1m or target_tf_minutes <= 1:
        return candles_1m
    result = []
    group = []
    for candle in candles_1m:
        group.append(candle)
        if len(group) >= target_tf_minutes:
            ts_first = group[0][0]
            o_first = float(group[0][1])
            h_max = max(float(g[2]) for g in group)
            l_min = min(float(g[3]) for g in group)
            c_last = float(group[-1][4])
            v_sum = sum(float(g[5]) for g in group)
            result.append((ts_first, o_first, h_max, l_min, c_last, v_sum))
            group = []
    return result


def _compute_adx(highs, lows, closes, period=14):
    """Compute ADX from high/low/close arrays."""
    if len(highs) < period * 2 + 1:
        return None
    n = len(highs)
    plus_dm = []
    minus_dm = []
    tr_list = []
    for i in range(1, n):
        h_diff = highs[i] - highs[i-1]
        l_diff = lows[i-1] - lows[i]
        plus_dm.append(h_diff if h_diff > l_diff and h_diff > 0 else 0)
        minus_dm.append(l_diff if l_diff > h_diff and l_diff > 0 else 0)
        tr = max(highs[i] - lows[i],
                 abs(highs[i] - closes[i-1]),
                 abs(lows[i] - closes[i-1]))
        tr_list.append(tr)

    if len(tr_list) < period:
        return None

    # Smoothed averages (Wilder's smoothing)
    atr = sum(tr_list[:period]) / period
    plus_di_smooth = sum(plus_dm[:period]) / period
    minus_di_smooth = sum(minus_dm[:period]) / period

    dx_list = []
    for i in range(period, len(tr_list)):
        atr = (atr * (period - 1) + tr_list[i]) / period
        plus_di_smooth = (plus_di_smooth * (period - 1) + plus_dm[i]) / period
        minus_di_smooth = (minus_di_smooth * (period - 1) + minus_dm[i]) / period

        if atr > 0:
            plus_di = 100 * plus_di_smooth / atr
            minus_di = 100 * minus_di_smooth / atr
        else:
            plus_di = 0
            minus_di = 0

        di_sum = plus_di + minus_di
        if di_sum > 0:
            dx = 100 * abs(plus_di - minus_di) / di_sum
        else:
            dx = 0
        dx_list.append(dx)

    if len(dx_list) < period:
        return None

    adx = sum(dx_list[:period]) / period
    for i in range(period, len(dx_list)):
        adx = (adx * (period - 1) + dx_list[i]) / period

    return adx


def _compute_donchian(highs, lows, period=20):
    """Compute Donchian Channel high/low."""
    if len(highs) < period:
        return None, None
    return max(highs[-period:]), min(lows[-period:])


_mtf_last_compute = 0
_MTF_INTERVAL_SEC = 60

print("=== INDICATOR ENGINE STARTED ===", flush=True)

from watchdog_helper import init_watchdog
init_watchdog(interval_sec=10)

db = get_conn(autocommit=True)

while True:
    try:
        with db.cursor() as cur:
            # 최신 캔들만 조회
            cur.execute(
                """
                SELECT ts, o, h, l, c, v
                FROM candles
                WHERE symbol=%s AND tf=%s
                ORDER BY ts DESC
                LIMIT 300
                """,
                (symbol, tf),
            )
            rows = cur.fetchall()

        if len(rows) < 120:
            print("Waiting candles:", len(rows), flush=True)
            time.sleep(10)
            continue

        # DESC → ASC
        rows = list(reversed(rows))

        ts, o, h, l, c, v = rows[-1]

        closes = [float(r[4]) for r in rows]
        highs  = [float(r[2]) for r in rows]
        lows   = [float(r[3]) for r in rows]
        vols   = [float(r[5]) for r in rows]

        # Bollinger Bands (20, 2)
        n = 20
        win = closes[-n:]
        mid = sma(win)
        var = sum((x - mid) ** 2 for x in win) / n
        sd = var ** 0.5
        up = mid + 2 * sd
        dn = mid - 2 * sd

        # Ichimoku (9, 26, 52)
        tenkan = (hh(highs[-9:]) + ll(lows[-9:])) / 2
        kijun  = (hh(highs[-26:]) + ll(lows[-26:])) / 2
        span_a = (tenkan + kijun) / 2
        span_b = (hh(highs[-52:]) + ll(lows[-52:])) / 2

        # Volume
        vol = vols[-1]
        vol_ma20 = sma(vols[-20:])
        vol_spike = vol > vol_ma20 * 2

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
        ma_50 = sma(closes[-50:]) if len(closes) >= 50 else None
        ma_200 = sma(closes[-200:]) if len(closes) >= 200 else None

        # EMA (9/21/50)
        ema_9 = ema(closes, 9)
        ema_21 = ema(closes, 21)
        ema_50 = ema(closes, 50) if len(closes) >= 50 else None

        # VWAP (UTC 00:00 intraday reset)
        vwap_val = None
        try:
            from datetime import datetime, timezone
            utc_today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            cum_vp = 0.0
            cum_vol = 0.0
            for r in rows:
                row_ts = r[0]
                if hasattr(row_ts, 'astimezone'):
                    row_ts_utc = row_ts.astimezone(timezone.utc)
                else:
                    row_ts_utc = row_ts.replace(tzinfo=timezone.utc)
                if row_ts_utc >= utc_today:
                    typical = (float(r[2]) + float(r[3]) + float(r[4])) / 3
                    vol_r = float(r[5])
                    cum_vp += typical * vol_r
                    cum_vol += vol_r
            if cum_vol > 0:
                vwap_val = cum_vp / cum_vol
        except Exception:
            pass

        # =========================
        # indicators 저장
        # =========================
        with db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO indicators (
                    symbol, tf, ts,
                    bb_mid, bb_up, bb_dn,
                    ich_tenkan, ich_kijun,
                    ich_span_a, ich_span_b,
                    vol, vol_ma20, vol_spike,
                    rsi_14, atr_14, ma_50, ma_200,
                    ema_9, ema_21, ema_50, vwap
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (symbol, tf, ts) DO UPDATE SET
                    bb_mid=EXCLUDED.bb_mid, bb_up=EXCLUDED.bb_up, bb_dn=EXCLUDED.bb_dn,
                    ich_tenkan=EXCLUDED.ich_tenkan, ich_kijun=EXCLUDED.ich_kijun,
                    ich_span_a=EXCLUDED.ich_span_a, ich_span_b=EXCLUDED.ich_span_b,
                    vol=EXCLUDED.vol, vol_ma20=EXCLUDED.vol_ma20, vol_spike=EXCLUDED.vol_spike,
                    rsi_14=EXCLUDED.rsi_14, atr_14=EXCLUDED.atr_14,
                    ma_50=EXCLUDED.ma_50, ma_200=EXCLUDED.ma_200,
                    ema_9=EXCLUDED.ema_9, ema_21=EXCLUDED.ema_21,
                    ema_50=EXCLUDED.ema_50, vwap=EXCLUDED.vwap
                """,
                (
                    symbol, tf, ts,
                    mid, up, dn,
                    tenkan, kijun,
                    span_a, span_b,
                    vol, vol_ma20, vol_spike,
                    rsi_14, atr_14, ma_50, ma_200,
                    ema_9, ema_21, ema_50, vwap_val
                ),
            )

        print(
            f"Saved indicators @ {ts} rsi={rsi_14} atr={atr_14} vol_spike={vol_spike}",
            flush=True
        )

        # ── MTF Indicator Computation (every 60s) ──
        _now = time.time()
        if _now - _mtf_last_compute >= _MTF_INTERVAL_SEC:
            _mtf_last_compute = _now
            try:
                with db.cursor() as mtf_cur:
                    # Fetch enough 1m candles for 1h EMA200 (200*60=12000, use 13000)
                    mtf_cur.execute("""
                        SELECT ts, o, h, l, c, v
                        FROM candles
                        WHERE symbol=%s AND tf=%s
                        ORDER BY ts DESC
                        LIMIT 13000
                    """, (symbol, tf))
                    mtf_rows = mtf_cur.fetchall()

                if len(mtf_rows) >= 1200:  # minimum for 1h EMA (20 bars)
                    mtf_rows_asc = list(reversed(mtf_rows))

                    # Resample to 15m and 1h
                    candles_15m = _resample_candles(mtf_rows_asc, 15)
                    candles_1h = _resample_candles(mtf_rows_asc, 60)

                    # Extract close/high/low arrays
                    closes_15m = [float(c[4]) for c in candles_15m]
                    highs_15m = [float(c[2]) for c in candles_15m]
                    lows_15m = [float(c[3]) for c in candles_15m]

                    closes_1h = [float(c[4]) for c in candles_1h]
                    highs_1h = [float(c[2]) for c in candles_1h]
                    lows_1h = [float(c[3]) for c in candles_1h]

                    # EMA calculations
                    ema_15m_50 = ema(closes_15m, 50) if len(closes_15m) >= 50 else None
                    ema_15m_200 = ema(closes_15m, 200) if len(closes_15m) >= 200 else None
                    ema_1h_50 = ema(closes_1h, 50) if len(closes_1h) >= 50 else None
                    ema_1h_200 = ema(closes_1h, 200) if len(closes_1h) >= 200 else None

                    # ADX on 1h
                    adx_1h = _compute_adx(highs_1h, lows_1h, closes_1h, 14)

                    # Donchian(20) on 15m
                    dc_high_15m, dc_low_15m = _compute_donchian(highs_15m, lows_15m, 20)

                    # ATR on 15m
                    atr_15m = None
                    if len(closes_15m) >= 15:
                        trs_15m = []
                        for i in range(-14, 0):
                            _hi = highs_15m[i]
                            _lo = lows_15m[i]
                            _pc = closes_15m[i - 1]
                            _tr = max(_hi - _lo, abs(_hi - _pc), abs(_lo - _pc))
                            trs_15m.append(_tr)
                        atr_15m = sum(trs_15m) / 14

                    # Upsert to mtf_indicators
                    with db.cursor() as mtf_cur:
                        mtf_cur.execute("""
                            CREATE TABLE IF NOT EXISTS mtf_indicators (
                                symbol TEXT PRIMARY KEY,
                                ema_15m_50 REAL, ema_15m_200 REAL,
                                ema_1h_50 REAL, ema_1h_200 REAL,
                                adx_1h REAL,
                                donchian_high_15m_20 REAL, donchian_low_15m_20 REAL,
                                atr_15m REAL,
                                updated_at TIMESTAMPTZ DEFAULT now()
                            );
                        """)
                        mtf_cur.execute("""
                            INSERT INTO mtf_indicators (
                                symbol, ema_15m_50, ema_15m_200,
                                ema_1h_50, ema_1h_200,
                                adx_1h, donchian_high_15m_20, donchian_low_15m_20,
                                atr_15m, updated_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                            ON CONFLICT (symbol) DO UPDATE SET
                                ema_15m_50=EXCLUDED.ema_15m_50,
                                ema_15m_200=EXCLUDED.ema_15m_200,
                                ema_1h_50=EXCLUDED.ema_1h_50,
                                ema_1h_200=EXCLUDED.ema_1h_200,
                                adx_1h=EXCLUDED.adx_1h,
                                donchian_high_15m_20=EXCLUDED.donchian_high_15m_20,
                                donchian_low_15m_20=EXCLUDED.donchian_low_15m_20,
                                atr_15m=EXCLUDED.atr_15m,
                                updated_at=now()
                        """, (symbol, ema_15m_50, ema_15m_200,
                              ema_1h_50, ema_1h_200,
                              adx_1h, dc_high_15m, dc_low_15m, atr_15m))

                    print(f"MTF saved: ADX_1h={adx_1h} EMA_1h_50={ema_1h_50} "
                          f"DC_15m=[{dc_low_15m},{dc_high_15m}] ATR_15m={atr_15m}",
                          flush=True)
                else:
                    print(f"MTF: insufficient candles ({len(mtf_rows)}/1200)", flush=True)
            except Exception as e:
                print(f"MTF computation error: {e}", flush=True)

        time.sleep(15)

    except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
        print(f"DB connection lost: {e}", flush=True)
        try:
            db.close()
        except Exception:
            pass
        try:
            db = get_conn(autocommit=True)
            print("DB reconnected", flush=True)
        except Exception as re:
            print(f"DB reconnect failed: {re}", flush=True)
        time.sleep(10)

    except Exception as e:
        print("Indicator error:", e, flush=True)
        time.sleep(10)
