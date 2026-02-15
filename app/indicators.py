import os
import time
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

print("=== INDICATOR ENGINE STARTED ===", flush=True)

last_ts = None

while True:
    try:
        db = get_conn(autocommit=True)

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

        db.close()

        if len(rows) < 120:
            print("Waiting candles:", len(rows), flush=True)
            time.sleep(10)
            continue

        # DESC → ASC
        rows = list(reversed(rows))

        ts, o, h, l, c, v = rows[-1]

        # 같은 ts 반복 방지
        if last_ts is not None and ts == last_ts:
            time.sleep(5)
            continue
        last_ts = ts

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

        # =========================
        # indicators 저장
        # =========================
        db = get_conn(autocommit=True)

        with db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO indicators (
                    symbol, tf, ts,
                    bb_mid, bb_up, bb_dn,
                    ich_tenkan, ich_kijun,
                    ich_span_a, ich_span_b,
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
                """,
                (
                    symbol, tf, ts,
                    mid, up, dn,
                    tenkan, kijun,
                    span_a, span_b,
                    vol, vol_ma20, vol_spike,
                    rsi_14, atr_14, ma_50, ma_200
                ),
            )

        db.close()

        print(
            f"Saved indicators @ {ts} rsi={rsi_14} atr={atr_14} vol_spike={vol_spike}",
            flush=True
        )

        time.sleep(60)

    except Exception as e:
        print("Indicator error:", e, flush=True)
        time.sleep(10)
