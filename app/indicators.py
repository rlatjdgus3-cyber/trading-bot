import os
import time
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# =========================
# DB 설정 (운영 기준: 5432)
# =========================
DB_DSN = dict(
    host=os.getenv("DB_HOST", "localhost"),
    port=int(os.getenv("DB_PORT", "5432")),
    dbname=os.getenv("DB_NAME", "trading"),
    user=os.getenv("DB_USER", "bot"),
    password=os.getenv("DB_PASS", "botpass"),
)

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
        db = psycopg2.connect(**DB_DSN)
        db.autocommit = True

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

        # =========================
        # indicators 저장
        # =========================
        db = psycopg2.connect(**DB_DSN)
        db.autocommit = True

        with db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO indicators (
                    symbol, tf, ts,
                    bb_mid, bb_up, bb_dn,
                    ich_tenkan, ich_kijun,
                    ich_span_a, ich_span_b,
                    vol, vol_ma20, vol_spike
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (symbol, tf, ts) DO NOTHING
                """,
                (
                    symbol, tf, ts,
                    mid, up, dn,
                    tenkan, kijun,
                    span_a, span_b,
                    vol, vol_ma20, vol_spike
                ),
            )

        db.close()

        print(
            f"Saved indicators @ {ts} vol_spike={vol_spike}",
            flush=True
        )

        time.sleep(60)

    except Exception as e:
        print("Indicator error:", e, flush=True)
        time.sleep(10)
