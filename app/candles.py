import os
import time
import traceback

import ccxt
from psycopg2 import OperationalError, InterfaceError
from dotenv import load_dotenv
from db_config import get_conn

load_dotenv()

SYMBOL = "BTC/USDT:USDT"
TF = "1m"
LIMIT = 200

def make_exchange():
    return ccxt.bybit({
        "apiKey": os.getenv("BYBIT_API_KEY"),
        "secret": os.getenv("BYBIT_SECRET"),
        "enableRateLimit": True,
        "timeout": 20000,
        "options": {"defaultType": "swap"},
    })

def connect_db():
    return get_conn(autocommit=False)

def upsert_ohlcv(conn, ohlcv):
    with conn.cursor() as cur:
        for ms, o, h, l, c, v in ohlcv:
            cur.execute(
                """
                INSERT INTO candles(symbol, tf, ts, o, h, l, c, v)
                VALUES (%s, %s, to_timestamp(%s/1000.0), %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, tf, ts) DO UPDATE
                SET o=EXCLUDED.o, h=EXCLUDED.h, l=EXCLUDED.l, c=EXCLUDED.c, v=EXCLUDED.v
                """,
                (SYMBOL, TF, ms, o, h, l, c, v),
            )
    conn.commit()

def log(msg):
    print(msg, flush=True)

def main():
    exchange = make_exchange()
    backoff = 5
    last_saved_ms = None

    log("=== CANDLE LOGGER STARTED (RESILIENT) ===")

    # DB 연결은 실패할 수 있으니 루프 안에서 보장
    db = None

    while True:
        try:
            if db is None or db.closed != 0:
                db = connect_db()

            ohlcv = exchange.fetch_ohlcv(SYMBOL, timeframe=TF, limit=LIMIT)
            if not ohlcv:
                log("[candles] empty ohlcv, sleep 10s")
                time.sleep(10)
                continue

            last_ms = ohlcv[-1][0]
            if last_saved_ms is not None and last_ms == last_saved_ms:
                log(f"[candles] no new candle (last_ms={last_ms}), sleep 10s")
                time.sleep(10)
                continue

            upsert_ohlcv(db, ohlcv)
            last_saved_ms = last_ms

            log(f"[candles] Saved {TF} last_ts_ms={last_ms}")
            backoff = 5
            time.sleep(60)

        except (ccxt.RequestTimeout, ccxt.NetworkError) as e:
            log(f"[candles] network error: {repr(e)} | retry in {backoff}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 120)

        except (OperationalError, InterfaceError) as e:
            log(f"[candles] DB error: {repr(e)} | reconnect in {backoff}s")
            try:
                if db:
                    db.close()
            except Exception:
                pass
            db = None
            time.sleep(backoff)
            backoff = min(backoff * 2, 120)

        except Exception as e:
            log(f"[candles] unexpected error: {repr(e)}")
            log(traceback.format_exc())
            time.sleep(backoff)
            backoff = min(backoff * 2, 120)

if __name__ == "__main__":
    main()
