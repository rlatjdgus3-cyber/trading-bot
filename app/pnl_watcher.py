import os
import time
import traceback
import ccxt
from dotenv import load_dotenv
load_dotenv('/root/trading-bot/app/.env')

# =========================
# BYBIT API KEY (.env에서 로드)
# =========================
BYBIT_API_KEY = os.getenv('BYBIT_API_KEY', '')
BYBIT_API_SECRET = os.getenv('BYBIT_SECRET', '')

# =========================
# CONFIG
# =========================
SYMBOL = "BTC/USDT:USDT"
PNL_UPPER_USD = 50     # +50 USD 이상
PNL_LOWER_USD = -50    # -50 USD 이하
CHECK_INTERVAL = 30    # seconds

if not BYBIT_API_KEY or not BYBIT_API_SECRET:
    raise SystemExit("BYBIT_API_KEY / BYBIT_SECRET 환경변수가 설정되지 않음")

# =========================
# BYBIT (lazy init with retry)
# =========================
_exchange = None


def _get_exchange():
    global _exchange
    if _exchange is not None:
        return _exchange
    _exchange = ccxt.bybit({
        "apiKey": BYBIT_API_KEY,
        "secret": BYBIT_API_SECRET,
        "enableRateLimit": True,
        "timeout": 20000,
        "options": {"defaultType": "swap"},
    })
    _exchange.load_markets()
    return _exchange


def pos_key(p: dict) -> str:
    return f"{p.get('symbol')}:{p.get('side')}"

print("=== PNL WATCHER STARTED ===")
print(f"[CONFIG] SYMBOL={SYMBOL} upper={PNL_UPPER_USD} lower={PNL_LOWER_USD}")

while True:
    try:
        ex = _get_exchange()
        positions = ex.fetch_positions([SYMBOL])

        has_position = False
        for p in positions:
            contracts = p.get("contracts") or 0
            if contracts == 0:
                continue

            has_position = True
            pnl = p.get("unrealizedPnl")
            if pnl is None:
                continue

            pnl = float(pnl)

            if pnl >= PNL_UPPER_USD:
                print(f"[PNL ALERT] +{pnl} USD | {pos_key(p)}")

            if pnl <= PNL_LOWER_USD:
                print(f"[PNL ALERT] {pnl} USD | {pos_key(p)}")

        if not has_position:
            print("[INFO] No open position")

        time.sleep(CHECK_INTERVAL)

    except Exception:
        _exchange = None  # Reset for reconnection on next cycle
        print("[PNL WATCHER ERROR]")
        print(traceback.format_exc())
        time.sleep(30)
