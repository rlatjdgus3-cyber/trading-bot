import time
import traceback
import ccxt

# =========================
# 🔑 BYBIT API KEY 설정
# =========================
# ⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇⬇
# 여기다가 네 API KEY를 문자열로 넣어
BYBIT_API_KEY = "NCqVE2XZnkBvKFyDtj"
# 여기다가 네 API SECRET을 문자열로 넣어
BYBIT_API_SECRET = "dzllyaimsVvccQJB0fI9H8E03b3K2TV0gzkA"
# ⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆⬆

# =========================
# CONFIG
# =========================
SYMBOL = "BTC/USDT:USDT"
PNL_UPPER_USD = 50     # +50 USD 이상
PNL_LOWER_USD = -50    # -50 USD 이하
CHECK_INTERVAL = 30    # seconds

if "여기에_" in BYBIT_API_KEY or "여기에_" in BYBIT_API_SECRET:
    raise SystemExit("❌ API KEY / SECRET을 코드에 아직 안 넣었음")

# =========================
# BYBIT
# =========================
exchange = ccxt.bybit({
    "apiKey": BYBIT_API_KEY,
    "secret": BYBIT_API_SECRET,
    "enableRateLimit": True,
    "options": {"defaultType": "linear"},
})

exchange.load_markets()

def pos_key(p: dict) -> str:
    return f"{p.get('symbol')}:{p.get('side')}"

print("=== PNL WATCHER STARTED ===")
print(f"[CONFIG] SYMBOL={SYMBOL} upper={PNL_UPPER_USD} lower={PNL_LOWER_USD}")

while True:
    try:
        positions = exchange.fetch_positions([SYMBOL])

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
        print("[PNL WATCHER ERROR]")
        print(traceback.format_exc())
        time.sleep(10)
