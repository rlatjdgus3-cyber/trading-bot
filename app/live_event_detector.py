"""
live_event_detector.py — Real-time volatility spike detection daemon.

Every 30s:
  1. Check KILL_SWITCH
  2. Fetch latest 10 5m candles from Bybit -> upsert to market_ohlcv
  3. Compute z-score from last 2h of market_ohlcv rolling stats
  4. If z-score > 2.0 and no event in last 2 min -> create LIVE_VOL_SPIKE event
  5. Link recent news (+/-30 min) via event_news
"""
import os
import sys
import time
import math
import json
import atexit
import traceback

sys.path.insert(0, '/root/trading-bot/app')
import ccxt
from psycopg2 import OperationalError, InterfaceError
from ccxt.base.errors import RateLimitExceeded, NetworkError
from db_config import get_conn
import fact_categories

SYMBOL = 'BTC/USDT:USDT'
TF = '5m'
POLL_SEC = 30
ZSCORE_THRESHOLD = 2.0    # 기존 3.0 — 더 빈번한 변동성 감지
COOLDOWN_SEC = 120        # 기존 300 — 감지 간격 단축
KILL_SWITCH_PATH = '/root/trading-bot/app/KILL_SWITCH'
PID_FILE = '/tmp/live_event_detector.pid'
LOG_PREFIX = '[live_event]'


def _log(msg):
    print(f"{LOG_PREFIX} {msg}", flush=True)


def _db_conn():
    return get_conn()


_exchange = None


def _get_exchange():
    global _exchange
    if _exchange is None:
        _exchange = ccxt.bybit({
            'apiKey': os.getenv('BYBIT_API_KEY'),
            'secret': os.getenv('BYBIT_SECRET'),
            'enableRateLimit': True,
            'timeout': 20000,
            'options': {'defaultType': 'swap'},
        })
    return _exchange


def _fetch_and_upsert(cur):
    """Fetch latest 5m candles and upsert to market_ohlcv.

    Retries once on RateLimitExceeded with a 2s pause.
    """
    ex = _get_exchange()
    for attempt in range(2):
        try:
            ohlcv = ex.fetch_ohlcv(SYMBOL, timeframe=TF, limit=10)
            break
        except RateLimitExceeded:
            if attempt == 0:
                _log('Rate limit hit on fetch_ohlcv, retry in 2s')
                time.sleep(2)
            else:
                raise
    if not ohlcv:
        return 0
    for ms, o, h, l, c, v in ohlcv:
        cur.execute("""
            INSERT INTO market_ohlcv (symbol, tf, ts, o, h, l, c, v)
            VALUES (%s, %s, to_timestamp(%s/1000.0), %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, tf, ts) DO UPDATE
            SET o=EXCLUDED.o, h=EXCLUDED.h, l=EXCLUDED.l,
                c=EXCLUDED.c, v=EXCLUDED.v;
        """, (SYMBOL, TF, ms, o, h, l, c, v))
    return len(ohlcv)


def _compute_zscore(cur):
    """Compute current volatility z-score from last 2h of 5m data.

    Uses the prior 12 returns (excluding current) as the reference window
    to avoid dampening the z-score when current return is extreme.

    Returns (zscore, btc_price, direction) or (None, None, None).
    """
    cur.execute("""
        SELECT ts, c FROM market_ohlcv
        WHERE symbol = 'BTC/USDT:USDT' AND tf = '5m'
        ORDER BY ts DESC
        LIMIT 26;
    """)
    rows = cur.fetchall()
    if len(rows) < 14:
        return None, None, None

    # Rows are DESC, reverse to ASC
    rows = list(reversed(rows))
    closes = [float(r[1]) for r in rows]

    # Compute log returns
    log_returns = []
    for i in range(1, len(closes)):
        if closes[i - 1] > 0:
            log_returns.append(math.log(closes[i] / closes[i - 1]))
        else:
            log_returns.append(0.0)

    if len(log_returns) < 13:
        return None, None, None

    # Window: log_returns[-13:-1] (excludes current return at -1)
    window = log_returns[-13:-1]
    current_return = log_returns[-1]

    n = len(window)
    mean = sum(window) / n
    variance = sum((x - mean) ** 2 for x in window) / n
    std = math.sqrt(variance) if variance > 0 else 0.0

    if std == 0:
        return 0.0, closes[-1], 'FLAT'

    zscore = (current_return - mean) / std
    btc_price = closes[-1]
    direction = 'UP' if current_return > 0 else 'DOWN'

    return zscore, btc_price, direction


def _check_cooldown(cur):
    """Return True if we should skip (event too recent)."""
    cur.execute("""
        SELECT id FROM events
        WHERE kind = 'LIVE_VOL_SPIKE'
          AND created_at >= now() - interval '%s seconds'
        LIMIT 1;
    """, (COOLDOWN_SEC,))
    return cur.fetchone() is not None


def _create_event(cur, zscore, btc_price, direction):
    """Create a LIVE_VOL_SPIKE event and link nearby news."""
    cur.execute("""
        SELECT id, title, summary FROM news
        WHERE ts >= now() - interval '30 minutes'
        ORDER BY ts DESC;
    """)
    related_news = cur.fetchall()

    combined_text = ' '.join(
        f"{t or ''} {s or ''}" for _, t, s in related_news
    )
    category = fact_categories.classify_news(combined_text)
    keywords = fact_categories.extract_macro_keywords(combined_text)

    cur.execute("""
        INSERT INTO events
            (kind, start_ts, symbol, vol_zscore,
             btc_price_at, direction, category, keywords, metadata)
        VALUES ('LIVE_VOL_SPIKE', now(), %s, %s,
                %s, %s, %s, %s, %s::jsonb)
        RETURNING id;
    """, (SYMBOL, abs(zscore), btc_price, direction, category, keywords,
          json.dumps({'news_count': len(related_news)}, default=str)))

    event_id = cur.fetchone()[0]

    for news_id, _, _ in related_news:
        cur.execute("""
            INSERT INTO event_news (event_id, news_id, relevance)
            VALUES (%s, %s, 1.0)
            ON CONFLICT (event_id, news_id) DO NOTHING;
        """, (event_id, news_id))

    _log(f"LIVE_VOL_SPIKE created: id={event_id} zscore={zscore:.2f} "
         f"price={btc_price:.1f} dir={direction} cat={category} "
         f"news={len(related_news)}")
    return event_id


def _cycle(conn):
    """One detection cycle. Uses persistent connection."""
    if os.path.exists(KILL_SWITCH_PATH):
        _log('KILL_SWITCH detected. Exiting.')
        sys.exit(0)

    # Also check DB kill switch (bot_config may not exist yet)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM bot_config WHERE key='KILL_SWITCH';")
            row = cur.fetchone()
            if row and row[0] == 'ON':
                _log('DB KILL_SWITCH=ON. Exiting.')
                sys.exit(0)
    except Exception:
        conn.rollback()

    with conn.cursor() as cur:
        count = _fetch_and_upsert(cur)
        if count == 0:
            return

        zscore, btc_price, direction = _compute_zscore(cur)
        if zscore is None:
            return

        abs_z = abs(zscore)
        if abs_z > ZSCORE_THRESHOLD:
            if _check_cooldown(cur):
                _log(f"z={zscore:.2f} but cooldown active, skipping.")
                return
            _create_event(cur, zscore, btc_price, direction)
        elif abs_z > 1.5:
            _log(f"Elevated z={zscore:.2f} (below threshold {ZSCORE_THRESHOLD})")


def _acquire_pid_lock():
    """Write PID file. Exit if another instance is alive."""
    if os.path.exists(PID_FILE):
        try:
            with open(PID_FILE) as f:
                old_pid = int(f.read().strip())
            os.kill(old_pid, 0)  # check alive
            _log(f'Another instance already running (pid={old_pid}). Exiting.')
            sys.exit(0)
        except (ProcessLookupError, ValueError):
            pass  # stale PID file — safe to overwrite
        except PermissionError:
            _log(f'Another instance may be running (pid check permission denied). Exiting.')
            sys.exit(0)
    with open(PID_FILE, 'w') as f:
        f.write(str(os.getpid()))
    atexit.register(_release_pid_lock)


def _release_pid_lock():
    """Remove PID file on exit."""
    try:
        if os.path.exists(PID_FILE):
            with open(PID_FILE) as f:
                if f.read().strip() == str(os.getpid()):
                    os.remove(PID_FILE)
    except Exception:
        pass


def main():
    _acquire_pid_lock()
    _log('=== LIVE EVENT DETECTOR START ===')
    conn = _db_conn()
    conn.autocommit = True
    backoff = 5

    while True:
        try:
            # Auto-reconnect on DB errors
            if conn is None or conn.closed != 0:
                conn = _db_conn()
                conn.autocommit = True
                _log("DB reconnected.")

            _cycle(conn)
            backoff = 5
            time.sleep(POLL_SEC)

        except (OperationalError, InterfaceError) as e:
            _log(f"DB error: {repr(e)} | reconnect in {backoff}s")
            try:
                if conn:
                    conn.close()
            except Exception:
                pass
            conn = None
            time.sleep(backoff)
            backoff = min(backoff * 2, 120)

        except (RateLimitExceeded, NetworkError) as e:
            _log(f"Exchange transient error: {type(e).__name__} | wait {POLL_SEC}s")
            time.sleep(POLL_SEC)
            # Don't increase backoff — transient issue

        except KeyboardInterrupt:
            _log("Interrupted. Shutting down.")
            break

        except Exception as e:
            _log(f"Unexpected error: {repr(e)}")
            _log(traceback.format_exc())
            time.sleep(backoff)
            backoff = min(backoff * 2, 120)


if __name__ == '__main__':
    main()
