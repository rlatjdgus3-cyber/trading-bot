#!/usr/bin/env python3
"""
macro_collector.py — 거시경제 지표 수집 데몬.

수집 대상:
  - QQQ   (NASDAQ-100 ETF)
  - SPY   (S&P 500 ETF)
  - DX-Y.NYB (Dollar Index, DXY)
  - ^TNX  (US 10-Year Treasury Yield)
  - ^VIX  (CBOE Volatility Index)

저장: macro_data 테이블 (source, ts, price, metadata)
폴링: 5분 간격 (시장 개장시), 15분 간격 (비개장시)

기존 nasdaq_proxy 데이터도 macro_data로 브릿지.
"""
import os
import sys
import time
import json
import traceback
from datetime import datetime, timezone

sys.path.insert(0, '/root/trading-bot/app')
from dotenv import load_dotenv
load_dotenv('/root/trading-bot/app/.env')

LOG_PREFIX = '[macro_collector]'

SYMBOLS = {
    'QQQ':      'QQQ',          # NASDAQ-100 ETF
    'SPY':      'SPY',          # S&P 500 ETF
    'DXY':      'DX-Y.NYB',    # Dollar Index
    'US10Y':    '^TNX',         # 10-Year Treasury Yield
    'VIX':      '^VIX',         # CBOE Volatility Index
}

POLL_INTERVAL_MARKET = 300   # 5 min during market hours
POLL_INTERVAL_OFF = 900      # 15 min outside market hours
US_MARKET_OPEN_UTC = 14      # 09:30 ET ≈ 14:30 UTC
US_MARKET_CLOSE_UTC = 21     # 16:00 ET ≈ 21:00 UTC


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _db_conn():
    import psycopg2
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=int(os.getenv('DB_PORT', '5432')),
        dbname=os.getenv('DB_NAME', 'trading'),
        user=os.getenv('DB_USER', 'bot'),
        password=os.getenv('DB_PASS', 'botpass'),
        connect_timeout=10,
        options='-c statement_timeout=30000',
    )


def _is_market_hours():
    """Check if US equity market is roughly open (UTC)."""
    now = datetime.now(timezone.utc)
    weekday = now.weekday()  # 0=Mon, 6=Sun
    if weekday >= 5:
        return False
    return US_MARKET_OPEN_UTC <= now.hour < US_MARKET_CLOSE_UTC


def _fetch_prices():
    """Fetch current prices for all macro symbols via yfinance."""
    import yfinance as yf

    results = {}
    tickers_str = ' '.join(SYMBOLS.values())
    try:
        tickers = yf.Tickers(tickers_str)
        for source_name, yf_symbol in SYMBOLS.items():
            try:
                ticker = tickers.tickers.get(yf_symbol)
                if ticker is None:
                    _log(f'{source_name}: ticker not found')
                    continue
                info = ticker.fast_info
                price = getattr(info, 'last_price', None)
                if price is None:
                    price = getattr(info, 'previous_close', None)
                if price is not None and price > 0:
                    results[source_name] = {
                        'price': float(price),
                        'metadata': {
                            'yf_symbol': yf_symbol,
                            'market_state': getattr(info, 'market_state', 'unknown'),
                        },
                    }
            except Exception as e:
                _log(f'{source_name} ({yf_symbol}) fetch error: {e}')
    except Exception as e:
        _log(f'yfinance batch fetch error: {e}')
        # Fallback: fetch individually
        for source_name, yf_symbol in SYMBOLS.items():
            try:
                ticker = yf.Ticker(yf_symbol)
                info = ticker.fast_info
                price = getattr(info, 'last_price', None)
                if price is None:
                    price = getattr(info, 'previous_close', None)
                if price is not None and price > 0:
                    results[source_name] = {
                        'price': float(price),
                        'metadata': {'yf_symbol': yf_symbol},
                    }
            except Exception as e2:
                _log(f'{source_name} individual fetch error: {e2}')

    return results


def _store_prices(conn, prices):
    """Store fetched prices into macro_data table."""
    if not prices:
        return 0

    stored = 0
    with conn.cursor() as cur:
        for source_name, data in prices.items():
            try:
                cur.execute("""
                    INSERT INTO macro_data (ts, source, price, metadata)
                    VALUES (now(), %s, %s, %s::jsonb)
                    ON CONFLICT (source, ts) DO NOTHING;
                """, (source_name, data['price'],
                      json.dumps(data.get('metadata', {}))))
                stored += 1
            except Exception as e:
                _log(f'store {source_name} error: {e}')
                try:
                    conn.rollback()
                except Exception:
                    pass
    try:
        conn.commit()
    except Exception as e:
        _log(f'commit error: {e}')
        try:
            conn.rollback()
        except Exception:
            pass
    return stored


def _bridge_nasdaq_proxy(conn):
    """Bridge unsynced nasdaq_proxy data to macro_data (one-time catch-up)."""
    try:
        with conn.cursor() as cur:
            # Check if nasdaq_proxy table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = 'nasdaq_proxy'
                );
            """)
            if not cur.fetchone()[0]:
                return

            # Check if bridge needed
            cur.execute("""
                SELECT count(*) FROM macro_data WHERE source = 'QQQ';
            """)
            macro_count = cur.fetchone()[0]

            cur.execute("""
                SELECT count(*) FROM nasdaq_proxy;
            """)
            proxy_count = cur.fetchone()[0]

            if proxy_count > 0 and macro_count < proxy_count:
                cur.execute("""
                    INSERT INTO macro_data (ts, source, price, metadata)
                    SELECT ts, 'QQQ', close, '{"bridged": true}'::jsonb
                    FROM nasdaq_proxy
                    WHERE NOT EXISTS (
                        SELECT 1 FROM macro_data m
                        WHERE m.source = 'QQQ'
                          AND date_trunc('minute', m.ts) = date_trunc('minute', nasdaq_proxy.ts)
                    )
                    ON CONFLICT (source, ts) DO NOTHING;
                """)
                conn.commit()
                bridged = cur.rowcount
                if bridged > 0:
                    _log(f'bridged {bridged} rows from nasdaq_proxy → macro_data')
    except Exception as e:
        _log(f'bridge_nasdaq_proxy error: {e}')


def run_once(conn=None):
    """Single collection cycle. Returns count of stored prices."""
    own_conn = conn is None
    if own_conn:
        conn = _db_conn()
        conn.autocommit = False

    try:
        prices = _fetch_prices()
        stored = _store_prices(conn, prices)

        labels = [f'{k}=${v["price"]:.2f}' for k, v in prices.items()]
        _log(f'collected {stored}/{len(SYMBOLS)}: {", ".join(labels)}')
        return stored
    except Exception as e:
        _log(f'run_once error: {e}')
        traceback.print_exc()
        return 0
    finally:
        if own_conn and conn:
            try:
                conn.close()
            except Exception:
                pass


def run_daemon():
    """Main daemon loop."""
    _log(f'starting daemon — symbols: {list(SYMBOLS.keys())}')

    conn = _db_conn()
    conn.autocommit = False

    # One-time bridge from nasdaq_proxy
    _bridge_nasdaq_proxy(conn)

    while True:
        try:
            run_once(conn)
        except Exception as e:
            _log(f'daemon error: {e}')
            traceback.print_exc()
            # Reconnect on error
            try:
                conn.close()
            except Exception:
                pass
            try:
                conn = _db_conn()
                conn.autocommit = False
            except Exception:
                _log('reconnect failed, retry in 60s')
                time.sleep(60)
                continue

        interval = POLL_INTERVAL_MARKET if _is_market_hours() else POLL_INTERVAL_OFF
        time.sleep(interval)


if __name__ == '__main__':
    run_daemon()
