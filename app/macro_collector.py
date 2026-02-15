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
from datetime import datetime, timezone, timedelta

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
    from db_config import get_conn
    return get_conn()


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
    """Store fetched prices into macro_data table.

    Uses autocommit for per-row independence so one failure doesn't
    rollback all rows. ON CONFLICT DO UPDATE to refresh same-ts data.
    """
    if not prices:
        return 0

    stored = 0
    prev_autocommit = conn.autocommit
    try:
        # Close any open transaction before switching autocommit
        if not prev_autocommit:
            try:
                conn.rollback()
            except Exception:
                pass
        conn.autocommit = True
        with conn.cursor() as cur:
            for source_name, data in prices.items():
                try:
                    cur.execute("""
                        INSERT INTO macro_data (ts, source, price, metadata)
                        VALUES (now(), %s, %s, %s::jsonb)
                        ON CONFLICT (source, ts)
                        DO UPDATE SET price = EXCLUDED.price,
                                      metadata = EXCLUDED.metadata;
                    """, (source_name, data['price'],
                          json.dumps(data.get('metadata', {}))))
                    stored += 1
                except Exception as e:
                    _log(f'store {source_name} error: {e}')
    finally:
        conn.autocommit = prev_autocommit
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


# ─── Macro Event Calendar ────────────────────────────────────────────
# FOMC/CPI/PPI/고용지표 일정 (UTC dates)
# 해당 일정 전후 구간에서 AUTO_EMERGENCY 민감도 상향

MACRO_EVENT_CALENDAR_2025 = [
    # FOMC decisions (announcement dates)
    ('2025-01-29', 'FOMC'),
    ('2025-03-19', 'FOMC'),
    ('2025-05-07', 'FOMC'),
    ('2025-06-18', 'FOMC'),
    ('2025-07-30', 'FOMC'),
    ('2025-09-17', 'FOMC'),
    ('2025-10-29', 'FOMC'),
    ('2025-12-17', 'FOMC'),
    # CPI releases
    ('2025-01-15', 'CPI'),
    ('2025-02-12', 'CPI'),
    ('2025-03-12', 'CPI'),
    ('2025-04-10', 'CPI'),
    ('2025-05-13', 'CPI'),
    ('2025-06-11', 'CPI'),
    ('2025-07-15', 'CPI'),
    ('2025-08-12', 'CPI'),
    ('2025-09-10', 'CPI'),
    ('2025-10-14', 'CPI'),
    ('2025-11-12', 'CPI'),
    ('2025-12-10', 'CPI'),
    # PPI releases
    ('2025-01-14', 'PPI'),
    ('2025-02-13', 'PPI'),
    ('2025-03-13', 'PPI'),
    ('2025-04-11', 'PPI'),
    ('2025-05-15', 'PPI'),
    ('2025-06-12', 'PPI'),
    ('2025-07-15', 'PPI'),
    ('2025-08-14', 'PPI'),
    ('2025-09-11', 'PPI'),
    ('2025-10-15', 'PPI'),
    ('2025-11-13', 'PPI'),
    ('2025-12-11', 'PPI'),
    # Non-Farm Payrolls (NFP)
    ('2025-01-10', 'NFP'),
    ('2025-02-07', 'NFP'),
    ('2025-03-07', 'NFP'),
    ('2025-04-04', 'NFP'),
    ('2025-05-02', 'NFP'),
    ('2025-06-06', 'NFP'),
    ('2025-07-03', 'NFP'),
    ('2025-08-01', 'NFP'),
    ('2025-09-05', 'NFP'),
    ('2025-10-03', 'NFP'),
    ('2025-11-07', 'NFP'),
    ('2025-12-05', 'NFP'),
]

MACRO_EVENT_CALENDAR_2026 = [
    # FOMC decisions
    ('2026-01-28', 'FOMC'),
    ('2026-03-18', 'FOMC'),
    ('2026-04-29', 'FOMC'),
    ('2026-06-17', 'FOMC'),
    ('2026-07-29', 'FOMC'),
    ('2026-09-16', 'FOMC'),
    ('2026-10-28', 'FOMC'),
    ('2026-12-16', 'FOMC'),
    # CPI releases (estimated)
    ('2026-01-13', 'CPI'),
    ('2026-02-11', 'CPI'),
    ('2026-03-11', 'CPI'),
    ('2026-04-14', 'CPI'),
    ('2026-05-12', 'CPI'),
    ('2026-06-10', 'CPI'),
    ('2026-07-14', 'CPI'),
    ('2026-08-12', 'CPI'),
    ('2026-09-15', 'CPI'),
    ('2026-10-13', 'CPI'),
    ('2026-11-10', 'CPI'),
    ('2026-12-09', 'CPI'),
    # PPI releases (estimated)
    ('2026-01-14', 'PPI'),
    ('2026-02-12', 'PPI'),
    ('2026-03-12', 'PPI'),
    ('2026-04-09', 'PPI'),
    ('2026-05-14', 'PPI'),
    ('2026-06-11', 'PPI'),
    ('2026-07-16', 'PPI'),
    ('2026-08-13', 'PPI'),
    ('2026-09-10', 'PPI'),
    ('2026-10-14', 'PPI'),
    ('2026-11-12', 'PPI'),
    ('2026-12-10', 'PPI'),
    # NFP (estimated — first Friday)
    ('2026-01-09', 'NFP'),
    ('2026-02-06', 'NFP'),
    ('2026-03-06', 'NFP'),
    ('2026-04-03', 'NFP'),
    ('2026-05-01', 'NFP'),
    ('2026-06-05', 'NFP'),
    ('2026-07-02', 'NFP'),
    ('2026-08-07', 'NFP'),
    ('2026-09-04', 'NFP'),
    ('2026-10-02', 'NFP'),
    ('2026-11-06', 'NFP'),
    ('2026-12-04', 'NFP'),
]

# Pre-parsed set for fast lookup
_MACRO_EVENT_DATES = {}
for _date_str, _etype in MACRO_EVENT_CALENDAR_2025 + MACRO_EVENT_CALENDAR_2026:
    _MACRO_EVENT_DATES.setdefault(_date_str, []).append(_etype)

# Window: event day -1h ~ +4h (UTC) around typical release time (13:30 UTC for most)
MACRO_EVENT_WINDOW_HOURS_BEFORE = 1
MACRO_EVENT_WINDOW_HOURS_AFTER = 4


def is_macro_event_window() -> dict:
    """Check if current time is within a macro event window.

    Returns:
        dict with 'active': bool, 'events': list of event types, 'date': str
        e.g. {'active': True, 'events': ['CPI'], 'date': '2026-02-11'}
    """
    now = datetime.now(timezone.utc)
    today_str = now.strftime('%Y-%m-%d')
    yesterday_str = (now - timedelta(days=1)).strftime('%Y-%m-%d')

    # Check today and yesterday (for late-night events)
    for date_str in (today_str, yesterday_str):
        if date_str in _MACRO_EVENT_DATES:
            # Typical release time: 13:30 UTC (8:30 ET)
            event_dt = datetime.strptime(date_str, '%Y-%m-%d').replace(
                hour=13, minute=30, tzinfo=timezone.utc
            )
            window_start = event_dt - timedelta(hours=MACRO_EVENT_WINDOW_HOURS_BEFORE)
            window_end = event_dt + timedelta(hours=MACRO_EVENT_WINDOW_HOURS_AFTER)

            if window_start <= now <= window_end:
                return {
                    'active': True,
                    'events': _MACRO_EVENT_DATES[date_str],
                    'date': date_str,
                }

    return {'active': False, 'events': [], 'date': ''}


def get_upcoming_macro_events(days_ahead: int = 7) -> list:
    """Return macro events within the next N days.

    Returns list of {'date': str, 'events': list[str]}.
    """
    now = datetime.now(timezone.utc)
    result = []
    for d in range(days_ahead + 1):
        date_str = (now + timedelta(days=d)).strftime('%Y-%m-%d')
        if date_str in _MACRO_EVENT_DATES:
            result.append({'date': date_str, 'events': _MACRO_EVENT_DATES[date_str]})
    return result


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
