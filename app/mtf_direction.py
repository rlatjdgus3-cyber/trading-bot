"""
mtf_direction.py — Multi-Timeframe Direction Gate (Phase 1).

최상위 진입 필터: 1h+15m EMA cross + ADX hysteresis로 방향 결정.
ff_unified_engine_v11 뒤에 배치.

Direction:
  LONG_ONLY  — 1h+15m 모두 EMA50>EMA200 + ADX confirmed
  SHORT_ONLY — 1h+15m 모두 EMA50<EMA200 + ADX confirmed
  NO_TRADE   — 조건 불충분 또는 conflicting

FAIL-OPEN: 에러 시 NO_TRADE 반환 (안전).
"""

import time

LOG_PREFIX = '[mtf_dir]'

LONG_ONLY = 'LONG_ONLY'
SHORT_ONLY = 'SHORT_ONLY'
NO_TRADE = 'NO_TRADE'

# ADX Hysteresis state machine
_adx_was_above_enter = False


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _get_config():
    """Load v1.1 config params. Returns dict with defaults."""
    try:
        from strategy_v3 import config_v3
        cfg = config_v3.get_all()
        v11 = cfg.get('unified_v11', {}) if isinstance(cfg.get('unified_v11'), dict) else {}
    except Exception:
        v11 = {}
    return {
        'adx_enter': v11.get('adx_enter_threshold', 27),
        'adx_keep': v11.get('adx_keep_threshold', 23),
        'staleness_sec': 120,
    }


def _fetch_mtf(cur, symbol='BTC/USDT:USDT'):
    """Fetch MTF indicators from DB."""
    try:
        cur.execute("""
            SELECT ema_15m_50, ema_15m_200, ema_1h_50, ema_1h_200,
                   adx_1h, donchian_high_15m_20, donchian_low_15m_20,
                   atr_15m, updated_at
            FROM mtf_indicators
            WHERE symbol = %s
            LIMIT 1;
        """, (symbol,))
        row = cur.fetchone()
        if not row:
            return None
        return {
            'ema_15m_50': float(row[0]) if row[0] else 0,
            'ema_15m_200': float(row[1]) if row[1] else 0,
            'ema_1h_50': float(row[2]) if row[2] else 0,
            'ema_1h_200': float(row[3]) if row[3] else 0,
            'adx_1h': float(row[4]) if row[4] else 0,
            'donchian_high_15m_20': float(row[5]) if row[5] else 0,
            'donchian_low_15m_20': float(row[6]) if row[6] else 0,
            'atr_15m': float(row[7]) if row[7] else 0,
            'updated_at': row[8],
        }
    except Exception as e:
        _log(f'_fetch_mtf error: {e}')
        return None


def _is_stale(row, max_age_sec=120):
    """Check if MTF data is stale."""
    if not row or not row.get('updated_at'):
        return True
    try:
        import datetime
        updated = row['updated_at']
        if hasattr(updated, 'timestamp'):
            age = time.time() - updated.timestamp()
        else:
            age = max_age_sec + 1  # unknown format → stale
        return age > max_age_sec
    except Exception:
        return True


def _compute_adx_state(adx_1h, cfg=None):
    """ADX hysteresis state machine.

    Returns: 'ENTER' (>27), 'KEEP' (>23 after ENTER), 'BELOW'
    """
    global _adx_was_above_enter
    if cfg is None:
        cfg = _get_config()

    adx_enter = cfg['adx_enter']
    adx_keep = cfg['adx_keep']

    if adx_1h >= adx_enter:
        _adx_was_above_enter = True
        return 'ENTER'
    elif adx_1h >= adx_keep and _adx_was_above_enter:
        return 'KEEP'
    else:
        _adx_was_above_enter = False
        return 'BELOW'


def compute_mtf_direction(cur, symbol='BTC/USDT:USDT'):
    """Compute MTF direction gate.

    Returns: dict with:
        direction: LONG_ONLY | SHORT_ONLY | NO_TRADE
        adx_1h: float
        adx_state: ENTER | KEEP | BELOW
        ema_1h_50, ema_1h_200, ema_15m_50, ema_15m_200: float
        trend_confirmed: bool
        reasons: list[str]
    """
    try:
        cfg = _get_config()
        row = _fetch_mtf(cur, symbol)

        base = {
            'direction': NO_TRADE,
            'adx_1h': 0, 'adx_state': 'BELOW',
            'ema_1h_50': 0, 'ema_1h_200': 0,
            'ema_15m_50': 0, 'ema_15m_200': 0,
            'trend_confirmed': False,
            'reasons': [],
        }

        if not row:
            base['reasons'] = ['MTF data missing']
            return base

        if _is_stale(row, cfg['staleness_sec']):
            base['reasons'] = ['MTF data stale']
            return base

        adx_1h = row['adx_1h']
        adx_state = _compute_adx_state(adx_1h, cfg)
        trend_confirmed = adx_state in ('ENTER', 'KEEP')

        result = {
            'adx_1h': adx_1h,
            'adx_state': adx_state,
            'ema_1h_50': row['ema_1h_50'],
            'ema_1h_200': row['ema_1h_200'],
            'ema_15m_50': row['ema_15m_50'],
            'ema_15m_200': row['ema_15m_200'],
            'donchian_high_15m_20': row.get('donchian_high_15m_20', 0),
            'donchian_low_15m_20': row.get('donchian_low_15m_20', 0),
            'atr_15m': row.get('atr_15m', 0),
            'trend_confirmed': trend_confirmed,
            'reasons': [],
        }

        if not trend_confirmed:
            result['direction'] = NO_TRADE
            result['reasons'] = [f'ADX {adx_1h:.1f} < {cfg["adx_keep"]} (state={adx_state})']
            return result

        # EMA Cross Direction
        long_cond = (row['ema_1h_50'] > row['ema_1h_200'] and
                     row['ema_15m_50'] > row['ema_15m_200'])
        short_cond = (row['ema_1h_50'] < row['ema_1h_200'] and
                      row['ema_15m_50'] < row['ema_15m_200'])

        if long_cond:
            result['direction'] = LONG_ONLY
            result['reasons'] = [
                f'ADX={adx_1h:.1f} ({adx_state})',
                f'1h EMA50({row["ema_1h_50"]:.0f})>200({row["ema_1h_200"]:.0f})',
                f'15m EMA50({row["ema_15m_50"]:.0f})>200({row["ema_15m_200"]:.0f})',
            ]
        elif short_cond:
            result['direction'] = SHORT_ONLY
            result['reasons'] = [
                f'ADX={adx_1h:.1f} ({adx_state})',
                f'1h EMA50({row["ema_1h_50"]:.0f})<200({row["ema_1h_200"]:.0f})',
                f'15m EMA50({row["ema_15m_50"]:.0f})<200({row["ema_15m_200"]:.0f})',
            ]
        else:
            result['direction'] = NO_TRADE
            result['reasons'] = [
                f'ADX={adx_1h:.1f} ({adx_state}) but EMA cross conflicting',
                f'1h: 50={row["ema_1h_50"]:.0f} vs 200={row["ema_1h_200"]:.0f}',
                f'15m: 50={row["ema_15m_50"]:.0f} vs 200={row["ema_15m_200"]:.0f}',
            ]

        return result

    except Exception as e:
        _log(f'compute_mtf_direction error: {e}')
        return {
            'direction': NO_TRADE,
            'adx_1h': 0, 'adx_state': 'BELOW',
            'ema_1h_50': 0, 'ema_1h_200': 0,
            'ema_15m_50': 0, 'ema_15m_200': 0,
            'trend_confirmed': False,
            'reasons': [f'error: {e}'],
        }
