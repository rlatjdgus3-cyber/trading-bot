"""
trend_triggers.py — Trend-Follow Entry Triggers (Phase 2).

Donchian(20) breakout on 15m + optional EMA20 pullback.
ff_unified_engine_v11 뒤에 배치.

FAIL-OPEN: 에러 시 triggered=False 반환 (진입 안 함).
"""

LOG_PREFIX = '[trend_trig]'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _fetch_mtf(cur, symbol='BTC/USDT:USDT'):
    """Fetch MTF indicators from DB."""
    try:
        cur.execute("""
            SELECT donchian_high_15m_20, donchian_low_15m_20, atr_15m
            FROM mtf_indicators
            WHERE symbol = %s
            LIMIT 1;
        """, (symbol,))
        row = cur.fetchone()
        if not row:
            return None
        return {
            'donchian_high_15m_20': float(row[0]) if row[0] else 0,
            'donchian_low_15m_20': float(row[1]) if row[1] else 0,
            'atr_15m': float(row[2]) if row[2] else 0,
        }
    except Exception as e:
        _log(f'_fetch_mtf error: {e}')
        return None


def evaluate_breakout(cur, symbol, mtf_direction, features):
    """Donchian(20) on 15m breakout check.

    Args:
        cur: DB cursor
        symbol: trading symbol
        mtf_direction: 'LONG_ONLY' | 'SHORT_ONLY' | 'NO_TRADE'
        features: dict with price, volume_z, atr_change_rate etc.

    Returns: dict with:
        triggered: bool
        side: 'LONG' | 'SHORT' | None
        trigger_price: float
        filter_passed: bool
        reasons: list[str]
    """
    try:
        if mtf_direction == 'NO_TRADE':
            return {'triggered': False, 'side': None, 'trigger_price': 0,
                    'filter_passed': False, 'reasons': ['MTF=NO_TRADE']}

        mtf_row = _fetch_mtf(cur, symbol)
        if not mtf_row:
            return {'triggered': False, 'side': None, 'trigger_price': 0,
                    'filter_passed': False, 'reasons': ['MTF data unavailable']}

        price = features.get('price', 0) if features else 0
        if price <= 0:
            return {'triggered': False, 'side': None, 'trigger_price': 0,
                    'filter_passed': False, 'reasons': ['price unavailable']}

        volume_z = features.get('volume_z', 0) if features else 0
        atr_change_rate = features.get('atr_change_rate', 0) if features else 0

        if mtf_direction == 'LONG_ONLY':
            dc_high = mtf_row['donchian_high_15m_20']
            if dc_high > 0 and price > dc_high:
                # Volume or ATR filter
                vol_ok = volume_z > 0
                atr_ok = atr_change_rate > 0
                filter_passed = vol_ok or atr_ok
                reasons = [
                    f'LONG breakout: price {price:.1f} > DC20_high {dc_high:.1f}',
                    f'vol_z={volume_z:.2f} ({"OK" if vol_ok else "FAIL"})',
                    f'atr_chg={atr_change_rate:.3f} ({"OK" if atr_ok else "FAIL"})',
                ]
                if filter_passed:
                    return {'triggered': True, 'side': 'LONG',
                            'trigger_price': dc_high, 'filter_passed': True,
                            'reasons': reasons}
                else:
                    reasons.append('filter not passed (vol+atr both <= 0)')
                    return {'triggered': False, 'side': 'LONG',
                            'trigger_price': dc_high, 'filter_passed': False,
                            'reasons': reasons}

        elif mtf_direction == 'SHORT_ONLY':
            dc_low = mtf_row['donchian_low_15m_20']
            if dc_low > 0 and price < dc_low:
                vol_ok = volume_z > 0
                atr_ok = atr_change_rate > 0
                filter_passed = vol_ok or atr_ok
                reasons = [
                    f'SHORT breakout: price {price:.1f} < DC20_low {dc_low:.1f}',
                    f'vol_z={volume_z:.2f} ({"OK" if vol_ok else "FAIL"})',
                    f'atr_chg={atr_change_rate:.3f} ({"OK" if atr_ok else "FAIL"})',
                ]
                if filter_passed:
                    return {'triggered': True, 'side': 'SHORT',
                            'trigger_price': dc_low, 'filter_passed': True,
                            'reasons': reasons}
                else:
                    reasons.append('filter not passed (vol+atr both <= 0)')
                    return {'triggered': False, 'side': 'SHORT',
                            'trigger_price': dc_low, 'filter_passed': False,
                            'reasons': reasons}

        return {'triggered': False, 'side': None, 'trigger_price': 0,
                'filter_passed': False,
                'reasons': [f'no breakout (dir={mtf_direction})']}

    except Exception as e:
        _log(f'evaluate_breakout error: {e}')
        return {'triggered': False, 'side': None, 'trigger_price': 0,
                'filter_passed': False, 'reasons': [f'error: {e}']}


def evaluate_pullback(cur, symbol, mtf_direction, features):
    """EMA20 pullback on 15m (optional trigger, OFF by default).

    Requires ff_pullback_trigger feature flag to be enabled.

    Returns: dict similar to evaluate_breakout.
    """
    try:
        import feature_flags
        if not feature_flags.is_enabled('ff_pullback_trigger'):
            return {'triggered': False, 'side': None, 'trigger_price': 0,
                    'filter_passed': False, 'reasons': ['pullback trigger OFF']}

        if mtf_direction == 'NO_TRADE':
            return {'triggered': False, 'side': None, 'trigger_price': 0,
                    'filter_passed': False, 'reasons': ['MTF=NO_TRADE']}

        # Pullback requires stronger ADX (>30)
        mtf_row = _fetch_mtf(cur, symbol)
        if not mtf_row:
            return {'triggered': False, 'side': None, 'trigger_price': 0,
                    'filter_passed': False, 'reasons': ['MTF data unavailable']}

        # Fetch ADX from mtf_indicators
        try:
            cur.execute("SELECT adx_1h FROM mtf_indicators WHERE symbol = %s;", (symbol,))
            r = cur.fetchone()
            adx_1h = float(r[0]) if r and r[0] else 0
        except Exception:
            adx_1h = 0

        if adx_1h < 30:
            return {'triggered': False, 'side': None, 'trigger_price': 0,
                    'filter_passed': False,
                    'reasons': [f'ADX {adx_1h:.1f} < 30 for pullback']}

        # EMA20 pullback: price within 0.5*ATR of EMA20
        ema_20 = features.get('ema_21', 0) if features else 0  # using ema_21 as proxy
        price = features.get('price', 0) if features else 0
        atr_15m = mtf_row.get('atr_15m', 0)

        if ema_20 <= 0 or price <= 0 or atr_15m <= 0:
            return {'triggered': False, 'side': None, 'trigger_price': 0,
                    'filter_passed': False, 'reasons': ['pullback data unavailable']}

        dist = abs(price - ema_20)
        threshold = 0.5 * atr_15m

        if dist <= threshold:
            side = 'LONG' if mtf_direction == 'LONG_ONLY' else 'SHORT'
            # Direction check: LONG needs price near EMA from above, SHORT from below
            if (side == 'LONG' and price >= ema_20) or \
               (side == 'SHORT' and price <= ema_20):
                return {'triggered': True, 'side': side,
                        'trigger_price': ema_20, 'filter_passed': True,
                        'reasons': [
                            f'pullback: |price-EMA20|={dist:.1f} <= 0.5*ATR={threshold:.1f}',
                            f'ADX={adx_1h:.1f} >= 30',
                        ]}

        return {'triggered': False, 'side': None, 'trigger_price': 0,
                'filter_passed': False,
                'reasons': [f'no pullback (dist={dist:.1f} vs thresh={threshold:.1f})']}

    except Exception as e:
        _log(f'evaluate_pullback error: {e}')
        return {'triggered': False, 'side': None, 'trigger_price': 0,
                'filter_passed': False, 'reasons': [f'error: {e}']}
