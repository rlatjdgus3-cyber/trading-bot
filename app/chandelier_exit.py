"""
chandelier_exit.py — Chandelier Exit + Time Stop (Phase 4).

Trend-Follow 청산 엔진:
  - Chandelier trailing stop: highest/lowest since entry - multiplier * ATR
  - Time stop: 12h + <1R unrealized → close

ff_unified_engine_v11 뒤에 배치.

FAIL-OPEN: 에러 시 None 반환 (청산 안 함, 기존 SL에 의존).
"""

import time

LOG_PREFIX = '[chandelier]'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _get_config():
    """Load v1.1 config params from top-level unified_v11 YAML section."""
    try:
        import yaml, os
        _path = os.path.join(os.path.dirname(__file__), 'config', 'strategy_modes.yaml')
        with open(_path, 'r') as _f:
            _full = yaml.safe_load(_f) or {}
        v11 = _full.get('unified_v11', {})
        if not isinstance(v11, dict):
            v11 = {}
    except Exception:
        v11 = {}
    return {
        'trail_atr_mult': v11.get('trail_atr_mult', 2.5),
        'time_stop_hours': v11.get('time_stop_hours', 12),
        'time_stop_min_r': v11.get('time_stop_min_r', 1.0),
        'sl_atr_mult': v11.get('sl_atr_mult', 2.0),
    }


class ChandelierTracker:
    """Tracks highest/lowest price since entry for Chandelier exit."""

    def __init__(self):
        self.highest_since_entry = None
        self.lowest_since_entry = None
        self.entry_time = None
        self.entry_price = None
        self.entry_side = None
        self.sl_distance = None  # initial SL distance for R calculation
        self.time_stop_hours = 12  # can be tightened by regime shift

    def on_entry(self, side, entry_price, sl_distance=None):
        """Called when a new position is opened."""
        self.entry_price = entry_price
        self.entry_side = side
        self.entry_time = time.time()
        self.highest_since_entry = entry_price
        self.lowest_since_entry = entry_price
        self.sl_distance = sl_distance
        cfg = _get_config()
        self.time_stop_hours = cfg['time_stop_hours']
        _log(f'on_entry: side={side} price={entry_price} sl_dist={sl_distance}')

    def on_close(self):
        """Called when position is closed. Reset state."""
        self.highest_since_entry = None
        self.lowest_since_entry = None
        self.entry_time = None
        self.entry_price = None
        self.entry_side = None
        self.sl_distance = None

    def update(self, current_price):
        """Update highest/lowest tracking."""
        if self.highest_since_entry is None:
            return
        self.highest_since_entry = max(self.highest_since_entry, current_price)
        self.lowest_since_entry = min(self.lowest_since_entry, current_price)

    def compute_trail_sl(self, atr_15m, multiplier=None):
        """Compute Chandelier trailing stop price."""
        if self.entry_side is None or self.highest_since_entry is None:
            return None
        cfg = _get_config()
        mult = multiplier or cfg['trail_atr_mult']

        if self.entry_side in ('long', 'LONG'):
            return self.highest_since_entry - (mult * atr_15m)
        else:
            return self.lowest_since_entry + (mult * atr_15m)

    def check_exit(self, current_price, atr_15m):
        """Check if Chandelier exit or time stop is triggered.

        Returns: ('CLOSE', reason) or None
        """
        if self.entry_side is None or self.entry_price is None:
            return None

        try:
            side = self.entry_side
            cfg = _get_config()

            # 1. Chandelier trailing stop
            trail_sl = self.compute_trail_sl(atr_15m)
            if trail_sl is not None:
                if side in ('long', 'LONG') and current_price <= trail_sl:
                    return ('CLOSE',
                            f'Chandelier: price {current_price:.1f} <= trail {trail_sl:.1f} '
                            f'(high={self.highest_since_entry:.1f})')
                if side in ('short', 'SHORT') and current_price >= trail_sl:
                    return ('CLOSE',
                            f'Chandelier: price {current_price:.1f} >= trail {trail_sl:.1f} '
                            f'(low={self.lowest_since_entry:.1f})')

            # 2. Time stop: N hours + <1R unrealized
            if self.entry_time:
                hours_held = (time.time() - self.entry_time) / 3600
                unrealized_r = self._compute_unrealized_r(current_price)

                if hours_held >= self.time_stop_hours and unrealized_r < cfg['time_stop_min_r']:
                    return ('CLOSE',
                            f'TimeStop: {hours_held:.1f}h >= {self.time_stop_hours}h '
                            f'+ {unrealized_r:.2f}R < {cfg["time_stop_min_r"]}R')

            return None

        except Exception as e:
            _log(f'check_exit error (FAIL-OPEN): {e}')
            return None

    def _compute_unrealized_r(self, current_price):
        """Compute unrealized profit in R multiples."""
        if not self.entry_price or not self.sl_distance or self.sl_distance <= 0:
            # Fallback: use ATR-based SL distance
            return 0.0

        if self.entry_side in ('long', 'LONG'):
            pnl = current_price - self.entry_price
        else:
            pnl = self.entry_price - current_price

        return pnl / self.sl_distance

    def get_status(self):
        """Return status dict for debugging."""
        hours_held = 0
        if self.entry_time:
            hours_held = (time.time() - self.entry_time) / 3600
        return {
            'entry_side': self.entry_side,
            'entry_price': self.entry_price,
            'highest': self.highest_since_entry,
            'lowest': self.lowest_since_entry,
            'sl_distance': self.sl_distance,
            'hours_held': round(hours_held, 2),
            'time_stop_hours': self.time_stop_hours,
        }


# Module-level singleton
_chandelier_tracker = ChandelierTracker()


def get_tracker():
    """Get singleton ChandelierTracker."""
    return _chandelier_tracker
