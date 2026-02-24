"""
strategy.modes.shock_breakout — MODE_C: Shock / Breakout Strategy.

Entry: NO entry on first breakout candle (absolute rule).
  - Retest entry: price returns to old VAH/VAL, holds support/resistance.
  - Hold entry: 2+ candles outside VA with sustained volume → enter on pullback.
TP: TP1 0.6-1.0% partial, then trailing.
SL: level-based (old VAH/VAL breach) + ATR hybrid.
ADD: max 1-2 stages, retest re-confirmation only.
Transition: if "new box forming" → partial exit, switch to MODE_B REBUILD.
"""

from strategy.modes.base import ModeStrategy
from strategy.common import risk, dedupe

LOG_PREFIX = '[mode_c]'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


class ShockBreakoutStrategy(ModeStrategy):
    MODE = 'C'

    def decide(self, ctx):
        features = ctx.get('features', {})
        position = ctx.get('position')
        config = ctx.get('config', {})
        regime_ctx = ctx.get('regime_ctx', {})
        price = features.get('price')
        vah = features.get('vah')
        val = features.get('val')
        poc = features.get('poc')
        volume_z = features.get('volume_z')
        impulse = features.get('impulse')
        candles = ctx.get('candles', [])

        if price is None or vah is None or val is None:
            return self._hold('MODE_C: missing price/vah/val data')

        buffer_pct = config.get('breakout_buffer_pct', 0.002)
        news_override_min = config.get('news_override_min_score', 60)
        trailing_enabled = config.get('trailing_stop_enabled', True)

        # ── If position exists: evaluate ADD, EXIT, or transition ──
        if position and position.get('side'):
            return self._evaluate_position(ctx, features, position, config)

        # ── No position: evaluate ENTRY ──

        # Determine breakout direction
        breakout_side = None
        old_level = None
        if price > vah * (1 + buffer_pct):
            breakout_side = 'LONG'
            old_level = vah
        elif price < val * (1 - buffer_pct):
            breakout_side = 'SHORT'
            old_level = val
        else:
            return self._hold('MODE_C: price inside VA, no breakout signal')

        # News override: check if news aligns with breakout direction
        news_score = regime_ctx.get('news_score', 0) if regime_ctx else 0
        news_aligns = False
        if abs(news_score) >= news_override_min:
            news_dir = 'LONG' if news_score > 0 else 'SHORT'
            news_aligns = (news_dir == breakout_side)

        # ABSOLUTE RULE: No entry on first breakout candle
        # News override: relax confirmation from 2→1 candles when aligned
        is_first_candle = self._is_first_breakout_candle(candles, vah, val, buffer_pct)
        if is_first_candle and not news_aligns:
            return self._hold(
                f'MODE_C: FIRST BREAKOUT CANDLE — no entry (side={breakout_side})',
                meta={'first_breakout_candle': True, 'chase_entry': True}
            )

        # Check for retest entry
        is_retest = self._check_retest_entry(candles, breakout_side, old_level, buffer_pct)
        # Check for hold entry (2+ candles outside VA with volume)
        is_hold_entry = self._check_hold_entry(candles, vah, val, volume_z, config)

        if not is_retest and not is_hold_entry and not news_aligns:
            return self._hold(
                f'MODE_C: waiting for retest or sustained hold outside VA (side={breakout_side})')

        entry_type = 'retest' if is_retest else ('hold' if is_hold_entry else 'news_override')
        reason_parts = [f'breakout_{breakout_side}', f'entry_type={entry_type}']
        if news_aligns:
            reason_parts.append(f'news_override(score={news_score})')

        # Compute TP/SL
        atr_val = self._get_atr_from_ctx(ctx)
        tp = risk.compute_tp('C', price, breakout_side, poc, config)
        sl = risk.compute_sl('C', price, breakout_side, atr_val, old_level, config)
        leverage = risk.clamp_leverage('C', 5, config)

        signal_key = dedupe.make_signal_key(
            features.get('symbol', 'BTC/USDT:USDT'), 'C', breakout_side, stage=1)

        # Use stop order for retest entries
        order_type = 'stop' if entry_type == 'retest' else 'market'

        meta = {
            'leverage': leverage,
            'old_level': old_level,
            'entry_type': entry_type,
            'mode': 'C',
        }
        # Trailing stop config
        if trailing_enabled:
            meta['trailing_activate_pct'] = config.get('trailing_activate_pct', 0.008)
            meta['trailing_pct'] = config.get('trailing_pct', 0.005)

        return {
            'action': 'ENTER',
            'side': breakout_side,
            'qty': None,
            'tp': tp,
            'sl': sl,
            'reason': f'MODE_C ENTER: {"; ".join(reason_parts)}',
            'signal_key': signal_key,
            'chase_entry': False,
            'order_type': order_type,
            'meta': meta,
        }

    def _evaluate_position(self, ctx, features, position, config):
        """Evaluate ADD, EXIT, or mode transition for existing position."""
        price = features.get('price')
        vah = features.get('vah')
        val = features.get('val')
        volume_z = features.get('volume_z')
        pos_side = position.get('side', '').upper()
        stage = position.get('stage', 1)
        max_stages = config.get('max_add_stages', 2)
        candles = ctx.get('candles', [])
        buffer_pct = config.get('breakout_buffer_pct', 0.002)

        # Check for "new box forming" → partial exit transition
        new_box = self._check_new_box_forming(candles, vah, val)
        if new_box:
            return {
                'action': 'EXIT',
                'side': pos_side,
                'qty': None,
                'tp': None,
                'sl': None,
                'reason': 'MODE_C EXIT: new box forming, transition to MODE_B REBUILD',
                'signal_key': None,
                'chase_entry': False,
                'order_type': 'market',
                'meta': {'exit_type': 'new_box_transition', 'transition_to': 'B'},
            }

        # ADD: check stage limit
        if stage >= max_stages:
            return self._hold(f'MODE_C: stage {stage} >= max {max_stages}')

        # ADD: retest re-confirmation at old level
        old_level = vah if pos_side == 'LONG' else val
        is_retest = self._check_retest_entry(candles, pos_side, old_level, buffer_pct)
        if not is_retest:
            return self._hold(f'MODE_C: ADD waiting retest confirmation at {old_level:.1f}')

        atr_val = self._get_atr_from_ctx(ctx)
        tp = risk.compute_tp('C', price, pos_side, None, config)
        sl = risk.compute_sl('C', price, pos_side, atr_val, old_level, config)
        signal_key = dedupe.make_signal_key(
            features.get('symbol', 'BTC/USDT:USDT'), 'C', pos_side, stage=stage + 1)

        return {
            'action': 'ADD',
            'side': pos_side,
            'qty': None,
            'tp': tp,
            'sl': sl,
            'reason': f'MODE_C ADD: stage {stage + 1}, retest confirmed',
            'signal_key': signal_key,
            'chase_entry': False,
            'order_type': 'market',
            'meta': {'stage': stage + 1, 'mode': 'C'},
        }

    def _is_first_breakout_candle(self, candles, vah, val, buffer_pct):
        """Check if current candle is the first breakout candle.

        First breakout = current candle outside VA but previous candle was inside.
        """
        if not candles or len(candles) < 2:
            return True  # Err on side of caution

        def _close(c):
            if isinstance(c, dict):
                v = c.get('c') or c.get('close')
                return float(v) if v is not None else None
            return None

        c0 = _close(candles[0])
        c1 = _close(candles[1])

        if c0 is None or c1 is None:
            return True

        upper = vah * (1 + buffer_pct)
        lower = val * (1 - buffer_pct)

        current_outside = c0 > upper or c0 < lower
        prev_inside = lower <= c1 <= upper

        return current_outside and prev_inside

    def _check_retest_entry(self, candles, side, old_level, buffer_pct):
        """Check for retest: price returned to old VAH/VAL and held.

        For LONG breakout: price dipped back toward VAH (from above), then bounced.
        For SHORT breakout: price rose back toward VAL (from below), then dropped.
        """
        if not candles or len(candles) < 3:
            return False

        def _vals(c):
            if isinstance(c, dict):
                def _v(k1, k2):
                    v = c.get(k1)
                    return float(v) if v is not None else (float(c[k2]) if c.get(k2) is not None else 0.0)
                return (_v('l', 'low'), _v('h', 'high'), _v('c', 'close'))
            return (0, 0, 0)

        retest_zone = old_level * buffer_pct * 3  # Wider zone for retest detection

        if side == 'LONG':
            # Look for dip toward VAH in recent candles
            for c in candles[1:5]:
                low, high, close = _vals(c)
                if low <= old_level + retest_zone and close > old_level:
                    return True
        elif side == 'SHORT':
            # Look for spike toward VAL in recent candles
            for c in candles[1:5]:
                low, high, close = _vals(c)
                if high >= old_level - retest_zone and close < old_level:
                    return True

        return False

    def _check_hold_entry(self, candles, vah, val, volume_z, config):
        """Check for hold entry: 2+ candles outside VA with sustained volume."""
        vol_z_min = config.get('volume_z_min', 2.0)
        if not candles or len(candles) < 3:
            return False

        outside_count = 0
        for c in candles[1:4]:  # Check candles [1], [2], [3]
            if isinstance(c, dict):
                close = c.get('c') or c.get('close')
                if close is not None:
                    close = float(close)
                    if close > vah or close < val:
                        outside_count += 1

        # Need 2+ outside + volume still elevated (at least half threshold)
        volume_ok = volume_z is not None and volume_z >= vol_z_min * 0.5
        return outside_count >= 2 and volume_ok

    def _check_new_box_forming(self, candles, vah, val):
        """Detect if a new consolidation box is forming after breakout.

        Signal: price returned inside VA for 5+ of last 8 candles.
        """
        if not candles or len(candles) < 8:
            return False

        inside_count = 0
        for c in candles[:8]:
            if isinstance(c, dict):
                close = c.get('c') or c.get('close')
                if close is not None:
                    close = float(close)
                    if val <= close <= vah:
                        inside_count += 1

        return inside_count >= 5

    def _get_atr_from_ctx(self, ctx):
        """Extract ATR value from context indicators."""
        indicators = ctx.get('indicators', {})
        if isinstance(indicators, dict):
            atr = indicators.get('atr_14')
            if atr is not None:
                return float(atr)
        return None
