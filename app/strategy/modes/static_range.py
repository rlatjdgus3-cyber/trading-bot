"""
strategy.modes.static_range — MODE_A: Static Range Strategy.

Entry: edge-fade at VAL/VAH with confirmation (rejection wick / higher-low).
Anti-chase: block if prior candle impulse is large in entry direction.
Prefer limit orders.
TP: 0.4-0.8% or POC.  SL: 0.5-0.9% or VA breach.
ADD: max 3 stages, price-level based + cooldown.
EXIT: partial at POC, full at opposite VA edge.
Cooldown: 3-5 min same-direction re-entry.
"""

from strategy.modes.base import ModeStrategy
from strategy.common import risk, dedupe

LOG_PREFIX = '[mode_a]'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


class StaticRangeStrategy(ModeStrategy):
    MODE = 'A'

    def decide(self, ctx):
        features = ctx.get('features', {})
        position = ctx.get('position')
        config = ctx.get('config', {})
        price = features.get('price')
        vah = features.get('vah')
        val = features.get('val')
        poc = features.get('poc')
        rp = features.get('range_position')
        impulse = features.get('impulse')
        candles = ctx.get('candles', [])

        if price is None or vah is None or val is None:
            return self._hold('MODE_A: missing price/vah/val data')

        rp_long = config.get('range_position_long', 0.15)
        rp_short = config.get('range_position_short', 0.85)
        anti_chase_max = config.get('anti_chase_impulse_max', 0.6)

        # ── If position exists: evaluate ADD or EXIT ──
        if position and position.get('side'):
            return self._evaluate_position(ctx, features, position, config)

        # ── No position: evaluate ENTRY ──
        if rp is None:
            return self._hold('MODE_A: range_position unavailable')

        side = None
        reason_parts = []

        # LONG: near VAL
        if rp <= rp_long:
            side = 'LONG'
            reason_parts.append(f'range_pos={rp:.3f} <= {rp_long} (near VAL)')
        # SHORT: near VAH
        elif rp >= rp_short:
            side = 'SHORT'
            reason_parts.append(f'range_pos={rp:.3f} >= {rp_short} (near VAH)')
        else:
            return self._hold(f'MODE_A: mid-range (rp={rp:.3f}), no edge entry')

        # Anti-chase: block if prior candle impulse > threshold in entry direction
        chase_entry = False
        if impulse is not None and impulse > anti_chase_max:
            # Check if impulse is in entry direction
            if len(candles) >= 1:
                last = candles[0] if isinstance(candles[0], dict) else {}
                c_open = last.get('o') or last.get('open')
                c_close = last.get('c') or last.get('close')
                if c_open is not None and c_close is not None:
                    candle_dir = 'LONG' if float(c_close) > float(c_open) else 'SHORT'
                    if candle_dir == side:
                        chase_entry = True
                        return self._hold(
                            f'MODE_A: ANTI_CHASE — impulse={impulse:.2f} > {anti_chase_max} '
                            f'in {side} direction',
                            meta={'chase_entry': True}
                        )

        # Confirmation: check for rejection wick or higher-low/lower-high
        confirmed = self._check_confirmation(candles, side)
        if not confirmed:
            return self._hold(
                f'MODE_A: waiting confirmation for {side} (rp={rp:.3f})',
                meta={'near_edge': True}
            )

        # Compute TP/SL
        level_price = val if side == 'LONG' else vah
        atr_val = self._get_atr_from_ctx(ctx)
        tp = risk.compute_tp('A', price, side, poc, config)
        sl = risk.compute_sl('A', price, side, atr_val, level_price, config)
        leverage = risk.clamp_leverage('A', 3, config)

        signal_key = dedupe.make_signal_key(
            features.get('symbol', 'BTC/USDT:USDT'), 'A', side, stage=1)

        reason_parts.append('edge confirmed')
        return {
            'action': 'ENTER',
            'side': side,
            'qty': None,  # qty computed by caller based on capital
            'tp': tp,
            'sl': sl,
            'reason': f'MODE_A ENTER: {"; ".join(reason_parts)}',
            'signal_key': signal_key,
            'chase_entry': False,
            'order_type': 'limit',
            'meta': {
                'leverage': leverage,
                'range_position': rp,
                'mode': 'A',
            },
        }

    def _evaluate_position(self, ctx, features, position, config):
        """Evaluate ADD or EXIT for existing position."""
        price = features.get('price')
        poc = features.get('poc')
        vah = features.get('vah')
        val = features.get('val')
        rp = features.get('range_position')
        pos_side = position.get('side', '').upper()
        stage = position.get('stage', 1)
        max_stages = config.get('max_add_stages', 3)

        if price is None:
            return self._hold('MODE_A: missing price for position eval')

        # EXIT: at opposite VA edge
        if pos_side == 'LONG' and rp is not None and rp >= 0.85:
            return {
                'action': 'EXIT',
                'side': pos_side,
                'qty': None,
                'tp': None,
                'sl': None,
                'reason': f'MODE_A EXIT: LONG at opposite edge (rp={rp:.3f})',
                'signal_key': None,
                'chase_entry': False,
                'order_type': 'market',
                'meta': {'exit_type': 'opposite_edge'},
            }
        if pos_side == 'SHORT' and rp is not None and rp <= 0.15:
            return {
                'action': 'EXIT',
                'side': pos_side,
                'qty': None,
                'tp': None,
                'sl': None,
                'reason': f'MODE_A EXIT: SHORT at opposite edge (rp={rp:.3f})',
                'signal_key': None,
                'chase_entry': False,
                'order_type': 'market',
                'meta': {'exit_type': 'opposite_edge'},
            }

        # ADD: check stage limit and cooldown
        if stage >= max_stages:
            return self._hold(f'MODE_A: stage {stage} >= max {max_stages}')

        # ADD: price must be favorable (deeper into edge)
        rp_long = config.get('range_position_long', 0.15)
        rp_short = config.get('range_position_short', 0.85)

        add_eligible = False
        if pos_side == 'LONG' and rp is not None and rp <= rp_long:
            add_eligible = True
        elif pos_side == 'SHORT' and rp is not None and rp >= rp_short:
            add_eligible = True

        if not add_eligible:
            return self._hold(f'MODE_A: ADD not eligible (rp={rp}, side={pos_side})')

        level_price = val if pos_side == 'LONG' else vah
        atr_val = self._get_atr_from_ctx(ctx)
        tp = risk.compute_tp('A', price, pos_side, poc, config)
        sl = risk.compute_sl('A', price, pos_side, atr_val, level_price, config)

        signal_key = dedupe.make_signal_key(
            features.get('symbol', 'BTC/USDT:USDT'), 'A', pos_side, stage=stage + 1)

        return {
            'action': 'ADD',
            'side': pos_side,
            'qty': None,
            'tp': tp,
            'sl': sl,
            'reason': f'MODE_A ADD: stage {stage + 1}, rp={rp:.3f}',
            'signal_key': signal_key,
            'chase_entry': False,
            'order_type': 'limit',
            'meta': {
                'stage': stage + 1,
                'mode': 'A',
            },
        }

    def _check_confirmation(self, candles, side):
        """Check for entry confirmation: rejection wick or higher-low/lower-high.

        Returns True if confirmed, False if waiting.
        """
        if not candles or len(candles) < 2:
            return False

        def _candle_vals(c):
            if isinstance(c, dict):
                return (
                    float(c.get('o') or c.get('open') or 0),
                    float(c.get('h') or c.get('high') or 0),
                    float(c.get('l') or c.get('low') or 0),
                    float(c.get('c') or c.get('close') or 0),
                )
            return (0, 0, 0, 0)

        c0_o, c0_h, c0_l, c0_c = _candle_vals(candles[0])
        c1_o, c1_h, c1_l, c1_c = _candle_vals(candles[1])

        if c0_h == 0 or c1_h == 0:
            return False

        body_0 = abs(c0_c - c0_o)
        range_0 = c0_h - c0_l if c0_h > c0_l else 0.01

        if side == 'LONG':
            # Rejection wick: long lower wick (>50% of range) + close above open
            lower_wick = min(c0_o, c0_c) - c0_l
            if lower_wick > range_0 * 0.5 and c0_c > c0_o:
                return True
            # Higher-low: current low > prior low
            if c0_l > c1_l:
                return True
        elif side == 'SHORT':
            # Rejection wick: long upper wick (>50% of range) + close below open
            upper_wick = c0_h - max(c0_o, c0_c)
            if upper_wick > range_0 * 0.5 and c0_c < c0_o:
                return True
            # Lower-high: current high < prior high
            if c0_h < c1_h:
                return True

        return False

    def _get_atr_from_ctx(self, ctx):
        """Extract ATR value from context indicators."""
        indicators = ctx.get('indicators', {})
        if isinstance(indicators, dict):
            atr = indicators.get('atr_14')
            if atr is not None:
                return float(atr)
        return None
