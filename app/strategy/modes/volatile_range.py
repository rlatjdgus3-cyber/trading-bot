"""
strategy.modes.volatile_range — MODE_B: Volatile / Drifting Range Strategy.

Entry: center-reversion only (POC ± trade_zone). No entries near edges.
First touch at edge = BLOCKED. Only retest entries.
DRIFT_SUBMODE: only trade in drift direction.
TP: 0.3-0.6%.  SL: 0.7-1.2%.
ADD: max 2 stages, drift-aligned + retest only.
REBUILD: VAH/VAL breached 3+ candles → cooldown → re-establish box.
"""

from strategy.modes.base import ModeStrategy
from strategy.common import risk, dedupe

LOG_PREFIX = '[mode_b]'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


class VolatileRangeStrategy(ModeStrategy):
    MODE = 'B'

    def decide(self, ctx):
        features = ctx.get('features', {})
        position = ctx.get('position')
        config = ctx.get('config', {})
        price = features.get('price')
        vah = features.get('vah')
        val = features.get('val')
        poc = features.get('poc')
        rp = features.get('range_position')
        drift_dir = features.get('drift_direction', 'NONE')
        drift_submode = ctx.get('drift_submode')
        candles = ctx.get('candles', [])

        if price is None or vah is None or val is None or poc is None:
            return self._hold('MODE_B: missing price/vah/val/poc data')

        no_trade_pct = config.get('no_trade_zone_edge_pct', 0.002)
        trade_center_pct = config.get('trade_zone_center_pct', 0.003)

        # ── No-trade zones: near edges ──
        upper_no_trade = vah * (1 - no_trade_pct)
        lower_no_trade = val * (1 + no_trade_pct)
        if upper_no_trade <= lower_no_trade:
            return self._hold('MODE_B: range too narrow — entire zone is no-trade')
        if price >= upper_no_trade or price <= lower_no_trade:
            edge = 'VAH' if price >= upper_no_trade else 'VAL'
            return self._hold(
                f'MODE_B: in no-trade zone near {edge} '
                f'(price={price:.1f}, bound={upper_no_trade:.1f}/{lower_no_trade:.1f})')

        # ── Check REBUILD condition: VA breached for 3+ candles ──
        rebuild_needed, rebuild_reason = self._check_rebuild(candles, vah, val, config)
        if rebuild_needed:
            if position and position.get('side'):
                # Existing position during rebuild → partial exit for safety
                return {
                    'action': 'EXIT',
                    'side': position.get('side', '').upper(),
                    'qty': None,
                    'tp': None,
                    'sl': None,
                    'reason': f'MODE_B REBUILD EXIT: {rebuild_reason}',
                    'signal_key': None,
                    'chase_entry': False,
                    'order_type': 'market',
                    'meta': {'exit_type': 'rebuild', 'mode': 'B'},
                }
            return self._hold(f'MODE_B REBUILD: {rebuild_reason}')

        # ── If position exists: evaluate ADD or EXIT ──
        if position and position.get('side'):
            return self._evaluate_position(ctx, features, position, config, drift_submode)

        # ── Trade zone: POC ± center_pct ──
        poc_upper = poc * (1 + trade_center_pct)
        poc_lower = poc * (1 - trade_center_pct)
        if price > poc_upper or price < poc_lower:
            return self._hold(
                f'MODE_B: outside trade zone '
                f'(price={price:.1f}, poc_zone=[{poc_lower:.1f}, {poc_upper:.1f}])')

        # ── Determine entry side based on position relative to POC ──
        side = None
        reason_parts = []

        if price < poc:
            side = 'LONG'
            reason_parts.append(f'price={price:.1f} < poc={poc:.1f} → LONG reversion')
        elif price > poc:
            side = 'SHORT'
            reason_parts.append(f'price={price:.1f} > poc={poc:.1f} → SHORT reversion')
        else:
            return self._hold('MODE_B: at POC, no directional signal')

        # ── DRIFT_SUBMODE: only trade in drift direction ──
        effective_drift = drift_submode or drift_dir
        if effective_drift == 'UP' and side == 'SHORT':
            return self._hold(f'MODE_B: drift=UP, blocking SHORT entry')
        if effective_drift == 'DOWN' and side == 'LONG':
            return self._hold(f'MODE_B: drift=DOWN, blocking LONG entry')

        # ── Require spread_ok + liquidity_ok if configured ──
        if config.get('require_spread_ok', True) and not features.get('spread_ok', True):
            return self._hold('MODE_B: spread too wide, entry blocked')
        if config.get('require_liquidity_ok', True) and not features.get('liquidity_ok', True):
            return self._hold('MODE_B: liquidity too low, entry blocked')

        # ── Require retest (not first touch) ──
        is_retest = self._check_retest(candles, side, poc)
        if not is_retest:
            return self._hold(
                f'MODE_B: first touch at trade zone for {side}, waiting retest')

        # Compute TP/SL — shorter TP target (box midpoint instead of opposite edge)
        atr_val = self._get_atr_from_ctx(ctx)
        # Use midpoint between POC and edge as TP target
        if side == 'LONG' and vah is not None:
            mid_tp_target = (poc + vah) / 2
        elif side == 'SHORT' and val is not None:
            mid_tp_target = (poc + val) / 2
        else:
            mid_tp_target = poc
        tp = risk.compute_tp('B', price, side, mid_tp_target, config)
        sl = risk.compute_sl('B', price, side, atr_val, None, config)
        leverage = risk.clamp_leverage('B', 3, config)

        # Apply size_multiplier (70% of normal sizing)
        size_multiplier = config.get('size_multiplier', 0.7)

        signal_key = dedupe.make_signal_key(
            features.get('symbol', 'BTC/USDT:USDT'), 'B', side, stage=1)

        reason_parts.append('retest confirmed')
        return {
            'action': 'ENTER',
            'side': side,
            'qty': None,
            'tp': tp,
            'sl': sl,
            'reason': f'MODE_B ENTER: {"; ".join(reason_parts)}',
            'signal_key': signal_key,
            'chase_entry': False,
            'order_type': 'market',
            'meta': {
                'leverage': leverage,
                'drift_submode': effective_drift,
                'mode': 'B',
                'size_multiplier': size_multiplier,
            },
        }

    def _evaluate_position(self, ctx, features, position, config, drift_submode):
        """Evaluate ADD or EXIT for existing position."""
        price = features.get('price')
        poc = features.get('poc')
        rp = features.get('range_position')
        drift_dir = features.get('drift_direction', 'NONE')
        pos_side = position.get('side', '').upper()
        stage = position.get('stage', 1)
        max_stages = config.get('max_add_stages', 2)

        # EXIT: if drift reversed against position
        effective_drift = drift_submode or drift_dir
        if pos_side == 'LONG' and effective_drift == 'DOWN':
            return {
                'action': 'EXIT',
                'side': pos_side,
                'qty': None,
                'tp': None,
                'sl': None,
                'reason': f'MODE_B EXIT: LONG but drift reversed to DOWN',
                'signal_key': None,
                'chase_entry': False,
                'order_type': 'market',
                'meta': {'exit_type': 'drift_reversal'},
            }
        if pos_side == 'SHORT' and effective_drift == 'UP':
            return {
                'action': 'EXIT',
                'side': pos_side,
                'qty': None,
                'tp': None,
                'sl': None,
                'reason': f'MODE_B EXIT: SHORT but drift reversed to UP',
                'signal_key': None,
                'chase_entry': False,
                'order_type': 'market',
                'meta': {'exit_type': 'drift_reversal'},
            }

        # ADD: check stage limit
        if stage >= max_stages:
            return self._hold(f'MODE_B: stage {stage} >= max {max_stages}')

        # ADD only if drift-aligned, price favorable, AND retest confirmed
        candles = ctx.get('candles', [])
        if not self._check_retest(candles, pos_side, poc):
            return self._hold(f'MODE_B: ADD waiting retest for {pos_side}')

        if pos_side == 'LONG' and price is not None and poc is not None and price < poc:
            atr_val = self._get_atr_from_ctx(ctx)
            tp = risk.compute_tp('B', price, pos_side, poc, config)
            sl = risk.compute_sl('B', price, pos_side, atr_val, None, config)
            signal_key = dedupe.make_signal_key(
                features.get('symbol', 'BTC/USDT:USDT'), 'B', pos_side, stage=stage + 1)
            return {
                'action': 'ADD',
                'side': pos_side,
                'qty': None,
                'tp': tp,
                'sl': sl,
                'reason': f'MODE_B ADD: LONG stage {stage + 1}, price < POC',
                'signal_key': signal_key,
                'chase_entry': False,
                'order_type': 'market',
                'meta': {'stage': stage + 1, 'mode': 'B'},
            }
        if pos_side == 'SHORT' and price is not None and poc is not None and price > poc:
            atr_val = self._get_atr_from_ctx(ctx)
            tp = risk.compute_tp('B', price, pos_side, poc, config)
            sl = risk.compute_sl('B', price, pos_side, atr_val, None, config)
            signal_key = dedupe.make_signal_key(
                features.get('symbol', 'BTC/USDT:USDT'), 'B', pos_side, stage=stage + 1)
            return {
                'action': 'ADD',
                'side': pos_side,
                'qty': None,
                'tp': tp,
                'sl': sl,
                'reason': f'MODE_B ADD: SHORT stage {stage + 1}, price > POC',
                'signal_key': signal_key,
                'chase_entry': False,
                'order_type': 'market',
                'meta': {'stage': stage + 1, 'mode': 'B'},
            }

        return self._hold(f'MODE_B: position {pos_side} held, no ADD trigger')

    def _check_rebuild(self, candles, vah, val, config):
        """Check if VA was breached for 3+ candles (REBUILD needed).

        Returns (needs_rebuild: bool, reason: str).
        """
        rebuild_candles = config.get('rebuild_cooldown_candles', 10)
        if not candles or len(candles) < 3:
            return (False, '')

        # Count candles outside VA
        outside_count = 0
        for c in candles[:rebuild_candles]:
            if isinstance(c, dict):
                close = c.get('c') or c.get('close')
                if close is not None:
                    close = float(close)
                    if close > vah or close < val:
                        outside_count += 1

        if outside_count >= 3:
            return (True, f'{outside_count} candles outside VA — rebuild cooldown')
        return (False, '')

    def _check_retest(self, candles, side, poc):
        """Check for retest pattern (not first touch).

        Retest: price approached zone, moved away, then returned.
        Simplified: at least 2 of last 5 candles were on the opposite side of POC.
        """
        if not candles or len(candles) < 3:
            return False

        opposite_count = 0
        for c in candles[1:6]:  # Skip current, check last 5
            if isinstance(c, dict):
                close = c.get('c') or c.get('close')
                if close is not None:
                    close = float(close)
                    if side == 'LONG' and close > poc:
                        opposite_count += 1
                    elif side == 'SHORT' and close < poc:
                        opposite_count += 1

        return opposite_count >= 2

    def _get_atr_from_ctx(self, ctx):
        """Extract ATR value from context indicators."""
        indicators = ctx.get('indicators', {})
        if isinstance(indicators, dict):
            atr = indicators.get('atr_14')
            if atr is not None:
                return float(atr)
        return None
