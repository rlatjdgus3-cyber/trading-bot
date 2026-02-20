"""
strategy.modes.base — Abstract base class for mode strategies.
"""


class ModeStrategy:
    """Base class for all mode strategies (A/B/C).

    Subclasses must implement decide().

    ctx dict contains:
        features     — dict from build_feature_snapshot()
        position     — dict or None ({side, total_qty, avg_entry_price, stage})
        regime_ctx   — dict from regime_reader.get_current_regime()
        config       — dict from YAML for this mode
        price        — float current price
        indicators   — dict latest indicator row
        vol_profile  — dict {poc, vah, val}
        candles      — list of recent candle dicts
    """

    MODE = None  # Override in subclass: 'A', 'B', 'C'

    def decide(self, ctx):
        """Decide action for current market state.

        Args:
            ctx: dict with all context needed for decision

        Returns:
            dict with:
                action:     'ENTER' | 'ADD' | 'EXIT' | 'HOLD'
                side:       'LONG' | 'SHORT' | None
                qty:        float or None
                tp:         float or None
                sl:         float or None
                reason:     str
                signal_key: str or None
                chase_entry: bool
                order_type: 'market' | 'limit'
                meta:       dict (extra info)
        """
        raise NotImplementedError

    def _hold(self, reason, meta=None):
        """Convenience: return HOLD decision."""
        return {
            'action': 'HOLD',
            'side': None,
            'qty': None,
            'tp': None,
            'sl': None,
            'reason': reason,
            'signal_key': None,
            'chase_entry': False,
            'order_type': 'market',
            'meta': meta or {},
        }
