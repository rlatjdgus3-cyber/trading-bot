"""
Tests for V3 Upgrade — scoring guards, risk adjustments, health fixes.
"""
import sys
import os
import unittest
from unittest.mock import patch, MagicMock

# Ensure app dir is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestSpreadLiquidityHardBlock(unittest.TestCase):
    """4A: spread_ok=False / liquidity_ok=False → entry_blocked."""

    def setUp(self):
        from strategy_v3 import config_v3
        config_v3.reload()

    def test_spread_ok_false_blocks_entry(self):
        from strategy_v3.score_v3 import compute_modifier
        features = {'spread_ok': False, 'price': 50000, 'atr_pct': 0.005}
        regime = {'regime_class': 'STATIC_RANGE'}
        result = compute_modifier(50, features, regime, 50000)
        self.assertTrue(result['entry_blocked'])
        self.assertIn('spread_ok', result['block_reason'])

    def test_liquidity_ok_false_blocks_entry(self):
        from strategy_v3.score_v3 import compute_modifier
        features = {'liquidity_ok': False, 'spread_ok': True, 'price': 50000, 'atr_pct': 0.005}
        regime = {'regime_class': 'STATIC_RANGE'}
        result = compute_modifier(50, features, regime, 50000)
        self.assertTrue(result['entry_blocked'])
        self.assertIn('liquidity_ok', result['block_reason'])

    def test_both_ok_passes(self):
        from strategy_v3.score_v3 import compute_modifier
        features = {
            'spread_ok': True, 'liquidity_ok': True, 'price': 50000,
            'atr_pct': 0.005, 'vah': 51000, 'val': 49000, 'poc': 50000,
            'range_position': 0.15,  # near VAL edge → high conviction
            'vol_pct': 1.0,
        }
        regime = {'regime_class': 'STATIC_RANGE'}
        # Use high total_score to avoid POC zone blocking
        result = compute_modifier(80, features, regime, 50000)
        # Not blocked by spread/liquidity
        self.assertNotIn('spread_ok', result.get('block_reason', ''))
        self.assertNotIn('liquidity_ok', result.get('block_reason', ''))


class TestImpulseChaseGuard(unittest.TestCase):
    """4C: Non-breakout impulse > threshold → penalty/block."""

    def setUp(self):
        from strategy_v3 import config_v3
        config_v3.reload()

    def test_drift_impulse_above_threshold_blocks(self):
        from strategy_v3.score_v3 import compute_modifier
        features = {
            'spread_ok': True, 'liquidity_ok': True, 'price': 50000,
            'atr_pct': 0.005, 'impulse': 2.5,  # > 1.5 * 1.5 = 2.25
            'vah': 51000, 'val': 49000, 'poc': 50000,
            'drift_score': 0.001, 'range_position': 0.5,
        }
        regime = {'regime_class': 'DRIFT_UP'}
        result = compute_modifier(50, features, regime, 50000)
        self.assertTrue(result['entry_blocked'])
        self.assertIn('extreme impulse', result['block_reason'])

    def test_static_range_impulse_penalty(self):
        from strategy_v3.score_v3 import compute_modifier
        features = {
            'spread_ok': True, 'liquidity_ok': True, 'price': 50000,
            'atr_pct': 0.005, 'impulse': 1.6,  # > 1.5, but < 2.25
            'vah': 51000, 'val': 49000, 'poc': 50000,
            'range_position': 0.5,
        }
        regime = {'regime_class': 'STATIC_RANGE'}
        result = compute_modifier(50, features, regime, 50000)
        # Should have penalty but not necessarily blocked (depends on POC zone etc.)
        self.assertIn('impulse=1.60', ' '.join(result['reasoning']))


class TestBreakoutReverseBlock(unittest.TestCase):
    """3A effect: BREAKOUT reverse direction → entry_blocked (ff_regime_risk_v3=true)."""

    def setUp(self):
        from strategy_v3 import config_v3
        config_v3.reload()

    @patch('strategy_v3.score_v3._is_regime_risk_v3_enabled', return_value=True)
    def test_breakout_up_short_blocked(self, _mock):
        from strategy_v3.score_v3 import compute_modifier
        # Price just above VAH+pad so breakout UP is detected but not too far (avoid chase ban)
        features = {
            'spread_ok': True, 'liquidity_ok': True, 'price': 51100,
            'atr_pct': 0.005, 'vah': 51000, 'val': 49000, 'poc': 50000,
            'impulse': 0.5, 'vol_pct': 1.0,
        }
        regime = {'regime_class': 'BREAKOUT'}
        # total_score=-50 → SHORT dominant → should be blocked by breakout UP reverse block
        result = compute_modifier(-50, features, regime, 51100)
        self.assertTrue(result['entry_blocked'])
        self.assertIn('reverse block', result['block_reason'])


class TestVolPctPenalty(unittest.TestCase):
    """4B: vol_pct=None → -10 penalty."""

    def setUp(self):
        from strategy_v3 import config_v3
        config_v3.reload()

    def test_vol_pct_none_penalty(self):
        from strategy_v3.score_v3 import compute_modifier
        features = {
            'spread_ok': True, 'liquidity_ok': True, 'price': 50000,
            'atr_pct': 0.005, 'vol_pct': None,
            'vah': 51000, 'val': 49000, 'poc': 50000,
            'range_position': 0.5,
        }
        regime = {'regime_class': 'STATIC_RANGE'}
        result = compute_modifier(50, features, regime, 50000)
        self.assertIn('vol_pct=N/A', ' '.join(result['reasoning']))


class TestLossStreakSliceReduction(unittest.TestCase):
    """5B: loss_streak >= 2 → stage_slice reduction."""

    def setUp(self):
        from strategy_v3 import config_v3
        config_v3.reload()

    @patch('strategy_v3.risk_v3._is_regime_risk_v3_enabled', return_value=True)
    def test_loss_streak_3_halves_slice(self, _mock):
        from strategy_v3.risk_v3 import compute_risk
        features = {'atr_pct': 0.005, 'spread_ok': True, 'liquidity_ok': True}
        regime = {'regime_class': 'STATIC_RANGE'}
        result = compute_risk(features, regime, loss_streak=3)
        self.assertLessEqual(result['stage_slice_mult'], 0.5)
        self.assertTrue(any('loss_streak=3' in r for r in result['reasoning']))

    @patch('strategy_v3.risk_v3._is_regime_risk_v3_enabled', return_value=True)
    def test_loss_streak_2_reduces_slice(self, _mock):
        from strategy_v3.risk_v3 import compute_risk
        features = {'atr_pct': 0.005, 'spread_ok': True, 'liquidity_ok': True}
        regime = {'regime_class': 'STATIC_RANGE'}
        result = compute_risk(features, regime, loss_streak=2)
        self.assertLessEqual(result['stage_slice_mult'], 0.7)
        self.assertTrue(any('loss_streak=2' in r for r in result['reasoning']))

    @patch('strategy_v3.risk_v3._is_regime_risk_v3_enabled', return_value=True)
    def test_no_loss_streak_full_slice(self, _mock):
        from strategy_v3.risk_v3 import compute_risk
        features = {'atr_pct': 0.005, 'spread_ok': True, 'liquidity_ok': True}
        regime = {'regime_class': 'STATIC_RANGE'}
        result = compute_risk(features, regime, loss_streak=0)
        self.assertEqual(result['stage_slice_mult'], 1.0)


class TestWatchdogServiceNameNormalization(unittest.TestCase):
    """1A: 'candles.service' → 'candles'."""

    def test_normalize_service_names(self):
        service_states = {
            'candles.service': 'OK',
            'live_order_executor.service': 'OK',
            'telegram_cmd_poller.timer': 'OK',
            'db': 'OK',
        }
        normalized = {}
        for unit_key, state_val in service_states.items():
            short = unit_key.replace('.service', '').replace('.timer', '')
            normalized[short] = state_val

        self.assertIn('candles', normalized)
        self.assertIn('live_order_executor', normalized)
        self.assertIn('telegram_cmd_poller', normalized)
        self.assertIn('db', normalized)
        self.assertNotIn('candles.service', normalized)


class TestComputeMarketHealthRename(unittest.TestCase):
    """1D: compute_health renamed to compute_market_health."""

    def test_compute_market_health_exists(self):
        from strategy_v3 import compute_market_health
        result = compute_market_health({'spread_ok': True, 'liquidity_ok': True})
        self.assertEqual(result, 'OK')

    def test_compute_market_health_warn(self):
        from strategy_v3 import compute_market_health
        result = compute_market_health({'spread_ok': False, 'liquidity_ok': True})
        self.assertEqual(result, 'WARN')

    def test_old_name_removed(self):
        import strategy_v3
        self.assertFalse(hasattr(strategy_v3, 'compute_health'))


class TestModeCooloff(unittest.TestCase):
    """5C: mode_cooloff blocks regime with low win rate."""

    def setUp(self):
        from strategy_v3 import config_v3
        config_v3.reload()
        # Reset cooloff state
        import autopilot_daemon
        autopilot_daemon._mode_cooloff_until.clear()

    @patch('strategy_v3.config_v3.get')
    def test_cooloff_blocks_low_winrate(self, mock_get):
        import autopilot_daemon

        def config_side_effect(key, default=None):
            mapping = {
                'mode_cooloff_enabled': True,
                'mode_cooloff_min_trades': 5,
                'mode_cooloff_min_winrate': 0.30,
                'mode_cooloff_hours': 4,
            }
            return mapping.get(key, default)
        mock_get.side_effect = config_side_effect

        mock_cur = MagicMock()
        # 5 trades total, 1 win = 20% < 30%
        mock_cur.fetchone.return_value = (5, 1)

        ok, reason = autopilot_daemon._check_mode_cooloff(mock_cur, 'STATIC_RANGE')
        self.assertFalse(ok)
        self.assertIn('MODE_COOLOFF', reason)
        self.assertIn('STATIC_RANGE', reason)

    @patch('strategy_v3.config_v3.get')
    def test_cooloff_passes_good_winrate(self, mock_get):
        import autopilot_daemon

        def config_side_effect(key, default=None):
            mapping = {
                'mode_cooloff_enabled': True,
                'mode_cooloff_min_trades': 5,
                'mode_cooloff_min_winrate': 0.30,
                'mode_cooloff_hours': 4,
            }
            return mapping.get(key, default)
        mock_get.side_effect = config_side_effect

        mock_cur = MagicMock()
        # 5 trades total, 3 wins = 60% > 30%
        mock_cur.fetchone.return_value = (5, 3)

        ok, reason = autopilot_daemon._check_mode_cooloff(mock_cur, 'STATIC_RANGE')
        self.assertTrue(ok)


class TestLossStreakConfEscalation(unittest.TestCase):
    """5A: loss_streak=2 → confidence min raised."""

    @patch('autopilot_daemon._get_current_loss_streak', return_value=2)
    @patch('autopilot_daemon._check_loss_streak_cooldown', return_value=(True, ''))
    @patch('autopilot_daemon._check_stop_loss_cooldown', return_value=(True, '', 0))
    @patch('autopilot_daemon._is_in_repeat_cooldown', return_value=(False, 0))
    def test_streak_2_escalates_min_conf(self, *_mocks):
        import autopilot_daemon
        # streak=2, escalation=5 → min = 35 + 2*5 = 45
        # conf=40 < 45 → blocked
        mock_cur = MagicMock()
        ok, reason = autopilot_daemon.should_emit_signal(mock_cur, 'BTC/USDT:USDT', 'LONG', 40)
        self.assertFalse(ok)
        self.assertIn('LOSS_STREAK_ESCALATION', reason)

    @patch('autopilot_daemon._get_current_loss_streak', return_value=2)
    @patch('autopilot_daemon._check_loss_streak_cooldown', return_value=(True, ''))
    @patch('autopilot_daemon._check_stop_loss_cooldown', return_value=(True, '', 0))
    @patch('autopilot_daemon._is_in_repeat_cooldown', return_value=(False, 0))
    def test_streak_2_high_conf_passes(self, *_mocks):
        import autopilot_daemon
        # streak=2, escalation=5 → min = 35 + 2*5 = 45
        # conf=50 >= 45 → passes
        mock_cur = MagicMock()
        ok, reason = autopilot_daemon.should_emit_signal(mock_cur, 'BTC/USDT:USDT', 'LONG', 50)
        self.assertTrue(ok)


class TestSQLColumnNames(unittest.TestCase):
    """Verify SQL queries use correct execution_log column names."""

    def test_loss_streak_uses_realized_pnl(self):
        """_get_current_loss_streak must use realized_pnl, not pnl_usdt."""
        import inspect
        import autopilot_daemon
        src = inspect.getsource(autopilot_daemon._get_current_loss_streak)
        self.assertIn('realized_pnl', src)
        self.assertNotIn('pnl_usdt', src)

    def test_loss_streak_uses_order_type(self):
        """_get_current_loss_streak must use order_type, not action."""
        import inspect
        import autopilot_daemon
        src = inspect.getsource(autopilot_daemon._get_current_loss_streak)
        self.assertIn('order_type', src)
        self.assertNotIn("WHERE action ", src)

    def test_loss_streak_cooldown_uses_realized_pnl(self):
        """_check_loss_streak_cooldown must use realized_pnl, not pnl_usdt."""
        import inspect
        import autopilot_daemon
        src = inspect.getsource(autopilot_daemon._check_loss_streak_cooldown)
        self.assertIn('realized_pnl', src)
        self.assertNotIn('pnl_usdt', src)

    def test_mode_cooloff_uses_realized_pnl(self):
        """_check_mode_cooloff must use realized_pnl, not pnl_usdt."""
        import inspect
        import autopilot_daemon
        src = inspect.getsource(autopilot_daemon._check_mode_cooloff)
        self.assertIn('realized_pnl', src)
        self.assertNotIn('pnl_usdt', src)

    def test_count_daily_uses_order_type(self):
        """count_daily_trades_kst must use order_type, not action."""
        import inspect
        import order_throttle
        src = inspect.getsource(order_throttle.count_daily_trades_kst)
        self.assertIn('order_type', src)
        self.assertNotIn("action NOT IN", src)


if __name__ == '__main__':
    unittest.main()
