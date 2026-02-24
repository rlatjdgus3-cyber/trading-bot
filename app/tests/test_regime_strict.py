"""
tests/test_regime_strict.py — Unit tests for strict breakout, regime risk, and loss streak.

Covers:
  1. Legacy mode: breakout_confirmed → BREAKOUT (regression)
  2. Strict mode: triple miss + breakout_confirmed → DRIFT (demotion)
  3. Strict mode: triple pass → BREAKOUT
  4. Boundary: volume_z=0.99 (FAIL), 1.0 (PASS)
  5. Boundary: atr_ratio=1.49 (FAIL), 1.5 (PASS)
  6. Structure: N=3, M=2, only 1 outside → FAIL
  7. STATIC_RANGE SL: [0.5%, 0.9%] range
  8. BREAKOUT SL: [0.8%, 2.0%] + atr_mult=2.0
  9. BREAKOUT UP + SHORT → hard block
  10. impulse > 1.5 → block
  11. 3-loss streak → 20min cooldown
  12. flag OFF → all legacy behavior preserved
"""

import sys
import os
import unittest
from unittest.mock import patch, MagicMock

# Ensure app directory is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def _base_features(**overrides):
    """Return a base feature dict with sensible defaults."""
    f = {
        'symbol': 'BTC/USDT:USDT',
        'price': 100000.0,
        'atr_pct': 0.005,
        'bb_width': 0.01,
        'volume_z': 2.0,
        'impulse': 0.5,
        'adx': 15.0,
        'poc': 99500.0,
        'vah': 100500.0,
        'val': 99000.0,
        'range_position': 0.5,
        'drift_score': 0.0001,
        'drift_direction': 'NONE',
        'poc_slope': 0.0005,
        'vol_pct': 0.5,
        'trend_strength': 0.3,
        'range_quality': 0.8,
        'spread_ok': True,
        'liquidity_ok': True,
        'atr_ratio': 1.0,
        'structure_breakout_pass': False,
        'structure_breakout_dir': None,
    }
    f.update(overrides)
    return f


def _base_regime_ctx(**overrides):
    """Return a base regime_ctx dict."""
    r = {
        'available': True,
        'regime': 'NORMAL',
        'adx_14': 15.0,
        'breakout_confirmed': False,
        'bbw_ratio': 1.0,
        'shock_type': None,
    }
    r.update(overrides)
    return r


class TestRegimeStrictBreakout(unittest.TestCase):
    """Tests for regime_v3._classify_raw and classify with strict breakout."""

    def setUp(self):
        """Reset regime state before each test."""
        from strategy_v3.regime_v3 import reset_state
        from strategy_v3 import config_v3
        config_v3.reload()
        reset_state()

    # ── Test 1: Legacy mode — breakout_confirmed → BREAKOUT ──
    @patch('feature_flags.is_enabled', return_value=False)
    def test_legacy_breakout_confirmed(self, _mock_ff):
        from strategy_v3.regime_v3 import classify
        features = _base_features()
        regime_ctx = _base_regime_ctx(breakout_confirmed=True)
        result = classify(features, regime_ctx)
        self.assertEqual(result['regime_class'], 'BREAKOUT')
        self.assertEqual(result['entry_mode'], 'BreakoutTrend')

    # ── Test 2: Strict mode — triple miss + breakout_confirmed → DRIFT ──
    @patch('feature_flags.is_enabled', return_value=True)
    def test_strict_demote_to_drift(self, _mock_ff):
        from strategy_v3.regime_v3 import classify
        features = _base_features(
            volume_z=0.5,
            atr_ratio=1.0,
            structure_breakout_pass=False,
            drift_direction='UP',
        )
        regime_ctx = _base_regime_ctx(breakout_confirmed=True)
        result = classify(features, regime_ctx)
        self.assertIn(result['regime_class'], ('DRIFT_UP', 'DRIFT_DOWN'))
        self.assertEqual(result['entry_mode'], 'DriftFollow')
        self.assertFalse(result.get('breakout_strict', True))

    # ── Test 3: Strict mode — triple pass → BREAKOUT ──
    @patch('feature_flags.is_enabled', return_value=True)
    def test_strict_triple_pass_breakout(self, _mock_ff):
        from strategy_v3.regime_v3 import classify
        features = _base_features(
            volume_z=1.5,
            atr_ratio=2.0,
            structure_breakout_pass=True,
            structure_breakout_dir='UP',
            adx=30.0,
        )
        regime_ctx = _base_regime_ctx()
        result = classify(features, regime_ctx)
        self.assertEqual(result['regime_class'], 'BREAKOUT')
        self.assertTrue(result.get('breakout_strict', False))

    # ── Test 4: Boundary — volume_z=0.99 (FAIL), 1.0 (PASS) ──
    @patch('feature_flags.is_enabled', return_value=True)
    def test_volume_z_boundary(self, _mock_ff):
        from strategy_v3.regime_v3 import _check_strict_breakout
        from strategy_v3 import config_v3
        cfg = config_v3.get_all()

        features_fail = _base_features(
            volume_z=0.99, atr_ratio=2.0, structure_breakout_pass=True)
        passed, info = _check_strict_breakout(features_fail, cfg)
        self.assertFalse(passed)
        self.assertFalse(info['volume_pass'])

        features_pass = _base_features(
            volume_z=1.0, atr_ratio=2.0, structure_breakout_pass=True)
        passed, info = _check_strict_breakout(features_pass, cfg)
        self.assertTrue(passed)
        self.assertTrue(info['volume_pass'])

    # ── Test 5: Boundary — atr_ratio=1.49 (FAIL), 1.5 (PASS) ──
    @patch('feature_flags.is_enabled', return_value=True)
    def test_atr_ratio_boundary(self, _mock_ff):
        from strategy_v3.regime_v3 import _check_strict_breakout
        from strategy_v3 import config_v3
        cfg = config_v3.get_all()

        features_fail = _base_features(
            volume_z=2.0, atr_ratio=1.49, structure_breakout_pass=True)
        passed, info = _check_strict_breakout(features_fail, cfg)
        self.assertFalse(passed)
        self.assertFalse(info['atr_pass'])

        features_pass = _base_features(
            volume_z=2.0, atr_ratio=1.5, structure_breakout_pass=True)
        passed, info = _check_strict_breakout(features_pass, cfg)
        self.assertTrue(passed)
        self.assertTrue(info['atr_pass'])

    # ── Test 6: Structure — N=3, M=2, only 1 outside → FAIL ──
    def test_structure_breakout_insufficient_candles(self):
        from strategy.common.features import compute_structure_breakout
        # Mock cursor that returns only 1 candle above VAH
        mock_cur = MagicMock()
        # vah=100500, val=99000, atr_val=500, price=100000
        # min_dist = max(500*0.25, 100000*0.0015) = max(125, 150) = 150
        # Need candles > 100500 + 150 = 100650
        # Return 3 candles: [100700, 100400, 100300] — only 1 above
        mock_cur.fetchall.return_value = [(100700,), (100400,), (100300,)]

        passed, direction, detail = compute_structure_breakout(
            mock_cur, vah=100500, val=99000, atr_val=500, price=100000,
            n=3, m=2, k_dist=0.25, pct_dist=0.0015)
        self.assertFalse(passed)
        self.assertIsNone(direction)
        self.assertEqual(detail.get('above_count'), 1)

    # ── Test 7: STATIC_RANGE SL: [0.5%, 0.9%] ──
    @patch('feature_flags.is_enabled', return_value=True)
    def test_static_range_sl_range(self, _mock_ff):
        from strategy_v3.risk_v3 import compute_risk
        features = _base_features(atr_pct=0.005)
        v3_regime = {'regime_class': 'STATIC_RANGE'}
        result = compute_risk(features, v3_regime)
        self.assertGreaterEqual(result['sl_pct'], 0.005)
        self.assertLessEqual(result['sl_pct'], 0.009)

    # ── Test 8: BREAKOUT SL: [0.8%, 2.0%] + atr_mult=2.0 ──
    @patch('feature_flags.is_enabled', return_value=True)
    def test_breakout_sl_range(self, _mock_ff):
        from strategy_v3.risk_v3 import compute_risk
        features = _base_features(atr_pct=0.008)
        v3_regime = {'regime_class': 'BREAKOUT'}
        result = compute_risk(features, v3_regime)
        # 2.0 * 0.008 = 0.016, within [0.008, 0.02]
        self.assertGreaterEqual(result['sl_pct'], 0.008)
        self.assertLessEqual(result['sl_pct'], 0.02)
        self.assertEqual(result.get('max_stage'), 1)

    # ── Test 9: BREAKOUT UP + SHORT → hard block ──
    @patch('feature_flags.is_enabled', return_value=True)
    def test_breakout_reverse_block(self, _mock_ff):
        from strategy_v3.score_v3 import compute_modifier
        features = _base_features(
            price=101000.0, vah=100500.0, val=99000.0,
            atr_pct=0.005, impulse=0.5)
        v3_regime = {'regime_class': 'BREAKOUT'}
        # total_score negative → SHORT dominant
        result = compute_modifier(-30, features, v3_regime, 101000.0)
        self.assertTrue(result['entry_blocked'])
        self.assertIn('reverse block', result['block_reason'])

    # ── Test 10: impulse > 1.5 → block ──
    @patch('feature_flags.is_enabled', return_value=True)
    def test_impulse_chase_block(self, _mock_ff):
        from strategy_v3.score_v3 import compute_modifier
        features = _base_features(
            price=101000.0, vah=100500.0, val=99000.0,
            atr_pct=0.005, impulse=2.0)
        v3_regime = {'regime_class': 'BREAKOUT'}
        # LONG (positive total_score) + BREAKOUT UP → not reverse blocked
        # but impulse > 1.5 → blocked
        result = compute_modifier(30, features, v3_regime, 101000.0)
        self.assertTrue(result['entry_blocked'])
        self.assertIn('impulse', result['block_reason'])

    # ── Test 11: 3-loss streak → 20min cooldown ──
    def test_loss_streak_cooldown(self):
        import datetime
        from autopilot_daemon import _check_loss_streak_cooldown

        mock_cur = MagicMock()

        # Simulate ff enabled
        with patch('feature_flags.is_enabled', return_value=True):
            # First query: 3 recent losses
            # Second query: last loss timestamp
            now = datetime.datetime.now(datetime.timezone.utc)
            recent_ts = now - datetime.timedelta(seconds=60)  # 60s ago

            mock_cur.fetchall.return_value = [(-50.0,), (-30.0,), (-20.0,)]
            mock_cur.fetchone.return_value = (recent_ts,)

            ok, reason = _check_loss_streak_cooldown(mock_cur)
            self.assertFalse(ok)
            self.assertIn('LOSS_STREAK_COOLDOWN', reason)

    # ── Test 12: flag OFF → all legacy behavior preserved ──
    @patch('feature_flags.is_enabled', return_value=False)
    def test_flag_off_preserves_legacy(self, _mock_ff):
        # Regime: legacy breakout_confirmed → BREAKOUT
        from strategy_v3.regime_v3 import classify, reset_state
        reset_state()
        features = _base_features(adx=30.0, volume_z=2.0)
        regime_ctx = _base_regime_ctx(breakout_confirmed=True)
        result = classify(features, regime_ctx)
        self.assertEqual(result['regime_class'], 'BREAKOUT')

        # Risk: legacy single SL range
        from strategy_v3.risk_v3 import compute_risk
        risk = compute_risk(features, result)
        # Should NOT have max_stage key in legacy mode
        self.assertNotIn('max_stage', risk)

        # Score: no reverse block in legacy mode
        from strategy_v3.score_v3 import compute_modifier
        score_result = compute_modifier(-30, features, result, 100000.0)
        # In legacy mode, reverse block should NOT activate
        # (only chase_ban or retest_pending might block)
        if score_result['entry_blocked']:
            self.assertNotIn('reverse block', score_result.get('block_reason', ''))

    # ── Test: Structure breakout with enough candles passes ──
    def test_structure_breakout_pass(self):
        from strategy.common.features import compute_structure_breakout
        mock_cur = MagicMock()
        # 3 candles, all above VAH + min_dist
        mock_cur.fetchall.return_value = [(100700,), (100700,), (100700,)]

        passed, direction, detail = compute_structure_breakout(
            mock_cur, vah=100500, val=99000, atr_val=500, price=100000,
            n=3, m=2, k_dist=0.25, pct_dist=0.0015)
        self.assertTrue(passed)
        self.assertEqual(direction, 'UP')


if __name__ == '__main__':
    unittest.main()
