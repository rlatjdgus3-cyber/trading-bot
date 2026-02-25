"""
tests/test_drift_counter_block.py — Drift counter-direction block tests.

Covers:
  1. DRIFT_DOWN + LONG → hard block (ff ON)
  2. DRIFT_UP + SHORT → hard block (ff ON)
  3. DRIFT_DOWN + SHORT → allowed (aligned)
  4. DRIFT_UP + LONG → allowed (aligned)
  5. ff OFF → penalty only, no block (legacy)
  6. STATIC_RANGE → not affected by drift block
  7. V2 ENTER path: DRIFT_DOWN + LONG → blocked via _v3_result check
  8. V2 ENTER path: V3 dominant != V2 side → blocked
"""

import sys
import os
import unittest
from unittest.mock import patch, MagicMock
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def _base_features(**overrides):
    """Return base feature dict for drift regime."""
    f = {
        'symbol': 'BTC/USDT:USDT',
        'price': 100000.0,
        'atr_pct': 0.005,
        'bb_width': 0.01,
        'volume_z': 1.0,
        'impulse': 0.3,
        'adx': 15.0,
        'poc': 99800.0,
        'vah': 100500.0,
        'val': 99000.0,
        'range_position': 0.5,
        'drift_score': 0.001,
        'drift_direction': 'DOWN',
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


class TestDriftCounterBlock(unittest.TestCase):
    """Tests for drift counter-direction hard block in score_v3."""

    def setUp(self):
        from strategy_v3 import config_v3
        config_v3.reload()

    # ── Test 1: DRIFT_DOWN + LONG → BLOCKED (ff ON) ──
    @patch('feature_flags.is_enabled', return_value=True)
    def test_drift_down_long_blocked(self, _mock_ff):
        from strategy_v3.score_v3 import compute_modifier
        features = _base_features(drift_direction='DOWN')
        v3_regime = {'regime_class': 'DRIFT_DOWN', 'entry_mode': 'DriftFollow'}
        # total_score +30 → dominant LONG
        result = compute_modifier(30, features, v3_regime, 100000.0)
        self.assertTrue(result['entry_blocked'],
                        'DRIFT_DOWN + LONG should be blocked')
        self.assertIn('DRIFT_DOWN', result['block_reason'])
        self.assertIn('LONG', result['block_reason'])

    # ── Test 2: DRIFT_UP + SHORT → BLOCKED (ff ON) ──
    @patch('feature_flags.is_enabled', return_value=True)
    def test_drift_up_short_blocked(self, _mock_ff):
        from strategy_v3.score_v3 import compute_modifier
        features = _base_features(drift_direction='UP')
        v3_regime = {'regime_class': 'DRIFT_UP', 'entry_mode': 'DriftFollow'}
        # total_score -30 → dominant SHORT
        result = compute_modifier(-30, features, v3_regime, 100000.0)
        self.assertTrue(result['entry_blocked'],
                        'DRIFT_UP + SHORT should be blocked')
        self.assertIn('DRIFT_UP', result['block_reason'])
        self.assertIn('SHORT', result['block_reason'])

    # ── Test 3: DRIFT_DOWN + SHORT → ALLOWED (aligned) ──
    @patch('feature_flags.is_enabled', return_value=True)
    def test_drift_down_short_allowed(self, _mock_ff):
        from strategy_v3.score_v3 import compute_modifier
        features = _base_features(drift_direction='DOWN')
        v3_regime = {'regime_class': 'DRIFT_DOWN', 'entry_mode': 'DriftFollow'}
        # total_score -30 → dominant SHORT (aligned with DRIFT_DOWN)
        result = compute_modifier(-30, features, v3_regime, 100000.0)
        self.assertFalse(result['entry_blocked'],
                         'DRIFT_DOWN + SHORT should be allowed (aligned)')

    # ── Test 4: DRIFT_UP + LONG → ALLOWED (aligned) ──
    @patch('feature_flags.is_enabled', return_value=True)
    def test_drift_up_long_allowed(self, _mock_ff):
        from strategy_v3.score_v3 import compute_modifier
        features = _base_features(drift_direction='UP')
        v3_regime = {'regime_class': 'DRIFT_UP', 'entry_mode': 'DriftFollow'}
        # total_score +30 → dominant LONG (aligned with DRIFT_UP)
        result = compute_modifier(30, features, v3_regime, 100000.0)
        self.assertFalse(result['entry_blocked'],
                         'DRIFT_UP + LONG should be allowed (aligned)')

    # ── Test 5: ff OFF → penalty only, no block (legacy) ──
    def test_flag_off_penalty_only(self):
        """When ff_drift_counter_block is OFF, use penalty instead of block."""
        def _ff_side_effect(flag_name):
            if flag_name == 'ff_drift_counter_block':
                return False
            return True  # other flags stay on

        with patch('feature_flags.is_enabled', side_effect=_ff_side_effect):
            from strategy_v3.score_v3 import compute_modifier
            features = _base_features(drift_direction='DOWN')
            v3_regime = {'regime_class': 'DRIFT_DOWN', 'entry_mode': 'DriftFollow'}
            # total_score +30 → dominant LONG (counter to DRIFT_DOWN)
            result = compute_modifier(30, features, v3_regime, 100000.0)
            self.assertFalse(result['entry_blocked'],
                             'ff OFF: should NOT block, only penalty')
            # Modifier should include counter penalty
            self.assertNotEqual(result['modifier'], 0,
                                'ff OFF: should apply penalty')

    # ── Test 6: STATIC_RANGE → not affected ──
    @patch('feature_flags.is_enabled', return_value=True)
    def test_static_range_not_affected(self, _mock_ff):
        from strategy_v3.score_v3 import compute_modifier
        features = _base_features(drift_direction='NONE', drift_score=0.0001)
        v3_regime = {'regime_class': 'STATIC_RANGE', 'entry_mode': 'MeanRev'}
        # LONG in STATIC_RANGE → should not trigger drift block
        result = compute_modifier(30, features, v3_regime, 100000.0)
        if result['entry_blocked']:
            self.assertNotIn('counter-drift', result.get('block_reason', ''),
                             'STATIC_RANGE should not trigger drift block')

    # ── Test 7: High confidence LONG still blocked in DRIFT_DOWN ──
    @patch('feature_flags.is_enabled', return_value=True)
    def test_high_confidence_long_still_blocked(self, _mock_ff):
        """Even high-confidence LONG (score=+80) should be blocked in DRIFT_DOWN."""
        from strategy_v3.score_v3 import compute_modifier
        features = _base_features(drift_direction='DOWN')
        v3_regime = {'regime_class': 'DRIFT_DOWN', 'entry_mode': 'DriftFollow'}
        result = compute_modifier(80, features, v3_regime, 100000.0)
        self.assertTrue(result['entry_blocked'],
                        'Even high-conf LONG should be blocked in DRIFT_DOWN')

    # ── Test 8: DRIFT_DOWN + score=0 (neutral) → dominant LONG → blocked ──
    @patch('feature_flags.is_enabled', return_value=True)
    def test_drift_down_neutral_score_blocked(self, _mock_ff):
        """score=0 → dominant='LONG' (>=0 check) → should be blocked."""
        from strategy_v3.score_v3 import compute_modifier
        features = _base_features(drift_direction='DOWN')
        v3_regime = {'regime_class': 'DRIFT_DOWN', 'entry_mode': 'DriftFollow'}
        result = compute_modifier(0, features, v3_regime, 100000.0)
        # score=0 → dominant='LONG' in _compute_drift_modifier
        self.assertTrue(result['entry_blocked'],
                        'Neutral score (0) → LONG → blocked in DRIFT_DOWN')


if __name__ == '__main__':
    unittest.main()
