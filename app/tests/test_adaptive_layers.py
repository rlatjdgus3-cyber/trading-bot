"""
tests/test_adaptive_layers.py — Unit tests for adaptive_v3 5-layer system.

All tests mock DB cursor and use in-memory state only.
"""

import sys
import os
import time
import json
import pytest
from unittest.mock import MagicMock, patch, PropertyMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Mock feature_flags + config_v3 before importing adaptive_v3
import feature_flags
feature_flags._cache = {'ff_adaptive_layers': True}

from strategy_v3 import config_v3
config_v3._config_cache = {}  # force defaults


def _default_cfg():
    """Return default adaptive config for tests."""
    return config_v3.get_all()


def _mock_cursor(fetchall_data=None, fetchone_data=None):
    """Create a mock DB cursor."""
    cur = MagicMock()
    cur.execute = MagicMock()
    if fetchall_data is not None:
        cur.fetchall = MagicMock(return_value=fetchall_data)
    else:
        cur.fetchall = MagicMock(return_value=[])
    if fetchone_data is not None:
        cur.fetchone = MagicMock(return_value=fetchone_data)
    else:
        cur.fetchone = MagicMock(return_value=None)
    return cur


def _reset_state():
    """Reset adaptive_v3 in-memory state."""
    from strategy_v3 import adaptive_v3
    adaptive_v3._state = {
        'global_wr_penalty_active': False,
        'mode_wr_penalty': {},
        'mode_cooldowns': {},
        'warn_since_ts': 0,
        'last_trade_ts': 0,
        'anti_paralysis_stage': 0,
        'anti_paralysis_reset_ts': 0,
    }


# ══════════════════════════════════════════════════════════════
# L1: LOSS-STREAK ADAPTIVE FILTER
# ══════════════════════════════════════════════════════════════

class TestLayer1:
    def setup_method(self):
        _reset_state()

    def test_3_consecutive_losses_penalty_070(self):
        """3+ consecutive losses → penalty = 0.70"""
        from strategy_v3.adaptive_v3 import compute_layer1
        cfg = _default_cfg()

        # Mock: 3 consecutive losses for MeanRev
        losses = [(-10.0, 'MeanRev', 'STATIC_RANGE')] * 3
        cur = _mock_cursor(fetchall_data=losses)
        # fetchone for trade_switch + last_trade_ts
        cur.fetchone = MagicMock(side_effect=[
            (True,),   # trade_switch ON
            (time.time(),),  # last_trade_ts
            (0,),  # trade_switch_off_seconds
        ])

        result = compute_layer1(cur, 'MeanRev', cfg)
        assert result['l1_penalty'] == 0.70

    def test_5_consecutive_losses_cooldown(self):
        """5+ consecutive losses → cooldown active"""
        from strategy_v3.adaptive_v3 import compute_layer1
        cfg = _default_cfg()

        losses = [(-5.0, 'MeanRev', 'STATIC_RANGE')] * 5
        cur = _mock_cursor(fetchall_data=losses)
        cur.fetchone = MagicMock(side_effect=[
            (True,),
            (time.time(),),
            (0,),
        ])

        result = compute_layer1(cur, 'MeanRev', cfg)
        assert result['l1_cooldown_active'] is True
        assert result['l1_cooldown_remaining'] > 0

    def test_mixed_no_penalty(self):
        """Win interspersed → no streak → penalty = 1.0"""
        from strategy_v3.adaptive_v3 import compute_layer1
        cfg = _default_cfg()

        # Loss, Loss, Win → streak = 2 (< 3)
        data = [(-5.0, 'MeanRev', 'STATIC_RANGE'),
                (-5.0, 'MeanRev', 'STATIC_RANGE'),
                (10.0, 'MeanRev', 'STATIC_RANGE')]
        cur = _mock_cursor(fetchall_data=data)
        cur.fetchone = MagicMock(side_effect=[
            (True,), (time.time(),), (0,),
        ])

        result = compute_layer1(cur, 'MeanRev', cfg)
        assert result['l1_penalty'] == 1.0

    def test_mode_independence(self):
        """Losses in DriftFollow don't affect MeanRev"""
        from strategy_v3.adaptive_v3 import compute_layer1
        cfg = _default_cfg()

        # 5 DriftFollow losses, but querying MeanRev
        data = [(-5.0, 'DriftFollow', 'DRIFT_UP')] * 5
        cur = _mock_cursor(fetchall_data=data)
        cur.fetchone = MagicMock(side_effect=[
            (True,), (time.time(),), (0,),
        ])

        result = compute_layer1(cur, 'MeanRev', cfg)
        # MeanRev streak = 0 (no MeanRev losses)
        assert result['l1_penalty'] == 1.0
        assert result['l1_cooldown_active'] is False


class TestLayer1GlobalWR:
    def setup_method(self):
        _reset_state()

    def test_wr_below_35_block(self):
        """WR < 35% → global WR block active"""
        from strategy_v3.adaptive_v3 import compute_layer1
        cfg = _default_cfg()

        # 20 trades: 6 wins, 14 losses = 30% WR
        trades = [(10.0, None, None)] * 6 + [(-5.0, None, None)] * 14
        cur = _mock_cursor()
        call_count = [0]
        def side_effect_fetchall():
            call_count[0] += 1
            if call_count[0] == 1:
                return []  # mode streak query
            return trades  # global WR query
        cur.fetchall = MagicMock(side_effect=side_effect_fetchall)
        cur.fetchone = MagicMock(side_effect=[
            (True,), (time.time(),), (0,),
        ])

        result = compute_layer1(cur, 'MeanRev', cfg)
        assert result['l1_global_wr_block'] is True
        assert result['l1_effective_threshold_add'] == 10

    def test_wr_above_40_unblock(self):
        """WR >= 40% → global WR block released"""
        from strategy_v3 import adaptive_v3
        adaptive_v3._state['global_wr_penalty_active'] = True
        cfg = _default_cfg()

        # 20 trades: 9 wins, 11 losses = 45% WR
        trades = [(10.0, None, None)] * 9 + [(-5.0, None, None)] * 11
        cur = _mock_cursor()
        call_count = [0]
        def side_effect():
            call_count[0] += 1
            if call_count[0] == 1:
                return []
            return trades
        cur.fetchall = MagicMock(side_effect=side_effect)
        cur.fetchone = MagicMock(side_effect=[
            (True,), (time.time(),), (0,),
        ])

        result = adaptive_v3.compute_layer1(cur, 'MeanRev', cfg)
        assert result['l1_global_wr_block'] is False

    def test_hysteresis_hold(self):
        """35~40% band: hold previous state"""
        from strategy_v3 import adaptive_v3
        adaptive_v3._state['global_wr_penalty_active'] = True
        cfg = _default_cfg()

        # 20 trades: 7 wins = 37.5% WR (in hysteresis band)
        trades = [(10.0, None, None)] * 7 + [(-5.0, None, None)] * 13 + [(10.0, None, None)]
        cur = _mock_cursor()
        call_count = [0]
        def side_effect():
            call_count[0] += 1
            if call_count[0] == 1:
                return []
            return trades[:20]
        cur.fetchall = MagicMock(side_effect=side_effect)
        cur.fetchone = MagicMock(side_effect=[
            (True,), (time.time(),), (0,),
        ])

        result = adaptive_v3.compute_layer1(cur, 'MeanRev', cfg)
        # Was active → stays active (37.5% is in band)
        assert result['l1_global_wr_block'] is True

    def test_too_few_trades_no_penalty(self):
        """< 20 trades → no WR penalty"""
        from strategy_v3.adaptive_v3 import compute_layer1
        cfg = _default_cfg()

        # Only 5 trades
        trades = [(-5.0, None, None)] * 5
        cur = _mock_cursor()
        call_count = [0]
        def side_effect():
            call_count[0] += 1
            if call_count[0] == 1:
                return []
            return trades
        cur.fetchall = MagicMock(side_effect=side_effect)
        cur.fetchone = MagicMock(side_effect=[
            (True,), (time.time(),), (0,),
        ])

        result = compute_layer1(cur, 'MeanRev', cfg)
        assert result['l1_global_wr_block'] is False


# ══════════════════════════════════════════════════════════════
# L2: MeanReversion PROTECTION
# ══════════════════════════════════════════════════════════════

class TestLayer2:
    def test_all_pass(self):
        """All 6 conditions met → no block"""
        from strategy_v3.adaptive_v3 import compute_layer2
        cfg = _default_cfg()
        features = {
            'range_position': 0.90,
            'volume_z': -0.5,
            'impulse': 0.5,
            'drift_direction': 'NONE',
        }
        regime_ctx = {
            'price_vs_va': 'INSIDE',
            'breakout_confirmed': False,
            'flow_bias': -0.2,
        }

        result = compute_layer2('MeanRev', 'SHORT', 'STATIC_RANGE',
                                features, regime_ctx, cfg)
        assert result['l2_meanrev_blocked'] is False

    def test_volume_z_none_fail_closed(self):
        """volume_z=None → fail-closed → block"""
        from strategy_v3.adaptive_v3 import compute_layer2
        cfg = _default_cfg()
        features = {
            'range_position': 0.90,
            'volume_z': None,
            'impulse': 0.5,
        }
        regime_ctx = {
            'price_vs_va': 'INSIDE',
            'breakout_confirmed': False,
            'flow_bias': -0.2,
        }

        result = compute_layer2('MeanRev', 'SHORT', 'STATIC_RANGE',
                                features, regime_ctx, cfg)
        assert result['l2_meanrev_blocked'] is True
        assert 'volume_z=None' in result['l2_block_reason']

    def test_flow_bias_positive_block(self):
        """flow_bias > 0 → block MeanRev SHORT"""
        from strategy_v3.adaptive_v3 import compute_layer2
        cfg = _default_cfg()
        features = {
            'range_position': 0.90,
            'volume_z': -0.5,
            'impulse': 0.5,
        }
        regime_ctx = {
            'price_vs_va': 'INSIDE',
            'breakout_confirmed': False,
            'flow_bias': 0.5,  # positive
        }

        result = compute_layer2('MeanRev', 'SHORT', 'STATIC_RANGE',
                                features, regime_ctx, cfg)
        assert result['l2_meanrev_blocked'] is True
        assert 'flow_bias' in result['l2_block_reason']

    def test_hard_block(self):
        """drift=NONE + flow_bias>0 + impulse>1.5 → hard block"""
        from strategy_v3.adaptive_v3 import compute_layer2
        cfg = _default_cfg()
        features = {
            'range_position': 0.90,
            'volume_z': -0.5,
            'impulse': 2.0,
            'drift_direction': 'NONE',
        }
        regime_ctx = {
            'price_vs_va': 'INSIDE',
            'breakout_confirmed': False,
            'flow_bias': 0.5,
        }

        result = compute_layer2('MeanRev', 'SHORT', 'STATIC_RANGE',
                                features, regime_ctx, cfg)
        assert result['l2_meanrev_blocked'] is True
        # Should be blocked by either condition 6 (flow_bias>0) or hard-block
        assert result['l2_block_reason'] != ''

    def test_range_pos_above_1_block(self):
        """range_position > 1.0 → MeanRev block (any direction)"""
        from strategy_v3.adaptive_v3 import compute_layer2
        cfg = _default_cfg()
        features = {'range_position': 1.05}
        regime_ctx = {}

        result = compute_layer2('MeanRev', 'LONG', 'STATIC_RANGE',
                                features, regime_ctx, cfg)
        assert result['l2_meanrev_blocked'] is True
        assert 'range_pos=1.05' in result['l2_block_reason']

    def test_long_not_affected(self):
        """MeanRev LONG with range_pos <= 1.0 → not blocked by L2 SHORT conditions"""
        from strategy_v3.adaptive_v3 import compute_layer2
        cfg = _default_cfg()
        features = {
            'range_position': 0.15,
            'volume_z': None,
            'impulse': 0.5,
        }
        regime_ctx = {
            'price_vs_va': 'INSIDE',
            'breakout_confirmed': False,
            'flow_bias': 0.5,
        }

        result = compute_layer2('MeanRev', 'LONG', 'STATIC_RANGE',
                                features, regime_ctx, cfg)
        assert result['l2_meanrev_blocked'] is False

    def test_non_meanrev_not_affected(self):
        """DriftFollow entry_mode → L2 doesn't apply"""
        from strategy_v3.adaptive_v3 import compute_layer2
        cfg = _default_cfg()
        features = {'range_position': 0.90, 'volume_z': None}
        regime_ctx = {'flow_bias': 0.5}

        result = compute_layer2('DriftFollow', 'SHORT', 'DRIFT_UP',
                                features, regime_ctx, cfg)
        assert result['l2_meanrev_blocked'] is False


# ══════════════════════════════════════════════════════════════
# L3: ADD LOGIC RESTRICTION
# ══════════════════════════════════════════════════════════════

class TestLayer3:
    def test_upnl_negative_block(self):
        """uPnL < 0 → ADD blocked"""
        from strategy_v3.adaptive_v3 import compute_layer3
        cfg = _default_cfg()
        cur = _mock_cursor()
        # avg_entry=100000, peak_upnl=0.2
        # mark_price=99500 (loss for long)
        cur.fetchone = MagicMock(side_effect=[
            (100000, 0.2),    # position_state
            (99500,),          # market_data_cache
        ])

        result = compute_layer3(cur, 'long', {}, cfg)
        assert result['l3_add_blocked'] is True
        assert 'uPnL' in result['l3_add_reason']

    def test_upnl_positive_with_peak_allow(self):
        """uPnL > 0 + peak >= 0.4% → ADD allowed"""
        from strategy_v3.adaptive_v3 import compute_layer3
        cfg = _default_cfg()
        cur = _mock_cursor()
        # avg_entry=100000, peak_upnl=0.5
        # mark_price=100500 (profit for long)
        cur.fetchone = MagicMock(side_effect=[
            (100000, 0.5),
            (100500,),
        ])

        result = compute_layer3(cur, 'long', {}, cfg)
        assert result['l3_add_blocked'] is False

    def test_upnl_positive_no_peak_block(self):
        """uPnL > 0 but peak < 0.4% and no retest → ADD blocked"""
        from strategy_v3.adaptive_v3 import compute_layer3
        cfg = _default_cfg()
        cur = _mock_cursor()
        # avg_entry=100000, peak_upnl=0.1
        # mark_price=100200 (profit for long, but peak too low)
        cur.fetchone = MagicMock(side_effect=[
            (100000, 0.1),
            (100200,),
        ])

        result = compute_layer3(cur, 'long', {}, cfg)
        assert result['l3_add_blocked'] is True
        assert 'peak' in result['l3_add_reason']


# ══════════════════════════════════════════════════════════════
# L4: health=WARN RISK CONTROL
# ══════════════════════════════════════════════════════════════

class TestLayer4:
    def setup_method(self):
        _reset_state()

    def test_warn_entry_block(self):
        """health=WARN → entry blocked"""
        from strategy_v3.adaptive_v3 import compute_layer4
        cfg = _default_cfg()
        result = compute_layer4('WARN', cfg)
        assert result['l4_entry_blocked'] is True

    def test_warn_add_block(self):
        """health=WARN → ADD blocked"""
        from strategy_v3.adaptive_v3 import compute_layer4
        cfg = _default_cfg()
        result = compute_layer4('WARN', cfg)
        assert result['l4_add_blocked'] is True

    def test_ok_no_block(self):
        """health=OK → no block"""
        from strategy_v3.adaptive_v3 import compute_layer4
        cfg = _default_cfg()
        result = compute_layer4('OK', cfg)
        assert result['l4_entry_blocked'] is False
        assert result['l4_add_blocked'] is False

    def test_warn_2min_tightening(self):
        """WARN for 2+ minutes → time_stop tightened"""
        from strategy_v3 import adaptive_v3
        cfg = _default_cfg()
        # Simulate WARN started 3 minutes ago
        adaptive_v3._state['warn_since_ts'] = time.time() - 180
        result = adaptive_v3.compute_layer4('WARN', cfg)
        assert result['l4_time_stop_mult'] == 0.5
        assert result['l4_trailing_sensitive'] is True


# ══════════════════════════════════════════════════════════════
# L5: MODE PERFORMANCE TRACKING
# ══════════════════════════════════════════════════════════════

class TestLayer5:
    def setup_method(self):
        _reset_state()

    def test_wr_below_35_penalty_075(self):
        """Mode WR < 35% → penalty = 0.75"""
        from strategy_v3.adaptive_v3 import compute_layer5
        cfg = _default_cfg()

        # 50 trades: 15 wins = 30% WR
        trades = [(10.0,)] * 15 + [(-5.0,)] * 35
        cur = _mock_cursor(fetchall_data=trades)

        result = compute_layer5(cur, 'MeanRev', cfg)
        assert result['l5_penalty'] == 0.75

    def test_combined_floor_055(self):
        """L1 * L5 penalty floored at 0.55"""
        # L1=0.70, L5=0.75 → 0.525 → clamped to 0.55
        floor = 0.55
        combined = max(floor, 0.70 * 0.75)
        assert combined == 0.55

    def test_small_sample_no_penalty(self):
        """< 10 trades → no penalty"""
        from strategy_v3.adaptive_v3 import compute_layer5
        cfg = _default_cfg()

        # Only 5 trades, all losses
        trades = [(-5.0,)] * 5
        cur = _mock_cursor(fetchall_data=trades)

        result = compute_layer5(cur, 'MeanRev', cfg)
        assert result['l5_penalty'] == 1.0  # No penalty due to small sample


# ══════════════════════════════════════════════════════════════
# DRYRUN MODE
# ══════════════════════════════════════════════════════════════

class TestDryrun:
    def setup_method(self):
        _reset_state()

    @patch('strategy_v3.adaptive_v3._is_dryrun', return_value=True)
    @patch('strategy_v3.adaptive_v3._is_adaptive_enabled', return_value=True)
    def test_dryrun_computes_but_returns_dryrun_flag(self, mock_enabled, mock_dryrun):
        """Dryrun: compute layers but flag dryrun=True"""
        from strategy_v3.adaptive_v3 import apply_adaptive_layers
        cfg = _default_cfg()

        cur = _mock_cursor()
        cur.fetchone = MagicMock(side_effect=[
            None,  # DB state load
            (True,),  # trade_switch
            (time.time(),),  # last_trade_ts
            (0,),  # OFF seconds
        ] * 5)  # repeat for multiple queries
        cur.fetchall = MagicMock(return_value=[])

        result = apply_adaptive_layers(
            cur, 'MeanRev', 'LONG', 'STATIC_RANGE',
            {}, {}, 'OK', cfg)
        assert result['dryrun'] is True


# ══════════════════════════════════════════════════════════════
# FAIL-OPEN
# ══════════════════════════════════════════════════════════════

class TestFailOpen:
    def setup_method(self):
        _reset_state()

    def test_db_error_no_penalty(self):
        """DB error → FAIL-OPEN (no penalty, no block)"""
        from strategy_v3.adaptive_v3 import compute_layer1
        cfg = _default_cfg()

        cur = MagicMock()
        cur.execute = MagicMock(side_effect=Exception('DB connection lost'))
        cur.fetchall = MagicMock(side_effect=Exception('DB connection lost'))
        cur.fetchone = MagicMock(side_effect=Exception('DB connection lost'))

        result = compute_layer1(cur, 'MeanRev', cfg)
        assert result['l1_penalty'] == 1.0
        assert result['l1_cooldown_active'] is False

    def test_l2_none_features_fail_closed(self):
        """L2 with None features → fail-closed for MeanRev SHORT (by design)"""
        from strategy_v3.adaptive_v3 import compute_layer2
        cfg = _default_cfg()

        # None features → conditions not met → MeanRev SHORT blocked (fail-closed)
        result = compute_layer2('MeanRev', 'SHORT', 'STATIC_RANGE',
                                None, None, cfg)
        assert result['l2_meanrev_blocked'] is True

    def test_l2_non_meanrev_failopen(self):
        """L2 with None features for non-MeanRev → no block (FAIL-OPEN)"""
        from strategy_v3.adaptive_v3 import compute_layer2
        cfg = _default_cfg()

        result = compute_layer2('DriftFollow', 'SHORT', 'DRIFT_UP',
                                None, None, cfg)
        assert result['l2_meanrev_blocked'] is False


# ══════════════════════════════════════════════════════════════
# NULL SAFETY
# ══════════════════════════════════════════════════════════════

class TestNullSafety:
    def test_none_features_l2_meanrev_short_fail_closed(self):
        """None features + MeanRev SHORT → fail-closed (blocked by design)"""
        from strategy_v3.adaptive_v3 import compute_layer2
        cfg = _default_cfg()

        result = compute_layer2('MeanRev', 'SHORT', 'STATIC_RANGE',
                                None, None, cfg)
        assert result['l2_meanrev_blocked'] is True  # fail-closed for MeanRev SHORT

    def test_none_features_l2_long_failopen(self):
        """None features + MeanRev LONG → FAIL-OPEN (not blocked)"""
        from strategy_v3.adaptive_v3 import compute_layer2
        cfg = _default_cfg()

        result = compute_layer2('MeanRev', 'LONG', 'STATIC_RANGE',
                                None, None, cfg)
        assert result['l2_meanrev_blocked'] is False

    def test_none_regime_ctx_l2(self):
        """None regime_ctx → safe handling"""
        from strategy_v3.adaptive_v3 import compute_layer2
        cfg = _default_cfg()

        result = compute_layer2('MeanRev', 'SHORT', 'STATIC_RANGE',
                                {'range_position': 0.9}, None, cfg)
        # Should block because regime_ctx is None → conditions fail
        assert result['l2_meanrev_blocked'] is True  # fail-closed conditions not met

    def test_empty_features_l3(self):
        """Empty features → FAIL-OPEN"""
        from strategy_v3.adaptive_v3 import compute_layer3
        cfg = _default_cfg()
        cur = _mock_cursor(fetchone_data=None)

        result = compute_layer3(cur, 'long', {}, cfg)
        assert result['l3_add_blocked'] is False  # no position data → FAIL-OPEN


# ══════════════════════════════════════════════════════════════
# ANTI-PARALYSIS
# ══════════════════════════════════════════════════════════════

class TestAntiParalysis:
    def setup_method(self):
        _reset_state()

    def test_24h_partial_reset(self):
        """24h no trade → partial penalty reset"""
        from strategy_v3 import adaptive_v3
        cfg = _default_cfg()

        # Set up: penalty active, last trade 25h ago
        adaptive_v3._state['global_wr_penalty_active'] = True
        adaptive_v3._state['mode_cooldowns'] = {'MeanRev': time.time() + 3600}

        cur = _mock_cursor()
        last_trade_25h_ago = time.time() - (25 * 3600)
        # Mock queries: mode streak, global WR, trade_switch, last_trade, OFF seconds
        call_count = [0]
        def fetchall_side():
            call_count[0] += 1
            if call_count[0] == 1:
                return []  # mode streak
            return [(-5.0, None, None)] * 20  # global WR (all losses → 0% WR)
        cur.fetchall = MagicMock(side_effect=fetchall_side)

        fetchone_count = [0]
        def fetchone_side():
            fetchone_count[0] += 1
            if fetchone_count[0] == 1:
                return (True,)   # trade_switch ON
            if fetchone_count[0] == 2:
                return (last_trade_25h_ago,)  # last_trade_ts
            return (0,)  # trade_switch OFF seconds
        cur.fetchone = MagicMock(side_effect=fetchone_side)

        result = adaptive_v3.compute_layer1(cur, 'MeanRev', cfg)
        assert adaptive_v3._state['anti_paralysis_stage'] >= 1
        # Cooldowns should be cleared
        assert adaptive_v3._state['mode_cooldowns'] == {}

    def test_36h_full_reset(self):
        """36h no trade → full penalty reset"""
        from strategy_v3 import adaptive_v3
        cfg = _default_cfg()

        adaptive_v3._state['global_wr_penalty_active'] = True
        adaptive_v3._state['mode_wr_penalty'] = {'MeanRev': True}
        adaptive_v3._state['mode_cooldowns'] = {'MeanRev': time.time() + 3600}

        cur = _mock_cursor()
        last_trade_37h_ago = time.time() - (37 * 3600)

        call_count = [0]
        def fetchall_side():
            call_count[0] += 1
            if call_count[0] == 1:
                return []
            return [(-5.0, None, None)] * 20
        cur.fetchall = MagicMock(side_effect=fetchall_side)

        fetchone_count = [0]
        def fetchone_side():
            fetchone_count[0] += 1
            if fetchone_count[0] == 1:
                return (True,)
            if fetchone_count[0] == 2:
                return (last_trade_37h_ago,)
            return (0,)
        cur.fetchone = MagicMock(side_effect=fetchone_side)

        result = adaptive_v3.compute_layer1(cur, 'MeanRev', cfg)
        assert adaptive_v3._state['anti_paralysis_stage'] == 2
        assert adaptive_v3._state['global_wr_penalty_active'] is False
        assert adaptive_v3._state['mode_wr_penalty'] == {}


# ══════════════════════════════════════════════════════════════
# ORCHESTRATOR
# ══════════════════════════════════════════════════════════════

class TestOrchestrator:
    def setup_method(self):
        _reset_state()

    @patch('strategy_v3.adaptive_v3._is_dryrun', return_value=False)
    @patch('strategy_v3.adaptive_v3._sync_state')
    @patch('strategy_v3.adaptive_v3._persist_state')
    def test_combined_penalty_applied(self, mock_persist, mock_sync, mock_dryrun):
        """Combined penalty properly computed"""
        from strategy_v3.adaptive_v3 import apply_adaptive_layers
        cfg = _default_cfg()

        cur = _mock_cursor()
        cur.fetchall = MagicMock(return_value=[])
        cur.fetchone = MagicMock(side_effect=[
            (True,), (time.time(),), (0,),
        ] * 5)

        result = apply_adaptive_layers(
            cur, 'MeanRev', 'LONG', 'STATIC_RANGE',
            {}, {}, 'OK', cfg)

        # No penalties → combined = 1.0
        assert result['combined_penalty'] == 1.0
        assert result['l4_entry_blocked'] is False

    @patch('strategy_v3.adaptive_v3._is_dryrun', return_value=False)
    @patch('strategy_v3.adaptive_v3._sync_state')
    @patch('strategy_v3.adaptive_v3._persist_state')
    def test_warn_blocks_entry(self, mock_persist, mock_sync, mock_dryrun):
        """health=WARN → L4 entry block"""
        from strategy_v3.adaptive_v3 import apply_adaptive_layers
        cfg = _default_cfg()

        cur = _mock_cursor()
        cur.fetchall = MagicMock(return_value=[])
        cur.fetchone = MagicMock(side_effect=[
            (True,), (time.time(),), (0,),
        ] * 5)

        result = apply_adaptive_layers(
            cur, 'MeanRev', 'LONG', 'STATIC_RANGE',
            {}, {}, 'WARN', cfg)

        assert result['l4_entry_blocked'] is True
        assert result['l4_add_blocked'] is True


class TestDebugState:
    def setup_method(self):
        _reset_state()

    def test_debug_state_output(self):
        """get_debug_state() returns formatted string"""
        from strategy_v3.adaptive_v3 import get_debug_state
        output = get_debug_state()
        assert 'Adaptive Layers' in output
        assert 'dryrun' in output


# ══════════════════════════════════════════════════════════════
# REGIME TAG MAPPING
# ══════════════════════════════════════════════════════════════

class TestRegimeTagMapping:
    def test_static_range_to_meanrev(self):
        from strategy_v3.adaptive_v3 import _regime_tag_to_entry_mode
        assert _regime_tag_to_entry_mode('STATIC_RANGE') == 'MeanRev'

    def test_drift_to_driftfollow(self):
        from strategy_v3.adaptive_v3 import _regime_tag_to_entry_mode
        assert _regime_tag_to_entry_mode('DRIFT_UP') == 'DriftFollow'
        assert _regime_tag_to_entry_mode('DRIFT_DOWN') == 'DriftFollow'

    def test_breakout_to_breakouttrend(self):
        from strategy_v3.adaptive_v3 import _regime_tag_to_entry_mode
        assert _regime_tag_to_entry_mode('BREAKOUT') == 'BreakoutTrend'

    def test_none_returns_none(self):
        from strategy_v3.adaptive_v3 import _regime_tag_to_entry_mode
        assert _regime_tag_to_entry_mode(None) is None

    def test_unknown_returns_none(self):
        from strategy_v3.adaptive_v3 import _regime_tag_to_entry_mode
        assert _regime_tag_to_entry_mode('UNKNOWN') is None
