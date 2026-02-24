"""
tests/test_event_decision.py — Event Decision Mode (EVENT_DECISION) 검증.

7개 시나리오:
  1. 플래시 급락: ret_1m=-0.5% → MODE_EVENT_DECISION → RISK_OFF_REDUCE
  2. 플래시 급등: ret_5m=+1.2% → MODE_EVENT_DECISION → HOLD
  3. 유동성 스트레스: liquidity_ok=NO → REVERSE → 가드가 HARD_EXIT로 강제
  4. 오펀 주문: HARD_EXIT 후 cleanup
  5. SL 실패: sync_event_stop 실패 → Telegram 경고
  6. FF OFF: ff_event_decision_mode=false → MODE_EVENT 유지
  7. 파싱 실패: 잘못된 Claude 응답 → HOLD fallback
"""

import sys
import os
import json
import time
import pytest
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import feature_flags
import event_trigger
import claude_api
import event_decision_engine


# ── Helpers ──────────────────────────────────────────────

def _reset_event_trigger():
    """Reset event_trigger module state for clean test."""
    event_trigger._prev_edge_state.update({
        'price_spike_1m': False,
        'price_spike_5m': False,
        'price_spike_15m': False,
        'volume_spike': False,
        'atr_increase': False,
        'range_position_extreme': False,
        'impulse_spike': False,
        'liquidity_stress': False,
    })
    event_trigger._last_trigger_ts.clear()
    event_trigger._last_evaluate_ts = 0
    event_trigger._event_bundle['triggers'] = []
    event_trigger._event_bundle['first_ts'] = 0
    event_trigger._event_bundle['whipsaw_first_ts'] = 0
    event_trigger._emergency_lock.clear()


def _evaluate_with_flush(snapshot, **kwargs):
    """Call evaluate with BUNDLE_WINDOW_SEC=0 so triggers flush immediately.

    The 30s bundle window normally prevents immediate trigger return.
    Setting it to 0 bypasses the accumulation window for testing.
    """
    saved = event_trigger.BUNDLE_WINDOW_SEC
    event_trigger.BUNDLE_WINDOW_SEC = 0
    try:
        result = event_trigger.evaluate(snapshot, **kwargs)
    finally:
        event_trigger.BUNDLE_WINDOW_SEC = saved
    return result


def _base_snapshot(**overrides):
    """Base snapshot with sensible defaults."""
    s = {
        'price': 95000,
        'returns': {
            'ret_1m': 0.0,
            'ret_5m': 0.0,
            'ret_15m': 0.0,
        },
        'vol_ratio': 1.0,
        'atr_14': 150,
        'atr_pct': 0.15,
        'rsi_14': 50,
        'regime': 'DRIFT',
        'drift': 0.0,
        'adx': 20,
        'impulse': 0.0,
        'volume_z': 0.0,
        'range_pos': 0.5,
        'breakout': False,
        'spread_ok': True,
        'liquidity_ok': True,
        'bb_upper': 96000,
        'bb_lower': 94000,
        'bb_mid': 95000,
        'snapshot_ts': time.time(),
        'vol_profile_ts': time.time(),
        'poc': 95000,
        'vah': 95500,
        'val': 94500,
        'orderbook_imbalance': 0,
        'slippage_est': 0,
        'bar_15m_returns': [0.1, -0.1, 0.05],
    }
    # Deep merge returns
    if 'returns' in overrides:
        s['returns'].update(overrides.pop('returns'))
    s.update(overrides)
    return s


def _mock_cursor():
    """Create a mock DB cursor."""
    cur = MagicMock()
    cur.execute = MagicMock()
    cur.fetchone = MagicMock(return_value=None)
    cur.fetchall = MagicMock(return_value=[])
    return cur


def _position(side='LONG', qty=0.01, entry=95000, mark=95000, upnl=0):
    return {
        'side': side,
        'qty': qty,
        'entry_price': entry,
        'mark_price': mark,
        'upnl': upnl,
        'upnl_pct': (upnl / (entry * qty) * 100) if entry * qty > 0 else 0,
        'leverage': 10,
        'liq_price': 85000 if side == 'LONG' else 105000,
    }


# ══════════════════════════════════════════════════════════════
# SCENARIO 1: 플래시 급락 → MODE_EVENT_DECISION → RISK_OFF_REDUCE
# ══════════════════════════════════════════════════════════════

class TestScenario1FlashDrop:
    """ret_1m=-0.5% → EVENT_DECISION trigger → Claude=RISK_OFF_REDUCE → REDUCE"""

    def test_event_trigger_fires_at_lower_threshold(self):
        """ff ON + ret_1m=-0.5% → MODE_EVENT_DECISION (기존 1.0% 대신 0.5%)"""
        _reset_event_trigger()
        feature_flags._cache = {'ff_event_decision_mode': True}

        snapshot = _base_snapshot(returns={'ret_1m': -0.55, 'ret_5m': -0.3, 'ret_15m': -0.1})
        result = _evaluate_with_flush(snapshot, cur=_mock_cursor())

        assert result.mode == event_trigger.MODE_EVENT_DECISION
        assert result.call_type == 'AUTO_EMERGENCY'
        trigger_types = [t['type'] for t in result.triggers]
        assert 'price_spike_1m' in trigger_types

    def test_safety_guard_reduce_ratio_clamp(self):
        """reduce_ratio 0.9 → 0.7 클램프"""
        action, params, reasons = event_decision_engine._apply_safety_guards(
            'RISK_OFF_REDUCE',
            {'reduce_ratio': 0.9},
            _position(),
            _base_snapshot(),
        )
        assert action == 'RISK_OFF_REDUCE'
        assert params['reduce_ratio'] == 0.70
        assert any('clamped' in r for r in reasons)

    def test_map_reduce_to_execution(self):
        """RISK_OFF_REDUCE → REDUCE enqueue"""
        cur = _mock_cursor()
        cur.fetchone.return_value = None  # no duplicate

        # Mock the RETURNING id
        cur.fetchone.side_effect = [None, (42,)]  # dedup check → None, INSERT → id

        eq_ids = event_decision_engine._map_action_to_execution(
            cur, 'RISK_OFF_REDUCE',
            {'reduce_ratio': 0.5},
            _position(qty=0.01),
            'BTC/USDT:USDT',
        )
        assert len(eq_ids) == 1
        assert eq_ids[0] == 42

    def test_end_to_end_reduce(self):
        """Full flow mock: Claude returns RISK_OFF_REDUCE → action mapped."""
        claude_response = {
            'event_class': 'FLASH_DROP',
            'confidence': 0.85,
            'action': 'RISK_OFF_REDUCE',
            'params': {'reduce_ratio': 0.5, 'freeze_minutes': 0,
                       'new_sl_type': '', 'new_sl_value': 0,
                       'reverse_size_ratio': 0, 'hedge_size_ratio': 0},
            'reasoning_short': 'Flash drop detected, reduce exposure',
            'safety_checks': {'orphan_orders_cleanup_required': False,
                              'stop_order_required': True, 'reverse_allowed': False},
            'fallback_used': False,
            'api_latency_ms': 500,
            'input_tokens': 100, 'output_tokens': 50,
            'estimated_cost_usd': 0.001, 'model': 'claude-sonnet',
            'call_type': 'AUTO_EMERGENCY',
        }

        cur = _mock_cursor()
        cur.fetchone.side_effect = [None, (99,)]  # dedup → None, INSERT → id

        er = event_trigger.EventResult(
            mode=event_trigger.MODE_EVENT_DECISION,
            triggers=[{'type': 'price_spike_1m', 'value': -0.55, 'threshold': 0.5}],
            event_hash='test_hash_1',
            call_type='AUTO_EMERGENCY',
        )

        with patch('claude_api.event_decision_analysis', return_value=claude_response), \
             patch('event_decision_engine._enforce_server_stop'), \
             patch('event_decision_engine._send_telegram'), \
             patch('event_decision_engine._acquire_entry_lock', return_value=('key', 120)), \
             patch('event_decision_engine._post_execution_cleanup'), \
             patch('event_decision_engine._build_snapshot_bundle', return_value={'position': {}, 'orders': {}, 'mctx': {}, 'microstructure': {}, 'recent_execution': [], 'system_health': {}, 'triggers': [], 'risk_config': {}}), \
             patch('os.path.exists', return_value=False):
            action, detail = event_decision_engine.handle_event_decision(
                cur, {}, er, _base_snapshot(), _position(), conn=MagicMock())

        assert action == 'RISK_OFF_REDUCE'
        assert detail['original_action'] == 'RISK_OFF_REDUCE'
        assert detail['event_class'] == 'FLASH_DROP'


# ══════════════════════════════════════════════════════════════
# SCENARIO 2: 플래시 급등 → HOLD
# ══════════════════════════════════════════════════════════════

class TestScenario2FlashPump:
    """ret_5m=+1.2% → EVENT_DECISION → Claude=HOLD → SL 확인 후 유지"""

    def test_event_trigger_5m_decision(self):
        """ff ON + ret_5m=+1.2% → MODE_EVENT_DECISION (기존 1.8% 대신 1.0%)"""
        _reset_event_trigger()
        feature_flags._cache = {'ff_event_decision_mode': True}

        snapshot = _base_snapshot(returns={'ret_1m': 0.3, 'ret_5m': 1.2, 'ret_15m': 0.5})
        result = _evaluate_with_flush(snapshot, cur=_mock_cursor())

        assert result.mode == event_trigger.MODE_EVENT_DECISION
        trigger_types = [t['type'] for t in result.triggers]
        assert 'price_spike_5m' in trigger_types

    def test_hold_no_execution(self):
        """HOLD → no execution_queue entries"""
        cur = _mock_cursor()
        eq_ids = event_decision_engine._map_action_to_execution(
            cur, 'HOLD', {}, _position(), 'BTC/USDT:USDT')
        assert eq_ids == []
        # Verify no INSERT was called
        for c in cur.execute.call_args_list:
            assert 'INSERT INTO execution_queue' not in str(c)


# ══════════════════════════════════════════════════════════════
# SCENARIO 3: 유동성 스트레스 → REVERSE → HARD_EXIT 강제
# ══════════════════════════════════════════════════════════════

class TestScenario3LiquidityStress:
    """liquidity_ok=NO → REVERSE 요청 → 가드가 HARD_EXIT로 강제 전환"""

    def test_liquidity_stress_trigger(self):
        """spread_ok=False → liquidity_stress trigger 발동"""
        _reset_event_trigger()
        feature_flags._cache = {'ff_event_decision_mode': True}

        snapshot = _base_snapshot(
            spread_ok=False, liquidity_ok=False,
            returns={'ret_1m': -0.6, 'ret_5m': -0.3, 'ret_15m': 0})
        result = _evaluate_with_flush(snapshot, cur=_mock_cursor())

        assert result.mode == event_trigger.MODE_EVENT_DECISION
        trigger_types = [t['type'] for t in result.triggers]
        assert 'liquidity_stress' in trigger_types

    def test_guard_blocks_reverse_on_liquidity_stress(self):
        """liquidity_ok=False + REVERSE → HARD_EXIT 강제 전환"""
        action, params, reasons = event_decision_engine._apply_safety_guards(
            'REVERSE',
            {'reverse_size_ratio': 0.2},
            _position(),
            _base_snapshot(spread_ok=False, liquidity_ok=False),
        )
        assert action == 'HARD_EXIT'
        assert any('liquidity stress' in r for r in reasons)

    def test_guard_blocks_hedge_on_liquidity_stress(self):
        """liquidity_ok=False + HEDGE → HARD_EXIT 강제 전환"""
        action, params, reasons = event_decision_engine._apply_safety_guards(
            'HEDGE',
            {'hedge_size_ratio': 0.2},
            _position(),
            _base_snapshot(liquidity_ok=False),
        )
        assert action == 'HARD_EXIT'
        assert any('liquidity stress' in r for r in reasons)

    def test_guard_allows_reduce_on_liquidity_stress(self):
        """liquidity_ok=False + RISK_OFF_REDUCE → 허용 (LIQUIDITY_STRESS_ALLOWED)"""
        action, params, reasons = event_decision_engine._apply_safety_guards(
            'RISK_OFF_REDUCE',
            {'reduce_ratio': 0.5},
            _position(),
            _base_snapshot(liquidity_ok=False),
        )
        assert action == 'RISK_OFF_REDUCE'


# ══════════════════════════════════════════════════════════════
# SCENARIO 4: 오펀 주문 — HARD_EXIT 후 cleanup
# ══════════════════════════════════════════════════════════════

class TestScenario4OrphanCleanup:
    """HARD_EXIT 후 → 포지션 0 → cleanup"""

    def test_hard_exit_enqueue(self):
        """HARD_EXIT → FULL_CLOSE enqueue"""
        cur = _mock_cursor()
        cur.fetchone.side_effect = [None, (77,)]  # dedup → None, INSERT → id

        eq_ids = event_decision_engine._map_action_to_execution(
            cur, 'HARD_EXIT', {},
            _position(side='LONG', qty=0.01),
            'BTC/USDT:USDT',
        )
        assert len(eq_ids) == 1
        assert eq_ids[0] == 77

        # Verify FULL_CLOSE was used
        insert_call = [c for c in cur.execute.call_args_list
                       if 'INSERT INTO execution_queue' in str(c)]
        assert len(insert_call) == 1
        assert 'FULL_CLOSE' in str(insert_call[0])

    def test_post_exit_cleanup_called(self):
        """HARD_EXIT triggers post_execution_cleanup (FAIL-OPEN on import error)"""
        # _post_execution_cleanup imports live_order_executor + orphan_cleanup
        # In test env these may not resolve — verify it doesn't crash (FAIL-OPEN)
        with patch.dict('sys.modules', {'live_order_executor': MagicMock()}):
            with patch('orphan_cleanup.post_exit_cleanup', return_value=(True, 'ok')) as mock_cleanup:
                event_decision_engine._post_execution_cleanup('BTC/USDT:USDT', 'HARD_EXIT')
                mock_cleanup.assert_called_once()

    def test_post_exit_cleanup_skip_non_exit(self):
        """HOLD → cleanup 미호출"""
        with patch('orphan_cleanup.post_exit_cleanup') as mock_cleanup:
            event_decision_engine._post_execution_cleanup('BTC/USDT:USDT', 'HOLD')
            mock_cleanup.assert_not_called()
            # The function itself tries to import and call


# ══════════════════════════════════════════════════════════════
# SCENARIO 5: SL 실패 → Telegram 경고
# ══════════════════════════════════════════════════════════════

class TestScenario5SLFailure:
    """sync_event_stop 실패 → Telegram 알림"""

    def test_enforce_stop_sends_telegram_on_failure(self):
        """SL 설정 실패 시 Telegram 경고 발송"""
        with patch('server_stop_manager.get_manager') as mock_mgr_fn, \
             patch('event_decision_engine._send_telegram') as mock_tg:
            mock_mgr = MagicMock()
            mock_mgr.sync_event_stop.return_value = (False, 'exchange timeout')
            mock_mgr_fn.return_value = mock_mgr

            event_decision_engine._enforce_server_stop(
                _mock_cursor(),
                _position(),
                {'new_sl_value': 0, 'new_sl_type': ''},
            )

            mock_tg.assert_called_once()
            assert 'HARD STOP SET FAILED' in mock_tg.call_args[0][0]

    def test_enforce_stop_no_alert_on_success(self):
        """SL 설정 성공 시 Telegram 미발송"""
        with patch('server_stop_manager.get_manager') as mock_mgr_fn, \
             patch('event_decision_engine._send_telegram') as mock_tg:
            mock_mgr = MagicMock()
            mock_mgr.sync_event_stop.return_value = (True, 'stop placed @ 93000')
            mock_mgr_fn.return_value = mock_mgr

            event_decision_engine._enforce_server_stop(
                _mock_cursor(),
                _position(),
                {'new_sl_value': 0, 'new_sl_type': ''},
            )

            mock_tg.assert_not_called()


# ══════════════════════════════════════════════════════════════
# SCENARIO 6: FF OFF → 기존 MODE_EVENT 유지
# ══════════════════════════════════════════════════════════════

class TestScenario6FFOff:
    """ff_event_decision_mode=false → MODE_EVENT (GPT-mini) 흐름 유지"""

    def test_ff_off_returns_mode_event(self):
        """FF OFF + ret_1m=-1.1% → MODE_EVENT (not EVENT_DECISION)"""
        _reset_event_trigger()
        feature_flags._cache = {'ff_event_decision_mode': False}

        snapshot = _base_snapshot(returns={'ret_1m': -1.1, 'ret_5m': -0.5, 'ret_15m': -0.2})
        result = _evaluate_with_flush(snapshot, cur=_mock_cursor())

        assert result.mode == event_trigger.MODE_EVENT
        assert result.call_type == 'AUTO'  # not AUTO_EMERGENCY

    def test_ff_off_uses_original_thresholds(self):
        """FF OFF + ret_1m=-0.5% → DEFAULT (below original 1.0% threshold)"""
        _reset_event_trigger()
        feature_flags._cache = {'ff_event_decision_mode': False}

        snapshot = _base_snapshot(returns={'ret_1m': -0.55, 'ret_5m': -0.1, 'ret_15m': 0})
        result = event_trigger.evaluate(snapshot, cur=_mock_cursor())

        # 0.55% < original 1.0% threshold → should NOT trigger
        assert result.mode == event_trigger.MODE_DEFAULT

    def test_ff_off_no_new_triggers(self):
        """FF OFF → range_position_extreme / impulse_spike / liquidity_stress 미발동"""
        _reset_event_trigger()
        feature_flags._cache = {'ff_event_decision_mode': False}

        # Extreme range_pos but no price spike
        snapshot = _base_snapshot(range_pos=1.5)
        result = event_trigger.evaluate(snapshot, cur=_mock_cursor())

        # No triggers → DEFAULT
        assert result.mode == event_trigger.MODE_DEFAULT


# ══════════════════════════════════════════════════════════════
# SCENARIO 7: 파싱 실패 → HOLD fallback
# ══════════════════════════════════════════════════════════════

class TestScenario7ParseFailure:
    """Claude 응답 invalid → HOLD fallback"""

    def test_empty_response(self):
        """빈 응답 → HOLD fallback"""
        result = claude_api._parse_event_decision_response('')
        assert result['action'] == 'HOLD'
        assert result['fallback_used'] is True

    def test_invalid_json(self):
        """잘못된 JSON → HOLD fallback"""
        result = claude_api._parse_event_decision_response('this is not json at all')
        assert result['action'] == 'HOLD'
        assert result['fallback_used'] is True

    def test_invalid_action(self):
        """유효하지 않은 action → HOLD fallback"""
        result = claude_api._parse_event_decision_response(json.dumps({
            'event_class': 'FLASH_DROP',
            'confidence': 0.8,
            'action': 'INVALID_ACTION',
            'params': {},
        }))
        assert result['action'] == 'HOLD'
        assert result['fallback_used'] is True

    def test_valid_json_parsed(self):
        """올바른 JSON → 정상 파싱"""
        result = claude_api._parse_event_decision_response(json.dumps({
            'event_class': 'FLASH_DROP',
            'confidence': 0.85,
            'action': 'RISK_OFF_REDUCE',
            'params': {
                'reduce_ratio': 0.5,
                'freeze_minutes': 10,
                'new_sl_type': 'ATR_TRAIL',
                'new_sl_value': 93000,
                'reverse_size_ratio': 0,
                'hedge_size_ratio': 0,
            },
            'reasoning_short': 'Flash drop, reduce risk',
            'safety_checks': {
                'orphan_orders_cleanup_required': False,
                'stop_order_required': True,
                'reverse_allowed': False,
            },
        }))
        assert result['action'] == 'RISK_OFF_REDUCE'
        assert result['event_class'] == 'FLASH_DROP'
        assert result['confidence'] == 0.85
        assert result['params']['reduce_ratio'] == 0.5
        assert result['fallback_used'] is False

    def test_markdown_wrapped_json(self):
        """```json 래핑된 응답 → 정상 파싱"""
        text = '```json\n{"event_class":"BREAKOUT","confidence":0.7,"action":"HOLD","params":{},"reasoning_short":"ok","safety_checks":{}}\n```'
        result = claude_api._parse_event_decision_response(text)
        assert result['action'] == 'HOLD'
        assert result['event_class'] == 'BREAKOUT'
        assert result['fallback_used'] is False

    def test_param_clamping(self):
        """파라미터 범위 클램핑: reduce_ratio > 1.0 → 1.0, freeze > 60 → 60"""
        result = claude_api._parse_event_decision_response(json.dumps({
            'event_class': 'FLASH_DROP',
            'confidence': 1.5,  # > 1.0 → clamped
            'action': 'RISK_OFF_REDUCE',
            'params': {
                'reduce_ratio': 2.0,     # > 1.0 → clamped
                'freeze_minutes': 120,   # > 60 → clamped
                'reverse_size_ratio': 5, # > 1.0 → clamped
                'hedge_size_ratio': -1,  # < 0 → clamped
            },
            'reasoning_short': 'test',
            'safety_checks': {},
        }))
        assert result['confidence'] == 1.0
        assert result['params']['reduce_ratio'] == 1.0
        assert result['params']['freeze_minutes'] == 60
        assert result['params']['reverse_size_ratio'] == 1.0
        assert result['params']['hedge_size_ratio'] == 0.0

    def test_regex_fallback_extraction(self):
        """JSON 앞에 텍스트 → regex fallback 추출"""
        text = 'Here is my analysis:\n\n{"event_class":"FAKEOUT","confidence":0.3,"action":"HOLD","params":{},"reasoning_short":"noise","safety_checks":{}}'
        result = claude_api._parse_event_decision_response(text)
        assert result['action'] == 'HOLD'
        assert result['event_class'] == 'FAKEOUT'
        assert result['fallback_used'] is False


# ══════════════════════════════════════════════════════════════
# ADDITIONAL: Safety guard edge cases
# ══════════════════════════════════════════════════════════════

class TestSafetyGuardEdgeCases:
    """Safety guard boundary conditions"""

    def test_no_position_exit_actions_to_hold(self):
        """포지션 없음 + HARD_EXIT/REVERSE/HEDGE → HOLD"""
        for action in ('HARD_EXIT', 'REVERSE', 'HEDGE', 'RISK_OFF_REDUCE'):
            a, p, reasons = event_decision_engine._apply_safety_guards(
                action, {}, {'side': '', 'qty': 0}, _base_snapshot())
            assert a == 'HOLD', f'{action} should become HOLD with no position'
            assert any('no position' in r for r in reasons)

    def test_unrecognized_action_to_hold(self):
        """미인식 action → HOLD"""
        a, p, reasons = event_decision_engine._apply_safety_guards(
            'YOLO_TRADE', {}, _position(), _base_snapshot())
        assert a == 'HOLD'
        assert any('unrecognized' in r for r in reasons)

    def test_freeze_new_entry_mapping(self):
        """FREEZE_NEW_ENTRY → execution_queue 미사용, lock만 설정"""
        cur = _mock_cursor()
        with patch('event_lock.acquire_lock') as mock_lock:
            eq_ids = event_decision_engine._map_action_to_execution(
                cur, 'FREEZE_NEW_ENTRY',
                {'freeze_minutes': 15},
                _position(), 'BTC/USDT:USDT')
            assert eq_ids == []
            mock_lock.assert_called_once()

    def test_reverse_mapping(self):
        """REVERSE → REVERSE_CLOSE + REVERSE_OPEN"""
        cur = _mock_cursor()
        cur.fetchone.side_effect = [None, (10,), None, (11,)]  # 2 dedup + 2 INSERT

        eq_ids = event_decision_engine._map_action_to_execution(
            cur, 'REVERSE', {'reverse_size_ratio': 0.2},
            _position(side='LONG', qty=0.01), 'BTC/USDT:USDT')
        assert len(eq_ids) == 2

    def test_hedge_mapping(self):
        """HEDGE → ADD (반대방향)"""
        cur = _mock_cursor()
        cur.fetchone.side_effect = [None, (20,)]  # dedup + INSERT

        eq_ids = event_decision_engine._map_action_to_execution(
            cur, 'HEDGE',
            {'hedge_size_ratio': 0.2},
            _position(side='LONG', qty=0.01, mark=95000),
            'BTC/USDT:USDT')
        assert len(eq_ids) == 1

    def test_all_guards_together(self):
        """모든 가드 동시 적용: 높은 비율 + 유동성 스트레스"""
        action, params, reasons = event_decision_engine._apply_safety_guards(
            'REVERSE',
            {'reduce_ratio': 0.9, 'reverse_size_ratio': 0.5,
             'hedge_size_ratio': 0.5, 'freeze_minutes': 120},
            _position(),
            _base_snapshot(spread_ok=False),
        )
        # REVERSE blocked by liquidity → HARD_EXIT
        assert action == 'HARD_EXIT'
        assert params['reduce_ratio'] == 0.70
        assert params['reverse_size_ratio'] == 0.30
        assert params['hedge_size_ratio'] == 0.30
        assert params['freeze_minutes'] == 60
        assert len(reasons) >= 4  # 4 clamps + liquidity


# ══════════════════════════════════════════════════════════════
# ADDITIONAL: New trigger checks
# ══════════════════════════════════════════════════════════════

class TestNewTriggerChecks:
    """New EVENT_DECISION-specific trigger functions"""

    def test_range_position_extreme_low(self):
        """range_pos < 0.0 → trigger"""
        event_trigger._prev_edge_state['range_position_extreme'] = False
        triggers = event_trigger._check_range_position_extreme(
            _base_snapshot(range_pos=-0.2))
        assert len(triggers) == 1
        assert triggers[0]['type'] == 'range_position_extreme'
        assert triggers[0]['direction'] == 'down'

    def test_range_position_extreme_high(self):
        """range_pos > 1.0 → trigger"""
        event_trigger._prev_edge_state['range_position_extreme'] = False
        triggers = event_trigger._check_range_position_extreme(
            _base_snapshot(range_pos=1.3))
        assert len(triggers) == 1
        assert triggers[0]['direction'] == 'up'

    def test_range_position_normal_no_trigger(self):
        """range_pos 0.5 → no trigger"""
        event_trigger._prev_edge_state['range_position_extreme'] = False
        triggers = event_trigger._check_range_position_extreme(
            _base_snapshot(range_pos=0.5))
        assert len(triggers) == 0

    def test_liquidity_stress_both_false(self):
        """spread_ok=False + liquidity_ok=False → trigger"""
        event_trigger._prev_edge_state['liquidity_stress'] = False
        triggers = event_trigger._check_liquidity_stress(
            _base_snapshot(spread_ok=False, liquidity_ok=False))
        assert len(triggers) == 1
        assert triggers[0]['type'] == 'liquidity_stress'

    def test_liquidity_stress_ok(self):
        """spread_ok=True + liquidity_ok=True → no trigger"""
        event_trigger._prev_edge_state['liquidity_stress'] = False
        triggers = event_trigger._check_liquidity_stress(
            _base_snapshot(spread_ok=True, liquidity_ok=True))
        assert len(triggers) == 0

    def test_impulse_spike_trigger(self):
        """impulse >= 1.0 → trigger"""
        event_trigger._prev_edge_state['impulse_spike'] = False
        with patch('strategy.common.features.compute_impulse', return_value=2.0):
            triggers = event_trigger._check_impulse_spike(
                _base_snapshot(impulse=2.0), _mock_cursor())
        assert len(triggers) == 1
        assert triggers[0]['type'] == 'impulse_spike'
        assert triggers[0]['direction'] == 'up'

    def test_impulse_spike_negative(self):
        """impulse <= -1.0 → trigger (down)"""
        event_trigger._prev_edge_state['impulse_spike'] = False
        with patch('strategy.common.features.compute_impulse', return_value=-1.8):
            triggers = event_trigger._check_impulse_spike(
                _base_snapshot(impulse=-1.8), _mock_cursor())
        assert len(triggers) == 1
        assert triggers[0]['direction'] == 'down'

    def test_impulse_normal_no_trigger(self):
        """impulse 0.5 → no trigger"""
        event_trigger._prev_edge_state['impulse_spike'] = False
        with patch('strategy.common.features.compute_impulse', return_value=0.5):
            triggers = event_trigger._check_impulse_spike(
                _base_snapshot(impulse=0.5), _mock_cursor())
        assert len(triggers) == 0
