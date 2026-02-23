"""
strategy_v3.adaptive_v3 — Adaptive 5-Layer defence system.

Layers:
  L1: Loss-streak adaptive filter (entry_mode별 streak + global WR + anti-paralysis)
  L2: MeanReversion protection (SHORT 허용 조건 강화)
  L3: ADD logic restriction (uPnL-based ADD gate)
  L4: health=WARN risk control (entry/ADD block + time_stop tighten)
  L5: Mode performance tracking (per-mode WR penalty)

State persistence: DB (adaptive_layer_state) + JSON fallback.
FAIL-OPEN: any error → no penalty, no block.
"""

import json
import os
import time
import traceback

LOG_PREFIX = '[adaptive_v3]'

_STATE_FILE = '/tmp/adaptive_v3_state.json'

# ── In-memory state ──
_state = {
    'global_wr_penalty_active': False,
    'mode_wr_penalty': {},        # {entry_mode: bool}
    'mode_cooldowns': {},         # {entry_mode: expire_ts}
    'warn_since_ts': 0,
    'last_trade_ts': 0,
    'anti_paralysis_stage': 0,    # 0=none, 1=partial_reset, 2=full_reset
    'anti_paralysis_reset_ts': 0,
    # D2-2: 히스테리시스 — penalty 완화 시 연속 개선 신호 카운트
    'wr_recovery_consecutive': 0,  # 연속 개선 카운트
    'last_wr_sample': 0.0,         # 직전 WR 값
}


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _get_cfg():
    """Load adaptive config from config_v3 with FAIL-OPEN defaults."""
    try:
        from strategy_v3 import config_v3
        return config_v3.get_all()
    except Exception:
        return {}


def _is_adaptive_enabled():
    """Check if ff_adaptive_layers flag is ON."""
    try:
        import feature_flags
        return feature_flags.is_enabled('ff_adaptive_layers')
    except Exception:
        return False


def _is_dryrun():
    """Check if adaptive_dryrun mode is ON."""
    cfg = _get_cfg()
    val = cfg.get('adaptive_dryrun', True)
    return str(val).lower() in ('true', '1', 'on')


# ── State Persistence ──

def _load_state_from_db(cur):
    """Load adaptive state from DB. Returns dict or None."""
    try:
        cur.execute("SELECT key, value FROM adaptive_layer_state;")
        rows = cur.fetchall()
        if not rows:
            return None
        result = {}
        for key, val in rows:
            result[key] = val
        return result
    except Exception:
        return None


def _save_state_to_db(cur, key, value):
    """Upsert a state key to DB."""
    try:
        cur.execute("""
            INSERT INTO adaptive_layer_state (key, value, updated_at)
            VALUES (%s, %s::jsonb, now())
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();
        """, (key, json.dumps(value, default=str)))
    except Exception as e:
        _log(f'save_state_to_db FAIL: {e}')


def _save_state_to_file():
    """Backup state to JSON file."""
    try:
        with open(_STATE_FILE, 'w') as f:
            json.dump(_state, f, default=str)
    except Exception:
        pass


def _load_state_from_file():
    """Load state from JSON file fallback."""
    try:
        if os.path.exists(_STATE_FILE):
            with open(_STATE_FILE, 'r') as f:
                return json.load(f)
    except Exception:
        pass
    return None


def _sync_state(cur):
    """Sync in-memory state with DB (load on start, save on change)."""
    global _state
    try:
        db_state = _load_state_from_db(cur)
        if db_state:
            for key, val in db_state.items():
                if key == 'global_wr_penalty_active':
                    _state['global_wr_penalty_active'] = bool(val.get('active', False))
                elif key == 'mode_wr_penalty':
                    _state['mode_wr_penalty'] = val
                elif key == 'mode_cooldowns':
                    _state['mode_cooldowns'] = val
                elif key == 'warn_since_ts':
                    _state['warn_since_ts'] = float(val.get('ts', 0))
                elif key == 'last_trade_ts':
                    _state['last_trade_ts'] = float(val.get('ts', 0))
                elif key == 'anti_paralysis_stage':
                    _state['anti_paralysis_stage'] = int(val.get('stage', 0))
                    _state['anti_paralysis_reset_ts'] = float(val.get('reset_ts', 0))
        else:
            file_state = _load_state_from_file()
            if file_state:
                _state.update(file_state)
    except Exception as e:
        _log(f'sync_state FAIL-OPEN: {e}')


def _persist_state(cur):
    """Persist current in-memory state to DB + file."""
    try:
        _save_state_to_db(cur, 'global_wr_penalty_active',
                          {'active': _state['global_wr_penalty_active']})
        _save_state_to_db(cur, 'mode_wr_penalty', _state['mode_wr_penalty'])
        _save_state_to_db(cur, 'mode_cooldowns', _state['mode_cooldowns'])
        _save_state_to_db(cur, 'warn_since_ts', {'ts': _state['warn_since_ts']})
        _save_state_to_db(cur, 'last_trade_ts', {'ts': _state['last_trade_ts']})
        _save_state_to_db(cur, 'anti_paralysis_stage',
                          {'stage': _state['anti_paralysis_stage'],
                           'reset_ts': _state['anti_paralysis_reset_ts']})
    except Exception as e:
        _log(f'persist_state DB FAIL: {e}')
    _save_state_to_file()


# ══════════════════════════════════════════════════════════════
# LAYER 1: LOSS-STREAK ADAPTIVE FILTER
# ══════════════════════════════════════════════════════════════

def _query_mode_loss_streak(cur, entry_mode, symbol='BTC/USDT:USDT'):
    """Count consecutive recent losses for a specific entry_mode."""
    try:
        cur.execute("""
            SELECT realized_pnl, entry_mode, regime_tag
            FROM execution_log
            WHERE symbol = %s
              AND order_type IN ('CLOSE', 'FULL_CLOSE', 'REDUCE', 'REVERSE_CLOSE')
              AND status = 'FILLED'
            ORDER BY last_fill_at DESC NULLS LAST
            LIMIT 20
        """, (symbol,))
        streak = 0
        for row in cur.fetchall():
            pnl, em, rt = row[0], row[1], row[2]
            # entry_mode fallback: old records without entry_mode use regime_tag
            effective_mode = em or _regime_tag_to_entry_mode(rt)
            if effective_mode != entry_mode:
                continue
            if pnl is not None and float(pnl) < 0:
                streak += 1
            else:
                break
        return streak
    except Exception:
        return 0


def _regime_tag_to_entry_mode(regime_tag):
    """Map regime_tag to entry_mode for backward compatibility."""
    if not regime_tag:
        return None
    mapping = {
        'STATIC_RANGE': 'MeanRev',
        'DRIFT_UP': 'DriftFollow',
        'DRIFT_DOWN': 'DriftFollow',
        'BREAKOUT': 'BreakoutTrend',
    }
    return mapping.get(regime_tag)


def _query_global_wr(cur, n_trades, symbol='BTC/USDT:USDT'):
    """Compute win rate from recent N trades. Returns (wr, total)."""
    try:
        cur.execute("""
            SELECT realized_pnl FROM execution_log
            WHERE symbol = %s
              AND order_type IN ('CLOSE', 'FULL_CLOSE', 'REDUCE', 'REVERSE_CLOSE')
              AND status = 'FILLED'
            ORDER BY last_fill_at DESC NULLS LAST
            LIMIT %s
        """, (symbol, n_trades))
        rows = cur.fetchall()
        total = len(rows)
        if total == 0:
            return (0.5, 0)  # no data → neutral
        wins = sum(1 for r in rows if r[0] is not None and float(r[0]) > 0)
        return (wins / total, total)
    except Exception:
        return (0.5, 0)


def _check_trade_switch_on(cur):
    """Check if trade_switch is ON. Returns True if ON."""
    try:
        cur.execute("SELECT enabled FROM trade_switch ORDER BY id DESC LIMIT 1;")
        row = cur.fetchone()
        if row:
            return bool(row[0])
        return True  # default ON
    except Exception:
        return True


def _get_last_trade_ts(cur, symbol='BTC/USDT:USDT'):
    """Get timestamp of most recent trade."""
    try:
        cur.execute("""
            SELECT extract(epoch from last_fill_at)
            FROM execution_log
            WHERE symbol = %s AND status = 'FILLED'
            ORDER BY last_fill_at DESC NULLS LAST
            LIMIT 1
        """, (symbol,))
        row = cur.fetchone()
        if row and row[0]:
            return float(row[0])
        return 0
    except Exception:
        return 0


def _get_trade_switch_off_seconds(cur):
    """Estimate total seconds trade_switch was OFF in the last 24h.
    Rough heuristic: count OFF→ON transitions."""
    try:
        cur.execute("""
            SELECT extract(epoch from updated_at), enabled
            FROM trade_switch
            ORDER BY updated_at DESC
            LIMIT 50
        """)
        rows = cur.fetchall()
        if not rows:
            return 0
        now = time.time()
        cutoff = now - 86400
        off_seconds = 0
        prev_ts = now
        for ts, enabled in rows:
            ts = float(ts) if ts else 0
            if ts < cutoff:
                break
            if not enabled:
                off_seconds += max(0, prev_ts - ts)
            prev_ts = ts
        return off_seconds
    except Exception:
        return 0


def compute_layer1(cur, entry_mode, cfg):
    """L1: Loss-streak adaptive filter.

    Returns dict:
        l1_penalty: float (1.0 = no penalty, 0.70 = penalized)
        l1_cooldown_active: bool
        l1_cooldown_remaining: int (seconds)
        l1_global_wr: float
        l1_global_wr_block: bool
        l1_effective_threshold_add: int (threshold increase)
        l1_effective_add_conf_min: int
    """
    result = {
        'l1_penalty': 1.0,
        'l1_cooldown_active': False,
        'l1_cooldown_remaining': 0,
        'l1_global_wr': 0.5,
        'l1_global_wr_block': False,
        'l1_effective_threshold_add': 0,
        'l1_effective_add_conf_min': 0,
    }

    try:
        # 1A. Entry-mode specific streak
        streak = _query_mode_loss_streak(cur, entry_mode)
        streak_penalty_3 = float(cfg.get('adaptive_l1_streak_penalty_3', 0.70))
        # P1-1: per-mode cooldown (fallback to global cooldown_5_sec)
        mode_cooldown_key = f'adaptive_l1_cooldown_5_sec_{entry_mode}'
        cooldown_5_sec = int(cfg.get(mode_cooldown_key,
                                     cfg.get('adaptive_l1_cooldown_5_sec', 7200)))

        if streak >= 5:
            # 5+ streak: cooldown
            now = time.time()
            cooldown_key = entry_mode
            cooldown_until = float(_state['mode_cooldowns'].get(cooldown_key, 0))
            if cooldown_until == 0:
                # Start cooldown
                _state['mode_cooldowns'][cooldown_key] = now + cooldown_5_sec
                cooldown_until = now + cooldown_5_sec

            if now < cooldown_until:
                result['l1_cooldown_active'] = True
                result['l1_cooldown_remaining'] = int(cooldown_until - now)
                result['l1_penalty'] = streak_penalty_3
            else:
                # Cooldown expired, clear
                _state['mode_cooldowns'].pop(cooldown_key, None)
        elif streak >= 3:
            result['l1_penalty'] = streak_penalty_3
        else:
            # Clear any expired cooldown
            _state['mode_cooldowns'].pop(entry_mode, None)

        # 1B. Global WR check
        wr_trades = int(cfg.get('adaptive_l1_global_wr_trades', 20))
        wr_low = float(cfg.get('adaptive_l1_global_wr_low', 0.35))
        wr_recovery = float(cfg.get('adaptive_l1_global_wr_recovery', 0.40))
        threshold_add = int(cfg.get('adaptive_l1_global_wr_threshold_add', 10))
        add_conf_min = int(cfg.get('adaptive_l1_global_wr_add_conf_min', 60))

        wr, total = _query_global_wr(cur, wr_trades)
        result['l1_global_wr'] = wr

        # 1C. Hysteresis — D2-2: penalty 완화에 N회 연속 개선 요구
        relax_consecutive = int(cfg.get('adaptive_penalty_relax_consecutive', 3))
        if total >= wr_trades:
            if wr < wr_low:
                _state['global_wr_penalty_active'] = True
                _state['wr_recovery_consecutive'] = 0  # 리셋
            elif wr >= wr_recovery:
                # D2-2: Consecutive improvement count — strict > (stagnation = no progress)
                _last_wr = _state.get('last_wr_sample', -1)  # default -1 so first sample counts
                if wr > _last_wr:
                    _state['wr_recovery_consecutive'] = _state.get('wr_recovery_consecutive', 0) + 1
                else:
                    _state['wr_recovery_consecutive'] = 0  # stagnation or worse = reset
                if _state.get('wr_recovery_consecutive', 0) >= relax_consecutive:
                    _state['global_wr_penalty_active'] = False
                    _state['wr_recovery_consecutive'] = 0
                    _log(f'[L1] WR penalty RELAXED after {relax_consecutive} consecutive improvements')
            # else: wr_low~wr_recovery → hold current state
            _state['last_wr_sample'] = wr

        if _state['global_wr_penalty_active']:
            result['l1_global_wr_block'] = True
            result['l1_effective_threshold_add'] = threshold_add
            result['l1_effective_add_conf_min'] = add_conf_min

        # 1D. Anti-Paralysis Reset
        _check_anti_paralysis(cur, cfg, result)

    except Exception as e:
        _log(f'L1 FAIL-OPEN: {e}')

    return result


def _check_anti_paralysis(cur, cfg, result):
    """Check and apply anti-paralysis reset if needed."""
    try:
        hours_1 = float(cfg.get('adaptive_anti_paralysis_hours_1', 24))
        hours_2 = float(cfg.get('adaptive_anti_paralysis_hours_2', 36))

        trade_switch_on = _check_trade_switch_on(cur)
        from strategy_v3 import compute_market_health
        # We don't have features here, so check DB for health
        health_ok = True  # assume OK unless we can determine otherwise

        if not trade_switch_on:
            return  # Don't count time when switch is OFF

        last_trade_ts = _get_last_trade_ts(cur)
        _state['last_trade_ts'] = last_trade_ts

        if last_trade_ts <= 0:
            return  # No trades ever → skip

        now = time.time()
        no_trade_duration = now - last_trade_ts

        # Subtract trade_switch OFF time
        off_seconds = _get_trade_switch_off_seconds(cur)
        effective_no_trade = no_trade_duration - off_seconds

        # D2-2: 24h zero-trade partial reset (one-shot, not every cycle)
        zero_trade_reset = cfg.get('adaptive_24h_zero_trade_partial_reset', True)
        if zero_trade_reset and effective_no_trade >= hours_1 * 3600 and health_ok:
            if not _state.get('_24h_partial_reset_done', False):
                # One-shot: ease penalty and reset hysteresis counter once
                if result.get('l1_penalty', 1.0) < 0.85:
                    result['l1_penalty'] = 0.85
                    _log('[L1] Anti-Paralysis: 24h zero-trade penalty eased to 0.85')
                _state['wr_recovery_consecutive'] = 0
                _state['_24h_partial_reset_done'] = True

        if effective_no_trade >= hours_2 * 3600 and health_ok:
            # Full reset
            if _state['anti_paralysis_stage'] < 2:
                _state['anti_paralysis_stage'] = 2
                _state['anti_paralysis_reset_ts'] = now
                _log('[L1] Anti-Paralysis: FULL RESET (36h no trade)')
            # Clear all penalties
            _state['mode_cooldowns'] = {}
            _state['global_wr_penalty_active'] = False
            _state['mode_wr_penalty'] = {}
            _state['wr_recovery_consecutive'] = 0
            result['l1_penalty'] = 1.0
            result['l1_cooldown_active'] = False
            result['l1_global_wr_block'] = False
            result['l1_effective_threshold_add'] = 0
            result['l1_effective_add_conf_min'] = 0

        elif effective_no_trade >= hours_1 * 3600 and health_ok:
            # Partial reset — exploratory entry with min size + tight stop
            if _state['anti_paralysis_stage'] < 1:
                _state['anti_paralysis_stage'] = 1
                _state['anti_paralysis_reset_ts'] = now
                _log('[L1] Anti-Paralysis: PARTIAL RESET (24h no trade) '
                     '— exploratory: min_size + tight stop')
            # Ease penalties
            _state['mode_cooldowns'] = {}
            result['l1_cooldown_active'] = False
            if result['l1_penalty'] < 0.85:
                result['l1_penalty'] = 0.85
            if result['l1_global_wr_block']:
                result['l1_effective_threshold_add'] = 5  # halved from 10
            # P1-1: Exploratory entry conditions (min size + tight stop)
            result['l1_anti_paralysis_mode'] = True
            result['l1_exploratory_slice_mult'] = 0.5  # half size
            result['l1_exploratory_sl_tighten'] = 0.7  # 30% tighter SL
        else:
            # Reset anti-paralysis counter if trade happened
            if _state['anti_paralysis_stage'] > 0 and last_trade_ts > _state['anti_paralysis_reset_ts']:
                _state['anti_paralysis_stage'] = 0
                _state['_24h_partial_reset_done'] = False  # reset one-shot flag
    except Exception as e:
        _log(f'Anti-paralysis check FAIL-OPEN: {e}')


# ══════════════════════════════════════════════════════════════
# LAYER 2: MeanReversion PROTECTION
# ══════════════════════════════════════════════════════════════

def compute_layer2(entry_mode, direction, regime_class, features, regime_ctx, cfg):
    """L2: MeanReversion protection conditions.

    Returns dict:
        l2_meanrev_blocked: bool
        l2_block_reason: str
    """
    result = {
        'l2_meanrev_blocked': False,
        'l2_block_reason': '',
    }

    try:
        if not features:
            features = {}
        if not regime_ctx:
            regime_ctx = {}

        from strategy_v3 import safe_float

        range_position = features.get('range_position')
        rp_val = safe_float(range_position) if range_position is not None else None

        # 2C. range_pos > 1.0 → MeanRev block (LONG/SHORT)
        if entry_mode == 'MeanRev' and rp_val is not None and rp_val > 1.0:
            result['l2_meanrev_blocked'] = True
            result['l2_block_reason'] = f'range_pos={rp_val:.2f} > 1.0 → MeanRev blocked, BREAKOUT/TREND priority'
            return result

        # 2A. MeanRev SHORT conditions
        if entry_mode == 'MeanRev' and direction == 'SHORT':
            reasons = []

            # Condition 1: regime_class == STATIC_RANGE
            if regime_class != 'STATIC_RANGE':
                reasons.append(f'regime={regime_class} != STATIC_RANGE')

            # Condition 2: price_vs_va == INSIDE
            price_vs_va = regime_ctx.get('price_vs_va')
            if price_vs_va != 'INSIDE':
                reasons.append(f'price_vs_va={price_vs_va} != INSIDE')

            # Condition 3: range_position >= 0.85
            rp_min = float(cfg.get('adaptive_l2_range_pos_short_min', 0.85))
            if rp_val is None or rp_val < rp_min:
                reasons.append(f'range_pos={rp_val} < {rp_min}')

            # Condition 4: breakout_confirmed == False (fail-closed on None)
            bc = regime_ctx.get('breakout_confirmed')
            if bc is None:
                reasons.append('breakout_confirmed=None (fail-closed)')
            elif isinstance(bc, str):
                if bc.lower() in ('true', '1', 'yes'):
                    reasons.append('breakout_confirmed=True')
            elif bc:
                reasons.append('breakout_confirmed=True')

            # Condition 5: volume_z <= 0 (fail-closed on None)
            volume_z = features.get('volume_z')
            if volume_z is None:
                reasons.append('volume_z=None (fail-closed)')
            elif safe_float(volume_z) > 0:
                reasons.append(f'volume_z={safe_float(volume_z):.2f} > 0')

            # Condition 6: flow_bias <= 0 (fail-closed on None)
            flow_bias = regime_ctx.get('flow_bias')
            if flow_bias is None:
                reasons.append('flow_bias=None (fail-closed)')
            elif safe_float(flow_bias) > 0:
                reasons.append(f'flow_bias={safe_float(flow_bias):.2f} > 0')

            if reasons:
                result['l2_meanrev_blocked'] = True
                result['l2_block_reason'] = 'MeanRev SHORT fail: ' + '; '.join(reasons)
                return result

        # 2B. Hard-block: counter-trend acceleration
        if entry_mode == 'MeanRev' and direction == 'SHORT':
            from strategy_v3 import safe_float as sf
            drift_direction = features.get('drift_direction', 'NONE')
            flow_bias = sf(regime_ctx.get('flow_bias'))
            impulse = sf(features.get('impulse'))
            impulse_threshold = float(cfg.get('adaptive_l2_impulse_hard_block', 1.5))

            if drift_direction == 'NONE' and flow_bias > 0 and impulse > impulse_threshold:
                result['l2_meanrev_blocked'] = True
                result['l2_block_reason'] = (
                    f'hard-block: drift=NONE + flow_bias={flow_bias:.2f}>0 '
                    f'+ impulse={impulse:.2f}>{impulse_threshold}')
                return result

        # P1-2: Strict SHORT protection (ff_l2_strict_short)
        try:
            import feature_flags
            if (feature_flags.is_enabled('ff_l2_strict_short')
                    and entry_mode == 'MeanRev' and direction == 'SHORT'):
                from strategy_v3 import safe_float as sf2
                impulse2 = sf2(features.get('impulse'))
                drift_dir = features.get('drift_direction', 'NONE')

                # Condition: impulse <= 0 → MeanRev SHORT blocked
                if impulse2 <= 0:
                    result['l2_meanrev_blocked'] = True
                    result['l2_block_reason'] = (
                        f'strict_short: impulse={impulse2:.2f}<=0 → '
                        f'MeanRev SHORT blocked (no momentum)')
                    return result

                # Condition: drift=NONE + impulse > 1.5 → MeanRev SHORT blocked
                if drift_dir == 'NONE' and impulse2 > 1.5:
                    result['l2_meanrev_blocked'] = True
                    result['l2_block_reason'] = (
                        f'strict_short: drift=NONE + impulse={impulse2:.2f}>1.5 → '
                        f'MeanRev SHORT blocked')
                    return result
        except Exception as e2:
            _log(f'L2 strict_short FAIL-OPEN: {e2}')

    except Exception as e:
        _log(f'L2 FAIL-OPEN: {e}')

    return result


# ══════════════════════════════════════════════════════════════
# LAYER 3: ADD LOGIC RESTRICTION
# ══════════════════════════════════════════════════════════════

def compute_layer3(cur, pos_side, features, cfg, symbol='BTC/USDT:USDT'):
    """L3: uPnL-based ADD restriction.

    Returns dict:
        l3_add_blocked: bool
        l3_add_reason: str
    """
    result = {
        'l3_add_blocked': False,
        'l3_add_reason': '',
    }

    try:
        # Get current uPnL
        cur.execute("""
            SELECT avg_entry_price, peak_upnl_pct FROM position_state
            WHERE symbol = %s;
        """, (symbol,))
        ps_row = cur.fetchone()
        if not ps_row or not ps_row[0]:
            return result  # No position data → FAIL-OPEN

        avg_entry = float(ps_row[0])
        peak_upnl_pct = float(ps_row[1] or 0)

        # Get current price
        cur.execute("SELECT mark_price FROM market_data_cache WHERE symbol = %s;", (symbol,))
        price_row = cur.fetchone()
        if not price_row or not price_row[0]:
            return result  # No price → FAIL-OPEN

        cur_price = float(price_row[0])
        if avg_entry <= 0 or cur_price <= 0:
            return result

        # Calculate uPnL %
        if pos_side == 'long':
            upnl_pct = (cur_price - avg_entry) / avg_entry * 100
        else:
            upnl_pct = (avg_entry - cur_price) / avg_entry * 100

        # 3A. Loss → ADD block
        if upnl_pct < 0:
            result['l3_add_blocked'] = True
            result['l3_add_reason'] = f'uPnL={upnl_pct:.2f}% < 0 → ADD blocked'
            return result

        # 3B. Profit + peak check
        peak_threshold = float(cfg.get('adaptive_l3_peak_upnl_threshold', 0.4))
        retest_confirmed = bool(features.get('retest_confirmed', False))

        if upnl_pct > 0 and (peak_upnl_pct >= peak_threshold or retest_confirmed):
            # ADD allowed
            return result

        # Not enough profit / no retest
        if upnl_pct > 0:
            result['l3_add_blocked'] = True
            result['l3_add_reason'] = (
                f'uPnL={upnl_pct:.2f}% > 0 but peak={peak_upnl_pct:.2f}% < {peak_threshold}% '
                f'and retest={retest_confirmed}')

    except Exception as e:
        _log(f'L3 FAIL-OPEN: {e}')

    return result


# ══════════════════════════════════════════════════════════════
# LAYER 4: health=WARN RISK CONTROL
# ══════════════════════════════════════════════════════════════

def compute_layer4(health, cfg):
    """L4: health=WARN risk control.

    Returns dict:
        l4_entry_blocked: bool
        l4_add_blocked: bool
        l4_time_stop_mult: float (1.0 = normal, 0.5 = tightened)
        l4_trailing_sensitive: bool
        l4_warn_duration: float (seconds since WARN started)
    """
    result = {
        'l4_entry_blocked': False,
        'l4_add_blocked': False,
        'l4_time_stop_mult': 1.0,
        'l4_trailing_sensitive': False,
        'l4_warn_duration': 0,
    }

    try:
        now = time.time()
        warn_tighten_sec = int(cfg.get('adaptive_l4_warn_tighten_sec', 120))
        time_stop_mult = float(cfg.get('adaptive_l4_time_stop_mult', 0.5))

        if health == 'WARN':
            # 4A. Immediate: block entry + ADD
            result['l4_entry_blocked'] = True
            result['l4_add_blocked'] = True

            # Track WARN duration
            if _state['warn_since_ts'] == 0:
                _state['warn_since_ts'] = now

            warn_duration = now - _state['warn_since_ts']
            result['l4_warn_duration'] = warn_duration

            # 4B. 2+ min WARN → tighten time_stop and trailing
            if warn_duration >= warn_tighten_sec:
                result['l4_time_stop_mult'] = time_stop_mult
                result['l4_trailing_sensitive'] = True
        else:
            # OK → reset
            if _state['warn_since_ts'] != 0:
                _state['warn_since_ts'] = 0

    except Exception as e:
        _log(f'L4 FAIL-OPEN: {e}')

    return result


# ══════════════════════════════════════════════════════════════
# LAYER 5: MODE PERFORMANCE TRACKING
# ══════════════════════════════════════════════════════════════

def _query_mode_wr(cur, entry_mode, n_trades, symbol='BTC/USDT:USDT'):
    """Compute per-mode win rate from recent N trades. Returns (wr, total)."""
    try:
        cur.execute("""
            SELECT realized_pnl FROM execution_log
            WHERE symbol = %s
              AND order_type IN ('CLOSE', 'FULL_CLOSE', 'REDUCE', 'REVERSE_CLOSE')
              AND status = 'FILLED'
              AND (entry_mode = %s OR (entry_mode IS NULL AND regime_tag = %s))
            ORDER BY last_fill_at DESC NULLS LAST
            LIMIT %s
        """, (symbol, entry_mode,
              _entry_mode_to_regime_tag(entry_mode), n_trades))
        rows = cur.fetchall()
        total = len(rows)
        if total == 0:
            return (0.5, 0)
        wins = sum(1 for r in rows if r[0] is not None and float(r[0]) > 0)
        return (wins / total, total)
    except Exception:
        return (0.5, 0)


def _entry_mode_to_regime_tag(entry_mode):
    """Reverse mapping for backward compat queries."""
    mapping = {
        'MeanRev': 'STATIC_RANGE',
        'DriftFollow': 'DRIFT_UP',  # also DRIFT_DOWN but we query by entry_mode primarily
        'BreakoutTrend': 'BREAKOUT',
    }
    return mapping.get(entry_mode, '')


def compute_layer5(cur, entry_mode, cfg):
    """L5: Mode performance tracking.

    Returns dict:
        l5_penalty: float (1.0 = no penalty, 0.75 = penalized)
        l5_mode_wr: float
        l5_mode_total: int
    """
    result = {
        'l5_penalty': 1.0,
        'l5_mode_wr': 0.5,
        'l5_mode_total': 0,
    }

    try:
        n_trades = int(cfg.get('adaptive_l5_trades', 50))
        min_sample = int(cfg.get('adaptive_l5_min_sample', 10))
        wr_low = float(cfg.get('adaptive_l5_wr_low', 0.35))
        wr_recovery = float(cfg.get('adaptive_l5_wr_recovery', 0.40))
        penalty_val = float(cfg.get('adaptive_l5_penalty', 0.75))

        wr, total = _query_mode_wr(cur, entry_mode, n_trades)
        result['l5_mode_wr'] = wr
        result['l5_mode_total'] = total

        if total < min_sample:
            return result  # Not enough data

        # Hysteresis
        mode_key = entry_mode
        was_penalized = _state['mode_wr_penalty'].get(mode_key, False)

        if wr < wr_low:
            _state['mode_wr_penalty'][mode_key] = True
        elif wr >= wr_recovery:
            _state['mode_wr_penalty'][mode_key] = False

        if _state['mode_wr_penalty'].get(mode_key, False):
            result['l5_penalty'] = penalty_val

    except Exception as e:
        _log(f'L5 FAIL-OPEN: {e}')

    return result


# ══════════════════════════════════════════════════════════════
# ORCHESTRATOR
# ══════════════════════════════════════════════════════════════

def apply_adaptive_layers(cur, entry_mode, direction, regime_class,
                          features, regime_ctx, health, cfg=None):
    """Main orchestrator: compute all layers and return combined result.

    Args:
        cur: DB cursor
        entry_mode: str (MeanRev, DriftFollow, BreakoutTrend)
        direction: str (LONG, SHORT)
        regime_class: str (STATIC_RANGE, DRIFT_UP, DRIFT_DOWN, BREAKOUT)
        features: dict from build_feature_snapshot()
        regime_ctx: dict from regime_reader.get_current_regime()
        health: str (OK, WARN)
        cfg: config dict (optional, auto-loaded if None)

    Returns:
        dict with combined results from all layers:
            combined_penalty: float (applied to total_score via multiplication)
            l1_cooldown_active: bool
            l1_global_wr_block: bool
            l1_effective_threshold_add: int
            l1_effective_add_conf_min: int
            l2_meanrev_blocked: bool
            l2_block_reason: str
            l4_entry_blocked: bool
            l4_add_blocked: bool
            l4_time_stop_mult: float
            l4_trailing_sensitive: bool
            l5_penalty: float
            dryrun: bool
            debug: dict (human-readable debug info)
    """
    if cfg is None:
        cfg = _get_cfg()

    dryrun = _is_dryrun()

    # Default: no penalties, no blocks
    combined = {
        'combined_penalty': 1.0,
        'l1_cooldown_active': False,
        'l1_cooldown_remaining': 0,
        'l1_global_wr': 0.5,
        'l1_global_wr_block': False,
        'l1_effective_threshold_add': 0,
        'l1_effective_add_conf_min': 0,
        'l2_meanrev_blocked': False,
        'l2_block_reason': '',
        'l4_entry_blocked': False,
        'l4_add_blocked': False,
        'l4_time_stop_mult': 1.0,
        'l4_trailing_sensitive': False,
        'l5_penalty': 1.0,
        'dryrun': dryrun,
        'debug': {},
    }

    try:
        # Sync state from DB
        _sync_state(cur)

        # L1: Loss-streak
        l1 = compute_layer1(cur, entry_mode, cfg)

        # L4: WARN control
        l4 = compute_layer4(health, cfg)

        # L5: Mode WR
        l5 = compute_layer5(cur, entry_mode, cfg)

        # Combined penalty with floor
        penalty_floor = float(cfg.get('adaptive_combined_penalty_floor', 0.55))
        raw_combined = l1['l1_penalty'] * l5['l5_penalty']
        combined_penalty = max(penalty_floor, raw_combined)

        # Populate result
        combined['combined_penalty'] = combined_penalty
        combined['l1_cooldown_active'] = l1['l1_cooldown_active']
        combined['l1_cooldown_remaining'] = l1['l1_cooldown_remaining']
        combined['l1_global_wr'] = l1['l1_global_wr']
        combined['l1_global_wr_block'] = l1['l1_global_wr_block']
        combined['l1_effective_threshold_add'] = l1['l1_effective_threshold_add']
        combined['l1_effective_add_conf_min'] = l1['l1_effective_add_conf_min']
        combined['l4_entry_blocked'] = l4['l4_entry_blocked']
        combined['l4_add_blocked'] = l4['l4_add_blocked']
        combined['l4_time_stop_mult'] = l4['l4_time_stop_mult']
        combined['l4_trailing_sensitive'] = l4['l4_trailing_sensitive']
        combined['l5_penalty'] = l5['l5_penalty']

        # Debug info
        combined['debug'] = _build_debug(l1, l4, l5, combined_penalty, dryrun, entry_mode)

        # Persist state
        _persist_state(cur)

        # Log
        _log_adaptive(combined, entry_mode, direction, dryrun)

    except Exception as e:
        _log(f'apply_adaptive_layers FAIL-OPEN: {e}')
        traceback.print_exc()

    # Dryrun: return computed values but callers should check dryrun flag
    # to decide whether to enforce
    return combined


def apply_adaptive_add_gate(cur, pos_side, features, health, cfg=None):
    """ADD-specific gate: L3 + L4 combined check.

    Returns dict:
        l3_add_blocked: bool
        l3_add_reason: str
        l4_add_blocked: bool
    """
    if cfg is None:
        cfg = _get_cfg()

    result = {
        'l3_add_blocked': False,
        'l3_add_reason': '',
        'l4_add_blocked': False,
    }

    try:
        l3 = compute_layer3(cur, pos_side, features or {}, cfg)
        l4 = compute_layer4(health, cfg)

        result['l3_add_blocked'] = l3['l3_add_blocked']
        result['l3_add_reason'] = l3['l3_add_reason']
        result['l4_add_blocked'] = l4['l4_add_blocked']
    except Exception as e:
        _log(f'apply_adaptive_add_gate FAIL-OPEN: {e}')

    return result


# ══════════════════════════════════════════════════════════════
# DEBUG / TELEGRAM OUTPUT
# ══════════════════════════════════════════════════════════════

def _build_debug(l1, l4, l5, combined_penalty, dryrun, entry_mode):
    """Build human-readable debug dict for Telegram/bundle."""
    return {
        'global_wr': f'{l1["l1_global_wr"]:.1%}',
        'global_wr_penalty_active': _state['global_wr_penalty_active'],
        'mode_wr': l5['l5_mode_wr'],
        'mode_wr_total': l5['l5_mode_total'],
        'entry_mode': entry_mode,
        'l1_penalty': l1['l1_penalty'],
        'l1_cooldown_active': l1['l1_cooldown_active'],
        'l1_cooldown_remaining': l1['l1_cooldown_remaining'],
        'l4_entry_blocked': l4['l4_entry_blocked'],
        'l4_warn_duration': l4['l4_warn_duration'],
        'l5_penalty': l5['l5_penalty'],
        'combined_penalty': combined_penalty,
        'anti_paralysis_stage': _state['anti_paralysis_stage'],
        'last_trade_ts': _state['last_trade_ts'],
        'dryrun': dryrun,
    }


def _log_adaptive(combined, entry_mode, direction, dryrun):
    """Log adaptive layer results."""
    prefix = '[ADAPTIVE_DRYRUN]' if dryrun else '[ADAPTIVE]'
    penalty = combined['combined_penalty']
    blocks = []
    if combined['l1_cooldown_active']:
        blocks.append(f'L1_cooldown({combined["l1_cooldown_remaining"]}s)')
    if combined['l1_global_wr_block']:
        blocks.append(f'L1_wr({combined["l1_global_wr"]:.1%})')
    if combined['l4_entry_blocked']:
        blocks.append('L4_warn')

    block_str = ', '.join(blocks) if blocks else 'none'
    _log(f'{prefix} mode={entry_mode} dir={direction} penalty={penalty:.2f} '
         f'blocks=[{block_str}]')


def get_debug_state():
    """Return formatted debug string for Telegram display."""
    try:
        lines = [
            '── Adaptive Layers ──',
            f'global WR penalty_active: {"YES" if _state["global_wr_penalty_active"] else "NO"}',
            f'mode WR penalties: {_state.get("mode_wr_penalty", {})}',
            f'cooldowns: {_state.get("mode_cooldowns", {})}',
            f'anti-paralysis stage: {_state["anti_paralysis_stage"]}',
            f'last_trade: {_state["last_trade_ts"]:.0f}',
            f'dryrun: {"ON" if _is_dryrun() else "OFF"}',
            f'enabled: {"ON" if _is_adaptive_enabled() else "OFF"}',
        ]
        return '\n'.join(lines)
    except Exception as e:
        return f'── Adaptive Layers ── ERROR: {e}'
