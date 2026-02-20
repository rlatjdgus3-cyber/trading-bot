"""
score_engine.py — Unified Score Engine (central orchestrator).

Computes a 4-axis weighted total score:
  TOTAL = 0.75*TECH + 0.10*POSITION + 0.10*REGIME + 0.05*NEWS_EVENT

REGIME: Based on price_event_stats (pure price-action volatility spike patterns).
NEWS_EVENT: Supplementary only (weight reduced due to limited historical data).

GUARD: NEWS_EVENT cannot trigger trades alone.
       If TECH + POSITION are both neutral (abs < 10), NEWS_EVENT is ignored.

Each axis: -100 (short) to +100 (long).
Output: total_score, abs_score (0-100), stage (1-7), dynamic_stop_loss_pct.
Legacy compatibility: long_score/short_score (0-100).
"""
import os
import sys
import json
sys.path.insert(0, '/root/trading-bot/app')
LOG_PREFIX = '[score_engine]'
SYMBOL = 'BTC/USDT:USDT'
DEFAULT_WEIGHTS = {
    'tech_w': 0.75,
    'position_w': 0.10,
    'regime_w': 0.10,
    'news_event_w': 0.05}
STAGE_THRESHOLDS = [
    (75, 7),
    (65, 6),
    (55, 5),
    (45, 4),
    (35, 3),
    (20, 2),
    (10, 1)]


def _signal_stage_label(abs_score):
    """Map absolute score to signal stage label (stg0-stg3)."""
    if abs_score >= 65:
        return 'stg3'  # strong
    if abs_score >= 45:
        return 'stg2'  # medium
    if abs_score >= 10:
        return 'stg1'  # weak
    return 'stg0'  # no signal


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _db_conn():
    from db_config import get_conn
    return get_conn()


def _load_weights(cur=None):
    '''Load axis weights from score_weights table, with openclaw_policies override.'''
    weights = dict(DEFAULT_WEIGHTS)
    try:
        cur.execute("""
            SELECT tech_w, position_w, regime_w, news_event_w
            FROM score_weights ORDER BY id DESC LIMIT 1;
        """)
        row = cur.fetchone()
        if row:
            weights = {
                'tech_w': float(row[0]),
                'position_w': float(row[1]),
                'regime_w': float(row[2]),
                'news_event_w': float(row[3])}
    except Exception:
        pass
    # openclaw_policies override
    try:
        cur.execute("""
            SELECT value FROM openclaw_policies WHERE key = 'score_weight_override';
        """)
        row = cur.fetchone()
        if row and row[0]:
            override = row[0] if isinstance(row[0], dict) else json.loads(row[0])
            for k in ('tech_w', 'position_w', 'regime_w', 'news_event_w'):
                if k in override:
                    weights[k] = float(override[k])
    except Exception:
        pass
    # Emergency news weight bump (TTL-based)
    try:
        cur.execute("SELECT value FROM openclaw_policies WHERE key = 'news_emergency_bump';")
        row = cur.fetchone()
        if row and row[0]:
            from datetime import datetime, timezone
            bump = row[0] if isinstance(row[0], dict) else json.loads(row[0])
            if bump.get('active'):
                expires_at = datetime.fromisoformat(bump['expires_at'])
                now = datetime.now(timezone.utc)
                if now < expires_at:
                    weights['news_event_w'] = float(bump.get('weight', 0.15))
                else:
                    cur.execute("DELETE FROM openclaw_policies WHERE key = 'news_emergency_bump';")
                    try:
                        cur.connection.commit()
                    except Exception:
                        pass
    except Exception:
        pass
    return weights


def score_to_stage(abs_score=None):
    '''Convert absolute score (0-100) to stage (1-7).'''
    for threshold, stage in STAGE_THRESHOLDS:
        if abs_score >= threshold:
            return stage
    return 1


def compute_dynamic_stop_loss(regime_score=None, macro_score=None, base=None):
    '''Compute dynamic stop-loss percentage.

    Higher risk (negative regime/macro) -> wider stop.
    Lower risk (positive regime/macro) -> tighter stop.

    Returns: float between 1.2% and 3.0%
    '''
    r = regime_score if regime_score is not None else 0
    m = macro_score if macro_score is not None else 0
    b = base if base is not None else 2.0
    risk_input = r * 0.6 + m * 0.4
    adjustment = (-risk_input / 100) * 1
    dynamic_sl = b + adjustment
    return max(1.2, min(3, round(dynamic_sl, 2)))


def _total_to_legacy(total_score=None):
    '''Convert total score (-100~+100) to legacy long_score/short_score (0-100).

    Mapping:
      total = +100 -> long=100, short=0
      total = 0    -> long=50,  short=50
      total = -100 -> long=0,   short=100
    '''
    long_score = int(round(50 + total_score / 2))
    long_score = max(0, min(100, long_score))
    short_score = 100 - long_score
    return (long_score, short_score)


def _compute_regime_from_events(cur):
    '''Compute regime score from price events (no news dependency).

    Uses events table volatility spikes + price_event_stats materialized view.
    1. Query recent 48h events with btc_move_4h data
    2. Time-weighted directional bias (recent events weighted more)
    3. Historical confirmation from price_event_stats (continuation_rate)
    4. Combine bias × confirmation → regime score

    Returns: dict with score (-100 to +100), components, details
    '''
    try:
        # Step 1: Recent events (48h)
        cur.execute("""
            SELECT direction, vol_zscore, btc_move_4h,
                   EXTRACT(EPOCH FROM (now() - start_ts)) / 3600 AS age_hours
            FROM events
            WHERE start_ts >= now() - interval '48 hours'
              AND btc_move_4h IS NOT NULL
            ORDER BY start_ts DESC LIMIT 20;
        """)
        recent = cur.fetchall()
        if not recent:
            return {'score': 0, 'source': 'price_events',
                    'components': {'recent_bias': 0, 'hist_confirm': 0.5},
                    'details': {'event_count': 0}}

        # Step 2: Time-weighted directional bias
        up_weight = 0.0
        down_weight = 0.0
        for direction, zscore, move_4h, age_h in recent:
            time_w = 1.0 if age_h < 6 else (0.5 if age_h < 24 else 0.2)
            mag = min(abs(float(zscore)) / 3.0, 2.0)
            if direction and direction.upper() == 'UP':
                up_weight += time_w * mag
            else:
                down_weight += time_w * mag

        total_w = up_weight + down_weight
        if total_w == 0:
            return {'score': 0, 'source': 'price_events',
                    'components': {'recent_bias': 0, 'hist_confirm': 0.5},
                    'details': {'event_count': len(recent)}}

        recent_bias = (up_weight - down_weight) / total_w  # -1.0 ~ +1.0

        # Step 3: Historical confirmation from price_event_stats
        # events table stores direction as lowercase ('up'/'down')
        dominant_dir = 'up' if recent_bias >= 0 else 'down'
        latest_zscore = abs(float(recent[0][1]))
        zscore_band = 'low' if latest_zscore < 4 else ('mid' if latest_zscore < 6 else 'high')

        hist_confirm = 0.5  # neutral default
        hist_count = 0
        hist_avg_move = 0
        try:
            cur.execute("""
                SELECT continuation_rate, event_count, avg_move_4h
                FROM price_event_stats
                WHERE direction = %s AND zscore_band = %s;
            """, (dominant_dir, zscore_band))
            row = cur.fetchone()
            if row and row[1] and int(row[1]) >= 10:
                hist_confirm = float(row[0])
                hist_count = int(row[1])
                hist_avg_move = float(row[2]) if row[2] else 0
        except Exception:
            pass  # MV may not exist yet

        # Step 4: Combine: bias × (0.5 + confirmation) × 100
        score = int(round(recent_bias * (0.5 + hist_confirm) * 100))
        score = max(-100, min(100, score))

        return {
            'score': score,
            'source': 'price_events',
            'components': {
                'recent_bias': round(recent_bias, 3),
                'hist_confirm': round(hist_confirm, 3),
            },
            'details': {
                'event_count': len(recent),
                'up_weight': round(up_weight, 2),
                'down_weight': round(down_weight, 2),
                'dominant_dir': dominant_dir,
                'zscore_band': zscore_band,
                'hist_samples': hist_count,
                'hist_avg_move_4h': round(hist_avg_move, 4),
            },
        }
    except Exception as e:
        _log(f'_compute_regime_from_events error: {e}')
        return {'score': 0, 'source': 'price_events',
                'components': {'recent_bias': 0, 'hist_confirm': 0.5},
                'details': {'error': str(e)}}


def compute_total(cur=None, exchange=None):
    '''Compute unified 4-axis total score.

    Formula: TOTAL = 0.75*TECH + 0.10*POSITION + 0.10*REGIME + 0.05*NEWS_EVENT

    GUARD: NEWS_EVENT is supplementary only.
           If both TECH and POSITION are neutral (abs < 10),
           NEWS_EVENT contribution is zeroed to prevent news-only trades.

    Args:
        cur: Database cursor (creates own connection if None)
        exchange: ccxt exchange instance (unused, kept for compat)

    Returns:
        {
            "total_score": float (-100 to +100),
            "abs_score": float (0 to 100),
            "dominant_side": "LONG" | "SHORT",
            "stage": int (1-7),
            "dynamic_stop_loss_pct": float,

            # Axis scores
            "tech_score": float,
            "position_score": float,
            "regime_score": float,
            "news_event_score": float,

            # Legacy compatibility
            "macro_score": float,
            "liquidity_score": float,
            "long_score": int (0-100),
            "short_score": int (0-100),
            "confidence": int,

            # Details
            "weights": dict,
            "axis_details": dict,
            "price": float | None,
            "news_event_guarded": bool,
        }
    '''
    own_conn = False
    conn = None
    try:
        if cur is None:
            conn = _db_conn()
            conn.autocommit = True
            cur = conn.cursor()
            own_conn = True

        weights = _load_weights(cur)

        # Import and compute each axis
        import tech_scorer
        import macro_scorer
        import position_scorer
        import news_event_scorer

        tech_result = tech_scorer.compute(cur)
        tech_score = tech_result.get('score', 0)

        macro_result = macro_scorer.compute(cur)
        macro_score = macro_result.get('score', 0)

        # Regime score - from price events (no news dependency)
        regime_result = _compute_regime_from_events(cur)
        regime_score = regime_result.get('score', 0)
        regime_detail = regime_result

        pos_result = position_scorer.compute(cur, tech_score)
        position_score = pos_result.get('score', 0)

        # NEWS_EVENT score (supplementary)
        news_evt_result = news_event_scorer.compute(cur)
        news_event_score = news_evt_result.get('score', 0)

        # ── BTC-QQQ regime correlation → dynamic NEWS_EVENT weight ──
        btc_qqq_regime = None
        try:
            import regime_correlation
            btc_qqq_regime = regime_correlation.get_current_regime(cur)
            base_news_w = weights['news_event_w']
            if btc_qqq_regime == 'COUPLED_RISK':
                weights['news_event_w'] = min(weights['news_event_w'], 0.03)
            elif btc_qqq_regime == 'DECOUPLED':
                weights['news_event_w'] = 0.02
            # Redistribute delta across other 3 axes proportionally
            delta = weights['news_event_w'] - base_news_w
            if abs(delta) > 0.001:
                other_keys = ['tech_w', 'position_w', 'regime_w']
                other_sum = sum(weights[k] for k in other_keys)
                if other_sum > 0:
                    for k in other_keys:
                        weights[k] -= delta * (weights[k] / other_sum)
        except Exception as e:
            _log(f'regime_correlation skip: {e}')

        # ── BREAKOUT regime: dynamically increase news_event_w ──
        breakout_news_override = False
        try:
            import regime_reader
            current_regime = regime_reader.get_current_regime(cur)
            if current_regime.get('available') and current_regime.get('regime') == 'BREAKOUT':
                # Increase news weight to 0.10-0.20 based on news strength
                base_news_w = weights['news_event_w']
                news_abs = abs(news_event_score)
                if news_abs >= 60:
                    new_news_w = 0.20
                elif news_abs >= 30:
                    new_news_w = 0.15
                else:
                    new_news_w = 0.10
                delta = new_news_w - base_news_w
                if delta > 0:
                    # Guard: if spread/liquidity bad, don't increase news weight
                    try:
                        from strategy.common.features import compute_spread_ok, compute_liquidity_ok
                        spread_ok = compute_spread_ok(cur)
                        liquidity_ok = compute_liquidity_ok(cur)
                        if not spread_ok or not liquidity_ok:
                            delta = 0  # Don't increase news weight with bad liquidity
                            _log('BREAKOUT news_w override blocked: spread/liquidity not OK')
                    except Exception:
                        pass  # FAIL-OPEN: proceed with override
                    if delta > 0:
                        weights['news_event_w'] = new_news_w
                        # Reduce tech_w proportionally, with floor at 0.30
                        weights['tech_w'] = max(0.30, weights['tech_w'] - delta)
                        breakout_news_override = True
                        _log(f'BREAKOUT news_w override: {base_news_w:.2f} -> {new_news_w:.2f}')
        except Exception as e:
            _log(f'BREAKOUT news override skip: {e}')

        # ── Dynamic news weight boost for high-impact political/macro events ──
        high_impact_news_boost = False
        try:
            news_details = news_evt_result.get('details', {})
            hi_meta = news_details.get('high_impact_meta', {})
            if (hi_meta.get('has_high_impact')
                    and hi_meta.get('top_impact', 0) >= 7
                    and hi_meta.get('top_source_quality', 0) >= 16):
                base_news_w = weights['news_event_w']
                boosted_w = min(0.15, base_news_w * 3)

                # News direction sign for corroboration alignment
                _news_bearish = news_event_score < 0

                # QQQ 30min corroboration: boost only if QQQ move aligns with news direction
                # QQQ drop corroborates bearish news; QQQ rise corroborates bullish news
                try:
                    cur.execute("""
                        WITH latest AS (SELECT price FROM macro_data WHERE source='QQQ' ORDER BY ts DESC LIMIT 1),
                             past AS (SELECT price FROM macro_data WHERE source='QQQ'
                                      AND ts <= now()-interval '30 minutes' ORDER BY ts DESC LIMIT 1)
                        SELECT (SELECT price FROM latest) - (SELECT price FROM past);
                    """)
                    qqq_row = cur.fetchone()
                    if qqq_row and qqq_row[0]:
                        qqq_change = float(qqq_row[0])
                        qqq_aligned = (qqq_change < 0 and _news_bearish) or (qqq_change > 0 and not _news_bearish)
                        if abs(qqq_change) > 1.50 and qqq_aligned:
                            boosted_w = min(0.18, boosted_w + 0.03)
                except Exception:
                    pass

                # VIX spike corroboration: VIX rise corroborates bearish news only
                try:
                    cur.execute("""
                        WITH latest AS (SELECT price FROM macro_data WHERE source='VIX' ORDER BY ts DESC LIMIT 1),
                             past AS (SELECT price FROM macro_data WHERE source='VIX'
                                      AND ts <= now()-interval '30 minutes' ORDER BY ts DESC LIMIT 1)
                        SELECT (SELECT price FROM latest), (SELECT price FROM past);
                    """)
                    vix_row = cur.fetchone()
                    if vix_row and vix_row[0] and vix_row[1] and float(vix_row[1]) > 0:
                        vix_change_pct = (float(vix_row[0]) - float(vix_row[1])) / float(vix_row[1])
                        if vix_change_pct > 0.10 and _news_bearish:
                            boosted_w = min(0.20, boosted_w + 0.02)
                except Exception:
                    pass

                # Final cap: 0.20
                boosted_w = min(0.20, boosted_w)
                delta = boosted_w - base_news_w
                if delta > 0:
                    weights['news_event_w'] = boosted_w
                    # Delta taken from tech_w only, with floor at 0.30
                    weights['tech_w'] = max(0.30, weights['tech_w'] - delta)
                    high_impact_news_boost = True
                    _log(f'HIGH_IMPACT news_w boost: {base_news_w:.2f} -> {boosted_w:.2f} '
                         f'(cat={hi_meta.get("top_category")} impact={hi_meta.get("top_impact")})')
        except Exception as e:
            _log(f'HIGH_IMPACT news boost skip: {e}')

        # GUARD: NEWS_EVENT cannot trigger trades alone.
        # If both TECH and POSITION are neutral (abs < 10), zero out NEWS_EVENT.
        news_event_guarded = False
        if abs(tech_score) < 10 and abs(position_score) < 10:
            news_event_score = 0
            news_event_guarded = True

        # Normalize weights to sum to 1.0 after all adjustments
        _w_sum = sum(weights.values())
        if _w_sum > 0 and abs(_w_sum - 1.0) > 0.001:
            for _wk in weights:
                weights[_wk] /= _w_sum

        # Weighted total (4-axis)
        total = (
            weights['tech_w'] * tech_score +
            weights['position_w'] * position_score +
            weights['regime_w'] * regime_score +
            weights['news_event_w'] * news_event_score
        )
        total = max(-100, min(100, round(total, 1)))

        abs_score = abs(total)
        dominant_side = 'LONG' if total >= 0 else 'SHORT'
        stage = score_to_stage(abs_score)

        # NEWS explicit guard: ensure NEWS alone cannot trigger entry (stage > 0)
        # If TECH and POSITION are both neutral, news_event_score is already zeroed,
        # but as defense-in-depth, explicitly cap stage to 0.
        if news_event_guarded and stage > 0:
            stage = 0

        # Dynamic stop-loss
        sl_base = 2.0
        try:
            cur.execute('SELECT dynamic_sl_base_pct FROM safety_limits ORDER BY id DESC LIMIT 1;')
            sl_row = cur.fetchone()
            if sl_row and sl_row[0]:
                sl_base = float(sl_row[0])
        except Exception:
            pass

        dynamic_sl = compute_dynamic_stop_loss(regime_score, macro_score, sl_base)

        (long_score, short_score) = _total_to_legacy(total)
        confidence = abs(long_score - short_score)

        price = tech_result.get('price')

        result = {
            'total_score': total,
            'abs_score': abs_score,
            'dominant_side': dominant_side,
            'stage': stage,
            'signal_stage': _signal_stage_label(abs_score),
            'dynamic_stop_loss_pct': dynamic_sl,
            'tech_score': tech_score,
            'position_score': position_score,
            'regime_score': regime_score,
            'news_event_score': news_event_score,
            'news_event_guarded': news_event_guarded,
            'breakout_news_override': breakout_news_override,
            'high_impact_news_boost': high_impact_news_boost,
            # Legacy compat
            'macro_score': macro_score,
            'liquidity_score': 0,
            'long_score': long_score,
            'short_score': short_score,
            'confidence': confidence,
            'weights': weights,
            'axis_details': {
                'tech': tech_result,
                'position': pos_result,
                'regime': regime_detail,
                'news_event': news_evt_result,
                'macro': macro_result,
            },
            'price': price,
            'btc_qqq_regime': btc_qqq_regime,
            'context': {
                'tech': tech_result.get('components', {}),
                'position': pos_result.get('components', {}),
                'macro': macro_result.get('components', {}),
                'news_event': news_evt_result.get('components', {}),
            },
        }

        # Record to score_history
        try:
            record_score(cur, result)
        except Exception:
            pass

        return result

    except Exception as e:
        _log(f'compute_total error: {e}')
        import traceback
        traceback.print_exc()
        return {
            'total_score': 0,
            'abs_score': 0,
            'dominant_side': 'LONG',
            'stage': 1,
            'dynamic_stop_loss_pct': 2.0,
            'tech_score': 0,
            'position_score': 0,
            'regime_score': 0,
            'news_event_score': 0,
            'news_event_guarded': False,
            'macro_score': 0,
            'liquidity_score': 0,
            'long_score': 50,
            'short_score': 50,
            'confidence': 0,
            'weights': DEFAULT_WEIGHTS,
            'axis_details': {},
            'price': None,
            'context': {},
            'error': str(e),
        }
    finally:
        if own_conn and conn:
            try:
                conn.close()
            except Exception:
                pass


def record_score(cur=None, result=None):
    '''Record score snapshot to score_history table.'''
    ctx = dict(result.get('context', {}))
    ctx['news_event_score'] = result.get('news_event_score', 0)
    ctx['news_event_guarded'] = result.get('news_event_guarded', False)
    cur.execute("""
            INSERT INTO score_history
                (symbol, total_score, tech_score, macro_event_score,
                 market_regime_score, liquidity_flow_score, position_context_score,
                 dominant_side, computed_stage, dynamic_stop_loss_pct,
                 btc_price, context)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb);
        """, (
        SYMBOL,
        result.get('total_score'),
        result.get('tech_score'),
        result.get('news_event_score', 0),
        result.get('regime_score'),
        0,  # liquidity_flow_score (deprecated axis, kept for schema compat)
        result.get('position_score'),
        result.get('dominant_side'),
        result.get('stage'),
        result.get('dynamic_stop_loss_pct'),
        result.get('price'),
        json.dumps(ctx, default=str, ensure_ascii=False)))


if __name__ == '__main__':
    result = compute_total()
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
