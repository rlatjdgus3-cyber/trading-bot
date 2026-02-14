"""
openclaw_engine.py — Directive parsing, execution, and audit engine.

Supports 5 directive types:
  WATCH_KEYWORDS, RISK_MODE, ANALYSIS_REQUEST, PIPELINE_TUNE, AUDIT

All directives are logged to openclaw_directives table.
Active policies are stored in openclaw_policies (key-value).
"""
import os
import sys
import json
import hashlib
import traceback

sys.path.insert(0, '/root/trading-bot/app')
LOG_PREFIX = '[openclaw_engine]'
CALLER = 'openclaw_gateway'

DIRECTIVE_TYPES = {
    'WATCH_KEYWORDS',
    'RISK_MODE',
    'ANALYSIS_REQUEST',
    'PIPELINE_TUNE',
    'AUDIT',
}

RISK_PRESETS = {
    'conservative': {
        'stop_loss_pct': 1.5,
        'max_daily_trades': 10,
        'capital_limit_usdt': 500,
        'dynamic_sl_min_pct': 1.0,
        'dynamic_sl_max_pct': 2.0,
    },
    'normal': {
        'stop_loss_pct': 2.0,
        'max_daily_trades': 20,
        'capital_limit_usdt': 900,
        'dynamic_sl_min_pct': 1.2,
        'dynamic_sl_max_pct': 3.0,
    },
    'aggressive': {
        'stop_loss_pct': 3.0,
        'max_daily_trades': 30,
        'capital_limit_usdt': 1500,
        'dynamic_sl_min_pct': 1.5,
        'dynamic_sl_max_pct': 4.0,
    },
}


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _compute_idempotency_key(dtype, params):
    raw = f"{dtype}:{json.dumps(params, sort_keys=True)}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def get_policy(conn, key, default=None):
    """Read a policy value from openclaw_policies. Used by downstream systems."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT value FROM openclaw_policies WHERE key = %s;", (key,))
            row = cur.fetchone()
            if row and row[0] is not None:
                val = row[0]
                return val if not isinstance(val, str) else json.loads(val)
    except Exception:
        pass
    return default


def parse_directive(text, intent_json=None):
    """Extract directive from NL text or intent JSON.

    Returns: {dtype, params, idempotency_key} or None if not a directive.
    """
    if intent_json and intent_json.get('dtype'):
        dtype = intent_json['dtype'].upper()
        params = intent_json.get('params', {})
        if dtype in DIRECTIVE_TYPES:
            return {
                'dtype': dtype,
                'params': params,
                'idempotency_key': _compute_idempotency_key(dtype, params),
            }

    t = (text or '').lower().strip()

    # WATCH_KEYWORDS
    for kw in ['키워드', '워치', '감시', 'keyword', 'watch']:
        if kw in t:
            params = _parse_keyword_text(text)
            return {
                'dtype': 'WATCH_KEYWORDS',
                'params': params,
                'idempotency_key': _compute_idempotency_key('WATCH_KEYWORDS', params),
            }

    # RISK_MODE
    for kw in ['리스크', 'risk', '위험모드', '보수', '공격']:
        if kw in t:
            mode = 'normal'
            if any(x in t for x in ['보수', 'conservative', '안전']):
                mode = 'conservative'
            elif any(x in t for x in ['공격', 'aggressive', '적극']):
                mode = 'aggressive'
            params = {'mode': mode}
            return {
                'dtype': 'RISK_MODE',
                'params': params,
                'idempotency_key': _compute_idempotency_key('RISK_MODE', params),
            }

    # AUDIT
    for kw in ['감사', 'audit', '오딧', '시스템점검', '시스템 점검']:
        if kw in t:
            return {
                'dtype': 'AUDIT',
                'params': {},
                'idempotency_key': _compute_idempotency_key('AUDIT', {}),
            }

    # PIPELINE_TUNE
    for kw in ['파이프라인', '가중치', 'weight', '임계', 'threshold']:
        if kw in t:
            params = _parse_pipeline_text(text)
            return {
                'dtype': 'PIPELINE_TUNE',
                'params': params,
                'idempotency_key': _compute_idempotency_key('PIPELINE_TUNE', params),
            }

    # ANALYSIS_REQUEST
    for kw in ['분석 요청', '딥분석', 'deep analysis', '정밀분석']:
        if kw in t:
            params = {'topic': 'strategy', 'context': {}}
            return {
                'dtype': 'ANALYSIS_REQUEST',
                'params': params,
                'idempotency_key': _compute_idempotency_key('ANALYSIS_REQUEST', params),
            }

    return None


def _parse_keyword_text(text):
    """Parse keyword directive from natural language."""
    t = (text or '').lower()
    if any(x in t for x in ['삭제', 'remove', '빼']):
        action = 'remove'
    elif any(x in t for x in ['추가', 'add', '넣']):
        action = 'add'
    elif any(x in t for x in ['설정', 'set', '세팅']):
        action = 'set'
    else:
        action = 'list'

    # Extract words that look like keywords (after action words)
    import re
    words = re.findall(r'[a-zA-Z가-힣]+', text or '')
    skip = {'키워드', '워치', '감시', 'keyword', 'watch', '추가', '삭제',
            'add', 'remove', 'set', '설정', '넣어', '빼', '해줘', '해',
            '에', '를', '을', '좀', 'list', '목록', '보여', '줘'}
    keywords = [w for w in words if w.lower() not in skip and len(w) >= 2]

    return {'action': action, 'keywords': keywords}


def _parse_pipeline_text(text):
    """Parse pipeline tune directive from natural language."""
    t = (text or '').lower()
    if any(x in t for x in ['임계', 'threshold', 'emergency']):
        return {'target': 'emergency_thresholds', 'values': {}}
    return {'target': 'score_weights', 'values': {}}


def execute_directive(conn, dtype, params, source='telegram'):
    """Execute a directive with idempotency check and DB logging.

    Returns: {status, result, directive_id, message}
    """
    dtype = (dtype or '').upper()
    if dtype not in DIRECTIVE_TYPES:
        return {
            'status': 'FAILED',
            'result': {},
            'directive_id': None,
            'message': f'Unknown directive type: {dtype}',
        }

    idem_key = _compute_idempotency_key(dtype, params)

    # Idempotency check
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, status, result FROM openclaw_directives
                WHERE idempotency_key = %s;
            """, (idem_key,))
            existing = cur.fetchone()
            if existing:
                return {
                    'status': existing[1],
                    'result': existing[2] or {},
                    'directive_id': existing[0],
                    'message': f'[{dtype}] Already applied (directive #{existing[0]})',
                }
    except Exception as e:
        _log(f'idempotency check error: {e}')

    # Insert PENDING directive
    directive_id = None
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO openclaw_directives (dtype, params, source, status, idempotency_key)
                VALUES (%s, %s::jsonb, %s, 'PENDING', %s)
                RETURNING id;
            """, (dtype, json.dumps(params, ensure_ascii=False), source, idem_key))
            row = cur.fetchone()
            directive_id = row[0] if row else None
    except Exception as e:
        _log(f'directive insert error: {e}')
        return {
            'status': 'FAILED',
            'result': {},
            'directive_id': None,
            'message': f'DB error: {e}',
        }

    # Execute handler
    try:
        if dtype == 'WATCH_KEYWORDS':
            result = _handle_watch_keywords(conn, params, directive_id)
        elif dtype == 'RISK_MODE':
            result = _handle_risk_mode(conn, params, directive_id)
        elif dtype == 'ANALYSIS_REQUEST':
            result = _handle_analysis_request(conn, params, directive_id,
                                              source=source)
        elif dtype == 'PIPELINE_TUNE':
            result = _handle_pipeline_tune(conn, params, directive_id)
        elif dtype == 'AUDIT':
            result = _handle_audit(conn)
        else:
            result = {'error': 'no handler'}

        # Mark APPLIED
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE openclaw_directives
                SET status = 'APPLIED', result = %s::jsonb, applied_at = now()
                WHERE id = %s;
            """, (json.dumps(result, default=str, ensure_ascii=False), directive_id))

        message = result.get('message', f'[{dtype}] Applied successfully')
        return {
            'status': 'APPLIED',
            'result': result,
            'directive_id': directive_id,
            'message': message,
        }

    except Exception as e:
        _log(f'directive execute error: {e}')
        traceback.print_exc()
        # Mark FAILED
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE openclaw_directives
                    SET status = 'FAILED', error = %s
                    WHERE id = %s;
                """, (str(e), directive_id))
        except Exception:
            pass
        return {
            'status': 'FAILED',
            'result': {},
            'directive_id': directive_id,
            'message': f'[{dtype}] Error: {e}',
        }


def _handle_watch_keywords(conn, params, directive_id=None):
    """Handle WATCH_KEYWORDS directive.
    params: {action: 'add'|'remove'|'set'|'list', keywords: [...]}
    """
    action = params.get('action', 'list')
    keywords = params.get('keywords', [])

    with conn.cursor() as cur:
        # Load current
        cur.execute(
            "SELECT value FROM openclaw_policies WHERE key = 'watch_keywords';")
        row = cur.fetchone()
        current = []
        if row and row[0]:
            val = row[0]
            current = val if isinstance(val, list) else json.loads(val)

        if action == 'list':
            return {
                'keywords': current,
                'count': len(current),
                'message': f'Watch keywords ({len(current)}): {", ".join(current[:20])}',
            }

        if action == 'add':
            updated = list(set(current + keywords))
        elif action == 'remove':
            remove_set = set(k.lower() for k in keywords)
            updated = [k for k in current if k.lower() not in remove_set]
        elif action == 'set':
            updated = list(set(keywords))
        else:
            updated = current

        cur.execute("""
            INSERT INTO openclaw_policies (key, value, updated_at, directive_id, description)
            VALUES ('watch_keywords', %s::jsonb, now(), %s, 'news_bot watch keywords')
            ON CONFLICT (key) DO UPDATE
            SET value = EXCLUDED.value, updated_at = now(), directive_id = EXCLUDED.directive_id;
        """, (json.dumps(updated, ensure_ascii=False), directive_id))

    return {
        'action': action,
        'keywords_added': keywords if action == 'add' else [],
        'keywords_removed': keywords if action == 'remove' else [],
        'current': updated,
        'count': len(updated),
        'message': f'Watch keywords updated ({action}): {len(updated)} keywords active',
    }


def _handle_risk_mode(conn, params, directive_id=None):
    """Handle RISK_MODE directive.
    params: {mode: 'conservative'|'normal'|'aggressive'} or individual params.
    """
    mode = params.get('mode', '').lower()
    if mode not in RISK_PRESETS:
        return {'error': f'Unknown risk mode: {mode}',
                'message': f'Unknown risk mode: {mode}. Use: conservative, normal, aggressive'}

    preset = RISK_PRESETS[mode]

    with conn.cursor() as cur:
        # Update safety_limits
        cur.execute("""
            UPDATE safety_limits SET
                stop_loss_pct = %s,
                max_daily_trades = %s,
                capital_limit_usdt = %s,
                dynamic_sl_min_pct = %s,
                dynamic_sl_max_pct = %s,
                updated_at = now()
            WHERE id = (SELECT id FROM safety_limits ORDER BY id DESC LIMIT 1);
        """, (
            preset['stop_loss_pct'],
            preset['max_daily_trades'],
            preset['capital_limit_usdt'],
            preset['dynamic_sl_min_pct'],
            preset['dynamic_sl_max_pct'],
        ))

        # Record in policies
        cur.execute("""
            INSERT INTO openclaw_policies (key, value, updated_at, directive_id, description)
            VALUES ('risk_mode', %s::jsonb, now(), %s, 'current risk mode')
            ON CONFLICT (key) DO UPDATE
            SET value = EXCLUDED.value, updated_at = now(), directive_id = EXCLUDED.directive_id;
        """, (json.dumps({'mode': mode, 'preset': preset}, ensure_ascii=False), directive_id))

    return {
        'mode': mode,
        'preset': preset,
        'message': f'Risk mode set to [{mode.upper()}]\n'
                   f'  stop_loss: {preset["stop_loss_pct"]}%\n'
                   f'  max_daily_trades: {preset["max_daily_trades"]}\n'
                   f'  capital_limit: ${preset["capital_limit_usdt"]}\n'
                   f'  SL range: {preset["dynamic_sl_min_pct"]}%-{preset["dynamic_sl_max_pct"]}%',
    }


def _handle_analysis_request(conn, params, directive_id=None, source='telegram'):
    """Handle ANALYSIS_REQUEST directive.
    params: {topic: 'strategy'|'crash'|'news'|'position', context: {}}
    source: 'telegram' → USER call_type, else AUTO.
    """
    topic = params.get('topic', 'strategy')
    context = params.get('context', {})
    ct = 'USER' if source == 'telegram' else 'AUTO'

    try:
        import claude_gate
        if topic == 'strategy':
            prompt = (
                f"비트코인 선물 트레이딩 전략 분석 요청입니다.\n"
                f"Context: {json.dumps(context, ensure_ascii=False, default=str)}\n\n"
                "=== 분석 구조 (반드시 아래 순서로 작성) ===\n\n"
                "1️⃣ 박스권 vs 추세 판정\n"
                "- 24~72h 고점/저점 범위와 현재가 위치\n"
                "- BB bandwidth + mid 기울기, Kijun/Cloud/POC/VAH/VAL 위치\n"
                "- 최근 돌파 시도 성공/실패 여부\n\n"
                "2️⃣ REGIME 해석\n"
                "A) 고변동 하락 추세 / B) 고변동 박스권 / C) 단순 노이즈\n\n"
                "3️⃣ 최종 결론 (반드시 하나 선택):\n"
                "A) 박스권 반등 / B) 추세 전환 진행 / "
                "C) 아직 불명확 — 확정 트리거 가격 제시\n\n"
                "마지막 줄: 최종 ACTION: HOLD/ADD/REDUCE/CLOSE/REVERSE\n"
                "1000자 이내."
            )
        else:
            prompt = (
                f"System audit analysis request.\n"
                f"Topic: {topic}\n"
                f"Context: {json.dumps(context, ensure_ascii=False, default=str)}\n\n"
                "Provide structured analysis in JSON format with keys: "
                "risk_level, recommendation, key_observations, action_items."
            )
        result = claude_gate.call_claude(
            gate='openclaw', prompt=prompt,
            cooldown_key=f'openclaw_analysis_{topic}',
            context={'intent': 'analysis_request', 'topic': topic, 'source': 'openclaw'},
            max_tokens=500, call_type=ct)

        # Log Claude call for caller attribution
        try:
            import event_lock as _el
            _el.log_claude_call(
                caller=CALLER, gate_type='openclaw', call_type=ct,
                model_used=result.get('model'),
                input_tokens=result.get('input_tokens', 0),
                output_tokens=result.get('output_tokens', 0),
                estimated_cost=result.get('estimated_cost_usd', 0),
                latency_ms=result.get('api_latency_ms', 0),
                allowed=not result.get('fallback_used', False),
                deny_reason=result.get('gate_reason') if result.get('fallback_used') else None)
        except Exception:
            pass

        if result.get('fallback_used'):
            return {
                'topic': topic,
                'analysis': 'Claude gate denied — analysis deferred',
                'message': f'Analysis request ({topic}): Claude gate denied, will retry later.',
            }

        text = result.get('text', '')
        return {
            'topic': topic,
            'analysis': text,
            'message': f'Analysis ({topic}):\n{text[:1500]}',
        }
    except Exception as e:
        return {
            'topic': topic,
            'error': str(e),
            'message': f'Analysis request failed: {e}',
        }


def _handle_pipeline_tune(conn, params, directive_id=None):
    """Handle PIPELINE_TUNE directive.
    params: {target: 'score_weights'|'emergency_thresholds', values: {}}
    """
    target = params.get('target', '')
    values = params.get('values', {})

    if target == 'score_weights':
        return _tune_score_weights(conn, values, directive_id)
    elif target == 'emergency_thresholds':
        return _tune_emergency_thresholds(conn, values, directive_id)
    else:
        return {'error': f'Unknown target: {target}',
                'message': f'Unknown pipeline target: {target}'}


def _tune_score_weights(conn, values, directive_id=None):
    """Update score_weights via openclaw_policies override."""
    valid_keys = ('tech_w', 'position_w', 'regime_w', 'news_event_w')
    override = {}
    for k in valid_keys:
        if k in values:
            override[k] = float(values[k])

    if not override:
        # Return current weights
        with conn.cursor() as cur:
            cur.execute("""
                SELECT tech_w, position_w, regime_w, news_event_w
                FROM score_weights ORDER BY id DESC LIMIT 1;
            """)
            row = cur.fetchone()
            if row:
                current = {
                    'tech_w': float(row[0]), 'position_w': float(row[1]),
                    'regime_w': float(row[2]), 'news_event_w': float(row[3]),
                }
                return {'current_weights': current,
                        'message': f'Current weights: {json.dumps(current)}'}
        return {'message': 'No weights found'}

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO openclaw_policies (key, value, updated_at, directive_id, description)
            VALUES ('score_weight_override', %s::jsonb, now(), %s, 'score weight override')
            ON CONFLICT (key) DO UPDATE
            SET value = EXCLUDED.value, updated_at = now(), directive_id = EXCLUDED.directive_id;
        """, (json.dumps(override, ensure_ascii=False), directive_id))

    return {
        'override': override,
        'message': f'Score weight override applied: {json.dumps(override)}',
    }


def _tune_emergency_thresholds(conn, values, directive_id=None):
    """Update emergency thresholds via openclaw_policies."""
    valid_keys = ('1m_pct', '5m_pct', 'vol_multiplier', 'consecutive_stops')
    thresholds = {}
    for k in valid_keys:
        if k in values:
            thresholds[k] = values[k]

    if not thresholds:
        # Return current
        current = get_policy(conn, 'emergency_thresholds')
        if current:
            return {'current': current,
                    'message': f'Current thresholds: {json.dumps(current)}'}
        return {'message': 'No custom thresholds (using defaults)'}

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO openclaw_policies (key, value, updated_at, directive_id, description)
            VALUES ('emergency_thresholds', %s::jsonb, now(), %s, 'emergency detector thresholds')
            ON CONFLICT (key) DO UPDATE
            SET value = EXCLUDED.value, updated_at = now(), directive_id = EXCLUDED.directive_id;
        """, (json.dumps(thresholds, ensure_ascii=False), directive_id))

    return {
        'thresholds': thresholds,
        'message': f'Emergency thresholds updated: {json.dumps(thresholds)}',
    }


def _handle_audit(conn):
    """Handle AUDIT directive. Collect comprehensive system state."""
    report = {}

    with conn.cursor() as cur:
        # Position state
        try:
            cur.execute("""
                SELECT symbol, side, total_qty, avg_entry_price, stage,
                       capital_used_usdt, updated_at
                FROM position_state LIMIT 5;
            """)
            rows = cur.fetchall()
            report['positions'] = [
                {'symbol': r[0], 'side': r[1], 'qty': float(r[2]) if r[2] else 0,
                 'entry': float(r[3]) if r[3] else 0, 'stage': r[4],
                 'capital_used': float(r[5]) if r[5] else 0,
                 'updated': str(r[6])}
                for r in rows
            ]
        except Exception:
            report['positions'] = []

        # Safety limits
        try:
            cur.execute("""
                SELECT capital_limit_usdt, max_daily_trades, stop_loss_pct,
                       dynamic_sl_min_pct, dynamic_sl_max_pct, updated_at
                FROM safety_limits ORDER BY id DESC LIMIT 1;
            """)
            row = cur.fetchone()
            if row:
                report['safety_limits'] = {
                    'capital_limit': float(row[0]),
                    'max_daily_trades': int(row[1]),
                    'stop_loss_pct': float(row[2]),
                    'sl_range': f'{float(row[3])}%-{float(row[4])}%',
                    'updated': str(row[5]),
                }
        except Exception:
            report['safety_limits'] = {}

        # Score weights
        try:
            cur.execute("""
                SELECT tech_w, position_w, regime_w, news_event_w
                FROM score_weights ORDER BY id DESC LIMIT 1;
            """)
            row = cur.fetchone()
            if row:
                report['score_weights'] = {
                    'tech_w': float(row[0]), 'position_w': float(row[1]),
                    'regime_w': float(row[2]), 'news_event_w': float(row[3]),
                }
        except Exception:
            report['score_weights'] = {}

        # Recent score_history
        try:
            cur.execute("""
                SELECT ts, total_score, dominant_side, computed_stage, btc_price
                FROM score_history ORDER BY ts DESC LIMIT 5;
            """)
            rows = cur.fetchall()
            report['recent_scores'] = [
                {'ts': str(r[0]), 'total': float(r[1]) if r[1] else 0,
                 'side': r[2], 'stage': r[3],
                 'price': float(r[4]) if r[4] else 0}
                for r in rows
            ]
        except Exception:
            report['recent_scores'] = []

        # Active policies
        try:
            cur.execute("""
                SELECT key, value, updated_at FROM openclaw_policies
                ORDER BY updated_at DESC LIMIT 20;
            """)
            rows = cur.fetchall()
            report['policies'] = [
                {'key': r[0], 'value': r[1], 'updated': str(r[2])}
                for r in rows
            ]
        except Exception:
            report['policies'] = []

        # Recent directives
        try:
            cur.execute("""
                SELECT id, ts, dtype, status, source
                FROM openclaw_directives ORDER BY id DESC LIMIT 10;
            """)
            rows = cur.fetchall()
            report['recent_directives'] = [
                {'id': r[0], 'ts': str(r[1]), 'dtype': r[2],
                 'status': r[3], 'source': r[4]}
                for r in rows
            ]
        except Exception:
            report['recent_directives'] = []

    # Format report
    lines = ['=== SYSTEM AUDIT ===']

    # Positions
    lines.append('\n[Positions]')
    if report.get('positions'):
        for p in report['positions']:
            if p.get('side'):
                lines.append(
                    f"  {p['symbol']}: {p['side']} qty={p['qty']} "
                    f"entry=${p['entry']:,.1f} stage={p['stage']} "
                    f"capital=${p['capital_used']:,.0f}")
            else:
                lines.append(f"  {p['symbol']}: no position")
    else:
        lines.append('  (none)')

    # Safety
    sl = report.get('safety_limits', {})
    if sl:
        lines.append('\n[Safety Limits]')
        lines.append(f"  capital_limit: ${sl.get('capital_limit', 0):,.0f}")
        lines.append(f"  max_daily_trades: {sl.get('max_daily_trades', 0)}")
        lines.append(f"  stop_loss: {sl.get('stop_loss_pct', 0)}%")
        lines.append(f"  SL range: {sl.get('sl_range', 'n/a')}")

    # Weights
    sw = report.get('score_weights', {})
    if sw:
        lines.append('\n[Score Weights]')
        lines.append(f"  tech={sw.get('tech_w')} pos={sw.get('position_w')} "
                     f"regime={sw.get('regime_w')} news={sw.get('news_event_w')}")

    # Recent scores
    lines.append('\n[Recent Scores]')
    for s in report.get('recent_scores', [])[:3]:
        lines.append(f"  {s['ts'][:16]} score={s['total']:+.1f} "
                     f"{s['side']} stg={s['stage']} ${s['price']:,.0f}")

    # Policies
    if report.get('policies'):
        lines.append('\n[Active Policies]')
        for p in report['policies'][:5]:
            val_str = json.dumps(p['value'], ensure_ascii=False)
            if len(val_str) > 60:
                val_str = val_str[:60] + '...'
            lines.append(f"  {p['key']}: {val_str}")

    # Recent directives
    if report.get('recent_directives'):
        lines.append('\n[Recent Directives]')
        for d in report['recent_directives'][:5]:
            lines.append(f"  #{d['id']} {d['ts'][:16]} {d['dtype']} "
                         f"[{d['status']}] src={d['source']}")

    message = '\n'.join(lines)
    report['message'] = message
    return report
