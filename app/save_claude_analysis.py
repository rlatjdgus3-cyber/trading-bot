"""
save_claude_analysis.py — Store Claude analysis I/O for feedback loop.

Library module imported by position_manager.py.
No daemon, no main loop.
"""
import json

LOG_PREFIX = '[save_claude]'


def _log(msg):
    print(f"{LOG_PREFIX} {msg}", flush=True)


def save_analysis(cur, kind, input_packet=None, output=None,
                  event_id=None, similar_events=None,
                  emergency_log_id=None,
                  model_used='claude-sonnet-4-20250514',
                  model_provider=None):
    """Insert a Claude analysis record into claude_analyses.

    Returns the new claude_analyses.id, or None on error.
    """
    if not similar_events:
        similar_events = []
    if not input_packet:
        input_packet = {}
    if not output:
        output = {}

    try:
        cur.execute("""
            INSERT INTO claude_analyses
                (kind, symbol, event_id, emergency_log_id,
                 input_packet, output_packet, similar_events_used,
                 risk_level, recommended_action, confidence,
                 reason_bullets, ttl_seconds,
                 model_used, model_provider, api_latency_ms, fallback_used,
                 input_tokens, output_tokens, estimated_cost_usd, gate_type)
            VALUES (%s, %s, %s, %s,
                    %s::jsonb, %s::jsonb, %s::jsonb,
                    %s, %s, %s,
                    %s::jsonb, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s)
            RETURNING id;
        """, (
            kind,
            'BTC/USDT:USDT',
            event_id,
            emergency_log_id,
            json.dumps(input_packet, default=str),
            json.dumps(output, default=str),
            json.dumps(similar_events, default=str),
            output.get('risk_level'),
            output.get('recommended_action'),
            output.get('confidence'),
            json.dumps(output.get('reason_bullets', []), ensure_ascii=False),
            output.get('ttl_seconds'),
            model_used,
            model_provider,
            output.get('api_latency_ms'),
            output.get('fallback_used', False),
            output.get('input_tokens'),
            output.get('output_tokens'),
            output.get('estimated_cost_usd'),
            output.get('gate_type'),
        ))
        row = cur.fetchone()
        ca_id = row[0] if row else None
        _log(f"saved analysis id={ca_id} kind={kind}")
        return ca_id
    except Exception as e:
        _log(f"error saving analysis: {repr(e)}")
        return None


def create_pending_outcome(cur, claude_analysis_id, executed_action,
                           execution_queue_id=None):
    """Create a pending outcome record for later evaluation.

    Returns analysis_outcomes.id, or None on error.
    """
    try:
        cur.execute("""
            INSERT INTO analysis_outcomes
                (claude_analysis_id, executed_action, execution_queue_id,
                 outcome_label, metadata)
            VALUES (%s, %s, %s, 'pending', '{}'::jsonb)
            ON CONFLICT (claude_analysis_id) DO NOTHING
            RETURNING id;
        """, (claude_analysis_id, executed_action, execution_queue_id))
        row = cur.fetchone()
        ao_id = row[0] if row else None
        _log(f"created pending outcome id={ao_id} for analysis={claude_analysis_id}")
        return ao_id
    except Exception as e:
        _log(f"error creating pending outcome: {repr(e)}")
        return None
