"""
health_scorer.py — Degraded-mode health scoring.

Components (total 100 pts):
  - service_health:        30 pts — required services running
  - exchange_connectivity:  25 pts — exchange API reachable + recent data
  - data_freshness:         20 pts — candles/indicators not stale
  - reconcile:              15 pts — exchange/DB state match
  - throttle_headroom:      10 pts — not near daily/hourly limits

Risk multiplier:
  100       → 1.0x (full sizing)
  75-99     → 0.8x
  50-74     → 0.5x
  25-49     → exits only (no new entries)
  <25       → full block
"""
import time

LOG_PREFIX = '[health_scorer]'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def compute_health_score(cur=None):
    """Compute overall health score (0-100) with component breakdown.

    Returns:
        (score: int, components: dict)
    """
    components = {}
    total = 0

    # 1. Service health (30 pts)
    svc_score = _score_service_health()
    components['service_health'] = svc_score
    total += svc_score

    # 2. Exchange connectivity (25 pts)
    exch_score = _score_exchange_connectivity()
    components['exchange_connectivity'] = exch_score
    total += exch_score

    # 3. Data freshness (20 pts)
    data_score = _score_data_freshness(cur)
    components['data_freshness'] = data_score
    total += data_score

    # 4. Reconcile (15 pts)
    recon_score = _score_reconcile(cur)
    components['reconcile'] = recon_score
    total += recon_score

    # 5. Throttle headroom (10 pts)
    throttle_score = _score_throttle_headroom(cur)
    components['throttle_headroom'] = throttle_score
    total += throttle_score

    total = max(0, min(100, total))
    return (total, components)


def get_risk_multiplier(score):
    """Map health score to risk multiplier for position sizing.

    Returns:
        (multiplier: float, mode: str)
        mode: 'FULL' | 'REDUCED' | 'HALF' | 'EXIT_ONLY' | 'BLOCKED'
    """
    if score >= 100:
        return (1.0, 'FULL')
    elif score >= 75:
        return (0.8, 'REDUCED')
    elif score >= 50:
        return (0.5, 'HALF')
    elif score >= 25:
        return (0.0, 'EXIT_ONLY')
    else:
        return (0.0, 'BLOCKED')


def _score_service_health():
    """Score service health: 30 pts max."""
    try:
        from local_query_executor import get_service_health_snapshot
        health = get_service_health_snapshot()
        req_down = health.get('required_down', [])
        req_unknown = health.get('required_unknown', [])
        total_required = health.get('required_total', 5)

        if not req_down and not req_unknown:
            return 30  # All good

        # Each down service loses proportional points
        down_penalty = len(req_down) * (30 / max(total_required, 1))
        unknown_penalty = len(req_unknown) * (10 / max(total_required, 1))
        return max(0, int(30 - down_penalty - unknown_penalty))
    except Exception as e:
        _log(f'service_health score error: {e}')
        return 15  # Unknown = half credit


def _score_exchange_connectivity():
    """Score exchange connectivity: 25 pts max."""
    try:
        import exchange_reader
        bal = exchange_reader.fetch_balance()
        if bal.get('data_status') == 'OK':
            return 25
        elif bal.get('data_status') == 'ERROR':
            return 0
        return 12  # Partial/unknown
    except Exception as e:
        _log(f'exchange_connectivity score error: {e}')
        return 0


def _score_data_freshness(cur):
    """Score data freshness: 20 pts max.
    Checks candles and indicators staleness."""
    if cur is None:
        return 10  # No cursor = unknown
    score = 0
    try:
        # Candles freshness (10 pts)
        cur.execute("""
            SELECT EXTRACT(EPOCH FROM (now() - max(ts))) AS age_sec
            FROM candles WHERE symbol = 'BTC/USDT:USDT' AND tf = '1m';
        """)
        row = cur.fetchone()
        if row and row[0] is not None:
            age = float(row[0])
            if age < 120:       # <2 min
                score += 10
            elif age < 300:     # <5 min
                score += 7
            elif age < 600:     # <10 min
                score += 3
            # else: 0
    except Exception:
        score += 5  # Unknown = partial

    try:
        # Indicators freshness (10 pts)
        cur.execute("""
            SELECT EXTRACT(EPOCH FROM (now() - max(ts))) AS age_sec
            FROM indicators WHERE symbol = 'BTC/USDT:USDT' AND tf = '1m';
        """)
        row = cur.fetchone()
        if row and row[0] is not None:
            age = float(row[0])
            if age < 120:
                score += 10
            elif age < 300:
                score += 7
            elif age < 600:
                score += 3
    except Exception:
        score += 5

    return min(20, score)


def _score_reconcile(cur):
    """Score reconcile status: 15 pts max."""
    if cur is None:
        return 8  # Unknown
    try:
        import exchange_reader
        exch = exchange_reader.fetch_position()
        strat = exchange_reader.fetch_position_strat()
        result = exchange_reader.reconcile(exch, strat)
        status = result.get('legacy', 'UNKNOWN') if isinstance(result, dict) else result
        if status == 'MATCH':
            return 15
        elif status == 'MISMATCH':
            return 0
        return 8  # UNKNOWN
    except Exception as e:
        _log(f'reconcile score error: {e}')
        return 8


def _score_throttle_headroom(cur):
    """Score throttle headroom: 10 pts max.
    Check how close we are to daily/hourly limits."""
    if cur is None:
        return 5
    try:
        # Daily usage
        cur.execute("""
            SELECT count(*) FROM execution_queue
            WHERE ts >= (now() AT TIME ZONE 'Asia/Seoul')::date AT TIME ZONE 'Asia/Seoul'
              AND status != 'REJECTED';
        """)
        daily_count = int(cur.fetchone()[0])

        # Get limits
        cur.execute("""
            SELECT max_daily_trades, max_hourly_trades
            FROM safety_limits ORDER BY id DESC LIMIT 1;
        """)
        row = cur.fetchone()
        max_daily = int(row[0]) if row and row[0] else 60
        max_hourly = int(row[1]) if row and row[1] else 15

        # Hourly usage
        cur.execute("""
            SELECT count(*) FROM execution_queue
            WHERE ts >= now() - interval '1 hour'
              AND status != 'REJECTED';
        """)
        hourly_count = int(cur.fetchone()[0])

        daily_ratio = daily_count / max_daily if max_daily > 0 else 0
        hourly_ratio = hourly_count / max_hourly if max_hourly > 0 else 0
        worst_ratio = max(daily_ratio, hourly_ratio)

        if worst_ratio < 0.5:
            return 10
        elif worst_ratio < 0.75:
            return 7
        elif worst_ratio < 0.9:
            return 3
        return 0  # At or near limit
    except Exception as e:
        _log(f'throttle_headroom score error: {e}')
        return 5
