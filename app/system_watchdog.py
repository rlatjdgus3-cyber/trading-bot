#!/usr/bin/env python3
"""
system_watchdog.py — 24h independent service monitor.

Monitors all critical trading services, auto-restarts on failure,
sends Telegram alerts. Zero LLM dependency. Runs independently of OpenClaw.

Runs as a oneshot via systemd timer (every 3 minutes).
"""
import os
import sys
import json
import time
import subprocess

LOG_PREFIX = '[watchdog]'
ALERT_STATE_FILE = '/tmp/system_watchdog_state.json'
ALERT_COOLDOWN_SEC = 600  # 10 min between same-service alerts
MAX_RESTART_ATTEMPTS = 2  # max auto-restarts per service per hour

# Services to monitor (name, critical_level)
# critical: auto-restart + alert
# important: alert only (no auto-restart for data services)
MONITORED_SERVICES = [
    # Execution pipeline (CRITICAL - must always run)
    ('live_order_executor.service', 'critical'),
    ('position_manager.service', 'critical'),
    ('fill_watcher.service', 'critical'),

    # Data pipeline (important)
    ('candles.service', 'important'),
    ('indicators.service', 'important'),
    ('news_bot.service', 'important'),

    # Watchers (important)
    ('position_watcher.service', 'important'),
]

# Timers to monitor (should be active)
MONITORED_TIMERS = [
    'telegram_cmd_poller.timer',
    'telegram_healthcheck.timer',
    'test_lifecycle.timer',
]


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _load_state() -> dict:
    try:
        with open(ALERT_STATE_FILE, 'r') as f:
            return json.load(f)
    except Exception:
        return {'alerts': {}, 'restarts': {}}


def _save_state(state: dict):
    state['last_run'] = time.time()
    try:
        with open(ALERT_STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)
    except Exception:
        pass


def _is_active(unit: str) -> bool:
    """Check if a systemd unit is active."""
    try:
        result = subprocess.run(
            ['systemctl', 'is-active', unit],
            capture_output=True, text=True, timeout=10)
        return result.stdout.strip() == 'active'
    except Exception:
        return False


def _restart_service(unit: str) -> bool:
    """Restart a systemd service. Returns True on success."""
    try:
        result = subprocess.run(
            ['systemctl', 'restart', unit],
            capture_output=True, text=True, timeout=30)
        return result.returncode == 0
    except Exception:
        return False


def _can_alert(state: dict, key: str) -> bool:
    """Check cooldown for alerts."""
    alerts = state.get('alerts', {})
    last = alerts.get(key, 0)
    return time.time() - last >= ALERT_COOLDOWN_SEC


def _record_alert(state: dict, key: str):
    alerts = state.setdefault('alerts', {})
    alerts[key] = time.time()


def _can_restart(state: dict, unit: str) -> bool:
    """Check if we can auto-restart (max N per hour)."""
    restarts = state.get('restarts', {})
    history = restarts.get(unit, [])
    # Filter to last hour
    cutoff = time.time() - 3600
    recent = [t for t in history if t > cutoff]
    return len(recent) < MAX_RESTART_ATTEMPTS


def _record_restart(state: dict, unit: str):
    restarts = state.setdefault('restarts', {})
    history = restarts.setdefault(unit, [])
    history.append(time.time())
    # Keep only last hour
    cutoff = time.time() - 3600
    restarts[unit] = [t for t in history if t > cutoff]


def _send_telegram(text: str):
    """Send alert via Telegram. No LLM."""
    try:
        from report_formatter import korean_output_guard
        text = korean_output_guard(text)
    except Exception:
        pass
    try:
        env = {}
        with open('/root/trading-bot/app/telegram_cmd.env', 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                k, v = line.split('=', 1)
                env[k.strip()] = v.strip()
        token = env.get('TELEGRAM_BOT_TOKEN', '')
        chat_id = env.get('TELEGRAM_ALLOWED_CHAT_ID', '')
        if not token or not chat_id:
            return

        import urllib.parse
        import urllib.request
        url = f'https://api.telegram.org/bot{token}/sendMessage'
        data = urllib.parse.urlencode({
            'chat_id': chat_id,
            'text': text[:3500],
            'disable_web_page_preview': 'true',
        }).encode('utf-8')
        req = urllib.request.Request(url, data=data, method='POST')
        urllib.request.urlopen(req, timeout=15)
    except Exception as e:
        _log(f'telegram send error: {e}')


def _check_db_connection() -> tuple:
    """Check PostgreSQL connectivity. Returns (ok, detail)."""
    try:
        from db_config import get_conn
        conn = get_conn(autocommit=True)
        with conn.cursor() as cur:
            cur.execute('SELECT 1;')
        conn.close()
        return (True, 'ok')
    except Exception as e:
        return (False, str(e))


def _check_bybit_connection() -> tuple:
    """Check Bybit API connectivity. Returns (ok, detail)."""
    try:
        import ccxt
        ex = ccxt.bybit({
            'apiKey': os.getenv('BYBIT_API_KEY', ''),
            'secret': os.getenv('BYBIT_SECRET', ''),
            'enableRateLimit': True,
            'options': {'defaultType': 'swap'},
        })
        ticker = ex.fetch_ticker('BTC/USDT:USDT')
        price = ticker.get('last', 0)
        return (True, f'BTC=${price:,.0f}')
    except Exception as e:
        return (False, str(e)[:100])


def _check_execution_queue_health() -> tuple:
    """Check for stuck PENDING items older than 10 minutes."""
    try:
        from db_config import get_conn
        conn = get_conn(autocommit=True)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT count(*) FROM execution_queue
                WHERE status = 'PENDING' AND ts < now() - interval '10 minutes';
            """)
            stuck = cur.fetchone()[0]
        conn.close()
        if stuck > 0:
            return (False, f'{stuck} stuck PENDING items (>10min)')
        return (True, 'ok')
    except Exception as e:
        return (False, str(e)[:100])


def _write_heartbeats_to_db(service_states):
    """Write service heartbeats to service_health_log.

    Called every watchdog cycle (~3min) so /debug health has fresh data
    even without explicit /debug health calls.
    """
    conn = None
    try:
        from db_config import get_conn
        conn = get_conn(autocommit=True)
        with conn.cursor() as cur:
            for svc, state_val in service_states.items():
                cur.execute(
                    "INSERT INTO service_health_log (service, state) VALUES (%s, %s)",
                    (svc, state_val))
    except Exception as e:
        _log(f'heartbeat DB write error (non-fatal): {e}')
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def main():
    from dotenv import load_dotenv
    load_dotenv('/root/trading-bot/app/.env')

    state = _load_state()
    issues = []
    fixed = []

    # 0. Collect service states for heartbeat recording
    service_states = {}

    # 1. Check monitored services
    for unit, level in MONITORED_SERVICES:
        if _is_active(unit):
            service_states[unit] = 'OK'
            continue

        service_states[unit] = 'DOWN'
        _log(f'DOWN: {unit} (level={level})')

        if level == 'critical' and _can_restart(state, unit):
            ok = _restart_service(unit)
            _record_restart(state, unit)
            if ok:
                _log(f'RESTARTED: {unit}')
                fixed.append(unit)
                # Verify it's actually up now
                time.sleep(2)
                if not _is_active(unit):
                    issues.append(f'CRITICAL: {unit} restart FAILED')
            else:
                issues.append(f'CRITICAL: {unit} restart command failed')
        else:
            issues.append(f'{"CRITICAL" if level == "critical" else "WARNING"}: {unit} is DOWN')

    # 2. Check monitored timers
    for timer in MONITORED_TIMERS:
        if not _is_active(timer):
            service_states[timer] = 'DOWN'
            _log(f'TIMER DOWN: {timer}')
            # Auto-restart timers (safe)
            ok = _restart_service(timer)
            if ok:
                fixed.append(timer)
            else:
                issues.append(f'WARNING: {timer} is DOWN')
        else:
            service_states[timer] = 'OK'

    # 3. Check DB
    db_ok, db_detail = _check_db_connection()
    if not db_ok:
        issues.append(f'CRITICAL: DB connection failed: {db_detail}')
    else:
        service_states['db'] = 'OK'

    # 3b. Record heartbeats to DB (after DB connectivity confirmed)
    if db_ok:
        _write_heartbeats_to_db(service_states)

    # 4. Check execution queue health
    eq_ok, eq_detail = _check_execution_queue_health()
    if not eq_ok:
        issues.append(f'WARNING: EQ stuck: {eq_detail}')

    # 5. Check kill switch
    if os.path.exists('/root/trading-bot/app/KILL_SWITCH'):
        issues.append('WARNING: KILL_SWITCH file exists — live_order_executor halted')

    # Send alerts if needed
    if issues or fixed:
        alert_key = '|'.join(sorted(issues + fixed))
        if _can_alert(state, 'system_check'):
            lines = ['[system_watchdog] Health Check']
            if fixed:
                lines.append(f'\nAuto-fixed ({len(fixed)}):')
                for f in fixed:
                    lines.append(f'  + {f} restarted')
            if issues:
                lines.append(f'\nIssues ({len(issues)}):')
                for i in issues:
                    lines.append(f'  ! {i}')
            else:
                lines.append('\nAll services OK after auto-fix.')

            _send_telegram('\n'.join(lines))
            _record_alert(state, 'system_check')
    else:
        _log('all services OK')

    _save_state(state)


if __name__ == '__main__':
    main()
