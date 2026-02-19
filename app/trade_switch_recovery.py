"""
trade_switch_recovery.py — Auto-recovery for trade_switch.

Rules:
  1. trade_switch OFF + manual_off_until > now() → manual TTL protection, no recovery
  2. trade_switch OFF + safety_manager gate PASS → auto ON
  3. trade_switch OFF + gate BLOCKED → stay OFF (log only)
  4. Settlement window (17:25-17:40 KST) → no recovery
"""
import sys
sys.path.insert(0, '/root/trading-bot/app')
LOG_PREFIX = '[trade_switch_recovery]'
_columns_ensured = False


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _notify_telegram(text):
    import urllib.parse
    import urllib.request
    try:
        with open('/root/trading-bot/app/telegram_cmd.env') as f:
            cfg = {}
            for line in f:
                line = line.strip()
                if '=' in line and not line.startswith('#'):
                    k, v = line.split('=', 1)
                    cfg[k.strip()] = v.strip()
        token = cfg.get('TELEGRAM_BOT_TOKEN', '')
        chat_id = cfg.get('TELEGRAM_ALLOWED_CHAT_ID', '')
        if not token or not chat_id:
            return
        url = f'https://api.telegram.org/bot{token}/sendMessage'
        data = urllib.parse.urlencode({
            'chat_id': chat_id,
            'text': text,
            'disable_web_page_preview': 'true'}).encode('utf-8')
        req = urllib.request.Request(url, data=data, method='POST')
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass


def _is_settlement_window():
    """Returns True if current time is within 17:25-17:40 KST (settlement window)."""
    import datetime
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo
    now_kst = datetime.datetime.now(ZoneInfo('Asia/Seoul'))
    t = now_kst.time()
    return datetime.time(17, 25) <= t <= datetime.time(17, 40)


def _ensure_columns(cur):
    """Defensive: ensure off_reason/manual_off_until columns exist. Idempotent."""
    global _columns_ensured
    if _columns_ensured:
        return
    try:
        cur.execute("ALTER TABLE trade_switch ADD COLUMN IF NOT EXISTS off_reason TEXT DEFAULT NULL;")
        cur.execute("ALTER TABLE trade_switch ADD COLUMN IF NOT EXISTS manual_off_until TIMESTAMPTZ DEFAULT NULL;")
        _columns_ensured = True
    except Exception as e:
        _log(f'_ensure_columns warning: {e}')


def try_auto_recover(cur):
    """Attempt auto-recovery of trade_switch.

    Returns (recovered: bool, reason: str).
    """
    _ensure_columns(cur)

    # 1. Check current trade_switch state
    cur.execute("""
        SELECT enabled, off_reason, manual_off_until
        FROM trade_switch ORDER BY id DESC LIMIT 1;
    """)
    row = cur.fetchone()
    if not row:
        return (False, 'no_trade_switch_row')

    enabled, off_reason, manual_off_until = row[0], row[1], row[2]

    # Already ON
    if enabled:
        return (False, 'already_on')

    # 2. Manual OFF with TTL still active
    if manual_off_until is not None:
        import datetime
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        if manual_off_until.tzinfo is None:
            manual_off_until = manual_off_until.replace(tzinfo=datetime.timezone.utc)
        if now_utc < manual_off_until:
            remaining = int((manual_off_until - now_utc).total_seconds())
            return (False, f'manual_ttl_active ({remaining}s remaining)')

    # 3. Settlement window protection
    if _is_settlement_window():
        return (False, 'settlement_window')

    # 4. Run safety gate checks (emergency=True to skip daily/hourly limits)
    import safety_manager
    gate_ok, gate_reason = safety_manager.run_all_checks(cur, emergency=True)

    if not gate_ok:
        return (False, f'gate_blocked: {gate_reason}')

    # 5. Gate PASS — recover!
    cur.execute("""
        UPDATE trade_switch
        SET enabled = true, off_reason = NULL, manual_off_until = NULL,
            updated_at = now()
        WHERE id = (SELECT id FROM trade_switch ORDER BY id DESC LIMIT 1);
    """)

    _log(f'AUTO-RECOVERED: off_reason was {off_reason!r}')
    _notify_telegram(
        f'✅ trade_switch 자동 복구\n'
        f'- 이전 사유: {off_reason or "unknown"}\n'
        f'- gate: PASS 확인')

    return (True, 'auto_recovered')


def set_off_with_reason(cur, reason, manual_ttl_minutes=0):
    """Set trade_switch OFF with off_reason and optional manual TTL.

    Args:
        cur: DB cursor
        reason: 'consecutive_stops', 'equity_drawdown', 'scheduled_settlement',
                'panic_close', 'manual'
        manual_ttl_minutes: if > 0, set manual_off_until = now() + interval
    """
    _ensure_columns(cur)

    if manual_ttl_minutes > 0:
        cur.execute("""
            UPDATE trade_switch
            SET enabled = false, off_reason = %s,
                manual_off_until = now() + make_interval(mins => %s),
                updated_at = now()
            WHERE id = (SELECT id FROM trade_switch ORDER BY id DESC LIMIT 1);
        """, (reason, manual_ttl_minutes))
    else:
        cur.execute("""
            UPDATE trade_switch
            SET enabled = false, off_reason = %s, manual_off_until = NULL,
                updated_at = now()
            WHERE id = (SELECT id FROM trade_switch ORDER BY id DESC LIMIT 1);
        """, (reason,))

    _log(f'trade_switch OFF: reason={reason} ttl={manual_ttl_minutes}m')
