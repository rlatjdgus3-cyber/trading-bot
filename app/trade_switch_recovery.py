"""
trade_switch_recovery.py — Auto-recovery for trade_switch.

Single source of truth for all trade_switch writes (ON/OFF).

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

# ── off_reason Standardized Values ──
OFF_REASONS = {
    'scheduled_settlement': '정기 정산 (17:25-17:40 KST)',
    'consecutive_stops': '연속 손절',
    'equity_drawdown': '자본 드로다운',
    'panic_close': '긴급 청산',
    'manual': '수동 OFF',
    'error_spike': '에러 급증 (order_throttle)',
    'rate_limit': 'API 속도 제한',
    'safety_gate_fail': '안전 게이트 실패',
    'monitoring_down': '모니터링 서비스 heartbeat 중단',
    'unknown': '원인 미상',
}


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


def _get_settlement_display():
    """Return settlement window status string for notifications."""
    import datetime
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo
    now_kst = datetime.datetime.now(ZoneInfo('Asia/Seoul'))
    t = now_kst.time()
    if datetime.time(17, 25) <= t <= datetime.time(17, 40):
        return '진행중 (ends ~17:40 KST)'
    elif t < datetime.time(17, 25):
        return '다음 예정: 17:25 KST'
    else:
        return '종료'


def _ensure_columns(cur):
    """Defensive: ensure all trade_switch columns exist. Idempotent."""
    global _columns_ensured
    if _columns_ensured:
        return
    try:
        cur.execute("ALTER TABLE trade_switch ADD COLUMN IF NOT EXISTS off_reason TEXT DEFAULT NULL;")
        cur.execute("ALTER TABLE trade_switch ADD COLUMN IF NOT EXISTS manual_off_until TIMESTAMPTZ DEFAULT NULL;")
        cur.execute("ALTER TABLE trade_switch ADD COLUMN IF NOT EXISTS last_changed_by TEXT DEFAULT 'unknown';")
        cur.execute("ALTER TABLE trade_switch ADD COLUMN IF NOT EXISTS last_auto_recover_ts TIMESTAMPTZ DEFAULT NULL;")
        cur.execute("ALTER TABLE trade_switch ADD COLUMN IF NOT EXISTS last_disable_ts TIMESTAMPTZ DEFAULT NULL;")
        _columns_ensured = True
    except Exception as e:
        _log(f'_ensure_columns warning: {e}')
        try:
            if cur.connection and not cur.connection.autocommit:
                cur.connection.rollback()
        except Exception:
            pass


def set_on(cur, changed_by='system', auto_recovered=False):
    """Set trade_switch ON (single source of truth).

    Args:
        cur: DB cursor
        changed_by: 'auto_recovery', 'manual', 'equity_guard', 'scheduled_settlement', etc.
        auto_recovered: if True, also set last_auto_recover_ts
    """
    _ensure_columns(cur)
    try:
        if auto_recovered:
            cur.execute("""
                UPDATE trade_switch
                SET enabled = true, off_reason = NULL, manual_off_until = NULL,
                    last_changed_by = %s, last_auto_recover_ts = now(), updated_at = now()
                WHERE id = (SELECT id FROM trade_switch ORDER BY id DESC LIMIT 1);
            """, (changed_by,))
        else:
            cur.execute("""
                UPDATE trade_switch
                SET enabled = true, off_reason = NULL, manual_off_until = NULL,
                    last_changed_by = %s, updated_at = now()
                WHERE id = (SELECT id FROM trade_switch ORDER BY id DESC LIMIT 1);
            """, (changed_by,))
    except Exception as e:
        # Fallback: if new columns missing, do simple UPDATE
        _log(f'set_on fallback (new columns missing?): {e}')
        try:
            # Ensure connection is usable (rollback any failed transaction state)
            if cur.connection and not cur.connection.autocommit:
                cur.connection.rollback()
        except Exception:
            pass
        cur.execute("""
            UPDATE trade_switch
            SET enabled = true, off_reason = NULL, manual_off_until = NULL,
                updated_at = now()
            WHERE id = (SELECT id FROM trade_switch ORDER BY id DESC LIMIT 1);
        """)
    _log(f'trade_switch ON: changed_by={changed_by} auto_recovered={auto_recovered}')


def set_off_with_reason(cur, reason, manual_ttl_minutes=0, changed_by='system'):
    """Set trade_switch OFF with off_reason and optional manual TTL.

    Args:
        cur: DB cursor
        reason: 'consecutive_stops', 'equity_drawdown', 'scheduled_settlement',
                'panic_close', 'manual', 'error_spike', etc.
        manual_ttl_minutes: if > 0, set manual_off_until = now() + interval
        changed_by: who initiated (default='system')
    """
    _ensure_columns(cur)

    try:
        if manual_ttl_minutes > 0:
            cur.execute("""
                UPDATE trade_switch
                SET enabled = false, off_reason = %s,
                    manual_off_until = now() + make_interval(mins => %s),
                    last_changed_by = %s, last_disable_ts = now(), updated_at = now()
                WHERE id = (SELECT id FROM trade_switch ORDER BY id DESC LIMIT 1);
            """, (reason, manual_ttl_minutes, changed_by))
        else:
            cur.execute("""
                UPDATE trade_switch
                SET enabled = false, off_reason = %s, manual_off_until = NULL,
                    last_changed_by = %s, last_disable_ts = now(), updated_at = now()
                WHERE id = (SELECT id FROM trade_switch ORDER BY id DESC LIMIT 1);
            """, (reason, changed_by))
    except Exception as e:
        # Fallback: if new columns missing, do simple UPDATE
        _log(f'set_off_with_reason fallback (new columns missing?): {e}')
        try:
            if cur.connection and not cur.connection.autocommit:
                cur.connection.rollback()
        except Exception:
            pass
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

    _log(f'trade_switch OFF: reason={reason} ttl={manual_ttl_minutes}m changed_by={changed_by}')


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
    set_on(cur, changed_by='auto_recovery', auto_recovered=True)

    # 6. Enhanced notification
    settlement = _get_settlement_display()
    reason_label = OFF_REASONS.get(off_reason, off_reason or 'unknown')
    is_normal = off_reason == 'scheduled_settlement'

    _log(f'AUTO-RECOVERED: off_reason was {off_reason!r}')
    _notify_telegram(
        f'{"✅" if is_normal else "⚠️"} trade_switch 자동 복구\n'
        f'- 이전 사유: {off_reason or "unknown"} → "{reason_label}"'
        f'{" (정상 정산 후 자동 복구)" if is_normal else ""}\n'
        f'- 복구 조건: gate PASS + settlement 종료 + manual TTL 없음\n'
        f'- settlement: {settlement}')

    return (True, 'auto_recovered')


def format_trade_status(cur):
    """P2-A: Format trade_switch status as one-line summary.

    Returns: str like
      TRADE: ON | reason: - | auto_recovered: false | settlement: 종료
    or
      TRADE: OFF | reason: consecutive_stops (연속 손절) | by: equity_guard | auto_recover: 17:42 KST
    """
    try:
        cur.execute("""
            SELECT enabled, off_reason, last_changed_by,
                   manual_off_until, last_auto_recover_ts, last_disable_ts
            FROM trade_switch ORDER BY id DESC LIMIT 1;
        """)
        row = cur.fetchone()
        if not row:
            return 'TRADE: UNKNOWN (no row)'

        enabled = row[0]
        off_reason = row[1] or '-'
        changed_by = row[2] or '-'
        manual_off_until = row[3]
        last_auto_recover = row[4]
        last_disable = row[5]

        settlement = _get_settlement_display()

        if enabled:
            auto_rec_str = 'true' if last_auto_recover else 'false'
            return (f'TRADE: ON | reason: - | auto_recovered: {auto_rec_str} '
                    f'| settlement: {settlement}')
        else:
            reason_label = OFF_REASONS.get(off_reason, off_reason)
            recover_str = '-'
            if manual_off_until:
                import datetime
                try:
                    import pytz
                    kst = pytz.timezone('Asia/Seoul')
                    kst_time = manual_off_until.astimezone(kst)
                    recover_str = kst_time.strftime('%H:%M KST')
                except Exception:
                    recover_str = str(manual_off_until)[:16]
            return (f'TRADE: OFF | reason: {off_reason} ({reason_label}) '
                    f'| by: {changed_by} | auto_recover: {recover_str}')
    except Exception as e:
        return f'TRADE: ERROR ({e})'
