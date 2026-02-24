"""
exchange_audit.py — Bybit 규정 변경 모니터링 + 10일 주기 점검.

기능:
  - execution_log에서 거부 건수 by error_type 집계
  - market_info 파라미터 변경 감지 (minQty, stepSize 등)
  - 리포트 생성 → exchange_policy_audit 테이블 + Telegram 알림
  - market_info 변경 즉시 감지: 해시 비교 → 변경 시 즉시 알림

Usage:
    python exchange_audit.py              # 10일 주기 점검 실행
    python exchange_audit.py --check-now  # 즉시 1회 실행
"""
import os
import sys
import json
import time
import hashlib
import traceback
from datetime import datetime, timezone, timedelta

sys.path.insert(0, '/root/trading-bot/app')

LOG_PREFIX = '[exchange_audit]'
AUDIT_INTERVAL_DAYS = 10
SYMBOL = 'BTC/USDT:USDT'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _db_conn():
    from db_config import get_conn
    return get_conn()


def _load_tg_env():
    """Load telegram credentials."""
    env_path = '/root/trading-bot/app/telegram_cmd.env'
    token, chat_id = '', ''
    try:
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                k, v = line.split('=', 1)
                if k.strip() == 'TELEGRAM_BOT_TOKEN':
                    token = v.strip()
                elif k.strip() == 'TELEGRAM_ALLOWED_CHAT_ID':
                    chat_id = v.strip()
    except Exception:
        pass
    return token, chat_id


def _send_telegram(text):
    """Send telegram notification."""
    import urllib.parse, urllib.request
    token, chat_id = _load_tg_env()
    if not token or not chat_id:
        return
    try:
        url = f'https://api.telegram.org/bot{token}/sendMessage'
        data = urllib.parse.urlencode({
            'chat_id': chat_id,
            'text': text[:4000],
            'disable_web_page_preview': 'true',
        }).encode('utf-8')
        req = urllib.request.Request(url, data=data, method='POST')
        urllib.request.urlopen(req, timeout=15)
    except Exception as e:
        _log(f'telegram send error: {e}')


def audit_error_stats(conn, days=10):
    """execution_log에서 거부/에러 건수 집계."""
    stats = {}
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COALESCE(error_code, 'unknown') AS err_code,
                    COALESCE(error_message, '') AS err_msg,
                    COUNT(*) AS cnt,
                    MAX(ts) AS last_seen
                FROM execution_log
                WHERE status IN ('REJECTED', 'ERROR', 'FAILED')
                  AND ts >= now() - make_interval(days => %s)
                GROUP BY error_code, error_message
                ORDER BY cnt DESC
                LIMIT 20;
            """, (days,))
            rows = cur.fetchall()
            stats['error_summary'] = [
                {
                    'error_code': r[0],
                    'error_message': (r[1] or '')[:100],
                    'count': r[2],
                    'last_seen': str(r[3]) if r[3] else '',
                }
                for r in rows
            ]
            stats['total_errors'] = sum(r[2] for r in rows)

            # Total orders in period
            cur.execute("""
                SELECT COUNT(*) FROM execution_log
                WHERE ts >= now() - make_interval(days => %s);
            """, (days,))
            stats['total_orders'] = cur.fetchone()[0]

            # Error rate
            if stats['total_orders'] > 0:
                stats['error_rate_pct'] = round(
                    stats['total_errors'] / stats['total_orders'] * 100, 2)
            else:
                stats['error_rate_pct'] = 0
    except Exception as e:
        _log(f'audit_error_stats error: {e}')
        stats['error'] = str(e)

    return stats


def check_market_info_changes(conn):
    """market_info 파라미터 변경 감지."""
    changes = []
    try:
        # Load current market info from exchange
        try:
            from exchange_compliance import _load_market_info
            from exchange_reader import _get_exchange
            ex = _get_exchange()
            current_info = _load_market_info(ex, SYMBOL)
        except (ImportError, AttributeError):
            current_info = None

        if not current_info:
            _log('market_info: cannot load current info')
            return changes

        # Compute hash
        info_str = json.dumps(current_info, sort_keys=True, default=str)
        current_hash = hashlib.sha256(info_str.encode()).hexdigest()[:16]

        # Compare with stored hash
        with conn.cursor() as cur:
            cur.execute("""
                SELECT metadata->>'market_info_hash' AS hash,
                       metadata->>'market_info' AS info
                FROM exchange_policy_audit
                WHERE symbol = %s
                ORDER BY ts DESC LIMIT 1;
            """, (SYMBOL,))
            row = cur.fetchone()
            prev_hash = row[0] if row else None
            prev_info = json.loads(row[1]) if row and row[1] else None

        if prev_hash and prev_hash != current_hash:
            # Detect specific changes
            if prev_info:
                for key in ('minQty', 'stepSize', 'tickSize', 'maxLeverage',
                            'minNotional', 'maxQty'):
                    old_val = prev_info.get(key)
                    new_val = current_info.get(key)
                    if old_val != new_val:
                        changes.append({
                            'param': key,
                            'old': old_val,
                            'new': new_val,
                        })
            else:
                changes.append({'param': 'all', 'old': 'unknown', 'new': 'changed'})

        # Store current info
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO exchange_policy_audit (symbol, ts, audit_type, metadata)
                VALUES (%s, now(), 'market_info_check', %s);
            """, (SYMBOL, json.dumps({
                'market_info_hash': current_hash,
                'market_info': current_info,
                'changes': changes,
            }, default=str)))
            conn.commit()
    except Exception as e:
        _log(f'market_info check error: {e}')
        try:
            conn.rollback()
        except Exception:
            pass

    return changes


def generate_audit_report(error_stats, market_changes, days=10):
    """감사 리포트 생성."""
    lines = [
        f'🔍 거래소 규정 점검 리포트 (최근 {days}일)',
        '━━━━━━━━━━━━━━━━━━━━━━',
        '',
    ]

    # Error stats
    lines.append('[📊 에러 통계]')
    total_orders = error_stats.get('total_orders', 0)
    total_errors = error_stats.get('total_errors', 0)
    error_rate = error_stats.get('error_rate_pct', 0)
    lines.append(f'총 주문: {total_orders}건 | 에러: {total_errors}건 ({error_rate}%)')

    errors = error_stats.get('error_summary', [])
    if errors:
        lines.append('')
        for i, e in enumerate(errors[:5], 1):
            lines.append(
                f'{i}. [{e["error_code"]}] {e["error_message"][:50]} — {e["count"]}회'
            )
    else:
        lines.append('에러 없음')

    # Market info changes
    lines.append('')
    lines.append('[⚙ 거래 파라미터 변경]')
    if market_changes:
        for ch in market_changes:
            lines.append(f'- {ch["param"]}: {ch["old"]} → {ch["new"]}')
    else:
        lines.append('변경 없음')

    return '\n'.join(lines)


def save_audit_result(conn, report_text, error_stats, market_changes):
    """감사 결과를 DB에 저장."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO exchange_policy_audit (symbol, ts, audit_type, metadata)
                VALUES (%s, now(), 'periodic_audit', %s);
            """, (SYMBOL, json.dumps({
                'report': report_text,
                'error_stats': error_stats,
                'market_changes': market_changes,
            }, default=str)))
            conn.commit()
    except Exception as e:
        _log(f'save_audit_result error: {e}')
        try:
            conn.rollback()
        except Exception:
            pass


def run_audit(days=None):
    """1회 감사 실행."""
    if days is None:
        days = AUDIT_INTERVAL_DAYS

    _log(f'START audit (days={days})')
    conn = _db_conn()
    try:
        error_stats = audit_error_stats(conn, days)
        market_changes = check_market_info_changes(conn)

        report = generate_audit_report(error_stats, market_changes, days)
        save_audit_result(conn, report, error_stats, market_changes)

        _log(f'audit complete: errors={error_stats.get("total_errors", 0)}, '
             f'changes={len(market_changes)}')

        # Telegram 알림
        _send_telegram(report)

        # 즉시 알림: 파라미터 변경 감지
        if market_changes:
            alert = '⚠️ 거래소 파라미터 변경 감지!\n'
            for ch in market_changes:
                alert += f'- {ch["param"]}: {ch["old"]} → {ch["new"]}\n'
            alert += '\n즉시 확인 필요'
            _send_telegram(alert)

        print(report, flush=True)
    except Exception as e:
        _log(f'audit error: {e}')
        traceback.print_exc()
    finally:
        conn.close()


def main():
    if '--check-now' in sys.argv:
        run_audit()
    else:
        # Daemon mode: run every AUDIT_INTERVAL_DAYS
        _log(f'daemon mode: interval={AUDIT_INTERVAL_DAYS} days')
        while True:
            try:
                run_audit()
            except Exception as e:
                _log(f'daemon error: {e}')
            sleep_sec = AUDIT_INTERVAL_DAYS * 86400
            _log(f'next audit in {AUDIT_INTERVAL_DAYS} days')
            time.sleep(sleep_sec)


if __name__ == '__main__':
    main()
