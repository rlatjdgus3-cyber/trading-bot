# Source Generated with Decompyle++
# File: local_query_executor.cpython-312.pyc (Python 3.12)

'''
Execute local queries that require NO LLM calls.
All data comes from DB, ccxt API, or systemd.
'''
import os
import re
import subprocess
import psycopg2
from dotenv import load_dotenv
load_dotenv('/root/trading-bot/app/.env')
SYMBOL = os.getenv('SYMBOL', 'BTC/USDT:USDT')
APP_DIR = '/root/trading-bot/app'
DB = dict(host=os.getenv('DB_HOST', 'localhost'), port=int(os.getenv('DB_PORT', '5432')), dbname=os.getenv('DB_NAME', 'trading'), user=os.getenv('DB_USER', 'bot'), password=os.getenv('DB_PASS', 'botpass'), connect_timeout=10, options='-c statement_timeout=30000')
WATCHED_SERVICES = [
    'candles',
    'executor',
    'indicators',
    'news_bot',
    'signal_logger',
    'vol_profile',
    'error_watcher',
    'pnl_watcher']
SERVICE_NAMES_KO = {
    'candles': '캔들 수집',
    'executor': '실행기(컨트롤러)',
    'indicators': '지표 계산',
    'news_bot': '뉴스 수집',
    'signal_logger': '시그널 기록',
    'vol_profile': '볼륨 프로파일',
    'error_watcher': '에러 감시',
    'pnl_watcher': '손익 감시'}

def _db():
    conn = psycopg2.connect(**DB)
    conn.autocommit = True
    return conn


def _run(cmd, timeout=25):
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    out = (p.stdout or '') + (p.stderr or '')
    return (p.returncode, out.strip())


def execute(query_type=None, original_text=None):
    handlers = {
        'status_full': _status_full,
        'health_check': _health_check,
        'btc_price': _btc_price,
        'news_summary': _news_summary,
        'equity_report': _equity_report,
        'daily_report': _daily_report,
        'recent_errors': _recent_errors,
        'indicator_snapshot': _indicator_snapshot,
        'volatility_summary': _volatility_summary,
        'position_info': _position_info,
        'score_summary': _score_summary,
        'db_health': _db_health,
        'claude_audit': _claude_audit}
    handler = handlers.get(query_type, _unknown)
    return handler(original_text)


def _status_full(_text=None):
    (rc, out) = _run([
        'python3',
        f'{APP_DIR}/status_full.py'], timeout=35)
    if rc != 0:
        return f'⚠ status_full 실패(rc={rc})\n{out[-3500:]}'
    if len(out) > 3500:
        return out[-3500:]
    return out


def _health_check(_text=None):
    (rc, out) = _run([
        'systemctl',
        'list-units',
        '--type=service'])
    if rc != 0:
        return '⚠ 서비스 상태 조회 실패'
    status_lines = []
    active_count = 0
    for svc in WATCHED_SERVICES:
        ko = SERVICE_NAMES_KO.get(svc, svc)
        found = False
        for line in out.splitlines():
            if f'{svc}.service' not in line:
                continue
            found = True
            ll = line.lower()
            if 'active running' in ll:
                status_lines.append(f'  ✔ {svc} ({ko}) — 정상 실행 중')
                active_count += 1
            elif 'inactive' in ll or 'dead' in ll:
                status_lines.append(f'  ❌ {svc} ({ko}) — 중지 상태')
            elif 'failed' in ll:
                status_lines.append(f'  🚨 {svc} ({ko}) — 오류 상태')
            elif 'masked' in ll:
                status_lines.append(f'  ⚠ {svc} ({ko}) — 마스킹됨 (실행 차단)')
            elif 'activating' in ll:
                status_lines.append(f'  ⏳ {svc} ({ko}) — 시작 중')
            else:
                status_lines.append(f'  ❓ {svc} ({ko}) — 상태 미확인')
            break
        if not found:
            status_lines.append(f'  ❓ {svc} ({ko}) — 미등록')
    total = len(WATCHED_SERVICES)
    header = [
        '🩺 서비스 상태 요약',
        f'  전체: {total}개 중 {active_count}개 정상',
        '']
    return '\n'.join(header + status_lines)


def _btc_price(_text=None):
    import ccxt
    ex = ccxt.bybit({
        'enableRateLimit': True})
    t = ex.fetch_ticker('BTC/USDT')
    last = t.get('last')
    ts = t.get('datetime', '')
    return f'BTC/USDT 현재가: {last} (bybit) {ts}'


def _news_summary(text=None):
    (minutes, limit) = _parse_minutes_and_limit(text)
    env = os.environ.copy()
    env['DATABASE_URL'] = f"postgresql://{DB['user']}:{DB['password']}@{DB['host']}:{DB['port']}/{DB['dbname']}"
    env['NEWS_SUMMARY_MINUTES'] = str(minutes)
    env['NEWS_SUMMARY_LIMIT'] = str(limit)
    p = subprocess.run([
        'python3',
        f'{APP_DIR}/news_bot.py',
        '--summary'], capture_output=True, text=True, timeout=25, env=env)
    out = (p.stdout or '').strip()
    err = (p.stderr or '').strip()
    if p.returncode != 0:
        return f'DB 뉴스 요약 실패(rc={p.returncode})\n{(err or out)[-3500:]}'
    if out:
        return out[:3500]
    return '뉴스 데이터 없음'


def _equity_report(_text=None):
    (rc, out) = _run([
        'python3',
        f'{APP_DIR}/equity_report.py'], timeout=35)
    if rc != 0:
        return f'equity_report 실패(rc={rc})\n{out[-3500:]}'
    if len(out) > 3500:
        return out[-3500:]
    return out


def _daily_report(_text=None):
    (rc, out) = _run([
        'python3',
        f'{APP_DIR}/daily_report.py'], timeout=35)
    if rc != 0:
        return f'daily_report 실패(rc={rc})\n{out[-3500:]}'
    if len(out) > 3500:
        return out[-3500:]
    return out


def _recent_errors(_text=None):
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ts, service, message
                FROM error_log
                ORDER BY ts DESC
                LIMIT 10;
            """)
            rows = cur.fetchall()
        if not rows:
            return '최근 에러 없음'
        lines = ['🚨 최근 에러 목록']
        for ts, svc, msg in rows:
            lines.append(f'  [{ts}] {svc}: {msg[:200]}')
        return '\n'.join(lines)
    except Exception as e:
        return f'에러 조회 실패: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _indicator_snapshot(_text=None):
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            # BTC current price from latest candle
            cur.execute("""
                SELECT c FROM candles
                WHERE symbol = %s AND tf = '1m'
                ORDER BY ts DESC LIMIT 1;
            """, (SYMBOL,))
            price_row = cur.fetchone()
            btc_price = float(price_row[0]) if price_row else None

            cur.execute("""
                SELECT ts, rsi_14, atr_14, bb_up, bb_mid, bb_dn,
                       ich_tenkan, ich_kijun, vol_spike, ma_50, ma_200
                FROM indicators
                WHERE symbol = %s AND tf = '1m'
                ORDER BY ts DESC LIMIT 1;
            """, (SYMBOL,))
            row = cur.fetchone()
        if not row:
            return '지표 데이터 없음'
        (ts, rsi, atr, bb_up, bb_mid, bb_dn, ich_t, ich_k, vol_spike,
         ma_50, ma_200) = row
        lines = [
            f'📊 지표 스냅샷 ({ts})',
            f'  BTC 현재가: ${btc_price:,.1f}' if btc_price else '  BTC 현재가: N/A',
            f'  RSI(14): {rsi}',
            f'  ATR(14): {atr}',
            f'  BB: upper={bb_up} mid={bb_mid} lower={bb_dn}',
            f'  Ichimoku: tenkan={ich_t} kijun={ich_k}',
            f'  MA: 50={ma_50} 200={ma_200}',
            f'  Volume spike: {"YES" if vol_spike else "NO"}',
        ]
        return '\n'.join(lines)
    except Exception as e:
        return f'지표 조회 실패: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _volatility_summary(_text=None):
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ts, atr_14, bb_up - bb_dn AS bb_width,
                       vol_spike
                FROM indicators
                WHERE symbol = %s AND tf = '1m'
                ORDER BY ts DESC LIMIT 5;
            """, (SYMBOL,))
            rows = cur.fetchall()
        if not rows:
            return '변동성 데이터 없음'
        lines = ['📈 변동성 요약 (최근 5건)']
        for ts, atr, bb_w, vs in rows:
            lines.append(f'  [{ts}] ATR={atr} BB폭={bb_w} spike={"Y" if vs else "N"}')
        return '\n'.join(lines)
    except Exception as e:
        return f'변동성 조회 실패: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _position_info(_text=None):
    parts = []
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT side, qty, avg_entry, symbol
                FROM dry_run_positions
                WHERE symbol = %s
                LIMIT 1;
            """, (SYMBOL,))
            row = cur.fetchone()
        if row:
            parts.append(f'📍 포지션: {row[0]} qty={row[1]} entry={row[2]} ({row[3]})')
        else:
            parts.append('📍 포지션 없음')
    except Exception as e:
        parts.append(f'포지션 조회 실패: {e}')
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
    return '\n'.join(parts) if parts else '포지션 정보 없음'


def _score_summary(_text=None):
    try:
        import score_engine
        r = score_engine.compute_total()
        ne = r.get('news_event_score', 0)
        guarded = r.get('news_event_guarded', False)
        ne_detail = r.get('axis_details', {}).get('news_event', {})
        ne_comp = ne_detail.get('components', {})
        cats = ne_detail.get('details', {}).get('category', {}).get('active_categories', [])
        lines = [
            f"📊 Score Engine (4-axis)",
            f"━━━━━━━━━━━━━━━━━━",
            f"TOTAL: {r.get('total_score', 0):+.1f} → {r.get('dominant_side', '?')} (stage {r.get('stage', '?')})",
            f"",
            f"TECH:      {r.get('tech_score', 0):+.0f}  (w=0.45)",
            f"POSITION:  {r.get('position_score', 0):+.0f}  (w=0.25)",
            f"REGIME:    {r.get('regime_score', 0):+.0f}  (w=0.25)",
            f"NEWS_EVENT:{ne:+.0f}  (w=0.05){' [GUARDED]' if guarded else ''}",
            f"",
            f"News Event 내역:",
            f"  sentiment: {ne_comp.get('recent_sentiment', 0):+d}",
            f"  category_bias: {ne_comp.get('category_bias', 0):+d}",
            f"  event_sim: {ne_comp.get('event_similarity', 0):+d}",
        ]
        if cats:
            lines.append(f"  active: {', '.join(cats[:5])}")
        if guarded:
            lines.append(f"  (TECH/POS 중립 → 뉴스 단독 판단 차단)")
        lines.append(f"")
        lines.append(f"Stop-Loss: {r.get('dynamic_stop_loss_pct', 2.0)}%")
        lines.append(f"BTC: {r.get('price', '?')}")
        return '\n'.join(lines)
    except Exception as e:
        return f'스코어 조회 실패: {e}'


def _db_health(_text=None):
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            tables = [
                ('candles', 'ts'),
                ('news', 'ts'),
                ('indicators', 'ts'),
                ('events', 'start_ts'),
                ('pm_decision_log', 'ts'),
                ('execution_log', 'ts'),
                ('score_history', 'ts'),
                ('macro_data', 'ts'),
            ]
            lines = ['🗄 DB 상태 점검']
            lines.append('━━━━━━━━━━━━━━━━━━')
            for tbl, ts_col in tables:
                try:
                    cur.execute(f"""
                        SELECT count(*),
                               min({ts_col})::text,
                               max({ts_col})::text,
                               count(*) FILTER (WHERE {ts_col} >= now() - interval '24 hours')
                        FROM {tbl};
                    """)
                    row = cur.fetchone()
                    total, min_ts, max_ts, recent = row
                    min_ts = (min_ts or '')[:16]
                    max_ts = (max_ts or '')[:16]
                    lines.append(f'  {tbl}: {total:,}건 (24h: {recent:,}건)')
                    lines.append(f'    범위: {min_ts} ~ {max_ts}')
                except Exception as e:
                    lines.append(f'  {tbl}: 조회 실패 ({e})')

            # news_impact_stats
            lines.append('')
            lines.append('[뉴스 영향 통계]')
            try:
                cur.execute("""
                    SELECT stats_version, count(*), sum(sample_count)
                    FROM news_impact_stats
                    GROUP BY stats_version
                    ORDER BY stats_version DESC LIMIT 1;
                """)
                row = cur.fetchone()
                if row:
                    lines.append(f'  버전: {row[0]} | 카테고리: {row[1]}개 | 샘플: {row[2]:,}건')
                else:
                    lines.append('  데이터 없음 (compute_news_impact_stats.py 실행 필요)')
            except Exception:
                lines.append('  테이블 미생성')

            # regime_correlation
            lines.append('')
            lines.append('[BTC-QQQ 상관관계]')
            try:
                import regime_correlation
                info = regime_correlation.get_correlation_info(cur)
                regime = info.get('regime', '?')
                corr = info.get('correlation')
                age = info.get('cache_age_sec')
                corr_str = f'{corr:.4f}' if corr is not None else 'N/A'
                age_str = f'{age}초 전' if age is not None else 'N/A'
                lines.append(f'  레짐: {regime} | 상관계수: {corr_str} | 캐시: {age_str}')
            except Exception as e:
                lines.append(f'  조회 실패 ({e})')

        return '\n'.join(lines)
    except Exception as e:
        return f'DB 상태 조회 실패: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _claude_audit(_text=None):
    """Claude API 사용량 감사 리포트."""
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            lines = ['🧠 Claude 사용량 감사']
            lines.append('━━━━━━━━━━━━━━━━━━')

            # Today's stats from claude_call_log
            cur.execute("""
                SELECT count(*),
                       coalesce(sum(estimated_cost), 0),
                       coalesce(sum(input_tokens), 0),
                       coalesce(sum(output_tokens), 0)
                FROM claude_call_log
                WHERE ts >= date_trunc('day', now() AT TIME ZONE 'Asia/Seoul');
            """)
            row = cur.fetchone()
            today_calls = row[0] or 0
            today_cost = float(row[1] or 0)
            today_input = row[2] or 0
            today_output = row[3] or 0
            lines.append(f'\n[오늘 사용량]')
            lines.append(f'  호출: {today_calls}건 | 비용: ${today_cost:.4f}')
            lines.append(f'  입력 토큰: {today_input:,} | 출력 토큰: {today_output:,}')

            # By gate_type today
            cur.execute("""
                SELECT gate_type, count(*), coalesce(sum(estimated_cost), 0)
                FROM claude_call_log
                WHERE ts >= date_trunc('day', now() AT TIME ZONE 'Asia/Seoul')
                GROUP BY gate_type ORDER BY count(*) DESC;
            """)
            gate_rows = cur.fetchall()
            if gate_rows:
                lines.append(f'\n[게이트별 분류]')
                for gr in gate_rows:
                    lines.append(f'  {gr[0] or "?"}: {gr[1]}건 (${float(gr[2]):.4f})')

            # Monthly stats
            cur.execute("""
                SELECT count(*),
                       coalesce(sum(estimated_cost), 0)
                FROM claude_call_log
                WHERE ts >= date_trunc('month', now());
            """)
            row = cur.fetchone()
            month_calls = row[0] or 0
            month_cost = float(row[1] or 0)
            lines.append(f'\n[이번 달 누적]')
            lines.append(f'  호출: {month_calls}건 | 비용: ${month_cost:.4f}')

            # Budget remaining (from claude_gate)
            try:
                import claude_gate
                lines.append(f'\n[예산 한도]')
                lines.append(f'  일일 호출 한도: {claude_gate.DAILY_CALL_LIMIT}')
                lines.append(f'  일일 비용 한도: ${claude_gate.DAILY_COST_LIMIT}')
                lines.append(f'  월간 비용 한도: ${claude_gate.MONTHLY_COST_LIMIT}')
                remaining_calls = max(0, claude_gate.DAILY_CALL_LIMIT - today_calls)
                remaining_cost = max(0, claude_gate.DAILY_COST_LIMIT - today_cost)
                lines.append(f'  남은 호출: {remaining_calls}건 | 남은 비용: ${remaining_cost:.2f}')
            except Exception:
                pass

            # Recent 5 calls
            cur.execute("""
                SELECT to_char(ts AT TIME ZONE 'Asia/Seoul', 'HH24:MI'),
                       gate_type, call_type,
                       action_result, estimated_cost
                FROM claude_call_log
                ORDER BY ts DESC LIMIT 5;
            """)
            recent = cur.fetchall()
            if recent:
                lines.append(f'\n[최근 호출 5건]')
                for r in recent:
                    cost_str = f'${float(r[4]):.4f}' if r[4] else '$0'
                    lines.append(f'  {r[0]} {r[1] or "?"}/{r[2] or "?"} '
                                 f'→ {r[3] or "?"} ({cost_str})')

        return '\n'.join(lines)
    except Exception as e:
        return f'Claude 감사 조회 실패: {e}'
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _unknown(_text=None):
    return '알 수 없는 조회 유형입니다. /help 을 참고하세요.'


def _parse_minutes_and_limit(text=None):
    '''Migrated from telegram_cmd_poller.py.'''
    t = (text or '').strip().lower()
    minutes = 1440
    limit = 20
    if '오늘' in t:
        minutes = 1440
    if '최근' not in t and re.search('\\d+\\s*(분|min|minute|시간|h|hour)', t):
        minutes = 60
    m = re.search('(\\d+)\\s*(분|min|minute)', t)
    if m:
        minutes = int(m.group(1))
    h = re.search('(\\d+)\\s*(시간|h|hour)', t)
    if h:
        minutes = int(h.group(1)) * 60
    n = re.search('(\\d+)\\s*(개|건)', t)
    if n:
        limit = int(n.group(1))
    minutes = max(5, min(minutes, 10080))
    limit = max(1, min(limit, 50))
    return (minutes, limit)
