# Source Generated with Decompyle++
# File: local_query_executor.cpython-312.pyc (Python 3.12)

'''
Execute local queries that require NO LLM calls.
All data comes from DB, ccxt API, or systemd.
'''
import os
import re
import subprocess
from db_config import get_conn, DB_CONFIG
SYMBOL = os.getenv('SYMBOL', 'BTC/USDT:USDT')
APP_DIR = '/root/trading-bot/app'
REQUIRED_SERVICES = [
    'candles',
    'executor',
    'indicators',
    'news_bot',
    'pnl_watcher']
OPTIONAL_SERVICES = [
    'signal_logger',
    'vol_profile',
    'error_watcher']
WATCHED_SERVICES = REQUIRED_SERVICES + OPTIONAL_SERVICES
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
    return get_conn(autocommit=True)


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
        'claude_audit': _claude_audit,
        'macro_summary': _macro_summary,
        'db_monthly_stats': _db_monthly_stats,
        'audit_report': _audit_report}
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


def _classify_service_state(line, found):
    """Classify service state to OK/DOWN/UNKNOWN."""
    if not found:
        return 'UNKNOWN'
    ll = line.lower()
    if 'active running' in ll:
        return 'OK'
    if 'failed' in ll or 'dead' in ll:
        return 'DOWN'
    if 'inactive' in ll:
        return 'DOWN'
    if 'masked' in ll or 'activating' in ll:
        return 'UNKNOWN'
    return 'UNKNOWN'

STATE_ICONS = {'OK': '✔', 'DOWN': '❌', 'UNKNOWN': '❓'}
STATE_KR = {'OK': '정상', 'DOWN': '중지', 'UNKNOWN': '미확인'}


def _health_check(_text=None):
    (rc, out) = _run([
        'systemctl',
        'list-units',
        '--type=service'])
    if rc != 0:
        return '⚠ 서비스 상태 조회 실패'
    status_lines = []
    states = {}  # svc -> state
    ok_count = 0
    down_count = 0
    unknown_count = 0
    for svc in WATCHED_SERVICES:
        ko = SERVICE_NAMES_KO.get(svc, svc)
        found = False
        matched_line = ''
        for line in out.splitlines():
            if f'{svc}.service' not in line:
                continue
            found = True
            matched_line = line
            break
        state = _classify_service_state(matched_line, found)
        states[svc] = state
        icon = STATE_ICONS[state]
        state_kr = STATE_KR[state]
        is_required = svc in REQUIRED_SERVICES
        req_tag = '' if is_required else ' (선택)'
        if state == 'OK':
            status_lines.append(f'  {icon} {svc} ({ko}) — {state_kr}{req_tag}')
            ok_count += 1
        elif state == 'DOWN':
            detail = '오류' if found and 'failed' in matched_line.lower() else '중지'
            status_lines.append(f'  {icon} {svc} ({ko}) — {detail}{req_tag}')
            down_count += 1
        else:
            detail = '미등록' if not found else '미확인'
            status_lines.append(f'  {icon} {svc} ({ko}) — {detail}{req_tag}')
            unknown_count += 1
    total = len(WATCHED_SERVICES)
    header = [
        '🩺 서비스 상태 요약',
        f'  전체: {total}개 | 정상: {ok_count} | 중지: {down_count} | 미확인: {unknown_count}',
        '']
    # 경고 메시지
    req_down = [s for s in REQUIRED_SERVICES if states.get(s) == 'DOWN']
    req_unknown = [s for s in REQUIRED_SERVICES if states.get(s) == 'UNKNOWN']
    warnings = []
    if req_down:
        warnings.append(f'⚠ 필수 서비스 중지: {", ".join(req_down)}')
    if len(req_unknown) >= 2:
        warnings.append(f'⚠ 필수 서비스 미확인 {len(req_unknown)}개: {", ".join(req_unknown)}')
    if warnings:
        status_lines.append('')
        status_lines.extend(warnings)
    # DB 기록
    _log_service_health(states)
    return '\n'.join(header + status_lines)


def _log_service_health(states):
    """service_health_log 테이블에 현재 상태 기록."""
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            for svc, state in states.items():
                cur.execute("""
                    INSERT INTO service_health_log (service, state)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING;
                """, (svc, state))
        conn.commit()
    except Exception:
        pass  # DB 미생성 시 무시
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def get_service_health_summary():
    """safety_manager에서 호출: 현재 서비스 상태 요약 반환.
    Returns dict: {'ok': int, 'down': list, 'unknown': list, 'required_down': list}
    """
    (rc, out) = _run([
        'systemctl',
        'list-units',
        '--type=service'])
    if rc != 0:
        return {'ok': 0, 'down': [], 'unknown': WATCHED_SERVICES[:], 'required_down': []}
    states = {}
    for svc in WATCHED_SERVICES:
        found = False
        matched_line = ''
        for line in out.splitlines():
            if f'{svc}.service' not in line:
                continue
            found = True
            matched_line = line
            break
        states[svc] = _classify_service_state(matched_line, found)
    ok = [s for s, st in states.items() if st == 'OK']
    down = [s for s, st in states.items() if st == 'DOWN']
    unknown = [s for s, st in states.items() if st == 'UNKNOWN']
    req_down = [s for s in REQUIRED_SERVICES if states.get(s) == 'DOWN']
    req_unknown = [s for s in REQUIRED_SERVICES if states.get(s) == 'UNKNOWN']
    return {
        'ok': len(ok),
        'down': down,
        'unknown': unknown,
        'required_down': req_down,
        'required_unknown': req_unknown,
    }


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
    env['DATABASE_URL'] = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
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
        ne_details = ne_detail.get('details', {})
        weights = r.get('weights', {})
        total = r.get('total_score', 0)
        tech = r.get('tech_score', 0)
        pos = r.get('position_score', 0)
        regime = r.get('regime_score', 0)
        dominant = r.get('dominant_side', '?')
        stage = r.get('stage', '?')
        tech_w = weights.get('tech_w', 0.45)
        pos_w = weights.get('position_w', 0.25)
        regime_w = weights.get('regime_w', 0.25)
        news_w = weights.get('news_event_w', 0.05)
        # 축별 가중 기여도
        tech_c = tech * tech_w
        pos_c = pos * pos_w
        regime_c = regime * regime_w
        news_c = ne * news_w
        lines = [
            f"📊 스코어 엔진 (4축)",
            f"━━━━━━━━━━━━━━━━━━",
            f"총점: {total:+.1f} → {dominant} (stage {stage})",
            f"",
            f"기술(TECH):   {tech:+.0f} × {tech_w} = {tech_c:+.1f}",
            f"포지션(POS):  {pos:+.0f} × {pos_w} = {pos_c:+.1f}",
            f"레짐(REG):    {regime:+.0f} × {regime_w} = {regime_c:+.1f}",
            f"뉴스(NEWS):   {ne:+.0f} × {news_w} = {news_c:+.1f}{' [차단됨]' if guarded else ''}",
            f"",
            f"엔진권고: {dominant} stg{stage} (총점 {total:+.1f})",
        ]
        # 현재 포지션 정보
        try:
            pos_info = _position_info()
            lines.append(f"현재포지션: {pos_info.replace('📍 ', '')}")
        except Exception:
            pass
        if guarded:
            lines.append(f"  ⚠ {dominant} 권고이나, TECH/POS 중립으로 뉴스 단독 차단")
        lines.append(f"")
        lines.append(f"뉴스 이벤트 내역:")
        lines.append(f"  소스품질: {ne_comp.get('source_quality', 0):.1f}/20")
        lines.append(f"  카테고리: {ne_comp.get('category_weight', 0):.1f}/25")
        lines.append(f"  최신성: {ne_comp.get('recency', 0):.1f}/15")
        lines.append(f"  시장반응: {ne_comp.get('market_reaction', 0):.1f}/25")
        lines.append(f"  키워드: {ne_comp.get('watchlist', 0):.1f}/15")
        dir_sign = ne_details.get('direction_sign', ne_details.get('macro_bonus', '?'))
        score_trace = ne_details.get('score_trace', '')
        if score_trace:
            lines.append(f"  추적: {score_trace}")
        lines.append(f"")
        lines.append(f"손절: {r.get('dynamic_stop_loss_pct', 2.0)}%")
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


def _audit_report(_text=None):
    """종합 감사 리포트: 차트흐름 + 이벤트 + 결정 + 실행 + 뉴스기여 + 규칙위반."""
    conn = None
    try:
        conn = _db()
        lines = ['📋 종합 감사 리포트']
        lines.append('━━━━━━━━━━━━━━━━━━')
        lines.append('⚠ 즉시 적용 금지 — 분석 자료')
        lines.append('')

        with conn.cursor() as cur:
            # 1. 차트 흐름
            lines.append('[1. 차트 흐름]')
            try:
                from news_strategy_report import _fetch_chart_flow
                chart = _fetch_chart_flow(cur)
                lines.append(f'  4h 추세: {chart.get("trend_4h", "?")}({chart.get("trend_4h_pct", 0):+.1f}%)')
                lines.append(f'  12h 추세: {chart.get("trend_12h", "?")}({chart.get("trend_12h_pct", 0):+.1f}%)')
                lines.append(f'  BB: {chart.get("bb_position", "?")} | Ichimoku: {chart.get("ichimoku_cloud", "?")}')
            except Exception as e:
                lines.append(f'  조회 실패: {e}')

            # 2. 이벤트 타임라인 (최근 24h)
            lines.append('')
            lines.append('[2. 이벤트 타임라인 (24h)]')
            try:
                cur.execute("""
                    SELECT to_char(ts AT TIME ZONE 'Asia/Seoul', 'MM-DD HH24:MI'),
                           mode, triggers, snapshot_price
                    FROM event_trigger_log
                    WHERE ts >= now() - interval '24 hours'
                    ORDER BY ts DESC LIMIT 10;
                """)
                evt_rows = cur.fetchall()
                if evt_rows:
                    for r in evt_rows:
                        triggers = r[2] or ''
                        if isinstance(triggers, (list, dict)):
                            import json
                            triggers = json.dumps(triggers, ensure_ascii=False)[:80]
                        lines.append(f'  {r[0]} [{r[1]}] {str(triggers)[:60]} price={r[3] or "?"}')
                else:
                    lines.append('  이벤트 없음')
            except Exception:
                lines.append('  조회 실패')

            # 3. 결정 로그 (최근 24h)
            lines.append('')
            lines.append('[3. 결정 로그 (24h)]')
            try:
                cur.execute("""
                    SELECT to_char(ts AT TIME ZONE 'Asia/Seoul', 'MM-DD HH24:MI'),
                           final_action, position_side, action_reason
                    FROM pm_decision_log
                    WHERE ts >= now() - interval '24 hours'
                    ORDER BY ts DESC LIMIT 10;
                """)
                dec_rows = cur.fetchall()
                if dec_rows:
                    for r in dec_rows:
                        lines.append(f'  {r[0]} {r[1] or "?"} {r[2] or ""} — {(r[3] or "")[:60]}')
                else:
                    lines.append('  결정 없음')
            except Exception:
                lines.append('  조회 실패')

            # 4. 실행 로그 (최근 24h)
            lines.append('')
            lines.append('[4. 실행 로그 (24h)]')
            try:
                cur.execute("""
                    SELECT to_char(ts AT TIME ZONE 'Asia/Seoul', 'MM-DD HH24:MI'),
                           order_type, direction, status,
                           filled_qty, avg_fill_price, realized_pnl
                    FROM execution_log
                    WHERE ts >= now() - interval '24 hours'
                    ORDER BY ts DESC LIMIT 10;
                """)
                exec_rows = cur.fetchall()
                if exec_rows:
                    for r in exec_rows:
                        pnl_str = f' PnL={float(r[6]):+.4f}' if r[6] else ''
                        lines.append(f'  {r[0]} {r[1]} {r[2] or ""} [{r[3]}]{pnl_str}')
                else:
                    lines.append('  실행 없음')
            except Exception:
                lines.append('  조회 실패')

            # 5. 뉴스 기여 (당일 전략 반영된 뉴스)
            lines.append('')
            lines.append('[5. 전략 반영 뉴스 (당일)]')
            try:
                cur.execute("""
                    SELECT title_ko, tier, impact_score, relevance_score, source
                    FROM news
                    WHERE ts >= date_trunc('day', now() AT TIME ZONE 'Asia/Seoul')
                      AND exclusion_reason IS NULL
                      AND COALESCE(tier, 'UNKNOWN') NOT IN ('TIERX')
                      AND impact_score >= 3
                    ORDER BY impact_score DESC
                    LIMIT 5;
                """)
                news_rows = cur.fetchall()
                if news_rows:
                    for i, r in enumerate(news_rows, 1):
                        title = (r[0] or '?')[:50]
                        lines.append(f'  {i}) [{r[1]}] ({r[2]}/10) {title} rel={r[3] or "?"}')
                else:
                    lines.append('  반영 뉴스 없음')
            except Exception:
                lines.append('  조회 실패')

            # 6. 규칙 위반 / 안전 차단 이력
            lines.append('')
            lines.append('[6. 안전 차단 이력 (24h)]')
            try:
                # error_log가 없을 수 있으므로 테이블 존재 확인
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_name = 'error_log' AND table_schema = 'public'
                    );
                """)
                has_error_log = cur.fetchone()[0]
                if has_error_log:
                    cur.execute("""
                        SELECT to_char(ts AT TIME ZONE 'Asia/Seoul', 'MM-DD HH24:MI'),
                               service, level, message
                        FROM error_log
                        WHERE ts >= now() - interval '24 hours'
                          AND (level IN ('CRITICAL', 'WARNING')
                               OR message ILIKE '%%block%%'
                               OR message ILIKE '%%차단%%')
                        ORDER BY ts DESC LIMIT 5;
                    """)
                    err_rows = cur.fetchall()
                    if err_rows:
                        for r in err_rows:
                            lines.append(f'  {r[0]} [{r[2]}] {r[1]}: {(r[3] or "")[:80]}')
                    else:
                        lines.append('  차단/경고 이력 없음')
                else:
                    # error_log 테이블 미생성 → safety_manager 차단 이력으로 대체
                    cur.execute("""
                        SELECT to_char(ts AT TIME ZONE 'Asia/Seoul', 'MM-DD HH24:MI'),
                               final_action, action_reason
                        FROM pm_decision_log
                        WHERE ts >= now() - interval '24 hours'
                          AND final_action IN ('HOLD', 'ABORT', 'SKIP')
                        ORDER BY ts DESC LIMIT 5;
                    """)
                    hold_rows = cur.fetchall()
                    if hold_rows:
                        for r in hold_rows:
                            lines.append(f'  {r[0]} {r[1]} — {(r[2] or "")[:60]}')
                    else:
                        lines.append('  차단/HOLD 이력 없음')
            except Exception:
                lines.append('  조회 실패')

            # 7. 17:30 청산 상태
            lines.append('')
            lines.append('[7. 17:30 청산 상태]')
            try:
                cur.execute("""
                    SELECT to_char(ts AT TIME ZONE 'Asia/Seoul', 'MM-DD HH24:MI'),
                           status, close_reason
                    FROM execution_log
                    WHERE order_type = 'SCHEDULED_CLOSE'
                      AND ts >= now() - interval '48 hours'
                    ORDER BY ts DESC LIMIT 3;
                """)
                sched_rows = cur.fetchall()
                if sched_rows:
                    for r in sched_rows:
                        lines.append(f'  {r[0]} [{r[1]}] {r[2] or ""}')
                else:
                    lines.append('  최근 48시간 예약 청산 기록 없음')
            except Exception:
                lines.append('  조회 실패')

        lines.append('')
        lines.append('⚠ 즉시 적용 금지 — 분석 자료')
        return '\n'.join(lines)
    except Exception as e:
        return f'감사 리포트 조회 실패: {e}'
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


def _macro_summary(_text=None):
    """매크로/거시경제 지표 최신 값 조회."""
    try:
        conn = _db()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (source) source, price, ts
                FROM macro_data
                ORDER BY source, ts DESC;
            """)
            rows = cur.fetchall()
        conn.close()
        if not rows:
            return '📊 매크로 데이터 없음'
        lines = ['📊 거시경제 지표 현황']
        source_kr = {
            'QQQ': 'QQQ(나스닥 추종)',
            'SPY': 'SPY(S&P500)',
            'DXY': 'DXY(달러 인덱스)',
            'US10Y': 'US10Y(미국 10년물)',
            'VIX': 'VIX(공포 지수)',
        }
        for row in rows:
            src = row[0]
            price = float(row[1]) if row[1] else 0
            ts = str(row[2])[:16] if row[2] else '?'
            label = source_kr.get(src, src)
            lines.append(f'- {label}: {price:,.2f} ({ts})')
        return '\n'.join(lines)
    except Exception as e:
        return f'⚠ 매크로 조회 오류: {e}'


def _db_monthly_stats(_text=None):
    """월별 데이터 저장량 리포트."""
    try:
        conn = _db()
        lines = ['📊 DB 월별 통계']
        lines.append('━━━━━━━━━━━━━━━━━━')

        # Section 1: Cross-table summary (total, 24h, date range)
        with conn.cursor() as cur:
            summary_query = """
                SELECT 'news' as tbl, COUNT(*) as total,
                       COUNT(*) FILTER (WHERE ts >= now() - interval '24 hours') as last_24h,
                       MIN(ts) as earliest, MAX(ts) as latest
                FROM news
                UNION ALL
                SELECT 'candles', COUNT(*),
                       COUNT(*) FILTER (WHERE ts >= now() - interval '24 hours'),
                       MIN(ts), MAX(ts)
                FROM candles
                UNION ALL
                SELECT 'macro_data', COUNT(*),
                       COUNT(*) FILTER (WHERE ts >= now() - interval '24 hours'),
                       MIN(ts), MAX(ts)
                FROM macro_data
                UNION ALL
                SELECT 'execution_log', COUNT(*),
                       COUNT(*) FILTER (WHERE ts >= now() - interval '24 hours'),
                       MIN(ts), MAX(ts)
                FROM execution_log
                UNION ALL
                SELECT 'score_history', COUNT(*),
                       COUNT(*) FILTER (WHERE ts >= now() - interval '24 hours'),
                       MIN(ts), MAX(ts)
                FROM score_history;
            """
            try:
                cur.execute(summary_query)
                rows = cur.fetchall()
                lines.append('\n[테이블 요약]')
                for row in rows:
                    tbl, total, last_24h, earliest, latest = row
                    earliest_str = str(earliest)[:16] if earliest else '-'
                    latest_str = str(latest)[:16] if latest else '-'
                    lines.append(f'  {tbl}: {total:,}건 (24h: {last_24h:,}건)')
                    lines.append(f'    {earliest_str} ~ {latest_str}')
            except Exception as e:
                lines.append(f'\n[테이블 요약] 조회 실패: {e}')

            # macro_events summary (new table)
            try:
                cur.execute("""
                    SELECT COUNT(*),
                           MIN(event_date)::text,
                           MAX(event_date)::text
                    FROM macro_events;
                """)
                row = cur.fetchone()
                if row and row[0]:
                    lines.append(f'  macro_events: {row[0]:,}건')
                    lines.append(f'    {(row[1] or "-")} ~ {(row[2] or "-")}')
                else:
                    lines.append('  macro_events: 0건')
            except Exception:
                lines.append('  macro_events: 테이블 미생성')

        # Section 2: Monthly breakdown per table
        tables = [
            ('candles', 'ts'),
            ('news', 'ts'),
            ('indicators', 'ts'),
            ('events', 'start_ts'),
            ('pm_decision_log', 'ts'),
            ('macro_data', 'ts'),
            ('score_history', 'ts'),
            ('macro_events', 'event_date'),
        ]
        lines.append('\n[월별 데이터량]')
        with conn.cursor() as cur:
            for table, ts_col in tables:
                try:
                    cur.execute(f"""
                        SELECT date_trunc('month', {ts_col}) AS month,
                               count(*) AS cnt
                        FROM {table}
                        GROUP BY month
                        ORDER BY month DESC
                        LIMIT 6;
                    """)
                    rows = cur.fetchall()
                    lines.append(f'\n  [{table}]')
                    if not rows:
                        lines.append('    데이터 없음')
                    else:
                        for row in rows:
                            month_str = str(row[0])[:7] if row[0] else '?'
                            lines.append(f'    {month_str}: {row[1]:,}건')
                except Exception:
                    lines.append(f'\n  [{table}] 조회 실패')
        conn.close()
        return '\n'.join(lines)
    except Exception as e:
        return f'⚠ 월별 통계 오류: {e}'
