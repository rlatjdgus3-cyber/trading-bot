# Source Generated with Decompyle++
# File: strategy_report.cpython-312.pyc (Python 3.12)

'''
Strategy Report Auto-Generation.
Runs via systemd timer at 09:00 KST and 18:00 KST.
Gathers summary data from DB, makes ONE GPT call, sends to Telegram.
'''
import os
import sys
import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
load_dotenv('/root/trading-bot/app/.env')
from db_config import get_conn
SYMBOL = os.getenv('SYMBOL', 'BTC/USDT:USDT')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '')
MODEL = os.getenv('OPENAI_MODEL', 'gpt-4o-mini')
TG_ENV_PATH = '/root/trading-bot/app/telegram_cmd.env'

def _load_tg_env():
    (token, chat_id) = ('', '')
    try:
        with open(TG_ENV_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                k, v = line.split('=', 1)
                k = k.strip()
                v = v.strip()
                if k == 'TELEGRAM_BOT_TOKEN':
                    token = v
                elif k == 'TELEGRAM_ALLOWED_CHAT_ID':
                    chat_id = v
    except Exception:
        pass
    return (token, chat_id)


def _send_telegram(text=None):
    from report_formatter import korean_output_guard
    (token, chat_id) = _load_tg_env()
    if not token or not chat_id:
        print('[strategy_report] SKIP: telegram env missing', flush=True)
        return None
    s = korean_output_guard(text or '')
    chunks = []
    while len(s) > 3800:
        chunks.append(s[:3800])
        s = s[3800:]
    chunks.append(s)
    for c in chunks:
        url = f'https://api.telegram.org/bot{token}/sendMessage'
        data = urllib.parse.urlencode({
            'chat_id': chat_id,
            'text': c,
            'disable_web_page_preview': 'true'}).encode('utf-8')
        req = urllib.request.Request(url, data=data, method='POST')
        urllib.request.urlopen(req, timeout=20)
    return None


def _db():
    conn = get_conn(autocommit=True)
    conn.autocommit = True
    return conn


def gather_data():
    '''Collect summary data from DB. No AI calls.'''
    conn = _db()
    data = {}
    try:
        with conn.cursor() as cur:
            # Top news
            cur.execute("""
                SELECT COALESCE(title_ko, title) AS title, impact_score
                FROM news
                WHERE ts > now() - interval '12 hours'
                  AND impact_score >= 6
                ORDER BY impact_score DESC
                LIMIT 5;
            """)
            data['top_news'] = [{'title': r[0], 'score': r[1]} for r in cur.fetchall()]

            # Indicator snapshot
            cur.execute("""
                SELECT bb_up, bb_mid, bb_dn,
                       ich_tenkan, ich_kijun, vol_spike
                FROM indicators
                WHERE symbol = %s
                ORDER BY ts DESC LIMIT 1;
            """, (SYMBOL,))
            row = cur.fetchone()
            if row:
                data['indicator'] = {
                    'bb_up': str(row[0]), 'bb_mid': str(row[1]), 'bb_dn': str(row[2]),
                    'ich_tenkan': str(row[3]), 'ich_kijun': str(row[4]),
                    'vol_spike': bool(row[5]),
                }

            # Position
            cur.execute("""
                SELECT side, qty, avg_entry
                FROM dry_run_positions
                WHERE symbol = %s LIMIT 1;
            """, (SYMBOL,))
            pos = cur.fetchone()
            if pos:
                data['position'] = {'side': pos[0], 'qty': str(pos[1]), 'entry': str(pos[2])}

            # 1h range
            cur.execute("""
                SELECT MIN(l), MAX(h), (array_agg(c ORDER BY ts DESC))[1]
                FROM candles
                WHERE symbol = %s AND tf = '1m'
                  AND ts > now() - interval '1 hour';
            """, (SYMBOL,))
            pr = cur.fetchone()
            if pr and pr[0]:
                data['1h_range'] = {'low': str(pr[0]), 'high': str(pr[1]), 'last': str(pr[2])}

            # Macro data (QQQ, SPY, DXY, US10Y, VIX)
            try:
                cur.execute("""
                    SELECT DISTINCT ON (source) source, price
                    FROM macro_data
                    ORDER BY source, ts DESC;
                """)
                macro_rows = cur.fetchall()
                data['macro'] = {r[0]: float(r[1]) for r in macro_rows if r[1]}
            except Exception:
                pass

            # Score history (latest)
            try:
                cur.execute("""
                    SELECT total_score, dominant_side, computed_stage
                    FROM score_history
                    ORDER BY ts DESC LIMIT 1;
                """)
                sc = cur.fetchone()
                if sc:
                    data['score'] = {
                        'total': float(sc[0]) if sc[0] else 0,
                        'side': sc[1] or 'LONG',
                        'stage': int(sc[2]) if sc[2] else 1,
                    }
            except Exception:
                pass

            # 24h trade summary
            try:
                cur.execute("""
                    SELECT count(*), COALESCE(sum(realized_pnl), 0)
                    FROM execution_log
                    WHERE ts > now() - interval '24 hours'
                      AND realized_pnl IS NOT NULL;
                """)
                tr = cur.fetchone()
                if tr:
                    data['trades_24h'] = {
                        'count': int(tr[0]),
                        'pnl': float(tr[1]),
                    }
            except Exception:
                pass
    except Exception as e:
        data['error'] = str(e)
    finally:
        conn.close()
    return data


def generate_report(data=None):
    '''Single GPT call with summary data.'''
    if not OPENAI_API_KEY:
        return _local_only_report(data)
    data_str = json.dumps(data, ensure_ascii=False, default=str)
    if len(data_str) > 2000:
        data_str = data_str[:2000] + '...'
    prompt = f'다음 트레이딩봇 데이터를 기반으로 한국어 전략 리포트를 작성하세요.\n\n데이터:\n{data_str}\n\n리포트 형식:\n1. 주요 뉴스 영향 요약 (미국/거시/나스닥 우선)\n2. 거시경제 환경 (QQQ, DXY, VIX, US10Y 포함)\n3. 현재 추세/국면 분석 (4h/12h 추세, BB 위치, Ichimoku 클라우드 포함)\n4. 뉴스→BTC 영향 경로 + 조건부 시나리오(2-3개)\n5. 현재 포지션/리스크/예산(70%) 현황\n6. 대응전략 (포지션/손절/익절/추가진입 조건)\n\n뉴스→BTC 인과 서술 규칙:\n- "~로 인해 ~했다" 같은 단정적 인과 문장 금지\n- 반드시 "~가능성", "~추정", "~에 따르면" 등 불확실성 표현 사용\n- 대체 가설(다른 원인) 1개 이상 병기\n- 근거 데이터 없으면 "근거 부족(인과 추정 금지)" 명시\n\n총 1000자 이내. 불릿 포인트 사용. 100% 한국어로 작성.\n영어 약어(BTC, ETF, CPI, BB, RSI 등)만 허용. 그 외 모든 내용은 한국어.\n※ 매매 실행 권한 없음. 분석/권고만.'
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY, timeout=20)
        resp = client.chat.completions.create(model=MODEL, messages=[
            {
                'role': 'user',
                'content': prompt}], max_tokens=600, temperature=0.3)
        return resp.choices[0].message.content.strip()
    except Exception as e:
        return _local_only_report(data) + f'\n\n(AI 호출 실패: {e})'


def _local_only_report(data=None):
    '''Fallback: report from raw data without AI.'''
    lines = [
        '(AI 미사용 — 데이터 요약만)']
    news = data.get('top_news', [])
    if news:
        lines.append('📰 주요 뉴스:')
        for n in news[:3]:
            lines.append(f"  • impact={n['score']} {n['title']}")
    else:
        lines.append('📰 최근 12h 고임팩트 뉴스 없음')
    ind = data.get('indicator')
    if ind:
        lines.append(f"📊 지표: BB mid={ind['bb_mid']} up={ind['bb_up']} dn={ind['bb_dn']}")
        lines.append(f"  Ichimoku: tenkan={ind['ich_tenkan']} kijun={ind['ich_kijun']}")
        lines.append(f"  Vol spike={'YES' if ind.get('vol_spike') else 'NO'}")
    pos = data.get('position')
    if pos:
        lines.append(f"📍 포지션: {pos['side']} qty={pos['qty']} entry={pos['entry']}")
    else:
        lines.append('📍 포지션 없음')
    pr = data.get('1h_range')
    if pr:
        lines.append(f"📈 1h 범위: {pr['low']}~{pr['high']} (현재={pr['last']})")
    return '\n'.join(lines)


def _gather_daily_performance():
    '''Gather execution_log PnL stats for today (KST).

    PnL source: execution_log.realized_pnl (populated by fill_watcher on CLOSE/REDUCE).
    Covers order types: CLOSE, REDUCE, REVERSE_CLOSE, EXIT, EMERGENCY_CLOSE, STOP_LOSS.
    '''
    conn = _db()
    data = {}
    try:
        kst = timezone(timedelta(hours=9))
        today_kst = datetime.now(kst).strftime('%Y-%m-%d')
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*),
                       COUNT(*) FILTER (WHERE realized_pnl > 0),
                       COUNT(*) FILTER (WHERE realized_pnl < 0),
                       COUNT(*) FILTER (WHERE realized_pnl = 0 OR realized_pnl IS NULL),
                       COALESCE(SUM(realized_pnl), 0),
                       COALESCE(AVG(realized_pnl), 0),
                       COALESCE(MAX(realized_pnl), 0),
                       COALESCE(MIN(realized_pnl), 0)
                FROM execution_log
                WHERE order_type IN ('CLOSE', 'REDUCE', 'REVERSE_CLOSE',
                                     'EXIT', 'EMERGENCY_CLOSE', 'STOP_LOSS')
                  AND status IN ('FILLED', 'VERIFIED')
                  AND (ts AT TIME ZONE 'Asia/Seoul')::date = %s::date;
            """, (today_kst,))
            row = cur.fetchone()
        if row:
            total = row[0]
            wins = row[1]
            data = {
                'total': total,
                'wins': wins,
                'losses': row[2],
                'neutral': row[3],
                'total_pnl': round(float(row[4]), 2),
                'avg_pnl': round(float(row[5]), 2),
                'best_pnl': round(float(row[6]), 2),
                'worst_pnl': round(float(row[7]), 2),
                'win_rate': f'{(wins / total * 100):.1f}%' if total > 0 else '0%',
                'max_loss_streak': 0,
            }
    except Exception as e:
        data['error'] = str(e)
    finally:
        conn.close()
    return data


def generate_daily_performance_report():
    '''Generate and send daily performance report.'''
    data = _gather_daily_performance()
    if 'error' in data:
        return f"(성능 리포트 불가: {data['error']})"
    if data.get('total', 0) == 0:
        return '(오늘 거래 기록 없음)'
    lines = []
    lines.append(f"거래: {data['total']}건 (W:{data['wins']} / L:{data['losses']} / N:{data['neutral']})")
    lines.append(f"승률: {data['win_rate']}")
    lines.append(f"총 PnL: {data['total_pnl']} USDT")
    lines.append(f"평균: {data['avg_pnl']} | 최고: {data['best_pnl']} | 최저: {data['worst_pnl']}")
    lines.append(f"최대 연속 손실: {data['max_loss_streak']}건")
    return '\n'.join(lines)


def _generate_ai_one_liner(data):
    """Generate AI 1-line summary + risk_level from report data."""
    if not OPENAI_API_KEY:
        return {}
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY, timeout=15)

        snap = data.get('snapshot', {})
        scores = data.get('scores', {})
        stats = data.get('stats', {})
        macro = data.get('macro_snapshot', {})
        pos = data.get('position', {})

        summary_parts = []
        summary_parts.append(f"BTC=${snap.get('price', 0):,.0f}")
        if macro:
            for sym in ('QQQ', 'SPY', 'DXY', 'VIX'):
                info = macro.get(sym, {})
                if info.get('price'):
                    summary_parts.append(f"{sym}={info['price']:.2f}")
        summary_parts.append(f"총점={scores.get('total', 0):+.1f}")
        summary_parts.append(f"거시하락={stats.get('bearish', 0)}건")

        prompt = (
            '다음 데이터를 보고 한국어 1줄 요약(50자 이내)과 risk_level(낮음/보통/높음) 반환.\n100% 한국어로 작성. 영어 약어(BTC, ETF, CPI 등)만 허용.\n'
            '단정적 인과("~로 인해 ~했다") 금지. "~가능성", "~추정" 등 불확실성 표현 사용.\n'
            f'데이터: {" ".join(summary_parts)}\n'
            f'포지션: {pos.get("side", "없음")}\n'
            '응답 JSON: {{"one_liner":"...","risk_level":"...","watch_items":["항목1","항목2"]}}'
        )
        resp = client.chat.completions.create(
            model=MODEL, messages=[{'role': 'user', 'content': prompt}],
            max_tokens=150, temperature=0.2)
        text = resp.choices[0].message.content.strip()
        return json.loads(text)
    except Exception as e:
        print(f'[strategy_report] AI one-liner error: {e}', flush=True)
        return {}


def main():
    print('[strategy_report] START', flush=True)
    conn = None
    try:
        conn = _db()
        with conn.cursor() as cur:
            from news_strategy_report import build_report_data
            from report_formatter import format_news_strategy_report

            data = build_report_data(cur, max_news=5)

            # AI 1줄 요약
            ai_summary = _generate_ai_one_liner(data)
            data['ai_summary'] = ai_summary

            report_body = format_news_strategy_report(data)
    except Exception as e:
        print(f'[strategy_report] build_report_data error: {e}', flush=True)
        # Fallback to legacy report
        data = gather_data()
        report_body = generate_report(data)
    finally:
        if conn:
            conn.close()

    kst = timezone(timedelta(hours=9))
    now_kst = datetime.now(kst)
    hour_kst = now_kst.hour
    period = "아침" if hour_kst < 12 else "저녁"
    header = f"📊 {period} 전략 리포트 ({now_kst.strftime('%m/%d %H:%M')} KST)\n{'━━━━━━━━━━━━━━━━━━━━━━'}\n\n"
    full_msg = header + report_body
    print(full_msg, flush=True)
    _send_telegram(full_msg)
    print('[strategy_report] DONE', flush=True)


def main_daily_performance():
    '''Entry point for daily performance report (systemd timer).'''
    print('[strategy_report] DAILY PERFORMANCE START', flush=True)
    report = generate_daily_performance_report()
    kst = timezone(timedelta(hours=9))
    now_kst = datetime.now(kst)
    header = f"📊 일일 성능 리포트 ({now_kst.strftime('%m/%d')} KST)\n{'━━━━━━━━━━━━━━━━━━━━━━'}\n\n"
    full_msg = header + report
    print(full_msg, flush=True)
    _send_telegram(full_msg)
    print('[strategy_report] DAILY PERFORMANCE DONE', flush=True)


# ============================================================
# live_test_2 종합보고서
# ============================================================
def _gather_live_test_2_data():
    """Gather comprehensive data for live_test_2 report."""
    RUN_ID = "LIVE_TEST_2_20260216_20260220"
    START_UTC = "2026-02-16T09:00:00+00:00"
    END_UTC = "2026-02-20T08:30:00+00:00"

    conn = _db()
    data = {
        'run_id': RUN_ID,
        'start': START_UTC,
        'end': END_UTC,
        'mode': 'REAL',
        'symbol': 'BTC ONLY',
        'cap': None,  # filled dynamically below
    }
    try:
        with conn.cursor() as cur:
            # Dynamic cap from equity limits
            try:
                import safety_manager
                _eq = safety_manager.get_equity_limits(cur)
                data['cap'] = round(_eq.get('operating_cap', 900), 0)
            except Exception:
                data['cap'] = 900

            # Total trades, wins, losses, PnL
            cur.execute("""
                SELECT COUNT(*),
                       COUNT(*) FILTER (WHERE realized_pnl > 0),
                       COUNT(*) FILTER (WHERE realized_pnl < 0),
                       COUNT(*) FILTER (WHERE realized_pnl = 0 OR realized_pnl IS NULL),
                       COALESCE(SUM(realized_pnl), 0),
                       COALESCE(AVG(realized_pnl), 0),
                       COALESCE(MAX(realized_pnl), 0),
                       COALESCE(MIN(realized_pnl), 0)
                FROM execution_log
                WHERE order_type IN ('CLOSE', 'REDUCE', 'REVERSE_CLOSE',
                                     'EXIT', 'EMERGENCY_CLOSE', 'STOP_LOSS',
                                     'SCHEDULED_CLOSE')
                  AND status IN ('FILLED', 'VERIFIED')
                  AND ts >= %s AND ts < %s;
            """, (START_UTC, END_UTC))
            row = cur.fetchone()
            if row:
                total = row[0]
                wins = row[1]
                data['trades'] = {
                    'total': total, 'wins': wins, 'losses': row[2],
                    'neutral': row[3],
                    'total_pnl': round(float(row[4]), 4),
                    'avg_pnl': round(float(row[5]), 4),
                    'best_pnl': round(float(row[6]), 4),
                    'worst_pnl': round(float(row[7]), 4),
                    'win_rate': f'{(wins / total * 100):.1f}%' if total > 0 else '0%',
                }

            # Max drawdown (running sum of realized_pnl)
            cur.execute("""
                SELECT MIN(running_pnl) FROM (
                    SELECT SUM(realized_pnl) OVER (ORDER BY ts) AS running_pnl
                    FROM execution_log
                    WHERE order_type IN ('CLOSE', 'REDUCE', 'REVERSE_CLOSE',
                                         'EXIT', 'EMERGENCY_CLOSE', 'STOP_LOSS',
                                         'SCHEDULED_CLOSE')
                      AND status IN ('FILLED', 'VERIFIED')
                      AND ts >= %s AND ts < %s
                ) sub;
            """, (START_UTC, END_UTC))
            dd_row = cur.fetchone()
            data['max_drawdown'] = round(float(dd_row[0]), 4) if dd_row and dd_row[0] else 0

            # Cap compliance: block/shrink counts
            cur.execute("""
                SELECT event, count(*)
                FROM live_executor_log
                WHERE event IN ('CAP_BLOCKED', 'CAP_SHRINK')
                  AND ts >= %s AND ts < %s
                GROUP BY event;
            """, (START_UTC, END_UTC))
            cap_events = {r[0]: r[1] for r in cur.fetchall()}
            data['cap_blocked'] = cap_events.get('CAP_BLOCKED', 0)
            data['cap_shrink'] = cap_events.get('CAP_SHRINK', 0)

            # 17:30 liquidation result
            cur.execute("""
                SELECT COUNT(*), COALESCE(SUM(realized_pnl), 0)
                FROM execution_log
                WHERE order_type = 'SCHEDULED_CLOSE'
                  AND status IN ('FILLED', 'VERIFIED')
                  AND ts >= %s AND ts < %s;
            """, (START_UTC, END_UTC))
            liq_row = cur.fetchone()
            data['liquidation'] = {
                'count': int(liq_row[0]) if liq_row else 0,
                'pnl': round(float(liq_row[1]), 4) if liq_row else 0,
            }

            # News stats
            cur.execute("""
                SELECT COUNT(*),
                       COUNT(*) FILTER (WHERE impact_score >= 7)
                FROM news
                WHERE ts >= %s AND ts < %s;
            """, (START_UTC, END_UTC))
            news_row = cur.fetchone()
            data['news'] = {
                'total': int(news_row[0]) if news_row else 0,
                'high_impact': int(news_row[1]) if news_row else 0,
            }

            # News path stats
            try:
                cur.execute("""
                    SELECT COUNT(*)
                    FROM news_24h_path
                    WHERE created_at >= %s AND created_at < %s;
                """, (START_UTC, END_UTC))
                path_row = cur.fetchone()
                data['news_paths'] = int(path_row[0]) if path_row else 0
            except Exception:
                data['news_paths'] = 'N/A'

    except Exception as e:
        data['error'] = str(e)
    finally:
        if conn:
            conn.close()
    return data


def _format_live_test_2_report(data):
    """Format live_test_2 comprehensive report text."""
    trades = data.get('trades', {})
    liq = data.get('liquidation', {})
    news = data.get('news', {})

    lines = [
        f"📊 2차 실매매 테스트 종합보고서",
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"",
        f"[설정]",
        f"  RUN_ID: {data['run_id']}",
        f"  기간: {data['start'][:10]} ~ {data['end'][:10]}",
        f"  모드: {data['mode']} / {data['symbol']} / cap={data['cap']} USDT",
        f"",
        f"[매매 결과]",
        f"  총 트레이드: {trades.get('total', 0)}건",
        f"  승/패/무: {trades.get('wins', 0)}/{trades.get('losses', 0)}/{trades.get('neutral', 0)}",
        f"  승률: {trades.get('win_rate', '0%')}",
        f"  총 PnL: {trades.get('total_pnl', 0)} USDT",
        f"  평균 PnL: {trades.get('avg_pnl', 0)} USDT",
        f"  최고/최저: {trades.get('best_pnl', 0)} / {trades.get('worst_pnl', 0)}",
        f"  최대 DD: {data.get('max_drawdown', 0)} USDT",
        f"",
        f"[캡 준수]",
        f"  {data.get('cap', '?')} USDT 캡 차단: {data.get('cap_blocked', 0)}건",
        f"  캡 축소: {data.get('cap_shrink', 0)}건",
        f"",
        f"[17:30 청산]",
        f"  청산 횟수: {liq.get('count', 0)}건",
        f"  청산 PnL: {liq.get('pnl', 0)} USDT",
        f"",
        f"[뉴스/경로]",
        f"  뉴스: {news.get('total', 0)}건 (고영향: {news.get('high_impact', 0)}건)",
        f"  24h 경로: {data.get('news_paths', 'N/A')}건",
    ]

    if data.get('error'):
        lines.append(f"\n(일부 데이터 조회 오류: {data['error']})")

    lines.append(f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━")
    return '\n'.join(lines)


def main_live_test_2():
    """Entry point for live_test_2 comprehensive report."""
    print('[strategy_report] LIVE_TEST_2 REPORT START', flush=True)
    data = _gather_live_test_2_data()
    report = _format_live_test_2_report(data)
    print(report, flush=True)
    _send_telegram(report)
    print('[strategy_report] LIVE_TEST_2 REPORT DONE', flush=True)


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'daily':
        main_daily_performance()
    elif len(sys.argv) > 1 and sys.argv[1] == 'live_test_2':
        main_live_test_2()
    else:
        main()
