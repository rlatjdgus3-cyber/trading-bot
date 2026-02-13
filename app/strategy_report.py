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
import psycopg2
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
load_dotenv('/root/trading-bot/app/.env')
SYMBOL = os.getenv('SYMBOL', 'BTC/USDT:USDT')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '')
MODEL = os.getenv('OPENAI_MODEL', 'gpt-4o-mini')
DB = dict(host=os.getenv('DB_HOST', 'localhost'), port=int(os.getenv('DB_PORT', '5432')), dbname=os.getenv('DB_NAME', 'trading'), user=os.getenv('DB_USER', 'bot'), password=os.getenv('DB_PASS', 'botpass'), connect_timeout=10, options='-c statement_timeout=30000')
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
    (token, chat_id) = _load_tg_env()
    if not token or not chat_id:
        print('[strategy_report] SKIP: telegram env missing', flush=True)
        return None
    s = text or ''
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
    conn = psycopg2.connect(**DB)
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
                SELECT title, impact_score
                FROM news
                WHERE ts > now() - interval '12 hours'
                  AND impact_score >= 6
                ORDER BY impact_score DESC
                LIMIT 5;
            """)
            data['top_news'] = [{'title': r[0], 'score': r[1]} for r in cur.fetchall()]

            # Indicator snapshot
            cur.execute("""
                SELECT bb_upper, bb_mid, bb_lower,
                       ichimoku_tenkan, ichimoku_kijun, volume_spike
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
    prompt = f'다음 트레이딩봇 데이터를 기반으로 한국어 전략 리포트를 작성하세요.\n\n데이터:\n{data_str}\n\n리포트 형식:\n1. 주요 뉴스 영향 요약 (2~3줄)\n2. 현재 추세/국면 분석 (볼린저밴드 위치, 이치모쿠 크로스 상태)\n3. 변동성 평가\n4. 핵심 지지/저항 레벨\n5. 전략 시나리오 2~3개\n6. 급변 시 대응 포인트\n\n총 800자 이내. 불릿 포인트 사용.\n※ 매매 실행 권한 없음. 분석/권고만.'
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
    '''Gather trade_process_log stats for today (KST).'''
    conn = _db()
    data = {}
    try:
        kst = timezone(timedelta(hours=9))
        today_kst = datetime.now(kst).strftime('%Y-%m-%d')
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*),
                       COUNT(*) FILTER (WHERE pnl > 0),
                       COUNT(*) FILTER (WHERE pnl < 0),
                       COUNT(*) FILTER (WHERE pnl = 0 OR pnl IS NULL),
                       COALESCE(SUM(pnl), 0),
                       COALESCE(AVG(pnl), 0),
                       COALESCE(MAX(pnl), 0),
                       COALESCE(MIN(pnl), 0)
                FROM trade_process_log
                WHERE action IN ('CLOSE', 'STOPLOSS')
                  AND ts::date = %s::date;
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


def main():
    print('[strategy_report] START', flush=True)
    data = gather_data()
    report = generate_report(data)
    kst = timezone(timedelta(hours=9))
    now_kst = datetime.now(kst)
    header = f"📊 전략 리포트 ({now_kst.strftime('%m/%d %H:%M')} KST)\n{'━━━━━━━━━━━━━━━━━━━━━━'}\n\n"
    full_msg = header + report
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

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'daily':
        main_daily_performance()
    else:
        main()
