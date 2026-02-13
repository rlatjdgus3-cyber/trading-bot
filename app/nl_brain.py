# Source Generated with Decompyle++
# File: nl_brain.cpython-312.pyc (Python 3.12)

import re
import json
from datetime import datetime, timezone, timedelta

def now_kst_str():
    kst = timezone(timedelta(hours=9))
    return datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S KST')


def get_btc_price():
    import ccxt
    ex = ccxt.bybit({
        'enableRateLimit': True})
    t = ex.fetch_ticker('BTC/USDT')
    last = t.get('last')
    ts = t.get('datetime') or ''
    return f'BTC/USDT 현재가: {last} (bybit spot) {ts}'


def db_connect():
    import psycopg2
    return psycopg2.connect(host='localhost', dbname='trading', user='bot', password='botpass', connect_timeout=10, options='-c statement_timeout=30000')


def fetch_latest_news(limit=5):
    rows = []
    try:
        db = db_connect()
        with db.cursor() as cur:
            cur.execute("""
                SELECT id, source, title, url, summary, impact_score, keywords
                FROM news
                ORDER BY id DESC
                LIMIT %s;
            """, (limit,))
            rows = cur.fetchall()
        db.close()
        return (rows, None)
    except Exception as e:
        return (rows, f'DB 에러: {e}')


def summarize_news(limit=5):
    (rows, err) = fetch_latest_news(limit=limit)
    if err:
        return err
    if not rows:
        return '뉴스가 아직 DB에 없습니다. (news_bot이 저장 중인지 확인 필요)'
    lines = [
        f'[NEWS] 최신 {len(rows)}개 요약']
    for rid, source, title, url, summary, impact, keywords in rows[::-1]:
        impact_s = f'{impact}'.rjust(2)
        lines.append(f'- ({source}) impact={impact_s} | {title}\n  {summary}\n  {url}')
    return '\n'.join(lines)


def extract_direction(summary=None):
    m = re.match('\\[(up|down|neutral)\\]\\s*', (summary or '').strip(), re.IGNORECASE)
    if not m:
        return 'neutral'
    return m.group(1).lower()


def strategy_from_news():
    (rows, err) = fetch_latest_news(limit=10)
    if err:
        return err
    if not rows:
        return '뉴스가 없어 전략 판단을 못 합니다. (DB news 비어있음)'
    scored = []
    for rid, source, title, url, summary, impact, keywords in rows:
        direction = extract_direction(summary or '')
        scored.append((int(impact or 0), direction, source, title, url, summary or ''))
    scored.sort(key=(lambda x: x[0]), reverse=True)
    top = scored[:3]
    score = 0
    for imp, direction, source, title, url, summary in top:
        if direction == 'up':
            score += imp
        elif direction == 'down':
            score -= imp
    if score > 5:
        verdict = 'LONG 유리'
    elif score < -5:
        verdict = 'SHORT 유리'
    else:
        verdict = 'NEUTRAL / HOLD'
    lines = [f'[전략 판단] score={score} → {verdict}']
    for imp, direction, source, title, url, summary in top:
        lines.append(f'  - [{direction}] impact={imp} ({source}) {title}')
    return '\n'.join(lines)


def normalize(text=None):
    t = (text or '').strip()
    t = re.sub('\\s+', ' ', t)
    return t


def handle(text=None):
    t = normalize(text)
    tl = t.lower()
    if not t:
        return '무슨 질문이든 해보세요. BTC 가격, 뉴스, 전략 분석 등.'
    if any(k in tl for k in ['btc', '비트코인', '가격', '시세', '얼마']):
        return get_btc_price()
    if any(k in tl for k in ['뉴스', 'news']):
        if any(k in tl for k in ['전략', '판단', '분석', 'strategy']):
            return strategy_from_news()
        return summarize_news()
    if any(k in tl for k in ['전략', 'strategy', '방향', '판단']):
        return strategy_from_news()
    return f'[nl_brain] 알 수 없는 요청: {t[:200]}'

if __name__ == '__main__':
    import sys
    text = ' '.join(sys.argv[1:]) if len(sys.argv) > 1 else ''
    print(handle(text))
