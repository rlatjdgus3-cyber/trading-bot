"""
Emergency Detector.
Detects extreme conditions and gathers context for AI advisory.
Does NOT execute trades. Detection only.
"""
import os
import traceback
import psycopg2
from decimal import Decimal
from dotenv import load_dotenv
load_dotenv('/root/trading-bot/app/.env')

SYMBOL = os.getenv('SYMBOL', 'BTC/USDT:USDT')
DB = dict(
    host=os.getenv('DB_HOST', 'localhost'),
    port=int(os.getenv('DB_PORT', '5432')),
    dbname=os.getenv('DB_NAME', 'trading'),
    user=os.getenv('DB_USER', 'bot'),
    password=os.getenv('DB_PASS', 'botpass'),
    connect_timeout=10,
    options='-c statement_timeout=30000')

THRESHOLDS = {
    '1m_pct': Decimal('1.0'),
    '5m_pct': Decimal('2.0'),
    'vol_multiplier': Decimal('3.0'),
    'consecutive_stops': 2}


def _db():
    return psycopg2.connect(**DB)


def check_price_crash(conn=None):
    alerts = []
    try:
        with conn.cursor() as cur:
            # Check 1m price move
            cur.execute("""
                SELECT ts, o, c FROM candles
                WHERE symbol = %s AND tf = '1m'
                ORDER BY ts DESC LIMIT 2;
            """, (SYMBOL,))
            rows = cur.fetchall()
            if len(rows) >= 2:
                latest = rows[0]
                prev = rows[1]
                if prev[1] and prev[1] > 0:
                    pct = abs((latest[2] - prev[2]) / prev[2]) * 100
                    if pct >= float(THRESHOLDS['1m_pct']):
                        direction = 'UP' if latest[2] > prev[2] else 'DOWN'
                        alerts.append({
                            'type': '1m_move',
                            'pct': round(float(pct), 2),
                            'direction': direction,
                            'ts': str(latest[0])})

            # Check 5m aggregated move
            cur.execute("""
                SELECT c FROM candles
                WHERE symbol = %s AND tf = '1m'
                ORDER BY ts DESC LIMIT 5;
            """, (SYMBOL,))
            candles = cur.fetchall()
            if len(candles) >= 5:
                newest = float(candles[0][0])
                oldest = float(candles[-1][0])
                if oldest > 0:
                    pct_5m = abs((newest - oldest) / oldest) * 100
                    if pct_5m >= float(THRESHOLDS['5m_pct']):
                        direction = 'UP' if newest > oldest else 'DOWN'
                        alerts.append({
                            'type': '5m_move',
                            'pct': round(pct_5m, 2),
                            'direction': direction})
    except Exception:
        traceback.print_exc()
    return alerts


def check_volume_spike(conn=None):
    alerts = []
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT vol, vol_ma20 FROM indicators
                WHERE symbol = %s AND tf = '1m'
                ORDER BY ts DESC LIMIT 1;
            """, (SYMBOL,))
            row = cur.fetchone()
            if row and row[0] and row[1] and float(row[1]) > 0:
                ratio = float(row[0]) / float(row[1])
                if ratio >= float(THRESHOLDS['vol_multiplier']):
                    alerts.append({
                        'type': 'volume_spike',
                        'ratio': round(ratio, 2)})
    except Exception:
        traceback.print_exc()
    return alerts


def check_consecutive_stops(conn=None):
    alerts = []
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT close_reason FROM execution_log
                WHERE close_reason LIKE '%%stop%%'
                ORDER BY ts DESC LIMIT %s;
            """, (THRESHOLDS['consecutive_stops'],))
            rows = cur.fetchall()
            if len(rows) >= THRESHOLDS['consecutive_stops']:
                alerts.append({
                    'type': 'consecutive_stops',
                    'count': len(rows)})
    except Exception:
        traceback.print_exc()
    return alerts


def gather_context(conn=None):
    '''Gather surrounding data for AI advisory.'''
    ctx = {}
    try:
        with conn.cursor() as cur:
            # Recent candles
            cur.execute("""
                SELECT ts, o, h, l, c, v FROM candles
                WHERE symbol = %s AND tf = '1m'
                ORDER BY ts DESC LIMIT 10;
            """, (SYMBOL,))
            ctx['candles'] = [
                {'ts': str(r[0]), 'o': float(r[1]), 'h': float(r[2]),
                 'l': float(r[3]), 'c': float(r[4]), 'v': float(r[5])}
                for r in cur.fetchall()
            ]

            # Indicators
            cur.execute("""
                SELECT ich_tenkan, ich_kijun, rsi_14, atr_14, vol, vol_ma20
                FROM indicators
                WHERE symbol = %s AND tf = '1m'
                ORDER BY ts DESC LIMIT 1;
            """, (SYMBOL,))
            ind_row = cur.fetchone()
            if ind_row:
                ctx['indicators'] = {
                    'tenkan': float(ind_row[0]) if ind_row[0] else None,
                    'kijun': float(ind_row[1]) if ind_row[1] else None,
                    'rsi': float(ind_row[2]) if ind_row[2] else None,
                    'atr': float(ind_row[3]) if ind_row[3] else None,
                    'vol': float(ind_row[4]) if ind_row[4] else None,
                    'vol_ma20': float(ind_row[5]) if ind_row[5] else None,
                }

            # Recent news
            cur.execute("""
                SELECT title, summary, impact_score FROM news
                WHERE ts >= now() - interval '2 hours'
                ORDER BY ts DESC LIMIT 5;
            """)
            ctx['news'] = [
                {'title': r[0], 'summary': r[1], 'impact': r[2]}
                for r in cur.fetchall()
            ]
    except Exception:
        traceback.print_exc()
    return ctx


def run_check():
    '''Main entry. Returns alert dict or None.'''
    conn = _db()
    all_alerts = []
    for checker in (check_price_crash, check_volume_spike, check_consecutive_stops):
        try:
            result = checker(conn)
            if result:
                all_alerts.extend(result)
        except Exception:
            traceback.print_exc()
    if not all_alerts:
        conn.close()
        return None
    ctx = gather_context(conn)
    conn.close()
    return {
        'alerts': all_alerts,
        'context': ctx,
        'priority': 'high'}


def format_alerts(data=None):
    '''Format alert data into human-readable text.'''
    if not data:
        return '긴급 상황 없음'
    lines = ['긴급 감지']
    for a in data.get('alerts', []):
        t = a.get('type', '')
        if t == '1m_move':
            lines.append(f"  - 1분봉 {a['direction']} {a['pct']}% ({a['ts']})")
        elif t == '5m_move':
            lines.append(f"  - 5분봉 {a['direction']} {a['pct']}%")
        elif t == 'volume_spike':
            lines.append(f"  - 거래량 폭증 {a['ratio']}x (MA20 대비)")
        elif t == 'consecutive_stops':
            lines.append(f"  - 연속 손절 {a['count']}회")
    return '\n'.join(lines)
