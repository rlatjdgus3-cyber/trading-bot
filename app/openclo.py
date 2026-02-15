# Source Generated with Decompyle++
# File: openclo.cpython-312.pyc (Python 3.12)

import os
import time
import json
import re
import secrets
import requests
import psycopg2
import datetime
from db_config import get_conn
import decimal
from dotenv import load_dotenv
load_dotenv('/root/trading-bot/app/.env')

# Telegram credentials from telegram_cmd.env (single source of truth)
def _load_tg_env():
    _env = {}
    try:
        with open('/root/trading-bot/app/telegram_cmd.env', 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                k, v = line.split('=', 1)
                _env[k.strip()] = v.strip()
    except Exception:
        pass
    return _env
_tg_env = _load_tg_env()
TG_TOKEN = _tg_env.get('TELEGRAM_BOT_TOKEN') or os.getenv('TELEGRAM_BOT_TOKEN')
TG_CHAT = _tg_env.get('TELEGRAM_ALLOWED_CHAT_ID') or os.getenv('TELEGRAM_CHAT_ID')
ANTHROPIC_KEY = os.getenv('ANTHROPIC_API_KEY') or None
if ANTHROPIC_KEY == 'DISABLED':
    ANTHROPIC_KEY = None
ANTHROPIC_URL = 'https://api.anthropic.com/v1/messages'
ANTHROPIC_MODEL = os.getenv('ANTHROPIC_MODEL', 'claude-sonnet-4-20250514')
db = get_conn(autocommit=True)


def _db_reconnect():
    global db
    try:
        db.close()
    except Exception:
        pass
    db = get_conn(autocommit=True)


def _db_query(func):
    """Execute a DB function with auto-reconnect on failure."""
    try:
        return func(db)
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        _db_reconnect()
        return func(db)


def tg_send(text=None):
    if not TG_TOKEN or not TG_CHAT:
        return None
    url = f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage'
    requests.post(url, json={
        'chat_id': TG_CHAT,
        'text': text[:3500]}, timeout=20)


def get_updates(offset=None):
    url = f'https://api.telegram.org/bot{TG_TOKEN}/getUpdates'
    params = {
        'timeout': 20}
    if offset:
        params['offset'] = offset
    r = requests.get(url, params=params, timeout=40)
    r.raise_for_status()
    return r.json()


def to_jsonable(x):
    if isinstance(x, decimal.Decimal):
        return float(x)
    if isinstance(x, (datetime.datetime, datetime.date)):
        return x.isoformat()
    if isinstance(x, bytes):
        return x.decode('utf-8', errors='replace')
    return str(x)


def clean_user_reply(s=None):
    if not s:
        return ''
    s = s.strip()
    s = re.sub('```json\\s*.*?```', '', s, flags=re.S | re.I).strip()
    s = re.sub('```\\s*.*?```', '', s, flags=re.S).strip()
    if s.startswith('{') and s.endswith('}'):
        return ''
    return s[:3200].strip()


def settings_get():
    def _run(conn):
        with conn.cursor() as cur:
            cur.execute("SELECT key, value FROM settings;")
            return {k: v for k, v in cur.fetchall()}
    return _db_query(_run)


def settings_set(key=None, value=None):
    def _run(conn):
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO settings (key, value) VALUES (%s, %s) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;",
                (key, json.dumps(value) if not isinstance(value, str) else value)
            )
    _db_query(_run)


def enqueue(cmd=None, args=None, who=None):
    def _run(conn):
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO command_queue (cmd, args, status, who) VALUES (%s, %s, 'pending', %s) RETURNING id;",
                (cmd, json.dumps(args or {}), who or 'openclo')
            )
            return cur.fetchone()[0]
    return _db_query(_run)


def cmd_status_tail(limit=8):
    def _run(conn):
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, ts, cmd, status, result FROM command_queue ORDER BY id DESC LIMIT %s;",
                (limit,)
            )
            return cur.fetchall()
    return _db_query(_run)


def get_status():
    def _run(conn):
        with conn.cursor() as cur:
            cur.execute("SELECT key, value FROM settings;")
            settings = {k: v for k, v in cur.fetchall()}
            cur.execute("SELECT enabled, updated_at FROM trade_switch LIMIT 1;")
            sw = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM live_order_once_lock;")
            lock = cur.fetchone()
        return {
            'settings': settings,
            'trade_switch': {'enabled': bool(sw[0]) if sw else False, 'updated_at': str(sw[1]) if sw else 'n/a'},
            'once_lock_count': int(lock[0]) if lock else 0,
        }
    return _db_query(_run)


def get_top_news(limit=5, min_impact=6):
    def _run(conn):
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, title, impact_score, summary, url FROM news "
                "WHERE impact_score >= %s ORDER BY id DESC LIMIT %s;",
                (min_impact, limit)
            )
            return cur.fetchall()
    rows = _db_query(_run)
    if not rows:
        return '최근 고임팩트 뉴스 없음'
    lines = [f'📰 최근 고임팩트 뉴스 ({len(rows)}건)']
    for nid, title, score, summary, url in rows:
        lines.append(f'- [{score}] {title}\n  {summary}\n  {url}')
    return '\n'.join(lines)


def anthropic_call(user_text=None, system_text=None, max_tokens=None,
                   call_type='AUTO'):
    if not ANTHROPIC_KEY:
        return 'Claude 키가 없어요.'
    try:
        import claude_gate
        prompt = ((system_text or '') + '\n\n' + (user_text or '')).strip()
        result = claude_gate.call_claude(
            gate='openclaw', prompt=prompt,
            cooldown_key='openclo', context={'intent': 'chat', 'source': 'openclaw'},
            max_tokens=max_tokens or 300, call_type=call_type)
        if result.get('fallback_used'):
            return f'Claude 게이트 거부: {result.get("gate_reason", "unknown")}'
        return result.get('text', '')
    except Exception as e:
        return f'Claude 오류: {e}'


def extract_json(text=None):
    m = re.search('\\{.*\\}', text or '', re.S)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except Exception:
        return None

PENDING = {}

def make_approval(action=None, args=None, ttl=300):
    token = secrets.token_hex(3)
    PENDING[token] = {
        'action': action,
        'args': args,
        'expires': time.time() + ttl}
    return token


def consume(token=None):
    obj = PENDING.get(token)
    if not obj:
        return None
    if time.time() > obj['expires']:
        PENDING.pop(token, None)
        return None
    PENDING.pop(token, None)
    return obj


def help_text():
    return ('✅ OpenClo (대화형+운영+디렉티브)\n명령어:\n/help\n/status\n/decision 또는 /d\n'
            '/news (중요뉴스)\n/arm (긴급명령 허용 ON)\n/disarm (긴급명령 OFF)\n/ops (최근 명령 결과)\n'
            '/audit (시스템 감사)\n/risk <mode> (conservative/normal/aggressive)\n'
            '/keywords (워치 키워드 목록)\n/directives (최근 디렉티브)\n'
            '/force (쿨다운 무시 + Claude 강제 실행)\n\n'
            '자연어 예시:\n- 오늘자 뉴스 정리해줘\n- 중요뉴스 가져와\n- 한글로\n'
            '- 뉴스 기준 7로 바꿔\n- 메인 재시작해줘\n- 긴급이야 매매 멈춰\n'
            '- BTC 키워드에 trump 추가해\n- 리스크 보수적으로 바꿔\n'
            '- 시스템 점검해줘\n')


def route_nl(user_text=None, status_snapshot=None):
    prompt = (
        '사용자 메시지를 분석해서 JSON으로 응답하세요.\n'
        '{"intent": "news|status|decision|set_setting|restart|pause_trading|resume_trading|directive|chat",\n'
        ' "reply": "한국어 자연어 응답",\n'
        ' "action": "none|restart|pause_trading|resume_trading|set_setting|directive",\n'
        ' "args": {}}\n\n'
        f'상태: {json.dumps(status_snapshot, ensure_ascii=False, default=to_jsonable)}\n\n'
        f'메시지: {user_text}'
    )
    raw = anthropic_call(prompt, system_text='', max_tokens=300)
    parsed = extract_json(raw)
    if parsed:
        return parsed
    return {'intent': 'chat', 'reply': raw, 'action': 'none', 'args': {}}


def make_decision(status=None):
    prompt = '아래 상태를 보고 하나만 선택: LONG / SHORT / HOLD\n그리고 이유 2줄 이내 한국어로.\n\n상태:\n' + json.dumps(status, ensure_ascii=False, default=to_jsonable)
    return anthropic_call(prompt, system_text='', max_tokens=220)


def handle(text=None):
    t = (text or '').strip()
    m = re.match('^(확인|승인)\\s+([0-9a-fA-F]{6})$', t)
    if m:
        token = m.group(2).lower()
        obj = consume(token)
        if not obj:
            return '❌ 승인코드가 없거나 만료됐어요.'
        action = obj['action']
        args = obj.get('args') or {}
        if action == 'restart':
            cid = enqueue('restart', args)
            return f'✅ 승인 완료. restart 요청 넣음 (cmd_id={cid})'
        if action == 'pause_trading':
            cid = enqueue('emergency', {
                'action': 'pause_trading'})
            return f'🚨 승인 완료. 매매 긴급정지 요청 (cmd_id={cid})'
        if action == 'resume_trading':
            cid = enqueue('emergency', {
                'action': 'resume_trading'})
            return f'✅ 승인 완료. 매매 재가동 요청 (cmd_id={cid})'
    if t == '/help':
        return help_text()
    if t == '/status':
        s = get_status()
        return '📊 STATUS\n' + json.dumps(s, ensure_ascii=False, indent=2, default=to_jsonable)
    if t in ('/decision', '/d'):
        s = get_status()
        return '🧠 판단\n' + make_decision(s)
    if t == '/news':
        s = settings_get()
        return get_top_news(limit=5, min_impact=int(s.get('news_threshold', 6)))
    if t == '/ops':
        rows = cmd_status_tail(8)
        lines = [
            '🧾 최근 명령']
        for cid, ts, cmd, st, res in rows:
            lines.append(f'- {cid} {cmd} {st} {res[:120] if res else ""}')
        return '\n'.join(lines)
    if t == '/arm':
        settings_set('trading_armed', True)
        return '✅ 긴급명령 ARM=ON'
    if t == '/disarm':
        settings_set('trading_armed', False)
        return '✅ 긴급명령 ARM=OFF'
    # Directive commands
    if t == '/audit':
        import openclaw_engine
        result = openclaw_engine.execute_directive(db, 'AUDIT', {}, source='telegram')
        return result.get('message', 'Audit complete')
    if t.startswith('/risk '):
        import openclaw_engine
        mode = t.split(' ', 1)[1].strip()
        result = openclaw_engine.execute_directive(db, 'RISK_MODE', {'mode': mode}, source='telegram')
        return result.get('message', 'Risk mode updated')
    if t == '/keywords' or t.startswith('/keywords '):
        import openclaw_engine
        args_text = t[len('/keywords'):].strip()
        if args_text:
            params = openclaw_engine._parse_keyword_text(args_text)
        else:
            params = {'action': 'list', 'keywords': []}
        result = openclaw_engine.execute_directive(db, 'WATCH_KEYWORDS', params, source='telegram')
        return result.get('message', 'Keywords processed')
    if t == '/force' or t.startswith('/force '):
        force_text = t[len('/force'):].strip() or '지금 BTC 전략 판단해줘'
        return '🔓 [FORCE] 쿨다운 무시, Claude 강제 실행\n\n' + anthropic_call(
            force_text,
            system_text='비트코인 선물 트레이딩 전략 판단을 내려주세요. 400자 이내.',
            max_tokens=500, call_type='USER')
    if t == '/directives':
        def _run(conn):
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, ts, dtype, status, source
                    FROM openclaw_directives ORDER BY id DESC LIMIT 10;
                """)
                return cur.fetchall()
        rows = _db_query(_run)
        if not rows:
            return 'No recent directives'
        lines = ['Recent directives:']
        for did, ts, dtype, status, source in rows:
            lines.append(f'  #{did} {str(ts)[:16]} {dtype} [{status}] src={source}')
        return '\n'.join(lines)
    s = get_status()
    r = route_nl(t, s)
    intent = r.get('intent', 'chat')
    reply = clean_user_reply(r.get('reply', ''))
    action = r.get('action', 'none')
    args = r.get('args') or {}
    if intent == 'news':
        ss = settings_get()
        txt = get_top_news(limit=5, min_impact=int(ss.get('news_threshold', 6)))
        if reply:
            return (reply + '\n\n' + txt).strip()
        return txt
    if intent == 'status':
        s2 = get_status()
        txt = '📊 STATUS\n' + json.dumps(s2, ensure_ascii=False, indent=2, default=to_jsonable)
        if reply:
            return (reply + '\n\n' + txt).strip()
        return txt
    if intent == 'decision':
        s2 = get_status()
        dec = make_decision(s2)
        txt = '🧠 판단\n' + dec
        if reply:
            return (reply + '\n\n' + txt).strip()
        return txt
    if action == 'set_setting':
        key = args.get('key')
        value = args.get('value')
        if not key:
            return reply or "설정 변경 문장을 더 구체적으로 말해줘요. 예: '뉴스 기준 7로'"
        settings_set(key, value)
        base = f'✅ 설정 변경: {key} = {value}'
        if reply:
            return (reply + '\n\n' + base).strip()
        return base
    if action == 'restart':
        service = args.get('service')
        ok = [
            'main',
            'candles',
            'indicators',
            'vol_profile',
            'news_bot',
            'dispatcher',
            'openclo']
        if service not in ok:
            return (reply + '\n\n' if reply else '') + "❌ 재시작 대상이 애매해요. 예: '메인 재시작해줘'"
        token = make_approval('restart', {
            'service': service})
        msg = f"⚠️ '{service}' 재시작 승인 필요: 확인 {token}"
        if reply:
            return (reply + '\n\n' + msg).strip()
        return msg
    if action in ('pause_trading', 'resume_trading'):
        st = settings_get()
        if not st.get('trading_armed', False):
            return (reply + '\n\n' if reply else '') + '❌ ARM이 꺼져 있어요. 먼저 /arm 하세요.'
        token = make_approval(action, {})
        msg = '🚨 매매 긴급정지 승인 필요: 확인 ' + token if action == 'pause_trading' else '✅ 매매 재가동 승인 필요: 확인 ' + token
        if reply:
            return (reply + '\n\n' + msg).strip()
        return msg
    if action == 'directive':
        import openclaw_engine
        dtype = args.get('dtype')
        params = args.get('params', {})
        if dtype:
            result = openclaw_engine.execute_directive(db, dtype, params, source='telegram')
            msg = result.get('message', 'Directive processed')
            if reply:
                return (reply + '\n\n' + msg).strip()
            return msg
        # Try parsing from original text
        parsed = openclaw_engine.parse_directive(t)
        if parsed:
            result = openclaw_engine.execute_directive(
                db, parsed['dtype'], parsed['params'], source='telegram')
            msg = result.get('message', 'Directive processed')
            if reply:
                return (reply + '\n\n' + msg).strip()
            return msg
    if reply:
        return reply
    return '무엇을 도와드릴까요?'


def main():
    print('=== OpenClo STARTED ===')
    tg_send('✅ OpenClo ONLINE\n/help 로 사용법')
    offset = None
    while True:
        try:
            data = get_updates(offset)
            for u in data.get('result', []):
                offset = u['update_id'] + 1
                msg = u.get('message', {})
                txt = msg.get('text')
                if not txt:
                    continue
                out = handle(txt)
                tg_send(out)
            time.sleep(1)
        except Exception as e:
            print(f'[openclo] error: {e}', flush=True)
            time.sleep(5)

if __name__ == '__main__':
    main()
