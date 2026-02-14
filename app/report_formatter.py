"""
report_formatter.py — 중앙 한국어 포매팅 모듈

순수 포매팅 전용. DB/API/네트워크 호출 없음.
어디서든 import 가능.
"""
import os
import re as _re

# ── 한국어 번역 상수 ──────────────────────────────────────

ACTION_KR = {
    'HOLD': '유지',
    'REDUCE': '부분 축소',
    'CLOSE': '전량 정리',
    'REVERSE': '반전',
    'OPEN_LONG': 'LONG 진입',
    'OPEN_SHORT': 'SHORT 진입',
    'ADD': '추가 진입',
    'ENTRY_POSSIBLE': '진입 가능',
    'ABORT': '중단',
    'SKIP': 'SKIP(API fail)',
}

DIRECTION_KR = {
    'UP': '상승',
    'DOWN': '하락',
    'NEUTRAL': '중립',
    'up': '상승',
    'down': '하락',
    'neutral': '중립',
}

TRIGGER_KR = {
    'rapid_price_move': '급격한 가격 변동',
    'volume_spike': '거래량 급등',
    'extreme_funding': '극단적 펀딩비',
    'extreme_score': '극단적 스코어',
    'price_move': '가격 변동',
    'score_flip': '스코어 반전',
    'regime_change': '시장 상태 전환',
    'vol_spike': '거래량 급등',
    'funding_extreme': '극단적 펀딩비',
    'event_emergency': '긴급 이벤트',
    'poc_shift': 'POC 이동(매물대 중심)',
    'vah_break': 'VAH 돌파(가치영역 상단)',
    'val_break': 'VAL 이탈(가치영역 하단)',
    'atr_increase': '변동성(ATR) 급증',
    'level_break': '주요 레벨 돌파',
    'price_spike_1m': '1분 급등락',
    'price_spike_5m': '5분 급등락',
    'price_spike_15m': '15분 급등락',
    'bb_squeeze': 'BB 스퀴즈(변동성 압축)',
    'kijun_cross': '기준선 교차',
    'ma_cross': '이동평균 교차',
}

RISK_KR = {
    'HIGH': '높음',
    'MEDIUM': '보통',
    'LOW': '낮음',
    'CRITICAL': '심각',
}

NEWS_MAGNITUDE_KR = {
    'weak': '약',
    'moderate': '보통',
    'strong': '강',
}

SUPPRESS_REASON_KR = {
    'db_event_lock': '이벤트 중복(DB 락)',
    'db_hash_lock': '동일 이벤트(해시 락)',
    'db_hold_suppress': 'HOLD 반복 억제',
    'local_dedupe': '로컬 중복 필터',
    'local_hold_repeat': 'HOLD 반복 필터',
    'local_consecutive_hold': '연속 HOLD 스킵',
}

MODEL_LABEL_KR = {
    'claude': '🧠 심층 분석(Claude)',
    'gpt-mini': '⚡ 빠른 분석(GPT-mini)',
    'suppressed': '🚫 이벤트 억제(중복/쿨다운)',
}

REASON_KR = {
    'POSITION_ANALYSIS_REPORT': '포지션 분석 근거',
    'MIXED_SIGNALS_WITH_POSITIVE_NEWS': '신호 혼재(뉴스 긍정)',
    'HOLD_CURRENT_POSITION': '현재 포지션 유지',
    'STRONG_TREND_CONTINUATION': '강한 추세 지속',
    'COUNTER_SIGNAL_DETECTED': '역방향 신호 감지',
    'NO_CLEAR_SIGNAL': '명확한 신호 없음',
    'INSUFFICIENT_CONFIRMATION': '확인 신호 부족',
    'RISK_TOO_HIGH': '위험도 과다',
    'BUDGET_EXHAUSTED': '예산 소진',
    'STOP_LOSS_NEAR': '손절가 근접',
    'VOLATILITY_TOO_HIGH': '변동성 과다',
    'TREND_WEAKENING': '추세 약화',
    'MOMENTUM_DIVERGENCE': '모멘텀 괴리',
    'LIQUIDITY_CONCERN': '유동성 우려',
    'NEWS_DRIVEN_CAUTION': '뉴스 기반 경계',
}

SAFETY_REASON_KR = {
    'daily trade limit': '일일 거래 한도 초과',
    'hourly trade limit': '시간당 거래 한도 초과',
    'circuit breaker': '서킷 브레이커 발동',
    'daily loss limit': '일일 손실 한도 초과',
    'total exposure would exceed budget': '총 노출 예산 초과',
    'max stages reached': '최대 단계 도달',
    'budget would exceed': '거래 예산 초과',
}


def _kr_safety_reason(reason: str) -> str:
    """safety_manager 영어 사유를 한국어로 변환."""
    if not reason:
        return '알 수 없는 사유'
    for en_key, kr_val in SAFETY_REASON_KR.items():
        if en_key in reason:
            # 괄호 안 수치 정보 보존
            paren = ''
            if '(' in reason:
                paren = ' ' + reason[reason.index('('):]
            return kr_val + paren
    return reason


DEBUG_MODE_PATH = '/root/trading-bot/app/.debug_mode'


# ── 디버그 모드 관리 ─────────────────────────────────────

def is_debug_on() -> bool:
    """디버그 모드 ON 여부."""
    try:
        with open(DEBUG_MODE_PATH, 'r') as f:
            return f.read().strip().lower() == 'on'
    except Exception:
        return False


def set_debug_mode(on: bool) -> str:
    """디버그 모드 설정. 결과 메시지 반환."""
    try:
        with open(DEBUG_MODE_PATH, 'w') as f:
            f.write('on' if on else 'off')
        state = 'ON' if on else 'OFF'
        return f'디버그 모드: {state}'
    except Exception as e:
        return f'디버그 모드 설정 실패: {e}'


def _debug_line(meta: dict = None) -> str:
    """디버그 푸터. 디버그 ON일 때만 내용 표시."""
    if not is_debug_on():
        return ''
    if not meta:
        return ''
    parts = []
    if meta.get('intent_name'):
        parts.append(f"intent={meta['intent_name']}")
    if meta.get('route'):
        parts.append(f"route={meta['route']}")
    if meta.get('provider'):
        parts.append(f"provider={meta['provider']}")
    if meta.get('call_type'):
        parts.append(f"call_type={meta['call_type']}")
    if meta.get('cost'):
        parts.append(f"cost=${meta['cost']:.4f}")
    if meta.get('latency'):
        parts.append(f"latency={meta['latency']}ms")
    if meta.get('model'):
        parts.append(f"model={meta['model']}")
    if not parts:
        return ''
    return '\n─\n' + ' | '.join(parts)


# ── 영어 비율 감지 ─────────────────────────────────────────

def detect_english_ratio(text: str) -> float:
    """텍스트의 영어 비율 반환 (0.0~1.0).
    숫자, 기호, 공백, 약어(BTC, USD 등)는 제외.
    순수 알파벳 단어 기준으로 비율 계산."""
    if not text:
        return 0.0
    # 허용 약어 — 이것들은 영어로 표시해도 OK
    ALLOWED_EN = {
        # 코인/통화
        'BTC', 'ETH', 'USDT', 'USD', 'KRW', 'SOL', 'XRP', 'DOGE',
        # 매매 액션
        'LONG', 'SHORT', 'HOLD', 'ADD', 'REDUCE', 'CLOSE', 'REVERSE', 'OPEN',
        'SKIPPED', 'ABORT', 'ENTRY', 'POSSIBLE',
        # 기술 지표
        'RSI', 'ATR', 'BB', 'MA', 'EMA', 'SMA', 'MACD', 'VWAP', 'OBV',
        'POC', 'VAH', 'VAL', 'KST', 'UTC',
        # 점수/라벨
        'TECH', 'POS', 'REGIME', 'NEWS', 'TOP',
        'SCORE', 'STAGE', 'NET', 'DEFAULT', 'EVENT', 'WATCHLIST',
        # 뉴스 카테고리/방향
        'MACRO', 'MARKET', 'REGULATION', 'BULLISH', 'BEARISH', 'NEUTRAL',
        'EXTREME',
        # AI/서비스
        'ON', 'OFF', 'OK', 'N/A', 'GPT', 'AI', 'API',
        'CLAUDE', 'BYBIT', 'ANTHROPIC', 'OPENAI',
        # 거시경제
        'SEC', 'ETF', 'CPI', 'FOMC', 'FED', 'BOJ', 'NFP', 'PCE',
        'DXY', 'QQQ', 'SPX', 'GDP', 'PPI',
        # 뉴스 소스 (고유 명사)
        'REUTERS', 'COINDESK', 'COINTELEGRAPH', 'BLOOMBERG',
        'DECRYPT', 'THEBLOCK',
        # 모델명 토큰
        'OPUS', 'SONNET', 'HAIKU', 'MINI',
    }
    # 3글자 이상 순수 알파벳 단어만 추출 (해시/버전 토큰 무시)
    words = _re.findall(r'[A-Za-z]{3,}', text)
    if len(words) < 3:
        # 영어 단어가 3개 미만이면 판정 불가 — 안전으로 처리
        return 0.0
    en_count = sum(1 for w in words if w.upper() not in ALLOWED_EN)
    return en_count / max(len(words), 1)


# Multi-word phrases replaced first (safe substring match),
# then single words with word boundary regex.
_PHRASE_EN_TO_KR = {
    'Stop-Loss': '손절',
    'stop loss': '손절',
    'Stop Loss': '손절',
    'Take Profit': '익절',
    'take profit': '익절',
    'Risk Level': '위험도',
    'risk level': '위험도',
    'No position': '포지션 없음',
    'no position': '포지션 없음',
}

# Single-word replacements — applied with word boundary (\b) regex
_WORD_EN_TO_KR = {
    'Entry': '진입',
    'entry': '진입',
    'Position': '포지션',
    'position': '포지션',
    'Confidence': '확신도',
    'confidence': '확신도',
    'Reason': '근거',
    'reason': '근거',
    'Action': '조치',
    'action': '조치',
    'Current': '현재',
    'current': '현재',
    'Signal': '신호',
    'signal': '신호',
    'Trigger': '트리거',
    'trigger': '트리거',
    'Summary': '요약',
    'summary': '요약',
    'Analysis': '분석',
    'analysis': '분석',
    'Recommendation': '권고',
    'recommendation': '권고',
    'Warning': '경고',
    'warning': '경고',
    'Error': '오류',
    'error': '오류',
    'Failed': '실패',
    'failed': '실패',
    'Success': '성공',
    'success': '성공',
    'Active': '활성',
    'active': '활성',
    'Inactive': '비활성',
    'inactive': '비활성',
    'Pending': '대기 중',
    'pending': '대기 중',
    'Completed': '완료',
    'completed': '완료',
}

# Pre-compile regex for single-word replacements (longest first to avoid partial match)
_WORD_RE_MAP = []
for _en in sorted(_WORD_EN_TO_KR, key=len, reverse=True):
    _WORD_RE_MAP.append((_re.compile(r'\b' + _re.escape(_en) + r'\b'), _WORD_EN_TO_KR[_en]))


def sanitize_telegram_text(text: str) -> str:
    """텔레그램 전송 전 영어→한국어 치환 + 비율 검사."""
    if not text:
        return text
    result = text
    # Phase 1: multi-word phrases (safe substring replace)
    for en, kr in _PHRASE_EN_TO_KR.items():
        result = result.replace(en, kr)
    # Phase 2: single words with word boundary
    for pattern, kr in _WORD_RE_MAP:
        result = pattern.sub(kr, result)
    ratio = detect_english_ratio(result)
    if ratio > 0.2:
        try:
            print(f'[report_formatter] LANGUAGE_WARNING: english_ratio={ratio:.2f} '
                  f'text_preview={result[:80]!r}', flush=True)
        except Exception:
            pass
    return result


# ── 유틸리티 ─────────────────────────────────────────────

def _kr_action(action: str) -> str:
    """액션을 한국어로 변환. 원문 병기."""
    kr = ACTION_KR.get(action, action)
    if kr == action:
        return action
    return f'{action} ({kr})'


def _kr_action_ctx(action: str, pos_side: str = None) -> str:
    """포지션 상태 인식 액션 변환. NONE+HOLD → 대기, 포지션+HOLD → 유지."""
    if action == 'HOLD' and (not pos_side or pos_side.upper() in ('NONE', '')):
        return 'HOLD (대기)'
    return _kr_action(action)


def _kr_trigger(trigger_type: str) -> str:
    """트리거 타입을 한국어로 변환."""
    return TRIGGER_KR.get(trigger_type, trigger_type)


def _kr_risk(risk: str) -> str:
    """위험도를 한국어로 변환."""
    if not risk:
        return '?'
    return RISK_KR.get(risk.upper(), risk)


def _kr_direction(direction: str) -> str:
    """방향을 한국어로 변환."""
    if not direction:
        return '?'
    return DIRECTION_KR.get(direction, direction)


def _kr_suppress_reason(reason: str) -> str:
    """억제 사유를 한국어로 변환."""
    return SUPPRESS_REASON_KR.get(reason, reason)


def _kr_reason_code(code: str) -> str:
    """reason_code를 한국어로 변환. 매핑 없으면 _ 분리 가독성 변환."""
    if not code:
        return '?'
    kr = REASON_KR.get(code)
    if kr:
        return kr
    return code.replace('_', ' ').title()


def _safe_float(val, default=0.0):
    """안전한 float 변환."""
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _safe_int(val, default=0):
    """안전한 int 변환."""
    if val is None:
        return default
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


def _format_price(price):
    """가격 포매팅."""
    p = _safe_float(price)
    if p == 0:
        return '$0'
    return f'${p:,.1f}'


def _format_pnl(pnl):
    """PnL 포매팅."""
    if pnl is None:
        return 'N/A'
    p = _safe_float(pnl)
    return f'{p:+.4f} USDT'


# ── 뉴스 분석 리포트 ──────────────────────────────────────

CATEGORY_KR = {
    'WAR': '전쟁/지정학',
    'US_POLITICS': '미국 정치',
    'FED_RATES': 'Fed/금리',
    'CPI_JOBS': 'CPI/고용',
    'NASDAQ_EQUITIES': '나스닥/주식',
    'REGULATION_SEC_ETF': '규제/SEC/ETF',
    'JAPAN_BOJ': '일본/BOJ',
    'CHINA': '중국',
    'FIN_STRESS': '금융 스트레스',
    'CRYPTO_SPECIFIC': '크립토',
    'OTHER': '기타',
}

MACRO_CATEGORIES = {
    'FED_RATES', 'CPI_JOBS', 'NASDAQ_EQUITIES', 'US_POLITICS',
    'WAR', 'JAPAN_BOJ', 'CHINA', 'FIN_STRESS',
}
CRYPTO_CATEGORIES = {'CRYPTO_SPECIFIC', 'REGULATION_SEC_ETF'}


def _parse_news_category(summary: str) -> str:
    """Extract category tag from summary field like '[up] [FED_RATES] ...'."""
    if not summary:
        return 'OTHER'
    # Find all bracket tags (case-insensitive)
    tags = _re.findall(r'\[([A-Za-z_]+)\]', summary)
    direction_tags = {'up', 'down', 'neutral'}
    for tag in tags:
        if tag.lower() in direction_tags:
            continue  # skip direction tags
        # Must be a category tag (uppercase convention)
        if tag in CATEGORY_KR:
            return tag
    return 'OTHER'


def _parse_news_direction(summary: str) -> str:
    """Extract direction from summary field."""
    if not summary:
        return ''
    sl = summary.lower()
    if sl.startswith('[up]') or sl.startswith('[bullish]'):
        return '상승'
    elif sl.startswith('[down]') or sl.startswith('[bearish]'):
        return '하락'
    elif sl.startswith('[neutral]'):
        return '중립'
    return ''


def _parse_impact_path(summary: str) -> str:
    """Extract impact_path from summary (after '|')."""
    if not summary or '|' not in summary:
        return ''
    return summary.split('|', 1)[1].strip()


def format_news_analysis(macro_news, crypto_news, news_score,
                         news_guarded, score_trace):
    """뉴스 분석 한국어 리포트 포매팅.

    macro_news: list of dicts (미국/거시 뉴스)
    crypto_news: list of dicts (크립토 뉴스)
    news_score: int (news_event_score)
    news_guarded: bool
    score_trace: str (영향 요약)
    """
    lines = []

    # Macro section
    lines.append('[미국/거시 Top3]')
    if not macro_news:
        lines.append('- 최근 6시간 거시 뉴스 없음')
    else:
        for i, n in enumerate(macro_news[:3], 1):
            impact = _safe_int(n.get('impact_score'))
            title = (n.get('title_ko') or n.get('title') or '')[:80]
            source = n.get('source', '')
            ts = (n.get('ts') or '')[:16]
            summary = n.get('summary', '')
            direction = _parse_news_direction(summary)
            category = _parse_news_category(summary)
            cat_kr = CATEGORY_KR.get(category, category)
            impact_path = _parse_impact_path(summary)

            dir_str = f' / {direction}' if direction else ''
            lines.append(f'{i}) ({impact}/10) {title}')
            lines.append(f'   {source} / {ts} / {cat_kr}{dir_str}')
            if impact_path:
                lines.append(f'   {impact_path}')

    lines.append('')

    # Crypto section
    lines.append('[크립토 Top3]')
    if not crypto_news:
        lines.append('- 최근 6시간 크립토 뉴스 없음')
    else:
        for i, n in enumerate(crypto_news[:3], 1):
            impact = _safe_int(n.get('impact_score'))
            title = (n.get('title_ko') or n.get('title') or '')[:80]
            source = n.get('source', '')
            ts = (n.get('ts') or '')[:16]
            summary = n.get('summary', '')
            direction = _parse_news_direction(summary)
            impact_path = _parse_impact_path(summary)

            dir_str = f' / {direction}' if direction else ''
            lines.append(f'{i}) ({impact}/10) {title}')
            lines.append(f'   {source} / {ts}{dir_str}')
            if impact_path:
                lines.append(f'   {impact_path}')

    lines.append('')

    # Score impact section
    lines.append('[이번 결정에 반영]')
    lines.append(f'- news_event_score: {_safe_float(news_score):+.0f}')
    if news_guarded:
        lines.append('- 뉴스 가드 적용됨')
    if score_trace:
        lines.append(f'- {score_trace}')
    else:
        lines.append('- 뉴스→결정: 변경 없음')

    return '\n'.join(lines)


# ── 전략 보고 ────────────────────────────────────────────

def format_strategy_report(claude_action, parsed, engine_action, engine_reason,
                           scores, pos_state, details, news_items,
                           watch_kw, execute_status, ai_meta,
                           claude_failed=False):
    """전략 보고 전체 포매팅."""
    parsed = parsed or {}
    scores = scores or {}
    pos_state = pos_state or {}
    details = details or {}
    news_items = news_items or []
    watch_kw = watch_kw or []
    ai_meta = ai_meta or {}

    claude_action = claude_action or 'HOLD'
    total = _safe_float(scores.get('total_score'))
    dominant = scores.get('dominant_side', 'LONG')
    stage = _safe_int(scores.get('stage', 1))
    tech = _safe_float(scores.get('tech_score'))
    pos_score = _safe_float(scores.get('position_score'))
    regime = _safe_float(scores.get('regime_score'))
    news_s = _safe_float(scores.get('news_event_score'))

    lines = []

    # ── [📌 요약] ──
    ps_side = (pos_state.get('side') or '').upper() if pos_state and pos_state.get('side') else 'NONE'

    lines.append('[📌 요약]')
    lines.append(f'- 최종: {_kr_action_ctx(claude_action, ps_side)}')

    if ps_side and ps_side != 'NONE':
        qty = _safe_float(pos_state.get('total_qty'))
        entry = _safe_float(pos_state.get('avg_entry_price'))
        lines.append(f'- 포지션: {ps_side} {qty} BTC @ {_format_price(entry)}')
    else:
        lines.append('- 포지션: 없음')

    # 참조신호: 디버그 모드에서만 표시
    if is_debug_on():
        lines.append(f'- 참조신호: {dominant} stage{stage} (총점 {total:+.1f})')

    # Claude 실패 시 SKIP 한 줄만, 성공 시 확신도+근거 표시
    if claude_failed:
        lines.append('- Claude: SKIP(API fail)')
    else:
        confidence = parsed.get('confidence')
        reason_code = parsed.get('reason_code', '')
        if confidence is not None:
            lines.append(f'- 확신도: {confidence}')
        if reason_code:
            lines.append(f'- 근거: {_kr_reason_code(reason_code)}')

    # ── [📊 점수 상세] ──
    lines.append('')
    lines.append('[📊 점수 상세]')
    lines.append(f'- TECH: {tech:+.0f} | POS: {pos_score:+.0f} | '
                 f'REGIME: {regime:+.0f} | NEWS: {news_s:+.0f}')
    lines.append(f'- 엔진 참조: {_kr_action(engine_action or "HOLD")}')
    if engine_reason:
        lines.append(f'  ({engine_reason})')

    # ── [📰 뉴스 요약] ──
    lines.append('')
    lines.append('[📰 뉴스 요약]')
    display_news = [n for n in news_items if n.get('relevance', 'MED') != 'LOW']
    if not display_news:
        lines.append('- 최근 6시간 고영향 뉴스 없음')
    else:
        for i, n in enumerate(display_news[:3], 1):
            impact = _safe_int(n.get('impact_score'))
            title = (n.get('title_ko') or n.get('title') or '')[:80]
            source = n.get('source', '')
            ts = (n.get('ts') or '')[:16]
            summary = n.get('summary', '')
            direction_tag = _parse_news_direction(summary)
            dir_str = f' / {direction_tag}' if direction_tag else ''
            lines.append(f'{i}) ({impact}/10) {title} / {source} / {ts}{dir_str}')

        # Watchlist 매칭
        matched = set()
        for n in display_news:
            text_lower = ((n.get('title') or '') + ' ' + (n.get('summary') or '')).lower()
            for kw in watch_kw:
                if kw in text_lower:
                    matched.add(kw)
        if matched:
            lines.append(f'- watchlist 매칭: {", ".join(sorted(matched))}')
        else:
            lines.append('- watchlist 매칭: 없음')

    # ── [🧠 판단 근거] (뉴스→결정 영향) ──
    lines.append('')
    lines.append('[🧠 판단 근거]')
    news_score = _safe_float(scores.get('news_event_score'))
    guarded = scores.get('news_event_guarded', False)
    if guarded or news_score == 0:
        lines.append(f'- 뉴스 영향: 없음 (score={news_score:+.0f}'
                     f'{", 가드 적용" if guarded else ""})')
        lines.append('- 뉴스→결정: 변경 없음')
    else:
        direction = '상승' if news_score > 0 else '하락'
        mag_abs = abs(news_score)
        magnitude = '약' if mag_abs < 20 else ('보통' if mag_abs < 50 else '강')
        lines.append(f'- 뉴스 영향: {magnitude} {direction} (score={news_score:+.0f})')
        if claude_action != engine_action:
            lines.append(f'- 뉴스→결정: {_kr_action(engine_action or "HOLD")} → '
                         f'{_kr_action(claude_action)} 변경')
        else:
            lines.append('- 뉴스→결정: 변경 없음')

    # ── [🎯 핵심 레벨] ──
    sl_dist = details.get('sl_dist_pct')
    sl_pct = details.get('stop_loss_pct', 2.0)
    if sl_dist is not None:
        lines.append('')
        lines.append('[🎯 핵심 레벨]')
        lines.append(f'- 손절: -{sl_pct}% | 현재 거리: {sl_dist:+.1f}%')

    # ── [⚠ 실행] ──
    if claude_action == 'REDUCE':
        reduce_pct = parsed.get('reduce_pct', 0)
        lines.append('')
        lines.append(f'[⚠ 실행] {_kr_action(claude_action)} {reduce_pct}%')
    elif claude_action in ('OPEN_LONG', 'OPEN_SHORT'):
        target_stage = parsed.get('target_stage', 1)
        lines.append('')
        lines.append(f'[⚠ 실행] {_kr_action(claude_action)} stage={target_stage}')

    lines.append(f'- 실행: {execute_status or "NO"}')

    # ── 디버그 정보 ──
    debug = _debug_line({
        'provider': ai_meta.get('model_provider', ''),
        'cost': ai_meta.get('estimated_cost_usd', 0),
        'latency': ai_meta.get('api_latency_ms', 0),
        'model': ai_meta.get('model', ''),
    })
    if debug:
        lines.append(debug)

    return '\n'.join(lines)


# ── 전략 판단 알림 ───────────────────────────────────────

def format_decision_alert(action, parsed, engine_action, scores, pos_state,
                          claude_failed=False):
    """전략 판단 알림 포매팅."""
    parsed = parsed or {}
    scores = scores or {}
    pos_state = pos_state or {}
    action = action or 'HOLD'

    total = _safe_float(scores.get('total_score'))
    side = (pos_state.get('side') or 'none').upper() if pos_state.get('side') else 'NONE'
    qty = _safe_float(pos_state.get('total_qty'))

    lines = [
        f'[📋 전략 판단]',
        f'- 최종: {_kr_action_ctx(action, side)}',
        f'- 엔진: {_kr_action(engine_action or "HOLD")} | 총점: {total:+.1f}',
    ]
    if claude_failed:
        lines.append('- Claude: SKIP(API fail)')
    else:
        lines.append(f'- 확신도: {parsed.get("confidence", "?")}')
        lines.append(f'- 근거: {_kr_reason_code(parsed.get("reason_code", "?"))}')
    lines.append(f'- 포지션: {side} {qty} BTC')
    return '\n'.join(lines)


# ── 실행 대기열 알림 ──────────────────────────────────────

def format_enqueue_alert(eq_id, action, parsed, pos_state):
    """실행 대기열 알림 포매팅."""
    parsed = parsed or {}
    action = action or '?'

    qty_info = ''
    if action == 'REDUCE':
        qty_info = f'축소 {parsed.get("reduce_pct", 0)}%'
    elif action in ('OPEN_LONG', 'OPEN_SHORT'):
        qty_info = f'stage={parsed.get("target_stage", 1)}'

    lines = [
        f'[⏳ 실행 대기]',
        f'- 액션: {_kr_action(action)}',
        f'- 대기열 ID: {eq_id}',
    ]
    if qty_info:
        lines.append(f'- 상세: {qty_info}')
    lines.append(f'- 근거: {_kr_reason_code(parsed.get("reason_code", "?"))}')
    return '\n'.join(lines)


# ── 긴급 알림 ────────────────────────────────────────────

def format_emergency_pre_alert(trigger_type, trigger_detail):
    """긴급 감지 사전 알림."""
    trigger_detail = trigger_detail or {}
    detail_str = ''
    if isinstance(trigger_detail, dict):
        parts = []
        for k, v in list(trigger_detail.items())[:4]:
            parts.append(f'{k}={v}')
        detail_str = ', '.join(parts)
    else:
        detail_str = str(trigger_detail)[:200]

    return (
        f'🚨 긴급 감지 → Claude 분석 중\n'
        f'- 유형: {_kr_trigger(trigger_type or "unknown")}\n'
        f'- 상세: {detail_str}'
    )


def format_emergency_post_alert(trigger_type, action, result):
    """긴급 조치 결과 알림."""
    result = result or {}
    action = action or 'HOLD'
    risk = _kr_risk(result.get('risk_level', ''))
    confidence = result.get('confidence', '?')

    reason_bullets = result.get('reason_bullets', [])
    reason_code = result.get('reason_code', '')
    reason = ', '.join(reason_bullets[:2]) if reason_bullets else reason_code

    lines = [f'🚨 긴급 조치: {_kr_action(action)}']
    lines.append(f'- 위험도: {risk}')
    lines.append(f'- 확신도: {confidence}')
    if reason:
        lines.append(f'- 근거: {reason}')

    if action == 'REDUCE':
        reduce_pct = result.get('reduce_pct', 50)
        lines.append(f'- 축소: {reduce_pct}%')
    elif action in ('OPEN_LONG', 'OPEN_SHORT'):
        target_stage = result.get('target_stage', 1)
        lines.append(f'- stage: {target_stage}')

    return '\n'.join(lines)


# ── 이벤트 알림 ──────────────────────────────────────────

def format_event_pre_alert(trigger_types, mode, model='claude', snapshot=None):
    """이벤트 감지 사전 알림."""
    trigger_types = trigger_types or []
    kr_types = [_kr_trigger(t) for t in trigger_types]
    model_label = MODEL_LABEL_KR.get(model, model)

    # 방향 표기: price_spike / volume_spike 트리거 시 ret_5m 기반 급등/급락 표시
    price_line = ''
    if snapshot:
        ret_5m = (snapshot.get('returns') or {}).get('ret_5m')
        if ret_5m is not None:
            spike_triggers = {'price_spike_1m', 'price_spike_5m',
                              'price_spike_15m', 'volume_spike'}
            if any(t in spike_triggers for t in trigger_types):
                direction = '급등' if ret_5m > 0 else '급락'
                price_line = f'\n- 가격: 5m {ret_5m:+.1f}% ({direction})'

    return (
        f'📡 이벤트 감지 → {model_label}\n'
        f'- 트리거: {", ".join(kr_types) or "?"}\n'
        f'- 모드: {mode or "?"}'
        f'{price_line}'
    )


def format_event_post_alert(trigger_types, action, result):
    """이벤트 조치 결과 알림."""
    result = result or {}
    action = action or 'HOLD'
    trigger_types = trigger_types or []
    kr_types = [_kr_trigger(t) for t in trigger_types]

    risk = _kr_risk(result.get('risk_level', ''))
    reason_bullets = result.get('reason_bullets', [])
    reason_code = result.get('reason_code', '')
    reason = ', '.join(reason_bullets[:2]) if reason_bullets else reason_code

    lines = [f'📡 이벤트 조치: {_kr_action(action)}']
    lines.append(f'- 트리거: {", ".join(kr_types) or "?"}')
    if risk and risk != '?':
        lines.append(f'- 위험도: {risk}')
    if reason:
        lines.append(f'- 근거: {reason}')

    if action == 'REDUCE':
        reduce_pct = result.get('reduce_pct', 50)
        lines.append(f'- 축소: {reduce_pct}%')
    elif action in ('OPEN_LONG', 'OPEN_SHORT'):
        target_stage = result.get('target_stage', 1)
        lines.append(f'- stage: {target_stage}')

    return '\n'.join(lines)


def format_async_claude_result(action, result, reason):
    """Claude 비동기 분석 결과 한국어 포매팅."""
    result = result or {}
    action = action or 'HOLD'
    confidence = result.get('confidence', '?')
    risk = _kr_risk(result.get('risk_level', ''))
    reason_bullets = result.get('reason_bullets', [])
    reason_code = result.get('reason_code', '')
    detail = ', '.join(reason_bullets[:2]) if reason_bullets else reason_code

    lines = [f'🧠 비동기 Claude 분석 완료 (claude_waited=false)']
    lines.append(f'- 조치: {_kr_action(action)}')
    lines.append(f'- 확신도: {confidence}')
    if risk and risk != '?':
        lines.append(f'- 위험도: {risk}')
    if detail:
        lines.append(f'- 근거: {detail}')
    lines.append(f'- 트리거: {reason or "?"}')

    if action == 'REDUCE':
        reduce_pct = result.get('reduce_pct', 50)
        lines.append(f'- 축소: {reduce_pct}%')

    return '\n'.join(lines)


# ── 서비스/이벤트/예산 알림 ────────────────────────────────

def format_service_start(build_sha, config_version, features=None):
    """서비스 시작 알림."""
    lines = [
        '🚀 서비스 시작',
        f'- 빌드: {build_sha}',
        f'- 설정: {config_version}',
    ]
    if features:
        for k, v in features.items():
            lines.append(f'- {k}: {v}')
    return '\n'.join(lines)


def format_event_suppressed(trigger_types, reason, remaining_sec=0, detail=None):
    """이벤트 억제 알림."""
    trigger_types = trigger_types or []
    kr_types = [_kr_trigger(t) for t in trigger_types]
    reason_kr = _kr_suppress_reason(reason)
    lines = [
        f'🚫 이벤트 억제: {reason_kr}',
        f'- 트리거: {", ".join(kr_types) or "?"}',
    ]
    if remaining_sec > 0:
        lines.append(f'- 잔여 시간: {remaining_sec}초')
    if is_debug_on() and detail:
        parts = [f'{k}={v}' for k, v in detail.items()]
        lines.append(f'- 상세: {", ".join(parts)}')
    return '\n'.join(lines)


def format_gpt_mini_fallback(trigger_types, gate_reason):
    """GPT-mini 전환 알림."""
    trigger_types = trigger_types or []
    kr_types = [_kr_trigger(t) for t in trigger_types]
    return (
        f'⚡ 빠른 분석(GPT-mini) 전환\n'
        f'- 트리거: {", ".join(kr_types) or "?"}\n'
        f'- 사유: {gate_reason or "?"}'
    )


def format_hold_suppress_notice(symbol, count, ttl_min, trigger_types):
    """HOLD 반복 억제 알림."""
    trigger_types = trigger_types or []
    kr_types = [_kr_trigger(t) for t in trigger_types]
    return (
        f'🚫 HOLD 반복 억제\n'
        f'- 종목: {symbol}\n'
        f'- 연속 HOLD: {count}회\n'
        f'- 억제 시간: {ttl_min}분\n'
        f'- 트리거: {", ".join(kr_types) or "?"}\n'
        f'- Claude 호출 차단 (락 만료 후 재개)'
    )


def format_budget_exceeded(reason, daily_report=''):
    """Claude 예산 초과 알림."""
    lines = [
        f'⚠️ Claude 예산 초과',
        f'- 사유: {reason}',
    ]
    if daily_report:
        lines.append('')
        lines.append(daily_report)
    return '\n'.join(lines)


def format_daily_cost_report(today, daily_calls, daily_limit,
                             daily_cost, daily_cost_limit,
                             monthly_cost, monthly_cost_limit,
                             auto_c=0, user_c=0, emerg_c=0,
                             error_remaining=0):
    """Claude 사용 리포트."""
    lines = [
        '=== Claude 사용 리포트 ===',
        f'날짜: {today}',
        f'일일 호출: {daily_calls}/{daily_limit}',
        f'일일 비용: ${daily_cost:.4f}/${daily_cost_limit}',
        f'월간 비용: ${monthly_cost:.4f}/${monthly_cost_limit}',
        f'호출 유형: 자동={auto_c} 사용자={user_c} 긴급={emerg_c}',
    ]
    if error_remaining > 0:
        lines.append(f'오류 차단: {error_remaining}초 남음')
    return '\n'.join(lines)


def format_lock_stats_report(hours, caller_stats, lock_stats):
    """Claude 호출 통계."""
    lines = [
        f'=== Claude 호출 통계 (최근 {hours}시간) ===',
    ]
    if not caller_stats:
        lines.append('기록된 호출 없음')
    else:
        total_calls = sum(s['total_calls'] for s in caller_stats)
        total_cost = sum(s['total_cost'] for s in caller_stats)
        lines.append(f'합계: {total_calls}회, ${total_cost:.4f}')
        lines.append('')
        for s in caller_stats:
            lines.append(
                f"  {s['caller']}: {s['allowed_calls']}회 허용 / "
                f"{s['denied_calls']}회 거부 / ${s['total_cost']:.4f}")

    lines.append(f'\n=== 활성 락 ===')
    lines.append(f"합계: {lock_stats.get('total_active', 0)}")
    lock_type_kr = {'event': '이벤트', 'hash': '해시', 'hold_sup': 'HOLD 억제'}
    for lt in ('event', 'hash', 'hold_sup'):
        cnt = lock_stats.get(lt, 0)
        if cnt:
            lines.append(f"  {lock_type_kr.get(lt, lt)}: {cnt}")

    return '\n'.join(lines)


# ── 체결 알림 ────────────────────────────────────────────

def format_fill_notify(fill_type, **kwargs):
    """체결 알림 포매팅 (8종).

    fill_type: entry, exit, timeout, canceled, add, reduce, reverse_close, reverse_open
    """
    if fill_type == 'entry':
        return _fill_entry(**kwargs)
    elif fill_type == 'exit':
        return _fill_exit(**kwargs)
    elif fill_type == 'timeout':
        return _fill_timeout(**kwargs)
    elif fill_type == 'canceled':
        return _fill_canceled(**kwargs)
    elif fill_type == 'add':
        return _fill_add(**kwargs)
    elif fill_type == 'reduce':
        return _fill_reduce(**kwargs)
    elif fill_type == 'reverse_close':
        return _fill_reverse_close(**kwargs)
    elif fill_type == 'reverse_open':
        return _fill_reverse_open(**kwargs)
    return f'[체결 알림] 유형: {fill_type}'


def _fill_entry(direction='', avg_price=0, filled_qty=0, fee_cost=0,
                fee_currency='', signal_id=None, start_stage=1,
                entry_pct=10, next_stage=2, pos_side=None, pos_qty=0,
                **_extra):
    pos_str = f'{pos_side} {pos_qty} BTC' if pos_side else 'NONE'
    sig_str = str(signal_id) if signal_id else 'N/A'
    budget_remain = 70 - _safe_float(entry_pct)
    return (
        f'✅ 진입 체결 완료\n'
        f'- 방향: {(direction or "?").upper()}\n'
        f'- 체결가: {_format_price(avg_price)}\n'
        f'- 수량: {filled_qty} BTC\n'
        f'- 수수료: {_safe_float(fee_cost):.4f} {fee_currency}\n'
        f'- 시그널: {sig_str}\n'
        f'- 단계: stage {start_stage} ({_safe_float(entry_pct):.0f}%)\n'
        f'- 잔여 예산: {budget_remain:.0f}% (stage{next_stage}~7)\n'
        f'- 포지션: {pos_str}'
    )


def _fill_exit(order_type='', direction='', avg_price=0, filled_qty=0,
               fee_cost=0, fee_currency='', realized_pnl=None,
               pos_side=None, pos_qty=0, close_reason='', **_extra):
    pos_str = f'{pos_side} {pos_qty} BTC' if pos_side else 'NONE'
    return (
        f'✅ 정리 체결 완료\n'
        f'- 유형: {order_type} {(direction or "").upper()}\n'
        f'- 체결가: {_format_price(avg_price)}\n'
        f'- 수량: {filled_qty} BTC\n'
        f'- 수수료: {_safe_float(fee_cost):.4f} {fee_currency}\n'
        f'- 손익: {_format_pnl(realized_pnl)}\n'
        f'- 포지션: {pos_str}\n'
        f'- 사유: {close_reason or "N/A"}'
    )


def _fill_timeout(order_type='', direction='', order_id='',
                  timeout_sec=60, **_extra):
    return (
        f'⏰ 주문 미체결 타임아웃\n'
        f'- 유형: {order_type} {(direction or "").upper()}\n'
        f'- 주문ID: {order_id}\n'
        f'- {timeout_sec}초 내 체결 안 됨\n'
        f'- 수동 확인 필요'
    )


def _fill_canceled(order_type='', direction='', order_id='', **_extra):
    return (
        f'❌ 주문 취소됨\n'
        f'- 유형: {order_type} {(direction or "").upper()}\n'
        f'- 주문ID: {order_id}\n'
        f'- 거래소에서 취소됨\n'
        f'- 수동 확인 필요'
    )


def _fill_add(direction='', avg_price=0, filled_qty=0, fee_cost=0,
              fee_currency='', new_stage='?', pos_side=None, pos_qty=0,
              budget_used_pct=0, budget_remaining=70, **_extra):
    return (
        f'✅ 추가 진입 체결 — {(direction or "?").upper()} ADD (stage {new_stage}/7)\n'
        f'- 체결가: {_format_price(avg_price)}\n'
        f'- 추가: {filled_qty} BTC\n'
        f'- 수수료: {_safe_float(fee_cost):.4f} {fee_currency}\n'
        f'- 총 포지션: {pos_side} {pos_qty} BTC\n'
        f'- 예산: {_safe_float(budget_used_pct):.0f}%/70% '
        f'(잔여 {_safe_float(budget_remaining):.0f}%)'
    )


def _fill_reduce(direction='', avg_price=0, filled_qty=0, fee_cost=0,
                 fee_currency='', realized_pnl=None, pos_side=None,
                 pos_qty=0, close_reason='', **_extra):
    return (
        f'✅ 부분 축소 체결\n'
        f'- {(direction or "?").upper()} REDUCE\n'
        f'- 체결가: {_format_price(avg_price)}\n'
        f'- 축소: {filled_qty} BTC\n'
        f'- 수수료: {_safe_float(fee_cost):.4f} {fee_currency}\n'
        f'- 손익: {_format_pnl(realized_pnl)}\n'
        f'- 남은 포지션: {pos_side} {pos_qty} BTC\n'
        f'- 사유: {close_reason or "N/A"}'
    )


def _fill_reverse_close(direction='', avg_price=0, filled_qty=0,
                         realized_pnl=None, position_verified=False,
                         pos_side=None, pos_qty=0, **_extra):
    pos_str = 'NONE' if position_verified else f'{pos_side} {pos_qty}'
    return (
        f'✅ 리버스 정리 완료\n'
        f'- {(direction or "?").upper()} REVERSE_CLOSE\n'
        f'- 체결가: {_format_price(avg_price)}\n'
        f'- 수량: {filled_qty} BTC\n'
        f'- 손익: {_format_pnl(realized_pnl)}\n'
        f'- 포지션: {pos_str}'
    )


def _fill_reverse_open(direction='', avg_price=0, filled_qty=0,
                        from_side='', pos_side=None, pos_qty=0,
                        entry_pct=10, start_stage=1, **_extra):
    budget_remain = 70 - _safe_float(entry_pct)
    return (
        f'✅ 리버스 진입 완료 — {from_side} → {(direction or "?").upper()}\n'
        f'- 체결가: {_format_price(avg_price)}\n'
        f'- 수량: {filled_qty} BTC\n'
        f'- 포지션: {pos_side} {pos_qty} BTC\n'
        f'- 예산: {_safe_float(entry_pct):.0f}%/70% '
        f'(stage{start_stage}, 잔여 {budget_remain:.0f}%)'
    )


# ── 뉴스→전략 통합 리포트 ────────────────────────────────

def format_news_strategy_report(data, detail=False):
    """7섹션 고정 한국어 뉴스→전략 리포트.

    data: news_strategy_report.build_report_data() 반환값.
    detail: True면 TOP 5 + 확장 trace.
    AI는 1줄요약+리스크만 담당. 나머지 6섹션은 데이터 기반.
    """
    snap = data.get('snapshot', {})
    scores = data.get('scores', {})
    pos = data.get('position', {})
    macro_news = data.get('macro_news', [])
    crypto_news = data.get('crypto_news', [])
    stats = data.get('stats', {})
    watch = data.get('watch_matches', [])
    trace_str = data.get('news_score_trace', '')
    constraints = data.get('action_constraints', {})
    ai = data.get('ai_summary', {})

    top_n = 5 if detail else 3
    lines = []

    # ── [📌 1줄 요약] (AI 생성) ──
    lines.append('[📌 1줄 요약]')
    one_liner = ai.get('one_liner', '')
    risk_level = ai.get('risk_level', '')
    if one_liner:
        lines.append(one_liner)
    else:
        # Fallback: data-driven summary
        side = scores.get('dominant_side', 'LONG')
        stage = scores.get('stage', 1)
        total = scores.get('total', 0)
        action = 'HOLD' if not pos.get('side') else '유지'
        lines.append(f'{side} stg{stage} | {action} | 총점 {total:+.1f}')

    # ── [📊 시장 스냅샷] ──
    lines.append('')
    lines.append('[📊 시장 스냅샷]')
    price = snap.get('price', 0)
    h24 = snap.get('high_24h', 0)
    l24 = snap.get('low_24h', 0)
    ret1h = snap.get('ret_1h')
    ret4h = snap.get('ret_4h')
    ret_parts = []
    if ret1h is not None:
        ret_parts.append(f'1h:{ret1h:+.1f}%')
    if ret4h is not None:
        ret_parts.append(f'4h:{ret4h:+.1f}%')
    ret_str = ' '.join(ret_parts)
    lines.append(f'BTC ${price:,.0f} (24h H:${h24:,.0f} L:${l24:,.0f}) {ret_str}')

    bb_mid = snap.get('bb_mid', 0)
    bb_up = snap.get('bb_up', 0)
    bb_dn = snap.get('bb_dn', 0)
    bb_bw = round(bb_up - bb_dn) if bb_up and bb_dn else 0
    tenkan = snap.get('ich_tenkan', 0)
    kijun = snap.get('ich_kijun', 0)
    tk_rel = '<' if tenkan < kijun else '>'
    lines.append(f'BB(mid:{bb_mid:,.0f} 폭:{bb_bw:,.0f}) '
                 f'Ich(tenkan:{tenkan:,.0f} {tk_rel} kijun:{kijun:,.0f})')

    total = scores.get('total', 0)
    side = scores.get('dominant_side', 'LONG')
    stage = scores.get('stage', 1)
    tech = scores.get('tech', 0)
    pos_s = scores.get('pos', 0)
    regime = scores.get('regime', 0)
    news_s = scores.get('news_event', 0)
    lines.append(f'Score: TOTAL {total:+.1f} -> {side} stg{stage} | '
                 f'TECH:{tech:+.0f} POS:{pos_s:+.0f} REG:{regime:+.0f} NEWS:{news_s:+.0f}')

    # ── [📰 미국/거시 TOP N] ──
    lines.append('')
    lines.append(f'[📰 미국/거시 TOP {min(len(macro_news), top_n)}]')
    if not macro_news:
        lines.append('- 최근 6시간 거시 뉴스 없음')
    else:
        for i, n in enumerate(macro_news[:top_n], 1):
            _append_news_item(lines, i, n)

    # ── [🪙 크립토 TOP N] ──
    lines.append('')
    lines.append(f'[🪙 크립토 TOP {min(len(crypto_news), top_n)}]')
    if not crypto_news:
        lines.append('- 최근 6시간 크립토 뉴스 없음')
    else:
        for i, n in enumerate(crypto_news[:top_n], 1):
            _append_news_item(lines, i, n)

    # ── [🧩 뉴스→전략 TRACE] ──
    lines.append('')
    lines.append('[🧩 뉴스→전략 TRACE]')
    news_event = scores.get('news_event', 0)
    news_w = scores.get('weights', {}).get('news_event_w', 0.05)
    contribution = abs(news_event * news_w)
    if trace_str:
        lines.append(f'점수: {news_event:+.0f} ({trace_str})')
    else:
        lines.append(f'점수: {news_event:+.0f}')
    lines.append(f'가중치: {news_w} ({contribution:.1f}p 기여)')

    guarded = scores.get('news_guarded', False)
    if guarded:
        lines.append('뉴스 가드 적용 (TECH+POS 중립)')

    # Action constraints
    c_parts = []
    if not constraints.get('can_open', True):
        c_parts.append('OPEN 불가')
    if not constraints.get('can_reverse', True):
        c_parts.append('REVERSE 불가')
    if c_parts:
        lines.append(f'제약: 뉴스 단독 {"/".join(c_parts)}')

    if watch:
        lines.append(f'watchlist: {", ".join(watch)}')

    # ── [🎯 핵심 레벨] ──
    lines.append('')
    lines.append('[🎯 핵심 레벨]')
    support_lines = []
    resist_lines = []
    if bb_dn:
        support_lines.append(f'${bb_dn:,.0f}(BB하단)')
    val = snap.get('val', 0)
    if val:
        support_lines.append(f'${val:,.0f}(VAL)')
    if bb_up:
        resist_lines.append(f'${bb_up:,.0f}(BB상단)')
    vah = snap.get('vah', 0)
    if vah:
        resist_lines.append(f'${vah:,.0f}(VAH)')
    if kijun:
        resist_lines.append(f'${kijun:,.0f}(Kijun)')

    if support_lines:
        lines.append(f'지지: {" / ".join(support_lines)}')
    if resist_lines:
        lines.append(f'저항: {" / ".join(resist_lines)}')

    sl_pct = pos.get('sl_pct', scores.get('dynamic_sl', 2.0))
    sl_price = pos.get('sl_price')
    sl_dist = pos.get('sl_dist')
    if pos.get('side'):
        sl_parts = [f'손절: -{sl_pct}%']
        if sl_price:
            sl_parts.append(f'(${sl_price:,.0f})')
        if sl_dist is not None:
            sl_parts.append(f'| 거리: {sl_dist:+.1f}%')
        lines.append(' '.join(sl_parts))
    else:
        lines.append(f'손절 기준: -{sl_pct}%')

    # ── [⚠ 리스크/다음 체크] ──
    lines.append('')
    lines.append('[⚠ 리스크/다음 체크]')
    bull = stats.get('bullish', 0)
    bear = stats.get('bearish', 0)
    if not risk_level:
        if bear >= 3 and bear > bull * 2:
            risk_level = '높음'
        elif bear > bull:
            risk_level = '보통'
        else:
            risk_level = '낮음'
    lines.append(f'리스크: {risk_level} (거시 하락 {bear}건 vs 상승 {bull}건, regime={regime:+.0f})')

    watch_items = ai.get('watch_items', [])
    next_check = ai.get('next_check', '')
    if watch_items:
        lines.append(f'모니터링: {", ".join(watch_items)}')
    elif next_check:
        lines.append(f'모니터링: {next_check}')

    return '\n'.join(lines)


def _append_news_item(lines, idx, n):
    """Append a single news item to report lines."""
    impact = _safe_int(n.get('impact_score'))
    title = (n.get('title_ko') or n.get('title') or '')[:70]
    cat_kr = n.get('category_kr') or CATEGORY_KR.get(
        _parse_news_category(n.get('summary', '')), '')
    direction = n.get('direction') or _parse_news_direction(n.get('summary', ''))
    source = n.get('source', '')
    ts = n.get('ts', '')

    dir_str = f' / {direction}' if direction else ''
    lines.append(f'{idx}) ({impact}/10) {title} — {cat_kr}{dir_str}')
    lines.append(f'   {source} {ts}')

    impact_path = n.get('impact_path', '')
    if impact_path:
        lines.append(f'   {impact_path}')

    # Trace data
    trace = n.get('trace', {})
    if trace:
        ret_30m = trace.get('btc_ret_30m')
        ret_2h = trace.get('btc_ret_2h')
        label = trace.get('label', '')
        z = trace.get('spike_zscore')

        parts = []
        if ret_30m is not None:
            parts.append(f'30m {ret_30m:+.1f}%')
        else:
            parts.append('30m 집계 중')
        if ret_2h is not None:
            parts.append(f'2h {ret_2h:+.1f}%')
        else:
            parts.append('2h 집계 중')
        if label:
            label_str = label
            if z is not None:
                label_str += f' (z={z:.1f})'
            parts.append(label_str)
        lines.append(f'   ▸ {" | ".join(parts)}')
