"""
strategy_core.py — 순수 매매 전략 로직 요약본
==============================================

프로젝트 전체에서 순수 전략 로직만 추출하여 정리한 참조 문서.
실제 실행 파일이 아닌, 전략 이해를 위한 참조용 파일.

소스 파일 매핑:
  - 레짐 판단:    strategy_v3/regime_v3.py
  - 스코어 수정:  strategy_v3/score_v3.py
  - 리스크 관리:  strategy_v3/risk_v3.py
  - 적응형 방어:  strategy_v3/adaptive_v3.py
  - 피처 계산:    strategy/common/features.py
  - V2 전략 모드: strategy/modes/static_range.py, volatile_range.py, shock_breakout.py
  - V2 라우팅:    strategy/regime_router.py
  - 스코어 엔진:  score_engine.py, direction_scorer.py
  - 오토파일럿:   autopilot_daemon.py
"""


# ═══════════════════════════════════════════════════════════════
# 1. 레짐(Regime) 판단 로직
# ═══════════════════════════════════════════════════════════════
#
# 시장을 4가지 상태로 분류하여 각 상태에 맞는 전략을 적용.
# 소스: strategy_v3/regime_v3.py → classify()
#
# ── 레짐 분류 기준 ──────────────────────────────────────────
#
# [1] STATIC_RANGE (횡보장) → MeanRev (평균회귀) 전략
#     조건: drift_score <= 0.0003 AND ADX <= 20
#     의미: 가격이 일정 범위 안에서 움직이는 박스권
#     전략: VA(Value Area) 가장자리에서 반대 방향 진입
#
# [2] DRIFT_UP / DRIFT_DOWN (추세 시작) → DriftFollow 전략
#     조건: drift_score >= 0.0008 AND drift_direction이 UP/DOWN
#           AND ADX < breakout 임계값(28)
#     의미: POC(Point of Control)가 한 방향으로 이동 중
#     전략: 추세 방향으로만 진입, 반대 방향 차단
#
# [3] BREAKOUT (돌파장) → BreakoutTrend 전략
#     조건 (strict 모드, 3중 확인):
#       - structure_breakout: 최근 N개 캔들 중 M개가 VA 밖에서 종가 형성
#       - volume_z >= 1.0: 거래량이 평균 대비 1 표준편차 이상
#       - atr_ratio >= 1.5: ATR이 최근 평균 대비 1.5배 이상 확대
#     대안 경로: ADX >= 28 + (BB확대 OR 거래량 스파이크) 중 1개
#     의미: 기존 가격대를 이탈하는 강한 방향성 움직임
#     전략: 돌파 방향 추종, 추격매수 방지, 리테스트 진입
#
# ── 히스테리시스 (레짐 전환 안정화) ─────────────────────────
#
# 레짐이 빠르게 왔다갔다 하는 것을 방지:
#   - regime_confirm_bars = 3: 새 레짐이 3번 연속 확인되어야 전환
#   - regime_min_dwell_sec = 300: 최소 5분간 현재 레짐 유지
#   - 첫 번째 호출 시에는 즉시 수용

REGIME_PARAMS = {
    'drift_static_max': 0.0003,     # 이 이하면 STATIC_RANGE
    'drift_trend_min': 0.0008,      # 이 이상이면 DRIFT
    'adx_range_max': 20,            # 이 이하면 레인지 장세
    'adx_breakout_min': 28,         # 이 이상이면 브레이크아웃 후보
    'regime_confirm_bars': 3,       # 레짐 전환에 필요한 연속 확인 횟수
    'regime_min_dwell_sec': 300,    # 최소 레짐 유지 시간 (초)
    'breakout_bb_expand_min': 1.3,  # BB 확대 비율 임계값
    'breakout_volume_z_min': 1.5,   # 거래량 z-score 임계값
}


# ═══════════════════════════════════════════════════════════════
# 2. 방향 스코어링 (Direction Scoring)
# ═══════════════════════════════════════════════════════════════
#
# 4축 가중 합산으로 매매 방향과 강도를 결정.
# 소스: score_engine.py → compute_total()
#
# 공식: TOTAL = 0.75*TECH + 0.10*POSITION + 0.10*REGIME + 0.05*NEWS
#
# 각 축은 -100(숏) ~ +100(롱) 범위
#
# [TECH 축] (가중치 75%) — tech_scorer.py
#   - 이치모쿠: 전환선/기준선 교차 (0~30점)
#   - 볼린저밴드: 가격의 밴드 내 위치 (0~25점)
#   - 거래량 스파이크 유무 (0~15점)
#   - 5분 모멘텀: 최근 5개 캔들의 가격 변화율 (0~10점)
#
# [POSITION 축] (가중치 10%) — position_scorer.py
#   - 현재 포지션 방향/사이즈 기반 바이어스
#
# [REGIME 축] (가중치 10%) — price events 기반
#   - 최근 48시간 변동성 스파이크 이벤트의 시간가중 방향성
#   - 과거 유사 패턴의 지속률(continuation_rate) 참조
#
# [NEWS 축] (가중치 5%) — 뉴스 감성 분석
#   - 가드: TECH+POSITION 모두 중립(abs<10)이면 NEWS 무시
#   - 뉴스만으로는 절대 트레이드를 트리거하지 않음
#
# ── 스코어 → 신호 단계 매핑 ────────────────────────────────
#   abs_score >= 75 → stg3 (강한 신호)
#   abs_score >= 45 → stg2 (중간 신호)
#   abs_score >= 10 → stg1 (약한 신호)
#   abs_score < 10  → stg0 (신호 없음)

SCORE_WEIGHTS = {
    'tech_w': 0.75,
    'position_w': 0.10,
    'regime_w': 0.10,
    'news_event_w': 0.05,
}

STAGE_THRESHOLDS = [
    (75, 'stg3'),  # 강한 확신
    (45, 'stg2'),  # 중간 확신
    (10, 'stg1'),  # 약한 확신
    (0,  'stg0'),  # 무신호
]


# ═══════════════════════════════════════════════════════════════
# 3. 진입 조건 (Entry Conditions)
# ═══════════════════════════════════════════════════════════════
#
# autopilot_daemon.py의 메인 루프에서 20초마다 실행.
# V3 → V2(shadow) 순서로 평가.

# ── 공통 진입 게이트 (모든 레짐 공통) ──────────────────────
#
# 다음 조건을 모두 통과해야 진입 가능:
#
# (1) KILL_SWITCH 미작동
# (2) trade_switch = ON
# (3) confidence >= 35 (V3 최소 임계값)
#     - conf 35~49: stage1만 허용 (최소 사이즈)
#     - conf >= 50: 정상 진입
# (4) spread_ok = True (스프레드가 0.05% 미만)
# (5) liquidity_ok = True (현재 거래량 >= 중앙값의 50%)
# (6) 일일 거래 제한: MAX_DAILY_TRADES = 60회 미만
# (7) order_throttle 통과 (진입 차단 조건 미해당)
# (8) shock_guard 미작동 (1분봉 급변 1.5%+ 감지 시 5분 동결)
# (9) 동일 방향 재진입 쿨다운: 15분 (REPEAT_SIGNAL_COOLDOWN_SEC = 900)
# (10) 연속 손절 쿨다운: 2회 연속 손절 시 30분 대기
# (11) 가격 재진입 검증: 이전 진입 가격과 동일 수준 차단
# (12) 신호 스팸 가드: 30분 내 동일 방향 3회 → 30분 쿨다운

ENTRY_GATES = {
    'min_confidence': 35,            # V3 최소 confidence
    'conf_add_threshold': 50,        # ADD에 필요한 최소 confidence
    'max_daily_trades': 60,
    'repeat_signal_cooldown_sec': 900,   # 동일 방향 재신호 쿨다운
    'stop_loss_cooldown_trigger': 2,     # 연속 손절 N회 이상 → 쿨다운
    'stop_loss_cooldown_pause_sec': 1800, # 손절 쿨다운 30분
}


# ── V3 레짐별 진입 조건 ────────────────────────────────────
#
# 소스: strategy_v3/score_v3.py → compute_modifier()
#
# V3는 기존 스코어에 레짐별 modifier를 적용하여 최종 판단.
# modifier 범위: -50 ~ +50
#
# [STATIC_RANGE — 평균회귀 진입]
#
#   LONG 조건:
#     - range_position <= 0.20 (가격이 VAL 근처, 하위 20%)
#     - edge_score_bonus +25 적용
#     - 캔들 확인: 하방 꼬리(rejection wick) > 레인지의 50% OR higher-low 패턴
#
#   SHORT 조건:
#     - range_position >= 0.80 (가격이 VAH 근처, 상위 20%)
#     - edge_score_bonus -25 적용 (숏 방향 강화)
#     - 캔들 확인: 상방 꼬리(rejection wick) > 레인지의 50% OR lower-high 패턴
#
#   차단 조건:
#     - POC 노이즈 존: |price - POC| <= max(0.5*ATR, 0.15*VA폭) → -40 페널티
#     - 엣지 오버슈트: VA 밖으로 1.5 ATR 이상 벗어남 → 진입 차단
#     - 임펄스 체이스: impulse > 1.5 → 페널티, > 2.25 → 차단
#     - [v1.1] MeanReversion kill-switch: ff_unified_engine_v11 ON → MR 진입 전면 차단

STATIC_RANGE_ENTRY = {
    'range_position_long': 0.20,   # 이 이하에서 LONG 진입
    'range_position_short': 0.80,  # 이 이상에서 SHORT 진입
    'poc_zone_penalty': 40,        # POC 근처 페널티
    'edge_score_bonus': 25,        # VA 가장자리 보너스
    'edge_overshoot_atr_mult': 1.5, # 오버슈트 차단 임계값
    'anti_chase_impulse_max': 0.6, # V2 안티체이스 임계값
}


# [DRIFT — 추세추종 진입]
#
#   LONG 조건 (DRIFT_UP):
#     - 스코어가 양수(LONG 우세) + drift_UP 정렬 → +15 보너스
#     - POC 근처 리테스트(풀백 진입) → +5 보너스
#
#   SHORT 조건 (DRIFT_DOWN):
#     - 스코어가 음수(SHORT 우세) + drift_DOWN 정렬 → -15 보너스(숏 강화)
#     - POC 근처 리테스트(풀백 진입) → -5 보너스(숏 강화)
#
#   차단 조건:
#     - 추세 반대 방향: DRIFT_UP인데 SHORT → +50 카운터 페널티 (LONG으로 밀기)
#     - 추세 반대 방향: DRIFT_DOWN인데 LONG → -50 카운터 페널티 (SHORT으로 밀기)
#     - 임펄스 체이스: impulse > 1.5 → 페널티, > 2.25 → 차단

DRIFT_ENTRY = {
    'drift_aligned_bonus': 15,      # 추세 정렬 보너스
    'drift_counter_penalty': 50,    # 추세 반대 페널티
    'poc_support_tolerance_atr': 0.3, # POC 리테스트 허용 범위
}


# [BREAKOUT — 돌파추종 진입]
#
#   LONG 조건:
#     - price > VAH + pad (pad = 0.3 * ATR)
#     - breakout_trend_bonus +25 적용
#     - 리테스트 확인: 가격이 VAH로 되돌아왔다가 지지 확인
#     - 또는 sustained hold: VA 밖에서 2+ 캔들 유지 + 거래량 유지
#
#   SHORT 조건:
#     - price < VAL - pad
#     - breakout_trend_bonus -25 적용 (숏 강화)
#     - 리테스트 또는 sustained hold 확인
#
#   차단 조건:
#     - 역방향 차단: BREAKOUT UP인데 SHORT 신호 → 무조건 차단
#     - 체이스 거리: |price - breakout_level| > 2.0 ATR → 차단
#     - 체이스 페널티: 1.0~2.0 ATR → 비례 페널티 (최대 -40)
#     - 임펄스 체이스: impulse > 1.5 → 차단
#     - 첫 돌파 캔들: 절대 진입 금지 (뉴스 오버라이드 시 1캔들로 완화)

BREAKOUT_ENTRY = {
    'breakout_pad_atr_mult': 0.3,   # VA 밖 패딩 (ATR 배수)
    'breakout_trend_bonus': 25,     # 돌파 방향 보너스
    'chase_ban_atr_mult': 2.0,      # 이 ATR 거리 초과 시 진입 차단
    'chase_score_penalty': 40,      # 체이스 거리 비례 페널티 최대값
    'impulse_chase_threshold': 1.5, # 임펄스 체이스 차단 임계값
    'retest_entry_enabled': True,   # 리테스트 진입 활성화
    'breakout_reverse_block': True, # 역방향 하드블록
}


# ═══════════════════════════════════════════════════════════════
# 4. 청산 조건 (Exit Conditions)
# ═══════════════════════════════════════════════════════════════
#
# 소스: strategy_v3/risk_v3.py, strategy/common/risk.py,
#       autopilot_daemon.py, position_manager.py

# ── 손절 (Stop Loss) ──────────────────────────────────────
#
# V3 기본 공식: SL = ATR_multiplier × ATR_pct
# sl_basis: UPNL_PCT (미실현 손익 기준, PRICE_PCT에서 전환됨)
#
# 레짐별 SL 범위:
#   STATIC_RANGE:  SL = 1.5 × ATR, 클램프 [0.5%, 0.9%]
#   DRIFT:         SL = 1.5 × ATR, 클램프 [0.6%, 1.2%]
#   BREAKOUT:      SL = 2.0 × ATR, 클램프 [0.8%, 2.0%]
#
# v1.1 공식: SL = 2.0 × ATR_15m (15분봉 ATR 기반)
#
# 서버사이드 스탑: Python SL보다 0.1% 넓게 설정 (안전마진)

SL_PARAMS = {
    # STATIC_RANGE
    'static_range_atr_mult': 1.5,
    'static_range_min_sl': 0.005,   # 0.5%
    'static_range_max_sl': 0.009,   # 0.9%

    # DRIFT
    'drift_atr_mult': 1.5,
    'drift_min_sl': 0.006,          # 0.6%
    'drift_max_sl': 0.012,          # 1.2%

    # BREAKOUT
    'breakout_atr_sl_mult': 2.0,
    'breakout_min_sl': 0.008,       # 0.8%
    'breakout_max_sl': 0.020,       # 2.0%

    'sl_basis': 'UPNL_PCT',         # 손절 기준: 미실현 손익 %
    'server_stop_offset_pct': 0.1,   # 서버 스탑 = SL + 0.1%
}


# ── 익절 (Take Profit) ────────────────────────────────────
#
# V3 기본 공식: TP = SL × R-ratio
#   tp_r_ratio = 1.2 (기본 1.2R)
#
# V2 모드별:
#   MODE_A (Static Range):
#     TP = min(POC, entry × (1 + 0.4%))  — POC까지 또는 0.4%
#     EXIT: range_position이 반대쪽 VA 도달 시 전량 청산
#
#   MODE_B (Volatile Range):
#     TP = entry × (1 + 0.3%)  — POC~반대 VA 중간점
#     EXIT: 드리프트 반전 시 청산 (예: LONG인데 drift→DOWN)
#     EXIT: VA를 3+캔들 이탈(REBUILD) → 청산
#
#   MODE_C (Breakout):
#     TP1 = entry × (1 + 0.6%) — 1차 부분 익절
#     Trailing Stop: 0.8% 활성화 후 0.5% 추적
#     EXIT: 새로운 박스권 형성(VA 안으로 복귀 5/8캔들) → 청산
#
# v1.1 공식: TP = SL × 3.0 (1:3 R:R 비율)
#
# PanicGuard 단계적 청산 (WS 기반 실시간):
#   0.35% adverse → SL 재조정(타이트닝)
#   0.60% adverse → 50% 포지션 감축
#   1.00% adverse → 전량 청산

TP_PARAMS = {
    'tp_r_ratio': 1.2,              # V3 기본 R 비율

    # V2 MODE_A (Static Range)
    'mode_a_tp_pct': [0.004, 0.008],  # 0.4% ~ 0.8%
    'mode_a_sl_pct': [0.005, 0.009],  # 0.5% ~ 0.9%

    # V2 MODE_B (Volatile Range)
    'mode_b_tp_pct': [0.003, 0.006],  # 0.3% ~ 0.6%
    'mode_b_sl_pct': [0.007, 0.012],  # 0.7% ~ 1.2%

    # V2 MODE_C (Breakout)
    'mode_c_tp1_pct': [0.006, 0.010],          # 0.6% ~ 1.0% (1차 익절)
    'mode_c_trailing_activate_pct': 0.008,      # 0.8% 이익 시 트레일링 활성화
    'mode_c_trailing_pct': 0.005,               # 0.5% 트레일링 폭

    # PanicGuard (WS 실시간)
    'panic_guard_tighten_pct': 0.35,  # SL 타이트닝 임계값
    'panic_guard_reduce_pct': 0.60,   # 50% 감축 임계값
    'panic_guard_close_pct': 1.00,    # 전량 청산 임계값
}


# ── 동적 SL 계산 ──────────────────────────────────────────
#
# 소스: score_engine.py → compute_dynamic_stop_loss()
#
# 공식: dynamic_sl = base + adjustment
#   base = 2.0%
#   risk_input = regime_score × 0.6 + macro_score × 0.4
#   adjustment = -(risk_input / 100) × 1
#   결과: 1.2% ~ 3.0% 범위로 클램프
#
# 의미: 시장이 위험할수록(음수 regime/macro) → SL 넓게
#       시장이 안정적일수록(양수) → SL 좁게


# ═══════════════════════════════════════════════════════════════
# 5. 포지션 사이징 (Position Sizing)
# ═══════════════════════════════════════════════════════════════
#
# 소스: safety_manager.py, strategy_v3/risk_v3.py,
#       strategy/common/risk.py

# ── V3 기본 사이징 ─────────────────────────────────────────
#
# 최대 stage = 1 (현재 설정, 피라미딩 사실상 비활성)
# stage_slice_mult: 평소 1.0, 조건에 따라 축소
#
# 축소 조건:
#   - BREAKOUT 레짐: × 0.5
#   - health=WARN: × 0.5로 상한
#   - impulse > chase_threshold: × 0.5
#   - 연속 손실 2회: × 0.7
#   - 연속 손실 3회+: × 0.5

# ── v1.1 ATR 기반 사이징 ──────────────────────────────────
#
# 소스: strategy_v3/risk_v3.py → compute_risk_v11()
#
# 공식:
#   sl_distance = 2.0 × ATR_15m
#   sl_pct = sl_distance / current_price
#   position_size = (equity × 0.75%) / sl_pct
#   cap: position_size <= equity × 20%
#   최소: 5 USDT

SIZING_V11 = {
    'risk_pct': 0.0075,      # 트레이드당 리스크: 자본의 0.75%
    'sl_atr_mult': 2.0,      # SL = 2 × ATR_15m
    'cap_max_pct': 0.20,     # 최대 자본 사용: 20%
    'min_size_usdt': 5,      # 최소 주문: 5 USDT
}


# ── V2 모드별 사이징 ──────────────────────────────────────
#
# MODE_A (Static Range): 최대 3단계 피라미딩
#   stage_allocation: [0.40, 0.35, 0.25]  — 1단계 40%, 2단계 35%, 3단계 25%
#   레버리지: 3x ~ 5x
#
# MODE_B (Volatile Range): 최대 2단계
#   size_multiplier: 0.7 (정상 사이즈의 70%)
#   레버리지: 3x ~ 5x
#
# MODE_C (Breakout): 최대 2단계
#   레버리지: 3x ~ 8x

SIZING_V2 = {
    'mode_a': {
        'max_stages': 3,
        'allocation': [0.40, 0.35, 0.25],
        'leverage': [3, 5],
        'add_cooldown_sec': 300,
    },
    'mode_b': {
        'max_stages': 2,
        'size_multiplier': 0.7,
        'leverage': [3, 5],
    },
    'mode_c': {
        'max_stages': 2,
        'leverage': [3, 8],
    },
}


# ═══════════════════════════════════════════════════════════════
# 6. 적응형 방어 시스템 (Adaptive Layers)
# ═══════════════════════════════════════════════════════════════
#
# 소스: strategy_v3/adaptive_v3.py
# 현재 상태: adaptive_dryrun = false (실전 적용 중)
#
# 5개 레이어가 진입/ADD를 단계적으로 필터링:
#
# [L1] 연속손실 필터 (Loss-Streak Adaptive)
#   - 모드별 연속 손실 3회 → penalty × 0.70 (사이즈 30% 감소)
#   - 연속 손실 5회 → 쿨다운 (모드별 7200초 = 2시간)
#   - 글로벌 승률 < 35% → conf 임계값 +10, ADD conf 최소 60
#   - 승률 회복 (>= 40%, 3회 연속 개선) → 페널티 해제
#   - 마비 방지: 24시간 무거래 → 부분 리셋, 36시간 → 전체 리셋
#
# [L2] MeanReversion 보호
#   - MR SHORT 진입 시 6가지 조건 모두 충족 필요:
#     (1) regime = STATIC_RANGE
#     (2) price_vs_va = INSIDE
#     (3) range_position >= 0.85
#     (4) breakout_confirmed = False
#     (5) volume_z <= 0
#     (6) flow_bias <= 0
#   - range_position > 1.0 → MR 진입 전면 차단 (BREAKOUT 우선)
#   - 가속 차단: drift=NONE + flow_bias>0 + impulse>1.5 → SHORT 차단
#
# [L3] ADD 로직 제한
#   - uPnL < 0% → ADD 차단 (손실 중 물타기 금지)
#   - uPnL > 0% but peak_upnl < 0.4% → ADD 보류
#   - uPnL > 0% + peak >= 0.4% 또는 리테스트 확인 → ADD 허용
#
# [L4] health=WARN 리스크 컨트롤
#   - 즉시: 진입 + ADD 차단
#   - 2분 경과: time_stop × 0.5 (타이트닝), 트레일링 민감 모드
#
# [L5] 모드별 성과 추적
#   - 최근 50 거래 중 모드별 승률 < 35% → penalty × 0.75
#   - 승률 >= 40% → 페널티 해제
#
# Combined penalty = L1 × L5 (최소 0.55, floor 보장)

ADAPTIVE_PARAMS = {
    'l1_streak_penalty_3': 0.70,     # 3연패 시 사이즈 × 0.70
    'l1_cooldown_5_sec': 7200,       # 5연패 시 2시간 쿨다운
    'l1_global_wr_low': 0.35,        # 이 승률 이하 → 페널티 활성화
    'l1_global_wr_recovery': 0.40,   # 이 승률 이상 + 3회 연속 → 해제

    'l2_range_pos_short_min': 0.85,  # MR SHORT 최소 range_position
    'l2_impulse_hard_block': 1.5,    # 가속 차단 impulse 임계값

    'l3_peak_upnl_threshold': 0.4,   # ADD 허용 최소 peak uPnL (%)

    'l4_warn_tighten_sec': 120,      # WARN 2분 후 타이트닝
    'l4_time_stop_mult': 0.5,        # 타이트닝 배수

    'l5_trades': 50,                 # 성과 추적 윈도우 크기
    'l5_wr_low': 0.35,              # 모드 페널티 임계값
    'l5_wr_recovery': 0.40,         # 모드 페널티 해제 임계값
    'l5_penalty': 0.75,             # 모드 페널티 배수

    'combined_penalty_floor': 0.55,  # 최소 페널티 (사이즈의 55%는 보장)
    'anti_paralysis_hours_1': 24,    # 부분 리셋 (24시간)
    'anti_paralysis_hours_2': 36,    # 전체 리셋 (36시간)
}


# ═══════════════════════════════════════════════════════════════
# 7. 추가 방어 메커니즘
# ═══════════════════════════════════════════════════════════════

# ── Shock Guard (급변 방어) ────────────────────────────────
#
# 소스: shock_guard.py
# 1분봉에서 급변 감지 시 신규 진입을 일시 동결.
#   threshold_pct: 1.5%   — 1분 내 1.5% 이상 변동
#   atr_mult: 2.5         — OR ATR의 2.5배 이상 변동
#   freeze_sec: 300       — 5분간 진입 동결
#   lookback_candles: 3   — 최근 3개 1분봉 확인

SHOCK_GUARD = {
    'threshold_pct': 1.5,
    'atr_mult': 2.5,
    'freeze_sec': 300,
    'lookback_candles': 3,
}

# ── No-Trade Zone (방향성 불확실 구간) ─────────────────────
#
# 소스: strategy_v3/regime_v3.py → is_no_trade_zone()
# trend_probability 0.30~0.70 구간 = 방향성 불확실 → 진입 차단
# (현재 ff_no_trade_zone = false로 비활성화)
#
# trend_probability 계산:
#   ADX 성분 (가중치 0.4): ADX 40+ → 0.4, 25~40 → 비례
#   Drift 성분 (가중치 0.3): drift >= 0.002 → 0.3
#   Volume_z 성분 (가중치 0.3): vol_z >= 2.0 → 0.3

NO_TRADE_ZONE = {
    'enabled': False,     # 현재 비활성화
    'ntx_low': 0.30,
    'ntx_high': 0.70,
}


# ═══════════════════════════════════════════════════════════════
# 8. 피처 스냅샷 (Feature Snapshot)
# ═══════════════════════════════════════════════════════════════
#
# 소스: strategy/common/features.py → build_feature_snapshot()
# 매 사이클마다 DB에서 실시간 계산하는 시장 피처:
#
# price           — 최신 1분봉 종가
# atr_pct         — ATR / price (변동성 비율)
# bb_width        — (BB상단 - BB하단) / BB중간 (밴드 폭)
# volume_z        — (현재 거래량 - 평균) / 표준편차 (50봉 기준)
# impulse         — |종가 - 시가| / ATR (캔들 강도)
# adx             — ADX_14 (추세 강도)
# poc, vah, val   — Volume Profile의 POC/VAH/VAL
# range_position  — (price - VAL) / (VAH - VAL) (VA 내 위치 0~1)
# drift_score     — POC 이동률 (10개 엔트리 기준)
# drift_direction — UP / DOWN / NONE
# poc_slope       — POC 변화 기울기
# vol_pct         — 실현 변동성 (5분봉 로그수익률의 표준편차)
# spread_ok       — bid-ask 스프레드 < 0.05%
# liquidity_ok    — 현재 거래량 >= 중앙값의 50%
# atr_ratio       — 현재 ATR / 평균 ATR (>1 = 확대 중)
# structure_breakout_pass — M/N 캔들이 VA 밖에서 종가 형성


# ═══════════════════════════════════════════════════════════════
# 9. 실행 흐름 요약 (Decision Pipeline)
# ═══════════════════════════════════════════════════════════════
#
# autopilot_daemon.py → 20초마다:
#
# ┌──────────────────────────────────────────────────────┐
# │ 1. KILL_SWITCH / trade_switch 확인                   │
# │ 2. direction_scorer.compute_scores()                 │
# │    → score_engine 4축 가중 합산                      │
# │    → confidence, dominant_side 결정                  │
# │ 3. regime_reader.get_current_regime()                │
# │    → SHOCK/VETO면 즉시 차단                          │
# │                                                      │
# │ ═══ V3 STRATEGY (실전) ═══                           │
# │ 4. build_feature_snapshot()                          │
# │ 5. regime_v3.classify() → 4-state 레짐 분류         │
# │ 6. score_v3.compute_modifier()                       │
# │    → 레짐별 보너스/페널티/차단 판정                  │
# │    → entry_blocked=True면 즉시 return                │
# │ 7. risk_v3.compute_risk()                            │
# │    → 레짐별 SL/TP/사이즈 조정                       │
# │ 8. adaptive_v3.apply_adaptive_layers()               │
# │    → L1~L5 5단계 적응형 필터                         │
# │    → combined_penalty 적용                           │
# │                                                      │
# │ ═══ V2 STRATEGY (shadow — 로그만) ═══                │
# │ 9. regime_router.route() → MODE_A/B/C 분류          │
# │ 10. 각 모드별 Strategy.decide()                      │
# │     → ENTER/ADD/EXIT/HOLD 판정                       │
# │                                                      │
# │ ═══ 진입 게이트 체인 ═══                             │
# │ 11. order_throttle → shock_guard → no_trade_zone    │
# │ 12. SL쿨다운 → 시그널디바운스 → 재진입검증          │
# │ 13. 스팸가드 → 임펄스밴 → 안전관리자                │
# │                                                      │
# │ 14. execution_queue에 INSERT                         │
# │     → executor.py가 실제 주문 실행                   │
# └──────────────────────────────────────────────────────┘


# ═══════════════════════════════════════════════════════════════
# 10. 주요 설정값 요약 (현재 적용 중)
# ═══════════════════════════════════════════════════════════════

ACTIVE_CONFIG = {
    # V3 활성화 상태
    'strategy_v3_enabled': True,
    'strategy_v2_mode': 'shadow',   # 로그만, 실행 안 함
    'adaptive_dryrun': False,        # 적응형 레이어 실전 적용

    # 주요 피처 플래그
    'ff_strict_breakout': True,      # 3중 확인 브레이크아웃
    'ff_regime_risk_v3': True,       # 레짐별 리스크 파라미터
    'ff_adaptive_layers': True,      # 적응형 방어 시스템
    'ff_shock_guard': True,          # 급변 방어
    'ff_panic_guard_ws': True,       # WS 기반 패닉 가드
    'ff_server_stop_orders': True,   # 서버사이드 스탑 주문
    'ff_unified_engine_v11': True,   # v1.1 통합 엔진
    'ff_event_decision_mode': True,  # Claude 이벤트 결정 모드
    'ff_no_trade_zone': False,       # 무거래 구간 (비활성화)
    'ff_global_add_block': True,     # 전역 ADD 차단 (지혈 모드)
}
