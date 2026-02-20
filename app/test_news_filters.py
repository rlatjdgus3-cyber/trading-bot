"""
test_news_filters.py — 뉴스 필터링 시스템 단위 테스트.

테스트 대상:
  1. _is_hard_excluded() 하드 제외 패턴
  2. worth_llm() AND-기반 키워드 매칭
  3. _get_source_tier() 소스 티어 분류
  4. 소스 티어 캡핑 검증
  5. TIER_MULTIPLIERS 적용 검증
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import unittest


class TestHardExcluded(unittest.TestCase):
    """_is_hard_excluded() 검증."""

    def setUp(self):
        from news_bot import _is_hard_excluded
        self.func = _is_hard_excluded

    def test_stock_recommendation(self):
        assert self.func("Is Amazon Stock a Good Buy?") == True

    def test_personal_story(self):
        assert self.func("How I Lost Everything in Crypto") == True

    def test_top_picks(self):
        assert self.func("Top 5 Stocks to Buy Now") == True

    def test_should_you_buy(self):
        assert self.func("Should You Buy Bitcoin Right Now?") == True

    def test_personal_finance(self):
        assert self.func("Personal Finance Tips for 2026") == True

    def test_retirement(self):
        assert self.func("Retirement Plan Strategies") == True

    def test_tips_article(self):
        assert self.func("10 Things You Need to Know About Investing") == True

    def test_stock_to_buy(self):
        assert self.func("Stocks to Buy Before Earnings Season") == True

    def test_my_husband_portfolio(self):
        assert self.func("My Husband Lost Our Portfolio on Meme Coins") == True

    def test_fed_not_excluded(self):
        assert self.func("Fed Holds Rates Steady at FOMC Meeting") == False

    def test_btc_etf_not_excluded(self):
        assert self.func("Bitcoin ETF Sees Record Inflows") == False

    def test_cpi_not_excluded(self):
        assert self.func("CPI Data Comes in Above Expectations") == False

    def test_war_not_excluded(self):
        assert self.func("Russia Launches Missile Strike on Ukraine") == False

    def test_btc_crash_not_excluded(self):
        assert self.func("Bitcoin Crashes Below 80K After Liquidation Cascade") == False


class TestWorthLlm(unittest.TestCase):
    """worth_llm() AND-기반 키워드 매칭 검증."""

    def setUp(self):
        from news_bot import worth_llm
        self.func = worth_llm

    def test_crypto_plus_impact(self):
        """crypto + impact 동시 충족."""
        assert self.func("Bitcoin crashes after Fed rate hike") == True

    def test_no_crypto_keyword(self):
        """crypto 키워드 없음 — 하지만 기존 KEYWORDS fallback으로 통과 가능."""
        # 'amazon stock' has no crypto OR macro standalone keyword
        # BUT 'stock' is not in KEYWORDS either → False
        result = self.func("Amazon stock drops 5%", active_keywords=[])
        assert result == False

    def test_macro_standalone_fed(self):
        """매크로 스탠드얼론: Fed 단독 통과."""
        assert self.func("Fed announces rate decision") == True

    def test_macro_standalone_cpi(self):
        """매크로 스탠드얼론: CPI 단독 통과."""
        assert self.func("CPI data released") == True

    def test_macro_standalone_fomc(self):
        """매크로 스탠드얼론: FOMC 단독 통과."""
        assert self.func("FOMC meeting minutes released") == True

    def test_macro_standalone_war(self):
        """매크로 스탠드얼론: war 단독 통과."""
        assert self.func("War escalation in Middle East") == True

    def test_macro_standalone_tariff(self):
        """매크로 스탠드얼론: tariff 단독 통과."""
        assert self.func("New tariff announced on Chinese imports") == True

    def test_crypto_feed_bypass(self):
        """크립토 전용 피드 바이패스."""
        assert self.func("Some random article", source="coindesk") == True
        assert self.func("Some random article", source="cointelegraph") == True

    def test_no_match(self):
        """아무 키워드 없음."""
        assert self.func("Local weather forecast sunny", active_keywords=[]) == False

    def test_crypto_without_impact(self):
        """crypto만 있고 impact 없음 — 기존 KEYWORDS에 'bitcoin'이 있으므로 통과."""
        assert self.func("Bitcoin mentioned in lifestyle article") == True

    def test_crypto_without_impact_strict(self):
        """active_keywords=[] means explicitly empty → no keywords to match → False."""
        # active_keywords=[] is not None, so code uses empty list → no match → False
        result = self.func("Bitcoin mentioned in lifestyle article",
                          active_keywords=[])
        assert result == False


class TestGetSourceTier(unittest.TestCase):
    """_get_source_tier() 소스 티어 분류 검증."""

    def setUp(self):
        from news_bot import _get_source_tier
        self.func = _get_source_tier

    def test_tier1_sources(self):
        assert self.func("bloomberg") == "TIER1_SOURCE"
        assert self.func("reuters") == "TIER1_SOURCE"
        assert self.func("wsj") == "TIER1_SOURCE"
        assert self.func("ft") == "TIER1_SOURCE"
        assert self.func("ap") == "TIER1_SOURCE"

    def test_tier2_sources(self):
        assert self.func("cnbc") == "TIER2_SOURCE"
        assert self.func("coindesk") == "TIER2_SOURCE"
        assert self.func("marketwatch") == "TIER2_SOURCE"
        assert self.func("cointelegraph") == "TIER2_SOURCE"

    def test_reference_only(self):
        assert self.func("yahoo_finance") == "REFERENCE_ONLY"

    def test_tier2_promoted(self):
        """bbc/investing promoted to TIER2_SOURCE."""
        assert self.func("investing") == "TIER2_SOURCE"
        assert self.func("bbc_business") == "TIER2_SOURCE"
        assert self.func("bbc_world") == "TIER2_SOURCE"

    def test_unknown_source(self):
        assert self.func("unknown_source") == "REFERENCE_ONLY"
        assert self.func("") == "REFERENCE_ONLY"
        assert self.func(None) == "REFERENCE_ONLY"


class TestSourceTierCapping(unittest.TestCase):
    """소스 티어 캡핑 검증: REFERENCE_ONLY → 최대 TIER3."""

    def test_reference_source_capped_to_tier3(self):
        """REFERENCE_ONLY 소스에서 GPT가 TIER1이라 해도 TIER3으로 캡."""
        from news_bot import _get_source_tier
        source_tier = _get_source_tier("yahoo_finance")
        gpt_tier = "TIER1"
        # 캡핑 로직 (news_bot.py main loop에서 수행)
        if source_tier == 'REFERENCE_ONLY' and gpt_tier in ('TIER1', 'TIER2'):
            gpt_tier = 'TIER3'
        assert gpt_tier == "TIER3"

    def test_tier1_source_not_capped(self):
        """TIER1 소스는 캡 미적용."""
        from news_bot import _get_source_tier
        source_tier = _get_source_tier("bloomberg")
        gpt_tier = "TIER1"
        if source_tier == 'REFERENCE_ONLY' and gpt_tier in ('TIER1', 'TIER2'):
            gpt_tier = 'TIER3'
        assert gpt_tier == "TIER1"


class TestTierMultipliers(unittest.TestCase):
    """TIER_MULTIPLIERS 적용 검증."""

    def setUp(self):
        from news_event_scorer import TIER_MULTIPLIERS
        self.mult = TIER_MULTIPLIERS

    def test_tierx_zero(self):
        """TIERX → 점수 기여 0."""
        assert self.mult['TIERX'] == 0.0

    def test_tier1_full(self):
        """TIER1 → 100% 반영."""
        assert self.mult['TIER1'] == 1.0

    def test_tier2_seventy(self):
        """TIER2 → 70% 반영."""
        assert self.mult['TIER2'] == 0.7

    def test_tier3_ten(self):
        """TIER3 → 10% 반영 (0.3→0.1 변경)."""
        assert self.mult['TIER3'] == 0.1

    def test_unknown_half(self):
        """UNKNOWN → 50% 반영 (기존 데이터 호환)."""
        assert self.mult['UNKNOWN'] == 0.5


class TestClassifyRelevance(unittest.TestCase):
    """_classify_relevance() 개선 검증."""

    def setUp(self):
        from news_event_scorer import _classify_relevance
        self.func = _classify_relevance

    def test_tierx_always_low(self):
        assert self.func("Any title", "FED_RATES", 10, tier='TIERX') == 'LOW'

    def test_high_relevance_score(self):
        assert self.func("Any", "OTHER", 5, relevance_score=0.9) == 'HIGH'

    def test_med_relevance_score(self):
        assert self.func("Any", "OTHER", 5, relevance_score=0.7) == 'MED'

    def test_low_relevance_score(self):
        assert self.func("Any", "OTHER", 5, relevance_score=0.3) == 'LOW'

    def test_fallback_without_db_values(self):
        """DB값 없으면 기존 휴리스틱."""
        result = self.func("Fed rate decision", "FED_RATES", 8)
        assert result == 'HIGH'


class TestIsGossip(unittest.TestCase):
    """기존 _is_gossip() 검증."""

    def setUp(self):
        from news_bot import _is_gossip
        self.func = _is_gossip

    def test_celebrity(self):
        assert self.func("Celebrity endorses Bitcoin") == True

    def test_lifestyle(self):
        assert self.func("Lifestyle changes in crypto era") == True

    def test_fed_not_gossip(self):
        assert self.func("Fed rate decision") == False


class TestReportOnlyRouting(unittest.TestCase):
    """P0-1: REPORT_ONLY 사전 라우팅 검증."""

    def setUp(self):
        from telegram_cmd_poller import _detect_report_only
        self.func = _detect_report_only

    def test_comprehensive_report(self):
        """종합 보고 → comprehensive_report."""
        assert self.func("종합 보고해줘") == "comprehensive_report"
        assert self.func("테스트 종합 보고하라") == "comprehensive_report"
        assert self.func("전체 총정리해줘") == "comprehensive_report"
        assert self.func("브리핑 해줘") == "comprehensive_report"

    def test_news_report(self):
        """뉴스 관련 보고 → news_report."""
        assert self.func("뉴스 리포트 보여줘") == "news_report"
        assert self.func("크립토 뉴스 요약해줘") == "news_report"

    def test_strategy_report(self):
        """전략 관련 보고 → strategy_report."""
        assert self.func("전략 보고해") == "strategy_report"
        assert self.func("매매 현황 보고해줘") == "strategy_report"

    def test_default_report(self):
        """기본 보고 → news_report."""
        assert self.func("요약해줘") == "news_report"
        assert self.func("현황 알려줘") == "news_report"

    def test_directive_not_captured(self):
        """DIRECTIVE 키워드가 포함되면 빈 문자열."""
        assert self.func("롱 청산해") == ""
        assert self.func("리스크 적용해") == ""
        assert self.func("설정 변경해줘") == ""
        assert self.func("포지션 줄여줘") == ""

    def test_no_report_keyword(self):
        """보고 키워드 없음 → 빈 문자열."""
        assert self.func("BTC 가격") == ""
        assert self.func("") == ""

    def test_점검_is_report(self):
        """점검 키워드 → comprehensive_report (확장됨)."""
        assert self.func("시스템 점검해") == "comprehensive_report"


class TestKeywordFallbackRouting(unittest.TestCase):
    """P0-1: gpt_router 키워드 폴백 라우팅 검증."""

    def setUp(self):
        from gpt_router import _keyword_fallback
        self.func = _keyword_fallback

    def test_report_before_audit(self):
        """보고 키워드 → QUESTION/report (run_audit보다 우선)."""
        r = self.func("종합 보고해줘")
        assert r['type'] == 'QUESTION'
        assert r['intent'] == 'report'

    def test_audit_still_works(self):
        """시스템 점검 → COMMAND/run_audit."""
        r = self.func("시스템 점검해줘")
        assert r['type'] == 'COMMAND'
        assert r['intent'] == 'run_audit'

    def test_reduce_not_report(self):
        """포지션 정리 → reduce_position (리포트 아님)."""
        r = self.func("포지션 정리해")
        assert r['type'] == 'COMMAND'
        assert r['intent'] == 'reduce_position'

    def test_report_with_정리(self):
        """리포트 정리 → report (reduce 아님)."""
        r = self.func("리포트 정리해줘")
        assert r['type'] == 'QUESTION'
        assert r['intent'] == 'report'


class TestStrategyCategories(unittest.TestCase):
    """STRATEGY_CATEGORIES 상수 검증."""

    def setUp(self):
        from news_event_scorer import (
            STRATEGY_TIER1, STRATEGY_TIER2, STRATEGY_CATEGORIES,
            STRATEGY_CATEGORY_MULT)
        self.tier1 = STRATEGY_TIER1
        self.tier2 = STRATEGY_TIER2
        self.categories = STRATEGY_CATEGORIES
        self.mult = STRATEGY_CATEGORY_MULT

    def test_tier1_categories(self):
        """TIER1 전략 카테고리: FED_RATES, CPI_JOBS, REGULATION_SEC_ETF, WAR."""
        assert 'FED_RATES' in self.tier1
        assert 'CPI_JOBS' in self.tier1
        assert 'REGULATION_SEC_ETF' in self.tier1
        assert 'WAR' in self.tier1

    def test_tier2_categories(self):
        """TIER2 전략 카테고리 확인."""
        assert 'NASDAQ_EQUITIES' in self.tier2
        assert 'CRYPTO_SPECIFIC' in self.tier2

    def test_strategy_is_union(self):
        """STRATEGY_CATEGORIES = TIER1 | TIER2."""
        assert self.categories == self.tier1 | self.tier2

    def test_tier1_mult_full(self):
        """TIER1 카테고리 → 가중치 1.0."""
        for cat in self.tier1:
            assert self.mult[cat] == 1.0

    def test_tier2_mult_seventy(self):
        """TIER2 카테고리 → 가중치 0.7."""
        for cat in self.tier2:
            assert self.mult[cat] == 0.7

    def test_other_not_in_strategy(self):
        """OTHER는 전략 카테고리에 없음."""
        assert 'OTHER' not in self.categories


class TestExpandedReportRouting(unittest.TestCase):
    """확장된 REPORT_ONLY 키워드 라우팅 검증."""

    def setUp(self):
        from telegram_cmd_poller import _detect_report_only
        self.func = _detect_report_only

    def test_audit_comprehensive(self):
        """감사/audit → comprehensive_report."""
        assert self.func("감사 리포트") == "comprehensive_report"
        assert self.func("audit 보고") == "comprehensive_report"

    def test_점검_comprehensive(self):
        """점검 → comprehensive_report."""
        assert self.func("시스템 점검 보고해줘") == "comprehensive_report"

    def test_분석_report(self):
        """분석 → news_report (뉴스 키워드 없으면 기본)."""
        result = self.func("분석해줘")
        assert result in ("news_report", "comprehensive_report")

    def test_왜그래_report(self):
        """왜 그래 → report 경로."""
        result = self.func("왜 그래 설명해줘")
        assert result != ""


if __name__ == '__main__':
    unittest.main()
