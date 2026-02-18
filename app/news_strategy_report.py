"""
news_strategy_report.py — Data assembly layer for news→strategy report.

Pure data assembly: DB queries + score_engine + macro_trace.
No formatting, no AI calls. Returns structured dict for report_formatter.
"""
import os
import sys
import json
import traceback
import re

sys.path.insert(0, '/root/trading-bot/app')
import report_formatter

LOG_PREFIX = '[news_strategy_report]'
SYMBOL = 'BTC/USDT:USDT'


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _safe_float(val, default=0.0):
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _fetch_snapshot(cur):
    """Fetch market snapshot: price, BB, RSI, ATR, Ichimoku, vol profile, returns."""
    snap = {}
    try:
        cur.execute("""
            SELECT rsi_14, atr_14, bb_up, bb_mid, bb_dn,
                   ich_tenkan, ich_kijun
            FROM indicators
            WHERE symbol = %s AND tf = '1m'
            ORDER BY ts DESC LIMIT 1;
        """, (SYMBOL,))
        row = cur.fetchone()
        if row:
            snap['rsi'] = round(_safe_float(row[0]), 1)
            snap['atr'] = round(_safe_float(row[1]), 1)
            snap['bb_up'] = _safe_float(row[2])
            snap['bb_mid'] = _safe_float(row[3])
            snap['bb_dn'] = _safe_float(row[4])
            snap['ich_tenkan'] = _safe_float(row[5])
            snap['ich_kijun'] = _safe_float(row[6])
    except Exception as e:
        _log(f'snapshot indicators error: {e}')

    # Vol profile
    try:
        cur.execute("""
            SELECT poc, vah, val FROM vol_profile
            WHERE symbol = %s
            ORDER BY ts DESC LIMIT 1;
        """, (SYMBOL,))
        row = cur.fetchone()
        if row:
            snap['poc'] = _safe_float(row[0])
            snap['vah'] = _safe_float(row[1])
            snap['val'] = _safe_float(row[2])
    except Exception:
        pass

    # Current price + 24h high/low from candles
    try:
        cur.execute("""
            SELECT c FROM candles
            WHERE symbol = %s AND tf = '1m'
            ORDER BY ts DESC LIMIT 1;
        """, (SYMBOL,))
        row = cur.fetchone()
        if row:
            snap['price'] = _safe_float(row[0])
    except Exception:
        pass

    if 'price' not in snap:
        try:
            cur.execute("""
                SELECT c FROM market_ohlcv
                WHERE symbol = %s AND tf = '5m'
                ORDER BY ts DESC LIMIT 1;
            """, (SYMBOL,))
            row = cur.fetchone()
            if row:
                snap['price'] = _safe_float(row[0])
        except Exception:
            pass

    # 24h high/low
    try:
        cur.execute("""
            SELECT MAX(h), MIN(l) FROM candles
            WHERE symbol = %s AND tf = '1m'
              AND ts >= now() - interval '24 hours';
        """, (SYMBOL,))
        row = cur.fetchone()
        if row:
            snap['high_24h'] = _safe_float(row[0])
            snap['low_24h'] = _safe_float(row[1])
    except Exception:
        pass

    # 1h / 4h returns
    price = snap.get('price', 0)
    if price > 0:
        for label, mins in [('ret_1h', 60), ('ret_4h', 240)]:
            try:
                cur.execute("""
                    SELECT c FROM candles
                    WHERE symbol = %s AND tf = '1m'
                      AND ts <= now() - %s * interval '1 minute'
                    ORDER BY ts DESC LIMIT 1;
                """, (SYMBOL, mins))
                row = cur.fetchone()
                if row and row[0]:
                    old = _safe_float(row[0])
                    if old > 0:
                        snap[label] = round((price - old) / old * 100, 2)
            except Exception:
                pass

    return snap


def _fetch_position(cur):
    """Fetch current position state."""
    try:
        cur.execute("""
            SELECT side, total_qty, avg_entry_price, stage, trade_budget_used_pct
            FROM position_state WHERE symbol = %s;
        """, (SYMBOL,))
        row = cur.fetchone()
        if row and row[0]:
            side = row[0]
            qty = _safe_float(row[1])
            entry = _safe_float(row[2])
            return {
                'side': side,
                'qty': qty,
                'entry': entry,
                'stage': int(row[3]) if row[3] else 0,
                'budget_pct': _safe_float(row[4]),
            }
    except Exception as e:
        _log(f'position fetch error: {e}')
    return {}


def _fetch_categorized_news(cur, max_news=5):
    """Fetch recent news categorized into macro/crypto/ignored with IDs for trace lookup."""
    macro_news = []
    crypto_news = []
    ignored_news = []
    stats = {'total': 0, 'enriched': 0, 'high_impact': 0,
             'bullish': 0, 'bearish': 0, 'neutral': 0,
             'categories': {}}

    try:
        # Stats
        cur.execute("""
            SELECT count(*) AS total,
                   count(*) FILTER (WHERE impact_score > 0) AS enriched,
                   count(*) FILTER (WHERE impact_score >= 7) AS high_impact,
                   count(*) FILTER (WHERE summary ILIKE '[up]%%') AS bullish,
                   count(*) FILTER (WHERE summary ILIKE '[down]%%') AS bearish,
                   count(*) FILTER (WHERE summary ILIKE '[neutral]%%') AS neutral_cnt
            FROM news
            WHERE ts >= now() - interval '6 hours';
        """)
        sr = cur.fetchone()
        if sr:
            stats['total'] = sr[0] or 0
            stats['enriched'] = sr[1] or 0
            stats['high_impact'] = sr[2] or 0
            stats['bullish'] = sr[3] or 0
            stats['bearish'] = sr[4] or 0
            stats['neutral'] = sr[5] or 0

        # News items with id (include tier/relevance columns)
        cur.execute("""
            SELECT id, title, source, impact_score, summary,
                   to_char(ts AT TIME ZONE 'Asia/Seoul', 'MM-DD HH24:MI') as ts_kr,
                   keywords, title_ko,
                   COALESCE(tier, 'UNKNOWN') AS tier,
                   relevance_score, source_tier, exclusion_reason
            FROM news
            WHERE ts >= now() - interval '6 hours'
              AND impact_score > 0
            ORDER BY impact_score DESC, ts DESC
            LIMIT 50;
        """)
        rows = cur.fetchall()

        for r in rows:
            news_id = r[0]
            summary_raw = r[4] or ''
            tier = r[8] or 'UNKNOWN'
            rel_score = float(r[9]) if r[9] is not None else None
            source_tier = r[10] or ''
            exclusion_reason = r[11] or None
            cat = report_formatter._parse_news_category(summary_raw)
            direction = report_formatter._parse_news_direction(summary_raw)
            impact_path = report_formatter._parse_impact_path(summary_raw)

            # Extract Korean summary (strip tags)
            summary_kr = re.sub(r'^\[.*?\]\s*', '', summary_raw)
            summary_kr = re.sub(r'^\[.*?\]\s*', '', summary_kr)
            if '|' in summary_kr:
                summary_kr = summary_kr.split('|', 1)[0].strip()

            item = {
                'id': news_id,
                'title': r[1] or '',
                'title_ko': r[7] or '',
                'source': r[2] or '',
                'impact_score': int(r[3]) if r[3] else 0,
                'summary': summary_raw,
                'summary_kr': summary_kr,
                'direction': direction,
                'category': cat,
                'category_kr': report_formatter.CATEGORY_KR.get(cat, cat),
                'impact_path': impact_path,
                'ts': r[5] or '',
                'keywords': list(r[6]) if r[6] else [],
                'trace': {},
                'tier': tier,
                'relevance_score': rel_score,
                'source_tier': source_tier,
            }
            stats['categories'][cat] = stats['categories'].get(cat, 0) + 1

            # 3-way 분류: 제외 vs 전략반영(macro/crypto)
            # Tier 기반 강화 필터:
            #   TIERX → 무조건 제외
            #   TIER3 + rel_score < 0.6 → 제외 (일반 시황)
            #   TIER1/TIER2 + STRATEGY_CATEGORIES → 전략 반영
            from news_event_scorer import STRATEGY_CATEGORIES
            if (exclusion_reason
                    or tier == 'TIERX'
                    or (rel_score is not None and rel_score < 0.6)
                    or (tier == 'TIER3' and cat not in STRATEGY_CATEGORIES)):
                ignore_reason = exclusion_reason or f'tier={tier}'
                if rel_score is not None and rel_score < 0.6 and not exclusion_reason:
                    ignore_reason = f'low_relevance: {rel_score}'
                elif tier == 'TIER3' and cat not in STRATEGY_CATEGORIES:
                    ignore_reason = 'tier=TIER3'
                item['ignore_reason'] = ignore_reason
                ignored_news.append(item)
            else:
                if cat in report_formatter.CRYPTO_CATEGORIES:
                    crypto_news.append(item)
                elif cat in report_formatter.MACRO_CATEGORIES:
                    macro_news.append(item)
                else:
                    if item['source'] in ('coindesk', 'cointelegraph'):
                        crypto_news.append(item)
                    else:
                        macro_news.append(item)

    except Exception as e:
        _log(f'_fetch_categorized_news error: {e}')

    return macro_news[:max_news], crypto_news[:max_news], ignored_news, stats


def _fetch_chart_flow(cur):
    """4h/12h 추세 방향, BB 포지션, Ichimoku 클라우드 위치 조회."""
    result = {
        'trend_4h': 'neutral',
        'trend_12h': 'neutral',
        'bb_position': 'middle',  # upper/middle/lower
        'ichimoku_cloud': 'neutral',  # above/below/inside
    }
    try:
        # 4h trend: compare last close vs 4h-ago close
        cur.execute("""
            SELECT c FROM candles
            WHERE symbol = 'BTC/USDT:USDT' AND tf = '1h'
            ORDER BY ts DESC LIMIT 1;
        """)
        last_row = cur.fetchone()
        cur.execute("""
            SELECT c FROM candles
            WHERE symbol = 'BTC/USDT:USDT' AND tf = '1h'
              AND ts <= now() - interval '4 hours'
            ORDER BY ts DESC LIMIT 1;
        """)
        h4_row = cur.fetchone()
        if last_row and h4_row and last_row[0] and h4_row[0]:
            price_now = float(last_row[0])
            price_4h = float(h4_row[0])
            diff_4h = (price_now - price_4h) / price_4h * 100
            result['trend_4h'] = '상승' if diff_4h > 0.3 else ('하락' if diff_4h < -0.3 else '횡보')
            result['trend_4h_pct'] = round(diff_4h, 2)

        # 12h trend
        cur.execute("""
            SELECT c FROM candles
            WHERE symbol = 'BTC/USDT:USDT' AND tf = '1h'
              AND ts <= now() - interval '12 hours'
            ORDER BY ts DESC LIMIT 1;
        """)
        h12_row = cur.fetchone()
        if last_row and h12_row and last_row[0] and h12_row[0]:
            price_now = float(last_row[0])
            price_12h = float(h12_row[0])
            diff_12h = (price_now - price_12h) / price_12h * 100
            result['trend_12h'] = '상승' if diff_12h > 0.5 else ('하락' if diff_12h < -0.5 else '횡보')
            result['trend_12h_pct'] = round(diff_12h, 2)

        # BB position
        cur.execute("""
            SELECT bb_up, bb_mid, bb_dn FROM indicators
            WHERE symbol = 'BTC/USDT:USDT'
            ORDER BY ts DESC LIMIT 1;
        """)
        bb_row = cur.fetchone()
        if bb_row and last_row and all(bb_row) and last_row[0]:
            price = float(last_row[0])
            bb_up = float(bb_row[0])
            bb_mid = float(bb_row[1])
            bb_dn = float(bb_row[2])
            if price > bb_up:
                result['bb_position'] = '상단 돌파'
            elif price > bb_mid:
                result['bb_position'] = '상단 영역'
            elif price > bb_dn:
                result['bb_position'] = '하단 영역'
            else:
                result['bb_position'] = '하단 이탈'

        # Ichimoku cloud position
        cur.execute("""
            SELECT ich_span_a, ich_span_b FROM indicators
            WHERE symbol = 'BTC/USDT:USDT'
            ORDER BY ts DESC LIMIT 1;
        """)
        ich_row = cur.fetchone()
        if ich_row and last_row and last_row[0]:
            price = float(last_row[0])
            senkou_a = float(ich_row[0]) if ich_row[0] else None
            senkou_b = float(ich_row[1]) if ich_row[1] else None
            if senkou_a and senkou_b:
                cloud_top = max(senkou_a, senkou_b)
                cloud_bot = min(senkou_a, senkou_b)
                if price > cloud_top:
                    result['ichimoku_cloud'] = '클라우드 위'
                elif price < cloud_bot:
                    result['ichimoku_cloud'] = '클라우드 아래'
                else:
                    result['ichimoku_cloud'] = '클라우드 내부'
    except Exception as e:
        _log(f'_fetch_chart_flow error: {e}')

    return result


def _fetch_conditional_scenarios(scores, snapshot):
    """2-3개 조건부 시나리오 생성."""
    scenarios = []
    price = snapshot.get('price', 0)
    bb_up = snapshot.get('bb_up', 0)
    bb_dn = snapshot.get('bb_dn', 0)
    val = snapshot.get('val', 0)
    vah = snapshot.get('vah', 0)

    if bb_up and price:
        dist_to_bb_up = (bb_up - price) / price * 100
        if 0 < dist_to_bb_up < 2:
            scenarios.append(
                f'BB 상단(${bb_up:,.0f}) 돌파 시 → 추세 강화, 추가 매수 검토'
            )

    if val and price:
        dist_to_val = (price - val) / price * 100
        if 0 < dist_to_val < 2:
            scenarios.append(
                f'VAL(${val:,.0f}) 이탈 시 → 매도 압력 증가, 축소/손절 검토'
            )

    if bb_dn and price:
        dist_to_bb_dn = (price - bb_dn) / price * 100
        if 0 < dist_to_bb_dn < 2:
            scenarios.append(
                f'BB 하단(${bb_dn:,.0f}) 터치 시 → 반등 가능, 분할 매수 검토'
            )

    if vah and price:
        dist_to_vah = (vah - price) / price * 100
        if 0 < dist_to_vah < 2:
            scenarios.append(
                f'VAH(${vah:,.0f}) 돌파 시 → 거래량 확인 후 추세 추종'
            )

    # Fallback scenario based on regime
    total_score = _safe_float(scores.get('total', 0))
    if not scenarios:
        if total_score > 30:
            scenarios.append('강세 지속 시 → 단계적 추가 진입 검토')
        elif total_score < -30:
            scenarios.append('약세 지속 시 → 포지션 축소 또는 손절 검토')
        else:
            scenarios.append('횡보 지속 시 → 관망, 방향성 확인 후 진입')

    return scenarios[:3]


def build_report_data(cur, exchange=None, max_news=5, detail=False):
    """Assemble all data for the news→strategy report.

    Returns structured dict consumed by report_formatter.format_news_strategy_report().
    """
    try:
        # 1. Compute pending macro_traces
        try:
            import macro_trace_computer
            macro_trace_computer.compute_pending_traces(cur)
        except Exception as e:
            _log(f'compute_pending_traces error: {e}')

        # 2. Score engine
        scores = {}
        try:
            import score_engine
            scores = score_engine.compute_total(cur, exchange)
        except Exception as e:
            _log(f'score_engine error: {e}')

        # 3. Position
        position = _fetch_position(cur)

        # 3b. Exchange reality check (FACT source)
        exchange_block = {}
        try:
            import exchange_reader
            exchange_block = exchange_reader.build_report_exchange_block()
        except Exception as e:
            _log(f'exchange_block error: {e}')
            exchange_block = {'exch_position': 'UNKNOWN', 'reconcile': 'UNKNOWN'}

        # 4. Market snapshot
        snapshot = _fetch_snapshot(cur)
        # Fill price from score engine if missing
        if not snapshot.get('price') and scores.get('price'):
            snapshot['price'] = scores['price']

        # 5. Categorized news (3-way: macro, crypto, ignored)
        max_n = max_news if not detail else max(max_news, 7)
        macro_news, crypto_news, ignored_news, stats = _fetch_categorized_news(cur, max_n)

        # 6. Attach macro_trace to each news item
        all_news = macro_news + crypto_news
        news_ids = [n['id'] for n in all_news if n.get('id')]
        traces = {}
        try:
            import macro_trace_computer
            traces = macro_trace_computer.get_traces_for_report(cur, news_ids)
        except Exception:
            pass

        for n in all_news:
            nid = n.get('id')
            if nid and nid in traces:
                n['trace'] = traces[nid]

        # 6b. Macro data snapshot (QQQ, SPY, DXY, US10Y, VIX)
        macro_snapshot = {}
        try:
            cur.execute("""
                SELECT DISTINCT ON (source) source, price,
                       to_char(ts AT TIME ZONE 'Asia/Seoul', 'MM-DD HH24:MI') as ts_kr
                FROM macro_data ORDER BY source, ts DESC;
            """)
            macro_snapshot = {r[0]: {'price': float(r[1]), 'ts_kr': r[2]}
                             for r in cur.fetchall() if r[1]}
        except Exception as e:
            _log(f'macro_snapshot error: {e}')

        # 7. Watch keywords
        watch_matches = set()
        try:
            watch_kw = []
            cur.execute("SELECT value FROM openclaw_policies WHERE key = 'watch_keywords';")
            row = cur.fetchone()
            if row and row[0]:
                val = row[0] if isinstance(row[0], list) else json.loads(row[0])
                if isinstance(val, list):
                    watch_kw = [str(k).lower() for k in val]
            if not watch_kw:
                from news_event_scorer import WATCH_KEYWORDS_DEFAULT
                watch_kw = WATCH_KEYWORDS_DEFAULT

            for n in all_news:
                text_lower = (n.get('title', '') + ' ' + n.get('summary', '')).lower()
                for kw in watch_kw:
                    if kw in text_lower:
                        watch_matches.add(kw)
        except Exception:
            pass

        # 8. News score trace + components
        news_evt = scores.get('axis_details', {}).get('news_event', {})
        news_components = news_evt.get('components', {})
        news_details = news_evt.get('details', {})
        score_trace = news_details.get('score_trace', '')

        # 9. Position PnL + SL info
        pos_data = {}
        if position.get('side'):
            price = snapshot.get('price', 0)
            entry = position.get('entry', 0)
            side = position['side']
            if price > 0 and entry > 0:
                if side == 'long':
                    pnl_pct = (price - entry) / entry * 100
                else:
                    pnl_pct = (entry - price) / entry * 100
                pos_data['unrealized_pnl_pct'] = round(pnl_pct, 2)

            sl_pct = _safe_float(scores.get('dynamic_stop_loss_pct', 2.0))
            pos_data['sl_pct'] = sl_pct
            if price > 0 and entry > 0:
                if side == 'long':
                    sl_dist = (price - entry) / entry * 100
                else:
                    sl_dist = (entry - price) / entry * 100
                pos_data['sl_dist'] = round(sl_dist, 2)
                pos_data['sl_price'] = round(
                    entry * (1 - sl_pct / 100) if side == 'long'
                    else entry * (1 + sl_pct / 100), 1)

        # Chart flow + conditional scenarios
        chart_flow = _fetch_chart_flow(cur)
        conditional_scenarios = _fetch_conditional_scenarios(scores, snapshot)

        # Macro data staleness check (Phase F)
        macro_stale_info = {}
        try:
            cur.execute("SELECT MAX(ts) FROM macro_data WHERE source = 'QQQ';")
            row = cur.fetchone()
            if row and row[0]:
                from datetime import datetime, timezone
                age_h = (datetime.now(timezone.utc) - row[0]).total_seconds() / 3600
                if age_h > 1:
                    macro_stale_info['macro_stale'] = True
                    macro_stale_info['macro_age_hours'] = round(age_h, 1)
        except Exception as e:
            _log(f'macro staleness check error: {e}')

        data = {
            'snapshot': snapshot,
            'scores': {
                'total': _safe_float(scores.get('total_score')),
                'tech': _safe_float(scores.get('tech_score')),
                'pos': _safe_float(scores.get('position_score')),
                'regime': _safe_float(scores.get('regime_score')),
                'news_event': _safe_float(scores.get('news_event_score')),
                'stage': scores.get('stage', 1),
                'dominant_side': scores.get('dominant_side', 'LONG'),
                'dynamic_sl': _safe_float(scores.get('dynamic_stop_loss_pct', 2.0)),
                'news_guarded': scores.get('news_event_guarded', False),
                'news_components': news_components,
                'weights': scores.get('weights', {}),
            },
            'position': {
                'side': position.get('side', ''),
                'qty': position.get('qty', 0),
                'entry': position.get('entry', 0),
                **pos_data,
            },
            'exchange_block': exchange_block,
            'macro_news': macro_news,
            'crypto_news': crypto_news,
            'ignored_news': ignored_news,
            'macro_snapshot': macro_snapshot,
            'stats': stats,
            'watch_matches': sorted(watch_matches),
            'news_score_trace': score_trace,
            'action_constraints': news_evt.get('action_constraints', {}),
            'chart_flow': chart_flow,
            'conditional_scenarios': conditional_scenarios,
            **macro_stale_info,
        }
        return data

    except Exception as e:
        _log(f'build_report_data error: {e}')
        traceback.print_exc()
        return {
            'snapshot': {},
            'scores': {'total': 0, 'tech': 0, 'pos': 0, 'regime': 0,
                       'news_event': 0, 'stage': 1, 'dominant_side': 'LONG',
                       'dynamic_sl': 2.0, 'news_guarded': False,
                       'news_components': {}, 'weights': {}},
            'position': {},
            'exchange_block': {'exch_position': 'UNKNOWN', 'reconcile': 'UNKNOWN'},
            'macro_news': [],
            'crypto_news': [],
            'ignored_news': [],
            'stats': {'total': 0, 'enriched': 0, 'high_impact': 0,
                      'bullish': 0, 'bearish': 0, 'neutral': 0,
                      'categories': {}},
            'watch_matches': [],
            'news_score_trace': '',
            'action_constraints': {},
            'macro_snapshot': {},
            'chart_flow': {},
            'conditional_scenarios': [],
            'error': str(e),
        }
