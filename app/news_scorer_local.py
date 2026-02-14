"""Local keyword scorer for news_bot stage-1 gating.

Keyword-weight based scoring. Every news item gets AI enrichment
(should_call_ai always returns True), but the score reflects
keyword relevance for prioritization.
"""
import time
import hashlib

# High-impact keywords (weight 2.0)
HIGH = {
    'fed', 'fomc', 'cpi', 'nfp', 'powell', 'rate cut', 'rate hike',
    'sec', 'etf', 'bitcoin', 'btc', 'hack', 'liquidation', 'crash',
    'trump', 'tariff', 'war', 'sanction', 'nasdaq',
}
# Mid-impact keywords (weight 1.0)
MID = {
    'crypto', 'inflation', 'bond', 'treasury', 'dollar', 'china',
    'boj', 'regulation', 'election', 'bank', 'congress', 'yields',
    'pce', 'unemployment', 'jobs report', 'dxy', 'us10y',
    'risk-off', 'risk off', 'equit', 'qqq', 'spx', 'sp500',
}

_recent = {}  # dedup cache: hash -> timestamp
_DEDUP_WINDOW = 600  # 10 minutes


def should_call_ai(title: str) -> tuple:
    """Returns (should_call: bool, score: float).

    Always returns True for should_call — every news gets AI enrichment.
    Score reflects keyword relevance (0.0-1.0).
    """
    t = (title or "").lower()

    # Dedup: skip if identical title seen within window
    h = hashlib.md5(t.encode()).hexdigest()[:12]
    now = time.time()
    # Clean old entries
    cutoff = now - _DEDUP_WINDOW
    stale = [k for k, v in _recent.items() if v < cutoff]
    for k in stale:
        del _recent[k]

    if h in _recent:
        return (False, 0.0)  # exact duplicate within window
    _recent[h] = now

    # Score calculation
    score = 0.0
    for kw in HIGH:
        if kw in t:
            score += 2.0
    for kw in MID:
        if kw in t:
            score += 1.0
    score = min(score / 4.0, 1.0)  # normalize to 0-1
    if score < 0.1:
        score = 0.3  # no keyword match still gets base score
    return (True, score)
