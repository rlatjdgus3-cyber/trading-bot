"""
bench_strategies — 4 benchmark signal strategies.

Each module exposes compute_signal(indicators, vol_profile, price, candles, historical_indicators=None)
returning {'signal': 'LONG'|'SHORT'|'FLAT', 'confidence': int, 'rationale': str, 'indicators': dict}.
"""
from bench_strategies.trend_follow import compute_signal as trend_follow_signal
from bench_strategies.mean_reversion import compute_signal as mean_reversion_signal
from bench_strategies.volume_vp import compute_signal as volume_vp_signal
from bench_strategies.volatility_regime import compute_signal as volatility_regime_signal

STRATEGY_REGISTRY = {
    'trend_follow': trend_follow_signal,
    'mean_reversion': mean_reversion_signal,
    'volume_vp': volume_vp_signal,
    'volatility_regime': volatility_regime_signal,
}

STRATEGY_LABELS = {
    'trend_follow': 'Trend-Follow (EMA/VWAP)',
    'mean_reversion': 'Mean-Reversion (BB/RSI)',
    'volume_vp': 'Volume/VP (POC/VAH/VAL)',
    'volatility_regime': 'Volatility/Regime (ATR/BBW)',
}
