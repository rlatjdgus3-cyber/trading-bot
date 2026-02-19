"""
trading_config.py — Single source of truth for trading constants.

All modules should import from here instead of duplicating values.
"""

# ── Core symbol ──
SYMBOL = 'BTC/USDT:USDT'
ALLOWED_SYMBOLS = frozenset({'BTC/USDT:USDT'})
