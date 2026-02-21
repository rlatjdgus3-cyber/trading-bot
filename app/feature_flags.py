"""
feature_flags.py — YAML-based feature flag loader.

Loads feature_flags section from config/strategy_modes.yaml.
Thread-safe, cached, FAIL-CLOSED (unknown/error → False).
"""

import os
import threading
import yaml

LOG_PREFIX = '[feature_flags]'

_cache = None
_lock = threading.Lock()
_CONFIG_PATH = os.path.join(os.path.dirname(__file__),
                            'config', 'strategy_modes.yaml')


def _log(msg):
    print(f'{LOG_PREFIX} {msg}', flush=True)


def _load_section():
    """Load feature_flags section from YAML. Returns dict (empty on error)."""
    global _cache
    if _cache is not None:
        return _cache
    with _lock:
        if _cache is not None:
            return _cache
        try:
            with open(_CONFIG_PATH, 'r') as f:
                full = yaml.safe_load(f) or {}
            _cache = full.get('feature_flags', {})
            return _cache
        except Exception as e:
            _log(f'load FAIL-CLOSED: {e}')
            _cache = {}
            return _cache


def is_enabled(flag_name):
    """Check if a feature flag is enabled. FAIL-CLOSED: returns False on error/missing."""
    try:
        section = _load_section()
        val = section.get(flag_name, False)
        return str(val).lower() in ('true', '1', 'on')
    except Exception:
        return False


def get_all():
    """Return all feature flags as dict."""
    return dict(_load_section())


def reload():
    """Force-reload flags (for hot reload / testing)."""
    global _cache
    with _lock:
        _cache = None
