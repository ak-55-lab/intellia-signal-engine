"""
Runtime configuration — wraps environment variables with the ability to
override at runtime via the Settings UI without a server restart.

Priority: runtime override > environment variable > default
"""

import os
from typing import Dict

_runtime: Dict[str, str] = {}


def get(key: str, default: str = "") -> str:
    return _runtime.get(key) or os.getenv(key, default)


def set_key(key: str, value: str) -> None:
    if value:
        _runtime[key] = value
    elif key in _runtime:
        del _runtime[key]


def get_all_public() -> Dict[str, str]:
    """Return masked versions of all configured keys for the settings UI."""
    keys = ["ANTHROPIC_API_KEY", "SUPABASE_URL", "SUPABASE_KEY"]
    result = {}
    for k in keys:
        val = get(k)
        if not val:
            result[k] = ""
        elif k.endswith("_KEY") or k.endswith("_SECRET"):
            # Show first 8 chars + mask
            result[k] = val[:8] + "••••••••" if len(val) > 8 else "••••••••"
        else:
            result[k] = val
    return result


def is_configured() -> Dict[str, bool]:
    return {
        "anthropic": bool(get("ANTHROPIC_API_KEY")),
        "supabase":  bool(get("SUPABASE_URL") and get("SUPABASE_KEY")),
    }
