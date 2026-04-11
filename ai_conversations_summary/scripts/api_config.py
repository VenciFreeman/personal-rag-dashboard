"""Compatibility shim that re-exports top-level api_config constants."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path

try:
    from core_service import get_settings
except Exception:
    get_settings = None


def _load_root_api_config():
    current = Path(__file__).resolve()
    for parent in current.parents:
        candidate = parent / "api_config.py"
        if candidate == current:
            continue
        if candidate.is_file():
            spec = importlib.util.spec_from_file_location("_root_api_config", candidate)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                return module
    return None


_ROOT = _load_root_api_config()


def _load_core_settings():
    if get_settings is None:
        return None
    try:
        return get_settings()
    except Exception:
        return None


_CORE_SETTINGS = _load_core_settings()

if _ROOT is None:
    API_BASE_URL = (os.getenv("DEEPSEEK_BASE_URL", getattr(_CORE_SETTINGS, "api_base_url", "https://api.deepseek.com")) or "https://api.deepseek.com").strip()
    API_KEY = (os.getenv("DEEPSEEK_API_KEY", getattr(_CORE_SETTINGS, "api_key", "")) or "").strip()
    MODEL = (os.getenv("DEEPSEEK_MODEL", getattr(_CORE_SETTINGS, "chat_model", "deepseek-chat")) or "deepseek-chat").strip()
    EMBEDDING_MODEL = (os.getenv("EMBEDDING_MODEL", getattr(_CORE_SETTINGS, "embedding_model", "BAAI/bge-base-zh-v1.5")) or "BAAI/bge-base-zh-v1.5").strip()
    TAVILY_API_KEY = (os.getenv("TAVILY_API_KEY", getattr(_CORE_SETTINGS, "tavily_api_key", "")) or "").strip()
    TIMEOUT = int(os.getenv("DEEPSEEK_TIMEOUT", str(getattr(_CORE_SETTINGS, "timeout", 120))) or "120")
else:
    API_BASE_URL = _ROOT.API_BASE_URL
    API_KEY = _ROOT.API_KEY
    MODEL = _ROOT.MODEL
    EMBEDDING_MODEL = _ROOT.EMBEDDING_MODEL
    TAVILY_API_KEY = getattr(_ROOT, "TAVILY_API_KEY", "")
    TIMEOUT = _ROOT.TIMEOUT
