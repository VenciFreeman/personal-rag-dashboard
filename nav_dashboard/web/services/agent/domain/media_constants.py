from __future__ import annotations

import os
import threading
from typing import Any
import re

from .. import agent_types
from ...media.media_taxonomy import (
    MEDIA_ABSTRACT_CONCEPT_CUES,
    MEDIA_BOOKISH_CUES,
    MEDIA_INTENT_KEYWORDS,
    MEDIA_REGION_ALIASES,
    MEDIAWIKI_FILLER_PATTERNS,
    MEDIAWIKI_QUERY_ALIASES,
    TMDB_AUDIOVISUAL_CUES,
    TMDB_MOVIE_CUES,
    TMDB_PERSON_CUES,
    TMDB_TV_CUES,
)
from ...planner.router_config import ROUTER_COLLECTION_NEGATIVE_CUES, ROUTER_MEDIA_DETAIL_CUES, ROUTER_MEDIA_SURFACE_CUES

try:
    from core_service import get_settings
except Exception:
    get_settings = None


def _load_optional_core_settings() -> Any:
    if get_settings is None:
        return None
    try:
        return get_settings()
    except Exception:
        return None


def _first_configured_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


_CORE_SETTINGS = _load_optional_core_settings()


def _build_mediawiki_user_agent() -> str:
    explicit = _first_configured_text(
        os.getenv("NAV_DASHBOARD_MEDIAWIKI_USER_AGENT", ""),
        getattr(_CORE_SETTINGS, "mediawiki_user_agent", ""),
    )
    if explicit:
        return explicit
    app_name = _first_configured_text(
        os.getenv("NAV_DASHBOARD_MEDIAWIKI_APP_NAME", ""),
        "PersonalAIStackAgent/0.1",
    )
    site = _first_configured_text(
        os.getenv("NAV_DASHBOARD_MEDIAWIKI_SITE_URL", ""),
        "https://localhost",
    )
    contact = _first_configured_text(
        os.getenv("NAV_DASHBOARD_MEDIAWIKI_CONTACT", ""),
        getattr(_CORE_SETTINGS, "mediawiki_contact", ""),
    )
    extra: list[str] = []
    if site:
        extra.append(site)
    if contact:
        extra.append(f"contact: {contact}")
    if extra:
        return f"{app_name} ({'; '.join(extra)})"
    return f"{app_name} (nav_dashboard local deployment)"


def _build_mediawiki_api_user_agent() -> str:
    explicit = _first_configured_text(
        os.getenv("NAV_DASHBOARD_MEDIAWIKI_API_USER_AGENT", ""),
        getattr(_CORE_SETTINGS, "mediawiki_api_user_agent", ""),
    )
    if explicit:
        return explicit
    contact = _first_configured_text(
        os.getenv("NAV_DASHBOARD_MEDIAWIKI_CONTACT", ""),
        getattr(_CORE_SETTINGS, "mediawiki_contact", ""),
    )
    if contact:
        return f"PersonalAIStackAgent/0.1 (contact: {contact})"
    return "PersonalAIStackAgent/0.1"


TOOL_EXPAND_MEDIAWIKI_CONCEPT = agent_types.TOOL_EXPAND_MEDIAWIKI_CONCEPT
TOOL_PARSE_MEDIAWIKI = agent_types.TOOL_PARSE_MEDIAWIKI
TOOL_SEARCH_BANGUMI = agent_types.TOOL_SEARCH_BANGUMI
TOOL_SEARCH_BY_CREATOR = agent_types.TOOL_SEARCH_BY_CREATOR
TOOL_SEARCH_MEDIAWIKI = agent_types.TOOL_SEARCH_MEDIAWIKI
TOOL_SEARCH_TMDB = agent_types.TOOL_SEARCH_TMDB

LIBRARY_TRACKER_BASE = os.getenv("NAV_DASHBOARD_LIBRARY_TRACKER_INTERNAL_URL", "http://127.0.0.1:8091").rstrip("/")

MEDIAWIKI_ZH_API = _first_configured_text(
    os.getenv("NAV_DASHBOARD_MEDIAWIKI_ZH_API_URL", ""),
    getattr(_CORE_SETTINGS, "mediawiki_zh_api_url", ""),
    "https://zh.wikipedia.org/w/api.php",
).rstrip("?")
MEDIAWIKI_EN_API = _first_configured_text(
    os.getenv("NAV_DASHBOARD_MEDIAWIKI_EN_API_URL", ""),
    getattr(_CORE_SETTINGS, "mediawiki_en_api_url", ""),
    "https://en.wikipedia.org/w/api.php",
).rstrip("?")
MEDIAWIKI_TIMEOUT = float(
    _first_configured_text(
        os.getenv("NAV_DASHBOARD_MEDIAWIKI_TIMEOUT", ""),
        getattr(_CORE_SETTINGS, "mediawiki_timeout", ""),
        "20",
    )
    or "20"
)
MEDIAWIKI_PARSE_TIMEOUT = max(3.0, min(MEDIAWIKI_TIMEOUT, 12.0))
MEDIAWIKI_SEARCH_TIMEOUT = max(2.0, min(MEDIAWIKI_TIMEOUT, 6.0))
MEDIAWIKI_USER_AGENT = _build_mediawiki_user_agent()
MEDIAWIKI_API_USER_AGENT = _build_mediawiki_api_user_agent()

TMDB_API_BASE_URL = _first_configured_text(
    os.getenv("NAV_DASHBOARD_TMDB_API_BASE_URL", ""),
    getattr(_CORE_SETTINGS, "tmdb_api_base_url", ""),
    "https://api.themoviedb.org/3",
).rstrip("/")
TMDB_API_KEY = _first_configured_text(
    os.getenv("NAV_DASHBOARD_TMDB_API_KEY", ""),
    getattr(_CORE_SETTINGS, "tmdb_api_key", ""),
)
TMDB_READ_ACCESS_TOKEN = _first_configured_text(
    os.getenv("NAV_DASHBOARD_TMDB_READ_ACCESS_TOKEN", ""),
    getattr(_CORE_SETTINGS, "tmdb_read_access_token", ""),
)
TMDB_TIMEOUT = float(
    _first_configured_text(
        os.getenv("NAV_DASHBOARD_TMDB_TIMEOUT", ""),
        getattr(_CORE_SETTINGS, "tmdb_timeout", ""),
        "20",
    )
    or "20"
)
TMDB_LANGUAGE = _first_configured_text(
    os.getenv("NAV_DASHBOARD_TMDB_LANGUAGE", ""),
    getattr(_CORE_SETTINGS, "tmdb_language", ""),
    "zh-CN",
)

BANGUMI_ACCESS_TOKEN = _first_configured_text(
    os.getenv("BANGUMI_ACCESS_TOKEN", ""),
    os.getenv("NAV_DASHBOARD_BANGUMI_ACCESS_TOKEN", ""),
    getattr(_CORE_SETTINGS, "bangumi_access_token", ""),
)
BANGUMI_API_BASE_URL = "https://api.bgm.tv"
BANGUMI_TIMEOUT = float(os.getenv("NAV_DASHBOARD_BANGUMI_TIMEOUT", "20") or "20")
BANGUMI_SUBJECT_TYPE_ANIME = 2
BANGUMI_SUBJECT_TYPE_REAL = 6

MEDIAWIKI_CONCEPT_CACHE: dict[str, Any] = {"lock": threading.RLock(), "entries": {}}
MEDIA_TITLE_MARKER_RE = re.compile(r"《[^》]+》")


__all__ = [name for name in globals() if name.isupper()]