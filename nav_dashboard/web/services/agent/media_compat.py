from __future__ import annotations

from typing import Any

from .domain.media_helpers import _extract_media_entities as _extract_media_entities_owner
from .domain import media_tools as media_tools_owner


def _extract_media_entities(query: str) -> list[str]:
    return _extract_media_entities_owner(query)


def _tool_query_media_record(
    query: str,
    query_profile: dict[str, Any],
    trace_id: str = "",
    options: dict[str, Any] | None = None,
    *,
    deps: Any | None = None,
):
    return media_tools_owner._tool_query_media_record(query, query_profile, trace_id, options, deps=deps)