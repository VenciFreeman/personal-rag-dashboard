"""media_tool_definitions.py

LLM-callable tool definitions for media entity resolution and library search.

These tool objects expose:
  • A JSON Schema that can be passed verbatim to any function-calling API
    (OpenAI, Anthropic tool_use, Gemini function_declarations, etc.)
  • A synchronous ``call(**kwargs) -> dict`` method
  • A human-readable description

Industrial patterns borrowed from:
  - LangChain BaseTool / StructuredTool  (name + description + _run)
  - LlamaIndex FunctionTool              (metadata + fn)
  - OpenAI function-calling spec         (JSON Schema subset)
  - Anthropic tool_use spec              (input_schema)

Typical planning-loop usage:
    tools = [RESOLVE_ENTITY, SEARCH_BY_ENTITY, SEARCH_BY_FILTERS, GET_TITLE_DETAIL]
    tool_schemas = [t.schema for t in tools]
    # → pass tool_schemas to LLM, parse its tool_call, dispatch to tool.call(**args)

The SchemaProjectionAdapter is used internally so callers NEVER need to know
the library's raw schema (video + category vs movie/anime/tv).
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable

from nav_dashboard.web.services.entity_resolver import (
    TitleRecord,
    TitleResolution,
    CreatorResolution,
    resolve_creator,
    resolve_entities,
    resolve_title,
)
from nav_dashboard.web.services.media_query_adapter import (
    SchemaProjectionAdapter,
    normalize_filter_map,
)

_adapter = SchemaProjectionAdapter()
_WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
_STRUCTURED_DIR  = _WORKSPACE_ROOT / "library_tracker" / "data" / "structured"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_all_records(media_type_filter: str = "") -> list[dict[str, Any]]:
    """Load raw records from all structured JSON files, optionally filtered."""
    records: list[dict[str, Any]] = []
    try:
        paths = sorted(_STRUCTURED_DIR.glob("*.json"), key=lambda p: p.name.lower())
    except Exception:
        return records
    for path in paths:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            rows = payload.get("records", []) if isinstance(payload, dict) else []
        except Exception:
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            if media_type_filter:
                if str(row.get("media_type", "")).strip().lower() != media_type_filter.lower():
                    continue
            records.append(row)
    return records


def _record_to_summary(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "title":       str(row.get("title") or "").strip(),
        "media_type":  str(row.get("media_type") or "").strip(),
        "category":    str(row.get("category") or "").strip(),
        "author":      str(row.get("author") or "").strip(),
        "nationality": str(row.get("nationality") or "").strip(),
        "date":        str(row.get("date") or "").strip(),
        "rating":      row.get("rating"),
        "review":      str(row.get("review") or "").strip()[:400],
    }


def _apply_filter(records: list[dict[str, Any]], projected_filters: dict[str, list[str]]) -> list[dict[str, Any]]:
    """Filter records using projected (library-native) filter dict."""
    result: list[dict[str, Any]] = []
    for row in records:
        match = True
        for field_name, allowed_values in projected_filters.items():
            if not allowed_values:
                continue
            cell = str(row.get(field_name) or "").strip().lower()
            # multi-value cells (category can be "小说, 奇幻")
            cell_values = {v.strip().lower() for v in cell.replace("，", ",").split(",") if v.strip()}
            cell_values.add(cell)  # also match full string
            allowed_lower = {v.strip().lower() for v in allowed_values if v.strip()}
            if not cell_values & allowed_lower:
                match = False
                break
        if match:
            result.append(row)
    return result


# ---------------------------------------------------------------------------
# Base class
# ---------------------------------------------------------------------------

@dataclass
class MediaTool:
    """Base class for all media-domain LLM-callable tools.

    Subclasses implement ``call(**kwargs) -> dict``.
    The ``schema`` property returns an Anthropic-style tool_use JSON schema
    (which is also compatible with OpenAI function-calling after wrapping in
    ``{"type":"function","function":schema}``).
    """

    name: str
    description: str
    parameters: dict[str, Any]   # JSON Schema "properties" object

    @property
    def schema(self) -> dict[str, Any]:
        """Anthropic tool_use / OpenAI-compatible schema dict."""
        props = self.parameters.get("properties", {})
        required = self.parameters.get("required", [])
        return {
            "name": self.name,
            "description": self.description,
            "input_schema": {
                "type": "object",
                "properties": props,
                "required": required,
            },
        }

    @property
    def openai_schema(self) -> dict[str, Any]:
        """OpenAI function-calling wrapper."""
        return {"type": "function", "function": self.schema}

    def call(self, **kwargs: Any) -> dict[str, Any]:  # noqa: ANN401
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Tool 1: resolve_entity
# ---------------------------------------------------------------------------
# Resolves a free-text name to a canonical library entity (title or creator).
# This is the first step before doing any search.  Industrial equivalent:
#   - Wikidata entity linking (exact/alias/redirect)
#   - Elasticsearch "entity-search" phase in DPR pipelines
#   - RecordLinkage / fuzzy dedup step in data warehouses

class _ResolveEntityTool(MediaTool):
    """Given a raw name string (possibly cross-language), return the canonical
    entity (title or creator) found in the local library, with confidence score.

    Returns one of:
      {kind: "title",   canonical, media_type, category, author, confidence, match_kind}
      {kind: "creator", canonical, media_type_hint, works_count,  confidence, match_kind}
      {kind: "not_found"}
    """

    def call(self, *, name: str, hint_media_type: str = "", min_confidence: float = 0.35) -> dict[str, Any]:
        name = str(name or "").strip()
        if not name:
            return {"kind": "not_found", "reason": "empty name"}

        # Run both title and creator resolution, then pick the higher-confidence result.
        # This avoids a title prefix match (e.g. "Tchaikovsky: Swan Lake") shadowing
        # a high-confidence creator alias match ("tchaikovsky" → 柴可夫斯基, conf 0.85).
        title_res   = resolve_title(name, hint_media_type=hint_media_type, min_confidence=min_confidence)
        creator_res = resolve_creator(name, hint_media_type=hint_media_type, min_confidence=min_confidence)

        title_conf   = title_res.confidence   if title_res   else 0.0
        creator_conf = creator_res.confidence if creator_res else 0.0

        # Prefer exact/alias title matches; prefer creator when it wins or ties a
        # non-exact title match (prefix/substring are unreliable for person names).
        use_creator = (
            creator_res is not None
            and (
                creator_conf > title_conf
                or (creator_conf == title_conf and title_res is not None and title_res.match_kind in {"prefix", "substring"})
            )
        )

        if use_creator and creator_res is not None:
            return {
                "kind": "creator",
                "canonical": creator_res.canonical,
                "media_type_hint": creator_res.media_type_hint,
                "works_count": len(creator_res.works),
                "confidence": round(creator_res.confidence, 3),
                "match_kind": creator_res.match_kind,
            }

        if title_res is not None:
            return {
                "kind": "title",
                "canonical": title_res.canonical,
                "media_type": title_res.media_type,
                "category": title_res.category,
                "author": title_res.author,
                "confidence": round(title_res.confidence, 3),
                "match_kind": title_res.match_kind,
            }

        return {"kind": "not_found", "name": name}


RESOLVE_ENTITY = _ResolveEntityTool(
    name="resolve_entity",
    description=(
        "Resolve a free-text name (possibly in a foreign language or alias form) to a "
        "canonical entity in the local media library. Returns either a title record, a "
        "creator record, or not_found. Always call this FIRST when the user mentions a "
        "specific title or person name, BEFORE calling search tools.\n"
        "Examples:\n"
        "  'tchaikovsky' → {kind:'creator', canonical:'柴可夫斯基', works_count:3}\n"
        "  '宫崎骏' → {kind:'creator', canonical:'宫崎骏', works_count:15}\n"
        "  '千と千尋' → {kind:'title', canonical:'千と千尋の神隠し', media_type:'video'}"
    ),
    parameters={
        "properties": {
            "name": {
                "type": "string",
                "description": "The name to resolve. Can be in any language or alias form.",
            },
            "hint_media_type": {
                "type": "string",
                "enum": ["video", "book", "music", "game", ""],
                "default": "",
                "description": "Optional media type hint to disambiguate (e.g. 'video' for films).",
            },
            "min_confidence": {
                "type": "number",
                "default": 0.35,
                "description": "Minimum match confidence (0–1). Lower → more lenient.",
            },
        },
        "required": ["name"],
    },
)


# ---------------------------------------------------------------------------
# Tool 2: get_title_detail
# ---------------------------------------------------------------------------
# Fetch full record for a canonical title.  No fuzzy matching — exact lookup.

class _GetTitleDetailTool(MediaTool):
    """Fetch the full library record for a canonical title returned by resolve_entity."""

    def call(self, *, canonical_title: str, media_type: str = "") -> dict[str, Any]:
        canonical_title = str(canonical_title or "").strip()
        if not canonical_title:
            return {"error": "canonical_title required"}

        from nav_dashboard.web.services.entity_resolver import _INDEX, _norm
        idx = _INDEX.ensure_fresh()
        nq = _norm(canonical_title)
        records = idx.title_idx.get(nq, [])
        if not records:
            # Try brute scan
            records = _load_all_records(media_type)
            records = [r for r in records if _norm(str(r.get("title", ""))) == nq]

        if not records:
            return {"found": False, "canonical_title": canonical_title}

        if media_type:
            filtered = [r for r in records if isinstance(r, TitleRecord) and r.media_type == media_type]
            if not filtered:
                filtered = [r for r in records if isinstance(r, dict) and r.get("media_type") == media_type]
            if filtered:
                records = filtered

        rec = records[0]
        if isinstance(rec, TitleRecord):
            return {"found": True, **{k: getattr(rec, k) for k in ("canonical", "media_type", "category", "author", "date", "rating")}}
        return {"found": True, **_record_to_summary(rec)}


GET_TITLE_DETAIL = _GetTitleDetailTool(
    name="get_title_detail",
    description=(
        "Fetch the full personal library record for a canonical title "
        "(as returned by resolve_entity). Returns rating, review, date watched/read, etc. "
        "Use this when you already know the exact canonical title from resolve_entity."
    ),
    parameters={
        "properties": {
            "canonical_title": {
                "type": "string",
                "description": "Exact canonical title string from resolve_entity result.",
            },
            "media_type": {
                "type": "string",
                "enum": ["video", "book", "music", "game", ""],
                "default": "",
                "description": "Optional media type to disambiguate same-title entries.",
            },
        },
        "required": ["canonical_title"],
    },
)


# ---------------------------------------------------------------------------
# Tool 3: search_by_creator
# ---------------------------------------------------------------------------
# Fetch all works by a canonical creator.  Resolves cross-language aliases first.

class _SearchByCreatorTool(MediaTool):
    """Return all library records by a given creator (director, author, composer, etc.).

    If the name isn't exactly in the library, attempts alias resolution.
    """

    def call(self, *, creator_name: str, media_type: str = "", max_results: int = 20) -> dict[str, Any]:
        creator_name = str(creator_name or "").strip()
        if not creator_name:
            return {"error": "creator_name required"}

        creator_res = resolve_creator(creator_name, hint_media_type=media_type, min_confidence=0.35)
        if creator_res is None:
            return {"found": False, "creator_name": creator_name}

        works = creator_res.works
        if media_type:
            filtered = [w for w in works if w.media_type == media_type]
            if filtered:
                works = filtered

        works = works[:max_results]
        return {
            "found": True,
            "canonical_creator": creator_res.canonical,
            "match_kind": creator_res.match_kind,
            "confidence": round(creator_res.confidence, 3),
            "media_type_hint": creator_res.media_type_hint,
            "works_count": len(creator_res.works),
            "works": [
                {
                    "title":    w.canonical,
                    "category": w.category,
                    "date":     w.date,
                    "rating":   w.rating,
                    "review":   w.review[:200] if w.review else "",
                }
                for w in works
            ],
        }


SEARCH_BY_CREATOR = _SearchByCreatorTool(
    name="search_by_creator",
    description=(
        "Return all personal library records for a given creator (director, author, "
        "composer, developer, etc.). Handles cross-language name aliases automatically.\n"
        "Examples:\n"
        "  creator_name='宫崎骏', media_type='video'  → his films in my library\n"
        "  creator_name='tchaikovsky', media_type='music' → his albums/pieces I have"
    ),
    parameters={
        "properties": {
            "creator_name": {
                "type": "string",
                "description": "Creator name in any language or alias form.",
            },
            "media_type": {
                "type": "string",
                "enum": ["video", "book", "music", "game", ""],
                "default": "",
                "description": "Filter to a specific media type.",
            },
            "max_results": {
                "type": "integer",
                "default": 20,
                "description": "Maximum number of works to return.",
            },
        },
        "required": ["creator_name"],
    },
)


# ---------------------------------------------------------------------------
# Tool 4: search_by_filters
# ---------------------------------------------------------------------------
# Semantic-slot filter search.  SchemaProjectionAdapter transparently handles
# movie→video+category:电影, director→author, etc.

class _SearchByFiltersTool(MediaTool):
    """Search the library using semantic filter slots.

    The adapter layer handles all schema projection automatically:
      • media_type='movie' → library field media_type='video', category='电影'
      • media_type='anime' → media_type='video', category='动画'
      • director/composer/developer → library field 'author'

    Returns matched records sorted by date desc.
    """

    def call(
        self,
        *,
        media_type: str = "",
        category: str = "",
        author: str = "",
        nationality: str = "",
        year: str = "",
        min_rating: float | None = None,
        sort_by: str = "date",
        max_results: int = 20,
    ) -> dict[str, Any]:
        raw_filters: dict[str, list[str]] = {}
        if media_type:
            raw_filters["media_type"] = [media_type]
        if category:
            raw_filters["category"] = [category]
        if author:
            raw_filters["author"] = [author]
        if nationality:
            raw_filters["nationality"] = [nationality]
        if year:
            raw_filters["year"] = [year]

        spec = _adapter.project(raw_filters)
        all_records = _load_all_records()
        matched = _apply_filter(all_records, spec.filters)

        if min_rating is not None:
            def _rating_ok(row: dict[str, Any]) -> bool:
                try:
                    return float(row.get("rating") or 0) >= float(min_rating)
                except Exception:
                    return False
            matched = [r for r in matched if _rating_ok(r)]

        if sort_by == "date":
            matched.sort(key=lambda r: str(r.get("date") or ""), reverse=True)
        elif sort_by == "rating":
            matched.sort(key=lambda r: float(r.get("rating") or 0), reverse=True)

        matched = matched[:max_results]
        return {
            "applied_filters": spec.filters,
            "resolved_media_type": spec.resolved_media_type,
            "result_count": len(matched),
            "results": [_record_to_summary(r) for r in matched],
        }


SEARCH_BY_FILTERS = _SearchByFiltersTool(
    name="search_by_filters",
    description=(
        "Search the personal library using semantic filter slots. "
        "Handles schema projection automatically — you can pass 'movie', 'anime', 'tv' "
        "as media_type and it will map to the correct library fields.\n"
        "Use this for collection queries like 'show me all Japanese films', "
        "'books by Chinese authors', 'games I rated above 8'."
    ),
    parameters={
        "properties": {
            "media_type": {
                "type": "string",
                "description": "Semantic media type: 'movie', 'anime', 'tv', 'book', 'music', 'game', 'video'.",
            },
            "category": {
                "type": "string",
                "description": "Genre/category string as it appears in the library.",
            },
            "author": {
                "type": "string",
                "description": "Creator name — for videos this is the director, for books the author, etc.",
            },
            "nationality": {
                "type": "string",
                "description": "Country/nationality of the work or creator.",
            },
            "year": {
                "type": "string",
                "description": "4-digit year the item was consumed/read/watched.",
            },
            "min_rating": {
                "type": "number",
                "description": "Only return records with rating >= this value.",
            },
            "sort_by": {
                "type": "string",
                "enum": ["date", "rating", "relevance"],
                "default": "date",
            },
            "max_results": {
                "type": "integer",
                "default": 20,
                "description": "Maximum number of results to return.",
            },
        },
        "required": [],
    },
)


# ---------------------------------------------------------------------------
# Tool 5: resolve_and_search
# ---------------------------------------------------------------------------
# Convenience compound tool: resolves the entity, then either fetches the
# full detail (title) or all works (creator).  This is the "just do it"
# single-step tool when you don't need fine-grained planning.

class _ResolveAndSearchTool(MediaTool):
    """Resolve an entity name and immediately return the matching library records.

    Combines resolve_entity + get_title_detail (for titles) or
    search_by_creator (for creators) in one call.

    Best used when you already know the user is asking about a specific entity
    and just need the library data without intermediate steps.
    """

    def call(self, *, name: str, hint_media_type: str = "", max_creator_works: int = 20) -> dict[str, Any]:
        name = str(name or "").strip()
        if not name:
            return {"error": "name required"}

        resolution = RESOLVE_ENTITY.call(name=name, hint_media_type=hint_media_type)
        kind = resolution.get("kind", "not_found")

        if kind == "not_found":
            return {"found": False, "name": name}

        if kind == "title":
            detail = GET_TITLE_DETAIL.call(
                canonical_title=resolution["canonical"],
                media_type=resolution.get("media_type", ""),
            )
            return {
                "found": True,
                "entity_kind": "title",
                "resolution": resolution,
                "detail": detail,
            }

        if kind == "creator":
            works = SEARCH_BY_CREATOR.call(
                creator_name=resolution["canonical"],
                media_type=hint_media_type,
                max_results=max_creator_works,
            )
            return {
                "found": True,
                "entity_kind": "creator",
                "resolution": resolution,
                "works": works,
            }

        return {"found": False, "name": name}


RESOLVE_AND_SEARCH = _ResolveAndSearchTool(
    name="resolve_and_search",
    description=(
        "Single-step tool: resolve a name to a canonical entity and immediately "
        "return the matching library data. Combines resolve_entity + detail/creator-search.\n"
        "Use this when the user asks about a specific title or person and you want all "
        "relevant library records in one pass without multi-step planning."
    ),
    parameters={
        "properties": {
            "name": {
                "type": "string",
                "description": "Title or creator name in any language.",
            },
            "hint_media_type": {
                "type": "string",
                "enum": ["video", "book", "music", "game", ""],
                "default": "",
                "description": "Optional media type hint.",
            },
            "max_creator_works": {
                "type": "integer",
                "default": 20,
                "description": "Max works to return if entity resolves to a creator.",
            },
        },
        "required": ["name"],
    },
)


# ---------------------------------------------------------------------------
# Public registry
# ---------------------------------------------------------------------------

ALL_MEDIA_TOOLS: list[MediaTool] = [
    RESOLVE_ENTITY,
    GET_TITLE_DETAIL,
    SEARCH_BY_CREATOR,
    SEARCH_BY_FILTERS,
    RESOLVE_AND_SEARCH,
]

# Anthropic-style tool schemas list (ready to pass to client.messages.create)
ANTHROPIC_TOOL_SCHEMAS: list[dict[str, Any]] = [t.schema for t in ALL_MEDIA_TOOLS]

# OpenAI-style function schemas list
OPENAI_TOOL_SCHEMAS: list[dict[str, Any]] = [t.openai_schema for t in ALL_MEDIA_TOOLS]

# Name → tool lookup
TOOL_REGISTRY: dict[str, MediaTool] = {t.name: t for t in ALL_MEDIA_TOOLS}


def dispatch_tool_call(name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    """Dispatch an LLM tool call by name with parsed arguments.

    Returns the tool result dict, or an error dict if unknown tool.
    """
    tool = TOOL_REGISTRY.get(name)
    if tool is None:
        return {"error": f"unknown tool: {name!r}", "available": sorted(TOOL_REGISTRY)}
    try:
        return tool.call(**arguments)
    except Exception as exc:
        return {"error": str(exc), "tool": name}
