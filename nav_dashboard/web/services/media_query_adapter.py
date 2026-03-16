from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Literal

FollowupMode = Literal["none", "inherit_filters", "inherit_entity", "inherit_timerange"]
FollowupQueryKind = Literal["standalone", "elliptical_followup"]
FollowupMergeStrategy = Literal["none", "carry", "replace", "augment"]

_SCOPE_OVERRIDE_FIELDS = {
    "media_type",
    "category",
    "genre",
    "nationality",
    "series",
    "platform",
    "author",
    "authors",
    "director",
    "directors",
    "actor",
    "actors",
    "tag",
    "tags",
    "year",
}

_MEDIA_TYPE_PROJECTIONS = {
    "movie": {"media_type": ["video"], "category": ["电影"]},
    "film": {"media_type": ["video"], "category": ["电影"]},
    "tv": {"media_type": ["video"], "category": ["电视剧"]},
    "series": {"media_type": ["video"], "category": ["电视剧"]},
    "show": {"media_type": ["video"], "category": ["电视剧"]},
    "anime": {"media_type": ["video"], "category": ["动画"]},
}

_VIDEO_CATEGORY_ALIASES = {
    "影片": "电影",
    "片子": "电影",
    "电影": "电影",
    "剧集": "电视剧",
    "连续剧": "电视剧",
    "电视剧": "电视剧",
    "番": "动画",
    "番剧": "动画",
    "动漫": "动画",
    "动画": "动画",
}


def normalize_filter_map(value: Any) -> dict[str, list[str]]:
    if not isinstance(value, dict):
        return {}
    normalized: dict[str, list[str]] = {}
    for key, raw_values in value.items():
        field_name = str(key or "").strip()
        if not field_name:
            continue
        if isinstance(raw_values, list):
            values = [str(item).strip() for item in raw_values if str(item).strip()]
        else:
            values = [str(raw_values).strip()] if str(raw_values).strip() else []
        if values:
            normalized[field_name] = values
    return normalized


def _merge_unique(base: dict[str, list[str]], field: str, values: list[str]) -> None:
    clean_values = [str(value).strip() for value in values if str(value).strip()]
    if not clean_values:
        return
    existing = [str(value).strip() for value in base.get(field, []) if str(value).strip()]
    seen = {value.casefold() for value in existing}
    for value in clean_values:
        folded = value.casefold()
        if folded in seen:
            continue
        seen.add(folded)
        existing.append(value)
    if existing:
        base[field] = existing


@dataclass
class FollowupResolution:
    mode: FollowupMode = "none"
    query_kind: FollowupQueryKind = "standalone"
    merge_strategy: FollowupMergeStrategy = "none"
    reasons: list[str] = field(default_factory=list)


@dataclass
class RetrievalFilterProjection:
    filters: dict[str, list[str]] = field(default_factory=dict)
    semantic_family: str = ""
    semantic_subtype: str = ""
    resolved_media_type: str = ""
    applied_repairs: list[str] = field(default_factory=list)


def resolve_followup_strategy(
    *,
    question: str,
    previous_has_media_context: bool,
    previous_has_entity: bool,
    has_explicit_fresh_scope: bool,
    has_explicit_entities: bool,
    has_title_marker: bool,
    looks_time_only_followup: bool,
    is_short_followup_surface: bool,
    wants_media_details: bool,
    wants_personal_evaluation: bool,
    is_collection_query: bool,
) -> FollowupResolution:
    text = str(question or "").strip()
    if not text or not previous_has_media_context:
        return FollowupResolution()
    if has_explicit_fresh_scope or has_explicit_entities or has_title_marker:
        return FollowupResolution(reasons=["explicit_current_scope"])
    if looks_time_only_followup:
        return FollowupResolution(
            mode="inherit_timerange",
            query_kind="elliptical_followup",
            merge_strategy="carry",
            reasons=["time_only_followup"],
        )
    if is_short_followup_surface:
        return FollowupResolution(
            mode="inherit_entity" if previous_has_entity else "inherit_filters",
            query_kind="elliptical_followup",
            merge_strategy="carry",
            reasons=["short_elliptical_followup"],
        )
    if wants_media_details and previous_has_entity and not is_collection_query:
        return FollowupResolution(
            mode="inherit_entity",
            query_kind="elliptical_followup",
            merge_strategy="carry",
            reasons=["detail_followup"],
        )
    if wants_personal_evaluation and previous_has_entity and not is_collection_query:
        return FollowupResolution(
            mode="inherit_entity",
            query_kind="elliptical_followup",
            merge_strategy="carry",
            reasons=["evaluation_followup"],
        )
    return FollowupResolution(reasons=["standalone_by_default"])


def merge_followup_filters(
    previous_filters: dict[str, list[str]] | None,
    current_filters: dict[str, list[str]] | None,
    *,
    strategy: FollowupMergeStrategy,
) -> dict[str, list[str]]:
    previous = normalize_filter_map(previous_filters)
    current = normalize_filter_map(current_filters)
    if strategy == "none" or not previous:
        return current
    if not current:
        return previous

    merged = {key: list(values) for key, values in previous.items()}
    scope_override = any(field in current for field in _SCOPE_OVERRIDE_FIELDS)
    if strategy in {"carry", "replace"} and scope_override:
        for field in _SCOPE_OVERRIDE_FIELDS:
            merged.pop(field, None)

    for field, values in current.items():
        if strategy == "augment" and field not in _SCOPE_OVERRIDE_FIELDS:
            _merge_unique(merged, field, values)
            continue
        merged[field] = list(values)
    return merged


def project_media_filters_to_library_schema(
    filters: dict[str, list[str]] | None,
    *,
    resolved_question: str = "",
) -> RetrievalFilterProjection:
    normalized = normalize_filter_map(filters)
    projected: dict[str, list[str]] = {}
    repairs: list[str] = []
    semantic_family = ""
    semantic_subtype = ""

    for value in normalized.get("media_type", []):
        lowered = str(value).strip().lower()
        projection = _MEDIA_TYPE_PROJECTIONS.get(lowered)
        if projection is None:
            _merge_unique(projected, "media_type", [lowered])
            if not semantic_family:
                semantic_family = lowered
            continue
        if lowered in {"movie", "film"}:
            semantic_family = "video"
            semantic_subtype = "movie"
        elif lowered in {"tv", "series", "show"}:
            semantic_family = "video"
            semantic_subtype = "tv"
        elif lowered == "anime":
            semantic_family = "video"
            semantic_subtype = "anime"
        for field, projected_values in projection.items():
            _merge_unique(projected, field, projected_values)
        repairs.append(f"media_type:{lowered}->library_schema")

    for field, values in normalized.items():
        if field == "media_type":
            continue
        if field == "category":
            normalized_categories = []
            for value in values:
                category = _VIDEO_CATEGORY_ALIASES.get(str(value).strip(), str(value).strip())
                if category != str(value).strip():
                    repairs.append(f"category:{value}->{category}")
                normalized_categories.append(category)
                if category == "电影" and not semantic_subtype:
                    semantic_family = semantic_family or "video"
                    semantic_subtype = "movie"
                elif category == "电视剧" and not semantic_subtype:
                    semantic_family = semantic_family or "video"
                    semantic_subtype = "tv"
                elif category == "动画" and not semantic_subtype:
                    semantic_family = semantic_family or "video"
                    semantic_subtype = "anime"
            _merge_unique(projected, field, normalized_categories)
            continue
        _merge_unique(projected, field, values)

    media_types = [str(value).strip().lower() for value in projected.get("media_type", []) if str(value).strip()]
    categories = [str(value).strip() for value in projected.get("category", []) if str(value).strip()]
    if "video" in media_types:
        if any(category == "电影" for category in categories):
            semantic_family = "video"
            semantic_subtype = semantic_subtype or "movie"
        elif any(category == "电视剧" for category in categories):
            semantic_family = "video"
            semantic_subtype = semantic_subtype or "tv"
        elif any(category == "动画" for category in categories):
            semantic_family = "video"
            semantic_subtype = semantic_subtype or "anime"
        else:
            semantic_family = semantic_family or "video"

    resolved_media_type = derive_resolved_media_type_label(projected, resolved_question=resolved_question)
    return RetrievalFilterProjection(
        filters=projected,
        semantic_family=semantic_family,
        semantic_subtype=semantic_subtype,
        resolved_media_type=resolved_media_type,
        applied_repairs=sorted(set(repairs)),
    )


def derive_resolved_media_type_label(filters: dict[str, list[str]] | None, *, resolved_question: str = "") -> str:
    normalized = normalize_filter_map(filters)
    media_types = [str(value).strip().lower() for value in normalized.get("media_type", []) if str(value).strip()]
    categories = [str(value).strip() for value in normalized.get("category", []) if str(value).strip()]
    if "video" in media_types and any(category == "动画" for category in categories):
        return "anime"
    if "video" in media_types and any(category == "电影" for category in categories):
        return "movie"
    if "video" in media_types and any(category == "电视剧" for category in categories):
        return "tv"
    if media_types:
        return media_types[0]

    text = str(resolved_question or "")
    if any(token in text for token in ("游戏", "打过", "玩过", "在玩", "通关")):
        return "game"
    if any(token in text for token in ("音乐", "专辑", "歌曲", "歌单", "听过")):
        return "music"
    if any(token in text for token in ("电影", "影片", "片子")):
        return "movie"
    if any(token in text for token in ("电视剧", "剧集", "连续剧", "美剧", "日剧", "韩剧", "英剧")):
        return "tv"
    if any(token in text for token in ("动画", "动漫", "番", "番剧", "新番")):
        return "anime"
    return ""


def maybe_retry_normalized_filters(
    raw_filters: dict[str, list[str]] | None,
    *,
    resolved_question: str = "",
    result_count: int,
) -> RetrievalFilterProjection | None:
    if result_count > 0:
        return None
    projection = project_media_filters_to_library_schema(raw_filters, resolved_question=resolved_question)
    if projection.filters == normalize_filter_map(raw_filters):
        return None
    return projection


# ---------------------------------------------------------------------------
# SchemaProjectionAdapter
# ---------------------------------------------------------------------------
# In the library_tracker structured JSON files, all creator roles are stored
# in a single "author" field regardless of media type:
#   video   → author is the director
#   book    → author is the writer
#   music   → author is the performer/band/composer
#   game    → author is the developer/studio
#
# The LLM or upstream code may emit filter keys like "director", "composer",
# "developer", etc.  This adapter remaps all such role fields to "author"
# before passing filters to project_media_filters_to_library_schema().

# Mapping from semantic creator-role field names → library "author" field.
# All plural variants are also included and normalised to singular "author".
_CREATOR_ROLE_TO_AUTHOR: dict[str, str] = {
    # video roles
    "director":     "author",
    "directors":    "author",
    # music roles
    "composer":     "author",
    "composers":    "author",
    "performer":    "author",
    "performers":   "author",
    "artist":       "author",
    "artists":      "author",
    "band":         "author",
    # book roles
    "writer":       "author",
    "writers":      "author",
    "illustrator":  "author",
    "illustrators": "author",
    # game roles
    "developer":    "author",
    "developers":   "author",
    "studio":       "author",
    "studios":      "author",
    # generic
    "creator":      "author",
    "creators":     "author",
    "producer":     "author",
    "producers":    "author",
}


def project_creator_role_fields(
    filters: dict[str, list[str]] | None,
) -> tuple[dict[str, list[str]], list[str]]:
    """Remap semantic creator-role fields (director/composer/…) to 'author'.

    Returns:
        (projected_filters, repairs)  where repairs lists each remapping made.
    """
    normalized = normalize_filter_map(filters)
    result: dict[str, list[str]] = {}
    repairs: list[str] = []
    for field_name, values in normalized.items():
        target = _CREATOR_ROLE_TO_AUTHOR.get(field_name.lower())
        if target:
            repairs.append(f"{field_name}->{target}")
            _merge_unique(result, target, values)
        else:
            _merge_unique(result, field_name, values)
    return result, repairs


# Re-use RetrievalFilterProjection as the output type — it already carries
# the library-native filter dict plus semantic family / repair metadata.
# Alias it for clarity at call sites.
LibraryQuerySpec = RetrievalFilterProjection


class SchemaProjectionAdapter:
    """Single barrier between semantic understanding and library query spec.

    Combines two projection steps:
      1. Creator-role remapping  — director/composer/developer/… → author
      2. Media-type projection   — movie/anime/tv/… → video + category

    Usage::

        adapter = SchemaProjectionAdapter()
        spec = adapter.project(llm_filters, resolved_question=question)
        # spec.filters is ready for library_tracker search
        # spec.resolved_media_type is e.g. "movie" / "anime" / "book"
    """

    def project(
        self,
        filters: dict[str, list[str]] | None,
        *,
        resolved_question: str = "",
    ) -> LibraryQuerySpec:
        """Project semantic filter slots to library-native query spec."""
        # Step 1 – remap creator roles
        remapped, role_repairs = project_creator_role_fields(filters)
        # Step 2 – project media type semantics
        projection = project_media_filters_to_library_schema(
            remapped, resolved_question=resolved_question
        )
        if role_repairs:
            projection.applied_repairs = sorted(
                set(projection.applied_repairs + role_repairs)
            )
        return projection