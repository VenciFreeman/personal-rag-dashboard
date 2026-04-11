from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

from ..agent.agent_types import (
    TOOL_QUERY_MEDIA,
    TOOL_SEARCH_BY_CREATOR,
    ToolExecution,
)
from .media_policy_rules import is_personal_review_mode as is_personal_media_review_mode
from .media_external_enrichment import (
    PerItemExpansionConfig,
    PerItemExpansionToolset,
    execute_per_item_expansion,
    normalize_title_for_match,
    title_similarity,
    validate_per_item_bangumi_match,
    validate_per_item_tmdb_match,
)


@dataclass
class MediaPolicyFlags:
    strict_scope_active: bool = False
    working_set_reused: bool = False
    answer_shape: str = ""
    media_family: str = ""


@dataclass
class MediaRetrievalRequest:
    query: str
    previous_working_set: dict[str, Any] = field(default_factory=dict)
    resolved_entities: list[str] = field(default_factory=list)
    policy_flags: MediaPolicyFlags = field(default_factory=MediaPolicyFlags)


@dataclass
class MediaRetrievalResponse:
    request: MediaRetrievalRequest
    main_results: list[dict[str, Any]] = field(default_factory=list)
    mention_results: list[dict[str, Any]] = field(default_factory=list)
    external_candidates: list[dict[str, Any]] = field(default_factory=list)
    validation: dict[str, Any] = field(default_factory=dict)
    layer_breakdown: dict[str, Any] = field(default_factory=dict)
    per_item_stats: dict[str, Any] = field(default_factory=dict)
    media_result_data: dict[str, Any] = field(default_factory=dict)


_TITLE_STRIP_RE = re.compile(r"[銆娿€嬨€屻€嶃€庛€忋€愩€戯紙锛?)\s銆€]+")
_CJK_RANGE_RE = re.compile(r"[\u3040-\u9fff\uf900-\ufaff\u3400-\u4dbf]+")


def _normalize_title_key(value: str) -> str:
    text = _TITLE_STRIP_RE.sub("", str(value or "")).strip().lower()
    return text


def _media_row_identity_key(row: dict[str, Any]) -> str:
    row_id = str(row.get("id") or "").strip().lower()
    if row_id:
        return f"id:{row_id}"
    title_key = _normalize_title_key(str(row.get("title") or ""))
    if not title_key:
        return ""
    media_type = str(row.get("media_type") or "").strip().lower()
    return f"title:{media_type}:{title_key}"


def _merge_unique_string_list(*values: Any) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for value in values:
        if isinstance(value, list):
            items = value
        else:
            items = [value]
        for item in items:
            text = str(item or "").strip()
            if not text:
                continue
            lowered = text.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            merged.append(text)
    return merged


def _merge_unique_object_list(*values: Any) -> list[Any]:
    merged: list[Any] = []
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, list):
            continue
        for item in value:
            if isinstance(item, dict):
                marker = json.dumps(item, ensure_ascii=False, sort_keys=True)
            else:
                marker = str(item)
            if not marker or marker in seen:
                continue
            seen.add(marker)
            merged.append(item)
    return merged


def _row_preference_key(row: dict[str, Any]) -> tuple[float, float, int, int, int, int, int]:
    return (
        _safe_score(row.get("score")),
        _safe_score(row.get("title_boost")),
        len(row.get("answer_layer_reasons") or []),
        len(row.get("alias_hits") or []),
        len(row.get("keyword_hits") or []),
        int(bool(str(row.get("review") or row.get("comment") or "").strip())),
        int(bool(str(row.get("date") or "").strip())),
    )


def _merge_media_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    preferred = max(rows, key=_row_preference_key)
    merged = dict(preferred)
    merged["score"] = max((_safe_score(row.get("score")) for row in rows), default=_safe_score(merged.get("score")))
    merged["title_boost"] = max((_safe_score(row.get("title_boost")) for row in rows), default=_safe_score(merged.get("title_boost")))
    merged["match_terms"] = normalize_media_match_terms(
        _merge_unique_string_list(*(row.get("match_terms") or [] for row in rows))
    )
    merged["matched_entities"] = _merge_unique_string_list(*(row.get("matched_entities") or [] for row in rows))
    merged["answer_layer_reasons"] = _merge_unique_string_list(*(row.get("answer_layer_reasons") or [] for row in rows))
    merged["alias_hits"] = _merge_unique_object_list(*(row.get("alias_hits") for row in rows))
    merged["keyword_hits"] = _merge_unique_object_list(*(row.get("keyword_hits") for row in rows))
    merged["retrieval_query"] = next(
        (
            text
            for text in (
                str(row.get("retrieval_query") or row.get("matched_query") or "").strip()
                for row in rows
            )
            if text
        ),
        str(merged.get("retrieval_query") or merged.get("matched_query") or "").strip(),
    )
    if merged.get("working_set_rank"):
        merged["working_set_rank"] = min(
            int(row.get("working_set_rank") or 999999)
            for row in rows
            if str(row.get("working_set_rank") or "").strip()
        )
    return merged


def dedupe_media_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    grouped: dict[str, list[dict[str, Any]]] = {}
    passthrough: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = _media_row_identity_key(row)
        if not key:
            passthrough.append(dict(row))
            continue
        grouped.setdefault(key, []).append(row)
    seen_keys: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = _media_row_identity_key(row)
        if not key or key in seen_keys:
            continue
        seen_keys.add(key)
        deduped.append(_merge_media_rows(grouped[key]))
    deduped.extend(passthrough)
    return deduped






def _safe_score(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _normalize_filter_map(value: Any) -> dict[str, list[str]]:
    if not isinstance(value, dict):
        return {}
    normalized: dict[str, list[str]] = {}
    for key, raw_values in value.items():
        clean_key = str(key or "").strip()
        if not clean_key:
            continue
        if isinstance(raw_values, list):
            values = [str(item).strip() for item in raw_values if str(item).strip()]
        else:
            single = str(raw_values or "").strip()
            values = [single] if single else []
        if values:
            normalized[clean_key] = values
    return normalized


def normalize_media_match_terms(values: list[str]) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for value in values:
        clean = _normalize_title_key(value)
        if not clean or len(clean) < 2 or clean in seen:
            continue
        seen.add(clean)
        normalized.append(clean)
    return normalized


def _media_row_keyword_hit_fields(row: dict[str, Any]) -> set[str]:
    hits = row.get("keyword_hits") if isinstance(row.get("keyword_hits"), list) else []
    fields: set[str] = set()
    for hit in hits:
        if not isinstance(hit, dict):
            continue
        field = str(hit.get("field") or "").strip().lower()
        if field:
            fields.add(field)
    return fields


def _media_row_intrinsic_match_reasons(
    row: dict[str, Any],
    *,
    strict_terms: list[str],
    family_terms: list[str],
) -> list[str]:
    reasons: list[str] = []
    intrinsic_fields = (
        ("title", "title"),
        ("author", "author"),
        ("publisher", "publisher"),
        ("channel", "channel"),
        ("category", "category"),
    )
    for field_name, reason_name in intrinsic_fields:
        normalized_value = _normalize_title_key(str(row.get(field_name) or ""))
        if not normalized_value:
            continue
        if strict_terms and any(term in normalized_value for term in strict_terms):
            reasons.append(f"strict_term_in_{reason_name}")
        if family_terms and any(term in normalized_value for term in family_terms):
            reasons.append(f"family_term_in_{reason_name}")
    return reasons


def classify_media_row_answer_layer(
    row: dict[str, Any],
    *,
    strict_scope_active: bool,
    strict_terms: list[str],
    family_terms: list[str],
    promote_context_fields: set[str] | None = None,
    promote_family_context_to_main: bool = False,
) -> tuple[str, list[str]]:
    if not strict_scope_active:
        return "main", ["non_strict_scope"]
    if bool(row.get("anchor_conflict")):
        return "mention", ["anchor_conflict"]

    main_reasons: list[str] = []
    contextual_reasons: list[str] = []
    promoted_main_reasons: list[str] = []
    promote_fields = {
        str(field).strip().lower()
        for field in (promote_context_fields or set())
        if str(field).strip()
    }
    normalized_title = _normalize_title_key(str(row.get("title") or ""))
    keyword_fields = _media_row_keyword_hit_fields(row)
    review_text = _normalize_title_key(str(row.get("review") or ""))

    if _safe_score(row.get("title_boost")) > 0:
        main_reasons.append("title_boost")
    if list(row.get("alias_hits") or []):
        main_reasons.append("alias_hit")
    if "title" in keyword_fields:
        main_reasons.append("keyword:title")
    if normalized_title and any(term in normalized_title for term in strict_terms):
        main_reasons.append("strict_term_in_title")
    if normalized_title and any(term in normalized_title for term in family_terms):
        main_reasons.append("family_term_in_title")
    for field_name, reason_name in (("author", "author"), ("publisher", "publisher"), ("channel", "channel"), ("category", "category")):
        normalized_value = _normalize_title_key(str(row.get(field_name) or ""))
        if not normalized_value:
            continue
        if strict_terms and any(term in normalized_value for term in strict_terms):
            reason = f"strict_term_in_{reason_name}"
            if reason_name in promote_fields:
                promoted_main_reasons.append(reason)
            else:
                contextual_reasons.append(reason)
        if family_terms and any(term in normalized_value for term in family_terms):
            reason = f"family_term_in_{reason_name}"
            if reason_name in promote_fields:
                promoted_main_reasons.append(reason)
            else:
                contextual_reasons.append(reason)
    for field in sorted(keyword_fields & {"author", "publisher", "channel", "category"}):
        reason = f"keyword:{field}"
        if field in promote_fields:
            promoted_main_reasons.append(reason)
        else:
            contextual_reasons.append(reason)

    if main_reasons or promoted_main_reasons:
        return "main", [*main_reasons, *promoted_main_reasons]
    if promote_family_context_to_main and contextual_reasons:
        return "main", contextual_reasons
    if contextual_reasons:
        return "mention", contextual_reasons
    if review_text and strict_terms and any(term in review_text for term in strict_terms):
        return "mention", ["strict_term_in_review"]
    if review_text and family_terms and any(term in review_text for term in family_terms):
        return "mention", ["family_term_in_review"]
    if bool(row.get("alias_expansion_match")):
        if promote_family_context_to_main:
            return "main", ["alias_expansion_related"]
        return "mention", ["alias_expansion_related"]
    if "review" in keyword_fields:
        return "mention", ["review_mention"]
    return "excluded", ["semantic_only_candidate"]


def split_media_results_for_answer_layers(
    rows: list[dict[str, Any]],
    *,
    strict_scope_active: bool,
    strict_terms: list[str],
    family_terms: list[str],
    promote_context_fields: set[str] | None = None,
    promote_family_context_to_main: bool = False,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    if not strict_scope_active:
        return list(rows), [], {
            "strict_scope_active": False,
            "main_count": len(rows),
            "mention_count": 0,
            "excluded_count": 0,
        }

    main_results: list[dict[str, Any]] = []
    mention_results: list[dict[str, Any]] = []
    excluded_count = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        layer, reasons = classify_media_row_answer_layer(
            row,
            strict_scope_active=strict_scope_active,
            strict_terms=strict_terms,
            family_terms=family_terms,
            promote_context_fields=promote_context_fields,
            promote_family_context_to_main=promote_family_context_to_main,
        )
        if layer == "excluded":
            excluded_count += 1
            continue
        cloned = dict(row)
        cloned["answer_layer"] = layer
        cloned["answer_layer_reasons"] = reasons
        if layer == "mention":
            mention_results.append(cloned)
        else:
            main_results.append(cloned)
    return main_results, mention_results, {
        "strict_scope_active": True,
        "main_count": len(main_results),
        "mention_count": len(mention_results),
        "excluded_count": excluded_count,
    }


def _coerce_rows(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _get_media_result_data(tool_results: list[ToolExecution]) -> dict[str, Any]:
    media_result = next(
        (
            item
            for item in tool_results
            if item.tool in {TOOL_QUERY_MEDIA, TOOL_SEARCH_BY_CREATOR} and isinstance(item.data, dict)
        ),
        None,
    )
    if media_result is None:
        return {}
    return dict(media_result.data)


def get_media_main_result_rows_from_data(data: dict[str, Any] | None) -> list[dict[str, Any]]:
    payload = data if isinstance(data, dict) else {}
    rows = payload.get("main_results")
    if not isinstance(rows, list):
        rows = payload.get("results")
    return dedupe_media_rows(_coerce_rows(rows))


def get_media_mention_rows_from_data(data: dict[str, Any] | None) -> list[dict[str, Any]]:
    payload = data if isinstance(data, dict) else {}
    rows = payload.get("mention_results")
    if not isinstance(rows, list):
        return []
    return dedupe_media_rows(_coerce_rows(rows))


def _extract_main_rows(media_result_data: dict[str, Any]) -> list[dict[str, Any]]:
    return get_media_main_result_rows_from_data(media_result_data)


def _extract_mention_rows(media_result_data: dict[str, Any]) -> list[dict[str, Any]]:
    mention_rows = get_media_mention_rows_from_data(media_result_data)
    if mention_rows:
        return mention_rows
    validation = media_result_data.get("validation") if isinstance(media_result_data.get("validation"), dict) else {}
    if int(validation.get("mention_result_count", 0) or 0) <= 0:
        return []
    return _coerce_rows(media_result_data.get("mention_rows"))


def _extract_external_candidates(tool_results: list[ToolExecution]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    for item in tool_results:
        data = item.data if isinstance(item.data, dict) else {}
        if not data.get("per_item_fanout"):
            continue
        rows = _coerce_rows(data.get("per_item_data"))
        if not rows:
            rows = _coerce_rows(data.get("results"))
        expanded_count = len(rows)
        source_title_count = int(data.get("source_title_count", 0) or 0)
        per_item_stats = {
            "tool": item.tool,
            "expanded_count": expanded_count,
            "source_title_count": source_title_count,
            "per_item_source": str(data.get("per_item_source") or "").strip(),
            "source_counts": dict(data.get("source_counts") or {}),
            "mixed_sources": bool(data.get("mixed_sources")),
            "per_item_expand_limit": int(data.get("per_item_expand_limit", 0) or 0),
        }
        return rows, per_item_stats
    return [], {}


def get_media_validation(tool_results: list[ToolExecution]) -> dict[str, Any]:
    media_result_data = _get_media_result_data(tool_results)
    validation = media_result_data.get("validation") if isinstance(media_result_data.get("validation"), dict) else {}
    normalized = dict(validation)
    normalized["returned_result_count"] = len(get_media_main_result_rows_from_data(media_result_data))
    normalized["mention_result_count"] = len(get_media_mention_rows_from_data(media_result_data))
    return normalized


def describe_planner_scope(planner_snapshot: dict[str, Any]) -> str:
    hard_filters = planner_snapshot.get("hard_filters") if isinstance(planner_snapshot.get("hard_filters"), dict) else {}
    series = hard_filters.get("series")
    if isinstance(series, list):
        series_values = [str(item).strip() for item in series if str(item).strip()]
        if len(series_values) == 1:
            return series_values[0]
        if series_values:
            return " / ".join(series_values)
    elif str(series or "").strip():
        return str(series).strip()
    media_type = str(hard_filters.get("media_type", "") or "").strip().lower()
    category = hard_filters.get("category")
    if media_type == "anime" or category == "鍔ㄧ敾" or (isinstance(category, list) and "鍔ㄧ敾" in category):
        return "鍔ㄧ敾鐣墽"
    if media_type == "video":
        return "瑙嗛"
    if media_type == "movie":
        return "鐢靛奖"
    if media_type == "tv":
        return "鍓ч泦"
    if media_type == "book":
        return "鍥句功"
    if media_type == "music":
        return "闊充箰"
    if media_type == "game":
        return "游戏"
    entity = str((planner_snapshot.get("query_text") or "") if isinstance(planner_snapshot, dict) else "").strip()
    if entity:
        return entity
    return "筛选条件"


def _collect_working_set_match_terms(row: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for key in ("title", "retrieval_query", "matched_query"):
        text = str(row.get(key) or "").strip()
        if text:
            values.append(text)
    for entity in row.get("matched_entities") if isinstance(row.get("matched_entities"), list) else []:
        text = str(entity).strip()
        if text:
            values.append(text)
    for hit in row.get("keyword_hits") if isinstance(row.get("keyword_hits"), list) else []:
        if not isinstance(hit, dict):
            continue
        term = str(hit.get("term") or "").strip()
        if term:
            values.append(term)
    for hit in row.get("alias_hits") if isinstance(row.get("alias_hits"), list) else []:
        if isinstance(hit, dict):
            for key in ("matched_text", "canonical_name", "raw_value", "canonical"):
                text = str(hit.get(key) or "").strip()
                if text:
                    values.append(text)
        else:
            text = str(hit).strip()
            if text:
                values.append(text)
    return normalize_media_match_terms(values)


def build_media_working_set(
    tool_results: list[ToolExecution],
    *,
    planner_snapshot: dict[str, Any],
    resolved_state: dict[str, Any],
    resolved_question: str,
    raw_question: str,
    answer_shape: str,
) -> dict[str, Any]:
    rows = dedupe_media_rows(get_media_main_result_rows_from_data(_get_media_result_data(tool_results)))
    if not rows:
        return {}

    items: list[dict[str, Any]] = []
    for index, row in enumerate(rows, start=1):
        title = str(row.get("title") or "").strip()
        if not title:
            continue
        items.append(
            {
                "rank": index,
                "id": str(row.get("id") or "").strip(),
                "title": title,
                "title_key": _normalize_title_key(title),
                "media_type": str(row.get("media_type") or "").strip(),
                "date": str(row.get("date") or "").strip(),
                "rating": row.get("rating"),
                "review": str(row.get("review") or row.get("comment") or "").strip(),
                "retrieval_query": str(row.get("retrieval_query") or row.get("matched_query") or "").strip(),
                "match_terms": _collect_working_set_match_terms(row),
                "answer_layer_reasons": [
                    str(reason).strip()
                    for reason in (row.get("answer_layer_reasons") or [])
                    if str(reason).strip()
                ],
            }
        )

    if not items:
        return {}

    return {
        "kind": "media",
        "scope": describe_planner_scope(planner_snapshot),
        "query": str(resolved_question or raw_question or "").strip(),
        "media_type": str(resolved_state.get("media_type") or "").strip(),
        "filters": _normalize_filter_map(resolved_state.get("filters")),
        "date_range": list(resolved_state.get("date_range") or []),
        "sort": str(resolved_state.get("sort") or "relevance"),
        "answer_shape": str(answer_shape or ""),
        "items": items,
    }


def build_media_retrieval_response(
    tool_results: list[ToolExecution],
    *,
    request: MediaRetrievalRequest,
) -> MediaRetrievalResponse:
    media_result_data = _get_media_result_data(tool_results)
    main_results = _extract_main_rows(media_result_data)
    mention_results = _extract_mention_rows(media_result_data)
    main_identity_keys = {
        key
        for row in main_results
        if (key := _media_row_identity_key(row))
    }
    mention_results = [
        row
        for row in mention_results
        if not (key := _media_row_identity_key(row)) or key not in main_identity_keys
    ]
    external_candidates, per_item_stats = _extract_external_candidates(tool_results)
    validation = media_result_data.get("validation") if isinstance(media_result_data.get("validation"), dict) else {}
    layer_breakdown = media_result_data.get("layer_breakdown") if isinstance(media_result_data.get("layer_breakdown"), dict) else {}
    normalized_validation = dict(validation)
    normalized_validation["returned_result_count"] = len(main_results)
    normalized_validation["mention_result_count"] = len(mention_results)
    normalized_layer_breakdown = dict(layer_breakdown)
    normalized_layer_breakdown["main_count"] = len(main_results)
    normalized_layer_breakdown["mention_count"] = len(mention_results)
    if per_item_stats:
        per_item_stats.setdefault("total_rows", len(main_results))
    return MediaRetrievalResponse(
        request=request,
        main_results=main_results,
        mention_results=mention_results,
        external_candidates=external_candidates,
        validation=normalized_validation,
        layer_breakdown=normalized_layer_breakdown,
        per_item_stats=per_item_stats,
        media_result_data=media_result_data,
    )
