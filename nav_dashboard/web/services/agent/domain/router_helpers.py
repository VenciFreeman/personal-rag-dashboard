from __future__ import annotations

import json
import re
from typing import Any

from . import router_constants as constants
from .media_core import _normalize_media_filter_map
from .media_helpers import _normalize_media_entities_and_filters
from ...media.entity_resolver import resolve_media_entities as _er_resolve_media_entity
from ...media.entity_resolver import serialize_media_entity_resolution as _serialize_media_entity_resolution_owner
from ...media.media_query_adapter import project_media_filters_to_library_schema
from ...planner.domain import RouterSemanticDeps
from ...tooling.tool_option_assembly import get_lookup_mode_from_state as _get_lookup_mode_from_state
from ..infra import runtime_infra
from .router_music_helpers import (
    _extract_music_work_hints,
    _has_music_signature_filters,
    _resolve_creator_canonicals,
    _resolve_music_work_canonical_entity,
)
from .router_query_helpers import (
    _approx_tokens,
    _build_media_ranking,
    _build_media_selection,
    _build_time_constraint,
    _classifier_token_count,
    _date_window_from_state,
    _decision_requires_tmdb,
    _derive_media_lookup_mode,
    _extract_media_time_hint,
    _extract_year_from_date_range,
    _has_media_intent_cues,
    _has_media_title_marker,
    _has_router_media_surface,
    _has_router_tech_cues,
    _has_specific_media_constraints,
    _infer_media_filters,
    _infer_requested_sort,
    _infer_router_freshness,
    _is_abstract_media_concept_query,
    _is_collection_media_query,
    _is_title_anchored_personal_review_query,
    _is_title_anchored_version_compare_query,
    _looks_like_time_only_followup,
    _map_router_query_type,
    _merge_filter_values,
    _normalize_query_type,
    _normalize_timing_breakdown,
    _parse_classifier_label,
    _parse_media_date_window,
    _question_requests_media_details,
    _question_requests_personal_evaluation,
    _render_resolved_question_from_decision,
    _replace_time_window_in_query,
    _resolve_query_profile,
    _router_followup_mode_label,
    _strip_semantic_hint_filter_fields,
)
from .router_state_helpers import (
    _build_conversation_state_snapshot,
    _build_media_followup_rewrite_queries,
    _build_state_diff,
    _derive_router_followup_resolution,
    _describe_inheritance_transition,
    _find_previous_assistant_message,
    _find_previous_trace_context,
    _find_previous_user_question,
    _get_previous_assistant_answer_summary,
    _get_previous_media_working_set,
    _has_explicit_fresh_media_scope,
    _has_referential_media_scope,
    _infer_prior_question_state,
    _is_context_dependent_followup,
    _is_short_followup_surface,
    _media_scope_label,
    _merge_router_filters,
    _normalize_search_mode,
    _resolved_media_type_label,
    _resolve_previous_working_set_item_followup,
    _resolve_router_title_alias_entities,
    _should_reuse_previous_working_set,
    _state_has_media_context,
    _strip_title_alias_self_filters,
    _strip_unsupported_creator_filters_for_fresh_title_scope,
)


def _default_router_llm_chat(*args: Any, **kwargs: Any) -> str:
    return runtime_infra._llm_chat(*args, **kwargs)


def _serialize_router_decision(decision: Any) -> dict[str, Any]:
    from ...planner.planner_contracts import serialize_router_decision as serialize_router_decision_contract

    return serialize_router_decision_contract(decision)


def _serialize_media_entity_resolution(resolution: Any) -> dict[str, Any]:
    return _serialize_media_entity_resolution_owner(resolution)


def _get_query_type(
    query_classification: dict[str, Any] | None = None,
    runtime_state: Any | None = None,
) -> str:
    if runtime_state is not None:
        return str(getattr(getattr(runtime_state, "decision", None), "query_type", constants.QUERY_TYPE_GENERAL) or constants.QUERY_TYPE_GENERAL)
    current = query_classification if isinstance(query_classification, dict) else {}
    router_payload = current.get("router_decision") if isinstance(current.get("router_decision"), dict) else {}
    if router_payload:
        value = str(router_payload.get("query_type") or constants.QUERY_TYPE_GENERAL).strip()
        if value:
            return value
    return str(current.get("query_type", constants.QUERY_TYPE_GENERAL) or constants.QUERY_TYPE_GENERAL)


def _extract_json_object_from_text(raw: str) -> dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        return {}
    candidates = [text]
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        candidates.append(text[start : end + 1])
    for candidate in candidates:
        try:
            payload = json.loads(candidate)
        except Exception:
            continue
        if isinstance(payload, dict):
            return payload
    return {}


def _coerce_string_list(value: Any) -> list[str]:
    if isinstance(value, str):
        raw_items = re.split(r"[\n,，;；、]+", value)
    elif isinstance(value, list):
        raw_items = value
    else:
        raw_items = []
    items: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        if isinstance(item, dict):
            text = str(item.get("name") or item.get("title") or item.get("value") or next(iter(item.values()), "") or "").strip()
        else:
            text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        items.append(text)
    return items


def _coerce_filter_map(value: Any) -> dict[str, list[str]]:
    if not isinstance(value, dict):
        return {}
    normalized: dict[str, list[str]] = {}
    ranking_like_fields = {"rating", "score", "review", "sort", "ranking"}
    for key, raw in value.items():
        field = str(key or "").strip()
        if not field or field.lower() in ranking_like_fields:
            continue
        values = _coerce_string_list(raw)
        if values:
            normalized[field] = values
    return _normalize_media_filter_map(normalized)


def build_planner_router_semantic_deps() -> RouterSemanticDeps:
    return RouterSemanticDeps(
        question_requests_personal_evaluation=lambda question: _question_requests_personal_evaluation(question),
        question_requests_media_details=lambda question: _question_requests_media_details(question),
        is_collection_media_query=_is_collection_media_query,
    )


_PLANNER_ROUTER_SEMANTIC_DEPS = build_planner_router_semantic_deps()
