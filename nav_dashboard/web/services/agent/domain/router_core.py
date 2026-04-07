from __future__ import annotations

import json
import re
import time as _time
from typing import Any

from ....prompts.agent_router_prompts import (
    build_media_query_classification_prompt,
    build_media_query_rewrite_prompt,
)
from ...media.entity_resolver import resolve_media_entities as _er_resolve_media_entity
from ...media.entity_resolver import serialize_media_entity_resolution as _serialize_media_entity_resolution
from ...media.media_query_adapter import merge_followup_filters, project_media_filters_to_library_schema
from ...planner import planner_contracts
from ...planner.context import (
    build_conversation_state_snapshot_from_decision as planner_build_conversation_state_snapshot_from_decision,
    build_planner_snapshot_from_decision as planner_build_planner_snapshot_from_decision,
    build_resolved_query_state_from_decision as planner_build_resolved_query_state_from_decision,
)
from ...planner.domain import apply_router_semantic_contract, resolve_router_metadata_anchors
from .media_core import _normalize_media_filter_map, _normalize_media_title_for_match, _sanitize_media_filters
from .media_helpers import (
    _extract_creator_from_collection_query,
    _extract_media_entities,
    _is_creator_collection_media_query,
    _looks_like_generic_media_scope,
    _normalize_media_entities_and_filters,
    _resolve_library_aliases,
)
from .router_constants import (
    ALLOWED_RANKING_MODES,
    CLASSIFIER_LABEL_MEDIA,
    CLASSIFIER_LABEL_OTHER,
    CLASSIFIER_LABEL_TECH,
    QUERY_TYPE_GENERAL,
    QUERY_TYPE_MEDIA,
    QUERY_TYPE_MIXED,
    QUERY_TYPE_TECH,
    ROUTER_CHAT_CUES,
    ROUTER_CONFIDENCE_HIGH,
    ROUTER_CONFIDENCE_MEDIUM,
    TECH_QUERY_DOC_SIM_THRESHOLD,
    TOOL_EXPAND_MEDIAWIKI_CONCEPT,
    TOOL_SEARCH_TMDB,
    TOOL_SEARCH_WEB,
)
from .router_helpers import (
    _PLANNER_ROUTER_SEMANTIC_DEPS,
    _approx_tokens,
    _build_media_ranking,
    _build_media_selection,
    _build_media_followup_rewrite_queries,
    _build_time_constraint,
    _build_conversation_state_snapshot,
    _build_state_diff,
    _classifier_token_count,
    _coerce_filter_map,
    _coerce_string_list,
    _default_router_llm_chat,
    _date_window_from_state,
    _describe_inheritance_transition,
    _decision_requires_tmdb,
    _derive_media_lookup_mode,
    _derive_router_followup_resolution,
    _extract_json_object_from_text,
    _extract_music_work_hints,
    _extract_year_from_date_range,
    _find_previous_trace_context,
    _find_previous_user_question,
    _get_previous_assistant_answer_summary,
    _get_previous_media_working_set,
    _get_lookup_mode_from_state,
    _get_query_type,
    _has_media_intent_cues,
    _has_media_title_marker,
    _has_music_signature_filters,
    _has_router_media_surface,
    _has_router_tech_cues,
    _has_specific_media_constraints,
    _infer_prior_question_state,
    _infer_media_filters,
    _infer_requested_sort,
    _infer_router_freshness,
    _is_abstract_media_concept_query,
    _is_collection_media_query,
    _is_context_dependent_followup,
    _is_title_anchored_personal_review_query,
    _is_title_anchored_version_compare_query,
    _map_router_query_type,
    _media_scope_label,
    _merge_filter_values,
    _merge_router_filters,
    _normalize_query_type,
    _normalize_search_mode,
    _normalize_timing_breakdown,
    _parse_classifier_label,
    _parse_media_date_window,
    _question_requests_media_details,
    _question_requests_personal_evaluation,
    _render_resolved_question_from_decision,
    _replace_time_window_in_query,
    _resolve_router_title_alias_entities,
    _resolve_music_work_canonical_entity,
    _resolve_previous_working_set_item_followup,
    _resolve_query_profile,
    _resolved_media_type_label,
    _router_followup_mode_label,
    _serialize_router_decision,
    _should_reuse_previous_working_set,
    _state_has_media_context,
    _strip_title_alias_self_filters,
    _strip_unsupported_creator_filters_for_fresh_title_scope,
    _strip_semantic_hint_filter_fields,
)
from .router_types import AgentRuntimeState, PlannedToolCall, RouterContextResolution, RouterDecision, RouterDeps, ToolExecution


def build_default_router_deps() -> RouterDeps:
    return RouterDeps(
        llm_chat=_default_router_llm_chat,
        find_previous_trace_context=_find_previous_trace_context,
        apply_router_semantic_repairs=_apply_router_semantic_repairs,
        classify_media_query_with_llm=_classify_media_query_with_llm,
        rewrite_tool_queries_with_llm=_rewrite_tool_queries_with_llm,
        resolve_media_entity=_er_resolve_media_entity,
        resolve_library_aliases=_resolve_library_aliases,
        planner_router_semantic_deps=_PLANNER_ROUTER_SEMANTIC_DEPS,
        perf_counter=_time.perf_counter,
        normalize_timing_breakdown=_normalize_timing_breakdown,
    )


def resolve_router_deps(deps: RouterDeps | None = None) -> RouterDeps:
    if deps is None:
        return build_default_router_deps()
    defaults = build_default_router_deps()
    return RouterDeps(
        llm_chat=deps.llm_chat,
        find_previous_trace_context=deps.find_previous_trace_context,
        apply_router_semantic_repairs=deps.apply_router_semantic_repairs,
        resolve_library_aliases=deps.resolve_library_aliases,
        planner_router_semantic_deps=deps.planner_router_semantic_deps,
        perf_counter=deps.perf_counter,
        normalize_timing_breakdown=deps.normalize_timing_breakdown,
        classify_media_query_with_llm=deps.classify_media_query_with_llm or defaults.classify_media_query_with_llm,
        rewrite_tool_queries_with_llm=deps.rewrite_tool_queries_with_llm or defaults.rewrite_tool_queries_with_llm,
        resolve_media_entity=deps.resolve_media_entity or defaults.resolve_media_entity,
    )


def _sanitize_ranking_mode(mode: str) -> str:
    normalized = str(mode or "relevance").strip().lower()
    return normalized if normalized in ALLOWED_RANKING_MODES else "relevance"


def _looks_like_generic_tool_query(text: str) -> bool:
    value = str(text or "").strip()
    if not value:
        return True
    compact = re.sub(r"\s+", "", value)
    generic_cues = (
        "请概述一下内容",
        "概述一下内容",
        "介绍一下",
        "详细介绍",
        "请详细介绍",
        "那个怎么样",
        "这个怎么样",
        "请概述",
        "请展开",
    )
    return compact in generic_cues


def _sanitize_tool_queries(candidate_queries: dict[str, str], fallback_queries: dict[str, str]) -> dict[str, str]:
    sanitized: dict[str, str] = {}
    for key in ("media_query", "doc_query", "tmdb_query", "web_query"):
        candidate = str(candidate_queries.get(key) or "").strip()
        fallback = str(fallback_queries.get(key) or "").strip()
        if candidate and not _looks_like_generic_tool_query(candidate):
            sanitized[key] = candidate
        elif fallback:
            sanitized[key] = fallback
    return sanitized


def _candidate_tool_query_bleeds_previous_entity(
    candidate: str,
    *,
    question: str,
    decision: RouterDecision,
    previous_state: dict[str, Any],
) -> bool:
    if decision.followup_mode != "none":
        return False
    evidence = decision.evidence if isinstance(decision.evidence, dict) else {}
    if evidence.get("working_set_followup") or evidence.get("working_set_item_followup"):
        return False
    normalized_candidate = _normalize_media_title_for_match(candidate)
    normalized_question = _normalize_media_title_for_match(question)
    if not normalized_candidate or not normalized_question:
        return False
    previous_terms = [
        _normalize_media_title_for_match(item)
        for item in [
            str(previous_state.get("entity") or "").strip(),
            *[str(item).strip() for item in list(previous_state.get("entities") or []) if str(item).strip()],
        ]
        if _normalize_media_title_for_match(item)
    ]
    if not previous_terms:
        return False
    for previous_term in previous_terms:
        if previous_term in normalized_candidate and previous_term not in normalized_question:
            return True
    return False


def _prune_stale_llm_tool_queries(
    candidate_queries: dict[str, str],
    *,
    question: str,
    decision: RouterDecision,
    previous_state: dict[str, Any],
) -> dict[str, str]:
    pruned: dict[str, str] = {}
    for key, value in dict(candidate_queries or {}).items():
        clean_key = str(key).strip()
        clean_value = str(value).strip()
        if not clean_key or not clean_value:
            continue
        if _candidate_tool_query_bleeds_previous_entity(
            clean_value,
            question=question,
            decision=decision,
            previous_state=previous_state,
        ):
            continue
        pruned[clean_key] = clean_value
    return pruned


def _should_skip_llm_tool_rewrite(question: str, decision: RouterDecision) -> bool:
    if decision.domain != "media":
        return True
    evidence = decision.evidence if isinstance(decision.evidence, dict) else {}
    alias_resolution = evidence.get("router_alias_resolution") if isinstance(evidence.get("router_alias_resolution"), dict) else {}
    has_alias_anchor = bool(list(alias_resolution.get("canonical_terms") or []))
    if evidence.get("title_anchored_version_compare_query") and decision.entities:
        return True
    if decision.lookup_mode == "entity_lookup" and decision.entities and (
        evidence.get("media_title_marked")
        or _question_requests_media_details(question)
        or _question_requests_personal_evaluation(question)
    ):
        return True
    if decision.query_class in {
        planner_contracts.ROUTER_QUERY_CLASS_PERSONAL_MEDIA_REVIEW_COLLECTION,
        planner_contracts.ROUTER_QUERY_CLASS_MUSIC_WORK_VERSIONS_COMPARE,
    } and decision.entities and has_alias_anchor:
        return True
    if (
        decision.lookup_mode == "filter_search"
        and decision.followup_mode == "none"
        and not decision.entities
        and str(decision.subject_scope or "") == planner_contracts.ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD
        and (
            len([str(item).strip() for item in list(decision.date_range or []) if str(item).strip()]) == 2
            or _has_specific_media_constraints(decision.filters)
            or str(decision.answer_shape or "") in {
                planner_contracts.ROUTER_ANSWER_SHAPE_LIST_PLUS_EXPAND,
                planner_contracts.ROUTER_ANSWER_SHAPE_COMPARE,
            }
        )
    ):
        return True
    return False


def _extract_title_alias_canonical_terms(alias_resolution: dict[str, Any] | None) -> list[str]:
    if not isinstance(alias_resolution, dict):
        return []
    entries = alias_resolution.get("entries") if isinstance(alias_resolution.get("entries"), list) else []
    canonical_terms: list[str] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        field_type = str(entry.get("field_type") or entry.get("field") or "").strip().lower()
        if field_type != "title":
            continue
        canonical = str(entry.get("canonical_name") or "").strip()
        if canonical and canonical not in canonical_terms:
            canonical_terms.append(canonical)
    if canonical_terms:
        return canonical_terms
    return [str(item).strip() for item in list(alias_resolution.get("canonical_terms") or []) if str(item).strip()]


def _build_tool_grade_rewritten_queries(
    question: str,
    decision: RouterDecision,
    previous_state: dict[str, Any],
    llm_queries: dict[str, str] | None = None,
) -> dict[str, str]:
    deterministic = _build_media_followup_rewrite_queries(
        question,
        previous_state,
        followup_mode=decision.followup_mode,
        entities=list(decision.entities),
        filters=_normalize_media_filter_map(decision.filters),
        media_type=str(decision.media_type or ""),
    ) if decision.domain == "media" else {}
    if decision.domain != "media":
        return {
            "doc_query": str(question or "").strip(),
            "web_query": str(question or "").strip(),
        }
    creator_evidence = decision.evidence if isinstance(decision.evidence, dict) else {}
    if creator_evidence.get("creator_collection_query"):
        creator_info = creator_evidence.get("creator_resolution") or {}
        creator_canonical = str(creator_info.get("canonical") or "").strip()
        if creator_canonical:
            deterministic["media_query"] = creator_canonical
    entity_phrase = "、".join(str(item).strip() for item in list(decision.entities or []) if str(item).strip())
    candidate_queries = _prune_stale_llm_tool_queries(
        dict(llm_queries or {}),
        question=question,
        decision=decision,
        previous_state=previous_state,
    )
    if len(list(decision.entities or [])) > 1:
        candidate_queries.pop("media_query", None)
        candidate_queries.pop("web_query", None)
    if decision.entities and (not deterministic.get("media_query") or _looks_like_generic_tool_query(deterministic.get("media_query"))):
        entity = entity_phrase or decision.entities[0]
        if _question_requests_personal_evaluation(question):
            deterministic["media_query"] = f"{entity} 个人评分与短评"
        elif _question_requests_media_details(question):
            deterministic["media_query"] = f"{entity} 条目细节"
        else:
            deterministic["media_query"] = entity
    if not deterministic.get("tmdb_query") and decision.entities:
        deterministic["tmdb_query"] = decision.entities[0]
    if not deterministic.get("doc_query"):
        deterministic["doc_query"] = str(question or "").strip()
    if not deterministic.get("web_query"):
        deterministic["web_query"] = deterministic.get("media_query") or str(question or "").strip()
    return _sanitize_tool_queries(candidate_queries, deterministic)


def _classify_media_query_with_llm(
    query: str,
    quota_state: dict[str, Any],
    *,
    previous_state: dict[str, Any] | None = None,
    history: list[dict[str, str]] | None = None,
    timing_sink: dict[str, Any] | None = None,
    deps: RouterDeps | None = None,
) -> dict[str, Any]:
    resolved_deps = resolve_router_deps(deps)
    previous = dict(previous_state or {})
    prompt_build_t0 = resolved_deps.perf_counter()
    prior_scope = {
        "lookup_mode": str(previous.get("lookup_mode") or ""),
        "media_type": str(previous.get("media_type") or ""),
        "entity": str(previous.get("entity") or ""),
        "entities": [str(item).strip() for item in (previous.get("entities") or []) if str(item).strip()],
        "filters": _normalize_media_filter_map(previous.get("filters")),
        "time_constraint": dict(previous.get("time_constraint") or {}),
        "ranking": dict(previous.get("ranking") or {}),
    }
    previous_answer_summary = _get_previous_assistant_answer_summary(history or [])
    prompt = build_media_query_classification_prompt(
        prior_scope=prior_scope,
        previous_answer_summary=previous_answer_summary or "N/A",
        query=query,
    )
    prompt_build_seconds = resolved_deps.perf_counter() - prompt_build_t0
    llm_request_seconds = 0.0
    llm_response_seconds = 0.0
    try:
        llm_request_t0 = resolved_deps.perf_counter()
        raw = resolved_deps.llm_chat(
            messages=[{"role": "user", "content": prompt}],
            backend="local",
            quota_state=quota_state,
            count_quota=False,
            max_tokens=400,
        )
        llm_request_seconds = resolved_deps.perf_counter() - llm_request_t0
        llm_response_t0 = resolved_deps.perf_counter()
        payload = _extract_json_object_from_text(raw)
        parsed_label = _parse_classifier_label(str(payload.get("label") or raw or ""))
        rewritten_queries = {
            str(key): str(value).strip()
            for key, value in ((payload.get("rewritten_queries") or {}).items() if isinstance(payload.get("rewritten_queries"), dict) else [])
            if str(key).strip() and str(value).strip()
        }
        llm_response_seconds = resolved_deps.perf_counter() - llm_response_t0
        return {
            "available": True,
            "answer": str(raw or "").strip(),
            "label": parsed_label,
            "is_media": parsed_label == CLASSIFIER_LABEL_MEDIA,
            "parsed": payload,
            "domain": str(payload.get("domain") or "general").strip() or "general",
            "lookup_mode": str(payload.get("lookup_mode") or "general_lookup").strip() or "general_lookup",
            "entities": _coerce_string_list(payload.get("entities")),
            "filters": _coerce_filter_map(payload.get("filters")),
            "time_window": dict(payload.get("time_window") or {}),
            "ranking": dict(payload.get("ranking") or {}),
            "followup_target": str(payload.get("followup_target") or "").strip(),
            "needs_comparison": bool(payload.get("needs_comparison")),
            "needs_explanation": bool(payload.get("needs_explanation")),
            "confidence": float(payload.get("confidence") or 0.0),
            "rewritten_queries": rewritten_queries,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "available": False,
            "answer": str(exc),
            "label": CLASSIFIER_LABEL_OTHER,
            "is_media": False,
            "parsed": None,
            "domain": "general",
            "lookup_mode": "general_lookup",
            "entities": [],
            "filters": {},
            "time_window": {},
            "ranking": {},
            "followup_target": "",
            "needs_comparison": False,
            "needs_explanation": False,
            "confidence": 0.0,
            "rewritten_queries": {},
        }
    finally:
        if timing_sink is not None:
            timing_sink.update(
                {
                    "router_prompt_build_seconds": prompt_build_seconds,
                    "router_llm_request_seconds": llm_request_seconds,
                    "router_llm_response_seconds": llm_response_seconds,
                }
            )


def _rewrite_tool_queries_with_llm(
    question: str,
    decision: RouterDecision,
    previous_state: dict[str, Any],
    quota_state: dict[str, Any],
    timing_sink: dict[str, Any] | None = None,
    deps: RouterDeps | None = None,
) -> dict[str, str]:
    resolved_deps = resolve_router_deps(deps)
    if decision.domain != "media":
        return {}
    if _should_skip_llm_tool_rewrite(question, decision):
        return {}
    prompt_build_t0 = resolved_deps.perf_counter()
    prior_scope = {
        "lookup_mode": str(previous_state.get("lookup_mode") or ""),
        "media_type": str(previous_state.get("media_type") or ""),
        "entity": str(previous_state.get("entity") or ""),
        "entities": [str(item).strip() for item in (previous_state.get("entities") or []) if str(item).strip()],
        "filters": _normalize_media_filter_map(previous_state.get("filters")),
        "time_constraint": dict(previous_state.get("time_constraint") or {}),
        "ranking": dict(previous_state.get("ranking") or {}),
    }
    decision_payload = {
        "domain": decision.domain,
        "lookup_mode": decision.lookup_mode,
        "entities": list(decision.entities),
        "filters": _normalize_media_filter_map(decision.filters),
        "selection": _normalize_media_filter_map(decision.selection),
        "time_constraint": dict(decision.time_constraint),
        "ranking": dict(decision.ranking),
        "followup_mode": decision.followup_mode,
        "media_type": decision.media_type,
    }
    prompt = build_media_query_rewrite_prompt(
        prior_scope=prior_scope,
        decision_payload=decision_payload,
        question=question,
    )
    prompt_build_seconds = resolved_deps.perf_counter() - prompt_build_t0
    llm_request_seconds = 0.0
    llm_response_seconds = 0.0
    try:
        llm_request_t0 = resolved_deps.perf_counter()
        raw = resolved_deps.llm_chat(
            messages=[{"role": "user", "content": prompt}],
            backend="local",
            quota_state=quota_state,
            count_quota=False,
            max_tokens=400,
        )
        llm_request_seconds = resolved_deps.perf_counter() - llm_request_t0
        llm_response_t0 = resolved_deps.perf_counter()
        payload = _extract_json_object_from_text(raw)
        rewritten = {
            str(key): str(value).strip()
            for key, value in payload.items()
            if str(key).strip() in {"media_query", "doc_query", "tmdb_query", "web_query"} and str(value).strip()
        }
        llm_response_seconds = resolved_deps.perf_counter() - llm_response_t0
        return rewritten
    except Exception:
        return {}
    finally:
        if timing_sink is not None:
            timing_sink.update(
                {
                    "router_rewrite_prompt_build_seconds": prompt_build_seconds,
                    "router_rewrite_llm_request_seconds": llm_request_seconds,
                    "router_rewrite_llm_response_seconds": llm_response_seconds,
                }
            )


def _apply_router_semantic_repairs(
    question: str,
    decision: RouterDecision,
    previous_state: dict[str, Any],
    *,
    deps: RouterDeps | None = None,
) -> RouterDecision:
    if deps is not None:
        apply_repairs = resolve_router_deps(deps).apply_router_semantic_repairs
        if apply_repairs is not _apply_router_semantic_repairs:
            return apply_repairs(question, decision, previous_state)

    repairs: list[str] = []
    ranking_mode = _sanitize_ranking_mode(str((decision.ranking or {}).get("mode") or decision.sort or "relevance"))
    requested_sort = _infer_requested_sort(question)
    collection_query = _is_collection_media_query(question)
    creator_collection_query = _is_creator_collection_media_query(question)
    if ranking_mode == "relevance" and requested_sort != "relevance":
        ranking_mode = requested_sort
        repairs.append("ranking:explicit_sort")
    if ranking_mode != "relevance" and not (
        collection_query or decision.lookup_mode == "filter_search" or _question_requests_personal_evaluation(question)
    ):
        ranking_mode = "relevance"
        repairs.append("ranking:normalized_to_relevance")
    if ranking_mode != str((decision.ranking or {}).get("mode") or "relevance"):
        decision.ranking = {**dict(decision.ranking or {}), "mode": ranking_mode, "source": "semantic_repair"}
        decision.sort = ranking_mode

    ev = dict(decision.evidence or {})
    alias_resolution = ev.get("router_alias_resolution") if isinstance(ev.get("router_alias_resolution"), dict) else {}
    metadata_anchor_resolution = ev.get("router_metadata_anchor_resolution") if isinstance(ev.get("router_metadata_anchor_resolution"), dict) else {}
    canonical_terms = [str(item).strip() for item in list(alias_resolution.get("canonical_terms") or []) if str(item).strip()]
    title_alias_terms = _extract_title_alias_canonical_terms(alias_resolution)
    creator_anchor_entries = [
        entry
        for entry in list(metadata_anchor_resolution.get("entries") or [])
        if isinstance(entry, dict) and str(entry.get("field_type") or entry.get("field") or "").strip().lower() in {"creator", "author"}
    ]
    ranking_compare_cues = ("最好", "最喜欢", "评分最高", "评价最高", "哪张", "哪部", "哪本", "哪首", "分层", "层次")
    version_cues = ("版本", "演绎", "录音", "评价", "咋样", "如何", "比较", "对比")
    title_anchored_compare = (
        decision.query_class == planner_contracts.ROUTER_QUERY_CLASS_PERSONAL_MEDIA_REVIEW_COLLECTION
        and any(cue in str(question or "") for cue in version_cues)
    )
    if not decision.entities and len(canonical_terms) == 1 and (ev.get("media_title_marked") or title_anchored_compare):
        decision.entities = [canonical_terms[0]]
        if not str(decision.followup_target or "").strip():
            decision.followup_target = canonical_terms[0]
        repairs.append("alias_entity:canonicalized")

    if (
        decision.domain == "media"
        and decision.followup_mode == "none"
        and title_alias_terms
        and (
            _question_requests_personal_evaluation(question)
            or bool(ev.get("title_anchored_personal_review_query"))
            or bool(ev.get("alias_title_collection_query"))
            or str(decision.lookup_mode or "") == "entity_lookup"
        )
    ):
        decision.entities = list(title_alias_terms)
        decision.lookup_mode = "entity_lookup"
        drop_broad_category = _question_requests_personal_evaluation(question)
        decision.filters = {
            field: values
            for field, values in _normalize_media_filter_map(decision.filters).items()
            if field != "author" and not (drop_broad_category and field == "category")
        }
        decision.selection = {
            field: values
            for field, values in _normalize_media_filter_map(decision.selection).items()
            if field != "author" and not (drop_broad_category and field == "category")
        }
        if not str(decision.followup_target or "").strip():
            decision.followup_target = title_alias_terms[0]
        repairs.append("title_alias_personal_review:entity_lookup")
        repairs.append("title_alias_personal_review:drop_author_filter")
        if drop_broad_category:
            repairs.append("title_alias_personal_review:drop_category_filter")

    if (
        decision.domain == "media"
        and decision.followup_mode == "none"
        and _question_requests_personal_evaluation(question)
        and bool(ev.get("title_anchored_personal_review_query"))
        and decision.entities
    ):
        normalized_filters = _normalize_media_filter_map(decision.filters)
        normalized_selection = _normalize_media_filter_map(decision.selection)
        if "category" in normalized_filters or "category" in normalized_selection:
            decision.filters = {field: values for field, values in normalized_filters.items() if field != "category"}
            decision.selection = {field: values for field, values in normalized_selection.items() if field != "category"}
            repairs.append("title_anchored_personal_review:drop_category_filter")

    if (
        decision.domain == "media"
        and decision.followup_mode == "none"
        and not decision.entities
        and creator_anchor_entries
        and any(token in str(question or "") for token in ("我", "我的", "自己", "记录"))
        and any(cue in str(question or "") for cue in ranking_compare_cues)
    ):
        creator_names = []
        creator_media_types = []
        seen_creator_names: set[str] = set()
        for entry in creator_anchor_entries:
            canonical_name = str(entry.get("canonical_name") or "").strip()
            if canonical_name and canonical_name.casefold() not in seen_creator_names:
                seen_creator_names.add(canonical_name.casefold())
                creator_names.append(canonical_name)
            media_type_value = str(entry.get("media_type") or "").strip().lower()
            if media_type_value and media_type_value not in creator_media_types:
                creator_media_types.append(media_type_value)
        if creator_names:
            merged_filters = _sanitize_media_filters(decision.filters)
            _merge_filter_values(merged_filters, "author", creator_names)
            if creator_media_types and not merged_filters.get("media_type"):
                merged_filters["media_type"] = creator_media_types
            decision.filters = merged_filters
            decision.lookup_mode = "filter_search"
            if creator_media_types:
                decision.media_type = creator_media_types[0]
            ev = dict(decision.evidence or {})
            ev["creator_collection_query"] = True
            ev["creator_resolution"] = {
                "canonical": creator_names[0],
                "media_type_hint": creator_media_types[0] if creator_media_types else "",
                "confidence": 0.2,
                "match_kind": "metadata_anchor_fallback",
            }
            decision.evidence = ev
            decision.arbitration = "creator_collection_wins"
            decision.reasons = [reason for reason in decision.reasons if not reason.startswith("arbitration:")] + ["arbitration:creator_collection_wins"]
            repairs.append("creator:resolved_from_metadata_anchor")

    music_work_hints = _extract_music_work_hints(question, decision.filters)
    filter_music_signature = _has_music_signature_filters(decision.filters)
    if music_work_hints["composer_hints"] and (music_work_hints["work_signature"] or filter_music_signature):
        if decision.lookup_mode == "general_lookup":
            decision.lookup_mode = "filter_search"
            repairs.append("lookup_mode:music_general_to_filter")
        comparison_cues = ("比较", "对比", "版本", "演绎", "评价", "咋样", "如何")
        if decision.needs_comparison or any(cue in str(question or "") for cue in comparison_cues):
            decision.needs_comparison = True
        if decision.intent != "media_lookup":
            decision.intent = "media_lookup"
            repairs.append("intent:mixed_to_media_for_music_work_compare")
        if decision.domain != "media":
            decision.domain = "media"
            repairs.append("domain:force_media_for_music_work_compare")
        if decision.needs_doc_rag:
            decision.needs_doc_rag = False
            repairs.append("needs_doc_rag:disabled_for_music_work_compare")
        decision.needs_media_db = True
        decision.arbitration = "music_work_compare_wins"
        reasons_wo_arb = [r for r in decision.reasons if not str(r).startswith("arbitration:")]
        decision.reasons = reasons_wo_arb + ["arbitration:music_work_compare_wins"]
        ev = dict(decision.evidence or {})
        ev["music_work_versions_compare"] = True
        ev["music_work_hints"] = music_work_hints
        decision.evidence = ev
        canonical_work_entity = _resolve_music_work_canonical_entity(question, music_work_hints)
        if canonical_work_entity:
            decision.entities = [canonical_work_entity]
            decision.followup_target = canonical_work_entity
            repairs.append("music_work_entity:canonicalized")
        repairs.append("music_work_signature:enforced")

    if (
        decision.domain == "media"
        and decision.lookup_mode in ("filter_search", "general_lookup")
        and decision.followup_mode == "none"
        and not decision.entities
        and (collection_query or creator_collection_query)
    ):
        creator_res = _extract_creator_from_collection_query(question)
        if creator_res:
            merged_filters = _sanitize_media_filters(decision.filters)
            _merge_filter_values(merged_filters, "author", [creator_res.canonical])
            decision.filters = merged_filters
            decision.lookup_mode = "filter_search"
            if creator_res.media_type_hint == "video":
                decision.needs_external_media_db = True
            ev = dict(decision.evidence or {})
            ev["creator_collection_query"] = True
            ev["creator_resolution"] = {
                "canonical": creator_res.canonical,
                "media_type_hint": creator_res.media_type_hint,
                "confidence": round(float(creator_res.confidence), 4),
                "match_kind": str(creator_res.match_kind),
            }
            decision.evidence = ev
            if "arbitration:llm_media_weak_general" not in " ".join(decision.reasons):
                decision.arbitration = "creator_collection_wins"
                existing = [r for r in decision.reasons if not r.startswith("arbitration:")]
                decision.reasons = existing + ["arbitration:creator_collection_wins"]
            repairs.append("creator:resolved_from_collection_query")

    if decision.domain == "media" and decision.followup_mode == "inherit_entity" and not decision.entities:
        previous_entity = str(previous_state.get("entity") or "").strip()
        if previous_entity:
            decision.entities = [previous_entity]
            repairs.append("entity:inherited_from_previous")

    if decision.domain == "media" and decision.lookup_mode == "entity_lookup" and not decision.entities:
        if decision.filters or len(decision.date_range) == 2 or decision.followup_mode in {"inherit_filters", "inherit_timerange"}:
            decision.lookup_mode = "filter_search"
            repairs.append("lookup_mode:entity_to_filter")
        else:
            decision.lookup_mode = "general_lookup"
            repairs.append("lookup_mode:entity_to_general")

    if decision.domain == "media" and decision.lookup_mode == "general_lookup" and decision.entities and not (
        decision.filters or len(decision.date_range) == 2 or decision.followup_mode in {"inherit_filters", "inherit_timerange"}
    ):
        decision.lookup_mode = "entity_lookup"
        repairs.append("lookup_mode:general_to_entity")

    if decision.domain == "media" and decision.lookup_mode == "filter_search" and not (
        decision.selection
        or decision.filters
        or len(decision.date_range) == 2
        or decision.followup_mode in {"inherit_filters", "inherit_timerange"}
        or collection_query
    ):
        if decision.entities:
            decision.lookup_mode = "entity_lookup"
            repairs.append("lookup_mode:filter_to_entity")
        else:
            decision.lookup_mode = "general_lookup"
            repairs.append("lookup_mode:filter_to_general")

    preserve_filter_search_entities = bool(decision.entities) and (
        decision.query_class in {
            planner_contracts.ROUTER_QUERY_CLASS_PERSONAL_MEDIA_REVIEW_COLLECTION,
            planner_contracts.ROUTER_QUERY_CLASS_MUSIC_WORK_VERSIONS_COMPARE,
        }
        or bool((decision.evidence or {}).get("media_title_marked"))
    )
    if decision.lookup_mode == "filter_search" and not preserve_filter_search_entities:
        decision.entities = []
    if decision.lookup_mode == "entity_lookup" and decision.entities:
        decision.followup_target = decision.entities[0]

    decision.selection = _build_media_selection(_normalize_media_filter_map(decision.filters), str(decision.media_type or ""))
    if repairs:
        reasons = [str(item) for item in decision.reasons if str(item).strip()]
        for repair in repairs:
            marker = f"repair:{repair}"
            if marker not in reasons:
                reasons.append(marker)
        decision.reasons = reasons
        evidence = dict(decision.evidence or {})
        evidence["semantic_repairs"] = repairs
        decision.evidence = evidence
    return decision


def _render_trace_resolved_question(decision: RouterDecision, previous_state: dict[str, Any]) -> str:
    current = str(decision.raw_question or "").strip()
    previous_question = str(previous_state.get("question") or "").strip()
    previous_entity = str(previous_state.get("entity") or "").strip()
    entity = decision.entities[0] if decision.entities else previous_entity
    if decision.followup_mode == "inherit_timerange" and previous_question:
        return _replace_time_window_in_query(previous_question, current)
    if decision.followup_mode == "inherit_entity" and entity and entity not in current:
        return f"{entity} {current}".strip()
    return current


def _build_resolved_query_state_from_decision(decision: RouterDecision) -> dict[str, Any]:
    return planner_build_resolved_query_state_from_decision(
        decision,
        normalize_media_filter_map=_normalize_media_filter_map,
        router_followup_mode_label=_router_followup_mode_label,
    )


def _build_conversation_state_snapshot_from_decision(
    question: str,
    decision: RouterDecision,
    resolved_query_state: dict[str, Any],
) -> dict[str, Any]:
    return planner_build_conversation_state_snapshot_from_decision(
        question,
        decision,
        resolved_query_state,
        get_lookup_mode_from_state=_get_lookup_mode_from_state,
        normalize_media_filter_map=_normalize_media_filter_map,
    )


def _build_planner_snapshot_from_decision(
    decision: RouterDecision,
    resolved_question: str,
    resolved_query_state: dict[str, Any],
    planned_tools: list[PlannedToolCall],
) -> dict[str, Any]:
    return planner_build_planner_snapshot_from_decision(
        decision,
        resolved_question,
        resolved_query_state,
        planned_tools,
        get_lookup_mode_from_state=_get_lookup_mode_from_state,
        normalize_media_filter_map=_normalize_media_filter_map,
        normalize_timing_breakdown=_normalize_timing_breakdown,
    )


def _resolve_router_context(
    original_question: str,
    history: list[dict[str, str]],
    decision: RouterDecision,
    previous_state: dict[str, Any],
    planned_tools: list[PlannedToolCall],
    *,
    deps: RouterDeps | None = None,
) -> RouterContextResolution:
    resolved_deps = resolve_router_deps(deps)
    resolved_question = _render_trace_resolved_question(decision, previous_state)
    resolved_query_state = _build_resolved_query_state_from_decision(decision)

    previous_trace = resolved_deps.find_previous_trace_context(history)
    previous_trace_state = previous_trace.get("conversation_state_after") if isinstance(previous_trace.get("conversation_state_after"), dict) else {}
    previous_trace_resolved_question = ""
    if isinstance(previous_trace.get("query_understanding"), dict):
        previous_trace_resolved_question = str(previous_trace.get("query_understanding", {}).get("resolved_question", "") or "").strip()
    previous_question = _find_previous_user_question(original_question, history)
    conversation_state_before = dict(previous_trace_state) if previous_trace_state else (
        _build_conversation_state_snapshot(previous_question, resolved_query_state=_infer_prior_question_state(previous_question, history))
        if previous_question else {}
    )
    conversation_state_after = _build_conversation_state_snapshot_from_decision(
        resolved_question,
        decision,
        resolved_query_state,
    )
    has_previous_context = bool(previous_question) or bool(previous_trace_state) or bool(previous_trace_resolved_question)
    detected_followup = has_previous_context and (
        bool(resolved_query_state.get("carry_over_from_previous_turn"))
        or _is_context_dependent_followup(original_question)
    )
    inheritance_applied = {
        "lookup_mode": _describe_inheritance_transition(
            conversation_state_before.get("lookup_mode"),
            conversation_state_after.get("lookup_mode"),
            detected_followup,
        ),
        "media_type": _describe_inheritance_transition(
            conversation_state_before.get("media_type"),
            conversation_state_after.get("media_type"),
            detected_followup,
        ),
        "filters": _describe_inheritance_transition(
            conversation_state_before.get("filters"),
            conversation_state_after.get("filters"),
            detected_followup,
        ),
        "date_range": _describe_inheritance_transition(
            conversation_state_before.get("date_range"),
            conversation_state_after.get("date_range"),
            detected_followup,
        ),
        "sort": _describe_inheritance_transition(
            conversation_state_before.get("sort"),
            conversation_state_after.get("sort"),
            detected_followup,
        ),
        "entity": _describe_inheritance_transition(
            conversation_state_before.get("entity"),
            conversation_state_after.get("entity"),
            detected_followup,
        ),
    }
    planner_snapshot = _build_planner_snapshot_from_decision(decision, resolved_question, resolved_query_state, planned_tools)
    return RouterContextResolution(
        resolved_question=resolved_question,
        resolved_query_state=resolved_query_state,
        conversation_state_before=conversation_state_before,
        conversation_state_after=conversation_state_after,
        detected_followup=detected_followup,
        inheritance_applied=inheritance_applied,
        state_diff=_build_state_diff(conversation_state_before, conversation_state_after),
        planner_snapshot=planner_snapshot,
        planning_timing_breakdown=dict(planner_snapshot.get("timing_breakdown") or {}),
    )


def _router_decision_to_query_classification(
    decision: RouterDecision,
    llm_media: dict[str, Any],
    previous_state: dict[str, Any],
    query_profile: dict[str, Any],
) -> dict[str, Any]:
    media_title_marked = bool(decision.evidence.get("media_title_marked"))
    media_intent_cues = bool(decision.evidence.get("media_intent_cues"))
    tech_cues = bool(decision.evidence.get("tech_cues"))
    router_alias_resolution = decision.evidence.get("router_alias_resolution") if isinstance(decision.evidence.get("router_alias_resolution"), dict) else {}
    router_entity_resolution = decision.evidence.get("router_entity_resolution") if isinstance(decision.evidence.get("router_entity_resolution"), dict) else {}
    return {
        "query_type": decision.query_type,
        "lookup_mode": decision.lookup_mode,
        "media_entity": decision.entities[0] if decision.entities else "",
        "media_entities": list(decision.entities),
        "followup_target": str(decision.followup_target or ""),
        "rewritten_queries": {str(key): str(value) for key, value in (decision.rewritten_queries or {}).items() if str(key).strip() and str(value).strip()},
        "media_specific": bool(decision.entities),
        "media_entity_confident": bool(decision.entities),
        "media_title_marked": media_title_marked,
        "media_intent_cues": media_intent_cues,
        "llm_media": llm_media,
        "doc_similarity": {},
        "tech_score": 0.0,
        "tech_threshold": TECH_QUERY_DOC_SIM_THRESHOLD,
        "weak_tech_threshold": max(0.18, TECH_QUERY_DOC_SIM_THRESHOLD - 0.10),
        "weak_tech_signal": False,
        "query_tokens": _classifier_token_count(decision.raw_question),
        "profile_token_count": int(query_profile.get("token_count", 0) or 0),
        "short_media_surface": bool(decision.evidence.get("profile") == "short" and decision.domain == "media"),
        "disable_media_search": False,
        "media_signal": decision.needs_media_db,
        "strong_tech_signal": decision.domain == "tech" and (tech_cues or decision.llm_label == CLASSIFIER_LABEL_TECH),
        "abstract_media_concept": bool(decision.evidence.get("abstract_media_concept")),
        "concept_hints": [str(item).strip() for item in list(router_entity_resolution.get("concept_hints") or []) if str(item).strip()],
        "primary_entity": dict(router_entity_resolution.get("primary_entity") or {}),
        "selection_reason": str((router_entity_resolution.get("evidence") or {}).get("selection_reason") or "").strip(),
        "router_alias_resolution": {
            "count": int(router_alias_resolution.get("count", 0) or 0),
            "matched_terms": [str(item).strip() for item in list(router_alias_resolution.get("matched_terms") or []) if str(item).strip()][:8],
            "canonical_terms": [str(item).strip() for item in list(router_alias_resolution.get("canonical_terms") or []) if str(item).strip()][:8],
        },
        "router_entity_resolution": dict(router_entity_resolution),
        "tmdb_candidate": bool(decision.needs_external_media_db),
        "force_media_state": decision.followup_mode != "none" and _state_has_media_context(previous_state),
        "router_decision": _serialize_router_decision(decision),
        "fallback_evidence": {},
    }


def _build_router_decision(
    question: str,
    history: list[dict[str, str]],
    quota_state: dict[str, Any],
    query_profile: dict[str, Any],
    *,
    deps: RouterDeps | None = None,
) -> tuple[RouterDecision, dict[str, Any], dict[str, Any]]:
    resolved_deps = resolve_router_deps(deps)
    router_t0 = resolved_deps.perf_counter()
    raw_question = str(question or "").strip()
    previous_question = _find_previous_user_question(raw_question, history)
    previous_trace = resolved_deps.find_previous_trace_context(history)
    previous_trace_state = previous_trace.get("conversation_state_after") if isinstance(previous_trace.get("conversation_state_after"), dict) else {}
    previous_state = dict(previous_trace_state) if previous_trace_state else _infer_prior_question_state(previous_question, history)
    previous_working_set = _get_previous_media_working_set(previous_trace)
    if previous_working_set:
        previous_state["working_set"] = previous_working_set
    working_set_item_followup = _resolve_previous_working_set_item_followup(raw_question, previous_state)

    classification_timing: dict[str, Any] = {}
    llm_classification_t0 = resolved_deps.perf_counter()
    llm_media = resolved_deps.classify_media_query_with_llm(
        raw_question,
        quota_state,
        previous_state=previous_state,
        history=history,
        timing_sink=classification_timing,
        deps=resolved_deps,
    )
    llm_classification_seconds = resolved_deps.perf_counter() - llm_classification_t0
    llm_label = str(llm_media.get("label") or CLASSIFIER_LABEL_OTHER)
    llm_entities = [
        str(item).strip()
        for item in llm_media.get("entities", [])
        if str(item).strip() and not _looks_like_generic_media_scope(str(item).strip())
    ]
    llm_filters = _sanitize_media_filters(llm_media.get("filters"))
    llm_lookup_mode = str(llm_media.get("lookup_mode") or "general_lookup").strip() or "general_lookup"
    llm_domain = str(llm_media.get("domain") or "general").strip() or "general"
    llm_ranking = dict(llm_media.get("ranking") or {})
    llm_media_type_hint_values = [
        str(item).strip().lower()
        for item in llm_filters.get("media_type", [])
        if str(item).strip()
    ]
    entity_resolution_t0 = resolved_deps.perf_counter()
    router_entity_resolution = resolved_deps.resolve_media_entity(
        raw_question,
        hint_media_type=llm_media_type_hint_values[0] if len(llm_media_type_hint_values) == 1 else "",
        min_confidence=0.35,
    )
    entity_resolution_seconds = resolved_deps.perf_counter() - entity_resolution_t0
    router_entity_resolution_payload = _serialize_media_entity_resolution(router_entity_resolution)
    router_primary_entity = dict(router_entity_resolution_payload.get("primary_entity") or {})
    router_selection_reason = str((router_entity_resolution_payload.get("evidence") or {}).get("selection_reason") or "").strip()
    router_concept_hints = [
        str(item).strip()
        for item in list(router_entity_resolution_payload.get("concept_hints") or [])
        if str(item).strip()
    ]
    router_inferred_filters = _infer_media_filters(raw_question)
    router_alias_filter_hint = project_media_filters_to_library_schema(
        _merge_router_filters(router_inferred_filters, llm_filters),
        resolved_question=raw_question,
    ).filters
    alias_resolution_t0 = resolved_deps.perf_counter()
    router_title_alias = _resolve_router_title_alias_entities(
        raw_question,
        filters=router_alias_filter_hint,
    )
    llm_only_media_type_hint = bool(llm_filters.get("media_type")) and not bool(router_inferred_filters.get("media_type"))
    if not list(router_title_alias.get("entities") or []) and llm_only_media_type_hint:
        relaxed_router_alias_filters = project_media_filters_to_library_schema(
            router_inferred_filters,
            resolved_question=raw_question,
        ).filters
        router_title_alias = _resolve_router_title_alias_entities(
            raw_question,
            filters=relaxed_router_alias_filters or None,
        )
    alias_resolution_seconds = resolved_deps.perf_counter() - alias_resolution_t0
    alias_title_entities = [
        str(item).strip()
        for item in router_title_alias.get("entities", [])
        if str(item).strip()
    ]
    alias_title_media_types = [
        str(item).strip().lower()
        for item in router_title_alias.get("media_types", [])
        if str(item).strip()
    ]
    alias_matched_keys = {
        _normalize_media_title_for_match(str(item).strip())
        for item in router_title_alias.get("matched_terms", [])
        if str(item).strip()
    }
    explicit_media_entities = [
        str(item).strip()
        for item in _extract_media_entities(raw_question)
        if str(item).strip() and not _looks_like_generic_media_scope(str(item).strip())
    ]
    if alias_matched_keys:
        explicit_media_entities = [
            item
            for item in explicit_media_entities
            if _normalize_media_title_for_match(item) not in alias_matched_keys
        ]
    media_entities = list(explicit_media_entities)
    if alias_title_entities:
        media_entities = [*alias_title_entities, *media_entities]
    if not media_entities and llm_entities:
        media_entities = llm_entities
    media_title_marked = bool(alias_title_entities or explicit_media_entities) or _has_media_title_marker(raw_question)
    media_surface = _has_router_media_surface(raw_question)
    abstract_media_concept = _is_abstract_media_concept_query(
        raw_question,
        {"label": llm_label, "domain": llm_domain},
    ) or (
        bool(router_concept_hints)
        and not bool(router_primary_entity)
        and llm_label != CLASSIFIER_LABEL_TECH
        and llm_domain == "media"
    )
    collection_query = _is_collection_media_query(raw_question)
    primary_entity_kind = str(router_primary_entity.get("kind") or "").strip()
    primary_entity_canonical = str(router_primary_entity.get("canonical") or "").strip()
    if not media_entities and primary_entity_canonical:
        if primary_entity_kind == "title" and not collection_query:
            media_entities = [primary_entity_canonical]
        elif primary_entity_kind == "creator" and llm_lookup_mode == "entity_lookup" and not collection_query:
            media_entities = [primary_entity_canonical]
    if collection_query and not media_title_marked:
        media_entities = []
    followup_resolution_t0 = resolved_deps.perf_counter()
    followup_resolution = _derive_router_followup_resolution(raw_question, previous_state)
    followup_mode = followup_resolution.mode
    working_set_followup = _should_reuse_previous_working_set(raw_question, previous_state)
    followup_resolution_seconds = resolved_deps.perf_counter() - followup_resolution_t0
    if working_set_item_followup and followup_mode == "none":
        followup_mode = "inherit_filters"
    if working_set_followup and followup_mode == "none":
        followup_mode = "inherit_filters"
    if working_set_item_followup:
        matched_item = working_set_item_followup.get("item") if isinstance(working_set_item_followup.get("item"), dict) else {}
        matched_title = str(working_set_item_followup.get("matched_title") or matched_item.get("title") or "").strip()
        if matched_title:
            media_entities = [matched_title]
            llm_entities = [matched_title]
        media_title_marked = True
        collection_query = False
        llm_lookup_mode = "entity_lookup"
    if working_set_followup:
        media_entities = []
        media_title_marked = False
        collection_query = True
        llm_lookup_mode = "filter_search"
        llm_entities = []
    if "referential_collection_followup" in followup_resolution.reasons:
        media_entities = []
        llm_entities = []
        media_title_marked = False
        collection_query = True
        llm_lookup_mode = "filter_search"
    inherited_filters = _normalize_media_filter_map(previous_state.get("filters")) if followup_mode in {"inherit_filters", "inherit_timerange"} else {}
    current_filters = _sanitize_media_filters(_merge_router_filters(_infer_media_filters(raw_question), llm_filters))
    semantic_filters = _normalize_media_filter_map(current_filters)
    current_filters = _strip_semantic_hint_filter_fields(current_filters)
    current_projection = project_media_filters_to_library_schema(current_filters, resolved_question=raw_question)
    current_filters = current_projection.filters
    current_filters = _strip_unsupported_creator_filters_for_fresh_title_scope(
        raw_question,
        current_filters,
        title_entities=media_entities,
    )
    filters = merge_followup_filters(
        inherited_filters,
        current_filters,
        strategy=followup_resolution.merge_strategy if followup_mode != "none" else "none",
    )
    if current_filters.get("year"):
        filters["year"] = [str(value).strip() for value in current_filters.get("year", []) if str(value).strip()]

    fallback_year = _extract_year_from_date_range(previous_state.get("date_range"))
    current_date_window = _parse_media_date_window(raw_question, fallback_year if followup_mode != "none" else "")
    date_range: list[str] = []
    time_constraint: dict[str, Any] = {}
    if current_date_window:
        date_range = [str(current_date_window.get("start") or ""), str(current_date_window.get("end") or "")]
        time_constraint = _build_time_constraint(current_date_window, source="current")
    elif followup_mode == "inherit_filters":
        date_range = [str(item or "") for item in list(previous_state.get("date_range") or [])[:2] if str(item or "")]
        inherited_window = _date_window_from_state({"date_range": date_range})
        if inherited_window:
            time_constraint = _build_time_constraint({**inherited_window, "kind": "inherited_range"}, source="inherited")

    entities = [str(item).strip() for item in media_entities if str(item).strip()]
    if not entities and followup_mode == "inherit_entity":
        previous_entities = [str(item).strip() for item in list(previous_state.get("entities") or []) if str(item).strip()]
        previous_entity = str(previous_state.get("entity") or "").strip()
        entities = previous_entities or ([previous_entity] if previous_entity else [])
    entities, entity_filters = _normalize_media_entities_and_filters(entities)
    entity_filters = _strip_title_alias_self_filters(
        entity_filters,
        title_terms=[*alias_title_entities, *list(router_title_alias.get("matched_terms", [])), *list(router_title_alias.get("canonical_terms", []))],
    )
    filters = merge_followup_filters(filters, entity_filters, strategy="augment")
    if len(alias_title_media_types) == 1:
        alias_media_type = alias_title_media_types[0]
        if alias_media_type:
            filters["media_type"] = [alias_media_type]
    filters = _sanitize_media_filters(filters)
    filters = _strip_semantic_hint_filter_fields(filters)
    semantic_filters = _normalize_media_filter_map({**semantic_filters, **_normalize_media_filter_map(filters)})

    title_anchored_personal_review_query = _is_title_anchored_personal_review_query(
        raw_question,
        entities=entities,
        alias_canonical_terms=_extract_title_alias_canonical_terms(router_title_alias),
        title_marked=media_title_marked,
        filters=filters,
        date_range=date_range,
        followup_mode=followup_mode,
    )
    title_anchored_version_compare_query = _is_title_anchored_version_compare_query(
        raw_question,
        entities=entities,
        alias_canonical_terms=_extract_title_alias_canonical_terms(router_title_alias),
        title_marked=media_title_marked,
        filters=filters,
        date_range=date_range,
        followup_mode=followup_mode,
    )
    media_intent_cues = _has_media_intent_cues(raw_question) or title_anchored_personal_review_query or title_anchored_version_compare_query

    projection = project_media_filters_to_library_schema(filters, resolved_question=raw_question)
    filters = projection.filters
    media_type = projection.resolved_media_type or _resolved_media_type_label(filters, raw_question)
    if working_set_item_followup:
        matched_item = working_set_item_followup.get("item") if isinstance(working_set_item_followup.get("item"), dict) else {}
        matched_media_type = str(matched_item.get("media_type") or "").strip()
        if matched_media_type:
            media_type = matched_media_type
    if not media_type and followup_mode in {"inherit_filters", "inherit_entity", "inherit_timerange"}:
        media_type = str(previous_state.get("media_type") or "").strip()
    if media_type == "anime":
        filters = merge_followup_filters(filters, {"media_type": ["video"], "category": ["动画"]}, strategy="augment")
    selection = _build_media_selection(filters, media_type)
    metadata_anchor_resolution: dict[str, Any] = {"queries": [], "entries": [], "matched_terms": [], "canonical_terms": []}

    freshness = _infer_router_freshness(raw_question)
    tech_cues = _has_router_tech_cues(raw_question)
    if tech_cues and llm_label == CLASSIFIER_LABEL_TECH and not media_title_marked and not media_intent_cues and not media_surface:
        entities = []
    media_signal = bool(entities) or media_surface or abstract_media_concept or collection_query or followup_mode != "none" or llm_domain == "media" or llm_lookup_mode != "general_lookup"
    tech_signal = tech_cues or llm_label == CLASSIFIER_LABEL_TECH or llm_domain == "tech"

    strong_tech_override = (
        llm_label == CLASSIFIER_LABEL_TECH
        and (tech_cues or llm_domain == "tech")
        and not bool(entities)
        and not media_surface
        and not media_intent_cues
        and not media_title_marked
        and not collection_query
    )
    tech_primary = strong_tech_override
    explicit_media_anchor = (
        bool(entities)
        or media_surface
        or media_intent_cues
        or collection_query
        or abstract_media_concept
        or bool(router_primary_entity)
        or working_set_followup
        or (followup_mode != "none" and _state_has_media_context(previous_state))
        or (llm_domain == "media" and llm_lookup_mode in {"filter_search", "entity_lookup"})
    )
    media_primary = explicit_media_anchor and not tech_primary
    mixed_domain = (
        not tech_primary
        and tech_signal
        and media_signal
        and (bool(entities) or media_title_marked or media_surface or media_intent_cues or followup_mode == "inherit_entity")
    )

    if tech_primary:
        arbitration: str = "tech_primary"
        intent = "knowledge_qa"
        domain = "tech"
    elif mixed_domain:
        arbitration = "mixed_due_to_entity_plus_tech"
        intent = "mixed"
        domain = "media"
    elif media_primary:
        if "referential_collection_followup" in followup_resolution.reasons or working_set_followup:
            arbitration = "followup_or_collection_wins"
        elif media_surface or media_intent_cues:
            arbitration = "media_surface_wins"
        elif bool(entities):
            arbitration = "entity_wins"
        elif abstract_media_concept:
            arbitration = "abstract_concept_wins"
        else:
            arbitration = "followup_or_collection_wins"
        intent = "media_lookup"
        domain = "media"
    elif media_signal and not tech_signal:
        arbitration = "llm_media_weak_general"
        intent = "knowledge_qa"
        domain = "general"
    elif tech_signal:
        arbitration = "tech_signal_only"
        intent = "knowledge_qa"
        domain = "tech"
    elif any(cue in raw_question.lower() for cue in ROUTER_CHAT_CUES):
        arbitration = "chat"
        intent = "chat"
        domain = "general"
    else:
        arbitration = "general_fallback"
        intent = "knowledge_qa"
        domain = "general"

    if domain != "media" and intent == "knowledge_qa":
        entities = []
        filters = {}
        semantic_filters = {}
        date_range = []
        time_constraint = {}
        media_type = ""
        selection = {}
        followup_target = ""

    needs_media_db = domain == "media"
    needs_doc_rag = domain == "tech" or intent == "mixed" or (domain == "general" and intent == "knowledge_qa")
    needs_web = freshness != "none"
    if domain == "media":
        metadata_anchor_resolution = resolve_router_metadata_anchors(
            raw_question,
            filters=filters,
            media_type=media_type,
            resolve_library_aliases=lambda query, *, filters=None: resolved_deps.resolve_library_aliases(query, filters=filters),
            looks_like_generic_media_scope=_looks_like_generic_media_scope,
        )

    reasons: list[str] = []
    if followup_mode != "none":
        reasons.append(f"followup:{followup_mode}")
        for marker in followup_resolution.reasons:
            reasons.append(f"followup_reason:{marker}")
    if entities:
        reasons.append("explicit_media_entity")
    if media_surface:
        reasons.append("media_surface")
    if media_intent_cues:
        reasons.append("media_intent_cues")
    if collection_query:
        reasons.append("collection_query")
    if abstract_media_concept:
        reasons.append("abstract_media_concept")
    if working_set_followup:
        reasons.append("working_set_followup")
    if working_set_item_followup:
        reasons.append("working_set_item_followup")
    if router_primary_entity:
        reasons.append(f"resolver_primary_entity:{primary_entity_kind or 'unknown'}")
    if router_selection_reason:
        reasons.append(f"resolver_selection:{router_selection_reason}")
    if router_concept_hints:
        reasons.append("resolver_concept_hints")
    if tech_cues:
        reasons.append("lexical_tech_cues")
    if llm_label == CLASSIFIER_LABEL_MEDIA:
        reasons.append("llm_label_media")
    elif llm_label == CLASSIFIER_LABEL_TECH:
        reasons.append("llm_label_tech")
    if strong_tech_override:
        reasons.append("strong_tech_override")
    reasons.append(f"arbitration:{arbitration}")
    if llm_domain == "media" and not strong_tech_override:
        reasons.append("llm_domain_media")
    if llm_lookup_mode != "general_lookup" and domain == "media":
        reasons.append(f"llm_lookup_mode:{llm_lookup_mode}")
    if freshness != "none":
        reasons.append(f"freshness:{freshness}")

    confidence = 0.42
    if entities:
        confidence = max(confidence, 0.92)
    if followup_mode != "none" and _state_has_media_context(previous_state):
        confidence = max(confidence, 0.84)
    if working_set_followup:
        confidence = max(confidence, 0.9)
    if media_intent_cues or collection_query or abstract_media_concept:
        confidence = max(confidence, 0.78)
    if tech_cues:
        confidence = max(confidence, 0.76)
    if llm_label in {CLASSIFIER_LABEL_MEDIA, CLASSIFIER_LABEL_TECH}:
        confidence = max(confidence, 0.68)
    if float(llm_media.get("confidence") or 0.0) > 0:
        confidence = max(confidence, min(0.96, float(llm_media.get("confidence") or 0.0)))
    if len(raw_question) <= 8 and not entities and not filters and not tech_cues and followup_mode == "none":
        confidence = min(confidence, 0.45)

    lookup_mode = _derive_media_lookup_mode(
        domain=domain,
        entities=entities,
        filters=filters,
        date_range=[item for item in date_range if item],
        followup_mode=followup_mode,
        abstract_media_concept=abstract_media_concept,
        collection_query=collection_query,
    )
    if working_set_item_followup and domain == "media" and entities:
        lookup_mode = "entity_lookup"
    alias_title_collection_query = (
        bool(alias_title_entities)
        and bool(entities)
        and collection_query
        and not _has_specific_media_constraints(filters)
        and len(date_range) != 2
        and (_question_requests_personal_evaluation(raw_question) or _question_requests_media_details(raw_question))
    )
    if domain == "media" and (alias_title_collection_query or title_anchored_personal_review_query):
        lookup_mode = "entity_lookup"
    if domain == "media" and llm_lookup_mode in {"filter_search", "entity_lookup", "concept_lookup"}:
        if alias_title_collection_query or title_anchored_personal_review_query:
            lookup_mode = "entity_lookup"
        elif working_set_item_followup and entities:
            lookup_mode = "entity_lookup"
        elif llm_lookup_mode == "entity_lookup" and not collection_query:
            lookup_mode = "entity_lookup"
        elif llm_lookup_mode == "filter_search" and (filters or followup_mode != "none" or collection_query or len(date_range) == 2):
            lookup_mode = "filter_search"
        elif llm_lookup_mode == "concept_lookup" and abstract_media_concept:
            lookup_mode = "concept_lookup"
    if lookup_mode == "filter_search":
        entities = []
        reasons = [reason for reason in reasons if reason != "explicit_media_entity"]
    ranking = _build_media_ranking(raw_question, lookup_mode, time_constraint)
    if isinstance(llm_ranking, dict) and str(llm_ranking.get("mode") or "").strip() and ranking.get("mode") == "relevance":
        ranking = {
            "mode": str(llm_ranking.get("mode") or "relevance").strip() or "relevance",
            "source": str(llm_ranking.get("source") or "llm").strip() or "llm",
        }
    previous_ranking = previous_state.get("ranking") if isinstance(previous_state.get("ranking"), dict) else {}
    previous_sort = str(previous_state.get("sort") or "").strip()
    if followup_mode == "inherit_timerange" and ranking.get("source") == "time_constraint_default" and previous_sort:
        ranking = dict(previous_ranking) if previous_ranking else {"mode": previous_sort, "source": "inherited"}
        ranking["mode"] = str(ranking.get("mode") or previous_sort)
        ranking["source"] = str(ranking.get("source") or "inherited")
    sort = str(ranking.get("mode") or "relevance")
    allow_downstream_entity_inference = domain == "media" and lookup_mode == "general_lookup" and confidence < ROUTER_CONFIDENCE_MEDIUM
    if domain == "media" and lookup_mode == "general_lookup" and not entities and not _has_specific_media_constraints(filters):
        confidence = min(confidence, 0.52)
        allow_downstream_entity_inference = True
    followup_target = str(llm_media.get("followup_target") or "").strip() or (entities[0] if entities else primary_entity_canonical or _media_scope_label(media_type, filters))
    if working_set_item_followup and entities:
        followup_target = entities[0]
    if working_set_followup and not followup_target:
        followup_target = str(previous_state.get("media_type") or "这些内容").strip() or "这些内容"
    if domain != "media" and intent == "knowledge_qa":
        followup_target = ""
    resolved_question = _render_resolved_question_from_decision(raw_question, previous_state, followup_mode, entities)
    decision = RouterDecision(
        raw_question=raw_question,
        resolved_question=resolved_question,
        intent=intent,
        domain=domain,
        lookup_mode=lookup_mode,
        selection=selection,
        time_constraint=time_constraint,
        ranking=ranking,
        entities=entities,
        filters=filters,
        date_range=[item for item in date_range if item],
        sort=sort,
        freshness=freshness,
        needs_web=needs_web,
        needs_doc_rag=needs_doc_rag,
        needs_media_db=needs_media_db,
        needs_external_media_db=False,
        followup_mode=followup_mode,
        followup_filter_strategy=followup_resolution.merge_strategy,
        confidence=confidence,
        reasons=reasons,
        media_type=media_type,
        llm_label=llm_label,
        query_type=QUERY_TYPE_GENERAL,
        allow_downstream_entity_inference=allow_downstream_entity_inference,
        followup_target=followup_target,
        needs_comparison=bool(llm_media.get("needs_comparison")) or title_anchored_version_compare_query,
        needs_explanation=bool(llm_media.get("needs_explanation")) or _question_requests_media_details(raw_question) or _question_requests_personal_evaluation(raw_question),
        rewritten_queries={},
        arbitration=arbitration,
        evidence={
            "media_title_marked": media_title_marked,
            "media_intent_cues": media_intent_cues,
            "collection_query": collection_query,
            "alias_title_collection_query": alias_title_collection_query,
            "title_anchored_personal_review_query": title_anchored_personal_review_query,
            "title_anchored_version_compare_query": title_anchored_version_compare_query,
            "router_alias_resolution": {
                "count": len(router_title_alias.get("entries", [])),
                "entries": list(router_title_alias.get("entries", []))[:8],
                "matched_terms": list(router_title_alias.get("matched_terms", []))[:8],
                "canonical_terms": list(router_title_alias.get("canonical_terms", []))[:8],
            },
            "router_metadata_anchor_resolution": {
                "count": len(metadata_anchor_resolution.get("entries", [])),
                "queries": list(metadata_anchor_resolution.get("queries", []))[:8],
                "entries": list(metadata_anchor_resolution.get("entries", []))[:8],
                "matched_terms": list(metadata_anchor_resolution.get("matched_terms", []))[:8],
                "canonical_terms": list(metadata_anchor_resolution.get("canonical_terms", []))[:8],
            },
            "abstract_media_concept": abstract_media_concept,
            "tech_cues": tech_cues,
            "llm_lookup_mode": llm_lookup_mode,
            "llm_domain": llm_domain,
            "profile": str(query_profile.get("profile", "medium") or "medium"),
            "semantic_filters": semantic_filters,
            "execution_filters": _normalize_media_filter_map(filters),
            "schema_repairs": list(projection.applied_repairs),
            "execution_filter_warnings": list(projection.contract_warnings),
            "router_entity_resolution": router_entity_resolution_payload,
            "working_set_followup": working_set_followup,
            "working_set_item_followup": bool(working_set_item_followup),
            "working_set_item": dict(working_set_item_followup) if working_set_item_followup else {},
            "working_set_size": len((previous_state.get("working_set") or {}).get("items") or []),
            "previous_working_set": dict(previous_state.get("working_set") or {}) if previous_state.get("working_set") else {},
            "previous_question": previous_question,
            "previous_subject_scope": str(previous_state.get("subject_scope") or "").strip(),
        },
    )
    semantic_repair_t0 = resolved_deps.perf_counter()
    decision = _apply_router_semantic_repairs(raw_question, decision, previous_state, deps=resolved_deps)
    semantic_repair_seconds = resolved_deps.perf_counter() - semantic_repair_t0
    llm_tool_queries: dict[str, str] = {}
    llm_rewrite_seconds = 0.0
    rewrite_timing: dict[str, Any] = {}
    if not _should_skip_llm_tool_rewrite(raw_question, decision):
        llm_rewrite_t0 = resolved_deps.perf_counter()
        llm_tool_queries = resolved_deps.rewrite_tool_queries_with_llm(
            raw_question,
            decision,
            previous_state,
            quota_state,
            timing_sink=rewrite_timing,
            deps=resolved_deps,
        )
        llm_rewrite_seconds = resolved_deps.perf_counter() - llm_rewrite_t0
    decision.rewritten_queries = _build_tool_grade_rewritten_queries(raw_question, decision, previous_state, llm_tool_queries)
    if decision.domain == "media":
        decision.resolved_question = str(
            (decision.rewritten_queries or {}).get("media_query")
            or _render_resolved_question_from_decision(raw_question, previous_state, decision.followup_mode, decision.entities)
        ).strip()
    else:
        decision.resolved_question = str((decision.rewritten_queries or {}).get("doc_query") or resolved_question).strip()
    router_decision_seconds = resolved_deps.perf_counter() - router_t0
    router_llm_seconds = llm_classification_seconds + llm_rewrite_seconds
    planning_timing_breakdown = resolved_deps.normalize_timing_breakdown(
        {
            "router_decision_seconds": router_decision_seconds,
            "router_prompt_build_seconds": classification_timing.get("router_prompt_build_seconds", 0.0),
            "router_llm_request_seconds": classification_timing.get("router_llm_request_seconds", 0.0),
            "router_llm_response_seconds": classification_timing.get("router_llm_response_seconds", 0.0),
            "router_llm_classification_seconds": llm_classification_seconds,
            "router_entity_resolution_seconds": entity_resolution_seconds,
            "router_alias_resolution_seconds": alias_resolution_seconds,
            "router_followup_resolution_seconds": followup_resolution_seconds,
            "router_semantic_repairs_seconds": semantic_repair_seconds,
            "router_rewrite_prompt_build_seconds": rewrite_timing.get("router_rewrite_prompt_build_seconds", 0.0),
            "router_rewrite_llm_request_seconds": rewrite_timing.get("router_rewrite_llm_request_seconds", 0.0),
            "router_rewrite_llm_response_seconds": rewrite_timing.get("router_rewrite_llm_response_seconds", 0.0),
            "router_llm_rewrite_seconds": llm_rewrite_seconds,
            "router_non_llm_seconds": max(0.0, router_decision_seconds - router_llm_seconds),
        }
    )
    decision.evidence = {
        **dict(decision.evidence or {}),
        "llm_tool_rewrites": llm_tool_queries,
        "tool_rewrite_source": "deterministic_plus_llm_rewrite" if llm_tool_queries else "deterministic",
        "planning_timing_breakdown": planning_timing_breakdown,
    }
    decision.query_type = _map_router_query_type(decision)
    decision = apply_router_semantic_contract(
        decision,
        deps=resolved_deps.planner_router_semantic_deps,
        metadata_anchor_resolution=metadata_anchor_resolution,
    )
    decision.needs_external_media_db = _decision_requires_tmdb(decision)
    return decision, llm_media, previous_state


def _classify_query_type(query: str, quota_state: dict[str, Any], query_profile: dict[str, Any]) -> dict[str, Any]:
    decision, llm_media, previous_state = _build_router_decision(query, [], quota_state, query_profile)
    return _router_decision_to_query_classification(decision, llm_media, previous_state, query_profile)


def _build_router_decision_path(
    query_classification: dict[str, Any],
    search_mode: str,
    planned_tools: list[PlannedToolCall],
    tool_results: list[ToolExecution],
) -> tuple[str, list[str]]:
    path: list[str] = []
    router_decision = query_classification.get("router_decision") if isinstance(query_classification.get("router_decision"), dict) else {}
    query_type = _get_query_type(query_classification=query_classification)
    domain = str(router_decision.get("domain") or "general")
    intent = str(router_decision.get("intent") or "knowledge_qa")
    followup_mode = str(router_decision.get("followup_mode") or "none")
    confidence = float(router_decision.get("confidence", 0.0) or 0.0)
    freshness = str(router_decision.get("freshness") or "none")

    path.append(f"domain:{domain}")
    path.append(f"intent:{intent}")
    if followup_mode != "none":
        path.append(f"followup:{followup_mode}")
    if bool(router_decision.get("entities")):
        path.append("explicit_media_entity")
    if any(call.name == TOOL_EXPAND_MEDIAWIKI_CONCEPT for call in planned_tools):
        path.append("external_concept_expand")
    if any(call.name == TOOL_SEARCH_TMDB for call in planned_tools):
        path.append("external_media_db")
    if freshness != "none":
        path.append(f"freshness:{freshness}")
    if confidence >= ROUTER_CONFIDENCE_HIGH:
        path.append("confidence:high")
    elif confidence >= ROUTER_CONFIDENCE_MEDIUM:
        path.append("confidence:medium")
    else:
        path.append("confidence:low")

    normalized_mode = _normalize_search_mode(search_mode)
    if normalized_mode == "hybrid" and any(call.name == TOOL_SEARCH_WEB for call in planned_tools):
        path.append("policy:web_fallback")
    elif normalized_mode == "local_only":
        path.append("local_only")

    if query_type == QUERY_TYPE_MIXED:
        path.append("mixed_multi_tool")
        category = "mixed_multi_tool"
    elif domain == "tech":
        category = "tech_rag"
    elif domain == "media":
        category = "media_lookup"
    elif "policy:web_fallback" in path:
        category = "web_fallback"
    else:
        category = "default_doc_rag"

    executed_tools = [item for item in tool_results if str(item.status or "").strip().lower() != "skipped"]
    if len(executed_tools) > 1:
        path.append("multi_tool_executed")

    return category, path
