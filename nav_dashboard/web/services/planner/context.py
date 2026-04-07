from __future__ import annotations

from typing import Any, Callable

from ..agent.agent_types import PlannedToolCall, RouterDecision

from .domain import ExecutionPlanShapingResult, RouterDecisionNormalizationResult


def finalize_query_classification(
    *,
    question: str,
    normalization: RouterDecisionNormalizationResult,
    shaped: ExecutionPlanShapingResult,
    serialize_router_context_resolution: Callable[[Any], dict[str, Any]],
    serialize_execution_plan: Callable[[Any], dict[str, Any]],
    serialize_post_retrieval_assessment: Callable[[Any], dict[str, Any]],
    get_ontology_load_statuses: Callable[[], Any],
) -> dict[str, Any]:
    query_classification = dict(normalization.query_classification)
    planning_artifact = shaped.runtime_state.planning_artifact
    fallback_evidence = serialize_post_retrieval_assessment(shaped.runtime_state.post_retrieval_assessment)
    query_classification["fallback_evidence"] = fallback_evidence
    query_classification["doc_similarity"] = dict(fallback_evidence.get("doc_similarity") or {})
    query_classification["tech_score"] = float(shaped.runtime_state.post_retrieval_assessment.tech_score or 0.0)
    query_classification["weak_tech_signal"] = bool(shaped.runtime_state.post_retrieval_assessment.weak_tech_signal)
    query_classification["strong_tech_signal"] = normalization.router_decision.domain == "tech"
    query_classification["original_question"] = question
    query_classification["context_resolution"] = serialize_router_context_resolution(
        planning_artifact.context_resolution or shaped.context_resolution
    )
    query_classification["resolved_question"] = shaped.resolved_question
    query_classification["resolved_query_state"] = dict(shaped.context_resolution.resolved_query_state)
    query_classification["query_type"] = normalization.router_decision.query_type
    query_classification["query_class"] = normalization.router_decision.query_class
    query_classification["subject_scope"] = normalization.router_decision.subject_scope
    query_classification["time_scope_type"] = normalization.router_decision.time_scope_type
    query_classification["answer_shape"] = normalization.router_decision.answer_shape
    query_classification["media_family"] = normalization.router_decision.media_family
    query_classification["metadata_anchors"] = list(normalization.router_decision.metadata_anchors or [])
    query_classification["scope_anchors"] = list(normalization.router_decision.scope_anchors or [])
    query_classification["execution_plan"] = serialize_execution_plan(
        planning_artifact.execution_plan or shaped.execution_plan
    )
    context_resolution = planning_artifact.context_resolution or shaped.context_resolution
    query_classification["conversation_state_before"] = dict(context_resolution.conversation_state_before)
    query_classification["detected_followup"] = bool(context_resolution.detected_followup)
    query_classification["inheritance_applied"] = dict(context_resolution.inheritance_applied)
    query_classification["conversation_state_after"] = dict(context_resolution.conversation_state_after)
    query_classification["state_diff"] = dict(context_resolution.state_diff)
    query_classification["planner_snapshot"] = dict(planning_artifact.planner_snapshot or {})
    query_classification["ontology_load_status"] = get_ontology_load_statuses()
    return query_classification


def build_resolved_query_state_from_decision(
    decision: RouterDecision,
    *,
    normalize_media_filter_map: Callable[[Any], dict[str, list[str]]],
    router_followup_mode_label: Callable[[str], str],
) -> dict[str, Any]:
    followup_mode = str(decision.followup_mode or "none")
    router_entity_resolution = decision.evidence.get("router_entity_resolution") if isinstance(decision.evidence.get("router_entity_resolution"), dict) else {}
    return {
        "lookup_mode": str(decision.lookup_mode or "general_lookup"),
        "selection": normalize_media_filter_map(decision.selection),
        "time_constraint": dict(decision.time_constraint),
        "ranking": dict(decision.ranking),
        "media_type": str(decision.media_type or ""),
        "filters": normalize_media_filter_map(decision.filters),
        "date_range": list(decision.date_range or []),
        "sort": str(decision.sort or "relevance"),
        "followup_target": str(decision.followup_target or ""),
        "followup_filter_strategy": str(decision.followup_filter_strategy or "none"),
        "rewritten_queries": {str(key): str(value) for key, value in (decision.rewritten_queries or {}).items() if str(key).strip() and str(value).strip()},
        "concept_hints": [str(item).strip() for item in list(router_entity_resolution.get("concept_hints") or []) if str(item).strip()],
        "primary_entity": dict(router_entity_resolution.get("primary_entity") or {}),
        "entity_selection_reason": str((router_entity_resolution.get("evidence") or {}).get("selection_reason") or "").strip(),
        "metadata_anchors": list(decision.metadata_anchors or []),
        "scope_anchors": list(decision.scope_anchors or []),
        "carry_over_from_previous_turn": followup_mode != "none",
        "inherited_context": {
            "used": followup_mode != "none",
            "kind": router_followup_mode_label(followup_mode),
            "filter_strategy": str(decision.followup_filter_strategy or "none"),
        },
    }


def build_conversation_state_snapshot_from_decision(
    question: str,
    decision: RouterDecision,
    resolved_query_state: dict[str, Any],
    *,
    get_lookup_mode_from_state: Callable[[dict[str, Any] | None], str],
    normalize_media_filter_map: Callable[[Any], dict[str, list[str]]],
) -> dict[str, Any]:
    entities = [str(item).strip() for item in list(decision.entities or []) if str(item).strip()]
    router_entity_resolution = decision.evidence.get("router_entity_resolution") if isinstance(decision.evidence.get("router_entity_resolution"), dict) else {}
    return {
        "question": str(question or "").strip(),
        "lookup_mode": get_lookup_mode_from_state(resolved_query_state),
        "selection": normalize_media_filter_map(resolved_query_state.get("selection")),
        "time_constraint": dict(resolved_query_state.get("time_constraint") or {}),
        "ranking": dict(resolved_query_state.get("ranking") or {}),
        "media_type": str(resolved_query_state.get("media_type", "") or ""),
        "entity": entities[0] if entities else "",
        "entities": entities,
        "filters": normalize_media_filter_map(resolved_query_state.get("filters")),
        "date_range": list(resolved_query_state.get("date_range") or []),
        "sort": str(resolved_query_state.get("sort", "") or ""),
        "followup_target": str(resolved_query_state.get("followup_target", "") or ""),
        "rewritten_media_query": str(((resolved_query_state.get("rewritten_queries") or {}) if isinstance(resolved_query_state.get("rewritten_queries"), dict) else {}).get("media_query") or ""),
        "concept_hints": [str(item).strip() for item in list(router_entity_resolution.get("concept_hints") or []) if str(item).strip()],
        "primary_entity": dict(router_entity_resolution.get("primary_entity") or {}),
        "entity_selection_reason": str((router_entity_resolution.get("evidence") or {}).get("selection_reason") or "").strip(),
        "metadata_anchors": list(decision.metadata_anchors or []),
        "scope_anchors": list(decision.scope_anchors or []),
    }


def build_planner_snapshot_from_decision(
    decision: RouterDecision,
    resolved_question: str,
    resolved_query_state: dict[str, Any],
    planned_tools: list[PlannedToolCall],
    *,
    get_lookup_mode_from_state: Callable[[dict[str, Any] | None], str],
    normalize_media_filter_map: Callable[[Any], dict[str, list[str]]],
    normalize_timing_breakdown: Callable[[Any], dict[str, float]],
) -> dict[str, Any]:
    state_after = build_conversation_state_snapshot_from_decision(
        resolved_question,
        decision,
        resolved_query_state,
        get_lookup_mode_from_state=get_lookup_mode_from_state,
        normalize_media_filter_map=normalize_media_filter_map,
    )
    hard_filters: dict[str, Any] = {}
    media_type = str(state_after.get("media_type", "") or "")
    if media_type:
        hard_filters["media_type"] = media_type
    for field, values in (state_after.get("filters") or {}).items():
        if not isinstance(values, list):
            continue
        clean_values = [str(value).strip() for value in values if str(value).strip()]
        if not clean_values:
            continue
        if field == "media_type" and media_type:
            continue
        hard_filters[str(field)] = clean_values[0] if len(clean_values) == 1 else clean_values
    date_range = state_after.get("date_range") or []
    if date_range:
        hard_filters["date_range"] = date_range
    soft_constraints: dict[str, Any] = {}
    sort = str(state_after.get("sort", "") or "")
    if sort:
        soft_constraints["sort"] = sort
    ranking = state_after.get("ranking") if isinstance(state_after.get("ranking"), dict) else {}
    if ranking:
        soft_constraints["ranking"] = ranking
    rewritten_queries = {str(key): str(value) for key, value in (decision.rewritten_queries or {}).items() if str(key).strip() and str(value).strip()}
    router_entity_resolution = decision.evidence.get("router_entity_resolution") if isinstance(decision.evidence.get("router_entity_resolution"), dict) else {}
    timing_breakdown = normalize_timing_breakdown(
        decision.evidence.get("planning_timing_breakdown") if isinstance(decision.evidence, dict) else {}
    )
    lookup_mode = get_lookup_mode_from_state(resolved_query_state)
    query_text: str | None = rewritten_queries.get("media_query") or (decision.entities[0] if decision.entities else None)
    if query_text is None and lookup_mode != "filter_search":
        query_text = resolved_question or None
    followup_mode = str(decision.followup_mode or "none")
    followup_mode_label = str((resolved_query_state.get("inherited_context") or {}).get("kind", "") or "none")
    return {
        "lookup_mode": lookup_mode,
        "selection": normalize_media_filter_map(state_after.get("selection")),
        "time_constraint": dict(state_after.get("time_constraint") or {}),
        "hard_filters": hard_filters,
        "soft_constraints": soft_constraints,
        "query_text": query_text,
        "rewritten_queries": rewritten_queries,
        "followup_mode": followup_mode,
        "followup_mode_label": followup_mode_label,
        "concept_hints": [str(item).strip() for item in list(router_entity_resolution.get("concept_hints") or []) if str(item).strip()],
        "primary_entity": dict(router_entity_resolution.get("primary_entity") or {}),
        "entity_selection_reason": str((router_entity_resolution.get("evidence") or {}).get("selection_reason") or "").strip(),
        "metadata_anchors": list(decision.metadata_anchors or []),
        "scope_anchors": list(decision.scope_anchors or []),
        "planned_tools": [call.name for call in planned_tools],
        "timing_breakdown": timing_breakdown,
    }