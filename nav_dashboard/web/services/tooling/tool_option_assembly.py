from __future__ import annotations

from typing import Any

from ..agent.agent_types import (
    CLASSIFIER_LABEL_OTHER,
    QUERY_TYPE_GENERAL,
    TOOL_QUERY_MEDIA,
    TOOL_SEARCH_BY_CREATOR,
    PlannedToolCall,
    RouterDecision,
)
from ..planner.planner_contracts import deserialize_router_decision as deserialize_router_decision_payload
from ..planner.planner_contracts import (
    ROUTER_ANSWER_SHAPE_NONE,
    ROUTER_MEDIA_FAMILY_NONE,
    ROUTER_QUERY_CLASS_MUSIC_WORK_VERSIONS_COMPARE,
    ROUTER_QUERY_CLASS_KNOWLEDGE_QA,
    ROUTER_QUERY_CLASS_PERSONAL_MEDIA_REVIEW_COLLECTION,
    ROUTER_SLOT_ANSWER_SHAPE,
    ROUTER_SLOT_MEDIA_FAMILY,
    ROUTER_SLOT_QUERY_CLASS,
    ROUTER_SLOT_SUBJECT_SCOPE,
    ROUTER_SLOT_TIME_SCOPE_TYPE,
    ROUTER_SUBJECT_SCOPE_GENERAL_KNOWLEDGE,
    ROUTER_TIME_SCOPE_NONE,
    serialize_router_decision_plan_contract,
    serialize_router_plan_contract_slots,
)
from ..media.media_query_adapter import (
    get_filter_contract_warnings,
    normalize_filter_map as normalize_library_filter_map,
)


def normalize_media_filter_map(value: Any) -> dict[str, list[str]]:
    return normalize_library_filter_map(value)


def deserialize_router_decision(payload: dict[str, Any], *, fallback_question: str = "") -> RouterDecision:
    return deserialize_router_decision_payload(payload, fallback_question=fallback_question)


def date_window_from_state(state: dict[str, Any] | None) -> dict[str, str]:
    current = state if isinstance(state, dict) else {}
    time_constraint = current.get("time_constraint") if isinstance(current.get("time_constraint"), dict) else {}
    if time_constraint:
        start = str(time_constraint.get("start") or "").strip()
        end = str(time_constraint.get("end") or "").strip()
        if start and end:
            return {"start": start, "end": end}
    explicit_window = current.get("date_window") if isinstance(current.get("date_window"), dict) else {}
    if explicit_window:
        start = str(explicit_window.get("start") or "").strip()
        end = str(explicit_window.get("end") or "").strip()
        if start and end:
            return {"start": start, "end": end}
    date_range = current.get("date_range") if isinstance(current.get("date_range"), list) else []
    if len(date_range) != 2:
        return {}
    start = str(date_range[0] or "").strip()
    end = str(date_range[1] or "").strip()
    if not start or not end:
        return {}
    return {"start": start, "end": end}


def build_media_selection(filters: dict[str, list[str]], media_type: str = "") -> dict[str, list[str]]:
    selection: dict[str, list[str]] = {}
    normalized = normalize_media_filter_map(filters)
    for field in (
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
    ):
        if field in normalized:
            selection[field] = list(normalized[field])
    if media_type and "media_type" not in selection:
        selection["media_type"] = [media_type]
    return selection


def get_lookup_mode_from_state(state: dict[str, Any] | None) -> str:
    current = state if isinstance(state, dict) else {}
    lookup_mode = str(current.get("lookup_mode") or "").strip()
    if lookup_mode:
        return lookup_mode
    legacy_intent = str(current.get("intent") or "").strip()
    if legacy_intent in {"filter_search", "entity_lookup", "concept_lookup", "general_lookup"}:
        return legacy_intent
    return "general_lookup"


def build_creator_tool_options_from_decision(decision: RouterDecision) -> dict[str, Any]:
    creator_resolution = decision.evidence.get("creator_resolution") if isinstance(decision.evidence.get("creator_resolution"), dict) else {}
    creator_canonical = str(creator_resolution.get("canonical") or "").strip()
    creator_media_type = str(creator_resolution.get("media_type_hint") or "").strip()
    normalized_filters = normalize_media_filter_map(decision.filters)
    options = {
        "creator_name": creator_canonical,
        "media_type": creator_media_type or str(decision.media_type or "").strip(),
        "filters": normalized_filters,
        "date_window": date_window_from_state({"date_range": decision.date_range}),
        "sort": str(decision.sort or "relevance"),
        "ranking": dict(decision.ranking or {}),
    }
    return {key: value for key, value in options.items() if value not in ({}, [], "", None)}


def build_media_tool_options_from_decision(decision: RouterDecision) -> dict[str, Any]:
    lookup_mode = str(decision.lookup_mode or "general_lookup")
    rewritten_queries = {
        str(key): str(value)
        for key, value in (decision.rewritten_queries or {}).items()
        if str(key).strip() and str(value).strip()
    }
    query_text: str | None = rewritten_queries.get("media_query") or (decision.entities[0] if lookup_mode == "entity_lookup" and decision.entities else None)
    if query_text is None and lookup_mode not in {"filter_search", "concept_lookup"}:
        query_text = decision.resolved_question or decision.raw_question or None
    evidence = dict(decision.evidence or {})
    music_hints = evidence.get("music_work_hints") if isinstance(evidence.get("music_work_hints"), dict) else {}
    composer_hints = [str(item).strip() for item in (music_hints.get("composer_hints") or []) if str(item).strip()]
    work_signature = [str(item).strip() for item in (music_hints.get("work_signature") or []) if str(item).strip()]
    instrument_hints = [str(item).strip() for item in (music_hints.get("instrument_hints") or []) if str(item).strip()]
    form_hints = [str(item).strip() for item in (music_hints.get("form_hints") or []) if str(item).strip()]
    work_family_hints = [str(item).strip() for item in (music_hints.get("work_family_hints") or []) if str(item).strip()]
    previous_working_set = evidence.get("previous_working_set") if isinstance(evidence.get("previous_working_set"), dict) and bool(evidence.get("working_set_followup")) else {}
    working_set_item = evidence.get("working_set_item") if isinstance(evidence.get("working_set_item"), dict) else {}
    normalized_selection = normalize_media_filter_map(decision.selection)
    normalized_filters = normalize_media_filter_map(decision.filters)
    filter_contract_warnings = get_filter_contract_warnings(normalized_filters)
    query_class = str(decision.query_class or ROUTER_QUERY_CLASS_KNOWLEDGE_QA)
    is_signature_constrained_music_compare = bool(composer_hints) and bool(
        work_signature or instrument_hints or form_hints or work_family_hints
    ) and "music" in {str(item).strip().lower() for item in normalized_filters.get("media_type", [])}
    is_title_anchored_personal_review = (
        query_class == ROUTER_QUERY_CLASS_PERSONAL_MEDIA_REVIEW_COLLECTION
        and bool(decision.entities)
        and (
            lookup_mode == "entity_lookup"
            or bool(evidence.get("title_anchored_personal_review_query"))
            or bool(evidence.get("media_title_marked"))
        )
    )
    if query_class == ROUTER_QUERY_CLASS_MUSIC_WORK_VERSIONS_COMPARE or is_signature_constrained_music_compare:
        for field in ("author", "category"):
            normalized_selection.pop(field, None)
            normalized_filters.pop(field, None)
    if is_title_anchored_personal_review:
        normalized_selection.pop("category", None)
        normalized_filters.pop("category", None)
    media_entities = list(decision.entities) if (
        decision.entities
        and (
            lookup_mode == "entity_lookup"
            or query_class in {ROUTER_QUERY_CLASS_PERSONAL_MEDIA_REVIEW_COLLECTION, ROUTER_QUERY_CLASS_MUSIC_WORK_VERSIONS_COMPARE}
            or bool(evidence.get("media_title_marked"))
        )
    ) else []

    options = {
        "selection": normalized_selection,
        "time_constraint": dict(decision.time_constraint),
        "ranking": dict(decision.ranking),
        "filters": normalized_filters,
        "date_window": date_window_from_state({"date_range": decision.date_range}),
        "sort": decision.sort,
        "lookup_mode": lookup_mode,
        "media_type": decision.media_type,
        "media_entities": media_entities,
        "allow_downstream_entity_inference": bool(decision.allow_downstream_entity_inference),
        "query_text": query_text,
        "rewritten_queries": rewritten_queries,
        "query_class": query_class,
        "composer_hints": composer_hints,
        "instrument_hints": instrument_hints,
        "form_hints": form_hints,
        "work_family_hints": work_family_hints,
        "work_signature": work_signature,
        "previous_working_set": previous_working_set,
        "working_set_item": working_set_item,
        "filter_contract_warnings": filter_contract_warnings,
    }
    options.update(serialize_router_decision_plan_contract(decision))
    return {key: value for key, value in options.items() if value not in ({}, [], "", None)}


def build_media_tool_options(
    question: str,
    resolved_query_state: dict[str, Any] | None,
    query_classification: dict[str, Any] | None,
) -> dict[str, Any]:
    state = resolved_query_state if isinstance(resolved_query_state, dict) else {}
    current = query_classification if isinstance(query_classification, dict) else {}
    router_decision = current.get("router_decision") if isinstance(current.get("router_decision"), dict) else {}
    if router_decision:
        return build_media_tool_options_from_decision(
            deserialize_router_decision(router_decision, fallback_question=question)
        )
    filters = normalize_media_filter_map(state.get("filters"))
    selection = normalize_media_filter_map(state.get("selection")) or build_media_selection(filters, str(state.get("media_type") or ""))
    time_constraint = dict(state.get("time_constraint") or {})
    ranking = dict(state.get("ranking") or {})
    date_window = date_window_from_state(state)
    lookup_mode = get_lookup_mode_from_state(state)
    media_entities = [str(item).strip() for item in (current.get("media_entities") or []) if str(item).strip()]
    query_text = str(current.get("media_entity") or "").strip() or None
    if query_text is None and lookup_mode != "filter_search":
        query_text = str(question or "").strip() or None
    options = {
        "selection": selection,
        "time_constraint": time_constraint,
        "ranking": ranking,
        "filters": filters,
        "date_window": date_window,
        "sort": str(state.get("sort", "relevance") or "relevance"),
        "lookup_mode": lookup_mode,
        "media_type": str(state.get("media_type", "") or ""),
        "media_entities": media_entities,
        "query_text": query_text,
    }
    options.update(
        serialize_router_plan_contract_slots(
            query_class=str(current.get(ROUTER_SLOT_QUERY_CLASS) or ROUTER_QUERY_CLASS_KNOWLEDGE_QA),
            subject_scope=str(current.get(ROUTER_SLOT_SUBJECT_SCOPE) or ROUTER_SUBJECT_SCOPE_GENERAL_KNOWLEDGE),
            time_scope_type=str(current.get(ROUTER_SLOT_TIME_SCOPE_TYPE) or ROUTER_TIME_SCOPE_NONE),
            answer_shape=str(current.get(ROUTER_SLOT_ANSWER_SHAPE) or ROUTER_ANSWER_SHAPE_NONE),
            media_family=str(current.get(ROUTER_SLOT_MEDIA_FAMILY) or ROUTER_MEDIA_FAMILY_NONE),
        )
    )
    return {key: value for key, value in options.items() if value not in ({}, [], "", None)}


def assemble_execution_tool_options(
    planned_tools: list[PlannedToolCall],
    *,
    decision: RouterDecision,
) -> list[PlannedToolCall]:
    assembled: list[PlannedToolCall] = []
    media_options = build_media_tool_options_from_decision(decision)
    creator_options = build_creator_tool_options_from_decision(decision)
    for call in planned_tools:
        options = dict(call.options or {})
        if call.name == TOOL_QUERY_MEDIA:
            options = media_options
        elif call.name == TOOL_SEARCH_BY_CREATOR:
            options = creator_options
        assembled.append(
            PlannedToolCall(
                name=call.name,
                query=call.query,
                options=options,
            )
        )
    return assembled