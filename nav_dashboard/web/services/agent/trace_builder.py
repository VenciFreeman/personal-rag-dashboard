from __future__ import annotations

from typing import Any, Callable

from .agent_types import QUERY_TYPE_GENERAL, TOOL_QUERY_MEDIA, PlannedToolCall, ToolExecution
from ..media.media_retrieval_service import get_media_main_result_rows_from_data, get_media_mention_rows_from_data


TRACE_SCHEMA_VERSION = 3
TRACE_SECTION_CONTRACT_VERSION = 1


def _round_stage_seconds(value: Any) -> float:
    try:
        return round(float(value or 0), 6)
    except (TypeError, ValueError):
        return 0.0


def _clean_second_map(values: dict[str, Any]) -> dict[str, float]:
    cleaned: dict[str, float] = {}
    for key, value in values.items():
        normalized_key = str(key).strip()
        if not normalized_key or value is None:
            continue
        cleaned[normalized_key] = _round_stage_seconds(value)
    return cleaned


def _sum_seconds(values: dict[str, Any]) -> float:
    return round(sum(_round_stage_seconds(value) for value in values.values()), 6)


def _build_section(
    *,
    total_seconds: float,
    breakdown: dict[str, Any] | None = None,
    llm_stats: dict[str, Any] | None = None,
    accounting_basis: str | None = None,
    children: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    section: dict[str, Any] = {
        "contract_version": TRACE_SECTION_CONTRACT_VERSION,
        "total_seconds": _round_stage_seconds(total_seconds),
        "breakdown": _clean_second_map(breakdown or {}),
    }
    if isinstance(llm_stats, dict) and llm_stats:
        section["llm_stats"] = dict(llm_stats)
    if accounting_basis:
        section["accounting_basis"] = str(accounting_basis)
    for child_name, child_value in (children or {}).items():
        if isinstance(child_value, dict) and child_value:
            section[str(child_name)] = child_value
    return section


def _build_answer_llm_stats(backend: str, model: str, llm_seconds: float, llm_stats: dict[str, Any]) -> dict[str, Any]:
    return {
        "backend": backend,
        "model": model,
        "latency_seconds": _round_stage_seconds(llm_seconds),
        "input_tokens_est": int(llm_stats.get("input_tokens_est", 0) or 0),
        "prompt_tokens_est": int(llm_stats.get("prompt_tokens_est", 0) or 0),
        "context_tokens_est": int(llm_stats.get("context_tokens_est", 0) or 0),
        "output_tokens_est": int(llm_stats.get("output_tokens_est", 0) or 0),
        "calls": int(llm_stats.get("calls", 0) or 0),
        "coverage": "answer_only",
    }


def _build_planner_llm_stats(backend: str, model: str, planner_timings: dict[str, Any]) -> dict[str, Any]:
    classification_seconds = _round_stage_seconds(planner_timings.get("router_llm_classification_seconds", 0.0))
    rewrite_seconds = _round_stage_seconds(planner_timings.get("router_llm_rewrite_seconds", 0.0))
    calls = int(classification_seconds > 0) + int(rewrite_seconds > 0)
    return {
        "backend": backend,
        "model": model,
        "latency_seconds": _round_stage_seconds(classification_seconds + rewrite_seconds),
        "calls": calls,
        "coverage": "planner_duration_only",
        "timing_sources": {
            "classification_seconds": classification_seconds,
            "rewrite_seconds": rewrite_seconds,
        },
    }


def _build_total_llm_stats(planner_llm_stats: dict[str, Any], answer_llm_stats: dict[str, Any]) -> dict[str, Any]:
    return {
        "backend": str(answer_llm_stats.get("backend") or planner_llm_stats.get("backend") or ""),
        "model": str(answer_llm_stats.get("model") or planner_llm_stats.get("model") or ""),
        "latency_seconds": _round_stage_seconds(
            float(planner_llm_stats.get("latency_seconds", 0.0) or 0.0) + float(answer_llm_stats.get("latency_seconds", 0.0) or 0.0)
        ),
        "input_tokens_est": int(answer_llm_stats.get("input_tokens_est", 0) or 0),
        "prompt_tokens_est": int(answer_llm_stats.get("prompt_tokens_est", 0) or 0),
        "context_tokens_est": int(answer_llm_stats.get("context_tokens_est", 0) or 0),
        "output_tokens_est": int(answer_llm_stats.get("output_tokens_est", 0) or 0),
        "calls": int(planner_llm_stats.get("calls", 0) or 0) + int(answer_llm_stats.get("calls", 0) or 0),
        "coverage": "duration=planner+answer;tokens=answer_only",
    }


def _build_aggregate_stage_timings(
    *,
    planning_seconds: float,
    execution_seconds: float,
    answer_seconds: float,
    observability_seconds: float,
    unaccounted_seconds: float,
    wall_clock_seconds: float,
) -> dict[str, float]:
    return {
        "planning_seconds": _round_stage_seconds(planning_seconds),
        "execution_seconds": _round_stage_seconds(execution_seconds),
        "answer_seconds": _round_stage_seconds(answer_seconds),
        "observability_seconds": _round_stage_seconds(observability_seconds),
        "unaccounted_seconds": _round_stage_seconds(unaccounted_seconds),
        "wall_clock_seconds": _round_stage_seconds(wall_clock_seconds),
    }


def _group_stage_timings(stages: dict[str, float]) -> dict[str, dict[str, float]]:
    return {
        "planner": {"planning_seconds": _round_stage_seconds(stages.get("planning_seconds", 0.0))},
        "execution": {"execution_seconds": _round_stage_seconds(stages.get("execution_seconds", 0.0))},
        "answer": {"answer_seconds": _round_stage_seconds(stages.get("answer_seconds", 0.0))},
        "observability": {"observability_seconds": _round_stage_seconds(stages.get("observability_seconds", 0.0))},
        "system": {
            "unaccounted_seconds": _round_stage_seconds(stages.get("unaccounted_seconds", 0.0)),
            "wall_clock_seconds": _round_stage_seconds(stages.get("wall_clock_seconds", 0.0)),
        },
    }


def build_agent_trace_record(
    *,
    trace_id: str,
    session_id: str,
    backend: str,
    search_mode: str,
    benchmark_mode: bool,
    stream_mode: bool,
    query_profile: dict[str, Any],
    runtime_state: Any,
    planned_tools: list[PlannedToolCall],
    tool_results: list[ToolExecution],
    doc_data: dict[str, Any],
    timings: dict[str, Any],
    llm_stats: dict[str, Any],
    answer_guardrail_mode: dict[str, Any],
    degraded_to_retrieval: bool,
    degrade_reason: str,
    wall_clock_seconds: float,
    planning_seconds: float,
    tool_execution_seconds: float,
    llm_seconds: float,
    response_finalize_seconds: float,
    get_llm_profile: Callable[[str], tuple[Any, Any, Any, Any]],
    get_resolved_query_state_from_runtime: Callable[[Any], dict[str, Any]],
    serialize_router_decision: Callable[[Any], dict[str, Any]],
    serialize_execution_plan: Callable[[Any], dict[str, Any]],
    serialize_post_retrieval_assessment: Callable[[Any], dict[str, Any]],
    get_planner_snapshot_from_runtime: Callable[[Any], dict[str, Any]],
    normalize_media_filter_map: Callable[[Any], dict[str, Any]],
    get_lookup_mode_from_state: Callable[[dict[str, Any]], str],
    compute_classification_conformance: Callable[..., dict[str, Any]],
    get_query_type: Callable[..., str],
    confidence_high: float,
    confidence_medium: float,
    now_iso: Callable[[], str],
) -> dict[str, Any]:
    _, llm_model, _, _ = get_llm_profile(backend)
    planning_artifact = getattr(runtime_state, "planning_artifact", None)
    execution_artifact = getattr(runtime_state, "execution_artifact", None)
    resolved_query_state = dict(getattr(planning_artifact, "resolved_query_state", {}) or {})
    if not resolved_query_state:
        resolved_query_state = get_resolved_query_state_from_runtime(runtime_state)
    serialized_decision = serialize_router_decision(runtime_state.decision)
    serialized_plan = serialize_execution_plan(runtime_state.execution_plan)
    decision_category = str(getattr(planning_artifact, "decision_category", "") or "")
    decision_path = list(getattr(planning_artifact, "decision_path", []) or [])
    vector_batches = list(doc_data.get("vector_batches") or [])
    vector_candidates = sum(len(batch.get("results") or []) for batch in vector_batches if isinstance(batch, dict))
    executed_tool_depth = sum(1 for item in tool_results if str(item.status or "").strip().lower() != "skipped")
    media_validation = dict(getattr(execution_artifact, "media_validation", {}) or {})
    candidate_source_breakdown = dict(getattr(execution_artifact, "candidate_source_breakdown", {}) or {})
    media_timing_breakdown = dict(getattr(execution_artifact, "media_timing_breakdown", {}) or {})
    media_layer_breakdown = dict(getattr(execution_artifact, "layer_breakdown", {}) or {})
    alias_resolution = dict(getattr(execution_artifact, "alias_resolution", {}) or {})
    streaming_plan = dict(getattr(execution_artifact, "streaming_plan", {}) or {})
    planner_snapshot = dict(getattr(planning_artifact, "planner_snapshot", {}) or {})
    planner_timing_breakdown = dict(getattr(planning_artifact, "planning_timing_breakdown", {}) or {})
    if not planner_snapshot:
        planner_snapshot = get_planner_snapshot_from_runtime(runtime_state)
    if not planner_timing_breakdown and isinstance(planner_snapshot.get("timing_breakdown"), dict):
        planner_timing_breakdown = dict(planner_snapshot.get("timing_breakdown") or {})
    planner_timing_breakdown = _clean_second_map(
        {
            **planner_timing_breakdown,
            **{key: value for key, value in timings.items() if str(key).strip().endswith("_seconds")},
        }
    )
    execution_plan = serialized_plan
    router_decision = serialized_decision
    router_evidence = router_decision.get("evidence") if isinstance(router_decision.get("evidence"), dict) else {}
    fallback_evidence = dict(getattr(execution_artifact, "fallback_evidence", {}) or {})
    if not fallback_evidence:
        fallback_evidence = serialize_post_retrieval_assessment(runtime_state.post_retrieval_assessment)
    guardrail_flags = dict(getattr(execution_artifact, "guardrail_flags", {}) or {})
    error_taxonomy = dict(getattr(execution_artifact, "error_taxonomy", {}) or {})
    working_set = dict(getattr(execution_artifact, "working_set", {}) or {})
    conversation_state_after = dict(runtime_state.context_resolution.conversation_state_after)
    if working_set:
        conversation_state_after["working_set"] = working_set
    query_semantic_filters = normalize_media_filter_map(router_evidence.get("semantic_filters"))
    query_execution_filters = normalize_media_filter_map(resolved_query_state.get("filters"))
    execution_filter_warnings = [
        str(item).strip()
        for item in (router_evidence.get("execution_filter_warnings") or [])
        if str(item).strip()
    ]
    router_alias_resolution = router_evidence.get("router_alias_resolution") if isinstance(router_evidence.get("router_alias_resolution"), dict) else {}
    router_alias_terms = [str(item).strip() for item in list(router_alias_resolution.get("canonical_terms") or []) if str(item).strip()][:8]
    router_alias_matches = [str(item).strip() for item in list(router_alias_resolution.get("matched_terms") or []) if str(item).strip()][:8]
    router = {
        "selected_tool": planned_tools[0].name if planned_tools else "",
        "planned_tools": [call.name for call in planned_tools],
        "resolved_question": str(runtime_state.context_resolution.resolved_question or ""),
        "domain": str(router_decision.get("domain") or "general"),
        "lookup_mode": get_lookup_mode_from_state(resolved_query_state),
        "decision_intent": str(router_decision.get("intent") or "knowledge_qa"),
        "decision_confidence": router_decision.get("confidence"),
        "decision_category": decision_category,
        "decision_path": decision_path,
        "planned_tool_depth": len(planned_tools),
        "executed_tool_depth": executed_tool_depth,
        "classifier_label": str(runtime_state.llm_media.get("label", "") or ""),
        "doc_similarity": fallback_evidence.get("doc_similarity", {}).get("score"),
        "media_entity_confident": bool(runtime_state.decision.entities),
        "entity_hit_count": len(list(runtime_state.decision.entities or [])),
        "alias_resolution_terms": router_alias_terms,
        "alias_matched_terms": router_alias_matches,
        "followup_target": str(runtime_state.decision.followup_target or ""),
        "rewritten_queries": {str(key): str(value) for key, value in (runtime_state.decision.rewritten_queries or {}).items() if str(key).strip() and str(value).strip()},
        "short_media_surface": bool(runtime_state.decision.evidence.get("profile") == "short" and runtime_state.decision.domain == "media"),
        "policy_reasons": list(execution_plan.get("reasons") or []),
        "fallback_evidence": fallback_evidence,
    }
    retrieval = {
        "vector_hits": len(list(doc_data.get("results") or [])),
        "vector_candidates": vector_candidates,
        "similarity_threshold": timings.get("doc_score_threshold"),
        "top1_score_before_rerank": doc_data.get("doc_top1_score_before_rerank"),
        "top1_score_after_rerank": doc_data.get("doc_top1_score"),
        "query_rewrite_status": str(doc_data.get("query_rewrite", {}).get("status", "") or ""),
        "query_rewrite_count": len(list(doc_data.get("query_rewrite", {}).get("queries") or [])),
        "graph_expansion_batches": 0,
        "raw_candidates_count": int(media_validation.get("raw_candidates_count", 0) or 0),
        "post_filter_count": int(media_validation.get("post_filter_count", 0) or 0),
        "returned_result_count": int(media_validation.get("returned_result_count", 0) or 0),
        "mention_result_count": int(media_validation.get("mention_result_count", media_layer_breakdown.get("mention_count", 0)) or 0),
        "dropped_by_validator": int(media_validation.get("dropped_by_validator", 0) or 0),
        "dropped_by_reference_limit": int(media_validation.get("dropped_by_reference_limit", 0) or 0),
        "validator_drop_reasons": media_validation.get("drop_reasons", {}),
        "reference_limit_drop_reasons": media_validation.get("reference_limit_drop_reasons", {}),
        "candidate_source_breakdown": candidate_source_breakdown,
        "media_timing_breakdown": {str(key): round(float(value or 0), 6) for key, value in media_timing_breakdown.items() if str(key).strip()},
        "layer_breakdown": {
            "strict_scope_active": bool(media_layer_breakdown.get("strict_scope_active")),
            "main_count": int(media_layer_breakdown.get("main_count", media_validation.get("returned_result_count", 0)) or 0),
            "mention_count": int(media_layer_breakdown.get("mention_count", 0) or 0),
            "excluded_count": int(media_layer_breakdown.get("excluded_count", 0) or 0),
        },
        "alias_resolution_count": max(
            int(alias_resolution.get("count", 0) or len(alias_resolution.get("entries") or []) or 0),
            int(router_alias_resolution.get("count", 0) or 0),
        ),
        "alias_hit_result_count": int(alias_resolution.get("hit_result_count", 0) or len(alias_resolution.get("hits") or []) or 0),
        "alias_resolution_terms": [
            term
            for term in [
                *router_alias_terms,
                *[
                    str(item.get("canonical") or item.get("canonical_name") or item.get("raw_value") or "").strip()
                    for item in (
                        alias_resolution.get("expanded_terms")
                        if isinstance(alias_resolution.get("expanded_terms"), list)
                        else (alias_resolution.get("entries") if isinstance(alias_resolution.get("entries"), list) else [])
                    )
                    if isinstance(item, dict) and str(item.get("canonical") or item.get("canonical_name") or item.get("raw_value") or "").strip()
                ],
            ]
            if term
        ][:8],
    }
    ranking = {
        "method": str(doc_data.get("rerank", {}).get("method", "") or ""),
        "rerank_k": len(list(doc_data.get("results") or [])),
        "top1_identity_changed": doc_data.get("doc_top1_identity_changed"),
        "top1_rank_shift": doc_data.get("doc_top1_rank_shift"),
    }
    tools = []
    for item in tool_results:
        data = item.data if isinstance(item.data, dict) else {}
        results = get_media_main_result_rows_from_data(data) if item.tool == TOOL_QUERY_MEDIA else (data.get("results") if isinstance(data.get("results"), list) else [])
        mention_results = get_media_mention_rows_from_data(data) if item.tool == TOOL_QUERY_MEDIA else []
        source_counts = data.get("source_counts") if isinstance(data.get("source_counts"), dict) else {}
        per_item_source = str(data.get("per_item_source", "") or "")
        display_name = item.tool
        if source_counts:
            ordered_sources = ", ".join(
                f"{name}:{count}"
                for name, count in sorted(
                    ((str(k), int(v or 0)) for k, v in source_counts.items() if str(k).strip()),
                    key=lambda pair: (-pair[1], pair[0]),
                )
            )
            display_name = f"{item.tool} [{ordered_sources}]"
        elif per_item_source:
            display_name = f"{item.tool} [{per_item_source}]"
        tools.append(
            {
                "name": item.tool,
                "display_name": display_name,
                "status": item.status,
                "latency_ms": data.get("latency_ms"),
                "result_count": len(results),
                "mention_count": len(mention_results),
                "cache_hit": bool(data.get("cache_hit")),
                "trace_stage": str(data.get("trace_stage", "") or ""),
                "per_item_source": per_item_source,
                "source_counts": dict(source_counts),
                "mixed_sources": bool(data.get("mixed_sources")),
            }
        )

    planner_breakdown = {
        "session_prepare_seconds": timings.get("session_prepare_seconds", 0.0),
        "query_profile_seconds": timings.get("query_profile_seconds", 0.0),
        "tool_planning_seconds": timings.get("tool_planning_seconds", 0.0),
    }
    router_breakdown = {
        "router_prompt_build_seconds": planner_timing_breakdown.get("router_prompt_build_seconds", 0.0),
        "router_llm_request_seconds": planner_timing_breakdown.get("router_llm_request_seconds", 0.0),
        "router_llm_response_seconds": planner_timing_breakdown.get("router_llm_response_seconds", 0.0),
        "router_llm_classification_seconds": planner_timing_breakdown.get("router_llm_classification_seconds", 0.0),
        "router_entity_resolution_seconds": planner_timing_breakdown.get("router_entity_resolution_seconds", 0.0),
        "router_alias_resolution_seconds": planner_timing_breakdown.get("router_alias_resolution_seconds", 0.0),
        "router_followup_resolution_seconds": planner_timing_breakdown.get("router_followup_resolution_seconds", 0.0),
        "router_semantic_repairs_seconds": planner_timing_breakdown.get("router_semantic_repairs_seconds", 0.0),
        "router_rewrite_prompt_build_seconds": planner_timing_breakdown.get("router_rewrite_prompt_build_seconds", 0.0),
        "router_rewrite_llm_request_seconds": planner_timing_breakdown.get("router_rewrite_llm_request_seconds", 0.0),
        "router_rewrite_llm_response_seconds": planner_timing_breakdown.get("router_rewrite_llm_response_seconds", 0.0),
        "router_llm_rewrite_seconds": planner_timing_breakdown.get("router_llm_rewrite_seconds", 0.0),
        "router_non_llm_seconds": planner_timing_breakdown.get("router_non_llm_seconds", 0.0),
    }
    router_total_seconds = float(planner_timing_breakdown.get("router_decision_seconds", 0.0) or _sum_seconds(router_breakdown))
    planner_llm_stats = _build_planner_llm_stats(backend, llm_model, planner_timing_breakdown)
    router_section = _build_section(
        total_seconds=router_total_seconds,
        breakdown=router_breakdown,
        llm_stats=planner_llm_stats,
    )
    tool_planning_total_seconds = float(planner_timing_breakdown.get("tool_planning_seconds", timings.get("tool_planning_seconds", 0.0)) or 0.0)
    tool_planning_other_seconds = max(
        0.0,
        tool_planning_total_seconds
        - router_total_seconds
        - float(planner_timing_breakdown.get("execution_plan_shape_seconds", 0.0) or 0.0)
        - float(planner_timing_breakdown.get("query_classification_finalize_contract_seconds", 0.0) or 0.0),
    )
    tool_planning_section = _build_section(
        total_seconds=tool_planning_total_seconds,
        breakdown={
            "router_seconds": router_total_seconds,
            "execution_plan_shape_seconds": planner_timing_breakdown.get("execution_plan_shape_seconds", 0.0),
            "query_classification_finalize_contract_seconds": planner_timing_breakdown.get("query_classification_finalize_contract_seconds", 0.0),
            "tool_planning_other_seconds": tool_planning_other_seconds,
        },
        llm_stats=planner_llm_stats,
        children={"router": router_section},
    )
    planner_section = _build_section(
        total_seconds=planning_seconds,
        breakdown=planner_breakdown,
        llm_stats=planner_llm_stats,
        accounting_basis="planner.total_seconds = session_prepare + query_profile + tool_planning",
        children={"tool_planning": tool_planning_section},
    )

    execution_breakdown = {
        "tool_execution_seconds": tool_execution_seconds,
        "vector_recall_seconds": timings.get("vector_recall_seconds", 0.0),
        "rerank_seconds": timings.get("rerank_seconds", 0.0),
        "reference_limit_seconds": timings.get("reference_limit_seconds", 0.0),
        "per_item_expansion_seconds": timings.get("per_item_expansion_seconds", 0.0),
        "post_retrieval_fallback_seconds": timings.get("post_retrieval_fallback_seconds", 0.0),
        "post_retrieval_evaluate_seconds": timings.get("post_retrieval_evaluate_seconds", 0.0),
        "post_retrieval_repairs_seconds": timings.get("post_retrieval_repairs_seconds", 0.0),
        "web_search_seconds": timings.get("web_search_seconds", 0.0),
    }
    execution_total_seconds = max(tool_execution_seconds, _sum_seconds(execution_breakdown))
    execution_section = _build_section(
        total_seconds=execution_total_seconds,
        breakdown=execution_breakdown,
        accounting_basis="execution.total_seconds tracks execution-stage wall time, including tool calls and post-tool expansion/repair work",
    )

    answer_breakdown = {
        "context_assembly_seconds": timings.get("context_assembly_seconds", 0.0),
        "answer_strategy_seconds": timings.get("answer_strategy_seconds", 0.0),
        "guardrail_mode_seconds": timings.get("guardrail_mode_seconds", 0.0),
        "structured_context_build_seconds": timings.get("structured_context_build_seconds", 0.0),
        "response_section_compose_seconds": timings.get("response_section_compose_seconds", 0.0),
        "prompt_render_seconds": timings.get("prompt_render_seconds", 0.0),
        "llm_seconds": llm_seconds,
        "final_guardrail_apply_seconds": timings.get("final_guardrail_apply_seconds", 0.0),
        "mention_append_seconds": timings.get("mention_append_seconds", 0.0),
        "response_finalize_seconds": response_finalize_seconds,
        "references_markdown_seconds": timings.get("references_markdown_seconds", 0.0),
    }
    answer_total_seconds = _sum_seconds(answer_breakdown)
    answer_llm_stats = _build_answer_llm_stats(backend, llm_model, llm_seconds, llm_stats)
    answer_section = _build_section(
        total_seconds=answer_total_seconds,
        breakdown=answer_breakdown,
        llm_stats=answer_llm_stats,
        accounting_basis="answer.total_seconds is additive across answer-phase leaves",
    )

    observability_breakdown = {
        "session_persist_seconds": timings.get("session_persist_seconds", 0.0),
        "metrics_record_seconds": timings.get("metrics_record_seconds", 0.0),
    }
    observability_total_seconds = _sum_seconds(observability_breakdown)
    observability_section = _build_section(
        total_seconds=observability_total_seconds,
        breakdown=observability_breakdown,
    )

    accounted_section_seconds = planning_seconds + execution_total_seconds + answer_total_seconds + observability_total_seconds
    unaccounted_seconds = max(0.0, float(wall_clock_seconds or 0) - accounted_section_seconds)
    system_section = _build_section(
        total_seconds=wall_clock_seconds,
        breakdown={
            "accounted_section_seconds": accounted_section_seconds,
            "unaccounted_seconds": unaccounted_seconds,
        },
        accounting_basis="wall_clock = planner + execution + answer + observability + unaccounted",
    )

    stages = _build_aggregate_stage_timings(
        planning_seconds=planning_seconds,
        execution_seconds=execution_total_seconds,
        answer_seconds=answer_total_seconds,
        observability_seconds=observability_total_seconds,
        unaccounted_seconds=unaccounted_seconds,
        wall_clock_seconds=wall_clock_seconds,
    )

    return {
        "schema_version": TRACE_SCHEMA_VERSION,
        "trace_id": trace_id,
        "timestamp": now_iso(),
        "entrypoint": "agent",
        "call_type": "benchmark_case" if benchmark_mode else ("chat_stream" if stream_mode else "chat"),
        "session_id": session_id,
        "search_mode": search_mode,
        "conversation_state_before": dict(runtime_state.context_resolution.conversation_state_before),
        "detected_followup": bool(runtime_state.context_resolution.detected_followup),
        "inheritance_applied": dict(runtime_state.context_resolution.inheritance_applied),
        "conversation_state_after": conversation_state_after,
        "state_diff": dict(runtime_state.context_resolution.state_diff),
        "working_set": working_set,
        "query_understanding": {
            "original_question": str(runtime_state.decision.raw_question or ""),
            "resolved_question": str(runtime_state.context_resolution.resolved_question or ""),
            "domain": str(router_decision.get("domain") or "general"),
            "decision_intent": str(router_decision.get("intent") or "knowledge_qa"),
            "lookup_mode": get_lookup_mode_from_state(resolved_query_state),
            "confidence": router_decision.get("confidence"),
            "selection": normalize_media_filter_map(resolved_query_state.get("selection")),
            "time_constraint": dict(resolved_query_state.get("time_constraint") or {}),
            "ranking": dict(resolved_query_state.get("ranking") or {}),
            "entities": list(runtime_state.decision.entities or []),
            "followup_target": str(runtime_state.decision.followup_target or ""),
            "needs_comparison": bool(runtime_state.decision.needs_comparison),
            "needs_explanation": bool(runtime_state.decision.needs_explanation),
            "metadata_anchors": list(getattr(runtime_state.decision, "metadata_anchors", []) or []),
            "scope_anchors": list(getattr(runtime_state.decision, "scope_anchors", []) or []),
            "rewritten_queries": {str(key): str(value) for key, value in (runtime_state.decision.rewritten_queries or {}).items() if str(key).strip() and str(value).strip()},
            "router_alias_resolution": {
                "count": int(router_alias_resolution.get("count", 0) or 0),
                "matched_terms": router_alias_matches,
                "canonical_terms": router_alias_terms,
            },
            "semantic_filters": query_semantic_filters,
            "execution_filters": query_execution_filters,
            "filters": query_execution_filters,
            "date_range": resolved_query_state.get("date_range", []),
            "inherited_context": resolved_query_state.get("inherited_context", {}),
            "carry_over_from_previous_turn": bool(resolved_query_state.get("carry_over_from_previous_turn")),
            "retrieval_plan": [call.name for call in planned_tools],
            "reasons": list(router_decision.get("reasons") or []),
            "arbitration": str(router_decision.get("arbitration") or "unknown"),
            "schema_repairs": list(router_evidence.get("schema_repairs") or []),
            "execution_filter_warnings": execution_filter_warnings,
            "classification_conformance": compute_classification_conformance(
                raw_question=str(runtime_state.decision.raw_question or ""),
                actual_domain=str(router_decision.get("domain") or "general"),
                actual_arbitration=str(router_decision.get("arbitration") or "unknown"),
            ),
        },
        "planner_snapshot": planner_snapshot,
        "execution_plan": execution_plan,
        "guardrail_flags": guardrail_flags,
        "error_taxonomy": error_taxonomy,
        "answer_guardrail_mode": answer_guardrail_mode,
        "query_type": str(runtime_state.decision.query_type or QUERY_TYPE_GENERAL),
        "query_profile": {
            "profile": str(query_profile.get("profile", "medium") or "medium"),
            "token_count": int(query_profile.get("token_count", 0) or 0),
        },
        "router": router,
        "tools": tools,
        "retrieval": retrieval,
        "ranking": ranking,
        "streaming_plan": streaming_plan,
        "planner": planner_section,
        "execution": execution_section,
        "answer": answer_section,
        "observability": observability_section,
        "system": system_section,
        "llm": {
            "contract_version": TRACE_SECTION_CONTRACT_VERSION,
            "planner": planner_llm_stats,
            "answer": answer_llm_stats,
            "total": _build_total_llm_stats(planner_llm_stats, answer_llm_stats),
        },
        "stages": stages,
        "timing_groups": _group_stage_timings(stages),
        "timing_accounting": {
            "basis": "section_totals_plus_unaccounted",
            "accounted_seconds": _round_stage_seconds(accounted_section_seconds),
            "unaccounted_seconds": _round_stage_seconds(unaccounted_seconds),
            "wall_clock_seconds": _round_stage_seconds(wall_clock_seconds),
        },
        "total_elapsed_seconds": round(float(wall_clock_seconds or 0), 6),
        "result": {
            "status": "ok",
            "no_context": int(timings.get("no_context", 0) or 0),
            "no_context_reason": str(timings.get("no_context_reason", "") or ""),
            "degraded_to_retrieval": bool(degraded_to_retrieval),
            "degrade_reason": str(degrade_reason or ""),
        },
    }
