from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Callable

from ..agent_types import AgentRuntimeState, PlannedToolCall, PlanningArtifact, ToolExecution


@dataclass(frozen=True)
class ToolExecutionRuntimeDeps:
    perf_counter: Callable[[], float]
    query_document_rag: Callable[[str, dict[str, Any], str], ToolExecution]
    query_media_record: Callable[[str, dict[str, Any], str, dict[str, Any]], ToolExecution]
    search_web: Callable[[str, str], ToolExecution]
    expand_document_query: Callable[[str], ToolExecution]
    expand_media_query: Callable[[str], ToolExecution]
    search_mediawiki_action: Callable[[str, str], ToolExecution]
    parse_mediawiki_page: Callable[[str, str], ToolExecution]
    expand_mediawiki_concept: Callable[[str, str], ToolExecution]
    search_tmdb_media: Callable[[str, str], ToolExecution]
    search_by_creator: Callable[..., ToolExecution]
    sanitize_media_filters: Callable[[Any], dict[str, Any]]
    tool_execution_factory: Callable[[str, str, str, Any], ToolExecution]
    tool_query_doc_rag: str
    tool_query_media: str
    tool_search_web: str
    tool_expand_doc_query: str
    tool_expand_media_query: str
    tool_search_mediawiki: str
    tool_parse_mediawiki: str
    tool_expand_mediawiki_concept: str
    tool_search_tmdb: str
    tool_search_by_creator: str
    unknown_tool_summary: str = "未知工具"


@dataclass(frozen=True)
class ToolPlanningRuntimeDeps:
    run_tool_planning_pipeline: Callable[..., tuple[Any, Any, dict[str, Any], dict[str, float]]]
    build_router_decision: Callable[..., Any]
    router_decision_to_query_classification: Callable[..., dict[str, Any]]
    build_plan: Callable[..., Any]
    resolve_router_context: Callable[..., Any]
    assemble_tool_options: Callable[[list[PlannedToolCall], Any], list[PlannedToolCall]]
    serialize_router_context_resolution: Callable[..., dict[str, Any]]
    serialize_execution_plan: Callable[..., dict[str, Any]]
    serialize_post_retrieval_assessment: Callable[..., dict[str, Any]]
    get_ontology_load_statuses: Callable[[], Any]
    perf_counter: Callable[[], float]
    build_router_decision_path: Callable[..., tuple[str, list[str]]]
    normalize_timing_breakdown: Callable[[Any], dict[str, float]]
    planning_artifact_factory: Callable[..., PlanningArtifact]


def execute_tool(
    call: PlannedToolCall,
    query_profile: dict[str, Any],
    trace_id: str,
    *,
    deps: ToolExecutionRuntimeDeps,
) -> ToolExecution:
    tool_t0 = deps.perf_counter()
    try:
        if call.name == deps.tool_query_doc_rag:
            result = deps.query_document_rag(call.query, query_profile, trace_id)
        elif call.name == deps.tool_query_media:
            result = deps.query_media_record(call.query, query_profile, trace_id, call.options)
        elif call.name == deps.tool_search_web:
            result = deps.search_web(call.query, trace_id)
        elif call.name == deps.tool_expand_doc_query:
            result = deps.expand_document_query(call.query)
        elif call.name == deps.tool_expand_media_query:
            result = deps.expand_media_query(call.query)
        elif call.name == deps.tool_search_mediawiki:
            result = deps.search_mediawiki_action(call.query, trace_id)
        elif call.name == deps.tool_parse_mediawiki:
            result = deps.parse_mediawiki_page(call.query, trace_id)
        elif call.name == deps.tool_expand_mediawiki_concept:
            result = deps.expand_mediawiki_concept(call.query, trace_id)
        elif call.name == deps.tool_search_tmdb:
            result = deps.search_tmdb_media(call.query, trace_id)
        elif call.name == deps.tool_search_by_creator:
            creator_name = str(call.options.get("creator_name") or call.query or "").strip()
            media_type = str(call.options.get("media_type") or "").strip()
            filters = deps.sanitize_media_filters(call.options.get("filters"))
            date_window = call.options.get("date_window") if isinstance(call.options.get("date_window"), dict) else {}
            sort = str(call.options.get("sort") or "relevance").strip() or "relevance"
            result = deps.search_by_creator(
                creator_name,
                trace_id,
                media_type=media_type,
                filters=filters,
                date_window=date_window,
                sort=sort,
            )
        else:
            result = deps.tool_execution_factory(call.name, "skipped", deps.unknown_tool_summary, {})
        latency_ms = round((deps.perf_counter() - tool_t0) * 1000, 1)
        data = dict(result.data) if isinstance(result.data, dict) else {}
        data.setdefault("latency_ms", latency_ms)
        return deps.tool_execution_factory(result.tool, result.status, result.summary, data)
    except Exception as exc:  # noqa: BLE001
        latency_ms = round((deps.perf_counter() - tool_t0) * 1000, 1)
        return deps.tool_execution_factory(call.name, "error", str(exc), {"results": [], "latency_ms": latency_ms})


def execute_tool_plan(
    calls: list[PlannedToolCall],
    query_profile: dict[str, Any],
    trace_id: str,
    *,
    execute_tool: Callable[[PlannedToolCall, dict[str, Any], str], ToolExecution],
    prefetched_tool_names: set[str] | None = None,
    planned_call_factory: Callable[..., PlannedToolCall] = PlannedToolCall,
) -> list[ToolExecution]:
    if not calls:
        return []
    prefetched_names = set(prefetched_tool_names or set())
    indexed_calls = [
        planned_call_factory(name=call.name, query=call.query, options=dict(call.options or {}), plan_index=index)
        for index, call in enumerate(calls)
    ]
    prefetched = [call for call in indexed_calls if call.name in prefetched_names]
    concurrent = [call for call in indexed_calls if call.name not in prefetched_names]
    results: list[ToolExecution] = []
    for call in prefetched:
        result = execute_tool(call, query_profile, trace_id)
        result.plan_index = int(call.plan_index)
        results.append(result)
    if concurrent:
        with ThreadPoolExecutor(max_workers=max(1, len(concurrent))) as pool:
            future_map = {pool.submit(execute_tool, call, query_profile, trace_id): call for call in concurrent}
            for future in as_completed(future_map):
                result = future.result()
                result.plan_index = int(future_map[future].plan_index)
                results.append(result)
    results.sort(key=lambda item: int(getattr(item, "plan_index", -1) or -1))
    return results


def plan_tool_calls(
    question: str,
    history: list[dict[str, str]],
    backend: str,
    quota_state: dict[str, Any],
    search_mode: str,
    query_profile: dict[str, Any],
    *,
    deps: ToolPlanningRuntimeDeps,
) -> tuple[list[PlannedToolCall], AgentRuntimeState, dict[str, Any]]:
    del backend
    normalization, shaped, query_classification, pipeline_timings = deps.run_tool_planning_pipeline(
        question=question,
        history=history,
        search_mode=search_mode,
        quota_state=quota_state,
        query_profile=query_profile,
        build_router_decision=deps.build_router_decision,
        router_decision_to_query_classification=deps.router_decision_to_query_classification,
        build_plan=deps.build_plan,
        resolve_router_context=deps.resolve_router_context,
        assemble_tool_options=deps.assemble_tool_options,
        serialize_router_context_resolution=deps.serialize_router_context_resolution,
        serialize_execution_plan=deps.serialize_execution_plan,
        serialize_post_retrieval_assessment=deps.serialize_post_retrieval_assessment,
        get_ontology_load_statuses=deps.get_ontology_load_statuses,
    )
    del normalization
    finalize_t0 = deps.perf_counter()
    decision_category, decision_path = deps.build_router_decision_path(
        query_classification=query_classification,
        search_mode=search_mode,
        planned_tools=shaped.runtime_state.execution_plan.planned_tools,
        tool_results=[],
    )
    finalize_seconds = deps.perf_counter() - finalize_t0
    planning_timing_breakdown = deps.normalize_timing_breakdown(
        shaped.runtime_state.context_resolution.planner_snapshot.get("timing_breakdown")
        if isinstance(shaped.runtime_state.context_resolution.planner_snapshot, dict)
        else {}
    )
    planning_timing_breakdown.update(
        deps.normalize_timing_breakdown(
            {
                **dict(pipeline_timings or {}),
                "query_classification_finalize_contract_seconds": finalize_seconds,
                "tool_planning_seconds": sum(float(value or 0.0) for value in dict(pipeline_timings or {}).values()) + finalize_seconds,
            }
        )
    )
    tool_planning_llm_seconds = float(planning_timing_breakdown.get("router_llm_classification_seconds", 0.0) or 0.0) + float(
        planning_timing_breakdown.get("router_llm_rewrite_seconds", 0.0) or 0.0
    )
    tool_planning_seconds = float(planning_timing_breakdown.get("tool_planning_seconds", 0.0) or 0.0)
    planning_timing_breakdown.update(
        deps.normalize_timing_breakdown(
            {
                "tool_planning_llm_seconds": tool_planning_llm_seconds,
                "tool_planning_non_llm_seconds": max(0.0, tool_planning_seconds - tool_planning_llm_seconds),
            }
        )
    )
    shaped.runtime_state.planning_artifact = deps.planning_artifact_factory(
        decision=shaped.runtime_state.decision,
        execution_plan=shaped.runtime_state.execution_plan,
        context_resolution=shaped.runtime_state.context_resolution,
        planner_snapshot={
            **dict(shaped.runtime_state.planning_artifact.planner_snapshot or {}),
            "timing_breakdown": planning_timing_breakdown,
        },
        resolved_query_state=dict(shaped.runtime_state.context_resolution.resolved_query_state or {}),
        planning_timing_breakdown=dict(planning_timing_breakdown),
        decision_category=decision_category,
        decision_path=list(decision_path),
        metadata_anchors=list(shaped.runtime_state.decision.metadata_anchors or []),
        scope_anchors=list(shaped.runtime_state.decision.scope_anchors or []),
    )
    shaped.runtime_state.context_resolution.planning_timing_breakdown = dict(planning_timing_breakdown)
    query_classification = {
        **dict(query_classification or {}),
        "planning_timing_breakdown": planning_timing_breakdown,
    }
    return shaped.planned_tools, shaped.runtime_state, query_classification
