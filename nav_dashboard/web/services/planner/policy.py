from __future__ import annotations

from typing import Any, Callable

from ..agent.agent_types import AgentRuntimeState, ExecutionPlan, PlannedToolCall, PlanningArtifact, RouterContextResolution, RouterDecision
from .domain import ExecutionPlanShapingResult, RouterDecisionNormalizationResult


def shape_execution_plan(
    *,
    question: str,
    history: list[dict[str, str]],
    search_mode: str,
    normalization: RouterDecisionNormalizationResult,
    build_plan: Callable[[RouterDecision, str], ExecutionPlan],
    resolve_router_context: Callable[[str, list[dict[str, str]], RouterDecision, dict[str, Any], list[PlannedToolCall]], RouterContextResolution],
    assemble_tool_options: Callable[[list[PlannedToolCall], RouterDecision], list[PlannedToolCall]],
    serialize_post_retrieval_assessment: Callable[[Any], dict[str, Any]],
) -> ExecutionPlanShapingResult:
    base_plan = build_plan(normalization.router_decision, search_mode)
    planned_tools = assemble_tool_options(list(base_plan.planned_tools), normalization.router_decision)
    context_resolution = resolve_router_context(
        question,
        history,
        normalization.router_decision,
        normalization.previous_context_state,
        planned_tools,
    )
    resolved_question = context_resolution.resolved_question
    tmdb_query_override = str((normalization.router_decision.rewritten_queries or {}).get("tmdb_query") or "").strip()
    normalized_calls: list[PlannedToolCall] = []
    for index, call in enumerate(planned_tools):
        normalized_query = call.query
        if call.name == "search_tmdb_media":
            if tmdb_query_override:
                normalized_query = tmdb_query_override
            elif not str(call.query or "").strip() and resolved_question:
                normalized_query = resolved_question
        normalized_calls.append(
            PlannedToolCall(
                name=call.name,
                query=normalized_query,
                options=dict(call.options or {}),
                plan_index=index,
            )
        )
    execution_plan = ExecutionPlan(
        decision=base_plan.decision,
        planned_tools=normalized_calls,
        primary_tool=normalized_calls[0].name if normalized_calls else "",
        fallback_tools=[call.name for call in normalized_calls[1:]],
        reasons=list(base_plan.reasons or []),
    )
    runtime_state = AgentRuntimeState(
        decision=normalization.router_decision,
        execution_plan=execution_plan,
        context_resolution=context_resolution,
        llm_media=normalization.llm_media,
        previous_context_state=dict(normalization.previous_context_state or {}),
        planning_artifact=PlanningArtifact(
            decision=normalization.router_decision,
            execution_plan=execution_plan,
            context_resolution=context_resolution,
            planner_snapshot=dict(context_resolution.planner_snapshot or {}),
            resolved_query_state=dict(context_resolution.resolved_query_state or {}),
            planning_timing_breakdown=dict(context_resolution.planning_timing_breakdown or {}),
            metadata_anchors=list(normalization.router_decision.metadata_anchors or []),
            scope_anchors=list(normalization.router_decision.scope_anchors or []),
        ),
    )
    return ExecutionPlanShapingResult(
        execution_plan=execution_plan,
        planned_tools=normalized_calls,
        context_resolution=context_resolution,
        runtime_state=runtime_state,
        resolved_question=resolved_question,
    )