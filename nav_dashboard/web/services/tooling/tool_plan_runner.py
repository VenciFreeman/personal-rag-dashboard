from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from ..agent.agent_types import AgentRuntimeState, PlannedToolCall, QueryProfile, QuotaState, ToolExecution
from ..agent.guardrail_flags_owner import GuardrailFlagDeps
from ..retrieval.post_retrieval_repairs_owner import PostRetrievalRepairDeps
from ..retrieval.result_layering_pipeline import run_result_layering_pipeline


@dataclass
class ToolPlanExecutionDeps:
    apply_reference_limits: Callable[..., Any]
    execute_per_item_expansion: Callable[..., Any]
    update_post_retrieval_fallback_evidence: Callable[..., Any]
    log_no_context_query: Callable[..., Any]
    post_retrieval_repair_owner_deps: PostRetrievalRepairDeps
    guardrail_flag_owner_deps: GuardrailFlagDeps
    build_guardrail_answer_mode: Callable[..., Any]
    log_agent_media_miss: Callable[..., Any]
    increment_quota_state: Callable[..., Any]
    resolve_agent_no_context: Callable[..., Any]
    get_query_type: Callable[..., str]
    get_planner_snapshot_from_runtime: Callable[..., dict[str, Any]]
    get_resolved_query_state_from_runtime: Callable[..., dict[str, Any]]


@dataclass
class ToolPlanRunResult:
    allowed_plan: list[PlannedToolCall]
    skipped_due_quota: list[str]
    executed_tool_results: list[ToolExecution]
    tool_results: list[ToolExecution]
    tool_execution_seconds: float
    query_profile: QueryProfile
    answer_mode: dict[str, Any]
    answer_strategy: dict[str, Any]
    post_retrieval_outcome: dict[str, Any]
    guardrail_flags: dict[str, Any]
    doc_data: Any
    doc_metrics: dict[str, Any]
    usage_metrics: dict[str, Any]
    reference_limit_seconds: float
    per_item_expansion_seconds: float
    fallback_evidence_seconds: float
    post_retrieval_evaluate_seconds: float
    post_retrieval_repairs_seconds: float
    answer_strategy_seconds: float
    guardrail_mode_seconds: float


def execute_tool_plan(
    *,
    planned: list[PlannedToolCall],
    exceeded: list[dict[str, Any]],
    deny_over_quota: bool,
    benchmark_mode: bool,
    session_id: str,
    question: str,
    query_profile: QueryProfile,
    trace_id: str,
    normalized_search_mode: str,
    quota_state: QuotaState,
    query_classification: dict[str, Any],
    runtime_state: AgentRuntimeState,
    resolve_allowed_plan: Callable[..., tuple[list[PlannedToolCall], list[str]]],
    execute_tool_phase: Callable[..., Any],
    execute_tool_plan_boundary: Any,
    deps: ToolPlanExecutionDeps,
    allowed_plan: list[PlannedToolCall] | None = None,
    skipped_due_quota: list[str] | None = None,
    defer_per_item_expansion: bool = False,
) -> ToolPlanRunResult:
    if allowed_plan is None or skipped_due_quota is None:
        allowed_plan, skipped_due_quota = resolve_allowed_plan(
            planned,
            exceeded,
            deny_over_quota=deny_over_quota,
        )
    tool_phase = execute_tool_phase(
        allowed_plan=allowed_plan,
        query_profile=query_profile,
        trace_id=trace_id,
        execute_tool_plan=execute_tool_plan_boundary,
    )
    executed_tool_results = list(tool_phase.tool_results)
    # Convert QuotaState to dict for mutable functions like increment_quota_state
    quota_state_dict = quota_state.to_dict() if hasattr(quota_state, "to_dict") else dict(quota_state)
    layering = run_result_layering_pipeline(
        question=question,
        trace_id=trace_id,
        normalized_search_mode=normalized_search_mode,
        benchmark_mode=benchmark_mode,
        quota_state=quota_state_dict,
        defer_per_item_expansion=defer_per_item_expansion,
        allowed_plan=allowed_plan,
        tool_results=executed_tool_results,
        query_profile=query_profile,
        query_classification=query_classification,
        runtime_state=runtime_state,
        apply_reference_limits=deps.apply_reference_limits,
        execute_per_item_expansion=deps.execute_per_item_expansion,
        update_post_retrieval_fallback_evidence=deps.update_post_retrieval_fallback_evidence,
        log_no_context_query=deps.log_no_context_query,
        post_retrieval_repair_owner_deps=deps.post_retrieval_repair_owner_deps,
        guardrail_flag_owner_deps=deps.guardrail_flag_owner_deps,
        build_guardrail_answer_mode=deps.build_guardrail_answer_mode,
        log_agent_media_miss=deps.log_agent_media_miss,
        increment_quota_state=deps.increment_quota_state,
        resolve_agent_no_context=deps.resolve_agent_no_context,
        get_query_type=deps.get_query_type,
        get_planner_snapshot_from_runtime=deps.get_planner_snapshot_from_runtime,
        get_resolved_query_state_from_runtime=deps.get_resolved_query_state_from_runtime,
    )
    tool_results = list(layering.tool_results)
    if skipped_due_quota:
        for tool_name in skipped_due_quota:
            tool_results.append(
                ToolExecution(tool=tool_name, status="skipped", summary="超过每日配额且已拒绝调用", data={"results": []})
            )
    return ToolPlanRunResult(
        allowed_plan=list(allowed_plan),
        skipped_due_quota=list(skipped_due_quota),
        executed_tool_results=executed_tool_results,
        tool_results=tool_results,
        tool_execution_seconds=float(tool_phase.tool_execution_seconds or 0.0),
        query_profile=QueryProfile.from_mapping(layering.query_profile),
        answer_mode=layering.answer_mode,
        answer_strategy=layering.answer_strategy,
        post_retrieval_outcome=dict(query_classification.get("post_retrieval_outcome") or {}),
        guardrail_flags=dict(query_classification.get("guardrail_flags") or {}),
        doc_data=layering.doc_data,
        doc_metrics=dict(layering.doc_metrics or {}),
        usage_metrics=dict(layering.usage_metrics or {}),
        reference_limit_seconds=float(layering.reference_limit_seconds or 0.0),
        per_item_expansion_seconds=float(layering.per_item_expansion_seconds or 0.0),
        fallback_evidence_seconds=float(layering.fallback_evidence_seconds or 0.0),
        post_retrieval_evaluate_seconds=float(layering.post_retrieval_evaluate_seconds or 0.0),
        post_retrieval_repairs_seconds=float(layering.post_retrieval_repairs_seconds or 0.0),
        answer_strategy_seconds=float(layering.answer_strategy_seconds or 0.0),
        guardrail_mode_seconds=float(layering.guardrail_mode_seconds or 0.0),
    )


def update_debug_trace_with_tool_results(debug_trace: dict[str, Any], tool_results: list[ToolExecution]) -> None:
    debug_trace["tool_results"] = [
        {"tool": item.tool, "status": item.status, "summary": item.summary, "data": item.data}
        for item in tool_results
    ]


def update_debug_trace_with_query_rewrite(debug_trace: dict[str, Any], doc_data: Any) -> None:
    if isinstance(doc_data, dict):
        debug_trace["query_rewrite"] = doc_data.get("query_rewrite", {})


def update_debug_trace_with_tool_plan(debug_trace: dict[str, Any], tool_plan: ToolPlanRunResult) -> None:
    update_debug_trace_with_tool_results(debug_trace, tool_plan.tool_results)
    update_debug_trace_with_query_rewrite(debug_trace, tool_plan.doc_data)