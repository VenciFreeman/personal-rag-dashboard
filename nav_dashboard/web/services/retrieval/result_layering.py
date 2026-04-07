from __future__ import annotations

from typing import Any, Callable

from ..agent.agent_types import AgentRuntimeState, PlannedToolCall, ToolExecution
from ..agent.guardrail_flags_owner import GuardrailFlagDeps
from .post_retrieval_repairs_owner import PostRetrievalRepairDeps
from .result_layering_contract import ResultLayeringOutcome
from .result_layering_pipeline import run_result_layering_pipeline


def process_tool_results(
    *,
    question: str,
    trace_id: str,
    normalized_search_mode: str,
    benchmark_mode: bool,
    quota_state: dict[str, Any],
    defer_per_item_expansion: bool = False,
    allowed_plan: list[PlannedToolCall],
    tool_results: list[ToolExecution],
    query_profile: dict[str, Any],
    query_classification: dict[str, Any],
    runtime_state: AgentRuntimeState,
    apply_reference_limits: Callable[[list[ToolExecution], str, dict[str, Any]], list[ToolExecution]],
    execute_per_item_expansion: Callable[..., list[ToolExecution]],
    update_post_retrieval_fallback_evidence: Callable[[AgentRuntimeState, dict[str, Any], list[ToolExecution], dict[str, Any], dict[str, Any]], None],
    log_no_context_query: Callable[..., Any],
    post_retrieval_repair_owner_deps: PostRetrievalRepairDeps,
    guardrail_flag_owner_deps: GuardrailFlagDeps,
    build_guardrail_answer_mode: Callable[[str, AgentRuntimeState, list[ToolExecution], dict[str, bool]], dict[str, Any]],
    log_agent_media_miss: Callable[[str, dict[str, Any]], None],
    increment_quota_state: Callable[..., Any],
    resolve_agent_no_context: Callable[[str, int, int], tuple[int, str]],
    get_query_type: Callable[..., str],
    get_planner_snapshot_from_runtime: Callable[[AgentRuntimeState], dict[str, Any]],
    get_resolved_query_state_from_runtime: Callable[[AgentRuntimeState], dict[str, Any]],
) -> ResultLayeringOutcome:
    """Compatibility facade; all apply-side orchestration lives in result_layering_pipeline."""
    return run_result_layering_pipeline(
        question=question,
        trace_id=trace_id,
        normalized_search_mode=normalized_search_mode,
        benchmark_mode=benchmark_mode,
        quota_state=quota_state,
        defer_per_item_expansion=defer_per_item_expansion,
        allowed_plan=allowed_plan,
        tool_results=tool_results,
        query_profile=query_profile,
        query_classification=query_classification,
        runtime_state=runtime_state,
        apply_reference_limits=apply_reference_limits,
        execute_per_item_expansion=execute_per_item_expansion,
        update_post_retrieval_fallback_evidence=update_post_retrieval_fallback_evidence,
        log_no_context_query=log_no_context_query,
        post_retrieval_repair_owner_deps=post_retrieval_repair_owner_deps,
        guardrail_flag_owner_deps=guardrail_flag_owner_deps,
        build_guardrail_answer_mode=build_guardrail_answer_mode,
        log_agent_media_miss=log_agent_media_miss,
        increment_quota_state=increment_quota_state,
        resolve_agent_no_context=resolve_agent_no_context,
        get_query_type=get_query_type,
        get_planner_snapshot_from_runtime=get_planner_snapshot_from_runtime,
        get_resolved_query_state_from_runtime=get_resolved_query_state_from_runtime,
    )