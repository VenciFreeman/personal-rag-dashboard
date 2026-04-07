from __future__ import annotations

from dataclasses import asdict, replace
import time as _time
from typing import Any, Callable

from ..answer.answer_policy import AnswerPolicy
from ..agent.agent_types import AgentRuntimeState, PlannedToolCall, ToolExecution, TOOL_QUERY_DOC_RAG, TOOL_QUERY_MEDIA, TOOL_SEARCH_WEB
from ..agent.guardrail_flags_owner import GuardrailFlagDeps
from ..media.media_answer_input_stage import apply_media_answer_input_side_effects, build_media_answer_inputs
from ..media.media_retrieval_service import MediaPolicyFlags, MediaRetrievalRequest, build_media_retrieval_response, build_media_working_set
from .post_retrieval_repairs_owner import PostRetrievalRepairDeps
from .post_retrieval_repairs_owner import apply_post_retrieval_repairs, reevaluate_post_retrieval
from .post_retrieval_stage import evaluate_post_retrieval_stage
from .result_layering_contract import AnswerInputsBuildResult, PostRetrievalEvaluation, ResultLayeringEvaluation, ResultLayeringOutcome
from .tool_result_preparation import prepare_tool_results


def _apply_post_retrieval_fallback_evidence(
    runtime_state: AgentRuntimeState,
    query_classification: dict[str, Any],
    tool_results: list[ToolExecution],
    doc_data: dict[str, Any],
    query_profile: dict[str, Any],
    update_post_retrieval_fallback_evidence: Callable[..., None],
) -> float:
    fallback_t0 = _time.perf_counter()
    update_post_retrieval_fallback_evidence(
        runtime_state,
        query_classification,
        tool_results,
        doc_data,
        query_profile,
    )
    return _time.perf_counter() - fallback_t0


def _apply_post_retrieval_repairs_and_reevaluate(
    *,
    trace_id: str,
    query_profile: dict[str, Any],
    runtime_state: AgentRuntimeState,
    tool_results: list[ToolExecution],
    post_retrieval: Any,
    repair_owner_deps: PostRetrievalRepairDeps,
) -> tuple[list[ToolExecution], Any, Any, dict[str, Any], float]:
    layered_tool_results = list(tool_results)
    post_retrieval_outcome = post_retrieval.post_retrieval_outcome
    answer_strategy = post_retrieval.proposed_answer_strategy
    guardrail_flags = dict(post_retrieval.guardrail_flags)
    post_retrieval_repairs_seconds = 0.0
    if post_retrieval.proposed_repair_calls:
        repair_results, post_retrieval_repairs_seconds = apply_post_retrieval_repairs(
            post_retrieval.proposed_repair_calls,
            query_profile,
            trace_id,
            deps=repair_owner_deps,
        )
        if repair_results:
            layered_tool_results.extend(repair_results)
            post_retrieval_outcome, guardrail_flags = reevaluate_post_retrieval(
                runtime_state,
                layered_tool_results,
                deps=repair_owner_deps,
            )
            answer_strategy = AnswerPolicy().determine(runtime_state.decision, post_retrieval_outcome)
    return layered_tool_results, post_retrieval_outcome, answer_strategy, guardrail_flags, float(post_retrieval_repairs_seconds or 0.0)


def _write_post_retrieval_classification(
    runtime_state: AgentRuntimeState,
    query_classification: dict[str, Any],
    guardrail_flags: dict[str, Any],
    post_retrieval_outcome: Any,
    answer_strategy: Any,
) -> None:
    post_retrieval_payload = asdict(post_retrieval_outcome)
    answer_strategy_payload = asdict(answer_strategy)
    runtime_state.execution_artifact = replace(
        runtime_state.execution_artifact,
        guardrail_flags=dict(guardrail_flags or {}),
        post_retrieval_outcome=post_retrieval_payload,
        answer_strategy=answer_strategy_payload,
    )
    query_classification["guardrail_flags"] = guardrail_flags
    query_classification["post_retrieval_outcome"] = post_retrieval_payload
    query_classification["answer_strategy"] = answer_strategy_payload


def _write_execution_observability_artifact(
    runtime_state: AgentRuntimeState,
    answer_inputs: Any,
) -> None:
    media_response = answer_inputs.media_response
    request = getattr(media_response, "request", None)
    previous_working_set = dict(getattr(request, "previous_working_set", {}) or {})
    validation = dict(getattr(media_response, "validation", {}) or {})
    layer_breakdown = dict(getattr(media_response, "layer_breakdown", {}) or {})
    media_result_data = dict(getattr(media_response, "media_result_data", {}) or {})
    runtime_state.execution_artifact = replace(
        runtime_state.execution_artifact,
        working_set=previous_working_set,
        media_validation=validation,
        candidate_source_breakdown=dict(media_result_data.get("candidate_source_breakdown") or {}),
        media_timing_breakdown=dict(media_result_data.get("timing_breakdown") or {}),
        layer_breakdown=layer_breakdown,
        alias_resolution=dict(media_result_data.get("alias_resolution") or {}),
    )


def _build_answer_inputs(
    *,
    question: str,
    trace_id: str,
    allowed_plan: list[PlannedToolCall],
    tool_results: list[ToolExecution],
    query_profile: dict[str, Any],
    query_classification: dict[str, Any],
    runtime_state: AgentRuntimeState,
    answer_shape: str,
    media_family: str,
    guardrail_flag_owner_deps: GuardrailFlagDeps,
    build_guardrail_answer_mode: Callable[..., dict[str, Any]],
    resolve_agent_no_context: Callable[[str, int, int], tuple[int, str]],
    get_query_type: Callable[..., str],
    get_planner_snapshot_from_runtime: Callable[[AgentRuntimeState], dict[str, Any]],
    get_resolved_query_state_from_runtime: Callable[[AgentRuntimeState], dict[str, Any]],
    doc_no_context_count: int,
) -> AnswerInputsBuildResult:
    answer_inputs = build_media_answer_inputs(
        question=question,
        trace_id=trace_id,
        allowed_plan=allowed_plan,
        tool_results=tool_results,
        query_profile=query_profile,
        query_classification=query_classification,
        runtime_state=runtime_state,
        answer_shape=answer_shape,
        media_family=media_family,
        media_tool_name=TOOL_QUERY_MEDIA,
        web_tool_name=TOOL_SEARCH_WEB,
        doc_tool_name=TOOL_QUERY_DOC_RAG,
        build_media_working_set=build_media_working_set,
        media_request_factory=MediaRetrievalRequest,
        media_policy_flags_factory=MediaPolicyFlags,
        build_media_retrieval_response=build_media_retrieval_response,
        guardrail_flag_owner_deps=guardrail_flag_owner_deps,
        build_guardrail_answer_mode=build_guardrail_answer_mode,
        resolve_agent_no_context=resolve_agent_no_context,
        get_query_type=get_query_type,
        get_planner_snapshot_from_runtime=get_planner_snapshot_from_runtime,
        get_resolved_query_state_from_runtime=get_resolved_query_state_from_runtime,
        doc_no_context_count=doc_no_context_count,
    )
    return AnswerInputsBuildResult(
        media_response=answer_inputs.media_response,
        answer_mode=dict(answer_inputs.answer_mode or {}),
        guardrail_flags=dict(answer_inputs.guardrail_flags or {}),
        guardrail_mode_seconds=float(answer_inputs.guardrail_mode_seconds or 0.0),
        usage_metrics=dict(answer_inputs.usage_metrics or {}),
        side_effect_requests=answer_inputs.side_effect_requests,
    )


def evaluate_result_layering(
    *,
    question: str,
    trace_id: str,
    normalized_search_mode: str,
    defer_per_item_expansion: bool,
    allowed_plan: list[PlannedToolCall],
    tool_results: list[ToolExecution],
    query_profile: dict[str, Any],
    query_classification: dict[str, Any],
    runtime_state: AgentRuntimeState,
    apply_reference_limits: Callable[[list[ToolExecution], str, dict[str, Any]], list[ToolExecution]],
    execute_per_item_expansion: Callable[..., list[ToolExecution]],
    post_retrieval_repair_owner_deps: PostRetrievalRepairDeps,
    guardrail_flag_owner_deps: GuardrailFlagDeps,
    build_guardrail_answer_mode: Callable[..., dict[str, Any]],
    resolve_agent_no_context: Callable[[str, int, int], tuple[int, str]],
    get_query_type: Callable[..., str],
    get_planner_snapshot_from_runtime: Callable[[AgentRuntimeState], dict[str, Any]],
    get_resolved_query_state_from_runtime: Callable[[AgentRuntimeState], dict[str, Any]],
) -> ResultLayeringEvaluation:
    prepared = prepare_tool_results(
        trace_id=trace_id,
        normalized_search_mode=normalized_search_mode,
        defer_per_item_expansion=defer_per_item_expansion,
        allowed_plan=allowed_plan,
        tool_results=tool_results,
        query_profile=query_profile,
        query_classification=query_classification,
        runtime_state=runtime_state,
        apply_reference_limits=apply_reference_limits,
        execute_per_item_expansion=execute_per_item_expansion,
    )

    post_retrieval = evaluate_post_retrieval_stage(
        tool_results=prepared.tool_results,
        query_profile=prepared.query_profile,
        runtime_state=runtime_state,
        doc_tool_name=TOOL_QUERY_DOC_RAG,
        normalized_search_mode=normalized_search_mode,
        repair_owner_deps=post_retrieval_repair_owner_deps,
    )
    facts = post_retrieval.post_retrieval_facts
    layered_tool_results, post_retrieval_outcome, answer_strategy, guardrail_flags, post_retrieval_repairs_seconds = _apply_post_retrieval_repairs_and_reevaluate(
        trace_id=trace_id,
        query_profile=prepared.query_profile,
        runtime_state=runtime_state,
        tool_results=prepared.tool_results,
        post_retrieval=post_retrieval,
        repair_owner_deps=post_retrieval_repair_owner_deps,
    )
    answer_inputs = _build_answer_inputs(
        question=question,
        trace_id=trace_id,
        allowed_plan=allowed_plan,
        tool_results=layered_tool_results,
        query_profile=prepared.query_profile,
        query_classification=query_classification,
        runtime_state=runtime_state,
        answer_shape=prepared.answer_shape,
        media_family=prepared.media_family,
        guardrail_flag_owner_deps=guardrail_flag_owner_deps,
        build_guardrail_answer_mode=build_guardrail_answer_mode,
        resolve_agent_no_context=resolve_agent_no_context,
        get_query_type=get_query_type,
        get_planner_snapshot_from_runtime=get_planner_snapshot_from_runtime,
        get_resolved_query_state_from_runtime=get_resolved_query_state_from_runtime,
        doc_no_context_count=int(facts.no_context_count or 0),
    )
    return ResultLayeringEvaluation(
        prepared=prepared,
        post_retrieval=PostRetrievalEvaluation(
            tool_results=layered_tool_results,
            facts=facts,
            fallback_evidence_seconds=0.0,
            post_retrieval_outcome=post_retrieval_outcome,
            answer_strategy=answer_strategy,
            post_retrieval_evaluate_seconds=float(post_retrieval.timings.get("post_retrieval_evaluate_seconds", 0.0) or 0.0),
            post_retrieval_repairs_seconds=float(post_retrieval_repairs_seconds or 0.0),
            answer_strategy_seconds=float(post_retrieval.timings.get("answer_strategy_seconds", 0.0) or 0.0),
        ),
        answer_inputs=answer_inputs,
    )


def apply_result_layering_side_effects(
    *,
    benchmark_mode: bool,
    question: str,
    trace_id: str,
    quota_state: dict[str, Any],
    evaluation: ResultLayeringEvaluation,
    query_classification: dict[str, Any],
    runtime_state: AgentRuntimeState,
    update_post_retrieval_fallback_evidence: Callable[..., None],
    log_agent_media_miss: Callable[..., None],
    log_no_context_query: Callable[..., Any],
    increment_quota_state: Callable[..., Any],
) -> float:
    fallback_evidence_seconds = _apply_post_retrieval_fallback_evidence(
        runtime_state,
        query_classification,
        evaluation.prepared.tool_results,
        evaluation.post_retrieval.facts.doc_data,
        evaluation.prepared.query_profile,
        update_post_retrieval_fallback_evidence,
    )
    _write_post_retrieval_classification(
        runtime_state,
        query_classification,
        evaluation.answer_inputs.guardrail_flags,
        evaluation.post_retrieval.post_retrieval_outcome,
        evaluation.post_retrieval.answer_strategy,
    )
    apply_media_answer_input_side_effects(
        benchmark_mode=benchmark_mode,
        question=question,
        query_profile=evaluation.prepared.query_profile,
        quota_state=quota_state,
        side_effect_requests=evaluation.answer_inputs.side_effect_requests,
        log_agent_media_miss=log_agent_media_miss,
        log_no_context_query=log_no_context_query,
        increment_quota_state=increment_quota_state,
    )
    query_classification["answer_guardrail_mode"] = {
        "mode": evaluation.answer_inputs.answer_mode.get("mode", "normal"),
        "reasons": list(evaluation.answer_inputs.answer_mode.get("reasons") or []),
    }
    _write_execution_observability_artifact(runtime_state, evaluation.answer_inputs)
    return fallback_evidence_seconds


def run_result_layering_pipeline(
    *,
    question: str,
    trace_id: str,
    normalized_search_mode: str,
    benchmark_mode: bool,
    quota_state: dict[str, Any],
    defer_per_item_expansion: bool,
    allowed_plan: list[PlannedToolCall],
    tool_results: list[ToolExecution],
    query_profile: dict[str, Any],
    query_classification: dict[str, Any],
    runtime_state: AgentRuntimeState,
    apply_reference_limits: Callable[[list[ToolExecution], str, dict[str, Any]], list[ToolExecution]],
    execute_per_item_expansion: Callable[..., list[ToolExecution]],
    update_post_retrieval_fallback_evidence: Callable[..., None],
    log_no_context_query: Callable[..., Any],
    post_retrieval_repair_owner_deps: PostRetrievalRepairDeps,
    guardrail_flag_owner_deps: GuardrailFlagDeps,
    build_guardrail_answer_mode: Callable[..., dict[str, Any]],
    log_agent_media_miss: Callable[..., None],
    increment_quota_state: Callable[..., Any],
    resolve_agent_no_context: Callable[[str, int, int], tuple[int, str]],
    get_query_type: Callable[..., str],
    get_planner_snapshot_from_runtime: Callable[[AgentRuntimeState], dict[str, Any]],
    get_resolved_query_state_from_runtime: Callable[[AgentRuntimeState], dict[str, Any]],
) -> Any:
    """Orchestrate result evaluation first, then apply state/logging side effects."""
    evaluation = evaluate_result_layering(
        question=question,
        trace_id=trace_id,
        normalized_search_mode=normalized_search_mode,
        defer_per_item_expansion=defer_per_item_expansion,
        allowed_plan=allowed_plan,
        tool_results=tool_results,
        query_profile=query_profile,
        query_classification=query_classification,
        runtime_state=runtime_state,
        apply_reference_limits=apply_reference_limits,
        execute_per_item_expansion=execute_per_item_expansion,
        post_retrieval_repair_owner_deps=post_retrieval_repair_owner_deps,
        guardrail_flag_owner_deps=guardrail_flag_owner_deps,
        build_guardrail_answer_mode=build_guardrail_answer_mode,
        resolve_agent_no_context=resolve_agent_no_context,
        get_query_type=get_query_type,
        get_planner_snapshot_from_runtime=get_planner_snapshot_from_runtime,
        get_resolved_query_state_from_runtime=get_resolved_query_state_from_runtime,
    )
    fallback_evidence_seconds = apply_result_layering_side_effects(
        benchmark_mode=benchmark_mode,
        question=question,
        trace_id=trace_id,
        quota_state=quota_state,
        evaluation=evaluation,
        query_classification=query_classification,
        runtime_state=runtime_state,
        update_post_retrieval_fallback_evidence=update_post_retrieval_fallback_evidence,
        log_agent_media_miss=log_agent_media_miss,
        log_no_context_query=log_no_context_query,
        increment_quota_state=increment_quota_state,
    )

    return ResultLayeringOutcome(
        query_profile=evaluation.prepared.query_profile,
        tool_results=evaluation.post_retrieval.tool_results,
        reference_limit_seconds=evaluation.prepared.reference_limit_seconds,
        per_item_expansion_seconds=evaluation.prepared.per_item_expansion_seconds,
        fallback_evidence_seconds=fallback_evidence_seconds,
        post_retrieval_evaluate_seconds=evaluation.post_retrieval.post_retrieval_evaluate_seconds,
        post_retrieval_repairs_seconds=evaluation.post_retrieval.post_retrieval_repairs_seconds,
        answer_strategy_seconds=evaluation.post_retrieval.answer_strategy_seconds,
        guardrail_mode_seconds=evaluation.answer_inputs.guardrail_mode_seconds,
        doc_data=evaluation.post_retrieval.facts.doc_data,
        doc_metrics=evaluation.post_retrieval.facts.doc_metrics,
        usage_metrics=evaluation.answer_inputs.usage_metrics,
        media_response=evaluation.answer_inputs.media_response,
        answer_mode=evaluation.answer_inputs.answer_mode,
        answer_strategy=evaluation.post_retrieval.answer_strategy,
        post_retrieval_outcome=evaluation.post_retrieval.post_retrieval_outcome,
        guardrail_flags=evaluation.answer_inputs.guardrail_flags,
    )