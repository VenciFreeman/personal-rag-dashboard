from __future__ import annotations

import time as _time
from typing import Any, Callable

from ..agent_round_runner import RoundRoutingDeps, prepare_round_answer
from ..agent_types import AnswerDeps, ExecutionDeps, ObservabilityDeps, PlannerDeps
from ..guardrail_flags_owner import GuardrailFlagDeps
from ...retrieval.post_retrieval_repairs_owner import PostRetrievalRepairDeps
from ..round_lifecycle_runner import RoundLifecycleDeps
from ...tooling.tool_plan_runner import ToolPlanExecutionDeps


def build_guardrail_flag_deps(
    *,
    get_resolved_query_state_from_runtime: Callable[..., dict[str, Any]],
    get_lookup_mode_from_state: Callable[[dict[str, Any]], str],
    get_planner_snapshot_from_runtime: Callable[..., dict[str, Any]],
    question_requests_media_details: Callable[[str], bool],
) -> GuardrailFlagDeps:
    return GuardrailFlagDeps(
        get_resolved_query_state_from_runtime=get_resolved_query_state_from_runtime,
        get_lookup_mode_from_state=get_lookup_mode_from_state,
        get_planner_snapshot_from_runtime=get_planner_snapshot_from_runtime,
        question_requests_media_details=question_requests_media_details,
    )


def build_post_retrieval_repair_deps(
    *,
    normalize_search_mode: Callable[[str], str],
    execute_tool_plan: Callable[..., Any],
    get_media_validation: Callable[[list[Any]], dict[str, Any]],
    guardrail_flag_deps: GuardrailFlagDeps,
) -> PostRetrievalRepairDeps:
    return PostRetrievalRepairDeps(
        normalize_search_mode=normalize_search_mode,
        execute_tool_plan=execute_tool_plan,
        get_media_validation=get_media_validation,
        guardrail_flag_deps=guardrail_flag_deps,
    )


def build_round_routing_deps(
    *,
    prepare_session_context: Callable[..., Any],
    plan_query_execution: Callable[..., Any],
    quota_exceeded: Callable[..., list[dict[str, Any]]],
    create_session: Callable[[], dict[str, Any]],
    get_session: Callable[[str], dict[str, Any] | None],
    save_session: Callable[[dict[str, Any]], None],
    derive_session_title: Callable[[str], str],
    now_iso: Callable[[], str],
    new_ephemeral_session_id: Callable[[], str],
    normalize_search_mode: Callable[[str], str],
    resolve_query_profile: Callable[[str], dict[str, Any]],
    load_quota_state: Callable[[], dict[str, Any]],
    plan_tool_calls: Callable[..., Any],
    serialize_planned_tools: Callable[..., list[dict[str, Any]]],
) -> RoundRoutingDeps:
    return RoundRoutingDeps(
        prepare_session_context=prepare_session_context,
        plan_query_execution=plan_query_execution,
        quota_exceeded=quota_exceeded,
        create_session=create_session,
        get_session=get_session,
        save_session=save_session,
        derive_session_title=derive_session_title,
        now_iso=now_iso,
        new_ephemeral_session_id=new_ephemeral_session_id,
        normalize_search_mode=normalize_search_mode,
        resolve_query_profile=resolve_query_profile,
        load_quota_state=load_quota_state,
        plan_tool_calls=plan_tool_calls,
        serialize_planned_tools=serialize_planned_tools,
    )


def build_tool_plan_execution_deps(
    *,
    apply_reference_limits: Callable[..., Any],
    execute_per_item_expansion: Callable[..., Any],
    update_post_retrieval_fallback_evidence: Callable[..., Any],
    log_no_context_query: Callable[..., Any],
    post_retrieval_repair_owner_deps: PostRetrievalRepairDeps,
    guardrail_flag_owner_deps: GuardrailFlagDeps,
    build_guardrail_answer_mode: Callable[..., Any],
    log_agent_media_miss: Callable[..., Any],
    increment_quota_state: Callable[..., Any],
    resolve_agent_no_context: Callable[..., Any],
    get_query_type: Callable[..., str],
    get_planner_snapshot_from_runtime: Callable[..., dict[str, Any]],
    get_resolved_query_state_from_runtime: Callable[..., dict[str, Any]],
) -> ToolPlanExecutionDeps:
    return ToolPlanExecutionDeps(
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


def build_round_lifecycle_deps(
    *,
    request_base_url: str,
    planner_deps: PlannerDeps,
    execution_deps: ExecutionDeps,
    answer_deps: AnswerDeps,
    observability_deps: ObservabilityDeps,
) -> RoundLifecycleDeps:
    return RoundLifecycleDeps(
        prepare_round_answer=lambda **kwargs: planner_deps.prepare_round_answer_fn(
            normalize_trace_id=planner_deps.normalize_trace_id,
            routing_deps=planner_deps.routing_deps,
            build_confirmation_payload=planner_deps.build_confirmation_payload,
            resolve_allowed_plan=planner_deps.resolve_allowed_plan,
            execute_tool_phase=planner_deps.execute_tool_phase,
            execute_tool_plan_boundary=planner_deps.execute_tool_plan_boundary,
            tool_plan_deps=planner_deps.tool_plan_deps,
            append_message=execution_deps.append_message,
            build_memory_context=execution_deps.build_memory_context,
            approx_tokens=execution_deps.approx_tokens,
            **kwargs,
        ),
        generate_sync_answer=lambda **kwargs: answer_deps.generate_sync_answer_fn(
            approx_tokens=execution_deps.approx_tokens,
            build_structured_media_answer=lambda question, runtime_state, tool_results: answer_deps.build_structured_media_answer(
                question,
                runtime_state,
                tool_results,
                request_base_url=request_base_url,
            ),
            summarize_answer=answer_deps.summarize_answer,
            fallback_retrieval_answer=answer_deps.fallback_retrieval_answer,
            apply_guardrail_answer_mode=answer_deps.apply_guardrail_answer_mode,
            append_media_mentions_to_answer=answer_deps.append_media_mentions_to_answer,
            compose_round_answer=answer_deps.compose_round_answer,
            **kwargs,
        ),
        start_streaming_answer=lambda **kwargs: answer_deps.start_streaming_answer_fn(
            render_streaming_answer=answer_deps.render_streaming_answer,
            approx_tokens=execution_deps.approx_tokens,
            build_structured_media_answer_chunks=lambda question, runtime_state, tool_results: answer_deps.build_structured_media_answer_chunks(
                question,
                runtime_state,
                tool_results,
                request_base_url=request_base_url,
            ),
            build_structured_media_answer=lambda question, runtime_state, tool_results: answer_deps.build_structured_media_answer(
                question,
                runtime_state,
                tool_results,
                request_base_url=request_base_url,
            ),
            build_structured_media_external_item_block=answer_deps.build_structured_media_external_item_block,
            execute_per_item_expansion=execution_deps.execute_per_item_expansion,
            summarize_answer=answer_deps.summarize_answer,
            fallback_retrieval_answer=answer_deps.fallback_retrieval_answer,
            **kwargs,
        ),
        finalize_round_answer=lambda **kwargs: answer_deps.finalize_round_answer_fn(
            request_base_url=request_base_url,
            perf_counter=_time.perf_counter,
            **kwargs,
        ),
        persist_round_response=lambda **kwargs: observability_deps.persist_round_response_fn(
            perf_counter=_time.perf_counter,
            approx_tokens=execution_deps.approx_tokens,
            persist_session_artifacts=observability_deps.persist_session_artifacts,
            append_message=execution_deps.append_message,
            update_memory_for_session=observability_deps.update_memory_for_session,
            schedule_generated_session_title=observability_deps.schedule_generated_session_title,
            auto_queue_bug_tickets=observability_deps.auto_queue_bug_tickets,
            write_debug_record=observability_deps.write_debug_record,
            record_metrics_safe=observability_deps.record_metrics_safe,
            record_agent_metrics=observability_deps.record_agent_metrics,
            build_agent_trace_record=observability_deps.build_agent_trace_record,
            write_agent_trace_record=observability_deps.write_agent_trace_record,
            get_query_type=observability_deps.get_query_type,
            web_search_daily_limit=observability_deps.web_search_daily_limit,
            deepseek_daily_limit=observability_deps.deepseek_daily_limit,
            serialize_planned_tools=observability_deps.serialize_planned_tools(kwargs["round_context"].planned),
            **kwargs,
        ),
        update_debug_trace_with_tool_plan=observability_deps.update_debug_trace_with_tool_plan,
        apply_guardrail_answer_mode=answer_deps.apply_guardrail_answer_mode,
        append_media_mentions_to_answer=answer_deps.append_media_mentions_to_answer,
        get_query_type=observability_deps.get_query_type,
        normalize_trace_id=planner_deps.normalize_trace_id,
        perf_counter=_time.perf_counter,
        approx_tokens=execution_deps.approx_tokens,
    )
