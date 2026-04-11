from __future__ import annotations

import time as _time
from dataclasses import dataclass
from typing import Any, Callable

from ..agent.guardrail_flags_owner import GuardrailFlagDeps, build_guardrail_flags


@dataclass(frozen=True)
class MediaAnswerInputSideEffectRequests:
    log_agent_media_miss: bool
    no_context_log: dict[str, Any] | None
    web_search_delta: int


@dataclass(frozen=True)
class MediaAnswerInputsBuildArtifacts:
    media_response: Any
    answer_mode: dict[str, Any]
    guardrail_flags: dict[str, Any]
    guardrail_mode_seconds: float
    usage_deltas: dict[str, int]
    usage_metrics: dict[str, Any]
    side_effect_requests: MediaAnswerInputSideEffectRequests


def build_media_answer_inputs(
    *,
    question: str,
    trace_id: str,
    allowed_plan: list[Any],
    tool_results: list[Any],
    query_profile: dict[str, Any],
    query_classification: dict[str, Any],
    runtime_state: Any,
    answer_shape: str,
    media_family: str,
    media_tool_name: str,
    web_tool_name: str,
    doc_tool_name: str,
    build_media_working_set: Callable[..., Any],
    media_request_factory: Callable[..., Any],
    media_policy_flags_factory: Callable[..., Any],
    build_media_retrieval_response: Callable[..., Any],
    guardrail_flag_owner_deps: GuardrailFlagDeps,
    build_guardrail_answer_mode: Callable[[str, Any, list[Any], dict[str, bool]], dict[str, Any]],
    resolve_agent_no_context: Callable[[str, int, int], tuple[int, str]],
    get_query_type: Callable[..., str],
    get_planner_snapshot_from_runtime: Callable[[Any], dict[str, Any]],
    get_resolved_query_state_from_runtime: Callable[[Any], dict[str, Any]],
    doc_no_context_count: int,
) -> MediaAnswerInputsBuildArtifacts:
    media_result = next((item for item in tool_results if getattr(item, "tool", "") == media_tool_name and isinstance(getattr(item, "data", None), dict)), None)
    media_result_data = media_result.data if media_result is not None and isinstance(media_result.data, dict) else {}
    layer_breakdown = media_result_data.get("layer_breakdown") if isinstance(media_result_data.get("layer_breakdown"), dict) else {}
    retrieval_adapter = media_result_data.get("retrieval_adapter") if isinstance(media_result_data.get("retrieval_adapter"), dict) else {}

    media_request = media_request_factory(
        query=question,
        previous_working_set=build_media_working_set(
            tool_results,
            planner_snapshot=get_planner_snapshot_from_runtime(runtime_state),
            resolved_state=get_resolved_query_state_from_runtime(runtime_state),
            resolved_question=str(runtime_state.context_resolution.resolved_question or ""),
            raw_question=str(runtime_state.decision.raw_question or ""),
            answer_shape=str(runtime_state.decision.answer_shape or ""),
        ),
        resolved_entities=list(runtime_state.decision.entities or []),
        policy_flags=media_policy_flags_factory(
            strict_scope_active=bool(layer_breakdown.get("strict_scope_active")),
            working_set_reused=bool(
                str(media_result_data.get("lookup_mode") or "").strip() == "working_set_followup"
                or bool(retrieval_adapter.get("working_set_reused"))
            ),
            answer_shape=answer_shape,
            media_family=media_family,
        ),
    )
    media_response = build_media_retrieval_response(tool_results, request=media_request)
    guardrail_flags = query_classification.get("guardrail_flags") if isinstance(query_classification.get("guardrail_flags"), dict) else build_guardrail_flags(
        runtime_state=runtime_state,
        media_validation=media_response.validation,
        deps=guardrail_flag_owner_deps,
    )
    guardrail_mode_t0 = _time.perf_counter()
    answer_mode = build_guardrail_answer_mode(question, runtime_state, tool_results, guardrail_flags)
    guardrail_mode_seconds = _time.perf_counter() - guardrail_mode_t0

    rag_used = int(any(getattr(call, "name", "") == doc_tool_name for call in allowed_plan))
    media_used = int(any(getattr(call, "name", "") == media_tool_name for call in allowed_plan))
    web_used = int(any(getattr(call, "name", "") == web_tool_name for call in allowed_plan))
    agent_no_context, agent_no_context_reason = resolve_agent_no_context(
        get_query_type(runtime_state=runtime_state),
        rag_used,
        int(doc_no_context_count or 0),
    )
    web_calls = sum(
        1
        for item in tool_results
        if getattr(item, "tool", "") == web_tool_name
        and getattr(item, "status", "") in {"ok", "empty", "error"}
        and not (isinstance(getattr(item, "data", None), dict) and item.data.get("cache_hit"))
    )
    usage_metrics = {
        "web_cache_hit": int(any(getattr(item, "tool", "") == web_tool_name and isinstance(getattr(item, "data", None), dict) and item.data.get("cache_hit") for item in tool_results)),
        "rag_used": rag_used,
        "media_used": media_used,
        "web_used": web_used,
        "agent_no_context": agent_no_context,
        "agent_no_context_reason": agent_no_context_reason,
    }
    return MediaAnswerInputsBuildArtifacts(
        media_response=media_response,
        answer_mode=answer_mode,
        guardrail_flags=guardrail_flags,
        guardrail_mode_seconds=guardrail_mode_seconds,
        usage_deltas={
            "web_search_delta": web_calls,
        },
        usage_metrics=usage_metrics,
        side_effect_requests=MediaAnswerInputSideEffectRequests(
            log_agent_media_miss=bool(any(getattr(call, "name", "") == media_tool_name for call in allowed_plan) and not media_response.main_results),
            no_context_log={
                "question": question,
                "source": "rag",
                "top1_score": None,
                "threshold": float(query_profile.get("doc_score_threshold", 0) or 0),
                "trace_id": trace_id,
                "reason": agent_no_context_reason or "below_threshold",
            } if int(doc_no_context_count or 0) else None,
            web_search_delta=web_calls,
        ),
    )


def apply_media_answer_input_side_effects(
    *,
    benchmark_mode: bool,
    question: str,
    query_profile: dict[str, Any],
    quota_state: dict[str, Any],
    side_effect_requests: MediaAnswerInputSideEffectRequests,
    log_agent_media_miss: Callable[[str, dict[str, Any]], None],
    log_no_context_query: Callable[..., Any],
    increment_quota_state: Callable[..., Any],
) -> None:
    if not benchmark_mode and side_effect_requests.log_agent_media_miss:
        log_agent_media_miss(question, query_profile)
    if not benchmark_mode and side_effect_requests.no_context_log:
        try:
            log_no_context_query(**side_effect_requests.no_context_log)
        except Exception:
            pass
    if side_effect_requests.web_search_delta:
        increment_quota_state(quota_state, web_search_delta=side_effect_requests.web_search_delta)
