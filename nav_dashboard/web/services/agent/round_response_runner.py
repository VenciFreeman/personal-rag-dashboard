from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from .agent_round_runner import AgentRoundContext
from ..tooling.tool_plan_runner import ToolPlanRunResult


@dataclass
class RoundResponseResult:
    trace_record: dict[str, Any]
    payload: dict[str, Any]


def persist_round_response(
    *,
    round_context: AgentRoundContext,
    tool_plan: ToolPlanRunResult,
    final_answer: str,
    llm_stats: dict[str, Any],
    response_timing_breakdown: dict[str, Any],
    llm_seconds: float,
    degraded_to_retrieval: bool,
    degrade_reason: str,
    backend: str,
    benchmark_mode: bool,
    debug: bool,
    stream_mode: bool,
    wall_t0: float,
    perf_counter: Callable[[], float],
    approx_tokens: Callable[[str], int],
    persist_session_artifacts: Callable[..., Any],
    append_message: Callable[..., None],
    update_memory_for_session: Callable[..., Any],
    schedule_generated_session_title: Callable[..., Any],
    auto_queue_bug_tickets: Callable[..., Any],
    write_debug_record: Callable[[str, dict[str, Any]], None],
    record_metrics_safe: Callable[..., float],
    record_agent_metrics: Callable[..., Any],
    build_agent_trace_record: Callable[..., dict[str, Any]],
    write_agent_trace_record: Callable[[dict[str, Any]], None],
    serialize_planned_tools: list[dict[str, Any]],
    get_query_type: Callable[..., str],
    web_search_daily_limit: int,
    deepseek_daily_limit: int,
) -> RoundResponseResult:
    serialized_query_profile = tool_plan.query_profile.to_dict() if hasattr(tool_plan.query_profile, "to_dict") else dict(tool_plan.query_profile)
    persist_result = persist_session_artifacts(
        benchmark_mode=benchmark_mode,
        session_id=round_context.session_id,
        question=round_context.question,
        final_answer=final_answer,
        trace_id=round_context.trace_id,
        append_message=append_message,
        update_memory_for_session=update_memory_for_session,
        schedule_generated_session_title=schedule_generated_session_title,
        auto_queue_bug_tickets=auto_queue_bug_tickets,
    )
    session_persist_seconds = float(persist_result.session_persist_seconds or 0.0)
    response_timing_breakdown["session_persist_seconds"] = round(session_persist_seconds, 6)
    if debug:
        round_context.debug_trace["final_answer_tokens_est"] = approx_tokens(final_answer)
        write_debug_record(round_context.session_id, round_context.debug_trace)

    doc_metrics = dict(tool_plan.doc_metrics or {})
    usage_metrics = dict(tool_plan.usage_metrics or {})
    doc_vector_recall_s = float(doc_metrics.get("vector_recall_seconds", 0) or 0)
    doc_rerank_s = float(doc_metrics.get("rerank_seconds", 0) or 0)
    doc_top1_score = doc_metrics.get("top1_score")
    doc_top1_score_before_rerank = doc_metrics.get("top1_score_before_rerank")
    doc_top1_identity_changed = doc_metrics.get("top1_identity_changed")
    doc_top1_rank_shift = doc_metrics.get("top1_rank_shift")
    doc_embed_cache_hit = int(doc_metrics.get("embed_cache_hit", 0) or 0)
    doc_query_rewrite_hit = int(doc_metrics.get("query_rewrite_hit", 0) or 0)
    doc_threshold = float(doc_metrics.get("threshold", 0) or 0)
    web_cache_hit = int(usage_metrics.get("web_cache_hit", 0) or 0)
    rag_used = int(usage_metrics.get("rag_used", 0) or 0)
    media_used = int(usage_metrics.get("media_used", 0) or 0)
    web_used = int(usage_metrics.get("web_used", 0) or 0)
    agent_no_context = int(usage_metrics.get("agent_no_context", 0) or 0)
    agent_no_context_reason = str(usage_metrics.get("agent_no_context_reason", "") or "")

    metrics_seconds = record_metrics_safe(
        benchmark_mode=benchmark_mode,
        record_agent_metrics=record_agent_metrics,
        metric_kwargs={
            "query_profile": str(tool_plan.query_profile.get("profile", "medium") or "medium"),
            "search_mode": round_context.normalized_search_mode,
            "query_type": get_query_type(runtime_state=round_context.runtime_state),
            "rag_used": rag_used,
            "media_used": media_used,
            "web_used": web_used,
            "no_context": agent_no_context,
            "no_context_reason": agent_no_context_reason,
            "trace_id": round_context.trace_id,
            "doc_score_threshold": doc_threshold,
            "doc_top1_score": float(doc_top1_score) if doc_top1_score is not None else None,
            "doc_top1_score_before_rerank": float(doc_top1_score_before_rerank) if doc_top1_score_before_rerank is not None else None,
            "doc_top1_identity_changed": int(doc_top1_identity_changed) if doc_top1_identity_changed is not None else None,
            "doc_top1_rank_shift": float(doc_top1_rank_shift) if doc_top1_rank_shift is not None else None,
            "embed_cache_hit": doc_embed_cache_hit,
            "query_rewrite_hit": doc_query_rewrite_hit,
            "vector_recall_seconds": doc_vector_recall_s,
            "rerank_seconds": doc_rerank_s,
            "wall_clock_seconds": perf_counter() - wall_t0,
        },
    )
    response_timing_breakdown["metrics_record_seconds"] = round(float(metrics_seconds or 0), 6)
    response_finalize_seconds = float(response_timing_breakdown.get("response_finalize_seconds", 0) or 0)

    trace_record = build_agent_trace_record(
        trace_id=round_context.trace_id,
        session_id=round_context.session_id,
        backend=backend,
        search_mode=round_context.normalized_search_mode,
        benchmark_mode=benchmark_mode,
        stream_mode=stream_mode,
        query_profile=serialized_query_profile,
        runtime_state=round_context.runtime_state,
        planned_tools=round_context.planned,
        tool_results=tool_plan.tool_results,
        doc_data=tool_plan.doc_data,
        timings={
            "session_prepare_seconds": round_context.session_prepare_seconds,
            "query_profile_seconds": round_context.query_profile_seconds,
            "tool_planning_seconds": round_context.tool_planning_seconds,
            **round_context.planning_timing_breakdown,
            "context_assembly_seconds": float(response_timing_breakdown.get("context_assembly_seconds", 0) or 0),
            "reference_limit_seconds": tool_plan.reference_limit_seconds,
            "per_item_expansion_seconds": tool_plan.per_item_expansion_seconds,
            "post_retrieval_fallback_seconds": tool_plan.fallback_evidence_seconds,
            "post_retrieval_evaluate_seconds": tool_plan.post_retrieval_evaluate_seconds,
            "post_retrieval_repairs_seconds": tool_plan.post_retrieval_repairs_seconds,
            "answer_strategy_seconds": tool_plan.answer_strategy_seconds,
            "guardrail_mode_seconds": tool_plan.guardrail_mode_seconds,
            **response_timing_breakdown,
            "vector_recall_seconds": doc_vector_recall_s,
            "rerank_seconds": doc_rerank_s,
            "no_context": agent_no_context,
            "no_context_reason": agent_no_context_reason,
            "doc_score_threshold": doc_threshold,
        },
        llm_stats=llm_stats,
        answer_guardrail_mode=round_context.query_classification.get("answer_guardrail_mode", {}),
        degraded_to_retrieval=degraded_to_retrieval,
        degrade_reason=degrade_reason,
        wall_clock_seconds=perf_counter() - wall_t0,
        planning_seconds=round_context.planning_seconds,
        tool_execution_seconds=tool_plan.tool_execution_seconds,
        llm_seconds=llm_seconds,
        response_finalize_seconds=response_finalize_seconds,
    )
    trace_record["post_retrieval_outcome"] = tool_plan.post_retrieval_outcome
    trace_record["answer_strategy"] = tool_plan.answer_strategy
    write_agent_trace_record(trace_record)

    payload = {
        "requires_confirmation": False,
        "trace_id": round_context.trace_id,
        "session_id": round_context.session_id,
        "answer": final_answer,
        "backend": backend,
        "search_mode": round_context.normalized_search_mode,
        "query_profile": serialized_query_profile,
        "query_classification": round_context.query_classification,
        "query_type": get_query_type(runtime_state=round_context.runtime_state),
        "query_understanding": trace_record.get("query_understanding", {}),
        "conversation_state_before": trace_record.get("conversation_state_before", {}),
        "detected_followup": bool(trace_record.get("detected_followup")),
        "inheritance_applied": trace_record.get("inheritance_applied", {}),
        "conversation_state_after": trace_record.get("conversation_state_after", {}),
        "state_diff": trace_record.get("state_diff", {}),
        "planner_snapshot": trace_record.get("planner_snapshot", {}),
        "guardrail_flags": trace_record.get("guardrail_flags", {}),
        "error_taxonomy": trace_record.get("error_taxonomy", {}),
        "answer_guardrail_mode": round_context.query_classification.get("answer_guardrail_mode", {}),
        "degraded_to_retrieval": degraded_to_retrieval,
        "degrade_reason": degrade_reason,
        "planned_tools": serialize_planned_tools,
        "tool_results": [
            {"tool": result.tool, "status": result.status, "summary": result.summary, "data": result.data}
            for result in tool_plan.tool_results
        ],
        "debug_enabled": bool(debug),
        "timings": {
            "vector_recall_seconds": doc_vector_recall_s,
            "rerank_seconds": doc_rerank_s,
            "no_context": agent_no_context,
            "no_context_reason": agent_no_context_reason,
            "doc_score_threshold": doc_threshold,
            "web_cache_hit": web_cache_hit,
        },
        "quota": {
            "date": round_context.quota_state.get("date"),
            "web_search": int(round_context.quota_state.get("web_search", 0) or 0),
            "web_search_limit": web_search_daily_limit,
            "deepseek": int(round_context.quota_state.get("deepseek", 0) or 0),
            "deepseek_limit": deepseek_daily_limit,
        },
    }
    return RoundResponseResult(trace_record=trace_record, payload=payload)
