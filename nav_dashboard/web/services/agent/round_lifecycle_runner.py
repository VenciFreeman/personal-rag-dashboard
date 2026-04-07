from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterator


@dataclass(frozen=True)
class RoundLifecycleDeps:
    prepare_round_answer: Callable[..., Any]
    generate_sync_answer: Callable[..., Any]
    start_streaming_answer: Callable[..., Any]
    finalize_round_answer: Callable[..., Any]
    persist_round_response: Callable[..., Any]
    update_debug_trace_with_tool_plan: Callable[..., None]
    apply_guardrail_answer_mode: Callable[[str, dict[str, Any]], str]
    append_media_mentions_to_answer: Callable[[str, str, Any, list[Any]], str]
    get_query_type: Callable[..., str]
    normalize_trace_id: Callable[[str], str]
    perf_counter: Callable[[], float]
    approx_tokens: Callable[[str], int]


def _result_rows_count(result: Any) -> int:
    data = getattr(result, "data", None)
    if not isinstance(data, dict):
        return 0
    main_results = data.get("main_results")
    if isinstance(main_results, list) and main_results:
        return len(main_results)
    results = data.get("results")
    if isinstance(results, list):
        return len(results)
    return 0


def _build_stream_evidence_summary(tool_results: list[Any]) -> str:
    doc_count = 0
    media_count = 0
    external_count = 0
    for result in tool_results:
        status = str(getattr(result, "status", "") or "").strip().lower()
        if status == "skipped":
            continue
        tool_name = str(getattr(result, "tool", "") or "").strip()
        count = _result_rows_count(result)
        if tool_name == "query_document_rag":
            doc_count += count
        elif tool_name in {"query_media_record", "search_by_creator"}:
            media_count += count
        elif tool_name in {"search_tmdb_media", "search_bangumi_subject", "search_mediawiki_action", "parse_mediawiki_page", "expand_mediawiki_concept", "search_web"}:
            external_count += count
    parts: list[str] = []
    if doc_count:
        parts.append(f"本地文档 {doc_count} 条")
    if media_count:
        parts.append(f"本地媒体 {media_count} 条")
    if external_count:
        parts.append(f"外部资料 {external_count} 条")
    if not parts:
        return ""
    return "本轮依据：" + "；".join(parts)


def run_round_sync(
    *,
    deps: RoundLifecycleDeps,
    question: str,
    session_id: str = "",
    history: list[dict[str, str]] | None = None,
    backend: str = "local",
    search_mode: str = "local_only",
    confirm_over_quota: bool = False,
    deny_over_quota: bool = False,
    debug: bool = False,
    benchmark_mode: bool = False,
    trace_id: str = "",
) -> dict[str, Any]:
    wall_t0 = deps.perf_counter()
    prepared = deps.prepare_round_answer(
        question=question,
        trace_id=trace_id,
        session_id=session_id,
        history=history,
        backend=backend,
        search_mode=search_mode,
        benchmark_mode=benchmark_mode,
        confirm_over_quota=confirm_over_quota,
        deny_over_quota=deny_over_quota,
        defer_per_item_expansion=False,
    )
    if prepared.confirmation_payload is not None:
        return prepared.confirmation_payload

    round_context = prepared.round_context
    tool_plan = prepared.tool_plan
    answer_context = prepared.answer_context
    if tool_plan is None or answer_context is None:
        raise RuntimeError("shared round preparation did not produce answer context")

    composition = deps.generate_sync_answer(
        question=round_context.question,
        history=round_context.history,
        memory_context=answer_context.memory_context,
        tool_results=tool_plan.tool_results,
        backend=backend,
        normalized_search_mode=round_context.normalized_search_mode,
        quota_state=round_context.quota_state,
        trace_id=round_context.trace_id,
        debug=debug,
        debug_trace=round_context.debug_trace,
        answer_mode=tool_plan.answer_mode,
        answer_strategy=tool_plan.answer_strategy,
        runtime_state=round_context.runtime_state,
    )
    response_timing_breakdown = dict(composition.response_timing_breakdown)
    response_timing_breakdown["context_assembly_seconds"] = round(float(answer_context.context_assembly_seconds or 0), 6)
    final_answer = deps.finalize_round_answer(
        answer=composition.answer,
        response_timing_breakdown=response_timing_breakdown,
        tool_results=tool_plan.tool_results,
    )
    response = deps.persist_round_response(
        round_context=round_context,
        tool_plan=tool_plan,
        final_answer=final_answer.final_answer,
        llm_stats=composition.llm_stats,
        response_timing_breakdown=response_timing_breakdown,
        llm_seconds=composition.llm_seconds,
        degraded_to_retrieval=composition.degraded_to_retrieval,
        degrade_reason=composition.degrade_reason,
        backend=backend,
        benchmark_mode=benchmark_mode,
        debug=debug,
        stream_mode=False,
        wall_t0=wall_t0,
    )
    return response.payload


def run_round_stream(
    *,
    deps: RoundLifecycleDeps,
    question: str,
    session_id: str = "",
    history: list[dict[str, str]] | None = None,
    backend: str = "local",
    search_mode: str = "local_only",
    confirm_over_quota: bool = False,
    deny_over_quota: bool = False,
    debug: bool = False,
    benchmark_mode: bool = False,
    trace_id: str = "",
) -> Iterator[dict[str, Any]]:
    wall_t0 = deps.perf_counter()
    resolved_trace_id = deps.normalize_trace_id(trace_id)

    try:
        yield {"type": "progress", "trace_id": resolved_trace_id, "message": "正在规划工具调用..."}
        prepared = deps.prepare_round_answer(
            question=question,
            trace_id=trace_id,
            session_id=session_id,
            history=history,
            backend=backend,
            search_mode=search_mode,
            benchmark_mode=benchmark_mode,
            confirm_over_quota=confirm_over_quota,
            deny_over_quota=deny_over_quota,
            defer_per_item_expansion=True,
        )
        round_context = prepared.round_context
        resolved_trace_id = round_context.trace_id
        yield {"type": "progress", "trace_id": resolved_trace_id, "message": f"查询分类：{deps.get_query_type(runtime_state=round_context.runtime_state)}"}
        if prepared.confirmation_payload is not None:
            yield {
                "type": "quota_exceeded",
                "trace_id": resolved_trace_id,
                "session_id": round_context.session_id,
                "message": "已超过今日 API 配额，是否继续调用超额工具？",
                "exceeded": prepared.confirmation_payload.get("exceeded", []),
                "planned_tools": prepared.confirmation_payload.get("planned_tools", []),
            }
            return

        tool_plan = prepared.tool_plan
        answer_context = prepared.answer_context
        if tool_plan is None or answer_context is None:
            raise RuntimeError("shared round preparation did not produce streaming context")
        tool_names = "、".join(call.name for call in prepared.preview_allowed_plan) if prepared.preview_allowed_plan else "无"
        yield {"type": "progress", "trace_id": resolved_trace_id, "message": f"计划调用工具：{tool_names}"}
        yield {"type": "progress", "trace_id": resolved_trace_id, "message": f"正在并行执行 {len(prepared.preview_allowed_plan)} 个工具..."}
        for result in tool_plan.tool_results:
            if str(getattr(result, "status", "") or "").strip().lower() == "skipped":
                continue
            yield {
                "type": "tool_done",
                "trace_id": resolved_trace_id,
                "tool": result.tool,
                "status": result.status,
                "summary": result.summary,
            }

        evidence_summary = _build_stream_evidence_summary(tool_plan.tool_results)
        if evidence_summary:
            yield {"type": "progress", "trace_id": resolved_trace_id, "message": evidence_summary}

        yield {"type": "progress", "trace_id": resolved_trace_id, "message": "工具执行完毕，正在生成回答..."}
        stream_generator = deps.start_streaming_answer(
            question=round_context.question,
            trace_id=resolved_trace_id,
            history=round_context.history,
            memory_context=answer_context.memory_context,
            tool_results=tool_plan.tool_results,
            backend=backend,
            normalized_search_mode=round_context.normalized_search_mode,
            quota_state=round_context.quota_state,
            debug=debug,
            debug_trace=round_context.debug_trace,
            answer_mode=tool_plan.answer_mode,
            answer_strategy=tool_plan.answer_strategy,
            runtime_state=round_context.runtime_state,
            answer_shape=str((round_context.query_classification or {}).get("answer_shape", "") or ""),
            media_family=str((round_context.query_classification or {}).get("media_family", "") or ""),
        )
        while True:
            try:
                event = next(stream_generator)
                yield event
            except StopIteration as stop:
                stream_result = stop.value
                break

        answer = stream_result.answer
        tool_plan.tool_results = list(stream_result.tool_results)
        tool_plan.per_item_expansion_seconds = max(tool_plan.per_item_expansion_seconds, float(stream_result.per_item_expansion_seconds or 0.0))
        deps.update_debug_trace_with_tool_plan(round_context.debug_trace, tool_plan)
        guardrail_apply_t0 = deps.perf_counter()
        answer = deps.apply_guardrail_answer_mode(answer, tool_plan.answer_mode)
        response_timing_breakdown = dict(stream_result.response_timing_breakdown)
        response_timing_breakdown["final_guardrail_apply_seconds"] = round(float(deps.perf_counter() - guardrail_apply_t0), 6)
        mention_append_t0 = deps.perf_counter()
        answer = deps.append_media_mentions_to_answer(answer, question, round_context.runtime_state, tool_plan.tool_results)
        response_timing_breakdown["mention_append_seconds"] = round(float(deps.perf_counter() - mention_append_t0), 6)
        stream_result.llm_stats["output_tokens_est"] = deps.approx_tokens(answer)
        response_timing_breakdown["context_assembly_seconds"] = round(float(answer_context.context_assembly_seconds or 0), 6)
        final_answer = deps.finalize_round_answer(
            answer=answer,
            response_timing_breakdown=response_timing_breakdown,
            tool_results=tool_plan.tool_results,
        )
        response = deps.persist_round_response(
            round_context=round_context,
            tool_plan=tool_plan,
            final_answer=final_answer.final_answer,
            llm_stats=stream_result.llm_stats,
            response_timing_breakdown=response_timing_breakdown,
            llm_seconds=stream_result.llm_seconds,
            degraded_to_retrieval=stream_result.degraded_to_retrieval,
            degrade_reason=stream_result.degrade_reason,
            backend=backend,
            benchmark_mode=benchmark_mode,
            debug=debug,
            stream_mode=True,
            wall_t0=wall_t0,
        )
        yield {
            "type": "done",
            "trace_id": resolved_trace_id,
            "payload": response.payload,
        }
    except Exception as exc:  # noqa: BLE001
        yield {"type": "error", "trace_id": locals().get("resolved_trace_id", deps.normalize_trace_id(trace_id)), "message": str(exc)}