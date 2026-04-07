from __future__ import annotations

import queue
import threading
import time as _time
from dataclasses import dataclass, field, replace as _dc_replace
from typing import Any, Callable, Iterator

from ..planner import planner_contracts
from .streaming_answer_policy import plan_streaming_answer


@dataclass
class StreamingAnswerResult:
    answer: str
    tool_results: list[Any]
    llm_stats: dict[str, Any] = field(default_factory=dict)
    response_timing_breakdown: dict[str, float] = field(default_factory=dict)
    llm_seconds: float = 0.0
    degraded_to_retrieval: bool = False
    degrade_reason: str = ""
    per_item_expansion_seconds: float = 0.0


def _update_runtime_streaming_plan(runtime_state: Any, plan_payload: dict[str, Any]) -> None:
    execution_artifact = getattr(runtime_state, "execution_artifact", None)
    if execution_artifact is None:
        return
    try:
        runtime_state.execution_artifact = _dc_replace(
            execution_artifact,
            streaming_plan=dict(plan_payload or {}),
        )
    except Exception:
        return


def render_streaming_answer(
    *,
    question: str,
    trace_id: str,
    history: list[dict[str, str]],
    memory_context: str,
    tool_results: list[Any],
    backend: str,
    normalized_search_mode: str,
    quota_state: dict[str, Any],
    debug: bool,
    debug_trace: dict[str, Any],
    answer_mode: dict[str, Any],
    answer_strategy: Any,
    runtime_state: Any,
    answer_shape: str,
    media_family: str,
    approx_tokens: Callable[[str], int],
    build_structured_media_answer_chunks: Callable[[str, Any, list[Any]], list[str]],
    build_structured_media_answer: Callable[[str, Any, list[Any]], str],
    build_structured_media_external_item_block: Callable[[dict[str, Any]], str],
    execute_per_item_expansion: Callable[..., list[Any]],
    summarize_answer: Callable[..., str],
    fallback_retrieval_answer: Callable[[str, list[Any], str], str],
) -> Iterator[dict[str, Any]]:
    del build_structured_media_answer_chunks
    del build_structured_media_answer
    del build_structured_media_external_item_block

    llm_stats: dict[str, Any] = {}
    response_timing_breakdown: dict[str, float] = {}
    degraded_to_retrieval = False
    degrade_reason = ""
    per_item_expansion_seconds = 0.0
    llm_seconds = 0.0
    suppress_personal_review_fanout = (
        str(getattr(getattr(runtime_state, "decision", None), "query_class", "") or "")
        == planner_contracts.ROUTER_QUERY_CLASS_PERSONAL_MEDIA_REVIEW_COLLECTION
        and str(answer_shape or "") == planner_contracts.ROUTER_ANSWER_SHAPE_COMPARE
    )

    def _emit_fanout_results() -> Iterator[dict[str, Any]]:
        for result in tool_results:
            if not (isinstance(result.data, dict) and result.data.get("per_item_fanout")):
                continue
            yield {
                "type": "tool_done",
                "trace_id": trace_id,
                "tool": result.tool,
                "status": result.status,
                "summary": result.summary,
            }

    plan = plan_streaming_answer(
        question=question,
        runtime_state=runtime_state,
        tool_results=tool_results,
        answer_mode=answer_mode,
        answer_strategy=answer_strategy,
        answer_shape=answer_shape,
        build_structured_media_answer_chunks=lambda *_args, **_kwargs: [],
    )
    streaming_plan_payload = {
        "mode": plan.mode,
        "fanout_phase": plan.fanout_phase,
        "summary_requested": True,
        "summary_attempted": False,
        "summary_succeeded": False,
        "contract_ok": True,
    }
    _update_runtime_streaming_plan(runtime_state, streaming_plan_payload)

    if plan.mode == "restricted":
        answer = plan.answer
        if answer:
            yield {"type": "answer_delta", "trace_id": trace_id, "delta": answer}
        llm_stats["backend"] = backend
        llm_stats["calls"] = 0
        llm_stats["input_tokens_est"] = 0
        llm_stats["prompt_tokens_est"] = 0
        llm_stats["context_tokens_est"] = 0
        llm_stats["memory_tokens_est"] = approx_tokens(memory_context)
        llm_stats["output_tokens_est"] = approx_tokens(answer)
        return StreamingAnswerResult(
            answer=answer,
            tool_results=tool_results,
            llm_stats=llm_stats,
            response_timing_breakdown=response_timing_breakdown,
            llm_seconds=llm_seconds,
            degraded_to_retrieval=degraded_to_retrieval,
            degrade_reason=degrade_reason,
            per_item_expansion_seconds=per_item_expansion_seconds,
        )

    try:
        if plan.fanout_phase == "before_llm" and not suppress_personal_review_fanout:
            per_item_expand_t0 = _time.perf_counter()
            tool_results = execute_per_item_expansion(
                tool_results,
                trace_id=trace_id,
                media_family=media_family,
                answer_shape=answer_shape,
            )
            per_item_expansion_seconds = _time.perf_counter() - per_item_expand_t0
            yield from _emit_fanout_results()

        llm_t0 = _time.perf_counter()
        summary_events: queue.Queue[dict[str, Any] | None] = queue.Queue()
        summary_holder: dict[str, Any] = {}
        emitted_summary_delta = False
        streaming_plan_payload["summary_attempted"] = True
        _update_runtime_streaming_plan(runtime_state, streaming_plan_payload)

        def _emit_answer_delta(delta: str) -> None:
            if delta:
                summary_events.put({"type": "answer_delta", "trace_id": trace_id, "delta": delta})

        def _run_summary() -> None:
            try:
                resolved_state = getattr(getattr(runtime_state, "context_resolution", None), "resolved_query_state", None)
                resolved_state = resolved_state if isinstance(resolved_state, dict) else {}
                summary_holder["answer"] = summarize_answer(
                    question=question,
                    history=history,
                    memory_context=memory_context,
                    tool_results=tool_results,
                    backend=backend,
                    search_mode=normalized_search_mode,
                    quota_state=quota_state,
                    trace_id=trace_id,
                    debug_sink=debug_trace if debug else None,
                    llm_stats_sink=llm_stats,
                    answer_strategy=answer_strategy,
                    stream_callback=_emit_answer_delta,
                    timing_sink=response_timing_breakdown,
                    followup_mode=str(getattr(getattr(runtime_state, "decision", None), "followup_mode", "none") or "none"),
                    carry_over_from_previous_turn=bool(resolved_state.get("carry_over_from_previous_turn")),
                )
            except Exception as exc:  # noqa: BLE001
                summary_holder["error"] = exc
            finally:
                summary_events.put(None)

        summary_thread = threading.Thread(target=_run_summary, daemon=True, name="agent-answer-stream")
        summary_thread.start()
        while True:
            event = summary_events.get()
            if event is None:
                break
            emitted_summary_delta = True
            yield event
        if "error" in summary_holder:
            raise summary_holder["error"]

        answer = str(summary_holder.get("answer", "") or "")
        if answer and not emitted_summary_delta:
            yield {"type": "answer_delta", "trace_id": trace_id, "delta": answer}
        llm_seconds = _time.perf_counter() - llm_t0
        streaming_plan_payload["summary_succeeded"] = True
        _update_runtime_streaming_plan(runtime_state, streaming_plan_payload)
    except Exception as exc:  # noqa: BLE001
        if (backend or "local").strip().lower() != "local":
            raise
        streaming_plan_payload["summary_error"] = str(exc)
        degraded_to_retrieval = True
        degrade_reason = str(exc)
        streaming_plan_payload["summary_fallback"] = "retrieval"
        streaming_plan_payload["contract_ok"] = False
        _update_runtime_streaming_plan(runtime_state, streaming_plan_payload)
        answer = fallback_retrieval_answer(question, tool_results, reason=degrade_reason)
        if answer:
            yield {"type": "answer_delta", "trace_id": trace_id, "delta": answer}
        llm_stats["output_tokens_est"] = approx_tokens(answer)

    return StreamingAnswerResult(
        answer=answer,
        tool_results=tool_results,
        llm_stats=llm_stats,
        response_timing_breakdown=response_timing_breakdown,
        llm_seconds=llm_seconds,
        degraded_to_retrieval=degraded_to_retrieval,
        degrade_reason=degrade_reason,
        per_item_expansion_seconds=per_item_expansion_seconds,
    )