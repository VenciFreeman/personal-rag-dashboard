from __future__ import annotations

import time as _time
from dataclasses import dataclass
from typing import Any, Callable

from .answer_composer import AnswerCompositionResult


@dataclass
class PreparedAnswerContext:
    memory_context: str
    context_assembly_seconds: float


def prepare_answer_context(
    *,
    session_id: str,
    benchmark_mode: bool,
    debug_trace: dict[str, Any],
    build_memory_context: Callable[[str], str],
    approx_tokens: Callable[[str], int],
) -> PreparedAnswerContext:
    context_assembly_t0 = _time.perf_counter()
    memory_context = "" if benchmark_mode else build_memory_context(session_id)
    context_assembly_seconds = _time.perf_counter() - context_assembly_t0
    debug_trace["memory_context"] = memory_context
    debug_trace["memory_tokens_est"] = approx_tokens(memory_context)
    return PreparedAnswerContext(
        memory_context=memory_context,
        context_assembly_seconds=context_assembly_seconds,
    )


def generate_sync_answer(
    *,
    question: str,
    history: list[dict[str, str]],
    memory_context: str,
    tool_results: list[Any],
    backend: str,
    normalized_search_mode: str,
    quota_state: dict[str, Any],
    trace_id: str,
    debug: bool,
    debug_trace: dict[str, Any],
    answer_mode: dict[str, Any],
    answer_strategy: dict[str, Any],
    runtime_state: Any,
    approx_tokens: Callable[[str], int],
    build_structured_media_answer: Callable[..., str],
    summarize_answer: Callable[..., AnswerCompositionResult],
    fallback_retrieval_answer: Callable[..., str],
    apply_guardrail_answer_mode: Callable[[str, dict[str, Any]], str],
    append_media_mentions_to_answer: Callable[[str, str, Any, list[Any]], str],
    compose_round_answer: Callable[..., AnswerCompositionResult],
) -> AnswerCompositionResult:
    return compose_round_answer(
        question=question,
        history=history,
        memory_context=memory_context,
        tool_results=tool_results,
        backend=backend,
        normalized_search_mode=normalized_search_mode,
        quota_state=quota_state,
        trace_id=trace_id,
        debug=debug,
        debug_trace=debug_trace,
        answer_mode=answer_mode,
        answer_strategy=answer_strategy,
        approx_tokens=approx_tokens,
        build_structured_media_answer=build_structured_media_answer,
        summarize_answer=summarize_answer,
        fallback_retrieval_answer=fallback_retrieval_answer,
        apply_guardrail_answer_mode=apply_guardrail_answer_mode,
        append_media_mentions_to_answer=append_media_mentions_to_answer,
        runtime_state=runtime_state,
    )


def start_streaming_answer(
    *,
    render_streaming_answer: Callable[..., Any],
    **kwargs: Any,
) -> Any:
    return render_streaming_answer(**kwargs)