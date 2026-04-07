from __future__ import annotations

import time as _time
from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class AnswerCompositionResult:
    answer: str
    llm_stats: dict[str, Any] = field(default_factory=dict)
    response_timing_breakdown: dict[str, float] = field(default_factory=dict)
    llm_seconds: float = 0.0
    degraded_to_retrieval: bool = False
    degrade_reason: str = ""


def _get_strategy_mode(answer_strategy: Any) -> str:
    if isinstance(answer_strategy, dict):
        return str(answer_strategy.get("mode") or "").strip()
    return str(getattr(answer_strategy, "mode", "") or "").strip()


def _get_style_hint(answer_strategy: Any, key: str, default: Any = None) -> Any:
    style_hints = {}
    if isinstance(answer_strategy, dict):
        style_hints = answer_strategy.get("style_hints") or {}
    else:
        style_hints = getattr(answer_strategy, "style_hints", None) or {}
    if not isinstance(style_hints, dict):
        return default
    return style_hints.get(key, default)


def compose_round_answer(
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
    answer_strategy: Any,
    approx_tokens: Callable[[str], int],
    build_structured_media_answer: Callable[[str, Any, list[Any]], str],
    summarize_answer: Callable[..., str],
    fallback_retrieval_answer: Callable[[str, list[Any], str], str],
    apply_guardrail_answer_mode: Callable[[str, dict[str, Any]], str],
    append_media_mentions_to_answer: Callable[[str, str, Any, list[Any]], str],
    runtime_state: Any,
) -> AnswerCompositionResult:
    llm_stats: dict[str, Any] = {}
    response_timing_breakdown: dict[str, float] = {}
    degraded_to_retrieval = False
    degrade_reason = ""

    llm_t0 = _time.perf_counter()
    if str(answer_mode.get("mode", "normal") or "normal") == "restricted":
        answer = str(answer_mode.get("answer", "") or "").strip()
        llm_stats["backend"] = backend
        llm_stats["calls"] = 0
        llm_stats["input_tokens_est"] = 0
        llm_stats["prompt_tokens_est"] = 0
        llm_stats["context_tokens_est"] = 0
        llm_stats["memory_tokens_est"] = approx_tokens(memory_context)
        llm_stats["output_tokens_est"] = approx_tokens(answer)
    else:
        try:
            resolved_state = getattr(getattr(runtime_state, "context_resolution", None), "resolved_query_state", None)
            resolved_state = resolved_state if isinstance(resolved_state, dict) else {}
            summarized_answer = summarize_answer(
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
                timing_sink=response_timing_breakdown,
                followup_mode=str(getattr(getattr(runtime_state, "decision", None), "followup_mode", "none") or "none"),
                carry_over_from_previous_turn=bool(resolved_state.get("carry_over_from_previous_turn")),
            )
            answer = summarized_answer
        except Exception as exc:  # noqa: BLE001
            if (backend or "local").strip().lower() == "local":
                degraded_to_retrieval = True
                degrade_reason = str(exc)
                answer = fallback_retrieval_answer(question, tool_results, reason=degrade_reason)
                llm_stats["output_tokens_est"] = approx_tokens(answer)
            else:
                raise

    llm_seconds = _time.perf_counter() - llm_t0
    guardrail_apply_t0 = _time.perf_counter()
    answer = apply_guardrail_answer_mode(answer, answer_mode)
    response_timing_breakdown["final_guardrail_apply_seconds"] = round(
        float(_time.perf_counter() - guardrail_apply_t0), 6
    )
    mention_append_t0 = _time.perf_counter()
    answer = append_media_mentions_to_answer(answer, question, runtime_state, tool_results)
    response_timing_breakdown["mention_append_seconds"] = round(
        float(_time.perf_counter() - mention_append_t0), 6
    )
    llm_stats["output_tokens_est"] = approx_tokens(answer)
    return AnswerCompositionResult(
        answer=answer,
        llm_stats=llm_stats,
        response_timing_breakdown=response_timing_breakdown,
        llm_seconds=llm_seconds,
        degraded_to_retrieval=degraded_to_retrieval,
        degrade_reason=degrade_reason,
    )