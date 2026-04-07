from __future__ import annotations

import time as _time
from dataclasses import dataclass
from typing import Any, Callable

from ..agent.agent_types import AgentRuntimeState, PlannedToolCall, ToolExecution
from ..media.media_strategy import resolve_media_strategy
from .result_layering_contract import PreparedToolResults


@dataclass(frozen=True)
class ToolResultPreparationSignals:
    answer_shape: str = ""
    query_class: str = ""
    subject_scope: str = ""
    media_family: str = ""


def _resolve_decision_signals(runtime_state: AgentRuntimeState, query_classification: dict[str, Any]) -> tuple[Any, str, str, str, str]:
    decision = getattr(runtime_state, "decision", None)
    answer_shape = str(getattr(decision, "answer_shape", "") or (query_classification or {}).get("answer_shape", "") or "")
    query_class = str(getattr(decision, "query_class", "") or (query_classification or {}).get("query_class", "") or "")
    subject_scope = str(getattr(decision, "subject_scope", "") or (query_classification or {}).get("subject_scope", "") or "")
    media_family = str(getattr(decision, "media_family", "") or (query_classification or {}).get("media_family", "") or "")
    return decision, answer_shape, query_class, subject_scope, media_family


def order_tool_results_by_plan(tool_results: list[Any], allowed_plan: list[Any]) -> list[Any]:
    if any(int(getattr(item, "plan_index", -1) or -1) >= 0 for item in tool_results):
        ordered_results = list(tool_results)
        ordered_results.sort(key=lambda item: int(getattr(item, "plan_index", -1) or -1))
        return ordered_results
    order = {call.name: index for index, call in enumerate(allowed_plan)}
    ordered_results = list(tool_results)
    ordered_results.sort(key=lambda item: order.get(getattr(item, "tool", ""), 999))
    return ordered_results


def apply_preparation_reference_limits(
    tool_results: list[Any],
    *,
    normalized_search_mode: str,
    query_profile: dict[str, Any],
    apply_reference_limits: Callable[[list[Any], str, dict[str, Any]], list[Any]],
) -> tuple[list[Any], float]:
    reference_limit_t0 = _time.perf_counter()
    limited_results = apply_reference_limits(tool_results, normalized_search_mode, query_profile)
    return limited_results, (_time.perf_counter() - reference_limit_t0)


def maybe_expand_prepared_media_results(
    tool_results: list[Any],
    *,
    trace_id: str,
    media_family: str,
    answer_shape: str,
    should_expand: bool,
    execute_per_item_expansion: Callable[..., list[Any]],
) -> tuple[list[Any], float]:
    if not should_expand:
        return list(tool_results), 0.0
    per_item_expand_t0 = _time.perf_counter()
    expanded = execute_per_item_expansion(
        list(tool_results),
        trace_id=trace_id,
        media_family=media_family,
        answer_shape=answer_shape,
    )
    return expanded, (_time.perf_counter() - per_item_expand_t0)


def prepare_tool_results(
    *,
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
) -> PreparedToolResults:
    updated_query_profile = dict(query_profile)
    decision, answer_shape, query_class, subject_scope, media_family = _resolve_decision_signals(runtime_state, query_classification)
    if answer_shape:
        updated_query_profile["answer_shape"] = answer_shape

    prepared_tool_results = order_tool_results_by_plan(tool_results, allowed_plan)
    prepared_tool_results, reference_limit_seconds = apply_preparation_reference_limits(
        prepared_tool_results,
        normalized_search_mode=normalized_search_mode,
        query_profile=updated_query_profile,
        apply_reference_limits=apply_reference_limits,
    )
    media_strategy = resolve_media_strategy(
        decision,
        query_class=query_class,
        subject_scope=subject_scope,
        answer_shape=answer_shape,
        media_family=media_family,
    )
    prepared_tool_results, per_item_expansion_seconds = maybe_expand_prepared_media_results(
        prepared_tool_results,
        trace_id=trace_id,
        media_family=media_family,
        answer_shape=answer_shape,
        should_expand=bool(media_strategy.should_run_per_item_expansion and not defer_per_item_expansion),
        execute_per_item_expansion=execute_per_item_expansion,
    )
    return PreparedToolResults(
        query_profile=updated_query_profile,
        tool_results=prepared_tool_results,
        answer_shape=answer_shape,
        query_class=query_class,
        subject_scope=subject_scope,
        media_family=media_family,
        reference_limit_seconds=reference_limit_seconds,
        per_item_expansion_seconds=per_item_expansion_seconds,
    )