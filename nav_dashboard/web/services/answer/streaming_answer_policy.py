from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Literal

from ..planner import planner_contracts


@dataclass(frozen=True)
class StreamingAnswerPlan:
    mode: Literal["restricted", "llm"]
    answer: str = ""
    fanout_phase: Literal["none", "after_structured", "before_llm"] = "none"


def _should_stream_external_fanout(answer_strategy: Any, *, answer_shape: str) -> bool:
    style_hints = getattr(answer_strategy, "style_hints", None) or {}
    if isinstance(answer_strategy, dict):
        style_hints = answer_strategy.get("style_hints") or {}
    if not isinstance(style_hints, dict):
        style_hints = {}
    if bool(style_hints.get("include_external")):
        return True
    return answer_shape == planner_contracts.ROUTER_ANSWER_SHAPE_LIST_PLUS_EXPAND


def plan_streaming_answer(
    *,
    question: str,
    runtime_state: Any,
    tool_results: list[Any],
    answer_mode: dict[str, Any],
    answer_strategy: Any,
    answer_shape: str,
    build_structured_media_answer_chunks: Callable[[str, Any, list[Any]], list[str]],
) -> StreamingAnswerPlan:
    if str(answer_mode.get("mode", "normal") or "normal") == "restricted":
        return StreamingAnswerPlan(
            mode="restricted",
            answer=str(answer_mode.get("answer", "") or "").strip(),
        )

    return StreamingAnswerPlan(
        mode="llm",
        fanout_phase="before_llm"
        if answer_shape == planner_contracts.ROUTER_ANSWER_SHAPE_LIST_PLUS_EXPAND
        else "none",
    )