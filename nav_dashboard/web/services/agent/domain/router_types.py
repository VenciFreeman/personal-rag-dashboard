from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from .. import agent_types


AgentRuntimeState = agent_types.AgentRuntimeState
PlannedToolCall = agent_types.PlannedToolCall
RouterContextResolution = agent_types.RouterContextResolution
RouterDecision = agent_types.RouterDecision
ToolExecution = agent_types.ToolExecution


@dataclass(frozen=True)
class RouterDeps:
    llm_chat: Callable[..., str]
    find_previous_trace_context: Callable[[list[dict[str, str]]], dict[str, Any]]
    apply_router_semantic_repairs: Callable[[str, RouterDecision, dict[str, Any]], RouterDecision]
    resolve_library_aliases: Callable[..., dict[str, Any]]
    planner_router_semantic_deps: Any
    perf_counter: Callable[[], float]
    normalize_timing_breakdown: Callable[[dict[str, Any] | None], dict[str, float]]
    classify_media_query_with_llm: Callable[..., dict[str, Any]] | None = None
    rewrite_tool_queries_with_llm: Callable[..., dict[str, str]] | None = None
    resolve_media_entity: Callable[..., Any] | None = None


__all__ = [
    "AgentRuntimeState",
    "PlannedToolCall",
    "RouterContextResolution",
    "RouterDecision",
    "RouterDeps",
    "ToolExecution",
]