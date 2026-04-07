from __future__ import annotations

"""Minimal router facade.

Owner logic lives in router_core. This module wires router dependencies to
their direct owner modules instead of routing back through agent_service.
"""

import time as _time
from typing import Any

from . import router_core as core
from . import media_helpers, router_helpers
from ..infra import runtime_infra
from ...media.entity_resolver import resolve_media_entities as _er_resolve_media_entity

RouterDeps = core.RouterDeps
RouterDecision = core.RouterDecision
PlannedToolCall = core.PlannedToolCall
RouterContextResolution = core.RouterContextResolution
build_default_router_deps = core.build_default_router_deps
resolve_router_deps = core.resolve_router_deps
_resolve_query_profile = core._resolve_query_profile


def _llm_chat(*args, **kwargs):
    return runtime_infra._llm_chat(*args, **kwargs)


def _find_previous_trace_context(*args, **kwargs):
    return router_helpers._find_previous_trace_context(*args, **kwargs)


def _classify_media_query_with_llm(*args, **kwargs):
    return core._classify_media_query_with_llm(*args, **kwargs)


def _rewrite_tool_queries_with_llm(*args, **kwargs):
    return core._rewrite_tool_queries_with_llm(*args, **kwargs)


def _apply_router_semantic_repairs(*args, **kwargs):
    return core._apply_router_semantic_repairs(*args, **kwargs)


def _build_router_deps() -> RouterDeps:
    base = core.build_default_router_deps()
    return RouterDeps(
        llm_chat=_llm_chat,
        find_previous_trace_context=_find_previous_trace_context,
        apply_router_semantic_repairs=_apply_router_semantic_repairs,
        classify_media_query_with_llm=_classify_media_query_with_llm,
        rewrite_tool_queries_with_llm=_rewrite_tool_queries_with_llm,
        resolve_media_entity=_er_resolve_media_entity,
        resolve_library_aliases=media_helpers._resolve_library_aliases,
        planner_router_semantic_deps=base.planner_router_semantic_deps,
        perf_counter=_time.perf_counter,
        normalize_timing_breakdown=base.normalize_timing_breakdown,
    )


def _build_router_decision(
    question: str,
    history: list[dict[str, str]],
    quota_state: dict[str, Any],
    query_profile: dict[str, Any],
    deps: RouterDeps | None = None,
) -> tuple[RouterDecision, dict[str, Any], dict[str, Any]]:
    return core._build_router_decision(
        question,
        history,
        quota_state,
        query_profile,
        deps=deps or _build_router_deps(),
    )


def _resolve_router_context(
    original_question: str,
    history: list[dict[str, str]],
    decision: RouterDecision,
    previous_state: dict[str, Any],
    planned_tools: list[PlannedToolCall],
) -> RouterContextResolution:
    return core._resolve_router_context(
        original_question,
        history,
        decision,
        previous_state,
        planned_tools,
    )


def _router_decision_to_query_classification(
    decision: RouterDecision,
    llm_media: dict[str, Any],
    previous_state: dict[str, Any],
    query_profile: dict[str, Any],
) -> dict[str, Any]:
    return core._router_decision_to_query_classification(decision, llm_media, previous_state, query_profile)


def _classify_query_type(query: str, quota_state: dict[str, Any], query_profile: dict[str, Any]) -> dict[str, Any]:
    return core._classify_query_type(query, quota_state, query_profile)


def _build_router_decision_path(
    query_classification: dict[str, Any],
    search_mode: str,
    planned_tools: list[PlannedToolCall],
    tool_results: list[core.ToolExecution],
) -> tuple[str, list[str]]:
    return core._build_router_decision_path(query_classification, search_mode, planned_tools, tool_results)


__all__ = [
    "RouterContextResolution",
    "RouterDecision",
    "RouterDeps",
    "PlannedToolCall",
    "build_default_router_deps",
    "resolve_router_deps",
    "_apply_router_semantic_repairs",
    "_build_router_decision",
    "_build_router_decision_path",
    "_classify_media_query_with_llm",
    "_classify_query_type",
    "_find_previous_trace_context",
    "_resolve_query_profile",
    "_resolve_router_context",
    "_rewrite_tool_queries_with_llm",
    "_router_decision_to_query_classification",
]