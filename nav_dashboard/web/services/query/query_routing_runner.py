from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from ..agent.agent_types import AgentRuntimeState, PlannedToolCall
from .query_understanding import QueryPlanningResult, SessionPreparationResult


@dataclass
class QueryRoutingRunResult:
    session_context: SessionPreparationResult
    planning: QueryPlanningResult
    exceeded: list[dict[str, Any]]


def run_query_routing(
    *,
    question: str,
    session_id: str,
    history: list[dict[str, str]] | None,
    backend: str,
    search_mode: str,
    benchmark_mode: bool,
    trace_id: str,
    prepare_session_context: Callable[..., SessionPreparationResult],
    plan_query_execution: Callable[..., QueryPlanningResult],
    quota_exceeded: Callable[[list[PlannedToolCall], str, dict[str, Any]], list[dict[str, Any]]],
    create_session: Callable[[], dict[str, Any]],
    get_session: Callable[[str], dict[str, Any] | None],
    save_session: Callable[[dict[str, Any]], None],
    derive_session_title: Callable[[str], str],
    now_iso: Callable[[], str],
    new_ephemeral_session_id: Callable[[], str],
    normalize_search_mode: Callable[[str], str],
    resolve_query_profile: Callable[[str], dict[str, Any]],
    load_quota_state: Callable[[], dict[str, Any]],
    plan_tool_calls: Callable[[str, list[dict[str, str]], str, dict[str, Any], str, dict[str, Any]], tuple[list[PlannedToolCall], AgentRuntimeState, dict[str, Any]]],
    serialize_planned_tools: Callable[[list[PlannedToolCall]], list[dict[str, Any]]],
) -> QueryRoutingRunResult:
    session_context = prepare_session_context(
        question=question,
        session_id=session_id,
        history=history,
        benchmark_mode=benchmark_mode,
        create_session=create_session,
        get_session=get_session,
        save_session=save_session,
        derive_session_title=derive_session_title,
        now_iso=now_iso,
        new_ephemeral_session_id=new_ephemeral_session_id,
    )
    planning = plan_query_execution(
        question=question,
        history=session_context.history,
        backend=backend,
        search_mode=search_mode,
        trace_id=trace_id,
        session_id=session_context.session_id,
        normalize_search_mode=normalize_search_mode,
        resolve_query_profile=resolve_query_profile,
        load_quota_state=load_quota_state,
        plan_tool_calls=plan_tool_calls,
        serialize_planned_tools=serialize_planned_tools,
        now_iso=now_iso,
    )
    exceeded = quota_exceeded(planning.planned, backend, planning.quota_state)
    return QueryRoutingRunResult(
        session_context=session_context,
        planning=planning,
        exceeded=exceeded,
    )