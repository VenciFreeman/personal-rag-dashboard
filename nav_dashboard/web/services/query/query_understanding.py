from __future__ import annotations

import time as _time
from dataclasses import dataclass
from typing import Any, Callable

from ..agent.agent_types import AgentRuntimeState, PlannedToolCall, QueryProfile, QuotaState


@dataclass
class SessionPreparationResult:
    session_id: str
    history: list[dict[str, str]]
    session_prepare_seconds: float


@dataclass
class QueryPlanningResult:
    normalized_search_mode: str
    query_profile: QueryProfile
    quota_state: QuotaState
    planned: list[PlannedToolCall]
    runtime_state: AgentRuntimeState
    query_classification: dict[str, Any]
    query_profile_seconds: float
    tool_planning_seconds: float
    planning_seconds: float
    planning_timing_breakdown: dict[str, float]
    debug_trace: dict[str, Any]


def prepare_session_context(
    *,
    question: str,
    session_id: str,
    history: list[dict[str, str]] | None,
    benchmark_mode: bool,
    create_session: Callable[[], dict[str, Any]],
    get_session: Callable[[str], dict[str, Any] | None],
    save_session: Callable[[dict[str, Any]], None],
    derive_session_title: Callable[[str], str],
    now_iso: Callable[[], str],
    new_ephemeral_session_id: Callable[[], str],
) -> SessionPreparationResult:
    session_prepare_t0 = _time.perf_counter()
    hist = history or []
    sid = (session_id or "").strip()
    if benchmark_mode:
        sid = sid or new_ephemeral_session_id()
    else:
        if not sid:
            sid = str(create_session().get("id", "")).strip()
        if not sid:
            raise RuntimeError("failed to create session")

        session = get_session(sid)
        if session and str(session.get("title", "")).strip() in {"", "新会话"}:
            if not bool(session.get("title_locked", False)):
                session["title"] = derive_session_title(question)
                session["updated_at"] = now_iso()
                save_session(session)
        if session and isinstance(session.get("messages"), list):
            hist = [
                {
                    "role": str(message.get("role", "")),
                    "content": str(message.get("text", "")),
                    "trace_id": str(message.get("trace_id", "")),
                }
                for message in session.get("messages", [])
            ]

    return SessionPreparationResult(
        session_id=sid,
        history=hist,
        session_prepare_seconds=_time.perf_counter() - session_prepare_t0,
    )


def plan_query_execution(
    *,
    question: str,
    history: list[dict[str, str]],
    backend: str,
    search_mode: str,
    trace_id: str,
    session_id: str,
    normalize_search_mode: Callable[[str], str],
    resolve_query_profile: Callable[[str], dict[str, Any]],
    load_quota_state: Callable[[], dict[str, Any]],
    plan_tool_calls: Callable[[str, list[dict[str, str]], str, dict[str, Any], str, dict[str, Any]], tuple[list[PlannedToolCall], AgentRuntimeState, dict[str, Any]]],
    serialize_planned_tools: Callable[[list[PlannedToolCall]], list[dict[str, Any]]],
    now_iso: Callable[[], str],
) -> QueryPlanningResult:
    normalized_search_mode = normalize_search_mode(search_mode)
    query_profile_t0 = _time.perf_counter()
    query_profile = QueryProfile.from_mapping(resolve_query_profile(question))
    query_profile_seconds = _time.perf_counter() - query_profile_t0
    quota_state = QuotaState.from_mapping(load_quota_state())
    plan_t0 = _time.perf_counter()
    planned, runtime_state, query_classification = plan_tool_calls(
        question,
        history,
        backend,
        quota_state,
        normalized_search_mode,
        query_profile,
    )
    tool_planning_seconds = _time.perf_counter() - plan_t0
    planner_snapshot = {}
    if runtime_state is not None:
        planner_snapshot = dict(runtime_state.planning_artifact.planner_snapshot or {})
        if not planner_snapshot and runtime_state.context_resolution:
            planner_snapshot = dict(runtime_state.context_resolution.planner_snapshot or {})
    planning_timing_breakdown = planner_snapshot.get("timing_breakdown") if isinstance(planner_snapshot, dict) else {}
    if not isinstance(planning_timing_breakdown, dict):
        planning_timing_breakdown = {}
    debug_trace = {
        "timestamp": now_iso(),
        "trace_id": trace_id,
        "session_id": session_id,
        "question": question,
        "search_mode": normalized_search_mode,
        "query_profile": query_profile.to_dict(),
        "query_classification": query_classification,
        "backend": backend,
        "history": history,
        "planned_tools": serialize_planned_tools(planned),
        "planning_timing_breakdown": planning_timing_breakdown,
        "reranker": {"status": "not_applicable"},
    }
    return QueryPlanningResult(
        normalized_search_mode=normalized_search_mode,
        query_profile=query_profile,
        quota_state=quota_state,
        planned=planned,
        runtime_state=runtime_state,
        query_classification=query_classification,
        query_profile_seconds=query_profile_seconds,
        tool_planning_seconds=tool_planning_seconds,
        planning_seconds=query_profile_seconds + tool_planning_seconds,
        planning_timing_breakdown=planning_timing_breakdown,
        debug_trace=debug_trace,
    )