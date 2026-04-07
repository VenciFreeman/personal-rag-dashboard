from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from ..answer.answer_generation_runner import PreparedAnswerContext, prepare_answer_context
from .agent_types import AgentRuntimeState, ConfirmationPayload, PlannedToolCall, QueryProfile, QuotaState
from ..query.query_routing_runner import QueryRoutingRunResult
from ..query.query_routing_runner import run_query_routing
from ..tooling.tool_plan_runner import ToolPlanExecutionDeps, ToolPlanRunResult, execute_tool_plan, update_debug_trace_with_tool_plan


@dataclass
class RoundRoutingDeps:
    prepare_session_context: Callable[..., Any]
    plan_query_execution: Callable[..., Any]
    quota_exceeded: Callable[..., list[dict[str, Any]]]
    create_session: Callable[[], dict[str, Any]]
    get_session: Callable[[str], dict[str, Any] | None]
    save_session: Callable[[dict[str, Any]], None]
    derive_session_title: Callable[[str], str]
    now_iso: Callable[[], str]
    new_ephemeral_session_id: Callable[[], str]
    normalize_search_mode: Callable[[str], str]
    resolve_query_profile: Callable[[str], dict[str, Any]]
    load_quota_state: Callable[[], dict[str, Any]]
    plan_tool_calls: Callable[..., Any]
    serialize_planned_tools: Callable[[list[PlannedToolCall]], list[dict[str, Any]]]


@dataclass
class RoundPreparationResult:
    round_context: AgentRoundContext
    preview_allowed_plan: list[PlannedToolCall]
    preview_skipped_due_quota: list[str]
    tool_plan: ToolPlanRunResult | None = None
    answer_context: PreparedAnswerContext | None = None
    confirmation_payload: ConfirmationPayload | None = None


@dataclass
class AgentRoundContext:
    question: str
    trace_id: str
    session_id: str
    history: list[dict[str, str]]
    normalized_search_mode: str
    query_profile: QueryProfile
    quota_state: QuotaState
    planned: list[PlannedToolCall]
    runtime_state: AgentRuntimeState
    query_classification: dict[str, Any]
    debug_trace: dict[str, Any]
    exceeded: list[dict[str, Any]]
    session_prepare_seconds: float
    query_profile_seconds: float
    tool_planning_seconds: float
    planning_seconds: float
    planning_timing_breakdown: dict[str, float] = field(default_factory=dict)


def normalize_round_request(
    *,
    question: str,
    trace_id: str,
    normalize_trace_id: Any,
) -> tuple[str, str]:
    normalized_question = str(question or "").strip()
    if not normalized_question:
        raise ValueError("question is required")
    return normalized_question, str(normalize_trace_id(trace_id) or "").strip()


def build_round_context(
    *,
    question: str,
    trace_id: str,
    routing_run: QueryRoutingRunResult,
) -> AgentRoundContext:
    session_ctx = routing_run.session_context
    planning = routing_run.planning
    return AgentRoundContext(
        question=question,
        trace_id=trace_id,
        session_id=session_ctx.session_id,
        history=list(session_ctx.history),
        normalized_search_mode=planning.normalized_search_mode,
        query_profile=planning.query_profile,
        quota_state=planning.quota_state,
        planned=list(planning.planned),
        runtime_state=planning.runtime_state,
        query_classification=planning.query_classification,
        debug_trace=planning.debug_trace,
        exceeded=list(routing_run.exceeded),
        session_prepare_seconds=float(session_ctx.session_prepare_seconds or 0.0),
        query_profile_seconds=float(planning.query_profile_seconds or 0.0),
        tool_planning_seconds=float(planning.tool_planning_seconds or 0.0),
        planning_seconds=float(session_ctx.session_prepare_seconds or 0.0) + float(planning.planning_seconds or 0.0),
        planning_timing_breakdown=dict(getattr(planning, "planning_timing_breakdown", {}) or {}),
    )


def build_confirmation_response(
    *,
    round_context: AgentRoundContext,
    serialize_planned_tools: Any,
    build_confirmation_payload: Any,
) -> ConfirmationPayload:
    return build_confirmation_payload(
        trace_id=round_context.trace_id,
        session_id=round_context.session_id,
        exceeded=round_context.exceeded,
        planned_tools=serialize_planned_tools(round_context.planned),
    )


def prepare_round_answer(
    *,
    question: str,
    trace_id: str,
    session_id: str,
    history: list[dict[str, str]] | None,
    backend: str,
    search_mode: str,
    benchmark_mode: bool,
    confirm_over_quota: bool,
    deny_over_quota: bool,
    normalize_trace_id: Callable[[str], str],
    routing_deps: RoundRoutingDeps,
    build_confirmation_payload: Callable[..., ConfirmationPayload],
    resolve_allowed_plan: Callable[..., tuple[list[PlannedToolCall], list[str]]],
    execute_tool_phase: Callable[..., Any],
    execute_tool_plan_boundary: Callable[..., Any],
    tool_plan_deps: ToolPlanExecutionDeps,
    append_message: Callable[..., None],
    build_memory_context: Callable[[str], str],
    approx_tokens: Callable[[str], int],
    defer_per_item_expansion: bool = False,
) -> RoundPreparationResult:
    normalized_question, resolved_trace_id = normalize_round_request(
        question=question,
        trace_id=trace_id,
        normalize_trace_id=normalize_trace_id,
    )
    routing_run = run_query_routing(
        question=normalized_question,
        session_id=session_id,
        history=history,
        backend=backend,
        search_mode=search_mode,
        benchmark_mode=benchmark_mode,
        trace_id=resolved_trace_id,
        prepare_session_context=routing_deps.prepare_session_context,
        plan_query_execution=routing_deps.plan_query_execution,
        quota_exceeded=routing_deps.quota_exceeded,
        create_session=routing_deps.create_session,
        get_session=routing_deps.get_session,
        save_session=routing_deps.save_session,
        derive_session_title=routing_deps.derive_session_title,
        now_iso=routing_deps.now_iso,
        new_ephemeral_session_id=routing_deps.new_ephemeral_session_id,
        normalize_search_mode=routing_deps.normalize_search_mode,
        resolve_query_profile=routing_deps.resolve_query_profile,
        load_quota_state=routing_deps.load_quota_state,
        plan_tool_calls=routing_deps.plan_tool_calls,
        serialize_planned_tools=routing_deps.serialize_planned_tools,
    )
    round_context = build_round_context(
        question=normalized_question,
        trace_id=resolved_trace_id,
        routing_run=routing_run,
    )
    if round_context.exceeded and not confirm_over_quota and not deny_over_quota:
        return RoundPreparationResult(
            round_context=round_context,
            preview_allowed_plan=[],
            preview_skipped_due_quota=[],
            confirmation_payload=build_confirmation_response(
                round_context=round_context,
                serialize_planned_tools=routing_deps.serialize_planned_tools,
                build_confirmation_payload=build_confirmation_payload,
            ),
        )

    preview_allowed_plan, preview_skipped_due_quota = resolve_allowed_plan(
        round_context.planned,
        round_context.exceeded,
        deny_over_quota=deny_over_quota,
    )
    if not benchmark_mode:
        append_message(round_context.session_id, "user", normalized_question)

    tool_plan = execute_tool_plan(
        planned=round_context.planned,
        exceeded=round_context.exceeded,
        deny_over_quota=deny_over_quota,
        benchmark_mode=benchmark_mode,
        session_id=round_context.session_id,
        question=normalized_question,
        query_profile=round_context.query_profile,
        trace_id=resolved_trace_id,
        normalized_search_mode=round_context.normalized_search_mode,
        quota_state=round_context.quota_state,
        query_classification=round_context.query_classification,
        runtime_state=round_context.runtime_state,
        resolve_allowed_plan=resolve_allowed_plan,
        execute_tool_phase=execute_tool_phase,
        execute_tool_plan_boundary=execute_tool_plan_boundary,
        deps=tool_plan_deps,
        allowed_plan=preview_allowed_plan,
        skipped_due_quota=preview_skipped_due_quota,
        defer_per_item_expansion=defer_per_item_expansion,
    )
    update_debug_trace_with_tool_plan(round_context.debug_trace, tool_plan)
    answer_context = prepare_answer_context(
        session_id=round_context.session_id,
        benchmark_mode=benchmark_mode,
        debug_trace=round_context.debug_trace,
        build_memory_context=build_memory_context,
        approx_tokens=approx_tokens,
    )
    return RoundPreparationResult(
        round_context=round_context,
        preview_allowed_plan=list(preview_allowed_plan),
        preview_skipped_due_quota=list(preview_skipped_due_quota),
        tool_plan=tool_plan,
        answer_context=answer_context,
    )