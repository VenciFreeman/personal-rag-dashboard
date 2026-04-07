from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from ..agent.agent_types import PlannedToolCall, ToolExecution


@dataclass
class ToolExecutionRunnerResult:
    allowed_plan: list[PlannedToolCall]
    skipped_due_quota: list[str]
    tool_results: list[ToolExecution]
    tool_execution_seconds: float


def run_tool_execution_stage(
    *,
    planned: list[PlannedToolCall],
    exceeded: list[dict[str, Any]],
    deny_over_quota: bool,
    benchmark_mode: bool,
    session_id: str,
    question: str,
    query_profile: dict[str, Any],
    trace_id: str,
    resolve_allowed_plan: Callable[..., tuple[list[PlannedToolCall], list[str]]],
    execute_tool_phase: Callable[..., Any],
    execute_tool_plan: Any,
    append_message: Callable[[str, str, str], None],
) -> ToolExecutionRunnerResult:
    allowed_plan, skipped_due_quota = resolve_allowed_plan(
        planned,
        exceeded,
        deny_over_quota=deny_over_quota,
    )
    if not benchmark_mode:
        append_message(session_id, "user", question)
    tool_phase = execute_tool_phase(
        allowed_plan=allowed_plan,
        query_profile=query_profile,
        trace_id=trace_id,
        execute_tool_plan=execute_tool_plan,
    )
    return ToolExecutionRunnerResult(
        allowed_plan=allowed_plan,
        skipped_due_quota=list(skipped_due_quota),
        tool_results=list(tool_phase.tool_results),
        tool_execution_seconds=float(tool_phase.tool_execution_seconds or 0.0),
    )