from __future__ import annotations

import time as _time
from dataclasses import dataclass
from typing import Any

from ..agent.agent_types import ConfirmationPayload, PlannedToolCall, ToolExecution, TOOL_SEARCH_WEB


@dataclass
class ToolExecutionPhaseResult:
    allowed_plan: list[PlannedToolCall]
    skipped_due_quota: list[str]
    tool_results: list[ToolExecution]
    tool_execution_seconds: float


def build_confirmation_payload(
    *,
    trace_id: str,
    session_id: str,
    exceeded: list[dict[str, Any]],
    planned_tools: list[dict[str, Any]],
) -> ConfirmationPayload:
    return ConfirmationPayload(
        trace_id=trace_id,
        session_id=session_id,
        exceeded=list(exceeded),
        planned_tools=list(planned_tools),
    )


def resolve_allowed_plan(
    planned: list[PlannedToolCall],
    exceeded: list[dict[str, Any]],
    *,
    deny_over_quota: bool,
) -> tuple[list[PlannedToolCall], list[str]]:
    if not exceeded or not deny_over_quota:
        return list(planned), []
    kinds = {str(item.get("kind", "")) for item in exceeded}
    allowed_plan: list[PlannedToolCall] = []
    skipped_due_quota: list[str] = []
    for call in planned:
        if call.name == TOOL_SEARCH_WEB and "web_search" in kinds:
            skipped_due_quota.append(call.name)
            continue
        allowed_plan.append(call)
    return allowed_plan, skipped_due_quota


def execute_tool_phase(
    *,
    allowed_plan: list[PlannedToolCall],
    query_profile: dict[str, Any],
    trace_id: str,
    execute_tool_plan: Any,
) -> ToolExecutionPhaseResult:
    tool_exec_t0 = _time.perf_counter()
    tool_results = execute_tool_plan(allowed_plan, query_profile, trace_id)
    return ToolExecutionPhaseResult(
        allowed_plan=list(allowed_plan),
        skipped_due_quota=[],
        tool_results=tool_results,
        tool_execution_seconds=_time.perf_counter() - tool_exec_t0,
    )