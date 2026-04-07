from __future__ import annotations

from typing import Any, Callable, Iterator

from ..agent_types import AnswerDeps, ExecutionDeps, ObservabilityDeps, PlannerDeps
from ..round_lifecycle_runner import RoundLifecycleDeps


def build_round_lifecycle_deps(
    *,
    round_lifecycle_builder: Callable[..., RoundLifecycleDeps],
    request_base_url: str,
    planner_deps: PlannerDeps,
    execution_deps: ExecutionDeps,
    answer_deps: AnswerDeps,
    observability_deps: ObservabilityDeps,
) -> RoundLifecycleDeps:
    return round_lifecycle_builder(
        request_base_url=request_base_url,
        planner_deps=planner_deps,
        execution_deps=execution_deps,
        answer_deps=answer_deps,
        observability_deps=observability_deps,
    )


def run_agent_round(
    *,
    build_round_lifecycle_deps: Callable[..., RoundLifecycleDeps],
    run_round_sync: Callable[..., dict[str, Any]],
    question: str,
    session_id: str = "",
    history: list[dict[str, str]] | None = None,
    backend: str = "local",
    search_mode: str = "local_only",
    confirm_over_quota: bool = False,
    deny_over_quota: bool = False,
    debug: bool = False,
    request_base_url: str = "",
    benchmark_mode: bool = False,
    trace_id: str = "",
) -> dict[str, Any]:
    return run_round_sync(
        deps=build_round_lifecycle_deps(request_base_url=request_base_url),
        question=question,
        session_id=session_id,
        history=history,
        backend=backend,
        search_mode=search_mode,
        confirm_over_quota=confirm_over_quota,
        deny_over_quota=deny_over_quota,
        debug=debug,
        benchmark_mode=benchmark_mode,
        trace_id=trace_id,
    )


def run_agent_round_stream(
    *,
    build_round_lifecycle_deps: Callable[..., RoundLifecycleDeps],
    run_round_stream: Callable[..., Iterator[dict[str, Any]]],
    question: str,
    session_id: str = "",
    history: list[dict[str, str]] | None = None,
    backend: str = "local",
    search_mode: str = "local_only",
    confirm_over_quota: bool = False,
    deny_over_quota: bool = False,
    debug: bool = False,
    request_base_url: str = "",
    benchmark_mode: bool = False,
    trace_id: str = "",
) -> Iterator[dict[str, Any]]:
    yield from run_round_stream(
        deps=build_round_lifecycle_deps(request_base_url=request_base_url),
        question=question,
        session_id=session_id,
        history=history,
        backend=backend,
        search_mode=search_mode,
        confirm_over_quota=confirm_over_quota,
        deny_over_quota=deny_over_quota,
        debug=debug,
        benchmark_mode=benchmark_mode,
        trace_id=trace_id,
    )
