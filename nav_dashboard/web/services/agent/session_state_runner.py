from __future__ import annotations

import time as _time
from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class SessionPersistResult:
    session_persist_seconds: float


def persist_session_artifacts(
    *,
    benchmark_mode: bool,
    session_id: str,
    question: str,
    final_answer: str,
    trace_id: str,
    append_message: Callable[..., None],
    update_memory_for_session: Callable[[str], None],
    schedule_generated_session_title: Callable[..., None],
    auto_queue_bug_tickets: Callable[..., None],
) -> SessionPersistResult:
    if benchmark_mode:
        return SessionPersistResult(session_persist_seconds=0.0)
    persist_t0 = _time.perf_counter()
    append_message(session_id, "assistant", final_answer, trace_id=trace_id)
    update_memory_for_session(session_id)
    schedule_generated_session_title(session_id, question, final_answer, lock=True)
    auto_queue_bug_tickets(final_answer, session_id=session_id, trace_id=trace_id)
    return SessionPersistResult(
        session_persist_seconds=_time.perf_counter() - persist_t0,
    )


def record_metrics_safe(
    *,
    benchmark_mode: bool,
    record_agent_metrics: Callable[..., None],
    metric_kwargs: dict[str, Any],
) -> float:
    if benchmark_mode:
        return 0.0
    try:
        metrics_t0 = _time.perf_counter()
        record_agent_metrics(**metric_kwargs)
        return _time.perf_counter() - metrics_t0
    except Exception:
        return 0.0