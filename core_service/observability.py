from __future__ import annotations

from pathlib import Path
from typing import Any

from .trace_store import get_trace_record
from .trace_store import list_trace_record_paths
from .trace_store import render_trace_export
from .trace_store import write_trace_record
from .usage_sync import notify_nav_dashboard_usage


def record_trace(record: dict[str, Any]) -> None:
    write_trace_record(record)


def record_usage(
    *,
    web_search_delta: int = 0,
    deepseek_delta: int = 0,
    count_daily: bool = True,
    events: list[dict[str, Any]] | None = None,
    timeout: float = 3.0,
    background: bool = False,
) -> None:
    notify_nav_dashboard_usage(
        web_search_delta=web_search_delta,
        deepseek_delta=deepseek_delta,
        count_daily=count_daily,
        events=events,
        timeout=timeout,
        background=background,
    )


def list_trace_records() -> list[Path]:
    return list_trace_record_paths()


__all__ = [
    "get_trace_record",
    "list_trace_records",
    "list_trace_record_paths",
    "notify_nav_dashboard_usage",
    "record_trace",
    "record_usage",
    "render_trace_export",
    "write_trace_record",
]