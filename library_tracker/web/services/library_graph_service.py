from __future__ import annotations

from typing import Any

from . import library_service_core as core


def rebuild_library_graph(*, progress_callback=None) -> dict[str, Any]:
    return core.rebuild_library_graph(progress_callback=progress_callback)


def sync_missing_library_graph(*, progress_callback=None) -> dict[str, Any]:
    return core.sync_missing_library_graph(progress_callback=progress_callback)


def start_graph_job(*, full: bool = False) -> dict[str, Any]:
    return core.start_graph_job(full=full)


def get_graph_job(job_id: str) -> dict[str, Any] | None:
    return core.get_graph_job(job_id)
