from __future__ import annotations

import time
from typing import Any

from . import library_alias_lifecycle_service
from . import library_embedding_refresh_service
from . import library_graph_service
from . import library_service_core as core


def rebuild_library_graph(*, progress_callback=None) -> dict[str, Any]:
    return library_graph_service.rebuild_library_graph(progress_callback=progress_callback)


def sync_missing_library_graph(*, progress_callback=None) -> dict[str, Any]:
    return library_graph_service.sync_missing_library_graph(progress_callback=progress_callback)


def start_graph_job(*, full: bool = False) -> dict[str, Any]:
    return library_graph_service.start_graph_job(full=full)


def get_graph_job(job_id: str) -> dict[str, Any] | None:
    return library_graph_service.get_graph_job(job_id)


def trigger_alias_proposal_background(item_ids: list[str] | None = None) -> None:
    library_alias_lifecycle_service.trigger_alias_proposal_background(item_ids=item_ids)


def trigger_alias_maintenance_background() -> dict[str, Any]:
    return library_alias_lifecycle_service.trigger_alias_maintenance_background()


def enqueue_item_refresh(item_ids: list[str] | None) -> dict[str, Any]:
    return library_alias_lifecycle_service.enqueue_item_refresh(item_ids)


def refresh_pending_embeddings() -> dict[str, Any]:
    return library_embedding_refresh_service.refresh_pending_embeddings()


def refresh_embeddings_for_item_ids(item_ids: list[str]) -> dict[str, Any]:
    return library_embedding_refresh_service.refresh_embeddings_for_item_ids(item_ids)


def _run_async_refresh_pipeline_for_item_id(item_id: str) -> dict[str, Any]:
    result = library_embedding_refresh_service.refresh_embeddings_for_item_ids([item_id])
    scanned = int(result.get("scanned", 0) or 0)
    failed = int(result.get("failed", 0) or 0)
    if scanned <= 0:
        return {
            "ok": True,
            "skipped": True,
            "reason": "item_not_found",
            "embedding": result,
            "alias": {"ok": True, "skipped": True, "reason": "item_not_found"},
        }
    if failed > 0:
        raise RuntimeError(f"embedding_refresh_failed:{item_id}")
    alias_result = library_alias_lifecycle_service.run_alias_refresh_for_item_id(item_id)
    return {"ok": True, "embedding": result, "alias": alias_result}


def _process_async_refresh_item(item_id: str, *, sleep_func=time.sleep) -> dict[str, Any]:
    last_error = ""
    for attempt in range(1, core._ASYNC_REFRESH_MAX_RETRIES + 1):
        try:
            result = _run_async_refresh_pipeline_for_item_id(item_id)
            result["attempts"] = attempt
            return result
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
            print(f"[ASYNC_REFRESH_WARNING] {item_id} attempt {attempt}/{core._ASYNC_REFRESH_MAX_RETRIES}: {exc}")
            if attempt < core._ASYNC_REFRESH_MAX_RETRIES and core._ASYNC_REFRESH_RETRY_DELAY_SECONDS > 0:
                sleep_func(core._ASYNC_REFRESH_RETRY_DELAY_SECONDS)
    return {"ok": False, "item_id": item_id, "attempts": core._ASYNC_REFRESH_MAX_RETRIES, "error": last_error}