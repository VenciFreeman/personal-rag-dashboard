from __future__ import annotations

import os
import queue
import threading
from typing import Any

from . import library_service_core as core


_ASYNC_REFRESH_QUEUE: queue.Queue[str] = queue.Queue()
_ASYNC_REFRESH_QUEUE_LOCK = threading.RLock()
_ASYNC_REFRESH_PENDING_IDS: set[str] = set()
_ASYNC_REFRESH_WORKER: threading.Thread | None = None
_ALIAS_MAINTENANCE_LOCK = threading.RLock()
_ALIAS_MAINTENANCE_RUNNING = False


def trigger_alias_proposal_background(item_ids: list[str] | None = None) -> None:
    llm_url = (os.getenv("LIBRARY_TRACKER_LOCAL_LLM_URL", "") or "").strip() or str(core._CORE_SETTINGS.local_llm_url or "").strip()
    if not llm_url:
        return

    normalized_ids = [str(item_id or "").strip() for item_id in (item_ids or []) if str(item_id or "").strip()]

    def _run_generate() -> None:
        try:
            all_items = core._iter_all_items()
            if normalized_ids:
                core.library_alias_store.generate_proposals_for_item_ids(all_items, item_ids=normalized_ids)
            else:
                core.library_alias_store.generate_proposals_for_items(all_items, persist=True)
            core.library_alias_store.prune_stale_alias_entries(all_items)
        except Exception:
            pass

    threading.Thread(target=_run_generate, name="library-alias-proposal-trigger", daemon=True).start()


def _normalize_async_refresh_item_ids(item_ids: list[str] | None) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for item_id in item_ids or []:
        clean = str(item_id or "").strip()
        if not clean or clean in seen:
            continue
        seen.add(clean)
        normalized.append(clean)
    return normalized


def enqueue_item_refresh(item_ids: list[str] | None) -> dict[str, Any]:
    normalized_ids = _normalize_async_refresh_item_ids(item_ids)
    if not normalized_ids:
        return {"queued_count": 0, "queued_item_ids": [], "queue_depth": _ASYNC_REFRESH_QUEUE.qsize()}
    _ensure_async_refresh_worker()
    queued_item_ids: list[str] = []
    with _ASYNC_REFRESH_QUEUE_LOCK:
        for item_id in normalized_ids:
            if item_id in _ASYNC_REFRESH_PENDING_IDS:
                continue
            _ASYNC_REFRESH_PENDING_IDS.add(item_id)
            _ASYNC_REFRESH_QUEUE.put(item_id)
            queued_item_ids.append(item_id)
        queue_depth = _ASYNC_REFRESH_QUEUE.qsize()
    return {"queued_count": len(queued_item_ids), "queued_item_ids": queued_item_ids, "queue_depth": queue_depth}


def run_alias_refresh_for_item_id(item_id: str) -> dict[str, Any]:
    llm_url = (os.getenv("LIBRARY_TRACKER_LOCAL_LLM_URL", "") or "").strip() or str(core._CORE_SETTINGS.local_llm_url or "").strip()
    all_items = core._iter_all_items()
    cleanup = core.library_alias_store.prune_stale_alias_entries(all_items)
    if not llm_url:
        return {"ok": True, "skipped": True, "reason": "llm_not_configured", "cleanup": cleanup}
    result = core.library_alias_store.generate_proposals_for_item_ids(all_items, item_ids=[item_id])
    result["cleanup"] = cleanup
    return result


def trigger_alias_maintenance_background() -> dict[str, Any]:
    global _ALIAS_MAINTENANCE_RUNNING
    with _ALIAS_MAINTENANCE_LOCK:
        if _ALIAS_MAINTENANCE_RUNNING:
            return {"queued": False, "reason": "already_running"}
        _ALIAS_MAINTENANCE_RUNNING = True

    def _run_maintenance() -> None:
        global _ALIAS_MAINTENANCE_RUNNING
        try:
            all_items = core._iter_all_items()
            core.library_alias_store.prune_stale_alias_entries(all_items)
        except Exception:
            pass
        finally:
            with _ALIAS_MAINTENANCE_LOCK:
                _ALIAS_MAINTENANCE_RUNNING = False

    threading.Thread(target=_run_maintenance, name="library-alias-maintenance-trigger", daemon=True).start()
    return {"queued": True}


def _async_refresh_worker_loop() -> None:
    from . import library_admin_service

    while True:
        item_id = str(_ASYNC_REFRESH_QUEUE.get() or "").strip()
        if not item_id:
            _ASYNC_REFRESH_QUEUE.task_done()
            continue
        try:
            library_admin_service._process_async_refresh_item(item_id)
        finally:
            with _ASYNC_REFRESH_QUEUE_LOCK:
                _ASYNC_REFRESH_PENDING_IDS.discard(item_id)
            _ASYNC_REFRESH_QUEUE.task_done()


def _ensure_async_refresh_worker() -> None:
    global _ASYNC_REFRESH_WORKER
    with _ASYNC_REFRESH_QUEUE_LOCK:
        if _ASYNC_REFRESH_WORKER is not None and _ASYNC_REFRESH_WORKER.is_alive():
            return
        worker = threading.Thread(target=_async_refresh_worker_loop, name="library-async-refresh-worker", daemon=True)
        worker.start()
        _ASYNC_REFRESH_WORKER = worker
