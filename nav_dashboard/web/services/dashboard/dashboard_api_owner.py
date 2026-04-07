from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from fastapi import HTTPException

from . import dashboard_runtime_data_service, dashboard_ticket_service, dashboard_usage_service


@dataclass(frozen=True)
class DashboardApiOwnerDeps:
    monotonic: Any
    overview_cache_lock: Any
    overview_cache_ttl: float
    get_overview_cache: Any
    set_overview_cache: Any
    overview_core_cache_lock: Any
    overview_core_cache_ttl: float
    get_overview_core_cache: Any
    set_overview_core_cache: Any
    build_information_notifications: Any
    build_overview: Any
    build_overview_core: Any
    build_failed_overview: Any
    persist_overview_snapshot: Any
    load_notification_state: Any
    save_notification_state: Any
    clear_dashboard_overview_cache: Any
    invalidate_overview_cache: Any
    load_missing_queries: Any
    no_context_log_path: Any
    is_benchmark_source: Any
    is_benchmark_trace_id: Any
    http_json_request: Any
    load_startup_status: Any
    load_startup_status_cached: Any
    owner_app_dir: Any
    ai_summary_internal_base_url: Any
    run_library_graph_rebuild: Any


_OWNER_DEPS: DashboardApiOwnerDeps | None = None


def configure(owner_deps: DashboardApiOwnerDeps) -> None:
    global _OWNER_DEPS
    _OWNER_DEPS = owner_deps


def _deps() -> DashboardApiOwnerDeps:
    if _OWNER_DEPS is None:
        raise RuntimeError("dashboard API owner is not configured")
    return _OWNER_DEPS


def get_dashboard_overview(*, force: bool = False) -> dict[str, Any]:
    deps = _deps()
    now = deps.monotonic()
    with deps.overview_cache_lock:
        cached_value, cached_at = deps.get_overview_cache()
        if not force and cached_value is not None and (now - cached_at) < deps.overview_cache_ttl:
            cached = dict(cached_value)
            cached["notifications"] = deps.build_information_notifications()
            return cached
    try:
        result = deps.build_overview()
    except Exception as exc:  # noqa: BLE001
        result = deps.build_failed_overview(exc)
    with deps.overview_cache_lock:
        deps.set_overview_cache(result, deps.monotonic())
    deps.persist_overview_snapshot(result)
    return result


def get_dashboard_overview_core() -> dict[str, Any]:
    deps = _deps()
    now = deps.monotonic()
    with deps.overview_core_cache_lock:
        cached_value, cached_at = deps.get_overview_core_cache()
        if cached_value is not None and (now - cached_at) < deps.overview_core_cache_ttl:
            cached = dict(cached_value)
            cached["notifications"] = deps.build_information_notifications()
            return cached
    result = deps.build_overview_core()
    with deps.overview_core_cache_lock:
        deps.set_overview_core_cache(result, deps.monotonic())
    return result


def dismiss_dashboard_notification(key: str) -> dict[str, Any]:
    deps = _deps()
    normalized_key = str(key or "").strip()
    if not normalized_key:
        raise HTTPException(status_code=400, detail="notification key required")
    state = deps.load_notification_state()
    dismissed = state.get("dismissed") if isinstance(state.get("dismissed"), dict) else {}
    dismissed[normalized_key] = {"dismissed_at": datetime.now().isoformat(timespec="seconds")}
    state["dismissed"] = dismissed
    deps.save_notification_state(state)
    deps.clear_dashboard_overview_cache()
    return {"ok": True, "key": normalized_key}


def load_missing_queries(*, days: int = 30, limit: int = 200, source: str = "all") -> list[dict[str, Any]]:
    deps = _deps()
    return deps.load_missing_queries(days=days, limit=limit, source=source)


def clear_missing_queries(*, source: str = "all") -> dict[str, Any]:
    deps = _deps()
    try:
        if deps.no_context_log_path.exists():
            source_filter = str(source or "all").strip().lower()
            if source_filter in {"", "all", "*"}:
                deps.no_context_log_path.write_text("", encoding="utf-8")
            else:
                lines = deps.no_context_log_path.read_text(encoding="utf-8").splitlines()
                kept: list[str] = []
                for line in lines:
                    raw = str(line or "").strip()
                    if not raw:
                        continue
                    try:
                        row = json.loads(raw)
                    except Exception:
                        kept.append(raw)
                        continue
                    if not isinstance(row, dict):
                        kept.append(raw)
                        continue
                    row_source = str(row.get("source", "unknown") or "unknown").strip().lower()
                    trace_id = str(row.get("trace_id", "") or "").strip()
                    if deps.is_benchmark_source(row_source) or deps.is_benchmark_trace_id(trace_id) or row_source != source_filter:
                        kept.append(raw)
                deps.no_context_log_path.write_text(("\n".join(kept) + ("\n" if kept else "")), encoding="utf-8")
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    deps.invalidate_overview_cache()
    return {"ok": True, "cleared": True, "source": str(source or "all")}


def invalidate_overview_cache() -> None:
    _deps().invalidate_overview_cache()


def clear_dashboard_overview_cache() -> None:
    _deps().clear_dashboard_overview_cache()


def build_ticket_ai_draft(payload: Any) -> dict[str, Any]:
    return dashboard_ticket_service.build_ticket_ai_draft(payload)


def parse_ticket_paste_payload(raw_text: str) -> dict[str, Any]:
    return dashboard_ticket_service.parse_ticket_paste_payload(raw_text)


def default_ticket_date_range() -> tuple[Any, Any]:
    return dashboard_ticket_service.default_ticket_date_range()


def runtime_data_summary(*, include_items: bool = False) -> dict[str, Any]:
    return dashboard_runtime_data_service.runtime_data_summary(include_items=include_items)


def cleanup_runtime_data_keys(requested: list[str]) -> dict[str, Any]:
    return dashboard_runtime_data_service.cleanup_runtime_data_keys(requested)


def http_json_request(method: str, url: str, *, payload: dict[str, Any] | None = None, timeout: float = 10.0) -> dict[str, Any]:
    return _deps().http_json_request(method, url, payload=payload, timeout=timeout)


def load_startup_status_cached() -> dict[str, Any]:
    return _deps().load_startup_status_cached()


def load_startup_status() -> dict[str, Any]:
    return _deps().load_startup_status()


def owner_app_dir() -> Any:
    return _deps().owner_app_dir


def load_usage_traces(*, days: int = 7, limit: int = 200, provider: str = "all") -> list[dict[str, Any]]:
    return dashboard_usage_service.load_usage_traces(days=days, limit=limit, provider=provider)


def usage_provider_label(provider: str) -> str:
    return dashboard_usage_service.usage_provider_label(provider)


def ai_summary_internal_base_url() -> str:
    return _deps().ai_summary_internal_base_url()


def run_library_graph_rebuild(
    *,
    full: bool = False,
    report_progress: Any = None,
    is_cancelled: Any = None,
) -> dict[str, Any]:
    return _deps().run_library_graph_rebuild(full=full, report_progress=report_progress, is_cancelled=is_cancelled)