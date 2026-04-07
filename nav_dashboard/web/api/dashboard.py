from __future__ import annotations

import csv
import io
import json
from datetime import datetime
from typing import Any
from urllib import parse as urlparse

from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import Response
from pydantic import BaseModel, Field

from core_service.feedback import append_feedback, clear_feedback, list_feedback
from core_service.lan_auth import validate_admin_reauth_token
from core_service.observability import get_trace_record, render_trace_export
from core_service.tickets import build_ticket_facets, create_ticket, delete_ticket, get_ticket, list_tickets, update_ticket
from nav_dashboard.web.api.response_models import DashboardTraceResponse, build_dashboard_trace_response
from nav_dashboard.web.clients.internal_services import library_tracker_internal_base_url
from nav_dashboard.web.services.dashboard import dashboard_api_owner, dashboard_jobs, dashboard_runtime_data_service, dashboard_ticket_service, dashboard_usage_service
from nav_dashboard.web.services.operations import data_backup_service
from nav_dashboard.web.services.shared import quota_service

router = APIRouter()


def _require_dashboard_admin_reauth(request: Request) -> None:
    token = str(request.headers.get("x-admin-reauth") or request.query_params.get("reauth_token") or "").strip()
    if not validate_admin_reauth_token(request, token):
        raise HTTPException(status_code=401, detail="admin_reauth_required")


class UsageAdjustPayload(BaseModel):
    month_web_search_calls: int
    month_deepseek_calls: int


class UsageEventPayload(BaseModel):
    provider: str
    feature: str = ""
    page: str = ""
    source: str = ""
    message: str = ""
    trace_id: str = ""
    session_id: str = ""
    count: int = 1


class NotificationDismissPayload(BaseModel):
    key: str


class RuntimeDataCleanupPayload(BaseModel):
    keys: list[str]


class DataBackupCreatePayload(BaseModel):
    apps: list[str] = Field(default_factory=list)


class DataBackupRestorePayload(BaseModel):
    payload: dict[str, Any] = Field(default_factory=dict)
    apps: list[str] = Field(default_factory=list)
    replace_existing: bool = False


class FeedbackPayload(BaseModel):
    source: str = "unknown"
    question: str = ""
    answer: str = ""
    trace_id: str = ""
    session_id: str = ""
    model: str = ""
    search_mode: str = ""
    query_type: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)


class TicketCreatePayload(BaseModel):
    ticket_id: str = ""
    title: str = ""
    status: str = "open"
    priority: str = "medium"
    domain: str = ""
    category: str = ""
    summary: str = ""
    related_traces: list[str] = Field(default_factory=list)
    repro_query: str = ""
    expected_behavior: str = ""
    actual_behavior: str = ""
    root_cause: str = ""
    fix_notes: str = ""
    additional_notes: str = ""
    created_by: str = "ai"
    updated_by: str = ""


class TicketUpdatePayload(BaseModel):
    title: str | None = None
    status: str | None = None
    priority: str | None = None
    domain: str | None = None
    category: str | None = None
    summary: str | None = None
    related_traces: list[str] | None = None
    repro_query: str | None = None
    expected_behavior: str | None = None
    actual_behavior: str | None = None
    root_cause: str | None = None
    fix_notes: str | None = None
    additional_notes: str | None = None
    updated_by: str | None = "human"


class TicketAIDraftPayload(BaseModel):
    trace_id: str = ""
    title: str = ""
    priority: str = "medium"
    domain: str = ""
    category: str = ""
    summary: str = ""
    related_traces: list[str] = Field(default_factory=list)
    repro_query: str = ""
    expected_behavior: str = ""
    actual_behavior: str = ""
    root_cause: str = ""
    fix_notes: str = ""
    additional_notes: str = ""
    created_by: str = "ai"
    updated_by: str = "ai"


class TicketPastePayload(BaseModel):
    text: str = ""


class LibraryAliasProposalReviewPayload(BaseModel):
    proposal_id: str
    action: str
    canonical_name: str = ""
    aliases: list[str] = Field(default_factory=list)


class UsageRecordPayload(BaseModel):
    web_search_delta: int = 0
    deepseek_delta: int = 0
    count_daily: bool = True
    events: list[UsageEventPayload] = Field(default_factory=list)


class ExternalDashboardJobPayload(BaseModel):
    job_id: str = ""
    job_type: str = "job"
    label: str = ""
    status: str = "queued"
    message: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)
    current: int | None = None
    total: int | None = None
    log: str = ""
    result: Any = None
    error: str = ""


@router.get("/api/dashboard/overview")
def get_dashboard_overview(force: bool = False) -> dict[str, Any]:
    return dashboard_api_owner.get_dashboard_overview(force=force)


@router.get("/api/dashboard/overview/core")
def get_dashboard_overview_core() -> dict[str, Any]:
    return dashboard_api_owner.get_dashboard_overview_core()


@router.post("/api/dashboard/notifications/dismiss")
def dismiss_dashboard_notification(payload: NotificationDismissPayload) -> dict[str, Any]:
    return dashboard_api_owner.dismiss_dashboard_notification(payload.key)


@router.get("/api/dashboard/missing-queries")
def get_dashboard_missing_queries(days: int = 30, limit: int = 200, source: str = "all") -> dict[str, Any]:
    rows = dashboard_api_owner.load_missing_queries(days=days, limit=limit, source=source)
    return {"ok": True, "days": max(1, int(days)), "source": str(source or "all"), "count": len(rows), "items": rows}


@router.get("/api/dashboard/missing-queries/export")
def export_dashboard_missing_queries(days: int = 30, limit: int = 5000, source: str = "all") -> Response:
    rows = dashboard_api_owner.load_missing_queries(days=days, limit=limit, source=source)
    header = "时间,来源,Top1分数,阈值,Trace ID,原因,Query"
    lines = [header]
    for row in rows:
        query = str(row.get("query", "")).replace('"', '""')
        trace_id = str(row.get("trace_id", "")).replace('"', '""')
        reason = str(row.get("reason", "")).replace('"', '""')
        top1 = "" if row.get("top1_score") is None else str(row.get("top1_score"))
        threshold = "" if row.get("threshold") is None else str(row.get("threshold"))
        lines.append(f'{str(row.get("ts", ""))},{str(row.get("source_label", ""))},{top1},{threshold},"{trace_id}","{reason}","{query}"')
    csv_body = "\ufeff" + "\r\n".join(lines)
    return Response(content=csv_body, media_type="text/csv; charset=utf-8")


@router.delete("/api/dashboard/missing-queries")
def clear_dashboard_missing_queries(source: str = "all") -> dict[str, Any]:
    return dashboard_api_owner.clear_missing_queries(source=source)


@router.get("/api/dashboard/feedback")
def get_dashboard_feedback(limit: int = 200, source: str = "all") -> dict[str, Any]:
    items = list_feedback(limit=limit, source=source)
    return {"ok": True, "count": len(items), "source": str(source or "all"), "items": items}


@router.get("/api/dashboard/feedback/export")
def export_dashboard_feedback(limit: int = 5000, source: str = "all") -> Response:
    items = list_feedback(limit=limit, source=source)
    return Response(content=json.dumps({"items": items}, ensure_ascii=False, indent=2), media_type="application/json; charset=utf-8")


@router.post("/api/dashboard/feedback")
def post_dashboard_feedback(payload: FeedbackPayload) -> dict[str, Any]:
    try:
        item = append_feedback(payload.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    dashboard_api_owner.invalidate_overview_cache()
    return {"ok": True, "item": item}


@router.delete("/api/dashboard/feedback")
def clear_dashboard_feedback(source: str = "all") -> dict[str, Any]:
    removed = clear_feedback(source=source)
    dashboard_api_owner.invalidate_overview_cache()
    return {"ok": True, "removed": removed, "source": str(source or "all")}


@router.post("/api/dashboard/tickets/ai-draft")
def build_dashboard_ticket_ai_draft(payload: TicketAIDraftPayload) -> dict[str, Any]:
    draft = dashboard_ticket_service.build_ticket_ai_draft(payload)
    return {"ok": True, "ticket": draft}


@router.post("/api/dashboard/tickets/parse")
def parse_dashboard_ticket(payload: TicketPastePayload) -> dict[str, Any]:
    try:
        ticket = dashboard_ticket_service.parse_ticket_paste_payload(payload.text)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "ticket": ticket}


@router.get("/api/dashboard/tickets")
def get_dashboard_tickets(
    status: str = "non_closed",
    priority: str = "all",
    domain: str = "all",
    category: str = "all",
    search: str = "",
    created_from: str = "",
    created_to: str = "",
    limit: int = 200,
    sort: str = "updated_desc",
) -> dict[str, Any]:
    used_default_date_range = False
    if not str(created_from or "").strip() and not str(created_to or "").strip():
        default_from, default_to = dashboard_ticket_service.default_ticket_date_range()
        created_from = default_from.isoformat()
        created_to = default_to.isoformat()
        used_default_date_range = True
    requested_status = str(status or "non_closed")
    if (
        used_default_date_range
        and requested_status == "non_closed"
        and str(status or "non_closed") == "non_closed"
        and str(priority or "all") == "all"
        and str(domain or "all") == "all"
        and str(category or "all") == "all"
        and not str(search or "").strip()
    ):
        status = dashboard_ticket_service.resolve_ticket_default_status(
            created_from=str(created_from or ""),
            created_to=str(created_to or ""),
            ticket_loader=list_tickets,
        )
    items = list_tickets(
        status=status,
        priority=priority,
        domain=domain,
        category=category,
        search=search,
        created_from=created_from,
        created_to=created_to,
        limit=limit,
        sort=sort,
    )
    all_items = list_tickets(limit=5000, sort=sort)
    return {
        "ok": True,
        "count": len(items),
        "items": items,
        "filters": build_ticket_facets(all_items),
        "applied_filters": {
            "status": str(status or "non_closed"),
            "priority": str(priority or "all"),
            "domain": str(domain or "all"),
            "category": str(category or "all"),
            "search": str(search or ""),
            "created_from": str(created_from or ""),
            "created_to": str(created_to or ""),
            "sort": str(sort or "updated_desc"),
        },
    }


@router.post("/api/dashboard/tickets")
def post_dashboard_ticket(payload: TicketCreatePayload) -> dict[str, Any]:
    try:
        item = create_ticket(payload.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    dashboard_api_owner.invalidate_overview_cache()
    return {"ok": True, "ticket": item}


@router.get("/api/dashboard/tickets/{ticket_id}")
def get_dashboard_ticket(ticket_id: str) -> dict[str, Any]:
    item = get_ticket(ticket_id)
    if item is None:
        raise HTTPException(status_code=404, detail=f"未找到 ticket_id={ticket_id} 对应的 ticket")
    return {"ok": True, "ticket": item}


@router.patch("/api/dashboard/tickets/{ticket_id}")
def patch_dashboard_ticket(ticket_id: str, payload: TicketUpdatePayload) -> dict[str, Any]:
    try:
        item = update_ticket(ticket_id, payload.model_dump(exclude_none=True))
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if "不存在" in message else 400
        raise HTTPException(status_code=status_code, detail=message) from exc
    dashboard_api_owner.invalidate_overview_cache()
    return {"ok": True, "ticket": item}


@router.delete("/api/dashboard/tickets/{ticket_id}")
def delete_dashboard_ticket(ticket_id: str, deleted_by: str = "human") -> dict[str, Any]:
    try:
        item = delete_ticket(ticket_id, deleted_by=deleted_by)
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if "不存在" in message else 400
        raise HTTPException(status_code=status_code, detail=message) from exc
    dashboard_api_owner.invalidate_overview_cache()
    return {"ok": True, "ticket": item}


@router.get("/api/dashboard/runtime-data")
def get_dashboard_runtime_data() -> dict[str, Any]:
    return {"ok": True, **dashboard_runtime_data_service.runtime_data_summary(include_items=True)}


@router.post("/api/dashboard/runtime-data/cleanup")
def cleanup_dashboard_runtime_data(payload: RuntimeDataCleanupPayload) -> dict[str, Any]:
    requested = [str(key).strip() for key in payload.keys if str(key).strip()]
    if not requested:
        raise HTTPException(status_code=400, detail="请至少选择一个可清理项")
    result = dashboard_runtime_data_service.cleanup_runtime_data_keys(requested)
    dashboard_api_owner.invalidate_overview_cache()
    return result


@router.get("/api/dashboard/data-backups")
def get_dashboard_data_backups(request: Request) -> dict[str, Any]:
    _require_dashboard_admin_reauth(request)
    return {
        "ok": True,
        "contract": data_backup_service.data_storage_contract(),
        "summary": data_backup_service.backup_summary(),
    }


@router.get("/api/dashboard/data-backups/export")
def export_dashboard_data_backups(request: Request, apps: str = "") -> Response:
    _require_dashboard_admin_reauth(request)
    selected = [item.strip() for item in str(apps or "").split(",") if item.strip()]
    payload = data_backup_service.export_main_data_contract(selected or None)
    archive_bytes = data_backup_service.build_backup_archive_bytes(payload)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Response(
        content=archive_bytes,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="personal_data_export_{stamp}.zip"'},
    )


@router.post("/api/dashboard/data-backups/create")
def create_dashboard_data_backup(request: Request, payload: DataBackupCreatePayload) -> dict[str, Any]:
    _require_dashboard_admin_reauth(request)
    try:
        result = data_backup_service.create_backup_snapshot(payload.apps or None)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    dashboard_api_owner.invalidate_overview_cache()
    return result


@router.post("/api/dashboard/data-backups/restore")
def restore_dashboard_data_backup(request: Request, payload: DataBackupRestorePayload) -> dict[str, Any]:
    _require_dashboard_admin_reauth(request)
    if not isinstance(payload.payload, dict) or not payload.payload:
        raise HTTPException(status_code=400, detail="缺少可恢复的备份内容")
    try:
        result = data_backup_service.restore_main_data_contract(
            payload.payload,
            apps=payload.apps or None,
            replace_existing=payload.replace_existing,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    dashboard_api_owner.invalidate_overview_cache()
    return result


@router.post("/api/dashboard/data-backups/restore-file")
async def restore_dashboard_data_backup_file(
    request: Request,
    file: UploadFile = File(...),
    apps: str = Form(""),
    replace_existing: bool = Form(True),
) -> dict[str, Any]:
    _require_dashboard_admin_reauth(request)
    raw_bytes = await file.read()
    selected = [item.strip() for item in str(apps or "").split(",") if item.strip()]
    try:
        payload = data_backup_service.load_backup_payload(file.filename or "backup.json", raw_bytes)
        result = data_backup_service.restore_main_data_contract(
            payload,
            apps=selected or None,
            replace_existing=bool(replace_existing),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    dashboard_api_owner.invalidate_overview_cache()
    return result


@router.get("/api/dashboard/trace", response_model=DashboardTraceResponse, response_model_exclude_unset=True)
def get_dashboard_trace(trace_id: str) -> dict[str, Any]:
    value = str(trace_id or "").strip()
    if not value:
        raise HTTPException(status_code=400, detail="trace_id is required")
    record = get_trace_record(value)
    if not record:
        raise HTTPException(status_code=404, detail=f"未找到 trace_id={value} 对应的追踪记录")
    return build_dashboard_trace_response(ok=True, trace=record, export_text=render_trace_export(record))


@router.get("/api/dashboard/trace/export")
def export_dashboard_trace(trace_id: str) -> Response:
    value = str(trace_id or "").strip()
    if not value:
        raise HTTPException(status_code=400, detail="trace_id is required")
    record = get_trace_record(value)
    if not record:
        raise HTTPException(status_code=404, detail=f"未找到 trace_id={value} 对应的追踪记录")
    return Response(content=render_trace_export(record), media_type="text/plain; charset=utf-8")


@router.get("/api/dashboard/library-alias-proposals")
def get_dashboard_library_alias_proposals(page: int = 1, page_size: int = 10) -> dict[str, Any]:
    base = library_tracker_internal_base_url().rstrip("/")
    target = f"{base}/api/library/alias-proposals?page={max(1, int(page or 1))}&page_size={max(1, min(int(page_size or 10), 50))}"
    return dashboard_api_owner.http_json_request("GET", target, timeout=20)


@router.post("/api/dashboard/library-alias-proposals/review")
def review_dashboard_library_alias_proposal(payload: LibraryAliasProposalReviewPayload) -> dict[str, Any]:
    base = library_tracker_internal_base_url().rstrip("/")
    result = dashboard_api_owner.http_json_request(
        "POST",
        f"{base}/api/library/alias-proposals/review",
        payload={
            "proposal_id": payload.proposal_id,
            "action": payload.action,
            "canonical_name": payload.canonical_name,
            "aliases": list(payload.aliases or []),
        },
        timeout=30,
    )
    dashboard_api_owner.clear_dashboard_overview_cache()
    return result


@router.get("/api/startup/status")
def get_startup_status_lean(fresh: bool = False) -> dict[str, Any]:
    status = dashboard_api_owner.load_startup_status() if fresh else dashboard_api_owner.load_startup_status_cached()
    return {"ok": True, "status": status.get("status", "unknown"), "last_checked_at": status.get("last_checked_at", "")}


@router.get("/api/dashboard/ontology-status")
def get_ontology_status() -> dict[str, Any]:
    from nav_dashboard.web.services.ontologies.ontology_loader import get_load_statuses as _get_ontology_load_statuses  # noqa: PLC0415

    propose_state: dict[str, Any] = {}
    propose_state_file = dashboard_api_owner.owner_app_dir() / "services" / "ontologies" / "proposed" / "_propose_state.json"
    if propose_state_file.exists():
        try:
            propose_state = json.loads(propose_state_file.read_text(encoding="utf-8"))
        except Exception:
            propose_state = {"error": "could not parse propose state"}
    return {"ok": True, "load_statuses": _get_ontology_load_statuses(), "propose_state": propose_state}


@router.patch("/api/dashboard/usage")
def adjust_dashboard_usage(payload: UsageAdjustPayload) -> dict[str, Any]:
    month_key = datetime.now().strftime("%Y-%m")
    try:
        updated = quota_service.set_monthly_quota_usage(
            web_search=payload.month_web_search_calls,
            deepseek=payload.month_deepseek_calls,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    dashboard_api_owner.clear_dashboard_overview_cache()
    return {"ok": True, "month": month_key, **updated}


@router.post("/api/dashboard/usage/record")
def record_dashboard_usage(payload: UsageRecordPayload) -> dict[str, Any]:
    web_inc = max(0, int(payload.web_search_delta or 0))
    deepseek_inc = max(0, int(payload.deepseek_delta or 0))
    count_daily = bool(payload.count_daily)
    events = [item.model_dump() for item in payload.events]
    if web_inc <= 0 and deepseek_inc <= 0 and not events:
        return {"ok": True, "skipped": True}
    try:
        if count_daily and (web_inc > 0 or deepseek_inc > 0):
            quota_state = quota_service.load_quota_state()
            quota_service.increment_quota_state(quota_state, web_search_delta=web_inc, deepseek_delta=deepseek_inc)
        elif web_inc > 0 or deepseek_inc > 0:
            quota_service.record_quota_usage(web_search_delta=web_inc, deepseek_delta=deepseek_inc)
        if events:
            dashboard_usage_service.record_usage_events(events)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    dashboard_api_owner.clear_dashboard_overview_cache()
    return {
        "ok": True,
        "web_search_delta": web_inc,
        "deepseek_delta": deepseek_inc,
        "count_daily": count_daily,
        "recorded_events": len(events),
    }


@router.get("/api/dashboard/usage/traces")
def get_dashboard_usage_traces(days: int = 7, limit: int = 200, provider: str = "all") -> dict[str, Any]:
    items = dashboard_usage_service.load_usage_traces(days=days, limit=limit, provider=provider)
    return {
        "ok": True,
        "days": max(1, int(days or 7)),
        "provider": str(provider or "all").strip() or "all",
        "count": len(items),
        "items": items,
    }


@router.get("/api/dashboard/usage/traces/export")
def export_dashboard_usage_traces(days: int = 7, limit: int = 5000, provider: str = "all") -> Response:
    items = dashboard_usage_service.load_usage_traces(days=days, limit=limit, provider=provider)
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(["timestamp", "provider", "feature", "page", "source", "message_preview", "count", "trace_id", "session_id"])
    for row in items:
        writer.writerow(
            [
                str(row.get("timestamp") or ""),
                str(row.get("provider_label") or dashboard_usage_service.usage_provider_label(str(row.get("provider") or ""))),
                str(row.get("feature") or ""),
                str(row.get("page") or ""),
                str(row.get("source") or ""),
                str(row.get("message_preview") or ""),
                str(row.get("count") or 1),
                str(row.get("trace_id") or ""),
                str(row.get("session_id") or ""),
            ]
        )
    return Response(
        content=buffer.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename=api_usage_traces_{provider or 'all'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"},
    )


@router.delete("/api/dashboard/usage/traces")
def clear_dashboard_usage_traces(provider: str = "all") -> dict[str, Any]:
    dashboard_api_owner.clear_dashboard_overview_cache()
    removed = dashboard_usage_service.clear_usage_trace_rows(provider=provider)
    return {"ok": True, "provider": str(provider or "all").strip() or "all", "removed": removed}


@router.post("/api/dashboard/trigger-rag-sync")
def trigger_rag_sync() -> dict[str, Any]:
    base = dashboard_api_owner.ai_summary_internal_base_url().rstrip("/")
    try:
        result = dashboard_api_owner.http_json_request("POST", f"{base}/api/workflow/run", payload={"action": "sync_embeddings"}, timeout=10)
        dashboard_api_owner.invalidate_overview_cache()
        return {"ok": True, "result": result}
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/api/dashboard/trigger-library-graph-rebuild")
def trigger_library_graph_rebuild(full: bool = False) -> dict[str, Any]:
    return dashboard_api_owner.run_library_graph_rebuild(full=full)


@router.get("/api/dashboard/jobs/{job_id}")
def get_dashboard_job(job_id: str) -> dict[str, Any]:
    job = dashboard_jobs.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"ok": True, "job": job}


@router.get("/api/dashboard/jobs")
def list_dashboard_jobs(job_type: str = "", only_active: bool = False) -> dict[str, Any]:
    return {"ok": True, "jobs": dashboard_jobs.list_jobs(job_type=str(job_type or "").strip(), only_active=bool(only_active))}


@router.post("/api/dashboard/jobs/external")
def upsert_external_dashboard_job(payload: ExternalDashboardJobPayload) -> dict[str, Any]:
    job = dashboard_jobs.upsert_external_job(
        job_id=payload.job_id,
        job_type=payload.job_type,
        label=payload.label,
        status=payload.status,
        message=payload.message,
        metadata=payload.metadata,
        current=payload.current,
        total=payload.total,
        log=payload.log,
        result=payload.result,
        error=payload.error,
    )
    dashboard_api_owner.invalidate_overview_cache()
    return {"ok": True, "job": job}


@router.post("/api/dashboard/jobs/{job_id}/cancel")
def cancel_dashboard_job(job_id: str) -> dict[str, Any]:
    job = dashboard_jobs.request_cancel(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"ok": True, "job": job}


@router.delete("/api/dashboard/jobs/clear-history")
def clear_dashboard_job_history() -> dict[str, Any]:
    result = dashboard_jobs.clear_history()
    dashboard_api_owner.invalidate_overview_cache()
    return {
        "ok": True,
        "removed_count": int(result.get("removed_count") or 0),
        "active_count": int(result.get("active_count") or 0),
        "jobs": list(result.get("removed_jobs") or []),
    }


@router.delete("/api/dashboard/jobs/{job_id}")
def delete_dashboard_job(job_id: str) -> dict[str, Any]:
    try:
        job = dashboard_jobs.delete_job(job_id)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    dashboard_api_owner.invalidate_overview_cache()
    return {"ok": True, "job": job}


@router.post("/api/dashboard/jobs/rag-sync")
def create_rag_sync_job() -> dict[str, Any]:
    def _target(report_progress, is_cancelled):
        if is_cancelled():
            report_progress(message="已取消", log="RAG 同步任务已取消")
            return {"cancelled": True}
        report_progress(message="正在触发 RAG 同步", log="开始触发 RAG 同步")
        result = trigger_rag_sync()
        report_progress(message="RAG 同步已提交", log="RAG 同步已提交", result=result)
        return result

    job = dashboard_jobs.create_job(job_type="rag_sync", label="RAG 增量同步", target=_target)
    return {"ok": True, "job": job}


@router.post("/api/dashboard/jobs/library-graph-rebuild")
def create_library_graph_rebuild_job() -> dict[str, Any]:
    def _target(report_progress, is_cancelled):
        if is_cancelled():
            report_progress(message="已取消", log="Library Graph 重建任务已取消")
            return {"cancelled": True}
        report_progress(message="正在补足 Library Graph 缺失项", log="开始补足 Library Graph 缺失项")
        result = dashboard_api_owner.run_library_graph_rebuild(full=False, report_progress=report_progress, is_cancelled=is_cancelled)
        report_progress(message="Library Graph 补缺已完成", log="Library Graph 补缺已完成", result=result)
        return result

    job = dashboard_jobs.create_job(job_type="library_graph_rebuild", label="Library Graph 补缺", metadata={"mode": "missing_only"}, target=_target)
    return {"ok": True, "job": job}


@router.post("/api/dashboard/jobs/library-graph-rebuild-full")
def create_library_graph_full_rebuild_job() -> dict[str, Any]:
    def _target(report_progress, is_cancelled):
        if is_cancelled():
            report_progress(message="已取消", log="Library Graph 全量重建任务已取消")
            return {"cancelled": True}
        report_progress(message="正在触发 Library Graph 全量重建", log="开始全量重建 Library Graph")
        result = dashboard_api_owner.run_library_graph_rebuild(full=True, report_progress=report_progress, is_cancelled=is_cancelled)
        report_progress(message="Library Graph 全量重建已完成", log="Library Graph 全量重建已完成", result=result)
        return result

    job = dashboard_jobs.create_job(job_type="library_graph_rebuild", label="Library Graph 全量重建", metadata={"mode": "full"}, target=_target)
    return {"ok": True, "job": job}


@router.post("/api/dashboard/jobs/runtime-data-cleanup")
def create_runtime_cleanup_job(payload: RuntimeDataCleanupPayload) -> dict[str, Any]:
    requested = [str(key).strip() for key in payload.keys if str(key).strip()]
    if not requested:
        raise HTTPException(status_code=400, detail="请至少选择一个可清理项")

    def _target(report_progress, is_cancelled):
        if is_cancelled():
            report_progress(message="已取消", log="运行时数据清理已取消")
            return {"cancelled": True}
        report_progress(message=f"正在清理 {len(requested)} 项运行时数据", log=f"开始清理 {len(requested)} 项运行时数据")
        result = dashboard_runtime_data_service.cleanup_runtime_data_keys(requested)
        dashboard_api_owner.invalidate_overview_cache()
        report_progress(message="运行时数据清理完成", log="运行时数据清理完成", result=result)
        return result

    job = dashboard_jobs.create_job(job_type="runtime_cleanup", label="运行时数据清理", metadata={"keys": requested}, target=_target)
    return {"ok": True, "job": job}