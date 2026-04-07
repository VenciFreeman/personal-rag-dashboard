from __future__ import annotations

import json
import threading
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable
from uuid import uuid4

from ..runtime_paths import DASHBOARD_JOBS_FILE, STATE_DIR

JobTarget = Callable[[Callable[..., None], Callable[[], bool]], Any]


@dataclass
class DashboardJob:
    id: str
    job_type: str
    label: str
    metadata: dict[str, Any] = field(default_factory=dict)
    status: str = "queued"
    message: str = ""
    created_at: str = field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))
    started_at: str = ""
    finished_at: str = ""
    current: int = 0
    total: int = 0
    logs: list[str] = field(default_factory=list)
    result: Any = None
    error: str = ""
    cancel_requested: bool = False


_LOCK = threading.Lock()
_JOBS: dict[str, DashboardJob] = {}
_MAX_LOGS = 800
_MAX_JOBS = 200


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _snapshot(job: DashboardJob) -> dict[str, Any]:
    return {
        "id": job.id,
        "type": job.job_type,
        "job_type": job.job_type,
        "label": job.label,
        "metadata": dict(job.metadata),
        "status": job.status,
        "message": job.message,
        "created_at": job.created_at,
        "updated_at": job.updated_at,
        "started_at": job.started_at,
        "finished_at": job.finished_at,
        "current": job.current,
        "total": job.total,
        "logs": list(job.logs),
        "result": job.result,
        "error": job.error,
        "cancel_requested": job.cancel_requested,
    }


def _job_from_payload(payload: dict[str, Any]) -> DashboardJob | None:
    try:
        return DashboardJob(
            id=str(payload.get("id") or uuid4().hex[:10]),
            job_type=str(payload.get("type") or payload.get("job_type") or "job"),
            label=str(payload.get("label") or payload.get("job_type") or "job"),
            metadata=dict(payload.get("metadata") or {}),
            status=str(payload.get("status") or "queued"),
            message=str(payload.get("message") or ""),
            created_at=str(payload.get("created_at") or _now_iso()),
            updated_at=str(payload.get("updated_at") or payload.get("created_at") or _now_iso()),
            started_at=str(payload.get("started_at") or ""),
            finished_at=str(payload.get("finished_at") or payload.get("finished_at") or payload.get("rinished_at") or ""),
            current=max(0, int(payload.get("current") or 0)),
            total=max(0, int(payload.get("total") or 0)),
            logs=[str(item) for item in list(payload.get("logs") or [])[-_MAX_LOGS:]],
            result=payload.get("result"),
            error=str(payload.get("error") or ""),
            cancel_requested=bool(payload.get("cancel_requested")),
        )
    except Exception:
        return None


def _append_log(job: DashboardJob, line: str) -> None:
    text = str(line or "").strip()
    if not text:
        return
    job.logs.append(f"[{_now_iso()}] {text}")
    if len(job.logs) > _MAX_LOGS:
        job.logs = job.logs[-_MAX_LOGS:]


def _prune_jobs() -> None:
    if len(_JOBS) <= _MAX_JOBS:
        return
    ordered = sorted(_JOBS.values(), key=lambda item: (item.updated_at, item.created_at))
    removable = [job.id for job in ordered if job.status in {"completed", "failed", "cancelled"}]
    while len(_JOBS) > _MAX_JOBS and removable:
        _JOBS.pop(removable.pop(0), None)


def _save_jobs_locked() -> None:
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        ordered = sorted(_JOBS.values(), key=lambda item: (item.updated_at, item.created_at), reverse=True)
        payload = {"jobs": [_snapshot(job) for job in ordered[:_MAX_JOBS]], "updated_at": _now_iso()}
        DASHBOARD_JOBS_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def _load_jobs() -> None:
    if not DASHBOARD_JOBS_FILE.exists():
        return
    try:
        raw = json.loads(DASHBOARD_JOBS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return
    items = raw.get("jobs") if isinstance(raw, dict) else []
    if not isinstance(items, list):
        return
    with _LOCK:
        _JOBS.clear()
        for item in items[-_MAX_JOBS:]:
            if not isinstance(item, dict):
                continue
            job = _job_from_payload(item)
            if job is None:
                continue
            if job.status in {"queued", "running"}:
                job.status = "failed"
                job.message = job.message or "服务重启，任务状态已失效"
                job.error = job.error or "dashboard service restarted before task completion"
                if not job.finished_at:
                    job.finished_at = _now_iso()
                job.updated_at = _now_iso()
            _JOBS[job.id] = job
        _prune_jobs()


def create_job(*, job_type: str, label: str, target: JobTarget, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    job = DashboardJob(
        id=uuid4().hex[:10],
        job_type=str(job_type or "job"),
        label=str(label or job_type or "job"),
        metadata=dict(metadata or {}),
    )
    with _LOCK:
        _JOBS[job.id] = job
        _prune_jobs()
        _save_jobs_locked()

    def _run() -> None:
        with _LOCK:
            current_job = _JOBS.get(job.id)
            if current_job is None:
                return
            current_job.status = "running"
            current_job.started_at = _now_iso()
            current_job.updated_at = current_job.started_at
            _save_jobs_locked()

        def report_progress(
            *,
            message: str | None = None,
            current: int | None = None,
            total: int | None = None,
            log: str | None = None,
            result: Any = None,
            metadata: dict[str, Any] | None = None,
        ) -> None:
            with _LOCK:
                current_job = _JOBS.get(job.id)
                if current_job is None:
                    return
                if message is not None:
                    current_job.message = str(message)
                if current is not None:
                    current_job.current = max(0, int(current))
                if total is not None:
                    current_job.total = max(0, int(total))
                if metadata:
                    current_job.metadata.update(metadata)
                if result is not None:
                    current_job.result = result
                if log:
                    _append_log(current_job, log)
                current_job.updated_at = _now_iso()
                _save_jobs_locked()

        def is_cancelled() -> bool:
            with _LOCK:
                current_job = _JOBS.get(job.id)
                return bool(current_job.cancel_requested) if current_job else True

        try:
            result = target(report_progress, is_cancelled)
            with _LOCK:
                current_job = _JOBS.get(job.id)
                if current_job is None:
                    return
                current_job.result = result
                current_job.finished_at = _now_iso()
                current_job.updated_at = current_job.finished_at
                if current_job.cancel_requested:
                    current_job.status = "cancelled"
                    current_job.message = current_job.message or "任务已取消"
                else:
                    current_job.status = "completed"
                    current_job.message = current_job.message or "任务完成"
                _save_jobs_locked()
        except Exception as exc:  # noqa: BLE001
            with _LOCK:
                current_job = _JOBS.get(job.id)
                if current_job is None:
                    return
                current_job.status = "failed"
                current_job.error = str(exc)
                current_job.finished_at = _now_iso()
                current_job.updated_at = current_job.finished_at
                _append_log(current_job, f"ERROR: {exc}")
                tb = traceback.format_exc(limit=6)
                if tb:
                    _append_log(current_job, tb)
                _save_jobs_locked()

    thread = threading.Thread(target=_run, daemon=True, name=f"dashboard-job-{job.job_type}-{job.id}")
    thread.start()
    return _snapshot(job)


def upsert_external_job(
    *,
    job_id: str = "",
    job_type: str,
    label: str,
    status: str,
    message: str = "",
    metadata: dict[str, Any] | None = None,
    current: int | None = None,
    total: int | None = None,
    log: str = "",
    result: Any = None,
    error: str = "",
) -> dict[str, Any]:
    normalized_status = str(status or "queued").strip().lower() or "queued"
    if normalized_status not in {"queued", "running", "completed", "failed", "cancelled"}:
        normalized_status = "queued"
    normalized_job_id = str(job_id or "").strip()
    now = _now_iso()
    with _LOCK:
        job = _JOBS.get(normalized_job_id) if normalized_job_id else None
        if job is None:
            job = DashboardJob(
                id=normalized_job_id or uuid4().hex[:10],
                job_type=str(job_type or "job").strip() or "job",
                label=str(label or job_type or "job").strip() or "job",
                metadata={"external": True, **dict(metadata or {})},
                status=normalized_status,
                message=str(message or "").strip(),
            )
            if normalized_status in {"running", "completed", "failed", "cancelled"}:
                job.started_at = now
            if normalized_status in {"completed", "failed", "cancelled"}:
                job.finished_at = now
            if current is not None:
                job.current = max(0, int(current or 0))
            if total is not None:
                job.total = max(0, int(total or 0))
            if result is not None:
                job.result = result
            if error:
                job.error = str(error)
            if log:
                _append_log(job, log)
            job.updated_at = now
            _JOBS[job.id] = job
            _prune_jobs()
            _save_jobs_locked()
            return _snapshot(job)

        job.job_type = str(job_type or job.job_type or "job").strip() or job.job_type
        job.label = str(label or job.label or job.job_type or "job").strip() or job.label
        job.status = normalized_status
        if message:
            job.message = str(message)
        if metadata:
            job.metadata.update(dict(metadata))
        job.metadata["external"] = True
        if current is not None:
            job.current = max(0, int(current or 0))
        if total is not None:
            job.total = max(0, int(total or 0))
        if result is not None:
            job.result = result
        if error:
            job.error = str(error)
        if log:
            _append_log(job, log)
        if normalized_status in {"running", "completed", "failed", "cancelled"} and not job.started_at:
            job.started_at = now
        if normalized_status in {"completed", "failed", "cancelled"}:
            job.finished_at = now
        elif normalized_status in {"queued", "running"}:
            job.finished_at = ""
        job.updated_at = now
        _prune_jobs()
        _save_jobs_locked()
        return _snapshot(job)


def get_job(job_id: str) -> dict[str, Any] | None:
    with _LOCK:
        job = _JOBS.get(str(job_id or "").strip())
        return _snapshot(job) if job else None


def list_jobs(*, job_type: str = "", only_active: bool = False) -> list[dict[str, Any]]:
    with _LOCK:
        jobs = list(_JOBS.values())
        if job_type:
            jobs = [job for job in jobs if job.job_type == job_type]
        if only_active:
            jobs = [job for job in jobs if job.status in {"queued", "running"}]
        jobs.sort(key=lambda item: (item.updated_at, item.created_at), reverse=True)
        return [_snapshot(job) for job in jobs]


def request_cancel(job_id: str) -> dict[str, Any] | None:
    with _LOCK:
        job = _JOBS.get(str(job_id or "").strip())
        if job is None:
            return None
        job.cancel_requested = True
        if job.status == "queued":
            job.status = "cancelled"
            job.finished_at = _now_iso()
        job.updated_at = _now_iso()
        if not job.message:
            job.message = "已请求取消"
        _append_log(job, "收到取消请求")
        _save_jobs_locked()
        return _snapshot(job)


def delete_job(job_id: str) -> dict[str, Any] | None:
    normalized_id = str(job_id or "").strip()
    with _LOCK:
        job = _JOBS.get(normalized_id)
        if job is None:
            return None
        if job.status in {"queued", "running"}:
            raise ValueError("Active jobs cannot be deleted")
        snapshot = _snapshot(job)
        _JOBS.pop(normalized_id, None)
        _save_jobs_locked()
        return snapshot


def clear_history() -> dict[str, Any]:
    with _LOCK:
        removed_jobs = [
            _snapshot(job)
            for job in sorted(_JOBS.values(), key=lambda item: (item.updated_at, item.created_at), reverse=True)
            if job.status not in {"queued", "running"}
        ]
        active_count = sum(1 for job in _JOBS.values() if job.status in {"queued", "running"})
        if removed_jobs:
            for job in removed_jobs:
                _JOBS.pop(str(job.get("id") or "").strip(), None)
            _save_jobs_locked()
        return {
            "removed_jobs": removed_jobs,
            "removed_count": len(removed_jobs),
            "active_count": active_count,
        }


_load_jobs()

