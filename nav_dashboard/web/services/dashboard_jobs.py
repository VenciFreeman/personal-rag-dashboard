from __future__ import annotations

import threading
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable
from uuid import uuid4

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
_MAX_LOGS = 200
_MAX_JOBS = 80


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _snapshot(job: DashboardJob) -> dict[str, Any]:
    return {
        "id": job.id,
        "type": job.job_type,
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


def _append_log(job: DashboardJob, line: str) -> None:
    text = str(line or "").strip()
    if not text:
        return
    stamped = f"[{_now_iso()}] {text}"
    job.logs.append(stamped)
    if len(job.logs) > _MAX_LOGS:
        job.logs = job.logs[-_MAX_LOGS:]


def _prune_jobs() -> None:
    if len(_JOBS) <= _MAX_JOBS:
        return
    ordered = sorted(_JOBS.values(), key=lambda item: (item.updated_at, item.created_at))
    removable = [job.id for job in ordered if job.status in {"completed", "failed", "cancelled"}]
    while len(_JOBS) > _MAX_JOBS and removable:
        _JOBS.pop(removable.pop(0), None)


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

    def _run() -> None:
        with _LOCK:
            current = _JOBS.get(job.id)
            if current is None:
                return
            current.status = "running"
            current.started_at = _now_iso()
            current.updated_at = current.started_at

        def report_progress(*, message: str | None = None, current: int | None = None, total: int | None = None, log: str | None = None, result: Any = None, metadata: dict[str, Any] | None = None) -> None:
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
                if current_job.cancel_requested and current_job.status != "failed":
                    current_job.status = "cancelled"
                    current_job.message = current_job.message or "任务已取消"
                else:
                    current_job.status = "completed"
                    current_job.message = current_job.message or "任务完成"
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

    thread = threading.Thread(target=_run, daemon=True, name=f"dashboard-job-{job.job_type}-{job.id}")
    thread.start()
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
        return _snapshot(job)
