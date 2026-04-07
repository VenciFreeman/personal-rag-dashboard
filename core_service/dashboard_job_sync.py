from __future__ import annotations

import json
import os
from typing import Any
from urllib import request as urlrequest


def _dashboard_external_job_url() -> str:
    base = (os.getenv("NAV_DASHBOARD_EXTERNAL_JOB_URL", "") or "").strip().rstrip("/")
    if not base:
        base = "http://127.0.0.1:8092"
    return f"{base}/api/dashboard/jobs/external"


def publish_nav_dashboard_job_update(
    *,
    job_type: str,
    label: str,
    status: str,
    job_id: str = "",
    message: str = "",
    metadata: dict[str, Any] | None = None,
    current: int | None = None,
    total: int | None = None,
    log: str = "",
    result: Any = None,
    error: str = "",
    timeout: float = 2.0,
) -> str:
    payload: dict[str, Any] = {
        "job_id": str(job_id or "").strip(),
        "job_type": str(job_type or "job").strip() or "job",
        "label": str(label or job_type or "job").strip() or "job",
        "status": str(status or "queued").strip() or "queued",
        "message": str(message or "").strip(),
        "metadata": dict(metadata or {}),
        "log": str(log or "").strip(),
        "error": str(error or "").strip(),
    }
    if current is not None:
        payload["current"] = max(0, int(current or 0))
    if total is not None:
        payload["total"] = max(0, int(total or 0))
    if result is not None:
        payload["result"] = result

    req = urlrequest.Request(
        _dashboard_external_job_url(),
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        opener = urlrequest.build_opener(urlrequest.ProxyHandler({}))
        with opener.open(req, timeout=timeout) as response:
            raw = response.read().decode("utf-8", errors="ignore")
    except Exception:
        return ""
    try:
        body = json.loads(raw)
    except Exception:
        return ""
    if not isinstance(body, dict):
        return ""
    job = body.get("job") if isinstance(body.get("job"), dict) else {}
    return str(job.get("id") or job.get("job_id") or "").strip()