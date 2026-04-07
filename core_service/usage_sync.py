from __future__ import annotations

import json
import os
import threading
from typing import Any
from urllib import request as urlrequest


def _dashboard_usage_record_url() -> str:
    base = (os.getenv("NAV_DASHBOARD_QUOTA_RECORD_URL", "") or "").strip().rstrip("/")
    if not base:
        base = "http://127.0.0.1:8092"
    return f"{base}/api/dashboard/usage/record"


def _sanitize_events(events: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    cleaned: list[dict[str, Any]] = []
    for row in events or []:
        if not isinstance(row, dict):
            continue
        item: dict[str, Any] = {}
        for key in ("provider", "feature", "page", "source", "message", "trace_id", "session_id"):
            value = str(row.get(key) or "").strip()
            if value:
                item[key] = value
        count = max(1, int(row.get("count", 1) or 1))
        item["count"] = count
        if item.get("provider"):
            cleaned.append(item)
    return cleaned


def notify_nav_dashboard_usage(
    *,
    web_search_delta: int = 0,
    deepseek_delta: int = 0,
    count_daily: bool = True,
    events: list[dict[str, Any]] | None = None,
    timeout: float = 3.0,
    background: bool = False,
) -> None:
    web_inc = max(0, int(web_search_delta or 0))
    deepseek_inc = max(0, int(deepseek_delta or 0))
    cleaned_events = _sanitize_events(events)
    if web_inc <= 0 and deepseek_inc <= 0 and not cleaned_events:
        return

    payload = json.dumps(
        {
            "web_search_delta": web_inc,
            "deepseek_delta": deepseek_inc,
            "count_daily": bool(count_daily),
            "events": cleaned_events,
        },
        ensure_ascii=False,
    ).encode("utf-8")
    req = urlrequest.Request(
        _dashboard_usage_record_url(),
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    def _send() -> None:
        try:
            opener = urlrequest.build_opener(urlrequest.ProxyHandler({}))
            with opener.open(req, timeout=timeout):
                pass
        except Exception:
            pass

    if background:
        threading.Thread(target=_send, daemon=True, name="nav-dashboard-usage-sync").start()
        return
    _send()