from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any
from urllib import parse as urlparse

import httpx

from nav_dashboard.web.config import AI_SUMMARY_URL_OVERRIDE, JOURNEY_URL_OVERRIDE, LIBRARY_TRACKER_URL_OVERRIDE, PROPERTY_URL_OVERRIDE


AI_SUMMARY_DEFAULT_PORT = 8000
LIBRARY_TRACKER_DEFAULT_PORT = 8091
PROPERTY_DEFAULT_PORT = 8093
JOURNEY_DEFAULT_PORT = 8094


@dataclass(slots=True)
class InternalServiceError(RuntimeError):
    status_code: int
    detail: str

    def __str__(self) -> str:
        return self.detail


def _base_url_from_override(raw_override: str, env_name: str, default_port: int) -> str:
    raw = (os.getenv(env_name, "") or "").strip().rstrip("/")
    if raw:
        return raw
    override = str(raw_override or "").strip()
    if override:
        parsed = urlparse.urlparse(override)
        if parsed.scheme and parsed.hostname:
            port = parsed.port or default_port
            return f"{parsed.scheme}://127.0.0.1:{port}"
    return f"http://127.0.0.1:{default_port}"


def ai_summary_internal_base_url() -> str:
    return _base_url_from_override(AI_SUMMARY_URL_OVERRIDE, "NAV_DASHBOARD_AI_SUMMARY_INTERNAL_URL", AI_SUMMARY_DEFAULT_PORT)


def library_tracker_internal_base_url() -> str:
    return _base_url_from_override(
        LIBRARY_TRACKER_URL_OVERRIDE,
        "NAV_DASHBOARD_LIBRARY_TRACKER_INTERNAL_URL",
        LIBRARY_TRACKER_DEFAULT_PORT,
    )


def property_internal_base_url() -> str:
    return _base_url_from_override(PROPERTY_URL_OVERRIDE, "NAV_DASHBOARD_PROPERTY_INTERNAL_URL", PROPERTY_DEFAULT_PORT)


def journey_internal_base_url() -> str:
    return _base_url_from_override(JOURNEY_URL_OVERRIDE, "NAV_DASHBOARD_JOURNEY_INTERNAL_URL", JOURNEY_DEFAULT_PORT)


def _normalize_payload(raw: str) -> dict[str, Any]:
    if not str(raw or "").strip():
        return {}
    payload = json.loads(raw)
    return payload if isinstance(payload, dict) else {"value": payload}


def request_json(
    method: str,
    url: str,
    *,
    payload: dict[str, Any] | None = None,
    timeout: float = 5.0,
    trace_id: str = "",
    headers: dict[str, str] | None = None,
    raise_for_status: bool = False,
    trust_env: bool | None = None,
) -> dict[str, Any]:
    request_headers = {"Accept": "application/json"}
    if trace_id:
        request_headers["X-Trace-Id"] = str(trace_id).strip()
    if payload is not None:
        request_headers["Content-Type"] = "application/json; charset=utf-8"
    if isinstance(headers, dict):
        for key, value in headers.items():
            clean_key = str(key or "").strip()
            clean_value = str(value or "").strip()
            if not clean_key or not clean_value:
                continue
            request_headers[clean_key] = clean_value
    try:
        with httpx.Client(timeout=timeout, trust_env=False if trust_env is None else bool(trust_env)) as client:
            response = client.request(method.upper(), url, json=payload, headers=request_headers)
        response.raise_for_status()
        return _normalize_payload(response.text)
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text.strip() or str(exc)
        if raise_for_status:
            raise InternalServiceError(status_code=int(exc.response.status_code), detail=detail) from exc
        return {"ok": False, "error": f"HTTP {exc.response.status_code}: {detail}", "status_code": int(exc.response.status_code)}
    except Exception as exc:  # noqa: BLE001
        if raise_for_status:
            raise InternalServiceError(status_code=502, detail=str(exc)) from exc
        return {"ok": False, "error": str(exc), "status_code": 502}


def get_json(url: str, *, timeout: float = 5.0, trace_id: str = "") -> dict[str, Any]:
    return request_json("GET", url, timeout=timeout, trace_id=trace_id)