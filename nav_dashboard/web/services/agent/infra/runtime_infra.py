from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any, Iterator
from urllib import parse as urlparse

from nav_dashboard.web.clients.internal_services import InternalServiceError, request_json
from nav_dashboard.web.services.agent import agent_types
from nav_dashboard.web.services.dashboard import dashboard_usage_service
from nav_dashboard.web.services.shared import quota_service
from nav_dashboard.web.services.runtime_paths import BUG_TICKET_OUTBOX_FILE

try:
    from core_service import get_settings
    from core_service.llm import chat_completion_with_retry, stream_chat_completion_text
except Exception as exc:  # noqa: BLE001
    get_settings = None
    chat_completion_with_retry = None
    stream_chat_completion_text = None
    _LLM_IMPORT_ERROR = exc
else:
    _LLM_IMPORT_ERROR = None


ToolExecution = agent_types.ToolExecution
TOOL_QUERY_DOC_RAG = agent_types.TOOL_QUERY_DOC_RAG
USAGE_TRACE_PREVIEW_CHARS = 120
USAGE_TRACE_RETENTION_DAYS = int(dashboard_usage_service.USAGE_TRACE_RETENTION_DAYS)
AI_SUMMARY_BASE = os.getenv("NAV_DASHBOARD_AI_SUMMARY_INTERNAL_URL", "http://127.0.0.1:8000").rstrip("/")
LOCAL_LLM_FALLBACK_URL = (
    os.getenv("NAV_DASHBOARD_LOCAL_LLM_URL", "")
    or os.getenv("AI_SUMMARY_LOCAL_LLM_URL", "http://127.0.0.1:1234/v1")
).strip()
LOCAL_LLM_FALLBACK_KEY = (
    os.getenv("NAV_DASHBOARD_LOCAL_LLM_API_KEY", "")
    or os.getenv("AI_SUMMARY_LOCAL_LLM_API_KEY", "local")
).strip() or "local"


def _load_optional_core_settings() -> Any:
    if get_settings is None:
        return None
    try:
        return get_settings()
    except Exception:
        return None


def _first_configured_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


_CORE_SETTINGS = _load_optional_core_settings()
LOCAL_LLM_FALLBACK_MODEL = _first_configured_text(
    os.getenv("NAV_DASHBOARD_LOCAL_LLM_MODEL", ""),
    os.getenv("AI_SUMMARY_LOCAL_LLM_MODEL", ""),
    getattr(_CORE_SETTINGS, "local_llm_model", ""),
)
DOC_QUERY_REWRITE_COUNT = 3
DOC_PRIMARY_QUERY_SCORE_BONUS = 0.05
DOC_VECTOR_TOP_N = 8
MAX_DOC_VECTOR_CANDIDATES = 16
DOC_SCORE_THRESHOLD = 0.35


def _http_json(
    method: str,
    url: str,
    payload: dict[str, Any] | None = None,
    timeout: float = 25.0,
    headers: dict[str, str] | None = None,
    trust_env: bool | None = None,
) -> dict[str, Any]:
    parsed = urlparse.urlparse(str(url or ""))
    hostname = str(parsed.hostname or "").strip().casefold()
    inferred_trust_env = bool(parsed.scheme in {"http", "https"} and hostname and hostname not in {"127.0.0.1", "localhost", "::1"})
    try:
        return request_json(
            method,
            url,
            payload=payload,
            timeout=timeout,
            headers=headers,
            trust_env=inferred_trust_env if trust_env is None else bool(trust_env),
            raise_for_status=True,
        )
    except InternalServiceError as exc:
        raise RuntimeError(f"HTTP {exc.status_code}: {exc.detail}") from exc
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(str(exc)) from exc


def _load_quota_state() -> dict[str, Any]:
    return quota_service.load_quota_state()


def _save_quota_state(state: dict[str, Any]) -> None:
    quota_service.save_quota_state(state)


def _current_month() -> str:
    return datetime.now().strftime("%Y-%m")


def _load_quota_history() -> dict[str, Any]:
    return quota_service.load_quota_history()


def _save_quota_history(history: dict[str, Any]) -> None:
    quota_service.save_quota_history(history)


def _set_monthly_quota_usage(*, web_search: int | None = None, deepseek: int | None = None) -> dict[str, int]:
    return quota_service.set_monthly_quota_usage(web_search=web_search, deepseek=deepseek)


def _record_quota_usage(*, web_search_delta: int = 0, deepseek_delta: int = 0) -> None:
    quota_service.record_quota_usage(web_search_delta=web_search_delta, deepseek_delta=deepseek_delta)


def _increment_quota_state(state: dict[str, Any], *, web_search_delta: int = 0, deepseek_delta: int = 0) -> None:
    quota_service.increment_quota_state(state, web_search_delta=web_search_delta, deepseek_delta=deepseek_delta)


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _normalize_usage_provider(value: str) -> str:
    return dashboard_usage_service.normalize_usage_provider(value)


def _normalize_usage_text(value: str) -> str:
    return " ".join(str(value or "").split())


def _usage_message_preview(value: str) -> str:
    return _normalize_usage_text(value)[: USAGE_TRACE_PREVIEW_CHARS]


def _parse_usage_timestamp(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _trim_usage_trace_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cutoff = datetime.now() - timedelta(days=USAGE_TRACE_RETENTION_DAYS)
    kept: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        provider = _normalize_usage_provider(str(row.get("provider") or ""))
        timestamp = _parse_usage_timestamp(str(row.get("timestamp") or row.get("ts") or ""))
        if not provider or timestamp is None or timestamp < cutoff:
            continue
        kept.append(
            {
                "timestamp": timestamp.isoformat(timespec="seconds"),
                "provider": provider,
                "feature": str(row.get("feature") or "").strip(),
                "page": str(row.get("page") or "").strip(),
                "source": str(row.get("source") or "").strip(),
                "message_preview": _usage_message_preview(str(row.get("message_preview") or row.get("message") or "")),
                "trace_id": str(row.get("trace_id") or "").strip(),
                "session_id": str(row.get("session_id") or "").strip(),
            }
        )
    kept.sort(key=lambda item: str(item.get("timestamp") or ""), reverse=True)
    return kept[:1000]


def _load_usage_trace_rows() -> list[dict[str, Any]]:
    return dashboard_usage_service.list_usage_trace_rows(days=USAGE_TRACE_RETENTION_DAYS, limit=1000, provider="all")


def _save_usage_trace_rows(rows: list[dict[str, Any]]) -> None:
    dashboard_usage_service.record_usage_events(rows)


def _record_usage_events(events: list[dict[str, Any]]) -> None:
    dashboard_usage_service.record_usage_events(events)


def _list_usage_trace_rows(*, days: int = USAGE_TRACE_RETENTION_DAYS, limit: int = 200, provider: str = "all") -> list[dict[str, Any]]:
    return dashboard_usage_service.list_usage_trace_rows(days=days, limit=limit, provider=provider)


def _clear_usage_trace_rows(provider: str = "all") -> int:
    return dashboard_usage_service.clear_usage_trace_rows(provider=provider)


_BUG_MARKER_PREFIX = "BUG-TICKET:"


def _auto_queue_bug_tickets(text: str, *, session_id: str = "", trace_id: str = "") -> None:
    from core_service.bug_ticket_payloads import parse_bug_ticket_payload

    if not text or _BUG_MARKER_PREFIX not in text:
        return
    tickets: list[dict[str, Any]] = []
    for line in text.splitlines():
        if _BUG_MARKER_PREFIX not in line:
            continue
        payload_text = line.split(_BUG_MARKER_PREFIX, 1)[1].strip().strip("`")
        try:
            payload = parse_bug_ticket_payload(payload_text)
        except ValueError:
            continue
        tickets.append(payload)
    if not tickets:
        return
    try:
        BUG_TICKET_OUTBOX_FILE.parent.mkdir(parents=True, exist_ok=True)
        with BUG_TICKET_OUTBOX_FILE.open("a", encoding="utf-8") as fh:
            for ticket in tickets:
                entry: dict[str, Any] = {
                    "queued_at": _now_iso(),
                    "source": "inline_response",
                    "session_id": session_id,
                    "trace_id": trace_id,
                    "file_paths": [],
                    "payload": ticket,
                }
                fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass


def _get_llm_profile(backend: str) -> tuple[str, str, str, int]:
    if chat_completion_with_retry is None:
        if isinstance(_LLM_IMPORT_ERROR, ModuleNotFoundError) and getattr(_LLM_IMPORT_ERROR, "name", "") == "openai":
            raise RuntimeError("Missing dependency: openai (required by core_service.llm_client)") from _LLM_IMPORT_ERROR
        detail = str(_LLM_IMPORT_ERROR) if _LLM_IMPORT_ERROR else "unknown import error"
        raise RuntimeError(f"LLM client unavailable: {detail}") from _LLM_IMPORT_ERROR

    selected = (backend or "local").strip().lower()
    settings = get_settings() if get_settings is not None else None

    if selected == "deepseek":
        if settings is None:
            raise RuntimeError("DeepSeek backend unavailable: core settings not found")
        if not (settings.api_key or "").strip():
            raise RuntimeError("DeepSeek backend unavailable: missing API key")
        return settings.api_base_url, settings.chat_model, settings.api_key, settings.timeout

    if settings is not None:
        local_url = (os.getenv("NAV_DASHBOARD_LOCAL_LLM_URL", "") or "").strip() or settings.local_llm_url
        local_model = (os.getenv("NAV_DASHBOARD_LOCAL_LLM_MODEL", "") or "").strip() or settings.local_llm_model or LOCAL_LLM_FALLBACK_MODEL
        local_key = (os.getenv("NAV_DASHBOARD_LOCAL_LLM_API_KEY", "") or "").strip() or settings.local_llm_api_key or LOCAL_LLM_FALLBACK_KEY
        timeout = settings.timeout
    else:
        local_url = (os.getenv("NAV_DASHBOARD_LOCAL_LLM_URL", "") or "").strip() or LOCAL_LLM_FALLBACK_URL
        local_model = (os.getenv("NAV_DASHBOARD_LOCAL_LLM_MODEL", "") or "").strip() or LOCAL_LLM_FALLBACK_MODEL
        local_key = (os.getenv("NAV_DASHBOARD_LOCAL_LLM_API_KEY", "") or "").strip() or LOCAL_LLM_FALLBACK_KEY
        timeout = 7200

    if local_url and not local_url.rstrip("/").endswith("/v1"):
        local_url = local_url.rstrip("/") + "/v1"
    return local_url, local_model, local_key, timeout


def _llm_chat(
    messages: list[dict[str, str]],
    backend: str,
    quota_state: dict[str, Any],
    count_quota: bool = True,
    max_tokens: int | None = None,
    temperature: float = 0.2,
    *,
    usage_feature: str = "",
    usage_page: str = "",
    usage_source: str = "nav_dashboard",
    usage_message: str = "",
    trace_id: str = "",
    session_id: str = "",
) -> str:
    base_url, model, api_key, timeout = _get_llm_profile(backend)
    is_deepseek = "api.deepseek.com" in (base_url or "").lower()
    if is_deepseek and count_quota:
        _increment_quota_state(quota_state, deepseek_delta=1)
        _record_usage_events(
            [
                {
                    "provider": "deepseek",
                    "feature": usage_feature or "nav_dashboard.agent.chat",
                    "page": usage_page or "dashboard_agent",
                    "source": usage_source,
                    "message": usage_message or next(
                        (str(item.get("content") or "") for item in reversed(messages) if str(item.get("role") or "") == "user"),
                        "",
                    ),
                    "trace_id": trace_id,
                    "session_id": session_id,
                }
            ]
        )

    return chat_completion_with_retry(
        api_key=api_key,
        base_url=base_url,
        model=model,
        timeout=timeout,
        messages=messages,
        temperature=float(temperature),
        max_retries=2,
        retry_delay=1.5,
        max_tokens=max_tokens,
    )


def _llm_chat_stream(
    messages: list[dict[str, str]],
    backend: str,
    quota_state: dict[str, Any],
    count_quota: bool = True,
    temperature: float = 0.2,
    *,
    usage_feature: str = "",
    usage_page: str = "",
    usage_source: str = "nav_dashboard",
    usage_message: str = "",
    trace_id: str = "",
    session_id: str = "",
) -> Iterator[str]:
    if stream_chat_completion_text is None:
        if isinstance(_LLM_IMPORT_ERROR, ModuleNotFoundError) and getattr(_LLM_IMPORT_ERROR, "name", "") == "openai":
            raise RuntimeError("Missing dependency: openai (required by core_service.llm_client)") from _LLM_IMPORT_ERROR
        detail = str(_LLM_IMPORT_ERROR) if _LLM_IMPORT_ERROR else "unknown import error"
        raise RuntimeError(f"LLM stream client unavailable: {detail}") from _LLM_IMPORT_ERROR

    base_url, model, api_key, timeout = _get_llm_profile(backend)
    is_deepseek = "api.deepseek.com" in (base_url or "").lower()
    if is_deepseek and count_quota:
        _increment_quota_state(quota_state, deepseek_delta=1)
        _record_usage_events(
            [
                {
                    "provider": "deepseek",
                    "feature": usage_feature or "nav_dashboard.agent.chat",
                    "page": usage_page or "dashboard_agent",
                    "source": usage_source,
                    "message": usage_message or next(
                        (str(item.get("content") or "") for item in reversed(messages) if str(item.get("role") or "") == "user"),
                        "",
                    ),
                    "trace_id": trace_id,
                    "session_id": session_id,
                }
            ]
        )

    yield from stream_chat_completion_text(
        api_key=api_key,
        base_url=base_url,
        model=model,
        timeout=timeout,
        messages=messages,
        temperature=float(temperature),
    )
