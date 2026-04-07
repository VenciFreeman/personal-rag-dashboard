from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from ..shared import quota_service
from ..runtime_paths import DATA_DIR, USAGE_TRACE_FILE


USAGE_TRACE_RETENTION_DAYS = 7
USAGE_TRACE_PREVIEW_CHARS = 20
_LOCK = threading.Lock()


def _safe_load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def load_monthly_quota_counts(*, history_path: Path) -> tuple[int, int, dict[str, int]]:
    month_key = datetime.now().strftime("%Y-%m")
    payload = _safe_load_json(history_path, default={})
    months = payload.get("months") if isinstance(payload, dict) and isinstance(payload.get("months"), dict) else {}
    row = months.get(month_key) if isinstance(months.get(month_key), dict) else {}
    web_month = int(row.get("web_search", 0) or 0)
    deepseek_month = int(row.get("deepseek", 0) or 0)

    daily = quota_service.load_quota_state()
    if web_month == 0 and deepseek_month == 0:
        web_month = int(daily.get("web_search", 0) or 0)
        deepseek_month = int(daily.get("deepseek", 0) or 0)

    return web_month, deepseek_month, {
        "today_web_search": int(daily.get("web_search", 0) or 0),
        "today_deepseek": int(daily.get("deepseek", 0) or 0),
        "daily_web_limit": int(quota_service.WEB_SEARCH_DAILY_LIMIT),
        "daily_deepseek_limit": int(quota_service.DEEPSEEK_DAILY_LIMIT),
    }


def normalize_usage_provider(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"web", "web_search", "tavily", "search_web"}:
        return "web_search"
    if normalized in {"deepseek", "llm"}:
        return "deepseek"
    return ""


def _normalize_usage_text(value: str) -> str:
    return " ".join(str(value or "").split())


def _usage_message_preview(value: str) -> str:
    return _normalize_usage_text(value)[:USAGE_TRACE_PREVIEW_CHARS]


def _normalize_usage_count(value: Any) -> int:
    try:
        return max(1, int(value or 1))
    except Exception:
        return 1


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
        provider = normalize_usage_provider(str(row.get("provider") or ""))
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
                "count": _normalize_usage_count(row.get("count", 1)),
            }
        )
    kept.sort(key=lambda item: str(item.get("timestamp") or ""), reverse=True)
    return kept[:1000]


def _load_usage_trace_rows() -> list[dict[str, Any]]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not USAGE_TRACE_FILE.exists():
        return []
    try:
        payload = json.loads(USAGE_TRACE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return []
    rows = payload.get("items") if isinstance(payload, dict) and isinstance(payload.get("items"), list) else []
    trimmed = _trim_usage_trace_rows(rows)
    if trimmed != rows:
        _save_usage_trace_rows(trimmed)
    return trimmed


def _save_usage_trace_rows(rows: list[dict[str, Any]]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    USAGE_TRACE_FILE.write_text(
        json.dumps({"items": _trim_usage_trace_rows(rows)}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def record_usage_events(events: list[dict[str, Any]]) -> None:
    normalized_events: list[dict[str, Any]] = []
    for row in events:
        if not isinstance(row, dict):
            continue
        provider = normalize_usage_provider(str(row.get("provider") or ""))
        if not provider:
            continue
        normalized_events.append(
            {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "provider": provider,
            "feature": str(row.get("feature") or "").strip(),
            "page": str(row.get("page") or "").strip(),
            "source": str(row.get("source") or "").strip(),
            "message_preview": _usage_message_preview(str(row.get("message") or row.get("message_preview") or "")),
            "trace_id": str(row.get("trace_id") or "").strip(),
            "session_id": str(row.get("session_id") or "").strip(),
            "count": _normalize_usage_count(row.get("count", 1)),
            }
        )
    if not normalized_events:
        return
    with _LOCK:
        rows = _load_usage_trace_rows()
        _save_usage_trace_rows(normalized_events + rows)


def list_usage_trace_rows(*, days: int = USAGE_TRACE_RETENTION_DAYS, limit: int = 200, provider: str = "all") -> list[dict[str, Any]]:
    normalized_provider = normalize_usage_provider(provider)
    cutoff = datetime.now() - timedelta(days=max(1, int(days or USAGE_TRACE_RETENTION_DAYS)))
    rows = _load_usage_trace_rows()
    filtered: list[dict[str, Any]] = []
    for row in rows:
        timestamp = _parse_usage_timestamp(str(row.get("timestamp") or ""))
        if timestamp is None or timestamp < cutoff:
            continue
        if normalized_provider and str(row.get("provider") or "") != normalized_provider:
            continue
        filtered.append(row)
        if len(filtered) >= max(1, int(limit)):
            break
    return filtered


def clear_usage_trace_rows(provider: str = "all") -> int:
    normalized_provider = normalize_usage_provider(provider)
    with _LOCK:
        rows = _load_usage_trace_rows()
        if not normalized_provider:
            removed = len(rows)
            _save_usage_trace_rows([])
            return removed
        kept = [row for row in rows if str(row.get("provider") or "") != normalized_provider]
        removed = len(rows) - len(kept)
        _save_usage_trace_rows(kept)
        return removed


def usage_provider_label(provider: str) -> str:
    normalized = normalize_usage_provider(provider) or str(provider or "").strip().lower()
    if normalized == "deepseek":
        return "DeepSeek"
    if normalized == "web_search":
        return "Tavily"
    return normalized or "未知"


def load_usage_traces(days: int = 7, limit: int = 200, provider: str = "all") -> list[dict[str, Any]]:
    rows = list_usage_trace_rows(days=days, limit=limit, provider=provider)
    items: list[dict[str, Any]] = []
    for row in rows:
        provider_key = str(row.get("provider") or "").strip()
        items.append({**row, "provider_label": usage_provider_label(provider_key)})
    return items
