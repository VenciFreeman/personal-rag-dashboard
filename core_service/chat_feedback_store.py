from __future__ import annotations

import json
import threading
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4


WORKSPACE_ROOT = Path(__file__).resolve().parent.parent
FEEDBACK_FILE = WORKSPACE_ROOT / "nav_dashboard" / "data" / "chat_feedback.json"
FEEDBACK_MAX = 2000
_LOCK = threading.Lock()


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_metadata(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    try:
        return json.loads(json.dumps(value, ensure_ascii=False, default=str))
    except Exception:
        return {}


def _normalize_item(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    answer = _safe_text(item.get("answer"))
    if not answer:
        return None
    return {
        "id": _safe_text(item.get("id")) or f"feedback_{uuid4().hex[:12]}",
        "created_at": _safe_text(item.get("created_at")) or datetime.now().isoformat(timespec="seconds"),
        "source": _safe_text(item.get("source")) or "unknown",
        "question": _safe_text(item.get("question")),
        "answer": answer,
        "trace_id": _safe_text(item.get("trace_id")),
        "session_id": _safe_text(item.get("session_id")),
        "model": _safe_text(item.get("model")),
        "search_mode": _safe_text(item.get("search_mode")),
        "query_type": _safe_text(item.get("query_type")),
        "metadata": _normalize_metadata(item.get("metadata")),
    }


def _load_items_locked() -> list[dict[str, Any]]:
    if not FEEDBACK_FILE.exists():
        return []
    try:
        payload = json.loads(FEEDBACK_FILE.read_text(encoding="utf-8"))
    except Exception:
        return []
    rows = payload.get("items") if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        return []
    items: list[dict[str, Any]] = []
    for row in rows:
        normalized = _normalize_item(row)
        if normalized is not None:
            items.append(normalized)
    return items[-FEEDBACK_MAX:]


def _save_items_locked(items: list[dict[str, Any]]) -> None:
    FEEDBACK_FILE.parent.mkdir(parents=True, exist_ok=True)
    FEEDBACK_FILE.write_text(
        json.dumps({"items": items[-FEEDBACK_MAX:]}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def list_feedback(*, source: str = "all", limit: int = 200) -> list[dict[str, Any]]:
    source_filter = _safe_text(source).lower()
    with _LOCK:
        items = list(_load_items_locked())
    if source_filter and source_filter not in {"all", "*"}:
        items = [row for row in items if _safe_text(row.get("source")).lower() == source_filter]
    items.sort(key=lambda row: _safe_text(row.get("created_at")), reverse=True)
    return items[: max(1, int(limit))]


def append_feedback(item: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_item(item)
    if normalized is None:
        raise ValueError("feedback item requires a non-empty answer")
    with _LOCK:
        items = _load_items_locked()
        duplicate = next(
            (
                row for row in items
                if _safe_text(row.get("source")) == normalized["source"]
                and _safe_text(row.get("trace_id")) == normalized["trace_id"]
                and _safe_text(row.get("question")) == normalized["question"]
                and _safe_text(row.get("answer")) == normalized["answer"]
            ),
            None,
        )
        if duplicate is not None:
            return duplicate
        items.append(normalized)
        _save_items_locked(items)
    return normalized


def clear_feedback(*, source: str = "all") -> int:
    source_filter = _safe_text(source).lower()
    with _LOCK:
        items = _load_items_locked()
        if source_filter and source_filter not in {"all", "*"}:
            kept = [row for row in items if _safe_text(row.get("source")).lower() != source_filter]
        else:
            kept = []
        removed = max(0, len(items) - len(kept))
        _save_items_locked(kept)
    return removed
