from __future__ import annotations

from datetime import datetime, timedelta
import json
import threading
from pathlib import Path
from typing import Any
from uuid import uuid4

from core_service.runtime_data import app_runtime_root


TICKETS_FILE = app_runtime_root("nav_dashboard") / "tickets" / "tickets.jsonl"
TICKET_STORAGE_AUDIT_FILE = app_runtime_root("core_service") / "ticket_storage_audit.jsonl"
_LOCK = threading.Lock()
_CLOSED_TICKET_STATUSES = {"resolved", "closed"}
_TREND_PRIORITIES = ("critical", "high", "medium")


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _safe_text_list(value: Any) -> list[str]:
    if isinstance(value, str):
        raw_items = value.replace("\r", "\n").replace(",", "\n").split("\n")
    elif isinstance(value, list):
        raw_items = value
    else:
        raw_items = []
    items: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        text = _safe_text(item)
        if not text or text in seen:
            continue
        seen.add(text)
        items.append(text)
    return items


def _json_safe(value: Any) -> Any:
    try:
        return json.loads(json.dumps(value, ensure_ascii=False, default=str))
    except Exception:
        return str(value)


def _field_value(payload: dict[str, Any], source: dict[str, Any], key: str, default: str = "") -> str:
    if key in payload:
        return _safe_text(payload.get(key))
    return _safe_text(source.get(key)) or default


def _make_ticket_id(now: datetime | None = None) -> str:
    moment = now or datetime.now()
    return f"TICKET-{moment.strftime('%Y%m%d')}-{uuid4().hex[:6].upper()}"


def _parse_iso_datetime(value: Any) -> datetime | None:
    text = _safe_text(value)
    if not text:
        return None
    try:
        normalized = text.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    except Exception:
        return None


def _normalize_ticket_priority(value: Any) -> str:
    key = _safe_text(value).lower()
    if key in {"critical", "p0", "sev0", "blocker"}:
        return "critical"
    if key in {"high", "p1", "sev1"}:
        return "high"
    return "medium"


def _week_bucket_start(moment: datetime) -> datetime:
    return (moment - timedelta(days=moment.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)


def _day_bucket_start(moment: datetime) -> datetime:
    return moment.replace(hour=0, minute=0, second=0, microsecond=0)


def _month_bucket_start(moment: datetime) -> datetime:
    return moment.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _month_bucket_next(moment: datetime) -> datetime:
    year = moment.year + (1 if moment.month == 12 else 0)
    month = 1 if moment.month == 12 else moment.month + 1
    return moment.replace(year=year, month=month, day=1, hour=0, minute=0, second=0, microsecond=0)


def _build_bucket_series(*, start: datetime, end: datetime, mode: str) -> list[datetime]:
    normalized = str(mode or "").strip().lower()
    if normalized == "day":
        cursor = _day_bucket_start(start)
        limit = _day_bucket_start(end)
        step = timedelta(days=1)
        values: list[datetime] = []
        while cursor <= limit:
            values.append(cursor)
            cursor += step
        return values
    if normalized == "month":
        cursor = _month_bucket_start(start)
        limit = _month_bucket_start(end)
        values = []
        while cursor <= limit:
            values.append(cursor)
            cursor = _month_bucket_next(cursor)
        return values
    cursor = _week_bucket_start(start)
    limit = _week_bucket_start(end)
    step = timedelta(days=7)
    values = []
    while cursor <= limit:
        values.append(cursor)
        cursor += step
    return values


def list_ticket_storage_paths() -> list[Path]:
    return [TICKETS_FILE]


def append_ticket_storage_audit(action: str, **fields: Any) -> None:
    payload = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "action": _safe_text(action),
    }
    for key, value in fields.items():
        if isinstance(value, Path):
            payload[str(key)] = str(value)
        else:
            payload[str(key)] = _json_safe(value)
    try:
        TICKET_STORAGE_AUDIT_FILE.parent.mkdir(parents=True, exist_ok=True)
        with TICKET_STORAGE_AUDIT_FILE.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        return


def _ensure_ticket_storage_location() -> None:
    TICKETS_FILE.parent.mkdir(parents=True, exist_ok=True)


def _title_fallback(payload: dict[str, Any]) -> str:
    for key in ("title", "summary", "actual_behavior", "repro_query"):
        text = _safe_text(payload.get(key))
        if text:
            first_line = text.splitlines()[0].strip()
            if first_line:
                return first_line[:120]
    return "未命名 Ticket"


def _normalize_ticket_state(payload: dict[str, Any], existing: dict[str, Any] | None = None) -> dict[str, Any]:
    now = datetime.now().isoformat(timespec="seconds")
    source = existing or {}
    created_at = _field_value(source, payload, "created_at") or _field_value(payload, source, "created_at") or now
    updated_at = _field_value(payload, source, "updated_at") or now
    created_by = _field_value(source, payload, "created_by") or _field_value(payload, source, "created_by") or "ai"
    updated_by = _field_value(payload, source, "updated_by") or _field_value(source, payload, "updated_by") or created_by
    ticket_id = _field_value(source, payload, "ticket_id") or _field_value(payload, source, "ticket_id") or _make_ticket_id()
    deleted_at = _field_value(payload, source, "deleted_at") or _field_value(source, payload, "deleted_at")
    deleted_by = _field_value(payload, source, "deleted_by") or _field_value(source, payload, "deleted_by")
    merged = {
        "ticket_id": ticket_id,
        "title": _field_value(payload, source, "title"),
        "status": (_field_value(payload, source, "status") or "open").lower(),
        "priority": (_field_value(payload, source, "priority") or "medium").lower(),
        "domain": _field_value(payload, source, "domain"),
        "category": _field_value(payload, source, "category"),
        "summary": _field_value(payload, source, "summary"),
        "related_traces": _safe_text_list(payload.get("related_traces") if payload.get("related_traces") is not None else source.get("related_traces")),
        "repro_query": _field_value(payload, source, "repro_query"),
        "expected_behavior": _field_value(payload, source, "expected_behavior"),
        "actual_behavior": _field_value(payload, source, "actual_behavior"),
        "root_cause": _field_value(payload, source, "root_cause"),
        "fix_notes": _field_value(payload, source, "fix_notes"),
        "additional_notes": _field_value(payload, source, "additional_notes"),
        "created_at": created_at,
        "updated_at": updated_at,
        "created_by": created_by,
        "updated_by": updated_by,
        "deleted_at": deleted_at,
        "deleted_by": deleted_by,
        "is_deleted": bool(deleted_at),
    }
    if not merged["title"]:
        merged["title"] = _title_fallback(merged)
    return merged


def _append_event_locked(event: dict[str, Any]) -> None:
    _ensure_ticket_storage_location()
    TICKETS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with TICKETS_FILE.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(_json_safe(event), ensure_ascii=False) + "\n")


def _load_ticket_events_locked() -> list[dict[str, Any]]:
    _ensure_ticket_storage_location()
    if not TICKETS_FILE.exists():
        return []
    try:
        lines = TICKETS_FILE.read_text(encoding="utf-8").splitlines()
    except Exception:
        return []
    events: list[dict[str, Any]] = []
    for line in lines:
        raw = _safe_text(line)
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue
        if isinstance(payload, dict):
            events.append(payload)
    return events


def _load_ticket_index_locked() -> dict[str, dict[str, Any]]:
    tickets: dict[str, dict[str, Any]] = {}
    for event in _load_ticket_events_locked():
        ticket_id = _safe_text(event.get("ticket_id"))
        event_type = _safe_text(event.get("event_type"))
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        if not ticket_id or not payload:
            continue
        if event_type == "ticket_created":
            tickets[ticket_id] = _normalize_ticket_state(payload)
            continue
        if event_type == "ticket_updated" and ticket_id in tickets:
            merged = dict(tickets[ticket_id])
            merged.update(payload)
            merged["ticket_id"] = ticket_id
            tickets[ticket_id] = _normalize_ticket_state(merged, existing=tickets[ticket_id])
            continue
        if event_type == "ticket_deleted" and ticket_id in tickets:
            merged = dict(tickets[ticket_id])
            merged.update(payload)
            merged["ticket_id"] = ticket_id
            tickets[ticket_id] = _normalize_ticket_state(merged, existing=tickets[ticket_id])
    return tickets


def _ticket_sort_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        _safe_text(row.get("updated_at")),
        _safe_text(row.get("created_at")),
        _safe_text(row.get("ticket_id")),
    )


def _sort_tickets(items: list[dict[str, Any]], sort: str = "updated_desc") -> list[dict[str, Any]]:
    normalized = _safe_text(sort).lower()
    reverse = normalized != "updated_asc"
    return sorted(items, key=_ticket_sort_key, reverse=reverse)


def list_tickets(
    *,
    status: str = "all",
    priority: str = "all",
    domain: str = "all",
    category: str = "all",
    search: str = "",
    created_from: str = "",
    created_to: str = "",
    limit: int = 200,
    include_deleted: bool = False,
    sort: str = "updated_desc",
) -> list[dict[str, Any]]:
    status_filter = _safe_text(status).lower()
    priority_filter = _safe_text(priority).lower()
    domain_filter = _safe_text(domain).lower()
    category_filter = _safe_text(category).lower()
    search_filter = _safe_text(search).lower()
    created_from_value = _safe_text(created_from)
    created_to_value = _safe_text(created_to)
    with _LOCK:
        items = list(_load_ticket_index_locked().values())
    filtered: list[dict[str, Any]] = []
    for item in items:
        if not include_deleted and bool(item.get("is_deleted")):
            continue
        item_status = _safe_text(item.get("status")).lower()
        if status_filter == "non_closed":
            if item_status == "closed":
                continue
        elif status_filter not in {"", "all", "*"} and item_status != status_filter:
            continue
        if priority_filter not in {"", "all", "*"} and _safe_text(item.get("priority")).lower() != priority_filter:
            continue
        if domain_filter not in {"", "all", "*"} and _safe_text(item.get("domain")).lower() != domain_filter:
            continue
        if category_filter not in {"", "all", "*"} and _safe_text(item.get("category")).lower() != category_filter:
            continue
        created_at = _safe_text(item.get("created_at"))
        created_day = created_at[:10] if len(created_at) >= 10 else created_at
        if created_from_value and created_day and created_day < created_from_value:
            continue
        if created_to_value and created_day and created_day > created_to_value:
            continue
        if search_filter:
            haystacks = [
                _safe_text(item.get("ticket_id")),
                _safe_text(item.get("title")),
                _safe_text(item.get("summary")),
                _safe_text(item.get("repro_query")),
                _safe_text(item.get("actual_behavior")),
                _safe_text(item.get("root_cause")),
                " ".join(_safe_text_list(item.get("related_traces"))),
            ]
            if not any(search_filter in text.lower() for text in haystacks if text):
                continue
        filtered.append(item)
    return _sort_tickets(filtered, sort=sort)[: max(1, int(limit))]


def get_ticket(ticket_id: str, *, include_deleted: bool = False) -> dict[str, Any] | None:
    value = _safe_text(ticket_id)
    if not value:
        return None
    with _LOCK:
        ticket = _load_ticket_index_locked().get(value)
    if ticket is None:
        return None
    if not include_deleted and bool(ticket.get("is_deleted")):
        return None
    return ticket


def create_ticket(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_ticket_state(payload if isinstance(payload, dict) else {})
    ticket_id = normalized["ticket_id"]
    with _LOCK:
        current = _load_ticket_index_locked()
        if ticket_id in current:
            raise ValueError(f"ticket_id={ticket_id} 已存在")
        _append_event_locked(
            {
                "event_id": f"ticket_event_{uuid4().hex[:12]}",
                "event_type": "ticket_created",
                "ticket_id": ticket_id,
                "timestamp": datetime.now().isoformat(timespec="seconds"),
                "payload": normalized,
            }
        )
    return normalized


def update_ticket(ticket_id: str, updates: dict[str, Any]) -> dict[str, Any]:
    value = _safe_text(ticket_id)
    if not value:
        raise ValueError("ticket_id is required")
    patch = dict(updates) if isinstance(updates, dict) else {}
    patch.pop("ticket_id", None)
    with _LOCK:
        current = _load_ticket_index_locked()
        existing = current.get(value)
        if existing is None:
            raise ValueError(f"ticket_id={value} 不存在")
        if bool(existing.get("is_deleted")):
            raise ValueError(f"ticket_id={value} 已删除")
        merged = dict(existing)
        merged.update(patch)
        merged["ticket_id"] = value
        merged["updated_at"] = datetime.now().isoformat(timespec="seconds")
        normalized = _normalize_ticket_state(merged, existing=existing)
        update_payload = {key: normalized.get(key) for key in normalized.keys() if normalized.get(key) != existing.get(key)}
        if not update_payload:
            return existing
        _append_event_locked(
            {
                "event_id": f"ticket_event_{uuid4().hex[:12]}",
                "event_type": "ticket_updated",
                "ticket_id": value,
                "timestamp": datetime.now().isoformat(timespec="seconds"),
                "payload": update_payload,
            }
        )
    return normalized


def delete_ticket(ticket_id: str, *, deleted_by: str = "human") -> dict[str, Any]:
    value = _safe_text(ticket_id)
    if not value:
        raise ValueError("ticket_id is required")
    with _LOCK:
        current = _load_ticket_index_locked()
        existing = current.get(value)
        if existing is None:
            raise ValueError(f"ticket_id={value} 不存在")
        if bool(existing.get("is_deleted")):
            return existing
        now = datetime.now().isoformat(timespec="seconds")
        payload = {
            "deleted_at": now,
            "deleted_by": _safe_text(deleted_by) or "human",
            "updated_at": now,
            "updated_by": _safe_text(deleted_by) or "human",
        }
        _append_event_locked(
            {
                "event_id": f"ticket_event_{uuid4().hex[:12]}",
                "event_type": "ticket_deleted",
                "ticket_id": value,
                "timestamp": now,
                "payload": payload,
            }
        )
        merged = dict(existing)
        merged.update(payload)
        merged["ticket_id"] = value
        return _normalize_ticket_state(merged, existing=existing)


def build_ticket_weekly_stats(*, weeks: int = 12, from_first_ticket: bool = False) -> dict[str, Any]:
    bucket_count = max(4, int(weeks or 12))
    with _LOCK:
        events = _load_ticket_events_locked()
        current = _load_ticket_index_locked()

    non_deleted_ids = {
        ticket_id
        for ticket_id, ticket in current.items()
        if isinstance(ticket, dict) and not bool(ticket.get("is_deleted"))
    }
    now = datetime.now()
    current_week = _week_bucket_start(now)
    buckets = [current_week - timedelta(days=7 * offset) for offset in range(bucket_count - 1, -1, -1)]
    bucket_lookup = {bucket.isoformat(): index for index, bucket in enumerate(buckets)}
    created_counts = [0 for _ in buckets]
    closed_counts = [0 for _ in buckets]
    created_priority_counts = [dict() for _ in buckets]
    created_at_by_ticket: dict[str, datetime] = {}
    closed_at_by_ticket: dict[str, datetime] = {}
    created_priority_by_ticket: dict[str, str] = {}
    status_by_ticket: dict[str, str] = {}
    status_counts: dict[str, int] = {}
    priority_counts: dict[str, int] = {}
    active_non_closed_ids: set[str] = set()
    submitted_last_week = 0
    submitted_last_month = 0
    closed_last_week = 0
    current_longest_open_days = 0
    recent_week_cutoff = now - timedelta(days=7)
    recent_month_cutoff = now - timedelta(days=30)

    for event in events:
        if not isinstance(event, dict):
            continue
        ticket_id = _safe_text(event.get("ticket_id"))
        if ticket_id not in non_deleted_ids:
            continue
        event_type = _safe_text(event.get("event_type"))
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        event_time = _parse_iso_datetime(event.get("timestamp")) or _parse_iso_datetime(payload.get("updated_at"))
        if event_type == "ticket_created":
            created_at = _parse_iso_datetime(payload.get("created_at")) or event_time
            if created_at is not None and ticket_id not in created_at_by_ticket:
                created_at_by_ticket[ticket_id] = created_at
            priority = _normalize_ticket_priority(payload.get("priority"))
            created_priority_by_ticket[ticket_id] = priority
            status = _safe_text(payload.get("status")).lower() or "open"
            status_by_ticket[ticket_id] = status
            if status == "closed" and ticket_id not in closed_at_by_ticket:
                closed_at_by_ticket[ticket_id] = _parse_iso_datetime(payload.get("updated_at")) or created_at or event_time or now
            continue
        if event_type == "ticket_updated":
            previous_status = status_by_ticket.get(ticket_id, "")
            next_status = _safe_text(payload.get("status")).lower() or previous_status
            if next_status:
                status_by_ticket[ticket_id] = next_status
            if (
                next_status == "closed"
                and previous_status != "closed"
                and ticket_id not in closed_at_by_ticket
            ):
                closed_at_by_ticket[ticket_id] = _parse_iso_datetime(payload.get("updated_at")) or event_time or now

    for ticket_id, created_at in created_at_by_ticket.items():
        bucket = _week_bucket_start(created_at)
        index = bucket_lookup.get(bucket.isoformat())
        priority = created_priority_by_ticket.get(ticket_id) or _normalize_ticket_priority((current.get(ticket_id) or {}).get("priority"))
        if index is not None:
            created_counts[index] += 1
            bucket_priority = created_priority_counts[index]
            bucket_priority[priority] = int(bucket_priority.get(priority, 0) or 0) + 1
        if created_at >= recent_week_cutoff:
            submitted_last_week += 1
        if created_at >= recent_month_cutoff:
            submitted_last_month += 1

    for closed_at in closed_at_by_ticket.values():
        bucket = _week_bucket_start(closed_at)
        index = bucket_lookup.get(bucket.isoformat())
        if index is not None:
            closed_counts[index] += 1
        if closed_at >= recent_week_cutoff:
            closed_last_week += 1

    for ticket_id, ticket in current.items():
        if not isinstance(ticket, dict) or bool(ticket.get("is_deleted")):
            continue
        status = _safe_text(ticket.get("status")).lower() or "open"
        if status == "closed":
            continue
        active_non_closed_ids.add(ticket_id)
        priority = _normalize_ticket_priority(ticket.get("priority"))
        status_counts[status] = status_counts.get(status, 0) + 1
        priority_counts[priority] = priority_counts.get(priority, 0) + 1
        created_at = _parse_iso_datetime(ticket.get("created_at"))
        if created_at is not None:
            age_days = max(0, (now - created_at).days)
            if age_days > current_longest_open_days:
                current_longest_open_days = age_days

    created_values = [value for value in created_at_by_ticket.values() if isinstance(value, datetime)]
    closed_values = [value for value in closed_at_by_ticket.values() if isinstance(value, datetime)]
    all_values = created_values + closed_values
    if all_values:
        earliest_ticket_time = min(all_values)
        latest_ticket_time = max(all_values + [now])
    else:
        earliest_ticket_time = now
        latest_ticket_time = now

    week_bucket_map = {
        bucket.isoformat(): {
            "bucket_start": bucket.date().isoformat(),
            "label": bucket.strftime("%m-%d"),
            "submitted": int(created_counts[index]),
            "closed": int(closed_counts[index]),
            "priority_submitted": {name: int(created_priority_counts[index].get(name, 0) or 0) for name in _TREND_PRIORITIES},
        }
        for index, bucket in enumerate(buckets)
    }

    if from_first_ticket and all_values:
        weekly_series = _build_bucket_series(start=earliest_ticket_time, end=latest_ticket_time, mode="week")
        weekly_rows = [
            week_bucket_map.get(
                bucket.isoformat(),
                {
                    "bucket_start": bucket.date().isoformat(),
                    "label": bucket.strftime("%m-%d"),
                    "submitted": 0,
                    "closed": 0,
                    "priority_submitted": {name: 0 for name in _TREND_PRIORITIES},
                },
            )
            for bucket in weekly_series
        ]
    else:
        weekly_rows = [
            {
                "bucket_start": bucket.date().isoformat(),
                "label": bucket.strftime("%m-%d"),
                "submitted": int(created_counts[index]),
                "closed": int(closed_counts[index]),
                "priority_submitted": {name: int(created_priority_counts[index].get(name, 0) or 0) for name in _TREND_PRIORITIES},
            }
            for index, bucket in enumerate(buckets)
        ]

    daily_created: dict[str, int] = {}
    daily_closed: dict[str, int] = {}
    daily_priority_created: dict[str, dict[str, int]] = {}
    monthly_created: dict[str, int] = {}
    monthly_closed: dict[str, int] = {}
    monthly_priority_created: dict[str, dict[str, int]] = {}
    for ticket_id, created_at in created_at_by_ticket.items():
        day_key = _day_bucket_start(created_at).isoformat()
        month_key = _month_bucket_start(created_at).isoformat()
        priority = created_priority_by_ticket.get(ticket_id) or _normalize_ticket_priority((current.get(ticket_id) or {}).get("priority"))
        daily_created[day_key] = daily_created.get(day_key, 0) + 1
        monthly_created[month_key] = monthly_created.get(month_key, 0) + 1
        day_bucket = daily_priority_created.setdefault(day_key, {})
        day_bucket[priority] = int(day_bucket.get(priority, 0) or 0) + 1
        month_bucket = monthly_priority_created.setdefault(month_key, {})
        month_bucket[priority] = int(month_bucket.get(priority, 0) or 0) + 1
    for closed_at in closed_values:
        day_key = _day_bucket_start(closed_at).isoformat()
        month_key = _month_bucket_start(closed_at).isoformat()
        daily_closed[day_key] = daily_closed.get(day_key, 0) + 1
        monthly_closed[month_key] = monthly_closed.get(month_key, 0) + 1

    if all_values:
        day_series = _build_bucket_series(start=earliest_ticket_time, end=latest_ticket_time, mode="day")
        month_series = _build_bucket_series(start=earliest_ticket_time, end=latest_ticket_time, mode="month")
    else:
        day_series = []
        month_series = []

    day_rows = [
        {
            "bucket_start": bucket.date().isoformat(),
            "label": bucket.strftime("%m-%d"),
            "submitted": int(daily_created.get(bucket.isoformat(), 0)),
            "closed": int(daily_closed.get(bucket.isoformat(), 0)),
            "priority_submitted": {name: int((daily_priority_created.get(bucket.isoformat(), {}) or {}).get(name, 0) or 0) for name in _TREND_PRIORITIES},
        }
        for bucket in day_series
    ]
    month_rows = [
        {
            "bucket_start": bucket.date().isoformat(),
            "label": bucket.strftime("%Y-%m"),
            "submitted": int(monthly_created.get(bucket.isoformat(), 0)),
            "closed": int(monthly_closed.get(bucket.isoformat(), 0)),
            "priority_submitted": {name: int((monthly_priority_created.get(bucket.isoformat(), {}) or {}).get(name, 0) or 0) for name in _TREND_PRIORITIES},
        }
        for bucket in month_series
    ]

    priority_counts = {name: int(priority_counts.get(name, 0) or 0) for name in _TREND_PRIORITIES}

    return {
        "weeks": weekly_rows,
        "trends": {
            "default_mode": "week",
            "start_date": earliest_ticket_time.date().isoformat() if all_values else "",
            "day": day_rows,
            "week": weekly_rows,
            "month": month_rows,
        },
        "summary": {
            "current_open_total": len(active_non_closed_ids),
            "ticket_total": len(non_deleted_ids),
            "submitted_last_week": submitted_last_week,
            "submitted_last_month": submitted_last_month,
            "closed_last_week": closed_last_week,
            "current_longest_open_days": current_longest_open_days,
        },
        "status_counts": status_counts,
        "priority_counts": priority_counts,
    }


def build_ticket_facets(items: list[dict[str, Any]]) -> dict[str, list[str]]:
    live_items = [item for item in items if not bool(item.get("is_deleted"))]
    statuses = sorted({_safe_text(item.get("status")) for item in live_items if _safe_text(item.get("status"))})
    priorities = sorted({_safe_text(item.get("priority")) for item in live_items if _safe_text(item.get("priority"))})
    domains = sorted({_safe_text(item.get("domain")) for item in live_items if _safe_text(item.get("domain"))})
    categories = sorted({_safe_text(item.get("category")) for item in live_items if _safe_text(item.get("category"))})
    return {
        "statuses": statuses,
        "priorities": priorities,
        "domains": domains,
        "categories": categories,
    }