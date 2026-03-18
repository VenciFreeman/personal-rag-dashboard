from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any


WORKSPACE_ROOT = Path(__file__).resolve().parent.parent
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

import core_service.ticket_store as ticket_store


BUG_MARKER_PREFIX = "BUG-TICKET:"
TRACE_ID_RE = re.compile(r"\btrace_[A-Za-z0-9_]+\b")
STATUS_ORDER = {"open": 0, "in_progress": 1, "resolved": 2, "closed": 3}
PRIORITY_ORDER = {"low": 0, "medium": 1, "high": 2, "critical": 3}
DEBUG_MAX_STRING_LEN = 1600
DEBUG_MAX_ITEMS = 40


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


def _normalize_compare_text(value: Any) -> str:
    text = _safe_text(value).lower()
    text = re.sub(r"[^\w\u4e00-\u9fff]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _json_output(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False))


def _debug_log_path(hook_input: dict[str, Any]) -> Path | None:
    raw_path = _safe_text(os.environ.get("BUG_TICKET_DEBUG_LOG"))
    base = _resolve_path(_safe_text(hook_input.get("cwd")) or str(WORKSPACE_ROOT), WORKSPACE_ROOT)
    if not raw_path:
        return base / "nav_dashboard" / "data" / "bug_ticket_hook_debug.jsonl"
    return _resolve_path(raw_path, base)


def _trim_debug_value(value: Any, *, depth: int = 0) -> Any:
    if depth >= 4:
        return _safe_text(value)[:DEBUG_MAX_STRING_LEN]
    if isinstance(value, str):
        return value[:DEBUG_MAX_STRING_LEN]
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    if isinstance(value, list):
        trimmed = [_trim_debug_value(item, depth=depth + 1) for item in value[:DEBUG_MAX_ITEMS]]
        if len(value) > DEBUG_MAX_ITEMS:
            trimmed.append(f"... {len(value) - DEBUG_MAX_ITEMS} more item(s)")
        return trimmed
    if isinstance(value, dict):
        items = list(value.items())[:DEBUG_MAX_ITEMS]
        trimmed_dict = {str(key): _trim_debug_value(item, depth=depth + 1) for key, item in items}
        if len(value) > DEBUG_MAX_ITEMS:
            trimmed_dict["__truncated_keys__"] = len(value) - DEBUG_MAX_ITEMS
        return trimmed_dict
    return _safe_text(value)[:DEBUG_MAX_STRING_LEN]


def _append_debug_log(hook_input: dict[str, Any], payload: dict[str, Any]) -> None:
    path = _debug_log_path(hook_input)
    if path is None:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        entry = {
            "logged_at": datetime.now().isoformat(timespec="seconds"),
            **payload,
        }
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(_trim_debug_value(entry), ensure_ascii=False) + "\n")
    except Exception:
        return


def _read_hook_input() -> dict[str, Any]:
    raw = sys.stdin.read().strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_hook_input_with_raw() -> tuple[str, dict[str, Any]]:
    raw = sys.stdin.read().strip()
    if not raw:
        return "", {}
    try:
        payload = json.loads(raw)
    except Exception:
        return raw, {}
    return raw, payload if isinstance(payload, dict) else {}


def _resolve_path(value: str, base: Path) -> Path:
    candidate = Path(value)
    if not candidate.is_absolute():
        candidate = base / candidate
    return candidate.resolve()


def _configure_ticket_file(hook_input: dict[str, Any]) -> Path:
    base = _resolve_path(_safe_text(hook_input.get("cwd")) or str(WORKSPACE_ROOT), WORKSPACE_ROOT)
    explicit_file = _safe_text(os.environ.get("BUG_TICKET_SYNC_FILE"))
    explicit_dir = _safe_text(os.environ.get("BUG_TICKET_SYNC_DIR"))
    if explicit_file:
        ticket_file = _resolve_path(explicit_file, base)
    elif explicit_dir:
        ticket_file = _resolve_path(explicit_dir, base) / "tickets.jsonl"
    else:
        ticket_file = base / "nav_dashboard" / "data" / "tickets.jsonl"
    ticket_store.TICKETS_FILE = ticket_file
    return ticket_file


def _outbox_file(ticket_file: Path) -> Path:
    explicit_path = _safe_text(os.environ.get("BUG_TICKET_OUTBOX_FILE"))
    if explicit_path:
        return _resolve_path(explicit_path, ticket_file.parent)
    return ticket_file.with_name("bug_ticket_outbox.jsonl")


def _backfill_state_file(ticket_file: Path) -> Path:
    explicit_path = _safe_text(os.environ.get("BUG_TICKET_BACKFILL_STATE_FILE"))
    if explicit_path:
        return _resolve_path(explicit_path, ticket_file.parent)
    return ticket_file.with_name("bug_ticket_backfill_state.json")


def _invocation_state_file(ticket_file: Path) -> Path:
    explicit_path = _safe_text(os.environ.get("BUG_TICKET_INVOCATION_STATE_FILE"))
    if explicit_path:
        return _resolve_path(explicit_path, ticket_file.parent)
    return ticket_file.with_name("bug_ticket_hook_invocations.json")


def _load_invocation_state(path: Path) -> dict[str, float]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    state: dict[str, float] = {}
    for key, value in payload.items():
        try:
            state[str(key)] = float(value)
        except Exception:
            continue
    return state


def _save_invocation_state(path: Path, state: dict[str, float]) -> None:
    trimmed_items = sorted(state.items(), key=lambda item: item[1], reverse=True)[:1000]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(trimmed_items), ensure_ascii=False, indent=2), encoding="utf-8")


def _fingerprint_hook_invocation(hook_input: dict[str, Any]) -> str:
    session_id = _safe_text(hook_input.get("sessionId"))
    hook_event_name = _safe_text(
        hook_input.get("hookEventName")
        or ((hook_input.get("hookSpecificOutput") or {}).get("hookEventName") if isinstance(hook_input.get("hookSpecificOutput"), dict) else "")
    )
    transcript_path = _safe_text(hook_input.get("transcript_path") or hook_input.get("transcriptPath"))
    transcript_inline = hook_input.get("transcript")
    transcript_text = _flatten_text(transcript_inline) if transcript_inline not in {None, ""} else ""

    if transcript_path:
        # Include file size so the fingerprint changes each time the transcript
        # file grows (i.e., each new conversation turn).  Without this, every
        # invocation inside the same session shares the same path-based fingerprint
        # and gets suppressed after the very first turn.
        try:
            fsize = Path(transcript_path).stat().st_size
        except Exception:
            fsize = 0
        transcript_digest = _normalize_compare_text(transcript_path)[:300] + f"|sz={fsize}"
    else:
        transcript_digest = _normalize_compare_text(transcript_text)[:400]

    return " | ".join(part for part in [hook_event_name, session_id, transcript_digest] if part)


def _should_skip_duplicate_invocation(ticket_file: Path, hook_input: dict[str, Any]) -> bool:
    fingerprint = _fingerprint_hook_invocation(hook_input)
    if not fingerprint:
        return False
    state_file = _invocation_state_file(ticket_file)
    state = _load_invocation_state(state_file)
    now = datetime.now().timestamp()
    ttl_seconds = max(60.0, float(_safe_text(os.environ.get("BUG_TICKET_INVOCATION_TTL_SECONDS")) or "900"))
    cutoff = now - ttl_seconds
    state = {key: value for key, value in state.items() if value >= cutoff}
    last_seen = float(state.get(fingerprint, 0.0) or 0.0)
    if last_seen >= cutoff:
        _append_debug_log(
            hook_input,
            {
                "phase": "duplicate_invocation_skipped",
                "fingerprint": fingerprint,
                "state_file": state_file.as_posix(),
                "hook_source": _safe_text(os.environ.get("BUG_TICKET_HOOK_SOURCE")) or "unknown",
            },
        )
        _save_invocation_state(state_file, state)
        return True
    state[fingerprint] = now
    _save_invocation_state(state_file, state)
    return False


def _workspace_storage_root() -> Path | None:
    explicit_root = _safe_text(os.environ.get("BUG_TICKET_WORKSPACE_STORAGE_ROOT"))
    if explicit_root:
        path = _resolve_path(explicit_root, WORKSPACE_ROOT)
        return path if path.exists() else None
    appdata = _safe_text(os.environ.get("APPDATA"))
    if not appdata:
        return None
    path = Path(appdata) / "Code" / "User" / "workspaceStorage"
    return path if path.exists() else None


def _code_logs_root() -> Path | None:
    explicit_root = _safe_text(os.environ.get("BUG_TICKET_CODE_LOGS_ROOT"))
    if explicit_root:
        path = _resolve_path(explicit_root, WORKSPACE_ROOT)
        return path if path.exists() else None
    appdata = _safe_text(os.environ.get("APPDATA"))
    if not appdata:
        return None
    path = Path(appdata) / "Code" / "logs"
    return path if path.exists() else None


def _load_backfill_state(path: Path) -> dict[str, float]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    state: dict[str, float] = {}
    for key, value in payload.items():
        try:
            state[str(key)] = float(value)
        except Exception:
            continue
    return state


def _save_backfill_state(path: Path, state: dict[str, float]) -> None:
    trimmed_items = sorted(state.items(), key=lambda item: item[1], reverse=True)[:2000]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(trimmed_items), ensure_ascii=False, indent=2), encoding="utf-8")


def _iter_workspace_storage_transcripts(root: Path, *, cutoff_epoch: float, max_files: int) -> list[Path]:
    candidates: list[tuple[float, Path]] = []
    seen: set[str] = set()
    patterns = [
        "*/GitHub.copilot-chat/chat-session-resources/**/*.txt",
        "*/GitHub.copilot-chat/chat-session-resources/**/*.json",
        "*/GitHub.copilot-chat/chat-session-resources/**/*.md",
    ]
    for pattern in patterns:
        for path in root.glob(pattern):
            if not path.is_file():
                continue
            suffix = str(path.suffix or "").strip().lower()
            if suffix not in {".txt", ".json", ".md"}:
                continue
            normalized = path.as_posix()
            if normalized in seen:
                continue
            seen.add(normalized)
            try:
                stat = path.stat()
            except OSError:
                continue
            mtime = stat.st_mtime
            if mtime < cutoff_epoch:
                continue
            if stat.st_size > 2 * 1024 * 1024:
                continue
            candidates.append((mtime, path))
    candidates.sort(key=lambda item: item[0], reverse=True)
    return [path for _, path in candidates[:max_files]]


def _iter_code_log_candidates(root: Path, *, cutoff_epoch: float, max_files: int) -> list[Path]:
    candidates: list[tuple[float, Path]] = []
    seen: set[str] = set()
    patterns = [
        "**/GitHub.copilot-chat/*.log",
        "**/GitHub.copilot-chat/*.txt",
        "**/GitHub.copilot-chat/*.json",
    ]
    for pattern in patterns:
        for path in root.glob(pattern):
            if not path.is_file():
                continue
            normalized = path.as_posix()
            if normalized in seen:
                continue
            seen.add(normalized)
            try:
                stat = path.stat()
            except OSError:
                continue
            if stat.st_mtime < cutoff_epoch or stat.st_size > 2 * 1024 * 1024:
                continue
            candidates.append((stat.st_mtime, path))
    candidates.sort(key=lambda item: item[0], reverse=True)
    return [path for _, path in candidates[:max_files]]


def _load_transcript(hook_input: dict[str, Any]) -> Any:
    transcript_path = _safe_text(hook_input.get("transcript_path") or hook_input.get("transcriptPath"))
    inline_transcript = hook_input.get("transcript")
    if inline_transcript is not None and inline_transcript != "":
        return inline_transcript
    if not transcript_path:
        return hook_input
    path = _resolve_path(transcript_path, WORKSPACE_ROOT)
    if not path.exists():
        return hook_input
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        try:
            return {"raw_text": path.read_text(encoding="utf-8-sig")}
        except Exception:
            return {}


def _count_role_messages(value: Any) -> int:
    role_texts: list[tuple[str, str]] = []
    _collect_role_texts(value, role_texts)
    return len(role_texts)


def _flatten_text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        parts = [_flatten_text(item) for item in value]
        return "\n".join(part for part in parts if part).strip()
    if isinstance(value, dict):
        parts: list[str] = []
        for key in ("text", "content", "message", "value", "prompt", "tool_response", "toolResponse", "response", "body"):
            if key in value:
                text = _flatten_text(value.get(key))
                if text:
                    parts.append(text)
        if parts:
            joined: list[str] = []
            for item in parts:
                if item not in joined:
                    joined.append(item)
            return "\n".join(joined).strip()
    return ""


def _collect_role_texts(value: Any, out: list[tuple[str, str]]) -> None:
    if isinstance(value, dict):
        role = _safe_text(value.get("role") or value.get("speaker") or value.get("author")).lower()
        text = ""
        for key in ("content", "text", "message", "response"):
            if key in value:
                text = _flatten_text(value.get(key))
                if text:
                    break
        if role and text:
            out.append((role, text))
        for child in value.values():
            _collect_role_texts(child, out)
        return
    if isinstance(value, list):
        for item in value:
            _collect_role_texts(item, out)


def _collect_file_paths(value: Any, out: set[str]) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            lowered = key.lower()
            if lowered in {"filepath", "path"} and isinstance(child, str):
                text = child.replace("\\", "/").strip()
                if text:
                    out.add(text)
            elif lowered in {"filepaths", "paths"} and isinstance(child, list):
                for item in child:
                    if isinstance(item, str) and item.strip():
                        out.add(item.replace("\\", "/").strip())
            _collect_file_paths(child, out)
        return
    if isinstance(value, list):
        for item in value:
            _collect_file_paths(item, out)


def _collect_all_strings(value: Any, out: list[str]) -> None:
    if isinstance(value, str):
        text = value.strip()
        if text:
            out.append(text)
        return
    if isinstance(value, dict):
        for child in value.values():
            _collect_all_strings(child, out)
        return
    if isinstance(value, list):
        for item in value:
            _collect_all_strings(item, out)


def _extract_bug_markers(texts: list[str]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for text in texts:
        for line in text.splitlines():
            if BUG_MARKER_PREFIX not in line:
                continue
            payload_text = line.split(BUG_MARKER_PREFIX, 1)[1].strip().strip("`")
            try:
                payload = json.loads(payload_text)
            except Exception:
                continue
            if isinstance(payload, dict):
                candidates.append(payload)
    return candidates


def _extract_bug_markers_from_any(value: Any) -> list[dict[str, Any]]:
    texts: list[str] = []
    _collect_all_strings(value, texts)
    return _extract_bug_markers(texts)


def _infer_domain_from_files(file_paths: list[str]) -> str:
    lowered = [item.lower() for item in file_paths]
    if any(path.startswith("nav_dashboard/") for path in lowered):
        return "nav_dashboard"
    if any(path.startswith("ai_conversations_summary/") for path in lowered):
        return "ai_conversations_summary"
    if any(path.startswith("library_tracker/") for path in lowered):
        return "library_tracker"
    if any(path.startswith("core_service/") for path in lowered):
        return "core_service"
    return ""


def _normalize_category(value: str) -> str:
    raw = _safe_text(value).lower().replace("-", "_")
    raw = re.sub(r"[^a-z0-9_]+", "_", raw)
    return re.sub(r"_+", "_", raw).strip("_")


def _merge_multiline(existing: str, incoming: str) -> str:
    left = _safe_text(existing)
    right = _safe_text(incoming)
    if not left:
        return right
    if not right or right in left:
        return left
    if left in right:
        return right
    return f"{left}\n\n{right}"


def _normalize_candidate(payload: dict[str, Any], *, session_id: str, file_paths: list[str], all_text: str) -> dict[str, Any]:
    related_traces = _safe_text_list(payload.get("related_traces"))
    if not related_traces:
        related_traces = sorted(set(TRACE_ID_RE.findall(json.dumps(payload, ensure_ascii=False))))
    status = _safe_text(payload.get("status")).lower() or "open"
    if status not in STATUS_ORDER:
        status = "resolved" if status in {"fixed", "done"} else "open"
    priority = _safe_text(payload.get("priority")).lower() or "medium"
    if priority not in PRIORITY_ORDER:
        priority = "medium"
    domain = _safe_text(payload.get("domain")) or _infer_domain_from_files(file_paths)
    category = _normalize_category(_safe_text(payload.get("category")))
    title = _safe_text(payload.get("title"))[:120]
    summary = _safe_text(payload.get("summary"))
    actual_behavior = _safe_text(payload.get("actual_behavior"))
    repro_query = _safe_text(payload.get("repro_query"))
    root_cause = _safe_text(payload.get("root_cause"))
    if not title:
        title = (summary or actual_behavior or repro_query or "未命名 Ticket").splitlines()[0][:120]
    notes = _safe_text(payload.get("additional_notes"))
    touched_files = ", ".join(file_paths[:8])
    session_note = f"hook_session={session_id}" if session_id else "hook_session=unknown"
    if touched_files:
        session_note = f"{session_note}; touched_files={touched_files}"
    notes = _merge_multiline(notes, session_note)
    return {
        "title": title or "未命名 Ticket",
        "status": status,
        "priority": priority,
        "domain": domain,
        "category": category,
        "summary": summary,
        "related_traces": related_traces,
        "repro_query": repro_query,
        "expected_behavior": _safe_text(payload.get("expected_behavior")),
        "actual_behavior": actual_behavior,
        "root_cause": root_cause,
        "fix_notes": _safe_text(payload.get("fix_notes")),
        "additional_notes": notes,
        "created_by": _safe_text(payload.get("created_by")) or _safe_text(os.environ.get("BUG_TICKET_CREATED_BY")) or "ai-hook",
        "updated_by": _safe_text(payload.get("updated_by")) or _safe_text(os.environ.get("BUG_TICKET_UPDATED_BY")) or "ai-hook",
    }


def _signature(payload: dict[str, Any]) -> str:
    parts = [
        payload.get("domain"),
        payload.get("category"),
        payload.get("title"),
        payload.get("summary"),
        payload.get("repro_query"),
        payload.get("actual_behavior"),
        payload.get("root_cause"),
    ]
    return " | ".join(part for part in (_normalize_compare_text(item) for item in parts) if part)


def _dedupe_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        key = _signature(candidate)
        if not key:
            continue
        if key not in merged:
            merged[key] = dict(candidate)
            continue
        current = merged[key]
        if STATUS_ORDER.get(candidate.get("status", "open"), 0) > STATUS_ORDER.get(current.get("status", "open"), 0):
            current["status"] = candidate.get("status", current.get("status"))
        if PRIORITY_ORDER.get(candidate.get("priority", "medium"), 1) > PRIORITY_ORDER.get(current.get("priority", "medium"), 1):
            current["priority"] = candidate.get("priority", current.get("priority"))
        for field in ("summary", "expected_behavior", "actual_behavior", "root_cause", "fix_notes", "additional_notes"):
            current[field] = _merge_multiline(_safe_text(current.get(field)), _safe_text(candidate.get(field)))
        current["related_traces"] = _safe_text_list(_safe_text_list(current.get("related_traces")) + _safe_text_list(candidate.get("related_traces")))
        if not _safe_text(current.get("title")):
            current["title"] = candidate.get("title", "")
        if not _safe_text(current.get("domain")):
            current["domain"] = candidate.get("domain", "")
        if not _safe_text(current.get("category")):
            current["category"] = candidate.get("category", "")
    return list(merged.values())


def _ticket_datetime(ticket: dict[str, Any]) -> datetime | None:
    for key in ("updated_at", "created_at"):
        value = _safe_text(ticket.get(key))
        if not value:
            continue
        try:
            return datetime.fromisoformat(value)
        except Exception:
            continue
    return None


def _recent_tickets() -> list[dict[str, Any]]:
    days = max(1, int(_safe_text(os.environ.get("BUG_TICKET_LOOKBACK_DAYS")) or "21"))
    limit = max(50, int(_safe_text(os.environ.get("BUG_TICKET_RECENT_LIMIT")) or "1000"))
    cutoff = datetime.now() - timedelta(days=days)
    items = ticket_store.list_tickets(limit=limit)
    recent: list[dict[str, Any]] = []
    for item in items:
        moment = _ticket_datetime(item)
        if moment is not None and moment < cutoff:
            continue
        recent.append(item)
    return recent


def _similarity(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    return SequenceMatcher(None, left, right).ratio()


def _match_existing(candidate: dict[str, Any], recent_tickets: list[dict[str, Any]]) -> dict[str, Any] | None:
    candidate_traces = set(_safe_text_list(candidate.get("related_traces")))
    candidate_signature = _signature(candidate)
    candidate_category = _safe_text(candidate.get("category"))
    candidate_domain = _safe_text(candidate.get("domain"))
    best_ticket: dict[str, Any] | None = None
    best_score = 0.0
    for item in recent_tickets:
        item_traces = set(_safe_text_list(item.get("related_traces")))
        if candidate_traces and item_traces and candidate_traces & item_traces:
            return item
        item_category = _safe_text(item.get("category"))
        item_domain = _safe_text(item.get("domain"))
        if candidate_category and item_category and candidate_category != item_category:
            continue
        if candidate_domain and item_domain and candidate_domain != item_domain:
            continue
        score = 0.0
        if candidate_category and candidate_category == item_category:
            score += 0.35
        if candidate_domain and candidate_domain == item_domain:
            score += 0.15
        candidate_title = _normalize_compare_text(candidate.get("title"))
        item_title = _normalize_compare_text(item.get("title"))
        if candidate_title and item_title and candidate_title == item_title:
            score += 0.3
        score += 0.2 * _similarity(candidate_title, item_title)
        score += 0.3 * _similarity(candidate_signature, _signature(item))
        if score > best_score:
            best_score = score
            best_ticket = item
    threshold = float(_safe_text(os.environ.get("BUG_TICKET_MATCH_THRESHOLD")) or "0.74")
    return best_ticket if best_ticket is not None and best_score >= threshold else None


def _merge_ticket_payload(existing: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    merged = dict(existing)
    merged["title"] = candidate.get("title") or existing.get("title")
    merged["status"] = candidate.get("status") or existing.get("status") or "open"
    merged["priority"] = (
        candidate.get("priority")
        if PRIORITY_ORDER.get(_safe_text(candidate.get("priority")), -1) >= PRIORITY_ORDER.get(_safe_text(existing.get("priority")), -1)
        else existing.get("priority")
    )
    for field in ("domain", "category", "summary", "repro_query", "expected_behavior", "actual_behavior", "root_cause"):
        incoming = _safe_text(candidate.get(field))
        if incoming:
            merged[field] = incoming
    for field in ("fix_notes", "additional_notes"):
        merged[field] = _merge_multiline(_safe_text(existing.get(field)), _safe_text(candidate.get(field)))
    merged["related_traces"] = _safe_text_list(_safe_text_list(existing.get("related_traces")) + _safe_text_list(candidate.get("related_traces")))
    merged["updated_by"] = candidate.get("updated_by") or _safe_text(os.environ.get("BUG_TICKET_UPDATED_BY")) or "ai-hook"
    return merged


def _extract_candidates(transcript: Any, session_id: str) -> list[dict[str, Any]]:
    role_texts: list[tuple[str, str]] = []
    _collect_role_texts(transcript, role_texts)
    if not role_texts:
        flat = _flatten_text(transcript)
        role_texts = [("assistant", flat)] if flat else []
    deduped_texts: list[str] = []
    for _, text in role_texts:
        if text and text not in deduped_texts:
            deduped_texts.append(text)
    all_strings: list[str] = []
    _collect_all_strings(transcript, all_strings)
    file_paths: set[str] = set()
    _collect_file_paths(transcript, file_paths)
    combined_text = "\n".join(all_strings)
    marker_payloads = _extract_bug_markers(deduped_texts)
    if not marker_payloads:
        marker_payloads = _extract_bug_markers_from_any(transcript)
    normalized = [
        _normalize_candidate(payload, session_id=session_id, file_paths=sorted(file_paths), all_text=combined_text)
        for payload in marker_payloads
    ]
    return _dedupe_candidates(normalized)


def _extract_outbox_candidates(outbox_file: Path) -> tuple[list[dict[str, Any]], list[str], int, int]:
    if not outbox_file.exists():
        return [], [], 0, 0
    try:
        raw_lines = outbox_file.read_text(encoding="utf-8").splitlines()
    except Exception:
        return [], [], 0, 0

    normalized: list[dict[str, Any]] = []
    retained_lines: list[str] = []
    consumed_count = 0
    invalid_count = 0
    for raw_line in raw_lines:
        line = raw_line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except Exception:
            retained_lines.append(raw_line)
            invalid_count += 1
            continue
        if not isinstance(entry, dict):
            retained_lines.append(raw_line)
            invalid_count += 1
            continue
        payload = entry.get("payload") if isinstance(entry.get("payload"), dict) else entry
        if not isinstance(payload, dict):
            retained_lines.append(raw_line)
            invalid_count += 1
            continue
        session_id = _safe_text(entry.get("session_id") or entry.get("sessionId") or entry.get("source_session"))
        file_paths = _safe_text_list(entry.get("file_paths") or entry.get("filePaths"))
        normalized.append(_normalize_candidate(payload, session_id=session_id, file_paths=file_paths, all_text=json.dumps(entry, ensure_ascii=False)))
        consumed_count += 1
    return _dedupe_candidates(normalized), retained_lines, consumed_count, invalid_count


def _rewrite_outbox_file(outbox_file: Path, retained_lines: list[str]) -> None:
    outbox_file.parent.mkdir(parents=True, exist_ok=True)
    if not retained_lines:
        outbox_file.write_text("", encoding="utf-8")
        return
    outbox_file.write_text("\n".join(retained_lines) + "\n", encoding="utf-8")


def _sync_outbox_fallback(ticket_file: Path, hook_input: dict[str, Any]) -> dict[str, Any]:
    outbox_file = _outbox_file(ticket_file)
    candidates, retained_lines, consumed_count, invalid_count = _extract_outbox_candidates(outbox_file)
    if not consumed_count and not invalid_count:
        return {"continue": True, "outbox_consumed": 0, "outbox_invalid": 0}

    created: list[str] = []
    updated: list[str] = []
    if candidates:
        recent = _recent_tickets()
        created, updated, recent = _sync_candidates(candidates, recent)
    _rewrite_outbox_file(outbox_file, retained_lines)
    summary = _summarize(created, updated, ticket_file)
    _append_debug_log(
        hook_input,
        {
            "phase": "outbox_fallback",
            "outbox_file": outbox_file.as_posix(),
            "outbox_consumed": consumed_count,
            "outbox_invalid": invalid_count,
            "candidate_categories": [_safe_text(item.get("category")) for item in candidates],
            "created": created,
            "updated": updated,
            "summary": summary,
        },
    )
    payload: dict[str, Any] = {
        "continue": True,
        "outbox_consumed": consumed_count,
        "outbox_invalid": invalid_count,
    }
    if summary:
        payload["systemMessage"] = summary
    return payload


def _summarize(created: list[str], updated: list[str], ticket_file: Path) -> str:
    parts: list[str] = []
    if created:
        parts.append(f"created {len(created)} ticket(s): {', '.join(created[:4])}")
    if updated:
        parts.append(f"updated {len(updated)} ticket(s): {', '.join(updated[:4])}")
    if not parts:
        return ""
    return f"Bug ticket sync -> {'; '.join(parts)} | store={ticket_file.as_posix()}"


def _sync_candidates(candidates: list[dict[str, Any]], recent: list[dict[str, Any]]) -> tuple[list[str], list[str], list[dict[str, Any]]]:
    created: list[str] = []
    updated: list[str] = []
    for candidate in candidates:
        existing = _match_existing(candidate, recent)
        if existing is None:
            item = ticket_store.create_ticket(candidate)
            created.append(_safe_text(item.get("ticket_id")))
            recent.insert(0, item)
            continue
        merged = _merge_ticket_payload(existing, candidate)
        item = ticket_store.update_ticket(_safe_text(existing.get("ticket_id")), merged)
        updated.append(_safe_text(item.get("ticket_id")))
        recent = [item if _safe_text(row.get("ticket_id")) == _safe_text(item.get("ticket_id")) else row for row in recent]
    return created, updated, recent


def _sync_workspace_storage_backfill(ticket_file: Path, hook_input: dict[str, Any]) -> dict[str, Any]:
    root = _workspace_storage_root()
    logs_root = _code_logs_root()
    if root is None and logs_root is None:
        return {"continue": True, "scanned_files": 0, "matched_files": 0}

    cutoff_hours = max(1, int(_safe_text(os.environ.get("BUG_TICKET_BACKFILL_HOURS")) or "72"))
    max_files = max(20, int(_safe_text(os.environ.get("BUG_TICKET_BACKFILL_MAX_FILES")) or "400"))
    cutoff_epoch = (datetime.now() - timedelta(hours=cutoff_hours)).timestamp()
    state_file = _backfill_state_file(ticket_file)
    previous_state = _load_backfill_state(state_file)
    next_state: dict[str, float] = {}

    recent = _recent_tickets()
    created: list[str] = []
    updated: list[str] = []
    scanned_files = 0
    matched_files = 0

    files: list[Path] = []
    if root is not None:
        files.extend(_iter_workspace_storage_transcripts(root, cutoff_epoch=cutoff_epoch, max_files=max_files))
    if logs_root is not None:
        files.extend(_iter_code_log_candidates(logs_root, cutoff_epoch=cutoff_epoch, max_files=max(20, max_files // 2)))

    deduped_files: list[Path] = []
    seen_files: set[str] = set()
    for path in files:
        key = path.as_posix()
        if key in seen_files:
            continue
        seen_files.add(key)
        deduped_files.append(path)

    files = deduped_files[:max_files]
    # Files modified within the last 10 minutes are always re-scanned,
    # regardless of whether their mtime is already in the backfill state.
    always_rescan_before = datetime.now().timestamp() - 600
    for path in files:
        key = path.as_posix()
        try:
            mtime = path.stat().st_mtime
        except OSError:
            continue
        previous_mtime = float(previous_state.get(key, 0.0) or 0.0)
        next_state[key] = max(previous_mtime, mtime)
        # Skip files whose mtime hasn't changed, UNLESS they're very recent
        if previous_mtime >= mtime and mtime < always_rescan_before:
            continue

        scanned_files += 1
        try:
            transcript_text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        next_state[key] = mtime
        if BUG_MARKER_PREFIX not in transcript_text:
            continue

        matched_files += 1
        session_folder = path.parents[1].name if len(path.parents) > 1 else "unknown"
        transcript_source = {
            "transcript": transcript_text,
            "transcript_path": key,
        }
        candidates = _extract_candidates(transcript_source, session_id=f"workspace-storage:{session_folder}")
        created_now, updated_now, recent = _sync_candidates(candidates, recent)
        created.extend(created_now)
        updated.extend(updated_now)

    _save_backfill_state(state_file, next_state)
    summary = _summarize(created, updated, ticket_file)
    _append_debug_log(
        hook_input,
        {
            "phase": "workspace_storage_backfill",
            "workspace_storage_root": root.as_posix() if root is not None else "",
            "code_logs_root": logs_root.as_posix() if logs_root is not None else "",
            "state_file": state_file.as_posix(),
            "scanned_files": scanned_files,
            "matched_files": matched_files,
            "created": created,
            "updated": updated,
            "summary": summary,
        },
    )
    payload: dict[str, Any] = {
        "continue": True,
        "scanned_files": scanned_files,
        "matched_files": matched_files,
    }
    if summary:
        payload["systemMessage"] = summary
    return payload


def main() -> int:
    raw_hook_input, hook_input = _read_hook_input_with_raw()
    scan_workspace_storage = "--scan-workspace-storage" in sys.argv[1:]
    if scan_workspace_storage and not hook_input:
        hook_input = {
            "cwd": str(WORKSPACE_ROOT),
            "hookEventName": "workspace_storage_backfill",
        }
    try:
        _append_debug_log(
            hook_input or {"cwd": str(WORKSPACE_ROOT)},
            {
                "phase": "launch",
                "argv": sys.argv[1:],
                "raw_input_len": len(raw_hook_input),
                "hook_input_keys": sorted(str(key) for key in hook_input.keys()),
                "transcript_path": _safe_text(hook_input.get("transcript_path") or hook_input.get("transcriptPath")),
                "session_id": _safe_text(hook_input.get("sessionId")),
                "hook_source": _safe_text(os.environ.get("BUG_TICKET_HOOK_SOURCE")) or "unknown",
                "scan_workspace_storage": scan_workspace_storage,
            },
        )
        ticket_file = _configure_ticket_file(hook_input)
        if scan_workspace_storage:
            _json_output(_sync_workspace_storage_backfill(ticket_file, hook_input))
            return 0
        if _should_skip_duplicate_invocation(ticket_file, hook_input):
            _json_output({"continue": True})
            return 0

        transcript = _load_transcript(hook_input)
        session_id = _safe_text(hook_input.get("sessionId"))
        hook_event_name = _safe_text(hook_input.get("hookEventName") or ((hook_input.get("hookSpecificOutput") or {}).get("hookEventName") if isinstance(hook_input.get("hookSpecificOutput"), dict) else ""))
        transcript_source = {"hook_input": hook_input, "transcript": transcript}
        candidates = _extract_candidates(transcript_source, session_id=session_id)
        _append_debug_log(
            hook_input,
            {
                "phase": "parsed",
                "hook_event_name": hook_event_name,
                "ticket_file": ticket_file.as_posix(),
                "session_id": session_id,
                "hook_input_keys": sorted(str(key) for key in hook_input.keys()),
                "transcript_type": type(transcript).__name__,
                "transcript_message_count": _count_role_messages(transcript),
                "candidates_count": len(candidates),
                "candidate_categories": [_safe_text(item.get("category")) for item in candidates],
                "candidate_titles": [_safe_text(item.get("title")) for item in candidates],
                "hook_input": hook_input,
                "transcript": transcript,
            },
        )
        if not candidates:
            outbox_payload = _sync_outbox_fallback(ticket_file, hook_input)
            if int(outbox_payload.get("outbox_consumed") or 0) > 0:
                _json_output(outbox_payload)
                return 0
            # Run workspace-storage backfill when:
            #   a) the hook event is a recognised stop-type event, OR
            #   b) the hook input was empty / minimal (raw_hook_input == "{}" or len <= 4).
            #      This happens when Copilot/Claude Code sends the hook without a transcript.
            input_was_empty = len(raw_hook_input) <= 4
            recognised_event = _safe_text(hook_event_name).lower() in {
                "stop", "subagentstop", "precompact", "userpromptsubmit"
            }
            if recognised_event or input_was_empty:
                fallback_payload = _sync_workspace_storage_backfill(ticket_file, hook_input)
                _json_output(fallback_payload)
                return 0
            _json_output({"continue": True})
            return 0

        recent = _recent_tickets()
        created, updated, recent = _sync_candidates(candidates, recent)

        payload: dict[str, Any] = {"continue": True}
        summary = _summarize(created, updated, ticket_file)
        if summary:
            payload["systemMessage"] = summary
        _append_debug_log(
            hook_input,
            {
                "phase": "synced",
                "hook_event_name": hook_event_name,
                "ticket_file": ticket_file.as_posix(),
                "session_id": session_id,
                "created": created,
                "updated": updated,
                "summary": summary,
            },
        )
        _json_output(payload)
        return 0
    except Exception as exc:
        _append_debug_log(
            hook_input,
            {
                "phase": "error",
                "error": repr(exc),
                "hook_input_keys": sorted(str(key) for key in hook_input.keys()),
                "hook_input": hook_input,
            },
        )
        _json_output({
            "continue": True,
            "systemMessage": f"Bug ticket sync warning: {exc}",
        })
        return 0


if __name__ == "__main__":
    raise SystemExit(main())