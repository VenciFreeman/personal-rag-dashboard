from __future__ import annotations

import html
import json
import os
import re
import threading
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from nav_dashboard.web.services.runtime_paths import (
    DEBUG_DIR,
    LEGACY_APPDATA_NAV_DASHBOARD_DATA_DIR,
    LEGACY_NAV_DASHBOARD_DATA_DIR,
    MEMORY_DIR,
    SESSIONS_DIR,
)

try:
    from core_service import get_settings
    from core_service.llm import chat_completion_with_retry
except Exception:  # noqa: BLE001
    get_settings = None
    chat_completion_with_retry = None


SESSION_FILE_PREFIX = "session_"
MEMORY_MAX_TURNS = 3
_LOCK = threading.RLock()
_DEFAULT_GREETING = "你好，我可以帮你查询本地文档、媒体记录，并结合需要调用扩展工具来补充回答。"


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _clip_text(value: Any, max_chars: int) -> str:
    text = str(value or "").strip()
    if max_chars <= 0 or len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip()


def _load_optional_core_settings() -> Any:
    if get_settings is None:
        return None
    try:
        return get_settings()
    except Exception:  # noqa: BLE001
        return None


_CORE_SETTINGS = _load_optional_core_settings()


def _first_configured_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _session_file_path(session_id: str) -> Path:
    return SESSIONS_DIR / f"{SESSION_FILE_PREFIX}{session_id}.json"


def _memory_file_path(session_id: str) -> Path:
    return MEMORY_DIR / f"memory_{session_id}.json"


def _legacy_session_file_paths(session_id: str) -> list[Path]:
    sid = str(session_id or "").strip()
    if not sid:
        return []
    roots = [
        LEGACY_APPDATA_NAV_DASHBOARD_DATA_DIR / "agent_sessions",
        LEGACY_NAV_DASHBOARD_DATA_DIR / "agent_sessions",
    ]
    paths: list[Path] = []
    for root in roots:
        candidate = root / f"{SESSION_FILE_PREFIX}{sid}.json"
        if candidate not in paths:
            paths.append(candidate)
    return paths


def _legacy_memory_file_paths(session_id: str) -> list[Path]:
    sid = str(session_id or "").strip()
    if not sid:
        return []
    roots = [
        LEGACY_APPDATA_NAV_DASHBOARD_DATA_DIR / "agent_sessions" / "_memory",
        LEGACY_NAV_DASHBOARD_DATA_DIR / "agent_sessions" / "_memory",
    ]
    paths: list[Path] = []
    for root in roots:
        candidate = root / f"memory_{sid}.json"
        if candidate not in paths:
            paths.append(candidate)
    return paths


def derive_session_title(question: str, max_len: int | None = None) -> str:
    text = re.sub(r"\s+", " ", str(question or "").strip())
    if not text:
        return "新会话"
    text = text.strip("，。！？!?;；:：")
    if max_len is None or max_len <= 0 or len(text) <= max_len:
        return text
    return text[:max_len].rstrip() + "..."


def _sanitize_session_title(title: str, max_len: int | None = None) -> str:
    text = re.sub(r"\s+", " ", str(title or "").strip())
    text = text.strip("，。！？!?;；:：")
    if not text:
        return ""
    if max_len is None or max_len <= 0 or len(text) <= max_len:
        return text
    return text[:max_len].rstrip() + "..."


def _normalize_title_compare_key(text: str) -> str:
    return re.sub(r"[^\w\u4e00-\u9fff]+", "", str(text or "").strip().lower())


def _looks_like_weak_session_title(title: str, question: str) -> bool:
    normalized_title = _normalize_title_compare_key(title)
    normalized_question = _normalize_title_compare_key(question)
    if not normalized_title:
        return True
    if normalized_question.startswith(normalized_title):
        return True
    weak_prefixes = (
        "请",
        "帮我",
        "请帮我",
        "比较",
        "请比较",
        "分析",
        "请分析",
        "解释",
        "请解释",
        "说明",
        "请说明",
    )
    return any(str(title or "").strip().startswith(prefix) for prefix in weak_prefixes)


def _build_answer_anchor_title(question: str, answer: str, max_len: int | None = None) -> str:
    lines = [re.sub(r"^#{1,6}\s*", "", line).strip() for line in str(answer or "").splitlines()]
    headings = [
        line
        for line in lines
        if line and line not in {"参考资料", "结论", "关键要点", "总结", "背景与关键机制", "典型使用场景及角色"}
    ]
    for heading in headings:
        candidate = _sanitize_session_title(heading, max_len=max_len)
        if candidate and not _looks_like_weak_session_title(candidate, question):
            return candidate
    return ""


def _generate_local_session_title(question: str, answer: str, max_len: int | None = None) -> str:
    title = ""
    local_url = _first_configured_text(
        os.getenv("NAV_DASHBOARD_LOCAL_LLM_URL", ""),
        os.getenv("AI_SUMMARY_LOCAL_LLM_URL", ""),
        getattr(_CORE_SETTINGS, "local_llm_url", ""),
    )
    local_model = _first_configured_text(
        os.getenv("NAV_DASHBOARD_LOCAL_LLM_MODEL", ""),
        os.getenv("AI_SUMMARY_LOCAL_LLM_MODEL", ""),
        getattr(_CORE_SETTINGS, "local_llm_model", ""),
    )
    local_key = (os.getenv("NAV_DASHBOARD_LOCAL_LLM_API_KEY", "") or os.getenv("AI_SUMMARY_LOCAL_LLM_API_KEY", "local")).strip() or "local"
    if local_url and local_model and chat_completion_with_retry is not None:
        try:
            title = chat_completion_with_retry(
                api_key=local_key,
                base_url=local_url,
                model=local_model,
                timeout=20,
                temperature=0.2,
                max_retries=1,
                retry_delay=1.0,
                messages=[
                    {
                        "role": "system",
                        "content": "你负责为中文问答会话生成标题。请输出一个简洁、可读、具体的中文标题，长度尽量控制在 8-18 个字，不要带引号、序号或句末标点。只输出标题本身。",
                    },
                    {
                        "role": "user",
                        "content": f"用户问题：{question}\n\n回答摘要：{_clip_text(answer, 800)}\n\n请生成标题。",
                    },
                ],
            )
        except Exception:  # noqa: BLE001
            title = ""
    normalized = _sanitize_session_title(title, max_len=max_len)
    if normalized and not _looks_like_weak_session_title(normalized, question):
        return normalized
    answer_anchor = _build_answer_anchor_title(question, answer, max_len=max_len)
    if answer_anchor:
        return answer_anchor
    fallback = _sanitize_session_title(question, max_len=max_len)
    return fallback or "新会话"


def schedule_generated_session_title(session_id: str, question: str, answer: str, *, lock: bool = True) -> None:
    sid = str(session_id or "").strip()
    if not sid or not str(answer or "").strip():
        return

    def _run() -> None:
        try:
            session = get_session(sid)
            if not session or bool(session.get("title_locked", False)):
                return
            title = _generate_local_session_title(question, answer)
            if not title:
                return
            set_session_title(sid, title, lock=lock)
        except Exception:  # noqa: BLE001
            return

    threading.Thread(target=_run, daemon=True, name=f"agent-title-{sid[:8]}").start()


def _normalize_session(raw: object) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    sid = str(raw.get("id", "")).strip()
    if not sid:
        return None
    title = str(raw.get("title", "新会话")).strip() or "新会话"
    created_at = str(raw.get("created_at", "")).strip() or _now_iso()
    updated_at = str(raw.get("updated_at", "")).strip() or created_at
    title_locked = bool(raw.get("title_locked", False))
    msgs_raw = raw.get("messages", [])
    messages: list[dict[str, Any]] = []
    if isinstance(msgs_raw, list):
        for item in msgs_raw:
            if not isinstance(item, dict):
                continue
            role = str(item.get("role", "")).strip()
            text = str(item.get("text", "")).strip()
            if not role or not text:
                continue
            trace_id = str(item.get("trace_id", "")).strip()
            normalized_message: dict[str, Any] = {"role": role, "text": text}
            if trace_id:
                normalized_message["trace_id"] = trace_id
            messages.append(normalized_message)
    return {
        "id": sid,
        "title": _sanitize_session_title(title) or "新会话",
        "created_at": created_at,
        "updated_at": updated_at,
        "title_locked": title_locked,
        "messages": messages,
    }


def _load_session_file(path: Path) -> dict[str, Any] | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return None
    return _normalize_session(raw)


def save_session(session: dict[str, Any]) -> None:
    normalized = _normalize_session(session)
    if normalized is None:
        return
    SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    _session_file_path(str(normalized["id"])).write_text(
        json.dumps(normalized, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _load_memory(session_id: str) -> dict[str, Any]:
    path = _memory_file_path(session_id)
    if not path.exists():
        return {
            "version": "v1",
            "session_id": session_id,
            "session_goal": "",
            "recent_turns": [],
            "updated_at": _now_iso(),
        }
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {
            "version": "v1",
            "session_id": session_id,
            "session_goal": "",
            "recent_turns": [],
            "updated_at": _now_iso(),
        }
    if not isinstance(raw, dict):
        raw = {}
    turns = raw.get("recent_turns", []) if isinstance(raw.get("recent_turns"), list) else []
    return {
        "version": "v1",
        "session_id": session_id,
        "session_goal": str(raw.get("session_goal", "")).strip(),
        "recent_turns": turns[-MEMORY_MAX_TURNS:],
        "updated_at": str(raw.get("updated_at", "")).strip() or _now_iso(),
    }


def _save_memory(session_id: str, memory: dict[str, Any]) -> None:
    MEMORY_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": "v1",
        "session_id": session_id,
        "session_goal": str(memory.get("session_goal", "")).strip(),
        "recent_turns": (memory.get("recent_turns", []) if isinstance(memory.get("recent_turns"), list) else [])[-MEMORY_MAX_TURNS:],
        "updated_at": _now_iso(),
    }
    _memory_file_path(session_id).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def list_sessions() -> list[dict[str, Any]]:
    SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    sessions: list[dict[str, Any]] = []
    with _LOCK:
        for path in sorted(SESSIONS_DIR.glob(f"{SESSION_FILE_PREFIX}*.json"), key=lambda item: item.name.lower()):
            data = _load_session_file(path)
            if data is not None:
                sessions.append(data)
    return sorted(sessions, key=lambda session: str(session.get("updated_at", "")), reverse=True)


def list_session_summaries(max_sessions: int = 30) -> list[dict[str, str]]:
    if max_sessions <= 0:
        return []
    if not SESSIONS_DIR.exists():
        return []
    rows: list[dict[str, str]] = []
    with _LOCK:
        for path in SESSIONS_DIR.glob(f"{SESSION_FILE_PREFIX}*.json"):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:  # noqa: BLE001
                continue
            if not isinstance(payload, dict):
                continue
            session_id = str(payload.get("id") or "").strip()
            if not session_id:
                continue
            rows.append(
                {
                    "id": session_id,
                    "title": str(payload.get("title") or "新会话"),
                    "updated_at": str(payload.get("updated_at") or ""),
                }
            )
    rows.sort(key=lambda session: session["updated_at"], reverse=True)
    return rows[:max_sessions]


def create_session(title: str = "新会话") -> dict[str, Any]:
    with _LOCK:
        now = _now_iso()
        session = {
            "id": str(uuid4()),
            "title": _sanitize_session_title(title) or "新会话",
            "created_at": now,
            "updated_at": now,
            "title_locked": False,
            "messages": [{"role": "system", "text": _DEFAULT_GREETING}],
        }
        save_session(session)
    return session


def set_session_title(session_id: str, title: str, lock: bool = True) -> dict[str, Any] | None:
    sid = str(session_id or "").strip()
    if not sid:
        return None
    new_title = _sanitize_session_title(title)
    if not new_title:
        return None
    with _LOCK:
        path = _session_file_path(sid)
        if not path.exists():
            return None
        session = _load_session_file(path)
        if session is None:
            return None
        session["title"] = new_title
        session["title_locked"] = bool(lock)
        session["updated_at"] = _now_iso()
        save_session(session)
        return session


def get_session(session_id: str) -> dict[str, Any] | None:
    sid = str(session_id or "").strip()
    if not sid:
        return None
    path = _session_file_path(sid)
    if not path.exists():
        return None
    with _LOCK:
        return _load_session_file(path)


def delete_session(session_id: str) -> bool:
    sid = str(session_id or "").strip()
    if not sid:
        return False
    with _LOCK:
        path = _session_file_path(sid)
        if not path.exists():
            return False
        try:
            path.unlink(missing_ok=True)
            _memory_file_path(sid).unlink(missing_ok=True)
            for legacy_path in _legacy_session_file_paths(sid):
                legacy_path.unlink(missing_ok=True)
            for legacy_memory_path in _legacy_memory_file_paths(sid):
                legacy_memory_path.unlink(missing_ok=True)
        except Exception:  # noqa: BLE001
            return False
        return True


def append_message(session_id: str, role: str, text: str, trace_id: str = "") -> None:
    sid = str(session_id or "").strip()
    if not sid:
        return
    with _LOCK:
        path = _session_file_path(sid)
        session = _load_session_file(path) if path.exists() else None
        if session is None:
            now = _now_iso()
            session = {
                "id": sid,
                "title": "新会话",
                "created_at": now,
                "updated_at": now,
                "messages": [],
            }
        message: dict[str, Any] = {"role": role, "text": text}
        normalized_trace_id = str(trace_id or "").strip()
        if normalized_trace_id:
            message["trace_id"] = normalized_trace_id
        session.setdefault("messages", []).append(message)
        session["updated_at"] = _now_iso()
        save_session(session)


def update_memory_for_session(session_id: str) -> None:
    session = get_session(session_id)
    if not session:
        return
    messages = session.get("messages", []) if isinstance(session.get("messages"), list) else []
    user_messages = [message for message in messages if str(message.get("role", "")).lower() == "user"]
    goal = str(user_messages[0].get("text", "")).strip()[:80] if user_messages else ""
    turns: list[dict[str, str]] = []
    for message in messages[-(MEMORY_MAX_TURNS * 2):]:
        role = str(message.get("role", "")).strip()
        text = str(message.get("text", "")).strip()
        if not role or not text:
            continue
        turns.append({"role": role, "text": text[:280]})
    memory = _load_memory(session_id)
    memory["session_goal"] = goal
    memory["recent_turns"] = turns[-MEMORY_MAX_TURNS:]
    _save_memory(session_id, memory)


def build_memory_context(session_id: str) -> str:
    sid = str(session_id or "").strip()
    if not sid:
        return ""
    memory = _load_memory(sid)
    goal = str(memory.get("session_goal", "")).strip()
    turns = memory.get("recent_turns", []) if isinstance(memory.get("recent_turns"), list) else []
    if not goal and not turns:
        return ""
    lines = ["[Memory]"]
    if goal:
        lines.append(f"- SessionGoal: {goal}")
    if turns:
        lines.append("- RecentTurns:")
        for item in turns[-MEMORY_MAX_TURNS:]:
            if not isinstance(item, dict):
                continue
            role = str(item.get("role", "")).strip()
            text = str(item.get("text", "")).strip()
            if role and text:
                lines.append(f"  - {role}: {text}")
    return "\n".join(lines).strip()


def session_activity_counts() -> tuple[int, int]:
    files = [path for path in SESSIONS_DIR.glob(f"{SESSION_FILE_PREFIX}*.json") if path.is_file()] if SESSIONS_DIR.exists() else []
    total_messages = 0
    active_sessions = 0
    with _LOCK:
        for path in files:
            payload = _load_session_file(path)
            messages = payload.get("messages") if isinstance(payload, dict) else []
            if not isinstance(messages, list):
                continue
            has_user = any(
                str(message.get("role", "")).lower() == "user"
                for message in messages
                if isinstance(message, dict)
            )
            if not has_user:
                continue
            active_sessions += 1
            first_user_idx = next(
                (
                    index
                    for index, message in enumerate(messages)
                    if isinstance(message, dict) and str(message.get("role", "")).lower() == "user"
                ),
                None,
            )
            if first_user_idx is None:
                continue
            total_messages += sum(
                1
                for message in messages[first_user_idx:]
                if isinstance(message, dict) and str(message.get("role", "")).lower() in {"user", "assistant"}
            )
    return active_sessions, total_messages


def render_session_list_items_html(max_sessions: int = 30) -> str:
    rows = list_session_summaries(max_sessions=max_sessions)
    if not rows:
        return ""
    parts = [
        f'<li data-session-id="{html.escape(str(row["id"]))}" title="{html.escape(str(row["title"]))}">'
        f'<div class="title">{html.escape(str(row["title"]))}</div>'
        f'<div class="meta">{html.escape(str(row["updated_at"]))}</div>'
        f"</li>"
        for row in rows
    ]
    return "\n".join(parts)


def sessions_json_payload(max_sessions: int = 30) -> str:
    return json.dumps(list_sessions()[:max_sessions], ensure_ascii=False)