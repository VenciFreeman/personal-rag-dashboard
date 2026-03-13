"""RAG session persistence and title utilities for local GUI.

This module keeps session-related text/file transformations isolated from Tk UI.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any


def sanitize_session_title(title: str) -> str:
    clean = re.sub(r"\s+", " ", (title or "").strip())
    if not clean:
        return "未命名会话"
    return clean[:30]


def derive_local_session_title(question: str, answer: str, max_len: int = 15) -> str:
    for raw_line in (answer or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("```"):
            continue
        line = re.sub(r"^#{1,6}\s*", "", line)
        line = re.sub(r"^\d+\.\s*", "", line)
        line = re.sub(r"^[-*+]\s*", "", line)
        line = re.sub(r"\[资料\s*\d+\]|\[\d+\]", "", line)
        line = line.strip(" :：-\t")
        if line:
            return sanitize_session_title(line)[:max_len]

    fallback = re.sub(r"[？?。！!]+$", "", (question or "").strip())
    if not fallback:
        fallback = "未命名会话"
    return sanitize_session_title(fallback)[:max_len]


def sanitize_filename_part(text: str) -> str:
    value = re.sub(r"[\\/:*?\"<>|]+", "_", text.strip())
    value = re.sub(r"\s+", "_", value)
    value = value.strip("._")
    if not value:
        return "untitled"
    return value[:64]


def session_file_name(session: dict[str, Any]) -> str:
    created_at = str(session.get("created_at", "")).strip() or datetime.now().strftime("%Y%m%d_%H%M%S")
    title = sanitize_filename_part(str(session.get("title", "未命名会话")))
    return f"{created_at}_{title}.md"


def build_rag_session_markdown(session: dict[str, Any]) -> str:
    title = sanitize_session_title(str(session.get("title", "未命名会话")))
    created_at = str(session.get("created_at", "")).strip() or datetime.now().strftime("%Y%m%d_%H%M%S")
    lines = [
        "# RAG 会话记录",
        f"> title: {title}",
        f"> created_at: {created_at}",
        "---",
    ]
    messages = session.get("messages", [])
    if isinstance(messages, list):
        for role, content in messages:
            role_text = str(role).strip() or "助手"
            lines.append(f"### {role_text}")
            text = str(content).strip()
            if text:
                lines.append(text)
            lines.append("---")
    return "\n\n".join(lines).strip() + "\n"


def parse_rag_session_markdown(text: str, fallback_system_message: str) -> tuple[str, str, list[tuple[str, str]]]:
    title = "未命名会话"
    created_at = datetime.now().strftime("%Y%m%d_%H%M%S")
    messages: list[tuple[str, str]] = []

    title_match = re.search(r"^>\s*title:\s*(.+)$", text, flags=re.MULTILINE)
    if title_match:
        title = sanitize_session_title(title_match.group(1))

    created_match = re.search(r"^>\s*created_at:\s*(.+)$", text, flags=re.MULTILINE)
    if created_match:
        created_at = created_match.group(1).strip()

    current_role: str | None = None
    buffer: list[str] = []
    for raw in text.splitlines():
        role_match = re.match(r"^###\s+(系统|用户|助手)\s*$", raw.strip())
        if role_match:
            if current_role is not None:
                messages.append((current_role, "\n".join(buffer).strip()))
            current_role = role_match.group(1)
            buffer = []
            continue
        if re.match(r"^\s*([-*_])\1{2,}\s*$", raw):
            continue
        if current_role is not None:
            buffer.append(raw)

    if current_role is not None:
        messages.append((current_role, "\n".join(buffer).strip()))

    messages = [(r, c) for r, c in messages if r and c]
    if not messages:
        messages = [("系统", fallback_system_message)]

    return title, created_at, messages
