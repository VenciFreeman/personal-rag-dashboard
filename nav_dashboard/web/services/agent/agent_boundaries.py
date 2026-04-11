from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from dataclasses import dataclass, field
from urllib import parse as urlparse

from ai_conversations_summary import runtime_paths as ai_summary_runtime_paths
from nav_dashboard.web.clients.internal_services import (
    InternalServiceError,
    ai_summary_internal_base_url,
    library_tracker_internal_base_url,
    request_json,
)


_WORKSPACE_ROOT = Path(__file__).resolve().parents[4]
_AI_SUMMARY_ROOT = _WORKSPACE_ROOT / "ai_conversations_summary"
_LIBRARY_TRACKER_ROOT = _WORKSPACE_ROOT / "library_tracker"

_DOC_GRAPH_SCRIPT = _AI_SUMMARY_ROOT / "scripts" / "rag_knowledge_graph.py"
_MEDIA_GRAPH_SCRIPT = _LIBRARY_TRACKER_ROOT / "scripts" / "expand_library_query.py"
_DOC_GRAPH_DIR = ai_summary_runtime_paths.VECTOR_DB_DIR
_MEDIA_GRAPH_DIR = _LIBRARY_TRACKER_ROOT / "data" / "vector_db"

_CACHE_DIR = ai_summary_runtime_paths.CACHE_DIR
_WEB_CACHE_PATH = _CACHE_DIR / "web_cache.db"
_NO_CONTEXT_LOG_PATH = _CACHE_DIR / "no_context_queries.jsonl"
_WEB_CACHE_TTL_SECONDS = 7 * 86_400
_NO_CONTEXT_LOCK = threading.Lock()
_AI_SUMMARY_BASE = ai_summary_internal_base_url().rstrip("/")
_LIBRARY_TRACKER_BASE = library_tracker_internal_base_url().rstrip("/")


BOUNDARY_TOOL_DESCRIPTIONS = {
    "expand_document_query": "使用知识图谱扩展文档查询（可获取相关概念）",
    "expand_media_query": "使用知识图谱扩展媒体查询",
}


@dataclass
class BoundaryCallResult:
    available: bool
    status: str
    summary: str
    data: dict[str, Any] = field(default_factory=dict)


def has_doc_graph_adapter() -> bool:
    return _DOC_GRAPH_SCRIPT.exists() and _DOC_GRAPH_DIR.exists()


def has_media_graph_adapter() -> bool:
    return _MEDIA_GRAPH_SCRIPT.exists() and _MEDIA_GRAPH_DIR.exists()


def get_available_boundary_tools() -> list[str]:
    tools: list[str] = []
    if has_doc_graph_adapter():
        tools.append("expand_document_query")
    if has_media_graph_adapter():
        tools.append("expand_media_query")
    return tools


def no_context_log_path() -> Path:
    return _NO_CONTEXT_LOG_PATH


def get_boundary_tool_prompt_lines() -> list[str]:
    return [f"- {name}: {BOUNDARY_TOOL_DESCRIPTIONS[name]}" for name in get_available_boundary_tools() if name in BOUNDARY_TOOL_DESCRIPTIONS]


def _run_json_subprocess(command: list[str], *, cwd: Path, timeout: int = 20) -> dict[str, Any]:
    completed = subprocess.run(
        command,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        check=False,
    )
    stdout = str(completed.stdout or "").strip()
    stderr = str(completed.stderr or "").strip()
    if completed.returncode != 0:
        message = stderr or stdout or f"subprocess exited with code {completed.returncode}"
        raise RuntimeError(message)
    if not stdout:
        return {}
    try:
        return json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid JSON from subprocess: {stdout[:240]}") from exc


def _http_json(method: str, url: str, *, payload: dict[str, Any] | None = None, timeout: float = 5.0) -> Any:
    try:
        return request_json(method, url, payload=payload, timeout=timeout, raise_for_status=True)
    except InternalServiceError as exc:
        raise RuntimeError(exc.detail) from exc


def expand_doc_query(query: str) -> dict[str, Any]:
    if not has_doc_graph_adapter():
        raise RuntimeError("document graph adapter unavailable")
    return _run_json_subprocess(
        [
            sys.executable,
            str(_DOC_GRAPH_SCRIPT),
            "--index-dir",
            str(_DOC_GRAPH_DIR),
            "--query",
            str(query or ""),
        ],
        cwd=_AI_SUMMARY_ROOT,
    )


def _expand_doc_query_http(query: str) -> dict[str, Any]:
    payload = _http_json(
        "POST",
        f"{_AI_SUMMARY_BASE}/api/agent-boundary/doc-graph/expand",
        payload={"query": str(query or "")},
        timeout=8.0,
    )
    return dict(payload) if isinstance(payload, dict) else {}


def expand_media_query(query: str) -> dict[str, Any]:
    if not has_media_graph_adapter():
        raise RuntimeError("media graph adapter unavailable")
    return _run_json_subprocess(
        [
            sys.executable,
            str(_MEDIA_GRAPH_SCRIPT),
            "--graph-dir",
            str(_MEDIA_GRAPH_DIR),
            "--query",
            str(query or ""),
        ],
        cwd=_LIBRARY_TRACKER_ROOT,
    )


def _expand_media_query_http(query: str) -> dict[str, Any]:
    payload = _http_json(
        "POST",
        f"{_LIBRARY_TRACKER_BASE}/api/library/agent-boundary/graph/expand",
        payload={"query": str(query or "")},
        timeout=8.0,
    )
    return dict(payload) if isinstance(payload, dict) else {}


def run_doc_graph_expand(query: str) -> BoundaryCallResult:
    if not has_doc_graph_adapter():
        return BoundaryCallResult(
            available=False,
            status="unavailable",
            summary="文档知识图谱不可用",
            data={"original": query, "expanded": query},
        )
    try:
        try:
            expansion = _expand_doc_query_http(query)
            channel = "http"
        except Exception:
            expansion = expand_doc_query(query)
            channel = "local"
        expanded_query = str(expansion.get("expanded_query") or query).strip() or query
        seed_concepts = expansion.get("seed_concepts") or []
        expanded_concepts = expansion.get("expanded_concepts") or []
        return BoundaryCallResult(
            available=True,
            status="ok",
            summary=f"查询扩展完成（通道: {channel}, 种子概念: {len(seed_concepts)}, 扩展概念: {len(expanded_concepts)}）",
            data={
                "original": query,
                "expanded": expanded_query,
                "seed_concepts": seed_concepts,
                "expanded_concepts": expanded_concepts,
            },
        )
    except Exception as exc:
        return BoundaryCallResult(
            available=True,
            status="error",
            summary=f"图谱扩展失败: {str(exc)}",
            data={"original": query, "expanded": query},
        )


def run_media_graph_expand(query: str) -> BoundaryCallResult:
    if not has_media_graph_adapter():
        return BoundaryCallResult(
            available=False,
            status="unavailable",
            summary="媒体知识图谱不可用",
            data={"original": query, "expanded": query, "constraints": {}},
        )
    try:
        try:
            expansion = _expand_media_query_http(query)
            channel = "http"
        except Exception:
            expansion = expand_media_query(query)
            channel = "local"
        expanded_query = str(expansion.get("expanded_query") or query).strip() or query
        expanded_concepts = expansion.get("expanded_concepts") or []
        constraints = expansion.get("constraints") or {}
        return BoundaryCallResult(
            available=True,
            status="ok",
            summary=f"查询扩展完成（通道: {channel}, 扩展概念: {len(expanded_concepts)}, 约束字段: {len(constraints)}）",
            data={
                "original": query,
                "expanded": expanded_query,
                "expanded_concepts": expanded_concepts,
                "constraints": constraints,
            },
        )
    except Exception as exc:
        return BoundaryCallResult(
            available=True,
            status="error",
            summary=f"图谱扩展失败: {str(exc)}",
            data={"original": query, "expanded": query, "constraints": {}},
        )


def _ensure_cache_dir() -> None:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _connect_web_cache() -> sqlite3.Connection:
    _ensure_cache_dir()
    conn = sqlite3.connect(str(_WEB_CACHE_PATH), timeout=10, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS web_cache (
            cache_key    TEXT PRIMARY KEY,
            query        TEXT NOT NULL,
            results_json TEXT NOT NULL,
            created_at   INTEGER NOT NULL
        )"""
    )
    return conn


def _web_cache_key(query: str, max_results: int) -> str:
    return hashlib.sha256(f"{int(max_results)}::{str(query or '')}".encode("utf-8")).hexdigest()


def get_cached_web_results(query: str, max_results: int) -> list[dict[str, Any]] | None:
    try:
        qs = urlparse.urlencode({"query": str(query or ""), "max_results": int(max_results)})
        payload = _http_json("GET", f"{_AI_SUMMARY_BASE}/api/agent-boundary/web-cache?{qs}", timeout=5.0)
        if isinstance(payload, dict) and bool(payload.get("hit")):
            rows = payload.get("results") if isinstance(payload.get("results"), list) else []
            return [item for item in rows if isinstance(item, dict)]
    except Exception:
        pass
    cache_key = _web_cache_key(query, max_results)
    cutoff = int(time.time()) - _WEB_CACHE_TTL_SECONDS
    try:
        with _connect_web_cache() as conn:
            row = conn.execute(
                "SELECT results_json FROM web_cache WHERE cache_key = ? AND created_at > ?",
                (cache_key, cutoff),
            ).fetchone()
    except Exception:
        return None
    if row is None:
        return None
    try:
        payload = json.loads(str(row[0] or "[]"))
    except Exception:
        return None
    return [item for item in payload if isinstance(item, dict)]


def set_cached_web_results(query: str, max_results: int, results: list[dict[str, Any]]) -> None:
    cache_key = _web_cache_key(query, max_results)
    clean_results = [dict(item) for item in results if isinstance(item, dict)]
    try:
        _http_json(
            "POST",
            f"{_AI_SUMMARY_BASE}/api/agent-boundary/web-cache",
            payload={"query": str(query or ""), "max_results": int(max_results), "results": clean_results},
            timeout=5.0,
        )
        return
    except Exception:
        pass
    try:
        with _connect_web_cache() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO web_cache (cache_key, query, results_json, created_at) VALUES (?, ?, ?, ?)",
                (cache_key, str(query or ""), json.dumps(clean_results, ensure_ascii=False), int(time.time())),
            )
    except Exception:
        return


def log_no_context_query(
    query: str,
    *,
    source: str,
    top1_score: float | None = None,
    threshold: float | None = None,
    trace_id: str = "",
    reason: str = "",
) -> None:
    try:
        _http_json(
            "POST",
            f"{_AI_SUMMARY_BASE}/api/agent-boundary/no-context-log",
            payload={
                "query": str(query or ""),
                "source": str(source or "unknown"),
                "top1_score": round(float(top1_score), 4) if top1_score is not None else None,
                "threshold": round(float(threshold), 4) if threshold is not None else None,
                "trace_id": str(trace_id or "").strip(),
                "reason": str(reason or "").strip(),
            },
            timeout=5.0,
        )
        return
    except Exception:
        pass
    record: dict[str, Any] = {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "source": str(source or "unknown"),
        "query": str(query or ""),
        "top1_score": round(float(top1_score), 4) if top1_score is not None else None,
        "threshold": round(float(threshold), 4) if threshold is not None else None,
    }
    if str(trace_id or "").strip():
        record["trace_id"] = str(trace_id).strip()
    if str(reason or "").strip():
        record["reason"] = str(reason).strip()
    try:
        _ensure_cache_dir()
        with _NO_CONTEXT_LOCK:
            with _NO_CONTEXT_LOG_PATH.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception:
        return