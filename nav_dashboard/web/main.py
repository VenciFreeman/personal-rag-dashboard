"""nav_dashboard/web/main.py
FastAPI 主应用入口 — Nav Dashboard（端口 8092）

职责：
  - 提供 HTML 页面渲染（Jinja2 模板）
  - 挂载 /static 静态文件
  - 聚合 agent_router（/api/agent）和 benchmark_router（/api/benchmark）
  - 实现 Dashboard 统计总览接口（/api/dashboard/overview）：
      · RAG 索引文档数、待重建文档数
      · RAG 知识图谱节点数/边数（读 knowledge_graph_rag.json）
      · Library 条目数、今年条目数、Library 知识图谱节点数/边数
      · 本月及今日 API 用量（Web Search / DeepSeek）
      · Agent 会话数/消息数、RAG Q&A 会话数/消息数
      · RAG System 启动状态
      · 向量检索时延分位统计
  - 快捷卡片增删改（/api/custom_cards/*）：持久化到 data/custom_cards.json
  - 图片上传接口（/api/custom_cards/upload）
  - 本月用量手动调整接口（PATCH /api/dashboard/usage）
  - Deploy 时间常量 _DEPLOY_TIME（进程启动时捕获，贯穿整个部署生命周期）
"""
from __future__ import annotations

import asyncio
import json
import math
import os
import re
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib import error as urlerror
from urllib import parse as urlparse
from urllib import request as urlrequest
from uuid import uuid4

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from web.api.agent import router as agent_router
from web.api.benchmark import router as benchmark_router
from web.config import AI_SUMMARY_URL_OVERRIDE, HOST, LIBRARY_TRACKER_URL_OVERRIDE, PORT
from web.services import agent_service

APP_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = APP_DIR.parent.parent
CUSTOM_CARDS_FILE = PROJECT_ROOT / "data" / "custom_cards.json"
CUSTOM_CARDS_MAX = 8
CUSTOM_CARD_UPLOAD_DIR = APP_DIR / "static" / "custom_cards"
NO_CONTEXT_LOG_PATH = PROJECT_ROOT / "ai_conversations_summary" / "data" / "cache" / "no_context_queries.jsonl"
ALLOWED_IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp", ".gif"}
CONTENT_TYPE_TO_EXT = {
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
}
templates = Jinja2Templates(directory=str(APP_DIR / "templates"))


class CustomCardPayload(BaseModel):
    title: str = ""
    url: str = ""
    image: str = ""


class UsageAdjustPayload(BaseModel):
    month_web_search_calls: int
    month_deepseek_calls: int


def _default_custom_cards() -> list[dict[str, str]]:
    cards = [
        {
            "title": "RAG System",
            "url": "http://127.0.0.1:8000/",
            "image": "",
        },
        {
            "title": "Library Tracker",
            "url": "http://127.0.0.1:8091/",
            "image": "",
        },
    ]
    while len(cards) < CUSTOM_CARDS_MAX:
        cards.append({"title": "", "url": "", "image": ""})
    return cards


def _normalize_card(item: object) -> dict[str, str]:
    if not isinstance(item, dict):
        return {"title": "", "url": "", "image": ""}

    title = str(item.get("title", "")).strip()
    url = str(item.get("url", "")).strip()
    image = str(item.get("image", "")).strip().replace("\\", "/")
    if image and not image.lower().startswith(("http://", "https://", "/")):
        image = "/static/" + image.lstrip("./")
    return {
        "title": title,
        "url": url,
        "image": image,
    }


def _save_custom_cards(cards: list[dict[str, str]]) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = [_normalize_card(item) for item in cards[:CUSTOM_CARDS_MAX]]
    while len(normalized) < CUSTOM_CARDS_MAX:
        normalized.append({"title": "", "url": "", "image": ""})

    CUSTOM_CARDS_FILE.parent.mkdir(parents=True, exist_ok=True)
    CUSTOM_CARDS_FILE.write_text(json.dumps(normalized, ensure_ascii=False, indent=2), encoding="utf-8")
    return normalized


def _load_custom_cards() -> list[dict[str, str]]:
    default_cards = _default_custom_cards()

    try:
        CUSTOM_CARDS_FILE.parent.mkdir(parents=True, exist_ok=True)
        if not CUSTOM_CARDS_FILE.exists():
            CUSTOM_CARDS_FILE.write_text(json.dumps(default_cards, ensure_ascii=False, indent=2), encoding="utf-8")
            return default_cards
        raw = json.loads(CUSTOM_CARDS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return default_cards

    if not isinstance(raw, list):
        return default_cards
    return _save_custom_cards([_normalize_card(item) for item in raw])


def _trigger_custom_card_compression() -> None:
    """Run custom card compression script asynchronously after save."""
    try:
        script_path = PROJECT_ROOT / "scripts" / "compress_custom_cards.py"
        if not script_path.exists():
            return
        subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            timeout=300,
            check=False,
        )
    except Exception:
        return

app = FastAPI(title="Nav Dashboard", version="0.1.0")

# Captured once at process start; stays constant until the next deployment.
_DEPLOY_TIME: str = datetime.now().isoformat(timespec="seconds")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=str(APP_DIR / "static")), name="static")
app.include_router(agent_router)
app.include_router(benchmark_router)


def _safe_load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _count_rag_index_docs() -> tuple[int, int, int]:
    # Respect AI_SUMMARY_VECTOR_DB_DIR env var (mirrors ai_conversations_summary/web/config.py logic)
    vector_db_env = (os.getenv("AI_SUMMARY_VECTOR_DB_DIR", "") or "").strip()
    if vector_db_env:
        vector_db_dir = Path(vector_db_env)
    else:
        vector_db_dir = PROJECT_ROOT / "core_service" / "data" / "vector_db"
    metadata_path = vector_db_dir / "metadata.json"
    payload = _safe_load_json(metadata_path, default=[])

    docs: list[dict[str, Any]] = []
    if isinstance(payload, list):
        docs = [row for row in payload if isinstance(row, dict)]
    elif isinstance(payload, dict):
        rows = payload.get("documents") if isinstance(payload.get("documents"), list) else []
        docs = [row for row in rows if isinstance(row, dict)]

    changed = sum(1 for row in docs if bool(row.get("changed", False)))
    docs_dir = PROJECT_ROOT / "ai_conversations_summary" / "documents"
    source_docs = len([p for p in docs_dir.rglob("*.md") if p.is_file() and p.name != ".gitkeep"]) if docs_dir.exists() else 0
    return len(docs), changed, source_docs


def _library_counts() -> tuple[int, dict[str, int], int, int, int, int]:
    structured_dir = PROJECT_ROOT / "library_tracker" / "data" / "structured"
    media_files = ["reading.json", "video.json", "music.json", "game.json"]
    by_media: dict[str, int] = {}
    total = 0
    this_year = 0
    current_year = str(datetime.now().year)
    for name in media_files:
        media_type = name.replace(".json", "")
        data = _safe_load_json(structured_dir / name, default=[])
        if isinstance(data, list):
            records = [r for r in data if isinstance(r, dict)]
        elif isinstance(data, dict):
            raw = data.get("records", [])
            records = [r for r in raw if isinstance(r, dict)] if isinstance(raw, list) else []
        else:
            records = []
        count = len(records)
        by_media[media_type] = count
        total += count
        for rec in records:
            d = str(rec.get("date", "") or "")
            if d.startswith(current_year):
                this_year += 1

    sqlite_rows = 0
    db_path = PROJECT_ROOT / "library_tracker" / "data" / "vector_db" / "library_embeddings.sqlite3"
    if db_path.exists():
        try:
            with sqlite3.connect(str(db_path)) as conn:
                row = conn.execute("SELECT COUNT(1) FROM item_embeddings").fetchone()
                sqlite_rows = int(row[0]) if row and row[0] is not None else 0
        except Exception:
            sqlite_rows = 0

    graph_nodes = 0
    graph_edges = 0
    graph_path = PROJECT_ROOT / "library_tracker" / "data" / "vector_db" / "library_knowledge_graph.json"
    graph_data = _safe_load_json(graph_path, default={})
    if isinstance(graph_data, dict):
        nodes = graph_data.get("nodes", {})
        edges = graph_data.get("edges", [])
        graph_nodes = len(nodes) if isinstance(nodes, dict) else 0
        graph_edges = len(edges) if isinstance(edges, list) else 0

    return total, by_media, sqlite_rows, graph_nodes, graph_edges, this_year


def _rag_graph_counts() -> tuple[int, int]:
    """Return (node_count, edge_count) from the RAG knowledge graph JSON."""
    graph_path = PROJECT_ROOT / "ai_conversations_summary" / "data" / "vector_db" / "knowledge_graph_rag.json"
    graph_data = _safe_load_json(graph_path, default={})
    if not isinstance(graph_data, dict):
        return 0, 0
    nodes = graph_data.get("nodes", {})
    edges = graph_data.get("edges", [])
    return (
        len(nodes) if isinstance(nodes, dict) else 0,
        len(edges) if isinstance(edges, list) else 0,
    )


def _load_monthly_quota_counts() -> tuple[int, int, dict[str, int]]:
    month_key = datetime.now().strftime("%Y-%m")
    history_path = APP_DIR.parent / "data" / "agent_quota_history.json"
    payload = _safe_load_json(history_path, default={})
    months = payload.get("months") if isinstance(payload, dict) and isinstance(payload.get("months"), dict) else {}
    row = months.get(month_key) if isinstance(months.get(month_key), dict) else {}
    web_month = int(row.get("web_search", 0) or 0)
    deepseek_month = int(row.get("deepseek", 0) or 0)

    daily = agent_service._load_quota_state()  # noqa: SLF001
    if web_month == 0 and deepseek_month == 0:
        # Backward compatibility: history file may not exist yet.
        web_month = int(daily.get("web_search", 0) or 0)
        deepseek_month = int(daily.get("deepseek", 0) or 0)

    return web_month, deepseek_month, {
        "today_web_search": int(daily.get("web_search", 0) or 0),
        "today_deepseek": int(daily.get("deepseek", 0) or 0),
        "daily_web_limit": int(agent_service.WEB_SEARCH_DAILY_LIMIT),
        "daily_deepseek_limit": int(agent_service.DEEPSEEK_DAILY_LIMIT),
    }


def _agent_session_counts() -> tuple[int, int]:
    sessions_dir = APP_DIR.parent / "data" / "agent_sessions"
    if not sessions_dir.exists():
        return 0, 0
    files = [p for p in sessions_dir.glob("session_*.json") if p.is_file()]
    total_messages = 0
    active_sessions = 0
    for path in files:
        payload = _safe_load_json(path, default={})
        messages = payload.get("messages") if isinstance(payload, dict) else []
        if not isinstance(messages, list):
            continue
        # Skip sessions that have no real user message; the initial "assistant" greeting
        # is injected on creation and should not count as an active session.
        has_user = any(
            str(m.get("role", "")).lower() == "user"
            for m in messages
            if isinstance(m, dict)
        )
        if not has_user:
            continue
        active_sessions += 1
        # Find first user message; skip the injected assistant greeting before it
        first_user_idx = next(
            (i for i, m in enumerate(messages)
             if isinstance(m, dict) and str(m.get("role", "")).lower() == "user"),
            None,
        )
        if first_user_idx is None:
            continue
        total_messages += sum(
            1 for m in messages[first_user_idx:]
            if isinstance(m, dict) and str(m.get("role", "")).lower() in {"user", "assistant"}
        )
    return active_sessions, total_messages


def _rag_qa_session_counts() -> tuple[int, int]:
    sessions_dir = PROJECT_ROOT / "ai_conversations_summary" / "data" / "rag_sessions"
    if not sessions_dir.exists():
        return 0, 0
    files = [p for p in sessions_dir.glob("session_*.json") if p.is_file()]
    total_messages = 0
    active_sessions = 0
    for path in files:
        payload = _safe_load_json(path, default={})
        messages = payload.get("messages") if isinstance(payload, dict) else []
        if not isinstance(messages, list):
            continue
        # RAG sessions always have a system welcome message injected on create;
        # only count sessions with at least one real user message.
        has_user = any(
            str(m.get("role", "")).lower() in {"用户", "user"}
            for m in messages
            if isinstance(m, dict)
        )
        if not has_user:
            continue
        active_sessions += 1
        # Count only user + assistant messages (exclude system overhead)
        total_messages += sum(
            1 for m in messages
            if isinstance(m, dict) and str(m.get("role", "")).lower() in {"用户", "user", "助手", "assistant"}
        )
    return active_sessions, total_messages


def _ai_summary_internal_base_url() -> str:
    raw = (os.getenv("NAV_DASHBOARD_AI_SUMMARY_INTERNAL_URL", "") or "").strip().rstrip("/")
    if raw:
        return raw
    if AI_SUMMARY_URL_OVERRIDE:
        parsed = urlparse.urlparse(AI_SUMMARY_URL_OVERRIDE)
        if parsed.scheme and parsed.hostname:
            port = parsed.port or 8000
            return f"{parsed.scheme}://{parsed.hostname}:{port}"
    return "http://127.0.0.1:8000"


def _http_json_get(url: str, timeout: float = 2.5) -> dict[str, Any]:
    req = urlrequest.Request(url=url, headers={"Accept": "application/json"}, method="GET")
    host = (urlparse.urlparse(url).hostname or "").lower()
    local_hosts = {"127.0.0.1", "localhost", "::1"}
    try:
        if host in local_hosts:
            opener = urlrequest.build_opener(urlrequest.ProxyHandler({}))
            with opener.open(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
        else:
            with urlrequest.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
        payload = json.loads(raw) if raw.strip() else {}
        return payload if isinstance(payload, dict) else {"value": payload}
    except urlerror.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else str(exc)
        return {"ok": False, "error": f"HTTP {exc.code}: {detail}"}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    pos = max(0.0, min(1.0, pct)) * (len(ordered) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(ordered) - 1)
    weight = pos - lo
    return float(ordered[lo] * (1.0 - weight) + ordered[hi] * weight)


def _timing_stats(values: list[float]) -> dict[str, float | int]:
    if not values:
        return {"count": 0, "avg": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0}
    cleaned = [max(0.0, float(v)) for v in values]
    avg = sum(cleaned) / max(1, len(cleaned))
    return {
        "count": len(cleaned),
        "avg": round(avg, 4),
        "p50": round(_percentile(cleaned, 0.50), 4),
        "p95": round(_percentile(cleaned, 0.95), 4),
        "p99": round(_percentile(cleaned, 0.99), 4),
    }


def _top_percent_mean(values: list[float], percent: float = 0.01) -> float | None:
    cleaned = [float(v) for v in values if isinstance(v, (int, float))]
    if not cleaned:
        return None
    cleaned.sort(reverse=True)
    take = max(1, int(math.ceil(len(cleaned) * max(0.0, min(1.0, float(percent))))))
    bucket = cleaned[:take]
    return round(sum(bucket) / len(bucket), 4) if bucket else None


def _safe_div(numerator: float | int, denominator: float | int) -> float | None:
    try:
        den = float(denominator)
        if den <= 0:
            return None
        return round(float(numerator) / den, 4)
    except Exception:
        return None


def _parse_iso_ts(value: str) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text).timestamp()
    except Exception:
        return None


def _load_missing_queries(days: int = 30, limit: int = 200, source: str = "") -> list[dict[str, Any]]:
    if not NO_CONTEXT_LOG_PATH.exists():
        return []
    now_ts = time.time()
    cutoff = now_ts - max(1, int(days)) * 86_400
    source_filter = str(source or "").strip().lower()
    out: list[dict[str, Any]] = []
    try:
        lines = NO_CONTEXT_LOG_PATH.read_text(encoding="utf-8").splitlines()
    except Exception:
        return []

    for line in lines:
        raw = str(line or "").strip()
        if not raw:
            continue
        try:
            row = json.loads(raw)
        except Exception:
            continue
        if not isinstance(row, dict):
            continue
        ts = str(row.get("ts", "")).strip()
        ts_epoch = _parse_iso_ts(ts)
        if ts_epoch is None or ts_epoch < cutoff:
            continue
        query = str(row.get("query", "")).strip()
        if not query:
            continue
        row_source = str(row.get("source", "unknown") or "unknown").strip()
        if source_filter and source_filter not in {"all", "*"} and row_source.lower() != source_filter:
            continue
        top1_score = row.get("top1_score")
        threshold = row.get("threshold")
        out.append(
            {
                "ts": ts,
                "source": row_source,
                "query": query,
                "top1_score": float(top1_score) if isinstance(top1_score, (int, float)) else None,
                "threshold": float(threshold) if isinstance(threshold, (int, float)) else None,
            }
        )

    out.sort(key=lambda x: str(x.get("ts", "")), reverse=True)
    return out[: max(1, int(limit))]


def _load_retrieval_latency_summary() -> dict[str, Any]:
    metrics_path = PROJECT_ROOT / "ai_conversations_summary" / "data" / "rag_sessions" / "retrieval_metrics.json"
    payload = _safe_load_json(metrics_path, default={})
    rows = payload.get("records") if isinstance(payload, dict) and isinstance(payload.get("records"), list) else []
    all_records = [row for row in rows if isinstance(row, dict)]
    # Exclude benchmark-tagged records from dashboard rolling stats
    records = [r for r in all_records if not str(r.get("source", "")).startswith("benchmark")][-20:]

    # Stage field names must match what record_retrieval_metrics() and app.js expect.
    # Records store flat fields (not nested under "timings").
    STAGE_KEYS = ("total", "rerank_seconds", "context_assembly_seconds", "web_search_seconds", "elapsed_seconds")
    # Also include legacy field names produced by older versions of the metrics code.
    LEGACY_ALIASES: dict[str, str] = {
        "vector_recall_seconds": "total",
        "context_build_seconds": "context_assembly_seconds",
    }

    stage_values: dict[str, list[float]] = {}
    compact_records: list[dict[str, Any]] = []
    for row in records:
        compact_records.append(
            {
                "timestamp": str(row.get("ts") or row.get("timestamp") or "").strip(),
                "source": str(row.get("source", "")).strip(),
                "search_mode": str(row.get("search_mode", "")).strip(),
                "query_profile": str(row.get("query_profile", "")).strip(),
                "token_count": int(row.get("token_count", 0) or 0),
            }
        )
        # Read flat stage fields from each record.
        for key in STAGE_KEYS:
            value = row.get(key)
            if value is None:
                # Fall back to legacy alias key.
                for legacy_key, canonical in LEGACY_ALIASES.items():
                    if canonical == key and row.get(legacy_key) is not None:
                        value = row.get(legacy_key)
                        break
            if value is None:
                continue
            try:
                score = float(value)
            except Exception:
                continue
            if score < 0:
                continue
            stage_values.setdefault(key, []).append(score)

    stages = {name: _timing_stats(values) for name, values in sorted(stage_values.items())}

    # Cache hit rates and no-context rate from per-record flags.
    n = len(records)
    embed_hits = sum(1 for r in records if float(r.get("embed_cache_hit", 0) or 0) > 0)
    web_hits = sum(1 for r in records if float(r.get("web_cache_hit", 0) or 0) > 0)
    no_ctx = sum(1 for r in records if float(r.get("no_context", 0) or 0) > 0)

    # Rerank quality: average top1 cosine score before rerank and delta (after - before)
    before_scores = [float(r["top1_score_before_rerank"]) for r in records if r.get("top1_score_before_rerank") is not None]
    after_scores = [float(r["top1_score_after_rerank"]) for r in records if r.get("top1_score_after_rerank") is not None]
    deltas = [
        float(r["top1_score_after_rerank"]) - float(r["top1_score_before_rerank"])
        for r in records
        if r.get("top1_score_before_rerank") is not None and r.get("top1_score_after_rerank") is not None
    ]
    rank_shifts = [
        float(r["top1_rank_shift"])
        for r in records
        if r.get("top1_rank_shift") is not None
    ]
    identity_changes = [
        int(r["top1_identity_changed"])
        for r in records
        if r.get("top1_identity_changed") is not None
    ]
    rerank_quality = {
        "avg_top1_before": round(sum(before_scores) / len(before_scores), 4) if before_scores else None,
        "avg_top1_after": round(sum(after_scores) / len(after_scores), 4) if after_scores else None,
        "avg_delta": round(sum(deltas) / len(deltas), 4) if deltas else None,
        "rerank_improvement": round(sum(deltas) / len(deltas), 4) if deltas else None,
        "avg_top1_local_doc_score": round(sum(after_scores) / len(after_scores), 4) if after_scores else None,
        "avg_top1_local_doc_score_p99": _top_percent_mean(after_scores, percent=0.01),
        "top1_identity_change_rate": round(sum(identity_changes) / len(identity_changes), 4) if identity_changes else None,
        "avg_rank_shift": round(sum(rank_shifts) / len(rank_shifts), 4) if rank_shifts else None,
    }

    # Per-profile (short/medium/long) stage breakdowns for export to benchmark/dashboard.
    by_profile: dict[str, Any] = {}
    for profile in ("short", "medium", "long"):
        prows = [r for r in records if r.get("query_profile") == profile]
        if not prows:
            continue
        pstage: dict[str, list[float]] = {}
        for row in prows:
            for key in STAGE_KEYS:
                value = row.get(key)
                if value is None:
                    for lk, canonical in LEGACY_ALIASES.items():
                        if canonical == key and row.get(lk) is not None:
                            value = row.get(lk)
                            break
                if value is None:
                    continue
                try:
                    fv = float(value)
                except Exception:
                    continue
                if fv >= 0:
                    pstage.setdefault(key, []).append(fv)
        by_profile[profile] = {
            "count": len(prows),
            "stages": {k: _timing_stats(v) for k, v in pstage.items()},
        }

    return {
        "records": compact_records,
        "record_count": len(compact_records),
        "stages": stages,
        "rerank_quality": rerank_quality,
        "embed_cache_hit_rate": round(embed_hits / n, 3) if n else None,
        "web_cache_hit_rate": round(web_hits / n, 3) if n else None,
        "no_context_rate": round(no_ctx / n, 3) if n else None,
        "by_profile": by_profile,
    }


def _load_agent_metrics_summary() -> dict[str, Any]:
    """Load nav_dashboard per-round agent metrics and compute aggregate stats."""
    path = PROJECT_ROOT / "nav_dashboard" / "data" / "agent_metrics.json"
    payload = _safe_load_json(path, default={})
    rows = [r for r in (payload.get("records") or []) if isinstance(r, dict)]
    rows = rows[-20:]
    n = len(rows)
    if not n:
        return {"records": 0, "web_cache_hit_rate": None, "no_context_rate": None, "by_profile": {}}

    web_hits = sum(1 for r in rows if int(r.get("web_cache_hit", 0) or 0))
    no_ctx = sum(1 for r in rows if int(r.get("no_context", 0) or 0))

    # Rerank quality delta (agent uses vector+keyword fusion — same scale 0-1)
    doc_before = [float(r["doc_top1_score_before_rerank"]) for r in rows if r.get("doc_top1_score_before_rerank") is not None]
    doc_after = [float(r["doc_top1_score"]) for r in rows if r.get("doc_top1_score") is not None]
    doc_deltas = [
        float(r["doc_top1_score"]) - float(r["doc_top1_score_before_rerank"])
        for r in rows
        if r.get("doc_top1_score") is not None and r.get("doc_top1_score_before_rerank") is not None
    ]
    rank_shifts = [
        float(r["doc_top1_rank_shift"])
        for r in rows
        if r.get("doc_top1_rank_shift") is not None
    ]
    identity_changes = [
        int(r["doc_top1_identity_changed"])
        for r in rows
        if r.get("doc_top1_identity_changed") is not None
    ]
    wall_vals = [float(r["wall_clock_seconds"]) for r in rows if r.get("wall_clock_seconds") and float(r["wall_clock_seconds"]) > 0]
    rerank_quality = {
        "avg_top1_before": round(sum(doc_before) / len(doc_before), 4) if doc_before else None,
        "avg_top1_after": round(sum(doc_after) / len(doc_after), 4) if doc_after else None,
        "avg_delta": round(sum(doc_deltas) / len(doc_deltas), 4) if doc_deltas else None,
        "rerank_improvement": round(sum(doc_deltas) / len(doc_deltas), 4) if doc_deltas else None,
        "avg_top1_local_doc_score": round(sum(doc_after) / len(doc_after), 4) if doc_after else None,
        "avg_top1_local_doc_score_p99": _top_percent_mean(doc_after, percent=0.01),
        "top1_identity_change_rate": round(sum(identity_changes) / len(identity_changes), 4) if identity_changes else None,
        "avg_rank_shift": round(sum(rank_shifts) / len(rank_shifts), 4) if rank_shifts else None,
    }

    by_profile: dict[str, Any] = {}
    for profile in ("short", "medium", "long"):
        prows = [r for r in rows if r.get("query_profile") == profile]
        if not prows:
            continue
        vec_vals = [float(r.get("vector_recall_seconds", 0) or 0) for r in prows]
        rer_vals = [float(r.get("rerank_seconds", 0) or 0) for r in prows]
        by_profile[profile] = {
            "count": len(prows),
            "vector_recall": _timing_stats(vec_vals),
            "rerank": _timing_stats(rer_vals),
        }

    return {
        "records": n,
        "web_cache_hit_rate": round(web_hits / n, 3) if n else None,
        "no_context_rate": round(no_ctx / n, 3) if n else None,
        "rerank_quality": rerank_quality,
        "wall_clock": _timing_stats(wall_vals),
        "by_profile": by_profile,
    }


def _load_startup_status() -> dict[str, Any]:
    base = _ai_summary_internal_base_url()
    endpoint = f"{base}/api/workflow/startup-status"
    payload = _http_json_get(endpoint, timeout=2.5)
    if not payload or payload.get("ok") is False and payload.get("status") is None:
        err = str(payload.get("error", "无法连接 startup-status 接口")).strip()
        return {
            "status": "unreachable",
            "last_checked_at": "",
            "check_result": {},
            "warmup_result": {},
            "logs": [f"startup-status 接口不可用: {err}"],
        }

    logs = payload.get("logs") if isinstance(payload.get("logs"), list) else []
    safe_logs = [str(line) for line in logs][-20:]
    return {
        "status": str(payload.get("status", "unknown")),
        "last_checked_at": str(payload.get("last_checked_at", "")),
        "check_result": payload.get("check_result") if isinstance(payload.get("check_result"), dict) else {},
        "warmup_result": payload.get("warmup_result") if isinstance(payload.get("warmup_result"), dict) else {},
        "logs": safe_logs,
    }


@app.get("/api/custom_cards")
def get_custom_cards() -> dict[str, Any]:
    return {"cards": _load_custom_cards()}


# ─── Dashboard overview TTL cache ─────────────────────────────────────────────
# The overview endpoint does many synchronous I/O operations (JSON file reads,
# glob over markdown files, SQLite queries, an HTTP call to startup-status).
# On LAN access multiple devices can hit it simultaneously and saturate the
# FastAPI sync thread pool, making all other API calls slow.
# Cache the result for OVERVIEW_CACHE_TTL seconds; the manual refresh button
# sets force=true to bypass the cache.

_OVERVIEW_CACHE_TTL = float(os.getenv("NAV_DASHBOARD_OVERVIEW_CACHE_TTL", "30"))
_overview_cache: dict[str, Any] | None = None
_overview_cache_at: float = 0.0
_overview_cache_lock = threading.Lock()


def _build_overview() -> dict[str, Any]:
    """Run all the expensive I/O and return the full overview dict."""
    rag_docs, rag_changed, rag_sources = _count_rag_index_docs()
    lib_total, lib_by_media, lib_vector_rows, lib_graph_nodes, lib_graph_edges, lib_this_year = _library_counts()
    rag_graph_nodes, rag_graph_edges = _rag_graph_counts()
    missing_queries = _load_missing_queries(days=30, limit=200)
    month_web, month_deepseek, quota_daily = _load_monthly_quota_counts()
    session_count, message_count = _agent_session_counts()
    rag_qa_sessions, rag_qa_messages = _rag_qa_session_counts()
    startup = _load_startup_status_cached()
    retrieval_latency = _load_retrieval_latency_summary()
    agent_metrics = _load_agent_metrics_summary()

    warnings: list[str] = []
    if startup.get("status") == "unreachable":
        warnings.append("Startup-status 接口不可达")

    return {
        "ok": True,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "deployed_at": _DEPLOY_TIME,
        "month": datetime.now().strftime("%Y-%m"),
        "rag": {
            "indexed_documents": rag_docs,
            "changed_pending": rag_changed,
            "source_markdown_files": rag_sources,
            "graph_nodes": rag_graph_nodes,
            "graph_edges": rag_graph_edges,
            "nodes_per_doc": _safe_div(rag_graph_nodes, rag_docs),
            "edges_per_node": _safe_div(rag_graph_edges, rag_graph_nodes),
        },
        "library": {
            "total_items": lib_total,
            "by_media": lib_by_media,
            "vector_rows": lib_vector_rows,
            "graph_nodes": lib_graph_nodes,
            "graph_edges": lib_graph_edges,
            "this_year_items": lib_this_year,
        },
        "api_usage": {
            "month_web_search_calls": month_web,
            "month_deepseek_calls": month_deepseek,
            **quota_daily,
        },
        "agent": {
            "session_count": session_count,
            "message_count": message_count,
        },
        "rag_qa": {
            "session_count": rag_qa_sessions,
            "message_count": rag_qa_messages,
        },
        "startup": startup,
        "retrieval_latency": retrieval_latency,
        "cache_stats": {
            "rag_embed_cache_hit_rate": retrieval_latency.get("embed_cache_hit_rate"),
            "rag_web_cache_hit_rate": retrieval_latency.get("web_cache_hit_rate"),
            "rag_no_context_rate": retrieval_latency.get("no_context_rate"),
            "agent_web_cache_hit_rate": agent_metrics.get("web_cache_hit_rate"),
            "agent_no_context_rate": agent_metrics.get("no_context_rate"),
        },
        "retrieval_by_profile": retrieval_latency.get("by_profile", {}),
        "agent_by_profile": agent_metrics.get("by_profile", {}),
        "rerank_quality": {
            "rag": retrieval_latency.get("rerank_quality", {}),
            "agent": agent_metrics.get("rerank_quality", {}),
        },
        "missing_queries_last_30d": {
            "count": len(missing_queries),
            "items": missing_queries,
            "sample_queries": [str(item.get("query", "")) for item in missing_queries[:3]],
        },
        "agent_wall_clock": agent_metrics.get("wall_clock", {}),
        "warnings": warnings,
    }


@app.get("/api/dashboard/overview")
def get_dashboard_overview(force: bool = False) -> dict[str, Any]:
    global _overview_cache, _overview_cache_at  # noqa: PLW0603
    now = time.monotonic()
    with _overview_cache_lock:
        if not force and _overview_cache is not None and (now - _overview_cache_at) < _OVERVIEW_CACHE_TTL:
            return _overview_cache
    result = _build_overview()
    with _overview_cache_lock:
        _overview_cache = result
        _overview_cache_at = time.monotonic()
    return result


@app.get("/api/dashboard/missing-queries")
def get_dashboard_missing_queries(days: int = 30, limit: int = 200, source: str = "all") -> dict[str, Any]:
    rows = _load_missing_queries(days=days, limit=limit, source=source)
    return {
        "ok": True,
        "days": max(1, int(days)),
        "source": str(source or "all"),
        "count": len(rows),
        "items": rows,
    }


@app.get("/api/dashboard/missing-queries/export")
def export_dashboard_missing_queries(days: int = 30, limit: int = 5000, source: str = "all") -> str:
    rows = _load_missing_queries(days=days, limit=limit, source=source)
    header = "ts,source,top1_score,threshold,query"
    lines = [header]
    for r in rows:
        query = str(r.get("query", "")).replace('"', '""')
        top1 = "" if r.get("top1_score") is None else str(r.get("top1_score"))
        threshold = "" if r.get("threshold") is None else str(r.get("threshold"))
        lines.append(
            f'{str(r.get("ts", ""))},{str(r.get("source", ""))},{top1},{threshold},"{query}"'
        )
    csv_body = "\ufeff" + "\r\n".join(lines)
    return Response(content=csv_body, media_type="text/csv; charset=utf-8")


@app.delete("/api/dashboard/missing-queries")
def clear_dashboard_missing_queries(source: str = "all") -> dict[str, Any]:
    try:
        if NO_CONTEXT_LOG_PATH.exists():
            source_filter = str(source or "all").strip().lower()
            if source_filter in {"", "all", "*"}:
                NO_CONTEXT_LOG_PATH.write_text("", encoding="utf-8")
            else:
                lines = NO_CONTEXT_LOG_PATH.read_text(encoding="utf-8").splitlines()
                kept: list[str] = []
                for line in lines:
                    raw = str(line or "").strip()
                    if not raw:
                        continue
                    try:
                        row = json.loads(raw)
                    except Exception:
                        kept.append(raw)
                        continue
                    if not isinstance(row, dict):
                        kept.append(raw)
                        continue
                    row_source = str(row.get("source", "unknown") or "unknown").strip().lower()
                    if row_source != source_filter:
                        kept.append(raw)
                NO_CONTEXT_LOG_PATH.write_text(("\n".join(kept) + ("\n" if kept else "")), encoding="utf-8")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    global _overview_cache, _overview_cache_at  # noqa: PLW0603
    with _overview_cache_lock:
        _overview_cache = None
        _overview_cache_at = 0.0
    return {"ok": True, "cleared": True, "source": str(source or "all")}


_STARTUP_STATUS_CACHE_TTL = float(os.getenv("NAV_DASHBOARD_STARTUP_STATUS_CACHE_TTL", "5"))
_startup_status_cache: dict[str, Any] | None = None
_startup_status_cache_at: float = 0.0
_startup_status_cache_lock = threading.Lock()


def _load_startup_status_cached() -> dict[str, Any]:
    """Cache the startup-status HTTP call for _STARTUP_STATUS_CACHE_TTL seconds."""
    global _startup_status_cache, _startup_status_cache_at  # noqa: PLW0603
    now = time.monotonic()
    with _startup_status_cache_lock:
        if _startup_status_cache is not None and (now - _startup_status_cache_at) < _STARTUP_STATUS_CACHE_TTL:
            return _startup_status_cache
    result = _load_startup_status()
    with _startup_status_cache_lock:
        _startup_status_cache = result
        _startup_status_cache_at = time.monotonic()
    return result


@app.get("/api/startup/status")
def get_startup_status_lean() -> dict[str, Any]:
    s = _load_startup_status_cached()
    return {"ok": True, "status": s.get("status", "unknown"), "last_checked_at": s.get("last_checked_at", "")}


@app.patch("/api/dashboard/usage")
def adjust_dashboard_usage(payload: UsageAdjustPayload) -> dict[str, Any]:
    month_key = datetime.now().strftime("%Y-%m")
    history_path = APP_DIR.parent / "data" / "agent_quota_history.json"
    data = _safe_load_json(history_path, default={})
    if not isinstance(data, dict):
        data = {}
    if "months" not in data or not isinstance(data.get("months"), dict):
        data["months"] = {}
    existing = data["months"].get(month_key, {})
    if not isinstance(existing, dict):
        existing = {}
    data["months"][month_key] = {
        **existing,
        "web_search": max(0, int(payload.month_web_search_calls)),
        "deepseek": max(0, int(payload.month_deepseek_calls)),
    }
    try:
        history_path.parent.mkdir(parents=True, exist_ok=True)
        history_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"ok": True, "month": month_key, **data["months"][month_key]}


class UsageRecordPayload(BaseModel):
    web_search_delta: int = 0
    deepseek_delta: int = 0


@app.post("/api/dashboard/usage/record")
def record_dashboard_usage(payload: UsageRecordPayload) -> dict[str, Any]:
    """Increment today's quota counters and append to monthly history.
    Called by external services (e.g. RAG QA) to record their API usage.
    """
    web_inc = max(0, int(payload.web_search_delta or 0))
    deepseek_inc = max(0, int(payload.deepseek_delta or 0))
    if web_inc <= 0 and deepseek_inc <= 0:
        return {"ok": True, "skipped": True}
    try:
        quota_state = agent_service._load_quota_state()  # noqa: SLF001
        agent_service._increment_quota_state(quota_state, web_search_delta=web_inc, deepseek_delta=deepseek_inc)  # noqa: SLF001
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"ok": True, "web_search_delta": web_inc, "deepseek_delta": deepseek_inc}


@app.post("/api/dashboard/trigger-rag-sync")
def trigger_rag_sync() -> dict[str, Any]:
    """Proxy to RAG workflow: trigger a sync_embeddings job to process changed docs."""
    base = _ai_summary_internal_base_url().rstrip("/")
    try:
        body = json.dumps({"action": "sync_embeddings"}).encode("utf-8")
        target_url = f"{base}/api/workflow/run"
        req = urlrequest.Request(
            url=target_url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        host = (urlparse.urlparse(target_url).hostname or "").lower()
        local_hosts = {"127.0.0.1", "localhost", "::1"}
        if host in local_hosts:
            opener = urlrequest.build_opener(urlrequest.ProxyHandler({}))
            resp_ctx = opener.open(req, timeout=10)
        else:
            resp_ctx = urlrequest.urlopen(req, timeout=10)

        with resp_ctx as resp:
            result = json.loads(resp.read())
        return {"ok": True, "result": result}
    except urlerror.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise HTTPException(status_code=exc.code, detail=detail) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.post("/api/custom_cards/slot/{index}")
def save_custom_card(index: int, payload: CustomCardPayload, background_tasks: BackgroundTasks) -> dict[str, Any]:
    if index < 0 or index >= CUSTOM_CARDS_MAX:
        raise HTTPException(status_code=400, detail=f"index out of range: {index}")

    cards = _load_custom_cards()
    payload_data = payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
    cards[index] = _normalize_card(payload_data)
    saved = _save_custom_cards(cards)
    background_tasks.add_task(_trigger_custom_card_compression)
    return {
        "ok": True,
        "card": saved[index],
        "cards": saved,
    }


@app.post("/api/custom_cards/upload")
async def upload_custom_card_image(request: Request, filename: str | None = None) -> dict[str, str | bool]:
    try:
        filename = str(filename or "").strip()
        content_type = (request.headers.get("content-type") or "").split(";")[0].strip().lower()
        if content_type and not content_type.startswith("image/"):
            raise HTTPException(status_code=400, detail="Content-Type must be image/*")

        ext = Path(filename).suffix.lower()
        if not ext:
            ext = CONTENT_TYPE_TO_EXT.get(content_type, "")
        if ext not in ALLOWED_IMAGE_EXTS:
            raise HTTPException(status_code=400, detail="仅支持 png/jpg/jpeg/webp/gif")

        if not filename:
            filename = f"card{ext}"

        stem = re.sub(r"[^a-zA-Z0-9_-]", "_", Path(filename).stem).strip("_") or "card"
        out_name = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{stem}_{uuid4().hex[:8]}{ext}"

        try:
            CUSTOM_CARD_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"目录创建失败: {str(e)}")
        
        out_path = CUSTOM_CARD_UPLOAD_DIR / out_name
        content = await request.body()
        if not content:
            raise HTTPException(status_code=400, detail="上传文件为空")
        
        try:
            out_path.write_bytes(content)
        except PermissionError:
            raise HTTPException(status_code=500, detail="文件写入权限不足")
        except OSError as e:
            raise HTTPException(status_code=500, detail=f"文件写入失败: {str(e)}")
        
        return {
            "ok": True,
            "image": f"/static/custom_cards/{out_name}",
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"上传过程出错: {str(e)}")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    scheme = request.url.scheme or "http"
    host = request.url.hostname or "127.0.0.1"
    ai_summary_url = AI_SUMMARY_URL_OVERRIDE or f"{scheme}://{host}:8000/"
    library_tracker_url = LIBRARY_TRACKER_URL_OVERRIDE or f"{scheme}://{host}:8091/"

    local_model = os.getenv("AI_SUMMARY_LOCAL_LLM_MODEL", "qwen2.5-7b-instruct").strip() or "qwen2.5-7b-instruct"
    deepseek_model = os.getenv("DEEPSEEK_MODEL", "deepseek-chat").strip() or "deepseek-chat"
    dashboard_prefill = get_dashboard_overview()

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "ai_summary_url": ai_summary_url,
            "library_tracker_url": library_tracker_url,
            "local_model": local_model,
            "deepseek_model": deepseek_model,
            "custom_cards_json": json.dumps(_load_custom_cards(), ensure_ascii=False),
            "dashboard_prefill_json": json.dumps(dashboard_prefill, ensure_ascii=False),
        },
    )


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


_uvicorn_server: Any = None  # holds the uvicorn.Server instance for graceful shutdown


@app.post("/api/shutdown")
async def api_shutdown() -> dict[str, str]:
    """Graceful shutdown endpoint — closes active connections before exiting."""
    async def _stop() -> None:
        await asyncio.sleep(0.25)
        if _uvicorn_server is not None:
            _uvicorn_server.should_exit = True
    asyncio.create_task(_stop())
    return {"status": "shutting_down"}


def run() -> None:
    import uvicorn
    global _uvicorn_server

    config = uvicorn.Config("web.main:app", host=HOST, port=PORT, reload=False)
    _uvicorn_server = uvicorn.Server(config)
    _uvicorn_server.run()


if __name__ == "__main__":
    run()
