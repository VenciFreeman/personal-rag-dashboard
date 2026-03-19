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
import shutil
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
from web.api.benchmark import QUERY_CASE_SETS
from web.config import AI_SUMMARY_URL_OVERRIDE, HOST, LIBRARY_TRACKER_URL_OVERRIDE, PORT
from web.services import agent_service, dashboard_jobs

APP_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = APP_DIR.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core_service.chat_feedback_store import append_feedback, clear_feedback, list_feedback
from core_service.config import get_settings
from core_service.ticket_store import (
    build_ticket_facets,
    build_ticket_weekly_stats,
    create_ticket,
    delete_ticket,
    get_ticket,
    list_ticket_storage_paths,
    list_tickets,
    update_ticket,
)
from core_service.trace_store import get_trace_record, list_trace_record_paths, render_trace_export

CUSTOM_CARDS_FILE = PROJECT_ROOT / "data" / "custom_cards.json"
CUSTOM_CARDS_MAX = 8
CUSTOM_CARD_UPLOAD_DIR = APP_DIR / "static" / "custom_cards"
NO_CONTEXT_LOG_PATH = PROJECT_ROOT / "ai_conversations_summary" / "data" / "cache" / "no_context_queries.jsonl"
DEPLOY_INFO_FILE = PROJECT_ROOT / "data" / "nav_dashboard_deploy.json"
ALLOWED_IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp", ".gif"}
CONTENT_TYPE_TO_EXT = {
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
}
SOURCE_LABELS = {
    "agent": "LLM Agent",
    "rag_qa": "RAG 问答",
    "rag_qa_stream": "RAG 问答（流式）",
    "benchmark_rag": "Benchmark / RAG",
    "benchmark_agent": "Benchmark / Agent",
    "agent_chat": "LLM Agent",
    "rag_chat": "RAG 问答",
}
templates = Jinja2Templates(directory=str(APP_DIR / "templates"))


def _static_version(filename: str) -> str:
    """Return a cache-busting token derived from the file's mtime.

    Using hex mtime keeps URLs short and changes automatically on every
    deploy/edit without requiring a manual version bump.
    """
    p = APP_DIR / "static" / filename
    try:
        return format(int(os.path.getmtime(p)), "x")
    except OSError:
        return "0"


# ── Dashboard SSR helpers ─────────────────────────────────────────────────────
def _esc(s: object) -> str:
    """HTML-escape a value for embedding in attribute or text content."""
    import html as _html
    return _html.escape(str(s) if s is not None else "")


def _fmt_n(v: object) -> str:
    """Format a number as comma-separated integer, or '—' if missing/invalid."""
    try:
        return f"{int(round(float(v))):,}"  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return "\u2014"


def _fmt_r(v: object) -> str:
    """Format a fraction as '12.3%', or '—' if missing/invalid."""
    try:
        return f"{float(v) * 100:.1f}%"  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return "\u2014"


def _fmt_duration(v: object) -> str:
    """Format seconds like the dashboard JS formatter."""
    try:
        n = float(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return "\u2014"
    if n < 0:
        return "\u2014"
    if n >= 1:
        return f"{n:.2f}s"
    if n >= 0.001:
        return f"{n * 1000:.1f}ms"
    if n > 0:
        return f"{int(round(n * 1_000_000))}\u00b5s"
    return "0s"


def _fmt_size(v: object) -> str:
    try:
        n = float(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return "0 MB"
    if n <= 0:
        return "0 MB"
    if n >= 1024 * 1024 * 1024:
        return f"{n / (1024 * 1024 * 1024):.2f} GB"
    if n >= 1024 * 1024:
        return f"{n / (1024 * 1024):.2f} MB"
    if n >= 1024:
        return f"{n / 1024:.1f} KB"
    return f"{int(round(n))} B"


def _fmt_signed(v: object, digits: int = 4) -> str:
    try:
        n = float(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return "\u2014"
    text = f"{n:.{digits}f}"
    return f"+{text}" if n > 0 else text


def _css_token(value: object) -> str:
    return re.sub(r"[^a-z0-9_-]+", "-", str(value or "").strip().lower())


def _stat_card(title: str, value: str, sub: str = "", role: str = "", state: str = "") -> str:
    extra = f' data-role="{_esc(role)}"' if role else ""
    classes = "stat-card" + (f" {_esc(state)}" if state else "")
    return (
        f'<article class="{classes}"{extra}>'
        f'<div class="stat-title">{_esc(title)}</div>'
        f'<div class="stat-value">{_esc(value)}</div>'
        f'<div class="stat-sub">{_esc(sub)}</div>'
        f"</article>"
    )


def _render_dashboard_core_cards_html(prefill: dict) -> str:  # type: ignore[type-arg]
    """Return pre-rendered stat-card HTML from overview data.

    Uses detailed latency/cache/rerank values when the full overview is
    available, and falls back to neutral placeholders when only the lighter
    prefill payload is present.
    """
    if not prefill or not isinstance(prefill, dict):
        return ""
    rag = prefill.get("rag") or {}
    lib = prefill.get("library") or {}
    gq = lib.get("graph_quality") or {}
    api = prefill.get("api_usage") or {}
    agent = prefill.get("agent") or {}
    rqa = prefill.get("rag_qa") or {}
    latency = prefill.get("retrieval_latency") or {}
    latency_stages = latency.get("stages") or {}
    cache_stats = prefill.get("cache_stats") or {}
    rerank_quality = prefill.get("rerank_quality") or {}
    rag_rerank = rerank_quality.get("rag") or {}
    agent_rerank = rerank_quality.get("agent") or {}
    missing_queries = prefill.get("missing_queries_last_30d") or {}
    agent_wall_clock = prefill.get("agent_wall_clock") or {}
    runtime_data = prefill.get("runtime_data") or {}
    chat_feedback = prefill.get("chat_feedback") or {}
    warns = prefill.get("warnings") or []

    nodes_per_doc = rag.get("nodes_per_doc")
    edges_per_node = rag.get("edges_per_node")
    pending = int(rag.get("changed_pending") or 0)
    warn_sub = " | ".join(str(w) for w in warns[:2]) if warns else "\u65e0\u544a\u8b66"

    dash = "\u2014"
    neutral_sub = "\u8be6\u60c5\u7edf\u8ba1\u52a0\u8f7d\u4e2d"

    total_stage = latency_stages.get("total") or {}
    rerank_stage = latency_stages.get("rerank_seconds") or {}
    elapsed_stage = latency_stages.get("elapsed_seconds") or {}

    cards = [
        _stat_card("RAG \u5df2\u7d22\u5f15\u6587\u6863", _fmt_n(rag.get("indexed_documents")),
                   f"\u603b\u6587\u6863\u6570 {_fmt_n(rag.get('source_markdown_files'))}"),
        _stat_card("\u4e66\u5f71\u97f3\u6e38\u620f\u603b\u6761\u76ee", _fmt_n(lib.get("total_items")),
                   f"\u4eca\u5e74\u6761\u76ee {_fmt_n(lib.get('this_year_items'))}"),
        _stat_card("RAG Graph \u8282\u70b9\u6570", _fmt_n(rag.get("graph_nodes")),
                   f"\u8fb9\u6570 {_fmt_n(rag.get('graph_edges'))}"),
        _stat_card("Library Graph \u8282\u70b9\u6570", _fmt_n(lib.get("graph_nodes")),
                   f"\u8fb9\u6570 {_fmt_n(lib.get('graph_edges'))} | \u8986\u76d6 {_fmt_r(gq.get('item_coverage_rate'))} | \u5b64\u70b9 {_fmt_r(gq.get('isolated_node_rate'))}",
                   "library-graph-summary"),
        _stat_card("RAG \u5e73\u5747\u8282\u70b9\u6570",
                   f"{float(nodes_per_doc):.2f}" if nodes_per_doc is not None else dash,
                   f"\u6bcf\u8282\u70b9\u5e73\u5747\u8fb9\u6570 {float(edges_per_node):.2f}" if edges_per_node is not None else f"\u6bcf\u8282\u70b9\u5e73\u5747\u8fb9\u6570 {dash}"),
        _stat_card("RAG \u5f85\u91cd\u5efa\u6587\u6863", _fmt_n(pending),
                   "\u7b49\u5f85\u540e\u53f0\u540c\u6b65" if pending > 0 else "\u5df2\u5168\u90e8\u540c\u6b65",
                   "rag-changed-pending"),
        _stat_card("\u672c\u6708 Tavily API \u8c03\u7528", _fmt_n(api.get("month_web_search_calls")),
                   f"\u4eca\u65e5 {_fmt_n(api.get('today_web_search'))} / \u9650\u989d {_fmt_n(api.get('daily_web_limit'))}",
                   "web-search-usage"),
        _stat_card("\u672c\u6708 DeepSeek API \u8c03\u7528", _fmt_n(api.get("month_deepseek_calls")),
                   f"\u4eca\u65e5 {_fmt_n(api.get('today_deepseek'))} / \u9650\u989d {_fmt_n(api.get('daily_deepseek_limit'))}",
                   "deepseek-usage"),
        _stat_card("Agent \u6d88\u606f\u603b\u6570", _fmt_n(agent.get("message_count")),
                   f"\u4f1a\u8bdd\u6570 {_fmt_n(agent.get('session_count'))}"),
        _stat_card("RAG Q&A \u6d88\u606f\u603b\u6570", _fmt_n(rqa.get("message_count")),
                   f"\u4f1a\u8bdd\u6570 {_fmt_n(rqa.get('session_count'))}"),
        _stat_card(
            "\u5411\u91cf\u53ec\u56de\u5747\u503c",
            _fmt_duration(total_stage.get("avg")) if total_stage else dash,
            f"\u8fd1 {_fmt_n(total_stage.get('count'))} \u6b21 | p50 {_fmt_duration(total_stage.get('p50'))}" if total_stage else neutral_sub,
        ),
        _stat_card(
            "RAG \u6a21\u578b\u91cd\u6392\u5747\u503c",
            _fmt_duration(rerank_stage.get("avg")) if rerank_stage else dash,
            f"\u8fd1 {_fmt_n(rerank_stage.get('count'))} \u6b21" if rerank_stage else neutral_sub,
        ),
        _stat_card(
            "\u68c0\u7d22\u5206\u4f4d p50",
            _fmt_duration(total_stage.get("p50")) if total_stage else dash,
            f"p95 {_fmt_duration(total_stage.get('p95'))} | p99 {_fmt_duration(total_stage.get('p99'))}" if total_stage else neutral_sub,
        ),
        _stat_card(
            "RAG \u5168\u6d41\u7a0b p50",
            _fmt_duration(elapsed_stage.get("p50")) if elapsed_stage else dash,
            f"p95 {_fmt_duration(elapsed_stage.get('p95'))} | Agent p50 {_fmt_duration(agent_wall_clock.get('p50'))}" if elapsed_stage or agent_wall_clock else neutral_sub,
        ),
        _stat_card(
            "RAG \u91cd\u6392\u5e8f\u6362\u699c\u7387",
            _fmt_r(rag_rerank.get("top1_identity_change_rate")) if rag_rerank else dash,
            f"Agent \u6362\u699c\u7387 {_fmt_r(agent_rerank.get('top1_identity_change_rate'))}" if rag_rerank or agent_rerank else neutral_sub,
        ),
        _stat_card(
            "RAG \u5e73\u5747\u6362\u699c",
            _fmt_signed(rag_rerank.get("avg_rank_shift"), 2) if rag_rerank else dash,
            f"Agent \u5e73\u5747\u6362\u699c {_fmt_signed(agent_rerank.get('avg_rank_shift'), 2)}" if rag_rerank or agent_rerank else neutral_sub,
        ),
        _stat_card(
            "Embedding \u7f13\u5b58\u547d\u4e2d\u7387",
            _fmt_r(cache_stats.get("rag_embed_cache_hit_rate")) if cache_stats else dash,
            f"\u8fd1 {_fmt_n(latency.get('record_count'))} \u6b21" if cache_stats or latency else neutral_sub,
        ),
        _stat_card(
            "Agent \u6587\u6863\u8c03\u7528\u7387",
            _fmt_r(cache_stats.get("agent_rag_trigger_rate")) if cache_stats else dash,
            f"Media {_fmt_r(cache_stats.get('agent_media_trigger_rate'))} | Web {_fmt_r(cache_stats.get('agent_web_trigger_rate'))}" if cache_stats else neutral_sub,
        ),
        _stat_card(
            "RAG Top1 \u5747\u503c",
            f"{float(rag_rerank.get('avg_top1_local_doc_score')):.4f}" if rag_rerank.get("avg_top1_local_doc_score") is not None else dash,
            f"Agent Top1 \u5747\u503c {float(agent_rerank.get('avg_top1_local_doc_score')):.4f}" if agent_rerank.get("avg_top1_local_doc_score") is not None else neutral_sub,
        ),
        _stat_card(
            "RAG \u672a\u547d\u4e2d\u7387",
            _fmt_r(cache_stats.get("rag_no_context_rate")) if cache_stats else dash,
            f"Agent \u672a\u547d\u4e2d\u7387 {_fmt_r(cache_stats.get('agent_no_context_rate'))}" if cache_stats else neutral_sub,
        ),
        _stat_card("\u6708\u68c0\u7d22\u7f3a\u5931\u95ee\u9898\u6570", _fmt_n(missing_queries.get("count")) if missing_queries else dash, "\u957f\u6309\u67e5\u770b\u5bfc\u51fa" if missing_queries else neutral_sub, "missing-queries-summary"),
        _stat_card("\u804a\u5929\u53cd\u9988\u6570", _fmt_n(chat_feedback.get("count")) if chat_feedback else dash, "\u957f\u6309\u67e5\u770b\u5bfc\u51fa" if chat_feedback else neutral_sub, "feedback-summary"),
        _stat_card("\u8fd0\u884c\u65f6\u6570\u636e", _fmt_size(runtime_data.get("total_size_bytes")) if runtime_data else dash, f"\u975e\u7a7a {_fmt_n(runtime_data.get('nonzero_items'))} \u9879 | \u957f\u6309\u67e5\u770b" if runtime_data else neutral_sub, "runtime-data-summary"),
        _stat_card("\u7cfb\u7edf\u544a\u8b66", _fmt_n(len(warns)), warn_sub, "warnings-summary"),
    ]
    return "\n".join(cards)


def _render_dashboard_latency_table_html(data: dict[str, Any]) -> str:
    latency = data.get("retrieval_latency") or {}
    stages = latency.get("stages") or {}
    if not isinstance(stages, dict) or not stages:
        return (
            "<tbody>"
            "<tr><td colspan=\"5\">暂无最近20次记录</td></tr>"
            "</tbody>"
        )

    stage_order = [
        ("total", "向量召回"),
        ("rerank_seconds", "模型重排"),
        ("context_assembly_seconds", "上下文组装"),
        ("web_search_seconds", "网络检索"),
        ("elapsed_seconds", "端到端总时长"),
    ]
    known = {key for key, _label in stage_order}
    extra = [key for key in stages.keys() if key not in known and key != "reranker_load_seconds"]
    ordered = stage_order[:-1] + [(key, key) for key in extra] + [stage_order[-1]]

    rows: list[str] = []
    for key, label in ordered:
        stat = stages.get(key)
        if not isinstance(stat, dict):
            continue
        tr_class = ' class="latency-row-total"' if key == "elapsed_seconds" else ""
        rows.append(
            f"<tr{tr_class}>"
            f"<td>{_esc(label)}</td>"
            f"<td>{_esc(_fmt_duration(stat.get('avg')))}</td>"
            f"<td>{_esc(_fmt_duration(stat.get('p50')))}</td>"
            f"<td>{_esc(_fmt_duration(stat.get('p95')))}</td>"
            f"<td>{_esc(_fmt_duration(stat.get('p99')))}</td>"
            "</tr>"
        )

    body = "".join(rows) or '<tr><td colspan="5">暂无数据</td></tr>'
    return (
        "<thead><tr><th>阶段</th><th>均值</th><th>p50</th><th>p95</th><th>p99</th></tr></thead>"
        f"<tbody>{body}</tbody>"
    )


def _ordered_bucket_entries(bucket: dict[str, Any], ordered_keys: list[str]) -> list[tuple[str, Any]]:
    seen: set[str] = set()
    rows: list[tuple[str, Any]] = []
    for key in ordered_keys:
        seen.add(key)
        rows.append((key, bucket.get(key) or {}))
    for key, value in bucket.items():
        if key in seen:
            continue
        rows.append((key, value))
    return rows


def _render_dashboard_observability_table_html(data: dict[str, Any]) -> str:
    rag_profiles = data.get("retrieval_by_profile") or {}
    rag_modes = data.get("retrieval_by_search_mode") or {}
    agent_profiles = data.get("agent_by_profile") or {}
    agent_modes = data.get("agent_by_search_mode") or {}
    rows: list[str] = []

    def push_section(title: str) -> None:
        rows.append(
            "<tr class=\"dashboard-observability-section\">"
            f"<td colspan=\"6\"><span class=\"dashboard-observability-title\">{_esc(title)}</span></td>"
            "</tr>"
        )

    def push_row(name: str, c1: str, c2: str, c3: str, c4: str, c5: str) -> None:
        rows.append(
            f"<tr><td>{_esc(name)}</td><td>{_esc(c1)}</td><td>{_esc(c2)}</td><td>{_esc(c3)}</td><td>{_esc(c4)}</td><td>{_esc(c5)}</td></tr>"
        )

    profile_names = {"short": "短查询", "medium": "中查询", "long": "长查询"}
    profile_keys = ["short", "medium", "long"]
    mode_keys = ["local_only", "hybrid"]

    if isinstance(rag_profiles, dict) and rag_profiles:
        push_section("RAG 分层观测")
        for key, value in _ordered_bucket_entries(rag_profiles, profile_keys):
            value = value if isinstance(value, dict) else {}
            push_row(
                f"{profile_names.get(key, key)} ({_fmt_n(value.get('count'))})",
                _fmt_r(value.get("no_context_rate")),
                _fmt_r(value.get("embed_cache_hit_rate")),
                _fmt_r(value.get("query_rewrite_rate")),
                _fmt_duration(((value.get("stages") or {}).get("elapsed_seconds") or {}).get("p50")),
                _fmt_duration(((value.get("stages") or {}).get("total") or {}).get("p50")),
            )

    if isinstance(rag_modes, dict) and rag_modes:
        push_section("RAG 检索模式")
        for key, value in _ordered_bucket_entries(rag_modes, mode_keys):
            value = value if isinstance(value, dict) else {}
            push_row(
                f"{key} ({_fmt_n(value.get('count'))})",
                _fmt_r(value.get("no_context_rate")),
                _fmt_r(value.get("embed_cache_hit_rate")),
                _fmt_r(value.get("query_rewrite_rate")),
                _fmt_duration((value.get("elapsed") or {}).get("p50")),
                _fmt_duration((value.get("total") or {}).get("p50")),
            )

    if isinstance(agent_profiles, dict) and agent_profiles:
        push_section("Agent 分层观测")
        for key, value in _ordered_bucket_entries(agent_profiles, profile_keys):
            value = value if isinstance(value, dict) else {}
            push_row(
                f"{profile_names.get(key, key)} ({_fmt_n(value.get('count'))})",
                _fmt_r(value.get("no_context_rate")),
                _fmt_r(value.get("embed_cache_hit_rate")),
                _fmt_r(value.get("query_rewrite_rate")),
                _fmt_duration((value.get("wall_clock") or {}).get("p50")),
                _fmt_duration((value.get("vector_recall") or {}).get("p50")),
            )

    if isinstance(agent_modes, dict) and agent_modes:
        push_section("Agent 检索模式")
        for key, value in _ordered_bucket_entries(agent_modes, mode_keys):
            value = value if isinstance(value, dict) else {}
            push_row(
                f"{key} ({_fmt_n(value.get('count'))})",
                _fmt_r(value.get("no_context_rate")),
                _fmt_r(value.get("embed_cache_hit_rate")),
                _fmt_r(value.get("query_rewrite_rate")),
                _fmt_duration((value.get("wall_clock") or {}).get("p50")),
                _fmt_duration((value.get("vector_recall") or {}).get("p50")),
            )

    if not rows:
        return "<tbody><tr><td colspan=\"6\">暂无分层观测数据</td></tr></tbody>"

    return (
        "<thead><tr><th>维度</th><th>检索未命中</th><th>Embed 缓存命中率</th><th>问题重写率</th><th>端到端用时</th><th>召回用时</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
    )


def _job_type_label(value: object) -> str:
    labels = {
        "benchmark": "Benchmark",
        "rag_sync": "RAG 同步",
        "library_graph_rebuild": "Library Graph",
        "runtime_cleanup": "运行时清理",
    }
    key = str(value or "")
    return labels.get(key, key or "未知任务")


def _job_status_label(value: object) -> str:
    labels = {
        "queued": "排队中",
        "running": "运行中",
        "completed": "已完成",
        "failed": "失败",
        "cancelled": "已取消",
    }
    key = str(value or "")
    return labels.get(key, key or "未知")


def _render_dashboard_jobs_html(payload: dict[str, Any]) -> str:
    jobs = payload.get("jobs") if isinstance(payload, dict) else []
    if not isinstance(jobs, list) or not jobs:
        return '<div class="dashboard-job-empty">当前暂无后台任务</div>'

    selected_id = str((jobs[0] or {}).get("id") or "")
    parts: list[str] = []
    for job in jobs:
        if not isinstance(job, dict):
            continue
        job_id = str(job.get("id") or "")
        status = str(job.get("status") or "queued")
        selected_cls = " is-selected" if job_id == selected_id else ""
        running_cls = (
            " is-running" if status == "running"
            else " is-failed" if status == "failed"
            else " is-cancelled" if status == "cancelled"
            else ""
        )
        summary = str(job.get("message") or "等待开始")
        metadata = job.get("metadata") if isinstance(job.get("metadata"), dict) else {}
        module_meta = "+".join(str(item or "") for item in (metadata.get("modules") or []) if str(item or "").strip())
        created_at = str(job.get("created_at") or "")
        logs = job.get("logs") if isinstance(job.get("logs"), list) else []
        log_text = "\n".join(str(line or "") for line in logs) if logs else (str(job.get("error") or "") or "暂无日志")
        can_cancel = status in {"queued", "running"}
        expanded = ""
        if job_id == selected_id:
            expanded = (
                '<div class="dashboard-job-expanded">'
                f'<div class="dashboard-meta">{_esc(_job_type_label(job.get("type")))} | {_esc(_job_status_label(status))} | 创建 {_esc(created_at or "-")}</div>'
                f'<pre class="dashboard-job-log-window">{_esc(log_text)}</pre>'
                '<div class="card-modal-actions dashboard-job-actions">'
                f'<button class="ghost" data-job-cancel-id="{_esc(job_id)}"{"" if can_cancel else " disabled"}>取消任务</button>'
                '</div></div>'
            )
        parts.append(
            f'<div class="dashboard-job-card{selected_cls}{running_cls}" data-job-id="{_esc(job_id)}">'
            '<div class="dashboard-job-title">'
            f'<strong>{_esc(job.get("label") or _job_type_label(job.get("type")))}</strong>'
            f'<span class="dashboard-job-badge status-{_esc(_css_token(status))}">{_esc(_job_status_label(status))}</span>'
            '</div>'
            f'<div class="dashboard-job-meta-line">{_esc(_job_type_label(job.get("type")))}{(" | " + _esc(module_meta)) if module_meta else ""}</div>'
            f'<div class="dashboard-job-meta-line">{_esc(summary)}</div>'
            f'<div class="dashboard-job-meta-line">{_esc(created_at)}</div>'
            f'{expanded}'
            '</div>'
        )
    return "\n".join(parts)


def _dashboard_ticket_breakdown_html(counts: dict[str, Any]) -> str:
    if not isinstance(counts, dict) or not counts:
        return '<span class="ticket-empty-state">暂无数据</span>'
    items = sorted(counts.items(), key=lambda item: int(item[1] or 0), reverse=True)
    return "".join(
        f'<span class="dashboard-ticket-chip"><span class="dashboard-ticket-chip-label">{_esc(label or "unknown")}</span><strong>{_esc(_fmt_n(count))}</strong></span>'
        for label, count in items
    )


def _render_dashboard_ticket_summary_meta_text(stats: dict[str, Any]) -> str:
    summary = stats.get("summary") if isinstance(stats, dict) else {}
    if not isinstance(summary, dict) or not summary:
        return "暂无 ticket 统计"
    return (
        f"当前遗留 {_fmt_n(summary.get('current_open_total') or 0)} | "
        f"近 1 周提交 {_fmt_n(summary.get('submitted_last_week') or 0)} | "
        f"近 1 周关闭 {_fmt_n(summary.get('closed_last_week') or 0)}"
    )


def _render_dashboard_ticket_summary_html(stats: dict[str, Any]) -> str:
    weeks = stats.get("weeks") if isinstance(stats, dict) else []
    if not isinstance(weeks, list) or not weeks:
        return '<div class="ticket-trend-empty">暂无统计数据</div>'
    summary = stats.get("summary") if isinstance(stats.get("summary"), dict) else {}
    status_counts = stats.get("status_counts") if isinstance(stats.get("status_counts"), dict) else {}
    priority_counts = stats.get("priority_counts") if isinstance(stats.get("priority_counts"), dict) else {}
    return (
        '<div class="dashboard-ticket-summary-grid">'
        f'<div class="dashboard-ticket-summary-card"><span>当前遗留</span><strong>{_esc(_fmt_n(summary.get("current_open_total") or 0))}</strong></div>'
        f'<div class="dashboard-ticket-summary-card"><span>Ticket 总数</span><strong>{_esc(_fmt_n(summary.get("ticket_total") or 0))}</strong></div>'
        f'<div class="dashboard-ticket-summary-card"><span>近 1 周提交</span><strong>{_esc(_fmt_n(summary.get("submitted_last_week") or 0))}</strong></div>'
        f'<div class="dashboard-ticket-summary-card"><span>近 1 月提交</span><strong>{_esc(_fmt_n(summary.get("submitted_last_month") or 0))}</strong></div>'
        f'<div class="dashboard-ticket-summary-card"><span>近 1 周关闭</span><strong>{_esc(_fmt_n(summary.get("closed_last_week") or 0))}</strong></div>'
        f'<div class="dashboard-ticket-summary-card"><span>当前最长遗留天数</span><strong>{_esc(_fmt_n(summary.get("current_longest_open_days") or 0))}</strong></div>'
        '</div>'
        '<div class="dashboard-ticket-breakdown">'
        '<section class="dashboard-ticket-breakdown-group"><h4>状态分布（未关闭）</h4>'
        f'<div class="dashboard-ticket-chip-row">{_dashboard_ticket_breakdown_html(status_counts)}</div></section>'
        '<section class="dashboard-ticket-breakdown-group"><h4>优先级分布（未关闭）</h4>'
        f'<div class="dashboard-ticket-chip-row">{_dashboard_ticket_breakdown_html(priority_counts)}</div></section>'
        '</div>'
    )


def _render_dashboard_ticket_trend_meta_text(stats: dict[str, Any]) -> str:
    weeks = stats.get("weeks") if isinstance(stats, dict) else []
    if not isinstance(weeks, list) or not weeks:
        return "暂无 ticket 周统计"
    return f"近 {len(weeks)} 周 | 每周提交趋势 | 每周关闭趋势（仅 closed）"


def _render_dashboard_ticket_trend_html(stats: dict[str, Any]) -> str:
    weeks = stats.get("weeks") if isinstance(stats, dict) else []
    if not isinstance(weeks, list) or not weeks:
        return '<div class="ticket-trend-empty">暂无图表数据</div>'

    submitted = [int((item or {}).get("submitted") or 0) for item in weeks if isinstance(item, dict)]
    closed = [int((item or {}).get("closed") or 0) for item in weeks if isinstance(item, dict)]
    max_value = max([1, *submitted, *closed])
    width = 620
    height = 180
    pad_left = 40
    pad_right = 18
    pad_top = 12
    pad_bottom = 34
    plot_width = width - pad_left - pad_right
    plot_height = height - pad_top - pad_bottom

    def x_for(index: int) -> float:
        if len(weeks) == 1:
            return pad_left + plot_width / 2
        return pad_left + (plot_width * index) / (len(weeks) - 1)

    def y_for(value: int) -> float:
        return pad_top + plot_height - (plot_height * value) / max_value

    submitted_points = " ".join(f"{x_for(index)},{y_for(value)}" for index, value in enumerate(submitted))
    closed_points = " ".join(f"{x_for(index)},{y_for(value)}" for index, value in enumerate(closed))
    y_ticks = sorted({0, int((max_value + 1) / 2), max_value})
    x_label_step = 2 if len(weeks) > 8 else 1

    return (
        '<div class="ticket-trend-legend">'
        '<span class="ticket-trend-legend-item"><i class="ticket-trend-legend-swatch submitted"></i>每周提交</span>'
        '<span class="ticket-trend-legend-item"><i class="ticket-trend-legend-swatch closed"></i>每周关闭</span>'
        '</div>'
        f'<svg class="ticket-trend-svg" viewBox="0 0 {width} {height}" role="img" aria-label="每周 Ticket 提交和关闭趋势图">'
        + "".join(
            f'<line class="ticket-trend-grid" x1="{pad_left}" y1="{y_for(tick)}" x2="{width - pad_right}" y2="{y_for(tick)}"></line>'
            f'<text class="ticket-trend-label" x="{pad_left - 8}" y="{y_for(tick) + 4}" text-anchor="end">{tick}</text>'
            for tick in y_ticks
        )
        + f'<line class="ticket-trend-axis" x1="{pad_left}" y1="{pad_top}" x2="{pad_left}" y2="{pad_top + plot_height}"></line>'
        + f'<line class="ticket-trend-axis" x1="{pad_left}" y1="{pad_top + plot_height}" x2="{width - pad_right}" y2="{pad_top + plot_height}"></line>'
        + f'<polyline class="ticket-trend-line submitted" points="{submitted_points}"></polyline>'
        + f'<polyline class="ticket-trend-line closed" points="{closed_points}"></polyline>'
        + "".join(
            f'<circle class="ticket-trend-dot submitted" cx="{x_for(index)}" cy="{y_for(submitted[index])}" r="3.5"></circle>'
            f'<circle class="ticket-trend-dot closed" cx="{x_for(index)}" cy="{y_for(closed[index])}" r="3.5"></circle>'
            + (f'<text class="ticket-trend-label" x="{x_for(index)}" y="{height - 10}" text-anchor="middle">{_esc(str((item or {}).get("label") or ""))}</text>' if index % x_label_step == 0 or index == len(weeks) - 1 else "")
            for index, item in enumerate(weeks)
            if isinstance(item, dict)
        )
        + '</svg>'
    )


def _render_dashboard_startup_logs_text(data: dict[str, Any]) -> str:
    startup = data.get("startup") or {}
    logs = startup.get("logs") if isinstance(startup, dict) else []
    logs = logs if isinstance(logs, list) else []
    status = str((startup or {}).get("status") or "unknown")
    checked_at = str((startup or {}).get("last_checked_at") or "")
    head = f"status={status}{f' | checked_at={checked_at}' if checked_at else ''}"
    if not logs:
        return f"{head}\n暂无日志"
    return f"{head}\n" + "\n".join(str(line or "") for line in logs)


def _ticket_badge_class(prefix: str, value: object) -> str:
    suffix = _css_token(value)
    return f"{prefix}-{suffix}" if suffix else ""


def _render_tickets_meta_text(payload: dict[str, Any]) -> str:
    items = payload.get("items") if isinstance(payload, dict) else []
    count = payload.get("count") if isinstance(payload, dict) else 0
    return f"当前 {_fmt_n(count or (len(items) if isinstance(items, list) else 0))} 条 ticket"


def _render_tickets_list_html(payload: dict[str, Any]) -> str:
    items = payload.get("items") if isinstance(payload, dict) else []
    if not isinstance(items, list) or not items:
        return '<div class="ticket-empty-state">当前筛选下暂无 ticket</div>'
    parts: list[str] = []
    for ticket in items:
        if not isinstance(ticket, dict):
            continue
        ticket_id = str(ticket.get("ticket_id") or "")
        title = str(ticket.get("title") or "未命名 Ticket")
        status = str(ticket.get("status") or "open")
        priority = str(ticket.get("priority") or "medium")
        domain = str(ticket.get("domain") or "—")
        category = str(ticket.get("category") or "—")
        updated = str(ticket.get("updated_at") or ticket.get("created_at") or "")
        parts.append(
            f'<article class="ticket-list-item" data-ticket-id="{_esc(ticket_id)}">'
            '<div class="ticket-list-top"><div>'
            f'<div class="ticket-list-title">{_esc(title)}</div>'
            f'<div class="ticket-list-id">{_esc(ticket_id or "未保存")}</div>'
            f'</div><div class="dashboard-meta">{_esc(updated)}</div></div>'
            '<div class="ticket-badge-row">'
            f'<span class="ticket-badge {_esc(_ticket_badge_class("status", status))}">{_esc(status)}</span>'
            f'<span class="ticket-badge {_esc(_ticket_badge_class("priority", priority))}">{_esc(priority)}</span>'
            f'<span class="ticket-badge">{_esc(domain)}</span>'
            f'<span class="ticket-badge">{_esc(category)}</span>'
            '</div></article>'
        )
    return "".join(parts)


def _summarize_assertion_scope_html(scope: dict[str, Any] | None) -> str:
    checks = scope.get("checks") if isinstance(scope, dict) else []
    if not isinstance(checks, list) or not checks:
        return "<span style=\"color:#5a5f50\">—</span>"
    passed = sum(1 for item in checks if isinstance(item, dict) and item.get("passed"))
    failed = len(checks) - passed
    base = f'<span style="color:{"#8fb08d" if failed == 0 else "#d9a6a6"}">{passed}/{len(checks)}</span>'
    return base if failed == 0 else f'{base} <span style="color:#d9a6a6">失败 {failed}</span>'


def _summarize_assertion_failures_html(scope: dict[str, Any] | None) -> str:
    checks = scope.get("checks") if isinstance(scope, dict) else []
    failed = [item for item in checks if isinstance(item, dict) and not item.get("passed")]
    if not failed:
        return '<span style="color:#8fb08d">全部通过</span>'
    parts: list[str] = []
    for item in failed:
        name = _esc(item.get("name") or "unknown")
        trace_ids = [str(value or "").strip() for value in (item.get("trace_ids") or []) if str(value or "").strip()]
        if not trace_ids:
            parts.append(f'<span class="bm-assert-fail-item">{name}</span>')
            continue
        preview = "".join(f'<span class="bm-assert-fail-trace"><code>{_esc(value)}</code></span>' for value in trace_ids[:2])
        more = f'<span class="bm-assert-fail-more">+{len(trace_ids) - 2} more</span>' if len(trace_ids) > 2 else ""
        parts.append(f'<span class="bm-assert-fail-item"><span>{name}</span>{preview}{more}</span>')
    return " ".join(parts)


def _render_benchmark_last_run_text(prefill: dict[str, Any]) -> str:
    results = ((prefill.get("history") or {}).get("results") or []) if isinstance(prefill, dict) else []
    if not isinstance(results, list) or not results:
        return "从未运行"
    last = results[-1] if isinstance(results[-1], dict) else {}
    summary = last.get("assertion_summary") if isinstance(last.get("assertion_summary"), dict) else None
    assertion_text = f" | 断言 {summary.get('passed', 0)}/{int(summary.get('passed', 0)) + int(summary.get('failed', 0))}" if summary else ""
    return f"上次运行: {str(last.get('timestamp') or '')}{assertion_text}"


def _render_benchmark_case_set_options_html(prefill: dict[str, Any]) -> str:
    case_sets = ((prefill.get("cases") or {}).get("case_sets") or []) if isinstance(prefill, dict) else []
    if not isinstance(case_sets, list) or not case_sets:
        return (
            '<option value="smoke_v1">smoke_v1 / 快速烟测样本池</option>'
            '<option value="regression_v1" selected>regression_v1 / 基线回归样本池</option>'
        )
    parts: list[str] = []
    for item in case_sets:
        if not isinstance(item, dict):
            continue
        case_id = str(item.get("id") or "")
        label = str(item.get("label") or item.get("id") or "")
        selected = " selected" if case_id == "regression_v1" else ""
        desc = f"{case_id} / {label}"
        parts.append(f'<option value="{_esc(case_id)}"{selected}>{_esc(desc)}</option>')
    return "".join(parts)


def _render_benchmark_history_table_html(prefill: dict[str, Any], test_set: str = "rag/short") -> str:
    results = ((prefill.get("history") or {}).get("results") or []) if isinstance(prefill, dict) else []
    if not isinstance(results, list) or not results:
        return '<tbody><tr><td colspan="6" style="text-align:center;color:#7a7f6f">运行测试后查看历史对比数据</td></tr></tbody>'

    module, length = (test_set.split("/", 1) + ["short"])[:2]
    relevant = [row for row in results if isinstance(row, dict) and isinstance(row.get(module), dict) and isinstance((row.get(module) or {}).get("by_length"), dict) and ((row.get(module) or {}).get("by_length") or {}).get(length)]
    if not relevant:
        mod = {"rag": "RAG", "agent": "AGENT", "hybrid": "HYBRID"}.get(module, module.upper())
        len_label = {"short": "短", "medium": "中", "long": "长"}.get(length, length)
        return f'<tbody><tr><td colspan="6" style="text-align:center;color:#7a7f6f">当前测试集（{_esc(mod)} / {_esc(len_label)}查询）暂无数据，请先运行包含该模块的测试</td></tr></tbody>'

    recent = list(reversed(relevant[-5:]))

    def fmt(v: object) -> str:
        try:
            n = float(v)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return "<span style='color:#5a5f50'>—</span>"
        if n <= 0:
            return "<span style='color:#5a5f50'>—</span>"
        if n >= 1:
            text = f"{n:.2f}s"
        elif n >= 0.001:
            text = f"{n * 1000:.1f}ms"
        else:
            text = f"{int(round(n * 1_000_000))}µs"
        return f'<span style="font-variant-numeric:tabular-nums">{text}</span>'

    def fmt_zeroable(v: object) -> str:
        try:
            n = float(v)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return "<span style='color:#5a5f50'>—</span>"
        if n == 0:
            return '<span style="font-variant-numeric:tabular-nums">0ms</span>'
        return fmt(n)

    rag_rows = [
        ("总时长均值", lambda d, _r: fmt(d.get("avg_wall_clock_s"))),
        ("总时长 p95", lambda d, _r: fmt(d.get("p95_wall_clock_s"))),
        ("端到端均值", lambda d, _r: fmt(d.get("avg_elapsed_s"))),
        ("向量召回均值", lambda d, _r: fmt(d.get("avg_total_s"))),
        ("向量召回 p95", lambda d, _r: fmt(d.get("p95_total_s"))),
        ("模型重排均值", lambda d, _r: fmt(d.get("avg_rerank_seconds_s"))),
        ("模型重排 p95", lambda d, _r: fmt(d.get("p95_rerank_seconds_s"))),
    ]
    agent_rows = [
        ("总时长均值", lambda d, _r: fmt(d.get("avg_wall_clock_s"))),
        ("总时长 p95", lambda d, _r: fmt(d.get("p95_wall_clock_s"))),
        ("端到端均值", lambda d, _r: fmt(d.get("avg_elapsed_s") if d.get("avg_elapsed_s") is not None else d.get("avg_wall_clock_s"))),
        ("端到端 p95", lambda d, _r: fmt(d.get("p95_elapsed_s") if d.get("p95_elapsed_s") is not None else d.get("p95_wall_clock_s"))),
        ("向量召回均值", lambda d, _r: fmt(d.get("avg_vector_recall_seconds_s"))),
        ("向量召回 p95", lambda d, _r: fmt(d.get("p95_vector_recall_seconds_s"))),
        ("融合排序均值", lambda d, _r: fmt_zeroable(d.get("avg_rerank_seconds_s"))),
        ("融合排序 p95", lambda d, _r: fmt_zeroable(d.get("p95_rerank_seconds_s"))),
    ]
    row_defs = (rag_rows if module == "rag" else agent_rows) + [
        ("当前集断言", lambda _d, run: _summarize_assertion_scope_html((((run.get("assertions") or {}).get(module) or {}).get("by_length") or {}).get(length)),),
        ("全局断言", lambda _d, run: _summarize_assertion_scope_html((((run.get("assertions") or {}).get(module) or {}).get("global"))),),
        ("当前集失败项", lambda _d, run: _summarize_assertion_failures_html((((run.get("assertions") or {}).get(module) or {}).get("by_length") or {}).get(length)),),
    ]

    head_cells: list[str] = []
    for index, run in enumerate(recent):
        ts = str(run.get("timestamp") or "").replace("T", " ")[5:16]
        mods = "+".join(
            {"rag": "RAG", "agent": "AGENT", "hybrid": "HYBRID"}.get(str(item), str(item).upper())
            for item in ((run.get("config") or {}).get("modules") or [])
        )
        latest_class = ' class="bm-latest"' if index == 0 else ""
        head_cells.append(
            f'<th{latest_class}>{_esc(ts)}<br><small style="font-weight:normal;color:#9a9f8f">{_esc(mods)}</small></th>'
        )
    thead = '<thead><tr><th style="min-width:120px">指标</th>' + "".join(head_cells) + '</tr></thead>'
    rows = []
    for label, getter in row_defs:
        cell_class = "benchmark-assert-cell" if ("断言" in label or "失败项" in label) else "benchmark-metric-cell"
        cells = []
        for run in recent:
            module_payload = run.get(module) if isinstance(run.get(module), dict) else {}
            by_length = module_payload.get("by_length") if isinstance(module_payload, dict) else {}
            data = by_length.get(length) if isinstance(by_length, dict) else {}
            cells.append(f'<td class="{cell_class}">{getter(data if isinstance(data, dict) else {}, run)}</td>')
        rows.append(f'<tr><td style="white-space:nowrap;color:#b0b89a;font-size:0.82em">{_esc(label)}</td>{"".join(cells)}</tr>')
    return f'{thead}<tbody>{"".join(rows)}</tbody>'


def _get_dashboard_prefill() -> dict[str, Any]:
    """Return the dashboard SSR payload.

    Prefer the full overview so the first HTML already contains the latency,
    observability, startup, and ticket insight blocks. If that build fails,
    fall back to the lighter core payload plus lightweight extras.
    """
    try:
        return get_dashboard_overview(force=False)
    except Exception:
        base = get_dashboard_overview_core()
        prefill: dict[str, Any] = dict(base)
        if "startup" not in prefill:
            try:
                prefill["startup"] = _load_startup_status_cached()
            except Exception:
                pass
        if "ticket_weekly_stats" not in prefill:
            try:
                prefill["ticket_weekly_stats"] = build_ticket_weekly_stats(weeks=12)
            except Exception:
                pass
        return prefill


def _tickets_prefill_data() -> dict[str, Any]:
    """Return the default non_closed ticket list for SSR JSON embedding.

    Matches the shape returned by GET /api/dashboard/tickets so that
    bootstrapTicketsTab() can consume it directly on first paint without
    making a network request.
    """
    try:
        items = list_tickets(status="non_closed", limit=200, sort="updated_desc")
        all_items = list_tickets(limit=5000, sort="updated_desc")
        return {
            "ok": True,
            "count": len(items),
            "items": items,
            "filters": build_ticket_facets(all_items),
            "applied_filters": {
                "status": "non_closed",
                "priority": "all",
                "domain": "all",
                "category": "all",
                "search": "",
                "created_from": "",
                "created_to": "",
                "sort": "updated_desc",
            },
        }
    except Exception:
        return {"ok": False, "count": 0, "items": [], "filters": {}, "applied_filters": {}}


def _benchmark_prefill_data() -> dict[str, Any]:
    """Return benchmark history + case sets for SSR JSON embedding.

    Lets bootstrapBenchmarkTab() render the history table and populate the
    case-set select on first paint, before any network requests fire.
    """
    try:
        from web.api.benchmark import _load_history as _bm_load_history
        from web.api.benchmark import get_benchmark_case_sets as _bm_cases
        return {
            "history": {"results": _bm_load_history()},
            "cases": _bm_cases(),
        }
    except Exception:
        return {"history": {"results": []}, "cases": {"case_sets": [], "chains": []}}


def _agent_sessions_html_ssr(max_sessions: int = 30) -> str:
    """Return pre-rendered <li> session items for the initial page load.

    Reads only the summary fields (id, title, updated_at) from each session
    file, avoiding the heavy message payloads loaded by list_sessions().
    """
    sessions_dir = APP_DIR.parent / "data" / "agent_sessions"
    if not sessions_dir.exists():
        return ""
    rows: list[dict[str, str]] = []
    for path in sessions_dir.glob("session_*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        rows.append({
            "id": str(payload.get("id") or ""),
            "title": str(payload.get("title") or "\u65b0\u4f1a\u8bdd"),
            "updated_at": str(payload.get("updated_at") or ""),
        })
    rows.sort(key=lambda s: s["updated_at"], reverse=True)
    rows = rows[:max_sessions]
    if not rows:
        return ""
    parts = [
        f'<li data-session-id="{_esc(s["id"])}" title="{_esc(s["title"])}">'
        f'<div class="title">{_esc(s["title"])}</div>'
        f'<div class="meta">{_esc(s["updated_at"])}</div>'
        f"</li>"
        for s in rows
    ]
    return "\n".join(parts)


BUG_TICKET_SYNC_SCRIPT = PROJECT_ROOT / "scripts" / "bug_ticket_sync_hook.py"
BUG_TICKET_SYNC_FILE = PROJECT_ROOT / "nav_dashboard" / "data" / "tickets.jsonl"
BUG_TICKET_BACKFILL_STATE_FILE = PROJECT_ROOT / "nav_dashboard" / "data" / "bug_ticket_backfill_state.json"
BUG_TICKET_DEBUG_LOG = PROJECT_ROOT / "nav_dashboard" / "data" / "bug_ticket_hook_debug.jsonl"
_bug_ticket_backfill_stop = threading.Event()
_bug_ticket_backfill_lock = threading.Lock()
_bug_ticket_backfill_thread: threading.Thread | None = None

_BENCHMARK_CASE_QUERIES = {
    str(query or "").strip()
    for case_set in QUERY_CASE_SETS.values()
    for queries in case_set.values()
    for query in queries
    if str(query or "").strip()
}


def _is_benchmark_source(source: str) -> bool:
    return str(source or "").strip().lower().startswith("benchmark")


def _is_benchmark_trace_id(trace_id: str) -> bool:
    return str(trace_id or "").strip().lower().startswith("benchmark_")


def _is_benchmark_case_query(query: str) -> bool:
    return str(query or "").strip() in _BENCHMARK_CASE_QUERIES


def _load_deploy_time() -> str:
    env_value = str(os.getenv("NAV_DASHBOARD_DEPLOYED_AT", "")).strip()
    if env_value:
        return env_value
    try:
        payload = json.loads(DEPLOY_INFO_FILE.read_text(encoding="utf-8")) if DEPLOY_INFO_FILE.exists() else {}
        value = str(payload.get("deployed_at") or "").strip() if isinstance(payload, dict) else ""
        if value:
            return value
    except Exception:
        pass
    return datetime.now().isoformat(timespec="seconds")


class CustomCardPayload(BaseModel):
    title: str = ""
    url: str = ""
    image: str = ""


class UsageAdjustPayload(BaseModel):
    month_web_search_calls: int
    month_deepseek_calls: int


class RuntimeDataCleanupPayload(BaseModel):
    keys: list[str]


class FeedbackPayload(BaseModel):
    source: str = "unknown"
    question: str = ""
    answer: str = ""
    trace_id: str = ""
    session_id: str = ""
    model: str = ""
    search_mode: str = ""
    query_type: str = ""
    metadata: dict[str, Any] = {}


class TicketCreatePayload(BaseModel):
    ticket_id: str = ""
    title: str = ""
    status: str = "open"
    priority: str = "medium"
    domain: str = ""
    category: str = ""
    summary: str = ""
    related_traces: list[str] = []
    repro_query: str = ""
    expected_behavior: str = ""
    actual_behavior: str = ""
    root_cause: str = ""
    fix_notes: str = ""
    additional_notes: str = ""
    created_by: str = "ai"
    updated_by: str = ""


class TicketUpdatePayload(BaseModel):
    title: str | None = None
    status: str | None = None
    priority: str | None = None
    domain: str | None = None
    category: str | None = None
    summary: str | None = None
    related_traces: list[str] | None = None
    repro_query: str | None = None
    expected_behavior: str | None = None
    actual_behavior: str | None = None
    root_cause: str | None = None
    fix_notes: str | None = None
    additional_notes: str | None = None
    updated_by: str | None = "human"


class TicketAIDraftPayload(BaseModel):
    trace_id: str = ""
    title: str = ""
    priority: str = "medium"
    domain: str = ""
    category: str = ""
    summary: str = ""
    related_traces: list[str] = []
    repro_query: str = ""
    expected_behavior: str = ""
    actual_behavior: str = ""
    root_cause: str = ""
    fix_notes: str = ""
    additional_notes: str = ""
    created_by: str = "ai"
    updated_by: str = "ai"


class TicketPastePayload(BaseModel):
    text: str = ""


def _default_custom_cards() -> list[dict[str, str]]:
    cards = [
        {
            "title": "RAG System",
            "url": str(AI_SUMMARY_URL_OVERRIDE or "").strip(),
            "image": "",
        },
        {
            "title": "Library Tracker",
            "url": str(LIBRARY_TRACKER_URL_OVERRIDE or "").strip(),
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


def _is_loopback_host(host: str) -> bool:
    return str(host or "").strip().lower() in {"127.0.0.1", "localhost", "::1"}


def _default_card_port(title: str) -> int | None:
    normalized = str(title or "").strip().lower()
    if normalized == "rag system":
        return 8000
    if normalized == "library tracker":
        return 8091
    return None


def _first_forwarded_value(value: str) -> str:
    return str(value or "").split(",", 1)[0].strip()


def _request_public_origin(request: Request) -> tuple[str, str]:
    forwarded = _first_forwarded_value(request.headers.get("forwarded", ""))
    forwarded_host = ""
    forwarded_proto = ""
    if forwarded:
        for segment in forwarded.split(";"):
            key, _, raw_value = segment.partition("=")
            if not _:
                continue
            normalized_key = key.strip().lower()
            normalized_value = raw_value.strip().strip('"')
            if normalized_key == "host" and not forwarded_host:
                forwarded_host = normalized_value
            elif normalized_key == "proto" and not forwarded_proto:
                forwarded_proto = normalized_value

    host = (
        _first_forwarded_value(request.headers.get("x-forwarded-host", ""))
        or forwarded_host
        or str(request.headers.get("host", "")).strip()
        or str(request.url.netloc or "").strip()
    ).rstrip("/")
    scheme = (
        _first_forwarded_value(request.headers.get("x-forwarded-proto", ""))
        or forwarded_proto
        or str(request.url.scheme or "http").strip()
        or "http"
    ).rstrip(":/")
    forwarded_port = _first_forwarded_value(request.headers.get("x-forwarded-port", ""))
    if host and forwarded_port and ":" not in host and not host.startswith("["):
        host = f"{host}:{forwarded_port}"
    if not host:
        hostname = request.url.hostname or "localhost"
        if request.url.port:
            host = f"{hostname}:{request.url.port}"
        else:
            host = hostname
    return scheme, host


def _rewrite_loopback_url_for_request(raw_url: str, request: Request, fallback_port: int) -> str:
    url_text = str(raw_url or "").strip()
    public_scheme, public_host = _request_public_origin(request)
    public_hostname = urlparse.urlparse(f"//{public_host}").hostname or request.url.hostname or "localhost"
    if not url_text:
        return f"{public_scheme}://{public_hostname}:{int(fallback_port)}/"

    parsed = urlparse.urlparse(url_text)
    if not parsed.scheme or not parsed.hostname:
        return url_text
    if not _is_loopback_host(parsed.hostname):
        return url_text

    target_host = public_hostname or parsed.hostname or "localhost"
    target_scheme = public_scheme or parsed.scheme or "http"
    target_port = parsed.port or int(fallback_port)
    target_path = parsed.path or "/"
    rewritten = parsed._replace(
        scheme=target_scheme,
        netloc=f"{target_host}:{target_port}",
        path=target_path,
    )
    return urlparse.urlunparse(rewritten)


def _browser_custom_cards(request: Request) -> list[dict[str, str]]:
    cards = _load_custom_cards()
    rewritten: list[dict[str, str]] = []
    for item in cards:
        row = _normalize_card(item)
        url_value = row.get("url", "")
        fallback_port = _default_card_port(row.get("title", ""))
        if url_value:
            parsed = urlparse.urlparse(url_value)
            row["url"] = _rewrite_loopback_url_for_request(
                url_value,
                request,
                parsed.port or 80,
            )
        elif fallback_port is not None:
            row["url"] = _rewrite_loopback_url_for_request("", request, fallback_port)
        rewritten.append(row)
    return rewritten


def _bug_ticket_sync_env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("BUG_TICKET_SYNC_FILE", str(BUG_TICKET_SYNC_FILE))
    env.setdefault("BUG_TICKET_BACKFILL_STATE_FILE", str(BUG_TICKET_BACKFILL_STATE_FILE))
    env.setdefault("BUG_TICKET_DEBUG_LOG", str(BUG_TICKET_DEBUG_LOG))
    env.setdefault("BUG_TICKET_INVOCATION_STATE_FILE", str(PROJECT_ROOT / "nav_dashboard" / "data" / "bug_ticket_hook_invocations.json"))
    env.setdefault("BUG_TICKET_HOOK_SOURCE", "dashboard-backfill")
    env.setdefault("BUG_TICKET_CREATED_BY", "ai-backfill")
    env.setdefault("BUG_TICKET_UPDATED_BY", "ai-backfill")
    return env


def _run_bug_ticket_workspace_backfill_once() -> None:
    if not BUG_TICKET_SYNC_SCRIPT.exists():
        return
    try:
        subprocess.run(
            [sys.executable, str(BUG_TICKET_SYNC_SCRIPT), "--scan-workspace-storage"],
            cwd=str(PROJECT_ROOT),
            env=_bug_ticket_sync_env(),
            capture_output=True,
            text=True,
            timeout=90,
            check=False,
        )
    except Exception:
        return


def _bug_ticket_workspace_backfill_loop() -> None:
    _run_bug_ticket_workspace_backfill_once()
    interval_seconds = max(30.0, float(os.getenv("BUG_TICKET_BACKFILL_INTERVAL_SECONDS", "120")))
    while not _bug_ticket_backfill_stop.wait(interval_seconds):
        _run_bug_ticket_workspace_backfill_once()


def _start_bug_ticket_workspace_backfill() -> None:
    global _bug_ticket_backfill_thread

    enabled = str(os.getenv("BUG_TICKET_BACKFILL_ENABLED", "1")).strip().lower() not in {"0", "false", "no"}
    if not enabled or not BUG_TICKET_SYNC_SCRIPT.exists():
        return
    with _bug_ticket_backfill_lock:
        if _bug_ticket_backfill_thread is not None and _bug_ticket_backfill_thread.is_alive():
            return
        _bug_ticket_backfill_stop.clear()
        _bug_ticket_backfill_thread = threading.Thread(
            target=_bug_ticket_workspace_backfill_loop,
            name="bug-ticket-backfill",
            daemon=True,
        )
        _bug_ticket_backfill_thread.start()


def _stop_bug_ticket_workspace_backfill() -> None:
    _bug_ticket_backfill_stop.set()


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
app.add_event_handler("startup", _start_bug_ticket_workspace_backfill)
app.add_event_handler("shutdown", _stop_bug_ticket_workspace_backfill)

# Captured once at process start; stays constant until the next deployment.
_DEPLOY_TIME: str = _load_deploy_time()

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


def _source_display_name(source: str) -> str:
    raw = str(source or "").strip()
    if not raw:
        return "未知来源"
    return SOURCE_LABELS.get(raw.lower(), raw)


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
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        items.append(text)
    return items


def _first_nonempty(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _infer_ticket_domain(trace: dict[str, Any], fallback: str = "") -> str:
    explicit = str(fallback or "").strip().lower()
    if explicit:
        return explicit
    router = trace.get("router") if isinstance(trace.get("router"), dict) else {}
    understanding = trace.get("query_understanding") if isinstance(trace.get("query_understanding"), dict) else {}
    query_type = str(trace.get("query_type", "") or "").strip().lower()
    selected_tool = str(router.get("selected_tool", "") or "").strip().lower()
    lookup_mode = str(understanding.get("lookup_mode", router.get("lookup_mode", "")) or "").strip().lower()
    domain = str(understanding.get("domain", router.get("domain", "")) or "").strip().lower()
    if "media" in query_type or domain == "media" or lookup_mode in {"general_lookup", "entity_lookup", "filter_search", "concept_lookup"}:
        return "media"
    if "web" in selected_tool:
        return "web"
    if "doc" in selected_tool or "rag" in selected_tool or "tech" in query_type:
        return "knowledge"
    return "agent"


def _infer_ticket_category(trace: dict[str, Any], fallback: str = "") -> str:
    explicit = str(fallback or "").strip().lower()
    if explicit:
        return explicit
    taxonomy = trace.get("error_taxonomy") if isinstance(trace.get("error_taxonomy"), dict) else {}
    router = trace.get("router") if isinstance(trace.get("router"), dict) else {}
    primary = str(taxonomy.get("primary_label", "") or "").strip().lower()
    if primary:
        return primary
    decision = str(router.get("decision_category", "") or "").strip().lower()
    if decision:
        return decision
    selected_tool = str(router.get("selected_tool", "") or "").strip().lower()
    if selected_tool:
        return selected_tool
    return "investigation"


def _empty_ticket_draft() -> dict[str, Any]:
    return {
        "ticket_id": "",
        "title": "",
        "status": "open",
        "priority": "medium",
        "domain": "",
        "category": "",
        "summary": "",
        "related_traces": [],
        "repro_query": "",
        "expected_behavior": "",
        "actual_behavior": "",
        "root_cause": "",
        "fix_notes": "",
        "additional_notes": "",
        "created_at": "",
        "updated_at": "",
    }


def _parse_ticket_paste_payload(raw_text: str) -> dict[str, Any]:
    raw = str(raw_text or "").strip()
    if not raw:
        raise ValueError("请先粘贴 BUG-TICKET 文本")

    payload_text = raw
    marker_index = raw.find("BUG-TICKET:")
    if marker_index >= 0:
        payload_text = raw[marker_index + len("BUG-TICKET:"):].strip()

    if not payload_text:
        raise ValueError("未找到 BUG-TICKET JSON 内容")

    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"BUG-TICKET JSON 解析失败: {exc.msg}") from exc

    if not isinstance(payload, dict):
        raise ValueError("BUG-TICKET 内容必须是 JSON 对象")

    ticket = _empty_ticket_draft()
    ticket.update(
        {
            "title": _first_nonempty(payload.get("title"), ticket["title"]),
            "status": _first_nonempty(payload.get("status"), ticket["status"]),
            "priority": _first_nonempty(payload.get("priority"), ticket["priority"]),
            "domain": _first_nonempty(payload.get("domain"), ticket["domain"]),
            "category": _first_nonempty(payload.get("category"), ticket["category"]),
            "summary": _first_nonempty(payload.get("summary"), ticket["summary"]),
            "related_traces": _safe_text_list(payload.get("related_traces")),
            "repro_query": _first_nonempty(payload.get("repro_query"), ticket["repro_query"]),
            "expected_behavior": _first_nonempty(payload.get("expected_behavior"), ticket["expected_behavior"]),
            "actual_behavior": _first_nonempty(payload.get("actual_behavior"), ticket["actual_behavior"]),
            "root_cause": _first_nonempty(payload.get("root_cause"), ticket["root_cause"]),
            "fix_notes": _first_nonempty(payload.get("fix_notes"), ticket["fix_notes"]),
            "additional_notes": _first_nonempty(payload.get("additional_notes"), ticket["additional_notes"]),
        }
    )
    return ticket


def _build_ticket_ai_draft(payload: TicketAIDraftPayload) -> dict[str, Any]:
    trace_id = str(payload.trace_id or "").strip()
    trace = get_trace_record(trace_id) if trace_id else None
    if trace_id and not trace:
        raise HTTPException(status_code=404, detail=f"未找到 trace_id={trace_id} 对应的追踪记录")

    understanding = trace.get("query_understanding") if isinstance(trace, dict) and isinstance(trace.get("query_understanding"), dict) else {}
    router = trace.get("router") if isinstance(trace, dict) and isinstance(trace.get("router"), dict) else {}
    result = trace.get("result") if isinstance(trace, dict) and isinstance(trace.get("result"), dict) else {}
    taxonomy = trace.get("error_taxonomy") if isinstance(trace, dict) and isinstance(trace.get("error_taxonomy"), dict) else {}
    guardrail_flags = trace.get("guardrail_flags") if isinstance(trace, dict) and isinstance(trace.get("guardrail_flags"), dict) else {}
    answer_guardrail_mode = trace.get("answer_guardrail_mode") if isinstance(trace, dict) and isinstance(trace.get("answer_guardrail_mode"), dict) else {}

    related_traces = _safe_text_list(payload.related_traces)
    if trace_id and trace_id not in related_traces:
        related_traces.insert(0, trace_id)

    resolved_question = _first_nonempty(
        payload.repro_query,
        understanding.get("resolved_question"),
        understanding.get("original_question"),
    )
    route = str(router.get("selected_tool", "") or "").strip()
    mode = str(answer_guardrail_mode.get("mode", "") or "").strip()
    mode_reasons = [str(item or "").strip() for item in list(answer_guardrail_mode.get("reasons") or []) if str(item or "").strip()]
    primary_error = str(taxonomy.get("primary_label", "") or "").strip()
    secondary_error = str(taxonomy.get("secondary_label", "") or "").strip()
    no_context_reason = str(result.get("no_context_reason", "") or "").strip()

    priority = str(payload.priority or "medium").strip().lower() or "medium"
    if primary_error in {"answer_restricted", "understanding_ambiguous"}:
        priority = "high"
    if primary_error in {"answer_incorrect_entity", "retrieval_empty_after_validation"}:
        priority = "critical"

    summary = _first_nonempty(
        payload.summary,
        f"{resolved_question or '未提供 query'} 的回答链路出现异常，route={route or '-'}，mode={mode or 'normal'}。",
    )
    actual_behavior = _first_nonempty(
        payload.actual_behavior,
        "\n".join(
            [
                line for line in [
                    f"复现 query: {resolved_question}" if resolved_question else "",
                    f"实际路由: {route}" if route else "",
                    f"错误分类: {primary_error}" if primary_error else "",
                    f"次级分类: {secondary_error}" if secondary_error else "",
                    f"Guardrail 模式: {mode}" if mode else "",
                    f"Guardrail 原因: {', '.join(mode_reasons)}" if mode_reasons else "",
                    f"未命中原因: {no_context_reason}" if no_context_reason else "",
                ] if line
            ]
        ),
    )
    root_cause = _first_nonempty(
        payload.root_cause,
        "; ".join(
            [
                item for item in [
                    primary_error,
                    secondary_error,
                    ", ".join(sorted([key for key, enabled in guardrail_flags.items() if enabled])),
                ] if item
            ]
        ),
    )
    expected_behavior = _first_nonempty(
        payload.expected_behavior,
        "应正确理解用户问题与上下文，选择匹配的检索/工具链路，并返回与实体、时间窗、过滤条件一致的结果。",
    )
    title = _first_nonempty(
        payload.title,
        resolved_question,
        actual_behavior.splitlines()[0] if actual_behavior else "",
        summary,
    )[:120]

    return {
        "ticket_id": "",
        "title": title or "未命名 Ticket",
        "status": "open",
        "priority": priority,
        "domain": _infer_ticket_domain(trace or {}, payload.domain),
        "category": _infer_ticket_category(trace or {}, payload.category),
        "summary": summary,
        "related_traces": related_traces,
        "repro_query": resolved_question,
        "expected_behavior": expected_behavior,
        "actual_behavior": actual_behavior,
        "root_cause": root_cause,
        "fix_notes": str(payload.fix_notes or "").strip(),
        "additional_notes": str(payload.additional_notes or "").strip(),
        "created_by": str(payload.created_by or "ai").strip() or "ai",
        "updated_by": str(payload.updated_by or "ai").strip() or "ai",
    }


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(PROJECT_ROOT)).replace("\\", "/")
    except Exception:
        return str(path).replace("\\", "/")


def _sqlite_family(path: Path) -> list[Path]:
    return [path, path.with_name(path.name + "-wal"), path.with_name(path.name + "-shm")]


def _runtime_data_targets() -> list[dict[str, Any]]:
    ai_data = PROJECT_ROOT / "ai_conversations_summary" / "data"
    cache_dir = ai_data / "cache"
    return [
        {
            "key": "benchmark_history_current",
            "label": "Benchmark 历史",
            "description": "Dashboard 当前基准测试结果，仅保留最近 5 次。",
            "paths": [PROJECT_ROOT / "nav_dashboard" / "data" / "benchmark" / "results.json"],
            "mode": "reset_benchmark_history",
        },
        {
            "key": "benchmark_history_legacy",
            "label": "旧版 Benchmark 结果",
            "description": "遗留的根目录 benchmark_results.json，不再被当前 Dashboard 使用。",
            "paths": [PROJECT_ROOT / "data" / "benchmark_results.json"],
            "mode": "delete_paths",
        },
        {
            "key": "rag_debug_records",
            "label": "RAG Debug 记录",
            "description": "ai_conversations_summary 调试输出与排错快照。",
            "paths": [ai_data / "rag_sessions" / "debug_data"],
            "mode": "clear_dir_contents",
        },
        {
            "key": "missing_query_log",
            "label": "检索未命中日志",
            "description": "最近未命中 query 的 JSONL 日志。",
            "paths": [cache_dir / "no_context_queries.jsonl"],
            "mode": "truncate_file",
        },
        {
            "key": "retrieval_metrics",
            "label": "RAG 检索统计",
            "description": "RAG rolling latency 指标文件，Dashboard 只取最近 20 条。",
            "paths": [ai_data / "rag_sessions" / "retrieval_metrics.json"],
            "mode": "delete_paths",
        },
        {
            "key": "agent_metrics",
            "label": "Agent 统计",
            "description": "Agent rolling metrics 文件，Dashboard 只取最近 20 条。",
            "paths": [PROJECT_ROOT / "nav_dashboard" / "data" / "agent_metrics.json"],
            "mode": "delete_paths",
        },
        {
            "key": "chat_feedback",
            "label": "聊天反馈",
            "description": "用户手动标记的 Agent / RAG 回答反馈。",
            "paths": [PROJECT_ROOT / "nav_dashboard" / "data" / "chat_feedback.json"],
            "mode": "delete_paths",
        },
        {
            "key": "trace_records",
            "label": "Trace 查询记录",
            "description": "Dashboard trace_id 查询使用的轻量追踪摘要。",
            "paths": list_trace_record_paths(),
            "mode": "delete_paths",
        },
        {
            "key": "ticket_records",
            "label": "Ticket 事件记录",
            "description": "Dashboard 内置 Jira-like ticket append-only 事件日志。",
            "paths": list_ticket_storage_paths(),
            "mode": "delete_paths",
        },
        {
            "key": "web_cache",
            "label": "Web 搜索缓存",
            "description": "Tavily 搜索缓存数据库，TTL 7 天。",
            "paths": _sqlite_family(cache_dir / "web_cache.db"),
            "mode": "delete_paths",
        },
        {
            "key": "embed_cache",
            "label": "Embedding 缓存",
            "description": "文本 embedding 缓存数据库，可手动清空后重建。",
            "paths": _sqlite_family(cache_dir / "embed_cache.db"),
            "mode": "delete_paths",
        },
        {
            "key": "legacy_ai_summary_vector_db",
            "label": "旧索引目录",
            "description": "ai_conversations_summary/data/vector_db 旧索引目录，当前默认索引已切到 core_service/data/vector_db。",
            "paths": [ai_data / "vector_db"],
            "mode": "delete_paths",
        },
        {
            "key": "raw_dir",
            "label": "原始导入目录",
            "description": "批处理分类归档后的原始文件缓存。",
            "paths": [ai_data / "raw_dir"],
            "mode": "clear_dir_contents",
        },
        {
            "key": "extracted_dir",
            "label": "解包中间目录",
            "description": "抽取/解包后的中间文件。",
            "paths": [ai_data / "extracted_dir"],
            "mode": "clear_dir_contents",
        },
        {
            "key": "split_dir",
            "label": "切分中间目录",
            "description": "文档切分阶段的中间产物。",
            "paths": [ai_data / "split_dir"],
            "mode": "clear_dir_contents",
        },
        {
            "key": "summarize_dir",
            "label": "摘要中间目录",
            "description": "批处理摘要阶段的中间产物。",
            "paths": [ai_data / "summarize_dir"],
            "mode": "clear_dir_contents",
        },
        {
            "key": "deepseek_api_audit",
            "label": "DeepSeek 审计缓存",
            "description": "DeepSeek API 调用审计与调试文件。",
            "paths": [ai_data / "deepseek_api_audit"],
            "mode": "clear_dir_contents",
        },
    ]


def _measure_path(path: Path) -> tuple[int, int, bool]:
    if not path.exists():
        return 0, 0, False
    if path.is_file():
        try:
            return int(path.stat().st_size), 1, True
        except Exception:
            return 0, 1, True

    total_bytes = 0
    total_files = 0
    for child in path.rglob("*"):
        if not child.is_file():
            continue
        total_files += 1
        try:
            total_bytes += int(child.stat().st_size)
        except Exception:
            continue
    return total_bytes, total_files, True


def _collect_runtime_data_items() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for target in _runtime_data_targets():
        total_bytes = 0
        file_count = 0
        existing_paths = 0
        for path in target["paths"]:
            size_bytes, files, exists = _measure_path(path)
            total_bytes += size_bytes
            file_count += files
            existing_paths += 1 if exists else 0
        items.append(
            {
                "key": str(target["key"]),
                "label": str(target["label"]),
                "description": str(target["description"]),
                "size_bytes": int(total_bytes),
                "size_mb": round(total_bytes / (1024 * 1024), 3),
                "file_count": int(file_count),
                "existing_paths": int(existing_paths),
                "paths": [_display_path(path) for path in target["paths"]],
            }
        )
    items.sort(key=lambda item: (int(item.get("size_bytes", 0)), int(item.get("file_count", 0))), reverse=True)
    return items


def _runtime_data_summary(include_items: bool = False) -> dict[str, Any]:
    items = _collect_runtime_data_items()
    total_bytes = sum(int(item.get("size_bytes", 0) or 0) for item in items)
    nonzero = sum(1 for item in items if int(item.get("size_bytes", 0) or 0) > 0)
    payload: dict[str, Any] = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "item_count": len(items),
        "nonzero_items": nonzero,
        "clearable_items": len(items),
        "total_size_bytes": total_bytes,
        "total_size_mb": round(total_bytes / (1024 * 1024), 3),
    }
    if include_items:
        payload["items"] = items
    return payload


def _clear_runtime_target(target: dict[str, Any]) -> None:
    mode = str(target.get("mode") or "")
    paths = [path for path in target.get("paths", []) if isinstance(path, Path)]

    if mode == "reset_benchmark_history":
        if not paths:
            return
        path = paths[0]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"results": []}, ensure_ascii=False, indent=2), encoding="utf-8")
        return

    if mode == "truncate_file":
        for path in paths:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("", encoding="utf-8")
        return

    if mode == "clear_dir_contents":
        for path in paths:
            path.mkdir(parents=True, exist_ok=True)
            for child in path.iterdir():
                if child.is_dir():
                    shutil.rmtree(child, ignore_errors=False)
                else:
                    child.unlink(missing_ok=True)
        return

    if mode == "delete_paths":
        for path in paths:
            if path.is_dir():
                shutil.rmtree(path, ignore_errors=False)
            else:
                path.unlink(missing_ok=True)
        return

    raise ValueError(f"unsupported runtime cleanup mode: {mode}")


def _resolve_ai_summary_vector_db_dir() -> Path:
    vector_db_env = (os.getenv("AI_SUMMARY_VECTOR_DB_DIR", "") or "").strip()
    if vector_db_env:
        return Path(vector_db_env)
    return PROJECT_ROOT / "core_service" / "data" / "vector_db"


def _count_rag_index_docs() -> tuple[int, int, int]:
    # Respect AI_SUMMARY_VECTOR_DB_DIR env var (mirrors ai_conversations_summary/web/config.py logic)
    vector_db_dir = _resolve_ai_summary_vector_db_dir()
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


def _library_graph_quality(total_items: int, vector_rows: int) -> dict[str, Any]:
    graph_path = PROJECT_ROOT / "library_tracker" / "data" / "vector_db" / "library_knowledge_graph.json"
    graph_data = _safe_load_json(graph_path, default={})
    if not isinstance(graph_data, dict):
        return {
            "item_node_count": 0,
            "processed_item_count": 0,
            "isolated_nodes": 0,
            "isolated_node_rate": None,
            "item_coverage_rate": None,
            "processed_coverage_rate": None,
            "vector_coverage_rate": None,
            "edges_per_node": None,
        }

    nodes = graph_data.get("nodes", {}) if isinstance(graph_data.get("nodes"), dict) else {}
    edges = graph_data.get("edges", []) if isinstance(graph_data.get("edges"), list) else []
    processed = [str(x) for x in graph_data.get("processed_items", []) if str(x).strip()]
    degrees: dict[str, int] = {str(node_id): 0 for node_id in nodes.keys()}
    item_node_count = 0
    for node_id, node in nodes.items():
        if isinstance(node, dict) and str(node.get("type", "")).strip() == "item":
            item_node_count += 1
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        src = str(edge.get("src", "")).strip()
        dst = str(edge.get("dst", "")).strip()
        if src:
            degrees[src] = degrees.get(src, 0) + 1
        if dst:
            degrees[dst] = degrees.get(dst, 0) + 1
    isolated_nodes = sum(1 for value in degrees.values() if int(value) <= 0)
    node_count = len(nodes)
    return {
        "item_node_count": item_node_count,
        "processed_item_count": len(processed),
        "isolated_nodes": isolated_nodes,
        "isolated_node_rate": _safe_div(isolated_nodes, node_count),
        "item_coverage_rate": _safe_div_capped(item_node_count, total_items),
        "processed_coverage_rate": _safe_div_capped(len(processed), total_items),
        "vector_coverage_rate": _safe_div_capped(item_node_count, vector_rows),
        "edges_per_node": _safe_div(len(edges), node_count),
    }


def _rag_graph_counts() -> tuple[int, int]:
    """Return (node_count, edge_count) from the RAG knowledge graph JSON."""
    graph_path = _resolve_ai_summary_vector_db_dir() / "knowledge_graph_rag.json"
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


def _safe_div_capped(numerator: float | int, denominator: float | int, *, max_value: float = 1.0) -> float | None:
    value = _safe_div(numerator, denominator)
    if value is None:
        return None
    try:
        return round(min(max_value, max(0.0, float(value))), 4)
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
        if _is_benchmark_case_query(query):
            continue
        row_source = str(row.get("source", "unknown") or "unknown").strip()
        if _is_benchmark_source(row_source):
            continue
        trace_id = str(row.get("trace_id", "") or "").strip()
        if _is_benchmark_trace_id(trace_id):
            continue
        if source_filter and source_filter not in {"all", "*"} and row_source.lower() != source_filter:
            continue
        top1_score = row.get("top1_score")
        threshold = row.get("threshold")
        reason = str(row.get("reason", "") or "").strip()
        out.append(
            {
                "ts": ts,
                "source": row_source,
                "source_label": _source_display_name(row_source),
                "query": query,
                "top1_score": float(top1_score) if isinstance(top1_score, (int, float)) else None,
                "threshold": float(threshold) if isinstance(threshold, (int, float)) else None,
                "trace_id": trace_id,
                "reason": reason,
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

    def _row_no_context(row: dict[str, Any]) -> int:
        if int(row.get("no_context", 0) or 0) > 0:
            return 1
        if str(row.get("no_context_reason", "") or "").strip():
            return 1
        top1_after = row.get("top1_score_after_rerank")
        threshold = row.get("similarity_threshold")
        try:
            if top1_after is not None and threshold is not None and float(top1_after) < float(threshold):
                return 1
        except Exception:
            pass
        return 0

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
    no_ctx = sum(_row_no_context(r) for r in records)
    rewrite_hits = sum(1 for r in records if float(r.get("query_rewrite_seconds", 0) or 0) > 0)

    # Only use rows with explicit vector-space after-rerank score semantics.
    rerank_pairs = []
    for row in records:
        before = row.get("top1_score_before_rerank")
        after = row.get("top1_score_after_rerank")
        if before is None or after is None:
            continue
        if row.get("top1_rerank_score_after_rerank") is None:
            continue
        try:
            rerank_pairs.append((float(before), float(after)))
        except Exception:
            continue
    before_scores = [before for before, _after in rerank_pairs]
    after_scores = [after for _before, after in rerank_pairs]
    deltas = [after - before for before, after in rerank_pairs]
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
            "no_context_rate": round(sum(_row_no_context(r) for r in prows) / len(prows), 4),
            "embed_cache_hit_rate": round(sum(1 for r in prows if float(r.get("embed_cache_hit", 0) or 0) > 0) / len(prows), 4),
            "web_cache_hit_rate": round(sum(1 for r in prows if float(r.get("web_cache_hit", 0) or 0) > 0) / len(prows), 4),
            "rewrite_seconds": _timing_stats([float(r.get("query_rewrite_seconds", 0) or 0) for r in prows if float(r.get("query_rewrite_seconds", 0) or 0) >= 0]),
            "query_rewrite_rate": round(sum(1 for r in prows if float(r.get("query_rewrite_seconds", 0) or 0) > 0) / len(prows), 4),
        }

    by_search_mode: dict[str, Any] = {}
    for mode in sorted({str(r.get("search_mode", "") or "").strip() for r in records if str(r.get("search_mode", "")).strip()}):
        mrows = [r for r in records if str(r.get("search_mode", "") or "").strip() == mode]
        if not mrows:
            continue
        by_search_mode[mode] = {
            "count": len(mrows),
            "elapsed": _timing_stats([float(r.get("elapsed_seconds", 0) or 0) for r in mrows if float(r.get("elapsed_seconds", 0) or 0) >= 0]),
            "total": _timing_stats([float(r.get("total", 0) or 0) for r in mrows if float(r.get("total", 0) or 0) >= 0]),
            "no_context_rate": round(sum(_row_no_context(r) for r in mrows) / len(mrows), 4),
            "embed_cache_hit_rate": round(sum(1 for r in mrows if float(r.get("embed_cache_hit", 0) or 0) > 0) / len(mrows), 4),
            "web_cache_hit_rate": round(sum(1 for r in mrows if float(r.get("web_cache_hit", 0) or 0) > 0) / len(mrows), 4),
            "query_rewrite_rate": round(sum(1 for r in mrows if float(r.get("query_rewrite_seconds", 0) or 0) > 0) / len(mrows), 4),
        }

    return {
        "records": compact_records,
        "record_count": len(compact_records),
        "stages": stages,
        "rerank_quality": rerank_quality,
        "embed_cache_hit_rate": round(embed_hits / n, 3) if n else None,
        "web_cache_hit_rate": round(web_hits / n, 3) if n else None,
        "query_rewrite_rate": round(rewrite_hits / n, 3) if n else None,
        "no_context_rate": round(no_ctx / n, 3) if n else None,
        "by_profile": by_profile,
        "by_search_mode": by_search_mode,
    }


def _load_agent_metrics_summary() -> dict[str, Any]:
    """Load nav_dashboard per-round agent metrics and compute aggregate stats."""
    path = PROJECT_ROOT / "nav_dashboard" / "data" / "agent_metrics.json"
    payload = _safe_load_json(path, default={})
    rows = [r for r in (payload.get("records") or []) if isinstance(r, dict)]
    rows = rows[-20:]

    def _row_no_context(row: dict[str, Any]) -> int:
        if int(row.get("no_context", 0) or 0) > 0:
            return 1
        if str(row.get("no_context_reason", "") or "").strip():
            return 1
        query_type = str(row.get("query_type", "") or "").strip().upper()
        if int(row.get("rag_used", 0) or 0) <= 0 and query_type in {"TECH_QUERY", "MIXED_QUERY"}:
            return 1
        top1 = row.get("doc_top1_score")
        threshold = row.get("doc_score_threshold")
        try:
            if top1 is not None and threshold is not None and float(top1) < float(threshold):
                return 1
        except Exception:
            pass
        return 0

    n = len(rows)
    if not n:
        return {
            "records": 0,
            "rag_trigger_rate": None,
            "media_trigger_rate": None,
            "web_trigger_rate": None,
            "no_context_rate": None,
            "by_profile": {},
            "by_search_mode": {},
            "by_query_type": {},
        }

    rag_hits = sum(1 for r in rows if int(r.get("rag_used", 0) or 0))
    media_hits = sum(1 for r in rows if int(r.get("media_used", 0) or 0))
    web_hits = sum(1 for r in rows if int(r.get("web_used", 0) or 0))
    no_ctx = sum(_row_no_context(r) for r in rows)

    # Rerank quality delta (same score scale before/after for each chain)
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
            "wall_clock": _timing_stats([float(r.get("wall_clock_seconds", 0) or 0) for r in prows]),
            "no_context_rate": round(sum(_row_no_context(r) for r in prows) / len(prows), 4),
            "embed_cache_hit_rate": round(sum(1 for r in prows if int(r.get("embed_cache_hit", 0) or 0)) / len(prows), 4),
            "query_rewrite_rate": round(sum(1 for r in prows if int(r.get("query_rewrite_hit", 0) or 0)) / len(prows), 4),
            "web_cache_hit_rate": round(sum(1 for r in prows if int(r.get("web_cache_hit", 0) or 0)) / len(prows), 4),
            "rag_trigger_rate": round(sum(1 for r in prows if int(r.get("rag_used", 0) or 0)) / len(prows), 4),
            "web_trigger_rate": round(sum(1 for r in prows if int(r.get("web_used", 0) or 0)) / len(prows), 4),
        }

    by_search_mode: dict[str, Any] = {}
    modes = sorted({str(r.get("search_mode", "") or "").strip() for r in rows if str(r.get("search_mode", "")).strip()})
    for mode in modes:
        mrows = [r for r in rows if str(r.get("search_mode", "") or "").strip() == mode]
        by_search_mode[mode] = {
            "count": len(mrows),
            "wall_clock": _timing_stats([float(r.get("wall_clock_seconds", 0) or 0) for r in mrows]),
            "vector_recall": _timing_stats([float(r.get("vector_recall_seconds", 0) or 0) for r in mrows]),
            "no_context_rate": round(sum(_row_no_context(r) for r in mrows) / len(mrows), 4),
            "embed_cache_hit_rate": round(sum(1 for r in mrows if int(r.get("embed_cache_hit", 0) or 0)) / len(mrows), 4),
            "query_rewrite_rate": round(sum(1 for r in mrows if int(r.get("query_rewrite_hit", 0) or 0)) / len(mrows), 4),
            "web_trigger_rate": round(sum(1 for r in mrows if int(r.get("web_used", 0) or 0)) / len(mrows), 4),
            "rag_trigger_rate": round(sum(1 for r in mrows if int(r.get("rag_used", 0) or 0)) / len(mrows), 4),
        }

    by_query_type: dict[str, Any] = {}
    query_types = sorted({str(r.get("query_type", "") or "").strip() for r in rows if str(r.get("query_type", "")).strip()})
    for query_type in query_types:
        qrows = [r for r in rows if str(r.get("query_type", "") or "").strip() == query_type]
        by_query_type[query_type] = {
            "count": len(qrows),
            "wall_clock": _timing_stats([float(r.get("wall_clock_seconds", 0) or 0) for r in qrows]),
            "vector_recall": _timing_stats([float(r.get("vector_recall_seconds", 0) or 0) for r in qrows]),
            "no_context_rate": round(sum(_row_no_context(r) for r in qrows) / len(qrows), 4),
            "embed_cache_hit_rate": round(sum(1 for r in qrows if int(r.get("embed_cache_hit", 0) or 0)) / len(qrows), 4),
            "query_rewrite_rate": round(sum(1 for r in qrows if int(r.get("query_rewrite_hit", 0) or 0)) / len(qrows), 4),
            "rag_trigger_rate": round(sum(1 for r in qrows if int(r.get("rag_used", 0) or 0)) / len(qrows), 4),
            "web_trigger_rate": round(sum(1 for r in qrows if int(r.get("web_used", 0) or 0)) / len(qrows), 4),
        }

    return {
        "records": n,
        "rag_trigger_rate": round(rag_hits / n, 3) if n else None,
        "media_trigger_rate": round(media_hits / n, 3) if n else None,
        "web_trigger_rate": round(web_hits / n, 3) if n else None,
        "no_context_rate": round(no_ctx / n, 3) if n else None,
        "rerank_quality": rerank_quality,
        "wall_clock": _timing_stats(wall_vals),
        "by_profile": by_profile,
        "by_search_mode": by_search_mode,
        "by_query_type": by_query_type,
    }


def _invalidate_overview_cache() -> None:
    global _overview_cache, _overview_cache_at  # noqa: PLW0603
    with _overview_cache_lock:
        _overview_cache = None
        _overview_cache_at = 0.0


def _cleanup_runtime_data_keys(requested: list[str]) -> dict[str, Any]:
    by_key = {str(item["key"]): item for item in _runtime_data_targets()}
    cleared: list[dict[str, Any]] = []
    failed: list[dict[str, str]] = []
    for key in requested:
        target = by_key.get(key)
        if target is None:
            failed.append({"key": key, "error": "unknown_key"})
            continue
        try:
            before = next((item for item in _collect_runtime_data_items() if item.get("key") == key), None)
            _clear_runtime_target(target)
            after = next((item for item in _collect_runtime_data_items() if item.get("key") == key), None)
            cleared.append(
                {
                    "key": key,
                    "label": str(target.get("label") or key),
                    "before_size_mb": before.get("size_mb") if isinstance(before, dict) else None,
                    "after_size_mb": after.get("size_mb") if isinstance(after, dict) else None,
                }
            )
        except Exception as exc:
            failed.append({"key": key, "error": str(exc)})
    _invalidate_overview_cache()
    return {
        "ok": not failed,
        "cleared": cleared,
        "failed": failed,
        "summary": _runtime_data_summary(include_items=True),
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
def get_custom_cards(request: Request) -> dict[str, Any]:
    return {"cards": _browser_custom_cards(request)}


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
    lib_graph_quality = _library_graph_quality(lib_total, lib_vector_rows)
    rag_graph_nodes, rag_graph_edges = _rag_graph_counts()
    missing_queries = _load_missing_queries(days=30, limit=200)
    feedback_items = list_feedback(limit=200)
    month_web, month_deepseek, quota_daily = _load_monthly_quota_counts()
    session_count, message_count = _agent_session_counts()
    rag_qa_sessions, rag_qa_messages = _rag_qa_session_counts()
    startup = _load_startup_status_cached()
    retrieval_latency = _load_retrieval_latency_summary()
    agent_metrics = _load_agent_metrics_summary()
    runtime_data = _runtime_data_summary(include_items=False)
    ticket_weekly_stats = build_ticket_weekly_stats(weeks=12)

    warnings: list[str] = []
    if startup.get("status") == "unreachable":
        warnings.append("Startup-status 接口不可达")
    if (lib_graph_quality.get("item_coverage_rate") or 0) < 0.7 and lib_total > 0:
        warnings.append("Library Graph 条目覆盖率偏低")
    if (lib_graph_quality.get("isolated_node_rate") or 0) > 0.2:
        warnings.append("Library Graph 孤点率偏高")

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
            "graph_quality": lib_graph_quality,
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
            "agent_rag_trigger_rate": agent_metrics.get("rag_trigger_rate"),
            "agent_media_trigger_rate": agent_metrics.get("media_trigger_rate"),
            "agent_web_trigger_rate": agent_metrics.get("web_trigger_rate"),
            "agent_no_context_rate": agent_metrics.get("no_context_rate"),
        },
        "retrieval_by_profile": retrieval_latency.get("by_profile", {}),
        "retrieval_by_search_mode": retrieval_latency.get("by_search_mode", {}),
        "agent_by_profile": agent_metrics.get("by_profile", {}),
        "agent_by_search_mode": agent_metrics.get("by_search_mode", {}),
        "agent_by_query_type": agent_metrics.get("by_query_type", {}),
        "rerank_quality": {
            "rag": retrieval_latency.get("rerank_quality", {}),
            "agent": agent_metrics.get("rerank_quality", {}),
        },
        "missing_queries_last_30d": {
            "count": len(missing_queries),
            "items": missing_queries,
            "sample_queries": [str(item.get("query", "")) for item in missing_queries[:3]],
        },
        "chat_feedback": {
            "count": len(feedback_items),
            "items": feedback_items[:20],
        },
        "ticket_weekly_stats": ticket_weekly_stats,
        "agent_wall_clock": agent_metrics.get("wall_clock", {}),
        "runtime_data": runtime_data,
        "warnings": warnings,
    }


def _build_overview_core() -> dict[str, Any]:
    """Fast subset of overview: only the counters needed for first-paint cards.

    Skips: latency traces, agent metrics, missing queries, feedback,
    runtime data, ticket trends — those are all deferred to the full overview.
    """
    rag_docs, rag_changed, rag_sources = _count_rag_index_docs()
    lib_total, lib_by_media, lib_vector_rows, lib_graph_nodes, lib_graph_edges, lib_this_year = _library_counts()
    lib_graph_quality = _library_graph_quality(lib_total, lib_vector_rows)
    rag_graph_nodes, rag_graph_edges = _rag_graph_counts()
    month_web, month_deepseek, quota_daily = _load_monthly_quota_counts()
    session_count, message_count = _agent_session_counts()
    rag_qa_sessions, rag_qa_messages = _rag_qa_session_counts()
    startup = _load_startup_status_cached()

    warnings: list[str] = []
    if startup.get("status") == "unreachable":
        warnings.append("Startup-status 接口不可达")
    if (lib_graph_quality.get("item_coverage_rate") or 0) < 0.7 and lib_total > 0:
        warnings.append("Library Graph 条目覆盖率偏低")
    if (lib_graph_quality.get("isolated_node_rate") or 0) > 0.2:
        warnings.append("Library Graph 孤点率偏高")

    return {
        "ok": True,
        "is_core": True,
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
            "graph_quality": lib_graph_quality,
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
        # Heavy fields are absent in core; frontend treats missing keys as {}
        "retrieval_latency": {},
        "cache_stats": {},
        "retrieval_by_profile": {},
        "retrieval_by_search_mode": {},
        "agent_by_profile": {},
        "agent_by_search_mode": {},
        "agent_by_query_type": {},
        "rerank_quality": {},
        "missing_queries_last_30d": {"count": 0, "items": [], "sample_queries": []},
        "chat_feedback": {"count": 0, "items": []},
        "ticket_weekly_stats": {},
        "agent_wall_clock": {},
        "runtime_data": {},
        "warnings": warnings,
    }


# ── Core overview cache (separate from full cache) ────────────────────────────
_OVERVIEW_CORE_CACHE_TTL = float(os.getenv("NAV_DASHBOARD_CORE_CACHE_TTL", "15"))
_overview_core_cache: dict[str, Any] | None = None
_overview_core_cache_at: float = 0.0
_overview_core_cache_lock = threading.Lock()



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


@app.get("/api/dashboard/overview/core")
def get_dashboard_overview_core() -> dict[str, Any]:
    """Fast-path overview: core counters only, no latency/traces/feedback/tickets.

    Served from a 15-second cache.  The frontend calls this on first bootstrap
    to paint cards quickly, then fires the full /api/dashboard/overview in the
    background to fill in the detail tables.
    """
    global _overview_core_cache, _overview_core_cache_at  # noqa: PLW0603
    now = time.monotonic()
    with _overview_core_cache_lock:
        if _overview_core_cache is not None and (now - _overview_core_cache_at) < _OVERVIEW_CORE_CACHE_TTL:
            return _overview_core_cache
    result = _build_overview_core()
    with _overview_core_cache_lock:
        _overview_core_cache = result
        _overview_core_cache_at = time.monotonic()
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
    header = "时间,来源,Top1分数,阈值,Trace ID,原因,Query"
    lines = [header]
    for r in rows:
        query = str(r.get("query", "")).replace('"', '""')
        trace_id = str(r.get("trace_id", "")).replace('"', '""')
        reason = str(r.get("reason", "")).replace('"', '""')
        top1 = "" if r.get("top1_score") is None else str(r.get("top1_score"))
        threshold = "" if r.get("threshold") is None else str(r.get("threshold"))
        lines.append(
            f'{str(r.get("ts", ""))},{str(r.get("source_label", ""))},{top1},{threshold},"{trace_id}","{reason}","{query}"'
        )
    csv_body = "\ufeff" + "\r\n".join(lines)
    return Response(content=csv_body, media_type="text/csv; charset=utf-8")


@app.get("/api/dashboard/feedback")
def get_dashboard_feedback(limit: int = 200, source: str = "all") -> dict[str, Any]:
    items = list_feedback(limit=limit, source=source)
    return {"ok": True, "count": len(items), "source": str(source or "all"), "items": items}


@app.get("/api/dashboard/feedback/export")
def export_dashboard_feedback(limit: int = 5000, source: str = "all") -> Response:
    items = list_feedback(limit=limit, source=source)
    return Response(
        content=json.dumps({"items": items}, ensure_ascii=False, indent=2),
        media_type="application/json; charset=utf-8",
    )


@app.post("/api/dashboard/feedback")
def post_dashboard_feedback(payload: FeedbackPayload) -> dict[str, Any]:
    try:
        item = append_feedback(payload.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _invalidate_overview_cache()
    return {"ok": True, "item": item}


@app.delete("/api/dashboard/feedback")
def clear_dashboard_feedback(source: str = "all") -> dict[str, Any]:
    removed = clear_feedback(source=source)
    _invalidate_overview_cache()
    return {"ok": True, "removed": removed, "source": str(source or "all")}


@app.post("/api/dashboard/tickets/ai-draft")
def build_dashboard_ticket_ai_draft(payload: TicketAIDraftPayload) -> dict[str, Any]:
    draft = _build_ticket_ai_draft(payload)
    return {"ok": True, "ticket": draft}


@app.post("/api/dashboard/tickets/parse")
def parse_dashboard_ticket(payload: TicketPastePayload) -> dict[str, Any]:
    try:
        ticket = _parse_ticket_paste_payload(payload.text)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "ticket": ticket}


@app.get("/api/dashboard/tickets")
def get_dashboard_tickets(
    status: str = "non_closed",
    priority: str = "all",
    domain: str = "all",
    category: str = "all",
    search: str = "",
    created_from: str = "",
    created_to: str = "",
    limit: int = 200,
    sort: str = "updated_desc",
) -> dict[str, Any]:
    items = list_tickets(
        status=status,
        priority=priority,
        domain=domain,
        category=category,
        search=search,
        created_from=created_from,
        created_to=created_to,
        limit=limit,
        sort=sort,
    )
    all_items = list_tickets(limit=5000, sort=sort)
    return {
        "ok": True,
        "count": len(items),
        "items": items,
        "filters": build_ticket_facets(all_items),
        "applied_filters": {
            "status": str(status or "all"),
            "priority": str(priority or "all"),
            "domain": str(domain or "all"),
            "category": str(category or "all"),
            "search": str(search or ""),
            "created_from": str(created_from or ""),
            "created_to": str(created_to or ""),
            "sort": str(sort or "updated_desc"),
        },
    }


@app.post("/api/dashboard/tickets")
def post_dashboard_ticket(payload: TicketCreatePayload) -> dict[str, Any]:
    try:
        item = create_ticket(payload.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _invalidate_overview_cache()
    return {"ok": True, "ticket": item}


@app.get("/api/dashboard/tickets/{ticket_id}")
def get_dashboard_ticket(ticket_id: str) -> dict[str, Any]:
    item = get_ticket(ticket_id)
    if item is None:
        raise HTTPException(status_code=404, detail=f"未找到 ticket_id={ticket_id} 对应的 ticket")
    return {"ok": True, "ticket": item}


@app.patch("/api/dashboard/tickets/{ticket_id}")
def patch_dashboard_ticket(ticket_id: str, payload: TicketUpdatePayload) -> dict[str, Any]:
    try:
        item = update_ticket(ticket_id, payload.model_dump(exclude_none=True))
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if "不存在" in message else 400
        raise HTTPException(status_code=status_code, detail=message) from exc
    _invalidate_overview_cache()
    return {"ok": True, "ticket": item}


@app.delete("/api/dashboard/tickets/{ticket_id}")
def delete_dashboard_ticket(ticket_id: str, deleted_by: str = "human") -> dict[str, Any]:
    try:
        item = delete_ticket(ticket_id, deleted_by=deleted_by)
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if "不存在" in message else 400
        raise HTTPException(status_code=status_code, detail=message) from exc
    _invalidate_overview_cache()
    return {"ok": True, "ticket": item}


@app.get("/api/dashboard/runtime-data")
def get_dashboard_runtime_data() -> dict[str, Any]:
    return {"ok": True, **_runtime_data_summary(include_items=True)}


@app.get("/api/dashboard/trace")
def get_dashboard_trace(trace_id: str) -> dict[str, Any]:
    value = str(trace_id or "").strip()
    if not value:
        raise HTTPException(status_code=400, detail="trace_id is required")
    record = get_trace_record(value)
    if not record:
        raise HTTPException(status_code=404, detail=f"未找到 trace_id={value} 对应的追踪记录")
    return {"ok": True, "trace": record, "export_text": render_trace_export(record)}


@app.get("/api/dashboard/trace/export")
def export_dashboard_trace(trace_id: str) -> Response:
    value = str(trace_id or "").strip()
    if not value:
        raise HTTPException(status_code=400, detail="trace_id is required")
    record = get_trace_record(value)
    if not record:
        raise HTTPException(status_code=404, detail=f"未找到 trace_id={value} 对应的追踪记录")
    return Response(content=render_trace_export(record), media_type="text/plain; charset=utf-8")


@app.post("/api/dashboard/runtime-data/cleanup")
def cleanup_dashboard_runtime_data(payload: RuntimeDataCleanupPayload) -> dict[str, Any]:
    requested = [str(key).strip() for key in payload.keys if str(key).strip()]
    if not requested:
        raise HTTPException(status_code=400, detail="请至少选择一个可清理项")
    return _cleanup_runtime_data_keys(requested)


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
                    trace_id = str(row.get("trace_id", "") or "").strip()
                    if _is_benchmark_source(row_source) or _is_benchmark_trace_id(trace_id) or row_source != source_filter:
                        kept.append(raw)
                NO_CONTEXT_LOG_PATH.write_text(("\n".join(kept) + ("\n" if kept else "")), encoding="utf-8")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    _invalidate_overview_cache()
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


def _library_tracker_internal_base_url() -> str:
    raw = (os.getenv("NAV_DASHBOARD_LIBRARY_TRACKER_INTERNAL_URL", "") or "").strip().rstrip("/")
    if raw:
        return raw
    if LIBRARY_TRACKER_URL_OVERRIDE:
        parsed = urlparse.urlparse(LIBRARY_TRACKER_URL_OVERRIDE)
        if parsed.scheme and parsed.hostname:
            port = parsed.port or 8091
            return f"{parsed.scheme}://127.0.0.1:{port}"
    return f"http://127.0.0.1:{LIBRARY_TRACKER_DEFAULT_PORT}"

@app.get("/api/startup/status")
def get_startup_status_lean() -> dict[str, Any]:
    s = _load_startup_status_cached()
    return {"ok": True, "status": s.get("status", "unknown"), "last_checked_at": s.get("last_checked_at", "")}


@app.get("/api/dashboard/ontology-status")
def get_ontology_status() -> dict[str, Any]:
    """Return ontology load statuses and propose state for all domains.

    Useful for diagnosing silent ontology fallbacks without needing a live trace.
    """
    from web.services.ontologies.ontology_loader import get_load_statuses as _get_ontology_load_statuses  # noqa: PLC0415
    propose_state: dict[str, Any] = {}
    propose_state_file = APP_DIR / "services" / "ontologies" / "proposed" / "_propose_state.json"
    if propose_state_file.exists():
        try:
            propose_state = json.loads(propose_state_file.read_text(encoding="utf-8"))
        except Exception:
            propose_state = {"error": "could not parse propose state"}
    return {
        "ok": True,
        "load_statuses": _get_ontology_load_statuses(),
        "propose_state": propose_state,
    }


@app.patch("/api/dashboard/usage")
def adjust_dashboard_usage(payload: UsageAdjustPayload) -> dict[str, Any]:
    month_key = datetime.now().strftime("%Y-%m")
    try:
        updated = agent_service._set_monthly_quota_usage(  # noqa: SLF001
            web_search=payload.month_web_search_calls,
            deepseek=payload.month_deepseek_calls,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"ok": True, "month": month_key, **updated}


class UsageRecordPayload(BaseModel):
    web_search_delta: int = 0
    deepseek_delta: int = 0
    count_daily: bool = True


@app.post("/api/dashboard/usage/record")
def record_dashboard_usage(payload: UsageRecordPayload) -> dict[str, Any]:
    """Increment today's quota counters and append to monthly history.
    Called by external services (e.g. RAG QA) to record their API usage.
    """
    web_inc = max(0, int(payload.web_search_delta or 0))
    deepseek_inc = max(0, int(payload.deepseek_delta or 0))
    count_daily = bool(payload.count_daily)
    if web_inc <= 0 and deepseek_inc <= 0:
        return {"ok": True, "skipped": True}
    try:
        if count_daily:
            quota_state = agent_service._load_quota_state()  # noqa: SLF001
            agent_service._increment_quota_state(quota_state, web_search_delta=web_inc, deepseek_delta=deepseek_inc)  # noqa: SLF001
        else:
            agent_service._record_quota_usage(web_search_delta=web_inc, deepseek_delta=deepseek_inc)  # noqa: SLF001
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {
        "ok": True,
        "web_search_delta": web_inc,
        "deepseek_delta": deepseek_inc,
        "count_daily": count_daily,
    }


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
        _invalidate_overview_cache()
        return {"ok": True, "result": result}
    except urlerror.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise HTTPException(status_code=exc.code, detail=detail) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


def _run_library_graph_rebuild(full: bool = False, *, report_progress=None, is_cancelled=None) -> dict[str, Any]:
    base = _library_tracker_internal_base_url().rstrip("/")
    start_url = f"{base}/api/library/graph/rebuild-job" if full else f"{base}/api/library/graph/sync-missing-job"
    try:
        req = urlrequest.Request(
            url=start_url,
            data=b"{}",
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        host = (urlparse.urlparse(start_url).hostname or "").lower()
        local_hosts = {"127.0.0.1", "localhost", "::1"}
        if host in local_hosts:
            opener = urlrequest.build_opener(urlrequest.ProxyHandler({}))
            resp_ctx = opener.open(req, timeout=15)
        else:
            resp_ctx = urlrequest.urlopen(req, timeout=15)

        with resp_ctx as resp:
            started = json.loads(resp.read())
        job = started.get("job", {}) if isinstance(started, dict) else {}
        job_id = str(job.get("id") or "").strip()
        if not job_id:
            raise RuntimeError("Library Graph job did not return job id")

        status_url = f"{base}/api/library/graph/jobs/{urlparse.quote(job_id)}"
        if report_progress is not None:
            report_progress(
                message="Library Graph 任务已启动",
                log=f"Library Graph 后台任务已启动: {job_id}",
                result={"job_id": job_id, "mode": "full" if full else "missing_only"},
            )

        deadline = time.time() + (7200 if full else 3600)
        consecutive_poll_errors = 0
        _MAX_CONSECUTIVE_POLL_ERRORS = 6  # ~30 s of failed polls before giving up
        while True:
            if is_cancelled is not None and is_cancelled():
                return {"ok": False, "cancelled": True, "job_id": job_id, "message": "dashboard polling cancelled"}
            if time.time() > deadline:
                raise TimeoutError("Library Graph 后台任务轮询超时")

            payload = _http_json_get(status_url, timeout=15)
            if not payload.get("ok", True) and "error" in payload and "job" not in payload:
                # Transient poll failure (library tracker unavailable etc.)
                consecutive_poll_errors += 1
                poll_error = str(payload.get("error") or "unknown")
                if consecutive_poll_errors >= _MAX_CONSECUTIVE_POLL_ERRORS:
                    raise RuntimeError(f"Library Graph 状态轮询持续失败（{consecutive_poll_errors} 次）: {poll_error}")
                if report_progress is not None:
                    report_progress(message=f"轮询暂时失败（{consecutive_poll_errors}/{_MAX_CONSECUTIVE_POLL_ERRORS}），重试中…", log=f"Library Graph 状态轮询失败: {poll_error}")
                time.sleep(5)
                continue
            consecutive_poll_errors = 0
            status_job = payload.get("job", {}) if isinstance(payload, dict) else {}
            status = str(status_job.get("status") or "unknown")
            message = str(status_job.get("message") or status)
            if report_progress is not None:
                report_progress(message=message, log=f"Library Graph 状态: {status}")
            if status == "completed":
                _invalidate_overview_cache()
                return {"ok": True, "result": status_job.get("result"), "job": status_job}
            if status == "failed":
                raise RuntimeError(str(status_job.get("error") or status_job.get("message") or "Library Graph job failed"))
            time.sleep(5)
    except urlerror.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise HTTPException(status_code=exc.code, detail=detail) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.post("/api/dashboard/trigger-library-graph-rebuild")
def trigger_library_graph_rebuild(full: bool = False) -> dict[str, Any]:
    return _run_library_graph_rebuild(full=full)


@app.get("/api/dashboard/jobs/{job_id}")
def get_dashboard_job(job_id: str) -> dict[str, Any]:
    job = dashboard_jobs.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"ok": True, "job": job}


@app.get("/api/dashboard/jobs")
def list_dashboard_jobs(job_type: str = "", only_active: bool = False) -> dict[str, Any]:
    return {
        "ok": True,
        "jobs": dashboard_jobs.list_jobs(job_type=str(job_type or "").strip(), only_active=bool(only_active)),
    }


@app.post("/api/dashboard/jobs/{job_id}/cancel")
def cancel_dashboard_job(job_id: str) -> dict[str, Any]:
    job = dashboard_jobs.request_cancel(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"ok": True, "job": job}


@app.post("/api/dashboard/jobs/rag-sync")
def create_rag_sync_job() -> dict[str, Any]:
    def _target(report_progress, is_cancelled):
        if is_cancelled():
            report_progress(message="已取消", log="RAG 同步任务已取消")
            return {"cancelled": True}
        report_progress(message="正在触发 RAG 同步", log="开始触发 RAG 同步")
        result = trigger_rag_sync()
        report_progress(message="RAG 同步已提交", log="RAG 同步已提交", result=result)
        return result

    job = dashboard_jobs.create_job(job_type="rag_sync", label="RAG 增量同步", target=_target)
    return {"ok": True, "job": job}


@app.post("/api/dashboard/jobs/library-graph-rebuild")
def create_library_graph_rebuild_job() -> dict[str, Any]:
    def _target(report_progress, is_cancelled):
        if is_cancelled():
            report_progress(message="已取消", log="Library Graph 重建任务已取消")
            return {"cancelled": True}
        report_progress(message="正在补足 Library Graph 缺失项", log="开始补足 Library Graph 缺失项")
        result = _run_library_graph_rebuild(full=False, report_progress=report_progress, is_cancelled=is_cancelled)
        report_progress(message="Library Graph 补缺已完成", log="Library Graph 补缺已完成", result=result)
        return result

    job = dashboard_jobs.create_job(job_type="library_graph_rebuild", label="Library Graph 补缺", metadata={"mode": "missing_only"}, target=_target)
    return {"ok": True, "job": job}


@app.post("/api/dashboard/jobs/library-graph-rebuild-full")
def create_library_graph_full_rebuild_job() -> dict[str, Any]:
    def _target(report_progress, is_cancelled):
        if is_cancelled():
            report_progress(message="已取消", log="Library Graph 全量重建任务已取消")
            return {"cancelled": True}
        report_progress(message="正在触发 Library Graph 全量重建", log="开始全量重建 Library Graph")
        result = _run_library_graph_rebuild(full=True, report_progress=report_progress, is_cancelled=is_cancelled)
        report_progress(message="Library Graph 全量重建已完成", log="Library Graph 全量重建已完成", result=result)
        return result

    job = dashboard_jobs.create_job(job_type="library_graph_rebuild", label="Library Graph 全量重建", metadata={"mode": "full"}, target=_target)
    return {"ok": True, "job": job}


@app.post("/api/dashboard/jobs/runtime-data-cleanup")
def create_runtime_cleanup_job(payload: RuntimeDataCleanupPayload) -> dict[str, Any]:
    requested = [str(key).strip() for key in payload.keys if str(key).strip()]
    if not requested:
        raise HTTPException(status_code=400, detail="请至少选择一个可清理项")

    def _target(report_progress, is_cancelled):
        if is_cancelled():
            report_progress(message="已取消", log="运行时数据清理已取消")
            return {"cancelled": True}
        report_progress(message=f"正在清理 {len(requested)} 项运行时数据", log=f"开始清理 {len(requested)} 项运行时数据")
        result = _cleanup_runtime_data_keys(requested)
        report_progress(message="运行时数据清理完成", log="运行时数据清理完成", result=result)
        return result

    job = dashboard_jobs.create_job(
        job_type="runtime_cleanup",
        label="运行时数据清理",
        metadata={"keys": requested},
        target=_target,
    )
    return {"ok": True, "job": job}


@app.post("/api/custom_cards/slot/{index}")
def save_custom_card(index: int, payload: CustomCardPayload, background_tasks: BackgroundTasks, request: Request) -> dict[str, Any]:
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
        "cards": _browser_custom_cards(request),
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
    ai_summary_url = _rewrite_loopback_url_for_request(AI_SUMMARY_URL_OVERRIDE, request, 8000)
    library_tracker_url = _rewrite_loopback_url_for_request(LIBRARY_TRACKER_URL_OVERRIDE, request, 8091)
    browser_cards = _browser_custom_cards(request)

    local_model = get_settings().local_llm_model
    deepseek_model = os.getenv("DEEPSEEK_MODEL", "deepseek-chat").strip() or "deepseek-chat"
    # Use the richest available overview for SSR prefill: full cached data
    # when warm (the common case), otherwise augmented core on cold boot.
    dashboard_prefill = _get_dashboard_prefill()
    dashboard_jobs_prefill = list_dashboard_jobs(only_active=True)
    tickets_prefill = _tickets_prefill_data()
    benchmark_prefill = _benchmark_prefill_data()

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "ai_summary_url": ai_summary_url,
            "library_tracker_url": library_tracker_url,
            "local_model": local_model,
            "deepseek_model": deepseek_model,
            "browser_cards": browser_cards,
            "custom_cards_json": json.dumps(browser_cards, ensure_ascii=False),
            "dashboard_prefill_json": json.dumps(dashboard_prefill, ensure_ascii=False),
            "dashboard_prefill": dashboard_prefill,
            "dashboard_grid_html": _render_dashboard_core_cards_html(dashboard_prefill),
            "dashboard_latency_table_html": _render_dashboard_latency_table_html(dashboard_prefill),
            "dashboard_observability_table_html": _render_dashboard_observability_table_html(dashboard_prefill),
            "dashboard_jobs_html": _render_dashboard_jobs_html(dashboard_jobs_prefill),
            "dashboard_ticket_summary_meta": _render_dashboard_ticket_summary_meta_text(dashboard_prefill.get("ticket_weekly_stats") if isinstance(dashboard_prefill, dict) else {}),
            "dashboard_ticket_summary_html": _render_dashboard_ticket_summary_html(dashboard_prefill.get("ticket_weekly_stats") if isinstance(dashboard_prefill, dict) else {}),
            "dashboard_ticket_trend_meta": _render_dashboard_ticket_trend_meta_text(dashboard_prefill.get("ticket_weekly_stats") if isinstance(dashboard_prefill, dict) else {}),
            "dashboard_ticket_trend_html": _render_dashboard_ticket_trend_html(dashboard_prefill.get("ticket_weekly_stats") if isinstance(dashboard_prefill, dict) else {}),
            "dashboard_startup_logs_text": _render_dashboard_startup_logs_text(dashboard_prefill),
            "agent_sessions_html": _agent_sessions_html_ssr(),
            "tickets_meta_text": _render_tickets_meta_text(tickets_prefill),
            "tickets_list_html": _render_tickets_list_html(tickets_prefill),
            "tickets_prefill_json": json.dumps(tickets_prefill, ensure_ascii=False),
            "benchmark_last_run_text": _render_benchmark_last_run_text(benchmark_prefill),
            "benchmark_case_set_options_html": _render_benchmark_case_set_options_html(benchmark_prefill),
            "benchmark_history_table_html": _render_benchmark_history_table_html(benchmark_prefill),
            "benchmark_prefill_json": json.dumps(benchmark_prefill, ensure_ascii=False),
            "js_version": _static_version("app.js"),
            "css_version": _static_version("app.css"),
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
