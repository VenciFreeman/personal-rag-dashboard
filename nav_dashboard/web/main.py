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
    - 快捷卡片增删改（/api/custom_cards/*）：持久化到 app runtime data/nav_dashboard/config/custom_cards.json
  - 图片上传接口（/api/custom_cards/upload）
  - 本月用量手动调整接口（PATCH /api/dashboard/usage）
  - Deploy 时间常量 _DEPLOY_TIME（进程启动时捕获，贯穿整个部署生命周期）
"""
from __future__ import annotations

import asyncio
import calendar
import csv
import io
import json
import math
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib import parse as urlparse
from uuid import uuid4

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from nav_dashboard.web.api.admin import router as admin_router
from nav_dashboard.web.api.agent import router as agent_router
from nav_dashboard.web.api.custom_cards import router as custom_cards_router
from nav_dashboard.web.api.dashboard import get_dashboard_overview_core
from nav_dashboard.web.api.dashboard import list_dashboard_jobs
from nav_dashboard.web.api.dashboard import router as dashboard_router
from nav_dashboard.web.api.benchmark import QUERY_CASE_SETS
from nav_dashboard.web.api.benchmark import router as benchmark_router
from nav_dashboard.web.clients.internal_services import InternalServiceError, ai_summary_internal_base_url, get_json, library_tracker_internal_base_url, request_json
from nav_dashboard.web.config import AI_SUMMARY_URL_OVERRIDE, HOST, JOURNEY_URL_OVERRIDE, LIBRARY_TRACKER_URL_OVERRIDE, PORT, PROPERTY_URL_OVERRIDE
from nav_dashboard.web.services import agent_service
from nav_dashboard.web.services.agent import agent_session_store
from nav_dashboard.web.services.agent.agent_boundaries import no_context_log_path
from nav_dashboard.web.services.dashboard import dashboard_api_owner, dashboard_custom_cards_service, dashboard_jobs, dashboard_overview_service, dashboard_page_service, dashboard_projection_service, dashboard_runtime_data_service, dashboard_ssr_service, dashboard_ticket_service, dashboard_usage_service
from nav_dashboard.web.services.operations import app_control_service, data_backup_service
from nav_dashboard.web.services.runtime_paths import AGENT_METRICS_FILE, BUG_TICKET_BACKFILL_STATE_FILE, BUG_TICKET_DEBUG_LOG, BUG_TICKET_INVOCATION_STATE_FILE, CUSTOM_CARDS_FILE, DEPLOY_INFO_FILE, NOTIFICATION_STATE_FILE, OVERVIEW_SNAPSHOT_FILE, QUOTA_HISTORY_FILE, TICKETS_FILE, ensure_nav_dashboard_runtime_layout

APP_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = APP_DIR.parent.parent
CORE_STATIC_DIR = PROJECT_ROOT / "core_service" / "static"
LIBRARY_TRACKER_DEFAULT_PORT = 8091

from core_service import display_model_name, get_settings
from core_service.auth import install_app_auth
from core_service.feedback import append_feedback, clear_feedback, list_feedback
from core_service.runtime_migration_cli import ensure_runtime_data_migrated
from core_service.tickets import (
    build_ticket_facets,
    build_ticket_weekly_stats,
    create_ticket,
    delete_ticket,
    get_ticket,
    list_ticket_storage_paths,
    list_tickets,
    update_ticket,
)
TEST_NOTIFICATION_KEY = "nav_dashboard_test_notice_v1"
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


def _core_static_version(filename: str) -> str:
    path = CORE_STATIC_DIR / filename
    try:
        return format(int(os.path.getmtime(path)), "x")
    except OSError:
        return "0"


def _overview_has_detail_snapshot(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    for key in ("retrieval_latency", "cache_stats", "ticket_weekly_stats", "agent_wall_clock", "runtime_data", "rerank_quality"):
        value = payload.get(key)
        if isinstance(value, dict) and value:
            return True
    return False


def _load_overview_snapshot() -> dict[str, Any] | None:
    if not OVERVIEW_SNAPSHOT_FILE.exists():
        return None
    try:
        snapshot = json.loads(OVERVIEW_SNAPSHOT_FILE.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(snapshot, dict):
        return None
    snapshot["notifications"] = _build_information_notifications()
    status = snapshot.get("overview_status")
    if not isinstance(status, dict) or str(status.get("mode") or "").strip() in {"", "core"}:
        snapshot["overview_status"] = _overview_status("full", message="详细统计已从最近一次成功快照恢复。")
    return snapshot


def _persist_overview_snapshot(payload: dict[str, Any]) -> None:
    if not _overview_has_detail_snapshot(payload):
        return
    snapshot = dict(payload)
    try:
        OVERVIEW_SNAPSHOT_FILE.parent.mkdir(parents=True, exist_ok=True)
        OVERVIEW_SNAPSHOT_FILE.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def _warm_dashboard_overview_snapshot() -> None:
    global _overview_cache, _overview_cache_at  # noqa: PLW0603
    try:
        if _load_overview_snapshot() is not None:
            return
        result = _build_overview()
    except Exception:
        return
    with _overview_cache_lock:
        _overview_cache = result
        _overview_cache_at = time.monotonic()
    _persist_overview_snapshot(result)


def _start_dashboard_overview_warmup() -> None:
    thread = threading.Thread(
        target=_warm_dashboard_overview_snapshot,
        name="dashboard-overview-warmup",
        daemon=True,
    )
    thread.start()


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
    return dashboard_ssr_service.render_dashboard_core_cards_html(prefill)


def _render_dashboard_latency_table_html(data: dict[str, Any]) -> str:
    return dashboard_ssr_service.render_dashboard_latency_table_html(data)


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
    return dashboard_ssr_service.render_dashboard_observability_table_html(data)


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
    return dashboard_ssr_service.render_dashboard_jobs_html(payload)


def _dashboard_ticket_breakdown_html(counts: dict[str, Any]) -> str:
    if not isinstance(counts, dict) or not counts:
        return '<span class="ticket-empty-state">暂无数据</span>'
    items = sorted(counts.items(), key=lambda item: int(item[1] or 0), reverse=True)
    return "".join(
        f'<span class="dashboard-ticket-chip"><span class="dashboard-ticket-chip-label">{_esc(label or "unknown")}</span><strong>{_esc(_fmt_n(count))}</strong></span>'
        for label, count in items
    )


def _render_dashboard_ticket_summary_meta_text(stats: dict[str, Any]) -> str:
    return dashboard_ssr_service.render_dashboard_ticket_summary_meta_text(stats)


def _render_dashboard_ticket_summary_html(stats: dict[str, Any]) -> str:
    return dashboard_ssr_service.render_dashboard_ticket_summary_html(stats)


def _render_dashboard_ticket_trend_meta_text(stats: dict[str, Any]) -> str:
    return dashboard_ssr_service.render_dashboard_ticket_trend_meta_text(stats)


def _render_dashboard_ticket_trend_html(stats: dict[str, Any]) -> str:
    return dashboard_ssr_service.render_dashboard_ticket_trend_html(stats)


def _render_dashboard_startup_logs_text(data: dict[str, Any]) -> str:
    return dashboard_ssr_service.render_dashboard_startup_logs_text(data)


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
        ("RAG 检索总时长均值", lambda d, _r: fmt(d.get("avg_total_s"))),
        ("RAG 检索总时长 p95", lambda d, _r: fmt(d.get("p95_total_s"))),
        ("模型重排均值", lambda d, _r: fmt(d.get("avg_rerank_seconds_s"))),
        ("模型重排 p95", lambda d, _r: fmt(d.get("p95_rerank_seconds_s"))),
    ]
    agent_rows = [
        ("总时长均值", lambda d, _r: fmt(d.get("avg_wall_clock_s"))),
        ("总时长 p95", lambda d, _r: fmt(d.get("p95_wall_clock_s"))),
        ("端到端均值", lambda d, _r: fmt(d.get("avg_elapsed_s") if d.get("avg_elapsed_s") is not None else d.get("avg_wall_clock_s"))),
        ("端到端 p95", lambda d, _r: fmt(d.get("p95_elapsed_s") if d.get("p95_elapsed_s") is not None else d.get("p95_wall_clock_s"))),
        ("文档检索均值", lambda d, _r: fmt(d.get("avg_vector_recall_seconds_s"))),
        ("文档检索 p95", lambda d, _r: fmt(d.get("p95_vector_recall_seconds_s"))),
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


def _overview_error_entry(section: str, exc: Exception) -> dict[str, str]:
    return {
        "section": str(section or "unknown"),
        "type": type(exc).__name__,
        "message": str(exc) or type(exc).__name__,
    }


def _overview_status(mode: str, *, errors: list[dict[str, str]] | None = None, message: str = "") -> dict[str, Any]:
    items = list(errors or [])
    return {
        "mode": str(mode or "unknown"),
        "message": str(message or "").strip(),
        "error_count": len(items),
        "errors": items,
        "heavy_sections_ready": mode == "full" and not items,
    }


def _run_overview_section(section: str, builder: Any, default: Any) -> tuple[Any, dict[str, str] | None]:
    try:
        return builder(), None
    except Exception as exc:  # noqa: BLE001
        return default, _overview_error_entry(section, exc)


def _build_failed_overview(exc: Exception) -> dict[str, Any]:
    error_entry = _overview_error_entry("full_overview", exc)
    try:
        fallback = _build_overview_core()
    except Exception:  # noqa: BLE001
        fallback = {
            "ok": False,
            "is_core": True,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "deployed_at": _DEPLOY_TIME,
            "month": datetime.now().strftime("%Y-%m"),
            "warnings": [],
            "notifications": _build_information_notifications(),
        }
    fallback["ok"] = False
    fallback["is_core"] = True
    fallback["overview_status"] = _overview_status(
        "core_fallback",
        errors=[error_entry],
        message="详细统计构建失败，当前回退到核心指标。",
    )
    return fallback


def _get_dashboard_prefill() -> dict[str, Any]:
    """Return the dashboard SSR payload.

    Prefer a warm full overview cache so the first HTML already contains the
    current dashboard values without doing fresh heavy work on every request.
    Fall back to the lighter core overview only when no full cache snapshot is
    ready yet.
    """
    try:
        with _overview_cache_lock:
            cached_full = dict(_overview_cache) if isinstance(_overview_cache, dict) else None
        if cached_full:
            prefill = cached_full
        else:
            snapshot = _load_overview_snapshot()
            prefill = dict(snapshot) if snapshot else dict(get_dashboard_overview_core())
        prefill["notifications"] = _build_information_notifications()
        return prefill
    except Exception as exc:
        base = {"ok": True, "notifications": _build_information_notifications()}
        prefill: dict[str, Any] = dict(base)
        prefill["notifications"] = _build_information_notifications()
        prefill["overview_status"] = _overview_status(
            "prefill_error",
            errors=[_overview_error_entry("dashboard_prefill", exc)],
            message="Dashboard 预填充失败，页面将等待前端重新拉取。",
        )
        if "startup" not in prefill:
            try:
                prefill["startup"] = _load_startup_status_cached()
            except Exception:
                pass
        if "ticket_weekly_stats" not in prefill:
            try:
                prefill["ticket_weekly_stats"] = build_ticket_weekly_stats(weeks=12, from_first_ticket=True)
            except Exception:
                pass
        return prefill


def _tickets_prefill_data() -> dict[str, Any]:
    """Return the default full ticket history for SSR JSON embedding.

    Matches the shape returned by GET /api/dashboard/tickets so that
    bootstrapTicketsTab() can consume it directly on first paint without
    making a network request.
    """
    try:
        week_ago, today = _default_ticket_date_range()
        default_status = dashboard_ticket_service.resolve_ticket_default_status(
            created_from=week_ago.isoformat(),
            created_to=today.isoformat(),
            ticket_loader=list_tickets,
        )
        items = list_tickets(
            status=default_status,
            created_from=week_ago.isoformat(),
            created_to=today.isoformat(),
            limit=200,
            sort="updated_desc",
        )
        all_items = list_tickets(limit=5000, sort="updated_desc")
        return {
            "ok": True,
            "count": len(items),
            "items": items,
            "filters": build_ticket_facets(all_items),
            "applied_filters": {
                "status": default_status,
                "priority": "all",
                "domain": "all",
                "category": "all",
                "search": "",
                "created_from": week_ago.isoformat(),
                "created_to": today.isoformat(),
                "sort": "updated_desc",
            },
        }
    except Exception:
        return {"ok": False, "count": 0, "items": [], "filters": {}, "applied_filters": {}}


def _default_ticket_date_range() -> tuple[date, date]:
    return dashboard_ticket_service.default_ticket_date_range(today=date.today())


def _benchmark_prefill_data() -> dict[str, Any]:
    return dashboard_page_service.build_benchmark_prefill_data()


def _agent_sessions_html_ssr(max_sessions: int = 30) -> str:
    return agent_session_store.render_session_list_items_html(max_sessions=max_sessions)


def _agent_sessions_json_ssr(max_sessions: int = 30) -> str:
    return agent_session_store.sessions_json_payload(max_sessions=max_sessions)


BUG_TICKET_SYNC_SCRIPT = PROJECT_ROOT / "scripts" / "bug_ticket_sync_hook.py"
BUG_TICKET_SYNC_FILE = TICKETS_FILE
_bug_ticket_backfill_stop = threading.Event()
_bug_ticket_backfill_lock = threading.Lock()
_bug_ticket_backfill_thread: threading.Thread | None = None

def _benchmark_case_queries() -> set[str]:
    return {
        str(query or "").strip()
        for case_set in QUERY_CASE_SETS.values()
        for queries in case_set.values()
        for query in queries
        if str(query or "").strip()
    }


def _is_benchmark_source(source: str) -> bool:
    return str(source or "").strip().lower().startswith("benchmark")


def _is_agent_source(source: str) -> bool:
    return str(source or "").strip().lower().startswith("agent")


def _is_benchmark_trace_id(trace_id: str) -> bool:
    return str(trace_id or "").strip().lower().startswith("benchmark_")


def _is_benchmark_case_query(query: str) -> bool:
    return str(query or "").strip() in _benchmark_case_queries()


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


class UsageEventPayload(BaseModel):
    provider: str
    feature: str = ""
    page: str = ""
    source: str = ""
    message: str = ""
    trace_id: str = ""
    session_id: str = ""
    count: int = 1


class NotificationDismissPayload(BaseModel):
    key: str


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


class LibraryAliasProposalReviewPayload(BaseModel):
    proposal_id: str
    action: str
    canonical_name: str = ""
    aliases: list[str] = Field(default_factory=list)


def _default_custom_cards() -> list[dict[str, str]]:
    return dashboard_custom_cards_service.default_custom_cards()


def _normalize_card(item: object) -> dict[str, str]:
    return dashboard_custom_cards_service.normalize_card(item)


def _save_custom_cards(cards: list[dict[str, str]]) -> list[dict[str, str]]:
    return dashboard_custom_cards_service.save_custom_cards(cards)


def _load_custom_cards() -> list[dict[str, str]]:
    return dashboard_custom_cards_service.load_custom_cards()


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_notification_state() -> dict[str, Any]:
    payload = _load_json_object(NOTIFICATION_STATE_FILE)
    dismissed = payload.get("dismissed")
    return {"dismissed": dismissed if isinstance(dismissed, dict) else {}}


def _save_notification_state(payload: dict[str, Any]) -> None:
    NOTIFICATION_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    NOTIFICATION_STATE_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _clear_dashboard_overview_cache() -> None:
    global _overview_cache, _overview_cache_at, _overview_core_cache, _overview_core_cache_at  # noqa: PLW0603
    with _overview_cache_lock:
        _overview_cache = None
        _overview_cache_at = 0.0
    with _overview_core_cache_lock:
        _overview_core_cache = None
        _overview_core_cache_at = 0.0
def _append_notification(
    notices: list[dict[str, str]],
    dismissed: dict[str, Any],
    *,
    key: str,
    title: str,
    message: str,
    level: str = "info",
    target: str = "",
    tab: str = "",
    report_kind: str = "",
    period_key: str = "",
    source: str = "",
    snapshot_month: str = "",
    snapshot_action: str = "",
) -> None:
    if not key or key in dismissed:
        return
    notices.append(
        {
            "key": key,
            "title": title,
            "message": message,
            "level": level,
            "target": target,
            "tab": tab,
            "report_kind": report_kind,
            "period_key": period_key,
            "source": source,
            "snapshot_month": snapshot_month,
            "snapshot_action": snapshot_action,
        }
    )
def _build_notice_href(base_url: str, notice: dict[str, Any]) -> str:
    root = str(base_url or "").strip()
    if not root:
        return ""
    query: dict[str, str] = {}
    for key in ("tab", "report_kind", "period_key", "source", "snapshot_month", "snapshot_action"):
        value = str(notice.get(key) or "").strip()
        if value:
            query[key] = value
    return f"{root.rstrip('/')}/?{urlparse.urlencode(query)}" if query else root


def _render_information_notifications_html(
    notices: list[dict[str, Any]],
    library_tracker_url: str,
    property_url: str,
) -> str:
    chunks: list[str] = []
    for notice in notices:
        target = str(notice.get("target") or "").strip()
        target_url = ""
        if target == "library":
            target_url = _build_notice_href(library_tracker_url, notice)
        elif target == "property":
            target_url = _build_notice_href(property_url, notice)
        action_html = (
            f'<a class="information-notice-link" href="{_esc(target_url)}" target="_blank" rel="noopener noreferrer">打开</a>'
            if target_url
            else ""
        )
        chunks.append(
            "".join(
                [
                    f'<div class="information-notice" data-level="{_esc(notice.get("level") or "info")}" data-key="{_esc(notice.get("key") or "")}">',
                    '<div class="information-notice-body">',
                    f'<div class="information-notice-title">{_esc(notice.get("title") or "通知")}</div>',
                    f'<div class="information-notice-message">{_esc(notice.get("message") or "")}</div>',
                    "</div>",
                    action_html,
                    f'<button type="button" class="information-notice-dismiss" data-dismiss-key="{_esc(notice.get("key") or "")}" aria-label="关闭通知">×</button>',
                    "</div>",
                ]
            )
        )
    return "\n".join(chunks)


def _build_information_notifications() -> list[dict[str, str]]:
    state = _load_notification_state()
    dismissed = state.get("dismissed") if isinstance(state.get("dismissed"), dict) else {}
    return dashboard_projection_service.build_information_notifications(
        dismissed,
        test_notification_key=TEST_NOTIFICATION_KEY,
    )


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
    return dashboard_custom_cards_service.browser_custom_cards(request)


def _bug_ticket_sync_env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("BUG_TICKET_SYNC_FILE", str(BUG_TICKET_SYNC_FILE))
    env.setdefault("BUG_TICKET_BACKFILL_STATE_FILE", str(BUG_TICKET_BACKFILL_STATE_FILE))
    env.setdefault("BUG_TICKET_DEBUG_LOG", str(BUG_TICKET_DEBUG_LOG))
    env.setdefault("BUG_TICKET_INVOCATION_STATE_FILE", str(BUG_TICKET_INVOCATION_STATE_FILE))
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
    dashboard_custom_cards_service.trigger_custom_card_compression()


def _ensure_runtime_data_migrated() -> None:
    try:
        ensure_runtime_data_migrated()
        ensure_nav_dashboard_runtime_layout()
    except Exception:
        return

app = FastAPI(title="Nav Dashboard", version="0.1.0")
app.add_event_handler("startup", _ensure_runtime_data_migrated)
app.add_event_handler("startup", _start_bug_ticket_workspace_backfill)
app.add_event_handler("startup", _start_dashboard_overview_warmup)
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
app.mount("/core-static", StaticFiles(directory=str(CORE_STATIC_DIR)), name="core-static")
app.include_router(admin_router)
app.include_router(agent_router)
app.include_router(custom_cards_router)
app.include_router(dashboard_router)
app.include_router(benchmark_router)
install_app_auth(app, app_id="nav_dashboard", app_title="Navigation Dashboard")

@app.on_event("startup")
async def _startup() -> None:
    data_backup_service.start_scheduler()


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


def _parse_ticket_paste_payload(raw_text: str) -> dict[str, Any]:
    return dashboard_ticket_service.parse_ticket_paste_payload(raw_text)


def _build_ticket_ai_draft(payload: TicketAIDraftPayload) -> dict[str, Any]:
    return dashboard_ticket_service.build_ticket_ai_draft(payload)


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(PROJECT_ROOT)).replace("\\", "/")
    except Exception:
        return str(path).replace("\\", "/")


def _sqlite_family(path: Path) -> list[Path]:
    return [path, path.with_name(path.name + "-wal"), path.with_name(path.name + "-shm")]


def _runtime_data_targets() -> list[dict[str, Any]]:
    return dashboard_runtime_data_service.runtime_data_targets()


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
    return dashboard_runtime_data_service.collect_runtime_data_items()


def _runtime_data_summary(include_items: bool = False) -> dict[str, Any]:
    return dashboard_runtime_data_service.runtime_data_summary(include_items=include_items)


def _clear_runtime_target(target: dict[str, Any]) -> None:
    dashboard_runtime_data_service.clear_runtime_target(target)


def _count_rag_index_docs() -> tuple[int, int, int]:
    return dashboard_overview_service.load_rag_index_summary()


def _library_counts() -> tuple[int, dict[str, int], int, int, int, int]:
    return dashboard_overview_service.load_library_summary()


def _load_library_alias_proposal_summary() -> dict[str, Any]:
    return dashboard_overview_service.load_library_alias_proposal_summary()


def _library_graph_quality(total_items: int, vector_rows: int) -> dict[str, Any]:
    return dashboard_overview_service.build_library_graph_quality(
        total_items=total_items,
        vector_rows=vector_rows,
        safe_div=_safe_div,
        safe_div_capped=_safe_div_capped,
    )


def _rag_graph_counts() -> tuple[int, int]:
    return dashboard_overview_service.load_rag_graph_counts()


def _load_monthly_quota_counts() -> tuple[int, int, dict[str, int]]:
    return dashboard_usage_service.load_monthly_quota_counts(history_path=QUOTA_HISTORY_FILE)


def _usage_provider_label(provider: str) -> str:
    return dashboard_usage_service.usage_provider_label(provider)


def _load_usage_traces(days: int = 7, limit: int = 200, provider: str = "all") -> list[dict[str, Any]]:
    return dashboard_usage_service.load_usage_traces(days=days, limit=limit, provider=provider)


def _agent_session_counts() -> tuple[int, int]:
    return agent_session_store.session_activity_counts()


def _rag_qa_session_counts() -> tuple[int, int]:
    return dashboard_overview_service.load_rag_session_summary()


def _ai_summary_internal_base_url() -> str:
    return ai_summary_internal_base_url()


def _http_json_get(url: str, timeout: float = 2.5) -> dict[str, Any]:
    return get_json(url, timeout=timeout)


def _http_json_request(method: str, url: str, payload: dict[str, Any] | None = None, timeout: float = 10.0) -> dict[str, Any]:
    try:
        return request_json(method, url, payload=payload, timeout=timeout, raise_for_status=True)
    except InternalServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


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


def _load_missing_queries(days: int = 30, limit: int = 200, source: str = "") -> list[dict[str, Any]]:
    payload = dashboard_projection_service.load_ai_summary_missing_queries(days=days, limit=limit, source=source)
    rows = payload.get("items") if isinstance(payload.get("items"), list) else []
    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        query = str(row.get("query", "") or "").strip()
        if not query or _is_benchmark_case_query(query):
            continue
        row_source = str(row.get("source", "unknown") or "unknown").strip()
        if _is_benchmark_source(row_source) or _is_agent_source(row_source):
            continue
        trace_id = str(row.get("trace_id", "") or "").strip()
        if _is_benchmark_trace_id(trace_id):
            continue
        top1_score = row.get("top1_score")
        threshold = row.get("threshold")
        out.append(
            {
                "ts": str(row.get("ts", "") or "").strip(),
                "source": row_source,
                "source_label": str(row.get("source_label", "") or _source_display_name(row_source)),
                "query": query,
                "top1_score": float(top1_score) if isinstance(top1_score, (int, float)) else None,
                "threshold": float(threshold) if isinstance(threshold, (int, float)) else None,
                "trace_id": trace_id,
                "reason": str(row.get("reason", "") or "").strip(),
            }
        )
    return out[: max(1, int(limit))]


def _load_retrieval_latency_summary() -> dict[str, Any]:
    payload = dashboard_projection_service.load_ai_summary_dashboard_overview()
    retrieval = payload.get("retrieval_latency") if isinstance(payload.get("retrieval_latency"), dict) else {}
    return retrieval


def _load_agent_metrics_summary() -> dict[str, Any]:
    """Load nav_dashboard per-round agent metrics and compute aggregate stats."""
    path = AGENT_METRICS_FILE
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
    result = dashboard_runtime_data_service.cleanup_runtime_data_keys(requested)
    _invalidate_overview_cache()
    return result


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


def _get_overview_cache_state() -> tuple[dict[str, Any] | None, float]:
    return _overview_cache, _overview_cache_at


def _set_overview_cache_state(value: dict[str, Any], cached_at: float) -> None:
    global _overview_cache, _overview_cache_at  # noqa: PLW0603
    _overview_cache = value
    _overview_cache_at = cached_at


def _build_overview() -> dict[str, Any]:
    """Run all the expensive I/O and return the full overview dict."""
    overview = _build_overview_core()
    overview["ok"] = True
    overview["is_core"] = False
    errors: list[dict[str, str]] = []

    retrieval_latency, error = _run_overview_section("retrieval_latency", _load_retrieval_latency_summary, {})
    if error:
        errors.append(error)
    agent_metrics, error = _run_overview_section("agent_metrics", _load_agent_metrics_summary, {})
    if error:
        errors.append(error)
    runtime_data, error = _run_overview_section("runtime_data", lambda: _runtime_data_summary(include_items=False), {})
    if error:
        errors.append(error)
    data_backups, error = _run_overview_section("data_backups", data_backup_service.backup_summary, {})
    if error:
        errors.append(error)
    ticket_weekly_stats, error = _run_overview_section("ticket_weekly_stats", lambda: build_ticket_weekly_stats(weeks=12, from_first_ticket=True), {})
    if error:
        errors.append(error)
    missing_queries, error = _run_overview_section("missing_queries_last_30d", lambda: _load_missing_queries(days=30, limit=200), [])
    if error:
        errors.append(error)
    feedback_items, error = _run_overview_section("chat_feedback", lambda: list_feedback(limit=200), [])
    if error:
        errors.append(error)

    overview.update(
        {
            "retrieval_latency": retrieval_latency,
            "cache_stats": {
                "rag_embed_cache_hit_rate": retrieval_latency.get("embed_cache_hit_rate") if isinstance(retrieval_latency, dict) else None,
                "rag_web_cache_hit_rate": retrieval_latency.get("web_cache_hit_rate") if isinstance(retrieval_latency, dict) else None,
                "rag_no_context_rate": retrieval_latency.get("no_context_rate") if isinstance(retrieval_latency, dict) else None,
                "agent_rag_trigger_rate": agent_metrics.get("rag_trigger_rate") if isinstance(agent_metrics, dict) else None,
                "agent_media_trigger_rate": agent_metrics.get("media_trigger_rate") if isinstance(agent_metrics, dict) else None,
                "agent_web_trigger_rate": agent_metrics.get("web_trigger_rate") if isinstance(agent_metrics, dict) else None,
                "agent_no_context_rate": agent_metrics.get("no_context_rate") if isinstance(agent_metrics, dict) else None,
            },
            "retrieval_by_profile": retrieval_latency.get("by_profile", {}) if isinstance(retrieval_latency, dict) else {},
            "retrieval_by_search_mode": retrieval_latency.get("by_search_mode", {}) if isinstance(retrieval_latency, dict) else {},
            "agent_by_profile": agent_metrics.get("by_profile", {}) if isinstance(agent_metrics, dict) else {},
            "agent_by_search_mode": agent_metrics.get("by_search_mode", {}) if isinstance(agent_metrics, dict) else {},
            "agent_by_query_type": agent_metrics.get("by_query_type", {}) if isinstance(agent_metrics, dict) else {},
            "rerank_quality": {
                "rag": retrieval_latency.get("rerank_quality", {}) if isinstance(retrieval_latency, dict) else {},
                "agent": agent_metrics.get("rerank_quality", {}) if isinstance(agent_metrics, dict) else {},
            },
            "missing_queries_last_30d": {
                "count": len(missing_queries),
                "items": missing_queries,
                "sample_queries": [str(item.get("query", "")) for item in missing_queries[:3]] if isinstance(missing_queries, list) else [],
            },
            "chat_feedback": {
                "count": len(feedback_items),
                "items": feedback_items[:20] if isinstance(feedback_items, list) else [],
            },
            "ticket_weekly_stats": ticket_weekly_stats,
            "agent_wall_clock": agent_metrics.get("wall_clock", {}) if isinstance(agent_metrics, dict) else {},
            "runtime_data": runtime_data,
            "data_backups": data_backups,
            "overview_status": _overview_status(
                "degraded_full" if errors else "full",
                errors=errors,
                message="部分详细统计加载失败。" if errors else "详细统计已就绪。",
            ),
        }
    )
    return overview


def _build_overview_core() -> dict[str, Any]:
    """Fast subset of overview: only the counters needed for first-paint cards.

    Skips: latency traces, agent metrics, missing queries, feedback,
    runtime data, ticket trends — those are all deferred to the full overview.
    """
    rag_docs, rag_changed, rag_sources = _count_rag_index_docs()
    lib_total, lib_by_media, lib_vector_rows, lib_graph_nodes, lib_graph_edges, lib_this_year = _library_counts()
    lib_graph_quality = _library_graph_quality(lib_total, lib_vector_rows)
    alias_proposal_summary = _load_library_alias_proposal_summary()
    rag_graph_nodes, rag_graph_edges = _rag_graph_counts()
    month_web, month_deepseek, quota_daily = _load_monthly_quota_counts()
    session_count, message_count = _agent_session_counts()
    rag_qa_sessions, rag_qa_messages = _rag_qa_session_counts()
    startup = _load_startup_status_cached()
    data_backups = data_backup_service.backup_summary()
    notifications = _build_information_notifications()

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
            "alias_proposal": alias_proposal_summary,
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
        "data_backups": data_backups,
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
        "notifications": notifications,
        "overview_status": _overview_status("core", message="当前仅包含核心指标，详细统计需等待 full overview。"),
    }


# ── Core overview cache (separate from full cache) ────────────────────────────
_OVERVIEW_CORE_CACHE_TTL = float(os.getenv("NAV_DASHBOARD_CORE_CACHE_TTL", "15"))
_overview_core_cache: dict[str, Any] | None = None
_overview_core_cache_at: float = 0.0
_overview_core_cache_lock = threading.Lock()


def _get_overview_core_cache_state() -> tuple[dict[str, Any] | None, float]:
    return _overview_core_cache, _overview_core_cache_at


def _set_overview_core_cache_state(value: dict[str, Any], cached_at: float) -> None:
    global _overview_core_cache, _overview_core_cache_at  # noqa: PLW0603
    _overview_core_cache = value
    _overview_core_cache_at = cached_at



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
    return library_tracker_internal_base_url()
def _run_library_graph_rebuild(full: bool = False, *, report_progress=None, is_cancelled=None) -> dict[str, Any]:
    base = _library_tracker_internal_base_url().rstrip("/")
    start_url = f"{base}/api/library/graph/rebuild-job" if full else f"{base}/api/library/graph/sync-missing-job"
    try:
        started = _http_json_request("POST", start_url, payload={}, timeout=15)
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
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


dashboard_api_owner.configure(
    dashboard_api_owner.DashboardApiOwnerDeps(
        monotonic=time.monotonic,
        overview_cache_lock=_overview_cache_lock,
        overview_cache_ttl=_OVERVIEW_CACHE_TTL,
        get_overview_cache=_get_overview_cache_state,
        set_overview_cache=_set_overview_cache_state,
        overview_core_cache_lock=_overview_core_cache_lock,
        overview_core_cache_ttl=_OVERVIEW_CORE_CACHE_TTL,
        get_overview_core_cache=_get_overview_core_cache_state,
        set_overview_core_cache=_set_overview_core_cache_state,
        build_information_notifications=_build_information_notifications,
        build_overview=_build_overview,
        build_overview_core=_build_overview_core,
        build_failed_overview=_build_failed_overview,
        persist_overview_snapshot=_persist_overview_snapshot,
        load_notification_state=_load_notification_state,
        save_notification_state=_save_notification_state,
        clear_dashboard_overview_cache=_clear_dashboard_overview_cache,
        invalidate_overview_cache=_invalidate_overview_cache,
        load_missing_queries=_load_missing_queries,
        no_context_log_path=no_context_log_path(),
        is_benchmark_source=_is_benchmark_source,
        is_benchmark_trace_id=_is_benchmark_trace_id,
        http_json_request=_http_json_request,
        load_startup_status=_load_startup_status,
        load_startup_status_cached=_load_startup_status_cached,
        owner_app_dir=APP_DIR,
        ai_summary_internal_base_url=_ai_summary_internal_base_url,
        run_library_graph_rebuild=_run_library_graph_rebuild,
    )
)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    ai_summary_url = _rewrite_loopback_url_for_request(AI_SUMMARY_URL_OVERRIDE, request, 8000)
    library_tracker_url = _rewrite_loopback_url_for_request(LIBRARY_TRACKER_URL_OVERRIDE, request, 8091)
    property_url = _rewrite_loopback_url_for_request(PROPERTY_URL_OVERRIDE, request, 8093)
    journey_url = _rewrite_loopback_url_for_request(JOURNEY_URL_OVERRIDE, request, 8094)
    browser_cards = _browser_custom_cards(request)

    local_model = display_model_name(get_settings().local_llm_model)
    deepseek_model = os.getenv("DEEPSEEK_MODEL", "deepseek-chat").strip() or "deepseek-chat"
    # Use the richest available overview for SSR prefill: full cached data
    # when warm (the common case), otherwise augmented core on cold boot.
    dashboard_prefill = _get_dashboard_prefill()
    dashboard_notifications_html = _render_information_notifications_html(
        list(dashboard_prefill.get("notifications") or []) if isinstance(dashboard_prefill, dict) else [],
        library_tracker_url,
        property_url,
    )
    dashboard_jobs_prefill = list_dashboard_jobs(only_active=True)
    tickets_prefill = _tickets_prefill_data()
    benchmark_prefill = _benchmark_prefill_data()
    tickets_applied_filters = dict(tickets_prefill.get("applied_filters") or {}) if isinstance(tickets_prefill, dict) else {}

    context = dashboard_page_service.build_index_context(
        request=request,
        ai_summary_url=ai_summary_url,
        library_tracker_url=library_tracker_url,
        property_url=property_url,
        journey_url=journey_url,
        local_model=local_model,
        deepseek_model=deepseek_model,
        browser_cards=browser_cards,
        dashboard_prefill=dashboard_prefill,
        dashboard_notifications_html=dashboard_notifications_html,
        dashboard_jobs_prefill=dashboard_jobs_prefill,
        tickets_prefill=tickets_prefill,
        benchmark_prefill=benchmark_prefill,
        render_dashboard_core_cards_html=_render_dashboard_core_cards_html,
        render_dashboard_latency_table_html=_render_dashboard_latency_table_html,
        render_dashboard_observability_table_html=_render_dashboard_observability_table_html,
        render_dashboard_jobs_html=_render_dashboard_jobs_html,
        render_dashboard_ticket_summary_meta_text=_render_dashboard_ticket_summary_meta_text,
        render_dashboard_ticket_summary_html=_render_dashboard_ticket_summary_html,
        render_dashboard_ticket_trend_meta_text=_render_dashboard_ticket_trend_meta_text,
        render_dashboard_ticket_trend_html=_render_dashboard_ticket_trend_html,
        render_dashboard_startup_logs_text=_render_dashboard_startup_logs_text,
        render_tickets_meta_text=_render_tickets_meta_text,
        render_tickets_list_html=_render_tickets_list_html,
        render_benchmark_last_run_text=_render_benchmark_last_run_text,
        render_benchmark_case_set_options_html=_render_benchmark_case_set_options_html,
        render_benchmark_history_table_html=_render_benchmark_history_table_html,
        core_ui_bootstrap_version=_core_static_version("ui_bootstrap.js"),
        core_app_shell_version=_core_static_version("app_shell.js"),
        core_shell_shared_css_version=_core_static_version("shell_shared.css"),
        core_sidebar_shared_css_version=_core_static_version("sidebar_shared.css"),
        js_version=_static_version("app.js"),
        css_version=_static_version("app.css"),
    )

    return templates.TemplateResponse(request=request, name="index.html", context=context)


def run() -> None:
    import uvicorn

    config = uvicorn.Config("web.main:app", host=HOST, port=PORT, reload=False)
    server = uvicorn.Server(config)
    app_control_service.register_uvicorn_server(server)
    server.run()


if __name__ == "__main__":
    run()
