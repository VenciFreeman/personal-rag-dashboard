from __future__ import annotations

import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

from send2trash import send2trash

from ai_conversations_summary.runtime_paths import (
    DATA_DIR as AI_SUMMARY_DATA_DIR,
    DEBUG_DIR as AI_SUMMARY_DEBUG_DIR,
    DEEPSEEK_AUDIT_DIR as AI_SUMMARY_DEEPSEEK_AUDIT_DIR,
    EXTRACTED_DIR as AI_SUMMARY_EXTRACTED_DIR,
    RAW_DIR as AI_SUMMARY_RAW_DIR,
    RETRIEVAL_METRICS_FILE as AI_SUMMARY_RETRIEVAL_METRICS_FILE,
    SPLIT_DIR as AI_SUMMARY_SPLIT_DIR,
    SUMMARIZE_DIR as AI_SUMMARY_SUMMARIZE_DIR,
    VECTOR_DB_DIR as AI_SUMMARY_VECTOR_DB_DIR,
)
from core_service.observability import list_trace_record_paths
from core_service.tickets import list_ticket_storage_paths

from ..runtime_paths import AGENT_METRICS_FILE, BENCHMARK_FILE, CHAT_FEEDBACK_FILE


PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(PROJECT_ROOT)).replace("\\", "/")
    except Exception:
        return str(path).replace("\\", "/")


def _sqlite_family(path: Path) -> list[Path]:
    return [path, path.with_name(path.name + "-wal"), path.with_name(path.name + "-shm")]


def runtime_data_targets() -> list[dict[str, Any]]:
    ai_data = AI_SUMMARY_DATA_DIR
    cache_dir = ai_data / "cache"
    return [
        {
            "key": "benchmark_history_current",
            "label": "Benchmark 历史",
            "description": "Dashboard 当前基准测试结果，仅保留最近 5 次。",
            "paths": [BENCHMARK_FILE],
            "mode": "reset_benchmark_history",
        },
        {
            "key": "benchmark_history_legacy",
            "label": "旧版 Benchmark 结果",
            "description": "遗留的工作区根目录 benchmark_results.json。",
            "paths": [PROJECT_ROOT / "data" / "benchmark_results.json"],
            "mode": "delete_paths",
        },
        {
            "key": "rag_debug_records",
            "label": "RAG Debug 记录",
            "description": "ai_conversations_summary 调试输出与排障快照。",
            "paths": [AI_SUMMARY_DEBUG_DIR],
            "mode": "clear_dir_contents",
        },
        {
            "key": "missing_query_log",
            "label": "未命中查询日志",
            "description": "最近未命中 query 的 JSONL 日志。",
            "paths": [cache_dir / "no_context_queries.jsonl"],
            "mode": "truncate_file",
        },
        {
            "key": "retrieval_metrics",
            "label": "RAG 检索统计",
            "description": "RAG rolling latency 指标文件。",
            "paths": [AI_SUMMARY_RETRIEVAL_METRICS_FILE],
            "mode": "delete_paths",
        },
        {
            "key": "agent_metrics",
            "label": "Agent 统计",
            "description": "Agent rolling metrics 文件。",
            "paths": [AGENT_METRICS_FILE],
            "mode": "delete_paths",
        },
        {
            "key": "chat_feedback",
            "label": "聊天反馈",
            "description": "Agent / RAG 回答反馈记录。",
            "paths": [CHAT_FEEDBACK_FILE],
            "mode": "delete_paths",
        },
        {
            "key": "trace_records",
            "label": "Trace 记录",
            "description": "Dashboard trace 查询使用的轻量追踪摘要。",
            "paths": list_trace_record_paths(),
            "mode": "delete_paths",
        },
        {
            "key": "web_cache",
            "label": "Web 搜索缓存",
            "description": "Tavily 搜索缓存数据库。",
            "paths": _sqlite_family(cache_dir / "web_cache.db"),
            "mode": "delete_paths",
        },
        {
            "key": "embed_cache",
            "label": "Embedding 缓存",
            "description": "文本 embedding 缓存数据库。",
            "paths": _sqlite_family(cache_dir / "embed_cache.db"),
            "mode": "delete_paths",
        },
        {
            "key": "legacy_ai_summary_vector_db",
            "label": "旧向量库目录",
            "description": "ai_conversations_summary 旧 vector_db 目录。",
            "paths": [PROJECT_ROOT / "ai_conversations_summary" / "data" / "vector_db"],
            "mode": "delete_paths",
        },
        {
            "key": "raw_dir",
            "label": "原始导入目录",
            "description": "原始导入文件缓存。",
            "paths": [AI_SUMMARY_RAW_DIR],
            "mode": "clear_dir_contents",
        },
        {
            "key": "extracted_dir",
            "label": "解包中间目录",
            "description": "抽取或解包后的中间文件。",
            "paths": [AI_SUMMARY_EXTRACTED_DIR],
            "mode": "clear_dir_contents",
        },
        {
            "key": "split_dir",
            "label": "切分中间目录",
            "description": "文档切分阶段的中间产物。",
            "paths": [AI_SUMMARY_SPLIT_DIR],
            "mode": "clear_dir_contents",
        },
        {
            "key": "summarize_dir",
            "label": "摘要中间目录",
            "description": "批处理摘要阶段的中间产物。",
            "paths": [AI_SUMMARY_SUMMARIZE_DIR],
            "mode": "clear_dir_contents",
        },
        {
            "key": "deepseek_api_audit",
            "label": "DeepSeek 审计缓存",
            "description": "DeepSeek API 调用审计与调试文件。",
            "paths": [AI_SUMMARY_DEEPSEEK_AUDIT_DIR],
            "mode": "clear_dir_contents",
        },
    ]


def measure_path(path: Path) -> tuple[int, int, bool]:
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


def collect_runtime_data_items() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for target in runtime_data_targets():
        total_bytes = 0
        file_count = 0
        existing_paths = 0
        for path in target["paths"]:
            size_bytes, files, exists = measure_path(path)
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


def runtime_data_summary(*, include_items: bool = False) -> dict[str, Any]:
    items = collect_runtime_data_items()
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


def _recycle_path(path: Path) -> None:
    if not path.exists():
        return
    send2trash(str(path))


def clear_runtime_target(target: dict[str, Any]) -> None:
    mode = str(target.get("mode") or "")
    paths = [path for path in target.get("paths", []) if isinstance(path, Path)]

    if mode == "reset_benchmark_history":
        if not paths:
            return
        path = paths[0]
        if path.exists():
            _recycle_path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"results": []}, ensure_ascii=False, indent=2), encoding="utf-8")
        return

    if mode == "truncate_file":
        for path in paths:
            if path.exists():
                _recycle_path(path)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("", encoding="utf-8")
        return

    if mode == "clear_dir_contents":
        for path in paths:
            path.mkdir(parents=True, exist_ok=True)
            for child in path.iterdir():
                _recycle_path(child)
        return

    if mode == "delete_paths":
        for path in paths:
            _recycle_path(path)
        return

    raise ValueError(f"unsupported runtime cleanup mode: {mode}")


def cleanup_runtime_data_keys(requested: list[str]) -> dict[str, Any]:
    by_key = {str(item["key"]): item for item in runtime_data_targets()}
    cleared: list[dict[str, Any]] = []
    failed: list[dict[str, str]] = []
    for key in requested:
        target = by_key.get(key)
        if target is None:
            failed.append({"key": key, "error": "unknown_key"})
            continue
        try:
            before = next((item for item in collect_runtime_data_items() if item.get("key") == key), None)
            clear_runtime_target(target)
            after = next((item for item in collect_runtime_data_items() if item.get("key") == key), None)
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
    return {
        "ok": not failed,
        "cleared": cleared,
        "failed": failed,
        "summary": runtime_data_summary(include_items=True),
    }


def resolve_ai_summary_vector_db_dir() -> Path:
    vector_db_env = (os.getenv("AI_SUMMARY_VECTOR_DB_DIR", "") or "").strip()
    if vector_db_env:
        return Path(vector_db_env)
    return AI_SUMMARY_VECTOR_DB_DIR
