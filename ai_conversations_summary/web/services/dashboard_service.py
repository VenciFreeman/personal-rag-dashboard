from __future__ import annotations

import json
import math
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from web.services import rag_service, workflow_service

try:
    from ai_conversations_summary.scripts.cache_db import NO_CONTEXT_LOG_PATH
except ImportError:
    from scripts.cache_db import NO_CONTEXT_LOG_PATH


SOURCE_LABELS = {
    "agent": "LLM Agent",
    "rag_qa": "RAG 问答",
    "rag_qa_stream": "RAG 问答（流式）",
    "benchmark_rag": "Benchmark / RAG",
    "benchmark_agent": "Benchmark / Agent",
    "agent_chat": "LLM Agent",
    "rag_chat": "RAG 问答",
}


def _safe_load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _source_display_name(source: str) -> str:
    key = str(source or "").strip().lower()
    return SOURCE_LABELS.get(key, str(source or "未知来源"))


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
    cleaned = [max(0.0, float(value)) for value in values]
    avg = sum(cleaned) / max(1, len(cleaned))
    return {
        "count": len(cleaned),
        "avg": round(avg, 4),
        "p50": round(_percentile(cleaned, 0.50), 4),
        "p95": round(_percentile(cleaned, 0.95), 4),
        "p99": round(_percentile(cleaned, 0.99), 4),
    }


def _top_percent_mean(values: list[float], percent: float = 0.01) -> float | None:
    cleaned = [float(value) for value in values if isinstance(value, (int, float))]
    if not cleaned:
        return None
    cleaned.sort(reverse=True)
    take = max(1, int(math.ceil(len(cleaned) * max(0.0, min(1.0, float(percent))))))
    bucket = cleaned[:take]
    return round(sum(bucket) / len(bucket), 4) if bucket else None


def _parse_iso_ts(value: str) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text).timestamp()
    except Exception:
        return None


def _load_graph_counts() -> tuple[int, int]:
    graph = _safe_load_json(workflow_service.VECTOR_DB_DIR / "knowledge_graph_rag.json", {})
    if not isinstance(graph, dict):
        return 0, 0
    nodes = graph.get("nodes", {})
    edges = graph.get("edges", [])
    return (
        len(nodes) if isinstance(nodes, dict) else 0,
        len(edges) if isinstance(edges, list) else 0,
    )


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


def get_session_activity_summary() -> dict[str, int]:
    sessions = rag_service.list_sessions()
    total_messages = 0
    active_sessions = 0
    for session in sessions:
        messages = session.get("messages") if isinstance(session, dict) else []
        if not isinstance(messages, list):
            continue
        has_user = any(
            str(message.get("role", "")).lower() in {"用户", "user"}
            for message in messages
            if isinstance(message, dict)
        )
        if not has_user:
            continue
        active_sessions += 1
        total_messages += sum(
            1
            for message in messages
            if isinstance(message, dict) and str(message.get("role", "")).lower() in {"用户", "user", "助手", "assistant"}
        )
    return {
        "session_count": active_sessions,
        "message_count": total_messages,
    }


def get_retrieval_latency_summary() -> dict[str, Any]:
    payload = _safe_load_json(rag_service.RETRIEVAL_METRICS_FILE, {})
    rows = payload.get("records") if isinstance(payload, dict) and isinstance(payload.get("records"), list) else []
    all_records = [row for row in rows if isinstance(row, dict)]
    records = [row for row in all_records if not str(row.get("source", "")).startswith("benchmark")][-20:]

    stage_keys = ("total", "rerank_seconds", "context_assembly_seconds", "web_search_seconds", "elapsed_seconds")
    legacy_aliases = {
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
        for key in stage_keys:
            value = row.get(key)
            if value is None:
                for legacy_key, canonical in legacy_aliases.items():
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
    count = len(records)
    embed_hits = sum(1 for row in records if float(row.get("embed_cache_hit", 0) or 0) > 0)
    web_hits = sum(1 for row in records if float(row.get("web_cache_hit", 0) or 0) > 0)
    no_context = sum(_row_no_context(row) for row in records)
    rewrite_hits = sum(1 for row in records if float(row.get("query_rewrite_seconds", 0) or 0) > 0)

    rerank_pairs: list[tuple[float, float]] = []
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
    rank_shifts = [float(row["top1_rank_shift"]) for row in records if row.get("top1_rank_shift") is not None]
    identity_changes = [int(row["top1_identity_changed"]) for row in records if row.get("top1_identity_changed") is not None]

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

    by_profile: dict[str, Any] = {}
    for profile in ("short", "medium", "long"):
        profile_rows = [row for row in records if row.get("query_profile") == profile]
        if not profile_rows:
            continue
        stage_map: dict[str, list[float]] = {}
        for row in profile_rows:
            for key in stage_keys:
                value = row.get(key)
                if value is None:
                    for legacy_key, canonical in legacy_aliases.items():
                        if canonical == key and row.get(legacy_key) is not None:
                            value = row.get(legacy_key)
                            break
                if value is None:
                    continue
                try:
                    score = float(value)
                except Exception:
                    continue
                if score >= 0:
                    stage_map.setdefault(key, []).append(score)
        by_profile[profile] = {
            "count": len(profile_rows),
            "stages": {key: _timing_stats(values) for key, values in stage_map.items()},
            "no_context_rate": round(sum(_row_no_context(row) for row in profile_rows) / len(profile_rows), 4),
            "embed_cache_hit_rate": round(sum(1 for row in profile_rows if float(row.get("embed_cache_hit", 0) or 0) > 0) / len(profile_rows), 4),
            "web_cache_hit_rate": round(sum(1 for row in profile_rows if float(row.get("web_cache_hit", 0) or 0) > 0) / len(profile_rows), 4),
            "rewrite_seconds": _timing_stats([
                float(row.get("query_rewrite_seconds", 0) or 0)
                for row in profile_rows
                if float(row.get("query_rewrite_seconds", 0) or 0) >= 0
            ]),
            "query_rewrite_rate": round(sum(1 for row in profile_rows if float(row.get("query_rewrite_seconds", 0) or 0) > 0) / len(profile_rows), 4),
        }

    by_search_mode: dict[str, Any] = {}
    modes = sorted({str(row.get("search_mode", "") or "").strip() for row in records if str(row.get("search_mode", "") or "").strip()})
    for mode in modes:
        mode_rows = [row for row in records if str(row.get("search_mode", "") or "").strip() == mode]
        if not mode_rows:
            continue
        by_search_mode[mode] = {
            "count": len(mode_rows),
            "elapsed": _timing_stats([float(row.get("elapsed_seconds", 0) or 0) for row in mode_rows if float(row.get("elapsed_seconds", 0) or 0) >= 0]),
            "total": _timing_stats([float(row.get("total", 0) or 0) for row in mode_rows if float(row.get("total", 0) or 0) >= 0]),
            "no_context_rate": round(sum(_row_no_context(row) for row in mode_rows) / len(mode_rows), 4),
            "embed_cache_hit_rate": round(sum(1 for row in mode_rows if float(row.get("embed_cache_hit", 0) or 0) > 0) / len(mode_rows), 4),
            "web_cache_hit_rate": round(sum(1 for row in mode_rows if float(row.get("web_cache_hit", 0) or 0) > 0) / len(mode_rows), 4),
            "query_rewrite_rate": round(sum(1 for row in mode_rows if float(row.get("query_rewrite_seconds", 0) or 0) > 0) / len(mode_rows), 4),
        }

    return {
        "records": compact_records,
        "record_count": len(compact_records),
        "stages": stages,
        "rerank_quality": rerank_quality,
        "embed_cache_hit_rate": round(embed_hits / count, 3) if count else None,
        "web_cache_hit_rate": round(web_hits / count, 3) if count else None,
        "query_rewrite_rate": round(rewrite_hits / count, 3) if count else None,
        "no_context_rate": round(no_context / count, 3) if count else None,
        "by_profile": by_profile,
        "by_search_mode": by_search_mode,
    }


def list_missing_queries(days: int = 30, limit: int = 200, source: str = "") -> list[dict[str, Any]]:
    if not NO_CONTEXT_LOG_PATH.exists():
        return []
    now_ts = time.time()
    cutoff = now_ts - max(1, int(days)) * 86_400
    source_filter = str(source or "").strip().lower()
    try:
        lines = NO_CONTEXT_LOG_PATH.read_text(encoding="utf-8").splitlines()
    except Exception:
        return []

    rows: list[dict[str, Any]] = []
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
        row_source = str(row.get("source", "unknown") or "unknown").strip()
        if source_filter and source_filter not in {"all", "*"} and row_source.lower() != source_filter:
            continue
        top1_score = row.get("top1_score")
        threshold = row.get("threshold")
        rows.append(
            {
                "ts": ts,
                "source": row_source,
                "source_label": _source_display_name(row_source),
                "query": str(row.get("query", "") or "").strip(),
                "top1_score": float(top1_score) if isinstance(top1_score, (int, float)) else None,
                "threshold": float(threshold) if isinstance(threshold, (int, float)) else None,
                "trace_id": str(row.get("trace_id", "") or "").strip(),
                "reason": str(row.get("reason", "") or "").strip(),
            }
        )

    rows.sort(key=lambda item: str(item.get("ts", "")), reverse=True)
    return rows[: max(1, int(limit))]


def get_dashboard_overview() -> dict[str, Any]:
    _manifest, docs = workflow_service._read_vector_metadata_docs()
    graph_nodes, graph_edges = _load_graph_counts()
    session_summary = get_session_activity_summary()
    return {
        "index": {
            "indexed_documents": len(docs),
            "changed_pending": sum(1 for row in docs if bool(row.get("changed", False))),
            "source_markdown_files": len(workflow_service._iter_document_markdown_files()),
            "graph_nodes": graph_nodes,
            "graph_edges": graph_edges,
        },
        "sessions": session_summary,
        "retrieval_latency": get_retrieval_latency_summary(),
    }