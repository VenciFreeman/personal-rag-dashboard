from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Any, Callable

from .agent_types import AgentRuntimeState, PlannedToolCall, ToolExecution


def _load_agent_metrics(metrics_file: Path) -> list[dict[str, Any]]:
    if not metrics_file.exists():
        return []
    try:
        payload = json.loads(metrics_file.read_text(encoding="utf-8"))
        rows = payload.get("records", []) if isinstance(payload, dict) else []
        return [row for row in rows if isinstance(row, dict)]
    except Exception:
        return []


def _save_agent_metrics(
    *,
    data_dir: Path,
    metrics_file: Path,
    metrics_max: int,
    rows: list[dict[str, Any]],
    now_fn: Callable[[], datetime],
) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "records": rows[-metrics_max:],
        "updated_at": now_fn().isoformat(timespec="seconds"),
    }
    try:
        metrics_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def record_agent_metrics(
    *,
    data_dir: Path,
    metrics_file: Path,
    metrics_max: int,
    lock: RLock,
    now_fn: Callable[[], datetime],
    query_profile: str,
    search_mode: str,
    query_type: str,
    rag_used: int,
    media_used: int,
    web_used: int,
    no_context: int,
    no_context_reason: str = "",
    trace_id: str = "",
    doc_score_threshold: float | None = None,
    doc_top1_score: float | None = None,
    doc_top1_score_before_rerank: float | None = None,
    doc_top1_identity_changed: int | None = None,
    doc_top1_rank_shift: float | None = None,
    embed_cache_hit: int = 0,
    query_rewrite_hit: int = 0,
    vector_recall_seconds: float = 0.0,
    rerank_seconds: float = 0.0,
    wall_clock_seconds: float = 0.0,
) -> None:
    row: dict[str, Any] = {
        "ts": now_fn().isoformat(timespec="seconds"),
        "query_profile": str(query_profile or "medium"),
        "search_mode": str(search_mode or "local_only"),
        "query_type": str(query_type or "general"),
        "rag_used": int(rag_used or 0),
        "media_used": int(media_used or 0),
        "web_used": int(web_used or 0),
        "no_context": int(no_context or 0),
        "no_context_reason": str(no_context_reason or "").strip(),
        "trace_id": str(trace_id or "").strip(),
        "doc_score_threshold": round(float(doc_score_threshold), 4) if doc_score_threshold is not None else None,
        "doc_top1_score": round(float(doc_top1_score), 4) if doc_top1_score is not None else None,
        "doc_top1_score_before_rerank": round(float(doc_top1_score_before_rerank), 4) if doc_top1_score_before_rerank is not None else None,
        "doc_top1_identity_changed": int(doc_top1_identity_changed) if doc_top1_identity_changed is not None else None,
        "doc_top1_rank_shift": round(float(doc_top1_rank_shift), 4) if doc_top1_rank_shift is not None else None,
        "embed_cache_hit": int(embed_cache_hit or 0),
        "query_rewrite_hit": int(query_rewrite_hit or 0),
        "vector_recall_seconds": round(float(vector_recall_seconds or 0), 6),
        "rerank_seconds": round(float(rerank_seconds or 0), 6),
        "wall_clock_seconds": round(float(wall_clock_seconds or 0), 6),
    }
    with lock:
        rows = _load_agent_metrics(metrics_file)
        rows.append(row)
        _save_agent_metrics(
            data_dir=data_dir,
            metrics_file=metrics_file,
            metrics_max=metrics_max,
            rows=rows,
            now_fn=now_fn,
        )


def build_agent_trace_record(
    *,
    trace_builder: Callable[..., dict[str, Any]],
    get_llm_profile: Callable[..., dict[str, Any]],
    get_resolved_query_state_from_runtime: Callable[..., dict[str, Any]],
    serialize_router_decision: Callable[..., dict[str, Any]],
    serialize_execution_plan: Callable[..., dict[str, Any]],
    serialize_post_retrieval_assessment: Callable[..., dict[str, Any]],
    build_guardrail_flags: Callable[..., dict[str, Any]],
    build_error_taxonomy: Callable[..., dict[str, Any]],
    get_planner_snapshot_from_runtime: Callable[..., dict[str, Any]],
    normalize_media_filter_map: Callable[..., dict[str, Any]],
    get_lookup_mode_from_state: Callable[[dict[str, Any]], str],
    compute_classification_conformance: Callable[..., dict[str, Any]],
    get_query_type: Callable[..., str],
    confidence_high: float,
    confidence_medium: float,
    now_iso: Callable[[], str],
    trace_id: str,
    session_id: str,
    backend: str,
    search_mode: str,
    benchmark_mode: bool,
    stream_mode: bool,
    query_profile: dict[str, Any],
    runtime_state: AgentRuntimeState,
    planned_tools: list[PlannedToolCall],
    tool_results: list[ToolExecution],
    doc_data: dict[str, Any],
    timings: dict[str, Any],
    llm_stats: dict[str, Any],
    answer_guardrail_mode: dict[str, Any],
    degraded_to_retrieval: bool,
    degrade_reason: str,
    wall_clock_seconds: float,
    planning_seconds: float,
    tool_execution_seconds: float,
    llm_seconds: float,
    response_finalize_seconds: float,
) -> dict[str, Any]:
    execution_artifact = getattr(runtime_state, "execution_artifact", None)
    media_validation = dict(getattr(execution_artifact, "media_validation", {}) or {}) if execution_artifact is not None else {}
    if not media_validation:
        assessment_payload = serialize_post_retrieval_assessment(runtime_state.post_retrieval_assessment)
        media_validation = dict(assessment_payload.get("media_validation") or {}) if isinstance(assessment_payload, dict) else {}
    guardrail_flags = dict(getattr(execution_artifact, "guardrail_flags", {}) or {}) if execution_artifact is not None else {}
    if not guardrail_flags:
        guardrail_flags = build_guardrail_flags(runtime_state, media_validation)
    error_taxonomy = dict(getattr(execution_artifact, "error_taxonomy", {}) or {}) if execution_artifact is not None else {}
    if not error_taxonomy:
        error_taxonomy = build_error_taxonomy(runtime_state, media_validation, guardrail_flags)
    if execution_artifact is not None:
        runtime_state.execution_artifact = replace(
            execution_artifact,
            guardrail_flags=guardrail_flags,
            error_taxonomy=error_taxonomy,
        )
    return trace_builder(
        trace_id=trace_id,
        session_id=session_id,
        backend=backend,
        search_mode=search_mode,
        benchmark_mode=benchmark_mode,
        stream_mode=stream_mode,
        query_profile=query_profile,
        runtime_state=runtime_state,
        planned_tools=planned_tools,
        tool_results=tool_results,
        doc_data=doc_data,
        timings=timings,
        llm_stats=llm_stats,
        answer_guardrail_mode=answer_guardrail_mode,
        degraded_to_retrieval=degraded_to_retrieval,
        degrade_reason=degrade_reason,
        wall_clock_seconds=wall_clock_seconds,
        planning_seconds=planning_seconds,
        tool_execution_seconds=tool_execution_seconds,
        llm_seconds=llm_seconds,
        response_finalize_seconds=response_finalize_seconds,
        get_llm_profile=get_llm_profile,
        get_resolved_query_state_from_runtime=get_resolved_query_state_from_runtime,
        serialize_router_decision=serialize_router_decision,
        serialize_execution_plan=serialize_execution_plan,
        serialize_post_retrieval_assessment=serialize_post_retrieval_assessment,
        get_planner_snapshot_from_runtime=get_planner_snapshot_from_runtime,
        normalize_media_filter_map=normalize_media_filter_map,
        get_lookup_mode_from_state=get_lookup_mode_from_state,
        compute_classification_conformance=compute_classification_conformance,
        get_query_type=get_query_type,
        confidence_high=confidence_high,
        confidence_medium=confidence_medium,
        now_iso=now_iso,
    )


def write_agent_trace_record(
    record: dict[str, Any],
    *,
    trace_writer: Callable[[dict[str, Any]], None] | None = None,
) -> None:
    if trace_writer is None:
        return
    try:
        trace_writer(record)
    except Exception:
        pass