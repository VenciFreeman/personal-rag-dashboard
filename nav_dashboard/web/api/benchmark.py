"""nav_dashboard/web/api/benchmark.py
性能基准测试 API 路由（/api/benchmark）

职责：
  POST /api/benchmark/run      — 运行基准测试，通过 SSE（Server-Sent Events）实时推送进度
                                  请求体：{ modules, query_count, run_id }
  DELETE /api/benchmark/run    — 中止当前进行中的测试（通过 run_id 标记取消标志位）
    GET  /api/benchmark/history  — 获取最近 N 次测试结果（存储于 app runtime data/benchmark/results.json）
  DELETE /api/benchmark/history — 清空历史

测试流程：
  1. 按 query_count 从 SHORT/MEDIUM/LONG 三个中文 Query 池各随机采样
  2. 对每条 query 顺序调用 RAG 或 Agent 接口，记录端到端时延及各阶段指标
  3. 每批（短/中/长）完成后聚合：avg/p50/p95 时延、成功率
  4. 全部完成后生成 result 事件并写入历史文件（中止时丢弃，不写入）

每次测试通过 run_id 绑定一个取消标志，DELETE 端点设置标志位后，
生成器循环在下条 query 开始前检查并提前退出。
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib import error as urlerror
from urllib import parse as urlparse
from urllib import request as urlrequest
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from nav_dashboard.web.clients.internal_services import request_json
from nav_dashboard.web.api.response_models import (
    API_SCHEMA_VERSION,
    BenchmarkCaseSetsResponse,
    BenchmarkHistoryResponse,
    BenchmarkRouterClassificationCaseMutationResponse,
    BenchmarkRouterClassificationCasesResponse,
    BenchmarkRouterClassificationHistoryResponse,
    BenchmarkRouterClassificationResponse,
    build_benchmark_case_sets_response,
    build_benchmark_history_response,
    build_benchmark_job_response,
    build_benchmark_router_classification_case_mutation_response,
    build_benchmark_router_classification_cases_response,
    build_benchmark_router_classification_history_response,
    build_benchmark_router_classification_response,
    build_benchmark_stream_event,
)
from nav_dashboard.web.config import AI_SUMMARY_URL_OVERRIDE, PORT
from nav_dashboard.web.services import benchmark_contracts
from nav_dashboard.web.services import benchmark_history_store
from nav_dashboard.web.services import benchmark_runner
from nav_dashboard.web.services.agent.benchmark_case_catalog import (
    BENCHMARK_CASE_SET_CASES,
    CASE_SET_LABELS,
    CASE_SET_METADATA,
    QUERY_CASE_SETS,
    normalize_followup_mode,
    resolve_case_batch,
)
from nav_dashboard.web.services.dashboard import dashboard_jobs
from nav_dashboard.web.services.planner import planner_contracts
from nav_dashboard.web.services.runtime_paths import BENCHMARK_FILE, ROUTER_CLS_CASES_FILE, ROUTER_CLS_HISTORY_FILE

router = APIRouter(prefix="/api/benchmark", tags=["benchmark"])

BENCHMARK_HISTORY_MAX = 5

CHAIN_SPECS: dict[str, dict[str, str]] = {
    "rag": {"label": "RAG Q&A", "family": "rag", "search_mode": "local_only"},
    "agent": {"label": "LLM Agent", "family": "agent", "search_mode": "local_only"},
    "hybrid": {"label": "混合路由 Agent", "family": "agent", "search_mode": "hybrid"},
}

_BENCHMARK_LENGTHS = ["short", "medium", "long"]
_NINETY_SECOND_P95_LIMITS = {"short": 90.0, "medium": 90.0, "long": 90.0, "global": 90.0}

DEFAULT_ASSERTION_LIMITS: dict[str, dict[str, Any]] = {
    "rag": {
        "max_no_context_rate": 0.35,
        "max_p95_wall_clock_s": dict(_NINETY_SECOND_P95_LIMITS),
        "max_p95_elapsed_s": dict(_NINETY_SECOND_P95_LIMITS),
    },
    "agent": {
        "max_no_context_rate": 0.45,
        "max_p95_wall_clock_s": dict(_NINETY_SECOND_P95_LIMITS),
        "max_p95_elapsed_s": dict(_NINETY_SECOND_P95_LIMITS),
    },
    "hybrid": {
        "max_no_context_rate": 0.45,
        "max_p95_wall_clock_s": dict(_NINETY_SECOND_P95_LIMITS),
        "max_p95_elapsed_s": dict(_NINETY_SECOND_P95_LIMITS),
    },
}

ASSERTION_LIMIT_OVERRIDES_BY_CASE_SET: dict[str, dict[str, dict[str, Any]]] = {
    "regression_v1": {
        "agent": {
            "max_p95_wall_clock_s": dict(_NINETY_SECOND_P95_LIMITS),
            "max_p95_elapsed_s": dict(_NINETY_SECOND_P95_LIMITS),
        },
        "rag": {
            "max_p95_wall_clock_s": dict(_NINETY_SECOND_P95_LIMITS),
            "max_p95_elapsed_s": dict(_NINETY_SECOND_P95_LIMITS),
        },
    },
    "session_contamination_v1": {
        "agent": {
            "max_p95_wall_clock_s": dict(_NINETY_SECOND_P95_LIMITS),
            "max_p95_elapsed_s": dict(_NINETY_SECOND_P95_LIMITS),
            "by_taxonomy": {
                "entity_detail_noise": {
                    "max_quality_fail_rate": 0.0,
                    "max_wrong_topic_leak_rate": 0.0,
                    "max_p95_wall_clock_s": {"global": 90.0},
                    "max_p95_elapsed_s": {"global": 90.0},
                },
                "followup_contamination": {
                    "max_quality_fail_rate": 0.0,
                    "max_wrong_topic_leak_rate": 0.0,
                    "max_followup_contamination_rate": 0.0,
                    "max_empty_answer_rate": 0.0,
                    "max_p95_wall_clock_s": {"global": 90.0},
                    "max_p95_elapsed_s": {"global": 90.0},
                },
                "compare_terminal_quality": {
                    "max_quality_fail_rate": 0.0,
                    "max_reference_order_violation_rate": 0.0,
                    "max_empty_answer_rate": 0.0,
                    "max_p95_wall_clock_s": {"global": 90.0},
                    "max_p95_elapsed_s": {"global": 90.0},
                },
                "personal_review_cross_contamination": {
                    "max_quality_fail_rate": 0.0,
                    "max_wrong_topic_leak_rate": 0.0,
                    "max_empty_answer_rate": 0.0,
                    "max_p95_wall_clock_s": {"global": 90.0},
                    "max_p95_elapsed_s": {"global": 90.0},
                },
                "strict_scope_alias_collection": {
                    "max_quality_fail_rate": 0.0,
                    "max_strict_scope_false_negative_rate": 0.0,
                    "max_empty_answer_rate": 0.0,
                    "max_p95_wall_clock_s": {"global": 90.0},
                    "max_p95_elapsed_s": {"global": 90.0},
                },
            },
        },
        "hybrid": {
            "max_p95_wall_clock_s": dict(_NINETY_SECOND_P95_LIMITS),
            "max_p95_elapsed_s": dict(_NINETY_SECOND_P95_LIMITS),
            "by_taxonomy": {
                "entity_detail_noise": {
                    "max_quality_fail_rate": 0.0,
                    "max_wrong_topic_leak_rate": 0.0,
                },
                "followup_contamination": {
                    "max_quality_fail_rate": 0.0,
                    "max_wrong_topic_leak_rate": 0.0,
                    "max_followup_contamination_rate": 0.0,
                },
                "compare_terminal_quality": {
                    "max_quality_fail_rate": 0.0,
                    "max_reference_order_violation_rate": 0.0,
                },
                "personal_review_cross_contamination": {
                    "max_quality_fail_rate": 0.0,
                    "max_wrong_topic_leak_rate": 0.0,
                },
                "strict_scope_alias_collection": {
                    "max_quality_fail_rate": 0.0,
                    "max_strict_scope_false_negative_rate": 0.0,
                },
            },
        },
    },
}

# ─── URL resolution ───────────────────────────────────────────────────────────

def _ai_summary_base() -> str:
    raw = (os.getenv("NAV_DASHBOARD_AI_SUMMARY_INTERNAL_URL", "") or "").strip().rstrip("/")
    if raw:
        return raw
    if AI_SUMMARY_URL_OVERRIDE:
        parsed = urlparse.urlparse(AI_SUMMARY_URL_OVERRIDE)
        if parsed.scheme and parsed.hostname:
            port = parsed.port or 8000
            return f"{parsed.scheme}://{parsed.hostname}:{port}"
    return "http://127.0.0.1:8000"


def _nav_dashboard_internal_base(request: Request | None = None) -> str:
    raw = (os.getenv("NAV_DASHBOARD_INTERNAL_URL", "") or "").strip().rstrip("/")
    if raw:
        return raw
    scheme = "http"
    port = int(PORT)
    if request is not None:
        try:
            request_scheme = str(request.url.scheme or "").strip()
            if request_scheme:
                scheme = request_scheme
        except Exception:
            pass
        try:
            request_port = int(request.url.port or 0)
            if request_port > 0:
                port = request_port
        except Exception:
            pass
    return f"{scheme}://127.0.0.1:{port}"


# ─── HTTP helper ──────────────────────────────────────────────────────────────

def _post_json(url: str, body: dict[str, Any], timeout: int = 180) -> dict[str, Any]:
    try:
        payload = request_json("POST", url, payload=body, timeout=timeout)
        if isinstance(payload, dict):
            error_text = str(payload.get("error") or "").strip()
            if not payload.get("ok", True) and error_text and not str(payload.get("_error") or "").strip():
                payload = dict(payload)
                payload["_error"] = error_text
        return payload
    except Exception as exc:
        return {"_error": str(exc)[:200]}


def _benchmark_trace_id(prefix: str) -> str:
    return f"benchmark_{prefix}_{uuid4().hex[:12]}"


def _coerce_named_tool_list(value: Any) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for item in value if isinstance(value, list) else []:
        if isinstance(item, dict):
            raw_name = item.get("name") or item.get("tool")
        else:
            raw_name = item
        name = str(raw_name or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        names.append(name)
    return names


def _coerce_text_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        items = value
    elif isinstance(value, tuple):
        items = list(value)
    else:
        items = [value]
    normalized: list[str] = []
    for item in items:
        text = str(item or "").strip()
        if text:
            normalized.append(text)
    return normalized


def _flatten_text_map(value: Any) -> str:
    if isinstance(value, dict):
        parts = [str(item or "").strip() for item in value.values()]
        return "\n".join(part for part in parts if part)
    return str(value or "").strip()


def _normalize_benchmark_text_line(value: Any) -> str:
    return benchmark_contracts.normalize_benchmark_text_line(value)


def _strip_rewritten_query_echo_lines(rewritten_blob: Any, *source_texts: Any) -> str:
    return benchmark_contracts.strip_rewritten_query_echo_lines(rewritten_blob, *source_texts)


def _extract_answer_reference_metrics(answer: str) -> dict[str, Any]:
    return benchmark_contracts.extract_answer_reference_metrics(answer)


def _evaluate_quality_assertions(case: dict[str, Any], record: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, bool], dict[str, bool]]:
    return benchmark_contracts.evaluate_quality_assertions(case, record)


def _validate_agent_payload_schema(payload: dict[str, Any]) -> dict[str, Any]:
    return benchmark_contracts.validate_agent_payload_schema(payload)


def extract_agent_benchmark_metrics(resp: dict[str, Any]) -> dict[str, Any]:
    return benchmark_contracts.extract_agent_benchmark_metrics(resp)


def _derive_rag_no_context(resp: dict[str, Any], timings: dict[str, Any]) -> tuple[int, str]:
    return benchmark_runner.derive_rag_no_context(resp, timings)


def _compact_benchmark_record(case: dict[str, Any], record: dict[str, Any]) -> dict[str, Any]:
    return benchmark_contracts.compact_benchmark_record(case, record)


def _collect_failed_trace_ids(records: list[dict[str, Any]] | None, *, kind: str) -> list[str]:
    return benchmark_contracts.collect_failed_trace_ids(records, kind=kind)


# ─── Per-query runners ────────────────────────────────────────────────────────

def _run_rag_query(ai_base: str, case: dict[str, Any]) -> dict[str, Any]:
    return benchmark_runner.run_rag_query(
        ai_base,
        case,
        post_json=_post_json,
        benchmark_trace_id=_benchmark_trace_id,
    )


def _run_agent_query(self_base: str, case: dict[str, Any], *, search_mode: str) -> dict[str, Any]:
    return benchmark_runner.run_agent_query(
        self_base,
        case,
        search_mode=search_mode,
        post_json=_post_json,
        benchmark_trace_id=_benchmark_trace_id,
        extract_agent_benchmark_metrics=extract_agent_benchmark_metrics,
        evaluate_quality_assertions=_evaluate_quality_assertions,
    )


def _resolve_case_batch(case_set_id: str, length: str, query_count: int) -> list[dict[str, Any]]:
    return resolve_case_batch(case_set_id, length, query_count)


def _resolve_case_batch_for_chain(case_set_id: str, length: str, query_count: int, chain: str) -> list[dict[str, Any]]:
    return benchmark_runner.resolve_case_batch_for_chain(case_set_id, length, query_count, chain)


def _resolve_case_queries(case_set_id: str, length: str, query_count: int) -> list[str]:
    return [str(case.get("query") or "") for case in _resolve_case_batch(case_set_id, length, query_count)]


def _select_total_case_batches(cases_by_length: dict[str, list[dict[str, Any]]], query_count: int) -> dict[str, list[dict[str, Any]]]:
    return benchmark_runner.select_total_case_batches(_BENCHMARK_LENGTHS, cases_by_length, query_count)


def _resolve_case_batches_for_chain(case_set_id: str, query_count: int, chain: str) -> dict[str, list[dict[str, Any]]]:
    return benchmark_runner.resolve_case_batches_for_chain(_BENCHMARK_LENGTHS, case_set_id, query_count, chain)


def _benchmark_case_detail(case: dict[str, Any]) -> dict[str, Any]:
    return benchmark_runner.benchmark_case_detail(case)


def _build_case_details(rows_by_length: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    return benchmark_runner.build_case_details(_BENCHMARK_LENGTHS, rows_by_length)


def _merge_case_batches_by_length(cases_by_chain: dict[str, dict[str, list[dict[str, Any]]]]) -> dict[str, list[dict[str, Any]]]:
    return benchmark_runner.merge_case_batches_by_length(_BENCHMARK_LENGTHS, cases_by_chain)


def _normalize_cases_by_chain_input(chains: list[str], cases_payload: dict[str, Any]) -> dict[str, dict[str, list[dict[str, Any]]]]:
    return benchmark_runner.normalize_cases_by_chain_input(_BENCHMARK_LENGTHS, CHAIN_SPECS, chains, cases_payload)


def _merge_assertion_limits(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    return benchmark_contracts.merge_assertion_limits(base, override)


def _group_rows_by_taxonomy(rows_by_bucket: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    return benchmark_contracts.group_rows_by_taxonomy(rows_by_bucket)


def _resolve_assertion_limits(chain: str, case_set_id: str, *, taxonomy: str = "") -> dict[str, Any]:
    return benchmark_contracts.resolve_assertion_limits(
        chain,
        case_set_id,
        DEFAULT_ASSERTION_LIMITS,
        ASSERTION_LIMIT_OVERRIDES_BY_CASE_SET,
        taxonomy=taxonomy,
    )


def _evaluate_assertions(
    chain: str,
    length: str,
    aggregate: dict[str, Any],
    case_set_id: str = "",
    records: list[dict[str, Any]] | None = None,
    *,
    limit_bucket: str | None = None,
    taxonomy: str = "",
) -> dict[str, Any]:
    return benchmark_contracts.evaluate_assertions(
        chain,
        length,
        aggregate,
        DEFAULT_ASSERTION_LIMITS,
        ASSERTION_LIMIT_OVERRIDES_BY_CASE_SET,
        case_set_id,
        records,
        limit_bucket=limit_bucket,
        taxonomy=taxonomy,
    )


def _attach_assertions(result: dict[str, Any], chains: list[str]) -> dict[str, Any]:
    return benchmark_contracts.attach_assertions(
        result,
        chains,
        DEFAULT_ASSERTION_LIMITS,
        ASSERTION_LIMIT_OVERRIDES_BY_CASE_SET,
    )


# ─── Aggregation ──────────────────────────────────────────────────────────────

def _percentile(values: list[float], p: float) -> float:
    return benchmark_contracts.percentile(values, p)


def _aggregate(records: list[dict[str, Any]]) -> dict[str, Any]:
    return benchmark_contracts.aggregate_records(records)


# ─── History storage ──────────────────────────────────────────────────────────

def _load_history() -> list[dict[str, Any]]:
    return benchmark_history_store.load_history(BENCHMARK_FILE, BENCHMARK_HISTORY_MAX, _attach_assertions)


def _save_result(result: dict[str, Any]) -> None:
    benchmark_history_store.save_result(BENCHMARK_FILE, BENCHMARK_HISTORY_MAX, result, _attach_assertions)


# ─── Benchmark generator (SSE source) ────────────────────────────────────────

def _build_result(chains: list[str], query_count: int, case_set_id: str, cases_by_chain: dict[str, dict[str, list[dict[str, Any]]]], rag_recs: dict[str, list[dict[str, Any]]], chain_records: dict[str, dict[str, list[dict[str, Any]]]]) -> dict[str, Any]:
    return benchmark_runner.build_result(
        _BENCHMARK_LENGTHS,
        CHAIN_SPECS,
        chains,
        query_count,
        case_set_id,
        cases_by_chain,
        rag_recs,
        chain_records,
        compact_benchmark_record=_compact_benchmark_record,
        group_rows_by_taxonomy=_group_rows_by_taxonomy,
        aggregate_records=_aggregate,
        attach_assertions=_attach_assertions,
    )


def _run_benchmark(chains: list[str], query_count: int, ai_base: str, self_base: str, case_set_id: str):
    yield from benchmark_runner.run_benchmark(
        _BENCHMARK_LENGTHS,
        CHAIN_SPECS,
        chains,
        query_count,
        ai_base,
        self_base,
        case_set_id,
        run_rag_query_fn=_run_rag_query,
        run_agent_query_fn=lambda base, case, search_mode: _run_agent_query(base, case, search_mode=search_mode),
        build_result_fn=_build_result,
        save_result_fn=_save_result,
    )


def _run_benchmark_job(chains: list[str], query_count: int, ai_base: str, self_base: str, case_set_id: str, report_progress, is_cancelled) -> dict[str, Any]:
    return benchmark_runner.run_benchmark_job(
        _BENCHMARK_LENGTHS,
        CHAIN_SPECS,
        chains,
        query_count,
        ai_base,
        self_base,
        case_set_id,
        report_progress=report_progress,
        is_cancelled=is_cancelled,
        run_rag_query_fn=_run_rag_query,
        run_agent_query_fn=lambda base, case, search_mode: _run_agent_query(base, case, search_mode=search_mode),
        compact_benchmark_record=_compact_benchmark_record,
        build_result_fn=_build_result,
        save_result_fn=_save_result,
    )


# ─── Pydantic models ──────────────────────────────────────────────────────────

class RunPayload(BaseModel):
    modules: list[str] = Field(default_factory=lambda: ["rag"])
    query_count_per_type: int = Field(default=3, ge=1, le=20)
    case_set_id: str = Field(default="regression_v1")


# ─── Endpoints ────────────────────────────────────────────────────────────────

@router.post("/run")
def post_run(payload: RunPayload, request: Request) -> StreamingResponse:
    modules = [m for m in payload.modules if m in ("rag", "agent", "hybrid")]
    if not modules:
        raise HTTPException(status_code=400, detail="请至少选择一个测试模块（rag、agent 或 hybrid）")
    query_count = max(1, min(20, payload.query_count_per_type))
    case_set_id = str(payload.case_set_id or "regression_v1").strip() or "regression_v1"
    if case_set_id not in QUERY_CASE_SETS:
        raise HTTPException(status_code=400, detail="未知回归测试集")
    ai_base = _ai_summary_base()
    self_base = _nav_dashboard_internal_base(request)

    def event_stream():
        try:
            for event in _run_benchmark(modules, query_count, ai_base, self_base, case_set_id):
                yield f"data: {json.dumps(build_benchmark_stream_event(event), ensure_ascii=False)}\n\n"
        except Exception as exc:
            yield f"data: {json.dumps(build_benchmark_stream_event({'type': 'error', 'message': str(exc)}), ensure_ascii=False)}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/history", response_model=BenchmarkHistoryResponse, response_model_exclude_unset=True)
def get_history() -> dict[str, Any]:
    return build_benchmark_history_response(_load_history())


@router.get("/cases", response_model=BenchmarkCaseSetsResponse, response_model_exclude_unset=True)
def get_benchmark_case_sets() -> dict[str, Any]:
    response = build_benchmark_case_sets_response(
        [
            {
                "id": case_id,
                "label": CASE_SET_LABELS.get(case_id, case_id),
                "lengths": {length: len(queries) for length, queries in lengths.items()},
                "max_query_count_per_type": int((CASE_SET_METADATA.get(case_id) or {}).get("max_query_count_per_type") or 0),
                "taxonomy_counts": dict((CASE_SET_METADATA.get(case_id) or {}).get("taxonomy_counts") or {}),
                "source_counts": dict((CASE_SET_METADATA.get(case_id) or {}).get("source_counts") or {}),
                "supported_modules": list((CASE_SET_METADATA.get(case_id) or {}).get("supported_modules") or []),
                "module_case_counts": dict((CASE_SET_METADATA.get(case_id) or {}).get("module_case_counts") or {}),
                "module_length_counts": dict((CASE_SET_METADATA.get(case_id) or {}).get("module_length_counts") or {}),
                "module_max_query_count_per_type": dict((CASE_SET_METADATA.get(case_id) or {}).get("module_max_query_count_per_type") or {}),
            }
            for case_id, lengths in QUERY_CASE_SETS.items()
        ],
        [{"id": chain, **spec} for chain, spec in CHAIN_SPECS.items()],
    )
    return response


@router.post("/jobs")
def create_benchmark_job(payload: RunPayload, request: Request) -> dict[str, Any]:
    modules = [m for m in payload.modules if m in ("rag", "agent", "hybrid")]
    if not modules:
        raise HTTPException(status_code=400, detail="请至少选择一个测试模块（rag、agent 或 hybrid）")
    query_count = max(1, min(20, payload.query_count_per_type))
    case_set_id = str(payload.case_set_id or "regression_v1").strip() or "regression_v1"
    if case_set_id not in QUERY_CASE_SETS:
        raise HTTPException(status_code=400, detail="未知回归测试集")
    ai_base = _ai_summary_base()
    self_base = _nav_dashboard_internal_base(request)
    job = dashboard_jobs.create_job(
        job_type="benchmark",
        label="性能基准测试",
        metadata={"modules": modules, "query_count_per_type": query_count, "case_set_id": case_set_id},
        target=lambda report_progress, is_cancelled: _run_benchmark_job(modules, query_count, ai_base, self_base, case_set_id, report_progress, is_cancelled),
    )
    return build_benchmark_job_response(ok=True, job=job)


@router.get("/jobs/{job_id}")
def get_benchmark_job(job_id: str) -> dict[str, Any]:
    job = dashboard_jobs.get_job(job_id)
    if not job or job.get("type") != "benchmark":
        raise HTTPException(status_code=404, detail="Job not found")
    return build_benchmark_job_response(ok=True, job=job)


@router.post("/jobs/{job_id}/cancel")
def cancel_benchmark_job(job_id: str) -> dict[str, Any]:
    job = dashboard_jobs.request_cancel(job_id)
    if not job or job.get("type") != "benchmark":
        raise HTTPException(status_code=404, detail="Job not found")
    return build_benchmark_job_response(ok=True, job=job)


@router.delete("/history")
def clear_history() -> dict[str, Any]:
    if BENCHMARK_FILE.exists():
        BENCHMARK_FILE.write_text(
            json.dumps({"results": []}, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    return {"ok": True}


# ─── Router Classification Benchmark ─────────────────────────────────────────
# Each case has:
#   query          — raw question text
#   mock_label     — synthetic LLM classifier label (MEDIA / TECH / OTHER)
#   mock_domain    — synthetic LLM domain
#   mock_entities  — synthetic entity list
#   mock_lu        — synthetic lookup_mode
#   exp_domain     — expected RouterDecision.domain
#   exp_arb        — expected RouterDecision.arbitration
#   note           — human-readable annotation
# The cases are deliberately diverse and cover the full arbitration matrix:
# tech_primary, entity_wins, media_surface_wins, abstract_concept_wins,
# mixed_due_to_entity_plus_tech, llm_media_weak_general, followup, etc.

LEGACY_ROUTER_CLASSIFICATION_CASES: list[dict[str, Any]] = [
    # ── tech_primary ──────────────────────────────────────────────────────────
    {"query": "机器学习的概念和应用",          "mock_label": "TECH", "mock_domain": "tech",    "mock_entities": [],        "mock_lu": "general_lookup",  "exp_domain": "tech",    "exp_arb": "tech_primary",            "note": "core tech — CJK token fix"},
    {"query": "深度学习架构原理是什么",          "mock_label": "TECH", "mock_domain": "tech",    "mock_entities": [],        "mock_lu": "general_lookup",  "exp_domain": "tech",    "exp_arb": "tech_primary",            "note": "tech with lexical cues"},
    {"query": "Transformer注意力机制的数学原理", "mock_label": "TECH", "mock_domain": "tech",    "mock_entities": [],        "mock_lu": "general_lookup",  "exp_domain": "tech",    "exp_arb": "tech_primary",            "note": "latin+CJK tech"},
    {"query": "RAG检索增强生成的工作流程",       "mock_label": "TECH", "mock_domain": "tech",    "mock_entities": [],        "mock_lu": "general_lookup",  "exp_domain": "tech",    "exp_arb": "tech_primary",            "note": "RAG acronym tech cue"},
    {"query": "什么是向量数据库",               "mock_label": "TECH", "mock_domain": "tech",    "mock_entities": [],        "mock_lu": "general_lookup",  "exp_domain": "tech",    "exp_arb": "tech_primary",            "note": "向量 tech cue"},
    # ── entity_wins / media_surface_wins ──────────────────────────────────────
    {"query": "《教父》的导演是谁",              "mock_label": "MEDIA", "mock_domain": "media",  "mock_entities": ["教父"],   "mock_lu": "entity_lookup",   "exp_domain": "media",   "exp_arb_any": ["entity_wins", "media_surface_wins"], "note": "《》 title → surface or entity"},
    {"query": "《三体》作者刘慈欣的其他作品",    "mock_label": "MEDIA", "mock_domain": "media",  "mock_entities": ["三体"],   "mock_lu": "entity_lookup",   "exp_domain": "media",   "exp_arb_any": ["entity_wins", "media_surface_wins"], "note": "book entity"},
    {"query": "波拉尼奥的小说有哪些",            "mock_label": "MEDIA", "mock_domain": "media",  "mock_entities": ["波拉尼奥"], "mock_lu": "entity_lookup",  "exp_domain": "media",   "exp_arb_any": ["entity_wins", "media_surface_wins"], "note": "author entity; 小说 fires media_surface_wins too"},
    # ── media_surface_wins ────────────────────────────────────────────────────
    {"query": "推荐几部法国新浪潮电影",          "mock_label": "MEDIA", "mock_domain": "media",  "mock_entities": [],        "mock_lu": "filter_search",   "exp_domain": "media",   "exp_arb": "media_surface_wins",      "note": "电影 surface cue"},
    {"query": "2023年值得一看的日本动漫",        "mock_label": "MEDIA", "mock_domain": "media",  "mock_entities": [],        "mock_lu": "filter_search",   "exp_domain": "media",   "exp_arb": "media_surface_wins",      "note": "动漫 surface + year"},
    # ── abstract_concept_wins ─────────────────────────────────────────────────
    {"query": "魔幻现实主义的叙事手法",          "mock_label": "MEDIA", "mock_domain": "media",  "mock_entities": [],        "mock_lu": "concept_lookup",  "exp_domain": "media",   "exp_arb": "abstract_concept_wins",   "note": "abstract concept no surface"},
    {"query": "新浪潮电影的主要特征",            "mock_label": "MEDIA", "mock_domain": "media",  "mock_entities": [],        "mock_lu": "concept_lookup",  "exp_domain": "media",   "exp_arb_any": ["abstract_concept_wins", "media_surface_wins"], "note": "新浪潮 abstract"},
    {"query": "拉美文学的代表作家",              "mock_label": "MEDIA", "mock_domain": "media",  "mock_entities": [],        "mock_lu": "concept_lookup",  "exp_domain": "media",   "exp_arb_any": ["abstract_concept_wins", "media_surface_wins"], "note": "拉美文学"},
    # ── mixed_due_to_entity_plus_tech ─────────────────────────────────────────
    {"query": "机器学习在电影推荐系统里的应用",  "mock_label": "TECH",  "mock_domain": "tech",   "mock_entities": ["电影推荐系统"], "mock_lu": "general_lookup", "exp_domain": "media",  "exp_arb": "mixed_due_to_entity_plus_tech", "note": "tech+entity=mixed"},
    # ── llm_media_weak_general ────────────────────────────────────────────────
    {"query": "从整体上来说这个领域",            "mock_label": "MEDIA", "mock_domain": "media",  "mock_entities": [],        "mock_lu": "general_lookup",  "exp_domain": "general", "exp_arb": "llm_media_weak_general",  "note": "LLM-only media, no anchor"},
    # ── general_fallback ──────────────────────────────────────────────────────
    {"query": "什么是量子纠缠",                 "mock_label": "OTHER", "mock_domain": "general", "mock_entities": [],        "mock_lu": "general_lookup",  "exp_domain": "general", "exp_arb": "general_fallback",        "note": "pure knowledge qa"},
    {"query": "气候变化的主要原因",              "mock_label": "OTHER", "mock_domain": "general", "mock_entities": [],        "mock_lu": "general_lookup",  "exp_domain": "general", "exp_arb": "general_fallback",        "note": "general knowledge"},
    # ── tech_signal_only ──────────────────────────────────────────────────────
    {"query": "如何实现微服务架构",              "mock_label": "OTHER", "mock_domain": "general", "mock_entities": [],        "mock_lu": "general_lookup",  "exp_domain": "tech",    "exp_arb": "tech_signal_only",        "note": "lexical tech without LLM confirm"},
    # ── CJK token counting (regression for trace_8bc75ef5b2ac4871) ────────────
    {"query": "机器学习的概念", "mock_label": "TECH", "mock_domain": "tech", "mock_entities": [], "mock_lu": "general_lookup", "exp_domain": "tech", "exp_arb": "tech_primary", "note": "6 CJK chars → medium profile, must not be short"},
]

# History file for router classification results
ROUTER_CLS_HISTORY_MAX = benchmark_history_store.ROUTER_CLS_HISTORY_MAX

_ROUTER_CLS_ALLOWED_DOMAINS = benchmark_history_store.ROUTER_CLS_ALLOWED_DOMAINS
_ROUTER_CLS_ALLOWED_LABELS = benchmark_history_store.ROUTER_CLS_ALLOWED_LABELS
_ROUTER_CLS_ALLOWED_LOOKUP_MODES = benchmark_history_store.ROUTER_CLS_ALLOWED_LOOKUP_MODES


def _coerce_router_cls_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        items = value
    elif isinstance(value, tuple):
        items = list(value)
    else:
        text = str(value).replace("\r", "\n")
        for token in ["|", ",", "\n", "\t"]:
            text = text.replace(token, "\n")
        items = text.split("\n")
    normalized: list[str] = []
    for item in items:
        text = str(item or "").strip()
        if text:
            normalized.append(text)
    return normalized


def _derive_router_cls_mock_label(expected_domain: str) -> str:
    if expected_domain == "media":
        return "MEDIA"
    if expected_domain == "tech":
        return "TECH"
    return "OTHER"


def _derive_router_cls_mock_domain(expected_domain: str) -> str:
    return expected_domain if expected_domain in {"media", "tech"} else "general"


def _derive_router_cls_lookup_mode(expected_domain: str, expected_arbitration: list[str], mock_entities: list[str]) -> str:
    if mock_entities:
        return "entity_lookup"
    if any("concept" in item for item in expected_arbitration):
        return "concept_lookup"
    if expected_domain == "media" and any("surface" in item for item in expected_arbitration):
        return "filter_search"
    return "general_lookup"


def _normalize_router_cls_case_record(payload: dict[str, Any], *, case_id: str | None = None) -> dict[str, Any]:
    return benchmark_history_store.normalize_router_cls_case_record(payload, case_id=case_id)


def _default_router_cls_case_records() -> list[dict[str, Any]]:
    return benchmark_history_store.default_router_cls_case_records(LEGACY_ROUTER_CLASSIFICATION_CASES)


def _load_router_cls_case_records() -> list[dict[str, Any]]:
    return benchmark_history_store.load_router_cls_case_records(ROUTER_CLS_CASES_FILE, LEGACY_ROUTER_CLASSIFICATION_CASES)


def _save_router_cls_case_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return benchmark_history_store.save_router_cls_case_records(ROUTER_CLS_CASES_FILE, records)


def _load_router_cls_history() -> list[dict[str, Any]]:
    return benchmark_history_store.load_router_cls_history(ROUTER_CLS_HISTORY_FILE)


def _save_router_cls_result(result: dict[str, Any]) -> None:
    benchmark_history_store.save_router_cls_result(ROUTER_CLS_HISTORY_FILE, result)


class RouterClassificationCasePayload(BaseModel):
    query: str = Field(..., min_length=1, max_length=400)
    expected_domain: str = Field(..., min_length=1)
    expected_arbitration: str | list[str] = Field(...)
    expected_query_class: str | None = Field(default=None)
    subject_scope: str | None = Field(default=None)
    time_scope_type: str | None = Field(default=None)
    answer_shape: str | None = Field(default=None)
    media_family: str | None = Field(default=None)
    followup_mode: str | None = Field(default=None)
    note: str = Field(default="", max_length=300)
    mock_label: str | None = Field(default=None)
    mock_domain: str | None = Field(default=None)
    mock_entities: list[str] | str | None = Field(default=None)
    mock_lookup_mode: str | None = Field(default=None)


def _build_router_cls_case_response(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(record.get("id") or ""),
        "query": str(record.get("query") or ""),
        "expected_domain": str(record.get("expected_domain") or ""),
        "expected_arbitration": record.get("expected_arbitration") or "",
        "expected_query_class": str(record.get("expected_query_class") or ""),
        "subject_scope": str(record.get("subject_scope") or ""),
        "time_scope_type": str(record.get("time_scope_type") or ""),
        "answer_shape": str(record.get("answer_shape") or ""),
        "media_family": str(record.get("media_family") or ""),
        "followup_mode": str(record.get("followup_mode") or ""),
        "note": str(record.get("note") or ""),
        "mock_label": str(record.get("mock_label") or ""),
        "mock_domain": str(record.get("mock_domain") or ""),
        "mock_entities": list(record.get("mock_entities") or []),
        "mock_lookup_mode": str(record.get("mock_lookup_mode") or ""),
        "updated_at": str(record.get("updated_at") or ""),
    }


def _run_router_classification_suite() -> dict[str, Any]:
    """Run all stored router classification cases in-process without LLM calls.

    Returns a result dict with per-case outcomes and aggregate pass/fail counts.
    """
    from contextlib import ExitStack as _ExitStack
    from unittest.mock import patch as _patch

    from nav_dashboard.web.services.agent.domain import router_service as _router_svc

    _empty_quota: dict = {}

    def _resolve_profile(q: str) -> dict:
        return _router_svc._resolve_query_profile(q)

    def _decide(
        query: str,
        mock_label: str,
        mock_domain: str,
        mock_entities: list,
        mock_lu: str,
        *,
        history: list[dict[str, Any]] | None = None,
        previous_trace_context: dict[str, Any] | None = None,
    ) -> Any:
        mock_llm = {
            "label": mock_label,
            "domain": mock_domain,
            "lookup_mode": mock_lu,
            "entities": list(mock_entities),
            "filters": {},
            "time_window": {},
            "ranking": {},
            "followup_target": "",
            "needs_comparison": False,
            "needs_explanation": False,
            "confidence": 0.85,
            "rewritten_queries": {},
        }
        with _ExitStack() as stack:
            stack.enter_context(_patch.object(_router_svc, "_classify_media_query_with_llm", return_value=mock_llm))
            stack.enter_context(_patch.object(_router_svc, "_rewrite_tool_queries_with_llm", return_value={}))
            if history or previous_trace_context is not None:
                stack.enter_context(_patch.object(_router_svc, "_find_previous_trace_context", return_value=previous_trace_context))
            decision, _, _ = _router_svc._build_router_decision(
                question=query,
                history=list(history or []),
                quota_state=_empty_quota,
                query_profile=_resolve_profile(query),
            )
        return decision

    cases_out: list[dict[str, Any]] = []
    passed = 0
    failed = 0
    violations: list[dict[str, Any]] = []

    stored_cases = _load_router_cls_case_records()

    for case in stored_cases:
        query = str(case.get("query") or "")
        exp_domain = str(case.get("expected_domain") or case.get("exp_domain") or "")
        raw_expected_arbitration = case.get("expected_arbitration")
        exp_arb_any: list[str] = _coerce_router_cls_list(raw_expected_arbitration)
        exp_arb = exp_arb_any[0] if len(exp_arb_any) == 1 else ""
        case_id = str(case.get("id") or "")
        history = case.get("history") if isinstance(case.get("history"), list) else []
        previous_trace_context = case.get("previous_trace_context") if isinstance(case.get("previous_trace_context"), dict) else None

        try:
            decision = _decide(
                query,
                str(case.get("mock_label") or "OTHER"),
                str(case.get("mock_domain") or "general"),
                list(case.get("mock_entities") or []),
                str(case.get("mock_lookup_mode") or case.get("mock_lu") or "general_lookup"),
                history=history,
                previous_trace_context=previous_trace_context,
            )
            actual_domain = str(decision.domain or "")
            actual_arb = str(decision.arbitration or "")
            actual_query_class = str(decision.query_class or "")
            actual_subject_scope = str(decision.subject_scope or "")
            actual_time_scope_type = str(decision.time_scope_type or "")
            actual_answer_shape = str(decision.answer_shape or "")
            actual_media_family = str(decision.media_family or "")
            actual_followup_mode = normalize_followup_mode(decision.followup_mode)
            expected_query_class = str(case.get("expected_query_class") or "")
            expected_subject_scope = str(case.get("subject_scope") or "")
            expected_time_scope_type = str(case.get("time_scope_type") or "")
            expected_answer_shape = str(case.get("answer_shape") or "")
            expected_media_family = str(case.get("media_family") or "")
            expected_followup_mode = normalize_followup_mode(case.get("followup_mode"))

            domain_ok = actual_domain == exp_domain
            arb_ok = (actual_arb == exp_arb) if exp_arb else (actual_arb in exp_arb_any) if exp_arb_any else True
            query_class_ok = not expected_query_class or actual_query_class == expected_query_class
            subject_scope_ok = not expected_subject_scope or actual_subject_scope == expected_subject_scope
            time_scope_type_ok = not expected_time_scope_type or actual_time_scope_type == expected_time_scope_type
            answer_shape_ok = not expected_answer_shape or actual_answer_shape == expected_answer_shape
            media_family_ok = not expected_media_family or actual_media_family == expected_media_family
            followup_mode_ok = not expected_followup_mode or actual_followup_mode == expected_followup_mode
            ok = all((domain_ok, arb_ok, query_class_ok, subject_scope_ok, time_scope_type_ok, answer_shape_ok, media_family_ok, followup_mode_ok))

            row: dict[str, Any] = {
                "id": case_id,
                "query": query,
                "expected_domain": exp_domain,
                "expected_arbitration": raw_expected_arbitration or exp_arb or exp_arb_any,
                "expected_query_class": str(case.get("expected_query_class") or ""),
                "subject_scope": str(case.get("subject_scope") or ""),
                "time_scope_type": str(case.get("time_scope_type") or ""),
                "answer_shape": str(case.get("answer_shape") or ""),
                "media_family": str(case.get("media_family") or ""),
                "followup_mode": str(case.get("followup_mode") or ""),
                "actual_domain": actual_domain,
                "actual_arbitration": actual_arb,
                "actual_query_class": str(decision.query_class or ""),
                "actual_subject_scope": str(decision.subject_scope or ""),
                "actual_time_scope_type": str(decision.time_scope_type or ""),
                "actual_answer_shape": str(decision.answer_shape or ""),
                "actual_media_family": str(decision.media_family or ""),
                "actual_followup_mode": str(decision.followup_mode or ""),
                "pass": ok,
                "note": str(case.get("note") or ""),
            }
            if not ok:
                row["violation"] = {
                    "domain_mismatch": not domain_ok,
                    "arbitration_mismatch": not arb_ok,
                    "query_class_mismatch": not query_class_ok,
                    "subject_scope_mismatch": not subject_scope_ok,
                    "time_scope_type_mismatch": not time_scope_type_ok,
                    "answer_shape_mismatch": not answer_shape_ok,
                    "media_family_mismatch": not media_family_ok,
                    "followup_mode_mismatch": not followup_mode_ok,
                }
                violations.append(
                    {
                        "query": query,
                        "expected_domain": exp_domain,
                        "actual_domain": actual_domain,
                        "expected_arbitration": exp_arb or exp_arb_any,
                        "actual_arbitration": actual_arb,
                        "expected_query_class": expected_query_class,
                        "actual_query_class": actual_query_class,
                        "expected_subject_scope": expected_subject_scope,
                        "actual_subject_scope": actual_subject_scope,
                        "expected_time_scope_type": expected_time_scope_type,
                        "actual_time_scope_type": actual_time_scope_type,
                        "expected_answer_shape": expected_answer_shape,
                        "actual_answer_shape": actual_answer_shape,
                        "expected_media_family": expected_media_family,
                        "actual_media_family": actual_media_family,
                        "expected_followup_mode": expected_followup_mode,
                        "actual_followup_mode": actual_followup_mode,
                    }
                )

            cases_out.append(row)
            if ok:
                passed += 1
            else:
                failed += 1
        except Exception as exc:
            cases_out.append({"id": case_id, "query": query, "pass": False, "error": str(exc)[:200]})
            failed += 1
            violations.append({"query": query, "error": str(exc)[:200]})

    total = passed + failed
    result = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "total": total,
        "passed": passed,
        "failed": failed,
        "pass_rate": round(passed / total, 4) if total else 0.0,
        "cases": cases_out,
        "violations": violations,
    }
    _save_router_cls_result(result)
    return result


@router.post("/router-classification", response_model=BenchmarkRouterClassificationResponse, response_model_exclude_unset=True)
def post_router_classification() -> dict[str, Any]:
    """Run the in-process router classification regression suite and return results."""
    result = _run_router_classification_suite()
    return build_benchmark_router_classification_response(result)


@router.get("/router-classification/cases", response_model=BenchmarkRouterClassificationCasesResponse, response_model_exclude_unset=True)
def get_router_classification_cases() -> dict[str, Any]:
    records = _load_router_cls_case_records()
    return build_benchmark_router_classification_cases_response([_build_router_cls_case_response(record) for record in records])


@router.post("/router-classification/cases", response_model=BenchmarkRouterClassificationCaseMutationResponse, response_model_exclude_unset=True)
def create_router_classification_case(payload: RouterClassificationCasePayload) -> dict[str, Any]:
    records = _load_router_cls_case_records()
    try:
        record = _normalize_router_cls_case_record(payload.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    records.append(record)
    _save_router_cls_case_records(records)
    return build_benchmark_router_classification_case_mutation_response(
        ok=True,
        case=_build_router_cls_case_response(record),
        cases=[_build_router_cls_case_response(item) for item in records],
    )


@router.patch("/router-classification/cases/{case_id}", response_model=BenchmarkRouterClassificationCaseMutationResponse, response_model_exclude_unset=True)
def update_router_classification_case(case_id: str, payload: RouterClassificationCasePayload) -> dict[str, Any]:
    normalized_case_id = str(case_id or "").strip()
    records = _load_router_cls_case_records()
    index = next((idx for idx, item in enumerate(records) if str(item.get("id") or "") == normalized_case_id), -1)
    if index < 0:
        raise HTTPException(status_code=404, detail="未找到对应的 router classification case")
    try:
        record = _normalize_router_cls_case_record(payload.model_dump(), case_id=normalized_case_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    records[index] = record
    _save_router_cls_case_records(records)
    return build_benchmark_router_classification_case_mutation_response(
        ok=True,
        case=_build_router_cls_case_response(record),
        cases=[_build_router_cls_case_response(item) for item in records],
    )


@router.delete("/router-classification/cases/{case_id}", response_model=BenchmarkRouterClassificationCaseMutationResponse, response_model_exclude_unset=True)
def delete_router_classification_case(case_id: str) -> dict[str, Any]:
    normalized_case_id = str(case_id or "").strip()
    records = _load_router_cls_case_records()
    filtered = [item for item in records if str(item.get("id") or "") != normalized_case_id]
    if len(filtered) == len(records):
        raise HTTPException(status_code=404, detail="未找到对应的 router classification case")
    _save_router_cls_case_records(filtered)
    return build_benchmark_router_classification_case_mutation_response(
        ok=True,
        cases=[_build_router_cls_case_response(item) for item in filtered],
    )


@router.get("/router-classification/history", response_model=BenchmarkRouterClassificationHistoryResponse, response_model_exclude_unset=True)
def get_router_classification_history() -> dict[str, Any]:
    return build_benchmark_router_classification_history_response(_load_router_cls_history())


# ─── Unit / regression test runner ───────────────────────────────────────────

_REPO_ROOT = Path(__file__).resolve().parents[3]
_UNIT_TEST_RUNNER = _REPO_ROOT / "scripts" / "dev" / "run_isolated_unit_suite.py"

UNIT_TEST_SUITES: dict[str, dict[str, Any]] = {
    "regression_router": {
        "label": "Router 回归",
        "file": _REPO_ROOT / "scripts" / "regression" / "regression_router.py",
    },
    "post_retrieval": {
        "label": "PostRetrieval Policy",
        "file": _REPO_ROOT / "tests" / "post_retrieval" / "test_policy.py",
    },
    "answer": {
        "label": "Answer Policy",
        "file": _REPO_ROOT / "tests" / "answer" / "test_policy.py",
    },
    "agent_e2e": {
        "label": "Agent E2E Chain",
        "file": _REPO_ROOT / "tests" / "agent_e2e" / "test_chain.py",
    },
}

def _run_unit_test_suite(suite_id: str, suite_def: dict[str, Any]) -> dict[str, Any]:
    """Run a unittest suite in an isolated subprocess to avoid shared module state."""
    file_path = Path(suite_def["file"])
    label = str(suite_def.get("label") or suite_id)
    timeout_seconds = max(30, int(suite_def.get("timeout_seconds") or 300))

    if not file_path.is_file():
        return {
            "id": suite_id,
            "label": label,
            "elapsed_seconds": 0.0,
            "passed": 0,
            "failed": 0,
            "errors": 1,
            "tests": [{"id": "<setup>", "status": "error", "message": f"File not found: {file_path}"}],
        }

    try:
        t0 = time.perf_counter()
        completed = subprocess.run(
            [
                sys.executable,
                str(_UNIT_TEST_RUNNER),
                suite_id,
                label,
                str(file_path),
            ],
            cwd=str(_REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
        elapsed = round(time.perf_counter() - t0, 3)
        stdout = str(completed.stdout or "").strip()
        stderr = str(completed.stderr or "").strip()
        if not stdout:
            raise RuntimeError(stderr or f"isolated unit runner returned {completed.returncode} without JSON output")
        result = json.loads(stdout)
        if not isinstance(result, dict):
            raise ValueError("isolated unit runner returned a non-object payload")
        result.setdefault("id", suite_id)
        result.setdefault("label", label)
        result["elapsed_seconds"] = round(float(result.get("elapsed_seconds") or elapsed), 3)
        result["passed"] = int(result.get("passed", 0) or 0)
        result["failed"] = int(result.get("failed", 0) or 0)
        result["errors"] = int(result.get("errors", 0) or 0)
        result["tests"] = list(result.get("tests") or [])
        if completed.returncode != 0 and result["errors"] <= 0 and result["failed"] <= 0:
            result["errors"] = 1
            result["tests"] = list(result["tests"]) + [{
                "id": "<runner>",
                "status": "error",
                "message": stderr[:400] or f"isolated unit runner exited with code {completed.returncode}",
            }]
        return result
    except subprocess.TimeoutExpired:
        return {
            "id": suite_id,
            "label": label,
            "elapsed_seconds": float(timeout_seconds),
            "passed": 0,
            "failed": 0,
            "errors": 1,
            "tests": [{"id": "<timeout>", "status": "error", "message": f"timeout after {timeout_seconds}s"}],
        }
    except Exception as exc:
        return {
            "id": suite_id,
            "label": label,
            "elapsed_seconds": 0.0,
            "passed": 0,
            "failed": 0,
            "errors": 1,
            "tests": [{"id": "<import>", "status": "error", "message": str(exc)[:400]}],
        }


class UnitTestPayload(BaseModel):
    suites: list[str] = Field(
        default_factory=lambda: list(UNIT_TEST_SUITES.keys())
    )


@router.get("/unit-tests/suites")
def get_unit_test_suites() -> dict[str, Any]:
    """Return available unittest suite definitions."""
    return {
        "suites": [
            {"id": sid, "label": sdef["label"]}
            for sid, sdef in UNIT_TEST_SUITES.items()
        ]
    }


@router.post("/unit-tests")
def post_unit_tests(payload: UnitTestPayload) -> dict[str, Any]:
    """Run selected unittest suites in-process without LLM calls."""
    suites_to_run = [s for s in payload.suites if s in UNIT_TEST_SUITES]
    if not suites_to_run:
        raise HTTPException(status_code=400, detail="未选择有效测试套件")

    suite_results: list[dict[str, Any]] = []
    total_passed = 0
    total_failed = 0
    total_errors = 0

    for suite_id in suites_to_run:
        result = _run_unit_test_suite(suite_id, UNIT_TEST_SUITES[suite_id])
        suite_results.append(result)
        total_passed += result["passed"]
        total_failed += result["failed"]
        total_errors += result["errors"]

    return {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "total_passed": total_passed,
        "total_failed": total_failed,
        "total_errors": total_errors,
        "suites": suite_results,
    }
