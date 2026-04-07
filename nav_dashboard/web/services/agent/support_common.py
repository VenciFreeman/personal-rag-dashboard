from __future__ import annotations

import importlib
import json
import os
import re
import time as _time
import urllib.parse as urlparse
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from nav_dashboard.web.clients.internal_services import InternalServiceError, request_json
from nav_dashboard.web.services.agent import agent_types
from nav_dashboard.web.services.agent.agent_boundaries import (
    get_cached_web_results,
    log_no_context_query as boundary_log_no_context_query,
    run_doc_graph_expand,
    run_media_graph_expand,
    set_cached_web_results,
)
from nav_dashboard.web.services.agent.agent_observability_owner import (
    build_agent_trace_record as build_agent_trace_record_owner,
    record_agent_metrics as record_agent_metrics,
    write_agent_trace_record as write_agent_trace_record_owner,
)
from nav_dashboard.web.services.agent.runtime import composition as _composition
from nav_dashboard.web.services.agent.session_state_runner import (
    persist_session_artifacts,
    record_metrics_safe,
)
from nav_dashboard.web.services.agent.trace_builder import build_agent_trace_record as trace_build_agent_trace_record
from nav_dashboard.web.services.answer.streaming_renderer import render_streaming_answer
from nav_dashboard.web.services.media.media_query_adapter import SchemaProjectionAdapter
from nav_dashboard.web.services.media.media_taxonomy import TMDB_AUDIOVISUAL_CUES
from nav_dashboard.web.services.planner import planner_contracts
from nav_dashboard.web.services.planner import router_config
from nav_dashboard.web.services.planner.planner_contracts import deserialize_router_decision as deserialize_router_decision_layer
from nav_dashboard.web.services.runtime_paths import DEBUG_DIR
from nav_dashboard.web.services.shared import quota_service
from nav_dashboard.web.services.tooling.tool_executor import build_confirmation_payload, execute_tool_phase, resolve_allowed_plan
from nav_dashboard.web.services.tooling.tool_option_assembly import (
    build_media_tool_options_from_decision as assemble_media_tool_options_from_decision,
    get_lookup_mode_from_state as get_lookup_mode_from_state_layer,
)
from nav_dashboard.web.services.tooling.tool_plan_runner import update_debug_trace_with_tool_plan
from nav_dashboard.web.services.tooling.tool_planning_pipeline import run_tool_planning_pipeline

from .domain import media_constants
from nav_dashboard.web.services.ontologies.music_work_signature import is_specific_music_marker
from .domain import media_core
from .infra import runtime_infra

try:
    from core_service import get_settings
    from core_service.trace_store import write_trace_record
except Exception:  # noqa: BLE001
    get_settings = None
    write_trace_record = None


AgentRuntimeState = agent_types.AgentRuntimeState
ExecutionPlan = agent_types.ExecutionPlan
PlanningArtifact = agent_types.PlanningArtifact
PlannedToolCall = agent_types.PlannedToolCall
PostRetrievalAssessment = agent_types.PostRetrievalAssessment
RouterContextResolution = agent_types.RouterContextResolution
RouterDecision = agent_types.RouterDecision
ToolExecution = agent_types.ToolExecution

CLASSIFIER_LABEL_MEDIA = agent_types.CLASSIFIER_LABEL_MEDIA
CLASSIFIER_LABEL_OTHER = agent_types.CLASSIFIER_LABEL_OTHER
CLASSIFIER_LABEL_TECH = agent_types.CLASSIFIER_LABEL_TECH
QUERY_TYPE_GENERAL = agent_types.QUERY_TYPE_GENERAL
QUERY_TYPE_MEDIA = agent_types.QUERY_TYPE_MEDIA
QUERY_TYPE_MIXED = agent_types.QUERY_TYPE_MIXED
QUERY_TYPE_TECH = agent_types.QUERY_TYPE_TECH
TOOL_EXPAND_DOC_QUERY = agent_types.TOOL_EXPAND_DOC_QUERY
TOOL_EXPAND_MEDIAWIKI_CONCEPT = agent_types.TOOL_EXPAND_MEDIAWIKI_CONCEPT
TOOL_EXPAND_MEDIA_QUERY = agent_types.TOOL_EXPAND_MEDIA_QUERY
TOOL_NAMES = agent_types.TOOL_NAMES
TOOL_PARSE_MEDIAWIKI = agent_types.TOOL_PARSE_MEDIAWIKI
TOOL_QUERY_DOC_RAG = agent_types.TOOL_QUERY_DOC_RAG
TOOL_QUERY_MEDIA = agent_types.TOOL_QUERY_MEDIA
TOOL_SEARCH_BANGUMI = agent_types.TOOL_SEARCH_BANGUMI
TOOL_SEARCH_BY_CREATOR = agent_types.TOOL_SEARCH_BY_CREATOR
TOOL_SEARCH_MEDIAWIKI = agent_types.TOOL_SEARCH_MEDIAWIKI
TOOL_SEARCH_TMDB = agent_types.TOOL_SEARCH_TMDB
TOOL_SEARCH_WEB = agent_types.TOOL_SEARCH_WEB

SHORT_QUERY_MAX_TOKENS = 5
LONG_QUERY_MIN_TOKENS = int(os.getenv("NAV_DASHBOARD_LONG_QUERY_MIN_TOKENS", "12") or "12")
SHORT_QUERY_THRESHOLD_DELTA = float(os.getenv("NAV_DASHBOARD_SHORT_QUERY_THRESHOLD_DELTA", "-0.08") or "-0.08")
LONG_QUERY_THRESHOLD_DELTA = float(os.getenv("NAV_DASHBOARD_LONG_QUERY_THRESHOLD_DELTA", "0.08") or "0.08")
SHORT_QUERY_VECTOR_TOP_N_DELTA = int(os.getenv("NAV_DASHBOARD_SHORT_QUERY_VECTOR_TOP_N_DELTA", "6") or "6")
LONG_QUERY_VECTOR_TOP_N_DELTA = int(os.getenv("NAV_DASHBOARD_LONG_QUERY_VECTOR_TOP_N_DELTA", "-4") or "-4")
SHORT_QUERY_TOP_K_DELTA = int(os.getenv("NAV_DASHBOARD_SHORT_QUERY_TOP_K_DELTA", "2") or "2")
LONG_QUERY_TOP_K_DELTA = int(os.getenv("NAV_DASHBOARD_LONG_QUERY_TOP_K_DELTA", "-1") or "-1")
DOC_VECTOR_TOP_N = int(os.getenv("NAV_DASHBOARD_DOC_VECTOR_TOP_N", "12") or "12")
DOC_SCORE_THRESHOLD = float(os.getenv("NAV_DASHBOARD_DOC_SCORE_THRESHOLD", "0.35") or "0.35")
TECH_QUERY_DOC_SIM_THRESHOLD = float(os.getenv("NAV_DASHBOARD_TECH_QUERY_DOC_SIM_THRESHOLD", "0.3") or "0.3")
LOCAL_TOP_K_DOC = int(os.getenv("NAV_DASHBOARD_LOCAL_TOP_K_DOC", "6") or "6")
HYBRID_TOP_K_DOC = int(os.getenv("NAV_DASHBOARD_HYBRID_TOP_K_DOC", "10") or "10")
LOCAL_TOP_K_MEDIA = int(os.getenv("NAV_DASHBOARD_LOCAL_TOP_K_MEDIA", "8") or "8")
HYBRID_TOP_K_MEDIA = int(os.getenv("NAV_DASHBOARD_HYBRID_TOP_K_MEDIA", "12") or "12")
HYBRID_TOP_K_WEB = int(os.getenv("NAV_DASHBOARD_HYBRID_TOP_K_WEB", "5") or "5")
COLLECTION_FILTER_TOP_K_MEDIA = int(os.getenv("NAV_DASHBOARD_COLLECTION_FILTER_TOP_K_MEDIA", "24") or "24")
MEDIA_KEYWORD_SCORE_THRESHOLD = float(os.getenv("NAV_DASHBOARD_MEDIA_KEYWORD_SCORE_THRESHOLD", "0.25") or "0.25")
MEDIA_VECTOR_SCORE_THRESHOLD = float(os.getenv("NAV_DASHBOARD_MEDIA_VECTOR_SCORE_THRESHOLD", "0.35") or "0.35")
WEB_SCORE_THRESHOLD = float(os.getenv("NAV_DASHBOARD_WEB_SCORE_THRESHOLD", "0.3") or "0.3")
WEB_SEARCH_DAILY_LIMIT = int(quota_service.WEB_SEARCH_DAILY_LIMIT)
DEEPSEEK_DAILY_LIMIT = int(quota_service.DEEPSEEK_DAILY_LIMIT)
PER_ITEM_EXPAND_LIMIT = int(os.getenv("NAV_DASHBOARD_PER_ITEM_EXPAND_LIMIT", "3") or "3")
PER_ITEM_EXPAND_MAX_WORKERS = int(os.getenv("NAV_DASHBOARD_PER_ITEM_EXPAND_MAX_WORKERS", "4") or "4")
PER_ITEM_TMDB_MIN_CONFIDENCE = float(os.getenv("NAV_DASHBOARD_PER_ITEM_TMDB_MIN_CONFIDENCE", "0.45") or "0.45")

_CLASSIFICATION_ORACLE = {
    "机器学习的概念和应用": {"domain": "tech", "arbitration": "tech_primary"},
    "《教父》的导演是谁": {"domain": "media", "arbitration": ["entity_wins", "media_surface_wins"]},
    "《三体》的作者是谁": {"domain": "media", "arbitration": ["entity_wins", "media_surface_wins"]},
    "推荐几部2020年的法国电影": {"domain": "media", "arbitration": "media_surface_wins"},
    "魔幻现实主义的叙事手法": {"domain": "media", "arbitration": "abstract_concept_wins"},
    "气候变化的主要原因": {"domain": "general", "arbitration": "general_fallback"},
}


def _load_optional_core_settings() -> Any:
    if get_settings is None:
        return None
    try:
        return get_settings()
    except Exception:  # noqa: BLE001
        return None


def _first_configured_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


_CORE_SETTINGS = _load_optional_core_settings()
LIBRARY_TRACKER_BASE = media_constants.LIBRARY_TRACKER_BASE
LIBRARY_TRACKER_PUBLIC_BASE = _first_configured_text(
    os.getenv("NAV_DASHBOARD_LIBRARY_TRACKER_PUBLIC_URL", ""),
    getattr(_CORE_SETTINGS, "library_tracker_public_url", ""),
)
TAVILY_API_KEY = _first_configured_text(
    os.getenv("TAVILY_API_KEY", ""),
    os.getenv("NAV_DASHBOARD_TAVILY_API_KEY", ""),
    getattr(_CORE_SETTINGS, "tavily_api_key", ""),
)
TMDB_API_KEY = media_constants.TMDB_API_KEY
TMDB_READ_ACCESS_TOKEN = media_constants.TMDB_READ_ACCESS_TOKEN
TMDB_TIMEOUT = media_constants.TMDB_TIMEOUT
BANGUMI_ACCESS_TOKEN = media_constants.BANGUMI_ACCESS_TOKEN
BANGUMI_SUBJECT_TYPE_ANIME = media_constants.BANGUMI_SUBJECT_TYPE_ANIME
BANGUMI_SUBJECT_TYPE_REAL = media_constants.BANGUMI_SUBJECT_TYPE_REAL
PER_ITEM_BANGUMI_MIN_CONFIDENCE = float(os.getenv("NAV_DASHBOARD_PER_ITEM_BANGUMI_MIN_CONFIDENCE", "0.45") or "0.45")

_ROUTER_HELPERS_COMPAT: Any | None = None
_MEDIA_HELPERS_COMPAT: Any | None = None
_ROUTER_CORE_COMPAT: Any | None = None


def _router_helpers_compat() -> Any:
    global _ROUTER_HELPERS_COMPAT
    if _ROUTER_HELPERS_COMPAT is None:
        _ROUTER_HELPERS_COMPAT = importlib.import_module("nav_dashboard.web.services.agent.domain.router_helpers")
    return _ROUTER_HELPERS_COMPAT


def _media_helpers_compat() -> Any:
    global _MEDIA_HELPERS_COMPAT
    if _MEDIA_HELPERS_COMPAT is None:
        _MEDIA_HELPERS_COMPAT = importlib.import_module("nav_dashboard.web.services.agent.domain.media_helpers")
    return _MEDIA_HELPERS_COMPAT


def _router_core_compat() -> Any:
    global _ROUTER_CORE_COMPAT
    if _ROUTER_CORE_COMPAT is None:
        _ROUTER_CORE_COMPAT = importlib.import_module("nav_dashboard.web.services.agent.domain.router_core")
    return _ROUTER_CORE_COMPAT


def _compat_serialize_router_decision(decision: Any) -> dict[str, Any]:
    return _router_helpers_compat()._serialize_router_decision(decision)


def _compat_get_query_type(
    value: Any = None,
    *,
    query_classification: dict[str, Any] | None = None,
    runtime_state: Any | None = None,
) -> str:
    if runtime_state is not None or query_classification is not None:
        return _router_helpers_compat()._get_query_type(
            query_classification=query_classification,
            runtime_state=runtime_state,
        )
    return _router_helpers_compat()._get_query_type(value)


def _compute_classification_conformance(*, raw_question: str, actual_domain: str, actual_arbitration: str) -> dict[str, Any]:
    question = str(raw_question or "").strip()
    domain = str(actual_domain or QUERY_TYPE_GENERAL).strip() or QUERY_TYPE_GENERAL
    arbitration = str(actual_arbitration or "unknown").strip() or "unknown"
    media_cues = _router_helpers_compat()._has_media_intent_cues(question)
    tech_cues = any(cue in question.lower() for cue in router_config.ROUTER_TECH_CUES)
    expected_domain = "media" if media_cues else "tech" if tech_cues else "general"
    return {
        "expected_domain": expected_domain,
        "actual_domain": domain,
        "actual_arbitration": arbitration,
        "matches_expected_domain": domain == expected_domain,
    }


def _http_json(
    method: str,
    url: str,
    payload: dict[str, Any] | None = None,
    timeout: float = 25.0,
    headers: dict[str, str] | None = None,
    trust_env: bool | None = None,
) -> dict[str, Any]:
    parsed = urlparse.urlparse(str(url or ""))
    hostname = str(parsed.hostname or "").strip().casefold()
    inferred_trust_env = bool(parsed.scheme in {"http", "https"} and hostname and hostname not in {"127.0.0.1", "localhost", "::1"})
    try:
        return request_json(
            method,
            url,
            payload=payload,
            timeout=timeout,
            headers=headers,
            trust_env=inferred_trust_env if trust_env is None else bool(trust_env),
            raise_for_status=True,
        )
    except InternalServiceError as exc:
        raise RuntimeError(f"HTTP {exc.status_code}: {exc.detail}") from exc
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(str(exc)) from exc


def _normalize_media_filter_map(value: Any) -> dict[str, list[str]]:
    return media_core._normalize_media_filter_map(value)


def _sanitize_media_filters(filters: Any) -> dict[str, list[str]]:
    return media_core._sanitize_media_filters(filters)


def _sort_media_results(rows: list[dict[str, Any]], sort_preference: str) -> list[dict[str, Any]]:
    return media_core._sort_media_results(rows, sort_preference)


def _clip_text(value: Any, max_chars: int) -> str:
    return media_core._clip_text(value, max_chars)


def _get_lookup_mode_from_state(state: dict[str, Any] | None) -> str:
    return get_lookup_mode_from_state_layer(state)


def _library_tracker_reference_base(request_base_url: str = "") -> str:
    explicit_base = (LIBRARY_TRACKER_PUBLIC_BASE or "").strip()
    internal_parsed = urlparse.urlparse(LIBRARY_TRACKER_BASE)
    tracker_scheme = (internal_parsed.scheme or "http").strip() or "http"
    tracker_port = internal_parsed.port
    local_hosts = {"127.0.0.1", "localhost", "::1"}

    if explicit_base:
        explicit_parsed = urlparse.urlparse(explicit_base)
        if explicit_parsed.scheme and explicit_parsed.hostname and str(explicit_parsed.hostname).strip().lower() not in local_hosts:
            return explicit_base.rstrip("/")

    req = (request_base_url or "").strip()
    if req:
        parsed = urlparse.urlparse(req)
        host = (parsed.hostname or "").strip()
        scheme = (parsed.scheme or "").strip()
        if host:
            final_scheme = scheme or tracker_scheme
            if ":" in host and not host.startswith("["):
                host = f"[{host}]"
            final_netloc = host
            if tracker_port:
                final_netloc = f"{host}:{tracker_port}"
            elif parsed.netloc:
                final_netloc = parsed.netloc
            return urlparse.urlunsplit((final_scheme, final_netloc, "", "", "")).rstrip("/")

    if explicit_base:
        return explicit_base.rstrip("/")
    return LIBRARY_TRACKER_BASE.rstrip("/")


def _serialize_planned_tool(call: PlannedToolCall) -> dict[str, Any]:
    payload = {"name": call.name, "query": call.query}
    if call.options:
        payload["options"] = call.options
    return payload


def _serialize_planned_tools(calls: list[PlannedToolCall]) -> list[dict[str, Any]]:
    return [_serialize_planned_tool(call) for call in calls]


def _serialize_execution_plan(plan: ExecutionPlan) -> dict[str, Any]:
    return {
        "primary_tool": plan.primary_tool,
        "planned_tools": _serialize_planned_tools(plan.planned_tools),
        "fallback_tools": list(plan.fallback_tools),
        "reasons": list(plan.reasons),
    }


def _serialize_router_context_resolution(resolution: RouterContextResolution) -> dict[str, Any]:
    return {
        "resolved_question": resolution.resolved_question,
        "resolved_query_state": dict(resolution.resolved_query_state),
        "conversation_state_before": dict(resolution.conversation_state_before),
        "conversation_state_after": dict(resolution.conversation_state_after),
        "detected_followup": bool(resolution.detected_followup),
        "inheritance_applied": dict(resolution.inheritance_applied),
        "state_diff": dict(resolution.state_diff),
        "planner_snapshot": dict(resolution.planner_snapshot),
        "metadata_anchors": list((resolution.resolved_query_state or {}).get("metadata_anchors") or []),
        "scope_anchors": list((resolution.resolved_query_state or {}).get("scope_anchors") or []),
    }


def _serialize_post_retrieval_assessment(assessment: PostRetrievalAssessment) -> dict[str, Any]:
    return {
        "status": str(assessment.status or "pending_post_retrieval"),
        "doc_similarity": dict(assessment.doc_similarity),
        "media_validation": dict(assessment.media_validation),
        "tmdb": dict(assessment.tmdb),
        "tech_score": round(float(assessment.tech_score or 0.0), 4),
        "weak_tech_signal": bool(assessment.weak_tech_signal),
    }


def _resolve_router_context(
    original_question: str,
    history: list[dict[str, str]],
    decision: RouterDecision,
    previous_state: dict[str, Any],
    planned_tools: list[PlannedToolCall],
) -> RouterContextResolution:
    from .domain import router_core

    return router_core._resolve_router_context(
        original_question,
        history,
        decision,
        previous_state,
        planned_tools,
    )


def _get_resolved_query_state_from_runtime(runtime_state: AgentRuntimeState | None) -> dict[str, Any]:
    if runtime_state is None:
        return {}
    return dict(runtime_state.context_resolution.resolved_query_state)


def _get_planner_snapshot_from_runtime(runtime_state: AgentRuntimeState | None) -> dict[str, Any]:
    if runtime_state is None:
        return {}
    if runtime_state.planning_artifact.planner_snapshot:
        return dict(runtime_state.planning_artifact.planner_snapshot)
    return dict(runtime_state.context_resolution.planner_snapshot)


def _resolve_agent_no_context(query_type: str, rag_used: int, doc_no_context: int) -> tuple[int, str]:
    normalized_type = _router_helpers_compat()._normalize_query_type(query_type)
    if int(doc_no_context or 0) > 0:
        return 1, "below_threshold"
    if int(rag_used or 0) <= 0 and normalized_type in {QUERY_TYPE_TECH, QUERY_TYPE_MIXED}:
        return 1, "knowledge_route_without_rag"
    return 0, ""


def _new_ephemeral_session_id() -> str:
    return f"ephemeral-{uuid4().hex}"


def _normalize_trace_id(trace_id: str) -> str:
    text = re.sub(r"[^A-Za-z0-9_-]+", "", str(trace_id or "").strip())
    if not text:
        return f"trace_{uuid4().hex[:16]}"
    if text.startswith("trace-"):
        text = f"trace_{text[6:]}"
    if text.startswith("trace_"):
        suffix = text[6:]
        if re.fullmatch(r"[0-9A-Fa-f]{16,64}", suffix):
            return f"trace_{suffix[:16]}"
        return text[:64]
    if re.fullmatch(r"[0-9A-Fa-f]{16,64}", text):
        return f"trace_{text[:16]}"
    return text[:64]


def _write_debug_record(session_id: str, payload: dict[str, Any]) -> None:
    sid = _normalize_trace_id(session_id)
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    target = DEBUG_DIR / f"debug_{sid}.json"
    target.write_text(json.dumps(dict(payload or {}), ensure_ascii=False, indent=2), encoding="utf-8")


def _tool_search_web(query: str, trace_id: str = "") -> ToolExecution:
    key = (TAVILY_API_KEY or "").strip()
    if not key:
        return ToolExecution(
            tool=TOOL_SEARCH_WEB,
            status="empty",
            summary="未配置 TAVILY_API_KEY",
            data={"trace_id": trace_id, "trace_stage": "agent.tool.search_web", "results": [], "cache_hit": False},
        )

    try:
        cached = get_cached_web_results(query, 5)
        if cached is not None:
            compact = [
                {"title": r.get("title"), "url": r.get("url"), "content": r.get("content"), "score": r.get("score")}
                for r in cached if isinstance(r, dict)
            ]
            return ToolExecution(
                tool=TOOL_SEARCH_WEB,
                status="ok",
                summary=f"命中 {len(compact)} 条网页结果（缓存）",
                data={"trace_id": trace_id, "trace_stage": "agent.tool.search_web", "results": compact, "cache_hit": True},
            )
    except Exception:
        pass

    payload = {
        "api_key": key,
        "query": query,
        "max_results": 5,
        "search_depth": "advanced",
        "include_answer": False,
        "include_raw_content": False,
    }
    data = _http_json("POST", "https://api.tavily.com/search", payload=payload, timeout=35.0)
    runtime_infra._increment_quota_state(runtime_infra._load_quota_state(), web_search_delta=1)
    runtime_infra._record_usage_events(
        [
            {
                "provider": "web_search",
                "feature": "nav_dashboard.agent.search_web",
                "page": "dashboard_agent",
                "source": "nav_dashboard",
                "message": query,
                "trace_id": trace_id,
            }
        ]
    )
    rows = data.get("results", []) if isinstance(data, dict) else []
    compact: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        compact.append(
            {
                "title": row.get("title"),
                "url": row.get("url"),
                "content": row.get("content"),
                "score": row.get("score"),
            }
        )
    if compact:
        set_cached_web_results(query, 5, compact)
    return ToolExecution(
        tool=TOOL_SEARCH_WEB,
        status="ok",
        summary=f"命中 {len(compact)} 条网页结果",
        data={"trace_id": trace_id, "trace_stage": "agent.tool.search_web", "results": compact, "cache_hit": False},
    )


def _tool_expand_document_query(query: str) -> ToolExecution:
    result = run_doc_graph_expand(query)
    return ToolExecution(tool=TOOL_EXPAND_DOC_QUERY, status=result.status, summary=result.summary, data=result.data)


def _tool_expand_media_query(query: str) -> ToolExecution:
    result = run_media_graph_expand(query)
    return ToolExecution(tool=TOOL_EXPAND_MEDIA_QUERY, status=result.status, summary=result.summary, data=result.data)


def _quota_exceeded(plan: list[PlannedToolCall], backend: str, quota_state: dict[str, Any]) -> list[dict[str, Any]]:
    exceeded: list[dict[str, Any]] = []
    web_needed = sum(1 for c in plan if c.name == TOOL_SEARCH_WEB)
    if web_needed > 0:
        now_count = int(quota_state.get("web_search", 0) or 0)
        if now_count + web_needed > WEB_SEARCH_DAILY_LIMIT:
            exceeded.append({"kind": "web_search", "current": now_count, "add": web_needed, "limit": WEB_SEARCH_DAILY_LIMIT})

    if (backend or "local").strip().lower() == "deepseek":
        deepseek_needed = 2
        now_deepseek = int(quota_state.get("deepseek", 0) or 0)
        if now_deepseek + deepseek_needed > DEEPSEEK_DAILY_LIMIT:
            exceeded.append({"kind": "deepseek", "current": now_deepseek, "add": deepseek_needed, "limit": DEEPSEEK_DAILY_LIMIT})

    return exceeded


def _build_media_tool_options_from_decision(decision: RouterDecision) -> dict[str, Any]:
    return assemble_media_tool_options_from_decision(decision)


def _build_music_signature_queries(
    composer_hints: list[str],
    work_signature: list[str],
    instrument_hints: list[str],
    form_hints: list[str],
    work_family_hints: list[str],
) -> list[str]:
    translation_map = {
        "钢琴": "piano",
        "小提琴": "violin",
        "协奏曲": "concerto",
        "钢琴协奏曲": "piano concerto",
        "小提琴协奏曲": "violin concerto",
        "交响曲": "symphony",
    }

    def _dedupe(values: list[str]) -> list[str]:
        seen: set[str] = set()
        deduped: list[str] = []
        for value in values:
            clean = str(value or "").strip()
            if not clean:
                continue
            folded = clean.casefold()
            if folded in seen:
                continue
            seen.add(folded)
            deduped.append(clean)
        return deduped

    composers = _dedupe(list(composer_hints or []))
    signature_tokens = _dedupe(list(work_signature or []))
    work_phrases = _dedupe(list(work_family_hints or []) + list(form_hints or []) + list(instrument_hints or []))
    translated_work_phrases = _dedupe([translation_map.get(item, "") for item in work_phrases + signature_tokens])
    specific_tokens = [token for token in signature_tokens if is_specific_music_marker(token)]

    queries: list[str] = []
    seen_queries: set[str] = set()

    def _append(*parts: str) -> None:
        query = " ".join(str(part or "").strip() for part in parts if str(part or "").strip())
        folded = query.casefold()
        if not query or folded in seen_queries:
            return
        seen_queries.add(folded)
        queries.append(query)

    for composer in composers:
        _append(composer)
        for work_phrase in work_phrases:
            _append(composer, work_phrase)
            for specific_token in specific_tokens:
                _append(composer, work_phrase, specific_token)
        if composer.isascii():
            for work_phrase in translated_work_phrases:
                _append(composer, work_phrase)
                for specific_token in specific_tokens:
                    _append(composer, work_phrase, specific_token)

    for token in signature_tokens:
        _append(token)
    for work_phrase in work_phrases:
        _append(work_phrase)
    for work_phrase in translated_work_phrases:
        _append(work_phrase)

    return queries


_schema_adapter = SchemaProjectionAdapter()
_deserialize_router_decision = deserialize_router_decision_layer

_build_agent_trace_record = build_agent_trace_record_owner
_write_agent_trace_record = write_agent_trace_record_owner


__all__ = [name for name in globals() if not name.startswith("__")]