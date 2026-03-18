"""nav_dashboard/web/services/agent_service.py
Agent 执行主链 — 工具调度、LLM 调用与会话管理

架构层次（由外到内）
──────────────────────────────────────────────────────────────
  agent_types.py          — 共享数据类与工具名称常量
  routing_policy.py       — RoutingPolicy：RouterDecision → ExecutionPlan
  post_retrieval_policy.py— PostRetrievalPolicy：检索结果质量评估与 repair 建议
  answer_policy.py        — AnswerPolicy：回答模式 + 证据分层策略
  agent_service.py        — 本文件：路由决策、工具执行、LLM 摘要、会话持久化

主链入口：run_agent_round()
──────────────────────────────────────────────────────────────
1. 配额检查（Web Search / DeepSeek 每日限额）
2. Query 长度分析 → _resolve_query_profile（short/medium/long 三档）
3. 路由决策 → _build_router_decision → RouterDecision
4. 工具规划 → RoutingPolicy.build_plan → ExecutionPlan
5. 工具执行（ThreadPoolExecutor 并发）
     query_document_rag  — 向量召回 + 重写 + score 过滤
     query_media_record  — 本地媒体库 keyword/vector 双模式
     search_web          — Tavily API（仅 hybrid 模式）
     expand_mediawiki_concept — 抽象媒体概念展开 / 创作者作品列表
     search_tmdb_media   — TMDB 外部影视信息
6. PostRetrievalPolicy → 结果质量分层（zero/weak/partial/proceed）
7. AnswerPolicy → 回答模式 + 证据分层策略（evidence_policy）
8. LLM 摘要生成 → _llm_summarize
9. 会话持久化（session JSON + 滑动记忆摘要）
"""
from __future__ import annotations

import html
import json
import importlib.util
import math
import os
import re
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict as _dc_asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Iterator, Literal
from urllib import error as urlerror
from urllib import parse as urlparse
from urllib import request as urlrequest
from uuid import uuid4

_LLM_IMPORT_ERROR: Exception | None = None

_WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
if str(_WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKSPACE_ROOT))

from nav_dashboard.web.services.agent_boundaries import (
    get_cached_web_results,
    get_available_boundary_tools,
    get_boundary_tool_prompt_lines,
    log_no_context_query as boundary_log_no_context_query,
    run_doc_graph_expand,
    run_media_graph_expand,
    set_cached_web_results,
)
from nav_dashboard.web.services.entity_resolver import (
    resolve_creator as er_resolve_creator,
    resolve_title as er_resolve_title,
)
from nav_dashboard.web.services.media_query_adapter import (
    SchemaProjectionAdapter,
    derive_resolved_media_type_label,
    maybe_retry_normalized_filters,
    merge_followup_filters,
    project_media_filters_to_library_schema,
    resolve_followup_strategy,
)

# Module-level singleton — avoids re-creating the adapter on every call
_schema_adapter = SchemaProjectionAdapter()
from nav_dashboard.web.services.router_config import (
    ROUTER_CHAT_CUES,
    ROUTER_COLLECTION_NEGATIVE_CUES,
    ROUTER_CONFIDENCE_HIGH,
    ROUTER_CONFIDENCE_MEDIUM,
    ROUTER_MEDIA_DETAIL_CUES,
    ROUTER_MEDIA_SURFACE_CUES,
    ROUTER_REALTIME_CUES,
    ROUTER_RECENT_CUES,
    ROUTER_TECH_CUES,
)
from nav_dashboard.web.services.media_taxonomy import (
    MEDIA_ABSTRACT_CONCEPT_CUES,
    MEDIA_BOOKISH_CUES,
    MEDIA_BOOK_CATEGORY_HINTS,
    MEDIA_COLLECTION_CUES,
    MEDIA_INTENT_KEYWORDS,
    MEDIA_REGION_ALIASES,
    MEDIA_VIDEO_CATEGORY_HINTS,
    MEDIAWIKI_FILLER_PATTERNS,
    MEDIAWIKI_QUERY_ALIASES,
    TMDB_AUDIOVISUAL_CUES,
    TMDB_MOVIE_CUES,
    TMDB_PERSON_CUES,
    TMDB_TV_CUES,
)
from nav_dashboard.web.services.music_ontology import (
    collect_composer_alias_hints,
    collect_music_ontology_hints,
    composer_override_signature_tokens,
    infer_music_filters_from_text,
    is_form_alias,
    is_instrument_alias,
    is_work_family_alias,
)

from nav_dashboard.web.services.post_retrieval_policy import (
    PostRetrievalPolicy as _PostRetrievalPolicy,
    PostRetrievalOutcome as _PostRetrievalOutcome,
)
from nav_dashboard.web.services.answer_policy import (
    AnswerPolicy as _AnswerPolicy,
    AnswerStrategy as _AnswerStrategy,
)
from nav_dashboard.web.services.agent_types import (  # noqa: E402
    TOOL_QUERY_DOC_RAG,
    TOOL_QUERY_MEDIA,
    TOOL_SEARCH_WEB,
    TOOL_EXPAND_DOC_QUERY,
    TOOL_EXPAND_MEDIA_QUERY,
    TOOL_SEARCH_MEDIAWIKI,
    TOOL_PARSE_MEDIAWIKI,
    TOOL_EXPAND_MEDIAWIKI_CONCEPT,
    TOOL_SEARCH_TMDB,
    TOOL_SEARCH_BANGUMI,
    TOOL_SEARCH_BY_CREATOR,
    TOOL_NAMES,
    QUERY_TYPE_TECH,
    QUERY_TYPE_MEDIA,
    QUERY_TYPE_MIXED,
    QUERY_TYPE_GENERAL,
    CLASSIFIER_LABEL_MEDIA,
    CLASSIFIER_LABEL_TECH,
    CLASSIFIER_LABEL_OTHER,
    PlannedToolCall,
    ToolExecution,
    RouterDecision,
    ExecutionPlan,
    RouterContextResolution,
    PostRetrievalAssessment,
    AgentRuntimeState,
)
from nav_dashboard.web.services.routing_policy import RoutingPolicy  # noqa: E402

try:
    from core_service.config import get_settings
    from core_service.llm_client import chat_completion_with_retry
    from core_service.trace_store import get_trace_record, write_trace_record
except Exception as exc:  # noqa: BLE001
    _LLM_IMPORT_ERROR = exc
    get_settings = None
    chat_completion_with_retry = None
    get_trace_record = None
    write_trace_record = None


DATA_DIR = Path(__file__).resolve().parents[2] / "data"
QUOTA_FILE = DATA_DIR / "agent_quota.json"
QUOTA_HISTORY_FILE = DATA_DIR / "agent_quota_history.json"
SESSION_FILE_PREFIX = "session_"
SESSIONS_DIR = DATA_DIR / "agent_sessions"
MEMORY_DIR = SESSIONS_DIR / "_memory"
DEBUG_DIR = SESSIONS_DIR / "debug_data"
MEMORY_MAX_TURNS = 3
AGENT_METRICS_FILE = DATA_DIR / "agent_metrics.json"
AGENT_METRICS_MAX = 20
_LOCK = threading.RLock()

TECH_SPACE_PREFIXES = (
    "ai-governance/",
    "career-learning/",
    "cognition-method/",
    "examples/",
    "industry-tech/",
    "science/",
)

DOC_SCORE_THRESHOLD = 0.45
WEB_SCORE_THRESHOLD = 0.5
MEDIA_KEYWORD_SCORE_THRESHOLD = float(os.getenv("NAV_DASHBOARD_MEDIA_KEYWORD_SCORE_THRESHOLD", "0.2"))
MEDIA_VECTOR_SCORE_THRESHOLD = float(os.getenv("NAV_DASHBOARD_MEDIA_VECTOR_SCORE_THRESHOLD", "0.35"))
TECH_QUERY_DOC_SIM_THRESHOLD = float(os.getenv("NAV_DASHBOARD_TECH_QUERY_DOC_SIM_THRESHOLD", "0.38"))
MEDIA_KEYWORD_BONUS_WEIGHT = float(os.getenv("NAV_DASHBOARD_MEDIA_KEYWORD_BONUS_WEIGHT", "0.05"))
LOCAL_TOP_K_DOC = 3
LOCAL_TOP_K_MEDIA = 3
HYBRID_TOP_K_DOC = 3
HYBRID_TOP_K_MEDIA = 3
HYBRID_TOP_K_WEB = 3
MAX_REFERENCE_ITEMS = int(os.getenv("NAV_DASHBOARD_MAX_REFERENCE_ITEMS", "6") or "6")
DOC_VECTOR_TOP_N = int(os.getenv("NAV_DASHBOARD_DOC_VECTOR_TOP_N", "12"))
DOC_VECTOR_TOP_N_SCALE = float(os.getenv("NAV_DASHBOARD_DOC_VECTOR_TOP_N_SCALE", "0.7") or "0.7")
DOC_QUERY_REWRITE_COUNT = max(1, min(2, int(os.getenv("NAV_DASHBOARD_QUERY_REWRITE_COUNT", "2"))))
DOC_PRIMARY_QUERY_SCORE_BONUS = float(os.getenv("NAV_DASHBOARD_PRIMARY_QUERY_SCORE_BONUS", "0.03") or "0.03")
ALLOWED_RANKING_MODES = {"relevance", "rating_asc", "rating_desc", "date_asc", "date_desc"}
SHORT_QUERY_MAX_TOKENS = 5
LONG_QUERY_MIN_TOKENS = int(os.getenv("NAV_DASHBOARD_LONG_QUERY_MIN_TOKENS", "12") or "12")
SHORT_QUERY_DOC_THRESHOLD_DELTA = float(os.getenv("NAV_DASHBOARD_SHORT_DOC_THRESHOLD_DELTA", "-0.08") or "-0.08")
LONG_QUERY_DOC_THRESHOLD_DELTA = float(os.getenv("NAV_DASHBOARD_LONG_DOC_THRESHOLD_DELTA", "0.08") or "0.08")
SHORT_QUERY_MEDIA_VECTOR_THRESHOLD_DELTA = float(os.getenv("NAV_DASHBOARD_SHORT_MEDIA_VECTOR_THRESHOLD_DELTA", "-0.04") or "-0.04")
LONG_QUERY_MEDIA_VECTOR_THRESHOLD_DELTA = float(os.getenv("NAV_DASHBOARD_LONG_MEDIA_VECTOR_THRESHOLD_DELTA", "0.05") or "0.05")
SHORT_QUERY_WEB_THRESHOLD_DELTA = float(os.getenv("NAV_DASHBOARD_SHORT_WEB_THRESHOLD_DELTA", "-0.06") or "-0.06")
LONG_QUERY_WEB_THRESHOLD_DELTA = float(os.getenv("NAV_DASHBOARD_LONG_WEB_THRESHOLD_DELTA", "0.05") or "0.05")
SHORT_QUERY_DOC_VECTOR_TOP_N_DELTA = int(os.getenv("NAV_DASHBOARD_SHORT_DOC_VECTOR_TOP_N_DELTA", "6") or "6")
LONG_QUERY_DOC_VECTOR_TOP_N_DELTA = int(os.getenv("NAV_DASHBOARD_LONG_DOC_VECTOR_TOP_N_DELTA", "-4") or "-4")
SHORT_QUERY_LIMIT_DELTA = int(os.getenv("NAV_DASHBOARD_SHORT_TOOL_LIMIT_DELTA", "1") or "1")
LONG_QUERY_LIMIT_DELTA = int(os.getenv("NAV_DASHBOARD_LONG_TOOL_LIMIT_DELTA", "-1") or "-1")
# For list_plus_expand collection queries the full result set is needed before per-item enrichment.
COLLECTION_FILTER_TOP_K_MEDIA = int(os.getenv("NAV_DASHBOARD_COLLECTION_FILTER_TOP_K", "15") or "15")
# Maximum number of local items to fan-out to per-item TMDB/Wiki enrichment.
# Kept deliberately small to avoid latency spikes (each item = 1 external call).
PER_ITEM_EXPAND_LIMIT = int(os.getenv("NAV_DASHBOARD_PER_ITEM_EXPAND_LIMIT", "8") or "8")
# Minimum per-item TMDB match confidence to accept a result.
# Below this the TMDB row is discarded to prevent wrong-item overviews.
PER_ITEM_TMDB_MIN_CONFIDENCE = float(os.getenv("NAV_DASHBOARD_PER_ITEM_TMDB_MIN_CONFIDENCE", "0.45") or "0.45")
_MEDIA_GRAPH_CACHE: dict[str, Any] = {"mtime": None, "degrees": {}}
_MEDIA_COMPARE_SPLIT_RE = re.compile(r"\s*(?:和|与|以及|及|跟|vs\.?|VS\.?|/|／|、)\s*")
_MEDIA_TITLE_MARKER_RE = re.compile(r"(?:《[^》]+》|「[^」]+」|『[^』]+』|“[^”]+”|\"[^\"]+\")")
_MEDIA_LIBRARY_VOCAB_CACHE: dict[str, Any] = {
    "signature": None,
    "data": {"nationalities": [], "authors": [], "categories": [], "titles": []},
}
_MEDIAWIKI_CONCEPT_CACHE: dict[str, Any] = {"entries": {}, "lock": threading.RLock()}

# ── Classification Oracle ─────────────────────────────────────────────────────
# Maps lowercased query text → expected {domain, arbitration}.  When a live
# agent trace matches a known oracle entry, query_understanding gets a
# classification_conformance field so the dashboard can flag regressions.
# arbitration may be a str (exact match) or list[str] (any-of match).
_CLASSIFICATION_ORACLE: dict[str, dict[str, Any]] = {
    "机器学习的概念和应用":            {"domain": "tech",    "arbitration": "tech_primary"},
    "深度学习架构原理是什么":            {"domain": "tech",    "arbitration": "tech_primary"},
    "transformer注意力机制的数学原理":   {"domain": "tech",    "arbitration": "tech_primary"},
    "rag检索增强生成的工作流程":          {"domain": "tech",    "arbitration": "tech_primary"},
    "什么是向量数据库":                 {"domain": "tech",    "arbitration": "tech_primary"},
    "《教父》的导演是谁":               {"domain": "media",   "arbitration": ["entity_wins", "media_surface_wins"]},
    "《三体》作者刘慈欣的其他作品":       {"domain": "media",   "arbitration": ["entity_wins", "media_surface_wins"]},
    "波拉尼奥的小说有哪些":             {"domain": "media",   "arbitration": ["entity_wins", "media_surface_wins"]},
    "推荐几部法国新浪潮电影":            {"domain": "media",   "arbitration": "media_surface_wins"},
    "魔幻现实主义的叙事手法":            {"domain": "media",   "arbitration": "abstract_concept_wins"},
    "拉美文学的代表作家":               {"domain": "media",   "arbitration": ["abstract_concept_wins", "media_surface_wins"]},
    "什么是量子纠缠":                   {"domain": "general", "arbitration": "general_fallback"},
    "气候变化的主要原因":               {"domain": "general", "arbitration": "general_fallback"},
}
# ─────────────────────────────────────────────────────────────────────────────
PROMPT_HISTORY_MAX_MESSAGES = int(os.getenv("NAV_DASHBOARD_PROMPT_HISTORY_MAX_MESSAGES", "6") or "6")
PROMPT_HISTORY_ITEM_MAX_CHARS = int(os.getenv("NAV_DASHBOARD_PROMPT_HISTORY_ITEM_MAX_CHARS", "360") or "360")
PROMPT_MEMORY_MAX_CHARS = int(os.getenv("NAV_DASHBOARD_PROMPT_MEMORY_MAX_CHARS", "1800") or "1800")
PROMPT_TOOL_CONTEXT_MAX_CHARS = int(os.getenv("NAV_DASHBOARD_PROMPT_TOOL_CONTEXT_MAX_CHARS", "5200") or "5200")
PROMPT_TOOL_CONTEXT_RETRY_CHARS = int(os.getenv("NAV_DASHBOARD_PROMPT_TOOL_CONTEXT_RETRY_CHARS", "3000") or "3000")
PROMPT_TOOL_RESULT_MAX_CHARS = int(os.getenv("NAV_DASHBOARD_PROMPT_TOOL_RESULT_MAX_CHARS", "1700") or "1700")
PROMPT_TOOL_RESULT_MIN_CHARS = int(os.getenv("NAV_DASHBOARD_PROMPT_TOOL_RESULT_MIN_CHARS", "420") or "420")


def _allowed_tool_names(search_mode: str) -> list[str]:
    normalized_mode = _normalize_search_mode(search_mode)
    allowed = [
        TOOL_QUERY_DOC_RAG,
        TOOL_QUERY_MEDIA,
        TOOL_SEARCH_MEDIAWIKI,
        TOOL_PARSE_MEDIAWIKI,
        TOOL_EXPAND_MEDIAWIKI_CONCEPT,
        TOOL_SEARCH_TMDB,
        TOOL_SEARCH_BANGUMI,
    ]
    if normalized_mode == "hybrid":
        allowed.append(TOOL_SEARCH_WEB)
    allowed.extend([tool_name for tool_name in get_available_boundary_tools() if tool_name in {TOOL_EXPAND_DOC_QUERY, TOOL_EXPAND_MEDIA_QUERY}])
    return allowed


def _build_tool_prompt_lines(search_mode: str) -> str:
    lines = [
        f"- {TOOL_QUERY_DOC_RAG}: 查询RAG文档知识库",
        f"- {TOOL_QUERY_MEDIA}: 查询书影音游记录",
        f"- {TOOL_SEARCH_MEDIAWIKI}: 使用 MediaWiki Action API 搜索百科条目",
        f"- {TOOL_PARSE_MEDIAWIKI}: 使用 MediaWiki Action API 解析百科页面内容",
        f"- {TOOL_EXPAND_MEDIAWIKI_CONCEPT}: 使用 MediaWiki 概念展开抽象标签并映射回本地媒体库",
        f"- {TOOL_SEARCH_TMDB}: 使用 TMDB 查询电影、剧集、演员等外部媒体信息",
    ]
    normalized_mode = _normalize_search_mode(search_mode)
    if normalized_mode == "hybrid":
        lines.append(f"- {TOOL_SEARCH_WEB}: 联网搜索最新信息")
    lines.extend(get_boundary_tool_prompt_lines())
    return "\n".join(lines)


def _new_trace_id() -> str:
    return f"trace_{uuid4().hex[:16]}"


def _normalize_trace_id(trace_id: str = "") -> str:
    value = re.sub(r"[^a-zA-Z0-9_.:-]", "", str(trace_id or "").strip())
    if value:
        return value[:80]
    return _new_trace_id()


def _approx_tokens(text: str) -> int:
    value = str(text or "")
    # CJK characters (Chinese/Japanese/Korean) are each ~1 token; Latin chars ~0.25 tokens
    cjk_count = sum(1 for c in value if '\u4e00' <= c <= '\u9fff' or '\u3040' <= c <= '\u30ff' or '\uac00' <= c <= '\ud7af')
    latin_chars = len(value) - cjk_count
    return max(0, cjk_count + int(latin_chars / 4))


def _clip_text(value: Any, max_chars: int) -> str:
    text = str(value or "")
    if max_chars <= 0:
        return ""
    if len(text) <= max_chars:
        return text
    if max_chars <= 3:
        return text[:max_chars]
    return text[: max_chars - 3].rstrip() + "..."


def _prompt_string_limit_for_key(key: str) -> int:
    lowered = str(key or "").strip().lower()
    if lowered in {"url", "path", "title", "display_title", "name", "date", "language", "media_type", "author"}:
        return 120
    if lowered in {"extract", "overview", "snippet", "content", "review", "summary"}:
        return 220
    return 160


def _sanitize_for_prompt(
    value: Any,
    *,
    key: str = "",
    max_depth: int = 3,
    max_list_items: int = 5,
    max_dict_items: int = 12,
) -> Any:
    if max_depth <= 0:
        return _clip_text(value, _prompt_string_limit_for_key(key))
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return _clip_text(value, _prompt_string_limit_for_key(key))
    if isinstance(value, dict):
        trimmed: dict[str, Any] = {}
        omitted = 0
        for idx, (raw_key, raw_value) in enumerate(value.items()):
            if idx >= max_dict_items:
                omitted += 1
                continue
            child_key = str(raw_key or "").strip()
            if child_key in {"trace_id", "trace_stage", "latency_ms", "query_profile", "html_text", "raw_html"}:
                continue
            trimmed[child_key] = _sanitize_for_prompt(
                raw_value,
                key=child_key,
                max_depth=max_depth - 1,
                max_list_items=max_list_items,
                max_dict_items=max_dict_items,
            )
        if omitted > 0:
            trimmed["_omitted_keys"] = omitted
        return trimmed
    if isinstance(value, list):
        local_limit = max_list_items
        lowered = str(key or "").strip().lower()
        if lowered == "results":
            local_limit = min(local_limit, 4)
        elif lowered in {"links", "categories", "aliases", "countries", "authors", "queries"}:
            local_limit = min(max(local_limit, 6), 8)
        elif lowered == "search_results":
            local_limit = min(local_limit, 3)
        trimmed_list = [
            _sanitize_for_prompt(
                item,
                key=key,
                max_depth=max_depth - 1,
                max_list_items=max_list_items,
                max_dict_items=max_dict_items,
            )
            for item in value[:local_limit]
        ]
        if len(value) > local_limit:
            trimmed_list.append({"_omitted_items": len(value) - local_limit})
        return trimmed_list
    return _clip_text(value, _prompt_string_limit_for_key(key))


def _minimal_prompt_tool_payload(exec_result: ToolExecution) -> dict[str, Any]:
    data = exec_result.data if isinstance(exec_result.data, dict) else {}
    rows = data.get("results", []) if isinstance(data.get("results"), list) else []
    payload: dict[str, Any] = {
        "tool": exec_result.tool,
        "status": exec_result.status,
        "summary": _clip_text(exec_result.summary, 240),
        "result_count": len(rows),
    }
    if exec_result.tool == TOOL_EXPAND_MEDIAWIKI_CONCEPT and isinstance(data, dict):
        payload["concept"] = _clip_text(data.get("concept", ""), 120)
        payload["filters"] = _sanitize_for_prompt(data.get("filters", {}), key="filters", max_depth=2, max_list_items=6)
        # Include abbreviated extract so the LLM can cite wiki content in the answer
        # body even when the full payload is trimmed down to minimal form.
        if rows:
            _mw_first = rows[0] if isinstance(rows[0], dict) else {}
            _mw_title = str(_mw_first.get("display_title") or _mw_first.get("title") or "").strip()
            _mw_extract = _clip_text(str(_mw_first.get("extract") or ""), 400)
            if _mw_title or _mw_extract:
                payload["top_result"] = {"title": _mw_title, "extract": _mw_extract}
    if exec_result.tool == TOOL_SEARCH_BY_CREATOR and isinstance(data, dict):
        payload["canonical_creator"] = str(data.get("canonical_creator") or "").strip()
        payload["found"] = bool(data.get("found"))
        payload["works_count"] = int(data.get("works_count") or 0)
        # Include first few works so the LLM sees concrete titles even in minimal path.
        works = data.get("results") if isinstance(data.get("results"), list) else []
        payload["works_preview"] = _sanitize_for_prompt(works[:6], key="data", max_depth=2, max_list_items=6, max_dict_items=6)
    return payload


def _format_tool_result(exec_result: ToolExecution, *, max_chars: int = PROMPT_TOOL_RESULT_MAX_CHARS) -> str:
    profiles = [
        {"max_depth": 3, "max_list_items": 5, "max_dict_items": 12},
        {"max_depth": 2, "max_list_items": 4, "max_dict_items": 10},
        {"max_depth": 2, "max_list_items": 3, "max_dict_items": 8},
    ]
    for profile in profiles:
        payload = {
            "tool": exec_result.tool,
            "status": exec_result.status,
            "summary": _clip_text(exec_result.summary, 240),
            "data": _sanitize_for_prompt(exec_result.data, key="data", **profile),
        }
        rendered = json.dumps(payload, ensure_ascii=False)
        if len(rendered) <= max_chars:
            return rendered
    return json.dumps(_minimal_prompt_tool_payload(exec_result), ensure_ascii=False)


def _build_tool_context_parts(tool_results: list[ToolExecution], *, max_total_chars: int) -> list[str]:
    parts = ["工具返回结果："]
    remaining = max_total_chars - len(parts[0])
    rendered_count = 0
    for index, result in enumerate(tool_results):
        remaining_tools = max(1, len(tool_results) - index)
        if remaining <= 80:
            break
        per_tool_budget = max(PROMPT_TOOL_RESULT_MIN_CHARS, min(PROMPT_TOOL_RESULT_MAX_CHARS, int(remaining / remaining_tools)))
        block = _format_tool_result(result, max_chars=per_tool_budget)
        if len(block) + 1 > remaining:
            block = _format_tool_result(result, max_chars=max(PROMPT_TOOL_RESULT_MIN_CHARS, remaining - 32))
        if len(block) + 1 > remaining:
            break
        parts.append(block)
        rendered_count += 1
        remaining -= len(block) + 1
    omitted = max(0, len(tool_results) - rendered_count)
    if omitted > 0 and remaining > 32:
        parts.append(f"还有 {omitted} 个工具结果因上下文预算被省略。")
    return parts


def _build_compact_tool_summary_lines(tool_results: list[ToolExecution]) -> list[str]:
    """Render a terse tool summary for prompts when full raw tool JSON is redundant.

    This is primarily used for per-item bucketed answers where the detailed
    local/external media content has already been pre-assembled into buckets and
    including the raw tool payload again only inflates prompt size.
    """
    lines: list[str] = []
    for result in tool_results:
        data = result.data if isinstance(result.data, dict) else {}
        result_count = len(data.get("results") or []) if isinstance(data.get("results"), list) else 0
        source_counts = data.get("source_counts") if isinstance(data.get("source_counts"), dict) else {}
        source_suffix = ""
        if source_counts:
            ordered = sorted(
                ((str(k), int(v or 0)) for k, v in source_counts.items() if str(k).strip()),
                key=lambda item: (-item[1], item[0]),
            )
            source_suffix = " | sources=" + ", ".join(f"{name}:{count}" for name, count in ordered)
        lines.append(f"- {result.tool} | {result.status} | results={result_count}{source_suffix}")
    return lines


def _compose_response_sections(
    tool_results: list[ToolExecution],
    answer_strategy: "Any | None",
) -> dict[str, Any]:
    """Pre-assemble compact data blocks from tool results so the LLM polishes
    an already-structured data, rather than discovering structure from raw JSON.

    Returns:
        local_lines  – bullet lines for local-library items (title, date, rating, comment).
        external_lines – labeled lines for TMDB / MediaWiki results.
        has_external – True when external_lines is non-empty.
    """
    if answer_strategy is None:
        return {"local_lines": [], "external_lines": [], "has_external": False}
    _style = getattr(answer_strategy, "style_hints", None) or {}
    _response_structure = str(_style.get("response_structure") or "")
    # Only activate when the answer policy requests a structured multi-source layout.
    if _response_structure not in {
        "local_list_plus_external_background",
        "local_record_plus_external_info",
        "local_list",
        "compare",
        "thematic_list",
    }:
        return {"local_lines": [], "external_lines": [], "has_external": False}

    local_lines: list[str] = []
    external_lines: list[str] = []
    # per_item_buckets: each entry is a single-item dict rendered as a complete
    # block of "local record + external overview" so the LLM receives bucketed,
    # deterministically paired data rather than two parallel flat lists.
    per_item_buckets: list[str] = []

    # ── Detect per-item fan-out result ───────────────────────────────────────
    # When present, build bucketed blocks instead of flat external_lines.
    _fanout_result = next(
        (r for r in tool_results
         if r.status in {"ok", "partial"}
         and isinstance(r.data, dict)
         and r.data.get("per_item_fanout")),
        None,
    )
    _fanout_data: list[dict[str, Any]] = []
    if _fanout_result is not None and isinstance(_fanout_result.data, dict):
        _raw = _fanout_result.data.get("per_item_data") or _fanout_result.data.get("results") or []
        _fanout_data = [d for d in _raw if isinstance(d, dict)]

    if _fanout_data:
        # Build an index keyed by normalised local_title for quick bucketing.
        _ext_by_title: dict[str, dict[str, Any]] = {}
        for item in _fanout_data:
            key = _normalize_title_for_match(str(item.get("local_title") or item.get("_source_title") or ""))
            if key:
                _ext_by_title[key] = item

        # Collect local rows to render
        _local_rows: list[dict[str, Any]] = []
        for result in tool_results:
            if result.status not in {"ok", "partial"} or not isinstance(result.data, dict):
                continue
            if result.tool in {TOOL_QUERY_MEDIA, TOOL_SEARCH_BY_CREATOR}:
                _raw_rows = result.data.get("results") or []
                _local_rows.extend([r for r in _raw_rows if isinstance(r, dict)])

        for row in _local_rows:
            title = str(row.get("title") or "").strip()
            if not title:
                continue
            date = str(row.get("date") or "").strip()
            rating = row.get("rating")
            comment = _clip_text(str(row.get("comment") or row.get("review") or ""), 80)

            block: list[str] = [f"## 《{title}》"]
            if date:
                block.append(f"  观看/阅读日期：{date}")
            if rating is not None:
                block.append(f"  个人评分：{rating}")
            if comment:
                block.append(f"  个人短评：{comment}")

            norm = _normalize_title_for_match(title)
            ext = _ext_by_title.get(norm)
            if ext:
                ext_source = str(ext.get("external_source") or "wiki")
                if ext_source == "bangumi":
                    source_label = "Bangumi"
                elif ext_source == "tmdb":
                    source_label = "TMDB"
                else:
                    source_label = "Wiki"
                confidence = float(ext.get("match_confidence") or 0.0)
                ext_overview = _clip_text(str(ext.get("external_overview") or ext.get("overview") or ""), 180)
                if ext_overview:
                    line = f"  剧情/简介（{source_label}外部参考）：{ext_overview}"
                    if confidence < 0.6:
                        line += "（外部信息可能非精确匹配）"
                    block.append(line)

            per_item_buckets.append("\n".join(block))

        return {
            "local_lines": local_lines,
            "external_lines": external_lines,
            "per_item_buckets": per_item_buckets,
            "has_external": bool(per_item_buckets),
        }

    # ── Standard (non-fan-out) rendering ─────────────────────────────────────
    for result in tool_results:
        if result.status not in {"ok", "partial"}:
            continue
        data = result.data if isinstance(result.data, dict) else {}
        rows: list[Any] = data.get("results", []) if isinstance(data.get("results"), list) else []

        if result.tool in {TOOL_QUERY_MEDIA, TOOL_SEARCH_BY_CREATOR}:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                title = str(row.get("title") or "").strip()
                if not title:
                    continue
                date = str(row.get("date") or "").strip()
                rating = row.get("rating")
                comment = _clip_text(str(row.get("comment") or row.get("review") or ""), 80)
                if _response_structure == "compare":
                    block_lines = [f"# 《{title}》"]
                    if date:
                        block_lines.append(f"  观看/阅读日期: {date}")
                    if rating is not None:
                        block_lines.append(f"  个人评分: {rating}")
                    if comment:
                        block_lines.append(f"  个人短评: {comment}")
                    local_lines.extend(block_lines)
                else:
                    parts: list[str] = [f"《{title}》"]
                    if date:
                        parts.append(date)
                    if rating is not None:
                        parts.append(f"评分 {rating}")
                    if comment:
                        parts.append(comment)
                    local_lines.append("・" + " | ".join(parts))

        elif result.tool == TOOL_SEARCH_TMDB:
            for row in rows[:4]:
                if not isinstance(row, dict):
                    continue
                title = str(row.get("title") or "").strip()
                overview = _clip_text(str(row.get("overview") or ""), 150)
                if title:
                    line = f"【TMDB】《{title}》"
                    if overview:
                        line += f"：{overview}"
                    external_lines.append(line)

        elif result.tool in {TOOL_EXPAND_MEDIAWIKI_CONCEPT, TOOL_SEARCH_MEDIAWIKI, TOOL_PARSE_MEDIAWIKI}:
            wiki_rows: list[dict[str, Any]] = []
            if result.tool == TOOL_PARSE_MEDIAWIKI:
                page = data.get("page")
                if isinstance(page, dict) and (page.get("title") or page.get("extract")):
                    wiki_rows = [page]
                elif rows:
                    wiki_rows = [r for r in rows[:2] if isinstance(r, dict)]
            else:
                wiki_rows = [r for r in rows[:2] if isinstance(r, dict)]
            for row in wiki_rows:
                wiki_title = str(row.get("display_title") or row.get("title") or "").strip()
                extract = _clip_text(str(row.get("extract") or ""), 250)
                if wiki_title or extract:
                    label = f"【Wiki】{wiki_title}" if wiki_title else "【Wiki】"
                    external_lines.append(f"{label}：{extract}" if extract else label)

    return {
        "local_lines": local_lines,
        "external_lines": external_lines,
        "per_item_buckets": per_item_buckets,
        "has_external": bool(external_lines),
    }


def _clip_memory_context(memory_context: str) -> str:
    return _clip_text(memory_context, PROMPT_MEMORY_MAX_CHARS)


def _trim_history_for_prompt(history: list[dict[str, str]]) -> list[str]:
    hist_lines: list[str] = []
    for msg in (history or [])[-PROMPT_HISTORY_MAX_MESSAGES:]:
        role = str(msg.get("role", "")).strip()
        content = _clip_text(str(msg.get("content", "")).strip(), PROMPT_HISTORY_ITEM_MAX_CHARS)
        if content:
            hist_lines.append(f"{role}: {content}")
    return hist_lines


def _is_context_length_error(exc: Exception) -> bool:
    text = str(exc or "")
    lowered = text.lower()
    return (
        "context length" in lowered
        or "n_keep" in lowered
        or "too many tokens" in lowered
        or "maximum context length" in lowered
        or "the number of tokens to keep" in lowered
    )


def _resolve_query_profile(query: str) -> dict[str, Any]:
    token_count = _approx_tokens(query)
    profile = "medium"
    doc_threshold_delta = 0.0
    media_vector_threshold_delta = 0.0
    web_threshold_delta = 0.0
    doc_vector_top_n_delta = 0
    limit_delta = 0

    if token_count < SHORT_QUERY_MAX_TOKENS:
        profile = "short"
        doc_threshold_delta = SHORT_QUERY_DOC_THRESHOLD_DELTA
        media_vector_threshold_delta = SHORT_QUERY_MEDIA_VECTOR_THRESHOLD_DELTA
        web_threshold_delta = SHORT_QUERY_WEB_THRESHOLD_DELTA
        doc_vector_top_n_delta = SHORT_QUERY_DOC_VECTOR_TOP_N_DELTA
        limit_delta = SHORT_QUERY_LIMIT_DELTA
    elif token_count >= LONG_QUERY_MIN_TOKENS:
        profile = "long"
        doc_threshold_delta = LONG_QUERY_DOC_THRESHOLD_DELTA
        media_vector_threshold_delta = LONG_QUERY_MEDIA_VECTOR_THRESHOLD_DELTA
        web_threshold_delta = LONG_QUERY_WEB_THRESHOLD_DELTA
        doc_vector_top_n_delta = LONG_QUERY_DOC_VECTOR_TOP_N_DELTA
        limit_delta = LONG_QUERY_LIMIT_DELTA

    referential_followup_cues = ("这些", "这些媒体", "这些作品", "这些条目", "它们", "前面这些", "上面这些")
    detail_followup_cues = ("具体细节", "细节信息", "详细信息", "作者", "出版方", "出版社", "渠道", "平台", "工作室")
    if any(cue in str(query or "") for cue in referential_followup_cues) and any(cue in str(query or "") for cue in detail_followup_cues):
        limit_delta = max(limit_delta, SHORT_QUERY_LIMIT_DELTA)

    raw_top_n = max(4, int(DOC_VECTOR_TOP_N + doc_vector_top_n_delta))
    scaled_top_n = max(4, int(round(raw_top_n * max(0.1, DOC_VECTOR_TOP_N_SCALE))))

    return {
        "profile": profile,
        "token_count": token_count,
        "doc_score_threshold": round(DOC_SCORE_THRESHOLD + doc_threshold_delta, 6),
        "media_keyword_score_threshold": round(MEDIA_KEYWORD_SCORE_THRESHOLD, 6),
        "media_vector_score_threshold": round(MEDIA_VECTOR_SCORE_THRESHOLD + media_vector_threshold_delta, 6),
        "web_score_threshold": round(WEB_SCORE_THRESHOLD + web_threshold_delta, 6),
        "doc_vector_top_n": scaled_top_n,
        "limit_delta": int(limit_delta),
    }


def _write_debug_record(session_id: str, record: dict[str, Any]) -> None:
    sid = (session_id or "").strip()
    if not sid:
        return
    try:
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = DEBUG_DIR / f"{ts}_{sid}.json"
        out.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        return


def _new_ephemeral_session_id() -> str:
    return f"benchmark_{uuid4()}"


# ─── Agent round metrics ──────────────────────────────────────────────────────

def _load_agent_metrics() -> list[dict[str, Any]]:
    if not AGENT_METRICS_FILE.exists():
        return []
    try:
        payload = json.loads(AGENT_METRICS_FILE.read_text(encoding="utf-8"))
        rows = payload.get("records", []) if isinstance(payload, dict) else []
        return [r for r in rows if isinstance(r, dict)]
    except Exception:
        return []


def _save_agent_metrics(rows: list[dict[str, Any]]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "records": rows[-AGENT_METRICS_MAX:],
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }
    try:
        AGENT_METRICS_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def record_agent_metrics(
    *,
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
    doc_top1_score: float | None,
    doc_top1_score_before_rerank: float | None,
    doc_top1_identity_changed: int | None,
    doc_top1_rank_shift: float | None,
    embed_cache_hit: int,
    query_rewrite_hit: int,
    vector_recall_seconds: float,
    rerank_seconds: float,
    wall_clock_seconds: float = 0.0,
) -> None:
    row: dict[str, Any] = {
        "ts": datetime.now().isoformat(timespec="seconds"),
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
    with _LOCK:
        rows = _load_agent_metrics()
        rows.append(row)
        _save_agent_metrics(rows)


def _load_shared_quota_defaults() -> tuple[int, int]:
    workspace_root = Path(__file__).resolve().parents[3]
    cfg_path = workspace_root / "api_config.py"
    if not cfg_path.is_file():
        return 50, 25
    try:
        spec = importlib.util.spec_from_file_location("_shared_api_config", cfg_path)
        if not spec or not spec.loader:
            return 50, 10
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        web_limit = int(getattr(module, "NAV_DASHBOARD_WEB_SEARCH_DAILY_LIMIT", 50) or 50)
        deepseek_limit = int(getattr(module, "NAV_DASHBOARD_DEEPSEEK_DAILY_LIMIT", 25) or 25)
        return web_limit, deepseek_limit
    except Exception:
        return 50, 25


_WEB_LIMIT_DEFAULT, _DEEPSEEK_LIMIT_DEFAULT = _load_shared_quota_defaults()
WEB_SEARCH_DAILY_LIMIT = int(os.getenv("NAV_DASHBOARD_WEB_SEARCH_DAILY_LIMIT", str(_WEB_LIMIT_DEFAULT)))
DEEPSEEK_DAILY_LIMIT = int(os.getenv("NAV_DASHBOARD_DEEPSEEK_DAILY_LIMIT", str(_DEEPSEEK_LIMIT_DEFAULT)))

AI_SUMMARY_BASE = os.getenv("NAV_DASHBOARD_AI_SUMMARY_INTERNAL_URL", "http://127.0.0.1:8000").rstrip("/")
LIBRARY_TRACKER_BASE = os.getenv("NAV_DASHBOARD_LIBRARY_TRACKER_INTERNAL_URL", "http://127.0.0.1:8091").rstrip("/")
LIBRARY_TRACKER_PUBLIC_BASE = (
    os.getenv("NAV_DASHBOARD_LIBRARY_TRACKER_URL", "")
    or os.getenv("NAV_DASHBOARD_LIBRARY_TRACKER_PUBLIC_URL", "")
).strip().rstrip("/")

TAVILY_API_KEY = (os.getenv("TAVILY_API_KEY", "") or "").strip()

LOCAL_LLM_FALLBACK_URL = (
    os.getenv("NAV_DASHBOARD_LOCAL_LLM_URL", "")
    or os.getenv("AI_SUMMARY_LOCAL_LLM_URL", "http://127.0.0.1:1234/v1")
).strip()
LOCAL_LLM_FALLBACK_MODEL = os.getenv("NAV_DASHBOARD_LOCAL_LLM_MODEL", "qwen2.5-7b-instruct").strip() or "qwen2.5-7b-instruct"
LOCAL_LLM_FALLBACK_KEY = (
    os.getenv("NAV_DASHBOARD_LOCAL_LLM_API_KEY", "")
    or os.getenv("AI_SUMMARY_LOCAL_LLM_API_KEY", "local")
).strip() or "local"


def _load_optional_core_settings() -> Any:
    if get_settings is None:
        return None
    try:
        return get_settings()
    except Exception:
        return None


_CORE_SETTINGS = _load_optional_core_settings()


def _first_configured_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _build_mediawiki_user_agent() -> str:
    explicit = _first_configured_text(
        os.getenv("NAV_DASHBOARD_MEDIAWIKI_USER_AGENT", ""),
        getattr(_CORE_SETTINGS, "mediawiki_user_agent", ""),
    )
    if explicit:
        return explicit
    app_name = _first_configured_text(
        os.getenv("NAV_DASHBOARD_MEDIAWIKI_APP_NAME", ""),
        "PersonalAIStackAgent/0.1",
    )
    site = _first_configured_text(
        os.getenv("NAV_DASHBOARD_MEDIAWIKI_SITE_URL", ""),
        "https://localhost.localdomain",
    )
    contact = _first_configured_text(
        os.getenv("NAV_DASHBOARD_MEDIAWIKI_CONTACT", ""),
        getattr(_CORE_SETTINGS, "mediawiki_contact", ""),
    )
    extra: list[str] = []
    if site:
        extra.append(site)
    if contact:
        extra.append(f"contact: {contact}")
    if extra:
        return f"{app_name} ({'; '.join(extra)})"
    return f"{app_name} (nav_dashboard local deployment)"


def _build_mediawiki_api_user_agent() -> str:
    explicit = _first_configured_text(
        os.getenv("NAV_DASHBOARD_MEDIAWIKI_API_USER_AGENT", ""),
        getattr(_CORE_SETTINGS, "mediawiki_api_user_agent", ""),
    )
    if explicit:
        return explicit
    contact = _first_configured_text(
        os.getenv("NAV_DASHBOARD_MEDIAWIKI_CONTACT", ""),
        getattr(_CORE_SETTINGS, "mediawiki_contact", ""),
    )
    if contact:
        return f"PersonalAIStackAgent/0.1 (contact: {contact})"
    return "PersonalAIStackAgent/0.1"


def _build_tmdb_headers() -> dict[str, str]:
    headers = {
        "accept": "application/json",
        "User-Agent": "PersonalAIStackAgent/0.1",
    }
    if TMDB_READ_ACCESS_TOKEN:
        headers["Authorization"] = f"Bearer {TMDB_READ_ACCESS_TOKEN}"
    return headers


MEDIAWIKI_ZH_API = _first_configured_text(
    os.getenv("NAV_DASHBOARD_MEDIAWIKI_ZH_API_URL", ""),
    getattr(_CORE_SETTINGS, "mediawiki_zh_api_url", ""),
    "https://zh.wikipedia.org/w/api.php",
).rstrip("?")
MEDIAWIKI_EN_API = _first_configured_text(
    os.getenv("NAV_DASHBOARD_MEDIAWIKI_EN_API_URL", ""),
    getattr(_CORE_SETTINGS, "mediawiki_en_api_url", ""),
    "https://en.wikipedia.org/w/api.php",
).rstrip("?")
MEDIAWIKI_TIMEOUT = float(
    _first_configured_text(
        os.getenv("NAV_DASHBOARD_MEDIAWIKI_TIMEOUT", ""),
        getattr(_CORE_SETTINGS, "mediawiki_timeout", ""),
        "20",
    )
    or "20"
)
MEDIAWIKI_USER_AGENT = _build_mediawiki_user_agent()
MEDIAWIKI_API_USER_AGENT = _build_mediawiki_api_user_agent()
TMDB_API_BASE_URL = _first_configured_text(
    os.getenv("NAV_DASHBOARD_TMDB_API_BASE_URL", ""),
    getattr(_CORE_SETTINGS, "tmdb_api_base_url", ""),
    "https://api.themoviedb.org/3",
).rstrip("/")
TMDB_API_KEY = _first_configured_text(
    os.getenv("NAV_DASHBOARD_TMDB_API_KEY", ""),
    getattr(_CORE_SETTINGS, "tmdb_api_key", ""),
)
TMDB_READ_ACCESS_TOKEN = _first_configured_text(
    os.getenv("NAV_DASHBOARD_TMDB_READ_ACCESS_TOKEN", ""),
    getattr(_CORE_SETTINGS, "tmdb_read_access_token", ""),
)
TMDB_TIMEOUT = float(
    _first_configured_text(
        os.getenv("NAV_DASHBOARD_TMDB_TIMEOUT", ""),
        getattr(_CORE_SETTINGS, "tmdb_timeout", ""),
        "20",
    )
    or "20"
)
TMDB_LANGUAGE = _first_configured_text(
    os.getenv("NAV_DASHBOARD_TMDB_LANGUAGE", ""),
    getattr(_CORE_SETTINGS, "tmdb_language", ""),
    "zh-CN",
)

BANGUMI_ACCESS_TOKEN = _first_configured_text(
    os.getenv("BANGUMI_ACCESS_TOKEN", ""),
    os.getenv("NAV_DASHBOARD_BANGUMI_ACCESS_TOKEN", ""),
    getattr(_CORE_SETTINGS, "bangumi_access_token", ""),
)
BANGUMI_API_BASE_URL = "https://api.bgm.tv"
BANGUMI_TIMEOUT = float(os.getenv("NAV_DASHBOARD_BANGUMI_TIMEOUT", "20") or "20")
# Bangumi subject types: 1=书籍, 2=动画, 3=音乐, 4=游戏, 6=三次元
BANGUMI_SUBJECT_TYPE_ANIME = 2
BANGUMI_SUBJECT_TYPE_REAL = 6
# Minimum Bangumi match confidence; same threshold as TMDB.
PER_ITEM_BANGUMI_MIN_CONFIDENCE = float(
    os.getenv("NAV_DASHBOARD_PER_ITEM_BANGUMI_MIN_CONFIDENCE", "0.45") or "0.45"
)

# PlannedToolCall, ToolExecution, RouterDecision, ExecutionPlan,
# RouterContextResolution, PostRetrievalAssessment, AgentRuntimeState
# are imported from agent_types at the top of this module.



def _serialize_planned_tool(call: PlannedToolCall) -> dict[str, Any]:
    payload = {"name": call.name, "query": call.query}
    if call.options:
        payload["options"] = call.options
    return payload


def _serialize_planned_tools(calls: list[PlannedToolCall]) -> list[dict[str, Any]]:
    return [_serialize_planned_tool(call) for call in calls]


def _serialize_router_decision(decision: RouterDecision) -> dict[str, Any]:
    return {
        "raw_question": decision.raw_question,
        "resolved_question": decision.resolved_question,
        "intent": decision.intent,
        "domain": decision.domain,
        "lookup_mode": decision.lookup_mode,
        "selection": _normalize_media_filter_map(decision.selection),
        "time_constraint": dict(decision.time_constraint),
        "ranking": dict(decision.ranking),
        "entities": list(decision.entities),
        "filters": _normalize_media_filter_map(decision.filters),
        "date_range": list(decision.date_range),
        "sort": decision.sort,
        "freshness": decision.freshness,
        "needs_web": bool(decision.needs_web),
        "needs_doc_rag": bool(decision.needs_doc_rag),
        "needs_media_db": bool(decision.needs_media_db),
        "needs_external_media_db": bool(decision.needs_external_media_db),
        "followup_mode": decision.followup_mode,
        "followup_filter_strategy": str(decision.followup_filter_strategy or "none"),
        "confidence": round(float(decision.confidence or 0.0), 4),
        "reasons": list(decision.reasons),
        "media_type": decision.media_type,
        "llm_label": decision.llm_label,
        "query_type": decision.query_type,
        "allow_downstream_entity_inference": bool(decision.allow_downstream_entity_inference),
        "followup_target": str(decision.followup_target or ""),
        "needs_comparison": bool(decision.needs_comparison),
        "needs_explanation": bool(decision.needs_explanation),
        "rewritten_queries": {str(key): str(value) for key, value in (decision.rewritten_queries or {}).items() if str(key).strip() and str(value).strip()},
        "evidence": dict(decision.evidence),
        "arbitration": str(decision.arbitration or "general_fallback"),
        "query_class": str(decision.query_class or "knowledge_qa"),
        "subject_scope": str(decision.subject_scope or "general_knowledge"),
        "time_scope_type": str(decision.time_scope_type or ""),
        "answer_shape": str(decision.answer_shape or ""),
        "media_family": str(decision.media_family or ""),
    }


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


def _get_query_type(
    query_classification: dict[str, Any] | None = None,
    runtime_state: AgentRuntimeState | None = None,
) -> str:
    if runtime_state is not None:
        return str(runtime_state.decision.query_type or QUERY_TYPE_GENERAL)
    current = query_classification if isinstance(query_classification, dict) else {}
    router_payload = current.get("router_decision") if isinstance(current.get("router_decision"), dict) else {}
    if router_payload:
        value = str(router_payload.get("query_type") or QUERY_TYPE_GENERAL).strip()
        if value:
            return value
    return str(current.get("query_type", QUERY_TYPE_GENERAL) or QUERY_TYPE_GENERAL)


def _deserialize_router_decision(payload: dict[str, Any], *, fallback_question: str = "") -> RouterDecision:
    return RouterDecision(
        raw_question=str(payload.get("raw_question") or fallback_question or ""),
        resolved_question=str(payload.get("resolved_question") or fallback_question or ""),
        intent=str(payload.get("intent") or "knowledge_qa"),
        domain=str(payload.get("domain") or "general"),
        lookup_mode=str(payload.get("lookup_mode") or "general_lookup"),
        selection=_normalize_media_filter_map(payload.get("selection")),
        time_constraint=dict(payload.get("time_constraint") or {}),
        ranking=dict(payload.get("ranking") or {}),
        entities=[str(item).strip() for item in (payload.get("entities") or []) if str(item).strip()],
        filters=_normalize_media_filter_map(payload.get("filters")),
        date_range=list(payload.get("date_range") or []),
        sort=str(payload.get("sort") or "relevance"),
        freshness=str(payload.get("freshness") or "none"),
        needs_web=bool(payload.get("needs_web")),
        needs_doc_rag=bool(payload.get("needs_doc_rag")),
        needs_media_db=bool(payload.get("needs_media_db")),
        needs_external_media_db=bool(payload.get("needs_external_media_db")),
        followup_mode=str(payload.get("followup_mode") or "none"),
        followup_filter_strategy=str(payload.get("followup_filter_strategy") or "none"),
        confidence=float(payload.get("confidence") or 0.0),
        reasons=[str(item) for item in (payload.get("reasons") or [])],
        media_type=str(payload.get("media_type") or ""),
        llm_label=str(payload.get("llm_label") or CLASSIFIER_LABEL_OTHER),
        query_type=str(payload.get("query_type") or QUERY_TYPE_GENERAL),
        allow_downstream_entity_inference=bool(payload.get("allow_downstream_entity_inference")),
        followup_target=str(payload.get("followup_target") or ""),
        needs_comparison=bool(payload.get("needs_comparison")),
        needs_explanation=bool(payload.get("needs_explanation")),
        rewritten_queries={str(key): str(value) for key, value in ((payload.get("rewritten_queries") or {}).items() if isinstance(payload.get("rewritten_queries"), dict) else []) if str(key).strip() and str(value).strip()},
        evidence=dict(payload.get("evidence") or {}),
        arbitration=str(payload.get("arbitration") or "general_fallback"),
    )


def _normalize_media_filter_map(value: Any) -> dict[str, list[str]]:
    if not isinstance(value, dict):
        return {}
    normalized: dict[str, list[str]] = {}
    for key, raw_values in value.items():
        field_name = str(key or "").strip()
        if not field_name:
            continue
        if isinstance(raw_values, list):
            values = [str(item).strip() for item in raw_values if str(item).strip()]
        else:
            values = [str(raw_values).strip()] if str(raw_values).strip() else []
        if values:
            normalized[field_name] = values
    return normalized


def _date_window_from_state(state: dict[str, Any] | None) -> dict[str, str]:
    current = state if isinstance(state, dict) else {}
    time_constraint = current.get("time_constraint") if isinstance(current.get("time_constraint"), dict) else {}
    if time_constraint:
        start = str(time_constraint.get("start") or "").strip()
        end = str(time_constraint.get("end") or "").strip()
        if start and end:
            return {"start": start, "end": end}
    explicit_window = current.get("date_window") if isinstance(current.get("date_window"), dict) else {}
    if explicit_window:
        start = str(explicit_window.get("start") or "").strip()
        end = str(explicit_window.get("end") or "").strip()
        if start and end:
            return {"start": start, "end": end}
    date_range = current.get("date_range") if isinstance(current.get("date_range"), list) else []
    if len(date_range) != 2:
        return {}
    start = str(date_range[0] or "").strip()
    end = str(date_range[1] or "").strip()
    if not start or not end:
        return {}
    return {"start": start, "end": end}


def _build_media_selection(filters: dict[str, list[str]], media_type: str = "") -> dict[str, list[str]]:
    selection: dict[str, list[str]] = {}
    normalized = _normalize_media_filter_map(filters)
    for field in (
        "media_type",
        "category",
        "genre",
        "nationality",
        "series",
        "platform",
        "author",
        "authors",
        "director",
        "directors",
        "actor",
        "actors",
        "tag",
        "tags",
        "year",
    ):
        if field in normalized:
            selection[field] = list(normalized[field])
    if media_type and "media_type" not in selection:
        selection["media_type"] = [media_type]
    return selection


def _build_time_constraint(date_window: dict[str, Any] | None, *, source: str = "none") -> dict[str, Any]:
    window = date_window if isinstance(date_window, dict) else {}
    start = str(window.get("start") or "").strip()
    end = str(window.get("end") or "").strip()
    if not start or not end:
        return {}
    payload = {
        "kind": str(window.get("kind") or "explicit_range").strip() or "explicit_range",
        "start": start,
        "end": end,
        "source": str(source or window.get("source") or "explicit").strip() or "explicit",
    }
    label = str(window.get("label") or "").strip()
    if label:
        payload["label"] = label
    return payload


def _get_lookup_mode_from_state(state: dict[str, Any] | None) -> str:
    current = state if isinstance(state, dict) else {}
    lookup_mode = str(current.get("lookup_mode") or "").strip()
    if lookup_mode:
        return lookup_mode
    legacy_intent = str(current.get("intent") or "").strip()
    if legacy_intent in {"filter_search", "entity_lookup", "concept_lookup", "general_lookup"}:
        return legacy_intent
    return "general_lookup"


def _infer_requested_sort(text: str) -> str:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return "relevance"
    low_cues = ("最低", "最差", "评分低", "评价最低", "评分最低", "最低分", "lowest", "lowest-rated", "worst")
    high_cues = ("最高", "最好", "评分高", "评价比较好的", "评分最高", "最高分", "best", "highest", "highest-rated")
    newest_cues = (
        "最新",
        "最近",
        "近半年",
        "最近半年",
        "过去半年",
        "半年内",
        "近6个月",
        "最近6个月",
        "过去6个月",
        "最近一年",
        "近一年",
        "过去一年",
        "今年",
        "去年",
        "上半年",
        "下半年",
    )
    oldest_cues = ("最早", "最先", "最旧", "较早")
    if any(cue in lowered for cue in low_cues):
        return "rating_asc"
    if any(cue in lowered for cue in high_cues):
        return "rating_desc"
    if any(cue in lowered for cue in oldest_cues):
        return "date_asc"
    if any(cue in lowered for cue in newest_cues):
        return "date_desc"
    return "relevance"


def _build_media_ranking(query: str, lookup_mode: str, time_constraint: dict[str, Any]) -> dict[str, Any]:
    requested = _infer_requested_sort(query)
    if requested != "relevance":
        return {"mode": requested, "source": "explicit"}
    if lookup_mode == "filter_search" and time_constraint:
        return {"mode": "date_desc", "source": "time_constraint_default"}
    return {"mode": "relevance", "source": "default"}


def _has_specific_media_constraints(filters: dict[str, list[str]]) -> bool:
    normalized = _normalize_media_filter_map(filters)
    for field, values in normalized.items():
        if not values:
            continue
        if field == "media_type":
            continue
        return True
    return False


def _rating_sort_value(row: dict[str, Any], *, descending: bool) -> float:
    value = row.get("rating")
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).strip())
    except Exception:
        return -1.0 if descending else 11.0


def _sort_media_results(rows: list[dict[str, Any]], sort_preference: str) -> list[dict[str, Any]]:
    normalized = str(sort_preference or "relevance").strip().lower()
    if normalized == "date_asc":
        rows.sort(
            key=lambda item: (
                str(item.get("date") or "9999-12-31"),
                -_safe_score(item.get("score")),
                _rating_sort_value(item, descending=True),
            )
        )
        return rows
    if normalized == "date_desc":
        rows.sort(
            key=lambda item: (
                str(item.get("date") or ""),
                _safe_score(item.get("score")),
                _rating_sort_value(item, descending=True),
            ),
            reverse=True,
        )
        return rows
    if normalized == "rating_asc":
        rows.sort(
            key=lambda item: (
                _rating_sort_value(item, descending=False),
                str(item.get("date") or ""),
                -_safe_score(item.get("score")),
            )
        )
        return rows
    if normalized == "rating_desc":
        rows.sort(
            key=lambda item: (
                _rating_sort_value(item, descending=True),
                str(item.get("date") or ""),
                _safe_score(item.get("score")),
            ),
            reverse=True,
        )
        return rows
    rows.sort(
        key=lambda item: (
            _safe_score(item.get("score")),
            _rating_sort_value(item, descending=True),
            str(item.get("date") or ""),
        ),
        reverse=True,
    )
    return rows


def _merge_router_filters(base: dict[str, list[str]], extra: dict[str, list[str]]) -> dict[str, list[str]]:
    merged = _normalize_media_filter_map(base)
    for field, values in _normalize_media_filter_map(extra).items():
        existing = list(merged.get(field, []))
        for value in values:
            if value not in existing:
                existing.append(value)
        if existing:
            merged[field] = existing
    return merged


def _infer_router_freshness(question: str) -> Literal["none", "recent", "realtime"]:
    text = str(question or "").strip().lower()
    if not text:
        return "none"
    if any(cue in text for cue in ROUTER_REALTIME_CUES):
        return "realtime"
    if any(cue in text for cue in ROUTER_RECENT_CUES):
        return "recent"
    return "none"


def _has_router_tech_cues(question: str) -> bool:
    text = str(question or "").strip().lower()
    if not text:
        return False
    return any(cue in text for cue in ROUTER_TECH_CUES)


def _has_router_media_surface(question: str) -> bool:
    text = str(question or "").strip()
    if not text:
        return False
    return bool(
        _has_media_title_marker(text)
        or _has_media_intent_cues(text)
        or any(cue in text for cue in ROUTER_MEDIA_SURFACE_CUES)
    )


def _router_followup_mode_label(mode: str) -> str:
    normalized = str(mode or "none").strip().lower()
    if normalized == "inherit_timerange":
        return "time_window_replace"
    if normalized in {"inherit_filters", "inherit_entity"}:
        return "followup_expand"
    return "none"


def _router_state_intent(decision: RouterDecision | dict[str, Any]) -> str:
    payload = _serialize_router_decision(decision) if isinstance(decision, RouterDecision) else dict(decision or {})
    domain = str(payload.get("domain") or "general")
    lookup_mode = str(payload.get("lookup_mode") or "general_lookup")
    entities = [str(item).strip() for item in (payload.get("entities") or []) if str(item).strip()]
    filters = _normalize_media_filter_map(payload.get("filters"))
    date_range = payload.get("date_range") if isinstance(payload.get("date_range"), list) else []
    followup_mode = str(payload.get("followup_mode") or "none")
    if domain == "media":
        if lookup_mode in {"filter_search", "entity_lookup", "concept_lookup"}:
            return lookup_mode
        if filters or len(date_range) == 2 or followup_mode in {"inherit_filters", "inherit_timerange"}:
            return "filter_search"
        if entities:
            return "entity_lookup"
    return "general_lookup"


def _derive_media_lookup_mode(
    *,
    domain: str,
    entities: list[str],
    filters: dict[str, list[str]],
    date_range: list[str],
    followup_mode: str,
    abstract_media_concept: bool,
    collection_query: bool,
) -> str:
    if domain != "media":
        return "general_lookup"
    if abstract_media_concept and not entities and not filters and len(date_range) != 2:
        return "concept_lookup"
    if len(date_range) == 2 or collection_query or followup_mode in {"inherit_filters", "inherit_timerange"}:
        return "filter_search"
    if _has_specific_media_constraints(filters):
        return "filter_search"
    if entities or followup_mode == "inherit_entity":
        return "entity_lookup"
    return "general_lookup"


def _render_resolved_question_from_decision(
    question: str,
    previous_state: dict[str, Any],
    followup_mode: str,
    entities: list[str],
) -> str:
    current = str(question or "").strip()
    if not current and entities:
        return entities[0]
    return current


def _render_trace_resolved_question(decision: RouterDecision, previous_state: dict[str, Any]) -> str:
    current = str(decision.raw_question or "").strip()
    previous_question = str(previous_state.get("question") or "").strip()
    previous_entity = str(previous_state.get("entity") or "").strip()
    entity = decision.entities[0] if decision.entities else previous_entity
    if decision.followup_mode == "inherit_timerange" and previous_question:
        return _replace_time_window_in_query(previous_question, current)
    if decision.followup_mode == "inherit_entity" and entity and entity not in current:
        return f"{entity} {current}".strip()
    return current


def _build_resolved_query_state_from_decision(decision: RouterDecision) -> dict[str, Any]:
    followup_mode = str(decision.followup_mode or "none")
    return {
        "lookup_mode": str(decision.lookup_mode or "general_lookup"),
        "selection": _normalize_media_filter_map(decision.selection),
        "time_constraint": dict(decision.time_constraint),
        "ranking": dict(decision.ranking),
        "media_type": str(decision.media_type or ""),
        "filters": _normalize_media_filter_map(decision.filters),
        "date_range": list(decision.date_range or []),
        "sort": str(decision.sort or "relevance"),
        "followup_target": str(decision.followup_target or ""),
        "followup_filter_strategy": str(decision.followup_filter_strategy or "none"),
        "rewritten_queries": {str(key): str(value) for key, value in (decision.rewritten_queries or {}).items() if str(key).strip() and str(value).strip()},
        "carry_over_from_previous_turn": followup_mode != "none",
        "inherited_context": {
            "used": followup_mode != "none",
            "kind": _router_followup_mode_label(followup_mode),
            "filter_strategy": str(decision.followup_filter_strategy or "none"),
        },
    }


def _build_conversation_state_snapshot_from_decision(
    question: str,
    decision: RouterDecision,
    resolved_query_state: dict[str, Any],
) -> dict[str, Any]:
    entities = [str(item).strip() for item in list(decision.entities or []) if str(item).strip()]
    return {
        "question": str(question or "").strip(),
        "lookup_mode": _get_lookup_mode_from_state(resolved_query_state),
        "selection": _normalize_media_filter_map(resolved_query_state.get("selection")),
        "time_constraint": dict(resolved_query_state.get("time_constraint") or {}),
        "ranking": dict(resolved_query_state.get("ranking") or {}),
        "media_type": str(resolved_query_state.get("media_type", "") or ""),
        "entity": entities[0] if entities else "",
        "entities": entities,
        "filters": _normalize_media_filter_map(resolved_query_state.get("filters")),
        "date_range": list(resolved_query_state.get("date_range") or []),
        "sort": str(resolved_query_state.get("sort", "") or ""),
        "followup_target": str(resolved_query_state.get("followup_target", "") or ""),
        "rewritten_media_query": str(((resolved_query_state.get("rewritten_queries") or {}) if isinstance(resolved_query_state.get("rewritten_queries"), dict) else {}).get("media_query") or ""),
    }


def _build_planner_snapshot_from_decision(
    decision: RouterDecision,
    resolved_question: str,
    resolved_query_state: dict[str, Any],
    planned_tools: list[PlannedToolCall],
) -> dict[str, Any]:
    state_after = _build_conversation_state_snapshot_from_decision(resolved_question, decision, resolved_query_state)
    hard_filters: dict[str, Any] = {}
    media_type = str(state_after.get("media_type", "") or "")
    if media_type:
        hard_filters["media_type"] = media_type
    for field, values in (state_after.get("filters") or {}).items():
        if not isinstance(values, list):
            continue
        clean_values = [str(value).strip() for value in values if str(value).strip()]
        if not clean_values:
            continue
        if field == "media_type" and media_type:
            continue
        hard_filters[str(field)] = clean_values[0] if len(clean_values) == 1 else clean_values
    date_range = state_after.get("date_range") or []
    if date_range:
        hard_filters["date_range"] = date_range
    soft_constraints: dict[str, Any] = {}
    sort = str(state_after.get("sort", "") or "")
    if sort:
        soft_constraints["sort"] = sort
    ranking = state_after.get("ranking") if isinstance(state_after.get("ranking"), dict) else {}
    if ranking:
        soft_constraints["ranking"] = ranking
    rewritten_queries = {str(key): str(value) for key, value in (decision.rewritten_queries or {}).items() if str(key).strip() and str(value).strip()}
    query_text: str | None = rewritten_queries.get("media_query") or (decision.entities[0] if decision.entities else None)
    if query_text is None and _get_lookup_mode_from_state(resolved_query_state) != "filter_search":
        query_text = resolved_question or None
    followup_mode = str((resolved_query_state.get("inherited_context") or {}).get("kind", "") or "none")
    return {
        "lookup_mode": _get_lookup_mode_from_state(resolved_query_state),
        "selection": _normalize_media_filter_map(state_after.get("selection")),
        "time_constraint": dict(state_after.get("time_constraint") or {}),
        "hard_filters": hard_filters,
        "soft_constraints": soft_constraints,
        "query_text": query_text,
        "rewritten_queries": rewritten_queries,
        "followup_mode": followup_mode,
        "planned_tools": [call.name for call in planned_tools],
    }


def _resolve_router_context(
    original_question: str,
    history: list[dict[str, str]],
    decision: RouterDecision,
    previous_state: dict[str, Any],
    planned_tools: list[PlannedToolCall],
) -> RouterContextResolution:
    resolved_question = _render_trace_resolved_question(decision, previous_state)
    resolved_query_state = _build_resolved_query_state_from_decision(decision)

    previous_trace = _find_previous_trace_context(history)
    previous_trace_state = previous_trace.get("conversation_state_after") if isinstance(previous_trace.get("conversation_state_after"), dict) else {}
    previous_trace_resolved_question = ""
    if isinstance(previous_trace.get("query_understanding"), dict):
        previous_trace_resolved_question = str(previous_trace.get("query_understanding", {}).get("resolved_question", "") or "").strip()
    previous_question = _find_previous_user_question(original_question, history)
    conversation_state_before = dict(previous_trace_state) if previous_trace_state else (
        _build_conversation_state_snapshot(previous_question, resolved_query_state=_infer_prior_question_state(previous_question, history))
        if previous_question else {}
    )
    conversation_state_after = _build_conversation_state_snapshot_from_decision(
        resolved_question,
        decision,
        resolved_query_state,
    )
    has_previous_context = bool(previous_question) or bool(previous_trace_state) or bool(previous_trace_resolved_question)
    detected_followup = has_previous_context and (
        bool(resolved_query_state.get("carry_over_from_previous_turn"))
        or _is_context_dependent_followup(original_question)
    )
    inheritance_applied = {
        "lookup_mode": _describe_inheritance_transition(
            conversation_state_before.get("lookup_mode"),
            conversation_state_after.get("lookup_mode"),
            detected_followup,
        ),
        "media_type": _describe_inheritance_transition(
            conversation_state_before.get("media_type"),
            conversation_state_after.get("media_type"),
            detected_followup,
        ),
        "filters": _describe_inheritance_transition(
            conversation_state_before.get("filters"),
            conversation_state_after.get("filters"),
            detected_followup,
        ),
        "date_range": _describe_inheritance_transition(
            conversation_state_before.get("date_range"),
            conversation_state_after.get("date_range"),
            detected_followup,
        ),
        "sort": _describe_inheritance_transition(
            conversation_state_before.get("sort"),
            conversation_state_after.get("sort"),
            detected_followup,
        ),
        "entity": _describe_inheritance_transition(
            conversation_state_before.get("entity"),
            conversation_state_after.get("entity"),
            detected_followup,
        ),
    }
    planner_snapshot = _build_planner_snapshot_from_decision(decision, resolved_question, resolved_query_state, planned_tools)
    return RouterContextResolution(
        resolved_question=resolved_question,
        resolved_query_state=resolved_query_state,
        conversation_state_before=conversation_state_before,
        conversation_state_after=conversation_state_after,
        detected_followup=detected_followup,
        inheritance_applied=inheritance_applied,
        state_diff=_build_state_diff(conversation_state_before, conversation_state_after),
        planner_snapshot=planner_snapshot,
    )


def _get_context_resolution(query_classification: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(query_classification, dict):
        return {}
    resolution = query_classification.get("context_resolution")
    return resolution if isinstance(resolution, dict) else {}


def _get_resolved_query_state(query_classification: dict[str, Any]) -> dict[str, Any]:
    resolution = _get_context_resolution(query_classification)
    state = resolution.get("resolved_query_state") if isinstance(resolution.get("resolved_query_state"), dict) else {}
    return dict(state) if state else {}


def _get_resolved_query_state_from_runtime(runtime_state: AgentRuntimeState | None) -> dict[str, Any]:
    if runtime_state is None:
        return {}
    return dict(runtime_state.context_resolution.resolved_query_state)


def _get_planner_snapshot(query_classification: dict[str, Any]) -> dict[str, Any]:
    resolution = _get_context_resolution(query_classification)
    snapshot = resolution.get("planner_snapshot") if isinstance(resolution.get("planner_snapshot"), dict) else {}
    return dict(snapshot) if snapshot else {}


def _get_planner_snapshot_from_runtime(runtime_state: AgentRuntimeState | None) -> dict[str, Any]:
    if runtime_state is None:
        return {}
    return dict(runtime_state.context_resolution.planner_snapshot)


def _is_short_followup_surface(question: str) -> bool:
    text = str(question or "").strip()
    if not text:
        return False
    compact = re.sub(r"\s+", "", text)
    pronoun_cues = ("那个", "这个", "那部", "这部", "那本", "这本", "它", "它们", "前者", "后者")
    followup_verbs = ("怎么样", "如何", "呢", "简介", "介绍", "细节", "评价", "评分", "展开", "详细", "讲了什么")
    if len(compact) <= 14 and any(cue in compact for cue in pronoun_cues) and any(cue in compact for cue in followup_verbs):
        return True
    return compact in {"那个呢", "这个呢", "那个怎么样", "这个怎么样", "简介呢", "详细呢", "评价呢", "评分呢"}


def _has_explicit_fresh_media_scope(question: str) -> bool:
    text = str(question or "").strip()
    if not text:
        return False
    if _is_short_followup_surface(text):
        return False
    explicit_entities = [
        entity for entity in _extract_media_entities(text)
        if entity and not _looks_like_generic_media_scope(entity)
    ]
    if explicit_entities or _has_media_title_marker(text):
        return True
    inferred_filters = _infer_media_filters(text)
    explicit_filter_keys = {
        "media_type",
        "category",
        "genre",
        "nationality",
        "series",
        "platform",
        "author",
        "authors",
        "director",
        "directors",
        "actor",
        "actors",
        "tag",
        "tags",
        "year",
    }
    has_explicit_filters = any(inferred_filters.get(key) for key in explicit_filter_keys)
    has_explicit_date = bool(_parse_media_date_window(text))
    has_personal_scope = any(
        cue in text
        for cue in ("我看过", "我看了", "我追过", "我补过", "我读过", "我读了", "我听过", "我玩过", "我打过", "我记录过")
    )
    if has_personal_scope and (has_explicit_filters or has_explicit_date or _is_collection_media_query(text)):
        return True
    if has_explicit_filters and len(re.sub(r"\s+", "", text)) >= 8:
        return True
    if has_explicit_date and _is_collection_media_query(text):
        return True
    return False


def _derive_router_followup_resolution(question: str, previous_state: dict[str, Any]):
    text = str(question or "").strip()
    explicit_entities = [
        entity for entity in _extract_media_entities(text)
        if entity and not _looks_like_generic_media_scope(entity)
    ]
    return resolve_followup_strategy(
        question=text,
        previous_has_media_context=_state_has_media_context(previous_state),
        previous_has_entity=bool(str(previous_state.get("entity") or "").strip()),
        has_explicit_fresh_scope=_has_explicit_fresh_media_scope(text),
        has_explicit_entities=bool(explicit_entities),
        has_title_marker=_has_media_title_marker(text),
        looks_time_only_followup=_looks_like_time_only_followup(text),
        is_short_followup_surface=_is_short_followup_surface(text),
        wants_media_details=_question_requests_media_details(text),
        wants_personal_evaluation=_question_requests_personal_evaluation(text),
        is_collection_query=_is_collection_media_query(text),
    )


def _derive_router_followup_mode(question: str, previous_state: dict[str, Any]) -> Literal["none", "inherit_filters", "inherit_entity", "inherit_timerange"]:
    return _derive_router_followup_resolution(question, previous_state).mode


def _map_router_query_type(decision: RouterDecision) -> str:
    if decision.needs_doc_rag and decision.needs_media_db:
        return QUERY_TYPE_MIXED
    if decision.needs_media_db:
        return QUERY_TYPE_MEDIA
    if decision.needs_doc_rag and decision.domain == "tech":
        return QUERY_TYPE_TECH
    return QUERY_TYPE_GENERAL


def _derive_query_class(decision: RouterDecision) -> str:
    """Map a completed RouterDecision to a single stable query_class string.

    Priority order (highest → lowest):
      1. media_creator_collection — evidence.creator_collection_query
      2. media_abstract_concept   — evidence.abstract_media_concept
            3. music_work_versions_compare — music composer/work version compare
            4. media_title_detail       — entity_lookup + single entity
            5. media_collection_filter  — filter_search (non-creator)
            6. mixed_knowledge_with_media — mixed domain intent
            7. followup_proxy           — any active follow-up mode
            8. knowledge_qa             — tech / general / knowledge intent
            9. general_qa               — catch-all
    """
    ev = dict(decision.evidence or {})
    if ev.get("creator_collection_query"):
        return "media_creator_collection"
    if ev.get("abstract_media_concept"):
        return "media_abstract_concept"
    if ev.get("music_work_versions_compare"):
        return "music_work_versions_compare"
    if decision.domain == "media" and decision.lookup_mode == "entity_lookup" and len(decision.entities) == 1:
        return "media_title_detail"
    if decision.domain == "media" and decision.lookup_mode == "filter_search":
        return "media_collection_filter"
    if decision.intent == "mixed":
        return "mixed_knowledge_with_media"
    if decision.followup_mode != "none":
        return "followup_proxy"
    if decision.domain in {"tech", "general"} or decision.intent == "knowledge_qa":
        return "knowledge_qa"
    return "general_qa"


# Personal-scope cues shared by subject_scope and has_personal_scope check.
# Exact-string cues cover the common contiguous-verb patterns.  The regex
# additionally catches cases where a date range (or other token) sits between
# "我" and the consumption verb, e.g. "我2024年7-10月看了哪些动画" where the
# substring "我看了" does not appear contiguously.
_PERSONAL_SCOPE_CUES = (
    "我看过", "我看了", "我追过", "我补过",
    "我读过", "我读了", "我听过",
    "我玩过", "我打过", "我记录过",
)
_PERSONAL_SCOPE_RE = re.compile(
    r"我.{0,25}[看追补读听玩打][了过]",
    re.UNICODE,
)


def _derive_subject_scope(decision: RouterDecision) -> str:
    """Return 'personal_record' when the question is about the user's own
    consumption history (e.g. "我看过…", "我2024年6月看了…"), otherwise
    'general_knowledge'."""
    text = str(decision.raw_question or "").strip()
    if any(cue in text for cue in _PERSONAL_SCOPE_CUES) or _PERSONAL_SCOPE_RE.search(text):
        return "personal_record"
    return "general_knowledge"


def _derive_time_scope_type(decision: RouterDecision) -> str:
    """Distinguish whether a date window constrains the user's *consumption*
    date (library record date) or the work's *publication/release* date.

    Rules:
      • personal_scope + date window → consumption_date
      • date window but no personal cues → publication_date
      • no date window → ""
    """
    if not decision.time_constraint:
        return ""
    text = str(decision.raw_question or "").strip()
    if any(cue in text for cue in _PERSONAL_SCOPE_CUES) or _PERSONAL_SCOPE_RE.search(text):
        return "consumption_date"
    return "publication_date"


def _derive_answer_shape(decision: RouterDecision) -> str:
    """Derive the expected shape of the answer from the router decision.

    Values:
      list_plus_expand — collection query where user wants per-item narrative
      detail_card      — single-entity lookup with explanation requested
      compare          — explicit comparison intent
      list_only        — plain collection list
      ""               — no strong shape signal (knowledge / general)
    """
    if decision.needs_comparison:
        return "compare"
    if decision.lookup_mode == "entity_lookup" and len(decision.entities) == 1:
        return "detail_card"
    is_collection = (
        decision.lookup_mode in ("filter_search", "general_lookup")
        and decision.domain == "media"
    ) or bool((decision.evidence or {}).get("creator_collection_query"))
    expand_cues = (
        "介绍", "展开", "分别说说", "详细说", "分别介绍", "说说",
        # Patterns like "分别是讲什么的 / 讲什么的 / 讲了什么 / 是什么内容"
        # semantically mean per-item expansion even though the surface phrasing
        # doesn't contain "介绍" or "展开".
        "分别是讲什么的", "讲什么的", "讲了什么", "是什么内容", "什么故事",
        "都是什么", "各自讲", "概述", "是讲什么",
    )
    text = str(decision.raw_question or "").strip()
    if is_collection and decision.needs_explanation:
        return "list_plus_expand"
    if is_collection and any(cue in text for cue in expand_cues):
        return "list_plus_expand"
    if is_collection:
        return "list_only"
    return ""


# Media type strings that identify audiovisual content.  Used by
# _derive_media_family and consumed by RoutingPolicy (TMDB eligibility),
# PostRetrievalPolicy (repair path), and AnswerPolicy (external block form).
# "video" is the canonical normalised value used by this system's adapter.
_AUDIOVISUAL_MEDIA_TYPES: frozenset[str] = frozenset({
    "video", "movie", "film", "tv", "drama", "anime", "animation",
    "documentary", "series", "show", "miniseries",
})
_BOOKISH_MEDIA_TYPES: frozenset[str] = frozenset({
    "book", "novel", "manga", "comic", "essay", "literature",
    "nonfiction", "fiction", "poetry",
})
_MUSIC_MEDIA_TYPES: frozenset[str] = frozenset({"music", "album", "song"})
_GAME_MEDIA_TYPES: frozenset[str] = frozenset({"game", "videogame", "boardgame"})


def _derive_media_family(decision: RouterDecision) -> str:
    """Classify the content family from decision.media_type.

    Returns one of: audiovisual | bookish | music | game | mixed | ""
    """
    mt = str(decision.media_type or "").lower().strip()
    if not mt:
        return ""
    # A single canonical token can match exactly.
    if mt in _AUDIOVISUAL_MEDIA_TYPES:
        return "audiovisual"
    if mt in _BOOKISH_MEDIA_TYPES:
        return "bookish"
    if mt in _MUSIC_MEDIA_TYPES:
        return "music"
    if mt in _GAME_MEDIA_TYPES:
        return "game"
    # Compound tokens like "video,book" or space-separated values.
    tokens = {t.strip() for t in mt.replace(",", " ").split()}
    families = set()
    for tok in tokens:
        if tok in _AUDIOVISUAL_MEDIA_TYPES:
            families.add("audiovisual")
        elif tok in _BOOKISH_MEDIA_TYPES:
            families.add("bookish")
        elif tok in _MUSIC_MEDIA_TYPES:
            families.add("music")
        elif tok in _GAME_MEDIA_TYPES:
            families.add("game")
    if len(families) == 1:
        return families.pop()
    if len(families) > 1:
        return "mixed"
    return ""



def _decision_requires_tmdb(decision: RouterDecision) -> bool:
    text = str(decision.raw_question or "").strip()
    if not text or not decision.needs_media_db:
        return False
    plot_detail_cues = ("剧情", "简介", "介绍", "讲了什么")
    evaluation_cues = ("评分", "评价")
    if decision.followup_mode == "inherit_filters" and any(cue in text for cue in plot_detail_cues):
        return True
    if decision.followup_mode == "inherit_entity" and any(cue in text for cue in (*ROUTER_MEDIA_DETAIL_CUES, *evaluation_cues)):
        return True
    if (_is_collection_media_query(text) or decision.followup_mode in {"inherit_filters", "inherit_timerange"}) and not decision.entities:
        return False
    if (decision.lookup_mode == "filter_search" or _has_specific_media_constraints(decision.filters) or decision.date_range) and not decision.entities:
        return False
    detail_cues = ROUTER_MEDIA_DETAIL_CUES
    if any(cue in text for cue in TMDB_AUDIOVISUAL_CUES):
        return True
    if "《" in text and "》" in text and any(cue in text for cue in detail_cues):
        return True
    if decision.entities and any(cue in text for cue in detail_cues):
        return True
    if decision.followup_mode == "inherit_entity" and any(cue in text for cue in detail_cues):
        return True
    return False


def _build_router_decision(
    question: str,
    history: list[dict[str, str]],
    quota_state: dict[str, Any],
    query_profile: dict[str, Any],
) -> tuple[RouterDecision, dict[str, Any], dict[str, Any]]:
    raw_question = str(question or "").strip()
    previous_question = _find_previous_user_question(raw_question, history)
    previous_trace = _find_previous_trace_context(history)
    previous_trace_state = previous_trace.get("conversation_state_after") if isinstance(previous_trace.get("conversation_state_after"), dict) else {}
    previous_state = dict(previous_trace_state) if previous_trace_state else _infer_prior_question_state(previous_question, history)

    llm_media = _classify_media_query_with_llm(raw_question, quota_state, previous_state=previous_state, history=history)
    llm_label = str(llm_media.get("label") or CLASSIFIER_LABEL_OTHER)
    llm_entities = [
        str(item).strip()
        for item in llm_media.get("entities", [])
        if str(item).strip() and not _looks_like_generic_media_scope(str(item).strip())
    ]
    llm_filters = _normalize_media_filter_map(llm_media.get("filters"))
    llm_lookup_mode = str(llm_media.get("lookup_mode") or "general_lookup").strip() or "general_lookup"
    llm_domain = str(llm_media.get("domain") or "general").strip() or "general"
    llm_ranking = dict(llm_media.get("ranking") or {})
    media_entities = [
        str(item).strip()
        for item in _extract_media_entities(raw_question)
        if str(item).strip() and not _looks_like_generic_media_scope(str(item).strip())
    ]
    if not media_entities and llm_entities:
        media_entities = llm_entities
    media_title_marked = _has_media_title_marker(raw_question)
    media_intent_cues = _has_media_intent_cues(raw_question)
    media_surface = _has_router_media_surface(raw_question)
    abstract_media_concept = _is_abstract_media_concept_query(raw_question)
    collection_query = _is_collection_media_query(raw_question)
    if collection_query and not media_title_marked:
        media_entities = []
    followup_resolution = _derive_router_followup_resolution(raw_question, previous_state)
    followup_mode = followup_resolution.mode
    inherited_filters = _normalize_media_filter_map(previous_state.get("filters")) if followup_mode in {"inherit_filters", "inherit_timerange"} else {}
    current_filters = _merge_router_filters(_infer_media_filters(raw_question), llm_filters)
    # SchemaProjectionAdapter handles both creator-role remapping (director→author)
    # and media-type semantic projection (movie→video+category:电影).
    current_projection = _schema_adapter.project(current_filters, resolved_question=raw_question)
    current_filters = current_projection.filters
    filters = merge_followup_filters(
        inherited_filters,
        current_filters,
        strategy=followup_resolution.merge_strategy if followup_mode != "none" else "none",
    )
    if current_filters.get("year"):
        filters["year"] = [str(value).strip() for value in current_filters.get("year", []) if str(value).strip()]

    fallback_year = _extract_year_from_date_range(previous_state.get("date_range"))
    current_date_window = _parse_media_date_window(raw_question, fallback_year if followup_mode != "none" else "")
    date_range: list[str] = []
    time_constraint: dict[str, Any] = {}
    if current_date_window:
        date_range = [str(current_date_window.get("start") or ""), str(current_date_window.get("end") or "")]
        time_constraint = _build_time_constraint(current_date_window, source="current")
    elif followup_mode == "inherit_filters":
        date_range = [str(item or "") for item in list(previous_state.get("date_range") or [])[:2] if str(item or "")]
        inherited_window = _date_window_from_state({"date_range": date_range})
        if inherited_window:
            time_constraint = _build_time_constraint({**inherited_window, "kind": "inherited_range"}, source="inherited")

    entities = [str(item).strip() for item in media_entities if str(item).strip()]
    if not entities and followup_mode == "inherit_entity":
        previous_entities = [str(item).strip() for item in list(previous_state.get("entities") or []) if str(item).strip()]
        previous_entity = str(previous_state.get("entity") or "").strip()
        entities = previous_entities or ([previous_entity] if previous_entity else [])
    entities, entity_filters = _normalize_media_entities_and_filters(entities)
    filters = merge_followup_filters(filters, entity_filters, strategy="augment")

    projection = _schema_adapter.project(filters, resolved_question=raw_question)
    filters = projection.filters
    media_type = projection.resolved_media_type or _resolved_media_type_label(filters, raw_question)
    if not media_type and followup_mode in {"inherit_filters", "inherit_entity", "inherit_timerange"}:
        media_type = str(previous_state.get("media_type") or "").strip()
    if media_type == "anime":
        filters = merge_followup_filters(filters, {"media_type": ["video"], "category": ["动画"]}, strategy="augment")
    selection = _build_media_selection(filters, media_type)

    freshness = _infer_router_freshness(raw_question)
    tech_cues = _has_router_tech_cues(raw_question)
    if tech_cues and llm_label == CLASSIFIER_LABEL_TECH and not media_title_marked and not media_intent_cues:
        entities = []
    media_signal = bool(entities) or media_surface or abstract_media_concept or collection_query or followup_mode != "none" or llm_domain == "media" or llm_lookup_mode != "general_lookup"
    tech_signal = tech_cues or llm_label == CLASSIFIER_LABEL_TECH or llm_domain == "tech"

    # ── Domain Arbitration Matrix ───────────────────────────────────────────────
    # Priority (highest to lowest):
    #   tech_primary       : lexical tech cues + LLM agrees TECH, no explicit media anchor
    #   media_primary      : explicit entity/title/filter/followup with no tech signal
    #   mixed              : entity or followup is present AND tech signal is present
    #   tech_signal_only   : only tech signals, no media (falls to "tech" domain)
    #   general            : neither domain signal
    # ──────────────────────────────────────────────────────────────────────────
    strong_tech_override = (
        tech_cues
        and llm_label == CLASSIFIER_LABEL_TECH
        and not bool(entities)
        and not media_surface
        and not media_intent_cues
    )
    # tech-primary: LLM says TECH, no explicit media anchor regardless of llm_lookup_mode
    # Handles queries like "机器学习的概念和应用".
    tech_primary = strong_tech_override
    # media-primary: explicit media anchor exists (entity/title, media_surface, intent cue,
    # collection, abstract concept like "拉美文学", or active followup) and no conflicting
    # strong tech signal. Handles: "推荐几部法国新浪潮电影", "《教父》的导演是谁", "拉美文学有哪些".
    explicit_media_anchor = (
        bool(entities)
        or media_surface
        or media_intent_cues
        or collection_query
        or abstract_media_concept  # e.g. "拉美文学", "新浪潮电影风格"
        or (followup_mode != "none" and _state_has_media_context(previous_state))
        # LLM explicitly classified as media filter/entity search — stronger than
        # just voting domain=media.  Still kept out of llm_media_weak_general path.
        or (llm_domain == "media" and llm_lookup_mode in {"filter_search", "entity_lookup"})
    )
    media_primary = explicit_media_anchor and not tech_primary
    # mixed: entity or inherited followup EXISTS alongside tech cues
    # Handles: "机器学习在电影推荐系统里的应用", "讲机器学习原理的书推荐".
    mixed_domain = (
        not tech_primary
        and tech_signal
        and media_signal
        and (bool(entities) or media_title_marked or followup_mode == "inherit_entity")
    )

    if tech_primary:
        arbitration: str = "tech_primary"
        intent: Literal["knowledge_qa", "media_lookup", "mixed", "chat"] = "knowledge_qa"
        domain: Literal["tech", "media", "general"] = "tech"
    elif mixed_domain:
        arbitration = "mixed_due_to_entity_plus_tech"
        intent = "mixed"
        domain = "media"  # media tools run first; doc RAG appended by RoutingPolicy
    elif media_primary:
        if media_surface or media_intent_cues:
            arbitration = "media_surface_wins"
        elif bool(entities):
            arbitration = "entity_wins"
        elif abstract_media_concept:
            arbitration = "abstract_concept_wins"
        else:
            arbitration = "followup_or_collection_wins"
        intent = "media_lookup"
        domain = "media"
    elif media_signal and not tech_signal:
        # Only LLM voted media with no structural anchor — safe-default to general
        # to prevent false media routing on queries like "机器学习的概念和应用".
        arbitration = "llm_media_weak_general"
        intent = "knowledge_qa"
        domain = "general"
    elif tech_signal:
        arbitration = "tech_signal_only"
        intent = "knowledge_qa"
        domain = "tech"
    elif any(cue in raw_question.lower() for cue in ROUTER_CHAT_CUES):
        arbitration = "chat"
        intent = "chat"
        domain = "general"
    else:
        arbitration = "general_fallback"
        intent = "knowledge_qa"
        domain = "general"

    needs_media_db = domain == "media"
    needs_doc_rag = domain == "tech" or intent == "mixed" or (domain == "general" and intent == "knowledge_qa")
    needs_web = freshness != "none"

    reasons: list[str] = []
    if followup_mode != "none":
        reasons.append(f"followup:{followup_mode}")
        for marker in followup_resolution.reasons:
            reasons.append(f"followup_reason:{marker}")
    if entities:
        reasons.append("explicit_media_entity")
    if media_surface:
        reasons.append("media_surface")
    if media_intent_cues:
        reasons.append("media_intent_cues")
    if collection_query:
        reasons.append("collection_query")
    if abstract_media_concept:
        reasons.append("abstract_media_concept")
    if tech_cues:
        reasons.append("lexical_tech_cues")
    if llm_label == CLASSIFIER_LABEL_MEDIA:
        reasons.append("llm_label_media")
    elif llm_label == CLASSIFIER_LABEL_TECH:
        reasons.append("llm_label_tech")
    if strong_tech_override:
        reasons.append("strong_tech_override")
    reasons.append(f"arbitration:{arbitration}")
    # Trace silent media signal sources so misrouting is diagnosable
    if llm_domain == "media" and not strong_tech_override:
        reasons.append("llm_domain_media")
    if llm_lookup_mode != "general_lookup" and domain == "media":
        reasons.append(f"llm_lookup_mode:{llm_lookup_mode}")
    if freshness != "none":
        reasons.append(f"freshness:{freshness}")

    confidence = 0.42
    if entities:
        confidence = max(confidence, 0.92)
    if followup_mode != "none" and _state_has_media_context(previous_state):
        confidence = max(confidence, 0.84)
    if media_intent_cues or collection_query or abstract_media_concept:
        confidence = max(confidence, 0.78)
    if tech_cues:
        confidence = max(confidence, 0.76)
    if llm_label in {CLASSIFIER_LABEL_MEDIA, CLASSIFIER_LABEL_TECH}:
        confidence = max(confidence, 0.68)
    if float(llm_media.get("confidence") or 0.0) > 0:
        confidence = max(confidence, min(0.96, float(llm_media.get("confidence") or 0.0)))
    if len(raw_question) <= 8 and not entities and not filters and not tech_cues and followup_mode == "none":
        confidence = min(confidence, 0.45)

    lookup_mode = _derive_media_lookup_mode(
        domain=domain,
        entities=entities,
        filters=filters,
        date_range=[item for item in date_range if item],
        followup_mode=followup_mode,
        abstract_media_concept=abstract_media_concept,
        collection_query=collection_query,
    )
    if domain == "media" and llm_lookup_mode in {"filter_search", "entity_lookup", "concept_lookup"}:
        if llm_lookup_mode == "entity_lookup" and not collection_query:
            lookup_mode = "entity_lookup"
        elif llm_lookup_mode == "filter_search" and (filters or followup_mode != "none" or collection_query or len(date_range) == 2):
            lookup_mode = "filter_search"
        elif llm_lookup_mode == "concept_lookup" and abstract_media_concept:
            lookup_mode = "concept_lookup"
    if lookup_mode == "filter_search":
        entities = []
        reasons = [reason for reason in reasons if reason != "explicit_media_entity"]
    ranking = _build_media_ranking(raw_question, lookup_mode, time_constraint)
    if isinstance(llm_ranking, dict) and str(llm_ranking.get("mode") or "").strip() and ranking.get("mode") == "relevance":
        ranking = {
            "mode": str(llm_ranking.get("mode") or "relevance").strip() or "relevance",
            "source": str(llm_ranking.get("source") or "llm").strip() or "llm",
        }
    previous_ranking = previous_state.get("ranking") if isinstance(previous_state.get("ranking"), dict) else {}
    previous_sort = str(previous_state.get("sort") or "").strip()
    if followup_mode == "inherit_timerange" and ranking.get("source") == "time_constraint_default" and previous_sort:
        ranking = dict(previous_ranking) if previous_ranking else {"mode": previous_sort, "source": "inherited"}
        ranking["mode"] = str(ranking.get("mode") or previous_sort)
        ranking["source"] = str(ranking.get("source") or "inherited")
    sort = str(ranking.get("mode") or "relevance")
    allow_downstream_entity_inference = domain == "media" and lookup_mode == "general_lookup" and confidence < ROUTER_CONFIDENCE_MEDIUM
    if domain == "media" and lookup_mode == "general_lookup" and not entities and not _has_specific_media_constraints(filters):
        confidence = min(confidence, 0.52)
        allow_downstream_entity_inference = True
    followup_target = str(llm_media.get("followup_target") or "").strip() or (entities[0] if entities else _media_scope_label(media_type, filters))
    resolved_question = _render_resolved_question_from_decision(raw_question, previous_state, followup_mode, entities)
    decision = RouterDecision(
        raw_question=raw_question,
        resolved_question=resolved_question,
        intent=intent,
        domain=domain,
        lookup_mode=lookup_mode,
        selection=selection,
        time_constraint=time_constraint,
        ranking=ranking,
        entities=entities,
        filters=filters,
        date_range=[item for item in date_range if item],
        sort=sort,
        freshness=freshness,
        needs_web=needs_web,
        needs_doc_rag=needs_doc_rag,
        needs_media_db=needs_media_db,
        needs_external_media_db=False,
        followup_mode=followup_mode,
        followup_filter_strategy=followup_resolution.merge_strategy,
        confidence=confidence,
        reasons=reasons,
        media_type=media_type,
        llm_label=llm_label,
        query_type=QUERY_TYPE_GENERAL,
        allow_downstream_entity_inference=allow_downstream_entity_inference,
        followup_target=followup_target,
        needs_comparison=bool(llm_media.get("needs_comparison")),
        needs_explanation=bool(llm_media.get("needs_explanation")) or _question_requests_media_details(raw_question) or _question_requests_personal_evaluation(raw_question),
        rewritten_queries={},
        arbitration=arbitration,
        evidence={
            "media_title_marked": media_title_marked,
            "media_intent_cues": media_intent_cues,
            "collection_query": collection_query,
            "abstract_media_concept": abstract_media_concept,
            "tech_cues": tech_cues,
            "llm_lookup_mode": llm_lookup_mode,
            "llm_domain": llm_domain,
            "profile": str(query_profile.get("profile", "medium") or "medium"),
        },
    )
    decision = _apply_router_semantic_repairs(raw_question, decision, previous_state)
    llm_tool_queries = _rewrite_tool_queries_with_llm(raw_question, decision, previous_state, quota_state)
    decision.rewritten_queries = _build_tool_grade_rewritten_queries(raw_question, decision, previous_state, llm_tool_queries)
    if decision.domain == "media":
        decision.resolved_question = str(
            (decision.rewritten_queries or {}).get("media_query")
            or _render_resolved_question_from_decision(raw_question, previous_state, decision.followup_mode, decision.entities)
        ).strip()
    else:
        decision.resolved_question = str((decision.rewritten_queries or {}).get("doc_query") or resolved_question).strip()
    decision.evidence = {
        **dict(decision.evidence or {}),
        "llm_tool_rewrites": llm_tool_queries,
        "tool_rewrite_source": "deterministic_plus_llm_rewrite" if llm_tool_queries else "deterministic",
    }
    decision.needs_external_media_db = _decision_requires_tmdb(decision)
    decision.query_type = _map_router_query_type(decision)
    decision.query_class = _derive_query_class(decision)
    # ── New semantic slots ──────────────────────────────────────────────────
    decision.subject_scope = _derive_subject_scope(decision)
    decision.time_scope_type = _derive_time_scope_type(decision)
    decision.answer_shape = _derive_answer_shape(decision)
    decision.media_family = _derive_media_family(decision)
    return decision, llm_media, previous_state


def _router_decision_to_query_classification(
    decision: RouterDecision,
    llm_media: dict[str, Any],
    previous_state: dict[str, Any],
    query_profile: dict[str, Any],
) -> dict[str, Any]:
    media_title_marked = bool(decision.evidence.get("media_title_marked"))
    media_intent_cues = bool(decision.evidence.get("media_intent_cues"))
    tech_cues = bool(decision.evidence.get("tech_cues"))
    return {
        "query_type": decision.query_type,
        "lookup_mode": decision.lookup_mode,
        "media_entity": decision.entities[0] if decision.entities else "",
        "media_entities": list(decision.entities),
        "followup_target": str(decision.followup_target or ""),
        "rewritten_queries": {str(key): str(value) for key, value in (decision.rewritten_queries or {}).items() if str(key).strip() and str(value).strip()},
        "media_specific": bool(decision.entities),
        "media_entity_confident": bool(decision.entities),
        "media_title_marked": media_title_marked,
        "media_intent_cues": media_intent_cues,
        "llm_media": llm_media,
        "doc_similarity": {},
        "tech_score": 0.0,
        "tech_threshold": TECH_QUERY_DOC_SIM_THRESHOLD,
        "weak_tech_threshold": max(0.18, TECH_QUERY_DOC_SIM_THRESHOLD - 0.10),
        "weak_tech_signal": False,
        "query_tokens": _classifier_token_count(decision.raw_question),
        "profile_token_count": int(query_profile.get("token_count", 0) or 0),
        "short_media_surface": bool(decision.evidence.get("profile") == "short" and decision.domain == "media"),
        "disable_media_search": False,
        "media_signal": decision.needs_media_db,
        "strong_tech_signal": decision.domain == "tech" and (tech_cues or decision.llm_label == CLASSIFIER_LABEL_TECH),
        "abstract_media_concept": bool(decision.evidence.get("abstract_media_concept")),
        "tmdb_candidate": bool(decision.needs_external_media_db),
        "force_media_state": decision.followup_mode != "none" and _state_has_media_context(previous_state),
        "router_decision": _serialize_router_decision(decision),
        "fallback_evidence": {},
    }


# RoutingPolicy is imported from routing_policy.py at the top of this module.
# See nav_dashboard/web/services/routing_policy.py for the full implementation.


def _resolve_agent_no_context(query_type: str, rag_used: int, doc_no_context: int) -> tuple[int, str]:
    normalized_type = _normalize_query_type(query_type)
    if int(doc_no_context or 0) > 0:
        return 1, "below_threshold"
    if int(rag_used or 0) <= 0 and normalized_type in {QUERY_TYPE_TECH, QUERY_TYPE_MIXED}:
        return 1, "knowledge_route_without_rag"
    return 0, ""


def _http_json(
    method: str,
    url: str,
    payload: dict[str, Any] | None = None,
    timeout: float = 25.0,
    headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    req_body = None
    request_headers = {"Accept": "application/json"}
    if isinstance(headers, dict):
        for key, value in headers.items():
            header_key = str(key or "").strip()
            header_value = str(value or "").strip()
            if header_key and header_value:
                request_headers[header_key] = header_value
    if payload is not None:
        clean_payload = dict(payload)
        req_body = json.dumps(clean_payload, ensure_ascii=False).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    req = urlrequest.Request(url=url, data=req_body, headers=request_headers, method=method.upper())
    host = (urlparse.urlparse(url).hostname or "").lower()
    local_hosts = {"127.0.0.1", "localhost", "::1"}
    try:
        if host in local_hosts:
            opener = urlrequest.build_opener(urlrequest.ProxyHandler({}))
            resp_ctx = opener.open(req, timeout=timeout)
        else:
            resp_ctx = urlrequest.urlopen(req, timeout=timeout)

        with resp_ctx as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            if not raw.strip():
                return {}
            data = json.loads(raw)
            if isinstance(data, dict):
                return data
            return {"value": data}
    except urlerror.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else str(exc)
        raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(str(exc)) from exc


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


def _load_quota_state() -> dict[str, Any]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not QUOTA_FILE.exists():
        return {"date": datetime.now().strftime("%Y-%m-%d"), "web_search": 0, "deepseek": 0}
    try:
        data = json.loads(QUOTA_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"date": datetime.now().strftime("%Y-%m-%d"), "web_search": 0, "deepseek": 0}
    if not isinstance(data, dict):
        data = {}
    today = datetime.now().strftime("%Y-%m-%d")
    if data.get("date") != today:
        return {"date": today, "web_search": 0, "deepseek": 0}
    return {
        "date": today,
        "web_search": int(data.get("web_search", 0) or 0),
        "deepseek": int(data.get("deepseek", 0) or 0),
    }


def _save_quota_state(state: dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    QUOTA_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _current_month() -> str:
    return datetime.now().strftime("%Y-%m")


def _load_quota_history() -> dict[str, Any]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not QUOTA_HISTORY_FILE.exists():
        return {"months": {}}
    try:
        payload = json.loads(QUOTA_HISTORY_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"months": {}}
    if not isinstance(payload, dict):
        return {"months": {}}
    months = payload.get("months")
    if not isinstance(months, dict):
        months = {}
    cleaned: dict[str, dict[str, int]] = {}
    for month, row in months.items():
        if not isinstance(row, dict):
            continue
        key = str(month or "").strip()
        if not key:
            continue
        cleaned[key] = {
            "web_search": int(row.get("web_search", 0) or 0),
            "deepseek": int(row.get("deepseek", 0) or 0),
        }
    return {"months": cleaned}


def _save_quota_history(history: dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    QUOTA_HISTORY_FILE.write_text(json.dumps(history, ensure_ascii=False, indent=2), encoding="utf-8")


def _set_monthly_quota_usage(*, web_search: int | None = None, deepseek: int | None = None) -> dict[str, int]:
    with _LOCK:
        history = _load_quota_history()
        months = history.get("months") if isinstance(history.get("months"), dict) else {}
        month_key = _current_month()
        current = months.get(month_key) if isinstance(months.get(month_key), dict) else {}
        next_row = {
            "web_search": int(current.get("web_search", 0) or 0),
            "deepseek": int(current.get("deepseek", 0) or 0),
        }
        if web_search is not None:
            next_row["web_search"] = max(0, int(web_search))
        if deepseek is not None:
            next_row["deepseek"] = max(0, int(deepseek))
        months[month_key] = next_row
        history["months"] = months
        _save_quota_history(history)
        return next_row


def _record_quota_usage(*, web_search_delta: int = 0, deepseek_delta: int = 0) -> None:
    web_inc = int(web_search_delta or 0)
    deepseek_inc = int(deepseek_delta or 0)
    if web_inc <= 0 and deepseek_inc <= 0:
        return
    with _LOCK:
        history = _load_quota_history()
        months = history.get("months") if isinstance(history.get("months"), dict) else {}
        month_key = _current_month()
        current = months.get(month_key) if isinstance(months.get(month_key), dict) else {}
        current_web = int(current.get("web_search", 0) or 0)
        current_deepseek = int(current.get("deepseek", 0) or 0)
        months[month_key] = {
            "web_search": current_web + max(0, web_inc),
            "deepseek": current_deepseek + max(0, deepseek_inc),
        }
        history["months"] = months
        _save_quota_history(history)


def _increment_quota_state(state: dict[str, Any], *, web_search_delta: int = 0, deepseek_delta: int = 0) -> None:
    web_inc = int(web_search_delta or 0)
    deepseek_inc = int(deepseek_delta or 0)
    with _LOCK:
        if web_inc > 0:
            state["web_search"] = int(state.get("web_search", 0) or 0) + web_inc
        if deepseek_inc > 0:
            state["deepseek"] = int(state.get("deepseek", 0) or 0) + deepseek_inc
        if web_inc > 0 or deepseek_inc > 0:
            _save_quota_state(state)
            _record_quota_usage(web_search_delta=web_inc, deepseek_delta=deepseek_inc)


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _session_file_path(session_id: str) -> Path:
    return SESSIONS_DIR / f"{SESSION_FILE_PREFIX}{session_id}.json"


def _memory_file_path(session_id: str) -> Path:
    return MEMORY_DIR / f"memory_{session_id}.json"


def _derive_session_title(question: str, max_len: int | None = None) -> str:
    text = re.sub(r"\s+", " ", str(question or "").strip())
    if not text:
        return "新会话"
    text = text.strip("，。！？!?;；:：")
    if max_len is None or max_len <= 0 or len(text) <= max_len:
        return text
    return text[:max_len].rstrip() + "..."


def _sanitize_session_title(title: str, max_len: int | None = None) -> str:
    text = re.sub(r"\s+", " ", str(title or "").strip())
    text = text.strip("，。！？!?;；:：")
    if not text:
        return ""
    if max_len is None or max_len <= 0 or len(text) <= max_len:
        return text
    return text[:max_len].rstrip() + "..."


def _normalize_title_compare_key(text: str) -> str:
    return re.sub(r"[^\w\u4e00-\u9fff]+", "", str(text or "").strip().lower())


def _looks_like_weak_session_title(title: str, question: str) -> bool:
    normalized_title = _normalize_title_compare_key(title)
    normalized_question = _normalize_title_compare_key(question)
    if not normalized_title:
        return True
    if normalized_question.startswith(normalized_title):
        return True
    weak_prefixes = (
        "请",
        "帮我",
        "请帮我",
        "比较",
        "请比较",
        "分析",
        "请分析",
        "解释",
        "请解释",
        "说明",
        "请说明",
    )
    return any(str(title or "").strip().startswith(prefix) for prefix in weak_prefixes)


def _build_answer_anchor_title(question: str, answer: str, max_len: int | None = None) -> str:
    lines = [re.sub(r"^#{1,6}\s*", "", line).strip() for line in str(answer or "").splitlines()]
    headings = [
        line for line in lines
        if line and line not in {"参考资料", "结论", "关键要点", "总结", "背景与关键机制", "典型使用场景及角色"}
    ]
    for heading in headings:
        candidate = _sanitize_session_title(heading, max_len=max_len)
        if candidate and not _looks_like_weak_session_title(candidate, question):
            return candidate
    return ""


def _generate_local_session_title(question: str, answer: str, max_len: int | None = None) -> str:
    title = ""
    local_url = (os.getenv("NAV_DASHBOARD_LOCAL_LLM_URL", "") or os.getenv("AI_SUMMARY_LOCAL_LLM_URL", "")).strip()
    local_model = (os.getenv("NAV_DASHBOARD_LOCAL_LLM_MODEL", "") or os.getenv("AI_SUMMARY_LOCAL_LLM_MODEL", "")).strip() or "qwen2.5-7b-instruct"
    local_key = (os.getenv("NAV_DASHBOARD_LOCAL_LLM_API_KEY", "") or os.getenv("AI_SUMMARY_LOCAL_LLM_API_KEY", "local")).strip() or "local"
    if local_url and local_model and chat_completion_with_retry is not None:
        try:
            title = chat_completion_with_retry(
                api_key=local_key,
                base_url=local_url,
                model=local_model,
                timeout=20,
                temperature=0.2,
                max_retries=1,
                retry_delay=1.0,
                messages=[
                    {
                        "role": "system",
                        "content": "你负责为中文问答会话生成标题。请输出一个简洁、可读、具体的中文标题，长度尽量控制在 8-18 个字，不要带引号、序号或句末标点。只输出标题本身。",
                    },
                    {
                        "role": "user",
                        "content": f"用户问题：{question}\n\n回答摘要：{_clip_text(answer, 800)}\n\n请生成标题。",
                    },
                ],
            )
        except Exception:
            title = ""
    normalized = _sanitize_session_title(title, max_len=max_len)
    if normalized and not _looks_like_weak_session_title(normalized, question):
        return normalized
    answer_anchor = _build_answer_anchor_title(question, answer, max_len=max_len)
    if answer_anchor:
        return answer_anchor
    fallback = _sanitize_session_title(question, max_len=max_len)
    return fallback or "新会话"


def _schedule_generated_session_title(session_id: str, question: str, answer: str, *, lock: bool = True) -> None:
    sid = str(session_id or "").strip()
    if not sid or not str(answer or "").strip():
        return

    def _run() -> None:
        try:
            session = get_session(sid)
            if not session or bool(session.get("title_locked", False)):
                return
            title = _generate_local_session_title(question, answer)
            if not title:
                return
            set_session_title(sid, title, lock=lock)
        except Exception:
            return

    threading.Thread(target=_run, daemon=True, name=f"agent-title-{sid[:8]}").start()


_BUG_MARKER_PREFIX = "BUG-TICKET:"


def _auto_queue_bug_tickets(text: str, *, session_id: str = "", trace_id: str = "") -> None:
    """Extract BUG-TICKET: markers from a response and append them to the outbox.

    This makes ticket ingest hook-independent: the outbox file is written
    in-process immediately after the response is generated, so the next hook
    invocation (even with empty stdin) will consume it via _sync_outbox_fallback.
    """
    if not text or _BUG_MARKER_PREFIX not in text:
        return
    tickets: list[dict[str, Any]] = []
    for line in text.splitlines():
        if _BUG_MARKER_PREFIX not in line:
            continue
        payload_text = line.split(_BUG_MARKER_PREFIX, 1)[1].strip().strip("`")
        try:
            payload = json.loads(payload_text)
            if isinstance(payload, dict):
                tickets.append(payload)
        except Exception:
            continue
    if not tickets:
        return
    outbox_path = _WORKSPACE_ROOT / "nav_dashboard" / "data" / "bug_ticket_outbox.jsonl"
    try:
        outbox_path.parent.mkdir(parents=True, exist_ok=True)
        with outbox_path.open("a", encoding="utf-8") as fh:
            for ticket in tickets:
                entry: dict[str, Any] = {
                    "queued_at": _now_iso(),
                    "source": "inline_response",
                    "session_id": session_id,
                    "trace_id": trace_id,
                    "file_paths": [],
                    "payload": ticket,
                }
                fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass


def _normalize_session(raw: object) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    sid = str(raw.get("id", "")).strip()
    if not sid:
        return None
    title = str(raw.get("title", "新会话")).strip() or "新会话"
    created_at = str(raw.get("created_at", "")).strip() or _now_iso()
    updated_at = str(raw.get("updated_at", "")).strip() or created_at
    title_locked = bool(raw.get("title_locked", False))
    msgs_raw = raw.get("messages", [])
    messages: list[dict[str, Any]] = []
    if isinstance(msgs_raw, list):
        for item in msgs_raw:
            if not isinstance(item, dict):
                continue
            role = str(item.get("role", "")).strip()
            text = str(item.get("text", "")).strip()
            if not role or not text:
                continue
            trace_id = str(item.get("trace_id", "")).strip()
            normalized_message: dict[str, Any] = {"role": role, "text": text}
            if trace_id:
                normalized_message["trace_id"] = trace_id
            messages.append(normalized_message)
    return {
        "id": sid,
        "title": _sanitize_session_title(title) or "新会话",
        "created_at": created_at,
        "updated_at": updated_at,
        "title_locked": title_locked,
        "messages": messages,
    }


def _load_session_file(path: Path) -> dict[str, Any] | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return _normalize_session(raw)


def _save_session(session: dict[str, Any]) -> None:
    normalized = _normalize_session(session)
    if normalized is None:
        return
    SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    _session_file_path(str(normalized["id"])).write_text(
        json.dumps(normalized, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _load_memory(session_id: str) -> dict[str, Any]:
    path = _memory_file_path(session_id)
    if not path.exists():
        return {
            "version": "v1",
            "session_id": session_id,
            "session_goal": "",
            "recent_turns": [],
            "updated_at": _now_iso(),
        }
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {
            "version": "v1",
            "session_id": session_id,
            "session_goal": "",
            "recent_turns": [],
            "updated_at": _now_iso(),
        }
    if not isinstance(raw, dict):
        raw = {}
    turns = raw.get("recent_turns", []) if isinstance(raw.get("recent_turns"), list) else []
    return {
        "version": "v1",
        "session_id": session_id,
        "session_goal": str(raw.get("session_goal", "")).strip(),
        "recent_turns": turns[-MEMORY_MAX_TURNS:],
        "updated_at": str(raw.get("updated_at", "")).strip() or _now_iso(),
    }


def _save_memory(session_id: str, memory: dict[str, Any]) -> None:
    MEMORY_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": "v1",
        "session_id": session_id,
        "session_goal": str(memory.get("session_goal", "")).strip(),
        "recent_turns": (memory.get("recent_turns", []) if isinstance(memory.get("recent_turns"), list) else [])[-MEMORY_MAX_TURNS:],
        "updated_at": _now_iso(),
    }
    _memory_file_path(session_id).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def list_sessions() -> list[dict[str, Any]]:
    SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    sessions: list[dict[str, Any]] = []
    with _LOCK:
        for path in sorted(SESSIONS_DIR.glob(f"{SESSION_FILE_PREFIX}*.json"), key=lambda p: p.name.lower()):
            data = _load_session_file(path)
            if data is not None:
                sessions.append(data)
    return sorted(sessions, key=lambda s: str(s.get("updated_at", "")), reverse=True)


def create_session(title: str = "新会话") -> dict[str, Any]:
    with _LOCK:
        now = _now_iso()
        session = {
            "id": str(uuid4()),
            "title": _sanitize_session_title(title) or "新会话",
            "created_at": now,
            "updated_at": now,
            "title_locked": False,
            "messages": [{"role": "system", "text": "你好，我可以帮你并行查询文档与媒体记录。"}],
        }
        _save_session(session)
    return session


def set_session_title(session_id: str, title: str, lock: bool = True) -> dict[str, Any] | None:
    sid = (session_id or "").strip()
    if not sid:
        return None
    new_title = _sanitize_session_title(title)
    if not new_title:
        return None
    with _LOCK:
        path = _session_file_path(sid)
        if not path.exists():
            return None
        session = _load_session_file(path)
        if session is None:
            return None
        session["title"] = new_title
        session["title_locked"] = bool(lock)
        session["updated_at"] = _now_iso()
        _save_session(session)
        return session


def get_session(session_id: str) -> dict[str, Any] | None:
    sid = (session_id or "").strip()
    if not sid:
        return None
    path = _session_file_path(sid)
    if not path.exists():
        return None
    with _LOCK:
        return _load_session_file(path)


def delete_session(session_id: str) -> bool:
    sid = (session_id or "").strip()
    if not sid:
        return False
    with _LOCK:
        path = _session_file_path(sid)
        if not path.exists():
            return False
        try:
            path.unlink(missing_ok=True)
            _memory_file_path(sid).unlink(missing_ok=True)
        except Exception:
            return False
        return True


def append_message(session_id: str, role: str, text: str, trace_id: str = "") -> None:
    sid = (session_id or "").strip()
    if not sid:
        return
    with _LOCK:
        path = _session_file_path(sid)
        session = _load_session_file(path) if path.exists() else None
        if session is None:
            now = _now_iso()
            session = {
                "id": sid,
                "title": "新会话",
                "created_at": now,
                "updated_at": now,
                "messages": [],
            }
        message: dict[str, Any] = {"role": role, "text": text}
        normalized_trace_id = str(trace_id or "").strip()
        if normalized_trace_id:
            message["trace_id"] = normalized_trace_id
        session.setdefault("messages", []).append(message)
        session["updated_at"] = _now_iso()
        _save_session(session)


def _update_memory_for_session(session_id: str) -> None:
    session = get_session(session_id)
    if not session:
        return
    msgs = session.get("messages", []) if isinstance(session.get("messages"), list) else []
    user_msgs = [m for m in msgs if str(m.get("role", "")).lower() == "user"]
    goal = str(user_msgs[0].get("text", "")).strip()[:80] if user_msgs else ""
    turns: list[dict[str, str]] = []
    for m in msgs[-(MEMORY_MAX_TURNS * 2) :]:
        role = str(m.get("role", "")).strip()
        text = str(m.get("text", "")).strip()
        if not role or not text:
            continue
        turns.append({"role": role, "text": text[:280]})
    memory = _load_memory(session_id)
    memory["session_goal"] = goal
    memory["recent_turns"] = turns[-MEMORY_MAX_TURNS:]
    _save_memory(session_id, memory)


def build_memory_context(session_id: str) -> str:
    sid = (session_id or "").strip()
    if not sid:
        return ""
    memory = _load_memory(sid)
    goal = str(memory.get("session_goal", "")).strip()
    turns = memory.get("recent_turns", []) if isinstance(memory.get("recent_turns"), list) else []
    if not goal and not turns:
        return ""
    lines = ["[Memory]"]
    if goal:
        lines.append(f"- SessionGoal: {goal}")
    if turns:
        lines.append("- RecentTurns:")
        for item in turns[-MEMORY_MAX_TURNS:]:
            if not isinstance(item, dict):
                continue
            role = str(item.get("role", "")).strip()
            text = str(item.get("text", "")).strip()
            if role and text:
                lines.append(f"  - {role}: {text}")
    return "\n".join(lines).strip()


def _get_llm_profile(backend: str) -> tuple[str, str, str, int]:
    if chat_completion_with_retry is None:
        if isinstance(_LLM_IMPORT_ERROR, ModuleNotFoundError) and getattr(_LLM_IMPORT_ERROR, "name", "") == "openai":
            raise RuntimeError("Missing dependency: openai (required by core_service.llm_client)") from _LLM_IMPORT_ERROR
        detail = str(_LLM_IMPORT_ERROR) if _LLM_IMPORT_ERROR else "unknown import error"
        raise RuntimeError(f"LLM client unavailable: {detail}") from _LLM_IMPORT_ERROR

    selected = (backend or "local").strip().lower()
    settings = get_settings() if get_settings is not None else None

    if selected == "deepseek":
        if settings is None:
            raise RuntimeError("DeepSeek backend unavailable: core settings not found")
        if not (settings.api_key or "").strip():
            raise RuntimeError("DeepSeek backend unavailable: missing API key")
        return settings.api_base_url, settings.chat_model, settings.api_key, settings.timeout

    # local by default
    if settings is not None:
        local_url = (os.getenv("NAV_DASHBOARD_LOCAL_LLM_URL", "") or "").strip() or settings.local_llm_url
        local_model = (os.getenv("NAV_DASHBOARD_LOCAL_LLM_MODEL", "") or "").strip() or settings.local_llm_model or LOCAL_LLM_FALLBACK_MODEL
        local_key = (os.getenv("NAV_DASHBOARD_LOCAL_LLM_API_KEY", "") or "").strip() or settings.local_llm_api_key or LOCAL_LLM_FALLBACK_KEY
        timeout = settings.timeout
    else:
        local_url = (os.getenv("NAV_DASHBOARD_LOCAL_LLM_URL", "") or "").strip() or LOCAL_LLM_FALLBACK_URL
        local_model = (os.getenv("NAV_DASHBOARD_LOCAL_LLM_MODEL", "") or "").strip() or LOCAL_LLM_FALLBACK_MODEL
        local_key = (os.getenv("NAV_DASHBOARD_LOCAL_LLM_API_KEY", "") or "").strip() or LOCAL_LLM_FALLBACK_KEY
        # No aggresssive timeout for local LLM — generation can be slow on consumer hardware.
        timeout = 7200

    if local_url and not local_url.rstrip("/").endswith("/v1"):
        local_url = local_url.rstrip("/") + "/v1"
    return local_url, local_model, local_key, timeout


def _llm_chat(messages: list[dict[str, str]], backend: str, quota_state: dict[str, Any], count_quota: bool = True) -> str:
    base_url, model, api_key, timeout = _get_llm_profile(backend)
    is_deepseek = "api.deepseek.com" in (base_url or "").lower()
    if is_deepseek and count_quota:
        _increment_quota_state(quota_state, deepseek_delta=1)

    return chat_completion_with_retry(
        api_key=api_key,
        base_url=base_url,
        model=model,
        timeout=timeout,
        messages=messages,
        temperature=0.2,
        max_retries=2,
        retry_delay=1.5,
    )


def _normalize_search_mode(search_mode: str) -> str:
    value = (search_mode or "").strip().lower()
    if value in {"hybrid", "web", "web_search", "web-search"}:
        return "hybrid"
    return "local_only"


def _normalize_query_type(value: str) -> str:
    raw = str(value or "").strip().upper()
    if raw in {QUERY_TYPE_TECH, QUERY_TYPE_MEDIA, QUERY_TYPE_MIXED, QUERY_TYPE_GENERAL}:
        return raw
    return QUERY_TYPE_GENERAL


def _parse_classifier_label(value: str) -> str:
    text = str(value or "").strip().upper()
    if text.startswith(CLASSIFIER_LABEL_MEDIA):
        return CLASSIFIER_LABEL_MEDIA
    if text.startswith(CLASSIFIER_LABEL_TECH):
        return CLASSIFIER_LABEL_TECH
    if text.startswith(CLASSIFIER_LABEL_OTHER):
        return CLASSIFIER_LABEL_OTHER
    if text in {"ANIME", "MOVIE", "FILM", "BOOK", "GAME", "MANGA", "NOVEL"}:
        return CLASSIFIER_LABEL_MEDIA
    if CLASSIFIER_LABEL_MEDIA in text:
        return CLASSIFIER_LABEL_MEDIA
    if CLASSIFIER_LABEL_TECH in text:
        return CLASSIFIER_LABEL_TECH
    return CLASSIFIER_LABEL_OTHER


def _classifier_token_count(query: str) -> int:
    text = str(query or "").strip().lower()
    if not text:
        return 0
    return len(re.findall(r"[a-z0-9_]+|[\u4e00-\u9fff]{2,}", text))


def _has_media_title_marker(query: str) -> bool:
    return bool(_MEDIA_TITLE_MARKER_RE.search(str(query or "")))


def _has_media_intent_cues(query: str) -> bool:
    text = str(query or "").strip().lower()
    if not text:
        return False
    return any(keyword in text for keyword in MEDIA_INTENT_KEYWORDS)


def _strip_query_scaffolding(query: str) -> str:
    text = str(query or "").strip()
    if not text:
        return ""
    for pattern in MEDIAWIKI_FILLER_PATTERNS:
        text = re.sub(pattern, " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text)
    return text.strip(" ，,。！？?；;：:")


def _is_abstract_media_concept_query(query: str, classification: dict[str, Any] | None = None) -> bool:
    text = _strip_query_scaffolding(query)
    if not text:
        return False
    current = classification or {}
    if bool(current.get("media_entity_confident")):
        return False
    concrete_region_hit = any(
        alias in text and len(nationalities) == 1
        for alias, nationalities in MEDIA_REGION_ALIASES.items()
    )
    if concrete_region_hit and not any(cue in text for cue in ("拉美", "拉丁美洲", "流派", "主义", "风格", "佳作", "冷门", "女性主义", "离散叙事", "魔幻现实主义", "后现代主义", "新浪潮")):
        return False
    if not (_has_media_intent_cues(text) or any(cue in text for cue in MEDIA_ABSTRACT_CONCEPT_CUES)):
        return False
    return any(cue in text for cue in MEDIA_ABSTRACT_CONCEPT_CUES if cue not in {"小说", "文学", "诗歌", "诗集", "散文", "作家"}) or any(
        cue in text for cue in ("拉美", "拉丁美洲", "流派", "主义", "风格", "佳作", "冷门", "女性主义", "离散叙事", "魔幻现实主义", "后现代主义", "新浪潮")
    )


def _merge_filter_values(base: dict[str, list[str]], field: str, values: list[str]) -> None:
    clean = [str(value).strip() for value in values if str(value).strip()]
    if not clean:
        return
    current = [str(value).strip() for value in base.get(field, []) if str(value).strip()]
    seen = {value.casefold() for value in current}
    for value in clean:
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        current.append(value)
    if current:
        base[field] = current


def _infer_media_filters(query: str) -> dict[str, list[str]]:
    text = str(query or "").strip()
    if not text:
        return {}

    filters: dict[str, list[str]] = {}
    if any(keyword in text for keyword in ("游戏", "打过", "玩过", "在玩", "通关")):
        _merge_filter_values(filters, "media_type", ["game"])
    if any(keyword in text for keyword in ("音乐", "专辑", "歌曲", "歌单", "听过")):
        _merge_filter_values(filters, "media_type", ["music"])
    music_filters = infer_music_filters_from_text(text)
    for field, values in music_filters.items():
        _merge_filter_values(filters, field, values)
    if any(keyword in text for keyword in ("电影", "影片", "片子")):
        _merge_filter_values(filters, "media_type", ["video"])
        _merge_filter_values(filters, "category", ["电影"])
    if any(keyword in text for keyword in ("电视剧", "剧集", "连续剧", "美剧", "日剧", "韩剧", "英剧")):
        _merge_filter_values(filters, "media_type", ["video"])
        _merge_filter_values(filters, "category", ["电视剧"])
    if any(keyword in text for keyword in MEDIA_BOOKISH_CUES):
        _merge_filter_values(filters, "media_type", ["book"])

    for alias, nationalities in MEDIA_REGION_ALIASES.items():
        if alias in text:
            _merge_filter_values(filters, "nationality", nationalities)

    for cue, categories in MEDIA_BOOK_CATEGORY_HINTS.items():
        if cue in text:
            _merge_filter_values(filters, "category", categories)

    for cue, categories in MEDIA_VIDEO_CATEGORY_HINTS.items():
        if cue in text:
            _merge_filter_values(filters, "category", categories)
            _merge_filter_values(filters, "media_type", ["video"])

    if re.search(r"(?:^|[\s，,。！？?；;：:~\-－—]|\d)番(?:呢|吗|吧|剧)?(?:$|[\s，,。！？?；;：:~\-－—])", text):
        _merge_filter_values(filters, "category", ["动画"])
        _merge_filter_values(filters, "media_type", ["video"])

    for year in re.findall(r"(20\d{2})年", text):
        _merge_filter_values(filters, "year", [year])

    return filters


def _extract_year_from_date_range(date_range: Any) -> str:
    if isinstance(date_range, list) and len(date_range) == 2:
        start = str(date_range[0] or "").strip()
        if len(start) >= 4 and start[:4].isdigit():
            return start[:4]
    return ""


def _extract_media_time_hint(query: str) -> dict[str, Any]:
    text = str(query or "").strip()
    if not text:
        return {}

    half_match = re.search(r"(?:(?P<year>20\d{2})\s*年?)?\s*(?P<half>上半年|下半年)", text)
    if half_match:
        half = str(half_match.group("half") or "").strip()
        return {
            "raw": str(half_match.group(0) or "").strip(),
            "year": str(half_match.group("year") or "").strip(),
            "explicit_year": bool(half_match.group("year")),
            "start_month": 1 if half == "上半年" else 7,
            "end_month": 6 if half == "上半年" else 12,
            "label": half,
        }

    range_match = re.search(r"(?:(?P<year>20\d{2})\s*年?)?\s*(?P<start>\d{1,2})(?:月)?\s*(?:到|至|[-~—－])\s*(?P<end>\d{1,2})月?", text)
    if range_match:
        start_month = max(1, min(12, int(range_match.group("start"))))
        end_month = max(1, min(12, int(range_match.group("end"))))
        if start_month > end_month:
            start_month, end_month = end_month, start_month
        return {
            "raw": str(range_match.group(0) or "").strip(),
            "year": str(range_match.group("year") or "").strip(),
            "explicit_year": bool(range_match.group("year")),
            "start_month": start_month,
            "end_month": end_month,
            "label": f"{start_month}-{end_month}月",
        }

    month_match = re.search(r"(?:(?P<year>20\d{2})\s*年?)?\s*(?P<month>\d{1,2})月", text)
    if month_match:
        month = max(1, min(12, int(month_match.group("month"))))
        return {
            "raw": str(month_match.group(0) or "").strip(),
            "year": str(month_match.group("year") or "").strip(),
            "explicit_year": bool(month_match.group("year")),
            "start_month": month,
            "end_month": month,
            "label": f"{month}月",
        }
    return {}


def _build_media_time_hint_text(query: str, fallback_year: str = "") -> str:
    hint = _extract_media_time_hint(query)
    if not hint:
        return ""
    year = str(hint.get("year") or fallback_year or "").strip()
    start_month = int(hint.get("start_month") or 0)
    end_month = int(hint.get("end_month") or 0)
    label = str(hint.get("label") or "").strip()
    if label in {"上半年", "下半年"}:
        return f"{year}年{label}" if year else label
    if start_month and end_month and start_month == end_month:
        return f"{year}年{start_month}月" if year else f"{start_month}月"
    if start_month and end_month:
        return f"{year}年{start_month}-{end_month}月" if year else f"{start_month}-{end_month}月"
    return f"{year}年{label}" if year and label else label


def _month_last_day(year: int, month: int) -> int:
    if month == 2:
        leap = year % 400 == 0 or (year % 4 == 0 and year % 100 != 0)
        return 29 if leap else 28
    if month in {4, 6, 9, 11}:
        return 30
    return 31


def _parse_media_date_window(query: str, fallback_year: str = "") -> dict[str, str]:
    text = str(query or "").strip()
    if not text:
        return {}

    today = date.today()
    compact = re.sub(r"\s+", "", text)
    if any(token in compact for token in ("过去半年", "近半年", "最近半年", "半年内", "近6个月", "最近6个月", "过去6个月", "6个月内")):
        start = today - timedelta(days=183)
        return {
            "start": start.isoformat(),
            "end": today.isoformat(),
            "kind": "past_6_months",
            "label": "最近半年",
        }
    if any(token in compact for token in ("过去一年", "近一年", "最近一年", "这一年")):
        start = today - timedelta(days=365)
        return {
            "start": start.isoformat(),
            "end": today.isoformat(),
            "kind": "past_1_year",
            "label": "最近一年",
        }
    if "过去两年" in compact or "近两年" in compact:
        start = today - timedelta(days=365 * 2)
        return {
            "start": start.isoformat(),
            "end": today.isoformat(),
            "kind": "past_2_years",
            "label": "最近两年",
        }
    if "去年" in compact:
        year = today.year - 1
        return {
            "start": f"{year:04d}-01-01",
            "end": f"{year:04d}-12-31",
            "kind": "calendar_year",
            "label": "去年",
        }
    if "今年" in compact:
        year = today.year
        return {
            "start": f"{year:04d}-01-01",
            "end": today.isoformat(),
            "kind": "calendar_year_to_date",
            "label": "今年",
        }

    hint = _extract_media_time_hint(text)
    if not hint:
        return {}

    year_text = str(hint.get("year") or fallback_year or "").strip()
    if not year_text.isdigit():
        return {}

    year = int(year_text)
    start_month = max(1, min(12, int(hint.get("start_month") or 0)))
    end_month = max(1, min(12, int(hint.get("end_month") or 0)))
    if not start_month or not end_month:
        return {}
    return {
        "start": f"{year:04d}-{start_month:02d}-01",
        "end": f"{year:04d}-{end_month:02d}-{_month_last_day(year, end_month):02d}",
        "kind": "explicit_range",
        "label": _build_media_time_hint_text(text, str(year)),
    }


def _looks_like_time_only_followup(query: str) -> bool:
    text = str(query or "").strip().strip("？?。！!，,；;：:")
    if not text:
        return False
    compact = re.sub(r"\s+", "", text)
    if re.fullmatch(r"(?:20\d{2}年?)?(?:上半年|下半年)(?:看了哪些)?(?:的)?(?:番|番剧|动画|动漫|新番)?(?:呢)?", compact):
        return True
    if re.fullmatch(r"(?:20\d{2}年?)?\d{1,2}(?:月)?(?:到|至|[-~—－])\d{1,2}月(?:看了哪些)?(?:的)?(?:番|番剧|动画|动漫|新番)?(?:呢)?", compact):
        return True
    return bool(re.fullmatch(r"(?:20\d{2}年?)?\d{1,2}月(?:看了哪些)?(?:的)?(?:番|番剧|动画|动漫|新番)?(?:呢)?", compact))


def _replace_time_window_in_query(previous_query: str, current_query: str) -> str:
    previous = str(previous_query or "").strip()
    current = str(current_query or "").strip().strip("？?。！!，,；;：:")
    if not previous or not current:
        return current or previous
    previous_year = _extract_year_from_date_range(_parse_media_date_window(previous))
    current_time_text = _build_media_time_hint_text(current, previous_year)
    replacement = current_time_text or current.replace("呢", "")
    replaced = re.sub(
        r"20\d{2}年\s*\d{1,2}(?:月)?\s*(?:到|至|[-~—－])\s*\d{1,2}月|20\d{2}年\s*\d{1,2}月|20\d{2}年\s*(?:上半年|下半年)",
        replacement,
        previous,
        count=1,
    )
    if replaced != previous:
        return replaced
    return f"{replacement} {previous}".strip()


def _state_has_media_context(state: dict[str, Any] | None) -> bool:
    current = state if isinstance(state, dict) else {}
    media_type = str(current.get("media_type") or "").strip()
    if media_type:
        return True

    filters = current.get("filters") if isinstance(current.get("filters"), dict) else {}
    meaningful_filter_keys = {
        "media_type",
        "category",
        "genre",
        "author",
        "authors",
        "director",
        "directors",
        "actor",
        "actors",
        "nationality",
        "platform",
        "series",
        "title",
        "tag",
        "tags",
    }
    for key, value in filters.items():
        if key in meaningful_filter_keys and value:
            return True

    entities = current.get("entities") if isinstance(current.get("entities"), list) else []
    if any(str(entity or "").strip() and not _looks_like_generic_media_scope(str(entity or "")) for entity in entities):
        return True

    entity = str(current.get("entity") or "").strip()
    if entity and not _looks_like_generic_media_scope(entity):
        return True

    return False


def _matches_media_date_window(item: dict[str, Any], date_window: dict[str, str] | None) -> bool:
    if not date_window:
        return True
    raw_date = str(item.get("date") or "").strip()
    start = str((date_window or {}).get("start") or "").strip()
    end = str((date_window or {}).get("end") or "").strip()
    if not raw_date or not start or not end:
        return False
    return start <= raw_date <= end


def _resolved_media_type_label(filters: dict[str, list[str]], resolved_question: str) -> str:
    return derive_resolved_media_type_label(filters, resolved_question=resolved_question)


def _build_resolved_query_state(
    original_question: str,
    resolved_question: str,
    query_classification: dict[str, Any],
) -> dict[str, Any]:
    current = query_classification if isinstance(query_classification, dict) else {}
    router_decision = current.get("router_decision") if isinstance(current.get("router_decision"), dict) else {}
    if router_decision:
        return _build_resolved_query_state_from_decision(
            _deserialize_router_decision(router_decision, fallback_question=resolved_question or original_question),
        )
    normalized_type = _normalize_query_type(current.get("query_type", QUERY_TYPE_GENERAL))
    force_media_state = bool(current.get("force_media_state"))
    carry_over = str(original_question or "").strip() != str(resolved_question or "").strip()
    inherited_kind = ""
    if carry_over and _looks_like_time_only_followup(original_question):
        inherited_kind = "time_window_replace"
    elif carry_over:
        inherited_kind = "followup_expand"

    if normalized_type not in {QUERY_TYPE_MEDIA, QUERY_TYPE_MIXED} and not force_media_state:
        return {
            "lookup_mode": "general_lookup",
            "selection": {},
            "time_constraint": {},
            "ranking": {"mode": "relevance", "source": "default"},
            "media_type": "",
            "filters": {},
            "date_range": [],
            "sort": "relevance",
            "carry_over_from_previous_turn": carry_over,
            "inherited_context": {
                "used": carry_over,
                "kind": inherited_kind,
            },
        }

    projected_filters = project_media_filters_to_library_schema(_infer_media_filters(resolved_question), resolved_question=resolved_question)
    filters = projected_filters.filters
    fallback_year = _extract_year_from_date_range(current.get("previous_date_range"))
    date_window = _parse_media_date_window(resolved_question, fallback_year)
    if _is_collection_media_query(resolved_question) or date_window:
        lookup_mode = "filter_search"
    elif bool(current.get("media_entity_confident")):
        lookup_mode = "entity_lookup"
    else:
        lookup_mode = "general_lookup"
    media_type = projected_filters.resolved_media_type or _resolved_media_type_label(filters, resolved_question)
    time_constraint = _build_time_constraint(date_window, source="compat")
    ranking = _build_media_ranking(resolved_question, lookup_mode, time_constraint)
    sort = str(ranking.get("mode") or "relevance")
    return {
        "lookup_mode": lookup_mode,
        "selection": _build_media_selection(filters, media_type),
        "time_constraint": time_constraint,
        "ranking": ranking,
        "media_type": media_type,
        "filters": filters,
        "date_range": [date_window.get("start", ""), date_window.get("end", "")] if date_window else [],
        "sort": sort,
        "carry_over_from_previous_turn": carry_over,
        "inherited_context": {
            "used": carry_over,
            "kind": inherited_kind,
            "filter_strategy": "carry" if carry_over else "none",
        },
    }


def _find_previous_user_question(current_question: str, history: list[dict[str, str]]) -> str:
    current = str(current_question or "").strip()
    for item in reversed(history or []):
        if not isinstance(item, dict):
            continue
        if str(item.get("role", "")).strip().lower() != "user":
            continue
        previous = str(item.get("content", "")).strip()
        if not previous or previous == current:
            continue
        return previous
    return ""


def _find_previous_assistant_message(history: list[dict[str, str]]) -> str:
    for item in reversed(history or []):
        if not isinstance(item, dict):
            continue
        if str(item.get("role", "")).strip().lower() != "assistant":
            continue
        text = str(item.get("content", "") or item.get("text", "")).strip()
        if text:
            return text
    return ""


def _infer_prior_question_state(question: str, history: list[dict[str, str]] | None = None) -> dict[str, Any]:
    text = str(question or "").strip()
    if not text:
        return {}
    query_profile = _resolve_query_profile(text)
    decision, llm_media, _ = _build_router_decision(text, [], {}, query_profile)
    query_classification = _router_decision_to_query_classification(decision, llm_media, {}, query_profile)
    resolved_state = _build_resolved_query_state(text, text, query_classification)
    snapshot = _build_conversation_state_snapshot(text, query_classification=query_classification, resolved_query_state=resolved_state)
    if _state_has_media_context(snapshot):
        return snapshot

    assistant_text = _find_previous_assistant_message(history or [])
    assistant_entities = [str(item).strip() for item in _extract_media_entities(assistant_text) if str(item).strip()]
    if _is_collection_media_query(text):
        snapshot["lookup_mode"] = "filter_search"
        snapshot["media_type"] = str(snapshot.get("media_type") or "video")
        snapshot["filters"] = _merge_router_filters(
            _normalize_media_filter_map(snapshot.get("filters")),
            {"series": [text]},
        )
        snapshot["selection"] = _build_media_selection(snapshot.get("filters") or {}, str(snapshot.get("media_type") or ""))
        snapshot["ranking"] = {"mode": str(snapshot.get("sort") or "relevance"), "source": "inferred"}
        snapshot["entity"] = ""
        snapshot["entities"] = []
    elif len(assistant_entities) >= 2:
        snapshot["lookup_mode"] = "filter_search"
        snapshot["media_type"] = str(snapshot.get("media_type") or "video")
        snapshot["filters"] = _merge_router_filters(
            _normalize_media_filter_map(snapshot.get("filters")),
            {"series": [text]},
        )
        snapshot["selection"] = _build_media_selection(snapshot.get("filters") or {}, str(snapshot.get("media_type") or ""))
        snapshot["ranking"] = {"mode": str(snapshot.get("sort") or "relevance"), "source": "inferred"}
        snapshot["entity"] = ""
        snapshot["entities"] = []
    elif len(assistant_entities) == 1:
        snapshot["lookup_mode"] = "entity_lookup"
        snapshot["entity"] = assistant_entities[0]
        snapshot["entities"] = [assistant_entities[0]]
    return snapshot


def _build_conversation_state_snapshot(
    question: str,
    query_classification: dict[str, Any] | None = None,
    resolved_query_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    text = str(question or "").strip()
    current = query_classification if isinstance(query_classification, dict) else {}
    state = dict(resolved_query_state) if isinstance(resolved_query_state, dict) else _build_resolved_query_state(
        text,
        text,
        current,
    )
    entities = [
        str(item).strip()
        for item in (current.get("media_entities") or [])
        if str(item).strip()
    ]
    if not entities and _state_has_media_context(state):
        entities = _extract_media_entities(text)
    return {
        "question": text,
        "lookup_mode": _get_lookup_mode_from_state(state),
        "selection": _normalize_media_filter_map(state.get("selection")),
        "time_constraint": dict(state.get("time_constraint") or {}),
        "ranking": dict(state.get("ranking") or {}),
        "media_type": str(state.get("media_type", "") or ""),
        "entity": entities[0] if entities else "",
        "entities": entities,
        "filters": _normalize_media_filter_map(state.get("filters")),
        "date_range": list(state.get("date_range") or []),
        "sort": str(state.get("sort", "") or ""),
    }


def _has_state_value(value: Any) -> bool:
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return value is not None


def _state_value_signature(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except TypeError:
        return repr(value)


def _describe_inheritance_transition(before_value: Any, after_value: Any, detected_followup: bool) -> str:
    if not detected_followup:
        return "not_applicable"
    before_present = _has_state_value(before_value)
    after_present = _has_state_value(after_value)
    if not before_present and not after_present:
        return "unchanged_empty"
    if _state_value_signature(before_value) == _state_value_signature(after_value):
        return "carried_over" if before_present else "unchanged_empty"
    if not before_present and after_present:
        return "introduced"
    if before_present and not after_present:
        return "cleared"
    return "overridden"


def _build_state_diff(before_state: dict[str, Any], after_state: dict[str, Any]) -> dict[str, Any]:
    diff: dict[str, Any] = {}
    for field in ("lookup_mode", "selection", "time_constraint", "ranking", "media_type", "entity", "filters", "date_range", "sort"):
        if _state_value_signature(before_state.get(field)) != _state_value_signature(after_state.get(field)):
            diff[field] = after_state.get(field)
    return diff


def _build_followup_transition(
    original_question: str,
    resolved_question: str,
    history: list[dict[str, str]],
    query_classification: dict[str, Any],
    resolved_query_state: dict[str, Any],
) -> dict[str, Any]:
    router_payload = query_classification.get("router_decision") if isinstance(query_classification.get("router_decision"), dict) else {}
    if router_payload:
        decision = _deserialize_router_decision(router_payload, fallback_question=original_question)
        resolution = _resolve_router_context(original_question, history, decision, _infer_prior_question_state(_find_previous_user_question(original_question, history), history), [])
        return {
            "conversation_state_before": dict(resolution.conversation_state_before),
            "detected_followup": bool(resolution.detected_followup),
            "inheritance_applied": dict(resolution.inheritance_applied),
            "conversation_state_after": dict(resolution.conversation_state_after),
            "state_diff": dict(resolution.state_diff),
        }
    previous_trace = _find_previous_trace_context(history)
    previous_trace_state = previous_trace.get("conversation_state_after") if isinstance(previous_trace.get("conversation_state_after"), dict) else {}
    previous_trace_resolved_question = ""
    if isinstance(previous_trace.get("query_understanding"), dict):
        previous_trace_resolved_question = str(previous_trace.get("query_understanding", {}).get("resolved_question", "") or "").strip()
    previous_question = _find_previous_user_question(original_question, history)
    conversation_state_before = dict(previous_trace_state) if previous_trace_state else (
        _build_conversation_state_snapshot(previous_question, resolved_query_state=_infer_prior_question_state(previous_question, history))
        if previous_question else {}
    )
    conversation_state_after = _build_conversation_state_snapshot(
        resolved_question,
        query_classification=query_classification,
        resolved_query_state=resolved_query_state,
    )
    has_previous_context = bool(previous_question) or bool(previous_trace_state) or bool(previous_trace_resolved_question)
    detected_followup = has_previous_context and (
        bool(resolved_query_state.get("carry_over_from_previous_turn"))
        or _is_context_dependent_followup(original_question)
    )
    inheritance_applied = {
        "lookup_mode": _describe_inheritance_transition(
            conversation_state_before.get("lookup_mode"),
            conversation_state_after.get("lookup_mode"),
            detected_followup,
        ),
        "media_type": _describe_inheritance_transition(
            conversation_state_before.get("media_type"),
            conversation_state_after.get("media_type"),
            detected_followup,
        ),
        "filters": _describe_inheritance_transition(
            conversation_state_before.get("filters"),
            conversation_state_after.get("filters"),
            detected_followup,
        ),
        "date_range": _describe_inheritance_transition(
            conversation_state_before.get("date_range"),
            conversation_state_after.get("date_range"),
            detected_followup,
        ),
        "sort": _describe_inheritance_transition(
            conversation_state_before.get("sort"),
            conversation_state_after.get("sort"),
            detected_followup,
        ),
        "entity": _describe_inheritance_transition(
            conversation_state_before.get("entity"),
            conversation_state_after.get("entity"),
            detected_followup,
        ),
    }
    return {
        "conversation_state_before": conversation_state_before,
        "detected_followup": detected_followup,
        "inheritance_applied": inheritance_applied,
        "conversation_state_after": conversation_state_after,
        "state_diff": _build_state_diff(conversation_state_before, conversation_state_after),
    }


def _build_planner_snapshot(
    resolved_question: str,
    query_classification: dict[str, Any],
    resolved_query_state: dict[str, Any],
    planned_tools: list[PlannedToolCall],
) -> dict[str, Any]:
    router_payload = query_classification.get("router_decision") if isinstance(query_classification.get("router_decision"), dict) else {}
    if router_payload:
        decision = _deserialize_router_decision(router_payload, fallback_question=resolved_question)
        return _build_planner_snapshot_from_decision(decision, resolved_question, resolved_query_state, planned_tools)
    state_after = _build_conversation_state_snapshot(
        resolved_question,
        query_classification=query_classification,
        resolved_query_state=resolved_query_state,
    )
    hard_filters: dict[str, Any] = {}
    media_type = str(state_after.get("media_type", "") or "")
    if media_type:
        hard_filters["media_type"] = media_type
    for field, values in (state_after.get("filters") or {}).items():
        if not isinstance(values, list):
            continue
        clean_values = [str(value).strip() for value in values if str(value).strip()]
        if not clean_values:
            continue
        if field == "media_type" and media_type:
            continue
        hard_filters[str(field)] = clean_values[0] if len(clean_values) == 1 else clean_values
    date_range = state_after.get("date_range") or []
    if date_range:
        hard_filters["date_range"] = date_range
    soft_constraints: dict[str, Any] = {}
    sort = str(state_after.get("sort", "") or "")
    if sort:
        soft_constraints["sort"] = sort
    ranking = state_after.get("ranking") if isinstance(state_after.get("ranking"), dict) else {}
    if ranking:
        soft_constraints["ranking"] = ranking
    entity = str(state_after.get("entity", "") or "")
    lookup_mode = _get_lookup_mode_from_state(state_after)
    query_text: str | None = entity or None
    if query_text is None and lookup_mode != "filter_search":
        query_text = resolved_question or None
    followup_mode = str((resolved_query_state.get("inherited_context") or {}).get("kind", "") or "none")
    return {
        "lookup_mode": lookup_mode,
        "selection": _normalize_media_filter_map(state_after.get("selection")),
        "time_constraint": dict(state_after.get("time_constraint") or {}),
        "hard_filters": hard_filters,
        "soft_constraints": soft_constraints,
        "query_text": query_text,
        "followup_mode": followup_mode,
        "planned_tools": [call.name for call in planned_tools],
    }


def _should_force_media_understanding(
    original_question: str,
    resolved_question: str,
    query_classification: dict[str, Any],
    previous_trace_state: dict[str, Any] | None,
) -> bool:
    router_decision = query_classification.get("router_decision") if isinstance(query_classification.get("router_decision"), dict) else {}
    if router_decision:
        return str(router_decision.get("domain") or "") == "media" and str(router_decision.get("followup_mode") or "none") != "none"
    normalized_type = _normalize_query_type(query_classification.get("query_type", QUERY_TYPE_GENERAL))
    return normalized_type in {QUERY_TYPE_MEDIA, QUERY_TYPE_MIXED}


def _build_media_tool_options_from_decision(decision: RouterDecision) -> dict[str, Any]:
    lookup_mode = str(decision.lookup_mode or "general_lookup")
    rewritten_queries = {str(key): str(value) for key, value in (decision.rewritten_queries or {}).items() if str(key).strip() and str(value).strip()}
    query_text: str | None = rewritten_queries.get("media_query") or (decision.entities[0] if lookup_mode == "entity_lookup" and decision.entities else None)
    if query_text is None and lookup_mode not in {"filter_search", "concept_lookup"}:
        query_text = decision.resolved_question or decision.raw_question or None
    evidence = dict(decision.evidence or {})
    music_hints = evidence.get("music_work_hints") if isinstance(evidence.get("music_work_hints"), dict) else {}
    composer_hints = [
        str(item).strip()
        for item in (music_hints.get("composer_hints") or [])
        if str(item).strip()
    ]
    work_signature = [
        str(item).strip()
        for item in (music_hints.get("work_signature") or [])
        if str(item).strip()
    ]
    instrument_hints = [
        str(item).strip()
        for item in (music_hints.get("instrument_hints") or [])
        if str(item).strip()
    ]
    form_hints = [
        str(item).strip()
        for item in (music_hints.get("form_hints") or [])
        if str(item).strip()
    ]
    work_family_hints = [
        str(item).strip()
        for item in (music_hints.get("work_family_hints") or [])
        if str(item).strip()
    ]
    normalized_selection = _normalize_media_filter_map(decision.selection)
    normalized_filters = _normalize_media_filter_map(decision.filters)
    query_class = str(decision.query_class or "knowledge_qa")
    # For composer-work version comparison in classical music, author should be
    # a soft retrieval hint (title matching), not a hard structured filter,
    # otherwise we can zero out true positives where author stores
    # performer/conductor instead of composer.
    if query_class == "music_work_versions_compare":
        for _field in ("author",):
            normalized_selection.pop(_field, None)
            normalized_filters.pop(_field, None)

    options = {
        "selection": normalized_selection,
        "time_constraint": dict(decision.time_constraint),
        "ranking": dict(decision.ranking),
        "filters": normalized_filters,
        "date_window": _date_window_from_state({"date_range": decision.date_range}),
        "sort": decision.sort,
        "lookup_mode": lookup_mode,
        "media_type": decision.media_type,
        "media_entities": list(decision.entities) if lookup_mode == "entity_lookup" else [],
        "allow_downstream_entity_inference": bool(decision.allow_downstream_entity_inference),
        "query_text": query_text,
        "rewritten_queries": rewritten_queries,
        "query_class": query_class,
        "composer_hints": composer_hints,
        "instrument_hints": instrument_hints,
        "form_hints": form_hints,
        "work_family_hints": work_family_hints,
        "work_signature": work_signature,
        # Semantic scope slots — drive date-field selection in _tool_query_media_record.
        "subject_scope": str(decision.subject_scope or "general_knowledge"),
        "time_scope_type": str(decision.time_scope_type or ""),
    }
    return {key: value for key, value in options.items() if value not in ({}, [], "", None)}


def _build_media_tool_options(
    question: str,
    resolved_query_state: dict[str, Any] | None,
    query_classification: dict[str, Any] | None,
) -> dict[str, Any]:
    state = resolved_query_state if isinstance(resolved_query_state, dict) else {}
    current = query_classification if isinstance(query_classification, dict) else {}
    router_decision = current.get("router_decision") if isinstance(current.get("router_decision"), dict) else {}
    if router_decision:
        return _build_media_tool_options_from_decision(
            _deserialize_router_decision(router_decision, fallback_question=question)
        )
    filters = _normalize_media_filter_map(state.get("filters"))
    selection = _normalize_media_filter_map(state.get("selection")) or _build_media_selection(filters, str(state.get("media_type") or ""))
    time_constraint = dict(state.get("time_constraint") or {})
    ranking = dict(state.get("ranking") or {})
    date_window = _date_window_from_state(state)
    lookup_mode = _get_lookup_mode_from_state(state)
    media_entities = [
        str(item).strip()
        for item in (current.get("media_entities") or [])
        if str(item).strip()
    ]
    query_text = str(current.get("media_entity") or "").strip() or None
    if query_text is None and lookup_mode != "filter_search":
        query_text = str(question or "").strip() or None
    options = {
        "selection": selection,
        "time_constraint": time_constraint,
        "ranking": ranking,
        "filters": filters,
        "date_window": date_window,
        "sort": str(state.get("sort", "relevance") or "relevance"),
        "lookup_mode": lookup_mode,
        "media_type": str(state.get("media_type", "") or ""),
        "media_entities": media_entities,
        "query_text": query_text,
    }
    return {key: value for key, value in options.items() if value not in ({}, [], "", None)}


# Suffixes that mark a query as a creator-collection lookup: "X的作品/电影/书/..."
_CREATOR_COLLECTION_SUFFIX_RE = re.compile(
    r"^(.{2,12})的(?:作品|所有作品|全部作品|一切作品|全集|书|全部书|所有书"
    r"|小说|全部小说|诗集|诗|散文|音乐|专辑|歌曲|全部专辑|电影|所有电影|全部电影"
    r"|游戏|剧集|影片|电视剧|动画|代表作|经典作品|经典著作|著作)",
    re.UNICODE,
)
# Alternative: "X写过什么", "X拍过哪些", "X创作了什么"
_CREATOR_VERB_RE = re.compile(
    r"^(.{2,12})(?:写过|写了|拍过|拍了|出版了|发行了|创作了|著有)",
    re.UNICODE,
)


def _extract_creator_from_collection_query(question: str) -> "Any | None":
    """Try to extract a creator name from queries like '加缪的作品有哪些'.

    Returns a CreatorResolution if a known creator is resolvable, else None.
    """
    text = str(question or "").strip()
    if not text or len(text) < 3:
        return None
    for pattern in (_CREATOR_COLLECTION_SUFFIX_RE, _CREATOR_VERB_RE):
        m = pattern.match(text)
        if m:
            candidate = m.group(1).strip()
            if candidate:
                res = er_resolve_creator(candidate, min_confidence=0.15)
                if res:
                    return res
    return None


def _is_collection_media_query(query: str) -> bool:
    text = str(query or "").strip()
    if not text:
        return False
    if _has_media_title_marker(text):
        return False
    lowered = text.lower()
    if any(cue in lowered for cue in ROUTER_COLLECTION_NEGATIVE_CUES) and not _has_router_media_surface(text):
        return False
    media_context = _has_router_media_surface(text)
    if not media_context:
        return False
    if any(cue in text for cue in ("三部曲", "系列", "几部")):
        return True
    return any(cue in text for cue in MEDIA_COLLECTION_CUES)


def _needs_filter_only_media_lookup(query: str, lookup_mode: str, media_entities: list[str], filters: dict[str, list[str]], date_window: dict[str, str]) -> bool:
    if str(lookup_mode or "") == "filter_search":
        return True
    if media_entities:
        return False
    if date_window:
        return True
    if _is_collection_media_query(query) and filters:
        return True
    return False


def _shape_media_row(row: dict[str, Any], *, score: float, retrieval_mode: str, retrieval_query: str, matched_entities: list[str]) -> dict[str, Any]:
    return {
        "id": row.get("id"),
        "title": row.get("title"),
        "media_type": row.get("media_type"),
        "category": row.get("category"),
        "author": row.get("author"),
        "publisher": row.get("publisher"),
        "channel": row.get("channel"),
        "rating": row.get("rating"),
        "date": row.get("date"),
        "score": round(score, 6),
        "semantic_score": 0.0,
        "keyword_score": round(_safe_score(row.get("score")), 6),
        "keyword_norm": 0.0,
        "graph_prior": 0.0,
        "graph_prior_raw": 0.0,
        "title_boost": 0.0,
        "url": row.get("url"),
        "retrieval_mode": retrieval_mode,
        "retrieval_query": retrieval_query,
        "matched_entities": matched_entities,
        "review": (str(row.get("review", ""))[:140] + "...") if str(row.get("review", "")) else "",
    }


def _split_agent_multi_tags(raw: Any) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    return [part.strip() for part in re.split(r"[;；，,、\n]+", text) if part.strip()]


def _validate_media_result_row(row: dict[str, Any], filters: dict[str, list[str]], date_window: dict[str, str]) -> list[str]:
    reasons: list[str] = []
    raw_date = str(row.get("date") or "").strip()
    selected_media_types = {str(value).strip().lower() for value in filters.get("media_type", []) if str(value).strip()}
    if selected_media_types:
        media_type = str(row.get("media_type") or "").strip().lower()
        if media_type not in selected_media_types:
            reasons.append("media_type_mismatch")

    selected_categories = {str(value).strip() for value in filters.get("category", []) if str(value).strip()}
    if selected_categories:
        category_tokens = set(_split_agent_multi_tags(row.get("category")))
        if not category_tokens or category_tokens.isdisjoint(selected_categories):
            reasons.append("category_mismatch")

    selected_years = {str(value).strip() for value in filters.get("year", []) if str(value).strip()}
    if selected_years:
        row_year = raw_date[:4]
        if not row_year:
            reasons.append("missing_release_date")
        elif row_year not in selected_years:
            reasons.append("year_mismatch")

    if date_window:
        if not raw_date:
            if "missing_release_date" not in reasons:
                reasons.append("missing_release_date")
        elif not _matches_media_date_window(row, date_window):
            reasons.append("date_out_of_range")
    return reasons


def _apply_media_result_validator(
    rows: list[dict[str, Any]],
    *,
    filters: dict[str, list[str]],
    date_window: dict[str, str],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    kept: list[dict[str, Any]] = []
    dropped_by_reason: dict[str, int] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        reasons = _validate_media_result_row(row, filters, date_window)
        if reasons:
            for reason in reasons:
                dropped_by_reason[reason] = dropped_by_reason.get(reason, 0) + 1
            continue
        kept.append(row)
    raw_count = len(rows)
    kept_count = len(kept)
    return kept, {
        "raw_candidates_count": raw_count,
        "post_filter_count": kept_count,
        "dropped_by_validator": max(0, raw_count - kept_count),
        "drop_reasons": dropped_by_reason,
    }


def _build_media_filter_queries(filters: dict[str, list[str]]) -> list[str]:
    queries: list[str] = []
    for field in ["nationality", "category", "author"]:
        for value in filters.get(field, []):
            clean = str(value).strip()
            if clean and clean not in queries:
                queries.append(clean)
    return queries


def _local_media_vocab_signature() -> tuple[tuple[str, int, int], ...]:
    structured_dir = _WORKSPACE_ROOT / "library_tracker" / "data" / "structured"
    entries: list[tuple[str, int, int]] = []
    try:
        for path in sorted(structured_dir.glob("*.json"), key=lambda item: item.name.lower()):
            try:
                stat = path.stat()
                entries.append((str(path), int(stat.st_mtime_ns), int(stat.st_size)))
            except Exception:
                entries.append((str(path), 0, 0))
    except Exception:
        return ((str(structured_dir / "reading.json"), 0, 0),)
    return tuple(entries) or ((str(structured_dir / "reading.json"), 0, 0),)


def _load_local_media_vocab() -> dict[str, list[str]]:
    signature = _local_media_vocab_signature()
    cached_signature = _MEDIA_LIBRARY_VOCAB_CACHE.get("signature")
    cached_data = _MEDIA_LIBRARY_VOCAB_CACHE.get("data") if isinstance(_MEDIA_LIBRARY_VOCAB_CACHE.get("data"), dict) else None
    if cached_signature == signature and cached_data:
        return cached_data

    structured_dir = _WORKSPACE_ROOT / "library_tracker" / "data" / "structured"
    nationalities: list[str] = []
    authors: list[str] = []
    categories: list[str] = []
    titles: list[str] = []
    seen_nat: set[str] = set()
    seen_author: set[str] = set()
    seen_category: set[str] = set()
    seen_title: set[str] = set()

    for path in sorted(structured_dir.glob("*.json"), key=lambda item: item.name.lower()):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            records = payload.get("records", []) if isinstance(payload, dict) else []
        except Exception:
            records = []

        for row in records:
            if not isinstance(row, dict):
                continue
            title = str(row.get("title") or "").strip()
            normalized_title = _normalize_media_title_for_match(title)
            if title and normalized_title and normalized_title not in seen_title and 1 < len(normalized_title) <= 48:
                seen_title.add(normalized_title)
                titles.append(title)
            nationality = str(row.get("nationality") or "").strip()
            if nationality and nationality not in seen_nat:
                seen_nat.add(nationality)
                nationalities.append(nationality)
            author = str(row.get("author") or "").strip()
            if author and author not in seen_author:
                seen_author.add(author)
                authors.append(author)
            for category in re.split(r"[;；，,、/]+", str(row.get("category") or "")):
                clean_category = category.strip()
                if clean_category and clean_category not in seen_category:
                    seen_category.add(clean_category)
                    categories.append(clean_category)

    data = {
        "nationalities": nationalities,
        "authors": authors,
        "categories": categories,
        "titles": titles,
    }
    _MEDIA_LIBRARY_VOCAB_CACHE["signature"] = signature
    _MEDIA_LIBRARY_VOCAB_CACHE["data"] = data
    return data


def _extract_media_entities_from_local_titles(query: str) -> list[str]:
    normalized_query = _normalize_media_title_for_match(query)
    if not normalized_query:
        return []
    vocab = _load_local_media_vocab()
    matched: list[str] = []
    for title in vocab.get("titles", []):
        clean_title = str(title).strip()
        normalized_title = _normalize_media_title_for_match(clean_title)
        if not normalized_title or len(normalized_title) < 2:
            continue
        if normalized_title in normalized_query:
            matched.append(clean_title)
    matched.sort(key=lambda item: len(_normalize_media_title_for_match(item)), reverse=True)
    dedup: list[str] = []
    seen: set[str] = set()
    for title in matched:
        key = _normalize_media_title_for_match(title)
        if key in seen:
            continue
        if any(key in _normalize_media_title_for_match(existing) for existing in dedup):
            continue
        seen.add(key)
        dedup.append(title)
        if len(dedup) >= 3:
            break
    return dedup


def _build_mediawiki_headers() -> dict[str, str]:
    headers = {
        "User-Agent": MEDIAWIKI_USER_AGENT,
        "Api-User-Agent": MEDIAWIKI_API_USER_AGENT,
    }
    return {key: value for key, value in headers.items() if str(value or "").strip()}


def _mediawiki_action_request(api_url: str, params: dict[str, Any], trace_id: str = "") -> dict[str, Any]:
    query_params = {
        "format": "json",
        "formatversion": 2,
        "utf8": 1,
        "errorformat": "plaintext",
        "maxlag": 5,
        **params,
    }
    if trace_id:
        query_params["requestid"] = trace_id
    url = f"{api_url}?{urlparse.urlencode(query_params, doseq=True)}"
    return _http_json("GET", url, timeout=MEDIAWIKI_TIMEOUT, headers=_build_mediawiki_headers())


def _clean_mediawiki_snippet(value: Any) -> str:
    text = str(value or "")
    text = re.sub(r"<style[\s\S]*?</style>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<script[\s\S]*?</script>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<!--([\s\S]*?)-->", " ", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _mediawiki_result_score(query: str, title: str, snippet: str) -> float:
    q = str(query or "").strip().casefold()
    t = str(title or "").strip().casefold()
    score = 0.0
    if q and t == q:
        score += 3.0
    elif q and q in t:
        score += 1.4
    elif t and q and t.replace(" ", "") == q.replace(" ", ""):
        score += 2.0
    if q and q in str(snippet or "").casefold():
        score += 0.5
    return score


def _mediawiki_page_url(api_url: str, title: str) -> str:
    parsed = urlparse.urlparse(api_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    return f"{base}/wiki/{urlparse.quote(str(title or '').replace(' ', '_'))}"


def _build_mediawiki_concept_queries(query: str) -> list[str]:
    base = _strip_query_scaffolding(query)
    candidates: list[str] = []
    for item in [base, str(query or "").strip()]:
        clean = str(item or "").strip()
        if clean and clean not in candidates:
            candidates.append(clean)
        for alias in MEDIAWIKI_QUERY_ALIASES.get(clean, []):
            if alias and alias not in candidates:
                candidates.append(alias)
    for key, aliases in MEDIAWIKI_QUERY_ALIASES.items():
        if key in base or key in str(query or ""):
            for alias in [key, *aliases]:
                if alias and alias not in candidates:
                    candidates.append(alias)
    return candidates[:6]


def _strip_tmdb_query_scaffolding(query: str) -> str:
    text = str(query or "").strip()
    if not text:
        return ""
    text = re.sub(r"^(我(?:最近|刚刚|想|想要)?(?:看过|看了|在看|想看|想查|想找)?|帮我|请问|查一下|搜一下)", " ", text, flags=re.IGNORECASE)
    text = re.sub(
        r"(?:是谁演的|是谁导演的|讲了什么|讲什么|有哪些|有什么|资料|信息|介绍|简介|剧情简介|评价|评分|票房|上映时间|director|cast|actor|actress|writer)$",
        " ",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"(?:这部|这个|这本|这套)?(?:电影|影片|片子|电视剧|剧集|剧|动漫|动画|番剧|漫画|小说|书)?呢$", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text)
    return text.strip(" ，,。！？?；;：:")


def _is_context_dependent_followup(question: str) -> bool:
    text = str(question or "").strip()
    if not text:
        return False
    if _has_explicit_fresh_media_scope(text):
        return False
    if not _looks_like_time_only_followup(text) and (_extract_media_entities(text) or _has_media_title_marker(text)):
        return False
    reference_scope_cues = (
        "这些",
        "这些媒体",
        "这些作品",
        "这些条目",
        "这几部",
        "这几本",
        "这几条",
        "这些番",
        "它们",
        "前面这些",
        "上面这些",
        "刚才这些",
    )
    detail_cues = (
        "具体细节",
        "细节信息",
        "详细信息",
        "详细资料",
        "作者",
        "出版方",
        "出版社",
        "发行方",
        "发行商",
        "渠道",
        "平台",
        "工作室",
        "厂牌",
        "制作公司",
    )
    has_reference_scope = any(cue in text for cue in reference_scope_cues)
    has_detail_request = any(cue in text for cue in detail_cues)
    if len(text) > 20 and not text.endswith("呢") and not (has_reference_scope or has_detail_request):
        return False
    return any(
        cue in text
        for cue in (
            "呢",
            "简介",
            "剧情",
            "介绍",
            "讲了什么",
            "评价",
            "评分",
            *reference_scope_cues,
            *detail_cues,
            "导演",
            "演员",
            "结局",
            "时间",
            "什么时候",
            "优缺点",
            "区别",
            "差异",
            "为什么",
            "展开",
            "详细",
        )
    )


def _resolve_contextual_question(question: str, history: list[dict[str, str]]) -> str:
    return str(question or "").strip()


def _guess_tmdb_search_path(query: str) -> str:
    text = str(query or "")
    has_person = any(cue in text for cue in TMDB_PERSON_CUES)
    has_movie = any(cue in text for cue in TMDB_MOVIE_CUES)
    has_tv = any(cue in text for cue in TMDB_TV_CUES)
    if has_movie and not has_tv and not has_person:
        return "search/movie"
    if has_tv and not has_movie and not has_person:
        return "search/tv"
    if has_person and text.startswith(TMDB_PERSON_CUES) and not has_movie and not has_tv:
        return "search/person"
    return "search/multi"


def _tmdb_media_url(media_type: str, item_id: Any) -> str:
    clean_type = str(media_type or "").strip().lower()
    clean_id = str(item_id or "").strip()
    if not clean_id:
        return ""
    if clean_type not in {"movie", "tv", "person"}:
        clean_type = "movie"
    return f"https://www.themoviedb.org/{clean_type}/{urlparse.quote(clean_id)}"


def _tmdb_request(path: str, params: dict[str, Any], trace_id: str = "") -> dict[str, Any]:
    if not TMDB_API_KEY and not TMDB_READ_ACCESS_TOKEN:
        raise RuntimeError("未配置 TMDB API 凭证")
    query_params = dict(params)
    if TMDB_API_KEY:
        query_params.setdefault("api_key", TMDB_API_KEY)
    url = f"{TMDB_API_BASE_URL}/{path.lstrip('/')}?{urlparse.urlencode(query_params, doseq=True)}"
    headers = _build_tmdb_headers()
    if trace_id:
        headers["X-Trace-Id"] = trace_id
    return _http_json("GET", url, timeout=TMDB_TIMEOUT, headers=headers)


def _tmdb_result_score(query: str, row: dict[str, Any]) -> float:
    title = str(row.get("title") or row.get("name") or "").strip()
    q = _strip_tmdb_query_scaffolding(query).casefold()
    t = title.casefold()
    score = float(row.get("popularity", 0.0) or 0.0) * 0.01
    vote_average = float(row.get("vote_average", 0.0) or 0.0)
    score += vote_average * 0.1
    if q and t == q:
        score += 2.5
    elif q and q in t:
        score += 1.0
    return round(score, 6)


def _tool_search_tmdb_media(query: str, trace_id: str = "", *, limit: int = 5) -> ToolExecution:
    if not TMDB_API_KEY and not TMDB_READ_ACCESS_TOKEN:
        return ToolExecution(
            tool=TOOL_SEARCH_TMDB,
            status="empty",
            summary="未配置 TMDB API 凭证",
            data={"trace_id": trace_id, "trace_stage": "agent.tool.search_tmdb_media", "results": []},
        )
    search_path = _guess_tmdb_search_path(query)
    media_entities = _extract_media_entities(query)
    lookup = str(media_entities[0] or "").strip() if media_entities else ""
    if not lookup:
        lookup = _strip_tmdb_query_scaffolding(query) or str(query or "").strip()
    payload = _tmdb_request(
        search_path,
        {
            "query": lookup,
            "language": TMDB_LANGUAGE,
            "include_adult": "false",
            "page": 1,
        },
        trace_id=trace_id,
    )
    rows = payload.get("results", []) if isinstance(payload, dict) else []
    compact: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        media_type = str(row.get("media_type") or "").strip().lower()
        if search_path == "search/movie":
            media_type = "movie"
        elif search_path == "search/tv":
            media_type = "tv"
        elif search_path == "search/person":
            media_type = "person"
        if media_type not in {"movie", "tv", "person"}:
            continue
        item_id = row.get("id")
        title = str(row.get("title") or row.get("name") or "").strip()
        if not title or item_id in {None, ""}:
            continue
        compact.append(
            {
                "id": item_id,
                "title": title,
                "media_type": media_type,
                "date": str(row.get("release_date") or row.get("first_air_date") or "").strip(),
                "overview": _clip_text(row.get("overview", ""), 320),
                "original_language": str(row.get("original_language") or "").strip(),
                "vote_average": row.get("vote_average"),
                "popularity": row.get("popularity"),
                "known_for_department": str(row.get("known_for_department") or "").strip(),
                "url": _tmdb_media_url(media_type, item_id),
                "score": _tmdb_result_score(lookup, row),
                "source": "tmdb",
            }
        )
    compact.sort(key=lambda item: _safe_score(item.get("score")), reverse=True)
    compact = compact[: max(1, int(limit))]
    return ToolExecution(
        tool=TOOL_SEARCH_TMDB,
        status="ok" if compact else "empty",
        summary=f"TMDB 命中 {len(compact)} 条结果（endpoint={search_path}）",
        data={
            "trace_id": trace_id,
            "trace_stage": "agent.tool.search_tmdb_media",
            "query": lookup,
            "endpoint": search_path,
            "results": compact,
        },
    )


def _tool_search_by_creator(
    creator_name: str,
    trace_id: str = "",
    *,
    media_type: str = "",
    max_results: int = 30,
) -> ToolExecution:
    """Structured lookup of all library works by a given creator via entity resolution.

    Unlike query_media_record (which runs a keyword search), this tool uses the
    entity resolver's creator index for an exact-match lookup.  It is the primary
    tool for ``media_creator_collection`` queries such as "加缪的作品有哪些".
    """
    creator_name = str(creator_name or "").strip()
    if not creator_name:
        return ToolExecution(
            tool=TOOL_SEARCH_BY_CREATOR,
            status="empty",
            summary="creator_name 为空，跳过 search_by_creator",
            data={"trace_id": trace_id, "trace_stage": "agent.tool.search_by_creator", "results": [], "found": False},
        )
    creator_res = er_resolve_creator(creator_name, min_confidence=0.35)
    if creator_res is None:
        return ToolExecution(
            tool=TOOL_SEARCH_BY_CREATOR,
            status="empty",
            summary=f"本地图书馆中未找到创作者「{creator_name}」的条目",
            data={
                "trace_id": trace_id,
                "trace_stage": "agent.tool.search_by_creator",
                "results": [],
                "found": False,
                "creator_name": creator_name,
            },
        )
    works = list(creator_res.works)
    if media_type:
        filtered = [w for w in works if w.media_type == media_type]
        if filtered:
            works = filtered
    works = works[:max(1, int(max_results))]
    result_rows = [
        {
            "title": w.canonical,
            "media_type": w.media_type,
            "category": w.category,
            "date": w.date,
            "rating": w.rating,
            "review": _clip_text(str(w.review or ""), 200),
            "source": "local_creator_index",
        }
        for w in works
    ]
    return ToolExecution(
        tool=TOOL_SEARCH_BY_CREATOR,
        status="ok" if result_rows else "empty",
        summary=(
            f"本地创作者检索：「{creator_res.canonical}」"
            f"（match_kind={creator_res.match_kind}, confidence={round(creator_res.confidence, 3)}），"
            f"共 {len(creator_res.works)} 部作品，返回 {len(result_rows)} 条"
        ),
        data={
            "trace_id": trace_id,
            "trace_stage": "agent.tool.search_by_creator",
            "found": True,
            "canonical_creator": creator_res.canonical,
            "match_kind": creator_res.match_kind,
            "confidence": round(creator_res.confidence, 3),
            "media_type_hint": creator_res.media_type_hint,
            "works_count": len(creator_res.works),
            "results": result_rows,
        },
    )


def _match_local_terms(haystacks: list[str], vocabulary: list[str], limit: int = 12) -> list[str]:
    matched: list[str] = []
    plain_haystacks = [str(item or "") for item in haystacks if str(item or "").strip()]
    for value in vocabulary:
        clean = str(value or "").strip()
        if not clean:
            continue
        if any(clean in haystack for haystack in plain_haystacks):
            matched.append(clean)
        if len(matched) >= limit:
            break
    return matched


def _concept_cache_key(query: str) -> str:
    return _strip_query_scaffolding(query).casefold() or str(query or "").strip().casefold()


def _get_cached_mediawiki_concept(query: str) -> dict[str, Any] | None:
    key = _concept_cache_key(query)
    with _MEDIAWIKI_CONCEPT_CACHE["lock"]:
        cached = _MEDIAWIKI_CONCEPT_CACHE["entries"].get(key)
        return dict(cached) if isinstance(cached, dict) else None


def _set_cached_mediawiki_concept(query: str, data: dict[str, Any]) -> None:
    key = _concept_cache_key(query)
    with _MEDIAWIKI_CONCEPT_CACHE["lock"]:
        _MEDIAWIKI_CONCEPT_CACHE["entries"][key] = dict(data)


def _tool_search_mediawiki_action(query: str, trace_id: str = "", *, limit: int = 5, languages: list[str] | None = None) -> ToolExecution:
    concept_queries = _build_mediawiki_concept_queries(query)
    target_languages = languages or ["zh", "en"]
    api_map = {"zh": MEDIAWIKI_ZH_API, "en": MEDIAWIKI_EN_API}
    results: list[dict[str, Any]] = []

    for lang in target_languages:
        api_url = api_map.get(lang)
        if not api_url:
            continue
        for candidate in concept_queries:
            payload = _mediawiki_action_request(
                api_url,
                {
                    "action": "query",
                    "list": "search",
                    "srsearch": candidate,
                    "srlimit": min(max(1, int(limit)), 10),
                    "srinfo": "totalhits|suggestion|rewrittenquery",
                    "srprop": "snippet|titlesnippet|sectiontitle|wordcount|timestamp",
                    "srenablerewrites": 1,
                },
                trace_id=trace_id,
            )
            query_data = payload.get("query", {}) if isinstance(payload.get("query"), dict) else {}
            rows = query_data.get("search", []) if isinstance(query_data.get("search"), list) else []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                title = str(row.get("title") or "").strip()
                snippet = _clean_mediawiki_snippet(row.get("snippet"))
                results.append(
                    {
                        "title": title,
                        "snippet": snippet,
                        "language": lang,
                        "source": "mediawiki_action_query",
                        "query": candidate,
                        "wordcount": row.get("wordcount"),
                        "timestamp": row.get("timestamp"),
                        "url": _mediawiki_page_url(api_url, title) if title else "",
                        "score": round(_mediawiki_result_score(candidate, title, snippet), 6),
                    }
                )
            if rows:
                break

    dedup: dict[tuple[str, str], dict[str, Any]] = {}
    for row in results:
        key = (str(row.get("language") or ""), str(row.get("title") or ""))
        current = dedup.get(key)
        if current is None or _safe_score(row.get("score")) > _safe_score(current.get("score")):
            dedup[key] = row
    compact = sorted(dedup.values(), key=lambda item: _safe_score(item.get("score")), reverse=True)[:limit]
    status = "ok" if compact else "empty"
    return ToolExecution(
        tool=TOOL_SEARCH_MEDIAWIKI,
        status=status,
        summary=f"MediaWiki 搜索命中 {len(compact)} 条",
        data={
            "trace_id": trace_id,
            "trace_stage": "agent.tool.search_mediawiki_action",
            "results": compact,
            "queries": concept_queries,
        },
    )


def _tool_parse_mediawiki_page(query: str, trace_id: str = "", *, preferred_language: str = "") -> ToolExecution:
    language_order = [preferred_language] if preferred_language else []
    for lang in ["zh", "en"]:
        if lang not in language_order:
            language_order.append(lang)
    rows: list[dict[str, Any]] = []
    payload: dict[str, Any] = {}
    title = ""
    lang = language_order[0] if language_order else "zh"
    api_url = MEDIAWIKI_ZH_API if lang == "zh" else MEDIAWIKI_EN_API
    score_value = 0.0

    for candidate in _build_mediawiki_concept_queries(query):
        for lang in language_order:
            api_url = MEDIAWIKI_ZH_API if lang == "zh" else MEDIAWIKI_EN_API
            try:
                direct_payload = _mediawiki_action_request(
                    api_url,
                    {
                        "action": "parse",
                        "page": candidate,
                        "prop": "text|links|categories|displaytitle|revid|iwlinks",
                        "redirects": 1,
                        "section": 0,
                        "disableeditsection": 1,
                        "disabletoc": 1,
                    },
                    trace_id=trace_id,
                )
            except Exception:
                continue
            parsed = direct_payload.get("parse", {}) if isinstance(direct_payload.get("parse"), dict) else {}
            direct_title = str(parsed.get("title") or "").strip()
            if direct_title:
                payload = direct_payload
                title = direct_title
                rows = [{"title": direct_title, "language": lang, "score": 1.0, "url": _mediawiki_page_url(api_url, direct_title)}]
                score_value = 1.0
                break
        if title:
            break

    if not title:
        search_result = _tool_search_mediawiki_action(query, trace_id, limit=3, languages=language_order)
        rows = search_result.data.get("results", []) if isinstance(search_result.data, dict) else []
        if not rows:
            return ToolExecution(
                tool=TOOL_PARSE_MEDIAWIKI,
                status="empty",
                summary="MediaWiki 页面解析未找到候选条目",
                data={"trace_id": trace_id, "trace_stage": "agent.tool.parse_mediawiki_page", "results": []},
            )
        target = rows[0]
        lang = str(target.get("language") or "zh")
        api_url = MEDIAWIKI_ZH_API if lang == "zh" else MEDIAWIKI_EN_API
        title = str(target.get("title") or "").strip()
        score_value = _safe_score(target.get("score"))
        payload = _mediawiki_action_request(
            api_url,
            {
                "action": "parse",
                "page": title,
                "prop": "text|links|categories|displaytitle|revid|iwlinks",
                "redirects": 1,
                "section": 0,
                "disableeditsection": 1,
                "disabletoc": 1,
            },
            trace_id=trace_id,
        )

    parsed = payload.get("parse", {}) if isinstance(payload.get("parse"), dict) else {}
    html_text = ""
    if isinstance(parsed.get("text"), str):
        html_text = parsed.get("text") or ""
    elif isinstance(parsed.get("text"), dict):
        html_text = str(parsed.get("text", {}).get("*", "") or "")
    plain_text = _clean_mediawiki_snippet(html_text)
    links = []
    for row in parsed.get("links", []) if isinstance(parsed.get("links"), list) else []:
        if not isinstance(row, dict):
            continue
        link_title = str(row.get("title") or "").strip()
        if link_title:
            links.append(link_title)
    categories = []
    for row in parsed.get("categories", []) if isinstance(parsed.get("categories"), list) else []:
        if not isinstance(row, dict):
            continue
        category = str(row.get("category") or row.get("*") or "").strip()
        if category:
            categories.append(category)
    page = {
        "title": title,
        "display_title": _clean_mediawiki_snippet(parsed.get("displaytitle") or title) or title,
        "language": lang,
        "url": _mediawiki_page_url(api_url, title),
        "extract": plain_text[:2400],
        "links": links[:80],
        "categories": categories[:40],
        "source": "mediawiki_action_parse",
        "score": score_value,
    }
    return ToolExecution(
        tool=TOOL_PARSE_MEDIAWIKI,
        status="ok",
        summary=f"MediaWiki 页面解析成功: {title}",
        data={
            "trace_id": trace_id,
            "trace_stage": "agent.tool.parse_mediawiki_page",
            "results": [page],
            "page": page,
            "search_results": rows,
        },
    )


def _tool_expand_mediawiki_concept(query: str, trace_id: str = "") -> ToolExecution:
    cached = _get_cached_mediawiki_concept(query)
    if cached is not None:
        return ToolExecution(
            tool=TOOL_EXPAND_MEDIAWIKI_CONCEPT,
            status="ok",
            summary=f"MediaWiki 概念展开缓存命中: {cached.get('concept', _strip_query_scaffolding(query))}",
            data=cached,
        )
    if not _is_abstract_media_concept_query(query):
        # Also allow creator names to proceed (so we can fetch their Wikipedia page
        # as an external bibliographic reference for creator collection queries).
        _mw_creator_hint = er_resolve_creator(query, min_confidence=0.5)
        if not _mw_creator_hint:
            data = {
                "trace_id": trace_id,
                "trace_stage": "agent.tool.expand_mediawiki_concept",
                "results": [],
                "concept": _strip_query_scaffolding(query) or str(query or "").strip(),
                "filters": {},
                "authors": [],
                "aliases": [],
            }
            return ToolExecution(tool=TOOL_EXPAND_MEDIAWIKI_CONCEPT, status="skipped", summary="当前问题不需要外部概念展开", data=data)

    search_tool = _tool_search_mediawiki_action(query, trace_id, limit=4)
    search_rows = search_tool.data.get("results", []) if isinstance(search_tool.data, dict) else []
    pages: list[dict[str, Any]] = []
    seen_pages: set[tuple[str, str]] = set()
    for row in search_rows[:4]:
        title = str(row.get("title") or "").strip()
        language = str(row.get("language") or "").strip()
        key = (language, title)
        if not title or key in seen_pages:
            continue
        seen_pages.add(key)
        parsed = _tool_parse_mediawiki_page(title, trace_id, preferred_language=language)
        if parsed.status == "ok" and isinstance(parsed.data, dict):
            page = parsed.data.get("page")
            if isinstance(page, dict):
                pages.append(page)
        if len(pages) >= 2:
            break

    vocab = _load_local_media_vocab()
    haystacks: list[str] = []
    aliases: list[str] = []
    result_rows: list[dict[str, Any]] = []
    for page in pages:
        haystacks.append(str(page.get("display_title") or ""))
        haystacks.append(str(page.get("title") or ""))
        haystacks.append(str(page.get("extract") or ""))
        haystacks.extend([str(value) for value in page.get("links", []) if str(value).strip()])
        haystacks.extend([str(value) for value in page.get("categories", []) if str(value).strip()])
        for alias in [str(page.get("display_title") or "").strip(), str(page.get("title") or "").strip()]:
            if alias and alias not in aliases:
                aliases.append(alias)
        result_rows.append(page)
    for row in search_rows:
        title = str(row.get("title") or "").strip()
        if title and title not in aliases:
            aliases.append(title)

    matched_countries = _match_local_terms(haystacks, vocab.get("nationalities", []), limit=12)
    matched_authors = _match_local_terms(haystacks, vocab.get("authors", []), limit=12)
    matched_categories = _match_local_terms(haystacks, vocab.get("categories", []), limit=8)

    filters: dict[str, list[str]] = {}
    _merge_filter_values(filters, "nationality", matched_countries)
    _merge_filter_values(filters, "category", matched_categories)
    if any(keyword in str(query or "") for keyword in MEDIA_BOOKISH_CUES):
        _merge_filter_values(filters, "media_type", ["book"])

    concept = _strip_query_scaffolding(query) or str(query or "").strip()
    data = {
        "trace_id": trace_id,
        "trace_stage": "agent.tool.expand_mediawiki_concept",
        "results": result_rows,
        "concept": concept,
        "aliases": aliases[:10],
        "countries": matched_countries,
        "authors": matched_authors,
        "categories": matched_categories,
        "filters": filters,
        "search_results": search_rows,
    }
    _set_cached_mediawiki_concept(query, data)
    return ToolExecution(
        tool=TOOL_EXPAND_MEDIAWIKI_CONCEPT,
        status="ok" if result_rows or matched_countries or matched_authors else "empty",
        summary=f"MediaWiki 概念展开完成（countries={len(matched_countries)}, authors={len(matched_authors)}, aliases={len(data['aliases'])}）",
        data=data,
    )


def _is_compact_media_entity_list(entities: list[str]) -> bool:
    clean_entities = [str(entity or "").strip() for entity in entities if str(entity or "").strip()]
    if not clean_entities:
        return False
    if len(clean_entities) > 2:
        return False
    return all(len(entity) <= 24 for entity in clean_entities)


def _extract_json_object_from_text(raw: str) -> dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        return {}
    candidates = [text]
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        candidates.append(text[start : end + 1])
    for candidate in candidates:
        try:
            payload = json.loads(candidate)
        except Exception:
            continue
        if isinstance(payload, dict):
            return payload
    return {}


def _coerce_string_list(value: Any) -> list[str]:
    if isinstance(value, str):
        raw_items = re.split(r"[\n,，;；、]+", value)
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


def _coerce_filter_map(value: Any) -> dict[str, list[str]]:
    if not isinstance(value, dict):
        return {}
    normalized: dict[str, list[str]] = {}
    ranking_like_fields = {"rating", "score", "review", "sort", "ranking"}
    for key, raw in value.items():
        field = str(key or "").strip()
        if not field:
            continue
        if field.lower() in ranking_like_fields:
            continue
        values = _coerce_string_list(raw)
        if values:
            normalized[field] = values
    return _normalize_media_filter_map(normalized)


def _get_previous_assistant_answer_summary(history: list[dict[str, str]]) -> str:
    for item in reversed(history or []):
        if not isinstance(item, dict):
            continue
        if str(item.get("role", "")).strip().lower() != "assistant":
            continue
        content = str(item.get("content", "") or "").strip()
        if content:
            return _clip_text(content, 280)
    return ""


def _media_scope_label(media_type: str, filters: dict[str, list[str]]) -> str:
    normalized_media_type = str(media_type or "").strip().lower()
    categories = [str(value).strip() for value in filters.get("category", []) if str(value).strip()]
    if normalized_media_type == "game":
        return "游戏"
    if normalized_media_type == "book":
        return ((" / ".join(categories) + "图书") if categories else "图书")
    if normalized_media_type == "music":
        return "音乐"
    if normalized_media_type == "anime":
        return "动画"
    if normalized_media_type == "movie":
        return "电影"
    if normalized_media_type in {"tv", "series"}:
        return "剧集"
    if normalized_media_type == "video":
        return " / ".join(categories) if categories else "视频"
    if categories:
        return " / ".join(categories)
    return "媒体条目"


def _build_media_followup_rewrite_queries(
    question: str,
    previous_state: dict[str, Any],
    *,
    followup_mode: str,
    entities: list[str],
    filters: dict[str, list[str]],
    media_type: str,
) -> dict[str, str]:
    raw_question = str(question or "").strip()
    if not raw_question:
        return {}
    entity = entities[0] if entities else str(previous_state.get("entity") or "").strip()
    effective_filters = merge_followup_filters(
        _normalize_media_filter_map(previous_state.get("filters")),
        filters,
        strategy="carry",
    )
    scope_label = _media_scope_label(media_type or str(previous_state.get("media_type") or ""), effective_filters)
    followup_target = entity or scope_label
    wants_review = _question_requests_personal_evaluation(raw_question)
    wants_detail = _question_requests_media_details(raw_question)
    ranking_mode = _infer_requested_sort(raw_question)

    media_query = ""
    if entity and (wants_review or wants_detail or followup_mode == "inherit_entity"):
        if wants_review and wants_detail:
            media_query = f"{entity} 个人评分、短评和条目细节"
        elif wants_review:
            media_query = f"{entity} 个人评分与短评"
        elif wants_detail:
            media_query = f"{entity} 条目细节"
        elif _is_short_followup_surface(raw_question) or any(cue in raw_question for cue in ("怎么样", "如何")):
            media_query = f"{entity} 条目细节与个人评价"
        else:
            media_query = f"{entity} {raw_question}".strip()
    elif followup_mode in {"inherit_filters", "inherit_timerange"} and _state_has_media_context(previous_state):
        if ranking_mode == "rating_asc":
            media_query = f"我记录过的{followup_target}里评分最低的项目"
        elif ranking_mode == "rating_desc":
            media_query = f"我记录过的{followup_target}里评分最高的项目"
        elif ranking_mode == "date_asc":
            media_query = f"我记录过的{followup_target}里时间最早的项目"
        elif ranking_mode == "date_desc":
            media_query = f"我记录过的{followup_target}里时间最近的项目"
        elif wants_review:
            media_query = f"我记录过的{followup_target}的评分与短评"
        elif wants_detail:
            media_query = f"我记录过的{followup_target}的条目细节"
        elif _is_context_dependent_followup(raw_question):
            media_query = f"延续上一轮范围的{followup_target}：{raw_question}".strip()
    elif entity:
        media_query = f"{entity} {raw_question}".strip()

    if not media_query:
        media_query = raw_question

    doc_query = media_query if followup_mode in {"inherit_filters", "inherit_timerange"} else raw_question
    tmdb_query = entity or media_query
    return {
        "media_query": media_query,
        "doc_query": doc_query,
        "tmdb_query": tmdb_query,
        "web_query": media_query,
    }


def _media_graph_degrees() -> dict[str, int]:
    graph_path = _WORKSPACE_ROOT / "library_tracker" / "data" / "vector_db" / "library_knowledge_graph.json"
    if not graph_path.exists():
        return {}
    try:
        mtime = graph_path.stat().st_mtime
    except Exception:
        return {}

    cached_mtime = _MEDIA_GRAPH_CACHE.get("mtime")
    cached_degrees = _MEDIA_GRAPH_CACHE.get("degrees") if isinstance(_MEDIA_GRAPH_CACHE.get("degrees"), dict) else {}
    if cached_mtime == mtime and cached_degrees:
        return cached_degrees

    try:
        payload = json.loads(graph_path.read_text(encoding="utf-8"))
    except Exception:
        return {}

    edges = payload.get("edges", []) if isinstance(payload, dict) and isinstance(payload.get("edges"), list) else []
    degrees: dict[str, int] = {}
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        src = str(edge.get("src") or "").strip()
        dst = str(edge.get("dst") or "").strip()
        if src.startswith("item:"):
            item_id = src.split(":", 1)[1]
            degrees[item_id] = degrees.get(item_id, 0) + 1
        if dst.startswith("item:"):
            item_id = dst.split(":", 1)[1]
            degrees[item_id] = degrees.get(item_id, 0) + 1

    _MEDIA_GRAPH_CACHE["mtime"] = mtime
    _MEDIA_GRAPH_CACHE["degrees"] = degrees
    return degrees


def _classify_media_query_with_llm(
    query: str,
    quota_state: dict[str, Any],
    *,
    previous_state: dict[str, Any] | None = None,
    history: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    previous = dict(previous_state or {})
    prior_scope = {
        "lookup_mode": str(previous.get("lookup_mode") or ""),
        "media_type": str(previous.get("media_type") or ""),
        "entity": str(previous.get("entity") or ""),
        "entities": [str(item).strip() for item in (previous.get("entities") or []) if str(item).strip()],
        "filters": _normalize_media_filter_map(previous.get("filters")),
        "time_constraint": dict(previous.get("time_constraint") or {}),
        "ranking": dict(previous.get("ranking") or {}),
    }
    previous_answer_summary = _get_previous_assistant_answer_summary(history or [])
    prompt = (
        "You are a query understanding planner for a personal knowledge + media agent.\n"
        "Return JSON only.\n"
        "Fields: label, domain, lookup_mode, entities, filters, time_window, ranking, followup_target, needs_comparison, needs_explanation, confidence.\n"
        "label must be one of MEDIA, TECH, OTHER.\n"
        "lookup_mode must be one of general_lookup, entity_lookup, filter_search, concept_lookup.\n"
        "Do not generate rewrites or natural-language restatements. Focus only on structured understanding.\n"
        "If the user asks for evaluation or ranking, prefer local personal-review phrasing.\n\n"
        f"Previous structured state:\n{json.dumps(prior_scope, ensure_ascii=False)}\n\n"
        f"Previous assistant answer summary:\n{previous_answer_summary or 'N/A'}\n\n"
        f"Current question:\n{query}"
    )
    try:
        raw = _llm_chat(
            messages=[{"role": "user", "content": prompt}],
            backend="local",
            quota_state=quota_state,
            count_quota=False,
        )
        payload = _extract_json_object_from_text(raw)
        parsed_label = _parse_classifier_label(str(payload.get("label") or raw or ""))
        rewritten_queries = {
            str(key): str(value).strip()
            for key, value in ((payload.get("rewritten_queries") or {}).items() if isinstance(payload.get("rewritten_queries"), dict) else [])
            if str(key).strip() and str(value).strip()
        }
        return {
            "available": True,
            "answer": str(raw or "").strip(),
            "label": parsed_label,
            "is_media": parsed_label == CLASSIFIER_LABEL_MEDIA,
            "parsed": payload,
            "domain": str(payload.get("domain") or "general").strip() or "general",
            "lookup_mode": str(payload.get("lookup_mode") or "general_lookup").strip() or "general_lookup",
            "entities": _coerce_string_list(payload.get("entities")),
            "filters": _coerce_filter_map(payload.get("filters")),
            "time_window": dict(payload.get("time_window") or {}),
            "ranking": dict(payload.get("ranking") or {}),
            "followup_target": str(payload.get("followup_target") or "").strip(),
            "needs_comparison": bool(payload.get("needs_comparison")),
            "needs_explanation": bool(payload.get("needs_explanation")),
            "confidence": float(payload.get("confidence") or 0.0),
            "rewritten_queries": rewritten_queries,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "available": False,
            "answer": str(exc),
            "label": CLASSIFIER_LABEL_OTHER,
            "is_media": False,
            "parsed": None,
            "domain": "general",
            "lookup_mode": "general_lookup",
            "entities": [],
            "filters": {},
            "time_window": {},
            "ranking": {},
            "followup_target": "",
            "needs_comparison": False,
            "needs_explanation": False,
            "confidence": 0.0,
            "rewritten_queries": {},
        }


def _rewrite_tool_queries_with_llm(
    question: str,
    decision: RouterDecision,
    previous_state: dict[str, Any],
    quota_state: dict[str, Any],
) -> dict[str, str]:
    if decision.domain != "media":
        return {}
    prior_scope = {
        "lookup_mode": str(previous_state.get("lookup_mode") or ""),
        "media_type": str(previous_state.get("media_type") or ""),
        "entity": str(previous_state.get("entity") or ""),
        "entities": [str(item).strip() for item in (previous_state.get("entities") or []) if str(item).strip()],
        "filters": _normalize_media_filter_map(previous_state.get("filters")),
        "time_constraint": dict(previous_state.get("time_constraint") or {}),
        "ranking": dict(previous_state.get("ranking") or {}),
    }
    decision_payload = {
        "domain": decision.domain,
        "lookup_mode": decision.lookup_mode,
        "entities": list(decision.entities),
        "filters": _normalize_media_filter_map(decision.filters),
        "selection": _normalize_media_filter_map(decision.selection),
        "time_constraint": dict(decision.time_constraint),
        "ranking": dict(decision.ranking),
        "followup_mode": decision.followup_mode,
        "media_type": decision.media_type,
    }
    prompt = (
        "You rewrite media questions into tool-grade retrieval queries only. Return JSON only.\n"
        "Fields: media_query, doc_query, tmdb_query, web_query.\n"
        "Rules:\n"
        "- media_query must be short, retrieval-grade, and include the concrete title/scope.\n"
        "- Avoid generic phrasing like 请概述一下内容 / 介绍一下 / 那个怎么样.\n"
        "- Preserve entity, filter, and time constraints from the structured decision.\n"
        "- Prefer local personal-review wording for evaluation questions.\n"
        "- For person/creator entity queries that do not mention a specific media type, keep media_query as the person name only — do NOT add category words like 电影, 书籍, 音乐.\n\n"
        f"Previous structured state:\n{json.dumps(prior_scope, ensure_ascii=False)}\n\n"
        f"Current structured decision:\n{json.dumps(decision_payload, ensure_ascii=False)}\n\n"
        f"Current question:\n{question}"
    )
    try:
        raw = _llm_chat(
            messages=[{"role": "user", "content": prompt}],
            backend="local",
            quota_state=quota_state,
            count_quota=False,
        )
        payload = _extract_json_object_from_text(raw)
        return {
            str(key): str(value).strip()
            for key, value in payload.items()
            if str(key).strip() in {"media_query", "doc_query", "tmdb_query", "web_query"} and str(value).strip()
        }
    except Exception:
        return {}


def _sanitize_ranking_mode(mode: str) -> str:
    normalized = str(mode or "relevance").strip().lower()
    return normalized if normalized in ALLOWED_RANKING_MODES else "relevance"


def _looks_like_generic_tool_query(text: str) -> bool:
    value = str(text or "").strip()
    if not value:
        return True
    compact = re.sub(r"\s+", "", value)
    generic_cues = (
        "请概述一下内容",
        "概述一下内容",
        "介绍一下",
        "详细介绍",
        "请详细介绍",
        "那个怎么样",
        "这个怎么样",
        "请概述",
        "请展开",
    )
    return compact in generic_cues


def _sanitize_tool_queries(candidate_queries: dict[str, str], fallback_queries: dict[str, str]) -> dict[str, str]:
    sanitized: dict[str, str] = {}
    for key in ("media_query", "doc_query", "tmdb_query", "web_query"):
        candidate = str(candidate_queries.get(key) or "").strip()
        fallback = str(fallback_queries.get(key) or "").strip()
        if candidate and not _looks_like_generic_tool_query(candidate):
            sanitized[key] = candidate
        elif fallback:
            sanitized[key] = fallback
    return sanitized


def _build_tool_grade_rewritten_queries(
    question: str,
    decision: RouterDecision,
    previous_state: dict[str, Any],
    llm_queries: dict[str, str] | None = None,
) -> dict[str, str]:
    deterministic = _build_media_followup_rewrite_queries(
        question,
        previous_state,
        followup_mode=decision.followup_mode,
        entities=list(decision.entities),
        filters=_normalize_media_filter_map(decision.filters),
        media_type=str(decision.media_type or ""),
    ) if decision.domain == "media" else {}
    if decision.domain != "media":
        return {
            "doc_query": str(question or "").strip(),
            "web_query": str(question or "").strip(),
        }
    # For creator collection queries, pin media_query to the canonical author name.
    # The filters.author already narrows to this creator; injecting media-type words
    # (e.g. "电影作品") would silently bias ranking against non-video works.
    _cr_ev = decision.evidence if isinstance(decision.evidence, dict) else {}
    if _cr_ev.get("creator_collection_query"):
        _cr_info = _cr_ev.get("creator_resolution") or {}
        _cr_canonical = str(_cr_info.get("canonical") or "").strip()
        if _cr_canonical:
            deterministic["media_query"] = _cr_canonical
    candidate_queries = dict(llm_queries or {})
    if decision.entities and (not deterministic.get("media_query") or _looks_like_generic_tool_query(deterministic.get("media_query"))):
        entity = decision.entities[0]
        if _question_requests_personal_evaluation(question):
            deterministic["media_query"] = f"{entity} 个人评分与短评"
        elif _question_requests_media_details(question):
            deterministic["media_query"] = f"{entity} 条目细节"
        else:
            deterministic["media_query"] = entity
    if not deterministic.get("tmdb_query") and decision.entities:
        deterministic["tmdb_query"] = decision.entities[0]
    if not deterministic.get("doc_query"):
        deterministic["doc_query"] = str(question or "").strip()
    if not deterministic.get("web_query"):
        deterministic["web_query"] = deterministic.get("media_query") or str(question or "").strip()
    return _sanitize_tool_queries(candidate_queries, deterministic)


def _apply_router_semantic_repairs(
    question: str,
    decision: RouterDecision,
    previous_state: dict[str, Any],
) -> RouterDecision:
    repairs: list[str] = []
    ranking_mode = _sanitize_ranking_mode(str((decision.ranking or {}).get("mode") or decision.sort or "relevance"))
    requested_sort = _infer_requested_sort(question)
    collection_query = _is_collection_media_query(question)
    if ranking_mode == "relevance" and requested_sort != "relevance":
        ranking_mode = requested_sort
        repairs.append("ranking:explicit_sort")
    if ranking_mode != "relevance" and not (collection_query or decision.lookup_mode == "filter_search" or _question_requests_personal_evaluation(question)):
        ranking_mode = "relevance"
        repairs.append("ranking:normalized_to_relevance")
    if ranking_mode != str((decision.ranking or {}).get("mode") or "relevance"):
        decision.ranking = {**dict(decision.ranking or {}), "mode": ranking_mode, "source": "semantic_repair"}
        decision.sort = ranking_mode

    # Music work-version compare repair:
    # Detect composer + work signature (e.g., "Tchaikovsky Violin Concerto")
    # and force local-first media compare path instead of mixed/doc-rag path.
    music_work_hints = _extract_music_work_hints(question, decision.filters)
    _filter_music_signature = _has_music_signature_filters(decision.filters)
    if music_work_hints["composer_hints"] and (music_work_hints["work_signature"] or _filter_music_signature):
        _merge_filter_values(decision.filters, "author", list(music_work_hints["composer_hints"]))
        if decision.lookup_mode == "general_lookup":
            decision.lookup_mode = "filter_search"
            repairs.append("lookup_mode:music_general_to_filter")
        comparison_cues = ("比较", "对比", "版本", "演绎", "评价", "咋样", "如何")
        if decision.needs_comparison or any(cue in str(question or "") for cue in comparison_cues):
            decision.needs_comparison = True
        if decision.intent != "media_lookup":
            decision.intent = "media_lookup"
            repairs.append("intent:mixed_to_media_for_music_work_compare")
        if decision.domain != "media":
            decision.domain = "media"
            repairs.append("domain:force_media_for_music_work_compare")
        if decision.needs_doc_rag:
            decision.needs_doc_rag = False
            repairs.append("needs_doc_rag:disabled_for_music_work_compare")
        decision.needs_media_db = True
        decision.arbitration = "music_work_compare_wins"
        reasons_wo_arb = [r for r in decision.reasons if not str(r).startswith("arbitration:")]
        decision.reasons = reasons_wo_arb + ["arbitration:music_work_compare_wins"]
        ev = dict(decision.evidence or {})
        ev["music_work_versions_compare"] = True
        ev["music_work_hints"] = music_work_hints
        decision.evidence = ev
        repairs.append("music_work_signature:enforced")

    if decision.domain == "media" and decision.followup_mode == "none" and _state_has_media_context(previous_state) and _is_short_followup_surface(question):
        decision.followup_mode = "inherit_entity" if str(previous_state.get("entity") or "").strip() else "inherit_filters"
        repairs.append(f"followup:{decision.followup_mode}")

    # ── Creator collection repair ────────────────────────────────────────────────
    # Handles queries like "加缪的作品有哪些" where the LLM returns filter_search
    # with empty filters because it didn't extract the creator as filters.author.
    # Resolution: detect the creator pattern, resolve with entity_resolver, inject
    # filters.author so the library filter-search finds actual works.
    if (
        decision.domain == "media"
        and decision.lookup_mode in ("filter_search", "general_lookup")
        and decision.followup_mode == "none"
        and not decision.filters
        and not decision.entities
    ):
        _creator_res = _extract_creator_from_collection_query(question)
        if _creator_res:
            decision.filters = {"author": [_creator_res.canonical]}
            decision.lookup_mode = "filter_search"
            # Enable external enrichment: MediaWiki for all creators;
            # TMDB additionally for video-domain creators (directors).
            if _creator_res.media_type_hint == "video":
                decision.needs_external_media_db = True
            _ev = dict(decision.evidence or {})
            _ev["creator_collection_query"] = True
            _ev["creator_resolution"] = {
                "canonical": _creator_res.canonical,
                "media_type_hint": _creator_res.media_type_hint,
                "confidence": round(float(_creator_res.confidence), 4),
                "match_kind": str(_creator_res.match_kind),
            }
            decision.evidence = _ev
            if "arbitration:llm_media_weak_general" not in " ".join(decision.reasons):
                decision.arbitration = "creator_collection_wins"
                _existing = [r for r in decision.reasons if not r.startswith("arbitration:")]
                decision.reasons = _existing + ["arbitration:creator_collection_wins"]
            repairs.append("creator:resolved_from_collection_query")

    if decision.domain == "media" and decision.followup_mode == "inherit_entity" and not decision.entities:
        previous_entity = str(previous_state.get("entity") or "").strip()
        if previous_entity:
            decision.entities = [previous_entity]
            repairs.append("entity:inherited_from_previous")

    if decision.domain == "media" and decision.lookup_mode == "entity_lookup" and not decision.entities:
        if decision.filters or len(decision.date_range) == 2 or decision.followup_mode in {"inherit_filters", "inherit_timerange"}:
            decision.lookup_mode = "filter_search"
            repairs.append("lookup_mode:entity_to_filter")
        else:
            decision.lookup_mode = "general_lookup"
            repairs.append("lookup_mode:entity_to_general")

    if decision.domain == "media" and decision.lookup_mode == "general_lookup" and decision.entities and not (
        decision.filters or len(decision.date_range) == 2 or decision.followup_mode in {"inherit_filters", "inherit_timerange"}
    ):
        decision.lookup_mode = "entity_lookup"
        repairs.append("lookup_mode:general_to_entity")

    if decision.domain == "media" and decision.lookup_mode == "filter_search" and not (
        decision.selection or decision.filters or len(decision.date_range) == 2 or decision.followup_mode in {"inherit_filters", "inherit_timerange"} or collection_query
    ):
        if decision.entities:
            decision.lookup_mode = "entity_lookup"
            repairs.append("lookup_mode:filter_to_entity")
        else:
            decision.lookup_mode = "general_lookup"
            repairs.append("lookup_mode:filter_to_general")

    if decision.lookup_mode == "filter_search":
        decision.entities = []
    if decision.lookup_mode == "entity_lookup" and decision.entities:
        decision.followup_target = decision.entities[0]

    decision.selection = _build_media_selection(_normalize_media_filter_map(decision.filters), str(decision.media_type or ""))
    if repairs:
        reasons = [str(item) for item in decision.reasons if str(item).strip()]
        for repair in repairs:
            marker = f"repair:{repair}"
            if marker not in reasons:
                reasons.append(marker)
        decision.reasons = reasons
        evidence = dict(decision.evidence or {})
        evidence["semantic_repairs"] = repairs
        decision.evidence = evidence
    return decision


def _estimate_doc_similarity(query: str, query_profile: dict[str, Any]) -> dict[str, Any]:
    top_k = max(3, min(6, int(query_profile.get("doc_vector_top_n", DOC_VECTOR_TOP_N) or DOC_VECTOR_TOP_N)))
    query_tokens = _classifier_token_count(query)
    candidate_queries = [query]
    rewrite_status = "skipped"
    if query_tokens >= LONG_QUERY_MIN_TOKENS or len(str(query or "")) >= 48:
        candidate_queries, rewrite_status = _rewrite_doc_queries(query)

    try:
        best_row: dict[str, Any] = {}
        best_query = query
        total_tech_rows = 0
        for candidate_query in candidate_queries:
            payload = _http_json(
                "GET",
                f"{AI_SUMMARY_BASE}/api/preview/search/vector?" + urlparse.urlencode({"q": candidate_query, "top_k": top_k}),
                timeout=20.0,
            )
            rows = payload.get("results", []) if isinstance(payload, dict) else []
            valid_rows = [row for row in rows if isinstance(row, dict)]
            tech_rows = [
                row for row in valid_rows
                if str(row.get("path", "")).strip().startswith(TECH_SPACE_PREFIXES)
            ]
            total_tech_rows += len(tech_rows)
            candidate_best = max(tech_rows, key=lambda row: _safe_score(row.get("score")), default={})
            if _safe_score(candidate_best.get("score")) > _safe_score(best_row.get("score")):
                best_row = candidate_best
                best_query = candidate_query

        top_score = _safe_score(best_row.get("score"))
        return {
            "score": round(top_score, 6),
            "top_path": str(best_row.get("path", "")).strip(),
            "count": total_tech_rows,
            "matched_query": best_query,
            "queries": candidate_queries,
            "rewrite_status": rewrite_status,
            "status": "ok",
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "score": 0.0,
            "top_path": "",
            "count": 0,
            "matched_query": query,
            "queries": candidate_queries,
            "rewrite_status": rewrite_status,
            "status": f"error:{exc}",
        }


def _classify_query_type(query: str, quota_state: dict[str, Any], query_profile: dict[str, Any]) -> dict[str, Any]:
    decision, llm_media, previous_state = _build_router_decision(query, [], quota_state, query_profile)
    return _router_decision_to_query_classification(decision, llm_media, previous_state, query_profile)


def _build_router_decision_path(
    query_classification: dict[str, Any],
    search_mode: str,
    planned_tools: list[PlannedToolCall],
    tool_results: list[ToolExecution],
) -> tuple[str, list[str]]:
    path: list[str] = []
    router_decision = query_classification.get("router_decision") if isinstance(query_classification.get("router_decision"), dict) else {}
    query_type = _get_query_type(query_classification=query_classification)
    domain = str(router_decision.get("domain") or "general")
    intent = str(router_decision.get("intent") or "knowledge_qa")
    followup_mode = str(router_decision.get("followup_mode") or "none")
    confidence = float(router_decision.get("confidence", 0.0) or 0.0)
    freshness = str(router_decision.get("freshness") or "none")

    path.append(f"domain:{domain}")
    path.append(f"intent:{intent}")
    if followup_mode != "none":
        path.append(f"followup:{followup_mode}")
    if bool(router_decision.get("entities")):
        path.append("explicit_media_entity")
    if any(call.name == TOOL_EXPAND_MEDIAWIKI_CONCEPT for call in planned_tools):
        path.append("external_concept_expand")
    if any(call.name == TOOL_SEARCH_TMDB for call in planned_tools):
        path.append("external_media_db")
    if freshness != "none":
        path.append(f"freshness:{freshness}")
    if confidence >= ROUTER_CONFIDENCE_HIGH:
        path.append("confidence:high")
    elif confidence >= ROUTER_CONFIDENCE_MEDIUM:
        path.append("confidence:medium")
    else:
        path.append("confidence:low")

    normalized_mode = _normalize_search_mode(search_mode)
    if normalized_mode == "hybrid" and any(call.name == TOOL_SEARCH_WEB for call in planned_tools):
        path.append("policy:web_fallback")
    elif normalized_mode == "local_only":
        path.append("local_only")

    if query_type == QUERY_TYPE_MIXED:
        path.append("mixed_multi_tool")
        category = "mixed_multi_tool"
    elif domain == "tech":
        category = "tech_rag"
    elif domain == "media":
        category = "media_lookup"
    elif "policy:web_fallback" in path:
        category = "web_fallback"
    else:
        category = "default_doc_rag"

    executed_tools = [item for item in tool_results if str(item.status or "").strip().lower() != "skipped"]
    if len(executed_tools) > 1:
        path.append("multi_tool_executed")

    return category, path


def _score_value(row: dict[str, Any]) -> float | None:
    value = row.get("score", None)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        text = str(value).strip()
        return float(text) if text else None
    except Exception:
        match_confidence = row.get("match_confidence", None)
        if isinstance(match_confidence, (int, float)):
            return float(match_confidence)
        try:
            text = str(match_confidence).strip()
            return float(text) if text else None
        except Exception:
            return None


def _filter_rows(
    rows: list[dict[str, Any]],
    limit: int,
    threshold: float,
    threshold_selector: Callable[[dict[str, Any]], float] | None = None,
) -> list[dict[str, Any]]:
    picked: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        score = _score_value(row)
        row_threshold = float(threshold_selector(row)) if threshold_selector is not None else float(threshold)
        if score is None or score <= row_threshold:
            continue
        cloned = dict(row)
        cloned["score"] = float(score)
        picked.append(cloned)
    picked.sort(key=lambda x: float(x.get("score", 0.0)), reverse=True)
    return picked[: max(1, int(limit))]


def _media_threshold_selector(row: dict[str, Any], keyword_threshold: float, vector_threshold: float) -> float:
    mode = str(row.get("retrieval_mode", "keyword") or "").strip().lower()
    if mode == "vector":
        return float(vector_threshold)
    return float(keyword_threshold)


def _select_media_vector_query(
    original_query: str,
    expanded_query: str,
    extracted_entity: str,
    rewritten_query: str,
) -> str:
    expanded = str(expanded_query or "").strip()
    original = str(original_query or "").strip()
    if expanded and expanded != original:
        return expanded
    if extracted_entity:
        return extracted_entity
    if rewritten_query:
        return rewritten_query
    return expanded or original


def _log_agent_media_miss(query: str, query_profile: dict[str, Any]) -> None:
    try:
        media_threshold = float(
            query_profile.get("media_vector_score_threshold", MEDIA_VECTOR_SCORE_THRESHOLD)
            or MEDIA_VECTOR_SCORE_THRESHOLD
        )
        boundary_log_no_context_query(
            query,
            source="agent_media",
            top1_score=None,
            threshold=media_threshold,
        )
    except Exception:
        pass


def _apply_reference_limits(tool_results: list[ToolExecution], search_mode: str, query_profile: dict[str, Any]) -> list[ToolExecution]:
    normalized_mode = _normalize_search_mode(search_mode)
    limit_delta = int(query_profile.get("limit_delta", 0) or 0)
    doc_limit_base = HYBRID_TOP_K_DOC if normalized_mode == "hybrid" else LOCAL_TOP_K_DOC
    media_limit_base = HYBRID_TOP_K_MEDIA if normalized_mode == "hybrid" else LOCAL_TOP_K_MEDIA
    web_limit_base = HYBRID_TOP_K_WEB if normalized_mode == "hybrid" else 0
    doc_limit = max(1, int(doc_limit_base + limit_delta))
    media_limit = max(1, int(media_limit_base + limit_delta))
    web_limit = max(0, int(web_limit_base + limit_delta))
    # For list_plus_expand collection queries expand the cap so all candidate
    # items survive before per-item external enrichment is applied.
    if str(query_profile.get("answer_shape", "") or "") == "list_plus_expand":
        media_limit = max(media_limit, COLLECTION_FILTER_TOP_K_MEDIA)
    doc_threshold = float(query_profile.get("doc_score_threshold", DOC_SCORE_THRESHOLD))
    media_keyword_threshold = float(query_profile.get("media_keyword_score_threshold", MEDIA_KEYWORD_SCORE_THRESHOLD))
    media_vector_threshold = float(query_profile.get("media_vector_score_threshold", MEDIA_VECTOR_SCORE_THRESHOLD))
    web_threshold = float(query_profile.get("web_score_threshold", WEB_SCORE_THRESHOLD))

    shaped: list[ToolExecution] = []
    for result in tool_results:
        if not isinstance(result.data, dict):
            shaped.append(result)
            continue
        rows = result.data.get("results", [])
        if not isinstance(rows, list):
            shaped.append(result)
            continue

        if result.tool == TOOL_QUERY_DOC_RAG:
            filtered = _filter_rows(rows, doc_limit, doc_threshold)
            summary = f"命中 {len(filtered)} 条文档（score>{doc_threshold}）"
            data = dict(result.data)
            data["results"] = filtered
            shaped.append(ToolExecution(tool=result.tool, status=result.status, summary=summary, data=data))
            continue

        if result.tool == TOOL_QUERY_MEDIA:
            threshold_pass_rows: list[dict[str, Any]] = []
            threshold_dropped = 0
            for row in rows:
                if not isinstance(row, dict):
                    continue
                score = _score_value(row)
                row_threshold = _media_threshold_selector(
                    row,
                    media_keyword_threshold,
                    media_vector_threshold,
                )
                if score is None or score <= float(row_threshold):
                    threshold_dropped += 1
                    continue
                cloned = dict(row)
                cloned["score"] = float(score)
                threshold_pass_rows.append(cloned)
            sort_preference = str(result.data.get("sort", "relevance") or "relevance")
            threshold_pass_rows = _sort_media_results(threshold_pass_rows, sort_preference)
            filtered = threshold_pass_rows[: max(1, int(media_limit))]
            summary = (
                f"命中 {len(filtered)} 条媒体记录"
                f"（keyword score>{media_keyword_threshold}; vector score>{media_vector_threshold}）"
            )
            data = dict(result.data)
            data["results"] = filtered
            validation = dict(data.get("validation") or {}) if isinstance(data.get("validation"), dict) else {}
            top_k_dropped = max(0, len(threshold_pass_rows) - len(filtered))
            validation["returned_result_count"] = len(filtered)
            validation["dropped_by_reference_limit"] = threshold_dropped + top_k_dropped
            validation["reference_limit_drop_reasons"] = {
                "below_score_threshold": threshold_dropped,
                "top_k_truncation": top_k_dropped,
            }
            data["validation"] = validation
            shaped.append(ToolExecution(tool=result.tool, status=result.status, summary=summary, data=data))
            continue

        if result.tool == TOOL_SEARCH_WEB:
            if web_limit <= 0:
                shaped.append(
                    ToolExecution(tool=result.tool, status="skipped", summary="本地回答模式已禁用联网搜索", data={"results": []})
                )
            else:
                filtered = _filter_rows(rows, web_limit, web_threshold)
                summary = f"命中 {len(filtered)} 条网页结果（score>{web_threshold}）"
                data = dict(result.data)
                data["results"] = filtered
                shaped.append(ToolExecution(tool=result.tool, status=result.status, summary=summary, data=data))
            continue

        if result.tool == TOOL_SEARCH_TMDB:
            data = dict(result.data)
            data["results"] = rows[: max(1, media_limit + 1)]
            shaped.append(
                ToolExecution(
                    tool=result.tool,
                    status=result.status,
                    summary=f"TMDB 命中 {len(data['results'])} 条外部媒体结果",
                    data=data,
                )
            )
            continue

        shaped.append(result)

    return shaped


def _parse_planned_tools(text: str, question: str, allowed_tool_names: list[str] | None = None) -> list[PlannedToolCall]:
    raw = (text or "").strip()
    if not raw:
        raw = "{}"

    allowed_names = set(allowed_tool_names or TOOL_NAMES)

    blob = raw
    fenced = re.search(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", raw)
    if fenced:
        blob = fenced.group(1)
    else:
        obj_match = re.search(r"(\{[\s\S]*\})", raw)
        if obj_match:
            blob = obj_match.group(1)

    try:
        data = json.loads(blob)
    except Exception:
        data = {}

    calls: list[PlannedToolCall] = []
    arr = data.get("tools", []) if isinstance(data, dict) else []
    if isinstance(arr, list):
        for item in arr:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "")).strip()
            if name not in allowed_names:
                continue
            args = item.get("args", {})
            query = ""
            options: dict[str, Any] = {}
            if isinstance(args, dict):
                query = str(args.get("query", "")).strip()
                raw_options = args.get("options")
                if isinstance(raw_options, dict):
                    options = dict(raw_options)
            if not query:
                query = question
            calls.append(PlannedToolCall(name=name, query=query, options=options))

    # Deduplicate by tool name while keeping first occurrence.
    dedup: list[PlannedToolCall] = []
    seen: set[str] = set()
    for call in calls:
        if call.name in seen:
            continue
        seen.add(call.name)
        dedup.append(call)
    if dedup:
        return dedup

    # Fallback heuristic planner.
    temp_profile = _resolve_query_profile(question)
    temp_decision, _, _ = _build_router_decision(question, [], {}, temp_profile)
    return RoutingPolicy().build_plan(temp_decision, "hybrid").planned_tools


def _parse_query_rewrite_output(raw: str, fallback: str, count: int) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return [fallback]

    parsed_queries: list[str] = []
    try:
        data = json.loads(text)
        if isinstance(data, dict) and isinstance(data.get("queries"), list):
            parsed_queries = [str(x).strip() for x in data.get("queries", []) if str(x).strip()]
        elif isinstance(data, list):
            parsed_queries = [str(x).strip() for x in data if str(x).strip()]
    except Exception:
        pass

    if not parsed_queries:
        for line in text.splitlines():
            value = line.strip().lstrip("-* ").strip()
            if value:
                parsed_queries.append(value)

    dedup: list[str] = []
    seen: set[str] = set()
    for query in [fallback] + parsed_queries:
        key = query.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        dedup.append(query.strip())
        if len(dedup) >= max(1, count):
            break
    return dedup or [fallback]


def _rewrite_doc_queries(query: str) -> tuple[list[str], str]:
    prompt = (
        "你是RAG查询改写助手。"
        "请基于用户问题输出最多2条中文检索query，用于title/summary/keywords结构文档。"
        "保留原问题的核心表述，不要偏离原语义。"
        "只输出JSON：{\"queries\":[\"q1\",\"q2\"]}。"
    )
    try:
        rewritten = _llm_chat(
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": query},
            ],
            backend="local",
            quota_state={"deepseek": 0},
            count_quota=False,
        )
        return _parse_query_rewrite_output(rewritten, query, DOC_QUERY_REWRITE_COUNT), "ok"
    except Exception as exc:  # noqa: BLE001
        return [query], f"fallback:{exc}"


def _safe_score(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _merge_doc_vector_results(
    rows_by_query: list[tuple[str, list[dict[str, Any]]]],
    *,
    primary_query: str = "",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    merged: dict[str, dict[str, Any]] = {}
    debug_batches: list[dict[str, Any]] = []
    normalized_primary = str(primary_query or "").strip().lower()

    for query, rows in rows_by_query:
        compact_rows: list[dict[str, Any]] = []
        for idx, row in enumerate(rows, start=1):
            if not isinstance(row, dict):
                continue
            path = str(row.get("path", "")).strip()
            if not path:
                continue
            score = _safe_score(row.get("score"))
            query_bonus = DOC_PRIMARY_QUERY_SCORE_BONUS if normalized_primary and query.strip().lower() == normalized_primary else 0.0
            compact_rows.append({"path": path, "score": score})
            existing = merged.get(path)
            priority_score = score + query_bonus
            if existing is None or priority_score > _safe_score(existing.get("query_priority_score")):
                merged[path] = {
                    "path": path,
                    "topic": row.get("topic"),
                    "vector_score": score,
                    "score": score,
                    "query_priority_boost": query_bonus,
                    "query_priority_score": priority_score,
                    "matched_rank": idx,
                    "matched_queries": [query],
                }
            else:
                matched = existing.get("matched_queries", [])
                if not isinstance(matched, list):
                    matched = []
                if query not in matched:
                    matched.append(query)
                existing["matched_queries"] = matched
        debug_batches.append({"query": query, "results": compact_rows})

    merged_rows = sorted(
        merged.values(),
        key=lambda x: (_safe_score(x.get("query_priority_score")), _safe_score(x.get("vector_score"))),
        reverse=True,
    )
    return merged_rows, debug_batches


def _build_keyword_score_map(queries: list[str], vector_top_n: int) -> dict[str, float]:
    keyword_scores: dict[str, float] = {}
    for query in queries:
        payload = _http_json(
            "GET",
            f"{AI_SUMMARY_BASE}/api/preview/search/keyword?" + urlparse.urlencode({"q": query, "limit": int(vector_top_n)}),
        )
        rows = payload.get("results", []) if isinstance(payload, dict) else []
        for row in rows:
            if not isinstance(row, dict):
                continue
            path = str(row.get("path", "")).strip()
            if not path:
                continue
            score = _safe_score(row.get("score"))
            if score > keyword_scores.get(path, -1.0):
                keyword_scores[path] = score
    return keyword_scores


def _rerank_merged_doc_rows(rows: list[dict[str, Any]], keyword_scores: dict[str, float]) -> list[dict[str, Any]]:
    # Normalize BM25 keyword scores to [0, 1] before fusion so the weights
    # are meaningful alongside cosine-similarity vector scores.
    max_kw = max(keyword_scores.values(), default=0.0)
    norm = max_kw if max_kw > 0.0 else 1.0
    reranked: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        path = str(item.get("path", "")).strip()
        vector_score = _safe_score(item.get("vector_score"))
        keyword_score = min(1.0, keyword_scores.get(path, 0.0) / norm)
        final_score = (0.75 * vector_score) + (0.25 * keyword_score)
        item["keyword_score"] = keyword_score
        item["score"] = final_score
        reranked.append(item)
    reranked.sort(key=lambda x: _safe_score(x.get("score")), reverse=True)
    return reranked


def _tool_query_document_rag(query: str, query_profile: dict[str, Any], trace_id: str = "") -> ToolExecution:
    import time as _time
    doc_vector_top_n = max(4, int(query_profile.get("doc_vector_top_n", DOC_VECTOR_TOP_N) or DOC_VECTOR_TOP_N))
    rewrite_queries, rewrite_status = _rewrite_doc_queries(query)
    vector_batches: list[tuple[str, list[dict[str, Any]]]] = []
    warnings: list[str] = []
    embed_cache_hit = 0

    _t_vec0 = _time.perf_counter()
    for rewritten_query in rewrite_queries:
        try:
            vec = _http_json(
                "GET",
                f"{AI_SUMMARY_BASE}/api/preview/search/vector?"
                + urlparse.urlencode({"q": rewritten_query, "top_k": max(6, int(doc_vector_top_n))}),
                headers={"X-Trace-Id": trace_id, "X-Trace-Stage": "agent.doc.vector_preview"},
            )
            vec_rows = vec.get("results", []) if isinstance(vec, dict) else []
            vector_batches.append((rewritten_query, [row for row in vec_rows if isinstance(row, dict)]))
            if isinstance(vec, dict) and float(vec.get("embed_cache_hit", 0) or 0) > 0:
                embed_cache_hit = 1
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"向量检索不可用({rewritten_query}): {exc}")
    _vector_recall_seconds = _time.perf_counter() - _t_vec0

    if not vector_batches and warnings:
        raise RuntimeError("; ".join(warnings))

    merged_rows, vector_debug = _merge_doc_vector_results(vector_batches, primary_query=query)
    keyword_scores: dict[str, float] = {}
    try:
        keyword_scores = _build_keyword_score_map(rewrite_queries, doc_vector_top_n)
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"关键词重排不可用: {exc}")

    _t_rerank0 = _time.perf_counter()
    reranked_rows = _rerank_merged_doc_rows(merged_rows, keyword_scores)
    _rerank_seconds = _time.perf_counter() - _t_rerank0

    # Determine whether the knowledge base covers this query (no_context if top-1 < threshold).
    _doc_threshold = float(query_profile.get("doc_score_threshold", DOC_SCORE_THRESHOLD) or DOC_SCORE_THRESHOLD)
    _top1_score_before: float | None = max((float(r.get("score", 0.0)) for r in merged_rows), default=None)
    _top1_score: float | None = max((float(r.get("score", 0.0)) for r in reranked_rows), default=None)
    _top1_path_before = str(merged_rows[0].get("path", "")).strip() if merged_rows else ""
    _top1_path_after = str(reranked_rows[0].get("path", "")).strip() if reranked_rows else ""
    _top1_identity_changed: int | None = None
    _top1_rank_shift: float | None = None
    if _top1_path_before and _top1_path_after:
        _top1_identity_changed = int(_top1_path_before != _top1_path_after)
        _before_rank_after_top1 = next(
            (idx + 1 for idx, row in enumerate(merged_rows) if str(row.get("path", "")).strip() == _top1_path_after),
            None,
        )
        if _before_rank_after_top1 is not None:
            # Positive means moved up in ranking (e.g. 7 -> 2 gives +5).
            _top1_rank_shift = float(_before_rank_after_top1 - 1)
    _no_context = 1 if (_top1_score is None or _top1_score < _doc_threshold) else 0

    summary = f"命中 {len(reranked_rows)} 条文档（rewrite={len(rewrite_queries)}，vector_top_n={doc_vector_top_n}）"
    if warnings:
        summary += f"（部分降级: {'; '.join(warnings)}）"

    query_rewrite_hit = int(str(rewrite_status or "").strip().lower() == "ok")

    return ToolExecution(
        tool=TOOL_QUERY_DOC_RAG,
        status="ok",
        summary=summary,
        data={
            "trace_id": trace_id,
            "trace_stage": "agent.tool.query_document_rag",
            "results": reranked_rows[: max(8, int(doc_vector_top_n))],
            "query_profile": query_profile,
            "query_rewrite": {
                "original": query,
                "queries": rewrite_queries,
                "status": rewrite_status,
            },
            "embed_cache_hit": embed_cache_hit,
            "query_rewrite_hit": query_rewrite_hit,
            "vector_batches": vector_debug,
            "rerank": {
                "method": "vector+keyword_fusion",
                "vector_weight": 0.85,
                "keyword_weight": 0.15,
            },
            "vector_recall_seconds": round(_vector_recall_seconds, 6),
            "rerank_seconds": round(_rerank_seconds, 6),
            "doc_top1_score": round(_top1_score, 4) if _top1_score is not None else None,
            "doc_top1_score_before_rerank": round(_top1_score_before, 4) if _top1_score_before is not None else None,
            "doc_top1_identity_changed": _top1_identity_changed,
            "doc_top1_rank_shift": round(_top1_rank_shift, 4) if _top1_rank_shift is not None else None,
            "no_context": _no_context,
        },
    )


def _rewrite_media_query(query: str) -> str:
    raw = (query or "").strip()
    if not raw:
        return ""

    normalized = raw
    for prefix in ["在我的数据库里", "我的数据库里", "在数据库里", "请问", "帮我", "我想知道", "想问下"]:
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix) :].strip(" ，。！？?；;:")

    match = re.search(r"(?:我)?对(?P<title>.+?)的?(?:评价|看法|评分|感受|印象)", normalized)
    if match:
        entities, _ = _normalize_media_entities_and_filters(_split_media_entities(match.group("title")))
        title = " ".join(entities).strip()
        if title:
            return f"{title} 评价"

    # If no explicit pattern is matched, keep the original query.
    return raw


def _split_media_entities(raw: str) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return []

    text = re.sub(r"(?:以及)?(?:两者|二者)的?(?:对比|比较|区别|差异).*$", "", text)
    text = re.sub(r"的?(?:评价|看法|评分|感受|印象|想法).*$", "", text)
    text = text.strip(" ，。！？?；;:：\"'“”‘’（）()")
    if not text:
        return []

    parts = _MEDIA_COMPARE_SPLIT_RE.split(text)
    dedup: list[str] = []
    seen: set[str] = set()
    for part in parts:
        value = str(part or "").strip(" ，。！？?；;:：\"'“”‘’（）()")
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        dedup.append(value)
    return dedup


def _looks_like_generic_media_scope(text: str) -> bool:
    normalized = str(text or "").strip()
    if not normalized:
        return True
    if normalized in {"我", "我的", "自己", "本人", "我们", "咱们", "咱", "那个", "这个", "那部", "这部", "那本", "这本", "它", "它们"}:
        return True
    compact = re.sub(r"\s+", "", normalized)
    if re.fullmatch(r"(?:20\d{2}年?)?\d{1,2}(?:月)?(?:到|至|[-~—－])\d{1,2}月(?:的)?(?:番|番剧|动画|动漫|新番)?", compact):
        return True
    if re.fullmatch(r"(?:20\d{2}年?)?\d{1,2}月(?:的)?(?:番|番剧|动画|动漫|新番)?", compact):
        return True
    if re.fullmatch(r"(?:20\d{2}年?)?(?:上半年|下半年)(?:的)?(?:番|番剧|动画|动漫|新番)?", compact):
        return True
    generic_markers = (
        "有哪些",
        "什么",
        "简介",
        "剧情",
        "介绍",
        "导演",
        "演员",
        "评分",
        "时间",
        "我阅读过",
        "我读过",
        "我看过",
        "我玩过",
        "番剧",
        "动画",
        "动漫",
        "新番",
        "评价比较高",
        "评分比较高",
        "文学",
        "小说",
        "诗歌",
        "作家",
    )
    return any(marker in normalized for marker in generic_markers)


def _extract_media_entities(query: str) -> list[str]:
    raw = (query or "").strip()
    if not raw:
        return []
    if _is_collection_media_query(raw):
        return []
    if _looks_like_time_only_followup(raw):
        return []

    normalized = raw
    for prefix in ["在我的数据库里", "我的数据库里", "在数据库里", "请问", "帮我", "我想知道", "想问下"]:
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix) :].strip(" ，。！？?；;:")

    match = re.search(r"(?:我)?对(?P<title>.+?)(?:的)?(?:评价|看法|评分|感受|印象|想法)", normalized)
    if match:
        entities, _ = _normalize_media_entities_and_filters(_split_media_entities(match.group("title")))
        if entities and not any(_looks_like_generic_media_scope(entity) for entity in entities):
            return entities

    match = re.search(r"^(?P<title>.+?)(?:的)?(?:对比|比较(?!高)|区别|差异)", normalized)
    if match:
        entities, _ = _normalize_media_entities_and_filters(_split_media_entities(match.group("title")))
        if entities:
            return entities

    match = re.search(r"^(?P<title>.+?)的(?:各个)?(?:主角|角色|剧情|介绍|评价|看法|分析|总结)", normalized)
    if match:
        entities, _ = _normalize_media_entities_and_filters(_split_media_entities(match.group("title")))
        if entities and not any(_looks_like_generic_media_scope(entity) for entity in entities):
            return entities

    match = re.search(r"^(?P<title>.+?)(?:这部|这个|这本|这套)?(?:电影|影片|片子|电视剧|剧集|剧|动漫|动画|番剧|漫画|小说|书)?呢$", normalized)
    if match:
        entities, _ = _normalize_media_entities_and_filters(_split_media_entities(match.group("title")))
        if entities and not any(_looks_like_generic_media_scope(entity) for entity in entities):
            return entities

    if any(token in normalized for token in ["对比", "比较", "区别", "差异", "评价", "看法", "评分"]):
        entities, _ = _normalize_media_entities_and_filters(_split_media_entities(normalized))
        if len(entities) >= 2 and not any(_looks_like_generic_media_scope(entity) for entity in entities):
            return entities

    entities, _ = _normalize_media_entities_and_filters(_extract_media_entities_from_local_titles(normalized))
    if entities and not any(_looks_like_generic_media_scope(entity) for entity in entities):
        return entities

    return []


def _extract_media_entity(query: str) -> str:
    entities = _extract_media_entities(query)
    return entities[0] if entities else ""


def _find_previous_trace_context(history: list[dict[str, str]], require_media_context: bool = False) -> dict[str, Any]:
    if get_trace_record is None:
        return {}
    for item in reversed(history or []):
        if not isinstance(item, dict):
            continue
        if str(item.get("role", "")).strip().lower() != "assistant":
            continue
        trace_id = str(item.get("trace_id", "") or "").strip()
        if not trace_id:
            continue
        try:
            record = get_trace_record(trace_id)
        except Exception:
            record = None
        if not isinstance(record, dict) or not record:
            continue
        if require_media_context:
            state = record.get("conversation_state_after") if isinstance(record.get("conversation_state_after"), dict) else {}
            if not _state_has_media_context(state):
                continue
        return record
    return {}


def _normalize_media_title_for_match(text: str) -> str:
    value = str(text or "").strip().lower()
    if not value:
        return ""
    value = re.sub(r"\s+", "", value)
    # Keep CJK, latin letters and digits for stable title matching.
    value = re.sub(r"[^\u4e00-\u9fffa-z0-9]", "", value)
    return value


def _strip_media_entity_boundary_terms(text: str) -> list[str]:
    raw = str(text or "").strip(" ，。！？?；;:：\"'“”‘’（）()")
    if not raw:
        return []
    candidates = [raw]
    boundary_terms = [
        "游戏",
        "galgame",
        "动漫",
        "动画",
        "番剧",
        "电影",
        "影片",
        "片子",
        "电视剧",
        "剧集",
        "漫画",
        "小说",
        "书",
        "音乐",
        "专辑",
        "歌曲",
    ]
    seen = {raw.casefold()}
    for term in boundary_terms:
        if raw.startswith(term) and len(raw) - len(term) >= 2:
            candidate = raw[len(term) :].strip(" ，。！？?；;:：\"'“”‘’（）()")
            if candidate and candidate.casefold() not in seen:
                seen.add(candidate.casefold())
                candidates.append(candidate)
        if raw.endswith(term) and len(raw) - len(term) >= 2:
            candidate = raw[: -len(term)].strip(" ，。！？?；;:：\"'“”‘’（）()")
            if candidate and candidate.casefold() not in seen:
                seen.add(candidate.casefold())
                candidates.append(candidate)
    return candidates


def _best_local_media_title_match(text: str) -> str:
    candidates = _extract_media_entities_from_local_titles(text)
    if not candidates:
        return ""
    normalized = _normalize_media_title_for_match(text)
    for title in candidates:
        if _normalize_media_title_for_match(title) == normalized:
            return title
    return candidates[0]


def _canonicalize_media_entity(entity: str) -> tuple[str, dict[str, list[str]]]:
    raw = str(entity or "").strip(" ，。！？?；;:：\"'\u201c\u201d\u2018\u2019（）()")
    if not raw:
        return "", {}
    inferred_filters = _infer_media_filters(raw)

    # --- entity resolver: multi-pass (exact / cross-language alias / prefix / substring) ---
    er_result = er_resolve_title(raw, min_confidence=0.5)
    if er_result:
        return er_result.canonical, inferred_filters

    # --- legacy substring scan ---
    direct_match = _best_local_media_title_match(raw)
    if direct_match:
        return direct_match, inferred_filters

    # --- stripped boundary terms ---
    for candidate in _strip_media_entity_boundary_terms(raw)[1:]:
        er_stripped = er_resolve_title(candidate, min_confidence=0.4)
        if er_stripped:
            return er_stripped.canonical, inferred_filters
        matched = _best_local_media_title_match(candidate)
        if matched:
            return matched, inferred_filters

    return raw, inferred_filters
def _normalize_media_entities_and_filters(
    entities: list[str],
    base_filters: dict[str, list[str]] | None = None,
) -> tuple[list[str], dict[str, list[str]]]:
    merged_filters = _normalize_media_filter_map(base_filters)
    normalized_entities: list[str] = []
    seen: set[str] = set()
    for item in entities:
        normalized_entity, inferred_filters = _canonicalize_media_entity(str(item).strip())
        for field, values in inferred_filters.items():
            _merge_filter_values(merged_filters, field, values)
        clean = normalized_entity or str(item).strip()
        key = _normalize_media_title_for_match(clean)
        if not clean or not key or key in seen:
            continue
        seen.add(key)
        normalized_entities.append(clean)
    return normalized_entities, merged_filters


def _media_title_match_boost(title: str, entity: str) -> float:
    t = _normalize_media_title_for_match(title)
    e = _normalize_media_title_for_match(entity)
    if not t or not e:
        return 0.0

    boost = 0.0
    if t == e:
        boost += 0.8
    elif t.startswith(e):
        suffix = t[len(e) :]
        boost += 0.62 if suffix and len(suffix) <= 8 else 0.45
    elif e in t:
        boost += 0.45

    # Penalize mismatched numeric sequel markers (e.g., 白色相簿 vs 白色相簿2).
    digits_e = "".join(re.findall(r"\d+", e))
    digits_t = "".join(re.findall(r"\d+", t))
    if digits_e and digits_e != digits_t:
        boost -= 0.55

    return boost


def _media_title_match_boost_any(title: str, entities: list[str]) -> float:
    if not entities:
        return 0.0
    return max((_media_title_match_boost(title, entity) for entity in entities), default=0.0)


def _title_matches_music_work_signature(
    title: str,
    work_signature: list[str],
    composer_hints: list[str],
    instrument_hints: list[str] | None = None,
    form_hints: list[str] | None = None,
    work_family_hints: list[str] | None = None,
) -> bool:
    normalized = _normalize_media_title_for_match(title)
    if not normalized:
        return False

    def _contains_any(tokens: list[str]) -> bool:
        for token in tokens:
            clean = _normalize_media_title_for_match(token)
            if clean and clean in normalized:
                return True
        return False

    def _contains_all(tokens: list[str]) -> bool:
        checks = [_normalize_media_title_for_match(token) for token in tokens if _normalize_media_title_for_match(token)]
        return bool(checks) and all(token in normalized for token in checks)

    composer_ok = _contains_any(composer_hints) if composer_hints else True
    if composer_hints and not composer_ok:
        # Cross-language composer identity bridge: compare resolver canonicals
        # from query hints and title-leading composer tokens.
        try:
            query_canonicals: set[str] = set()
            for hint in composer_hints:
                _res = er_resolve_creator(hint, min_confidence=0.5)
                if _res and str(_res.canonical or "").strip():
                    query_canonicals.add(str(_res.canonical).strip().casefold())

            title_candidates: list[str] = []
            raw_title = str(title or "").strip()
            if raw_title:
                leading = raw_title.split(":", 1)[0].split("：", 1)[0].strip()
                if leading:
                    title_candidates.append(leading)
                for token in re.split(r"[;&/&]| and ", leading):
                    clean = str(token or "").strip()
                    if clean and clean not in title_candidates:
                        title_candidates.append(clean)

            title_canonicals: set[str] = set()
            for candidate in title_candidates:
                _res = er_resolve_creator(candidate, min_confidence=0.5)
                if _res and str(_res.canonical or "").strip():
                    title_canonicals.add(str(_res.canonical).strip().casefold())

            composer_ok = bool(query_canonicals and title_canonicals and query_canonicals.intersection(title_canonicals))
        except Exception:
            composer_ok = False
    _instrument_hints = [str(item).strip() for item in (instrument_hints or []) if str(item).strip()]
    _form_hints = [str(item).strip() for item in (form_hints or []) if str(item).strip()]
    _work_family_hints = [str(item).strip() for item in (work_family_hints or []) if str(item).strip()]
    work_tokens = [str(item).strip() for item in work_signature if str(item).strip()]

    # Backward compatibility: if explicit buckets are missing, infer from signature.
    if not _instrument_hints:
        _instrument_hints = [
            token for token in work_tokens
            if is_instrument_alias(token)
        ]
    if not _form_hints:
        _form_hints = [
            token for token in work_tokens
            if is_form_alias(token)
        ]
    if not _work_family_hints:
        _work_family_hints = [
            token for token in work_tokens
            if is_work_family_alias(token)
        ]

    family_hit = _contains_any(_work_family_hints)
    instrument_hit = _contains_any(_instrument_hints) if _instrument_hints else False
    form_hit = _contains_any(_form_hints) if _form_hints else False
    if _instrument_hints and _form_hints:
        work_ok = family_hit or (instrument_hit and form_hit)
    elif _work_family_hints:
        work_ok = family_hit
    else:
        work_ok = _contains_any(work_tokens)

    # If specific numeric/opus markers are present in signature, require at
    # least one marker to match so broad families (e.g. piano concerto) do not
    # pull unrelated composers/works.
    def _is_specific_music_marker(token: str) -> bool:
        raw_token = str(token or "").strip().lower()
        normalized_token = _normalize_media_title_for_match(raw_token)
        if not raw_token and not normalized_token:
            return False
        if re.search(r"op\.?\s*\d+", raw_token):
            return True
        if re.search(r"no\.?\s*\d+", raw_token):
            return True
        if re.search(r"第\s*\d+", str(token or "")):
            return True
        if normalized_token.startswith("op") and any(ch.isdigit() for ch in normalized_token):
            return True
        if normalized_token.startswith("no") and any(ch.isdigit() for ch in normalized_token):
            return True
        return False

    specific_tokens = [tok for tok in work_tokens if _is_specific_music_marker(tok)]
    if work_ok and specific_tokens:
        work_ok = _contains_any(specific_tokens)

    return composer_ok and work_ok


def _filter_music_compare_rows(
    rows: list[dict[str, Any]],
    *,
    work_signature: list[str],
    composer_hints: list[str],
    instrument_hints: list[str],
    form_hints: list[str],
    work_family_hints: list[str],
) -> list[dict[str, Any]]:
    strict = [
        row
        for row in rows
        if _title_matches_music_work_signature(
            str(row.get("title") or ""),
            work_signature=work_signature,
            composer_hints=composer_hints,
            instrument_hints=instrument_hints,
            form_hints=form_hints,
            work_family_hints=work_family_hints,
        )
    ]
    if strict:
        return strict

    # Fallback for cross-language composer alias gaps:
    # keep strong work-family constraints but relax composer-title hit.
    relaxed = [
        row
        for row in rows
        if _title_matches_music_work_signature(
            str(row.get("title") or ""),
            work_signature=work_signature,
            composer_hints=[],
            instrument_hints=instrument_hints,
            form_hints=form_hints,
            work_family_hints=work_family_hints,
        )
    ]
    return relaxed


def _extract_music_work_hints(
    question: str,
    filters: dict[str, list[str]] | None = None,
) -> dict[str, list[str]]:
    text = str(question or "").strip()
    lowered = text.lower()
    normalized_filters = _normalize_media_filter_map(filters)

    media_type_values = [str(item).strip().lower() for item in normalized_filters.get("media_type", []) if str(item).strip()]
    filter_surface = " ".join(
        str(value)
        for key, values in normalized_filters.items()
        for value in ([key] + (values if isinstance(values, list) else []))
    )
    mixed_surface = f"{text} {filter_surface}".strip()
    mixed_lower = mixed_surface.lower()
    ontology_hints = collect_music_ontology_hints(mixed_surface)
    instrument_hints = [str(item).strip() for item in ontology_hints.get("instrument_hints", []) if str(item).strip()]
    form_hints = [str(item).strip() for item in ontology_hints.get("form_hints", []) if str(item).strip()]
    work_family_hints = [str(item).strip() for item in ontology_hints.get("work_family_hints", []) if str(item).strip()]
    work_family_keys = [str(item).strip() for item in ontology_hints.get("work_family_keys", []) if str(item).strip()]

    music_surface = (
        "music" in media_type_values
        or any(k in normalized_filters for k in ("乐器", "作品类型", "instrument", "work_type"))
        or bool(instrument_hints)
        or bool(form_hints)
        or bool(work_family_hints)
        or "听过" in text
    )
    if not music_surface:
        return {
            "composer_hints": [],
            "instrument_hints": [],
            "form_hints": [],
            "work_family_hints": [],
            "work_signature": [],
        }

    work_signature: list[str] = [*instrument_hints, *form_hints, *work_family_hints]
    no_match = re.search(r"(?:no\.?|number|第)\s*([0-9]{1,2})", mixed_lower)
    if no_match:
        idx = no_match.group(1)
        work_signature.extend([f"no.{idx}", f"no {idx}", f"第{idx}"])
    op_match = re.search(r"op\.?\s*(\d{1,3})", lowered)
    if op_match:
        op_no = op_match.group(1)
        work_signature.extend([f"op.{op_no}", f"op {op_no}"])

    composer_hints: list[str] = []
    seed_candidates: list[str] = []
    if "柴可夫斯基" in text:
        seed_candidates.append("柴可夫斯基")
    if "tchaikovsky" in lowered:
        seed_candidates.append("Tchaikovsky")

    for cn_name in re.findall(r"[\u4e00-\u9fff]{2,8}", text):
        if cn_name not in seed_candidates:
            seed_candidates.append(cn_name)
    for en_name in re.findall(r"[A-Za-z][A-Za-z\-\s]{2,32}", text):
        clean_en = str(en_name).strip()
        if clean_en and clean_en not in seed_candidates:
            seed_candidates.append(clean_en)

    for candidate in seed_candidates[:10]:
        resolved = er_resolve_creator(candidate, min_confidence=0.6)
        if resolved and str(resolved.canonical or "").strip():
            canonical = str(resolved.canonical).strip()
            if canonical not in composer_hints:
                composer_hints.append(canonical)
            # Enrich aliases from resolved creator works. In classical metadata,
            # author may be performer/conductor while composer appears in title.
            for _work in list(getattr(resolved, "works", []) or [])[:80]:
                try:
                    _author = str(getattr(_work, "author", "") or "").strip()
                    if _author and _author not in composer_hints:
                        composer_hints.append(_author)
                    _title = str(getattr(_work, "canonical", "") or "").strip()
                    if not _title:
                        _title = str(getattr(_work, "title", "") or "").strip()
                    if ":" in _title or "：" in _title:
                        _prefix = _title.split(":", 1)[0].split("：", 1)[0].strip()
                        if _prefix and len(_prefix) <= 40 and _prefix not in composer_hints:
                            composer_hints.append(_prefix)
                except Exception:
                    continue

    # Cross-language composer alias expansion via entity_resolver alias table.
    try:
        from nav_dashboard.web.services import entity_resolver as _er_mod  # noqa: PLC0415

        _norm = getattr(_er_mod, "_norm", None)
        _aliases = getattr(_er_mod, "_ALIASES", None)
        if callable(_norm) and isinstance(_aliases, dict):
            extra_aliases: list[str] = []
            for hint in list(composer_hints):
                key = _norm(hint)
                for alt in list(_aliases.get(key, []) or [])[:12]:
                    clean_alt = str(alt or "").strip()
                    if clean_alt:
                        extra_aliases.append(clean_alt)
            for alias in extra_aliases:
                if alias not in composer_hints:
                    composer_hints.append(alias)
    except Exception:
        pass

    # Extend composer hints from ontology aliases so maintainers can add names
    # without touching algorithm code.
    for alias in collect_composer_alias_hints(mixed_surface):
        if alias not in composer_hints:
            composer_hints.append(alias)

    # Title-driven alias discovery: resolve "composer + work_family" against local
    # titles, then mine the lead token before ':' as a canonical composer hint.
    for _composer in list(composer_hints)[:8]:
        for _family in list(work_family_hints)[:6]:
            _probe = f"{_composer} {_family}".strip()
            _resolved_title = er_resolve_title(_probe, min_confidence=0.45)
            if not _resolved_title:
                continue
            _canon_title = str(getattr(_resolved_title, "canonical", "") or "").strip()
            if not _canon_title:
                continue
            if ":" in _canon_title or "：" in _canon_title:
                _prefix = _canon_title.split(":", 1)[0].split("：", 1)[0].strip()
                if _prefix and len(_prefix) <= 40 and _prefix not in composer_hints:
                    composer_hints.append(_prefix)

    for token in composer_override_signature_tokens(composer_hints, work_family_keys):
        if token not in work_signature:
            work_signature.append(token)

    dedup_composers: list[str] = []
    seen_composers: set[str] = set()
    for item in composer_hints:
        key = str(item).strip().casefold()
        if not key or key in seen_composers:
            continue
        seen_composers.add(key)
        dedup_composers.append(str(item).strip())

    dedup_signature: list[str] = []
    seen_signature: set[str] = set()
    for item in work_signature:
        key = str(item).strip().casefold()
        if not key or key in seen_signature:
            continue
        seen_signature.add(key)
        dedup_signature.append(str(item).strip())

    return {
        "composer_hints": dedup_composers,
        "instrument_hints": [item for item in dict.fromkeys(instrument_hints) if str(item).strip()],
        "form_hints": [item for item in dict.fromkeys(form_hints) if str(item).strip()],
        "work_family_hints": [item for item in dict.fromkeys(work_family_hints) if str(item).strip()],
        "work_signature": dedup_signature,
    }


def _has_music_signature_filters(filters: dict[str, list[str]] | None) -> bool:
    normalized = _normalize_media_filter_map(filters)

    def _contains_alias(field: str, predicate) -> bool:
        values = [str(item).strip() for item in normalized.get(field, []) if str(item).strip()]
        return any(predicate(value) for value in values)

    has_instrument = _contains_alias("instrument", is_instrument_alias) or _contains_alias("work_type", is_instrument_alias)
    has_concerto = _contains_alias("work_type", is_form_alias) or _contains_alias("instrument", is_form_alias)
    return has_instrument and has_concerto


def _build_answer_focus_hints(question: str, tool_results: list[ToolExecution]) -> str:
    lines: list[str] = []
    normalized_question = str(question or "")
    wants_when = any(token in normalized_question for token in ["什么时候", "何时", "哪天", "哪一年", "观影时间", "时间"])
    wants_summary = any(token in normalized_question for token in ["剧情", "简介", "介绍", "讲了什么"])

    media_result = next((item for item in tool_results if item.tool == TOOL_QUERY_MEDIA), None)
    creator_result = next((item for item in tool_results if item.tool == TOOL_SEARCH_BY_CREATOR), None)
    tmdb_result = next((item for item in tool_results if item.tool == TOOL_SEARCH_TMDB), None)
    mediawiki_result = next((item for item in tool_results if item.tool in {TOOL_SEARCH_MEDIAWIKI, TOOL_PARSE_MEDIAWIKI, TOOL_EXPAND_MEDIAWIKI_CONCEPT}), None)

    # For creator collection queries, the primary evidence is from search_by_creator.
    if creator_result and isinstance(creator_result.data, dict):
        canonical = str(creator_result.data.get("canonical_creator") or "").strip()
        works = creator_result.data.get("results") if isinstance(creator_result.data.get("results"), list) else []
        if canonical and works:
            lines.append(f"本地创作者检索命中：「{canonical}」，共找到 {len(works)} 部作品，请基于这些作品列表作答。")
            lines.append("如无特殊说明，回答应列出本地库中的作品，外部 Wiki 信息作补充标注。")

    if media_result and isinstance(media_result.data, dict):
        exact_match = media_result.data.get("top_exact_match") if isinstance(media_result.data.get("top_exact_match"), dict) else None
        family_matches = media_result.data.get("top_family_matches") if isinstance(media_result.data.get("top_family_matches"), list) else []
        if exact_match:
            title = str(exact_match.get("title") or "").strip()
            date = str(exact_match.get("date") or "").strip()
            rating = exact_match.get("rating")
            review = str(exact_match.get("review") or "").strip()
            lines.append("本地知识库优先：如果命中明确媒体条目，先回答本地库中的观看日期、评分和个人短评，再补充外部信息。")
            if title:
                lines.append(f"本地精确命中标题：{title}")
            if wants_when and date:
                lines.append(f"本地记录的观看日期：{date}")
            if rating not in {None, ""}:
                lines.append(f"本地记录评分：{rating}")
            if review:
                lines.append(f"本地短评摘要：{_clip_text(review, 180)}")
        elif media_result.data.get("media_entities"):
            family_titles = [str(item.get("title") or "").strip() for item in family_matches if isinstance(item, dict) and str(item.get("title") or "").strip()]
            if family_titles:
                lines.append("本地媒体库命中了同一作品族的多个相关条目，优先围绕这些条目回答，不要跳到无关作品。")
                lines.append(f"同系列相关条目：{' / '.join(family_titles[:3])}")
            else:
                lines.append("如果本地媒体结果没有精确标题命中，不要把模糊匹配条目当作用户正在询问的那部作品。")

    if wants_summary and tmdb_result and isinstance(tmdb_result.data, dict):
        rows = tmdb_result.data.get("results", []) if isinstance(tmdb_result.data.get("results"), list) else []
        if rows:
            top = rows[0] if isinstance(rows[0], dict) else {}
            overview = str(top.get("overview") or "").strip()
            title = str(top.get("title") or "").strip()
            if overview:
                lines.append("外部参考仅作补充：TMDB 提供的是外部剧情简介，不属于你的本地知识库。")
                if title:
                    lines.append(f"TMDB 命中标题：{title}")
                lines.append(f"TMDB 简介摘要：{_clip_text(overview, 220)}")

    if mediawiki_result and isinstance(mediawiki_result.data, dict):
        rows = mediawiki_result.data.get("results", []) if isinstance(mediawiki_result.data.get("results"), list) else []
        if rows:
            _mw_first = rows[0] if isinstance(rows[0], dict) else {}
            _mw_extract = str(_mw_first.get("extract") or "").strip()
            _mw_title = str(_mw_first.get("display_title") or _mw_first.get("title") or "").strip()
            if len(_mw_extract) > 100:
                lines.append(
                    f"外部 Wiki 参考资料（标注为外部来源后可纳入正文）——「{_mw_title}」："
                    + _clip_text(_mw_extract, 400)
                )
            lines.append("若引用 Wiki，请明确标注为外部参考，不要表述成你的本地知识库内容。")

    return "\n".join(lines).strip()


def _tool_query_media_record(
    query: str,
    query_profile: dict[str, Any],
    trace_id: str = "",
    options: dict[str, Any] | None = None,
) -> ToolExecution:
    tool_options = dict(options) if isinstance(options, dict) else {}
    lookup_mode = str(tool_options.get("lookup_mode") or "general_lookup").strip() or "general_lookup"
    allow_downstream_entity_inference = bool(tool_options.get("allow_downstream_entity_inference"))
    source_query = str(query or "").strip()
    tool_query = str(tool_options.get("query_text") or source_query or "").strip()
    rewritten_query = _rewrite_media_query(tool_query)
    option_entities = tool_options.get("media_entities") if isinstance(tool_options.get("media_entities"), list) else []
    media_entities = [str(item).strip() for item in option_entities if str(item).strip()]
    if lookup_mode == "general_lookup" and allow_downstream_entity_inference and not media_entities:
        media_entities = _extract_media_entities(tool_query)
    selection_filters = _normalize_media_filter_map(tool_options.get("selection"))
    normalized_option_filters = selection_filters or _normalize_media_filter_map(tool_options.get("filters"))
    raw_option_filters = dict(normalized_option_filters)
    media_entities, inferred_filters = _normalize_media_entities_and_filters(media_entities, normalized_option_filters)
    composer_hints = [
        str(item).strip()
        for item in (tool_options.get("composer_hints") or [])
        if str(item).strip()
    ]
    work_signature = [
        str(item).strip()
        for item in (tool_options.get("work_signature") or [])
        if str(item).strip()
    ]
    instrument_hints = [
        str(item).strip()
        for item in (tool_options.get("instrument_hints") or [])
        if str(item).strip()
    ]
    form_hints = [
        str(item).strip()
        for item in (tool_options.get("form_hints") or [])
        if str(item).strip()
    ]
    work_family_hints = [
        str(item).strip()
        for item in (tool_options.get("work_family_hints") or [])
        if str(item).strip()
    ]
    query_class = str(tool_options.get("query_class") or "").strip()
    force_signature_lookup = (
        query_class == "music_work_versions_compare"
        and (bool(work_family_hints) or (bool(instrument_hints) and bool(form_hints)))
    )
    if query_class == "music_work_versions_compare":
        for _field in ("author",):
            normalized_option_filters.pop(_field, None)
            inferred_filters.pop(_field, None)
    # Resolve any unmatched entities as creators and augment author filter
    for _ent in list(media_entities):
        if not er_resolve_title(_ent, min_confidence=0.5):
            _cr = er_resolve_creator(_ent, min_confidence=0.7)
            if _cr:
                _merge_filter_values(inferred_filters, "author", [_cr.canonical])
    if query_class != "music_work_versions_compare":
        for _composer in composer_hints:
            _merge_filter_values(inferred_filters, "author", [_composer])
    # P1 fallback: filter_search arrived with empty entities AND no author filter
    # (e.g. router received lookup_mode=filter_search, entities=[], filters={}
    # because the LLM saw "creator collection" but didn't structurise the creator).
    # Try to extract the creator directly from the query text as a last resort.
    if lookup_mode == "filter_search" and not media_entities and not inferred_filters.get("author"):
        _cr_fallback = _extract_creator_from_collection_query(tool_query or source_query)
        if _cr_fallback:
            _merge_filter_values(inferred_filters, "author", [_cr_fallback.canonical])
    option_projection = _schema_adapter.project(inferred_filters, resolved_question=tool_query)
    inferred_filters = option_projection.filters
    extracted_entity = media_entities[0] if media_entities else ""
    if not inferred_filters and lookup_mode == "general_lookup":
        inferred_filters = _schema_adapter.project(_infer_media_filters(tool_query), resolved_question=tool_query).filters
    date_window = _date_window_from_state(tool_options)
    if not date_window and lookup_mode == "general_lookup":
        date_window = _parse_media_date_window(tool_query)
    # ── time_scope_type: route date constraint to the correct field ──────────
    # consumption_date (default) → filter on item's `date` field (when the user
    #   read/watched it); handled downstream by _matches_media_date_window.
    # publication_date → filter on item's `year` field (the work's release year);
    #   convert the date window to a year range added to the filter dict so the
    #   library search API applies it, then clear date_window to avoid double
    #   filtering on the consumption date.
    time_scope_type = str(tool_options.get("time_scope_type") or "").strip()
    if time_scope_type == "publication_date" and date_window:
        # Use range semantics (year_range=[start, end]) rather than enumerating
        # every year; _matches_filters in library_service handles this key.
        _start_year_str = str(date_window.get("start") or "")[:4]
        _end_year_str = str(date_window.get("end") or "")[:4]
        if _start_year_str.isdigit() and _end_year_str.isdigit():
            _merge_filter_values(inferred_filters, "year_range", [_start_year_str, _end_year_str])
        date_window = {}  # suppress consumption-date filtering
    ranking = tool_options.get("ranking") if isinstance(tool_options.get("ranking"), dict) else {}
    sort_preference = str(ranking.get("mode") or tool_options.get("sort") or "relevance").strip() or "relevance"
    if sort_preference == "relevance" and lookup_mode == "general_lookup":
        sort_preference = _infer_requested_sort(tool_query)
    mediawiki_concept = _get_cached_mediawiki_concept(tool_query)
    if mediawiki_concept is None and _is_abstract_media_concept_query(tool_query):
        concept_result = _tool_expand_mediawiki_concept(tool_query, trace_id)
        mediawiki_concept = concept_result.data if isinstance(concept_result.data, dict) else None

    skip_graph_expansion = lookup_mode == "filter_search"
    if skip_graph_expansion:
        graph_tool = ToolExecution(
            tool=TOOL_EXPAND_MEDIA_QUERY,
            status="skipped",
            summary="过滤型媒体查询跳过图谱扩展",
            data={"original": tool_query, "expanded": rewritten_query or tool_query, "constraints": {}},
        )
    elif TOOL_EXPAND_MEDIA_QUERY in get_available_boundary_tools():
        graph_tool = _tool_expand_media_query(tool_query)
    else:
        graph_tool = ToolExecution(
            tool=TOOL_EXPAND_MEDIA_QUERY,
            status="skipped",
            summary="媒体图谱扩展未启用",
            data={"original": tool_query, "expanded": rewritten_query or tool_query, "constraints": {}},
        )
    graph_data = graph_tool.data if isinstance(graph_tool.data, dict) else {}
    vector_query = _select_media_vector_query(
        tool_query,
        str(graph_data.get("expanded") or ""),
        extracted_entity,
        rewritten_query,
    )
    filters = {
        str(key): [str(value).strip() for value in values if str(value).strip()]
        for key, values in (graph_data.get("constraints") or {}).items()
        if isinstance(values, list)
    }
    for field, values in inferred_filters.items():
        _merge_filter_values(filters, field, values)
    if isinstance(mediawiki_concept, dict):
        for field, values in (mediawiki_concept.get("filters") or {}).items():
            if isinstance(values, list):
                _merge_filter_values(filters, str(field), [str(value).strip() for value in values if str(value).strip()])
    projected_filters = _schema_adapter.project(filters, resolved_question=tool_query)
    filters = projected_filters.filters
    filter_queries = _build_media_filter_queries(filters)

    keyword_queries: list[str] = []
    for entity in media_entities:
        if entity not in keyword_queries:
            keyword_queries.append(entity)
        entity_review_query = f"{entity} 评价"
        if entity_review_query not in keyword_queries:
            keyword_queries.append(entity_review_query)
    if extracted_entity and extracted_entity not in keyword_queries:
        keyword_queries.append(extracted_entity)
    if rewritten_query and rewritten_query not in keyword_queries:
        keyword_queries.append(rewritten_query)

    # ── Entity-resolver alias expansion (MediaWiki-independent) ─────────────
    # For each entity that resolves as a creator, inject the canonical
    # (library-stored) author name into both keyword and vector queries.
    # This covers cross-language aliases like 柴可夫斯基 → Tchaikovsky
    # without requiring MediaWiki to have run first.
    _resolver_canonicals: list[str] = []
    for _alias_ent in media_entities:
        _cr_exp = er_resolve_creator(_alias_ent, min_confidence=0.5)
        if _cr_exp and _cr_exp.canonical:
            _canon = _cr_exp.canonical.strip()
            if _canon and _canon not in keyword_queries:
                keyword_queries.append(_canon)
            if _canon not in _resolver_canonicals:
                _resolver_canonicals.append(_canon)

    if isinstance(mediawiki_concept, dict):
        aliases = mediawiki_concept.get("aliases", []) if isinstance(mediawiki_concept.get("aliases"), list) else []
        authors = mediawiki_concept.get("authors", []) if isinstance(mediawiki_concept.get("authors"), list) else []
        for alias in aliases:
            clean_alias = str(alias).strip()
            if clean_alias and clean_alias not in keyword_queries:
                keyword_queries.append(clean_alias)
        for author in authors:
            clean_author = str(author).strip()
            if clean_author and clean_author not in keyword_queries:
                keyword_queries.append(clean_author)
    for filter_query in filter_queries:
        if filter_query not in keyword_queries:
            keyword_queries.append(filter_query)
    for sig in work_signature:
        if sig and sig not in keyword_queries:
            keyword_queries.append(sig)
    for sig in instrument_hints:
        if sig and sig not in keyword_queries:
            keyword_queries.append(sig)
    for sig in form_hints:
        if sig and sig not in keyword_queries:
            keyword_queries.append(sig)
    for sig in work_family_hints:
        if sig and sig not in keyword_queries:
            keyword_queries.append(sig)
    for hint in composer_hints:
        if hint and hint not in keyword_queries:
            keyword_queries.append(hint)

    vector_queries: list[str] = []
    for entity in media_entities:
        if entity and entity not in vector_queries:
            vector_queries.append(entity)
    if not vector_queries and vector_query:
        vector_queries.append(vector_query)
    for _canon in _resolver_canonicals:
        if _canon and _canon not in vector_queries:
            vector_queries.append(_canon)
    if isinstance(mediawiki_concept, dict):
        for alias in mediawiki_concept.get("aliases", []) if isinstance(mediawiki_concept.get("aliases"), list) else []:
            clean_alias = str(alias).strip()
            if clean_alias and clean_alias not in vector_queries:
                vector_queries.append(clean_alias)
    for filter_query in filter_queries:
        if filter_query and filter_query not in vector_queries:
            vector_queries.append(filter_query)
    for sig in work_signature:
        if sig and sig not in vector_queries:
            vector_queries.append(sig)
    for sig in instrument_hints:
        if sig and sig not in vector_queries:
            vector_queries.append(sig)
    for sig in form_hints:
        if sig and sig not in vector_queries:
            vector_queries.append(sig)
    for sig in work_family_hints:
        if sig and sig not in vector_queries:
            vector_queries.append(sig)
    for hint in composer_hints:
        if hint and hint not in vector_queries:
            vector_queries.append(hint)

    if _needs_filter_only_media_lookup(tool_query, lookup_mode, media_entities, filters, date_window) and not force_signature_lookup:
        fallback_payload = _http_json(
            "POST",
            f"{LIBRARY_TRACKER_BASE}/api/library/search",
            payload={
                "query": "",
                "mode": "keyword",
                "limit": 500,
                "filters": filters,
            },
            headers={"X-Trace-Id": trace_id, "X-Trace-Stage": "agent.media.filter_window"},
        )
        fallback_rows = fallback_payload.get("results", []) if isinstance(fallback_payload, dict) else []
        repair_projection = maybe_retry_normalized_filters(raw_option_filters or filters, resolved_question=tool_query, result_count=len(fallback_rows))
        repair_applied = False
        if repair_projection is not None:
            retry_payload = _http_json(
                "POST",
                f"{LIBRARY_TRACKER_BASE}/api/library/search",
                payload={
                    "query": "",
                    "mode": "keyword",
                    "limit": 500,
                    "filters": repair_projection.filters,
                },
                headers={"X-Trace-Id": trace_id, "X-Trace-Stage": "agent.media.filter_window.retry"},
            )
            retry_rows = retry_payload.get("results", []) if isinstance(retry_payload, dict) else []
            if retry_rows:
                fallback_rows = retry_rows
                filters = repair_projection.filters
                repair_applied = True
        compact = [
            _shape_media_row(
                row,
                score=max(_safe_score(row.get("rating")), _safe_score(row.get("score"))),
                retrieval_mode="filter_only",
                retrieval_query="",
                matched_entities=media_entities,
            )
            for row in fallback_rows
            if isinstance(row, dict) and _matches_media_date_window(row, date_window)
        ]
        if query_class == "music_work_versions_compare" and (work_signature or composer_hints):
            compact = _filter_music_compare_rows(
                compact,
                work_signature=work_signature,
                composer_hints=composer_hints,
                instrument_hints=instrument_hints,
                form_hints=form_hints,
                work_family_hints=work_family_hints,
            )
        compact, validation = _apply_media_result_validator(compact, filters=filters, date_window=date_window)
        compact = _sort_media_results(compact, sort_preference)
        return ToolExecution(
            tool=TOOL_QUERY_MEDIA,
            status="ok",
            summary=(
                f"命中 {len(compact)} 条媒体记录"
                f"（lookup_mode={lookup_mode}, filter_only, sort={sort_preference}, date_window={json.dumps(date_window, ensure_ascii=False)}）"
            ),
            data={
                "trace_id": trace_id,
                "trace_stage": "agent.tool.query_media_record",
                "results": compact,
                "query_profile": query_profile,
                "lookup_mode": lookup_mode,
                "media_entities": media_entities,
                "top_exact_match": None,
                "graph_expansion": {
                    "status": graph_tool.status,
                    "summary": graph_tool.summary,
                    "expanded_query": vector_query,
                    "constraints": filters,
                },
                "retrieval_adapter": {
                    "schema_repairs": projected_filters.applied_repairs,
                    "option_repairs": option_projection.applied_repairs,
                    "retry_applied": repair_applied,
                },
                "mediawiki_concept": mediawiki_concept or {},
                "date_window": date_window,
                "sort": sort_preference,
                "candidate_source_breakdown": {
                    "local_filter_index": len(fallback_rows),
                    "local_keyword_index": 0,
                    "local_vector_index": 0,
                    "web": 0,
                },
                "validation": validation,
            },
        )

    keyword_rows: list[dict[str, Any]] = []
    for q_item in keyword_queries:
        payload = _http_json(
            "POST",
            f"{LIBRARY_TRACKER_BASE}/api/library/search",
            payload={
                "query": q_item,
                "mode": "keyword",
                "limit": 8,
                "filters": filters,
            },
            headers={"X-Trace-Id": trace_id, "X-Trace-Stage": "agent.media.keyword"},
        )
        current = payload.get("results", []) if isinstance(payload, dict) else []
        for row in current:
            if not isinstance(row, dict):
                continue
            cloned = dict(row)
            cloned["matched_query"] = q_item
            keyword_rows.append(cloned)

    vector_rows: list[dict[str, Any]] = []
    for q_item in vector_queries:
        vec_payload = _http_json(
            "POST",
            f"{LIBRARY_TRACKER_BASE}/api/library/search",
            payload={
                "query": q_item,
                "mode": "vector",
                "limit": 8,
                "filters": filters,
            },
            headers={"X-Trace-Id": trace_id, "X-Trace-Stage": "agent.media.vector"},
        )
        current = vec_payload.get("results", []) if isinstance(vec_payload, dict) else []
        for row in current:
            if not isinstance(row, dict):
                continue
            cloned = dict(row)
            cloned["matched_query"] = q_item
            vector_rows.append(cloned)
    candidate_source_breakdown = {
        "local_filter_index": 0,
        "local_keyword_index": len(keyword_rows),
        "local_vector_index": len(vector_rows),
        "web": 0,
    }
    graph_degrees = _media_graph_degrees()

    merged: dict[str, dict[str, Any]] = {}
    for item in vector_rows:
        key = str(item.get("id") or item.get("title") or "").strip()
        if not key:
            continue
        merged[key] = {
            **dict(item),
            "semantic_score": _safe_score(item.get("score")),
            "keyword_score": 0.0,
            "retrieval_mode": "vector",
        }

    for item in keyword_rows:
        key = str(item.get("id") or item.get("title") or "").strip()
        if not key:
            continue
        keyword_score = _safe_score(item.get("score"))
        current = merged.get(key)
        if current is None:
            if vector_rows and not media_entities and not extracted_entity:
                continue
            merged[key] = {
                **dict(item),
                "semantic_score": 0.0,
                "keyword_score": keyword_score,
                "retrieval_mode": "keyword",
            }
            continue
        if keyword_score > _safe_score(current.get("keyword_score")):
            current.update(dict(item))
            current["keyword_score"] = keyword_score
        if _safe_score(current.get("semantic_score")) > 0:
            current["retrieval_mode"] = "hybrid"

    used_mode = "vector"
    if vector_rows and keyword_rows:
        used_mode = "hybrid"
    elif keyword_rows and not vector_rows:
        used_mode = "keyword"

    max_keyword_score = max((_safe_score(item.get("keyword_score")) for item in merged.values()), default=0.0)
    max_graph_prior = max((math.log(graph_degrees.get(str(item.get("id") or "").strip(), 0) + 1.0) for item in merged.values()), default=0.0)

    compact: list[dict[str, Any]] = []
    signature_targets = [
        str(item).strip()
        for item in [*composer_hints, *instrument_hints, *form_hints, *work_family_hints, *work_signature]
        if str(item).strip()
    ]
    for item in merged.values():
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "")
        semantic_score = _safe_score(item.get("semantic_score"))
        keyword_score = _safe_score(item.get("keyword_score"))
        item_id = str(item.get("id") or "").strip()
        graph_prior_raw = math.log(graph_degrees.get(item_id, 0) + 1.0)
        keyword_norm = (keyword_score / max_keyword_score) if max_keyword_score > 0 else 0.0
        graph_prior = (graph_prior_raw / max_graph_prior) if max_graph_prior > 0 else 0.0
        title_targets = media_entities or [extracted_entity or rewritten_query or query]
        for target in signature_targets:
            if target not in title_targets:
                title_targets.append(target)
        title_boost = _media_title_match_boost_any(title, title_targets)
        score = (0.85 * semantic_score) + (0.10 * keyword_norm) + (0.05 * graph_prior) + title_boost
        compact.append(
            {
                "id": item.get("id"),
                "title": item.get("title"),
                "media_type": item.get("media_type"),
                "category": item.get("category"),
                "author": item.get("author"),
                "publisher": item.get("publisher"),
                "channel": item.get("channel"),
                "rating": item.get("rating"),
                "date": item.get("date"),
                "score": round(score, 6),
                "semantic_score": round(semantic_score, 6),
                "keyword_score": round(keyword_score, 6),
                "keyword_norm": round(keyword_norm, 6),
                "graph_prior": round(graph_prior, 6),
                "graph_prior_raw": round(graph_prior_raw, 6),
                "title_boost": round(title_boost, 6),
                "url": item.get("url"),
                "retrieval_mode": str(item.get("retrieval_mode") or used_mode),
                "retrieval_query": str(item.get("matched_query") or vector_query),
                "matched_entities": media_entities,
                "review": (str(item.get("review", ""))[:140] + "...") if str(item.get("review", "")) else "",
            }
        )
    compact = _sort_media_results(compact, sort_preference)
    if media_entities:
        compact = [row for row in compact if _safe_score(row.get("title_boost")) > 0]
    compact, validation = _apply_media_result_validator(compact, filters=filters, date_window=date_window)
    compact = _sort_media_results(compact, sort_preference)
    top_exact_match = None
    top_family_matches: list[dict[str, Any]] = []
    if media_entities:
        exact_matches = [
            row for row in compact
            if _media_title_match_boost_any(str(row.get("title") or ""), media_entities) >= 0.8
        ]
        if exact_matches:
            top_exact_match = exact_matches[0]
        top_family_matches = [
            row for row in compact
            if _media_title_match_boost_any(str(row.get("title") or ""), media_entities) >= 0.6
        ][:3]
    if not compact and filters:
        fallback_payload = _http_json(
            "POST",
            f"{LIBRARY_TRACKER_BASE}/api/library/search",
            payload={
                "query": "",
                "mode": "keyword",
                "limit": 8,
                "filters": filters,
            },
            headers={"X-Trace-Id": trace_id, "X-Trace-Stage": "agent.media.filter_fallback"},
        )
        fallback_rows = fallback_payload.get("results", []) if isinstance(fallback_payload, dict) else []
        for row in fallback_rows:
            if not isinstance(row, dict):
                continue
            if _matches_media_date_window(row, date_window):
                compact.append(
                    _shape_media_row(
                        row,
                        score=_safe_score(row.get("rating")),
                        retrieval_mode="filter_only",
                        retrieval_query="",
                        matched_entities=media_entities,
                    )
                )
    compact = _sort_media_results(compact, sort_preference)
    if query_class == "music_work_versions_compare" and (work_signature or composer_hints):
        compact = _filter_music_compare_rows(
            compact,
            work_signature=work_signature,
            composer_hints=composer_hints,
            instrument_hints=instrument_hints,
            form_hints=form_hints,
            work_family_hints=work_family_hints,
        )
    used_query = f"vector:{' || '.join(vector_queries or [vector_query])}"
    if keyword_queries:
        used_query += f" | keyword:{' || '.join(keyword_queries)}"
    if filters:
        used_query += f" | filters:{json.dumps(filters, ensure_ascii=False)}"
    return ToolExecution(
        tool=TOOL_QUERY_MEDIA,
        status="ok",
        summary=(
            f"命中 {len(compact)} 条媒体记录"
            f"（lookup_mode={lookup_mode}, entities={len(media_entities)}, mode={used_mode}, sort={sort_preference}, query={used_query}）"
        ),
        data={
            "trace_id": trace_id,
            "trace_stage": "agent.tool.query_media_record",
            "results": compact,
            "query_profile": query_profile,
            "lookup_mode": lookup_mode,
            "media_entities": media_entities,
            "top_exact_match": top_exact_match,
            "top_family_matches": top_family_matches,
            "graph_expansion": {
                "status": graph_tool.status,
                "summary": graph_tool.summary,
                "expanded_query": vector_query,
                "constraints": filters,
            },
            "mediawiki_concept": mediawiki_concept or {},
            "date_window": date_window,
            "sort": sort_preference,
            "candidate_source_breakdown": candidate_source_breakdown,
            "validation": validation,
        },
    )


def _tool_search_web(query: str, trace_id: str = "") -> ToolExecution:
    key = (TAVILY_API_KEY or "").strip()
    if not key:
        return ToolExecution(
            tool=TOOL_SEARCH_WEB,
            status="empty",
            summary="未配置 TAVILY_API_KEY",
            data={"trace_id": trace_id, "trace_stage": "agent.tool.search_web", "results": [], "cache_hit": False},
        )

    # Check web search cache before making an API call.
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
    # Cache the results for future queries.
    if compact:
        set_cached_web_results(query, 5, compact)
    return ToolExecution(
        tool=TOOL_SEARCH_WEB,
        status="ok",
        summary=f"命中 {len(compact)} 条网页结果",
        data={"trace_id": trace_id, "trace_stage": "agent.tool.search_web", "results": compact, "cache_hit": False},
    )


def _tool_expand_document_query(query: str) -> ToolExecution:
    """Expand document query using knowledge graph (ai_conversations_summary)."""
    result = run_doc_graph_expand(query)
    return ToolExecution(
        tool=TOOL_EXPAND_DOC_QUERY,
        status=result.status,
        summary=result.summary,
        data=result.data,
    )


def _tool_expand_media_query(query: str) -> ToolExecution:
    """Expand media query using library knowledge graph."""
    result = run_media_graph_expand(query)
    return ToolExecution(
        tool=TOOL_EXPAND_MEDIA_QUERY,
        status=result.status,
        summary=result.summary,
        data=result.data,
    )


def _execute_tool(call: PlannedToolCall, query_profile: dict[str, Any], trace_id: str) -> ToolExecution:
    import time as _time

    _tool_t0 = _time.perf_counter()
    try:
        if call.name == TOOL_QUERY_DOC_RAG:
            result = _tool_query_document_rag(call.query, query_profile, trace_id)
        elif call.name == TOOL_QUERY_MEDIA:
            result = _tool_query_media_record(call.query, query_profile, trace_id, call.options)
        elif call.name == TOOL_SEARCH_WEB:
            result = _tool_search_web(call.query, trace_id)
        elif call.name == TOOL_EXPAND_DOC_QUERY:
            result = _tool_expand_document_query(call.query)
        elif call.name == TOOL_EXPAND_MEDIA_QUERY:
            result = _tool_expand_media_query(call.query)
        elif call.name == TOOL_SEARCH_MEDIAWIKI:
            result = _tool_search_mediawiki_action(call.query, trace_id)
        elif call.name == TOOL_PARSE_MEDIAWIKI:
            result = _tool_parse_mediawiki_page(call.query, trace_id)
        elif call.name == TOOL_EXPAND_MEDIAWIKI_CONCEPT:
            result = _tool_expand_mediawiki_concept(call.query, trace_id)
        elif call.name == TOOL_SEARCH_TMDB:
            result = _tool_search_tmdb_media(call.query, trace_id)
        elif call.name == TOOL_SEARCH_BY_CREATOR:
            # call.options may carry creator_name (canonical) and media_type
            creator_name = str(call.options.get("creator_name") or call.query or "").strip()
            media_type = str(call.options.get("media_type") or "").strip()
            result = _tool_search_by_creator(creator_name, trace_id, media_type=media_type)
        else:
            result = ToolExecution(tool=call.name, status="skipped", summary="未知工具", data={})
        latency_ms = round((_time.perf_counter() - _tool_t0) * 1000, 1)
        data = dict(result.data) if isinstance(result.data, dict) else {}
        data.setdefault("latency_ms", latency_ms)
        return ToolExecution(tool=result.tool, status=result.status, summary=result.summary, data=data)
    except Exception as exc:  # noqa: BLE001
        latency_ms = round((_time.perf_counter() - _tool_t0) * 1000, 1)
        return ToolExecution(tool=call.name, status="error", summary=str(exc), data={"results": [], "latency_ms": latency_ms})


def _plan_tool_calls(
    question: str,
    history: list[dict[str, str]],
    backend: str,
    quota_state: dict[str, Any],
    search_mode: str,
) -> tuple[list[PlannedToolCall], AgentRuntimeState, dict[str, Any]]:
    query_profile = _resolve_query_profile(question)
    router_decision, llm_media, previous_context_state = _build_router_decision(question, history, quota_state, query_profile)
    query_classification = _router_decision_to_query_classification(router_decision, llm_media, previous_context_state, query_profile)
    query_classification["previous_date_range"] = previous_context_state.get("date_range", []) if isinstance(previous_context_state, dict) else []
    plan = RoutingPolicy().build_plan(router_decision, search_mode)
    planned = list(plan.planned_tools)
    context_resolution = _resolve_router_context(
        question,
        history,
        router_decision,
        previous_context_state,
        planned,
    )
    resolved_question = context_resolution.resolved_question
    runtime_state = AgentRuntimeState(
        decision=router_decision,
        execution_plan=plan,
        context_resolution=context_resolution,
        llm_media=llm_media,
        previous_context_state=dict(previous_context_state or {}),
    )
    for call in planned:
        if call.name == TOOL_SEARCH_TMDB and resolved_question and resolved_question != call.query:
            call.query = resolved_question

    fallback_evidence = _serialize_post_retrieval_assessment(runtime_state.post_retrieval_assessment)
    query_classification["fallback_evidence"] = fallback_evidence
    query_classification["doc_similarity"] = dict(fallback_evidence.get("doc_similarity") or {})
    query_classification["tech_score"] = float(runtime_state.post_retrieval_assessment.tech_score or 0.0)
    query_classification["weak_tech_signal"] = bool(runtime_state.post_retrieval_assessment.weak_tech_signal)
    query_classification["strong_tech_signal"] = router_decision.domain == "tech"

    query_classification["original_question"] = question
    query_classification["context_resolution"] = _serialize_router_context_resolution(context_resolution)
    query_classification["resolved_question"] = resolved_question
    query_classification["resolved_query_state"] = dict(context_resolution.resolved_query_state)
    query_classification["query_type"] = router_decision.query_type
    query_classification["query_class"] = router_decision.query_class
    query_classification["subject_scope"] = router_decision.subject_scope
    query_classification["time_scope_type"] = router_decision.time_scope_type
    query_classification["answer_shape"] = router_decision.answer_shape
    query_classification["media_family"] = router_decision.media_family
    query_classification["execution_plan"] = _serialize_execution_plan(plan)
    query_classification["conversation_state_before"] = dict(context_resolution.conversation_state_before)
    query_classification["detected_followup"] = bool(context_resolution.detected_followup)
    query_classification["inheritance_applied"] = dict(context_resolution.inheritance_applied)
    query_classification["conversation_state_after"] = dict(context_resolution.conversation_state_after)
    query_classification["state_diff"] = dict(context_resolution.state_diff)
    query_classification["planner_snapshot"] = dict(context_resolution.planner_snapshot)
    return planned, runtime_state, query_classification


def _execute_tool_plan(calls: list[PlannedToolCall], query_profile: dict[str, Any], trace_id: str) -> list[ToolExecution]:
    if not calls:
        return []
    prefetched = [call for call in calls if call.name == TOOL_EXPAND_MEDIAWIKI_CONCEPT]
    concurrent = [call for call in calls if call.name != TOOL_EXPAND_MEDIAWIKI_CONCEPT]
    results: list[ToolExecution] = []
    for call in prefetched:
        results.append(_execute_tool(call, query_profile, trace_id))
    if concurrent:
        with ThreadPoolExecutor(max_workers=max(1, len(concurrent))) as pool:
            future_map = {pool.submit(_execute_tool, call, query_profile, trace_id): call for call in concurrent}
            for future in as_completed(future_map):
                results.append(future.result())
    return results


# ── Per-item match helpers ────────────────────────────────────────────────────
_TITLE_STRIP_RE = re.compile(r"[《》「」『』【】（）()\s　]+")
_TITLE_YEAR_SUFFIX_RE = re.compile(r"\s*\(?[12]\d{3}\)?\s*$")
# Full-width ASCII normalization (ａ→a, Ａ→A, ０→0, …)
_FULLWIDTH_OFFSET_UPPER = ord("Ａ") - ord("A")
_FULLWIDTH_OFFSET_LOWER = ord("ａ") - ord("a")
_FULLWIDTH_OFFSET_DIGIT = ord("０") - ord("0")
_CJK_RANGE_RE = re.compile(r"[\u3040-\u9fff\uf900-\ufaff\u3400-\u4dbf]+")


def _normalize_fullwidth(text: str) -> str:
    """Convert full-width ASCII letters/digits to ASCII equivalents."""
    chars: list[str] = []
    for ch in text:
        cp = ord(ch)
        if 0xFF01 <= cp <= 0xFF5E:
            # Full-width forms: U+FF01..U+FF5E → U+0021..U+007E
            chars.append(chr(cp - 0xFEE0))
        else:
            chars.append(ch)
    return "".join(chars)


def _cjk_bigrams(text: str) -> set[str]:
    """Extract consecutive character bigrams from CJK runs in *text*.

    CJK scripts have no whitespace between words, so token-splitting is useless.
    Bigrams approximate a partial n-gram overlap for short CJK titles.

    Example: "进击的巨人" → {"进击", "击的", "的巨", "巨人"}
    """
    bigrams: set[str] = []
    for match in _CJK_RANGE_RE.finditer(text):
        run = match.group()
        for i in range(len(run) - 1):
            bigrams.append(run[i : i + 2])
    return set(bigrams)


def _normalize_title_for_match(title: str) -> str:
    """Normalise a title string for fuzzy equality checks.

    Strips CJK/ASCII brackets, whitespace, and trailing year suffixes, then
    lowercases.  Also normalises full-width ASCII characters.  The result is
    used for token-overlap comparison only — the original title is always
    presented to the user.
    """
    t = _normalize_fullwidth(title)
    t = _TITLE_STRIP_RE.sub(" ", t).strip()
    t = _TITLE_YEAR_SUFFIX_RE.sub("", t).strip()
    return t.lower()


def _title_similarity(a: str, b: str) -> float:
    """Return a similarity score ∈ [0.0, 1.0] between two normalised title strings.

    Uses CJK bigram Jaccard for titles containing CJK characters; falls back to
    plain token Jaccard for purely Latin titles.  Exact/substring matches short-
    circuit early.
    """
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    # Substring containment with length-ratio guard
    shorter, longer = sorted([a, b], key=len)
    if shorter in longer and len(shorter) / max(len(longer), 1) >= 0.6:
        return 0.85

    has_cjk_a = bool(_CJK_RANGE_RE.search(a))
    has_cjk_b = bool(_CJK_RANGE_RE.search(b))
    if has_cjk_a or has_cjk_b:
        # CJK bigram Jaccard
        bg_a = _cjk_bigrams(a)
        bg_b = _cjk_bigrams(b)
        # Also include ASCII tokens for hybrid titles (e.g. "Re:Zero")
        tok_a = set(re.findall(r"[a-z0-9]{2,}", a))
        tok_b = set(re.findall(r"[a-z0-9]{2,}", b))
        combined_a = bg_a | tok_a
        combined_b = bg_b | tok_b
        union = combined_a | combined_b
        if not union:
            return 0.0
        return round(len(combined_a & combined_b) / len(union), 3)
    else:
        # Latin-only: token Jaccard
        tok_a = set(a.split())
        tok_b = set(b.split())
        union = tok_a | tok_b
        if not union:
            return 0.0
        return round(len(tok_a & tok_b) / len(union), 3)


def _validate_per_item_tmdb_match(local_title: str, tmdb_row: dict[str, Any]) -> float:
    """Return a confidence score ∈ [0.0, 1.0] that *tmdb_row* matches *local_title*.

    Checks both the display title and the original title (romanised/JP) from TMDB
    against the local title using CJK-bigram-aware similarity.  The highest score
    across all alias candidates wins.

    Note: local_date is the *consumption* date, not the release year, so year
    comparison between local and TMDB is intentionally omitted to avoid false
    negatives for older titles watched recently.
    """
    if not local_title:
        return 0.0

    local_norm = _normalize_title_for_match(local_title)
    if not local_norm:
        return 0.0

    # Collect all name candidates from the TMDB row
    candidates_raw = [
        str(tmdb_row.get("title") or "").strip(),
        str(tmdb_row.get("original_title") or "").strip(),
        str(tmdb_row.get("name") or "").strip(),
        str(tmdb_row.get("original_name") or "").strip(),
    ]
    best = 0.0
    for cand in candidates_raw:
        if not cand:
            continue
        cand_norm = _normalize_title_for_match(cand)
        if not cand_norm:
            continue
        score = _title_similarity(local_norm, cand_norm)
        if score > best:
            best = score
    return round(best, 3)


def _validate_per_item_bangumi_match(local_title: str, bgm_row: dict[str, Any]) -> float:
    """Return a confidence score ∈ [0.0, 1.0] that *bgm_row* matches *local_title*.

    Bangumi provides both `name` (usually Japanese) and `name_cn` (Chinese), so
    we check both — especially useful when the local record stores the Chinese
    name while Bangumi's search returns the Japanese canonical name.
    """
    if not local_title:
        return 0.0

    local_norm = _normalize_title_for_match(local_title)
    if not local_norm:
        return 0.0

    candidates_raw = [
        str(bgm_row.get("name_cn") or "").strip(),   # Chinese name — check first
        str(bgm_row.get("name") or "").strip(),       # Japanese/canonical name
    ]
    best = 0.0
    for cand in candidates_raw:
        if not cand:
            continue
        cand_norm = _normalize_title_for_match(cand)
        if not cand_norm:
            continue
        score = _title_similarity(local_norm, cand_norm)
        if score > best:
            best = score
    return round(best, 3)


def _tool_search_bangumi(
    title: str,
    trace_id: str = "",
    *,
    subject_type: int = BANGUMI_SUBJECT_TYPE_ANIME,
    limit: int = 5,
) -> ToolExecution:
    """Search Bangumi (bgm.tv) for a subject by title.

    Uses POST /v0/search/subjects with an optional subject_type filter.
    Requires BANGUMI_ACCESS_TOKEN and a descriptive User-Agent per Bangumi policy.

    Returns a ToolExecution whose data["results"] contains compact subject dicts
    with fields: id, name (JP), name_cn (CN), summary, date, score, source.
    """
    if not BANGUMI_ACCESS_TOKEN:
        return ToolExecution(
            tool=TOOL_SEARCH_BANGUMI,
            status="empty",
            summary="未配置 Bangumi Access Token",
            data={"trace_id": trace_id, "trace_stage": "agent.tool.search_bangumi", "results": []},
        )

    url = f"{BANGUMI_API_BASE_URL}/v0/search/subjects?limit={limit}"
    headers = {
        "Authorization": f"Bearer {BANGUMI_ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        # Bangumi policy requires a descriptive User-Agent
        "User-Agent": "personal-ai-stack/1.0 (https://github.com/personal)",
    }
    if trace_id:
        headers["X-Trace-Id"] = trace_id

    body: dict[str, Any] = {"keyword": title, "sort": "match"}
    if subject_type:
        body["filter"] = {"type": [subject_type]}

    try:
        raw = _http_json("POST", url, payload=body, timeout=BANGUMI_TIMEOUT, headers=headers)
    except Exception as exc:  # noqa: BLE001
        return ToolExecution(
            tool=TOOL_SEARCH_BANGUMI,
            status="error",
            summary=f"Bangumi 搜索失败：{exc}",
            data={"trace_id": trace_id, "trace_stage": "agent.tool.search_bangumi", "results": []},
        )

    rows = raw.get("data") or [] if isinstance(raw, dict) else []
    compact: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        bgm_id = row.get("id")
        name = str(row.get("name") or "").strip()
        name_cn = str(row.get("name_cn") or "").strip()
        if not name and not name_cn:
            continue
        rating_block = row.get("rating") or {}
        score = float(rating_block.get("score") or 0.0) if isinstance(rating_block, dict) else 0.0
        compact.append({
            "id": bgm_id,
            "name": name,           # Japanese/canonical name
            "name_cn": name_cn,     # Chinese name
            "title": name_cn or name,  # display title
            "summary": _clip_text(str(row.get("summary") or ""), 320),
            "date": str(row.get("date") or "").strip(),
            "score": score,
            "type": row.get("type"),
            "source": "bangumi",
            "url": f"https://bgm.tv/subject/{bgm_id}" if bgm_id else "",
        })

    compact.sort(key=lambda item: float(item.get("score") or 0.0), reverse=True)

    return ToolExecution(
        tool=TOOL_SEARCH_BANGUMI,
        status="ok" if compact else "empty",
        summary=f"Bangumi 命中 {len(compact)} 条结果（keyword={title!r}）",
        data={
            "trace_id": trace_id,
            "trace_stage": "agent.tool.search_bangumi",
            "results": compact,
        },
    )


def _execute_per_item_expansion(
    tool_results: list[ToolExecution],
    *,
    trace_id: str,
    media_family: str = "",
) -> list[ToolExecution]:
    """Phase-2 generic per-item fan-out enrichment.

    Called after local results are finalized.  For each title in the local
    media result set, fetches external background data from the most suitable
    source given *media_family*:

      audiovisual  → Bangumi (anime) or TMDB; tries Bangumi first when the token
                     is available, then falls back to TMDB if Bangumi returns no
                     confident match.  For non-anime titles TMDB is tried first.
      bookish      → MediaWiki parse per-title
      music        → MediaWiki parse per-title
      game         → MediaWiki parse per-title
      ""           → MediaWiki parse (safe default)

    Match confidence is validated using CJK-bigram-aware _title_similarity; any
    candidate scoring below the relevant MIN_CONFIDENCE constant is discarded so
    wrong-item overviews never reach the LLM.

    The enriched data is provided in two complementary forms:
      per_item_data  — structured list of {local_title, local_date, local_rating,
                        local_review, external_title, external_overview,
                        external_source, match_confidence} dicts, one per item
                        that successfully resolved.  The composer and LLM prompt
                        use this for bucketed, deterministic rendering.
      results        — raw external rows (backward-compatible field for
                        _format_tool_result / fallback rendering).
      per_item_fanout_stats — lightweight list of {local_title, external_title,
                        match_confidence, source} for trace reporting.

    Returns *tool_results* unchanged if no enrichable local rows exist or if
    the fan-out produced zero results.
    """
    media_result = next(
        (r for r in tool_results if r.tool == TOOL_QUERY_MEDIA and r.status in {"ok", "partial"}),
        None,
    )
    if media_result is None or not isinstance(media_result.data, dict):
        return tool_results

    local_rows = [r for r in (media_result.data.get("results") or []) if isinstance(r, dict)]
    if not local_rows:
        return tool_results

    sample = local_rows[:PER_ITEM_EXPAND_LIMIT]

    _is_audiovisual = media_family == "audiovisual"
    _use_bangumi = _is_audiovisual and bool(BANGUMI_ACCESS_TOKEN)
    _use_tmdb = _is_audiovisual and bool(TMDB_API_KEY or TMDB_READ_ACCESS_TOKEN)

    def _fetch_bangumi(row: dict[str, Any]) -> dict[str, Any] | None:
        """Try Bangumi first (anime type), then type-agnostic if no hit."""
        title = str(row.get("title") or "").strip()
        if not title:
            return None
        # Try anime type first, then real (type=6) as fallback for live-action
        for subject_type in (BANGUMI_SUBJECT_TYPE_ANIME, BANGUMI_SUBJECT_TYPE_REAL):
            try:
                result = _tool_search_bangumi(title, trace_id, subject_type=subject_type)
            except Exception:  # noqa: BLE001
                continue
            if result.status not in {"ok", "partial"} or not isinstance(result.data, dict):
                continue
            bgm_rows = result.data.get("results") or []
            for candidate in (bgm_rows[:3] if isinstance(bgm_rows, list) else []):
                if not isinstance(candidate, dict):
                    continue
                confidence = _validate_per_item_bangumi_match(title, candidate)
                if confidence >= PER_ITEM_BANGUMI_MIN_CONFIDENCE:
                    ext_title = str(candidate.get("name_cn") or candidate.get("name") or "").strip()
                    ext_overview = _clip_text(str(candidate.get("summary") or ""), 200)
                    return {
                        "local_title": title,
                        "local_date": str(row.get("date") or "").strip(),
                        "local_rating": row.get("rating"),
                        "local_review": _clip_text(str(row.get("review") or ""), 120),
                        "external_title": ext_title,
                        "external_overview": ext_overview,
                        "external_source": "bangumi",
                        "match_confidence": confidence,
                        "id": candidate.get("id"),
                        "url": str(candidate.get("url") or "").strip(),
                        "score": candidate.get("score", confidence),
                        # Backward-compat
                        "title": ext_title,
                        "overview": ext_overview,
                        "_source_title": title,
                    }
        return None

    def _fetch_tmdb(row: dict[str, Any]) -> dict[str, Any] | None:
        title = str(row.get("title") or "").strip()
        if not title:
            return None
        try:
            result = _tool_search_tmdb_media(title, trace_id)
        except Exception:  # noqa: BLE001
            return None
        if result.status not in {"ok", "partial"} or not isinstance(result.data, dict):
            return None
        tmdb_rows = result.data.get("results") or []
        for candidate in (tmdb_rows[:3] if isinstance(tmdb_rows, list) else []):
            if not isinstance(candidate, dict):
                continue
            confidence = _validate_per_item_tmdb_match(title, candidate)
            if confidence >= PER_ITEM_TMDB_MIN_CONFIDENCE:
                ext_title = str(candidate.get("title") or "").strip()
                ext_overview = _clip_text(str(candidate.get("overview") or ""), 200)
                return {
                    "local_title": title,
                    "local_date": str(row.get("date") or "").strip(),
                    "local_rating": row.get("rating"),
                    "local_review": _clip_text(str(row.get("review") or ""), 120),
                    "external_title": ext_title,
                    "external_overview": ext_overview,
                    "external_source": "tmdb",
                    "match_confidence": confidence,
                    "id": candidate.get("id"),
                    "url": str(candidate.get("url") or "").strip(),
                    "score": candidate.get("score", confidence),
                    # Keep raw fields for backward-compat rendering
                    "title": ext_title,
                    "overview": ext_overview,
                    "media_type": str(candidate.get("media_type") or "").strip(),
                    "date": str(candidate.get("date") or "").strip(),
                    "_source_title": title,
                }
        return None

    def _fetch_wiki(row: dict[str, Any]) -> dict[str, Any] | None:
        title = str(row.get("title") or "").strip()
        if not title:
            return None
        try:
            result = _tool_parse_mediawiki_page(title, trace_id)
        except Exception:  # noqa: BLE001
            return None
        if result.status not in {"ok", "partial"} or not isinstance(result.data, dict):
            return None
        page = result.data.get("page")
        if not isinstance(page, dict):
            # Fallback: try search-then-first-result
            try:
                search_res = _tool_search_mediawiki_action(title, trace_id, limit=2)
            except Exception:  # noqa: BLE001
                return None
            if not isinstance(search_res.data, dict):
                return None
            sr_rows = search_res.data.get("results") or []
            top = next((r for r in sr_rows if isinstance(r, dict)), None)
            if not top:
                return None
            page = top
        ext_title = str(page.get("display_title") or page.get("title") or "").strip()
        extract = _clip_text(str(page.get("extract") or page.get("snippet") or ""), 200)
        if not ext_title and not extract:
            return None
        conf = _title_similarity(
            _normalize_title_for_match(title),
            _normalize_title_for_match(ext_title),
        ) if ext_title else 0.7
        return {
            "local_title": title,
            "local_date": str(row.get("date") or "").strip(),
            "local_rating": row.get("rating"),
            "local_review": _clip_text(str(row.get("review") or ""), 120),
            "external_title": ext_title,
            "external_overview": extract,
            "external_source": "wiki",
            "match_confidence": max(conf, 0.5),
            "url": str(page.get("url") or "").strip(),
            "score": max(conf, 0.5),
            # Backward-compat fields
            "title": ext_title,
            "overview": extract,
            "_source_title": title,
        }

    def _fetch_audiovisual(row: dict[str, Any]) -> dict[str, Any] | None:
        """For audiovisual: try Bangumi first, fall back to TMDB."""
        if _use_bangumi:
            result = _fetch_bangumi(row)
            if result is not None:
                return result
        if _use_tmdb:
            return _fetch_tmdb(row)
        return None

    fetch_fn = _fetch_audiovisual if _is_audiovisual else _fetch_wiki

    per_item_data: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=min(PER_ITEM_EXPAND_LIMIT, len(sample))) as pool:
        futures = {pool.submit(fetch_fn, row): row for row in sample}
        for future in as_completed(futures):
            item = future.result()
            if item is not None:
                per_item_data.append(item)

    if not per_item_data:
        return tool_results

    # Determine the dominant source for naming the ToolExecution
    source_counts: dict[str, int] = {}
    for item in per_item_data:
        src = str(item.get("external_source") or "wiki")
        source_counts[src] = source_counts.get(src, 0) + 1
    external_source_label = max(source_counts, key=source_counts.__getitem__) if source_counts else "wiki"
    mixed_sources = len(source_counts) > 1

    if external_source_label == "bangumi":
        fanout_tool = TOOL_SEARCH_BANGUMI
        source_display = "Bangumi"
    elif external_source_label == "tmdb":
        fanout_tool = TOOL_SEARCH_TMDB
        source_display = "TMDB"
    else:
        fanout_tool = TOOL_EXPAND_MEDIAWIKI_CONCEPT
        source_display = "Wiki"

    # Lightweight per-item stats for trace reporting
    per_item_fanout_stats = [
        {
            "local_title": item.get("local_title", ""),
            "external_title": item.get("external_title", ""),
            "match_confidence": item.get("match_confidence", 0.0),
            "source": item.get("external_source", ""),
        }
        for item in per_item_data
    ]

    fanout_exec = ToolExecution(
        tool=fanout_tool,
        status="ok",
        summary=(
            f"{source_display} 逐项补充 "
            f"{len(per_item_data)}/{len(sample)} 条（per-item fan-out, source={external_source_label}"
            f"{', mixed' if mixed_sources else ''}）"
        ),
        data={
            "trace_id": trace_id,
            "trace_stage": f"agent.tool.per_item_fanout.{external_source_label}",
            "results": per_item_data,  # backward-compat: raw rows
            "per_item_fanout": True,
            "per_item_data": per_item_data,  # structured buckets for composer
            "per_item_source": external_source_label,
            "per_item_sources": sorted(source_counts.keys()),
            "source_counts": dict(source_counts),
            "mixed_sources": mixed_sources,
            "source_title_count": len(sample),
            "per_item_fanout_stats": per_item_fanout_stats,
        },
    )

    # Replace any prior whole-query result for the same tool; insert right after
    # the local media result for coherent ordering.
    updated = [r for r in tool_results if r.tool != fanout_tool]
    media_idx = next(
        (i for i, r in enumerate(updated) if r.tool == TOOL_QUERY_MEDIA),
        len(updated) - 1,
    )
    updated.insert(media_idx + 1, fanout_exec)
    return updated


def _quota_exceeded(plan: list[PlannedToolCall], backend: str, quota_state: dict[str, Any]) -> list[dict[str, Any]]:
    exceeded: list[dict[str, Any]] = []
    web_needed = sum(1 for c in plan if c.name == TOOL_SEARCH_WEB)
    if web_needed > 0:
        now_count = int(quota_state.get("web_search", 0) or 0)
        if now_count + web_needed > WEB_SEARCH_DAILY_LIMIT:
            exceeded.append(
                {
                    "kind": "web_search",
                    "current": now_count,
                    "add": web_needed,
                    "limit": WEB_SEARCH_DAILY_LIMIT,
                }
            )

    if (backend or "local").strip().lower() == "deepseek":
        # Planning + final synthesis at least 2 calls.
        deepseek_needed = 2
        now_deepseek = int(quota_state.get("deepseek", 0) or 0)
        if now_deepseek + deepseek_needed > DEEPSEEK_DAILY_LIMIT:
            exceeded.append(
                {
                    "kind": "deepseek",
                    "current": now_deepseek,
                    "add": deepseek_needed,
                    "limit": DEEPSEEK_DAILY_LIMIT,
                }
            )

    return exceeded


def _summarize_answer(
    *,
    question: str,
    history: list[dict[str, str]],
    memory_context: str,
    tool_results: list[ToolExecution],
    backend: str,
    search_mode: str,
    quota_state: dict[str, Any],
    trace_id: str,
    debug_sink: dict[str, Any] | None = None,
    llm_stats_sink: dict[str, Any] | None = None,
    answer_strategy: "_AnswerStrategy | None" = None,
) -> str:
    hist_lines = _trim_history_for_prompt(history)
    clipped_memory_context = _clip_memory_context(memory_context)
    answer_focus_hints = _build_answer_focus_hints(question, tool_results)

    normalized_search_mode = _normalize_search_mode(search_mode)
    has_web_tool = any(result.tool == TOOL_SEARCH_WEB for result in tool_results)

    # ── Base system prompt ─────────────────────────────────────────
    system_prompt = (
        "你是个人助理。请综合工具结果回答用户问题。"
        "如果某个工具失败/为空，明确说明并尽量用其它工具补足。"
        "回答使用中文，结构清晰，避免编造。"
        "只允许使用工具结果中的事实；如果工具未给出证据必须明确说不确定。"
        "遇到同名/近似作品时优先按标题精确匹配（例如含数字续作）。"
    )

    # ── ResponseComposer: propagate response_structure + evidence_policy ──
    # answer_strategy carries structured signals from AnswerPolicy.  Use them
    # to give the LLM explicit section-layout instructions so it knows how to
    # organise multi-source results (local-first, then labelled external).
    if answer_strategy is not None:
        _style = answer_strategy.style_hints
        _response_structure = str(_style.get("response_structure") or "")
        _ep: dict[str, Any] = _style.get("evidence_policy") or {}
        if _response_structure == "local_list_plus_external_background":
            system_prompt += (
                "请先列出本地库中检索到的所有相关作品（每条带：标题、观看/阅读日期、评分、短评），"
                "列表结束后另起一段，以「外部补充」为标题，"
                "补充创作者背景或该类型作品的整体特点（来自 Wiki / TMDB）。"
            )
        elif _response_structure == "local_record_plus_external_info":
            system_prompt += (
                "请先基于本地库记录回答（观看/阅读日期、评分、个人短评），"
                "再另起段落，以「外部参考」为标题，"
                "补充该作品的外部简介或创作背景（来自 TMDB / Wiki）。"
            )
        elif _response_structure == "thematic_list":
            system_prompt += (
                "请按主题或类型特征组织作品列表，结合本地库条目与外部资料，"
                "说明这类作品的共同特征，每部作品标注本地库中的评分/日期（如有）。"
            )
        elif _response_structure == "compare":
            system_prompt += (
                "用户要求对比多部作品。请为每部作品分别列出本地库记录（评分、日期、短评），"
                "然后给出横向对比总结（如：共同点、差异点、推荐顺序）。"
                "如有外部资料（TMDB / Wiki），可在对比表格或段落中注明来源。"
            )
        elif _response_structure == "local_list":
            system_prompt += (
                "请只列出本地库中的相关作品（标题、日期、评分、短评），不要引用外部文章或网络资料。"
            )
        if _ep.get("must_label_external"):
            system_prompt += (
                "所有来自外部资料（TMDB / Wiki / 网络）的内容必须用「外部参考」标注，"
                "与本地库内容明确区分。"
            )
        if _ep.get("must_weaken_uncertain_claims"):
            system_prompt += (
                "对未在工具结果中得到验证的事实，请用“可能”“据外部资料”等措辞表达不确定性，"
                "不要以肯定口吗断言。"
            )
        # ── subject_scope: personal_record frames local data as primary truth ──
        _subject_scope = _style.get("subject_scope") or ""
        if _subject_scope == "personal_record":
            system_prompt += (
                "这是个人观看/阅读记录类查询。请以本地库记录（日期、评分、个人短评）为主要依据，"
                "外部参考（TMDB / Wiki）仅作补充标注，不要用外部资料覆盖或动摇本地记录。"
            )
        # ── needs_expansion: warn when expansion data is missing ──────
        _needs_expansion_hint = bool(_style.get("needs_expansion"))
        _expansion_unavailable = bool(_style.get("expansion_unavailable"))
        _expansion_missing = bool(_style.get("expansion_missing"))
        if _expansion_unavailable:
            # All known expansion avenues exhausted — no repair path remains.
            system_prompt += (
                "用户希望对每部作品展开介绍。外部手册（TMDB / Wiki）已尝试装载但未返回有效内容。"
                "请在正文首尾明确告知用户详细外部信息暂时无法获取，"
                "仅基于本地库记录回答，并建议用户在外部平台（如 TMDB、豆瓣）查询详细介绍。"
            )
        elif _expansion_missing:
            # Expansion tools were not scheduled at all (T1).
            system_prompt += (
                "用户希望对每部作品展开介绍，但本次未调用外部扩展工具。"
                "请在本地库记录范围内尽可能展开说明，并明确告知用户详细介绍暂不可用。"
            )
        elif _needs_expansion_hint:
            system_prompt += (
                "用户希望对每部作品展开介绍，但外部扩展工具（TMDB / Wiki）未返回有效内容。"
                "请在本地库记录范围内尽可能展开说明，并明确告知用户详细外部信息暂不可用。"
            )

    # ── Per-item fan-out enrichment: when per-item data was fetched per title, ──
    # instruct the LLM to use the pre-bucketed blocks for per-item narration.
    _fanout_result = next(
        (r for r in tool_results
         if isinstance(r.data, dict) and r.data.get("per_item_fanout")),
        None,
    )
    if _fanout_result is not None:
        _fanout_source = str((_fanout_result.data or {}).get("per_item_source") or "external")
        _fanout_label = "TMDB" if _fanout_source == "tmdb" else "Wiki"
        system_prompt += (
            f"用户询问集合内各作品的具体内容。上方「已整理的逐项资料」中每个「## 《标题》」块"
            f"已经包含该作品的本地记录（观看日期/评分/短评）和来自 {_fanout_label} 的剧情简介。"
            "请直接按这些块逐项列出，每个作品一个条目："
            "先写本地记录信息，再在该条目内附上「剧情/简介（外部参考）」。"
            "如果某个作品没有外部简介，只列本地记录，无需额外说明。"
            "不要把所有外部简介集中写在一段——必须每部作品各自内联展示。"
        )

    if normalized_search_mode == "local_only" or not has_web_tool:
        system_prompt += "本轮未执行联网搜索，严禁写出“联网搜索”“网络搜索”“进行网络搜索”“经过搜索”等表述，也不要假装调用过外部 API。"

    # Pre-assemble structured data blocks so the LLM polishes, not discovers.
    _composed = _compose_response_sections(tool_results, answer_strategy)

    budgets = [PROMPT_TOOL_CONTEXT_MAX_CHARS, PROMPT_TOOL_CONTEXT_RETRY_CHARS, 1800]
    answer = ""
    last_exc: Exception | None = None
    total_calls = 0
    total_input_tokens = 0
    total_prompt_tokens = 0
    total_context_tokens = 0
    final_debug_request: dict[str, Any] | None = None

    for budget in budgets:
        context_parts = _build_tool_context_parts(tool_results, max_total_chars=budget)
        prompt_blocks = hist_lines + [f"当前问题: {question}"]
        if clipped_memory_context:
            prompt_blocks.extend(["", clipped_memory_context])
        if answer_focus_hints:
            prompt_blocks.extend(["", "回答提示：", answer_focus_hints])
        # Inject pre-structured data blocks when the answer policy set a layout.
        # This lets the LLM polish pre-assembled structure rather than extract
        # it from raw tool JSON, making the response more deterministic.
        _per_item_buckets = _composed.get("per_item_buckets") or []
        if _per_item_buckets:
            # The bucket blocks already contain the local records plus external
            # overviews for each title. Re-including raw media/fanout tool JSON
            # would duplicate the same information and blow up prompt tokens.
            compact_tool_results = [
                item
                for item in tool_results
                if item.tool not in {
                    TOOL_QUERY_MEDIA,
                    TOOL_SEARCH_BY_CREATOR,
                    TOOL_SEARCH_TMDB,
                    TOOL_SEARCH_BANGUMI,
                    TOOL_EXPAND_MEDIAWIKI_CONCEPT,
                    TOOL_SEARCH_MEDIAWIKI,
                    TOOL_PARSE_MEDIAWIKI,
                }
            ]
            context_parts = _build_tool_context_parts(compact_tool_results, max_total_chars=budget) if compact_tool_results else []
            # Bucketed per-item data: each bucket is a self-contained block
            # containing local record fields + external overview for one item.
            # The LLM renders each block as a list entry without needing to
            # cross-reference two separate flat lists.
            prompt_blocks.extend(["", "[已整理的逐项资料（每个作品一个块）]"])
            prompt_blocks.extend(["外部信息来源标注说明：Bangumi=bgm.tv动画数据库，TMDB=影视数据库，Wiki=维基百科。"])
            prompt_blocks.extend(["注意：标注【外部信息可能非精确匹配】的条目请谨慎使用其外部简介。"])
            prompt_blocks.extend(["", "[工具摘要]"])
            prompt_blocks.extend(_build_compact_tool_summary_lines(tool_results))
            prompt_blocks.extend(_per_item_buckets)
        else:
            if _composed["local_lines"]:
                prompt_blocks.extend(["", "[已整理的本地记录]"])
                prompt_blocks.extend(_composed["local_lines"])
            if _composed["external_lines"]:
                prompt_blocks.extend(["", "[已整理的外部资料]"])
                prompt_blocks.extend(_composed["external_lines"])
        prompt_blocks.extend(["", *context_parts])
        user_prompt = "\n".join(prompt_blocks)
        context_tokens_est = _approx_tokens("\n".join(context_parts))
        input_tokens_est = _approx_tokens(system_prompt) + _approx_tokens(user_prompt)
        prompt_tokens_est = max(0, input_tokens_est - context_tokens_est)
        final_debug_request = {
            "trace_id": trace_id,
            "trace_stage": "agent.llm.summarize",
            "backend": backend,
            "system_prompt": system_prompt,
            "user_prompt": user_prompt,
            "memory_tokens_est": _approx_tokens(clipped_memory_context),
            "input_tokens_est": input_tokens_est,
            "prompt_tokens_est": prompt_tokens_est,
            "context_tokens_est": context_tokens_est,
            "tool_context_budget_chars": budget,
        }
        try:
            total_calls += 1
            total_input_tokens += input_tokens_est
            total_prompt_tokens += prompt_tokens_est
            total_context_tokens += context_tokens_est
            answer = _llm_chat(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                backend=backend,
                quota_state=quota_state,
            )
            if normalized_search_mode == "local_only" and not has_web_tool and re.search(r"联网搜索|网络搜索|进行网络搜索|经过搜索", answer):
                total_calls += 1
                total_input_tokens += input_tokens_est
                total_prompt_tokens += prompt_tokens_est
                total_context_tokens += context_tokens_est
                answer = _llm_chat(
                    messages=[
                        {
                            "role": "system",
                            "content": system_prompt + "你刚才错误声称做了联网搜索。现在请仅基于当前工具结果重写答案，不要提到网络或搜索引擎。",
                        },
                        {"role": "user", "content": user_prompt},
                    ],
                    backend=backend,
                    quota_state=quota_state,
                )
            last_exc = None
            break
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if _is_context_length_error(exc) and budget != budgets[-1]:
                continue
            raise

    if last_exc is not None:
        raise last_exc

    if debug_sink is not None and final_debug_request is not None:
        debug_sink["llm_request"] = final_debug_request
    if llm_stats_sink is not None:
        llm_stats_sink["backend"] = backend
        llm_stats_sink["input_tokens_est"] = total_input_tokens
        llm_stats_sink["prompt_tokens_est"] = total_prompt_tokens
        llm_stats_sink["context_tokens_est"] = total_context_tokens
        llm_stats_sink["memory_tokens_est"] = _approx_tokens(clipped_memory_context)
        llm_stats_sink["calls"] = total_calls
    if debug_sink is not None:
        debug_sink["llm_response"] = {
            "trace_id": trace_id,
            "trace_stage": "agent.llm.summarize",
            "output_tokens_est": _approx_tokens(answer),
        }
    if llm_stats_sink is not None:
        llm_stats_sink["output_tokens_est"] = _approx_tokens(answer)
    return answer


def _build_guardrail_flags(
    runtime_state: AgentRuntimeState,
    media_validation: dict[str, Any],
) -> dict[str, bool]:
    original_question = str(runtime_state.decision.raw_question or "").strip()
    resolved_state = _get_resolved_query_state_from_runtime(runtime_state)
    router_decision = runtime_state.decision
    carry_over = bool(resolved_state.get("carry_over_from_previous_turn"))
    lookup_mode = _get_lookup_mode_from_state(resolved_state)
    router_domain = str(router_decision.domain or "general")
    router_confidence = float(router_decision.confidence or 0.0)
    raw_candidates_count = int(media_validation.get("raw_candidates_count", 0) or 0)
    dropped_by_validator = int(media_validation.get("dropped_by_validator", 0) or 0)
    dropped_by_reference_limit = int(media_validation.get("dropped_by_reference_limit", 0) or 0)
    returned_result_count = int(media_validation.get("returned_result_count", media_validation.get("post_filter_count", 0)) or 0)
    planner_snapshot = _get_planner_snapshot_from_runtime(runtime_state)
    hard_filters = planner_snapshot.get("hard_filters") if isinstance(planner_snapshot.get("hard_filters"), dict) else {}
    series_scope = hard_filters.get("series")
    detail_query = _question_requests_media_details(str(runtime_state.context_resolution.resolved_question or original_question))
    short_ambiguous_surface = len(original_question) <= 12 and any(token in original_question for token in ("那个", "这个", "那部", "这部", "那本", "这本"))
    title_marked_without_entity = bool(runtime_state.decision.evidence.get("media_title_marked")) and not bool(runtime_state.decision.entities)
    rewritten_media_query = str((runtime_state.decision.rewritten_queries or {}).get("media_query") or "").strip()
    rewrite_stabilized = bool(rewritten_media_query) and rewritten_media_query != original_question
    low_confidence_understanding = bool(original_question) and not carry_over and (
        short_ambiguous_surface
        or title_marked_without_entity
        or (
            router_confidence < 0.55
            and (
                router_domain == "general"
                or (
                    len(original_question) <= 12
                    and not runtime_state.decision.entities
                    and not resolved_state.get("filters")
                    and not resolved_state.get("media_type")
                )
            )
        )
    )
    if rewrite_stabilized and router_domain == "media":
        low_confidence_understanding = False
    # Tech-domain questions should not trigger restricted guardrail: they need doc-RAG or
    # general LLM fallback, never a "please clarify media type" wall.
    if router_domain == "tech":
        low_confidence_understanding = False
    # General-domain knowledge questions also should not hit restricted: just answer with LLM.
    if router_domain == "general":
        low_confidence_understanding = False
    # Media restricted only when there is a genuine context-ambiguity flag (short ambiguous
    # surface, title marker without a resolved entity) -- not just low confidence.
    if router_domain == "media" and low_confidence_understanding:
        if not (short_ambiguous_surface or title_marked_without_entity):
            low_confidence_understanding = False
    return {
        "low_confidence_understanding": low_confidence_understanding,
        "high_validator_drop_rate": raw_candidates_count > 0 and (dropped_by_validator / raw_candidates_count) >= 0.4,
        "insufficient_valid_results": (
            lookup_mode == "filter_search"
            and raw_candidates_count > 0
            and returned_result_count <= 1
            and not (detail_query and bool(series_scope))
        ),
        "state_inheritance_ambiguous": bool(runtime_state.context_resolution.detected_followup) and not bool(resolved_state.get("carry_over_from_previous_turn")),
        "answer_truncated_by_reference_limit": dropped_by_reference_limit > 0,
    }


def _build_error_taxonomy(
    runtime_state: AgentRuntimeState,
    media_validation: dict[str, Any],
    guardrail_flags: dict[str, bool],
) -> dict[str, Any]:
    validator_drop_reasons = media_validation.get("drop_reasons") if isinstance(media_validation.get("drop_reasons"), dict) else {}
    dominant_validator_reason = ""
    dominant_count = -1
    for reason, count in validator_drop_reasons.items():
        current_count = int(count or 0)
        if current_count > dominant_count:
            dominant_validator_reason = str(reason)
            dominant_count = current_count

    if guardrail_flags.get("state_inheritance_ambiguous"):
        layer = "query_understanding"
        primary_error_type = "state_inheritance_ambiguous"
    elif guardrail_flags.get("low_confidence_understanding"):
        layer = "query_understanding"
        primary_error_type = "low_confidence_understanding"
    elif dominant_validator_reason:
        layer = "validation"
        primary_error_type = dominant_validator_reason
    elif guardrail_flags.get("insufficient_valid_results"):
        layer = "retrieval"
        primary_error_type = "insufficient_valid_results"
    elif guardrail_flags.get("answer_truncated_by_reference_limit"):
        layer = "answer_synthesis"
        primary_error_type = "reference_limit_truncation"
    else:
        layer = "none"
        primary_error_type = "none"

    return {
        "layer": layer,
        "primary_error_type": primary_error_type,
        "dominant_validator_reason": dominant_validator_reason,
    }


def _update_post_retrieval_fallback_evidence(
    runtime_state: AgentRuntimeState,
    query_classification: dict[str, Any],
    tool_results: list[ToolExecution],
    doc_data: dict[str, Any],
    query_profile: dict[str, Any],
) -> None:
    doc_score = doc_data.get("doc_top1_score") if isinstance(doc_data, dict) else None
    doc_threshold = float(query_profile.get("doc_score_threshold", DOC_SCORE_THRESHOLD) or DOC_SCORE_THRESHOLD)
    doc_similarity = {
        "available": bool(doc_data),
        "score": round(float(doc_score), 4) if doc_score is not None else None,
        "threshold": round(doc_threshold, 4),
        "top1_score_before_rerank": doc_data.get("doc_top1_score_before_rerank") if isinstance(doc_data, dict) else None,
        "no_context": bool(doc_data.get("no_context")) if isinstance(doc_data, dict) else False,
    }
    media_validation = _get_media_validation(tool_results)
    tmdb_result = next((item for item in tool_results if item.tool == TOOL_SEARCH_TMDB), None)
    tmdb_rows = tmdb_result.data.get("results", []) if (tmdb_result and isinstance(tmdb_result.data, dict) and isinstance(tmdb_result.data.get("results"), list)) else []
    tech_score = float(doc_score or 0.0) if doc_score is not None else 0.0
    weak_threshold = max(0.18, TECH_QUERY_DOC_SIM_THRESHOLD - 0.10)
    assessment = PostRetrievalAssessment(
        status="ready",
        doc_similarity=doc_similarity,
        media_validation={
            "raw_candidates_count": int(media_validation.get("raw_candidates_count", 0) or 0),
            "returned_result_count": int(media_validation.get("returned_result_count", 0) or 0),
            "dropped_by_validator": int(media_validation.get("dropped_by_validator", 0) or 0),
            "dropped_by_reference_limit": int(media_validation.get("dropped_by_reference_limit", 0) or 0),
        },
        tmdb={
            "requested": tmdb_result is not None,
            "result_count": len(tmdb_rows),
        },
        tech_score=tech_score,
        weak_tech_signal=weak_threshold <= tech_score < TECH_QUERY_DOC_SIM_THRESHOLD,
    )
    runtime_state.post_retrieval_assessment = assessment
    query_classification["fallback_evidence"] = _serialize_post_retrieval_assessment(assessment)
    query_classification["doc_similarity"] = doc_similarity
    query_classification["tech_score"] = tech_score
    query_classification["weak_tech_signal"] = bool(assessment.weak_tech_signal)


def _build_agent_trace_record(
    *,
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
) -> dict[str, Any]:
    _, llm_model, _, _ = _get_llm_profile(backend)
    resolved_query_state = _get_resolved_query_state_from_runtime(runtime_state)
    serialized_decision = _serialize_router_decision(runtime_state.decision)
    serialized_plan = _serialize_execution_plan(runtime_state.execution_plan)
    decision_category, decision_path = _build_router_decision_path(
        query_classification={
            "router_decision": serialized_decision,
            "execution_plan": serialized_plan,
            "resolved_question": runtime_state.context_resolution.resolved_question,
        },
        search_mode=search_mode,
        planned_tools=planned_tools,
        tool_results=tool_results,
    )
    vector_batches = list(doc_data.get("vector_batches") or [])
    vector_candidates = sum(
        len(batch.get("results") or [])
        for batch in vector_batches
        if isinstance(batch, dict)
    )
    executed_tool_depth = sum(1 for item in tool_results if str(item.status or "").strip().lower() != "skipped")
    media_tool_data = next(
        (item.data for item in tool_results if item.tool == TOOL_QUERY_MEDIA and isinstance(item.data, dict)),
        {},
    )
    media_validation = media_tool_data.get("validation") if isinstance(media_tool_data.get("validation"), dict) else {}
    candidate_source_breakdown = media_tool_data.get("candidate_source_breakdown") if isinstance(media_tool_data.get("candidate_source_breakdown"), dict) else {}
    planner_snapshot = _get_planner_snapshot_from_runtime(runtime_state)
    execution_plan = serialized_plan
    router_decision = serialized_decision
    fallback_evidence = _serialize_post_retrieval_assessment(runtime_state.post_retrieval_assessment)
    guardrail_flags = _build_guardrail_flags(runtime_state, media_validation)
    error_taxonomy = _build_error_taxonomy(runtime_state, media_validation, guardrail_flags)
    router = {
        "selected_tool": planned_tools[0].name if planned_tools else "",
        "planned_tools": [call.name for call in planned_tools],
        "resolved_question": str(runtime_state.context_resolution.resolved_question or ""),
        "domain": str(router_decision.get("domain") or "general"),
        "lookup_mode": _get_lookup_mode_from_state(resolved_query_state),
        "decision_intent": str(router_decision.get("intent") or "knowledge_qa"),
        "decision_confidence": router_decision.get("confidence"),
        "decision_category": decision_category,
        "decision_path": decision_path,
        "planned_tool_depth": len(planned_tools),
        "executed_tool_depth": executed_tool_depth,
        "classifier_label": str(runtime_state.llm_media.get("label", "") or ""),
        "doc_similarity": fallback_evidence.get("doc_similarity", {}).get("score"),
        "media_entity_confident": bool(runtime_state.decision.entities),
        "entity_hit_count": len(list(runtime_state.decision.entities or [])),
        "followup_target": str(runtime_state.decision.followup_target or ""),
        "rewritten_queries": {str(key): str(value) for key, value in (runtime_state.decision.rewritten_queries or {}).items() if str(key).strip() and str(value).strip()},
        "short_media_surface": bool(runtime_state.decision.evidence.get("profile") == "short" and runtime_state.decision.domain == "media"),
        "policy_reasons": list(execution_plan.get("reasons") or []),
        "fallback_evidence": fallback_evidence,
    }
    retrieval = {
        "vector_hits": len(list(doc_data.get("results") or [])),
        "vector_candidates": vector_candidates,
        "similarity_threshold": timings.get("doc_score_threshold"),
        "top1_score_before_rerank": doc_data.get("doc_top1_score_before_rerank"),
        "top1_score_after_rerank": doc_data.get("doc_top1_score"),
        "query_rewrite_status": str(doc_data.get("query_rewrite", {}).get("status", "") or ""),
        "query_rewrite_count": len(list(doc_data.get("query_rewrite", {}).get("queries") or [])),
        "graph_expansion_batches": 0,
        "raw_candidates_count": int(media_validation.get("raw_candidates_count", 0) or 0),
        "post_filter_count": int(media_validation.get("post_filter_count", 0) or 0),
        "returned_result_count": int(media_validation.get("returned_result_count", 0) or 0),
        "dropped_by_validator": int(media_validation.get("dropped_by_validator", 0) or 0),
        "dropped_by_reference_limit": int(media_validation.get("dropped_by_reference_limit", 0) or 0),
        "validator_drop_reasons": media_validation.get("drop_reasons", {}),
        "reference_limit_drop_reasons": media_validation.get("reference_limit_drop_reasons", {}),
        "candidate_source_breakdown": candidate_source_breakdown,
    }
    ranking = {
        "method": str(doc_data.get("rerank", {}).get("method", "") or ""),
        "rerank_k": len(list(doc_data.get("results") or [])),
        "top1_identity_changed": doc_data.get("doc_top1_identity_changed"),
        "top1_rank_shift": doc_data.get("doc_top1_rank_shift"),
    }
    tools = []
    for item in tool_results:
        data = item.data if isinstance(item.data, dict) else {}
        results = data.get("results") if isinstance(data.get("results"), list) else []
        source_counts = data.get("source_counts") if isinstance(data.get("source_counts"), dict) else {}
        per_item_source = str(data.get("per_item_source", "") or "")
        display_name = item.tool
        if source_counts:
            ordered_sources = ", ".join(
                f"{name}:{count}"
                for name, count in sorted(
                    ((str(k), int(v or 0)) for k, v in source_counts.items() if str(k).strip()),
                    key=lambda pair: (-pair[1], pair[0]),
                )
            )
            display_name = f"{item.tool} [{ordered_sources}]"
        elif per_item_source:
            display_name = f"{item.tool} [{per_item_source}]"
        tools.append(
            {
                "name": item.tool,
                "display_name": display_name,
                "status": item.status,
                "latency_ms": data.get("latency_ms"),
                "result_count": len(results),
                "cache_hit": bool(data.get("cache_hit")),
                "trace_stage": str(data.get("trace_stage", "") or ""),
                "per_item_source": per_item_source,
                "source_counts": dict(source_counts),
                "mixed_sources": bool(data.get("mixed_sources")),
            }
        )
    return {
        "trace_id": trace_id,
        "timestamp": _now_iso(),
        "entrypoint": "agent",
        "call_type": "benchmark_case" if benchmark_mode else ("chat_stream" if stream_mode else "chat"),
        "session_id": session_id,
        "search_mode": search_mode,
        "conversation_state_before": dict(runtime_state.context_resolution.conversation_state_before),
        "detected_followup": bool(runtime_state.context_resolution.detected_followup),
        "inheritance_applied": dict(runtime_state.context_resolution.inheritance_applied),
        "conversation_state_after": dict(runtime_state.context_resolution.conversation_state_after),
        "state_diff": dict(runtime_state.context_resolution.state_diff),
        "query_understanding": {
            "original_question": str(runtime_state.decision.raw_question or ""),
            "resolved_question": str(runtime_state.context_resolution.resolved_question or ""),
            "domain": str(router_decision.get("domain") or "general"),
            "decision_intent": str(router_decision.get("intent") or "knowledge_qa"),
            "lookup_mode": _get_lookup_mode_from_state(resolved_query_state),
            "confidence": router_decision.get("confidence"),
            "selection": _normalize_media_filter_map(resolved_query_state.get("selection")),
            "time_constraint": dict(resolved_query_state.get("time_constraint") or {}),
            "ranking": dict(resolved_query_state.get("ranking") or {}),
            "entities": list(runtime_state.decision.entities or []),
            "followup_target": str(runtime_state.decision.followup_target or ""),
            "needs_comparison": bool(runtime_state.decision.needs_comparison),
            "needs_explanation": bool(runtime_state.decision.needs_explanation),
            "rewritten_queries": {str(key): str(value) for key, value in (runtime_state.decision.rewritten_queries or {}).items() if str(key).strip() and str(value).strip()},
            "filters": resolved_query_state.get("filters", {}),
            "date_range": resolved_query_state.get("date_range", []),
            "inherited_context": resolved_query_state.get("inherited_context", {}),
            "carry_over_from_previous_turn": bool(resolved_query_state.get("carry_over_from_previous_turn")),
            "retrieval_plan": [call.name for call in planned_tools],
            "reasons": list(router_decision.get("reasons") or []),
            "arbitration": str(router_decision.get("arbitration") or "unknown"),
            "classification_conformance": (
                lambda q=str(runtime_state.decision.raw_question or "").strip().lower(),
                       d=str(router_decision.get("domain") or "general"),
                       a=str(router_decision.get("arbitration") or "unknown"): (
                    (lambda oracle=_CLASSIFICATION_ORACLE.get(q): (
                        None if oracle is None else
                        {
                            "status": "match" if (
                                d == oracle["domain"] and (
                                    a == oracle["arbitration"] if isinstance(oracle["arbitration"], str)
                                    else a in oracle["arbitration"]
                                )
                            ) else "mismatch",
                            "expected_domain": oracle["domain"],
                            "expected_arbitration": oracle["arbitration"],
                            "actual_domain": d,
                            "actual_arbitration": a,
                        }
                    ))()
                )()
            ),
        },
        "planner_snapshot": planner_snapshot,
        "execution_plan": execution_plan,
        "guardrail_flags": guardrail_flags,
        "error_taxonomy": error_taxonomy,
        "answer_guardrail_mode": answer_guardrail_mode,
        "query_type": str(runtime_state.decision.query_type or QUERY_TYPE_GENERAL),
        "query_profile": {
            "profile": str(query_profile.get("profile", "medium") or "medium"),
            "token_count": int(query_profile.get("token_count", 0) or 0),
        },
        "router": router,
        "tools": tools,
        "retrieval": retrieval,
        "ranking": ranking,
        "llm": {
            "backend": backend,
            "model": llm_model,
            "latency_seconds": round(float(llm_seconds or 0), 6),
            "input_tokens_est": int(llm_stats.get("input_tokens_est", 0) or 0),
            "prompt_tokens_est": int(llm_stats.get("prompt_tokens_est", 0) or 0),
            "context_tokens_est": int(llm_stats.get("context_tokens_est", 0) or 0),
            "output_tokens_est": int(llm_stats.get("output_tokens_est", 0) or 0),
            "calls": int(llm_stats.get("calls", 0) or 0),
        },
        "stages": {
            "planning_seconds": round(float(planning_seconds or 0), 6),
            "session_prepare_seconds": round(float(timings.get("session_prepare_seconds", 0) or 0), 6),
            "query_profile_seconds": round(float(timings.get("query_profile_seconds", 0) or 0), 6),
            "tool_planning_seconds": round(float(timings.get("tool_planning_seconds", 0) or 0), 6),
            "context_assembly_seconds": round(float(timings.get("context_assembly_seconds", 0) or 0), 6),
            "tool_execution_seconds": round(float(tool_execution_seconds or 0), 6),
            "vector_recall_seconds": round(float(timings.get("vector_recall_seconds", 0) or 0), 6),
            "rerank_seconds": round(float(timings.get("rerank_seconds", 0) or 0), 6),
            "llm_seconds": round(float(llm_seconds or 0), 6),
            "wall_clock_seconds": round(float(wall_clock_seconds or 0), 6),
        },
        "total_elapsed_seconds": round(float(wall_clock_seconds or 0), 6),
        "result": {
            "status": "ok",
            "no_context": int(timings.get("no_context", 0) or 0),
            "no_context_reason": str(timings.get("no_context_reason", "") or ""),
            "degraded_to_retrieval": bool(degraded_to_retrieval),
            "degrade_reason": str(degrade_reason or ""),
        },
    }


def _write_agent_trace_record(record: dict[str, Any]) -> None:
    if write_trace_record is None:
        return
    try:
        write_trace_record(record)
    except Exception:
        pass


def _fallback_retrieval_answer(question: str, tool_results: list[ToolExecution], reason: str = "") -> str:
    lines = ["未检测到可用的大模型，已自动降级为检索回复（bge-base 向量检索链路）。", "", f"问题：{question}", ""]

    doc_result = next((x for x in tool_results if x.tool == TOOL_QUERY_DOC_RAG), None)
    media_result = next((x for x in tool_results if x.tool == TOOL_QUERY_MEDIA), None)
    web_result = next((x for x in tool_results if x.tool == TOOL_SEARCH_WEB), None)
    mediawiki_concept_result = next((x for x in tool_results if x.tool == TOOL_EXPAND_MEDIAWIKI_CONCEPT), None)
    tmdb_result = next((x for x in tool_results if x.tool == TOOL_SEARCH_TMDB), None)

    if doc_result and isinstance(doc_result.data, dict):
        rows = doc_result.data.get("results", [])
        if rows:
            lines.append("文档检索结果：")
            for row in rows[:6]:
                path = str(row.get("path", "")).strip()
                score = row.get("score", None)
                if path:
                    score_text = f" (score={score:.4f})" if isinstance(score, (int, float)) else ""
                    lines.append(f"- {path}{score_text}")
            lines.append("")

    if media_result and isinstance(media_result.data, dict):
        rows = media_result.data.get("results", [])
        if rows:
            lines.append("媒体记录结果：")
            for row in rows[:6]:
                title = str(row.get("title", "")).strip()
                mtype = str(row.get("media_type", "")).strip()
                date = str(row.get("date", "")).strip()
                if title:
                    extra = " / ".join([x for x in [mtype, date] if x])
                    lines.append(f"- {title}{(' (' + extra + ')') if extra else ''}")
            lines.append("")

    if web_result and isinstance(web_result.data, dict):
        rows = web_result.data.get("results", [])
        if rows:
            lines.append("联网结果：")
            for row in rows[:6]:
                title = str(row.get("title", row.get("url", "网页"))).strip()
                url = str(row.get("url", "")).strip()
                if url:
                    lines.append(f"- {title}: {url}")
            lines.append("")

    if mediawiki_concept_result and isinstance(mediawiki_concept_result.data, dict):
        concept = str(mediawiki_concept_result.data.get("concept") or "").strip()
        countries = mediawiki_concept_result.data.get("countries", []) if isinstance(mediawiki_concept_result.data.get("countries"), list) else []
        authors = mediawiki_concept_result.data.get("authors", []) if isinstance(mediawiki_concept_result.data.get("authors"), list) else []
        if concept and (countries or authors):
            lines.append(f"概念展开结果：{concept}")
            if countries:
                lines.append(f"- 关联国家: {', '.join(str(item) for item in countries[:8])}")
            if authors:
                lines.append(f"- 关联作者: {', '.join(str(item) for item in authors[:8])}")
            lines.append("")

    if tmdb_result and isinstance(tmdb_result.data, dict):
        rows = tmdb_result.data.get("results", [])
        if rows:
            lines.append("TMDB 外部媒体结果：")
            for row in rows[:6]:
                title = str(row.get("title", "")).strip()
                media_type = str(row.get("media_type", "")).strip()
                date = str(row.get("date", "")).strip()
                if title:
                    extra = " / ".join([x for x in [media_type, date] if x])
                    lines.append(f"- {title}{(' (' + extra + ')') if extra else ''}")
            lines.append("")

    if reason:
        if "Missing dependency: openai" in reason:
            lines.append("降级原因：当前环境未安装 `openai` 依赖，已切换到纯检索模式。")
            lines.append("建议：执行 `pip install -r nav_dashboard/requirements.txt` 后可恢复 LLM 汇总能力。")
        else:
            lines.append(f"降级原因：{reason}")
    return "\n".join(lines).strip()


def _get_media_validation(tool_results: list[ToolExecution]) -> dict[str, Any]:
    media_result = next((item for item in tool_results if item.tool == TOOL_QUERY_MEDIA and isinstance(item.data, dict)), None)
    if media_result is None:
        return {}
    validation = media_result.data.get("validation") if isinstance(media_result.data.get("validation"), dict) else {}
    return dict(validation)


def _get_media_result_rows(tool_results: list[ToolExecution]) -> list[dict[str, Any]]:
    media_result = next((item for item in tool_results if item.tool == TOOL_QUERY_MEDIA and isinstance(item.data, dict)), None)
    if media_result is None:
        return []
    rows = media_result.data.get("results") if isinstance(media_result.data.get("results"), list) else []
    return [row for row in rows if isinstance(row, dict)]


def _format_guardrail_date_range(date_range: Any) -> str:
    if not isinstance(date_range, list) or len(date_range) != 2:
        return ""
    start = str(date_range[0] or "").strip()
    end = str(date_range[1] or "").strip()
    if not start or not end:
        return ""
    return f"{start} 至 {end}"


def _describe_planner_scope(planner_snapshot: dict[str, Any]) -> str:
    hard_filters = planner_snapshot.get("hard_filters") if isinstance(planner_snapshot.get("hard_filters"), dict) else {}
    series = hard_filters.get("series")
    if isinstance(series, list):
        series_values = [str(item).strip() for item in series if str(item).strip()]
        if len(series_values) == 1:
            return series_values[0]
        if series_values:
            return " / ".join(series_values)
    elif str(series or "").strip():
        return str(series).strip()
    media_type = str(hard_filters.get("media_type", "") or "").strip().lower()
    category = hard_filters.get("category")
    if media_type == "anime" or category == "动画" or (isinstance(category, list) and "动画" in category):
        return "动画番剧"
    if media_type == "video":
        return "视频"
    if media_type == "movie":
        return "电影"
    if media_type == "tv":
        return "剧集"
    if media_type == "book":
        return "图书"
    if media_type == "music":
        return "音乐"
    if media_type == "game":
        return "游戏"
    entity = str((planner_snapshot.get("query_text") or "") if isinstance(planner_snapshot, dict) else "").strip()
    if entity:
        return entity
    return "筛选条件"


def _build_followup_answer_note(runtime_state: AgentRuntimeState) -> str:
    if not bool(runtime_state.context_resolution.detected_followup):
        return ""
    resolved_state = _get_resolved_query_state_from_runtime(runtime_state)
    if not bool(resolved_state.get("carry_over_from_previous_turn")):
        return ""
    planner_snapshot = _get_planner_snapshot_from_runtime(runtime_state)
    inheritance = dict(runtime_state.context_resolution.inheritance_applied)
    parts: list[str] = []
    scope = _describe_planner_scope(planner_snapshot)
    if inheritance.get("media_type") == "carried_over" or inheritance.get("filters") in {"carried_over", "overridden"}:
        parts.append(f"沿用了上一轮的{scope}约束")
    date_range = _format_guardrail_date_range(runtime_state.context_resolution.conversation_state_after.get("date_range"))
    if inheritance.get("date_range") == "overridden" and date_range:
        parts.append(f"将时间窗替换为 {date_range}")
    elif inheritance.get("date_range") == "carried_over" and date_range:
        parts.append(f"保留了时间窗 {date_range}")
    if inheritance.get("entity") == "cleared":
        parts.append("清除了上一轮的具体作品实体")
    if not parts:
        parts.append("沿用了上一轮的上下文")
    return "处理说明：" + "，".join(parts) + "。"


def _question_requests_personal_evaluation(question: str, query_classification: dict[str, Any] | None = None) -> bool:
    text = str(question or "").strip()
    if not text and isinstance(query_classification, dict):
        text = str(query_classification.get("resolved_question", "") or "").strip()
    lowered = text.lower()
    if any(cue in text for cue in ("评价", "评分", "评论", "短评", "看法", "感受", "印象", "我的评价")):
        return True
    return any(cue in lowered for cue in ("review", "rating", "comment"))


def _question_requests_media_details(question: str, query_classification: dict[str, Any] | None = None) -> bool:
    text = str(question or "").strip()
    if not text and isinstance(query_classification, dict):
        text = str(query_classification.get("resolved_question", "") or "").strip()
    lowered = text.lower()
    detail_cues = (
        *ROUTER_MEDIA_DETAIL_CUES,
        "具体细节",
        "细节信息",
        "详细信息",
        "详细资料",
        "作者",
        "出版方",
        "出版社",
        "发行方",
        "发行商",
        "渠道",
        "平台",
        "工作室",
        "厂牌",
        "制作公司",
    )
    if any(cue in text for cue in detail_cues):
        return True
    return any(cue in lowered for cue in ("author", "publisher", "channel", "platform", "studio", "detail"))


def _format_media_rating(value: Any) -> str:
    if value in {None, ""}:
        return ""
    try:
        number = float(value)
    except Exception:
        return str(value).strip()
    if number.is_integer():
        return f"{int(number)}/10"
    return f"{number:.1f}/10"


def _build_media_row_detail_lines(row: dict[str, Any], *, include_review: bool, include_metadata: bool = False) -> list[str]:
    title = str(row.get("title", "") or "").strip() or "未命名条目"
    rating = _format_media_rating(row.get("rating"))
    date = str(row.get("date", "") or "").strip()
    media_type = str(row.get("media_type", "") or "").strip()
    meta_parts = [part for part in [f"评分：{rating}" if rating else "", f"日期：{date}" if date else "", media_type] if part]
    lines = [f"**{title}**"]
    if meta_parts:
        lines.append(f"- {' | '.join(meta_parts)}")
    if include_metadata:
        author = str(row.get("author", "") or "").strip()
        publisher = str(row.get("publisher", "") or "").strip()
        channel = str(row.get("channel", "") or "").strip()
        category = str(row.get("category", "") or "").strip()
        detail_parts = [
            f"作者：{author}" if author else "",
            f"出版方：{publisher}" if publisher else "",
            f"渠道：{channel}" if channel else "",
            f"分类：{category}" if category else "",
        ]
        detail_line = " | ".join(part for part in detail_parts if part)
        if detail_line:
            lines.append(f"- {detail_line}")
    review = str(row.get("review", "") or "").strip()
    if include_review:
        if review:
            lines.append(f"- 短评：{review}")
        else:
            lines.append("- 短评：未记录")
    return lines


def _build_structured_media_answer(
    question: str,
    runtime_state: AgentRuntimeState,
    tool_results: list[ToolExecution],
    *,
    include_guardrail_explanation: bool = False,
    include_followup_note: bool = False,
) -> str:
    rows = _get_media_result_rows(tool_results)
    if not rows:
        return ""
    query_type = _get_query_type(runtime_state=runtime_state).strip().upper()
    if query_type != QUERY_TYPE_MEDIA:
        return ""
    non_media_fact_tools = [
        item for item in tool_results
        if item.tool not in {TOOL_QUERY_MEDIA, TOOL_EXPAND_MEDIAWIKI_CONCEPT}
        and isinstance(item.data, dict)
        and isinstance(item.data.get("results"), list)
        and item.data.get("results")
    ]
    if non_media_fact_tools:
        return ""

    resolved_state = _get_resolved_query_state_from_runtime(runtime_state)
    resolved_question = str(runtime_state.context_resolution.resolved_question or question or "").strip()
    lookup_mode = _get_lookup_mode_from_state(resolved_state)
    wants_review = _question_requests_personal_evaluation(resolved_question)
    wants_detail = _question_requests_media_details(resolved_question)
    if lookup_mode != "filter_search" and not wants_review and not wants_detail:
        return ""

    validation = _get_media_validation(tool_results)
    returned_count = int(validation.get("returned_result_count", len(rows)) or 0)
    planner_snapshot = _get_planner_snapshot_from_runtime(runtime_state)
    scope = _describe_planner_scope(planner_snapshot)
    lines: list[str] = []
    followup_note = _build_followup_answer_note(runtime_state)
    if include_followup_note and followup_note:
        lines.append(followup_note)

    if include_guardrail_explanation:
        if returned_count > 0:
            lines.append(f"结果说明：按当前约束严格过滤后，仅找到 {returned_count} 条符合条件的结果。")
        else:
            lines.append("结果说明：按当前约束严格过滤后，未找到严格满足条件的结果。")
        drop_reasons = validation.get("drop_reasons") if isinstance(validation.get("drop_reasons"), dict) else {}
        missing_release_date = int(drop_reasons.get("missing_release_date", 0) or 0)
        date_out_of_range = int(drop_reasons.get("date_out_of_range", 0) or 0)
        if missing_release_date > 0:
            lines.append(f"有 {missing_release_date} 条候选缺少完整日期，因此未纳入。")
        if date_out_of_range > 0:
            lines.append(f"有 {date_out_of_range} 条候选超出时间范围，因此已排除。")
    elif returned_count > 0:
        if wants_review and wants_detail:
            lines.append(f"按当前条件，找到 {returned_count} 条符合条件的{scope}，下面列出你的评分、短评和条目细节。")
        elif wants_review:
            lines.append(f"按当前条件，找到 {returned_count} 条符合条件的{scope}，下面优先列出你的评分和短评。")
        elif wants_detail:
            lines.append(f"按当前条件，找到 {returned_count} 条符合条件的{scope}，下面列出条目的细节信息。")
        else:
            lines.append(f"按当前条件，找到 {returned_count} 条符合条件的{scope}。")

    if include_guardrail_explanation and rows:
        lines.append("严格满足条件的结果：")

    for index, row in enumerate(rows[: max(1, min(6, len(rows)))], start=1):
        detail_lines = _build_media_row_detail_lines(row, include_review=wants_review, include_metadata=wants_detail)
        if not detail_lines:
            continue
        first, *rest = detail_lines
        lines.append(f"{index}. {first}")
        for item in rest:
            lines.append(f"   {item}")

    return "\n".join(line for line in lines if str(line).strip()).strip()


def _format_media_row_brief(row: dict[str, Any]) -> str:
    title = str(row.get("title", "") or "").strip()
    media_type = str(row.get("media_type", "") or "").strip()
    date = str(row.get("date", "") or "").strip()
    details = " / ".join(part for part in [media_type, date] if part)
    if title and details:
        return f"{title}（{details}）"
    return title or details


def _build_restricted_guardrail_answer(
    question: str,
    runtime_state: AgentRuntimeState,
    tool_results: list[ToolExecution],
    guardrail_flags: dict[str, bool],
) -> str:
    lines: list[str] = []
    followup_note = _build_followup_answer_note(runtime_state)
    if followup_note:
        lines.append(followup_note)
    if guardrail_flags.get("low_confidence_understanding"):
        lines.append("当前问题上下文不够明确，我无法安全判断要沿用哪一轮的对象或筛选条件，因此不直接给出结论。")
        if runtime_state.decision.domain == "media":
            lines.append("请明确作品名、时间范围或媒体类型后再问一次。")
        else:
            lines.append("可以补充更多背景或换个更具体的说法后再问一次。")
        return "\n\n".join(lines).strip()

    structured = _build_structured_media_answer(
        question,
        runtime_state,
        tool_results,
        include_guardrail_explanation=True,
        include_followup_note=True,
    )
    if structured:
        return structured

    validation = _get_media_validation(tool_results)
    rows = _get_media_result_rows(tool_results)
    returned_count = int(validation.get("returned_result_count", len(rows)) or 0)
    if returned_count > 0:
        lines.append(f"结果说明：按当前约束严格过滤后，仅找到 {returned_count} 条符合条件的结果。")
    else:
        lines.append("结果说明：按当前约束严格过滤后，未找到严格满足条件的结果。")

    drop_reasons = validation.get("drop_reasons") if isinstance(validation.get("drop_reasons"), dict) else {}
    missing_release_date = int(drop_reasons.get("missing_release_date", 0) or 0)
    date_out_of_range = int(drop_reasons.get("date_out_of_range", 0) or 0)
    if missing_release_date > 0:
        lines.append(f"有 {missing_release_date} 条候选缺少完整日期，因此未纳入。")
    if date_out_of_range > 0:
        lines.append(f"有 {date_out_of_range} 条候选超出时间范围，因此已排除。")
    if rows:
        lines.append("严格满足条件的结果：")
        for row in rows[:4]:
            brief = _format_media_row_brief(row)
            if brief:
                lines.append(f"- {brief}")
    return "\n".join(lines).strip()


def _build_guardrail_answer_mode(
    question: str,
    runtime_state: AgentRuntimeState,
    tool_results: list[ToolExecution],
    guardrail_flags: dict[str, bool],
) -> dict[str, Any]:
    reasons: list[str] = []
    annotation_lines: list[str] = []
    media_rows = _get_media_result_rows(tool_results)
    has_grounded_media_results = bool(media_rows)
    if guardrail_flags.get("low_confidence_understanding"):
        if has_grounded_media_results:
            rewritten_media_query = str((runtime_state.decision.rewritten_queries or {}).get("media_query") or runtime_state.context_resolution.resolved_question or question or "").strip()
            if rewritten_media_query:
                annotation_lines.append(f"处理说明：我按“{rewritten_media_query}”来理解这次追问；如果你想换范围或对象，可以直接改写约束。")
        else:
            reasons.append("low_confidence_understanding")
    if guardrail_flags.get("insufficient_valid_results"):
        reasons.append("insufficient_valid_results")
    if reasons:
        return {
            "mode": "restricted",
            "reasons": reasons,
            "answer": _build_restricted_guardrail_answer(question, runtime_state, tool_results, guardrail_flags),
            "annotations": [],
        }

    followup_note = _build_followup_answer_note(runtime_state)
    if followup_note:
        annotation_lines.append(followup_note)
    if guardrail_flags.get("answer_truncated_by_reference_limit"):
        validation = _get_media_validation(tool_results)
        returned_count = int(validation.get("returned_result_count", 0) or 0)
        if returned_count > 0:
            annotation_lines.append(f"结果说明：以下仅返回严格满足条件的 {returned_count} 条。")
    mode = "annotated" if annotation_lines else "normal"
    return {
        "mode": mode,
        "reasons": [],
        "answer": "",
        "annotations": annotation_lines,
    }


def _apply_guardrail_answer_mode(answer: str, answer_mode: dict[str, Any]) -> str:
    mode = str(answer_mode.get("mode", "normal") or "normal")
    if mode == "restricted":
        return str(answer_mode.get("answer", "") or "").strip()
    annotations = [str(item).strip() for item in (answer_mode.get("annotations") or []) if str(item).strip()]
    body = str(answer or "").strip()
    if annotations and body:
        return "\n\n".join([*annotations, body]).strip()
    if annotations:
        return "\n\n".join(annotations).strip()
    return body


def _build_references_markdown(tool_results: list[ToolExecution], *, request_base_url: str = "") -> str:
    library_ref_base = _library_tracker_reference_base(request_base_url)
    doc_refs: list[tuple[float, str]] = []
    media_refs: list[tuple[float, str]] = []
    external_refs: list[tuple[float, str]] = []

    for result in tool_results:
        if not isinstance(result.data, dict):
            continue
        rows = result.data.get("results", [])
        if not isinstance(rows, list):
            continue

        if result.tool == TOOL_QUERY_DOC_RAG:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                path = str(row.get("path", "")).strip()
                score = _score_value(row)
                if not path or score is None:
                    continue
                doc_uri = f"doc://{urlparse.quote(path)}"
                doc_refs.append((score, f"- [本地文档: {path} ({score:.4f})]({doc_uri})"))

        elif result.tool == TOOL_QUERY_MEDIA:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                title = str(row.get("title", "")).strip()
                media_type = str(row.get("media_type", "")).strip()
                item_id = str(row.get("id", "")).strip()
                score = _score_value(row)
                if not title or score is None:
                    continue
                label = f"媒体: {title}"
                if media_type:
                    label += f" ({media_type})"
                if item_id:
                    url = f"{library_ref_base}/?item={urlparse.quote(item_id)}"
                    media_refs.append((score, f"- [本地媒体库: {title}{(' (' + media_type + ')') if media_type else ''} ({score:.4f})]({url})"))
                else:
                    media_refs.append((score, f"- 本地媒体库: {label} ({score:.4f})"))

        elif result.tool == TOOL_SEARCH_WEB:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                title = str(row.get("title", row.get("url", "网页"))).strip() or "网页"
                url = str(row.get("url", "")).strip()
                score = _score_value(row)
                if not url or score is None:
                    continue
                external_refs.append((score, f"- [外部网页: {title} ({score:.4f})]({url})"))

        elif result.tool in {TOOL_SEARCH_MEDIAWIKI, TOOL_PARSE_MEDIAWIKI, TOOL_EXPAND_MEDIAWIKI_CONCEPT}:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                title = str(row.get("display_title") or row.get("title") or "Wikipedia").strip() or "Wikipedia"
                url = str(row.get("url", "")).strip()
                score = _score_value(row)
                if not url:
                    continue
                label = f"外部 Wiki: {title}"
                if score is None:
                    external_refs.append((0.0, f"- [{label}]({url})"))
                else:
                    external_refs.append((score, f"- [{label} ({score:.4f})]({url})"))

        elif result.tool == TOOL_SEARCH_TMDB:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                title = str(row.get("title") or "TMDB").strip() or "TMDB"
                media_type = str(row.get("media_type") or "").strip()
                url = str(row.get("url", "")).strip()
                score = _score_value(row)
                if not url:
                    continue
                label = f"外部 TMDB: {title}"
                if media_type:
                    label += f" ({media_type})"
                if score is None:
                    external_refs.append((0.0, f"- [{label}]({url})"))
                else:
                    external_refs.append((score, f"- [{label} ({score:.4f})]({url})"))

    if not doc_refs and not media_refs and not external_refs:
        return ""

    local_limit = max(1, int(MAX_REFERENCE_ITEMS))
    external_limit = max(1, min(3, int(MAX_REFERENCE_ITEMS)))
    sections: list[str] = []
    if media_refs:
        media_lines = [item[1] for item in sorted(media_refs, key=lambda x: x[0], reverse=True)[:local_limit]]
        sections.append("### 本地媒体库参考\n" + "\n".join(media_lines))
    if doc_refs:
        doc_lines = [item[1] for item in sorted(doc_refs, key=lambda x: x[0], reverse=True)[:local_limit]]
        sections.append("### 本地文档参考\n" + "\n".join(doc_lines))
    if external_refs:
        external_lines = [item[1] for item in sorted(external_refs, key=lambda x: x[0], reverse=True)[:external_limit]]
        sections.append("### 外部参考\n" + "\n".join(external_lines))
    return "\n\n" + "\n\n".join(sections)


def run_agent_round(
    *,
    question: str,
    session_id: str = "",
    history: list[dict[str, str]] | None = None,
    backend: str = "local",
    search_mode: str = "local_only",
    confirm_over_quota: bool = False,
    deny_over_quota: bool = False,
    debug: bool = False,
    request_base_url: str = "",
    benchmark_mode: bool = False,
    trace_id: str = "",
) -> dict[str, Any]:
    import time as _wall_time
    _wall_t0 = _wall_time.perf_counter()
    q = (question or "").strip()
    if not q:
        raise ValueError("question is required")
    resolved_trace_id = _normalize_trace_id(trace_id)
    _session_prepare_t0 = _wall_time.perf_counter()

    hist = history or []
    session = None
    sid = (session_id or "").strip()
    if benchmark_mode:
        sid = sid or _new_ephemeral_session_id()
    else:
        if not sid:
            sid = str(create_session().get("id", "")).strip()
        if not sid:
            raise RuntimeError("failed to create session")

        session = get_session(sid)
        if session and str(session.get("title", "")).strip() in {"", "新会话"}:
            if not bool(session.get("title_locked", False)):
                session["title"] = _derive_session_title(q)
                session["updated_at"] = _now_iso()
                _save_session(session)
        if session and isinstance(session.get("messages"), list):
            hist = [
                {"role": str(m.get("role", "")), "content": str(m.get("text", "")), "trace_id": str(m.get("trace_id", ""))}
                for m in session.get("messages", [])
            ]
    _session_prepare_seconds = _wall_time.perf_counter() - _session_prepare_t0

    normalized_search_mode = _normalize_search_mode(search_mode)
    _query_profile_t0 = _wall_time.perf_counter()
    query_profile = _resolve_query_profile(q)
    _query_profile_seconds = _wall_time.perf_counter() - _query_profile_t0
    quota_state = _load_quota_state()
    _plan_t0 = _wall_time.perf_counter()
    planned, runtime_state, query_classification = _plan_tool_calls(q, hist, backend, quota_state, normalized_search_mode)
    _tool_planning_seconds = _wall_time.perf_counter() - _plan_t0
    _planning_seconds = _session_prepare_seconds + _query_profile_seconds + _tool_planning_seconds
    debug_trace: dict[str, Any] = {
        "timestamp": _now_iso(),
        "trace_id": resolved_trace_id,
        "session_id": sid,
        "question": q,
        "search_mode": normalized_search_mode,
        "query_profile": query_profile,
        "query_classification": query_classification,
        "backend": backend,
        "history": hist,
        "planned_tools": _serialize_planned_tools(planned),
        "reranker": {"status": "not_applicable"},
    }

    exceeded = _quota_exceeded(planned, backend, quota_state)
    if exceeded and not confirm_over_quota and not deny_over_quota:
        return {
            "requires_confirmation": True,
            "trace_id": resolved_trace_id,
            "session_id": sid,
            "confirmation_message": "已超过今日 API 配额，是否继续调用超额工具？",
            "exceeded": exceeded,
            "planned_tools": _serialize_planned_tools(planned),
        }

    allowed_plan: list[PlannedToolCall] = []
    skipped_due_quota: list[str] = []
    if exceeded and deny_over_quota:
        kinds = {str(x.get("kind", "")) for x in exceeded}
        for c in planned:
            if c.name == TOOL_SEARCH_WEB and "web_search" in kinds:
                skipped_due_quota.append(c.name)
                continue
            allowed_plan.append(c)
    else:
        allowed_plan = list(planned)

    if not benchmark_mode:
        append_message(sid, "user", q)

    _tool_exec_t0 = _wall_time.perf_counter()
    tool_results = _execute_tool_plan(allowed_plan, query_profile, resolved_trace_id)
    _tool_execution_seconds = _wall_time.perf_counter() - _tool_exec_t0

    # Inject answer_shape from query_classification so _apply_reference_limits
    # can raise the top-K cap for list_plus_expand collection queries.
    _qc_answer_shape = str((query_classification or {}).get("answer_shape", "") or "")
    if _qc_answer_shape:
        query_profile = dict(query_profile)
        query_profile["answer_shape"] = _qc_answer_shape

    # Keep planner order in final report.
    order = {call.name: i for i, call in enumerate(allowed_plan)}
    tool_results.sort(key=lambda x: order.get(x.tool, 999))
    tool_results = _apply_reference_limits(tool_results, normalized_search_mode, query_profile)
    # Phase-2 fan-out: per-item external enrichment for all list_plus_expand
    # collection queries.  media_family determines the external source:
    #   audiovisual → TMDB per-title (validated match)
    #   bookish / music / game / "" → MediaWiki parse per-title
    _qc_media_family = str((query_classification or {}).get("media_family", "") or "")
    if _qc_answer_shape == "list_plus_expand":
        tool_results = _execute_per_item_expansion(
            tool_results, trace_id=resolved_trace_id, media_family=_qc_media_family
        )
    debug_trace["tool_results"] = [
        {"tool": r.tool, "status": r.status, "summary": r.summary, "data": r.data}
        for r in tool_results
    ]
    doc_tool_result = next((r for r in tool_results if r.tool == TOOL_QUERY_DOC_RAG), None)
    if doc_tool_result and isinstance(doc_tool_result.data, dict):
        debug_trace["query_rewrite"] = doc_tool_result.data.get("query_rewrite", {})

    # Extract doc timing and no_context info for metrics.
    _doc_data = doc_tool_result.data if (doc_tool_result and isinstance(doc_tool_result.data, dict)) else {}
    _doc_vector_recall_s = float(_doc_data.get("vector_recall_seconds", 0) or 0)
    _doc_rerank_s = float(_doc_data.get("rerank_seconds", 0) or 0)
    _doc_top1_score = _doc_data.get("doc_top1_score")
    _doc_top1_score_before_rerank = _doc_data.get("doc_top1_score_before_rerank")
    _doc_top1_identity_changed = _doc_data.get("doc_top1_identity_changed")
    _doc_top1_rank_shift = _doc_data.get("doc_top1_rank_shift")
    _doc_no_context = int(_doc_data.get("no_context", 0) or 0)
    _doc_embed_cache_hit = int(_doc_data.get("embed_cache_hit", 0) or 0)
    _doc_query_rewrite_hit = int(_doc_data.get("query_rewrite_hit", 0) or 0)
    _doc_threshold = float(query_profile.get("doc_score_threshold", DOC_SCORE_THRESHOLD) or DOC_SCORE_THRESHOLD)
    _update_post_retrieval_fallback_evidence(runtime_state, query_classification, tool_results, _doc_data, query_profile)

    # Log no-context queries to shared jsonl file.
    if _doc_no_context and not benchmark_mode:
        try:
            boundary_log_no_context_query(
                q,
                source="agent",
                top1_score=float(_doc_top1_score) if _doc_top1_score is not None else None,
                threshold=_doc_threshold,
                trace_id=resolved_trace_id,
                reason="below_threshold",
            )
        except Exception:
            pass

    media_tool_result = next((r for r in tool_results if r.tool == TOOL_QUERY_MEDIA), None)
    media_rows = media_tool_result.data.get("results", []) if (media_tool_result and isinstance(media_tool_result.data, dict)) else []
    media_validation = _get_media_validation(tool_results)
    guardrail_flags = _build_guardrail_flags(runtime_state, media_validation)
    answer_mode = _build_guardrail_answer_mode(q, runtime_state, tool_results, guardrail_flags)
    query_classification["guardrail_flags"] = guardrail_flags
    query_classification["answer_guardrail_mode"] = {
        "mode": answer_mode.get("mode", "normal"),
        "reasons": list(answer_mode.get("reasons") or []),
    }
    _post_retrieval_outcome: _PostRetrievalOutcome = _PostRetrievalPolicy().evaluate(
        runtime_state.decision, tool_results, guardrail_flags
    )
    _answer_strategy: _AnswerStrategy = _AnswerPolicy().determine(
        runtime_state.decision, _post_retrieval_outcome
    )
    query_classification["post_retrieval_outcome"] = _dc_asdict(_post_retrieval_outcome)
    query_classification["answer_strategy"] = _dc_asdict(_answer_strategy)
    if not benchmark_mode and any(call.name == TOOL_QUERY_MEDIA for call in allowed_plan) and not media_rows:
        _log_agent_media_miss(q, query_profile)

    # Quota accounting: only count actual API calls (not cache hits).
    web_calls = sum(
        1 for r in tool_results
        if r.tool == TOOL_SEARCH_WEB
        and r.status in {"ok", "empty", "error"}
        and not (isinstance(r.data, dict) and r.data.get("cache_hit"))
    )
    if web_calls:
        _increment_quota_state(quota_state, web_search_delta=web_calls)

    # Also check for cache hits among web results (for metrics).
    _web_cache_hit = int(any(
        r.tool == TOOL_SEARCH_WEB
        and isinstance(r.data, dict)
        and r.data.get("cache_hit")
        for r in tool_results
    ))
    _rag_used = int(any(call.name == TOOL_QUERY_DOC_RAG for call in allowed_plan))
    _media_used = int(any(call.name == TOOL_QUERY_MEDIA for call in allowed_plan))
    _web_used = int(any(call.name == TOOL_SEARCH_WEB for call in allowed_plan))
    _agent_no_context, _agent_no_context_reason = _resolve_agent_no_context(
        _get_query_type(runtime_state=runtime_state),
        _rag_used,
        _doc_no_context,
    )

    if _agent_no_context and not _doc_no_context and not benchmark_mode:
        try:
            boundary_log_no_context_query(
                q,
                source="agent",
                top1_score=float(_doc_top1_score) if _doc_top1_score is not None else None,
                threshold=_doc_threshold,
                trace_id=resolved_trace_id,
                reason=_agent_no_context_reason,
            )
        except Exception:
            pass

    if skipped_due_quota:
        for tool_name in skipped_due_quota:
            tool_results.append(
                ToolExecution(tool=tool_name, status="skipped", summary="超过每日配额且已拒绝调用", data={"results": []})
            )

    degraded_to_retrieval = False
    degrade_reason = ""
    _context_assembly_t0 = _wall_time.perf_counter()
    memory_context = "" if benchmark_mode else build_memory_context(sid)
    _context_assembly_seconds = _wall_time.perf_counter() - _context_assembly_t0
    debug_trace["memory_context"] = memory_context
    debug_trace["memory_tokens_est"] = _approx_tokens(memory_context)
    _llm_stats: dict[str, Any] = {}
    _llm_t0 = _wall_time.perf_counter()
    structured_media_answer = _build_structured_media_answer(q, runtime_state, tool_results)
    if str(answer_mode.get("mode", "normal") or "normal") == "restricted":
        answer = str(answer_mode.get("answer", "") or "").strip()
        _llm_stats["backend"] = backend
        _llm_stats["calls"] = 0
        _llm_stats["input_tokens_est"] = 0
        _llm_stats["prompt_tokens_est"] = 0
        _llm_stats["context_tokens_est"] = 0
        _llm_stats["memory_tokens_est"] = _approx_tokens(memory_context)
        _llm_stats["output_tokens_est"] = _approx_tokens(answer)
    elif structured_media_answer:
        answer = structured_media_answer
        _llm_stats["backend"] = backend
        _llm_stats["calls"] = 0
        _llm_stats["input_tokens_est"] = 0
        _llm_stats["prompt_tokens_est"] = 0
        _llm_stats["context_tokens_est"] = 0
        _llm_stats["memory_tokens_est"] = _approx_tokens(memory_context)
        _llm_stats["output_tokens_est"] = _approx_tokens(answer)
    else:
        try:
            answer = _summarize_answer(
                question=q,
                history=hist,
                memory_context=memory_context,
                tool_results=tool_results,
                backend=backend,
                search_mode=normalized_search_mode,
                quota_state=quota_state,
                trace_id=resolved_trace_id,
                debug_sink=debug_trace if debug else None,
                llm_stats_sink=_llm_stats,
                answer_strategy=_answer_strategy,
            )
        except Exception as exc:  # noqa: BLE001
            # If local model is unavailable, provide a retrieval-only fallback reply.
            if (backend or "local").strip().lower() == "local":
                degraded_to_retrieval = True
                degrade_reason = str(exc)
                answer = _fallback_retrieval_answer(q, tool_results, reason=degrade_reason)
                _llm_stats["output_tokens_est"] = _approx_tokens(answer)
            else:
                raise
    _llm_seconds = _wall_time.perf_counter() - _llm_t0
    answer = _apply_guardrail_answer_mode(answer, answer_mode)
    _llm_stats["output_tokens_est"] = _approx_tokens(answer)

    references_md = _build_references_markdown(tool_results, request_base_url=request_base_url)
    final_answer = answer
    if references_md:
        final_answer = f"{answer}{references_md}"

    if not benchmark_mode:
        append_message(sid, "assistant", final_answer, trace_id=resolved_trace_id)
        _update_memory_for_session(sid)
        _schedule_generated_session_title(sid, q, final_answer, lock=True)
        _auto_queue_bug_tickets(final_answer, session_id=sid, trace_id=resolved_trace_id)
    if debug:
        debug_trace["final_answer_tokens_est"] = _approx_tokens(final_answer)
        _write_debug_record(sid, debug_trace)

    # Record per-round agent metrics (best-effort; never raise). Skip for benchmark runs.
    if not benchmark_mode:
        try:
            record_agent_metrics(
                query_profile=str(query_profile.get("profile", "medium") or "medium"),
                search_mode=normalized_search_mode,
                query_type=_get_query_type(runtime_state=runtime_state),
                rag_used=_rag_used,
                media_used=_media_used,
                web_used=_web_used,
                no_context=_agent_no_context,
                no_context_reason=_agent_no_context_reason,
                trace_id=resolved_trace_id,
                doc_score_threshold=_doc_threshold,
                doc_top1_score=float(_doc_top1_score) if _doc_top1_score is not None else None,
                doc_top1_score_before_rerank=float(_doc_top1_score_before_rerank) if _doc_top1_score_before_rerank is not None else None,
                doc_top1_identity_changed=int(_doc_top1_identity_changed) if _doc_top1_identity_changed is not None else None,
                doc_top1_rank_shift=float(_doc_top1_rank_shift) if _doc_top1_rank_shift is not None else None,
                embed_cache_hit=_doc_embed_cache_hit,
                query_rewrite_hit=_doc_query_rewrite_hit,
                vector_recall_seconds=_doc_vector_recall_s,
                rerank_seconds=_doc_rerank_s,
                wall_clock_seconds=_wall_time.perf_counter() - _wall_t0,
            )
        except Exception:
            pass

    trace_record = _build_agent_trace_record(
        trace_id=resolved_trace_id,
        session_id=sid,
        backend=backend,
        search_mode=normalized_search_mode,
        benchmark_mode=benchmark_mode,
        stream_mode=False,
        query_profile=query_profile,
        runtime_state=runtime_state,
        planned_tools=planned,
        tool_results=tool_results,
        doc_data=_doc_data,
        timings={
            "session_prepare_seconds": _session_prepare_seconds,
            "query_profile_seconds": _query_profile_seconds,
            "tool_planning_seconds": _tool_planning_seconds,
            "context_assembly_seconds": _context_assembly_seconds,
            "vector_recall_seconds": _doc_vector_recall_s,
            "rerank_seconds": _doc_rerank_s,
            "no_context": _agent_no_context,
            "no_context_reason": _agent_no_context_reason,
            "doc_score_threshold": _doc_threshold,
        },
        llm_stats=_llm_stats,
        answer_guardrail_mode=query_classification.get("answer_guardrail_mode", {}),
        degraded_to_retrieval=degraded_to_retrieval,
        degrade_reason=degrade_reason,
        wall_clock_seconds=_wall_time.perf_counter() - _wall_t0,
        planning_seconds=_planning_seconds,
        tool_execution_seconds=_tool_execution_seconds,
        llm_seconds=_llm_seconds,
    )
    trace_record["post_retrieval_outcome"] = query_classification.get("post_retrieval_outcome", {})
    trace_record["answer_strategy"] = query_classification.get("answer_strategy", {})
    _write_agent_trace_record(trace_record)

    return {
        "requires_confirmation": False,
        "trace_id": resolved_trace_id,
        "session_id": sid,
        "answer": final_answer,
        "backend": backend,
        "search_mode": normalized_search_mode,
        "query_profile": query_profile,
        "query_classification": query_classification,
        "query_type": _get_query_type(runtime_state=runtime_state),
        "query_understanding": trace_record.get("query_understanding", {}),
        "conversation_state_before": trace_record.get("conversation_state_before", {}),
        "detected_followup": bool(trace_record.get("detected_followup")),
        "inheritance_applied": trace_record.get("inheritance_applied", {}),
        "conversation_state_after": trace_record.get("conversation_state_after", {}),
        "state_diff": trace_record.get("state_diff", {}),
        "planner_snapshot": trace_record.get("planner_snapshot", {}),
        "guardrail_flags": trace_record.get("guardrail_flags", {}),
        "error_taxonomy": trace_record.get("error_taxonomy", {}),
        "answer_guardrail_mode": query_classification.get("answer_guardrail_mode", {}),
        "degraded_to_retrieval": degraded_to_retrieval,
        "degrade_reason": degrade_reason,
        "planned_tools": _serialize_planned_tools(planned),
        "tool_results": [
            {"tool": r.tool, "status": r.status, "summary": r.summary, "data": r.data}
            for r in tool_results
        ],
        "debug_enabled": bool(debug),
        "timings": {
            "vector_recall_seconds": _doc_vector_recall_s,
            "rerank_seconds": _doc_rerank_s,
            "no_context": _agent_no_context,
            "no_context_reason": _agent_no_context_reason,
            "doc_score_threshold": _doc_threshold,
            "web_cache_hit": _web_cache_hit,
        },
        "quota": {
            "date": quota_state.get("date"),
            "web_search": int(quota_state.get("web_search", 0) or 0),
            "web_search_limit": WEB_SEARCH_DAILY_LIMIT,
            "deepseek": int(quota_state.get("deepseek", 0) or 0),
            "deepseek_limit": DEEPSEEK_DAILY_LIMIT,
        },
    }


def run_agent_round_stream(
    *,
    question: str,
    session_id: str = "",
    history: list[dict[str, str]] | None = None,
    backend: str = "local",
    search_mode: str = "local_only",
    confirm_over_quota: bool = False,
    deny_over_quota: bool = False,
    debug: bool = False,
    request_base_url: str = "",
    benchmark_mode: bool = False,
    trace_id: str = "",
) -> Iterator[dict[str, Any]]:
    """Streaming variant of run_agent_round — yields SSE-ready dicts.

    Events emitted (in order):
      {"type": "progress", "message": str}  — stage announcements
      {"type": "quota_exceeded", "message": str, "exceeded": [...], "planned_tools": [...], "session_id": str}
      {"type": "tool_done", "tool": str, "status": str, "summary": str}  — per tool
      {"type": "done", "payload": dict}  — final result (same shape as run_agent_round)
      {"type": "error", "message": str}  — on uncaught error
    """
    import time as _wall_time
    _wall_t0 = _wall_time.perf_counter()

    try:
        q = (question or "").strip()
        if not q:
            yield {"type": "error", "message": "question is required"}
            return
        resolved_trace_id = _normalize_trace_id(trace_id)
        _session_prepare_t0 = _wall_time.perf_counter()

        hist = history or []
        session = None
        sid = (session_id or "").strip()
        if benchmark_mode:
            sid = sid or _new_ephemeral_session_id()
        else:
            if not sid:
                sid = str(create_session().get("id", "")).strip()
            if not sid:
                yield {"type": "error", "message": "failed to create session"}
                return

            session = get_session(sid)
            if session and str(session.get("title", "")).strip() in {"", "新会话"}:
                if not bool(session.get("title_locked", False)):
                    session["title"] = _derive_session_title(q)
                    session["updated_at"] = _now_iso()
                    _save_session(session)
            if session and isinstance(session.get("messages"), list):
                hist = [
                    {"role": str(m.get("role", "")), "content": str(m.get("text", "")), "trace_id": str(m.get("trace_id", ""))}
                    for m in session.get("messages", [])
                ]
        _session_prepare_seconds = _wall_time.perf_counter() - _session_prepare_t0

        normalized_search_mode = _normalize_search_mode(search_mode)
        _query_profile_t0 = _wall_time.perf_counter()
        query_profile = _resolve_query_profile(q)
        _query_profile_seconds = _wall_time.perf_counter() - _query_profile_t0
        quota_state = _load_quota_state()

        yield {"type": "progress", "trace_id": resolved_trace_id, "message": "正在规划工具调用..."}

        _plan_t0 = _wall_time.perf_counter()
        planned, runtime_state, query_classification = _plan_tool_calls(q, hist, backend, quota_state, normalized_search_mode)
        _tool_planning_seconds = _wall_time.perf_counter() - _plan_t0
        _planning_seconds = _session_prepare_seconds + _query_profile_seconds + _tool_planning_seconds

        debug_trace: dict[str, Any] = {
            "timestamp": _now_iso(),
            "trace_id": resolved_trace_id,
            "session_id": sid,
            "question": q,
            "search_mode": normalized_search_mode,
            "query_profile": query_profile,
            "query_classification": query_classification,
            "backend": backend,
            "history": hist,
            "planned_tools": _serialize_planned_tools(planned),
            "reranker": {"status": "not_applicable"},
        }

        yield {"type": "progress", "trace_id": resolved_trace_id, "message": f"查询分类：{_get_query_type(runtime_state=runtime_state)}"}

        exceeded = _quota_exceeded(planned, backend, quota_state)
        if exceeded and not confirm_over_quota and not deny_over_quota:
            yield {
                "type": "quota_exceeded",
                "trace_id": resolved_trace_id,
                "session_id": sid,
                "message": "已超过今日 API 配额，是否继续调用超额工具？",
                "exceeded": exceeded,
                "planned_tools": _serialize_planned_tools(planned),
            }
            return

        allowed_plan: list[PlannedToolCall] = []
        skipped_due_quota: list[str] = []
        if exceeded and deny_over_quota:
            kinds = {str(x.get("kind", "")) for x in exceeded}
            for c in planned:
                if c.name == TOOL_SEARCH_WEB and "web_search" in kinds:
                    skipped_due_quota.append(c.name)
                    continue
                allowed_plan.append(c)
        else:
            allowed_plan = list(planned)

        tool_names_str = "、".join(c.name for c in allowed_plan) if allowed_plan else "无"
        yield {"type": "progress", "trace_id": resolved_trace_id, "message": f"计划调用工具：{tool_names_str}"}

        if not benchmark_mode:
            append_message(sid, "user", q)

        yield {"type": "progress", "trace_id": resolved_trace_id, "message": f"正在并行执行 {len(allowed_plan)} 个工具..."}

        _tool_exec_t0 = _wall_time.perf_counter()
        tool_results = _execute_tool_plan(allowed_plan, query_profile, resolved_trace_id)
        for result in tool_results:
            yield {
                "type": "tool_done",
                "trace_id": resolved_trace_id,
                "tool": result.tool,
                "status": result.status,
                "summary": result.summary,
            }
        _tool_execution_seconds = _wall_time.perf_counter() - _tool_exec_t0

        # Inject answer_shape into query_profile for reference-limit expansion.
        _qc_answer_shape = str((query_classification or {}).get("answer_shape", "") or "")
        if _qc_answer_shape:
            query_profile = dict(query_profile)
            query_profile["answer_shape"] = _qc_answer_shape

        # Keep planner order in final report.
        order = {call.name: i for i, call in enumerate(allowed_plan)}
        tool_results.sort(key=lambda x: order.get(x.tool, 999))
        tool_results = _apply_reference_limits(tool_results, normalized_search_mode, query_profile)
        # Phase-2 fan-out: per-item TMDB/Wiki enrichment for all list_plus_expand queries.
        _qc_media_family = str((query_classification or {}).get("media_family", "") or "")
        if _qc_answer_shape == "list_plus_expand":
            tool_results = _execute_per_item_expansion(
                tool_results, trace_id=resolved_trace_id, media_family=_qc_media_family
            )
        debug_trace["tool_results"] = [
            {"tool": r.tool, "status": r.status, "summary": r.summary, "data": r.data}
            for r in tool_results
        ]
        doc_tool_result = next((r for r in tool_results if r.tool == TOOL_QUERY_DOC_RAG), None)
        if doc_tool_result and isinstance(doc_tool_result.data, dict):
            debug_trace["query_rewrite"] = doc_tool_result.data.get("query_rewrite", {})

        _doc_data = doc_tool_result.data if (doc_tool_result and isinstance(doc_tool_result.data, dict)) else {}
        _doc_vector_recall_s = float(_doc_data.get("vector_recall_seconds", 0) or 0)
        _doc_rerank_s = float(_doc_data.get("rerank_seconds", 0) or 0)
        _doc_top1_score = _doc_data.get("doc_top1_score")
        _doc_top1_score_before_rerank = _doc_data.get("doc_top1_score_before_rerank")
        _doc_top1_identity_changed = _doc_data.get("doc_top1_identity_changed")
        _doc_top1_rank_shift = _doc_data.get("doc_top1_rank_shift")
        _doc_no_context = int(_doc_data.get("no_context", 0) or 0)
        _doc_embed_cache_hit = int(_doc_data.get("embed_cache_hit", 0) or 0)
        _doc_query_rewrite_hit = int(_doc_data.get("query_rewrite_hit", 0) or 0)
        _doc_threshold = float(query_profile.get("doc_score_threshold", DOC_SCORE_THRESHOLD) or DOC_SCORE_THRESHOLD)
        _update_post_retrieval_fallback_evidence(runtime_state, query_classification, tool_results, _doc_data, query_profile)

        if _doc_no_context and not benchmark_mode:
            try:
                boundary_log_no_context_query(
                    q,
                    source="agent",
                    top1_score=float(_doc_top1_score) if _doc_top1_score is not None else None,
                    threshold=_doc_threshold,
                    trace_id=resolved_trace_id,
                    reason="below_threshold",
                )
            except Exception:
                pass

        media_tool_result = next((r for r in tool_results if r.tool == TOOL_QUERY_MEDIA), None)
        media_rows = media_tool_result.data.get("results", []) if (media_tool_result and isinstance(media_tool_result.data, dict)) else []
        media_validation = _get_media_validation(tool_results)
        guardrail_flags = _build_guardrail_flags(runtime_state, media_validation)
        answer_mode = _build_guardrail_answer_mode(q, runtime_state, tool_results, guardrail_flags)
        query_classification["guardrail_flags"] = guardrail_flags
        query_classification["answer_guardrail_mode"] = {
            "mode": answer_mode.get("mode", "normal"),
            "reasons": list(answer_mode.get("reasons") or []),
        }
        _post_retrieval_outcome: _PostRetrievalOutcome = _PostRetrievalPolicy().evaluate(
            runtime_state.decision, tool_results, guardrail_flags
        )
        _answer_strategy: _AnswerStrategy = _AnswerPolicy().determine(
            runtime_state.decision, _post_retrieval_outcome
        )
        query_classification["post_retrieval_outcome"] = _dc_asdict(_post_retrieval_outcome)
        query_classification["answer_strategy"] = _dc_asdict(_answer_strategy)
        if not benchmark_mode and any(call.name == TOOL_QUERY_MEDIA for call in allowed_plan) and not media_rows:
            _log_agent_media_miss(q, query_profile)

        web_calls = sum(
            1 for r in tool_results
            if r.tool == TOOL_SEARCH_WEB
            and r.status in {"ok", "empty", "error"}
            and not (isinstance(r.data, dict) and r.data.get("cache_hit"))
        )
        if web_calls:
            _increment_quota_state(quota_state, web_search_delta=web_calls)

        _web_cache_hit = int(any(
            r.tool == TOOL_SEARCH_WEB
            and isinstance(r.data, dict)
            and r.data.get("cache_hit")
            for r in tool_results
        ))
        _rag_used = int(any(call.name == TOOL_QUERY_DOC_RAG for call in allowed_plan))
        _media_used = int(any(call.name == TOOL_QUERY_MEDIA for call in allowed_plan))
        _web_used = int(any(call.name == TOOL_SEARCH_WEB for call in allowed_plan))
        _agent_no_context, _agent_no_context_reason = _resolve_agent_no_context(
            _get_query_type(runtime_state=runtime_state),
            _rag_used,
            _doc_no_context,
        )

        if _agent_no_context and not _doc_no_context and not benchmark_mode:
            try:
                boundary_log_no_context_query(
                    q,
                    source="agent",
                    top1_score=float(_doc_top1_score) if _doc_top1_score is not None else None,
                    threshold=_doc_threshold,
                    trace_id=resolved_trace_id,
                    reason=_agent_no_context_reason,
                )
            except Exception:
                pass

        if skipped_due_quota:
            for tool_name in skipped_due_quota:
                tool_results.append(
                    ToolExecution(tool=tool_name, status="skipped", summary="超过每日配额且已拒绝调用", data={"results": []})
                )

        yield {"type": "progress", "trace_id": resolved_trace_id, "message": "工具执行完毕，正在生成回答..."}

        degraded_to_retrieval = False
        degrade_reason = ""
        _context_assembly_t0 = _wall_time.perf_counter()
        memory_context = "" if benchmark_mode else build_memory_context(sid)
        _context_assembly_seconds = _wall_time.perf_counter() - _context_assembly_t0
        debug_trace["memory_context"] = memory_context
        debug_trace["memory_tokens_est"] = _approx_tokens(memory_context)
        _llm_stats: dict[str, Any] = {}
        _llm_t0 = _wall_time.perf_counter()
        structured_media_answer = _build_structured_media_answer(q, runtime_state, tool_results)
        if str(answer_mode.get("mode", "normal") or "normal") == "restricted":
            answer = str(answer_mode.get("answer", "") or "").strip()
            _llm_stats["backend"] = backend
            _llm_stats["calls"] = 0
            _llm_stats["input_tokens_est"] = 0
            _llm_stats["prompt_tokens_est"] = 0
            _llm_stats["context_tokens_est"] = 0
            _llm_stats["memory_tokens_est"] = _approx_tokens(memory_context)
            _llm_stats["output_tokens_est"] = _approx_tokens(answer)
        elif structured_media_answer:
            answer = structured_media_answer
            _llm_stats["backend"] = backend
            _llm_stats["calls"] = 0
            _llm_stats["input_tokens_est"] = 0
            _llm_stats["prompt_tokens_est"] = 0
            _llm_stats["context_tokens_est"] = 0
            _llm_stats["memory_tokens_est"] = _approx_tokens(memory_context)
            _llm_stats["output_tokens_est"] = _approx_tokens(answer)
        else:
            try:
                answer = _summarize_answer(
                    question=q,
                    history=hist,
                    memory_context=memory_context,
                    tool_results=tool_results,
                    backend=backend,
                    search_mode=normalized_search_mode,
                    quota_state=quota_state,
                    trace_id=resolved_trace_id,
                    debug_sink=debug_trace if debug else None,
                    llm_stats_sink=_llm_stats,
                    answer_strategy=_answer_strategy,
                )
            except Exception as exc:  # noqa: BLE001
                if (backend or "local").strip().lower() == "local":
                    degraded_to_retrieval = True
                    degrade_reason = str(exc)
                    answer = _fallback_retrieval_answer(q, tool_results, reason=degrade_reason)
                    _llm_stats["output_tokens_est"] = _approx_tokens(answer)
                else:
                    raise
        _llm_seconds = _wall_time.perf_counter() - _llm_t0
        answer = _apply_guardrail_answer_mode(answer, answer_mode)
        _llm_stats["output_tokens_est"] = _approx_tokens(answer)

        references_md = _build_references_markdown(tool_results, request_base_url=request_base_url)
        final_answer = answer
        if references_md:
            final_answer = f"{answer}{references_md}"

        if not benchmark_mode:
            append_message(sid, "assistant", final_answer, trace_id=resolved_trace_id)
            _update_memory_for_session(sid)
            _schedule_generated_session_title(sid, q, final_answer, lock=True)
            _auto_queue_bug_tickets(final_answer, session_id=sid, trace_id=resolved_trace_id)
        if debug:
            debug_trace["final_answer_tokens_est"] = _approx_tokens(final_answer)
            _write_debug_record(sid, debug_trace)

        if not benchmark_mode:
            try:
                record_agent_metrics(
                    query_profile=str(query_profile.get("profile", "medium") or "medium"),
                    search_mode=normalized_search_mode,
                    query_type=_get_query_type(runtime_state=runtime_state),
                    rag_used=_rag_used,
                    media_used=_media_used,
                    web_used=_web_used,
                    no_context=_agent_no_context,
                    no_context_reason=_agent_no_context_reason,
                    trace_id=resolved_trace_id,
                    doc_score_threshold=_doc_threshold,
                    doc_top1_score=float(_doc_top1_score) if _doc_top1_score is not None else None,
                    doc_top1_score_before_rerank=float(_doc_top1_score_before_rerank) if _doc_top1_score_before_rerank is not None else None,
                    doc_top1_identity_changed=int(_doc_top1_identity_changed) if _doc_top1_identity_changed is not None else None,
                    doc_top1_rank_shift=float(_doc_top1_rank_shift) if _doc_top1_rank_shift is not None else None,
                    embed_cache_hit=_doc_embed_cache_hit,
                    query_rewrite_hit=_doc_query_rewrite_hit,
                    vector_recall_seconds=_doc_vector_recall_s,
                    rerank_seconds=_doc_rerank_s,
                    wall_clock_seconds=_wall_time.perf_counter() - _wall_t0,
                )
            except Exception:
                pass

        trace_record = _build_agent_trace_record(
            trace_id=resolved_trace_id,
            session_id=sid,
            backend=backend,
            search_mode=normalized_search_mode,
            benchmark_mode=benchmark_mode,
            stream_mode=True,
            query_profile=query_profile,
            runtime_state=runtime_state,
            planned_tools=planned,
            tool_results=tool_results,
            doc_data=_doc_data,
            timings={
                "session_prepare_seconds": _session_prepare_seconds,
                "query_profile_seconds": _query_profile_seconds,
                "tool_planning_seconds": _tool_planning_seconds,
                "context_assembly_seconds": _context_assembly_seconds,
                "vector_recall_seconds": _doc_vector_recall_s,
                "rerank_seconds": _doc_rerank_s,
                "no_context": _agent_no_context,
                "no_context_reason": _agent_no_context_reason,
                "doc_score_threshold": _doc_threshold,
            },
            llm_stats=_llm_stats,
            answer_guardrail_mode=query_classification.get("answer_guardrail_mode", {}),
            degraded_to_retrieval=degraded_to_retrieval,
            degrade_reason=degrade_reason,
            wall_clock_seconds=_wall_time.perf_counter() - _wall_t0,
            planning_seconds=_planning_seconds,
            tool_execution_seconds=_tool_execution_seconds,
            llm_seconds=_llm_seconds,
        )
        trace_record["post_retrieval_outcome"] = query_classification.get("post_retrieval_outcome", {})
        trace_record["answer_strategy"] = query_classification.get("answer_strategy", {})
        _write_agent_trace_record(trace_record)

        yield {
            "type": "done",
            "trace_id": resolved_trace_id,
            "payload": {
                "requires_confirmation": False,
                "trace_id": resolved_trace_id,
                "session_id": sid,
                "answer": final_answer,
                "backend": backend,
                "search_mode": normalized_search_mode,
                "query_profile": query_profile,
                "query_classification": query_classification,
                "query_type": _get_query_type(runtime_state=runtime_state),
                "conversation_state_before": trace_record.get("conversation_state_before", {}),
                "detected_followup": bool(trace_record.get("detected_followup")),
                "inheritance_applied": trace_record.get("inheritance_applied", {}),
                "conversation_state_after": trace_record.get("conversation_state_after", {}),
                "state_diff": trace_record.get("state_diff", {}),
                "planner_snapshot": trace_record.get("planner_snapshot", {}),
                "guardrail_flags": trace_record.get("guardrail_flags", {}),
                "error_taxonomy": trace_record.get("error_taxonomy", {}),
                "answer_guardrail_mode": query_classification.get("answer_guardrail_mode", {}),
                "degraded_to_retrieval": degraded_to_retrieval,
                "degrade_reason": degrade_reason,
                "planned_tools": [{"name": c.name, "query": c.query} for c in planned],
                "tool_results": [
                    {"tool": r.tool, "status": r.status, "summary": r.summary, "data": r.data}
                    for r in tool_results
                ],
                "debug_enabled": bool(debug),
                "timings": {
                    "vector_recall_seconds": _doc_vector_recall_s,
                    "rerank_seconds": _doc_rerank_s,
                    "no_context": _agent_no_context,
                    "no_context_reason": _agent_no_context_reason,
                    "doc_score_threshold": _doc_threshold,
                    "web_cache_hit": _web_cache_hit,
                },
                "quota": {
                    "date": quota_state.get("date"),
                    "web_search": int(quota_state.get("web_search", 0) or 0),
                    "web_search_limit": WEB_SEARCH_DAILY_LIMIT,
                    "deepseek": int(quota_state.get("deepseek", 0) or 0),
                    "deepseek_limit": DEEPSEEK_DAILY_LIMIT,
                },
            },
        }

    except Exception as exc:  # noqa: BLE001
        yield {"type": "error", "trace_id": locals().get("resolved_trace_id", _normalize_trace_id(trace_id)), "message": str(exc)}
