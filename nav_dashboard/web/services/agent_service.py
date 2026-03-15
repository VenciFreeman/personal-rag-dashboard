"""nav_dashboard/web/services/agent_service.py
Agent 规划、工具执行与会话管理核心服务

═══════════════════════════════════════════════════════════════
  一次 Agent 对话的完整链路（run_agent_round）
═══════════════════════════════════════════════════════════════

  1. 配额检查
       · 每日 Web Search 配额（WEB_SEARCH_DAILY_LIMIT）
       · 每日 DeepSeek 调用配额（DEEPSEEK_DAILY_LIMIT）
       · 超限时先返回确认请求，由前端用户确认后再继续

  2. Query 长度分析（_resolve_query_profile）
       · 基于 token 估算（字符数/4）分 short / medium / long 三档
       · 不同档位对应不同的分数阈值、检索 top-n 参数、结果数量上限

  3. 工具规划（LLM planning → _parse_planned_tools）
       · 将问题、工具描述、记忆摘要传给本地 LLM，输出 JSON：
           {"tools": [{"name": "...", "args": {"query": "..."}}]}
       · 解析后去重，提取最多一次每个工具的调用
       · 若 LLM 输出无效/超时，回退到启发式规划器

  4. 工具执行（ThreadPoolExecutor 并行）
       a. query_document_rag
            · Query Rewrite（可选）：本地 LLM 将原始问题改写为最多 3 条检索 query
            · 多 query 向量召回：每条 query 分别调用 /api/rag/ask（top_n=20），
              结果 merge（同 doc_id 取最高分）
            · 结果经过 score 过滤（阈值自 query_profile 读取）
       b. query_media_record
            · 调用 Library Tracker /api/library/search（keyword + vector 双模式）
            · 两类结果分别按独立阈值过滤
       c. search_web
            · 调用 Tavily Search API，返回摘要及 URL
            · 仅 hybrid 模式启用；local_only 模式跳过
       d. expand_document_query / expand_media_query
            · 通过知识图谱扩展检索关键词（调用对应子项目的 graph expand 函数）
            · 扩展后将新关键词追加到 query_document_rag / query_media_record 结果

  5. 结果过滤与排序（_apply_reference_limits）
       · 按工具类型使用独立阈值对 score 字段过滤
       · 最终每类工具保留最多 top-k 条结果（short/long query 动态调整 ±N 条）

  6. 答案生成（_llm_summarize）
       · 将过滤后的工具结果拼装为 context，连同用户问题一起构建 prompt
       · 调用本地 LLM 或 DeepSeek 生成最终自然语言回答
       · 若本地 LLM 不可用，降级为仅返回检索结果的文本摘要
       · 回答末尾自动附加参考来源列表（文档标题/媒体条目/网址）

  7. 会话持久化
       · 问题和回答追加写入 data/agent_sessions/session_<id>.json
       · 滑动更新记忆摘要 data/agent_sessions/_memory/memory_<id>.json
         （最近 MEMORY_MAX_TURNS 轮）
       · debug=True 时将规划输入、工具结果、LLM Token 估算落盘到
         data/agent_sessions/debug_data/

═══════════════════════════════════════════════════════════════
  配额与历史记录
═══════════════════════════════════════════════════════════════
  · 今日用量：data/agent_quota.json（跨天自动清零）
  · 月度历史：data/agent_quota_history.json（可通过 PATCH /api/dashboard/usage 手动调整）
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
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterator
from urllib import error as urlerror
from urllib import parse as urlparse
from urllib import request as urlrequest
from uuid import uuid4

_LLM_IMPORT_ERROR: Exception | None = None

_WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
if str(_WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKSPACE_ROOT))
_AI_SUMMARY_SCRIPTS_DIR = _WORKSPACE_ROOT / "ai_conversations_summary" / "scripts"
if str(_AI_SUMMARY_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_AI_SUMMARY_SCRIPTS_DIR))

import ask_rag as ask_rag_script
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

# Optional knowledge graph support for query expansion
_GRAPH_IMPORT_ERROR: Exception | None = None
_doc_graph_expand = None
_media_graph_expand = None
try:
    sys.path.insert(0, str(_WORKSPACE_ROOT / "ai_conversations_summary"))
    from scripts.rag_knowledge_graph import expand_query_by_graph as doc_expand_query_by_graph
    _doc_graph_expand = doc_expand_query_by_graph
except Exception as exc:  # noqa: BLE001
    _GRAPH_IMPORT_ERROR = exc

try:
    sys.path.insert(0, str(_WORKSPACE_ROOT / "library_tracker"))
    library_graph_path = _WORKSPACE_ROOT / "library_tracker" / "web" / "services" / "library_graph.py"
    media_spec = importlib.util.spec_from_file_location("_library_tracker_graph", library_graph_path)
    if media_spec is None or media_spec.loader is None:
        raise RuntimeError(f"failed to load spec for {library_graph_path}")
    media_module = importlib.util.module_from_spec(media_spec)
    media_spec.loader.exec_module(media_module)
    _media_graph_expand = getattr(media_module, "expand_library_query", None)
    if _media_graph_expand is None:
        raise AttributeError("expand_library_query not found in library_graph.py")
except Exception as exc:  # noqa: BLE001
    _GRAPH_IMPORT_ERROR = exc

# Optional cache support (embedding cache + web search cache)
_CACHE_IMPORT_ERROR: Exception | None = None
_get_web_cache = None
_log_no_context_query = None
try:
    from scripts.cache_db import get_web_cache as _get_web_cache
    from scripts.cache_db import log_no_context_query as _log_no_context_query
except Exception as exc:  # noqa: BLE001
    _CACHE_IMPORT_ERROR = exc


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

TOOL_QUERY_DOC_RAG = "query_document_rag"
TOOL_QUERY_MEDIA = "query_media_record"
TOOL_SEARCH_WEB = "search_web"
TOOL_EXPAND_DOC_QUERY = "expand_document_query"
TOOL_EXPAND_MEDIA_QUERY = "expand_media_query"
TOOL_SEARCH_MEDIAWIKI = "search_mediawiki_action"
TOOL_PARSE_MEDIAWIKI = "parse_mediawiki_page"
TOOL_EXPAND_MEDIAWIKI_CONCEPT = "expand_mediawiki_concept"
TOOL_SEARCH_TMDB = "search_tmdb_media"

QUERY_TYPE_TECH = "TECH_QUERY"
QUERY_TYPE_MEDIA = "MEDIA_QUERY"
QUERY_TYPE_MIXED = "MIXED_QUERY"
QUERY_TYPE_GENERAL = "GENERAL_QUERY"
CLASSIFIER_LABEL_MEDIA = "MEDIA"
CLASSIFIER_LABEL_TECH = "TECH"
CLASSIFIER_LABEL_OTHER = "OTHER"
TECH_SPACE_PREFIXES = (
    "ai-governance/",
    "career-learning/",
    "cognition-method/",
    "examples/",
    "industry-tech/",
    "science/",
)

TOOL_NAMES = [
    TOOL_QUERY_DOC_RAG,
    TOOL_QUERY_MEDIA,
    TOOL_SEARCH_WEB,
    TOOL_EXPAND_DOC_QUERY,
    TOOL_EXPAND_MEDIA_QUERY,
    TOOL_SEARCH_MEDIAWIKI,
    TOOL_PARSE_MEDIAWIKI,
    TOOL_EXPAND_MEDIAWIKI_CONCEPT,
    TOOL_SEARCH_TMDB,
]
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
_MEDIA_GRAPH_CACHE: dict[str, Any] = {"mtime": None, "degrees": {}}
_MEDIA_COMPARE_SPLIT_RE = re.compile(r"\s*(?:和|与|以及|及|跟|vs\.?|VS\.?|/|／|、)\s*")
_MEDIA_TITLE_MARKER_RE = re.compile(r"(?:《[^》]+》|「[^」]+」|『[^』]+』|“[^”]+”|\"[^\"]+\")")
_MEDIA_LIBRARY_VOCAB_CACHE: dict[str, Any] = {
    "signature": None,
    "data": {"nationalities": [], "authors": [], "categories": [], "titles": []},
}
_MEDIAWIKI_CONCEPT_CACHE: dict[str, Any] = {"entries": {}, "lock": threading.RLock()}
PROMPT_HISTORY_MAX_MESSAGES = int(os.getenv("NAV_DASHBOARD_PROMPT_HISTORY_MAX_MESSAGES", "6") or "6")
PROMPT_HISTORY_ITEM_MAX_CHARS = int(os.getenv("NAV_DASHBOARD_PROMPT_HISTORY_ITEM_MAX_CHARS", "360") or "360")
PROMPT_MEMORY_MAX_CHARS = int(os.getenv("NAV_DASHBOARD_PROMPT_MEMORY_MAX_CHARS", "1800") or "1800")
PROMPT_TOOL_CONTEXT_MAX_CHARS = int(os.getenv("NAV_DASHBOARD_PROMPT_TOOL_CONTEXT_MAX_CHARS", "5200") or "5200")
PROMPT_TOOL_CONTEXT_RETRY_CHARS = int(os.getenv("NAV_DASHBOARD_PROMPT_TOOL_CONTEXT_RETRY_CHARS", "3000") or "3000")
PROMPT_TOOL_RESULT_MAX_CHARS = int(os.getenv("NAV_DASHBOARD_PROMPT_TOOL_RESULT_MAX_CHARS", "1700") or "1700")
PROMPT_TOOL_RESULT_MIN_CHARS = int(os.getenv("NAV_DASHBOARD_PROMPT_TOOL_RESULT_MIN_CHARS", "420") or "420")


def _is_doc_graph_available() -> bool:
    return _doc_graph_expand is not None


def _is_media_graph_available() -> bool:
    return _media_graph_expand is not None


def _allowed_tool_names(search_mode: str) -> list[str]:
    normalized_mode = _normalize_search_mode(search_mode)
    allowed = [
        TOOL_QUERY_DOC_RAG,
        TOOL_QUERY_MEDIA,
        TOOL_SEARCH_MEDIAWIKI,
        TOOL_PARSE_MEDIAWIKI,
        TOOL_EXPAND_MEDIAWIKI_CONCEPT,
        TOOL_SEARCH_TMDB,
    ]
    if normalized_mode == "hybrid":
        allowed.append(TOOL_SEARCH_WEB)
    if _is_doc_graph_available():
        allowed.append(TOOL_EXPAND_DOC_QUERY)
    if _is_media_graph_available():
        allowed.append(TOOL_EXPAND_MEDIA_QUERY)
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
    if _is_doc_graph_available():
        lines.append(f"- {TOOL_EXPAND_DOC_QUERY}: 使用知识图谱扩展文档查询（可获取相关概念）")
    if _is_media_graph_available():
        lines.append(f"- {TOOL_EXPAND_MEDIA_QUERY}: 使用知识图谱扩展媒体查询")
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
    return max(0, int(len(value) / 4))


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


@dataclass
class PlannedToolCall:
    name: str
    query: str


@dataclass
class ToolExecution:
    tool: str
    status: str
    summary: str
    data: Any


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
    # Prefer explicit public URL; otherwise derive from current request host with tracker port.
    if LIBRARY_TRACKER_PUBLIC_BASE:
        return LIBRARY_TRACKER_PUBLIC_BASE

    internal_parsed = urlparse.urlparse(LIBRARY_TRACKER_BASE)
    tracker_scheme = (internal_parsed.scheme or "http").strip() or "http"
    tracker_port = internal_parsed.port

    req = (request_base_url or "").strip()
    if req:
        parsed = urlparse.urlparse(req)
        host = (parsed.hostname or "").strip()
        scheme = (parsed.scheme or "").strip()
        if host:
            final_scheme = scheme or tracker_scheme
            if tracker_port:
                return f"{final_scheme}://{host}:{tracker_port}"
            return f"{final_scheme}://{host}"

    return LIBRARY_TRACKER_BASE


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
    if local_url and local_model:
        try:
            title = ask_rag_script.generate_session_title(
                question,
                answer,
                api_key=local_key,
                api_url=local_url,
                model=local_model,
                timeout=20,
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
    media_types = [str(value).strip().lower() for value in filters.get("media_type", []) if str(value).strip()]
    categories = [str(value).strip() for value in filters.get("category", []) if str(value).strip()]
    if "video" in media_types and any(category == "动画" for category in categories):
        return "anime"
    if media_types:
        return media_types[0]
    if any(token in str(resolved_question or "") for token in ("动画", "动漫", "番", "番剧", "新番")):
        return "anime"
    return ""


def _build_resolved_query_state(
    original_question: str,
    resolved_question: str,
    query_classification: dict[str, Any],
) -> dict[str, Any]:
    filters = _infer_media_filters(resolved_question)
    fallback_year = _extract_year_from_date_range(query_classification.get("previous_date_range"))
    date_window = _parse_media_date_window(resolved_question, fallback_year)
    carry_over = str(original_question or "").strip() != str(resolved_question or "").strip()
    inherited_kind = ""
    if carry_over and _looks_like_time_only_followup(original_question):
        inherited_kind = "time_window_replace"
    elif carry_over:
        inherited_kind = "followup_expand"
    if _is_collection_media_query(resolved_question) or date_window:
        intent = "filter_search"
        sort = "rating_desc"
    elif bool(query_classification.get("media_entity_confident")):
        intent = "entity_lookup"
        sort = "relevance"
    else:
        intent = "general_lookup"
        sort = "relevance"
    return {
        "intent": intent,
        "media_type": _resolved_media_type_label(filters, resolved_question),
        "filters": filters,
        "date_range": [date_window.get("start", ""), date_window.get("end", "")] if date_window else [],
        "sort": sort,
        "carry_over_from_previous_turn": carry_over,
        "inherited_context": {
            "used": carry_over,
            "kind": inherited_kind,
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


def _build_conversation_state_snapshot(
    question: str,
    query_classification: dict[str, Any] | None = None,
    resolved_query_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    text = str(question or "").strip()
    if not text:
        return {}
    current = query_classification or {}
    entities = [
        str(item).strip()
        for item in (current.get("media_entities") or _extract_media_entities(text))
        if str(item).strip()
    ]
    state = resolved_query_state or _build_resolved_query_state(
        text,
        text,
        {
            "media_entity_confident": bool(current.get("media_entity_confident")) or bool(entities),
        },
    )
    return {
        "question": text,
        "intent": str(state.get("intent", "") or ""),
        "media_type": str(state.get("media_type", "") or ""),
        "entity": entities[0] if entities else "",
        "entities": entities,
        "filters": state.get("filters", {}),
        "date_range": state.get("date_range", []),
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
    for field in ("intent", "media_type", "entity", "filters", "date_range", "sort"):
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
    previous_trace = _find_previous_trace_context(history)
    previous_trace_state = previous_trace.get("conversation_state_after") if isinstance(previous_trace.get("conversation_state_after"), dict) else {}
    previous_trace_resolved_question = ""
    if isinstance(previous_trace.get("query_understanding"), dict):
        previous_trace_resolved_question = str(previous_trace.get("query_understanding", {}).get("resolved_question", "") or "").strip()
    previous_question = _find_previous_user_question(original_question, history)
    conversation_state_before = dict(previous_trace_state) if previous_trace_state else (_build_conversation_state_snapshot(previous_question) if previous_question else {})
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
        "intent": _describe_inheritance_transition(
            conversation_state_before.get("intent"),
            conversation_state_after.get("intent"),
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
    entity = str(state_after.get("entity", "") or "")
    intent = str(state_after.get("intent", "") or "")
    query_text: str | None = entity or None
    if query_text is None and intent != "filter_search":
        query_text = resolved_question or None
    followup_mode = str((resolved_query_state.get("inherited_context") or {}).get("kind", "") or "none")
    return {
        "intent": intent,
        "hard_filters": hard_filters,
        "soft_constraints": soft_constraints,
        "query_text": query_text,
        "followup_mode": followup_mode,
        "planned_tools": [call.name for call in planned_tools],
    }


def _is_collection_media_query(query: str) -> bool:
    text = str(query or "").strip()
    if not text:
        return False
    if _has_media_title_marker(text):
        return False
    return any(cue in text for cue in MEDIA_COLLECTION_CUES)


def _needs_filter_only_media_lookup(query: str, media_entities: list[str], filters: dict[str, list[str]], date_window: dict[str, str]) -> bool:
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
    current = str(question or "").strip()
    if not current or not _is_context_dependent_followup(current):
        return current
    previous_trace = _find_previous_trace_context(history)
    previous = ""
    if isinstance(previous_trace.get("query_understanding"), dict):
        previous = str(previous_trace.get("query_understanding", {}).get("resolved_question", "") or "").strip()
    if not previous and isinstance(previous_trace.get("conversation_state_after"), dict):
        previous = str(previous_trace.get("conversation_state_after", {}).get("question", "") or "").strip()
    if not previous:
        previous = _find_previous_user_question(current, history)
    if _looks_like_time_only_followup(current):
        previous_state = previous_trace.get("conversation_state_after") if isinstance(previous_trace.get("conversation_state_after"), dict) else {}
        previous_question = _find_previous_user_question(current, history)
        rich_previous_trace = _find_previous_trace_context(history, require_media_context=True)
        rich_previous = ""
        if isinstance(rich_previous_trace.get("query_understanding"), dict):
            rich_previous = str(rich_previous_trace.get("query_understanding", {}).get("resolved_question", "") or "").strip()
        if not rich_previous and isinstance(rich_previous_trace.get("conversation_state_after"), dict):
            rich_previous = str(rich_previous_trace.get("conversation_state_after", {}).get("question", "") or "").strip()
        if previous_question and _is_context_dependent_followup(previous_question) and rich_previous and not _state_has_media_context(previous_state):
            previous = f"{rich_previous} {previous_question}".strip()
        elif rich_previous:
            previous = rich_previous
    if previous:
        if _looks_like_time_only_followup(current):
            return _replace_time_window_in_query(previous, current)
        return f"{previous} {current}".strip()
    return current


def _should_route_tmdb(query: str, classification: dict[str, Any] | None = None) -> bool:
    text = str(query or "").strip()
    if not text:
        return False
    if _is_collection_media_query(text):
        return False
    lowered = text.casefold()
    if any(cue in lowered for cue in ("文学", "小说", "诗歌", "散文", "作家", "读过", "阅读", "书")) and not any(cue in text for cue in TMDB_AUDIOVISUAL_CUES):
        return False
    if any(cue in text for cue in TMDB_AUDIOVISUAL_CUES):
        return True
    current = classification or {}
    if any(cue in text for cue in ("剧情", "简介", "介绍", "讲了什么")) and bool(current.get("media_intent_cues")):
        return not _is_abstract_media_concept_query(text, current)
    return bool(current.get("media_entity_confident")) and bool(current.get("media_intent_cues")) and not _is_abstract_media_concept_query(text, current)


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


def _classify_media_query_with_llm(query: str, quota_state: dict[str, Any]) -> dict[str, Any]:
    prompt = (
        "You are a classifier.\n\n"
        "Decide if the query asks about a specific media item "
        "(movie, anime, book, game), a technical/software topic, or something else.\n\n"
        "Reply with ONLY one token:\n\n"
        "MEDIA\n"
        "TECH\n"
        "OTHER\n\n"
        f"Query:\n{query}"
    )
    try:
        raw = _llm_chat(
            messages=[{"role": "user", "content": prompt}],
            backend="local",
            quota_state=quota_state,
            count_quota=False,
        )
        parsed = _parse_classifier_label(raw)
        return {
            "available": True,
            "answer": str(raw or "").strip(),
            "label": parsed,
            "is_media": parsed == CLASSIFIER_LABEL_MEDIA,
            "parsed": parsed,
        }
    except Exception as exc:  # noqa: BLE001
        return {"available": False, "answer": str(exc), "label": CLASSIFIER_LABEL_OTHER, "is_media": False, "parsed": None}


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
    media_entities = _extract_media_entities(query)
    extracted_entity = media_entities[0] if media_entities else ""
    llm_media = _classify_media_query_with_llm(query, quota_state)
    doc_similarity = _estimate_doc_similarity(query, query_profile)
    tech_score = float(doc_similarity.get("score", 0.0) or 0.0)
    classifier_label = str(llm_media.get("label") or CLASSIFIER_LABEL_OTHER)
    media_specific = bool(extracted_entity)
    query_tokens = _classifier_token_count(query)
    profile_name = str(query_profile.get("profile", "") or "").strip().lower()
    profile_token_count = int(query_profile.get("token_count", query_tokens) or query_tokens)
    media_title_marked = _has_media_title_marker(query)
    media_intent_cues = _has_media_intent_cues(query)
    short_media_surface = (
        query_tokens <= 8
        and profile_token_count <= 10
        and profile_name != "long"
    )
    media_entity_confident = media_specific and (
        media_title_marked
        or media_intent_cues
        or (short_media_surface and _is_compact_media_entity_list(media_entities))
    )
    weak_tech_threshold = max(0.18, TECH_QUERY_DOC_SIM_THRESHOLD - 0.10)
    weak_tech_signal = tech_score >= weak_tech_threshold
    disable_media_search = (
        query_tokens > 12
        and not media_entity_confident
        and not media_intent_cues
    )
    media_signal = (
        media_entity_confident
        or (media_intent_cues and not disable_media_search)
        or (
            classifier_label == CLASSIFIER_LABEL_MEDIA
            and (media_intent_cues or short_media_surface)
            and not disable_media_search
        )
    )
    abstract_media_concept = _is_abstract_media_concept_query(query)
    tmdb_candidate = _should_route_tmdb(
        query,
        {
            "media_entity_confident": media_entity_confident,
            "media_intent_cues": media_intent_cues,
        },
    )
    strong_tech_signal = tech_score >= TECH_QUERY_DOC_SIM_THRESHOLD or classifier_label == CLASSIFIER_LABEL_TECH

    if media_signal and strong_tech_signal:
        query_type = QUERY_TYPE_MIXED
    elif media_entity_confident or abstract_media_concept:
        query_type = QUERY_TYPE_MEDIA
    elif tech_score >= TECH_QUERY_DOC_SIM_THRESHOLD:
        query_type = QUERY_TYPE_TECH
    elif weak_tech_signal and not media_intent_cues:
        query_type = QUERY_TYPE_TECH
    elif classifier_label == CLASSIFIER_LABEL_TECH:
        query_type = QUERY_TYPE_TECH
    elif media_signal:
        query_type = QUERY_TYPE_MEDIA
    else:
        query_type = QUERY_TYPE_GENERAL

    return {
        "query_type": query_type,
        "media_entity": extracted_entity,
        "media_entities": media_entities,
        "media_specific": media_specific,
        "media_entity_confident": media_entity_confident,
        "media_title_marked": media_title_marked,
        "media_intent_cues": media_intent_cues,
        "llm_media": llm_media,
        "doc_similarity": doc_similarity,
        "tech_score": round(tech_score, 6),
        "tech_threshold": TECH_QUERY_DOC_SIM_THRESHOLD,
        "weak_tech_threshold": weak_tech_threshold,
        "weak_tech_signal": weak_tech_signal,
        "query_tokens": query_tokens,
        "profile_token_count": profile_token_count,
        "short_media_surface": short_media_surface,
        "disable_media_search": disable_media_search,
        "media_signal": media_signal,
        "strong_tech_signal": strong_tech_signal,
        "abstract_media_concept": abstract_media_concept,
        "tmdb_candidate": tmdb_candidate,
    }


def _build_router_decision_path(
    query_classification: dict[str, Any],
    search_mode: str,
    planned_tools: list[PlannedToolCall],
    tool_results: list[ToolExecution],
) -> tuple[str, list[str]]:
    path: list[str] = []
    query_type = str(query_classification.get("query_type", QUERY_TYPE_GENERAL) or QUERY_TYPE_GENERAL)
    classifier_label = str(query_classification.get("llm_media", {}).get("label", "") or "")
    doc_similarity = float(query_classification.get("doc_similarity", {}).get("score", 0.0) or 0.0)
    weak_tech_threshold = float(query_classification.get("weak_tech_threshold", 0.0) or 0.0)

    if bool(query_classification.get("media_entity_confident")):
        path.append("media_entity")
    elif bool(query_classification.get("media_signal")):
        path.append("media_intent")
    if any(call.name == TOOL_EXPAND_MEDIAWIKI_CONCEPT for call in planned_tools):
        path.append("external_concept_expand")
    if any(call.name == TOOL_SEARCH_TMDB for call in planned_tools):
        path.append("external_media_db")

    if doc_similarity >= TECH_QUERY_DOC_SIM_THRESHOLD:
        path.append("embedding_similarity_strong")
    elif weak_tech_threshold > 0 and doc_similarity >= weak_tech_threshold:
        path.append("embedding_similarity_weak")

    if classifier_label == CLASSIFIER_LABEL_TECH:
        path.append("llm_classifier_tech")
    elif classifier_label == CLASSIFIER_LABEL_MEDIA:
        path.append("llm_classifier_media")

    normalized_mode = _normalize_search_mode(search_mode)
    if normalized_mode == "hybrid" and any(call.name == TOOL_SEARCH_WEB for call in planned_tools):
        path.append("hybrid_web_fallback")
    elif normalized_mode == "local_only":
        path.append("local_only")

    if query_type == QUERY_TYPE_MIXED:
        path.append("mixed_multi_tool")
        category = "mixed_multi_tool"
    elif "embedding_similarity_strong" in path or "llm_classifier_tech" in path:
        category = "tech_rag"
    elif "media_entity" in path or "llm_classifier_media" in path:
        category = "media_lookup"
    elif "hybrid_web_fallback" in path:
        category = "web_fallback"
    else:
        category = "default_doc_rag"

    executed_tools = [item for item in tool_results if str(item.status or "").strip().lower() != "skipped"]
    if len(executed_tools) > 1:
        path.append("multi_tool_executed")

    return category, path


def _build_tool_plan_from_query_type(
    question: str,
    query_type: str,
    search_mode: str,
    query_classification: dict[str, Any] | None = None,
) -> list[PlannedToolCall]:
    normalized_type = _normalize_query_type(query_type)
    normalized_mode = _normalize_search_mode(search_mode)
    current = query_classification or {}
    needs_concept_expand = _is_abstract_media_concept_query(question, current)
    needs_tmdb = _should_route_tmdb(question, current)
    if normalized_type == QUERY_TYPE_MEDIA:
        plan: list[PlannedToolCall] = []
        if needs_concept_expand:
            plan.append(PlannedToolCall(name=TOOL_EXPAND_MEDIAWIKI_CONCEPT, query=question))
        plan.append(PlannedToolCall(name=TOOL_QUERY_MEDIA, query=question))
        if needs_tmdb:
            plan.append(PlannedToolCall(name=TOOL_SEARCH_TMDB, query=question))
        return plan
    if normalized_type == QUERY_TYPE_MIXED:
        plan = []
        if needs_concept_expand:
            plan.append(PlannedToolCall(name=TOOL_EXPAND_MEDIAWIKI_CONCEPT, query=question))
        plan.extend(
            [
                PlannedToolCall(name=TOOL_QUERY_DOC_RAG, query=question),
                PlannedToolCall(name=TOOL_QUERY_MEDIA, query=question),
            ]
        )
        if needs_tmdb:
            plan.append(PlannedToolCall(name=TOOL_SEARCH_TMDB, query=question))
        return plan
    if needs_tmdb:
        return [PlannedToolCall(name=TOOL_SEARCH_TMDB, query=question)]
    if normalized_type == QUERY_TYPE_TECH:
        return [PlannedToolCall(name=TOOL_QUERY_DOC_RAG, query=question)]
    if normalized_mode == "hybrid":
        return [PlannedToolCall(name=TOOL_SEARCH_WEB, query=question)]
    return [PlannedToolCall(name=TOOL_QUERY_DOC_RAG, query=question)]


def _score_value(row: dict[str, Any]) -> float | None:
    value = row.get("score", None)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        text = str(value).strip()
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
    if _log_no_context_query is None:
        return
    try:
        media_threshold = float(
            query_profile.get("media_vector_score_threshold", MEDIA_VECTOR_SCORE_THRESHOLD)
            or MEDIA_VECTOR_SCORE_THRESHOLD
        )
        _log_no_context_query(
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
            threshold_pass_rows.sort(key=lambda x: float(x.get("score", 0.0)), reverse=True)
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
            if isinstance(args, dict):
                query = str(args.get("query", "")).strip()
            if not query:
                query = question
            calls.append(PlannedToolCall(name=name, query=query))

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
    lowered = (question or "").lower()
    default_tools = [PlannedToolCall(name=TOOL_QUERY_DOC_RAG, query=question), PlannedToolCall(name=TOOL_QUERY_MEDIA, query=question)]
    if any(k in lowered for k in ["最新", "今天", "新闻", "联网", "实时", "recent", "news"]):
        default_tools.append(PlannedToolCall(name=TOOL_SEARCH_WEB, query=question))
    if _is_abstract_media_concept_query(question):
        default_tools.insert(0, PlannedToolCall(name=TOOL_EXPAND_MEDIAWIKI_CONCEPT, query=question))
    if _should_route_tmdb(question):
        default_tools.append(PlannedToolCall(name=TOOL_SEARCH_TMDB, query=question))
    return default_tools


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
    reranked: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        path = str(item.get("path", "")).strip()
        vector_score = _safe_score(item.get("vector_score"))
        keyword_score = keyword_scores.get(path, 0.0)
        final_score = (0.85 * vector_score) + (0.15 * keyword_score)
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
        title = str(match.group("title") or "").strip(" ，。！？?；;:\"'“”‘’（）()")
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
    if normalized in {"我", "我的", "自己", "本人", "我们", "咱们", "咱"}:
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
        entities = _split_media_entities(match.group("title"))
        if entities and not any(_looks_like_generic_media_scope(entity) for entity in entities):
            return entities

    match = re.search(r"^(?P<title>.+?)(?:的)?(?:对比|比较(?!高)|区别|差异)", normalized)
    if match:
        entities = _split_media_entities(match.group("title"))
        if entities:
            return entities

    match = re.search(r"^(?P<title>.+?)的(?:各个)?(?:主角|角色|剧情|介绍|评价|看法|分析|总结)", normalized)
    if match:
        entities = _split_media_entities(match.group("title"))
        if entities and not any(_looks_like_generic_media_scope(entity) for entity in entities):
            return entities

    match = re.search(r"^(?P<title>.+?)(?:这部|这个|这本|这套)?(?:电影|影片|片子|电视剧|剧集|剧|动漫|动画|番剧|漫画|小说|书)?呢$", normalized)
    if match:
        entities = _split_media_entities(match.group("title"))
        if entities and not any(_looks_like_generic_media_scope(entity) for entity in entities):
            return entities

    if any(token in normalized for token in ["对比", "比较", "区别", "差异", "评价", "看法", "评分"]):
        entities = _split_media_entities(normalized)
        if len(entities) >= 2 and not any(_looks_like_generic_media_scope(entity) for entity in entities):
            return entities

    entities = _extract_media_entities_from_local_titles(normalized)
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


def _media_title_match_boost(title: str, entity: str) -> float:
    t = _normalize_media_title_for_match(title)
    e = _normalize_media_title_for_match(entity)
    if not t or not e:
        return 0.0

    boost = 0.0
    if t == e:
        boost += 0.8
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


def _build_answer_focus_hints(question: str, tool_results: list[ToolExecution]) -> str:
    lines: list[str] = []
    normalized_question = str(question or "")
    wants_when = any(token in normalized_question for token in ["什么时候", "何时", "哪天", "哪一年", "观影时间", "时间"])
    wants_summary = any(token in normalized_question for token in ["剧情", "简介", "介绍", "讲了什么"])

    media_result = next((item for item in tool_results if item.tool == TOOL_QUERY_MEDIA), None)
    tmdb_result = next((item for item in tool_results if item.tool == TOOL_SEARCH_TMDB), None)
    mediawiki_result = next((item for item in tool_results if item.tool in {TOOL_SEARCH_MEDIAWIKI, TOOL_PARSE_MEDIAWIKI, TOOL_EXPAND_MEDIAWIKI_CONCEPT}), None)

    if media_result and isinstance(media_result.data, dict):
        exact_match = media_result.data.get("top_exact_match") if isinstance(media_result.data.get("top_exact_match"), dict) else None
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
            lines.append("若引用 Wiki，请明确标注为外部参考，不要表述成你的本地知识库内容。")

    return "\n".join(lines).strip()


def _tool_query_media_record(query: str, query_profile: dict[str, Any], trace_id: str = "") -> ToolExecution:
    rewritten_query = _rewrite_media_query(query)
    media_entities = _extract_media_entities(query)
    extracted_entity = media_entities[0] if media_entities else ""
    inferred_filters = _infer_media_filters(query)
    date_window = _parse_media_date_window(query)
    mediawiki_concept = _get_cached_mediawiki_concept(query)
    if mediawiki_concept is None and _is_abstract_media_concept_query(query):
        concept_result = _tool_expand_mediawiki_concept(query, trace_id)
        mediawiki_concept = concept_result.data if isinstance(concept_result.data, dict) else None

    graph_tool = _tool_expand_media_query(query) if _is_media_graph_available() else ToolExecution(
        tool=TOOL_EXPAND_MEDIA_QUERY,
        status="skipped",
        summary="媒体图谱扩展未启用",
        data={"original": query, "expanded": rewritten_query or query, "constraints": {}},
    )
    graph_data = graph_tool.data if isinstance(graph_tool.data, dict) else {}
    vector_query = _select_media_vector_query(
        query,
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

    vector_queries: list[str] = []
    for entity in media_entities:
        if entity and entity not in vector_queries:
            vector_queries.append(entity)
    if not vector_queries and vector_query:
        vector_queries.append(vector_query)
    if isinstance(mediawiki_concept, dict):
        for alias in mediawiki_concept.get("aliases", []) if isinstance(mediawiki_concept.get("aliases"), list) else []:
            clean_alias = str(alias).strip()
            if clean_alias and clean_alias not in vector_queries:
                vector_queries.append(clean_alias)
    for filter_query in filter_queries:
        if filter_query and filter_query not in vector_queries:
            vector_queries.append(filter_query)

    if _needs_filter_only_media_lookup(query, media_entities, filters, date_window):
        fallback_payload = _http_json(
            "POST",
            f"{LIBRARY_TRACKER_BASE}/api/library/search",
            payload={
                "query": "",
                "mode": "keyword",
                "limit": 80,
                "filters": filters,
            },
            headers={"X-Trace-Id": trace_id, "X-Trace-Stage": "agent.media.filter_window"},
        )
        fallback_rows = fallback_payload.get("results", []) if isinstance(fallback_payload, dict) else []
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
        compact, validation = _apply_media_result_validator(compact, filters=filters, date_window=date_window)
        compact.sort(
            key=lambda item: (
                _safe_score(item.get("rating")),
                str(item.get("date") or ""),
                _safe_score(item.get("score")),
            ),
            reverse=True,
        )
        return ToolExecution(
            tool=TOOL_QUERY_MEDIA,
            status="ok",
            summary=f"命中 {len(compact)} 条媒体记录（filter_only, date_window={json.dumps(date_window, ensure_ascii=False)}）",
            data={
                "trace_id": trace_id,
                "trace_stage": "agent.tool.query_media_record",
                "results": compact,
                "query_profile": query_profile,
                "media_entities": media_entities,
                "top_exact_match": None,
                "graph_expansion": {
                    "status": graph_tool.status,
                    "summary": graph_tool.summary,
                    "expanded_query": vector_query,
                    "constraints": filters,
                },
                "mediawiki_concept": mediawiki_concept or {},
                "date_window": date_window,
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
    compact.sort(key=lambda x: _safe_score(x.get("score")), reverse=True)
    if media_entities:
        compact = [row for row in compact if _safe_score(row.get("title_boost")) > 0]
    compact, validation = _apply_media_result_validator(compact, filters=filters, date_window=date_window)
    top_exact_match = None
    if media_entities:
        exact_matches = [
            row for row in compact
            if _media_title_match_boost_any(str(row.get("title") or ""), media_entities) >= 0.8
        ]
        if exact_matches:
            top_exact_match = exact_matches[0]
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
    used_query = f"vector:{' || '.join(vector_queries or [vector_query])}"
    if keyword_queries:
        used_query += f" | keyword:{' || '.join(keyword_queries)}"
    if filters:
        used_query += f" | filters:{json.dumps(filters, ensure_ascii=False)}"
    return ToolExecution(
        tool=TOOL_QUERY_MEDIA,
        status="ok",
        summary=f"命中 {len(compact)} 条媒体记录（entities={len(media_entities) or 1}, mode={used_mode}, query={used_query}）",
        data={
            "trace_id": trace_id,
            "trace_stage": "agent.tool.query_media_record",
            "results": compact,
            "query_profile": query_profile,
            "media_entities": media_entities,
            "top_exact_match": top_exact_match,
            "graph_expansion": {
                "status": graph_tool.status,
                "summary": graph_tool.summary,
                "expanded_query": vector_query,
                "constraints": filters,
            },
            "mediawiki_concept": mediawiki_concept or {},
            "date_window": date_window,
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
    if _get_web_cache is not None:
        try:
            cached = _get_web_cache().get(query, 5)
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
    if _get_web_cache is not None and compact:
        try:
            _get_web_cache().set(query, 5, compact)
        except Exception:
            pass
    return ToolExecution(
        tool=TOOL_SEARCH_WEB,
        status="ok",
        summary=f"命中 {len(compact)} 条网页结果",
        data={"trace_id": trace_id, "trace_stage": "agent.tool.search_web", "results": compact, "cache_hit": False},
    )


def _tool_expand_document_query(query: str) -> ToolExecution:
    """Expand document query using knowledge graph (ai_conversations_summary)."""
    if _doc_graph_expand is None:
        summary = "文档知识图谱不可用（未安装或导入失败）"
        return ToolExecution(
            tool=TOOL_EXPAND_DOC_QUERY,
            status="unavailable",
            summary=summary,
            data={"original": query, "expanded": query},
        )
    
    try:
        ai_summary_data_dir = _WORKSPACE_ROOT / "ai_conversations_summary" / "data" / "vector_db"
        if not ai_summary_data_dir.exists():
            summary = "文档图谱索引不存在"
            return ToolExecution(
                tool=TOOL_EXPAND_DOC_QUERY,
                status="empty",
                summary=summary,
                data={"original": query, "expanded": query},
            )
        
        expansion = _doc_graph_expand(ai_summary_data_dir, query)
        expanded_query = str(expansion.get("expanded_query") or query).strip() or query
        seed_concepts = expansion.get("seed_concepts") or []
        expanded_concepts = expansion.get("expanded_concepts") or []
        
        summary = f"查询扩展完成（种子概念: {len(seed_concepts)}, 扩展概念: {len(expanded_concepts)}）"
        return ToolExecution(
            tool=TOOL_EXPAND_DOC_QUERY,
            status="ok",
            summary=summary,
            data={
                "original": query,
                "expanded": expanded_query,
                "seed_concepts": seed_concepts,
                "expanded_concepts": expanded_concepts,
            },
        )
    except Exception as exc:  # noqa: BLE001
        summary = f"图谱扩展失败: {str(exc)}"
        return ToolExecution(
            tool=TOOL_EXPAND_DOC_QUERY,
            status="error",
            summary=summary,
            data={"original": query, "expanded": query},
        )


def _tool_expand_media_query(query: str) -> ToolExecution:
    """Expand media query using library knowledge graph."""
    if _media_graph_expand is None:
        summary = "媒体知识图谱不可用（未安装或导入失败）"
        return ToolExecution(
            tool=TOOL_EXPAND_MEDIA_QUERY,
            status="unavailable",
            summary=summary,
            data={"original": query, "expanded": query},
        )
    
    try:
        library_data_dir = _WORKSPACE_ROOT / "library_tracker" / "data" / "vector_db"
        if not library_data_dir.exists():
            summary = "媒体图谱索引不存在"
            return ToolExecution(
                tool=TOOL_EXPAND_MEDIA_QUERY,
                status="empty",
                summary=summary,
                data={"original": query, "expanded": query},
            )
        
        expansion = _media_graph_expand(graph_dir=library_data_dir, query=query)
        expanded_query = str(expansion.get("expanded_query") or query).strip() or query
        expanded_concepts = expansion.get("expanded_concepts") or []
        constraints = expansion.get("constraints") or {}
        
        summary = f"查询扩展完成（扩展概念: {len(expanded_concepts)}, 约束字段: {len(constraints)}）"
        return ToolExecution(
            tool=TOOL_EXPAND_MEDIA_QUERY,
            status="ok",
            summary=summary,
            data={
                "original": query,
                "expanded": expanded_query,
                "expanded_concepts": expanded_concepts,
                "constraints": constraints,
            },
        )
    except Exception as exc:  # noqa: BLE001
        summary = f"图谱扩展失败: {str(exc)}"
        return ToolExecution(
            tool=TOOL_EXPAND_MEDIA_QUERY,
            status="error",
            summary=summary,
            data={"original": query, "expanded": query},
        )


def _execute_tool(call: PlannedToolCall, query_profile: dict[str, Any], trace_id: str) -> ToolExecution:
    import time as _time

    _tool_t0 = _time.perf_counter()
    try:
        if call.name == TOOL_QUERY_DOC_RAG:
            result = _tool_query_document_rag(call.query, query_profile, trace_id)
        elif call.name == TOOL_QUERY_MEDIA:
            result = _tool_query_media_record(call.query, query_profile, trace_id)
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
) -> tuple[list[PlannedToolCall], dict[str, Any]]:
    resolved_question = _resolve_contextual_question(question, history)
    query_profile = _resolve_query_profile(resolved_question)
    query_classification = _classify_query_type(resolved_question, quota_state, query_profile)
    planned = _build_tool_plan_from_query_type(
        resolved_question,
        query_classification.get("query_type", QUERY_TYPE_GENERAL),
        search_mode,
        query_classification,
    )
    previous_trace_context = _find_previous_trace_context(history, require_media_context=True)
    previous_trace_state = previous_trace_context.get("conversation_state_after") if isinstance(previous_trace_context.get("conversation_state_after"), dict) else {}
    query_classification["previous_date_range"] = previous_trace_state.get("date_range", []) if isinstance(previous_trace_state, dict) else []
    resolved_query_state = _build_resolved_query_state(
        question,
        resolved_question,
        query_classification,
    )
    followup_transition = _build_followup_transition(
        question,
        resolved_question,
        history,
        query_classification,
        resolved_query_state,
    )

    original_question_text = str(question or "").strip()
    followup_before_state = followup_transition.get("conversation_state_before") if isinstance(followup_transition.get("conversation_state_before"), dict) else {}
    referential_media_followup = (
        bool(followup_transition.get("detected_followup"))
        and str(resolved_query_state.get("intent", "") or "") == "filter_search"
        and (bool(followup_before_state.get("filters")) or bool(followup_before_state.get("media_type")))
        and not _extract_media_entities(original_question_text)
        and (
            _question_requests_media_details(original_question_text)
            or _question_requests_personal_evaluation(original_question_text)
            or any(cue in original_question_text for cue in ("这些", "这些媒体", "这些作品", "这些条目", "它们", "前面这些", "上面这些"))
        )
    )
    resolved_media_evaluation_followup = (
        str(resolved_query_state.get("intent", "") or "") == "filter_search"
        and _state_has_media_context(
            {
                "media_type": resolved_query_state.get("media_type"),
                "filters": resolved_query_state.get("filters"),
                "entity": query_classification.get("media_entity"),
                "entities": query_classification.get("media_entities"),
            }
        )
        and _question_requests_personal_evaluation(resolved_question, query_classification)
    )
    resolved_media_filter_query = (
        str(resolved_query_state.get("intent", "") or "") == "filter_search"
        and _state_has_media_context(
            {
                "media_type": resolved_query_state.get("media_type"),
                "filters": resolved_query_state.get("filters"),
                "entity": query_classification.get("media_entity"),
                "entities": query_classification.get("media_entities"),
            }
        )
        and (bool(resolved_query_state.get("date_range")) or _is_collection_media_query(resolved_question) or _looks_like_time_only_followup(original_question_text))
    )
    if referential_media_followup or resolved_media_evaluation_followup or resolved_media_filter_query:
        query_classification["query_type"] = QUERY_TYPE_MEDIA
        planned = _build_tool_plan_from_query_type(
            resolved_question,
            QUERY_TYPE_MEDIA,
            search_mode,
            query_classification,
        )

    query_classification["original_question"] = question
    query_classification["resolved_question"] = resolved_question
    query_classification["resolved_query_state"] = resolved_query_state
    query_classification.update(followup_transition)
    query_classification["planner_snapshot"] = _build_planner_snapshot(
        resolved_question,
        query_classification,
        resolved_query_state,
        planned,
    )
    return planned, query_classification


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
) -> str:
    hist_lines = _trim_history_for_prompt(history)
    clipped_memory_context = _clip_memory_context(memory_context)
    answer_focus_hints = _build_answer_focus_hints(question, tool_results)

    normalized_search_mode = _normalize_search_mode(search_mode)
    has_web_tool = any(result.tool == TOOL_SEARCH_WEB for result in tool_results)
    system_prompt = (
        "你是个人助理。请综合工具结果回答用户问题。"
        "如果某个工具失败/为空，明确说明并尽量用其它工具补足。"
        "回答使用中文，结构清晰，避免编造。"
        "只允许使用工具结果中的事实；如果工具未给出证据必须明确说不确定。"
        "遇到同名/近似作品时优先按标题精确匹配（例如含数字续作）。"
    )
    if normalized_search_mode == "local_only" or not has_web_tool:
        system_prompt += "本轮未执行联网搜索，严禁写出“联网搜索”“网络搜索”“进行网络搜索”“经过搜索”等表述，也不要假装调用过外部 API。"

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
    query_classification: dict[str, Any],
    media_validation: dict[str, Any],
) -> dict[str, bool]:
    original_question = str(query_classification.get("original_question", "") or "").strip()
    resolved_state = query_classification.get("resolved_query_state") if isinstance(query_classification.get("resolved_query_state"), dict) else {}
    carry_over = bool(resolved_state.get("carry_over_from_previous_turn"))
    intent = str(resolved_state.get("intent", "") or "")
    raw_candidates_count = int(media_validation.get("raw_candidates_count", 0) or 0)
    dropped_by_validator = int(media_validation.get("dropped_by_validator", 0) or 0)
    dropped_by_reference_limit = int(media_validation.get("dropped_by_reference_limit", 0) or 0)
    returned_result_count = int(media_validation.get("returned_result_count", media_validation.get("post_filter_count", 0)) or 0)
    short_ambiguous_surface = len(original_question) <= 12 and any(token in original_question for token in ("那个", "这个", "那部", "这部", "那本", "这本"))
    low_confidence_understanding = bool(original_question) and not carry_over and (
        (
            len(original_question) <= 12
            and not query_classification.get("media_entities")
            and not resolved_state.get("filters")
            and not resolved_state.get("media_type")
        )
        or short_ambiguous_surface
    )
    return {
        "low_confidence_understanding": low_confidence_understanding,
        "high_validator_drop_rate": raw_candidates_count > 0 and (dropped_by_validator / raw_candidates_count) >= 0.4,
        "insufficient_valid_results": intent == "filter_search" and raw_candidates_count > 0 and returned_result_count <= 1,
        "state_inheritance_ambiguous": bool(query_classification.get("detected_followup")) and not bool(resolved_state.get("carry_over_from_previous_turn")),
        "answer_truncated_by_reference_limit": dropped_by_reference_limit > 0,
    }


def _build_error_taxonomy(
    query_classification: dict[str, Any],
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


def _build_agent_trace_record(
    *,
    trace_id: str,
    session_id: str,
    backend: str,
    search_mode: str,
    benchmark_mode: bool,
    stream_mode: bool,
    query_profile: dict[str, Any],
    query_classification: dict[str, Any],
    planned_tools: list[PlannedToolCall],
    tool_results: list[ToolExecution],
    doc_data: dict[str, Any],
    timings: dict[str, Any],
    llm_stats: dict[str, Any],
    degraded_to_retrieval: bool,
    degrade_reason: str,
    wall_clock_seconds: float,
    planning_seconds: float,
    tool_execution_seconds: float,
    llm_seconds: float,
) -> dict[str, Any]:
    _, llm_model, _, _ = _get_llm_profile(backend)
    resolved_query_state = query_classification.get("resolved_query_state") if isinstance(query_classification.get("resolved_query_state"), dict) else {}
    decision_category, decision_path = _build_router_decision_path(
        query_classification=query_classification,
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
    planner_snapshot = query_classification.get("planner_snapshot") if isinstance(query_classification.get("planner_snapshot"), dict) else {}
    guardrail_flags = _build_guardrail_flags(query_classification, media_validation)
    error_taxonomy = _build_error_taxonomy(query_classification, media_validation, guardrail_flags)
    answer_guardrail_mode = query_classification.get("answer_guardrail_mode") if isinstance(query_classification.get("answer_guardrail_mode"), dict) else {}
    router = {
        "selected_tool": planned_tools[0].name if planned_tools else "",
        "planned_tools": [call.name for call in planned_tools],
        "resolved_question": str(query_classification.get("resolved_question", "") or ""),
        "detected_intent": str(resolved_query_state.get("intent", "") or ""),
        "decision_category": decision_category,
        "decision_path": decision_path,
        "planned_tool_depth": len(planned_tools),
        "executed_tool_depth": executed_tool_depth,
        "classifier_label": str(query_classification.get("llm_media", {}).get("label", "") or ""),
        "doc_similarity": query_classification.get("doc_similarity", {}).get("score"),
        "media_entity_confident": bool(query_classification.get("media_entity_confident")),
        "entity_hit_count": len(list(query_classification.get("media_entities") or [])),
        "short_media_surface": bool(query_classification.get("short_media_surface")),
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
        tools.append(
            {
                "name": item.tool,
                "status": item.status,
                "latency_ms": data.get("latency_ms"),
                "result_count": len(results),
                "cache_hit": bool(data.get("cache_hit")),
                "trace_stage": str(data.get("trace_stage", "") or ""),
            }
        )
    return {
        "trace_id": trace_id,
        "timestamp": _now_iso(),
        "entrypoint": "agent",
        "call_type": "benchmark_case" if benchmark_mode else ("chat_stream" if stream_mode else "chat"),
        "session_id": session_id,
        "search_mode": search_mode,
        "conversation_state_before": query_classification.get("conversation_state_before", {}),
        "detected_followup": bool(query_classification.get("detected_followup")),
        "inheritance_applied": query_classification.get("inheritance_applied", {}),
        "conversation_state_after": query_classification.get("conversation_state_after", {}),
        "state_diff": query_classification.get("state_diff", {}),
        "query_understanding": {
            "original_question": str(query_classification.get("original_question", "") or ""),
            "resolved_question": str(query_classification.get("resolved_question", "") or ""),
            "detected_intent": str(resolved_query_state.get("intent", "") or ""),
            "entities": list(query_classification.get("media_entities") or []),
            "filters": resolved_query_state.get("filters", {}),
            "date_range": resolved_query_state.get("date_range", []),
            "inherited_context": resolved_query_state.get("inherited_context", {}),
            "carry_over_from_previous_turn": bool(resolved_query_state.get("carry_over_from_previous_turn")),
            "retrieval_plan": [call.name for call in planned_tools],
        },
        "planner_snapshot": planner_snapshot,
        "guardrail_flags": guardrail_flags,
        "error_taxonomy": error_taxonomy,
        "answer_guardrail_mode": answer_guardrail_mode,
        "query_type": str(query_classification.get("query_type", QUERY_TYPE_GENERAL) or QUERY_TYPE_GENERAL),
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


def _build_followup_answer_note(query_classification: dict[str, Any]) -> str:
    if not bool(query_classification.get("detected_followup")):
        return ""
    resolved_state = query_classification.get("resolved_query_state") if isinstance(query_classification.get("resolved_query_state"), dict) else {}
    if not bool(resolved_state.get("carry_over_from_previous_turn")):
        return ""
    planner_snapshot = query_classification.get("planner_snapshot") if isinstance(query_classification.get("planner_snapshot"), dict) else {}
    inheritance = query_classification.get("inheritance_applied") if isinstance(query_classification.get("inheritance_applied"), dict) else {}
    parts: list[str] = []
    scope = _describe_planner_scope(planner_snapshot)
    if inheritance.get("media_type") == "carried_over" or inheritance.get("filters") in {"carried_over", "overridden"}:
        parts.append(f"沿用了上一轮的{scope}约束")
    date_range = _format_guardrail_date_range((query_classification.get("conversation_state_after") or {}).get("date_range"))
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
    query_classification: dict[str, Any],
    tool_results: list[ToolExecution],
    *,
    include_guardrail_explanation: bool = False,
    include_followup_note: bool = False,
) -> str:
    rows = _get_media_result_rows(tool_results)
    if not rows:
        return ""
    query_type = str(query_classification.get("query_type", "") or "").strip().upper()
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

    resolved_state = query_classification.get("resolved_query_state") if isinstance(query_classification.get("resolved_query_state"), dict) else {}
    resolved_question = str(query_classification.get("resolved_question", "") or question or "").strip()
    intent = str(resolved_state.get("intent", "") or "").strip()
    wants_review = _question_requests_personal_evaluation(resolved_question, query_classification)
    wants_detail = _question_requests_media_details(resolved_question, query_classification)
    if intent != "filter_search" and not wants_review and not wants_detail:
        return ""

    validation = _get_media_validation(tool_results)
    returned_count = int(validation.get("returned_result_count", len(rows)) or 0)
    planner_snapshot = query_classification.get("planner_snapshot") if isinstance(query_classification.get("planner_snapshot"), dict) else {}
    scope = _describe_planner_scope(planner_snapshot)
    lines: list[str] = []
    followup_note = _build_followup_answer_note(query_classification)
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
    query_classification: dict[str, Any],
    tool_results: list[ToolExecution],
    guardrail_flags: dict[str, bool],
) -> str:
    structured = _build_structured_media_answer(
        question,
        query_classification,
        tool_results,
        include_guardrail_explanation=True,
        include_followup_note=True,
    )
    if structured:
        return structured

    lines: list[str] = []
    followup_note = _build_followup_answer_note(query_classification)
    if followup_note:
        lines.append(followup_note)
    validation = _get_media_validation(tool_results)
    rows = _get_media_result_rows(tool_results)
    if guardrail_flags.get("low_confidence_understanding"):
        lines.append("当前问题上下文不够明确，我无法安全判断要沿用哪一轮的对象或筛选条件，因此不直接给出结论。")
        lines.append("请明确作品名、时间范围或媒体类型后再问一次。")
        return "\n\n".join(lines).strip()

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
    query_classification: dict[str, Any],
    tool_results: list[ToolExecution],
    guardrail_flags: dict[str, bool],
) -> dict[str, Any]:
    reasons: list[str] = []
    annotation_lines: list[str] = []
    if guardrail_flags.get("low_confidence_understanding"):
        reasons.append("low_confidence_understanding")
    if guardrail_flags.get("insufficient_valid_results"):
        reasons.append("insufficient_valid_results")
    if reasons:
        return {
            "mode": "restricted",
            "reasons": reasons,
            "answer": _build_restricted_guardrail_answer(question, query_classification, tool_results, guardrail_flags),
            "annotations": [],
        }

    followup_note = _build_followup_answer_note(query_classification)
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

    normalized_search_mode = _normalize_search_mode(search_mode)
    query_profile = _resolve_query_profile(q)
    quota_state = _load_quota_state()
    _plan_t0 = _wall_time.perf_counter()
    planned, query_classification = _plan_tool_calls(q, hist, backend, quota_state, normalized_search_mode)
    _planning_seconds = _wall_time.perf_counter() - _plan_t0
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
        "planned_tools": [{"name": c.name, "query": c.query} for c in planned],
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
            "planned_tools": [{"name": c.name, "query": c.query} for c in planned],
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

    # Keep planner order in final report.
    order = {call.name: i for i, call in enumerate(allowed_plan)}
    tool_results.sort(key=lambda x: order.get(x.tool, 999))
    tool_results = _apply_reference_limits(tool_results, normalized_search_mode, query_profile)
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

    # Log no-context queries to shared jsonl file.
    if _doc_no_context and _log_no_context_query is not None and not benchmark_mode:
        try:
            _log_no_context_query(
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
    guardrail_flags = _build_guardrail_flags(query_classification, media_validation)
    answer_mode = _build_guardrail_answer_mode(q, query_classification, tool_results, guardrail_flags)
    query_classification["guardrail_flags"] = guardrail_flags
    query_classification["answer_guardrail_mode"] = {
        "mode": answer_mode.get("mode", "normal"),
        "reasons": list(answer_mode.get("reasons") or []),
    }
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
        str(query_classification.get("query_type", QUERY_TYPE_GENERAL) or QUERY_TYPE_GENERAL),
        _rag_used,
        _doc_no_context,
    )

    if _agent_no_context and not _doc_no_context and _log_no_context_query is not None and not benchmark_mode:
        try:
            _log_no_context_query(
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
    memory_context = "" if benchmark_mode else build_memory_context(sid)
    debug_trace["memory_context"] = memory_context
    debug_trace["memory_tokens_est"] = _approx_tokens(memory_context)
    _llm_stats: dict[str, Any] = {}
    _llm_t0 = _wall_time.perf_counter()
    structured_media_answer = _build_structured_media_answer(q, query_classification, tool_results)
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
    if debug:
        debug_trace["final_answer_tokens_est"] = _approx_tokens(final_answer)
        _write_debug_record(sid, debug_trace)

    # Record per-round agent metrics (best-effort; never raise). Skip for benchmark runs.
    if not benchmark_mode:
        try:
            record_agent_metrics(
                query_profile=str(query_profile.get("profile", "medium") or "medium"),
                search_mode=normalized_search_mode,
                query_type=str(query_classification.get("query_type", "general") or "general"),
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
        query_classification=query_classification,
        planned_tools=planned,
        tool_results=tool_results,
        doc_data=_doc_data,
        timings={
            "vector_recall_seconds": _doc_vector_recall_s,
            "rerank_seconds": _doc_rerank_s,
            "no_context": _agent_no_context,
            "no_context_reason": _agent_no_context_reason,
            "doc_score_threshold": _doc_threshold,
        },
        llm_stats=_llm_stats,
        degraded_to_retrieval=degraded_to_retrieval,
        degrade_reason=degrade_reason,
        wall_clock_seconds=_wall_time.perf_counter() - _wall_t0,
        planning_seconds=_planning_seconds,
        tool_execution_seconds=_tool_execution_seconds,
        llm_seconds=_llm_seconds,
    )
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

        normalized_search_mode = _normalize_search_mode(search_mode)
        query_profile = _resolve_query_profile(q)
        quota_state = _load_quota_state()

        yield {"type": "progress", "trace_id": resolved_trace_id, "message": "正在规划工具调用..."}

        _plan_t0 = _wall_time.perf_counter()
        planned, query_classification = _plan_tool_calls(q, hist, backend, quota_state, normalized_search_mode)
        _planning_seconds = _wall_time.perf_counter() - _plan_t0

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
            "planned_tools": [{"name": c.name, "query": c.query} for c in planned],
            "reranker": {"status": "not_applicable"},
        }

        yield {"type": "progress", "trace_id": resolved_trace_id, "message": f"查询分类：{query_classification.get('query_type', QUERY_TYPE_GENERAL)}"}

        exceeded = _quota_exceeded(planned, backend, quota_state)
        if exceeded and not confirm_over_quota and not deny_over_quota:
            yield {
                "type": "quota_exceeded",
                "trace_id": resolved_trace_id,
                "session_id": sid,
                "message": "已超过今日 API 配额，是否继续调用超额工具？",
                "exceeded": exceeded,
                "planned_tools": [{"name": c.name, "query": c.query} for c in planned],
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

        # Keep planner order in final report.
        order = {call.name: i for i, call in enumerate(allowed_plan)}
        tool_results.sort(key=lambda x: order.get(x.tool, 999))
        tool_results = _apply_reference_limits(tool_results, normalized_search_mode, query_profile)
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

        if _doc_no_context and _log_no_context_query is not None and not benchmark_mode:
            try:
                _log_no_context_query(
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
        guardrail_flags = _build_guardrail_flags(query_classification, media_validation)
        answer_mode = _build_guardrail_answer_mode(q, query_classification, tool_results, guardrail_flags)
        query_classification["guardrail_flags"] = guardrail_flags
        query_classification["answer_guardrail_mode"] = {
            "mode": answer_mode.get("mode", "normal"),
            "reasons": list(answer_mode.get("reasons") or []),
        }
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
            str(query_classification.get("query_type", QUERY_TYPE_GENERAL) or QUERY_TYPE_GENERAL),
            _rag_used,
            _doc_no_context,
        )

        if _agent_no_context and not _doc_no_context and _log_no_context_query is not None and not benchmark_mode:
            try:
                _log_no_context_query(
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
        memory_context = "" if benchmark_mode else build_memory_context(sid)
        debug_trace["memory_context"] = memory_context
        debug_trace["memory_tokens_est"] = _approx_tokens(memory_context)
        _llm_stats: dict[str, Any] = {}
        _llm_t0 = _wall_time.perf_counter()
        structured_media_answer = _build_structured_media_answer(q, query_classification, tool_results)
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
        if debug:
            debug_trace["final_answer_tokens_est"] = _approx_tokens(final_answer)
            _write_debug_record(sid, debug_trace)

        if not benchmark_mode:
            try:
                record_agent_metrics(
                    query_profile=str(query_profile.get("profile", "medium") or "medium"),
                    search_mode=normalized_search_mode,
                    query_type=str(query_classification.get("query_type", "general") or "general"),
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
            query_classification=query_classification,
            planned_tools=planned,
            tool_results=tool_results,
            doc_data=_doc_data,
            timings={
                "vector_recall_seconds": _doc_vector_recall_s,
                "rerank_seconds": _doc_rerank_s,
                "no_context": _agent_no_context,
                "no_context_reason": _agent_no_context_reason,
                "doc_score_threshold": _doc_threshold,
            },
            llm_stats=_llm_stats,
            degraded_to_retrieval=degraded_to_retrieval,
            degrade_reason=degrade_reason,
            wall_clock_seconds=_wall_time.perf_counter() - _wall_t0,
            planning_seconds=_planning_seconds,
            tool_execution_seconds=_tool_execution_seconds,
            llm_seconds=_llm_seconds,
        )
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
