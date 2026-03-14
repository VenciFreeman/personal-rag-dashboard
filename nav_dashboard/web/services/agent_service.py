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

try:
    from core_service.config import get_settings
    from core_service.llm_client import chat_completion_with_retry
    from core_service.trace_store import write_trace_record
except Exception as exc:  # noqa: BLE001
    _LLM_IMPORT_ERROR = exc
    get_settings = None
    chat_completion_with_retry = None
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

TOOL_NAMES = [TOOL_QUERY_DOC_RAG, TOOL_QUERY_MEDIA, TOOL_SEARCH_WEB, TOOL_EXPAND_DOC_QUERY, TOOL_EXPAND_MEDIA_QUERY]
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
_MEDIA_INTENT_KEYWORDS = (
    "电影",
    "影片",
    "影视",
    "电视剧",
    "剧集",
    "动漫",
    "动画",
    "番",
    "漫画",
    "轻小说",
    "小说",
    "游戏",
    "gal",
    "visual novel",
    "角色",
    "主角",
    "剧情",
    "作者",
    "导演",
    "演员",
    "配乐",
    "结局",
    "观后感",
    "读后感",
    "游玩体验",
    "作品",
)


def _is_doc_graph_available() -> bool:
    return _doc_graph_expand is not None


def _is_media_graph_available() -> bool:
    return _media_graph_expand is not None


def _allowed_tool_names(search_mode: str) -> list[str]:
    normalized_mode = _normalize_search_mode(search_mode)
    allowed = [TOOL_QUERY_DOC_RAG, TOOL_QUERY_MEDIA]
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
    ]
    normalized_mode = _normalize_search_mode(search_mode)
    if normalized_mode == "hybrid":
        lines.append(f"- {TOOL_SEARCH_WEB}: 联网搜索最新信息")
    if _is_doc_graph_available():
        lines.append(f"- {TOOL_EXPAND_DOC_QUERY}: 使用知识图谱扩展文档查询（可获取相关概念）")
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


def _derive_session_title(question: str, max_len: int = 24) -> str:
    text = re.sub(r"\s+", " ", str(question or "").strip())
    if not text:
        return "新会话"
    text = text.strip("，。！？!?;；:：")
    if len(text) <= max_len:
        return text
    return text[:max_len].rstrip() + "..."


def _normalize_session(raw: object) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    sid = str(raw.get("id", "")).strip()
    if not sid:
        return None
    title = str(raw.get("title", "新会话")).strip() or "新会话"
    created_at = str(raw.get("created_at", "")).strip() or _now_iso()
    updated_at = str(raw.get("updated_at", "")).strip() or created_at
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
        "title": title,
        "created_at": created_at,
        "updated_at": updated_at,
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
            "title": str(title or "新会话").strip() or "新会话",
            "created_at": now,
            "updated_at": now,
            "messages": [{"role": "assistant", "text": "你好，我可以帮你并行查询文档与媒体记录。"}],
        }
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
    return any(keyword in text for keyword in _MEDIA_INTENT_KEYWORDS)


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
    media_signal = media_entity_confident or (
        classifier_label == CLASSIFIER_LABEL_MEDIA
        and (media_intent_cues or short_media_surface)
        and not disable_media_search
    )
    strong_tech_signal = tech_score >= TECH_QUERY_DOC_SIM_THRESHOLD or classifier_label == CLASSIFIER_LABEL_TECH

    if media_signal and strong_tech_signal:
        query_type = QUERY_TYPE_MIXED
    elif media_entity_confident:
        query_type = QUERY_TYPE_MEDIA
    elif tech_score >= TECH_QUERY_DOC_SIM_THRESHOLD:
        query_type = QUERY_TYPE_TECH
    elif weak_tech_signal and not media_intent_cues:
        query_type = QUERY_TYPE_TECH
    elif classifier_label == CLASSIFIER_LABEL_TECH:
        query_type = QUERY_TYPE_TECH
    elif classifier_label == CLASSIFIER_LABEL_MEDIA and (media_entity_confident or media_intent_cues) and not disable_media_search:
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


def _build_tool_plan_from_query_type(question: str, query_type: str, search_mode: str) -> list[PlannedToolCall]:
    normalized_type = _normalize_query_type(query_type)
    normalized_mode = _normalize_search_mode(search_mode)
    if normalized_type == QUERY_TYPE_MEDIA:
        return [PlannedToolCall(name=TOOL_QUERY_MEDIA, query=question)]
    if normalized_type == QUERY_TYPE_MIXED:
        return [
            PlannedToolCall(name=TOOL_QUERY_DOC_RAG, query=question),
            PlannedToolCall(name=TOOL_QUERY_MEDIA, query=question),
        ]
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
            filtered = _filter_rows(
                rows,
                media_limit,
                media_keyword_threshold,
                threshold_selector=lambda row: _media_threshold_selector(
                    row,
                    media_keyword_threshold,
                    media_vector_threshold,
                ),
            )
            summary = (
                f"命中 {len(filtered)} 条媒体记录"
                f"（keyword score>{media_keyword_threshold}; vector score>{media_vector_threshold}）"
            )
            data = dict(result.data)
            data["results"] = filtered
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


def _extract_media_entities(query: str) -> list[str]:
    raw = (query or "").strip()
    if not raw:
        return []

    normalized = raw
    for prefix in ["在我的数据库里", "我的数据库里", "在数据库里", "请问", "帮我", "我想知道", "想问下"]:
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix) :].strip(" ，。！？?；;:")

    match = re.search(r"(?:我)?对(?P<title>.+?)(?:的)?(?:评价|看法|评分|感受|印象|想法)", normalized)
    if match:
        entities = _split_media_entities(match.group("title"))
        if entities:
            return entities

    match = re.search(r"^(?P<title>.+?)(?:的)?(?:对比|比较|区别|差异)", normalized)
    if match:
        entities = _split_media_entities(match.group("title"))
        if entities:
            return entities

    match = re.search(r"^(?P<title>.+?)的(?:各个)?(?:主角|角色|剧情|介绍|评价|看法|分析|总结)", normalized)
    if match:
        entities = _split_media_entities(match.group("title"))
        if entities:
            return entities

    if any(token in normalized for token in ["对比", "比较", "区别", "差异", "评价", "看法", "评分"]):
        entities = _split_media_entities(normalized)
        if len(entities) >= 2:
            return entities

    return []


def _extract_media_entity(query: str) -> str:
    entities = _extract_media_entities(query)
    return entities[0] if entities else ""


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


def _tool_query_media_record(query: str, query_profile: dict[str, Any], trace_id: str = "") -> ToolExecution:
    rewritten_query = _rewrite_media_query(query)
    media_entities = _extract_media_entities(query)
    extracted_entity = media_entities[0] if media_entities else ""

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

    vector_queries: list[str] = []
    for entity in media_entities:
        if entity and entity not in vector_queries:
            vector_queries.append(entity)
    if not vector_queries and vector_query:
        vector_queries.append(vector_query)

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
                "author": item.get("author"),
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
            "graph_expansion": {
                "status": graph_tool.status,
                "summary": graph_tool.summary,
                "expanded_query": vector_query,
                "constraints": filters,
            },
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
    query_profile = _resolve_query_profile(question)
    query_classification = _classify_query_type(question, quota_state, query_profile)
    planned = _build_tool_plan_from_query_type(question, query_classification.get("query_type", QUERY_TYPE_GENERAL), search_mode)
    return planned, query_classification


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


def _format_tool_result(exec_result: ToolExecution) -> str:
    return json.dumps(
        {
            "tool": exec_result.tool,
            "status": exec_result.status,
            "summary": exec_result.summary,
            "data": exec_result.data,
        },
        ensure_ascii=False,
    )


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
    context_parts = ["工具返回结果："]
    for result in tool_results:
        context_parts.append(_format_tool_result(result))

    hist_lines = []
    for msg in (history or [])[-8:]:
        role = str(msg.get("role", "")).strip()
        content = str(msg.get("content", "")).strip()
        if content:
            hist_lines.append(f"{role}: {content}")

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

    prompt_blocks = hist_lines + [f"当前问题: {question}"]
    if memory_context:
        prompt_blocks.extend(["", memory_context])
    prompt_blocks.extend(["", *context_parts])
    user_prompt = "\n".join(prompt_blocks)
    context_tokens_est = _approx_tokens("\n".join(context_parts))
    input_tokens_est = _approx_tokens(system_prompt) + _approx_tokens(user_prompt)
    prompt_tokens_est = max(0, input_tokens_est - context_tokens_est)
    if debug_sink is not None:
        debug_sink["llm_request"] = {
            "trace_id": trace_id,
            "trace_stage": "agent.llm.summarize",
            "backend": backend,
            "system_prompt": system_prompt,
            "user_prompt": user_prompt,
            "memory_tokens_est": _approx_tokens(memory_context),
            "input_tokens_est": input_tokens_est,
            "prompt_tokens_est": prompt_tokens_est,
            "context_tokens_est": context_tokens_est,
        }
    if llm_stats_sink is not None:
        llm_stats_sink["backend"] = backend
        llm_stats_sink["input_tokens_est"] = input_tokens_est
        llm_stats_sink["prompt_tokens_est"] = prompt_tokens_est
        llm_stats_sink["context_tokens_est"] = context_tokens_est
        llm_stats_sink["memory_tokens_est"] = _approx_tokens(memory_context)
        llm_stats_sink["calls"] = 1
    answer = _llm_chat(
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        backend=backend,
        quota_state=quota_state,
    )
    if normalized_search_mode == "local_only" and not has_web_tool and re.search(r"联网搜索|网络搜索|进行网络搜索|经过搜索", answer):
        if llm_stats_sink is not None:
            llm_stats_sink["calls"] = int(llm_stats_sink.get("calls", 1) or 1) + 1
            llm_stats_sink["input_tokens_est"] = int(llm_stats_sink.get("input_tokens_est", 0) or 0) + input_tokens_est
            llm_stats_sink["prompt_tokens_est"] = int(llm_stats_sink.get("prompt_tokens_est", 0) or 0) + prompt_tokens_est
            llm_stats_sink["context_tokens_est"] = int(llm_stats_sink.get("context_tokens_est", 0) or 0) + context_tokens_est
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
    if debug_sink is not None:
        debug_sink["llm_response"] = {
            "trace_id": trace_id,
            "trace_stage": "agent.llm.summarize",
            "output_tokens_est": _approx_tokens(answer),
        }
    if llm_stats_sink is not None:
        llm_stats_sink["output_tokens_est"] = _approx_tokens(answer)
    return answer


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
    router = {
        "selected_tool": planned_tools[0].name if planned_tools else "",
        "planned_tools": [call.name for call in planned_tools],
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

    if reason:
        if "Missing dependency: openai" in reason:
            lines.append("降级原因：当前环境未安装 `openai` 依赖，已切换到纯检索模式。")
            lines.append("建议：执行 `pip install -r nav_dashboard/requirements.txt` 后可恢复 LLM 汇总能力。")
        else:
            lines.append(f"降级原因：{reason}")
    return "\n".join(lines).strip()


def _build_references_markdown(tool_results: list[ToolExecution], *, request_base_url: str = "") -> str:
    library_ref_base = _library_tracker_reference_base(request_base_url)
    refs: list[tuple[float, str]] = []

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
                refs.append((score, f"- [文档: {path} ({score:.4f})]({doc_uri})"))

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
                    refs.append((score, f"- [{label} ({score:.4f})]({url})"))
                else:
                    refs.append((score, f"- {label} ({score:.4f})"))

        elif result.tool == TOOL_SEARCH_WEB:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                title = str(row.get("title", row.get("url", "网页"))).strip() or "网页"
                url = str(row.get("url", "")).strip()
                score = _score_value(row)
                if not url or score is None:
                    continue
                refs.append((score, f"- [网页: {title} ({score:.4f})]({url})"))

    if not refs:
        return ""

    lines = [item[1] for item in sorted(refs, key=lambda x: x[0], reverse=True)]
    return "\n\n### 参考资料\n" + "\n".join(lines)


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
            session["title"] = _derive_session_title(q)
            session["updated_at"] = _now_iso()
            _save_session(session)
        if session and isinstance(session.get("messages"), list):
            hist = [{"role": str(m.get("role", "")), "content": str(m.get("text", ""))} for m in session.get("messages", [])]

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

    tool_results: list[ToolExecution] = []
    _tool_exec_t0 = _wall_time.perf_counter()
    with ThreadPoolExecutor(max_workers=max(1, len(allowed_plan))) as pool:
        future_map = {pool.submit(_execute_tool, call, query_profile, resolved_trace_id): call for call in allowed_plan}
        for future in as_completed(future_map):
            tool_results.append(future.result())
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
    if any(call.name == TOOL_QUERY_MEDIA for call in allowed_plan) and not media_rows:
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

    references_md = _build_references_markdown(tool_results, request_base_url=request_base_url)
    final_answer = answer
    if references_md:
        final_answer = f"{answer}{references_md}"

    if not benchmark_mode:
        append_message(sid, "assistant", final_answer, trace_id=resolved_trace_id)
        _update_memory_for_session(sid)
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

    _write_agent_trace_record(
        _build_agent_trace_record(
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
    )

    return {
        "requires_confirmation": False,
        "trace_id": resolved_trace_id,
        "session_id": sid,
        "answer": final_answer,
        "backend": backend,
        "search_mode": normalized_search_mode,
        "query_profile": query_profile,
        "query_classification": query_classification,
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
                session["title"] = _derive_session_title(q)
                session["updated_at"] = _now_iso()
                _save_session(session)
            if session and isinstance(session.get("messages"), list):
                hist = [{"role": str(m.get("role", "")), "content": str(m.get("text", ""))} for m in session.get("messages", [])]

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

        tool_results: list[ToolExecution] = []
        _tool_exec_t0 = _wall_time.perf_counter()
        with ThreadPoolExecutor(max_workers=max(1, len(allowed_plan))) as pool:
            future_map = {pool.submit(_execute_tool, call, query_profile, resolved_trace_id): call for call in allowed_plan}
            for future in as_completed(future_map):
                result = future.result()
                tool_results.append(result)
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
        if any(call.name == TOOL_QUERY_MEDIA for call in allowed_plan) and not media_rows:
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

        references_md = _build_references_markdown(tool_results, request_base_url=request_base_url)
        final_answer = answer
        if references_md:
            final_answer = f"{answer}{references_md}"

        if not benchmark_mode:
            append_message(sid, "assistant", final_answer, trace_id=resolved_trace_id)
            _update_memory_for_session(sid)
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

        _write_agent_trace_record(
            _build_agent_trace_record(
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
        )

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
