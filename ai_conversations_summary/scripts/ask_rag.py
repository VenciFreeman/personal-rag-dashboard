"""RAG question-answering entrypoint for local embedding + vector retrieval.

Flow:
1) Embed query with local embedding model (for example BAAI/bge-base-zh-v1.5).
2) Search vector index (FAISS/Chroma).
3) Load retrieved markdown files as context.
4) Ask chat LLM to answer strictly based on context.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4


WORKSPACE_ROOT = Path(__file__).resolve().parent.parent.parent
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from api_config import API_BASE_URL, API_KEY, EMBEDDING_MODEL, MODEL, TAVILY_API_KEY, TIMEOUT
from core_service.config import get_settings
try:
    from core_service.llm_client import chat_completion, stream_chat_completion_text
except ModuleNotFoundError:
    def chat_completion(
        *,
        api_key: str,
        base_url: str,
        model: str,
        timeout: int,
        messages: list[dict[str, str]],
        temperature: float = 0.2,
        max_tokens: int | None = None,
    ) -> str:
        try:
            from openai import OpenAI
        except ModuleNotFoundError as exc:
            raise RuntimeError("Missing dependency: openai. Install with: pip install openai") from exc

        client = OpenAI(api_key=api_key, base_url=base_url, timeout=timeout)
        kwargs: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "stream": False,
        }
        if max_tokens is not None:
            kwargs["max_tokens"] = max_tokens
        response = client.chat.completions.create(**kwargs)
        if not response.choices or not response.choices[0].message:
            raise RuntimeError("LLM response is empty")
        text = (response.choices[0].message.content or "").strip()
        if not text:
            raise RuntimeError("LLM response text is empty")
        return text

    def stream_chat_completion_text(
        *,
        api_key: str,
        base_url: str,
        model: str,
        timeout: int,
        messages: list[dict[str, str]],
        temperature: float = 0.2,
        max_tokens: int | None = None,
    ):
        try:
            from openai import OpenAI
        except ModuleNotFoundError as exc:
            raise RuntimeError("Missing dependency: openai. Install with: pip install openai") from exc

        client = OpenAI(api_key=api_key, base_url=base_url, timeout=timeout)
        kwargs: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "stream": True,
        }
        if max_tokens is not None:
            kwargs["max_tokens"] = max_tokens
        response_stream = client.chat.completions.create(**kwargs)
        for chunk in response_stream:
            if not chunk.choices:
                continue
            delta = chunk.choices[0].delta
            if delta and delta.content:
                yield str(delta.content)
from rag_vector_index import (
    RAGIndexError,
    _embed_texts_local,
    _normalize_embedding_model_value,
    search_vector_index_with_diagnostics,
)
from rag_knowledge_graph import expand_query_by_graph, sync_rag_graph


DEEPSEEK_AUDIT_DIR = PROJECT_ROOT / "data" / "deepseek_api_audit"
_CORE_SETTINGS = get_settings()
DEFAULT_RESPONSE_MAX_TOKENS_WITH_CONTEXT = int(_CORE_SETTINGS.rag_response_max_tokens_with_context)
DEFAULT_RESPONSE_MAX_TOKENS_NO_CONTEXT = int(_CORE_SETTINGS.rag_response_max_tokens_no_context)
DEFAULT_LOCAL_ONLY_READ_WEB_CACHE = bool(_CORE_SETTINGS.rag_local_only_read_web_cache)
DEFAULT_VECTOR_TOP_N = int(os.getenv("AI_SUMMARY_VECTOR_TOP_N", "10"))
DEFAULT_RERANK_TOP_K = int(os.getenv("AI_SUMMARY_RERANK_TOP_K", "5"))
DEFAULT_ENABLE_RERANK = os.getenv("AI_SUMMARY_ENABLE_RERANK", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
DEFAULT_RERANKER_MODEL = os.getenv("AI_SUMMARY_RERANKER_MODEL", "BAAI/bge-reranker-base").strip() or "BAAI/bge-reranker-base"
DEFAULT_MAX_CONTEXT_DOCS = int(os.getenv("AI_SUMMARY_MAX_CONTEXT_DOCS", "6") or "6")
DEFAULT_ENABLE_RERANK_TOP1_GUARD = os.getenv("AI_SUMMARY_ENABLE_RERANK_TOP1_GUARD", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
DEFAULT_RERANK_GUARD_MAX_TOP1_DROP = float(os.getenv("AI_SUMMARY_RERANK_GUARD_MAX_TOP1_DROP", "0.03") or "0.03")
DEFAULT_RERANK_GUARD_MAX_TOP1_DROP_RATIO = float(os.getenv("AI_SUMMARY_RERANK_GUARD_MAX_TOP1_DROP_RATIO", "0.08") or "0.08")
DEFAULT_RERANK_FUSION_ALPHA = float(os.getenv("AI_SUMMARY_RERANK_FUSION_ALPHA", "0.35") or "0.35")
DEFAULT_ENABLE_DYNAMIC_RERANK_ALPHA = os.getenv("AI_SUMMARY_ENABLE_DYNAMIC_RERANK_ALPHA", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
DEFAULT_DYNAMIC_RERANK_ALPHA_DIFF_SCALE = float(os.getenv("AI_SUMMARY_DYNAMIC_RERANK_ALPHA_DIFF_SCALE", "5.0") or "5.0")
DEFAULT_DYNAMIC_RERANK_ALPHA_DIFF_CENTER = float(os.getenv("AI_SUMMARY_DYNAMIC_RERANK_ALPHA_DIFF_CENTER", "0.05") or "0.05")
DEFAULT_RERANK_TOP1_GAP_THRESHOLD = float(os.getenv("AI_SUMMARY_RERANK_TOP1_GAP_THRESHOLD", "0.10") or "0.10")
DEFAULT_RERANK_STRONG_TOP1_THRESHOLD = float(os.getenv("AI_SUMMARY_RERANK_STRONG_TOP1_THRESHOLD", "0.60") or "0.60")
DEFAULT_SHORT_QUERY_RERANK_CANDIDATE_K = int(os.getenv("AI_SUMMARY_SHORT_QUERY_RERANK_CANDIDATE_K", "3") or "3")
DEFAULT_SHORT_QUERY_MAX_CHARS = int(os.getenv("AI_SUMMARY_SHORT_QUERY_MAX_CHARS", "16") or "16")
DEFAULT_MAX_VECTOR_CANDIDATES = int(os.getenv("AI_SUMMARY_MAX_VECTOR_CANDIDATES", "12") or "12")
DEFAULT_ENABLE_QUERY_REWRITE = os.getenv("AI_SUMMARY_ENABLE_QUERY_REWRITE", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
DEFAULT_QUERY_REWRITE_COUNT = max(1, min(2, int(os.getenv("AI_SUMMARY_QUERY_REWRITE_COUNT", "2"))))
DEFAULT_PRIMARY_QUERY_SCORE_BONUS = float(os.getenv("AI_SUMMARY_PRIMARY_QUERY_SCORE_BONUS", "0.03") or "0.03")
BUILD_INDEX_ON_DEMAND = os.getenv("AI_SUMMARY_RAG_BUILD_INDEX_ON_DEMAND", "0").strip().lower() in {"1", "true", "yes", "on"}
DEFAULT_PASSAGE_FIRST_K = int(os.getenv("AI_SUMMARY_PASSAGE_FIRST_K", "3") or "3")
_RERANKER_CACHE: dict[str, Any] = {}


def _configure_stdio_utf8() -> None:
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream is None:
            continue
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            try:
                reconfigure(encoding="utf-8", errors="replace")
            except Exception:
                pass


def _is_deepseek_url(url: str) -> bool:
    value = (url or "").strip().lower()
    return "api.deepseek.com" in value


def _mask_secret(value: str) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    if len(text) <= 10:
        return "*" * len(text)
    return f"{text[:6]}...{text[-4:]}"


def _write_deepseek_audit_log(entry: dict[str, Any]) -> None:
    try:
        DEEPSEEK_AUDIT_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        rid = str(entry.get("request_id", "")).strip() or str(uuid4())
        out = DEEPSEEK_AUDIT_DIR / f"{ts}_{rid}.json"
        out.write_text(json.dumps(entry, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        # Audit logging must never break the main Q&A path.
        return


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    script_dir = Path(__file__).resolve().parent
    root_dir = script_dir.parent
    default_index_dir = os.getenv(
        "AI_SUMMARY_VECTOR_DB_DIR",
        str(root_dir.parent / "core_service" / "data" / "vector_db"),
    )

    parser = argparse.ArgumentParser(description="RAG Q&A over local documents")
    parser.add_argument("--question", default="", help="Question text. If empty, reads from stdin.")
    parser.add_argument("--documents-dir", default=str(root_dir / "documents"), help="Documents directory")
    parser.add_argument("--index-dir", default=default_index_dir, help="Vector index directory")
    parser.add_argument("--backend", default="faiss", choices=["auto", "faiss", "chroma"], help="Vector backend")
    parser.add_argument("--search-mode", default="hybrid", choices=["hybrid", "local_only"], help="Retrieval mode")
    parser.add_argument("--top-k", type=int, default=5, help="Final top K context size")
    parser.add_argument("--vector-top-n", type=int, default=max(1, DEFAULT_VECTOR_TOP_N), help="Vector recall candidate count before reranking")
    parser.add_argument("--enable-rerank", default="true" if DEFAULT_ENABLE_RERANK else "false", help="Enable reranking: true/false")
    parser.add_argument("--rerank-top-k", type=int, default=max(1, DEFAULT_RERANK_TOP_K), help="Top K kept after reranking")
    parser.add_argument("--reranker-model", default=DEFAULT_RERANKER_MODEL, help="Cross-encoder reranker model")
    parser.add_argument(
        "--enable-rerank-top1-guard",
        default="true" if DEFAULT_ENABLE_RERANK_TOP1_GUARD else "false",
        help="Protect against rerank swaps that materially lower the original vector top1 score: true/false",
    )
    parser.add_argument(
        "--rerank-guard-max-top1-drop",
        type=float,
        default=max(0.0, DEFAULT_RERANK_GUARD_MAX_TOP1_DROP),
        help="Maximum allowed absolute vector-score drop when rerank changes top1 before guard restores the original top1",
    )
    parser.add_argument(
        "--rerank-guard-max-top1-drop-ratio",
        type=float,
        default=max(0.0, DEFAULT_RERANK_GUARD_MAX_TOP1_DROP_RATIO),
        help="Maximum allowed relative vector-score drop ratio when rerank changes top1 before guard restores the original top1",
    )
    parser.add_argument(
        "--rerank-fusion-alpha",
        type=float,
        default=min(1.0, max(0.0, DEFAULT_RERANK_FUSION_ALPHA)),
        help="Weight assigned to normalized rerank score when fusing rerank and vector score (0..1)",
    )
    parser.add_argument(
        "--enable-dynamic-rerank-alpha",
        default="true" if DEFAULT_ENABLE_DYNAMIC_RERANK_ALPHA else "false",
        help="Adapt rerank fusion alpha per query using rerank confidence and vector-gap confidence: true/false",
    )
    parser.add_argument(
        "--dynamic-rerank-alpha-diff-scale",
        type=float,
        default=DEFAULT_DYNAMIC_RERANK_ALPHA_DIFF_SCALE,
        help="Sigmoid scale applied to rerank soft-diff when adapting fusion alpha",
    )
    parser.add_argument(
        "--dynamic-rerank-alpha-diff-center",
        type=float,
        default=DEFAULT_DYNAMIC_RERANK_ALPHA_DIFF_CENTER,
        help="Soft-diff midpoint used by the adaptive fusion alpha sigmoid",
    )
    parser.add_argument(
        "--rerank-top1-gap-threshold",
        type=float,
        default=max(0.0, DEFAULT_RERANK_TOP1_GAP_THRESHOLD),
        help="Block rerank top1 swaps when baseline vector top1-top2 gap exceeds this threshold",
    )
    parser.add_argument(
        "--rerank-strong-top1-threshold",
        type=float,
        default=max(0.0, DEFAULT_RERANK_STRONG_TOP1_THRESHOLD),
        help="Treat vector top1 as strong when its score exceeds this threshold; strong top1 swaps are blocked when the gap is also large",
    )
    parser.add_argument(
        "--short-query-rerank-candidate-k",
        type=int,
        default=max(1, DEFAULT_SHORT_QUERY_RERANK_CANDIDATE_K),
        help="For short queries, only rerank this many top vector candidates",
    )
    parser.add_argument(
        "--short-query-max-chars",
        type=int,
        default=max(1, DEFAULT_SHORT_QUERY_MAX_CHARS),
        help="Queries at or below this length are treated as short for conservative reranking",
    )
    parser.add_argument("--similarity-threshold", type=float, default=0.5, help="Minimum similarity score to include document (0.0-1.0)")
    parser.add_argument("--max-context-chars", type=int, default=20000, help="Max total context chars")
    parser.add_argument("--max-context-docs", type=int, default=max(1, DEFAULT_MAX_CONTEXT_DOCS), help="Maximum number of retrieved references included in context")
    parser.add_argument(
        "--max-chars-per-doc",
        type=int,
        default=5000,
        help="Max chars loaded per retrieved markdown",
    )
    parser.add_argument("--api-url", default=os.getenv("DEEPSEEK_BASE_URL", API_BASE_URL), help="LLM API base URL")
    parser.add_argument("--api-key", default=os.getenv("DEEPSEEK_API_KEY", API_KEY), help="LLM API key")
    parser.add_argument("--model", default=os.getenv("DEEPSEEK_MODEL", MODEL), help="LLM model name")
    parser.add_argument(
        "--embedding-model",
        default=(
            os.getenv("LOCAL_EMBEDDING_MODEL", "").strip()
            or os.getenv("DEEPSEEK_EMBEDDING_MODEL", "").strip()
            or (EMBEDDING_MODEL or "").strip()
            or "BAAI/bge-base-zh-v1.5"
        ),
        help="Embedding model used for retrieval",
    )
    parser.add_argument("--timeout", type=int, default=TIMEOUT, help="Timeout in seconds")
    parser.add_argument("--call-type", default="answer", help="Call type, e.g. answer or memory_update")
    parser.add_argument("--memory-context", default="", help="Serialized memory context for current session")
    parser.add_argument(
        "--context-mode",
        default="passage_first",
        choices=["topic", "full", "passage_first"],
        help="Context assembly mode: passage_first=real content for top docs + metadata for rest (default), topic=metadata-only, full=all docs full markdown body",
    )
    parser.add_argument("--output-json", default="", help="Optional JSON output file path")
    parser.add_argument("--debug", default="false", help="Enable debug trace output in payload")
    parser.add_argument("--session-id", default="", help="Session id for debug trace correlation")
    parser.add_argument("--trace-id", default="", help="End-to-end trace id for this query")
    parser.add_argument("--enable-query-rewrite", default="true" if DEFAULT_ENABLE_QUERY_REWRITE else "false", help="Enable local-LLM query rewrite before retrieval")
    parser.add_argument("--query-rewrite-count", type=int, default=DEFAULT_QUERY_REWRITE_COUNT, help="How many rewritten queries to keep")
    parser.add_argument("--max-vector-candidates", type=int, default=max(1, DEFAULT_MAX_VECTOR_CANDIDATES), help="Maximum merged local vector candidates kept for rerank/context assembly")
    parser.add_argument("--stream", action="store_true", help="Enable streaming output via stdout")
    parser.add_argument(
        "--no-embed-cache",
        action="store_true",
        help="Bypass embedding cache (e.g. for benchmark runs to measure true latency)",
    )
    parser.add_argument(
        "--reranker-server-url",
        default="",
        help="URL of the in-process reranker sidecar (e.g. http://127.0.0.1:PORT). "
             "When provided, reranking calls the sidecar instead of cold-loading the model.",
    )
    parser.add_argument(
        "--embed-server-url",
        default="",
        help="URL of the in-process embedding sidecar (e.g. http://127.0.0.1:PORT). "
             "When provided, embedding calls the sidecar instead of cold-loading the model.",
    )
    parser.add_argument(
        "--allow-local-fallback",
        action="store_true",
        help="When local LLM endpoint is unavailable, degrade to retrieval-only local answer",
    )
    return parser.parse_args(argv)


def build_runtime_args(**overrides: Any) -> argparse.Namespace:
    args = _parse_args([])
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


def _parse_bool_arg(value: object, *, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if not text:
        return default
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _normalize_local_llm_url(url: str) -> str:
    value = (url or "").strip()
    if not value:
        return ""
    if not value.rstrip("/").endswith("/v1"):
        return value.rstrip("/") + "/v1"
    return value


def _parse_rewritten_queries(raw: str, *, fallback: str, count: int) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return [fallback]

    candidates: list[str] = []
    try:
        data = json.loads(text)
        if isinstance(data, dict) and isinstance(data.get("queries"), list):
            candidates = [str(x).strip() for x in data.get("queries", []) if str(x).strip()]
        elif isinstance(data, list):
            candidates = [str(x).strip() for x in data if str(x).strip()]
    except Exception:
        pass

    if not candidates:
        for line in text.splitlines():
            value = line.strip().lstrip("-* ").strip()
            if value:
                candidates.append(value)

    dedup: list[str] = []
    seen: set[str] = set()
    for q in [fallback] + candidates:
        key = q.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        dedup.append(q.strip())
        if len(dedup) >= max(1, count):
            break
    return dedup or [fallback]


# Compiled once at module level; used by _should_rewrite_query.
_COMPLEXITY_PATTERNS = re.compile(
    r"对比|区别|差异|比较|优缺点|优劣|pros|cons|versus\b|(?<![A-Za-z])vs(?![A-Za-z])"
    r"|为什么.*怎|[,，].{3,}[,，]",
    re.IGNORECASE,
)


def _should_rewrite_query(question: str) -> bool:
    """Return True when the query is long enough or complex enough to benefit from rewriting.

    Rules (OR logic):
    - length > 20 characters
    - contains comparison / contrast keywords
    - contains multiple clauses (comma-separated)
    - contains mixed question-word pairs (为什么…怎)
    """
    q = (question or "").strip()
    if len(q) > 20:
        return True
    return bool(_COMPLEXITY_PATTERNS.search(q))


def _rewrite_queries_with_local_llm(
    *,
    question: str,
    memory_context: str,
    rewrite_count: int,
    timeout: int,
) -> tuple[list[str], str]:
    local_url = _normalize_local_llm_url(os.getenv("AI_SUMMARY_LOCAL_LLM_URL", "http://127.0.0.1:1234"))
    local_model = os.getenv("AI_SUMMARY_LOCAL_LLM_MODEL", "").strip() or _CORE_SETTINGS.local_llm_model
    local_key = os.getenv("AI_SUMMARY_LOCAL_LLM_API_KEY", "local").strip() or "local"
    if not local_url:
        return [question], "fallback:missing_local_llm_url"

    system_prompt = (
        "你是RAG检索查询改写助手。"
        "请将用户问题改写为最多2条用于中文知识库检索的查询。"
        "保留原问题的核心表述，不要偏离原语义。"
        "知识库文档结构包含title/summary/keywords/topic。"
        "只返回JSON：{\"queries\":[\"q1\",\"q2\"]}。"
    )
    user_prompt = f"问题: {question}\n会话记忆:\n{memory_context or '<empty>'}"
    rewrite_timeout_env = str(os.getenv("AI_SUMMARY_QUERY_REWRITE_TIMEOUT_SECONDS", "8") or "8").strip()
    try:
        rewrite_timeout = int(rewrite_timeout_env)
    except Exception:
        rewrite_timeout = 8
    rewrite_timeout = max(3, min(max(3, int(timeout)), rewrite_timeout))
    try:
        raw = chat_completion(
            api_key=local_key,
            base_url=local_url,
            model=local_model,
            timeout=rewrite_timeout,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.1,
            max_tokens=220,
        )
        return _parse_rewritten_queries(raw, fallback=question, count=rewrite_count), "ok"
    except Exception as exc:  # noqa: BLE001
        return [question], f"fallback:{exc}"


def _vector_merge_key(row: dict[str, Any]) -> str:
    for key in ("relative_path", "path", "file_path", "id"):
        value = str(row.get(key, "")).strip()
        if value:
            return value.lower()
    return str(row)


def _merge_multi_query_vector_rows(
    query_results: list[tuple[str, list[dict[str, Any]]]],
    *,
    primary_query: str = "",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    merged: dict[str, dict[str, Any]] = {}
    debug_batches: list[dict[str, Any]] = []
    normalized_primary = str(primary_query or "").strip().lower()

    for query_text, rows in query_results:
        compact_rows: list[dict[str, Any]] = []
        for idx, item in enumerate(rows, start=1):
            if not isinstance(item, dict):
                continue
            row = dict(item)
            row_score = float(row.get("score", 0.0))
            row["score"] = row_score
            row["matched_query"] = query_text
            row["matched_rank"] = idx
            query_bonus = DEFAULT_PRIMARY_QUERY_SCORE_BONUS if normalized_primary and query_text.strip().lower() == normalized_primary else 0.0
            row["query_priority_boost"] = query_bonus
            row["query_priority_score"] = row_score + query_bonus
            compact_rows.append(
                {
                    "path": row.get("relative_path") or row.get("path") or row.get("file_path"),
                    "score": row_score,
                }
            )

            key = _vector_merge_key(row)
            existing = merged.get(key)
            existing_priority = float(existing.get("query_priority_score", existing.get("score", 0.0))) if existing is not None else None
            if existing is None or float(row.get("query_priority_score", row_score)) > float(existing_priority or 0.0):
                row["matched_queries"] = [query_text]
                merged[key] = row
            else:
                matched_queries = existing.get("matched_queries", [])
                if not isinstance(matched_queries, list):
                    matched_queries = []
                if query_text not in matched_queries:
                    matched_queries.append(query_text)
                existing["matched_queries"] = matched_queries

        debug_batches.append({"query": query_text, "rows": compact_rows})

    merged_rows = sorted(
        merged.values(),
        key=lambda x: (
            float(x.get("query_priority_score", x.get("score", 0.0))),
            float(x.get("score", 0.0)),
        ),
        reverse=True,
    )
    return merged_rows, debug_batches


def _cap_vector_candidates(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    normalized_limit = max(1, int(limit))
    return [dict(row) for row in rows[:normalized_limit] if isinstance(row, dict)]


def _approx_tokens(text: str) -> int:
    value = str(text or "")
    return max(0, int(len(value) / 4))


def _data_roots() -> list[Path]:
    workspace_root = Path(__file__).resolve().parent.parent
    core_data_root = workspace_root.parent / "core_service" / "data"
    return [core_data_root, workspace_root / "data"]


def _find_local_snapshot_path(model_name: str) -> Path | None:
    if "/" not in model_name:
        return None
    safe_id = model_name.replace("/", "--")
    pattern = f"**/models--{safe_id}/snapshots/*"
    candidates: list[Path] = []
    for root in _data_roots():
        cache_root = root / "local_models"
        if not cache_root.exists():
            continue
        candidates.extend([p for p in cache_root.glob(pattern) if p.is_dir()])
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def _find_local_model_dir_candidates(model_name: str) -> list[Path]:
    raw = (model_name or "").strip()
    if not raw:
        return []

    candidates: list[Path] = []
    as_path = Path(raw)
    if as_path.exists():
        candidates.append(as_path)

    leaf = raw.split("/")[-1].strip()
    for root in _data_roots():
        local_models_root = root / "local_models"
        if not local_models_root.exists():
            continue
        candidates.append(local_models_root / raw.replace("\\", "/"))
        if "/" in raw:
            candidates.append(local_models_root / raw.replace("/", "--"))
        if leaf:
            candidates.append(local_models_root / leaf)

    uniq: list[Path] = []
    seen: set[str] = set()
    for path in candidates:
        key = str(path.resolve()) if path.exists() else str(path)
        if key in seen:
            continue
        seen.add(key)
        if path.is_dir():
            uniq.append(path)
    return uniq


def _load_local_reranker(model_name: str):
    from sentence_transformers import CrossEncoder

    attempts: list[tuple[str, dict[str, Any]]] = []
    local_only_args = {
        "automodel_args": {"local_files_only": True},
        "tokenizer_args": {"local_files_only": True},
    }

    for local_dir in _find_local_model_dir_candidates(model_name):
        attempts.append((str(local_dir.resolve()), dict(local_only_args)))

    snapshot = _find_local_snapshot_path(model_name)
    if snapshot is not None:
        attempts.append((str(snapshot.resolve()), dict(local_only_args)))

    # Last fallback keeps old behavior for environments with internet access.
    attempts.append((model_name, {}))

    last_exc: Exception | None = None
    for model_ref, kwargs in attempts:
        try:
            return CrossEncoder(model_ref, **kwargs)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"Failed to load reranker model: {model_name}")


def _build_rerank_text(item: dict[str, Any], documents_dir: Path) -> str:
    title = str(item.get("title", "")).strip()
    summary = str(item.get("summary", "")).strip()
    topic = str(item.get("topic", "")).strip()
    keywords = _normalize_keywords(item.get("keywords", []))
    keyword_text = ", ".join(keywords)

    if not title or not summary:
        path = _resolve_result_path(item, documents_dir)
        if path is not None and path.is_file():
            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
                if not title:
                    for line in text.splitlines()[:120]:
                        line = line.strip()
                        if line.startswith("> - **title**") and ":" in line:
                            title = line.split(":", 1)[1].strip().strip('"')
                            break
                        if line.startswith("# "):
                            title = line[2:].strip()
                            break
                if not summary:
                    for line in text.splitlines()[:120]:
                        line = line.strip()
                        if line.startswith("> - **summary**") and ":" in line:
                            summary = line.split(":", 1)[1].strip().strip('"')
                            break
            except Exception:
                pass

    if not summary:
        summary = topic

    return (
        f"Title: {title}\n\n"
        f"Summary:\n{summary}\n\n"
        f"Keywords:\n{keyword_text}"
    ).strip()


def _result_identity(item: dict[str, Any]) -> str:
    return str(item.get("relative_path") or item.get("path") or item.get("file_path") or "").strip()


def _vector_score(item: dict[str, Any]) -> float:
    return float(item.get("vector_score", item.get("score", 0.0)) or 0.0)


def _minmax_normalize(values: list[float]) -> list[float]:
    if not values:
        return []
    if len(values) == 1:
        return [1.0]
    min_value = min(values)
    max_value = max(values)
    spread = max_value - min_value
    if spread <= 1e-9:
        return [1.0 for _ in values]
    return [(value - min_value) / spread for value in values]


def _softmax_normalize(values: list[float]) -> list[float]:
    if not values:
        return []
    if len(values) == 1:
        return [1.0]
    max_value = max(values)
    exp_values = [math.exp(max(-50.0, min(50.0, value - max_value))) for value in values]
    total = sum(exp_values) or 1.0
    return [value / total for value in exp_values]


def _sigmoid(value: float) -> float:
    clipped = max(-50.0, min(50.0, float(value)))
    return 1.0 / (1.0 + math.exp(-clipped))


def _is_short_query(query: str, *, max_chars: int) -> bool:
    normalized = str(query or "").strip()
    if not normalized:
        return False
    return len(normalized) <= max(1, int(max_chars)) and not _should_rewrite_query(normalized)


def _select_rerank_candidate_count(
    *,
    query: str,
    row_count: int,
    top_k: int,
    short_query_candidate_k: int,
    short_query_max_chars: int,
) -> tuple[int, str]:
    if row_count <= 0:
        return 0, "empty"
    requested = max(1, min(int(row_count), int(top_k)))
    if _is_short_query(query, max_chars=short_query_max_chars):
        return max(1, min(requested, int(short_query_candidate_k))), "short"
    return requested, "default"


def _apply_rerank_score_fusion(ranked_rows: list[dict[str, Any]], *, fusion_alpha: float) -> list[dict[str, Any]]:
    alpha = min(1.0, max(0.0, float(fusion_alpha)))
    vector_values = [_vector_score(row) for row in ranked_rows]
    rerank_values = [float(row.get("rerank_score", 0.0) or 0.0) for row in ranked_rows]
    vector_norm = _minmax_normalize(vector_values)
    rerank_norm = _softmax_normalize(rerank_values)

    fused_rows: list[dict[str, Any]] = []
    for row, vector_score_norm, rerank_score_norm in zip(ranked_rows, vector_norm, rerank_norm):
        item = dict(row)
        item["vector_norm_score"] = float(vector_score_norm)
        item["rerank_norm_score"] = float(rerank_score_norm)
        item["final_score"] = float(alpha * rerank_score_norm + (1.0 - alpha) * vector_score_norm)
        fused_rows.append(item)
    return fused_rows


def _compute_dynamic_fusion_alpha(
    *,
    baseline_rows: list[dict[str, Any]],
    rerank_norm_scores: list[float],
    base_alpha: float,
    enabled: bool,
    gap_threshold: float,
    diff_scale: float,
    diff_center: float,
) -> dict[str, Any]:
    baseline_top1 = _vector_score(baseline_rows[0]) if baseline_rows else 0.0
    baseline_top2 = _vector_score(baseline_rows[1]) if len(baseline_rows) > 1 else 0.0
    vector_gap = max(0.0, baseline_top1 - baseline_top2)
    sorted_rerank = sorted((float(score or 0.0) for score in rerank_norm_scores), reverse=True)
    rerank_soft_top1 = sorted_rerank[0] if sorted_rerank else 1.0
    rerank_soft_top2 = sorted_rerank[1] if len(sorted_rerank) > 1 else 0.0
    rerank_soft_diff = max(0.0, rerank_soft_top1 - rerank_soft_top2)
    normalized_base_alpha = min(1.0, max(0.0, float(base_alpha)))
    normalized_gap_threshold = max(0.0, float(gap_threshold))

    if not enabled:
        return {
            "base_alpha": normalized_base_alpha,
            "effective_alpha": normalized_base_alpha,
            "dynamic_alpha_enabled": False,
            "vector_gap": vector_gap,
            "rerank_soft_top1": rerank_soft_top1,
            "rerank_soft_top2": rerank_soft_top2,
            "rerank_soft_diff": rerank_soft_diff,
            "confidence_factor": 1.0,
            "alpha_reason": "disabled",
        }

    if vector_gap >= normalized_gap_threshold:
        return {
            "base_alpha": normalized_base_alpha,
            "effective_alpha": 0.0,
            "dynamic_alpha_enabled": True,
            "vector_gap": vector_gap,
            "rerank_soft_top1": rerank_soft_top1,
            "rerank_soft_top2": rerank_soft_top2,
            "rerank_soft_diff": rerank_soft_diff,
            "confidence_factor": 0.0,
            "alpha_reason": f"vector_gap_block:{vector_gap:.4f}>={normalized_gap_threshold:.4f}",
        }

    confidence_factor = _sigmoid(float(diff_scale) * (rerank_soft_diff - float(diff_center)))
    effective_alpha = normalized_base_alpha * confidence_factor
    return {
        "base_alpha": normalized_base_alpha,
        "effective_alpha": effective_alpha,
        "dynamic_alpha_enabled": True,
        "vector_gap": vector_gap,
        "rerank_soft_top1": rerank_soft_top1,
        "rerank_soft_top2": rerank_soft_top2,
        "rerank_soft_diff": rerank_soft_diff,
        "confidence_factor": confidence_factor,
        "alpha_reason": (
            f"sigmoid(diff={rerank_soft_diff:.4f},scale={float(diff_scale):.2f},center={float(diff_center):.4f})"
        ),
    }


def _apply_rerank_top1_guard(
    *,
    baseline_rows: list[dict[str, Any]],
    ranked_rows: list[dict[str, Any]],
    top_k: int,
    enabled: bool,
    max_drop: float,
    max_drop_ratio: float,
    top1_gap_threshold: float,
    strong_top1_threshold: float,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    guard_info: dict[str, Any] = {
        "enabled": bool(enabled),
        "triggered": False,
        "reason": "",
        "before_path": "",
        "after_path": "",
        "before_vector_score": None,
        "after_vector_score": None,
        "vector_delta": None,
        "vector_drop": None,
        "vector_drop_ratio": None,
        "baseline_gap": None,
        "swap_blocked_by_gap": False,
        "top1_gap_threshold": max(0.0, float(top1_gap_threshold)),
        "strong_top1_threshold": max(0.0, float(strong_top1_threshold)),
    }
    if not baseline_rows or not ranked_rows:
        return ranked_rows[: max(1, int(top_k))], guard_info

    before_item = baseline_rows[0]
    after_item = ranked_rows[0]
    before_path = _result_identity(before_item)
    after_path = _result_identity(after_item)
    before_score = _vector_score(before_item)
    after_score = _vector_score(after_item)
    second_score = _vector_score(baseline_rows[1]) if len(baseline_rows) > 1 else 0.0
    baseline_gap = max(0.0, before_score - second_score)
    vector_delta = after_score - before_score
    vector_drop = max(0.0, before_score - after_score)
    vector_drop_ratio = (vector_drop / before_score) if before_score > 1e-9 else 0.0
    guard_info.update(
        {
            "before_path": before_path,
            "after_path": after_path,
            "before_vector_score": before_score,
            "after_vector_score": after_score,
            "vector_delta": vector_delta,
            "vector_drop": vector_drop,
            "vector_drop_ratio": vector_drop_ratio,
            "baseline_gap": baseline_gap,
        }
    )
    if not enabled or not before_path or not after_path or before_path == after_path:
        return ranked_rows[: max(1, int(top_k))], guard_info

    if before_score >= max(0.0, strong_top1_threshold) and baseline_gap >= max(0.0, top1_gap_threshold):
        original_top1 = next((row for row in ranked_rows if _result_identity(row) == before_path), None)
        if original_top1 is None:
            original_top1 = dict(before_item)
            original_top1["vector_score"] = _vector_score(original_top1)
            original_top1["score"] = _vector_score(original_top1)
        guarded_rows = [dict(original_top1)]
        guarded_rows.extend(dict(row) for row in ranked_rows if _result_identity(row) != before_path)
        guard_info["triggered"] = True
        guard_info["swap_blocked_by_gap"] = True
        guard_info["reason"] = (
            f"swap_blocked: strong_vector_top1 gap={baseline_gap:.4f} "
            f"thresholds=({max(0.0, top1_gap_threshold):.4f}, {max(0.0, strong_top1_threshold):.4f})"
        )
        return guarded_rows[: max(1, int(top_k))], guard_info

    if vector_drop <= 0:
        return ranked_rows[: max(1, int(top_k))], guard_info

    if vector_drop <= max(0.0, max_drop) and vector_drop_ratio <= max(0.0, max_drop_ratio):
        return ranked_rows[: max(1, int(top_k))], guard_info

    original_top1 = next((row for row in ranked_rows if _result_identity(row) == before_path), None)
    if original_top1 is None:
        original_top1 = dict(before_item)
        original_top1["vector_score"] = _vector_score(original_top1)
        original_top1["score"] = _vector_score(original_top1)

    guarded_rows = [dict(original_top1)]
    guarded_rows.extend(dict(row) for row in ranked_rows if _result_identity(row) != before_path)
    guard_info["triggered"] = True
    guard_info["reason"] = (
        f"swap_blocked: vector_drop={vector_drop:.4f} ratio={vector_drop_ratio:.4f} "
        f"limits=({max(0.0, max_drop):.4f}, {max(0.0, max_drop_ratio):.4f})"
    )
    return guarded_rows[: max(1, int(top_k))], guard_info


def _finalize_reranked_rows(
    *,
    baseline_rows: list[dict[str, Any]],
    ranked_rows: list[dict[str, Any]],
    top_k: int,
    status: str,
    guard_enabled: bool,
    guard_max_drop: float,
    guard_max_drop_ratio: float,
    fusion_alpha: float,
    dynamic_alpha_enabled: bool,
    dynamic_alpha_diff_scale: float,
    dynamic_alpha_diff_center: float,
    top1_gap_threshold: float,
    strong_top1_threshold: float,
) -> tuple[list[dict[str, Any]], str, dict[str, Any]]:
    rerank_norm_scores = _softmax_normalize([float(row.get("rerank_score", 0.0) or 0.0) for row in ranked_rows])
    alpha_info = _compute_dynamic_fusion_alpha(
        baseline_rows=baseline_rows,
        rerank_norm_scores=rerank_norm_scores,
        base_alpha=fusion_alpha,
        enabled=dynamic_alpha_enabled,
        gap_threshold=top1_gap_threshold,
        diff_scale=dynamic_alpha_diff_scale,
        diff_center=dynamic_alpha_diff_center,
    )
    fused_rows = _apply_rerank_score_fusion(ranked_rows, fusion_alpha=float(alpha_info.get("effective_alpha", fusion_alpha) or 0.0))
    sorted_rows = sorted(
        fused_rows,
        key=lambda x: (
            float(x.get("final_score", x.get("rerank_score", 0.0)) or 0.0),
            float(x.get("rerank_score", 0.0) or 0.0),
            _vector_score(x),
        ),
        reverse=True,
    )
    final_rows, guard_info = _apply_rerank_top1_guard(
        baseline_rows=baseline_rows,
        ranked_rows=sorted_rows,
        top_k=top_k,
        enabled=guard_enabled,
        max_drop=guard_max_drop,
        max_drop_ratio=guard_max_drop_ratio,
        top1_gap_threshold=top1_gap_threshold,
        strong_top1_threshold=strong_top1_threshold,
    )
    guard_info["fusion_alpha"] = float(alpha_info.get("effective_alpha", fusion_alpha) or 0.0)
    guard_info["fusion_alpha_base"] = float(alpha_info.get("base_alpha", fusion_alpha) or 0.0)
    guard_info["dynamic_alpha_enabled"] = bool(alpha_info.get("dynamic_alpha_enabled"))
    guard_info["rerank_soft_top1"] = alpha_info.get("rerank_soft_top1")
    guard_info["rerank_soft_top2"] = alpha_info.get("rerank_soft_top2")
    guard_info["rerank_soft_diff"] = alpha_info.get("rerank_soft_diff")
    guard_info["rerank_confidence_factor"] = alpha_info.get("confidence_factor")
    guard_info["fusion_alpha_reason"] = str(alpha_info.get("alpha_reason", "") or "")
    final_status = status
    if guard_info.get("triggered"):
        final_status = f"{status}|top1_guard"
    return final_rows, final_status, guard_info


def _rerank_local_rows(
    *,
    query: str,
    rows: list[dict[str, Any]],
    documents_dir: Path,
    top_k: int,
    reranker_model: str,
    embedding_model: str,
    guard_enabled: bool,
    guard_max_drop: float,
    guard_max_drop_ratio: float,
    fusion_alpha: float,
    dynamic_alpha_enabled: bool,
    dynamic_alpha_diff_scale: float,
    dynamic_alpha_diff_center: float,
    top1_gap_threshold: float,
    strong_top1_threshold: float,
    short_query_candidate_k: int,
    short_query_max_chars: int,
    reranker_server_url: str = "",
) -> tuple[list[dict[str, Any]], str, float, float, dict[str, Any]]:
    """Returns (rows, status, infer_seconds, load_seconds, guard_info).

    infer_seconds = time spent only inside reranker.predict() / sidecar call
    load_seconds  = time cold-loading the model (0 if sidecar is used or cache hit)
    """
    if not rows:
        return [], "empty_input", 0.0, 0.0, {"enabled": bool(guard_enabled), "triggered": False, "reason": "empty_input"}

    candidate_count, candidate_profile = _select_rerank_candidate_count(
        query=query,
        row_count=len(rows),
        top_k=top_k,
        short_query_candidate_k=short_query_candidate_k,
        short_query_max_chars=short_query_max_chars,
    )
    candidate_rows = [dict(row) for row in rows[:candidate_count]]
    tail_rows = [dict(row) for row in rows[candidate_count:]]

    if not candidate_rows:
        return [], "empty_candidates", 0.0, 0.0, {
            "enabled": bool(guard_enabled),
            "triggered": False,
            "reason": "empty_candidates",
            "candidate_count": 0,
            "candidate_profile": candidate_profile,
        }

    # ─ Path A: delegate to the in-process reranker sidecar (zero cold-load) ─
    if reranker_server_url:
        try:
            from urllib import request as _urlreq  # noqa: PLC0415

            pairs: list[tuple[str, str]] = [
                (query, _build_rerank_text(row, documents_dir)) for row in candidate_rows
            ]
            body = json.dumps(
                {"model": reranker_model, "pairs": pairs}, ensure_ascii=False
            ).encode("utf-8")
            req = _urlreq.Request(
                reranker_server_url.rstrip("/"),
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            _infer_t0 = time.perf_counter()
            opener = _urlreq.build_opener(_urlreq.ProxyHandler({}))
            with opener.open(req, timeout=30) as resp:
                result = json.loads(resp.read().decode("utf-8"))
            _infer_elapsed = time.perf_counter() - _infer_t0

            if "error" in result:
                raise RuntimeError(result["error"])

            scores = result["scores"]
            ranked_rows: list[dict[str, Any]] = []
            for row, score in zip(candidate_rows, scores):
                item = dict(row)
                item["vector_score"] = float(item.get("vector_score", item.get("score", 0.0)))
                item["rerank_score"] = float(score)
                item["score"] = float(item.get("vector_score", item.get("score", 0.0)))
                ranked_rows.append(item)
            reranked_candidates, final_status, guard_info = _finalize_reranked_rows(
                baseline_rows=candidate_rows,
                ranked_rows=ranked_rows,
                top_k=candidate_count,
                status="sidecar",
                guard_enabled=guard_enabled,
                guard_max_drop=guard_max_drop,
                guard_max_drop_ratio=guard_max_drop_ratio,
                fusion_alpha=fusion_alpha,
                dynamic_alpha_enabled=dynamic_alpha_enabled,
                dynamic_alpha_diff_scale=dynamic_alpha_diff_scale,
                dynamic_alpha_diff_center=dynamic_alpha_diff_center,
                top1_gap_threshold=top1_gap_threshold,
                strong_top1_threshold=strong_top1_threshold,
            )
            guard_info["candidate_count"] = candidate_count
            guard_info["candidate_profile"] = candidate_profile
            final_rows = reranked_candidates + tail_rows
            return final_rows, final_status, _infer_elapsed, 0.0, guard_info
        except Exception as _sidecar_exc:  # noqa: BLE001
            print(f"PROGRESS: 重排 sidecar 调用失败（{_sidecar_exc}），正在降级为本地模型加载...", flush=True)
            # fall through to local model load

    # ─ Path B: load model locally (cold-start or sidecar unavailable) ─
    try:
        _load_t0 = time.perf_counter()
        reranker = _RERANKER_CACHE.get(reranker_model)
        if reranker is None:
            reranker = _load_local_reranker(reranker_model)
            _RERANKER_CACHE[reranker_model] = reranker
        _load_elapsed = time.perf_counter() - _load_t0

        pairs = []
        for row in candidate_rows:
            doc_text = _build_rerank_text(row, documents_dir)
            pairs.append((query, doc_text))

        _infer_t0 = time.perf_counter()
        scores = reranker.predict(pairs)
        _infer_elapsed = time.perf_counter() - _infer_t0

        ranked_rows = []
        for row, score in zip(candidate_rows, scores):
            item = dict(row)
            item["vector_score"] = float(item.get("vector_score", item.get("score", 0.0)))
            item["rerank_score"] = float(score)
            item["score"] = float(item.get("vector_score", item.get("score", 0.0)))
            ranked_rows.append(item)

        reranked_candidates, final_status, guard_info = _finalize_reranked_rows(
            baseline_rows=candidate_rows,
            ranked_rows=ranked_rows,
            top_k=candidate_count,
            status="ok",
            guard_enabled=guard_enabled,
            guard_max_drop=guard_max_drop,
            guard_max_drop_ratio=guard_max_drop_ratio,
            fusion_alpha=fusion_alpha,
            dynamic_alpha_enabled=dynamic_alpha_enabled,
            dynamic_alpha_diff_scale=dynamic_alpha_diff_scale,
            dynamic_alpha_diff_center=dynamic_alpha_diff_center,
            top1_gap_threshold=top1_gap_threshold,
            strong_top1_threshold=strong_top1_threshold,
        )
        guard_info["candidate_count"] = candidate_count
        guard_info["candidate_profile"] = candidate_profile
        final_rows = reranked_candidates + tail_rows
        return final_rows, final_status, _infer_elapsed, _load_elapsed, guard_info
    except Exception as cross_exc:  # noqa: BLE001
        try:
            normalized_model = _normalize_embedding_model_value(embedding_model)
            texts = [_build_rerank_text(row, documents_dir) for row in candidate_rows]
            vectors = _embed_texts_local([query] + texts, model=normalized_model, batch_size=32)
            query_vec = vectors[0]
            doc_vecs = vectors[1:]

            def _norm(vec: list[float]) -> float:
                return sum(x * x for x in vec) ** 0.5

            qn = _norm(query_vec) or 1e-12
            ranked_rows: list[dict[str, Any]] = []
            for row, vec in zip(candidate_rows, doc_vecs):
                dn = _norm(vec) or 1e-12
                score = sum(a * b for a, b in zip(query_vec, vec)) / (qn * dn)
                item = dict(row)
                item["vector_score"] = float(item.get("vector_score", item.get("score", 0.0)))
                item["rerank_score"] = float(score)
                item["score"] = float(item.get("vector_score", item.get("score", 0.0)))
                ranked_rows.append(item)

            reranked_candidates, final_status, guard_info = _finalize_reranked_rows(
                baseline_rows=candidate_rows,
                ranked_rows=ranked_rows,
                top_k=candidate_count,
                status=f"fallback_embedding:{cross_exc}",
                guard_enabled=guard_enabled,
                guard_max_drop=guard_max_drop,
                guard_max_drop_ratio=guard_max_drop_ratio,
                fusion_alpha=fusion_alpha,
                dynamic_alpha_enabled=dynamic_alpha_enabled,
                dynamic_alpha_diff_scale=dynamic_alpha_diff_scale,
                dynamic_alpha_diff_center=dynamic_alpha_diff_center,
                top1_gap_threshold=top1_gap_threshold,
                strong_top1_threshold=strong_top1_threshold,
            )
            guard_info["candidate_count"] = candidate_count
            guard_info["candidate_profile"] = candidate_profile
            final_rows = reranked_candidates + tail_rows
            return final_rows, final_status, 0.0, 0.0, guard_info
        except Exception as embed_exc:  # noqa: BLE001
            fallback = sorted(rows, key=lambda x: float(x.get("score", 0.0)), reverse=True)
            return (
                fallback[: max(1, int(top_k))],
                f"failed:{cross_exc}; fallback_failed:{embed_exc}",
                0.0,
                0.0,
                {
                    "enabled": bool(guard_enabled),
                    "triggered": False,
                    "reason": "fallback_vector_order",
                    "candidate_count": candidate_count,
                    "candidate_profile": candidate_profile,
                },
            )


def _is_local_llm_unavailable_error(message: str) -> bool:
    text = (message or "").strip().lower()
    if not text:
        return False

    markers = [
        "connection error",
        "connection refused",
        "connection reset",
        "failed to establish a new connection",
        "max retries exceeded",
        "api connection",
        "service unavailable",
        "timed out",
        "timeout",
        "winerror 10061",
        "errno 111",
        "cannot connect",
        "could not connect",
        "incorrect proxy service",
        "403 forbidden",
        "ie friendly error message",
        "<!doctype html",
        "<html",
    ]
    return any(marker in text for marker in markers)


def _build_local_fallback_answer(
    *,
    question: str,
    used_docs: list[dict[str, Any]],
) -> str:
    ordered_docs = [d for d in used_docs if isinstance(d, dict)]
    if not ordered_docs:
        return (
            "[提示] 本机未检测到可用的本地大模型服务，已降级为仅检索模式。\n\n"
            "当前没有检索到可用资料，请尝试换个问法或补充知识库内容。"
        )

    lines = [
        "[提示] 本机未检测到可用的本地大模型服务，已自动降级为仅检索模式。",
        "",
        f"### 问题\n{question}",
        "",
        "### 检索到的相关资料",
    ]
    for idx, item in enumerate(ordered_docs[:6], start=1):
        title = str(item.get("title", "")).strip()
        path = str(item.get("path", "")).strip() or "<unknown>"
        display = title or path
        score = float(item.get("score", 0.0))
        topic = str(item.get("topic", "")).strip()
        topic_text = f"；topic={topic}" if topic else ""
        lines.append(f"- [{idx}] `{display}`（score={score:.4f}{topic_text}）")

    lines.extend(
        [
            "",
            "### 说明",
            "- 当前回答未经过本地大模型生成，仅基于向量检索结果列出证据文档。",
            "- 启动本地 OpenAI 兼容服务后可恢复完整生成式回答。",
        ]
    )
    return "\n".join(lines).strip()

def _resolve_result_path(item: dict[str, Any], documents_dir: Path) -> Path | None:
    rel = str(item.get("relative_path", "")).strip()
    if rel:
        return documents_dir / Path(rel.replace("\\", "/"))

    file_path_text = str(item.get("file_path", "")).strip()
    if not file_path_text:
        return None

    file_path = Path(file_path_text)
    if file_path.is_absolute():
        return file_path
    return documents_dir / file_path


def _normalize_keywords(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        if "|" in text:
            return [x.strip() for x in text.split("|") if x.strip()]
        return [x.strip() for x in text.split(",") if x.strip()]
    return []


def _search_web_tavily(
    *,
    query: str,
    max_results: int,
    tavily_api_key: str,
    allow_network: bool = True,
) -> tuple[list[dict[str, Any]], str]:
    key = (tavily_api_key or "").strip() or os.getenv("TAVILY_API_KEY", "").strip()
    if not key:
        return [], "missing_api_key"

    # Check web search cache before calling the API.
    _wcache = None
    try:
        from cache_db import get_web_cache
        _wcache = get_web_cache()
        _cached = _wcache.get(query, max_results)
        if _cached is not None:
            return _cached, "cache_hit"
    except Exception:
        _wcache = None

    if not allow_network:
        return [], "disabled_cache_only"

    try:
        from langchain_tavily import TavilySearch
    except ModuleNotFoundError:
        return [], "missing_dependency"

    os.environ["TAVILY_API_KEY"] = key
    try:
        search = TavilySearch(max_results=max(1, int(max_results)))
        payload = search.invoke((query or "").strip())
    except Exception as exc:  # noqa: BLE001
        return [], f"search_error:{exc}"

    raw_results = payload.get("results", []) if isinstance(payload, dict) else []
    if not isinstance(raw_results, list):
        return [], "invalid_response"

    normalized: list[dict[str, Any]] = []
    for item in raw_results:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title", "")).strip()
        url = str(item.get("url", "")).strip()
        content = str(item.get("content", "")).strip()
        score = float(item.get("score", 0.0) or 0.0)
        if not (title or url or content):
            continue
        normalized.append(
            {
                "title": title or url or "web_result",
                "url": url,
                "content": content,
                "score": score,
            }
        )
    normalized.sort(key=lambda x: float(x.get("score", 0.0)), reverse=True)
    result_rows = normalized[: max(1, int(max_results))]
    # Cache successful results for future queries.
    try:
        if _wcache is not None and result_rows:
            _wcache.set(query, max_results, result_rows)
    except Exception:
        pass
    return result_rows, "ok"


def _compute_retrieval_confidence(
    *,
    top1_final: float,
    candidate_count: int,
    top2_final: float = 0.0,
) -> str:
    """Return 'strong', 'moderate', 'weak', or 'none'.

    Uses the definitive final score (rerank/fusion where available, vector
    otherwise) as a single source of truth.  This avoids the old ambiguity
    where top1_score, top2_score, and rerank_top1 could each be the rerank
    score, causing double-counting.
    """
    if candidate_count == 0 or top1_final < 0.30:
        return "none"
    gap = top1_final - top2_final if candidate_count >= 2 else top1_final
    if top1_final >= 0.60 and gap >= 0.10:
        return "strong"
    if top1_final >= 0.45:
        return "moderate"
    return "weak"


# ── Citation contract helpers ────────────────────────────────────────────────
_CITATION_REF_RE = re.compile(r"\[资料(\d+)\]")
_THINK_BLOCK_RE = re.compile(r"<think>.*?</think>", re.DOTALL)


def _parse_context_blocks(context_text: str) -> dict[int, str]:
    """Return {doc_number: passage_content} from the formatted context string."""
    result: dict[int, str] = {}
    if not context_text:
        return result
    parts = re.split(r"(?=\[资料\d+\])", context_text)
    for part in parts:
        m = re.match(r"\[资料(\d+)\][^\n]*\n?(.*)", part, re.DOTALL)
        if m:
            result[int(m.group(1))] = m.group(2).strip()
    return result


def _citation_token_overlap(text_a: str, text_b: str) -> int:
    """Count shared CJK bigrams + ASCII 2-char words between two strings."""
    def _tok(s: str) -> set[str]:
        toks: set[str] = set(re.findall(r"[a-z0-9]{2,}", s.lower()))
        cjk = re.findall(r"[\u4e00-\u9fff\u3400-\u4dbf\uf900-\ufaff]", s)
        toks.update(cjk[i] + cjk[i + 1] for i in range(len(cjk) - 1))
        return toks
    return len(_tok(text_a) & _tok(text_b))


def _reconcile_citations(
    answer: str,
    used_docs: list[dict[str, Any]],
    context_text: str,
    *,
    overlap_threshold: int = 5,
) -> tuple[str, dict[str, Any]]:
    """Paragraph-level citation contract: bind answer text to source passages.

    This is a structural post-processing step that tightens the link between
    answer text and the passages actually used, without requiring the LLM to
    produce perfect citations.  It operates paragraph-by-paragraph:

    1. **Phantom-ref stripping** — removes ``[资料N]`` where N > len(used_docs)
       (hallucinated doc numbers that have no backing passage).
    2. **Claim-to-doc mapping** — builds ``doc_to_claims`` from paragraphs
       that already carry a valid ``[资料N]`` marker.
    3. **Auto-annotation** — for uncited paragraphs with >= *overlap_threshold*
       shared tokens against a context block, appends the best-matching
       ``[资料N]`` inline.  Threshold default (5) is deliberately conservative.
    4. **Think-block awareness** — ``<think>...</think>`` sections are passed
       through unchanged and excluded from annotation.

    Returns ``(augmented_answer, citation_map)`` where ``citation_map`` has:
      cited_by_model  – doc indices the LLM explicitly cited
      auto_annotated  – doc indices appended by this function
      phantom_refs    – out-of-range doc indices the LLM emitted (stripped)
      uncited_docs    – context doc indices never referenced in the answer
      doc_to_claims   – {str(N): [para snippet, ...]} for downstream audit

    Limitation: binding is at *paragraph* granularity, not individual claim
    sentences.  A paragraph citing multiple facts from different docs will be
    attributed to whichever doc has the highest token overlap.  Sentence-level
    claim binding is the natural next step for stronger verifiability.
    """
    total_docs = len(used_docs)
    context_blocks = _parse_context_blocks(context_text)

    # Strip <think> blocks before semantic analysis; preserve them in output.
    analysis_text = _THINK_BLOCK_RE.sub("", answer)

    # All [资料N] numbers the model emitted (in analysis text only)
    raw_cited = {int(m) for m in _CITATION_REF_RE.findall(analysis_text)}
    cited_by_model = {n for n in raw_cited if 1 <= n <= total_docs}
    phantom_refs = sorted(raw_cited - cited_by_model)

    doc_to_claims: dict[str, list[str]] = {}
    auto_annotated: set[int] = set()

    # Process paragraph by paragraph
    paras = answer.split("\n")
    out_paras: list[str] = []
    inside_think = False
    for para in paras:
        stripped = para.strip()
        # Track <think> blocks so we don't annotate reasoning text
        if "<think>" in stripped:
            inside_think = True
        if inside_think:
            if "</think>" in stripped:
                inside_think = False
            out_paras.append(para)
            continue
        if not stripped:
            out_paras.append(para)
            continue

        # Strip phantom refs (cited number out of valid range)
        clean_para = para
        if phantom_refs:
            phantom_pat = re.compile(
                r"\[资料(" + "|".join(str(n) for n in phantom_refs) + r")\]"
            )
            clean_para = phantom_pat.sub("", para).rstrip()

        # Which valid docs does this paragraph already cite?
        para_cited = {int(m) for m in _CITATION_REF_RE.findall(clean_para) if 1 <= int(m) <= total_docs}
        for n in para_cited:
            doc_to_claims.setdefault(str(n), []).append(clean_para[:200])

        # Auto-annotate only if no citation and clear evidence found
        if not para_cited and context_blocks and len(stripped) > 20:
            best_n, best_score = 0, 0
            for doc_n, passage in context_blocks.items():
                if not (1 <= doc_n <= total_docs):
                    continue
                score = _citation_token_overlap(clean_para, passage)
                if score > best_score:
                    best_n, best_score = doc_n, score
            if best_score >= overlap_threshold:
                clean_para = clean_para + f" [资料{best_n}]"
                doc_to_claims.setdefault(str(best_n), []).append(clean_para[:200])
                auto_annotated.add(best_n)

        out_paras.append(clean_para)

    all_cited = cited_by_model | auto_annotated
    uncited_docs = [n for n in range(1, total_docs + 1) if n not in all_cited]

    citation_map: dict[str, Any] = {
        "cited_by_model": sorted(cited_by_model),
        "auto_annotated": sorted(auto_annotated),
        "phantom_refs": phantom_refs,
        "uncited_docs": uncited_docs,
        "doc_to_claims": doc_to_claims,
    }
    return "\n".join(out_paras), citation_map


# ─────────────────────────────────────────────────────────────────────────────
def _build_topic_context_segment(item: dict[str, Any], index_no: int) -> tuple[str, dict[str, Any]]:
    rel = str(item.get("relative_path", "")).strip() or str(item.get("file_path", "")).strip() or "<unknown>"
    title = str(item.get("title", "")).strip()
    summary = str(item.get("summary", "")).strip()
    topic = str(item.get("topic", "")).strip() or "untitled-topic"
    score = float(item.get("score", 0.0))
    rerank_score = item.get("rerank_score")
    keywords = _normalize_keywords(item.get("keywords", []))
    if not summary:
        summary = topic

    kw_text = ", ".join(keywords[:12]) if keywords else ""
    score_text = f"score={score:.4f}"
    try:
        if rerank_score is not None:
            score_text = f"{score_text} rerank={float(rerank_score):.4f}"
    except Exception:
        pass
    body_lines = [
        f"[资料{index_no}] path={rel} {score_text}",
        f"topic: {topic}",
    ]
    if title:
        body_lines.append(f"title: {title}")
    if summary:
        body_lines.append(f"summary: {summary}")
    if kw_text:
        body_lines.append(f"keywords: {kw_text}")

    used_doc = {
        "path": rel,
        "score": score,
        "title": title,
        "summary": summary,
        "topic": topic,
    }
    try:
        if rerank_score is not None:
            used_doc["rerank_score"] = float(rerank_score)
    except Exception:
        pass
    return "\n".join(body_lines), used_doc


def _extract_relevant_passage(
    text: str,
    query: str,
    max_chars: int,
    *,
    reranker_fn: Any | None = None,
    passage_candidates: int = 12,
) -> str:
    """Extract the most query-relevant passage window from a markdown document.

    Pipeline:
    1. Split *text* into paragraphs at blank-line boundaries.
    2. **Token-overlap pre-filter** — score every paragraph by shared CJK
       bigrams + ASCII 2-char words with *query*; select the top
       *passage_candidates* (default 12) that have at least one match.
    3. **Semantic re-ranking** (when *reranker_fn* is provided) — run the
       cross-encoder on (query, paragraph[:1200]) pairs from step 2 and
       pick the highest-scoring paragraph.  Uses the already-warm reranker
       from the doc-level rerank step (zero cold-start cost).
    4. **Window expansion** — expand a contiguous window around the best
       paragraph (alternating prev/next) until *max_chars* is reached.
    5. Falls back to document prefix only when no query terms match at all.

    Limitation: window expansion is fixed left-right alternation and does not
    score neighbouring paragraphs by relevance.  This is the natural next
    optimisation point if passage precision matters further.
    """
    if not text or max_chars <= 0:
        return text[:max_chars] if max_chars > 0 else ""
    if not query:
        return text[:max_chars]

    # Build query token set: ASCII words ≥2 chars + single CJK chars + CJK bigrams
    q_low = query.lower()
    q_tokens: set[str] = set(re.findall(r"[a-z0-9]{2,}", q_low))
    cjk = re.findall(r"[\u4e00-\u9fff\u3400-\u4dbf\uf900-\ufaff]", query)
    q_tokens.update(cjk)
    q_tokens.update(cjk[i] + cjk[i + 1] for i in range(len(cjk) - 1))

    paragraphs = [p for p in re.split(r"\n{2,}", text) if p.strip()]
    if not paragraphs:
        return text[:max_chars]

    def _token_score(p: str) -> int:
        low = p.lower()
        return sum(1 for t in q_tokens if t in low)

    token_scores = [_token_score(p) for p in paragraphs]
    any_match = q_tokens and max(token_scores, default=0) > 0

    best = -1

    # Semantic selection: pre-filter to top-N by token overlap, then rerank
    if reranker_fn is not None and any_match:
        top_indices = sorted(
            range(len(paragraphs)),
            key=lambda i: token_scores[i],
            reverse=True,
        )[:passage_candidates]
        candidate_indices = [i for i in top_indices if token_scores[i] > 0]
        if candidate_indices:
            pairs = [(query, paragraphs[i][:1200]) for i in candidate_indices]
            try:
                rk_scores = reranker_fn(pairs)
                if len(rk_scores) == len(candidate_indices):
                    best = candidate_indices[max(range(len(rk_scores)), key=lambda k: rk_scores[k])]
            except Exception:
                pass

    # Token-overlap fallback
    if best < 0:
        if not any_match:
            return text[:max_chars]
        best = max(range(len(token_scores)), key=lambda i: token_scores[i])

    # Expand window around best paragraph, alternating prev/next
    lo = hi = best
    total = len(paragraphs[best])
    while True:
        grew = False
        if lo > 0:
            add = len(paragraphs[lo - 1]) + 2  # +2 for the "\n\n" separator
            if total + add <= max_chars:
                lo -= 1
                total += add
                grew = True
        if hi < len(paragraphs) - 1:
            add = len(paragraphs[hi + 1]) + 2
            if total + add <= max_chars:
                hi += 1
                total += add
                grew = True
        if not grew:
            break

    return "\n\n".join(paragraphs[lo : hi + 1])[:max_chars]


def _load_context(
    *,
    results: list[dict[str, Any]],
    documents_dir: Path,
    max_context_chars: int,
    max_context_docs: int,
    max_chars_per_doc: int,
    similarity_threshold: float = 0.0,
    context_mode: str = "passage_first",
    passage_first_k: int = 3,
    query: str = "",
    reranker_fn: Any | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    """Load and build context from search results, filtering by similarity threshold.

    context_mode:
      passage_first — top passage_first_k docs get real markdown content;
                      remaining docs fall back to topic/summary metadata (default).
      topic         — all docs use title/summary/keywords metadata only.
      full          — all docs load full markdown body.
    """
    filtered_results = [
        item for item in results
        if float(item.get("score", 0.0)) >= similarity_threshold
    ]

    context_parts: list[str] = []
    used_docs: list[dict[str, Any]] = []
    total_chars = 0

    mode = (context_mode or "passage_first").strip().lower()
    pk = max(1, int(passage_first_k)) if mode == "passage_first" else 0

    for i, item in enumerate(filtered_results[: max(1, int(max_context_docs))], start=1):
        use_full = (mode == "full") or (mode == "passage_first" and i <= pk)

        if not use_full:
            segment, used_doc = _build_topic_context_segment(item, i)
            segment = segment + "\n"
        else:
            file_path = _resolve_result_path(item, documents_dir)
            if file_path is None or not file_path.is_file():
                # Fallback to topic/summary when file is missing
                segment, used_doc = _build_topic_context_segment(item, i)
                segment = segment + "\n"
            else:
                try:
                    raw_text = file_path.read_text(encoding="utf-8", errors="ignore")
                except Exception:
                    segment, used_doc = _build_topic_context_segment(item, i)
                    segment = segment + "\n"
                else:
                    clipped_text = _extract_relevant_passage(raw_text, query, max_chars_per_doc, reranker_fn=reranker_fn)
                    rel = str(item.get("relative_path", "")).strip() or file_path.name
                    topic = str(item.get("topic", "")).strip()
                    score = float(item.get("score", 0.0))
                    segment = (
                        f"[资料{i}] path={rel} score={score:.4f} topic={topic}\n"
                        f"{clipped_text}\n"
                    )
                    used_doc = {
                        "path": rel,
                        "score": score,
                        "topic": topic,
                    }

        if total_chars + len(segment) > max_context_chars:
            break

        context_parts.append(segment)
        total_chars += len(segment)
        used_docs.append(used_doc)

    return "\n\n".join(context_parts).strip(), used_docs


def _append_web_context(
    *,
    context_text: str,
    used_docs: list[dict[str, Any]],
    web_results: list[dict[str, Any]],
    max_context_chars: int,
    max_context_docs: int,
) -> tuple[str, list[dict[str, Any]]]:
    if not web_results:
        return context_text, used_docs

    parts: list[str] = []
    total_chars = len(context_text)
    next_index = len(used_docs) + 1
    docs = list(used_docs)

    remaining_slots = max(0, int(max_context_docs) - len(docs))
    for item in web_results[:remaining_slots]:
        title = str(item.get("title", "")).strip() or "web_result"
        url = str(item.get("url", "")).strip() or ""
        content = str(item.get("content", "")).strip()
        score = float(item.get("score", 0.0) or 0.0)

        snippet = content[:800]
        segment = (
            f"[资料{next_index}] path={url or title} score={score:.4f} source=web\n"
            f"title: {title}\n"
            f"content: {snippet}\n"
        )
        if total_chars + len(segment) > max_context_chars:
            break

        parts.append(segment)
        docs.append(
            {
                "path": url or title,
                "title": title,
                "score": score,
                "topic": "web_search",
            }
        )
        total_chars += len(segment)
        next_index += 1

    extra = "\n\n".join(parts).strip()
    if not extra:
        return context_text, docs
    if context_text.strip():
        return f"{context_text}\n\n{extra}", docs
    return extra, docs


def _load_context_hybrid(
    *,
    rows: list[dict[str, Any]],
    documents_dir: Path,
    max_context_chars: int,
    max_context_docs: int,
    max_chars_per_doc: int,
    similarity_threshold: float = 0.0,
    context_mode: str = "passage_first",
    passage_first_k: int = 3,
    query: str = "",
    reranker_fn: Any | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    """Build context from mixed local/web rows sorted by score (desc)."""
    def _sort_score(item: dict[str, Any]) -> float:
        source = str(item.get("source", "local")).strip().lower()
        if source != "web":
            try:
                return float(item.get("rerank_score", item.get("score", 0.0)) or 0.0)
            except Exception:
                return 0.0
        try:
            return float(item.get("score", 0.0) or 0.0)
        except Exception:
            return 0.0

    sorted_rows = sorted(rows, key=_sort_score, reverse=True)
    mode = (context_mode or "passage_first").strip().lower()
    pk = max(1, int(passage_first_k)) if mode == "passage_first" else 0

    context_parts: list[str] = []
    used_docs: list[dict[str, Any]] = []
    total_chars = 0
    idx = 1
    local_count = 0  # track how many local docs have been included for passage_first

    for item in sorted_rows:
        if len(used_docs) >= max(1, int(max_context_docs)):
            break
        source = str(item.get("source", "local")).strip().lower()
        score = float(item.get("score", 0.0))

        if source != "web" and score < float(similarity_threshold):
            continue

        if source == "web":
            title = str(item.get("title", "")).strip() or "web_result"
            url = str(item.get("url", "")).strip()
            content = str(item.get("content", "")).strip()[:800]
            segment = (
                f"[资料{idx}] path={url or title} score={score:.4f} source=web\n"
                f"title: {title}\n"
                f"content: {content}\n"
            )
            used_doc = {
                "path": url or title,
                "title": title,
                "score": score,
                "topic": "web_search",
            }
        else:
            local_count += 1
            use_full = (mode == "full") or (mode == "passage_first" and local_count <= pk)

            if not use_full:
                segment, used_doc = _build_topic_context_segment(item, idx)
                segment = segment + "\n"
            else:
                file_path = _resolve_result_path(item, documents_dir)
                if file_path is None or not file_path.is_file():
                    # Fallback to topic/summary when file is missing
                    segment, used_doc = _build_topic_context_segment(item, idx)
                    segment = segment + "\n"
                else:
                    try:
                        raw_text = file_path.read_text(encoding="utf-8", errors="ignore")
                    except Exception:
                        segment, used_doc = _build_topic_context_segment(item, idx)
                        segment = segment + "\n"
                    else:
                        clipped_text = _extract_relevant_passage(raw_text, query, max_chars_per_doc, reranker_fn=reranker_fn)
                        rel = str(item.get("relative_path", "")).strip() or file_path.name
                        topic = str(item.get("topic", "")).strip()
                        rerank_score = item.get("rerank_score")
                        score_text = f"score={score:.4f}"
                        try:
                            if rerank_score is not None:
                                score_text = f"{score_text} rerank={float(rerank_score):.4f}"
                        except Exception:
                            pass
                        segment = (
                            f"[资料{idx}] path={rel} {score_text} topic={topic}\n"
                            f"{clipped_text}\n"
                        )
                        used_doc = {
                            "path": rel,
                            "score": score,
                            "topic": topic,
                        }
                        try:
                            if rerank_score is not None:
                                used_doc["rerank_score"] = float(rerank_score)
                        except Exception:
                            pass

        if total_chars + len(segment) > max_context_chars:
            break
        context_parts.append(segment)
        used_docs.append(used_doc)
        total_chars += len(segment)
        idx += 1

    return "\n\n".join(context_parts).strip(), used_docs


def _ask_llm(
    *,
    api_url: str,
    api_key: str,
    model: str,
    timeout: int,
    question: str,
    context_text: str,
    call_type: str = "answer",
    memory_context: str = "",
    stream: bool = False,
    stream_callback: Any = None,
    trace_id: str = "",
    debug_trace: dict[str, Any] | None = None,
    llm_stats_sink: dict[str, Any] | None = None,
    retrieval_confidence: str = "",
) -> str:
    if not api_url or not api_key or not model:
        raise RuntimeError("Missing API settings: api_url/api_key/model are required")

    response_max_tokens = DEFAULT_RESPONSE_MAX_TOKENS_WITH_CONTEXT if context_text.strip() else DEFAULT_RESPONSE_MAX_TOKENS_NO_CONTEXT

    system_prompt = (
        "你是一个知识助手。回答时区分三类证据来源：\n"
        "① 本地资料事实 — 直接陈述，可用「[资料N]」轻度标注来源编号\n"
        "② 通用知识补充 — 行内或末尾标注「[通用知识]」\n"
        "③ 推断/外推 — 标注「[推断]」，语气保守\n"
        "严禁混淆三类来源或编造资料中不存在的事实。\n"
        f"当前调用类型 call_type={call_type or 'answer'}。\n\n"
        "4) 如果资料无法回答，应明确说明\n"
        "5) 输出使用Markdown\n"
        "6) 默认给出更完整的解释；优先包含结论、原因机制、影响或局限。\n"
    )
    
    memory_block = (memory_context or "").strip()
    memory_section = f"\n会话记忆(可为空):\n{memory_block}\n" if memory_block else ""

    # Retrieval confidence note — injected when results are weak or absent
    _conf = (retrieval_confidence or "").strip().lower()
    if _conf == "none":
        confidence_note = "检索结果：本地知识库中未找到相关资料，请主要依赖通用知识（须标注「[通用知识]」）。\n"
    elif _conf == "weak":
        confidence_note = "检索置信度：弱（相关性偏低），资料仅供参考，请适当降低确定性，补充通用知识须标注「[通用知识]」。\n"
    else:
        confidence_note = ""

    if context_text.strip():
        user_prompt = (
            "请先阅读下面的本地检索资料，再回答用户问题。\n"
            "会话记忆：\n"
            f"{memory_section}"
            f"{confidence_note}"
            "检索资料：\n"
            f"资料:\n{context_text}\n\n"
            "用户问题：\n"
            f"问题:\n{question}\n"
            "回答要求：\n"
            "1) 如果问题复杂，允许进行简短思考（不超过400字），用<think>思考内容</think>标签包裹；\n"
            "2) 基于资料和会话记忆（如果不为空）给出最终答案；\n"
            "3) 资料不足或有必要时可用通用知识补充，须标注「[通用知识]」；\n"
            "4) 默认至少写成 3 个要点或 2 段以上，说明背景、关键机制、实际影响；\n"
            "5) 不要编造不存在的细节；\n"
            "6) 可用「[资料N]」格式轻度引用来源，末尾不必重复完整资料列表。\n\n"
        )
    else:
        user_prompt = (
            "会话记忆：\n"
            f"{memory_section}"
            f"{confidence_note}"
            "用户问题：\n"
            f"问题:\n{question}\n"
            "本地知识库中未找到与问题高度相关的资料。\n"
            "请基于通用知识回答下面问题，并明确标注「[通用知识]」，区分已知事实与推断（标「[推断]」）。\n"
            "回答要求：\n"
            "1) 明确说明未找到相关本地资料；\n"
            "2) 基于通用知识给出尽量完整的分析，不要只给结论；\n"
            "3) 默认至少写成 3 个要点，覆盖背景、原因机制、影响或建议；\n"
            "4) 不要编造不存在的细节。\n\n"
        )

    request_id = str(trace_id or "").strip() or str(uuid4())
    should_audit = _is_deepseek_url(api_url)
    request_messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]
    context_tokens_est = _approx_tokens(context_text)
    input_tokens_est = _approx_tokens(system_prompt) + _approx_tokens(user_prompt)
    prompt_tokens_est = max(0, input_tokens_est - context_tokens_est)
    if debug_trace is not None:
        debug_trace["llm_request"] = {
            "trace_id": request_id,
            "api_url": api_url,
            "model": model,
            "messages": request_messages,
            "memory_tokens_est": _approx_tokens(memory_context),
            "input_tokens_est": input_tokens_est,
            "prompt_tokens_est": prompt_tokens_est,
            "context_tokens_est": context_tokens_est,
        }
    if llm_stats_sink is not None:
        llm_stats_sink["api_url"] = api_url
        llm_stats_sink["model"] = model
        llm_stats_sink["response_max_tokens"] = response_max_tokens
        llm_stats_sink["input_tokens_est"] = input_tokens_est
        llm_stats_sink["prompt_tokens_est"] = prompt_tokens_est
        llm_stats_sink["context_tokens_est"] = context_tokens_est
        llm_stats_sink["memory_tokens_est"] = _approx_tokens(memory_context)
        llm_stats_sink["calls"] = 1

    if stream:
        # Stream mode: output chunks to stdout in real-time.
        try:
            response_stream = stream_chat_completion_text(
                api_key=api_key,
                base_url=api_url,
                model=model,
                messages=request_messages,
                temperature=0.2,
                timeout=timeout,
                max_tokens=response_max_tokens,
            )
        except Exception as exc:  # noqa: BLE001
            if should_audit:
                _write_deepseek_audit_log(
                    {
                        "request_id": request_id,
                        "created_at": datetime.now().isoformat(timespec="seconds"),
                        "api_provider": "deepseek",
                        "api_url": api_url,
                        "api_key_masked": _mask_secret(api_key),
                        "model": model,
                        "call_type": call_type,
                        "stream": True,
                        "request_messages": request_messages,
                        "response_max_tokens": response_max_tokens,
                        "response_text": "",
                        "error": str(exc),
                    }
                )
            raise RuntimeError(str(exc)) from exc
        
        answer_chunks = []
        try:
            for content in response_stream:
                answer_chunks.append(content)
                if callable(stream_callback):
                    stream_callback(str(content))
                else:
                    # Output chunk as JSON on a single line so embedded newlines are preserved.
                    print(f"STREAM_CHUNK_JSON: {json.dumps(content, ensure_ascii=False)}", flush=True)
        except Exception as exc:  # noqa: BLE001
            if should_audit:
                _write_deepseek_audit_log(
                    {
                        "request_id": request_id,
                        "created_at": datetime.now().isoformat(timespec="seconds"),
                        "api_provider": "deepseek",
                        "api_url": api_url,
                        "api_key_masked": _mask_secret(api_key),
                        "model": model,
                        "call_type": call_type,
                        "stream": True,
                        "request_messages": request_messages,
                        "response_max_tokens": response_max_tokens,
                        "response_text": "".join(answer_chunks),
                        "error": str(exc),
                    }
                )
            raise RuntimeError(str(exc)) from exc
        
        answer = "".join(answer_chunks).strip()
        if not answer:
            raise RuntimeError("LLM response text is empty")
        output_tokens_est = _approx_tokens(answer)
        truncation_suspected = (
            output_tokens_est >= max(200, int(response_max_tokens * 0.9))
            and not re.search(r"[。！？.!?）)】\]」』>]\s*$", answer)
        )
        if truncation_suspected:
            answer += "\n\n_[回答可能因长度上限被截断，可继续追问“继续”获取后续内容]_"
            output_tokens_est = _approx_tokens(answer)
        if debug_trace is not None:
            debug_trace["llm_response"] = {
                "trace_id": request_id,
                "response_max_tokens": response_max_tokens,
                "output_tokens_est": output_tokens_est,
                "truncation_suspected": truncation_suspected,
            }
        if llm_stats_sink is not None:
            llm_stats_sink["output_tokens_est"] = output_tokens_est
            llm_stats_sink["truncation_suspected"] = truncation_suspected
        if should_audit:
            _write_deepseek_audit_log(
                {
                    "request_id": request_id,
                    "created_at": datetime.now().isoformat(timespec="seconds"),
                    "api_provider": "deepseek",
                    "api_url": api_url,
                    "api_key_masked": _mask_secret(api_key),
                    "model": model,
                    "call_type": call_type,
                    "stream": True,
                    "request_messages": request_messages,
                    "response_max_tokens": response_max_tokens,
                    "output_tokens_est": output_tokens_est,
                    "truncation_suspected": truncation_suspected,
                    "response_text": answer,
                    "error": "",
                }
            )
        return answer
    else:
        # Non-stream mode: original behavior.
        try:
            answer = chat_completion(
                api_key=api_key,
                base_url=api_url,
                model=model,
                messages=request_messages,
                temperature=0.2,
                timeout=timeout,
                max_tokens=response_max_tokens,
            )
        except Exception as exc:  # noqa: BLE001
            if should_audit:
                _write_deepseek_audit_log(
                    {
                        "request_id": request_id,
                        "created_at": datetime.now().isoformat(timespec="seconds"),
                        "api_provider": "deepseek",
                        "api_url": api_url,
                        "api_key_masked": _mask_secret(api_key),
                        "model": model,
                        "call_type": call_type,
                        "stream": False,
                        "request_messages": request_messages,
                        "response_max_tokens": response_max_tokens,
                        "response_text": "",
                        "error": str(exc),
                    }
                )
            raise RuntimeError(str(exc)) from exc
        output_tokens_est = _approx_tokens(answer)
        truncation_suspected = (
            output_tokens_est >= max(200, int(response_max_tokens * 0.9))
            and not re.search(r"[。！？.!?）)】\]」』>]\s*$", answer)
        )
        if truncation_suspected:
            answer += "\n\n_[回答可能因长度上限被截断，可继续追问“继续”获取后续内容]_"
            output_tokens_est = _approx_tokens(answer)
        if should_audit:
            _write_deepseek_audit_log(
                {
                    "request_id": request_id,
                    "created_at": datetime.now().isoformat(timespec="seconds"),
                    "api_provider": "deepseek",
                    "api_url": api_url,
                    "api_key_masked": _mask_secret(api_key),
                    "model": model,
                    "call_type": call_type,
                    "stream": False,
                    "request_messages": request_messages,
                    "response_max_tokens": response_max_tokens,
                    "output_tokens_est": output_tokens_est,
                    "truncation_suspected": truncation_suspected,
                    "response_text": answer,
                    "error": "",
                }
            )
        if debug_trace is not None:
            debug_trace["llm_response"] = {
                "trace_id": request_id,
                "response_max_tokens": response_max_tokens,
                "output_tokens_est": output_tokens_est,
                "truncation_suspected": truncation_suspected,
            }
        if llm_stats_sink is not None:
            llm_stats_sink["output_tokens_est"] = output_tokens_est
            llm_stats_sink["truncation_suspected"] = truncation_suspected
        return answer


def _fallback_session_title(question: str, max_len: int | None = None) -> str:
    normalized = " ".join((question or "").split())
    normalized = re.sub(r"^(请|帮我|请帮我|麻烦|比较|请比较|分析|请分析|解释|请解释|说明|请说明)\s*", "", normalized)
    normalized = normalized.strip("，。！？!?；;:： ")
    if not normalized:
        return "未命名会话"
    if max_len is None or max_len <= 0 or len(normalized) <= max_len:
        return normalized
    return normalized[:max_len]


def _generate_session_title(question: str, answer: str, api_key: str, api_url: str, model: str, timeout: int) -> str:
    """Use LLM to generate an informative session title without hard truncation."""

    title_prompt = (
        f"请根据以下问答生成一个信息密度高、不要照抄提问开头的中文标题。"
        f"保留关键限定词，一般 10 到 20 字即可。"
        f"如果问题是在比较多个对象，请优先总结比较主题或对象类别。"
        f"直接输出标题，不要加引号、句号或其他说明。\n\n"
        f"问题：{question[:200]}\n"
        f"回答：{answer[:300]}\n\n"
        f"标题："
    )
    
    try:
        title = chat_completion(
            api_key=api_key,
            base_url=api_url,
            model=model,
            timeout=timeout,
            messages=[
                {"role": "system", "content": "你是一个擅长总结的助手。你的任务是根据用户的问答生成简短标题。"},
                {"role": "user", "content": title_prompt},
            ],
            temperature=0.3,
            max_tokens=50,
        )
        # Remove quotes if LLM added them.
        title = title.strip('"\'《》〈〉')
        if title:
            return title
    except Exception:
        pass
    
    return _fallback_session_title(question)


def generate_session_title(question: str, answer: str, *, api_key: str, api_url: str, model: str, timeout: int) -> str:
    return _generate_session_title(question, answer, api_key=api_key, api_url=api_url, model=model, timeout=timeout)


def _emit_progress(args: argparse.Namespace, message: str, progress_callback: Any = None) -> None:
    if not getattr(args, "stream", False):
        return
    text = str(message or "").strip()
    if not text:
        return
    if callable(progress_callback):
        progress_callback(text)
        return
    print(f"PROGRESS: {text}", flush=True)


def _emit_stream_chunk(args: argparse.Namespace, content: str, stream_callback: Any = None) -> None:
    if not getattr(args, "stream", False):
        return
    text = str(content or "")
    if not text:
        return
    if callable(stream_callback):
        stream_callback(text)
        return
    print(f"STREAM_CHUNK_JSON: {json.dumps(text, ensure_ascii=False)}", flush=True)


def run_rag_query(
    args: argparse.Namespace,
    *,
    progress_callback: Any = None,
    stream_callback: Any = None,
) -> dict[str, Any]:
    # Propagate embedding sidecar URL to rag_vector_index via env var.
    _embed_server_url = str(getattr(args, "embed_server_url", "") or "").strip()
    if _embed_server_url:
        os.environ["_RAG_EMBED_SIDECAR_URL"] = _embed_server_url

    question = (getattr(args, "question", "") or "").strip()
    if not question:
        raise RuntimeError("Question is empty")

    t0 = time.perf_counter()
    _emit_progress(args, "正在检查索引并准备检索...", progress_callback)

    search_mode = (args.search_mode or "hybrid").strip().lower()
    debug_enabled = _parse_bool_arg(getattr(args, "debug", False), default=False)
    trace_id = str(getattr(args, "trace_id", "") or "").strip() or f"trace_{uuid4().hex[:16]}"
    enable_rerank = _parse_bool_arg(args.enable_rerank, default=DEFAULT_ENABLE_RERANK)
    enable_query_rewrite = _parse_bool_arg(getattr(args, "enable_query_rewrite", False), default=DEFAULT_ENABLE_QUERY_REWRITE)
    rewrite_count = max(1, int(getattr(args, "query_rewrite_count", DEFAULT_QUERY_REWRITE_COUNT)))
    max_vector_candidates = max(1, int(getattr(args, "max_vector_candidates", DEFAULT_MAX_VECTOR_CANDIDATES) or DEFAULT_MAX_VECTOR_CANDIDATES))
    vector_top_n = min(max_vector_candidates, max(1, int(args.vector_top_n)))
    rerank_top_k = max(1, int(args.rerank_top_k))
    allow_local_only_web_cache = DEFAULT_LOCAL_ONLY_READ_WEB_CACHE
    if search_mode == "local_only":
        final_local_top_k = max(1, int(args.top_k))
        web_top_k = 3 if allow_local_only_web_cache else 0
        allow_web_network = False
    else:
        final_local_top_k = rerank_top_k
        web_top_k = 3
        allow_web_network = True
    max_context_chars = max(1000, int(args.max_context_chars))
    max_context_docs = max(1, int(getattr(args, "max_context_docs", DEFAULT_MAX_CONTEXT_DOCS) or DEFAULT_MAX_CONTEXT_DOCS))

    _embed_warmup_t0 = time.perf_counter()
    try:
        _embed_texts_local(["热启动"], model=args.embedding_model, batch_size=1)
    except Exception:
        pass
    _embed_warmup_seconds = round(time.perf_counter() - _embed_warmup_t0, 3)

    rewrite_t0 = time.perf_counter()
    rewrite_queries = [question]
    rewrite_status = "disabled"
    if enable_query_rewrite and _should_rewrite_query(question):
        _emit_progress(args, "正在改写查询并扩展检索词...", progress_callback)
        rewrite_queries, rewrite_status = _rewrite_queries_with_local_llm(
            question=question,
            memory_context=str(args.memory_context or ""),
            rewrite_count=rewrite_count,
            timeout=int(args.timeout),
        )
    elif enable_query_rewrite:
        rewrite_status = "skipped:simple_query"
    rewrite_elapsed = round(time.perf_counter() - rewrite_t0, 3)
    _emit_progress(args, "正在进行图谱扩展并准备向量召回...", progress_callback)

    per_query_rows: list[tuple[str, list[dict[str, Any]]]] = []
    timings: dict[str, Any] = {
        "prepare_index": 0.0,
        "embed_query": 0.0,
        "faiss_search": 0.0,
        "total": 0.0,
        "embed_warmup_seconds": _embed_warmup_seconds,
        "query_rewrite_seconds": rewrite_elapsed,
        "query_rewrite_status": rewrite_status,
        "query_rewrite_queries": rewrite_queries,
    }
    expanded_batches: list[dict[str, Any]] = []
    retrieval_queries: list[tuple[str, str]] = []

    _graph_file = Path(args.index_dir) / "knowledge_graph_rag.json"
    _meta_file = Path(args.index_dir) / "metadata.json"
    _graph_needs_build = _meta_file.exists() and (
        not _graph_file.exists() or _graph_file.stat().st_size < 200
    )
    if _graph_needs_build:
        _emit_progress(args, "首次构建知识图谱索引（只需一次）...", progress_callback)
        try:
            sync_rag_graph(Path(args.index_dir), only_missing=True, use_llm=False)
        except Exception:
            pass

    for rewrite_query in rewrite_queries:
        graph_expand = expand_query_by_graph(Path(args.index_dir), rewrite_query)
        expanded_query = str(graph_expand.get("expanded_query") or rewrite_query).strip() or rewrite_query
        retrieval_queries.append((rewrite_query, expanded_query))
        expanded_batches.append(
            {
                "rewrite_query": rewrite_query,
                "expanded_query": expanded_query,
                "graph_active": expanded_query != rewrite_query,
                "seed_concepts": list(graph_expand.get("seed_concepts") or []),
                "expanded_concepts": list(graph_expand.get("expanded_concepts") or []),
            }
        )

    _emit_progress(args, f"正在进行向量召回（检索批次 {len(retrieval_queries)}）...", progress_callback)

    for rewrite_query, expanded_query in retrieval_queries:
        one_results, one_timing = search_vector_index_with_diagnostics(
            query=expanded_query,
            documents_dir=Path(args.documents_dir),
            index_dir=Path(args.index_dir),
            top_k=vector_top_n,
            backend=args.backend,
            build_if_missing=BUILD_INDEX_ON_DEMAND,
            embedding_model=args.embedding_model,
            timeout=int(args.timeout),
            no_embed_cache=bool(getattr(args, "no_embed_cache", False)),
        )
        rows = [dict(item) for item in one_results if isinstance(item, dict)]
        per_query_rows.append((rewrite_query, rows))
        for key in ("prepare_index", "embed_query", "faiss_search", "total"):
            timings[key] = float(timings.get(key, 0.0)) + float(one_timing.get(key, 0.0) or 0.0)
        timings["embed_cache_hit"] = max(
            float(timings.get("embed_cache_hit", 0.0)),
            float(one_timing.get("embed_cache_hit", 0.0) or 0.0),
        )

    results, vector_query_batches = _merge_multi_query_vector_rows(per_query_rows, primary_query=question)
    results = _cap_vector_candidates(results, max_vector_candidates)

    local_threshold = float(args.similarity_threshold)
    local_rows = [dict(item) for item in results if isinstance(item, dict)]
    threshold_rows = [r for r in local_rows if float(r.get("score", 0.0)) >= local_threshold]
    rerank_input_rows = threshold_rows or local_rows
    rerank_status = "disabled"
    _rerank_infer_s = 0.0
    _rerank_load_s = 0.0
    rerank_guard_enabled = _parse_bool_arg(
        getattr(args, "enable_rerank_top1_guard", DEFAULT_ENABLE_RERANK_TOP1_GUARD),
        default=DEFAULT_ENABLE_RERANK_TOP1_GUARD,
    )
    rerank_guard_info: dict[str, Any] = {"enabled": rerank_guard_enabled, "triggered": False, "reason": "disabled"}

    if enable_rerank:
        _emit_progress(args, f"已向量召回 {len(results)} 个候选，正在重排本地结果...", progress_callback)
        reranked_local_rows, rerank_status, _rerank_infer_s, _rerank_load_s, rerank_guard_info = _rerank_local_rows(
            query=question,
            rows=rerank_input_rows,
            documents_dir=Path(args.documents_dir),
            top_k=final_local_top_k,
            reranker_model=str(args.reranker_model or DEFAULT_RERANKER_MODEL).strip(),
            embedding_model=str(args.embedding_model or EMBEDDING_MODEL or "BAAI/bge-base-zh-v1.5").strip(),
            guard_enabled=rerank_guard_enabled,
            guard_max_drop=float(getattr(args, "rerank_guard_max_top1_drop", DEFAULT_RERANK_GUARD_MAX_TOP1_DROP) or 0.0),
            guard_max_drop_ratio=float(getattr(args, "rerank_guard_max_top1_drop_ratio", DEFAULT_RERANK_GUARD_MAX_TOP1_DROP_RATIO) or 0.0),
            fusion_alpha=float(getattr(args, "rerank_fusion_alpha", DEFAULT_RERANK_FUSION_ALPHA) or DEFAULT_RERANK_FUSION_ALPHA),
            dynamic_alpha_enabled=_parse_bool_arg(getattr(args, "enable_dynamic_rerank_alpha", DEFAULT_ENABLE_DYNAMIC_RERANK_ALPHA), default=DEFAULT_ENABLE_DYNAMIC_RERANK_ALPHA),
            dynamic_alpha_diff_scale=float(getattr(args, "dynamic_rerank_alpha_diff_scale", DEFAULT_DYNAMIC_RERANK_ALPHA_DIFF_SCALE) or DEFAULT_DYNAMIC_RERANK_ALPHA_DIFF_SCALE),
            dynamic_alpha_diff_center=float(getattr(args, "dynamic_rerank_alpha_diff_center", DEFAULT_DYNAMIC_RERANK_ALPHA_DIFF_CENTER) or DEFAULT_DYNAMIC_RERANK_ALPHA_DIFF_CENTER),
            top1_gap_threshold=float(getattr(args, "rerank_top1_gap_threshold", DEFAULT_RERANK_TOP1_GAP_THRESHOLD) or 0.0),
            strong_top1_threshold=float(getattr(args, "rerank_strong_top1_threshold", DEFAULT_RERANK_STRONG_TOP1_THRESHOLD) or 0.0),
            short_query_candidate_k=int(getattr(args, "short_query_rerank_candidate_k", DEFAULT_SHORT_QUERY_RERANK_CANDIDATE_K) or DEFAULT_SHORT_QUERY_RERANK_CANDIDATE_K),
            short_query_max_chars=int(getattr(args, "short_query_max_chars", DEFAULT_SHORT_QUERY_MAX_CHARS) or DEFAULT_SHORT_QUERY_MAX_CHARS),
            reranker_server_url=str(getattr(args, "reranker_server_url", "") or "").strip(),
        )
        _rerank_total = round(_rerank_infer_s + _rerank_load_s, 2)
        _emit_progress(args, f"重排完成（{rerank_status}，{_rerank_total}s），共保留 {len(reranked_local_rows)} 条", progress_callback)
    else:
        reranked_local_rows = sorted(rerank_input_rows, key=lambda x: float(x.get("score", 0.0)), reverse=True)[:final_local_top_k]

    timings["rerank_seconds"] = round(_rerank_infer_s, 3)
    timings["reranker_load_seconds"] = round(_rerank_load_s, 3)
    _top1_path_before = _result_identity(local_rows[0]) if local_rows else ""
    _top1_path_after = _result_identity(reranked_local_rows[0]) if reranked_local_rows else ""
    _top1_vector_score_before = float(local_rows[0].get("score", 0.0) or 0.0) if local_rows else 0.0
    _top1_rerank_score_after = 0.0
    _top1_vector_score_after = 0.0
    _top1_final_score_after = 0.0
    if reranked_local_rows:
        _top1_rerank_score_after = float(reranked_local_rows[0].get("rerank_score", reranked_local_rows[0].get("score", 0.0)) or 0.0)
        _top1_vector_score_after = float(reranked_local_rows[0].get("vector_score", reranked_local_rows[0].get("score", 0.0)) or 0.0)
        _top1_final_score_after = float(reranked_local_rows[0].get("final_score", reranked_local_rows[0].get("rerank_score", reranked_local_rows[0].get("score", 0.0))) or 0.0)
    _top1_vector_delta = _top1_vector_score_after - _top1_vector_score_before
    _top1_identity_changed = None
    _top1_rank_shift = None
    if _top1_path_before and _top1_path_after:
        _top1_identity_changed = 1 if _top1_path_before != _top1_path_after else 0
        _before_rank_after_top1 = next((idx + 1 for idx, row in enumerate(local_rows) if _result_identity(row) == _top1_path_after), None)
        if _before_rank_after_top1 is not None:
            _top1_rank_shift = float(_before_rank_after_top1 - 1)
    timings["local_top1_score_after_rerank"] = _top1_rerank_score_after
    timings["local_top1_vector_score_after_rerank"] = _top1_vector_score_after
    timings["local_top1_rerank_score_after_rerank"] = _top1_rerank_score_after
    timings["local_top1_final_score_after_rerank"] = _top1_final_score_after
    timings["local_top1_vector_score_delta_after_rerank"] = _top1_vector_delta
    timings["local_top1_identity_changed"] = _top1_identity_changed
    timings["local_top1_rank_shift"] = _top1_rank_shift
    timings["local_top1_path_before_rerank"] = _top1_path_before
    timings["local_top1_path_after_rerank"] = _top1_path_after
    timings["local_rerank_guard_enabled"] = 1 if rerank_guard_info.get("enabled") else 0
    timings["local_rerank_guard_triggered"] = 1 if rerank_guard_info.get("triggered") else 0
    timings["local_rerank_guard_reason"] = str(rerank_guard_info.get("reason", "") or "")
    timings["local_rerank_guard_vector_drop"] = rerank_guard_info.get("vector_drop")
    timings["local_rerank_guard_vector_drop_ratio"] = rerank_guard_info.get("vector_drop_ratio")
    timings["local_rerank_guard_baseline_gap"] = rerank_guard_info.get("baseline_gap")
    timings["local_rerank_guard_swap_blocked_by_gap"] = 1 if rerank_guard_info.get("swap_blocked_by_gap") else 0
    timings["local_rerank_candidate_count"] = float(rerank_guard_info.get("candidate_count", len(rerank_input_rows)) or 0)
    timings["local_rerank_candidate_profile"] = str(rerank_guard_info.get("candidate_profile", "") or "")
    timings["local_rerank_fusion_alpha"] = float(rerank_guard_info.get("fusion_alpha", getattr(args, "rerank_fusion_alpha", DEFAULT_RERANK_FUSION_ALPHA)) or 0.0)
    timings["local_rerank_fusion_alpha_base"] = float(rerank_guard_info.get("fusion_alpha_base", getattr(args, "rerank_fusion_alpha", DEFAULT_RERANK_FUSION_ALPHA)) or 0.0)
    timings["local_rerank_dynamic_alpha_enabled"] = 1 if rerank_guard_info.get("dynamic_alpha_enabled") else 0
    timings["local_rerank_rerank_soft_top1"] = rerank_guard_info.get("rerank_soft_top1")
    timings["local_rerank_rerank_soft_top2"] = rerank_guard_info.get("rerank_soft_top2")
    timings["local_rerank_rerank_soft_diff"] = rerank_guard_info.get("rerank_soft_diff")
    timings["local_rerank_confidence_factor"] = rerank_guard_info.get("rerank_confidence_factor")
    timings["local_rerank_fusion_alpha_reason"] = str(rerank_guard_info.get("fusion_alpha_reason", "") or "")
    timings["local_rerank_top1_gap_threshold"] = float(rerank_guard_info.get("top1_gap_threshold", getattr(args, "rerank_top1_gap_threshold", DEFAULT_RERANK_TOP1_GAP_THRESHOLD)) or 0.0)
    timings["local_rerank_strong_top1_threshold"] = float(rerank_guard_info.get("strong_top1_threshold", getattr(args, "rerank_strong_top1_threshold", DEFAULT_RERANK_STRONG_TOP1_THRESHOLD)) or 0.0)

    debug_trace: dict[str, Any] = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "trace_id": trace_id,
        "question": question,
        "query": question,
        "rewritten_queries": rewrite_queries,
        "query_rewrite_status": rewrite_status,
        "graph_expansion_batches": expanded_batches,
        "session_id": str(getattr(args, "session_id", "") or "").strip(),
        "search_mode": search_mode,
        "similarity_threshold": local_threshold,
        "vector_top_n": vector_top_n,
        "max_vector_candidates": max_vector_candidates,
        "rerank_top_k": final_local_top_k,
        "rerank_fusion_alpha": timings["local_rerank_fusion_alpha"],
        "enable_rerank": enable_rerank,
        "reranker_model": str(args.reranker_model or DEFAULT_RERANKER_MODEL).strip(),
        "vector_query_batches": vector_query_batches,
        "vector_candidates": local_rows,
        "threshold_candidates": threshold_rows,
        "reranked_local": reranked_local_rows,
        "rerank_guard": rerank_guard_info,
    }

    timings["rerank_status"] = rerank_status
    timings["vector_top_n"] = float(vector_top_n)
    timings["max_vector_candidates"] = float(max_vector_candidates)
    timings["rerank_top_k"] = float(final_local_top_k)
    timings["local_before_threshold"] = float(len(local_rows))
    timings["local_after_threshold"] = float(len(threshold_rows))
    timings["local_after_rerank"] = float(len(reranked_local_rows))
    timings["local_top1_score"] = max((float(r.get("score", 0.0)) for r in local_rows), default=0.0)

    web_t0 = time.perf_counter()
    web_results: list[dict[str, Any]] = []
    web_status = "disabled"
    if web_top_k > 0:
        message = "正在读取本地 Web 缓存..." if not allow_web_network else "正在联网补充相关资料..."
        _emit_progress(args, message, progress_callback)
        tavily_key = os.getenv("TAVILY_API_KEY", "").strip() or (TAVILY_API_KEY or "").strip()
        web_results, web_status = _search_web_tavily(
            query=question,
            max_results=web_top_k,
            tavily_api_key=tavily_key,
            allow_network=allow_web_network,
        )
    hybrid_rows: list[dict[str, Any]] = []
    for item in reranked_local_rows:
        if not isinstance(item, dict):
            continue
        row = dict(item)
        row["source"] = "local"
        hybrid_rows.append(row)
    for item in web_results:
        if not isinstance(item, dict):
            continue
        row = dict(item)
        row["source"] = "web"
        hybrid_rows.append(row)

    _emit_progress(args, "正在装载上下文片段...", progress_callback)

    # Build a passage-level reranker callable reusing the already-warm model
    # from the doc-level rerank step above (zero cold-start cost).
    _rk_model = str(args.reranker_model or DEFAULT_RERANKER_MODEL).strip()
    _rk_sidecar = str(getattr(args, "reranker_server_url", "") or "").strip()

    def _passage_reranker_fn(pairs: list[tuple[str, str]]) -> list[float]:
        if _rk_sidecar:
            try:
                from urllib import request as _urlreq  # noqa: PLC0415
                body = json.dumps({"model": _rk_model, "pairs": pairs}, ensure_ascii=False).encode()
                req = _urlreq.Request(
                    _rk_sidecar.rstrip("/"), data=body,
                    headers={"Content-Type": "application/json"}, method="POST",
                )
                opener = _urlreq.build_opener(_urlreq.ProxyHandler({}))
                with opener.open(req, timeout=10) as resp:
                    return [float(s) for s in json.loads(resp.read().decode())["scores"]]
            except Exception:
                pass
        _rk = _RERANKER_CACHE.get(_rk_model)
        if _rk is None:
            return []
        try:
            return [float(s) for s in _rk.predict(pairs)]
        except Exception:
            return []

    context_t0 = time.perf_counter()
    context_text, used_docs = _load_context_hybrid(
        rows=hybrid_rows,
        documents_dir=Path(args.documents_dir),
        max_context_chars=max_context_chars,
        max_context_docs=max_context_docs,
        max_chars_per_doc=max(500, int(args.max_chars_per_doc)),
        similarity_threshold=local_threshold,
        context_mode=str(args.context_mode or "passage_first"),
        passage_first_k=DEFAULT_PASSAGE_FIRST_K,
        query=question,
        reranker_fn=_passage_reranker_fn if enable_rerank else None,
    )
    timings["context_assembly_seconds"] = round(time.perf_counter() - context_t0, 6)
    timings["web_search_seconds"] = round(time.perf_counter() - web_t0, 3)
    timings["web_search_status"] = web_status
    debug_trace["web_results"] = web_results
    debug_trace["used_context_docs"] = used_docs
    debug_trace["context_tokens_est"] = _approx_tokens(context_text)
    debug_trace["max_context_docs"] = max_context_docs

    filtered_count = len([r for r in reranked_local_rows if float(r.get("vector_score", r.get("score", 0.0)) or 0.0) >= local_threshold])
    _emit_progress(
        args,
        f"本地候选 {len(results)} -> 重排保留 {len(reranked_local_rows)}（阈值后可用 {filtered_count}），联网补充 {len(web_results)} 个结果，上下文已加载（{len(context_text)} 字符），正在请求模型生成回答...",
        progress_callback,
    )

    # Multi-signal retrieval confidence — uses definitive final_score as sole source
    _top2_final_score_after = float(
        reranked_local_rows[1].get("final_score", reranked_local_rows[1].get("rerank_score", reranked_local_rows[1].get("score", 0.0))) or 0.0
    ) if len(reranked_local_rows) >= 2 else 0.0
    _retrieval_confidence = _compute_retrieval_confidence(
        top1_final=_top1_final_score_after,
        top2_final=_top2_final_score_after,
        candidate_count=len(reranked_local_rows),
    )
    timings["retrieval_confidence"] = _retrieval_confidence

    llm_stats: dict[str, Any] = {}
    llm_t0 = time.perf_counter()
    try:
        answer = _ask_llm(
            api_url=args.api_url,
            api_key=args.api_key,
            model=args.model,
            timeout=int(args.timeout),
            question=question,
            context_text=context_text,
            call_type=str(args.call_type or "answer"),
            memory_context=str(args.memory_context or ""),
            stream=args.stream,
            stream_callback=stream_callback,
            trace_id=trace_id,
            debug_trace=debug_trace if debug_enabled else None,
            llm_stats_sink=llm_stats,
            retrieval_confidence=_retrieval_confidence,
        )
    except Exception as exc:  # noqa: BLE001
        if args.allow_local_fallback and _is_local_llm_unavailable_error(str(exc)):
            answer = _build_local_fallback_answer(question=question, used_docs=used_docs)
            llm_stats["output_tokens_est"] = _approx_tokens(answer)
            _emit_stream_chunk(args, answer, stream_callback)
        else:
            raise
    llm_elapsed = round(time.perf_counter() - llm_t0, 6)

    # Citation contract: structurally bind answer claims to source passages.
    # Strips phantom refs, auto-annotates strongly-matched uncited paragraphs,
    # and attaches a structured citation_map to the payload for downstream use.
    if context_text.strip():
        answer, citation_map = _reconcile_citations(answer, used_docs, context_text)
    else:
        citation_map: dict[str, Any] = {
            "cited_by_model": [],
            "auto_annotated": [],
            "phantom_refs": [],
            "uncited_docs": [],
            "doc_to_claims": {},
        }

    session_title = _fallback_session_title(question)

    payload = {
        "trace_id": trace_id,
        "call_type": str(args.call_type or "answer"),
        "question": question,
        "query": question,
        "session_title": session_title,
        "answer": answer,
        "retrieved_count": len(used_docs),
        "retrieved_local_count": len(reranked_local_rows),
        "retrieved_local_vector_candidates": len(results),
        "retrieved_web_count": len(web_results),
        "search_mode": search_mode,
        "vector_top_n": vector_top_n,
        "max_vector_candidates": max_vector_candidates,
        "enable_rerank": enable_rerank,
        "rerank_top_k": final_local_top_k,
        "reranker_model": str(args.reranker_model or DEFAULT_RERANKER_MODEL).strip(),
        "graph_expansion_batches": expanded_batches,
        "used_context_docs": used_docs,
        "max_context_docs": max_context_docs,
        "citation_map": citation_map,
        "timings": timings,
        "llm": {
            "model": str(args.model or "").strip(),
            "latency_seconds": llm_elapsed,
            "input_tokens_est": int(llm_stats.get("input_tokens_est", 0) or 0),
            "prompt_tokens_est": int(llm_stats.get("prompt_tokens_est", 0) or 0),
            "context_tokens_est": int(llm_stats.get("context_tokens_est", 0) or 0),
            "output_tokens_est": int(llm_stats.get("output_tokens_est", 0) or 0),
            "calls": int(llm_stats.get("calls", 0) or 0),
        },
        "elapsed_seconds": round(time.perf_counter() - t0, 3),
    }
    if debug_enabled:
        debug_trace["timings"] = timings
        payload["debug_trace"] = debug_trace
    return payload


def main() -> None:
    _configure_stdio_utf8()
    args = _parse_args()

    question = (args.question or "").strip()
    if not question:
        question = sys.stdin.read().strip()
    if not question:
        raise RuntimeError("Question is empty")
    args.question = question

    payload = run_rag_query(args)

    if args.output_json:
        out = Path(args.output_json)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(str(payload.get("answer", "")))


if __name__ == "__main__":
    try:
        main()
    except (RAGIndexError, RuntimeError) as exc:
        print(f"ERROR: {exc}")
        raise SystemExit(2) from exc
