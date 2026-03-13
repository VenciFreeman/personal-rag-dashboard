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
    ):
        try:
            from openai import OpenAI
        except ModuleNotFoundError as exc:
            raise RuntimeError("Missing dependency: openai. Install with: pip install openai") from exc

        client = OpenAI(api_key=api_key, base_url=base_url, timeout=timeout)
        response_stream = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature,
            stream=True,
        )
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
DEFAULT_VECTOR_TOP_N = int(os.getenv("AI_SUMMARY_VECTOR_TOP_N", "10"))
DEFAULT_RERANK_TOP_K = int(os.getenv("AI_SUMMARY_RERANK_TOP_K", "5"))
DEFAULT_ENABLE_RERANK = os.getenv("AI_SUMMARY_ENABLE_RERANK", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
DEFAULT_RERANKER_MODEL = os.getenv("AI_SUMMARY_RERANKER_MODEL", "BAAI/bge-reranker-base").strip() or "BAAI/bge-reranker-base"
DEFAULT_ENABLE_QUERY_REWRITE = os.getenv("AI_SUMMARY_ENABLE_QUERY_REWRITE", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
DEFAULT_QUERY_REWRITE_COUNT = max(1, min(2, int(os.getenv("AI_SUMMARY_QUERY_REWRITE_COUNT", "2"))))
DEFAULT_PRIMARY_QUERY_SCORE_BONUS = float(os.getenv("AI_SUMMARY_PRIMARY_QUERY_SCORE_BONUS", "0.03") or "0.03")
BUILD_INDEX_ON_DEMAND = os.getenv("AI_SUMMARY_RAG_BUILD_INDEX_ON_DEMAND", "0").strip().lower() in {"1", "true", "yes", "on"}
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


def _parse_args() -> argparse.Namespace:
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
    parser.add_argument("--similarity-threshold", type=float, default=0.5, help="Minimum similarity score to include document (0.0-1.0)")
    parser.add_argument("--max-context-chars", type=int, default=20000, help="Max total context chars")
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
        default="topic",
        choices=["topic", "full"],
        help="Context assembly mode: topic=metadata-only (default), full=load markdown body",
    )
    parser.add_argument("--output-json", default="", help="Optional JSON output file path")
    parser.add_argument("--debug", default="false", help="Enable debug trace output in payload")
    parser.add_argument("--session-id", default="", help="Session id for debug trace correlation")
    parser.add_argument("--enable-query-rewrite", default="true" if DEFAULT_ENABLE_QUERY_REWRITE else "false", help="Enable local-LLM query rewrite before retrieval")
    parser.add_argument("--query-rewrite-count", type=int, default=DEFAULT_QUERY_REWRITE_COUNT, help="How many rewritten queries to keep")
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
    return parser.parse_args()


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
    local_model = os.getenv("AI_SUMMARY_LOCAL_LLM_MODEL", "qwen2.5-7b-instruct").strip() or "qwen2.5-7b-instruct"
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


def _rerank_local_rows(
    *,
    query: str,
    rows: list[dict[str, Any]],
    documents_dir: Path,
    top_k: int,
    reranker_model: str,
    embedding_model: str,
    reranker_server_url: str = "",
) -> tuple[list[dict[str, Any]], str, float, float]:
    """Returns (rows, status, infer_seconds, load_seconds).

    infer_seconds = time spent only inside reranker.predict() / sidecar call
    load_seconds  = time cold-loading the model (0 if sidecar is used or cache hit)
    """
    if not rows:
        return [], "empty_input", 0.0, 0.0

    # ─ Path A: delegate to the in-process reranker sidecar (zero cold-load) ─
    if reranker_server_url:
        try:
            from urllib import request as _urlreq  # noqa: PLC0415

            pairs: list[tuple[str, str]] = [
                (query, _build_rerank_text(row, documents_dir)) for row in rows
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
            for row, score in zip(rows, scores):
                item = dict(row)
                item["vector_score"] = float(item.get("score", 0.0))
                item["rerank_score"] = float(score)
                item["score"] = float(score)
                ranked_rows.append(item)
            ranked_rows.sort(key=lambda x: float(x.get("rerank_score", 0.0)), reverse=True)
            return ranked_rows[: max(1, int(top_k))], "sidecar", _infer_elapsed, 0.0
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
        for row in rows:
            doc_text = _build_rerank_text(row, documents_dir)
            pairs.append((query, doc_text))

        _infer_t0 = time.perf_counter()
        scores = reranker.predict(pairs)
        _infer_elapsed = time.perf_counter() - _infer_t0

        ranked_rows = []
        for row, score in zip(rows, scores):
            item = dict(row)
            item["vector_score"] = float(item.get("score", 0.0))
            item["rerank_score"] = float(score)
            item["score"] = float(score)
            ranked_rows.append(item)

        ranked_rows.sort(key=lambda x: float(x.get("rerank_score", 0.0)), reverse=True)
        return ranked_rows[: max(1, int(top_k))], "ok", _infer_elapsed, _load_elapsed
    except Exception as cross_exc:  # noqa: BLE001
        try:
            normalized_model = _normalize_embedding_model_value(embedding_model)
            texts = [_build_rerank_text(row, documents_dir) for row in rows]
            vectors = _embed_texts_local([query] + texts, model=normalized_model, batch_size=32)
            query_vec = vectors[0]
            doc_vecs = vectors[1:]

            def _norm(vec: list[float]) -> float:
                return sum(x * x for x in vec) ** 0.5

            qn = _norm(query_vec) or 1e-12
            ranked_rows: list[dict[str, Any]] = []
            for row, vec in zip(rows, doc_vecs):
                dn = _norm(vec) or 1e-12
                score = sum(a * b for a, b in zip(query_vec, vec)) / (qn * dn)
                item = dict(row)
                item["vector_score"] = float(item.get("score", 0.0))
                item["rerank_score"] = float(score)
                item["score"] = float(score)
                ranked_rows.append(item)

            ranked_rows.sort(key=lambda x: float(x.get("rerank_score", 0.0)), reverse=True)
            return ranked_rows[: max(1, int(top_k))], f"fallback_embedding:{cross_exc}", 0.0, 0.0
        except Exception as embed_exc:  # noqa: BLE001
            fallback = sorted(rows, key=lambda x: float(x.get("score", 0.0)), reverse=True)
            return fallback[: max(1, int(top_k))], f"failed:{cross_exc}; fallback_failed:{embed_exc}", 0.0, 0.0


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


def _build_topic_context_segment(item: dict[str, Any], index_no: int) -> tuple[str, dict[str, Any]]:
    rel = str(item.get("relative_path", "")).strip() or str(item.get("file_path", "")).strip() or "<unknown>"
    title = str(item.get("title", "")).strip()
    summary = str(item.get("summary", "")).strip()
    topic = str(item.get("topic", "")).strip() or "untitled-topic"
    score = float(item.get("score", 0.0))
    keywords = _normalize_keywords(item.get("keywords", []))
    if not summary:
        summary = topic

    kw_text = ", ".join(keywords[:12]) if keywords else ""
    body_lines = [
        f"[资料{index_no}] path={rel} score={score:.4f}",
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
    return "\n".join(body_lines), used_doc


def _load_context(
    *,
    results: list[dict[str, Any]],
    documents_dir: Path,
    max_context_chars: int,
    max_chars_per_doc: int,
    similarity_threshold: float = 0.0,
    context_mode: str = "topic",
) -> tuple[str, list[dict[str, Any]]]:
    """Load and build context from search results, filtering by similarity threshold.
    
    Args:
        results: Search results with score field
        documents_dir: Directory containing documents
        max_context_chars: Maximum total context characters
        max_chars_per_doc: Maximum characters per document
        similarity_threshold: Minimum similarity score (0.0-1.0). Docs below this are filtered.
    
    Returns:
        (context_text, used_docs) tuple
    """
    # Filter results by similarity threshold first.
    filtered_results = [
        item for item in results
        if float(item.get("score", 0.0)) >= similarity_threshold
    ]
    
    context_parts: list[str] = []
    used_docs: list[dict[str, Any]] = []
    total_chars = 0

    mode = (context_mode or "topic").strip().lower()

    for i, item in enumerate(filtered_results, start=1):
        if mode == "topic":
            segment, used_doc = _build_topic_context_segment(item, i)
            segment = segment + "\n"
        else:
            file_path = _resolve_result_path(item, documents_dir)
            if file_path is None or not file_path.is_file():
                continue

            try:
                raw_text = file_path.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue

            clipped_text = raw_text[:max_chars_per_doc]
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
) -> tuple[str, list[dict[str, Any]]]:
    if not web_results:
        return context_text, used_docs

    parts: list[str] = []
    total_chars = len(context_text)
    next_index = len(used_docs) + 1
    docs = list(used_docs)

    for item in web_results:
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
    max_chars_per_doc: int,
    similarity_threshold: float = 0.0,
    context_mode: str = "topic",
) -> tuple[str, list[dict[str, Any]]]:
    """Build context from mixed local/web rows sorted by score (desc)."""
    sorted_rows = sorted(rows, key=lambda x: float(x.get("score", 0.0)), reverse=True)
    mode = (context_mode or "topic").strip().lower()

    context_parts: list[str] = []
    used_docs: list[dict[str, Any]] = []
    total_chars = 0
    idx = 1

    for item in sorted_rows:
        source = str(item.get("source", "local")).strip().lower()
        score = float(item.get("score", 0.0))

        # Keep existing threshold behavior for local vector hits only.
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
            if mode == "topic":
                segment, used_doc = _build_topic_context_segment(item, idx)
                segment = segment + "\n"
            else:
                file_path = _resolve_result_path(item, documents_dir)
                if file_path is None or not file_path.is_file():
                    continue
                try:
                    raw_text = file_path.read_text(encoding="utf-8", errors="ignore")
                except Exception:
                    continue
                clipped_text = raw_text[:max_chars_per_doc]
                rel = str(item.get("relative_path", "")).strip() or file_path.name
                topic = str(item.get("topic", "")).strip()
                segment = (
                    f"[资料{idx}] path={rel} score={score:.4f} topic={topic}\n"
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
    debug_trace: dict[str, Any] | None = None,
) -> str:
    if not api_url or not api_key or not model:
        raise RuntimeError("Missing API settings: api_url/api_key/model are required")

    system_prompt = (
        "你是知识助手。优先使用检索资料，同时可以结合通用知识补充，并明确标注不确定点。\n"
        f"当前调用类型 call_type={call_type or 'answer'}。\n\n"
        "**输出格式要求（必须严格遵守）**：\n"
        "1) 必须使用标准Markdown格式输出；\n"
        "2) 请用中文思考和回答；\n"
        #"3) 列表项前后必须空行（- * 1. 2.）；\n"
        #"4) 段落之间必须用空行分隔；\n"
        #"5) 代码块使用```包裹，前后空行；\n"
        #"6) 分割线使用三个横杠（---）单独成行，前后空行。"
    )
    
    # Build user prompt based on whether we have context or not.
    memory_block = (memory_context or "").strip()
    memory_section = f"\n会话记忆(可为空):\n{memory_block}\n" if memory_block else ""

    if context_text.strip():
        user_prompt = (
            "请先阅读下面的本地检索资料，再回答用户问题。\n"
            "回答要求：\n"
            "1) 请进行简短思考（不超过400字），用<think>思考内容</think>标签包裹；\n"
            "2) 基于资料和会话记忆 (如果有提供)给出最终答案；\n"
            "3) 资料不足处可用通用知识补充，但要标注'推断/可能/不确定'；\n"
            "4) 不要编造不存在于资料或常识中的细节；\n"
            "5) 禁止在输出中列出参考资料。\n\n"
            f"{memory_section}"
            f"资料:\n{context_text}\n\n"
            f"问题:\n{question}\n"
        )
    else:
        user_prompt = (
            "本地知识库中未找到与问题高度相关的资料（相似度过低）。\n"
            "请基于你的通用知识尝试回答以下问题，但请明确标注这是基于通用知识的推断，而非本地资料。\n"
            "回答要求：\n"
            "1) 明确说明未找到相关本地资料；\n"
            "2) 基于通用知识给出可能的答案，标注'基于通用知识推断'；\n"
            "3) 不要编造不存在的细节。\n\n"
            f"{memory_section}"
            f"问题:\n{question}\n"
        )


    request_id = str(uuid4())
    should_audit = _is_deepseek_url(api_url)
    request_messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]
    if debug_trace is not None:
        debug_trace["llm_request"] = {
            "api_url": api_url,
            "model": model,
            "messages": request_messages,
            "memory_tokens_est": _approx_tokens(memory_context),
            "input_tokens_est": _approx_tokens(system_prompt) + _approx_tokens(user_prompt),
        }

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
                        "response_text": "",
                        "error": str(exc),
                    }
                )
            raise RuntimeError(str(exc)) from exc
        
        answer_chunks = []
        try:
            for content in response_stream:
                answer_chunks.append(content)
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
                        "response_text": "".join(answer_chunks),
                        "error": str(exc),
                    }
                )
            raise RuntimeError(str(exc)) from exc
        
        answer = "".join(answer_chunks).strip()
        if not answer:
            raise RuntimeError("LLM response text is empty")
        if debug_trace is not None:
            debug_trace["llm_response"] = {
                "output_tokens_est": _approx_tokens(answer),
            }
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
                        "response_text": "",
                        "error": str(exc),
                    }
                )
            raise RuntimeError(str(exc)) from exc
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
                    "response_text": answer,
                    "error": "",
                }
            )
        if debug_trace is not None:
            debug_trace["llm_response"] = {
                "output_tokens_est": _approx_tokens(answer),
            }
        return answer


def _fallback_session_title(question: str, max_len: int = 16) -> str:
    normalized = " ".join((question or "").split())
    if not normalized:
        return "未命名会话"
    return normalized[:max_len]


def _generate_session_title(question: str, answer: str, api_key: str, api_url: str, model: str, timeout: int) -> str:
    """Use LLM to generate a concise 15-character session title."""

    title_prompt = (
        f"请根据以下问答生成一个15字以内的简短标题，直接输出标题即可，不要加引号或其他说明。\n\n"
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
        if title and len(title) <= 30:
            return title[:15]
    except Exception:
        pass
    
    return _fallback_session_title(question, 15)


def main() -> None:
    _configure_stdio_utf8()
    args = _parse_args()

    # Propagate embedding sidecar URL to rag_vector_index via env var.
    _embed_server_url = str(getattr(args, "embed_server_url", "") or "").strip()
    if _embed_server_url:
        os.environ["_RAG_EMBED_SIDECAR_URL"] = _embed_server_url

    question = (args.question or "").strip()
    if not question:
        question = sys.stdin.read().strip()
    if not question:
        raise RuntimeError("Question is empty")

    t0 = time.perf_counter()
    
    # Output progress for GUI streaming mode.
    if args.stream:
        print("PROGRESS: 正在检查索引并准备检索...", flush=True)
    
    search_mode = (args.search_mode or "hybrid").strip().lower()
    debug_enabled = _parse_bool_arg(getattr(args, "debug", False), default=False)
    enable_rerank = _parse_bool_arg(args.enable_rerank, default=DEFAULT_ENABLE_RERANK)
    enable_query_rewrite = _parse_bool_arg(getattr(args, "enable_query_rewrite", False), default=DEFAULT_ENABLE_QUERY_REWRITE)
    rewrite_count = max(1, int(getattr(args, "query_rewrite_count", DEFAULT_QUERY_REWRITE_COUNT)))
    vector_top_n = max(1, int(args.vector_top_n))
    rerank_top_k = max(1, int(args.rerank_top_k))
    if search_mode == "local_only":
        final_local_top_k = max(1, int(args.top_k))
        web_top_k = 0
    else:
        final_local_top_k = rerank_top_k
        web_top_k = 3
    max_context_chars = max(1000, int(args.max_context_chars))

    # Pre-warm the embedding model so cold-start cost is paid before any timed section.
    # _LOCAL_EMBED_MODEL_CACHE in rag_vector_index keeps the encoder alive for this process.
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
        if args.stream:
            print("PROGRESS: 正在改写查询并扩展检索词...", flush=True)
        rewrite_queries, rewrite_status = _rewrite_queries_with_local_llm(
            question=question,
            memory_context=str(args.memory_context or ""),
            rewrite_count=rewrite_count,
            timeout=int(args.timeout),
        )
    elif enable_query_rewrite:
        # Query is short and simple — rewrite would add no value.
        rewrite_status = "skipped:simple_query"
    rewrite_elapsed = round(time.perf_counter() - rewrite_t0, 3)
    if args.stream:
        print("PROGRESS: 正在进行图谱扩展并准备向量召回...", flush=True)

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

    # Lazy-build the knowledge graph from index metadata.
    # Requires metadata.json to already exist (written by the vector index build below).
    # Triggers if: graph file missing, or graph is essentially empty (< 200 bytes means no nodes).
    _graph_file = Path(args.index_dir) / "knowledge_graph_rag.json"
    _meta_file = Path(args.index_dir) / "metadata.json"
    _graph_needs_build = _meta_file.exists() and (
        not _graph_file.exists() or _graph_file.stat().st_size < 200
    )
    if _graph_needs_build:
        if args.stream:
            print("PROGRESS: 首次构建知识图谱索引（只需一次）...", flush=True)
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

    if args.stream:
        print(f"PROGRESS: 正在进行向量召回（检索批次 {len(retrieval_queries)}）...", flush=True)

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
        # Propagate embed cache hit flag: 1.0 if ANY batch query hit the embedding cache
        timings["embed_cache_hit"] = max(
            float(timings.get("embed_cache_hit", 0.0)),
            float(one_timing.get("embed_cache_hit", 0.0) or 0.0),
        )

    results, vector_query_batches = _merge_multi_query_vector_rows(per_query_rows, primary_query=question)

    local_threshold = float(args.similarity_threshold)
    local_rows = [dict(item) for item in results if isinstance(item, dict)]
    threshold_rows = [r for r in local_rows if float(r.get("score", 0.0)) >= local_threshold]
    rerank_input_rows = threshold_rows or local_rows
    rerank_status = "disabled"
    _rerank_infer_s = 0.0
    _rerank_load_s = 0.0

    if enable_rerank:
        if args.stream:
            print(f"PROGRESS: 已向量召回 {len(results)} 个候选，正在重排本地结果...", flush=True)
        reranked_local_rows, rerank_status, _rerank_infer_s, _rerank_load_s = _rerank_local_rows(
            query=question,
            rows=rerank_input_rows,
            documents_dir=Path(args.documents_dir),
            top_k=final_local_top_k,
            reranker_model=str(args.reranker_model or DEFAULT_RERANKER_MODEL).strip(),
            embedding_model=str(args.embedding_model or EMBEDDING_MODEL or "BAAI/bge-base-zh-v1.5").strip(),
            reranker_server_url=str(getattr(args, "reranker_server_url", "") or "").strip(),
        )
        if args.stream:
            _rerank_total = round(_rerank_infer_s + _rerank_load_s, 2)
            print(f"PROGRESS: 重排完成（{rerank_status}，{_rerank_total}s），共保留 {len(reranked_local_rows)} 条", flush=True)
    else:
        reranked_local_rows = sorted(rerank_input_rows, key=lambda x: float(x.get("score", 0.0)), reverse=True)[
            :final_local_top_k
        ]
    # rerank_seconds = pure reranker.predict() inference time (excludes model cold-load)
    # reranker_load_seconds = model load from disk (0 when cached in warm process; non-zero for subprocess cold-start)
    timings["rerank_seconds"] = round(_rerank_infer_s, 3)
    timings["reranker_load_seconds"] = round(_rerank_load_s, 3)
    _top1_path_before = str(local_rows[0].get("path", "")).strip() if local_rows else ""
    _top1_path_after = str(reranked_local_rows[0].get("path", "")).strip() if reranked_local_rows else ""
    _top1_identity_changed = None
    _top1_rank_shift = None
    if _top1_path_before and _top1_path_after:
        _top1_identity_changed = 1 if _top1_path_before != _top1_path_after else 0
        _before_rank_after_top1 = next(
            (idx + 1 for idx, row in enumerate(local_rows) if str(row.get("path", "")).strip() == _top1_path_after),
            None,
        )
        if _before_rank_after_top1 is not None:
            # Positive means moved up in ranking (e.g. 7 -> 2 gives +5).
            _top1_rank_shift = float(_before_rank_after_top1 - 1)
    # Record top1 score after reranking (cross-encoder score for neural rerank; cosine sim for fallback)
    timings["local_top1_score_after_rerank"] = max(
        (float(r.get("score", 0.0)) for r in reranked_local_rows), default=0.0
    )
    timings["local_top1_identity_changed"] = _top1_identity_changed
    timings["local_top1_rank_shift"] = _top1_rank_shift
    timings["local_top1_path_before_rerank"] = _top1_path_before
    timings["local_top1_path_after_rerank"] = _top1_path_after

    debug_trace: dict[str, Any] = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "question": question,
        "query": question,
        "rewritten_queries": rewrite_queries,
        "query_rewrite_status": rewrite_status,
        "graph_expansion_batches": expanded_batches,
        "session_id": str(getattr(args, "session_id", "") or "").strip(),
        "search_mode": search_mode,
        "similarity_threshold": local_threshold,
        "vector_top_n": vector_top_n,
        "rerank_top_k": final_local_top_k,
        "enable_rerank": enable_rerank,
        "reranker_model": str(args.reranker_model or DEFAULT_RERANKER_MODEL).strip(),
        "vector_query_batches": vector_query_batches,
        "vector_candidates": local_rows,
        "threshold_candidates": threshold_rows,
        "reranked_local": reranked_local_rows,
    }

    timings["rerank_status"] = rerank_status
    timings["vector_top_n"] = float(vector_top_n)
    timings["rerank_top_k"] = float(final_local_top_k)
    timings["local_before_threshold"] = float(len(local_rows))
    timings["local_after_threshold"] = float(len(threshold_rows))
    timings["local_after_rerank"] = float(len(reranked_local_rows))
    timings["local_top1_score"] = max((float(r.get("score", 0.0)) for r in local_rows), default=0.0)

    web_t0 = time.perf_counter()
    web_results: list[dict[str, Any]] = []
    web_status = "disabled"
    if web_top_k > 0:
        if args.stream:
            print("PROGRESS: 正在联网补充相关资料...", flush=True)
        tavily_key = os.getenv("TAVILY_API_KEY", "").strip() or (TAVILY_API_KEY or "").strip()
        web_results, web_status = _search_web_tavily(
            query=question,
            max_results=web_top_k,
            tavily_api_key=tavily_key,
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

    if args.stream:
        print("PROGRESS: 正在装载上下文片段...", flush=True)

    context_t0 = time.perf_counter()
    context_text, used_docs = _load_context_hybrid(
        rows=hybrid_rows,
        documents_dir=Path(args.documents_dir),
        max_context_chars=max_context_chars,
        max_chars_per_doc=max(500, int(args.max_chars_per_doc)),
        similarity_threshold=local_threshold,
        context_mode=str(args.context_mode or "topic"),
    )
    timings["context_assembly_seconds"] = round(time.perf_counter() - context_t0, 6)
    timings["web_search_seconds"] = round(time.perf_counter() - web_t0, 3)
    timings["web_search_status"] = web_status
    debug_trace["web_results"] = web_results
    debug_trace["used_context_docs"] = used_docs
    debug_trace["context_tokens_est"] = _approx_tokens(context_text)
    
    if args.stream:
        filtered_count = len([r for r in reranked_local_rows if float(r.get("score", 0.0)) >= local_threshold])
        print(
            f"PROGRESS: 本地候选 {len(results)} -> 重排保留 {len(reranked_local_rows)}（阈值后可用 {filtered_count}），联网补充 {len(web_results)} 个结果，上下文已加载（{len(context_text)} 字符），正在请求模型生成回答...",
            flush=True,
        )
    
    # Exactly one external/local LLM call when available.
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
            debug_trace=debug_trace if debug_enabled else None,
        )
    except Exception as exc:  # noqa: BLE001
        if args.allow_local_fallback and _is_local_llm_unavailable_error(str(exc)):
            answer = _build_local_fallback_answer(question=question, used_docs=used_docs)
            if args.stream:
                print(f"STREAM_CHUNK_JSON: {json.dumps(answer, ensure_ascii=False)}", flush=True)
        else:
            raise

    # Session title is always local (no external API call).
    session_title = _fallback_session_title(question)

    payload = {
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
        "enable_rerank": enable_rerank,
        "rerank_top_k": final_local_top_k,
        "reranker_model": str(args.reranker_model or DEFAULT_RERANKER_MODEL).strip(),
        "graph_expansion_batches": expanded_batches,
        "used_context_docs": used_docs,
        "timings": timings,
        "elapsed_seconds": round(time.perf_counter() - t0, 3),
    }
    if debug_enabled:
        debug_trace["timings"] = timings
        payload["debug_trace"] = debug_trace

    if args.output_json:
        out = Path(args.output_json)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(answer)


if __name__ == "__main__":
    try:
        main()
    except (RAGIndexError, RuntimeError) as exc:
        print(f"ERROR: {exc}")
        raise SystemExit(2) from exc
