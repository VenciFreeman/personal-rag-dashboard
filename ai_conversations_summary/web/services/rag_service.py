from __future__ import annotations

import argparse
import importlib.util
import json
import os
import queue
import re
import socket
import sys
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import urlparse
from urllib.parse import quote
from uuid import uuid4

WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))
SCRIPTS_DIR = WORKSPACE_ROOT / "ai_conversations_summary" / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import ask_rag as ask_rag_script

from web.config import DOCUMENTS_DIR, RAG_SESSIONS_DIR, VECTOR_DB_DIR
from web.services.context import (
    DEFAULT_API_BASE_URL,
    DEFAULT_API_KEY,
    DEFAULT_MODEL,
    DEFAULT_TIMEOUT,
    DEFAULT_TAVILY_API_KEY,
    RAGIndexError,
    search_vector_index_with_diagnostics,
)
from rag_knowledge_graph import expand_query_by_graph
from web.services.preview_service import resolve_embedding_model
from core_service.config import get_settings
from core_service.trace_store import write_trace_record

_LOCK = threading.Lock()
_PROCESS_LOCK = threading.Lock()
_ACTIVE_PROCESSES: dict[str, dict[str, Any]] = {}
_ABORTED_SESSIONS: set[str] = set()
_MEMORY_QUEUE: queue.Queue[dict[str, str]] = queue.Queue()
_MEMORY_WORKER_STARTED = False
_CORE_SETTINGS = get_settings()

SESSION_FILE_PREFIX = "session_"
MEMORY_DIR = RAG_SESSIONS_DIR / "_memory"
DEBUG_DIR = RAG_SESSIONS_DIR / "debug_data"
MEMORY_MAX_TOKENS = 1000

# Optional local OpenAI-compatible LLM endpoint (for example LM Studio server).
_local_llm_url_raw = os.getenv("AI_SUMMARY_LOCAL_LLM_URL", "http://127.0.0.1:1234").strip()
if _local_llm_url_raw and not re.search(r"/v1/?$", _local_llm_url_raw):
    LOCAL_LLM_API_URL = _local_llm_url_raw.rstrip("/") + "/v1"
else:
    LOCAL_LLM_API_URL = _local_llm_url_raw

LOCAL_LLM_MODEL = os.getenv("AI_SUMMARY_LOCAL_LLM_MODEL", "").strip() or _CORE_SETTINGS.local_llm_model
LOCAL_LLM_API_KEY = os.getenv("AI_SUMMARY_LOCAL_LLM_API_KEY", "local").strip() or "local"
LOCAL_LLM_MAX_CONTEXT_CHARS = int(os.getenv("AI_SUMMARY_LOCAL_LLM_MAX_CONTEXT_CHARS", "2600"))
LOCAL_LLM_MAX_CHARS_PER_DOC = int(os.getenv("AI_SUMMARY_LOCAL_LLM_MAX_CHARS_PER_DOC", "700"))
ALLOW_REMOTE_LOCAL_LLM = os.getenv("AI_SUMMARY_LOCAL_LLM_ALLOW_REMOTE", "0").strip() == "1"
HYBRID_LOCAL_TOP_K = 3
HYBRID_WEB_TOP_K = 3
LOCAL_ONLY_TOP_K = 5
VECTOR_TOP_N = int(os.getenv("AI_SUMMARY_VECTOR_TOP_N", "12"))
MAX_VECTOR_CANDIDATES = int(os.getenv("AI_SUMMARY_MAX_VECTOR_CANDIDATES", "12") or "12")
RERANK_TOP_K = int(os.getenv("AI_SUMMARY_RERANK_TOP_K", "5"))
ENABLE_RERANK = os.getenv("AI_SUMMARY_ENABLE_RERANK", "1").strip().lower() not in {"0", "false", "no", "off"}
RERANKER_MODEL = os.getenv("AI_SUMMARY_RERANKER_MODEL", "BAAI/bge-reranker-v2-m3").strip() or "BAAI/bge-reranker-v2-m3"
ASK_RAG_SUBPROCESS_TIMEOUT_SECONDS = int(os.getenv("AI_SUMMARY_ASK_RAG_SUBPROCESS_TIMEOUT_SECONDS", "0") or "0")
BUILD_INDEX_ON_DEMAND = os.getenv("AI_SUMMARY_RAG_BUILD_INDEX_ON_DEMAND", "0").strip().lower() in {"1", "true", "yes", "on"}
MAX_REFERENCE_ITEMS = int(os.getenv("AI_SUMMARY_MAX_REFERENCE_ITEMS", "6") or "6")
LOCAL_ONLY_READ_WEB_CACHE = bool(_CORE_SETTINGS.rag_local_only_read_web_cache)

SHORT_QUERY_MAX_TOKENS = 5
LONG_QUERY_MIN_TOKENS = int(os.getenv("AI_SUMMARY_LONG_QUERY_MIN_TOKENS", "12") or "12")
SHORT_QUERY_THRESHOLD_DELTA = float(os.getenv("AI_SUMMARY_SHORT_QUERY_THRESHOLD_DELTA", "-0.08") or "-0.08")
LONG_QUERY_THRESHOLD_DELTA = float(os.getenv("AI_SUMMARY_LONG_QUERY_THRESHOLD_DELTA", "0.08") or "0.08")
SHORT_QUERY_VECTOR_TOP_N_DELTA = int(os.getenv("AI_SUMMARY_SHORT_QUERY_VECTOR_TOP_N_DELTA", "6") or "6")
LONG_QUERY_VECTOR_TOP_N_DELTA = int(os.getenv("AI_SUMMARY_LONG_QUERY_VECTOR_TOP_N_DELTA", "-4") or "-4")
SHORT_QUERY_TOP_K_DELTA = int(os.getenv("AI_SUMMARY_SHORT_QUERY_TOP_K_DELTA", "2") or "2")
LONG_QUERY_TOP_K_DELTA = int(os.getenv("AI_SUMMARY_LONG_QUERY_TOP_K_DELTA", "-1") or "-1")

RETRIEVAL_METRICS_FILE = RAG_SESSIONS_DIR / "retrieval_metrics.json"
RETRIEVAL_METRICS_MAX = 20


def _new_trace_id() -> str:
    return f"trace_{uuid4().hex[:16]}"


def _normalize_trace_id(trace_id: str = "") -> str:
    value = re.sub(r"[^a-zA-Z0-9_.:-]", "", str(trace_id or "").strip())
    if value:
        return value[:80]
    return _new_trace_id()

# ─── In-process reranker sidecar ──────────────────────────────────────────────
# A minimal HTTP server that wraps the CrossEncoder and stays alive for the
# lifetime of the rag_service process.  ask_rag.py subprocesses pass
# --reranker-server-url to avoid cold-loading the model on every request.

_RERANKER_SIDECAR_PORT: int = 0  # 0 = not yet started
_RERANKER_SIDECAR_LOCK = threading.Lock()
_RERANKER_SIDECAR_CACHE: dict[str, Any] = {}  # model_name -> CrossEncoder

# ─── In-process embedding sidecar ────────────────────────────────────────────
# Same pattern as the reranker: keeps SentenceTransformer warm so each
# ask_rag.py subprocess skips the ~7 s cold-load for the embedding model.
_EMBED_SIDECAR_PORT: int = 0
_EMBED_SIDECAR_LOCK = threading.Lock()
_EMBED_SIDECAR_CACHE: dict[str, Any] = {}  # model_name -> SentenceTransformer


def _get_reranker_sidecar_url() -> str:
    """Start the sidecar if needed and return its base URL."""
    global _RERANKER_SIDECAR_PORT  # noqa: PLW0603
    with _RERANKER_SIDECAR_LOCK:
        if _RERANKER_SIDECAR_PORT > 0:
            return f"http://127.0.0.1:{_RERANKER_SIDECAR_PORT}"
        port = _start_reranker_sidecar()
        _RERANKER_SIDECAR_PORT = port
        return f"http://127.0.0.1:{port}"


def _start_reranker_sidecar() -> int:
    """Bind on a free port, start the HTTP server in a daemon thread, return port."""

    class _Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: Any) -> None:  # silence access log
            pass

        def do_POST(self) -> None:  # noqa: N802
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length).decode("utf-8"))
                model_name = str(body.get("model", RERANKER_MODEL)).strip() or RERANKER_MODEL
                pairs = body.get("pairs", [])

                reranker = _RERANKER_SIDECAR_CACHE.get(model_name)
                if reranker is None:
                    # Import here to avoid hard dep at module load time.
                    from sentence_transformers import CrossEncoder  # noqa: PLC0415
                    reranker = CrossEncoder(model_name)
                    _RERANKER_SIDECAR_CACHE[model_name] = reranker

                t0 = time.perf_counter()
                scores = reranker.predict(pairs)
                infer_s = time.perf_counter() - t0

                resp = json.dumps({
                    "scores": [float(s) for s in scores],
                    "infer_seconds": round(infer_s, 4),
                }).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(resp)))
                self.end_headers()
                self.wfile.write(resp)
            except Exception as exc:  # noqa: BLE001
                err = json.dumps({"error": str(exc)}).encode("utf-8")
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(err)))
                self.end_headers()
                self.wfile.write(err)

    # Bind to OS-assigned free port. Use ThreadingHTTPServer so concurrent
    # rerank requests (e.g. benchmark + live query) are handled in parallel.
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    port = server.server_address[1]

    def _serve() -> None:
        server.serve_forever()

    t = threading.Thread(target=_serve, daemon=True, name="reranker-sidecar")
    t.start()
    return port


def _prewarm_reranker_sidecar() -> None:
    """Send a dummy request to load the CrossEncoder model before the first real query."""
    try:
        import json as _json
        from urllib import request as _urlreq
        url = _get_reranker_sidecar_url()
        body = _json.dumps({"model": RERANKER_MODEL, "pairs": [["warmup", "warmup"]]}).encode("utf-8")
        req = _urlreq.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
        opener = _urlreq.build_opener(_urlreq.ProxyHandler({}))
        with opener.open(req, timeout=180) as resp:
            resp.read()
    except Exception:
        pass  # Best-effort; silently ignored if model not available


if ENABLE_RERANK:
    threading.Thread(target=_prewarm_reranker_sidecar, daemon=True, name="reranker-prewarm").start()


def _get_embed_sidecar_url() -> str:
    """Start the embedding sidecar if needed and return its base URL."""
    global _EMBED_SIDECAR_PORT  # noqa: PLW0603
    with _EMBED_SIDECAR_LOCK:
        if _EMBED_SIDECAR_PORT > 0:
            return f"http://127.0.0.1:{_EMBED_SIDECAR_PORT}"
        port = _start_embed_sidecar()
        _EMBED_SIDECAR_PORT = port
        return f"http://127.0.0.1:{port}"


def _start_embed_sidecar() -> int:
    """Bind on a free port, start the embedding HTTP server in a daemon thread."""

    class _Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: Any) -> None:
            pass

        def do_POST(self) -> None:  # noqa: N802
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length).decode("utf-8"))
                model_name = str(body.get("model", "")).strip()
                texts = body.get("texts", [])
                if not model_name or not isinstance(texts, list):
                    raise ValueError("model and texts required")

                encoder = _EMBED_SIDECAR_CACHE.get(model_name)
                if encoder is None:
                    from sentence_transformers import SentenceTransformer  # noqa: PLC0415

                    os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")
                    os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
                    os.environ.setdefault("HF_HUB_OFFLINE", "1")
                    os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")
                    encoder = SentenceTransformer(model_name, local_files_only=True)
                    _EMBED_SIDECAR_CACHE[model_name] = encoder

                t0 = time.perf_counter()
                vectors = encoder.encode(
                    texts,
                    batch_size=max(1, min(32, len(texts))),
                    convert_to_numpy=True,
                    normalize_embeddings=False,
                    show_progress_bar=False,
                )
                infer_s = time.perf_counter() - t0

                resp = json.dumps({
                    "vectors": vectors.tolist(),
                    "infer_seconds": round(infer_s, 4),
                }).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(resp)))
                self.end_headers()
                self.wfile.write(resp)
            except Exception as exc:  # noqa: BLE001
                err = json.dumps({"error": str(exc)}).encode("utf-8")
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(err)))
                self.end_headers()
                self.wfile.write(err)

    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    port = server.server_address[1]

    def _serve() -> None:
        server.serve_forever()

    t = threading.Thread(target=_serve, daemon=True, name="embed-sidecar")
    t.start()
    return port


def _prewarm_embed_sidecar() -> None:
    """Send a dummy request to load the SentenceTransformer model."""
    try:
        import json as _json
        from urllib import request as _urlreq
        emb_model = resolve_embedding_model()
        url = _get_embed_sidecar_url()
        body = _json.dumps({"model": emb_model, "texts": ["warmup"]}).encode("utf-8")
        req = _urlreq.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
        opener = _urlreq.build_opener(_urlreq.ProxyHandler({}))
        with opener.open(req, timeout=180) as resp:
            resp.read()
    except Exception:
        pass


threading.Thread(target=_prewarm_embed_sidecar, daemon=True, name="embed-prewarm").start()

# ─── Dashboard quota sync (non-blocking best-effort) ──────────────────────────
_NAV_DASHBOARD_QUOTA_URL = os.getenv("NAV_DASHBOARD_QUOTA_RECORD_URL", "").strip()


def _notify_nav_dashboard_quota(*, web_search_delta: int = 0, deepseek_delta: int = 0) -> None:
    """Fire-and-forget: tell nav_dashboard to record quota usage for RAG QA calls.
    Runs in a background thread so it never blocks the answer stream.
    """
    web_inc = max(0, int(web_search_delta or 0))
    deepseek_inc = max(0, int(deepseek_delta or 0))
    if web_inc <= 0 and deepseek_inc <= 0:
        return

    # Derive the nav_dashboard base URL: prefer env override, else detect running port.
    base = _NAV_DASHBOARD_QUOTA_URL.rstrip("/") if _NAV_DASHBOARD_QUOTA_URL else ""
    if not base:
        base = "http://127.0.0.1:8092"
    url = f"{base}/api/dashboard/usage/record"

    def _fire() -> None:
        import json as _json
        from urllib import error as _urlerror, request as _urlrequest
        payload = _json.dumps({"web_search_delta": web_inc, "deepseek_delta": deepseek_inc}, ensure_ascii=False).encode()
        req = _urlrequest.Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
        try:
            opener = _urlrequest.build_opener(_urlrequest.ProxyHandler({}))
            with opener.open(req, timeout=3):
                pass
        except Exception:
            pass  # quota sync is best-effort; never raise

    import threading as _threading
    _threading.Thread(target=_fire, daemon=True).start()


def _check_rag_quota_exceeded(mode: str, search_mode: str) -> list[dict[str, Any]]:
    """Read nav_dashboard quota state and return a list of exceeded quota descriptors.
    Returns empty list if within limits or if quota state cannot be determined.
    """
    from pathlib import Path as _Path
    nav_data_dir = _Path(__file__).resolve().parents[3] / "nav_dashboard" / "data"
    quota_file = nav_data_dir / "agent_quota.json"
    if not quota_file.exists():
        return []
    try:
        raw = json.loads(quota_file.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return []
        today = datetime.now().strftime("%Y-%m-%d")
        if raw.get("date") != today:
            return []  # quota resets at day change
        web_count = int(raw.get("web_search", 0) or 0)
        deepseek_count = int(raw.get("deepseek", 0) or 0)
    except Exception:
        return []

    # Read limits from agent_service defaults; fall back to safe values
    try:
        import importlib.util as _ilu
        _spec = _ilu.spec_from_file_location(
            "_nav_agent_svc",
            _Path(__file__).resolve().parents[3] / "nav_dashboard" / "web" / "services" / "agent_service.py",
        )
    except Exception:
        _spec = None

    web_limit = 50
    deepseek_limit = 25
    if _spec:
        try:
            _mod = importlib.util.module_from_spec(_spec)
            _spec.loader.exec_module(_mod)  # type: ignore[union-attr]
            web_limit = int(getattr(_mod, "WEB_SEARCH_DAILY_LIMIT", web_limit) or web_limit)
            deepseek_limit = int(getattr(_mod, "DEEPSEEK_DAILY_LIMIT", deepseek_limit) or deepseek_limit)
        except Exception:
            pass

    normalized_mode = (mode or "local").strip().lower()
    normalized_search = _normalize_search_mode(search_mode)

    exceeded: list[dict[str, Any]] = []
    if normalized_search == "hybrid" and web_count >= web_limit:
        exceeded.append({"kind": "web_search", "current": web_count, "limit": web_limit})
    if normalized_mode in {"deepseek", "reasoner"} and deepseek_count >= deepseek_limit:
        exceeded.append({"kind": "deepseek", "current": deepseek_count, "limit": deepseek_limit})
    return exceeded


def _register_active_request(session_id: str, **state: Any) -> None:
    sid = (session_id or "").strip()
    if not sid:
        return
    with _PROCESS_LOCK:
        _ACTIVE_PROCESSES[sid] = dict(state)


def _clear_active_request(session_id: str) -> tuple[bool, dict[str, Any]]:
    sid = (session_id or "").strip()
    with _PROCESS_LOCK:
        was_aborted = sid in _ABORTED_SESSIONS
        state = _ACTIVE_PROCESSES.pop(sid, {}) if sid else {}
        if sid:
            _ABORTED_SESSIONS.discard(sid)
    return was_aborted, state


def _build_ask_rag_runtime_args(
    *,
    session_id: str,
    trace_id: str,
    question: str,
    normalized_search_mode: str,
    effective_top_k: int,
    effective_vector_top_n: int,
    effective_similarity_threshold: float,
    rerank_top_k: int,
    emb: str,
    url: str,
    key: str,
    mdl: str,
    debug: bool,
    use_local_llm: bool,
    memory_context: str,
    no_embed_cache: bool,
    benchmark_mode: bool,
    stream: bool,
) -> argparse.Namespace:
    return ask_rag_script.build_runtime_args(
        question=question,
        documents_dir=str(DOCUMENTS_DIR),
        index_dir=str(VECTOR_DB_DIR),
        backend="faiss",
        search_mode=normalized_search_mode,
        top_k=effective_top_k,
        vector_top_n=effective_vector_top_n,
        max_vector_candidates=max(1, int(MAX_VECTOR_CANDIDATES)),
        enable_rerank="true" if ENABLE_RERANK else "false",
        rerank_top_k=rerank_top_k,
        reranker_model=RERANKER_MODEL,
        similarity_threshold=float(effective_similarity_threshold),
        debug="true" if debug else "false",
        session_id=session_id,
        trace_id=trace_id,
        embedding_model=emb,
        api_url=url,
        api_key=key,
        model=mdl,
        timeout=str(7200 if use_local_llm else int(DEFAULT_TIMEOUT)),
        call_type="answer",
        memory_context=memory_context,
        max_context_chars=str(max(800, int(LOCAL_LLM_MAX_CONTEXT_CHARS))),
        max_chars_per_doc=str(max(300, int(LOCAL_LLM_MAX_CHARS_PER_DOC))),
        no_embed_cache=bool(no_embed_cache or benchmark_mode),
        stream=bool(stream),
    )


def _session_title_is_unlocked(session_id: str) -> bool:
    sid = (session_id or "").strip()
    if not sid:
        return False
    with _LOCK:
        session = _load_session_file(_session_file_path(sid))
        if session is None:
            return False
        return not bool(session.get("title_locked", False))


def _normalize_title_compare_key(text: str) -> str:
    return re.sub(r"[^\w\u4e00-\u9fff]+", "", str(text or "").strip().lower())


def _looks_like_weak_session_title(title: str, question: str) -> bool:
    normalized_title = _normalize_title_compare_key(title)
    normalized_question = _normalize_title_compare_key(question)
    if not normalized_title:
        return True
    if normalized_question.startswith(normalized_title):
        return True
    return any(
        str(title or "").strip().startswith(prefix)
        for prefix in ("请", "帮我", "请帮我", "比较", "请比较", "分析", "请分析", "解释", "请解释", "说明", "请说明")
    )


def _build_answer_anchor_title(question: str, answer: str, max_len: int | None = None) -> str:
    lines = [re.sub(r"^#{1,6}\s*", "", line).strip() for line in str(answer or "").splitlines()]
    headings = [
        line for line in lines
        if line and line not in {"参考资料", "结论", "关键要点", "总结", "背景与关键机制", "典型使用场景及角色"}
    ]
    for heading in headings:
        candidate = sanitize_session_title(heading, max_len=max_len)
        if candidate and not _looks_like_weak_session_title(candidate, question):
            return candidate
    return ""


def _generate_local_session_title(question: str, answer: str, max_len: int | None = None) -> str:
    local_endpoint_is_safe = _is_loopback_url(LOCAL_LLM_API_URL) and not _is_deepseek_url(LOCAL_LLM_API_URL)
    can_use_local_title_llm = bool(LOCAL_LLM_API_URL and LOCAL_LLM_MODEL) and (local_endpoint_is_safe or ALLOW_REMOTE_LOCAL_LLM)
    if can_use_local_title_llm and _is_local_llm_reachable(LOCAL_LLM_API_URL):
        try:
            title = ask_rag_script.generate_session_title(
                question,
                answer,
                api_key=LOCAL_LLM_API_KEY,
                api_url=LOCAL_LLM_API_URL,
                model=LOCAL_LLM_MODEL,
                timeout=min(30, max(5, int(DEFAULT_TIMEOUT))),
            )
            normalized = sanitize_session_title(title, max_len=max_len)
            if normalized and not _looks_like_weak_session_title(normalized, question):
                return normalized
        except Exception:
            pass
    answer_anchor = _build_answer_anchor_title(question, answer, max_len=max_len)
    if answer_anchor:
        return answer_anchor
    return suggest_local_session_title_from_qa(question, answer, max_len=max_len)


def schedule_generated_session_title(session_id: str, question: str, answer: str, *, lock: bool = True) -> None:
    sid = (session_id or "").strip()
    if not sid or not str(answer or "").strip():
        return

    def _run() -> None:
        try:
            if not _session_title_is_unlocked(sid):
                return
            title = _generate_local_session_title(question, answer)
            if title:
                set_session_title_if_unlocked(sid, title, lock=lock)
        except Exception:
            return

    threading.Thread(target=_run, daemon=True, name=f"rag-title-{sid[:8]}").start()


def _heartbeat_progress_message(message: str, stage_started_at: float) -> str:
    clean = str(message or "").strip() or "正在处理问答请求"
    clean = re.sub(r"[.。]+$", "", clean)
    return f"{clean}..."


def _normalize_search_mode(search_mode: str) -> str:
    value = (search_mode or "").strip().lower()
    if value in {"local", "local_only", "local-only"}:
        return "local_only"
    return "hybrid"


def _resolve_search_profile(search_mode: str, top_k: int) -> tuple[int, bool, bool]:
    normalized = _normalize_search_mode(search_mode)
    if normalized == "local_only":
        local_k = max(1, int(top_k) if str(top_k).strip() else LOCAL_ONLY_TOP_K)
        return local_k, False, LOCAL_ONLY_READ_WEB_CACHE
    return HYBRID_LOCAL_TOP_K, True, True


def _estimate_query_tokens(text: str) -> int:
    raw = str(text or "").strip()
    if not raw:
        return 0
    latin_parts = re.findall(r"[A-Za-z0-9_]+", raw)
    cjk_chars = re.findall(r"[\u4e00-\u9fff]", raw)
    cjk_tokens = max(1, len(cjk_chars) // 2) if cjk_chars else 0
    return max(1, len(latin_parts) + cjk_tokens)


def _resolve_query_profile(question: str, *, base_threshold: float, base_vector_top_n: int, base_top_k: int = 5) -> dict[str, Any]:
    token_count = _estimate_query_tokens(question)
    profile = "medium"
    threshold = float(base_threshold)
    vector_top_n = int(base_vector_top_n)
    top_k = int(base_top_k)

    if token_count <= SHORT_QUERY_MAX_TOKENS:
        profile = "short"
        threshold = max(0.0, threshold + SHORT_QUERY_THRESHOLD_DELTA)
        vector_top_n = max(4, vector_top_n + SHORT_QUERY_VECTOR_TOP_N_DELTA)
        top_k = max(1, top_k + SHORT_QUERY_TOP_K_DELTA)
    elif token_count >= LONG_QUERY_MIN_TOKENS:
        profile = "long"
        threshold = min(0.98, threshold + LONG_QUERY_THRESHOLD_DELTA)
        vector_top_n = max(4, vector_top_n + LONG_QUERY_VECTOR_TOP_N_DELTA)
        top_k = max(1, top_k + LONG_QUERY_TOP_K_DELTA)

    vector_top_n = min(max(1, int(MAX_VECTOR_CANDIDATES)), int(vector_top_n))

    return {
        "profile": profile,
        "token_count": token_count,
        "similarity_threshold": round(float(threshold), 6),
        "vector_top_n": int(vector_top_n),
        "top_k": int(top_k),
    }


def _percentile(values: list[float], ratio: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(v) for v in values)
    if len(ordered) == 1:
        return ordered[0]
    index = int(round((len(ordered) - 1) * max(0.0, min(1.0, ratio))))
    return ordered[index]


def _load_retrieval_metrics_records() -> list[dict[str, Any]]:
    if not RETRIEVAL_METRICS_FILE.exists():
        return []
    try:
        payload = json.loads(RETRIEVAL_METRICS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(payload, dict):
        return []
    rows = payload.get("records")
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _save_retrieval_metrics_records(rows: list[dict[str, Any]]) -> None:
    RAG_SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "records": rows[-RETRIEVAL_METRICS_MAX :],
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }
    RETRIEVAL_METRICS_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _coerce_stage_seconds(timings: dict[str, Any], *keys: str) -> float:
    for key in keys:
        value = timings.get(key)
        try:
            if value is None:
                continue
            number = float(value)
            if number >= 0:
                return number
        except Exception:
            continue
    return 0.0


def _resolve_no_context(timings_dict: dict[str, Any]) -> tuple[int, str]:
    """Return (no_context: int, no_context_reason: str) from a timings dict.

    Single source of truth used by both sync and stream paths to ensure
    consistent no_context semantics:
      - below_threshold  : local_after_threshold == 0
      - confidence_none  : retrieval_confidence == 'none' (top1 < 0.30)
      - "" (0)           : otherwise
    """
    _lat = timings_dict.get("local_after_threshold")
    _conf = str(timings_dict.get("retrieval_confidence") or "").strip().lower()
    if _lat is not None and float(_lat) == 0.0:
        return 1, "below_threshold"
    if _conf == "none":
        return 1, "confidence_none"
    return 0, ""


def record_retrieval_metrics(
    *,
    source: str,
    search_mode: str,
    query_profile: str,
    token_count: int,
    timings: dict[str, Any],
    elapsed_seconds: float = 0.0,
    embed_cache_hit: int = 0,
    web_cache_hit: int = 0,
    no_context: int = 0,
    no_context_reason: str = "",
    trace_id: str = "",
    similarity_threshold: float | None = None,
    top1_score_before_rerank: float | None = None,
    top1_score_after_rerank: float | None = None,
    top1_rerank_score_after_rerank: float | None = None,
    top1_identity_changed: int | None = None,
    top1_rank_shift: float | None = None,
) -> None:
    if not isinstance(timings, dict):
        return
    # Field names must match what nav_dashboard/_load_retrieval_latency_summary and app.js expect:
    #   stages.total                 -> ask_rag.py timings["total"] (accumulated vector-search total across all rewrite queries)
    #   stages.rerank_seconds        -> timings["rerank_seconds"]
    #   stages.context_assembly_seconds -> timings["context_assembly_seconds"]
    #   stages.web_search_seconds    -> timings["web_search_seconds"]
    row = {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "source": str(source or "rag").strip() or "rag",
        "search_mode": _normalize_search_mode(search_mode),
        "query_profile": str(query_profile or "medium"),
        "token_count": int(token_count or 0),
        "total": _coerce_stage_seconds(timings, "total", "vector_recall_seconds", "vector_search_seconds", "faiss_search"),
        "rerank_seconds": _coerce_stage_seconds(timings, "rerank_seconds"),
        "reranker_load_seconds": _coerce_stage_seconds(timings, "reranker_load_seconds"),
        "context_assembly_seconds": _coerce_stage_seconds(timings, "context_assembly_seconds", "context_build_seconds", "context_seconds"),
        "web_search_seconds": _coerce_stage_seconds(timings, "web_search_seconds"),
        "elapsed_seconds": round(max(0.0, float(elapsed_seconds or 0)), 4),
        "embed_cache_hit": int(embed_cache_hit or 0),
        "web_cache_hit": int(web_cache_hit or 0),
        "no_context": int(no_context or 0),
        "no_context_reason": str(no_context_reason or "").strip(),
        "trace_id": str(trace_id or "").strip(),
        "similarity_threshold": round(float(similarity_threshold), 4) if similarity_threshold is not None else None,
        "top1_score_before_rerank": round(float(top1_score_before_rerank), 4) if top1_score_before_rerank is not None else None,
        "top1_score_after_rerank": round(float(top1_score_after_rerank), 4) if top1_score_after_rerank is not None else None,
        "top1_rerank_score_after_rerank": round(float(top1_rerank_score_after_rerank), 4) if top1_rerank_score_after_rerank is not None else None,
        "top1_identity_changed": int(top1_identity_changed) if top1_identity_changed is not None else None,
        "top1_rank_shift": round(float(top1_rank_shift), 4) if top1_rank_shift is not None else None,
    }
    rows = _load_retrieval_metrics_records()
    rows.append(row)
    _save_retrieval_metrics_records(rows)


def _build_rag_trace_record(
    *,
    trace_id: str,
    session_id: str,
    source: str,
    mode: str,
    search_mode: str,
    query_profile: dict[str, Any],
    similarity_threshold: float,
    payload: dict[str, Any],
    timings: dict[str, Any],
    no_context: int,
    no_context_reason: str,
) -> dict[str, Any]:
    used_docs = payload.get("used_context_docs") if isinstance(payload.get("used_context_docs"), list) else []
    llm = payload.get("llm") if isinstance(payload.get("llm"), dict) else {}
    graph_batches = payload.get("graph_expansion_batches") if isinstance(payload.get("graph_expansion_batches"), list) else []
    call_type = "benchmark_case" if str(source or "").startswith("benchmark") else str(payload.get("call_type") or "answer")
    return {
        "trace_id": trace_id,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "entrypoint": "rag",
        "call_type": "qa_stream" if source == "rag_qa_stream" else (call_type if call_type == "benchmark_case" else "qa"),
        "session_id": session_id,
        "search_mode": search_mode,
        "query_type": "",
        "query_profile": {
            "profile": str(query_profile.get("profile", "medium") or "medium"),
            "token_count": int(query_profile.get("token_count", 0) or 0),
        },
        "router": {
            "selected_tool": "ask_rag",
            "planned_tools": ["vector_recall", "llm_answer"],
            "decision_category": "local_rag_pipeline",
            "decision_path": ["local_rag_pipeline", "vector_recall", "llm_answer"],
            "planned_tool_depth": 2,
            "executed_tool_depth": 2,
            "classifier_label": "",
            "doc_similarity": timings.get("local_top1_vector_score_after_rerank", timings.get("local_top1_score")),
            "media_entity_confident": False,
        },
        "tools": [
            {
                "name": "vector_recall",
                "status": "ok",
                "latency_ms": round(float(timings.get("total", 0) or 0) * 1000, 1),
                "result_count": int(payload.get("retrieved_local_count", 0) or 0),
                "trace_stage": "rag.vector_recall",
            },
            {
                "name": "llm_answer",
                "status": "ok",
                "latency_ms": round(float(llm.get("latency_seconds", 0) or 0) * 1000, 1),
                "result_count": int(payload.get("retrieved_count", 0) or 0),
                "trace_stage": "rag.llm.answer",
            },
        ],
        "retrieval": {
            "vector_hits": int(payload.get("retrieved_local_count", 0) or 0),
            "vector_candidates": int(payload.get("retrieved_local_vector_candidates", 0) or 0),
            "similarity_threshold": round(float(similarity_threshold), 4),
            "top1_score_before_rerank": timings.get("local_top1_score"),
            "top1_score_after_rerank": timings.get("local_top1_vector_score_after_rerank", timings.get("local_top1_score")),
            "top1_rerank_score_after_rerank": timings.get("local_top1_rerank_score_after_rerank", timings.get("local_top1_score_after_rerank")),
            "top1_path_before_rerank": timings.get("local_top1_path_before_rerank"),
            "top1_path_after_rerank": timings.get("local_top1_path_after_rerank"),
            "query_rewrite_status": str(timings.get("query_rewrite_status", "") or ""),
            "query_rewrite_count": len(list(timings.get("query_rewrite_queries") or [])),
            "graph_expansion_batches": len(graph_batches),
        },
        "ranking": {
            "method": str(payload.get("reranker_model", "") or "") and "cross_encoder_fused",
            "rerank_k": int(payload.get("rerank_top_k", 0) or 0),
            "rerank_candidate_count": int(timings.get("local_rerank_candidate_count", 0) or 0),
            "rerank_candidate_profile": str(timings.get("local_rerank_candidate_profile", "") or ""),
            "fusion_alpha": timings.get("local_rerank_fusion_alpha"),
            "fusion_alpha_base": timings.get("local_rerank_fusion_alpha_base"),
            "dynamic_alpha_enabled": timings.get("local_rerank_dynamic_alpha_enabled"),
            "rerank_soft_top1": timings.get("local_rerank_rerank_soft_top1"),
            "rerank_soft_top2": timings.get("local_rerank_rerank_soft_top2"),
            "rerank_soft_diff": timings.get("local_rerank_rerank_soft_diff"),
            "rerank_confidence_factor": timings.get("local_rerank_confidence_factor"),
            "fusion_alpha_reason": timings.get("local_rerank_fusion_alpha_reason"),
            "top1_rerank_score": timings.get("local_top1_rerank_score_after_rerank", timings.get("local_top1_score_after_rerank")),
            "top1_final_score": timings.get("local_top1_final_score_after_rerank"),
            "top1_vector_delta": timings.get("local_top1_vector_score_delta_after_rerank"),
            "top1_identity_changed": timings.get("local_top1_identity_changed"),
            "top1_rank_shift": timings.get("local_top1_rank_shift"),
            "baseline_gap": timings.get("local_rerank_guard_baseline_gap"),
            "swap_blocked_by_gap": timings.get("local_rerank_guard_swap_blocked_by_gap"),
            "guard_triggered": timings.get("local_rerank_guard_triggered"),
            "guard_reason": timings.get("local_rerank_guard_reason"),
        },
        "llm": {
            "backend": mode,
            "model": str(llm.get("model", "") or payload.get("mode", "")),
            "latency_seconds": round(float(llm.get("latency_seconds", 0) or 0), 6),
            "input_tokens_est": int(llm.get("input_tokens_est", 0) or 0),
            "prompt_tokens_est": int(llm.get("prompt_tokens_est", 0) or 0),
            "context_tokens_est": int(llm.get("context_tokens_est", 0) or 0),
            "output_tokens_est": int(llm.get("output_tokens_est", 0) or 0),
            "calls": int(llm.get("calls", 0) or 0),
        },
        "stages": {
            "vector_recall_seconds": round(float(timings.get("total", 0) or 0), 6),
            "rerank_seconds": round(float(timings.get("rerank_seconds", 0) or 0), 6),
            "context_assembly_seconds": round(float(timings.get("context_assembly_seconds", 0) or 0), 6),
            "web_search_seconds": round(float(timings.get("web_search_seconds", 0) or 0), 6),
            "llm_seconds": round(float(llm.get("latency_seconds", 0) or 0), 6),
            "wall_clock_seconds": round(float(payload.get("elapsed_seconds", 0) or 0), 6),
        },
        "total_elapsed_seconds": round(float(payload.get("elapsed_seconds", 0) or 0), 6),
        "result": {
            "status": "ok",
            "no_context": int(no_context or 0),
            "no_context_reason": str(no_context_reason or ""),
            "degraded_to_retrieval": False,
            "used_context_docs": len(used_docs),
        },
    }


def _write_rag_trace_record(record: dict[str, Any]) -> None:
    try:
        write_trace_record(record)
    except Exception:
        pass


def get_retrieval_metrics_summary() -> dict[str, Any]:
    rows = _load_retrieval_metrics_records()
    rows = rows[-RETRIEVAL_METRICS_MAX :]
    stages = ["vector_recall_seconds", "rerank_seconds", "context_build_seconds", "web_search_seconds"]
    summary: dict[str, Any] = {
        "records": rows,
        "window": len(rows),
        "stages": {},
    }
    for stage in stages:
        values = [float(r.get(stage, 0.0) or 0.0) for r in rows]
        if not values:
            summary["stages"][stage] = {"avg": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0}
            continue
        avg = sum(values) / len(values)
        summary["stages"][stage] = {
            "avg": round(avg, 4),
            "p50": round(_percentile(values, 0.50), 4),
            "p95": round(_percentile(values, 0.95), 4),
            "p99": round(_percentile(values, 0.99), 4),
        }
    return summary


class RAGTaskAborted(RuntimeError):
    pass


def _is_deepseek_url(url: str) -> bool:
    value = (url or "").strip().lower()
    return "api.deepseek.com" in value


def _is_loopback_url(url: str) -> bool:
    value = (url or "").strip()
    if not value:
        return False
    try:
        parsed = urlparse(value)
    except Exception:
        return False
    host = (parsed.hostname or "").strip().lower()
    return host in {"127.0.0.1", "localhost", "::1"}


def _is_local_llm_reachable(url: str, timeout_seconds: float = 1.2) -> bool:
    value = (url or "").strip()
    if not value:
        return False
    try:
        parsed = urlparse(value)
    except Exception:
        return False
    host = (parsed.hostname or "").strip()
    port = int(parsed.port or (443 if (parsed.scheme or "").lower() == "https" else 80))
    if not host:
        return False
    try:
        with socket.create_connection((host, port), timeout=max(0.3, float(timeout_seconds))):
            return True
    except Exception:
        return False


def _strip_ansi(text: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", text or "")


def _friendly_subprocess_error(output: str, code: int) -> str:
    plain = _strip_ansi(output).strip()
    lowered = plain.lower()

    if "authenticationerror" in lowered or "authentication fails" in lowered or "401" in lowered:
        return "API 认证失败：请检查 API_BASE_URL 与 API_KEY 是否匹配，或切换到可用的本地 OpenAI 兼容服务。"

    # Prefer concise ERROR: line from ask_rag.py if present.
    for line in plain.splitlines():
        cleaned = line.strip()
        if cleaned.startswith("ERROR:"):
            return cleaned.removeprefix("ERROR:").strip() or f"ask_rag.py failed: {code}"

    return plain or f"ask_rag.py failed: {code}"


def _sanitize_command_for_debug(command: list[str]) -> list[str]:
    sanitized = list(command)
    secret_flags = {"--api-key"}
    i = 0
    while i < len(sanitized):
        token = str(sanitized[i]).strip().lower()
        if token in secret_flags and i + 1 < len(sanitized):
            sanitized[i + 1] = "***"
            i += 2
            continue
        i += 1
    return sanitized


def _looks_like_html_error_blob(message: str) -> bool:
    text = (message or "").strip().lower()
    if not text:
        return False
    return (
        "ie friendly error message" in text
        or "<!doctype html" in text
        or "<html" in text
        or text.startswith("<!--")
    )


def _humanize_error_message(message: str) -> str:
    raw = _strip_ansi(message or "").strip()
    if not raw:
        return "未知错误"

    if _looks_like_html_error_blob(raw):
        return (
            "请求返回了 HTML 错误页（常见于网关/代理拦截或服务地址不可用）。"
            "若本机未部署本地大模型，这是正常现象，系统将尝试降级为检索回答。"
        )

    return raw


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
        "ie friendly error message",
        "<!doctype html",
        "<html",
    ]
    return any(marker in text for marker in markers)


def _local_fallback_notice() -> str:
    return (
        "本机未检测到可用的本地大模型服务。"
        "已自动降级为仅基于本地知识库检索结果（bge-base embedding）生成回答。"
    )


def _local_mode_disallow_notice() -> str:
    return (
        "当前为 local 模式，但检测到 AI_SUMMARY_LOCAL_LLM_URL 指向非本机地址（或 DeepSeek 域名）。"
        "为避免误用远程模型，已自动降级为仅检索回答。"
        "如确需远程 OpenAI 兼容地址，请设置 AI_SUMMARY_LOCAL_LLM_ALLOW_REMOTE=1。"
    )


def _looks_like_generic_subprocess_error(message: str) -> bool:
    text = (message or "").strip().lower()
    if not text:
        return True
    return (
        text == "ask_rag.py failed: 2"
        or text.startswith("ask_rag.py failed:")
        or text in {"unknown error", "未知错误"}
    )


def _prepend_fallback_notice(answer: str, notice: str) -> str:
    body = (answer or "").strip()
    prefix = f"[提示] {notice}"
    if not body:
        return prefix
    return f"{prefix}\n\n---\n\n{body}"


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


def _query_terms(question: str) -> list[str]:
    text = re.sub(r"[^\w\u4e00-\u9fff]+", " ", (question or "").lower())
    terms = [t for t in text.split() if len(t) >= 2]
    # Keep order while deduplicating.
    ordered: list[str] = []
    seen: set[str] = set()
    for term in terms:
        if term in seen:
            continue
        seen.add(term)
        ordered.append(term)
    return ordered


def _pick_relevant_clips(question: str, plain_text: str, max_items: int = 5) -> list[str]:
    terms = _query_terms(question)
    parts = re.split(r"[。！？!?；;\n\r]", plain_text)
    candidates: list[tuple[int, str]] = []
    for part in parts:
        sentence = re.sub(r"\s+", " ", part).strip()
        if len(sentence) < 12:
            continue
        score = 0
        for term in terms:
            if term in sentence.lower():
                score += 1
        if score > 0:
            candidates.append((score, sentence))

    # Prefer sentences covering more query terms.
    candidates.sort(key=lambda x: x[0], reverse=True)
    selected = [text for _score, text in candidates[:max_items]]
    if selected:
        return selected

    # Fallback: first meaningful segments.
    fallback: list[str] = []
    for part in parts:
        sentence = re.sub(r"\s+", " ", part).strip()
        if len(sentence) >= 12:
            fallback.append(sentence)
        if len(fallback) >= max_items:
            break
    return fallback


def _strip_markdown_for_clip_extraction(text: str) -> str:
    value = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
    if not value.strip():
        return ""

    # Drop fenced code blocks and flatten links to labels to reduce markdown noise.
    value = re.sub(r"```[\s\S]*?```", " ", value)
    value = re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", r"\1", value)
    value = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1", value)
    value = re.sub(r"`([^`]+)`", r"\1", value)

    cleaned_lines: list[str] = []
    for raw_line in value.split("\n"):
        line = raw_line.strip()
        if not line:
            cleaned_lines.append("")
            continue

        line = re.sub(r"^\s{0,3}#{1,6}\s*", "", line)
        line = re.sub(r"^\s*>+\s*", "", line)
        line = re.sub(r"^\s*[-*+]\s+", "", line)
        line = re.sub(r"^\s*\d+[\.\)\u3001\uFF0E]\s+", "", line)
        line = re.sub(r"\*\*([^*]+)\*\*", r"\1", line)
        line = re.sub(r"\*([^*]+)\*", r"\1", line)
        line = re.sub(r"^---+$", "", line)
        cleaned_lines.append(line)

    cleaned = "\n".join(cleaned_lines)
    cleaned = re.sub(r"[ \t]+", " ", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def _build_local_only_answer(question: str, rows: list[dict[str, Any]]) -> tuple[str, list[dict[str, Any]]]:
    evidence_lines: list[str] = []
    synthesis_points: list[str] = []
    used_docs: list[dict[str, Any]] = []
    max_refs = HYBRID_LOCAL_TOP_K + HYBRID_WEB_TOP_K

    for idx, item in enumerate(rows[:max_refs], start=1):
        source = str(item.get("source", "")).strip().lower()
        raw = ""
        rel = ""
        title = str(item.get("title", "")).strip()
        summary_text = str(item.get("summary", "")).strip()
        topic = str(item.get("topic", "")).strip()
        if source == "web":
            rel = str(item.get("url", "")).strip() or str(item.get("title", "")).strip() or "web_result"
            raw = str(item.get("content", "")).strip()
        else:
            file_path = _resolve_result_path(item, DOCUMENTS_DIR)
            rel = str(item.get("relative_path", "")).strip() or str(item.get("path", "")).strip()
            if not rel and file_path is not None:
                rel = file_path.name
            if file_path is not None and file_path.is_file():
                try:
                    raw = file_path.read_text(encoding="utf-8", errors="ignore")
                except Exception:
                    raw = ""

        if not rel:
            continue

        score = float(item.get("score", 0.0))

        summary_clean = _strip_markdown_for_clip_extraction(summary_text)
        topic_clean = _strip_markdown_for_clip_extraction(topic)
        title_clean = _strip_markdown_for_clip_extraction(title)

        # Prefer structured fields from vector metadata to avoid markdown-noisy snippets.
        metadata_points: list[str] = []
        if summary_clean:
            summary_clips = _pick_relevant_clips(question, summary_clean, max_items=1)
            summary_point = (summary_clips[0] if summary_clips else summary_clean.split("\n", 1)[0]).strip()
            if summary_point:
                metadata_points.append(summary_point[:140])
        if topic_clean and topic_clean.lower() not in {p.lower() for p in metadata_points}:
            metadata_points.append(topic_clean[:100])
        if title_clean and title_clean.lower() not in {p.lower() for p in metadata_points}:
            metadata_points.append(title_clean[:100])

        if metadata_points:
            synthesis_points.extend(metadata_points[:2])
        else:
            plain = _strip_markdown_for_clip_extraction(raw)
            clips = _pick_relevant_clips(question, plain, max_items=2)
            for clip in clips:
                synthesis_points.append(clip)

        # Keep references compact; avoid dumping long raw excerpts in final answer.
        src_tag = "web" if source == "web" else "local"
        evidence_lines.append(f"- [{idx}] {rel}（score={score:.4f}，source={src_tag}）")
        used_doc: dict[str, Any] = {"path": rel, "score": score, "topic": topic}
        if source == "web":
            used_doc["title"] = title or rel
        used_docs.append(used_doc)

    if not evidence_lines:
        answer = "未检索到可用资料。请尝试换一个问法，或先补齐知识库文档后再提问。"
        return answer, used_docs

    unique_points: list[str] = []
    seen_points: set[str] = set()
    for point in synthesis_points:
        p = point.strip()
        if not p or p in seen_points:
            continue
        seen_points.add(p)
        unique_points.append(p)
        if len(unique_points) >= 5:
            break

    if unique_points:
        abstract_parts = unique_points[:3]
        # Build a smoother narrative paragraph instead of itemized quote stitching.
        summary = "。".join([re.sub(r"[。；;]+$", "", part) for part in abstract_parts]).strip()
        if summary and not summary.endswith("。"):
            summary += "。"
    else:
        summary = "结合已检索资料，当前问题可以从背景、关键条件和结果影响三个方面来理解。"

    # Keep only short distilled points.
    distilled = [re.sub(r"\s+", " ", p).strip() for p in unique_points[:4]]
    key_points = "\n".join([f"- {p[:120]}" for p in distilled]) or "- 暂无可提炼的关键点"
    evidence = "\n".join(evidence_lines[:max_refs])

    answer = (
        f"### 结论\n"
        f"针对问题“{question}”，结合检索到的上下文信息，综合判断如下：{summary}\n\n"
        "### 关键要点\n"
        f"{key_points}\n\n"
        "### 依据资料\n"
        f"{evidence}"
    )
    return answer, used_docs


def _ask_rag_local_without_api(question: str, top_k: int, embedding_model: str, search_mode: str = "hybrid") -> dict[str, Any]:
    local_top_k, enable_web_search, allow_web_cache = _resolve_search_profile(search_mode, top_k)
    graph_expand = expand_query_by_graph(VECTOR_DB_DIR, question)
    expanded_query = str(graph_expand.get("expanded_query") or question).strip() or question
    try:
        rows, timings = search_vector_index_with_diagnostics(
            query=expanded_query,
            documents_dir=DOCUMENTS_DIR,
            index_dir=VECTOR_DB_DIR,
            top_k=local_top_k,
            backend="faiss",
            build_if_missing=BUILD_INDEX_ON_DEMAND,
            embedding_model=embedding_model,
            timeout=int(DEFAULT_TIMEOUT),
        )
    except RAGIndexError as exc:
        raise RuntimeError(str(exc)) from exc

    web_rows: list[dict[str, Any]] = []
    web_status = "disabled"
    if enable_web_search or allow_web_cache:
        web_rows, web_status = _search_web_tavily_rows(question, HYBRID_WEB_TOP_K)
        if not enable_web_search and web_status != "cache_hit":
            web_rows = []
            web_status = "disabled_cache_only"
    merged_rows = sorted(list(rows) + list(web_rows), key=lambda x: float(x.get("score", 0.0)), reverse=True)
    answer, used_docs = _build_local_only_answer(question, merged_rows)
    timings["web_search_status"] = web_status
    timings["web_search_count"] = len(web_rows)
    return {
        "question": question,
        "session_title": (question or "").strip()[:16] or "新会话",
        "answer": answer,
        "retrieved_count": len(merged_rows),
        "retrieved_local_count": len(rows),
        "retrieved_web_count": len(web_rows),
        "search_mode": _normalize_search_mode(search_mode),
        "used_context_docs": used_docs,
        "timings": timings,
        "elapsed_seconds": 0.0,
        "embedding_model": embedding_model,
        "graph_expansion": graph_expand,
        "mode": "local",
    }


def _search_web_tavily_rows(question: str, max_results: int) -> tuple[list[dict[str, Any]], str]:
    key = os.getenv("TAVILY_API_KEY", "").strip() or (DEFAULT_TAVILY_API_KEY or "").strip()
    if not key:
        return [], "missing_api_key"

    # Check web search cache before calling the API.
    _wcache = None
    try:
        from cache_db import get_web_cache
        _wcache = get_web_cache()
        _cached = _wcache.get(question, max_results)
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
        payload = TavilySearch(max_results=max(1, int(max_results))).invoke((question or "").strip())
    except Exception as exc:  # noqa: BLE001
        return [], f"search_error:{exc}"

    results = payload.get("results", []) if isinstance(payload, dict) else []
    if not isinstance(results, list):
        return [], "invalid_response"

    rows: list[dict[str, Any]] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title", "")).strip()
        url = str(item.get("url", "")).strip()
        content = str(item.get("content", "")).strip()
        score = float(item.get("score", 0.0) or 0.0)
        if not (title or url or content):
            continue
        rows.append(
            {
                "source": "web",
                "title": title,
                "url": url,
                "content": content,
                "score": score,
                "topic": "web_search",
            }
        )
    rows.sort(key=lambda x: float(x.get("score", 0.0)), reverse=True)
    result_rows = rows[: max(1, int(max_results))]
    # Cache successful results.
    try:
        if _wcache is not None and result_rows:
            _wcache.set(question, max_results, result_rows)
    except Exception:
        pass
    return result_rows, "ok"


def _build_local_fallback_payload(question: str, top_k: int, embedding_model: str, notice: str, search_mode: str = "hybrid") -> dict[str, Any]:
    payload = _ask_rag_local_without_api(question, top_k, embedding_model, search_mode)
    payload["answer"] = _prepend_fallback_notice(str(payload.get("answer", "")), notice)
    payload["degraded"] = True
    payload["degrade_reason"] = "local_llm_unavailable"
    payload["degrade_notice"] = notice
    return payload


def _build_minimal_degraded_payload(
    question: str,
    notice: str,
    reason: str,
    *,
    search_mode: str,
    embedding_model: str,
    mode: str,
) -> dict[str, Any]:
    answer = (
        f"[提示] {notice}\n\n"
        "当前已降级为最小可用回答模式。\n"
        "由于向量索引暂不可用（常见原因：embedding 模型与索引维度不一致），"
        "请先在 Workflow 点击“补齐向量”重建索引后再试。"
    )
    return {
        "question": question,
        "session_title": (question or "").strip()[:16] or "新会话",
        "answer": answer,
        "retrieved_count": 0,
        "retrieved_local_count": 0,
        "retrieved_web_count": 0,
        "search_mode": _normalize_search_mode(search_mode),
        "used_context_docs": [],
        "timings": {},
        "elapsed_seconds": 0.0,
        "embedding_model": embedding_model,
        "mode": mode,
        "degraded": True,
        "degrade_reason": reason,
        "degrade_notice": notice,
    }


def _safe_build_local_fallback_payload(
    question: str,
    top_k: int,
    embedding_model: str,
    notice: str,
    search_mode: str,
    mode: str,
    reason_prefix: str,
) -> dict[str, Any]:
    try:
        payload = _build_local_fallback_payload(question, top_k, embedding_model, notice, search_mode)
    except Exception as fallback_exc:  # noqa: BLE001
        payload = _build_minimal_degraded_payload(
            question,
            notice,
            f"{reason_prefix}:{fallback_exc}",
            search_mode=search_mode,
            embedding_model=embedding_model,
            mode=mode,
        )
    payload["mode"] = mode
    payload["embedding_model"] = embedding_model
    return payload


def resolve_chat_model(mode: str, requested_model: str | None) -> str:
    value = (mode or "").strip().lower()
    if value == "deepseek":
        return "deepseek-chat"
    if value == "reasoner":
        return "deepseek-reasoner"
    model = (requested_model or "").strip() or DEFAULT_MODEL
    return model


def readable_model_name(model_value: str) -> str:
    value = (model_value or "").strip()
    if not value:
        return ""

    normalized = value.replace("\\", "/")
    marker = "/models--"
    if marker in normalized:
        tail = normalized.split(marker, 1)[1]
        repo = tail.split("/", 1)[0].replace("--", "/")
        name = repo.split("/", 1)[-1].strip()
        if name:
            return name

    parts = [p for p in normalized.split("/") if p]
    if "snapshots" in parts:
        idx = parts.index("snapshots")
        if idx >= 1:
            candidate = parts[idx - 1]
            if candidate:
                return candidate

    if len(parts) >= 2 and parts[-2] in {"local_models", "models"}:
        return parts[-1]

    if "/" in value:
        return parts[-1] if parts else value
    return value


def default_chat_config() -> dict[str, str]:
    # For display purposes: show local LLM model if configured, else embedding model.
    embed_model = resolve_embedding_model()
    display_model = LOCAL_LLM_MODEL if LOCAL_LLM_MODEL else embed_model
    return {
        "api_url": DEFAULT_API_BASE_URL,
        "api_key": DEFAULT_API_KEY,
        "model": DEFAULT_MODEL,
        "embedding_model": display_model,
        "mode": "local",
    }


def sanitize_session_title(title: str, max_len: int | None = None) -> str:
    clean = re.sub(r"\s+", " ", (title or "").strip())
    if not clean:
        return "未命名会话"
    if max_len is None or max_len <= 0 or len(clean) <= max_len:
        return clean
    return clean[:max_len]


def suggest_local_session_title(question: str, max_len: int | None = None) -> str:
    text = re.sub(r"\s+", " ", (question or "").strip())
    if not text:
        return "未命名会话"

    # Prefer the first semantic segment to mimic a concise local summary title.
    first = re.split(r"[。！？!?；;\n\r]", text, maxsplit=1)[0].strip()
    first = re.sub(r'^["\'`\[\(【（]+', "", first)
    first = re.sub(r'["\'`\]\)】）]+$', "", first)
    return sanitize_session_title(first or text, max_len=max_len)


def suggest_local_session_title_from_qa(question: str, answer: str, max_len: int | None = None) -> str:
    q = re.sub(r"\s+", " ", (question or "").strip())
    a = re.sub(r"\s+", " ", (answer or "").strip())

    if a:
        # Remove markdown references and boilerplate sections before extracting title phrase.
        cleaned = re.sub(r"\[资料\s*\d+\]|\[\d+\]", "", a)
        cleaned = re.sub(r"^\s*参考标注\s*:\s*.*$", "", cleaned, flags=re.MULTILINE)
        cleaned = re.sub(r"^\s*#+\s*参考资料\s*$", "", cleaned, flags=re.MULTILINE)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        first = re.split(r"[。！？!?；;\n\r]", cleaned, maxsplit=1)[0].strip()
        first = re.sub(r"^[\"'`\[\(【（]+", "", first)
        first = re.sub(r"[\"'`\]\)】）]+$", "", first)
        if first and len(first) >= 4:
            return sanitize_session_title(first, max_len=max_len)

    return suggest_local_session_title(q, max_len=max_len)


def format_local_answer_with_refs(
    answer: str,
    used_docs: list[dict[str, Any]] | None,
    *,
    citation_map: dict[str, Any] | None = None,
) -> str:
    """Append a '### 参考资料' section to *answer*.

    When *citation_map* is supplied (produced by ``_reconcile_citations`` in
    ask_rag.py) the reference section is restricted to docs that were actually
    cited or auto-annotated in the answer, trimming noise from the context
    window that the model never referenced.  Falls back to the full *used_docs*
    list when *citation_map* is absent (e.g. local-only mode, fallback paths).
    """
    text = (answer or "").strip()
    if not text:
        return text

    text = re.sub(r"\[资料\s*(\d+)\]", r"[\1]", text)

    # Restrict reference entries to cited docs when citation_map is available.
    all_docs = used_docs or []
    if citation_map is not None:
        cited_indices: set[int] = (
            set(citation_map.get("cited_by_model") or [])
            | set(citation_map.get("auto_annotated") or [])
        )
        if cited_indices:
            # used_docs is 0-indexed; citation indices are 1-based
            docs = [all_docs[i - 1] for i in sorted(cited_indices) if 1 <= i <= len(all_docs)]
        else:
            docs = all_docs  # nothing specifically cited — keep all
    else:
        docs = all_docs

    ref_entries: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for item in docs:
        if not isinstance(item, dict):
            continue
        path = str(item.get("path", "")).strip().replace("\\", "/")
        title = str(item.get("title", "")).strip()
        score = item.get("score")
        rerank_score = item.get("rerank_score")
        if not path:
            continue
        lowered = path.lower()
        is_web = lowered.startswith("http://") or lowered.startswith("https://")
        link = path if is_web else f"doc://{quote(path)}"
        label = title if (is_web and title) else path
        key = f"{label}|{link}"
        if key in seen_keys:
            continue
        seen_keys.add(key)
        ref_entries.append({"label": label, "link": link, "score": score, "rerank_score": rerank_score})

    if not ref_entries:
        return text

    ranked_entries = sorted(ref_entries, key=lambda entry: float(entry.get("score", 0.0) or 0.0), reverse=True)[: max(1, int(MAX_REFERENCE_ITEMS))]

    if not re.search(r"\[\d+\]", text):
        markers = "".join(f"[{idx}]" for idx in range(1, len(ranked_entries) + 1))
        text = f"{text}\n\n参考标注：{markers}"

    lines = ["---", "### 参考资料"]
    for idx, entry in enumerate(ranked_entries, start=1):
        score_text = ""
        try:
            if entry.get("score") is not None:
                score_text = f" score={float(entry.get('score')):.4f}"
                if entry.get("rerank_score") is not None:
                    score_text = f"{score_text} rerank={float(entry.get('rerank_score')):.4f}"
        except Exception:
            score_text = ""
        lines.append(f"- [{idx}] [{entry['label']}{score_text}]({entry['link']})")
    return f"{text}\n\n" + "\n".join(lines)


def format_answer_with_refs(
    answer: str,
    used_docs: list[dict[str, Any]] | None,
    *,
    mode: str,
    citation_map: dict[str, Any] | None = None,
) -> str:
    normalized_mode = (mode or "").strip().lower() or "local"
    if normalized_mode == "local":
        return format_local_answer_with_refs(answer, used_docs, citation_map=citation_map)

    text = (answer or "").strip()
    if not text:
        return text

    docs = used_docs or []
    ref_entries: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for item in docs:
        if not isinstance(item, dict):
            continue
        path = str(item.get("path", "")).strip().replace("\\", "/")
        title = str(item.get("title", "")).strip()
        score = item.get("score")
        rerank_score = item.get("rerank_score")
        if not path:
            continue
        lowered = path.lower()
        is_web = lowered.startswith("http://") or lowered.startswith("https://")
        link = path if is_web else f"doc://{quote(path)}"
        label = title if (is_web and title) else path
        key = f"{label}|{link}"
        if key in seen_keys:
            continue
        seen_keys.add(key)
        ref_entries.append({
            "label": label,
            "link": link,
            "score": score,
            "rerank_score": rerank_score,
        })

    if not ref_entries:
        return text

    # External-API mode: remove inline markers and keep links in a dedicated section.
    text = re.sub(r"\[资料\s*\d+\]|\[\d+\]", "", text)
    text = re.sub(r"\s{2,}", " ", text).strip()

    lines = ["---", "### 参考资料"]
    def _score_sort_key(entry: dict[str, Any]) -> float:
        try:
            return float(entry.get("score", 0.0) or 0.0)
        except Exception:
            return 0.0

    for idx, entry in enumerate(sorted(ref_entries, key=_score_sort_key, reverse=True)[: max(1, int(MAX_REFERENCE_ITEMS))], start=1):
        score_text = ""
        try:
            if entry.get("score") is not None:
                score_text = f" score={float(entry.get('score')):.4f}"
                if entry.get("rerank_score") is not None:
                    score_text = f"{score_text} rerank={float(entry.get('rerank_score')):.4f}"
        except Exception:
            score_text = ""
        lines.append(f"- [{idx}] [{entry['label']}{score_text}]({entry['link']})")
    return f"{text}\n\n" + "\n".join(lines)


def _session_file_path(session_id: str) -> Path:
    return RAG_SESSIONS_DIR / f"{SESSION_FILE_PREFIX}{session_id}.json"


def _memory_file_path(session_id: str) -> Path:
    return MEMORY_DIR / f"memory_{session_id}.json"


def _iter_session_files() -> list[Path]:
    if not RAG_SESSIONS_DIR.exists():
        return []
    return sorted(RAG_SESSIONS_DIR.glob(f"{SESSION_FILE_PREFIX}*.json"), key=lambda p: p.name.lower())


def _normalize_messages(messages_raw: object) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    if not isinstance(messages_raw, list):
        return result
    for item in messages_raw:
        role = ""
        text = ""
        trace_id = ""
        if isinstance(item, dict):
            role = str(item.get("role", "")).strip()
            text = str(item.get("text", "")).strip()
            trace_id = str(item.get("trace_id", "")).strip()
        elif isinstance(item, (tuple, list)) and len(item) >= 2:
            role = str(item[0]).strip()
            text = str(item[1]).strip()
        if role and text:
            normalized: dict[str, str] = {"role": role, "text": text}
            if trace_id:
                normalized["trace_id"] = trace_id
            result.append(normalized)
    return result


def _normalize_session(raw: object) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    now = datetime.now().isoformat(timespec="seconds")
    sid = str(raw.get("id", "")).strip() or str(uuid4())
    title = sanitize_session_title(str(raw.get("title", "新会话")) or "新会话")
    created_at = str(raw.get("created_at", "")).strip() or now
    updated_at = str(raw.get("updated_at", "")).strip() or created_at
    title_locked = bool(raw.get("title_locked", False))
    messages = _normalize_messages(raw.get("messages", []))
    if not messages:
        messages = [{"role": "系统", "text": "欢迎使用 RAG Q&A。"}]
    return {
        "id": sid,
        "title": title,
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
    normalized = _normalize_session(raw)
    if normalized is None:
        return None
    return normalized


def _save_session(session: dict[str, Any]) -> None:
    normalized = _normalize_session(session)
    if normalized is None:
        raise RuntimeError("会话格式无效")
    sid = str(normalized["id"])
    path = _session_file_path(sid)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(normalized, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_memory(session_id: str) -> dict[str, Any]:
    path = _memory_file_path(session_id)
    if not path.exists():
        return {
            "version": 1,
            "session_id": session_id,
            "updated_at": "",
            "timestamp": "",
            "call_type": "memory_update",
            "session_goal": "",
            "decisions": [],
            "open_issues": [],
            "assumptions": [],
            "recent_turns": [],
        }
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {
            "version": 1,
            "session_id": session_id,
            "updated_at": "",
            "timestamp": "",
            "call_type": "memory_update",
            "session_goal": "",
            "decisions": [],
            "open_issues": [],
            "assumptions": [],
            "recent_turns": [],
        }
    if not isinstance(raw, dict):
        return {
            "version": 1,
            "session_id": session_id,
            "updated_at": "",
            "timestamp": "",
            "call_type": "memory_update",
            "session_goal": "",
            "decisions": [],
            "open_issues": [],
            "assumptions": [],
            "recent_turns": [],
        }
    raw.setdefault("version", 1)
    raw["session_id"] = session_id
    raw.setdefault("updated_at", "")
    raw.setdefault("timestamp", str(raw.get("updated_at", "")))
    raw.setdefault("call_type", "memory_update")
    raw.setdefault("session_goal", "")
    raw.setdefault("decisions", [])
    raw.setdefault("open_issues", [])
    raw.setdefault("assumptions", [])
    raw.setdefault("recent_turns", [])
    return raw


def _save_memory(session_id: str, memory: dict[str, Any]) -> None:
    path = _memory_file_path(session_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    memory["session_id"] = session_id
    ts = str(memory.get("updated_at", "")).strip() or datetime.now().isoformat(timespec="seconds")
    memory["updated_at"] = ts
    memory["timestamp"] = ts
    memory["call_type"] = "memory_update"
    trimmed = _trim_memory_to_budget(memory, max_tokens=MEMORY_MAX_TOKENS)
    path.write_text(json.dumps(trimmed, ensure_ascii=False, indent=2), encoding="utf-8")


def _parse_iso_timestamp(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _strip_think_blocks(text: str) -> str:
    # Remove hidden reasoning blocks from short-term memory snapshots.
    cleaned = re.sub(r"<think>[\s\S]*?</think>", "", str(text or ""), flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", cleaned).strip()


def _extract_recent_turns(session_id: str, rounds: int = 3) -> list[dict[str, str]]:
    sid = (session_id or "").strip()
    if not sid or rounds <= 0:
        return []

    path = _session_file_path(sid)
    if not path.exists():
        return []
    session = _load_session_file(path)
    if not session:
        return []

    messages = session.get("messages", [])
    if not isinstance(messages, list):
        return []

    user_roles = {"用户", "user"}
    assistant_roles = {"助手", "assistant"}

    pairs: list[dict[str, str]] = []
    pending_question = ""
    for item in messages:
        if not isinstance(item, dict):
            continue
        role = str(item.get("role", "")).strip().lower()
        text = _strip_think_blocks(str(item.get("text", "")))
        if not text:
            continue

        if role in user_roles:
            pending_question = text
            continue
        if role in assistant_roles and pending_question:
            pairs.append({"question": pending_question, "answer": text})
            pending_question = ""

    if not pairs:
        return []
    return pairs[-rounds:]


def _approx_tokens(text: str) -> int:
    if not text:
        return 0
    cjk = re.findall(r"[\u4e00-\u9fff]", text)
    words = re.findall(r"[A-Za-z0-9_]+", text)
    return len(cjk) + len(words) + max(0, len(text) // 20)


def _memory_token_estimate(memory: dict[str, Any]) -> int:
    return _approx_tokens(json.dumps(memory, ensure_ascii=False))


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
        # Debug persistence should not affect normal answer flow.
        return


def _trim_memory_to_budget(memory: dict[str, Any], max_tokens: int) -> dict[str, Any]:
    value = dict(memory)
    value.setdefault("decisions", [])
    value.setdefault("open_issues", [])
    value.setdefault("assumptions", [])
    value.setdefault("recent_turns", [])
    while _memory_token_estimate(value) > max_tokens:
        if isinstance(value.get("recent_turns"), list) and value["recent_turns"]:
            value["recent_turns"] = value["recent_turns"][1:]
            continue
        if isinstance(value.get("open_issues"), list) and value["open_issues"]:
            value["open_issues"] = value["open_issues"][1:]
            continue
        if isinstance(value.get("assumptions"), list) and value["assumptions"]:
            value["assumptions"] = value["assumptions"][1:]
            continue
        if isinstance(value.get("decisions"), list) and len(value["decisions"]) > 1:
            value["decisions"] = value["decisions"][1:]
            continue
        goal = str(value.get("session_goal", ""))
        if len(goal) > 80:
            value["session_goal"] = goal[:80]
            continue
        break
    return value


def build_memory_context(session_id: str) -> str:
    """Build human-readable memory context string for LLM prompt injection."""
    memory = _load_memory(session_id)
    if not any([
        str(memory.get("session_goal", "")).strip(),
        memory.get("decisions"),
        memory.get("open_issues"),
        memory.get("assumptions"),
        memory.get("recent_turns"),
    ]):
        return ""
    
    payload = _trim_memory_to_budget(memory, max_tokens=MEMORY_MAX_TOKENS)
    
    # Format as human-readable text instead of raw JSON for better LLM comprehension.
    lines = []
    
    goal = str(payload.get("session_goal", "")).strip()
    if goal:
        lines.append(f"会话目标: {goal}")
    
    decisions = [d for d in payload.get("decisions", []) if isinstance(d, dict) and d.get("status") != "superseded"]
    if decisions:
        lines.append("\n重要决策:")
        for item in decisions:
            text = str(item.get("text", "")).strip()
            if text:
                lines.append(f"  - {text}")
    
    open_issues = [i for i in payload.get("open_issues", []) if isinstance(i, dict) and i.get("status") == "open"]
    if open_issues:
        lines.append("\n未决问题:")
        for item in open_issues:
            text = str(item.get("text", "")).strip()
            if text:
                lines.append(f"  - {text}")
    
    assumptions = [a for a in payload.get("assumptions", []) if isinstance(a, dict) and a.get("status") in ("active", "validated")]
    if assumptions:
        lines.append("\n背景假设:")
        for item in assumptions:
            text = str(item.get("text", "")).strip()
            status = item.get("status", "active")
            marker = "✓" if status == "validated" else "·"
            if text:
                lines.append(f"  {marker} {text}")

    recent_turns = [t for t in payload.get("recent_turns", []) if isinstance(t, dict)]
    if recent_turns:
        lines.append("\n最近3轮对话正文:")
        for idx, turn in enumerate(recent_turns[-3:], start=1):
            q = _strip_think_blocks(str(turn.get("question", "")))
            a = _strip_think_blocks(str(turn.get("answer", "")))
            if q:
                lines.append(f"  Q{idx}: {q}")
            if a:
                lines.append(f"  A{idx}: {a}")
    
    return "\n".join(lines).strip()


def _apply_memory_delta(base: dict[str, Any], delta: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    if str(delta.get("session_goal", "")).strip():
        merged["session_goal"] = str(delta.get("session_goal", "")).strip()

    decisions = merged.get("decisions", [])
    if not isinstance(decisions, list):
        decisions = []
    next_decision_id = max([int(item.get("id", 0)) for item in decisions if isinstance(item, dict)] + [0]) + 1
    for item in delta.get("decisions", []) if isinstance(delta.get("decisions", []), list) else []:
        if not isinstance(item, dict):
            continue
        text = str(item.get("text", "")).strip()
        status = str(item.get("status", "append")).strip().lower() or "append"
        if not text:
            continue
        record = {
            "id": int(item.get("id", next_decision_id)),
            "text": text,
            "status": "superseded" if status == "superseded" else "append",
            "created_at": datetime.now().isoformat(timespec="seconds"),
        }
        if record["id"] >= next_decision_id:
            next_decision_id = record["id"] + 1
        decisions.append(record)
    merged["decisions"] = decisions

    open_issues = merged.get("open_issues", [])
    if not isinstance(open_issues, list):
        open_issues = []
    for item in delta.get("open_issues", []) if isinstance(delta.get("open_issues", []), list) else []:
        if not isinstance(item, dict):
            continue
        text = str(item.get("text", "")).strip()
        status = str(item.get("status", "open")).strip().lower() or "open"
        if not text:
            continue
        open_issues.append({
            "text": text,
            "status": "closed" if status == "closed" else "open",
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        })
    merged["open_issues"] = open_issues

    assumptions = merged.get("assumptions", [])
    if not isinstance(assumptions, list):
        assumptions = []
    for item in delta.get("assumptions", []) if isinstance(delta.get("assumptions", []), list) else []:
        if not isinstance(item, dict):
            continue
        text = str(item.get("text", "")).strip()
        status = str(item.get("status", "active")).strip().lower() or "active"
        if not text:
            continue
        if status not in {"active", "validated", "invalidated"}:
            status = "active"
        assumptions.append({
            "text": text,
            "status": status,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        })
    merged["assumptions"] = assumptions
    merged["version"] = int(merged.get("version", 1)) + 1
    return merged


def _default_memory_delta(existing: dict[str, Any]) -> dict[str, Any]:
    return {
        "session_goal": str(existing.get("session_goal", "")).strip(),
        "decisions": [],
        "open_issues": [],
        "assumptions": [],
    }


def _parse_memory_delta_text(raw: str) -> dict[str, Any] | None:
    text = str(raw or "").strip()
    if not text:
        return None

    # Try direct JSON first.
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass

    # Common fallback: model wraps JSON in markdown code fences.
    fence_match = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", text, re.IGNORECASE)
    if fence_match:
        candidate = fence_match.group(1).strip()
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass

    # Last fallback: extract the first JSON object span.
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        candidate = text[start : end + 1]
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return None
    return None


def _generate_memory_delta(session_id: str, question: str, answer: str) -> dict[str, Any]:
    existing = _load_memory(session_id)
    prompt = {
        "call_type": "memory_update",
        "instruction": (
            "你是会话记忆摘要器。分析最新问答，提取关键要素更新结构化记忆。\n"
            "注意：这是滚动摘要，不是问答历史记录！只提取有长期价值的决策/问题/假设。"
        ),
        "field_definitions": {
            "session_goal": "会话的核心目标/探讨主题（一句话概括，随讨论深入可更新）",
            "decisions": "用户做出的重要决策、达成的结论、约定的事项（不要记录问答本身！）",
            "open_issues": "讨论中提出但尚未解决的问题（已回答的问题不记录）",
            "assumptions": "讨论中假定的前提条件、背景假设",
        },
        "update_rules": {
            "decisions": "新决策用 append；当新决策覆盖旧决策时，旧的标记 superseded",
            "open_issues": "新问题用 open；已解决的标记 closed",
            "assumptions": "新假设用 active；验证的标记 validated；推翻的标记 invalidated",
            "quality": "只提取有持续价值的信息，忽略无关紧要的细节",
            "max_tokens": MEMORY_MAX_TOKENS,
        },
        "examples": {
            "good_decision": "用户决定采用方案A而不是方案B",
            "bad_decision": "用户问了什么是XYZ（这是问答历史，不是决策）",
            "good_open_issue": "如何在预算内实现功能X",
            "good_assumption": "假设用户使用的是Windows系统",
        },
        "existing_memory": existing,
        "latest_turn": {"question": question, "answer": answer},
        "output_schema": {
            "session_goal": "string",
            "decisions": [{"text": "string", "status": "append|superseded"}],
            "open_issues": [{"text": "string", "status": "open|closed"}],
            "assumptions": [{"text": "string", "status": "active|validated|invalidated"}],
        },
    }

    if not LOCAL_LLM_API_URL or not LOCAL_LLM_MODEL:
        return _default_memory_delta(existing)

    try:
        import importlib

        openai_mod = importlib.import_module("openai")
        OpenAI = getattr(openai_mod, "OpenAI", None)
        if OpenAI is None:
            raise RuntimeError("openai.OpenAI unavailable")

        client = OpenAI(api_key=LOCAL_LLM_API_KEY, base_url=LOCAL_LLM_API_URL, timeout=int(DEFAULT_TIMEOUT))
        messages = [
            {
                "role": "system",
                "content": (
                    "你是会话记忆摘要器。任务：提取会话中的关键决策、问题、假设，构建滚动摘要。\n"
                    "重要：这不是问答历史记录器！只提取有长期价值的结构化信息。\n"
                    "输出纯 JSON，不要 markdown 代码块，不要解释文字。"
                ),
            },
            {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)},
        ]

        completion = None
        try:
            # Some local OpenAI-compatible servers reject json_object.
            completion = client.chat.completions.create(
                model=LOCAL_LLM_MODEL,
                messages=messages,
                temperature=0.1,
                response_format={"type": "json_object"},
            )
        except Exception as first_exc:
            if "response_format" not in str(first_exc).lower():
                raise
            completion = client.chat.completions.create(
                model=LOCAL_LLM_MODEL,
                messages=messages,
                temperature=0.1,
            )

        raw = completion.choices[0].message.content if completion and completion.choices else ""
        data = _parse_memory_delta_text(str(raw or ""))
        if isinstance(data, dict):
            return data
    except Exception as e:
        # Log the error for debugging instead of silently failing
        import traceback
        error_msg = f"Memory delta generation failed: {e}"
        try:
            import sys
            print(f"[MEMORY_ERROR] {error_msg}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
        except:
            pass

    return _default_memory_delta(existing)


def _memory_worker() -> None:
    while True:
        job = _MEMORY_QUEUE.get()
        try:
            sid = str(job.get("session_id", "")).strip()
            question = str(job.get("question", "")).strip()
            answer = str(job.get("answer", "")).strip()
            update_ts = str(job.get("update_ts", "")).strip()
            recent_turns = job.get("recent_turns", [])
            if not sid or not question or not answer:
                continue
            current = _load_memory(sid)

            current_ts = _parse_iso_timestamp(str(current.get("updated_at", "")))
            job_ts = _parse_iso_timestamp(update_ts)
            # Resolve concurrent updates by timestamp: newer updates win.
            if current_ts and job_ts and job_ts < current_ts:
                continue

            delta = _generate_memory_delta(sid, question, answer)
            merged = _apply_memory_delta(current, delta)
            merged["updated_at"] = update_ts or datetime.now().isoformat(timespec="seconds")
            merged["timestamp"] = merged["updated_at"]
            merged["recent_turns"] = recent_turns if isinstance(recent_turns, list) else _extract_recent_turns(sid, rounds=3)
            _save_memory(sid, merged)
        except Exception as e:
            # Log errors in memory worker for debugging
            import sys
            try:
                print(f"[MEMORY_WORKER_ERROR] Session {job.get('session_id', 'unknown')}: {e}", file=sys.stderr)
            except:
                pass
            continue


def _ensure_memory_worker() -> None:
    global _MEMORY_WORKER_STARTED
    if _MEMORY_WORKER_STARTED:
        return
    worker = threading.Thread(target=_memory_worker, daemon=True)
    worker.start()
    _MEMORY_WORKER_STARTED = True


def schedule_memory_update(session_id: str, question: str, answer: str) -> None:
    sid = (session_id or "").strip()
    q = (question or "").strip()
    a = (answer or "").strip()
    if not sid or not q or not a:
        return
    _ensure_memory_worker()
    update_ts = datetime.now().isoformat(timespec="seconds")
    _MEMORY_QUEUE.put({
        "session_id": sid,
        "question": q,
        "answer": a,
        "update_ts": update_ts,
        "recent_turns": _extract_recent_turns(sid, rounds=3),
    })


def _migrate_legacy_sessions_if_needed() -> None:
    if _iter_session_files():
        return
    legacy_aggregate = RAG_SESSIONS_DIR / "web_sessions.json"
    if legacy_aggregate.exists():
        try:
            payload = json.loads(legacy_aggregate.read_text(encoding="utf-8"))
            sessions = payload.get("sessions", []) if isinstance(payload, dict) else []
            if isinstance(sessions, list):
                for raw in sessions:
                    normalized = _normalize_session(raw)
                    if normalized is None:
                        continue
                    _save_session(normalized)
        except Exception:
            pass


def list_sessions() -> list[dict[str, Any]]:
    with _LOCK:
        _migrate_legacy_sessions_if_needed()
        sessions: list[dict[str, Any]] = []
        for path in _iter_session_files():
            data = _load_session_file(path)
            if data is not None:
                sessions.append(data)
        return sorted(sessions, key=lambda s: str(s.get("updated_at", "")), reverse=True)


def list_sessions_summary() -> list[dict[str, Any]]:
    """Return lightweight session metadata (no messages) for fast sidebar loading.

    The lock is held only for the migration check and to snapshot the file list.
    Individual file reads happen outside the lock so that concurrent append_message
    calls (which also hold _LOCK) are not blocked for the full iteration.
    """
    with _LOCK:
        _migrate_legacy_sessions_if_needed()
        paths = list(_iter_session_files())

    summaries: list[dict[str, Any]] = []
    for path in paths:
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(raw, dict):
            continue
        sid = str(raw.get("id", "")).strip()
        if not sid:
            continue
        messages = raw.get("messages", [])
        summaries.append({
            "id": sid,
            "title": sanitize_session_title(str(raw.get("title", "新会话")) or "新会话"),
            "created_at": str(raw.get("created_at", "") or ""),
            "updated_at": str(raw.get("updated_at", "") or ""),
            "title_locked": bool(raw.get("title_locked", False)),
            "message_count": len(messages) if isinstance(messages, list) else 0,
        })
    return sorted(summaries, key=lambda s: str(s.get("updated_at", "")), reverse=True)


def create_session(title: str = "新会话") -> dict[str, Any]:
    with _LOCK:
        now = datetime.now().isoformat(timespec="seconds")
        session = {
            "id": str(uuid4()),
            "title": sanitize_session_title(title or "新会话"),
            "created_at": now,
            "updated_at": now,
            "title_locked": False,
            "messages": [{"role": "系统", "text": "欢迎使用 RAG Q&A。"}],
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
    return _load_session_file(path)


def delete_session(session_id: str) -> bool:
    sid = (session_id or "").strip()
    if not sid:
        return False
    with _LOCK:
        session_path = _session_file_path(sid)
        if not session_path.exists():
            return False
        try:
            session_path.unlink(missing_ok=True)
        except Exception:
            return False
        try:
            _memory_file_path(sid).unlink(missing_ok=True)
        except Exception:
            pass
        return True


def append_message(session_id: str, role: str, text: str) -> None:
    append_message_with_trace(session_id, role, text, trace_id="")


def append_message_with_trace(session_id: str, role: str, text: str, trace_id: str = "") -> None:
    sid = (session_id or "").strip()
    if not sid:
        return
    with _LOCK:
        path = _session_file_path(sid)
        session = _load_session_file(path) if path.exists() else None
        if session is None:
            now = datetime.now().isoformat(timespec="seconds")
            session = {
                "id": sid,
                "title": "新会话",
                "created_at": now,
                "updated_at": now,
                "title_locked": False,
                "messages": [],
            }
        row = {"role": role, "text": text}
        normalized_trace_id = str(trace_id or "").strip()
        if normalized_trace_id:
            row["trace_id"] = normalized_trace_id
        session.setdefault("messages", []).append(row)
        session["updated_at"] = datetime.now().isoformat(timespec="seconds")
        _save_session(session)


def _post_stream_finalize_async(
    *,
    session_id: str,
    answer: str,
    question: str,
    trace_id: str,
    normalized_mode: str,
    payload: dict[str, Any],
    timings: dict[str, Any],
) -> None:
    def _run() -> None:
        try:
            if answer:
                append_message_with_trace(session_id, "助手", answer, trace_id=trace_id)
                schedule_memory_update(session_id, question, answer)

            is_deepseek_call = normalized_mode in {"deepseek", "reasoner"}
            web_count = int(payload.get("retrieved_web_count", 0) or 0)
            web_from_cache = str(timings.get("web_search_status", "")) == "cache_hit"
            _notify_nav_dashboard_quota(
                web_search_delta=1 if (web_count > 0 and not web_from_cache) else 0,
                deepseek_delta=1 if is_deepseek_call else 0,
            )
            schedule_generated_session_title(session_id, question, answer, lock=True)
        except Exception:
            return

    threading.Thread(target=_run, daemon=True, name=f"rag-finalize-{session_id[:8]}").start()


def set_session_title_if_unlocked(session_id: str, title: str, lock: bool = True) -> bool:
    sid = (session_id or "").strip()
    if not sid:
        return False
    new_title = sanitize_session_title(title)
    if not new_title:
        return False
    with _LOCK:
        path = _session_file_path(sid)
        if not path.exists():
            return False
        session = _load_session_file(path)
        if session is None:
            return False
        if bool(session.get("title_locked", False)):
            return False
        session["title"] = new_title
        session["title_locked"] = bool(lock)
        session["updated_at"] = datetime.now().isoformat(timespec="seconds")
        _save_session(session)
        return True


def set_session_title(session_id: str, title: str, lock: bool = True) -> dict[str, Any] | None:
    sid = (session_id or "").strip()
    if not sid:
        return None
    new_title = sanitize_session_title(title)
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
        session["updated_at"] = datetime.now().isoformat(timespec="seconds")
        _save_session(session)
        return session


def ask_rag(
    *,
    session_id: str,
    trace_id: str = "",
    mode: str,
    question: str,
    api_url: str | None,
    api_key: str | None,
    model: str | None,
    embedding_model: str | None,
    search_mode: str = "hybrid",
    top_k: int = 5,
    similarity_threshold: float = 0.45,
    debug: bool = False,
    no_embed_cache: bool = False,
    benchmark_mode: bool = False,
) -> dict[str, Any]:
    q = (question or "").strip()
    if not q:
        raise ValueError("问题不能为空")
    resolved_trace_id = _normalize_trace_id(trace_id)

    url = (api_url or "").strip() or DEFAULT_API_BASE_URL
    key = (api_key or "").strip() or DEFAULT_API_KEY
    mdl = resolve_chat_model(mode, model)
    emb = (embedding_model or "").strip() or resolve_embedding_model()

    normalized_mode = (mode or "").strip().lower() or "local"
    memory_context = build_memory_context(session_id)

    local_endpoint_is_safe = _is_loopback_url(LOCAL_LLM_API_URL) and not _is_deepseek_url(LOCAL_LLM_API_URL)
    use_local_llm = (
        normalized_mode == "local"
        and bool(LOCAL_LLM_API_URL and LOCAL_LLM_MODEL)
        and (local_endpoint_is_safe or ALLOW_REMOTE_LOCAL_LLM)
    )
    normalized_search_mode = _normalize_search_mode(search_mode)
    final_top_k = max(1, int(top_k))
    query_profile = _resolve_query_profile(
        q,
        base_threshold=float(similarity_threshold),
        base_vector_top_n=max(1, int(VECTOR_TOP_N)),
        base_top_k=final_top_k,
    )
    effective_similarity_threshold = float(query_profile["similarity_threshold"])
    effective_vector_top_n = min(max(1, int(MAX_VECTOR_CANDIDATES)), int(query_profile["vector_top_n"]))
    effective_top_k = int(query_profile["top_k"])
    rerank_top_k = max(1, min(int(RERANK_TOP_K), effective_top_k))
    enable_rerank_arg = "true" if ENABLE_RERANK else "false"

    if normalized_mode == "local" and not use_local_llm:
        # Pure local retrieval-only mode.
        payload = _ask_rag_local_without_api(q, effective_top_k, emb, normalized_search_mode)
        payload["trace_id"] = resolved_trace_id
        if debug:
            _write_debug_record(
                session_id,
                {
                    "timestamp": datetime.now().isoformat(timespec="seconds"),
                    "trace_id": resolved_trace_id,
                    "session_id": session_id,
                    "debug_enabled": True,
                    "source": "rag_service_local_only",
                    "question": q,
                    "search_mode": normalized_search_mode,
                    "similarity_threshold": float(effective_similarity_threshold),
                    "query_profile": query_profile,
                    "memory_context": memory_context,
                    "memory_tokens_est": _approx_tokens(memory_context),
                    "payload": payload,
                },
            )
        if LOCAL_LLM_API_URL and not (local_endpoint_is_safe or ALLOW_REMOTE_LOCAL_LLM):
            payload["answer"] = _prepend_fallback_notice(str(payload.get("answer", "")), _local_mode_disallow_notice())
            payload["degraded"] = True
            payload["degrade_reason"] = "local_mode_remote_llm_blocked"
        return payload

    if use_local_llm and not _is_local_llm_reachable(LOCAL_LLM_API_URL):
        notice = _local_fallback_notice()
        payload = _safe_build_local_fallback_payload(
            q,
            top_k,
            emb,
            notice,
            normalized_search_mode,
            normalized_mode,
            "local_llm_unreachable_preflight_fallback_failed",
        )
        if debug:
            _write_debug_record(
                session_id,
                {
                    "timestamp": datetime.now().isoformat(timespec="seconds"),
                    "trace_id": resolved_trace_id,
                    "session_id": session_id,
                    "debug_enabled": True,
                    "source": "rag_service_preflight_fallback_stream",
                    "question": q,
                    "search_mode": normalized_search_mode,
                    "reason": "local_llm_unreachable_preflight",
                    "local_llm_url": LOCAL_LLM_API_URL,
                    "payload": payload,
                },
            )
        return payload

    if use_local_llm and not _is_local_llm_reachable(LOCAL_LLM_API_URL):
        notice = _local_fallback_notice()
        payload = _safe_build_local_fallback_payload(
            q,
            top_k,
            emb,
            notice,
            normalized_search_mode,
            normalized_mode,
            "local_llm_unreachable_preflight_stream_fallback_failed",
        )
        if debug:
            _write_debug_record(
                session_id,
                {
                    "timestamp": datetime.now().isoformat(timespec="seconds"),
                    "trace_id": resolved_trace_id,
                    "session_id": session_id,
                    "debug_enabled": True,
                    "source": "rag_service_preflight_fallback",
                    "question": q,
                    "search_mode": normalized_search_mode,
                    "reason": "local_llm_unreachable_preflight",
                    "local_llm_url": LOCAL_LLM_API_URL,
                    "payload": payload,
                },
            )
        return payload

    if use_local_llm:
        # Local model generation via OpenAI-compatible endpoint.
        url = LOCAL_LLM_API_URL
        mdl = LOCAL_LLM_MODEL
        key = LOCAL_LLM_API_KEY

    if not url or not mdl:
        raise RuntimeError("请先配置 API_BASE_URL、API_KEY、MODEL")
    if normalized_mode in {"deepseek", "reasoner"} and not key:
        raise RuntimeError("DeepSeek/深度思考模式需要 API_KEY")

    runtime_args = _build_ask_rag_runtime_args(
        session_id=session_id,
        trace_id=resolved_trace_id,
        question=q,
        normalized_search_mode=normalized_search_mode,
        effective_top_k=effective_top_k,
        effective_vector_top_n=effective_vector_top_n,
        effective_similarity_threshold=effective_similarity_threshold,
        rerank_top_k=rerank_top_k,
        emb=emb,
        url=url,
        key=key,
        mdl=mdl,
        debug=debug,
        use_local_llm=use_local_llm,
        memory_context=memory_context,
        no_embed_cache=no_embed_cache,
        benchmark_mode=benchmark_mode,
        stream=False,
    )

    try:
        payload = ask_rag_script.run_rag_query(runtime_args)
    except Exception as exc:  # noqa: BLE001
        raw_error_text = str(exc)
        error_text = _humanize_error_message(raw_error_text)
        if debug:
            _write_debug_record(
                session_id,
                {
                    "timestamp": datetime.now().isoformat(timespec="seconds"),
                    "trace_id": resolved_trace_id,
                    "session_id": session_id,
                    "debug_enabled": True,
                    "source": "rag_service_inprocess_error",
                    "question": q,
                    "query": q,
                    "search_mode": normalized_search_mode,
                    "mode": normalized_mode,
                    "similarity_threshold": float(effective_similarity_threshold),
                    "vector_top_n": effective_vector_top_n,
                    "rerank_top_k": rerank_top_k,
                    "enable_rerank": bool(ENABLE_RERANK),
                    "reranker_model": RERANKER_MODEL,
                    "query_profile": query_profile,
                    "memory_context": memory_context,
                    "memory_tokens_est": _approx_tokens(memory_context),
                    "runtime_args": {
                        "search_mode": normalized_search_mode,
                        "top_k": effective_top_k,
                        "vector_top_n": effective_vector_top_n,
                        "rerank_top_k": rerank_top_k,
                        "timeout": 7200 if use_local_llm else int(DEFAULT_TIMEOUT),
                    },
                    "raw_error_text": raw_error_text,
                    "error_text": error_text,
                },
            )
        should_fallback_local = use_local_llm and (
            _is_local_llm_unavailable_error(raw_error_text)
            or _looks_like_generic_subprocess_error(raw_error_text)
        )
        if should_fallback_local:
            notice = _local_fallback_notice()
            fallback_payload = _safe_build_local_fallback_payload(
                q,
                top_k,
                emb,
                notice,
                normalized_search_mode,
                normalized_mode,
                "local_llm_unavailable_and_fallback_failed",
            )
            fallback_payload["mode"] = normalized_mode
            fallback_payload["embedding_model"] = emb
            if debug:
                memory_tokens = _approx_tokens(memory_context)
                answer_tokens = _approx_tokens(str(fallback_payload.get("answer", "")))
                _write_debug_record(
                    session_id,
                    {
                        "timestamp": datetime.now().isoformat(timespec="seconds"),
                        "trace_id": resolved_trace_id,
                        "session_id": session_id,
                        "debug_enabled": True,
                        "source": "rag_service_local_fallback",
                        "question": q,
                        "search_mode": normalized_search_mode,
                        "similarity_threshold": float(effective_similarity_threshold),
                        "query_profile": query_profile,
                        "memory_context": memory_context,
                        "memory_tokens_est": memory_tokens,
                        "output_tokens_est": answer_tokens,
                        "payload": fallback_payload,
                    },
                )
            return fallback_payload
        raise RuntimeError(error_text) from exc

    payload["trace_id"] = str(payload.get("trace_id") or resolved_trace_id)
    payload["embedding_model"] = emb
    payload["mode"] = normalized_mode
    payload["query_profile"] = query_profile
    payload["similarity_threshold"] = float(effective_similarity_threshold)
    payload["vector_top_n"] = int(payload.get("vector_top_n") or effective_vector_top_n)
    _timings_dict = payload.get("timings", {}) if isinstance(payload.get("timings"), dict) else {}
    _no_context, _no_context_reason = _resolve_no_context(_timings_dict)
    if not benchmark_mode:
        record_retrieval_metrics(
            source="rag_qa",
            search_mode=normalized_search_mode,
            query_profile=str(query_profile.get("profile") or "medium"),
            token_count=int(query_profile.get("token_count") or 0),
            timings=_timings_dict,
            elapsed_seconds=float(payload.get("elapsed_seconds") or 0),
            embed_cache_hit=1 if float(_timings_dict.get("embed_cache_hit") or 0) > 0 else 0,
            web_cache_hit=1 if str(_timings_dict.get("web_search_status", "")) == "cache_hit" else 0,
            no_context=_no_context,
            no_context_reason=_no_context_reason,
            trace_id=resolved_trace_id,
            similarity_threshold=float(effective_similarity_threshold),
            top1_score_before_rerank=float(_timings_dict["local_top1_score"]) if _timings_dict.get("local_top1_score") is not None else None,
            top1_score_after_rerank=float(_timings_dict["local_top1_vector_score_after_rerank"]) if _timings_dict.get("local_top1_vector_score_after_rerank") is not None else None,
            top1_rerank_score_after_rerank=float(_timings_dict["local_top1_rerank_score_after_rerank"]) if _timings_dict.get("local_top1_rerank_score_after_rerank") is not None else None,
            top1_identity_changed=int(_timings_dict["local_top1_identity_changed"]) if _timings_dict.get("local_top1_identity_changed") is not None else None,
            top1_rank_shift=float(_timings_dict["local_top1_rank_shift"]) if _timings_dict.get("local_top1_rank_shift") is not None else None,
        )
    _write_rag_trace_record(
        _build_rag_trace_record(
            trace_id=resolved_trace_id,
            session_id=session_id,
            source="benchmark_rag" if benchmark_mode else "rag_qa",
            mode=normalized_mode,
            search_mode=normalized_search_mode,
            query_profile=query_profile,
            similarity_threshold=float(effective_similarity_threshold),
            payload=payload,
            timings=_timings_dict,
            no_context=_no_context,
            no_context_reason=_no_context_reason,
        )
    )
    if _no_context and not benchmark_mode:
        try:
            from cache_db import log_no_context_query
            log_no_context_query(
                q,
                source="rag_qa",
                top1_score=float(_timings_dict.get("local_top1_score") or 0),
                threshold=float(effective_similarity_threshold),
                trace_id=resolved_trace_id,
                reason=_no_context_reason,
            )
        except Exception:
            pass
    if debug:
        debug_trace = payload.get("debug_trace") if isinstance(payload.get("debug_trace"), dict) else {}
        _write_debug_record(
            session_id,
            {
                "timestamp": datetime.now().isoformat(timespec="seconds"),
                "trace_id": resolved_trace_id,
                "session_id": session_id,
                "debug_enabled": True,
                "source": "rag_service",
                "question": q,
                "query": q,
                "search_mode": normalized_search_mode,
                "similarity_threshold": float(effective_similarity_threshold),
                "query_profile": query_profile,
                "trace": debug_trace,
                "payload_summary": {
                    "retrieved_local_count": payload.get("retrieved_local_count"),
                    "retrieved_local_vector_candidates": payload.get("retrieved_local_vector_candidates"),
                    "retrieved_web_count": payload.get("retrieved_web_count"),
                    "vector_top_n": payload.get("vector_top_n"),
                    "rerank_top_k": payload.get("rerank_top_k"),
                    "timings": payload.get("timings"),
                },
            },
        )
    return payload


def ask_rag_stream(
    *,
    session_id: str,
    trace_id: str = "",
    mode: str,
    question: str,
    api_url: str | None,
    api_key: str | None,
    model: str | None,
    embedding_model: str | None,
    search_mode: str = "hybrid",
    top_k: int = 5,
    similarity_threshold: float = 0.45,
    debug: bool = False,
    confirm_over_quota: bool = False,
    no_embed_cache: bool = False,
    benchmark_mode: bool = False,
) -> Iterator[dict[str, Any]]:
    q = (question or "").strip()
    if not q:
        raise ValueError("问题不能为空")
    resolved_trace_id = _normalize_trace_id(trace_id)

    url = (api_url or "").strip() or DEFAULT_API_BASE_URL
    key = (api_key or "").strip() or DEFAULT_API_KEY
    mdl = resolve_chat_model(mode, model)
    emb = (embedding_model or "").strip() or resolve_embedding_model()

    normalized_mode = (mode or "").strip().lower() or "local"

    # Pre-flight quota check (only for API-consuming modes)
    if not confirm_over_quota and normalized_mode not in {"local"}:
        exceeded = _check_rag_quota_exceeded(normalized_mode, search_mode)
        if exceeded:
            parts = []
            for exc in exceeded:
                kind_label = "Web搜索" if exc["kind"] == "web_search" else "DeepSeek"
                parts.append(f"{kind_label}（今日已用 {exc['current']}/{exc['limit']}）")
            msg = "今日 API 配额已达上限：" + "、".join(parts) + "。是否仍要继续调用？"
            yield {"type": "quota_exceeded", "trace_id": resolved_trace_id, "message": msg, "exceeded": exceeded}
            return

    memory_context = build_memory_context(session_id)

    local_endpoint_is_safe = _is_loopback_url(LOCAL_LLM_API_URL) and not _is_deepseek_url(LOCAL_LLM_API_URL)
    use_local_llm = (
        normalized_mode == "local"
        and bool(LOCAL_LLM_API_URL and LOCAL_LLM_MODEL)
        and (local_endpoint_is_safe or ALLOW_REMOTE_LOCAL_LLM)
    )
    normalized_search_mode = _normalize_search_mode(search_mode)
    final_top_k = max(1, int(top_k))
    query_profile = _resolve_query_profile(
        q,
        base_threshold=float(similarity_threshold),
        base_vector_top_n=max(1, int(VECTOR_TOP_N)),
        base_top_k=final_top_k,
    )
    effective_similarity_threshold = float(query_profile["similarity_threshold"])
    effective_vector_top_n = min(max(1, int(MAX_VECTOR_CANDIDATES)), int(query_profile["vector_top_n"]))
    effective_top_k = int(query_profile["top_k"])
    rerank_top_k = max(1, min(int(RERANK_TOP_K), effective_top_k))
    enable_rerank_arg = "true" if ENABLE_RERANK else "false"

    if normalized_mode == "local" and not use_local_llm:
        # Pure local retrieval-only mode.
        try:
            yield {"type": "progress", "trace_id": resolved_trace_id, "message": "正在检索并生成本地回答..."}
            payload = _ask_rag_local_without_api(q, top_k, emb, normalized_search_mode)
            payload["trace_id"] = resolved_trace_id
            if debug:
                _write_debug_record(
                    session_id,
                    {
                        "timestamp": datetime.now().isoformat(timespec="seconds"),
                        "trace_id": resolved_trace_id,
                        "session_id": session_id,
                        "debug_enabled": True,
                        "source": "rag_service_stream_local_only",
                        "question": q,
                        "search_mode": normalized_search_mode,
                        "similarity_threshold": float(effective_similarity_threshold),
                        "query_profile": query_profile,
                        "memory_context": memory_context,
                        "memory_tokens_est": _approx_tokens(memory_context),
                        "payload": payload,
                    },
                )
            if LOCAL_LLM_API_URL and not (local_endpoint_is_safe or ALLOW_REMOTE_LOCAL_LLM):
                payload["answer"] = _prepend_fallback_notice(str(payload.get("answer", "")), _local_mode_disallow_notice())
                payload["degraded"] = True
                payload["degrade_reason"] = "local_mode_remote_llm_blocked"
            answer = str(payload.get("answer", "")).strip()
            used_docs = payload.get("used_context_docs", [])
            docs = used_docs if isinstance(used_docs, list) else []
            answer = format_answer_with_refs(answer, docs, mode=normalized_mode, citation_map=payload.get("citation_map") if isinstance(payload.get("citation_map"), dict) else None)
            payload["answer"] = answer
            payload["mode"] = normalized_mode
            payload["embedding_model"] = emb

            if answer:
                # Use larger chunks so headings/lists are less likely to be split mid-line.
                chunks = [answer[i : i + 220] for i in range(0, len(answer), 220)]
                for chunk in chunks:
                    yield {"type": "chunk", "trace_id": resolved_trace_id, "text": chunk}

            payload["session_id"] = session_id
            yield {"type": "done", "trace_id": resolved_trace_id, "payload": payload}
            _post_stream_finalize_async(
                session_id=session_id,
                answer=answer,
                question=q,
                trace_id=resolved_trace_id,
                normalized_mode=normalized_mode,
                payload=payload,
                timings={},
            )
            return
        except Exception as exc:
            error_message = _humanize_error_message(str(exc) or "本地检索失败")
            yield {"type": "error", "trace_id": resolved_trace_id, "message": error_message}
        return

    if use_local_llm:
        # Local model generation via OpenAI-compatible endpoint.
        url = LOCAL_LLM_API_URL
        mdl = LOCAL_LLM_MODEL
        key = LOCAL_LLM_API_KEY

    if not url or not mdl:
        raise RuntimeError("请先配置 API_BASE_URL、API_KEY、MODEL")
    if normalized_mode in {"deepseek", "reasoner"} and not key:
        raise RuntimeError("DeepSeek/深度思考模式需要 API_KEY")

    runtime_args = _build_ask_rag_runtime_args(
        session_id=session_id,
        trace_id=resolved_trace_id,
        question=q,
        normalized_search_mode=normalized_search_mode,
        effective_top_k=effective_top_k,
        effective_vector_top_n=effective_vector_top_n,
        effective_similarity_threshold=effective_similarity_threshold,
        rerank_top_k=rerank_top_k,
        emb=emb,
        url=url,
        key=key,
        mdl=mdl,
        debug=debug,
        use_local_llm=use_local_llm,
        memory_context=memory_context,
        no_embed_cache=no_embed_cache,
        benchmark_mode=benchmark_mode,
        stream=True,
    )

    collected_chunks: list[str] = []
    event_queue: queue.Queue[dict[str, Any]] = queue.Queue()
    cancel_event = threading.Event()

    def _progress_callback(message: str) -> None:
        if cancel_event.is_set():
            raise RAGTaskAborted("已中止")
        event_queue.put({"kind": "progress", "message": str(message or "")})

    def _stream_callback(text: str) -> None:
        if cancel_event.is_set():
            raise RAGTaskAborted("已中止")
        event_queue.put({"kind": "chunk", "text": str(text or "")})

    def _worker() -> None:
        try:
            payload = ask_rag_script.run_rag_query(
                runtime_args,
                progress_callback=_progress_callback,
                stream_callback=_stream_callback,
            )
            event_queue.put({"kind": "done", "payload": payload})
        except RAGTaskAborted:
            event_queue.put({"kind": "aborted"})
        except Exception as exc:  # noqa: BLE001
            event_queue.put({"kind": "error", "message": str(exc)})

    worker = threading.Thread(target=_worker, daemon=True, name=f"rag-stream-{session_id[:8]}")
    _register_active_request(session_id, cancel_event=cancel_event, thread=worker)
    worker.start()

    try:
        last_heartbeat = time.monotonic()
        last_progress_message = "正在启动 RAG 进程内执行..."
        last_progress_started_at = time.monotonic()
        yield {"type": "progress", "trace_id": resolved_trace_id, "message": last_progress_message}

        while True:
            if cancel_event.is_set():
                partial = "".join(collected_chunks).strip()
                if partial:
                    append_message_with_trace(session_id, "助手", partial + "\n\n_[已中止]_", trace_id=resolved_trace_id)
                yield {"type": "aborted", "trace_id": resolved_trace_id}
                return

            try:
                event = event_queue.get(timeout=0.2)
            except queue.Empty:
                if not worker.is_alive():
                    break
                if time.monotonic() - last_heartbeat >= 0.8:
                    yield {"type": "progress", "trace_id": resolved_trace_id, "message": _heartbeat_progress_message(last_progress_message, last_progress_started_at)}
                    last_heartbeat = time.monotonic()
                continue

            kind = str(event.get("kind") or "")
            if kind == "progress":
                message = str(event.get("message") or "").strip()
                if not message:
                    continue
                last_progress_message = message
                last_progress_started_at = time.monotonic()
                last_heartbeat = time.monotonic()
                yield {"type": "progress", "trace_id": resolved_trace_id, "message": message}
                continue

            if kind == "chunk":
                text = str(event.get("text") or "")
                if not text:
                    continue
                collected_chunks.append(text)
                last_heartbeat = time.monotonic()
                yield {"type": "chunk", "trace_id": resolved_trace_id, "text": text}
                continue

            if kind == "aborted":
                partial = "".join(collected_chunks).strip()
                if partial:
                    append_message_with_trace(session_id, "助手", partial + "\n\n_[已中止]_", trace_id=resolved_trace_id)
                yield {"type": "aborted", "trace_id": resolved_trace_id}
                return

            if kind == "error":
                raw_error_text = str(event.get("message") or "")
                error_text = _humanize_error_message(raw_error_text or "未知错误")
                should_fallback_local = use_local_llm and (
                    _is_local_llm_unavailable_error(raw_error_text)
                    or _looks_like_generic_subprocess_error(raw_error_text)
                )
                if should_fallback_local:
                    notice = _local_fallback_notice()
                    yield {"type": "progress", "trace_id": resolved_trace_id, "message": "本地大模型不可用，正在降级为检索回答..."}
                    fallback_payload = _safe_build_local_fallback_payload(
                        q,
                        top_k,
                        emb,
                        notice,
                        normalized_search_mode,
                        normalized_mode,
                        "stream_local_llm_unavailable_and_fallback_failed",
                    )
                    fallback_answer = str(fallback_payload.get("answer", "")).strip()
                    fallback_docs = fallback_payload.get("used_context_docs", [])
                    docs = fallback_docs if isinstance(fallback_docs, list) else []
                    fallback_answer = format_answer_with_refs(fallback_answer, docs, mode=normalized_mode)
                    fallback_payload["answer"] = fallback_answer
                    fallback_payload["mode"] = normalized_mode
                    fallback_payload["embedding_model"] = emb
                    fallback_payload["session_id"] = session_id
                    fallback_payload["trace_id"] = resolved_trace_id
                    if debug:
                        _write_debug_record(
                            session_id,
                            {
                                "timestamp": datetime.now().isoformat(timespec="seconds"),
                                "trace_id": resolved_trace_id,
                                "session_id": session_id,
                                "debug_enabled": True,
                                "source": "rag_service_stream_local_fallback",
                                "question": q,
                                "search_mode": normalized_search_mode,
                                "similarity_threshold": float(effective_similarity_threshold),
                                "query_profile": query_profile,
                                "payload": fallback_payload,
                            },
                        )
                    if fallback_answer:
                        fallback_chunks = [fallback_answer[i : i + 220] for i in range(0, len(fallback_answer), 220)]
                        for chunk in fallback_chunks:
                            yield {"type": "chunk", "trace_id": resolved_trace_id, "text": chunk}
                    _post_stream_finalize_async(
                        session_id=session_id,
                        answer=fallback_answer,
                        question=q,
                        trace_id=resolved_trace_id,
                        normalized_mode=normalized_mode,
                        payload=fallback_payload,
                        timings=fallback_payload.get("timings", {}) if isinstance(fallback_payload.get("timings"), dict) else {},
                    )
                    yield {"type": "done", "trace_id": resolved_trace_id, "payload": fallback_payload}
                    return

                partial = "".join(collected_chunks).strip()
                if partial:
                    append_message_with_trace(session_id, "助手", partial + "\n\n---\n\n_[输出中断]_", trace_id=resolved_trace_id)
                append_message(session_id, "系统", f"RAG Q&A 失败：{error_text}")
                yield {"type": "error", "trace_id": resolved_trace_id, "message": error_text}
                return

            if kind == "done":
                payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
                payload["trace_id"] = str(payload.get("trace_id") or resolved_trace_id)
                answer = str(payload.get("answer", "")).strip() or "".join(collected_chunks).strip()
                used_docs = payload.get("used_context_docs", [])
                docs = used_docs if isinstance(used_docs, list) else []
                answer = format_answer_with_refs(answer, docs, mode=normalized_mode, citation_map=payload.get("citation_map") if isinstance(payload.get("citation_map"), dict) else None)

                if answer and not collected_chunks:
                    synthetic_chunks = [answer[i : i + 240] for i in range(0, len(answer), 240)]
                    for chunk in synthetic_chunks:
                        yield {"type": "chunk", "trace_id": resolved_trace_id, "text": chunk}

                payload["answer"] = answer
                payload["mode"] = normalized_mode
                payload["embedding_model"] = emb
                payload["query_profile"] = query_profile
                payload["similarity_threshold"] = float(effective_similarity_threshold)
                payload["vector_top_n"] = int(payload.get("vector_top_n") or effective_vector_top_n)
                payload["session_id"] = session_id
                _timings_dict = payload.get("timings", {}) if isinstance(payload.get("timings"), dict) else {}
                _no_context, _no_context_reason = _resolve_no_context(_timings_dict)
                if not benchmark_mode:
                    record_retrieval_metrics(
                        source="rag_qa_stream",
                        search_mode=normalized_search_mode,
                        query_profile=str(query_profile.get("profile") or "medium"),
                        token_count=int(query_profile.get("token_count") or 0),
                        timings=_timings_dict,
                        elapsed_seconds=float(payload.get("elapsed_seconds") or 0),
                        embed_cache_hit=1 if float(_timings_dict.get("embed_cache_hit") or 0) > 0 else 0,
                        web_cache_hit=1 if str(_timings_dict.get("web_search_status", "")) == "cache_hit" else 0,
                        no_context=_no_context,
                        no_context_reason=_no_context_reason,
                        trace_id=resolved_trace_id,
                        similarity_threshold=float(effective_similarity_threshold),
                        top1_score_before_rerank=float(_timings_dict["local_top1_score"]) if _timings_dict.get("local_top1_score") is not None else None,
                        top1_score_after_rerank=float(_timings_dict["local_top1_vector_score_after_rerank"]) if _timings_dict.get("local_top1_vector_score_after_rerank") is not None else None,
                        top1_rerank_score_after_rerank=float(_timings_dict["local_top1_rerank_score_after_rerank"]) if _timings_dict.get("local_top1_rerank_score_after_rerank") is not None else None,
                        top1_identity_changed=int(_timings_dict["local_top1_identity_changed"]) if _timings_dict.get("local_top1_identity_changed") is not None else None,
                        top1_rank_shift=float(_timings_dict["local_top1_rank_shift"]) if _timings_dict.get("local_top1_rank_shift") is not None else None,
                    )
                _write_rag_trace_record(
                    _build_rag_trace_record(
                        trace_id=resolved_trace_id,
                        session_id=session_id,
                        source="benchmark_rag" if benchmark_mode else "rag_qa_stream",
                        mode=normalized_mode,
                        search_mode=normalized_search_mode,
                        query_profile=query_profile,
                        similarity_threshold=float(effective_similarity_threshold),
                        payload=payload,
                        timings=_timings_dict,
                        no_context=_no_context,
                        no_context_reason=_no_context_reason,
                    )
                )
                if _no_context and not benchmark_mode:
                    try:
                        from cache_db import log_no_context_query
                        log_no_context_query(
                            q,
                            source="rag_qa",
                            top1_score=float(_timings_dict.get("local_top1_score") or 0),
                            threshold=float(effective_similarity_threshold),
                            trace_id=resolved_trace_id,
                            reason=_no_context_reason,
                        )
                    except Exception:
                        pass
                if debug:
                    debug_trace = payload.get("debug_trace") if isinstance(payload.get("debug_trace"), dict) else {}
                    _write_debug_record(
                        session_id,
                        {
                            "timestamp": datetime.now().isoformat(timespec="seconds"),
                            "trace_id": resolved_trace_id,
                            "session_id": session_id,
                            "debug_enabled": True,
                            "source": "rag_service_stream",
                            "question": q,
                            "search_mode": normalized_search_mode,
                            "similarity_threshold": float(effective_similarity_threshold),
                            "query_profile": query_profile,
                            "trace": debug_trace,
                            "payload_summary": {
                                "retrieved_local_count": payload.get("retrieved_local_count"),
                                "retrieved_local_vector_candidates": payload.get("retrieved_local_vector_candidates"),
                                "retrieved_web_count": payload.get("retrieved_web_count"),
                                "vector_top_n": payload.get("vector_top_n"),
                                "rerank_top_k": payload.get("rerank_top_k"),
                                "timings": payload.get("timings"),
                            },
                        },
                    )

                yield {"type": "done", "trace_id": resolved_trace_id, "payload": payload}
                _post_stream_finalize_async(
                    session_id=session_id,
                    answer=answer,
                    question=q,
                    trace_id=resolved_trace_id,
                    normalized_mode=normalized_mode,
                    payload=payload,
                    timings=_timings_dict,
                )
                return

        error_message = _humanize_error_message("未知错误")
        partial = "".join(collected_chunks).strip()
        if partial:
            append_message_with_trace(session_id, "助手", partial + "\n\n---\n\n_[输出中断]_", trace_id=resolved_trace_id)
        append_message(session_id, "系统", f"RAG Q&A 失败：{error_message}")
        yield {"type": "error", "trace_id": resolved_trace_id, "message": error_message}
    finally:
        _, state = _clear_active_request(session_id)
        cancel_event.set()
        thread = state.get("thread") if isinstance(state, dict) else None
        if isinstance(thread, threading.Thread) and thread.is_alive():
            thread.join(timeout=0.1)


def abort_session(session_id: str) -> bool:
    sid = (session_id or "").strip()
    if not sid:
        return False
    with _PROCESS_LOCK:
        state = _ACTIVE_PROCESSES.get(sid)
        if not isinstance(state, dict):
            return False
        _ABORTED_SESSIONS.add(sid)
        cancel_event = state.get("cancel_event")
    if isinstance(cancel_event, threading.Event):
        cancel_event.set()
    return True
