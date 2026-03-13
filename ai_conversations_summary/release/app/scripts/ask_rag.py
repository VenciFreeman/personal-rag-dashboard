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
from rag_vector_index import RAGIndexError, search_vector_index_with_diagnostics


DEEPSEEK_AUDIT_DIR = PROJECT_ROOT / "data" / "deepseek_api_audit"


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

    parser = argparse.ArgumentParser(description="RAG Q&A over local documents")
    parser.add_argument("--question", default="", help="Question text. If empty, reads from stdin.")
    parser.add_argument("--documents-dir", default=str(root_dir / "documents"), help="Documents directory")
    parser.add_argument("--index-dir", default=str(root_dir / "data" / "vector_db"), help="Vector index directory")
    parser.add_argument("--backend", default="faiss", choices=["auto", "faiss", "chroma"], help="Vector backend")
    parser.add_argument("--search-mode", default="hybrid", choices=["hybrid", "local_only"], help="Retrieval mode")
    parser.add_argument("--top-k", type=int, default=5, help="Top K retrieval count")
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
    parser.add_argument("--stream", action="store_true", help="Enable streaming output via stdout")
    parser.add_argument(
        "--allow-local-fallback",
        action="store_true",
        help="When local LLM endpoint is unavailable, degrade to retrieval-only local answer",
    )
    return parser.parse_args()


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
    return normalized[: max(1, int(max_results))], "ok"


def _build_topic_context_segment(item: dict[str, Any], index_no: int) -> tuple[str, dict[str, Any]]:
    rel = str(item.get("relative_path", "")).strip() or str(item.get("file_path", "")).strip() or "<unknown>"
    topic = str(item.get("topic", "")).strip() or "untitled-topic"
    score = float(item.get("score", 0.0))
    keywords = _normalize_keywords(item.get("keywords", []))
    embedding_text = str(item.get("embedding_text", "")).strip()

    summary_topics = ""
    for line in embedding_text.splitlines():
        lowered = line.lower().strip()
        if lowered.startswith("summary_topics:"):
            summary_topics = line.split(":", 1)[1].strip() if ":" in line else ""
            break

    kw_text = ", ".join(keywords[:12]) if keywords else ""
    body_lines = [
        f"[资料{index_no}] path={rel} score={score:.4f}",
        f"topic: {topic}",
    ]
    if kw_text:
        body_lines.append(f"keywords: {kw_text}")
    if summary_topics:
        body_lines.append(f"summary_topics: {summary_topics}")

    used_doc = {
        "path": rel,
        "score": score,
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

    question = (args.question or "").strip()
    if not question:
        question = sys.stdin.read().strip()
    if not question:
        raise RuntimeError("Question is empty")

    t0 = time.perf_counter()
    
    # Output progress for GUI streaming mode.
    if args.stream:
        print("PROGRESS: 正在检索相关文档...", flush=True)
    
    search_mode = (args.search_mode or "hybrid").strip().lower()
    if search_mode == "local_only":
        local_top_k = max(1, int(args.top_k))
        web_top_k = 0
    else:
        local_top_k = 3
        web_top_k = 3
    max_context_chars = max(1000, int(args.max_context_chars))

    results, timings = search_vector_index_with_diagnostics(
        query=question,
        documents_dir=Path(args.documents_dir),
        index_dir=Path(args.index_dir),
        top_k=local_top_k,
        backend=args.backend,
        build_if_missing=True,
        embedding_model=args.embedding_model,
        timeout=int(args.timeout),
    )

    if args.stream:
        print(f"PROGRESS: 已检索到 {len(results)} 个相关文档，正在加载上下文...", flush=True)
    
    web_t0 = time.perf_counter()
    web_results: list[dict[str, Any]] = []
    web_status = "disabled"
    if web_top_k > 0:
        tavily_key = os.getenv("TAVILY_API_KEY", "").strip() or (TAVILY_API_KEY or "").strip()
        web_results, web_status = _search_web_tavily(
            query=question,
            max_results=web_top_k,
            tavily_api_key=tavily_key,
        )
    hybrid_rows: list[dict[str, Any]] = []
    for item in results:
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

    context_text, used_docs = _load_context_hybrid(
        rows=hybrid_rows,
        documents_dir=Path(args.documents_dir),
        max_context_chars=max_context_chars,
        max_chars_per_doc=max(500, int(args.max_chars_per_doc)),
        similarity_threshold=float(args.similarity_threshold),
        context_mode=str(args.context_mode or "topic"),
    )
    timings["web_search_seconds"] = round(time.perf_counter() - web_t0, 3)
    timings["web_search_status"] = web_status
    
    if args.stream:
        filtered_count = len([r for r in results if float(r.get("score", 0.0)) >= float(args.similarity_threshold)])
        print(f"PROGRESS: 本地过滤后 {filtered_count}/{len(results)} 个文档，联网补充 {len(web_results)} 个结果，上下文已加载（{len(context_text)} 字符），正在生成回答...", flush=True)
    
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
        "session_title": session_title,
        "answer": answer,
        "retrieved_count": len(used_docs),
        "retrieved_local_count": len(results),
        "retrieved_web_count": len(web_results),
        "search_mode": search_mode,
        "used_context_docs": used_docs,
        "timings": timings,
        "elapsed_seconds": round(time.perf_counter() - t0, 3),
    }

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
