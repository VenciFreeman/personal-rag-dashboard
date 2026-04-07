from __future__ import annotations

from typing import Any
from dataclasses import dataclass
from pathlib import Path

from ai_conversations_summary.embedding_runtime import resolve_embedding_model as resolve_runtime_embedding_model
from web.config import DOCUMENTS_DIR, VECTOR_DB_DIR
from web.services.context import (
    DEFAULT_EMBEDDING_MODEL,
    DEFAULT_TIMEOUT,
    RAGIndexError,
    search_vector_index_with_diagnostics,
)

EXCLUDED_ROOT_DIRS = {"example", "examples"}


@dataclass
class PreviewHit:
    path: str
    score: float | None = None
    topic: str | None = None


def _iter_markdown_files(root: Path) -> list[Path]:
    files = [p for p in root.rglob("*.md") if p.is_file() and p.name != ".gitkeep"]
    return sorted(files, key=lambda p: p.as_posix().lower())


def build_documents_tree() -> dict[str, object]:
    def build_node(path: Path, is_root: bool = False) -> dict[str, object]:
        if path.is_file():
            return {"type": "file", "name": path.name, "path": path.relative_to(DOCUMENTS_DIR).as_posix()}

        children: list[dict[str, object]] = []
        dirs = sorted((p for p in path.iterdir() if p.is_dir()), key=lambda p: p.name.lower(), reverse=True)
        files = sorted((p for p in path.iterdir() if p.is_file()), key=lambda p: p.name.lower(), reverse=True)
        for child in [*dirs, *files]:
            if child.name == ".gitkeep":
                continue
            if is_root and child.is_dir() and child.name.lower() in EXCLUDED_ROOT_DIRS:
                continue
            children.append(build_node(child))
        rel = "" if path == DOCUMENTS_DIR else path.relative_to(DOCUMENTS_DIR).as_posix()
        return {"type": "dir", "name": path.name, "path": rel, "children": children}

    DOCUMENTS_DIR.mkdir(parents=True, exist_ok=True)
    return build_node(DOCUMENTS_DIR, is_root=True)


def build_directory_children(rel_path: str) -> list[dict[str, object]]:
    """Return the shallow (one-level) children of *rel_path* for lazy tree loading.

    Each dir entry carries ``has_children: bool`` instead of a populated
    ``children`` list so the caller can show a caret without a recursive scan.
    """
    DOCUMENTS_DIR.mkdir(parents=True, exist_ok=True)
    if rel_path:
        safe = rel_path.replace("\\", "/").strip("/")
        dir_path = (DOCUMENTS_DIR / safe).resolve()
        if not str(dir_path).startswith(str(DOCUMENTS_DIR.resolve())):
            return []
    else:
        dir_path = DOCUMENTS_DIR

    if not dir_path.is_dir():
        return []

    is_root = dir_path == DOCUMENTS_DIR
    dirs = sorted((p for p in dir_path.iterdir() if p.is_dir()), key=lambda p: p.name.lower(), reverse=True)
    files = sorted(
        (p for p in dir_path.iterdir() if p.is_file() and p.name != ".gitkeep"),
        key=lambda p: p.name.lower(),
        reverse=True,
    )

    children: list[dict[str, object]] = []
    for child in [*dirs, *files]:
        if child.name == ".gitkeep":
            continue
        if is_root and child.is_dir() and child.name.lower() in EXCLUDED_ROOT_DIRS:
            continue
        rel = child.relative_to(DOCUMENTS_DIR).as_posix()
        if child.is_dir():
            has_children = any(
                p for p in child.iterdir() if p.name != ".gitkeep"
            )
            children.append({"type": "dir", "name": child.name, "path": rel, "has_children": has_children})
        else:
            children.append({"type": "file", "name": child.name, "path": rel})
    return children


def read_markdown(relative_path: str) -> str:
    safe_rel = relative_path.replace("\\", "/").strip("/")
    path = (DOCUMENTS_DIR / safe_rel).resolve()
    if not str(path).startswith(str(DOCUMENTS_DIR.resolve())):
        raise ValueError("Invalid path")
    if not path.exists() or not path.is_file() or path.suffix.lower() != ".md":
        raise FileNotFoundError("Markdown file not found")
    return path.read_text(encoding="utf-8", errors="ignore")


def _bm25_tokenize(text: str) -> list[str]:
    """Tokenize text for BM25: ASCII word tokens + CJK character bigrams."""
    import re
    tokens: list[str] = []
    # ASCII word tokens (lowercased)
    tokens.extend(re.findall(r"[a-z0-9]+", text.lower()))
    # CJK character bigrams
    cjk_chars = re.findall(r"[\u4e00-\u9fff\u3400-\u4dbf\uf900-\ufaff]", text)
    tokens.extend(cjk_chars[i] + cjk_chars[i + 1] for i in range(len(cjk_chars) - 1))
    return tokens


def _bm25_score(
    query_tokens: list[str],
    doc_tokens: list[str],
    avgdl: float = 500.0,
    k1: float = 1.5,
    b: float = 0.75,
) -> float:
    """BM25 TF score (IDF=1 per term) for a single document."""
    if not query_tokens or not doc_tokens:
        return 0.0
    dl = len(doc_tokens)
    freq: dict[str, int] = {}
    for t in doc_tokens:
        freq[t] = freq.get(t, 0) + 1
    score = 0.0
    norm = k1 * (1.0 - b + b * dl / avgdl)
    for qt in set(query_tokens):
        tf = freq.get(qt, 0)
        if tf > 0:
            score += (tf * (k1 + 1.0)) / (tf + norm)
    return score


def keyword_search(query: str, max_results: int = 50) -> list[PreviewHit]:
    q = query.strip()
    if not q:
        return []

    query_tokens = _bm25_tokenize(q)
    if not query_tokens:
        return []

    # Collect all candidate files and their BM25 scores
    scored: list[tuple[float, str]] = []
    all_lengths: list[int] = []
    entries: list[tuple[str, list[str]]] = []

    for file_path in _iter_markdown_files(DOCUMENTS_DIR):
        rel = file_path.relative_to(DOCUMENTS_DIR).as_posix()
        if rel.split("/", 1)[0].lower() in EXCLUDED_ROOT_DIRS:
            continue
        try:
            text = file_path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        doc_tokens = _bm25_tokenize(rel + " " + text)
        all_lengths.append(len(doc_tokens))
        entries.append((rel, doc_tokens))

    avgdl = sum(all_lengths) / len(all_lengths) if all_lengths else 500.0

    for rel, doc_tokens in entries:
        s = _bm25_score(query_tokens, doc_tokens, avgdl=avgdl)
        if s > 0.0:
            scored.append((s, rel))

    scored.sort(reverse=True)
    return [PreviewHit(path=rel, score=round(s, 4)) for s, rel in scored[:max_results]]


def resolve_embedding_model() -> str:
    model = resolve_runtime_embedding_model(index_dir=VECTOR_DB_DIR)
    if model:
        return model
    return (DEFAULT_EMBEDDING_MODEL or "").strip() or "BAAI/bge-base-zh-v1.5"


def vector_search(query: str, *, top_k: int = 10, embedding_model: str | None = None) -> tuple[list[PreviewHit], dict[str, Any]]:
    model = (embedding_model or "").strip() or resolve_embedding_model()
    try:
        rows, timings = search_vector_index_with_diagnostics(
            query=query,
            documents_dir=DOCUMENTS_DIR,
            index_dir=VECTOR_DB_DIR,
            top_k=max(1, int(top_k)),
            backend="faiss",
            build_if_missing=True,
            embedding_model=model,
            timeout=int(DEFAULT_TIMEOUT),
        )
    except RAGIndexError as exc:
        raise RuntimeError(str(exc)) from exc

    hits: list[PreviewHit] = []
    for item in rows:
        rel = str(item.get("relative_path", "")).strip()
        if not rel:
            continue
        if rel.split("/", 1)[0].lower() in EXCLUDED_ROOT_DIRS:
            continue
        hits.append(
            PreviewHit(
                path=rel,
                score=float(item.get("score", 0.0)),
                topic=str(item.get("topic", "")).strip() or None,
            )
        )
    return hits, timings if isinstance(timings, dict) else {}
