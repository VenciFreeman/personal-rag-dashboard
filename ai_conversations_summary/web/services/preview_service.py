from __future__ import annotations

from typing import Any
from dataclasses import dataclass
from pathlib import Path

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


def read_markdown(relative_path: str) -> str:
    safe_rel = relative_path.replace("\\", "/").strip("/")
    path = (DOCUMENTS_DIR / safe_rel).resolve()
    if not str(path).startswith(str(DOCUMENTS_DIR.resolve())):
        raise ValueError("Invalid path")
    if not path.exists() or not path.is_file() or path.suffix.lower() != ".md":
        raise FileNotFoundError("Markdown file not found")
    return path.read_text(encoding="utf-8", errors="ignore")


def keyword_search(query: str, max_results: int = 50) -> list[PreviewHit]:
    q = query.strip().lower()
    if not q:
        return []

    results: list[PreviewHit] = []
    for file_path in _iter_markdown_files(DOCUMENTS_DIR):
        rel = file_path.relative_to(DOCUMENTS_DIR).as_posix()
        if rel.split("/", 1)[0].lower() in EXCLUDED_ROOT_DIRS:
            continue
        if q in rel.lower():
            results.append(PreviewHit(path=rel))
            continue
        try:
            text = file_path.read_text(encoding="utf-8", errors="ignore").lower()
        except Exception:
            continue
        if q in text:
            results.append(PreviewHit(path=rel))
        if len(results) >= max_results:
            break
    return results


def resolve_embedding_model() -> str:
    import os
    from pathlib import Path

    model = (
        os.getenv("LOCAL_EMBEDDING_MODEL", "").strip()
        or os.getenv("DEEPSEEK_EMBEDDING_MODEL", "").strip()
        or (DEFAULT_EMBEDDING_MODEL or "").strip()
        or "BAAI/bge-base-zh-v1.5"
    )
    if model and Path(model).is_absolute() and not Path(model).exists():
        # Keep preview search stable even when user/session env keeps stale paths.
        return "BAAI/bge-base-zh-v1.5"
    return model


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
