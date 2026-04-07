"""Local embedding + vector index utilities for markdown knowledge base.

Design notes:
- Local-only embedding inference (offline-friendly).
- FAISS as primary backend, Chroma optional.
- CLI supports build/search/sync-missing operations.
"""

import argparse
import ast
import io
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from dataclasses import dataclass
from hashlib import sha1, sha256
from pathlib import Path
from typing import Any, Callable

WORKSPACE_ROOT = Path(__file__).resolve().parent.parent.parent
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

try:
    from ai_conversations_summary.scripts.api_config import EMBEDDING_MODEL, TIMEOUT
except ImportError:
    try:
        from scripts.api_config import EMBEDDING_MODEL, TIMEOUT
    except ImportError:
        from api_config import EMBEDDING_MODEL, TIMEOUT  # type: ignore[no-redef]
from ai_conversations_summary.embedding_runtime import resolve_embedding_model as resolve_runtime_embedding_model
from core_service.runtime_data import iter_core_runtime_data_roots
from ai_conversations_summary.runtime_paths import DATA_DIR, DOCUMENTS_DIR as DEFAULT_DOCUMENTS_DIR, VECTOR_DB_DIR as DEFAULT_VECTOR_DB_DIR


QUOTE_FIELD_RE = re.compile(r"^\s*>\s*-\s*\*\*(tags|categories)\*\*:\s*(.+?)\s*$", re.IGNORECASE)
TITLE_LINE_RE = re.compile(r"^\s*>\s*-\s*\*\*title\*\*:\s*\"?(.+?)\"?\s*$", re.IGNORECASE)
SUMMARY_LINE_RE = re.compile(r"^\s*>\s*-\s*\*\*summary\*\*:\s*\"?(.+?)\"?\s*$", re.IGNORECASE)
LABEL_HEADING_RE = re.compile(r"^\s*##\s*标签\s*$")
TOPIC_OVERVIEW_HEADING_RE = re.compile(r"^\s*#{1,3}\s*本文概览\s*$")
BULLET_LINE_RE = re.compile(r"^\s*[-*]\s+(.+?)\s*$")
TOPIC_PREFIX_RE = re.compile(r"^主题\s*\d+\s*[:：]\s*(.+)$")
MARKDOWN_H1_RE = re.compile(r"^\s*#\s+(.+?)\s*$")


class RAGIndexError(RuntimeError):
    pass


@dataclass
class TopicRecord:
    id: str
    file_path: str
    relative_path: str
    title: str
    summary: str
    topic: str
    keywords: list[str]
    embedding_text: str
    source_hash: str


_LOCAL_EMBED_MODEL_CACHE: dict[str, Any] = {}
_STDIO_FALLBACK_STREAMS: list[io.TextIOBase] = []


def _print_json_safe(payload: Any) -> None:
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    try:
        print(text)
    except UnicodeEncodeError:
        # Fallback for non-UTF8 Windows consoles (for example cp1252).
        print(json.dumps(payload, ensure_ascii=True, indent=2))


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_metadata_payload(meta_path: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if not meta_path.exists():
        return {}, []

    raw = json.loads(meta_path.read_text(encoding="utf-8"))
    if isinstance(raw, list):
        docs = [row for row in raw if isinstance(row, dict)]
        return {}, docs
    if isinstance(raw, dict):
        manifest = raw.get("manifest") if isinstance(raw.get("manifest"), dict) else {}
        docs_raw = raw.get("documents") if isinstance(raw.get("documents"), list) else []
        docs = [row for row in docs_raw if isinstance(row, dict)]
        return dict(manifest), docs

    raise RAGIndexError("metadata.json is invalid (expected list or object)")


def _write_metadata_payload(
    meta_path: Path,
    *,
    documents: list[dict[str, Any]],
    embedding_model: str,
    dimension: int,
    build_time: str | None = None,
    extra_manifest: dict[str, Any] | None = None,
) -> None:
    manifest: dict[str, Any] = {
        "format_version": 2,
        "embedding_model": str(embedding_model or "").strip(),
        "dimension": int(dimension),
        "build_time": str(build_time or _utc_now_iso()),
    }
    if isinstance(extra_manifest, dict):
        for key, value in extra_manifest.items():
            if key in {"embedding_model", "dimension", "build_time", "format_version"}:
                continue
            manifest[key] = value

    payload = {
        "manifest": manifest,
        "documents": documents,
    }
    meta_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _record_to_metadata_dict(record: TopicRecord, *, changed: bool = False) -> dict[str, Any]:
    return {
        "id": record.id,
        "file_path": record.file_path,
        "relative_path": record.relative_path,
        "title": record.title,
        "summary": record.summary,
        "topic": record.topic,
        "keywords": record.keywords,
        "embedding_text": record.embedding_text,
        "source_hash": record.source_hash,
        "changed": bool(changed),
    }


def _data_roots() -> list[Path]:
    roots: list[Path] = []
    seen: set[str] = set()
    for root in [DATA_DIR, *iter_core_runtime_data_roots()]:
        key = str(root)
        if key in seen:
            continue
        seen.add(key)
        roots.append(root)
    return roots


def _ensure_stdio_streams() -> None:
    # pythonw/no-console can leave stdio as None; some deps call .isatty() unguarded.
    if sys.stdout is None:
        stream = open(os.devnull, "w", encoding="utf-8", errors="ignore")
        _STDIO_FALLBACK_STREAMS.append(stream)
        sys.stdout = stream
    if sys.stderr is None:
        stream = open(os.devnull, "w", encoding="utf-8", errors="ignore")
        _STDIO_FALLBACK_STREAMS.append(stream)
        sys.stderr = stream


def _is_local_embedding_model(model: str) -> bool:
    text = (model or "").strip().lower()
    if not text:
        return False
    if text.startswith("local:"):
        return True

    local_markers = (
        "bge",
        "nomic-embed",
        "mxbai-embed",
        "gte-",
        "multilingual-e5",
        "intfloat/e5",
        "sentence-transformers/",
        "baai/",
    )
    return any(marker in text for marker in local_markers)


def _find_local_snapshot_path(model_name: str) -> Path | None:
    # Try to resolve HuggingFace-style cached snapshot under project data/local_models.
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
    # Accept multiple common folder layouts to reduce setup friction on offline systems.
    raw = model_name.strip()
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
        # 1) Keep full id as nested path: data/local_models/BAAI/bge-base-zh-v1.5
        candidates.append(local_models_root / raw.replace("\\", "/"))

        # 2) Keep full id as flattened path: data/local_models/BAAI--bge-base-zh-v1.5
        if "/" in raw:
            candidates.append(local_models_root / raw.replace("/", "--"))

        # 3) Use model leaf dir: data/local_models/bge-base-zh-v1.5
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


def _relocate_legacy_local_model_path(model_value: str) -> str:
    """Map stale absolute model paths to current workspace local_models when possible."""
    raw = (model_value or "").strip()
    if not raw:
        return ""

    path_text = raw.replace("\\", "/")
    lowered = path_text.lower()
    marker = "/data/local_models/"
    marker_index = lowered.find(marker)
    if marker_index < 0:
        return raw

    suffix = path_text[marker_index + len(marker) :].strip("/")
    if not suffix:
        return raw

    # Legacy paths may include extra project-specific prefixes before models--*.
    # Try both the full suffix and a trimmed suffix starting at models--*.
    suffix_candidates = [suffix]
    models_marker = "/models--"
    models_index = suffix.lower().find(models_marker)
    if models_index >= 0:
        trimmed = suffix[models_index + 1 :].strip("/")
        if trimmed and trimmed not in suffix_candidates:
            suffix_candidates.append(trimmed)

    for root in _data_roots():
        for suffix_item in suffix_candidates:
            remapped = (root / "local_models" / Path(suffix_item)).resolve()
            if remapped.exists():
                return str(remapped)

    # Keep original value if remapped location does not exist to avoid masking setup issues.
    return raw


def _normalize_embedding_model_value(model_value: str) -> str:
    raw = (model_value or "").strip()
    if not raw:
        return ""

    path_candidate = Path(raw)
    if path_candidate.is_absolute() and not path_candidate.exists():
        remapped = _relocate_legacy_local_model_path(raw)
        if remapped != raw:
            return remapped
        # If a stale absolute path points to a BGE model, fall back to the default id so
        # SentenceTransformer can resolve from local cache roots.
        lowered = raw.lower()
        if "bge-" in lowered:
            return "BAAI/bge-base-zh-v1.5"
        return raw
    return raw


def _parse_list_like(raw: str) -> list[str]:
    value = raw.strip()
    if not value:
        return []

    if value.startswith("["):
        try:
            parsed = ast.literal_eval(value)
            if isinstance(parsed, list):
                return [str(x).strip().strip('"\'') for x in parsed if str(x).strip()]
        except Exception:
            pass

    chunks = [x.strip().strip('"\'') for x in re.split(r"[,，]", value)]
    return [x for x in chunks if x]


def _extract_keywords(markdown_text: str) -> list[str]:
    lines = markdown_text.splitlines()
    values: list[str] = []

    for line in lines[:120]:
        m = QUOTE_FIELD_RE.match(line)
        if not m:
            continue
        values.extend(_parse_list_like(m.group(2)))

    label_idx = -1
    for i, line in enumerate(lines):
        if LABEL_HEADING_RE.match(line):
            label_idx = i
            break

    if label_idx >= 0:
        for line in lines[label_idx + 1 : label_idx + 30]:
            if line.strip().startswith("#"):
                break
            line = line.strip()
            if not line:
                continue
            values.extend(_parse_list_like(line))

    # Preserve insertion order while deduplicating.
    dedup: list[str] = []
    seen: set[str] = set()
    for v in values:
        key = v.lower()
        if key and key not in seen:
            dedup.append(v)
            seen.add(key)
    return dedup


def _extract_title(markdown_text: str) -> str:
    for line in markdown_text.splitlines()[:80]:
        m = TITLE_LINE_RE.match(line)
        if m:
            return m.group(1).strip()
    for line in markdown_text.splitlines()[:120]:
        h = MARKDOWN_H1_RE.match(line)
        if h:
            return h.group(1).strip()
    return ""


def _extract_summary(markdown_text: str) -> str:
    for line in markdown_text.splitlines()[:100]:
        m = SUMMARY_LINE_RE.match(line)
        if m:
            return m.group(1).strip()
    return ""


def _extract_topics(markdown_text: str) -> list[str]:
    lines = markdown_text.splitlines()
    start = -1
    for i, line in enumerate(lines):
        if TOPIC_OVERVIEW_HEADING_RE.match(line):
            start = i
            break

    topics: list[str] = []
    if start >= 0:
        for line in lines[start + 1 : start + 80]:
            if line.strip().startswith("#"):
                break
            m = BULLET_LINE_RE.match(line)
            if not m:
                continue
            item = m.group(1).strip()
            prefix = TOPIC_PREFIX_RE.match(item)
            if prefix:
                item = prefix.group(1).strip()
            if item:
                topics.append(item)

    if topics:
        return topics

    title = _extract_title(markdown_text)
    return [title] if title else ["untitled-topic"]


def _extract_embedding_body(markdown_text: str, *, max_chars: int = 4000) -> str:
    lines = markdown_text.splitlines()
    body_lines: list[str] = []
    for line in lines:
        raw = line.strip()
        if not raw:
            continue
        # Skip quoted metadata rows and markdown headings for cleaner semantic text.
        if raw.startswith(">"):
            continue
        if raw.startswith("#"):
            continue
        if raw.startswith("-") and "**" in raw:
            continue
        body_lines.append(raw)
    body = "\n".join(body_lines).strip()
    if max_chars > 0 and len(body) > max_chars:
        return body[:max_chars]
    return body


def _compute_source_hash(markdown_text: str) -> str:
    # Use raw file text hash to detect any source change that requires re-embedding.
    return sha256(markdown_text.encode("utf-8", errors="ignore")).hexdigest()


def build_topic_records(documents_dir: Path) -> list[TopicRecord]:
    # Convert each markdown file into one stable, searchable record.
    if not documents_dir.exists():
        raise FileNotFoundError(f"Documents directory not found: {documents_dir}")

    records: list[TopicRecord] = []
    for file_path in sorted(documents_dir.rglob("*.md")):
        if not file_path.is_file():
            continue

        text = file_path.read_text(encoding="utf-8", errors="ignore")
        keywords = _extract_keywords(text)
        title = _extract_title(text)
        topics = _extract_topics(text)
        rel = file_path.relative_to(documents_dir).as_posix()
        keyword_text = ", ".join(keywords)

        # Use a small set of overview topics as body summary to keep doc-level vectors stable.
        topic_summary = " | ".join(topics[:3]).strip()
        if not topic_summary:
            topic_summary = "untitled-topic"
        summary = _extract_summary(text) or topic_summary
        title = title or file_path.stem

        rec_id = sha1(rel.encode("utf-8")).hexdigest()
        emb_text = _extract_embedding_body(text, max_chars=4000)
        if not emb_text:
            emb_text = summary or topic_summary or title
        records.append(
            TopicRecord(
                id=rec_id,
                file_path=rel,
                relative_path=rel,
                title=title,
                summary=summary,
                topic=topic_summary,
                keywords=keywords,
                embedding_text=emb_text,
                source_hash=_compute_source_hash(text),
            )
        )

    return records


def _resolve_api_settings(
    api_url: str | None = None,
    api_key: str | None = None,
    embedding_model: str | None = None,
    index_dir: Path | None = None,
    timeout: int | None = None,
) -> tuple[str, str, str, int]:
    _ = api_url, api_key
    preferred_model = (
        embedding_model
        or os.getenv("LOCAL_EMBEDDING_MODEL")
        or os.getenv("DEEPSEEK_EMBEDDING_MODEL")
        or EMBEDDING_MODEL
    ).strip()
    resolved_model = resolve_runtime_embedding_model(preferred_model, index_dir=index_dir or DEFAULT_VECTOR_DB_DIR)
    resolved_model = _normalize_embedding_model_value(resolved_model)
    resolved_timeout = int(timeout if timeout is not None else TIMEOUT)

    if not resolved_model:
        raise RAGIndexError("Missing EMBEDDING_MODEL (or fallback MODEL) for embedding requests")
    if not _is_local_embedding_model(resolved_model):
        raise RAGIndexError(
            "Only local embedding models are supported now. "
            "Set environment variable LOCAL_EMBEDDING_MODEL (or DEEPSEEK_EMBEDDING_MODEL) to a local model, "
            "for example: BAAI/bge-base-zh-v1.5"
        )

    return "", "", resolved_model, resolved_timeout


def _embed_texts_local(
    texts: list[str],
    *,
    model: str,
    batch_size: int = 32,
) -> list[list[float]]:
    _ensure_stdio_streams()

    # ── Sidecar path: delegate to the embedding sidecar server if available ──
    _sidecar_url = os.environ.get("_RAG_EMBED_SIDECAR_URL", "").strip()
    if _sidecar_url:
        try:
            import json as _json
            from urllib import request as _urlreq

            body = _json.dumps({"model": model.strip(), "texts": texts}, ensure_ascii=False).encode("utf-8")
            req = _urlreq.Request(
                _sidecar_url.rstrip("/"),
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            opener = _urlreq.build_opener(_urlreq.ProxyHandler({}))
            with opener.open(req, timeout=60) as resp:
                result = _json.loads(resp.read().decode("utf-8"))
            if "error" in result:
                raise RuntimeError(result["error"])
            return result["vectors"]
        except Exception:
            pass  # fall through to local model load

    os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")
    os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
    # Keep embedding path strictly offline to avoid hidden network probes.
    os.environ.setdefault("HF_HUB_OFFLINE", "1")
    os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")

    model_name = model.strip()
    if model_name.lower().startswith("local:"):
        model_name = model_name.split(":", 1)[1].strip()
    if not model_name:
        raise RAGIndexError("Local embedding model is empty")

    try:
        from sentence_transformers import SentenceTransformer
    except ModuleNotFoundError as exc:
        raise RAGIndexError(
            "Missing dependency: sentence-transformers. Install with: pip install sentence-transformers"
        ) from exc

    encoder = _LOCAL_EMBED_MODEL_CACHE.get(model_name)
    if encoder is None:
        cache_dir = _data_roots()[0] / "hf_cache"
        cache_dir.mkdir(parents=True, exist_ok=True)

        # Strict local-only: never download from network.
        attempts: list[tuple[str, str | None]] = []

        # If user passes/keeps local folders, load from those candidate folders first.
        for local_dir in _find_local_model_dir_candidates(model_name):
            attempts.append((str(local_dir.resolve()), None))

        snapshot_path = _find_local_snapshot_path(model_name)
        if snapshot_path is not None:
            attempts.append((str(snapshot_path), None))

        # Try default HuggingFace cache first, then project cache.
        attempts.append((model_name, None))
        attempts.append((model_name, str(cache_dir)))
        for root in _data_roots():
            attempts.append((model_name, str(root / "local_models")))

        last_exc: Exception | None = None
        for model_ref, cache_folder in attempts:
            try:
                kwargs: dict[str, Any] = {
                    "local_files_only": True,
                }
                if cache_folder:
                    kwargs["cache_folder"] = cache_folder
                encoder = SentenceTransformer(model_ref, **kwargs)
                break
            except Exception as exc:  # noqa: BLE001
                last_exc = exc

        if encoder is None:
            detail = str(last_exc) if last_exc else "unknown error"
            raise RAGIndexError(
                f"Failed to load local embedding model '{model_name}' in offline mode: {detail}. "
                "Please pre-download the model to a local folder and set LOCAL_EMBEDDING_MODEL "
                "(or DEEPSEEK_EMBEDDING_MODEL) to that folder path."
            ) from last_exc

        _LOCAL_EMBED_MODEL_CACHE[model_name] = encoder

    try:
        vectors = encoder.encode(
            texts,
            batch_size=max(1, batch_size),
            convert_to_numpy=True,
            normalize_embeddings=False,
            show_progress_bar=False,
        )
    except Exception as exc:  # noqa: BLE001
        raise RAGIndexError(f"Local embedding inference failed with model '{model_name}': {exc}") from exc

    return vectors.tolist()


def _embed_texts(
    texts: list[str],
    *,
    api_url: str,
    api_key: str,
    model: str,
    timeout: int,
    batch_size: int = 64,
    _cache_stats: dict | None = None,
    no_cache: bool = False,
) -> list[list[float]]:
    _ = api_url, api_key, timeout
    # Try embedding cache first; fall through to inference on any failure.
    if no_cache:
        return _embed_texts_local(texts, model=model, batch_size=batch_size)
    try:
        from cache_db import get_embed_cache
        cache = get_embed_cache()
        cached_vecs = cache.get_batch(texts, model)
        results: list[list[float] | None] = list(cached_vecs)
        uncached_indices = [i for i, v in enumerate(results) if v is None]
        uncached_texts = [texts[i] for i in uncached_indices]
        if _cache_stats is not None:
            n_hit = sum(1 for v in results if v is not None)
            _cache_stats["embed_hits"] = _cache_stats.get("embed_hits", 0) + n_hit
            _cache_stats["embed_misses"] = _cache_stats.get("embed_misses", 0) + len(uncached_texts)
        if uncached_texts:
            new_vecs = _embed_texts_local(uncached_texts, model=model, batch_size=batch_size)
            cache.set_batch(uncached_texts, model, new_vecs)
            for i, vec in zip(uncached_indices, new_vecs):
                results[i] = vec
        return [v for v in results]  # type: ignore[return-value]
    except Exception:
        pass
    # Cache unavailable — fall back to direct inference.
    return _embed_texts_local(texts, model=model, batch_size=batch_size)


def _resolve_bulk_embedding_batch_size(model_name: str) -> int:
    raw = str(os.getenv("AI_SUMMARY_RAG_EMBED_BATCH_SIZE", "") or "").strip()
    if raw:
        try:
            return max(1, int(raw))
        except Exception:
            pass

    normalized = str(model_name or "").strip().lower()
    if "bge-base" in normalized:
        return 4
    return 16


def _select_backend(preferred: str) -> str:
    choice = preferred.lower()
    if choice not in {"auto", "faiss", "chroma"}:
        raise RAGIndexError(f"Unsupported backend: {preferred}")

    if choice in {"auto", "faiss"}:
        try:
            import faiss  # noqa: F401
            import numpy  # noqa: F401
            return "faiss"
        except Exception:
            if choice == "faiss":
                raise RAGIndexError("FAISS backend requested but faiss-cpu is not available")

    if choice in {"auto", "chroma"}:
        try:
            import chromadb  # noqa: F401
            return "chroma"
        except Exception:
            if choice == "chroma":
                raise RAGIndexError("Chroma backend requested but chromadb is not available")

    raise RAGIndexError("No vector backend available. Install either faiss-cpu or chromadb")


def _write_backend_meta(index_dir: Path, backend: str) -> None:
    index_dir.mkdir(parents=True, exist_ok=True)
    (index_dir / "backend.json").write_text(
        json.dumps({"backend": backend}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _save_faiss(index_dir: Path, records: list[TopicRecord], vectors: list[list[float]]) -> None:
    try:
        import faiss
        import numpy as np
    except ModuleNotFoundError as exc:
        raise RAGIndexError("FAISS backend requires faiss-cpu and numpy") from exc

    if not records:
        raise RAGIndexError("No topic records found to index")

    matrix = np.asarray(vectors, dtype="float32")
    if matrix.ndim != 2 or matrix.shape[0] != len(records):
        raise RAGIndexError("Embedding matrix shape mismatch")

    faiss.normalize_L2(matrix)
    index = faiss.IndexFlatIP(matrix.shape[1])
    index.add(matrix)

    index_dir.mkdir(parents=True, exist_ok=True)
    faiss.write_index(index, str(index_dir / "faiss.index"))
    metadata_docs = [_record_to_metadata_dict(r, changed=False) for r in records]
    _write_metadata_payload(
        index_dir / "metadata.json",
        documents=metadata_docs,
        embedding_model="",
        dimension=int(matrix.shape[1]),
    )
    _write_backend_meta(index_dir, "faiss")


def _load_faiss_index_and_metadata(index_dir: Path) -> tuple[Any, list[dict[str, Any]]]:
    try:
        import faiss
    except ModuleNotFoundError as exc:
        raise RAGIndexError("FAISS backend requires faiss-cpu") from exc

    index_path = index_dir / "faiss.index"
    meta_path = index_dir / "metadata.json"
    if not index_path.exists() or not meta_path.exists():
        raise RAGIndexError("FAISS index files missing (faiss.index/metadata.json)")

    index = faiss.read_index(str(index_path))
    _manifest, metadata_docs = _read_metadata_payload(meta_path)
    return index, metadata_docs


def get_missing_embedding_records(*, documents_dir: Path, index_dir: Path) -> tuple[list[TopicRecord], list[TopicRecord]]:
    all_records = build_topic_records(documents_dir)
    if not all_records:
        return [], []

    existing_paths: set[str] = set()
    meta_path = index_dir / "metadata.json"
    if meta_path.exists():
        try:
            _manifest, docs = _read_metadata_payload(meta_path)
            for item in docs:
                rel = str(item.get("relative_path", "")).strip()
                if rel:
                    existing_paths.add(rel)
        except Exception:
            pass

    missing_records = [r for r in all_records if r.relative_path not in existing_paths]
    return all_records, missing_records


def sync_missing_embeddings_to_faiss(
    *,
    documents_dir: Path,
    index_dir: Path,
    api_url: str | None = None,
    api_key: str | None = None,
    embedding_model: str | None = None,
    timeout: int | None = None,
) -> dict[str, Any]:
	# Incremental path: only embed documents not present in metadata.
    try:
        import faiss
        import numpy as np
    except ModuleNotFoundError as exc:
        raise RAGIndexError("Missing dependency: faiss-cpu/numpy") from exc

    all_records, missing_records = get_missing_embedding_records(
        documents_dir=documents_dir,
        index_dir=index_dir,
    )
    if not all_records:
        raise RAGIndexError(f"No markdown documents found under: {documents_dir}")

    resolved_url, resolved_key, resolved_model, resolved_timeout = _resolve_api_settings(
        api_url=api_url,
        api_key=api_key,
        embedding_model=embedding_model,
        index_dir=index_dir,
        timeout=timeout,
    )
    embedding_batch_size = _resolve_bulk_embedding_batch_size(resolved_model)

    index_dir.mkdir(parents=True, exist_ok=True)
    index_path = index_dir / "faiss.index"
    meta_path = index_dir / "metadata.json"

    manifest_before: dict[str, Any] = {}
    metadata_docs: list[dict[str, Any]] = []
    if meta_path.exists():
        try:
            manifest_before, metadata_docs = _read_metadata_payload(meta_path)
        except Exception:
            manifest_before, metadata_docs = {}, []

    # Keep one row per relative_path.
    current_meta_by_rel: dict[str, dict[str, Any]] = {}
    for row in metadata_docs:
        rel = str(row.get("relative_path") or "").strip()
        if rel:
            current_meta_by_rel[rel] = row

    missing_records = [r for r in all_records if r.relative_path not in current_meta_by_rel]
    changed_records = [
        r
        for r in all_records
        if r.relative_path in current_meta_by_rel
        and (
            str(current_meta_by_rel[r.relative_path].get("source_hash") or "") != r.source_hash
            or bool(current_meta_by_rel[r.relative_path].get("changed", False))
        )
    ]
    pending_records = [*missing_records, *changed_records]

    if not pending_records:
        # Backfill manifest even when there is no pending embedding work.
        dim = int(manifest_before.get("dimension") or 0)
        if dim <= 0 and index_path.exists():
            index_obj, _docs = _load_faiss_index_and_metadata(index_dir)
            dim = int(index_obj.d)
        if dim <= 0:
            dim = 768
        docs_final = []
        for r in all_records:
            existing = dict(current_meta_by_rel.get(r.relative_path, {}))
            existing.update(_record_to_metadata_dict(r, changed=False))
            docs_final.append(existing)
        _write_metadata_payload(
            meta_path,
            documents=docs_final,
            embedding_model=resolved_model,
            dimension=dim,
            extra_manifest={"records": len(docs_final)},
        )
        _write_backend_meta(index_dir, "faiss")
        return {
            "backend": "faiss",
            "documents_total": len(all_records),
            "missing_documents": 0,
            "changed_documents": 0,
            "added_documents": 0,
            "updated_documents": 0,
            "index_dir": str(index_dir),
            "missing_relative_paths": [],
            "changed_relative_paths": [],
        }

    vectors = _embed_texts(
        [r.embedding_text for r in pending_records],
        api_url=resolved_url,
        api_key=resolved_key,
        model=resolved_model,
        timeout=resolved_timeout,
        batch_size=embedding_batch_size,
    )

    matrix = np.asarray(vectors, dtype="float32")
    if matrix.ndim != 2 or matrix.shape[0] != len(pending_records):
        raise RAGIndexError("Embedding matrix shape mismatch when syncing missing documents")
    faiss.normalize_L2(matrix)

    pending_by_rel = {r.relative_path: r for r in pending_records}
    pending_vec_by_rel: dict[str, Any] = {}
    for rec, vec in zip(pending_records, matrix, strict=True):
        pending_vec_by_rel[rec.relative_path] = vec

    existing_index = None
    existing_docs: list[dict[str, Any]] = []
    old_vec_by_rel: dict[str, Any] = {}
    if index_path.exists() and meta_path.exists():
        existing_index, existing_docs = _load_faiss_index_and_metadata(index_dir)
        if existing_index.d != int(matrix.shape[1]):
            raise RAGIndexError(
                f"FAISS dimension mismatch: index has {existing_index.d}, new embeddings have {matrix.shape[1]}"
            )
        for i, row in enumerate(existing_docs):
            rel = str(row.get("relative_path") or "").strip()
            if rel:
                old_vec_by_rel[rel] = existing_index.reconstruct(int(i))

    final_docs: list[dict[str, Any]] = []
    final_vecs: list[Any] = []
    for r in all_records:
        rel = r.relative_path
        final_docs.append(_record_to_metadata_dict(r, changed=False))
        if rel in pending_vec_by_rel:
            final_vecs.append(pending_vec_by_rel[rel])
            continue
        if rel in old_vec_by_rel:
            final_vecs.append(old_vec_by_rel[rel])
            continue
        raise RAGIndexError(f"Missing vector for record during sync: {rel}")

    final_matrix = np.asarray(final_vecs, dtype="float32")
    if final_matrix.ndim != 2 or final_matrix.shape[0] != len(final_docs):
        raise RAGIndexError("Embedding matrix shape mismatch when rebuilding synced index")
    faiss.normalize_L2(final_matrix)
    new_index = faiss.IndexFlatIP(final_matrix.shape[1])
    new_index.add(final_matrix)

    faiss.write_index(new_index, str(index_path))
    _write_metadata_payload(
        meta_path,
        documents=final_docs,
        embedding_model=resolved_model,
        dimension=int(final_matrix.shape[1]),
        extra_manifest={"records": len(final_docs)},
    )
    _write_backend_meta(index_dir, "faiss")

    return {
        "backend": "faiss",
        "documents_total": len(all_records),
        "missing_documents": len(missing_records),
        "changed_documents": len(changed_records),
        "added_documents": len(missing_records),
        "updated_documents": len(changed_records),
        "index_dir": str(index_dir),
        "missing_relative_paths": [r.relative_path for r in missing_records],
        "changed_relative_paths": [r.relative_path for r in changed_records],
    }


def _save_chroma(index_dir: Path, records: list[TopicRecord], vectors: list[list[float]]) -> None:
    try:
        import chromadb
    except ModuleNotFoundError as exc:
        raise RAGIndexError("Chroma backend requires chromadb") from exc

    if not records:
        raise RAGIndexError("No topic records found to index")

    chroma_dir = index_dir / "chroma"
    chroma_dir.mkdir(parents=True, exist_ok=True)
    client = chromadb.PersistentClient(path=str(chroma_dir))

    collection_name = "docs_topics"
    try:
        client.delete_collection(collection_name)
    except Exception:
        pass
    collection = client.create_collection(collection_name)

    collection.add(
        ids=[r.id for r in records],
        embeddings=vectors,
        documents=[r.embedding_text for r in records],
        metadatas=[
            {
                "file_path": r.file_path,
                "relative_path": r.relative_path,
                "title": r.title,
                "summary": r.summary,
                "topic": r.topic,
                "keywords": "|".join(r.keywords),
                "embedding_text": r.embedding_text,
            }
            for r in records
        ],
    )
    _write_backend_meta(index_dir, "chroma")


def build_vector_index(
    *,
    documents_dir: Path,
    index_dir: Path,
    backend: str = "auto",
    api_url: str | None = None,
    api_key: str | None = None,
    embedding_model: str | None = None,
    timeout: int | None = None,
) -> dict[str, Any]:
    records = build_topic_records(documents_dir)
    if not records:
        raise RAGIndexError(f"No markdown documents found under: {documents_dir}")

    resolved_url, resolved_key, resolved_model, resolved_timeout = _resolve_api_settings(
        api_url=api_url,
        api_key=api_key,
        embedding_model=embedding_model,
        index_dir=index_dir,
        timeout=timeout,
    )
    selected_backend = _select_backend(backend)
    embedding_batch_size = _resolve_bulk_embedding_batch_size(resolved_model)

    vectors = _embed_texts(
        [r.embedding_text for r in records],
        api_url=resolved_url,
        api_key=resolved_key,
        model=resolved_model,
        timeout=resolved_timeout,
        batch_size=embedding_batch_size,
    )

    if selected_backend == "faiss":
        _save_faiss(index_dir, records, vectors)
        # Backfill manifest fields with resolved runtime values.
        matrix_dim = int(len(vectors[0])) if vectors else 0
        _write_metadata_payload(
            index_dir / "metadata.json",
            documents=[_record_to_metadata_dict(r, changed=False) for r in records],
            embedding_model=resolved_model,
            dimension=max(1, matrix_dim),
            extra_manifest={"records": len(records)},
        )
    else:
        _save_chroma(index_dir, records, vectors)

    return {
        "backend": selected_backend,
        "records": len(records),
        "documents_dir": str(documents_dir),
        "index_dir": str(index_dir),
        "embedding_batch_size": embedding_batch_size,
    }


def _load_backend(index_dir: Path) -> str:
    meta_file = index_dir / "backend.json"
    if meta_file.exists():
        data = json.loads(meta_file.read_text(encoding="utf-8"))
        backend = str(data.get("backend", "")).strip().lower()
        if backend:
            return backend

    if (index_dir / "faiss.index").exists():
        return "faiss"
    if (index_dir / "chroma").exists():
        return "chroma"
    raise RAGIndexError(f"Vector index not found in: {index_dir}")


def _resolve_record_file_path(meta: dict[str, Any], documents_dir: Path) -> Path | None:
    # Prefer relative_path because it survives workspace relocation.
    rel = str(meta.get("relative_path", "")).strip()
    if rel:
        return documents_dir / Path(rel.replace("\\", "/"))

    file_path_text = str(meta.get("file_path", "")).strip()
    if not file_path_text:
        return None

    path = Path(file_path_text)
    if path.is_absolute():
        return path
    return documents_dir / path


def _prune_stale_faiss_entries(
    *,
    documents_dir: Path,
    index_dir: Path,
    dry_run: bool = False,
) -> dict[str, Any]:
    try:
        import faiss
        import numpy as np
    except ModuleNotFoundError as exc:
        raise RAGIndexError("FAISS prune requires faiss-cpu and numpy") from exc

    index, metadata = _load_faiss_index_and_metadata(index_dir)
    manifest_before, _docs_before = _read_metadata_payload(index_dir / "metadata.json")
    total_before = len(metadata)
    stale_indices: list[int] = []
    stale_paths: list[str] = []

    # Scan metadata and mark entries whose backing markdown file no longer exists.
    for i, item in enumerate(metadata):
        if not isinstance(item, dict):
            stale_indices.append(i)
            stale_paths.append("<invalid-metadata-item>")
            continue

        resolved = _resolve_record_file_path(item, documents_dir)
        if resolved is None or not resolved.is_file():
            stale_indices.append(i)
            stale_paths.append(str(item.get("relative_path") or item.get("file_path") or "<unknown>"))

    if not stale_indices:
        return {
            "backend": "faiss",
            "documents_total_before": total_before,
            "documents_total_after": total_before,
            "removed_documents": 0,
            "dry_run": dry_run,
            "removed_relative_paths": [],
        }

    stale_set = set(stale_indices)
    keep_indices = [i for i in range(total_before) if i not in stale_set]
    if dry_run:
        return {
            "backend": "faiss",
            "documents_total_before": total_before,
            "documents_total_after": len(keep_indices),
            "removed_documents": len(stale_indices),
            "dry_run": True,
            "removed_relative_paths": stale_paths,
        }

    kept_metadata: list[dict[str, Any]] = [metadata[i] for i in keep_indices if isinstance(metadata[i], dict)]
    # FAISS has no in-place row deletion; rebuild a new index from kept vectors.
    new_index = faiss.IndexFlatIP(index.d)
    if keep_indices:
        kept_vectors = [index.reconstruct(int(i)) for i in keep_indices]
        matrix = np.asarray(kept_vectors, dtype="float32")
        if matrix.ndim == 2 and matrix.shape[0] > 0:
            faiss.normalize_L2(matrix)
            new_index.add(matrix)

    faiss.write_index(new_index, str(index_dir / "faiss.index"))
    dim = int(new_index.d)
    _write_metadata_payload(
        index_dir / "metadata.json",
        documents=kept_metadata,
        embedding_model=str(manifest_before.get("embedding_model") or ""),
        dimension=dim,
        extra_manifest={"records": len(kept_metadata)},
    )
    _write_backend_meta(index_dir, "faiss")

    return {
        "backend": "faiss",
        "documents_total_before": total_before,
        "documents_total_after": len(kept_metadata),
        "removed_documents": len(stale_indices),
        "dry_run": False,
        "removed_relative_paths": stale_paths,
    }


def _prune_stale_chroma_entries(
    *,
    documents_dir: Path,
    index_dir: Path,
    dry_run: bool = False,
) -> dict[str, Any]:
    try:
        import chromadb
    except ModuleNotFoundError as exc:
        raise RAGIndexError("Chroma prune requires chromadb") from exc

    chroma_dir = index_dir / "chroma"
    if not chroma_dir.exists():
        raise RAGIndexError(f"Chroma directory not found: {chroma_dir}")

    client = chromadb.PersistentClient(path=str(chroma_dir))
    try:
        collection = client.get_collection("docs_topics")
    except Exception as exc:  # noqa: BLE001
        raise RAGIndexError("Chroma collection 'docs_topics' not found") from exc

    # Read all IDs + metadata once, then delete stale IDs in a single call.
    rows = collection.get(include=["metadatas"])
    ids = rows.get("ids", []) or []
    metadatas = rows.get("metadatas", []) or []

    stale_ids: list[str] = []
    stale_paths: list[str] = []
    for doc_id, meta in zip(ids, metadatas):
        item = meta if isinstance(meta, dict) else {}
        resolved = _resolve_record_file_path(item, documents_dir)
        if resolved is None or not resolved.is_file():
            stale_ids.append(str(doc_id))
            stale_paths.append(str(item.get("relative_path") or item.get("file_path") or "<unknown>"))

    if stale_ids and not dry_run:
        collection.delete(ids=stale_ids)

    if not dry_run:
        _write_backend_meta(index_dir, "chroma")
    total_before = len(ids)
    total_after = total_before - len(stale_ids)
    return {
        "backend": "chroma",
        "documents_total_before": total_before,
        "documents_total_after": total_after,
        "removed_documents": len(stale_ids),
        "dry_run": dry_run,
        "removed_relative_paths": stale_paths,
    }


def prune_stale_index_entries(
    *,
    documents_dir: Path,
    index_dir: Path,
    backend: str = "auto",
    dry_run: bool = False,
) -> dict[str, Any]:
    if not index_dir.exists():
        raise RAGIndexError(f"Vector index directory not found: {index_dir}")

    # Auto-detect current backend from index artifacts unless user forces one.
    selected_backend = _load_backend(index_dir)
    if backend != "auto":
        selected_backend = _select_backend(backend)

    if selected_backend == "faiss":
        return _prune_stale_faiss_entries(documents_dir=documents_dir, index_dir=index_dir, dry_run=dry_run)
    if selected_backend == "chroma":
        return _prune_stale_chroma_entries(documents_dir=documents_dir, index_dir=index_dir, dry_run=dry_run)

    raise RAGIndexError(f"Unsupported backend loaded: {selected_backend}")


def _search_faiss(
    index_dir: Path,
    query_vector: list[float],
    top_k: int,
    expected_embedding_model: str | None = None,
) -> list[dict[str, Any]]:
    try:
        import faiss
        import numpy as np
    except ModuleNotFoundError as exc:
        raise RAGIndexError("FAISS search requires faiss-cpu and numpy") from exc

    index_path = index_dir / "faiss.index"
    meta_path = index_dir / "metadata.json"
    if not index_path.exists() or not meta_path.exists():
        raise RAGIndexError("FAISS index files missing (faiss.index/metadata.json)")

    manifest, metadata = _read_metadata_payload(meta_path)
    index = faiss.read_index(str(index_path))

    query = np.asarray([query_vector], dtype="float32")
    if int(index.d) != int(query.shape[1]):
        raise RAGIndexError(
            f"FAISS dimension mismatch: index has {index.d}, query embedding has {query.shape[1]}"
        )

    manifest_dim = int(manifest.get("dimension") or 0)
    if manifest_dim > 0 and manifest_dim != int(index.d):
        raise RAGIndexError(
            f"FAISS metadata dimension mismatch: metadata has {manifest_dim}, index has {index.d}"
        )

    expected = str(expected_embedding_model or "").strip()
    if expected:
        manifest_model = str(manifest.get("embedding_model") or "").strip()
        if manifest_model and _normalize_embedding_model_value(manifest_model) != _normalize_embedding_model_value(expected):
            raise RAGIndexError(
                f"FAISS embedding model mismatch: metadata has {manifest_model}, query uses {expected}"
            )

    faiss.normalize_L2(query)

    scores, idx = index.search(query, top_k)
    results: list[dict[str, Any]] = []
    for score, i in zip(scores[0], idx[0]):
        if i < 0 or i >= len(metadata):
            continue
        item = dict(metadata[i])
        item["score"] = float(score)
        results.append(item)
    return results


def _search_faiss_with_auto_rebuild(
    *,
    query_vector: list[float],
    top_k: int,
    documents_dir: Path,
    index_dir: Path,
    build_if_missing: bool,
    api_url: str | None,
    api_key: str | None,
    embedding_model: str | None,
    timeout: int | None,
) -> tuple[list[dict[str, Any]], bool]:
    try:
        return _search_faiss(index_dir, query_vector, top_k, expected_embedding_model=embedding_model), False
    except RAGIndexError as exc:
        msg = str(exc).lower()
        if "dimension mismatch" not in msg and "embedding model mismatch" not in msg:
            raise
        if not build_if_missing:
            raise

        # Embedding model changed (for example bge-base -> bge-base).
        # Rebuild index once, then retry search.
        build_vector_index(
            documents_dir=documents_dir,
            index_dir=index_dir,
            backend="faiss",
            api_url=api_url,
            api_key=api_key,
            embedding_model=embedding_model,
            timeout=timeout,
        )
        return _search_faiss(index_dir, query_vector, top_k), True


def _search_chroma(index_dir: Path, query_vector: list[float], top_k: int) -> list[dict[str, Any]]:
    try:
        import chromadb
    except ModuleNotFoundError as exc:
        raise RAGIndexError("Chroma search requires chromadb") from exc

    chroma_dir = index_dir / "chroma"
    if not chroma_dir.exists():
        raise RAGIndexError(f"Chroma directory not found: {chroma_dir}")

    client = chromadb.PersistentClient(path=str(chroma_dir))
    collection = client.get_collection("docs_topics")

    res = collection.query(query_embeddings=[query_vector], n_results=top_k)
    ids = res.get("ids", [[]])[0]
    dists = res.get("distances", [[]])[0]
    metas = res.get("metadatas", [[]])[0]

    results: list[dict[str, Any]] = []
    for _id, dist, meta in zip(ids, dists, metas):
        score = 1.0 / (1.0 + float(dist))
        item = dict(meta or {})
        item["id"] = _id
        item["score"] = score
        if "keywords" in item and isinstance(item["keywords"], str):
            item["keywords"] = [x for x in item["keywords"].split("|") if x]
        results.append(item)
    return results


def _ensure_index_ready_for_search(
    *,
    documents_dir: Path,
    index_dir: Path,
    backend: str,
    build_if_missing: bool,
    api_url: str | None,
    api_key: str | None,
    embedding_model: str | None,
    timeout: int | None,
) -> None:
    def _has_backend_artifacts(selected_backend: str) -> bool:
        if selected_backend == "faiss":
            return (index_dir / "faiss.index").exists() and (index_dir / "metadata.json").exists()
        if selected_backend == "chroma":
            return (index_dir / "chroma").exists()
        return False

    # Case 1: index directory does not exist.
    if not index_dir.exists():
        if build_if_missing:
            build_vector_index(
                documents_dir=documents_dir,
                index_dir=index_dir,
                backend=backend,
                api_url=api_url,
                api_key=api_key,
                embedding_model=embedding_model,
                timeout=timeout,
            )
            return
        raise RAGIndexError(f"Vector index directory not found: {index_dir}")

    # Case 2: directory exists but backend marker/index files are missing.
    try:
        loaded_backend = _load_backend(index_dir)
    except RAGIndexError:
        if build_if_missing:
            build_vector_index(
                documents_dir=documents_dir,
                index_dir=index_dir,
                backend=backend,
                api_url=api_url,
                api_key=api_key,
                embedding_model=embedding_model,
                timeout=timeout,
            )
            return
        raise

    selected_backend = _select_backend(backend) if backend != "auto" else loaded_backend
    if not _has_backend_artifacts(selected_backend):
        if build_if_missing:
            build_vector_index(
                documents_dir=documents_dir,
                index_dir=index_dir,
                backend=selected_backend,
                api_url=api_url,
                api_key=api_key,
                embedding_model=embedding_model,
                timeout=timeout,
            )
            return
        if selected_backend == "faiss":
            raise RAGIndexError("FAISS index files missing (faiss.index/metadata.json)")
        if selected_backend == "chroma":
            raise RAGIndexError("Chroma index directory missing (chroma)")


def search_vector_index(
    *,
    query: str,
    documents_dir: Path,
    index_dir: Path,
    top_k: int = 10,
    backend: str = "auto",
    build_if_missing: bool = True,
    api_url: str | None = None,
    api_key: str | None = None,
    embedding_model: str | None = None,
    timeout: int | None = None,
) -> list[dict[str, Any]]:
    if not query.strip():
        return []

    _ensure_index_ready_for_search(
        documents_dir=documents_dir,
        index_dir=index_dir,
        backend=backend,
        build_if_missing=build_if_missing,
        api_url=api_url,
        api_key=api_key,
        embedding_model=embedding_model,
        timeout=timeout,
    )

    resolved_url, resolved_key, resolved_model, resolved_timeout = _resolve_api_settings(
        api_url=api_url,
        api_key=api_key,
        embedding_model=embedding_model,
        index_dir=index_dir,
        timeout=timeout,
    )

    query_vector = _embed_texts(
        [query],
        api_url=resolved_url,
        api_key=resolved_key,
        model=resolved_model,
        timeout=resolved_timeout,
        batch_size=1,
    )[0]

    selected_backend = _load_backend(index_dir)
    if backend != "auto":
        selected_backend = _select_backend(backend)

    if selected_backend == "faiss":
        results, _rebuilt = _search_faiss_with_auto_rebuild(
            query_vector=query_vector,
            top_k=top_k,
            documents_dir=documents_dir,
            index_dir=index_dir,
            build_if_missing=build_if_missing,
            api_url=api_url,
            api_key=api_key,
            embedding_model=resolved_model,
            timeout=resolved_timeout,
        )
        return results
    if selected_backend == "chroma":
        return _search_chroma(index_dir, query_vector, top_k)

    raise RAGIndexError(f"Unsupported backend loaded: {selected_backend}")


def search_vector_index_with_diagnostics(
    *,
    query: str,
    documents_dir: Path,
    index_dir: Path,
    top_k: int = 10,
    backend: str = "auto",
    build_if_missing: bool = True,
    api_url: str | None = None,
    api_key: str | None = None,
    embedding_model: str | None = None,
    timeout: int | None = None,
    stage_callback: Callable[[str], None] | None = None,
    no_embed_cache: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, float]]:
    timings: dict[str, float] = {}
    t0 = time.perf_counter()

    def _mark_stage(name: str) -> float:
        if stage_callback is not None:
            stage_callback(name)
        return time.perf_counter()

    if not query.strip():
        timings["total"] = time.perf_counter() - t0
        return [], timings

    stage_t = _mark_stage("prepare_index")
    _ensure_index_ready_for_search(
        documents_dir=documents_dir,
        index_dir=index_dir,
        backend=backend,
        build_if_missing=build_if_missing,
        api_url=api_url,
        api_key=api_key,
        embedding_model=embedding_model,
        timeout=timeout,
    )
    timings["prepare_index"] = time.perf_counter() - stage_t

    resolved_url, resolved_key, resolved_model, resolved_timeout = _resolve_api_settings(
        api_url=api_url,
        api_key=api_key,
        embedding_model=embedding_model,
        index_dir=index_dir,
        timeout=timeout,
    )

    _embed_cache_stats: dict = {}
    stage_t = _mark_stage("embed_query")
    query_vector = _embed_texts(
        [query],
        api_url=resolved_url,
        api_key=resolved_key,
        model=resolved_model,
        timeout=resolved_timeout,
        batch_size=1,
        _cache_stats=_embed_cache_stats,
        no_cache=no_embed_cache,
    )[0]
    timings["embed_query"] = time.perf_counter() - stage_t
    timings["embed_cache_hit"] = 1.0 if _embed_cache_stats.get("embed_hits", 0) > 0 else 0.0

    selected_backend = _load_backend(index_dir)
    if backend != "auto":
        selected_backend = _select_backend(backend)

    if selected_backend == "faiss":
        stage_t = _mark_stage("faiss_search")
        results, rebuilt = _search_faiss_with_auto_rebuild(
            query_vector=query_vector,
            top_k=top_k,
            documents_dir=documents_dir,
            index_dir=index_dir,
            build_if_missing=build_if_missing,
            api_url=api_url,
            api_key=api_key,
            embedding_model=resolved_model,
            timeout=resolved_timeout,
        )
        timings["faiss_search"] = time.perf_counter() - stage_t
        timings["faiss_rebuilt"] = 1.0 if rebuilt else 0.0
    elif selected_backend == "chroma":
        stage_t = _mark_stage("chroma_search")
        results = _search_chroma(index_dir, query_vector, top_k)
        timings["chroma_search"] = time.perf_counter() - stage_t
    else:
        raise RAGIndexError(f"Unsupported backend loaded: {selected_backend}")

    timings["total"] = time.perf_counter() - t0
    return results, timings


def _parse_args() -> argparse.Namespace:
    script_dir = Path(__file__).resolve().parent
    root_dir = script_dir.parent
    default_index_dir = os.getenv("AI_SUMMARY_VECTOR_DB_DIR", str(DEFAULT_VECTOR_DB_DIR))

    parser = argparse.ArgumentParser(description="Build/search RAG vector index from documents")
    parser.add_argument("--documents-dir", default=str(DEFAULT_DOCUMENTS_DIR), help="Documents directory")
    parser.add_argument("--index-dir", default=default_index_dir, help="Vector index directory")
    parser.add_argument("--backend", default="auto", choices=["auto", "faiss", "chroma"], help="Vector backend")
    parser.add_argument("--query", default="", help="Optional query text to run vector search")
    parser.add_argument("--sync-missing", action="store_true", help="Only embed and append documents missing from FAISS metadata")
    parser.add_argument("--prune-stale", action="store_true", help="Remove index entries whose source documents are missing")
    parser.add_argument("--dry-run", action="store_true", help="Preview prune changes without writing index data")
    parser.add_argument("--top-k", type=int, default=10, help="Top K search results")
    parser.add_argument("--embedding-model", default="", help="Override embedding model")
    parser.add_argument("--output-json", default="", help="Optional path to write JSON output")
    parser.add_argument("--timeout", type=int, default=TIMEOUT, help="Request timeout")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    documents_dir = Path(args.documents_dir)
    index_dir = Path(args.index_dir)

    if args.query.strip():
        results = search_vector_index(
            query=args.query,
            documents_dir=documents_dir,
            index_dir=index_dir,
            top_k=max(1, args.top_k),
            backend=args.backend,
            build_if_missing=True,
            embedding_model=args.embedding_model or None,
            timeout=args.timeout,
        )
        if args.output_json:
            out_path = Path(args.output_json)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"Vector search results: {len(results)}")
            return

        print(f"Vector search results: {len(results)}")
        for i, item in enumerate(results, start=1):
            print(f"{i}. score={item.get('score', 0):.4f} topic={item.get('topic', '')} file={item.get('relative_path', '')}")
        return

    if args.sync_missing:
        stats = sync_missing_embeddings_to_faiss(
            documents_dir=documents_dir,
            index_dir=index_dir,
            embedding_model=args.embedding_model or None,
            timeout=args.timeout,
        )
        _print_json_safe(stats)
        return

    if args.prune_stale:
        stats = prune_stale_index_entries(
            documents_dir=documents_dir,
            index_dir=index_dir,
            backend=args.backend,
            dry_run=args.dry_run,
        )
        _print_json_safe(stats)
        return

    stats = build_vector_index(
        documents_dir=documents_dir,
        index_dir=index_dir,
        backend=args.backend,
        embedding_model=args.embedding_model or None,
        timeout=args.timeout,
    )
    _print_json_safe(stats)


if __name__ == "__main__":
    main()
