"""Local embedding + vector index utilities for markdown knowledge base.

Design notes:
- Local-only embedding inference (offline-friendly).
- FAISS as primary backend, Chroma optional.
- CLI supports build/search/sync-missing operations.
"""

import argparse
import io
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from hashlib import sha1
from pathlib import Path
from typing import Any, Callable

try:
    from core_service.config import get_settings
except ModuleNotFoundError:
    from config import get_settings

_SETTINGS = get_settings()
EMBEDDING_MODEL = _SETTINGS.embedding_model
TIMEOUT = _SETTINGS.timeout


BULLET_LINE_RE = re.compile(r"^\s*[-*]\s+(.+?)\s*$")
HEADING_RE = re.compile(r"^\s*#{1,6}\s+(.+?)\s*$")


class RAGIndexError(RuntimeError):
    pass


@dataclass
class TopicRecord:
    id: str
    file_path: str
    relative_path: str
    topic: str
    keywords: list[str]
    embedding_text: str


_LOCAL_EMBED_MODEL_CACHE: dict[str, Any] = {}
_STDIO_FALLBACK_STREAMS: list[io.TextIOBase] = []


def _data_roots() -> list[Path]:
    core_service_root = Path(__file__).resolve().parent
    legacy_root = core_service_root.parent / "ai_conversations_summary" / "data"
    # Prefer shared core_service data first, then legacy ai_conversations_summary data.
    return [core_service_root / "data", legacy_root]


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

    # Pick newest snapshot folder.
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

    for root in _data_roots():
        remapped = (root / "local_models" / Path(suffix)).resolve()
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
        return _relocate_legacy_local_model_path(raw)
    return raw


def _extract_first_heading(markdown_text: str) -> str:
    for line in markdown_text.splitlines()[:120]:
        m = HEADING_RE.match(line)
        if m:
            return m.group(1).strip()
    return ""


def _extract_generic_points(markdown_text: str, max_items: int = 3) -> list[str]:
    points: list[str] = []
    for line in markdown_text.splitlines()[:260]:
        m = BULLET_LINE_RE.match(line)
        if not m:
            continue
        item = re.sub(r"\s+", " ", m.group(1)).strip()
        if item:
            points.append(item)
        if len(points) >= max_items:
            break
    return points


def _extract_plain_text(markdown_text: str, max_chars: int = 1800) -> str:
    lines: list[str] = []
    for line in markdown_text.splitlines():
        text = line.strip()
        if not text:
            continue
        # Keep content generic and avoid markdown markers skewing embeddings.
        text = re.sub(r"^#{1,6}\s+", "", text)
        text = re.sub(r"^[-*]\s+", "", text)
        text = re.sub(r"`{1,3}", "", text)
        lines.append(text)
        if len(" ".join(lines)) >= max_chars:
            break
    joined = re.sub(r"\s+", " ", " ".join(lines)).strip()
    return joined[:max_chars]


def build_topic_records(documents_dir: Path) -> list[TopicRecord]:
    # Convert each markdown file into one stable, searchable generic record.
    if not documents_dir.exists():
        raise FileNotFoundError(f"Documents directory not found: {documents_dir}")

    records: list[TopicRecord] = []
    for file_path in sorted(documents_dir.rglob("*.md")):
        if not file_path.is_file():
            continue

        text = file_path.read_text(encoding="utf-8", errors="ignore")
        rel = file_path.relative_to(documents_dir).as_posix()
        title = _extract_first_heading(text) or file_path.stem
        points = _extract_generic_points(text)
        topic_summary = " | ".join(points).strip() or title
        plain_text = _extract_plain_text(text)

        rec_id = sha1(rel.encode("utf-8")).hexdigest()
        emb_text = (
            f"title: {title}\n"
            f"path: {rel}\n"
            f"summary: {topic_summary}\n"
            f"content: {plain_text}"
        ).strip()
        records.append(
            TopicRecord(
                id=rec_id,
                file_path=rel,
                relative_path=rel,
                topic=topic_summary,
                keywords=[],
                embedding_text=emb_text,
            )
        )

    return records


def _resolve_api_settings(
    api_url: str | None = None,
    api_key: str | None = None,
    embedding_model: str | None = None,
    timeout: int | None = None,
) -> tuple[str, str, str, int]:
    _ = api_url, api_key
    resolved_model = (
        embedding_model
        or os.getenv("LOCAL_EMBEDDING_MODEL")
        or os.getenv("DEEPSEEK_EMBEDDING_MODEL")
        or EMBEDDING_MODEL
    ).strip()
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
    os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")
    os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")

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
) -> list[list[float]]:
    _ = api_url, api_key, timeout
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

    metadata = [
        {
            "id": r.id,
            "file_path": r.file_path,
            "relative_path": r.relative_path,
            "topic": r.topic,
            "keywords": r.keywords,
            "embedding_text": r.embedding_text,
        }
        for r in records
    ]
    (index_dir / "metadata.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
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
    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    if not isinstance(metadata, list):
        raise RAGIndexError("metadata.json is invalid (expected list)")
    return index, metadata


def get_missing_embedding_records(*, documents_dir: Path, index_dir: Path) -> tuple[list[TopicRecord], list[TopicRecord]]:
    all_records = build_topic_records(documents_dir)
    if not all_records:
        return [], []

    existing_paths: set[str] = set()
    meta_path = index_dir / "metadata.json"
    if meta_path.exists():
        try:
            data = json.loads(meta_path.read_text(encoding="utf-8"))
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
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
        timeout=timeout,
    )
    embedding_batch_size = _resolve_bulk_embedding_batch_size(resolved_model)

    index_dir.mkdir(parents=True, exist_ok=True)
    index_path = index_dir / "faiss.index"
    meta_path = index_dir / "metadata.json"

    if not missing_records:
        _write_backend_meta(index_dir, "faiss")
        return {
            "backend": "faiss",
            "documents_total": len(all_records),
            "missing_documents": 0,
            "added_documents": 0,
            "index_dir": str(index_dir),
            "missing_relative_paths": [],
        }

    vectors = _embed_texts(
        [r.embedding_text for r in missing_records],
        api_url=resolved_url,
        api_key=resolved_key,
        model=resolved_model,
        timeout=resolved_timeout,
        batch_size=embedding_batch_size,
    )

    matrix = np.asarray(vectors, dtype="float32")
    if matrix.ndim != 2 or matrix.shape[0] != len(missing_records):
        raise RAGIndexError("Embedding matrix shape mismatch when syncing missing documents")
    faiss.normalize_L2(matrix)

    metadata: list[dict[str, Any]]
    if index_path.exists() and meta_path.exists():
        index, metadata = _load_faiss_index_and_metadata(index_dir)
        if index.d != int(matrix.shape[1]):
            raise RAGIndexError(
                f"FAISS dimension mismatch: index has {index.d}, new embeddings have {matrix.shape[1]}"
            )
        index.add(matrix)
    else:
        index = faiss.IndexFlatIP(matrix.shape[1])
        index.add(matrix)
        metadata = []

    metadata.extend(
        {
            "id": r.id,
            "file_path": r.file_path,
            "relative_path": r.relative_path,
            "topic": r.topic,
            "keywords": r.keywords,
            "embedding_text": r.embedding_text,
        }
        for r in missing_records
    )

    faiss.write_index(index, str(index_path))
    meta_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_backend_meta(index_dir, "faiss")

    return {
        "backend": "faiss",
        "documents_total": len(all_records),
        "missing_documents": len(missing_records),
        "added_documents": len(missing_records),
        "index_dir": str(index_dir),
        "missing_relative_paths": [r.relative_path for r in missing_records],
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
                "topic": r.topic,
                "keywords": "|".join(r.keywords),
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
    (index_dir / "metadata.json").write_text(json.dumps(kept_metadata, ensure_ascii=False, indent=2), encoding="utf-8")
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


def _search_faiss(index_dir: Path, query_vector: list[float], top_k: int) -> list[dict[str, Any]]:
    try:
        import faiss
        import numpy as np
    except ModuleNotFoundError as exc:
        raise RAGIndexError("FAISS search requires faiss-cpu and numpy") from exc

    index_path = index_dir / "faiss.index"
    meta_path = index_dir / "metadata.json"
    if not index_path.exists() or not meta_path.exists():
        raise RAGIndexError("FAISS index files missing (faiss.index/metadata.json)")

    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    index = faiss.read_index(str(index_path))

    query = np.asarray([query_vector], dtype="float32")
    if int(index.d) != int(query.shape[1]):
        raise RAGIndexError(
            f"FAISS dimension mismatch: index has {index.d}, query embedding has {query.shape[1]}"
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
        return _search_faiss(index_dir, query_vector, top_k), False
    except RAGIndexError as exc:
        msg = str(exc).lower()
        if "dimension mismatch" not in msg:
            raise
        if not build_if_missing:
            raise

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
        timeout=timeout,
    )

    stage_t = _mark_stage("embed_query")
    query_vector = _embed_texts(
        [query],
        api_url=resolved_url,
        api_key=resolved_key,
        model=resolved_model,
        timeout=resolved_timeout,
        batch_size=1,
    )[0]
    timings["embed_query"] = time.perf_counter() - stage_t

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

    parser = argparse.ArgumentParser(description="Build/search RAG vector index from documents")
    parser.add_argument("--documents-dir", default=str(root_dir / "documents"), help="Documents directory")
    parser.add_argument("--index-dir", default=str(root_dir / "data" / "vector_db"), help="Vector index directory")
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
        print(json.dumps(stats, ensure_ascii=False, indent=2))
        return

    if args.prune_stale:
        stats = prune_stale_index_entries(
            documents_dir=documents_dir,
            index_dir=index_dir,
            backend=args.backend,
            dry_run=args.dry_run,
        )
        print(json.dumps(stats, ensure_ascii=False, indent=2))
        return

    stats = build_vector_index(
        documents_dir=documents_dir,
        index_dir=index_dir,
        backend=args.backend,
        embedding_model=args.embedding_model or None,
        timeout=args.timeout,
    )
    print(json.dumps(stats, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
