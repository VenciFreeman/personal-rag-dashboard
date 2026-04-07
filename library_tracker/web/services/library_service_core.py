from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sqlite3
import shutil
import threading
import time
import unicodedata
from datetime import datetime
from dataclasses import dataclass
import importlib
from io import BytesIO
from pathlib import Path
from typing import Any
from uuid import uuid4

from PIL import Image

from core_service import get_settings
from ..settings import (
    COVERS_DIR,
    MEDIA_FILES,
    STRUCTURED_CONCEPTS_DIR,
    VECTOR_DB_DIR,
    get_alias_bucket_dir,
    get_concept_ontology_path,
    get_entity_file_path,
    get_preferred_concept_ontology_path,
    get_preferred_entity_file_path,
)
from . import library_alias_store, library_graph

TEXT_FIELDS = ["title", "author", "nationality", "category", "channel", "review", "publisher", "url"]
GENERIC_QUERY_TOKENS = {
    "的话",
    "哪些",
    "哪个",
    "什么",
    "怎么",
    "如何",
    "可以",
    "常见",
    "比如",
    "例如",
    "之类",
    "之类的",
    "有关",
    "相关",
    "介绍",
    "一下",
    "我想知道",
    "想问下",
    "请问",
    "帮我",
}
MULTI_WORD_LATIN_PHRASE_RE = re.compile(r"[a-z0-9_]+(?:[\s\-:/&+]+[a-z0-9_]+)+")
FILTER_FIELDS = ["year", "rating", "media_type", "nationality", "category", "channel", "author"]
FORM_SUGGESTION_FIELDS = ["author", "nationality", "category", "channel", "publisher"]
MULTI_TAG_FIELDS = {"author", "nationality", "category", "publisher"}
STATS_DIMENSION_FIELDS = ["category", "nationality", "channel", "author", "publisher"]
AI_LABEL_SOURCE_FIELDS = [
    "media_type",
    "date",
    "title",
    "author",
    "nationality",
    "category",
    "channel",
    "publisher",
    "rating",
    "review",
]
_CONTENT_TYPE_EXT = {
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
}

_CORE_SETTINGS = get_settings()
_local_llm_url_raw = os.getenv("LIBRARY_TRACKER_LOCAL_LLM_URL", "http://127.0.0.1:1234").strip()
if _local_llm_url_raw and not re.search(r"/v1/?$", _local_llm_url_raw):
    LOCAL_LLM_API_URL = _local_llm_url_raw.rstrip("/") + "/v1"
else:
    LOCAL_LLM_API_URL = _local_llm_url_raw
LOCAL_LLM_MODEL = os.getenv("LIBRARY_TRACKER_LOCAL_LLM_MODEL", "").strip() or _CORE_SETTINGS.local_llm_model
LOCAL_LLM_API_KEY = os.getenv("LIBRARY_TRACKER_LOCAL_LLM_API_KEY", "local").strip() or "local"
LOCAL_LLM_TIMEOUT = int(os.getenv("LIBRARY_TRACKER_LOCAL_LLM_TIMEOUT", "120"))
EMBEDDING_DB_PATH = VECTOR_DB_DIR / "library_embeddings.sqlite3"
_KEYWORD_CORPUS_CACHE: dict[str, Any] = {"signature": None, "texts": []}
_GRAPH_JOB_LOCK = threading.RLock()
_GRAPH_JOBS: dict[str, dict[str, Any]] = {}
_GRAPH_JOB_HISTORY_LIMIT = 12
_ASYNC_REFRESH_MAX_RETRIES = max(1, int(os.getenv("LIBRARY_TRACKER_ASYNC_REFRESH_MAX_RETRIES", "3") or "3"))
_ASYNC_REFRESH_RETRY_DELAY_SECONDS = max(0.0, float(os.getenv("LIBRARY_TRACKER_ASYNC_REFRESH_RETRY_DELAY_SECONDS", "2") or "2"))


def rebuild_library_graph(*, progress_callback=None) -> dict[str, Any]:
    all_items = _iter_all_items()
    library_graph.initialize_empty_graph(VECTOR_DB_DIR)
    graph_stats = library_graph.sync_library_graph(
        graph_dir=VECTOR_DB_DIR,
        items=all_items,
        target_item_ids=None,
        only_missing=False,
        progress_callback=progress_callback,
    )
    return {
        "ok": True,
        "item_count": len(all_items),
        "graph": graph_stats,
    }


def sync_missing_library_graph(*, progress_callback=None) -> dict[str, Any]:
    all_items = _iter_all_items()
    graph_stats = library_graph.sync_library_graph(
        graph_dir=VECTOR_DB_DIR,
        items=all_items,
        target_item_ids=None,
        only_missing=True,
        progress_callback=progress_callback,
    )
    return {
        "ok": True,
        "item_count": len(all_items),
        "graph": graph_stats,
        "mode": "missing_only",
    }


def _trim_graph_jobs() -> None:
    if len(_GRAPH_JOBS) <= _GRAPH_JOB_HISTORY_LIMIT:
        return
    ordered = sorted(_GRAPH_JOBS.values(), key=lambda item: str(item.get("created_at", "")))
    overflow = max(0, len(ordered) - _GRAPH_JOB_HISTORY_LIMIT)
    for item in ordered[:overflow]:
        _GRAPH_JOBS.pop(str(item.get("id") or ""), None)


def _graph_job_public(job: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(job.get("id") or ""),
        "mode": str(job.get("mode") or "missing_only"),
        "status": str(job.get("status") or "queued"),
        "message": str(job.get("message") or ""),
        "created_at": str(job.get("created_at") or ""),
        "started_at": str(job.get("started_at") or ""),
        "finished_at": str(job.get("finished_at") or ""),
        "result": job.get("result"),
        "error": job.get("error"),
    }


def _update_graph_job(job_id: str, **fields: Any) -> dict[str, Any] | None:
    with _GRAPH_JOB_LOCK:
        job = _GRAPH_JOBS.get(str(job_id or ""))
        if job is None:
            return None
        job.update(fields)
        return _graph_job_public(job)


def trigger_alias_proposal_background(item_ids: list[str] | None = None) -> None:
    from . import library_alias_lifecycle_service

    library_alias_lifecycle_service.trigger_alias_proposal_background(item_ids=item_ids)


def enqueue_item_refresh(item_ids: list[str] | None) -> dict[str, Any]:
    from . import library_alias_lifecycle_service

    return library_alias_lifecycle_service.enqueue_item_refresh(item_ids)


def start_graph_job(*, full: bool = False) -> dict[str, Any]:
    mode = "full" if full else "missing_only"
    job_id = str(uuid4())
    created_at = datetime.now().isoformat(timespec="seconds")
    job = {
        "id": job_id,
        "mode": mode,
        "status": "queued",
        "message": "等待开始",
        "created_at": created_at,
        "started_at": "",
        "finished_at": "",
        "result": None,
        "error": None,
    }
    with _GRAPH_JOB_LOCK:
        _GRAPH_JOBS[job_id] = job
        _trim_graph_jobs()

    def _runner() -> None:
        started_at = datetime.now().isoformat(timespec="seconds")
        _update_graph_job(job_id, status="running", started_at=started_at, message="正在重建 Library Graph")

        def _on_progress(snapshot: dict[str, Any]) -> None:
            processed = int(snapshot.get("processed_item_count") or 0)
            nodes_total = int(snapshot.get("nodes_total") or 0)
            edges_total = int(snapshot.get("edges_total") or 0)
            label = "正在全量重建 Library Graph" if full else "正在补足 Library Graph"
            _update_graph_job(
                job_id,
                message=f"{label} | 已处理 {processed} 项 | 节点 {nodes_total} | 边 {edges_total}",
            )

        try:
            result = rebuild_library_graph(progress_callback=_on_progress) if full else sync_missing_library_graph(progress_callback=_on_progress)
            finished_at = datetime.now().isoformat(timespec="seconds")
            _update_graph_job(
                job_id,
                status="completed",
                finished_at=finished_at,
                message="Library Graph 重建完成" if full else "Library Graph 补缺完成",
                result=result,
            )
        except Exception as exc:  # noqa: BLE001
            finished_at = datetime.now().isoformat(timespec="seconds")
            _update_graph_job(
                job_id,
                status="failed",
                finished_at=finished_at,
                message="Library Graph 重建失败" if full else "Library Graph 补缺失败",
                error=str(exc),
            )

    thread = threading.Thread(target=_runner, name=f"library-graph-{job_id}", daemon=True)
    thread.start()
    return _graph_job_public(job)


def get_graph_job(job_id: str) -> dict[str, Any] | None:
    with _GRAPH_JOB_LOCK:
        job = _GRAPH_JOBS.get(str(job_id or ""))
        if job is None:
            return None
        return _graph_job_public(job)


def _embedding_status_value(raw: Any) -> int:
    try:
        return 1 if int(raw or 0) == 1 else 0
    except Exception:
        return 0


@dataclass
class SearchResult:
    item: dict[str, Any]
    score: float
    alias_hits: list[dict[str, Any]] | None = None


class ItemNotFoundError(KeyError):
    pass


class BadItemIdError(ValueError):
    pass


# ── Search result cache ───────────────────────────────────────────────────────
# Pagination is cheap when the full scored list is cached across page turns.
# Cache is keyed on (query, mode, filters); TTL prevents stale data on edits.
_SEARCH_CACHE: dict[str, dict[str, Any]] = {}
_SEARCH_CACHE_TTL = 60.0  # seconds


def _search_cache_key(query: str, mode: str, filters: dict[str, list[str]] | None) -> str:
    raw = json.dumps({"q": query or "", "mode": mode, "f": filters or {}}, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(raw.encode()).hexdigest()


def invalidate_search_cache() -> None:
    """Call after any write operation so the next search re-scores."""
    _SEARCH_CACHE.clear()
    _invalidate_metadata_cache()


# ── Metadata caches (filter options, form suggestions, facets) ────────────────
# These are computed from a full item scan and are expensive.
# They are valid until the next write, so cache them indefinitely and bust on
# every _save_payload() call.
#
# Facet results are keyed by filter-combo hash.  To prevent unbounded growth
# (unique filter combos accumulate across a session) we keep at most
# _FACETS_CACHE_MAX_ENTRIES entries using a simple insertion-order eviction.

_FACETS_CACHE_MAX_ENTRIES = 32

_META_STATIC: dict[str, Any] = {}   # "filter_options", "form_suggestions"
_META_FACETS: dict[str, Any] = {}   # "facets_<hash>" — bounded LRU-style
_META_FACETS_ORDER: list[str] = []  # insertion order for eviction
_META_CACHE_LOCK = threading.Lock()


def _invalidate_metadata_cache() -> None:
    with _META_CACHE_LOCK:
        _META_STATIC.clear()
        _META_FACETS.clear()
        _META_FACETS_ORDER.clear()


def _cached_filter_options() -> dict[str, list[str]]:
    key = "filter_options"
    with _META_CACHE_LOCK:
        if key in _META_STATIC:
            return _META_STATIC[key]
    result = _compute_filter_options()
    with _META_CACHE_LOCK:
        _META_STATIC[key] = result
    return result


def _compute_filter_options() -> dict[str, list[str]]:
    items = _iter_all_items()
    options: dict[str, set[str]] = {k: set() for k in FILTER_FIELDS}
    for item in items:
        for field in FILTER_FIELDS:
            if field in MULTI_TAG_FIELDS:
                for token in _split_multi_tags(item.get(field)):
                    options[field].add(token)
                continue
            value = _filter_scalar_value(item, field)
            if value:
                options[field].add(value)
    return {k: _sort_filter_values(k, v) for k, v in options.items()}


def _cached_form_suggestions() -> dict[str, list[str]]:
    key = "form_suggestions"
    with _META_CACHE_LOCK:
        if key in _META_STATIC:
            return _META_STATIC[key]
    result = _compute_form_suggestions()
    with _META_CACHE_LOCK:
        _META_STATIC[key] = result
    return result


def _compute_form_suggestions() -> dict[str, list[str]]:
    items = _iter_all_items()
    options: dict[str, set[str]] = {field: set() for field in FORM_SUGGESTION_FIELDS}
    for item in items:
        for field in FORM_SUGGESTION_FIELDS:
            if field in MULTI_TAG_FIELDS:
                for token in _split_multi_tags(item.get(field)):
                    options[field].add(token)
                continue
            value = str(item.get(field) or "").strip()
            if value:
                options[field].add(value)
    return {field: _sort_text_values(values) for field, values in options.items()}


def _cached_facet_counts(filters: dict[str, list[str]] | None = None) -> dict[str, dict[str, int]]:
    filter_key = hashlib.md5(
        json.dumps(filters or {}, sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()
    key = f"facets_{filter_key}"
    with _META_CACHE_LOCK:
        if key in _META_FACETS:
            return _META_FACETS[key]
    result = _compute_facet_counts(filters)
    with _META_CACHE_LOCK:
        if key not in _META_FACETS:
            # Evict oldest entry when capacity is reached
            while len(_META_FACETS_ORDER) >= _FACETS_CACHE_MAX_ENTRIES:
                oldest = _META_FACETS_ORDER.pop(0)
                _META_FACETS.pop(oldest, None)
            _META_FACETS[key] = result
            _META_FACETS_ORDER.append(key)
    return result


def _compute_facet_counts(filters: dict[str, list[str]] | None = None) -> dict[str, dict[str, int]]:
    items = _iter_all_items()
    normalized_filters = filters or {}
    counts: dict[str, dict[str, int]] = {field: {} for field in FILTER_FIELDS}

    for target_field in FILTER_FIELDS:
        other_filters: dict[str, list[str]] = {
            field: list(normalized_filters.get(field, []))
            for field in FILTER_FIELDS
            if field != target_field
        }
        field_counts: dict[str, int] = {}
        for item in items:
            if not _matches_filters(item, other_filters):
                continue
            if target_field in MULTI_TAG_FIELDS:
                for token in _split_multi_tags(item.get(target_field)):
                    field_counts[token] = field_counts.get(token, 0) + 1
                continue
            value = _filter_scalar_value(item, target_field)
            if not value:
                continue
            field_counts[value] = field_counts.get(value, 0) + 1
        counts[target_field] = field_counts

    return counts


def get_bootstrap_data(initial_query: str = "", initial_limit: int = 50) -> dict[str, Any]:
    from . import library_query_service

    return library_query_service.get_bootstrap_data(initial_query=initial_query, initial_limit=initial_limit)


def _json_path(media_type: str) -> Path:
    filename = MEDIA_FILES.get(media_type)
    if not filename:
        raise ValueError(f"Unsupported media_type: {media_type}")
    return get_entity_file_path(media_type)


def _load_payload(media_type: str) -> dict[str, Any]:
    path = _json_path(media_type)
    if not path.exists():
        payload = {
            "source": str(path),
            "profile": media_type,
            "record_count": 0,
            "records": [],
        }
        _save_payload(media_type, payload)
        return payload
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if "records" not in payload or not isinstance(payload["records"], list):
        payload["records"] = []
    changed = False
    normalized_records: list[dict[str, Any]] = []
    for record in payload.get("records", []):
        if not isinstance(record, dict):
            continue
        normalized = dict(record)
        migrated_date = _normalize_date_value(normalized.get("date"), normalized)
        if migrated_date != normalized.get("date"):
            normalized["date"] = migrated_date
            changed = True
        if "year" in normalized:
            normalized.pop("year", None)
            changed = True
        if "month" in normalized:
            normalized.pop("month", None)
            changed = True
        if "day" in normalized:
            normalized.pop("day", None)
            changed = True
        normalized_records.append(normalized)

    if _migrate_legacy_embedding_fields(media_type, normalized_records):
        changed = True

    if changed:
        payload["records"] = normalized_records
        _save_payload(media_type, payload)
    else:
        payload["records"] = normalized_records
    return payload


def _save_payload(media_type: str, payload: dict[str, Any]) -> None:
    invalidate_search_cache()
    path = _json_path(media_type)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload["record_count"] = len(payload.get("records", []))
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _load_json_document(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _write_json_document(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _iter_bucket_json_payloads(bucket: str) -> dict[str, Any]:
    bucket_dir = get_alias_bucket_dir(bucket)
    payloads: dict[str, Any] = {}
    if not bucket_dir.exists():
        return payloads
    for path in sorted(bucket_dir.glob("*.json")):
        payloads[path.name] = _load_json_document(path, {"entries": []})
    return payloads


def _replace_directory_json_payloads(target_dir: Path, payloads: dict[str, Any], *, replace_existing: bool) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    if replace_existing:
        for path in list(target_dir.glob("*.json")):
            path.unlink(missing_ok=True)
    for name, payload in sorted((payloads or {}).items()):
        clean_name = Path(str(name or "")).name
        if not clean_name.endswith(".json"):
            continue
        _write_json_document(target_dir / clean_name, payload)


def export_library_contract() -> dict[str, Any]:
    from . import library_contract_service

    return library_contract_service.export_library_contract()


def import_library_contract(payload: dict[str, Any], replace_existing: bool = False) -> dict[str, Any]:
    from . import library_contract_service

    return library_contract_service.import_library_contract(payload, replace_existing=replace_existing)


def _parse_item_id(item_id: str) -> tuple[str, int]:
    m = re.match(r"^(book|video|music|game):(\d+)$", item_id)
    if not m:
        raise BadItemIdError(f"Invalid item id: {item_id}")
    return m.group(1), int(m.group(2))


def _embedding_db_conn() -> sqlite3.Connection:
    VECTOR_DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(EMBEDDING_DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS item_embeddings (
            item_id TEXT PRIMARY KEY,
            embedding_status INTEGER NOT NULL DEFAULT 0,
            ai_label TEXT,
            embedding_json TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_item_embeddings_status ON item_embeddings(embedding_status)"
    )
    return conn


def _embedding_row_count() -> int:
    try:
        with _embedding_db_conn() as conn:
            row = conn.execute("SELECT COUNT(1) FROM item_embeddings").fetchone()
    except Exception:
        return 0
    return int(row[0]) if row and row[0] is not None else 0


def _graph_dashboard_summary() -> dict[str, int]:
    path = VECTOR_DB_DIR / library_graph.GRAPH_FILE_NAME
    if not path.exists():
        return {
            "nodes": 0,
            "edges": 0,
            "processed_items": 0,
            "item_nodes": 0,
            "isolated_nodes": 0,
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {
            "nodes": 0,
            "edges": 0,
            "processed_items": 0,
            "item_nodes": 0,
            "isolated_nodes": 0,
        }
    if not isinstance(payload, dict):
        return {
            "nodes": 0,
            "edges": 0,
            "processed_items": 0,
            "item_nodes": 0,
            "isolated_nodes": 0,
        }

    nodes = payload.get("nodes", {}) if isinstance(payload.get("nodes"), dict) else {}
    edges = payload.get("edges", []) if isinstance(payload.get("edges"), list) else []
    processed = payload.get("processed_items", []) if isinstance(payload.get("processed_items"), list) else []

    degrees: dict[str, int] = {str(node_id): 0 for node_id in nodes.keys()}
    item_nodes = 0
    for node_id, node in nodes.items():
        if isinstance(node, dict) and str(node.get("type", "")).strip() == "item":
            item_nodes += 1
        degrees.setdefault(str(node_id), 0)
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        src = str(edge.get("src", "")).strip()
        dst = str(edge.get("dst", "")).strip()
        if src:
            degrees[src] = degrees.get(src, 0) + 1
        if dst:
            degrees[dst] = degrees.get(dst, 0) + 1

    return {
        "nodes": len(nodes),
        "edges": len(edges),
        "processed_items": len([item for item in processed if str(item).strip()]),
        "item_nodes": item_nodes,
        "isolated_nodes": sum(1 for degree in degrees.values() if int(degree) <= 0),
    }


def _upsert_embedding_state(
    conn: sqlite3.Connection,
    *,
    item_id: str,
    embedding_status: int,
    ai_label: str | None,
    embedding: dict[str, float] | None,
) -> None:
    now = datetime.now().isoformat(timespec="seconds")
    emb_json = json.dumps(embedding, ensure_ascii=False) if isinstance(embedding, dict) else None
    conn.execute(
        """
        INSERT INTO item_embeddings(item_id, embedding_status, ai_label, embedding_json, updated_at)
        VALUES(?, ?, ?, ?, ?)
        ON CONFLICT(item_id) DO UPDATE SET
            embedding_status=excluded.embedding_status,
            ai_label=excluded.ai_label,
            embedding_json=excluded.embedding_json,
            updated_at=excluded.updated_at
        """,
        (item_id, int(embedding_status), ai_label, emb_json, now),
    )


def _mark_item_pending(item_id: str) -> None:
    with _embedding_db_conn() as conn:
        _upsert_embedding_state(
            conn,
            item_id=item_id,
            embedding_status=0,
            ai_label=None,
            embedding=None,
        )
        conn.commit()


def _load_embedding_states(item_ids: list[str] | None = None) -> dict[str, dict[str, Any]]:
    with _embedding_db_conn() as conn:
        rows: list[sqlite3.Row] = []
        if item_ids:
            chunk_size = 800
            for i in range(0, len(item_ids), chunk_size):
                chunk = item_ids[i : i + chunk_size]
                if not chunk:
                    continue
                placeholders = ",".join(["?"] * len(chunk))
                rows.extend(
                    conn.execute(
                        f"SELECT item_id, embedding_status, ai_label, embedding_json FROM item_embeddings WHERE item_id IN ({placeholders})",
                        chunk,
                    ).fetchall()
                )
        else:
            rows = conn.execute(
                "SELECT item_id, embedding_status, ai_label, embedding_json FROM item_embeddings"
            ).fetchall()

    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        emb_obj: dict[str, float] | None = None
        raw_emb = row["embedding_json"]
        if raw_emb:
            try:
                parsed = json.loads(raw_emb)
                if isinstance(parsed, dict):
                    emb_obj = {str(k): float(v) for k, v in parsed.items() if isinstance(v, (int, float))}
            except Exception:
                emb_obj = None
        result[str(row["item_id"])] = {
            "embedding_status": _embedding_status_value(row["embedding_status"]),
            "ai_label": str(row["ai_label"] or "").strip() or None,
            "embedding": emb_obj,
        }
    return result


def _migrate_legacy_embedding_fields(media_type: str, records: list[dict[str, Any]]) -> bool:
    """Move legacy per-record embedding fields from json into sqlite once."""
    if not records:
        return False
    changed = False
    with _embedding_db_conn() as conn:
        for idx, record in enumerate(records):
            if not isinstance(record, dict):
                continue
            item_id = f"{media_type}:{idx}"
            legacy_status = _embedding_status_value(record.get("embedding_status"))
            legacy_ai = str(record.get("ai_label") or "").strip() or None
            legacy_emb = record.get("embedding") if isinstance(record.get("embedding"), dict) else None
            if legacy_status == 1 or legacy_ai is not None or legacy_emb is not None:
                _upsert_embedding_state(
                    conn,
                    item_id=item_id,
                    embedding_status=legacy_status,
                    ai_label=legacy_ai,
                    embedding=legacy_emb,
                )

            if "embedding_status" in record:
                record.pop("embedding_status", None)
                changed = True
            if "ai_label" in record:
                record.pop("ai_label", None)
                changed = True
            if "embedding" in record:
                record.pop("embedding", None)
                changed = True
        conn.commit()
    return changed


def _normalize_item(item: dict[str, Any], media_type: str, index: int) -> dict[str, Any]:
    normalized = dict(item)
    normalized["media_type"] = normalized.get("media_type") or media_type
    normalized["id"] = f"{media_type}:{index}"
    return normalized


def _iter_all_items() -> list[dict[str, Any]]:
    all_items: list[dict[str, Any]] = []
    for media_type in MEDIA_FILES:
        payload = _load_payload(media_type)
        for index, item in enumerate(payload.get("records", [])):
            if not isinstance(item, dict):
                continue
            all_items.append(_normalize_item(item, media_type, index))
    return all_items


def _keyword_corpus_signature() -> tuple[tuple[str, int, int], ...]:
    signature: list[tuple[str, int, int]] = []
    for media_type in MEDIA_FILES:
        path = _json_path(media_type)
        try:
            stat = path.stat()
            signature.append((media_type, int(stat.st_mtime_ns), int(stat.st_size)))
        except Exception:
            signature.append((media_type, 0, 0))
    alias_mtime, alias_size = library_alias_store.alias_proposal_file_signature()
    signature.append(("alias_proposal", alias_mtime, alias_size))
    return tuple(signature)


def _keyword_corpus_texts() -> list[str]:
    signature = _keyword_corpus_signature()
    cached_signature = _KEYWORD_CORPUS_CACHE.get("signature")
    cached_texts = _KEYWORD_CORPUS_CACHE.get("texts") if isinstance(_KEYWORD_CORPUS_CACHE.get("texts"), list) else []
    if cached_signature == signature and cached_texts:
        return cached_texts

    texts = [_search_text(item) for item in _iter_all_items()]
    _KEYWORD_CORPUS_CACHE["signature"] = signature
    _KEYWORD_CORPUS_CACHE["texts"] = texts
    return texts


def _keyword_idf(token: str) -> float:
    value = str(token or "").strip().lower()
    if not value:
        return 0.0
    texts = _keyword_corpus_texts()
    if not texts:
        return 0.0
    df = sum(1 for text in texts if value in text)
    total = len(texts)
    return math.log((total + 1.0) / (df + 1.0)) + 1.0


def _matches_filters(item: dict[str, Any], filters: dict[str, list[str]] | None) -> bool:
    if not filters:
        return True
    # ── year_range: ["start_year", "end_year"] — range semantics, inclusive ──
    # Preferred over enumerating every year in filters["year"] for multi-year
    # windows.  start/end are 4-digit year strings; both must be present.
    year_range_vals = [str(v).strip() for v in filters.get("year_range", []) if str(v).strip()]
    if len(year_range_vals) >= 2:
        _item_yr = _item_year(item)
        try:
            _yr_start = int(year_range_vals[0])
            _yr_end = int(year_range_vals[-1])
            if _item_yr is None or not (_yr_start <= _item_yr <= _yr_end):
                return False
        except (ValueError, TypeError):
            pass
    for field in FILTER_FIELDS:
        selected = [str(v).strip() for v in filters.get(field, []) if str(v).strip()] if field in filters else []
        if not selected:
            continue
        selected_set = set(selected)
        if field in MULTI_TAG_FIELDS:
            value_tokens = set(_split_multi_tags(item.get(field)))
            if not value_tokens or value_tokens.isdisjoint(selected_set):
                return False
            continue
        value = _filter_scalar_value(item, field)
        if value not in selected_set:
            return False
    return True


def _split_multi_tags(raw: Any) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    normalized = text.replace("\r", "\n")
    parts = [seg.strip() for seg in re.split(r"[;；，,、\n]+", normalized) if seg.strip()]
    deduped: list[str] = []
    seen: set[str] = set()
    for part in parts:
        part = part.strip('"\'`“”‘’ ')
        if not part:
            continue
        if part in seen:
            continue
        seen.add(part)
        deduped.append(part)
    return deduped


def _normalize_multi_tag_field(raw: Any) -> str | None:
    tags = _split_multi_tags(raw)
    return ", ".join(tags) if tags else None


def _sort_text_values(values: set[str]) -> list[str]:
    return sorted(values, key=lambda value: (value.casefold(), value))


def _sort_filter_values(field: str, values: set[str]) -> list[str]:
    if field in {"year", "rating"}:
        numeric: list[tuple[int, str]] = []
        fallback: list[str] = []
        for value in values:
            try:
                numeric.append((int(str(value).strip()), str(value).strip()))
            except Exception:
                fallback.append(str(value).strip())
        numeric.sort(key=lambda item: item[0], reverse=True)
        return [value for _num, value in numeric] + _sort_text_values(set(fallback))
    return _sort_text_values(values)


def _filter_scalar_value(item: dict[str, Any], field: str) -> str:
    if field == "year":
        year_value = _int_or_none(item.get("year"))
        if year_value is None:
            year_value = _item_year(item)
        return str(year_value) if year_value is not None else ""
    if field == "rating":
        rating_value = _int_or_none(item.get("rating"))
        return str(rating_value) if rating_value is not None else ""
    return str(item.get(field) or "").strip()


def _normalize_search_text(text: str) -> str:
    normalized = unicodedata.normalize("NFKC", str(text or "")).strip().lower()
    normalized = re.sub(r"[\s\-:/&+]+", " ", normalized)
    return normalized


def _search_text(item: dict[str, Any]) -> str:
    parts = [str(item.get(k) or "") for k in TEXT_FIELDS]
    return _normalize_search_text(" ".join(parts))


def _extract_keyword_terms(query: str) -> list[str]:
    q = _normalize_search_text(query)
    if not q:
        return []

    terms: list[str] = []
    seen: set[str] = set()

    def add_term(raw: str) -> None:
        value = str(raw or "").strip().lower()
        if not value:
            return
        if value in GENERIC_QUERY_TOKENS:
            return
        if re.fullmatch(r"[\u4e00-\u9fff]+", value) and len(value) < 2:
            return
        if re.fullmatch(r"[a-z0-9_]+", value) and len(value) < 2:
            return
        if value in seen:
            return
        seen.add(value)
        terms.append(value)

    add_term(q)

    prefix = q.split("的", 1)[0].strip()
    if len(prefix) >= 2:
        add_term(prefix)

    latin_phrase_ranges: list[tuple[int, int]] = []
    for match in MULTI_WORD_LATIN_PHRASE_RE.finditer(q):
        normalized_phrase = re.sub(r"[\s\-:/&+]+", " ", match.group(0)).strip()
        if " " not in normalized_phrase:
            continue
        add_term(normalized_phrase)
        latin_phrase_ranges.append(match.span())

    def _covered_by_latin_phrase(start: int, end: int) -> bool:
        return any(start >= phrase_start and end <= phrase_end for phrase_start, phrase_end in latin_phrase_ranges)

    for seg_match in re.finditer(r"[a-z0-9_]+|[\u4e00-\u9fff]{2,}", q):
        segment = seg_match.group(0)
        if re.fullmatch(r"[a-z0-9_]+", segment) and _covered_by_latin_phrase(*seg_match.span()):
            continue
        add_term(segment)

    return terms


def _normalize_ai_label(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    text = text.replace("，", ",").replace("、", ",").replace("；", ",").replace(";", ",")
    lines = [seg.strip() for seg in re.split(r"[\n,]+", text) if seg.strip()]
    tags: list[str] = []
    for line in lines:
        token = re.sub(r"^[0-9\-\*\.\)\(\[\]\s]+", "", line).strip()
        token = token.strip("\"'` ")
        if not token:
            continue
        if token not in tags:
            tags.append(token)
        if len(tags) >= 5:
            break
    return ", ".join(tags)


def _build_ai_label_source_text(item: dict[str, Any]) -> str:
    values: list[str] = []
    for field in AI_LABEL_SOURCE_FIELDS:
        value = item.get(field)
        text = str(value or "").strip()
        if text:
            values.append(text)
    return "\n".join(values).strip()


def _generate_ai_label(item: dict[str, Any]) -> str:
    source_text = _build_ai_label_source_text(item)
    if not source_text:
        return ""
    if not LOCAL_LLM_API_URL or not LOCAL_LLM_MODEL:
        raise RuntimeError("Local LLM config missing for ai_label generation")

    openai_mod = importlib.import_module("openai")
    OpenAI = getattr(openai_mod, "OpenAI", None)
    if OpenAI is None:
        raise RuntimeError("openai.OpenAI unavailable")

    client = OpenAI(api_key=LOCAL_LLM_API_KEY, base_url=LOCAL_LLM_API_URL, timeout=LOCAL_LLM_TIMEOUT)
    messages = [
        {
            "role": "system",
            "content": (
                "你是内容标签生成器。"
                "输入是一段条目内容（无字段名、非JSON）。"
                "请只输出5个中文标签，用逗号分隔。"
                "不要输出解释、不要编号、不要多余文本。"
            ),
        },
        {
            "role": "user",
            "content": source_text,
        },
    ]

    completion = client.chat.completions.create(
        model=LOCAL_LLM_MODEL,
        messages=messages,
        temperature=0.2,
    )
    content = ""
    if completion.choices and completion.choices[0].message:
        content = str(completion.choices[0].message.content or "")
    label = _normalize_ai_label(content)
    if not label:
        raise RuntimeError("Empty ai_label from local LLM")
    return label


def _build_semantic_text(item: dict[str, Any]) -> str:
    # Keep vector space focused on content text; metadata is represented in graph.
    title = str(item.get("title") or "").strip()
    review = str(item.get("review") or "").strip()
    text = "\n".join([x for x in [title, review] if x]).strip()
    return text or title


def _matches_graph_constraints(item: dict[str, Any], constraints: dict[str, list[str]] | None) -> bool:
    if not constraints:
        return True
    for field, selected in (constraints or {}).items():
        options = [str(x).strip() for x in (selected or []) if str(x).strip()]
        if not options:
            continue
        option_set = set(options)
        if field in MULTI_TAG_FIELDS:
            values = set(_split_multi_tags(item.get(field)))
            if not values or values.isdisjoint(option_set):
                return False
            continue
        value = str(item.get(field) or "").strip()
        if value not in option_set:
            return False
    return True


def _keyword_score(item: dict[str, Any], query: str, *, alias_hits: list[dict[str, Any]] | None = None) -> float:
    del alias_hits
    q = query.strip().lower()
    if not q:
        return 0.0
    text = _search_text(item)
    if not text:
        return 0.0
    tokens = _extract_keyword_terms(q)

    if not tokens:
        return 0.0

    return float(sum(_keyword_idf(t) for t in tokens if t in text))


def _tokenize_for_vector(text: str) -> list[str]:
    text = _normalize_search_text(text)
    if not text:
        return []
    chunks = re.findall(r"[a-z0-9_]+|[\u4e00-\u9fff]+", text)
    terms: list[str] = []
    for chunk in chunks:
        if re.fullmatch(r"[\u4e00-\u9fff]+", chunk):
            chars = list(chunk)
            terms.extend(chars)
            if len(chars) >= 2:
                terms.extend("".join(chars[i : i + 2]) for i in range(len(chars) - 1))
        else:
            terms.append(chunk)
    return terms


def _vectorize(text: str) -> dict[str, float]:
    vec: dict[str, float] = {}
    for term in _tokenize_for_vector(text):
        vec[term] = vec.get(term, 0.0) + 1.0
    return vec


def _cosine(a: dict[str, float], b: dict[str, float]) -> float:
    if not a or not b:
        return 0.0
    dot = sum(v * b.get(k, 0.0) for k, v in a.items())
    if dot <= 0:
        return 0.0
    na = math.sqrt(sum(v * v for v in a.values()))
    nb = math.sqrt(sum(v * v for v in b.values()))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def _to_result(item: dict[str, Any], score: float) -> dict[str, Any]:
    result = dict(item)
    # Internal fields are not needed by frontend list rendering.
    result.pop("embedding", None)
    result.pop("embedding_status", None)
    result.pop("ai_label", None)
    result["score"] = round(float(score), 6)
    return result


def _date_sort_key(item: dict[str, Any]) -> tuple[int, int, int]:
    raw = str(item.get("date") or "").strip()
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", raw)
    if not m:
        return (0, 0, 0)
    try:
        return (int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except Exception:
        return (0, 0, 0)


def _keyword_hits(item: dict[str, Any], query: str, context_chars: int = 5) -> list[dict[str, str]]:
    q = (query or "").strip()
    if not q:
        return []

    q_lower = q.lower()
    tokens = _extract_keyword_terms(q_lower)
    hits: list[dict[str, str]] = []

    for field in TEXT_FIELDS:
        value = str(item.get(field) or "")
        if not value:
            continue
        value_lower = value.lower()

        idx = value_lower.find(q_lower)
        matched_len = len(q)
        matched_text = value[idx : idx + matched_len] if idx >= 0 else ""

        if idx < 0:
            token_match = None
            for token in sorted(tokens, key=len, reverse=True):
                t_idx = value_lower.find(token)
                if t_idx >= 0:
                    token_match = (t_idx, token)
                    break
            if not token_match:
                continue
            idx = token_match[0]
            matched_len = len(token_match[1])
            matched_text = value[idx : idx + matched_len]

        start = max(0, idx - context_chars)
        end = min(len(value), idx + matched_len + context_chars)
        snippet = value[start:end]
        if start > 0:
            snippet = "..." + snippet
        if end < len(value):
            snippet = snippet + "..."

        hits.append(
            {
                "field": field,
                "snippet": snippet,
                "term": matched_text,
            }
        )

    return hits


def _build_sql_preview(filters: dict[str, list[str]] | None, query: str, mode: str) -> str:
    clauses: list[str] = []
    filters = filters or {}
    for field in FILTER_FIELDS:
        values = [str(v).strip() for v in filters.get(field, []) if str(v).strip()]
        if not values:
            continue
        if field == "year":
            if len(values) == 1:
                clauses.append(f"substr(date, 1, 4) = '{values[0]}'")
            else:
                joined = ", ".join(f"'{v}'" for v in values)
                clauses.append(f"substr(date, 1, 4) IN ({joined})")
            continue
        if len(values) == 1:
            clauses.append(f"{field} = '{values[0]}'")
        else:
            joined = ", ".join(f"'{v}'" for v in values)
            clauses.append(f"{field} IN ({joined})")

    if query.strip():
        escaped = query.strip().replace("'", "''")
        if mode == "vector":
            clauses.append(f"VECTOR_SIM(content, '{escaped}') > 0")
        else:
            clauses.append(f"content LIKE '%{escaped}%'")

    where = " AND ".join(clauses) if clauses else "1 = 1"
    return f"SELECT * FROM library_items WHERE {where} ORDER BY score DESC;"


def get_filter_options() -> dict[str, list[str]]:
    from . import library_query_service

    return library_query_service.get_filter_options()


def get_form_suggestions() -> dict[str, list[str]]:
    from . import library_query_service

    return library_query_service.get_form_suggestions()


def get_facet_counts(filters: dict[str, list[str]] | None = None) -> dict[str, dict[str, int]]:
    from . import library_query_service

    return library_query_service.get_facet_counts(filters)


def _item_year(item: dict[str, Any]) -> int | None:
    raw = str(item.get("date") or "").strip()
    m = re.match(r"^(\d{4})-", raw)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None


def _stats_labels(item: dict[str, Any], field: str) -> list[str]:
    if field in MULTI_TAG_FIELDS:
        labels = _split_multi_tags(item.get(field))
    else:
        value = str(item.get(field) or "").strip()
        labels = [value] if value else []
    return labels or ["(未填写)"]


def _stats_yearly_rows(items: list[dict[str, Any]], media_types: list[str]) -> list[dict[str, Any]]:
    buckets: dict[int, dict[str, int]] = {}
    for item in items:
        year = _item_year(item)
        media_type = str(item.get("media_type") or "").strip().lower()
        if year is None or media_type not in media_types:
            continue
        row = buckets.setdefault(year, {key: 0 for key in media_types})
        row[media_type] += 1
    output: list[dict[str, Any]] = []
    for year in sorted(buckets):
        row = {"year": year, **buckets[year]}
        row["total"] = sum(int(row.get(media_type) or 0) for media_type in media_types)
        output.append(row)
    return output


def get_stats_dashboard(field: str, year: int | None = None) -> dict[str, Any]:
    from . import library_query_service

    return library_query_service.get_stats_dashboard(field, year=year)


def get_stats_overview() -> dict[str, Any]:
    from . import library_query_service

    return library_query_service.get_stats_overview()


def get_stats_pie(field: str, year: int | None = None) -> dict[str, Any]:
    from . import library_query_service

    return library_query_service.get_stats_pie(field, year=year)


def search_items(
    query: str,
    mode: str,
    filters: dict[str, list[str]] | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    from . import library_query_service

    return library_query_service.search_items(query=query, mode=mode, filters=filters, limit=limit, offset=offset)


def _compute_scored(
    q: str,
    mode: str,
    filters: dict[str, list[str]] | None,
) -> tuple[list[SearchResult], dict[str, Any]]:
    """Build and sort the full scored result list (no pagination)."""
    items = [item for item in _iter_all_items() if _matches_filters(item, filters)]
    graph_expand: dict[str, Any] = {}

    if mode == "vector":
        graph_expand = library_graph.expand_library_query(graph_dir=VECTOR_DB_DIR, query=q)
        query_for_vector = str(graph_expand.get("expanded_query") or q)
        graph_constraints = graph_expand.get("constraints") or {}
        items = [item for item in items if _matches_graph_constraints(item, graph_constraints)]
        q_vec = _vectorize(query_for_vector)
        item_ids = [str(item.get("id") or "") for item in items if str(item.get("id") or "")]
        emb_states = _load_embedding_states(item_ids)
        scored: list[SearchResult] = []
        for item in items:
            item_id = str(item.get("id") or "")
            state = emb_states.get(item_id)
            if not state:
                continue
            if _embedding_status_value(state.get("embedding_status")) != 1:
                continue
            emb = state.get("embedding")
            if not isinstance(emb, dict):
                continue
            emb_vec = {str(k): float(v) for k, v in emb.items() if isinstance(v, (int, float))}
            scored.append(SearchResult(item=item, score=_cosine(q_vec, emb_vec)))
        if q.strip():
            scored = [r for r in scored if r.score > 0]
        scored.sort(key=lambda x: (x.score, _date_sort_key(x.item)), reverse=True)
    else:
        if not q.strip():
            scored = [SearchResult(item=item, score=0.0) for item in items]
            scored.sort(key=lambda x: _date_sort_key(x.item), reverse=True)
        else:
            scored = []
            for item in items:
                scored.append(SearchResult(item=item, score=_keyword_score(item, q)))
            scored = [r for r in scored if r.score > 0]
            scored.sort(key=lambda x: (x.score, _date_sort_key(x.item)), reverse=True)

    return scored, graph_expand


def get_item(item_id: str) -> dict[str, Any]:
    from . import library_query_service

    return library_query_service.get_item(item_id)


def _sanitize_item_for_storage(item: dict[str, Any], fallback_media_type: str | None = None) -> dict[str, Any]:
    media_type = str(item.get("media_type") or fallback_media_type or "").strip().lower()
    if media_type not in MEDIA_FILES:
        raise ValueError("media_type must be one of book/video/music/game")

    cleaned: dict[str, Any] = {
        "media_type": media_type,
        "date": _normalize_date_value(item.get("date"), item),
        "title": item.get("title"),
        "author": item.get("author"),
        "nationality": item.get("nationality"),
        "category": item.get("category"),
        "channel": item.get("channel"),
        "publisher": item.get("publisher"),
        "rating": item.get("rating"),
        "review": item.get("review"),
        "url": item.get("url"),
        "cover_path": item.get("cover_path"),
    }

    if not str(cleaned.get("title") or "").strip():
        raise ValueError("title is required")

    for key, value in list(cleaned.items()):
        if isinstance(value, str):
            value = value.strip()
            cleaned[key] = value or None

    for field in MULTI_TAG_FIELDS:
        cleaned[field] = _normalize_multi_tag_field(cleaned.get(field))

    return cleaned


def _int_or_none(v: Any) -> int | None:
    try:
        if v is None:
            return None
        s = str(v).strip()
        if not s:
            return None
        return int(float(s))
    except Exception:
        return None


def _normalize_date_value(raw_date: Any, item: dict[str, Any] | None = None) -> str | None:
    text = str(raw_date or "").strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) == 8:
        y = _int_or_none(digits[:4])
        m = _int_or_none(digits[4:6])
        d = _int_or_none(digits[6:8])
        if y and y > 0:
            m = 1 if not m or m <= 0 else min(12, m)
            d = 1 if not d or d <= 0 else min(31, d)
            return f"{y:04d}-{m:02d}-{d:02d}"

    if item:
        y = _int_or_none(item.get("year"))
        m = _int_or_none(item.get("month"))
        d = _int_or_none(item.get("day"))
        if y and y > 0:
            m = 1 if not m or m <= 0 else min(12, m)
            d = 1 if not d or d <= 0 else min(31, d)
            return f"{y:04d}-{m:02d}-{d:02d}"

    return None


def _sanitize_title_for_filename(title: str | None) -> str:
    """Convert title to safe filename slug."""
    if not title:
        return ""
    # Remove non-alphanumeric chars, keep Chinese/Japanese/Korean characters
    slug = re.sub(r'[<>:"/\\|?*]', '', title.strip())
    slug = re.sub(r'\s+', '_', slug)
    # Limit length to prevent filesystem issues
    if len(slug) > 50:
        slug = slug[:50]
    return slug or ""


def save_cover_bytes(
    data: bytes,
    content_type: str,
    original_filename: str | None = None,
    title: str | None = None,
    overwrite_path: str | None = None,
) -> str:
    from . import library_mutation_service

    return library_mutation_service.save_cover_bytes(
        data,
        content_type,
        original_filename=original_filename,
        title=title,
        overwrite_path=overwrite_path,
    )


def delete_item(item_id: str) -> dict[str, Any]:
    from . import library_mutation_service

    return library_mutation_service.delete_item(item_id)


def add_item(item: dict[str, Any]) -> dict[str, Any]:
    from . import library_mutation_service

    return library_mutation_service.add_item(item)


def update_item(item_id: str, patch: dict[str, Any]) -> dict[str, Any]:
    from . import library_mutation_service

    return library_mutation_service.update_item(item_id, patch)


def refresh_pending_embeddings() -> dict[str, Any]:
    from . import library_embedding_refresh_service

    return library_embedding_refresh_service.refresh_pending_embeddings()


def refresh_embeddings_for_item_ids(item_ids: list[str]) -> dict[str, Any]:
    from . import library_embedding_refresh_service

    return library_embedding_refresh_service.refresh_embeddings_for_item_ids(item_ids)
