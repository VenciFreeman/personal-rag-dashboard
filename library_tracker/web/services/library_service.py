from __future__ import annotations

import json
import math
import os
import re
import sqlite3
from datetime import datetime
from dataclasses import dataclass
import importlib
from io import BytesIO
from pathlib import Path
from typing import Any
from uuid import uuid4

from PIL import Image

from web.settings import COVERS_DIR, DATA_DIR, MEDIA_FILES, VECTOR_DB_DIR
from . import library_graph

TEXT_FIELDS = ["title", "author", "nationality", "category", "channel", "review", "publisher", "url"]
FILTER_FIELDS = ["media_type", "nationality", "category", "channel"]
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

_local_llm_url_raw = os.getenv("LIBRARY_TRACKER_LOCAL_LLM_URL", "http://127.0.0.1:1234").strip()
if _local_llm_url_raw and not re.search(r"/v1/?$", _local_llm_url_raw):
    LOCAL_LLM_API_URL = _local_llm_url_raw.rstrip("/") + "/v1"
else:
    LOCAL_LLM_API_URL = _local_llm_url_raw
LOCAL_LLM_MODEL = os.getenv("LIBRARY_TRACKER_LOCAL_LLM_MODEL", "qwen2.5-7b-instruct").strip()
LOCAL_LLM_API_KEY = os.getenv("LIBRARY_TRACKER_LOCAL_LLM_API_KEY", "local").strip() or "local"
LOCAL_LLM_TIMEOUT = int(os.getenv("LIBRARY_TRACKER_LOCAL_LLM_TIMEOUT", "120"))
EMBEDDING_DB_PATH = VECTOR_DB_DIR / "library_embeddings.sqlite3"


def _embedding_status_value(raw: Any) -> int:
    try:
        return 1 if int(raw or 0) == 1 else 0
    except Exception:
        return 0


@dataclass
class SearchResult:
    item: dict[str, Any]
    score: float


class ItemNotFoundError(KeyError):
    pass


class BadItemIdError(ValueError):
    pass


def _json_path(media_type: str) -> Path:
    filename = MEDIA_FILES.get(media_type)
    if not filename:
        raise ValueError(f"Unsupported media_type: {media_type}")
    return DATA_DIR / filename


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
    path = _json_path(media_type)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload["record_count"] = len(payload.get("records", []))
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


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


def _matches_filters(item: dict[str, Any], filters: dict[str, list[str]] | None) -> bool:
    if not filters:
        return True
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
        value = str(item.get(field) or "").strip()
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


def _search_text(item: dict[str, Any]) -> str:
    parts = [str(item.get(k) or "") for k in TEXT_FIELDS]
    return " ".join(parts).strip().lower()


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


def _keyword_score(item: dict[str, Any], query: str) -> float:
    q = query.strip().lower()
    if not q:
        return 0.0
    text = _search_text(item)
    if not text:
        return 0.0
    count = text.count(q)
    if count > 0:
        return float(count)

    # Natural-language Chinese queries are often long; extract title-like segment and chunks.
    tokens: list[str] = [t for t in re.split(r"\s+", q) if t]
    prefix = q.split("的", 1)[0].strip()
    if len(prefix) >= 2 and prefix not in tokens:
        tokens.append(prefix)

    for seg in re.findall(r"[a-z0-9_]+|[\u4e00-\u9fff]{2,}", q):
        val = str(seg).strip()
        if len(val) >= 2 and val not in tokens:
            tokens.append(val)

    if not tokens:
        return 0.0

    return float(sum(1 for t in tokens if t in text))


def _tokenize_for_vector(text: str) -> list[str]:
    text = (text or "").lower().strip()
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
    tokens = [t for t in re.split(r"\s+", q_lower) if t]
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
    items = _iter_all_items()
    options: dict[str, set[str]] = {k: set() for k in FILTER_FIELDS}
    for item in items:
        for field in FILTER_FIELDS:
            if field in MULTI_TAG_FIELDS:
                for token in _split_multi_tags(item.get(field)):
                    options[field].add(token)
                continue
            value = str(item.get(field) or "").strip()
            if value:
                options[field].add(value)
    return {k: _sort_text_values(v) for k, v in options.items()}


def get_form_suggestions() -> dict[str, list[str]]:
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


def get_facet_counts(filters: dict[str, list[str]] | None = None) -> dict[str, dict[str, int]]:
    """Return per-field option counts under current cross-field filters.

    For each target field, count values while applying filters from other fields only.
    This enables linked filtering behavior in the frontend.
    """
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
            value = str(item.get(target_field) or "").strip()
            if not value:
                continue
            field_counts[value] = field_counts.get(value, 0) + 1
        counts[target_field] = field_counts

    return counts


def _item_year(item: dict[str, Any]) -> int | None:
    raw = str(item.get("date") or "").strip()
    m = re.match(r"^(\d{4})-", raw)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None


def get_stats_overview() -> dict[str, Any]:
    media_types = list(MEDIA_FILES.keys())
    current_year = datetime.now().year
    items = _iter_all_items()
    total_by_media = {k: 0 for k in media_types}
    current_year_by_media = {k: 0 for k in media_types}
    years: set[int] = set()

    for item in items:
        media_type = str(item.get("media_type") or "").strip().lower()
        if media_type not in total_by_media:
            continue
        total_by_media[media_type] += 1
        y = _item_year(item)
        if y is not None:
            years.add(y)
        if y == current_year:
            current_year_by_media[media_type] += 1

    return {
        "current_year": current_year,
        "media_types": media_types,
        "total_by_media": total_by_media,
        "current_year_by_media": current_year_by_media,
        "total_all": sum(total_by_media.values()),
        "current_year_all": sum(current_year_by_media.values()),
        "available_years": sorted(years, reverse=True),
        "dimension_fields": list(STATS_DIMENSION_FIELDS),
    }


def get_stats_pie(field: str, year: int | None = None) -> dict[str, Any]:
    dimension = str(field or "").strip().lower()
    if dimension not in STATS_DIMENSION_FIELDS:
        raise ValueError(f"Unsupported field: {dimension}")

    media_types = list(MEDIA_FILES.keys())
    counts_by_media: dict[str, dict[str, int]] = {k: {} for k in media_types}
    items = _iter_all_items()

    for item in items:
        media_type = str(item.get("media_type") or "").strip().lower()
        if media_type not in counts_by_media:
            continue
        if year is not None and _item_year(item) != year:
            continue

        if dimension in MULTI_TAG_FIELDS:
            labels = _split_multi_tags(item.get(dimension))
        else:
            val = str(item.get(dimension) or "").strip()
            labels = [val] if val else []

        if not labels:
            labels = ["(未填写)"]

        bucket = counts_by_media[media_type]
        for label in labels:
            bucket[label] = bucket.get(label, 0) + 1

    charts: dict[str, list[dict[str, Any]]] = {}
    for media_type in media_types:
        pairs = sorted(counts_by_media[media_type].items(), key=lambda kv: (-kv[1], kv[0]))
        charts[media_type] = [{"label": k, "value": v} for k, v in pairs[:12]]

    return {
        "field": dimension,
        "year": year,
        "charts": charts,
    }


def search_items(
    query: str,
    mode: str,
    filters: dict[str, list[str]] | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    mode = (mode or "keyword").strip().lower()
    if mode not in {"keyword", "vector"}:
        mode = "keyword"

    items = [item for item in _iter_all_items() if _matches_filters(item, filters)]
    q = query or ""

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
            scored = [SearchResult(item=item, score=_keyword_score(item, q)) for item in items]
            scored = [r for r in scored if r.score > 0]
            scored.sort(key=lambda x: (x.score, _date_sort_key(x.item)), reverse=True)

    trimmed = scored[: max(1, int(limit))]
    return {
        "query": q,
        "mode": mode,
        "sql_preview": _build_sql_preview(filters, q, mode),
        "count": len(trimmed),
        "graph_expansion": graph_expand if mode == "vector" else {},
        "results": [
            {
                **_to_result(r.item, r.score),
                "keyword_hits": _keyword_hits(r.item, q) if mode == "keyword" and q.strip() else [],
            }
            for r in trimmed
        ],
    }


def get_item(item_id: str) -> dict[str, Any]:
    media_type, index = _parse_item_id(item_id)
    payload = _load_payload(media_type)
    records = payload.get("records", [])
    if index < 0 or index >= len(records):
        raise ItemNotFoundError(item_id)
    item = records[index]
    if not isinstance(item, dict):
        raise ItemNotFoundError(item_id)
    return _normalize_item(item, media_type, index)


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
    if not data:
        raise ValueError("Empty cover content")

    ext = _CONTENT_TYPE_EXT.get((content_type or "").lower(), "")
    if not ext:
        # Fallback to original filename extension when content-type is uncommon.
        raw_name = (original_filename or "").strip()
        if raw_name:
            candidate = Path(raw_name).suffix.lower()
            if candidate in {".jpg", ".jpeg", ".png", ".webp", ".gif"}:
                ext = ".jpg" if candidate == ".jpeg" else candidate
    if not ext:
        raise ValueError("Unsupported image type")

    # Compress image if larger than 3MB
    max_size = 3 * 1024 * 1024
    if len(data) > max_size:
        try:
            img = Image.open(BytesIO(data))
            # Convert RGBA to RGB if necessary
            if img.mode in ("RGBA", "LA", "P"):
                rgb_img = Image.new("RGB", img.size, (255, 255, 255))
                if img.mode == "P":
                    img = img.convert("RGBA")
                rgb_img.paste(img, mask=img.split()[-1] if img.mode in ("RGBA", "LA") else None)
                img = rgb_img
            
            # Compress with decreasing quality until under 3MB
            quality = 85
            while quality > 20:
                buffer = BytesIO()
                img.save(buffer, format="JPEG", quality=quality, optimize=True)
                compressed_data = buffer.getvalue()
                if len(compressed_data) <= max_size:
                    data = compressed_data
                    ext = ".jpg"  # Force JPEG extension after compression
                    break
                quality -= 10
        except Exception as e:
            # If compression fails, use original data
            print(f"[COMPRESS_WARNING] Failed to compress image: {e}")

    COVERS_DIR.mkdir(parents=True, exist_ok=True)

    normalized_overwrite = str(overwrite_path or "").replace("\\", "/").strip()
    if normalized_overwrite:
        if not normalized_overwrite.startswith("covers/"):
            raise ValueError("Invalid overwrite path")
        rel_name = normalized_overwrite[len("covers/"):].strip()
        if not rel_name or "/" in rel_name:
            raise ValueError("Invalid overwrite path")
        out_path = COVERS_DIR / rel_name
        out_path.write_bytes(data)
        return f"covers/{rel_name}"

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Generate filename with title slug if available
    title_slug = _sanitize_title_for_filename(title)
    if title_slug:
        filename = f"{title_slug}_{stamp}{ext}"
    else:
        filename = f"{stamp}_{uuid4().hex[:10]}{ext}"
    
    out_path = COVERS_DIR / filename
    out_path.write_bytes(data)
    # Return path relative to /media mount root.
    return f"covers/{filename}"


def add_item(item: dict[str, Any]) -> dict[str, Any]:
    cleaned = _sanitize_item_for_storage(item)
    media_type = str(cleaned["media_type"])
    payload = _load_payload(media_type)
    records = payload.get("records", [])
    if not isinstance(records, list):
        records = []
    records.append(cleaned)
    payload["records"] = records
    _save_payload(media_type, payload)
    index = len(records) - 1
    item_id = f"{media_type}:{index}"
    _mark_item_pending(item_id)
    return _normalize_item(cleaned, media_type, index)


def update_item(item_id: str, patch: dict[str, Any]) -> dict[str, Any]:
    media_type, index = _parse_item_id(item_id)
    payload = _load_payload(media_type)
    records = payload.get("records", [])
    if index < 0 or index >= len(records):
        raise ItemNotFoundError(item_id)
    base = records[index]
    if not isinstance(base, dict):
        raise ItemNotFoundError(item_id)

    merged = dict(base)
    for key, value in patch.items():
        if key == "id":
            continue
        merged[key] = value
    merged["media_type"] = media_type

    cleaned = _sanitize_item_for_storage(merged, fallback_media_type=media_type)
    # Any content update invalidates embedding; refresh action will regenerate it.
    records[index] = cleaned
    payload["records"] = records
    _save_payload(media_type, payload)
    _mark_item_pending(item_id)
    return _normalize_item(cleaned, media_type, index)


def refresh_pending_embeddings() -> dict[str, Any]:
    """Generate ai_label and embeddings for records with embedding_status=0 and mark as 1."""
    scanned = 0
    refreshed = 0
    failed = 0
    all_items = _iter_all_items()
    item_ids = [str(item.get("id") or "") for item in all_items if str(item.get("id") or "")]
    states = _load_embedding_states(item_ids)

    with _embedding_db_conn() as conn:
        pending_writes = 0
        for item in all_items:
            item_id = str(item.get("id") or "")
            if not item_id:
                continue
            scanned += 1
            state = states.get(item_id)
            status = _embedding_status_value((state or {}).get("embedding_status"))
            if status == 1:
                continue

            try:
                semantic_text = _build_semantic_text(item)
                ai_label = _generate_ai_label(item)
                vec = _vectorize(semantic_text)
                _upsert_embedding_state(
                    conn,
                    item_id=item_id,
                    embedding_status=1,
                    ai_label=ai_label,
                    embedding=vec if vec else None,
                )
                refreshed += 1
                pending_writes += 1
            except Exception as exc:
                _upsert_embedding_state(
                    conn,
                    item_id=item_id,
                    embedding_status=0,
                    ai_label=None,
                    embedding=None,
                )
                failed += 1
                pending_writes += 1
                print(f"[EMBED_REFRESH_WARNING] {item_id}: {exc}")

            if pending_writes >= 20:
                conn.commit()
                pending_writes = 0

        if pending_writes:
            conn.commit()

    graph_stats = library_graph.sync_library_graph(
        graph_dir=VECTOR_DB_DIR,
        items=all_items,
        target_item_ids=None,
        only_missing=True,
    )
    return {
        "scanned": scanned,
        "refreshed": refreshed,
        "failed": failed,
        "graph": graph_stats,
    }


def refresh_embeddings_for_item_ids(item_ids: list[str]) -> dict[str, Any]:
    """Refresh embeddings for specific items (typically newly added or updated rows)."""
    normalized_ids = [str(x).strip() for x in (item_ids or []) if str(x).strip()]
    if not normalized_ids:
        return {"scanned": 0, "refreshed": 0, "failed": 0}

    target_set = set(normalized_ids)
    scanned = 0
    refreshed = 0
    failed = 0
    all_items = _iter_all_items()
    item_map = {str(item.get("id") or ""): item for item in all_items if str(item.get("id") or "") in target_set}
    states = _load_embedding_states(list(item_map.keys()))

    with _embedding_db_conn() as conn:
        pending_writes = 0
        for item_id in normalized_ids:
            item = item_map.get(item_id)
            if not item:
                continue
            scanned += 1
            try:
                semantic_text = _build_semantic_text(item)
                ai_label = _generate_ai_label(item)
                vec = _vectorize(semantic_text)
                _upsert_embedding_state(
                    conn,
                    item_id=item_id,
                    embedding_status=1,
                    ai_label=ai_label,
                    embedding=vec if vec else None,
                )
                refreshed += 1
                pending_writes += 1
            except Exception as exc:
                # Keep pending state when single-item refresh fails.
                _upsert_embedding_state(
                    conn,
                    item_id=item_id,
                    embedding_status=0,
                    ai_label=(states.get(item_id) or {}).get("ai_label"),
                    embedding=(states.get(item_id) or {}).get("embedding"),
                )
                failed += 1
                pending_writes += 1
                print(f"[EMBED_REFRESH_WARNING] {item_id}: {exc}")

        if pending_writes:
            conn.commit()

    graph_stats = library_graph.sync_library_graph(
        graph_dir=VECTOR_DB_DIR,
        items=list(item_map.values()),
        target_item_ids=set(normalized_ids),
        only_missing=True,
    )
    return {
        "scanned": scanned,
        "refreshed": refreshed,
        "failed": failed,
        "graph": graph_stats,
    }
