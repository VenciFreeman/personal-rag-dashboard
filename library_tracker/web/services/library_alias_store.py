from __future__ import annotations

import json
import os
import re
import threading
import unicodedata
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from core_service import get_settings
from ..settings import get_alias_bucket_dir

_WRITE_LOCK = threading.RLock()
_CORE_SETTINGS = get_settings()

MEDIA_TYPES = ("book", "video", "music", "game")
ENTRY_FIELDS = ("title", "author", "publisher")
FIELD_TYPE_MAP = {"title": "title", "author": "creator", "publisher": "publisher"}
ITEM_FIELD_MAP = {"title": "title", "creator": "author", "publisher": "publisher"}
SPLIT_VALUE_FIELDS = {"author", "publisher"}
MAX_CONTEXT_ITEMS = 3
DEFAULT_BATCH_SIZE = 12

PROPOSAL_DIR = get_alias_bucket_dir("proposal")
APPROVED_DIR = get_alias_bucket_dir("approved")
KEEP_DIR = get_alias_bucket_dir("keep_original")
_APPROVED_REGISTRY_CACHE: dict[str, Any] = {"signature": None, "registry": None}

_SPLIT_PATTERN = re.compile(r"[;,，；/、|&\n\r\t]+")
_WHITESPACE_RE = re.compile(r"\s+")


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _normalize_key(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    text = _WHITESPACE_RE.sub("", text)
    return text.casefold().strip()


def _fingerprint(field_type: str, media_type: str, raw_value: str) -> str:
    return f"{field_type}|{media_type}|{_normalize_key(raw_value)}"


def _stable_sort_key(entry: dict[str, Any]) -> tuple[str, str]:
    normalized = _normalize_key(entry.get("normalized") or entry.get("canonical"))
    canonical = _normalize_text(entry.get("canonical"))
    return (normalized, canonical.casefold())


def _entry_field_from_key(key: str) -> str:
    parts = str(key or "").split("|", 1)
    field = parts[1].strip().lower() if len(parts) == 2 else ""
    return field if field in ENTRY_FIELDS else "title"


def _field_type_for_field(field: str) -> str:
    return FIELD_TYPE_MAP.get(field, "title")


def _field_for_field_type(field_type: str) -> str:
    return ITEM_FIELD_MAP.get(str(field_type or "").strip().lower(), "title")


def _bucket_filename(media_type: str, field: str, bucket: str) -> str:
    if bucket == "proposal":
        return f"{media_type}_{field}_proposals.json"
    if bucket == "approved":
        return f"{media_type}_{field}_alias.json"
    if bucket == "keep_original":
        return f"{media_type}_{field}_keep.json"
    raise ValueError(f"Unsupported bucket: {bucket}")


def _bucket_dir(bucket: str) -> Path:
    return get_alias_bucket_dir(bucket)


def _bucket_path(media_type: str, field: str, bucket: str) -> Path:
    return _bucket_dir(bucket) / _bucket_filename(media_type, field, bucket)


def _bucket_path_candidates(media_type: str, field: str, bucket: str) -> list[Path]:
    return [_bucket_path(media_type, field, bucket)]


def _empty_payload() -> dict[str, list[dict[str, Any]]]:
    return {"entries": []}


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def _load_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        return _empty_payload()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return _empty_payload()
    if not isinstance(payload, dict):
        return _empty_payload()
    entries = payload.get("entries")
    payload["entries"] = [entry for entry in entries if isinstance(entry, dict)] if isinstance(entries, list) else []
    return payload


def _load_payload_with_compat(paths: list[Path]) -> dict[str, Any]:
    merged_entries: list[dict[str, Any]] = []
    for path in paths:
        payload = _load_payload(path)
        merged_entries.extend([entry for entry in list(payload.get("entries") or []) if isinstance(entry, dict)])
    if not merged_entries:
        return _empty_payload()
    return {"entries": merged_entries}


def _dedupe_texts(values: list[Any], *, exclude: str = "") -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    excluded_key = _normalize_key(exclude)
    for value in values:
        text = _normalize_text(value)
        if not text:
            continue
        key = _normalize_key(text)
        if not key or key == excluded_key or key in seen:
            continue
        seen.add(key)
        output.append(text)
    return output


def _split_value(value: Any, *, field: str) -> list[str]:
    text = _normalize_text(value)
    if not text:
        return []
    if field not in SPLIT_VALUE_FIELDS:
        return [text]
    return [part.strip() for part in _SPLIT_PATTERN.split(text) if part and part.strip()]


def _sample_label(item: dict[str, Any], *, field: str, canonical: str) -> str:
    title = _normalize_text(item.get("title"))
    author = _normalize_text(item.get("author"))
    publisher = _normalize_text(item.get("publisher"))
    if field == "title":
        suffix = f"author={author}" if author else (f"publisher={publisher}" if publisher else "")
        return f"{canonical} | {suffix}" if suffix else canonical
    if field == "author":
        suffix = f"title={title}" if title else (f"publisher={publisher}" if publisher else "")
        return f"{canonical} | {suffix}" if suffix else canonical
    suffix = f"title={title}" if title else (f"author={author}" if author else "")
    return f"{canonical} | {suffix}" if suffix else canonical


def _proposal_entry_from_candidate(candidate: dict[str, Any], *, aliases: list[str], confidence: float) -> dict[str, Any]:
    field = str(candidate.get("field") or "title")
    canonical = _normalize_text(candidate.get("canonical"))
    normalized = _normalize_key(candidate.get("normalized") or canonical)
    return {
        "id": str(uuid4()),
        "key": f"{candidate['media_type']}|{field}",
        "media_type": candidate["media_type"],
        "canonical": canonical,
        "normalized": normalized,
        "aliases": _dedupe_texts(aliases, exclude=canonical),
        "confidence": max(0.0, min(1.0, float(confidence or 0.0))),
        "samples": list(candidate.get("samples") or []),
        "source_item_ids": list(candidate.get("source_item_ids") or []),
        "generated_at": _now_iso(),
        "status": "proposal",
    }


def _approved_entry_from_proposal(entry: dict[str, Any], aliases: list[str]) -> dict[str, Any]:
    return {
        "key": str(entry.get("key") or ""),
        "media_type": str(entry.get("media_type") or ""),
        "canonical": _normalize_text(entry.get("canonical")),
        "normalized": _normalize_key(entry.get("normalized") or entry.get("canonical")),
        "aliases": _dedupe_texts(aliases, exclude=_normalize_text(entry.get("canonical"))),
        "status": "approved",
    }


def _keep_entry_from_proposal(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "key": str(entry.get("key") or ""),
        "media_type": str(entry.get("media_type") or ""),
        "canonical": _normalize_text(entry.get("canonical")),
        "normalized": _normalize_key(entry.get("normalized") or entry.get("canonical")),
        "status": "keep_original",
    }


def _sorted_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(entries, key=_stable_sort_key)


def _load_bucket_entries(media_type: str, field: str, bucket: str) -> list[dict[str, Any]]:
    with _WRITE_LOCK:
        payload = _load_payload_with_compat(_bucket_path_candidates(media_type, field, bucket))
        entries = payload.get("entries") if isinstance(payload.get("entries"), list) else []
        normalized_entries: list[dict[str, Any]] = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            row = dict(entry)
            row.setdefault("key", f"{media_type}|{field}")
            row.setdefault("media_type", media_type)
            row["field"] = field
            row["field_type"] = _field_type_for_field(field)
            row["canonical"] = _normalize_text(row.get("canonical"))
            row["normalized"] = _normalize_key(row.get("normalized") or row.get("canonical"))
            if bucket == "approved":
                row["aliases"] = _dedupe_texts(list(row.get("aliases") or []), exclude=row.get("canonical"))
                row["status"] = "approved"
            elif bucket == "proposal":
                row["aliases"] = _dedupe_texts(list(row.get("aliases") or []), exclude=row.get("canonical"))
                row["samples"] = [str(item).strip() for item in list(row.get("samples") or []) if str(item).strip()][:MAX_CONTEXT_ITEMS]
                row["source_item_ids"] = [str(item).strip() for item in list(row.get("source_item_ids") or []) if str(item).strip()]
                row["status"] = "proposal"
                row["id"] = str(row.get("id") or uuid4())
                row["generated_at"] = str(row.get("generated_at") or _now_iso())
                try:
                    row["confidence"] = max(0.0, min(1.0, float(row.get("confidence") or 0.0)))
                except Exception:
                    row["confidence"] = 0.0
            else:
                row.pop("aliases", None)
                row["status"] = "keep_original"
            normalized_entries.append(row)
        return _sorted_entries(normalized_entries)


def _write_bucket_entries(media_type: str, field: str, bucket: str, entries: list[dict[str, Any]]) -> None:
    payload = {"entries": _sorted_entries(entries)}
    _atomic_write_json(_bucket_path(media_type, field, bucket), payload)


def _all_bucket_paths() -> list[Path]:
    paths: list[Path] = []
    for bucket in ("proposal", "approved", "keep_original"):
        for media_type in MEDIA_TYPES:
            for field in ENTRY_FIELDS:
                paths.append(_bucket_path(media_type, field, bucket))
    return paths


def _approved_bucket_paths() -> list[Path]:
    paths: list[Path] = []
    for media_type in MEDIA_TYPES:
        for field in ENTRY_FIELDS:
            paths.extend(_bucket_path_candidates(media_type, field, "approved"))
    return paths


def alias_proposal_file_signature() -> tuple[int, int]:
    with _WRITE_LOCK:
        mtime_total = 0
        size_total = 0
        for path in _all_bucket_paths():
            try:
                stat = path.stat()
            except Exception:
                continue
            mtime_total += int(stat.st_mtime_ns)
            size_total += int(stat.st_size)
        return (mtime_total, size_total)


def approved_alias_registry_signature() -> tuple[int, int]:
    with _WRITE_LOCK:
        mtime_total = 0
        size_total = 0
        for path in _approved_bucket_paths():
            try:
                stat = path.stat()
            except Exception:
                continue
            mtime_total += int(stat.st_mtime_ns)
            size_total += int(stat.st_size)
        return (mtime_total, size_total)


def _bucket_has_normalized(media_type: str, field: str, normalized: str) -> bool:
    if not normalized:
        return False
    for bucket in ("approved", "keep_original", "proposal"):
        for entry in _load_bucket_entries(media_type, field, bucket):
            if _normalize_key(entry.get("normalized") or entry.get("canonical")) == normalized:
                return True
    return False


def _iter_candidate_inputs(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}
    ordered: list[dict[str, Any]] = []
    for item in items:
        media_type = str(item.get("media_type") or "").strip().lower()
        if media_type not in MEDIA_TYPES:
            continue
        item_id = str(item.get("id") or "").strip()
        for field in ENTRY_FIELDS:
            for raw_value in _split_value(item.get(field), field=field):
                canonical = _normalize_text(raw_value)
                normalized = _normalize_key(canonical)
                if not canonical or not normalized:
                    continue
                fingerprint = f"{media_type}|{field}|{normalized}"
                bucket = buckets.get(fingerprint)
                if bucket is None:
                    bucket = {
                        "media_type": media_type,
                        "field": field,
                        "canonical": canonical,
                        "normalized": normalized,
                        "samples": [],
                        "source_item_ids": [],
                    }
                    buckets[fingerprint] = bucket
                    ordered.append(bucket)
                if item_id and item_id not in bucket["source_item_ids"]:
                    bucket["source_item_ids"].append(item_id)
                sample = _sample_label(item, field=field, canonical=canonical)
                if sample and sample not in bucket["samples"] and len(bucket["samples"]) < MAX_CONTEXT_ITEMS:
                    bucket["samples"].append(sample)
    return ordered


def _active_candidate_fingerprints(items: list[dict[str, Any]]) -> set[str]:
    fingerprints: set[str] = set()
    for candidate in _iter_candidate_inputs(items):
        media_type = str(candidate.get("media_type") or "").strip().lower()
        field = str(candidate.get("field") or "").strip().lower()
        normalized = _normalize_key(candidate.get("normalized") or candidate.get("canonical"))
        if media_type in MEDIA_TYPES and field in ENTRY_FIELDS and normalized:
            fingerprints.add(f"{media_type}|{field}|{normalized}")
    return fingerprints


def _llm_settings() -> tuple[str, str, str]:
    url = (os.getenv("LIBRARY_TRACKER_LOCAL_LLM_URL") or "").strip() or _CORE_SETTINGS.local_llm_url or "http://127.0.0.1:1234/v1"
    if url and not url.rstrip("/").endswith("/v1"):
        url = url.rstrip("/") + "/v1"
    model = (os.getenv("LIBRARY_TRACKER_LOCAL_LLM_MODEL") or "").strip() or _CORE_SETTINGS.local_llm_model
    api_key = (os.getenv("LIBRARY_TRACKER_LOCAL_LLM_API_KEY") or "").strip() or _CORE_SETTINGS.local_llm_api_key or "local"
    return url, model, api_key


def _extract_json_object(raw: str) -> dict[str, Any] | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    fence = re.search(r"```(?:json)?\s*([\s\S]*?)```", text, flags=re.IGNORECASE)
    if fence:
        try:
            parsed = json.loads(fence.group(1).strip())
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        try:
            parsed = json.loads(text[start : end + 1])
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return None
    return None


def _call_llm(prompt: str, *, timeout: int = 120) -> str:
    url, model, api_key = _llm_settings()
    if not url or not model:
        raise RuntimeError("Local LLM is not configured")
    endpoint = url.rstrip("/") + "/chat/completions"
    body = json.dumps(
        {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.2,
            "max_tokens": 1800,
        },
        ensure_ascii=False,
    ).encode("utf-8")
    req = urllib.request.Request(
        endpoint,
        data=body,
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    choices = payload.get("choices") or []
    if not choices:
        raise RuntimeError("LLM returned no choices")
    return str(choices[0].get("message", {}).get("content", "") or "").strip()


def _build_generation_prompt(candidates: list[dict[str, Any]]) -> str:
    prompt_payload = []
    for index, row in enumerate(candidates, start=1):
        prompt_payload.append(
            {
                "candidate_id": str(index),
                "media_type": row["media_type"],
                "field": row["field"],
                "canonical": row["canonical"],
                "samples": row.get("samples") or [],
            }
        )
    return (
        "你是个人媒体库 alias 生成器。\n"
        "任务：为 title/author/publisher 生成常见检索别名、简称、缩写或其他常用写法，供人工审核。\n"
        "输出必须是 JSON 对象，格式为 {\"proposals\":[{\"candidate_id\":\"1\",\"aliases\":[...],\"confidence\":0.0}]}。\n"
        "规则：\n"
        "1. aliases 只保留常见变体，不要重复 canonical。\n"
        "2. 只补常见检索写法，不要为了翻译而翻译；仅在常见用法稳定时补中英文名、简称、缩写、昵称或常见译名。\n"
        "3. 没有把握时 aliases 可以为空，confidence 降低。\n"
        "4. 不要输出解释文本。\n\n"
        f"输入候选：\n{json.dumps(prompt_payload, ensure_ascii=False, indent=2)}"
    )


def _llm_generate_batch(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    raw = _call_llm(_build_generation_prompt(candidates))
    parsed = _extract_json_object(raw)
    rows = parsed.get("proposals") if isinstance(parsed, dict) and isinstance(parsed.get("proposals"), list) else []
    by_id = {str(index): row for index, row in enumerate(candidates, start=1)}
    outputs: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        candidate = by_id.get(str(row.get("candidate_id") or "").strip())
        if candidate is None:
            continue
        fingerprint = f"{candidate['media_type']}|{candidate['field']}|{candidate['normalized']}"
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        try:
            confidence = float(row.get("confidence") or 0.0)
        except Exception:
            confidence = 0.0
        outputs.append(
            {
                "candidate": candidate,
                "aliases": _dedupe_texts(list(row.get("aliases") or []), exclude=candidate["canonical"]),
                "confidence": max(0.0, min(1.0, confidence)),
            }
        )
    return outputs


def _proposal_counts() -> dict[str, int]:
    return {bucket: 0 for bucket in ("proposal", "approved", "keep_original")}


def get_alias_proposal_summary() -> dict[str, Any]:
    with _WRITE_LOCK:
        summary = _proposal_counts()
        latest = ""
        for media_type in MEDIA_TYPES:
            for field in ENTRY_FIELDS:
                summary["proposal"] += len(_load_bucket_entries(media_type, field, "proposal"))
                summary["approved"] += len(_load_bucket_entries(media_type, field, "approved"))
                summary["keep_original"] += len(_load_bucket_entries(media_type, field, "keep_original"))
        for path in _all_bucket_paths():
            try:
                stamp = datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds")
            except Exception:
                continue
            latest = max(latest, stamp)
        return {
            "pending_count": summary["proposal"],
            "approved_count": summary["approved"],
            "keep_original_count": summary["keep_original"],
            "updated_at": latest,
        }


def clear_alias_proposals() -> dict[str, Any]:
    with _WRITE_LOCK:
        for path in _all_bucket_paths():
            if path.exists():
                path.unlink()
    return get_alias_proposal_summary()


def list_proposals(*, page: int = 1, page_size: int = 10) -> dict[str, Any]:
    with _WRITE_LOCK:
        rows: list[dict[str, Any]] = []
        for media_type in MEDIA_TYPES:
            for field in ENTRY_FIELDS:
                rows.extend(_load_bucket_entries(media_type, field, "proposal"))
        rows.sort(key=lambda row: str(row.get("generated_at") or ""), reverse=True)
        normalized_page_size = max(1, min(int(page_size or 10), 50))
        normalized_page = max(1, int(page or 1))
        total = len(rows)
        start = (normalized_page - 1) * normalized_page_size
        end = start + normalized_page_size
        return {
            "items": rows[start:end],
            "page": normalized_page,
            "page_size": normalized_page_size,
            "total": total,
            "total_pages": max(1, (total + normalized_page_size - 1) // normalized_page_size),
            "summary": get_alias_proposal_summary(),
        }


def _find_proposal_by_id(proposal_id: str) -> tuple[str, str, list[dict[str, Any]], dict[str, Any], int] | tuple[None, None, list[Any], None, int]:
    target_id = str(proposal_id or "").strip()
    for media_type in MEDIA_TYPES:
        for field in ENTRY_FIELDS:
            entries = _load_bucket_entries(media_type, field, "proposal")
            for index, entry in enumerate(entries):
                if str(entry.get("id") or "") == target_id:
                    return media_type, field, entries, entry, index
    return None, None, [], None, -1


def review_proposal(*, proposal_id: str, action: str, canonical_name: str = "", aliases: list[str] | None = None) -> dict[str, Any]:
    del canonical_name
    normalized_action = str(action or "").strip().lower() or "modify"
    if normalized_action not in {"modify", "keep_original", "accept"}:
        raise ValueError("Unsupported review action")

    alias_values = _dedupe_texts(list(aliases or []))

    with _WRITE_LOCK:
        media_type, field, proposal_entries, target_entry, proposal_index = _find_proposal_by_id(proposal_id)
        if target_entry is None or proposal_index < 0 or media_type is None or field is None:
            raise ValueError("Proposal not found")

        remaining_proposals = [row for idx, row in enumerate(proposal_entries) if idx != proposal_index]
        approved_entries = _load_bucket_entries(media_type, field, "approved")
        keep_entries = _load_bucket_entries(media_type, field, "keep_original")
        normalized = _normalize_key(target_entry.get("normalized") or target_entry.get("canonical"))

        approved_entries = [row for row in approved_entries if _normalize_key(row.get("normalized") or row.get("canonical")) != normalized]
        keep_entries = [row for row in keep_entries if _normalize_key(row.get("normalized") or row.get("canonical")) != normalized]

        response_payload: dict[str, Any]
        if alias_values and normalized_action != "keep_original":
            approved_entry = _approved_entry_from_proposal(target_entry, alias_values)
            approved_entries.append(approved_entry)
            response_payload = {"approved_entry": approved_entry, "action": "approved"}
        else:
            keep_entry = _keep_entry_from_proposal(target_entry)
            keep_entries.append(keep_entry)
            response_payload = {"keep_original_entry": keep_entry, "action": "keep_original"}

        proposal_path = _bucket_path(media_type, field, "proposal")
        target_bucket = "approved" if response_payload["action"] == "approved" else "keep_original"
        target_path = _bucket_path(media_type, field, target_bucket)
        proposal_payload = {"entries": _sorted_entries(remaining_proposals)}
        target_payload = {"entries": _sorted_entries(approved_entries if target_bucket == "approved" else keep_entries)}

        proposal_tmp = proposal_path.with_suffix(proposal_path.suffix + ".tmp")
        target_tmp = target_path.with_suffix(target_path.suffix + ".tmp")
        proposal_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        proposal_tmp.write_text(json.dumps(proposal_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        target_tmp.write_text(json.dumps(target_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        proposal_tmp.replace(proposal_path)
        target_tmp.replace(target_path)
        summary = get_alias_proposal_summary()

    return {"ok": True, **response_payload, "summary": summary}


def _entry_variants(entry: dict[str, Any]) -> set[str]:
    values = [_normalize_text(entry.get("canonical"))]
    values.extend(list(entry.get("aliases") or []))
    return {_normalize_key(value) for value in values if _normalize_key(value)}


def _entry_terms(entry: dict[str, Any]) -> list[str]:
    canonical = _normalize_text(entry.get("canonical"))
    aliases = _dedupe_texts(list(entry.get("aliases") or []), exclude=canonical)
    terms: list[str] = []
    if canonical:
        terms.append(canonical)
    terms.extend(aliases)
    return terms


def get_approved_alias_registry() -> dict[str, Any]:
    signature = approved_alias_registry_signature()
    cached_signature = _APPROVED_REGISTRY_CACHE.get("signature")
    cached_registry = _APPROVED_REGISTRY_CACHE.get("registry")
    if signature == cached_signature and isinstance(cached_registry, dict):
        return cached_registry

    by_term: dict[str, list[dict[str, Any]]] = {}
    by_field_type: dict[str, dict[str, list[dict[str, Any]]]] = {"title": {}, "creator": {}, "publisher": {}}

    for media_type in MEDIA_TYPES:
        for field in ENTRY_FIELDS:
            field_type = _field_type_for_field(field)
            for entry in _load_bucket_entries(media_type, field, "approved"):
                canonical = _normalize_text(entry.get("canonical"))
                normalized = _normalize_key(entry.get("normalized") or canonical)
                aliases = _dedupe_texts(list(entry.get("aliases") or []), exclude=canonical)
                terms = _dedupe_texts([canonical, *aliases])
                record = {
                    "key": str(entry.get("key") or f"{media_type}|{field}"),
                    "field": field,
                    "field_type": field_type,
                    "media_type": media_type,
                    "canonical": canonical,
                    "normalized": normalized,
                    "aliases": aliases,
                    "terms": terms,
                }
                for term in terms:
                    term_key = _normalize_key(term)
                    if not term_key:
                        continue
                    by_term.setdefault(term_key, [])
                    if record not in by_term[term_key]:
                        by_term[term_key].append(record)
                    field_bucket = by_field_type.setdefault(field_type, {})
                    field_bucket.setdefault(term_key, [])
                    if record not in field_bucket[term_key]:
                        field_bucket[term_key].append(record)

    registry = {"by_term": by_term, "by_field_type": by_field_type}
    _APPROVED_REGISTRY_CACHE["signature"] = signature
    _APPROVED_REGISTRY_CACHE["registry"] = registry
    return registry


def approved_aliases_for_item(item: dict[str, Any]) -> list[str]:
    media_type = str(item.get("media_type") or "").strip().lower()
    if media_type not in MEDIA_TYPES:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for field in ENTRY_FIELDS:
        item_values = _split_value(item.get(field), field=field)
        if not item_values:
            continue
        existing = _load_bucket_entries(media_type, field, "approved")
        for item_value in item_values:
            normalized = _normalize_key(item_value)
            for entry in existing:
                if _normalize_key(entry.get("normalized") or entry.get("canonical")) != normalized:
                    continue
                for alias in list(entry.get("aliases") or []):
                    text = _normalize_text(alias)
                    key = _normalize_key(text)
                    if not text or key in seen or key == normalized:
                        continue
                    seen.add(key)
                    out.append(text)
    return out


def resolve_query_aliases(query: str, *, media_type_hint: str = "", max_entries: int = 8) -> dict[str, Any]:
    normalized_query = _normalize_text(query)
    query_key = _normalize_key(normalized_query)
    if not query_key:
        return {"query": normalized_query, "entries": [], "hits": [], "expanded_terms": []}

    media_hint = str(media_type_hint or "").strip().lower()
    matches: list[dict[str, Any]] = []
    for media_type in MEDIA_TYPES:
        if media_hint and media_type != media_hint:
            continue
        for field in ENTRY_FIELDS:
            field_type = _field_type_for_field(field)
            for entry in _load_bucket_entries(media_type, field, "approved"):
                canonical = _normalize_text(entry.get("canonical"))
                normalized = _normalize_key(entry.get("normalized") or canonical)
                terms = _entry_terms(entry)
                matched_text = ""
                variants: list[str] = []
                for term in terms:
                    variant = _normalize_key(term)
                    if not variant:
                        continue
                    variants.append(variant)
                    if _query_matches_alias_term(normalized_query, query_key, term):
                        matched_text = term
                        break
                if not matched_text:
                    continue
                aliases = _dedupe_texts(terms[1:], exclude=canonical)
                key = str(entry.get("key") or f"{media_type}|{field}")
                matches.append(
                    {
                        "id": key,
                        "key": key,
                        "field": field,
                        "field_type": field_type,
                        "media_type": media_type,
                        "canonical": canonical,
                        "canonical_name": canonical,
                        "raw_value": canonical,
                        "aliases": aliases,
                        "matched_text": matched_text,
                        "variants": sorted(variants),
                        "normalized": normalized,
                        "expanded_terms": [canonical, *aliases],
                    }
                )
    matches.sort(key=lambda row: (0 if row.get("field_type") == "creator" else 1, -len(str(row.get("matched_text") or ""))))
    limited_matches = matches[: max(1, int(max_entries or 1))]
    expanded_terms = [
        {
            "key": str(row.get("key") or ""),
            "media_type": str(row.get("media_type") or ""),
            "field": str(row.get("field") or ""),
            "field_type": str(row.get("field_type") or ""),
            "canonical": _normalize_text(row.get("canonical") or row.get("canonical_name")),
            "normalized": _normalize_key(row.get("normalized") or row.get("canonical") or row.get("canonical_name")),
            "matched_text": _normalize_text(row.get("matched_text")),
            "terms": _dedupe_texts(list(row.get("expanded_terms") or [])),
        }
        for row in limited_matches
    ]
    return {"query": normalized_query, "entries": limited_matches, "hits": limited_matches, "expanded_terms": expanded_terms}


def _query_matches_alias_term(query_text: str, query_key: str, term: Any) -> bool:
    normalized_term = _normalize_text(term)
    term_key = _normalize_key(normalized_term)
    if not term_key:
        return False
    if re.fullmatch(r"[a-z0-9]{1,4}", term_key):
        return re.search(rf"(?<![a-z0-9]){re.escape(normalized_term)}(?![a-z0-9])", query_text) is not None
    return term_key in query_key


def alias_hits_for_item(item: dict[str, Any], resolved_query: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(resolved_query, dict):
        return []
    entries = resolved_query.get("entries") if isinstance(resolved_query.get("entries"), list) else []
    media_type = str(item.get("media_type") or "").strip().lower()
    hits: list[dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        entry_media = str(entry.get("media_type") or "").strip().lower()
        if entry_media and media_type and entry_media != media_type:
            continue
        field_type = str(entry.get("field_type") or "")
        item_field = _field_for_field_type(field_type)
        for item_value in _split_value(item.get(item_field), field=item_field):
            if _normalize_key(item_value) != _normalize_key(entry.get("normalized") or entry.get("canonical_name")):
                continue
            hits.append(
                {
                    "field_type": field_type,
                    "item_field": item_field,
                    "matched_text": _normalize_text(entry.get("matched_text")),
                    "canonical_name": _normalize_text(entry.get("canonical_name")),
                    "raw_value": _normalize_text(entry.get("raw_value")),
                }
            )
            break
    return hits


def prune_stale_alias_entries(items: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    all_items = list(items or [])
    active_fingerprints = _active_candidate_fingerprints(all_items)
    removed_entries: list[dict[str, str]] = []
    removed_by_bucket = {bucket: 0 for bucket in ("proposal", "approved", "keep_original")}

    with _WRITE_LOCK:
        for media_type in MEDIA_TYPES:
            for field in ENTRY_FIELDS:
                for bucket in ("proposal", "approved", "keep_original"):
                    entries = _load_bucket_entries(media_type, field, bucket)
                    if not entries:
                        continue
                    retained: list[dict[str, Any]] = []
                    removed: list[dict[str, Any]] = []
                    for entry in entries:
                        normalized = _normalize_key(entry.get("normalized") or entry.get("canonical"))
                        fingerprint = f"{media_type}|{field}|{normalized}"
                        if normalized and fingerprint in active_fingerprints:
                            retained.append(entry)
                            continue
                        removed.append(entry)
                    if not removed:
                        continue
                    removed_by_bucket[bucket] += len(removed)
                    for row in removed:
                        removed_entries.append(
                            {
                                "bucket": bucket,
                                "media_type": media_type,
                                "field": field,
                                "canonical": _normalize_text(row.get("canonical")),
                            }
                        )
                    _write_bucket_entries(media_type, field, bucket, retained)

    return {
        "ok": True,
        "removed_total": len(removed_entries),
        "removed_by_bucket": removed_by_bucket,
        "removed_entries": removed_entries,
    }


def build_generation_queue(items: list[dict[str, Any]], *, max_candidates: int | None = None, persist_existing_links: bool = False) -> dict[str, Any]:
    del persist_existing_links
    candidates = []
    scanned = 0
    skipped = 0
    for candidate in _iter_candidate_inputs(items):
        scanned += 1
        if _bucket_has_normalized(candidate["media_type"], candidate["field"], candidate["normalized"]):
            skipped += 1
            continue
        candidates.append(candidate)
        if max_candidates is not None and len(candidates) >= max(1, int(max_candidates)):
            break
    return {
        "queued_candidates": candidates,
        "scanned_candidates": scanned,
        "linked_existing": 0,
        "skipped_existing": skipped,
    }


def generate_proposals_for_candidates(candidates: list[dict[str, Any]], *, registry: dict[str, Any] | None = None, persist: bool = True, batch_size: int = DEFAULT_BATCH_SIZE) -> dict[str, Any]:
    del registry
    normalized_candidates: list[dict[str, Any]] = []
    skipped_existing = 0
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        media_type = str(candidate.get("media_type") or "").strip().lower()
        field = str(candidate.get("field") or "").strip().lower() or _field_for_field_type(str(candidate.get("field_type") or "title"))
        canonical = _normalize_text(candidate.get("canonical") or candidate.get("raw_value"))
        normalized = _normalize_key(candidate.get("normalized") or canonical)
        if media_type not in MEDIA_TYPES or field not in ENTRY_FIELDS or not canonical or not normalized:
            continue
        if _bucket_has_normalized(media_type, field, normalized):
            skipped_existing += 1
            continue
        normalized_candidates.append(
            {
                "media_type": media_type,
                "field": field,
                "canonical": canonical,
                "normalized": normalized,
                "samples": [str(item).strip() for item in list(candidate.get("samples") or []) if str(item).strip()][:MAX_CONTEXT_ITEMS],
                "source_item_ids": [str(item).strip() for item in list(candidate.get("source_item_ids") or []) if str(item).strip()],
            }
        )

    created_entries: list[dict[str, Any]] = []
    if not normalized_candidates:
        return {"created": 0, "skipped_existing": skipped_existing, "pending_total": get_alias_proposal_summary()["pending_count"], "created_entries": []}

    if not persist:
        pending_entries: list[dict[str, Any]] = []
    else:
        pending_entries = []

    try:
        for offset in range(0, len(normalized_candidates), max(1, batch_size)):
            batch = normalized_candidates[offset : offset + max(1, batch_size)]
            results = _llm_generate_batch(batch)
            result_map = {
                f"{row['candidate']['media_type']}|{row['candidate']['field']}|{row['candidate']['normalized']}": row
                for row in results
            }
            for candidate in batch:
                fingerprint = f"{candidate['media_type']}|{candidate['field']}|{candidate['normalized']}"
                llm_row = result_map.get(fingerprint)
                entry = _proposal_entry_from_candidate(
                    candidate,
                    aliases=list(llm_row.get("aliases") or []) if llm_row else [],
                    confidence=float(llm_row.get("confidence") or 0.0) if llm_row else 0.0,
                )
                pending_entries.append(entry)
                created_entries.append(entry)
    except Exception:
        return {"created": 0, "skipped_existing": skipped_existing, "pending_total": get_alias_proposal_summary()["pending_count"], "created_entries": []}

    if persist and created_entries:
        with _WRITE_LOCK:
            grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
            for entry in created_entries:
                field = _entry_field_from_key(entry.get("key"))
                grouped.setdefault((str(entry.get("media_type") or ""), field), []).append(entry)
            for (media_type, field), new_entries in grouped.items():
                existing = _load_bucket_entries(media_type, field, "proposal")
                existing_keys = {_normalize_key(row.get("normalized") or row.get("canonical")) for row in existing}
                merged = list(existing)
                for row in new_entries:
                    normalized = _normalize_key(row.get("normalized") or row.get("canonical"))
                    if normalized in existing_keys:
                        continue
                    existing_keys.add(normalized)
                    merged.append(row)
                _write_bucket_entries(media_type, field, "proposal", merged)

    return {
        "created": len(created_entries),
        "skipped_existing": skipped_existing,
        "pending_total": get_alias_proposal_summary()["pending_count"],
        "created_entries": created_entries,
    }


def generate_proposals_for_items(items: list[dict[str, Any]], *, persist: bool = True, batch_size: int = DEFAULT_BATCH_SIZE, max_candidates: int | None = None) -> dict[str, Any]:
    queue = build_generation_queue(items, max_candidates=max_candidates, persist_existing_links=False)
    generated_candidates = list(queue.get("queued_candidates") or [])
    result = generate_proposals_for_candidates(generated_candidates, persist=persist, batch_size=batch_size)
    return {
        "created": int(result.get("created", 0) or 0),
        "linked_existing": 0,
        "skipped_existing": int(queue.get("skipped_existing", 0) or 0) + int(result.get("skipped_existing", 0) or 0),
        "scanned_candidates": int(queue.get("scanned_candidates", 0) or 0),
        "pending_total": int(result.get("pending_total", 0) or 0),
        "created_entries": list(result.get("created_entries") or []),
    }


def generate_proposals_for_item_ids(items: list[dict[str, Any]], *, item_ids: list[str]) -> dict[str, Any]:
    target_set = {str(item_id or "").strip() for item_id in item_ids if str(item_id or "").strip()}
    subset = [item for item in items if str(item.get("id") or "") in target_set]
    if not subset:
        return {"created": 0, "linked_existing": 0, "skipped_existing": 0, "scanned_candidates": 0, "pending_total": get_alias_proposal_summary()["pending_count"], "created_entries": []}
    return generate_proposals_for_items(subset, persist=True)
