from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

PROJECT_ROOT = Path(__file__).resolve().parent.parent
WORKSPACE_ROOT = PROJECT_ROOT.parent
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from core_service.runtime_data import app_runtime_root  # noqa: E402


STRUCTURED_DIR = app_runtime_root("library_tracker") / "structured"
ENTITIES_DIR = STRUCTURED_DIR / "entities"
ALIASES_DIR = STRUCTURED_DIR / "aliases"
PROPOSAL_DIR = ALIASES_DIR / "proposal"
APPROVED_DIR = ALIASES_DIR / "approved"
KEEP_DIR = ALIASES_DIR / "keep_original"

MEDIA_TYPES = ("book", "video", "music", "game")
ENTRY_FIELDS = ("title", "author", "publisher")
MEDIA_FILES = {
    "book": "reading.json",
    "video": "video.json",
    "music": "music.json",
    "game": "game.json",
}
SPLIT_VALUE_FIELDS = {"author", "publisher"}
MAX_CONTEXT_ITEMS = 3

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


def _stable_sort_key(entry: dict[str, Any]) -> tuple[str, str]:
    normalized = _normalize_key(entry.get("normalized") or entry.get("canonical"))
    canonical = _normalize_text(entry.get("canonical"))
    return (normalized, canonical.casefold())


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


def _bucket_filename(media_type: str, field: str, bucket: str) -> str:
    if bucket == "proposal":
        return f"{media_type}_{field}_proposals.json"
    if bucket == "approved":
        return f"{media_type}_{field}_alias.json"
    if bucket == "keep_original":
        return f"{media_type}_{field}_keep.json"
    raise ValueError(f"Unsupported bucket: {bucket}")


def _bucket_dir(bucket: str) -> Path:
    if bucket == "proposal":
        return PROPOSAL_DIR
    if bucket == "approved":
        return APPROVED_DIR
    if bucket == "keep_original":
        return KEEP_DIR
    raise ValueError(f"Unsupported bucket: {bucket}")


def _bucket_path(media_type: str, field: str, bucket: str) -> Path:
    return _bucket_dir(bucket) / _bucket_filename(media_type, field, bucket)


def _empty_payload() -> dict[str, list[dict[str, Any]]]:
    return {"entries": []}


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


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def _load_bucket_entries(media_type: str, field: str, bucket: str) -> list[dict[str, Any]]:
    payload = _load_payload(_bucket_path(media_type, field, bucket))
    entries = payload.get("entries") if isinstance(payload.get("entries"), list) else []
    rows: list[dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        row = dict(entry)
        row["key"] = str(row.get("key") or f"{media_type}|{field}")
        row["media_type"] = str(row.get("media_type") or media_type)
        row["canonical"] = _normalize_text(row.get("canonical"))
        row["normalized"] = _normalize_key(row.get("normalized") or row.get("canonical"))
        row["field"] = field
        if bucket == "approved":
            row["aliases"] = _dedupe_texts(list(row.get("aliases") or []), exclude=row.get("canonical"))
            row["status"] = "approved"
        elif bucket == "proposal":
            row["aliases"] = _dedupe_texts(list(row.get("aliases") or []), exclude=row.get("canonical"))
            row["samples"] = [str(item).strip() for item in list(row.get("samples") or []) if str(item).strip()][:MAX_CONTEXT_ITEMS]
            row["source_item_ids"] = [str(item).strip() for item in list(row.get("source_item_ids") or []) if str(item).strip()]
            try:
                row["confidence"] = max(0.0, min(1.0, float(row.get("confidence") or 0.0)))
            except Exception:
                row["confidence"] = 0.0
            row["generated_at"] = str(row.get("generated_at") or "")
            row["id"] = str(row.get("id") or "")
            row["status"] = "proposal"
        else:
            row.pop("aliases", None)
            row["status"] = "keep_original"
        rows.append(row)
    return sorted(rows, key=_stable_sort_key)


def _write_bucket_entries(media_type: str, field: str, bucket: str, entries: list[dict[str, Any]]) -> None:
    _atomic_write_json(_bucket_path(media_type, field, bucket), {"entries": sorted(entries, key=_stable_sort_key)})


def _iter_all_items() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for media_type, filename in MEDIA_FILES.items():
        path = ENTITIES_DIR / filename
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        records = payload.get("records") if isinstance(payload, dict) else []
        if not isinstance(records, list):
            continue
        for index, record in enumerate(records):
            if not isinstance(record, dict):
                continue
            item = dict(record)
            item["media_type"] = str(item.get("media_type") or media_type)
            item["id"] = f"{media_type}:{index}"
            items.append(item)
    return items


def _find_matching_context(*, media_type: str, field: str, canonical: str, aliases: list[str]) -> tuple[list[str], list[str]]:
    match_terms = {_normalize_key(canonical)}
    match_terms.update(_normalize_key(alias) for alias in aliases if _normalize_key(alias))
    samples: list[str] = []
    source_item_ids: list[str] = []
    for item in _iter_all_items():
        if str(item.get("media_type") or "") != media_type:
            continue
        values = _split_value(item.get(field), field=field)
        if not any(_normalize_key(value) in match_terms for value in values):
            continue
        item_id = str(item.get("id") or "").strip()
        if item_id and item_id not in source_item_ids:
            source_item_ids.append(item_id)
        sample = _sample_label(item, field=field, canonical=canonical)
        if sample and sample not in samples and len(samples) < MAX_CONTEXT_ITEMS:
            samples.append(sample)
    return samples, source_item_ids


def _build_proposal_entry(entry: dict[str, Any], *, confidence_override: float | None) -> dict[str, Any]:
    canonical = _normalize_text(entry.get("canonical"))
    aliases = _dedupe_texts(list(entry.get("aliases") or []), exclude=canonical)
    media_type = str(entry.get("media_type") or "")
    field = str(entry.get("field") or "")
    samples, source_item_ids = _find_matching_context(media_type=media_type, field=field, canonical=canonical, aliases=aliases)
    if confidence_override is None:
        confidence = 1.0 if aliases else 0.0
    else:
        confidence = max(0.0, min(1.0, float(confidence_override)))
    return {
        "id": str(uuid4()),
        "key": str(entry.get("key") or f"{media_type}|{field}"),
        "media_type": media_type,
        "canonical": canonical,
        "normalized": _normalize_key(entry.get("normalized") or canonical),
        "aliases": aliases,
        "confidence": confidence,
        "samples": samples,
        "source_item_ids": source_item_ids,
        "generated_at": _now_iso(),
        "status": "proposal",
    }


def _find_matches(*, canonical: str, media_type: str, field: str, bucket: str) -> list[dict[str, Any]]:
    target = _normalize_key(canonical)
    matches: list[dict[str, Any]] = []
    media_types = (media_type,) if media_type else MEDIA_TYPES
    fields = (field,) if field else ENTRY_FIELDS
    for current_media_type in media_types:
        for current_field in fields:
            for entry in _load_bucket_entries(current_media_type, current_field, bucket):
                normalized = _normalize_key(entry.get("normalized") or entry.get("canonical"))
                if normalized != target:
                    continue
                row = dict(entry)
                row["bucket"] = bucket
                matches.append(row)
    return matches


def reopen_entry(*, canonical: str, media_type: str = "", field: str = "", confidence: float | None = None, dry_run: bool = False) -> dict[str, Any]:
    normalized_media_type = str(media_type or "").strip().lower()
    normalized_field = str(field or "").strip().lower()
    if normalized_media_type and normalized_media_type not in MEDIA_TYPES:
        raise ValueError(f"Unsupported media type: {media_type}")
    if normalized_field and normalized_field not in ENTRY_FIELDS:
        raise ValueError(f"Unsupported field: {field}")

    matches = _find_matches(canonical=canonical, media_type=normalized_media_type, field=normalized_field, bucket="approved")
    matches.extend(_find_matches(canonical=canonical, media_type=normalized_media_type, field=normalized_field, bucket="keep_original"))
    if not matches:
        raise ValueError("No matching canonical found in alias/keep_original")
    if len(matches) > 1:
        details = ", ".join(f"{row['bucket']}:{row['media_type']}:{row['field']}:{row['canonical']}" for row in matches)
        raise ValueError(f"Canonical is ambiguous, please narrow with --media-type/--field: {details}")

    target = matches[0]
    target_media_type = str(target.get("media_type") or "")
    target_field = str(target.get("field") or "")
    normalized = _normalize_key(target.get("normalized") or target.get("canonical"))
    proposal_entries = [
        row
        for row in _load_bucket_entries(target_media_type, target_field, "proposal")
        if _normalize_key(row.get("normalized") or row.get("canonical")) != normalized
    ]
    approved_entries = [
        row
        for row in _load_bucket_entries(target_media_type, target_field, "approved")
        if _normalize_key(row.get("normalized") or row.get("canonical")) != normalized
    ]
    keep_entries = [
        row
        for row in _load_bucket_entries(target_media_type, target_field, "keep_original")
        if _normalize_key(row.get("normalized") or row.get("canonical")) != normalized
    ]
    proposal_entry = _build_proposal_entry(target, confidence_override=confidence)
    proposal_entries.append(proposal_entry)

    result = {
        "canonical": proposal_entry["canonical"],
        "media_type": target_media_type,
        "field": target_field,
        "from_bucket": str(target.get("bucket") or ""),
        "proposal_path": str(_bucket_path(target_media_type, target_field, "proposal")),
        "source_path": str(_bucket_path(target_media_type, target_field, str(target.get("bucket") or ""))),
        "proposal_entry": proposal_entry,
        "dry_run": dry_run,
    }

    if dry_run:
        return result

    _write_bucket_entries(target_media_type, target_field, "proposal", proposal_entries)
    _write_bucket_entries(target_media_type, target_field, "approved", approved_entries)
    _write_bucket_entries(target_media_type, target_field, "keep_original", keep_entries)
    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Move an alias/keep_original canonical entry back into proposal for manual re-review.")
    parser.add_argument("canonical", help="Canonical name to reopen into proposal")
    parser.add_argument("--media-type", choices=MEDIA_TYPES, default="", help="Optional media type to disambiguate")
    parser.add_argument("--field", choices=ENTRY_FIELDS, default="", help="Optional field to disambiguate")
    parser.add_argument("--confidence", type=float, default=None, help="Optional confidence override for the reopened proposal")
    parser.add_argument("--dry-run", action="store_true", help="Preview the moved proposal entry without writing files")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    try:
        result = reopen_entry(
            canonical=args.canonical,
            media_type=args.media_type,
            field=args.field,
            confidence=args.confidence,
            dry_run=bool(args.dry_run),
        )
    except Exception as exc:
        print(f"ERROR: {exc}")
        return 1

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())