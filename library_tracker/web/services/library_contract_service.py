from __future__ import annotations

from typing import Any

from datetime import datetime
from pathlib import Path

from . import library_service_core as core


def export_library_contract() -> dict[str, Any]:
    media_payloads: dict[str, Any] = {}
    record_counts: dict[str, int] = {}
    for media_type in ("book", "video", "music", "game"):
        payload = core._load_payload(media_type)
        media_payloads[media_type] = payload
        record_counts[media_type] = len(payload.get("records") or []) if isinstance(payload, dict) else 0
    alias_buckets = {
        "approved": core._iter_bucket_json_payloads("approved"),
        "proposal": core._iter_bucket_json_payloads("proposal"),
        "keep_original": core._iter_bucket_json_payloads("keep_original"),
    }
    concept_ontology = core._load_json_document(core.get_concept_ontology_path(), {})
    return {
        "contract": "library_tracker_backup",
        "version": 1,
        "exported_at": datetime.now().isoformat(timespec="seconds"),
        "media_payloads": media_payloads,
        "alias_buckets": alias_buckets,
        "concept_ontology": concept_ontology,
        "summary": {
            "record_counts": record_counts,
            "alias_file_counts": {bucket: len(files) for bucket, files in alias_buckets.items()},
            "concept_present": bool(concept_ontology),
        },
    }


def import_library_contract(payload: dict[str, Any], replace_existing: bool = False) -> dict[str, Any]:
    media_payloads = payload.get("media_payloads") if isinstance(payload.get("media_payloads"), dict) else {}
    alias_buckets = payload.get("alias_buckets") if isinstance(payload.get("alias_buckets"), dict) else {}
    concept_ontology = payload.get("concept_ontology") if isinstance(payload.get("concept_ontology"), dict) else {}

    written_media: list[str] = []
    for media_type in ("book", "video", "music", "game"):
        if media_type not in media_payloads:
            continue
        target = core.get_preferred_entity_file_path(media_type)
        core._write_json_document(target, media_payloads.get(media_type) or {"records": []})
        written_media.append(media_type)

    for bucket in ("approved", "proposal", "keep_original"):
        target_dir = core.get_alias_bucket_dir(bucket)
        bucket_payload = alias_buckets.get(bucket) if isinstance(alias_buckets.get(bucket), dict) else {}
        core._replace_directory_json_payloads(target_dir, bucket_payload, replace_existing=replace_existing)

    concept_target = core.get_preferred_concept_ontology_path()
    if replace_existing and core.STRUCTURED_CONCEPTS_DIR.exists() and not concept_ontology:
        concept_target.unlink(missing_ok=True)
    elif concept_ontology:
        core._write_json_document(concept_target, concept_ontology)

    core.invalidate_search_cache()
    return {
        "ok": True,
        "contract": "library_tracker_backup",
        "version": 1,
        "replace_existing": bool(replace_existing),
        "media_profiles": written_media,
        "record_counts": {
            media_type: len(((media_payloads.get(media_type) or {}).get("records") or []))
            for media_type in written_media
        },
        "alias_file_counts": {
            bucket: len(alias_buckets.get(bucket) or []) if isinstance(alias_buckets.get(bucket), dict) else 0
            for bucket in ("approved", "proposal", "keep_original")
        },
        "concept_restored": bool(concept_ontology),
    }