from __future__ import annotations

from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any
from uuid import uuid4

from PIL import Image

from . import library_service_core as core


def save_cover_bytes(
    data: bytes,
    content_type: str,
    original_filename: str | None = None,
    title: str | None = None,
    overwrite_path: str | None = None,
) -> str:
    if not data:
        raise ValueError("Empty cover content")

    ext = core._CONTENT_TYPE_EXT.get((content_type or "").lower(), "")
    if not ext:
        raw_name = (original_filename or "").strip()
        if raw_name:
            candidate = Path(raw_name).suffix.lower()
            if candidate in {".jpg", ".jpeg", ".png", ".webp", ".gif"}:
                ext = ".jpg" if candidate == ".jpeg" else candidate
    if not ext:
        raise ValueError("Unsupported image type")

    max_size = 3 * 1024 * 1024
    if len(data) > max_size:
        try:
            image = Image.open(BytesIO(data))
            if image.mode in ("RGBA", "LA", "P"):
                rgb_image = Image.new("RGB", image.size, (255, 255, 255))
                if image.mode == "P":
                    image = image.convert("RGBA")
                rgb_image.paste(image, mask=image.split()[-1] if image.mode in ("RGBA", "LA") else None)
                image = rgb_image

            quality = 85
            while quality > 20:
                buffer = BytesIO()
                image.save(buffer, format="JPEG", quality=quality, optimize=True)
                compressed = buffer.getvalue()
                if len(compressed) <= max_size:
                    data = compressed
                    ext = ".jpg"
                    break
                quality -= 10
        except Exception as error:
            print(f"[COMPRESS_WARNING] Failed to compress image: {error}")

    core.COVERS_DIR.mkdir(parents=True, exist_ok=True)

    normalized_overwrite = str(overwrite_path or "").replace("\\", "/").strip()
    if normalized_overwrite:
        if not normalized_overwrite.startswith("covers/"):
            raise ValueError("Invalid overwrite path")
        rel_name = normalized_overwrite[len("covers/") :].strip()
        if not rel_name or "/" in rel_name:
            raise ValueError("Invalid overwrite path")
        out_path = core.COVERS_DIR / rel_name
        out_path.write_bytes(data)
        return f"covers/{rel_name}"

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    title_slug = core._sanitize_title_for_filename(title)
    if title_slug:
        filename = f"{title_slug}_{stamp}{ext}"
    else:
        filename = f"{stamp}_{uuid4().hex[:10]}{ext}"

    out_path = core.COVERS_DIR / filename
    out_path.write_bytes(data)
    return f"covers/{filename}"


def add_item(item: dict[str, Any]) -> dict[str, Any]:
    cleaned = core._sanitize_item_for_storage(item)
    media_type = str(cleaned["media_type"])
    payload = core._load_payload(media_type)
    records = payload.get("records", [])
    if not isinstance(records, list):
        records = []
    records.append(cleaned)
    payload["records"] = records
    core._save_payload(media_type, payload)
    index = len(records) - 1
    item_id = f"{media_type}:{index}"
    core._mark_item_pending(item_id)
    return core._normalize_item(cleaned, media_type, index)


def update_item(item_id: str, patch: dict[str, Any]) -> dict[str, Any]:
    media_type, index = core._parse_item_id(item_id)
    source_payload = core._load_payload(media_type)
    records = source_payload.get("records", [])
    if index < 0 or index >= len(records):
        raise core.ItemNotFoundError(item_id)
    base = records[index]
    if not isinstance(base, dict):
        raise core.ItemNotFoundError(item_id)

    merged = dict(base)
    for key, value in patch.items():
        if key == "id":
            continue
        merged[key] = value

    cleaned = core._sanitize_item_for_storage(merged, fallback_media_type=media_type)
    target_media_type = str(cleaned.get("media_type") or media_type)
    if target_media_type == media_type:
        records[index] = cleaned
        source_payload["records"] = records
        core._save_payload(media_type, source_payload)
        core._mark_item_pending(item_id)
        return core._normalize_item(cleaned, media_type, index)

    records[index] = None  # type: ignore[assignment]
    source_payload["records"] = records
    core._save_payload(media_type, source_payload)

    try:
        target_payload = core._load_payload(target_media_type)
        target_records = target_payload.get("records", [])
        if not isinstance(target_records, list):
            target_records = []
        target_records.append(cleaned)
        target_payload["records"] = target_records
        core._save_payload(target_media_type, target_payload)
    except Exception:
        records[index] = base
        source_payload["records"] = records
        core._save_payload(media_type, source_payload)
        raise

    new_index = len(target_records) - 1
    new_item_id = f"{target_media_type}:{new_index}"

    try:
        with core._embedding_db_conn() as conn:
            conn.execute("DELETE FROM item_embeddings WHERE item_id = ?", (item_id,))
            conn.commit()
    except Exception:
        pass

    try:
        core.library_graph.remove_item_from_graph(core.VECTOR_DB_DIR, item_id)
    except Exception:
        pass

    core._mark_item_pending(new_item_id)
    return core._normalize_item(cleaned, target_media_type, new_index)


def delete_item(item_id: str) -> dict[str, Any]:
    media_type, index = core._parse_item_id(item_id)
    payload = core._load_payload(media_type)
    records = payload.get("records", [])
    if not isinstance(records, list) or index < 0 or index >= len(records):
        raise core.ItemNotFoundError(item_id)

    records[index] = None  # type: ignore[assignment]
    payload["records"] = records
    core._save_payload(media_type, payload)
    core.invalidate_search_cache()
    core._invalidate_metadata_cache()

    try:
        with core._embedding_db_conn() as conn:
            conn.execute("DELETE FROM item_embeddings WHERE item_id = ?", (item_id,))
    except Exception:
        pass

    graph_stats: dict[str, Any] = {}
    try:
        graph_stats = core.library_graph.remove_item_from_graph(core.VECTOR_DB_DIR, item_id)
    except Exception:
        pass

    return {"ok": True, "item_id": item_id, "graph": graph_stats}
