from __future__ import annotations

import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from urllib import parse as urlparse
from uuid import uuid4

from fastapi import HTTPException, Request

from nav_dashboard.web.config import AI_SUMMARY_URL_OVERRIDE, LIBRARY_TRACKER_URL_OVERRIDE
from nav_dashboard.web.services.runtime_paths import CUSTOM_CARDS_FILE


PROJECT_ROOT = Path(__file__).resolve().parents[3]
CUSTOM_CARDS_MAX = 8
CUSTOM_CARD_UPLOAD_DIR = Path(__file__).resolve().parents[1] / "static" / "custom_cards"
ALLOWED_IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp", ".gif"}
CONTENT_TYPE_TO_EXT = {
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
}


def default_custom_cards() -> list[dict[str, str]]:
    cards = [
        {"title": "RAG System", "url": str(AI_SUMMARY_URL_OVERRIDE or "").strip(), "image": ""},
        {"title": "Library Tracker", "url": str(LIBRARY_TRACKER_URL_OVERRIDE or "").strip(), "image": ""},
    ]
    while len(cards) < CUSTOM_CARDS_MAX:
        cards.append({"title": "", "url": "", "image": ""})
    return cards


def normalize_card(item: object) -> dict[str, str]:
    if not isinstance(item, dict):
        return {"title": "", "url": "", "image": ""}
    title = str(item.get("title", "")).strip()
    url = str(item.get("url", "")).strip()
    image = str(item.get("image", "")).strip().replace("\\", "/")
    if image and not image.lower().startswith(("http://", "https://", "/")):
        image = "/static/" + image.lstrip("./")
    return {"title": title, "url": url, "image": image}


def save_custom_cards(cards: list[dict[str, str]]) -> list[dict[str, str]]:
    normalized = [normalize_card(item) for item in cards[:CUSTOM_CARDS_MAX]]
    while len(normalized) < CUSTOM_CARDS_MAX:
        normalized.append({"title": "", "url": "", "image": ""})
    CUSTOM_CARDS_FILE.parent.mkdir(parents=True, exist_ok=True)
    CUSTOM_CARDS_FILE.write_text(json.dumps(normalized, ensure_ascii=False, indent=2), encoding="utf-8")
    return normalized


def load_custom_cards() -> list[dict[str, str]]:
    default_cards = default_custom_cards()
    try:
        CUSTOM_CARDS_FILE.parent.mkdir(parents=True, exist_ok=True)
        if not CUSTOM_CARDS_FILE.exists():
            CUSTOM_CARDS_FILE.write_text(json.dumps(default_cards, ensure_ascii=False, indent=2), encoding="utf-8")
            return default_cards
        raw = json.loads(CUSTOM_CARDS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return default_cards
    if not isinstance(raw, list):
        return default_cards
    return save_custom_cards([normalize_card(item) for item in raw])


def _is_loopback_host(host: str) -> bool:
    return str(host or "").strip().lower() in {"127.0.0.1", "localhost", "::1"}


def _default_card_port(title: str) -> int | None:
    normalized = str(title or "").strip().lower()
    if normalized == "rag system":
        return 8000
    if normalized == "library tracker":
        return 8091
    return None


def _first_forwarded_value(value: str) -> str:
    return str(value or "").split(",", 1)[0].strip()


def _request_public_origin(request: Request) -> tuple[str, str]:
    forwarded = _first_forwarded_value(request.headers.get("forwarded", ""))
    forwarded_host = ""
    forwarded_proto = ""
    if forwarded:
        for segment in forwarded.split(";"):
            key, separator, raw_value = segment.partition("=")
            if not separator:
                continue
            normalized_key = key.strip().lower()
            normalized_value = raw_value.strip().strip('"')
            if normalized_key == "host" and not forwarded_host:
                forwarded_host = normalized_value
            elif normalized_key == "proto" and not forwarded_proto:
                forwarded_proto = normalized_value

    host = (
        _first_forwarded_value(request.headers.get("x-forwarded-host", ""))
        or forwarded_host
        or str(request.headers.get("host", "")).strip()
        or str(request.url.netloc or "").strip()
    ).rstrip("/")
    scheme = (
        _first_forwarded_value(request.headers.get("x-forwarded-proto", ""))
        or forwarded_proto
        or str(request.url.scheme or "http").strip()
        or "http"
    ).rstrip(":/")
    forwarded_port = _first_forwarded_value(request.headers.get("x-forwarded-port", ""))
    if host and forwarded_port and ":" not in host and not host.startswith("["):
        host = f"{host}:{forwarded_port}"
    if not host:
        hostname = request.url.hostname or "localhost"
        host = f"{hostname}:{request.url.port}" if request.url.port else hostname
    return scheme, host


def rewrite_loopback_url_for_request(raw_url: str, request: Request, fallback_port: int) -> str:
    url_text = str(raw_url or "").strip()
    public_scheme, public_host = _request_public_origin(request)
    public_hostname = urlparse.urlparse(f"//{public_host}").hostname or request.url.hostname or "localhost"
    if not url_text:
        return f"{public_scheme}://{public_hostname}:{int(fallback_port)}/"

    parsed = urlparse.urlparse(url_text)
    if not parsed.scheme or not parsed.hostname:
        return url_text
    if not _is_loopback_host(parsed.hostname):
        return url_text

    target_port = parsed.port or int(fallback_port)
    rewritten = parsed._replace(
        scheme=public_scheme or parsed.scheme or "http",
        netloc=f"{public_hostname}:{target_port}",
        path=parsed.path or "/",
    )
    return urlparse.urlunparse(rewritten)


def browser_custom_cards(request: Request) -> list[dict[str, str]]:
    rewritten: list[dict[str, str]] = []
    for item in load_custom_cards():
        row = normalize_card(item)
        url_value = row.get("url", "")
        fallback_port = _default_card_port(row.get("title", ""))
        if url_value:
            parsed = urlparse.urlparse(url_value)
            row["url"] = rewrite_loopback_url_for_request(url_value, request, parsed.port or fallback_port or 80)
        elif fallback_port is not None:
            row["url"] = rewrite_loopback_url_for_request("", request, fallback_port)
        rewritten.append(row)
    return rewritten


def save_custom_card(index: int, payload_data: dict[str, str]) -> tuple[dict[str, str], list[dict[str, str]]]:
    if index < 0 or index >= CUSTOM_CARDS_MAX:
        raise HTTPException(status_code=400, detail=f"index out of range: {index}")
    cards = load_custom_cards()
    cards[index] = normalize_card(payload_data)
    saved = save_custom_cards(cards)
    return saved[index], saved


def upload_custom_card_image(*, filename: str, content_type: str, content: bytes) -> str:
    normalized_filename = str(filename or "").strip()
    normalized_content_type = str(content_type or "").split(";", 1)[0].strip().lower()
    if normalized_content_type and not normalized_content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="Content-Type must be image/*")

    ext = Path(normalized_filename).suffix.lower()
    if not ext:
        ext = CONTENT_TYPE_TO_EXT.get(normalized_content_type, "")
    if ext not in ALLOWED_IMAGE_EXTS:
        raise HTTPException(status_code=400, detail="Only png/jpg/jpeg/webp/gif are supported")

    if not normalized_filename:
        normalized_filename = f"card{ext}"
    stem = re.sub(r"[^a-zA-Z0-9_-]", "_", Path(normalized_filename).stem).strip("_") or "card"
    out_name = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{stem}_{uuid4().hex[:8]}{ext}"

    try:
        CUSTOM_CARD_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to create upload directory: {exc}") from exc
    if not content:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    out_path = CUSTOM_CARD_UPLOAD_DIR / out_name
    try:
        out_path.write_bytes(content)
    except PermissionError as exc:
        raise HTTPException(status_code=500, detail="Permission denied while writing upload") from exc
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"Failed to write upload: {exc}") from exc
    return f"/static/custom_cards/{out_name}"


def trigger_custom_card_compression() -> None:
    try:
        script_path = PROJECT_ROOT / "scripts" / "data_maintenance" / "compress_custom_cards.py"
        if not script_path.exists():
            return
        subprocess.run([sys.executable, str(script_path)], capture_output=True, timeout=300, check=False)
    except Exception:
        return
