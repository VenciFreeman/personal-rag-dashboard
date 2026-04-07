from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

REPORT_BACKENDS = {"local", "deepseek"}

JOB_TYPE_PROPERTY_ASSET_MONTHLY = "property_asset_monthly"
JOB_TYPE_PROPERTY_MARKET_WEEKLY = "property_market_weekly"
JOB_TYPE_LIBRARY_QUARTERLY = "library_quarterly"
JOB_TYPE_LIBRARY_YEARLY = "library_yearly"
JOB_TYPE_JOURNEY_TRIP_ARCHIVE_REVIEW = "journey_trip_archive_review"

ALL_JOB_TYPES = {
    JOB_TYPE_PROPERTY_ASSET_MONTHLY,
    JOB_TYPE_PROPERTY_MARKET_WEEKLY,
    JOB_TYPE_LIBRARY_QUARTERLY,
    JOB_TYPE_LIBRARY_YEARLY,
    JOB_TYPE_JOURNEY_TRIP_ARCHIVE_REVIEW,
}


def sanitize_period_key(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return "report"
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"[^0-9A-Za-z_.-]+", "-", text)
    text = re.sub(r"-{2,}", "-", text).strip("-._")
    return text or "report"


def build_report_filename(period_key: str, job_type: str, backend: str) -> str:
    if job_type not in ALL_JOB_TYPES:
        raise ValueError(f"invalid job type: {job_type}")
    if backend not in REPORT_BACKENDS:
        raise ValueError(f"invalid backend: {backend}")
    return f"{sanitize_period_key(period_key)}_{job_type}_{backend}.md"


def build_frontmatter(meta: dict[str, Any]) -> str:
    lines = ["---"]
    for key, value in meta.items():
        safe_value = str(value or "").replace("\n", " ").strip()
        lines.append(f"{key}: {safe_value}")
    lines.append("---")
    return "\n".join(lines)


def parse_frontmatter(raw: str) -> tuple[dict[str, str], str]:
    text = str(raw or "")
    meta: dict[str, str] = {}
    body = text
    if text.startswith("---\n"):
        parts = text.split("\n---\n", 1)
        if len(parts) == 2:
            header, body = parts
            for line in header.splitlines()[1:]:
                if ":" not in line:
                    continue
                key, value = line.split(":", 1)
                meta[key.strip()] = value.strip()
    return meta, body.strip()


def write_report(path: Path, meta: dict[str, Any], markdown: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = build_frontmatter(meta) + "\n" + str(markdown or "").strip() + "\n"
    path.write_text(content, encoding="utf-8")


def load_json_file(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default
    return payload if isinstance(payload, type(default)) else default


def save_json_file(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def truncate_text_by_chars(text: str, max_chars: int) -> str:
    clean = str(text or "").strip()
    if max_chars <= 0 or len(clean) <= max_chars:
        return clean
    return clean[: max(0, max_chars - 1)].rstrip() + "…"


def ensure_storage_paths(paths: list[Path]) -> None:
    from .report_engine import ensure_storage_paths as _ensure_storage_paths

    _ensure_storage_paths(paths)


def load_state_payload(path: Path, defaults: dict[str, Any], *, load_json=load_json_file) -> dict[str, Any]:
    from .report_engine import load_state_payload as _load_state_payload

    return _load_state_payload(path, defaults, load_json=load_json)


def save_state_payload(path: Path, payload: dict[str, Any]) -> None:
    from .report_engine import save_state_payload as _save_state_payload

    _save_state_payload(path, payload)


def parse_report_record(
    path: Path,
    *,
    valid_kinds: set[str],
    job_type_by_kind: dict[str, str],
    source_resolver=None,
    record_normalizer=None,
) -> dict[str, Any] | None:
    from .report_engine import parse_report_record as _parse_report_record

    return _parse_report_record(
        path,
        valid_kinds=valid_kinds,
        job_type_by_kind=job_type_by_kind,
        source_resolver=source_resolver,
        record_normalizer=record_normalizer,
    )


def delete_report_files(rows: list[dict[str, Any]], *, period_key: str, source: str | None = None) -> list[str]:
    from .report_engine import delete_report_files as _delete_report_files

    return _delete_report_files(rows, period_key=period_key, source=source)


def open_report_folder(path: Path) -> dict[str, Any]:
    from .report_engine import open_report_folder as _open_report_folder

    return _open_report_folder(path)


def summarize_report_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    from .report_engine import summarize_report_rows as _summarize_report_rows

    return _summarize_report_rows(rows)


def build_report_meta(
    *,
    report_kind: str,
    job_type: str,
    report_label: str,
    period_key: str,
    period_label: str,
    source: str,
    generated_at: str,
    model_label: str,
    title: str,
    summary: str,
) -> dict[str, Any]:
    from .report_engine import build_report_meta as _build_report_meta

    return _build_report_meta(
        report_kind=report_kind,
        job_type=job_type,
        report_label=report_label,
        period_key=period_key,
        period_label=period_label,
        source=source,
        generated_at=generated_at,
        model_label=model_label,
        title=title,
        summary=summary,
    )


def persist_report_record(
    *,
    path: Path,
    meta: dict[str, Any],
    markdown: str,
    parse_record=None,
) -> dict[str, Any]:
    from .report_engine import persist_report_record as _persist_report_record

    return _persist_report_record(path=path, meta=meta, markdown=markdown, parse_record=parse_record)
