from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any, Callable, Iterable

from .reporting import parse_frontmatter, save_json_file, write_report


def ensure_storage_paths(paths: Iterable[Path]) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def load_state_payload(path: Path, defaults: dict[str, Any], *, load_json: Callable[[Path, Any], Any]) -> dict[str, Any]:
    payload = load_json(path, {})
    merged = dict(defaults)
    if isinstance(payload, dict):
        for key in defaults:
            merged[key] = payload.get(key, defaults[key])
    return merged


def save_state_payload(path: Path, payload: dict[str, Any]) -> None:
    save_json_file(path, payload)


def parse_report_record(
    path: Path,
    *,
    valid_kinds: set[str],
    job_type_by_kind: dict[str, str],
    source_resolver: Callable[[Path, dict[str, str]], str] | None = None,
    record_normalizer: Callable[[dict[str, Any], dict[str, str], Path], dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    try:
        raw = path.read_text(encoding="utf-8")
    except Exception:
        return None
    meta, body = parse_frontmatter(raw)
    kind = str(meta.get("report_kind") or "").strip()
    if kind not in valid_kinds:
        return None
    source = source_resolver(path, meta) if callable(source_resolver) else str(meta.get("source") or "local")
    record = {
        "report_kind": kind,
        "job_type": str(meta.get("job_type") or job_type_by_kind.get(kind, "")),
        "period_key": str(meta.get("period_key") or ""),
        "period_label": str(meta.get("period_label") or meta.get("period_key") or ""),
        "source": source,
        "generated_at": str(meta.get("generated_at") or ""),
        "model_label": str(meta.get("model_label") or ""),
        "title": str(meta.get("title") or path.stem),
        "file_name": path.name,
        "path": str(path),
        "markdown": body,
        "summary": str(meta.get("summary") or ""),
    }
    if callable(record_normalizer):
        record = record_normalizer(record, meta, path)
    return record


def delete_report_files(rows: list[dict[str, Any]], *, period_key: str, source: str | None = None) -> list[str]:
    removed: list[str] = []
    for row in rows:
        if str(row.get("period_key") or "") != str(period_key or ""):
            continue
        if source and str(row.get("source") or "") != str(source or ""):
            continue
        path = Path(str(row.get("path") or ""))
        if path.exists():
            path.unlink()
            removed.append(str(path))
    return removed


def open_report_folder(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError("analysis report path not found")
    folder = path.parent
    try:
        if hasattr(os, "startfile"):
            os.startfile(str(folder))  # type: ignore[attr-defined]
        elif os.name == "posix":
            subprocess.Popen(["xdg-open", str(folder)])
        else:
            subprocess.Popen(["open", str(folder)])
    except FileNotFoundError:
        raise
    except Exception as exc:
        raise RuntimeError(f"failed to open report location: {exc}") from exc
    return {"ok": True, "folder": str(folder), "path": str(path)}


def summarize_report_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keys = ("report_kind", "job_type", "period_key", "period_label", "source", "generated_at", "model_label", "title", "file_name", "summary")
    return [{key: row[key] for key in keys} for row in rows]


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
    return {
        "report_kind": report_kind,
        "job_type": job_type,
        "report_label": report_label,
        "period_key": period_key,
        "period_label": period_label,
        "source": source,
        "generated_at": generated_at,
        "model_label": model_label,
        "title": title,
        "summary": summary,
    }


def persist_report_record(
    *,
    path: Path,
    meta: dict[str, Any],
    markdown: str,
    parse_record: Callable[[Path], dict[str, Any] | None] | None = None,
) -> dict[str, Any]:
    write_report(path, meta, markdown)
    if callable(parse_record):
        parsed = parse_record(path)
        if parsed:
            return parsed
    return {
        "report_kind": str(meta.get("report_kind") or ""),
        "job_type": str(meta.get("job_type") or ""),
        "period_key": str(meta.get("period_key") or ""),
        "period_label": str(meta.get("period_label") or meta.get("period_key") or ""),
        "source": str(meta.get("source") or ""),
        "generated_at": str(meta.get("generated_at") or ""),
        "model_label": str(meta.get("model_label") or ""),
        "title": str(meta.get("title") or path.stem),
        "file_name": path.name,
        "path": str(path),
        "markdown": str(markdown or "").strip(),
        "summary": str(meta.get("summary") or ""),
    }