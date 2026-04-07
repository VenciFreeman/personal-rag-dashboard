from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from ai_conversations_summary.runtime_paths import DOCUMENTS_DIR, VECTOR_DB_DIR


def _normalize_rel(value: str) -> str:
    text = (value or "").strip().replace("\\", "/")
    while text.startswith("./"):
        text = text[2:]
    if text.startswith("documents/"):
        text = text[len("documents/") :]
    return text


def _derive_relative_path(file_path: str, documents_dir: Path) -> str:
    raw = (file_path or "").strip()
    if not raw:
        return ""

    candidate = Path(raw)
    if candidate.is_absolute():
        try:
            return candidate.relative_to(documents_dir).as_posix()
        except Exception:
            pass

    normalized = raw.replace("\\", "/")
    marker = "/documents/"
    low = normalized.lower()
    idx = low.find(marker)
    if idx >= 0:
        return normalized[idx + len(marker) :].lstrip("/")

    return _normalize_rel(normalized)


def normalize_metadata(
    *,
    metadata_path: Path,
    documents_dir: Path,
    file_path_mode: str,
    dry_run: bool,
    create_backup: bool,
) -> dict[str, Any]:
    payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    manifest: dict[str, Any] = {}
    rows: list[dict[str, Any]]
    if isinstance(payload, list):
        rows = [row for row in payload if isinstance(row, dict)]
    elif isinstance(payload, dict):
        if isinstance(payload.get("manifest"), dict):
            manifest = dict(payload.get("manifest"))
        docs = payload.get("documents")
        if not isinstance(docs, list):
            raise RuntimeError("metadata.json must be a JSON array or object with documents")
        rows = [row for row in docs if isinstance(row, dict)]
    else:
        raise RuntimeError("metadata.json must be a JSON array or object with documents")

    changed_rows = 0
    rel_filled = 0
    file_path_rewritten = 0
    abs_file_path_before = 0
    abs_file_path_after = 0

    for row in rows:

        current_rel = _normalize_rel(str(row.get("relative_path") or ""))
        current_file = str(row.get("file_path") or "").strip()

        if Path(current_file).is_absolute():
            abs_file_path_before += 1

        derived_rel = current_rel or _derive_relative_path(current_file, documents_dir)
        derived_rel = _normalize_rel(derived_rel)

        row_changed = False

        if derived_rel and derived_rel != current_rel:
            row["relative_path"] = derived_rel
            rel_filled += 1
            row_changed = True

        if file_path_mode == "relative":
            new_file_path = derived_rel
        elif file_path_mode == "empty":
            new_file_path = ""
        else:
            new_file_path = current_file

        if new_file_path != current_file:
            row["file_path"] = new_file_path
            file_path_rewritten += 1
            row_changed = True

        if Path(str(row.get("file_path") or "")).is_absolute():
            abs_file_path_after += 1

        if row_changed:
            changed_rows += 1

    if not dry_run:
        if create_backup:
            backup_path = metadata_path.with_suffix(metadata_path.suffix + ".bak")
            backup_path.write_text(metadata_path.read_text(encoding="utf-8"), encoding="utf-8")
        out_payload = {
            "manifest": manifest,
            "documents": rows,
        }
        metadata_path.write_text(json.dumps(out_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "metadata_path": str(metadata_path),
        "documents_dir": str(documents_dir),
        "file_path_mode": file_path_mode,
        "dry_run": dry_run,
        "rows_total": len(rows),
        "rows_changed": changed_rows,
        "relative_path_filled": rel_filled,
        "file_path_rewritten": file_path_rewritten,
        "absolute_file_path_before": abs_file_path_before,
        "absolute_file_path_after": abs_file_path_after,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize vector metadata paths")
    parser.add_argument(
        "--metadata-path",
        default=str(VECTOR_DB_DIR / "metadata.json"),
        help="Path to metadata.json",
    )
    parser.add_argument(
        "--documents-dir",
        default=str(DOCUMENTS_DIR),
        help="Root documents directory",
    )
    parser.add_argument(
        "--file-path-mode",
        choices=["relative", "empty", "keep"],
        default="relative",
        help="How to rewrite file_path values",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing")
    parser.add_argument("--no-backup", action="store_true", help="Disable .bak backup output")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    metadata_path = Path(args.metadata_path)
    documents_dir = Path(args.documents_dir)
    if not metadata_path.exists():
        raise RuntimeError(f"Metadata file not found: {metadata_path}")

    result = normalize_metadata(
        metadata_path=metadata_path,
        documents_dir=documents_dir,
        file_path_mode=str(args.file_path_mode),
        dry_run=bool(args.dry_run),
        create_backup=not bool(args.no_backup),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
