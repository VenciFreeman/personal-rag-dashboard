from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
WORKSPACE_ROOT = PROJECT_ROOT.parent
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from core_service.runtime_data import app_runtime_root  # noqa: E402


STRUCTURED_ROOT = app_runtime_root("library_tracker") / "structured"
SOURCE_DIR = STRUCTURED_ROOT / "exports"
OUTPUT_DIR = STRUCTURED_ROOT / "aliases"

SOURCE_FILES = [
    "book_title.json",
    "book_author.json",
    "book_publisher.json",
    "video_title.json",
    "video_author.json",
    "video_publisher.json",
    "music_title.json",
    "music_author.json",
    "music_publisher.json",
    "game_title.json",
    "game_author.json",
    "game_publisher.json",
]


def _load_entries(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    entries = payload.get("entries", []) if isinstance(payload, dict) else []
    return [entry for entry in entries if isinstance(entry, dict)]


def _sort_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(entries, key=lambda entry: str(entry.get("canonical") or "").casefold())


def _normalize_aliases(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    output: list[str] = []
    seen: set[str] = set()
    for item in value:
        text = str(item or "").strip()
        if not text:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        output.append(text)
    return output


def _rewrite_entries(entries: list[dict[str, Any]], *, status: str, with_aliases: bool) -> dict[str, list[dict[str, Any]]]:
    rewritten: list[dict[str, Any]] = []
    for entry in _sort_entries(entries):
        aliases = _normalize_aliases(entry.get("aliases"))
        row = dict(entry)
        row["aliases"] = aliases if with_aliases else []
        row["status"] = status
        rewritten.append(row)
    return {"entries": rewritten}


def main() -> None:
    approved_dir = OUTPUT_DIR / "approved"
    keep_dir = OUTPUT_DIR / "keep_original"
    approved_dir.mkdir(parents=True, exist_ok=True)
    keep_dir.mkdir(parents=True, exist_ok=True)

    for filename in SOURCE_FILES:
        source_path = SOURCE_DIR / filename
        entries = _load_entries(source_path)
        alias_entries = [entry for entry in entries if _normalize_aliases(entry.get("aliases"))]
        keep_entries = [entry for entry in entries if not _normalize_aliases(entry.get("aliases"))]

        stem = source_path.stem
        alias_path = approved_dir / f"{stem}_alias.json"
        keep_path = keep_dir / f"{stem}_keep.json"

        alias_payload = _rewrite_entries(alias_entries, status="approved", with_aliases=True)
        keep_payload = _rewrite_entries(keep_entries, status="keep_original", with_aliases=False)

        alias_path.write_text(json.dumps(alias_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        keep_path.write_text(json.dumps(keep_payload, ensure_ascii=False, indent=2), encoding="utf-8")

        print(f"{filename}: alias={len(alias_entries)}, keep={len(keep_entries)}")


if __name__ == "__main__":
    main()
