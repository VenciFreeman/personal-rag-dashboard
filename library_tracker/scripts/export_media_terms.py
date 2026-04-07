from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Iterable

PROJECT_ROOT = Path(__file__).resolve().parent.parent
WORKSPACE_ROOT = PROJECT_ROOT.parent
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from core_service.runtime_data import app_runtime_root  # noqa: E402


STRUCTURED_ROOT = app_runtime_root("library_tracker") / "structured"
DEFAULT_INPUT_DIR = STRUCTURED_ROOT / "entities"
DEFAULT_OUTPUT_DIR = STRUCTURED_ROOT / "exports"

MEDIA_FILES = {
    "book": "reading.json",
    "video": "video.json",
    "music": "music.json",
    "game": "game.json",
}

_SPLIT_PATTERN = re.compile(r"[;,，；/、|&\n\r\t]+")


def _load_records(path: Path) -> list[dict]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    records = payload.get("records", []) if isinstance(payload, dict) else []
    return [row for row in records if isinstance(row, dict)]


def _iter_tokens(raw: str) -> Iterable[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    parts = _SPLIT_PATTERN.split(text)
    return (p.strip() for p in parts if p and p.strip())


def _collect_media_terms(records: list[dict]) -> dict[str, list[str]]:
    titles: set[str] = set()
    authors: set[str] = set()
    publishers: set[str] = set()

    for row in records:
        title = str(row.get("title") or "").strip()
        if title:
            titles.add(title)

        for token in _iter_tokens(str(row.get("author") or "")):
            authors.add(token)

        for token in _iter_tokens(str(row.get("publisher") or "")):
            publishers.add(token)

    return {
        "title": sorted(titles),
        "author": sorted(authors),
        "publisher": sorted(publishers),
    }


def _build_entries(media_type: str, field: str, terms: list[str]) -> dict[str, list[dict[str, object]]]:
    return {
        "entries": [
            {
                "key": f"{media_type}|{field}",
                "media_type": media_type,
                "canonical": term,
                "aliases": [],
                "status": "proposal",
            }
            for term in terms
        ]
    }


def export_terms(input_dir: Path, output_dir: Path) -> dict[str, dict[str, int]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    counts: dict[str, dict[str, int]] = {}

    for media_type, filename in MEDIA_FILES.items():
        source_file = input_dir / filename
        records = _load_records(source_file)
        terms_by_field = _collect_media_terms(records)
        media_counts: dict[str, int] = {}

        for field in ("title", "author", "publisher"):
            terms = terms_by_field[field]
            target_file = output_dir / f"{media_type}_{field}.json"
            payload = _build_entries(media_type, field, terms)
            target_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            media_counts[field] = len(terms)

        counts[media_type] = media_counts

    return counts


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Export deduplicated title/author/publisher terms per media type "
            "from library_tracker structured JSON files."
        )
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help="Directory containing reading.json/video.json/music.json/game.json",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory to write output JSON files",
    )
    args = parser.parse_args()

    counts = export_terms(args.input_dir, args.output_dir)
    for media_type in MEDIA_FILES:
        media_counts = counts.get(media_type, {})
        print(
            f"{media_type}: "
            f"title={media_counts.get('title', 0)}, "
            f"author={media_counts.get('author', 0)}, "
            f"publisher={media_counts.get('publisher', 0)}"
        )


if __name__ == "__main__":
    main()
