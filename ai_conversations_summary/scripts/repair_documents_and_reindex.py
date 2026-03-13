from __future__ import annotations

import argparse
import json
import re
import shutil
from dataclasses import dataclass
from pathlib import Path


QUOTE_DATE_RE = re.compile(r"^\s*>\s*-\s*\*\*date\*\*:\s*(.+?)\s*$", re.IGNORECASE)
TOPIC_HEADER_RE = re.compile(r"^#{1,3}\s*主题\s*([0-9一二三四五六七八九十]+)\s*[：:]\s*(.+?)\s*$", re.MULTILINE)
DATE_TEXT_RE = re.compile(r"(\d{4})[-_](\d{2})[-_](\d{2})")
DATE_YYMMDD_RE = re.compile(r"\b(\d{6})\b")
FILE_PREFIX_YYMMDD_RE = re.compile(r"^(\d{6})(?:[_-]|$)")
FILE_PREFIX_YYYYMMDD_RE = re.compile(r"^(\d{8})(?:[_-]|$)")
FILE_PREFIX_YYYY_MM_DD_RE = re.compile(r"^(\d{4})[-_](\d{2})[-_](\d{2})(?:[-_]\d{4,6})?$")


@dataclass
class RepairStats:
    scanned: int = 0
    moved_for_resplit: int = 0
    renamed_by_date: int = 0
    skipped_no_date: int = 0


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def _extract_date_prefix(text: str) -> str:
    for line in text.splitlines()[:80]:
        m = QUOTE_DATE_RE.match(line)
        if not m:
            continue
        raw = m.group(1).strip().strip('"\'')
        if not raw:
            continue
        date_match = DATE_TEXT_RE.search(raw)
        if date_match:
            yyyy, mm, dd = date_match.groups()
            return f"{yyyy[2:]}{mm}{dd}"
        short_match = DATE_YYMMDD_RE.search(raw)
        if short_match:
            return short_match.group(1)
    return ""


def _extract_existing_prefix(stem: str) -> str:
    m6 = FILE_PREFIX_YYMMDD_RE.match(stem)
    if m6:
        return m6.group(1)

    m8 = FILE_PREFIX_YYYYMMDD_RE.match(stem)
    if m8:
        value = m8.group(1)
        return f"{value[2:4]}{value[4:6]}{value[6:8]}"

    m_dash = FILE_PREFIX_YYYY_MM_DD_RE.match(stem)
    if m_dash:
        yyyy, mm, dd = m_dash.groups()
        return f"{yyyy[2:]}{mm}{dd}"

    return ""


def _remove_prefix(stem: str) -> str:
    stem = re.sub(r"^(\d{6})([_-]?)", "", stem)
    stem = re.sub(r"^(\d{8})([_-]?)", "", stem)
    stem = re.sub(r"^(\d{4}[-_]\d{2}[-_]\d{2}(?:[-_]\d{4,6})?)([_-]?)", "", stem)
    return stem.lstrip("_-")


def _ensure_unique(path: Path) -> Path:
    if not path.exists():
        return path
    base = path.stem
    suffix = path.suffix
    parent = path.parent
    idx = 2
    while True:
        candidate = parent / f"{base}_{idx}{suffix}"
        if not candidate.exists():
            return candidate
        idx += 1


def _is_unsplit_multi_topic(text: str) -> bool:
    if "本文概览" not in text:
        return False
    return len(TOPIC_HEADER_RE.findall(text)) >= 2


def run_repair(*, root: Path, dry_run: bool = False) -> dict[str, object]:
    documents_dir = root / "documents"
    summarize_dir = root / "data" / "summarize_dir"
    summarize_dir.mkdir(parents=True, exist_ok=True)

    stats = RepairStats()
    moved_files: list[str] = []
    renamed_files: list[dict[str, str]] = []

    for file_path in sorted(documents_dir.rglob("*.md")):
        if not file_path.is_file():
            continue
        stats.scanned += 1
        text = _read_text(file_path)

        if _is_unsplit_multi_topic(text):
            target = _ensure_unique(summarize_dir / file_path.name)
            moved_files.append(f"{file_path} -> {target}")
            stats.moved_for_resplit += 1
            if not dry_run:
                shutil.move(str(file_path), str(target))
            continue

        expected_prefix = _extract_date_prefix(text)
        if not expected_prefix:
            stats.skipped_no_date += 1
            continue

        stem = file_path.stem
        current_prefix = _extract_existing_prefix(stem)
        if current_prefix == expected_prefix:
            continue

        title_stem = _remove_prefix(stem)
        if not title_stem:
            title_stem = stem
        new_name = f"{expected_prefix}_{title_stem}{file_path.suffix}"
        target = _ensure_unique(file_path.parent / new_name)
        renamed_files.append({"from": str(file_path), "to": str(target)})
        stats.renamed_by_date += 1
        if not dry_run:
            file_path.rename(target)

    return {
        "dry_run": dry_run,
        "documents_scanned": stats.scanned,
        "moved_for_resplit": stats.moved_for_resplit,
        "renamed_by_date": stats.renamed_by_date,
        "skipped_no_date": stats.skipped_no_date,
        "moved_examples": moved_files[:20],
        "renamed_examples": renamed_files[:20],
    }


def _parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description="Repair document naming/splitting inconsistencies and prepare for reindex")
    parser.add_argument("--root", default=str(root), help="ai_conversations_summary root directory")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes only")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    payload = run_repair(root=Path(args.root), dry_run=bool(args.dry_run))
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
