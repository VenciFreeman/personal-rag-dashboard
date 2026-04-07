"""Export records with missing metadata fields into a grouped markdown report.

This script scans library_tracker structured JSON files and writes one markdown
file that is overwritten on every run.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple


WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from core_service.runtime_data import app_runtime_root


DEFAULT_STRUCTURED_FILES: Sequence[str] = (
    "reading.json",
    "music.json",
    "video.json",
    "game.json",
)

# Support both the correct field name and the typo requested by the user.
TARGET_FIELDS: Sequence[str] = (
    "author",
    "nationality",
    "natyionality",
    "category",
    "channel",
    "publisher",
    "url",
)


def is_blank(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return False


def present_value_for(record: Dict[str, object], logical_field: str) -> object:
    if logical_field == "nationality":
        if "nationality" in record:
            return record.get("nationality")
        return record.get("natyionality")
    return record.get(logical_field)


def missing_logical_fields(record: Dict[str, object]) -> List[str]:
    logical_fields = [
        "author",
        "nationality",
        "category",
        "channel",
        "publisher",
        "url",
    ]
    missing: List[str] = []
    for field in logical_fields:
        if is_blank(present_value_for(record, field)):
            missing.append(field)
    return missing


def collect_records(structured_dir: Path, file_names: Iterable[str]) -> List[Tuple[str, Dict[str, object]]]:
    structured_dir = resolve_entity_dir(structured_dir)
    rows: List[Tuple[str, Dict[str, object]]] = []
    for name in file_names:
        file_path = structured_dir / name
        if not file_path.exists():
            continue
        payload = json.loads(file_path.read_text(encoding="utf-8"))
        records = payload.get("records") or []
        if not isinstance(records, list):
            continue
        for record in records:
            if isinstance(record, dict):
                rows.append((name, record))
    return rows


def resolve_entity_dir(structured_dir: Path) -> Path:
    candidate = Path(structured_dir)
    if candidate.name == "entities":
        return candidate
    entities_dir = candidate / "entities"
    if entities_dir.exists() and entities_dir.is_dir():
        return entities_dir
    return candidate


def build_report_lines(rows: List[Tuple[str, Dict[str, object]]]) -> List[str]:
    grouped: Dict[int, List[Tuple[str, Dict[str, object], List[str]]]] = defaultdict(list)
    total_with_missing = 0

    for source_name, record in rows:
        missing = missing_logical_fields(record)
        if not missing:
            continue
        total_with_missing += 1
        grouped[len(missing)].append((source_name, record, missing))

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines: List[str] = [
        "# Library Tracker 缺失字段清单",
        "",
        f"- 生成时间: {now}",
        f"- 扫描记录总数: {len(rows)}",
        f"- 存在缺失字段的记录数: {total_with_missing}",
        "",
    ]

    if not grouped:
        lines.append("当前没有命中缺失字段的记录。")
        return lines

    for missing_count in sorted(grouped.keys(), reverse=True):
        entries = grouped[missing_count]
        lines.append(f"## 缺失 {missing_count} 个字段 ({len(entries)} 条)")
        lines.append("")
        for index, (source_name, record, missing_fields) in enumerate(entries, start=1):
            title = str(record.get("title") or "(无标题)").strip() or "(无标题)"
            media_type = str(record.get("media_type") or "").strip()
            date_value = str(record.get("date") or "").strip()
            missing_text = "、".join(missing_fields)
            meta = []
            if media_type:
                meta.append(media_type)
            if date_value:
                meta.append(date_value)
            meta_text = " | ".join(meta)
            if meta_text:
                lines.append(
                    f"{index}. {title}（来源: {source_name}；{meta_text}；缺失: {missing_text}）"
                )
            else:
                lines.append(
                    f"{index}. {title}（来源: {source_name}；缺失: {missing_text}）"
                )
        lines.append("")

    return lines


def main() -> None:
    runtime_root = app_runtime_root("library_tracker")
    default_structured_dir = runtime_root / "structured" / "entities"
    default_output = runtime_root / "analysis" / "reports" / "missing_metadata_report.md"

    parser = argparse.ArgumentParser(description="Export missing metadata records to markdown")
    parser.add_argument(
        "--structured-dir",
        type=Path,
        default=default_structured_dir,
        help="Directory containing structured JSON files",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=default_output,
        help="Output markdown path (will be overwritten)",
    )
    parser.add_argument(
        "--files",
        nargs="*",
        default=list(DEFAULT_STRUCTURED_FILES),
        help="Structured JSON filenames to scan",
    )
    args = parser.parse_args()

    rows = collect_records(args.structured_dir, args.files)
    lines = build_report_lines(rows)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote markdown report: {args.output}")


if __name__ == "__main__":
    main()
