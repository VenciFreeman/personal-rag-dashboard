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


KEEP_DIR = app_runtime_root("library_tracker") / "structured" / "aliases" / "keep_original"


def process_file(path: Path) -> int:
    payload = json.loads(path.read_text(encoding="utf-8"))
    entries = payload.get("entries", []) if isinstance(payload, dict) else []
    updated = 0

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        if "aliases" in entry:
            entry.pop("aliases", None)
            updated += 1

    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return updated


def main() -> None:
    total_files = 0
    total_entries = 0

    for path in sorted(KEEP_DIR.glob("*_keep.json")):
        count = process_file(path)
        total_files += 1
        total_entries += count
        print(f"{path.name}: {count}")

    print(f"files={total_files}, entries={total_entries}")


if __name__ == "__main__":
    main()
