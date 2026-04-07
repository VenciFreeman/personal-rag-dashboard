from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from core_service.runtime_data import KNOWN_RUNTIME_DATA_ENTRIES, migrate_core_runtime_data


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate shared core runtime data into the project-level runtime root")
    parser.add_argument("--apply", action="store_true", help="Copy files instead of printing the migration plan")
    parser.add_argument(
        "--entry",
        action="append",
        default=[],
        help=f"Restrict migration to one runtime entry (default: all of {', '.join(KNOWN_RUNTIME_DATA_ENTRIES)})",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    result = migrate_core_runtime_data(
        dry_run=not bool(args.apply),
        entries=args.entry or None,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()