from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Any


WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from core_service.runtime_data import legacy_app_runtime_root, workspace_root  # noqa: E402


WORKSPACE_LEGACY_DIRS = (
    "core_service/data",
    "nav_dashboard/data",
    "library_tracker/data",
    "property/data",
    "journey/data",
)
WORKSPACE_LEGACY_FILES = (
    "data/custom_cards.json",
    "data/nav_dashboard_deploy.json",
    "data/nav_dashboard_notifications.json",
    "data/nav_dashboard_overview_snapshot.json",
    "data/journey.db",
    "data/property.db",
)
APPDATA_LEGACY_APPS = ("core_service", "nav_dashboard", "library_tracker", "property", "journey")
FROZEN_LEGACY_ROOT = Path("data") / "core_service" / "legacy_frozen"


def frozen_legacy_root() -> Path:
    return workspace_root() / FROZEN_LEGACY_ROOT


def _frozen_relative_path(path: Path) -> Path:
    root = workspace_root()
    if path.is_relative_to(root):
        return Path("workspace") / path.relative_to(root)
    for app_name in APPDATA_LEGACY_APPS:
        app_root = legacy_app_runtime_root(app_name)
        if path == app_root:
            return Path("appdata") / app_name
        if path.is_relative_to(app_root):
            return Path("appdata") / app_name / path.relative_to(app_root)
    return Path("external") / path.name


def _backup_path(path: Path) -> Path:
    return frozen_legacy_root() / _frozen_relative_path(path)


def _conflict_backup_path(path: Path) -> Path:
    base_path = _backup_path(path)
    base_name = base_path.name
    index = 2
    while True:
        candidate = base_path.with_name(f"{base_name}.{index}")
        if not candidate.exists():
            return candidate
        index += 1


def _merge_tree_missing_only(source: Path, target: Path) -> None:
    for item in source.rglob("*"):
        relative = item.relative_to(source)
        target_item = target / relative
        if item.is_dir():
            target_item.mkdir(parents=True, exist_ok=True)
            continue
        if target_item.exists():
            continue
        target_item.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(item), str(target_item))


def _prune_empty_dirs(root: Path) -> None:
    if not root.exists() or not root.is_dir():
        return
    for item in sorted(root.rglob("*"), key=lambda value: len(value.parts), reverse=True):
        if item.is_dir():
            try:
                item.rmdir()
            except OSError:
                continue
    try:
        root.rmdir()
    except OSError:
        pass


def legacy_freeze_targets(*, include_appdata: bool = False) -> list[Path]:
    targets = [workspace_root() / relative for relative in WORKSPACE_LEGACY_DIRS]
    targets.extend(workspace_root() / relative for relative in WORKSPACE_LEGACY_FILES)
    if include_appdata:
        targets.extend(legacy_app_runtime_root(app_name) for app_name in APPDATA_LEGACY_APPS)
    seen: set[Path] = set()
    unique_targets: list[Path] = []
    for target in targets:
        if target in seen:
            continue
        seen.add(target)
        unique_targets.append(target)
    return unique_targets


def collect_legacy_freeze_status(*, include_appdata: bool = False) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for target in legacy_freeze_targets(include_appdata=include_appdata):
        backup = _backup_path(target)
        rows.append(
            {
                "path": str(target),
                "path_relative": str(target.relative_to(workspace_root())).replace("\\", "/") if target.is_relative_to(workspace_root()) else str(target),
                "backup_path": str(backup),
                "exists": target.exists(),
                "backup_exists": backup.exists(),
                "state": (
                    "conflict"
                    if target.exists() and backup.exists()
                    else "active"
                    if target.exists()
                    else "frozen"
                    if backup.exists()
                    else "missing"
                ),
            }
        )
    return rows


def apply_legacy_freeze(*, include_appdata: bool = False) -> dict[str, Any]:
    renamed: list[str] = []
    skipped: list[str] = []
    merged: list[str] = []
    conflicts: list[str] = []
    for row in collect_legacy_freeze_status(include_appdata=include_appdata):
        target = Path(row["path"])
        backup = Path(row["backup_path"])
        if row["state"] == "active":
            backup.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(target), str(backup))
            renamed.append(str(target))
        elif row["state"] == "conflict":
            if target.is_dir() and backup.is_dir():
                _merge_tree_missing_only(target, backup)
                _prune_empty_dirs(target)
                if not target.exists():
                    merged.append(str(target))
                else:
                    conflict_path = _conflict_backup_path(target)
                    conflict_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(target), str(conflict_path))
                    merged.append(str(target))
            else:
                conflicts.append(str(target))
        else:
            skipped.append(str(target))
    return {
        "action": "apply",
        "renamed": renamed,
        "merged": merged,
        "skipped": skipped,
        "conflicts": conflicts,
        "status": collect_legacy_freeze_status(include_appdata=include_appdata),
    }


def restore_legacy_freeze(*, include_appdata: bool = False) -> dict[str, Any]:
    restored: list[str] = []
    skipped: list[str] = []
    conflicts: list[str] = []
    for row in collect_legacy_freeze_status(include_appdata=include_appdata):
        target = Path(row["path"])
        backup = Path(row["backup_path"])
        if row["state"] == "frozen":
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(backup), str(target))
            restored.append(str(target))
        elif row["state"] == "conflict":
            conflicts.append(str(target))
        else:
            skipped.append(str(target))
    return {
        "action": "restore",
        "restored": restored,
        "skipped": skipped,
        "conflicts": conflicts,
        "status": collect_legacy_freeze_status(include_appdata=include_appdata),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Move legacy workspace data roots into data/core_service/legacy_frozen and optionally restore them.")
    parser.add_argument("command", choices=("status", "apply", "restore"))
    parser.add_argument("--include-appdata", action="store_true", help="Also freeze legacy LOCALAPPDATA runtime roots")
    args = parser.parse_args()

    if args.command == "status":
        payload: dict[str, Any] = {"action": "status", "status": collect_legacy_freeze_status(include_appdata=bool(args.include_appdata))}
    elif args.command == "apply":
        payload = apply_legacy_freeze(include_appdata=bool(args.include_appdata))
    else:
        payload = restore_legacy_freeze(include_appdata=bool(args.include_appdata))
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())