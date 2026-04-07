from __future__ import annotations

import argparse
import json
import shutil
import threading
from pathlib import Path
from typing import Any

from core_service.runtime_data import app_runtime_root, legacy_app_runtime_root, migrate_core_runtime_data
from nav_dashboard.web.services.runtime_paths import nav_dashboard_runtime_migration_plan


_RUNTIME_MIGRATION_LOCK = threading.Lock()
_RUNTIME_MIGRATION_DONE = False
APP_RUNTIME_MIGRATION_MARKER = ".app_runtime_data_migrated.json"


def _app_runtime_marker_path(target_root: Path) -> Path:
    return target_root / APP_RUNTIME_MIGRATION_MARKER


def _write_app_runtime_marker(target_root: Path, payload: dict[str, Any]) -> None:
    marker = _app_runtime_marker_path(target_root)
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _copy_path(source: Path, target: Path) -> None:
    if source.is_dir():
        shutil.copytree(source, target, dirs_exist_ok=True)
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)


def _load_json_file(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _count_nonempty_custom_cards(rows: Any) -> int:
    if not isinstance(rows, list):
        return 0
    count = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("title") or "").strip() and str(row.get("url") or "").strip():
            count += 1
    return count


def _looks_like_default_custom_cards(rows: Any) -> bool:
    if not isinstance(rows, list) or len(rows) < 2:
        return False
    normalized = [row if isinstance(row, dict) else {} for row in rows]
    first_title = str(normalized[0].get("title") or "").strip()
    second_title = str(normalized[1].get("title") or "").strip()
    if first_title != "RAG System" or second_title != "Library Tracker":
        return False
    for row in normalized[2:]:
        if str(row.get("title") or "").strip() or str(row.get("url") or "").strip() or str(row.get("image") or "").strip():
            return False
    return True


def _should_restore_custom_cards(source: Path, target: Path) -> bool:
    source_rows = _load_json_file(source)
    target_rows = _load_json_file(target)
    if _count_nonempty_custom_cards(source_rows) <= 0:
        return False
    if _count_nonempty_custom_cards(target_rows) <= 0:
        return True
    return _looks_like_default_custom_cards(target_rows)


def _merge_notification_state(source: Path, target: Path) -> bool:
    source_payload = _load_json_file(source)
    target_payload = _load_json_file(target)
    if not isinstance(source_payload, dict) or not isinstance(target_payload, dict):
        return False
    source_dismissed = dict(source_payload.get("dismissed") or {}) if isinstance(source_payload.get("dismissed"), dict) else {}
    target_dismissed = dict(target_payload.get("dismissed") or {}) if isinstance(target_payload.get("dismissed"), dict) else {}
    merged = dict(source_dismissed)
    merged.update(target_dismissed)
    if merged == target_dismissed:
        return False
    updated_payload = dict(target_payload)
    updated_payload["dismissed"] = merged
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(updated_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return True


def _merge_directory_missing_only(source: Path, target: Path) -> int:
    copied = 0
    for item in source.rglob("*"):
        relative = item.relative_to(source)
        target_item = target / relative
        if item.is_dir():
            target_item.mkdir(parents=True, exist_ok=True)
            continue
        target_item.parent.mkdir(parents=True, exist_ok=True)
        if target_item.exists():
            continue
        shutil.copy2(item, target_item)
        copied += 1
    return copied


def migrate_nav_dashboard_runtime_data(*, dry_run: bool = True) -> dict[str, Any]:
    target_root = app_runtime_root("nav_dashboard")
    marker = _app_runtime_marker_path(target_root)
    marker_exists = marker.exists()
    copied: list[str] = []
    merged: list[str] = []
    preserved: list[str] = []
    skipped: list[str] = []
    plan: list[dict[str, Any]] = []
    for item in nav_dashboard_runtime_migration_plan():
        source = Path(item["source"])
        target = Path(item["target"])
        entry = str(item["entry"])
        source_exists = source.exists()
        target_exists = target.exists()
        if marker_exists:
            action = "skip_marker"
        elif source_exists and source.is_dir() and target_exists:
            action = "merge_missing"
        elif source_exists and not target_exists:
            action = "copy"
        elif entry == "nav_dashboard_notifications.json" and source_exists and target_exists:
            action = "merge_notifications"
        elif entry == "custom_cards.json" and source_exists and target_exists and _should_restore_custom_cards(source, target):
            action = "restore_custom_cards"
        elif source_exists and target_exists:
            action = "keep_existing"
        else:
            action = "skip_missing"
        plan.append(
            {
                "entry": entry,
                "source": str(source),
                "target": str(target),
                "source_exists": source_exists,
                "target_exists": target_exists,
                "kind": "dir" if source.is_dir() else "file",
                "action": action,
            }
        )
        if action == "copy":
            copied.append(entry)
            if not dry_run:
                _copy_path(source, target)
        elif action == "merge_missing":
            merged.append(entry)
            if not dry_run:
                _merge_directory_missing_only(source, target)
        elif action == "merge_notifications":
            merged.append(entry)
            if not dry_run:
                _merge_notification_state(source, target)
        elif action == "restore_custom_cards":
            copied.append(entry)
            if not dry_run:
                _copy_path(source, target)
        elif action == "keep_existing":
            preserved.append(entry)
        else:
            skipped.append(entry)
    result = {
        "dry_run": dry_run,
        "target_root": str(target_root),
        "migration_marker": str(marker),
        "marker_exists": marker_exists,
        "plan": plan,
        "copied_entries": copied,
        "merged_entries": merged,
        "preserved_entries": preserved,
        "skipped_entries": skipped,
    }
    if not dry_run and not marker_exists:
        _write_app_runtime_marker(
            target_root,
            {
                "app_name": "nav_dashboard",
                "target_root": str(target_root),
                "copied_entries": copied,
                "merged_entries": merged,
                "preserved_entries": preserved,
                "skipped_entries": skipped,
            },
        )
    return result


def _workspace_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _migrate_app_runtime_tree(app_name: str, *, repo_legacy_root: Path, dry_run: bool = True) -> dict[str, Any]:
    target_root = app_runtime_root(app_name)
    marker = _app_runtime_marker_path(target_root)
    marker_exists = marker.exists()
    source_roots: list[Path] = []
    for candidate in (legacy_app_runtime_root(app_name), repo_legacy_root):
        if candidate not in source_roots:
            source_roots.append(candidate)

    projected_target_exists = target_root.exists()
    copied: list[str] = []
    merged: list[str] = []
    skipped: list[str] = []
    plan: list[dict[str, Any]] = []

    for source in source_roots:
        source_exists = source.exists()
        if marker_exists:
            action = "skip_marker"
        elif source_exists and not projected_target_exists:
            action = "copy"
            projected_target_exists = True
        elif source_exists:
            action = "merge_missing"
        else:
            action = "skip_missing"
        plan.append(
            {
                "source": str(source),
                "target": str(target_root),
                "source_exists": source_exists,
                "target_exists": target_root.exists(),
                "kind": "dir",
                "action": action,
            }
        )
        if action == "copy":
            copied.append(str(source))
            if not dry_run:
                _copy_path(source, target_root)
        elif action == "merge_missing":
            merged.append(str(source))
            if not dry_run:
                target_root.mkdir(parents=True, exist_ok=True)
                _merge_directory_missing_only(source, target_root)
        else:
            skipped.append(str(source))

    result = {
        "app_name": app_name,
        "dry_run": dry_run,
        "target_root": str(target_root),
        "migration_marker": str(marker),
        "marker_exists": marker_exists,
        "plan": plan,
        "copied_entries": copied,
        "merged_entries": merged,
        "skipped_entries": skipped,
    }
    if not dry_run and not marker_exists:
        _write_app_runtime_marker(
            target_root,
            {
                "app_name": app_name,
                "target_root": str(target_root),
                "copied_entries": copied,
                "merged_entries": merged,
                "skipped_entries": skipped,
            },
        )
    return result


def migrate_library_tracker_runtime_data(*, dry_run: bool = True) -> dict[str, Any]:
    return _migrate_app_runtime_tree(
        "library_tracker",
        repo_legacy_root=_workspace_root() / "library_tracker" / "data",
        dry_run=dry_run,
    )


def migrate_property_runtime_data(*, dry_run: bool = True) -> dict[str, Any]:
    return _migrate_app_runtime_tree(
        "property",
        repo_legacy_root=_workspace_root() / "property" / "data",
        dry_run=dry_run,
    )


def migrate_journey_runtime_data(*, dry_run: bool = True) -> dict[str, Any]:
    return _migrate_app_runtime_tree(
        "journey",
        repo_legacy_root=_workspace_root() / "journey" / "data",
        dry_run=dry_run,
    )


def ensure_runtime_data_migrated() -> dict[str, Any]:
    global _RUNTIME_MIGRATION_DONE  # noqa: PLW0603
    with _RUNTIME_MIGRATION_LOCK:
        if _RUNTIME_MIGRATION_DONE:
            return {"ok": True, "already_ran": True}
        payload = {
            "core_service": migrate_core_runtime_data(dry_run=False),
            "nav_dashboard": migrate_nav_dashboard_runtime_data(dry_run=False),
            "library_tracker": migrate_library_tracker_runtime_data(dry_run=False),
            "property": migrate_property_runtime_data(dry_run=False),
            "journey": migrate_journey_runtime_data(dry_run=False),
        }
        _RUNTIME_MIGRATION_DONE = True
        return {"ok": True, "already_ran": False, "payload": payload}


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate runtime state from legacy repo-local paths into canonical app-data roots.")
    parser.add_argument("--apply", action="store_true", help="Perform the migration instead of printing the dry-run plan.")
    args = parser.parse_args()

    dry_run = not args.apply
    payload = {
        "core_service": migrate_core_runtime_data(dry_run=dry_run),
        "nav_dashboard": migrate_nav_dashboard_runtime_data(dry_run=dry_run),
        "library_tracker": migrate_library_tracker_runtime_data(dry_run=dry_run),
        "property": migrate_property_runtime_data(dry_run=dry_run),
        "journey": migrate_journey_runtime_data(dry_run=dry_run),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()