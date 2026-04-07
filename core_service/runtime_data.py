from __future__ import annotations

import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


APP_DATA_ENV_VAR = "PERSONAL_AI_STACK_APP_DATA_DIR"
RUNTIME_DATA_ENV_VAR = "CORE_RUNTIME_DATA_DIR"
MIGRATION_MARKER_FILE = ".core_runtime_data_migrated.json"
KNOWN_RUNTIME_DATA_ENTRIES = (
    "lan_auth.sqlite3",
    "vector_db",
    "local_models",
    "hf_cache",
)
BACKUP_SNAPSHOTS_DIRNAME = "backup_snapshots"


def workspace_root() -> Path:
    return Path(__file__).resolve().parent.parent


def legacy_app_data_root() -> Path:
    explicit = (os.getenv(APP_DATA_ENV_VAR) or "").strip()
    if explicit:
        return Path(explicit)
    if os.name == "nt":
        base = (os.getenv("LOCALAPPDATA") or "").strip()
        if base:
            return Path(base) / "PersonalAIStack"
    if sys_platform() == "darwin":
        return Path.home() / "Library" / "Application Support" / "PersonalAIStack"
    xdg = (os.getenv("XDG_STATE_HOME") or "").strip()
    if xdg:
        return Path(xdg) / "personal-ai-stack"
    return Path.home() / ".local" / "share" / "personal-ai-stack"


def default_app_data_root() -> Path:
    explicit = (os.getenv(APP_DATA_ENV_VAR) or "").strip()
    if explicit:
        return Path(explicit)
    return workspace_root() / "data"


def sys_platform() -> str:
    try:
        import sys

        return sys.platform
    except Exception:
        return ""


def app_runtime_root(app_name: str) -> Path:
    normalized = str(app_name or "").strip().replace("\\", "/").strip("/")
    if not normalized:
        raise ValueError("app_name is required")
    return default_app_data_root() / normalized


def legacy_app_runtime_root(app_name: str) -> Path:
    normalized = str(app_name or "").strip().replace("\\", "/").strip("/")
    if not normalized:
        raise ValueError("app_name is required")
    return legacy_app_data_root() / normalized


def legacy_core_runtime_data_root() -> Path:
    return workspace_root() / "core_service" / "data"


def core_runtime_data_root() -> Path:
    explicit = (os.getenv(RUNTIME_DATA_ENV_VAR) or "").strip()
    if explicit:
        return Path(explicit)
    return app_runtime_root("core_service")


def preferred_core_runtime_data_root() -> Path:
    return core_runtime_data_root()


def migration_marker_path() -> Path:
    return core_runtime_data_root() / MIGRATION_MARKER_FILE


def active_core_runtime_data_root() -> Path:
    return core_runtime_data_root()


def iter_core_runtime_data_roots(*, include_legacy: bool | None = None) -> list[Path]:
    use_legacy = bool(include_legacy)
    roots: list[Path] = []
    candidates = [core_runtime_data_root()]
    if use_legacy:
        candidates.append(legacy_app_runtime_root("core_service"))
    for candidate in candidates:
        if candidate not in roots:
            roots.append(candidate)
    return roots


def iter_core_runtime_path_candidates(*parts: str | Path, include_legacy: bool | None = None) -> list[Path]:
    return [root.joinpath(*parts) for root in iter_core_runtime_data_roots(include_legacy=include_legacy)]


def resolve_existing_core_runtime_path(*parts: str | Path, include_legacy: bool | None = None) -> Path | None:
    for candidate in iter_core_runtime_path_candidates(*parts, include_legacy=include_legacy):
        if candidate.exists():
            return candidate
    return None


def core_runtime_path(*parts: str | Path, prefer_existing: bool = False, include_legacy: bool | None = None) -> Path:
    if prefer_existing:
        existing = resolve_existing_core_runtime_path(*parts, include_legacy=include_legacy)
        if existing is not None:
            return existing
    return core_runtime_data_root().joinpath(*parts)


def ensure_core_runtime_dir(*parts: str | Path) -> Path:
    path = core_runtime_path(*parts, prefer_existing=False)
    path.mkdir(parents=True, exist_ok=True)
    return path


def backup_snapshots_root() -> Path:
    return core_runtime_data_root() / BACKUP_SNAPSHOTS_DIRNAME


def ensure_backup_snapshots_dir() -> Path:
    path = backup_snapshots_root()
    path.mkdir(parents=True, exist_ok=True)
    return path


def ensure_core_runtime_parent(*parts: str | Path) -> Path:
    path = core_runtime_path(*parts, prefer_existing=False)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def shared_vector_db_dir() -> Path:
    return core_runtime_path("vector_db")


def local_models_dir() -> Path:
    return core_runtime_path("local_models")


def hf_cache_dir() -> Path:
    return core_runtime_path("hf_cache")


def lan_auth_db_path() -> Path:
    return core_runtime_path("lan_auth.sqlite3")


def _normalize_entries(entries: Iterable[str] | None = None) -> list[str]:
    if entries is None:
        return list(KNOWN_RUNTIME_DATA_ENTRIES)
    normalized: list[str] = []
    seen: set[str] = set()
    for entry in entries:
        text = str(entry or "").strip().replace("\\", "/")
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


def _core_runtime_migration_source_roots() -> list[Path]:
    roots: list[Path] = []
    for candidate in (legacy_app_runtime_root("core_service"), legacy_core_runtime_data_root()):
        if candidate not in roots:
            roots.append(candidate)
    return roots


def _resolve_core_runtime_migration_source(entry: str) -> tuple[Path, bool]:
    for root in _core_runtime_migration_source_roots():
        candidate = root / entry
        if candidate.exists():
            return candidate, True
    fallback_root = _core_runtime_migration_source_roots()[0]
    return fallback_root / entry, False


def plan_core_runtime_data_migration(entries: Iterable[str] | None = None) -> list[dict[str, Any]]:
    target_root = core_runtime_data_root()
    plan: list[dict[str, Any]] = []
    for entry in _normalize_entries(entries):
        source, source_exists = _resolve_core_runtime_migration_source(entry)
        target = target_root / entry
        target_exists = target.exists()
        if source_exists and not target_exists:
            action = "copy"
        elif source_exists and target_exists:
            action = "keep_existing"
        else:
            action = "skip_missing"
        plan.append(
            {
                "entry": entry,
                "source": str(source),
                "target": str(target),
                "source_roots": [str(root) for root in _core_runtime_migration_source_roots()],
                "kind": "dir" if source.is_dir() else "file",
                "source_exists": source_exists,
                "target_exists": target_exists,
                "action": action,
            }
        )
    return plan


def migrate_core_runtime_data(*, dry_run: bool = True, entries: Iterable[str] | None = None, write_marker: bool = True) -> dict[str, Any]:
    target_root = core_runtime_data_root()
    plan = plan_core_runtime_data_migration(entries)
    copied: list[str] = []
    preserved: list[str] = []
    skipped: list[str] = []

    if not dry_run:
        target_root.mkdir(parents=True, exist_ok=True)

    for item in plan:
        source = Path(item["source"])
        target = Path(item["target"])
        action = str(item["action"])
        entry = str(item["entry"])
        if action == "copy":
            copied.append(entry)
            if dry_run:
                continue
            if source.is_dir():
                shutil.copytree(source, target, dirs_exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, target)
            continue
        if action == "keep_existing":
            preserved.append(entry)
            continue
        skipped.append(entry)

    marker_payload = {
        "migrated_at": datetime.now().isoformat(timespec="seconds"),
        "source_roots": [str(root) for root in _core_runtime_migration_source_roots()],
        "target_root": str(target_root),
        "copied_entries": copied,
        "preserved_entries": preserved,
        "skipped_entries": skipped,
    }
    marker = migration_marker_path()
    if not dry_run and write_marker:
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text(json.dumps(marker_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "source_roots": [str(root) for root in _core_runtime_migration_source_roots()],
        "target_root": str(target_root),
        "migration_marker": str(marker),
        "dry_run": dry_run,
        "plan": plan,
        "copied_entries": copied,
        "preserved_entries": preserved,
        "skipped_entries": skipped,
        "marker_payload": marker_payload,
    }