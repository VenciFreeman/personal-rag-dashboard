from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Iterable


RUNTIME_DATA_ENV_VAR = "CORE_RUNTIME_DATA_DIR"
MIGRATION_MARKER_FILE = ".core_runtime_data_migrated.json"
BACKUP_SNAPSHOTS_DIRNAME = "backup_snapshots"


def workspace_root() -> Path:
    return Path(__file__).resolve().parent.parent


def default_app_data_root() -> Path:
    return workspace_root() / "data"


def app_runtime_root(app_name: str) -> Path:
    normalized = str(app_name or "").strip().replace("\\", "/").strip("/")
    if not normalized:
        raise ValueError("app_name is required")
    return default_app_data_root() / normalized


def legacy_app_runtime_root(app_name: str) -> Path:
    return app_runtime_root(app_name)


def legacy_core_runtime_data_root() -> Path:
    return core_runtime_data_root()


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
    return [core_runtime_data_root()]


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
        return []
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
    return []


def _resolve_core_runtime_migration_source(entry: str) -> tuple[Path, bool]:
    for root in _core_runtime_migration_source_roots():
        candidate = root / entry
        if candidate.exists():
            return candidate, True
    return core_runtime_data_root() / entry, False


def plan_core_runtime_data_migration(entries: Iterable[str] | None = None) -> list[dict[str, Any]]:
    return []


def migrate_core_runtime_data(*, dry_run: bool = True, entries: Iterable[str] | None = None, write_marker: bool = True) -> dict[str, Any]:
    target_root = core_runtime_data_root()
    plan = plan_core_runtime_data_migration(entries)

    if not dry_run:
        target_root.mkdir(parents=True, exist_ok=True)

    return {
        "source_roots": [],
        "target_root": str(target_root),
        "migration_marker": "",
        "dry_run": dry_run,
        "plan": plan,
        "copied_entries": [],
        "preserved_entries": [],
        "skipped_entries": [],
        "marker_payload": {},
    }