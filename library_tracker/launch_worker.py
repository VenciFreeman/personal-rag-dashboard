from __future__ import annotations

import os
import sys
from pathlib import Path


def _prepend_workspace_root() -> None:
    workspace_root = Path(__file__).resolve().parent.parent
    workspace_root_str = str(workspace_root)
    if workspace_root_str not in sys.path:
        sys.path.insert(0, workspace_root_str)


def _load_workspace_env_local() -> None:
    root = Path(__file__).resolve().parent.parent
    env_file = root / "env.local.ps1"
    if not env_file.exists():
        return
    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if not line.lower().startswith("$env:") or "=" not in line:
            continue
        left, right = line.split("=", 1)
        key = left.split(":", 1)[1].strip()
        value = right.strip()
        if not key:
            continue
        if value.startswith(("\"", "'")) and value.endswith(("\"", "'")) and len(value) >= 2:
            value = value[1:-1]
        os.environ[key] = value


def _maybe_reexec_into_venv() -> None:
    root = Path(__file__).resolve().parent
    venv_candidates = [
        root.parent / ".venv" / "Scripts" / "python.exe",
        root / ".venv" / "Scripts" / "python.exe",
    ]
    venv_python = next((path for path in venv_candidates if path.exists()), None)
    if venv_python is None:
        return
    try:
        current = Path(sys.executable).resolve()
        target = Path(venv_python).resolve()
    except Exception:
        return
    if str(current).lower() == str(target).lower():
        return
    os.execv(str(target), [str(target), str(Path(__file__).resolve()), *sys.argv[1:]])


_maybe_reexec_into_venv()
_prepend_workspace_root()
_load_workspace_env_local()


def _migrate_runtime_data_if_needed() -> None:
    try:
        from core_service.runtime_migration_cli import ensure_runtime_data_migrated

        ensure_runtime_data_migrated()
    except Exception:
        return


_migrate_runtime_data_if_needed()

from web.worker import run


if __name__ == "__main__":
    run()