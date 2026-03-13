"""Python interpreter discovery utilities for local GUI runtime.

These helpers are GUI-agnostic and can be reused by other local scripts.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Callable


def discover_python_from_where(no_window_creationflags: Callable[[], int]) -> list[str]:
    try:
        result = subprocess.run(
            ["where", "python"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=5,
            check=False,
            creationflags=no_window_creationflags(),
        )
    except Exception:
        return []

    if result.returncode != 0:
        return []

    candidates: list[str] = []
    for line in result.stdout.splitlines():
        path_text = line.strip()
        if path_text and Path(path_text).exists():
            candidates.append(path_text)
    return candidates


def build_python_candidates(
    workspace_root: Path,
    script_dir: Path,
    no_window_creationflags: Callable[[], int],
) -> list[str]:
    candidates: list[str] = []

    env_python = (os.getenv("AI_SUMMARY_PYTHON") or "").strip()
    if env_python:
        candidates.append(env_python)

    virtual_env = (os.getenv("VIRTUAL_ENV") or "").strip()
    if virtual_env:
        candidates.append(str(Path(virtual_env) / "Scripts" / "python.exe"))

    for env_name in (".venv", "venv", "env"):
        candidates.append(str(workspace_root.parent / env_name / "Scripts" / "python.exe"))
        candidates.append(str(workspace_root / env_name / "Scripts" / "python.exe"))
        candidates.append(str(script_dir / env_name / "Scripts" / "python.exe"))

    candidates.extend(discover_python_from_where(no_window_creationflags))

    candidates.append("python")

    if sys.executable:
        candidates.append(sys.executable)

    unique: list[str] = []
    seen: set[str] = set()
    for item in candidates:
        key = item.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def python_supports_module(
    python_cmd: str,
    module_name: str,
    no_window_creationflags: Callable[[], int],
) -> bool:
    try:
        probe_code = (
            "import importlib.util,sys; "
            f"sys.exit(0 if importlib.util.find_spec({module_name!r}) else 1)"
        )
        result = subprocess.run(
            [python_cmd, "-c", probe_code],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=15,
            check=False,
            creationflags=no_window_creationflags(),
        )
        return result.returncode == 0
    except Exception:
        return False


def resolve_python_executable(
    workspace_root: Path,
    script_dir: Path,
    no_window_creationflags: Callable[[], int],
    required_module: str | None = None,
) -> str:
    for candidate in build_python_candidates(workspace_root, script_dir, no_window_creationflags):
        if any(sep in candidate for sep in ("\\", "/")) and not Path(candidate).exists():
            continue

        if required_module and not python_supports_module(candidate, required_module, no_window_creationflags):
            continue
        return candidate

    return "python"


def resolve_python_for_module(
    workspace_root: Path,
    script_dir: Path,
    no_window_creationflags: Callable[[], int],
    required_module: str,
) -> str | None:
    for candidate in build_python_candidates(workspace_root, script_dir, no_window_creationflags):
        if any(sep in candidate for sep in ("\\", "/")) and not Path(candidate).exists():
            continue
        if python_supports_module(candidate, required_module, no_window_creationflags):
            return candidate
    return None
