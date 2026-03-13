from __future__ import annotations

import argparse
import os
import platform
import subprocess
import sys
from pathlib import Path


def _run(cmd: list[str], dry_run: bool = False) -> None:
    print("[cmd]", " ".join(cmd))
    if dry_run:
        return
    subprocess.run(cmd, check=True)


def _resolve_layout(project_root: Path) -> tuple[Path, Path, Path, Path]:
    workspace_root = project_root.parent
    has_core_service = (workspace_root / "core_service").exists()

    if has_core_service:
        venv_dir = workspace_root / ".venv"
        data_root = workspace_root / "core_service" / "data"
    else:
        venv_dir = project_root / ".venv"
        data_root = project_root / "data"

    requirements_path = project_root / "requirements.txt"
    local_models_root = data_root / "local_models"
    hf_cache_root = data_root / "hf_cache"
    vector_db_root = data_root / "vector_db"

    return venv_dir, requirements_path, local_models_root, hf_cache_root, vector_db_root


def _venv_python(venv_dir: Path) -> Path:
    if platform.system().lower().startswith("win"):
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def _download_model(
    python_bin: Path,
    model_id: str,
    local_models_root: Path,
    hf_cache_root: Path,
    dry_run: bool = False,
) -> None:
    code = (
        "from sentence_transformers import SentenceTransformer; "
        f"SentenceTransformer({model_id!r}, cache_folder={str(hf_cache_root)!r}); "
        f"print('model ready:', {model_id!r}, 'cache:', {str(hf_cache_root)!r})"
    )

    env = os.environ.copy()
    env["HF_HOME"] = str(hf_cache_root)

    cmd = [str(python_bin), "-c", code]
    print("[cmd]", " ".join(cmd))
    if dry_run:
        return
    subprocess.run(cmd, check=True, env=env)

    # Keep a stable target root for project conventions.
    local_models_root.mkdir(parents=True, exist_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="One-click setup for ai_conversations_summary")
    parser.add_argument("--dry-run", action="store_true", help="Print commands without executing")
    parser.add_argument("--skip-model", action="store_true", help="Skip embedding model download")
    parser.add_argument("--model-id", default="BAAI/bge-base-zh-v1.5", help="Embedding model id to pre-download")
    parser.add_argument("--python", default=sys.executable, help="Base Python executable used to create venv")
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent

    venv_dir, requirements_path, local_models_root, hf_cache_root, vector_db_root = _resolve_layout(project_root)

    print("[info] project_root:", project_root)
    print("[info] venv_dir:", venv_dir)
    print("[info] requirements:", requirements_path)
    print("[info] local_models_root:", local_models_root)
    print("[info] hf_cache_root:", hf_cache_root)
    print("[info] vector_db_root:", vector_db_root)

    if not requirements_path.exists():
        raise FileNotFoundError(f"requirements.txt not found: {requirements_path}")

    local_models_root.mkdir(parents=True, exist_ok=True)
    hf_cache_root.mkdir(parents=True, exist_ok=True)
    vector_db_root.mkdir(parents=True, exist_ok=True)

    if not venv_dir.exists():
        _run([args.python, "-m", "venv", str(venv_dir)], dry_run=args.dry_run)

    python_bin = _venv_python(venv_dir)
    _run([str(python_bin), "-m", "pip", "install", "--upgrade", "pip"], dry_run=args.dry_run)
    _run([str(python_bin), "-m", "pip", "install", "-r", str(requirements_path)], dry_run=args.dry_run)

    if not args.skip_model:
        _download_model(
            python_bin=python_bin,
            model_id=args.model_id,
            local_models_root=local_models_root,
            hf_cache_root=hf_cache_root,
            dry_run=args.dry_run,
        )

    print("[done] setup completed")


if __name__ == "__main__":
    main()
