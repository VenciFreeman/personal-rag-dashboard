from __future__ import annotations

import argparse
import platform
import subprocess
import sys
from pathlib import Path


def run(cmd: list[str], dry_run: bool = False) -> None:
    print("[cmd]", " ".join(cmd))
    if dry_run:
        return
    subprocess.run(cmd, check=True)


def venv_python(venv_dir: Path) -> Path:
    if platform.system().lower().startswith("win"):
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def main() -> None:
    parser = argparse.ArgumentParser(description="One-click setup for library_tracker")
    parser.add_argument("--dry-run", action="store_true", help="Print commands without executing")
    parser.add_argument("--python", default=sys.executable, help="Base Python executable used to create venv")
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    workspace_root = project_root.parent

    venv_dir = workspace_root / ".venv"
    requirements_path = project_root / "requirements.txt"
    structured_dir = project_root / "data" / "structured"

    print("[info] project_root:", project_root)
    print("[info] venv_dir:", venv_dir)
    print("[info] requirements:", requirements_path)

    if not requirements_path.exists():
        raise FileNotFoundError(f"requirements.txt not found: {requirements_path}")

    structured_dir.mkdir(parents=True, exist_ok=True)

    if not venv_dir.exists():
        run([args.python, "-m", "venv", str(venv_dir)], dry_run=args.dry_run)

    py = venv_python(venv_dir)
    run([str(py), "-m", "pip", "install", "--upgrade", "pip"], dry_run=args.dry_run)
    run([str(py), "-m", "pip", "install", "-r", str(requirements_path)], dry_run=args.dry_run)

    print("[done] setup completed")


if __name__ == "__main__":
    main()
