"""FastAPI Web UI launcher (Preview + RAG).

Double-click friendly behavior on Windows:
- If the target port is occupied, terminate listeners on that port.
- Auto-open the web page in the default browser once the server is reachable.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
import webbrowser
from pathlib import Path


def _prepend_workspace_root() -> None:
    workspace_root = Path(__file__).resolve().parent.parent
    workspace_root_str = str(workspace_root)
    if workspace_root_str not in sys.path:
        sys.path.insert(0, workspace_root_str)


def _load_workspace_env_local() -> None:
    """Load root env.local.ps1 and override stale inherited env vars for launcher use."""
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
    """Ensure double-click always runs with workspace venv Python."""
    root = Path(__file__).resolve().parent
    venv_candidates = [
        root.parent / ".venv" / "Scripts" / "python.exe",
        root / ".venv" / "Scripts" / "python.exe",
    ]
    venv_python = next((p for p in venv_candidates if p.exists()), None)
    if venv_python is None:
        return

    try:
        current = Path(sys.executable).resolve()
        target = Path(venv_python).resolve()
    except Exception:
        return

    if str(current).lower() == str(target).lower():
        return

    # Replace current process so we do not lose state when launched by double-click.
    os.execv(str(target), [str(target), str(Path(__file__).resolve()), *sys.argv[1:]])


_maybe_reexec_into_venv()
_prepend_workspace_root()
_load_workspace_env_local()

from web.config import HOST, PORT
from web.main import run


def _list_listening_pids_on_port(port: int) -> set[int]:
    """Return all PIDs listening on the given TCP port (Windows netstat)."""
    try:
        result = subprocess.run(
            ["netstat", "-ano", "-p", "tcp"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
    except Exception:
        return set()

    pids: set[int] = set()
    pattern = re.compile(r"^\s*TCP\s+\S+:(\d+)\s+\S+\s+LISTENING\s+(\d+)\s*$", re.IGNORECASE)
    for line in (result.stdout or "").splitlines():
        match = pattern.match(line)
        if not match:
            continue
        line_port = int(match.group(1))
        if line_port != int(port):
            continue
        try:
            pids.add(int(match.group(2)))
        except Exception:
            continue
    return pids


def _terminate_pid(pid: int) -> bool:
    if pid <= 0:
        return False
    if pid == os.getpid():
        return False
    try:
        result = subprocess.run(
            ["taskkill", "/PID", str(pid), "/F", "/T"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        return result.returncode == 0
    except Exception:
        return False


def _free_port_if_occupied(port: int) -> None:
    pids = _list_listening_pids_on_port(port)
    if not pids:
        return

    print(f"[launch_web] Port {port} is occupied. Attempting to terminate listeners: {sorted(pids)}")
    for pid in sorted(pids):
        ok = _terminate_pid(pid)
        print(f"[launch_web] taskkill PID {pid}: {'ok' if ok else 'failed'}")

    # Give OS a short moment to release socket state.
    time.sleep(0.6)


def _wait_server_then_open_browser(host: str, port: int) -> None:
    open_host = "127.0.0.1" if host in {"0.0.0.0", "::"} else host
    url = f"http://{open_host}:{port}/"
    healthz = f"http://{open_host}:{port}/healthz"

    def _should_open_browser() -> bool:
        if os.getenv("PERSONAL_AI_STACK_DISABLE_BROWSER_OPEN", "0").strip().lower() in {"1", "true", "yes", "on"}:
            return False
        lock_path = Path(tempfile.gettempdir()) / "personal_ai_stack_browser_open.lock"
        now_ts = time.time()
        try:
            if lock_path.exists():
                prev_ts = float(lock_path.read_text(encoding="utf-8").strip() or "0")
                if now_ts - prev_ts < 8.0:
                    return False
        except Exception:
            pass
        try:
            lock_path.write_text(str(now_ts), encoding="utf-8")
        except Exception:
            pass
        return True

    def _open_url(target_url: str) -> bool:
        if not _should_open_browser():
            print(f"[launch_web] Browser auto-open skipped (throttled): {target_url}")
            return False
        # On Windows, prefer os.startfile - it's more reliable and won't double-open.
        if sys.platform == "win32":
            try:
                os.startfile(target_url)  # type: ignore[attr-defined]
                return True
            except Exception:
                pass
        # Fallback to webbrowser for other platforms or if startfile fails.
        try:
            webbrowser.open(target_url, new=2)
        except Exception:
            pass
        return True

    local_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    deadline = time.time() + 20.0
    while time.time() < deadline:
        try:
            with local_opener.open(healthz, timeout=1.2) as resp:
                if resp.status == 200:
                    if _open_url(url):
                        print(f"[launch_web] Opened browser: {url}")
                    return
        except Exception:
            time.sleep(0.25)

    # Fallback: open anyway so user can see eventual startup logs/errors.
    _open_url(url)
    print(f"[launch_web] Timed out waiting for /healthz, opened browser anyway: {url}")


def main() -> None:
    _free_port_if_occupied(PORT)
    opener = threading.Thread(target=_wait_server_then_open_browser, args=(HOST, PORT), daemon=True)
    opener.start()
    run()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
