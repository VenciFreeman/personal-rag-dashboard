"""Top-level GUI launcher.

Responsibilities:
- Prefer `pythonw.exe` on Windows to avoid console popups.
- Add `scripts/` to `sys.path`.
- Delegate real app startup to `scripts/gui_launcher.py`.
"""

from pathlib import Path
import os
import runpy
import subprocess
import sys


def _relaunch_with_pythonw_on_windows(script_path: Path) -> None:
	if sys.platform != "win32":
		return
	if os.getenv("AI_SUMMARY_GUI_PYW_RELAUNCHED") == "1":
		return

	current_exe = Path(sys.executable or "")
	root_dir = script_path.resolve().parent
	venv_candidates = [
		root_dir.parent / ".venv" / "Scripts" / "pythonw.exe",
		root_dir / ".venv" / "Scripts" / "pythonw.exe",
	]

	pythonw = next((p for p in venv_candidates if p.exists()), None)
	if pythonw is not None:
		pass
	else:
		if current_exe.name.lower() != "python.exe":
			return
		pythonw = current_exe.with_name("pythonw.exe")
	if not pythonw.exists():
		return

	env = os.environ.copy()
	env["AI_SUMMARY_GUI_PYW_RELAUNCHED"] = "1"
	flags = 0
	if hasattr(subprocess, "DETACHED_PROCESS"):
		flags |= subprocess.DETACHED_PROCESS
	if hasattr(subprocess, "CREATE_NEW_PROCESS_GROUP"):
		flags |= subprocess.CREATE_NEW_PROCESS_GROUP

	try:
		subprocess.Popen(
			[str(pythonw), str(script_path)],
			env=env,
			creationflags=flags,
			close_fds=True,
		)
		raise SystemExit(0)
	except Exception:
		# Fallback to current process if relaunch fails.
		return


def _hide_console_on_windows() -> None:
	if sys.platform != "win32":
		return
	try:
		import ctypes
		from ctypes import wintypes

		kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
		user32 = ctypes.WinDLL("user32", use_last_error=True)
		kernel32.GetConsoleWindow.restype = wintypes.HWND
		SW_HIDE = 0
		hwnd = kernel32.GetConsoleWindow()
		if hwnd:
			user32.ShowWindow(hwnd, SW_HIDE)
	except Exception:
		# Keep launcher functional even if WinAPI is unavailable.
		return


def main() -> None:
	# Keep this file thin; GUI implementation lives in scripts/gui_launcher.py.
	_relaunch_with_pythonw_on_windows(Path(__file__).resolve())
	_hide_console_on_windows()
	root_dir = Path(__file__).resolve().parent
	scripts_dir = root_dir / "scripts"
	if str(scripts_dir) not in sys.path:
		sys.path.insert(0, str(scripts_dir))

	runpy.run_path(str(scripts_dir / "gui_launcher.py"), run_name="__main__")


if __name__ == "__main__":
	main()
