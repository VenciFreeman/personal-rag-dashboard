"""Shared GUI helpers used by gui_launcher.

This module isolates reusable utility code so the main launcher file focuses on
UI wiring and workflow orchestration.
"""

from __future__ import annotations

import ctypes
import os
import subprocess
import sys
import tkinter as tk
from pathlib import Path


def env_float(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def env_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def relaunch_with_pythonw_on_windows(script_path: Path) -> None:
    if sys.platform != "win32":
        return
    if os.getenv("AI_SUMMARY_GUI_PYW_RELAUNCHED") == "1":
        return

    current_exe = Path(sys.executable or "")
    workspace_root = script_path.resolve().parent.parent
    venv_candidates = [
        workspace_root.parent / ".venv" / "Scripts" / "pythonw.exe",
        workspace_root / ".venv" / "Scripts" / "pythonw.exe",
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


def hide_console_on_windows() -> None:
    if sys.platform != "win32":
        return
    try:
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        user32 = ctypes.WinDLL("user32", use_last_error=True)
        kernel32.GetConsoleWindow.restype = ctypes.c_void_p
        sw_hide = 0
        hwnd = kernel32.GetConsoleWindow()
        if hwnd:
            user32.ShowWindow(hwnd, sw_hide)
    except Exception:
        # Keep app launch resilient if WinAPI calls are unavailable.
        return


def no_window_creationflags() -> int:
    if sys.platform == "win32":
        return subprocess.CREATE_NO_WINDOW
    return 0


class HoverToolTip:
    def __init__(self, widget: tk.Widget, text: str, bg: str, fg: str) -> None:
        self.widget = widget
        self.text = text
        self.bg = bg
        self.fg = fg
        self.tip_window: tk.Toplevel | None = None
        self.after_id: str | None = None

        self.widget.bind("<Enter>", self._on_enter, add="+")
        self.widget.bind("<Leave>", self._on_leave, add="+")
        self.widget.bind("<ButtonPress>", self._on_leave, add="+")

    def _on_enter(self, _event: tk.Event) -> None:
        self.after_id = self.widget.after(350, self._show)

    def _on_leave(self, _event: tk.Event) -> None:
        if self.after_id is not None:
            self.widget.after_cancel(self.after_id)
            self.after_id = None
        self._hide()

    def _show(self) -> None:
        if self.tip_window is not None:
            return

        x = self.widget.winfo_pointerx() + 14
        y = self.widget.winfo_pointery() + 12

        self.tip_window = tk.Toplevel(self.widget)
        self.tip_window.wm_overrideredirect(True)
        self.tip_window.wm_geometry(f"+{x}+{y}")

        label = tk.Label(
            self.tip_window,
            text=self.text,
            justify=tk.LEFT,
            bg=self.bg,
            fg=self.fg,
            relief=tk.SOLID,
            borderwidth=1,
            padx=8,
            pady=4,
            font=("Microsoft YaHei UI", 9),
        )
        label.pack()

    def _hide(self) -> None:
        if self.tip_window is not None:
            self.tip_window.destroy()
            self.tip_window = None
