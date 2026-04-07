from __future__ import annotations

import os
from pathlib import Path

from playwright.sync_api import sync_playwright

WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
APP_JS = WORKSPACE_ROOT / "nav_dashboard" / "web" / "static" / "app.js"


def _resolve_chromium_executable() -> str | None:
    explicit = str(os.getenv("PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH", "") or "").strip()
    if explicit and Path(explicit).exists():
        return explicit
    fallback = Path.home() / "AppData" / "Local" / "ms-playwright" / "chromium-1208" / "chrome-win64" / "chrome.exe"
    if fallback.exists():
        return str(fallback)
    return None


def _build_launch_kwargs() -> dict[str, object]:
    headless_env = str(os.getenv("PLAYWRIGHT_HEADLESS", "1") or "1").strip().lower()
    launch_kwargs: dict[str, object] = {"headless": headless_env not in {"0", "false", "no"}}
    executable_path = _resolve_chromium_executable()
    if executable_path:
        launch_kwargs["executable_path"] = executable_path
    return launch_kwargs


def main() -> int:
    src = APP_JS.read_text(encoding="utf-8")
    with sync_playwright() as p:
        browser = p.chromium.launch(**_build_launch_kwargs())
        page = browser.new_page()
        page.goto("about:blank")
        try:
            page.add_script_tag(content=src)
            print("PARSE_OK")
            return 0
        except Exception as exc:
            print(type(exc).__name__)
            print(str(exc))
            return 1
        finally:
            browser.close()


if __name__ == "__main__":
    raise SystemExit(main())
