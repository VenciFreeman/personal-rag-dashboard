from __future__ import annotations

import json
from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path(__file__).with_suffix(".json")
CHROMIUM_EXE = Path.home() / "AppData" / "Local" / "ms-playwright" / "chromium-1208" / "chrome-win64" / "chrome.exe"


def main() -> int:
    data: dict[str, object] = {"page_errors": [], "console": []}
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, executable_path=str(CHROMIUM_EXE) if CHROMIUM_EXE.exists() else None)
        page = browser.new_page()

        def on_pageerror(err):
            payload = {
                "type": type(err).__name__,
                "message": getattr(err, "message", str(err)),
                "stack": getattr(err, "stack", None),
                "name": getattr(err, "name", None),
            }
            data["page_errors"].append(payload)

        def on_console(msg):
            values = []
            try:
                for arg in msg.args:
                    values.append(arg.json_value())
            except Exception:
                values = []
            data["console"].append({"type": msg.type, "text": msg.text, "args": values})

        page.on("pageerror", on_pageerror)
        page.on("console", on_console)
        page.goto("http://127.0.0.1:8092/", wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(1500)
        browser.close()

    OUT.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(OUT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
