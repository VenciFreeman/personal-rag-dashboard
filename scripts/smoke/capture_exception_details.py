from __future__ import annotations

import json
from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path(__file__).with_suffix(".json")
CHROMIUM_EXE = Path.home() / "AppData" / "Local" / "ms-playwright" / "chromium-1208" / "chrome-win64" / "chrome.exe"


def main() -> int:
    events: list[dict[str, object]] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, executable_path=str(CHROMIUM_EXE) if CHROMIUM_EXE.exists() else None)
        page = browser.new_page()
        session = page.context.new_cdp_session(page)
        session.send("Runtime.enable")

        def on_exception(params):
            events.append(params)

        session.on("Runtime.exceptionThrown", on_exception)
        page.goto("http://127.0.0.1:8092/", wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(2000)
        browser.close()

    OUT.write_text(json.dumps(events, ensure_ascii=False, indent=2), encoding="utf-8")
    print(OUT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
