from __future__ import annotations

from pathlib import Path

from playwright.sync_api import sync_playwright

APP_JS = Path(r"c:\Users\Vincent\Desktop\personal-ai-stack\nav_dashboard\web\static\app.js")
CHROMIUM_EXE = Path.home() / "AppData" / "Local" / "ms-playwright" / "chromium-1208" / "chrome-win64" / "chrome.exe"


def main() -> int:
    src = APP_JS.read_text(encoding="utf-8")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, executable_path=str(CHROMIUM_EXE) if CHROMIUM_EXE.exists() else None)
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
