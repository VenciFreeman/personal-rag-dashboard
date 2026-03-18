from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Callable

from playwright.sync_api import sync_playwright

BASES = [
    "http://127.0.0.1:8092",
    "http://localhost:8092",
    "http://127.0.0.1:8000",
]

RESULT_PATH = Path(__file__).resolve().with_name("ui_smoke_playwright_result.json")
CHROMIUM_EXE = Path.home() / "AppData" / "Local" / "ms-playwright" / "chromium-1208" / "chrome-win64" / "chrome.exe"


def _check_active(page, panel_id: str) -> tuple[bool, str]:
    ok = page.locator(panel_id).evaluate("el => el.classList.contains('active')")
    return bool(ok), f"{panel_id} active"


def main() -> int:
    result: dict[str, object] = {
        "base": None,
        "steps": [],
        "page_errors": [],
        "console_errors": [],
    }

    with sync_playwright() as p:
        launch_kwargs = {"headless": False}
        if CHROMIUM_EXE.exists():
            launch_kwargs["executable_path"] = str(CHROMIUM_EXE)
        browser = p.chromium.launch(**launch_kwargs)
        page = browser.new_page()

        page.on("pageerror", lambda e: result["page_errors"].append(str(e)))

        def on_console(msg):
            if msg.type in {"error", "warning"}:
                result["console_errors"].append(f"{msg.type}: {msg.text}")

        page.on("console", on_console)

        opened = False
        for base in BASES:
            try:
                resp = page.goto(base + "/", wait_until="domcontentloaded", timeout=15000)
                if resp and resp.ok:
                    result["base"] = base
                    opened = True
                    break
            except Exception:
                continue
        if not opened:
            browser.close()
            payload = {"error": "cannot_open_dashboard", **result}
            RESULT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            print(RESULT_PATH)
            return 2

        page.wait_for_timeout(1200)

        def run_step(name: str, selector: str, check: Callable[[], tuple[bool, str]] | None = None):
            step = {"name": name, "selector": selector, "clicked": False, "ok": False, "detail": ""}
            try:
                loc = page.locator(selector).first
                if loc.count() == 0:
                    step["detail"] = "selector not found"
                    result["steps"].append(step)
                    return
                loc.click(timeout=8000)
                page.wait_for_timeout(400)
                step["clicked"] = True
                if check is None:
                    step["ok"] = True
                else:
                    ok, detail = check()
                    step["ok"] = bool(ok)
                    step["detail"] = detail
            except Exception as exc:
                step["detail"] = str(exc)
            result["steps"].append(step)

        run_step("tab-agent", "#tab-agent", lambda: _check_active(page, "#panel-agent"))
        run_step("tab-dashboard", "#tab-dashboard", lambda: _check_active(page, "#panel-dashboard"))
        run_step("tab-tickets", "#tab-tickets", lambda: _check_active(page, "#panel-tickets"))
        run_step("tab-benchmark", "#tab-benchmark", lambda: _check_active(page, "#panel-benchmark"))

        run_step("tab-agent-again", "#tab-agent", lambda: _check_active(page, "#panel-agent"))
        run_step("ask-local", "#qa-ask-local")
        run_step("ask-hybrid", "#qa-ask")

        run_step("tab-tickets-again", "#tab-tickets", lambda: _check_active(page, "#panel-tickets"))
        run_step("ticket-first-item", "#tickets-list .ticket-list-item")

        run_step("tab-benchmark-again", "#tab-benchmark", lambda: _check_active(page, "#panel-benchmark"))
        run_step("benchmark-run", "#bm-run-btn")
        run_step("benchmark-router-cls", "#bm-router-cls-run")
        run_step("benchmark-unit-tests", "#bm-unit-tests-run")

        browser.close()

    RESULT_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(RESULT_PATH)

    failed = [s for s in result["steps"] if not s.get("ok")]
    if failed or result["page_errors"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
