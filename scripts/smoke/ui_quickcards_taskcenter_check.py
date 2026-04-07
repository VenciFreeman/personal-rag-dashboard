from __future__ import annotations

import json
from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path(__file__).with_suffix(".json")
CHROMIUM_EXE = Path.home() / "AppData" / "Local" / "ms-playwright" / "chromium-1208" / "chrome-win64" / "chrome.exe"


def main() -> int:
    result: dict[str, object] = {"quick_card": {}, "task_center_mobile": {}, "page_errors": [], "console_errors": []}
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, executable_path=str(CHROMIUM_EXE) if CHROMIUM_EXE.exists() else None)
        page = browser.new_page(viewport={"width": 390, "height": 844}, is_mobile=True)
        page.on("pageerror", lambda e: result["page_errors"].append(str(e)))
        page.on("console", lambda msg: result["console_errors"].append(f"{msg.type}: {msg.text}") if msg.type in {"error", "warning"} else None)
        page.goto("http://127.0.0.1:8092/", wait_until="networkidle", timeout=30000)

        card = page.locator("#custom-card-grid .custom-square-card").first
        card.dispatch_event("pointerdown", {"pointerType": "touch", "isPrimary": True, "button": 0})
        page.wait_for_timeout(750)
        card.dispatch_event("pointerup", {"pointerType": "touch", "isPrimary": True, "button": 0})
        page.wait_for_timeout(250)
        modal_visible = page.locator("#custom-card-modal").evaluate("el => !el.classList.contains('hidden')")
        result["quick_card"] = {"modal_visible_after_longpress": bool(modal_visible)}
        if modal_visible:
            page.locator("#custom-card-cancel").click()
            page.wait_for_timeout(200)

        page.locator("#tab-dashboard").click()
        page.wait_for_timeout(1200)
        page.locator("#dashboard-jobs-filter").select_option("all")
        page.wait_for_timeout(500)

        job_card = page.locator("#dashboard-jobs-list .dashboard-job-card").first
        card_count = page.locator("#dashboard-jobs-list .dashboard-job-card").count()
        expanded_visible = False
        log_box_visible = False
        actions_visible = False
        if card_count:
            job_card.click()
            page.wait_for_timeout(500)
            expanded = page.locator("#dashboard-jobs-list .dashboard-job-expanded").first
            expanded_visible = expanded.count() > 0 and expanded.is_visible()
            if expanded_visible:
                log_box = expanded.locator(".dashboard-job-log-window").first
                log_box_visible = log_box.count() > 0 and log_box.is_visible()
                actions = expanded.locator(".dashboard-job-actions").first
                actions_visible = actions.count() > 0 and actions.is_visible()
        result["task_center_mobile"] = {
            "job_cards": card_count,
            "expanded_visible": expanded_visible,
            "log_box_visible": log_box_visible,
            "actions_visible": actions_visible,
        }
        browser.close()

    OUT.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(OUT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
