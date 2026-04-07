from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Callable

from playwright.sync_api import sync_playwright

BASES = [
    "http://127.0.0.1:8092",
    "http://localhost:8092",
    "http://127.0.0.1:8000",
]

RESULT_PATH = Path(__file__).resolve().with_name("ui_smoke_playwright_result.json")
EXITCODE_PATH = Path(__file__).resolve().with_name("ui_smoke_exitcode.txt")
CHROMIUM_EXE = Path.home() / "AppData" / "Local" / "ms-playwright" / "chromium-1208" / "chrome-win64" / "chrome.exe"


def _check_active(page, panel_id: str) -> tuple[bool, str]:
    ok = page.locator(panel_id).evaluate("el => el.classList.contains('active')")
    return bool(ok), f"{panel_id} active"


def _check_ticket_date_defaults(page) -> tuple[bool, str]:
    created_from = page.locator("#tickets-created-from").input_value(timeout=8000)
    created_to = page.locator("#tickets-created-to").input_value(timeout=8000)
    expected_to = date.today().isoformat()
    expected_from = (date.today() - timedelta(days=6)).isoformat()
    ok = created_from == expected_from and created_to == expected_to
    detail = json.dumps(
        {
            "created_from": created_from,
            "created_to": created_to,
            "expected_from": expected_from,
            "expected_to": expected_to,
        },
        ensure_ascii=False,
    )
    return ok, detail


def _persist_exit_code(code: int) -> None:
    EXITCODE_PATH.write_text(str(int(code)), encoding="utf-8")


def _record_and_return(result: dict[str, object], code: int) -> int:
    RESULT_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    _persist_exit_code(code)
    print(RESULT_PATH)
    return code


def _check_ticket_list_or_empty(page) -> tuple[bool, str]:
    item = page.locator("#tickets-list .ticket-list-item").first
    if item.count() > 0:
        item.click(timeout=8000)
        page.wait_for_timeout(400)
        return True, "clicked first ticket item"
    empty_state = page.locator("#tickets-list .ticket-empty-state").first
    if empty_state.count() > 0:
        detail = empty_state.inner_text(timeout=4000).strip()
        if detail:
            return True, detail
    return False, "neither ticket item nor empty state found"


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
            return _record_and_return(payload, 2)

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
        result["steps"].append(
            {
                "name": "tickets-date-defaults",
                "selector": "#tickets-created-from,#tickets-created-to",
                "clicked": False,
                "ok": False,
                "detail": "",
            }
        )
        try:
            ok, detail = _check_ticket_date_defaults(page)
            result["steps"][-1]["ok"] = bool(ok)
            result["steps"][-1]["detail"] = detail
        except Exception as exc:
            result["steps"][-1]["detail"] = str(exc)
        run_step("tab-benchmark", "#tab-benchmark", lambda: _check_active(page, "#panel-benchmark"))

        run_step("tab-agent-again", "#tab-agent", lambda: _check_active(page, "#panel-agent"))
        run_step("ask-local", "#qa-ask-local")
        run_step("ask-hybrid", "#qa-ask")

        run_step("tab-tickets-again", "#tab-tickets", lambda: _check_active(page, "#panel-tickets"))
        result["steps"].append(
            {
                "name": "ticket-first-item",
                "selector": "#tickets-list .ticket-list-item,#tickets-list .ticket-empty-state",
                "clicked": False,
                "ok": False,
                "detail": "",
            }
        )
        try:
            ok, detail = _check_ticket_list_or_empty(page)
            result["steps"][-1]["ok"] = bool(ok)
            result["steps"][-1]["detail"] = detail
            result["steps"][-1]["clicked"] = detail == "clicked first ticket item"
        except Exception as exc:
            result["steps"][-1]["detail"] = str(exc)

        run_step("tab-benchmark-again", "#tab-benchmark", lambda: _check_active(page, "#panel-benchmark"))
        run_step("benchmark-run", "#bm-run-btn")
        run_step("benchmark-router-cls", "#bm-router-cls-run")
        run_step("benchmark-unit-tests", "#bm-unit-tests-run")

        browser.close()

    failed = [s for s in result["steps"] if not s.get("ok")]
    if failed or result["page_errors"]:
        return _record_and_return(result, 1)
    return _record_and_return(result, 0)


if __name__ == "__main__":
    raise SystemExit(main())
