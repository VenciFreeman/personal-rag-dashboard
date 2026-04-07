from __future__ import annotations

import threading
from datetime import date, datetime
from pathlib import Path
from typing import Any

from core_service.reporting import (
    delete_report_files,
    load_json_file,
    load_state_payload,
    open_report_folder,
    save_state_payload,
    summarize_report_rows,
)

from . import library_analysis_core as shared
from . import library_analysis_generation_service


def _refresh_scheduled_reports_if_needed() -> None:
    try:
        ensure_scheduled_reports()
    except Exception:
        return


def read_report(kind: str, period_key: str | None = None, source: str | None = None) -> dict[str, Any] | None:
    if kind not in shared.REPORT_KINDS:
        raise ValueError("invalid report kind")
    report = shared._preferred_report(kind, period_key=period_key)
    if source:
        if source not in shared.REPORT_BACKENDS:
            raise ValueError("invalid source")
        rows = [row for row in shared._list_reports(kind) if row.get("source") == source]
        if period_key:
            rows = [row for row in rows if row.get("period_key") == period_key]
        report = rows[0] if rows else None
    return report


def open_report_location(kind: str, period_key: str, source: str | None = None) -> dict[str, Any]:
    report = read_report(kind, period_key=period_key, source=source)
    if report is None:
        raise FileNotFoundError("analysis report not found")
    return open_report_folder(Path(str(report.get("path") or "")))


def delete_report(kind: str, period_key: str, source: str | None = None) -> dict[str, Any]:
    if kind not in shared.REPORT_KINDS:
        raise ValueError("invalid report kind")
    if source and source not in shared.REPORT_BACKENDS:
        raise ValueError("invalid source")
    removed = delete_report_files(shared._list_reports(kind), period_key=period_key, source=source)
    if not removed:
        raise FileNotFoundError("analysis report not found")
    return {"ok": True, "removed_files": removed}


def get_analysis_state() -> dict[str, Any]:
    _refresh_scheduled_reports_if_needed()
    quarterly_reports = shared._list_reports(shared.REPORT_KIND_QUARTERLY)
    yearly_reports = shared._list_reports(shared.REPORT_KIND_YEARLY)
    return {
        "quarterly": {
            "reports": summarize_report_rows(quarterly_reports),
            "preferred_period_key": quarterly_reports[0]["period_key"] if quarterly_reports else shared._previous_quarter_period().key,
        },
        "yearly": {
            "reports": summarize_report_rows(yearly_reports),
            "preferred_period_key": yearly_reports[0]["period_key"] if yearly_reports else shared._previous_year_period().key,
        },
        "paths": {
            "quarterly_reports": str(shared._report_dir(shared.REPORT_KIND_QUARTERLY)),
            "yearly_reports": str(shared._report_dir(shared.REPORT_KIND_YEARLY)),
        },
    }


def _load_scheduler_state() -> dict[str, str]:
    shared.ensure_analysis_storage()
    payload = load_state_payload(
        shared.STATE_PATH,
        {"quarterly_period_key": "", "yearly_period_key": ""},
        load_json=load_json_file,
    )
    return {
        "quarterly_period_key": str(payload.get("quarterly_period_key") or ""),
        "yearly_period_key": str(payload.get("yearly_period_key") or ""),
    }


def ensure_scheduled_reports(now: datetime | None = None) -> dict[str, Any]:
    current = now or datetime.now()
    state = _load_scheduler_state()
    actions = {"quarterly": False, "yearly": False}

    quarterly_period = shared._previous_quarter_period(current.date())
    quarter_start_month = ((int(quarterly_period.key[-1]) - 1) * 3) + 4
    quarter_start_year = int(quarterly_period.key[:4]) if quarter_start_month <= 12 else int(quarterly_period.key[:4]) + 1
    if quarter_start_month > 12:
        quarter_start_month -= 12
    quarterly_release = date(quarter_start_year, quarter_start_month, 1)
    if current.date() >= quarterly_release:
        quarterly_report = shared._preferred_report(shared.REPORT_KIND_QUARTERLY, quarterly_period.key)
        if not quarterly_report:
            library_analysis_generation_service.generate_report(shared.REPORT_KIND_QUARTERLY, "local", period_key=quarterly_period.key, manual=False)
            actions["quarterly"] = True
            quarterly_report = shared._preferred_report(shared.REPORT_KIND_QUARTERLY, quarterly_period.key)
        if quarterly_report:
            state["quarterly_period_key"] = quarterly_period.key

    yearly_period = shared._previous_year_period(current.date())
    yearly_release = date(int(yearly_period.key) + 1, 1, 1)
    if current.date() >= yearly_release:
        yearly_report = shared._preferred_report(shared.REPORT_KIND_YEARLY, yearly_period.key)
        if not yearly_report:
            library_analysis_generation_service.generate_report(shared.REPORT_KIND_YEARLY, "local", period_key=yearly_period.key, manual=False)
            actions["yearly"] = True
            yearly_report = shared._preferred_report(shared.REPORT_KIND_YEARLY, yearly_period.key)
        if yearly_report:
            state["yearly_period_key"] = yearly_period.key

    save_state_payload(shared.STATE_PATH, state)
    return actions


def _scheduler_loop() -> None:
    while not shared._SCHEDULER_STOP.wait(shared.SCHEDULER_INTERVAL_SECONDS):
        try:
            ensure_scheduled_reports()
        except Exception:
            continue


def start_scheduler() -> None:
    if shared._SCHEDULER_STARTED:
        return
    shared.ensure_analysis_storage()
    ensure_scheduled_reports()
    thread = threading.Thread(target=_scheduler_loop, name="library-analysis-scheduler", daemon=True)
    thread.start()
    shared._SCHEDULER_STARTED = True
