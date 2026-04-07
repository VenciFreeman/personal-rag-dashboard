from __future__ import annotations

from datetime import datetime
from typing import Any

from . import library_analysis_core, library_analysis_generation_service, library_analysis_report_store

ReportPeriod = library_analysis_core.ReportPeriod

ANALYSIS_ROOT = library_analysis_core.ANALYSIS_ROOT
REPORTS_ROOT = library_analysis_core.REPORTS_ROOT
STATE_PATH = library_analysis_core.STATE_PATH

REPORT_KIND_QUARTERLY = library_analysis_core.REPORT_KIND_QUARTERLY
REPORT_KIND_YEARLY = library_analysis_core.REPORT_KIND_YEARLY
REPORT_KINDS = library_analysis_core.REPORT_KINDS
REPORT_KIND_TO_JOB_TYPE = library_analysis_core.REPORT_KIND_TO_JOB_TYPE
REPORT_KIND_LABELS = library_analysis_core.REPORT_KIND_LABELS
REPORT_KIND_TITLES = library_analysis_core.REPORT_KIND_TITLES
REPORT_BACKENDS = library_analysis_core.REPORT_BACKENDS
SCHEDULER_INTERVAL_SECONDS = library_analysis_core.SCHEDULER_INTERVAL_SECONDS
_LOCK = library_analysis_core._LOCK
_SCHEDULER_STOP = library_analysis_core._SCHEDULER_STOP


def ensure_analysis_storage() -> None:
    library_analysis_core.ensure_analysis_storage()


def read_report(kind: str, period_key: str | None = None, source: str | None = None) -> dict[str, Any] | None:
    return library_analysis_report_store.read_report(kind, period_key=period_key, source=source)


def generate_report(kind: str, backend: str, period_key: str | None = None, manual: bool = True) -> dict[str, Any]:
    return library_analysis_generation_service.generate_report(kind, backend, period_key=period_key, manual=manual)


def open_report_location(kind: str, period_key: str, source: str | None = None) -> dict[str, Any]:
    return library_analysis_report_store.open_report_location(kind, period_key=period_key, source=source)


def delete_report(kind: str, period_key: str, source: str | None = None) -> dict[str, Any]:
    return library_analysis_report_store.delete_report(kind, period_key=period_key, source=source)


def get_analysis_state() -> dict[str, Any]:
    return library_analysis_report_store.get_analysis_state()


def ensure_scheduled_reports(now: datetime | None = None) -> dict[str, Any]:
    return library_analysis_report_store.ensure_scheduled_reports(now=now)


def start_scheduler() -> None:
    library_analysis_report_store.start_scheduler()