from __future__ import annotations

import importlib
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path


WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

if "web" not in sys.modules:
    sys.modules["web"] = importlib.import_module("library_tracker.web")

from library_tracker.web.services.library_analysis_report_store import read_report  # noqa: E402
from library_tracker.web.services.library_period_summary_builder import build_period_context  # noqa: E402
from library_tracker.web.services.library_report_service import render_report_markdown, resolve_period, save_report, summary_from_markdown  # noqa: E402
from library_tracker.web.services.library_spotlight_service import build_default_sections  # noqa: E402


START_QUARTER_YEAR = 2017
START_QUARTER = 4
START_YEAR = 2017


@dataclass(frozen=True)
class GenerationResult:
    kind: str
    period_key: str
    status: str
    generated_at: str = ""


def _quarter_keys(start_year: int, start_quarter: int, end_year: int, end_quarter: int) -> list[str]:
    keys: list[str] = []
    year = start_year
    quarter = start_quarter
    while (year, quarter) <= (end_year, end_quarter):
        keys.append(f"{year:04d}-Q{quarter}")
        quarter += 1
        if quarter > 4:
            year += 1
            quarter = 1
    return keys


def _previous_quarter(today: date) -> tuple[int, int]:
    quarter = ((today.month - 1) // 3) + 1
    if quarter == 1:
        return today.year - 1, 4
    return today.year, quarter - 1


def _generate_fallback(kind: str, period_key: str) -> GenerationResult:
    existing = read_report(kind, period_key=period_key)
    if existing is not None:
        return GenerationResult(kind=kind, period_key=period_key, status="exists", generated_at=str(existing.get("generated_at") or ""))
    period = resolve_period(kind, period_key)
    context = build_period_context(kind, period)
    sections = build_default_sections(context)
    markdown = render_report_markdown(period, context, sections)
    report = save_report(kind, "local", period, markdown, "规则兜底", summary_from_markdown(markdown))
    return GenerationResult(kind=kind, period_key=period_key, status="generated", generated_at=str(report.get("generated_at") or ""))


def main() -> int:
    today = date.today()
    end_quarter_year, end_quarter = _previous_quarter(today)
    end_year = today.year - 1

    quarterly_keys = _quarter_keys(START_QUARTER_YEAR, START_QUARTER, end_quarter_year, end_quarter)
    yearly_keys = [str(year) for year in range(START_YEAR, end_year + 1)]

    generated = 0
    skipped = 0

    print("[Quarterly] start")
    for period_key in quarterly_keys:
        result = _generate_fallback("quarterly", period_key)
        print(f"[quarterly] {period_key} -> {result.status} {result.generated_at}".rstrip())
        if result.status == "generated":
            generated += 1
        else:
            skipped += 1

    print("[Yearly] start")
    for period_key in yearly_keys:
        result = _generate_fallback("yearly", period_key)
        print(f"[yearly] {period_key} -> {result.status} {result.generated_at}".rstrip())
        if result.status == "generated":
            generated += 1
        else:
            skipped += 1

    print(f"done generated={generated} skipped={skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())