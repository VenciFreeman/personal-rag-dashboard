from __future__ import annotations

from typing import Any

from . import library_analysis_core as shared


def build_quarterly_context(period: shared.ReportPeriod) -> dict[str, Any]:
    current_rows = shared._records_for_period(shared.REPORT_KIND_QUARTERLY, period.key)
    previous_key = shared._previous_period_key(shared.REPORT_KIND_QUARTERLY, period.key)
    previous_rows = shared._records_for_period(shared.REPORT_KIND_QUARTERLY, previous_key)
    return shared._build_common_context(shared.REPORT_KIND_QUARTERLY, period, current_rows, previous_rows)


def build_quarterly_context_fast(period: shared.ReportPeriod) -> dict[str, Any]:
    current_rows = shared._records_for_period(shared.REPORT_KIND_QUARTERLY, period.key)
    previous_key = shared._previous_period_key(shared.REPORT_KIND_QUARTERLY, period.key)
    previous_rows = shared._records_for_period(shared.REPORT_KIND_QUARTERLY, previous_key)
    return shared._build_common_context(
        shared.REPORT_KIND_QUARTERLY,
        period,
        current_rows,
        previous_rows,
        include_external_references=False,
    )


def build_yearly_context(period: shared.ReportPeriod) -> dict[str, Any]:
    current_rows = shared._records_for_period(shared.REPORT_KIND_YEARLY, period.key)
    previous_key = shared._previous_period_key(shared.REPORT_KIND_YEARLY, period.key)
    previous_rows = shared._records_for_period(shared.REPORT_KIND_YEARLY, previous_key)
    context = shared._build_common_context(
        shared.REPORT_KIND_YEARLY,
        period,
        current_rows,
        previous_rows,
        include_external_references=False,
    )
    year = int(period.key)
    quarter_inputs = shared._quarter_report_inputs_for_year(year)
    quarter_rows: list[dict[str, Any]] = []
    quarter_contexts: list[dict[str, Any]] = []
    for quarter_key in shared._quarter_keys_for_year(year):
        quarter_context = build_quarterly_context_fast(shared._quarter_period_from_key(quarter_key))
        quarter_contexts.append(quarter_context)
        for row in quarter_context["comparison_rows"]:
            quarter_rows.append(
                {
                    "quarter": quarter_key.split("-")[1],
                    "media_type": row["media_type"],
                    "media_label": row["media_label"],
                    "count": row["current_count"],
                    "avg_rating": row["current_avg_rating"],
                    "representative_title": (quarter_context["representatives"].get(row["media_type"]) or {}).get("title") or "—",
                    "representative_id": (quarter_context["representatives"].get(row["media_type"]) or {}).get("id") or "",
                    "representative_rating": (quarter_context["representatives"].get(row["media_type"]) or {}).get("rating"),
                }
            )
    context["quarter_inputs"] = quarter_inputs
    context["trend_rows"] = quarter_rows
    context["annual_trend_signals"] = shared._annual_trend_signals(quarter_contexts)
    return context


def build_period_context(kind: str, period: shared.ReportPeriod) -> dict[str, Any]:
    if kind == shared.REPORT_KIND_YEARLY:
        return build_yearly_context(period)
    return build_quarterly_context(period)