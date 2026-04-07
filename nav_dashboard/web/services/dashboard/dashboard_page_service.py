from __future__ import annotations

import json
from typing import Any, Callable

from ..agent import agent_session_store


def build_benchmark_prefill_data() -> dict[str, Any]:
    try:
        from nav_dashboard.web.api.benchmark import _load_history as load_history
        from nav_dashboard.web.api.benchmark import get_benchmark_case_sets

        return {
            "history": {"results": load_history()},
            "cases": get_benchmark_case_sets(),
        }
    except Exception:  # noqa: BLE001
        return {"history": {"results": []}, "cases": {"case_sets": [], "chains": []}}


def build_index_context(
    *,
    request: Any,
    ai_summary_url: str,
    library_tracker_url: str,
    property_url: str,
    journey_url: str,
    local_model: str,
    deepseek_model: str,
    browser_cards: list[dict[str, str]],
    dashboard_prefill: dict[str, Any],
    dashboard_notifications_html: str,
    dashboard_jobs_prefill: dict[str, Any],
    tickets_prefill: dict[str, Any],
    benchmark_prefill: dict[str, Any],
    render_dashboard_core_cards_html: Callable[[dict[str, Any]], str],
    render_dashboard_latency_table_html: Callable[[dict[str, Any]], str],
    render_dashboard_observability_table_html: Callable[[dict[str, Any]], str],
    render_dashboard_jobs_html: Callable[[dict[str, Any]], str],
    render_dashboard_ticket_summary_meta_text: Callable[[dict[str, Any]], str],
    render_dashboard_ticket_summary_html: Callable[[dict[str, Any]], str],
    render_dashboard_ticket_trend_meta_text: Callable[[dict[str, Any]], str],
    render_dashboard_ticket_trend_html: Callable[[dict[str, Any]], str],
    render_dashboard_startup_logs_text: Callable[[dict[str, Any]], str],
    render_tickets_meta_text: Callable[[dict[str, Any]], str],
    render_tickets_list_html: Callable[[dict[str, Any]], str],
    render_benchmark_last_run_text: Callable[[dict[str, Any]], str],
    render_benchmark_case_set_options_html: Callable[[dict[str, Any]], str],
    render_benchmark_history_table_html: Callable[[dict[str, Any]], str],
    core_ui_bootstrap_version: str,
    core_app_shell_version: str,
    core_shell_shared_css_version: str,
    core_sidebar_shared_css_version: str,
    js_version: str,
    css_version: str,
) -> dict[str, Any]:
    tickets_applied_filters = dict(tickets_prefill.get("applied_filters") or {}) if isinstance(tickets_prefill, dict) else {}
    ticket_weekly_stats = dashboard_prefill.get("ticket_weekly_stats") if isinstance(dashboard_prefill, dict) else {}
    if not isinstance(ticket_weekly_stats, dict):
        ticket_weekly_stats = {}

    return {
        "request": request,
        "ai_summary_url": ai_summary_url,
        "library_tracker_url": library_tracker_url,
        "property_url": property_url,
        "journey_url": journey_url,
        "local_model": local_model,
        "deepseek_model": deepseek_model,
        "browser_cards": browser_cards,
        "custom_cards_json": json.dumps(browser_cards, ensure_ascii=False),
        "dashboard_prefill_json": json.dumps(dashboard_prefill, ensure_ascii=False),
        "dashboard_prefill": dashboard_prefill,
        "dashboard_notifications_html": dashboard_notifications_html,
        "dashboard_grid_html": render_dashboard_core_cards_html(dashboard_prefill),
        "dashboard_latency_table_html": render_dashboard_latency_table_html(dashboard_prefill),
        "dashboard_observability_table_html": render_dashboard_observability_table_html(dashboard_prefill),
        "dashboard_jobs_html": render_dashboard_jobs_html(dashboard_jobs_prefill),
        "dashboard_ticket_summary_meta": render_dashboard_ticket_summary_meta_text(ticket_weekly_stats),
        "dashboard_ticket_summary_html": render_dashboard_ticket_summary_html(ticket_weekly_stats),
        "dashboard_ticket_trend_meta": render_dashboard_ticket_trend_meta_text(ticket_weekly_stats),
        "dashboard_ticket_trend_html": render_dashboard_ticket_trend_html(ticket_weekly_stats),
        "dashboard_startup_logs_text": render_dashboard_startup_logs_text(dashboard_prefill),
        "agent_sessions_html": agent_session_store.render_session_list_items_html(),
        "agent_sessions_json": agent_session_store.sessions_json_payload(),
        "tickets_meta_text": render_tickets_meta_text(tickets_prefill),
        "tickets_list_html": render_tickets_list_html(tickets_prefill),
        "tickets_created_from_value": str(tickets_applied_filters.get("created_from") or ""),
        "tickets_created_to_value": str(tickets_applied_filters.get("created_to") or ""),
        "tickets_prefill_json": json.dumps(tickets_prefill, ensure_ascii=False),
        "benchmark_last_run_text": render_benchmark_last_run_text(benchmark_prefill),
        "benchmark_case_set_options_html": render_benchmark_case_set_options_html(benchmark_prefill),
        "benchmark_history_table_html": render_benchmark_history_table_html(benchmark_prefill),
        "benchmark_prefill_json": json.dumps(benchmark_prefill, ensure_ascii=False),
        "core_ui_bootstrap_version": core_ui_bootstrap_version,
        "core_app_shell_version": core_app_shell_version,
        "core_shell_shared_css_version": core_shell_shared_css_version,
        "core_sidebar_shared_css_version": core_sidebar_shared_css_version,
        "js_version": js_version,
        "css_version": css_version,
    }
