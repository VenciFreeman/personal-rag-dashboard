from __future__ import annotations

import shutil
from pathlib import Path

from core_service import tickets as core_tickets
from core_service.runtime_data import app_runtime_root


PROJECT_ROOT = Path(__file__).resolve().parents[3]
NAV_DASHBOARD_RUNTIME_ROOT = app_runtime_root("nav_dashboard")
LEGACY_NAV_DASHBOARD_DATA_DIR = PROJECT_ROOT / "nav_dashboard" / "data"

DATA_DIR = NAV_DASHBOARD_RUNTIME_ROOT
CONFIG_DIR = DATA_DIR / "config"
STATE_DIR = DATA_DIR / "state"
OBSERVABILITY_DIR = DATA_DIR / "observability"
TICKETS_DIR = DATA_DIR / "tickets"
SESSIONS_DIR = DATA_DIR / "agent_sessions"
MEMORY_DIR = SESSIONS_DIR / "_memory"
DEBUG_DIR = SESSIONS_DIR / "debug_data"

CUSTOM_CARDS_FILE = CONFIG_DIR / "custom_cards.json"
QUOTA_FILE = STATE_DIR / "agent_quota.json"
QUOTA_HISTORY_FILE = STATE_DIR / "agent_quota_history.json"
USAGE_TRACE_FILE = OBSERVABILITY_DIR / "api_usage_traces.json"
AGENT_METRICS_FILE = OBSERVABILITY_DIR / "agent_metrics.json"
CHAT_FEEDBACK_FILE = OBSERVABILITY_DIR / "chat_feedback.json"
DASHBOARD_JOBS_FILE = STATE_DIR / "dashboard_jobs.json"

DEPLOY_INFO_FILE = STATE_DIR / "nav_dashboard_deploy.json"
NOTIFICATION_STATE_FILE = STATE_DIR / "nav_dashboard_notifications.json"
OVERVIEW_SNAPSHOT_FILE = STATE_DIR / "nav_dashboard_overview_snapshot.json"
TICKETS_FILE = TICKETS_DIR / "tickets.jsonl"
BUG_TICKET_OUTBOX_FILE = TICKETS_DIR / "bug_ticket_outbox.jsonl"
BUG_TICKET_INVOCATION_STATE_FILE = TICKETS_DIR / "bug_ticket_hook_invocations.json"
BUG_TICKET_BACKFILL_STATE_FILE = TICKETS_DIR / "bug_ticket_backfill_state.json"
BUG_TICKET_DEBUG_LOG = TICKETS_DIR / "bug_ticket_hook_debug.jsonl"

TRACE_RECORDS_DIR = DATA_DIR / "trace_records"
BENCHMARK_DIR = DATA_DIR / "benchmark"
BENCHMARK_CASES_FILE = BENCHMARK_DIR / "query_case_sets.json"
BENCHMARK_FILE = BENCHMARK_DIR / "results.json"
ROUTER_CLS_CASES_FILE = BENCHMARK_DIR / "router_cls_cases.json"
ROUTER_CLS_HISTORY_FILE = BENCHMARK_DIR / "router_cls_results.json"


def _should_audit_ticket_path(path: Path) -> bool:
    try:
        resolved = path.resolve()
    except OSError:
        resolved = path
    names = {resolved.name.lower(), resolved.parent.name.lower() if resolved.parent else ""}
    if "tickets" in names:
        return True
    return resolved.name.lower() in {
        "tickets.jsonl",
        "bug_ticket_outbox.jsonl",
        "bug_ticket_hook_invocations.json",
        "bug_ticket_backfill_state.json",
        "bug_ticket_hook_debug.jsonl",
    }


def _audit_ticket_path_move(action: str, source: Path, target: Path) -> None:
    if not (_should_audit_ticket_path(source) or _should_audit_ticket_path(target)):
        return
    try:
        source_exists = source.exists()
    except OSError:
        source_exists = False
    try:
        target_exists = target.exists()
    except OSError:
        target_exists = False
    core_tickets.append_ticket_storage_audit(
        action,
        source=source,
        target=target,
        source_exists=source_exists,
        target_exists=target_exists,
    )


def _runtime_layout_entries() -> list[tuple[Path, Path]]:
    return [
        (DATA_DIR / "custom_cards.json", CUSTOM_CARDS_FILE),
        (DATA_DIR / "agent_quota.json", QUOTA_FILE),
        (DATA_DIR / "agent_quota_history.json", QUOTA_HISTORY_FILE),
        (DATA_DIR / "dashboard_jobs.json", DASHBOARD_JOBS_FILE),
        (DATA_DIR / "nav_dashboard_deploy.json", DEPLOY_INFO_FILE),
        (DATA_DIR / "nav_dashboard_notifications.json", NOTIFICATION_STATE_FILE),
        (DATA_DIR / "nav_dashboard_overview_snapshot.json", OVERVIEW_SNAPSHOT_FILE),
        (DATA_DIR / "api_usage_traces.json", USAGE_TRACE_FILE),
        (DATA_DIR / "agent_metrics.json", AGENT_METRICS_FILE),
        (DATA_DIR / "chat_feedback.json", CHAT_FEEDBACK_FILE),
        (DATA_DIR / "bug_ticket_outbox.jsonl", BUG_TICKET_OUTBOX_FILE),
        (DATA_DIR / "bug_ticket_hook_invocations.json", BUG_TICKET_INVOCATION_STATE_FILE),
        (DATA_DIR / "bug_ticket_backfill_state.json", BUG_TICKET_BACKFILL_STATE_FILE),
        (DATA_DIR / "bug_ticket_hook_debug.jsonl", BUG_TICKET_DEBUG_LOG),
    ]


def _remove_empty_parents(path: Path, *, stop: Path) -> None:
    current = path.parent
    while current != stop and current.exists():
        try:
            current.rmdir()
        except OSError:
            break
        current = current.parent


def _merge_directory_contents(source: Path, target: Path) -> None:
    target.mkdir(parents=True, exist_ok=True)
    for child in source.iterdir():
        destination = target / child.name
        if child.is_dir():
            if destination.exists() and destination.is_dir():
                _merge_directory_contents(child, destination)
                try:
                    child.rmdir()
                except OSError:
                    pass
                continue
            shutil.move(str(child), str(destination))
            continue
        if destination.exists():
            continue
        shutil.move(str(child), str(destination))
    try:
        source.rmdir()
    except OSError:
        pass


def _normalize_layout_entry(source: Path, target: Path) -> None:
    if source == target or not source.exists():
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    if source.is_dir():
        if target.exists() and target.is_dir():
            _merge_directory_contents(source, target)
            _audit_ticket_path_move("runtime_layout_merge", source, target)
        elif not target.exists():
            shutil.move(str(source), str(target))
            _audit_ticket_path_move("runtime_layout_move", source, target)
        return
    if target.exists():
        return
    shutil.move(str(source), str(target))
    _audit_ticket_path_move("runtime_layout_move", source, target)
    _remove_empty_parents(source, stop=DATA_DIR)


def ensure_nav_dashboard_runtime_layout() -> None:
    for directory in (DATA_DIR, CONFIG_DIR, STATE_DIR, OBSERVABILITY_DIR, TICKETS_DIR, SESSIONS_DIR, BENCHMARK_DIR, TRACE_RECORDS_DIR):
        directory.mkdir(parents=True, exist_ok=True)
    for source, target in _runtime_layout_entries():
        _normalize_layout_entry(source, target)
    _normalize_layout_entry(DATA_DIR / "tickets.jsonl", TICKETS_FILE)
    _normalize_layout_entry(DATA_DIR / "tickets" / "tickets.jsonl", TICKETS_FILE)


def _select_legacy_source(*candidates: Path) -> Path:
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def nav_dashboard_runtime_migration_plan() -> list[dict[str, Path | str]]:
    plan: list[dict[str, Path | str]] = [
        {"entry": "bug_ticket_outbox.jsonl", "source": LEGACY_NAV_DASHBOARD_DATA_DIR / "bug_ticket_outbox.jsonl", "target": BUG_TICKET_OUTBOX_FILE},
        {"entry": "bug_ticket_hook_invocations.json", "source": LEGACY_NAV_DASHBOARD_DATA_DIR / "bug_ticket_hook_invocations.json", "target": BUG_TICKET_INVOCATION_STATE_FILE},
        {"entry": "bug_ticket_backfill_state.json", "source": LEGACY_NAV_DASHBOARD_DATA_DIR / "bug_ticket_backfill_state.json", "target": BUG_TICKET_BACKFILL_STATE_FILE},
        {"entry": "bug_ticket_hook_debug.jsonl", "source": LEGACY_NAV_DASHBOARD_DATA_DIR / "bug_ticket_hook_debug.jsonl", "target": BUG_TICKET_DEBUG_LOG},
        {"entry": "agent_quota.json", "source": LEGACY_NAV_DASHBOARD_DATA_DIR / "agent_quota.json", "target": QUOTA_FILE},
        {"entry": "agent_quota_history.json", "source": LEGACY_NAV_DASHBOARD_DATA_DIR / "agent_quota_history.json", "target": QUOTA_HISTORY_FILE},
        {"entry": "api_usage_traces.json", "source": LEGACY_NAV_DASHBOARD_DATA_DIR / "api_usage_traces.json", "target": USAGE_TRACE_FILE},
        {"entry": "agent_metrics.json", "source": LEGACY_NAV_DASHBOARD_DATA_DIR / "agent_metrics.json", "target": AGENT_METRICS_FILE},
        {"entry": "chat_feedback.json", "source": LEGACY_NAV_DASHBOARD_DATA_DIR / "chat_feedback.json", "target": CHAT_FEEDBACK_FILE},
        {"entry": "dashboard_jobs.json", "source": LEGACY_NAV_DASHBOARD_DATA_DIR / "dashboard_jobs.json", "target": DASHBOARD_JOBS_FILE},
        {"entry": "tickets.jsonl", "source": _select_legacy_source(LEGACY_NAV_DASHBOARD_DATA_DIR / "tickets.jsonl", LEGACY_NAV_DASHBOARD_DATA_DIR / "tickets" / "tickets.jsonl"), "target": TICKETS_FILE},
        {"entry": "benchmark/query_case_sets.json", "source": LEGACY_NAV_DASHBOARD_DATA_DIR / "benchmark" / "query_case_sets.json", "target": BENCHMARK_CASES_FILE},
        {"entry": "benchmark/results.json", "source": LEGACY_NAV_DASHBOARD_DATA_DIR / "benchmark" / "results.json", "target": BENCHMARK_FILE},
        {"entry": "benchmark/router_cls_cases.json", "source": LEGACY_NAV_DASHBOARD_DATA_DIR / "benchmark" / "router_cls_cases.json", "target": ROUTER_CLS_CASES_FILE},
        {"entry": "benchmark/router_cls_results.json", "source": LEGACY_NAV_DASHBOARD_DATA_DIR / "benchmark" / "router_cls_results.json", "target": ROUTER_CLS_HISTORY_FILE},
        {"entry": "agent_sessions", "source": LEGACY_NAV_DASHBOARD_DATA_DIR / "agent_sessions", "target": SESSIONS_DIR},
    ]
    extra_trace_sources: dict[str, Path] = {}
    for root in (LEGACY_NAV_DASHBOARD_DATA_DIR,):
        for source in sorted(root.glob("trace_records_*.jsonl"), key=lambda item: item.name.lower()):
            extra_trace_sources.setdefault(source.name, source)
    for name, source in sorted(extra_trace_sources.items(), key=lambda item: item[0].lower()):
        plan.append({"entry": name, "source": source, "target": TRACE_RECORDS_DIR / name})
    return plan


ensure_nav_dashboard_runtime_layout()