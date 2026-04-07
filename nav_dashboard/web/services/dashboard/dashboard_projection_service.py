from __future__ import annotations

import calendar
from datetime import date
from urllib.parse import urlencode
from typing import Any

from nav_dashboard.web.clients.internal_services import ai_summary_internal_base_url, get_json, library_tracker_internal_base_url, property_internal_base_url


def _report_source_for_period(reports: Any, period_key: str) -> str:
    rows = [row for row in reports if isinstance(row, dict)] if isinstance(reports, list) else []
    matching = [row for row in rows if str(row.get("period_key") or "").strip() == period_key]
    if not matching:
        return ""
    matching.sort(
        key=lambda row: (
            1 if str(row.get("source") or "").strip() == "deepseek" else 0,
            str(row.get("generated_at") or ""),
        ),
        reverse=True,
    )
    return str(matching[0].get("source") or "").strip()


def _append_report_notification_if_present(
    notices: list[dict[str, str]],
    dismissed: dict[str, Any],
    *,
    reports: Any,
    preferred_period_key: Any,
    key_prefix: str,
    title: str,
    message: str,
    target: str,
    tab: str,
    report_kind: str,
) -> None:
    period_key = str(preferred_period_key or "").strip()
    if not period_key:
        return
    source = _report_source_for_period(reports, period_key)
    if not source:
        return
    _append_notification(
        notices,
        dismissed,
        key=f"{key_prefix}:{period_key}",
        title=title,
        message=message.format(period_key=period_key),
        target=target,
        tab=tab,
        report_kind=report_kind,
        period_key=period_key,
        source=source,
    )


def load_library_stats_overview() -> dict[str, Any]:
    base = library_tracker_internal_base_url().rstrip("/")
    payload = get_json(f"{base}/api/library/stats/overview", timeout=5.0)
    return payload if isinstance(payload, dict) else {}


def load_library_alias_proposal_summary() -> dict[str, Any]:
    overview = load_library_stats_overview()
    summary = overview.get("alias_proposal") if isinstance(overview.get("alias_proposal"), dict) else None
    if summary is not None:
        return summary
    base = library_tracker_internal_base_url().rstrip("/")
    payload = get_json(f"{base}/api/library/alias-proposals/summary", timeout=5.0)
    return payload if isinstance(payload, dict) else {}


def load_library_analysis_state() -> dict[str, Any]:
    base = library_tracker_internal_base_url().rstrip("/")
    payload = get_json(f"{base}/api/library/analysis/state", timeout=5.0)
    return payload if isinstance(payload, dict) else {}


def load_ai_summary_dashboard_overview() -> dict[str, Any]:
    base = ai_summary_internal_base_url().rstrip("/")
    payload = get_json(f"{base}/api/rag/dashboard/overview", timeout=5.0)
    return payload if isinstance(payload, dict) else {}


def load_ai_summary_missing_queries(*, days: int = 30, limit: int = 200, source: str = "") -> dict[str, Any]:
    base = ai_summary_internal_base_url().rstrip("/")
    query = urlencode({"days": int(days), "limit": int(limit), "source": str(source or "")})
    payload = get_json(f"{base}/api/rag/dashboard/missing-queries?{query}", timeout=5.0)
    return payload if isinstance(payload, dict) else {}


def load_property_analysis_state() -> dict[str, Any]:
    base = property_internal_base_url().rstrip("/")
    payload = get_json(f"{base}/api/property/analysis/state", timeout=5.0)
    return payload if isinstance(payload, dict) else {}


def _append_notification(
    notices: list[dict[str, str]],
    dismissed: dict[str, Any],
    *,
    key: str,
    title: str,
    message: str,
    level: str = "info",
    target: str = "",
    tab: str = "",
    report_kind: str = "",
    period_key: str = "",
    source: str = "",
    snapshot_month: str = "",
    snapshot_action: str = "",
) -> None:
    if str(key or "").strip() in dismissed:
        return
    notices.append(
        {
            "key": key,
            "title": title,
            "message": message,
            "level": level,
            "target": target,
            "tab": tab,
            "report_kind": report_kind,
            "period_key": period_key,
            "source": source,
            "snapshot_month": snapshot_month,
            "snapshot_action": snapshot_action,
        }
    )


def build_information_notifications(
    dismissed: dict[str, Any],
    *,
    test_notification_key: str,
    today: date | None = None,
) -> list[dict[str, str]]:
    notices: list[dict[str, str]] = []
    _append_notification(
        notices,
        dismissed,
        key=test_notification_key,
        title="测试通知",
        message="这是 Information 区的新通知样式测试。点击右侧 × 后，这条测试通知不会再出现。",
        level="test",
    )

    library_state = load_library_analysis_state()
    quarterly = library_state.get("quarterly") if isinstance(library_state.get("quarterly"), dict) else {}
    yearly = library_state.get("yearly") if isinstance(library_state.get("yearly"), dict) else {}

    _append_report_notification_if_present(
        notices,
        dismissed,
        reports=quarterly.get("reports"),
        preferred_period_key=quarterly.get("preferred_period_key"),
        key_prefix="library-quarterly",
        title="书影音季度报告已生成",
        message="{period_key} 的书影音季度报告已生成，可以前往分析页查看或补充修改。",
        target="library",
        tab="analysis",
        report_kind="quarterly",
    )

    _append_report_notification_if_present(
        notices,
        dismissed,
        reports=yearly.get("reports"),
        preferred_period_key=yearly.get("preferred_period_key"),
        key_prefix="library-yearly",
        title="书影音年度报告已生成",
        message="{period_key} 的书影音年度报告已生成，可以前往分析页查看或补充修改。",
        target="library",
        tab="analysis",
        report_kind="yearly",
    )

    property_state = load_property_analysis_state()
    market = property_state.get("market") if isinstance(property_state.get("market"), dict) else {}
    asset = property_state.get("asset") if isinstance(property_state.get("asset"), dict) else {}

    _append_report_notification_if_present(
        notices,
        dismissed,
        reports=market.get("reports"),
        preferred_period_key=market.get("preferred_period_key"),
        key_prefix="property-market",
        title="市场周报已生成",
        message="{period_key} 的市场周报已生成，可以前往资产分析页查看。",
        target="property",
        tab="analysis",
        report_kind="market",
    )

    _append_report_notification_if_present(
        notices,
        dismissed,
        reports=asset.get("reports"),
        preferred_period_key=asset.get("preferred_period_key"),
        key_prefix="property-asset",
        title="资产月报已生成",
        message="{period_key} 的资产月报已生成，可以前往资产分析页查看。",
        target="property",
        tab="analysis",
        report_kind="asset",
    )

    current_day = today or date.today()
    last_day = calendar.monthrange(current_day.year, current_day.month)[1]
    if current_day.day == last_day:
        month_key = current_day.strftime("%Y-%m")
        _append_notification(
            notices,
            dismissed,
            key=f"asset-month-end-reminder:{month_key}",
            title="月底资产更新提醒",
            message="临近月底，请更新本月资产快照和收支情况，避免月报生成时缺少最新数据。",
            level="reminder",
            target="property",
            tab="snapshot",
            snapshot_month=month_key,
            snapshot_action="edit",
        )
    return notices