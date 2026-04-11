from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any, Callable

from fastapi import HTTPException

from core_service.bug_ticket_payloads import parse_bug_ticket_payload
from core_service.observability import get_trace_record


def default_ticket_date_range(*, today: date | None = None) -> tuple[date, date]:
    current_day = today or date.today()
    return (current_day - timedelta(days=6), current_day)


def resolve_ticket_default_status(
    *,
    created_from: str,
    created_to: str,
    ticket_loader: Callable[..., list[dict[str, Any]]],
) -> str:
    non_closed_items = ticket_loader(
        status="non_closed",
        created_from=created_from,
        created_to=created_to,
        limit=1,
        sort="updated_desc",
    )
    if non_closed_items:
        return "non_closed"
    any_items = ticket_loader(
        status="all",
        created_from=created_from,
        created_to=created_to,
        limit=1,
        sort="updated_desc",
    )
    if any_items:
        return "all"
    return "non_closed"


def _safe_text_list(value: Any) -> list[str]:
    if isinstance(value, str):
        raw_items = value.replace("\r", "\n").replace(",", "\n").split("\n")
    elif isinstance(value, list):
        raw_items = value
    else:
        raw_items = []
    items: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        items.append(text)
    return items


def _first_nonempty(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _infer_ticket_domain(trace: dict[str, Any], fallback: str = "") -> str:
    explicit = str(fallback or "").strip().lower()
    if explicit:
        return explicit
    router = trace.get("router") if isinstance(trace.get("router"), dict) else {}
    understanding = trace.get("query_understanding") if isinstance(trace.get("query_understanding"), dict) else {}
    query_type = str(trace.get("query_type", "") or "").strip().lower()
    selected_tool = str(router.get("selected_tool", "") or "").strip().lower()
    lookup_mode = str(understanding.get("lookup_mode", router.get("lookup_mode", "")) or "").strip().lower()
    domain = str(understanding.get("domain", router.get("domain", "")) or "").strip().lower()
    if "media" in query_type or domain == "media" or lookup_mode in {"general_lookup", "entity_lookup", "filter_search", "concept_lookup"}:
        return "media"
    if "web" in selected_tool:
        return "web"
    if "doc" in selected_tool or "rag" in selected_tool or "tech" in query_type:
        return "knowledge"
    return "agent"


def _infer_ticket_category(trace: dict[str, Any], fallback: str = "") -> str:
    explicit = str(fallback or "").strip().lower()
    if explicit:
        return explicit
    taxonomy = trace.get("error_taxonomy") if isinstance(trace.get("error_taxonomy"), dict) else {}
    router = trace.get("router") if isinstance(trace.get("router"), dict) else {}
    primary = str(taxonomy.get("primary_label", "") or "").strip().lower()
    if primary:
        return primary
    decision = str(router.get("decision_category", "") or "").strip().lower()
    if decision:
        return decision
    selected_tool = str(router.get("selected_tool", "") or "").strip().lower()
    if selected_tool:
        return selected_tool
    return "investigation"


def _empty_ticket_draft() -> dict[str, Any]:
    return {
        "ticket_id": "",
        "title": "",
        "status": "open",
        "priority": "medium",
        "domain": "",
        "category": "",
        "summary": "",
        "related_traces": [],
        "repro_query": "",
        "expected_behavior": "",
        "actual_behavior": "",
        "root_cause": "",
        "fix_notes": "",
        "additional_notes": "",
        "created_at": "",
        "updated_at": "",
    }


def parse_ticket_paste_payload(raw_text: str) -> dict[str, Any]:
    raw = str(raw_text or "").strip()
    if not raw:
        raise ValueError("请先粘贴 BUG-TICKET 文本")

    payload_text = raw
    marker_index = raw.find("BUG-TICKET:")
    if marker_index >= 0:
        payload_text = raw[marker_index + len("BUG-TICKET:"):].strip()

    if not payload_text:
        raise ValueError("未找到 BUG-TICKET JSON 内容")

    payload = parse_bug_ticket_payload(payload_text)

    ticket = _empty_ticket_draft()
    ticket.update(
        {
            "title": _first_nonempty(payload.get("title"), ticket["title"]),
            "status": _first_nonempty(payload.get("status"), ticket["status"]),
            "priority": _first_nonempty(payload.get("priority"), ticket["priority"]),
            "domain": _first_nonempty(payload.get("domain"), ticket["domain"]),
            "category": _first_nonempty(payload.get("category"), ticket["category"]),
            "summary": _first_nonempty(payload.get("summary"), ticket["summary"]),
            "related_traces": _safe_text_list(payload.get("related_traces")),
            "repro_query": _first_nonempty(payload.get("repro_query"), ticket["repro_query"]),
            "expected_behavior": _first_nonempty(payload.get("expected_behavior"), ticket["expected_behavior"]),
            "actual_behavior": _first_nonempty(payload.get("actual_behavior"), ticket["actual_behavior"]),
            "root_cause": _first_nonempty(payload.get("root_cause"), ticket["root_cause"]),
            "fix_notes": _first_nonempty(payload.get("fix_notes"), ticket["fix_notes"]),
            "additional_notes": _first_nonempty(payload.get("additional_notes"), ticket["additional_notes"]),
        }
    )
    return ticket


def build_ticket_ai_draft(
    payload: Any,
    *,
    trace_loader: Callable[[str], dict[str, Any] | None] = get_trace_record,
) -> dict[str, Any]:
    trace_id = str(getattr(payload, "trace_id", "") or "").strip()
    trace = trace_loader(trace_id) if trace_id else None
    if trace_id and not trace:
        raise HTTPException(status_code=404, detail=f"未找到 trace_id={trace_id} 对应的追踪记录")

    understanding = trace.get("query_understanding") if isinstance(trace, dict) and isinstance(trace.get("query_understanding"), dict) else {}
    router = trace.get("router") if isinstance(trace, dict) and isinstance(trace.get("router"), dict) else {}
    result = trace.get("result") if isinstance(trace, dict) and isinstance(trace.get("result"), dict) else {}
    taxonomy = trace.get("error_taxonomy") if isinstance(trace, dict) and isinstance(trace.get("error_taxonomy"), dict) else {}
    guardrail_flags = trace.get("guardrail_flags") if isinstance(trace, dict) and isinstance(trace.get("guardrail_flags"), dict) else {}
    answer_guardrail_mode = trace.get("answer_guardrail_mode") if isinstance(trace, dict) and isinstance(trace.get("answer_guardrail_mode"), dict) else {}

    related_traces = _safe_text_list(getattr(payload, "related_traces", []))
    if trace_id and trace_id not in related_traces:
        related_traces.insert(0, trace_id)

    resolved_question = _first_nonempty(
        getattr(payload, "repro_query", ""),
        understanding.get("resolved_question"),
        understanding.get("original_question"),
    )
    route = str(router.get("selected_tool", "") or "").strip()
    mode = str(answer_guardrail_mode.get("mode", "") or "").strip()
    mode_reasons = [str(item or "").strip() for item in list(answer_guardrail_mode.get("reasons") or []) if str(item or "").strip()]
    primary_error = str(taxonomy.get("primary_label", "") or "").strip()
    secondary_error = str(taxonomy.get("secondary_label", "") or "").strip()
    no_context_reason = str(result.get("no_context_reason", "") or "").strip()

    priority = str(getattr(payload, "priority", "medium") or "medium").strip().lower() or "medium"
    if primary_error in {"answer_restricted", "understanding_ambiguous"}:
        priority = "high"
    if primary_error in {"answer_incorrect_entity", "retrieval_empty_after_validation"}:
        priority = "critical"

    summary = _first_nonempty(
        getattr(payload, "summary", ""),
        f"{resolved_question or '未提供 query'} 的回答链路出现异常，route={route or '-'}，mode={mode or 'normal'}。",
    )
    actual_behavior = _first_nonempty(
        getattr(payload, "actual_behavior", ""),
        "\n".join(
            [
                line for line in [
                    f"复现 query: {resolved_question}" if resolved_question else "",
                    f"实际路由: {route}" if route else "",
                    f"错误分类: {primary_error}" if primary_error else "",
                    f"次级分类: {secondary_error}" if secondary_error else "",
                    f"Guardrail 模式: {mode}" if mode else "",
                    f"Guardrail 原因: {', '.join(mode_reasons)}" if mode_reasons else "",
                    f"未命中原因: {no_context_reason}" if no_context_reason else "",
                ] if line
            ]
        ),
    )
    root_cause = _first_nonempty(
        getattr(payload, "root_cause", ""),
        "; ".join(
            [
                item for item in [
                    primary_error,
                    secondary_error,
                    ", ".join(sorted([key for key, enabled in guardrail_flags.items() if enabled])),
                ] if item
            ]
        ),
    )
    expected_behavior = _first_nonempty(
        getattr(payload, "expected_behavior", ""),
        "应正确理解用户问题与上下文，选择匹配的检索/工具链路，并返回与实体、时间窗、过滤条件一致的结果。",
    )
    title = _first_nonempty(
        getattr(payload, "title", ""),
        resolved_question,
        actual_behavior.splitlines()[0] if actual_behavior else "",
        summary,
    )[:120]

    return {
        "ticket_id": "",
        "title": title or "未命名 Ticket",
        "status": "open",
        "priority": priority,
        "domain": _infer_ticket_domain(trace or {}, getattr(payload, "domain", "")),
        "category": _infer_ticket_category(trace or {}, getattr(payload, "category", "")),
        "summary": summary,
        "related_traces": related_traces,
        "repro_query": resolved_question,
        "expected_behavior": expected_behavior,
        "actual_behavior": actual_behavior,
        "root_cause": root_cause,
        "fix_notes": str(getattr(payload, "fix_notes", "") or "").strip(),
        "additional_notes": str(getattr(payload, "additional_notes", "") or "").strip(),
        "created_by": str(getattr(payload, "created_by", "ai") or "ai").strip() or "ai",
        "updated_by": str(getattr(payload, "updated_by", "ai") or "ai").strip() or "ai",
    }