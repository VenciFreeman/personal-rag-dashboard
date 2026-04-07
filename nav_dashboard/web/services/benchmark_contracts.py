from __future__ import annotations

import re
from typing import Any

from nav_dashboard.web.api.response_models import API_SCHEMA_VERSION
from nav_dashboard.web.services.agent.benchmark_case_catalog import normalize_followup_mode
from nav_dashboard.web.services.planner import planner_contracts


def coerce_text_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        items = value
    elif isinstance(value, tuple):
        items = list(value)
    else:
        items = [value]
    normalized: list[str] = []
    for item in items:
        text = str(item or "").strip()
        if text:
            normalized.append(text)
    return normalized


def flatten_text_map(value: Any) -> str:
    if isinstance(value, dict):
        parts = [str(item or "").strip() for item in value.values()]
        return "\n".join(part for part in parts if part)
    return str(value or "").strip()


def normalize_benchmark_text_line(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return re.sub(r"\s+", " ", text)


def strip_rewritten_query_echo_lines(rewritten_blob: Any, *source_texts: Any) -> str:
    ignored_lines = {
        normalized
        for normalized in (normalize_benchmark_text_line(text) for text in source_texts)
        if normalized
    }
    kept_lines: list[str] = []
    for raw_line in str(rewritten_blob or "").splitlines():
        line = str(raw_line or "").strip()
        if not line:
            continue
        if normalize_benchmark_text_line(line) in ignored_lines:
            continue
        kept_lines.append(line)
    return "\n".join(kept_lines)


def extract_answer_reference_metrics(answer: str) -> dict[str, Any]:
    text = str(answer or "")
    reference_line_pattern = re.compile(r"(?m)^(?:\[(\d+)\]\s+\[[^\]]+\]\([^)]+\)|<a\s+href=\"[^\"]+\">\[(\d+)\]</a>\s+\[[^\]]+\]\([^)]+\))")
    reference_numbers = [
        int(match.group(1) or match.group(2))
        for match in reference_line_pattern.finditer(text)
    ]
    first_reference_line = reference_line_pattern.search(text)
    answer_body = text[: first_reference_line.start()].rstrip() if first_reference_line else text.strip()
    inline_reference_numbers = [int(match.group(1)) for match in re.finditer(r"\[(\d+)\]", answer_body)]
    expected_sequence = list(range(1, len(reference_numbers) + 1))
    reference_lines_sequential = reference_numbers == expected_sequence
    inline_first_occurrence_numbers = list(dict.fromkeys(inline_reference_numbers))
    inline_monotonic = all(
        later >= earlier
        for earlier, later in zip(inline_first_occurrence_numbers, inline_first_occurrence_numbers[1:])
    )
    known_reference_numbers = set(reference_numbers)
    inline_known = all(number in known_reference_numbers for number in inline_first_occurrence_numbers)
    return {
        "answer_body": answer_body,
        "reference_numbers": reference_numbers,
        "inline_reference_numbers": inline_reference_numbers,
        "references_monotonic": bool(reference_lines_sequential and inline_monotonic and inline_known),
    }


def evaluate_quality_assertions(case: dict[str, Any], record: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, bool], dict[str, bool]]:
    expected = dict(case.get("quality_assertions") or {}) if isinstance(case.get("quality_assertions"), dict) else {}
    checks: list[dict[str, Any]] = []
    flags: dict[str, bool] = {}
    applicability: dict[str, bool] = {}

    def add_check(name: str, expected_value: Any, actual_value: Any, passed: bool, *, flag_name: str | None = None) -> None:
        checks.append(
            {
                "name": name,
                "expected": expected_value,
                "actual": actual_value,
                "passed": passed,
            }
        )
        if flag_name:
            applicability[flag_name] = True
            flags[flag_name] = not passed

    if expected.get("answer_nonempty"):
        passed = not bool(record.get("answer_empty"))
        add_check("answer_nonempty", True, passed, passed, flag_name="empty_answer")

    if expected.get("answer_contains_any"):
        values = coerce_text_list(expected.get("answer_contains_any"))
        answer_text = str(record.get("answer") or "")
        add_check("answer_contains_any", values, answer_text[:160], any(value in answer_text for value in values))

    if expected.get("answer_contains_all"):
        values = coerce_text_list(expected.get("answer_contains_all"))
        answer_text = str(record.get("answer") or "")
        add_check("answer_contains_all", values, answer_text[:160], all(value in answer_text for value in values))

    if expected.get("answer_not_contains"):
        values = coerce_text_list(expected.get("answer_not_contains"))
        answer_text = str(record.get("answer") or "")
        add_check(
            "answer_not_contains",
            values,
            answer_text[:160],
            all(value not in answer_text for value in values),
            flag_name="wrong_topic_leak",
        )

    if expected.get("resolved_question_not_contains"):
        values = coerce_text_list(expected.get("resolved_question_not_contains"))
        resolved_question = str(record.get("resolved_question") or "")
        add_check(
            "resolved_question_not_contains",
            values,
            resolved_question,
            all(value not in resolved_question for value in values),
            flag_name="wrong_topic_leak",
        )

    if expected.get("rewritten_queries_not_contains"):
        values = coerce_text_list(expected.get("rewritten_queries_not_contains"))
        rewritten_blob = strip_rewritten_query_echo_lines(
            record.get("rewritten_query_blob") or "",
            case.get("query") or "",
            record.get("resolved_question") or "",
        )
        add_check(
            "rewritten_queries_not_contains",
            values,
            rewritten_blob,
            all(value not in rewritten_blob for value in values),
            flag_name="followup_contamination",
        )

    if expected.get("guardrail_mode_not"):
        disallowed = set(coerce_text_list(expected.get("guardrail_mode_not")))
        actual_mode = str(record.get("answer_guardrail_mode") or "")
        add_check("guardrail_mode_not", sorted(disallowed), actual_mode, actual_mode not in disallowed)

    if expected.get("references_monotonic"):
        actual_monotonic = bool(record.get("references_monotonic"))
        add_check("references_monotonic", True, actual_monotonic, actual_monotonic, flag_name="reference_order_violation")

    if expected.get("strict_scope_nonzero_if_active"):
        strict_scope_active = bool(record.get("strict_scope_active"))
        returned_result_count = int(record.get("returned_result_count", 0) or 0)
        add_check(
            "strict_scope_nonzero_if_active",
            True,
            {"strict_scope_active": strict_scope_active, "returned_result_count": returned_result_count},
            (not strict_scope_active) or returned_result_count > 0,
            flag_name="strict_scope_false_negative",
        )

    if expected.get("returned_result_count_min") is not None:
        minimum = int(expected.get("returned_result_count_min") or 0)
        actual_count = int(record.get("returned_result_count", 0) or 0)
        add_check("returned_result_count_min", minimum, actual_count, actual_count >= minimum)

    return checks, flags, applicability


def validate_agent_payload_schema(payload: dict[str, Any]) -> dict[str, Any]:
    raw_version = payload.get("api_schema_version")
    if raw_version in (None, ""):
        return {
            "schema_version": None,
            "schema_supported": True,
            "accepted": True,
            "reason": "",
        }
    try:
        schema_version = int(raw_version)
    except (TypeError, ValueError):
        return {
            "schema_version": None,
            "schema_supported": False,
            "accepted": False,
            "reason": "invalid_agent_api_schema_version",
        }
    schema_supported = schema_version == API_SCHEMA_VERSION
    return {
        "schema_version": schema_version,
        "schema_supported": schema_supported,
        "accepted": schema_supported,
        "reason": "" if schema_supported else f"unsupported_agent_api_schema_version:{schema_version}",
    }


def extract_agent_benchmark_metrics(resp: dict[str, Any]) -> dict[str, Any]:
    payload = dict(resp or {}) if isinstance(resp, dict) else {}
    extensions = payload.get("extensions") if isinstance(payload.get("extensions"), dict) else {}
    if extensions:
        payload = {**extensions, **payload}
    schema_check = validate_agent_payload_schema(payload)
    trace_id = str(payload.get("trace_id") or "").strip()
    if not schema_check["accepted"]:
        return {
            "trace_id": trace_id,
            "error": schema_check["reason"],
            "timings": {},
            "doc_data": {},
            "query_type": "",
            "query_class": "",
            "subject_scope": "",
            "time_scope_type": "",
            "answer_shape": "",
            "media_family": "",
            "followup_mode": "",
            "strict_scope_active": False,
            "planned_tools": [],
            "no_context": 0,
            "no_context_reason": "",
        }

    timings = payload.get("timings") if isinstance(payload.get("timings"), dict) else {}
    query_classification = payload.get("query_classification") if isinstance(payload.get("query_classification"), dict) else {}
    query_understanding = payload.get("query_understanding") if isinstance(payload.get("query_understanding"), dict) else {}
    planner_snapshot = payload.get("planner_snapshot") if isinstance(payload.get("planner_snapshot"), dict) else {}
    if not planner_snapshot and isinstance(query_classification.get("planner_snapshot"), dict):
        planner_snapshot = dict(query_classification.get("planner_snapshot") or {})

    router_payload = query_classification.get("router_decision") if isinstance(query_classification.get("router_decision"), dict) else {}
    router_contract = planner_contracts.validate_router_decision_contract_payload(
        router_payload,
        allow_unknown_schema=False,
    ) if router_payload else {
        "schema_version": None,
        "schema_supported": True,
        "accepted": True,
        "reason": "",
    }
    if router_payload and not router_contract["accepted"]:
        return {
            "trace_id": trace_id,
            "error": router_contract["reason"],
            "timings": timings,
            "doc_data": {},
            "query_type": str(query_classification.get("query_type") or payload.get("query_type") or "").strip(),
            "query_class": "",
            "subject_scope": "",
            "time_scope_type": "",
            "answer_shape": "",
            "media_family": "",
            "followup_mode": "",
            "strict_scope_active": False,
            "planned_tools": coerce_named_tool_list(payload.get("planned_tools")),
            "no_context": 0,
            "no_context_reason": "",
            "answer": str(payload.get("answer") or ""),
        }

    tool_results = payload.get("tool_results") if isinstance(payload.get("tool_results"), list) else []
    answer = str(payload.get("answer") or "")
    answer_reference_metrics = extract_answer_reference_metrics(answer)
    doc_result = next(
        (
            item for item in tool_results
            if isinstance(item, dict) and str(item.get("tool", "")).strip() == "query_document_rag" and isinstance(item.get("data"), dict)
        ),
        None,
    )
    doc_data = doc_result.get("data") if isinstance(doc_result, dict) else {}
    planned_tools = coerce_named_tool_list(payload.get("planned_tools"))
    query_type = str(query_classification.get("query_type") or payload.get("query_type") or router_payload.get("query_type") or "").strip()
    if int(timings.get("no_context", 0) or 0) > 0:
        no_context = 1
        no_context_reason = str(timings.get("no_context_reason", "") or "below_threshold")
    elif "query_document_rag" not in planned_tools and query_type.upper() in {"TECH_QUERY", "MIXED_QUERY"}:
        no_context = 1
        no_context_reason = "knowledge_route_without_rag"
    else:
        no_context = 0
        no_context_reason = ""

    source = router_payload if router_payload else query_classification
    local_media_tool_names = {"query_media_record", "search_by_creator"}
    local_media_payloads = [
        item.get("data")
        for item in tool_results
        if isinstance(item, dict)
        and str(item.get("tool", "")).strip() in local_media_tool_names
        and isinstance(item.get("data"), dict)
    ]
    strict_scope_active = False
    returned_result_count = 0
    for media_payload in local_media_payloads:
        media_layer_breakdown = media_payload.get("layer_breakdown") if isinstance(media_payload.get("layer_breakdown"), dict) else {}
        media_validation = media_payload.get("validation") if isinstance(media_payload.get("validation"), dict) else {}
        strict_scope_active = strict_scope_active or bool(media_layer_breakdown.get("strict_scope_active"))
        returned_result_count = max(
            returned_result_count,
            int(media_validation.get("returned_result_count", media_layer_breakdown.get("main_count", 0)) or 0),
        )
    rewritten_queries = query_classification.get("rewritten_queries") if isinstance(query_classification.get("rewritten_queries"), dict) else {}
    if not rewritten_queries and isinstance(planner_snapshot.get("rewritten_queries"), dict):
        rewritten_queries = dict(planner_snapshot.get("rewritten_queries") or {})
    answer_guardrail_mode = payload.get("answer_guardrail_mode") if isinstance(payload.get("answer_guardrail_mode"), dict) else {}
    return {
        "trace_id": trace_id,
        "error": "",
        "timings": timings,
        "doc_data": doc_data if isinstance(doc_data, dict) else {},
        "query_type": query_type,
        "query_class": str(source.get("query_class") or "").strip(),
        "subject_scope": str(source.get("subject_scope") or "").strip(),
        "time_scope_type": str(source.get("time_scope_type") or "").strip(),
        "answer_shape": str(source.get("answer_shape") or "").strip(),
        "media_family": str(source.get("media_family") or "").strip(),
        "followup_mode": normalize_followup_mode(
            planner_snapshot.get("followup_mode")
            or query_understanding.get("followup_mode")
            or source.get("followup_mode")
            or ""
        ),
        "strict_scope_active": strict_scope_active,
        "planned_tools": planned_tools,
        "no_context": no_context,
        "no_context_reason": no_context_reason,
        "answer": answer,
        "answer_body": str(answer_reference_metrics.get("answer_body") or ""),
        "answer_empty": not bool(str(answer_reference_metrics.get("answer_body") or "").strip()),
        "reference_numbers": list(answer_reference_metrics.get("reference_numbers") or []),
        "inline_reference_numbers": list(answer_reference_metrics.get("inline_reference_numbers") or []),
        "references_monotonic": bool(answer_reference_metrics.get("references_monotonic")),
        "reference_order_violation": not bool(answer_reference_metrics.get("references_monotonic")) and bool(answer_reference_metrics.get("reference_numbers")),
        "resolved_question": str(query_understanding.get("resolved_question") or "").strip(),
        "rewritten_query_blob": flatten_text_map(rewritten_queries),
        "answer_guardrail_mode": str(answer_guardrail_mode.get("mode") or "").strip(),
        "answer_guardrail_reasons": [str(item).strip() for item in list(answer_guardrail_mode.get("reasons") or []) if str(item).strip()],
        "returned_result_count": returned_result_count,
        "strict_scope_false_negative": bool(strict_scope_active and returned_result_count <= 0),
    }


def compact_benchmark_record(case: dict[str, Any], record: dict[str, Any]) -> dict[str, Any]:
    planned_tools = [str(name).strip() for name in list(record.get("planned_tools") or []) if str(name).strip()]
    expected_tools = [str(name).strip() for name in list(case.get("expected_tools") or []) if str(name).strip()]
    contract_checks: list[dict[str, Any]] = []

    def add_exact_check(name: str, expected: Any, actual: Any) -> None:
        expected_text = str(expected or "").strip()
        if not expected_text:
            return
        contract_checks.append(
            {
                "name": name,
                "expected": expected_text,
                "actual": str(actual or "").strip(),
                "passed": str(actual or "").strip() == expected_text,
            }
        )

    if expected_tools:
        contract_checks.append(
            {
                "name": "expected_tools",
                "expected": expected_tools,
                "actual": planned_tools,
                "passed": all(name in planned_tools for name in expected_tools),
            }
        )
    add_exact_check("expected_query_class", case.get("expected_query_class"), record.get("query_class"))
    add_exact_check("subject_scope", case.get("subject_scope"), record.get("subject_scope"))
    add_exact_check("time_scope_type", case.get("time_scope_type"), record.get("time_scope_type"))
    add_exact_check("answer_shape", case.get("answer_shape"), record.get("answer_shape"))
    add_exact_check("media_family", case.get("media_family"), record.get("media_family"))
    add_exact_check("followup_mode", normalize_followup_mode(case.get("followup_mode")), normalize_followup_mode(record.get("followup_mode")))
    if case.get("strict_scope_expected") is not None:
        contract_checks.append(
            {
                "name": "strict_scope_expected",
                "expected": bool(case.get("strict_scope_expected")),
                "actual": bool(record.get("strict_scope_active")),
                "passed": bool(record.get("strict_scope_active")) == bool(case.get("strict_scope_expected")),
            }
        )
    quality_checks, quality_flags, quality_flag_applicability = evaluate_quality_assertions(case, record)
    return {
        "id": str(case.get("id") or "").strip(),
        "query": str(case.get("query") or "").strip(),
        "length": str(case.get("length") or "").strip(),
        "source": str(case.get("source") or "").strip(),
        "taxonomy": str(case.get("taxonomy") or "").strip(),
        "expected_query_type": str(case.get("expected_query_type") or "").strip(),
        "expected_domain": str(case.get("expected_domain") or "").strip(),
        "expected_arbitration": str(case.get("expected_arbitration") or "").strip(),
        "expected_query_class": str(case.get("expected_query_class") or "").strip(),
        "subject_scope": str(case.get("subject_scope") or "").strip(),
        "time_scope_type": str(case.get("time_scope_type") or "").strip(),
        "answer_shape": str(case.get("answer_shape") or "").strip(),
        "media_family": str(case.get("media_family") or "").strip(),
        "followup_mode": str(case.get("followup_mode") or "").strip(),
        "strict_scope_expected": case.get("strict_scope_expected"),
        "quality_assertions": dict(case.get("quality_assertions") or {}) if isinstance(case.get("quality_assertions"), dict) else {},
        "expected_tools": expected_tools,
        "trace_id": str(record.get("trace_id") or "").strip(),
        "query_type": str(record.get("query_type") or "").strip(),
        "query_class": str(record.get("query_class") or "").strip(),
        "subject_scope_actual": str(record.get("subject_scope") or "").strip(),
        "time_scope_type_actual": str(record.get("time_scope_type") or "").strip(),
        "answer_shape_actual": str(record.get("answer_shape") or "").strip(),
        "media_family_actual": str(record.get("media_family") or "").strip(),
        "followup_mode_actual": str(record.get("followup_mode") or "").strip(),
        "strict_scope_active": bool(record.get("strict_scope_active")),
        "planned_tools": planned_tools,
        "contract_checks": contract_checks,
        "contract_pass": all(bool(item.get("passed")) for item in contract_checks) if contract_checks else True,
        "quality_checks": quality_checks,
        "quality_pass": all(bool(item.get("passed")) for item in quality_checks) if quality_checks else True,
        "quality_flags": quality_flags,
        "quality_flag_applicability": quality_flag_applicability,
        "quality_check_count": len(quality_checks),
        "no_context": int(record.get("no_context", 0) or 0),
        "no_context_reason": str(record.get("no_context_reason") or "").strip(),
        "doc_top1_score": record.get("doc_top1_score"),
        "doc_score_threshold": record.get("doc_score_threshold"),
        "answer_empty": bool(record.get("answer_empty")),
        "reference_order_violation": bool(record.get("reference_order_violation")),
        "strict_scope_false_negative": bool(record.get("strict_scope_false_negative")),
        "returned_result_count": int(record.get("returned_result_count", 0) or 0),
        "answer_guardrail_mode": str(record.get("answer_guardrail_mode") or "").strip(),
        "resolved_question": str(record.get("resolved_question") or "").strip(),
        "rewritten_query_blob": str(record.get("rewritten_query_blob") or "").strip(),
        "error": record.get("error"),
    }


def coerce_named_tool_list(value: Any) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for item in value if isinstance(value, list) else []:
        if isinstance(item, dict):
            raw_name = item.get("name") or item.get("tool")
        else:
            raw_name = item
        name = str(raw_name or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        names.append(name)
    return names


def collect_failed_trace_ids(records: list[dict[str, Any]] | None, *, kind: str) -> list[str]:
    trace_ids: list[str] = []
    for row in records or []:
        if not isinstance(row, dict):
            continue
        if kind == "errors":
            failed = bool(row.get("error"))
        elif kind == "no_context_rate":
            failed = int(row.get("no_context", 0) or 0) > 0
        else:
            failed = False
        if not failed:
            continue
        value = str(row.get("trace_id") or "").strip()
        if value and value not in trace_ids:
            trace_ids.append(value)
    return trace_ids


def merge_assertion_limits(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            nested = dict(merged.get(key) or {})
            nested.update(value)
            merged[key] = nested
        else:
            merged[key] = value
    return merged


def group_rows_by_taxonomy(rows_by_bucket: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for rows in rows_by_bucket.values():
        for row in rows:
            if not isinstance(row, dict):
                continue
            taxonomy = str(row.get("taxonomy") or "uncategorized").strip() or "uncategorized"
            grouped.setdefault(taxonomy, []).append(row)
    return grouped


def resolve_assertion_limits(
    chain: str,
    case_set_id: str,
    default_assertion_limits: dict[str, dict[str, Any]],
    assertion_limit_overrides_by_case_set: dict[str, dict[str, dict[str, Any]]],
    *,
    taxonomy: str = "",
) -> dict[str, Any]:
    base = dict(default_assertion_limits.get(chain, default_assertion_limits["agent"]))
    override = (
        assertion_limit_overrides_by_case_set.get(case_set_id, {}).get(chain, {})
        if case_set_id
        else {}
    )
    base = merge_assertion_limits(base, override)
    taxonomy_limits = base.get("by_taxonomy") if isinstance(base.get("by_taxonomy"), dict) else {}
    if taxonomy:
        override_for_taxonomy = taxonomy_limits.get(taxonomy)
        if isinstance(override_for_taxonomy, dict):
            base = merge_assertion_limits(base, override_for_taxonomy)
    base.pop("by_taxonomy", None)
    return base


def evaluate_assertions(
    chain: str,
    length: str,
    aggregate: dict[str, Any],
    default_assertion_limits: dict[str, dict[str, Any]],
    assertion_limit_overrides_by_case_set: dict[str, dict[str, dict[str, Any]]],
    case_set_id: str = "",
    records: list[dict[str, Any]] | None = None,
    *,
    limit_bucket: str | None = None,
    taxonomy: str = "",
) -> dict[str, Any]:
    limits = resolve_assertion_limits(
        chain,
        case_set_id,
        default_assertion_limits,
        assertion_limit_overrides_by_case_set,
        taxonomy=taxonomy,
    )
    resolved_limit_bucket = str(limit_bucket or length or "global").strip() or "global"
    checks: list[dict[str, Any]] = []
    errors = int(aggregate.get("errors", 0) or 0)
    count = int(aggregate.get("count", 0) or 0)
    errors_check = {
        "name": "errors",
        "actual": errors,
        "expected": "0",
        "passed": errors == 0,
    }
    error_trace_ids = collect_failed_trace_ids(records, kind="errors")
    if error_trace_ids:
        errors_check["trace_ids"] = error_trace_ids
    checks.append(errors_check)
    if count > 0:
        no_context_rate = float(aggregate.get("no_context_rate", 0) or 0)
        max_no_context = float(limits.get("max_no_context_rate", 1.0) or 1.0)
        no_context_check = {
            "name": "no_context_rate",
            "actual": round(no_context_rate, 4),
            "expected": f"<= {max_no_context}",
            "passed": no_context_rate <= max_no_context,
        }
        no_context_trace_ids = collect_failed_trace_ids(records, kind="no_context_rate")
        if no_context_trace_ids:
            no_context_check["trace_ids"] = no_context_trace_ids
        checks.append(no_context_check)
        p95_wall = float(aggregate.get("p95_wall_clock_s", 0) or 0)
        wall_limits = limits.get("max_p95_wall_clock_s", {}) or {}
        wall_limit = float(wall_limits.get(resolved_limit_bucket, wall_limits.get("global", 999999)))
        checks.append({
            "name": "p95_wall_clock_s",
            "actual": round(p95_wall, 4),
            "expected": f"<= {wall_limit}",
            "passed": p95_wall <= wall_limit,
        })
        p95_elapsed = float(aggregate.get("p95_elapsed_s", aggregate.get("p95_wall_clock_s", 0)) or 0)
        elapsed_limits = limits.get("max_p95_elapsed_s", {}) or {}
        elapsed_limit = float(elapsed_limits.get(resolved_limit_bucket, elapsed_limits.get("global", 999999)))
        checks.append({
            "name": "p95_elapsed_s",
            "actual": round(p95_elapsed, 4),
            "expected": f"<= {elapsed_limit}",
            "passed": p95_elapsed <= elapsed_limit,
        })
        if int(aggregate.get("quality_case_count", 0) or 0) > 0:
            quality_fail_rate = float(aggregate.get("quality_fail_rate", 0) or 0)
            max_quality_fail_rate = float(limits.get("max_quality_fail_rate", 0.0) or 0.0)
            checks.append({
                "name": "quality_fail_rate",
                "actual": round(quality_fail_rate, 4),
                "expected": f"<= {max_quality_fail_rate}",
                "passed": quality_fail_rate <= max_quality_fail_rate,
            })
        if int(aggregate.get("answer_case_count", 0) or 0) > 0:
            empty_answer_rate = float(aggregate.get("empty_answer_rate", 0) or 0)
            max_empty_answer_rate = float(limits.get("max_empty_answer_rate", 0.0) or 0.0)
            checks.append({
                "name": "empty_answer_rate",
                "actual": round(empty_answer_rate, 4),
                "expected": f"<= {max_empty_answer_rate}",
                "passed": empty_answer_rate <= max_empty_answer_rate,
            })
        if int(aggregate.get("strict_scope_case_count", 0) or 0) > 0:
            strict_scope_false_negative_rate = float(aggregate.get("strict_scope_false_negative_rate", 0) or 0)
            max_strict_scope_false_negative_rate = float(limits.get("max_strict_scope_false_negative_rate", 0.0) or 0.0)
            checks.append({
                "name": "strict_scope_false_negative_rate",
                "actual": round(strict_scope_false_negative_rate, 4),
                "expected": f"<= {max_strict_scope_false_negative_rate}",
                "passed": strict_scope_false_negative_rate <= max_strict_scope_false_negative_rate,
            })
        if int(aggregate.get("reference_case_count", 0) or 0) > 0:
            reference_order_violation_rate = float(aggregate.get("reference_order_violation_rate", 0) or 0)
            max_reference_order_violation_rate = float(limits.get("max_reference_order_violation_rate", 0.0) or 0.0)
            checks.append({
                "name": "reference_order_violation_rate",
                "actual": round(reference_order_violation_rate, 4),
                "expected": f"<= {max_reference_order_violation_rate}",
                "passed": reference_order_violation_rate <= max_reference_order_violation_rate,
            })
        for metric_name in ("wrong_topic_leak_rate", "followup_contamination_rate"):
            metric_case_count = int(aggregate.get(metric_name.replace("_rate", "_case_count"), 0) or 0)
            if metric_case_count <= 0:
                continue
            actual_rate = float(aggregate.get(metric_name, 0) or 0)
            max_rate = float(limits.get(f"max_{metric_name}", 0.0) or 0.0)
            checks.append({
                "name": metric_name,
                "actual": round(actual_rate, 4),
                "expected": f"<= {max_rate}",
                "passed": actual_rate <= max_rate,
            })
    passed = all(bool(item.get("passed")) for item in checks)
    return {"passed": passed, "checks": checks}


def attach_assertions(
    result: dict[str, Any],
    chains: list[str],
    default_assertion_limits: dict[str, dict[str, Any]],
    assertion_limit_overrides_by_case_set: dict[str, dict[str, dict[str, Any]]],
) -> dict[str, Any]:
    assertions: dict[str, Any] = {}
    summary = {"passed": 0, "failed": 0}
    config = result.get("config") if isinstance(result.get("config"), dict) else {}
    case_set_id = str(config.get("case_set_id") or "").strip()
    for chain in chains:
        chain_payload = result.get(chain) if isinstance(result.get(chain), dict) else {}
        if not isinstance(chain_payload, dict):
            continue
        by_length = chain_payload.get("by_length") if isinstance(chain_payload.get("by_length"), dict) else {}
        by_taxonomy = chain_payload.get("by_taxonomy") if isinstance(chain_payload.get("by_taxonomy"), dict) else {}
        records_by_length = chain_payload.get("records_by_length") if isinstance(chain_payload.get("records_by_length"), dict) else {}
        records_by_taxonomy = chain_payload.get("records_by_taxonomy") if isinstance(chain_payload.get("records_by_taxonomy"), dict) else {}
        global_records: list[dict[str, Any]] = []
        for value in records_by_length.values():
            if isinstance(value, list):
                global_records.extend(item for item in value if isinstance(item, dict))
        chain_assertions = {
            "by_length": {},
            "by_taxonomy": {},
            "global": evaluate_assertions(
                chain,
                "global",
                chain_payload.get("global") if isinstance(chain_payload.get("global"), dict) else {},
                default_assertion_limits,
                assertion_limit_overrides_by_case_set,
                case_set_id=case_set_id,
                records=global_records,
                limit_bucket="global",
            ),
        }
        for length, aggregate in by_length.items():
            if isinstance(aggregate, dict):
                chain_assertions["by_length"][length] = evaluate_assertions(
                    chain,
                    str(length),
                    aggregate,
                    default_assertion_limits,
                    assertion_limit_overrides_by_case_set,
                    case_set_id=case_set_id,
                    records=records_by_length.get(length) if isinstance(records_by_length.get(length), list) else [],
                    limit_bucket=str(length),
                )
        for taxonomy, aggregate in by_taxonomy.items():
            if isinstance(aggregate, dict):
                chain_assertions["by_taxonomy"][taxonomy] = evaluate_assertions(
                    chain,
                    str(taxonomy),
                    aggregate,
                    default_assertion_limits,
                    assertion_limit_overrides_by_case_set,
                    case_set_id=case_set_id,
                    records=records_by_taxonomy.get(taxonomy) if isinstance(records_by_taxonomy.get(taxonomy), list) else [],
                    limit_bucket="global",
                    taxonomy=str(taxonomy),
                )
        for value in list(chain_assertions["by_length"].values()) + list(chain_assertions["by_taxonomy"].values()) + [chain_assertions["global"]]:
            if value.get("passed"):
                summary["passed"] += 1
            else:
                summary["failed"] += 1
        assertions[chain] = chain_assertions
    result["assertions"] = assertions
    result["assertion_summary"] = summary
    return result


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = int(round((len(ordered) - 1) * max(0.0, min(1.0, p / 100.0))))
    return ordered[idx]


def aggregate_records(records: list[dict[str, Any]]) -> dict[str, Any]:
    if not records:
        return {"count": 0, "errors": 0}
    errors = sum(1 for r in records if r.get("error"))
    valid = [r for r in records if not r.get("error")]
    out: dict[str, Any] = {"count": len(records), "errors": errors}
    if not valid:
        return out
    n = len(valid)
    out["avg_wall_clock_s"] = round(sum(r["wall_clock_s"] for r in valid) / n, 3)
    wall_vals = [r["wall_clock_s"] for r in valid]
    out["p50_wall_clock_s"] = round(percentile(wall_vals, 50), 3)
    out["p95_wall_clock_s"] = round(percentile(wall_vals, 95), 3)
    out["p99_wall_clock_s"] = round(percentile(wall_vals, 99), 3)

    if any("elapsed_s" in r for r in valid):
        vals = [float(r.get("elapsed_s") or 0) for r in valid]
        out["avg_elapsed_s"] = round(sum(vals) / n, 3)
        out["p50_elapsed_s"] = round(percentile(vals, 50), 3)
        out["p95_elapsed_s"] = round(percentile(vals, 95), 3)
        out["p99_elapsed_s"] = round(percentile(vals, 99), 3)

    no_ctx = sum(1 for r in valid if int(r.get("no_context", 0) or 0))
    out["no_context_rate"] = round(no_ctx / n, 3) if n else 0.0

    answer_rows = [r for r in valid if "answer_empty" in r]
    if answer_rows:
        out["answer_case_count"] = len(answer_rows)
        out["empty_answer_rate"] = round(
            sum(1 for r in answer_rows if bool(r.get("answer_empty"))) / len(answer_rows),
            3,
        )

    strict_scope_rows = [
        r for r in valid
        if bool((r.get("quality_flag_applicability") or {}).get("strict_scope_false_negative"))
    ]
    if strict_scope_rows:
        out["strict_scope_case_count"] = len(strict_scope_rows)
        out["strict_scope_false_negative_rate"] = round(
            sum(1 for r in strict_scope_rows if bool((r.get("quality_flags") or {}).get("strict_scope_false_negative"))) / len(strict_scope_rows),
            3,
        )

    reference_rows = [r for r in valid if bool(r.get("reference_order_violation")) or bool(r.get("references_monotonic"))]
    if reference_rows:
        out["reference_case_count"] = len(reference_rows)
        out["reference_order_violation_rate"] = round(
            sum(1 for r in reference_rows if bool(r.get("reference_order_violation"))) / len(reference_rows),
            3,
        )

    quality_rows = [r for r in valid if int(r.get("quality_check_count", 0) or 0) > 0]
    if quality_rows:
        out["quality_case_count"] = len(quality_rows)
        out["quality_fail_rate"] = round(
            sum(1 for r in quality_rows if not bool(r.get("quality_pass", True))) / len(quality_rows),
            3,
        )
        for flag_name in ("wrong_topic_leak", "followup_contamination"):
            applicable_rows = [
                r for r in quality_rows
                if bool((r.get("quality_flag_applicability") or {}).get(flag_name))
            ]
            if not applicable_rows:
                continue
            out[f"{flag_name}_case_count"] = len(applicable_rows)
            out[f"{flag_name}_rate"] = round(
                sum(1 for r in applicable_rows if bool((r.get("quality_flags") or {}).get(flag_name))) / len(applicable_rows),
                3,
            )

    sums: dict[str, float] = {}
    cnts: dict[str, int] = {}
    vals_by_stage: dict[str, list[float]] = {}
    for r in valid:
        for k, v in (r.get("timings") or {}).items():
            try:
                fv = float(v)
            except Exception:
                continue
            if fv < 0:
                continue
            sums[k] = sums.get(k, 0.0) + fv
            cnts[k] = cnts.get(k, 0) + 1
            vals_by_stage.setdefault(k, []).append(fv)
    for k, s in sums.items():
        c = cnts[k]
        out[f"avg_{k}_s"] = round(s / c, 4) if c else 0.0
    for k, vlist in vals_by_stage.items():
        if len(vlist) >= 1:
            out[f"p50_{k}_s"] = round(percentile(vlist, 50), 4)
            out[f"p95_{k}_s"] = round(percentile(vlist, 95), 4)
            out[f"p99_{k}_s"] = round(percentile(vlist, 99), 4)
    return out