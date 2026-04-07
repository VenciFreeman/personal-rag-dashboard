from __future__ import annotations

from functools import partial
from typing import Any

from nav_dashboard.web.services.agent.agent_observability_owner import (
    build_agent_trace_record as build_agent_trace_record_owner,
    record_agent_metrics,
    write_agent_trace_record as write_agent_trace_record_owner,
)
from nav_dashboard.web.services.agent.trace_builder import build_agent_trace_record as trace_build_agent_trace_record
from nav_dashboard.web.services.planner.router_config import ROUTER_CONFIDENCE_HIGH, ROUTER_CONFIDENCE_MEDIUM

from .support_answering import _build_guardrail_flags
from .support_common import (
    QUERY_TYPE_MIXED,
    QUERY_TYPE_TECH,
    _compat_get_query_type,
    _compat_serialize_router_decision,
    _compute_classification_conformance,
    _get_lookup_mode_from_state,
    _get_planner_snapshot_from_runtime,
    _get_resolved_query_state_from_runtime,
    _normalize_media_filter_map,
    _serialize_execution_plan,
    _serialize_post_retrieval_assessment,
    runtime_infra,
    write_trace_record,
)


def _planner_confidence_high() -> float:
    return float(ROUTER_CONFIDENCE_HIGH or 0.9)


def _planner_confidence_medium() -> float:
    return float(ROUTER_CONFIDENCE_MEDIUM or 0.65)


def _build_error_taxonomy(
    runtime_state: Any,
    media_validation: dict[str, Any],
    guardrail_flags: dict[str, bool],
) -> dict[str, Any]:
    validator_drop_reasons = media_validation.get("drop_reasons") if isinstance(media_validation.get("drop_reasons"), dict) else {}
    dominant_validator_reason = ""
    dominant_count = -1
    for reason, count in validator_drop_reasons.items():
        current_count = int(count or 0)
        if current_count > dominant_count:
            dominant_validator_reason = str(reason)
            dominant_count = current_count

    tech_score = float(getattr(getattr(runtime_state, "post_retrieval_assessment", None), "tech_score", 0.0) or 0.0)
    if guardrail_flags.get("state_inheritance_ambiguous"):
        layer = "query_understanding"
        primary_error_type = "state_inheritance_ambiguous"
    elif guardrail_flags.get("low_confidence_understanding"):
        layer = "query_understanding"
        primary_error_type = "low_confidence_understanding"
    elif dominant_validator_reason:
        layer = "validation"
        primary_error_type = dominant_validator_reason
    elif guardrail_flags.get("insufficient_valid_results"):
        layer = "retrieval"
        primary_error_type = "insufficient_valid_results"
    elif guardrail_flags.get("answer_truncated_by_reference_limit"):
        layer = "answer_synthesis"
        primary_error_type = "reference_limit_truncation"
    elif tech_score <= 0 and _compat_get_query_type(getattr(runtime_state, "decision", None)) in {QUERY_TYPE_TECH, QUERY_TYPE_MIXED}:
        layer = "retrieval"
        primary_error_type = "knowledge_route_without_rag"
    else:
        layer = "none"
        primary_error_type = "none"

    return {
        "layer": layer,
        "primary_error_type": primary_error_type,
        "dominant_validator_reason": dominant_validator_reason,
    }


_build_agent_trace_record = partial(
    build_agent_trace_record_owner,
    trace_builder=trace_build_agent_trace_record,
    get_llm_profile=runtime_infra._get_llm_profile,
    get_resolved_query_state_from_runtime=_get_resolved_query_state_from_runtime,
    serialize_router_decision=_compat_serialize_router_decision,
    serialize_execution_plan=_serialize_execution_plan,
    serialize_post_retrieval_assessment=_serialize_post_retrieval_assessment,
    build_guardrail_flags=_build_guardrail_flags,
    build_error_taxonomy=_build_error_taxonomy,
    get_planner_snapshot_from_runtime=_get_planner_snapshot_from_runtime,
    normalize_media_filter_map=_normalize_media_filter_map,
    get_lookup_mode_from_state=_get_lookup_mode_from_state,
    compute_classification_conformance=_compute_classification_conformance,
    get_query_type=_compat_get_query_type,
    confidence_high=_planner_confidence_high(),
    confidence_medium=_planner_confidence_medium(),
    now_iso=runtime_infra._now_iso,
)


_write_agent_trace_record = partial(write_agent_trace_record_owner, trace_writer=write_trace_record)


__all__ = [name for name in globals() if not name.startswith("__")]