from __future__ import annotations

import time as _time
from typing import Any, Callable

from ..planner.context import finalize_query_classification
from ..planner.domain import ExecutionPlanShapingResult, RouterDecisionNormalizationResult
from ..planner.policy import shape_execution_plan
from ..planner.router import normalize_router_decision


def run_tool_planning_pipeline(
    *,
    question: str,
    history: list[dict[str, str]],
    search_mode: str,
    quota_state: dict[str, Any],
    query_profile: dict[str, Any],
    build_router_decision: Callable[..., Any],
    router_decision_to_query_classification: Callable[..., dict[str, Any]],
    build_plan: Callable[..., Any],
    resolve_router_context: Callable[..., Any],
    assemble_tool_options: Callable[..., Any],
    serialize_router_context_resolution: Callable[..., dict[str, Any]],
    serialize_execution_plan: Callable[..., dict[str, Any]],
    serialize_post_retrieval_assessment: Callable[..., dict[str, Any]],
    get_ontology_load_statuses: Callable[[], Any],
) -> tuple[RouterDecisionNormalizationResult, ExecutionPlanShapingResult, dict[str, Any], dict[str, float]]:
    normalization_t0 = _time.perf_counter()
    normalization = normalize_router_decision(
        question=question,
        history=history,
        quota_state=quota_state,
        query_profile=query_profile,
        build_router_decision=build_router_decision,
        router_decision_to_query_classification=router_decision_to_query_classification,
    )
    normalization_seconds = _time.perf_counter() - normalization_t0
    shape_t0 = _time.perf_counter()
    shaped = shape_execution_plan(
        question=question,
        history=history,
        search_mode=search_mode,
        normalization=normalization,
        build_plan=build_plan,
        resolve_router_context=resolve_router_context,
        assemble_tool_options=assemble_tool_options,
        serialize_post_retrieval_assessment=serialize_post_retrieval_assessment,
    )
    shape_seconds = _time.perf_counter() - shape_t0
    finalize_t0 = _time.perf_counter()
    query_classification = finalize_query_classification(
        question=question,
        normalization=normalization,
        shaped=shaped,
        serialize_router_context_resolution=serialize_router_context_resolution,
        serialize_execution_plan=serialize_execution_plan,
        serialize_post_retrieval_assessment=serialize_post_retrieval_assessment,
        get_ontology_load_statuses=get_ontology_load_statuses,
    )
    finalize_seconds = _time.perf_counter() - finalize_t0
    return normalization, shaped, query_classification, {
        "router_normalization_seconds": normalization_seconds,
        "execution_plan_shape_seconds": shape_seconds,
        "query_classification_finalize_seconds": finalize_seconds,
    }