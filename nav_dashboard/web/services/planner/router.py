from __future__ import annotations

from typing import Any, Callable

from ..agent.agent_types import RouterDecision
from .domain import RouterDecisionNormalizationResult


def normalize_router_decision(
    *,
    question: str,
    history: list[dict[str, str]],
    quota_state: dict[str, Any],
    query_profile: dict[str, Any],
    build_router_decision: Callable[[str, list[dict[str, str]], dict[str, Any], dict[str, Any]], tuple[RouterDecision, dict[str, Any], dict[str, Any]]],
    router_decision_to_query_classification: Callable[[RouterDecision, dict[str, Any], dict[str, Any], dict[str, Any]], dict[str, Any]],
) -> RouterDecisionNormalizationResult:
    router_decision, llm_media, previous_context_state = build_router_decision(question, history, quota_state, query_profile)
    query_classification = router_decision_to_query_classification(router_decision, llm_media, previous_context_state, query_profile)
    query_classification["previous_date_range"] = previous_context_state.get("date_range", []) if isinstance(previous_context_state, dict) else []
    return RouterDecisionNormalizationResult(
        router_decision=router_decision,
        llm_media=llm_media,
        previous_context_state=dict(previous_context_state or {}),
        query_classification=query_classification,
    )