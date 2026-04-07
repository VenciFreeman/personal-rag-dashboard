from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from .agent_types import AgentRuntimeState


@dataclass(frozen=True)
class GuardrailFlagDeps:
    get_resolved_query_state_from_runtime: Callable[[AgentRuntimeState], dict[str, Any]]
    get_lookup_mode_from_state: Callable[[dict[str, Any]], str]
    get_planner_snapshot_from_runtime: Callable[[AgentRuntimeState], dict[str, Any]]
    question_requests_media_details: Callable[[str], bool]


def build_guardrail_flags(
    *,
    runtime_state: AgentRuntimeState,
    media_validation: dict[str, Any],
    deps: GuardrailFlagDeps,
) -> dict[str, bool]:
    def _is_generic_followup_target(value: str) -> bool:
        target = str(value or "").strip()
        if not target:
            return True
        generic_prefixes = ("这些", "这几", "那几", "这两", "那两")
        generic_values = {
            "这些内容",
            "这些作品",
            "这些媒体",
            "这些动画",
            "这些电影",
            "这些书",
            "这些音乐",
            "这些游戏",
            "这些条目",
        }
        return target in generic_values or any(target.startswith(prefix) for prefix in generic_prefixes)

    original_question = str(runtime_state.decision.raw_question or "").strip()
    resolved_state = deps.get_resolved_query_state_from_runtime(runtime_state)
    router_decision = runtime_state.decision
    carry_over = bool(resolved_state.get("carry_over_from_previous_turn"))
    lookup_mode = deps.get_lookup_mode_from_state(resolved_state)
    router_domain = str(router_decision.domain or "general")
    router_confidence = float(router_decision.confidence or 0.0)
    raw_candidates_count = int(media_validation.get("raw_candidates_count", 0) or 0)
    dropped_by_validator = int(media_validation.get("dropped_by_validator", 0) or 0)
    dropped_by_reference_limit = int(media_validation.get("dropped_by_reference_limit", 0) or 0)
    returned_result_count = int(media_validation.get("returned_result_count", media_validation.get("post_filter_count", 0)) or 0)
    planner_snapshot = deps.get_planner_snapshot_from_runtime(runtime_state)
    hard_filters = planner_snapshot.get("hard_filters") if isinstance(planner_snapshot.get("hard_filters"), dict) else {}
    series_scope = hard_filters.get("series")
    detail_query = deps.question_requests_media_details(str(runtime_state.context_resolution.resolved_question or original_question))
    short_ambiguous_surface = len(original_question) <= 12 and any(token in original_question for token in ("那个", "这个", "那部", "这部", "那本", "这本"))
    title_marked_without_entity = bool(runtime_state.decision.evidence.get("media_title_marked")) and not bool(runtime_state.decision.entities)
    rewritten_media_query = str((runtime_state.decision.rewritten_queries or {}).get("media_query") or "").strip()
    rewrite_stabilized = bool(rewritten_media_query) and rewritten_media_query != original_question
    followup_target = str(getattr(router_decision, "followup_target", "") or resolved_state.get("followup_target") or "").strip()
    anchored_followup = (
        carry_over
        or bool(router_decision.entities)
        or str(router_decision.followup_mode or "none").strip() != "none"
        or (followup_target and not _is_generic_followup_target(followup_target))
    )
    low_confidence_understanding = bool(original_question) and not carry_over and (
        short_ambiguous_surface
        or title_marked_without_entity
        or (
            router_confidence < 0.55
            and (
                router_domain == "general"
                or (
                    len(original_question) <= 12
                    and not runtime_state.decision.entities
                    and not resolved_state.get("filters")
                    and not resolved_state.get("media_type")
                )
            )
        )
    )
    if rewrite_stabilized and router_domain == "media":
        low_confidence_understanding = False
    if router_domain == "tech":
        low_confidence_understanding = False
    if router_domain == "general":
        low_confidence_understanding = False
    if router_domain == "media" and low_confidence_understanding:
        if not (short_ambiguous_surface or title_marked_without_entity):
            low_confidence_understanding = False
    return {
        "low_confidence_understanding": low_confidence_understanding,
        "high_validator_drop_rate": raw_candidates_count > 0 and (dropped_by_validator / raw_candidates_count) >= 0.4,
        "insufficient_valid_results": (
            lookup_mode == "filter_search"
            and raw_candidates_count > 0
            and returned_result_count <= 1
            and not (detail_query and bool(series_scope))
        ),
        "state_inheritance_ambiguous": bool(runtime_state.context_resolution.detected_followup) and not anchored_followup,
        "answer_truncated_by_reference_limit": dropped_by_reference_limit > 0,
    }