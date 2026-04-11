from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .media_policy_rules import (
    is_personal_review_mode,
    should_expand_external,
    should_run_per_item_expansion,
    should_render_mentions,
)


@dataclass(frozen=True)
class MediaStrategySignals:
    query_class: str = ""
    subject_scope: str = "general_knowledge"
    answer_shape: str = ""
    media_family: str = ""
    media_type: str = ""
    lookup_mode: str = ""
    needs_explanation: bool = False
    entity_count: int = 0
    is_personal_review_mode: bool = False
    should_expand_external: bool = False
    should_run_per_item_expansion: bool = False
    should_render_mentions: bool = True


def resolve_media_strategy(
    decision: Any | None = None,
    *,
    query_class: str = "",
    subject_scope: str = "",
    answer_shape: str = "",
    media_family: str = "",
    media_type: str = "",
    lookup_mode: str = "",
    needs_explanation: bool | None = None,
    entity_count: int | None = None,
) -> MediaStrategySignals:
    resolved_query_class = str(getattr(decision, "query_class", "") or query_class or "").strip()
    resolved_subject_scope = str(getattr(decision, "subject_scope", "") or subject_scope or "general_knowledge").strip()
    resolved_answer_shape = str(getattr(decision, "answer_shape", "") or answer_shape or "").strip()
    resolved_media_family = str(getattr(decision, "media_family", "") or media_family or "").strip()
    resolved_media_type = str(getattr(decision, "media_type", "") or media_type or "").strip()
    resolved_lookup_mode = str(getattr(decision, "lookup_mode", "") or lookup_mode or "").strip()
    resolved_needs_explanation = bool(
        getattr(decision, "needs_explanation", needs_explanation) if decision is not None else needs_explanation
    )
    resolved_entity_count = (
        len(list(getattr(decision, "entities", []) or []))
        if decision is not None
        else int(entity_count or 0)
    )
    personal_review_mode = is_personal_review_mode(
        decision,
        query_class=resolved_query_class,
        subject_scope=resolved_subject_scope,
        answer_shape=resolved_answer_shape,
    )
    expand_external = should_expand_external(
        decision,
        query_class=resolved_query_class,
        subject_scope=resolved_subject_scope,
        answer_shape=resolved_answer_shape,
        lookup_mode=resolved_lookup_mode,
        needs_explanation=resolved_needs_explanation,
        entity_count=resolved_entity_count,
    )
    render_mentions = should_render_mentions(
        decision,
        query_class=resolved_query_class,
        subject_scope=resolved_subject_scope,
        answer_shape=resolved_answer_shape,
        lookup_mode=resolved_lookup_mode,
        needs_explanation=resolved_needs_explanation,
        entity_count=resolved_entity_count,
    )
    run_per_item_expansion = should_run_per_item_expansion(
        decision,
        query_class=resolved_query_class,
        subject_scope=resolved_subject_scope,
        answer_shape=resolved_answer_shape,
        lookup_mode=resolved_lookup_mode,
        needs_explanation=resolved_needs_explanation,
        entity_count=resolved_entity_count,
    )
    return MediaStrategySignals(
        query_class=resolved_query_class,
        subject_scope=resolved_subject_scope,
        answer_shape=resolved_answer_shape,
        media_family=resolved_media_family,
        media_type=resolved_media_type,
        lookup_mode=resolved_lookup_mode,
        needs_explanation=resolved_needs_explanation,
        entity_count=resolved_entity_count,
        is_personal_review_mode=personal_review_mode,
        should_expand_external=expand_external,
        should_run_per_item_expansion=run_per_item_expansion,
        should_render_mentions=render_mentions,
    )

