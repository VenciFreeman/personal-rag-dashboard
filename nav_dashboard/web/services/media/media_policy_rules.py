from __future__ import annotations

from typing import Any

from nav_dashboard.web.services.planner import planner_contracts


_PERSONAL_REVIEW_EXPAND_SHAPES = {
    planner_contracts.ROUTER_ANSWER_SHAPE_LIST_ONLY,
    planner_contracts.ROUTER_ANSWER_SHAPE_LIST_PLUS_EXPAND,
}
_PERSONAL_EXTERNAL_ALLOWED_SHAPES = {
    planner_contracts.ROUTER_ANSWER_SHAPE_LIST_PLUS_EXPAND,
    planner_contracts.ROUTER_ANSWER_SHAPE_DETAIL_CARD,
}
_EXPLICIT_EXTERNAL_REFERENCE_CUES = (
    "外部参考",
    "外部资料",
    "外部来源",
    "参考来源",
    "外部链接",
    "维基",
    "wiki",
    "wikipedia",
    "tmdb",
)
_AUDIOVISUAL_MEDIA_TYPES = {"video", "movie", "tv", "anime", "作品"}
_BOOKISH_MEDIA_TYPES = {"book", "图书"}


def _resolve_signal(decision: Any | None, attr_name: str, fallback: str = "") -> str:
    return str(getattr(decision, attr_name, "") or fallback or "").strip()


def requests_explicit_external_reference(decision: Any | None = None) -> bool:
    text = str(getattr(decision, "raw_question", "") or "").strip().casefold()
    if not text:
        return False
    return any(cue.casefold() in text for cue in _EXPLICIT_EXTERNAL_REFERENCE_CUES)


def is_personal_review_mode(
    decision: Any | None = None,
    *,
    query_class: str = "",
    subject_scope: str = "",
    answer_shape: str = "",
) -> bool:
    resolved_query_class = _resolve_signal(decision, "query_class", query_class)
    resolved_subject_scope = _resolve_signal(decision, "subject_scope", subject_scope)
    resolved_answer_shape = _resolve_signal(decision, "answer_shape", answer_shape)
    return resolved_query_class == planner_contracts.ROUTER_QUERY_CLASS_PERSONAL_MEDIA_REVIEW_COLLECTION or (
        resolved_subject_scope == planner_contracts.ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD
        and resolved_answer_shape in _PERSONAL_REVIEW_EXPAND_SHAPES
    )


def should_render_mentions(
    decision: Any | None = None,
    *,
    query_class: str = "",
    subject_scope: str = "",
    answer_shape: str = "",
    lookup_mode: str = "",
    needs_explanation: bool | None = None,
    entity_count: int | None = None,
) -> bool:
    if not is_personal_review_mode(
        decision,
        query_class=query_class,
        subject_scope=subject_scope,
        answer_shape=answer_shape,
    ):
        return True
    return False


def has_personal_review_supplemental_focus(
    decision: Any | None = None,
    *,
    query_class: str = "",
    subject_scope: str = "",
    answer_shape: str = "",
    lookup_mode: str = "",
    needs_explanation: bool | None = None,
    entity_count: int | None = None,
) -> bool:
    if not is_personal_review_mode(
        decision,
        query_class=query_class,
        subject_scope=subject_scope,
        answer_shape=answer_shape,
    ):
        return False
    resolved_lookup_mode = _resolve_signal(decision, "lookup_mode", lookup_mode)
    resolved_needs_explanation = bool(
        getattr(decision, "needs_explanation", needs_explanation) if decision is not None else needs_explanation
    )
    resolved_entity_count = len(list(getattr(decision, "entities", []) or [])) if decision is not None else int(entity_count or 0)
    return (
        resolved_lookup_mode == "entity_lookup"
        and resolved_needs_explanation
        and resolved_entity_count >= 2
    )


def should_expand_external(
    decision: Any | None = None,
    *,
    query_class: str = "",
    subject_scope: str = "",
    answer_shape: str = "",
    lookup_mode: str = "",
    needs_explanation: bool | None = None,
    entity_count: int | None = None,
) -> bool:
    explicit_external_reference = requests_explicit_external_reference(decision)
    if is_personal_review_mode(
        decision,
        query_class=query_class,
        subject_scope=subject_scope,
        answer_shape=answer_shape,
    ):
        resolved_answer_shape = _resolve_signal(decision, "answer_shape", answer_shape)
        if resolved_answer_shape in _PERSONAL_EXTERNAL_ALLOWED_SHAPES or explicit_external_reference:
            return True
        return has_personal_review_supplemental_focus(
            decision,
            query_class=query_class,
            subject_scope=subject_scope,
            answer_shape=answer_shape,
            lookup_mode=lookup_mode,
            needs_explanation=needs_explanation,
            entity_count=entity_count,
        )
    resolved_subject_scope = _resolve_signal(decision, "subject_scope", subject_scope)
    resolved_answer_shape = _resolve_signal(decision, "answer_shape", answer_shape)
    resolved_lookup_mode = _resolve_signal(decision, "lookup_mode", lookup_mode)
    resolved_needs_explanation = bool(
        getattr(decision, "needs_explanation", needs_explanation) if decision is not None else needs_explanation
    )
    resolved_entity_count = len(list(getattr(decision, "entities", []) or [])) if decision is not None else int(entity_count or 0)
    if resolved_subject_scope == planner_contracts.ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD:
        return resolved_answer_shape in _PERSONAL_EXTERNAL_ALLOWED_SHAPES or explicit_external_reference
    if resolved_answer_shape == planner_contracts.ROUTER_ANSWER_SHAPE_LIST_PLUS_EXPAND:
        return True
    if resolved_lookup_mode == "entity_lookup" and resolved_needs_explanation and resolved_entity_count == 1:
        return True
    return False


def should_run_per_item_expansion(
    decision: Any | None = None,
    *,
    query_class: str = "",
    subject_scope: str = "",
    answer_shape: str = "",
    lookup_mode: str = "",
    needs_explanation: bool | None = None,
    entity_count: int | None = None,
) -> bool:
    if not should_expand_external(
        decision,
        query_class=query_class,
        subject_scope=subject_scope,
        answer_shape=answer_shape,
        lookup_mode=lookup_mode,
        needs_explanation=needs_explanation,
        entity_count=entity_count,
    ):
        return False
    resolved_answer_shape = _resolve_signal(decision, "answer_shape", answer_shape)
    if resolved_answer_shape == planner_contracts.ROUTER_ANSWER_SHAPE_LIST_PLUS_EXPAND:
        return True
    return has_personal_review_supplemental_focus(
        decision,
        query_class=query_class,
        subject_scope=subject_scope,
        answer_shape=answer_shape,
        lookup_mode=lookup_mode,
        needs_explanation=needs_explanation,
        entity_count=entity_count,
    )


def should_render_external_appendix(
    decision: Any | None = None,
    *,
    query_class: str = "",
    subject_scope: str = "",
    answer_shape: str = "",
    lookup_mode: str = "",
    needs_explanation: bool | None = None,
    entity_count: int | None = None,
) -> bool:
    return should_expand_external(
        decision,
        query_class=query_class,
        subject_scope=subject_scope,
        answer_shape=answer_shape,
        lookup_mode=lookup_mode,
        needs_explanation=needs_explanation,
        entity_count=entity_count,
    )


def is_audiovisual_media(
    decision: Any | None = None,
    *,
    media_family: str = "",
    media_type: str = "",
) -> bool:
    resolved_family = _resolve_signal(decision, "media_family", media_family)
    resolved_type = _resolve_signal(decision, "media_type", media_type).lower()
    return resolved_family == planner_contracts.ROUTER_MEDIA_FAMILY_AUDIOVISUAL or resolved_type in _AUDIOVISUAL_MEDIA_TYPES


def is_bookish_media(
    decision: Any | None = None,
    *,
    media_family: str = "",
    media_type: str = "",
) -> bool:
    resolved_family = _resolve_signal(decision, "media_family", media_family)
    resolved_type = _resolve_signal(decision, "media_type", media_type).lower()
    return resolved_family == planner_contracts.ROUTER_MEDIA_FAMILY_BOOKISH or resolved_type in _BOOKISH_MEDIA_TYPES

