from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from . import planner_contracts
from ..media.media_policy_rules import is_audiovisual_media, is_bookish_media
from ..media.media_strategy import resolve_media_strategy


@dataclass(frozen=True)
class ExternalEnrichmentDecision:
    allow_external: bool = False
    allow_query_level_tmdb: bool = False
    allow_query_level_wiki: bool = False
    allow_per_item_tmdb: bool = False
    allow_per_item_bangumi: bool = False
    allow_per_item_wiki: bool = False
    max_book_wiki_items: int = 0
    query_focus_entity: str = ""


def decide_external_enrichment(
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
) -> ExternalEnrichmentDecision:
    media_strategy = resolve_media_strategy(
        decision,
        query_class=query_class,
        subject_scope=subject_scope,
        answer_shape=answer_shape,
        media_family=media_family,
        media_type=media_type,
        lookup_mode=lookup_mode,
        needs_explanation=needs_explanation,
        entity_count=entity_count,
    )
    allow_external = media_strategy.should_expand_external
    audiovisual = is_audiovisual_media(decision, media_family=media_family, media_type=media_type)
    bookish = is_bookish_media(decision, media_family=media_family, media_type=media_type)
    resolved_query_class = media_strategy.query_class
    resolved_answer_shape = media_strategy.answer_shape
    resolved_lookup_mode = media_strategy.lookup_mode
    resolved_needs_explanation = media_strategy.needs_explanation
    resolved_entities = [
        str(item).strip()
        for item in list(getattr(decision, "entities", []) or [])
        if str(item).strip()
    ] if decision is not None else []
    resolved_entity_count = int(entity_count if entity_count is not None else len(resolved_entities))
    resolved_subject_scope = media_strategy.subject_scope
    focus_entity = ""
    supplemental_personal_focus = (
        allow_external
        and resolved_subject_scope == planner_contracts.ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD
        and resolved_lookup_mode == "entity_lookup"
        and resolved_needs_explanation
        and resolved_entity_count >= 2
    )
    single_entity_explanation = (
        allow_external
        and resolved_lookup_mode == "entity_lookup"
        and resolved_needs_explanation
        and resolved_entity_count == 1
    )
    if single_entity_explanation:
        focus_entity = str(getattr(decision, "followup_target", "") or "").strip() if decision is not None else ""
        if not focus_entity and resolved_entities:
            focus_entity = resolved_entities[0]
    query_level_detail = single_entity_explanation and bool(focus_entity)
    per_item_expand = allow_external and (
        resolved_answer_shape == planner_contracts.ROUTER_ANSWER_SHAPE_LIST_PLUS_EXPAND
        or supplemental_personal_focus
    )
    query_level_tmdb = query_level_detail and audiovisual
    if (
        allow_external
        and audiovisual
        and resolved_subject_scope == planner_contracts.ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD
        and resolved_answer_shape == planner_contracts.ROUTER_ANSWER_SHAPE_LIST_PLUS_EXPAND
    ):
        query_level_tmdb = True
    return ExternalEnrichmentDecision(
        allow_external=allow_external,
        allow_query_level_tmdb=query_level_tmdb,
        allow_query_level_wiki=query_level_detail,
        allow_per_item_tmdb=per_item_expand and audiovisual,
        allow_per_item_bangumi=per_item_expand and audiovisual,
        allow_per_item_wiki=per_item_expand and (bookish or not audiovisual),
        max_book_wiki_items=2 if per_item_expand and bookish else 0,
        query_focus_entity=focus_entity,
    )