"""nav_dashboard/web/services/routing_policy.py

RoutingPolicy — converts a RouterDecision into an ExecutionPlan (the ordered
list of tools the agent should call).

Pure logic: no I/O, no LLM calls.

Design notes:
    · Shared helper/types come from planner_contracts.py and agent_types.py.
    · All dataclass types come from agent_types.py, which is the shared leaf
    module for this package.

Typical usage (inside agent_service)::

    from nav_dashboard.web.services.planner.routing_policy import RoutingPolicy
    plan = RoutingPolicy().build_plan(decision, search_mode)
"""
from __future__ import annotations

from nav_dashboard.web.services.agent.agent_types import (
    TOOL_EXPAND_MEDIAWIKI_CONCEPT,
    TOOL_PARSE_MEDIAWIKI,
    TOOL_QUERY_DOC_RAG,
    TOOL_QUERY_MEDIA,
    TOOL_SEARCH_BY_CREATOR,
    TOOL_SEARCH_TMDB,
    TOOL_SEARCH_WEB,
    ExecutionPlan,
    PlannedToolCall,
    RouterDecision,
)
from .external_enrichment_policy import decide_external_enrichment
from ..media.media_strategy import resolve_media_strategy
from .planner_contracts import (
    ROUTER_ANSWER_SHAPE_LIST_PLUS_EXPAND,
    ROUTER_ANSWER_SHAPE_SUMMARY,
    ROUTER_QUERY_CLASS_MUSIC_WORK_VERSIONS_COMPARE,
    ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD,
    normalize_search_mode,
)


class RoutingPolicy:
    """Stateless planner: RouterDecision + search_mode → ExecutionPlan.

    Tool ordering rules (applied in this order, then deduplicated):
      1.  media_primary      — query_media_record when needs_media_db AND not
                               a creator_collection_query (those use search_by_creator)
      2.  doc_primary/secondary— query_document_rag when needs_doc_rag
                                 (inserted at front for tech or mixed_due_to_entity_plus_tech)
      3.  mediawiki_concept  — expand_mediawiki_concept for abstract media concepts
      4.  creator_primary    — search_by_creator + expand_mediawiki_concept for creator
                               collection queries (replaces query_media_record)
      5.  tmdb_secondary     — search_tmdb_media when needs_external_media_db
      6.  tmdb_entity_detail — search_tmdb_media for single-entity explanation queries
      7.  mediawiki_entity_detail — expand_mediawiki_concept for the same queries
      8.  web_freshness      — search_web when needs_web and mode == "hybrid"
    """

    def build_plan(self, decision: RouterDecision, search_mode: str) -> ExecutionPlan:
        normalized_mode = normalize_search_mode(search_mode)
        planned_tools: list[PlannedToolCall] = []
        reasons: list[str] = []
        rewritten_queries = {
            str(key): str(value)
            for key, value in (decision.rewritten_queries or {}).items()
            if str(key).strip() and str(value).strip()
        }
        media_strategy = resolve_media_strategy(decision)
        enrichment = decide_external_enrichment(decision)
        creator_resolution = (
            decision.evidence.get("creator_resolution")
            if isinstance(decision.evidence.get("creator_resolution"), dict)
            else {}
        )
        alias_scoped_game_compare = bool(
            decision.evidence.get("creator_collection_query")
            and str(creator_resolution.get("match_kind") or "").strip() == "alias"
            and str(creator_resolution.get("media_type_hint") or "").strip() == "game"
            and media_strategy.answer_shape == "compare"
        )
        _creator_collection_primary = bool(
            decision.evidence.get("creator_collection_query")
            and media_strategy.query_class == "personal_media_review_collection"
            and not alias_scoped_game_compare
        )

        # ── 1. media_primary ─────────────────────────────────────────────────
        # Creator collection queries are handled by step 4 (creator_primary) which
        # uses the dedicated search_by_creator tool instead of query_media_record.
        _creator_collection = bool(decision.evidence.get("creator_collection_query"))
        if decision.needs_media_db and not _creator_collection_primary:
            planned_tools.append(
                PlannedToolCall(
                    name=TOOL_QUERY_MEDIA,
                    query=(
                        rewritten_queries.get("media_query")
                        or decision.resolved_question
                        or decision.raw_question
                    ),
                )
            )
            reasons.append("policy:media_primary")

        # ── 2. doc_primary / doc_secondary ───────────────────────────────────
        _query_class = media_strategy.query_class
        _suppress_doc_for_music_compare = _query_class == ROUTER_QUERY_CLASS_MUSIC_WORK_VERSIONS_COMPARE
        _suppress_doc_for_creator_collection = bool(decision.evidence.get("creator_collection_query"))
        if decision.needs_doc_rag and not _suppress_doc_for_music_compare and not _suppress_doc_for_creator_collection:
            is_tech_primary_mixed = decision.arbitration == "mixed_due_to_entity_plus_tech"
            insert_at_front = decision.domain == "tech" or is_tech_primary_mixed
            doc_call = PlannedToolCall(
                name=TOOL_QUERY_DOC_RAG,
                query=(
                    rewritten_queries.get("doc_query")
                    or decision.resolved_question
                    or decision.raw_question
                ),
            )
            if insert_at_front:
                planned_tools.insert(0, doc_call)
                reasons.append("policy:doc_primary")
            else:
                planned_tools.append(doc_call)
                reasons.append("policy:doc_secondary")
        elif decision.needs_doc_rag and _suppress_doc_for_music_compare:
            reasons.append("policy:doc_suppressed_music_work_compare")
        elif decision.needs_doc_rag and _suppress_doc_for_creator_collection:
            reasons.append("policy:doc_suppressed_creator_collection")

        # ── 3. mediawiki_concept ─────────────────────────────────────────────
        structured_personal_filter_query = (
            media_strategy.subject_scope == ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD
            and decision.lookup_mode == "filter_search"
            and bool((decision.filters or {}) or list(decision.date_range or []))
        )
        if (
            decision.domain == "media"
            and bool(decision.evidence.get("abstract_media_concept"))
            and not structured_personal_filter_query
        ):
            planned_tools.insert(
                0,
                PlannedToolCall(
                    name=TOOL_EXPAND_MEDIAWIKI_CONCEPT,
                    query=rewritten_queries.get("media_query") or decision.raw_question,
                ),
            )
            reasons.append("policy:mediawiki_concept")

        # ── 4. creator_primary ───────────────────────────────────────────────
        # For creator collection queries, search_by_creator is the primary local
        # tool (structured creator-index lookup), and expand_mediawiki_concept
        # provides external biography / works list.  query_media_record is NOT
        # used for this path — the dedicated tool already queries the library
        # by author with exact entity resolution, so broad keyword search adds noise.
        if _creator_collection_primary:
            creator_res_info = (
                decision.evidence.get("creator_resolution")
                if isinstance(decision.evidence.get("creator_resolution"), dict)
                else {}
            )
            creator_canonical = str(creator_res_info.get("canonical") or "").strip()
            creator_media_type = str(creator_res_info.get("media_type_hint") or "").strip()
            mw_query = creator_canonical or rewritten_queries.get("media_query") or decision.raw_question
            # search_by_creator as primary local tool
            planned_tools.insert(
                0,
                PlannedToolCall(
                    name=TOOL_SEARCH_BY_CREATOR,
                    query=creator_canonical or decision.raw_question,
                ),
            )
            reasons.append("policy:creator_primary")
            # expand_mediawiki_concept as external enrichment
            if TOOL_EXPAND_MEDIAWIKI_CONCEPT not in {t.name for t in planned_tools}:
                planned_tools.append(
                    PlannedToolCall(name=TOOL_EXPAND_MEDIAWIKI_CONCEPT, query=mw_query)
                )
                reasons.append("policy:mediawiki_creator")
        elif _creator_collection:
            creator_res_info = (
                decision.evidence.get("creator_resolution")
                if isinstance(decision.evidence.get("creator_resolution"), dict)
                else {}
            )
            creator_canonical = str(creator_res_info.get("canonical") or "").strip()
            mw_query = creator_canonical or rewritten_queries.get("media_query") or decision.raw_question
            if TOOL_EXPAND_MEDIAWIKI_CONCEPT not in {t.name for t in planned_tools}:
                planned_tools.append(
                    PlannedToolCall(name=TOOL_EXPAND_MEDIAWIKI_CONCEPT, query=mw_query)
                )
                reasons.append("policy:mediawiki_creator")

        # ── 5. tmdb_secondary ────────────────────────────────────────────────
        # subject_scope=personal_record: the user is querying their own library.
        # Proactive TMDB / MediaWiki enrichment is suppressed by default because
        # the local records are the source of truth for these queries.
        #
        # Exception — if the user explicitly requests expansion (answer_shape is
        # list_plus_expand or detail_card), they want per-item background details
        # even though the query is framed from their personal history:
        #   ✓ "我看过的这几部片子分别讲了什么？" → list_plus_expand → allow enrichment
        #   ✓ "我看的《权游》是什么故事？"     → detail_card   → allow enrichment
        #   ✗ "我近两年看过哪些悬疑剧？"       → list_only     → local-only
        # ── 5a. collection_filter_expand (Phase 1 only) ───────────────────────
        # For media_collection_filter + list_plus_expand queries, external enrichment
        # is a two-phase operation:
        #   Phase 1 (here):   query_media_record retrieves the local item list.
        #   Phase 2 (post-ex): _execute_per_item_expansion fans out a TMDB call per
        #                      title so each item gets its own overview, not one
        #                      whole-query lookup.
        # Deliberately NO proactive TMDB/Wiki call here — passing the whole question
        # to TMDB/Wiki is wrong for "list + explain each" queries.  The fan-out
        # phase in agent_service runs after local results are known.

        if decision.needs_external_media_db and enrichment.allow_query_level_tmdb:
            planned_tools.append(
                PlannedToolCall(
                    name=TOOL_SEARCH_TMDB,
                    query=(
                        rewritten_queries.get("tmdb_query")
                        or decision.resolved_question
                        or decision.raw_question
                    ),
                )
            )
            reasons.append("policy:tmdb_secondary")

        # ── 6–7. entity_detail enrichment ────────────────────────────────────
        # Single-entity explanation queries get TMDB (plot/credits) and
        # MediaWiki (background) so the answer layer has external evidence.
        # Suppressed for personal_record + non-expansion answer shapes.
        #
        # For personal+expand queries, respect enrich priority:
        # Wiki is always added; TMDB only when the content is audiovisual.
        proactive_detail_enrichment = bool(
            enrichment.query_focus_entity
            and decision.domain == "media"
            and (
                media_strategy.subject_scope == ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD
                or media_strategy.answer_shape == ROUTER_ANSWER_SHAPE_SUMMARY
                or media_strategy.answer_shape == ROUTER_ANSWER_SHAPE_LIST_PLUS_EXPAND
                or (
                    media_strategy.lookup_mode == "entity_lookup"
                    and media_strategy.needs_explanation
                    and len(list(getattr(decision, "entities", []) or [])) == 1
                )
            )
        )
        if enrichment.allow_query_level_wiki and proactive_detail_enrichment:
            if TOOL_SEARCH_TMDB not in {t.name for t in planned_tools}:
                if enrichment.allow_query_level_tmdb:
                    planned_tools.append(
                        PlannedToolCall(
                            name=TOOL_SEARCH_TMDB,
                            query=rewritten_queries.get("tmdb_query") or enrichment.query_focus_entity,
                        )
                    )
                    reasons.append("policy:tmdb_entity_detail")
            media_family = str(getattr(decision, "media_family", "") or "")
            wiki_query = enrichment.query_focus_entity or rewritten_queries.get("media_query") or decision.raw_question
            if media_family == "bookish":
                if TOOL_PARSE_MEDIAWIKI not in {t.name for t in planned_tools}:
                    planned_tools.append(
                        PlannedToolCall(
                            name=TOOL_PARSE_MEDIAWIKI,
                            query=wiki_query,
                        )
                    )
                    reasons.append("policy:mediawiki_entity_detail_parse")
            elif TOOL_EXPAND_MEDIAWIKI_CONCEPT not in {t.name for t in planned_tools}:
                planned_tools.append(
                    PlannedToolCall(
                        name=TOOL_EXPAND_MEDIAWIKI_CONCEPT,
                        query=wiki_query,
                    )
                )
                reasons.append("policy:mediawiki_entity_detail")

        # ── 8. web_freshness ─────────────────────────────────────────────────
        if decision.needs_web and normalized_mode == "hybrid":
            planned_tools.append(
                PlannedToolCall(
                    name=TOOL_SEARCH_WEB,
                    query=(
                        rewritten_queries.get("web_query")
                        or decision.resolved_question
                        or decision.raw_question
                    ),
                )
            )
            reasons.append("policy:web_freshness")

        # ── deduplicate (keep first occurrence) ──────────────────────────────
        deduped: list[PlannedToolCall] = []
        seen: set[str] = set()
        for call in planned_tools:
            if call.name in seen:
                continue
            seen.add(call.name)
            deduped.append(call)

        primary_tool = deduped[0].name if deduped else ""
        fallback_tools = [call.name for call in deduped[1:]]
        return ExecutionPlan(
            decision=decision,
            planned_tools=deduped,
            primary_tool=primary_tool,
            fallback_tools=fallback_tools,
            reasons=reasons,
        )
