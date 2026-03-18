"""nav_dashboard/web/services/routing_policy.py

RoutingPolicy — converts a RouterDecision into an ExecutionPlan (the ordered
list of tools the agent should call).

Pure logic: no I/O, no LLM calls.

Design notes:
  · Imports runtime helpers from agent_service lazily (inside build_plan) to
    avoid a circular import at module load time — agent_service imports this
    module at the top level.
  · All dataclass types come from agent_types.py, which is the shared leaf
    module for this package.

Typical usage (inside agent_service)::

    from nav_dashboard.web.services.routing_policy import RoutingPolicy
    plan = RoutingPolicy().build_plan(decision, search_mode)
"""
from __future__ import annotations

from nav_dashboard.web.services.agent_types import (
    TOOL_EXPAND_MEDIAWIKI_CONCEPT,
    TOOL_QUERY_DOC_RAG,
    TOOL_QUERY_MEDIA,
    TOOL_SEARCH_BY_CREATOR,
    TOOL_SEARCH_TMDB,
    TOOL_SEARCH_WEB,
    ExecutionPlan,
    PlannedToolCall,
    RouterDecision,
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
        # Lazy imports to break the circular dependency with agent_service.
        from nav_dashboard.web.services.agent_service import (  # noqa: PLC0415
            _build_media_tool_options_from_decision,
            _normalize_search_mode,
        )

        normalized_mode = _normalize_search_mode(search_mode)
        planned_tools: list[PlannedToolCall] = []
        reasons: list[str] = []
        rewritten_queries = {
            str(key): str(value)
            for key, value in (decision.rewritten_queries or {}).items()
            if str(key).strip() and str(value).strip()
        }

        # ── 1. media_primary ─────────────────────────────────────────────────
        # Creator collection queries are handled by step 4 (creator_primary) which
        # uses the dedicated search_by_creator tool instead of query_media_record.
        _creator_collection = bool(decision.evidence.get("creator_collection_query"))
        if decision.needs_media_db and not _creator_collection:
            planned_tools.append(
                PlannedToolCall(
                    name=TOOL_QUERY_MEDIA,
                    query=(
                        rewritten_queries.get("media_query")
                        or decision.resolved_question
                        or decision.raw_question
                    ),
                    options=_build_media_tool_options_from_decision(decision),
                )
            )
            reasons.append("policy:media_primary")

        # ── 2. doc_primary / doc_secondary ───────────────────────────────────
        _query_class = str(getattr(decision, "query_class", "") or "")
        _suppress_doc_for_music_compare = _query_class == "music_work_versions_compare"
        if decision.needs_doc_rag and not _suppress_doc_for_music_compare:
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

        # ── 3. mediawiki_concept ─────────────────────────────────────────────
        if decision.domain == "media" and bool(decision.evidence.get("abstract_media_concept")):
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
        if _creator_collection:
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
                    options={
                        "creator_name": creator_canonical,
                        "media_type": creator_media_type,
                    },
                ),
            )
            reasons.append("policy:creator_primary")
            # expand_mediawiki_concept as external enrichment
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
        _subject_scope = str(getattr(decision, "subject_scope", "") or "")
        _is_personal = _subject_scope == "personal_record"
        _answer_shape = str(getattr(decision, "answer_shape", "") or "")
        # Suppress steps 5–7 for personal queries UNLESS the user explicitly
        # wants expansion/detail (in which case external evidence is requested).
        _personal_suppress_external = _is_personal and _answer_shape not in {"list_plus_expand", "detail_card"}
        # media_family is a shared contract field derived once in agent_service.
        # TMDB is only relevant for audiovisual content; avoid it for books, music, etc.
        _media_family = str(getattr(decision, "media_family", "") or "")
        _is_audiovisual = _media_family == "audiovisual"
        _personal_expand = _is_personal and _answer_shape in {"list_plus_expand", "detail_card"}

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

        if decision.needs_external_media_db and not _personal_suppress_external:
            # For personal+expand with non-audiovisual content, skip TMDB entirely.
            if _personal_expand and not _is_audiovisual:
                # Skip TMDB; rely on MediaWiki via step 6-7 or the creator enrichment path.
                pass
            else:
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
        if (
            not _personal_suppress_external
            and decision.domain == "media"
            and decision.lookup_mode == "entity_lookup"
            and bool(getattr(decision, "needs_explanation", False))
            and len(decision.entities) == 1
        ):
            if TOOL_SEARCH_TMDB not in {t.name for t in planned_tools}:
                # For personal+expand with non-audiovisual content, skip TMDB.
                if not _personal_expand or _is_audiovisual:
                    planned_tools.append(
                        PlannedToolCall(
                            name=TOOL_SEARCH_TMDB,
                            query=rewritten_queries.get("tmdb_query") or decision.entities[0],
                        )
                    )
                    reasons.append("policy:tmdb_entity_detail")
            if TOOL_EXPAND_MEDIAWIKI_CONCEPT not in {t.name for t in planned_tools}:
                planned_tools.append(
                    PlannedToolCall(
                        name=TOOL_EXPAND_MEDIAWIKI_CONCEPT,
                        query=rewritten_queries.get("media_query") or decision.entities[0],
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
