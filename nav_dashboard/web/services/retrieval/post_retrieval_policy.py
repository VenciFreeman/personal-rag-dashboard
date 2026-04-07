"""PostRetrievalPolicy — evaluates tool results after retrieval and decides
how the answer layer should proceed.

Design principles:
- Pure data-in / dataclass-out: no I/O, no LLM calls.
- Imports only stdlib + types from agent_service to stay lightweight.
- agent_service continues to own _build_guardrail_flags; this module
  **consumes** that output (guardrail_flags dict) rather than recomputing it.

Typical usage inside agent_service::

    from nav_dashboard.web.services.retrieval.post_retrieval_policy import (
        PostRetrievalPolicy, PostRetrievalOutcome,
    )
    outcome = PostRetrievalPolicy().evaluate(decision, tool_results, guardrail_flags)
    query_classification["post_retrieval_outcome"] = dataclasses.asdict(outcome)
"""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, TYPE_CHECKING

from ..planner import planner_contracts as planner_contracts
from ..planner.external_enrichment_policy import decide_external_enrichment
from ..media.media_strategy import resolve_media_strategy
from ..ontologies.music_work_signature import (
    title_matches_music_work_signature,
)
from ..ontologies.music_ontology import (
    is_form_alias,
    is_instrument_alias,
    is_work_family_alias,
)

try:
    from ..ontologies.video_ontology import (
        is_genre_alias as is_video_genre_alias,
        is_entity_alias as is_video_entity_alias,
        collect_video_ontology_hints as _collect_video_hints,
    )
except Exception:  # noqa: BLE001
    def is_video_genre_alias(token: str) -> bool:  # type: ignore[misc]
        return False
    def is_video_entity_alias(token: str) -> bool:  # type: ignore[misc]
        return False
    def _collect_video_hints(text: str) -> dict:  # type: ignore[misc]
        return {}

try:
    from ..ontologies.book_ontology import (
        is_genre_alias as is_book_genre_alias,
        is_entity_alias as is_book_entity_alias,
        collect_book_ontology_hints as _collect_book_hints,
    )
except Exception:  # noqa: BLE001
    def is_book_genre_alias(token: str) -> bool:  # type: ignore[misc]
        return False
    def is_book_entity_alias(token: str) -> bool:  # type: ignore[misc]
        return False
    def _collect_book_hints(text: str) -> dict:  # type: ignore[misc]
        return {}

if TYPE_CHECKING:
    # Avoid circular import at runtime; only used for type hints.
    from ..agent.agent_types import RouterDecision, ToolExecution

# Tool name constants (mirrored to avoid circular import at module load time).
_TOOL_QUERY_DOC_RAG = "query_document_rag"
_TOOL_QUERY_MEDIA = "query_media_record"
_TOOL_SEARCH_WEB = "search_web"
_TOOL_SEARCH_TMDB = "search_tmdb_media"  # must match agent_service.TOOL_SEARCH_TMDB
_TOOL_SEARCH_BY_CREATOR = "search_by_creator"
_TOOL_PARSE_MEDIAWIKI = "parse_mediawiki_page"

# Repair tools offered when results are weak / zero.
_REPAIR_TOOL_MEDIAWIKI = "expand_mediawiki_concept"
_REPAIR_TOOL_WEB = "search_web"
_REPAIR_TOOL_TMDB = "search_tmdb_media"  # must match agent_service.TOOL_SEARCH_TMDB


@dataclass
class PostRetrievalOutcome:
    """Structured signal for the answer layer on what to do with retrieved results.

    Attributes:
        status:         One of "proceed", "zero_results", "weak_results",
                        "off_domain", "conflicting", "partial_local".
        action:         What the answer layer should do:
                        "use_results" | "fallback_to_llm" | "degrade" | "enrich".
        repair_tools:   Suggested additional tools to run before answering
                        (empty list if none needed).
        degrade_reason: Human-readable reason when action == "degrade".
        zero_results:   Convenience flag — True when retrieval returned nothing.
        weak_results:   Convenience flag — True for partial / low-quality results.
        off_domain:     Convenience flag — guardrail detected off-domain routing.
        partial_local:  Convenience flag — local results exist but are incomplete;
                        external enrichment is recommended.
    """

    status: str = "proceed"
    action: str = "use_results"
    repair_tools: list[str] = field(default_factory=list)
    degrade_reason: str = ""
    zero_results: bool = False
    weak_results: bool = False
    off_domain: bool = False
    partial_local: bool = False
    # True when the query semantics and the retrieved results are misaligned:
    # e.g. a creator_collection_query where search_by_creator found nothing,
    # or a broad keyword topk returned for what should be a targeted lookup.
    semantic_mismatch: bool = False
    # True when the query requests per-item expansion ("分别介绍", "详细说说…")
    # but no expansion tool (TMDB / MediaWiki) was included in the plan.
    # The answer layer should note that each item may lack detail.
    needs_expansion: bool = False
    # expansion_missing: True when expansion was requested but no expansion
    # tool ran at all (T1 — tools were absent from the plan).
    expansion_missing: bool = False
    # expansion_unavailable: True when the policy has no further repair path
    # to suggest (MediaWiki ran dry AND TMDB is ineligible or also dry).
    # The answer layer should tell the user expansion details cannot be fetched.
    # Note: this does NOT imply a retry happened — it means all known avenues
    # are exhausted according to current policy.
    expansion_unavailable: bool = False


class PostRetrievalPolicy:
    """Stateless policy: given the router decision, tool results, and pre-computed
    guardrail flags, classify the retrieval outcome and advise the answer layer.

    Four non-exclusive outcome classes (evaluated in priority order):
      1. zero_results   — retrieval returned no usable records.
      2. off_domain     — guardrail flagged ambiguous understanding.
      3. weak_results   — some results but below quality bar.
      4. conflicting    — multiple tools returned incompatible signals.
    """

    def evaluate(
        self,
        decision: "RouterDecision",
        tool_results: list["ToolExecution"],
        guardrail_flags: dict[str, Any],
    ) -> PostRetrievalOutcome:
        media_strategy = resolve_media_strategy(decision)
        domain = str(getattr(decision, "domain", "general") or "general")
        query_class = media_strategy.query_class
        media_family = media_strategy.media_family
        answer_shape = media_strategy.answer_shape
        # Pre-compute whether this query requests per-item expansion but
        # expansion tools are absent, ran dry, or returned weak data.
        # Three tiers:
        #   T1: expansion tool(s) did not run at all
        #   T2: expansion tool(s) ran but returned no usable rows
        #   T3: expansion tool(s) ran with some data (quality assessed elsewhere)
        # _needs_expansion is True for T1 and T2.
        _expansion_cues = (
            "介绍", "展开", "分别说说", "详细说", "分别介绍", "说说",
            "分别是讲什么的", "讲什么的", "讲了什么", "是什么内容", "什么故事",
            "都是什么", "各自讲", "概述", "是讲什么",
        )
        _raw_q = str(getattr(decision, "raw_question", "") or "")
        _collection_classes = {
            planner_contracts.ROUTER_QUERY_CLASS_MEDIA_CREATOR_COLLECTION,
            planner_contracts.ROUTER_QUERY_CLASS_MEDIA_ABSTRACT_CONCEPT,
            planner_contracts.ROUTER_QUERY_CLASS_MEDIA_COLLECTION_FILTER,
        }
        _wants_expansion = bool(
            media_strategy.should_run_per_item_expansion
            or (
                query_class in _collection_classes
                and any(cue in _raw_q for cue in _expansion_cues)
            )
        )
        _mediawiki_ran_flag = _mediawiki_ran(tool_results)
        _tmdb_ran_flag = _tmdb_ran(tool_results)
        _expansion_ran = _mediawiki_ran_flag or _tmdb_ran_flag
        # Check if expansion tools ran but returned empty/weak data (T2).
        _expansion_ran_dry = _expansion_ran and not _get_expansion_rows(tool_results)
        _needs_expansion = _wants_expansion and (not _expansion_ran or _expansion_ran_dry)
        lookup_mode = media_strategy.lookup_mode
        entities = list(getattr(decision, "entities", None) or [])
        creator_collection = bool(
            (getattr(decision, "evidence", None) or {}).get("creator_collection_query")
        )
        external_enrichment = decide_external_enrichment(
            decision,
            query_class=media_strategy.query_class,
            subject_scope=media_strategy.subject_scope,
            answer_shape=media_strategy.answer_shape,
            media_family=media_strategy.media_family,
            media_type=media_strategy.media_type,
            lookup_mode=media_strategy.lookup_mode,
            needs_explanation=media_strategy.needs_explanation,
            entity_count=media_strategy.entity_count,
        )

        media_rows = _get_rows(_TOOL_QUERY_MEDIA, tool_results)
        doc_rows = _get_rows(_TOOL_QUERY_DOC_RAG, tool_results)
        creator_rows = _get_creator_rows(tool_results)
        media_ran = _tool_ran(_TOOL_QUERY_MEDIA, tool_results)
        doc_ran = _tool_ran(_TOOL_QUERY_DOC_RAG, tool_results)
        creator_ran = _tool_ran(_TOOL_SEARCH_BY_CREATOR, tool_results)
        # For creator queries, the "effective" local result set is search_by_creator.
        effective_local_rows = creator_rows if creator_ran else media_rows

        # ── semantic mismatch: music work versions compare ───────────────────
        # For queries like "Tchaikovsky violin concerto 版本比较", local rows must
        # satisfy title-level composer + work signature constraints. Otherwise,
        # do not trust the result set as a valid compare base.
        music_hints = _get_music_work_hints(decision)
        is_music_versions_compare = query_class == planner_contracts.ROUTER_QUERY_CLASS_MUSIC_WORK_VERSIONS_COMPARE
        if is_music_versions_compare and effective_local_rows:
            matched_rows = _rows_match_music_signature(
                effective_local_rows,
                composer_hints=music_hints["composer_hints"],
                work_signature=music_hints["work_signature"],
                instrument_hints=music_hints["instrument_hints"],
                form_hints=music_hints["form_hints"],
                work_family_hints=music_hints["work_family_hints"],
            )
            if not matched_rows:
                return PostRetrievalOutcome(
                    status="weak_results",
                    action="enrich",
                    repair_tools=[_REPAIR_TOOL_WEB],
                    degrade_reason="music_work_signature_mismatch",
                    weak_results=True,
                    semantic_mismatch=True,
                )

        # ── semantic mismatch: video/book genre filter vs result categories ─
        # When the query explicitly filters for a video or book genre (via
        # ontology alias matching) and ALL local rows lack that genre in their
        # category/tag fields, flag a mismatch so the answer layer can enrich.
        if (
            effective_local_rows
            and media_family in ("audiovisual", "bookish")
            and lookup_mode == "filter_search"
        ):
            _genre_mismatch = _check_domain_genre_mismatch(
                decision, effective_local_rows, media_family,
            )
            if _genre_mismatch:
                return PostRetrievalOutcome(
                    status="weak_results",
                    action="enrich",
                    repair_tools=[_REPAIR_TOOL_MEDIAWIKI],
                    degrade_reason="genre_filter_mismatch",
                    weak_results=True,
                    semantic_mismatch=True,
                )

        # ── semantic mismatch: video/book entity alias vs retrieved rows ──────
        # When the query names a known entity alias (franchise, series, studio)
        # from the ontology, at least one retrieved row must mention it in its
        # title/category/tags.  Only fires on entity_aliases matches (NOT
        # creator or genre), and is skipped for creator-collection queries where
        # the result set is creator records, not media titles.
        if effective_local_rows and not creator_ran and media_family in ("audiovisual", "bookish"):
            _domain_hints = _get_domain_entity_hints(_raw_q, media_family)
            _entity_hints_list = list(_domain_hints.get("entity_hints") or [])
            if _entity_hints_list:
                if _check_entity_alias_mismatch(effective_local_rows, _entity_hints_list):
                    return PostRetrievalOutcome(
                        status="weak_results",
                        action="enrich",
                        repair_tools=[_REPAIR_TOOL_MEDIAWIKI],
                        degrade_reason="entity_alias_mismatch",
                        weak_results=True,
                        semantic_mismatch=True,
                    )

        # ── 1. zero_results ──────────────────────────────────────────────────
        # For creator queries, zero means search_by_creator returned nothing.
        if creator_ran and not creator_rows:
            return PostRetrievalOutcome(
                status="zero_results",
                action="enrich",
                repair_tools=[_REPAIR_TOOL_MEDIAWIKI],
                degrade_reason="creator_not_in_local_library",
                zero_results=True,
                semantic_mismatch=True,
            )

        if media_ran and not media_rows and not doc_rows and not creator_rows:
            repair: list[str] = []
            single_entity_lookup = domain == "media" and lookup_mode == "entity_lookup" and len(entities) == 1
            if single_entity_lookup:
                repair.append(_REPAIR_TOOL_TMDB)
                if external_enrichment.allow_query_level_wiki:
                    repair.append(_TOOL_PARSE_MEDIAWIKI if media_family == "bookish" else _REPAIR_TOOL_MEDIAWIKI)
            elif domain == "media" and external_enrichment.allow_query_level_tmdb:
                repair.append(_REPAIR_TOOL_TMDB)
            elif domain == "media" and external_enrichment.allow_query_level_wiki:
                repair.append(_TOOL_PARSE_MEDIAWIKI if media_family == "bookish" else _REPAIR_TOOL_MEDIAWIKI)
            elif domain == "media":
                repair.append(_REPAIR_TOOL_WEB)
            return PostRetrievalOutcome(
                status="zero_results",
                action="fallback_to_llm" if not repair else "enrich",
                repair_tools=repair,
                degrade_reason="no_retrieval_results",
                zero_results=True,
            )

        if doc_ran and not doc_rows and not media_rows:
            return PostRetrievalOutcome(
                status="zero_results",
                action="fallback_to_llm",
                repair_tools=[],
                degrade_reason="doc_no_context",
                zero_results=True,
            )

        # ── 2. off_domain (guardrail signals) ────────────────────────────────
        if guardrail_flags.get("low_confidence_understanding") or guardrail_flags.get("state_inheritance_ambiguous"):
            return PostRetrievalOutcome(
                status="off_domain",
                action="degrade",
                repair_tools=[],
                degrade_reason=_first_true_flag(
                    guardrail_flags,
                    ("state_inheritance_ambiguous", "low_confidence_understanding"),
                ),
                off_domain=True,
            )

        # ── 3. weak_results ──────────────────────────────────────────────────
        if guardrail_flags.get("insufficient_valid_results") or guardrail_flags.get("high_validator_drop_rate"):
            repair_for_weak: list[str] = []
            if domain == "media" and not effective_local_rows:
                repair_for_weak.append(_REPAIR_TOOL_WEB)
            return PostRetrievalOutcome(
                status="weak_results",
                action="enrich" if repair_for_weak else "use_results",
                repair_tools=repair_for_weak,
                degrade_reason=_first_true_flag(
                    guardrail_flags,
                    ("insufficient_valid_results", "high_validator_drop_rate"),
                ),
                weak_results=True,
            )

        # ── 4. enrich_when_local_is_partial ──────────────────────────────────
        # Local results exist but the query warrants external enrichment:
        #   • entity_lookup + needs_explanation → TMDB (plot/credits) + MediaWiki (background)
        #   • creator_collection query → MediaWiki works list / biography
        #   • needs_expansion (T1/T2) → add the missing expansion tool so the
        #     answer layer can get per-item detail rather than just prompting the LLM
        if effective_local_rows and domain == "media":
            suppress_bookish_list_wiki = (
                media_family == planner_contracts.ROUTER_MEDIA_FAMILY_BOOKISH
                and answer_shape == planner_contracts.ROUTER_ANSWER_SHAPE_LIST_PLUS_EXPAND
            )
            suppress_bookish_detail_wiki = (
                media_family == planner_contracts.ROUTER_MEDIA_FAMILY_BOOKISH
                and answer_shape == planner_contracts.ROUTER_ANSWER_SHAPE_DETAIL_CARD
                and media_strategy.subject_scope != planner_contracts.ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD
                and _bookish_detail_row_is_sufficient(effective_local_rows)
            )
            collection_expand_without_focus = (
                answer_shape in {
                    planner_contracts.ROUTER_ANSWER_SHAPE_LIST_PLUS_EXPAND,
                    planner_contracts.ROUTER_ANSWER_SHAPE_SUMMARY,
                }
                and lookup_mode in {"filter_search", "general_lookup"}
                and len(entities) != 1
                and not creator_collection
                and media_strategy.subject_scope != planner_contracts.ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD
            )
            repair_partial: list[str] = []
            if not collection_expand_without_focus and external_enrichment.allow_query_level_tmdb and not _tmdb_ran(tool_results):
                repair_partial.append(_REPAIR_TOOL_TMDB)
            if (
                not collection_expand_without_focus
                and not suppress_bookish_detail_wiki
                and external_enrichment.allow_query_level_wiki
                and not _mediawiki_ran(tool_results)
            ):
                repair_partial.append(_TOOL_PARSE_MEDIAWIKI if media_family == "bookish" else _REPAIR_TOOL_MEDIAWIKI)
            if creator_collection and not _mediawiki_ran(tool_results):
                if _REPAIR_TOOL_MEDIAWIKI not in repair_partial:
                    repair_partial.append(_REPAIR_TOOL_MEDIAWIKI)
            # needs_expansion repair: expansion was wanted (T1 or T2) — try the
            # complementary tool that hasn't run or ran dry.
            # Query-class-aware: TMDB is not a universal fallback.
            # creator and abstract-concept queries (books, literature, history,
            # person biographies) should only use MediaWiki; TMDB is for
            # audiovisual titles and would return irrelevant results otherwise.
            _WIKI_ONLY_QUERY_CLASSES = {
                planner_contracts.ROUTER_QUERY_CLASS_MEDIA_CREATOR_COLLECTION,
                planner_contracts.ROUTER_QUERY_CLASS_MEDIA_ABSTRACT_CONCEPT,
            }
            _is_wiki_only_class = query_class in _WIKI_ONLY_QUERY_CLASSES
            if _needs_expansion and not collection_expand_without_focus:
                if (
                    external_enrichment.allow_per_item_tmdb
                    and not _is_wiki_only_class
                    and not _tmdb_ran_flag
                    and _REPAIR_TOOL_TMDB not in repair_partial
                ):
                    repair_partial.append(_REPAIR_TOOL_TMDB)
                if (
                    external_enrichment.allow_per_item_wiki
                    and not suppress_bookish_list_wiki
                    and not _mediawiki_ran_flag
                    and _REPAIR_TOOL_MEDIAWIKI not in repair_partial
                ):
                    repair_partial.append(_REPAIR_TOOL_MEDIAWIKI)
            # Determine expansion state flags for the answer layer:
            #   expansion_missing = T1: wanted but nothing ran at all.
            #   expansion_repair_attempted_and_failed = T2 with no repair path left
            #     (repair_partial is empty, meaning all expansion avenues exhausted).
            _expansion_missing = _wants_expansion and not _expansion_ran
            _expansion_unavailable = (
                _wants_expansion
                and _expansion_ran
                and _expansion_ran_dry
                and not repair_partial  # no further repair recommended → exhausted
            )
            if repair_partial or _expansion_missing or _expansion_unavailable:
                return PostRetrievalOutcome(
                    status="partial_local",
                    action="enrich" if repair_partial else "use_results",
                    repair_tools=repair_partial,
                    partial_local=True,
                    needs_expansion=_needs_expansion,
                    expansion_missing=_expansion_missing,
                    expansion_unavailable=_expansion_unavailable,
                )

        # ── 5. conflicting ───────────────────────────────────────────────────
        # When both doc and media returned results for a non-mixed query the
        # answer layer should prefer the domain-primary tool but note conflict.
        if doc_rows and media_rows and domain not in ("mixed", "media"):
            return PostRetrievalOutcome(
                status="conflicting",
                action="use_results",
                repair_tools=[],
                degrade_reason="doc_and_media_both_returned_for_non_mixed_domain",
            )

        # ── default: all clear ───────────────────────────────────────────────
        return PostRetrievalOutcome(status="proceed", action="use_results", needs_expansion=_needs_expansion)

# ── helpers ───────────────────────────────────────────────────────────────────

def _tmdb_ran(tool_results: list["ToolExecution"]) -> bool:
    return _tool_ran(_TOOL_SEARCH_TMDB, tool_results)


def _mediawiki_ran(tool_results: list["ToolExecution"]) -> bool:
    return _tool_ran(_REPAIR_TOOL_MEDIAWIKI, tool_results) or _tool_ran(_TOOL_PARSE_MEDIAWIKI, tool_results)


def _get_creator_rows(tool_results: list["ToolExecution"]) -> list[dict]:
    """Return results rows from search_by_creator tool if it ran and found works."""
    for item in tool_results:
        if item.tool == _TOOL_SEARCH_BY_CREATOR and isinstance(item.data, dict):
            rows = item.data.get("results")
            if isinstance(rows, list):
                return rows
    return []


def _get_rows(tool_name: str, tool_results: list["ToolExecution"]) -> list[dict]:
    for item in tool_results:
        if item.tool == tool_name and isinstance(item.data, dict):
            rows = item.data.get("results")
            if not isinstance(rows, list) or not rows:
                rows = item.data.get("main_results")
            if isinstance(rows, list):
                return rows
    return []


def _bookish_detail_row_is_sufficient(rows: list[dict[str, Any]]) -> bool:
    if not rows:
        return False
    primary = rows[0] if isinstance(rows[0], dict) else {}
    review = str(primary.get("review") or primary.get("comment") or "").strip()
    if len(review) >= 40:
        return True
    detail_fields = 0
    for field in ("author", "publisher", "channel", "category", "date", "rating"):
        if str(primary.get(field) or "").strip():
            detail_fields += 1
    return detail_fields >= 3


def _tool_ran(tool_name: str, tool_results: list["ToolExecution"]) -> bool:
    for item in tool_results:
        if item.tool == tool_name and str(item.status or "").strip().lower() != "skipped":
            return True
    return False


def _get_expansion_rows(tool_results: list["ToolExecution"]) -> list[dict]:
    """Return all rows from any expansion tool (MediaWiki or TMDB) that ran."""
    expansion_tools = {_REPAIR_TOOL_MEDIAWIKI, _REPAIR_TOOL_TMDB}
    rows: list[dict] = []
    for item in tool_results:
        if item.tool in expansion_tools and isinstance(item.data, dict):
            batch = item.data.get("results")
            if isinstance(batch, list):
                rows.extend(batch)
    return rows


def _get_music_work_hints(decision: "RouterDecision") -> dict[str, list[str]]:
    evidence = getattr(decision, "evidence", None) or {}
    hints = evidence.get("music_work_hints") if isinstance(evidence, dict) and isinstance(evidence.get("music_work_hints"), dict) else {}
    composer_hints = [str(item).strip() for item in (hints.get("composer_hints") or []) if str(item).strip()]
    work_signature = [str(item).strip() for item in (hints.get("work_signature") or []) if str(item).strip()]
    instrument_hints = [str(item).strip() for item in (hints.get("instrument_hints") or []) if str(item).strip()]
    form_hints = [str(item).strip() for item in (hints.get("form_hints") or []) if str(item).strip()]
    work_family_hints = [str(item).strip() for item in (hints.get("work_family_hints") or []) if str(item).strip()]
    return {
        "composer_hints": composer_hints,
        "instrument_hints": instrument_hints,
        "form_hints": form_hints,
        "work_family_hints": work_family_hints,
        "work_signature": work_signature,
    }


def _rows_match_music_signature(
    rows: list[dict[str, Any]],
    *,
    composer_hints: list[str],
    instrument_hints: list[str],
    form_hints: list[str],
    work_family_hints: list[str],
    work_signature: list[str],
) -> list[dict[str, Any]]:
    strict = [
        row for row in rows
        if title_matches_music_work_signature(
            str(row.get("title") or ""),
            composer_hints=composer_hints,
            instrument_hints=instrument_hints,
            form_hints=form_hints,
            work_family_hints=work_family_hints,
            work_signature=work_signature,
        )
    ]
    if strict:
        return strict
    # Fallback for cross-language alias gaps: relax composer-title hit.
    relaxed = [
        row for row in rows
        if title_matches_music_work_signature(
            str(row.get("title") or ""),
            composer_hints=[],
            instrument_hints=instrument_hints,
            form_hints=form_hints,
            work_family_hints=work_family_hints,
            work_signature=work_signature,
        )
    ]
    return relaxed


def _first_true_flag(flags: dict[str, Any], keys: tuple[str, ...]) -> str:
    for k in keys:
        if flags.get(k):
            return k
    return ""


def _check_domain_genre_mismatch(
    decision: "RouterDecision",
    rows: list[dict],
    media_family: str,
) -> bool:
    """Return True when the query specifies a video/book genre via ontology
    aliases but none of the top retrieved rows contain that genre in their
    category/type/tag fields.

    Only fires when all of the following hold:
    - ``media_family`` is "audiovisual" or "bookish"
    - ``decision.filters`` carries non-empty ``category`` values
    - At least one token is a known alias (not a free-form string)
    - None of the first 5 rows mention any of the genre tokens
    """
    raw_filters = getattr(decision, "filters", None) or {}
    # Normalise: accept both dict[str, list] and dict[str, str]
    category_values: list[str] = []
    raw_cat = raw_filters.get("category") or raw_filters.get("genre")
    if isinstance(raw_cat, list):
        category_values = [str(v) for v in raw_cat if v]
    elif isinstance(raw_cat, str) and raw_cat:
        category_values = [raw_cat]

    if not category_values:
        return False

    is_known_alias = is_video_genre_alias if media_family == "audiovisual" else is_book_genre_alias
    # Require at least one token to be an ontology-recognised alias so we
    # don't fire on arbitrary free-form category strings.
    if not any(is_known_alias(t) for t in category_values):
        return False

    # Only attempt metadata consistency checks when local rows actually carry
    # category-like fields. Title-only rows are too sparse and should not be
    # degraded into a semantic mismatch.
    if not any(
        any(str(row.get(field) or "").strip() for field in ("category", "genre", "tags", "type"))
        for row in rows[:5]
    ):
        return False

    def _row_matches(row: dict) -> bool:
        row_text = " ".join([
            str(row.get("title") or ""),
            str(row.get("category") or ""),
            str(row.get("genre") or ""),
            str(row.get("tags") or ""),
            str(row.get("type") or ""),
        ]).lower()
        for token in category_values:
            if token.lower() in row_text:
                return True
        return False

    return not any(_row_matches(r) for r in rows[:5])


def _get_domain_entity_hints(raw_q: str, media_family: str) -> dict[str, Any]:
    """Return ontology hints dict for *raw_q* using the appropriate domain collector."""
    try:
        if media_family == "audiovisual":
            return _collect_video_hints(raw_q)
        else:
            return _collect_book_hints(raw_q)
    except Exception:
        return {}


def _check_entity_alias_mismatch(
    rows: list[dict[str, Any]],
    entity_hints: list[str],
) -> bool:
    """Return True when none of the first 5 rows mention any of the entity alias hints.

    Checks title, category, director, author, publisher, tags, and notes fields.
    Only fires when *entity_hints* is non-empty.
    """
    if not entity_hints or not rows:
        return False

    def _row_has_entity(row: dict) -> bool:
        row_text = " ".join([
            str(row.get("title") or ""),
            str(row.get("category") or ""),
            str(row.get("director") or ""),
            str(row.get("author") or ""),
            str(row.get("publisher") or ""),
            str(row.get("tags") or ""),
            str(row.get("notes") or ""),
        ]).lower()
        for hint in entity_hints:
            if str(hint or "").lower() in row_text:
                return True
        return False

    return not any(_row_has_entity(r) for r in rows[:5])
