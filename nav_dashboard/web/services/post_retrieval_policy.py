"""PostRetrievalPolicy — evaluates tool results after retrieval and decides
how the answer layer should proceed.

Design principles:
- Pure data-in / dataclass-out: no I/O, no LLM calls.
- Imports only stdlib + types from agent_service to stay lightweight.
- agent_service continues to own _build_guardrail_flags; this module
  **consumes** that output (guardrail_flags dict) rather than recomputing it.

Typical usage inside agent_service::

    from nav_dashboard.web.services.post_retrieval_policy import (
        PostRetrievalPolicy, PostRetrievalOutcome,
    )
    outcome = PostRetrievalPolicy().evaluate(decision, tool_results, guardrail_flags)
    query_classification["post_retrieval_outcome"] = dataclasses.asdict(outcome)
"""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, TYPE_CHECKING

from nav_dashboard.web.services.music_ontology import (
    is_form_alias,
    is_instrument_alias,
    is_work_family_alias,
)

if TYPE_CHECKING:
    # Avoid circular import at runtime; only used for type hints.
    from nav_dashboard.web.services.agent_service import RouterDecision, ToolExecution

# Tool name constants (mirrored to avoid circular import at module load time).
_TOOL_QUERY_DOC_RAG = "query_document_rag"
_TOOL_QUERY_MEDIA = "query_media_record"
_TOOL_SEARCH_WEB = "search_web"
_TOOL_SEARCH_TMDB = "search_tmdb_media"  # must match agent_service.TOOL_SEARCH_TMDB
_TOOL_SEARCH_BY_CREATOR = "search_by_creator"

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
        domain = str(getattr(decision, "domain", "general") or "general")
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
        _query_class = str(getattr(decision, "query_class", "") or "")
        _collection_classes = {"media_creator_collection", "media_abstract_concept", "media_collection_filter"}
        _wants_expansion = (
            _query_class in _collection_classes
            and any(cue in _raw_q for cue in _expansion_cues)
        )
        _mediawiki_ran_flag = _mediawiki_ran(tool_results)
        _tmdb_ran_flag = _tmdb_ran(tool_results)
        _expansion_ran = _mediawiki_ran_flag or _tmdb_ran_flag
        # Check if expansion tools ran but returned empty/weak data (T2).
        _expansion_ran_dry = _expansion_ran and not _get_expansion_rows(tool_results)
        _needs_expansion = _wants_expansion and (not _expansion_ran or _expansion_ran_dry)
        lookup_mode = str(getattr(decision, "lookup_mode", "") or "")
        entities = list(getattr(decision, "entities", None) or [])
        creator_collection = bool(
            (getattr(decision, "evidence", None) or {}).get("creator_collection_query")
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
        is_music_versions_compare = str(getattr(decision, "query_class", "") or "") == "music_work_versions_compare"
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
            if domain == "media" and lookup_mode == "entity_lookup" and entities:
                repair.append(_REPAIR_TOOL_TMDB)
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
            repair_partial: list[str] = []
            needs_explanation_flag = bool(getattr(decision, "needs_explanation", False))
            if lookup_mode == "entity_lookup" and needs_explanation_flag:
                if not _tmdb_ran(tool_results):
                    repair_partial.append(_REPAIR_TOOL_TMDB)
                if not _mediawiki_ran(tool_results):
                    repair_partial.append(_REPAIR_TOOL_MEDIAWIKI)
            if creator_collection and not _mediawiki_ran(tool_results):
                if _REPAIR_TOOL_MEDIAWIKI not in repair_partial:
                    repair_partial.append(_REPAIR_TOOL_MEDIAWIKI)
            # needs_expansion repair: expansion was wanted (T1 or T2) — try the
            # complementary tool that hasn't run or ran dry.
            # Query-class-aware: TMDB is not a universal fallback.
            # creator and abstract-concept queries (books, literature, history,
            # person biographies) should only use MediaWiki; TMDB is for
            # audiovisual titles and would return irrelevant results otherwise.
            _WIKI_ONLY_QUERY_CLASSES = {"media_creator_collection", "media_abstract_concept"}
            _is_wiki_only_class = _query_class in _WIKI_ONLY_QUERY_CLASSES
            if _needs_expansion:
                if not _mediawiki_ran_flag and _REPAIR_TOOL_MEDIAWIKI not in repair_partial:
                    repair_partial.append(_REPAIR_TOOL_MEDIAWIKI)
                elif _mediawiki_ran_flag and _expansion_ran_dry and not _is_wiki_only_class:
                    # MediaWiki ran dry; TMDB is a valid fallback only for audiovisual queries.
                    if not _tmdb_ran_flag and _REPAIR_TOOL_TMDB not in repair_partial:
                        repair_partial.append(_REPAIR_TOOL_TMDB)
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
    return _tool_ran(_REPAIR_TOOL_MEDIAWIKI, tool_results)


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
            if isinstance(rows, list):
                return rows
    return []


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


def _normalize_title_token(text: str) -> str:
    return "".join(ch.lower() for ch in str(text or "") if ch.isalnum() or "\u4e00" <= ch <= "\u9fff")


def _row_matches_music_signature(
    row: dict[str, Any],
    composer_hints: list[str],
    work_signature: list[str],
    instrument_hints: list[str] | None = None,
    form_hints: list[str] | None = None,
    work_family_hints: list[str] | None = None,
) -> bool:
    title = _normalize_title_token(str(row.get("title") or ""))
    if not title:
        return False

    def _contains_any(tokens: list[str]) -> bool:
        for token in tokens:
            normalized = _normalize_title_token(token)
            if normalized and normalized in title:
                return True
        return False

    composer_ok = _contains_any(composer_hints) if composer_hints else True
    _instrument_hints = [str(item).strip() for item in (instrument_hints or []) if str(item).strip()]
    _form_hints = [str(item).strip() for item in (form_hints or []) if str(item).strip()]
    _work_family_hints = [str(item).strip() for item in (work_family_hints or []) if str(item).strip()]
    if not _instrument_hints:
        _instrument_hints = [
            token for token in work_signature
            if is_instrument_alias(token)
        ]
    if not _form_hints:
        _form_hints = [token for token in work_signature if is_form_alias(token)]
    if not _work_family_hints:
        _work_family_hints = [token for token in work_signature if is_work_family_alias(token)]

    family_hit = _contains_any(_work_family_hints)
    instrument_hit = _contains_any(_instrument_hints) if _instrument_hints else False
    form_hit = _contains_any(_form_hints) if _form_hints else False
    if _instrument_hints and _form_hints:
        work_ok = family_hit or (instrument_hit and form_hit)
    elif _work_family_hints:
        work_ok = family_hit
    else:
        work_ok = _contains_any(work_signature)

    def _is_specific_music_marker(token: str) -> bool:
        raw_token = str(token or "").strip().lower()
        normalized_token = _normalize_title_token(raw_token)
        if not raw_token and not normalized_token:
            return False
        if re.search(r"op\.?\s*\d+", raw_token):
            return True
        if re.search(r"no\.?\s*\d+", raw_token):
            return True
        if re.search(r"第\s*\d+", str(token or "")):
            return True
        if normalized_token.startswith("op") and any(ch.isdigit() for ch in normalized_token):
            return True
        if normalized_token.startswith("no") and any(ch.isdigit() for ch in normalized_token):
            return True
        return False

    specific_tokens = [tok for tok in work_signature if _is_specific_music_marker(tok)]
    if work_ok and specific_tokens:
        work_ok = _contains_any(specific_tokens)

    return composer_ok and work_ok


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
        if _row_matches_music_signature(
            row,
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
        if _row_matches_music_signature(
            row,
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
