"""AnswerPolicy — determines the answer strategy given the router decision
and the post-retrieval outcome.

Pure data-in / dataclass-out: no I/O, no LLM calls.

Five answer modes:
  knowledge_answer     — tech / general domain, no media entities:
                         inline explanation from doc context or LLM knowledge.
  collection_answer    — media filter_search / no specific entity:
                         list / collection style output.
    personal_media_review_collection — personal media review collection query:
                                                 local records first, no external background in the main body.
  entity_detail_answer — media entity_lookup with a specific entity:
                         detail card + optional narrative.
  fallback_answer      — true ambiguity only (off-domain, ambiguous followup,
                         or zero results with no repair path):
                         LLM-only or clarification prompt.

Evidence policy separates *what* the answer layer may claim from *where* it
may source content.  Every AnswerStrategy carries an ``evidence_policy`` dict
inside ``style_hints`` with these boolean keys:

    local_claims_allowed       — may assert facts drawn from local library records.
    external_enrichment_allowed— may incorporate TMDB / MediaWiki / web content.
    must_label_external        — must visually label externally sourced content.
    must_weaken_uncertain_claims— must hedge uncertain or unverified claims.

Typical usage inside agent_service::

    from nav_dashboard.web.services.answer.answer_policy import AnswerPolicy, AnswerStrategy
    strategy = AnswerPolicy().determine(decision, post_retrieval_outcome)
    query_classification["answer_strategy"] = dataclasses.asdict(strategy)
"""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, TYPE_CHECKING

from ..planner import planner_contracts as planner_contracts
from ..media.media_strategy import resolve_media_strategy

if TYPE_CHECKING:
    from ..agent.agent_types import RouterDecision
    from ..retrieval.post_retrieval_policy import PostRetrievalOutcome


@dataclass
class AnswerStrategy:
    """Structured signal for the answer synthesis layer.

    Attributes:
        mode:        One of "knowledge_answer" | "collection_answer" |
                 "personal_media_review_collection" |
                 "entity_detail_answer" | "fallback_answer".
        style_hints: Freeform dict passed to the prompt builder.
                     Always includes "evidence_policy" — see module docstring.
                     Other keys: "list_style", "needs_narrative", "needs_comparison",
                                 "needs_explanation", "max_items", "degrade_reason".
    """

    mode: str = "knowledge_answer"
    style_hints: dict[str, Any] = field(default_factory=dict)


def _evidence_policy(
    *,
    local_claims_allowed: bool = True,
    external_enrichment_allowed: bool = False,
    must_label_external: bool = False,
    must_weaken_uncertain_claims: bool = False,
) -> dict[str, bool]:
    """Return a canonical evidence-policy dict for inclusion in style_hints."""
    return {
        "local_claims_allowed": local_claims_allowed,
        "external_enrichment_allowed": external_enrichment_allowed,
        "must_label_external": must_label_external,
        "must_weaken_uncertain_claims": must_weaken_uncertain_claims,
    }


_DETAIL_FACET_PATTERN = re.compile(
    r"导演|演员|卡司|配音|剧情|简介|评分|票房|上映|作者|出版社|原作|制作|staff|cast|director",
    re.IGNORECASE,
)


def _has_specific_detail_facets(question: str) -> bool:
    text = str(question or "").strip()
    if not text:
        return False
    matches = _DETAIL_FACET_PATTERN.findall(text)
    return len(matches) >= 2


def _is_mechanical_media_collection(
    *,
    answer_shape: str,
    response_structure: str,
    needs_comparison: bool,
    needs_explanation: bool,
    needs_expansion: bool,
    subject_scope: str,
    partial_local: bool,
    weak_results: bool,
    external_enrichment_allowed: bool,
) -> bool:
    return (
        answer_shape in {
            planner_contracts.ROUTER_ANSWER_SHAPE_LIST_ONLY,
            planner_contracts.ROUTER_ANSWER_SHAPE_SUMMARY,
        }
        and response_structure == "local_list"
        and not needs_comparison
        and not needs_explanation
        and not needs_expansion
        and subject_scope != planner_contracts.ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD
        and not partial_local
        and not weak_results
        and not external_enrichment_allowed
    )


def _prefers_personal_timebox_collection_synthesis(
    decision: "RouterDecision",
    *,
    answer_shape: str,
    subject_scope: str,
    needs_comparison: bool,
    needs_explanation: bool,
    needs_expansion: bool,
) -> bool:
    if subject_scope != planner_contracts.ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD:
        return False
    if answer_shape not in {
        planner_contracts.ROUTER_ANSWER_SHAPE_LIST_ONLY,
        planner_contracts.ROUTER_ANSWER_SHAPE_SUMMARY,
    }:
        return False
    if needs_comparison or needs_explanation or needs_expansion:
        return False
    date_range = [str(item or "").strip() for item in list(getattr(decision, "date_range", None) or [])]
    time_constraint = getattr(decision, "time_constraint", None) or {}
    explicit_range = str(time_constraint.get("kind") or "").strip() == "explicit_range"
    time_scope_type = str(getattr(decision, "time_scope_type", "") or "").strip()
    return bool(
        explicit_range
        and len([item for item in date_range if item]) >= 2
        and time_scope_type == planner_contracts.ROUTER_TIME_SCOPE_CONSUMPTION_DATE
    )


class AnswerPolicy:
    """Stateless policy: maps (RouterDecision, PostRetrievalOutcome) → AnswerStrategy.

    Guardrail philosophy: separate evidence tiers, do not block answers.
    Only produce ``fallback_answer`` for genuine ambiguity (off-domain routing,
    ambiguous follow-up proxy) or zero results with no repair path.
    All other outcomes produce the structurally correct answer mode, carrying
    an ``evidence_policy`` dict that constrains *how* the answer layer sources
    and labels its content.

    Priority order:
      1. True fallback — off-domain OR (zero results AND no repair path).
      2. Entity detail — single media entity with entity_lookup.
      3. Collection — media filter_search or multi-entity.
      4. Knowledge — tech / general.
      5. Default knowledge fallback.
    """

    def determine(
        self,
        decision: "RouterDecision",
        post_retrieval_outcome: "PostRetrievalOutcome",
    ) -> AnswerStrategy:
        media_strategy = resolve_media_strategy(decision)
        domain = str(getattr(decision, "domain", "general") or "general")
        intent = str(getattr(decision, "intent", "knowledge_qa") or "knowledge_qa")
        lookup_mode = media_strategy.lookup_mode
        entities = list(getattr(decision, "entities", None) or [])
        needs_comparison = bool(getattr(decision, "needs_comparison", False))
        needs_explanation = bool(getattr(decision, "needs_explanation", False))
        arbitration = str(getattr(decision, "arbitration", "") or "")
        query_class = media_strategy.query_class or str(
            getattr(decision, "query_class", planner_contracts.ROUTER_QUERY_CLASS_KNOWLEDGE_QA)
            or planner_contracts.ROUTER_QUERY_CLASS_KNOWLEDGE_QA
        )
        # answer_shape is now a first-class input: it can override the structural
        # branch choices derived from lookup_mode / entities / query_class.
        # Values: list_only | list_plus_expand | detail_card | compare | ""
        answer_shape = media_strategy.answer_shape
        # subject_scope drives evidence priority: personal_record = local first,
        # external only as clearly labelled supplement.
        subject_scope = media_strategy.subject_scope

        action = str(getattr(post_retrieval_outcome, "action", "use_results") or "use_results")
        degrade_reason = str(getattr(post_retrieval_outcome, "degrade_reason", "") or "")
        zero_results = bool(getattr(post_retrieval_outcome, "zero_results", False))
        weak_results = bool(getattr(post_retrieval_outcome, "weak_results", False))
        off_domain = bool(getattr(post_retrieval_outcome, "off_domain", False))
        partial_local = bool(getattr(post_retrieval_outcome, "partial_local", False))
        repair_tools = list(getattr(post_retrieval_outcome, "repair_tools", None) or [])
        # needs_expansion: expansion was wanted but tools ran dry or not at all.
        needs_expansion = bool(getattr(post_retrieval_outcome, "needs_expansion", False))
        # expansion_missing: wanted but no expansion tool ran (T1).
        expansion_missing = bool(getattr(post_retrieval_outcome, "expansion_missing", False))
        # expansion_unavailable: all known expansion avenues are exhausted.
        expansion_unavailable = bool(
            getattr(post_retrieval_outcome, "expansion_unavailable", False)
        )

        # ── Evidence policy ───────────────────────────────────────────────────
        # Determine appropriate evidence constraints based on retrieval outcome.
        if off_domain and action == "degrade":
            ep = _evidence_policy(
                local_claims_allowed=False,
                external_enrichment_allowed=False,
            )
        elif zero_results and action == "fallback_to_llm":
            ep = _evidence_policy(
                local_claims_allowed=False,
                external_enrichment_allowed=False,
            )
        elif zero_results and action == "enrich":
            # No local results, but external repair path available.
            ep = _evidence_policy(
                local_claims_allowed=False,
                external_enrichment_allowed=True,
                must_label_external=True,
            )
        elif weak_results:
            # Some local results but quality is low; allow external, weaken claims.
            ep = _evidence_policy(
                local_claims_allowed=True,
                external_enrichment_allowed=True,
                must_label_external=bool(repair_tools),
                must_weaken_uncertain_claims=True,
            )
        elif partial_local:
            # Local results cover personal record; external adds background/context.
            ep = _evidence_policy(
                local_claims_allowed=True,
                external_enrichment_allowed=True,
                must_label_external=True,
            )
        else:
            # Good local results; external tools may have run as planned enrichment.
            # Also enable enrichment for creator-collection queries where MediaWiki
            # was planned up-front (not as a repair), so the content appears in body.
            _ev = dict(getattr(decision, "evidence", None) or {})
            _creator_query = bool(_ev.get("creator_collection_query"))
            # subject_scope=personal_record alignment with RoutingPolicy:
            # external_enrichment is allowed only when answer_shape explicitly
            # requests expansion (list_plus_expand / detail_card), mirroring the
            # step-5/6-7 suppression logic in routing_policy.py.
            # must_label_external is always True for personal queries so that any
            # external content that did make it through is visually separated.
            _personal = subject_scope == planner_contracts.ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD
            _personal_expand = _personal and answer_shape in {
                planner_contracts.ROUTER_ANSWER_SHAPE_LIST_PLUS_EXPAND,
                planner_contracts.ROUTER_ANSWER_SHAPE_DETAIL_CARD,
            }
            _planned_external = bool(
                media_strategy.should_expand_external
                and (
                    media_strategy.needs_explanation
                    or answer_shape == planner_contracts.ROUTER_ANSWER_SHAPE_LIST_PLUS_EXPAND
                    or _personal_expand
                    or _creator_query
                )
            )
            ep = _evidence_policy(
                local_claims_allowed=True,
                external_enrichment_allowed=bool(repair_tools) or _creator_query or _personal_expand or _planned_external,
                must_label_external=bool(repair_tools) or _creator_query or _personal or _planned_external,
            )

        # ── 1. True fallback — only for genuine ambiguity / no data ──────────
        if (off_domain and action == "degrade") or (zero_results and action == "fallback_to_llm"):
            return AnswerStrategy(
                mode="fallback_answer",
                style_hints={
                    "degrade_reason": degrade_reason,
                    "zero_results": zero_results,
                    "off_domain": off_domain,
                    "evidence_policy": ep,
                },
            )

        if query_class == planner_contracts.ROUTER_QUERY_CLASS_PERSONAL_MEDIA_REVIEW_COLLECTION:
            allow_external = media_strategy.should_expand_external
            compare_mode = bool(answer_shape == planner_contracts.ROUTER_ANSWER_SHAPE_COMPARE or needs_comparison)
            ep = _evidence_policy(
                local_claims_allowed=True,
                external_enrichment_allowed=allow_external,
                must_label_external=True,
                must_weaken_uncertain_claims=bool(weak_results),
            )
            return AnswerStrategy(
                mode="personal_media_review_collection",
                style_hints={
                    "list_style": True,
                    "max_items": 10,
                    "focus_item_limit": 5,
                    "narrative_outline": "overview_then_key_items",
                    "needs_comparison": needs_comparison,
                    "answer_shape": answer_shape,
                    "subject_scope": subject_scope,
                    "needs_expansion": False,
                    "expansion_missing": expansion_missing,
                    "expansion_unavailable": expansion_unavailable,
                    "response_structure": "compare" if compare_mode else "local_review_list",
                    "include_mentions": media_strategy.should_render_mentions,
                    "include_external": allow_external,
                    "llm_summary_on_structured": True,
                    "structured_appendix_expected": not compare_mode,
                    "evidence_policy": ep,
                },
            )

        # ── 2. Entity detail ──────────────────────────────────────────────────
        # answer_shape == "detail_card" can also trigger this path even without
        # a strict entity_lookup, as long as we have exactly one entity.
        if (
            domain == "media"
            and (lookup_mode == "entity_lookup" or answer_shape == planner_contracts.ROUTER_ANSWER_SHAPE_DETAIL_CARD)
            and len(entities) == 1
        ):
            specific_detail_facets = _has_specific_detail_facets(getattr(decision, "raw_question", ""))
            detail_overlay = bool(
                subject_scope == planner_contracts.ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD
                and (answer_shape == planner_contracts.ROUTER_ANSWER_SHAPE_DETAIL_CARD or specific_detail_facets)
            )
            return AnswerStrategy(
                mode="entity_detail_answer",
                style_hints={
                    "entity": entities[0],
                    "narrative_outline": "entity_then_personal_take",
                    "needs_explanation": needs_explanation,
                    "needs_comparison": needs_comparison,
                    # Contract: local personal review → then external overview/credits.
                    "response_structure": "local_record_plus_external_info",
                    "subject_scope": subject_scope,
                    "llm_summary_on_structured": not detail_overlay,
                    "structured_appendix_expected": bool(ep.get("external_enrichment_allowed")),
                    "evidence_policy": ep,
                },
            )

        # ── 3. Collection ─────────────────────────────────────────────────────
        if domain == "media" and (
            lookup_mode in ("filter_search", "general_lookup")
            or len(entities) > 1
            or intent == "collection_query"
            or answer_shape in (
                planner_contracts.ROUTER_ANSWER_SHAPE_LIST_ONLY,
                planner_contracts.ROUTER_ANSWER_SHAPE_SUMMARY,
                planner_contracts.ROUTER_ANSWER_SHAPE_LIST_PLUS_EXPAND,
                planner_contracts.ROUTER_ANSWER_SHAPE_COMPARE,
            )
        ):
            # answer_shape is the primary structural signal; fall back to
            # query_class-derived response_structure when shape is not explicit.
            if answer_shape == planner_contracts.ROUTER_ANSWER_SHAPE_COMPARE:
                # Comparison queries: list with side-by-side structure.
                response_structure = "compare"
                needs_comparison = True
            elif answer_shape == planner_contracts.ROUTER_ANSWER_SHAPE_LIST_PLUS_EXPAND or needs_expansion:
                # Synthesis-first collection answers should stay LLM-led: overview
                # + selected key items, rather than dumping local rows.
                response_structure = "curated_collection_synthesis"
            elif _prefers_personal_timebox_collection_synthesis(
                decision,
                answer_shape=answer_shape,
                subject_scope=subject_scope,
                needs_comparison=needs_comparison,
                needs_explanation=needs_explanation,
                needs_expansion=needs_expansion,
            ):
                response_structure = "curated_collection_synthesis"
            elif query_class == planner_contracts.ROUTER_QUERY_CLASS_MEDIA_CREATOR_COLLECTION:
                response_structure = "curated_collection_synthesis"
            elif query_class == planner_contracts.ROUTER_QUERY_CLASS_MEDIA_ABSTRACT_CONCEPT:
                response_structure = "thematic_list"
            else:
                response_structure = "local_list"
            mechanical_collection = _is_mechanical_media_collection(
                answer_shape=answer_shape,
                response_structure=response_structure,
                needs_comparison=needs_comparison,
                needs_explanation=needs_explanation,
                needs_expansion=needs_expansion,
                subject_scope=subject_scope,
                partial_local=partial_local,
                weak_results=weak_results,
                external_enrichment_allowed=bool(ep.get("external_enrichment_allowed")),
            )
            return AnswerStrategy(
                mode="collection_answer",
                style_hints={
                    "list_style": True,
                    "max_items": 10,
                    "focus_item_limit": 5,
                    "overview_item_limit": 16,
                    "narrative_outline": "overview_then_key_items",
                    "needs_comparison": needs_comparison,
                    "answer_shape": answer_shape,
                    "subject_scope": subject_scope,
                    "needs_expansion": needs_expansion,
                    "expansion_missing": expansion_missing,
                    "expansion_unavailable": expansion_unavailable,
                    "response_structure": response_structure,
                    "llm_summary_on_structured": not mechanical_collection,
                    "structured_appendix_expected": bool(ep.get("external_enrichment_allowed")),
                    "evidence_policy": ep,
                },
            )

        # ── 4. Knowledge (tech / general) ─────────────────────────────────────
        if domain in ("tech", "general") or intent == "knowledge_qa":
            return AnswerStrategy(
                mode="knowledge_answer",
                style_hints={
                    "needs_explanation": needs_explanation,
                    "needs_comparison": needs_comparison,
                    "needs_narrative": arbitration in ("tech_primary", "tech_signal_only"),
                    "evidence_policy": ep,
                },
            )

        # ── 5. Mixed: tech knowledge is primary ───────────────────────────────
        if intent == "mixed" or arbitration.startswith("mixed_due_to"):
            return AnswerStrategy(
                mode="knowledge_answer",
                style_hints={
                    "needs_explanation": needs_explanation,
                    "mixed": True,
                    "evidence_policy": ep,
                },
            )

        # ── Default ───────────────────────────────────────────────────────────
        return AnswerStrategy(mode="knowledge_answer", style_hints={"evidence_policy": ep})
