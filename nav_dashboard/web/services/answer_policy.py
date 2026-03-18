"""AnswerPolicy — determines the answer strategy given the router decision
and the post-retrieval outcome.

Pure data-in / dataclass-out: no I/O, no LLM calls.

Four answer modes:
  knowledge_answer     — tech / general domain, no media entities:
                         inline explanation from doc context or LLM knowledge.
  collection_answer    — media filter_search / no specific entity:
                         list / collection style output.
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

    from nav_dashboard.web.services.answer_policy import AnswerPolicy, AnswerStrategy
    strategy = AnswerPolicy().determine(decision, post_retrieval_outcome)
    query_classification["answer_strategy"] = dataclasses.asdict(strategy)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from nav_dashboard.web.services.agent_service import RouterDecision
    from nav_dashboard.web.services.post_retrieval_policy import PostRetrievalOutcome


@dataclass
class AnswerStrategy:
    """Structured signal for the answer synthesis layer.

    Attributes:
        mode:        One of "knowledge_answer" | "collection_answer" |
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
        domain = str(getattr(decision, "domain", "general") or "general")
        intent = str(getattr(decision, "intent", "knowledge_qa") or "knowledge_qa")
        lookup_mode = str(getattr(decision, "lookup_mode", "") or "")
        entities = list(getattr(decision, "entities", None) or [])
        needs_comparison = bool(getattr(decision, "needs_comparison", False))
        needs_explanation = bool(getattr(decision, "needs_explanation", False))
        arbitration = str(getattr(decision, "arbitration", "") or "")
        query_class = str(getattr(decision, "query_class", "knowledge_qa") or "knowledge_qa")
        # answer_shape is now a first-class input: it can override the structural
        # branch choices derived from lookup_mode / entities / query_class.
        # Values: list_only | list_plus_expand | detail_card | compare | ""
        answer_shape = str(getattr(decision, "answer_shape", "") or "")
        # subject_scope drives evidence priority: personal_record = local first,
        # external only as clearly labelled supplement.
        subject_scope = str(getattr(decision, "subject_scope", "general_knowledge") or "general_knowledge")

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
            _personal = subject_scope == "personal_record"
            _personal_expand = _personal and answer_shape in {"list_plus_expand", "detail_card"}
            ep = _evidence_policy(
                local_claims_allowed=True,
                external_enrichment_allowed=bool(repair_tools) or _creator_query or _personal_expand,
                must_label_external=bool(repair_tools) or _creator_query or _personal,
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

        # ── 2. Entity detail ──────────────────────────────────────────────────
        # answer_shape == "detail_card" can also trigger this path even without
        # a strict entity_lookup, as long as we have exactly one entity.
        if (
            domain == "media"
            and (lookup_mode == "entity_lookup" or answer_shape == "detail_card")
            and len(entities) == 1
        ):
            return AnswerStrategy(
                mode="entity_detail_answer",
                style_hints={
                    "entity": entities[0],
                    "needs_explanation": needs_explanation,
                    "needs_comparison": needs_comparison,
                    # Contract: local personal review → then external overview/credits.
                    "response_structure": "local_record_plus_external_info",
                    "subject_scope": subject_scope,
                    "evidence_policy": ep,
                },
            )

        # ── 3. Collection ─────────────────────────────────────────────────────
        if domain == "media" and (
            lookup_mode in ("filter_search", "general_lookup")
            or len(entities) > 1
            or intent == "collection_query"
            or answer_shape in ("list_only", "list_plus_expand", "compare")
        ):
            # answer_shape is the primary structural signal; fall back to
            # query_class-derived response_structure when shape is not explicit.
            if answer_shape == "compare":
                # Comparison queries: list with side-by-side structure.
                response_structure = "compare"
                needs_comparison = True
            elif answer_shape == "list_plus_expand" or needs_expansion:
                # Per-item narrative needed (explicit or inferred from missing
                # expansion data): always attach external background section.
                response_structure = "local_list_plus_external_background"
            elif query_class == "media_creator_collection":
                response_structure = "local_list_plus_external_background"
            elif query_class == "media_abstract_concept":
                response_structure = "thematic_list"
            else:
                response_structure = "local_list"
            return AnswerStrategy(
                mode="collection_answer",
                style_hints={
                    "list_style": True,
                    "max_items": 10,
                    "needs_comparison": needs_comparison,
                    "answer_shape": answer_shape,
                    "subject_scope": subject_scope,
                    "needs_expansion": needs_expansion,
                    "expansion_missing": expansion_missing,
                    "expansion_unavailable": expansion_unavailable,
                    "response_structure": response_structure,
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
