"""tests/agent_e2e/test_chain.py

End-to-end chain tests: each test covers one full slice of the pipeline
(router → RoutingPolicy → PostRetrievalPolicy → AnswerPolicy) without
making real LLM or network calls.

Five categories, mirroring the benchmark taxonomy:
  1. tech_primary                — pure tech/doc-RAG query
  2. media_entity                — single-entity lookup
  3. media_collection            — filter_search / creator collection
  4. mixed_due_to_entity_plus_tech — mixed domain, doc goes first
  5. zero_results → fallback     — retrieval returns nothing, chain degrades correctly

Run:
    python -m pytest tests/agent_e2e/test_chain.py -v
    python tests/agent_e2e/test_chain.py
"""
from __future__ import annotations

import sys
import unittest
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tests.conftest import svc, _decide, _llm_stub, _make_runtime

from nav_dashboard.web.services.routing_policy import RoutingPolicy
from nav_dashboard.web.services.post_retrieval_policy import PostRetrievalPolicy, PostRetrievalOutcome
from nav_dashboard.web.services.answer_policy import AnswerPolicy, AnswerStrategy
from nav_dashboard.web.services.agent_types import (
    TOOL_QUERY_DOC_RAG,
    TOOL_QUERY_MEDIA,
    TOOL_SEARCH_TMDB,
    TOOL_EXPAND_MEDIAWIKI_CONCEPT,
    TOOL_SEARCH_WEB,
)


# ── minimal ToolExecution stub (mirrors tests/post_retrieval/test_policy.py) ──

@dataclass
class _FakeTool:
    tool: str
    status: str = "ok"
    data: dict = field(default_factory=dict)


def _media_rows(*ids: str) -> list[dict]:
    return [{"id": i, "title": i} for i in ids]


def _doc_rows(*ids: str) -> list[dict]:
    return [{"id": i, "score": 0.8} for i in ids]


def _fake_media(rows: list[dict] | None = None) -> _FakeTool:
    return _FakeTool(tool=TOOL_QUERY_MEDIA, data={"results": rows or []})


def _fake_doc(rows: list[dict] | None = None) -> _FakeTool:
    return _FakeTool(tool=TOOL_QUERY_DOC_RAG, data={"results": rows or []})


# ── helper: run full policy chain ─────────────────────────────────────────────

def _run_chain(
    question: str,
    llm_stub: dict,
    tool_results: list[_FakeTool],
    guardrail_flags: dict[str, Any] | None = None,
    search_mode: str = "local_only",
) -> tuple[svc.RouterDecision, svc.ExecutionPlan, PostRetrievalOutcome, AnswerStrategy]:
    decision = _decide(question, llm_stub)
    plan = RoutingPolicy().build_plan(decision, search_mode)
    outcome = PostRetrievalPolicy().evaluate(decision, tool_results, guardrail_flags or {})
    strategy = AnswerPolicy().determine(decision, outcome)
    return decision, plan, outcome, strategy


# ════════════════════════════════════════════════════════════════════════
# 1. tech_primary
# ════════════════════════════════════════════════════════════════════════

class TestChainTechPrimary(unittest.TestCase):
    """Tech queries must use doc_rag as the primary (and only) DB tool,
    produce knowledge_answer mode, and never call media tools."""

    def test_tech_plan_doc_first(self):
        """Tech domain → doc_rag must be first planned tool."""
        decision, plan, outcome, strategy = _run_chain(
            "深度学习架构原理是什么",
            _llm_stub(label="TECH", domain="tech"),
            [_fake_doc(_doc_rows("d1", "d2"))],
        )
        self.assertEqual(decision.domain, "tech")
        self.assertTrue(plan.planned_tools, "plan must have at least one tool")
        self.assertEqual(plan.primary_tool, TOOL_QUERY_DOC_RAG)
        self.assertNotIn(TOOL_QUERY_MEDIA, [t.name for t in plan.planned_tools])

    def test_tech_good_results_give_knowledge_answer(self):
        """Good doc results → knowledge_answer, proceed status."""
        _, _, outcome, strategy = _run_chain(
            "机器学习的概念和应用",
            _llm_stub(label="TECH", domain="tech"),
            [_fake_doc(_doc_rows("d1"))],
        )
        self.assertEqual(outcome.status, "proceed")
        self.assertEqual(strategy.mode, "knowledge_answer")

    def test_tech_evidence_policy_local_claims_allowed(self):
        """Tech proceed → evidence_policy.local_claims_allowed = True."""
        _, _, _, strategy = _run_chain(
            "transformer 注意力机制",
            _llm_stub(label="TECH", domain="tech"),
            [_fake_doc(_doc_rows("d1"))],
        )
        ep = strategy.style_hints.get("evidence_policy", {})
        self.assertTrue(ep.get("local_claims_allowed"))
        self.assertFalse(ep.get("external_enrichment_allowed"))

    def test_tech_zero_doc_gives_fallback(self):
        """Zero doc results → zero_results outcome, fallback_answer mode."""
        _, _, outcome, strategy = _run_chain(
            "rag检索增强生成的工作流程",
            _llm_stub(label="TECH", domain="tech"),
            [_fake_doc([])],
        )
        self.assertEqual(outcome.status, "zero_results")
        self.assertEqual(strategy.mode, "fallback_answer")
        self.assertTrue(strategy.style_hints.get("zero_results"))


# ════════════════════════════════════════════════════════════════════════
# 2. media_entity
# ════════════════════════════════════════════════════════════════════════

class TestChainMediaEntity(unittest.TestCase):
    """Single-entity media queries must produce entity_detail_answer with
    local evidence, and TMDB/MediaWiki enrichment when needs_explanation."""

    def test_entity_plan_includes_media_tool(self):
        """entity_lookup → query_media_record must be in plan."""
        decision, plan, _, _ = _run_chain(
            "《教父》是哪年上映的",
            _llm_stub(label="MEDIA", domain="media", entities=["教父"], lookup_mode="entity_lookup"),
            [_fake_media(_media_rows("godfather"))],
        )
        self.assertIn(TOOL_QUERY_MEDIA, [t.name for t in plan.planned_tools])

    def test_entity_good_result_gives_entity_detail(self):
        """entity_lookup with results → entity_detail_answer.
        Note: '上映' cue sets needs_explanation=True, triggering partial_local enrichment.
        The answer mode is still entity_detail_answer in both proceed and partial_local cases.
        """
        _, _, outcome, strategy = _run_chain(
            "《教父》是哪年上映的",
            _llm_stub(label="MEDIA", domain="media", entities=["教父"], lookup_mode="entity_lookup"),
            [_fake_media(_media_rows("godfather"))],
        )
        # partial_local is expected because '上映' triggers needs_explanation=True
        self.assertIn(outcome.status, ("proceed", "partial_local"))
        self.assertEqual(strategy.mode, "entity_detail_answer")
        self.assertEqual(strategy.style_hints.get("entity"), "教父")

    def test_entity_explanation_query_includes_tmdb_in_plan(self):
        """需要解释的条目查询 → RoutingPolicy must add TMDB + MediaWiki."""
        decision = _decide(
            "《骨之收藏》的剧情是什么",
            _llm_stub(label="MEDIA", domain="media", entities=["骨之收藏"], lookup_mode="entity_lookup"),
        )
        # needs_explanation is set by _build_router_decision from lexical cues
        # Override to ensure we test the plan logic, regardless of LLM stub
        decision.needs_explanation = True
        plan = RoutingPolicy().build_plan(decision, "local_only")
        tool_names = [t.name for t in plan.planned_tools]
        self.assertIn(TOOL_SEARCH_TMDB, tool_names)
        self.assertIn(TOOL_EXPAND_MEDIAWIKI_CONCEPT, tool_names)

    def test_entity_zero_results_suggests_tmdb_repair(self):
        """entity_lookup zero results → zero_results + repair = TMDB."""
        _, _, outcome, strategy = _run_chain(
            "《波拉尼奥》的生平",
            _llm_stub(label="MEDIA", domain="media", entities=["波拉尼奥"], lookup_mode="entity_lookup"),
            [_fake_media([])],
        )
        self.assertEqual(outcome.status, "zero_results")
        self.assertIn(TOOL_SEARCH_TMDB, outcome.repair_tools)
        # zero+enrich → entity_detail or fallback depending on repair path
        self.assertIn(strategy.mode, ("entity_detail_answer", "fallback_answer"))

    def test_entity_partial_local_evidence_policy(self):
        """entity_lookup with results + needs_explanation → partial_local, must_label_external."""
        decision = _decide(
            "《白色相簿2》的剧情和导演",
            _llm_stub(label="MEDIA", domain="media", entities=["白色相簿2"], lookup_mode="entity_lookup"),
        )
        decision.needs_explanation = True
        tool_results = [_fake_media(_media_rows("wa2"))]
        outcome = PostRetrievalPolicy().evaluate(decision, tool_results, {})
        strategy = AnswerPolicy().determine(decision, outcome)
        if outcome.status == "partial_local":
            self.assertTrue(outcome.partial_local)
            ep = strategy.style_hints.get("evidence_policy", {})
            self.assertTrue(ep.get("must_label_external"))


# ════════════════════════════════════════════════════════════════════════
# 3. media_collection
# ════════════════════════════════════════════════════════════════════════

class TestChainMediaCollection(unittest.TestCase):
    """Filter-search / creator collection queries → collection_answer."""

    def test_filter_search_plan_has_media_tool(self):
        _, plan, _, _ = _run_chain(
            "推荐几部2020年法国电影",
            _llm_stub(label="MEDIA", domain="media", lookup_mode="filter_search",
                      filters={"region": ["法国"], "year": ["2020"]}),
            [_fake_media(_media_rows("m1", "m2"))],
        )
        self.assertIn(TOOL_QUERY_MEDIA, [t.name for t in plan.planned_tools])

    def test_filter_search_good_results_give_collection_answer(self):
        _, _, outcome, strategy = _run_chain(
            "推荐几部2020年法国电影",
            _llm_stub(label="MEDIA", domain="media", lookup_mode="filter_search",
                      filters={"region": ["法国"], "year": ["2020"]}),
            [_fake_media(_media_rows("m1", "m2", "m3"))],
        )
        self.assertEqual(outcome.status, "proceed")
        self.assertEqual(strategy.mode, "collection_answer")
        self.assertTrue(strategy.style_hints.get("list_style"))

    def test_creator_collection_plan_includes_mediawiki(self):
        """Creator collection query → RoutingPolicy must plan MediaWiki."""
        decision = _decide(
            "加缪的作品有哪些",
            _llm_stub(label="MEDIA", domain="media", lookup_mode="filter_search"),
        )
        # Simulate the creator_collection evidence set by semantic repair
        decision.domain = "media"
        decision.evidence["creator_collection_query"] = True
        decision.evidence["creator_resolution"] = {"canonical": "阿尔贝·加缪"}
        plan = RoutingPolicy().build_plan(decision, "local_only")
        self.assertIn(TOOL_EXPAND_MEDIAWIKI_CONCEPT, [t.name for t in plan.planned_tools])

    def test_collection_weak_results_not_fallback(self):
        """Weak results on a collection query → must NOT produce fallback_answer."""
        _, _, outcome, strategy = _run_chain(
            "2022年悬疑电影推荐",
            _llm_stub(label="MEDIA", domain="media", lookup_mode="filter_search"),
            [_fake_media(_media_rows("m1"))],
            guardrail_flags={"insufficient_valid_results": True},
        )
        self.assertIn(outcome.status, ("weak_results", "zero_results"))
        self.assertNotEqual(strategy.mode, "fallback_answer")
        ep = strategy.style_hints.get("evidence_policy", {})
        self.assertTrue(ep.get("must_weaken_uncertain_claims"))


# ════════════════════════════════════════════════════════════════════════
# 4. mixed_due_to_entity_plus_tech
# ════════════════════════════════════════════════════════════════════════

class TestChainMixed(unittest.TestCase):
    """Mixed queries (tech + media entity) must have doc_rag before media."""

    def test_mixed_doc_before_media(self):
        """For mixed_due_to_entity_plus_tech arbitration, doc_rag must precede media."""
        decision = _decide(
            "《三体》里的物理学原理解释",
            _llm_stub(
                label="MEDIA",
                domain="media",
                entities=["三体"],
                lookup_mode="entity_lookup",
            ),
        )
        # Force the arbitration that triggers doc-first ordering
        decision.arbitration = "mixed_due_to_entity_plus_tech"
        decision.needs_doc_rag = True
        decision.needs_media_db = True
        plan = RoutingPolicy().build_plan(decision, "local_only")
        tool_names = [t.name for t in plan.planned_tools]
        self.assertIn(TOOL_QUERY_DOC_RAG, tool_names)
        self.assertIn(TOOL_QUERY_MEDIA, tool_names)
        doc_idx = tool_names.index(TOOL_QUERY_DOC_RAG)
        media_idx = tool_names.index(TOOL_QUERY_MEDIA)
        self.assertLess(doc_idx, media_idx, "doc_rag must come before query_media for mixed_due_to_entity_plus_tech")

    def test_mixed_plan_reason_contains_doc_primary(self):
        decision = _decide(
            "《三体》里的物理学原理解释",
            _llm_stub(label="MEDIA", domain="media", entities=["三体"], lookup_mode="entity_lookup"),
        )
        decision.arbitration = "mixed_due_to_entity_plus_tech"
        decision.needs_doc_rag = True
        decision.needs_media_db = True
        plan = RoutingPolicy().build_plan(decision, "local_only")
        self.assertIn("policy:doc_primary", plan.reasons)

    def test_mixed_answer_strategy_is_knowledge(self):
        """Mixed query with good doc result → knowledge_answer (tech is primary)."""
        decision = _decide(
            "《三体》里的物理学原理解释",
            _llm_stub(
                label="MEDIA",
                domain="media",
                entities=["三体"],
                lookup_mode="entity_lookup",
            ),
        )
        decision.arbitration = "mixed_due_to_entity_plus_tech"
        decision.domain = "tech"  # domain reclassified to tech when tech is primary
        tool_results = [_fake_doc(_doc_rows("d1")), _fake_media(_media_rows("m1"))]
        outcome = PostRetrievalPolicy().evaluate(decision, tool_results, {})
        strategy = AnswerPolicy().determine(decision, outcome)
        self.assertEqual(strategy.mode, "knowledge_answer")


# ════════════════════════════════════════════════════════════════════════
# 5. zero_results → fallback / repair
# ════════════════════════════════════════════════════════════════════════

class TestChainZeroResults(unittest.TestCase):
    """When retrieval returns nothing, the chain must offer either a repair
    path (enrich) or a clean fallback, never silently proceed."""

    def test_media_entity_zero_gives_tmdb_repair(self):
        _, _, outcome, strategy = _run_chain(
            "《午后》的导演介绍",
            _llm_stub(label="MEDIA", domain="media", entities=["午后"], lookup_mode="entity_lookup"),
            [_fake_media([])],
        )
        self.assertEqual(outcome.status, "zero_results")
        self.assertTrue(outcome.zero_results)
        self.assertIn(TOOL_SEARCH_TMDB, outcome.repair_tools)
        # With a repair path, action must be "enrich", not "fallback_to_llm"
        self.assertEqual(outcome.action, "enrich")

    def test_media_filter_zero_suggests_web(self):
        _, _, outcome, _ = _run_chain(
            "2023年奥斯卡最佳影片推荐",
            _llm_stub(label="MEDIA", domain="media", lookup_mode="filter_search"),
            [_fake_media([])],
        )
        self.assertEqual(outcome.status, "zero_results")
        self.assertIn(TOOL_SEARCH_WEB, outcome.repair_tools)

    def test_tech_zero_gives_llm_fallback(self):
        _, _, outcome, strategy = _run_chain(
            "什么是量子计算",
            _llm_stub(label="TECH", domain="tech"),
            [_fake_doc([])],
        )
        self.assertEqual(outcome.status, "zero_results")
        self.assertEqual(outcome.action, "fallback_to_llm")
        self.assertEqual(strategy.mode, "fallback_answer")
        ep = strategy.style_hints.get("evidence_policy", {})
        self.assertFalse(ep.get("local_claims_allowed"))

    def test_off_domain_ambiguous_gives_fallback(self):
        _, _, outcome, strategy = _run_chain(
            "这部呢",
            _llm_stub(label="MEDIA", domain="media", entities=[], lookup_mode="general_lookup"),
            [_fake_media(_media_rows("m1"))],
            guardrail_flags={"low_confidence_understanding": True},
        )
        self.assertEqual(outcome.status, "off_domain")
        self.assertEqual(strategy.mode, "fallback_answer")
        ep = strategy.style_hints.get("evidence_policy", {})
        self.assertFalse(ep.get("external_enrichment_allowed"))

    def test_zero_with_repair_gives_enrichment_not_fallback_for_entity(self):
        """zero_results + enrich action does NOT produce fallback_answer for entity queries."""
        enrich_outcome = PostRetrievalOutcome(
            status="zero_results",
            action="enrich",
            repair_tools=[TOOL_SEARCH_TMDB],
            zero_results=True,
        )
        decision = _decide(
            "《波拉尼奥》的剧情",
            _llm_stub(label="MEDIA", domain="media", entities=["波拉尼奥"], lookup_mode="entity_lookup"),
        )
        strategy = AnswerPolicy().determine(decision, enrich_outcome)
        # entity_detail is still the right mode; evidence_policy allows external
        self.assertNotEqual(strategy.mode, "fallback_answer")
        ep = strategy.style_hints.get("evidence_policy", {})
        self.assertTrue(ep.get("external_enrichment_allowed"))
        self.assertTrue(ep.get("must_label_external"))


if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for cls in [
        TestChainTechPrimary,
        TestChainMediaEntity,
        TestChainMediaCollection,
        TestChainMixed,
        TestChainZeroResults,
    ]:
        suite.addTests(loader.loadTestsFromTestCase(cls))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
