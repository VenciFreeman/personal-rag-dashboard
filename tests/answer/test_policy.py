"""Unit tests for AnswerPolicy.

Run:
    python -m pytest tests/answer/test_policy.py -v
    python tests/answer/test_policy.py
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

# Ensure repo root is on sys.path so 'tests' package is importable
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tests.conftest import svc, _decide, _llm_stub

from nav_dashboard.web.services.answer_policy import AnswerPolicy, AnswerStrategy
from nav_dashboard.web.services.post_retrieval_policy import PostRetrievalOutcome


def _proceed() -> PostRetrievalOutcome:
    return PostRetrievalOutcome(status="proceed", action="use_results")


def _zero() -> PostRetrievalOutcome:
    return PostRetrievalOutcome(
        status="zero_results",
        action="fallback_to_llm",
        degrade_reason="no_retrieval_results",
        zero_results=True,
    )


def _degrade(reason: str = "insufficient_valid_results") -> PostRetrievalOutcome:
    return PostRetrievalOutcome(
        status="weak_results",
        action="degrade",
        degrade_reason=reason,
        weak_results=True,
    )


def _off_domain() -> PostRetrievalOutcome:
    return PostRetrievalOutcome(
        status="off_domain",
        action="degrade",
        off_domain=True,
        degrade_reason="low_confidence_understanding",
    )


class TestAnswerPolicyFallback(unittest.TestCase):
    policy = AnswerPolicy()

    def test_zero_results_no_repair_gives_fallback(self):
        """zero_results with fallback_to_llm (no repair path) → fallback_answer."""
        d = _decide("推荐几部2020年的法国电影",
                    _llm_stub(label="MEDIA", domain="media", lookup_mode="filter_search"))
        strategy = self.policy.determine(d, _zero())
        self.assertEqual(strategy.mode, "fallback_answer")
        self.assertTrue(strategy.style_hints.get("zero_results"))

    def test_degrade_without_off_domain_gives_correct_mode_not_fallback(self):
        """weak_results (not off-domain) → must_weaken evidence policy, NOT fallback_answer."""
        d = _decide("2020年悬疑片推荐",
                    _llm_stub(label="MEDIA", domain="media", lookup_mode="filter_search"))
        strategy = self.policy.determine(d, _degrade())
        self.assertNotEqual(strategy.mode, "fallback_answer")
        ep = strategy.style_hints.get("evidence_policy", {})
        self.assertTrue(ep.get("must_weaken_uncertain_claims"))

    def test_off_domain_gives_fallback(self):
        d = _decide("这部呢",
                    _llm_stub(label="MEDIA", domain="media", entities=[], lookup_mode="general_lookup"))
        strategy = self.policy.determine(d, _off_domain())
        self.assertEqual(strategy.mode, "fallback_answer")
        self.assertTrue(strategy.style_hints.get("off_domain"))


class TestAnswerPolicyEntityDetail(unittest.TestCase):
    policy = AnswerPolicy()

    def test_single_entity_lookup_gives_entity_detail(self):
        d = _decide(
            "《教父》的导演是谁",
            _llm_stub(label="MEDIA", domain="media", entities=["教父"], lookup_mode="entity_lookup"),
        )
        strategy = self.policy.determine(d, _proceed())
        self.assertEqual(strategy.mode, "entity_detail_answer")
        self.assertEqual(strategy.style_hints.get("entity"), "教父")

    def test_explanation_request_propagated(self):
        d = _decide(
            "《骨之收藏》的导演和剧情介绍",
            _llm_stub(label="MEDIA", domain="media", entities=["骨之收藏"],
                      lookup_mode="entity_lookup"),
        )
        strategy = self.policy.determine(d, _proceed())
        self.assertEqual(strategy.mode, "entity_detail_answer")
        self.assertTrue(strategy.style_hints.get("needs_explanation"))


class TestAnswerPolicyCollection(unittest.TestCase):
    policy = AnswerPolicy()

    def test_filter_search_gives_collection(self):
        d = _decide(
            "推荐几部2020年的法国电影",
            _llm_stub(label="MEDIA", domain="media", lookup_mode="filter_search",
                      filters={"region": ["法国"], "year": ["2020"]}),
        )
        strategy = self.policy.determine(d, _proceed())
        self.assertEqual(strategy.mode, "collection_answer")
        self.assertTrue(strategy.style_hints.get("list_style"))

    def test_multi_entity_gives_collection(self):
        # Two entities → collection even if lookup_mode is entity_lookup
        d = _decide(
            "比较波拉尼奥和马尔克斯的风格",
            _llm_stub(label="MEDIA", domain="media",
                      entities=["波拉尼奥", "马尔克斯"], lookup_mode="entity_lookup"),
        )
        strategy = self.policy.determine(d, _proceed())
        self.assertEqual(strategy.mode, "collection_answer")


class TestAnswerPolicyKnowledge(unittest.TestCase):
    policy = AnswerPolicy()

    def test_tech_gives_knowledge(self):
        d = _decide(
            "机器学习的概念和应用",
            _llm_stub(label="TECH", domain="tech"),
        )
        strategy = self.policy.determine(d, _proceed())
        self.assertEqual(strategy.mode, "knowledge_answer")

    def test_general_gives_knowledge(self):
        d = _decide(
            "什么是量子纠缠",
            _llm_stub(label="OTHER", domain="general"),
        )
        strategy = self.policy.determine(d, _proceed())
        self.assertEqual(strategy.mode, "knowledge_answer")

    def test_tech_narrative_hint_set(self):
        d = _decide(
            "深度学习架构原理是什么",
            _llm_stub(label="TECH", domain="tech"),
        )
        strategy = self.policy.determine(d, _proceed())
        self.assertEqual(strategy.mode, "knowledge_answer")
        self.assertTrue(strategy.style_hints.get("needs_narrative"))


class TestAnswerPolicyReturnType(unittest.TestCase):
    policy = AnswerPolicy()

    def test_returns_answer_strategy(self):
        d = _decide("什么是量子纠缠", _llm_stub(label="OTHER", domain="general"))
        result = self.policy.determine(d, _proceed())
        self.assertIsInstance(result, AnswerStrategy)

    def test_mode_is_always_set(self):
        for query, stub, outcome in [
            ("机器学习", _llm_stub(label="TECH", domain="tech"), _proceed()),
            ("《三体》的作者", _llm_stub(label="MEDIA", domain="media", entities=["三体"], lookup_mode="entity_lookup"), _proceed()),
            ("推荐电影", _llm_stub(label="MEDIA", domain="media", lookup_mode="filter_search"), _zero()),
        ]:
            d = _decide(query, stub)
            s = self.policy.determine(d, outcome)
            self.assertIn(s.mode, ("knowledge_answer", "collection_answer", "entity_detail_answer", "fallback_answer"))


class TestAnswerPolicyEvidencePolicy(unittest.TestCase):
    """Evidence-policy dict must be present in style_hints for every outcome."""
    policy = AnswerPolicy()

    def _ep(self, strategy: AnswerStrategy) -> dict:
        ep = strategy.style_hints.get("evidence_policy")
        self.assertIsInstance(ep, dict, "evidence_policy missing from style_hints")
        return ep

    def test_proceed_local_claims_allowed(self):
        d = _decide("《教父》的导演是谁",
                    _llm_stub(label="MEDIA", domain="media", entities=["教父"], lookup_mode="entity_lookup"))
        ep = self._ep(self.policy.determine(d, _proceed()))
        self.assertTrue(ep["local_claims_allowed"])
        self.assertFalse(ep["external_enrichment_allowed"])  # no repair_tools in proceed

    def test_off_domain_no_local_claims(self):
        d = _decide("这部呢",
                    _llm_stub(label="MEDIA", domain="media", entities=[], lookup_mode="general_lookup"))
        ep = self._ep(self.policy.determine(d, _off_domain()))
        self.assertFalse(ep["local_claims_allowed"])
        self.assertFalse(ep["external_enrichment_allowed"])

    def test_zero_no_repair_no_external(self):
        d = _decide("推荐几還2020年的法国电影",
                    _llm_stub(label="MEDIA", domain="media", lookup_mode="filter_search"))
        ep = self._ep(self.policy.determine(d, _zero()))
        self.assertFalse(ep["local_claims_allowed"])
        self.assertFalse(ep["external_enrichment_allowed"])

    def test_enrich_zero_external_allowed(self):
        """zero_results with enrich action → external allowed, must_label."""
        enrich_zero = PostRetrievalOutcome(
            status="zero_results",
            action="enrich",
            repair_tools=["search_tmdb_media"],
            zero_results=True,
        )
        d = _decide("《波拉尼奥》的导演",
                    _llm_stub(label="MEDIA", domain="media", entities=["波拉尼奥"], lookup_mode="entity_lookup"))
        ep = self._ep(self.policy.determine(d, enrich_zero))
        self.assertFalse(ep["local_claims_allowed"])
        self.assertTrue(ep["external_enrichment_allowed"])
        self.assertTrue(ep["must_label_external"])

    def test_partial_local_external_allowed_must_label(self):
        partial = PostRetrievalOutcome(
            status="partial_local",
            action="enrich",
            repair_tools=["search_tmdb_media", "expand_mediawiki_concept"],
            partial_local=True,
        )
        d = _decide("《白色相剌2》的剧情简介",
                    _llm_stub(label="MEDIA", domain="media", entities=["白色相剌2"], lookup_mode="entity_lookup"))
        strategy = self.policy.determine(d, partial)
        self.assertEqual(strategy.mode, "entity_detail_answer")
        ep = self._ep(strategy)
        self.assertTrue(ep["local_claims_allowed"])
        self.assertTrue(ep["external_enrichment_allowed"])
        self.assertTrue(ep["must_label_external"])

    def test_weak_must_weaken_claims(self):
        d = _decide("推荐悬疑片",
                    _llm_stub(label="MEDIA", domain="media", lookup_mode="filter_search"))
        strategy = self.policy.determine(d, _degrade())
        self.assertNotEqual(strategy.mode, "fallback_answer")
        ep = self._ep(strategy)
        self.assertTrue(ep["must_weaken_uncertain_claims"])
        self.assertTrue(ep["local_claims_allowed"])


class TestAnswerStrategyContract(unittest.TestCase):
    """Contract tests for AnswerPolicy response_structure and evidence_policy.

    Verifies that the 5-slot plan contract flows correctly through AnswerPolicy:
      - subject_scope=personal_record   → must_label_external always True
      - personal + list_only            → external_enrichment_allowed=False
      - personal + list_plus_expand     → external_enrichment_allowed=True
      - answer_shape=compare            → response_structure="compare"
      - answer_shape=list_plus_expand   → response_structure="local_list_plus_external_background"
      - media_abstract_concept class    → response_structure="thematic_list"
    """
    policy = AnswerPolicy()

    def _ep(self, strategy: AnswerStrategy) -> dict:
        return strategy.style_hints.get("evidence_policy", {})

    def _rs(self, strategy: AnswerStrategy) -> str:
        return str(strategy.style_hints.get("response_structure") or "")

    # ── personal_record evidence policy ──────────────────────────────────────

    def test_personal_list_only_external_not_allowed(self):
        """personal_record + list_only → external_enrichment_allowed must be False."""
        d = _decide(
            "我看过哪些悬疑剧",
            _llm_stub(label="MEDIA", domain="media", lookup_mode="filter_search"),
        )
        self.assertEqual(d.subject_scope, "personal_record")
        self.assertEqual(d.answer_shape, "list_only")
        strategy = self.policy.determine(d, _proceed())
        ep = self._ep(strategy)
        self.assertFalse(
            ep.get("external_enrichment_allowed"),
            "personal_record + list_only must not allow external enrichment",
        )

    def test_personal_list_only_must_label_external(self):
        """personal_record always requires must_label_external regardless of shape."""
        d = _decide(
            "我看过哪些悬疑剧",
            _llm_stub(label="MEDIA", domain="media", lookup_mode="filter_search"),
        )
        strategy = self.policy.determine(d, _proceed())
        ep = self._ep(strategy)
        self.assertTrue(
            ep.get("must_label_external"),
            "personal_record must always set must_label_external",
        )

    def test_personal_expand_external_allowed(self):
        """personal_record + list_plus_expand → external_enrichment_allowed=True."""
        d = _decide(
            "我看过哪些剧，分别介绍一下",
            _llm_stub(label="MEDIA", domain="media", lookup_mode="filter_search"),
        )
        self.assertEqual(d.subject_scope, "personal_record")
        self.assertEqual(d.answer_shape, "list_plus_expand")
        strategy = self.policy.determine(d, _proceed())
        ep = self._ep(strategy)
        self.assertTrue(
            ep.get("external_enrichment_allowed"),
            "personal_record + list_plus_expand must allow external enrichment",
        )

    # ── response_structure derivation ────────────────────────────────────────

    def test_compare_shape_gives_compare_structure(self):
        """answer_shape=compare → response_structure='compare'."""
        d = _decide(
            "《教父》和《教父2》哪个更好",
            _llm_stub(label="MEDIA", domain="media",
                      entities=["教父", "教父2"],
                      lookup_mode="entity_lookup"),
        )
        # Force comparison signal
        from nav_dashboard.web.services.agent_types import RouterDecision
        d2 = RouterDecision(
            raw_question=d.raw_question,
            resolved_question=d.resolved_question,
            intent="media_lookup",
            domain="media",
            lookup_mode="filter_search",
            entities=["教父", "教父2"],
            needs_comparison=True,
            query_class="media_collection_filter",
            answer_shape="compare",
            subject_scope="general_knowledge",
        )
        strategy = self.policy.determine(d2, _proceed())
        self.assertEqual(self._rs(strategy), "compare")

    def test_list_plus_expand_shape_gives_local_list_plus_external(self):
        """answer_shape=list_plus_expand → response_structure contains 'local_list_plus'."""
        d = _decide(
            "我近两年看过哪些剧，分别介绍一下",
            _llm_stub(label="MEDIA", domain="media", lookup_mode="filter_search"),
        )
        strategy = self.policy.determine(d, _proceed())
        self.assertIn(
            "local_list_plus", self._rs(strategy),
            "list_plus_expand must produce a local_list_plus_* response_structure",
        )

    def test_media_abstract_concept_gives_thematic_list(self):
        """media_abstract_concept query_class → response_structure='thematic_list'."""
        from nav_dashboard.web.services.agent_types import RouterDecision
        d = RouterDecision(
            raw_question="新现实主义电影和老电影的区别",
            resolved_question="新现实主义电影和老电影的区别",
            intent="media_lookup",
            domain="media",
            lookup_mode="concept_lookup",
            query_class="media_abstract_concept",
            answer_shape="list_only",
            subject_scope="general_knowledge",
            evidence={"abstract_media_concept": True},
        )
        strategy = self.policy.determine(d, _proceed())
        self.assertEqual(self._rs(strategy), "thematic_list")

    def test_entity_detail_gives_local_record_plus_external(self):
        """entity_lookup + single entity → response_structure='local_record_plus_external_info'."""
        d = _decide(
            "《教父》的导演是谁",
            _llm_stub(label="MEDIA", domain="media",
                      entities=["教父"], lookup_mode="entity_lookup"),
        )
        strategy = self.policy.determine(d, _proceed())
        self.assertEqual(self._rs(strategy), "local_record_plus_external_info")

    # ── style_hints contract completeness ────────────────────────────────────

    def test_collection_strategy_carries_subject_scope(self):
        """Collection strategies must forward subject_scope in style_hints."""
        d = _decide(
            "我看过哪些悬疑剧",
            _llm_stub(label="MEDIA", domain="media", lookup_mode="filter_search"),
        )
        strategy = self.policy.determine(d, _proceed())
        self.assertEqual(strategy.style_hints.get("subject_scope"), "personal_record")

    def test_collection_strategy_carries_expansion_flags(self):
        """Collection strategies must expose expansion state fields in style_hints."""
        from nav_dashboard.web.services.post_retrieval_policy import PostRetrievalOutcome
        partial_with_expansion = PostRetrievalOutcome(
            status="partial_local",
            action="use_results",
            partial_local=True,
            needs_expansion=True,
            expansion_missing=True,
        )
        d = _decide(
            "我近两年看过哪些剧，分别介绍一下",
            _llm_stub(label="MEDIA", domain="media", lookup_mode="filter_search"),
        )
        strategy = self.policy.determine(d, partial_with_expansion)
        self.assertIn("expansion_missing", strategy.style_hints,
                      "expansion_missing must be forwarded in style_hints")
        self.assertIn("expansion_unavailable", strategy.style_hints,
                      "expansion_unavailable must be forwarded in style_hints")


if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for cls in [
        TestAnswerPolicyFallback,
        TestAnswerPolicyEntityDetail,
        TestAnswerPolicyCollection,
        TestAnswerPolicyKnowledge,
        TestAnswerPolicyReturnType,
        TestAnswerPolicyEvidencePolicy,
        TestAnswerStrategyContract,
    ]:
        suite.addTests(loader.loadTestsFromTestCase(cls))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
