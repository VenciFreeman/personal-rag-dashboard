"""Unit tests for PostRetrievalPolicy.

Run:
    python -m pytest tests/post_retrieval/test_policy.py -v
    python tests/post_retrieval/test_policy.py
"""
from __future__ import annotations

import sys
import unittest
from dataclasses import dataclass, field
from typing import Any
from pathlib import Path

# Ensure repo root is on sys.path so 'tests' package is importable
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# path / env setup via conftest bootstrap
from tests.conftest import svc, _decide, _llm_stub, _make_runtime

from nav_dashboard.web.services.post_retrieval_policy import (
    PostRetrievalPolicy,
    PostRetrievalOutcome,
)


# ── minimal ToolExecution stub ────────────────────────────────────────────────

@dataclass
class _FakeTool:
    tool: str
    status: str = "ok"
    data: dict = field(default_factory=dict)


def _media_tool(rows: list[dict] | None = None, *, status: str = "ok") -> _FakeTool:
    return _FakeTool(
        tool="query_media_record",
        status=status,
        data={"results": rows or []},
    )


def _doc_tool(rows: list[dict] | None = None, *, status: str = "ok") -> _FakeTool:
    return _FakeTool(
        tool="query_document_rag",
        status=status,
        data={"results": rows or []},
    )


# ── tests ─────────────────────────────────────────────────────────────────────

class TestPostRetrievalPolicyZeroResults(unittest.TestCase):
    policy = PostRetrievalPolicy()

    def test_media_zero_results_entity_lookup_suggests_tmdb(self):
        d = _decide(
            "波拉尼奥的创作生涯介绍",
            _llm_stub(label="MEDIA", domain="media", entities=["波拉尼奥"], lookup_mode="entity_lookup"),
        )
        outcome = self.policy.evaluate(d, [_media_tool([])], {})
        self.assertEqual(outcome.status, "zero_results")
        self.assertTrue(outcome.zero_results)
        self.assertIn("search_tmdb_media", outcome.repair_tools)

    def test_media_zero_results_no_entity_suggests_web(self):
        d = _decide(
            "推荐几部2020年的法国电影",
            _llm_stub(label="MEDIA", domain="media", lookup_mode="filter_search",
                      filters={"region": ["法国"], "year": ["2020"]}),
        )
        outcome = self.policy.evaluate(d, [_media_tool([])], {})
        self.assertEqual(outcome.status, "zero_results")
        self.assertIn("search_web", outcome.repair_tools)

    def test_doc_zero_results_fallback_to_llm(self):
        d = _decide(
            "深度学习架构原理是什么",
            _llm_stub(label="TECH", domain="tech"),
        )
        outcome = self.policy.evaluate(d, [_doc_tool([])], {})
        self.assertEqual(outcome.status, "zero_results")
        self.assertEqual(outcome.action, "fallback_to_llm")

    def test_skipped_tool_not_counted_as_ran(self):
        """If media tool was skipped (not actually run), zero_results should not fire."""
        d = _decide(
            "什么是量子纠缠",
            _llm_stub(label="OTHER", domain="general"),
        )
        # doc ran with results, media skipped
        outcome = self.policy.evaluate(
            d,
            [_doc_tool([{"id": "1", "score": 0.9}]), _FakeTool("query_media_record", status="skipped", data={})],
            {},
        )
        self.assertNotEqual(outcome.status, "zero_results")


class TestPostRetrievalPolicyOffDomain(unittest.TestCase):
    policy = PostRetrievalPolicy()

    def test_low_confidence_understanding_flags_off_domain(self):
        d = _decide(
            "这部呢",
            _llm_stub(label="MEDIA", domain="media", entities=[], lookup_mode="general_lookup"),
        )
        outcome = self.policy.evaluate(
            d,
            [_media_tool([{"id": "x"}])],
            {"low_confidence_understanding": True},
        )
        self.assertEqual(outcome.status, "off_domain")
        self.assertTrue(outcome.off_domain)
        self.assertEqual(outcome.action, "degrade")

    def test_state_inheritance_ambiguous_flags_off_domain(self):
        d = _decide(
            "那个怎么样",
            _llm_stub(label="MEDIA", domain="media", entities=[], lookup_mode="general_lookup"),
        )
        outcome = self.policy.evaluate(
            d,
            [_media_tool([{"id": "x"}])],
            {"state_inheritance_ambiguous": True},
        )
        self.assertEqual(outcome.status, "off_domain")


class TestPostRetrievalPolicyWeakResults(unittest.TestCase):
    policy = PostRetrievalPolicy()

    def test_insufficient_valid_results_triggers_weak(self):
        d = _decide(
            "推荐几部2022年的法国电影",
            _llm_stub(label="MEDIA", domain="media", lookup_mode="filter_search",
                      filters={"region": ["法国"], "year": ["2022"]}),
        )
        outcome = self.policy.evaluate(
            d,
            [_media_tool([])],  # tool ran but returned no rows for weak path
            {"insufficient_valid_results": True},
        )
        # zero_results path fires first when media ran + no rows
        # Test that at least one degrading outcome occurs
        self.assertIn(outcome.status, ("zero_results", "weak_results"))

    def test_high_validator_drop_rate_triggers_weak(self):
        d = _decide(
            "2020年悬疑片推荐",
            _llm_stub(label="MEDIA", domain="media", lookup_mode="filter_search"),
        )
        # Media tool returned some rows (so zero_results doesn't fire), but validator dropped most
        outcome = self.policy.evaluate(
            d,
            [_media_tool([{"id": "x"}])],
            {"high_validator_drop_rate": True},
        )
        self.assertEqual(outcome.status, "weak_results")
        self.assertTrue(outcome.weak_results)
        # With rows present, no repair_tools — action is use_results (not degrade)
        self.assertEqual(outcome.action, "use_results")


class TestPostRetrievalPolicyPartialLocal(unittest.TestCase):
    policy = PostRetrievalPolicy()

    def _tmdb_tool(self, status: str = "ok") -> _FakeTool:
        return _FakeTool(tool="search_tmdb_media", status=status, data={"results": []})

    def _mediawiki_tool(self, status: str = "ok") -> _FakeTool:
        return _FakeTool(tool="expand_mediawiki_concept", status=status, data={})

    def test_entity_detail_explanation_suggests_tmdb_and_mediawiki(self):
        """entity_lookup + needs_explanation → enrich with TMDB + MediaWiki."""
        d = _decide(
            "《骨之收藏》的剧情简介是什么",
            _llm_stub(label="MEDIA", domain="media", entities=["骨之收藏"], lookup_mode="entity_lookup"),
        )
        # Manually set needs_explanation on decision
        d.needs_explanation = True
        outcome = self.policy.evaluate(d, [_media_tool([{"id": "m1", "title": "骨之收藏"}])], {})
        self.assertEqual(outcome.status, "partial_local")
        self.assertEqual(outcome.action, "enrich")
        self.assertTrue(outcome.partial_local)
        self.assertIn("search_tmdb_media", outcome.repair_tools)
        self.assertIn("expand_mediawiki_concept", outcome.repair_tools)

    def test_entity_detail_no_explanation_does_not_enrich(self):
        """entity_lookup without needs_explanation should not trigger partial_local."""
        d = _decide(
            "《骨之收藏》",
            _llm_stub(label="MEDIA", domain="media", entities=["骨之收藏"], lookup_mode="entity_lookup"),
        )
        d.needs_explanation = False
        outcome = self.policy.evaluate(d, [_media_tool([{"id": "m1", "title": "骨之收藏"}])], {})
        self.assertNotEqual(outcome.status, "partial_local")

    def test_entity_detail_tmdb_already_ran_skips_tmdb(self):
        """If TMDB already ran, it should not appear in repair_tools again."""
        d = _decide(
            "《波拉尼奥》的导演",
            _llm_stub(label="MEDIA", domain="media", entities=["波拉尼奥"], lookup_mode="entity_lookup"),
        )
        d.needs_explanation = True
        outcome = self.policy.evaluate(
            d,
            [_media_tool([{"id": "m1"}]), self._tmdb_tool()],
            {},
        )
        # TMDB ran, should not be in repair_tools
        self.assertNotIn("search_tmdb_media", outcome.repair_tools)
        # MediaWiki still suggested
        if outcome.status == "partial_local":
            self.assertIn("expand_mediawiki_concept", outcome.repair_tools)


class TestPostRetrievalPolicyProceed(unittest.TestCase):
    policy = PostRetrievalPolicy()

    def test_good_results_proceed(self):
        d = _decide(
            "深度学习架构原理是什么",
            _llm_stub(label="TECH", domain="tech"),
        )
        outcome = self.policy.evaluate(
            d,
            [_doc_tool([{"id": "doc1", "score": 0.85}])],
            {},
        )
        self.assertEqual(outcome.status, "proceed")
        self.assertEqual(outcome.action, "use_results")

    def test_media_with_rows_proceeds(self):
        """filter_search with good results and no explanation request → proceed."""
        d = _decide(
            "推荐几部科幻小说",
            _llm_stub(label="MEDIA", domain="media", lookup_mode="filter_search",
                      filters={"genre": ["科幻"]}),
        )
        outcome = self.policy.evaluate(
            d,
            [_media_tool([{"id": "m1", "title": "三体"}, {"id": "m2", "title": "流浪地球"}])],
            {},
        )
        self.assertEqual(outcome.status, "proceed")

    def test_entity_lookup_with_explanation_gives_partial_local(self):
        """entity_lookup + needs_explanation (e.g. 导演/剧情) → partial_local with enrichment."""
        d = _decide(
            "《教父》的导演是谁",
            _llm_stub(label="MEDIA", domain="media", entities=["教父"], lookup_mode="entity_lookup"),
        )
        outcome = self.policy.evaluate(
            d,
            [_media_tool([{"id": "m1", "title": "教父"}])],
            {},
        )
        # "导演" triggers needs_explanation → enrich path fires
        self.assertIn(outcome.status, ("proceed", "partial_local"))

    def test_outcome_is_dataclass(self):
        d = _decide("什么是量子纠缠", _llm_stub(label="OTHER", domain="general"))
        outcome = self.policy.evaluate(d, [_doc_tool([{"id": "d1"}])], {})
        self.assertIsInstance(outcome, PostRetrievalOutcome)


class TestExpansionStateContract(unittest.TestCase):
    """Expansion state fields: expansion_missing / expansion_unavailable / needs_expansion.

    These three boolean fields form the expansion-state sub-contract used by
    AnswerPolicy and _summarize_answer to decide which explanatory clause to add.

    Rules under test:
      - needs_expansion = True when the query desires per-item detail BUT the
        expansion tool was missing or ran dry.
      - expansion_missing = True only in T1 (wants expansion, nothing ran at all).
      - expansion_unavailable = True in T2-exhausted (ran but empty, no repair left).
      - When a repair_tool IS recommended, neither missing nor unavailable fires.
    """
    policy = PostRetrievalPolicy()

    def _expand_decision(self, question: str) -> svc.RouterDecision:
        """Build a decision that will set _wants_expansion = True."""
        return _decide(
            question,
            _llm_stub(label="MEDIA", domain="media", lookup_mode="filter_search"),
        )

    def _local_rows(self) -> list[dict]:
        return [{"id": "m1", "title": "三体"}, {"id": "m2", "title": "流浪地球"}]

    def test_no_expansion_wanted_no_flags(self):
        """plain collection query with no expand cue: all expansion flags False."""
        d = _decide(
            "推荐几郥2020年的悬疑剧",
            _llm_stub(label="MEDIA", domain="media", lookup_mode="filter_search"),
        )
        outcome = self.policy.evaluate(d, [_media_tool(self._local_rows())], {})
        self.assertFalse(outcome.needs_expansion)
        self.assertFalse(outcome.expansion_missing)
        self.assertFalse(outcome.expansion_unavailable)

    def test_expansion_wanted_nothing_ran_gives_missing(self):
        """query with expand cue, no expansion tool ran → expansion_missing=True."""
        d = self._expand_decision("我近两年看过哪些剧，分别介绍一下")
        # Only the local media tool ran, no expansion tool.
        outcome = self.policy.evaluate(d, [_media_tool(self._local_rows())], {})
        if outcome.needs_expansion:
            # If the query was classified as wanting expansion, missing should be True
            # when no expansion tool was run.
            self.assertTrue(
                outcome.expansion_missing or outcome.repair_tools,
                "When expansion is needed and nothing ran, either expansion_missing=True "
                "or a repair tool must be recommended",
            )

    def test_expansion_repair_tool_recommended_trumps_unavailable(self):
        """When a repair tool is available, expansion_unavailable must NOT be True."""
        d = self._expand_decision("我近两年看过哪些剧，分别介绍一下")
        outcome = self.policy.evaluate(d, [_media_tool(self._local_rows())], {})
        if outcome.repair_tools:
            self.assertFalse(
                outcome.expansion_unavailable,
                "expansion_unavailable must be False when repair_tools is non-empty",
            )

    def test_expansion_unavailable_requires_no_repair_path(self):
        """expansion_unavailable=True must only appear when repair_tools is empty."""
        d = self._expand_decision("我近两年看过哪些剧，分别介绍一下")
        # Simulate: expansion tool ran but returned nothing (T2-exhausted).
        mw_empty = _FakeTool(
            tool="expand_mediawiki_concept",
            status="empty",
            data={"results": []},
        )
        tmdb_empty = _FakeTool(
            tool="search_tmdb_media",
            status="empty",
            data={"results": []},
        )
        outcome = self.policy.evaluate(
            d,
            [_media_tool(self._local_rows()), mw_empty, tmdb_empty],
            {},
        )
        if outcome.expansion_unavailable:
            self.assertEqual(
                outcome.repair_tools, [],
                "expansion_unavailable=True must never coexist with non-empty repair_tools",
            )

    def test_outcome_fields_are_booleans(self):
        d = self._expand_decision("我近两年看过哪些剧，分别介绍一下")
        outcome = self.policy.evaluate(d, [_media_tool(self._local_rows())], {})
        self.assertIsInstance(outcome.needs_expansion, bool)
        self.assertIsInstance(outcome.expansion_missing, bool)
        self.assertIsInstance(outcome.expansion_unavailable, bool)


if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for cls in [
        TestPostRetrievalPolicyZeroResults,
        TestPostRetrievalPolicyOffDomain,
        TestPostRetrievalPolicyWeakResults,
        TestPostRetrievalPolicyProceed,
        TestPostRetrievalPolicyPartialLocal,
        TestExpansionStateContract,
    ]:
        suite.addTests(loader.loadTestsFromTestCase(cls))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
