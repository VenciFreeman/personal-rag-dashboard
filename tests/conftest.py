"""Shared fixtures for nav_dashboard regression tests.

Importing `svc` here so each test module can do::

    from tests.conftest import svc, _decide, _llm_stub, _profile, _make_runtime

instead of duplicating bootstrap code.
"""
from __future__ import annotations

import os
from unittest.mock import patch

from tests._app_bootstrap import ensure_repo_on_path

ensure_repo_on_path()

# Minimal env stubs so imports that reach for LLM clients don't crash.
os.environ.setdefault("DEEPSEEK_API_KEY", "stub")
os.environ.setdefault("LOCAL_LLM_URL", "http://127.0.0.1:11434")

import nav_dashboard.web.services.agent_service as svc  # noqa: E402
from nav_dashboard.web.services.agent.domain import router_service as router_owner  # noqa: E402

# ── test helpers ──────────────────────────────────────────────────────────────
_EMPTY_QUOTA: dict = {}


def _profile(query: str) -> dict:
    return svc._resolve_query_profile(query)


def _llm_stub(
    *,
    label: str = "OTHER",
    domain: str = "general",
    lookup_mode: str = "general_lookup",
    entities: list[str] | None = None,
    filters: dict | None = None,
    confidence: float = 0.8,
) -> dict:
    """Build a fake _classify_media_query_with_llm return value."""
    return {
        "label": label,
        "domain": domain,
        "lookup_mode": lookup_mode,
        "entities": entities or [],
        "filters": filters or {},
        "time_window": {},
        "ranking": {},
        "followup_target": "",
        "needs_comparison": False,
        "needs_explanation": False,
        "confidence": confidence,
        "rewritten_queries": {},
    }


def _decide(
    question: str,
    llm_response: dict,
    *,
    previous_state: dict | None = None,
) -> svc.RouterDecision:
    """Call _build_router_decision with mocked LLM + no rewrite LLM calls."""
    with (
        patch.object(svc, "_classify_media_query_with_llm", return_value=llm_response),
        patch.object(svc, "_rewrite_tool_queries_with_llm", return_value={}),
    ):
        base_deps = router_owner.build_default_router_deps()
        deps = router_owner.RouterDeps(
            llm_chat=base_deps.llm_chat,
            find_previous_trace_context=base_deps.find_previous_trace_context,
            apply_router_semantic_repairs=base_deps.apply_router_semantic_repairs,
            resolve_library_aliases=svc._resolve_library_aliases,
            planner_router_semantic_deps=base_deps.planner_router_semantic_deps,
            perf_counter=base_deps.perf_counter,
            normalize_timing_breakdown=base_deps.normalize_timing_breakdown,
            classify_media_query_with_llm=svc._classify_media_query_with_llm,
            rewrite_tool_queries_with_llm=svc._rewrite_tool_queries_with_llm,
            resolve_media_entity=svc._er_resolve_media_entity,
        )
        decision, _, _ = svc._build_router_decision(
            question=question,
            history=[] if previous_state is None else [
                {
                    "role": "assistant",
                    "content": "__trace__",
                    "trace": {"conversation_state_after": previous_state},
                }
            ],
            quota_state=_EMPTY_QUOTA,
            query_profile=_profile(question),
            deps=deps,
        )
    return decision


def _make_runtime(decision: svc.RouterDecision) -> svc.AgentRuntimeState:
    plan = svc.ExecutionPlan(decision=decision)
    resolution = svc.RouterContextResolution(resolved_question=decision.raw_question)
    return svc.AgentRuntimeState(decision=decision, execution_plan=plan, context_resolution=resolution)
