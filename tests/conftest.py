"""Shared fixtures for nav_dashboard regression tests.

Importing `svc` here so each test module can do::

    from tests.conftest import svc, _decide, _llm_stub, _profile, _make_runtime

instead of duplicating bootstrap code.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import patch

# ── repo root on sys.path ─────────────────────────────────────────────────────
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# Minimal env stubs so imports that reach for LLM clients don't crash.
os.environ.setdefault("DEEPSEEK_API_KEY", "stub")
os.environ.setdefault("LOCAL_LLM_URL", "http://127.0.0.1:11434")

import nav_dashboard.web.services.agent_service as svc  # noqa: E402

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
        )
    return decision


def _make_runtime(decision: svc.RouterDecision) -> svc.AgentRuntimeState:
    plan = svc.ExecutionPlan(decision=decision)
    resolution = svc.RouterContextResolution(resolved_question=decision.raw_question)
    return svc.AgentRuntimeState(decision=decision, execution_plan=plan, context_resolution=resolution)
