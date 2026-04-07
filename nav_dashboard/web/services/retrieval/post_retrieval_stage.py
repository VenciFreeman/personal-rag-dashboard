from __future__ import annotations

import time as _time
from dataclasses import dataclass, field
from typing import Any

from ..answer.answer_policy import AnswerPolicy
from .post_retrieval_repairs_owner import PostRetrievalRepairDeps, plan_post_retrieval_repairs, reevaluate_post_retrieval
from .result_layering_contract import PostRetrievalFacts


@dataclass(frozen=True)
class PostRetrievalStageResult:
    post_retrieval_facts: PostRetrievalFacts
    post_retrieval_outcome: Any
    proposed_repair_calls: list[Any] = field(default_factory=list)
    proposed_answer_strategy: Any = None
    guardrail_flags: dict[str, Any] = field(default_factory=dict)
    timings: dict[str, float] = field(default_factory=dict)


def collect_doc_metrics(doc_data: dict[str, Any], query_profile: dict[str, Any]) -> dict[str, Any]:
    return {
        "vector_recall_seconds": float(doc_data.get("vector_recall_seconds", 0) or 0),
        "rerank_seconds": float(doc_data.get("rerank_seconds", 0) or 0),
        "top1_score": doc_data.get("doc_top1_score"),
        "top1_score_before_rerank": doc_data.get("doc_top1_score_before_rerank"),
        "top1_identity_changed": doc_data.get("doc_top1_identity_changed"),
        "top1_rank_shift": doc_data.get("doc_top1_rank_shift"),
        "no_context": int(doc_data.get("no_context", 0) or 0),
        "embed_cache_hit": int(doc_data.get("embed_cache_hit", 0) or 0),
        "query_rewrite_hit": int(doc_data.get("query_rewrite_hit", 0) or 0),
        "threshold": float(query_profile.get("doc_score_threshold", 0) or 0),
    }


def evaluate_post_retrieval_stage(
    *,
    tool_results: list[Any],
    query_profile: dict[str, Any],
    runtime_state: Any,
    doc_tool_name: str,
    normalized_search_mode: str,
    repair_owner_deps: PostRetrievalRepairDeps,
) -> PostRetrievalStageResult:
    doc_tool_result = next((item for item in tool_results if getattr(item, "tool", "") == doc_tool_name), None)
    doc_data = doc_tool_result.data if (doc_tool_result and isinstance(doc_tool_result.data, dict)) else {}
    doc_metrics = collect_doc_metrics(doc_data, query_profile)

    evaluate_t0 = _time.perf_counter()
    post_retrieval_outcome, guardrail_flags = reevaluate_post_retrieval(
        runtime_state,
        tool_results,
        deps=repair_owner_deps,
    )
    proposed_repair_calls = plan_post_retrieval_repairs(
        runtime_state,
        post_retrieval_outcome,
        tool_results,
        search_mode=normalized_search_mode,
        deps=repair_owner_deps,
    )
    post_retrieval_evaluate_seconds = _time.perf_counter() - evaluate_t0

    answer_strategy_t0 = _time.perf_counter()
    proposed_answer_strategy = AnswerPolicy().determine(runtime_state.decision, post_retrieval_outcome)
    answer_strategy_seconds = _time.perf_counter() - answer_strategy_t0

    return PostRetrievalStageResult(
        post_retrieval_facts=PostRetrievalFacts(
            doc_data=dict(doc_data),
            doc_metrics=dict(doc_metrics),
            no_context_count=int(doc_metrics.get("no_context", 0) or 0),
        ),
        post_retrieval_outcome=post_retrieval_outcome,
        proposed_repair_calls=proposed_repair_calls,
        proposed_answer_strategy=proposed_answer_strategy,
        guardrail_flags=guardrail_flags,
        timings={
            "post_retrieval_evaluate_seconds": post_retrieval_evaluate_seconds,
            "answer_strategy_seconds": answer_strategy_seconds,
        },
    )