from __future__ import annotations

import time as _time
from dataclasses import asdict, dataclass, replace
from typing import Any, Callable

from ..planner import planner_contracts
from ..agent.agent_types import (
    TOOL_EXPAND_MEDIAWIKI_CONCEPT,
    TOOL_PARSE_MEDIAWIKI,
    TOOL_QUERY_DOC_RAG,
    TOOL_SEARCH_TMDB,
    TOOL_SEARCH_WEB,
    AgentRuntimeState,
    PlannedToolCall,
    ToolExecution,
)
from ..answer.answer_policy import AnswerPolicy, AnswerStrategy
from ..agent.guardrail_flags_owner import GuardrailFlagDeps, build_guardrail_flags
from .post_retrieval_policy import PostRetrievalOutcome, PostRetrievalPolicy


@dataclass(frozen=True)
class PostRetrievalRepairDeps:
    normalize_search_mode: Callable[[str], str]
    execute_tool_plan: Callable[[list[PlannedToolCall], dict[str, Any], str], list[ToolExecution]]
    get_media_validation: Callable[[list[ToolExecution]], dict[str, Any]]
    guardrail_flag_deps: GuardrailFlagDeps


def build_post_retrieval_repair_calls(
    runtime_state: AgentRuntimeState,
    outcome: PostRetrievalOutcome,
    tool_results: list[ToolExecution],
    *,
    search_mode: str,
    deps: PostRetrievalRepairDeps,
) -> list[PlannedToolCall]:
    if str(outcome.action or "") != "enrich":
        return []

    existing_tools = {
        str(item.tool or "").strip()
        for item in tool_results
        if str(item.tool or "").strip()
    }
    decision = runtime_state.decision
    rewritten_queries = {
        str(key): str(value)
        for key, value in (decision.rewritten_queries or {}).items()
        if str(key).strip() and str(value).strip()
    }
    resolved_question = str(runtime_state.context_resolution.resolved_question or decision.resolved_question or decision.raw_question or "").strip()
    raw_question = str(decision.raw_question or resolved_question).strip()
    normalized_mode = deps.normalize_search_mode(search_mode)
    suppress_bookish_list_wiki = (
        str(decision.media_family or "") == planner_contracts.ROUTER_MEDIA_FAMILY_BOOKISH
        and str(decision.answer_shape or "") == planner_contracts.ROUTER_ANSWER_SHAPE_LIST_PLUS_EXPAND
    )

    calls: list[PlannedToolCall] = []

    def _append_unique(call: PlannedToolCall) -> None:
        if str(call.name or "").strip() in existing_tools:
            return
        if any(
            str(existing.name or "").strip() == str(call.name or "").strip()
            and str(existing.query or "").strip() == str(call.query or "").strip()
            for existing in calls
        ):
            return
        calls.append(call)

    for tool_name in list(outcome.repair_tools or []):
        name = str(tool_name or "").strip()
        if not name or name in existing_tools:
            continue
        if name == TOOL_EXPAND_MEDIAWIKI_CONCEPT and suppress_bookish_list_wiki:
            continue
        if name == TOOL_SEARCH_WEB:
            if normalized_mode == "local_only":
                if TOOL_QUERY_DOC_RAG in existing_tools or any(call.name == TOOL_QUERY_DOC_RAG for call in calls):
                    continue
                calls.append(
                    PlannedToolCall(
                        name=TOOL_QUERY_DOC_RAG,
                        query=rewritten_queries.get("doc_query") or resolved_question or raw_question,
                    )
                )
                continue
            _append_unique(
                PlannedToolCall(
                    name=TOOL_SEARCH_WEB,
                    query=rewritten_queries.get("web_query") or resolved_question or raw_question,
                )
            )
            continue
        if name == TOOL_QUERY_DOC_RAG:
            _append_unique(
                PlannedToolCall(
                    name=TOOL_QUERY_DOC_RAG,
                    query=rewritten_queries.get("doc_query") or resolved_question or raw_question,
                )
            )
            continue
        if name == TOOL_SEARCH_TMDB:
            tmdb_query = str(decision.entities[0] or "").strip() if len(decision.entities) == 1 else ""
            _append_unique(
                PlannedToolCall(
                    name=TOOL_SEARCH_TMDB,
                    query=rewritten_queries.get("tmdb_query") or tmdb_query or resolved_question or raw_question,
                )
            )
            continue
        if name == TOOL_EXPAND_MEDIAWIKI_CONCEPT:
            wiki_query = str(decision.entities[0] or "").strip() if len(decision.entities) == 1 else ""
            _append_unique(
                PlannedToolCall(
                    name=TOOL_EXPAND_MEDIAWIKI_CONCEPT,
                    query=rewritten_queries.get("media_query") or wiki_query or resolved_question or raw_question,
                )
            )
            continue
        if name == TOOL_PARSE_MEDIAWIKI:
            wiki_query = str(decision.entities[0] or "").strip() if len(decision.entities) == 1 else ""
            _append_unique(
                PlannedToolCall(
                    name=TOOL_PARSE_MEDIAWIKI,
                    query=rewritten_queries.get("media_query") or wiki_query or resolved_question or raw_question,
                )
            )
            continue
    return calls


def plan_post_retrieval_repairs(
    runtime_state: AgentRuntimeState,
    outcome: PostRetrievalOutcome,
    tool_results: list[ToolExecution],
    *,
    search_mode: str,
    deps: PostRetrievalRepairDeps,
) -> list[PlannedToolCall]:
    return build_post_retrieval_repair_calls(
        runtime_state,
        outcome,
        tool_results,
        search_mode=search_mode,
        deps=deps,
    )


def apply_post_retrieval_repairs(
    repair_calls: list[PlannedToolCall],
    query_profile: dict[str, Any],
    trace_id: str,
    *,
    deps: PostRetrievalRepairDeps,
) -> tuple[list[ToolExecution], float]:
    if not repair_calls:
        return [], 0.0
    repair_t0 = _time.perf_counter()
    repair_results = deps.execute_tool_plan(repair_calls, query_profile, trace_id)
    return repair_results, (_time.perf_counter() - repair_t0)


def reevaluate_post_retrieval(
    runtime_state: AgentRuntimeState,
    tool_results: list[ToolExecution],
    *,
    deps: PostRetrievalRepairDeps,
) -> tuple[PostRetrievalOutcome, dict[str, bool]]:
    media_validation = deps.get_media_validation(tool_results)
    guardrail_flags = build_guardrail_flags(
        runtime_state=runtime_state,
        media_validation=media_validation,
        deps=deps.guardrail_flag_deps,
    )
    outcome = PostRetrievalPolicy().evaluate(runtime_state.decision, tool_results, guardrail_flags)
    return outcome, guardrail_flags


def run_post_retrieval_repairs(
    runtime_state: AgentRuntimeState,
    query_classification: dict[str, Any],
    tool_results: list[ToolExecution],
    query_profile: dict[str, Any],
    *,
    trace_id: str,
    search_mode: str,
    deps: PostRetrievalRepairDeps,
) -> tuple[list[ToolExecution], PostRetrievalOutcome, AnswerStrategy, dict[str, float]]:
    post_retrieval_evaluate_seconds = 0.0
    post_retrieval_repairs_seconds = 0.0
    evaluate_t0 = _time.perf_counter()
    outcome, guardrail_flags = reevaluate_post_retrieval(
        runtime_state,
        tool_results,
        deps=deps,
    )
    repair_calls = plan_post_retrieval_repairs(
        runtime_state,
        outcome,
        tool_results,
        search_mode=search_mode,
        deps=deps,
    )
    post_retrieval_evaluate_seconds += _time.perf_counter() - evaluate_t0
    if repair_calls:
        repair_results, repair_seconds = apply_post_retrieval_repairs(
            repair_calls,
            query_profile,
            trace_id,
            deps=deps,
        )
        post_retrieval_repairs_seconds += repair_seconds
        if repair_results:
            tool_results = list(tool_results) + repair_results
            reevaluate_t0 = _time.perf_counter()
            outcome, guardrail_flags = reevaluate_post_retrieval(
                runtime_state,
                tool_results,
                deps=deps,
            )
            post_retrieval_evaluate_seconds += _time.perf_counter() - reevaluate_t0

    post_retrieval_payload = asdict(outcome)
    answer_strategy_t0 = _time.perf_counter()
    answer_strategy = AnswerPolicy().determine(runtime_state.decision, outcome)
    answer_strategy_seconds = _time.perf_counter() - answer_strategy_t0
    answer_strategy_payload = asdict(answer_strategy)
    runtime_state.execution_artifact = replace(
        runtime_state.execution_artifact,
        guardrail_flags=dict(guardrail_flags or {}),
        post_retrieval_outcome=post_retrieval_payload,
        answer_strategy=answer_strategy_payload,
    )
    query_classification["guardrail_flags"] = guardrail_flags
    query_classification["post_retrieval_outcome"] = post_retrieval_payload
    query_classification["answer_strategy"] = answer_strategy_payload
    return tool_results, outcome, answer_strategy, {
        "post_retrieval_evaluate_seconds": post_retrieval_evaluate_seconds,
        "post_retrieval_repairs_seconds": post_retrieval_repairs_seconds,
        "answer_strategy_seconds": answer_strategy_seconds,
    }