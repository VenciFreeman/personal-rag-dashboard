from __future__ import annotations

import sys

from .agent import agent_types as _agent_types
from .agent import session_state_runner as _session_state_runner
from .agent import support_answering as _support_answering
from .agent import support_common as _support_common
from .agent import support_observability as _support_observability
from .agent import support_retrieval as _support_retrieval
from .agent.domain import router_helpers as _router_helpers
from .agent.domain import doc_tools as _doc_tools
from .agent.domain import media_helpers as _media_helpers
from .agent.domain import router_core as _router_core
from .agent.domain import media_tools as _media_tools
from .agent.domain import router_service as _router
from .agent.infra import runtime_infra as _runtime
from .agent.runtime import tool_runtime as _tool_runtime_owner
from .agent.runtime import composition as _composition
from .agent.runtime import entry as _entry
from .agent.runtime import tool_runtime as _tool_runtime
from .agent.agent_boundaries import log_no_context_query as boundary_log_no_context_query
from .agent import agent_session_store as _session_store
from .query import query_understanding as _query_understanding
from .agent.agent_round_runner import prepare_round_answer
from .answer.answer_composer import compose_round_answer as _compose_round_answer
from .answer.answer_generation_runner import generate_sync_answer, start_streaming_answer
from .answer.final_answer_runner import finalize_round_answer
from .answer.streaming_renderer import render_streaming_answer
from .media import entity_resolver as _entity_resolver
from .agent.round_lifecycle_runner import run_round_stream, run_round_sync
from .agent.round_response_runner import persist_round_response
from .ontologies.ontology_loader import get_load_statuses as _get_ontology_load_statuses
from .planner.routing_policy import RoutingPolicy
from .tooling.tool_executor import build_confirmation_payload, execute_tool_phase, resolve_allowed_plan
from .tooling.tool_option_assembly import assemble_execution_tool_options as assemble_execution_tool_options_layer
from .tooling.tool_plan_runner import update_debug_trace_with_tool_plan
from .tooling.tool_planning_pipeline import run_tool_planning_pipeline


_get_query_type = _router_helpers._get_query_type


def _module_exports(module, names: set[str]) -> dict[str, tuple[object, str]]:
    return {name: (module, name) for name in names}


_EXPORT_SOURCES: dict[str, tuple[object, str]] = {"RoutingPolicy": (RoutingPolicy, "")}
_EXPORT_SOURCES.update(
    _module_exports(
        _router,
        {
            "_apply_router_semantic_repairs",
            "_classify_media_query_with_llm",
            "_classify_query_type",
            "_resolve_query_profile",
            "_rewrite_tool_queries_with_llm",
            "_router_decision_to_query_classification",
        },
    )
)
_EXPORT_SOURCES.update(
    _module_exports(
        _media_tools,
        {
            "MediaToolDeps",
            "_best_local_media_title_match",
            "build_default_media_tool_deps",
            "_canonicalize_media_entity",
            "_concept_cache_key",
            "_get_cached_mediawiki_concept",
            "_is_alias_expansion_result_relevant",
            "_match_local_terms",
            "_media_keyword_hit_fields",
            "_media_title_match_boost",
            "_media_title_match_boost_any",
            "_music_title_has_composer_anchor_conflict",
            "_normalize_media_entities_and_filters",
            "_normalize_media_title_for_match",
            "_resolve_creator_canonicals",
            "_set_cached_mediawiki_concept",
            "_strip_media_entity_boundary_terms",
            "_tmdb_request",
            "_tmdb_result_score",
            "resolve_media_tool_deps",
        },
    )
)
_EXPORT_SOURCES.update(
    _module_exports(
        _doc_tools,
        {
            "_rewrite_doc_queries",
            "_tool_query_document_rag",
        },
    )
)
_EXPORT_SOURCES.update(
    _module_exports(
        _runtime,
        {
            "_auto_queue_bug_tickets",
            "_clear_usage_trace_rows",
            "_current_month",
            "_get_llm_profile",
            "_http_json",
            "_increment_quota_state",
            "_list_usage_trace_rows",
            "_llm_chat",
            "_llm_chat_stream",
            "_load_quota_history",
            "_load_quota_state",
            "_load_usage_trace_rows",
            "_normalize_usage_provider",
            "_normalize_usage_text",
            "_now_iso",
            "_parse_usage_timestamp",
            "_record_quota_usage",
            "_record_usage_events",
            "_save_quota_history",
            "_save_quota_state",
            "_save_usage_trace_rows",
            "_set_monthly_quota_usage",
            "_trim_usage_trace_rows",
            "_usage_message_preview",
        },
    )
)
_EXPORT_SOURCES.update(
    _module_exports(
        _tool_runtime,
        {
            "_build_post_retrieval_repair_calls",
            "_execute_tool",
            "_plan_tool_calls",
            "_run_post_retrieval_repairs",
        },
    )
)
_EXPORT_SOURCES.update(
    _module_exports(
        _agent_types,
        {
            "AgentRuntimeState",
            "ExecutionPlan",
            "PlannedToolCall",
            "RouterContextResolution",
            "RouterDecision",
            "TOOL_EXPAND_MEDIAWIKI_CONCEPT",
            "TOOL_PARSE_MEDIAWIKI",
            "TOOL_QUERY_DOC_RAG",
            "TOOL_QUERY_MEDIA",
            "TOOL_SEARCH_BANGUMI",
            "TOOL_SEARCH_BY_CREATOR",
            "TOOL_SEARCH_TMDB",
            "TOOL_SEARCH_WEB",
            "ToolExecution",
        },
    )
)
_EXPORT_SOURCES.update(
    _module_exports(
        _support_common,
        {
            "COLLECTION_FILTER_TOP_K_MEDIA",
            "QUERY_TYPE_MEDIA",
            "SHORT_QUERY_MAX_TOKENS",
            "_CLASSIFICATION_ORACLE",
            "_build_music_signature_queries",
            "_deserialize_router_decision",
            "_schema_adapter",
        },
    )
)
_EXPORT_SOURCES.update(
    _module_exports(
        _support_answering,
        {
            "_build_followup_answer_note",
            "_build_guardrail_flags",
            "_build_media_row_citation_lookup",
            "_build_references_markdown",
            "_build_structured_media_answer",
            "_get_media_answer_render_deps",
            "_get_media_render_contract_builder_deps",
        },
    )
)
_EXPORT_SOURCES.update(
    _module_exports(
        _support_retrieval,
        {
            "_apply_reference_limits",
            "_get_media_result_rows",
            "_get_media_validation",
        },
    )
)
_EXPORT_SOURCES.update(
    _module_exports(
        _media_helpers,
        {
            "_extract_creator_from_collection_query",
            "_resolve_library_aliases",
        },
    )
)
_EXPORT_SOURCES.update(
    _module_exports(
        _router_helpers,
        {
            "_extract_music_work_hints",
            "_find_previous_trace_context",
            "_get_query_type",
            "_is_abstract_media_concept_query",
            "_resolve_previous_working_set_item_followup",
            "_serialize_router_decision",
        },
    )
)
_EXPORT_SOURCES.update(
    _module_exports(
        _router_core,
        {
            "_build_tool_grade_rewritten_queries",
        },
    )
)
_EXPORT_SOURCES.update(
    {
        "_er_resolve_creator_hit": (_entity_resolver.resolve_creator_hit, ""),
        "_er_resolve_media_entity": (_entity_resolver.resolve_media_entities, ""),
        "_er_resolve_title_hit": (_entity_resolver.resolve_title_hit, ""),
    }
)
_EXPORT_SOURCES.update(
    _module_exports(
        _router_helpers,
        {
            "_approx_tokens",
            "_is_collection_media_query",
            "_question_requests_media_details",
            "_question_requests_personal_evaluation",
        },
    )
)
_EXPORT_SOURCES.update(
    _module_exports(
        _tool_runtime,
        {"execute_tool", "execute_tool_plan", "plan_tool_calls", "ToolExecutionRuntimeDeps", "ToolPlanningRuntimeDeps"},
    )
)
_EXPORT_SOURCES.update(
    {
        "prepare_round_answer": (prepare_round_answer, ""),
        "generate_sync_answer": (generate_sync_answer, ""),
        "start_streaming_answer": (start_streaming_answer, ""),
        "finalize_round_answer": (finalize_round_answer, ""),
        "persist_round_response": (persist_round_response, ""),
        "run_round_sync": (run_round_sync, ""),
        "run_round_stream": (run_round_stream, ""),
    }
)

__all__ = sorted(
    set(_EXPORT_SOURCES)
    | {
        "_build_router_decision",
        "_build_guardrail_answer_mode",
        "_build_media_tool_options_from_decision",
        "_execute_tool_plan",
        "_summarize_answer",
        "_tool_expand_mediawiki_concept",
        "_tool_parse_mediawiki_page",
        "_tool_query_media_record",
        "_tool_search_bangumi",
        "_tool_search_by_creator",
        "_tool_search_mediawiki_action",
        "_tool_search_tmdb_media",
        "run_agent_round",
        "run_agent_round_stream",
    }
)


def _compat_value(name: str):
    if name in globals():
        return globals()[name]
    source = _EXPORT_SOURCES.get(name)
    if source is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    provider, attr_name = source
    if isinstance(provider, type):
        return provider
    if attr_name:
        return getattr(provider, attr_name)
    return provider


def __getattr__(name: str):
    return _compat_value(name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))


def _facade_value(name: str):
    return getattr(sys.modules[__name__], name)


def _build_media_tool_deps() -> _media_tools.MediaToolDeps:
    return _media_tools.MediaToolDeps(
        http_json=_facade_value("_http_json"),
        resolve_library_aliases=_facade_value("_resolve_library_aliases"),
        resolve_title_hit=_facade_value("_er_resolve_title_hit"),
        resolve_creator_hit=_facade_value("_er_resolve_creator_hit"),
    )


def _build_router_deps() -> _router.RouterDeps:
    return _router.RouterDeps(
        llm_chat=_facade_value("_llm_chat"),
        find_previous_trace_context=_facade_value("_find_previous_trace_context"),
        apply_router_semantic_repairs=_facade_value("_apply_router_semantic_repairs"),
        classify_media_query_with_llm=_facade_value("_classify_media_query_with_llm"),
        rewrite_tool_queries_with_llm=_facade_value("_rewrite_tool_queries_with_llm"),
        resolve_media_entity=_entity_resolver.resolve_media_entities,
        resolve_library_aliases=_media_helpers._resolve_library_aliases,
        planner_router_semantic_deps=_router_helpers._PLANNER_ROUTER_SEMANTIC_DEPS,
        perf_counter=_support_common._time.perf_counter,
        normalize_timing_breakdown=_router_helpers._normalize_timing_breakdown,
    )


def _summarize_answer(*args, **kwargs):
    return _support_answering._summarize_answer(*args, **kwargs)


def _build_router_decision(*args, **kwargs):
    kwargs.setdefault("deps", _build_router_deps())
    return _router._build_router_decision(*args, **kwargs)


def _tool_query_media_record(*args, **kwargs):
    kwargs.setdefault("deps", _build_media_tool_deps())
    return _media_tools._tool_query_media_record(*args, **kwargs)


def _build_media_tool_options_from_decision(*args, **kwargs):
    return _support_common._build_media_tool_options_from_decision(*args, **kwargs)


def _build_guardrail_answer_mode(*args, **kwargs):
    kwargs.setdefault("get_media_result_rows", _support_retrieval._get_media_result_rows)
    kwargs.setdefault("get_media_validation", _support_retrieval._get_media_validation)
    return _support_answering._build_guardrail_answer_mode(*args, **kwargs)


def _tool_search_tmdb_media(*args, **kwargs):
    kwargs.setdefault("deps", _build_media_tool_deps())
    return _media_tools._tool_search_tmdb_media(*args, **kwargs)


def _tool_search_by_creator(*args, **kwargs):
    kwargs.setdefault("deps", _build_media_tool_deps())
    return _media_tools._tool_search_by_creator(*args, **kwargs)


def _tool_search_mediawiki_action(*args, **kwargs):
    kwargs.setdefault("deps", _build_media_tool_deps())
    return _media_tools._tool_search_mediawiki_action(*args, **kwargs)


def _tool_parse_mediawiki_page(*args, **kwargs):
    kwargs.setdefault("deps", _build_media_tool_deps())
    return _media_tools._tool_parse_mediawiki_page(*args, **kwargs)


def _tool_expand_mediawiki_concept(*args, **kwargs):
    kwargs.setdefault("deps", _build_media_tool_deps())
    return _media_tools._tool_expand_mediawiki_concept(*args, **kwargs)


def _tool_search_bangumi(*args, **kwargs):
    kwargs.setdefault("deps", _build_media_tool_deps())
    return _media_tools._tool_search_bangumi(*args, **kwargs)


def _build_tool_execution_runtime_deps() -> _tool_runtime.ToolExecutionRuntimeDeps:
    return _tool_runtime.ToolExecutionRuntimeDeps(
        perf_counter=_support_common._time.perf_counter,
        query_document_rag=_doc_tools._tool_query_document_rag,
        query_media_record=_tool_query_media_record,
        search_web=_support_common._tool_search_web,
        expand_document_query=_support_common._tool_expand_document_query,
        expand_media_query=_support_common._tool_expand_media_query,
        search_mediawiki_action=_tool_search_mediawiki_action,
        parse_mediawiki_page=_tool_parse_mediawiki_page,
        expand_mediawiki_concept=_tool_expand_mediawiki_concept,
        search_tmdb_media=_tool_search_tmdb_media,
        search_by_creator=_tool_search_by_creator,
        sanitize_media_filters=_support_common._sanitize_media_filters,
        tool_execution_factory=_agent_types.ToolExecution,
        tool_query_doc_rag=_agent_types.TOOL_QUERY_DOC_RAG,
        tool_query_media=_agent_types.TOOL_QUERY_MEDIA,
        tool_search_web=_agent_types.TOOL_SEARCH_WEB,
        tool_expand_doc_query=_agent_types.TOOL_EXPAND_DOC_QUERY,
        tool_expand_media_query=_agent_types.TOOL_EXPAND_MEDIA_QUERY,
        tool_search_mediawiki=_agent_types.TOOL_SEARCH_MEDIAWIKI,
        tool_parse_mediawiki=_agent_types.TOOL_PARSE_MEDIAWIKI,
        tool_expand_mediawiki_concept=_agent_types.TOOL_EXPAND_MEDIAWIKI_CONCEPT,
        tool_search_tmdb=_agent_types.TOOL_SEARCH_TMDB,
        tool_search_by_creator=_agent_types.TOOL_SEARCH_BY_CREATOR,
    )


def _execute_tool(call: _agent_types.PlannedToolCall, query_profile: dict[str, object], trace_id: str) -> _agent_types.ToolExecution:
    return _tool_runtime.execute_tool(call, query_profile, trace_id, deps=_build_tool_execution_runtime_deps())


def _execute_tool_plan(*args, **kwargs):
    return _tool_runtime_owner.execute_tool_plan(
        *args,
        execute_tool=_execute_tool,
        prefetched_tool_names={_agent_types.TOOL_EXPAND_MEDIAWIKI_CONCEPT},
        planned_call_factory=_agent_types.PlannedToolCall,
        **kwargs,
    )


def _build_tool_planning_runtime_deps():
    return _tool_runtime_owner.ToolPlanningRuntimeDeps(
        run_tool_planning_pipeline=run_tool_planning_pipeline,
        build_router_decision=_build_router_decision,
        router_decision_to_query_classification=_router._router_decision_to_query_classification,
        build_plan=RoutingPolicy().build_plan,
        resolve_router_context=_support_common._resolve_router_context,
        assemble_tool_options=lambda planned_tools, router_decision: assemble_execution_tool_options_layer(
            planned_tools,
            decision=router_decision,
        ),
        serialize_router_context_resolution=_support_common._serialize_router_context_resolution,
        serialize_execution_plan=_support_common._serialize_execution_plan,
        serialize_post_retrieval_assessment=_support_common._serialize_post_retrieval_assessment,
        get_ontology_load_statuses=_get_ontology_load_statuses,
        perf_counter=_support_common._time.perf_counter,
        build_router_decision_path=_router._build_router_decision_path,
        normalize_timing_breakdown=_router_helpers._normalize_timing_breakdown,
        planning_artifact_factory=_agent_types.PlanningArtifact,
    )


def _plan_tool_calls(*args, **kwargs):
    return _tool_runtime_owner.plan_tool_calls(*args, deps=_build_tool_planning_runtime_deps(), **kwargs)


def _build_guardrail_flag_deps():
    return _composition.build_guardrail_flag_deps(
        get_resolved_query_state_from_runtime=_support_common._get_resolved_query_state_from_runtime,
        get_lookup_mode_from_state=_support_common._get_lookup_mode_from_state,
        get_planner_snapshot_from_runtime=_support_common._get_planner_snapshot_from_runtime,
        question_requests_media_details=_router_helpers._question_requests_media_details,
    )


def _build_post_retrieval_repair_owner_deps():
    return _composition.build_post_retrieval_repair_deps(
        normalize_search_mode=_router_helpers._normalize_search_mode,
        execute_tool_plan=_execute_tool_plan,
        get_media_validation=_support_retrieval._get_media_validation,
        guardrail_flag_deps=_build_guardrail_flag_deps(),
    )


def _build_round_routing_deps():
    return _composition.build_round_routing_deps(
        prepare_session_context=_query_understanding.prepare_session_context,
        plan_query_execution=_query_understanding.plan_query_execution,
        quota_exceeded=_support_common._quota_exceeded,
        create_session=_session_store.create_session,
        get_session=_session_store.get_session,
        save_session=_session_store.save_session,
        derive_session_title=_session_store.derive_session_title,
        now_iso=_runtime._now_iso,
        new_ephemeral_session_id=_support_common._new_ephemeral_session_id,
        normalize_search_mode=_router_helpers._normalize_search_mode,
        resolve_query_profile=_router._resolve_query_profile,
        load_quota_state=_runtime._load_quota_state,
        plan_tool_calls=_plan_tool_calls,
        serialize_planned_tools=_support_common._serialize_planned_tools,
    )


def _build_tool_plan_execution_deps():
    return _composition.build_tool_plan_execution_deps(
        apply_reference_limits=_support_retrieval._apply_reference_limits,
        execute_per_item_expansion=_support_retrieval._execute_per_item_expansion,
        update_post_retrieval_fallback_evidence=_support_retrieval._update_post_retrieval_fallback_evidence,
        log_no_context_query=boundary_log_no_context_query,
        post_retrieval_repair_owner_deps=_build_post_retrieval_repair_owner_deps(),
        guardrail_flag_owner_deps=_build_guardrail_flag_deps(),
        build_guardrail_answer_mode=_build_guardrail_answer_mode,
        log_agent_media_miss=_support_retrieval._log_agent_media_miss,
        increment_quota_state=_runtime._increment_quota_state,
        resolve_agent_no_context=_support_common._resolve_agent_no_context,
        get_query_type=_get_query_type,
        get_planner_snapshot_from_runtime=_support_common._get_planner_snapshot_from_runtime,
        get_resolved_query_state_from_runtime=_support_common._get_resolved_query_state_from_runtime,
    )


def _build_round_lifecycle_deps(*, request_base_url: str):
    return _entry.build_round_lifecycle_deps(
        round_lifecycle_builder=_composition.build_round_lifecycle_deps,
        request_base_url=request_base_url,
        planner_deps=_agent_types.PlannerDeps(
            prepare_round_answer_fn=prepare_round_answer,
            normalize_trace_id=_support_common._normalize_trace_id,
            routing_deps=_build_round_routing_deps(),
            build_confirmation_payload=build_confirmation_payload,
            resolve_allowed_plan=resolve_allowed_plan,
            execute_tool_phase=execute_tool_phase,
            execute_tool_plan_boundary=_execute_tool_plan,
            tool_plan_deps=_build_tool_plan_execution_deps(),
        ),
        execution_deps=_agent_types.ExecutionDeps(
            append_message=_session_store.append_message,
            build_memory_context=_session_store.build_memory_context,
            approx_tokens=_router_helpers._approx_tokens,
            execute_per_item_expansion=_support_retrieval._execute_per_item_expansion,
        ),
        answer_deps=_agent_types.AnswerDeps(
            generate_sync_answer_fn=generate_sync_answer,
            start_streaming_answer_fn=start_streaming_answer,
            finalize_round_answer_fn=lambda **kwargs: finalize_round_answer(
                answer_has_inline_reference_markers=_support_answering._answer_has_inline_reference_markers,
                build_references_markdown=_support_answering._build_references_markdown,
                **kwargs,
            ),
            build_structured_media_answer=_support_answering._build_structured_media_answer,
            summarize_answer=_summarize_answer,
            fallback_retrieval_answer=_support_answering._fallback_retrieval_answer,
            apply_guardrail_answer_mode=_support_answering._apply_guardrail_answer_mode,
            append_media_mentions_to_answer=_support_answering._append_media_mentions_to_answer,
            compose_round_answer=_compose_round_answer,
            render_streaming_answer=render_streaming_answer,
            build_structured_media_answer_chunks=_support_answering._build_structured_media_answer_chunks,
            build_structured_media_external_item_block=_support_answering._build_structured_media_external_item_block,
        ),
        observability_deps=_agent_types.ObservabilityDeps(
            persist_round_response_fn=persist_round_response,
            persist_session_artifacts=_session_state_runner.persist_session_artifacts,
            update_memory_for_session=_session_store.update_memory_for_session,
            schedule_generated_session_title=_session_store.schedule_generated_session_title,
            auto_queue_bug_tickets=_runtime._auto_queue_bug_tickets,
            write_debug_record=_support_common._write_debug_record,
            record_metrics_safe=_session_state_runner.record_metrics_safe,
            record_agent_metrics=_support_observability.record_agent_metrics,
            build_agent_trace_record=_support_observability._build_agent_trace_record,
            write_agent_trace_record=_support_observability._write_agent_trace_record,
            get_query_type=_get_query_type,
            web_search_daily_limit=_support_common.WEB_SEARCH_DAILY_LIMIT,
            deepseek_daily_limit=_support_common.DEEPSEEK_DAILY_LIMIT,
            serialize_planned_tools=_support_common._serialize_planned_tools,
            update_debug_trace_with_tool_plan=update_debug_trace_with_tool_plan,
        ),
    )


def run_agent_round(
    *,
    question: str,
    session_id: str = "",
    history: list[dict[str, str]] | None = None,
    backend: str = "local",
    search_mode: str = "local_only",
    confirm_over_quota: bool = False,
    deny_over_quota: bool = False,
    debug: bool = False,
    request_base_url: str = "",
    benchmark_mode: bool = False,
    trace_id: str = "",
):
    return _entry.run_agent_round(
        build_round_lifecycle_deps=_build_round_lifecycle_deps,
        run_round_sync=run_round_sync,
        question=question,
        session_id=session_id,
        history=history,
        backend=backend,
        search_mode=search_mode,
        confirm_over_quota=confirm_over_quota,
        deny_over_quota=deny_over_quota,
        debug=debug,
        request_base_url=request_base_url,
        benchmark_mode=benchmark_mode,
        trace_id=trace_id,
    )


def run_agent_round_stream(
    *,
    question: str,
    session_id: str = "",
    history: list[dict[str, str]] | None = None,
    backend: str = "local",
    search_mode: str = "local_only",
    confirm_over_quota: bool = False,
    deny_over_quota: bool = False,
    debug: bool = False,
    request_base_url: str = "",
    benchmark_mode: bool = False,
    trace_id: str = "",
):
    yield from _entry.run_agent_round_stream(
        build_round_lifecycle_deps=_build_round_lifecycle_deps,
        run_round_stream=run_round_stream,
        question=question,
        session_id=session_id,
        history=history,
        backend=backend,
        search_mode=search_mode,
        confirm_over_quota=confirm_over_quota,
        deny_over_quota=deny_over_quota,
        debug=debug,
        request_base_url=request_base_url,
        benchmark_mode=benchmark_mode,
        trace_id=trace_id,
    )
