from __future__ import annotations

from typing import Any, Mapping

from pydantic import BaseModel, ConfigDict, Field

API_SCHEMA_VERSION = 1


class ContractModel(BaseModel):
    extensions: dict[str, Any] = Field(default_factory=dict)
    model_config = ConfigDict(extra="forbid")


class VersionedAPIModel(ContractModel):
    api_schema_version: int = API_SCHEMA_VERSION
    model_config = ConfigDict(extra="forbid")


class PlannedToolResponse(ContractModel):
    name: str = ""
    query: str = ""


class ToolResultResponse(ContractModel):
    tool: str = ""
    status: str = ""
    summary: str = ""
    data: dict[str, Any] = Field(default_factory=dict)


class QueryUnderstandingResponse(ContractModel):
    resolved_question: str = ""
    query_class: str = ""
    subject_scope: str = ""
    time_scope_type: str = ""
    answer_shape: str = ""
    media_family: str = ""
    followup_mode: str = ""
    planner_snapshot: dict[str, Any] = Field(default_factory=dict)
    router_decision: dict[str, Any] = Field(default_factory=dict)
    retrieval_plan: list[str] = Field(default_factory=list)
    metadata_anchors: list[dict[str, Any]] = Field(default_factory=list)
    scope_anchors: list[dict[str, Any]] = Field(default_factory=list)


class AgentChatResponse(VersionedAPIModel):
    requires_confirmation: bool = False
    trace_id: str = ""
    session_id: str = ""
    answer: str = ""
    backend: str = ""
    search_mode: str = ""
    query_profile: dict[str, Any] = Field(default_factory=dict)
    query_classification: dict[str, Any] = Field(default_factory=dict)
    query_type: str = ""
    query_understanding: QueryUnderstandingResponse = Field(default_factory=QueryUnderstandingResponse)
    planned_tools: list[PlannedToolResponse] = Field(default_factory=list)
    tool_results: list[ToolResultResponse] = Field(default_factory=list)


class AgentStreamEvent(VersionedAPIModel):
    type: str
    trace_id: str = ""
    session_id: str = ""
    message: str = ""
    tool: str = ""
    status: str = ""
    summary: str = ""
    delta: str = ""
    payload: dict[str, Any] | None = None


class BenchmarkHistoryResponse(VersionedAPIModel):
    results: list["BenchmarkHistoryItemResponse"] = Field(default_factory=list)


class BenchmarkHistoryItemResponse(ContractModel):
    timestamp: str = ""
    total: int = 0
    passed: int = 0
    failed: int = 0
    pass_rate: float = 0.0
    assertion_summary: dict[str, Any] | None = None


class BenchmarkCaseSetResponse(ContractModel):
    id: str = ""
    label: str = ""
    lengths: dict[str, int] = Field(default_factory=dict)
    max_query_count_per_type: int = 0
    taxonomy_counts: dict[str, int] = Field(default_factory=dict)
    source_counts: dict[str, int] = Field(default_factory=dict)
    supported_modules: list[str] = Field(default_factory=list)
    module_case_counts: dict[str, int] = Field(default_factory=dict)
    module_length_counts: dict[str, dict[str, int]] = Field(default_factory=dict)
    module_max_query_count_per_type: dict[str, int] = Field(default_factory=dict)


class BenchmarkChainResponse(ContractModel):
    id: str = ""
    label: str = ""
    family: str = ""
    search_mode: str = ""


class BenchmarkCaseSetsResponse(VersionedAPIModel):
    case_sets: list[BenchmarkCaseSetResponse] = Field(default_factory=list)
    chains: list[BenchmarkChainResponse] = Field(default_factory=list)


class BenchmarkRunResultResponse(ContractModel):
    timestamp: str = ""
    total: int = 0
    passed: int = 0
    failed: int = 0
    pass_rate: float = 0.0
    cases: dict[str, Any] = Field(default_factory=dict)
    case_details: dict[str, Any] = Field(default_factory=dict)
    assertions: dict[str, Any] = Field(default_factory=dict)
    assertion_summary: dict[str, Any] | None = None


class BenchmarkJobPayloadResponse(ContractModel):
    id: str = ""
    status: str = ""
    progress: float = 0.0
    current: int = 0
    total: int = 0
    message: str = ""
    error: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)
    logs: list[Any] = Field(default_factory=list)
    result: BenchmarkRunResultResponse | dict[str, Any] | None = None


class BenchmarkJobResponse(VersionedAPIModel):
    ok: bool = True
    job: BenchmarkJobPayloadResponse = Field(default_factory=BenchmarkJobPayloadResponse)


class BenchmarkRouterClassificationResponse(VersionedAPIModel):
    timestamp: str = ""
    total: int = 0
    passed: int = 0
    failed: int = 0
    pass_rate: float = 0.0
    cases: list[dict[str, Any]] = Field(default_factory=list)
    violations: list[dict[str, Any]] = Field(default_factory=list)


class BenchmarkRouterClassificationCasesResponse(VersionedAPIModel):
    cases: list[dict[str, Any]] = Field(default_factory=list)


class BenchmarkRouterClassificationCaseMutationResponse(VersionedAPIModel):
    ok: bool = True
    case: dict[str, Any] = Field(default_factory=dict)
    cases: list[dict[str, Any]] = Field(default_factory=list)


class BenchmarkRouterClassificationHistoryResponse(VersionedAPIModel):
    results: list[dict[str, Any]] = Field(default_factory=list)


class BenchmarkStreamEvent(VersionedAPIModel):
    type: str
    message: str = ""


class DashboardTraceResponse(VersionedAPIModel):
    ok: bool = True
    trace: dict[str, Any] = Field(default_factory=dict)
    export_text: str = ""


BenchmarkHistoryResponse.model_rebuild()


def _prepare_contract_payload(model_cls: type[BaseModel], payload: Mapping[str, Any] | None) -> dict[str, Any]:
    data = dict(payload or {})
    field_names = set(model_cls.model_fields)
    known: dict[str, Any] = {key: value for key, value in data.items() if key in field_names}
    extras = {key: value for key, value in data.items() if key not in field_names}
    extensions = known.get("extensions") if isinstance(known.get("extensions"), dict) else {}
    if extensions or extras:
        known["extensions"] = {**extensions, **extras}
    return known


def _dump_contract_model(model: BaseModel) -> dict[str, Any]:
    return _prune_empty_extensions(model.model_dump(mode="json"))


def _dump_sparse_contract_model(model: BaseModel) -> dict[str, Any]:
    return _prune_empty_extensions(model.model_dump(mode="json", exclude_defaults=True))


def _prune_empty_extensions(value: Any) -> Any:
    if isinstance(value, dict):
        pruned: dict[str, Any] = {}
        for key, nested in value.items():
            cleaned = _prune_empty_extensions(nested)
            if key == "extensions" and cleaned == {}:
                continue
            pruned[key] = cleaned
        return pruned
    if isinstance(value, list):
        return [_prune_empty_extensions(item) for item in value]
    return value


def _build_query_understanding(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    return _dump_sparse_contract_model(QueryUnderstandingResponse.model_validate(_prepare_contract_payload(QueryUnderstandingResponse, payload)))


def _build_planned_tools(items: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    return [
        _dump_contract_model(PlannedToolResponse.model_validate(_prepare_contract_payload(PlannedToolResponse, item)))
        for item in list(items or [])
    ]


def _build_tool_results(items: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    return [
        _dump_contract_model(ToolResultResponse.model_validate(_prepare_contract_payload(ToolResultResponse, item)))
        for item in list(items or [])
    ]


def _build_benchmark_history_items(results: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    return [
        _dump_sparse_contract_model(BenchmarkHistoryItemResponse.model_validate(_prepare_contract_payload(BenchmarkHistoryItemResponse, item)))
        for item in list(results or [])
    ]


def _build_benchmark_case_sets(case_sets: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    return [
        _dump_contract_model(BenchmarkCaseSetResponse.model_validate(_prepare_contract_payload(BenchmarkCaseSetResponse, item)))
        for item in list(case_sets or [])
    ]


def _build_benchmark_chains(chains: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    return [
        _dump_contract_model(BenchmarkChainResponse.model_validate(_prepare_contract_payload(BenchmarkChainResponse, item)))
        for item in list(chains or [])
    ]


def _build_benchmark_run_result(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    return _dump_contract_model(BenchmarkRunResultResponse.model_validate(_prepare_contract_payload(BenchmarkRunResultResponse, payload)))


def _build_benchmark_job_payload(job: Mapping[str, Any] | None) -> dict[str, Any]:
    payload = dict(job or {})
    result = payload.get("result")
    if isinstance(result, dict):
        payload["result"] = _build_benchmark_run_result(result)
    return _dump_contract_model(BenchmarkJobPayloadResponse.model_validate(_prepare_contract_payload(BenchmarkJobPayloadResponse, payload)))


def build_agent_chat_response(payload: dict[str, Any]) -> dict[str, Any]:
    data = dict(payload or {})
    query_understanding = _build_query_understanding(data.get("query_understanding"))
    planned_tools = _build_planned_tools(data.get("planned_tools"))
    tool_results = _build_tool_results(data.get("tool_results"))
    data["query_understanding"] = query_understanding
    data["planned_tools"] = planned_tools
    data["tool_results"] = tool_results
    response = _dump_contract_model(
        AgentChatResponse.model_validate(_prepare_contract_payload(AgentChatResponse, {**data, "api_schema_version": API_SCHEMA_VERSION}))
    )
    response["query_understanding"] = query_understanding
    response["planned_tools"] = planned_tools
    response["tool_results"] = tool_results
    return response


def build_agent_stream_event(event: dict[str, Any]) -> dict[str, Any]:
    payload = dict(event or {})
    if payload.get("type") == "done" and isinstance(payload.get("payload"), dict):
        payload["payload"] = build_agent_chat_response(payload["payload"])
    return _dump_contract_model(AgentStreamEvent.model_validate(_prepare_contract_payload(AgentStreamEvent, {**payload, "api_schema_version": API_SCHEMA_VERSION})))


def build_benchmark_history_response(results: list[dict[str, Any]]) -> dict[str, Any]:
    items = _build_benchmark_history_items(results)
    response = _dump_contract_model(
        BenchmarkHistoryResponse.model_validate(
            _prepare_contract_payload(
                BenchmarkHistoryResponse,
                {"api_schema_version": API_SCHEMA_VERSION, "results": items},
            )
        )
    )
    response["results"] = items
    return response


def build_benchmark_case_sets_response(case_sets: list[dict[str, Any]], chains: list[dict[str, Any]]) -> dict[str, Any]:
    return _dump_contract_model(
        BenchmarkCaseSetsResponse.model_validate(
            _prepare_contract_payload(
                BenchmarkCaseSetsResponse,
                {
                    "api_schema_version": API_SCHEMA_VERSION,
                    "case_sets": _build_benchmark_case_sets(case_sets),
                    "chains": _build_benchmark_chains(chains),
                },
            )
        )
    )


def build_benchmark_job_response(*, ok: bool, job: dict[str, Any]) -> dict[str, Any]:
    return _dump_contract_model(
        BenchmarkJobResponse.model_validate(
            _prepare_contract_payload(
                BenchmarkJobResponse,
                {
                    "api_schema_version": API_SCHEMA_VERSION,
                    "ok": bool(ok),
                    "job": _build_benchmark_job_payload(job),
                },
            )
        )
    )


def build_benchmark_router_classification_response(payload: dict[str, Any]) -> dict[str, Any]:
    return _dump_contract_model(
        BenchmarkRouterClassificationResponse.model_validate(
            _prepare_contract_payload(BenchmarkRouterClassificationResponse, {**dict(payload or {}), "api_schema_version": API_SCHEMA_VERSION})
        )
    )


def build_benchmark_router_classification_cases_response(cases: list[dict[str, Any]]) -> dict[str, Any]:
    return _dump_contract_model(
        BenchmarkRouterClassificationCasesResponse.model_validate(
            _prepare_contract_payload(BenchmarkRouterClassificationCasesResponse, {"api_schema_version": API_SCHEMA_VERSION, "cases": list(cases or [])})
        )
    )


def build_benchmark_router_classification_case_mutation_response(*, ok: bool, case: dict[str, Any] | None = None, cases: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return _dump_contract_model(
        BenchmarkRouterClassificationCaseMutationResponse.model_validate(
            _prepare_contract_payload(
                BenchmarkRouterClassificationCaseMutationResponse,
                {
                    "api_schema_version": API_SCHEMA_VERSION,
                    "ok": bool(ok),
                    "case": dict(case or {}),
                    "cases": list(cases or []),
                },
            )
        )
    )


def build_benchmark_router_classification_history_response(results: list[dict[str, Any]]) -> dict[str, Any]:
    return _dump_contract_model(
        BenchmarkRouterClassificationHistoryResponse.model_validate(
            _prepare_contract_payload(BenchmarkRouterClassificationHistoryResponse, {"api_schema_version": API_SCHEMA_VERSION, "results": list(results or [])})
        )
    )


def build_benchmark_stream_event(event: dict[str, Any]) -> dict[str, Any]:
    return _dump_contract_model(
        BenchmarkStreamEvent.model_validate(
            _prepare_contract_payload(BenchmarkStreamEvent, {**dict(event or {}), "api_schema_version": API_SCHEMA_VERSION})
        )
    )


def build_dashboard_trace_response(*, ok: bool, trace: dict[str, Any] | None = None, export_text: str = "") -> dict[str, Any]:
    return _dump_contract_model(
        DashboardTraceResponse.model_validate(
            _prepare_contract_payload(
                DashboardTraceResponse,
                {
                    "api_schema_version": API_SCHEMA_VERSION,
                    "ok": bool(ok),
                    "trace": dict(trace or {}),
                    "export_text": str(export_text or ""),
                },
            )
        )
    )
