"""nav_dashboard/web/services/agent/agent_types.py

Shared dataclasses and tool-name constants for the nav_dashboard agent.

All modules in this package import from here rather than from each other
to avoid circular dependencies. ``agent_service.py`` re-exports every
symbol so that existing callers using ``svc.RouterDecision`` etc. continue
to work without changes.
"""
from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any, Callable, Literal

# Tool identifier strings
TOOL_QUERY_DOC_RAG = "query_document_rag"
TOOL_QUERY_MEDIA = "query_media_record"
TOOL_SEARCH_WEB = "search_web"
TOOL_EXPAND_DOC_QUERY = "expand_document_query"
TOOL_EXPAND_MEDIA_QUERY = "expand_media_query"
TOOL_SEARCH_MEDIAWIKI = "search_mediawiki_action"
TOOL_PARSE_MEDIAWIKI = "parse_mediawiki_page"
TOOL_EXPAND_MEDIAWIKI_CONCEPT = "expand_mediawiki_concept"
TOOL_SEARCH_TMDB = "search_tmdb_media"
TOOL_SEARCH_BANGUMI = "search_bangumi_subject"
TOOL_SEARCH_BY_CREATOR = "search_by_creator"

TOOL_NAMES = [
    TOOL_QUERY_DOC_RAG,
    TOOL_QUERY_MEDIA,
    TOOL_SEARCH_WEB,
    TOOL_EXPAND_DOC_QUERY,
    TOOL_EXPAND_MEDIA_QUERY,
    TOOL_SEARCH_MEDIAWIKI,
    TOOL_PARSE_MEDIAWIKI,
    TOOL_EXPAND_MEDIAWIKI_CONCEPT,
    TOOL_SEARCH_TMDB,
    TOOL_SEARCH_BANGUMI,
    TOOL_SEARCH_BY_CREATOR,
]

# Classifier / query-type constants
QUERY_TYPE_TECH = "TECH_QUERY"
QUERY_TYPE_MEDIA = "MEDIA_QUERY"
QUERY_TYPE_MIXED = "MIXED_QUERY"
QUERY_TYPE_GENERAL = "GENERAL_QUERY"
CLASSIFIER_LABEL_MEDIA = "MEDIA"
CLASSIFIER_LABEL_TECH = "TECH"
CLASSIFIER_LABEL_OTHER = "OTHER"


@dataclass(frozen=True)
class MappingPayload(Mapping[str, Any]):
    values: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, payload: Any) -> "MappingPayload":
        if isinstance(payload, cls):
            return payload
        if isinstance(payload, Mapping):
            return cls(values=dict(payload))
        return cls()

    def __getitem__(self, key: str) -> Any:
        return self.values[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.values)

    def __len__(self) -> int:
        return len(self.values)

    def get(self, key: str, default: Any = None) -> Any:
        return self.values.get(key, default)

    def to_dict(self) -> dict[str, Any]:
        return dict(self.values)


@dataclass(frozen=True)
class QueryProfile(MappingPayload):
    pass


@dataclass(frozen=True)
class QuotaState(MappingPayload):
    pass


@dataclass(frozen=True)
class ConfirmationPayload(Mapping[str, Any]):
    trace_id: str
    session_id: str
    exceeded: list[dict[str, Any]] = field(default_factory=list)
    planned_tools: list[dict[str, Any]] = field(default_factory=list)
    requires_confirmation: bool = True
    confirmation_message: str = "已超过今日 API 配额，是否继续调用超额工具？"

    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.to_dict())

    def __len__(self) -> int:
        return len(self.to_dict())

    def get(self, key: str, default: Any = None) -> Any:
        return self.to_dict().get(key, default)

    def to_dict(self) -> dict[str, Any]:
        return {
            "requires_confirmation": bool(self.requires_confirmation),
            "trace_id": self.trace_id,
            "session_id": self.session_id,
            "confirmation_message": self.confirmation_message,
            "exceeded": list(self.exceeded),
            "planned_tools": list(self.planned_tools),
        }


@dataclass
class PlannedToolCall:
    name: str
    query: str
    options: dict[str, Any] = field(default_factory=dict)
    plan_index: int = -1


@dataclass
class ToolExecution:
    tool: str
    status: str
    summary: str
    data: Any
    plan_index: int = -1


@dataclass
class RouterDecision:
    raw_question: str
    resolved_question: str
    intent: Literal["knowledge_qa", "media_lookup", "mixed", "chat"]
    domain: Literal["tech", "media", "general"]
    lookup_mode: Literal["general_lookup", "entity_lookup", "filter_search", "concept_lookup"] = "general_lookup"
    selection: dict[str, list[str]] = field(default_factory=dict)
    time_constraint: dict[str, Any] = field(default_factory=dict)
    ranking: dict[str, Any] = field(default_factory=dict)
    entities: list[str] = field(default_factory=list)
    filters: dict[str, list[str]] = field(default_factory=dict)
    date_range: list[str] = field(default_factory=list)
    sort: str = "relevance"
    freshness: Literal["none", "recent", "realtime"] = "none"
    needs_web: bool = False
    needs_doc_rag: bool = False
    needs_media_db: bool = False
    needs_external_media_db: bool = False
    followup_mode: Literal["none", "inherit_filters", "inherit_entity", "inherit_timerange"] = "none"
    followup_filter_strategy: Literal["none", "carry", "replace", "augment"] = "none"
    confidence: float = 0.0
    reasons: list[str] = field(default_factory=list)
    media_type: str = ""
    llm_label: str = CLASSIFIER_LABEL_OTHER
    query_type: str = QUERY_TYPE_GENERAL
    allow_downstream_entity_inference: bool = False
    followup_target: str = ""
    needs_comparison: bool = False
    needs_explanation: bool = False
    rewritten_queries: dict[str, str] = field(default_factory=dict)
    evidence: dict[str, Any] = field(default_factory=dict)
    arbitration: str = "general_fallback"
    query_class: str = "knowledge_qa"
    subject_scope: str = "general_knowledge"
    time_scope_type: str = ""
    answer_shape: str = ""
    media_family: str = ""
    metadata_anchors: list[dict[str, Any]] = field(default_factory=list)
    scope_anchors: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class ExecutionPlan:
    decision: RouterDecision
    planned_tools: list[PlannedToolCall] = field(default_factory=list)
    primary_tool: str = ""
    fallback_tools: list[str] = field(default_factory=list)
    reasons: list[str] = field(default_factory=list)


@dataclass
class RouterContextResolution:
    resolved_question: str
    resolved_query_state: dict[str, Any] = field(default_factory=dict)
    conversation_state_before: dict[str, Any] = field(default_factory=dict)
    conversation_state_after: dict[str, Any] = field(default_factory=dict)
    detected_followup: bool = False
    inheritance_applied: dict[str, str] = field(default_factory=dict)
    state_diff: dict[str, Any] = field(default_factory=dict)
    planner_snapshot: dict[str, Any] = field(default_factory=dict)
    planning_timing_breakdown: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class PlanningArtifact:
    decision: RouterDecision | None = None
    execution_plan: ExecutionPlan | None = None
    context_resolution: RouterContextResolution | None = None
    planner_snapshot: dict[str, Any] = field(default_factory=dict)
    resolved_query_state: dict[str, Any] = field(default_factory=dict)
    planning_timing_breakdown: dict[str, float] = field(default_factory=dict)
    decision_category: str = ""
    decision_path: list[str] = field(default_factory=list)
    metadata_anchors: list[dict[str, Any]] = field(default_factory=list)
    scope_anchors: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class PostRetrievalAssessment:
    status: str = "pending_post_retrieval"
    doc_similarity: dict[str, Any] = field(default_factory=dict)
    media_validation: dict[str, Any] = field(default_factory=dict)
    tmdb: dict[str, Any] = field(default_factory=dict)
    tech_score: float = 0.0
    weak_tech_signal: bool = False


@dataclass(frozen=True)
class ExecutionArtifact:
    fallback_evidence: dict[str, Any] = field(default_factory=dict)
    doc_similarity: dict[str, Any] = field(default_factory=dict)
    tech_score: float = 0.0
    weak_tech_signal: bool = False
    post_retrieval_outcome: dict[str, Any] = field(default_factory=dict)
    answer_strategy: dict[str, Any] = field(default_factory=dict)
    guardrail_flags: dict[str, Any] = field(default_factory=dict)
    error_taxonomy: dict[str, Any] = field(default_factory=dict)
    doc_data: dict[str, Any] = field(default_factory=dict)
    doc_metrics: dict[str, Any] = field(default_factory=dict)
    usage_metrics: dict[str, Any] = field(default_factory=dict)
    working_set: dict[str, Any] = field(default_factory=dict)
    media_validation: dict[str, Any] = field(default_factory=dict)
    candidate_source_breakdown: dict[str, Any] = field(default_factory=dict)
    media_timing_breakdown: dict[str, Any] = field(default_factory=dict)
    layer_breakdown: dict[str, Any] = field(default_factory=dict)
    alias_resolution: dict[str, Any] = field(default_factory=dict)
    streaming_plan: dict[str, Any] = field(default_factory=dict)


@dataclass
class AgentRuntimeState:
    decision: RouterDecision
    execution_plan: ExecutionPlan
    context_resolution: RouterContextResolution
    llm_media: dict[str, Any] = field(default_factory=dict)
    previous_context_state: dict[str, Any] = field(default_factory=dict)
    post_retrieval_assessment: PostRetrievalAssessment = field(
        default_factory=PostRetrievalAssessment
    )
    planning_artifact: PlanningArtifact = field(default_factory=PlanningArtifact)
    execution_artifact: ExecutionArtifact = field(default_factory=ExecutionArtifact)


@dataclass(frozen=True)
class PlannerDeps:
    prepare_round_answer_fn: Callable[..., Any]
    normalize_trace_id: Callable[[str], str]
    routing_deps: Any
    build_confirmation_payload: Callable[..., ConfirmationPayload]
    resolve_allowed_plan: Callable[..., Any]
    execute_tool_phase: Callable[..., Any]
    execute_tool_plan_boundary: Callable[..., Any]
    tool_plan_deps: Any


@dataclass(frozen=True)
class ExecutionDeps:
    append_message: Callable[..., None]
    build_memory_context: Callable[[str], str]
    approx_tokens: Callable[[str], int]
    execute_per_item_expansion: Callable[..., Any]


@dataclass(frozen=True)
class AnswerDeps:
    generate_sync_answer_fn: Callable[..., Any]
    start_streaming_answer_fn: Callable[..., Any]
    finalize_round_answer_fn: Callable[..., Any]
    build_structured_media_answer: Callable[..., str]
    summarize_answer: Callable[..., Any]
    fallback_retrieval_answer: Callable[..., str]
    apply_guardrail_answer_mode: Callable[[str, dict[str, Any]], str]
    append_media_mentions_to_answer: Callable[[str, str, Any, list[Any]], str]
    compose_round_answer: Callable[..., Any]
    render_streaming_answer: Callable[..., Any]
    build_structured_media_answer_chunks: Callable[..., list[str]]
    build_structured_media_external_item_block: Callable[..., str]


@dataclass(frozen=True)
class ObservabilityDeps:
    persist_round_response_fn: Callable[..., Any]
    persist_session_artifacts: Callable[..., Any]
    update_memory_for_session: Callable[..., Any]
    schedule_generated_session_title: Callable[..., Any]
    auto_queue_bug_tickets: Callable[..., Any]
    write_debug_record: Callable[..., Any]
    record_metrics_safe: Callable[..., Any]
    record_agent_metrics: Callable[..., Any]
    build_agent_trace_record: Callable[..., dict[str, Any]]
    write_agent_trace_record: Callable[[dict[str, Any]], None]
    get_query_type: Callable[..., str]
    web_search_daily_limit: int
    deepseek_daily_limit: int
    serialize_planned_tools: Callable[..., list[dict[str, Any]]]
    update_debug_trace_with_tool_plan: Callable[..., None]