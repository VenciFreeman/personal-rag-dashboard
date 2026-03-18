"""nav_dashboard/web/services/agent_types.py

Shared dataclasses and tool-name constants for the nav_dashboard agent.

All modules in this package import from here rather than from each other
to avoid circular dependencies.  ``agent_service.py`` re-exports every
symbol so that existing callers using ``svc.RouterDecision`` etc. continue
to work without changes.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

# ── Tool identifier strings ───────────────────────────────────────────────────
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

# ── Classifier / query-type constants ─────────────────────────────────────────
QUERY_TYPE_TECH = "TECH_QUERY"
QUERY_TYPE_MEDIA = "MEDIA_QUERY"
QUERY_TYPE_MIXED = "MIXED_QUERY"
QUERY_TYPE_GENERAL = "GENERAL_QUERY"
CLASSIFIER_LABEL_MEDIA = "MEDIA"
CLASSIFIER_LABEL_TECH = "TECH"
CLASSIFIER_LABEL_OTHER = "OTHER"


# ── Dataclasses ───────────────────────────────────────────────────────────────

@dataclass
class PlannedToolCall:
    name: str
    query: str
    options: dict[str, Any] = field(default_factory=dict)


@dataclass
class ToolExecution:
    tool: str
    status: str
    summary: str
    data: Any


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
    # Fine-grained semantic class of the query; populated after semantic repairs.
    # Drives tool selection in RoutingPolicy and response structure in AnswerPolicy.
    # Values: knowledge_qa | media_title_detail | media_creator_collection |
    #         media_collection_filter | media_abstract_concept |
    #         music_work_versions_compare |
    #         mixed_knowledge_with_media | followup_proxy | general_qa
    query_class: str = "knowledge_qa"
    # Whether the question is about the user's personal consumption record
    # ("我看过/我读过…") or general knowledge about a work/creator.
    # Values: personal_record | general_knowledge
    subject_scope: str = "general_knowledge"
    # What kind of date window the user is constraining:
    #   consumption_date  — filter by when the user consumed the item (library date field)
    #   publication_date  — filter by the work's release/publication year
    #   ""                — no meaningful date scope detected
    time_scope_type: str = ""
    # Intended answer shape; drives response_structure in AnswerPolicy.
    # Values: list_only | list_plus_expand | detail_card | compare | ""
    answer_shape: str = ""
    # Media content family — derived from media_type + evidence; shared by
    # RoutingPolicy, PostRetrievalPolicy, and AnswerPolicy so each layer
    # uses the same signal instead of re-inferring audiovisual/bookish.
    # Values: audiovisual | bookish | music | game | mixed | ""
    media_family: str = ""


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


@dataclass
class PostRetrievalAssessment:
    status: str = "pending_post_retrieval"
    doc_similarity: dict[str, Any] = field(default_factory=dict)
    media_validation: dict[str, Any] = field(default_factory=dict)
    tmdb: dict[str, Any] = field(default_factory=dict)
    tech_score: float = 0.0
    weak_tech_signal: bool = False


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
