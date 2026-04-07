from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..agent.agent_types import ToolExecution
from ..media.media_retrieval_service import MediaRetrievalResponse


@dataclass
class PostRetrievalFacts:
    doc_data: dict[str, Any] = field(default_factory=dict)
    doc_metrics: dict[str, Any] = field(default_factory=dict)
    no_context_count: int = 0


@dataclass
class ResultLayeringOutcome:
    query_profile: dict[str, Any]
    tool_results: list[ToolExecution]
    reference_limit_seconds: float
    per_item_expansion_seconds: float
    fallback_evidence_seconds: float
    post_retrieval_evaluate_seconds: float
    post_retrieval_repairs_seconds: float
    answer_strategy_seconds: float
    guardrail_mode_seconds: float
    doc_data: dict[str, Any] = field(default_factory=dict)
    doc_metrics: dict[str, Any] = field(default_factory=dict)
    usage_metrics: dict[str, Any] = field(default_factory=dict)
    media_response: MediaRetrievalResponse | None = None
    answer_mode: dict[str, Any] = field(default_factory=dict)
    answer_strategy: Any = None
    post_retrieval_outcome: Any = None
    guardrail_flags: dict[str, Any] = field(default_factory=dict)


@dataclass
class PreparedToolResults:
    query_profile: dict[str, Any]
    tool_results: list[ToolExecution]
    answer_shape: str
    query_class: str
    subject_scope: str
    media_family: str
    reference_limit_seconds: float
    per_item_expansion_seconds: float


@dataclass
class PostRetrievalEvaluation:
    tool_results: list[ToolExecution]
    facts: PostRetrievalFacts
    fallback_evidence_seconds: float
    post_retrieval_outcome: Any
    answer_strategy: Any
    post_retrieval_evaluate_seconds: float
    post_retrieval_repairs_seconds: float
    answer_strategy_seconds: float


@dataclass
class AnswerInputsBuildResult:
    media_response: MediaRetrievalResponse
    answer_mode: dict[str, Any]
    guardrail_flags: dict[str, Any]
    guardrail_mode_seconds: float
    usage_metrics: dict[str, Any]
    side_effect_requests: Any = None


@dataclass
class ResultLayeringEvaluation:
    prepared: PreparedToolResults
    post_retrieval: PostRetrievalEvaluation
    answer_inputs: AnswerInputsBuildResult