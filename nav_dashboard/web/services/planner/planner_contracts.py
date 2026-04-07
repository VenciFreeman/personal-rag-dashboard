from __future__ import annotations

from typing import Any

from ..agent.agent_types import CLASSIFIER_LABEL_OTHER, QUERY_TYPE_GENERAL, RouterDecision
from ..media.media_query_adapter import normalize_filter_map as normalize_media_filter_map

ROUTER_DECISION_SCHEMA_VERSION_FIELD = "schema_version"
ROUTER_DECISION_SCHEMA_VERSION = 3
ROUTER_DECISION_PREVIOUS_SCHEMA_VERSION = 2
ROUTER_DECISION_LEGACY_SCHEMA_VERSION = 1
ROUTER_DECISION_SUPPORTED_SCHEMA_VERSIONS = frozenset({
    ROUTER_DECISION_LEGACY_SCHEMA_VERSION,
    ROUTER_DECISION_PREVIOUS_SCHEMA_VERSION,
    ROUTER_DECISION_SCHEMA_VERSION,
})

ROUTER_SLOT_QUERY_CLASS = "query_class"
ROUTER_SLOT_SUBJECT_SCOPE = "subject_scope"
ROUTER_SLOT_TIME_SCOPE_TYPE = "time_scope_type"
ROUTER_SLOT_ANSWER_SHAPE = "answer_shape"
ROUTER_SLOT_MEDIA_FAMILY = "media_family"

ROUTER_QUERY_CLASS_KNOWLEDGE_QA = "knowledge_qa"
ROUTER_QUERY_CLASS_MEDIA_TITLE_DETAIL = "media_title_detail"
ROUTER_QUERY_CLASS_MEDIA_CREATOR_COLLECTION = "media_creator_collection"
ROUTER_QUERY_CLASS_MEDIA_ABSTRACT_CONCEPT = "media_abstract_concept"
ROUTER_QUERY_CLASS_MEDIA_COLLECTION_FILTER = "media_collection_filter"
ROUTER_QUERY_CLASS_MIXED_KNOWLEDGE_WITH_MEDIA = "mixed_knowledge_with_media"
ROUTER_QUERY_CLASS_FOLLOWUP_PROXY = "followup_proxy"
ROUTER_QUERY_CLASS_GENERAL_QA = "general_qa"
ROUTER_QUERY_CLASS_PERSONAL_MEDIA_REVIEW_COLLECTION = "personal_media_review_collection"
ROUTER_QUERY_CLASS_MUSIC_WORK_VERSIONS_COMPARE = "music_work_versions_compare"
ROUTER_QUERY_CLASS_WORKING_SET_ITEM_DETAIL_FOLLOWUP = "working_set_item_detail_followup"

ROUTER_QUERY_CLASSES = (
    ROUTER_QUERY_CLASS_KNOWLEDGE_QA,
    ROUTER_QUERY_CLASS_MEDIA_TITLE_DETAIL,
    ROUTER_QUERY_CLASS_MEDIA_CREATOR_COLLECTION,
    ROUTER_QUERY_CLASS_MEDIA_ABSTRACT_CONCEPT,
    ROUTER_QUERY_CLASS_MEDIA_COLLECTION_FILTER,
    ROUTER_QUERY_CLASS_MIXED_KNOWLEDGE_WITH_MEDIA,
    ROUTER_QUERY_CLASS_FOLLOWUP_PROXY,
    ROUTER_QUERY_CLASS_GENERAL_QA,
    ROUTER_QUERY_CLASS_PERSONAL_MEDIA_REVIEW_COLLECTION,
    ROUTER_QUERY_CLASS_MUSIC_WORK_VERSIONS_COMPARE,
    ROUTER_QUERY_CLASS_WORKING_SET_ITEM_DETAIL_FOLLOWUP,
)

ROUTER_SUBJECT_SCOPE_GENERAL_KNOWLEDGE = "general_knowledge"
ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD = "personal_record"

ROUTER_TIME_SCOPE_NONE = ""
ROUTER_TIME_SCOPE_CONSUMPTION_DATE = "consumption_date"
ROUTER_TIME_SCOPE_PUBLICATION_DATE = "publication_date"

ROUTER_ANSWER_SHAPE_NONE = ""
ROUTER_ANSWER_SHAPE_LIST_ONLY = "list_only"
ROUTER_ANSWER_SHAPE_SUMMARY = "summary"
ROUTER_ANSWER_SHAPE_LIST_PLUS_EXPAND = "list_plus_expand"
ROUTER_ANSWER_SHAPE_DETAIL_CARD = "detail_card"
ROUTER_ANSWER_SHAPE_COMPARE = "compare"

ROUTER_MEDIA_FAMILY_NONE = ""
ROUTER_MEDIA_FAMILY_AUDIOVISUAL = "audiovisual"
ROUTER_MEDIA_FAMILY_BOOKISH = "bookish"
ROUTER_MEDIA_FAMILY_MUSIC = "music"
ROUTER_MEDIA_FAMILY_GAME = "game"
ROUTER_MEDIA_FAMILY_MIXED = "mixed"

ROUTER_DECISION_PLAN_SLOTS = (
    ROUTER_SLOT_QUERY_CLASS,
    ROUTER_SLOT_SUBJECT_SCOPE,
    ROUTER_SLOT_TIME_SCOPE_TYPE,
    ROUTER_SLOT_ANSWER_SHAPE,
    ROUTER_SLOT_MEDIA_FAMILY,
)


def normalize_search_mode(search_mode: str) -> str:
    value = str(search_mode or "").strip().lower()
    if value in {"hybrid", "web", "web_search", "web-search"}:
        return "hybrid"
    return "local_only"


def resolve_router_decision_schema_version(payload: dict[str, Any]) -> int | None:
    raw_version = payload.get(ROUTER_DECISION_SCHEMA_VERSION_FIELD)
    if raw_version in (None, ""):
        return None
    try:
        return int(raw_version)
    except (TypeError, ValueError):
        return None


def validate_router_decision_contract_payload(
    payload: dict[str, Any] | None,
    *,
    allow_unknown_schema: bool,
) -> dict[str, Any]:
    current = payload if isinstance(payload, dict) else {}
    schema_version = resolve_router_decision_schema_version(current)
    schema_supported = schema_version in ROUTER_DECISION_SUPPORTED_SCHEMA_VERSIONS if schema_version is not None else True
    accepted = schema_supported or allow_unknown_schema
    reason = ""
    if not accepted:
        if schema_version is None:
            reason = "missing_router_decision_schema_version"
        else:
            reason = f"unsupported_router_decision_schema_version:{schema_version}"
    return {
        "schema_version": schema_version,
        "schema_supported": schema_supported,
        "accepted": accepted,
        "reason": reason,
    }


def serialize_router_plan_contract_slots(
    *,
    query_class: str = "",
    subject_scope: str = "",
    time_scope_type: str = "",
    answer_shape: str = "",
    media_family: str = "",
) -> dict[str, str]:
    return {
        ROUTER_SLOT_QUERY_CLASS: str(query_class or ROUTER_QUERY_CLASS_KNOWLEDGE_QA),
        ROUTER_SLOT_SUBJECT_SCOPE: str(subject_scope or ROUTER_SUBJECT_SCOPE_GENERAL_KNOWLEDGE),
        ROUTER_SLOT_TIME_SCOPE_TYPE: str(time_scope_type or ROUTER_TIME_SCOPE_NONE),
        ROUTER_SLOT_ANSWER_SHAPE: str(answer_shape or ROUTER_ANSWER_SHAPE_NONE),
        ROUTER_SLOT_MEDIA_FAMILY: str(media_family or ROUTER_MEDIA_FAMILY_NONE),
    }


def serialize_router_decision_plan_contract(decision: RouterDecision) -> dict[str, str]:
    return serialize_router_plan_contract_slots(
        query_class=decision.query_class,
        subject_scope=decision.subject_scope,
        time_scope_type=decision.time_scope_type,
        answer_shape=decision.answer_shape,
        media_family=decision.media_family,
    )


def _normalize_anchor_list(value: Any) -> list[dict[str, Any]]:
    anchors: list[dict[str, Any]] = []
    for item in value or []:
        if not isinstance(item, dict):
            continue
        normalized = {
            str(key): item[key]
            for key in item.keys()
            if str(key).strip()
        }
        if normalized:
            anchors.append(normalized)
    return anchors


def serialize_router_decision(decision: RouterDecision) -> dict[str, Any]:
    payload = {
        ROUTER_DECISION_SCHEMA_VERSION_FIELD: ROUTER_DECISION_SCHEMA_VERSION,
        "raw_question": decision.raw_question,
        "resolved_question": decision.resolved_question,
        "intent": decision.intent,
        "domain": decision.domain,
        "lookup_mode": decision.lookup_mode,
        "selection": normalize_media_filter_map(decision.selection),
        "time_constraint": dict(decision.time_constraint),
        "ranking": dict(decision.ranking),
        "entities": list(decision.entities),
        "filters": normalize_media_filter_map(decision.filters),
        "date_range": list(decision.date_range),
        "sort": decision.sort,
        "freshness": decision.freshness,
        "needs_web": bool(decision.needs_web),
        "needs_doc_rag": bool(decision.needs_doc_rag),
        "needs_media_db": bool(decision.needs_media_db),
        "needs_external_media_db": bool(decision.needs_external_media_db),
        "followup_mode": decision.followup_mode,
        "followup_filter_strategy": str(decision.followup_filter_strategy or "none"),
        "confidence": round(float(decision.confidence or 0.0), 4),
        "reasons": list(decision.reasons),
        "media_type": decision.media_type,
        "llm_label": decision.llm_label,
        "query_type": decision.query_type,
        "allow_downstream_entity_inference": bool(decision.allow_downstream_entity_inference),
        "followup_target": str(decision.followup_target or ""),
        "needs_comparison": bool(decision.needs_comparison),
        "needs_explanation": bool(decision.needs_explanation),
        "metadata_anchors": _normalize_anchor_list(decision.metadata_anchors),
        "scope_anchors": _normalize_anchor_list(decision.scope_anchors),
        "rewritten_queries": {
            str(key): str(value)
            for key, value in (decision.rewritten_queries or {}).items()
            if str(key).strip() and str(value).strip()
        },
        "evidence": dict(decision.evidence),
        "arbitration": str(decision.arbitration or "general_fallback"),
    }
    payload.update(serialize_router_decision_plan_contract(decision))
    return payload


def deserialize_router_decision(payload: dict[str, Any], *, fallback_question: str = "") -> RouterDecision:
    # RouterDecision payloads are intentionally best-effort backward compatible.
    # Versions 1 and 2 are the explicitly supported schema revisions; missing or
    # unknown versions still deserialize the stable shared fields below and fall
    # back to current defaults for anything absent.
    _schema_check = validate_router_decision_contract_payload(payload, allow_unknown_schema=True)
    _schema_version = _schema_check["schema_version"]
    _schema_supported = bool(_schema_check["schema_supported"])
    return RouterDecision(
        raw_question=str(payload.get("raw_question") or fallback_question or ""),
        resolved_question=str(payload.get("resolved_question") or fallback_question or ""),
        intent=str(payload.get("intent") or "knowledge_qa"),
        domain=str(payload.get("domain") or "general"),
        lookup_mode=str(payload.get("lookup_mode") or "general_lookup"),
        selection=normalize_media_filter_map(payload.get("selection")),
        time_constraint=dict(payload.get("time_constraint") or {}),
        ranking=dict(payload.get("ranking") or {}),
        entities=[str(item).strip() for item in (payload.get("entities") or []) if str(item).strip()],
        filters=normalize_media_filter_map(payload.get("filters")),
        date_range=list(payload.get("date_range") or []),
        sort=str(payload.get("sort") or "relevance"),
        freshness=str(payload.get("freshness") or "none"),
        needs_web=bool(payload.get("needs_web")),
        needs_doc_rag=bool(payload.get("needs_doc_rag")),
        needs_media_db=bool(payload.get("needs_media_db")),
        needs_external_media_db=bool(payload.get("needs_external_media_db")),
        followup_mode=str(payload.get("followup_mode") or "none"),
        followup_filter_strategy=str(payload.get("followup_filter_strategy") or "none"),
        confidence=float(payload.get("confidence") or 0.0),
        reasons=[str(item) for item in (payload.get("reasons") or [])],
        media_type=str(payload.get("media_type") or ""),
        llm_label=str(payload.get("llm_label") or CLASSIFIER_LABEL_OTHER),
        query_type=str(payload.get("query_type") or QUERY_TYPE_GENERAL),
        allow_downstream_entity_inference=bool(payload.get("allow_downstream_entity_inference")),
        followup_target=str(payload.get("followup_target") or ""),
        needs_comparison=bool(payload.get("needs_comparison")),
        needs_explanation=bool(payload.get("needs_explanation")),
        metadata_anchors=_normalize_anchor_list(payload.get("metadata_anchors")),
        scope_anchors=_normalize_anchor_list(payload.get("scope_anchors")),
        rewritten_queries={
            str(key): str(value)
            for key, value in ((payload.get("rewritten_queries") or {}).items() if isinstance(payload.get("rewritten_queries"), dict) else [])
            if str(key).strip() and str(value).strip()
        },
        evidence={
            **dict(payload.get("evidence") or {}),
            **({"router_decision_schema_supported": _schema_supported} if _schema_version is not None else {}),
            **({"router_decision_schema_version": _schema_version} if _schema_version is not None else {}),
        },
        arbitration=str(payload.get("arbitration") or "general_fallback"),
        **serialize_router_plan_contract_slots(
            query_class=str(payload.get(ROUTER_SLOT_QUERY_CLASS) or ROUTER_QUERY_CLASS_KNOWLEDGE_QA),
            subject_scope=str(payload.get(ROUTER_SLOT_SUBJECT_SCOPE) or ROUTER_SUBJECT_SCOPE_GENERAL_KNOWLEDGE),
            time_scope_type=str(payload.get(ROUTER_SLOT_TIME_SCOPE_TYPE) or ROUTER_TIME_SCOPE_NONE),
            answer_shape=str(payload.get(ROUTER_SLOT_ANSWER_SHAPE) or ROUTER_ANSWER_SHAPE_NONE),
            media_family=str(payload.get(ROUTER_SLOT_MEDIA_FAMILY) or ROUTER_MEDIA_FAMILY_NONE),
        ),
    )
