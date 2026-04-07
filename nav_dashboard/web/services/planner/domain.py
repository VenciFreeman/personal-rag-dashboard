from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable

from . import planner_contracts
from ..agent.agent_types import AgentRuntimeState, ExecutionPlan, PlannedToolCall, RouterContextResolution, RouterDecision


@dataclass(frozen=True)
class RouterDecisionNormalizationResult:
    router_decision: RouterDecision
    llm_media: dict[str, Any]
    previous_context_state: dict[str, Any]
    query_classification: dict[str, Any]


@dataclass(frozen=True)
class ExecutionPlanShapingResult:
    execution_plan: ExecutionPlan
    planned_tools: list[PlannedToolCall]
    context_resolution: RouterContextResolution
    runtime_state: AgentRuntimeState
    resolved_question: str


@dataclass(frozen=True)
class RouterSemanticDeps:
    question_requests_personal_evaluation: Callable[[str], bool]
    question_requests_media_details: Callable[[str], bool]
    is_collection_media_query: Callable[[str], bool]


_PERSONAL_SCOPE_CUES = (
    "我看过", "我看了", "我追过", "我补过",
    "我读过", "我读了", "我听过",
    "我玩过", "我打过", "我记录过",
)
_PERSONAL_SCOPE_RE = re.compile(
    r"我.{0,25}[看追补读听玩打][了过]",
    re.UNICODE,
)
_IMPLICIT_PERSONAL_SCOPE_RE = re.compile(
    r"^(?:最近|近期|最近一段时间|这段时间|这几年|近几年)?(?:我)?(?:听过|看过|读过|玩过|追过|补过|收藏过|买过|记录过)",
    re.UNICODE,
)
_EXPLICIT_PERSONAL_RECORD_RE = re.compile(
    r"(?:个人|自己的|我的)(?:记录|评分|评价|短评|评论|看法|感受|印象)(?:与(?:短评|评论|评价|评分))?",
    re.UNICODE,
)
_FIRST_PERSON_PERSONAL_RECORD_RE = re.compile(
    r"我.{0,30}(?:记录|评分|评价|短评|评论|看法|感受|印象)",
    re.UNICODE,
)
_MEDIA_ENTITY_STRUCTURAL_SUFFIX_RE = re.compile(
    r"的?(?:(?:个人|自己的|我的)?(?:评分|评价|短评|评论|看法|感受|印象)(?:与(?:短评|评论|评价|评分))?|(?:对比|比较|区别|差异)|(?:条目细节|条目详情|详细介绍|分别介绍|分别说说)).*$",
    re.UNICODE,
)
_MEDIA_ALIAS_SCOPE_PREFIX_RE = re.compile(
    r"^(?:我|你|他|她|它)?(?:最近|近两年|近几年|之前|以前)?(?:看过|玩过|读过|听过|买过|补过|收藏过)?(?:哪些|什么|有啥|有哪些|有哪几(?:部|本|个|张)|哪几(?:部|本|个|张))?"
)
_MEDIA_ALIAS_SCOPE_TRAILING_RE = re.compile(
    r"(?:有哪些|有什么|都有哪些|都有什么|分别介绍一下|分别介绍|分别讲什么|分别讲讲|介绍一下|介绍|评价怎么样|评价如何|怎么样|如何|讲什么|讲了什么|是什么内容|内容|简介|呢|吧|呀|啊|吗|嘛)+$"
)
_MEDIA_ALIAS_SCOPE_NOUNS = (
    "游戏",
    "galgame",
    "动漫",
    "动画",
    "番剧",
    "番",
    "电影",
    "剧集",
    "电视剧",
    "剧",
    "书",
    "小说",
    "专辑",
    "音乐",
    "作品",
    "媒体",
    "条目",
)

_AUDIOVISUAL_MEDIA_TYPES: frozenset[str] = frozenset({
    "video", "movie", "film", "tv", "drama", "anime", "animation",
    "documentary", "series", "show", "miniseries",
})
_BOOKISH_MEDIA_TYPES: frozenset[str] = frozenset({
    "book", "novel", "manga", "comic", "essay", "literature",
    "nonfiction", "fiction", "poetry",
})
_MUSIC_MEDIA_TYPES: frozenset[str] = frozenset({"music", "album", "song"})
_GAME_MEDIA_TYPES: frozenset[str] = frozenset({"game", "videogame", "boardgame"})


def has_personal_scope_surface(text: str) -> bool:
    question = str(text or "").strip()
    if not question:
        return False
    return bool(
        any(cue in question for cue in _PERSONAL_SCOPE_CUES)
        or _PERSONAL_SCOPE_RE.search(question)
        or _IMPLICIT_PERSONAL_SCOPE_RE.search(question)
        or _FIRST_PERSON_PERSONAL_RECORD_RE.search(question)
    )


def targets_personal_record_without_first_person(text: str) -> bool:
    question = str(text or "").strip()
    if not question:
        return False
    return bool(_EXPLICIT_PERSONAL_RECORD_RE.search(question))


def _question_requests_summary(text: str) -> bool:
    question = str(text or "").strip()
    if not question:
        return False
    summary_cues = (
        "总结", "概括", "概述", "归纳", "总的来说", "整体来看", "总体上", "层次", "分层", "梳理",
    )
    return any(cue in question for cue in summary_cues)


def _infer_media_family_from_filters(filters: dict[str, Any] | None) -> str:
    normalized = planner_contracts.normalize_media_filter_map(filters)
    values = list(normalized.get("media_type") or [])
    if len(values) == 1:
        return derive_media_family_from_media_type(values[0])
    return planner_contracts.ROUTER_MEDIA_FAMILY_NONE


def _infer_media_family_from_question(text: str) -> str:
    question = str(text or "").strip()
    if not question:
        return planner_contracts.ROUTER_MEDIA_FAMILY_NONE
    if any(cue in question for cue in ("玩过", "打过", "游戏", "galgame", "型月")):
        return planner_contracts.ROUTER_MEDIA_FAMILY_GAME
    if any(cue in question for cue in ("看过", "动画", "动漫", "番剧", "电影", "剧集", "京吹", "利兹与青鸟")):
        return planner_contracts.ROUTER_MEDIA_FAMILY_AUDIOVISUAL
    if any(cue in question for cue in ("听过", "专辑", "歌曲", "音乐", "唱片", "版本", "Beyond")):
        return planner_contracts.ROUTER_MEDIA_FAMILY_MUSIC
    if any(cue in question for cue in ("读过", "书", "小说", "漫画", "文学", "鼠疫")):
        return planner_contracts.ROUTER_MEDIA_FAMILY_BOOKISH
    return planner_contracts.ROUTER_MEDIA_FAMILY_NONE


def trim_media_entity_structural_suffix(text: str) -> str:
    normalized = _MEDIA_ENTITY_STRUCTURAL_SUFFIX_RE.sub("", str(text or "").strip())
    return normalized.strip(" ，。！？?；;:：\"'“”‘’（）()")


def extract_media_alias_anchor_queries(
    text: str,
    *,
    looks_like_generic_media_scope: Callable[[str], bool] | None = None,
) -> list[str]:
    generic_scope_predicate = looks_like_generic_media_scope or (lambda _text: False)
    raw = str(text or "").strip().strip("？?。！!，,；;：:")
    if not raw:
        return []
    candidates: list[str] = []
    seen: set[str] = set()

    def _append(candidate: str) -> None:
        clean = str(candidate or "").strip().strip("的 ")
        if not clean or len(clean) < 2 or len(clean) > 24 or generic_scope_predicate(clean):
            return
        folded = clean.casefold()
        if folded in seen:
            return
        seen.add(folded)
        candidates.append(clean)

    trimmed = _MEDIA_ALIAS_SCOPE_PREFIX_RE.sub("", raw).strip()
    trimmed = re.sub(r"[，,、]\s*", "", trimmed)
    trimmed = _MEDIA_ALIAS_SCOPE_TRAILING_RE.sub("", trimmed).strip().strip("的 ")
    _append(trimmed)
    for noun in _MEDIA_ALIAS_SCOPE_NOUNS:
        if not trimmed.endswith(noun):
            continue
        _append(trimmed[: -len(noun)].strip())
    return candidates


def resolve_router_metadata_anchors(
    text: str,
    *,
    filters: dict[str, list[str]] | None,
    media_type: str,
    resolve_library_aliases: Callable[..., dict[str, Any]],
    looks_like_generic_media_scope: Callable[[str], bool] | None = None,
    max_queries: int = 3,
) -> dict[str, Any]:
    anchor_queries = extract_media_alias_anchor_queries(
        text,
        looks_like_generic_media_scope=looks_like_generic_media_scope,
    )[:max_queries]
    if not anchor_queries:
        return {"queries": [], "entries": [], "matched_terms": [], "canonical_terms": []}

    resolved_filters = {
        str(field): [str(value).strip() for value in values if str(value).strip()]
        for field, values in dict(filters or {}).items()
        if isinstance(values, list)
    }
    if media_type and not resolved_filters.get("media_type"):
        resolved_filters["media_type"] = [str(media_type).strip().lower()]

    entries: list[dict[str, Any]] = []
    matched_terms: list[str] = []
    canonical_terms: list[str] = []
    seen_entries: set[str] = set()
    seen_matched_terms: set[str] = set()
    seen_canonical_terms: set[str] = set()

    for query in anchor_queries:
        resolution = resolve_library_aliases(query, filters=resolved_filters)
        for entry in resolution.get("entries") or []:
            if not isinstance(entry, dict):
                continue
            field_type = str(entry.get("field_type") or entry.get("field") or "").strip().lower()
            if not field_type or field_type == "title":
                continue
            canonical_name = str(entry.get("canonical_name") or entry.get("canonical") or entry.get("raw_value") or "").strip()
            matched_text = str(entry.get("matched_text") or query or "").strip()
            media_type_value = str(entry.get("media_type") or media_type or "").strip().lower()
            entry_key = "|".join([
                str(entry.get("key") or "").strip(),
                field_type,
                media_type_value,
                canonical_name.casefold(),
                matched_text.casefold(),
            ])
            if not canonical_name or entry_key in seen_entries:
                continue
            seen_entries.add(entry_key)
            entries.append(
                {
                    "query": query,
                    "key": str(entry.get("key") or "").strip(),
                    "field": str(entry.get("field") or field_type).strip(),
                    "field_type": field_type,
                    "media_type": media_type_value,
                    "canonical_name": canonical_name,
                    "matched_text": matched_text,
                    "expanded_terms": [
                        str(term).strip()
                        for term in list(entry.get("expanded_terms") or [])
                        if str(term).strip()
                    ],
                }
            )
            matched_folded = matched_text.casefold()
            if matched_text and matched_folded not in seen_matched_terms:
                seen_matched_terms.add(matched_folded)
                matched_terms.append(matched_text)
            canonical_folded = canonical_name.casefold()
            if canonical_folded not in seen_canonical_terms:
                seen_canonical_terms.add(canonical_folded)
                canonical_terms.append(canonical_name)

    return {
        "queries": anchor_queries,
        "entries": entries,
        "matched_terms": matched_terms,
        "canonical_terms": canonical_terms,
    }


def derive_metadata_anchors(metadata_anchor_resolution: dict[str, Any] | None) -> list[dict[str, Any]]:
    anchors: list[dict[str, Any]] = []
    seen: set[str] = set()
    anchor_priority = {
        "publisher": 0,
        "platform": 1,
        "channel": 2,
        "studio": 3,
        "creator": 4,
        "author": 4,
    }
    resolution = metadata_anchor_resolution if isinstance(metadata_anchor_resolution, dict) else {}
    for entry in resolution.get("entries") or []:
        if not isinstance(entry, dict):
            continue
        field_type = str(entry.get("field_type") or entry.get("field") or "metadata").strip().lower() or "metadata"
        canonical_name = str(entry.get("canonical_name") or entry.get("canonical") or entry.get("raw_value") or "").strip()
        matched_text = str(entry.get("matched_text") or "").strip()
        media_type = str(entry.get("media_type") or "").strip().lower()
        anchor_key = "|".join([field_type, media_type, canonical_name.casefold(), matched_text.casefold()])
        if not canonical_name or anchor_key in seen:
            continue
        seen.add(anchor_key)
        anchors.append(
            {
                "scope_kind": "metadata",
                "anchor_type": field_type,
                "field": str(entry.get("field") or field_type).strip() or field_type,
                "field_type": field_type,
                "media_type": media_type,
                "canonical_name": canonical_name,
                "matched_text": matched_text,
                "query": str(entry.get("query") or "").strip(),
                "expanded_terms": [
                    str(term).strip()
                    for term in list(entry.get("expanded_terms") or [])
                    if str(term).strip()
                ],
            }
        )
    anchors.sort(
        key=lambda item: (
            int(anchor_priority.get(str(item.get("field_type") or item.get("anchor_type") or "metadata").strip().lower(), 99)),
            str(item.get("media_type") or "").strip().lower(),
            str(item.get("canonical_name") or "").strip().casefold(),
            str(item.get("matched_text") or "").strip().casefold(),
        )
    )
    return anchors


def derive_scope_anchors(decision: RouterDecision, metadata_anchors: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    anchors: list[dict[str, Any]] = []
    seen: set[str] = set()
    for entity in decision.entities or []:
        clean = str(entity or "").strip()
        if not clean:
            continue
        entity_key = f"entity|{clean.casefold()}"
        if entity_key in seen:
            continue
        seen.add(entity_key)
        anchors.append(
            {
                "scope_kind": "entity",
                "anchor_type": "title_entity",
                "value": clean,
                "media_type": str(decision.media_type or "").strip().lower(),
            }
        )
    for anchor in metadata_anchors or []:
        if not isinstance(anchor, dict):
            continue
        canonical_name = str(anchor.get("canonical_name") or anchor.get("value") or "").strip()
        anchor_type = str(anchor.get("anchor_type") or anchor.get("field_type") or "metadata").strip().lower() or "metadata"
        media_type = str(anchor.get("media_type") or decision.media_type or "").strip().lower()
        anchor_key = f"metadata|{anchor_type}|{media_type}|{canonical_name.casefold()}"
        if not canonical_name or anchor_key in seen:
            continue
        seen.add(anchor_key)
        anchors.append(
            {
                "scope_kind": "metadata",
                "anchor_type": anchor_type,
                "value": canonical_name,
                "field": str(anchor.get("field") or anchor.get("field_type") or anchor_type).strip() or anchor_type,
                "field_type": str(anchor.get("field_type") or anchor_type).strip() or anchor_type,
                "media_type": media_type,
                "matched_text": str(anchor.get("matched_text") or "").strip(),
            }
        )
    return anchors


def derive_query_class(decision: RouterDecision, *, deps: RouterSemanticDeps) -> str:
    ev = dict(decision.evidence or {})
    if ev.get("working_set_item_followup"):
        return planner_contracts.ROUTER_QUERY_CLASS_WORKING_SET_ITEM_DETAIL_FOLLOWUP
    single_entity_personal_review_detail = (
        decision.domain == "media"
        and decision.subject_scope == planner_contracts.ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD
        and len(decision.entities) == 1
        and deps.question_requests_personal_evaluation(str(decision.raw_question or ""))
        and not bool(ev.get("creator_collection_query"))
        and not bool(ev.get("title_anchored_version_compare_query"))
        and not bool(ev.get("alias_title_collection_query"))
    )
    personal_review_query = (
        deps.question_requests_personal_evaluation(str(decision.raw_question or ""))
        or bool(ev.get("creator_collection_query"))
        or bool(ev.get("title_anchored_version_compare_query"))
        or bool(ev.get("title_anchored_personal_review_query"))
        or bool(ev.get("alias_title_collection_query"))
    )
    if (
        decision.domain == "media"
        and decision.subject_scope == planner_contracts.ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD
        and personal_review_query
        and not single_entity_personal_review_detail
    ):
        return planner_contracts.ROUTER_QUERY_CLASS_PERSONAL_MEDIA_REVIEW_COLLECTION
    if ev.get("creator_collection_query"):
        return planner_contracts.ROUTER_QUERY_CLASS_MEDIA_CREATOR_COLLECTION
    if ev.get("abstract_media_concept"):
        return planner_contracts.ROUTER_QUERY_CLASS_MEDIA_ABSTRACT_CONCEPT
    if ev.get("music_work_versions_compare"):
        return planner_contracts.ROUTER_QUERY_CLASS_MUSIC_WORK_VERSIONS_COMPARE
    if decision.domain == "media" and decision.lookup_mode == "entity_lookup" and len(decision.entities) == 1:
        return planner_contracts.ROUTER_QUERY_CLASS_MEDIA_TITLE_DETAIL
    if decision.domain == "media" and decision.lookup_mode == "filter_search":
        return planner_contracts.ROUTER_QUERY_CLASS_MEDIA_COLLECTION_FILTER
    if decision.intent == "mixed":
        return planner_contracts.ROUTER_QUERY_CLASS_MIXED_KNOWLEDGE_WITH_MEDIA
    if decision.followup_mode != "none":
        return planner_contracts.ROUTER_QUERY_CLASS_FOLLOWUP_PROXY
    if decision.domain in {"tech", "general"} or decision.intent == "knowledge_qa":
        return planner_contracts.ROUTER_QUERY_CLASS_KNOWLEDGE_QA
    return planner_contracts.ROUTER_QUERY_CLASS_GENERAL_QA


def derive_subject_scope(decision: RouterDecision, *, deps: RouterSemanticDeps) -> str:
    text = str(decision.raw_question or "").strip()
    if has_personal_scope_surface(text):
        return planner_contracts.ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD
    if decision.domain == "media" and deps.question_requests_personal_evaluation(text) and targets_personal_record_without_first_person(text):
        return planner_contracts.ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD
    evidence = dict(decision.evidence or {})
    previous_subject_scope = str(evidence.get("previous_subject_scope") or "").strip()
    if previous_subject_scope == planner_contracts.ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD and decision.followup_mode != "none":
        return planner_contracts.ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD
    if decision.followup_mode != "none" or evidence.get("working_set_followup") or evidence.get("working_set_item_followup"):
        previous_question = str(evidence.get("previous_question") or "").strip()
        previous_working_set = evidence.get("previous_working_set") if isinstance(evidence.get("previous_working_set"), dict) else {}
        inherited_text = previous_question or str(previous_working_set.get("query") or "").strip()
        if has_personal_scope_surface(inherited_text):
            return planner_contracts.ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD
        if decision.domain == "media" and deps.question_requests_personal_evaluation(inherited_text) and targets_personal_record_without_first_person(inherited_text):
            return planner_contracts.ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD
    return planner_contracts.ROUTER_SUBJECT_SCOPE_GENERAL_KNOWLEDGE


def derive_time_scope_type(decision: RouterDecision) -> str:
    if not decision.time_constraint:
        return planner_contracts.ROUTER_TIME_SCOPE_NONE
    if str(decision.subject_scope or "").strip() == planner_contracts.ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD:
        return planner_contracts.ROUTER_TIME_SCOPE_CONSUMPTION_DATE
    text = str(decision.raw_question or "").strip()
    if has_personal_scope_surface(text):
        return planner_contracts.ROUTER_TIME_SCOPE_CONSUMPTION_DATE
    evidence = dict(decision.evidence or {})
    if decision.followup_mode != "none" or evidence.get("working_set_followup") or evidence.get("working_set_item_followup"):
        previous_question = str(evidence.get("previous_question") or "").strip()
        previous_working_set = evidence.get("previous_working_set") if isinstance(evidence.get("previous_working_set"), dict) else {}
        inherited_text = previous_question or str(previous_working_set.get("query") or "").strip()
        if inherited_text and has_personal_scope_surface(inherited_text):
            return planner_contracts.ROUTER_TIME_SCOPE_CONSUMPTION_DATE
    return planner_contracts.ROUTER_TIME_SCOPE_PUBLICATION_DATE


def derive_answer_shape(decision: RouterDecision, *, deps: RouterSemanticDeps) -> str:
    text = str(decision.raw_question or "").strip()
    evidence = dict(decision.evidence or {})
    single_entity_personal_review_detail = (
        decision.domain == "media"
        and decision.subject_scope == planner_contracts.ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD
        and len(decision.entities) == 1
        and deps.question_requests_personal_evaluation(text)
        and not bool(evidence.get("creator_collection_query"))
        and not bool(evidence.get("title_anchored_version_compare_query"))
        and not bool(evidence.get("alias_title_collection_query"))
    )
    is_personal_review_collection = (
        decision.domain == "media"
        and decision.subject_scope == planner_contracts.ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD
        and (
            deps.question_requests_personal_evaluation(text)
            or bool(evidence.get("creator_collection_query"))
            or bool(evidence.get("title_anchored_personal_review_query"))
            or bool(evidence.get("alias_title_collection_query"))
        )
    )
    if decision.needs_comparison:
        return planner_contracts.ROUTER_ANSWER_SHAPE_COMPARE
    if single_entity_personal_review_detail:
        return planner_contracts.ROUTER_ANSWER_SHAPE_DETAIL_CARD
    if (
        decision.domain == "media"
        and decision.followup_mode == "inherit_entity"
        and len(decision.entities) == 1
        and deps.question_requests_personal_evaluation(text)
    ):
        return planner_contracts.ROUTER_ANSWER_SHAPE_DETAIL_CARD
    if is_personal_review_collection and (
        bool(evidence.get("creator_collection_query"))
        and (
            str((decision.ranking or {}).get("mode") or decision.sort or "relevance") != "relevance"
            or any(cue in text for cue in ("最好", "最喜欢", "评分最高", "评价最高", "哪张", "哪部", "哪本", "哪首", "分层", "层次"))
        )
    ):
        return planner_contracts.ROUTER_ANSWER_SHAPE_COMPARE
    if is_personal_review_collection and any(cue in text for cue in ("比较总结", "对比总结", "先总后分")):
        return planner_contracts.ROUTER_ANSWER_SHAPE_COMPARE
    if is_personal_review_collection and _question_requests_summary(text):
        return planner_contracts.ROUTER_ANSWER_SHAPE_SUMMARY
    if (
        decision.domain == "media"
        and deps.question_requests_personal_evaluation(text)
        and not deps.question_requests_media_details(text)
        and (
            len(decision.entities) != 1
            or bool(evidence.get("creator_collection_query"))
            or bool(evidence.get("alias_title_collection_query"))
        )
    ):
        return planner_contracts.ROUTER_ANSWER_SHAPE_LIST_ONLY
    if bool(evidence.get("alias_title_collection_query")) and decision.domain == "media":
        if is_personal_review_collection and not deps.question_requests_media_details(text):
            return planner_contracts.ROUTER_ANSWER_SHAPE_LIST_ONLY
        return planner_contracts.ROUTER_ANSWER_SHAPE_LIST_PLUS_EXPAND
    if bool(evidence.get("working_set_item_followup")) and decision.domain == "media":
        return planner_contracts.ROUTER_ANSWER_SHAPE_DETAIL_CARD
    if bool(evidence.get("working_set_followup")) and decision.domain == "media":
        if deps.question_requests_personal_evaluation(text) and not deps.question_requests_media_details(text):
            return planner_contracts.ROUTER_ANSWER_SHAPE_LIST_ONLY
        return planner_contracts.ROUTER_ANSWER_SHAPE_LIST_PLUS_EXPAND
    expand_cues = (
        "介绍", "展开", "分别说说", "详细说", "分别介绍", "说说",
        "分别是讲什么的", "讲什么的", "讲了什么", "是什么内容", "什么故事",
        "都是什么", "各自讲", "概述", "是讲什么",
    )
    explicit_collection = (
        decision.domain == "media"
        and (
            deps.is_collection_media_query(text)
            or bool(evidence.get("creator_collection_query"))
            or bool(evidence.get("working_set_followup"))
        )
    )
    if decision.intent == "mixed" or str(decision.arbitration or "").startswith("mixed_due_to"):
        if explicit_collection and decision.needs_explanation:
            return planner_contracts.ROUTER_ANSWER_SHAPE_LIST_PLUS_EXPAND
        if explicit_collection and any(cue in text for cue in expand_cues):
            return planner_contracts.ROUTER_ANSWER_SHAPE_LIST_PLUS_EXPAND
        if explicit_collection:
            return planner_contracts.ROUTER_ANSWER_SHAPE_LIST_ONLY
        return planner_contracts.ROUTER_ANSWER_SHAPE_NONE
    if decision.domain == "media" and decision.lookup_mode == "entity_lookup" and len(decision.entities) > 1:
        return planner_contracts.ROUTER_ANSWER_SHAPE_LIST_ONLY
    if decision.lookup_mode == "entity_lookup" and len(decision.entities) == 1:
        return planner_contracts.ROUTER_ANSWER_SHAPE_DETAIL_CARD
    is_collection = (
        decision.lookup_mode in ("filter_search", "general_lookup")
        and decision.domain == "media"
    ) or bool(evidence.get("creator_collection_query"))
    if is_collection and decision.needs_explanation:
        return planner_contracts.ROUTER_ANSWER_SHAPE_LIST_PLUS_EXPAND
    if is_collection and any(cue in text for cue in expand_cues):
        return planner_contracts.ROUTER_ANSWER_SHAPE_LIST_PLUS_EXPAND
    if is_collection:
        return planner_contracts.ROUTER_ANSWER_SHAPE_LIST_ONLY
    return planner_contracts.ROUTER_ANSWER_SHAPE_NONE


def derive_media_family_from_media_type(media_type: str) -> str:
    mt = str(media_type or "").lower().strip()
    if not mt:
        return planner_contracts.ROUTER_MEDIA_FAMILY_NONE
    if mt in _AUDIOVISUAL_MEDIA_TYPES:
        return planner_contracts.ROUTER_MEDIA_FAMILY_AUDIOVISUAL
    if mt in _BOOKISH_MEDIA_TYPES:
        return planner_contracts.ROUTER_MEDIA_FAMILY_BOOKISH
    if mt in _MUSIC_MEDIA_TYPES:
        return planner_contracts.ROUTER_MEDIA_FAMILY_MUSIC
    if mt in _GAME_MEDIA_TYPES:
        return planner_contracts.ROUTER_MEDIA_FAMILY_GAME
    tokens = {t.strip() for t in mt.replace(",", " ").split()}
    families = set()
    for tok in tokens:
        if tok in _AUDIOVISUAL_MEDIA_TYPES:
            families.add(planner_contracts.ROUTER_MEDIA_FAMILY_AUDIOVISUAL)
        elif tok in _BOOKISH_MEDIA_TYPES:
            families.add(planner_contracts.ROUTER_MEDIA_FAMILY_BOOKISH)
        elif tok in _MUSIC_MEDIA_TYPES:
            families.add(planner_contracts.ROUTER_MEDIA_FAMILY_MUSIC)
        elif tok in _GAME_MEDIA_TYPES:
            families.add(planner_contracts.ROUTER_MEDIA_FAMILY_GAME)
    if len(families) == 1:
        return families.pop()
    if len(families) > 1:
        return planner_contracts.ROUTER_MEDIA_FAMILY_MIXED
    return planner_contracts.ROUTER_MEDIA_FAMILY_NONE


def derive_media_family(decision: RouterDecision) -> str:
    text = str(decision.raw_question or "").strip()
    inferred_from_question = _infer_media_family_from_question(text)
    inferred_from_filters = _infer_media_family_from_filters(getattr(decision, "filters", None))
    inferred_from_selection = _infer_media_family_from_filters(getattr(decision, "selection", None))
    media_type_family = derive_media_family_from_media_type(decision.media_type)

    if (
        inferred_from_question
        and inferred_from_question != media_type_family
        and str(getattr(decision, "query_class", "") or "") == planner_contracts.ROUTER_QUERY_CLASS_PERSONAL_MEDIA_REVIEW_COLLECTION
    ):
        return inferred_from_question
    if inferred_from_question == planner_contracts.ROUTER_MEDIA_FAMILY_GAME and media_type_family in {
        planner_contracts.ROUTER_MEDIA_FAMILY_NONE,
        planner_contracts.ROUTER_MEDIA_FAMILY_AUDIOVISUAL,
    }:
        return inferred_from_question
    for candidate in (media_type_family, inferred_from_filters, inferred_from_selection, inferred_from_question):
        if candidate:
            return candidate
    evidence = dict(getattr(decision, "evidence", None) or {})
    anchor_resolution = evidence.get("router_metadata_anchor_resolution") if isinstance(evidence.get("router_metadata_anchor_resolution"), dict) else {}
    for entry in list(anchor_resolution.get("entries") or []):
        if not isinstance(entry, dict):
            continue
        family = derive_media_family_from_media_type(str(entry.get("media_type") or ""))
        if family:
            return family
    return planner_contracts.ROUTER_MEDIA_FAMILY_NONE


def apply_router_semantic_contract(
    decision: RouterDecision,
    *,
    deps: RouterSemanticDeps,
    metadata_anchor_resolution: dict[str, Any] | None = None,
) -> RouterDecision:
    decision.subject_scope = derive_subject_scope(decision, deps=deps)
    decision.time_scope_type = derive_time_scope_type(decision)
    decision.answer_shape = derive_answer_shape(decision, deps=deps)
    decision.media_family = derive_media_family(decision)
    decision.query_class = derive_query_class(decision, deps=deps)
    decision.metadata_anchors = derive_metadata_anchors(metadata_anchor_resolution)
    decision.scope_anchors = derive_scope_anchors(decision, decision.metadata_anchors)
    return decision