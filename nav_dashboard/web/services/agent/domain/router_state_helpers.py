from __future__ import annotations

import json
import re
from difflib import SequenceMatcher
from typing import Any

from ...media.media_query_adapter import merge_followup_filters, resolve_followup_strategy
from ...planner.context import build_resolved_query_state_from_decision as planner_build_resolved_query_state_from_decision
from ...tooling.tool_option_assembly import get_lookup_mode_from_state as _get_lookup_mode_from_state
from .media_core import _normalize_media_filter_map, _normalize_media_title_for_match, _sanitize_media_filters
from .media_helpers import (
    _canonicalize_media_entity,
    _extract_media_entities,
    _looks_like_generic_media_scope,
    _resolve_library_aliases,
)
from .router_query_helpers import (
    _build_media_selection,
    _has_media_title_marker,
    _has_router_media_surface,
    _has_specific_media_constraints,
    _infer_media_filters,
    _infer_requested_sort,
    _is_collection_media_query,
    _looks_like_time_only_followup,
    _parse_media_date_window,
    _question_requests_media_details,
    _question_requests_personal_evaluation,
    _resolve_query_profile,
    _router_followup_mode_label,
)

try:
    from core_service.trace_store import get_trace_record
except Exception:
    get_trace_record = None


def _derive_router_followup_resolution(question: str, previous_state: dict[str, Any]) -> Any:
    text = str(question or "").strip()
    explicit_entities = [
        entity
        for entity in _extract_media_entities(text)
        if entity and not _looks_like_generic_media_scope(entity)
    ]
    return resolve_followup_strategy(
        question=text,
        previous_has_media_context=_state_has_media_context(previous_state),
        previous_has_entity=bool(str(previous_state.get("entity") or "").strip()),
        has_referential_scope=_has_referential_media_scope(text),
        has_explicit_fresh_scope=_has_explicit_fresh_media_scope(text),
        has_explicit_entities=bool(explicit_entities),
        has_title_marker=_has_media_title_marker(text),
        looks_time_only_followup=_looks_like_time_only_followup(text),
        is_short_followup_surface=_is_short_followup_surface(text),
        wants_media_details=_question_requests_media_details(text),
        wants_personal_evaluation=_question_requests_personal_evaluation(text),
        is_collection_query=_is_collection_media_query(text),
    )


def _merge_router_filters(base: dict[str, list[str]], extra: dict[str, list[str]]) -> dict[str, list[str]]:
    merged = _normalize_media_filter_map(base)
    for field, values in _normalize_media_filter_map(extra).items():
        existing = list(merged.get(field, []))
        for value in values:
            if value not in existing:
                existing.append(value)
        if existing:
            merged[field] = existing
    return merged


def _find_previous_trace_context(history: list[dict[str, str]], require_media_context: bool = False) -> dict[str, Any]:
    if get_trace_record is None:
        return {}
    for item in reversed(history or []):
        if not isinstance(item, dict):
            continue
        if str(item.get("role", "")).strip().lower() != "assistant":
            continue
        trace_id = str(item.get("trace_id", "") or "").strip()
        if not trace_id:
            continue
        try:
            record = get_trace_record(trace_id)
        except Exception:
            record = None
        if not isinstance(record, dict) or not record:
            continue
        if require_media_context:
            state = record.get("conversation_state_after") if isinstance(record.get("conversation_state_after"), dict) else {}
            if not _state_has_media_context(state):
                continue
        return record
    return {}


def _find_previous_user_question(current_question: str, history: list[dict[str, str]]) -> str:
    current = str(current_question or "").strip()
    for item in reversed(history or []):
        if not isinstance(item, dict):
            continue
        if str(item.get("role", "")).strip().lower() != "user":
            continue
        previous = str(item.get("content", "")).strip()
        if not previous or previous == current:
            continue
        return previous
    return ""


def _find_previous_assistant_message(history: list[dict[str, str]]) -> str:
    for item in reversed(history or []):
        if not isinstance(item, dict):
            continue
        if str(item.get("role", "")).strip().lower() != "assistant":
            continue
        text = str(item.get("content", "") or item.get("text", "")).strip()
        if text:
            return text
    return ""


def _state_has_media_context(state: dict[str, Any] | None) -> bool:
    current = state if isinstance(state, dict) else {}
    media_type = str(current.get("media_type") or "").strip()
    if media_type:
        return True
    filters = current.get("filters") if isinstance(current.get("filters"), dict) else {}
    meaningful_filter_keys = {
        "media_type", "category", "genre", "author", "authors", "director", "directors", "actor", "actors", "nationality", "platform", "series", "title", "tag", "tags",
    }
    for key, value in filters.items():
        if key in meaningful_filter_keys and value:
            return True
    entities = current.get("entities") if isinstance(current.get("entities"), list) else []
    if any(str(entity or "").strip() and not _looks_like_generic_media_scope(str(entity or "")) for entity in entities):
        return True
    entity = str(current.get("entity") or "").strip()
    return bool(entity and not _looks_like_generic_media_scope(entity))


def _resolved_media_type_label(filters: dict[str, list[str]], resolved_question: str) -> str:
    normalized = _normalize_media_filter_map(filters)
    media_types = [str(value).strip().lower() for value in normalized.get("media_type", []) if str(value).strip()]
    categories = [str(value).strip() for value in normalized.get("category", []) if str(value).strip()]
    if "video" in media_types and any(category == "动画" for category in categories):
        return "anime"
    if "video" in media_types and any(category == "电影" for category in categories):
        return "movie"
    if "video" in media_types and any(category == "电视剧" for category in categories):
        return "tv"
    if media_types:
        return media_types[0]
    text = str(resolved_question or "")
    if any(token in text for token in ("游戏", "打过", "玩过", "在玩", "通关")):
        return "game"
    if any(token in text for token in ("音乐", "专辑", "歌曲", "歌单", "听过")):
        return "music"
    if any(token in text for token in ("电影", "影片", "片子")):
        return "movie"
    if any(token in text for token in ("电视剧", "剧集", "连续剧", "美剧", "日剧", "韩剧", "英剧")):
        return "tv"
    if any(token in text for token in ("动画", "动漫", "番", "番剧", "新番")):
        return "anime"
    return ""


def _resolve_router_title_alias_entities(
    query: str,
    *,
    filters: dict[str, list[str]] | None = None,
    trace_id: str = "",
    trace_stage: str = "agent.router.alias_resolve",
    max_entries: int = 8,
) -> dict[str, Any]:
    alias_resolution = _resolve_library_aliases(
        query,
        filters=filters,
        trace_id=trace_id,
        trace_stage=trace_stage,
        max_entries=max_entries,
    )
    entries = alias_resolution.get("entries") if isinstance(alias_resolution.get("entries"), list) else []
    title_entries: list[dict[str, Any]] = []
    entities: list[str] = []
    media_types: list[str] = []
    matched_terms: list[str] = []
    canonical_terms: list[str] = []
    seen_entities: set[str] = set()
    seen_entry_keys: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        if str(entry.get("field_type") or "").strip().lower() != "title":
            continue
        canonical = str(entry.get("canonical_name") or entry.get("canonical") or entry.get("raw_value") or "").strip()
        matched_text = str(entry.get("matched_text") or "").strip()
        media_type = str(entry.get("media_type") or "").strip().lower()
        entry_key = "|".join([
            str(entry.get("key") or "").strip(),
            canonical.casefold(),
            matched_text.casefold(),
            media_type,
        ])
        if not canonical or entry_key in seen_entry_keys:
            continue
        seen_entry_keys.add(entry_key)
        title_entries.append(
            {
                "key": str(entry.get("key") or "").strip(),
                "field": str(entry.get("field") or "").strip(),
                "field_type": "title",
                "media_type": media_type,
                "canonical_name": canonical,
                "matched_text": matched_text,
                "expanded_terms": [str(term).strip() for term in list(entry.get("expanded_terms") or []) if str(term).strip()],
            }
        )
        entity_key = _normalize_media_title_for_match(canonical)
        if entity_key and entity_key not in seen_entities:
            seen_entities.add(entity_key)
            entities.append(canonical)
        if media_type and media_type not in media_types:
            media_types.append(media_type)
        if matched_text and matched_text not in matched_terms:
            matched_terms.append(matched_text)
        if canonical not in canonical_terms:
            canonical_terms.append(canonical)
    return {
        "query": str(alias_resolution.get("query") or query or "").strip(),
        "entries": title_entries,
        "entities": entities,
        "media_types": media_types,
        "matched_terms": matched_terms,
        "canonical_terms": canonical_terms,
        "raw": alias_resolution,
    }


def _infer_prior_question_state(question: str, history: list[dict[str, str]] | None = None) -> dict[str, Any]:
    text = str(question or "").strip()
    if not text:
        return {}
    query_profile = _resolve_query_profile(text)
    from . import router_service as _router_service

    decision, llm_media, _ = _router_service._build_router_decision(text, [], {}, query_profile)
    query_classification = _router_service._router_decision_to_query_classification(decision, llm_media, {}, query_profile)
    resolved_state = planner_build_resolved_query_state_from_decision(
        decision,
        normalize_media_filter_map=_normalize_media_filter_map,
        router_followup_mode_label=_router_followup_mode_label,
    )
    snapshot = _build_conversation_state_snapshot(text, query_classification=query_classification, resolved_query_state=resolved_state)
    if _state_has_media_context(snapshot):
        return snapshot
    assistant_text = _find_previous_assistant_message(history or [])
    assistant_entities = [str(item).strip() for item in _extract_media_entities(assistant_text) if str(item).strip()]
    if _is_collection_media_query(text) or len(assistant_entities) >= 2:
        snapshot["lookup_mode"] = "filter_search"
        snapshot["media_type"] = str(snapshot.get("media_type") or "video")
        snapshot["filters"] = _merge_router_filters(
            _normalize_media_filter_map(snapshot.get("filters")),
            {"series": [text]},
        )
        snapshot["selection"] = _build_media_selection(snapshot.get("filters") or {}, str(snapshot.get("media_type") or ""))
        snapshot["ranking"] = {"mode": str(snapshot.get("sort") or "relevance"), "source": "inferred"}
        snapshot["entity"] = ""
        snapshot["entities"] = []
    elif len(assistant_entities) == 1:
        snapshot["lookup_mode"] = "entity_lookup"
        snapshot["entity"] = assistant_entities[0]
        snapshot["entities"] = [assistant_entities[0]]
    return snapshot


def _build_conversation_state_snapshot(
    question: str,
    query_classification: dict[str, Any] | None = None,
    resolved_query_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    text = str(question or "").strip()
    current = query_classification if isinstance(query_classification, dict) else {}
    state = dict(resolved_query_state) if isinstance(resolved_query_state, dict) else {}
    entities = [
        str(item).strip()
        for item in (current.get("media_entities") or [])
        if str(item).strip()
    ]
    if not entities and _state_has_media_context(state):
        entities = _extract_media_entities(text)
    return {
        "question": text,
        "lookup_mode": _get_lookup_mode_from_state(state),
        "selection": _normalize_media_filter_map(state.get("selection")),
        "time_constraint": dict(state.get("time_constraint") or {}),
        "ranking": dict(state.get("ranking") or {}),
        "media_type": str(state.get("media_type", "") or ""),
        "entity": entities[0] if entities else "",
        "entities": entities,
        "filters": _normalize_media_filter_map(state.get("filters")),
        "date_range": list(state.get("date_range") or []),
        "sort": str(state.get("sort", "") or ""),
    }


def _has_state_value(value: Any) -> bool:
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return value is not None


def _state_value_signature(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except TypeError:
        return repr(value)


def _describe_inheritance_transition(before_value: Any, after_value: Any, detected_followup: bool) -> str:
    if not detected_followup:
        return "not_applicable"
    before_present = _has_state_value(before_value)
    after_present = _has_state_value(after_value)
    if not before_present and not after_present:
        return "unchanged_empty"
    if _state_value_signature(before_value) == _state_value_signature(after_value):
        return "carried_over" if before_present else "unchanged_empty"
    if not before_present and after_present:
        return "introduced"
    if before_present and not after_present:
        return "cleared"
    return "overridden"


def _build_state_diff(before_state: dict[str, Any], after_state: dict[str, Any]) -> dict[str, Any]:
    diff: dict[str, Any] = {}
    for field in ("lookup_mode", "selection", "time_constraint", "ranking", "media_type", "entity", "filters", "date_range", "sort"):
        if _state_value_signature(before_state.get(field)) != _state_value_signature(after_state.get(field)):
            diff[field] = after_state.get(field)
    return diff


def _is_short_followup_surface(question: str) -> bool:
    text = str(question or "").strip()
    if not text:
        return False
    compact = re.sub(r"\s+", "", text)
    pronoun_cues = (
        "那个", "这个", "那部", "这部", "那本", "这本", "那张", "这张", "这两", "那两", "这两个", "那两个", "这两部", "这两本", "这两条", "这两张", "它", "它们", "前者", "后者",
    )
    followup_verbs = ("怎么样", "如何", "呢", "简介", "介绍", "细节", "评价", "评分", "几分", "打几分", "展开", "详细", "讲了什么", "分别介绍", "分别讲讲")
    if len(compact) <= 14 and any(cue in compact for cue in pronoun_cues) and any(cue in compact for cue in followup_verbs):
        return True
    return compact in {"那个呢", "这个呢", "那个怎么样", "这个怎么样", "简介呢", "详细呢", "评价呢", "评分呢"}


def _has_referential_media_scope(question: str) -> bool:
    text = re.sub(r"\s+", "", str(question or "").strip())
    if not text:
        return False
    direct_cues = (
        "这些", "那些", "这几", "那几", "这两", "那两", "这两个", "那两个", "这两部", "这两本", "这两条", "这两张", "那两部", "那两本", "那两条", "那两张", "前面这些", "上面这些", "刚才这些", "上一轮这些", "上一条这些", "它们",
    )
    if any(cue in text for cue in direct_cues):
        return True
    return bool(re.search(r"(?:这|那)(?:[一二两三四五六几]|\d{1,2})(?:个|部|本|条|张)(?:专辑|作品|条目|内容|电影|影片|剧集|书|小说|音乐|媒体)?", text))


def _has_explicit_fresh_media_scope(question: str) -> bool:
    text = str(question or "").strip()
    if not text:
        return False
    if _is_short_followup_surface(text):
        return False
    explicit_entities = [entity for entity in _extract_media_entities(text) if entity and not _looks_like_generic_media_scope(entity)]
    if explicit_entities or _has_media_title_marker(text):
        return True
    inferred_filters = _infer_media_filters(text)
    explicit_filter_keys = {
        "media_type", "category", "genre", "nationality", "series", "platform", "author", "authors", "director", "directors", "actor", "actors", "tag", "tags", "year",
    }
    has_explicit_filters = any(inferred_filters.get(key) for key in explicit_filter_keys)
    has_explicit_date = bool(_parse_media_date_window(text))
    has_personal_scope = any(cue in text for cue in ("我看过", "我看了", "我追过", "我补过", "我读过", "我读了", "我听过", "我玩过", "我打过", "我记录过"))
    if _has_referential_media_scope(text):
        return bool(has_personal_scope or has_explicit_date)
    if has_personal_scope and (has_explicit_filters or has_explicit_date or _is_collection_media_query(text)):
        return True
    if has_explicit_filters and len(re.sub(r"\s+", "", text)) >= 8:
        return True
    if has_explicit_date and _is_collection_media_query(text):
        return True
    if any(cue in text for cue in ("这些", "这几", "上面", "上一条", "前面", "刚才", "刚刚")):
        return False
    return False


def _is_context_dependent_followup(question: str) -> bool:
    text = str(question or "").strip()
    if not text:
        return False
    if _has_explicit_fresh_media_scope(text):
        return False
    if not _looks_like_time_only_followup(text) and (_extract_media_entities(text) or _has_media_title_marker(text)):
        return False
    reference_scope_cues = (
        "这些", "这些媒体", "这些作品", "这些条目", "这两", "这两个", "这两部", "这两本", "这两条", "这两张", "这几部", "这几本", "这几条", "这几张", "这些番", "它们", "前面这些", "上面这些", "刚才这些",
    )
    detail_cues = (
        "具体细节", "细节信息", "详细信息", "详细资料", "作者", "出版方", "出版社", "发行方", "发行商", "渠道", "平台", "工作室", "厂牌", "制作公司",
    )
    has_reference_scope = any(cue in text for cue in reference_scope_cues)
    has_detail_request = any(cue in text for cue in detail_cues)
    if len(text) > 20 and not text.endswith("呢") and not (has_reference_scope or has_detail_request):
        return False
    return any(cue in text for cue in ("呢", "简介", "剧情", "介绍", "讲了什么", "评价", "评分", *reference_scope_cues, *detail_cues, "导演", "演员", "结局", "时间", "什么时候", "优缺点", "区别", "差异", "为什么", "展开", "详细"))


def _get_previous_media_working_set(previous_trace: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(previous_trace, dict):
        return {}
    working_set = previous_trace.get("working_set") if isinstance(previous_trace.get("working_set"), dict) else {}
    items = working_set.get("items") if isinstance(working_set.get("items"), list) else []
    if str(working_set.get("kind") or "").strip() != "media" or not items:
        return {}
    return {**working_set, "items": [dict(item) for item in items if isinstance(item, dict)]}


def _extract_working_set_rank_reference(question: str, working_set: dict[str, Any]) -> dict[str, Any]:
    items = working_set.get("items") if isinstance(working_set.get("items"), list) else []
    if not items:
        return {}
    text = str(question or "").strip()
    if not text:
        return {}
    rank_map = {"一": 1, "二": 2, "两": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9, "十": 10}
    match = re.search(r"第\s*([一二两三四五六七八九十\d]+)\s*(?:本|部|条|个)", text)
    if not match:
        return {}
    token = str(match.group(1) or "").strip()
    rank = int(token) if token.isdigit() else rank_map.get(token, 0)
    if rank <= 0:
        return {}
    matched_item = next((dict(item) for item in items if int(item.get("rank") or 0) == rank), {})
    if not matched_item:
        return {}
    return {"item": matched_item, "matched_title": str(matched_item.get("title") or "").strip(), "query_phrase": match.group(0), "match_score": 1.0, "match_type": "ordinal_reference"}


def _extract_working_set_title_candidates(question: str) -> list[str]:
    text = str(question or "").strip()
    if not text:
        return []
    candidates: list[str] = []
    for entity in _extract_media_entities(text):
        clean = str(entity).strip()
        if clean:
            candidates.append(clean)
    stripped = text
    leading_patterns = [
        r"^(?:请)?(?:再)?(?:帮我)?(?:介绍一下|介绍下|介绍|讲讲|讲下|说说|展开讲讲|展开说说|详细讲讲|详细说说|具体讲讲|聊聊|展开介绍一下)",
        r"^(?:麻烦)?(?:再)?(?:帮我)?(?:讲一下|说一下|分析一下)",
    ]
    for pattern in leading_patterns:
        stripped = re.sub(pattern, "", stripped).strip(" ，,。！？?；;：:")
    stripped = re.sub(r"(?:一下|一下子)$", "", stripped).strip(" ，,。！？?；;：:")
    trailing_patterns = [r"(?:这|那)?(?:本|部|条|个)(?:书|片|作品|内容|东西)?$", r"(?:这|那)?(?:本|部|条|个)$", r"(?:书|片|作品|内容)$"]
    for pattern in trailing_patterns:
        stripped = re.sub(pattern, "", stripped).strip(" ，,。！？?；;：:")
    if stripped:
        candidates.append(stripped)
    for item in re.findall(r"《([^》]+)》", text):
        clean = str(item).strip()
        if clean:
            candidates.append(clean)
    deduped: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        clean = str(candidate).strip(" ，,。！？?；;：:'\"“”‘’（）()")
        key = _normalize_media_title_for_match(clean)
        if not clean or len(key) < 2 or key in seen or _looks_like_generic_media_scope(clean):
            continue
        seen.add(key)
        deduped.append(clean)
    return deduped


def _working_set_title_similarity(query_title: str, item_title: str) -> float:
    query_key = _normalize_media_title_for_match(query_title)
    item_key = _normalize_media_title_for_match(item_title)
    if not query_key or not item_key:
        return 0.0
    if query_key == item_key:
        return 1.0
    if query_key in item_key or item_key in query_key:
        shorter = min(len(query_key), len(item_key))
        longer = max(len(query_key), len(item_key))
        if shorter >= 2:
            return min(0.98, 0.84 + (shorter / max(1, longer)) * 0.1)
    ratio = SequenceMatcher(None, query_key, item_key).ratio()
    shared_chars = len(set(query_key) & set(item_key))
    if len(query_key) == len(item_key) and shared_chars >= max(2, len(item_key) - 1):
        ratio = max(ratio, 0.82)
    return ratio


def _working_set_item_match_score(query_title: str, item: dict[str, Any]) -> tuple[float, str]:
    title = str(item.get("title") or "").strip()
    best_score = _working_set_title_similarity(query_title, title)
    best_source = "title_exact" if best_score >= 0.999 else "title_fuzzy"
    for term in item.get("match_terms") if isinstance(item.get("match_terms"), list) else []:
        clean_term = str(term).strip()
        if not clean_term:
            continue
        term_score = _working_set_title_similarity(query_title, clean_term)
        normalized_query = _normalize_media_title_for_match(query_title)
        normalized_term = _normalize_media_title_for_match(clean_term)
        if normalized_query and normalized_query == normalized_term:
            term_score = max(term_score, 0.97)
        if term_score > best_score:
            best_score = term_score
            best_source = "working_set_match_term"
    return best_score, best_source


def _resolve_previous_working_set_item_followup(question: str, previous_state: dict[str, Any]) -> dict[str, Any]:
    working_set = previous_state.get("working_set") if isinstance(previous_state.get("working_set"), dict) else {}
    items = working_set.get("items") if isinstance(working_set.get("items"), list) else []
    text = str(question or "").strip()
    if not items or not text or _has_explicit_fresh_media_scope(text):
        return {}
    if not (_question_requests_media_details(text) or _has_referential_media_scope(text) or _is_context_dependent_followup(text)):
        return {}
    ordinal_match = _extract_working_set_rank_reference(text, working_set)
    if ordinal_match:
        return ordinal_match
    best_match: dict[str, Any] = {}
    best_score = 0.0
    second_score = 0.0
    for candidate in _extract_working_set_title_candidates(text):
        for item in items:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or "").strip()
            if not title:
                continue
            score, match_source = _working_set_item_match_score(candidate, item)
            if score > best_score:
                second_score = best_score
                best_score = score
                best_match = {"item": dict(item), "matched_title": title, "query_phrase": candidate, "match_score": round(score, 4), "match_type": match_source}
            elif score > second_score:
                second_score = score
    if best_score >= 0.78 and best_score - second_score >= 0.04:
        return best_match
    return {}


def _should_reuse_previous_working_set(question: str, previous_state: dict[str, Any]) -> bool:
    working_set = previous_state.get("working_set") if isinstance(previous_state.get("working_set"), dict) else {}
    items = working_set.get("items") if isinstance(working_set.get("items"), list) else []
    if not items:
        return False
    if _resolve_previous_working_set_item_followup(question, previous_state):
        return False
    text = str(question or "").strip()
    if not text or _has_explicit_fresh_media_scope(text):
        return False
    if _extract_media_entities(text) or _has_media_title_marker(text):
        return False
    if not (_is_context_dependent_followup(text) or _has_referential_media_scope(text)):
        return False
    followup_action_cues = (
        "进一步介绍", "进一步讲", "分别是讲什么的", "分别讲什么", "分别介绍", "分别介绍一下", "分别讲讲", "各自介绍", "各自讲讲", "展开说说", "具体讲讲", "详细讲讲", "讲什么", "讲了什么", "是什么内容", "内容", "简介", "剧情", "细节", "详细", "展开", "分别", "选几", "挑几",
    )
    has_referential_surface = _has_referential_media_scope(text)
    return has_referential_surface or any(cue in text for cue in followup_action_cues) or _question_requests_media_details(text)


def _get_previous_assistant_answer_summary(history: list[dict[str, str]]) -> str:
    for item in reversed(history or []):
        if not isinstance(item, dict):
            continue
        if str(item.get("role", "")).strip().lower() != "assistant":
            continue
        content = str(item.get("content", "") or "").strip()
        if content:
            return content[:280]
    return ""


def _media_scope_label(media_type: str, filters: dict[str, list[str]]) -> str:
    normalized_media_type = str(media_type or "").strip().lower()
    categories = [str(value).strip() for value in filters.get("category", []) if str(value).strip()]
    if normalized_media_type == "game":
        return "游戏"
    if normalized_media_type == "book":
        return ((" / ".join(categories) + "图书") if categories else "图书")
    if normalized_media_type == "music":
        return "音乐"
    if normalized_media_type == "anime":
        return "动画"
    if normalized_media_type == "movie":
        return "电影"
    if normalized_media_type in {"tv", "series"}:
        return "剧集"
    if normalized_media_type == "video":
        return " / ".join(categories) if categories else "视频"
    if categories:
        return " / ".join(categories)
    return "媒体条目"


def _build_media_followup_rewrite_queries(
    question: str,
    previous_state: dict[str, Any],
    *,
    followup_mode: str,
    entities: list[str],
    filters: dict[str, list[str]],
    media_type: str,
) -> dict[str, str]:
    raw_question = str(question or "").strip()
    if not raw_question:
        return {}
    entity_list = [str(item).strip() for item in entities if str(item).strip()]
    inherited_entity = str(previous_state.get("entity") or "").strip() if followup_mode == "inherit_entity" else ""
    entity = entity_list[0] if entity_list else inherited_entity
    entity_phrase = "、".join(entity_list[:4]) if entity_list else entity
    effective_filters = merge_followup_filters(_normalize_media_filter_map(previous_state.get("filters")), filters, strategy="carry")
    scope_label = _media_scope_label(media_type or str(previous_state.get("media_type") or ""), effective_filters)
    followup_target = entity_phrase or scope_label
    wants_review = _question_requests_personal_evaluation(raw_question)
    wants_detail = _question_requests_media_details(raw_question)
    ranking_mode = _infer_requested_sort(raw_question)
    media_query = ""
    if entity_phrase and (wants_review or wants_detail or followup_mode == "inherit_entity"):
        if wants_review and wants_detail:
            media_query = f"{entity_phrase} 个人评分、短评和条目细节"
        elif wants_review:
            media_query = f"{entity_phrase} 个人评分与短评"
        elif wants_detail:
            media_query = f"{entity_phrase} 条目细节"
        elif _is_short_followup_surface(raw_question) or any(cue in raw_question for cue in ("怎么样", "如何")):
            media_query = f"{entity_phrase} 条目细节与个人评价"
        else:
            media_query = f"{entity_phrase} {raw_question}".strip()
    elif followup_mode in {"inherit_filters", "inherit_timerange"} and _state_has_media_context(previous_state):
        if wants_review and followup_target:
            media_query = f"{followup_target} 个人评分与短评"
        elif wants_detail and followup_target:
            media_query = f"{followup_target} 条目细节"
        elif ranking_mode != "relevance" and followup_target:
            media_query = f"{followup_target} 按{ranking_mode}排序"
        elif followup_target:
            media_query = f"{followup_target} {raw_question}".strip()
    return {
        "media_query": media_query.strip(),
        "doc_query": raw_question,
        "tmdb_query": entity_phrase or raw_question,
        "web_query": entity_phrase or raw_question,
    }


def _normalize_search_mode(search_mode: str) -> str:
    from ...planner.planner_contracts import normalize_search_mode as normalize_search_mode_contract

    return normalize_search_mode_contract(search_mode)


def _strip_unsupported_creator_filters_for_fresh_title_scope(
    question: str,
    filters: dict[str, list[str]],
    *,
    title_entities: list[str],
) -> dict[str, list[str]]:
    text = str(question or "").strip()
    if not text or not filters or not title_entities or not _has_explicit_fresh_media_scope(text):
        return filters
    sanitized = _normalize_media_filter_map(filters)
    if not sanitized:
        return filters
    lowered_question = text.casefold()
    for field in ("author", "authors", "director", "directors", "actor", "actors"):
        values = [str(value).strip() for value in sanitized.get(field, []) if str(value).strip()]
        if not values:
            continue
        supported_values = [value for value in values if value.casefold() in lowered_question]
        if supported_values:
            sanitized[field] = supported_values
        else:
            sanitized.pop(field, None)
    return _sanitize_media_filters(sanitized)


def _strip_title_alias_self_filters(
    filters: dict[str, list[str]],
    *,
    title_terms: list[str],
) -> dict[str, list[str]]:
    if not filters or not title_terms:
        return filters
    sanitized = _normalize_media_filter_map(filters)
    if not sanitized:
        return filters
    normalized_titles = {_normalize_media_title_for_match(str(term).strip()) for term in title_terms if str(term).strip()}
    normalized_titles.discard("")
    if not normalized_titles:
        return filters
    for field in ("author", "authors", "director", "directors", "actor", "actors"):
        values = [str(value).strip() for value in sanitized.get(field, []) if str(value).strip()]
        if not values:
            continue
        kept_values = [value for value in values if _normalize_media_title_for_match(value) not in normalized_titles]
        if kept_values:
            sanitized[field] = kept_values
        else:
            sanitized.pop(field, None)
    return _sanitize_media_filters(sanitized)
