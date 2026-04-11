from __future__ import annotations

import re
from datetime import date, timedelta
from typing import Any, Literal

from . import router_constants as constants
from .media_core import _normalize_media_filter_map, _sanitize_media_filters
from .media_helpers import _extract_creator_from_collection_query, _is_creator_collection_media_query
from ...media.media_taxonomy import TMDB_AUDIOVISUAL_CUES


def _estimate_query_tokens(text: str) -> int:
    raw = str(text or "").strip()
    if not raw:
        return 0
    latin_parts = re.findall(r"[A-Za-z0-9_]+", raw)
    cjk_chars = re.findall(r"[\u4e00-\u9fff]", raw)
    return max(1, len(latin_parts) + len(cjk_chars))


def _approx_tokens(text: str) -> int:
    if not text:
        return 0
    cjk = re.findall(r"[\u4e00-\u9fff]", text)
    words = re.findall(r"[A-Za-z0-9_]+", text)
    return len(cjk) + len(words) + max(0, len(text) // 20)


def _resolve_query_profile(question: str, *, base_threshold: float = 0.35, base_vector_top_n: int = constants.DOC_VECTOR_TOP_N, base_top_k: int = 5) -> dict[str, Any]:
    token_count = _estimate_query_tokens(question)
    profile = "medium"
    threshold = float(base_threshold)
    vector_top_n = int(base_vector_top_n)
    top_k = int(base_top_k)

    if token_count <= constants.SHORT_QUERY_MAX_TOKENS:
        profile = "short"
        threshold = max(0.0, threshold + constants.SHORT_QUERY_THRESHOLD_DELTA)
        vector_top_n = max(4, vector_top_n + constants.SHORT_QUERY_VECTOR_TOP_N_DELTA)
        top_k = max(1, top_k + constants.SHORT_QUERY_TOP_K_DELTA)
    elif token_count >= constants.LONG_QUERY_MIN_TOKENS:
        profile = "long"
        threshold = min(0.98, threshold + constants.LONG_QUERY_THRESHOLD_DELTA)
        vector_top_n = max(4, vector_top_n + constants.LONG_QUERY_VECTOR_TOP_N_DELTA)
        top_k = max(1, top_k + constants.LONG_QUERY_TOP_K_DELTA)

    return {
        "profile": profile,
        "token_count": token_count,
        "similarity_threshold": round(float(threshold), 6),
        "doc_vector_top_n": int(max(1, vector_top_n)),
        "top_k": int(max(1, top_k)),
    }


def _normalize_query_type(value: str) -> str:
    raw = str(value or "").strip().upper()
    if raw in {constants.QUERY_TYPE_TECH, constants.QUERY_TYPE_MEDIA, constants.QUERY_TYPE_MIXED, constants.QUERY_TYPE_GENERAL}:
        return raw
    return constants.QUERY_TYPE_GENERAL


def _parse_classifier_label(value: str) -> str:
    text = str(value or "").strip().upper()
    if text.startswith(constants.CLASSIFIER_LABEL_MEDIA):
        return constants.CLASSIFIER_LABEL_MEDIA
    if text.startswith(constants.CLASSIFIER_LABEL_TECH):
        return constants.CLASSIFIER_LABEL_TECH
    if text.startswith(constants.CLASSIFIER_LABEL_OTHER):
        return constants.CLASSIFIER_LABEL_OTHER
    if text in {"ANIME", "MOVIE", "FILM", "BOOK", "GAME", "MANGA", "NOVEL"}:
        return constants.CLASSIFIER_LABEL_MEDIA
    if constants.CLASSIFIER_LABEL_MEDIA in text:
        return constants.CLASSIFIER_LABEL_MEDIA
    if constants.CLASSIFIER_LABEL_TECH in text:
        return constants.CLASSIFIER_LABEL_TECH
    return constants.CLASSIFIER_LABEL_OTHER


def _map_router_query_type(decision: Any) -> str:
    intent = str(getattr(decision, "intent", "") or "").strip().lower()
    domain = str(getattr(decision, "domain", "") or "").strip().lower()
    if intent == "mixed":
        return constants.QUERY_TYPE_MIXED
    if domain == "media":
        return constants.QUERY_TYPE_MEDIA
    if domain == "tech":
        return constants.QUERY_TYPE_TECH
    return constants.QUERY_TYPE_GENERAL


def _classifier_token_count(query: str) -> int:
    text = str(query or "").strip().lower()
    if not text:
        return 0
    return len(re.findall(r"[a-z0-9_]+|[\u4e00-\u9fff]{2,}", text))


def _has_media_title_marker(query: str) -> bool:
    return bool(constants.MEDIA_TITLE_MARKER_RE.search(str(query or "")))


def _has_media_intent_cues(query: str) -> bool:
    text = str(query or "").strip().lower()
    if not text:
        return False
    return any(keyword in text for keyword in constants.MEDIA_INTENT_KEYWORDS)


def _strip_query_scaffolding(query: str) -> str:
    text = str(query or "").strip()
    if not text:
        return ""
    for pattern in constants.MEDIAWIKI_FILLER_PATTERNS:
        text = re.sub(pattern, " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text)
    return text.strip(" ，,。！？?；;：:")


def _is_abstract_media_concept_query(query: str, classification: dict[str, Any] | None = None) -> bool:
    text = _strip_query_scaffolding(query)
    if not text:
        return False
    current = classification or {}
    current_label = _parse_classifier_label(current.get("label") if isinstance(current, dict) else "")
    current_domain = str(current.get("domain") or "").strip().lower() if isinstance(current, dict) else ""
    tech_disambiguation_cues = (
        "技术",
        "区块链",
        "协议",
        "框架",
        "编程",
        "工程",
        "数据库",
        "操作系统",
        "编译器",
        "网络安全",
        "芯片",
    )
    if bool(current.get("media_entity_confident")):
        return False
    if current_label == constants.CLASSIFIER_LABEL_TECH or current_domain == "tech":
        return False
    concrete_region_hit = any(alias in text and len(nationalities) == 1 for alias, nationalities in constants.MEDIA_REGION_ALIASES.items())
    if concrete_region_hit and not any(cue in text for cue in ("拉美", "拉丁美洲", "流派", "主义", "风格", "佳作", "冷门", "女性主义", "离散叙事", "魔幻现实主义", "后现代主义", "新浪潮")):
        return False
    if any(cue in text.lower() for cue in constants.ROUTER_TECH_CUES) or any(cue in text for cue in tech_disambiguation_cues):
        return False
    if not (_has_media_intent_cues(text) or any(cue in text for cue in constants.MEDIA_ABSTRACT_CONCEPT_CUES)):
        return False
    return any(cue in text for cue in constants.MEDIA_ABSTRACT_CONCEPT_CUES if cue not in {"小说", "文学", "诗歌", "诗集", "散文", "作家"}) or any(
        cue in text for cue in ("拉美", "拉丁美洲", "流派", "主义", "风格", "佳作", "冷门", "女性主义", "离散叙事", "魔幻现实主义", "后现代主义", "新浪潮")
    )


def _extract_year_from_date_range(date_range: Any) -> str:
    if isinstance(date_range, list) and len(date_range) == 2:
        start = str(date_range[0] or "").strip()
        if len(start) >= 4 and start[:4].isdigit():
            return start[:4]
    return ""


def _extract_media_time_hint(query: str) -> dict[str, Any]:
    text = str(query or "").strip()
    if not text:
        return {}
    half_match = re.search(r"(?:(?P<year>20\d{2})\s*年?)?\s*(?P<half>上半年|下半年)", text)
    if half_match:
        half = str(half_match.group("half") or "").strip()
        return {"raw": str(half_match.group(0) or "").strip(), "year": str(half_match.group("year") or "").strip(), "explicit_year": bool(half_match.group("year")), "start_month": 1 if half == "上半年" else 7, "end_month": 6 if half == "上半年" else 12, "label": half}
    range_match = re.search(r"(?:(?P<year>20\d{2})\s*年?)?\s*(?P<start>\d{1,2})(?:月)?\s*(?:到|至|[-~—－])\s*(?P<end>\d{1,2})月?", text)
    if range_match:
        start_month = max(1, min(12, int(range_match.group("start"))))
        end_month = max(1, min(12, int(range_match.group("end"))))
        if start_month > end_month:
            start_month, end_month = end_month, start_month
        return {"raw": str(range_match.group(0) or "").strip(), "year": str(range_match.group("year") or "").strip(), "explicit_year": bool(range_match.group("year")), "start_month": start_month, "end_month": end_month, "label": f"{start_month}-{end_month}月"}
    month_match = re.search(r"(?:(?P<year>20\d{2})\s*年?)?\s*(?P<month>\d{1,2})月", text)
    if month_match:
        month = max(1, min(12, int(month_match.group("month"))))
        return {"raw": str(month_match.group(0) or "").strip(), "year": str(month_match.group("year") or "").strip(), "explicit_year": bool(month_match.group("year")), "start_month": month, "end_month": month, "label": f"{month}月"}
    return {}


def _build_media_time_hint_text(query: str, fallback_year: str = "") -> str:
    hint = _extract_media_time_hint(query)
    if not hint:
        return ""
    year = str(hint.get("year") or fallback_year or "").strip()
    start_month = int(hint.get("start_month") or 0)
    end_month = int(hint.get("end_month") or 0)
    label = str(hint.get("label") or "").strip()
    if label in {"上半年", "下半年"}:
        return f"{year}年{label}" if year else label
    if start_month and end_month and start_month == end_month:
        return f"{year}年{start_month}月" if year else f"{start_month}月"
    if start_month and end_month:
        return f"{year}年{start_month}-{end_month}月" if year else f"{start_month}-{end_month}月"
    return f"{year}年{label}" if year and label else label


def _month_last_day(year: int, month: int) -> int:
    if month == 2:
        leap = year % 400 == 0 or (year % 4 == 0 and year % 100 != 0)
        return 29 if leap else 28
    if month in {4, 6, 9, 11}:
        return 30
    return 31


def _parse_media_date_window(query: str, fallback_year: str = "") -> dict[str, str]:
    text = str(query or "").strip()
    if not text:
        return {}
    today = date.today()
    compact = re.sub(r"\s+", "", text)
    if any(token in compact for token in ("过去半年", "近半年", "最近半年", "半年内", "近6个月", "最近6个月", "过去6个月", "6个月内")):
        start = today - timedelta(days=183)
        return {"start": start.isoformat(), "end": today.isoformat(), "kind": "past_6_months", "label": "最近半年"}
    if any(token in compact for token in ("过去一年", "近一年", "最近一年", "这一年")):
        start = today - timedelta(days=365)
        return {"start": start.isoformat(), "end": today.isoformat(), "kind": "past_1_year", "label": "最近一年"}
    if "过去两年" in compact or "近两年" in compact:
        start = today - timedelta(days=365 * 2)
        return {"start": start.isoformat(), "end": today.isoformat(), "kind": "past_2_years", "label": "最近两年"}
    if "去年" in compact:
        year = today.year - 1
        return {"start": f"{year:04d}-01-01", "end": f"{year:04d}-12-31", "kind": "calendar_year", "label": "去年"}
    if "今年" in compact:
        year = today.year
        return {"start": f"{year:04d}-01-01", "end": today.isoformat(), "kind": "calendar_year_to_date", "label": "今年"}
    hint = _extract_media_time_hint(text)
    if not hint:
        return {}
    year_text = str(hint.get("year") or fallback_year or "").strip()
    if not year_text.isdigit():
        return {}
    year = int(year_text)
    start_month = max(1, min(12, int(hint.get("start_month") or 0)))
    end_month = max(1, min(12, int(hint.get("end_month") or 0)))
    if not start_month or not end_month:
        return {}
    return {"start": f"{year:04d}-{start_month:02d}-01", "end": f"{year:04d}-{end_month:02d}-{_month_last_day(year, end_month):02d}", "kind": "explicit_range", "label": _build_media_time_hint_text(text, str(year))}


def _looks_like_time_only_followup(query: str) -> bool:
    text = str(query or "").strip().strip("？?。！!，,；;：:")
    if not text:
        return False
    compact = re.sub(r"\s+", "", text)
    if re.fullmatch(r"(?:20\d{2}年?)?(?:上半年|下半年)(?:看了哪些)?(?:的)?(?:番|番剧|动画|动漫|新番)?(?:呢)?", compact):
        return True
    if re.fullmatch(r"(?:20\d{2}年?)?\d{1,2}(?:月)?(?:到|至|[-~—－])\d{1,2}月(?:看了哪些)?(?:的)?(?:番|番剧|动画|动漫|新番)?(?:呢)?", compact):
        return True
    return bool(re.fullmatch(r"(?:20\d{2}年?)?\d{1,2}月(?:看了哪些)?(?:的)?(?:番|番剧|动画|动漫|新番)?(?:呢)?", compact))


def _replace_time_window_in_query(previous_query: str, current_query: str) -> str:
    previous = str(previous_query or "").strip()
    current = str(current_query or "").strip().strip("？?。！!，,；;：:")
    if not previous or not current:
        return current or previous
    previous_year = _extract_year_from_date_range(_parse_media_date_window(previous))
    current_time_text = _build_media_time_hint_text(current, previous_year)
    replacement = current_time_text or current.replace("呢", "")
    replaced = re.sub(r"20\d{2}年\s*\d{1,2}(?:月)?\s*(?:到|至|[-~—－])\s*\d{1,2}月|20\d{2}年\s*\d{1,2}月|20\d{2}年\s*(?:上半年|下半年)", replacement, previous, count=1)
    if replaced != previous:
        return replaced
    return f"{replacement} {previous}".strip()


def _date_window_from_state(state: dict[str, Any] | None) -> dict[str, str]:
    payload = state if isinstance(state, dict) else {}
    date_range = list(payload.get("date_range") or [])
    if len(date_range) >= 2:
        start = str(date_range[0] or "").strip()
        end = str(date_range[1] or "").strip()
        if start and end:
            return {"start": start, "end": end}
    time_constraint = payload.get("time_constraint") if isinstance(payload.get("time_constraint"), dict) else {}
    start = str(time_constraint.get("start") or "").strip()
    end = str(time_constraint.get("end") or "").strip()
    if start and end:
        return {"start": start, "end": end}
    return {}


def _build_media_selection(filters: dict[str, list[str]], media_type: str = "") -> dict[str, list[str]]:
    selection = _normalize_media_filter_map(filters)
    resolved_media_type = str(media_type or "").strip().lower()
    if resolved_media_type:
        selection["media_type"] = [resolved_media_type]
    return selection


def _build_time_constraint(date_window: dict[str, Any] | None, *, source: str = "none") -> dict[str, Any]:
    window = date_window if isinstance(date_window, dict) else {}
    start = str(window.get("start") or "").strip()
    end = str(window.get("end") or "").strip()
    if not start or not end:
        return {}
    payload = {
        "kind": str(window.get("kind") or "explicit_range").strip() or "explicit_range",
        "start": start,
        "end": end,
        "source": str(source or window.get("source") or "explicit").strip() or "explicit",
    }
    label = str(window.get("label") or "").strip()
    if label:
        payload["label"] = label
    return payload


def _infer_requested_sort(text: str) -> str:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return "relevance"
    low_cues = ("最低", "最差", "评分低", "评价最低", "评分最低", "最低分", "lowest", "lowest-rated", "worst")
    high_cues = ("最高", "最好", "评分高", "评价比较好的", "评分最高", "最高分", "best", "highest", "highest-rated")
    newest_cues = (
        "最新",
        "最近",
        "近半年",
        "最近半年",
        "过去半年",
        "半年内",
        "近6个月",
        "最近6个月",
        "过去6个月",
        "最近一年",
        "近一年",
        "过去一年",
        "今年",
        "去年",
        "上半年",
        "下半年",
    )
    oldest_cues = ("最早", "最先", "最旧", "较早")
    if any(cue in lowered for cue in low_cues):
        return "rating_asc"
    if any(cue in lowered for cue in high_cues):
        return "rating_desc"
    if any(cue in lowered for cue in oldest_cues):
        return "date_asc"
    if any(cue in lowered for cue in newest_cues):
        return "date_desc"
    return "relevance"


def _build_media_ranking(query: str, lookup_mode: str, time_constraint: dict[str, Any]) -> dict[str, Any]:
    requested = _infer_requested_sort(query)
    if requested != "relevance":
        return {"mode": requested, "source": "explicit"}
    if lookup_mode == "filter_search" and time_constraint:
        return {"mode": "date_desc", "source": "time_constraint_default"}
    return {"mode": "relevance", "source": "default"}


def _has_specific_media_constraints(filters: dict[str, list[str]]) -> bool:
    normalized = _normalize_media_filter_map(filters)
    for field, values in normalized.items():
        if values and field != "media_type":
            return True
    return False


def _is_title_anchored_personal_review_query(
    question: str,
    *,
    entities: list[str],
    alias_canonical_terms: list[str] | None = None,
    title_marked: bool,
    filters: dict[str, list[str]],
    date_range: list[str],
    followup_mode: str,
) -> bool:
    clean_entities = [
        str(item).strip()
        for item in [*(entities or []), *(alias_canonical_terms or [])]
        if str(item).strip()
    ]
    title_anchor_present = bool(title_marked or list(alias_canonical_terms or []))
    if not title_anchor_present or not clean_entities:
        return False
    if str(followup_mode or "none") != "none":
        return False
    if len([str(item).strip() for item in date_range if str(item).strip()]) == 2:
        return False
    effective_filters = _normalize_media_filter_map(filters)
    if alias_canonical_terms:
        effective_filters = {
            field: values
            for field, values in effective_filters.items()
            if field != "author"
        }
    if _has_specific_media_constraints(effective_filters):
        return False
    return _question_requests_personal_evaluation(question) or _question_requests_media_details(question)


def _is_title_anchored_version_compare_query(
    question: str,
    *,
    entities: list[str],
    alias_canonical_terms: list[str] | None = None,
    title_marked: bool,
    filters: dict[str, list[str]],
    date_range: list[str],
    followup_mode: str,
) -> bool:
    text = str(question or "").strip()
    clean_entities = [
        str(item).strip()
        for item in [*(entities or []), *(alias_canonical_terms or [])]
        if str(item).strip()
    ]
    title_anchor_present = bool(title_marked or list(alias_canonical_terms or []))
    if not title_anchor_present or not clean_entities:
        return False
    if str(followup_mode or "none") != "none":
        return False
    if len([str(item).strip() for item in date_range if str(item).strip()]) == 2:
        return False
    effective_filters = _normalize_media_filter_map(filters)
    if alias_canonical_terms:
        effective_filters = {
            field: values
            for field, values in effective_filters.items()
            if field != "author"
        }
    if _has_specific_media_constraints(effective_filters):
        return False
    version_cues = ("版本", "演绎", "录音", "比较", "对比")
    compare_cues = ("评价", "咋样", "如何")
    return any(cue in text for cue in version_cues) and any(cue in text for cue in compare_cues)


def _merge_filter_values(base: dict[str, list[str]], field: str, values: list[str]) -> None:
    from . import media_core as media_core_owner

    media_core_owner._merge_filter_values(base, field, values)


def _infer_media_filters(query: str) -> dict[str, list[str]]:
    from . import media_core as media_core_owner

    return media_core_owner._infer_media_filters(query)


def _render_resolved_question_from_decision(
    question: str,
    previous_state: dict[str, Any],
    followup_mode: str,
    entities: list[str],
) -> str:
    current = str(question or "").strip()
    if not current and entities:
        return entities[0]
    return current


def _normalize_timing_breakdown(values: dict[str, Any] | None) -> dict[str, float]:
    if not isinstance(values, dict):
        return {}
    normalized: dict[str, float] = {}
    for key, value in values.items():
        name = str(key or "").strip()
        if not name:
            continue
        try:
            number = max(0.0, float(value or 0.0))
        except Exception:
            continue
        normalized[name] = round(number, 6)
    return normalized


def _derive_media_lookup_mode(
    *,
    domain: str,
    entities: list[str],
    filters: dict[str, list[str]],
    date_range: list[str],
    followup_mode: str,
    abstract_media_concept: bool,
    collection_query: bool,
) -> str:
    if domain != "media":
        return "general_lookup"
    if abstract_media_concept and not entities and not filters and len(date_range) != 2:
        return "concept_lookup"
    if len(date_range) == 2 or collection_query or followup_mode in {"inherit_filters", "inherit_timerange"}:
        return "filter_search"
    if _has_specific_media_constraints(filters):
        return "filter_search"
    if entities or followup_mode == "inherit_entity":
        return "entity_lookup"
    return "general_lookup"


def _strip_semantic_hint_filter_fields(filters: dict[str, list[str]] | None) -> dict[str, list[str]]:
    normalized = _normalize_media_filter_map(filters)
    stripped = {str(field): [str(value).strip() for value in values if str(value).strip()] for field, values in normalized.items() if field not in {"instrument", "work_type"}}
    return _sanitize_media_filters(stripped)


def _infer_router_freshness(question: str) -> Literal["none", "recent", "realtime"]:
    text = str(question or "").strip().lower()
    if not text:
        return "none"
    if any(cue in text for cue in constants.ROUTER_REALTIME_CUES):
        return "realtime"
    if any(cue in text for cue in constants.ROUTER_RECENT_CUES):
        return "recent"
    return "none"


def _has_router_tech_cues(question: str) -> bool:
    text = str(question or "").strip().lower()
    if not text:
        return False
    for cue in constants.ROUTER_TECH_CUES:
        normalized_cue = str(cue or "").strip().lower()
        if not normalized_cue:
            continue
        if re.fullmatch(r"[a-z]+", normalized_cue):
            if re.search(rf"(?<![a-z]){re.escape(normalized_cue)}(?![a-z])", text):
                return True
        elif normalized_cue in text:
            return True
    return False


def _has_router_media_surface(question: str) -> bool:
    text = str(question or "").strip()
    if not text:
        return False
    return bool(_has_media_title_marker(text) or _has_media_intent_cues(text) or any(cue in text for cue in constants.ROUTER_MEDIA_SURFACE_CUES))


def _router_followup_mode_label(mode: str) -> str:
    normalized = str(mode or "none").strip().lower()
    if normalized == "inherit_timerange":
        return "time_window_replace"
    if normalized in {"inherit_filters", "inherit_entity"}:
        return "followup_expand"
    return "none"


def _is_collection_media_query(query: str) -> bool:
    text = str(query or "").strip()
    if not text:
        return False
    if _has_media_title_marker(text):
        return False
    lowered = text.lower()
    if any(cue in lowered for cue in constants.ROUTER_COLLECTION_NEGATIVE_CUES) and not _has_router_media_surface(text):
        return False
    if not _has_router_media_surface(text):
        return False
    if _is_creator_collection_media_query(text):
        return True
    if any(cue in text for cue in ("三部曲", "系列", "几部")):
        return True
    return any(cue in text for cue in constants.MEDIA_COLLECTION_CUES)


def _question_requests_personal_evaluation(question: str, query_classification: dict[str, Any] | None = None) -> bool:
    text = str(question or "").strip()
    if not text and isinstance(query_classification, dict):
        text = str(query_classification.get("resolved_question", "") or "").strip()
    lowered = text.lower()
    if any(cue in text for cue in ("评价", "评分", "评论", "短评", "看法", "感受", "印象", "我的评价", "几分", "打几分", "评分最高", "评价最高", "最高分", "最低分", "最喜欢", "最好的一", "最差的一")):
        return True
    if re.search(r"我.{0,20}(?:给了?|打了?)(?:它|这(?:部|本|张|首|个)|那(?:部|本|张|首|个))?.{0,6}(?:几分|多少分|分数)", text):
        return True
    if re.search(r"(?:最好|最差|最喜欢|评分最高|评价最高).{0,8}(?:一(?:部|本|张|首|个|款)|哪(?:部|本|张|首|个|款)|是哪个|是哪(?:部|本|张|首|个|款))", text):
        return True
    return any(cue in lowered for cue in ("review", "rating", "comment"))


def _question_requests_media_details(question: str, query_classification: dict[str, Any] | None = None) -> bool:
    text = str(question or "").strip()
    if not text and isinstance(query_classification, dict):
        text = str(query_classification.get("resolved_question", "") or "").strip()
    lowered = text.lower()
    detail_cues = (*constants.ROUTER_MEDIA_DETAIL_CUES, "介绍", "介绍一下", "分别介绍", "分别介绍一下", "分别讲什么", "分别讲讲", "讲什么", "讲了什么", "是什么内容", "概述", "具体细节", "细节信息", "详细信息", "详细资料", "作者", "出版方", "出版社", "发行方", "发行商", "渠道", "平台", "工作室", "厂牌", "制作公司")
    if any(cue in text for cue in detail_cues):
        return True
    return any(cue in lowered for cue in ("author", "publisher", "channel", "platform", "studio", "detail"))


def _decision_requires_tmdb(decision: Any) -> bool:
    from ...planner import planner_contracts

    text = str(getattr(decision, "raw_question", "") or "").strip()
    if not text or not bool(getattr(decision, "needs_media_db", False)):
        return False
    media_family = str(getattr(decision, "media_family", "") or "").strip()
    if media_family and media_family != planner_contracts.ROUTER_MEDIA_FAMILY_AUDIOVISUAL:
        return False
    plot_detail_cues = ("剧情", "简介", "介绍", "讲了什么")
    evaluation_cues = ("评分", "评价")
    if getattr(decision, "followup_mode", "") == "inherit_filters" and any(cue in text for cue in plot_detail_cues):
        return True
    if getattr(decision, "followup_mode", "") == "inherit_entity" and any(cue in text for cue in (*constants.ROUTER_MEDIA_DETAIL_CUES, *evaluation_cues)):
        return True
    if (_is_collection_media_query(text) or getattr(decision, "followup_mode", "") in {"inherit_filters", "inherit_timerange"}) and not getattr(decision, "entities", None):
        return False
    if (getattr(decision, "lookup_mode", "") == "filter_search" or _has_specific_media_constraints(getattr(decision, "filters", {})) or getattr(decision, "date_range", None)) and not getattr(decision, "entities", None):
        return False
    detail_cues = constants.ROUTER_MEDIA_DETAIL_CUES
    if any(cue in text for cue in TMDB_AUDIOVISUAL_CUES):
        return True
    if "《" in text and "》" in text and any(cue in text for cue in detail_cues):
        return True
    if getattr(decision, "entities", None) and any(cue in text for cue in detail_cues):
        return True
    if getattr(decision, "followup_mode", "") == "inherit_entity" and any(cue in text for cue in detail_cues):
        return True
    return False
