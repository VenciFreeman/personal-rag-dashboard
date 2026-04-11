from __future__ import annotations

import re
import urllib.parse as urlparse
from types import SimpleNamespace
from typing import Any
import time as _time

from nav_dashboard.web.clients.internal_services import InternalServiceError, request_json
from nav_dashboard.web.services.ontologies.music_ontology import collect_composer_alias_hints
from nav_dashboard.web.services.planner.domain import trim_media_entity_structural_suffix as planner_trim_media_entity_structural_suffix
from ...media.entity_resolver import resolve_creator_hit as _er_resolve_creator_hit
from ...media.entity_resolver import resolve_title_hit as _er_resolve_title_hit

from . import media_constants as constants
from .media_core import (
    _extract_media_entities_from_local_titles,
    _infer_media_filters,
    _merge_filter_values,
    _normalize_media_filter_map,
    _normalize_media_title_for_match,
    _sanitize_media_filters,
    _strip_media_entity_boundary_terms,
)


_MEDIA_COMPARE_SPLIT_RE = re.compile(r"\s*(?:和|跟|与|及|以及|还有|加上|对比|比较|vs\.?|VS\.?)\s*")
_CREATOR_COLLECTION_SUFFIX_RE = re.compile(
    r"^(.{2,12})的(?:作品|所有作品|全部作品|一切作品|全集|书|全部书|所有书"
    r"|小说|全部小说|诗集|诗|散文|音乐|专辑|歌曲|全部专辑|电影|所有电影|全部电影"
    r"|游戏|剧集|影片|电视剧|动画|代表作|经典作品|经典著作|著作)",
    re.UNICODE,
)
_CREATOR_COLLECTION_ANYWHERE_RE = re.compile(
    r"(.{2,24})的(?:作品|所有作品|全部作品|一切作品|全集|书|全部书|所有书"
    r"|小说|全部小说|诗集|诗|散文|音乐|专辑|歌曲|全部专辑|电影|所有电影|全部电影"
    r"|游戏|剧集|影片|电视剧|动画|代表作|经典作品|经典著作|著作)",
    re.UNICODE,
)
_CREATOR_VERB_RE = re.compile(
    r"^(.{2,12})(?:写过|写了|拍过|拍了|出版了|发行了|创作了|著有)",
    re.UNICODE,
)
_CREATOR_RANKING_RE = re.compile(
    r"^(.{2,16})(?:里|中).{0,18}(?:最好|最喜欢|评分最高|评价最高|最好的一(?:张|部|本|首)|哪(?:张|部|本|首|个))",
    re.UNICODE,
)
_CREATOR_COLLECTION_PREFIX_RE = re.compile(
    r"^(?:最近|近期|最近一段时间|这段时间|最近我|我|我最近|我近期|我这段时间|这几年|近几年|这几年我|近几年我)?"
    r"(?:听过|看过|读过|玩过|追过|收藏过|买过|补过|接触过)?"
    r"(?:的)?(?:哪些|哪几部|哪几本|哪几张|哪几首|什么|有哪(?:些|几)?(?:部|本|张|首|个|套)?)?",
    re.UNICODE,
)
_CREATOR_COLLECTION_LOOSE_RE = re.compile(
    r"(.{2,24})\s*(专辑|作品|电影|影片|动画|番剧|剧集|书|小说|游戏|歌曲|歌|唱片)",
    re.UNICODE,
)
_ROUTER_ENTITY_QUERY_PREFIXES = (
    "在我的数据库里",
    "我的数据库里",
    "在数据库里",
    "请问",
    "帮我",
    "我想知道",
    "想问下",
)
_ROUTER_ENTITY_LEADING_SCAFFOLD_PATTERNS = (
    re.compile(r"^(?:请(?:你)?|麻烦(?:你)?|帮我|我想知道|想问下)\s*", re.UNICODE),
    re.compile(r"^(?:系统地|详细地?|具体地?|全面地?|简要地?|简单地?|大致地?|大概地?)\s*", re.UNICODE),
    re.compile(r"^(?:分析|比较|对比|说明|讲讲|说说|总结|概述|介绍|聊聊)\s*", re.UNICODE),
)


def _http_json(
    method: str,
    url: str,
    payload: dict[str, Any] | None = None,
    timeout: float = 25.0,
    headers: dict[str, str] | None = None,
    trust_env: bool | None = None,
) -> dict[str, Any]:
    parsed = urlparse.urlparse(str(url or ""))
    hostname = str(parsed.hostname or "").strip().casefold()
    inferred_trust_env = bool(parsed.scheme in {"http", "https"} and hostname and hostname not in {"127.0.0.1", "localhost", "::1"})
    try:
        return request_json(
            method,
            url,
            payload=payload,
            timeout=timeout,
            headers=headers,
            trust_env=inferred_trust_env if trust_env is None else bool(trust_env),
            raise_for_status=True,
        )
    except InternalServiceError as exc:
        raise RuntimeError(f"HTTP {exc.status_code}: {exc.detail}") from exc
    except Exception as exc:
        raise RuntimeError(str(exc)) from exc


def _resolve_library_aliases(
    query: str,
    *,
    filters: dict[str, list[str]] | None = None,
    trace_id: str = "",
    trace_stage: str = "agent.media.alias_resolve",
    max_entries: int = 8,
) -> dict[str, Any]:
    normalized_query = str(query or "").strip()
    if not normalized_query:
        return {"query": "", "entries": [], "hits": [], "expanded_terms": []}
    sanitized_filters = _sanitize_media_filters(filters)
    media_type_filters = [
        str(value).strip().lower()
        for value in sanitized_filters.get("media_type", [])
        if str(value).strip()
    ]
    media_type_hint = media_type_filters[0] if len(media_type_filters) == 1 else ""
    return _http_json(
        "POST",
        f"{constants.LIBRARY_TRACKER_BASE}/api/library/alias-resolve",
        payload={
            "query": normalized_query,
            "media_type_hint": media_type_hint,
            "trace_id": str(trace_id or "").strip(),
            "trace_stage": str(trace_stage or "agent.media.alias_resolve").strip(),
            "max_entries": max(1, int(max_entries)),
        },
    )


def _build_tmdb_headers() -> dict[str, str]:
    headers = {
        "accept": "application/json",
        "User-Agent": "PersonalAIStackAgent/0.1",
    }
    if constants.TMDB_READ_ACCESS_TOKEN:
        headers["Authorization"] = f"Bearer {constants.TMDB_READ_ACCESS_TOKEN}"
    return headers


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
    if bool(current.get("media_entity_confident")):
        return False
    concrete_region_hit = any(
        alias in text and len(nationalities) == 1
        for alias, nationalities in constants.MEDIA_REGION_ALIASES.items()
    )
    if concrete_region_hit and not any(cue in text for cue in ("拉美", "拉丁美洲", "流派", "主义", "风格", "佳作", "冷门", "女性主义", "离散叙事", "魔幻现实主义", "后现代主义", "新浪潮")):
        return False
    if not (
        any(keyword in text.lower() for keyword in constants.MEDIA_INTENT_KEYWORDS)
        or any(cue in text for cue in constants.MEDIA_ABSTRACT_CONCEPT_CUES)
    ):
        return False
    return any(cue in text for cue in constants.MEDIA_ABSTRACT_CONCEPT_CUES if cue not in {"小说", "文学", "诗歌", "诗集", "散文", "作家"}) or any(
        cue in text for cue in ("拉美", "拉丁美洲", "流派", "主义", "风格", "佳作", "冷门", "女性主义", "离散叙事", "魔幻现实主义", "后现代主义", "新浪潮")
    )


def _has_media_title_marker(query: str) -> bool:
    return bool(constants.MEDIA_TITLE_MARKER_RE.search(str(query or "")))


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


def _has_media_surface(question: str) -> bool:
    text = str(question or "").strip()
    if not text:
        return False
    return bool(
        _has_media_title_marker(text)
        or any(keyword in text.lower() for keyword in constants.MEDIA_INTENT_KEYWORDS)
        or any(cue in text for cue in constants.ROUTER_MEDIA_SURFACE_CUES)
    )


def _best_local_media_title_match(text: str) -> str:
    candidates = _extract_media_entities_from_local_titles(text)
    if not candidates:
        return ""
    normalized = _normalize_media_title_for_match(text)
    for title in candidates:
        if _normalize_media_title_for_match(title) == normalized:
            return title
    return candidates[0]


def _canonicalize_media_entity(entity: str, *, resolve_title_hit: Any | None = None) -> tuple[str, dict[str, list[str]]]:
    title_resolver = resolve_title_hit or _er_resolve_title_hit
    raw = str(entity or "").strip(" ，。！？?；;:\uff1a\"'\u201c\u201d\u2018\u2019（）()")
    if not raw:
        return "", {}
    inferred_filters = _infer_media_filters(raw)
    er_result = title_resolver(raw, min_confidence=0.5)
    if er_result:
        return er_result.canonical, inferred_filters
    direct_match = _best_local_media_title_match(raw)
    if direct_match:
        return direct_match, inferred_filters
    for candidate in _strip_media_entity_boundary_terms(raw)[1:]:
        er_stripped = title_resolver(candidate, min_confidence=0.4)
        if er_stripped:
            return er_stripped.canonical, inferred_filters
        matched = _best_local_media_title_match(candidate)
        if matched:
            return matched, inferred_filters
    return raw, inferred_filters


def _normalize_media_entities_and_filters(
    entities: list[str],
    base_filters: dict[str, list[str]] | None = None,
    *,
    resolve_title_hit: Any | None = None,
) -> tuple[list[str], dict[str, list[str]]]:
    merged_filters = _normalize_media_filter_map(base_filters)
    normalized_entities: list[str] = []
    seen: set[str] = set()
    for item in entities:
        normalized_entity, inferred_filters = _canonicalize_media_entity(str(item).strip(), resolve_title_hit=resolve_title_hit)
        for field, values in inferred_filters.items():
            _merge_filter_values(merged_filters, field, values)
        clean = normalized_entity or str(item).strip()
        key = _normalize_media_title_for_match(clean)
        if not clean or not key or key in seen:
            continue
        seen.add(key)
        normalized_entities.append(clean)
    return normalized_entities, merged_filters


def _resolve_creator_canonicals(tokens: list[str], *, resolve_creator_hit: Any | None = None) -> set[str]:
    creator_resolver = resolve_creator_hit or _er_resolve_creator_hit
    canonicals: set[str] = set()
    for token in tokens:
        clean = str(token or "").strip()
        if not clean:
            continue
        try:
            resolved = creator_resolver(clean, min_confidence=0.5)
        except Exception:
            resolved = None
        canonical = str(getattr(resolved, "canonical", "") or "").strip()
        if canonical:
            canonicals.add(canonical.casefold())
    return canonicals


def _rewrite_media_query(query: str) -> str:
    raw = (query or "").strip()
    if not raw:
        return ""
    normalized = _normalize_router_media_entity_candidate(raw, keep_empty_fallback=True)
    match = re.search(r"(?:我)?对(?P<title>.+?)的?(?:个人)?(?:评价|看法|评分|感受|印象)", normalized)
    if match:
        entities, _ = _normalize_media_entities_and_filters(_split_media_entities(match.group("title")))
        title = " ".join(entities).strip()
        if title:
            return f"{title} 评价"
    return raw


def _split_media_entities(raw: str) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    text = re.sub(r"(?:以及)?(?:两者|二者)的?(?:对比|比较|区别|差异).*$", "", text)
    text = planner_trim_media_entity_structural_suffix(text)
    text = re.sub(r"的?(?:评价|看法|评分|感受|印象|想法).*$", "", text)
    text = text.strip(" ，。！？?；;:：\"'“”‘’（）()")
    if not text:
        return []
    protected_titles: dict[str, str] = {}

    def _protect_title(match: re.Match[str]) -> str:
        key = f"__MEDIA_TITLE_{len(protected_titles)}__"
        protected_titles[key] = match.group(0)
        return key

    text = re.sub(r"《[^》]+》", _protect_title, text)
    parts = _MEDIA_COMPARE_SPLIT_RE.split(text)
    dedup: list[str] = []
    seen: set[str] = set()
    for part in parts:
        value = str(part or "")
        for key, title in protected_titles.items():
            value = value.replace(key, title)
        value = value.strip(" ，。！？?；;:：\"'“”‘’（）()")
        value = re.sub(r"^(?:我对|对于|关于|我看过的|我读过的|我听过的|我玩过的)", "", value).strip(" ，。！？?；;:：\"'“”‘’（）()")
        value = planner_trim_media_entity_structural_suffix(value)
        value = re.sub(r"(?:这|那)?(?:几|两|各)?(?:部|本|条|张)?(?:作品|条目|项目|系列)$", "", value).strip(" ，。！？?；;:：\"'“”‘’（）()")
        value = re.sub(r"(?:几部|几本|几条|几张|几款)$", "", value).strip(" ，。！？?；;:：\"'“”‘’（）()")
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        dedup.append(value)
    return dedup


def _extract_explicit_media_entities_from_segment(raw: str) -> list[str]:
    split_entities, _ = _normalize_media_entities_and_filters(_split_media_entities(raw))
    local_entities, _ = _normalize_media_entities_and_filters(_extract_media_entities_from_local_titles(raw))
    if not local_entities and not _has_media_surface(raw) and not _has_media_title_marker(raw):
        return []
    merged: list[str] = []
    local_keys = [_normalize_media_title_for_match(item) for item in local_entities if str(item).strip()]
    for entity in [*local_entities, *split_entities]:
        clean = str(entity).strip()
        if not clean:
            continue
        key = _normalize_media_title_for_match(clean)
        if key and any(local_key and local_key != key and local_key in key for local_key in local_keys):
            continue
        if _looks_like_generic_media_scope(clean):
            continue
        if clean not in merged:
            merged.append(clean)
    return merged


def _looks_like_generic_media_scope(text: str) -> bool:
    normalized = str(text or "").strip()
    if not normalized:
        return True
    if normalized in {"我", "我的", "自己", "本人", "我们", "咱们", "咱", "那个", "这个", "那部", "这部", "那本", "这本", "它", "它们"}:
        return True
    compact = re.sub(r"\s+", "", normalized)
    if re.fullmatch(r"(?:这|那|它|其)(?:一)?(?:部|本|张|首|个|套|款)?(?:我自己的|自己的|我的)?(?:评价|评分|看法|感受|印象)?(?:呢)?", compact):
        return True
    if re.fullmatch(r"(?:这|那)(?:一)?(?:部|本|张|首|个|套|款)(?:我自己|自己|我的)?", compact):
        return True
    if re.fullmatch(r"(?:我|我的|自己|本人)?(?:的)?(?:评价|评分|看法|感受|印象)", compact):
        return True
    if re.fullmatch(r"(?:20\d{2}年?)?\d{1,2}(?:月)?(?:到|至|[-~—－])\d{1,2}月(?:的)?(?:番|番剧|动画|动漫|新番)?", compact):
        return True
    if re.fullmatch(r"(?:20\d{2}年?)?\d{1,2}月(?:的)?(?:番|番剧|动画|动漫|新番)?", compact):
        return True
    if re.fullmatch(r"(?:20\d{2}年?)?(?:上半年|下半年)(?:的)?(?:番|番剧|动画|动漫|新番)?", compact):
        return True
    generic_markers = (
        "有哪些", "什么", "简介", "剧情", "介绍", "导演", "演员", "评分", "时间", "我阅读过", "我读过", "我看过", "我玩过", "番剧", "动画", "动漫", "新番", "评价比较高", "评分比较高", "文学", "小说", "诗歌", "作家",
    )
    return any(marker in normalized for marker in generic_markers)


def _normalize_router_media_entity_candidate(text: str, *, keep_empty_fallback: bool = False) -> str:
    normalized = str(text or "").strip(" ，。！？?；;:：,\t\r\n")
    if not normalized:
        return ""
    previous = None
    while normalized and normalized != previous:
        previous = normalized
        for prefix in _ROUTER_ENTITY_QUERY_PREFIXES:
            if normalized.startswith(prefix):
                normalized = normalized[len(prefix):].strip(" ，。！？?；;:：,")
        for pattern in _ROUTER_ENTITY_LEADING_SCAFFOLD_PATTERNS:
            updated = pattern.sub("", normalized).strip(" ，。！？?；;:：,")
            if updated != normalized:
                normalized = updated
    if normalized:
        return "" if _looks_like_generic_media_scope(normalized) else normalized
    if keep_empty_fallback:
        return str(text or "").strip()
    return ""


def _looks_like_creator_surface_candidate(text: str) -> bool:
    candidate = str(text or "").strip()
    if not candidate or len(candidate) > 16:
        return False
    if any(char.isdigit() for char in candidate):
        return False
    if candidate.endswith(("在", "里", "中", "的")):
        return False
    if candidate.startswith(("把", "按", "请", "帮", "推荐", "总结", "概括")):
        return False
    if any(token in candidate for token in ("推荐", "哪些", "哪几", "什么", "最近", "近期", "系统", "应用", "电影推荐", "几部", "几本", "几张", "几首", "法国", "历史", "社科")):
        return False
    if _looks_like_generic_media_scope(candidate):
        return False
    return bool(re.fullmatch(r"[A-Za-z][A-Za-z0-9 .&'_-]{1,23}|[\u4e00-\u9fffA-Za-z][\u4e00-\u9fffA-Za-z0-9 .&'_-]{1,15}", candidate))


def _extract_creator_from_collection_query(
    question: str,
    *,
    resolve_creator_hit: Any | None = None,
) -> Any | None:
    text = str(question or "").strip()
    if not text or len(text) < 3:
        return None
    creator_resolver = resolve_creator_hit or _er_resolve_creator_hit
    for pattern in (_CREATOR_COLLECTION_SUFFIX_RE, _CREATOR_VERB_RE):
        match = pattern.match(text)
        if match:
            candidate = match.group(1).strip()
            if candidate:
                result = creator_resolver(candidate, min_confidence=0.15)
                if result:
                    return result
    ranking_match = _CREATOR_RANKING_RE.match(text)
    if ranking_match:
        candidate = ranking_match.group(1).strip()
        if candidate:
            result = creator_resolver(candidate, min_confidence=0.15)
            if result:
                return result
            if _looks_like_creator_surface_candidate(candidate):
                media_hint = "music" if any(token in text for token in ("张", "首", "专辑", "歌曲", "音乐")) else ""
                return SimpleNamespace(
                    canonical=candidate,
                    media_type_hint=media_hint,
                    confidence=0.2,
                    match_kind="surface_fallback",
                    works=[],
                )
    search_match = _CREATOR_COLLECTION_ANYWHERE_RE.search(text)
    if search_match:
        candidate = _CREATOR_COLLECTION_PREFIX_RE.sub("", search_match.group(1).strip()).strip(" ，,：:；;？?。.!！")
        if candidate:
            result = creator_resolver(candidate, min_confidence=0.15)
            if result:
                return result
    stripped = text
    for prefix in ("把我听过的", "把我看过的", "把我读过的", "把我玩过的", "我听过的", "我看过的", "我读过的", "我玩过的"):
        if stripped.startswith(prefix):
            stripped = stripped[len(prefix):].strip()
            break
    loose_match = _CREATOR_COLLECTION_LOOSE_RE.search(stripped)
    if loose_match:
        candidate = loose_match.group(1).strip(" ，,：:；;？?。.!！")
        noun = loose_match.group(2).strip()
        if candidate:
            result = creator_resolver(candidate, min_confidence=0.15)
            if result:
                return result
            media_hint = "music" if noun in {"专辑", "歌曲", "歌", "唱片"} else ""
            if _looks_like_creator_surface_candidate(candidate):
                return SimpleNamespace(
                    canonical=candidate,
                    media_type_hint=media_hint,
                    confidence=0.2,
                    match_kind="surface_loose_collection",
                    works=[],
                )
    composer_aliases = [str(alias).strip() for alias in collect_composer_alias_hints(text) if str(alias).strip()]
    if composer_aliases:
        return SimpleNamespace(
            canonical=composer_aliases[0],
            media_type_hint="music",
            confidence=1.0,
            match_kind="ontology_alias",
            works=[],
        )
    for alias in composer_aliases:
        clean_alias = str(alias).strip()
        if not clean_alias:
            continue
        result = creator_resolver(clean_alias, min_confidence=0.15)
        if result:
            return result
    return None


def _is_creator_collection_media_query(question: str, *, resolve_creator_hit: Any | None = None) -> bool:
    text = str(question or "").strip()
    ranking_style_query = bool(_CREATOR_RANKING_RE.match(text))
    if not text or (not _has_media_surface(text) and not ranking_style_query):
        return False
    if not _extract_creator_from_collection_query(text, resolve_creator_hit=resolve_creator_hit):
        return False
    collection_nouns = ("作品", "专辑", "电影", "影片", "动画", "番剧", "剧集", "书", "小说", "游戏", "歌曲", "歌", "唱片", "张", "部", "本", "首")
    collection_scope_cues = ("哪些", "哪几", "有哪", "最近", "近期", "听过", "看过", "读过", "玩过", "评分", "评价", "短评", "推荐", "整理", "归类", "归纳", "版本", "别名", "覆盖", "漏掉", "条目", "按作品", "按版本", "最好", "最喜欢", "评分最高", "评价最高", "哪张", "哪部", "哪本", "哪首")
    return any(cue in text for cue in collection_nouns) and any(cue in text for cue in collection_scope_cues)


def _is_collection_media_query(query: str, *, resolve_creator_hit: Any | None = None) -> bool:
    text = str(query or "").strip()
    if not text:
        return False
    if _has_media_title_marker(text):
        return False
    lowered = text.lower()
    if any(cue in lowered for cue in constants.ROUTER_COLLECTION_NEGATIVE_CUES) and not _has_media_surface(text):
        return False
    if not _has_media_surface(text):
        return False
    if _is_creator_collection_media_query(text, resolve_creator_hit=resolve_creator_hit):
        return True
    if any(cue in text for cue in ("三部曲", "系列", "几部")):
        return True
    return any(cue in text for cue in ("哪些", "哪几", "有哪", "推荐", "合集", "整理"))


def _extract_media_entities(query: str, *, resolve_title_hit: Any | None = None, resolve_creator_hit: Any | None = None) -> list[str]:
    raw = str(query or "").strip()
    if not raw:
        return []
    if _looks_like_time_only_followup(raw):
        return []
    collection_query = _is_collection_media_query(raw, resolve_creator_hit=resolve_creator_hit)
    creator_collection_query = _is_creator_collection_media_query(raw, resolve_creator_hit=resolve_creator_hit)
    normalized = _normalize_router_media_entity_candidate(raw, keep_empty_fallback=True)
    if _looks_like_generic_media_scope(normalized):
        return []
    match = re.search(r"(?:我)?对(?P<title>.+?)(?:的)?(?:个人)?(?:评价|看法|评分|感受|印象|想法)", normalized)
    if match:
        entities = _extract_explicit_media_entities_from_segment(match.group("title"))
        if entities and not any(_looks_like_generic_media_scope(entity) for entity in entities):
            return entities
    match = re.search(r"^(?P<title>.+?)(?:的)?(?:对比|比较(?!高)|区别|差异)", normalized)
    if match:
        entities = _extract_explicit_media_entities_from_segment(match.group("title"))
        if entities:
            return entities
    match = re.search(r"^(?P<title>.+?)的(?:各个)?(?:主角|角色|剧情|介绍|评价|看法|分析|总结)", normalized)
    if match:
        entities = _extract_explicit_media_entities_from_segment(match.group("title"))
        if entities and not any(_looks_like_generic_media_scope(entity) for entity in entities):
            return entities
    match = re.search(r"^(?P<title>.+?)(?:这部|这个|这本|这套)?(?:电影|影片|片子|电视剧|剧集|剧|动漫|动画|番剧|漫画|小说|书)?呢$", normalized)
    if match:
        entities = _extract_explicit_media_entities_from_segment(match.group("title"))
        if entities and not any(_looks_like_generic_media_scope(entity) for entity in entities):
            return entities
    if any(token in normalized for token in ["对比", "比较", "区别", "差异", "评价", "看法", "评分"]):
        entities = _extract_explicit_media_entities_from_segment(normalized)
        if len(entities) >= 2 and not any(_looks_like_generic_media_scope(entity) for entity in entities):
            return entities
    if (collection_query or creator_collection_query) and not (
        _question_requests_personal_evaluation(normalized)
        or _question_requests_media_details(normalized)
        or _has_media_title_marker(normalized)
    ):
        return []
    entities, _ = _normalize_media_entities_and_filters(
        _extract_media_entities_from_local_titles(normalized),
        resolve_title_hit=resolve_title_hit,
    )
    if entities and not any(_looks_like_generic_media_scope(entity) for entity in entities):
        return entities
    return []


def _resolve_media_keyword_rows(
    keyword_queries: list[str],
    *,
    filters: dict[str, list[str]],
    trace_id: str,
    candidate_window_limit: int,
    library_tracker_base: str,
    http_json: Any,
    resolve_library_aliases: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str], float, float]:
    keyword_rows: list[dict[str, Any]] = []
    alias_resolution_entries: list[dict[str, Any]] = []
    seen_alias_entries: set[str] = set()
    expanded_keyword_queries: list[str] = []
    seen_expanded_keyword_queries: set[str] = set()
    alias_resolution_seconds = 0.0
    keyword_search_seconds = 0.0

    for q_item in keyword_queries:
        alias_t0 = _time.perf_counter()
        alias_resolution = resolve_library_aliases(
            q_item,
            filters=filters,
            trace_id=trace_id,
            trace_stage="agent.media.alias_resolve",
        )
        alias_resolution_seconds += _time.perf_counter() - alias_t0
        keyword_t0 = _time.perf_counter()
        payload = http_json(
            "POST",
            f"{library_tracker_base}/api/library/search",
            payload={
                "query": q_item,
                "mode": "keyword",
                "limit": candidate_window_limit,
                "filters": filters,
            },
            headers={"X-Trace-Id": trace_id, "X-Trace-Stage": "agent.media.keyword"},
        )
        keyword_search_seconds += _time.perf_counter() - keyword_t0
        for expansion in alias_resolution.get("expanded_terms") if isinstance(alias_resolution.get("expanded_terms"), list) else []:
            if not isinstance(expansion, dict):
                continue
            for term in expansion.get("terms") if isinstance(expansion.get("terms"), list) else []:
                clean_term = str(term).strip()
                if not clean_term or clean_term in keyword_queries or clean_term in seen_expanded_keyword_queries:
                    continue
                seen_expanded_keyword_queries.add(clean_term)
                expanded_keyword_queries.append(clean_term)
        for entry in alias_resolution.get("entries") if isinstance(alias_resolution.get("entries"), list) else []:
            if not isinstance(entry, dict):
                continue
            fallback_key = "|".join(
                [
                    str(entry.get("field_type") or "").strip().lower(),
                    str(entry.get("media_type") or "").strip().lower(),
                    str(entry.get("raw_value") or "").strip().casefold(),
                    str(entry.get("canonical_name") or "").strip().casefold(),
                ]
            )
            entry_key = str(entry.get("id") or fallback_key).strip()
            if not entry_key or entry_key in seen_alias_entries:
                continue
            seen_alias_entries.add(entry_key)
            alias_resolution_entries.append(
                {
                    "key": str(entry.get("key") or ""),
                    "field": str(entry.get("field") or ""),
                    "field_type": str(entry.get("field_type") or ""),
                    "media_type": str(entry.get("media_type") or ""),
                    "raw_value": str(entry.get("raw_value") or ""),
                    "canonical_name": str(entry.get("canonical_name") or ""),
                    "matched_text": str(entry.get("matched_text") or ""),
                    "expanded_terms": list(entry.get("expanded_terms") or []),
                }
            )
        current = payload.get("results", []) if isinstance(payload, dict) else []
        for row in current:
            if not isinstance(row, dict):
                continue
            cloned = dict(row)
            cloned["matched_query"] = q_item
            cloned["alias_hits"] = list(row.get("alias_hits") or [])
            cloned["keyword_hits"] = list(row.get("keyword_hits") or [])
            cloned["alias_expansion_match"] = False
            keyword_rows.append(cloned)

    for q_item in expanded_keyword_queries:
        keyword_expand_t0 = _time.perf_counter()
        payload = http_json(
            "POST",
            f"{library_tracker_base}/api/library/search",
            payload={
                "query": q_item,
                "mode": "keyword",
                "limit": candidate_window_limit,
                "filters": filters,
            },
            headers={"X-Trace-Id": trace_id, "X-Trace-Stage": "agent.media.keyword.alias_expand"},
        )
        keyword_search_seconds += _time.perf_counter() - keyword_expand_t0
        current = payload.get("results", []) if isinstance(payload, dict) else []
        for row in current:
            if not isinstance(row, dict):
                continue
            cloned = dict(row)
            cloned["matched_query"] = q_item
            cloned["alias_hits"] = list(row.get("alias_hits") or [])
            cloned["keyword_hits"] = list(row.get("keyword_hits") or [])
            cloned["alias_expansion_query"] = q_item
            cloned["alias_expansion_match"] = True
            keyword_rows.append(cloned)

    return keyword_rows, alias_resolution_entries, expanded_keyword_queries, alias_resolution_seconds, keyword_search_seconds


def _concept_cache_key(query: str) -> str:
    return _strip_query_scaffolding(query).casefold() or str(query or "").strip().casefold()


def _get_cached_mediawiki_concept(query: str) -> dict[str, Any] | None:
    key = _concept_cache_key(query)
    with constants.MEDIAWIKI_CONCEPT_CACHE["lock"]:
        cached = constants.MEDIAWIKI_CONCEPT_CACHE["entries"].get(key)
        return dict(cached) if isinstance(cached, dict) else None


def _set_cached_mediawiki_concept(query: str, data: dict[str, Any]) -> None:
    key = _concept_cache_key(query)
    with constants.MEDIAWIKI_CONCEPT_CACHE["lock"]:
        constants.MEDIAWIKI_CONCEPT_CACHE["entries"][key] = dict(data)


def _build_mediawiki_headers() -> dict[str, str]:
    headers = {
        "User-Agent": constants.MEDIAWIKI_USER_AGENT,
        "Api-User-Agent": constants.MEDIAWIKI_API_USER_AGENT,
    }
    return {key: value for key, value in headers.items() if str(value or "").strip()}


def _mediawiki_action_request(
    api_url: str,
    params: dict[str, Any],
    trace_id: str = "",
    *,
    timeout_override: float | None = None,
) -> dict[str, Any]:
    query_params = {
        "format": "json",
        "formatversion": 2,
        "utf8": 1,
        "errorformat": "plaintext",
        "maxlag": 5,
        **params,
    }
    if trace_id:
        query_params["requestid"] = trace_id
    url = f"{api_url}?{urlparse.urlencode(query_params, doseq=True)}"
    timeout_value = float(timeout_override) if timeout_override is not None else constants.MEDIAWIKI_TIMEOUT
    return _http_json("GET", url, timeout=timeout_value, headers=_build_mediawiki_headers())


def _build_mediawiki_concept_queries(query: str) -> list[str]:
    base = _strip_query_scaffolding(query)
    candidates: list[str] = []
    for item in [base, str(query or "").strip()]:
        clean = str(item or "").strip()
        if clean and clean not in candidates:
            candidates.append(clean)
        for alias in constants.MEDIAWIKI_QUERY_ALIASES.get(clean, []):
            if alias and alias not in candidates:
                candidates.append(alias)
    for key, aliases in constants.MEDIAWIKI_QUERY_ALIASES.items():
        if key in base or key in str(query or ""):
            for alias in [key, *aliases]:
                if alias and alias not in candidates:
                    candidates.append(alias)
    return candidates[:6]