from __future__ import annotations

import html
import json
import os
import re
from dataclasses import dataclass
from typing import Any, Callable
from urllib import parse as urlparse

from .. import agent_types
from ...media.entity_resolver import resolve_creator_hit as _er_resolve_creator_hit
from ...media.entity_resolver import resolve_title_hit as _er_resolve_title_hit
from ...ontologies.music_work_signature import filter_music_compare_rows
from ...planner.domain import extract_media_alias_anchor_queries
from ..support_common import _build_music_signature_queries
from .media_constants import (
    BANGUMI_ACCESS_TOKEN,
    BANGUMI_API_BASE_URL,
    LIBRARY_TRACKER_BASE,
    BANGUMI_SUBJECT_TYPE_ANIME,
    BANGUMI_TIMEOUT,
    MEDIA_BOOKISH_CUES,
    MEDIAWIKI_EN_API,
    MEDIAWIKI_PARSE_TIMEOUT,
    MEDIAWIKI_SEARCH_TIMEOUT,
    MEDIAWIKI_ZH_API,
    TMDB_API_BASE_URL,
    TMDB_API_KEY,
    TMDB_LANGUAGE,
    TMDB_READ_ACCESS_TOKEN,
    TMDB_TIMEOUT,
    TOOL_EXPAND_MEDIAWIKI_CONCEPT,
    TOOL_PARSE_MEDIAWIKI,
    TOOL_SEARCH_BANGUMI,
    TOOL_SEARCH_BY_CREATOR,
    TOOL_SEARCH_MEDIAWIKI,
    TOOL_SEARCH_TMDB,
)
from .media_core import (
    _clip_text,
    _is_alias_expansion_result_relevant,
    _load_local_media_vocab,
    _merge_filter_values,
    _matches_media_date_window,
    _matches_media_filters,
    _media_title_match_boost_any,
    _music_title_has_composer_anchor_conflict,
    _normalize_media_title_for_match,
    _sanitize_media_filters,
    _safe_score,
    _sort_media_results,
)
from .media_helpers import (
    _canonicalize_media_entity,
    _build_mediawiki_concept_queries,
    _build_mediawiki_headers,
    _build_tmdb_headers,
    _extract_media_entities,
    _get_cached_mediawiki_concept,
    _http_json,
    _is_abstract_media_concept_query,
    _mediawiki_action_request,
    _normalize_media_entities_and_filters,
    _resolve_media_keyword_rows,
    _resolve_library_aliases,
    _resolve_creator_canonicals,
    _set_cached_mediawiki_concept,
    _strip_query_scaffolding,
)
from .media_types import ToolExecution
from .mediawiki_tools import clean_mediawiki_snippet as _clean_mediawiki_snippet
from .mediawiki_tools import mediawiki_page_url as _mediawiki_page_url
from .mediawiki_tools import mediawiki_result_score as _mediawiki_result_score
from .tmdb_tools import guess_tmdb_search_path as _guess_tmdb_search_path
from .tmdb_tools import strip_tmdb_query_scaffolding as _strip_tmdb_query_scaffolding
from .tmdb_tools import tmdb_media_url as _tmdb_media_url
from .tmdb_tools import tmdb_result_score as _tmdb_result_score


TOOL_QUERY_MEDIA = agent_types.TOOL_QUERY_MEDIA


@dataclass(frozen=True)
class MediaToolDeps:
    http_json: Callable[..., dict[str, Any]]
    resolve_library_aliases: Callable[..., dict[str, Any]]
    resolve_title_hit: Callable[..., Any]
    resolve_creator_hit: Callable[..., Any]


def build_default_media_tool_deps() -> MediaToolDeps:
    return MediaToolDeps(
        http_json=_http_json,
        resolve_library_aliases=_resolve_library_aliases,
        resolve_title_hit=_er_resolve_title_hit,
        resolve_creator_hit=_er_resolve_creator_hit,
    )


def resolve_media_tool_deps(deps: MediaToolDeps | None = None) -> MediaToolDeps:
    return deps if deps is not None else build_default_media_tool_deps()


_COLLECTION_FILTER_TOP_K_MEDIA = int(os.getenv("NAV_DASHBOARD_COLLECTION_FILTER_TOP_K_MEDIA", "24") or "24")


def _unique_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = str(row.get("id") or row.get("title") or json.dumps(row, ensure_ascii=False, sort_keys=True)).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(dict(row))
    return deduped


def _media_search_payload(
    query: str,
    *,
    mode: str,
    filters: dict[str, list[str]],
    limit: int,
    trace_id: str,
    deps: MediaToolDeps,
) -> list[dict[str, Any]]:
    payload = deps.http_json(
        "POST",
        f"{LIBRARY_TRACKER_BASE}/api/library/search",
        payload={
            "query": query,
            "mode": mode,
            "limit": limit,
            "filters": filters,
        },
        headers={"X-Trace-Id": trace_id, "X-Trace-Stage": f"agent.media.{mode}"},
    )
    rows = payload.get("results", []) if isinstance(payload, dict) else []
    return [dict(row) for row in rows if isinstance(row, dict)]


def _prune_music_vector_queries(queries: list[str]) -> list[str]:
    pruned: list[str] = []
    for query in queries:
        clean = str(query or "").strip()
        if not clean:
            continue
        normalized = clean.casefold()
        token_count = len([token for token in re.split(r"\s+", clean) if token])
        has_specific_marker = bool(re.search(r"(?:op\.?\s*\d+|no\.?\s*\d+|第\s*\d+)", normalized))
        if token_count < 2 and not has_specific_marker:
            continue
        pruned.append(clean)
    return pruned


def _build_media_result(
    *,
    query_text: str,
    trace_id: str,
    results: list[dict[str, Any]],
    options: dict[str, Any],
    validation: dict[str, Any],
    extra: dict[str, Any] | None = None,
) -> ToolExecution:
    layer_breakdown = extra.get("layer_breakdown") if isinstance(extra, dict) else None
    if not isinstance(layer_breakdown, dict):
        layer_breakdown = {
            "strict_scope_active": True,
            "main_count": len(results),
            "mention_count": 0,
            "excluded_count": 0,
        }
    payload = {
        "trace_id": trace_id,
        "trace_stage": "agent.tool.query_media_record",
        "query": query_text,
        "results": results,
        "main_results": results,
        "mention_results": [],
        "validation": validation,
        "layer_breakdown": layer_breakdown,
        "lookup_mode": options.get("lookup_mode"),
        "query_class": options.get("query_class"),
        "subject_scope": options.get("subject_scope"),
        "time_scope_type": options.get("time_scope_type"),
        "answer_shape": options.get("answer_shape"),
        "media_family": options.get("media_family"),
    }
    if extra:
        payload.update(extra)
    return ToolExecution(
        tool=TOOL_QUERY_MEDIA,
        status="ok" if results else "empty",
        summary=f"命中 {len(results)} 条媒体记录",
        data=payload,
    )


def _tool_query_media_record(
    query: str,
    query_profile: dict[str, Any],
    trace_id: str = "",
    options: dict[str, Any] | None = None,
    *,
    deps: MediaToolDeps | None = None,
) -> ToolExecution:
    resolved_deps = resolve_media_tool_deps(deps)
    resolved_options = dict(options or {})
    query_text = str(resolved_options.get("query_text") or query or "").strip()
    lookup_mode = str(resolved_options.get("lookup_mode") or "general_lookup").strip().lower()
    sort_mode = str((resolved_options.get("ranking") or {}).get("mode") or resolved_options.get("sort") or "relevance").strip().lower()
    filters = _sanitize_media_filters(resolved_options.get("filters"))
    raw_date_window = resolved_options.get("date_window")
    date_window = {
        str(key or "").strip(): str(value or "").strip()
        for key, value in dict(raw_date_window or {}).items()
        if str(key or "").strip() in {"start", "end"} and str(value or "").strip()
    }
    if not date_window:
        date_range = [str(item or "").strip() for item in list(resolved_options.get("date_range") or [])[:2]]
        if len([item for item in date_range if item]) == 2:
            date_window = {"start": date_range[0], "end": date_range[1]}
    media_entities = [str(item).strip() for item in list(resolved_options.get("media_entities") or []) if str(item).strip()]
    query_class = str(resolved_options.get("query_class") or "").strip().lower()
    composer_hints = [str(item).strip() for item in list(resolved_options.get("composer_hints") or []) if str(item).strip()]
    instrument_hints = [str(item).strip() for item in list(resolved_options.get("instrument_hints") or []) if str(item).strip()]
    form_hints = [str(item).strip() for item in list(resolved_options.get("form_hints") or []) if str(item).strip()]
    work_family_hints = [str(item).strip() for item in list(resolved_options.get("work_family_hints") or []) if str(item).strip()]
    work_signature = [str(item).strip() for item in list(resolved_options.get("work_signature") or []) if str(item).strip()]

    search_filters = dict(filters)
    soft_filter_relaxation: list[str] = []
    raw_candidate_count = 0
    rows: list[dict[str, Any]] = []
    extra: dict[str, Any] = {}
    limit = _COLLECTION_FILTER_TOP_K_MEDIA

    is_music_compare = (
        search_filters.get("media_type") == ["music"]
        and bool(composer_hints)
        and bool(work_signature)
        and query_class == "personal_media_review_collection"
    )

    if lookup_mode == "entity_lookup" and media_entities:
        if search_filters.pop("author", None) is not None:
            soft_filter_relaxation.append("author")
        if search_filters.pop("category", None) is not None:
            soft_filter_relaxation.append("category")
        for entity in media_entities:
            exact_rows = _media_search_payload(
                entity,
                mode="keyword",
                filters=search_filters,
                limit=limit,
                trace_id=trace_id,
                deps=resolved_deps,
            )
            raw_candidate_count += len(exact_rows)
            rows.extend(exact_rows)
            if rows:
                break
    if not rows and is_music_compare:
        if search_filters.pop("author", None) is not None:
            soft_filter_relaxation.append("author")
        if search_filters.pop("category", None) is not None:
            soft_filter_relaxation.append("category")
        keyword_queries = _build_music_signature_queries(
            composer_hints,
            work_signature,
            instrument_hints,
            form_hints,
            work_family_hints,
        )
        keyword_rows, alias_resolution_entries, expanded_keyword_queries, _alias_seconds, _keyword_seconds = _resolve_media_keyword_rows(
            keyword_queries,
            filters=search_filters,
            trace_id=trace_id,
            candidate_window_limit=limit,
            library_tracker_base=LIBRARY_TRACKER_BASE,
            http_json=resolved_deps.http_json,
            resolve_library_aliases=resolved_deps.resolve_library_aliases,
        )
        rows.extend(keyword_rows)
        raw_candidate_count += len(keyword_rows)
        if alias_resolution_entries:
            extra["alias_resolution"] = {"entries": alias_resolution_entries}
        for vector_query in _prune_music_vector_queries(keyword_queries):
            vector_rows = _media_search_payload(
                vector_query,
                mode="vector",
                filters=search_filters,
                limit=limit,
                trace_id=trace_id,
                deps=resolved_deps,
            )
            raw_candidate_count += len(vector_rows)
            for row in vector_rows:
                row.setdefault("matched_query", vector_query)
                rows.append(row)
        rows = _unique_rows(rows)
        if rows:
            rows = filter_music_compare_rows(
                rows,
                work_signature=work_signature,
                composer_hints=composer_hints,
                instrument_hints=instrument_hints,
                form_hints=form_hints,
                work_family_hints=work_family_hints,
                resolve_creator_canonicals=_resolve_creator_canonicals,
                has_composer_anchor_conflict=_music_title_has_composer_anchor_conflict,
            )
        if rows:
            focus_title = str(rows[0].get("title") or "").strip()
            if focus_title:
                exact_rows = _media_search_payload(
                    focus_title,
                    mode="keyword",
                    filters=search_filters,
                    limit=limit,
                    trace_id=trace_id,
                    deps=resolved_deps,
                )
                raw_candidate_count += len(exact_rows)
                rows = _unique_rows(exact_rows + rows)
        if not rows and soft_filter_relaxation:
            blank_rows = _media_search_payload(
                "",
                mode="keyword",
                filters=search_filters,
                limit=limit,
                trace_id=trace_id,
                deps=resolved_deps,
            )
            raw_candidate_count += len(blank_rows)
            rows = _unique_rows(blank_rows)
    elif not rows:
        alias_anchor_queries = extract_media_alias_anchor_queries(query_text)
        filter_driven_collection_query = lookup_mode == "filter_search" and not media_entities and bool(date_window or search_filters)
        if filter_driven_collection_query:
            keyword_queries = [""]
        else:
            keyword_queries = alias_anchor_queries or ([query_text] if query_text else [""])
        keyword_rows, alias_entries, _expanded_queries, _alias_seconds, _keyword_seconds = _resolve_media_keyword_rows(
            keyword_queries,
            filters=search_filters,
            trace_id=trace_id,
            candidate_window_limit=limit,
            library_tracker_base=LIBRARY_TRACKER_BASE,
            http_json=resolved_deps.http_json,
            resolve_library_aliases=resolved_deps.resolve_library_aliases,
        )
        rows.extend(keyword_rows)
        raw_candidate_count += len(keyword_rows)
        if alias_entries:
            extra["alias_resolution"] = {"entries": alias_entries}
            extra["layer_breakdown"] = {"strict_scope_active": True}

    rows = _unique_rows(rows)
    if date_window:
        rows = [row for row in rows if _matches_media_date_window(row, date_window)]
    rows = _sort_media_results(rows, sort_mode)
    validation = {
        "raw_candidates_count": raw_candidate_count,
        "returned_result_count": len(rows),
        "mention_result_count": 0,
        "dropped_by_validator": max(0, raw_candidate_count - len(rows)),
        "dropped_by_reference_limit": 0,
    }
    if soft_filter_relaxation:
        extra["soft_filter_relaxation"] = soft_filter_relaxation
    return _build_media_result(
        query_text=query_text,
        trace_id=trace_id,
        results=rows,
        options=resolved_options,
        validation=validation,
        extra=extra,
    )


def _tmdb_request(path: str, params: dict[str, Any], trace_id: str = "", *, deps: MediaToolDeps | None = None) -> dict[str, Any]:
    resolved_deps = resolve_media_tool_deps(deps)
    if not TMDB_API_KEY and not TMDB_READ_ACCESS_TOKEN:
        raise RuntimeError("未配置 TMDB API 凭证")
    query_params = dict(params)
    if TMDB_API_KEY:
        query_params.setdefault("api_key", TMDB_API_KEY)
    url = f"{TMDB_API_BASE_URL}/{path.lstrip('/')}?{urlparse.urlencode(query_params, doseq=True)}"
    headers = _build_tmdb_headers()
    if trace_id:
        headers["X-Trace-Id"] = trace_id
    return resolved_deps.http_json("GET", url, timeout=TMDB_TIMEOUT, headers=headers)


def _tool_search_tmdb_media(
    query: str,
    trace_id: str = "",
    *,
    limit: int = 5,
    timeout_override: float | None = None,
    deps: MediaToolDeps | None = None,
) -> ToolExecution:
    resolved_deps = resolve_media_tool_deps(deps)
    if not TMDB_API_KEY and not TMDB_READ_ACCESS_TOKEN:
        return ToolExecution(
            tool=TOOL_SEARCH_TMDB,
            status="empty",
            summary="未配置 TMDB API 凭证",
            data={"trace_id": trace_id, "trace_stage": "agent.tool.search_tmdb_media", "results": []},
        )
    search_path = _guess_tmdb_search_path(query)
    media_entities = _extract_media_entities(query)
    lookup = str(media_entities[0] or "").strip() if media_entities else ""
    if not lookup:
        lookup = _strip_tmdb_query_scaffolding(query) or str(query or "").strip()
    request_params = {
        "query": lookup,
        "language": TMDB_LANGUAGE,
        "include_adult": "false",
        "page": 1,
    }
    if timeout_override is None:
        payload = _tmdb_request(search_path, request_params, trace_id=trace_id, deps=resolved_deps)
    else:
        if not TMDB_API_KEY and not TMDB_READ_ACCESS_TOKEN:
            raise RuntimeError("未配置 TMDB API 凭证")
        query_params = dict(request_params)
        if TMDB_API_KEY:
            query_params.setdefault("api_key", TMDB_API_KEY)
        url = f"{TMDB_API_BASE_URL}/{search_path.lstrip('/')}?{urlparse.urlencode(query_params, doseq=True)}"
        headers = _build_tmdb_headers()
        if trace_id:
            headers["X-Trace-Id"] = trace_id
        payload = resolved_deps.http_json("GET", url, timeout=float(timeout_override), headers=headers)
    rows = payload.get("results", []) if isinstance(payload, dict) else []
    compact: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        media_type = str(row.get("media_type") or "").strip().lower()
        if search_path == "search/movie":
            media_type = "movie"
        elif search_path == "search/tv":
            media_type = "tv"
        elif search_path == "search/person":
            media_type = "person"
        if media_type not in {"movie", "tv", "person"}:
            continue
        item_id = row.get("id")
        title = str(row.get("title") or row.get("name") or "").strip()
        if not title or item_id in {None, ""}:
            continue
        compact.append(
            {
                "id": item_id,
                "title": title,
                "media_type": media_type,
                "date": str(row.get("release_date") or row.get("first_air_date") or "").strip(),
                "overview": _clip_text(row.get("overview", ""), 320),
                "original_language": str(row.get("original_language") or "").strip(),
                "vote_average": row.get("vote_average"),
                "popularity": row.get("popularity"),
                "known_for_department": str(row.get("known_for_department") or "").strip(),
                "url": _tmdb_media_url(media_type, item_id),
                "score": _tmdb_result_score(lookup, row),
                "source": "tmdb",
            }
        )
    compact.sort(key=lambda item: _safe_score(item.get("score")), reverse=True)
    compact = compact[: max(1, int(limit))]
    return ToolExecution(
        tool=TOOL_SEARCH_TMDB,
        status="ok" if compact else "empty",
        summary=f"TMDB 命中 {len(compact)} 条结果（endpoint={search_path}）",
        data={
            "trace_id": trace_id,
            "trace_stage": "agent.tool.search_tmdb_media",
            "query": lookup,
            "endpoint": search_path,
            "results": compact,
        },
    )


def _tool_search_by_creator(
    creator_name: str,
    trace_id: str = "",
    *,
    media_type: str = "",
    filters: dict[str, list[str]] | None = None,
    date_window: dict[str, str] | None = None,
    sort: str = "relevance",
    max_results: int = 30,
    deps: MediaToolDeps | None = None,
) -> ToolExecution:
    resolved_deps = resolve_media_tool_deps(deps)
    creator_name = str(creator_name or "").strip()
    if not creator_name:
        return ToolExecution(
            tool=TOOL_SEARCH_BY_CREATOR,
            status="empty",
            summary="creator_name 为空，跳过 search_by_creator",
            data={"trace_id": trace_id, "trace_stage": "agent.tool.search_by_creator", "results": [], "found": False},
        )
    normalized_filters = _sanitize_media_filters(filters)

    def _creator_alias_fallback(reason: str) -> ToolExecution:
        fallback_filters = {
            str(key): [str(value).strip() for value in values if str(value).strip()]
            for key, values in normalized_filters.items()
            if str(key).strip() and str(key).strip() != "author"
        }
        fallback_result = _tool_query_media_record(
            creator_name,
            {"profile": "medium"},
            trace_id,
            {
                "lookup_mode": "filter_search",
                "query_class": "media_creator_collection",
                "filters": fallback_filters,
                "media_type": media_type,
                "sort": sort,
                "ranking": {"mode": sort},
            },
            deps=resolved_deps,
        )
        fallback_data = dict(fallback_result.data or {}) if isinstance(fallback_result.data, dict) else {}
        fallback_rows = fallback_data.get("results") if isinstance(fallback_data.get("results"), list) else []
        if fallback_rows:
            fallback_data.update(
                {
                    "trace_id": trace_id,
                    "trace_stage": "agent.tool.search_by_creator.fallback",
                    "found": True,
                    "creator_name": creator_name,
                    "creator_resolution_fallback": reason,
                }
            )
            return ToolExecution(
                tool=TOOL_SEARCH_BY_CREATOR,
                status="ok",
                summary=f"创作者索引未命中，回退别名/标题检索命中 {len(fallback_rows)} 条结果",
                data=fallback_data,
            )
        return ToolExecution(
            tool=TOOL_SEARCH_BY_CREATOR,
            status="empty",
            summary=f"本地图书馆中未找到创作者「{creator_name}」的条目",
            data={
                "trace_id": trace_id,
                "trace_stage": "agent.tool.search_by_creator",
                "results": [],
                "found": False,
                "creator_name": creator_name,
                "creator_resolution_fallback": reason,
            },
        )

    creator_res = resolved_deps.resolve_creator_hit(creator_name, min_confidence=0.35)
    if creator_res is None:
        return _creator_alias_fallback("creator_index_miss")
    if media_type and not normalized_filters.get("media_type"):
        _merge_filter_values(normalized_filters, "media_type", [media_type])
    result_rows = [
        {
            "id": work.item_id,
            "title": work.canonical,
            "media_type": work.media_type,
            "category": work.category,
            "author": creator_res.canonical,
            "date": work.date,
            "rating": work.rating,
            "review": _clip_text(str(work.review or ""), 200),
            "source": "local_creator_index",
            "score": 1.0,
        }
        for work in creator_res.works
    ]
    if normalized_filters:
        result_rows = [row for row in result_rows if _matches_media_filters(row, normalized_filters)]
    if date_window:
        result_rows = [row for row in result_rows if _matches_media_date_window(row, date_window)]
    result_rows = _sort_media_results(result_rows, sort)[: max(1, int(max_results))]
    validation = {
        "raw_candidates_count": len(creator_res.works),
        "returned_result_count": len(result_rows),
        "mention_result_count": 0,
        "dropped_by_validator": 0,
        "dropped_by_reference_limit": 0,
    }
    return ToolExecution(
        tool=TOOL_SEARCH_BY_CREATOR,
        status="ok" if result_rows else "empty",
        summary=(
            f"本地创作者检索：「{creator_res.canonical}」"
            f"（match_kind={creator_res.match_kind}, confidence={round(creator_res.confidence, 3)}），"
            f"共 {len(creator_res.works)} 部作品，返回 {len(result_rows)} 条"
        ),
        data={
            "trace_id": trace_id,
            "trace_stage": "agent.tool.search_by_creator",
            "found": True,
            "canonical_creator": creator_res.canonical,
            "match_kind": creator_res.match_kind,
            "confidence": round(creator_res.confidence, 3),
            "media_type_hint": creator_res.media_type_hint,
            "works_count": len(creator_res.works),
            "applied_filters": normalized_filters,
            "date_window": dict(date_window or {}),
            "sort": str(sort or "relevance"),
            "results": result_rows,
            "main_results": list(result_rows),
            "mention_results": [],
            "validation": validation,
            "layer_breakdown": {
                "strict_scope_active": True,
                "main_count": len(result_rows),
                "mention_count": 0,
                "excluded_count": 0,
            },
        },
    )


def _match_local_terms(haystacks: list[str], vocabulary: list[str], limit: int = 12) -> list[str]:
    matched: list[str] = []
    plain_haystacks = [str(item or "") for item in haystacks if str(item or "").strip()]
    for value in vocabulary:
        clean = str(value or "").strip()
        if not clean:
            continue
        if any(clean in haystack for haystack in plain_haystacks):
            matched.append(clean)
        if len(matched) >= limit:
            break
    return matched


def _tool_search_mediawiki_action(
    query: str,
    trace_id: str = "",
    *,
    limit: int = 5,
    languages: list[str] | None = None,
    deps: MediaToolDeps | None = None,
) -> ToolExecution:
    resolved_deps = resolve_media_tool_deps(deps)
    concept_queries = _build_mediawiki_concept_queries(query)
    target_languages = languages or ["zh", "en"]
    api_map = {"zh": MEDIAWIKI_ZH_API, "en": MEDIAWIKI_EN_API}
    results: list[dict[str, Any]] = []
    for lang in target_languages:
        api_url = api_map.get(lang)
        if not api_url:
            continue
        for candidate in concept_queries:
            try:
                payload = _mediawiki_action_request(
                    api_url,
                    {
                        "action": "query",
                        "list": "search",
                        "srsearch": candidate,
                        "srlimit": min(max(1, int(limit)), 10),
                        "srinfo": "totalhits|suggestion|rewrittenquery",
                        "srprop": "snippet|titlesnippet|sectiontitle|wordcount|timestamp",
                        "srenablerewrites": 1,
                    },
                    trace_id=trace_id,
                    timeout_override=MEDIAWIKI_SEARCH_TIMEOUT,
                )
            except Exception:
                continue
            query_data = payload.get("query", {}) if isinstance(payload.get("query"), dict) else {}
            rows = query_data.get("search", []) if isinstance(query_data.get("search"), list) else []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                title = str(row.get("title") or "").strip()
                snippet = _clean_mediawiki_snippet(row.get("snippet"))
                results.append(
                    {
                        "title": title,
                        "snippet": snippet,
                        "language": lang,
                        "source": "mediawiki_action_query",
                        "query": candidate,
                        "wordcount": row.get("wordcount"),
                        "timestamp": row.get("timestamp"),
                        "url": _mediawiki_page_url(api_url, title) if title else "",
                        "score": round(_mediawiki_result_score(candidate, title, snippet), 6),
                    }
                )
            if rows:
                break
    dedup: dict[tuple[str, str], dict[str, Any]] = {}
    for row in results:
        key = (str(row.get("language") or ""), str(row.get("title") or ""))
        current = dedup.get(key)
        if current is None or _safe_score(row.get("score")) > _safe_score(current.get("score")):
            dedup[key] = row
    compact = sorted(dedup.values(), key=lambda item: _safe_score(item.get("score")), reverse=True)[:limit]
    return ToolExecution(
        tool=TOOL_SEARCH_MEDIAWIKI,
        status="ok" if compact else "empty",
        summary=f"MediaWiki 搜索命中 {len(compact)} 条",
        data={
            "trace_id": trace_id,
            "trace_stage": "agent.tool.search_mediawiki_action",
            "results": compact,
            "queries": concept_queries,
        },
    )


def _tool_parse_mediawiki_page(
    query: str,
    trace_id: str = "",
    *,
    preferred_language: str = "",
    deps: MediaToolDeps | None = None,
) -> ToolExecution:
    resolved_deps = resolve_media_tool_deps(deps)
    language_order = [preferred_language] if preferred_language else []
    for lang in ["zh", "en"]:
        if lang not in language_order:
            language_order.append(lang)
    rows: list[dict[str, Any]] = []
    payload: dict[str, Any] = {}
    title = ""
    lang = language_order[0] if language_order else "zh"
    api_url = MEDIAWIKI_ZH_API if lang == "zh" else MEDIAWIKI_EN_API
    score_value = 0.0
    exact_query = str(query or "").strip()
    for lang in language_order:
        api_url = MEDIAWIKI_ZH_API if lang == "zh" else MEDIAWIKI_EN_API
        try:
            direct_payload = _mediawiki_action_request(
                api_url,
                {
                    "action": "parse",
                    "page": exact_query,
                    "prop": "text|links|categories|displaytitle|revid|iwlinks",
                    "redirects": 1,
                    "section": 0,
                    "disableeditsection": 1,
                    "disabletoc": 1,
                },
                trace_id=trace_id,
                timeout_override=MEDIAWIKI_PARSE_TIMEOUT,
            )
        except Exception:
            continue
        parsed = direct_payload.get("parse", {}) if isinstance(direct_payload.get("parse"), dict) else {}
        direct_title = str(parsed.get("title") or "").strip()
        if direct_title:
            payload = direct_payload
            title = direct_title
            rows = [{"title": direct_title, "language": lang, "score": 1.0, "url": _mediawiki_page_url(api_url, direct_title)}]
            score_value = 1.0
            break
    if not title:
        search_result = _tool_search_mediawiki_action(query, trace_id, limit=3, languages=language_order, deps=resolved_deps)
        rows = search_result.data.get("results", []) if isinstance(search_result.data, dict) else []
        if not rows:
            return ToolExecution(
                tool=TOOL_PARSE_MEDIAWIKI,
                status="empty",
                summary="MediaWiki 页面解析未找到候选条目",
                data={"trace_id": trace_id, "trace_stage": "agent.tool.parse_mediawiki_page", "results": []},
            )
        target = rows[0]
        lang = str(target.get("language") or "zh")
        api_url = MEDIAWIKI_ZH_API if lang == "zh" else MEDIAWIKI_EN_API
        title = str(target.get("title") or "").strip()
        score_value = _safe_score(target.get("score"))
        try:
            payload = _mediawiki_action_request(
                api_url,
                {
                    "action": "parse",
                    "page": title,
                    "prop": "text|links|categories|displaytitle|revid|iwlinks",
                    "redirects": 1,
                    "section": 0,
                    "disableeditsection": 1,
                    "disabletoc": 1,
                },
                trace_id=trace_id,
                timeout_override=MEDIAWIKI_PARSE_TIMEOUT,
            )
        except Exception:
            page = {
                "title": title,
                "display_title": title,
                "language": lang,
                "url": str(target.get("url") or _mediawiki_page_url(api_url, title)),
                "extract": _clean_mediawiki_snippet(target.get("snippet") or "")[:600],
                "links": [],
                "categories": [],
                "source": "mediawiki_action_search_fallback",
                "score": score_value,
            }
            return ToolExecution(
                tool=TOOL_PARSE_MEDIAWIKI,
                status="partial",
                summary=f"MediaWiki 页面解析失败，改用搜索摘要: {title}",
                data={
                    "trace_id": trace_id,
                    "trace_stage": "agent.tool.parse_mediawiki_page",
                    "results": [page],
                    "page": page,
                    "search_results": rows,
                },
            )
    parsed = payload.get("parse", {}) if isinstance(payload.get("parse"), dict) else {}
    html_text = parsed.get("text") if isinstance(parsed.get("text"), str) else str(parsed.get("text", {}).get("*", "") or "")
    plain_text = _clean_mediawiki_snippet(html_text)
    links = [str(row.get("title") or "").strip() for row in parsed.get("links", []) if isinstance(row, dict) and str(row.get("title") or "").strip()]
    categories = [
        str(row.get("category") or row.get("*") or "").strip()
        for row in parsed.get("categories", [])
        if isinstance(row, dict) and str(row.get("category") or row.get("*") or "").strip()
    ]
    page = {
        "title": title,
        "display_title": _clean_mediawiki_snippet(parsed.get("displaytitle") or title) or title,
        "language": lang,
        "url": _mediawiki_page_url(api_url, title),
        "extract": plain_text[:2400],
        "links": links[:80],
        "categories": categories[:40],
        "source": "mediawiki_action_parse",
        "score": score_value,
    }
    return ToolExecution(
        tool=TOOL_PARSE_MEDIAWIKI,
        status="ok",
        summary=f"MediaWiki 页面解析成功: {title}",
        data={
            "trace_id": trace_id,
            "trace_stage": "agent.tool.parse_mediawiki_page",
            "results": [page],
            "page": page,
            "search_results": rows,
        },
    )


def _tool_expand_mediawiki_concept(query: str, trace_id: str = "", *, deps: MediaToolDeps | None = None) -> ToolExecution:
    resolved_deps = resolve_media_tool_deps(deps)
    cached = _get_cached_mediawiki_concept(query)
    if cached is not None:
        return ToolExecution(
            tool=TOOL_EXPAND_MEDIAWIKI_CONCEPT,
            status="ok",
            summary=f"MediaWiki 概念展开缓存命中: {cached.get('concept', _strip_query_scaffolding(query))}",
            data=cached,
        )
    if not _is_abstract_media_concept_query(query):
        creator_hint = resolved_deps.resolve_creator_hit(query, min_confidence=0.5)
        if not creator_hint:
            data = {
                "trace_id": trace_id,
                "trace_stage": "agent.tool.expand_mediawiki_concept",
                "results": [],
                "concept": _strip_query_scaffolding(query) or str(query or "").strip(),
                "filters": {},
                "authors": [],
                "aliases": [],
            }
            return ToolExecution(tool=TOOL_EXPAND_MEDIAWIKI_CONCEPT, status="skipped", summary="当前问题不需要外部概念展开", data=data)
    pages: list[dict[str, Any]] = []
    seen_pages: set[tuple[str, str]] = set()
    concept_queries = _build_mediawiki_concept_queries(query)
    for candidate in concept_queries[:2]:
        if len(pages) >= 2:
            break
        parsed = _tool_parse_mediawiki_page(candidate, trace_id, deps=resolved_deps)
        if parsed.status not in {"ok", "partial"} or not isinstance(parsed.data, dict):
            continue
        page = parsed.data.get("page")
        if not isinstance(page, dict):
            continue
        title = str(page.get("title") or "").strip()
        language = str(page.get("language") or "").strip()
        key = (language, title)
        if not title or key in seen_pages:
            continue
        seen_pages.add(key)
        pages.append(page)
    search_tool = _tool_search_mediawiki_action(query, trace_id, limit=4, deps=resolved_deps)
    search_rows = search_tool.data.get("results", []) if isinstance(search_tool.data, dict) else []
    for row in search_rows[:4]:
        title = str(row.get("title") or "").strip()
        language = str(row.get("language") or "").strip()
        key = (language, title)
        if not title or key in seen_pages:
            continue
        seen_pages.add(key)
        parsed = _tool_parse_mediawiki_page(title, trace_id, preferred_language=language, deps=resolved_deps)
        if parsed.status not in {"ok", "partial"} or not isinstance(parsed.data, dict):
            continue
        page = parsed.data.get("page")
        if isinstance(page, dict):
            pages.append(page)
        if len(pages) >= 2:
            break
    vocab = _load_local_media_vocab()
    haystacks: list[str] = []
    aliases: list[str] = []
    result_rows: list[dict[str, Any]] = []
    for page in pages:
        haystacks.extend(
            [
                str(page.get("display_title") or ""),
                str(page.get("title") or ""),
                str(page.get("extract") or ""),
                *[str(value) for value in page.get("links", []) if str(value).strip()],
                *[str(value) for value in page.get("categories", []) if str(value).strip()],
            ]
        )
        for alias in [str(page.get("display_title") or "").strip(), str(page.get("title") or "").strip()]:
            if alias and alias not in aliases:
                aliases.append(alias)
        result_rows.append(page)
    for row in search_rows:
        title = str(row.get("title") or "").strip()
        if title and title not in aliases:
            aliases.append(title)
    matched_countries = _match_local_terms(haystacks, vocab.get("nationalities", []), limit=12)
    matched_authors = _match_local_terms(haystacks, vocab.get("authors", []), limit=12)
    matched_categories = _match_local_terms(haystacks, vocab.get("categories", []), limit=8)
    filters: dict[str, list[str]] = {}
    _merge_filter_values(filters, "nationality", matched_countries)
    _merge_filter_values(filters, "category", matched_categories)
    if any(keyword in str(query or "") for keyword in MEDIA_BOOKISH_CUES):
        _merge_filter_values(filters, "media_type", ["book"])
    concept = _strip_query_scaffolding(query) or str(query or "").strip()
    data = {
        "trace_id": trace_id,
        "trace_stage": "agent.tool.expand_mediawiki_concept",
        "results": result_rows,
        "concept": concept,
        "aliases": aliases[:10],
        "countries": matched_countries,
        "authors": matched_authors,
        "categories": matched_categories,
        "filters": filters,
        "search_results": search_rows,
    }
    _set_cached_mediawiki_concept(query, data)
    return ToolExecution(
        tool=TOOL_EXPAND_MEDIAWIKI_CONCEPT,
        status="ok" if result_rows or matched_countries or matched_authors else "empty",
        summary=f"MediaWiki 概念展开完成（countries={len(matched_countries)}, authors={len(matched_authors)}, aliases={len(data['aliases'])}）",
        data=data,
    )


def _tool_search_bangumi(
    title: str,
    trace_id: str = "",
    *,
    subject_type: int = BANGUMI_SUBJECT_TYPE_ANIME,
    limit: int = 5,
    deps: MediaToolDeps | None = None,
) -> ToolExecution:
    resolved_deps = resolve_media_tool_deps(deps)
    if not BANGUMI_ACCESS_TOKEN:
        return ToolExecution(
            tool=TOOL_SEARCH_BANGUMI,
            status="empty",
            summary="未配置 Bangumi Access Token",
            data={"trace_id": trace_id, "trace_stage": "agent.tool.search_bangumi", "results": []},
        )
    url = f"{BANGUMI_API_BASE_URL}/v0/search/subjects?limit={limit}"
    headers = {
        "Authorization": f"Bearer {BANGUMI_ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "personal-ai-stack/1.0 (https://github.com/personal)",
    }
    if trace_id:
        headers["X-Trace-Id"] = trace_id
    body: dict[str, Any] = {"keyword": title, "sort": "match"}
    if subject_type:
        body["filter"] = {"type": [subject_type]}
    try:
        raw = resolved_deps.http_json("POST", url, payload=body, timeout=BANGUMI_TIMEOUT, headers=headers)
    except Exception as exc:  # noqa: BLE001
        return ToolExecution(
            tool=TOOL_SEARCH_BANGUMI,
            status="error",
            summary=f"Bangumi 搜索失败：{exc}",
            data={"trace_id": trace_id, "trace_stage": "agent.tool.search_bangumi", "results": []},
        )
    rows = raw.get("data") or [] if isinstance(raw, dict) else []
    compact: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        bgm_id = row.get("id")
        name = str(row.get("name") or "").strip()
        name_cn = str(row.get("name_cn") or "").strip()
        if not name and not name_cn:
            continue
        rating_block = row.get("rating") or {}
        score = float(rating_block.get("score") or 0.0) if isinstance(rating_block, dict) else 0.0
        compact.append(
            {
                "id": bgm_id,
                "name": name,
                "name_cn": name_cn,
                "title": name_cn or name,
                "summary": _clip_text(str(row.get("summary") or ""), 320),
                "date": str(row.get("date") or "").strip(),
                "score": score,
                "type": row.get("type"),
                "source": "bangumi",
                "url": f"https://bgm.tv/subject/{bgm_id}" if bgm_id else "",
            }
        )
    compact.sort(key=lambda item: float(item.get("score") or 0.0), reverse=True)
    return ToolExecution(
        tool=TOOL_SEARCH_BANGUMI,
        status="ok" if compact else "empty",
        summary=f"Bangumi 命中 {len(compact)} 条结果（keyword={title!r}）",
        data={"trace_id": trace_id, "trace_stage": "agent.tool.search_bangumi", "results": compact},
    )
