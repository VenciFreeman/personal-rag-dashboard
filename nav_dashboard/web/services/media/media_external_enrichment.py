from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Callable

from ..agent.agent_types import (
    TOOL_EXPAND_MEDIAWIKI_CONCEPT,
    TOOL_QUERY_MEDIA,
    TOOL_SEARCH_BANGUMI,
    TOOL_SEARCH_TMDB,
    ToolExecution,
)


@dataclass(frozen=True)
class PerItemExpansionConfig:
    media_family: str = ""
    answer_shape: str = ""
    allow_per_item_tmdb: bool = False
    allow_per_item_bangumi: bool = False
    allow_per_item_wiki: bool = False
    expand_limit: int = 10
    max_workers: int = 4
    tmdb_min_confidence: float = 0.38
    bangumi_min_confidence: float = 0.45
    tmdb_timeout: float = 8.0
    max_book_wiki_items: int = 2
    bangumi_access_token_available: bool = False
    tmdb_available: bool = False
    bangumi_subject_types: tuple[int, ...] = (2, 6)


@dataclass(frozen=True)
class PerItemExpansionToolset:
    media_title_match_boost_any: Callable[[str, list[str]], float]
    safe_score: Callable[[Any], float]
    clip_text: Callable[[Any, int], str]
    search_bangumi: Callable[..., ToolExecution]
    search_tmdb_media: Callable[..., ToolExecution]
    parse_mediawiki_page: Callable[..., ToolExecution]
    search_mediawiki_action: Callable[..., ToolExecution]


_TITLE_STRIP_RE = re.compile(r"[銆娿€嬨€屻€嶃€庛€忋€愩€戯紙锛?)\s銆€]+")
_TITLE_YEAR_SUFFIX_RE = re.compile(r"\s*\(?[12]\d{3}\)?\s*$")
_CJK_RANGE_RE = re.compile(r"[\u3040-\u9fff\uf900-\ufaff\u3400-\u4dbf]+")


def _normalize_fullwidth(text: str) -> str:
    chars: list[str] = []
    for ch in text:
        cp = ord(ch)
        if 0xFF01 <= cp <= 0xFF5E:
            chars.append(chr(cp - 0xFEE0))
        else:
            chars.append(ch)
    return "".join(chars)


def _cjk_bigrams(text: str) -> set[str]:
    bigrams: list[str] = []
    for match in _CJK_RANGE_RE.finditer(text):
        run = match.group()
        for index in range(len(run) - 1):
            bigrams.append(run[index : index + 2])
    return set(bigrams)


def normalize_title_for_match(title: str) -> str:
    normalized = _normalize_fullwidth(title)
    normalized = _TITLE_STRIP_RE.sub(" ", normalized).strip()
    normalized = _TITLE_YEAR_SUFFIX_RE.sub("", normalized).strip()
    return normalized.lower()


def title_similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    shorter, longer = sorted([a, b], key=len)
    if shorter in longer and len(shorter) / max(len(longer), 1) >= 0.6:
        return 0.85

    has_cjk_a = bool(_CJK_RANGE_RE.search(a))
    has_cjk_b = bool(_CJK_RANGE_RE.search(b))
    if has_cjk_a or has_cjk_b:
        bigrams_a = _cjk_bigrams(a)
        bigrams_b = _cjk_bigrams(b)
        token_a = set(re.findall(r"[a-z0-9]{2,}", a))
        token_b = set(re.findall(r"[a-z0-9]{2,}", b))
        combined_a = bigrams_a | token_a
        combined_b = bigrams_b | token_b
        union = combined_a | combined_b
        if not union:
            return 0.0
        return round(len(combined_a & combined_b) / len(union), 3)

    token_a = set(a.split())
    token_b = set(b.split())
    union = token_a | token_b
    if not union:
        return 0.0
    return round(len(token_a & token_b) / len(union), 3)


def validate_per_item_tmdb_match(local_title: str, tmdb_row: dict[str, Any]) -> float:
    if not local_title:
        return 0.0

    local_norm = normalize_title_for_match(local_title)
    if not local_norm:
        return 0.0

    candidates_raw = [
        str(tmdb_row.get("title") or "").strip(),
        str(tmdb_row.get("original_title") or "").strip(),
        str(tmdb_row.get("name") or "").strip(),
        str(tmdb_row.get("original_name") or "").strip(),
    ]
    best = 0.0
    for candidate in candidates_raw:
        if not candidate:
            continue
        candidate_norm = normalize_title_for_match(candidate)
        if not candidate_norm:
            continue
        score = title_similarity(local_norm, candidate_norm)
        if score > best:
            best = score
    return round(best, 3)


def validate_per_item_bangumi_match(local_title: str, bangumi_row: dict[str, Any]) -> float:
    if not local_title:
        return 0.0

    local_norm = normalize_title_for_match(local_title)
    if not local_norm:
        return 0.0

    candidates_raw = [
        str(bangumi_row.get("name_cn") or "").strip(),
        str(bangumi_row.get("name") or "").strip(),
    ]
    best = 0.0
    for candidate in candidates_raw:
        if not candidate:
            continue
        candidate_norm = normalize_title_for_match(candidate)
        if not candidate_norm:
            continue
        score = title_similarity(local_norm, candidate_norm)
        if score > best:
            best = score
    return round(best, 3)


def execute_per_item_expansion(
    tool_results: list[ToolExecution],
    *,
    trace_id: str,
    config: PerItemExpansionConfig,
    toolset: PerItemExpansionToolset,
    item_callback: Callable[[dict[str, Any], int, int], None] | None = None,
) -> list[ToolExecution]:
    media_result = next(
        (item for item in tool_results if item.tool == TOOL_QUERY_MEDIA and item.status in {"ok", "partial"}),
        None,
    )
    if media_result is None or not isinstance(media_result.data, dict):
        return tool_results

    local_rows = [
        row
        for row in list(media_result.data.get("main_results") or media_result.data.get("results") or [])
        if isinstance(row, dict)
    ]
    if not local_rows:
        return tool_results

    media_entities = [
        str(item).strip()
        for item in list(media_result.data.get("media_entities") or [])
        if str(item).strip()
    ]
    prioritized_rows = sorted(
        enumerate(local_rows),
        key=lambda pair: (
            toolset.media_title_match_boost_any(str(pair[1].get("title") or ""), media_entities) >= 0.8,
            toolset.media_title_match_boost_any(str(pair[1].get("title") or ""), media_entities),
            toolset.safe_score(pair[1].get("score")),
            -pair[0],
        ),
        reverse=True,
    )
    sample = [row for _, row in prioritized_rows[: max(1, int(config.expand_limit or 1))]]
    if not sample:
        return tool_results

    is_audiovisual = config.media_family == "audiovisual"
    is_bookish = config.media_family == "bookish"
    use_bangumi = bool(config.allow_per_item_bangumi and config.bangumi_access_token_available)
    use_tmdb = bool(config.allow_per_item_tmdb and config.tmdb_available)
    use_wiki = bool(config.allow_per_item_wiki)
    allow_bookish_wiki = (
        use_wiki
        and not is_bookish
        or str(config.answer_shape or "") == "detail_card"
        or len(sample) <= max(1, int(config.max_book_wiki_items or 1))
    )

    if not any((use_bangumi, use_tmdb, use_wiki)):
        return tool_results

    if is_bookish and use_wiki and not allow_bookish_wiki:
        return tool_results

    def _extract_primary_author(row: dict[str, Any]) -> str:
        author = str(row.get("author") or "").strip()
        if not author:
            return ""
        for separator in (",", "，", "/", "、", ";", "；"):
            if separator in author:
                author = author.split(separator, 1)[0].strip()
                break
        return author

    def _build_wiki_query_candidates(row: dict[str, Any]) -> list[str]:
        title = str(row.get("title") or "").strip()
        if not title:
            return []
        author = _extract_primary_author(row)
        candidates = [
            f"{title} {author} 书" if author else "",
            f"{title} {author}" if author else "",
            f"{title} 书",
            title,
        ]
        seen: set[str] = set()
        ordered: list[str] = []
        for candidate in candidates:
            clean = str(candidate or "").strip()
            if not clean or clean in seen:
                continue
            seen.add(clean)
            ordered.append(clean)
        return ordered

    def _fetch_bangumi(row: dict[str, Any]) -> dict[str, Any] | None:
        title = str(row.get("title") or "").strip()
        if not title:
            return None
        for subject_type in config.bangumi_subject_types:
            try:
                result = toolset.search_bangumi(title, trace_id, subject_type=subject_type)
            except Exception:
                continue
            if result.status not in {"ok", "partial"} or not isinstance(result.data, dict):
                continue
            bangumi_rows = result.data.get("results") or []
            for candidate in (bangumi_rows[:3] if isinstance(bangumi_rows, list) else []):
                if not isinstance(candidate, dict):
                    continue
                confidence = validate_per_item_bangumi_match(title, candidate)
                if confidence < config.bangumi_min_confidence:
                    continue
                external_title = str(candidate.get("name_cn") or candidate.get("name") or "").strip()
                external_overview = toolset.clip_text(str(candidate.get("summary") or ""), 200)
                return {
                    "local_title": title,
                    "local_date": str(row.get("date") or "").strip(),
                    "local_rating": row.get("rating"),
                    "local_review": toolset.clip_text(str(row.get("review") or ""), 120),
                    "external_title": external_title,
                    "external_overview": external_overview,
                    "external_source": "bangumi",
                    "match_confidence": confidence,
                    "id": candidate.get("id"),
                    "url": str(candidate.get("url") or "").strip(),
                    "score": candidate.get("score", confidence),
                    "title": external_title,
                    "overview": external_overview,
                    "_source_title": title,
                }
        return None

    def _fetch_tmdb(row: dict[str, Any]) -> dict[str, Any] | None:
        title = str(row.get("title") or "").strip()
        if not title:
            return None
        try:
            result = toolset.search_tmdb_media(title, trace_id, limit=3, timeout_override=config.tmdb_timeout)
        except Exception:
            return None
        if result.status not in {"ok", "partial"} or not isinstance(result.data, dict):
            return None
        tmdb_rows = result.data.get("results") or []
        for candidate in (tmdb_rows[:3] if isinstance(tmdb_rows, list) else []):
            if not isinstance(candidate, dict):
                continue
            confidence = validate_per_item_tmdb_match(title, candidate)
            if confidence < config.tmdb_min_confidence:
                continue
            external_title = str(candidate.get("title") or "").strip()
            external_overview = toolset.clip_text(str(candidate.get("overview") or ""), 200)
            return {
                "local_title": title,
                "local_date": str(row.get("date") or "").strip(),
                "local_rating": row.get("rating"),
                "local_review": toolset.clip_text(str(row.get("review") or ""), 120),
                "external_title": external_title,
                "external_overview": external_overview,
                "external_source": "tmdb",
                "match_confidence": confidence,
                "id": candidate.get("id"),
                "url": str(candidate.get("url") or "").strip(),
                "score": candidate.get("score", confidence),
                "title": external_title,
                "overview": external_overview,
                "media_type": str(candidate.get("media_type") or "").strip(),
                "date": str(candidate.get("date") or "").strip(),
                "_source_title": title,
            }
        return None

    def _fetch_wiki(row: dict[str, Any]) -> dict[str, Any] | None:
        title = str(row.get("title") or "").strip()
        if not title:
            return None
        page: dict[str, Any] | None = None
        for query in _build_wiki_query_candidates(row):
            try:
                result = toolset.parse_mediawiki_page(query, trace_id)
            except Exception:
                continue
            if result.status in {"ok", "partial"} and isinstance(result.data, dict):
                candidate_page = result.data.get("page")
                if isinstance(candidate_page, dict):
                    page = candidate_page
                    break
            try:
                search_result = toolset.search_mediawiki_action(query, trace_id, limit=2)
            except Exception:
                continue
            if not isinstance(search_result.data, dict):
                continue
            search_rows = search_result.data.get("results") or []
            page = next((candidate for candidate in search_rows if isinstance(candidate, dict)), None)
            if isinstance(page, dict):
                break
        if not isinstance(page, dict):
            return None
        external_title = str(page.get("display_title") or page.get("title") or "").strip()
        external_overview = toolset.clip_text(str(page.get("extract") or page.get("snippet") or ""), 200)
        if not external_title and not external_overview:
            return None
        confidence = (
            title_similarity(normalize_title_for_match(title), normalize_title_for_match(external_title))
            if external_title
            else 0.7
        )
        return {
            "local_title": title,
            "local_date": str(row.get("date") or "").strip(),
            "local_rating": row.get("rating"),
            "local_review": toolset.clip_text(str(row.get("review") or ""), 120),
            "external_title": external_title,
            "external_overview": external_overview,
            "external_source": "wiki",
            "match_confidence": max(confidence, 0.5),
            "url": str(page.get("url") or "").strip(),
            "score": max(confidence, 0.5),
            "title": external_title,
            "overview": external_overview,
            "_source_title": title,
        }

    def _fetch_audiovisual(row: dict[str, Any]) -> dict[str, Any] | None:
        if use_bangumi:
            bangumi_item = _fetch_bangumi(row)
            if bangumi_item is not None:
                return bangumi_item
        if use_tmdb:
            tmdb_item = _fetch_tmdb(row)
            if tmdb_item is not None:
                return tmdb_item
        if use_wiki:
            return _fetch_wiki(row)
        return None

    fetch_fn = _fetch_audiovisual if is_audiovisual and (use_bangumi or use_tmdb) else _fetch_wiki

    ordered_items: list[tuple[int, dict[str, Any]]] = []
    max_workers = max(1, min(int(config.max_workers or 1), len(sample)))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(fetch_fn, row): index for index, row in enumerate(sample)}
        for future in as_completed(futures):
            item = future.result()
            if item is None:
                continue
            ordered_items.append((futures[future], item))
            if item_callback is None:
                continue
            try:
                item_callback(item, len(ordered_items), len(sample))
            except Exception:
                pass
    ordered_items.sort(key=lambda pair: pair[0])
    per_item_data = [item for _, item in ordered_items]
    if not per_item_data:
        return tool_results

    source_counts: dict[str, int] = {}
    for item in per_item_data:
        source = str(item.get("external_source") or "wiki")
        source_counts[source] = source_counts.get(source, 0) + 1
    dominant_source = max(source_counts, key=source_counts.__getitem__) if source_counts else "wiki"
    mixed_sources = len(source_counts) > 1

    if dominant_source == "bangumi":
        fanout_tool = TOOL_SEARCH_BANGUMI
        source_display = "Bangumi"
    elif dominant_source == "tmdb":
        fanout_tool = TOOL_SEARCH_TMDB
        source_display = "TMDB"
    else:
        fanout_tool = TOOL_EXPAND_MEDIAWIKI_CONCEPT
        source_display = "Wiki"

    per_item_fanout_stats = [
        {
            "local_title": item.get("local_title", ""),
            "external_title": item.get("external_title", ""),
            "match_confidence": item.get("match_confidence", 0.0),
            "source": item.get("external_source", ""),
        }
        for item in per_item_data
    ]

    fanout_exec = ToolExecution(
        tool=fanout_tool,
        status="ok",
        summary=(
            f"{source_display} 閫愰」琛ュ厖 "
            f"{len(per_item_data)}/{len(sample)} 鏉★紙per-item fan-out, source={dominant_source}"
            f"{', mixed' if mixed_sources else ''}）"
        ),
        data={
            "trace_id": trace_id,
            "trace_stage": f"agent.tool.per_item_fanout.{dominant_source}",
            "results": per_item_data,
            "per_item_fanout": True,
            "per_item_data": per_item_data,
            "per_item_source": dominant_source,
            "per_item_sources": sorted(source_counts.keys()),
            "source_counts": dict(source_counts),
            "mixed_sources": mixed_sources,
            "source_title_count": len(sample),
            "total_result_count": len(local_rows),
            "per_item_expand_limit": int(config.expand_limit or 0),
            "per_item_fanout_stats": per_item_fanout_stats,
        },
    )

    updated = [result for result in tool_results if result.tool != fanout_tool]
    media_index = next(
        (index for index, result in enumerate(updated) if result.tool == TOOL_QUERY_MEDIA),
        len(updated) - 1,
    )
    updated.insert(media_index + 1, fanout_exec)
    return updated
