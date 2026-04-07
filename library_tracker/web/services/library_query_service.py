from __future__ import annotations

import re
import time
from datetime import datetime
from typing import Any

from . import library_service_core as core


def get_bootstrap_data(initial_query: str = "", initial_limit: int = 50) -> dict[str, Any]:
    return {
        "filter_options": core._cached_filter_options(),
        "suggestions": core._cached_form_suggestions(),
        "facets": core._cached_facet_counts(None),
        "initial_results": search_items(
            query=initial_query,
            mode="keyword",
            filters=None,
            limit=initial_limit,
            offset=0,
        ),
    }


def get_filter_options() -> dict[str, list[str]]:
    return core._cached_filter_options()


def get_form_suggestions() -> dict[str, list[str]]:
    return core._cached_form_suggestions()


def get_facet_counts(filters: dict[str, list[str]] | None = None) -> dict[str, dict[str, int]]:
    return core._cached_facet_counts(filters)


def _item_year(item: dict[str, Any]) -> int | None:
    raw = str(item.get("date") or "").strip()
    match = re.match(r"^(\d{4})-", raw)
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


def _stats_labels(item: dict[str, Any], field: str) -> list[str]:
    if field in core.MULTI_TAG_FIELDS:
        labels = core._split_multi_tags(item.get(field))
    else:
        value = str(item.get(field) or "").strip()
        labels = [value] if value else []
    return labels or ["(未填写)"]


def _stats_yearly_rows(items: list[dict[str, Any]], media_types: list[str]) -> list[dict[str, Any]]:
    buckets: dict[int, dict[str, int]] = {}
    for item in items:
        year = _item_year(item)
        media_type = str(item.get("media_type") or "").strip().lower()
        if year is None or media_type not in media_types:
            continue
        row = buckets.setdefault(year, {key: 0 for key in media_types})
        row[media_type] += 1
    output: list[dict[str, Any]] = []
    for year in sorted(buckets):
        row = {"year": year, **buckets[year]}
        row["total"] = sum(int(row.get(media_type) or 0) for media_type in media_types)
        output.append(row)
    return output


def get_stats_dashboard(field: str, year: int | None = None) -> dict[str, Any]:
    dimension = str(field or "").strip().lower()
    if dimension not in core.STATS_DIMENSION_FIELDS:
        raise ValueError(f"Unsupported field: {dimension}")

    overview = get_stats_overview()
    media_types = list(core.MEDIA_FILES.keys())
    items = core._iter_all_items()
    donut_counts: dict[str, dict[str, int]] = {media_type: {} for media_type in media_types}
    yearly_label_counts: dict[str, dict[int, dict[str, int]]] = {media_type: {} for media_type in media_types}
    label_totals_by_media: dict[str, dict[str, int]] = {media_type: {} for media_type in media_types}

    for item in items:
        media_type = str(item.get("media_type") or "").strip().lower()
        if media_type not in donut_counts:
            continue
        item_year = _item_year(item)
        labels = _stats_labels(item, dimension)
        if year is None or item_year == year:
            media_bucket = donut_counts[media_type]
            for label in labels:
                media_bucket[label] = media_bucket.get(label, 0) + 1
        if item_year is None:
            continue
        year_bucket = yearly_label_counts[media_type].setdefault(item_year, {})
        total_bucket = label_totals_by_media[media_type]
        for label in labels:
            year_bucket[label] = year_bucket.get(label, 0) + 1
            if label != "(未填写)":
                total_bucket[label] = total_bucket.get(label, 0) + 1

    donuts: dict[str, list[dict[str, Any]]] = {}
    for media_type in media_types:
        pairs = sorted(donut_counts[media_type].items(), key=lambda item: (-item[1], item[0]))[:12]
        donuts[media_type] = [{"label": label, "value": count} for label, count in pairs]

    yearly_counts = _stats_yearly_rows(items, media_types)
    trend_options = [{"value": media_type, "label": overview.get("media_labels", {}).get(media_type, media_type)} for media_type in media_types]
    trend_series: dict[str, list[dict[str, Any]]] = {}
    for media_type in media_types:
        top_labels = [
            label
            for label, _count in sorted(label_totals_by_media[media_type].items(), key=lambda item: (-item[1], item[0]))[:6]
        ]
        rows: list[dict[str, Any]] = []
        for bucket_year in sorted(yearly_label_counts[media_type]):
            label_counts = yearly_label_counts[media_type][bucket_year]
            row = {"year": bucket_year}
            for label in top_labels:
                row[label] = int(label_counts.get(label) or 0)
            row["total"] = sum(int(label_counts.get(label) or 0) for label in top_labels)
            rows.append(row)
        trend_series[media_type] = rows

    return {
        "field": dimension,
        "year": year,
        "overview": overview,
        "donuts": donuts,
        "yearly_counts": yearly_counts,
        "trend_options": trend_options,
        "trend_series": trend_series,
    }


def get_stats_overview() -> dict[str, Any]:
    media_types = list(core.MEDIA_FILES.keys())
    current_year = datetime.now().year
    items = core._iter_all_items()
    alias_proposal_summary = core.library_alias_store.get_alias_proposal_summary()
    graph_summary = core._graph_dashboard_summary()
    total_by_media = {key: 0 for key in media_types}
    current_year_by_media = {key: 0 for key in media_types}
    years: set[int] = set()

    for item in items:
        media_type = str(item.get("media_type") or "").strip().lower()
        if media_type not in total_by_media:
            continue
        total_by_media[media_type] += 1
        item_year = _item_year(item)
        if item_year is not None:
            years.add(item_year)
        if item_year == current_year:
            current_year_by_media[media_type] += 1

    return {
        "current_year": current_year,
        "media_types": media_types,
        "media_labels": {media_type: media_type.capitalize() for media_type in media_types},
        "total_by_media": total_by_media,
        "current_year_by_media": current_year_by_media,
        "total_all": sum(total_by_media.values()),
        "current_year_all": sum(current_year_by_media.values()),
        "available_years": sorted(years, reverse=True),
        "dimension_fields": list(core.STATS_DIMENSION_FIELDS),
        "alias_proposal": alias_proposal_summary,
        "vector_rows": core._embedding_row_count(),
        "graph": graph_summary,
    }


def get_stats_pie(field: str, year: int | None = None) -> dict[str, Any]:
    dimension = str(field or "").strip().lower()
    if dimension not in core.STATS_DIMENSION_FIELDS:
        raise ValueError(f"Unsupported field: {dimension}")

    media_types = list(core.MEDIA_FILES.keys())
    counts_by_media: dict[str, dict[str, int]] = {media_type: {} for media_type in media_types}
    items = core._iter_all_items()

    for item in items:
        media_type = str(item.get("media_type") or "").strip().lower()
        if media_type not in counts_by_media:
            continue
        if year is not None and _item_year(item) != year:
            continue

        if dimension in core.MULTI_TAG_FIELDS:
            labels = core._split_multi_tags(item.get(dimension))
        else:
            value = str(item.get(dimension) or "").strip()
            labels = [value] if value else []

        if not labels:
            labels = ["(未填写)"]

        bucket = counts_by_media[media_type]
        for label in labels:
            bucket[label] = bucket.get(label, 0) + 1

    charts: dict[str, list[dict[str, Any]]] = {}
    for media_type in media_types:
        pairs = sorted(counts_by_media[media_type].items(), key=lambda item: (-item[1], item[0]))
        charts[media_type] = [{"label": label, "value": value} for label, value in pairs[:12]]

    return {
        "field": dimension,
        "year": year,
        "charts": charts,
    }


def search_items(
    query: str,
    mode: str,
    filters: dict[str, list[str]] | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    normalized_mode = (mode or "keyword").strip().lower()
    if normalized_mode not in {"keyword", "vector"}:
        normalized_mode = "keyword"

    normalized_query = query or ""
    cache_key = core._search_cache_key(normalized_query, normalized_mode, filters)
    now = time.monotonic()
    cached = core._SEARCH_CACHE.get(cache_key)

    if cached and now - cached["ts"] < core._SEARCH_CACHE_TTL:
        scored: list[core.SearchResult] = cached["scored"]
        graph_expand: dict[str, Any] = cached["graph_expand"]
    else:
        scored, graph_expand = core._compute_scored(normalized_query, normalized_mode, filters)
        core._SEARCH_CACHE[cache_key] = {"ts": now, "scored": scored, "graph_expand": graph_expand}

    page_size = max(1, int(limit))
    page_offset = max(0, int(offset))
    total_count = len(scored)
    trimmed = scored[page_offset : page_offset + page_size]
    return {
        "query": normalized_query,
        "mode": normalized_mode,
        "sql_preview": core._build_sql_preview(filters, normalized_query, normalized_mode),
        "count": len(trimmed),
        "total_count": total_count,
        "offset": page_offset,
        "limit": page_size,
        "graph_expansion": graph_expand if normalized_mode == "vector" else {},
        "results": [
            {
                **core._to_result(result.item, result.score),
                "keyword_hits": core._keyword_hits(result.item, normalized_query) if normalized_mode == "keyword" and normalized_query.strip() else [],
                "alias_hits": list(result.alias_hits or []),
            }
            for result in trimmed
        ],
    }


def get_item(item_id: str) -> dict[str, Any]:
    media_type, index = core._parse_item_id(item_id)
    payload = core._load_payload(media_type)
    records = payload.get("records", [])
    if index < 0 or index >= len(records):
        raise core.ItemNotFoundError(item_id)
    item = records[index]
    if not isinstance(item, dict):
        raise core.ItemNotFoundError(item_id)
    return core._normalize_item(item, media_type, index)
