from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from nav_dashboard.web.services.ontologies.book_ontology import infer_book_filters_from_text
from nav_dashboard.web.services.media.media_query_adapter import normalize_filter_map as normalize_media_filter_map_layer
from nav_dashboard.web.services.media.media_taxonomy import MEDIA_BOOK_CATEGORY_HINTS, MEDIA_BOOKISH_CUES, MEDIA_REGION_ALIASES, MEDIA_VIDEO_CATEGORY_HINTS
from nav_dashboard.web.services.ontologies.music_ontology import collect_composer_alias_hints, infer_music_filters_from_text
from nav_dashboard.web.services.ontologies.music_ontology import infer_music_filters_from_text
from nav_dashboard.web.services.ontologies.video_ontology import infer_video_filters_from_text


_WORKSPACE_ROOT = Path(__file__).resolve().parents[5]
_MEDIA_LIBRARY_VOCAB_CACHE: dict[str, Any] = {}


def _normalize_media_title_for_match(text: str) -> str:
    value = str(text or "").strip().lower()
    if not value:
        return ""
    value = re.sub(r"\s+", "", value)
    return re.sub(r"[^\u4e00-\u9fffa-z0-9]", "", value)


def _strip_media_entity_boundary_terms(text: str) -> list[str]:
    raw = str(text or "").strip(" ，。！？?；;:\uff1a\"'“”‘’（）()")
    if not raw:
        return []
    candidates = [raw]
    boundary_terms = ["游戏", "galgame", "动漫", "动画", "番剧", "电影", "影片", "片子", "歌曲"]
    seen = {raw.casefold()}
    for term in boundary_terms:
        if raw.startswith(term) and len(raw) - len(term) >= 2:
            candidate = raw[len(term) :].strip(" ，。！？?；;:\uff1a\"'“”‘’（）()")
            if candidate and candidate.casefold() not in seen:
                seen.add(candidate.casefold())
                candidates.append(candidate)
        if raw.endswith(term) and len(raw) - len(term) >= 2:
            candidate = raw[: -len(term)].strip(" ，。！？?；;:\uff1a\"'“”‘’（）()")
            if candidate and candidate.casefold() not in seen:
                seen.add(candidate.casefold())
                candidates.append(candidate)
    return candidates


def _media_title_match_boost(title: str, entity: str) -> float:
    normalized_title = _normalize_media_title_for_match(title)
    normalized_entity = _normalize_media_title_for_match(entity)
    if not normalized_title or not normalized_entity:
        return 0.0
    boost = 0.0
    if normalized_title == normalized_entity:
        boost += 0.8
    elif normalized_title.startswith(normalized_entity):
        suffix = normalized_title[len(normalized_entity) :]
        boost += 0.62 if suffix and len(suffix) <= 8 else 0.45
    elif normalized_entity in normalized_title:
        boost += 0.45
    digits_entity = "".join(re.findall(r"\d+", normalized_entity))
    digits_title = "".join(re.findall(r"\d+", normalized_title))
    if digits_entity and digits_entity != digits_title:
        boost -= 0.55
    return boost


def _media_title_match_boost_any(title: str, entities: list[str]) -> float:
    if not entities:
        return 0.0
    return max((_media_title_match_boost(title, entity) for entity in entities), default=0.0)


def _media_keyword_hit_fields(row: dict[str, Any]) -> set[str]:
    hits = row.get("keyword_hits") if isinstance(row.get("keyword_hits"), list) else []
    fields: set[str] = set()
    for hit in hits:
        if not isinstance(hit, dict):
            continue
        field = str(hit.get("field") or "").strip().lower()
        if field:
            fields.add(field)
    return fields


def _is_alias_expansion_result_relevant(row: dict[str, Any], *, alias_terms: list[str]) -> bool:
    if not isinstance(row, dict):
        return False
    if list(row.get("alias_hits") or []):
        return True
    title = str(row.get("title") or "").strip()
    if _media_title_match_boost_any(title, alias_terms) >= 0.45:
        return True
    keyword_fields = _media_keyword_hit_fields(row)
    return "title" in keyword_fields and _media_title_match_boost_any(title, alias_terms) > 0


def _music_title_has_composer_anchor_conflict(title: str, composer_hints: list[str]) -> bool:
    if not composer_hints:
        return False
    title_aliases = collect_composer_alias_hints(title)
    if not title_aliases:
        return False
    query_aliases: set[str] = set()
    for hint in composer_hints:
        query_aliases.update(_normalize_media_title_for_match(alias) for alias in collect_composer_alias_hints(hint))
        normalized_hint = _normalize_media_title_for_match(hint)
        if normalized_hint:
            query_aliases.add(normalized_hint)
    title_alias_keys = {
        _normalize_media_title_for_match(alias)
        for alias in title_aliases
        if _normalize_media_title_for_match(alias)
    }
    if not query_aliases or not title_alias_keys:
        return False
    return title_alias_keys.isdisjoint(query_aliases)


def _normalize_media_filter_map(value: Any) -> dict[str, list[str]]:
    return normalize_media_filter_map_layer(value)


_VALID_MEDIA_TYPE_FILTER_VALUES: frozenset[str] = frozenset({
    "video",
    "book",
    "music",
    "game",
    "movie",
    "film",
    "tv",
    "series",
    "show",
    "anime",
})


def _sanitize_media_filters(filters: Any) -> dict[str, list[str]]:
    normalized = _normalize_media_filter_map(filters)
    cleaned: dict[str, list[str]] = {}
    for field, values in normalized.items():
        clean_values: list[str] = []
        for value in values:
            clean = str(value).strip()
            if not clean:
                continue
            if field == "media_type" and clean.lower() not in _VALID_MEDIA_TYPE_FILTER_VALUES:
                continue
            clean_values.append(clean)
        if clean_values:
            cleaned[field] = clean_values
    return cleaned


_MEDIA_MULTI_TAG_FILTER_FIELDS: frozenset[str] = frozenset({
    "author",
    "nationality",
    "category",
    "publisher",
})


def _split_media_filter_tags(raw: Any) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    normalized = text.replace("\r", "\n")
    parts = [segment.strip() for segment in re.split(r"[;；，,、\n]+", normalized) if segment.strip()]
    deduped: list[str] = []
    seen: set[str] = set()
    for part in parts:
        cleaned = part.strip('"\'`“”‘’ ')
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        deduped.append(cleaned)
    return deduped


def _media_filter_year(row: dict[str, Any]) -> int | None:
    try:
        value = int(str(row.get("year") or "").strip())
        return value if 1000 <= value <= 2999 else None
    except Exception:
        pass
    match = re.search(r"\b([12]\d{3})\b", str(row.get("date") or ""))
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


def _media_filter_scalar_value(row: dict[str, Any], field: str) -> str:
    if field == "year":
        year_value = _media_filter_year(row)
        return str(year_value) if year_value is not None else ""
    if field == "rating":
        try:
            return str(int(float(row.get("rating") or 0)))
        except Exception:
            return ""
    return str(row.get(field) or "").strip()


def _matches_media_filters(row: dict[str, Any], filters: dict[str, list[str]] | None) -> bool:
    if not filters:
        return True
    year_range_values = [str(value).strip() for value in filters.get("year_range", []) if str(value).strip()]
    if len(year_range_values) >= 2:
        row_year = _media_filter_year(row)
        try:
            start_year = int(year_range_values[0])
            end_year = int(year_range_values[-1])
            if row_year is None or not (start_year <= row_year <= end_year):
                return False
        except (TypeError, ValueError):
            pass

    for field, selected_values in filters.items():
        if field == "year_range":
            continue
        selected = [str(value).strip() for value in selected_values if str(value).strip()]
        if not selected:
            continue
        selected_set = set(selected)
        if field in _MEDIA_MULTI_TAG_FILTER_FIELDS:
            row_values = set(_split_media_filter_tags(row.get(field)))
            if not row_values or row_values.isdisjoint(selected_set):
                return False
            continue
        if _media_filter_scalar_value(row, field) not in selected_set:
            return False
    return True


def _matches_media_date_window(item: dict[str, Any], date_window: dict[str, str] | None) -> bool:
    if not date_window:
        return True
    raw_date = str(item.get("date") or "").strip()
    start = str((date_window or {}).get("start") or "").strip()
    end = str((date_window or {}).get("end") or "").strip()
    if not raw_date or not start or not end:
        return False
    return start <= raw_date <= end


def _clip_text(value: Any, max_chars: int) -> str:
    text = str(value or "").strip()
    if max_chars <= 0 or len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip()


def _merge_filter_values(base: dict[str, list[str]], field: str, values: list[str]) -> None:
    clean = [str(value).strip() for value in values if str(value).strip()]
    if not clean:
        return
    current = [str(value).strip() for value in base.get(field, []) if str(value).strip()]
    seen = {value.casefold() for value in current}
    for value in clean:
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        current.append(value)
    if current:
        base[field] = current


def _infer_media_filters(query: str) -> dict[str, list[str]]:
    text = str(query or "").strip()
    if not text:
        return {}

    filters: dict[str, list[str]] = {}
    if any(keyword in text for keyword in ("游戏", "打过", "玩过", "在玩", "通关")):
        _merge_filter_values(filters, "media_type", ["game"])
    if any(keyword in text for keyword in ("音乐", "专辑", "歌曲", "歌单", "听过")):
        _merge_filter_values(filters, "media_type", ["music"])
    music_filters = infer_music_filters_from_text(text)
    for field, values in music_filters.items():
        _merge_filter_values(filters, field, values)
    if any(keyword in text for keyword in ("电影", "影片", "片子")):
        _merge_filter_values(filters, "media_type", ["video"])
        _merge_filter_values(filters, "category", ["电影"])
    if any(keyword in text for keyword in ("电视剧", "剧集", "连续剧", "美剧", "日剧", "韩剧", "英剧")):
        _merge_filter_values(filters, "media_type", ["video"])
        _merge_filter_values(filters, "category", ["电视剧"])
    if any(keyword in text for keyword in MEDIA_BOOKISH_CUES):
        _merge_filter_values(filters, "media_type", ["book"])

    for alias, nationalities in MEDIA_REGION_ALIASES.items():
        if alias in text:
            _merge_filter_values(filters, "nationality", nationalities)

    for cue, categories in MEDIA_BOOK_CATEGORY_HINTS.items():
        if cue in text:
            _merge_filter_values(filters, "category", categories)

    for cue, categories in MEDIA_VIDEO_CATEGORY_HINTS.items():
        if cue in text:
            _merge_filter_values(filters, "category", categories)
            _merge_filter_values(filters, "media_type", ["video"])

    video_filters = infer_video_filters_from_text(text)
    for field, values in video_filters.items():
        _merge_filter_values(filters, field, values)

    book_filters = infer_book_filters_from_text(text)
    for field, values in book_filters.items():
        _merge_filter_values(filters, field, values)

    if re.search(r"(?:^|[\s，,。！？?；;：:~\-－—]|\d)番(?:呢|吗|吧|剧)?(?:$|[\s，,。！？?；;：:~\-－—])", text):
        _merge_filter_values(filters, "category", ["动画"])
        _merge_filter_values(filters, "media_type", ["video"])

    for year in re.findall(r"(20\d{2})年", text):
        _merge_filter_values(filters, "year", [year])

    return filters


def _safe_score(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _rating_sort_value(row: dict[str, Any], *, descending: bool) -> float:
    value = row.get("rating")
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).strip())
    except Exception:
        return -1.0 if descending else 11.0


def _sort_media_results(rows: list[dict[str, Any]], sort_preference: str) -> list[dict[str, Any]]:
    normalized = str(sort_preference or "relevance").strip().lower()
    if normalized == "date_asc":
        rows.sort(
            key=lambda item: (
                str(item.get("date") or "9999-12-31"),
                -_safe_score(item.get("score")),
                _rating_sort_value(item, descending=True),
            )
        )
        return rows
    if normalized == "date_desc":
        rows.sort(
            key=lambda item: (
                str(item.get("date") or ""),
                _safe_score(item.get("score")),
                _rating_sort_value(item, descending=True),
            ),
            reverse=True,
        )
        return rows
    if normalized == "rating_asc":
        rows.sort(
            key=lambda item: (
                _rating_sort_value(item, descending=False),
                str(item.get("date") or ""),
                -_safe_score(item.get("score")),
            )
        )
        return rows
    if normalized == "rating_desc":
        rows.sort(
            key=lambda item: (
                _rating_sort_value(item, descending=True),
                str(item.get("date") or ""),
                _safe_score(item.get("score")),
            ),
            reverse=True,
        )
        return rows
    rows.sort(
        key=lambda item: (
            _safe_score(item.get("score")),
            _rating_sort_value(item, descending=True),
            str(item.get("date") or ""),
        ),
        reverse=True,
    )
    return rows


def _local_media_vocab_signature() -> tuple[tuple[str, int, int], ...]:
    structured_dir = _WORKSPACE_ROOT / "library_tracker" / "data" / "structured"
    try:
        entries: list[tuple[str, int, int]] = []
        for path in sorted(structured_dir.glob("*.json"), key=lambda item: item.name.lower()):
            try:
                stat = path.stat()
                entries.append((str(path), int(stat.st_mtime_ns), int(stat.st_size)))
            except Exception:
                entries.append((str(path), 0, 0))
    except Exception:
        return ((str(structured_dir / "reading.json"), 0, 0),)
    return tuple(entries) or ((str(structured_dir / "reading.json"), 0, 0),)


def _load_local_media_vocab() -> dict[str, list[str]]:
    signature = _local_media_vocab_signature()
    cached_signature = _MEDIA_LIBRARY_VOCAB_CACHE.get("signature")
    cached_data = _MEDIA_LIBRARY_VOCAB_CACHE.get("data") if isinstance(_MEDIA_LIBRARY_VOCAB_CACHE.get("data"), dict) else None
    if cached_signature == signature and cached_data:
        return cached_data

    structured_dir = _WORKSPACE_ROOT / "library_tracker" / "data" / "structured"
    nationalities: list[str] = []
    authors: list[str] = []
    categories: list[str] = []
    titles: list[str] = []
    seen_nat: set[str] = set()
    seen_author: set[str] = set()
    seen_category: set[str] = set()
    seen_title: set[str] = set()

    for path in sorted(structured_dir.glob("*.json"), key=lambda item: item.name.lower()):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            records = payload.get("records", []) if isinstance(payload, dict) else []
        except Exception:
            records = []

        for row in records:
            if not isinstance(row, dict):
                continue
            title = str(row.get("title") or "").strip()
            normalized_title = _normalize_media_title_for_match(title)
            if title and normalized_title and normalized_title not in seen_title and 1 < len(normalized_title) <= 48:
                seen_title.add(normalized_title)
                titles.append(title)
            nationality = str(row.get("nationality") or "").strip()
            if nationality and nationality not in seen_nat:
                seen_nat.add(nationality)
                nationalities.append(nationality)
            author = str(row.get("author") or "").strip()
            if author and author not in seen_author:
                seen_author.add(author)
                authors.append(author)
            for category in re.split(r"[;；，,、/]+", str(row.get("category") or "")):
                clean_category = category.strip()
                if clean_category and clean_category not in seen_category:
                    seen_category.add(clean_category)
                    categories.append(clean_category)

    data = {
        "nationalities": nationalities,
        "authors": authors,
        "categories": categories,
        "titles": titles,
    }
    _MEDIA_LIBRARY_VOCAB_CACHE["signature"] = signature
    _MEDIA_LIBRARY_VOCAB_CACHE["data"] = data
    return data


def _extract_media_entities_from_local_titles(query: str) -> list[str]:
    normalized_query = _normalize_media_title_for_match(query)
    if not normalized_query:
        return []
    vocab = _load_local_media_vocab()
    matched: list[str] = []
    for title in vocab.get("titles", []):
        clean_title = str(title).strip()
        normalized_title = _normalize_media_title_for_match(clean_title)
        if not normalized_title or len(normalized_title) < 2:
            continue
        if normalized_title in normalized_query:
            matched.append(clean_title)
    matched.sort(key=lambda item: len(_normalize_media_title_for_match(item)), reverse=True)
    dedup: list[str] = []
    seen: set[str] = set()
    for title in matched:
        key = _normalize_media_title_for_match(title)
        if key in seen:
            continue
        if any(key in _normalize_media_title_for_match(existing) for existing in dedup):
            continue
        seen.add(key)
        dedup.append(title)
        if len(dedup) >= 3:
            break
    return dedup