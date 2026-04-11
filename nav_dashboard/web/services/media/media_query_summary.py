from __future__ import annotations

from typing import Any


def _clip_values(values: list[str], limit: int = 3) -> str:
    cleaned = [str(value).strip() for value in values if str(value).strip()]
    if not cleaned:
        return ""
    if len(cleaned) <= limit:
        return " / ".join(cleaned)
    return f"{' / '.join(cleaned[:limit])} 等{len(cleaned)}项"


def _lookup_mode_label(value: str) -> str:
    mapping = {
        "entity_lookup": "实体检索",
        "filter_search": "筛选检索",
        "general_lookup": "泛化检索",
        "working_set_followup": "沿用上一轮结果集",
        "working_set_item_lookup": "锁定上一轮条目",
    }
    return mapping.get(str(value or "").strip(), str(value or "").strip() or "检索")


def _retrieval_mode_label(value: str) -> str:
    mapping = {
        "hybrid": "关键词 + 向量",
        "keyword": "关键词",
        "vector": "向量",
        "filter_only": "仅筛选",
    }
    return mapping.get(str(value or "").strip(), str(value or "").strip() or "未知")


def build_media_query_summary_lines(
    *,
    result_count: int,
    mention_count: int = 0,
    lookup_mode: str,
    retrieval_mode: str,
    sort_preference: str,
    filters: dict[str, list[str]] | None = None,
    media_entities: list[str] | None = None,
    composer_hints: list[str] | None = None,
    work_family_hints: list[str] | None = None,
    instrument_hints: list[str] | None = None,
    form_hints: list[str] | None = None,
    keyword_queries: list[str] | None = None,
    vector_queries: list[str] | None = None,
) -> list[str]:
    filter_map = filters if isinstance(filters, dict) else {}
    first_line = f"命中 {int(result_count)} 条媒体记录"
    if mention_count:
        first_line += f"，扩展提及 {int(mention_count)} 条"
    lines = [first_line]
    lines.append(
        f"检索方式：{_lookup_mode_label(lookup_mode)}；召回：{_retrieval_mode_label(retrieval_mode)}；排序：{str(sort_preference or 'relevance').strip() or 'relevance'}"
    )

    scope_parts: list[str] = []
    media_type_values = [str(value).strip() for value in (filter_map.get("media_type") or []) if str(value).strip()]
    if media_type_values:
        scope_parts.append(f"范围：{_clip_values(media_type_values, 2)}")
    other_filters = []
    for field in ("category", "author", "nationality", "channel", "publisher", "year", "rating"):
        values = [str(value).strip() for value in (filter_map.get(field) or []) if str(value).strip()]
        if values:
            other_filters.append(f"{field}={_clip_values(values, 2)}")
    if other_filters:
        scope_parts.append(f"筛选：{'；'.join(other_filters[:3])}")
    entities = [str(value).strip() for value in (media_entities or []) if str(value).strip()]
    if entities:
        scope_parts.append(f"关键词：{_clip_values(entities, 2)}")
    if scope_parts:
        lines.append("；".join(scope_parts))

    music_parts: list[str] = []
    composers = [str(value).strip() for value in (composer_hints or []) if str(value).strip()]
    families = [str(value).strip() for value in (work_family_hints or []) if str(value).strip()]
    instruments = [str(value).strip() for value in (instrument_hints or []) if str(value).strip()]
    forms = [str(value).strip() for value in (form_hints or []) if str(value).strip()]
    if composers:
        music_parts.append(f"作曲家：{_clip_values(composers, 2)}")
    if families:
        music_parts.append(f"作品族：{_clip_values(families, 2)}")
    elif instruments or forms:
        music_parts.append(f"作品形态：{_clip_values([*instruments, *forms], 3)}")
    if music_parts:
        lines.append("；".join(music_parts))

    query_parts: list[str] = []
    keyword_values = [str(value).strip() for value in (keyword_queries or []) if str(value).strip()]
    vector_values = [str(value).strip() for value in (vector_queries or []) if str(value).strip()]
    if keyword_values:
        query_parts.append(f"关键词检索：{_clip_values(keyword_values, 3)}")
    if vector_values:
        query_parts.append(f"向量检索：{_clip_values(vector_values, 2)}")
    if query_parts:
        lines.append("；".join(query_parts))

    return lines


def build_media_query_summary(**kwargs: Any) -> str:
    lines = build_media_query_summary_lines(**kwargs)
    if not lines:
        return ""
    return lines[0]
