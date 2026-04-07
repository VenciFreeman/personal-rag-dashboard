from __future__ import annotations

import json
import os
import re
import time as _time
from dataclasses import dataclass
from typing import Any, Callable, Iterator

from ...prompts.answer_prompts import build_answer_system_prompt
from ..planner import planner_contracts
from ..agent.agent_types import (
    TOOL_EXPAND_MEDIAWIKI_CONCEPT,
    TOOL_PARSE_MEDIAWIKI,
    TOOL_QUERY_MEDIA,
    TOOL_SEARCH_BANGUMI,
    TOOL_SEARCH_BY_CREATOR,
    TOOL_SEARCH_MEDIAWIKI,
    TOOL_SEARCH_TMDB,
    TOOL_SEARCH_WEB,
    ToolExecution,
)


PROMPT_HISTORY_MAX_MESSAGES = int(os.getenv("NAV_DASHBOARD_PROMPT_HISTORY_MAX_MESSAGES", "6") or "6")
PROMPT_HISTORY_ITEM_MAX_CHARS = int(os.getenv("NAV_DASHBOARD_PROMPT_HISTORY_ITEM_MAX_CHARS", "360") or "360")
PROMPT_MEMORY_MAX_CHARS = int(os.getenv("NAV_DASHBOARD_PROMPT_MEMORY_MAX_CHARS", "1800") or "1800")
PROMPT_TOOL_CONTEXT_MAX_CHARS = int(os.getenv("NAV_DASHBOARD_PROMPT_TOOL_CONTEXT_MAX_CHARS", "5200") or "5200")
PROMPT_TOOL_CONTEXT_RETRY_CHARS = int(os.getenv("NAV_DASHBOARD_PROMPT_TOOL_CONTEXT_RETRY_CHARS", "3000") or "3000")
PROMPT_TOOL_RESULT_MAX_CHARS = int(os.getenv("NAV_DASHBOARD_PROMPT_TOOL_RESULT_MAX_CHARS", "1700") or "1700")
PROMPT_TOOL_RESULT_MIN_CHARS = int(os.getenv("NAV_DASHBOARD_PROMPT_TOOL_RESULT_MIN_CHARS", "420") or "420")


@dataclass(frozen=True)
class PromptAssemblyDeps:
    normalize_search_mode: Callable[[str], str]
    build_answer_focus_hints: Callable[[str, list[ToolExecution]], str]
    build_reference_hint_lines: Callable[[list[ToolExecution]], list[str]]
    llm_chat: Callable[..., str]
    llm_chat_stream: Callable[..., Iterator[str]]
    is_context_length_error: Callable[[Exception], bool]
    get_media_main_result_rows_from_data: Callable[[dict[str, Any] | None], list[dict[str, Any]]]
    get_media_mention_rows_from_data: Callable[[dict[str, Any] | None], list[dict[str, Any]]]
    normalize_title_for_match: Callable[[str], str]
    format_media_rating: Callable[[Any], str]
    approx_tokens: Callable[[str], int]
    clip_text: Callable[[Any, int], str]


def _prompt_string_limit_for_key(key: str) -> int:
    lowered = str(key or "").strip().lower()
    if lowered in {"url", "path", "title", "display_title", "name", "date", "language", "media_type", "author"}:
        return 120
    if lowered in {"extract", "overview", "snippet", "content", "review", "summary"}:
        return 220
    return 160


def _sanitize_for_prompt(
    value: Any,
    *,
    clip_text: Callable[[Any, int], str],
    key: str = "",
    max_depth: int = 3,
    max_list_items: int = 5,
    max_dict_items: int = 12,
) -> Any:
    if max_depth <= 0:
        return clip_text(value, _prompt_string_limit_for_key(key))
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return clip_text(value, _prompt_string_limit_for_key(key))
    if isinstance(value, dict):
        trimmed: dict[str, Any] = {}
        omitted = 0
        for idx, (raw_key, raw_value) in enumerate(value.items()):
            if idx >= max_dict_items:
                omitted += 1
                continue
            child_key = str(raw_key or "").strip()
            if child_key in {"trace_id", "trace_stage", "latency_ms", "query_profile", "html_text", "raw_html"}:
                continue
            trimmed[child_key] = _sanitize_for_prompt(
                raw_value,
                clip_text=clip_text,
                key=child_key,
                max_depth=max_depth - 1,
                max_list_items=max_list_items,
                max_dict_items=max_dict_items,
            )
        if omitted > 0:
            trimmed["_omitted_keys"] = omitted
        return trimmed
    if isinstance(value, list):
        local_limit = max_list_items
        lowered = str(key or "").strip().lower()
        if lowered == "results":
            local_limit = min(local_limit, 4)
        elif lowered in {"links", "categories", "aliases", "countries", "authors", "queries"}:
            local_limit = min(max(local_limit, 6), 8)
        elif lowered == "search_results":
            local_limit = min(local_limit, 3)
        trimmed_list = [
            _sanitize_for_prompt(
                item,
                clip_text=clip_text,
                key=key,
                max_depth=max_depth - 1,
                max_list_items=max_list_items,
                max_dict_items=max_dict_items,
            )
            for item in value[:local_limit]
        ]
        if len(value) > local_limit:
            trimmed_list.append({"_omitted_items": len(value) - local_limit})
        return trimmed_list
    return clip_text(value, _prompt_string_limit_for_key(key))


def _minimal_prompt_tool_payload(exec_result: ToolExecution, deps: PromptAssemblyDeps) -> dict[str, Any]:
    data = exec_result.data if isinstance(exec_result.data, dict) else {}
    rows = deps.get_media_main_result_rows_from_data(data) if exec_result.tool == TOOL_QUERY_MEDIA else (data.get("results", []) if isinstance(data.get("results"), list) else [])
    mention_rows = deps.get_media_mention_rows_from_data(data) if exec_result.tool == TOOL_QUERY_MEDIA else []
    payload: dict[str, Any] = {
        "tool": exec_result.tool,
        "status": exec_result.status,
        "summary": deps.clip_text(exec_result.summary, 240),
        "result_count": len(rows),
    }
    if mention_rows:
        payload["mention_count"] = len(mention_rows)
    if exec_result.tool == TOOL_EXPAND_MEDIAWIKI_CONCEPT and isinstance(data, dict):
        payload["concept"] = deps.clip_text(data.get("concept", ""), 120)
        payload["filters"] = _sanitize_for_prompt(data.get("filters", {}), clip_text=deps.clip_text, key="filters", max_depth=2, max_list_items=6)
        if rows:
            first = rows[0] if isinstance(rows[0], dict) else {}
            title = str(first.get("display_title") or first.get("title") or "").strip()
            extract = deps.clip_text(str(first.get("extract") or ""), 400)
            if title or extract:
                payload["top_result"] = {"title": title, "extract": extract}
    if exec_result.tool == TOOL_SEARCH_BY_CREATOR and isinstance(data, dict):
        payload["canonical_creator"] = str(data.get("canonical_creator") or "").strip()
        payload["found"] = bool(data.get("found"))
        payload["works_count"] = int(data.get("works_count") or 0)
        works = data.get("results") if isinstance(data.get("results"), list) else []
        payload["works_preview"] = _sanitize_for_prompt(works[:6], clip_text=deps.clip_text, key="data", max_depth=2, max_list_items=6, max_dict_items=6)
    return payload


def _format_media_row_for_prompt(row: dict[str, Any], deps: PromptAssemblyDeps) -> str:
    title = str(row.get("display_title") or row.get("title") or "").strip() or "未命名条目"
    media_type = str(row.get("media_type") or "").strip()
    author = str(row.get("author") or row.get("creator") or "").strip()
    date = str(row.get("date") or row.get("release_date") or "").strip()
    rating = deps.format_media_rating(row.get("rating")) if row.get("rating") is not None else ""
    review = deps.clip_text(str(row.get("review") or row.get("comment") or row.get("overview") or row.get("extract") or "").strip(), 120)
    parts = [f"《{title}》"]
    if media_type:
        parts.append(media_type)
    if author:
        parts.append(f"作者/创作者={author}")
    if date:
        parts.append(f"日期={date}")
    if rating:
        parts.append(f"评分={rating}")
    line = " | ".join(parts)
    if review:
        line += f" | 摘要={review}"
    return line


def _format_media_tool_result(exec_result: ToolExecution, deps: PromptAssemblyDeps, *, max_chars: int) -> str:
    data = exec_result.data if isinstance(exec_result.data, dict) else {}
    main_rows = deps.get_media_main_result_rows_from_data(data)
    mention_rows = deps.get_media_mention_rows_from_data(data)
    source_counts = data.get("source_counts") if isinstance(data.get("source_counts"), dict) else {}
    query_text = str(data.get("query") or data.get("normalized_query") or "").strip()
    lines = [f"工具: {exec_result.tool} | 状态: {exec_result.status}"]
    if exec_result.summary:
        lines.append(f"摘要: {deps.clip_text(exec_result.summary, 180)}")
    if query_text:
        lines.append(f"检索词: {deps.clip_text(query_text, 120)}")
    lines.append(f"主结果数: {len(main_rows)}")
    if mention_rows:
        lines.append(f"提及结果数: {len(mention_rows)}")
    if source_counts:
        ordered_sources = sorted(
            ((str(key).strip(), int(value or 0)) for key, value in source_counts.items() if str(key).strip()),
            key=lambda item: (-item[1], item[0]),
        )
        if ordered_sources:
            lines.append("来源分布: " + ", ".join(f"{name}:{count}" for name, count in ordered_sources[:4]))
    if main_rows:
        lines.append("主结果预览:")
        for row in main_rows[:4]:
            if isinstance(row, dict):
                lines.append(f"- {_format_media_row_for_prompt(row, deps)}")
    if mention_rows:
        preview_titles = []
        for row in mention_rows[:4]:
            if not isinstance(row, dict):
                continue
            title = str(row.get("display_title") or row.get("title") or "").strip()
            if title:
                preview_titles.append(title)
        if preview_titles:
            lines.append("提及条目: " + " / ".join(preview_titles))
    rendered = "\n".join(lines)
    return deps.clip_text(rendered, max_chars)


def _format_tool_result(exec_result: ToolExecution, deps: PromptAssemblyDeps, *, max_chars: int = PROMPT_TOOL_RESULT_MAX_CHARS) -> str:
    if exec_result.tool in {TOOL_QUERY_MEDIA, TOOL_SEARCH_BY_CREATOR}:
        return _format_media_tool_result(exec_result, deps, max_chars=max_chars)
    profiles = [
        {"max_depth": 3, "max_list_items": 5, "max_dict_items": 12},
        {"max_depth": 2, "max_list_items": 4, "max_dict_items": 10},
        {"max_depth": 2, "max_list_items": 3, "max_dict_items": 8},
    ]
    for profile in profiles:
        payload = {
            "tool": exec_result.tool,
            "status": exec_result.status,
            "summary": deps.clip_text(exec_result.summary, 240),
            "data": _sanitize_for_prompt(exec_result.data, clip_text=deps.clip_text, key="data", **profile),
        }
        rendered = json.dumps(payload, ensure_ascii=False)
        if len(rendered) <= max_chars:
            return rendered
    return json.dumps(_minimal_prompt_tool_payload(exec_result, deps), ensure_ascii=False)


def _build_tool_context_parts(tool_results: list[ToolExecution], deps: PromptAssemblyDeps, *, max_total_chars: int) -> list[str]:
    parts = ["工具返回结果："]
    remaining = max_total_chars - len(parts[0])
    rendered_count = 0
    for index, result in enumerate(tool_results):
        remaining_tools = max(1, len(tool_results) - index)
        if remaining <= 80:
            break
        per_tool_budget = max(PROMPT_TOOL_RESULT_MIN_CHARS, min(PROMPT_TOOL_RESULT_MAX_CHARS, int(remaining / remaining_tools)))
        block = _format_tool_result(result, deps, max_chars=per_tool_budget)
        if len(block) + 1 > remaining:
            block = _format_tool_result(result, deps, max_chars=max(PROMPT_TOOL_RESULT_MIN_CHARS, remaining - 32))
        if len(block) + 1 > remaining:
            break
        parts.append(block)
        rendered_count += 1
        remaining -= len(block) + 1
    omitted = max(0, len(tool_results) - rendered_count)
    if omitted > 0 and remaining > 32:
        parts.append(f"还有 {omitted} 个工具结果因上下文预算被省略。")
    return parts


def _build_compact_tool_summary_lines(tool_results: list[ToolExecution], deps: PromptAssemblyDeps) -> list[str]:
    lines: list[str] = []
    for result in tool_results:
        data = result.data if isinstance(result.data, dict) else {}
        result_count = len(deps.get_media_main_result_rows_from_data(data)) if result.tool == TOOL_QUERY_MEDIA else (len(data.get("results") or []) if isinstance(data.get("results"), list) else 0)
        mention_count = len(deps.get_media_mention_rows_from_data(data)) if result.tool == TOOL_QUERY_MEDIA else 0
        source_counts = data.get("source_counts") if isinstance(data.get("source_counts"), dict) else {}
        source_suffix = ""
        if source_counts:
            ordered = sorted(
                ((str(key), int(value or 0)) for key, value in source_counts.items() if str(key).strip()),
                key=lambda item: (-item[1], item[0]),
            )
            source_suffix = " | sources=" + ", ".join(f"{name}:{count}" for name, count in ordered)
        mention_suffix = f" | mentions={mention_count}" if mention_count > 0 else ""
        lines.append(f"- {result.tool} | {result.status} | results={result_count}{mention_suffix}{source_suffix}")
    return lines


def _compose_response_sections(
    tool_results: list[ToolExecution],
    answer_strategy: Any | None,
    deps: PromptAssemblyDeps,
) -> dict[str, Any]:
    if answer_strategy is None:
        return {"local_lines": [], "external_lines": [], "has_external": False}
    style = getattr(answer_strategy, "style_hints", None) or {}
    response_structure = str(style.get("response_structure") or "")
    if response_structure not in {
        "local_list_plus_external_background",
        "curated_collection_synthesis",
        "local_record_plus_external_info",
        "local_list",
        "local_review_list",
        "compare",
        "thematic_list",
    }:
        return {"local_lines": [], "external_lines": [], "has_external": False}

    focus_item_limit = max(1, int(style.get("focus_item_limit") or 10))
    overview_item_limit = max(focus_item_limit, int(style.get("overview_item_limit") or 16))
    synthesis_first = response_structure in {
        "curated_collection_synthesis",
        "local_list_plus_external_background",
        "local_review_list",
        "compare",
        "thematic_list",
    }

    def _build_collection_overview(rows: list[dict[str, Any]]) -> list[str]:
        usable_rows = [row for row in rows if isinstance(row, dict)]
        if not usable_rows or not synthesis_first:
            return []
        titles = [f"《{str(row.get('title') or '').strip()}》" for row in usable_rows if str(row.get("title") or "").strip()]
        titles = [title for title in titles if title]
        if not titles:
            return []
        shown_titles = "、".join(titles[:overview_item_limit])
        selected_count = min(len(usable_rows), focus_item_limit)
        summary_line = f"- 集合概览：本地结果共 {len(usable_rows)} 条；总述里可优先点名 {shown_titles}"
        if len(titles) > overview_item_limit:
            summary_line += f" 等 {len(titles)} 部作品。"
        else:
            summary_line += "。"
        requirement = (
            f"- 展开要求：正文必须展开 {selected_count} 条代表项；请明确交代这是从 {len(usable_rows)} 条本地命中里挑出的重点样本，其余只在总述中概括提及。"
            if len(usable_rows) >= selected_count
            else f"- 展开要求：正文按实际命中展开 {len(usable_rows)} 条代表项；请明确交代结果不足 {selected_count} 条。"
        )
        return [summary_line, requirement]

    def _append_row_evidence(target: list[str], row: dict[str, Any]) -> None:
        title = str(row.get("title") or "").strip()
        if not title:
            return
        date = str(row.get("date") or "").strip()
        rating = row.get("rating")
        comment = deps.clip_text(str(row.get("comment") or row.get("review") or ""), 80)
        target.append(f"- 本地记录证据：标题=《{title}》")
        if date:
            target.append(f"  日期={date}")
        if rating is not None:
            target.append(f"  评分={deps.format_media_rating(rating)}")
        if comment:
            target.append(f"  短评={comment}")

    local_lines: list[str] = []
    external_lines: list[str] = []
    per_item_buckets: list[str] = []
    fanout_result = next(
        (
            result
            for result in tool_results
            if result.status in {"ok", "partial"}
            and isinstance(result.data, dict)
            and result.data.get("per_item_fanout")
        ),
        None,
    )
    fanout_data: list[dict[str, Any]] = []
    if fanout_result is not None and isinstance(fanout_result.data, dict):
        raw = fanout_result.data.get("per_item_data") or fanout_result.data.get("results") or []
        fanout_data = [item for item in raw if isinstance(item, dict)]

    if fanout_data:
        external_by_title: dict[str, dict[str, Any]] = {}
        for item in fanout_data:
            key = deps.normalize_title_for_match(str(item.get("local_title") or item.get("_source_title") or ""))
            if key:
                external_by_title[key] = item

        local_rows: list[dict[str, Any]] = []
        for result in tool_results:
            if result.status not in {"ok", "partial"} or not isinstance(result.data, dict):
                continue
            if result.tool in {TOOL_QUERY_MEDIA, TOOL_SEARCH_BY_CREATOR}:
                raw_rows = deps.get_media_main_result_rows_from_data(result.data) if result.tool == TOOL_QUERY_MEDIA else (result.data.get("results") or [])
                local_rows.extend([row for row in raw_rows if isinstance(row, dict)])

        local_lines.extend(_build_collection_overview(local_rows))
        focus_rows = local_rows[:focus_item_limit] if synthesis_first else local_rows

        for row in focus_rows:
            title = str(row.get("title") or "").strip()
            if not title:
                continue
            date = str(row.get("date") or "").strip()
            rating = row.get("rating")
            comment = deps.clip_text(str(row.get("comment") or row.get("review") or ""), 80)

            block: list[str] = [f"- 本地记录证据：标题=《{title}》"]
            if date:
                block.append(f"  日期={date}")
            if rating is not None:
                block.append(f"  评分={deps.format_media_rating(rating)}")
            if comment:
                block.append(f"  短评={comment}")

            external_item = external_by_title.get(deps.normalize_title_for_match(title))
            if external_item:
                external_source = str(external_item.get("external_source") or "wiki")
                if external_source == "bangumi":
                    source_label = "Bangumi"
                elif external_source == "tmdb":
                    source_label = "TMDB"
                else:
                    source_label = "Wiki"
                confidence = float(external_item.get("match_confidence") or 0.0)
                overview = deps.clip_text(str(external_item.get("external_overview") or external_item.get("overview") or ""), 180)
                if overview:
                    line = f"  外部补充({source_label})={overview}"
                    if confidence < 0.6:
                        line += "（可能非精确匹配）"
                    block.append(line)

            per_item_buckets.append("\n".join(block))

        if synthesis_first and len(local_rows) > len(focus_rows):
            local_lines.append(f"- 其余 {len(local_rows) - len(focus_rows)} 条本地结果仅用于总述，不要在正文逐条展开。")

        return {
            "local_lines": local_lines,
            "external_lines": external_lines,
            "per_item_buckets": per_item_buckets,
            "has_external": bool(per_item_buckets),
        }

    for result in tool_results:
        if result.status not in {"ok", "partial"}:
            continue
        data = result.data if isinstance(result.data, dict) else {}
        rows: list[Any] = deps.get_media_main_result_rows_from_data(data) if result.tool == TOOL_QUERY_MEDIA else (data.get("results", []) if isinstance(data.get("results"), list) else [])

        if result.tool in {TOOL_QUERY_MEDIA, TOOL_SEARCH_BY_CREATOR}:
            local_lines.extend(_build_collection_overview([row for row in rows if isinstance(row, dict)]))
            focus_rows = rows[:focus_item_limit] if synthesis_first else rows
            for row in focus_rows:
                if not isinstance(row, dict):
                    continue
                if response_structure == "compare":
                    _append_row_evidence(local_lines, row)
                else:
                    _append_row_evidence(local_lines, row)
            if synthesis_first and len(rows) > len(focus_rows):
                local_lines.append(f"- 其余 {len(rows) - len(focus_rows)} 条本地结果只用于总述和点名，不要在正文逐条复述。")

        elif result.tool == TOOL_SEARCH_TMDB:
            for row in rows[:4]:
                if not isinstance(row, dict):
                    continue
                title = str(row.get("title") or "").strip()
                overview = deps.clip_text(str(row.get("overview") or ""), 150)
                if title:
                    line = f"【TMDB】《{title}》"
                    if overview:
                        line += f"：{overview}"
                    external_lines.append(line)

        elif result.tool in {TOOL_EXPAND_MEDIAWIKI_CONCEPT, TOOL_SEARCH_MEDIAWIKI, TOOL_PARSE_MEDIAWIKI}:
            wiki_rows: list[dict[str, Any]] = []
            if result.tool == TOOL_PARSE_MEDIAWIKI:
                page = data.get("page")
                if isinstance(page, dict) and (page.get("title") or page.get("extract")):
                    wiki_rows = [page]
                elif rows:
                    wiki_rows = [row for row in rows[:2] if isinstance(row, dict)]
            else:
                wiki_rows = [row for row in rows[:2] if isinstance(row, dict)]
            for row in wiki_rows:
                wiki_title = str(row.get("display_title") or row.get("title") or "").strip()
                extract = deps.clip_text(str(row.get("extract") or ""), 250)
                if wiki_title or extract:
                    label = f"【Wiki】{wiki_title}" if wiki_title else "【Wiki】"
                    external_lines.append(f"{label}：{extract}" if extract else label)

    return {
        "local_lines": local_lines,
        "external_lines": external_lines,
        "per_item_buckets": per_item_buckets,
        "has_external": bool(external_lines),
    }


def _clip_memory_context(memory_context: str, deps: PromptAssemblyDeps) -> str:
    return deps.clip_text(memory_context, PROMPT_MEMORY_MAX_CHARS)


def _trim_history_for_prompt(
    history: list[dict[str, str]],
    deps: PromptAssemblyDeps,
    *,
    followup_mode: str = "none",
    carry_over_from_previous_turn: bool = False,
) -> list[str]:
    if str(followup_mode or "none") == "none" and not carry_over_from_previous_turn:
        return []

    hist_lines: list[str] = []
    for msg in (history or [])[-PROMPT_HISTORY_MAX_MESSAGES:]:
        role = str(msg.get("role", "")).strip()
        content = deps.clip_text(str(msg.get("content", "")).strip(), PROMPT_HISTORY_ITEM_MAX_CHARS)
        if content:
            hist_lines.append(f"{role}: {content}")
    return hist_lines


def _build_system_prompt(
    *,
    answer_strategy: Any | None,
    tool_results: list[ToolExecution],
    normalized_search_mode: str,
    has_web_tool: bool,
    followup_mode: str = "none",
    carry_over_from_previous_turn: bool = False,
) -> str:
    return build_answer_system_prompt(
        answer_strategy=answer_strategy,
        tool_results=tool_results,
        normalized_search_mode=normalized_search_mode,
        has_web_tool=has_web_tool,
        followup_mode=followup_mode,
        carry_over_from_previous_turn=carry_over_from_previous_turn,
    )


def _preferred_answer_temperature(answer_strategy: Any | None) -> float:
    if answer_strategy is None:
        return 0.2
    style = getattr(answer_strategy, "style_hints", {}) or {}
    response_structure = str(style.get("response_structure") or "")
    if response_structure in {"compare", "curated_collection_synthesis", "local_record_plus_external_info", "local_review_list"}:
        return 0.35
    return 0.2


def summarize_answer(
    *,
    question: str,
    history: list[dict[str, str]],
    memory_context: str,
    tool_results: list[ToolExecution],
    backend: str,
    search_mode: str,
    quota_state: dict[str, Any],
    trace_id: str,
    deps: PromptAssemblyDeps,
    debug_sink: dict[str, Any] | None = None,
    llm_stats_sink: dict[str, Any] | None = None,
    answer_strategy: Any | None = None,
    stream_callback: Callable[[str], None] | None = None,
    timing_sink: dict[str, float] | None = None,
    followup_mode: str = "none",
    carry_over_from_previous_turn: bool = False,
) -> str:
    timing_data = timing_sink if isinstance(timing_sink, dict) else {}
    structured_context_t0 = _time.perf_counter()
    hist_lines = _trim_history_for_prompt(
        history,
        deps,
        followup_mode=followup_mode,
        carry_over_from_previous_turn=carry_over_from_previous_turn,
    )
    clipped_memory_context = _clip_memory_context(memory_context, deps)
    answer_focus_hints = deps.build_answer_focus_hints(question, tool_results)

    normalized_search_mode = deps.normalize_search_mode(search_mode)
    has_web_tool = any(result.tool == TOOL_SEARCH_WEB for result in tool_results)
    system_prompt = _build_system_prompt(
        answer_strategy=answer_strategy,
        tool_results=tool_results,
        normalized_search_mode=normalized_search_mode,
        has_web_tool=has_web_tool,
        followup_mode=followup_mode,
        carry_over_from_previous_turn=carry_over_from_previous_turn,
    )

    compose_t0 = _time.perf_counter()
    composed = _compose_response_sections(tool_results, answer_strategy, deps)
    timing_data["response_section_compose_seconds"] = round(float(_time.perf_counter() - compose_t0), 6)
    timing_data["structured_context_build_seconds"] = round(float(_time.perf_counter() - structured_context_t0), 6)

    budgets = [PROMPT_TOOL_CONTEXT_MAX_CHARS, PROMPT_TOOL_CONTEXT_RETRY_CHARS, 1800]
    answer = ""
    last_exc: Exception | None = None
    total_calls = 0
    total_input_tokens = 0
    total_prompt_tokens = 0
    total_context_tokens = 0
    total_prompt_render_seconds = 0.0
    final_debug_request: dict[str, Any] | None = None

    for budget in budgets:
        prompt_render_t0 = _time.perf_counter()
        context_parts = _build_tool_context_parts(tool_results, deps, max_total_chars=budget)
        reference_hint_lines = deps.build_reference_hint_lines(tool_results)
        prompt_blocks = hist_lines + [f"当前问题: {question}"]
        if clipped_memory_context:
            prompt_blocks.extend(["", clipped_memory_context])
        if answer_focus_hints:
            prompt_blocks.extend(["", "回答提示：", answer_focus_hints])
        per_item_buckets = composed.get("per_item_buckets") or []
        if per_item_buckets:
            compact_tool_results = [
                item
                for item in tool_results
                if item.tool not in {
                    TOOL_QUERY_MEDIA,
                    TOOL_SEARCH_BY_CREATOR,
                    TOOL_SEARCH_TMDB,
                    TOOL_SEARCH_BANGUMI,
                    TOOL_EXPAND_MEDIAWIKI_CONCEPT,
                    TOOL_SEARCH_MEDIAWIKI,
                    TOOL_PARSE_MEDIAWIKI,
                }
            ]
            context_parts = _build_tool_context_parts(compact_tool_results, deps, max_total_chars=budget) if compact_tool_results else []
            prompt_blocks.extend(["", "[已整理的逐项证据]"])
            prompt_blocks.extend(["外部信息来源标注说明：Bangumi=bgm.tv动画数据库，TMDB=影视数据库，Wiki=维基百科。"])
            prompt_blocks.extend(["注意：标注【外部信息可能非精确匹配】的条目请谨慎使用其外部简介。"])
            prompt_blocks.extend(["", "[工具摘要]"])
            prompt_blocks.extend(_build_compact_tool_summary_lines(tool_results, deps))
            prompt_blocks.extend(per_item_buckets)
        else:
            if composed["local_lines"]:
                prompt_blocks.extend(["", "[已整理的本地证据]"])
                prompt_blocks.extend(composed["local_lines"])
            if composed["external_lines"]:
                prompt_blocks.extend(["", "[已整理的外部证据]"])
                prompt_blocks.extend(composed["external_lines"])
        prompt_blocks.extend(["", *context_parts])
        user_prompt = "\n".join(prompt_blocks)
        total_prompt_render_seconds += _time.perf_counter() - prompt_render_t0
        context_tokens_est = deps.approx_tokens("\n".join(context_parts))
        input_tokens_est = deps.approx_tokens(system_prompt) + deps.approx_tokens(user_prompt)
        prompt_tokens_est = max(0, input_tokens_est - context_tokens_est)
        final_debug_request = {
            "trace_id": trace_id,
            "trace_stage": "agent.llm.summarize",
            "backend": backend,
            "system_prompt": system_prompt,
            "user_prompt": user_prompt,
            "memory_tokens_est": deps.approx_tokens(clipped_memory_context),
            "input_tokens_est": input_tokens_est,
            "prompt_tokens_est": prompt_tokens_est,
            "context_tokens_est": context_tokens_est,
            "tool_context_budget_chars": budget,
        }
        try:
            total_calls += 1
            total_input_tokens += input_tokens_est
            total_prompt_tokens += prompt_tokens_est
            total_context_tokens += context_tokens_est
            llm_messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ]
            if stream_callback is not None:
                chunks: list[str] = []
                preferred_temperature = _preferred_answer_temperature(answer_strategy)
                for delta in deps.llm_chat_stream(
                    messages=llm_messages,
                    backend=backend,
                    quota_state=quota_state,
                    temperature=preferred_temperature,
                    usage_feature="nav_dashboard.agent.answer_stream",
                    usage_page="dashboard_agent",
                    usage_message=question,
                    trace_id=trace_id,
                ):
                    if not delta:
                        continue
                    chunks.append(delta)
                    stream_callback(delta)
                answer = "".join(chunks).strip()
            else:
                preferred_temperature = _preferred_answer_temperature(answer_strategy)
                answer = deps.llm_chat(
                    messages=llm_messages,
                    backend=backend,
                    quota_state=quota_state,
                    temperature=preferred_temperature,
                    usage_feature="nav_dashboard.agent.answer",
                    usage_page="dashboard_agent",
                    usage_message=question,
                    trace_id=trace_id,
                )
            if stream_callback is None and normalized_search_mode == "local_only" and not has_web_tool and re.search(r"联网搜索|网络搜索|进行网络搜索|经过搜索", answer):
                total_calls += 1
                total_input_tokens += input_tokens_est
                total_prompt_tokens += prompt_tokens_est
                total_context_tokens += context_tokens_est
                answer = deps.llm_chat(
                    messages=[
                        {
                            "role": "system",
                            "content": system_prompt + "你刚才错误声称做了联网搜索。现在请仅基于当前工具结果重写答案，不要提到网络或搜索引擎。",
                        },
                        {"role": "user", "content": user_prompt},
                    ],
                    backend=backend,
                    quota_state=quota_state,
                    temperature=_preferred_answer_temperature(answer_strategy),
                    usage_feature="nav_dashboard.agent.answer_rewrite",
                    usage_page="dashboard_agent",
                    usage_message=question,
                    trace_id=trace_id,
                )
            last_exc = None
            break
        except Exception as exc:
            last_exc = exc
            if deps.is_context_length_error(exc) and budget != budgets[-1]:
                continue
            raise

    if last_exc is not None:
        raise last_exc

    if debug_sink is not None and final_debug_request is not None:
        debug_sink["llm_request"] = final_debug_request
    if llm_stats_sink is not None:
        llm_stats_sink["backend"] = backend
        llm_stats_sink["input_tokens_est"] = total_input_tokens
        llm_stats_sink["prompt_tokens_est"] = total_prompt_tokens
        llm_stats_sink["context_tokens_est"] = total_context_tokens
        llm_stats_sink["memory_tokens_est"] = deps.approx_tokens(clipped_memory_context)
        llm_stats_sink["calls"] = total_calls
    if debug_sink is not None:
        debug_sink["llm_response"] = {
            "trace_id": trace_id,
            "trace_stage": "agent.llm.summarize",
            "output_tokens_est": deps.approx_tokens(answer),
        }
    if llm_stats_sink is not None:
        llm_stats_sink["output_tokens_est"] = deps.approx_tokens(answer)
    timing_data["prompt_render_seconds"] = round(float(total_prompt_render_seconds or 0), 6)
    return answer