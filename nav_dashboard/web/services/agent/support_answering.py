from __future__ import annotations

import re
import urllib.parse as urlparse
from typing import Any, Callable, Iterator

from nav_dashboard.web.services.agent.guardrail_flags_owner import GuardrailFlagDeps, build_guardrail_flags as owner_build_guardrail_flags
from nav_dashboard.web.services.answer.prompt_assembly import PromptAssemblyDeps, summarize_answer as assemble_prompted_answer
from nav_dashboard.web.services.media.media_answer_renderer import (
    append_media_mentions_to_answer as media_append_mentions_to_answer,
    build_media_mention_appendix as media_build_mention_appendix,
    build_structured_media_answer as media_build_structured_answer,
    build_structured_media_answer_chunks as media_build_structured_answer_chunks,
    build_structured_media_external_item_block as media_build_external_item_block,
    build_structured_media_mention_block as media_build_mention_block,
)
from nav_dashboard.web.services.media.media_fast_path import build_media_answer_render_deps as build_media_fast_path_render_deps
from nav_dashboard.web.services.media.media_policy_rules import is_personal_review_mode as is_personal_media_review_mode
from nav_dashboard.web.services.media.media_policy_rules import should_render_external_appendix, should_render_mentions
from nav_dashboard.web.services.media.media_render_contract import (
    MediaRenderContract,
    MediaRenderContractBuilderDeps,
    build_followup_answer_note as media_build_followup_answer_note,
    build_media_render_contract as media_build_render_contract,
)
from nav_dashboard.web.services.media.media_retrieval_service import describe_planner_scope as media_describe_planner_scope
from nav_dashboard.web.services.media.media_retrieval_service import get_media_main_result_rows_from_data as media_get_main_result_rows_from_data
from nav_dashboard.web.services.media.media_retrieval_service import get_media_mention_rows_from_data as media_get_mention_rows_from_data
from nav_dashboard.web.services.media.media_retrieval_service import normalize_media_match_terms as media_normalize_match_terms

from .domain import media_core
from .runtime import composition as _composition
from .support_common import (
    QUERY_TYPE_MEDIA,
    TOOL_EXPAND_MEDIAWIKI_CONCEPT,
    TOOL_PARSE_MEDIAWIKI,
    TOOL_QUERY_MEDIA,
    TOOL_SEARCH_BANGUMI,
    TOOL_SEARCH_BY_CREATOR,
    TOOL_SEARCH_MEDIAWIKI,
    TOOL_SEARCH_TMDB,
    TOOL_SEARCH_WEB,
    AgentRuntimeState,
    ToolExecution,
    _clip_text,
    _compat_get_query_type,
    _get_lookup_mode_from_state,
    _get_planner_snapshot_from_runtime,
    _get_resolved_query_state_from_runtime,
    _library_tracker_reference_base,
    _router_helpers_compat,
)
from .support_retrieval import (
    _get_media_mention_rows,
    _get_media_result_rows,
    _get_media_validation,
    _get_per_item_expansion_stats,
    _score_value,
)
from .infra import runtime_infra


def _is_context_length_error(exc: Exception) -> bool:
    text = str(exc or "").lower()
    return any(token in text for token in ("context length", "maximum context", "too many tokens", "context_window_exceeded"))


def _format_media_rating(value: Any) -> str:
    if value in {None, ""}:
        return ""
    try:
        number = float(value)
    except Exception:
        return str(value).strip()
    if number.is_integer():
        return f"{int(number)}/10"
    return f"{number:.1f}/10"


def _build_answer_focus_hints(question: str, tool_results: list[ToolExecution]) -> str:
    lines: list[str] = []
    normalized_question = str(question or "")
    wants_when = any(token in normalized_question for token in ["什么时候", "何时", "哪天", "哪一年", "观影时间", "时间"])
    wants_summary = any(token in normalized_question for token in ["剧情", "简介", "介绍", "讲了什么"])

    media_result = next((item for item in tool_results if item.tool == TOOL_QUERY_MEDIA), None)
    creator_result = next((item for item in tool_results if item.tool == TOOL_SEARCH_BY_CREATOR), None)
    tmdb_result = next((item for item in tool_results if item.tool == TOOL_SEARCH_TMDB), None)
    mediawiki_result = next((item for item in tool_results if item.tool in {TOOL_SEARCH_MEDIAWIKI, TOOL_PARSE_MEDIAWIKI, TOOL_EXPAND_MEDIAWIKI_CONCEPT}), None)

    if creator_result and isinstance(creator_result.data, dict):
        canonical = str(creator_result.data.get("canonical_creator") or "").strip()
        works = creator_result.data.get("results") if isinstance(creator_result.data.get("results"), list) else []
        if canonical and works:
            lines.append(f"本地创作者检索命中：「{canonical}」，共找到 {len(works)} 部作品，请基于这些作品列表作答。")
            lines.append("如无特殊说明，回答应列出本地库中的作品，外部 Wiki 信息作补充标注。")

    if media_result and isinstance(media_result.data, dict):
        exact_match = media_result.data.get("top_exact_match") if isinstance(media_result.data.get("top_exact_match"), dict) else None
        family_matches = media_result.data.get("top_family_matches") if isinstance(media_result.data.get("top_family_matches"), list) else []
        mention_rows = _get_media_mention_rows([media_result])
        layer_breakdown = media_result.data.get("layer_breakdown") if isinstance(media_result.data.get("layer_breakdown"), dict) else {}
        alias_resolution = media_result.data.get("alias_resolution") if isinstance(media_result.data.get("alias_resolution"), dict) else {}
        alias_entries = alias_resolution.get("entries") if isinstance(alias_resolution.get("entries"), list) else []
        alias_hits = alias_resolution.get("hits") if isinstance(alias_resolution.get("hits"), list) else alias_entries
        if alias_hits:
            alias_terms: list[str] = []
            for entry in alias_entries[:3]:
                if not isinstance(entry, dict):
                    continue
                matched_text = str(entry.get("matched_text") or "").strip()
                raw_value = str(entry.get("raw_value") or "").strip()
                if matched_text and raw_value:
                    alias_terms.append(f"{matched_text}→{raw_value}")
            if alias_terms:
                lines.append(f"本地 alias 映射已命中：{' / '.join(alias_terms)}。请把这些本地条目视为有效命中继续回答。")
        if exact_match:
            title = str(exact_match.get("title") or "").strip()
            date = str(exact_match.get("date") or "").strip()
            rating = exact_match.get("rating")
            review = str(exact_match.get("review") or "").strip()
            lines.append("本地知识库优先：如果命中明确媒体条目，先回答本地库中的观看日期、评分和个人短评，再补充外部信息。")
            if title:
                lines.append(f"本地精确命中标题：{title}")
            if wants_when and date:
                lines.append(f"本地记录的观看日期：{date}")
            if rating not in {None, ""}:
                lines.append(f"本地记录评分：{rating}")
            if review:
                lines.append(f"本地短评摘要：{_clip_text(review, 180)}")
        elif media_result.data.get("media_entities"):
            family_titles = [str(item.get("title") or "").strip() for item in family_matches if isinstance(item, dict) and str(item.get("title") or "").strip()]
            if family_titles:
                lines.append("本地媒体库命中了同一作品族的多个相关条目，优先围绕这些条目回答，不要跳到无关作品。")
                lines.append(f"同系列相关条目：{' / '.join(family_titles[:3])}")
        personal_review_scope = is_personal_media_review_mode(
            query_class=str(media_result.data.get("query_class") or "").strip(),
            subject_scope=str(media_result.data.get("subject_scope") or "").strip(),
            answer_shape=str(media_result.data.get("answer_shape") or "").strip(),
        )
        if mention_rows and bool(layer_breakdown.get("strict_scope_active")) and not personal_review_scope:
            mention_titles = [str(item.get("title") or "").strip() for item in mention_rows if str(item.get("title") or "").strip()]
            lines.append("下列条目只是你在其他条目的短评或附属字段里提到过该系列，不属于系列主结果。")
            if mention_titles:
                lines.append(f"扩展提及条目：{' / '.join(mention_titles[:4])}")

    if wants_summary and tmdb_result and isinstance(tmdb_result.data, dict):
        rows = tmdb_result.data.get("results", []) if isinstance(tmdb_result.data.get("results"), list) else []
        if rows:
            top = rows[0] if isinstance(rows[0], dict) else {}
            overview = str(top.get("overview") or "").strip()
            title = str(top.get("title") or "").strip()
            if overview:
                lines.append("TMDB 外部摘要仅在确实补强回答主线时再吸收一句；如果没有增益，直接忽略，不要解释。")
                if title:
                    lines.append(f"TMDB 命中标题：{title}")
                lines.append(f"TMDB 简介摘要：{_clip_text(overview, 220)}")

    if mediawiki_result and isinstance(mediawiki_result.data, dict):
        rows = mediawiki_result.data.get("results", []) if isinstance(mediawiki_result.data.get("results"), list) else []
        if rows:
            first = rows[0] if isinstance(rows[0], dict) else {}
            extract = str(first.get("extract") or "").strip()
            title = str(first.get("display_title") or first.get("title") or "").strip()
            if len(extract) > 100:
                title_prefix = f"「{title}」" if title else "该条目"
                lines.append(f"Wiki 外部摘要 {title_prefix}：" + _clip_text(extract, 400))

    return "\n".join(lines).strip()


def _build_references_markdown(
    tool_results: list[ToolExecution],
    *,
    request_base_url: str = "",
    include_citation_line: bool = True,
) -> str:
    library_ref_base = _library_tracker_reference_base(request_base_url)
    doc_refs: list[tuple[float, str, str]] = []
    media_refs: list[tuple[float, str, str]] = []
    external_refs: list[tuple[float, str, str]] = []
    seen_refs: set[tuple[str, str]] = set()

    def _append_reference(bucket: list[tuple[float, str, str]], score: float | None, label: str, url: str = "") -> None:
        normalized_label = str(label or "").strip()
        if not normalized_label:
            return
        dedupe_key = (normalized_label, str(url or "").strip())
        if dedupe_key in seen_refs:
            return
        seen_refs.add(dedupe_key)
        bucket.append((float(score or 0.0), normalized_label, str(url or "").strip()))

    def _render_reference_section(title: str, refs: list[tuple[float, str, str]], start_index: int) -> tuple[int, str, list[str]]:
        lines: list[str] = []
        inline_links: list[str] = []
        next_index = start_index
        for _score, label, url in refs:
            linked = f"[{label}]({url})" if url else label
            lines.append(f"[{next_index}] {linked}".strip())
            inline_links.append(f"[{next_index}]({url})" if url else f"[{next_index}]")
            next_index += 1
        return next_index, f"### {title}\n" + "\n".join(lines), inline_links

    for result in tool_results:
        if not isinstance(result.data, dict):
            continue
        rows = result.data.get("results", [])
        if not isinstance(rows, list):
            continue

        if result.tool in {TOOL_QUERY_MEDIA, TOOL_SEARCH_BY_CREATOR}:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                title = str(row.get("title", "")).strip()
                media_type = str(row.get("media_type", "")).strip()
                item_id = str(row.get("id", "")).strip()
                score = _score_value(row)
                if not title or score is None:
                    continue
                label = f"本地媒体库: {title}{(' (' + media_type + ')') if media_type else ''} ({score:.4f})"
                url = f"{library_ref_base}/?item={urlparse.quote(item_id)}" if item_id else ""
                _append_reference(media_refs, score, label, url)
        elif result.tool == TOOL_SEARCH_WEB:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                title = str(row.get("title", row.get("url", "网页"))).strip() or "网页"
                url = str(row.get("url", "")).strip()
                score = _score_value(row)
                if not url or score is None:
                    continue
                _append_reference(external_refs, score, f"外部网页: {title} ({score:.4f})", url)
        elif result.tool in {TOOL_SEARCH_MEDIAWIKI, TOOL_PARSE_MEDIAWIKI, TOOL_EXPAND_MEDIAWIKI_CONCEPT, TOOL_SEARCH_TMDB, TOOL_SEARCH_BANGUMI}:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                title = str(row.get("display_title") or row.get("title") or row.get("name_cn") or row.get("name") or "外部资料").strip()
                url = str(row.get("url", "")).strip()
                score = _score_value(row)
                if not url:
                    continue
                _append_reference(external_refs, score, f"外部参考: {title}" + (f" ({score:.4f})" if score is not None else ""), url)

    if not doc_refs and not media_refs and not external_refs:
        return ""

    sections: list[str] = []
    next_index = 1
    if media_refs:
        next_index, section_text, _ = _render_reference_section("本地媒体库参考", list(media_refs), next_index)
        sections.append(section_text)
    if doc_refs:
        next_index, section_text, _ = _render_reference_section("本地文档参考", sorted(doc_refs, key=lambda x: x[0], reverse=True), next_index)
        sections.append(section_text)
    if external_refs:
        _next_index, section_text, _ = _render_reference_section("外部参考", sorted(external_refs, key=lambda x: x[0], reverse=True), next_index)
        sections.append(section_text)

    citation_line = ""
    prefix = f"\n\n{citation_line}\n\n" if citation_line and include_citation_line else "\n\n"
    return prefix + "\n\n".join(sections)


def _build_reference_hint_lines(tool_results: list[ToolExecution]) -> list[str]:
    references = _build_references_markdown(tool_results, include_citation_line=False)
    if not references:
        return []
    hint_lines: list[str] = []
    current_heading = ""
    for raw_line in str(references or "").splitlines():
        line = str(raw_line or "").strip()
        if not line:
            continue
        if line.startswith("### "):
            current_heading = line.replace("###", "", 1).strip()
            continue
        match = re.match(r"^\[(\d+)\]\s+(.+)$", line)
        if not match:
            continue
        index = str(match.group(1) or "").strip()
        body = str(match.group(2) or "").strip()
        body = re.sub(r"^\[([^\]]+)\]\([^)]+\)$", r"\1", body)
        source_prefix = "本地条目"
        if current_heading == "外部参考":
            source_prefix = "外部条目"
        elif current_heading == "本地文档参考":
            source_prefix = "本地文档条目"
        hint_lines.append(f"[{index}] 对应{source_prefix}：{body}")
    return hint_lines


def _normalize_media_reference_key(title: str, media_type: str = "") -> str:
    return f"{str(media_type or '').strip().lower()}::{str(title or '').strip().lower()}"


def _build_media_citation_lookup(tool_results: list[ToolExecution], *, request_base_url: str = "") -> dict[str, dict[str, str]]:
    library_ref_base = _library_tracker_reference_base(request_base_url)
    media_refs: list[tuple[str, str, str]] = []
    for result in tool_results:
        if result.tool not in {TOOL_QUERY_MEDIA, TOOL_SEARCH_BY_CREATOR} or not isinstance(result.data, dict):
            continue
        rows = result.data.get("main_results", result.data.get("results", []))
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            title = str(row.get("title", "")).strip()
            media_type = str(row.get("media_type", "")).strip()
            item_id = str(row.get("id", "")).strip()
            if not title:
                continue
            url = f"{library_ref_base}/?item={urlparse.quote(item_id)}" if item_id else ""
            media_refs.append((item_id, _normalize_media_reference_key(title, media_type), url))
    by_id: dict[str, str] = {}
    by_key: dict[str, str] = {}
    next_index = 1
    for item_id, key, url in media_refs:
        marker = f"[[{next_index}]]({url})" if url else f"[{next_index}]"
        inserted = False
        if item_id and item_id not in by_id:
            by_id[item_id] = marker
            inserted = True
        if key and key not in by_key:
            by_key[key] = marker
            inserted = True
        if inserted:
            next_index += 1
    return {"by_id": by_id, "by_key": by_key}


def _build_media_row_citation_lookup(tool_results: list[ToolExecution], *, request_base_url: str = ""):
    lookup = _build_media_citation_lookup(tool_results, request_base_url=request_base_url)
    by_id = dict(lookup.get("by_id") or {})
    by_key = dict(lookup.get("by_key") or {})

    def _lookup(row: dict[str, Any]) -> str:
        if not isinstance(row, dict):
            return ""
        item_id = str(row.get("id") or "").strip()
        if item_id and item_id in by_id:
            return by_id[item_id]
        key = _normalize_media_reference_key(str(row.get("title") or ""), str(row.get("media_type") or ""))
        return by_key.get(key, "")

    return _lookup


def _get_media_answer_render_deps():
    return build_media_fast_path_render_deps(clip_text=_clip_text)


def _get_media_render_contract_builder_deps() -> MediaRenderContractBuilderDeps:
    return MediaRenderContractBuilderDeps(
        query_type_media=QUERY_TYPE_MEDIA,
        tool_query_media=TOOL_QUERY_MEDIA,
        tool_search_by_creator=TOOL_SEARCH_BY_CREATOR,
        tool_expand_mediawiki_concept=TOOL_EXPAND_MEDIAWIKI_CONCEPT,
        tool_search_tmdb=TOOL_SEARCH_TMDB,
        tool_search_bangumi=TOOL_SEARCH_BANGUMI,
        tool_search_mediawiki=TOOL_SEARCH_MEDIAWIKI,
        tool_parse_mediawiki=TOOL_PARSE_MEDIAWIKI,
        tool_search_web=TOOL_SEARCH_WEB,
        normalize_media_match_terms=media_normalize_match_terms,
        get_query_type=_compat_get_query_type,
        get_media_result_rows=_get_media_result_rows,
        get_media_mention_rows=_get_media_mention_rows,
        get_media_validation=_get_media_validation,
        get_per_item_expansion_stats=_get_per_item_expansion_stats,
        get_planner_snapshot_from_runtime=_get_planner_snapshot_from_runtime,
        get_resolved_query_state_from_runtime=_get_resolved_query_state_from_runtime,
        get_lookup_mode_from_state=_get_lookup_mode_from_state,
        describe_planner_scope=media_describe_planner_scope,
        question_requests_personal_evaluation=_router_helpers_compat()._question_requests_personal_evaluation,
        question_requests_media_details=_router_helpers_compat()._question_requests_media_details,
        should_render_mentions=should_render_mentions,
        should_render_external_appendix=should_render_external_appendix,
    )


def _build_media_render_contract(question: str, runtime_state: AgentRuntimeState, tool_results: list[ToolExecution]) -> MediaRenderContract:
    return media_build_render_contract(question, runtime_state, tool_results, deps=_get_media_render_contract_builder_deps())


def _build_followup_answer_note(runtime_state: AgentRuntimeState) -> str:
    return media_build_followup_answer_note(runtime_state, deps=_get_media_render_contract_builder_deps())


def _build_structured_media_external_item_block(row: dict[str, Any], *, include_divider: bool = False) -> str:
    return media_build_external_item_block(row, clip_text=_clip_text, include_divider=include_divider)


def _build_structured_media_answer_chunks(
    question: str,
    runtime_state: AgentRuntimeState,
    tool_results: list[ToolExecution],
    *,
    include_guardrail_explanation: bool = False,
    include_followup_note: bool = False,
    request_base_url: str = "",
) -> list[str]:
    contract = _build_media_render_contract(question, runtime_state, tool_results)
    return media_build_structured_answer_chunks(
        deps=_get_media_answer_render_deps(),
        contract=contract,
        citation_lookup=_build_media_row_citation_lookup(tool_results, request_base_url=request_base_url),
    )


def _build_structured_media_answer(
    question: str,
    runtime_state: AgentRuntimeState,
    tool_results: list[ToolExecution],
    *,
    include_guardrail_explanation: bool = False,
    include_followup_note: bool = False,
    include_external_appendix: bool = True,
    request_base_url: str = "",
) -> str:
    contract = _build_media_render_contract(question, runtime_state, tool_results)
    return media_build_structured_answer(
        deps=_get_media_answer_render_deps(),
        contract=contract,
        include_external_appendix=include_external_appendix,
        citation_lookup=_build_media_row_citation_lookup(tool_results, request_base_url=request_base_url),
    )


def _append_media_mentions_to_answer(answer: str, question: str, runtime_state: AgentRuntimeState, tool_results: list[ToolExecution]) -> str:
    return media_append_mentions_to_answer(answer, deps=_get_media_answer_render_deps(), contract=_build_media_render_contract(question, runtime_state, tool_results))


def _build_guardrail_flag_deps() -> GuardrailFlagDeps:
    return _composition.build_guardrail_flag_deps(
        get_resolved_query_state_from_runtime=_get_resolved_query_state_from_runtime,
        get_lookup_mode_from_state=_get_lookup_mode_from_state,
        get_planner_snapshot_from_runtime=_get_planner_snapshot_from_runtime,
        question_requests_media_details=_router_helpers_compat()._question_requests_media_details,
    )


def _build_guardrail_flags(runtime_state: AgentRuntimeState, media_validation: dict[str, Any]) -> dict[str, bool]:
    return owner_build_guardrail_flags(runtime_state=runtime_state, media_validation=media_validation, deps=_build_guardrail_flag_deps())


def _build_restricted_guardrail_answer(
    question: str,
    runtime_state: AgentRuntimeState,
    tool_results: list[ToolExecution],
    guardrail_flags: dict[str, bool],
) -> str:
    lines: list[str] = []
    followup_note = _build_followup_answer_note(runtime_state)
    if followup_note:
        lines.append(followup_note)
    if guardrail_flags.get("low_confidence_understanding"):
        lines.append("当前问题上下文不够明确，我无法安全判断要沿用哪一轮的对象或筛选条件，因此不直接给出结论。")
        return "\n\n".join(lines).strip()

    structured = _build_structured_media_answer(question, runtime_state, tool_results, include_guardrail_explanation=True, include_followup_note=True)
    if structured:
        return structured

    validation = _get_media_validation(tool_results)
    rows = _get_media_result_rows(tool_results)
    returned_count = int(validation.get("returned_result_count", len(rows)) or 0)
    if returned_count > 0:
        lines.append(f"结果说明：按当前约束严格过滤后，仅找到 {returned_count} 条符合条件的结果。")
    else:
        lines.append("结果说明：按当前约束严格过滤后，未找到严格满足条件的结果。")
    return "\n".join(lines).strip()


def _build_guardrail_answer_mode(
    question: str,
    runtime_state: AgentRuntimeState,
    tool_results: list[ToolExecution],
    guardrail_flags: dict[str, bool],
    get_media_result_rows: Callable[[list[ToolExecution]], list[dict[str, Any]]] | None = None,
    get_media_validation: Callable[[list[ToolExecution]], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    media_result_rows = get_media_result_rows or _get_media_result_rows
    media_validation_getter = get_media_validation or _get_media_validation
    reasons: list[str] = []
    annotation_lines: list[str] = []
    media_rows = media_result_rows(tool_results)
    has_grounded_media_results = bool(media_rows)
    media_result = next((item for item in tool_results if item.tool == TOOL_QUERY_MEDIA and isinstance(item.data, dict)), None)
    media_result_data = media_result.data if media_result and isinstance(media_result.data, dict) else {}
    layer_breakdown = media_result_data.get("layer_breakdown") if isinstance(media_result_data.get("layer_breakdown"), dict) else {}
    strict_scope_active = bool(layer_breakdown.get("strict_scope_active"))
    if guardrail_flags.get("low_confidence_understanding") and not has_grounded_media_results:
        reasons.append("low_confidence_understanding")
    if reasons:
        return {"mode": "restricted", "reasons": reasons, "answer": _build_restricted_guardrail_answer(question, runtime_state, tool_results, guardrail_flags), "annotations": []}

    if guardrail_flags.get("answer_truncated_by_reference_limit") and not has_grounded_media_results:
        validation = media_validation_getter(tool_results)
        returned_count = int(validation.get("returned_result_count", 0) or 0)
        if returned_count > 0:
            annotation_lines.append(
                f"结果说明：以下仅返回{'严格满足条件的' if strict_scope_active else '排序靠前的'} {returned_count} 条{'结果' if strict_scope_active else '相关结果'}。"
            )
    mode = "annotated" if annotation_lines else "normal"
    return {"mode": mode, "reasons": [], "answer": "", "annotations": annotation_lines}


def _apply_guardrail_answer_mode(answer: str, answer_mode: dict[str, Any]) -> str:
    mode = str(answer_mode.get("mode", "normal") or "normal")
    if mode == "restricted":
        return str(answer_mode.get("answer", "") or "").strip()
    annotations = [str(item).strip() for item in (answer_mode.get("annotations") or []) if str(item).strip()]
    body = str(answer or "").strip()
    if annotations and body:
        return "\n\n".join([*annotations, body]).strip()
    if annotations:
        return "\n\n".join(annotations).strip()
    return body


def _answer_has_inline_reference_markers(answer: str) -> bool:
    return bool(re.search(r"(?:\[\[\d+\]\]\([^\)]+\)|\[\d+\]\([^\)]+\))", str(answer or "")))


def _get_prompt_assembly_deps() -> PromptAssemblyDeps:
    return PromptAssemblyDeps(
        normalize_search_mode=_router_helpers_compat()._normalize_search_mode,
        build_answer_focus_hints=_build_answer_focus_hints,
        build_reference_hint_lines=_build_reference_hint_lines,
        llm_chat=runtime_infra._llm_chat,
        llm_chat_stream=runtime_infra._llm_chat_stream,
        is_context_length_error=_is_context_length_error,
        get_media_main_result_rows_from_data=media_get_main_result_rows_from_data,
        get_media_mention_rows_from_data=media_get_mention_rows_from_data,
        normalize_title_for_match=media_core._normalize_media_title_for_match,
        format_media_rating=_format_media_rating,
        approx_tokens=_router_helpers_compat()._approx_tokens,
        clip_text=_clip_text,
    )


def _summarize_answer(
    *,
    question: str,
    history: list[dict[str, str]],
    memory_context: str,
    tool_results: list[ToolExecution],
    backend: str,
    search_mode: str,
    quota_state: dict[str, Any],
    trace_id: str,
    debug_sink: dict[str, Any] | None = None,
    llm_stats_sink: dict[str, Any] | None = None,
    answer_strategy: Any | None = None,
    stream_callback: Callable[[str], None] | None = None,
    timing_sink: dict[str, float] | None = None,
    followup_mode: str = "none",
    carry_over_from_previous_turn: bool = False,
) -> str:
    return assemble_prompted_answer(
        question=question,
        history=history,
        memory_context=memory_context,
        tool_results=tool_results,
        backend=backend,
        search_mode=search_mode,
        quota_state=quota_state,
        trace_id=trace_id,
        deps=_get_prompt_assembly_deps(),
        debug_sink=debug_sink,
        llm_stats_sink=llm_stats_sink,
        answer_strategy=answer_strategy,
        stream_callback=stream_callback,
        timing_sink=timing_sink,
        followup_mode=followup_mode,
        carry_over_from_previous_turn=carry_over_from_previous_turn,
    )


def _fallback_retrieval_answer(question: str, tool_results: list[ToolExecution], reason: str = "") -> str:
    lines = ["未检测到可用的大模型，已自动降级为检索回复。", "", f"问题：{question}", ""]
    media_result = next((x for x in tool_results if x.tool == TOOL_QUERY_MEDIA), None)
    web_result = next((x for x in tool_results if x.tool == TOOL_SEARCH_WEB), None)
    if media_result and isinstance(media_result.data, dict):
        rows = media_result.data.get("results", [])
        if rows:
            lines.append("媒体记录结果：")
            for row in rows[:6]:
                title = str(row.get("title", "")).strip()
                if title:
                    lines.append(f"- {title}")
            lines.append("")
    if web_result and isinstance(web_result.data, dict):
        rows = web_result.data.get("results", [])
        if rows:
            lines.append("联网结果：")
            for row in rows[:6]:
                title = str(row.get("title", row.get("url", "网页"))).strip()
                url = str(row.get("url", "")).strip()
                if url:
                    lines.append(f"- {title}: {url}")
            lines.append("")
    if reason:
        lines.append(f"降级原因：{reason}")
    return "\n".join(lines).strip()


__all__ = [name for name in globals() if not name.startswith("__")]