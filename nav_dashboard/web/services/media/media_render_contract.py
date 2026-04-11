from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from ..planner import planner_contracts
from ..agent.agent_types import ToolExecution
from .media_strategy import resolve_media_strategy


@dataclass(frozen=True)
class MediaRenderContractBuilderDeps:
    query_type_media: str
    tool_query_media: str
    tool_search_by_creator: str
    tool_expand_mediawiki_concept: str
    tool_search_tmdb: str
    tool_search_bangumi: str
    tool_search_mediawiki: str
    tool_parse_mediawiki: str
    tool_search_web: str
    normalize_media_match_terms: Callable[[list[str]], list[str]]
    get_query_type: Callable[..., str]
    get_media_result_rows: Callable[[list[ToolExecution]], list[dict[str, Any]]]
    get_media_mention_rows: Callable[[list[ToolExecution]], list[dict[str, Any]]]
    get_media_validation: Callable[[list[ToolExecution]], dict[str, Any]]
    get_per_item_expansion_stats: Callable[[list[ToolExecution]], dict[str, Any]]
    get_planner_snapshot_from_runtime: Callable[[Any], dict[str, Any]]
    get_resolved_query_state_from_runtime: Callable[[Any], dict[str, Any]]
    get_lookup_mode_from_state: Callable[[dict[str, Any]], str]
    describe_planner_scope: Callable[[dict[str, Any]], str]
    question_requests_personal_evaluation: Callable[[str], bool]
    question_requests_media_details: Callable[[str], bool]
    should_render_mentions: Callable[..., bool]
    should_render_external_appendix: Callable[..., bool]


@dataclass(frozen=True)
class MediaRenderContract:
    show_main: bool = True
    include_reviews: bool = False
    include_metadata: bool = False
    include_mentions: bool = False
    include_external: bool = False
    row_limit: int = 8
    mention_limit: int = 4
    external_limit: int = 4
    intro_mode: str = "generic"
    style: str = "generic"
    query_class: str = ""
    subject_scope: str = ""
    answer_shape: str = ""
    ranking_mode: str = "relevance"
    group_mode: str = "none"
    intro_lines: tuple[str, ...] = ()
    main_rows: tuple[dict[str, Any], ...] = ()
    mention_rows: tuple[dict[str, Any], ...] = ()
    external_rows: tuple[dict[str, Any], ...] = ()


def format_guardrail_date_range(date_range: Any) -> str:
    if not isinstance(date_range, list) or len(date_range) != 2:
        return ""
    start = str(date_range[0] or "").strip()
    end = str(date_range[1] or "").strip()
    if not start or not end:
        return ""
    return f"{start} 鑷?{end}"


def build_followup_answer_note(runtime_state: Any, *, deps: Any) -> str:
    if not bool(runtime_state.context_resolution.detected_followup):
        return ""
    resolved_state = deps.get_resolved_query_state_from_runtime(runtime_state)
    if not bool(resolved_state.get("carry_over_from_previous_turn")):
        return ""
    planner_snapshot = deps.get_planner_snapshot_from_runtime(runtime_state)
    inheritance = dict(runtime_state.context_resolution.inheritance_applied)
    parts: list[str] = []
    scope = deps.describe_planner_scope(planner_snapshot)
    if inheritance.get("media_type") == "carried_over" or inheritance.get("filters") in {"carried_over", "overridden"}:
        parts.append(f"沿用了上一轮的{scope}约束")
    date_range = format_guardrail_date_range(runtime_state.context_resolution.conversation_state_after.get("date_range"))
    if inheritance.get("date_range") == "overridden" and date_range:
        parts.append(f"将时间窗替换为 {date_range}")
    elif inheritance.get("date_range") == "carried_over" and date_range:
        parts.append(f"保留了时间窗 {date_range}")
    if inheritance.get("entity") == "cleared":
        parts.append("清除了上一轮的具体作品实体")
    if not parts:
        parts.append("沿用了上一轮的上下文")
    return "处理说明：" + "；".join(parts) + "。"


def _normalize_media_render_ranking_mode(value: Any) -> str:
    normalized = str(value or "relevance").strip().lower()
    if normalized in {"rating_desc", "rating_asc", "date_desc", "date_asc", "relevance"}:
        return normalized
    return "relevance"


def _question_has_high_low_review_split(question: str) -> bool:
    lowered = str(question or "").strip().lower()
    if not lowered:
        return False
    high_cues = ("最高", "最好", "评分高", "评价高", "评分最高", "最高分", "best", "highest", "highest-rated")
    low_cues = ("最低", "最差", "较差", "差的", "差", "评分低", "评价低", "评分最低", "最低分", "worst", "lowest", "lowest-rated")
    return any(cue in lowered for cue in high_cues) and any(cue in lowered for cue in low_cues)


def _media_render_rating_number(row: dict[str, Any], *, descending: bool) -> float:
    value = row.get("rating")
    try:
        return float(value)
    except Exception:
        return -1.0 if descending else 11.0


def _normalize_media_render_value(value: Any, *, deps: MediaRenderContractBuilderDeps) -> str:
    normalized = deps.normalize_media_match_terms([str(value or "")])
    if normalized:
        return normalized[0]
    return str(value or "").strip().lower()


def _media_row_display_bucket(
    row: dict[str, Any],
    resolved_entities: list[str],
    *,
    deps: MediaRenderContractBuilderDeps,
) -> tuple[int, int, int]:
    reasons = {str(reason).strip() for reason in (row.get("answer_layer_reasons") or []) if str(reason).strip()}
    normalized_entities = {
        _normalize_media_render_value(entity, deps=deps)
        for entity in resolved_entities
        if _normalize_media_render_value(entity, deps=deps)
    }
    title_key = _normalize_media_render_value(row.get("title"), deps=deps)
    matched_entities = {
        _normalize_media_render_value(entity, deps=deps)
        for entity in (row.get("matched_entities") or [])
        if _normalize_media_render_value(entity, deps=deps)
    }
    match_terms = {
        _normalize_media_render_value(term, deps=deps)
        for term in (row.get("match_terms") or [])
        if _normalize_media_render_value(term, deps=deps)
    }
    direct_entity_hit = bool(
        normalized_entities
        and (title_key in normalized_entities or bool(matched_entities & normalized_entities) or bool(match_terms & normalized_entities))
    )
    strict_title_hit = direct_entity_hit or bool(reasons & {"title_boost", "alias_hit", "keyword:title", "strict_term_in_title"})
    family_hit = "family_term_in_title" in reasons
    retrieval_mode = str(row.get("retrieval_mode") or "").strip()
    working_set_rank = int(row.get("working_set_rank") or 9999)
    if strict_title_hit or retrieval_mode == "working_set_item":
        return (0, 0 if retrieval_mode == "working_set_item" else 1, working_set_rank)
    if family_hit:
        return (1, 1, working_set_rank)
    return (2, 1, working_set_rank)


def _order_media_render_rows(
    rows: list[dict[str, Any]],
    resolved_entities: list[str],
    *,
    deps: MediaRenderContractBuilderDeps,
) -> list[dict[str, Any]]:
    indexed_rows = list(enumerate(rows))
    indexed_rows.sort(key=lambda item: (_media_row_display_bucket(item[1], resolved_entities, deps=deps), item[0]))
    return [row for _, row in indexed_rows]


def _sort_media_rows_for_render(
    rows: list[dict[str, Any]],
    resolved_entities: list[str],
    *,
    ranking_mode: str,
    deps: MediaRenderContractBuilderDeps,
) -> list[dict[str, Any]]:
    indexed_rows = list(enumerate(rows))
    normalized = _normalize_media_render_ranking_mode(ranking_mode)
    if normalized == "rating_desc":
        indexed_rows.sort(
            key=lambda item: (
                _media_render_rating_number(item[1], descending=True),
                float(item[1].get("score") or 0.0),
                str(item[1].get("date") or ""),
                -item[0],
            ),
            reverse=True,
        )
        return [row for _, row in indexed_rows]
    if normalized == "rating_asc":
        indexed_rows.sort(
            key=lambda item: (
                _media_render_rating_number(item[1], descending=False),
                -float(item[1].get("score") or 0.0),
                str(item[1].get("date") or ""),
                item[0],
            )
        )
        return [row for _, row in indexed_rows]
    if normalized == "date_desc":
        indexed_rows.sort(key=lambda item: (str(item[1].get("date") or ""), -item[0]), reverse=True)
        return [row for _, row in indexed_rows]
    if normalized == "date_asc":
        indexed_rows.sort(key=lambda item: (str(item[1].get("date") or "9999-12-31"), item[0]))
        return [row for _, row in indexed_rows]
    return _order_media_render_rows(rows, resolved_entities, deps=deps)


def _extract_media_external_rows(
    tool_results: list[ToolExecution],
    external_limit: int,
    *,
    deps: MediaRenderContractBuilderDeps,
) -> list[dict[str, Any]]:
    media_rows: list[dict[str, Any]] = []
    for item in tool_results:
        data = item.data if isinstance(getattr(item, "data", None), dict) else {}
        if getattr(item, "tool", "") in {deps.tool_query_media, deps.tool_search_by_creator}:
            rows = data.get("results") if isinstance(data.get("results"), list) else []
            media_rows = [row for row in rows if isinstance(row, dict)]
            break
    for item in tool_results:
        data = item.data if isinstance(getattr(item, "data", None), dict) else {}
        if data.get("per_item_fanout"):
            rows = data.get("per_item_data") if isinstance(data.get("per_item_data"), list) else []
            return [dict(row) for row in rows if isinstance(row, dict)][:external_limit]
        if getattr(item, "tool", "") == deps.tool_parse_mediawiki and isinstance(data.get("page"), dict):
            page = dict(data.get("page") or {})
            local_title = str(media_rows[0].get("title") or "").strip() if media_rows else ""
            return [{
                "local_title": local_title or str(page.get("display_title") or page.get("title") or "").strip(),
                "title": str(page.get("display_title") or page.get("title") or "").strip(),
                "external_overview": str(page.get("extract") or "").strip(),
                "external_source": "wiki",
            }]
        if getattr(item, "tool", "") == deps.tool_search_tmdb:
            rows = data.get("results") if isinstance(data.get("results"), list) else []
            return [{
                "local_title": str(media_rows[0].get("title") or "").strip() if media_rows else str(row.get("title") or "").strip(),
                "title": str(row.get("title") or row.get("name") or "").strip(),
                "external_overview": str(row.get("overview") or "").strip(),
                "external_source": "tmdb",
            } for row in rows if isinstance(row, dict)][:external_limit]
    return []


def _can_render_structured_media(
    tool_results: list[ToolExecution],
    runtime_state: Any,
    *,
    deps: MediaRenderContractBuilderDeps,
) -> bool:
    rows = deps.get_media_result_rows(tool_results)
    if not rows:
        return False
    if deps.get_query_type(runtime_state=runtime_state).strip().upper() != deps.query_type_media:
        return False
    non_media_fact_tools = [
        item
        for item in tool_results
        if getattr(item, "tool", "")
        not in {
            deps.tool_query_media,
            deps.tool_search_by_creator,
            deps.tool_expand_mediawiki_concept,
            deps.tool_search_tmdb,
            deps.tool_search_bangumi,
            deps.tool_search_mediawiki,
            deps.tool_parse_mediawiki,
        }
        and getattr(item, "tool", "") != deps.tool_search_web
        and isinstance(getattr(item, "data", None), dict)
        and isinstance(item.data.get("results"), list)
        and item.data.get("results")
        and not bool(item.data.get("per_item_fanout"))
    ]
    return not non_media_fact_tools


def build_media_render_contract(
    question: str,
    runtime_state: Any,
    tool_results: list[ToolExecution],
    *,
    deps: MediaRenderContractBuilderDeps,
) -> MediaRenderContract:
    if not _can_render_structured_media(tool_results, runtime_state, deps=deps):
        return MediaRenderContract(show_main=False)
    resolved_state = deps.get_resolved_query_state_from_runtime(runtime_state)
    resolved_question = str(getattr(runtime_state.context_resolution, "resolved_question", "") or question or "").strip()
    lookup_mode = str(deps.get_lookup_mode_from_state(resolved_state) or "")
    decision = getattr(runtime_state, "decision", None)
    media_strategy = resolve_media_strategy(decision, lookup_mode=lookup_mode)
    query_class = media_strategy.query_class
    subject_scope = media_strategy.subject_scope
    answer_shape = media_strategy.answer_shape
    entity_count = media_strategy.entity_count
    needs_explanation = media_strategy.needs_explanation

    wants_review = deps.question_requests_personal_evaluation(resolved_question)
    wants_detail = deps.question_requests_media_details(resolved_question)
    if (
        subject_scope == planner_contracts.ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD
        and answer_shape == planner_contracts.ROUTER_ANSWER_SHAPE_DETAIL_CARD
    ):
        wants_review = True
        wants_detail = True

    intro_mode = "generic"
    if wants_review and wants_detail:
        intro_mode = "review_and_detail"
    elif wants_review:
        intro_mode = "review_only"
    elif wants_detail:
        intro_mode = "detail_only"

    if lookup_mode != "filter_search" and not wants_review and not wants_detail:
        return MediaRenderContract(show_main=False)

    ranking_mode = _normalize_media_render_ranking_mode((getattr(decision, "ranking", {}) or {}).get("mode") or getattr(decision, "sort", "relevance"))
    group_mode = "none"
    if query_class == planner_contracts.ROUTER_QUERY_CLASS_PERSONAL_MEDIA_REVIEW_COLLECTION:
        if _question_has_high_low_review_split(question):
            group_mode = "best_worst"
            if ranking_mode == "relevance":
                ranking_mode = "rating_desc"
        elif ranking_mode == "rating_desc":
            group_mode = "highest_first"
        elif ranking_mode == "rating_asc":
            group_mode = "lowest_first"

    rows = [row for row in deps.get_media_result_rows(tool_results) if isinstance(row, dict)]
    mention_rows = [row for row in deps.get_media_mention_rows(tool_results) if isinstance(row, dict)]
    validation = deps.get_media_validation(tool_results)
    expansion_stats = deps.get_per_item_expansion_stats(tool_results)
    planner_snapshot = deps.get_planner_snapshot_from_runtime(runtime_state)
    media_result = next((item for item in tool_results if getattr(item, "tool", "") in {deps.tool_query_media, deps.tool_search_by_creator} and isinstance(getattr(item, "data", None), dict)), None)
    media_result_data = media_result.data if media_result is not None and isinstance(media_result.data, dict) else {}
    layer_breakdown = media_result_data.get("layer_breakdown") if isinstance(media_result_data.get("layer_breakdown"), dict) else {}
    working_set_reused = bool(
        str(media_result_data.get("lookup_mode") or "").strip() == "working_set_followup"
        or bool((media_result_data.get("retrieval_adapter") or {}).get("working_set_reused"))
    )
    scope = deps.describe_planner_scope(planner_snapshot)
    returned_count = int(validation.get("returned_result_count", len(rows)) or 0)
    strict_scope_active = bool(layer_breakdown.get("strict_scope_active"))
    intro_lines: list[str] = []
    followup_note = build_followup_answer_note(runtime_state, deps=deps)
    if followup_note:
        intro_lines.append(followup_note)
    if returned_count > 0:
        if working_set_reused:
            intro_lines.append(f"基于上一轮结果集，继续展开这 {returned_count} 条{scope}。")
        if intro_mode == "review_and_detail":
            intro_lines.append(f"按当前条件，找到 {returned_count} 条符合条件的{scope}，下面列出你的评分、短评和条目细节。")
        elif intro_mode == "review_only":
            intro_lines.append(f"按当前条件，找到 {returned_count} 条符合条件的{scope}，下面优先列出你的评分和短评。")
        elif intro_mode == "detail_only":
            if not working_set_reused:
                intro_lines.append(f"按当前条件，找到 {returned_count} 条符合条件的{scope}，下面列出条目的细节信息和你的短评。")
        elif not working_set_reused:
            intro_lines.append(f"按当前条件，找到 {returned_count} 条符合条件的{scope}。")
    expanded_count = int(expansion_stats.get("expanded_count", 0) or 0)
    source_title_count = int(expansion_stats.get("source_title_count", 0) or 0)
    total_rows = int(expansion_stats.get("total_rows", len(rows)) or len(rows))
    per_item_source = str(expansion_stats.get("per_item_source") or "").strip()
    if expanded_count > 0 and total_rows > source_title_count > 0:
        source_label = per_item_source.upper() if per_item_source else "外部来源"
        intro_lines.append(
            f"补充说明：本轮仅对前 {source_title_count}/{total_rows} 条结果尝试外部条目补充，其中 {expanded_count} 条成功补到 {source_label} 简介；其余结果仍以本地库记录为准。"
        )

    resolved_entities = [str(entity).strip() for entity in (getattr(decision, "entities", []) or []) if str(entity).strip()]
    style = "collection"
    if subject_scope == planner_contracts.ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD and intro_mode == "review_only":
        style = "personal_review"
    elif answer_shape == planner_contracts.ROUTER_ANSWER_SHAPE_DETAIL_CARD or intro_mode == "detail_only":
        style = "detail_card"
    include_mentions = bool(media_strategy.should_render_mentions)
    if answer_shape == planner_contracts.ROUTER_ANSWER_SHAPE_DETAIL_CARD or style == "detail_card":
        include_mentions = False

    return MediaRenderContract(
        show_main=True,
        include_reviews=bool(wants_review or wants_detail),
        include_metadata=bool(wants_detail),
        include_mentions=include_mentions,
        include_external=media_strategy.should_expand_external,
        row_limit=max(1, len(rows)),
        mention_limit=4,
        external_limit=4,
        intro_mode=intro_mode,
        style=style,
        query_class=query_class,
        subject_scope=subject_scope,
        answer_shape=answer_shape,
        ranking_mode=ranking_mode,
        group_mode=group_mode,
        intro_lines=tuple(intro_lines),
        main_rows=tuple(_sort_media_rows_for_render(rows, resolved_entities, ranking_mode=ranking_mode, deps=deps)),
        mention_rows=tuple(mention_rows[:4]),
        external_rows=tuple(_extract_media_external_rows(tool_results, 4, deps=deps)[:4]),
    )

