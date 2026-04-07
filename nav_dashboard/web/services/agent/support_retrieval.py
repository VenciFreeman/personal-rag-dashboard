from __future__ import annotations

from dataclasses import replace as _dc_replace
from typing import Any, Callable

from nav_dashboard.web.services.media.media_external_enrichment import (
    PerItemExpansionConfig,
    PerItemExpansionToolset,
    execute_per_item_expansion as media_execute_per_item_expansion,
)
from nav_dashboard.web.services.media.media_retrieval_service import (
    MediaPolicyFlags,
    MediaRetrievalRequest,
    build_media_retrieval_response,
    get_media_main_result_rows_from_data as media_get_main_result_rows_from_data,
    get_media_mention_rows_from_data as media_get_mention_rows_from_data,
    get_media_validation as media_get_validation,
)
from nav_dashboard.web.services.planner import planner_contracts

from .domain import media_core
from .domain import media_tools
from .support_common import (
    BANGUMI_ACCESS_TOKEN,
    BANGUMI_SUBJECT_TYPE_ANIME,
    BANGUMI_SUBJECT_TYPE_REAL,
    COLLECTION_FILTER_TOP_K_MEDIA,
    DOC_SCORE_THRESHOLD,
    HYBRID_TOP_K_DOC,
    HYBRID_TOP_K_MEDIA,
    HYBRID_TOP_K_WEB,
    LOCAL_TOP_K_DOC,
    LOCAL_TOP_K_MEDIA,
    MEDIA_KEYWORD_SCORE_THRESHOLD,
    MEDIA_VECTOR_SCORE_THRESHOLD,
    PER_ITEM_BANGUMI_MIN_CONFIDENCE,
    PER_ITEM_EXPAND_LIMIT,
    PER_ITEM_EXPAND_MAX_WORKERS,
    PER_ITEM_TMDB_MIN_CONFIDENCE,
    QUERY_TYPE_MIXED,
    QUERY_TYPE_TECH,
    TECH_QUERY_DOC_SIM_THRESHOLD,
    TMDB_API_KEY,
    TMDB_READ_ACCESS_TOKEN,
    TMDB_TIMEOUT,
    TOOL_QUERY_DOC_RAG,
    TOOL_QUERY_MEDIA,
    TOOL_SEARCH_TMDB,
    TOOL_SEARCH_WEB,
    AgentRuntimeState,
    PostRetrievalAssessment,
    ToolExecution,
    WEB_SCORE_THRESHOLD,
    _clip_text,
    _resolve_agent_no_context,
    _router_helpers_compat,
    _serialize_post_retrieval_assessment,
)


def _get_media_validation(tool_results: list[ToolExecution]) -> dict[str, Any]:
    return media_get_validation(tool_results)


def _build_media_retrieval_boundary(tool_results: list[ToolExecution]) -> Any:
    return build_media_retrieval_response(
        tool_results,
        request=MediaRetrievalRequest(
            query="",
            previous_working_set={},
            resolved_entities=[],
            policy_flags=MediaPolicyFlags(),
        ),
    )


def _get_media_result_rows(tool_results: list[ToolExecution]) -> list[dict[str, Any]]:
    return _build_media_retrieval_boundary(tool_results).main_results


def _get_media_mention_rows(tool_results: list[ToolExecution]) -> list[dict[str, Any]]:
    return _build_media_retrieval_boundary(tool_results).mention_results


def _get_per_item_expansion_stats(tool_results: list[ToolExecution]) -> dict[str, Any]:
    return dict(_build_media_retrieval_boundary(tool_results).per_item_stats)


def _is_structured_personal_scope_query(result_data: dict[str, Any]) -> bool:
    if not isinstance(result_data, dict):
        return False
    if str(result_data.get("lookup_mode") or "").strip() != "filter_search":
        return False
    if str(result_data.get("subject_scope") or "").strip() != planner_contracts.ROUTER_SUBJECT_SCOPE_PERSONAL_RECORD:
        return False
    answer_shape = str(result_data.get("answer_shape") or "").strip()
    return answer_shape in {
        planner_contracts.ROUTER_ANSWER_SHAPE_LIST_ONLY,
        planner_contracts.ROUTER_ANSWER_SHAPE_LIST_PLUS_EXPAND,
        planner_contracts.ROUTER_ANSWER_SHAPE_COMPARE,
    }


def _preserve_structured_media_rows(
    rows: list[dict[str, Any]],
    *,
    result_data: dict[str, Any],
    limit: int,
) -> list[dict[str, Any]]:
    sort_preference = str(result_data.get("sort", "relevance") or "relevance")
    preserved = media_core._sort_media_results([dict(row) for row in rows if isinstance(row, dict)], sort_preference)
    preserved = preserved[: max(1, int(limit or 1))]
    for row in preserved:
        row["reference_limit_preserved"] = True
    return preserved


def _should_preserve_strict_scope_row(row: dict[str, Any], *, strict_scope_active: bool) -> bool:
    if not strict_scope_active or not isinstance(row, dict):
        return False
    reasons = [str(item).strip() for item in list(row.get("answer_layer_reasons") or []) if str(item).strip()]
    strong_reason_prefixes = (
        "strict_term_in_",
        "family_term_in_",
        "keyword:title",
        "keyword:author",
        "keyword:publisher",
        "keyword:channel",
        "keyword:category",
    )
    if any(reason == "alias_hit" or reason == "title_boost" or reason.startswith(strong_reason_prefixes) for reason in reasons):
        return True
    if _safe_score(row.get("title_boost")) > 0:
        return True
    if list(row.get("alias_hits") or []):
        return True
    keyword_hits = row.get("keyword_hits") if isinstance(row.get("keyword_hits"), list) else []
    for hit in keyword_hits:
        if not isinstance(hit, dict):
            continue
        field = str(hit.get("field") or "").strip().lower()
        if field in {"title", "author", "publisher", "channel", "category"}:
            return True
    return False


def _merge_preserved_reference_rows(
    rows: list[dict[str, Any]],
    preserved_rows: list[dict[str, Any]],
    *,
    sort_preference: str,
    limit: int,
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for row in [*rows, *preserved_rows]:
        if not isinstance(row, dict):
            continue
        row_id = str(row.get("id") or "").strip()
        if row_id and row_id in seen_ids:
            continue
        cloned = dict(row)
        cloned["reference_limit_preserved"] = True
        merged.append(cloned)
        if row_id:
            seen_ids.add(row_id)
    merged = media_core._sort_media_results(merged, sort_preference)
    return merged[: max(1, int(limit or 1))]


def _score_value(row: dict[str, Any]) -> float | None:
    value = row.get("score", None)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        text = str(value).strip()
        return float(text) if text else None
    except Exception:
        match_confidence = row.get("match_confidence", None)
        if isinstance(match_confidence, (int, float)):
            return float(match_confidence)
        try:
            text = str(match_confidence).strip()
            return float(text) if text else None
        except Exception:
            return None


def _safe_score(value: Any) -> float:
    try:
        if value in {None, ""}:
            return 0.0
        return float(value)
    except Exception:
        return 0.0


def _filter_rows(
    rows: list[dict[str, Any]],
    limit: int,
    threshold: float,
    threshold_selector: Callable[[dict[str, Any]], float] | None = None,
) -> list[dict[str, Any]]:
    picked: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        score = _score_value(row)
        row_threshold = float(threshold_selector(row)) if threshold_selector is not None else float(threshold)
        if score is None or score <= row_threshold:
            continue
        cloned = dict(row)
        cloned["score"] = float(score)
        picked.append(cloned)
    picked.sort(key=lambda x: float(x.get("score", 0.0)), reverse=True)
    return picked[: max(1, int(limit))]


def _media_threshold_selector(row: dict[str, Any], keyword_threshold: float, vector_threshold: float) -> float:
    mode = str(row.get("retrieval_mode", "keyword") or "").strip().lower()
    threshold = float(vector_threshold) if mode == "vector" else float(keyword_threshold)
    if list(row.get("alias_hits") or []):
        return max(0.08, threshold - 0.12)
    return threshold


def _media_mention_threshold_selector(row: dict[str, Any], keyword_threshold: float, vector_threshold: float) -> float:
    base_threshold = _media_threshold_selector(row, keyword_threshold, vector_threshold)
    mode = str(row.get("retrieval_mode", "keyword") or "").strip().lower()
    reasons = {
        str(reason).strip()
        for reason in (row.get("answer_layer_reasons") or [])
        if str(reason).strip()
    }
    relaxed_threshold = base_threshold - (0.12 if mode == "vector" else 0.08)
    if reasons & {"strict_term_in_title", "family_term_in_title", "title_boost", "alias_hit", "keyword:title"}:
        relaxed_threshold -= 0.05
    return max(0.02, float(relaxed_threshold))


def _log_agent_media_miss(query: str, query_profile: dict[str, Any]) -> None:
    try:
        media_threshold = float(query_profile.get("media_vector_score_threshold", MEDIA_VECTOR_SCORE_THRESHOLD) or MEDIA_VECTOR_SCORE_THRESHOLD)
        boundary_log_no_context_query(
            query,
            source="agent_media",
            top1_score=None,
            threshold=media_threshold,
        )
    except Exception:
        pass


def _apply_reference_limits(tool_results: list[ToolExecution], search_mode: str, query_profile: dict[str, Any]) -> list[ToolExecution]:
    normalized_mode = _router_helpers_compat()._normalize_search_mode(search_mode)
    limit_delta = int(query_profile.get("limit_delta", 0) or 0)
    doc_limit_base = HYBRID_TOP_K_DOC if normalized_mode == "hybrid" else LOCAL_TOP_K_DOC
    media_limit_base = HYBRID_TOP_K_MEDIA if normalized_mode == "hybrid" else LOCAL_TOP_K_MEDIA
    web_limit_base = HYBRID_TOP_K_WEB if normalized_mode == "hybrid" else 0
    doc_limit = max(1, int(doc_limit_base + limit_delta))
    media_limit = max(1, int(media_limit_base + limit_delta))
    web_limit = max(0, int(web_limit_base + limit_delta))
    if str(query_profile.get("answer_shape", "") or "") == planner_contracts.ROUTER_ANSWER_SHAPE_LIST_PLUS_EXPAND:
        media_limit = max(media_limit, COLLECTION_FILTER_TOP_K_MEDIA)
    doc_threshold = float(query_profile.get("doc_score_threshold", DOC_SCORE_THRESHOLD))
    media_keyword_threshold = float(query_profile.get("media_keyword_score_threshold", MEDIA_KEYWORD_SCORE_THRESHOLD))
    media_vector_threshold = float(query_profile.get("media_vector_score_threshold", MEDIA_VECTOR_SCORE_THRESHOLD))
    web_threshold = float(query_profile.get("web_score_threshold", WEB_SCORE_THRESHOLD))

    shaped: list[ToolExecution] = []
    for result in tool_results:
        if not isinstance(result.data, dict):
            shaped.append(result)
            continue
        rows = result.data.get("results", [])
        if not isinstance(rows, list):
            shaped.append(result)
            continue

        if result.tool == TOOL_QUERY_DOC_RAG:
            filtered = _filter_rows(rows, doc_limit, doc_threshold)
            data = dict(result.data)
            data["results"] = filtered
            shaped.append(ToolExecution(tool=result.tool, status=result.status, summary=f"命中 {len(filtered)} 条文档（score>{doc_threshold}）", data=data))
            continue

        if result.tool == TOOL_QUERY_MEDIA:
            media_entities = result.data.get("media_entities") if isinstance(result.data.get("media_entities"), list) else []
            lookup_mode = str(result.data.get("lookup_mode") or "").strip()
            query_class = str(result.data.get("query_class") or "").strip()
            answer_shape = str(query_profile.get("answer_shape", "") or result.data.get("answer_shape") or "").strip()
            collection_like_list = answer_shape in (
                planner_contracts.ROUTER_ANSWER_SHAPE_LIST_ONLY,
                planner_contracts.ROUTER_ANSWER_SHAPE_LIST_PLUS_EXPAND,
                planner_contracts.ROUTER_ANSWER_SHAPE_COMPARE,
            ) and (
                lookup_mode == "filter_search"
                or len([item for item in media_entities if str(item).strip()]) > 1
                or query_class == planner_contracts.ROUTER_QUERY_CLASS_PERSONAL_MEDIA_REVIEW_COLLECTION
            )
            effective_media_limit = max(media_limit, COLLECTION_FILTER_TOP_K_MEDIA) if collection_like_list else media_limit
            effective_mention_limit = max(1, min(4, effective_media_limit))
            main_rows = media_get_main_result_rows_from_data(result.data)
            mention_rows = media_get_mention_rows_from_data(result.data)
            strict_scope_active = bool(((result.data or {}).get("layer_breakdown") or {}).get("strict_scope_active"))
            threshold_pass_rows: list[dict[str, Any]] = []
            threshold_dropped = 0
            for row in main_rows:
                if not isinstance(row, dict):
                    continue
                score = _score_value(row)
                row_threshold = _media_threshold_selector(row, media_keyword_threshold, media_vector_threshold)
                preserve_strict_scope = _should_preserve_strict_scope_row(row, strict_scope_active=strict_scope_active)
                if not preserve_strict_scope and (score is None or score <= float(row_threshold)):
                    threshold_dropped += 1
                    continue
                cloned = dict(row)
                cloned["score"] = float(score or 0.0)
                threshold_pass_rows.append(cloned)
            sort_preference = str(result.data.get("sort", "relevance") or "relevance")
            threshold_pass_rows = media_core._sort_media_results(threshold_pass_rows, sort_preference)
            disable_top_k_truncation = bool(collection_like_list or _is_structured_personal_scope_query(result.data))
            filtered = list(threshold_pass_rows) if disable_top_k_truncation else threshold_pass_rows[: max(1, int(effective_media_limit))]
            if strict_scope_active and main_rows:
                preserved_main_rows = [
                    dict(row)
                    for row in main_rows
                    if isinstance(row, dict) and _should_preserve_strict_scope_row(row, strict_scope_active=True)
                ]
                if preserved_main_rows and len(filtered) < min(len(preserved_main_rows), max(1, int(effective_media_limit))):
                    filtered = _merge_preserved_reference_rows(
                        filtered,
                        preserved_main_rows,
                        sort_preference=sort_preference,
                        limit=(len(threshold_pass_rows) if disable_top_k_truncation else effective_media_limit),
                    )
            if not filtered and main_rows and (strict_scope_active or _is_structured_personal_scope_query(result.data)):
                filtered = _preserve_structured_media_rows(main_rows, result_data=result.data, limit=effective_media_limit)
                threshold_dropped = max(0, len(main_rows) - len(filtered))

            mention_filtered: list[dict[str, Any]] = []
            mention_threshold_dropped = 0
            for row in mention_rows:
                if not isinstance(row, dict):
                    continue
                score = _score_value(row)
                row_threshold = _media_mention_threshold_selector(row, media_keyword_threshold, media_vector_threshold)
                if score is None or score <= float(row_threshold):
                    mention_threshold_dropped += 1
                    continue
                cloned = dict(row)
                cloned["score"] = float(score)
                mention_filtered.append(cloned)
            mention_filtered = media_core._sort_media_results(mention_filtered, sort_preference)[:effective_mention_limit]

            summary = (
                f"命中 {len(filtered)} 条媒体记录"
                f"，扩展提及 {len(mention_filtered)} 条"
                f"（keyword score>{media_keyword_threshold}; vector score>{media_vector_threshold}）"
            )
            data = dict(result.data)
            data["results"] = filtered
            data["main_results"] = filtered
            data["mention_results"] = mention_filtered
            validation = dict(data.get("validation") or {}) if isinstance(data.get("validation"), dict) else {}
            top_k_dropped = 0 if disable_top_k_truncation else max(0, len(threshold_pass_rows) - len(filtered))
            validation["returned_result_count"] = len(filtered)
            validation["mention_result_count"] = len(mention_filtered)
            validation["dropped_by_reference_limit"] = threshold_dropped + top_k_dropped
            validation["reference_limit_drop_reasons"] = {
                "below_score_threshold": threshold_dropped,
                "top_k_truncation": top_k_dropped,
                "mention_below_score_threshold": mention_threshold_dropped,
            }
            data["validation"] = validation
            shaped.append(ToolExecution(tool=result.tool, status=result.status, summary=summary, data=data))
            continue

        if result.tool == TOOL_SEARCH_WEB:
            if web_limit <= 0:
                shaped.append(ToolExecution(tool=result.tool, status="skipped", summary="本地回答模式已禁用联网搜索", data={"results": []}))
            else:
                filtered = _filter_rows(rows, web_limit, web_threshold)
                data = dict(result.data)
                data["results"] = filtered
                shaped.append(ToolExecution(tool=result.tool, status=result.status, summary=f"命中 {len(filtered)} 条网页结果（score>{web_threshold}）", data=data))
            continue

        if result.tool == TOOL_SEARCH_TMDB:
            data = dict(result.data)
            data["results"] = rows[: max(1, media_limit + 1)]
            shaped.append(ToolExecution(tool=result.tool, status=result.status, summary=f"TMDB 命中 {len(data['results'])} 条外部媒体结果", data=data))
            continue

        shaped.append(result)

    return shaped


def _execute_per_item_expansion(
    tool_results: list[ToolExecution],
    *,
    trace_id: str,
    media_family: str = "",
    answer_shape: str = "",
    item_callback: Callable[[dict[str, Any], int, int], None] | None = None,
) -> list[ToolExecution]:
    from nav_dashboard.web.services.planner.external_enrichment_policy import decide_external_enrichment  # noqa: PLC0415

    media_result = next(
        (
            item
            for item in tool_results
            if getattr(item, "tool", "") == TOOL_QUERY_MEDIA and isinstance(getattr(item, "data", None), dict)
        ),
        None,
    )
    media_result_data = media_result.data if media_result is not None and isinstance(media_result.data, dict) else {}
    media_entities = [str(item).strip() for item in list(media_result_data.get("media_entities") or []) if str(item).strip()]

    enrichment = decide_external_enrichment(
        query_class=str(media_result_data.get("query_class") or "").strip(),
        subject_scope=str(media_result_data.get("subject_scope") or "").strip(),
        answer_shape=answer_shape,
        media_family=media_family,
        lookup_mode=str(media_result_data.get("lookup_mode") or "").strip(),
        needs_explanation=bool(media_result_data.get("needs_explanation")),
        entity_count=len(media_entities),
    )
    return media_execute_per_item_expansion(
        tool_results,
        trace_id=trace_id,
        config=PerItemExpansionConfig(
            media_family=media_family,
            answer_shape=answer_shape,
            allow_per_item_tmdb=enrichment.allow_per_item_tmdb,
            allow_per_item_bangumi=enrichment.allow_per_item_bangumi,
            allow_per_item_wiki=enrichment.allow_per_item_wiki,
            expand_limit=PER_ITEM_EXPAND_LIMIT,
            max_workers=PER_ITEM_EXPAND_MAX_WORKERS,
            tmdb_min_confidence=PER_ITEM_TMDB_MIN_CONFIDENCE,
            bangumi_min_confidence=PER_ITEM_BANGUMI_MIN_CONFIDENCE,
            tmdb_timeout=min(TMDB_TIMEOUT, 8.0),
            max_book_wiki_items=max(1, int(enrichment.max_book_wiki_items or 2)),
            bangumi_access_token_available=bool(BANGUMI_ACCESS_TOKEN),
            tmdb_available=bool(TMDB_API_KEY or TMDB_READ_ACCESS_TOKEN),
            bangumi_subject_types=(BANGUMI_SUBJECT_TYPE_ANIME, BANGUMI_SUBJECT_TYPE_REAL),
        ),
        toolset=PerItemExpansionToolset(
            media_title_match_boost_any=media_core._media_title_match_boost_any,
            safe_score=_safe_score,
            clip_text=_clip_text,
            search_bangumi=media_tools._tool_search_bangumi,
            search_tmdb_media=media_tools._tool_search_tmdb_media,
            parse_mediawiki_page=media_tools._tool_parse_mediawiki_page,
            search_mediawiki_action=media_tools._tool_search_mediawiki_action,
        ),
        item_callback=item_callback,
    )


def _update_post_retrieval_fallback_evidence(
    runtime_state: AgentRuntimeState,
    query_classification: dict[str, Any],
    tool_results: list[ToolExecution],
    doc_data: dict[str, Any],
    query_profile: dict[str, Any],
) -> None:
    doc_score = doc_data.get("doc_top1_score") if isinstance(doc_data, dict) else None
    doc_threshold = float(query_profile.get("doc_score_threshold", DOC_SCORE_THRESHOLD) or DOC_SCORE_THRESHOLD)
    doc_similarity = {
        "available": bool(doc_data),
        "score": round(float(doc_score), 4) if doc_score is not None else None,
        "threshold": round(doc_threshold, 4),
        "top1_score_before_rerank": doc_data.get("doc_top1_score_before_rerank") if isinstance(doc_data, dict) else None,
        "no_context": bool(doc_data.get("no_context")) if isinstance(doc_data, dict) else False,
    }
    media_validation = _get_media_validation(tool_results)
    tmdb_result = next((item for item in tool_results if item.tool == TOOL_SEARCH_TMDB), None)
    tmdb_rows = tmdb_result.data.get("results", []) if (tmdb_result and isinstance(tmdb_result.data, dict) and isinstance(tmdb_result.data.get("results"), list)) else []
    tech_score = float(doc_score or 0.0) if doc_score is not None else 0.0
    weak_threshold = max(0.18, TECH_QUERY_DOC_SIM_THRESHOLD - 0.10)
    assessment = PostRetrievalAssessment(
        status="ready",
        doc_similarity=doc_similarity,
        media_validation={
            "raw_candidates_count": int(media_validation.get("raw_candidates_count", 0) or 0),
            "returned_result_count": int(media_validation.get("returned_result_count", 0) or 0),
            "dropped_by_validator": int(media_validation.get("dropped_by_validator", 0) or 0),
            "dropped_by_reference_limit": int(media_validation.get("dropped_by_reference_limit", 0) or 0),
        },
        tmdb={"requested": tmdb_result is not None, "result_count": len(tmdb_rows)},
        tech_score=tech_score,
        weak_tech_signal=weak_threshold <= tech_score < TECH_QUERY_DOC_SIM_THRESHOLD,
    )
    runtime_state.post_retrieval_assessment = assessment
    runtime_state.execution_artifact = _dc_replace(
        runtime_state.execution_artifact,
        fallback_evidence=_serialize_post_retrieval_assessment(assessment),
        doc_similarity=doc_similarity,
        tech_score=tech_score,
        weak_tech_signal=bool(assessment.weak_tech_signal),
    )
    query_classification["fallback_evidence"] = _serialize_post_retrieval_assessment(assessment)
    query_classification["doc_similarity"] = doc_similarity
    query_classification["tech_score"] = tech_score
    query_classification["weak_tech_signal"] = bool(assessment.weak_tech_signal)


__all__ = [name for name in globals() if not name.startswith("__")]