from __future__ import annotations

import json
from typing import Any
from urllib import parse as urlparse

from ....prompts.doc_prompts import build_doc_query_rewrite_system_prompt
from ..infra import runtime_infra


def _parse_query_rewrite_output(raw: str, fallback: str, count: int) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return [fallback]

    parsed_queries: list[str] = []
    try:
        data = json.loads(text)
        if isinstance(data, dict) and isinstance(data.get("queries"), list):
            parsed_queries = [str(x).strip() for x in data.get("queries", []) if str(x).strip()]
        elif isinstance(data, list):
            parsed_queries = [str(x).strip() for x in data if str(x).strip()]
    except Exception:
        pass

    if not parsed_queries:
        for line in text.splitlines():
            value = line.strip().lstrip("-* ").strip()
            if value:
                parsed_queries.append(value)

    dedup: list[str] = []
    seen: set[str] = set()
    for query in [fallback, *parsed_queries]:
        key = query.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        dedup.append(query.strip())
        if len(dedup) >= max(1, count):
            break
    return dedup or [fallback]


def _rewrite_doc_queries(query: str) -> tuple[list[str], str]:
    prompt = build_doc_query_rewrite_system_prompt()
    try:
        rewritten = runtime_infra._llm_chat(
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": query},
            ],
            backend="local",
            quota_state={"deepseek": 0},
            count_quota=False,
        )
        return _parse_query_rewrite_output(rewritten, query, runtime_infra.DOC_QUERY_REWRITE_COUNT), "ok"
    except Exception as exc:  # noqa: BLE001
        return [query], f"fallback:{exc}"


def _safe_score(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _merge_doc_vector_results(
    rows_by_query: list[tuple[str, list[dict[str, Any]]]],
    *,
    primary_query: str = "",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    merged: dict[str, dict[str, Any]] = {}
    debug_batches: list[dict[str, Any]] = []
    normalized_primary = str(primary_query or "").strip().lower()

    for query, rows in rows_by_query:
        compact_rows: list[dict[str, Any]] = []
        for idx, row in enumerate(rows, start=1):
            if not isinstance(row, dict):
                continue
            path = str(row.get("path", "")).strip()
            if not path:
                continue
            score = _safe_score(row.get("score"))
            query_bonus = runtime_infra.DOC_PRIMARY_QUERY_SCORE_BONUS if normalized_primary and query.strip().lower() == normalized_primary else 0.0
            compact_rows.append({"path": path, "score": score})
            existing = merged.get(path)
            priority_score = score + query_bonus
            if existing is None or priority_score > _safe_score(existing.get("query_priority_score")):
                merged[path] = {
                    "path": path,
                    "topic": row.get("topic"),
                    "vector_score": score,
                    "score": score,
                    "query_priority_boost": query_bonus,
                    "query_priority_score": priority_score,
                    "matched_rank": idx,
                    "matched_queries": [query],
                }
            else:
                matched = existing.get("matched_queries", [])
                if not isinstance(matched, list):
                    matched = []
                if query not in matched:
                    matched.append(query)
                existing["matched_queries"] = matched
        debug_batches.append({"query": query, "results": compact_rows})

    merged_rows = sorted(
        merged.values(),
        key=lambda item: (_safe_score(item.get("query_priority_score")), _safe_score(item.get("vector_score"))),
        reverse=True,
    )
    return merged_rows, debug_batches


def _build_keyword_score_map(queries: list[str], vector_top_n: int) -> dict[str, float]:
    keyword_scores: dict[str, float] = {}
    for query in queries:
        payload = runtime_infra._http_json(
            "GET",
            f"{runtime_infra.AI_SUMMARY_BASE}/api/preview/search/keyword?" + urlparse.urlencode({"q": query, "limit": int(vector_top_n)}),
        )
        rows = payload.get("results", []) if isinstance(payload, dict) else []
        for row in rows:
            if not isinstance(row, dict):
                continue
            path = str(row.get("path", "")).strip()
            if not path:
                continue
            score = _safe_score(row.get("score"))
            if score > keyword_scores.get(path, -1.0):
                keyword_scores[path] = score
    return keyword_scores


def _rerank_merged_doc_rows(rows: list[dict[str, Any]], keyword_scores: dict[str, float]) -> list[dict[str, Any]]:
    max_kw = max(keyword_scores.values(), default=0.0)
    norm = max_kw if max_kw > 0.0 else 1.0
    reranked: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        path = str(item.get("path", "")).strip()
        vector_score = _safe_score(item.get("vector_score"))
        keyword_score = min(1.0, keyword_scores.get(path, 0.0) / norm)
        final_score = (0.75 * vector_score) + (0.25 * keyword_score)
        item["keyword_score"] = keyword_score
        item["score"] = final_score
        reranked.append(item)
    reranked.sort(key=lambda item: _safe_score(item.get("score")), reverse=True)
    return reranked


def _cap_doc_vector_candidates(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    normalized_limit = max(1, int(limit))
    return [dict(row) for row in rows[:normalized_limit] if isinstance(row, dict)]


def _tool_query_document_rag(query: str, query_profile: dict[str, Any], trace_id: str = "") -> runtime_infra.ToolExecution:
    import time as _time

    doc_vector_top_n = min(
        max(1, int(runtime_infra.MAX_DOC_VECTOR_CANDIDATES)),
        max(4, int(query_profile.get("doc_vector_top_n", runtime_infra.DOC_VECTOR_TOP_N) or runtime_infra.DOC_VECTOR_TOP_N)),
    )
    rewrite_queries, rewrite_status = _rewrite_doc_queries(query)
    vector_batches: list[tuple[str, list[dict[str, Any]]]] = []
    warnings: list[str] = []
    embed_cache_hit = 0

    vector_t0 = _time.perf_counter()
    for rewritten_query in rewrite_queries:
        try:
            vec = runtime_infra._http_json(
                "GET",
                f"{runtime_infra.AI_SUMMARY_BASE}/api/preview/search/vector?"
                + urlparse.urlencode({"q": rewritten_query, "top_k": max(6, int(doc_vector_top_n))}),
                headers={"X-Trace-Id": trace_id, "X-Trace-Stage": "agent.doc.vector_preview"},
            )
            vec_rows = vec.get("results", []) if isinstance(vec, dict) else []
            vector_batches.append((rewritten_query, [row for row in vec_rows if isinstance(row, dict)]))
            if isinstance(vec, dict) and float(vec.get("embed_cache_hit", 0) or 0) > 0:
                embed_cache_hit = 1
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"向量检索不可用({rewritten_query}): {exc}")
    vector_recall_seconds = _time.perf_counter() - vector_t0

    if not vector_batches and warnings:
        raise RuntimeError("; ".join(warnings))

    merged_rows, vector_debug = _merge_doc_vector_results(vector_batches, primary_query=query)
    merged_rows = _cap_doc_vector_candidates(merged_rows, runtime_infra.MAX_DOC_VECTOR_CANDIDATES)
    keyword_scores: dict[str, float] = {}
    try:
        keyword_scores = _build_keyword_score_map(rewrite_queries, doc_vector_top_n)
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"关键词重排不可用: {exc}")

    rerank_t0 = _time.perf_counter()
    reranked_rows = _rerank_merged_doc_rows(merged_rows, keyword_scores)
    rerank_seconds = _time.perf_counter() - rerank_t0

    doc_threshold = float(query_profile.get("doc_score_threshold", runtime_infra.DOC_SCORE_THRESHOLD) or runtime_infra.DOC_SCORE_THRESHOLD)
    top1_score_before: float | None = max((float(row.get("score", 0.0)) for row in merged_rows), default=None)
    top1_score: float | None = max((float(row.get("score", 0.0)) for row in reranked_rows), default=None)
    top1_path_before = str(merged_rows[0].get("path", "")).strip() if merged_rows else ""
    top1_path_after = str(reranked_rows[0].get("path", "")).strip() if reranked_rows else ""
    top1_identity_changed: int | None = None
    top1_rank_shift: float | None = None
    if top1_path_before and top1_path_after:
        top1_identity_changed = int(top1_path_before != top1_path_after)
        before_rank_after_top1 = next(
            (idx + 1 for idx, row in enumerate(merged_rows) if str(row.get("path", "")).strip() == top1_path_after),
            None,
        )
        if before_rank_after_top1 is not None:
            top1_rank_shift = float(before_rank_after_top1 - 1)
    no_context = 1 if (top1_score is None or top1_score < doc_threshold) else 0

    summary = f"命中 {len(reranked_rows)} 条文档（rewrite={len(rewrite_queries)}，vector_top_n={doc_vector_top_n}）"
    if warnings:
        summary += f"（部分降级: {'; '.join(warnings)}）"

    query_rewrite_hit = int(str(rewrite_status or "").strip().lower() == "ok")

    return runtime_infra.ToolExecution(
        tool=runtime_infra.TOOL_QUERY_DOC_RAG,
        status="ok",
        summary=summary,
        data={
            "trace_id": trace_id,
            "trace_stage": "agent.tool.query_document_rag",
            "results": reranked_rows[: max(8, int(doc_vector_top_n))],
            "query_profile": query_profile,
            "query_rewrite": {
                "original": query,
                "queries": rewrite_queries,
                "status": rewrite_status,
            },
            "embed_cache_hit": embed_cache_hit,
            "query_rewrite_hit": query_rewrite_hit,
            "vector_batches": vector_debug,
            "rerank": {
                "method": "vector+keyword_fusion",
                "vector_weight": 0.85,
                "keyword_weight": 0.15,
            },
            "vector_recall_seconds": round(vector_recall_seconds, 6),
            "rerank_seconds": round(rerank_seconds, 6),
            "doc_top1_score": round(top1_score, 4) if top1_score is not None else None,
            "doc_top1_score_before_rerank": round(top1_score_before, 4) if top1_score_before is not None else None,
            "doc_top1_identity_changed": top1_identity_changed,
            "doc_top1_rank_shift": round(top1_rank_shift, 4) if top1_rank_shift is not None else None,
            "no_context": no_context,
        },
    )
