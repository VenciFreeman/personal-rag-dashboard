from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from ai_conversations_summary.runtime_paths import VECTOR_DB_DIR
from scripts.cache_db import get_web_cache, log_no_context_query
from scripts.rag_knowledge_graph import expand_query_by_graph

router = APIRouter(prefix="/api/agent-boundary", tags=["agent-boundary"])

_DOC_GRAPH_DIR = VECTOR_DB_DIR


class GraphExpandRequest(BaseModel):
    query: str = ""
    max_expand: int = Field(default=6, ge=1, le=24)


class WebCacheSetRequest(BaseModel):
    query: str = ""
    max_results: int = Field(default=10, ge=1, le=50)
    results: list[dict[str, Any]] = Field(default_factory=list)


class NoContextLogRequest(BaseModel):
    query: str = ""
    source: str = "agent"
    top1_score: float | None = None
    threshold: float | None = None
    trace_id: str = ""
    reason: str = ""


@router.post("/doc-graph/expand")
def expand_doc_graph(req: GraphExpandRequest) -> dict[str, Any]:
    try:
        return expand_query_by_graph(_DOC_GRAPH_DIR, str(req.query or ""), max_expand=int(req.max_expand or 6))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/web-cache")
def get_web_cache_entry(
    query: str = Query("", min_length=0),
    max_results: int = Query(10, ge=1, le=50),
) -> dict[str, Any]:
    results = get_web_cache().get(str(query or ""), int(max_results or 10))
    return {
        "hit": results is not None,
        "results": [item for item in (results or []) if isinstance(item, dict)],
    }


@router.post("/web-cache")
def set_web_cache_entry(req: WebCacheSetRequest) -> dict[str, Any]:
    clean_results = [dict(item) for item in req.results if isinstance(item, dict)]
    get_web_cache().set(str(req.query or ""), int(req.max_results or 10), clean_results)
    return {"ok": True, "count": len(clean_results)}


@router.post("/no-context-log")
def append_no_context_log(req: NoContextLogRequest) -> dict[str, Any]:
    log_no_context_query(
        str(req.query or ""),
        source=str(req.source or "agent"),
        top1_score=req.top1_score,
        threshold=req.threshold,
        trace_id=str(req.trace_id or ""),
        reason=str(req.reason or ""),
    )
    return {"ok": True}
