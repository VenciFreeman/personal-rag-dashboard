from __future__ import annotations

import subprocess
from typing import Any

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from pydantic import BaseModel, Field

from web.services import library_graph, library_service
from web.settings import VECTOR_DB_DIR

router = APIRouter(prefix="/api/library", tags=["library"])


def _trigger_cover_compression() -> None:
    """
    Background task: asynchronously compress oversized cover images.
    Runs in a separate thread without blocking the response.
    """
    try:
        from pathlib import Path
        import sys
        
        script_path = Path(__file__).resolve().parents[2] / "scripts" / "compress_covers.py"
        if not script_path.exists():
            return
        
        # Run compression script in background thread
        subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            timeout=300,  # 5 min timeout
            check=False,
        )
    except Exception:  # noqa: BLE001
        # Silently ignore compression errors to avoid blocking user response
        pass


def _trigger_embedding_refresh() -> None:
    """Background task: recompute embeddings and sync graph for any pending items."""
    try:
        library_service.refresh_pending_embeddings()
    except Exception:  # noqa: BLE001
        pass


class SearchRequest(BaseModel):
    query: str = ""
    mode: str = Field(default="keyword", description="keyword or vector")
    limit: int = Field(default=50, ge=1, le=500)
    offset: int = Field(default=0, ge=0, le=100000)
    filters: dict[str, list[str]] = Field(default_factory=dict)


class FacetRequest(BaseModel):
    filters: dict[str, list[str]] = Field(default_factory=dict)


class ItemPayload(BaseModel):
    item: dict[str, Any]


class EmbeddingRefreshItemsRequest(BaseModel):
    item_ids: list[str] = Field(default_factory=list)


class GraphExpandRequest(BaseModel):
    query: str = ""
    max_expand: int = Field(default=6, ge=1, le=24)


@router.get("/meta")
def get_meta() -> dict[str, Any]:
    return {"filters": library_service.get_filter_options()}


@router.get("/suggestions")
def get_suggestions() -> dict[str, Any]:
    return {"fields": library_service.get_form_suggestions()}


@router.post("/facets")
def get_facets(req: FacetRequest) -> dict[str, Any]:
    return {"facets": library_service.get_facet_counts(req.filters)}


@router.get("/bootstrap")
def get_bootstrap(limit: int = 50) -> dict[str, Any]:
    """Single-request cold-start data: filter options + suggestions + facets + first page.

    Replaces the sequential /meta + /suggestions + /facets + /search calls that
    the frontend issued on every page load.  All four data sets are computed in
    one pass (with caching) and returned together.
    """
    return library_service.get_bootstrap_data(initial_query="", initial_limit=max(1, min(limit, 200)))


@router.get("/stats/overview")
def get_stats_overview() -> dict[str, Any]:
    return library_service.get_stats_overview()


@router.get("/stats/pie")
def get_stats_pie(field: str = "category", year: int | None = None) -> dict[str, Any]:
    try:
        return library_service.get_stats_pie(field=field, year=year)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/cover")
async def upload_cover(
    request: Request,
    filename: str | None = None,
    title: str | None = None,
    overwrite_path: str | None = None,
) -> dict[str, Any]:
    content_type = (request.headers.get("content-type") or "").split(";")[0].strip().lower()
    if not content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="Content-Type must be image/*")

    data = await request.body()
    if not data:
        raise HTTPException(status_code=400, detail="Empty file body")

    try:
        rel_path = library_service.save_cover_bytes(
            data,
            content_type=content_type,
            original_filename=filename,
            title=title,
            overwrite_path=overwrite_path,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {"path": rel_path}


@router.post("/search")
def search(req: SearchRequest) -> dict[str, Any]:
    return library_service.search_items(
        query=req.query,
        mode=req.mode,
        filters=req.filters,
        limit=req.limit,
        offset=req.offset,
    )


@router.post("/embedding/refresh")
def refresh_embedding() -> dict[str, Any]:
    stats = library_service.refresh_pending_embeddings()
    return {"ok": True, **stats}


@router.post("/embedding/refresh-items")
def refresh_embedding_items(req: EmbeddingRefreshItemsRequest) -> dict[str, Any]:
    stats = library_service.refresh_embeddings_for_item_ids(req.item_ids)
    return {"ok": True, **stats}


@router.post("/graph/rebuild")
def rebuild_graph() -> dict[str, Any]:
    return library_service.rebuild_library_graph()


@router.post("/graph/sync-missing")
def sync_missing_graph() -> dict[str, Any]:
    return library_service.sync_missing_library_graph()


@router.post("/graph/rebuild-job")
def rebuild_graph_job() -> dict[str, Any]:
    return {"ok": True, "job": library_service.start_graph_job(full=True)}


@router.post("/graph/sync-missing-job")
def sync_missing_graph_job() -> dict[str, Any]:
    return {"ok": True, "job": library_service.start_graph_job(full=False)}


@router.post("/agent-boundary/graph/expand")
def expand_graph_query(req: GraphExpandRequest) -> dict[str, Any]:
    return library_graph.expand_library_query(
        graph_dir=VECTOR_DB_DIR,
        query=str(req.query or ""),
        max_expand=max(1, int(req.max_expand or 6)),
    )


@router.get("/graph/jobs/{job_id}")
def get_graph_job(job_id: str) -> dict[str, Any]:
    job = library_service.get_graph_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Graph job not found")
    return {"ok": True, "job": job}


@router.get("/item/{item_id}")
def get_item(item_id: str) -> dict[str, Any]:
    try:
        item = library_service.get_item(item_id)
    except library_service.BadItemIdError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except library_service.ItemNotFoundError:
        raise HTTPException(status_code=404, detail="Item not found") from None
    return {"item": item}


@router.post("/item")
def create_item(req: ItemPayload, background_tasks: BackgroundTasks) -> dict[str, Any]:
    try:
        item = library_service.add_item(req.item)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    
    # Trigger cover compression and embedding refresh in background
    background_tasks.add_task(_trigger_cover_compression)
    background_tasks.add_task(_trigger_embedding_refresh)
    
    return {"item": item}


@router.put("/item/{item_id}")
def edit_item(item_id: str, req: ItemPayload, background_tasks: BackgroundTasks) -> dict[str, Any]:
    try:
        item = library_service.update_item(item_id, req.item)
    except library_service.BadItemIdError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except library_service.ItemNotFoundError:
        raise HTTPException(status_code=404, detail="Item not found") from None
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    
    # Trigger cover compression and embedding refresh in background
    background_tasks.add_task(_trigger_cover_compression)
    background_tasks.add_task(_trigger_embedding_refresh)
    
    return {"item": item}
