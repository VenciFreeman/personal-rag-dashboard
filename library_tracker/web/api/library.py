from __future__ import annotations

import subprocess
import threading
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from web.services import analysis_service
from web.services import library_alias_store, library_graph, library_service
from web.settings import VECTOR_DB_DIR

router = APIRouter(prefix="/api/library", tags=["library"])


def _run_cover_compression() -> None:
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


def _trigger_cover_compression_background() -> None:
    threading.Thread(target=_run_cover_compression, name="library-cover-compression", daemon=True).start()


class SearchRequest(BaseModel):
    query: str = ""
    mode: str = Field(default="keyword", description="keyword or vector")
    limit: int = Field(default=50, ge=1, le=500)
    offset: int = Field(default=0, ge=0, le=100000)
    filters: dict[str, list[str]] = Field(default_factory=dict)


class AliasResolveRequest(BaseModel):
    query: str = ""
    media_type_hint: str = ""
    max_entries: int = Field(default=8, ge=1, le=64)


class FacetRequest(BaseModel):
    filters: dict[str, list[str]] = Field(default_factory=dict)


class ItemPayload(BaseModel):
    item: dict[str, Any]


class EmbeddingRefreshItemsRequest(BaseModel):
    item_ids: list[str] = Field(default_factory=list)


class GraphExpandRequest(BaseModel):
    query: str = ""
    max_expand: int = Field(default=6, ge=1, le=24)


class AnalysisGenerateIn(BaseModel):
    kind: str
    backend: str = "local"
    period_key: str | None = None


class AnalysisReportActionIn(BaseModel):
    kind: str
    period_key: str
    source: str | None = None


class LibraryImportIn(BaseModel):
    replace_existing: bool = False
    contract: str = ""
    version: int = 1
    exported_at: str = ""
    media_payloads: dict[str, dict[str, Any]] = Field(default_factory=dict)
    alias_buckets: dict[str, dict[str, Any]] = Field(default_factory=dict)
    concept_ontology: dict[str, Any] = Field(default_factory=dict)


class AliasProposalReviewIn(BaseModel):
    proposal_id: str
    action: str = Field(description="accept, reject, modify, or keep_original")
    canonical_name: str = ""
    aliases: list[str] = Field(default_factory=list)


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


@router.get("/stats/dashboard")
def get_stats_dashboard(field: str = "category", year: int | None = None) -> dict[str, Any]:
    try:
        return library_service.get_stats_dashboard(field=field, year=year)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/export")
def export_library() -> dict[str, Any]:
    return library_service.export_library_contract()


@router.post("/import")
def import_library(body: LibraryImportIn) -> dict[str, Any]:
    return library_service.import_library_contract(
        body.model_dump(),
        replace_existing=body.replace_existing,
    )


@router.get("/alias-proposals/summary")
def get_alias_proposal_summary() -> dict[str, Any]:
    return library_alias_store.get_alias_proposal_summary()


@router.get("/alias-proposals")
def get_alias_proposals(page: int = 1, page_size: int = 10) -> dict[str, Any]:
    return library_alias_store.list_proposals(page=page, page_size=page_size)


@router.post("/alias-proposals/review")
def review_alias_proposal(body: AliasProposalReviewIn) -> dict[str, Any]:
    try:
        result = library_alias_store.review_proposal(
            proposal_id=body.proposal_id,
            action=body.action,
            canonical_name=body.canonical_name,
            aliases=body.aliases,
        )
        library_service.invalidate_search_cache()
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/alias-resolve")
def alias_resolve(req: AliasResolveRequest) -> dict[str, Any]:
    return library_alias_store.resolve_query_aliases(
        req.query,
        media_type_hint=req.media_type_hint,
        max_entries=req.max_entries,
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
@router.get("/analysis/state")
def get_analysis_state() -> dict[str, Any]:
    return analysis_service.get_analysis_state()


@router.get("/analysis/report")
def get_analysis_report(kind: str, period_key: str | None = None, source: str | None = None) -> dict[str, Any]:
    try:
        report = analysis_service.read_report(kind, period_key=period_key, source=source)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if report is None:
        raise HTTPException(status_code=404, detail="analysis report not found")
    return report


@router.delete("/analysis/report")
def delete_analysis_report(kind: str, period_key: str, source: str | None = None) -> dict[str, Any]:
    try:
        result = analysis_service.delete_report(kind, period_key=period_key, source=source)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {**result, "state": analysis_service.get_analysis_state()}


@router.post("/analysis/report/open-location")
def open_analysis_report_location(body: AnalysisReportActionIn) -> dict[str, Any]:
    try:
        return analysis_service.open_report_location(body.kind, body.period_key, source=body.source)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/analysis/generate")
def generate_analysis(body: AnalysisGenerateIn) -> dict[str, Any]:
    try:
        report = analysis_service.generate_report(body.kind, body.backend, period_key=body.period_key, manual=True)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"ok": True, "report": report, "state": analysis_service.get_analysis_state()}


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
def create_item(req: ItemPayload) -> dict[str, Any]:
    try:
        item = library_service.add_item(req.item)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    _trigger_cover_compression_background()
    background_refresh = library_service.enqueue_item_refresh([str(item.get("id") or "")])

    return {"item": item, "background_refresh": background_refresh}


@router.put("/item/{item_id}")
def edit_item(item_id: str, req: ItemPayload) -> dict[str, Any]:
    try:
        item = library_service.update_item(item_id, req.item)
    except library_service.BadItemIdError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except library_service.ItemNotFoundError:
        raise HTTPException(status_code=404, detail="Item not found") from None
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    _trigger_cover_compression_background()
    background_refresh = library_service.enqueue_item_refresh([str(item.get("id") or "")])

    return {"item": item, "background_refresh": background_refresh}


@router.delete("/item/{item_id}")
def delete_item(item_id: str) -> dict[str, Any]:
    try:
        result = library_service.delete_item(item_id)
    except library_service.BadItemIdError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except library_service.ItemNotFoundError:
        raise HTTPException(status_code=404, detail="Item not found") from None
    background_refresh = library_service.trigger_alias_maintenance_background()
    return {**result, "background_refresh": background_refresh}
