from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from web.services import preview_service

router = APIRouter(prefix="/api/preview", tags=["preview"])


@router.get("/tree")
def get_tree() -> dict[str, object]:
    return {"tree": preview_service.build_documents_tree()}


@router.get("/file")
def get_file(path: str = Query(..., description="Relative markdown path under documents")) -> dict[str, str]:
    try:
        text = preview_service.read_markdown(path)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="File not found") from None
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"path": path, "markdown": text}


@router.get("/search/keyword")
def search_keyword(q: str = Query(..., min_length=1), limit: int = Query(50, ge=1, le=200)) -> dict[str, object]:
    hits = preview_service.keyword_search(q, max_results=limit)
    return {
        "query": q,
        "count": len(hits),
        "results": [{"path": h.path, "score": h.score, "topic": h.topic} for h in hits],
    }


@router.get("/search/vector")
def search_vector(
    q: str = Query(..., min_length=1),
    top_k: int = Query(10, ge=1, le=50),
    embedding_model: str = Query(""),
) -> dict[str, object]:
    try:
        hits = preview_service.vector_search(q, top_k=top_k, embedding_model=embedding_model or None)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "query": q,
        "count": len(hits),
        "embedding_model": embedding_model or preview_service.resolve_embedding_model(),
        "results": [{"path": h.path, "score": h.score, "topic": h.topic} for h in hits],
    }
