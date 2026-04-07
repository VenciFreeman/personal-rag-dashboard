from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from core_service.feedback import append_feedback
from web.services import dashboard_service, rag_service
from web.services.rag_service import RAGTaskAborted

router = APIRouter(prefix="/api/rag", tags=["rag"])


class AskPayload(BaseModel):
    question: str = Field(min_length=1)
    session_id: str = ""
    trace_id: str = ""
    mode: str = "local"
    api_url: str = ""
    api_key: str = ""
    model: str = ""
    embedding_model: str = ""
    search_mode: str = "hybrid"
    top_k: int = 5
    similarity_threshold: float = 0.4
    debug: bool = False
    confirm_over_quota: bool = False
    no_embed_cache: bool = False
    benchmark_mode: bool = False


class SessionCreatePayload(BaseModel):
    title: str = "新会话"


class SessionRenamePayload(BaseModel):
    title: str = Field(min_length=1)
    lock: bool = True


class AbortPayload(BaseModel):
    session_id: str = Field(min_length=1)


class FeedbackPayload(BaseModel):
    question: str = ""
    answer: str = Field(min_length=1)
    trace_id: str = ""
    session_id: str = ""
    model: str = ""
    search_mode: str = ""
    metadata: dict[str, object] = Field(default_factory=dict)


@router.get("/config")
def get_rag_config() -> dict[str, str]:
    cfg = rag_service.default_chat_config()
    cfg["embedding_model_display"] = rag_service.readable_model_name(cfg["embedding_model"])
    return cfg


@router.get("/sessions")
def get_sessions() -> dict[str, object]:
    return {"sessions": rag_service.list_sessions_summary()}


@router.get("/dashboard/overview")
def get_dashboard_overview() -> dict[str, object]:
    return dashboard_service.get_dashboard_overview()


@router.get("/dashboard/missing-queries")
def get_dashboard_missing_queries(days: int = 30, limit: int = 200, source: str = "") -> dict[str, object]:
    return {"items": dashboard_service.list_missing_queries(days=days, limit=limit, source=source)}


@router.get("/sessions/{session_id}")
def get_session(session_id: str) -> dict[str, object]:
    session = rag_service.get_session(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found")
    return session

@router.post("/sessions")
def post_session(payload: SessionCreatePayload) -> dict[str, object]:
    return rag_service.create_session(payload.title)


@router.patch("/sessions/{session_id}")
def patch_session(session_id: str, payload: SessionRenamePayload) -> dict[str, object]:
    session = rag_service.set_session_title(session_id, payload.title, lock=payload.lock)
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found")
    return session


@router.delete("/sessions/{session_id}")
def delete_session(session_id: str) -> dict[str, bool]:
    ok = rag_service.delete_session(session_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Session not found")
    return {"ok": True}


@router.post("/ask")
def post_ask(payload: AskPayload, request: Request) -> dict[str, object]:
    is_benchmark = payload.benchmark_mode
    session_id = payload.session_id.strip()
    request_trace_id = payload.trace_id or str(request.headers.get("X-Trace-Id", ""))

    if is_benchmark:
        # Benchmark mode: skip all session/memory operations so benchmark queries
        # never appear in session history or pollute dashboard metrics.
        if not session_id:
            session_id = "_benchmark_"
    else:
        if not session_id:
            session = rag_service.create_session()
            session_id = str(session["id"])
        rag_service.append_message_with_trace(session_id, "用户", payload.question.strip(), trace_id=request_trace_id)

    try:
        answer_payload = rag_service.ask_rag(
            session_id=session_id,
            trace_id=request_trace_id,
            mode=payload.mode,
            question=payload.question,
            api_url=payload.api_url,
            api_key=payload.api_key,
            model=payload.model,
            embedding_model=payload.embedding_model,
            search_mode=payload.search_mode,
            top_k=payload.top_k,
            similarity_threshold=payload.similarity_threshold,
            debug=payload.debug,
            no_embed_cache=payload.no_embed_cache,
            benchmark_mode=payload.benchmark_mode,
        )
    except RAGTaskAborted:
        if not is_benchmark:
            rag_service.append_message(session_id, "系统", "已中止")
        return {
            "session_id": session_id,
            "trace_id": request_trace_id,
            "aborted": True,
            "answer": "",
            "mode": (payload.mode or "local").strip().lower() or "local",
        }
    except Exception as exc:  # noqa: BLE001
        if not is_benchmark:
            rag_service.append_message(session_id, "助手", f"RAG Q&A 失败：{exc}")
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    answer = str(answer_payload.get("answer", "")).strip()
    mode = str(answer_payload.get("mode", payload.mode)).strip().lower() or "local"
    used_docs = answer_payload.get("used_context_docs", [])
    docs = used_docs if isinstance(used_docs, list) else []
    answer = rag_service.format_answer_with_refs(answer, docs, mode=mode)
    answer_payload["answer"] = answer

    if not is_benchmark:
        if answer:
            rag_service.append_message_with_trace(session_id, "助手", answer, trace_id=request_trace_id)
            rag_service.schedule_memory_update(session_id, payload.question, answer)
            rag_service.schedule_generated_session_title(session_id, payload.question, answer, lock=True)

    answer_payload["session_id"] = session_id
    answer_payload["mode"] = mode
    answer_payload["embedding_model_display"] = rag_service.readable_model_name(str(answer_payload.get("embedding_model", "")))
    return answer_payload


@router.post("/ask_stream")
def post_ask_stream(payload: AskPayload, request: Request) -> StreamingResponse:
    session_id = payload.session_id.strip()
    request_trace_id = payload.trace_id or str(request.headers.get("X-Trace-Id", ""))
    if not session_id:
        session = rag_service.create_session()
        session_id = str(session["id"])

    rag_service.append_message_with_trace(session_id, "用户", payload.question.strip(), trace_id=request_trace_id)

    def event_stream():
        try:
            for event in rag_service.ask_rag_stream(
                session_id=session_id,
                trace_id=request_trace_id,
                mode=payload.mode,
                question=payload.question,
                api_url=payload.api_url,
                api_key=payload.api_key,
                model=payload.model,
                embedding_model=payload.embedding_model,
                search_mode=payload.search_mode,
                top_k=payload.top_k,
                similarity_threshold=payload.similarity_threshold,
                debug=payload.debug,
                confirm_over_quota=payload.confirm_over_quota,
            ):
                payload_text = json.dumps(event, ensure_ascii=False)
                yield f"data: {payload_text}\n\n"
        except Exception as exc:  # noqa: BLE001
            err = json.dumps({"type": "error", "trace_id": request_trace_id, "message": str(exc)}, ensure_ascii=False)
            yield f"data: {err}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/abort")
def post_abort(payload: AbortPayload) -> dict[str, bool]:
    ok = rag_service.abort_session(payload.session_id)
    return {"ok": ok}


@router.post("/feedback")
def post_feedback(payload: FeedbackPayload) -> dict[str, object]:
    try:
        item = append_feedback(
            {
                "source": "rag_chat",
                "question": payload.question,
                "answer": payload.answer,
                "trace_id": payload.trace_id,
                "session_id": payload.session_id,
                "model": payload.model,
                "search_mode": payload.search_mode,
                "metadata": payload.metadata,
            }
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "item": item}
