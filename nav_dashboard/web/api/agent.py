"""nav_dashboard/web/api/agent.py
Agent 聊天 API 路由（/api/agent）

端点：
  GET  /api/agent/sessions          — 列出所有会话（按 updated_at 倒序）
  POST /api/agent/sessions          — 新建会话，返回 session 对象
  DELETE /api/agent/sessions/{id}   — 删除指定会话及其记忆文件
  POST /api/agent/chat              — 执行一轮 Agent 对话，返回完整回复
  POST /api/agent/chat_stream       — 执行一轮 Agent 对话，SSE 流式进度推送

ChatPayload 参数：
  question          — 用户问题
  session_id        — 可选，不传则自动创建新会话
  history           — 客户端传入的对话历史（备用，服务端以文件为准）
  backend           — "local"（默认）| "deepseek"
  search_mode       — "local_only"（默认）| "hybrid"（允许联网搜索）
  confirm_over_quota — 当配额超限时用户已确认继续
  deny_over_quota    — 用户拒绝配额超限操作
  debug             — 是否落盘 debug 数据

实际会话逻辑全部由 web.services.agent_service 实现（见该模块注释）。
"""
from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from web.services import agent_service

router = APIRouter(prefix="/api/agent", tags=["agent"])


def _first_forwarded_value(value: str) -> str:
    return str(value or "").split(",", 1)[0].strip()


def _public_request_base_url(request: Request) -> str:
    forwarded = _first_forwarded_value(request.headers.get("forwarded", ""))
    forwarded_host = ""
    forwarded_proto = ""
    if forwarded:
        for segment in forwarded.split(";"):
            key, _, raw_value = segment.partition("=")
            if not _:
                continue
            normalized_key = key.strip().lower()
            normalized_value = raw_value.strip().strip('"')
            if normalized_key == "host" and not forwarded_host:
                forwarded_host = normalized_value
            elif normalized_key == "proto" and not forwarded_proto:
                forwarded_proto = normalized_value

    host = (
        _first_forwarded_value(request.headers.get("x-forwarded-host", ""))
        or forwarded_host
        or str(request.headers.get("host", "")).strip()
        or str(request.url.netloc or "").strip()
    ).rstrip("/")
    scheme = (
        _first_forwarded_value(request.headers.get("x-forwarded-proto", ""))
        or forwarded_proto
        or str(request.url.scheme or "http").strip()
        or "http"
    ).rstrip(":/")
    forwarded_port = _first_forwarded_value(request.headers.get("x-forwarded-port", ""))
    if host and forwarded_port and ":" not in host and not host.startswith("["):
        host = f"{host}:{forwarded_port}"
    if not host:
        hostname = request.url.hostname or "localhost"
        if request.url.port:
            host = f"{hostname}:{request.url.port}"
        else:
            host = hostname
    return f"{scheme}://{host}/"


class ChatPayload(BaseModel):
    question: str = Field(min_length=1)
    session_id: str = ""
    trace_id: str = ""
    history: list[dict[str, str]] = Field(default_factory=list)
    backend: str = "local"
    search_mode: str = "local_only"
    confirm_over_quota: bool = False
    deny_over_quota: bool = False
    debug: bool = False
    benchmark_mode: bool = False


class SessionCreatePayload(BaseModel):
    title: str = "新会话"


class SessionRenamePayload(BaseModel):
    title: str = Field(min_length=1)
    lock: bool = True


@router.get("/sessions")
def get_sessions() -> dict[str, Any]:
    return {"sessions": agent_service.list_sessions()}


@router.post("/sessions")
def post_session(payload: SessionCreatePayload) -> dict[str, Any]:
    return agent_service.create_session(payload.title)


@router.patch("/sessions/{session_id}")
def patch_session(session_id: str, payload: SessionRenamePayload) -> dict[str, Any]:
    session = agent_service.set_session_title(session_id, payload.title, lock=payload.lock)
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found")
    return session


@router.delete("/sessions/{session_id}")
def delete_session(session_id: str) -> dict[str, bool]:
    ok = agent_service.delete_session(session_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Session not found")
    return {"ok": True}


@router.post("/chat")
def post_chat(payload: ChatPayload, request: Request) -> dict[str, Any]:
    try:
        return agent_service.run_agent_round(
            question=payload.question,
            session_id=payload.session_id,
            trace_id=payload.trace_id or str(request.headers.get("X-Trace-Id", "")),
            history=payload.history,
            backend=payload.backend,
            search_mode=payload.search_mode,
            confirm_over_quota=payload.confirm_over_quota,
            deny_over_quota=payload.deny_over_quota,
            debug=payload.debug,
            request_base_url=_public_request_base_url(request),
            benchmark_mode=payload.benchmark_mode,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        msg = str(exc)
        if "Missing dependency: openai" in msg:
            detail = (
                "当前环境缺少 `openai` 依赖，LLM 汇总不可用。"
                "\n建议执行：`pip install openai` 或 `pip install -r nav_dashboard/requirements.txt`。"
                "\n你也可以继续使用自动降级的检索回复模式。"
            )
            raise HTTPException(status_code=500, detail=detail) from exc
        raise HTTPException(status_code=500, detail=msg) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/chat_stream")
def post_chat_stream(payload: ChatPayload, request: Request) -> StreamingResponse:
    """SSE streaming variant of /api/agent/chat.

    Emits newline-delimited SSE frames:
        data: {"type": "progress", "message": "..."}
        data: {"type": "tool_done", "tool": "...", "status": "...", "summary": "..."}
        data: {"type": "quota_exceeded", "message": "...", ...}
        data: {"type": "done", "payload": {...}}
        data: {"type": "error", "message": "..."}
    """

    def _sse_generator():
        try:
            for event in agent_service.run_agent_round_stream(
                question=payload.question,
                session_id=payload.session_id,
                trace_id=payload.trace_id or str(request.headers.get("X-Trace-Id", "")),
                history=payload.history,
                backend=payload.backend,
                search_mode=payload.search_mode,
                confirm_over_quota=payload.confirm_over_quota,
                deny_over_quota=payload.deny_over_quota,
                debug=payload.debug,
                request_base_url=_public_request_base_url(request),
                benchmark_mode=payload.benchmark_mode,
            ):
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
        except Exception as exc:  # noqa: BLE001
            err = {"type": "error", "message": str(exc)}
            yield f"data: {json.dumps(err, ensure_ascii=False)}\n\n"

    return StreamingResponse(
        _sse_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
