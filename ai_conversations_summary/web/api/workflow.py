from __future__ import annotations

import base64
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from web.services import workflow_service

router = APIRouter(prefix="/api/workflow", tags=["workflow"])


class ConfigSavePayload(BaseModel):
    base_url: str = ""
    model: str = ""
    api_key: str = ""


class StatsQueryPayload(BaseModel):
    start_date: str = ""
    end_date: str = ""


class RunPayload(BaseModel):
    action: str = Field(min_length=1)
    source: str = "deepseek"
    start_date: str = ""
    end_date: str = ""
    base_url: str = ""
    model: str = ""
    api_key: str = ""
    dry_run: bool = False
    embedding_model: str = ""


class UploadFileItem(BaseModel):
    name: str = ""
    content_base64: str = ""


class UploadPayload(BaseModel):
    files: list[UploadFileItem] = Field(default_factory=list)


class CreateFilePayload(BaseModel):
    file_name: str = ""
    content: str = ""


@router.get("/config")
def get_config() -> dict[str, str]:
    return workflow_service.get_workflow_config()


@router.post("/config/save")
def save_config(payload: ConfigSavePayload) -> dict[str, Any]:
    try:
        return workflow_service.save_workflow_config(payload.base_url, payload.model, payload.api_key)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/stats")
def get_stats(payload: StatsQueryPayload) -> dict[str, Any]:
    return workflow_service.get_extracted_stats(payload.start_date, payload.end_date)


@router.post("/upload")
def upload_raw(payload: UploadPayload) -> dict[str, Any]:
    blobs: list[tuple[str, bytes]] = []
    for item in payload.files:
        name = str(item.name or "").strip()
        raw = str(item.content_base64 or "").strip()
        if not name or not raw:
            continue
        try:
            content = base64.b64decode(raw, validate=False)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid base64 content for file: {name}") from exc
        if not content:
            continue
        blobs.append((name, content))
    return workflow_service.save_uploaded_raw_files(blobs)


@router.post("/create-file")
def create_file(payload: CreateFilePayload) -> dict[str, Any]:
    try:
        return workflow_service.create_extracted_markdown(payload.file_name, payload.content)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/run")
def run_action(payload: RunPayload) -> dict[str, Any]:
    try:
        data = workflow_service.start_job(payload.action, payload.model_dump())
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return data


@router.get("/active")
def get_active() -> dict[str, Any]:
    active = workflow_service.get_active_job()
    return {"active": active}


@router.get("/startup-status")
def get_startup_status() -> dict[str, Any]:
    return workflow_service.get_startup_status()


@router.get("/jobs/{job_id}")
def get_job(job_id: str) -> dict[str, Any]:
    try:
        return workflow_service.get_job(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Job not found") from None
