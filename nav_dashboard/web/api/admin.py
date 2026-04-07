from __future__ import annotations

from fastapi import APIRouter

from nav_dashboard.web.services.operations import app_control_service


router = APIRouter()


@router.get("/healthz")
def healthz() -> dict[str, str]:
    return app_control_service.healthz()


@router.post("/api/shutdown")
async def api_shutdown() -> dict[str, str]:
    app_control_service.request_shutdown()
    return {"status": "shutting_down"}