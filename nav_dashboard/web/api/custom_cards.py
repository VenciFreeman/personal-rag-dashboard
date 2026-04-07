from __future__ import annotations

from typing import Any

from fastapi import APIRouter, BackgroundTasks, Request
from pydantic import BaseModel

from nav_dashboard.web.services.dashboard import dashboard_custom_cards_service


router = APIRouter()


class CustomCardPayload(BaseModel):
    title: str = ""
    url: str = ""
    image: str = ""


@router.get("/api/custom_cards")
def get_custom_cards(request: Request) -> dict[str, Any]:
    return {"cards": dashboard_custom_cards_service.browser_custom_cards(request)}


@router.post("/api/custom_cards/slot/{index}")
def save_custom_card(index: int, payload: CustomCardPayload, background_tasks: BackgroundTasks, request: Request) -> dict[str, Any]:
    card, _saved_cards = dashboard_custom_cards_service.save_custom_card(index, payload.model_dump())
    background_tasks.add_task(dashboard_custom_cards_service.trigger_custom_card_compression)
    return {
        "ok": True,
        "card": card,
        "cards": dashboard_custom_cards_service.browser_custom_cards(request),
    }


@router.post("/api/custom_cards/upload")
async def upload_custom_card_image(request: Request, filename: str | None = None) -> dict[str, str | bool]:
    image = dashboard_custom_cards_service.upload_custom_card_image(
        filename=str(filename or "").strip(),
        content_type=request.headers.get("content-type") or "",
        content=await request.body(),
    )
    return {"ok": True, "image": image}