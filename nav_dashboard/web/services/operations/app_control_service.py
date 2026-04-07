from __future__ import annotations

import asyncio
from typing import Any


_uvicorn_server: Any = None


def register_uvicorn_server(server: Any) -> None:
    global _uvicorn_server
    _uvicorn_server = server


def healthz() -> dict[str, str]:
    return {"status": "ok"}


def request_shutdown(*, delay_seconds: float = 0.25) -> None:
    async def _stop() -> None:
        await asyncio.sleep(delay_seconds)
        if _uvicorn_server is not None:
            _uvicorn_server.should_exit = True

    asyncio.create_task(_stop())