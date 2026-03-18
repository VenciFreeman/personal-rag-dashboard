from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from web.api.library import router as library_router
from web.config import HOST, PORT
from web.settings import MEDIA_DIR

APP_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(APP_DIR / "templates"))

app = FastAPI(title="Library Tracker Web UI", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=str(APP_DIR / "static")), name="static")
MEDIA_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/media", StaticFiles(directory=str(MEDIA_DIR)), name="media")
app.include_router(library_router)


@app.on_event("startup")
async def _startup_embedding_check() -> None:
    """On startup, finish any embedding/graph rebuilds that were interrupted last run.

    Delayed by 8 seconds so the first user requests are not competing with
    the embedding worker for CPU / IO.
    """
    import threading
    import time
    from web.services import library_service

    def _run():
        time.sleep(8)
        try:
            library_service.refresh_pending_embeddings()
        except Exception:  # noqa: BLE001
            pass

    threading.Thread(target=_run, daemon=True, name="lib-startup-embed-check").start()


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


def run() -> None:
    import uvicorn

    uvicorn.run("web.main:app", host=HOST, port=PORT, reload=False)


if __name__ == "__main__":
    run()
