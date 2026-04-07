from __future__ import annotations

from pathlib import Path
import sys
import threading


WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from core_service.auth import install_app_auth
from web.api.library import router as library_router
from web.config import HOST, PORT
from web.services import library_analysis_report_store
from web.settings import MEDIA_DIR

APP_DIR = Path(__file__).resolve().parent
CORE_STATIC_DIR = APP_DIR.parents[1] / "core_service" / "static"
templates = Jinja2Templates(directory=str(APP_DIR / "templates"))


def _local_asset_version() -> str:
    version_parts: list[int] = []
    for path in [
        APP_DIR / "static" / "app.css",
        APP_DIR / "static" / "app.js",
        APP_DIR / "static" / "analysis.js",
        APP_DIR / "static" / "modules" / "query_workspace.js",
        APP_DIR / "static" / "modules" / "editor.js",
        APP_DIR / "static" / "modules" / "stats.js",
    ]:
        try:
            version_parts.append(int(path.stat().st_mtime_ns))
        except OSError:
            continue
    return str(max(version_parts)) if version_parts else "1"

app = FastAPI(title="Library Tracker Web UI", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=str(APP_DIR / "static")), name="static")
app.mount("/core-static", StaticFiles(directory=str(CORE_STATIC_DIR)), name="core-static")
MEDIA_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/media", StaticFiles(directory=str(MEDIA_DIR)), name="media")
app.include_router(library_router)
install_app_auth(app, app_id="library_tracker", app_title="Library Tracker")


def _start_scheduler_background() -> None:
    try:
        library_analysis_report_store.start_scheduler()
    except Exception:
        return


@app.on_event("startup")
async def _startup() -> None:
    threading.Thread(target=_start_scheduler_background, name="library-web-scheduler-bootstrap", daemon=True).start()


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "request": request,
            "local_asset_version": _local_asset_version(),
        },
    )


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


def run() -> None:
    import uvicorn

    uvicorn.run(app, host=HOST, port=PORT, reload=False)


if __name__ == "__main__":
    run()
