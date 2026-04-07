from __future__ import annotations

import sys
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from core_service.auth import install_app_auth
from web.api.agent_boundary import router as agent_boundary_router
from web.api.preview import router as preview_router
from web.api.rag import router as rag_router
from web.api.workflow import router as workflow_router
from web.config import ENABLE_CF_ACCESS_HEADERS, HOST, PORT

APP_DIR = Path(__file__).resolve().parent
CORE_STATIC_DIR = APP_DIR.parents[1] / "core_service" / "static"
templates = Jinja2Templates(directory=str(APP_DIR / "templates"))

app = FastAPI(title="RAG System Web UI", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=str(APP_DIR / "static")), name="static")
app.mount("/core-static", StaticFiles(directory=str(CORE_STATIC_DIR)), name="core-static")
app.include_router(agent_boundary_router)
app.include_router(preview_router)
app.include_router(rag_router)
app.include_router(workflow_router)
install_app_auth(app, app_id="rag_system", app_title="RAG System")


@app.middleware("http")
async def cf_access_identity_middleware(request: Request, call_next):
    # Future extension: Cloudflare Access identity can be trusted when explicitly enabled.
    if ENABLE_CF_ACCESS_HEADERS:
        request.state.cf_email = request.headers.get("CF-Access-Authenticated-User-Email", "")
    response = await call_next(request)
    return response


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(request=request, name="index.html", context={"request": request})


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


def run() -> None:
    import uvicorn

    uvicorn.run("web.main:app", host=HOST, port=PORT, reload=False)


if __name__ == "__main__":
    run()
