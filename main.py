"""
TG PDF Stream — FastAPI entry point
====================================
Local dev:   uvicorn main:app --reload
Production:  gunicorn main:app -k uvicorn.workers.UvicornWorker --workers 1 --threads 4 --timeout 120

Why --workers 1?
  Telethon's TelegramClient is NOT safe to share across processes.
  Use threads (--threads N) for concurrency, not multiple workers.
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI , HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from app.core.config import HERE, GROUPS  # adjust import if GROUPS lives elsewhere
from app.core.logging import configure_logging
from app.api.routes import router
from app.services.redis_service import redis_svc
from app.services.telegram_service import telegram_svc

configure_logging(os.environ.get("LOG_LEVEL", "INFO"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ────────────────────────────────────────────────────────────────
    await redis_svc.connect()
    await telegram_svc.start()
    yield
    # ── Shutdown ───────────────────────────────────────────────────────────────
    await telegram_svc.stop()
    await redis_svc.close()

app = FastAPI(
    title="TG PDF Stream",
    version="3.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url=None,
)

app.include_router(router)
app.mount("/static", StaticFiles(directory="templates/static"), name="static")

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def index() -> HTMLResponse:
    html = (HERE / "templates" / "index.html").read_text("utf-8")
    return HTMLResponse(html)

@app.get("/{group_name}", response_class=HTMLResponse, include_in_schema=False)
async def group_page(group_name: str) -> HTMLResponse:
    """
    Serves templates/{group_name}.html if the file exists.
    Returns 404 if no matching template is found.
    Never intercepts /api/* because those routes are registered first.
    """
    template = HERE / "templates" / f"{group_name}"
    if not template.exists():
        raise HTTPException(status_code=404, detail=f"Page '{group_name}' not found")
    html = template.read_text("utf-8")
    return HTMLResponse(html)
