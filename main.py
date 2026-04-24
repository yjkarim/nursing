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

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from app.core.config import HERE
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
    print("ENV TG_API_ID =", os.environ.get("TG_API_ID"))

app = FastAPI(
    title="TG PDF Stream",
    version="3.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url=None,
)

app.include_router(router)


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def index() -> HTMLResponse:
    html = (HERE / "templates" / "index.html").read_text("utf-8")
    return HTMLResponse(html)
