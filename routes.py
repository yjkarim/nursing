"""
API routes — clean, thin handlers.

Each endpoint delegates to services; no business logic lives here.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse

from app.core.config import MIN_HITS_TO_CACHE
from app.services import cache_service as cs
from app.services import redis_service as rs
from app.services import telegram_service as ts

log = logging.getLogger("tg.routes")
router = APIRouter()


# ── GET /api/files ─────────────────────────────────────────────────────────────

@router.get("/api/files")
async def list_files() -> list[dict]:
    """
    Return cached PDF metadata.
    Priority: Redis → disk fallback.
    """
    data = await rs.redis_svc.get_meta()
    if data is None:
        data = cs.load_meta_disk()
    return data


# ── POST /api/refresh ──────────────────────────────────────────────────────────

@router.post("/api/refresh")
async def refresh(full: bool = Query(False)) -> dict:
    """
    Sync PDF metadata from Telegram.

    - Incremental by default: only fetches messages newer than the last scan.
    - Pass ?full=true to force a complete re-scan from the beginning.

    The incremental scan uses min_id stored in Redis to skip already-seen
    messages — much faster for large groups.
    """
    try:
        # Incremental: start from last known message id
        min_id = 0 if full else await rs.redis_svc.get_last_msg_id()

        new_pdfs, max_id = await ts.telegram_svc.fetch_pdfs(min_id=min_id)

        if new_pdfs or full:
            if full:
                # Full scan replaces everything
                merged = new_pdfs
            else:
                # Incremental: merge new entries into existing metadata
                existing = await rs.redis_svc.get_meta() or cs.load_meta_disk()
                existing_ids = {f["id"] for f in existing}
                merged = existing + [f for f in new_pdfs if f["id"] not in existing_ids]

            await rs.redis_svc.set_meta(merged)
            cs.save_meta_disk(merged)
        else:
            merged = await rs.redis_svc.get_meta() or cs.load_meta_disk()

        # Persist the highest message id for next incremental scan
        if max_id > min_id:
            await rs.redis_svc.set_last_msg_id(max_id)

        log.info(
            "Refresh complete: new=%d total=%d min_id=%d→%d full=%s",
            len(new_pdfs), len(merged), min_id, max_id, full,
        )
        return {
            "ok":        True,
            "new":       len(new_pdfs),
            "total":     len(merged),
            "files":     merged,
        }

    except Exception as exc:
        log.error("Refresh failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))


# ── GET /api/stream/{message_id} ───────────────────────────────────────────────

@router.get("/api/stream/{message_id}")
async def stream_pdf(
    request: Request,
    message_id: int,
    dl: str = Query("0"),
) -> Response:
    """
    Serve a PDF to the client.

    Cache lookup order:
      1. Disk cache (file exists AND marked ready in Redis) → FileResponse / StreamingResponse with Range
      2. Telegram live download → StreamingResponse (+ optional disk write)

    Concurrency:
      • Only ONE request downloads a file from Telegram at a time (Redis lock).
      • Concurrent requests for the same uncached file wait (up to LOCK_TTL)
        for the first download to finish, then serve from disk.
      • If the wait times out, they stream from Telegram without writing.
    """
    # ── Resolve file metadata ──────────────────────────────────────────────────
    meta  = await rs.redis_svc.get_meta() or cs.load_meta_disk()
    entry = next((f for f in meta if f["id"] == message_id), None)
    name  = entry["name"] if entry else f"document_{message_id}.pdf"
    size: Optional[int] = entry.get("size") if entry else None

    disk = cs.pdf_path(message_id)

    # ── 1. Disk cache HIT ──────────────────────────────────────────────────────
    #
    # We check BOTH that the file exists on disk AND that Redis has marked it
    # as fully written. This prevents serving a .pdf file that is still being
    # written by another request (race condition fix).
    if disk.exists() and await rs.redis_svc.is_disk_ready(message_id):
        file_size = disk.stat().st_size
        etag      = cs.compute_etag(disk)

        log.info("[HIT-DISK] id=%d  size=%d  etag=%s", message_id, file_size, etag)
        return await _serve_from_disk(request, disk, name, dl, etag, file_size)

    # ── 2. Increment hit counter ───────────────────────────────────────────────
    hits = await rs.redis_svc.increment_hits(message_id)
    should_persist = hits >= MIN_HITS_TO_CACHE

    log.info("[MISS] id=%d  hits=%d  persist=%s", message_id, hits, should_persist)

    # ── 3. Distributed lock — prevent duplicate Telegram downloads ─────────────
    persist_this_request = False

    if should_persist:
        lock_acquired = await rs.redis_svc.acquire_lock(message_id)

        if lock_acquired:
            # This request wins the download + write responsibility
            persist_this_request = True
        else:
            # Another request is already downloading — wait for it to finish
            log.info("[WAIT-LOCK] id=%d — waiting for peer download", message_id)
            waited = await _wait_for_disk(message_id, disk)

            if waited:
                # Peer finished successfully — serve from disk
                file_size = disk.stat().st_size
                etag      = cs.compute_etag(disk)
                log.info("[HIT-AFTER-LOCK] id=%d", message_id)
                return await _serve_from_disk(
                    request, disk, name, dl, etag, file_size, status="HIT-LOCK"
                )

            # Peer timed out or failed — stream from Telegram without caching
            log.warning("[LOCK-TIMEOUT] id=%d — streaming passthrough", message_id)
            return _telegram_passthrough(
                message_id, name, dl,
                # Don't pass size — we can't guarantee correctness on partial downloads
            )

    # ── 4. Live Telegram stream (with optional disk write) ─────────────────────
    return _telegram_stream(
        message_id=message_id,
        name=name,
        dl=dl,
        persist=persist_this_request,
        disk=disk,
        size=size,
    )


# ── GET /api/health ────────────────────────────────────────────────────────────

@router.get("/api/health")
async def health() -> dict:
    return {
        "status":    "ok",
        "redis":     rs.redis_svc.available,
        "telegram":  ts.telegram_svc.is_connected(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

async def _wait_for_disk(msg_id: int, disk_p: "Path", timeout: int = 60) -> bool:
    """
    Poll until disk file appears and is marked ready, or timeout expires.
    Returns True if the file became available.
    """
    for _ in range(timeout):
        await asyncio.sleep(1)
        if disk_p.exists() and await rs.redis_svc.is_disk_ready(msg_id):
            return True
    return False


async def _serve_from_disk(
    request: Request,
    path: "Path",
    name: str,
    dl: str,
    etag: str,
    file_size: int,
    status: str = "HIT-DISK",
) -> Response:
    """
    Serve a fully-written cached file.
    Supports: ETag / 304 · Range / 206 · full 200.
    """
    from app.core.config import CHUNK_SIZE

    # ── 304 Not Modified ───────────────────────────────────────────────────────
    client_etag = request.headers.get("If-None-Match", "").strip('"')
    if client_etag == etag:
        return Response(status_code=304)

    # Build shared headers (no Content-Length in streaming responses — see note)
    cd      = _content_disposition(name, inline=dl != "1")
    headers = {
        "Content-Disposition":    cd,
        "ETag":                   f'"{etag}"',
        "Cache-Control":          "private, max-age=3600",
        "Accept-Ranges":          "bytes",
        "X-Cache-Status":         status,
        "X-Content-Type-Options": "nosniff",
    }

    range_header = request.headers.get("Range")

    # ── 206 Partial Content (Range request) ────────────────────────────────────
    if range_header:
        try:
            start, end = _parse_range(range_header, file_size)
        except ValueError as exc:
            return Response(
                status_code=416,
                headers={"Content-Range": f"bytes */{file_size}"},
                content=str(exc),
            )
        length = end - start + 1
        headers["Content-Range"]  = f"bytes {start}-{end}/{file_size}"
        headers["Content-Length"] = str(length)
        log.info("[206] id from path  bytes %d-%d/%d", start, end, file_size)
        return StreamingResponse(
            cs.stream_disk(path, start, end),
            status_code=206,
            media_type="application/pdf",
            headers=headers,
        )

    # ── 200 Full file ──────────────────────────────────────────────────────────
    # Use FastAPI FileResponse for efficient kernel-level sendfile().
    # NOTE: FileResponse sets Content-Length automatically from the file stat.
    log.info("[200-FILE] %s  %d bytes", path.name, file_size)
    headers["Content-Type"] = "application/pdf"
    return FileResponse(
        path=path,
        media_type="application/pdf",
        filename=name if dl == "1" else None,
        headers={k: v for k, v in headers.items() if k != "Content-Disposition"},
        content_disposition_type="attachment" if dl == "1" else "inline",
    )


def _telegram_passthrough(msg_id: int, name: str, dl: str) -> StreamingResponse:
    """
    Stream from Telegram without writing to disk.
    Used when the lock is held by a peer that timed out.
    Content-Length is intentionally omitted — we don't know the actual
    bytes that will arrive and must not lie to the browser.
    """
    async def _gen():
        try:
            async for chunk in ts.telegram_svc.iter_chunks(msg_id):
                yield chunk
        except Exception as exc:
            log.error("[PASSTHROUGH-ERROR] id=%d: %s", msg_id, exc)
            # Generator ends; client gets an incomplete file and browser
            # will show a download error — better than a hung connection.

    return StreamingResponse(
        _gen(),
        status_code=200,
        media_type="application/pdf",
        headers={
            "Content-Disposition":    _content_disposition(name, inline=dl != "1"),
            "Cache-Control":          "no-store",
            "X-Cache-Status":         "PASSTHROUGH",
            "X-Content-Type-Options": "nosniff",
            "Accept-Ranges":          "none",
        },
    )


def _telegram_stream(
    message_id: int,
    name: str,
    dl: str,
    persist: bool,
    disk: "Path",
    size: Optional[int],
) -> StreamingResponse:
    """
    Stream from Telegram, optionally writing to disk atomically.

    IMPORTANT: Content-Length header is NOT set on streaming responses.
    Setting it incorrectly causes RuntimeError in uvicorn/starlette when
    the actual bytes differ from the declared length (e.g. on error).
    The browser handles chunked transfer encoding transparently.
    """
    async def _gen():
        writer = cs.AtomicWriter(message_id) if persist else None
        try:
            if writer:
                async with writer:
                    async for chunk in ts.telegram_svc.iter_chunks(message_id):
                        await writer.write(chunk)
                        yield chunk

                # After successful atomic rename, mark as ready
                if writer.success:
                    await rs.redis_svc.mark_disk_ready(message_id)
                    asyncio.create_task(cs.lru_evict())
                else:
                    await rs.redis_svc.release_lock(message_id)
            else:
                async for chunk in ts.telegram_svc.iter_chunks(message_id):
                    yield chunk

        except Exception as exc:
            log.error("[STREAM-ERROR] id=%d: %s", message_id, exc, exc_info=True)
            # Release lock so future requests can retry
            if persist:
                await rs.redis_svc.release_lock(message_id)
            # Do not yield anything further — the generator ends cleanly.
            # The client will see a truncated response; this is unavoidable
            # once streaming has started. Raising here would cause an
            # unhandled exception in the ASGI layer.

        finally:
            # Always release the lock, even on unexpected exits
            if persist:
                await rs.redis_svc.release_lock(message_id)

    return StreamingResponse(
        _gen(),
        status_code=200,
        media_type="application/pdf",
        headers={
            "Content-Disposition":    _content_disposition(name, inline=dl != "1"),
            "Cache-Control":          "no-store",
            "X-Cache-Status":         "MISS",
            "X-Content-Type-Options": "nosniff",
            "Accept-Ranges":          "none",
        },
        # ⚠️  Content-Length deliberately omitted here.
        # See docstring above.
    )


# ── HTTP helpers ───────────────────────────────────────────────────────────────

def _content_disposition(name: str, inline: bool) -> str:
    """Build a RFC 5987-compliant Content-Disposition value."""
    disposition = "inline" if inline else "attachment"
    ascii_name  = name.encode("ascii", "ignore").decode() or "document.pdf"
    utf8_name   = quote(name, safe="")
    return f'{disposition}; filename="{ascii_name}"; filename*=UTF-8\'\'{utf8_name}'


def _parse_range(header: str, file_size: int) -> tuple[int, int]:
    """
    Parse 'Range: bytes=start-end' header.
    Returns (start, end) both inclusive.
    Raises ValueError on malformed or out-of-bounds ranges.
    """
    if not header.startswith("bytes="):
        raise ValueError("Only byte ranges supported")
    raw_start, _, raw_end = header[6:].partition("-")
    start = int(raw_start) if raw_start else 0
    end   = int(raw_end)   if raw_end   else file_size - 1

    if not (0 <= start <= end < file_size):
        raise ValueError(
            f"Range {start}-{end} is invalid for file size {file_size}"
        )
    return start, end
