"""
API routes — thin handlers, all business logic in services.
"""
from __future__ import annotations
import asyncio
import logging
from typing import Optional
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse, Response, StreamingResponse

import cache_service as cs
import redis_service as rs
import telegram_service as ts
from config import MIN_HITS_TO_CACHE, CHUNK_SIZE

log = logging.getLogger("tg.routes")
router = APIRouter()


# ── GET /api/files ─────────────────────────────────────────────────────────────

@router.get("/api/files")
async def list_files() -> list[dict]:
    data = await rs.redis_svc.get_meta()
    if data is None:
        data = cs.load_meta_disk()
    return data or []


# ── POST /api/refresh ──────────────────────────────────────────────────────────

@router.post("/api/refresh")
async def refresh(full: bool = Query(False)) -> dict:
    """
    Sync PDF metadata from Telegram.

    Logic:
      1. Load existing data from Redis → disk fallback
      2. If no existing data OR full=true → full scan (min_id=0)
         Otherwise                        → incremental scan (min_id=last_seen)
      3. Merge: existing + new (deduplicated by message id)
      4. ONLY write back if merged result is non-empty
      5. NEVER overwrite good data with an empty result

    This guarantees data never disappears after a refresh.
    """
    try:
        # ── Step 1: Load what we already have ─────────────────────────────────
        # Always prefer Redis; fall back to disk (survives Redis restart).
        existing: list[dict] = await rs.redis_svc.get_meta() or cs.load_meta_disk()
        existing_count = len(existing)

        # ── Step 2: Decide scan mode ───────────────────────────────────────────
        # Force a full scan when:
        #   a) caller passed ?full=true
        #   b) storage is empty — could be first run or data loss recovery
        #   c) min_id is stored but we have no file list (inconsistent state)
        stored_min_id = await rs.redis_svc.get_last_msg_id()

        need_full_scan = (
            full
            or existing_count == 0          # nothing stored → must do full scan
            or (stored_min_id > 0 and existing_count == 0)  # min_id exists but no files
        )

        if need_full_scan:
            scan_min_id = 0
            log.info(
                "Full scan triggered — reason: full=%s existing=%d stored_min_id=%d",
                full, existing_count, stored_min_id,
            )
        else:
            scan_min_id = stored_min_id
            log.info(
                "Incremental scan — min_id=%d existing=%d",
                scan_min_id, existing_count,
            )

        # ── Step 3: Fetch from Telegram ────────────────────────────────────────
        new_pdfs, max_id = await ts.telegram_svc.fetch_pdfs(min_id=scan_min_id)

        # ── Step 4: Merge without duplicates ──────────────────────────────────
        # Use a dict keyed by message_id — last writer wins (handles edits).
        # Start with existing, then overlay new entries so new data takes precedence.
        if need_full_scan:
            # Full scan replaces everything — but only if Telegram returned data
            if not new_pdfs:
                # Telegram returned nothing on a full scan.
                # This is suspicious (empty channel?) — keep existing data safe.
                log.warning(
                    "Full scan returned 0 PDFs — keeping existing %d entries unchanged",
                    existing_count,
                )
                return {
                    "ok":    True,
                    "new":   0,
                    "total": existing_count,
                    "files": existing,
                }
            merged_map: dict[int, dict] = {f["id"]: f for f in new_pdfs}
        else:
            # Incremental merge: start from existing, add/update with new
            merged_map = {f["id"]: f for f in existing}   # seed with old data
            for f in new_pdfs:
                merged_map[f["id"]] = f                   # overlay new entries

        merged: list[dict] = list(merged_map.values())

        # Sort by id descending (newest first) — consistent ordering for frontend
        merged.sort(key=lambda f: f["id"], reverse=True)

        # ── Step 5: Safety gate — never write empty result ─────────────────────
        # If the merge somehow produced nothing but we had data before, abort.
        if not merged and existing_count > 0:
            log.error(
                "BUG: merge produced empty list but had %d existing entries — "
                "refusing to overwrite. Please report this.",
                existing_count,
            )
            return {
                "ok":    True,
                "new":   0,
                "total": existing_count,
                "files": existing,
            }

        # ── Step 6: Persist ────────────────────────────────────────────────────
        # Write to Redis (primary) AND disk (fallback for Redis restarts).
        if need_full_scan:
            # Full scan: delete stale Redis hash entries and rewrite from scratch
            await rs.redis_svc.replace_meta(merged)
        else:
            # Incremental: HSET-merge into existing hash (never deletes old entries)
            await rs.redis_svc.set_meta(merged)

        cs.save_meta_disk(merged)             # permanent until next write

        # Advance the min_id cursor only after a successful persist
        if max_id > stored_min_id:
            await rs.redis_svc.set_last_msg_id(max_id)

        # ── Step 7: Log result ─────────────────────────────────────────────────
        added = len(merged) - existing_count
        if not new_pdfs:
            log.info(
                "No new PDFs found — keeping existing %d entries unchanged",
                existing_count,
            )
        else:
            log.info(
                "Merged %d new PDFs with %d existing → total %d  (min_id %d→%d)",
                len(new_pdfs), existing_count, len(merged), scan_min_id, max_id,
            )

        return {
            "ok":    True,
            "new":   len(new_pdfs),
            "total": len(merged),
            "files": merged,
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
    Serve a PDF.

    Priority:
      1. Disk cache (file exists + marked ready) → FileResponse / 206 Range
      2. Telegram live stream + optional atomic disk write

    Concurrency safety:
      • Redis SET NX lock prevents duplicate Telegram downloads.
      • Concurrent requests wait up to 60 s then get a passthrough stream.
      • Files are only served from disk AFTER atomic rename + Redis ready flag.
    """
    meta  = await rs.redis_svc.get_meta() or cs.load_meta_disk()
    entry = next((f for f in meta if f["id"] == message_id), None)
    name  = entry["name"] if entry else f"document_{message_id}.pdf"
    size: Optional[int] = entry.get("size") if entry else None
    disk  = cs.pdf_path(message_id)

    # ── 1. Disk cache HIT ──────────────────────────────────────────────────────
    if disk.exists() and await rs.redis_svc.is_disk_ready(message_id):
        file_size = disk.stat().st_size
        etag      = cs.compute_etag(disk)
        log.info("[HIT-DISK] id=%d  size=%d", message_id, file_size)
        return await _serve_disk(request, disk, name, dl, etag, file_size)

    # ── 2. Increment hit counter ───────────────────────────────────────────────
    hits           = await rs.redis_svc.increment_hits(message_id)
    should_persist = hits >= MIN_HITS_TO_CACHE
    log.info("[MISS] id=%d  hits=%d  persist=%s", message_id, hits, should_persist)

    # ── 3. Distributed lock ────────────────────────────────────────────────────
    persist_this = False
    if should_persist:
        if await rs.redis_svc.acquire_lock(message_id):
            persist_this = True
        else:
            # Wait for the peer download to finish
            log.info("[WAIT-LOCK] id=%d", message_id)
            ready = await _wait_disk_ready(message_id, disk, timeout=60)
            if ready:
                file_size = disk.stat().st_size
                etag      = cs.compute_etag(disk)
                log.info("[HIT-AFTER-LOCK] id=%d", message_id)
                return await _serve_disk(request, disk, name, dl, etag, file_size, "HIT-LOCK")
            log.warning("[LOCK-TIMEOUT] id=%d — passthrough", message_id)
            return _passthrough_stream(message_id, name, dl)

    # ── 4. Telegram stream (with optional disk write) ──────────────────────────
    return _telegram_stream(message_id, name, dl, persist_this, disk)


# ── GET /api/health ────────────────────────────────────────────────────────────

@router.get("/api/health")
async def health() -> dict:
    return {
        "status":   "ok",
        "redis":    rs.redis_svc.available,
        "telegram": ts.telegram_svc.is_connected(),
    }


# ─── Internal helpers ──────────────────────────────────────────────────────────

async def _wait_disk_ready(msg_id: int, disk: "Path", timeout: int) -> bool:
    for _ in range(timeout):
        await asyncio.sleep(1)
        if disk.exists() and await rs.redis_svc.is_disk_ready(msg_id):
            return True
    return False


async def _serve_disk(
    request: Request,
    path: "Path",
    name: str,
    dl: str,
    etag: str,
    file_size: int,
    status: str = "HIT-DISK",
) -> Response:
    """Serve a fully-written cached file. Supports ETag/304 and Range/206."""

    # 304 Not Modified
    if request.headers.get("If-None-Match", "").strip('"') == etag:
        return Response(status_code=304)

    cd      = _content_disposition(name, inline=dl != "1")
    headers = {
        "Content-Disposition":    cd,
        "ETag":                   f'"{etag}"',
        "Cache-Control":          "private, max-age=3600",
        "Accept-Ranges":          "bytes",
        "X-Cache-Status":         status,
        "X-Content-Type-Options": "nosniff",
    }

    range_hdr = request.headers.get("Range")

    # 206 Partial Content
    if range_hdr:
        try:
            start, end = _parse_range(range_hdr, file_size)
        except ValueError as exc:
            return Response(
                status_code=416,
                headers={"Content-Range": f"bytes */{file_size}"},
                content=str(exc),
            )
        length = end - start + 1
        headers["Content-Range"]  = f"bytes {start}-{end}/{file_size}"
        headers["Content-Length"] = str(length)
        log.info("[206] bytes %d-%d/%d  %s", start, end, file_size, path.name)
        return StreamingResponse(
            cs.stream_disk(path, start, end),
            status_code=206,
            media_type="application/pdf",
            headers=headers,
        )

    # 200 Full — FastAPI FileResponse uses kernel sendfile()
    log.info("[200-FILE] %s  %d bytes", path.name, file_size)
    return FileResponse(
        path=path,
        media_type="application/pdf",
        headers={k: v for k, v in headers.items() if k != "Content-Disposition"},
        content_disposition_type="attachment" if dl == "1" else "inline",
        filename=name,
    )


def _passthrough_stream(msg_id: int, name: str, dl: str) -> StreamingResponse:
    """Stream from Telegram without disk write — used on lock timeout."""
    async def _gen():
        try:
            async for chunk in ts.telegram_svc.iter_chunks(msg_id):
                yield chunk
        except Exception as exc:
            log.error("[PASSTHROUGH-ERROR] id=%d: %s", msg_id, exc)

    return StreamingResponse(
        _gen(),
        media_type="application/pdf",
        headers={
            # ⚠️ No Content-Length on streaming — prevents RuntimeError
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
) -> StreamingResponse:
    """
    Stream from Telegram. If persist=True, write atomically to disk.
    Content-Length is intentionally OMITTED to prevent RuntimeError
    when actual bytes differ from declared length.
    """
    async def _gen():
        writer = cs.AtomicWriter(message_id) if persist else None
        try:
            if writer:
                async with writer:
                    async for chunk in ts.telegram_svc.iter_chunks(message_id):
                        await writer.write(chunk)
                        yield chunk
                # After atomic rename succeeded → mark ready
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
        finally:
            if persist:
                await rs.redis_svc.release_lock(message_id)

    return StreamingResponse(
        _gen(),
        media_type="application/pdf",
        headers={
            "Content-Disposition":    _content_disposition(name, inline=dl != "1"),
            "Cache-Control":          "no-store",
            "X-Cache-Status":         "MISS",
            "X-Content-Type-Options": "nosniff",
            "Accept-Ranges":          "none",
        },
    )


def _content_disposition(name: str, inline: bool) -> str:
    disposition = "inline" if inline else "attachment"
    ascii_name  = name.encode("ascii", "ignore").decode() or "document.pdf"
    utf8_name   = quote(name, safe="")
    return f'{disposition}; filename="{ascii_name}"; filename*=UTF-8\'\'{utf8_name}'


def _parse_range(header: str, file_size: int) -> tuple[int, int]:
    if not header.startswith("bytes="):
        raise ValueError("Only byte ranges supported")
    raw_start, _, raw_end = header[6:].partition("-")
    start = int(raw_start) if raw_start else 0
    end   = int(raw_end)   if raw_end   else file_size - 1
    if not (0 <= start <= end < file_size):
        raise ValueError(f"Range {start}-{end} invalid for size {file_size}")
    return start, end
