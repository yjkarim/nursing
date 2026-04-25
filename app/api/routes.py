"""
API routes — thin handlers, all business logic in services.

# CHANGED: Every endpoint now accepts `group: str` as a required query parameter.
# The group is validated once at the top of each handler via get_group_id()
# (which raises HTTP 400 for unknown slugs before any I/O is performed).
# All downstream calls pass `group` explicitly so Redis keys, disk paths,
# and Telegram entities are always group-scoped.
"""
from __future__ import annotations
import asyncio
import logging
from typing import Optional
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse, Response, StreamingResponse

from app.services import cache_service as cs
from app.services import redis_service as rs
from app.services import telegram_service as ts
from app.core.config import MIN_HITS_TO_CACHE, CHUNK_SIZE, get_group_id, GROUPS

log = logging.getLogger("tg.routes")
router = APIRouter()


# ── GET /api/files ─────────────────────────────────────────────────────────────

@router.get("/api/files")
async def list_files(
    group: str = Query(..., description="Group slug, e.g. grade1"),
) -> list[dict]:
    # CHANGED: validate group, then fetch group-specific metadata.
    get_group_id(group)  # raises HTTP 400 if unknown
    data = await rs.redis_svc.get_meta(group)
    if data is None:
        data = cs.load_meta_disk(group)
    return data or []


# ── GET /api/groups ────────────────────────────────────────────────────────────
# NEW: Expose available group slugs so the frontend can populate a switcher.

@router.get("/api/groups")
async def list_groups() -> list[str]:
    """Return the list of valid group slugs."""
    return sorted(GROUPS.keys())


# ── POST /api/refresh ──────────────────────────────────────────────────────────

@router.post("/api/refresh")
async def refresh(
    group: str = Query(..., description="Group slug, e.g. grade1"),
    full: bool = Query(False),
) -> dict:
    """
    Sync PDF metadata from Telegram for a specific group.

    # CHANGED: All Redis calls and disk operations are group-scoped.
    # The logic is identical to the original single-tenant version —
    # only the variable `group` is threaded through every call.
    """
    get_group_id(group)  # validate early

    try:
        # ── Step 1: Load what we already have ─────────────────────────────────
        existing: list[dict] = await rs.redis_svc.get_meta(group) or cs.load_meta_disk(group)
        existing_count = len(existing)

        # ── Step 2: Decide scan mode ───────────────────────────────────────────
        stored_min_id = await rs.redis_svc.get_last_msg_id(group)

        need_full_scan = (
            full
            or existing_count == 0
            or (stored_min_id > 0 and existing_count == 0)
        )

        if need_full_scan:
            scan_min_id = 0
            log.info(
                "[group=%s] Full scan — reason: full=%s existing=%d stored_min_id=%d",
                group, full, existing_count, stored_min_id,
            )
        else:
            scan_min_id = stored_min_id
            log.info(
                "[group=%s] Incremental scan — min_id=%d existing=%d",
                group, scan_min_id, existing_count,
            )

        # ── Step 3: Fetch from Telegram ────────────────────────────────────────
        new_pdfs, max_id = await ts.telegram_svc.fetch_pdfs(group=group, min_id=scan_min_id)

        # ── Step 4: Merge without duplicates ──────────────────────────────────
        if need_full_scan:
            if not new_pdfs:
                log.warning(
                    "[group=%s] Full scan returned 0 PDFs — keeping existing %d entries",
                    group, existing_count,
                )
                return {"ok": True, "new": 0, "total": existing_count, "files": existing}
            merged_map: dict[int, dict] = {f["id"]: f for f in new_pdfs}
        else:
            merged_map = {f["id"]: f for f in existing}
            for f in new_pdfs:
                merged_map[f["id"]] = f

        merged: list[dict] = list(merged_map.values())
        merged.sort(key=lambda f: f["id"], reverse=True)

        # ── Step 5: Safety gate ────────────────────────────────────────────────
        if not merged and existing_count > 0:
            log.error(
                "[group=%s] BUG: merge produced empty list but had %d existing entries — "
                "refusing to overwrite.",
                group, existing_count,
            )
            return {"ok": True, "new": 0, "total": existing_count, "files": existing}

        # ── Step 6: Persist ────────────────────────────────────────────────────
        if need_full_scan:
            await rs.redis_svc.replace_meta(group, merged)
        else:
            await rs.redis_svc.set_meta(group, merged)

        cs.save_meta_disk(group, merged)

        if max_id > stored_min_id:
            await rs.redis_svc.set_last_msg_id(group, max_id)

        # ── Step 7: Log result ─────────────────────────────────────────────────
        if not new_pdfs:
            log.info(
                "[group=%s] No new PDFs found — keeping existing %d entries unchanged",
                group, existing_count,
            )
        else:
            log.info(
                "[group=%s] Merged %d new PDFs with %d existing → total %d  (min_id %d→%d)",
                group, len(new_pdfs), existing_count, len(merged), scan_min_id, max_id,
            )

        return {"ok": True, "new": len(new_pdfs), "total": len(merged), "files": merged}

    except HTTPException:
        raise
    except Exception as exc:
        log.error("[group=%s] Refresh failed: %s", group, exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))


# ── GET /api/stream/{message_id} ───────────────────────────────────────────────

@router.get("/api/stream/{message_id}")
async def stream_pdf(
    request: Request,
    message_id: int,
    group: str = Query(..., description="Group slug, e.g. grade1"),
    dl: str = Query("0"),
) -> Response:
    """
    Serve a PDF from a specific group.

    # CHANGED: disk path, Redis flags/locks, and Telegram entity are all
    # group-scoped. A message_id=123 in grade1 is completely independent
    # from message_id=123 in grade2 — they live in different directories
    # and have different Redis keys.

    Priority:
      1. Disk cache (file exists + marked ready) → FileResponse / 206 Range
      2. Telegram live stream + optional atomic disk write
    """
    get_group_id(group)  # validate early; raises HTTP 400 if unknown

    meta  = await rs.redis_svc.get_meta(group) or cs.load_meta_disk(group)
    entry = next((f for f in meta if f["id"] == message_id), None)
    name  = entry["name"] if entry else f"document_{message_id}.pdf"
    size: Optional[int] = entry.get("size") if entry else None
    disk  = cs.pdf_path(group, message_id)

    # ── 1. Disk cache HIT ──────────────────────────────────────────────────────
    if disk.exists() and await rs.redis_svc.is_disk_ready(group, message_id):
        file_size = disk.stat().st_size
        etag      = cs.compute_etag(disk)
        log.info("[group=%s] [HIT-DISK] id=%d  size=%d", group, message_id, file_size)
        return await _serve_disk(request, disk, name, dl, etag, file_size)

    # ── 2. Increment hit counter ───────────────────────────────────────────────
    hits           = await rs.redis_svc.increment_hits(group, message_id)
    should_persist = hits >= MIN_HITS_TO_CACHE
    log.info("[group=%s] [MISS] id=%d  hits=%d  persist=%s", group, message_id, hits, should_persist)

    # ── 3. Distributed lock ────────────────────────────────────────────────────
    persist_this = False
    if should_persist:
        if await rs.redis_svc.acquire_lock(group, message_id):
            persist_this = True
        else:
            log.info("[group=%s] [WAIT-LOCK] id=%d", group, message_id)
            ready = await _wait_disk_ready(group, message_id, disk, timeout=60)
            if ready:
                file_size = disk.stat().st_size
                etag      = cs.compute_etag(disk)
                log.info("[group=%s] [HIT-AFTER-LOCK] id=%d", group, message_id)
                return await _serve_disk(request, disk, name, dl, etag, file_size, "HIT-LOCK")
            log.warning("[group=%s] [LOCK-TIMEOUT] id=%d — passthrough", group, message_id)
            return _passthrough_stream(group, message_id, name, dl)

    # ── 4. Telegram stream (with optional disk write) ──────────────────────────
    return _telegram_stream(group, message_id, name, dl, persist_this, disk)


# ── GET /api/health ────────────────────────────────────────────────────────────

@router.get("/api/health")
async def health() -> dict:
    return {
        "status":   "ok",
        "redis":    rs.redis_svc.available,
        "telegram": ts.telegram_svc.is_connected(),
        "groups":   sorted(GROUPS.keys()),   # NEW: surface available groups
    }


# ─── Internal helpers ──────────────────────────────────────────────────────────

async def _wait_disk_ready(
    group: str, msg_id: int, disk: "Path", timeout: int
) -> bool:
    for _ in range(timeout):
        await asyncio.sleep(1)
        if disk.exists() and await rs.redis_svc.is_disk_ready(group, msg_id):
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

    log.info("[200-FILE] %s  %d bytes", path.name, file_size)
    return FileResponse(
        path=path,
        media_type="application/pdf",
        headers={k: v for k, v in headers.items() if k != "Content-Disposition"},
        content_disposition_type="attachment" if dl == "1" else "inline",
        filename=name,
    )


def _passthrough_stream(
    group: str, msg_id: int, name: str, dl: str
) -> StreamingResponse:
    """Stream from Telegram without disk write — used on lock timeout."""
    async def _gen():
        try:
            async for chunk in ts.telegram_svc.iter_chunks(group, msg_id):
                yield chunk
        except Exception as exc:
            log.error("[group=%s] [PASSTHROUGH-ERROR] id=%d: %s", group, msg_id, exc)

    return StreamingResponse(
        _gen(),
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
    group: str,
    message_id: int,
    name: str,
    dl: str,
    persist: bool,
    disk: "Path",
) -> StreamingResponse:
    """
    Stream from Telegram. If persist=True, write atomically to disk.
    Content-Length intentionally omitted to prevent RuntimeError when actual
    bytes differ from declared length.
    """
    async def _gen():
        # CHANGED: AtomicWriter receives group so the file lands in the
        # correct subdirectory (/cache/{group}/{msg_id}.pdf).
        writer = cs.AtomicWriter(group, message_id) if persist else None
        try:
            if writer:
                async with writer:
                    async for chunk in ts.telegram_svc.iter_chunks(group, message_id):
                        await writer.write(chunk)
                        yield chunk
                if writer.success:
                    await rs.redis_svc.mark_disk_ready(group, message_id)
                    # CHANGED: lru_evict now takes group to stay within group's dir.
                    asyncio.create_task(cs.lru_evict(group))
                else:
                    await rs.redis_svc.release_lock(group, message_id)
            else:
                async for chunk in ts.telegram_svc.iter_chunks(group, message_id):
                    yield chunk
        except Exception as exc:
            log.error(
                "[group=%s] [STREAM-ERROR] id=%d: %s", group, message_id, exc, exc_info=True
            )
        finally:
            if persist:
                await rs.redis_svc.release_lock(group, message_id)

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
