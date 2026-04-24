"""
Disk cache service.

Responsibilities:
  • Atomic file writes (write to .tmp → os.replace → mark ready in Redis)
  • Safe file reads (only serve files marked as ready)
  • Metadata persistence on disk (Redis cold-start fallback)
  • LRU eviction when disk usage exceeds limit
  • ETag computation (size + mtime fingerprint)

Thread/concurrency safety:
  • os.replace() is atomic on POSIX and Windows (same filesystem).
  • Files are only marked "ready" in Redis AFTER rename.
  • Readers check the Redis ready-flag before serving from disk.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
from pathlib import Path
from typing import AsyncIterator, Optional

import aiofiles
import aiofiles.os

from app.core.config import (
    CACHE_DIR,
    CHUNK_SIZE,
    DISK_MAX_BYTES,
)

log = logging.getLogger("tg.cache")


# ── Path helpers ───────────────────────────────────────────────────────────────

def pdf_path(msg_id: int) -> Path:
    return CACHE_DIR / f"{msg_id}.pdf"

def tmp_path(msg_id: int) -> Path:
    return CACHE_DIR / f"{msg_id}.pdf.tmp"

def meta_path() -> Path:
    return CACHE_DIR / "metadata.json"


# ── ETag ───────────────────────────────────────────────────────────────────────

def compute_etag(path: Path) -> str:
    """Cheap ETag based on file size + mtime. No full file hash needed."""
    st = path.stat()
    raw = f"{st.st_size}-{st.st_mtime_ns}"
    return hashlib.md5(raw.encode()).hexdigest()


# ── Metadata (disk fallback) ───────────────────────────────────────────────────

def load_meta_disk() -> list[dict]:
    """Read metadata.json from disk. Returns [] on any error."""
    p = meta_path()
    try:
        if p.exists():
            return json.loads(p.read_text("utf-8"))
    except Exception as exc:
        log.warning("metadata.json read error: %s", exc)
    return []


def save_meta_disk(data: list[dict]) -> None:
    """Atomically write metadata.json to disk."""
    p   = meta_path()
    tmp = p.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), "utf-8")
        os.replace(tmp, p)
    except Exception as exc:
        log.warning("metadata.json write error: %s", exc)
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass


# ── Atomic writer ──────────────────────────────────────────────────────────────

class AtomicWriter:
    """
    Async context manager for safe concurrent disk writes.

    Usage:
        async with AtomicWriter(msg_id) as writer:
            async for chunk in source:
                await writer.write(chunk)
        # File is now on disk and os.replace() has been called.

    On any exception inside the block, the .tmp file is cleaned up and
    the final .pdf is NOT created — preventing partial reads.
    """

    def __init__(self, msg_id: int) -> None:
        self._final = pdf_path(msg_id)
        self._tmp   = tmp_path(msg_id)
        self._fh    = None
        self.success = False

    async def __aenter__(self) -> "AtomicWriter":
        self._fh = await aiofiles.open(self._tmp, "wb")
        return self

    async def write(self, data: bytes) -> None:
        assert self._fh is not None, "AtomicWriter used outside context manager"
        await self._fh.write(data)

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        # Always close the file handle first
        try:
            await self._fh.close()
        except Exception as exc:
            log.warning("AtomicWriter close error: %s", exc)

        if exc_type is None:
            # Happy path: rename tmp → final (atomic on POSIX + Windows)
            try:
                os.replace(self._tmp, self._final)
                self.success = True
                log.info(
                    "Cached to disk: %s (%s bytes)",
                    self._final.name,
                    self._final.stat().st_size,
                )
            except Exception as exc:
                log.error("Atomic rename failed: %s", exc)
                self.success = False
        else:
            # Error path: remove the incomplete temp file
            log.warning(
                "AtomicWriter aborting due to %s: %s — removing temp file",
                exc_type.__name__, exc_val,
            )
            try:
                self._tmp.unlink(missing_ok=True)
            except Exception:
                pass

        # Do not suppress exceptions
        return False


# ── Disk streaming ─────────────────────────────────────────────────────────────

async def stream_disk(
    path: Path,
    start: int = 0,
    end: Optional[int] = None,
) -> AsyncIterator[bytes]:
    """
    Async generator: read a file from disk supporting byte ranges.

    Args:
        path:  absolute path to the cached PDF
        start: byte offset to start reading from (0-indexed, inclusive)
        end:   last byte to read (inclusive). None = read to EOF.
    """
    async with aiofiles.open(path, "rb") as f:
        if start > 0:
            await f.seek(start)

        remaining: Optional[int] = (end - start + 1) if end is not None else None

        while True:
            to_read = CHUNK_SIZE
            if remaining is not None:
                to_read = min(to_read, remaining)
                if to_read <= 0:
                    break
            chunk = await f.read(to_read)
            if not chunk:
                break
            if remaining is not None:
                remaining -= len(chunk)
            yield chunk


# ── LRU eviction ──────────────────────────────────────────────────────────────

async def lru_evict() -> None:
    """
    Delete the oldest cached PDFs until total disk usage < DISK_MAX_BYTES.
    Called as a background asyncio.Task after each new file is written.
    """
    try:
        files = sorted(
            CACHE_DIR.glob("*.pdf"),
            key=lambda p: p.stat().st_mtime,
        )
        total = sum(p.stat().st_size for p in files)

        while total > DISK_MAX_BYTES and files:
            oldest = files.pop(0)
            freed  = oldest.stat().st_size
            oldest.unlink(missing_ok=True)
            total -= freed
            log.info(
                "LRU evicted: %s  freed %.1f MB",
                oldest.name, freed / 1_048_576,
            )
    except Exception as exc:
        log.warning("LRU eviction error: %s", exc)
