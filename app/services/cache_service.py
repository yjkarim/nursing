"""
Disk cache service.

Responsibilities:
  • Atomic file writes (write to .tmp → os.replace → mark ready in Redis)
  • Safe file reads (only serve files marked as ready)
  • Metadata persistence on disk (Redis cold-start fallback)
  • LRU eviction when disk usage exceeds limit
  • ETag computation (size + mtime fingerprint)

# CHANGED: All path helpers and AtomicWriter now accept `group: str`.
# Each group gets its own subdirectory under CACHE_DIR:
#   /cache/{group}/{message_id}.pdf
# This guarantees that message IDs from different groups never collide on disk,
# even if Telegram assigns the same numeric ID in two different channels.

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


# ── Path helpers (per group) ───────────────────────────────────────────────────

def _group_dir(group: str) -> Path:
    """Return (and create) the cache subdirectory for a group."""
    d = CACHE_DIR / group
    d.mkdir(parents=True, exist_ok=True)
    return d

# CHANGED: pdf_path and tmp_path now include the group segment.
def pdf_path(group: str, msg_id: int) -> Path:
    return _group_dir(group) / f"{msg_id}.pdf"

def tmp_path(group: str, msg_id: int) -> Path:
    return _group_dir(group) / f"{msg_id}.pdf.tmp"

# CHANGED: metadata is also stored per group so disk fallback is isolated.
def meta_path(group: str) -> Path:
    return _group_dir(group) / "metadata.json"


# ── ETag ───────────────────────────────────────────────────────────────────────

def compute_etag(path: Path) -> str:
    """Cheap ETag based on file size + mtime. No full file hash needed."""
    st = path.stat()
    raw = f"{st.st_size}-{st.st_mtime_ns}"
    return hashlib.md5(raw.encode()).hexdigest()


# ── Metadata (disk fallback, per group) ───────────────────────────────────────

def load_meta_disk(group: str) -> list[dict]:
    """Read metadata.json for a group from disk. Returns [] on any error."""
    p = meta_path(group)
    try:
        if p.exists():
            return json.loads(p.read_text("utf-8"))
    except Exception as exc:
        log.warning("[group=%s] metadata.json read error: %s", group, exc)
    return []


def save_meta_disk(group: str, data: list[dict]) -> None:
    """Atomically write metadata.json for a group to disk."""
    p   = meta_path(group)
    tmp = p.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), "utf-8")
        os.replace(tmp, p)
    except Exception as exc:
        log.warning("[group=%s] metadata.json write error: %s", group, exc)
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass


# ── Atomic writer (per group) ─────────────────────────────────────────────────

class AtomicWriter:
    """
    Async context manager for safe concurrent disk writes.

    # CHANGED: Accepts group so files land in /cache/{group}/{msg_id}.pdf.
    # This prevents a message ID from group A from overwriting the same ID
    # from group B if Telegram reuses numeric IDs across channels.

    Usage:
        async with AtomicWriter(group, msg_id) as writer:
            async for chunk in source:
                await writer.write(chunk)
        # File is now on disk and os.replace() has been called.

    On any exception inside the block, the .tmp file is cleaned up and
    the final .pdf is NOT created — preventing partial reads.
    """

    def __init__(self, group: str, msg_id: int) -> None:
        self._final   = pdf_path(group, msg_id)
        self._tmp     = tmp_path(group, msg_id)
        self._fh      = None
        self.success  = False
        self._group   = group   # kept for log messages only

    async def __aenter__(self) -> "AtomicWriter":
        self._fh = await aiofiles.open(self._tmp, "wb")
        return self

    async def write(self, data: bytes) -> None:
        assert self._fh is not None, "AtomicWriter used outside context manager"
        await self._fh.write(data)

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        try:
            await self._fh.close()
        except Exception as exc:
            log.warning("[group=%s] AtomicWriter close error: %s", self._group, exc)

        if exc_type is None:
            try:
                os.replace(self._tmp, self._final)
                self.success = True
                log.info(
                    "[group=%s] Cached to disk: %s (%s bytes)",
                    self._group,
                    self._final.name,
                    self._final.stat().st_size,
                )
            except Exception as exc:
                log.error("[group=%s] Atomic rename failed: %s", self._group, exc)
                self.success = False
        else:
            log.warning(
                "[group=%s] AtomicWriter aborting due to %s: %s — removing temp file",
                self._group, exc_type.__name__, exc_val,
            )
            try:
                self._tmp.unlink(missing_ok=True)
            except Exception:
                pass

        return False  # do not suppress exceptions


# ── Disk streaming ─────────────────────────────────────────────────────────────

async def stream_disk(
    path: Path,
    start: int = 0,
    end: Optional[int] = None,
) -> AsyncIterator[bytes]:
    """
    Async generator: read a file from disk supporting byte ranges.
    (Unchanged — path already encodes the group via pdf_path().)
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


# ── LRU eviction (per group) ──────────────────────────────────────────────────

async def lru_evict(group: str) -> None:
    """
    Delete the oldest cached PDFs for a specific group until total disk usage
    for that group < DISK_MAX_BYTES.

    # CHANGED: Eviction is now scoped to one group's directory.
    # This prevents a burst of downloads for grade3 from evicting cached
    # files that belong to grade1 or grade2.
    Called as a background asyncio.Task after each new file is written.
    """
    try:
        group_dir = _group_dir(group)
        files = sorted(
            group_dir.glob("*.pdf"),
            key=lambda p: p.stat().st_mtime,
        )
        total = sum(p.stat().st_size for p in files)

        while total > DISK_MAX_BYTES and files:
            oldest = files.pop(0)
            freed  = oldest.stat().st_size
            oldest.unlink(missing_ok=True)
            total -= freed
            log.info(
                "[group=%s] LRU evicted: %s  freed %.1f MB",
                group, oldest.name, freed / 1_048_576,
            )
    except Exception as exc:
        log.warning("[group=%s] LRU eviction error: %s", group, exc)
