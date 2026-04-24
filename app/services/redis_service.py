"""
Redis service — async wrapper around redis.asyncio.

Design decisions:
  • Every public method is safe to call when Redis is unavailable.
    It falls back gracefully so the app remains functional (degraded mode).
  • No business logic here — just atomic Redis primitives.
  • Single shared connection pool, created once at app startup.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Optional

import redis.asyncio as aioredis
from redis.asyncio import Redis

from app.core.config import (
    HITS_TTL, LAST_MSG_KEY, LOCK_TTL,
    META_TTL, REDIS_URL,
)

log = logging.getLogger("tg.redis")

# ── Redis key schema ───────────────────────────────────────────────────────────
_K_META      = "tg:meta"            # JSON list of PDF metadata
_K_HITS      = "tg:hits:{id}"      # per-file hit counter (int)
_K_LOCK      = "tg:lock:{id}"      # distributed download lock
_K_DISK_OK   = "tg:disk_ok:{id}"   # flag: file is fully written to disk


class RedisService:
    """
    Thin async wrapper. All methods catch exceptions internally and log a
    warning instead of raising — the app degrades gracefully without Redis.
    """

    def __init__(self) -> None:
        self._r: Optional[Redis] = None

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def connect(self) -> None:
        try:
            self._r = aioredis.from_url(
                REDIS_URL,
                decode_responses=True,
                socket_connect_timeout=3,
                socket_timeout=3,
                retry_on_timeout=True,
            )
            await self._r.ping()
            log.info("Redis connected ✓  url=%s", REDIS_URL)
        except Exception as exc:
            log.warning("Redis unavailable (%s) — running in degraded mode", exc)
            self._r = None

    async def close(self) -> None:
        if self._r:
            try:
                await self._r.aclose()
            except Exception:
                pass

    @property
    def available(self) -> bool:
        return self._r is not None

    # ── Internal helpers ───────────────────────────────────────────────────────

    async def _get(self, key: str) -> Optional[str]:
        try:
            if self._r:
                return await self._r.get(key)
        except Exception as exc:
            log.warning("Redis GET %s failed: %s", key, exc)
        return None

    async def _set(self, key: str, value: str, ex: int) -> None:
        try:
            if self._r:
                await self._r.set(key, value, ex=ex)
        except Exception as exc:
            log.warning("Redis SET %s failed: %s", key, exc)

    async def _delete(self, *keys: str) -> None:
        try:
            if self._r and keys:
                await self._r.delete(*keys)
        except Exception as exc:
            log.warning("Redis DEL failed: %s", exc)

    # ── Metadata ───────────────────────────────────────────────────────────────

    async def get_meta(self) -> Optional[list[dict]]:
        raw = await self._get(_K_META)
        if raw:
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                pass
        return None

    async def set_meta(self, data: list[dict]) -> None:
        await self._set(_K_META, json.dumps(data, ensure_ascii=False), ex=META_TTL)

    async def invalidate_meta(self) -> None:
        await self._delete(_K_META)

    # ── Last scanned message id (incremental Telegram scan) ───────────────────

    async def get_last_msg_id(self) -> int:
        val = await self._get(LAST_MSG_KEY)
        return int(val) if val else 0

    async def set_last_msg_id(self, msg_id: int) -> None:
        # No TTL — we want this to persist across restarts
        try:
            if self._r:
                await self._r.set(LAST_MSG_KEY, str(msg_id))
        except Exception as exc:
            log.warning("Redis set_last_msg_id failed: %s", exc)

    # ── Hit counter ────────────────────────────────────────────────────────────

    async def increment_hits(self, msg_id: int) -> int:
        """Atomically increment and return the new hit count. Returns 1 on failure."""
        key = _K_HITS.format(id=msg_id)
        try:
            if self._r:
                count = await self._r.incr(key)
                if count == 1:
                    await self._r.expire(key, HITS_TTL)
                return count
        except Exception as exc:
            log.warning("Redis INCR %s failed: %s", key, exc)
        return 1

    # ── Distributed download lock ──────────────────────────────────────────────

    async def acquire_lock(self, msg_id: int) -> bool:
        """
        Try to acquire an exclusive download lock for msg_id.
        Returns True if the lock was acquired (this request should download).
        Returns False if another request already holds the lock.
        Uses SET NX EX — atomic, no WATCH/MULTI needed.
        """
        key = _K_LOCK.format(id=msg_id)
        try:
            if self._r:
                result = await self._r.set(key, "1", nx=True, ex=LOCK_TTL)
                return result is True
        except Exception as exc:
            log.warning("Redis lock acquire %s failed: %s", key, exc)
        # Fallback: grant the lock (no Redis = no distributed coordination)
        return True

    async def release_lock(self, msg_id: int) -> None:
        await self._delete(_K_LOCK.format(id=msg_id))

    async def is_locked(self, msg_id: int) -> bool:
        val = await self._get(_K_LOCK.format(id=msg_id))
        return val is not None

    # ── Disk-ready flag ────────────────────────────────────────────────────────
    # Set AFTER atomic rename so readers know the file is complete.

    async def mark_disk_ready(self, msg_id: int) -> None:
        await self._set(_K_DISK_OK.format(id=msg_id), "1", ex=HITS_TTL)

    async def is_disk_ready(self, msg_id: int) -> bool:
        val = await self._get(_K_DISK_OK.format(id=msg_id))
        return val is not None

    async def unmark_disk_ready(self, msg_id: int) -> None:
        await self._delete(_K_DISK_OK.format(id=msg_id))


# ── Singleton ──────────────────────────────────────────────────────────────────
redis_svc = RedisService()
