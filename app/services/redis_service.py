"""
Redis service — async wrapper with graceful degradation.
Every method works even when Redis is unavailable (falls back to safe defaults).

Storage design for metadata:
  tg:meta        → JSON list, TTL=META_TTL  (fast read cache)
  tg:meta:hash   → Redis Hash keyed by msg_id (persistent, no TTL)

  Having two stores solves two problems:
    1. tg:meta TTL expiry: hash survives and can rebuild the list
    2. Redis restart: disk backup rebuilds both
"""
from __future__ import annotations
import json
import logging
from typing import Optional

import redis.asyncio as aioredis
from redis.asyncio import Redis

from config import HITS_TTL, LAST_MSG_KEY, LOCK_TTL, META_TTL, REDIS_URL

log = logging.getLogger("tg.redis")

# ── Key schema ─────────────────────────────────────────────────────────────────
_K_META      = "tg:meta"          # JSON list cache (TTL)
_K_META_HASH = "tg:meta:hash"     # Hash: msg_id → JSON entry (no TTL, persistent)
_K_HITS      = "tg:hits:{id}"
_K_LOCK      = "tg:lock:{id}"
_K_DISK_OK   = "tg:disk_ok:{id}"


class RedisService:
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
            log.warning("Redis unavailable (%s) — degraded mode (no caching coordination)", exc)
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

    # ── Low-level helpers ──────────────────────────────────────────────────────

    async def _get(self, key: str) -> Optional[str]:
        try:
            if self._r:
                return await self._r.get(key)
        except Exception as exc:
            log.warning("Redis GET %s: %s", key, exc)
        return None

    async def _set(self, key: str, value: str, ex: int) -> None:
        try:
            if self._r:
                await self._r.set(key, value, ex=ex)
        except Exception as exc:
            log.warning("Redis SET %s: %s", key, exc)

    async def _delete(self, *keys: str) -> None:
        try:
            if self._r and keys:
                await self._r.delete(*keys)
        except Exception as exc:
            log.warning("Redis DEL: %s", exc)

    # ── Metadata — dual storage ────────────────────────────────────────────────

    async def get_meta(self) -> Optional[list[dict]]:
        """
        Read metadata.
        Priority:
          1. tg:meta JSON list (fast, TTL-based cache)
          2. tg:meta:hash (persistent, rebuilt if list expired)
        Returns None only when both are empty (truly first run).
        """
        # Layer 1: TTL list cache
        raw = await self._get(_K_META)
        if raw:
            try:
                data = json.loads(raw)
                if data:  # never return an empty list from cache
                    return data
            except json.JSONDecodeError:
                log.warning("tg:meta JSON decode error — falling back to hash")

        # Layer 2: Persistent hash (survives TTL expiry and Redis restarts)
        try:
            if self._r:
                hash_data = await self._r.hgetall(_K_META_HASH)
                if hash_data:
                    entries = []
                    for raw_entry in hash_data.values():
                        try:
                            entries.append(json.loads(raw_entry))
                        except json.JSONDecodeError:
                            continue
                    if entries:
                        entries.sort(key=lambda f: f["id"], reverse=True)
                        log.info(
                            "Rebuilt metadata from hash store: %d entries", len(entries)
                        )
                        # Refresh the TTL list cache from the hash
                        await self._set(
                            _K_META,
                            json.dumps(entries, ensure_ascii=False),
                            ex=META_TTL,
                        )
                        return entries
        except Exception as exc:
            log.warning("Redis HGETALL %s: %s", _K_META_HASH, exc)

        return None

    async def set_meta(self, data: list[dict]) -> None:
        """
        Write metadata to both stores atomically (pipeline).
        - tg:meta:hash  → persistent, no TTL, keyed by msg_id
        - tg:meta       → TTL list cache for fast reads
        Never writes empty data to protect against accidental overwrites.
        """
        if not data:
            log.warning("set_meta called with empty list — refusing to write")
            return

        try:
            if self._r:
                pipe = self._r.pipeline()

                # Build hash mapping: {str(msg_id): JSON entry}
                hash_mapping = {
                    str(entry["id"]): json.dumps(entry, ensure_ascii=False)
                    for entry in data
                }

                # HSET bulk update — adds/updates entries without deleting others.
                # This means deleted Telegram messages linger in the hash, but
                # that is acceptable (they won't appear unless refresh adds them).
                # Use full replace only on ?full=true (handled by caller passing
                # the complete merged list).
                pipe.hset(_K_META_HASH, mapping=hash_mapping)

                # Also store sorted JSON list for fast O(1) reads
                pipe.set(
                    _K_META,
                    json.dumps(data, ensure_ascii=False),
                    ex=META_TTL,
                )

                await pipe.execute()
                log.debug("set_meta: wrote %d entries to hash + list cache", len(data))
            else:
                # Redis unavailable — nothing to do here; disk write happens in caller
                log.debug("set_meta: Redis unavailable, skipping Redis write")

        except Exception as exc:
            log.warning("Redis set_meta error: %s", exc)

    async def replace_meta(self, data: list[dict]) -> None:
        """
        Full replacement — deletes the hash and rewrites from scratch.
        Used only when ?full=true to remove stale entries.
        Never replaces with empty data.
        """
        if not data:
            log.warning("replace_meta called with empty list — refusing to replace")
            return
        try:
            if self._r:
                pipe = self._r.pipeline()
                pipe.delete(_K_META_HASH)  # wipe old entries
                hash_mapping = {
                    str(entry["id"]): json.dumps(entry, ensure_ascii=False)
                    for entry in data
                }
                pipe.hset(_K_META_HASH, mapping=hash_mapping)
                pipe.set(_K_META, json.dumps(data, ensure_ascii=False), ex=META_TTL)
                await pipe.execute()
                log.info("replace_meta: replaced hash with %d entries", len(data))
        except Exception as exc:
            log.warning("Redis replace_meta error: %s", exc)

    async def invalidate_meta(self) -> None:
        """Clear the TTL list cache only. Hash stays intact."""
        await self._delete(_K_META)

    # ── Last scanned message id ────────────────────────────────────────────────

    async def get_last_msg_id(self) -> int:
        val = await self._get(LAST_MSG_KEY)
        return int(val) if val else 0

    async def set_last_msg_id(self, msg_id: int) -> None:
        """Stored WITHOUT TTL — must survive Redis restarts."""
        try:
            if self._r:
                await self._r.set(LAST_MSG_KEY, str(msg_id))
        except Exception as exc:
            log.warning("Redis set_last_msg_id: %s", exc)

    # ── Hit counter ────────────────────────────────────────────────────────────

    async def increment_hits(self, msg_id: int) -> int:
        key = _K_HITS.format(id=msg_id)
        try:
            if self._r:
                count = await self._r.incr(key)
                if count == 1:
                    await self._r.expire(key, HITS_TTL)
                return count
        except Exception as exc:
            log.warning("Redis INCR %s: %s", key, exc)
        return 1

    # ── Distributed lock ───────────────────────────────────────────────────────

    async def acquire_lock(self, msg_id: int) -> bool:
        """SET NX EX — returns True if this caller won the lock."""
        key = _K_LOCK.format(id=msg_id)
        try:
            if self._r:
                result = await self._r.set(key, "1", nx=True, ex=LOCK_TTL)
                return result is True
        except Exception as exc:
            log.warning("Redis lock acquire %s: %s", key, exc)
        return True  # no Redis → always grant

    async def release_lock(self, msg_id: int) -> None:
        await self._delete(_K_LOCK.format(id=msg_id))

    async def is_locked(self, msg_id: int) -> bool:
        val = await self._get(_K_LOCK.format(id=msg_id))
        return val is not None

    # ── Disk-ready flag ────────────────────────────────────────────────────────

    async def mark_disk_ready(self, msg_id: int) -> None:
        await self._set(_K_DISK_OK.format(id=msg_id), "1", ex=HITS_TTL)

    async def is_disk_ready(self, msg_id: int) -> bool:
        val = await self._get(_K_DISK_OK.format(id=msg_id))
        return val is not None

    async def unmark_disk_ready(self, msg_id: int) -> None:
        await self._delete(_K_DISK_OK.format(id=msg_id))


# Singleton
redis_svc = RedisService()
