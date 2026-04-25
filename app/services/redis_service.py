"""
Redis service — async wrapper with graceful degradation.
Every method works even when Redis is unavailable (falls back to safe defaults).

# CHANGED: All public methods now accept a `group: str` parameter.
# Every Redis key is prefixed with the group slug to guarantee complete
# isolation between tenants. No key from group A can ever collide with
# a key from group B.

Storage design for metadata (per group):
  tg:{group}:meta        → JSON list, TTL=META_TTL  (fast read cache)
  tg:{group}:meta:hash   → Redis Hash keyed by msg_id (persistent, no TTL)

Having two stores solves two problems:
  1. tg:{group}:meta TTL expiry: hash survives and can rebuild the list
  2. Redis restart: disk backup rebuilds both
"""
from __future__ import annotations
import json
import logging
from typing import Optional

import redis.asyncio as aioredis
from redis.asyncio import Redis

from app.core.config import HITS_TTL, LOCK_TTL, META_TTL, REDIS_URL

log = logging.getLogger("tg.redis")


# ── Key schema (group-namespaced) ──────────────────────────────────────────────
# NEW: All keys include the group slug as the second segment.
# This makes SCAN / DEBUG easy: `redis-cli KEYS "tg:grade1:*"` isolates one tenant.

def _k_meta(group: str) -> str:
    return f"tg:{group}:meta"

def _k_meta_hash(group: str) -> str:
    return f"tg:{group}:meta:hash"

def _k_hits(group: str, msg_id: int) -> str:
    return f"tg:{group}:hits:{msg_id}"

def _k_lock(group: str, msg_id: int) -> str:
    return f"tg:{group}:lock:{msg_id}"

def _k_disk_ok(group: str, msg_id: int) -> str:
    return f"tg:{group}:disk_ok:{msg_id}"

def _k_last_msg(group: str) -> str:
    return f"tg:{group}:last_msg_id"


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
            log.warning(
                "Redis unavailable (%s) — degraded mode (no caching coordination)", exc
            )
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

    # ── Metadata — dual storage (per group) ───────────────────────────────────

    async def get_meta(self, group: str) -> Optional[list[dict]]:
        """
        Read metadata for a specific group.
        Priority:
          1. tg:{group}:meta JSON list (fast, TTL-based cache)
          2. tg:{group}:meta:hash (persistent, rebuilt if list expired)
        Returns None only when both are empty (truly first run for this group).
        """
        # Layer 1: TTL list cache
        raw = await self._get(_k_meta(group))
        if raw:
            try:
                data = json.loads(raw)
                if data:
                    return data
            except json.JSONDecodeError:
                log.warning("[group=%s] tg:meta JSON decode error — falling back to hash", group)

        # Layer 2: Persistent hash (survives TTL expiry and Redis restarts)
        try:
            if self._r:
                hash_data = await self._r.hgetall(_k_meta_hash(group))
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
                            "[group=%s] Rebuilt metadata from hash store: %d entries",
                            group, len(entries),
                        )
                        # Refresh TTL list cache from the hash
                        await self._set(
                            _k_meta(group),
                            json.dumps(entries, ensure_ascii=False),
                            ex=META_TTL,
                        )
                        return entries
        except Exception as exc:
            log.warning("Redis HGETALL %s: %s", _k_meta_hash(group), exc)

        return None

    async def set_meta(self, group: str, data: list[dict]) -> None:
        """
        Write metadata for a group to both stores atomically (pipeline).
        - tg:{group}:meta:hash  → persistent, no TTL, keyed by msg_id
        - tg:{group}:meta       → TTL list cache for fast reads
        Never writes empty data.
        """
        if not data:
            log.warning("[group=%s] set_meta called with empty list — refusing to write", group)
            return

        try:
            if self._r:
                pipe = self._r.pipeline()
                hash_mapping = {
                    str(entry["id"]): json.dumps(entry, ensure_ascii=False)
                    for entry in data
                }
                pipe.hset(_k_meta_hash(group), mapping=hash_mapping)
                pipe.set(
                    _k_meta(group),
                    json.dumps(data, ensure_ascii=False),
                    ex=META_TTL,
                )
                await pipe.execute()
                log.debug(
                    "[group=%s] set_meta: wrote %d entries to hash + list cache",
                    group, len(data),
                )
            else:
                log.debug("[group=%s] set_meta: Redis unavailable, skipping Redis write", group)
        except Exception as exc:
            log.warning("[group=%s] Redis set_meta error: %s", group, exc)

    async def replace_meta(self, group: str, data: list[dict]) -> None:
        """
        Full replacement for a group — deletes the hash and rewrites from scratch.
        Used only when ?full=true. Never replaces with empty data.
        """
        if not data:
            log.warning(
                "[group=%s] replace_meta called with empty list — refusing to replace", group
            )
            return
        try:
            if self._r:
                pipe = self._r.pipeline()
                pipe.delete(_k_meta_hash(group))
                hash_mapping = {
                    str(entry["id"]): json.dumps(entry, ensure_ascii=False)
                    for entry in data
                }
                pipe.hset(_k_meta_hash(group), mapping=hash_mapping)
                pipe.set(_k_meta(group), json.dumps(data, ensure_ascii=False), ex=META_TTL)
                await pipe.execute()
                log.info(
                    "[group=%s] replace_meta: replaced hash with %d entries", group, len(data)
                )
        except Exception as exc:
            log.warning("[group=%s] Redis replace_meta error: %s", group, exc)

    async def invalidate_meta(self, group: str) -> None:
        """Clear the TTL list cache for a group only. Hash stays intact."""
        await self._delete(_k_meta(group))

    # ── Last scanned message id (per group) ────────────────────────────────────

    async def get_last_msg_id(self, group: str) -> int:
        val = await self._get(_k_last_msg(group))
        return int(val) if val else 0

    async def set_last_msg_id(self, group: str, msg_id: int) -> None:
        """Stored WITHOUT TTL — must survive Redis restarts."""
        try:
            if self._r:
                await self._r.set(_k_last_msg(group), str(msg_id))
        except Exception as exc:
            log.warning("[group=%s] Redis set_last_msg_id: %s", group, exc)

    # ── Hit counter (per group + message) ─────────────────────────────────────

    async def increment_hits(self, group: str, msg_id: int) -> int:
        key = _k_hits(group, msg_id)
        try:
            if self._r:
                count = await self._r.incr(key)
                if count == 1:
                    await self._r.expire(key, HITS_TTL)
                return count
        except Exception as exc:
            log.warning("Redis INCR %s: %s", key, exc)
        return 1

    # ── Distributed lock (per group + message) ─────────────────────────────────

    async def acquire_lock(self, group: str, msg_id: int) -> bool:
        """SET NX EX — returns True if this caller won the lock."""
        key = _k_lock(group, msg_id)
        try:
            if self._r:
                result = await self._r.set(key, "1", nx=True, ex=LOCK_TTL)
                return result is True
        except Exception as exc:
            log.warning("Redis lock acquire %s: %s", key, exc)
        return True  # no Redis → always grant

    async def release_lock(self, group: str, msg_id: int) -> None:
        await self._delete(_k_lock(group, msg_id))

    async def is_locked(self, group: str, msg_id: int) -> bool:
        val = await self._get(_k_lock(group, msg_id))
        return val is not None

    # ── Disk-ready flag (per group + message) ──────────────────────────────────

    async def mark_disk_ready(self, group: str, msg_id: int) -> None:
        await self._set(_k_disk_ok(group, msg_id), "1", ex=HITS_TTL)

    async def is_disk_ready(self, group: str, msg_id: int) -> bool:
        val = await self._get(_k_disk_ok(group, msg_id))
        return val is not None

    async def unmark_disk_ready(self, group: str, msg_id: int) -> None:
        await self._delete(_k_disk_ok(group, msg_id))


# Singleton
redis_svc = RedisService()
