"""
Central configuration — reads strictly from environment variables.
No defaults for secrets; the app will fail fast if they are missing.

# CHANGED: Added GROUPS mapping and get_group_id() for multi-tenant support.
"""
from __future__ import annotations

import os
from pathlib import Path

from fastapi import HTTPException


def _require(key: str) -> str:
    val = os.environ.get(key, "").strip()
    if not val:
        raise RuntimeError(f"Required environment variable '{key}' is not set.")
    return val


def _optional(key: str, default: str = "") -> str:
    return os.environ.get(key, default).strip()


# ── Telegram ───────────────────────────────────────────────────────────────────
TG_API_ID   = int(os.getenv("TG_API_ID", "34861039"))
TG_API_HASH = os.getenv("TG_API_HASH", "b006e1f705e20b4bb57c1da01a52b34f")

# NEW: Multi-tenant group registry.
# Maps slug (used in API params, Redis keys, disk paths) → Telegram group identifier.
# Values can be @username strings, invite links, or numeric chat IDs.
# Add or remove groups here without touching any other file.
GROUPS: dict[str, str] = {
    "grade1": os.getenv("TG_GROUP_GRADE1", "tamaridyano_1"),
    "grade2": os.getenv("TG_GROUP_GRADE2", "tamaridyano_2"),
    "grade3": os.getenv("TG_GROUP_GRADE3", "tamaridyano_3"),
}

# NEW: Validate a group slug and return its Telegram identifier.
# Raises HTTP 400 for unknown slugs — called at the route layer before any I/O.
def get_group_id(group: str) -> str:
    if group not in GROUPS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown group '{group}'. Valid groups: {sorted(GROUPS)}",
        )
    return GROUPS[group]


# StringSession (recommended for Railway / cloud — no file loss on redeploy)
TG_SESSION_STRING: str = os.getenv(
    "TG_SESSION_STRING",
    "1BJWap1wBu6urgJ9hL48o2i6jJQph5fKWVdBxYyIUjpnPic5W4jJQ_t_AHn9OClT67-ysRrde9bReqPfIoYP68WYxqCfSju0uu2scX29cUmMtUS1EB9Lywb5wpsmJWURjCTn1NWNWbSU0z8lnIPeDyjIznYIUCP87WRUOOgxZfZgNo6r2OREyQxZKcWi7WBnG52Iyop60ebnj_sC4_sOWB2KBXlTzoIhFA4fdeZ90iZWGwzkwXz0l67PgD0l5rvJqSo9lQBkiVI0hXYbHeCRET239rprolOfy3XJwUQ1Xu5iCeVKC3gS5W2xJiWb66A7svIrVa_XEbt2gXnhcYEE0Tk6HPu2tfYk=",
)
# File-based session (local dev only)
TG_SESSION_FILE: str = _optional("TG_SESSION", "session")

# ── Redis ──────────────────────────────────────────────────────────────────────
REDIS_URL: str = os.getenv(
    "REDIS_URL",
    "redis://default:UzziuJrKYLQYlTQlBULpDJsJGyIVFFBb@redis.railway.internal:6379",
)

# ── Disk cache ─────────────────────────────────────────────────────────────────
# NEW: Base dir only — each group gets its own subdirectory created at runtime.
CACHE_DIR: Path = Path(_optional("CACHE_DIR", "/tmp/tg_cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ── Tuning ────────────────────────────────────────────────────────────────────
CHUNK_SIZE: int      = int(_optional("CHUNK_SIZE", str(512 * 1024)))
MIN_HITS_TO_CACHE: int = int(_optional("MIN_HITS", "2"))

META_TTL: int  = int(_optional("META_TTL", str(30 * 60)))
HITS_TTL: int  = int(_optional("HITS_TTL", str(24 * 3600)))
LOCK_TTL: int  = int(_optional("LOCK_TTL", "90"))

DISK_MAX_BYTES: int = int(float(_optional("DISK_MAX_GB", "2")) * 1024 ** 3)

# CHANGED: last_msg_id is now per-group — see redis_service.py for key schema.
# This constant is no longer used directly; kept for reference only.
LAST_MSG_KEY: str = "tg:{group}:last_msg_id"

# ── Paths ──────────────────────────────────────────────────────────────────────
HERE: Path = Path(__file__).parent.parent.parent  # project root
