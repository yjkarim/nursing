"""
Central configuration — reads strictly from environment variables.
No defaults for secrets; the app will fail fast if they are missing.
"""
from __future__ import annotations

import os
from pathlib import Path


def _require(key: str) -> str:
    val = os.environ.get(key, "").strip()
    if not val:
        raise RuntimeError(f"Required environment variable '{key}' is not set.")
    return val


def _optional(key: str, default: str = "") -> str:
    return os.environ.get(key, default).strip()


# ── Telegram ───────────────────────────────────────────────────────────────────
TG_API_ID: int = int(_require("TG_API_ID"))
TG_API_HASH: str = _require("TG_API_HASH")
TG_GROUP: str = _require("TG_GROUP")

# StringSession (recommended for Railway / cloud — no file loss on redeploy)
# Generate once locally with:  python gen_session.py
TG_SESSION_STRING: str = _optional("TG_SESSION_STRING")
# File-based session (local dev only — never use on stateless deployments)
TG_SESSION_FILE: str = _optional("TG_SESSION", "session")

# ── Redis ──────────────────────────────────────────────────────────────────────
REDIS_URL: str = _optional("REDIS_URL", "redis://localhost:6379")

# ── Disk cache ─────────────────────────────────────────────────────────────────
CACHE_DIR: Path = Path(_optional("CACHE_DIR", "/tmp/tg_cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ── Tuning ────────────────────────────────────────────────────────────────────
# Chunk size for streaming from Telegram and reading from disk
CHUNK_SIZE: int = int(_optional("CHUNK_SIZE", str(512 * 1024)))  # 512 KB

# Persist a file to disk after this many requests (avoids caching cold files)
MIN_HITS_TO_CACHE: int = int(_optional("MIN_HITS", "2"))

# Redis TTLs (seconds)
META_TTL: int = int(_optional("META_TTL", str(30 * 60)))   # 30 min
HITS_TTL: int = int(_optional("HITS_TTL", str(24 * 3600))) # 24 h
LOCK_TTL: int = int(_optional("LOCK_TTL", "90"))           # 90 s per download

# LRU eviction threshold
DISK_MAX_BYTES: int = int(float(_optional("DISK_MAX_GB", "2")) * 1024 ** 3)

# Incremental Telegram scan: persist last seen message id to avoid full scans
LAST_MSG_KEY: str = "tg:last_msg_id"

# ── Paths ──────────────────────────────────────────────────────────────────────
HERE: Path = Path(__file__).parent.parent.parent  # project root
