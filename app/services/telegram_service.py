"""
Telegram service — single TelegramClient for the entire app lifetime.

Key design points:
  • Client lives inside FastAPI's own asyncio event loop (no extra threads).
  • Incremental scan: uses min_id to avoid re-reading old messages on refresh.
  • iter_chunks() is a clean async generator — callers own the try/except.

# CHANGED: fetch_pdfs() and iter_chunks() now accept `group: str`.
# The group slug is resolved to its Telegram entity (username / chat-id)
# via get_group_id() before any Telegram API call.
# The single TelegramClient instance is reused for all groups — Telethon
# resolves entities on-the-fly, so no extra client instances are needed.
"""
from __future__ import annotations
import asyncio
import logging
from typing import AsyncIterator

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaDocument

from app.core.config import (
    CHUNK_SIZE,
    TG_API_HASH,
    TG_API_ID,
    TG_SESSION_FILE,
    TG_SESSION_STRING,
    get_group_id,
)

log = logging.getLogger("tg.telegram")


class TelegramService:
    def __init__(self) -> None:
        session = (
            StringSession(TG_SESSION_STRING)
            if TG_SESSION_STRING
            else TG_SESSION_FILE
        )

        self._client = TelegramClient(session, TG_API_ID, TG_API_HASH)

        self._started = False
        self._lock = asyncio.Lock()

    # ── Lifecycle ─────────────────────────────────────────

    async def start(self) -> None:
        async with self._lock:
            if self._started:
                log.warning("Telegram already started — skipping")
                return

            self._started = True
            await self._client.start()

            me = await self._client.get_me()
            log.info("Telegram connected as %r (id=%s)", me.first_name, me.id)

    async def stop(self) -> None:
        if not self._started:
            return

        self._started = False
        await self._client.disconnect()
        log.info("Telegram disconnected.")

    def is_connected(self) -> bool:
        return self._client.is_connected()
    
    # ── PDF metadata scan (per group) ──────────────────────────────────────────

    async def fetch_pdfs(self, group: str, min_id: int = 0) -> tuple[list[dict], int]:
        """
        Scan the specified group for PDF messages.

        # CHANGED: `group` slug is resolved to Telegram entity via get_group_id().
        # min_id remains per-group (stored in Redis under tg:{group}:last_msg_id).

        Args:
            group:  group slug ("grade1", "grade2", …)
            min_id: Only fetch messages with id > min_id (incremental scan).
                    Pass 0 for a full scan.

        Returns:
            (list of pdf metadata dicts, highest message id seen)
        """
        tg_entity = get_group_id(group)   # validates slug; raises HTTP 400 if unknown
        results: list[dict] = []
        max_seen_id: int = min_id

        async for msg in self._client.iter_messages(
            tg_entity,
            min_id=min_id,
            reverse=True,   # oldest first — needed for correct min_id tracking
        ):
            max_seen_id = max(max_seen_id, msg.id)

            if not (msg.media and isinstance(msg.media, MessageMediaDocument)):
                continue

            doc = msg.media.document
            if doc.mime_type != "application/pdf":
                continue

            name = next(
                (
                    attr.file_name
                    for attr in doc.attributes
                    if hasattr(attr, "file_name") and attr.file_name
                ),
                f"document_{msg.id}.pdf",
            )
            results.append({"id": msg.id, "name": name, "size": doc.size})

        log.info(
            "[group=%s] fetch_pdfs: min_id=%s → found %d PDFs, max_id=%s",
            group, min_id, len(results), max_seen_id,
        )
        return results, max_seen_id

    # ── Streaming (per group) ──────────────────────────────────────────────────

    async def iter_chunks(self, group: str, msg_id: int) -> AsyncIterator[bytes]:
        """
        Yield raw PDF bytes from Telegram chunk by chunk.

        # CHANGED: group slug resolves to the correct Telegram entity.
        # Without this, a msg_id from grade1 could be fetched from grade3's
        # channel if the old single-group TG_GROUP was still hardcoded.

        Raises:
            ValueError: if the message doesn't exist or isn't a PDF document.
            HTTPException (400): if group slug is unknown (from get_group_id).
        """
        tg_entity = get_group_id(group)
        msg = await self._client.get_messages(tg_entity, ids=msg_id)

        if msg is None:
            raise ValueError(f"Message {msg_id} not found in group {tg_entity!r}")

        if not isinstance(msg.media, MessageMediaDocument):
            raise ValueError(f"Message {msg_id} has no document media")

        if msg.media.document.mime_type != "application/pdf":
            raise ValueError(
                f"Message {msg_id} is not a PDF "
                f"(mime={msg.media.document.mime_type!r})"
            )

        async for chunk in self._client.iter_download(
            msg.media, chunk_size=CHUNK_SIZE
        ):
            yield bytes(chunk)


# ── Singleton ──────────────────────────────────────────────────────────────────
telegram_svc = TelegramService()
