"""
Telegram service — single TelegramClient for the entire app lifetime.

Key design points:
  • Client lives inside FastAPI's own asyncio event loop (no extra threads).
  • Incremental scan: uses min_id to avoid re-reading old messages on refresh.
  • iter_chunks() is a clean async generator — callers own the try/except.
"""
from __future__ import annotations

import logging
from typing import AsyncIterator

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaDocument

from app.core.config import (
    CHUNK_SIZE,
    TG_API_HASH,
    TG_API_ID,
    TG_GROUP,
    TG_SESSION_FILE,
    TG_SESSION_STRING,
)

log = logging.getLogger("tg.telegram")


class TelegramService:
    def __init__(self) -> None:
        session = StringSession(TG_SESSION_STRING) if TG_SESSION_STRING else TG_SESSION_FILE
        self._client = TelegramClient(session, TG_API_ID, TG_API_HASH)

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Connect and authenticate. Non-interactive — session must already exist."""
        await self._client.start()
        me = await self._client.get_me()
        log.info("Telegram connected as %r (id=%s)", me.first_name, me.id)

    async def stop(self) -> None:
        await self._client.disconnect()
        log.info("Telegram disconnected.")

    def is_connected(self) -> bool:
        return self._client.is_connected()

    # ── PDF metadata scan ──────────────────────────────────────────────────────

    async def fetch_pdfs(self, min_id: int = 0) -> tuple[list[dict], int]:
        """
        Scan the group for PDF messages.

        Args:
            min_id: Only fetch messages with id > min_id (incremental scan).
                    Pass 0 for a full scan.

        Returns:
            (list of pdf metadata dicts, highest message id seen)
        """
        results: list[dict] = []
        max_seen_id: int = min_id

        # iter_messages with min_id skips everything already processed.
        # reverse=True goes oldest→newest so we can track max_seen_id correctly.
        async for msg in self._client.iter_messages(
            TG_GROUP,
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
            "fetch_pdfs: min_id=%s → found %d PDFs, max_id=%s",
            min_id, len(results), max_seen_id,
        )
        return results, max_seen_id

    # ── Streaming ──────────────────────────────────────────────────────────────

    async def iter_chunks(self, msg_id: int) -> AsyncIterator[bytes]:
        """
        Yield raw PDF bytes from Telegram chunk by chunk.

        Raises:
            ValueError: if the message doesn't exist or isn't a PDF document.
        """
        msg = await self._client.get_messages(TG_GROUP, ids=msg_id)

        if msg is None:
            raise ValueError(f"Message {msg_id} not found in group {TG_GROUP!r}")

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
