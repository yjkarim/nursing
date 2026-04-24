"""
Run this ONCE locally to generate a Telethon StringSession.
Copy the printed string into your Railway env var: TG_SESSION_STRING

Usage:
    python gen_session.py
"""
import asyncio

from telethon import TelegramClient
from telethon.sessions import StringSession


async def main() -> None:
    api_id   = int(input("TG_API_ID  : ").strip())
    api_hash =     input("TG_API_HASH: ").strip()

    async with TelegramClient(StringSession(), api_id, api_hash) as client:
        me = await client.get_me()
        print(f"\n✅  Authenticated as: {me.first_name} (id={me.id})")
        print("\nPaste the following into your TG_SESSION_STRING env var:\n")
        print(client.session.save())
        print()


if __name__ == "__main__":
    asyncio.run(main())
