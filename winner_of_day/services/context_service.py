from __future__ import annotations

import asyncio


class MessageDeduplicator:
    def __init__(self, max_size: int = 200_000):
        self.max_size = int(max_size)
        self._seen: set[tuple[int, int]] = set()
        self._lock = asyncio.Lock()

    async def try_mark(self, peer_id: int, conversation_message_id: int) -> bool:
        key = (int(peer_id or 0), int(conversation_message_id or 0))
        if key[0] <= 0 or key[1] <= 0:
            return True
        async with self._lock:
            if key in self._seen:
                return False
            self._seen.add(key)
            if len(self._seen) > self.max_size:
                self._seen.clear()
                self._seen.add(key)
            return True

    async def release(self, peer_id: int, conversation_message_id: int) -> None:
        key = (int(peer_id or 0), int(conversation_message_id or 0))
        if key[0] <= 0 or key[1] <= 0:
            return
        async with self._lock:
            self._seen.discard(key)


class ContextService:
    pass
