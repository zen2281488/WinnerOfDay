from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable


class GameService:
    def __init__(self):
        self._locks: dict[int, asyncio.Lock] = {}

    def get_lock(self, peer_id: int) -> asyncio.Lock:
        key = int(peer_id or 0)
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        return lock

    async def run_with_peer_lock(self, peer_id: int, runner: Callable[[], Awaitable[None]]) -> bool:
        lock = self.get_lock(peer_id)
        if lock.locked():
            return False
        async with lock:
            await runner()
        return True
