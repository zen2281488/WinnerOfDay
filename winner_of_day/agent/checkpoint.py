from __future__ import annotations

import os

import aiosqlite
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver


class AgentCheckpoint:
    def __init__(self, db_path: str):
        self.db_path = str(db_path or "data/langgraph_agent.sqlite3")
        self._conn: aiosqlite.Connection | None = None
        self._saver: AsyncSqliteSaver | None = None

    @property
    def saver(self) -> AsyncSqliteSaver | None:
        return self._saver

    async def start(self) -> AsyncSqliteSaver:
        if self._saver is not None:
            return self._saver
        folder = os.path.dirname(self.db_path)
        if folder:
            os.makedirs(folder, exist_ok=True)
        self._conn = await aiosqlite.connect(self.db_path)
        self._saver = AsyncSqliteSaver(self._conn)
        await self._saver.setup()
        return self._saver

    async def stop(self) -> None:
        self._saver = None
        if self._conn is not None:
            await self._conn.close()
            self._conn = None
