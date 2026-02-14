from __future__ import annotations

import aiosqlite


def connect_db(db_path: str):
    return aiosqlite.connect(db_path)
