from __future__ import annotations

import asyncio
from dataclasses import dataclass, field


@dataclass(slots=True)
class RuntimeState:
    user_name_cache: dict[int, str] = field(default_factory=dict)
    user_name_cache_last_seen_ts: dict[int, int] = field(default_factory=dict)
    user_profile_cache_by_id: dict[int, tuple[str, int, int]] = field(default_factory=dict)
    user_profile_cache_last_access_ts: dict[int, int] = field(default_factory=dict)
    peer_user_profile_cache_by_key: dict[tuple[int, int], tuple[str, int, int]] = field(default_factory=dict)
    peer_user_profile_cache_last_access_ts: dict[tuple[int, int], int] = field(default_factory=dict)
    peer_title_cache_by_peer: dict[int, tuple[str, int, int]] = field(default_factory=dict)
    peer_title_cache_last_access_ts: dict[int, int] = field(default_factory=dict)
    peer_title_last_refresh_ts_by_peer: dict[int, int] = field(default_factory=dict)
    last_bot_message_ts_by_peer: dict[int, int] = field(default_factory=dict)
    messages_since_bot_by_peer: dict[int, int] = field(default_factory=dict)
    proactive_locks: dict[int, asyncio.Lock] = field(default_factory=dict)
    game_locks: dict[int, asyncio.Lock] = field(default_factory=dict)
    last_reaction_ts_by_peer: dict[int, int] = field(default_factory=dict)
    last_reaction_cmid_by_peer: dict[int, int] = field(default_factory=dict)
    processed_message_keys: set[tuple[int, int]] = field(default_factory=set)
    processed_message_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
