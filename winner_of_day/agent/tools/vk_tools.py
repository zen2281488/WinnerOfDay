from __future__ import annotations

from typing import Any


class VkTools:
    def __init__(self, bot):
        self.bot = bot

    async def send_message(self, *, peer_id: int, text: str, reply_to_cmid: int = 0) -> int:
        payload: dict[str, Any] = {
            "peer_id": int(peer_id),
            "message": str(text or ""),
            "random_id": 0,
        }
        if int(reply_to_cmid or 0) > 0:
            payload["reply_to"] = int(reply_to_cmid)
        response = await self.bot.api.request("messages.send", payload)
        try:
            return int(response or 0)
        except (TypeError, ValueError):
            return 0

    async def send_reaction(self, *, peer_id: int, cmid: int, reaction_id: int) -> int:
        payload = {
            "peer_id": int(peer_id),
            "cmid": int(cmid),
            "reaction_id": int(reaction_id),
        }
        response = await self.bot.api.request("messages.sendReaction", payload)
        try:
            return int(response or 1)
        except (TypeError, ValueError):
            return 1
