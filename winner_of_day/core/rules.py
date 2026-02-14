from __future__ import annotations

import re
from typing import Callable

from vkbottle.bot import Message
from vkbottle.dispatch.rules import ABCRule


_bot_group_id_provider: Callable[[], int | None] = lambda: None


def configure_bot_group_id_provider(provider: Callable[[], int | None]) -> None:
    global _bot_group_id_provider
    _bot_group_id_provider = provider


def has_bot_mention(text: str) -> bool:
    group_id = _bot_group_id_provider()
    if not text or not group_id:
        return False
    group_id_str = str(group_id)
    lowered = text.lower()
    if f"@club{group_id_str}" in lowered or f"@public{group_id_str}" in lowered:
        return True
    return re.search(rf"\[(club|public){group_id_str}\|", lowered) is not None


def strip_bot_mention(text: str) -> str:
    group_id = _bot_group_id_provider()
    if not text or not group_id:
        return text
    group_id_str = str(group_id)
    cleaned = re.sub(rf"\[(club|public){group_id_str}\|[^\]]+\]", "", text, flags=re.IGNORECASE)
    cleaned = re.sub(rf"@(?:club|public){group_id_str}\b", "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip()


class StartswithRule(ABCRule[Message]):
    def __init__(self, prefix: str):
        self.prefix = prefix.lower()

    async def check(self, event: Message) -> bool:
        raw_text = event.text or ""
        text = strip_bot_mention(raw_text).strip().lower()
        return text.startswith(self.prefix)


class EqualsRule(ABCRule[Message]):
    def __init__(self, text: str):
        self.text = text.lower()

    async def check(self, event: Message) -> bool:
        raw_text = event.text or ""
        return strip_bot_mention(raw_text).strip().lower() == self.text


def is_chatbot_trigger_message(message: Message, *, admin_user_id: int | None = None) -> bool:
    text = str(message.text or "")
    reply_message = getattr(message, "reply_message", None)
    reply_from_id = getattr(reply_message, "from_id", None) if reply_message is not None else None
    group_id = _bot_group_id_provider()
    is_reply_to_bot = bool(group_id and reply_from_id == -group_id)
    is_admin_dm = bool(
        admin_user_id
        and message.from_id == admin_user_id
        and message.peer_id == message.from_id
    )

    if not text:
        return bool(is_reply_to_bot)

    if text.lstrip().startswith("/"):
        return False
    if re.match(r"^\s*(?:\[(?:club|public)\d+\|[^\]]+\]|@(?:club|public)\d+)\s*/", text, flags=re.IGNORECASE):
        return False
    cleaned = strip_bot_mention(text)
    if cleaned.lstrip().startswith("/"):
        return False

    return is_admin_dm or has_bot_mention(text) or is_reply_to_bot


class ChatbotTriggerRule(ABCRule[Message]):
    def __init__(self, *, admin_user_id: int | None = None):
        self.admin_user_id = admin_user_id

    async def check(self, event: Message) -> bool:
        return is_chatbot_trigger_message(event, admin_user_id=self.admin_user_id)
