from __future__ import annotations

import re

from .text import normalize_spaces


def parse_user_id(value: str) -> int | None:
    cleaned = (value or "").strip()
    if not cleaned:
        return None
    match = re.search(r"\[id(\d+)\|", cleaned, flags=re.IGNORECASE)
    if match:
        return int(match.group(1))
    match = re.search(r"@id(\d+)\b", cleaned, flags=re.IGNORECASE)
    if match:
        return int(match.group(1))
    match = re.fullmatch(r"id(\d+)", cleaned, flags=re.IGNORECASE)
    if match:
        return int(match.group(1))
    if cleaned.isdigit():
        return int(cleaned)
    return None


def parse_llm_scope(value: str) -> str | None:
    if not value:
        return None
    normalized = value.strip().lower()
    if normalized in ("chat", "чат"):
        return "chat"
    if normalized in ("game", "игра"):
        return "game"
    if normalized in ("ops", "operational", "оперативка", "оперативный", "служебный", "служебка"):
        return "ops"
    return None


def normalize_username(value: str) -> str:
    return normalize_spaces(value).casefold()


def _coerce_positive_int(value) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _coerce_int(value) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def extract_reply_text(message) -> str:
    reply_message = getattr(message, "reply_message", None)
    if not reply_message:
        return ""
    reply_text = getattr(reply_message, "text", None)
    if reply_text is None and isinstance(reply_message, dict):
        reply_text = reply_message.get("text")
    return str(reply_text) if reply_text else ""


def extract_reply_from_id(message):
    reply_message = getattr(message, "reply_message", None)
    if not reply_message:
        return None
    reply_from_id = getattr(reply_message, "from_id", None)
    if reply_from_id is None and isinstance(reply_message, dict):
        reply_from_id = reply_message.get("from_id")
    return reply_from_id


def extract_reply_conversation_message_id(message) -> int | None:
    reply_message = getattr(message, "reply_message", None)
    if not reply_message:
        return None
    reply_cmid = getattr(reply_message, "conversation_message_id", None)
    if reply_cmid is None and isinstance(reply_message, dict):
        reply_cmid = reply_message.get("conversation_message_id") or reply_message.get("cmid")
    return _coerce_positive_int(reply_cmid)


def get_conversation_message_id(message) -> int | None:
    if message is None:
        return None

    def pick(value) -> int | None:
        return _coerce_positive_int(value)

    for key in ("conversation_message_id", "cmid"):
        candidate = pick(getattr(message, key, None))
        if candidate:
            return candidate
        if isinstance(message, dict):
            candidate = pick(message.get(key))
            if candidate:
                return candidate

    for container_key in ("object", "message"):
        container = getattr(message, container_key, None)
        if container is None and isinstance(message, dict):
            container = message.get(container_key)
        if container is None:
            continue
        candidate = pick(getattr(container, "conversation_message_id", None))
        if candidate:
            return candidate
        if isinstance(container, dict):
            candidate = pick(container.get("conversation_message_id") or container.get("cmid"))
            if candidate:
                return candidate

    return None
