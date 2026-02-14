from __future__ import annotations

from typing import Any

from .models import AgentState


def _coerce_positive_int(value: Any) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return 0
    return number if number > 0 else 0


def extract_conversation_message_id(message: Any) -> int:
    candidate = getattr(message, "conversation_message_id", None)
    if candidate is None and isinstance(message, dict):
        candidate = message.get("conversation_message_id") or message.get("cmid")
    return _coerce_positive_int(candidate)


def build_initial_state(message: Any) -> AgentState:
    peer_id = _coerce_positive_int(getattr(message, "peer_id", None))
    actor_id = _coerce_positive_int(getattr(message, "from_id", None))
    text = str(getattr(message, "text", "") or "").strip()
    return {
        "peer_id": peer_id,
        "actor_id": actor_id,
        "cmid": extract_conversation_message_id(message),
        "text": text,
        "context": {},
        "decision": {},
        "action_result": {},
        "error": "",
    }
