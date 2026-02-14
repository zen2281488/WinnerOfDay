from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, TypedDict

AgentAction = Literal["none", "send_message", "react"]


def _coerce_positive_int(value: Any) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return 0
    return number if number > 0 else 0


@dataclass(slots=True)
class AgentDecision:
    action: AgentAction = "none"
    text: str = ""
    reply_to_cmid: int = 0
    target_cmid: int = 0
    reaction_id: int = 0
    reason: str = ""

    @classmethod
    def from_value(cls, value: Any) -> "AgentDecision":
        if isinstance(value, cls):
            return value
        if not isinstance(value, dict):
            return cls(action="none", reason="invalid_payload")
        action = str(value.get("action") or "none").strip().lower()
        if action not in {"none", "send_message", "react"}:
            action = "none"
        text = str(value.get("text") or "").strip()
        return cls(
            action=action,  # type: ignore[arg-type]
            text=text,
            reply_to_cmid=_coerce_positive_int(value.get("reply_to_cmid")),
            target_cmid=_coerce_positive_int(value.get("target_cmid")),
            reaction_id=_coerce_positive_int(value.get("reaction_id")),
            reason=str(value.get("reason") or "").strip(),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "text": self.text,
            "reply_to_cmid": int(self.reply_to_cmid or 0),
            "target_cmid": int(self.target_cmid or 0),
            "reaction_id": int(self.reaction_id or 0),
            "reason": self.reason,
        }


@dataclass(slots=True)
class AgentActionResult:
    executed: bool = False
    vk_method: str = ""
    vk_response_id: int = 0
    error: str = ""

    @classmethod
    def from_value(cls, value: Any) -> "AgentActionResult":
        if isinstance(value, cls):
            return value
        if not isinstance(value, dict):
            return cls()
        return cls(
            executed=bool(value.get("executed")),
            vk_method=str(value.get("vk_method") or ""),
            vk_response_id=_coerce_positive_int(value.get("vk_response_id")),
            error=str(value.get("error") or "").strip(),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "executed": bool(self.executed),
            "vk_method": self.vk_method,
            "vk_response_id": int(self.vk_response_id or 0),
            "error": self.error,
        }


class AgentState(TypedDict, total=False):
    peer_id: int
    actor_id: int
    cmid: int
    text: str
    context: dict[str, Any]
    decision: dict[str, Any]
    action_result: dict[str, Any]
    error: str
