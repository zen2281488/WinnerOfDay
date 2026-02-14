import pytest

from tests.agent_fakes import DummyContext, DummyLegacyBot
from winner_of_day.agent.models import AgentDecision
from winner_of_day.services.agent_runtime_service import AgentRuntimeService


def test_agent_decision_schema_valid_payload():
    decision = AgentDecision.from_value(
        {
            "action": "send_message",
            "text": "hi",
            "reply_to_cmid": 7,
            "target_cmid": 0,
            "reaction_id": 0,
            "reason": "ok",
        }
    )
    assert decision.action == "send_message"
    assert decision.reply_to_cmid == 7
    assert decision.text == "hi"


def test_agent_decision_schema_invalid_payload_falls_back_to_none():
    decision = AgentDecision.from_value({"action": "ban_user"})
    assert decision.action == "none"


@pytest.mark.asyncio
async def test_decide_invalid_json_returns_none_action():
    class RuntimeWithInvalidJson(AgentRuntimeService):
        async def _venice_request_text(self, payload):  # noqa: ARG002
            return "not-json"

    ctx = DummyContext(settings={"CHATBOT_AGENT_MAX_CHARS": "120"})
    runtime = RuntimeWithInvalidJson(ctx, DummyLegacyBot())
    state = {"peer_id": 2000000001, "actor_id": 11, "cmid": 3, "text": "test", "context": {}}

    out = await runtime.decide(state)
    decision = AgentDecision.from_value(out.get("decision"))
    assert decision.action == "none"
