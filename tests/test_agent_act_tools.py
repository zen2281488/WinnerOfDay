import pytest

from tests.agent_fakes import DummyContext, DummyLegacyBot
from winner_of_day.agent.models import AgentActionResult
from winner_of_day.services.agent_runtime_service import AgentRuntimeService


@pytest.mark.asyncio
async def test_act_send_message_uses_messages_send():
    ctx = DummyContext(settings={"CHATBOT_AGENT_MODE": "active"})
    runtime = AgentRuntimeService(ctx, DummyLegacyBot())
    state = {
        "peer_id": 2000000001,
        "actor_id": 7,
        "cmid": 5,
        "decision": {
            "action": "send_message",
            "text": "agent text",
            "reply_to_cmid": 0,
            "target_cmid": 0,
            "reaction_id": 0,
            "reason": "ok",
        },
    }

    out = await runtime.act(state)
    result = AgentActionResult.from_value(out.get("action_result"))
    assert result.executed is True
    assert result.vk_method == "messages.send"
    assert ctx.bot.api.calls[-1][0] == "messages.send"


@pytest.mark.asyncio
async def test_act_react_uses_messages_send_reaction():
    ctx = DummyContext(settings={"CHATBOT_AGENT_MODE": "active"})
    runtime = AgentRuntimeService(ctx, DummyLegacyBot())
    state = {
        "peer_id": 2000000001,
        "actor_id": 7,
        "cmid": 9,
        "decision": {
            "action": "react",
            "text": "",
            "reply_to_cmid": 0,
            "target_cmid": 9,
            "reaction_id": 3,
            "reason": "ok",
        },
    }

    out = await runtime.act(state)
    result = AgentActionResult.from_value(out.get("action_result"))
    assert result.executed is True
    assert result.vk_method == "messages.sendReaction"
    assert ctx.bot.api.calls[-1][0] == "messages.sendReaction"
