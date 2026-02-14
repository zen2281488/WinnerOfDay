import pytest

from tests.agent_fakes import DummyContext, DummyLegacyBot
from winner_of_day.agent.models import AgentActionResult
from winner_of_day.services.agent_runtime_service import AgentRuntimeService


@pytest.mark.asyncio
async def test_active_mode_executes_single_action():
    ctx = DummyContext(settings={"CHATBOT_AGENT_MODE": "active"})
    runtime = AgentRuntimeService(ctx, DummyLegacyBot())
    state = {
        "peer_id": 2000000002,
        "actor_id": 2,
        "cmid": 12,
        "decision": {
            "action": "send_message",
            "text": "active text",
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
    assert len(ctx.bot.api.calls) == 1
