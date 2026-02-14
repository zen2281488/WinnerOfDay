import pytest

from tests.agent_fakes import DummyContext, DummyLegacyBot
from winner_of_day.agent.models import AgentActionResult
from winner_of_day.services.agent_runtime_service import AgentRuntimeService


@pytest.mark.asyncio
async def test_shadow_mode_does_not_call_vk_api():
    ctx = DummyContext(
        settings={
            "CHATBOT_AGENT_MODE": "shadow",
        }
    )
    runtime = AgentRuntimeService(ctx, DummyLegacyBot())
    state = {
        "peer_id": 2000000001,
        "actor_id": 1,
        "cmid": 4,
        "decision": {
            "action": "send_message",
            "text": "shadow text",
            "reply_to_cmid": 0,
            "target_cmid": 0,
            "reaction_id": 0,
            "reason": "test",
        },
    }

    out = await runtime.act(state)
    result = AgentActionResult.from_value(out.get("action_result"))
    assert result.executed is False
    assert result.vk_method == "shadow"
    assert ctx.bot.api.calls == []
