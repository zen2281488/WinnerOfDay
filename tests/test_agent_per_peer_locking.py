import asyncio

import pytest

from tests.agent_fakes import DummyContext, DummyLegacyBot, DummyMessage
from winner_of_day.services.agent_runtime_service import AgentRuntimeService


@pytest.mark.asyncio
async def test_per_peer_locking_prevents_double_execution():
    class FakeGraph:
        def __init__(self, runtime: AgentRuntimeService):
            self.runtime = runtime
            self.calls = 0

        async def ainvoke(self, state, config=None):  # noqa: ARG002
            self.calls += 1
            await asyncio.sleep(0.05)
            peer_id = int(state.get("peer_id") or 0)
            self.runtime._get_state_map("last_bot_message_ts_by_peer")[peer_id] = self.runtime._now_ts()
            self.runtime._get_state_map("messages_since_bot_by_peer")[peer_id] = 0
            return {"action_result": {"executed": True, "vk_method": "messages.send"}}

    ctx = DummyContext(
        settings={
            "CHATBOT_AGENT_ENABLED": "true",
            "CHATBOT_AGENT_ENGINE": "langgraph",
            "CHATBOT_AGENT_MODE": "active",
            "CHATBOT_AGENT_PROBABILITY": "1",
            "CHATBOT_AGENT_COOLDOWN_SECONDS": "120",
            "CHATBOT_AGENT_MIN_MESSAGES_SINCE_BOT": "1",
        }
    )
    runtime = AgentRuntimeService(ctx, DummyLegacyBot())
    fake_graph = FakeGraph(runtime)
    runtime._graph = fake_graph

    message = DummyMessage(peer_id=2000000009, from_id=77, text="hello")
    outcomes = await asyncio.gather(
        runtime.handle_incoming_message(message),
        runtime.handle_incoming_message(message),
    )

    assert fake_graph.calls == 1
    assert outcomes.count(True) == 1
    assert outcomes.count(False) == 1
