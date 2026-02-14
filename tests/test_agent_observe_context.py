import pytest

from tests.agent_fakes import DummyContext, DummyLegacyBot
from winner_of_day.services.agent_runtime_service import AgentRuntimeService


@pytest.mark.asyncio
async def test_observe_collects_summary_memory_and_context():
    ctx = DummyContext()
    runtime = AgentRuntimeService(ctx, DummyLegacyBot())
    state = {"peer_id": 2000000001, "actor_id": 10, "cmid": 2, "text": "hello"}

    out = await runtime.observe(state)
    context = out.get("context") or {}

    assert context.get("summary_prompt") == "summary:2000000001"
    assert context.get("user_memory_prompt") == "memory:2000000001:10"
    assert isinstance(context.get("peer_turns"), list)
