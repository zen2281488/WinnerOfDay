import pytest

from tests.agent_fakes import DummyContext, DummyLegacyBot
from winner_of_day.agent.models import AgentDecision
from winner_of_day.services.agent_runtime_service import AgentRuntimeService


@pytest.mark.asyncio
async def test_decide_returns_none_on_venice_failure():
    class RuntimeWithVeniceError(AgentRuntimeService):
        async def _venice_request_text(self, payload):  # noqa: ARG002
            raise RuntimeError("HTTP 503: unavailable")

    ctx = DummyContext()
    runtime = RuntimeWithVeniceError(ctx, DummyLegacyBot())
    state = {"peer_id": 2000000010, "actor_id": 77, "cmid": 3, "text": "q", "context": {}}

    out = await runtime.decide(state)
    decision = AgentDecision.from_value(out.get("decision"))
    assert decision.action == "none"
    assert out.get("error")
