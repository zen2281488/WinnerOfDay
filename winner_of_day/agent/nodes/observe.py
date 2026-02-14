from __future__ import annotations

from winner_of_day.agent.models import AgentState


async def observe_node(state: AgentState, runtime) -> AgentState:
    return await runtime.observe(state)
