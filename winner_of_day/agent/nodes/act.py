from __future__ import annotations

from winner_of_day.agent.models import AgentState


async def act_node(state: AgentState, runtime) -> AgentState:
    return await runtime.act(state)
