from __future__ import annotations

from langgraph.graph import END, START, StateGraph

from .models import AgentState
from .nodes import act_node, decide_node, observe_node, record_node


def build_agent_graph(runtime, *, checkpointer=None):
    graph = StateGraph(AgentState)

    async def _observe(state: AgentState):
        return await observe_node(state, runtime)

    async def _decide(state: AgentState):
        return await decide_node(state, runtime)

    async def _act(state: AgentState):
        return await act_node(state, runtime)

    async def _record(state: AgentState):
        return await record_node(state, runtime)

    graph.add_node("observe", _observe)
    graph.add_node("decide", _decide)
    graph.add_node("act", _act)
    graph.add_node("record", _record)

    graph.add_edge(START, "observe")
    graph.add_edge("observe", "decide")
    graph.add_edge("decide", "act")
    graph.add_edge("act", "record")
    graph.add_edge("record", END)

    if checkpointer is not None:
        return graph.compile(checkpointer=checkpointer)
    return graph.compile()
