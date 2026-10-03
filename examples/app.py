"""Smallest app after `langgraph-hitl setup` (which created ./.hitl and connected "manager").

    pip install "langgraph-external-hitl[all]"
    langgraph-hitl setup
    python app.py 1.4.2
"""
import asyncio
import contextlib
import sys
import uuid
from typing import TypedDict

from langgraph.graph import END, START, StateGraph

from langgraph_external_hitl import HITL, ApprovalOption, ConfigError, HitlError, request_approval

APPROVER = "manager"   # the approver name you chose in `langgraph-hitl setup`


class State(TypedDict, total=False):
    version: str
    decision: str


def approve(state: State) -> State:
    result = request_approval(                      # FIRST statement of the node (it pauses here)
        recipient=APPROVER,                         # a registered approver - from your code, never LLM output
        title="Deploy application",
        message=f"Deploy version {state['version']}?",
        options=[
            ApprovalOption("production", "Deploy to Production"),
            ApprovalOption("staging", "Deploy to Staging"),
            ApprovalOption("reject", "Reject"),
        ],
    )
    return {"decision": result.option_id}


def act(state: State) -> State:
    print(f"Decision: {state['decision']}")         # your business action goes here (keep it idempotent)
    return {}


def build(checkpointer):
    g = StateGraph(State)
    g.add_node("approve", approve)
    g.add_node("act", act)
    g.add_edge(START, "approve")
    g.add_edge("approve", "act")
    g.add_edge("act", END)
    return g.compile(checkpointer=checkpointer)


async def main(version: str) -> None:
    async with HITL.from_config() as hitl:          # reads ./.hitl
        graph = build(hitl.checkpointer)
        await hitl.start(graph, {"version": version}, thread_id=f"deploy-{uuid.uuid4().hex[:8]}",
                         recipient=APPROVER)
        print("Approval sent to Telegram. Waiting for decisions (Ctrl+C to stop)...")
        await hitl.run(graph)                       # receives clicks, resumes the graph (one worker)


if __name__ == "__main__":
    try:
        with contextlib.suppress(KeyboardInterrupt):
            asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "1.4.2"))
    except (HitlError, ConfigError) as e:   # setup missing, unknown approver, rejected token, ...
        sys.exit(f"Error: {e}")
