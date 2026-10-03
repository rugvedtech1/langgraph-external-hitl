"""Shared test graphs for Part 6.1 (module-level types so LangGraph can resolve hints)."""
import operator
from typing import Annotated, TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

from langgraph_external_hitl import ApprovalOption
from langgraph_external_hitl.bridge import request_approval

MANAGER = [ApprovalOption("approve", "Approve", style="success"),
           ApprovalOption("reduce", "Approve 50%"), ApprovalOption("reject", "Reject", style="danger")]
FINANCE = [ApprovalOption("release", "Release funds"), ApprovalOption("hold", "Hold")]


class Job(TypedDict, total=False):
    amount: int
    manager: str | None
    finance: str | None
    approval_ids: Annotated[list, operator.add]
    result: str


def build(saver, executed: list):
    def assess(s: Job) -> Job:
        return {}

    def manager(s: Job) -> Job:
        r = request_approval(f"Refund {s['amount']}", "Manager decision", MANAGER)
        return {"manager": r.option_id, "approval_ids": [r.approval_id]}

    def finance(s: Job) -> Job:
        r = request_approval(f"Release {s['amount']}", "Finance decision", FINANCE)
        return {"finance": r.option_id, "approval_ids": [r.approval_id]}

    def execute(s: Job) -> Job:
        executed.append((s.get("amount"), s.get("manager"), s.get("finance")))
        return {"result": "executed"}

    def stop(s: Job) -> Job:
        return {"result": "rejected"}

    def route_assess(s: Job):
        return "execute" if s["amount"] < 100 else "manager"

    def route_manager(s: Job):
        if s.get("manager") not in ("approve", "reduce"):
            return "stop"
        return "finance" if s["amount"] >= 1000 else "execute"

    def route_finance(s: Job):
        return "execute" if s.get("finance") == "release" else "stop"

    g = StateGraph(Job)
    for n, f in [("assess", assess), ("manager", manager), ("finance", finance), ("execute", execute),
                 ("stop", stop)]:
        g.add_node(n, f)
    g.add_edge(START, "assess")
    g.add_conditional_edges("assess", route_assess, ["execute", "manager"])
    g.add_conditional_edges("manager", route_manager, ["stop", "finance", "execute"])
    g.add_conditional_edges("finance", route_finance, ["execute", "stop"])
    g.add_edge("execute", END)
    g.add_edge("stop", END)
    return g.compile(checkpointer=saver)


class Foreign(TypedDict, total=False):
    answer: str


def build_foreign(saver):
    def ask(s: Foreign) -> Foreign:
        return {"answer": interrupt({"question": "free text please"})}
    g = StateGraph(Foreign)
    g.add_node("ask", ask)
    g.add_edge(START, "ask")
    g.add_edge("ask", END)
    return g.compile(checkpointer=saver)
