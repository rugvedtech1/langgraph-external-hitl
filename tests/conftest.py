"""Shared fixtures. Tests import the INSTALLED package (src/ is not on sys.path)."""
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal, TypedDict

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class FakeApi:
    """Records Bot API calls; no network."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self._next_message_id = 100

    async def call(self, method: str, **params: Any) -> Any:
        self.calls.append((method, params))
        if method == "sendMessage":
            self._next_message_id += 1
            return {"message_id": self._next_message_id, "chat": {"id": params["chat_id"]}}
        return True

    def methods(self) -> list[str]:
        return [m for m, _ in self.calls]

    def answers(self) -> list[str]:
        return [p["text"] for m, p in self.calls if m == "answerCallbackQuery"]

    def edits(self) -> list[str]:
        return [p["text"] for m, p in self.calls if m == "editMessageText"]


class S(TypedDict, total=False):
    """Test graph state. Module level: LangGraph resolves node type hints here."""
    action: str
    approved: bool
    approval_id: str | None
    decided_by: int | None
    result: str


def build_test_graph(checkpointer: Any, on_execute):
    """Approve/reject test graph, kept here so tests do not import examples."""
    from langgraph.graph import END, START, StateGraph

    from langgraph_external_hitl.bridge import request_approval

    def approval(state: S) -> S:
        d = request_approval(state["action"])
        return {"approved": d.approved, "approval_id": d.approval_id, "decided_by": d.approver_user_id}

    def execute(state: S, config) -> S:  # 'config' is injected by name
        on_execute(state["action"], config["configurable"]["thread_id"])
        return {"result": "executed"}

    def cancel(state: S) -> S:
        return {"result": "cancelled"}

    def route(state: S) -> Literal["execute", "cancel"]:
        return "execute" if state.get("approved") is True else "cancel"

    b = StateGraph(S)
    b.add_node("approval", approval)
    b.add_node("execute", execute)
    b.add_node("cancel", cancel)
    b.add_edge(START, "approval")
    b.add_conditional_edges("approval", route, ["execute", "cancel"])
    b.add_edge("execute", END)
    b.add_edge("cancel", END)
    return b.compile(checkpointer=checkpointer)


@asynccontextmanager
async def hitl_env(tmp: Path, executed: list):
    """Real file-based SQLite for both the approval store and the checkpointer."""
    from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

    from langgraph_external_hitl import ApprovalStore
    from langgraph_external_hitl.bridge import HitlBridge

    async with AsyncSqliteSaver.from_conn_string(str(tmp / "checkpoints.db")) as saver:
        graph = build_test_graph(saver, on_execute=lambda action, tid: executed.append((action, tid)))
        store = ApprovalStore(str(tmp / "approvals.db"))
        try:
            yield HitlBridge(graph, store)
        finally:
            store.close()


@pytest.fixture
def fake_api_cls():
    return FakeApi


@pytest.fixture
def env_factory():
    return hitl_env


@pytest.fixture
def project_root() -> Path:
    return PROJECT_ROOT


@pytest.fixture
def graph_builder():
    return build_test_graph


class FlakyApi(FakeApi):
    """FakeApi whose calls fail for selected methods with a given exception factory."""

    def __init__(self, fail: dict | None = None) -> None:
        super().__init__()
        self.fail = fail or {}

    async def call(self, method: str, **params: Any) -> Any:
        if method in self.fail:
            self.calls.append((method, params))
            raise self.fail[method]()
        return await super().call(method, **params)


@pytest.fixture
def flaky_api_cls():
    return FlakyApi
