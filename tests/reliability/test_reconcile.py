"""Part 5 / T7-T10: find_unresumed() classification and safe-only reconcile()."""
import asyncio
from typing import TypedDict

from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from langgraph.graph import END, START, StateGraph

from langgraph_external_hitl import ApprovalStore
from langgraph_external_hitl.bridge import HitlBridge, request_approval, thread_config

USER, CHAT, MSG = 7, 7, 5
NOW = 1_000_000


async def approve_without_resume(b, decision="approve", action="pay"):
    """A won decision whose resume never ran (e.g. crash or network failure right after consume)."""
    a = await b.start(approver_user_id=USER, chat_id=CHAT, action=action, now=NOW, ttl_s=300)
    b.store.set_message_id(a.approval_id, MSG)
    r = await b.decide(approval_id=a.approval_id, decision=decision, user_id=USER,
                       chat_id=CHAT, message_id=MSG, now=NOW + 1)
    assert r.outcome == "won"
    return b.store.get(a.approval_id)


def test_ready_to_resume_is_resumed_once(tmp_path, env_factory):
    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            a = await approve_without_resume(b)
            [u] = await b.find_unresumed()
            assert u.state == "ready_to_resume"
            report = await b.reconcile(NOW + 2)
            assert len(report.resumed) == 1 and report.resumed[0][1].graph_result == "executed"
            assert executed == [("pay", a.thread_id)] and b.store.get(a.approval_id).resumed_at == NOW + 2
            again = await b.reconcile(NOW + 3)                        # T10: idempotent
            assert not again.resumed and not again.marked and not again.needs_attention
            assert len(executed) == 1
    asyncio.run(t())


def test_ready_to_resume_rejected_is_cancelled(tmp_path, env_factory):
    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            await approve_without_resume(b, decision="reject")
            report = await b.reconcile(NOW + 2)
            assert report.resumed[0][1].graph_result == "cancelled" and executed == []
    asyncio.run(t())


def test_completed_unmarked_is_only_marked(tmp_path, env_factory):
    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            a = await approve_without_resume(b)
            await b.resume(a, "approve", NOW + 1)
            b.store._conn.execute("UPDATE approvals SET resumed_at = NULL WHERE approval_id=?", (a.approval_id,))
            [u] = await b.find_unresumed()
            assert u.state == "completed_unmarked"
            report = await b.reconcile(NOW + 2)
            assert [x.approval_id for x in report.marked] == [a.approval_id] and not report.resumed
            assert len(executed) == 1
    asyncio.run(t())


class S(TypedDict, total=False):
    action: str
    approved: bool
    approval_id: str | None
    result: str


def crashing_graph(saver, side_effects: list, crash: dict):
    def approval(s: S) -> S:
        d = request_approval(s["action"])
        return {"approved": d.approved, "approval_id": d.approval_id}

    def execute(s: S) -> S:
        side_effects.append(s["approval_id"])           # the side effect happens ...
        if crash["on"]:
            raise RuntimeError("crash after side effect")  # ... then the node fails
        return {"result": "executed"}

    g = StateGraph(S)
    g.add_node("approval", approval)
    g.add_node("execute", execute)
    g.add_edge(START, "approval")
    g.add_conditional_edges("approval", lambda s: "execute" if s.get("approved") else END, ["execute", END])
    g.add_edge("execute", END)
    return g.compile(checkpointer=saver)


def test_partial_is_reported_and_never_retried(tmp_path):
    async def t():
        side_effects, crash = [], {"on": True}
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "cp.db")) as saver:
            store = ApprovalStore(str(tmp_path / "a.db"))
            b = HitlBridge(crashing_graph(saver, side_effects, crash), store)
            a = await approve_without_resume(b)
            res = await b.resume(a, "approve", NOW + 1)
            assert res.status == "failed" and side_effects == [a.approval_id]

            crash["on"] = False                                # even if a retry WOULD succeed ...
            [u] = await b.find_unresumed()
            assert u.state == "partial" and u.graph_next == ("execute",)
            report = await b.reconcile(NOW + 2)
            assert report.partial and not report.resumed and not report.marked
            assert side_effects == [a.approval_id]            # ... it is NOT retried
            snap = await b.graph.aget_state(thread_config(a.thread_id))
            assert snap.next == ("execute",)                   # thread left untouched
            assert store.get(a.approval_id).resumed_at is None
            report2 = await b.reconcile(NOW + 3)               # still only reported
            assert report2.partial and side_effects == [a.approval_id]
            store.close()
    asyncio.run(t())


def test_digest_mismatch_and_unknown_thread_reported_legacy_ignored(tmp_path, env_factory):
    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            a1 = await approve_without_resume(b)
            b.store._conn.execute("UPDATE approvals SET payload_digest='x' WHERE approval_id=?", (a1.approval_id,))
            a2 = await approve_without_resume(b)
            b.store._conn.execute("UPDATE approvals SET thread_id='gone' WHERE approval_id=?", (a2.approval_id,))
            a3 = b.store.create(USER, CHAT, "legacy", now=NOW, ttl_s=300)
            b.store.set_message_id(a3.approval_id, 9)
            b.store.consume(a3.approval_id, "approve", USER, CHAT, 9, NOW + 1)
            states = {u.approval.approval_id: u.state for u in await b.find_unresumed()}
            assert states == {a1.approval_id: "digest_mismatch", a2.approval_id: "unknown_thread"}
            assert a3.approval_id not in states                  # legacy (no graph) row is not reported
            assert (await b.classify(b.store.get(a3.approval_id))).state == "unlinked"
            report = await b.reconcile(NOW + 2)
            assert len(report.needs_attention) == 2 and not report.resumed and executed == []
            snap = await b.graph.aget_state(thread_config("gone"))
            assert snap.values == {}                           # no run was started
    asyncio.run(t())


def test_concurrent_reconciles_resume_once(tmp_path, env_factory):
    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            await approve_without_resume(b)
            r1, r2 = await asyncio.gather(b.reconcile(NOW + 2), b.reconcile(NOW + 2))
            assert len(executed) == 1
            statuses = [res.status for _, res in r1.resumed + r2.resumed]
            assert statuses.count("resumed") >= 1
    asyncio.run(t())


def test_live_resume_and_reconcile_race_executes_once(tmp_path, env_factory):
    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            a = await approve_without_resume(b)
            await asyncio.gather(b.resume(a, "approve", NOW + 2), b.reconcile(NOW + 2))
            assert len(executed) == 1 and b.store.get(a.approval_id).resumed_at is not None
    asyncio.run(t())


def test_resume_twice_second_is_noop(tmp_path, env_factory):
    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            a = await approve_without_resume(b)
            first = await b.resume(a, "approve", NOW + 2)
            second = await b.resume(a, "approve", NOW + 3)
            assert first.status == second.status == "resumed" and len(executed) == 1
    asyncio.run(t())


def test_find_overdue_is_read_only(tmp_path, env_factory):
    async def t():
        async with env_factory(tmp_path, []) as b:
            a = await b.start(approver_user_id=USER, chat_id=CHAT, action="x", now=NOW, ttl_s=10)
            assert [x.approval_id for x in b.store.find_overdue(NOW + 11)] == [a.approval_id]
            assert b.store.find_overdue(NOW + 5) == []
            assert b.store.get(a.approval_id).status == "pending"
    asyncio.run(t())


def test_direct_resume_on_partial_thread_does_not_continue(tmp_path):
    """Calling bridge.resume() on a PARTIAL approval must not continue the thread
    (a resume Command would let LangGraph run the pending action node again)."""
    async def t():
        side_effects, crash = [], {"on": True}
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "cp.db")) as saver:
            store = ApprovalStore(str(tmp_path / "a.db"))
            b = HitlBridge(crashing_graph(saver, side_effects, crash), store)
            a = await approve_without_resume(b)
            assert (await b.resume(a, "approve", NOW + 1)).status == "failed"
            crash["on"] = False
            res = await b.resume(store.get(a.approval_id), "approve", NOW + 2)
            assert res.status == "not_applied"
            assert side_effects == [a.approval_id]
            snap = await b.graph.aget_state(thread_config(a.thread_id))
            assert snap.next == ("execute",) and store.get(a.approval_id).resumed_at is None
            store.close()
    asyncio.run(t())
