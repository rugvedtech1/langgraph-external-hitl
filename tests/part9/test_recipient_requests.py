"""Phase 4: request_approval(recipient=...) - binding, fail-closed resolution, pinning policy."""
import asyncio
from typing import TypedDict

import pytest
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command

from langgraph_external_hitl import ApprovalOption, ApprovalStore, ChannelConnections, HitlService
from langgraph_external_hitl.bridge import HitlBridge, build_payload, request_approval, thread_config
from langgraph_external_hitl.digest import payload_digest
from langgraph_external_hitl.errors import InvalidRecipientNameError, NotTrackedError
from langgraph_external_hitl.recipients import RecipientRegistry
from langgraph_external_hitl.testing import FakeChannel

OPTS = [ApprovalOption("production", "Deploy to Production"), ApprovalOption("staging", "Deploy to Staging"),
        ApprovalOption("reject", "Reject")]


class S(TypedDict, total=False):
    who: str | None
    choice: str | None
    rec: str | None


def build(saver):
    def approve(s: S) -> S:
        if s.get("who"):
            r = request_approval(recipient=s["who"], title="Deploy application", message="Deploy 1.4.2?",
                                 options=OPTS)
        else:
            r = request_approval(title="Deploy application", message="Deploy 1.4.2?", options=OPTS)
        return {"choice": r.option_id, "rec": r.recipient}
    g = StateGraph(S)
    g.add_node("approve", approve)
    g.add_edge(START, "approve")
    g.add_edge("approve", END)
    return g.compile(checkpointer=saver)


class Env:
    def __init__(self, tmp_path):
        self.store = ApprovalStore(str(tmp_path / "hitl.db"))
        self.graph = build(InMemorySaver())
        self.bridge = HitlBridge(self.graph, self.store)
        self.fake = FakeChannel()
        self.svc = HitlService(self.bridge, self.fake, clock=lambda: 1000)
        self.reg = RecipientRegistry(self.store)
        self.conns = ChannelConnections(self.store, "fake")

    def register(self, name, actor):
        r = self.reg.create(name)
        self.conns.bind(r.recipient_id, actor, {}, 1000)
        return r

    async def run(self, tid, who):
        await self.graph.ainvoke({"who": who}, thread_config(tid), durability="sync")
        return await self.svc.deliver_pending(tid)

    async def click(self, res, actor, choice=0):
        a = res.approvals[0]
        ref = next(d["external_ref"] for d in self.store.deliveries(a.approval_id) if d["channel"] == "fake")
        return await self.svc.handle(self.fake.request(a, choice, actor_ref=actor, delivery_ref=ref))


def run(c):
    return asyncio.run(c)


def test_payload_versions_and_digest_covers_recipient():
    assert build_payload("t") == {"action": "t"}                                   # 0.2 form unchanged
    v2 = build_payload("t", "m", OPTS)
    v3 = build_payload("t", "m", OPTS, recipient="Manager")
    assert v2["v"] == 2 and "recipient" not in v2
    assert v3["v"] == 3 and v3["recipient"] == "manager"
    assert payload_digest(v3) != payload_digest(build_payload("t", "m", OPTS, recipient="cfo"))
    with pytest.raises(InvalidRecipientNameError):
        build_payload("t", "m", OPTS, recipient="Bad Name")


def test_with_recipient_untracked_thread_is_pinned_and_delivered(tmp_path):
    async def t():
        e = Env(tmp_path)
        m = e.register("manager", "alice")
        res = await e.run("job-1", "manager")
        assert res.state == "pending" and len(e.fake.sent) == 1
        assert e.store.get_thread("job-1").recipient_external_user_id == m.recipient_id
        a = e.store.get(res.approvals[0].approval_id)
        assert (a.recipient_name, a.payload_version, a.recipient_id) == ("manager", 3, m.recipient_id)
        rep = await e.click(res, "alice", 1)
        assert rep.outcome == "won"
        st = (await e.graph.aget_state(thread_config("job-1"))).values
        assert (st["choice"], st["rec"]) == ("staging", "manager")
        e.store.close()
    run(t())


def test_without_recipient_unchanged_behavior(tmp_path):
    async def t():
        e = Env(tmp_path)
        e.conns.bind("user-raw", "bob", {}, 1000)
        await e.svc.track("job-2", "user-raw")                                   # 0.4/0.5 style
        res = await e.run("job-2", None)
        assert res.state == "pending" and e.store.get(res.approvals[0].approval_id).payload_version == 2
        assert (await e.click(res, "bob", 0)).outcome == "won"
        with pytest.raises(NotTrackedError):                                    # untracked + no literal
            await e.run("job-untracked", None)
        e.store.close()
    run(t())


def test_unknown_recipient_fails_closed_no_fallback(tmp_path):
    async def t():
        e = Env(tmp_path)
        e.register("manager", "alice")                                           # a connected recipient exists
        res = await e.run("job-3", "ghost")
        assert res.state == "unknown_recipient" and not res.approvals and e.fake.sent == []
        assert e.store.get_thread("job-3") is None                               # nobody pinned
        e.store.close()
    run(t())


def test_pinned_thread_cannot_be_redirected(tmp_path):
    async def t():
        e = Env(tmp_path)
        m = e.register("manager", "alice")
        e.register("cfo", "carol")
        await e.svc.track("job-4", m.recipient_id)                               # trusted app pinned manager
        res = await e.run("job-4", "cfo")                                       # graph asks for cfo
        assert res.state == "recipient_mismatch" and e.fake.sent == []
        e.store.close()
    run(t())


def test_digest_binding_rejects_changed_recipient(tmp_path):
    async def t():
        e = Env(tmp_path)
        e.register("manager", "alice")
        res = await e.run("job-5", "manager")
        a = res.approvals[0]
        forged = payload_digest(build_payload("Deploy application", "Deploy 1.4.2?", OPTS, recipient="cfo"))
        await e.graph.ainvoke(Command(resume={a.interrupt_id: {"option_id": "production", "payload_digest": forged}}),
                              thread_config("job-5"))
        st = (await e.graph.aget_state(thread_config("job-5"))).values
        assert st["choice"] is None                                             # binding failed -> no option
        e.store.close()
    run(t())


def test_wrong_actor_and_duplicate(tmp_path):
    async def t():
        e = Env(tmp_path)
        e.register("manager", "alice")
        res = await e.run("job-6", "manager")
        assert (await e.click(res, "mallory", 0)).outcome == "not_authorized"   # name is not authorization
        assert (await e.click(res, "alice", 0)).outcome == "won"
        assert (await e.click(res, "alice", 2)).outcome == "already_decided"
        e.store.close()
    run(t())


def test_invalid_name_in_node_raises(tmp_path):
    async def t():
        e = Env(tmp_path)
        with pytest.raises(InvalidRecipientNameError):
            await e.graph.ainvoke({"who": "Not Valid"}, thread_config("job-7"))
        e.store.close()
    run(t())
