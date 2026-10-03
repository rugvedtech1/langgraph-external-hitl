"""LangGraph bridge tests (Part 3 behaviour), real SQLite + AsyncSqliteSaver."""
import asyncio

from langgraph.types import Command

from langgraph_external_hitl.digest import payload_digest
from langgraph_external_hitl.bridge import HitlBridge, thread_config

USER, CHAT, MSG = 111, 111, 5
NOW, TTL = 1_000_000, 300
ACTION = "DEMO: delete 3 files in /tmp/demo"


async def new_approval(b: HitlBridge, action=ACTION):
    a = await b.start(approver_user_id=USER, chat_id=CHAT, action=action, now=NOW, ttl_s=TTL)
    b.store.set_message_id(a.approval_id, MSG)
    return b.store.get(a.approval_id)


async def click(b: HitlBridge, a, decision="approve", user=USER, msg=MSG, now=NOW + 1):
    r = await b.decide(approval_id=a.approval_id, decision=decision, user_id=user,
                       chat_id=CHAT, message_id=msg, now=now)
    res = await b.resume(r.approval, decision, now) if r.outcome == "won" else None
    return r, res


def run(c):
    return asyncio.run(c)


def test_start_stores_thread_interrupt_and_digest(tmp_path, env_factory):
    async def t():
        async with env_factory(tmp_path, []) as b:
            a = await new_approval(b)
            assert a.thread_id and a.interrupt_id and a.payload_digest and a.resumed_at is None
            snap = await b.graph.aget_state(thread_config(a.thread_id))
            assert [i.id for i in snap.interrupts] == [a.interrupt_id]
            assert snap.next == ("approval",)
            assert a.payload_digest == payload_digest({"action": ACTION})
    run(t())


def test_approve_executes_exactly_once(tmp_path, env_factory):
    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            a = await new_approval(b)
            r, res = await click(b, a, "approve")
            assert r.outcome == "won" and res.status == "resumed" and res.graph_result == "executed"
            assert executed == [(ACTION, a.thread_id)]
            final = b.store.get(a.approval_id)
            assert final.selected_option_id == "approve" and final.resumed_at == NOW + 1
            snap = await b.graph.aget_state(thread_config(a.thread_id))
            assert snap.next == () and snap.interrupts == ()
            assert snap.values["approval_id"] == a.approval_id and snap.values["decided_by"] == USER
    run(t())


def test_reject_cancels_and_never_executes(tmp_path, env_factory):
    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            a = await new_approval(b)
            r, res = await click(b, a, "reject")
            assert r.outcome == "won" and res.status == "resumed" and res.graph_result == "cancelled"
            assert executed == [] and b.store.get(a.approval_id).selected_option_id == "reject"
    run(t())


def test_restart_between_interrupt_and_click(tmp_path, env_factory):
    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            a = await new_approval(b)
        async with env_factory(tmp_path, executed) as b2:
            reloaded = b2.store.get(a.approval_id)
            assert reloaded.status == "pending" and reloaded.interrupt_id == a.interrupt_id
            r, res = await click(b2, reloaded, "approve")
            assert r.outcome == "won" and res.status == "resumed" and res.graph_result == "executed"
        assert executed == [(ACTION, a.thread_id)]
    run(t())


def test_duplicate_click_does_not_resume_again(tmp_path, env_factory):
    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            a = await new_approval(b)
            r1, _ = await click(b, a, "approve")
            r2, res2 = await click(b, a, "approve", now=NOW + 2)
            r3, res3 = await click(b, a, "reject", now=NOW + 3)
            assert r1.outcome == "won"
            assert r2.outcome == r3.outcome == "already_decided" and res2 is None and res3 is None
            assert executed == [(ACTION, a.thread_id)]
    run(t())


def test_concurrent_clicks_one_resume(tmp_path, env_factory):
    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            a = await new_approval(b)
            results = await asyncio.gather(click(b, a, "approve"), click(b, a, "reject"))
            assert sorted(r.outcome for r, _ in results) == ["already_decided", "won"]
            resumed = [res for _, res in results if res is not None]
            assert len(resumed) == 1 and resumed[0].status == "resumed"
            winner = b.store.get(a.approval_id).selected_option_id
            assert len(executed) == (1 if winner == "approve" else 0)
    run(t())


def test_stale_graph_already_finished(tmp_path, env_factory):
    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            a = await new_approval(b)
            await b.graph.ainvoke(Command(resume={a.interrupt_id: {"decision": "reject"}}),
                                  thread_config(a.thread_id))
            r, res = await click(b, a, "approve")
            assert r.outcome == "stale" and res is None
            assert b.store.get(a.approval_id).status == "pending" and executed == []
    run(t())


def test_invalid_interrupt_id(tmp_path, env_factory):
    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            a = await new_approval(b)
            b.store._conn.execute("UPDATE approvals SET interrupt_id=? WHERE approval_id=?",
                                  ("0" * 32, a.approval_id))
            r, res = await click(b, b.store.get(a.approval_id), "approve")
            assert r.outcome == "stale" and res is None and executed == []
            snap = await b.graph.aget_state(thread_config(a.thread_id))
            assert snap.next == ("approval",)
    run(t())


def test_unlinked_row_is_never_resumed(tmp_path, env_factory):
    async def t():
        async with env_factory(tmp_path, []) as b:
            a = b.store.create(USER, CHAT, "legacy part-2 row", now=NOW, ttl_s=TTL)
            b.store.set_message_id(a.approval_id, MSG)
            r, res = await click(b, b.store.get(a.approval_id), "approve")
            assert r.outcome == "stale" and res is None
    run(t())


def test_payload_digest_mismatch_is_refused(tmp_path, env_factory):
    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            a = await new_approval(b)
            b.store._conn.execute("UPDATE approvals SET payload_digest=? WHERE approval_id=?",
                                  (payload_digest({"action": "something else"}), a.approval_id))
            r, res = await click(b, b.store.get(a.approval_id), "approve")
            assert r.outcome == "stale" and res is None and executed == []
            assert b.store.get(a.approval_id).status == "pending"
    run(t())


def test_cross_thread_link_is_refused(tmp_path, env_factory):
    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            a = await new_approval(b)
            other = await new_approval(b)
            assert a.interrupt_id != other.interrupt_id
            b.store._conn.execute("UPDATE approvals SET thread_id=? WHERE approval_id=?",
                                  (other.thread_id, a.approval_id))
            r, res = await click(b, b.store.get(a.approval_id), "approve")
            assert r.outcome == "stale" and res is None and executed == []
            snap = await b.graph.aget_state(thread_config(other.thread_id))
            assert snap.next == ("approval",)
    run(t())


def test_unknown_thread_is_not_started(tmp_path, env_factory):
    async def t():
        async with env_factory(tmp_path, []) as b:
            a = await new_approval(b)
            b.store._conn.execute("UPDATE approvals SET thread_id=? WHERE approval_id=?",
                                  ("no-such-thread", a.approval_id))
            r, res = await click(b, b.store.get(a.approval_id), "approve")
            assert r.outcome == "stale" and res is None
            snap = await b.graph.aget_state(thread_config("no-such-thread"))
            assert snap.values == {} and snap.next == ()
    run(t())


def test_wrong_user_or_message_does_not_touch_graph(tmp_path, env_factory):
    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            a = await new_approval(b)
            r1, _ = await click(b, a, "approve", user=999)
            r2, _ = await click(b, a, "approve", msg=MSG + 1)
            assert r1.outcome == "not_authorized" and r2.outcome == "wrong_message"
            assert b.store.get(a.approval_id).status == "pending" and executed == []
    run(t())


def test_expired_does_not_resume(tmp_path, env_factory):
    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            a = await new_approval(b)
            r, res = await click(b, a, "approve", now=NOW + TTL + 1)
            assert r.outcome == "expired" and res is None and executed == []
            snap = await b.graph.aget_state(thread_config(a.thread_id))
            assert snap.next == ("approval",)
    run(t())


class _Wrap:
    """Graph wrapper whose ainvoke misbehaves; aget_state delegates to the real graph."""

    def __init__(self, graph, mode):
        self.graph, self.mode = graph, mode

    async def ainvoke(self, *args, **kw):
        if self.mode == "raise":
            raise RuntimeError("graph down")
        return {}  # silent no-op (LangGraph issue #8836 style)

    async def aget_state(self, cfg):
        return await self.graph.aget_state(cfg)


def test_resume_failure_leaves_resumed_at_empty(tmp_path, env_factory):
    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            a = await new_approval(b)
            bad = HitlBridge(_Wrap(b.graph, "raise"), b.store)
            r = await bad.decide(approval_id=a.approval_id, decision="approve", user_id=USER,
                                 chat_id=CHAT, message_id=MSG, now=NOW + 1)
            assert r.outcome == "won"
            res = await bad.resume(r.approval, "approve", NOW + 1)
            assert res.status == "failed"
            final = b.store.get(a.approval_id)
            assert final.selected_option_id == "approve" and final.resumed_at is None and executed == []
    run(t())


def test_silent_noop_resume_is_detected(tmp_path, env_factory):
    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            a = await new_approval(b)
            noop = HitlBridge(_Wrap(b.graph, "noop"), b.store)
            r = await noop.decide(approval_id=a.approval_id, decision="approve", user_id=USER,
                                  chat_id=CHAT, message_id=MSG, now=NOW + 1)
            res = await noop.resume(r.approval, "approve", NOW + 1)
            assert res.status == "not_applied"
            assert b.store.get(a.approval_id).resumed_at is None and executed == []
    run(t())


def test_action_executes_once_across_restart_and_duplicates(tmp_path, env_factory):
    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            a = await new_approval(b)
        async with env_factory(tmp_path, executed) as b:
            await click(b, b.store.get(a.approval_id), "approve")
        async with env_factory(tmp_path, executed) as b:
            r, res = await click(b, b.store.get(a.approval_id), "approve", now=NOW + 5)
            assert r.outcome == "already_decided" and res is None
            await b.graph.ainvoke(Command(resume={a.interrupt_id: {"decision": "approve"}}),
                                  thread_config(a.thread_id))
        assert executed == [(ACTION, a.thread_id)]
    run(t())


def test_request_approval_non_dict_value_is_reject(tmp_path, env_factory):
    """Defence in depth: a malformed resume value never approves."""
    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            a = await new_approval(b)
            await b.graph.ainvoke(Command(resume={a.interrupt_id: ["approve"]}),
                                  thread_config(a.thread_id))
            snap = await b.graph.aget_state(thread_config(a.thread_id))
            assert snap.values["result"] == "cancelled" and executed == []
    run(t())
