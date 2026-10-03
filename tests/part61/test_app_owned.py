"""Part 6.1: application-owned invocation, deliver_pending, sequential approvals, completion."""
import asyncio
import sqlite3
import sys
import threading
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

sys.path.insert(0, str(Path(__file__).parent))
from graphs61 import build, build_foreign  # noqa: E402

from langgraph_external_hitl import (ApprovalStore, ConnectionManager, NotTrackedError,  # noqa: E402
                                     RecipientMismatchError)
from langgraph_external_hitl.bridge import HitlBridge, thread_config  # noqa: E402
from langgraph_external_hitl.telegram import TelegramApprovalBot, TelegramConfig, TelegramError  # noqa: E402

NOW = 1_000_000
TG = 111


class Api:
    def __init__(self):
        self.calls, self.mid, self.fail = [], 500, {}

    async def call(self, method, **p):
        self.calls.append((method, p))
        if method in self.fail:
            raise self.fail.pop(method)()
        if method == "sendMessage":
            self.mid += 1
            return {"message_id": self.mid}
        return True

    def sends(self):
        return [p for m, p in self.calls if m == "sendMessage"]

    def answers(self):
        return [p["text"] for m, p in self.calls if m == "answerCallbackQuery"]


@asynccontextmanager
async def env(tmp_path, *, executed=None, completed=None, graph="job", seed=True, clock=None):
    executed = [] if executed is None else executed
    async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "cp.db")) as saver:
        store = ApprovalStore(str(tmp_path / "a.db"))
        g = build(saver, executed) if graph == "job" else build_foreign(saver)
        clock = clock or {"t": NOW}

        def on_completed(tid, values):
            if completed is not None:
                completed.append((tid, values.get("result")))
        bridge = HitlBridge(g, store, on_completed=on_completed)
        cm = ConnectionManager(store, "my_bot")
        if seed and cm.get("user-A") is None:
            cm.seed("user-A", TG, TG, NOW)
        api = Api()
        bot = TelegramApprovalBot(TelegramConfig(token="1:T"), bridge, api=api, clock=lambda: clock["t"],
                                  connections=cm)
        try:
            yield bot, api, clock, executed
        finally:
            store.close()


async def app_event(bot, job_id, amount, recipient="user-A"):
    """What a real application does: track -> invoke its own graph -> deliver_pending."""
    await bot.track(job_id, recipient)
    await bot.bridge.graph.ainvoke({"amount": amount}, {"configurable": {"thread_id": job_id}},
                                   durability="sync")
    return await bot.deliver_pending(job_id)


def click(bot, user, approval_id, index, mid, cq="q"):
    return bot.handle_update({"update_id": 1, "callback_query": {
        "id": cq, "from": {"id": user}, "data": f"v2:{approval_id}:{index}",
        "message": {"message_id": mid, "chat": {"id": user}}}})


def run(c):
    return asyncio.run(c)


def test_t1_app_owned_thread_one_message_and_resume_same_thread(tmp_path):
    async def t():
        completed = []
        async with env(tmp_path, completed=completed) as (bot, api, clock, executed):
            r = await app_event(bot, "job-42", 500)
            assert r.state == "pending" and len(r.sent) == 1 and len(api.sends()) == 1
            a = r.approvals[0]
            assert a.thread_id == "job-42" and a.message_id == api.mid and a.delivered_at == NOW
            assert a.delivery_state == "sent" and a.delivery_attempts == 1
            await click(bot, TG, a.approval_id, 0, a.message_id)
            snap = await bot.bridge.graph.aget_state(thread_config("job-42"))
            assert snap.values["result"] == "executed" and snap.values["manager"] == "approve"
            assert executed == [(500, "approve", None)] and completed == [("job-42", "executed")]
            assert bot.bridge.store.get_thread("job-42").status == "completed"
    run(t())


def test_t2_no_approval_needed_completes_without_message(tmp_path):
    async def t():
        completed = []
        async with env(tmp_path, completed=completed) as (bot, api, clock, executed):
            r = await app_event(bot, "job-small", 10)
            assert r.state == "completed" and not r.sent and api.sends() == []
            assert completed == [("job-small", "executed")]
            r2 = await bot.deliver_pending("job-small")              # idempotent completion
            assert r2.state == "completed" and completed == [("job-small", "executed")]
    run(t())


def test_t3_deliver_pending_twice_sends_once(tmp_path):
    async def t():
        async with env(tmp_path) as (bot, api, clock, executed):
            await app_event(bot, "job-1", 500)
            r2 = await bot.deliver_pending("job-1")
            r3 = await bot.deliver_pending("job-1", "user-A")
            assert len(api.sends()) == 1 and not r2.sent and not r3.sent
            assert ("already_delivered" in {x[1] for x in r2.skipped})
            assert bot.bridge.store._conn.execute("SELECT COUNT(*) FROM approvals").fetchone()[0] == 1
    run(t())


def test_t4_concurrent_deliver_pending_sends_once(tmp_path):
    async def t():
        async with env(tmp_path) as (bot, api, clock, executed):
            await bot.track("job-c", "user-A")
            await bot.bridge.graph.ainvoke({"amount": 500}, {"configurable": {"thread_id": "job-c"}},
                                           durability="sync")
            results = await asyncio.gather(*[bot.deliver_pending("job-c") for _ in range(5)])
            assert len(api.sends()) == 1 and sum(len(r.sent) for r in results) == 1
    run(t())


def test_t4b_claim_is_atomic_across_connections(tmp_path):
    path = str(tmp_path / "a.db")
    s = ApprovalStore(path)
    a = s.reserve(thread_id="t", interrupt_id="i", approver_user_id=1, chat_id=1, title="x", message=None,
                  options=None, payload_digest="d", payload_version=1, connection_id=None,
                  recipient_external_user_id=None, now=NOW, ttl_s=60)
    wins, barrier = [], threading.Barrier(8)

    def go():
        st = ApprovalStore(path)
        try:
            barrier.wait()
            wins.append(st.claim_delivery(a.approval_id, NOW + 1))
        finally:
            st.close()
    ts = [threading.Thread(target=go) for _ in range(8)]
    [x.start() for x in ts]
    [x.join() for x in ts]
    assert wins.count(True) == 1
    again = s.reserve(thread_id="t", interrupt_id="i", approver_user_id=2, chat_id=2, title="y", message=None,
                      options=None, payload_digest="e", payload_version=1, connection_id=None,
                      recipient_external_user_id=None, now=NOW, ttl_s=60)
    assert again.approval_id == a.approval_id and again.title == "x"          # reserve is idempotent
    with pytest.raises(sqlite3.IntegrityError):                                # DB-level uniqueness
        s._conn.execute("INSERT INTO approvals (approval_id, title, options_json, status, created_at, expires_at,"
                        " thread_id, interrupt_id) VALUES ('z', 't', '[]', 'pending', 0, 1, 't', 'i')")
    assert not s.claim_delivery(a.approval_id, NOW + 30)                       # lease held
    assert s.claim_delivery(a.approval_id, NOW + 62)                           # lease expired
    s.close()


def test_t5_two_sequential_approvals_auto_delivered(tmp_path):
    async def t():
        completed = []
        async with env(tmp_path, completed=completed) as (bot, api, clock, executed):
            r = await app_event(bot, "job-big", 5000)
            a = r.approvals[0]
            assert a.title == "Refund 5000" and len(api.sends()) == 1
            await click(bot, TG, a.approval_id, 0, a.message_id)          # manager approves
            assert len(api.sends()) == 2                                    # B sent automatically
            b = bot.bridge.store.get_by_interrupt(
                "job-big", (await bot.bridge.graph.aget_state(thread_config("job-big"))).interrupts[0].id)
            assert b.title == "Release 5000" and b.message_id == api.mid and b.delivered_at is not None
            assert completed == [] and executed == []
            await click(bot, TG, b.approval_id, 0, b.message_id, cq="q2")   # finance releases
            assert executed == [(5000, "approve", "release")] and completed == [("job-big", "executed")]
            assert len(api.sends()) == 2
            snap = await bot.bridge.graph.aget_state(thread_config("job-big"))
            assert snap.next == () and len(snap.values["approval_ids"]) == 2
    run(t())


def test_t8_restart_after_delivery_no_duplicate_then_click(tmp_path):
    async def t():
        async with env(tmp_path) as (bot, api, clock, executed):
            r = await app_event(bot, "job-r", 500)
            a = r.approvals[0]
        async with env(tmp_path) as (bot2, api2, clock2, executed2):
            rep = await bot2.recover()
            assert api2.sends() == [] and rep.deliveries[0].state == "pending"
            await click(bot2, TG, a.approval_id, 2, a.message_id)          # reject
            assert bot2.bridge.store.get(a.approval_id).selected_option_id == "reject"
            assert (await bot2.bridge.graph.aget_state(thread_config("job-r"))).values["result"] == "rejected"
    run(t())


def test_t10_recipient_pinning(tmp_path):
    async def t():
        async with env(tmp_path) as (bot, api, clock, executed):
            await bot.track("job-p", "user-A")
            await bot.track("job-p", "user-A")                              # same recipient: ok
            with pytest.raises(RecipientMismatchError):
                await bot.track("job-p", "user-B")
            with pytest.raises(RecipientMismatchError):
                await bot.deliver_pending("job-p", "user-B")
            with pytest.raises(NotTrackedError):
                await bot.deliver_pending("never-tracked")
            for bad in ["", "has space", "x" * 129, "slash/no"]:
                with pytest.raises(ValueError):
                    await bot.track(bad, "user-A")
            with pytest.raises(ValueError):
                await bot.track("job-q", "")
    run(t())


def test_t11_not_connected_and_blocked_fail_closed_then_retry(tmp_path):
    async def t():
        async with env(tmp_path, seed=False) as (bot, api, clock, executed):
            r = await app_event(bot, "job-n", 500)
            assert r.state == "not_connected" and not r.approvals and api.sends() == []
            assert bot.bridge.store._conn.execute("SELECT COUNT(*) FROM approvals").fetchone()[0] == 0
            bot.connections.seed("user-A", TG, TG, NOW)
            bot.connections.mark_blocked(TG, NOW)
            r = await bot.deliver_pending("job-n")
            assert r.state == "blocked" and api.sends() == []
            bot.connections.mark_unblocked(TG, NOW)
            r = await bot.deliver_pending("job-n")
            assert r.state == "pending" and len(r.sent) == 1
            # 403 on send -> blocked + undeliverable, no retry storm
            await bot.track("job-403", "user-A")
            await bot.bridge.graph.ainvoke({"amount": 600}, {"configurable": {"thread_id": "job-403"}},
                                           durability="sync")
            api.fail["sendMessage"] = lambda: TelegramError("sendMessage", 403, "Forbidden: bot was blocked by the user")
            r = await bot.deliver_pending("job-403")
            assert r.state == "blocked" and bot.connections.get("user-A").status == "blocked"
            assert r.approvals[0].status == "undeliverable"
    run(t())


def test_transient_send_failure_is_retried(tmp_path):
    async def t():
        async with env(tmp_path) as (bot, api, clock, executed):
            await bot.track("job-t", "user-A")
            await bot.bridge.graph.ainvoke({"amount": 500}, {"configurable": {"thread_id": "job-t"}},
                                           durability="sync")
            api.fail["sendMessage"] = lambda: TelegramError("sendMessage", 500, "Internal Server Error")
            r = await bot.deliver_pending("job-t")
            assert r.failed and not r.sent and r.approvals[0].delivery_state == "failed"
            r = await bot.deliver_pending("job-t")                           # claim released: retry
            assert len(r.sent) == 1 and r.approvals[0].delivery_attempts == 2
    run(t())


def test_t12_foreign_interrupt_ignored(tmp_path):
    async def t():
        async with env(tmp_path, graph="foreign") as (bot, api, clock, executed):
            await bot.track("job-f", "user-A")
            await bot.bridge.graph.ainvoke({}, {"configurable": {"thread_id": "job-f"}}, durability="sync")
            r = await bot.deliver_pending("job-f")
            assert r.state == "foreign_interrupt" and api.sends() == [] and not r.approvals
            rep = await bot.recover()
            assert rep.reconcile.resumed == [] and api.sends() == []
            snap = await bot.bridge.graph.aget_state(thread_config("job-f"))
            assert snap.next == ("ask",)                                     # never resumed
    run(t())


def test_t13_stale_approval_a_cannot_resume_b(tmp_path):
    async def t():
        async with env(tmp_path) as (bot, api, clock, executed):
            r = await app_event(bot, "job-s", 5000)
            a = r.approvals[0]
            await click(bot, TG, a.approval_id, 0, a.message_id)
            await click(bot, TG, a.approval_id, 0, a.message_id, cq="replay")  # stale A replay
            await click(bot, TG, a.approval_id, 2, a.message_id, cq="replay2")
            assert api.answers()[-2:] == ["Already decided: Approve.", "Already decided: Approve."]
            snap = await bot.bridge.graph.aget_state(thread_config("job-s"))
            assert snap.next == ("finance",) and executed == []              # B still pending
    run(t())


def test_t15_bot_request_approval_returns_none_without_approval(tmp_path):
    async def t():
        completed = []
        async with env(tmp_path, completed=completed) as (bot, api, clock, executed):
            assert await bot.request_approval("user-A", graph_input={"amount": 5}) is None
            assert api.sends() == [] and executed == [(5, None, None)] and len(completed) == 1
            a = await bot.request_approval("user-A", graph_input={"amount": 500}, thread_id="my-job")
            assert a.thread_id == "my-job" and a.message_id == api.mid
    run(t())


def test_t16_astream_driven_application(tmp_path):
    async def t():
        async with env(tmp_path) as (bot, api, clock, executed):
            await bot.track("job-stream", "user-A")
            events = [e async for e in bot.bridge.graph.astream(
                {"amount": 500}, {"configurable": {"thread_id": "job-stream"}}, stream_mode="updates",
                durability="sync")]
            assert "__interrupt__" in events[-1]
            r = await bot.deliver_pending("job-stream")
            assert len(r.sent) == 1
            await click(bot, TG, r.approvals[0].approval_id, 1, r.approvals[0].message_id)
            assert executed == [(500, "reduce", None)]
    run(t())


def test_on_resumed_and_failing_hooks_never_break_flow(tmp_path):
    async def t():
        calls = []
        async with env(tmp_path) as (bot, api, clock, executed):
            def boom_completed(tid, values):
                calls.append(("completed", tid))
                raise RuntimeError("app bug")
            bot.bridge.on_completed = boom_completed
            bot.bridge.on_resumed = lambda a, res: calls.append(("resumed", res.status))
            r = await app_event(bot, "job-h", 500)
            await click(bot, TG, r.approvals[0].approval_id, 0, r.approvals[0].message_id)
            assert executed == [(500, "approve", None)]
            assert calls == [("resumed", "resumed"), ("completed", "job-h")]
            t_ = bot.bridge.store.get_thread("job-h")
            assert t_.status == "completed" and t_.completion_notified_at is None   # will be retried
            bot.bridge.on_completed = lambda tid, v: calls.append(("completed-ok", tid))
            rep = await bot.recover()
            assert rep.completions_notified == ["job-h"] and calls[-1] == ("completed-ok", "job-h")
            assert (await bot.recover()).completions_notified == []                  # recorded now
    run(t())


def test_ghost_message_after_send_crash_cannot_decide(tmp_path):
    """T7 in-process: send succeeded but recording it failed; re-send after lease; only the
    recorded message can decide."""
    async def t():
        async with env(tmp_path) as (bot, api, clock, executed):
            await bot.track("job-g", "user-A")
            await bot.bridge.graph.ainvoke({"amount": 500}, {"configurable": {"thread_id": "job-g"}},
                                           durability="sync")
            real = bot.bridge.store.mark_sent

            def crash(*a, **k):
                raise sqlite3.OperationalError("disk I/O error")
            bot.bridge.store.mark_sent = crash
            with pytest.raises(sqlite3.OperationalError):
                await bot.deliver_pending("job-g")
            bot.bridge.store.mark_sent = real
            ghost = api.mid
            assert (await bot.deliver_pending("job-g")).sent == []           # lease still held
            clock["t"] = NOW + 61
            r = await bot.deliver_pending("job-g")
            assert len(r.sent) == 1 and api.mid == ghost + 1
            aid = r.approvals[0].approval_id
            await click(bot, TG, aid, 0, ghost)
            assert api.answers()[-1] == "This button does not belong to this request."
            await click(bot, TG, aid, 0, api.mid, cq="q2")
            assert executed == [(500, "approve", None)]
    run(t())
