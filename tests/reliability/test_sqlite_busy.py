"""Part 5 / T5: SQLite lock contention must not crash handlers or change state."""
import asyncio
import sqlite3
import time
from contextlib import asynccontextmanager

from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from langgraph_external_hitl import ApprovalStore
from langgraph_external_hitl.bridge import HitlBridge
from langgraph_external_hitl.telegram import TelegramApprovalBot, TelegramConfig

USER = 7


@asynccontextmanager
async def env(tmp, graph_builder, executed):
    async with AsyncSqliteSaver.from_conn_string(str(tmp / "cp.db")) as saver:
        store = ApprovalStore(str(tmp / "approvals.db"), timeout=0.2)
        try:
            yield HitlBridge(graph_builder(saver, lambda a, t: executed.append(a)), store)
        finally:
            store.close()


def make_bot(b, api):
    async def on_start(u, c):
        return await b.start(approver_user_id=u, chat_id=c, action="pay", now=int(time.time()), ttl_s=300)
    return TelegramApprovalBot(TelegramConfig(token="1:T", approver_user_ids=frozenset({USER})),
                               b, on_start=on_start, api=api)


def test_sqlite_busy_on_click_answers_retry_and_keeps_pending(tmp_path, graph_builder, fake_api_cls):
    async def t():
        executed = []
        async with env(tmp_path, graph_builder, executed) as b:
            api = fake_api_cls()
            bot = make_bot(b, api)
            await bot.handle_start({"from": {"id": USER}, "chat": {"id": USER, "type": "private"}, "text": "/start"})
            aid = api.calls[-1][1]["reply_markup"]["inline_keyboard"][0][0]["callback_data"].split(":")[1]
            mid = b.store.get(aid).message_id
            cq = {"id": "q1", "from": {"id": USER}, "data": f"a:{aid}",
                  "message": {"message_id": mid, "chat": {"id": USER}}}

            blocker = sqlite3.connect(tmp_path / "approvals.db", isolation_level=None)
            blocker.execute("BEGIN IMMEDIATE")                  # another writer holds the lock
            await bot.handle_callback(cq)
            assert api.answers()[-1] == "Temporarily unavailable, please try again."
            blocker.execute("ROLLBACK"); blocker.close()
            assert b.store.get(aid).status == "pending" and executed == []

            await bot.handle_callback(cq | {"id": "q2"})          # user taps again
            assert api.answers()[-1] == "Recorded: Approve."
            assert b.store.get(aid).resumed_at is not None and executed == ["pay"]
    asyncio.run(t())


def test_sqlite_busy_on_start_replies_and_does_not_crash(tmp_path, graph_builder, fake_api_cls):
    async def t():
        async with env(tmp_path, graph_builder, []) as b:
            api = fake_api_cls()
            blocker = sqlite3.connect(tmp_path / "approvals.db", isolation_level=None)
            blocker.execute("BEGIN IMMEDIATE")
            await make_bot(b, api).handle_start(
                {"from": {"id": USER}, "chat": {"id": USER, "type": "private"}, "text": "/start"})
            blocker.execute("ROLLBACK"); blocker.close()
            assert "Could not create an approval request" in api.calls[-1][1]["text"]
            assert b.store._conn.execute("SELECT COUNT(*) FROM approvals").fetchone()[0] == 0
    asyncio.run(t())


def test_mark_resumed_failure_is_recoverable(tmp_path, graph_builder):
    """Graph resumed but resumed_at could not be written -> reconcile marks it later, no re-run."""
    async def t():
        executed = []
        async with env(tmp_path, graph_builder, executed) as b:
            a = await b.start(approver_user_id=USER, chat_id=USER, action="pay", now=1_000, ttl_s=300)
            b.store.set_message_id(a.approval_id, 5)
            r = await b.decide(approval_id=a.approval_id, decision="approve", user_id=USER,
                               chat_id=USER, message_id=5, now=1_001)
            real = b.store.mark_resumed

            def locked(*args):
                raise sqlite3.OperationalError("database is locked")
            b.store.mark_resumed = locked
            res = await b.resume(r.approval, "approve", 1_001)
            b.store.mark_resumed = real
            assert res.status == "resumed" and b.store.get(a.approval_id).resumed_at is None
            report = await b.reconcile(1_002)
            assert [x.approval_id for x in report.marked] == [a.approval_id] and not report.resumed
            assert executed == ["pay"]
    asyncio.run(t())
