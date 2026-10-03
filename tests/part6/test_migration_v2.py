"""Part 6: schema v1 -> v2 migration, rollback, and in-flight v1 approval compatibility."""
import asyncio
import sqlite3

import pytest

from langgraph_external_hitl import ApprovalStore, SchemaVersionError
from langgraph_external_hitl.store import SCHEMA_VERSION
from langgraph_external_hitl.telegram import TelegramApprovalBot, TelegramConfig

V1_DDL = """CREATE TABLE approvals (
    approval_id TEXT PRIMARY KEY, approver_user_id INTEGER NOT NULL, chat_id INTEGER NOT NULL,
    message_id INTEGER, action TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
    created_at INTEGER NOT NULL, expires_at INTEGER NOT NULL, decided_at INTEGER,
    thread_id TEXT, interrupt_id TEXT, payload_digest TEXT, resumed_at INTEGER)"""


def make_v1(path, rows):
    c = sqlite3.connect(path)
    c.execute(V1_DDL)
    c.executemany("INSERT INTO approvals VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    c.execute("PRAGMA user_version = 1")
    c.commit(); c.close()


def test_v1_rows_mapped(tmp_path):
    p = str(tmp_path / "a.db")
    make_v1(p, [("p", 1, 1, 5, "pending one", "pending", 0, 10**10, None, "t", "i", "d", None),
                ("ok", 1, 1, 6, "approved one", "approved", 0, 9, 3, "t2", "i2", "d2", 4),
                ("no", 1, 1, 7, "rejected one", "rejected", 0, 9, 3, None, None, None, None),
                ("ex", 1, 1, 8, "expired one", "expired", 0, 9, 9, None, None, None, None)])
    s = ApprovalStore(p)
    assert s.schema_version == SCHEMA_VERSION == 5
    got = {a: s.get(a) for a in ("p", "ok", "no", "ex")}
    assert (got["p"].status, got["p"].selected_option_id, got["p"].payload_version) == ("pending", None, 1)
    assert (got["ok"].status, got["ok"].selected_option_id, got["ok"].resumed_at) == ("decided", "approve", 4)
    assert (got["no"].status, got["no"].selected_option_id) == ("decided", "reject")
    assert got["ex"].status == "expired" and got["p"].title == "pending one"
    assert [o.id for o in got["p"].options] == ["approve", "reject"]
    assert got["p"].thread_id == "t" and got["p"].payload_digest == "d"
    tables = {r[0] for r in s._conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"approvals", "channel_connections", "connection_tokens", "approval_deliveries"} <= tables and "approvals_v1" not in tables
    s.close()
    s2 = ApprovalStore(p)                     # idempotent reopen
    assert s2.schema_version == 5 and s2.get("ok").selected_option_id == "approve"
    s2.close()


def test_failed_migration_rolls_back(tmp_path):
    p = str(tmp_path / "a.db")
    make_v1(p, [("bad", 1, 1, 5, "x", "weird-status", 0, 10, None, None, None, None, None)])
    with pytest.raises(sqlite3.IntegrityError):
        ApprovalStore(p)
    c = sqlite3.connect(p)
    assert c.execute("PRAGMA user_version").fetchone()[0] == 1
    tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert tables == {"approvals"} and c.execute("SELECT status FROM approvals").fetchone()[0] == "weird-status"
    c.close()


def test_newer_schema_refused(tmp_path):
    p = str(tmp_path / "a.db")
    c = sqlite3.connect(p); c.execute("PRAGMA user_version = 6"); c.close()
    with pytest.raises(SchemaVersionError):
        ApprovalStore(p)


def test_inflight_v1_approval_still_clickable_after_upgrade(tmp_path, env_factory, graph_builder,
                                                            fake_api_cls):
    """A v1 approval (paused graph, legacy {"action"} payload, legacy a:/r: buttons) created
    before the upgrade can still be decided and resumed after migrating to v2."""
    from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

    from langgraph_external_hitl.bridge import HitlBridge

    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            a = await b.start(approver_user_id=7, chat_id=7, action="legacy deploy", now=1000, ttl_s=10**9)
        assert a.payload_version == 1
        v1 = str(tmp_path / "v1.db")
        make_v1(v1, [(a.approval_id, 7, 7, 55, "legacy deploy", "pending", 1000, 1000 + 10**9, None,
                      a.thread_id, a.interrupt_id, a.payload_digest, None)])
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "checkpoints.db")) as saver:
            store = ApprovalStore(v1)                       # migrates v1 -> v2
            try:
                bridge = HitlBridge(graph_builder(saver, lambda act, tid: executed.append(act)), store)
                api = fake_api_cls()
                bot = TelegramApprovalBot(TelegramConfig(token="1:T"), bridge, api=api, clock=lambda: 2000)
                await bot.handle_update({"update_id": 1, "callback_query": {
                    "id": "q", "from": {"id": 7}, "data": f"a:{a.approval_id}",
                    "message": {"message_id": 55, "chat": {"id": 7}}}})
                row = store.get(a.approval_id)
                assert (row.status, row.selected_option_id) == ("decided", "approve")
                assert row.resumed_at is not None and executed == ["legacy deploy"]
                assert api.answers() == ["Recorded: Approve."]
            finally:
                store.close()
    asyncio.run(t())
