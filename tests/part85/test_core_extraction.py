"""Part 8.5: v3 -> v4 migration, cross-channel exactly-once, core independence from Telegram."""
import asyncio
import json
import subprocess
import sys

from langgraph.checkpoint.memory import InMemorySaver

import importlib.util as _ilu
import pathlib as _pl
_spec = _ilu.spec_from_file_location("_hitl_test_graphs", _pl.Path(__file__).resolve().parents[1] / "conftest.py")
_mod = _ilu.module_from_spec(_spec); _spec.loader.exec_module(_mod)
build_test_graph = _mod.build_test_graph
from langgraph_external_hitl import (ApprovalStore, ChannelConnections, ConnectionManager, HitlService,
                                     SchemaVersionError)
from langgraph_external_hitl.bridge import HitlBridge
from langgraph_external_hitl.store import (_APPROVALS_V2, _CONNECTIONS_V2, _V3_DDL, _V3_DELIVERY_COLUMNS,
                                           PRESET_JSON)
from langgraph_external_hitl.telegram import TelegramApprovalBot, TelegramConfig
from langgraph_external_hitl.testing import FakeChannel
import pytest
import sqlite3


def make_v3(path, extra_sql=()):
    c = sqlite3.connect(path)
    c.execute(_APPROVALS_V2)
    for s in _CONNECTIONS_V2.split(";"):
        if s.strip():
            c.execute(s)
    for name, typ in _V3_DELIVERY_COLUMNS.items():
        c.execute(f"ALTER TABLE approvals ADD COLUMN {name} {typ}")
    for s in _V3_DDL.split(";"):
        if s.strip():
            c.execute(s)
    c.execute("INSERT INTO telegram_connections VALUES ('conn1','user-A',111,222,'alice','Alice','active',1,1,NULL,NULL)")
    c.execute("INSERT INTO approvals (approval_id, connection_id, recipient_external_user_id, approver_user_id, chat_id,"
              " message_id, title, options_json, status, created_at, expires_at, thread_id, interrupt_id, payload_digest,"
              " payload_version, delivery_state, delivered_at, delivery_attempts)"
              " VALUES ('a-sent','conn1','user-A',111,222,77,'Deploy?',?,'pending',10,9999999999,'t1','i1','d',2,'sent',11,1)",
              (PRESET_JSON,))
    c.execute("INSERT INTO approvals (approval_id, approver_user_id, chat_id, title, options_json, status, selected_option_id,"
              " created_at, expires_at, decided_at, payload_version) VALUES ('a-old',5,5,'old',?,'decided','approve',1,2,2,1)",
              (PRESET_JSON,))
    c.execute("INSERT INTO connection_tokens (token_hash, external_user_id, created_at, expires_at, claim_id,"
              " claimed_by_tg_id, claimed_chat_id, claimed_at) VALUES ('h1','user-B',1,9999999999,'claimX',333,333,2)")
    c.execute("INSERT INTO hitl_threads VALUES ('t1','user-A','active',10,10,NULL,NULL)")
    for s in extra_sql:
        c.execute(s)
    c.execute("PRAGMA user_version = 3")
    c.commit(); c.close()


def test_v3_to_v4_preserves_telegram_data(tmp_path):
    p = str(tmp_path / "hitl.db")
    make_v3(p)
    s = ApprovalStore(p)
    assert s.schema_version == 5
    a = s.get("a-sent")
    assert (a.channel, a.actor_ref, a.address, a.external_ref) == ("telegram", "111", {"chat_id": 222}, "222:77")
    assert (a.approver_user_id, a.chat_id, a.message_id, a.connection_id) == (111, 222, 77, "conn1")   # 0.4 views
    assert (a.delivery_state, a.delivered_at, a.delivery_attempts, a.recipient_id) == ("sent", 11, 1, "user-A")
    old = s.get("a-old")
    assert (old.status, old.selected_option_id, old.decided_delivery_id is not None) == ("decided", "approve", True)
    assert s.consume("a-sent", "approve", 111, 222, 77, 20).outcome == "won"            # still decidable
    cm = ConnectionManager(s)
    c = cm.get("user-A")
    assert (c.telegram_user_id, c.chat_id, c.username, c.first_name, c.channel) == (111, 222, "alice", "Alice", "telegram")
    assert cm.confirm("claimX", 333, 333, 3).recipient_id == "user-B"                   # in-flight token migrated
    tables = {r[0] for r in s._conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"approvals", "approval_deliveries", "channel_connections", "connection_tokens", "hitl_threads"} <= tables
    assert "telegram_connections" not in tables
    cols = {r[1] for r in s._conn.execute("PRAGMA table_info(approvals)")}
    assert not {"approver_user_id", "chat_id", "message_id"} & cols                        # channel-neutral
    s.close()
    s2 = ApprovalStore(p); assert s2.schema_version == 5; s2.close()                       # idempotent


def test_v3_to_v4_failure_rolls_back(tmp_path):
    p = str(tmp_path / "hitl.db")
    make_v3(p, extra_sql=["CREATE TABLE approval_deliveries (x INTEGER)"])               # forces a failure
    with pytest.raises(sqlite3.OperationalError):
        ApprovalStore(p)
    c = sqlite3.connect(p)
    assert c.execute("PRAGMA user_version").fetchone()[0] == 3
    assert c.execute("SELECT message_id FROM approvals WHERE approval_id='a-sent'").fetchone()[0] == 77
    assert c.execute("SELECT COUNT(*) FROM telegram_connections").fetchone()[0] == 1
    c.close()


def test_newer_schema_refused(tmp_path):
    p = str(tmp_path / "hitl.db")
    c = sqlite3.connect(p); c.execute("PRAGMA user_version = 6"); c.close()
    with pytest.raises(SchemaVersionError):
        ApprovalStore(p)


class _Api:
    def __init__(self):
        self.calls, self.mid = [], 100

    async def call(self, method, **p):
        self.calls.append((method, p))
        if method == "sendMessage":
            self.mid += 1
            return {"message_id": self.mid}
        return True


def test_same_approval_on_two_channels_decided_exactly_once(tmp_path):
    async def t():
        executed, completed = [], []
        store = ApprovalStore(str(tmp_path / "hitl.db"))
        graph = build_test_graph(InMemorySaver(), lambda a, tid: executed.append(a))
        bridge = HitlBridge(graph, store, on_completed=lambda tid, v: completed.append(tid))
        fake = FakeChannel("web")
        web = HitlService(bridge, fake, clock=lambda: 1000)
        api = _Api()
        bot = TelegramApprovalBot(TelegramConfig(token="1:T"), bridge, api=api, clock=lambda: 1000,
                                  connections=ConnectionManager(store, "bot"))
        ChannelConnections(store, "web").bind("alice", "alice@web", {}, 1000)
        bot.connections.seed("alice", 111, 111, 1000)
        await web.track("job", "alice")
        await graph.ainvoke({"action": "ship"}, {"configurable": {"thread_id": "job"}}, durability="sync")
        r1 = await web.deliver_pending("job")
        r2 = await bot.deliver_pending("job")
        assert len(r1.sent) == len(r2.sent) == 1 and r1.approvals[0].approval_id == r2.approvals[0].approval_id
        aid = r1.approvals[0].approval_id
        assert {d["channel"] for d in store.deliveries(aid)} == {"web", "telegram"}
        web_ref = next(d["external_ref"] for d in store.deliveries(aid) if d["channel"] == "web")
        web_req = fake.request(fake.sent[0], 0, actor_ref="alice@web", delivery_ref=web_ref)
        tg_update = {"update_id": 1, "callback_query": {"id": "q", "from": {"id": 111}, "data": f"v2:{aid}:1",
                                                        "message": {"message_id": api.mid, "chat": {"id": 111}}}}
        rep, _ = await asyncio.gather(web.handle(web_req), bot.handle_update(tg_update))
        toast = [p["text"] for m, p in api.calls if m == "answerCallbackQuery"][-1]
        telegram_won = toast.startswith("Recorded")
        assert (rep.outcome == "won") != telegram_won, (rep.outcome, toast)       # exactly one winner
        assert rep.outcome in ("won", "already_decided") and (telegram_won or toast.startswith("Already decided"))
        assert store.get(aid).status == "decided" and len(completed) == 1
        assert len(executed) == (1 if store.get(aid).selected_option_id == "approve" else 0)
        store.close()
    asyncio.run(t())


def test_core_imports_without_telegram_or_langgraph():
    code = ("import sys; sys.modules['httpx'] = None; sys.modules['langgraph'] = None\n"
            "import langgraph_external_hitl as h\n"
            "from langgraph_external_hitl import HitlService, ChannelConnections, FakeChannel\n" if False else
            "import sys; sys.modules['httpx'] = None; sys.modules['langgraph'] = None\n"
            "import langgraph_external_hitl as h, langgraph_external_hitl.service, langgraph_external_hitl.channels\n"
            "import langgraph_external_hitl.testing\n"
            "print('ok', 'telegram' in sys.modules.get('langgraph_external_hitl').__dict__)\n")
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    assert r.stdout.strip() == "ok False"


def test_store_claim_refuses_non_pending_approval(tmp_path):
    s = ApprovalStore(str(tmp_path / "hitl.db"))
    a = s.create(7, 7, "x", now=0, ttl_s=10 ** 6)
    s.set_message_id(a.approval_id, 5, 1)
    b = s.create(7, 7, "y", now=0, ttl_s=10 ** 6)
    assert s.consume(a.approval_id, "approve", 7, 7, 5, 2).outcome == "won"
    did = s.get(b.approval_id).delivery_id
    s.mark_undeliverable(b.approval_id, 3)
    assert not s.claim(did, 4)                                  # non-pending: never (re)claimed for sending
    c = s.create(7, 7, "z", now=0, ttl_s=10 ** 6)
    assert s.claim(s.get(c.approval_id).delivery_id, 4)         # pending: claimable
    s.close()
