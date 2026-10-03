"""Part 5 / T12-T14 + binding + audit: permissions, redaction, action length, binding, audit."""
import asyncio
import logging
import os
import stat
import sys

import httpx
import pytest
from langgraph.types import Command

from langgraph_external_hitl import MAX_ACTION_LENGTH, ActionTooLongError, ApprovalStore, install_redaction, redact, register_secret
from langgraph_external_hitl.digest import payload_digest
from langgraph_external_hitl._audit import audit
from langgraph_external_hitl.bridge import thread_config
from langgraph_external_hitl.telegram import TelegramApprovalBot, TelegramConfig

posix = pytest.mark.skipif(os.name != "posix", reason="POSIX permissions")
TOKEN = "555666777:AAF-another-FAKE-token-used-only-in-tests-123"
USER = 7


# ---------- T12: file permissions ----------

@posix
def test_db_created_0600(tmp_path):
    old = os.umask(0o022)
    try:
        ApprovalStore(str(tmp_path / "a.db")).close()
    finally:
        os.umask(old)
    assert stat.S_IMODE(os.stat(tmp_path / "a.db").st_mode) == 0o600


@posix
def test_warns_on_group_or_world_accessible_db(tmp_path, caplog):
    p = tmp_path / "a.db"
    ApprovalStore(str(p)).close()
    os.chmod(p, 0o644)
    with caplog.at_level(logging.WARNING, logger="langgraph_external_hitl.store"):
        ApprovalStore(str(p)).close()
    assert any("recommended 0600" in r.getMessage() for r in caplog.records)


@posix
def test_no_warning_for_private_db(tmp_path, caplog):
    p = tmp_path / "a.db"
    ApprovalStore(str(p)).close()
    with caplog.at_level(logging.WARNING, logger="langgraph_external_hitl.store"):
        ApprovalStore(str(p)).close()
    assert not caplog.records


# ---------- T13: redaction ----------

def test_redact_known_secret_and_url_pattern():
    register_secret(TOKEN)
    assert TOKEN not in redact(f"token={TOKEN}")
    assert "bot<redacted>" in redact("https://api.telegram.org/bot123456:ABCdefGHI_jkl-MNO/getMe")
    assert redact("nothing secret here 42") == "nothing secret here 42"


def test_install_redaction_covers_httpx_info_logs(tmp_path):
    import io
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    root = logging.getLogger()
    root.addHandler(handler)
    try:
        install_redaction(TOKEN, quiet_http_loggers=False)
        lg = logging.getLogger("httpx")
        lg.setLevel(logging.INFO)
        lg.info("HTTP Request: POST https://api.telegram.org/bot%s/getUpdates", TOKEN)
        logging.getLogger("some.other.lib").error("failed %s", f"bot{TOKEN}")
    finally:
        root.removeHandler(handler)
    out = buf.getvalue()
    assert TOKEN not in out and "bot<redacted>" in out


def test_install_redaction_quiets_http_loggers():
    install_redaction(TOKEN)
    assert logging.getLogger("httpx").level == logging.WARNING


def test_token_redacted_in_all_bot_log_records(tmp_path, env_factory, flaky_api_cls, caplog):
    caplog.set_level(logging.DEBUG)
    req = httpx.Request("POST", f"https://api.telegram.org/bot{TOKEN}/x")
    fail = {m: (lambda m=m: httpx.ConnectError(f"{m} failed at bot{TOKEN}", request=req))
            for m in ("answerCallbackQuery", "editMessageText")}

    async def t():
        async with env_factory(tmp_path, []) as b:
            api = flaky_api_cls(fail)
            cfg = TelegramConfig(token=TOKEN, approver_user_ids=frozenset({USER}))

            async def on_start(u, c):
                return await b.start(approver_user_id=u, chat_id=c, action="x", now=10**9, ttl_s=10**6)
            bot = TelegramApprovalBot(cfg, b, on_start=on_start, api=api)
            await bot.handle_start({"from": {"id": USER}, "chat": {"id": USER, "type": "private"}, "text": "/start"})
            aid = api.calls[-1][1]["reply_markup"]["inline_keyboard"][0][0]["callback_data"].split(":")[1]
            await bot.handle_callback({"id": "q", "from": {"id": USER}, "data": f"a:{aid}",
                                       "message": {"message_id": b.store.get(aid).message_id, "chat": {"id": USER}}})
    asyncio.run(t())
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "failed" in text and TOKEN not in text


# ---------- T14: action length ----------

def test_action_too_long_rejected_before_insert(tmp_path):
    s = ApprovalStore(str(tmp_path / "a.db"))
    with pytest.raises(ActionTooLongError):
        s.create(1, 1, "x" * (MAX_ACTION_LENGTH + 1), now=0, ttl_s=10)
    with pytest.raises(ActionTooLongError):
        s.create(1, 1, "   ", now=0, ttl_s=10)
    assert s._conn.execute("SELECT COUNT(*) FROM approvals").fetchone()[0] == 0
    s.create(1, 1, "x" * MAX_ACTION_LENGTH, now=0, ttl_s=10)   # boundary is allowed
    s.close()


def test_action_too_long_rejected_before_graph_runs(tmp_path, env_factory):
    async def t():
        async with env_factory(tmp_path, []) as b:
            calls = []
            real = b.graph.ainvoke

            async def spy(*a, **k):
                calls.append(a)
                return await real(*a, **k)
            b.graph.ainvoke = spy
            with pytest.raises(ActionTooLongError):
                await b.start(approver_user_id=1, chat_id=1, action="y" * (MAX_ACTION_LENGTH + 1),
                              now=0, ttl_s=10)
            assert calls == []
    asyncio.run(t())


def test_bot_start_with_too_long_action_replies_cleanly(tmp_path, env_factory, fake_api_cls):
    async def t():
        async with env_factory(tmp_path, []) as b:
            api = fake_api_cls()

            async def on_start(u, c):
                return await b.start(approver_user_id=u, chat_id=c, action="z" * 5000, now=0, ttl_s=10)
            bot = TelegramApprovalBot(TelegramConfig(token="1:T", approver_user_ids=frozenset({USER})),
                                      b, on_start=on_start, api=api)
            await bot.handle_start({"from": {"id": USER}, "chat": {"id": USER, "type": "private"}, "text": "/start"})
            assert "Could not create an approval request" in api.calls[-1][1]["text"]
    asyncio.run(t())


# ---------- binding inside the graph ----------

async def _paused(b):
    a = await b.start(approver_user_id=USER, chat_id=USER, action="transfer 10", now=0, ttl_s=10**9)
    return a


def test_resume_without_digest_is_rejected_in_graph(tmp_path, env_factory):
    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            a = await _paused(b)
            await b.graph.ainvoke(Command(resume={a.interrupt_id: {"decision": "approve"}}),
                                  thread_config(a.thread_id))
            snap = await b.graph.aget_state(thread_config(a.thread_id))
            assert snap.values["result"] == "cancelled" and executed == []
    asyncio.run(t())


def test_resume_with_wrong_digest_is_rejected_in_graph(tmp_path, env_factory):
    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            a = await _paused(b)
            forged = payload_digest({"action": "transfer 10000"})
            await b.graph.ainvoke(Command(resume={a.interrupt_id: {"decision": "approve",
                                                                   "payload_digest": forged}}),
                                  thread_config(a.thread_id))
            snap = await b.graph.aget_state(thread_config(a.thread_id))
            assert snap.values["result"] == "cancelled" and executed == []
    asyncio.run(t())


def test_resume_with_correct_digest_executes(tmp_path, env_factory):
    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            a = await _paused(b)
            await b.graph.ainvoke(Command(resume={a.interrupt_id: {"decision": "approve",
                                                                   "payload_digest": a.payload_digest}}),
                                  thread_config(a.thread_id))
            assert executed == [("transfer 10", a.thread_id)]
    asyncio.run(t())


# ---------- audit ----------

def test_audit_events_are_safe(tmp_path, env_factory, caplog):
    caplog.set_level(logging.INFO, logger="langgraph_external_hitl.audit")
    secret_action = "wire 1,000,000 to account 12345678"

    async def t():
        async with env_factory(tmp_path, []) as b:
            a = await b.start(approver_user_id=USER, chat_id=USER, action=secret_action, now=0, ttl_s=10**9)
            b.store.set_message_id(a.approval_id, 5)
            r = await b.decide(approval_id=a.approval_id, decision="reject", user_id=USER,
                               chat_id=USER, message_id=5, now=1)
            await b.resume(r.approval, "reject", 1)
            return a
    a = asyncio.run(t())
    lines = [r.getMessage() for r in caplog.records if r.name == "langgraph_external_hitl.audit"]
    events = [ln.split()[0] for ln in lines]
    assert events == ["approval.created", "approval.decided", "approval.resume"]
    joined = "\n".join(lines)
    assert secret_action not in joined and f"digest={a.payload_digest[:12]}" in joined
    assert a.payload_digest not in joined                       # only a prefix is logged
    assert "outcome=won" in joined and "status=resumed" in joined


def test_audit_rejects_unknown_fields():
    with pytest.raises(ValueError):
        audit("x", action="leak me")
