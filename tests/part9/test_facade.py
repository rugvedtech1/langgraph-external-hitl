"""Phase 5: HITL facade end-to-end with a fake Telegram API (no live credentials)."""
import asyncio
from typing import TypedDict

import pytest
from langgraph.graph import END, START, StateGraph

from langgraph_external_hitl import HITL, ApprovalOption, ConfigError, ConnectionManager, UnknownRecipientError
from langgraph_external_hitl import request_approval as lazy_request_approval
from langgraph_external_hitl.config import HitlConfig, save_config
from langgraph_external_hitl.telegram import InvalidBotTokenError, TelegramError

TOKEN = "123456789:AAH-facade-test-secret-token-abcdefgh"
OPTS = [ApprovalOption("production", "Deploy to Production"), ApprovalOption("reject", "Reject")]
DONE: list = []


class S(TypedDict, total=False):
    version: str
    decision: str | None


def approve(s: S) -> S:
    r = lazy_request_approval(recipient="manager", title="Deploy application",
                              message=f"Deploy version {s['version']}?", options=OPTS)
    return {"decision": r.option_id}


def deploy(s: S) -> S:
    DONE.append(s["decision"])
    return {}


def build(cp):
    g = StateGraph(S)
    g.add_node("approve", approve)
    g.add_node("deploy", deploy)
    g.add_edge(START, "approve"); g.add_edge("approve", "deploy"); g.add_edge("deploy", END)
    return g.compile(checkpointer=cp)


class Api:
    def __init__(self, unauthorized=False):
        self.calls, self.mid, self.unauthorized = [], 100, unauthorized

    async def call(self, method, **p):
        self.calls.append((method, p))
        if self.unauthorized:
            raise TelegramError(method, 401, "Unauthorized")
        if method == "getMe":
            return {"id": 9, "is_bot": True, "username": "my_bot", "first_name": "My Bot"}
        if method == "sendMessage":
            self.mid += 1
            return {"message_id": self.mid}
        if method == "getWebhookInfo":
            return {"url": ""}
        if method == "getUpdates":
            raise asyncio.CancelledError()
        return True

    def toasts(self):
        return [p["text"] for m, p in self.calls if m == "answerCallbackQuery"]


def click(hitl, aid, user, mid, idx=0):
    return hitl.bot.handle_update({"update_id": 1, "callback_query": {
        "id": "q", "from": {"id": user}, "data": f"v2:{aid}:{idx}", "message": {"message_id": mid, "chat": {"id": 555}}}})


def make(tmp_path, api):
    return HITL(telegram_bot_token=TOKEN, db_path=tmp_path / "hitl.db", checkpoint_path=tmp_path / "cp.db",
                api=api, clock=lambda: 1000)


def seed_manager(hitl):
    r = hitl.recipients.create("manager")
    ConnectionManager(hitl.store).seed(r.recipient_id, 555, 555, 1000)
    return r


def test_start_approve_wrong_user_duplicate(tmp_path):
    DONE.clear()

    async def t():
        api = Api()
        async with make(tmp_path, api) as hitl:
            seed_manager(hitl)
            g = build(hitl.checkpointer)
            res = await hitl.start(g, {"version": "1.4.2"}, thread_id="deploy-1", recipient="manager")
            assert res.state == "pending" and len(res.sent) == 1
            send = [p for m, p in api.calls if m == "sendMessage"][0]
            assert send["chat_id"] == 555 and "Deploy version 1.4.2?" in send["text"]
            aid = res.approvals[0].approval_id
            await click(hitl, aid, 666, api.mid)                                 # wrong Telegram user
            assert api.toasts()[-1].startswith("You are not authorized") and DONE == []
            await click(hitl, aid, 555, api.mid, 0)                              # the connected user
            assert api.toasts()[-1].startswith("Recorded") and DONE == ["production"]
            await click(hitl, aid, 555, api.mid, 1)                              # duplicate
            assert api.toasts()[-1].startswith("Already decided") and DONE == ["production"]
    asyncio.run(t())


def test_restart_recovery_then_decide(tmp_path):
    DONE.clear()

    async def t():
        api = Api()
        async with make(tmp_path, api) as hitl:
            seed_manager(hitl)
            g = build(hitl.checkpointer)
            res = await hitl.start(g, {"version": "2"}, thread_id="deploy-2", recipient="manager")
            aid, mid = res.approvals[0].approval_id, api.mid
        api2 = Api()                                                             # "restart"
        async with make(tmp_path, api2) as hitl2:
            g2 = build(hitl2.checkpointer)
            with pytest.raises(asyncio.CancelledError):
                await hitl2.run(g2)                                              # getMe + recover + poll
            assert not [c for c in api2.calls if c[0] == "sendMessage"]          # no duplicate delivery
            hitl2.bot = None
            hitl2.bind(g2)
            await click(hitl2, aid, 555, mid, 1)
            assert DONE == ["reject"]
    asyncio.run(t())


def test_literal_recipient_without_start_recipient(tmp_path):
    async def t():
        api = Api()
        async with make(tmp_path, api) as hitl:
            r = seed_manager(hitl)
            res = await hitl.start(build(hitl.checkpointer), {"version": "3"}, thread_id="deploy-3")
            assert res.state == "pending" and hitl.store.get_thread("deploy-3").recipient_external_user_id == r.recipient_id
    asyncio.run(t())


def test_unknown_recipient_and_unbound(tmp_path):
    async def t():
        async with make(tmp_path, Api()) as hitl:
            with pytest.raises(UnknownRecipientError):
                await hitl.start(build(hitl.checkpointer), {"version": "1"}, thread_id="x", recipient="ghost")
            unbound = HITL(telegram_bot_token=TOKEN, db_path=tmp_path / "b.db")
            try:
                with pytest.raises(ValueError):
                    await unbound.deliver_pending("x")
            finally:
                await unbound.aclose()
    asyncio.run(t())


def test_run_with_revoked_token_is_friendly(tmp_path):
    async def t():
        async with make(tmp_path, Api(unauthorized=True)) as hitl:
            with pytest.raises(InvalidBotTokenError, match="BotFather"):
                await hitl.run(build(hitl.checkpointer))
    asyncio.run(t())


def test_from_config_and_from_env(tmp_path, monkeypatch):
    home = tmp_path / ".hitl"
    save_config(HitlConfig(home=home, bot_username="my_bot", setup_state="awaiting_connection",
                           telegram_bot_token=TOKEN))
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    with pytest.raises(ConfigError, match="not complete"):
        HITL.from_config(home)
    save_config(HitlConfig(home=home, bot_username="my_bot", setup_state="complete", telegram_bot_token=TOKEN))
    h = HITL.from_config(home)
    assert h.bot_username == "my_bot" and h.checkpoint_path == home / "checkpoints.db" and TOKEN not in repr(h)
    h.store.close()
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", TOKEN)
    monkeypatch.setenv("HITL_DB_PATH", str(tmp_path / "env" / ".hitl" / "hitl.db"))
    h2 = HITL.from_env()
    assert (tmp_path / "env" / ".hitl" / ".gitignore").exists() and h2.checkpoint_path is None
    h2.store.close()
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN")
    with pytest.raises(ConfigError, match="langgraph-hitl setup"):
        HITL.from_env()
