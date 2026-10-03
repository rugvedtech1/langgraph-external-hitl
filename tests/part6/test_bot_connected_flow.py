"""Part 6: Telegram bot with one-time connection, N options, multi-user isolation (fake API)."""
import asyncio
import logging
import operator
from contextlib import asynccontextmanager
from typing import Annotated, TypedDict

import pytest
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command

from langgraph_external_hitl import ApprovalOption, ApprovalStore, ConnectionManager, NotConnectedError, RecipientUnavailableError
from langgraph_external_hitl.digest import payload_digest
from langgraph_external_hitl.bridge import HitlBridge, request_approval, thread_config
from langgraph_external_hitl.telegram import TelegramApprovalBot, TelegramConfig, TelegramError

NOW = 1_000_000
A, B = 111, 222
OPTS3 = [ApprovalOption("production", "Deploy to Production", description="Live", style="danger"),
         ApprovalOption("staging", "Deploy to Staging"), ApprovalOption("cancel", "Cancel")]
OPTS10 = [ApprovalOption(f"o{i}", f"Choice {i}") for i in range(10)]


class S(TypedDict, total=False):
    title: str
    n: int
    chosen: Annotated[list, operator.add]


def option_graph(saver):
    def approval(s: S) -> S:
        r = request_approval(s["title"], "Pick one", OPTS10 if s.get("n") == 10 else OPTS3)
        return {"chosen": [r.option_id]}
    g = StateGraph(S)
    g.add_node("approval", approval)
    g.add_edge(START, "approval")
    g.add_edge("approval", END)
    return g.compile(checkpointer=saver)


class Api:
    def __init__(self):
        self.calls, self.mid, self.fail = [], 500, {}

    async def call(self, method, **p):
        self.calls.append((method, p))
        if method in self.fail:
            raise self.fail[method]()
        if method == "sendMessage":
            self.mid += 1
            return {"message_id": self.mid}
        if method == "getMe":
            return {"id": 1, "username": "my_bot"}
        return True

    def sends(self, chat=None):
        return [p for m, p in self.calls if m == "sendMessage" and (chat is None or p["chat_id"] == chat)]

    def answers(self):
        return [p["text"] for m, p in self.calls if m == "answerCallbackQuery"]


@asynccontextmanager
async def env(tmp_path):
    async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "cp.db")) as saver:
        store = ApprovalStore(str(tmp_path / "a.db"))
        api = Api()
        clock = {"t": NOW}
        bridge = HitlBridge(option_graph(saver), store)
        bot = TelegramApprovalBot(TelegramConfig(token="1:T", bot_username="my_bot"), bridge, api=api,
                                  clock=lambda: clock["t"], connections=ConnectionManager(store, "my_bot"),
                                  app_name="Acme <Ops>")
        try:
            yield bot, api, clock
        finally:
            store.close()


def msg(user, text, chat=None):
    return {"update_id": 1, "message": {"from": {"id": user, "first_name": f"U{user}", "username": f"u{user}"},
                                        "chat": {"id": chat or user, "type": "private"}, "text": text}}


def cb(user, data, mid, chat=None, cq="q"):
    return {"update_id": 2, "callback_query": {"id": cq, "from": {"id": user}, "data": data,
                                               "message": {"message_id": mid, "chat": {"id": chat or user}}}}


async def connect(bot, api, ext, user):
    link = bot.connections.create_link(ext, NOW)
    await bot.handle_update(msg(user, f"/start {link.token}"))
    confirm = api.sends(user)[-1]
    data = confirm["reply_markup"]["inline_keyboard"][0][0]["callback_data"]
    assert data.startswith("c2:") and "Acme &lt;Ops&gt;" in confirm["text"]
    await bot.handle_update(cb(user, data, 1))
    return bot.connections.get(ext)


async def request(bot, api, ext, n=3, title="Deploy"):
    a = await bot.request_approval(ext, graph_input={"title": title, "n": n})
    return a, api.sends()[-1]


def run(coro):
    return asyncio.run(coro)


def test_connect_once_then_many_approvals_without_start(tmp_path):
    async def t():
        async with env(tmp_path) as (bot, api, clock):
            hooks = []
            bot.on_connected = lambda c: hooks.append(c.external_user_id)
            conn = await connect(bot, api, "user-A", A)
            assert conn.telegram_user_id == A and conn.chat_id == A and hooks == ["user-A"]
            starts_before = sum(1 for m, p in api.calls if m == "sendMessage")
            for _ in range(3):                       # proactive messages, no /start in between
                a, sent = await request(bot, api, "user-A")
                assert sent["chat_id"] == A and sent["parse_mode"] == "HTML"
            assert sum(1 for m, p in api.calls if m == "sendMessage") == starts_before + 3
    run(t())


@pytest.mark.parametrize("n,index,expected", [(3, 0, "production"), (3, 1, "staging"), (3, 2, "cancel"),
                                              (10, 9, "o9")])
def test_option_click_resumes_graph_with_option_id(tmp_path, n, index, expected):
    async def t():
        async with env(tmp_path) as (bot, api, clock):
            await connect(bot, api, "user-A", A)
            a, sent = await request(bot, api, "user-A", n=n)
            labels = [b["text"] for row in sent["reply_markup"]["inline_keyboard"] for b in row]
            assert len(labels) == n
            await bot.handle_update(cb(A, f"v2:{a.approval_id}:{index}", sent_mid(api)))
            row = bot.bridge.store.get(a.approval_id)
            assert row.selected_option_id == expected and row.resumed_at is not None
            snap = await bot.bridge.graph.aget_state(thread_config(a.thread_id))
            assert snap.values["chosen"] == [expected]
            assert api.answers()[-1] == f"Recorded: {labels[index]}."
    run(t())


def sent_mid(api):
    return api.mid


def test_duplicate_stale_invalid_and_wrong_user_callbacks(tmp_path):
    async def t():
        async with env(tmp_path) as (bot, api, clock):
            await connect(bot, api, "user-A", A)
            await connect(bot, api, "user-B", B)
            a, _ = await request(bot, api, "user-A")
            mid = api.mid
            base = len(api.answers())
            await bot.handle_update(cb(A, f"v2:{a.approval_id}:7", mid))          # out of range
            await bot.handle_update(cb(B, f"v2:{a.approval_id}:0", mid, chat=B))  # wrong user/chat
            await bot.handle_update(cb(B, f"v2:{a.approval_id}:0", mid, chat=A))  # forged chat
            await bot.handle_update(cb(A, f"v2:{a.approval_id}:0", mid + 1))      # wrong message
            await bot.handle_update(cb(A, "v2:doesnotexist:0", mid))              # wrong approval id
            assert bot.bridge.store.get(a.approval_id).status == "pending"
            await bot.handle_update(cb(A, f"v2:{a.approval_id}:1", mid))          # valid
            await bot.handle_update(cb(A, f"v2:{a.approval_id}:0", mid))          # duplicate/replay
            assert api.answers()[base:] == ["This choice is not valid.",
                                     "You are not authorized to decide this request.",
                                     "You are not authorized to decide this request.",
                                     "This button does not belong to this request.",
                                     "Unknown or invalid request.",
                                     "Recorded: Deploy to Staging.",
                                     "Already decided: Deploy to Staging."]
            assert bot.bridge.store.get(a.approval_id).selected_option_id == "staging"
            clock["t"] = NOW + 10_000                                             # stale/expired
            b, _ = await request(bot, api, "user-A")
            clock["t"] += 10_000
            await bot.handle_update(cb(A, f"v2:{b.approval_id}:0", api.mid))
            assert api.answers()[-1] == "This request has expired."
    run(t())


def test_multi_user_isolation(tmp_path):
    async def t():
        async with env(tmp_path) as (bot, api, clock):
            await connect(bot, api, "user-A", A)
            await connect(bot, api, "user-B", B)
            a, sa = await request(bot, api, "user-A")
            mid_a = api.mid
            b, sb = await request(bot, api, "user-B")
            mid_b = api.mid
            assert sa["chat_id"] == A and sb["chat_id"] == B
            assert (a.approver_user_id, b.approver_user_id) == (A, B)
            assert a.recipient_external_user_id == "user-A" and b.recipient_external_user_id == "user-B"
            await bot.handle_update(cb(B, f"v2:{a.approval_id}:0", mid_a, chat=B))   # B tampers with A's id
            await bot.handle_update(cb(A, f"v2:{b.approval_id}:0", mid_b, chat=A))   # A tampers with B's id
            assert bot.bridge.store.get(a.approval_id).status == "pending"
            assert bot.bridge.store.get(b.approval_id).status == "pending"
            await bot.handle_update(cb(B, f"v2:{b.approval_id}:2", mid_b))
            await bot.handle_update(cb(A, f"v2:{a.approval_id}:0", mid_a))
            assert bot.bridge.store.get(a.approval_id).selected_option_id == "production"
            assert bot.bridge.store.get(b.approval_id).selected_option_id == "cancel"
    run(t())


def test_not_connected_blocked_disconnected_and_403(tmp_path, caplog):
    async def t():
        async with env(tmp_path) as (bot, api, clock):
            with pytest.raises(NotConnectedError):
                await bot.request_approval("nobody", graph_input={"title": "x"})
            await connect(bot, api, "user-A", A)
            blocked = []
            bot.on_blocked = lambda c: blocked.append(c.external_user_id)
            kicked = {"update_id": 9, "my_chat_member": {"chat": {"id": A, "type": "private"}, "from": {"id": A},
                                                         "new_chat_member": {"status": "kicked"}}}
            await bot.handle_update(kicked)
            assert bot.connections.get("user-A").status == "blocked" and blocked == ["user-A"]
            with pytest.raises(RecipientUnavailableError):
                await bot.request_approval("user-A", graph_input={"title": "x"})
            unblock = {"update_id": 10, "my_chat_member": {"chat": {"id": A, "type": "private"}, "from": {"id": A},
                                                           "new_chat_member": {"status": "member"}}}
            await bot.handle_update(unblock)
            assert bot.connections.get("user-A").status == "active"           # no new token needed
            a, _ = await request(bot, api, "user-A")
            assert a.status == "pending"
            # 403 on send -> blocked + undeliverable
            api.fail["sendMessage"] = lambda: TelegramError("sendMessage", 403, "Forbidden: bot was blocked by the user")
            with pytest.raises(RecipientUnavailableError):
                await bot.request_approval("user-A", graph_input={"title": "y"})
            api.fail.clear()
            assert bot.connections.get("user-A").status == "blocked"
            rows = bot.bridge.store._conn.execute("SELECT status FROM approvals WHERE title='y'").fetchall()
            assert [r[0] for r in rows] == ["undeliverable"]
    run(t())


def test_disconnect_and_replaced_connection_block_pending_clicks(tmp_path):
    async def t():
        async with env(tmp_path) as (bot, api, clock):
            await connect(bot, api, "user-A", A)
            a, _ = await request(bot, api, "user-A")
            mid = api.mid
            await bot.handle_update(msg(A, "/disconnect"))
            assert bot.connections.get("user-A") is None
            await bot.handle_update(cb(A, f"v2:{a.approval_id}:0", mid))
            assert api.answers()[-1] == "This Telegram account is no longer connected."
            assert bot.bridge.store.get(a.approval_id).status == "pending"
            # reconnect with a DIFFERENT Telegram account: old account's pending click is refused
            await connect(bot, api, "user-A", A)
            b, _ = await request(bot, api, "user-A")
            mid_b = api.mid
            await connect(bot, api, "user-A", B)
            await bot.handle_update(cb(A, f"v2:{b.approval_id}:0", mid_b))
            assert api.answers()[-1] == "This Telegram account is no longer connected."
            assert bot.bridge.store.get(b.approval_id).status == "pending"
    run(t())


def test_start_flows_and_rate_limit(tmp_path):
    async def t():
        async with env(tmp_path) as (bot, api, clock):
            await bot.handle_update(msg(A, "/start"))
            assert "not an authorized approver" in api.sends(A)[-1]["text"]
            for i in range(5):
                await bot.handle_update(msg(A, f"/start c_{'X' * 42}{i}"))
                assert "invalid or expired" in api.sends(A)[-1]["text"]
            n = len(api.sends(A))
            link = bot.connections.create_link("user-A", NOW)
            await bot.handle_update(msg(A, f"/start {link.token}"))      # rate-limited: silently ignored
            assert len(api.sends(A)) == n
            await bot.handle_update(msg(B, f"/start {link.token}"))      # another user is unaffected
            assert api.sends(B)[-1]["reply_markup"]["inline_keyboard"][0][0]["callback_data"].startswith("c2:")
            cancel = api.sends(B)[-1]["reply_markup"]["inline_keyboard"][0][1]["callback_data"]
            await bot.handle_update(cb(B, cancel, 1))
            assert bot.connections.get("user-A") is None and api.answers()[-1] == "Cancelled."
            await bot.handle_update(msg(B, "/start", chat=B))
            group = msg(A, f"/start {link.token}")
            group["message"]["chat"]["type"] = "group"
            await bot.handle_update(group)
            assert "private chat" in api.sends(A)[-1]["text"]
    run(t())


def test_connected_start_does_not_create_approvals_and_username_change(tmp_path):
    async def t():
        async with env(tmp_path) as (bot, api, clock):
            await connect(bot, api, "user-A", A)
            await bot.handle_update(msg(A, "/start"))
            assert "is connected to" in api.sends(A)[-1]["text"]
            assert bot.bridge.store._conn.execute("SELECT COUNT(*) FROM approvals").fetchone()[0] == 0
            renamed = msg(A, "hello")
            renamed["message"]["from"]["username"] = "new_name"
            await bot.handle_update(renamed)
            c = bot.connections.get("user-A")
            assert c.username == "new_name" and c.telegram_user_id == A
    run(t())


def test_tampered_resume_values_rejected_in_graph(tmp_path):
    async def t():
        async with env(tmp_path) as (bot, api, clock):
            await connect(bot, api, "user-A", A)
            a, _ = await request(bot, api, "user-A")
            cfg = thread_config(a.thread_id)
            # right digest but option id not in the list
            await bot.bridge.graph.ainvoke(Command(resume={a.interrupt_id: {
                "option_id": "delete_everything", "payload_digest": a.payload_digest}}), cfg)
            snap = await bot.bridge.graph.aget_state(cfg)
            assert snap.values["chosen"] == [None]
            b, _ = await request(bot, api, "user-A")
            # valid option but digest of a different payload
            await bot.bridge.graph.ainvoke(Command(resume={b.interrupt_id: {
                "option_id": "production", "payload_digest": payload_digest({"x": 1})}}), thread_config(b.thread_id))
            snap = await bot.bridge.graph.aget_state(thread_config(b.thread_id))
            assert snap.values["chosen"] == [None]
    run(t())


def test_restart_persistence_connected_user(tmp_path):
    async def t():
        async with env(tmp_path) as (bot, api, clock):
            await connect(bot, api, "user-A", A)
            a, _ = await request(bot, api, "user-A")
            mid = api.mid
        async with env(tmp_path) as (bot2, api2, clock2):                 # restarted worker
            assert bot2.connections.get("user-A").chat_id == A
            await bot2.handle_update(cb(A, f"v2:{a.approval_id}:1", mid))
            assert bot2.bridge.store.get(a.approval_id).selected_option_id == "staging"
            b = await bot2.request_approval("user-A", graph_input={"title": "again"})
            assert api2.sends()[-1]["chat_id"] == A and b.status == "pending"
    run(t())


def test_approval_result_approved_property():
    from langgraph_external_hitl.bridge import ApprovalResult
    r = ApprovalResult("staging", None, "id", 1, options=tuple(OPTS3))
    with pytest.raises(ValueError):
        _ = r.approved
    from langgraph_external_hitl import APPROVE_REJECT
    assert ApprovalResult("approve", None, "id", 1, options=APPROVE_REJECT).approved is True


def test_link_tokens_and_bot_token_not_logged(tmp_path, caplog):
    caplog.set_level(logging.DEBUG)

    async def t():
        async with env(tmp_path) as (bot, api, clock):
            link = bot.connections.create_link("user-A", NOW)
            await bot.handle_update(msg(A, f"/start {link.token}"))
            return link.token
    token = run(t())
    from langgraph_external_hitl import redact
    pkg = "\n".join(r.getMessage() for r in caplog.records if r.name.startswith("langgraph_external_hitl"))
    assert pkg and token not in pkg and "1:T" not in pkg
    assert token not in redact(f"someone logged {token}")          # registered as a secret
