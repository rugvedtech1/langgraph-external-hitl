"""Telegram-layer tests: fake Bot API (no network) + real graph + real SQLite."""
import asyncio
import time
from typing import Any

from langgraph_external_hitl.telegram import TelegramApprovalBot, TelegramConfig
from langgraph_external_hitl.telegram._handlers import parse_callback_data

USER = 6400268237
ACTION = "DEMO: delete 3 files in /tmp/demo"


def make_bot(bridge, api, approvers=frozenset({USER}), clock=lambda: int(time.time())):
    cfg = TelegramConfig(token="123:TEST", approver_user_ids=approvers)

    async def on_start(user_id: int, chat_id: int):
        return await bridge.start(approver_user_id=user_id, chat_id=chat_id, action=ACTION,
                                  now=clock(), ttl_s=cfg.approval_ttl_s)

    return TelegramApprovalBot(cfg, bridge, on_start=on_start, api=api, clock=clock)


def start_msg(user: int = USER) -> dict[str, Any]:
    return {"from": {"id": user}, "chat": {"id": user, "type": "private"}, "text": "/start"}


def callback(data: str, message_id: int, user: int = USER, cq_id: str = "q1") -> dict[str, Any]:
    return {"id": cq_id, "from": {"id": user}, "data": data,
            "message": {"message_id": message_id, "chat": {"id": user}}}


async def send_request(bot, api):
    await bot.handle_update({"update_id": 1, "message": start_msg()})
    _, params = api.calls[-1]
    approval_id = params["reply_markup"]["inline_keyboard"][0][0]["callback_data"].split(":")[1]
    return approval_id, bot.bridge.store.get(approval_id).message_id


def test_parse_callback_data():
    assert parse_callback_data("a:abc") == ("approve", "abc")
    assert parse_callback_data("r:abc") == ("reject", "abc")
    for bad in ["", "abc", "x:abc", "a:", ":abc", "a:" + "z" * 65]:
        assert parse_callback_data(bad) is None


def test_start_unauthorized_creates_nothing(tmp_path, env_factory, fake_api_cls):
    async def t():
        async with env_factory(tmp_path, []) as b:
            api = fake_api_cls()
            bot = make_bot(b, api, approvers=frozenset())
            await bot.handle_start(start_msg())
            assert api.methods() == ["sendMessage"] and "not an authorized" in api.calls[0][1]["text"]
            assert b.store._conn.execute("SELECT COUNT(*) FROM approvals").fetchone()[0] == 0
    asyncio.run(t())


def test_start_without_hook_only_replies(tmp_path, env_factory, fake_api_cls):
    async def t():
        async with env_factory(tmp_path, []) as b:
            api = fake_api_cls()
            bot = TelegramApprovalBot(TelegramConfig(token="1:T", approver_user_ids=frozenset({USER})),
                                      b, api=api)
            await bot.handle_start(start_msg())
            assert api.methods() == ["sendMessage"] and "are an authorized approver" in api.calls[0][1]["text"]
            assert b.store._conn.execute("SELECT COUNT(*) FROM approvals").fetchone()[0] == 0
    asyncio.run(t())


def test_group_chat_refused(tmp_path, env_factory, fake_api_cls):
    async def t():
        async with env_factory(tmp_path, []) as b:
            api = fake_api_cls()
            bot = make_bot(b, api)
            msg = start_msg() | {"chat": {"id": -100, "type": "group"}}
            await bot.handle_start(msg)
            assert "private chat" in api.calls[0][1]["text"]
    asyncio.run(t())


def test_start_runs_graph_then_sends_and_links(tmp_path, env_factory, fake_api_cls):
    async def t():
        async with env_factory(tmp_path, []) as b:
            api = fake_api_cls()
            approval_id, mid = await send_request(make_bot(b, api), api)
            a = b.store.get(approval_id)
            assert a.status == "pending" and a.message_id == mid == 101
            assert a.thread_id and a.interrupt_id and a.payload_digest
            assert len(f"a:{approval_id}".encode()) <= 64
            assert a.thread_id not in str(api.calls)          # thread_id never sent to Telegram
    asyncio.run(t())


def test_approve_resumes_graph_once_and_edits(tmp_path, env_factory, fake_api_cls):
    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            api = fake_api_cls()
            bot = make_bot(b, api)
            approval_id, mid = await send_request(bot, api)
            await bot.handle_callback(callback(f"a:{approval_id}", mid, cq_id="q1"))
            await bot.handle_callback(callback(f"a:{approval_id}", mid, cq_id="q2"))
            assert api.answers() == ["Recorded: Approve.", "Already decided: Approve."]
            assert len(executed) == 1
            assert len(api.edits()) == 1 and "Graph: executed" in api.edits()[0]
            assert b.store.get(approval_id).resumed_at is not None
    asyncio.run(t())


def test_reject_resumes_graph_cancelled(tmp_path, env_factory, fake_api_cls):
    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            api = fake_api_cls()
            bot = make_bot(b, api)
            approval_id, mid = await send_request(bot, api)
            await bot.handle_callback(callback(f"r:{approval_id}", mid))
            assert api.answers() == ["Recorded: Reject."]
            assert executed == [] and "Graph: cancelled" in api.edits()[0]
    asyncio.run(t())


def test_invalid_data_is_answered_without_graph(tmp_path, env_factory, fake_api_cls):
    async def t():
        async with env_factory(tmp_path, []) as b:
            api = fake_api_cls()
            await make_bot(b, api).handle_callback(callback("x:zzz", 1))
            assert api.calls == [("answerCallbackQuery", {"callback_query_id": "q1",
                                                           "text": "Unknown or invalid request."})]
    asyncio.run(t())


def test_wrong_user_no_resume(tmp_path, env_factory, fake_api_cls):
    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            api = fake_api_cls()
            bot = make_bot(b, api)
            approval_id, mid = await send_request(bot, api)
            await bot.handle_callback(callback(f"a:{approval_id}", mid, user=999))
            assert api.answers() == ["You are not authorized to decide this request."]
            assert executed == [] and b.store.get(approval_id).status == "pending"
    asyncio.run(t())


def test_stale_click_edits_message_and_does_not_consume(tmp_path, env_factory, fake_api_cls):
    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            api = fake_api_cls()
            bot = make_bot(b, api)
            approval_id, mid = await send_request(bot, api)
            b.store._conn.execute("UPDATE approvals SET interrupt_id=? WHERE approval_id=?",
                                  ("f" * 32, approval_id))
            await bot.handle_callback(callback(f"a:{approval_id}", mid))
            assert api.answers() == ["This request is no longer valid."]
            assert "NO LONGER VALID" in api.edits()[0]
            assert executed == [] and b.store.get(approval_id).status == "pending"
    asyncio.run(t())


def test_restart_between_send_and_click(tmp_path, env_factory, fake_api_cls):
    async def t():
        executed = []
        api = fake_api_cls()
        async with env_factory(tmp_path, executed) as b:
            approval_id, mid = await send_request(make_bot(b, api), api)
        async with env_factory(tmp_path, executed) as b:
            await make_bot(b, api).handle_callback(callback(f"a:{approval_id}", mid))
            assert b.store.get(approval_id).selected_option_id == "approve"
        assert len(executed) == 1
    asyncio.run(t())


def test_expired_click_edits_message_no_resume(tmp_path, env_factory, fake_api_cls):
    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            api = fake_api_cls()
            approval_id, mid = await send_request(make_bot(b, api), api)
            expires = b.store.get(approval_id).expires_at
            late_bot = make_bot(b, api, clock=lambda: expires + 1)
            await late_bot.handle_callback(callback(f"a:{approval_id}", mid))
            assert api.answers() == ["This request has expired."]
            assert executed == [] and b.store.get(approval_id).status == "expired"
            assert "EXPIRED" in api.edits()[0]
    asyncio.run(t())
