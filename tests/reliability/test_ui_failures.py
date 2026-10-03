"""Part 5 / T1-T4: Telegram UI failures must never prevent the LangGraph resume."""
import asyncio
import json
import logging
import time

import httpx

from langgraph_external_hitl.telegram import TelegramApprovalBot, TelegramConfig, TelegramError

USER = 7
TOKEN = "987654321:AAH-THIS-IS-A-FAKE-TOKEN-FOR-TESTS-xyz"


def make_bot(bridge, api):
    cfg = TelegramConfig(token=TOKEN, approver_user_ids=frozenset({USER}))

    async def on_start(u, c):
        return await bridge.start(approver_user_id=u, chat_id=c, action="pay invoice 42",
                                  now=int(time.time()), ttl_s=300)

    return TelegramApprovalBot(cfg, bridge, on_start=on_start, api=api)


async def start_and_click(bot, api, data_prefix="a"):
    await bot.handle_update({"update_id": 1, "message": {"from": {"id": USER},
                             "chat": {"id": USER, "type": "private"}, "text": "/start"}})
    sends = [p for m, p in api.calls if m == "sendMessage"]
    aid = sends[-1]["reply_markup"]["inline_keyboard"][0][0]["callback_data"].split(":")[1]
    mid = bot.bridge.store.get(aid).message_id
    await bot.handle_update({"update_id": 2, "callback_query": {
        "id": "q1", "from": {"id": USER}, "data": f"{data_prefix}:{aid}",
        "message": {"message_id": mid, "chat": {"id": USER}}}})
    return aid


def url_error(cls):
    req = httpx.Request("POST", f"https://api.telegram.org/bot{TOKEN}/answerCallbackQuery")
    return lambda: cls(f"boom for bot{TOKEN}", request=req)


def run_case(tmp_path, env_factory, flaky_api_cls, fail, data_prefix="a"):
    async def t():
        executed = []
        async with env_factory(tmp_path, executed) as b:
            api = flaky_api_cls(fail)
            aid = await start_and_click(make_bot(b, api), api, data_prefix)
            return b.store.get(aid), executed, api
    return asyncio.run(t())


def test_answer_network_error_still_resumes(tmp_path, env_factory, flaky_api_cls):
    a, executed, api = run_case(tmp_path, env_factory, flaky_api_cls,
                                {"answerCallbackQuery": url_error(httpx.ConnectError)})
    assert a.selected_option_id == "approve" and a.resumed_at is not None and len(executed) == 1
    assert "editMessageText" in api.methods()


def test_answer_non_json_502_still_resumes(tmp_path, env_factory, flaky_api_cls):
    a, executed, _ = run_case(tmp_path, env_factory, flaky_api_cls,
                              {"answerCallbackQuery": lambda: json.JSONDecodeError("x", "<html>", 0)})
    assert a.resumed_at is not None and len(executed) == 1


def test_answer_timeout_still_resumes(tmp_path, env_factory, flaky_api_cls):
    a, executed, _ = run_case(tmp_path, env_factory, flaky_api_cls,
                              {"answerCallbackQuery": url_error(httpx.ReadTimeout)})
    assert a.resumed_at is not None and len(executed) == 1


def test_answer_unexpected_exception_still_resumes(tmp_path, env_factory, flaky_api_cls):
    a, executed, _ = run_case(tmp_path, env_factory, flaky_api_cls,
                              {"answerCallbackQuery": lambda: RuntimeError("weird")})
    assert a.resumed_at is not None and len(executed) == 1


def test_reject_with_answer_failure_still_resumes_cancelled(tmp_path, env_factory, flaky_api_cls):
    a, executed, _ = run_case(tmp_path, env_factory, flaky_api_cls,
                              {"answerCallbackQuery": url_error(httpx.ConnectError)}, data_prefix="r")
    assert a.selected_option_id == "reject" and a.resumed_at is not None and executed == []


def test_edit_failure_after_resume_is_harmless(tmp_path, env_factory, flaky_api_cls):
    a, executed, _ = run_case(tmp_path, env_factory, flaky_api_cls,
                              {"editMessageText": url_error(httpx.ConnectError)})
    assert a.resumed_at is not None and len(executed) == 1


def test_expired_callback_400_is_final_and_quiet(tmp_path, env_factory, flaky_api_cls, caplog):
    caplog.set_level(logging.INFO)
    err = lambda: TelegramError("answerCallbackQuery", 400,  # noqa: E731
                                "Bad Request: query is too old and response timeout expired or query ID is invalid")
    a, executed, api = run_case(tmp_path, env_factory, flaky_api_cls, {"answerCallbackQuery": err})
    assert a.resumed_at is not None and len(executed) == 1
    assert api.methods().count("answerCallbackQuery") == 1          # no retry
    assert any("answerCallbackQuery skipped" in r.getMessage() for r in caplog.records)


def test_unauthorized_reply_failure_does_not_crash(tmp_path, env_factory, flaky_api_cls):
    async def t():
        async with env_factory(tmp_path, []) as b:
            api = flaky_api_cls({"sendMessage": url_error(httpx.ConnectError)})
            bot = TelegramApprovalBot(TelegramConfig(token=TOKEN, approver_user_ids=frozenset()), b, api=api)
            await bot.handle_start({"from": {"id": 5}, "chat": {"id": 5, "type": "private"}, "text": "/start"})
            assert api.methods() == ["sendMessage"]
    asyncio.run(t())
