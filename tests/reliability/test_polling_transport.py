"""Polling loop + transport error paths (mocked Telegram over httpx.MockTransport)."""
import asyncio
import json
import logging

import httpx
import pytest

from langgraph_external_hitl.telegram import TelegramApprovalBot, TelegramConfig, TelegramError
from langgraph_external_hitl.telegram._api import BotApi

TOKEN = "444555666:AAG-polling-FAKE-token-for-tests-only-99"


def patch_transport(monkeypatch, handler):
    orig = httpx.AsyncClient
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: orig(transport=httpx.MockTransport(handler), **kw))


def test_bot_api_error_paths(monkeypatch):
    def handler(req):
        m = req.url.path.rsplit("/", 1)[1]
        if m == "ok":
            return httpx.Response(200, json={"ok": True, "result": 1})
        if m == "bad":
            return httpx.Response(400, json={"ok": False, "error_code": 400, "description": "Bad Request: x"})
        return httpx.Response(502, text="<html>bad gateway</html>")
    patch_transport(monkeypatch, handler)

    async def t():
        api = BotApi(TOKEN)
        assert await api.call("ok") == 1
        with pytest.raises(TelegramError) as ei:
            await api.call("bad")
        assert ei.value.error_code == 400 and ei.value.method == "bad" and TOKEN not in str(ei.value)
        with pytest.raises(ValueError):
            await api.call("html")
        await api.aclose()
    asyncio.run(t())


def test_run_polling_survives_409_and_non_json(monkeypatch, tmp_path, env_factory, caplog):
    caplog.set_level(logging.INFO)
    polls = {"n": 0}
    seen = []

    def handler(req):
        m = req.url.path.rsplit("/", 1)[1]
        body = json.loads(req.content or b"{}")
        if m == "getMe":
            return httpx.Response(200, json={"ok": True, "result": {"id": 1, "username": "b"}})
        if m == "getWebhookInfo":
            return httpx.Response(200, json={"ok": True, "result": {"url": ""}})
        if m == "getUpdates":
            polls["n"] += 1
            if polls["n"] == 1:
                return httpx.Response(409, json={"ok": False, "error_code": 409,
                                                 "description": "Conflict: terminated by other getUpdates request"})
            if polls["n"] == 2:
                return httpx.Response(502, text="<html>oops</html>")
            if polls["n"] == 3:
                return httpx.Response(200, json={"ok": True, "result": [{"update_id": 5, "message": {
                    "from": {"id": 9}, "chat": {"id": 9, "type": "private"}, "text": "/start"}}]})
            raise asyncio.CancelledError()
        seen.append((m, body.get("text")))
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 1}})
    patch_transport(monkeypatch, handler)

    async def no_sleep(_):
        return None

    async def t():
        async with env_factory(tmp_path, []) as b:
            monkeypatch.setattr(asyncio, "sleep", no_sleep)
            bot = TelegramApprovalBot(TelegramConfig(token=TOKEN, approver_user_ids=frozenset()), b)
            with pytest.raises(asyncio.CancelledError):
                await bot.run_polling()
    asyncio.run(t())
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "poll error: TelegramError: getUpdates: 409" in text
    assert "poll error: JSONDecodeError" in text
    assert seen and "not an authorized approver" in seen[0][1]
    assert TOKEN not in text


def test_run_polling_refuses_when_webhook_set(monkeypatch, tmp_path, env_factory):
    def handler(req):
        m = req.url.path.rsplit("/", 1)[1]
        if m == "getMe":
            return httpx.Response(200, json={"ok": True, "result": {"id": 1}})
        return httpx.Response(200, json={"ok": True, "result": {"url": "https://example.com/hook"}})
    patch_transport(monkeypatch, handler)

    async def t():
        async with env_factory(tmp_path, []) as b:
            with pytest.raises(SystemExit):
                await TelegramApprovalBot(TelegramConfig(token=TOKEN), b).run_polling()
    asyncio.run(t())
