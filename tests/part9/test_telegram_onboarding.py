"""Phase 3: getMe / webhook / friendly errors / token masking."""
import asyncio
import logging

import httpx
import pytest

from langgraph_external_hitl.telegram import TelegramError
from langgraph_external_hitl.telegram.onboarding import (InvalidBotTokenError, PollerConflictError,
                                                         TelegramApiError, TelegramNetworkError,
                                                         delete_webhook, get_me, mask_token, webhook_url)

TOKEN = "123456789:AAH-onboarding-secret-token-abcdefghij"


class Api:
    def __init__(self, results=None, errors=None):
        self.results, self.errors, self.calls = results or {}, errors or {}, []

    async def call(self, method, **p):
        self.calls.append((method, p))
        if method in self.errors:
            raise self.errors[method]()
        return self.results[method]


def run(c):
    return asyncio.run(c)


def test_get_me_discovers_username():
    me = run(get_me(Api({"getMe": {"id": 42, "is_bot": True, "username": "my_bot", "first_name": "My Bot"}})))
    assert (me.id, me.username, me.name) == (42, "my_bot", "My Bot")


@pytest.mark.parametrize("code", [401, 404])
def test_invalid_token_friendly(code):
    api = Api(errors={"getMe": lambda: TelegramError("getMe", code, f"Unauthorized {TOKEN}")})
    with pytest.raises(InvalidBotTokenError) as ei:
        run(get_me(api))
    assert "BotFather" in str(ei.value) and TOKEN not in str(ei.value)


def test_network_failure_friendly():
    req = httpx.Request("POST", f"https://api.telegram.org/bot{TOKEN}/getMe")
    api = Api(errors={"getMe": lambda: httpx.ConnectError(f"boom bot{TOKEN}", request=req)})
    with pytest.raises(TelegramNetworkError) as ei:
        run(get_me(api))
    assert TOKEN not in str(ei.value) and "api.telegram.org" in str(ei.value)


def test_conflict_409_friendly():
    api = Api(errors={"getWebhookInfo": lambda: TelegramError("getUpdates", 409, "Conflict: terminated by other")})
    with pytest.raises(PollerConflictError, match="only one may poll"):
        run(webhook_url(api))


def test_other_api_error_and_non_json():
    with pytest.raises(TelegramApiError):
        run(get_me(Api(errors={"getMe": lambda: TelegramError("getMe", 500, "Internal")})))
    with pytest.raises(TelegramApiError, match="non-JSON"):
        run(get_me(Api(errors={"getMe": lambda: ValueError("x")})))
    with pytest.raises(TelegramApiError):
        run(get_me(Api({"getMe": {"id": 1, "is_bot": False, "username": "human"}})))


def test_webhook_inspect_and_delete():
    api = Api({"getWebhookInfo": {"url": "https://example.com/hook"}, "deleteWebhook": True})
    assert run(webhook_url(api)) == "https://example.com/hook"
    run(delete_webhook(api))
    assert api.calls[-1] == ("deleteWebhook", {"drop_pending_updates": False})
    assert run(webhook_url(Api({"getWebhookInfo": {"url": ""}}))) == ""


def test_mask_token():
    assert mask_token(TOKEN) == "123456789:…ghij" and TOKEN[12:30] not in mask_token(TOKEN)
    assert mask_token(None) == "<not set>" and mask_token("garbage") == "<not set>"


def test_polling_409_logs_human_message(tmp_path, env_factory, caplog):
    from langgraph_external_hitl.telegram import TelegramApprovalBot, TelegramConfig
    caplog.set_level(logging.ERROR)
    n = {"i": 0}

    class PollApi:
        async def call(self, method, **p):
            if method == "getMe":
                return {"id": 1, "username": "b"}
            if method == "getWebhookInfo":
                return {"url": ""}
            n["i"] += 1
            if n["i"] == 1:
                raise TelegramError("getUpdates", 409, "Conflict")
            raise asyncio.CancelledError()

    async def no_sleep(_):
        return None

    async def t():
        async with env_factory(tmp_path, []) as b:
            asyncio_sleep = asyncio.sleep
            asyncio.sleep = no_sleep
            try:
                with pytest.raises(asyncio.CancelledError):
                    await TelegramApprovalBot(TelegramConfig(token="1:T"), b, api=PollApi()).run_polling()
            finally:
                asyncio.sleep = asyncio_sleep
    run(t())
    assert any("only one worker per bot token" in r.getMessage() for r in caplog.records)
