"""Telegram onboarding helpers used by `langgraph-hitl setup`, `doctor` and the HITL facade.

All failures become ``TelegramSetupError`` subclasses with a human-readable message that
never contains the bot token.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from .._redact import redact
from ..errors import HitlError
from ._api import TelegramError


class TelegramSetupError(HitlError):
    """Base class; ``str(e)`` is safe to show to users."""


class InvalidBotTokenError(TelegramSetupError):
    pass


class TelegramNetworkError(TelegramSetupError):
    pass


class PollerConflictError(TelegramSetupError):
    pass


class TelegramApiError(TelegramSetupError):
    pass


@dataclass(frozen=True)
class BotIdentity:
    id: int
    username: str
    name: str


def mask_token(token: str | None) -> str:
    """'123456789:AAH…wxyz' style display (bot id + last 4 chars only)."""
    if not token or ":" not in token:
        return "<not set>"
    bot_id, secret = token.split(":", 1)
    return f"{bot_id}:…{secret[-4:]}" if len(secret) >= 8 else f"{bot_id}:…"


def friendly(exc: BaseException, method: str) -> TelegramSetupError:
    """Map a Bot API / network exception to a user-facing error."""
    if isinstance(exc, TelegramSetupError):
        return exc
    if isinstance(exc, TelegramError):
        if exc.error_code in (401, 404):
            return InvalidBotTokenError(
                "Telegram rejected the bot token (401 Unauthorized). It may be mistyped, revoked or reset. "
                "Get the current token from @BotFather -> /mybots -> your bot -> API Token.")
        if exc.error_code == 409:
            return PollerConflictError(
                "Another program is already receiving updates for this bot (409 Conflict). "
                "Stop the other worker/bot process (only one may poll a bot token) and try again.")
        return TelegramApiError(f"Telegram API error in {method}: {redact(exc.description or exc)}")
    if isinstance(exc, (httpx.HTTPError, OSError)):
        return TelegramNetworkError(
            f"Could not reach api.telegram.org ({type(exc).__name__}). Check your internet connection, "
            "proxy or firewall and try again.")
    if isinstance(exc, ValueError):
        return TelegramApiError(f"Unexpected (non-JSON) response from Telegram in {method}.")
    return TelegramApiError(f"{method} failed: {type(exc).__name__}")


async def _call(api: Any, method: str, **params: Any) -> Any:
    try:
        return await api.call(method, **params)
    except Exception as e:  # noqa: BLE001 - mapped to a friendly error
        raise friendly(e, method) from None


async def get_me(api: Any) -> BotIdentity:
    """Validate the token and discover the bot identity (no username needed from the user)."""
    me = await _call(api, "getMe")
    if not me.get("is_bot", True) or not me.get("username"):
        raise TelegramApiError("getMe did not return a bot account")
    return BotIdentity(id=int(me["id"]), username=me["username"], name=me.get("first_name") or me["username"])


async def webhook_url(api: Any) -> str:
    """Current webhook URL ('' if none). Polling (getUpdates) cannot work while a webhook is set."""
    info = await _call(api, "getWebhookInfo")
    return info.get("url") or ""


async def delete_webhook(api: Any) -> None:
    """Remove the webhook (call only after the user explicitly confirmed)."""
    await _call(api, "deleteWebhook", drop_pending_updates=False)


__all__ = ["BotIdentity", "InvalidBotTokenError", "PollerConflictError", "TelegramApiError",
           "TelegramNetworkError", "TelegramSetupError", "delete_webhook", "friendly", "get_me",
           "mask_token", "webhook_url"]
