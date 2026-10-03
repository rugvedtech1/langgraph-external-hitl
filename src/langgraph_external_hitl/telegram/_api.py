"""Minimal Telegram Bot API transport over httpx (internal)."""
from __future__ import annotations

from typing import Any

import httpx

from .._redact import protect_logger, register_secret

API_BASE = "https://api.telegram.org"


class TelegramError(RuntimeError):
    """Bot API returned ok=false. Never includes the URL (it contains the token)."""

    def __init__(self, method: str, error_code: int | None, description: str | None) -> None:
        super().__init__(f"{method}: {error_code} {description}")
        self.method = method
        self.error_code = error_code
        self.description = description


class BotApi:
    def __init__(self, token: str, poll_timeout_s: int = 30) -> None:
        register_secret(token)
        for name in ("httpx", "httpcore"):  # their INFO lines contain the token URL
            protect_logger(name)
        self._base = f"{API_BASE}/bot{token}"
        self._http = httpx.AsyncClient(timeout=poll_timeout_s + 15)  # > long-poll timeout

    def __repr__(self) -> str:
        return "BotApi(<token hidden>)"

    async def call(self, method: str, **params: Any) -> Any:
        payload = {k: v for k, v in params.items() if v is not None}
        resp = await self._http.post(f"{self._base}/{method}", json=payload)
        body = resp.json()  # non-JSON (e.g. 502 HTML) raises ValueError
        if not body.get("ok"):
            raise TelegramError(method, body.get("error_code"), body.get("description"))
        return body["result"]

    async def aclose(self) -> None:
        await self._http.aclose()
