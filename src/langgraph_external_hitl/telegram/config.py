"""Telegram configuration. Explicit by default; ``from_env()`` is opt-in."""
from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field


@dataclass(frozen=True)
class TelegramConfig:
    token: str = field(repr=False)            # never shown in repr/logs
    approver_user_ids: frozenset[int] = frozenset()
    approval_ttl_s: int = 300
    poll_timeout_s: int = 30
    bot_username: str | None = None           # needed to build connection links offline

    def __post_init__(self) -> None:
        if not self.token:
            raise ValueError("TelegramConfig.token must not be empty")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "TelegramConfig":
        """Read TELEGRAM_BOT_TOKEN, APPROVER_USER_IDS, APPROVAL_TTL_S, TELEGRAM_BOT_USERNAME."""
        e = os.environ if env is None else env
        token = e.get("TELEGRAM_BOT_TOKEN", "")
        if not token:
            raise ValueError("TELEGRAM_BOT_TOKEN is not set")
        ids = frozenset(int(x) for x in e.get("APPROVER_USER_IDS", "").split(",") if x.strip())
        return cls(token=token, approver_user_ids=ids,
                   approval_ttl_s=int(e.get("APPROVAL_TTL_S", "300")),
                   bot_username=(e.get("TELEGRAM_BOT_USERNAME") or None))


__all__ = ["TelegramConfig"]
