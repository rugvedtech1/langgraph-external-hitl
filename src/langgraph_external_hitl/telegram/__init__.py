"""Telegram channel (requires the ``[telegram]`` extra)."""
from __future__ import annotations

from ..errors import MissingDependencyError

try:
    import httpx  # noqa: F401
except ImportError as e:  # pragma: no cover - exercised via packaging tests
    raise MissingDependencyError("Telegram", "telegram") from e

from ._api import TelegramError
from .onboarding import (BotIdentity, InvalidBotTokenError, PollerConflictError,
                         TelegramNetworkError, TelegramSetupError, mask_token)
from ._handlers import DeliveryResult, RecoveryReport, TelegramAdapter, TelegramApprovalBot
from .config import TelegramConfig

__all__ = [
    "BotIdentity",
    "InvalidBotTokenError",
    "PollerConflictError",
    "TelegramNetworkError",
    "TelegramSetupError",
    "mask_token",
    "DeliveryResult",
    "RecoveryReport",
    "TelegramAdapter",
    "TelegramApprovalBot",
    "TelegramConfig",
    "TelegramError",
]
