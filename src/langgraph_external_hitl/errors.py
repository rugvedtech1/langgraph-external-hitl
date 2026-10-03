"""Package exceptions (public)."""
from __future__ import annotations


class HitlError(Exception):
    """Base class for all langgraph-external-hitl errors."""


class StartError(HitlError):
    """The graph did not pause at exactly one interrupt."""


class SchemaVersionError(HitlError):
    """The approvals database was created by a newer, incompatible version."""


class ActionTooLongError(HitlError, ValueError):
    """The action text is empty or longer than MAX_ACTION_LENGTH (Telegram message limit)."""


class InvalidOptionsError(HitlError, ValueError):
    """The approval options are invalid (count, ids, labels, descriptions, styles)."""


class NotConnectedError(HitlError):
    """The application user has no active Telegram connection."""


class RecipientUnavailableError(HitlError):
    """The recipient's Telegram connection is blocked, disconnected or unreachable."""


class RecipientMismatchError(HitlError):
    """A tracked thread is pinned to a different recipient (fails closed)."""


class NotTrackedError(HitlError):
    """deliver_pending() was called for a thread that was never tracked and no recipient was given."""


class InvalidRecipientNameError(HitlError, ValueError):
    """A recipient name does not match ^[a-z][a-z0-9_-]{0,31}$ (or is reserved)."""


class DuplicateRecipientError(HitlError, ValueError):
    """A recipient with this name already exists."""


class UnknownRecipientError(HitlError, LookupError):
    """No registered recipient has this name (fails closed: there is no fallback recipient)."""


class MissingDependencyError(HitlError, ImportError):
    """An optional extra is required for this feature but is not installed."""

    def __init__(self, feature: str, extra: str) -> None:
        super().__init__(
            f"{feature} support requires the '{extra}' extra: "
            f"pip install 'langgraph-external-hitl[{extra}]'"
        )
        self.extra = extra


__all__ = ["DuplicateRecipientError", "InvalidRecipientNameError", "UnknownRecipientError",
           "ActionTooLongError", "HitlError", "InvalidOptionsError", "MissingDependencyError",
           "NotConnectedError", "NotTrackedError", "RecipientMismatchError",
           "RecipientUnavailableError", "SchemaVersionError", "StartError"]
