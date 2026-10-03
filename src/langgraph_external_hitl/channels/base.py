"""The channel adapter contract.

An adapter (Telegram today; Web, WhatsApp later) does four things:
  * ``send(approval)``      deliver one approval to the recipient's channel address -> DeliveryReceipt
  * ``update(approval, r)`` best-effort: reflect the outcome in the delivered message (may no-op)
  * receive interactions and turn them into a ``DecisionRequest`` for ``HitlService.decide()``
  * run its own receive loop / webhook / onboarding flow (channel-specific)

Adapters never decide validity: the core verifies actor, delivery reference, option, expiry,
connection liveness and single use. Adapters raise ``ChannelError`` on delivery failures.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:  # no runtime import cycles
    from ..store import Approval


class ChannelError(Exception):
    """Delivery failure. ``permanent=True`` (e.g. blocked, unauthorized): do not retry;
    ``reason="blocked"`` marks the recipient's connection blocked. Transient errors are retried later."""

    def __init__(self, message: str, *, permanent: bool, reason: str = "error") -> None:
        super().__init__(message)
        self.permanent = permanent
        self.reason = reason


@dataclass(frozen=True)
class DeliveryReceipt:
    external_ref: str           # channel's reference for the delivered message (opaque to the core)


@dataclass(frozen=True)
class DecisionRequest:
    """A human interaction translated by an adapter. Untrusted until the core validates it."""
    approval_id: str
    choice: int | str           # option index (from the UI) or option id
    channel: str
    actor_ref: str              # who acted, in the channel's identity space
    delivery_ref: str           # which delivered message the interaction came from


@dataclass(frozen=True)
class DecisionReport:
    """What the core tells an adapter after handling a decision (for UI updates/acknowledgements)."""
    outcome: str                # won | not_authorized | wrong_message | already_decided | expired | stale | ...
    approval: Any               # Approval | None
    changed: bool = False
    resume_status: str | None = None     # resumed | not_applied | failed (only when outcome == "won")
    graph_result: Any = None
    next_state: str | None = None        # pending (next approval sent) | completed | None


@runtime_checkable
class ApprovalChannel(Protocol):
    name: str

    async def send(self, approval: "Approval") -> DeliveryReceipt: ...

    async def update(self, approval: "Approval", report: DecisionReport) -> None: ...
