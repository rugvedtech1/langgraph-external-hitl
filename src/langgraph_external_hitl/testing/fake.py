"""FakeChannel: an in-memory ApprovalChannel for tests (no network, no Telegram)."""
from __future__ import annotations

from ..channels.base import ChannelError, DecisionReport, DecisionRequest, DeliveryReceipt


class FakeChannel:
    """Records deliveries/updates; can be told to fail the next send or every update."""

    name = "fake"

    def __init__(self, name: str = "fake") -> None:
        self.name = name
        self.sent: list = []                 # approvals sent, in order
        self.updates: list[DecisionReport] = []
        self._fail: list[ChannelError] = []
        self.fail_updates = False
        self._n = 0

    def fail_next_send(self, *, permanent: bool, reason: str = "error") -> None:
        self._fail.append(ChannelError(f"fake {reason}", permanent=permanent, reason=reason))

    async def send(self, approval) -> DeliveryReceipt:
        if self._fail:
            raise self._fail.pop(0)
        self._n += 1
        self.sent.append(approval)
        return DeliveryReceipt(f"{self.name}-msg-{self._n}")

    async def update(self, approval, report: DecisionReport) -> None:
        if self.fail_updates:
            raise RuntimeError("fake update failure")
        self.updates.append(report)

    def request(self, approval, choice, *, actor_ref: str, delivery_ref: str | None = None) -> DecisionRequest:
        """What this channel would produce when ``actor_ref`` clicks ``choice`` on ``approval``."""
        ref = delivery_ref if delivery_ref is not None else approval.external_ref
        return DecisionRequest(approval.approval_id, choice, self.name, actor_ref, ref)
