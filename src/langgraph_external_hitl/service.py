"""HitlService: the channel-neutral approval workflow (Part 8.5).

Application-facing:   track(thread_id, recipient) -> your own graph.ainvoke(...) -> deliver_pending(thread_id)
                      recover() at startup; on_completed via HitlBridge
Adapter-facing:       decide(DecisionRequest) -> (adapter acknowledges) -> finish(result)
                      or handle(DecisionRequest) = decide + finish

The service never parses channel payloads and never renders messages; a single
``ApprovalChannel`` adapter does that. Guarantees (unchanged from 0.4): decision exactly once
per approval; delivery at least once; resume at most once through library paths;
completion callback at least once; side effects are the application's responsibility.
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from ._redact import redact
from .channels.base import ApprovalChannel, ChannelError, DecisionReport, DecisionRequest
from .connections import ChannelConnection, ChannelConnections
from .errors import NotConnectedError, NotTrackedError, RecipientUnavailableError, StartError
from .store import Approval, validate_action

if TYPE_CHECKING:  # the LangGraph extra is only needed when the service is used
    from .bridge import DecideResult, HitlBridge

logger = logging.getLogger("langgraph_external_hitl.service")


def now_s() -> int:
    return int(time.time())


@dataclass
class DeliveryResult:
    """Outcome of deliver_pending(). Delivery is AT LEAST ONCE (see README)."""
    thread_id: str
    state: str           # pending|completed|running_or_partial|unknown_thread|not_connected|blocked|
                         # foreign_interrupt|channel_unavailable
    approvals: list[Approval] = field(default_factory=list)
    sent: list[str] = field(default_factory=list)       # approval ids sent by THIS call
    skipped: list[tuple[str, str]] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)     # transient send failures (retry later)


@dataclass
class RecoveryReport:
    reconcile: Any = None
    deliveries: list[DeliveryResult] = field(default_factory=list)
    completions_notified: list[str] = field(default_factory=list)
    errors: list[tuple[str, str]] = field(default_factory=list)


ConnHook = Callable[[ChannelConnection], Any]


class HitlService:
    """Channel-neutral approval workflow bound to one channel adapter."""

    def __init__(self, bridge: "HitlBridge", channel: ApprovalChannel, *,
                 connections: ChannelConnections | None = None, clock: Callable[[], int] = now_s,
                 approval_ttl_s: int = 300, on_blocked: ConnHook | None = None) -> None:
        self.bridge = bridge
        self.channel = channel
        self.connections = connections or ChannelConnections(bridge.store, channel.name)
        self.clock = clock
        self.approval_ttl_s = approval_ttl_s
        self.on_blocked = on_blocked

    @property
    def store(self):
        return self.bridge.store

    async def _fire(self, hook: ConnHook | None, conn: ChannelConnection | None) -> None:
        if hook is None or conn is None:
            return
        try:
            res = hook(conn)
            if asyncio.iscoroutine(res):
                await res
        except Exception as e:
            logger.warning("connection hook failed: %s: %s", type(e).__name__, redact(e))

    def _delivery_id(self, approval: Approval) -> int | None:
        d = next((x for x in self.store.deliveries(approval.approval_id) if x["channel"] == self.channel.name), None)
        return d["delivery_id"] if d else None

    # ---------- application-facing ----------

    async def track(self, thread_id: str, recipient: str) -> None:
        """Register an application-owned thread for ``recipient``. Call BEFORE invoking the graph."""
        self.bridge.track(thread_id, recipient, self.clock())

    async def deliver_pending(self, thread_id: str, recipient: str | None = None, *,
                              ttl_s: int | None = None) -> DeliveryResult:
        """Send the approval(s) a paused, application-owned thread is waiting for. Never invokes the
        graph. Idempotent per (approval, channel) - an atomic claim/lease protects concurrent callers."""
        now = self.clock()
        if recipient is not None:
            self.bridge.track(thread_id, recipient, now)       # pins / verifies the recipient
        elif self.store.get_thread(thread_id) is None and not await self.bridge.has_recipient_literal(thread_id):
            raise NotTrackedError(f"thread {thread_id!r} is not tracked; call track() first")
        plan = await self.bridge.prepare_delivery(thread_id, now, ttl_s or self.approval_ttl_s,
                                                  channel=self.channel.name)
        result = DeliveryResult(thread_id, plan.state, skipped=list(plan.skipped))
        for a in plan.to_send:
            did = self._delivery_id(a)
            assert did is not None
            try:
                receipt = await self.channel.send(a)
            except ChannelError as e:
                if e.permanent and e.reason == "blocked":
                    self.store.mark_undeliverable(a.approval_id, now)
                    if self.connections.set_blocked(a.actor_ref, now):
                        await self._fire(self.on_blocked, self.connections.get_by_actor(a.actor_ref))
                    result.state = "blocked"
                    result.skipped.append((a.approval_id, "blocked"))
                else:
                    self.store.mark_failed(did, e.reason)
                    result.failed.append(a.approval_id)
                    if e.permanent:
                        result.state = "channel_unavailable"
                    logger.warning("delivery failed approval_id=%s channel=%s reason=%s permanent=%s",
                                   a.approval_id, self.channel.name, e.reason, e.permanent)
                continue
            self.store.mark_sent(did, receipt.external_ref, now)
            result.sent.append(a.approval_id)
            logger.info("sent    approval_id=%s  channel=%s  thread_id=%s", a.approval_id, self.channel.name,
                        thread_id)
        result.approvals = [self.store.get(a.approval_id) for a in plan.approvals]  # type: ignore[misc]
        return result

    async def recover(self, now: int | None = None) -> RecoveryReport:
        """Startup recovery: reconcile decided-but-unresumed approvals (safe cases only), deliver
        pending approvals of active tracked threads, retry missed completion callbacks."""
        now = now if now is not None else self.clock()
        report = RecoveryReport(reconcile=await self.bridge.reconcile(now))
        for t in self.store.active_threads():
            try:
                report.deliveries.append(await self.deliver_pending(t.thread_id))
            except Exception as e:  # one bad thread must not stop recovery
                report.errors.append((t.thread_id, type(e).__name__))
                logger.warning("recover: thread %s: %s: %s", t.thread_id, type(e).__name__, redact(e))
        for t in self.store.unnotified_completed_threads():
            if await self.bridge.handle_completion(t.thread_id, now):
                report.completions_notified.append(t.thread_id)
        return report

    async def request_approval(self, recipient: str, *, graph_input: dict[str, Any] | None = None,
                               action: str | None = None, ttl_s: int | None = None,
                               thread_id: str | None = None) -> Approval | None:
        """Convenience: track -> invoke the graph -> deliver_pending. None if no approval was needed."""
        conn = self.connections.get(recipient)
        if conn is None:
            raise NotConnectedError(f"application user {recipient!r} has no {self.channel.name} connection")
        if not conn.is_active:
            raise RecipientUnavailableError(f"{self.channel.name} connection of {recipient!r} is {conn.status}")
        if action is not None:
            validate_action(action)
        if graph_input is None:
            if action is None:
                raise ValueError("either action or graph_input is required")
            graph_input = {"action": action}
        thread_id = thread_id or str(uuid.uuid4())
        self.bridge.track(thread_id, recipient, self.clock())
        await self.bridge.graph.ainvoke(graph_input, {"configurable": {"thread_id": thread_id}}, durability="sync")
        result = await self.deliver_pending(thread_id, ttl_s=ttl_s)
        if result.state == "completed":
            return None
        if result.state == "foreign_interrupt":
            raise StartError("the graph paused at an interrupt that was not produced by request_approval()")
        if result.state == "blocked":
            raise RecipientUnavailableError("recipient has blocked the channel")
        if result.state == "not_connected":
            raise NotConnectedError(f"application user {recipient!r} has no {self.channel.name} connection")
        if not result.approvals:
            raise StartError(f"no approval to deliver (thread state: {result.state})")
        return result.approvals[0]

    # ---------- adapter-facing ----------

    async def decide(self, req: DecisionRequest) -> "DecideResult":
        """Validate and record a decision (exactly once). Raises sqlite3 errors if the DB is busy."""
        return await self.bridge.decide_request(req, self.clock())

    async def finish(self, result: "DecideResult") -> DecisionReport:
        """After decide(): resume the graph for a won decision (then deliver the next approval or
        complete), and let the adapter update the delivered message. Never undoes the decision."""
        a = result.approval
        if result.outcome != "won" or a is None:
            report = DecisionReport(result.outcome, a, bool(result.consume and result.consume.changed))
        else:
            now = self.clock()
            res = await self.bridge.resume(a, a.selected_option_id, now)
            logger.info("resume  approval_id=%s  thread_id=%s  option=%s  status=%s  graph_result=%s",
                        a.approval_id, a.thread_id, a.selected_option_id, res.status, res.graph_result)
            next_state = None
            if res.status == "resumed" and a.thread_id and self.store.get_thread(a.thread_id):
                try:  # tracked thread: next approval automatically, or completion handling
                    nxt = await self.deliver_pending(a.thread_id)
                    if nxt.state == "pending" and (nxt.sent or nxt.approvals):
                        next_state = "pending"
                    elif nxt.state == "completed":
                        next_state = "completed"
                except Exception as e:  # recover() retries; never undo the decision
                    logger.warning("auto-delivery after resume failed thread_id=%s: %s: %s",
                                   a.thread_id, type(e).__name__, redact(e))
            report = DecisionReport("won", a, True, res.status, res.graph_result, next_state)
        if report.outcome in ("won", "stale") or (report.outcome == "expired" and report.changed):
            try:
                if a is not None:  # the adapter must see ITS delivery of this approval
                    a = self.store.get(a.approval_id, self.channel.name) or a
                    report = DecisionReport(report.outcome, a, report.changed, report.resume_status,
                                            report.graph_result, report.next_state)
                await self.channel.update(a, report)
            except Exception as e:  # best effort
                logger.warning("channel update failed: %s: %s", type(e).__name__, redact(e))
        return report

    async def handle(self, req: DecisionRequest) -> DecisionReport:
        """decide() + finish() for channels without a separate acknowledgement step."""
        return await self.finish(await self.decide(req))


__all__ = ["DeliveryResult", "HitlService", "RecoveryReport"]
