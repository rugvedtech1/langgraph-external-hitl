"""LangGraph bridge (requires the ``[langgraph]`` extra). Verified in Parts 3-5.

Flow per click:
  1. pre-check  (interrupt still pending + payload digest matches)
  2. consume    (atomic single-use, ApprovalStore)
  3. resume     (Command(resume={interrupt_id: value}) on the stored thread)
  4. post-check (interrupt gone -> resumed_at)

Recovery (Part 5): ``find_unresumed()`` classifies decided-but-unconfirmed
approvals; ``reconcile()`` acts ONLY on the two safe classes. A PARTIAL thread
(the interrupt is consumed but the graph did not finish, e.g. a crash inside a
later node) is reported and never resumed or continued automatically.

LangGraph executes nodes at least once. This package resumes each approval at
most once through its own paths, but it cannot make a side effect exactly-once:
actions must be idempotent (use ``approval_id`` as the idempotency key).
"""
from __future__ import annotations

import asyncio
import logging
import time
import warnings
import sqlite3
import uuid
from dataclasses import dataclass, field
from typing import Any, Literal

from ._audit import audit
from ._redact import redact
from .digest import payload_digest
from collections.abc import Sequence

from .errors import InvalidOptionsError, MissingDependencyError, StartError
from .options import APPROVE_REJECT, ApprovalOption, options_from_json, validate_request
from .channels.base import DecisionRequest
from .connections import ChannelConnections
from .errors import InvalidRecipientNameError
from .recipients import RecipientRegistry, normalize_name
from .store import (DEFAULT_DELIVERY_LEASE_S, Approval, ApprovalStore, ConsumeResult, ThreadRecord,
                    validate_action, validate_thread_id)

try:
    from langgraph.types import Command, interrupt
except ImportError as e:  # pragma: no cover - exercised via packaging tests
    raise MissingDependencyError("LangGraph", "langgraph") from e

logger = logging.getLogger(__name__)

ResumeStatus = Literal["resumed", "not_applied", "failed"]
DeliveryState = Literal["pending", "completed", "running_or_partial", "unknown_thread",
                        "not_connected", "blocked", "foreign_interrupt"]
RecoveryState = Literal["ready_to_resume", "completed_unmarked", "partial",
                        "digest_mismatch", "unknown_thread", "unlinked"]


def thread_config(thread_id: str) -> dict[str, Any]:
    return {"configurable": {"thread_id": thread_id}}


# ---------- inside the graph ----------

@dataclass(frozen=True)
class ApprovalResult:
    """What the human chose, as seen inside the graph node."""
    option_id: str | None                 # None if the resume was invalid / unbound
    option: ApprovalOption | None
    approval_id: str | None
    approver_user_id: int | None          # Telegram user id that clicked
    external_user_id: str | None = None   # application user (connected flow)
    binding_ok: bool = True               # False if not bound to this exact payload
    options: tuple[ApprovalOption, ...] = ()
    recipient: str | None = None          # friendly recipient name from the payload (v3), if any

    @property
    def approved(self) -> bool:
        """True if the "approve" option was chosen (only for option sets that contain "approve")."""
        if not any(o.id == "approve" for o in self.options):
            raise ValueError("approved is only defined when an option with id 'approve' exists; "
                             "use option_id")
        return self.option_id == "approve"

    def __bool__(self) -> bool:  # 0.2 compatibility for approve/reject graphs
        return self.option_id == "approve"


ApprovalDecision = ApprovalResult  # 0.2 name


def build_payload(title: str, message: str | None = None,
                  options: Sequence[ApprovalOption] | None = None,
                  recipient: str | None = None) -> dict[str, Any]:
    """The interrupt payload. Legacy form ``{"action": title}`` when only a title is given
    (keeps 0.2 in-flight approvals verifiable); v2 with options; v3 when a ``recipient`` name is
    given (the name is part of the digest-covered payload, so it cannot be changed silently)."""
    if recipient is None and message is None and options is None:
        return {"action": title}
    opts = validate_request(title, message, APPROVE_REJECT if options is None else options)
    payload: dict[str, Any] = {"v": 2, "title": title, "message": message, "options": [o.to_dict() for o in opts]}
    if recipient is not None:
        payload = {"v": 3, **{k: v for k, v in payload.items() if k != "v"}, "recipient": normalize_name(recipient)}
    return payload


def payload_options(payload: dict[str, Any]) -> tuple[ApprovalOption, ...]:
    if payload.get("v") in (2, 3):
        return tuple(ApprovalOption.from_dict(d) for d in payload["options"])
    return APPROVE_REJECT


def request_approval(title: str, message: str | None = None,
                     options: Sequence[ApprovalOption] | None = None, *,
                     recipient: str | None = None) -> ApprovalResult:
    """Pause the graph until the human picks one of ``options``.

    Call this as the FIRST statement of a dedicated approval node: on resume
    LangGraph re-runs the node from the top. Put side effects in a separate
    node and make them idempotent (key them by ``approval_id``).

    ``request_approval("Deploy?")`` (title only) is the 0.2 approve/reject form.
    Binding: the resume value must carry the digest of this exact payload and an
    option id from this exact option list; otherwise ``option_id`` is None.

    ``recipient="manager"`` (optional) names a REGISTERED approver (see `langgraph-hitl setup`).
    Pass a string literal from trusted code - never LLM/user-controlled state. Unknown names fail
    closed and a thread already pinned to another recipient is never redirected. The name is a
    lookup key only; authorization uses the approver's channel identity (Telegram user id).
    """
    payload = build_payload(title, message, options, recipient)
    opts = payload_options(payload)
    value = interrupt(payload)
    if not isinstance(value, dict):
        return ApprovalResult(None, None, None, None, None, binding_ok=False, options=opts)
    binding_ok = value.get("payload_digest") == payload_digest(payload)
    option_id = value.get("option_id")
    if option_id is None and value.get("decision") in ("approve", "reject"):
        option_id = value["decision"]  # 0.2 resume value
    option = next((o for o in opts if o.id == option_id), None)
    ok = binding_ok and option is not None
    return ApprovalResult(
        option_id=option_id if ok else None, option=option if ok else None,
        approval_id=value.get("approval_id"), approver_user_id=value.get("approver_user_id"),
        external_user_id=value.get("external_user_id"), binding_ok=binding_ok, options=opts,
        recipient=payload.get("recipient"))


# ---------- results ----------

@dataclass(frozen=True)
class DecideResult:
    outcome: str                   # ConsumeResult outcome, or "stale"
    approval: Approval | None
    consume: ConsumeResult | None  # None when refused by the pre-check


@dataclass(frozen=True)
class ResumeResult:
    status: ResumeStatus
    graph_result: str | None       # e.g. "executed" / "cancelled" when finished


@dataclass(frozen=True)
class UnresumedApproval:
    approval: Approval
    state: RecoveryState
    graph_next: tuple[str, ...] = ()


@dataclass
class ReconcileReport:
    resumed: list[tuple[Approval, ResumeResult]] = field(default_factory=list)
    marked: list[Approval] = field(default_factory=list)
    needs_attention: list[UnresumedApproval] = field(default_factory=list)

    @property
    def partial(self) -> list[UnresumedApproval]:
        return [u for u in self.needs_attention if u.state == "partial"]


def is_approval_payload(value: Any) -> bool:
    """True only for payloads produced by request_approval() (v2 or legacy 0.2 form)."""
    if not isinstance(value, dict):
        return False
    if value.get("v") == 2:
        return isinstance(value.get("title"), str) and isinstance(value.get("options"), list)
    if value.get("v") == 3:
        return (isinstance(value.get("title"), str) and isinstance(value.get("options"), list)
                and isinstance(value.get("recipient"), str))
    return set(value) == {"action"} and isinstance(value.get("action"), str)


@dataclass
class DeliveryPlan:
    """Result of inspecting a thread and reserving/claiming its pending approvals (internal)."""
    thread_id: str
    state: DeliveryState
    approvals: list[Approval] = field(default_factory=list)   # rows for pending approval interrupts
    to_send: list[Approval] = field(default_factory=list)     # rows THIS caller has claimed
    skipped: list[tuple[str, str]] = field(default_factory=list)  # (approval_id|interrupt_id, reason)


# ---------- bridge ----------

class HitlBridge:
    """Connects an ApprovalStore to a compiled LangGraph graph (with checkpointer)."""

    def __init__(self, graph: Any, store: ApprovalStore, *,
                 on_completed: Any = None, on_resumed: Any = None,
                 delivery_lease_s: int = DEFAULT_DELIVERY_LEASE_S) -> None:
        self.graph = graph
        self.store = store
        self.on_completed = on_completed   # (thread_id, values) -> None | Awaitable; AT LEAST ONCE
        self.on_resumed = on_resumed       # (approval, ResumeResult) -> None | Awaitable; best effort
        self.delivery_lease_s = delivery_lease_s
        self._resume_lock = asyncio.Lock()  # one resume at a time within this process

    # ---------- app-owned invocation (Part 6.1) ----------

    def track(self, thread_id: str, recipient: str, now: int | None = None) -> ThreadRecord:
        """Register ``thread_id`` for ``recipient`` BEFORE invoking the graph (crash recovery).
        Re-tracking with the same recipient is a no-op; a different recipient raises."""
        rec = self.store.track_thread(thread_id, recipient, now if now is not None else int(time.time()))
        audit("thread.tracked", thread_id=thread_id, user_id=recipient, now=rec.updated_at)
        return rec

    async def prepare_delivery(self, thread_id: str, now: int, ttl_s: int,
                               channel: str = "telegram") -> DeliveryPlan:
        """Inspect the paused thread (never invokes the graph), reserve one approval per
        (thread_id, interrupt_id), ensure its delivery on ``channel`` (snapshot of the recipient's
        connection) and claim the deliveries not yet sent. Channel-neutral."""
        validate_thread_id(thread_id)
        thread = self.store.get_thread(thread_id)
        snap = await self.graph.aget_state(thread_config(thread_id))
        interrupts = list(snap.interrupts)
        nxt = tuple(snap.next)
        if not interrupts:
            if not snap.values and not nxt:
                return DeliveryPlan(thread_id, "unknown_thread")
            if not nxt:
                await self.handle_completion(thread_id, now, snap.values)
                return DeliveryPlan(thread_id, "completed")
            return DeliveryPlan(thread_id, "running_or_partial")
        plan = DeliveryPlan(thread_id, "pending")
        ours = [i for i in interrupts if is_approval_payload(i.value)]
        for i in interrupts:
            if i not in ours:
                plan.skipped.append((i.id, "foreign_interrupt"))
        if not ours:
            plan.state = "foreign_interrupt"
            return plan
        # Recipient literals (payload v3): resolve via the registry, fail closed, never redirect.
        registry = RecipientRegistry(self.store)
        refused: set[str] = set()
        for intr in ours:
            name = intr.value.get("recipient") if intr.value.get("v") == 3 else None
            if name is None:
                continue
            try:
                rec = registry.get(name)
            except InvalidRecipientNameError:
                rec = None
            if rec is None:
                plan.state, reason = "unknown_recipient", "unknown_recipient"
            elif thread is None:
                thread = self.track(thread_id, rec.recipient_id, now)       # pin on first delivery
                continue
            elif thread.recipient_external_user_id == rec.recipient_id:
                continue
            else:
                plan.state, reason = "recipient_mismatch", "recipient_mismatch"
            refused.add(intr.id)
            plan.skipped.append((intr.id, reason))
            audit("approval.refused", thread_id=thread_id, interrupt_id=intr.id, reason=reason, now=now)
        ours = [i for i in ours if i.id not in refused]
        if not ours:
            return plan
        if thread is None:
            raise ValueError(f"thread {thread_id!r} is not tracked; call track() first")
        connections = ChannelConnections(self.store, channel)
        for intr in ours:
            row = self.store.get_by_interrupt(thread_id, intr.id)
            delivery = None
            if row is not None:
                delivery = next((d for d in self.store.deliveries(row.approval_id) if d["channel"] == channel), None)
            if delivery is None:
                conn = connections.get(thread.recipient_external_user_id)
                if conn is None:
                    plan.state = "not_connected"
                    plan.skipped.append((intr.id, "not_connected"))
                    continue
                if not conn.is_active:
                    plan.state = "blocked"
                    plan.skipped.append((intr.id, conn.status))
                    continue
                if row is None:
                    value = intr.value
                    if value.get("v") in (2, 3):
                        title, message, opts, version = (value["title"], value.get("message"),
                                                         payload_options(value), value["v"])
                    else:
                        title, message, opts, version = value["action"], None, None, 1
                    row = self.store.reserve_approval(
                        thread_id=thread_id, interrupt_id=intr.id, recipient_id=thread.recipient_external_user_id,
                        title=title, message=message, options=opts, payload_digest=payload_digest(value),
                        payload_version=version, now=now, ttl_s=ttl_s, recipient_name=value.get("recipient"))
                    audit("approval.created", approval_id=row.approval_id, thread_id=thread_id,
                          interrupt_id=intr.id, approver_user_id=conn.actor_ref, digest=row.payload_digest,
                          action_len=len(title), now=now)
                self.store.ensure_delivery(row.approval_id, channel=channel, actor_ref=conn.actor_ref,
                                           address=conn.address, connection_id=conn.connection_id, now=now)
                delivery = next(d for d in self.store.deliveries(row.approval_id) if d["channel"] == channel)
                row = self.store.get(row.approval_id)
            plan.approvals.append(row)
            if row.status != "pending":
                plan.skipped.append((row.approval_id, f"status_{row.status}"))
            elif delivery["delivered_at"] is not None:
                plan.skipped.append((row.approval_id, "already_delivered"))
            elif self.store.claim(delivery["delivery_id"], now, self.delivery_lease_s):
                plan.to_send.append(self.store.get(row.approval_id, channel))  # type: ignore[arg-type]
            else:
                plan.skipped.append((row.approval_id, "delivery_in_progress"))
        if plan.approvals and plan.state in ("not_connected", "blocked", "unknown_recipient", "recipient_mismatch"):
            plan.state = "pending"
        return plan

    async def handle_completion(self, thread_id: str, now: int, values: dict | None = None) -> bool:
        """Mark a tracked thread completed and run on_completed (AT LEAST ONCE).
        The notification is recorded only after the callback returned without error."""
        thread = self.store.get_thread(thread_id)
        if thread is None or thread.status == "cancelled":
            return False
        if thread.status == "active":
            self.store.mark_thread_completed(thread_id, now)
            audit("thread.completed", thread_id=thread_id, now=now)
        thread = self.store.get_thread(thread_id)
        if thread is None or thread.completion_notified_at is not None:
            return False
        if self.on_completed is not None:
            if values is None:
                values = (await self.graph.aget_state(thread_config(thread_id))).values
            try:
                res = self.on_completed(thread_id, values)
                if asyncio.iscoroutine(res):
                    await res
            except Exception as e:  # leave unnotified: recover() retries
                logger.warning("on_completed failed thread_id=%s: %s: %s", thread_id,
                               type(e).__name__, redact(e))
                return False
        self.store.mark_completion_notified(thread_id, now)
        return True

    async def start(self, *, approver_user_id: int, chat_id: int, now: int, ttl_s: int,
                    action: str | None = None, graph_input: dict[str, Any] | None = None,
                    connection_id: str | None = None,
                    recipient_external_user_id: str | None = None) -> Approval:
        """DEPRECATED (0.4): use ``track()`` + your own ``graph.ainvoke()`` + ``deliver_pending()``.

        Start a new thread, wait for its single interrupt, and store the link.

        The recipient (``approver_user_id``/``chat_id``, plus ``connection_id`` for
        connected users) is snapshotted into the approval row.
        """
        warnings.warn("HitlBridge.start() is deprecated; invoke the graph yourself and use "
                      "TelegramApprovalBot.track() + deliver_pending()", DeprecationWarning, stacklevel=2)
        if action is not None:
            validate_action(action)  # before any graph run: no orphan thread for bad input
        if graph_input is None:
            if action is None:
                raise ValueError("either action or graph_input is required")
            graph_input = {"action": action}
        thread_id = str(uuid.uuid4())
        result = await self.graph.ainvoke(graph_input, thread_config(thread_id), durability="sync")
        interrupts = result.get("__interrupt__", ())
        if len(interrupts) != 1:
            raise StartError(f"expected exactly 1 interrupt, got {len(interrupts)}")
        intr = interrupts[0]
        value = intr.value
        digest = payload_digest(value)
        if isinstance(value, dict) and value.get("v") == 2:
            title, message, opts, version = (value["title"], value.get("message"),
                                             payload_options(value), 2)
        elif isinstance(value, dict) and "action" in value:
            title, message, opts, version = str(value["action"]), None, None, 1
        else:
            raise StartError("interrupt payload was not produced by request_approval()")
        approval = self.store.create(
            approver_user_id, chat_id, title, now=now, ttl_s=ttl_s, message=message, options=opts,
            thread_id=thread_id, interrupt_id=intr.id, payload_digest=digest, payload_version=version,
            connection_id=connection_id, recipient_external_user_id=recipient_external_user_id)
        audit("approval.created", approval_id=approval.approval_id, thread_id=thread_id,
              interrupt_id=intr.id, approver_user_id=approver_user_id, chat_id=chat_id,
              digest=digest, action_len=len(title), now=now)
        return approval

    async def is_live(self, a: Approval) -> bool:
        """True only if the stored interrupt is still pending on the stored thread
        AND its payload is exactly what was sent for approval."""
        if not (a.thread_id and a.interrupt_id and a.payload_digest):
            return False  # not linked to a graph -> never resume
        snapshot = await self.graph.aget_state(thread_config(a.thread_id))
        for intr in snapshot.interrupts:
            if intr.id == a.interrupt_id:
                return payload_digest(intr.value) == a.payload_digest
        return False

    async def has_recipient_literal(self, thread_id: str) -> bool:
        """True if the paused thread waits on a request_approval(recipient=...) interrupt."""
        snap = await self.graph.aget_state(thread_config(thread_id))
        return any(is_approval_payload(i.value) and i.value.get("v") == 3 for i in snap.interrupts)

    async def decide_request(self, req: DecisionRequest, now: int) -> DecideResult:
        """Channel-neutral: pre-check (interrupt still pending + digest) then the atomic decision.
        May raise sqlite3.OperationalError (e.g. locked)."""
        a = self.store.get(req.approval_id)
        d = None
        if a is not None:
            d = next((x for x in self.store.deliveries(req.approval_id) if x["channel"] == req.channel), None)
        # Pre-check only for an interaction that would otherwise win, so unauthorized or
        # mismatched clicks learn nothing about the graph.
        if (a is not None and d is not None and a.status == "pending" and d["actor_ref"] == str(req.actor_ref)
                and d["external_ref"] == str(req.delivery_ref) and a.expires_at > now):
            if not await self.is_live(a):
                audit("approval.decided", approval_id=req.approval_id, user_id=req.actor_ref,
                      decision=str(req.choice), outcome="stale", now=now)
                return DecideResult("stale", a, None)
        result = self.store.decide(req.approval_id, req.choice, channel=req.channel, actor_ref=req.actor_ref,
                                   delivery_ref=req.delivery_ref, now=now)
        audit("approval.decided", approval_id=req.approval_id, user_id=req.actor_ref,
              decision=(result.approval.selected_option_id if result.outcome == "won" and result.approval
                        else str(req.choice)), outcome=result.outcome, now=now)
        return DecideResult(result.outcome, result.approval, result)

    async def decide(self, *, approval_id: str, decision: int | str, user_id: int,
                     chat_id: int, message_id: int, now: int) -> DecideResult:
        """0.4-compatible Telegram-shaped wrapper around ``decide_request``."""
        return await self.decide_request(DecisionRequest(approval_id, decision, "telegram", str(user_id),
                                                         f"{chat_id}:{message_id}"), now)

    async def resume(self, approval: Approval, decision: str | None, now: int) -> ResumeResult:
        """Apply a WON decision. Always resumes BY ID with a non-empty, word-keyed dict
        that carries the approved payload digest (checked inside request_approval)."""
        assert approval.thread_id and approval.interrupt_id
        cfg = thread_config(approval.thread_id)
        option_id = approval.selected_option_id or decision
        if option_id is None:
            raise ValueError("approval has no selected option")
        value: dict[str, Any] = {
            "v": 2, "option_id": option_id, "approval_id": approval.approval_id,
            "approver_user_id": approval.approver_user_id,
            "external_user_id": approval.recipient_external_user_id,
            "payload_digest": approval.payload_digest}
        if option_id in ("approve", "reject"):
            value["decision"] = option_id  # readable by 0.2 graph code
        async with self._resume_lock:
            # Re-check under the lock: never send a resume to a thread that is no
            # longer waiting on this exact interrupt (e.g. resumed concurrently).
            if not await self.is_live(approval):
                snap = await self.graph.aget_state(cfg)
                if not snap.interrupts and not tuple(snap.next) and snap.values:
                    try:
                        self.store.mark_resumed(approval.approval_id, now)
                    except sqlite3.Error:
                        pass
                    return ResumeResult("resumed", snap.values.get("result"))
                audit("approval.resume", approval_id=approval.approval_id,
                      thread_id=approval.thread_id, status="not_applied", now=now)
                return ResumeResult("not_applied", None)
            try:
                # durability="sync": the consumed interrupt is checkpointed BEFORE the next
                # node (the side effect) starts, so a hard crash inside that node is
                # classified as PARTIAL, never as ready_to_resume (which would re-run it).
                await self.graph.ainvoke(Command(resume={approval.interrupt_id: value}), cfg,
                                         durability="sync")
            except Exception as e:
                logger.error("resume failed approval_id=%s: %s: %s",
                             approval.approval_id, type(e).__name__, redact(e))
                audit("approval.resume", approval_id=approval.approval_id,
                      thread_id=approval.thread_id, status="failed", now=now)
                return ResumeResult("failed", None)

            snapshot = await self.graph.aget_state(cfg)
            if any(i.id == approval.interrupt_id for i in snapshot.interrupts):
                logger.warning("resume NOT applied approval_id=%s (interrupt still pending)",
                               approval.approval_id)
                audit("approval.resume", approval_id=approval.approval_id,
                      thread_id=approval.thread_id, status="not_applied", now=now)
                return ResumeResult("not_applied", None)
            graph_result = snapshot.values.get("result")
            try:
                self.store.mark_resumed(approval.approval_id, now)
            except sqlite3.Error as e:  # graph DID resume; reconcile() will mark it later
                logger.warning("resumed but could not record resumed_at approval_id=%s: %s",
                               approval.approval_id, type(e).__name__)
            audit("approval.resume", approval_id=approval.approval_id, thread_id=approval.thread_id,
                  status="resumed", graph_result=graph_result, now=now)
            result = ResumeResult("resumed", graph_result)
        if self.on_resumed is not None:  # best effort; never affects the decision or recovery
            try:
                res = self.on_resumed(approval, result)
                if asyncio.iscoroutine(res):
                    await res
            except Exception as e:
                logger.warning("on_resumed failed approval_id=%s: %s: %s", approval.approval_id,
                               type(e).__name__, redact(e))
        return result

    # ---------- recovery ----------

    async def classify(self, a: Approval) -> UnresumedApproval:
        """Classify one decided-but-unconfirmed approval. Read-only."""
        if not (a.thread_id and a.interrupt_id and a.payload_digest):
            return UnresumedApproval(a, "unlinked")
        snapshot = await self.graph.aget_state(thread_config(a.thread_id))
        nxt = tuple(snapshot.next)
        if not snapshot.values and not nxt and not snapshot.interrupts:
            return UnresumedApproval(a, "unknown_thread")
        for intr in snapshot.interrupts:
            if intr.id == a.interrupt_id:
                ok = payload_digest(intr.value) == a.payload_digest
                return UnresumedApproval(a, "ready_to_resume" if ok else "digest_mismatch", nxt)
        if not nxt:
            return UnresumedApproval(a, "completed_unmarked", nxt)
        # Interrupt consumed but the graph did not finish: a later node may already
        # have run (or partly run) its side effect. Never continued automatically.
        return UnresumedApproval(a, "partial", nxt)

    async def find_unresumed(self) -> list[UnresumedApproval]:
        """Read-only recovery scan of decided approvals without resumed_at."""
        return [await self.classify(a) for a in self.store.find_unresumed()]

    async def reconcile(self, now: int) -> ReconcileReport:
        """Recover only the SAFE cases:

        * ready_to_resume    -> resume by interrupt ID (the action has not run yet)
        * completed_unmarked -> record resumed_at (the graph already finished)

        Everything else (partial, digest_mismatch, unknown_thread, unlinked) is
        reported in ``needs_attention`` and left untouched for an operator.
        """
        report = ReconcileReport()
        for item in await self.find_unresumed():
            a = item.approval
            fresh = self.store.get(a.approval_id)
            if fresh is None or fresh.resumed_at is not None:
                continue  # handled concurrently
            if item.state == "ready_to_resume" and a.decision is not None:
                res = await self.resume(fresh, a.decision, now)
                report.resumed.append((fresh, res))
            elif item.state == "completed_unmarked":
                if self.store.mark_resumed(a.approval_id, now):
                    report.marked.append(fresh)
            else:
                report.needs_attention.append(item)
            audit("approval.reconcile", approval_id=a.approval_id, thread_id=a.thread_id,
                  state=item.state, now=now)
        for u in report.partial:
            logger.warning("PARTIAL approval_id=%s thread_id=%s next=%s: interrupt consumed but the "
                           "graph did not finish; side effects may have run. NOT resumed "
                           "automatically - operator action required.",
                           u.approval.approval_id, u.approval.thread_id, list(u.graph_next))
        return report


__all__ = [
    "ApprovalDecision",
    "ApprovalResult",
    "DecideResult",
    "DeliveryPlan",
    "HitlBridge",
    "ReconcileReport",
    "RecoveryState",
    "ResumeResult",
    "UnresumedApproval",
    "request_approval",
    "thread_config",
]
