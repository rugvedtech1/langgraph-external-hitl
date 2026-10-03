"""Channel contract suite: behaviour every ApprovalChannel adapter must satisfy.

An adapter author implements a ``ChannelHarness`` (how to connect a recipient, how a human
interaction looks on that channel, how to make the next send fail) and runs every function
in ``CONTRACT_CHECKS`` against it (see tests/contract for Fake and Telegram harnesses).
Each check is ``async def check(h: ChannelHarness) -> None`` and raises AssertionError.
"""
from __future__ import annotations

import logging
from typing import Any, Protocol

from ..connections import ChannelConnections


class ChannelHarness(Protocol):
    service: Any            # HitlService bound to the adapter under test
    clock: dict             # {"t": int} controls the service clock
    secret: str             # a secret the adapter holds (e.g. bot token) - must never be logged

    def address_for(self, actor_ref: str) -> dict: ...
    async def start_job(self, thread_id: str, recipient: str) -> Any: ...   # track + ainvoke + deliver
    async def interact(self, approval: Any, choice: Any, *, actor_ref: str,
                       delivery_ref: str | None = None) -> str: ...        # -> outcome string
    def sent_count(self) -> int: ...
    def fail_next_send(self, *, permanent: bool, reason: str) -> None: ...
    def break_updates(self) -> None: ...
    def graph_result(self, thread_id: str) -> Any: ...


ACTOR, OTHER = "1001", "2002"


def _connect(h: ChannelHarness, recipient: str, actor: str):
    conns = ChannelConnections(h.service.store, h.service.channel.name)
    token, _ = conns.create_token(recipient, h.clock["t"])
    claim = conns.claim_token(token, actor, h.address_for(actor), h.clock["t"])
    assert claim is not None
    conn = conns.confirm_claim(claim.claim_id, actor, h.address_for(actor), h.clock["t"])
    assert conn is not None and conn.is_active
    return conns


async def _pending(h: ChannelHarness, job: str):
    _connect(h, "alice", ACTOR)
    r = await h.start_job(job, "alice")
    assert r.state == "pending" and len(r.sent) == 1, r
    return h.service.store.get(r.approvals[0].approval_id)


async def check_01_delivers(h):
    a = await _pending(h, "j1")
    assert h.sent_count() == 1 and a.delivery_state == "sent" and a.external_ref and a.channel == h.service.channel.name


async def check_02_correct_actor_decides_and_resumes(h):
    a = await _pending(h, "j2")
    assert await h.interact(a, 0, actor_ref=ACTOR) == "won"
    assert h.graph_result("j2") == "executed"
    assert h.service.store.get(a.approval_id).selected_option_id == "approve"


async def check_03_wrong_actor_rejected(h):
    a = await _pending(h, "j3")
    assert await h.interact(a, 0, actor_ref=OTHER) == "not_authorized"
    assert h.service.store.get(a.approval_id).status == "pending"


async def check_04_duplicate_rejected(h):
    a = await _pending(h, "j4")
    assert await h.interact(a, 1, actor_ref=ACTOR) == "won"
    assert await h.interact(a, 0, actor_ref=ACTOR) == "already_decided"
    assert h.service.store.get(a.approval_id).selected_option_id == "reject"
    assert h.graph_result("j4") == "cancelled"


async def check_05_foreign_delivery_rejected(h):
    a = await _pending(h, "j5")
    assert await h.interact(a, 0, actor_ref=ACTOR, delivery_ref="foreign") == "wrong_message"
    assert h.service.store.get(a.approval_id).status == "pending"


async def check_06_expired_rejected(h):
    a = await _pending(h, "j6")
    h.clock["t"] += 10 ** 6
    assert await h.interact(a, 0, actor_ref=ACTOR) == "expired"
    assert h.service.store.get(a.approval_id).status == "expired"


async def check_07_option_tampering_rejected(h):
    a = await _pending(h, "j7")
    assert await h.interact(a, 99, actor_ref=ACTOR) == "invalid_option"
    assert h.service.store.get(a.approval_id).status == "pending"


async def check_08_update_failure_does_not_block_decision(h):
    a = await _pending(h, "j8")
    h.break_updates()
    assert await h.interact(a, 0, actor_ref=ACTOR) == "won"
    assert h.graph_result("j8") == "executed" and h.service.store.get(a.approval_id).resumed_at is not None


async def check_09_failure_classification(h):
    _connect(h, "alice", ACTOR)
    h.fail_next_send(permanent=False, reason="network")
    r = await h.start_job("j9a", "alice")
    assert r.failed and not r.sent and h.service.store.get(r.failed[0]).delivery_state == "failed"
    r2 = await h.service.deliver_pending("j9a")                 # transient: retried
    assert len(r2.sent) == 1
    h.fail_next_send(permanent=True, reason="unauthorized")
    r3 = await h.start_job("j9b", "alice")
    assert r3.state == "channel_unavailable" and r3.failed
    r4 = await _blocked_case(h)
    assert r4.state == "blocked"


async def _blocked_case(h):
    _connect(h, "carol", OTHER)
    h.fail_next_send(permanent=True, reason="blocked")
    r = await h.start_job("j9c", "carol")
    a = r.approvals[0]
    assert a.status == "undeliverable"
    assert ChannelConnections(h.service.store, h.service.channel.name).get("carol").status == "blocked"
    return r


async def check_10_recovery_resends_once(h):
    _connect(h, "alice", ACTOR)
    h.fail_next_send(permanent=False, reason="network")
    await h.start_job("j10", "alice")
    before = h.sent_count()
    rep = await h.service.recover()
    assert h.sent_count() == before + 1 and any(d.sent for d in rep.deliveries)
    rep2 = await h.service.recover()
    assert h.sent_count() == before + 1 and not any(d.sent for d in rep2.deliveries)


async def check_11_connection_lifecycle(h):
    conns = _connect(h, "alice", ACTOR)
    r = await h.start_job("j11", "alice")
    a = h.service.store.get(r.approvals[0].approval_id)
    conns.disconnect("alice", h.clock["t"])
    assert await h.interact(a, 0, actor_ref=ACTOR) == "disconnected"
    r2 = await h.start_job("j11b", "alice")
    assert r2.state == "not_connected" and not r2.sent
    _connect(h, "alice", OTHER)                                  # reconnect with another account
    r3 = await h.service.deliver_pending("j11b")
    assert len(r3.sent) == 1


async def check_12_secrets_never_logged(h):
    import io
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    root = logging.getLogger("langgraph_external_hitl")
    root.addHandler(handler)
    old = root.level
    root.setLevel(logging.DEBUG)
    try:
        a = await _pending(h, "j12")
        await h.interact(a, 0, actor_ref=ACTOR)
        h.fail_next_send(permanent=False, reason="network")
        await h.start_job("j12b", "alice")
    finally:
        root.removeHandler(handler)
        root.setLevel(old)
    assert buf.getvalue() and h.secret not in buf.getvalue()


CONTRACT_CHECKS = [check_01_delivers, check_02_correct_actor_decides_and_resumes, check_03_wrong_actor_rejected,
                   check_04_duplicate_rejected, check_05_foreign_delivery_rejected, check_06_expired_rejected,
                   check_07_option_tampering_rejected, check_08_update_failure_does_not_block_decision,
                   check_09_failure_classification, check_10_recovery_resends_once, check_11_connection_lifecycle,
                   check_12_secrets_never_logged]
