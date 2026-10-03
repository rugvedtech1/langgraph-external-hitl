"""Part 5 / T15-T16: property-based tests (Hypothesis)."""
import sqlite3
import tempfile
from pathlib import Path

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

from langgraph_external_hitl import ApprovalStore
from langgraph_external_hitl.telegram._handlers import parse_callback_data

USERS = st.sampled_from([1, 2, 3])
TIMES = st.integers(min_value=0, max_value=400)


class StoreMachine(RuleBasedStateMachine):
    """Random interleavings of create / consume / expire / mark_resumed / reopen."""

    def __init__(self):
        super().__init__()
        self.dir = tempfile.mkdtemp()
        self.path = str(Path(self.dir) / "a.db")
        self.store = ApprovalStore(self.path)
        self.ids: list[str] = []
        self.wins: dict[str, int] = {}
        self.final: dict[str, str] = {}      # first terminal status seen per approval

    @rule(user=USERS, ttl=st.integers(min_value=1, max_value=200), now=TIMES)
    def create(self, user, ttl, now):
        a = self.store.create(user, user, "act", now=now, ttl_s=ttl)
        self.store.set_message_id(a.approval_id, 5)
        self.ids.append(a.approval_id)

    @precondition(lambda self: self.ids)
    @rule(data=st.data(), user=USERS, decision=st.sampled_from(["approve", "reject"]), now=TIMES,
          msg=st.sampled_from([5, 6]))
    def consume(self, data, user, decision, now, msg):
        aid = data.draw(st.sampled_from(self.ids))
        r = self.store.consume(aid, decision, user, user, msg, now)
        if r.outcome == "won":
            self.wins[aid] = self.wins.get(aid, 0) + 1

    @precondition(lambda self: self.ids)
    @rule(data=st.data(), now=TIMES)
    def expire(self, data, now):
        self.store.expire_if_due(data.draw(st.sampled_from(self.ids)), now)

    @precondition(lambda self: self.ids)
    @rule(data=st.data(), now=TIMES)
    def mark(self, data, now):
        self.store.mark_resumed(data.draw(st.sampled_from(self.ids)), now)

    @rule()
    def reopen(self):
        self.store.close()
        self.store = ApprovalStore(self.path)

    @invariant()
    def check_store_invariants(self):
        for aid in self.ids:
            a = self.store.get(aid)
            assert self.wins.get(aid, 0) <= 1                                   # at most one winner
            if a.status != "pending":
                assert self.final.setdefault(aid, (a.status, a.selected_option_id)) == (a.status, a.selected_option_id)         # terminal is final
                assert a.decided_at is not None
            if a.resumed_at is not None:
                assert a.status == "decided"                     # only after a decision
            if a.status == "decided":
                assert self.wins.get(aid, 0) == 1

    def teardown(self):
        self.store.close()


TestStoreMachine = StoreMachine.TestCase
TestStoreMachine.settings = settings(max_examples=150, stateful_step_count=30, deadline=None,
                                     suppress_health_check=[HealthCheck.too_slow])


@settings(max_examples=2000, deadline=None)
@given(st.text() | st.binary().map(lambda b: b.decode("latin-1")))
def test_parse_callback_data_never_crashes_and_is_strict(data):
    r = parse_callback_data(data)
    if r is not None:
        decision, aid = r
        assert decision in ("approve", "reject")
        assert data == ("a:" if decision == "approve" else "r:") + aid
        assert 0 < len(aid) <= 64


@given(st.text(min_size=1, max_size=64).filter(lambda s: s.strip()))
def test_parse_roundtrip(aid):
    assert parse_callback_data(f"a:{aid}") == ("approve", aid)
    assert parse_callback_data(f"r:{aid}") == ("reject", aid)
