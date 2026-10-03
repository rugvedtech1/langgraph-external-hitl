"""Storage tests (Part 2 + Part 3 + Part 4 schema versioning). Stdlib only."""
import sqlite3
import threading
from pathlib import Path

import pytest

from langgraph_external_hitl import ApprovalStore, SchemaVersionError
from langgraph_external_hitl.store import SCHEMA_VERSION

NOW = 1_000_000
TTL = 60
USER, CHAT, MSG = 111, 111, 5


@pytest.fixture
def db(tmp_path: Path) -> str:
    return str(tmp_path / "approvals.db")  # a FILE: :memory: is private per connection


@pytest.fixture
def store(db: str):
    s = ApprovalStore(db)
    yield s
    s.close()


def make(store: ApprovalStore) -> str:
    a = store.create(USER, CHAT, "demo action", now=NOW, ttl_s=TTL)
    store.set_message_id(a.approval_id, MSG)
    return a.approval_id


# ---------- Part 2 behaviours ----------

def test_approve(store):
    aid = make(store)
    r = store.consume(aid, "approve", USER, CHAT, MSG, NOW + 1)
    assert r.outcome == "won" and r.changed
    assert r.approval.selected_option_id == "approve" and r.approval.decided_at == NOW + 1


def test_reject(store):
    aid = make(store)
    r = store.consume(aid, "reject", USER, CHAT, MSG, NOW + 1)
    assert r.outcome == "won" and r.approval.selected_option_id == "reject"


def test_double_click_second_loses(store):
    aid = make(store)
    assert store.consume(aid, "approve", USER, CHAT, MSG, NOW + 1).outcome == "won"
    r = store.consume(aid, "reject", USER, CHAT, MSG, NOW + 2)
    assert r.outcome == "already_decided" and not r.changed
    assert store.get(aid).selected_option_id == "approve"


def test_wrong_user(store):
    aid = make(store)
    r = store.consume(aid, "approve", 222, CHAT, MSG, NOW + 1)
    assert r.outcome == "not_authorized" and r.approval is None
    assert store.get(aid).status == "pending"


def test_unknown_id(store):
    make(store)
    assert store.consume("does-not-exist", "approve", USER, CHAT, MSG, NOW).outcome == "unknown"


def test_wrong_message_or_chat(store):
    aid = make(store)
    assert store.consume(aid, "approve", USER, CHAT, MSG + 1, NOW).outcome == "wrong_message"
    assert store.consume(aid, "approve", USER, CHAT + 1, MSG, NOW).outcome == "wrong_message"
    assert store.get(aid).status == "pending"


def test_message_id_not_yet_set_cannot_be_consumed(store):
    a = store.create(USER, CHAT, "x", now=NOW, ttl_s=TTL)
    assert store.consume(a.approval_id, "approve", USER, CHAT, MSG, NOW).outcome == "wrong_message"


def test_set_message_id_only_once(store):
    aid = make(store)
    assert store.set_message_id(aid, 999) is False
    assert store.get(aid).message_id == MSG


def test_expired(store):
    aid = make(store)
    r = store.consume(aid, "approve", USER, CHAT, MSG, NOW + TTL)
    assert r.outcome == "expired" and r.changed
    assert store.get(aid).status == "expired"
    r2 = store.consume(aid, "approve", USER, CHAT, MSG, NOW + TTL + 1)
    assert r2.outcome == "already_decided" and not r2.changed


def test_invalid_decision_rejected(store):
    aid = make(store)
    r = store.consume(aid, "maybe", USER, CHAT, MSG, NOW)
    assert r.outcome == "invalid_option" and store.get(aid).status == "pending"


@pytest.mark.parametrize("first", ["approve", "reject"])
def test_terminal_states_are_final(store, first):
    aid = make(store)
    store.consume(aid, first, USER, CHAT, MSG, NOW + 1)
    final = store.get(aid).status
    for d in ("approve", "reject"):
        assert store.consume(aid, d, USER, CHAT, MSG, NOW + 2).changed is False
    assert store.expire_if_due(aid, NOW + TTL + 10) is False
    assert store.get(aid).status == final


def test_expire_if_due_not_before_expiry(store):
    aid = make(store)
    assert store.expire_if_due(aid, NOW + TTL - 1) is False
    assert store.get(aid).status == "pending"


def test_status_check_constraint(store):
    aid = make(store)
    with pytest.raises(sqlite3.IntegrityError):
        store._conn.execute("UPDATE approvals SET status='bogus' WHERE approval_id=?", (aid,))


def test_restart_pending_approval_survives(db):
    s1 = ApprovalStore(db)
    aid = make(s1)
    s1.close()
    s2 = ApprovalStore(db)
    a = s2.get(aid)
    assert a is not None and a.status == "pending" and a.message_id == MSG
    assert s2.consume(aid, "approve", USER, CHAT, MSG, NOW + 1).outcome == "won"
    s2.close()
    s3 = ApprovalStore(db)
    assert s3.get(aid).selected_option_id == "approve"
    assert s3.consume(aid, "reject", USER, CHAT, MSG, NOW + 2).outcome == "already_decided"
    s3.close()


def test_concurrent_approve_vs_reject_exactly_one_wins(db):
    setup = ApprovalStore(db)
    for _ in range(100):
        aid = make(setup)
        barrier = threading.Barrier(2)
        results: dict[str, str] = {}
        errors: list[BaseException] = []

        def racer(decision: str) -> None:
            s = ApprovalStore(db)   # own connection per thread
            try:
                barrier.wait()
                results[decision] = s.consume(aid, decision, USER, CHAT, MSG, NOW + 1).outcome
            except BaseException as e:
                errors.append(e)
            finally:
                s.close()

        threads = [threading.Thread(target=racer, args=(d,)) for d in ("approve", "reject")]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors, errors
        assert sorted(results.values()) == ["already_decided", "won"], results
        winner = next(d for d, o in results.items() if o == "won")
        final = setup.get(aid)
        assert final.selected_option_id == winner
        assert final.decided_at == NOW + 1
    setup.close()


# ---------- Part 3 behaviours ----------

def test_part3_columns_stored(store):
    a = store.create(USER, CHAT, "x", now=NOW, ttl_s=TTL,
                     thread_id="t1", interrupt_id="i1", payload_digest="d1")
    got = store.get(a.approval_id)
    assert (got.thread_id, got.interrupt_id, got.payload_digest, got.resumed_at) == ("t1", "i1", "d1", None)


def test_mark_resumed_only_once_and_only_after_decision(store):
    aid = make(store)
    assert store.mark_resumed(aid, NOW) is False
    store.consume(aid, "approve", USER, CHAT, MSG, NOW + 1)
    assert store.mark_resumed(aid, NOW + 2) is True
    assert store.mark_resumed(aid, NOW + 3) is False
    assert store.get(aid).resumed_at == NOW + 2


# ---------- Migration (Part 2 and Part 3 databases) ----------

PART2_DDL = """CREATE TABLE approvals (
    approval_id TEXT PRIMARY KEY, approver_user_id INTEGER NOT NULL, chat_id INTEGER NOT NULL,
    message_id INTEGER, action TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending','approved','rejected','expired')),
    created_at INTEGER NOT NULL, expires_at INTEGER NOT NULL, decided_at INTEGER)"""


def test_migrates_existing_part2_database(db):
    old = sqlite3.connect(db)
    old.execute(PART2_DDL)
    old.execute("INSERT INTO approvals VALUES ('old1', 1, 1, 8, 'a', 'approved', 1, 2, 2)")
    old.commit(); old.close()
    s = ApprovalStore(db)
    row = s.get("old1")
    assert row.selected_option_id == "approve" and row.thread_id is None and row.resumed_at is None
    assert s.schema_version == SCHEMA_VERSION == 5
    s.close()


def test_migrates_existing_part3_database(db):
    old = sqlite3.connect(db)
    old.execute(PART2_DDL)
    for col, typ in [("thread_id", "TEXT"), ("interrupt_id", "TEXT"),
                     ("payload_digest", "TEXT"), ("resumed_at", "INTEGER")]:
        old.execute(f"ALTER TABLE approvals ADD COLUMN {col} {typ}")
    old.execute("INSERT INTO approvals VALUES ('p3', 1, 1, 9, 'a', 'approved', 1, 2, 2, 't', 'i', 'd', 3)")
    old.commit(); old.close()
    s = ApprovalStore(db)
    row = s.get("p3")
    assert (row.thread_id, row.interrupt_id, row.payload_digest, row.resumed_at) == ("t", "i", "d", 3)
    assert s.schema_version == 5
    s.close()


def test_reopen_is_idempotent(db):
    ApprovalStore(db).close()
    s = ApprovalStore(db)
    assert s.schema_version == 5
    s.close()


def test_newer_schema_is_refused(db):
    c = sqlite3.connect(db)
    c.execute("PRAGMA user_version = 99")
    c.close()
    with pytest.raises(SchemaVersionError):
        ApprovalStore(db)
