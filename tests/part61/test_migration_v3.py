"""Part 6.1 / T14: schema v2 -> v3 migration, back-fill, duplicate detection, rollback."""
import sqlite3

import pytest

from langgraph_external_hitl import ApprovalStore, SchemaVersionError
from langgraph_external_hitl.store import SCHEMA_VERSION
from langgraph_external_hitl.store import _APPROVALS_V2, _CONNECTIONS_V2, PRESET_JSON


def make_v2(path, rows):
    c = sqlite3.connect(path)
    c.execute(_APPROVALS_V2)
    for stmt in _CONNECTIONS_V2.split(";"):
        if stmt.strip():
            c.execute(stmt)
    for r in rows:
        c.execute("INSERT INTO approvals (approval_id, connection_id, recipient_external_user_id,"
                  " approver_user_id, chat_id, message_id, title, options_json, selected_option_id, status,"
                  " created_at, expires_at, decided_at, thread_id, interrupt_id, payload_digest, resumed_at)"
                  " VALUES (?,?,?,1,1,?,?,?,?,?,?,?,?,?,?,?,?)", r)
    c.execute("PRAGMA user_version = 2")
    c.commit()
    c.close()


ROWS = [
    # approval_id, conn, recipient, message_id, title, options, sel, status, created, expires, decided, thread, intr, digest, resumed
    ("p1", "c1", "user-A", 7, "pending sent", PRESET_JSON, None, "pending", 10, 10**10, None, "t1", "i1", "d", None),
    ("p2", "c1", "user-A", None, "pending unsent", PRESET_JSON, None, "pending", 11, 10**10, None, "t2", "i2", "d", None),
    ("d1", "c1", "user-A", 8, "done", PRESET_JSON, "approve", "decided", 12, 10**10, 13, "t3", "i3", "d", 14),
    ("l1", None, None, 9, "legacy allowlist", PRESET_JSON, None, "pending", 15, 10**10, None, "t4", "i4", "d", None),
    ("n1", None, None, None, "no thread", PRESET_JSON, None, "pending", 16, 10**10, None, None, None, None, None),
]


def test_v2_to_v3_backfill_and_preservation(tmp_path):
    p = str(tmp_path / "a.db")
    make_v2(p, ROWS)
    s = ApprovalStore(p)
    assert s.schema_version == SCHEMA_VERSION == 5
    p1, p2, d1, l1, n1 = (s.get(x) for x in ("p1", "p2", "d1", "l1", "n1"))
    assert (p1.delivered_at, p1.delivery_state, p1.delivery_attempts) == (10, "sent", 1)
    assert (p2.delivered_at, p2.delivery_state, p2.delivery_attempts) == (None, "reserved", 0)
    assert d1.selected_option_id == "approve" and d1.resumed_at == 14 and l1.title == "legacy allowlist"
    assert n1.thread_id is None
    t1, t2, t3 = (s.get_thread(x) for x in ("t1", "t2", "t3"))
    assert (t1.status, t1.recipient_external_user_id) == ("active", "user-A") and t2.status == "active"
    assert t3.status == "completed" and t3.completion_notified_at is not None      # no spurious callbacks
    assert s.get_thread("t4") is None                                               # unknown recipient
    s.close()
    s2 = ApprovalStore(p)                                                           # idempotent reopen
    assert s2.schema_version == 5 and s2.get("p1").delivery_state == "sent"
    s2.close()


def test_v1_and_v0_chain_to_v3(tmp_path):
    p = str(tmp_path / "a.db")
    c = sqlite3.connect(p)
    c.execute("CREATE TABLE approvals (approval_id TEXT PRIMARY KEY, approver_user_id INTEGER NOT NULL,"
              " chat_id INTEGER NOT NULL, message_id INTEGER, action TEXT NOT NULL, status TEXT NOT NULL,"
              " created_at INTEGER NOT NULL, expires_at INTEGER NOT NULL, decided_at INTEGER)")
    c.execute("INSERT INTO approvals VALUES ('old', 1, 1, 5, 'x', 'approved', 1, 2, 2)")
    c.commit(); c.close()
    s = ApprovalStore(p)
    a = s.get("old")
    assert s.schema_version == 5 and a.selected_option_id == "approve" and a.delivered_at == 1
    s.close()


def test_duplicate_thread_interrupt_fails_closed_and_rolls_back(tmp_path):
    p = str(tmp_path / "a.db")
    dup = list(ROWS[0]); dup[0] = "p1-dup"
    make_v2(p, [ROWS[0], tuple(dup)])
    with pytest.raises(SchemaVersionError, match="duplicate"):
        ApprovalStore(p)
    c = sqlite3.connect(p)
    assert c.execute("PRAGMA user_version").fetchone()[0] == 2
    cols = {r[1] for r in c.execute("PRAGMA table_info(approvals)")}
    tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "delivered_at" not in cols and "hitl_threads" not in tables
    assert c.execute("SELECT COUNT(*) FROM approvals").fetchone()[0] == 2
    c.close()


def test_newer_than_v3_refused(tmp_path):
    p = str(tmp_path / "a.db")
    c = sqlite3.connect(p); c.execute("PRAGMA user_version = 6"); c.close()
    with pytest.raises(SchemaVersionError):
        ApprovalStore(p)
