"""Phase 1: schema v5 + RecipientRegistry."""
import sqlite3

import pytest

from langgraph_external_hitl import ApprovalStore
from langgraph_external_hitl.errors import (DuplicateRecipientError, InvalidRecipientNameError,
                                            SchemaVersionError, UnknownRecipientError)
from langgraph_external_hitl.recipients import RecipientRegistry, normalize_name


@pytest.fixture
def reg(tmp_path):
    s = ApprovalStore(str(tmp_path / "hitl.db"))
    yield RecipientRegistry(s, clock=lambda: 1000)
    s.close()


def test_create_resolve_list(reg):
    r = reg.create("Manager", display_name="Team lead")
    assert r.name == "manager" and r.recipient_id.startswith("rcp_") and r.display_name == "Team lead"
    assert reg.resolve("MANAGER").recipient_id == r.recipient_id          # case-folded lookup
    assert [x.name for x in reg.list()] == ["manager"]
    assert reg.by_id(r.recipient_id).name == "manager"


@pytest.mark.parametrize("bad", ["", "1abc", "has space", "a" * 33, "über", "-x", "all", "none", "any", None])
def test_invalid_names(reg, bad):
    with pytest.raises(InvalidRecipientNameError):
        reg.create(bad)


def test_valid_name_edges():
    assert normalize_name("a") == "a" and normalize_name("a" * 32) == "a" * 32
    assert normalize_name(" Ops_lead-2 ") == "ops_lead-2"


def test_duplicate_case_insensitive(reg):
    reg.create("manager")
    with pytest.raises(DuplicateRecipientError):
        reg.create("Manager")


def test_unknown_fails_closed(reg):
    with pytest.raises(UnknownRecipientError):
        reg.resolve("ghost")
    assert reg.get("ghost") is None


def test_rename_keeps_stable_id_and_remove(reg):
    r = reg.create("manager")
    r2 = reg.rename("manager", "lead")
    assert r2.recipient_id == r.recipient_id and r2.renamed_from == "manager"
    with pytest.raises(UnknownRecipientError):
        reg.resolve("manager")
    reg.create("other")
    with pytest.raises(DuplicateRecipientError):
        reg.rename("lead", "other")
    reg.remove("lead")
    with pytest.raises(UnknownRecipientError):
        reg.resolve("lead")
    assert reg.create("lead").recipient_id != r.recipient_id               # name reusable, new id


def test_rename_remove_refused_with_pending(reg):
    r = reg.create("manager")
    reg.store._insert_approval(title="x", message=None, options=None, now=0, ttl_s=10, recipient_id=r.recipient_id,
                               thread_id=None, interrupt_id=None, payload_digest=None, payload_version=2)
    with pytest.raises(ValueError):
        reg.rename("manager", "lead")
    with pytest.raises(ValueError):
        reg.remove("manager")
    assert reg.rename("manager", "lead", force=True).name == "lead"


def test_fresh_db_is_v5_with_registry(tmp_path):
    s = ApprovalStore(str(tmp_path / "hitl.db"))
    assert s.schema_version == 5
    cols = {r[1] for r in s._conn.execute("PRAGMA table_info(approvals)")}
    assert "recipient_name" in cols
    s.close()


def test_v4_to_v5_migration_keeps_data(tmp_path):
    p = str(tmp_path / "hitl.db")
    s = ApprovalStore(p)                         # build a v5 db, then downgrade it to a v4 shape
    a = s.create(7, 7, "x", now=0, ttl_s=10 ** 6)
    s._conn.execute("DROP TABLE recipients")
    s._conn.execute("ALTER TABLE approvals DROP COLUMN recipient_name")
    s._conn.execute("PRAGMA user_version = 4")
    s.close()
    s2 = ApprovalStore(p)
    assert s2.schema_version == 5 and s2.get(a.approval_id).title == "x"
    RecipientRegistry(s2).create("manager")
    s2.close()


def test_v4_to_v5_failure_rolls_back(tmp_path):
    p = str(tmp_path / "hitl.db")
    s = ApprovalStore(p)
    s._conn.execute("DROP TABLE recipients")
    s._conn.execute("ALTER TABLE approvals DROP COLUMN recipient_name")
    s._conn.execute("CREATE TABLE recipients (bogus INTEGER)")              # makes the v5 index fail
    s._conn.execute("PRAGMA user_version = 4")
    s.close()
    with pytest.raises(sqlite3.OperationalError):
        ApprovalStore(p)
    c = sqlite3.connect(p)
    assert c.execute("PRAGMA user_version").fetchone()[0] == 4
    c.close()


def test_newer_than_v5_refused(tmp_path):
    p = str(tmp_path / "hitl.db")
    c = sqlite3.connect(p); c.execute("PRAGMA user_version = 6"); c.close()
    with pytest.raises(SchemaVersionError):
        ApprovalStore(p)


def test_unknown_message_lists_registered_names(reg):
    with pytest.raises(UnknownRecipientError, match="no approvers are registered yet"):
        reg.resolve("manager")
    reg.create("testerwired")
    with pytest.raises(UnknownRecipientError, match=r"registered approvers: testerwired.*langgraph-hitl setup"):
        reg.resolve("manager")
