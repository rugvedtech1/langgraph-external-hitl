"""Part 6: one-time deep-link connection tokens and connection lifecycle (core, no network)."""
import re
import sqlite3
import threading

import pytest

from langgraph_external_hitl import ApprovalStore, ConnectionManager
from langgraph_external_hitl.connections import hash_token

NOW = 1_000_000


@pytest.fixture
def cm(tmp_path):
    s = ApprovalStore(str(tmp_path / "a.db"))
    yield ConnectionManager(s, bot_username="my_bot")
    s.close()


def connect(cm, ext="user-A", tg=111, chat=111, now=NOW, username="alice"):
    link = cm.create_link(ext, now)
    c = cm.claim(link.token, tg, chat, now + 1, username=username, first_name="Alice")
    assert c is not None
    return cm.confirm(c.claim_id, tg, chat, now + 2)


def test_link_format_and_token_properties(cm):
    link = cm.create_link("user-A", NOW)
    assert link.url == f"https://t.me/my_bot?start={link.token}"
    assert re.fullmatch(r"c_[A-Za-z0-9_-]{43}", link.token) and len(link.token) <= 64
    assert link.expires_at == NOW + 600 and link.token not in repr(link)
    rows = cm._conn.execute("SELECT token_hash FROM connection_tokens").fetchall()
    assert [r[0] for r in rows] == [hash_token(link.token)]           # only the hash is stored
    assert link.token not in str(cm._conn.execute("SELECT * FROM connection_tokens").fetchall())


def test_create_link_validation(cm):
    for bad in ["", "   ", "x" * 257]:
        with pytest.raises(ValueError):
            cm.create_link(bad, NOW)
    for ttl in (10, 3601):
        with pytest.raises(ValueError):
            cm.create_link("u", NOW, ttl_s=ttl)
    with pytest.raises(ValueError):
        ConnectionManager(cm.store).create_link("u", NOW)                # no bot username


def test_first_connection(cm):
    conn = connect(cm)
    assert (conn.external_user_id, conn.telegram_user_id, conn.chat_id, conn.status) == ("user-A", 111, 111, "active")
    assert conn.username == "alice" and cm.get("user-A") == conn


def test_expired_reused_wrong_malformed_tokens(cm):
    link = cm.create_link("user-A", NOW, ttl_s=60)
    assert cm.claim(link.token, 111, 111, NOW + 61) is None                # expired
    link = cm.create_link("user-A", NOW)
    c = cm.claim(link.token, 111, 111, NOW + 1)
    assert cm.confirm(c.claim_id, 111, 111, NOW + 2) is not None
    assert cm.claim(link.token, 111, 111, NOW + 3) is None                 # reused
    assert cm.confirm(c.claim_id, 111, 111, NOW + 3) is None               # confirm replay
    assert cm.claim("c_" + "A" * 43, 111, 111, NOW) is None                # wrong
    for bad in ["", "abc", "c_short", "c_" + "!" * 43, "c_" + "A" * 44, None]:
        assert cm.claim(bad, 111, 111, NOW) is None                        # malformed


def test_new_link_revokes_old_unused_link(cm):
    old = cm.create_link("user-A", NOW)
    new = cm.create_link("user-A", NOW + 5)
    assert cm.claim(old.token, 111, 111, NOW + 6) is None
    assert cm.claim(new.token, 111, 111, NOW + 6) is not None


def test_claimed_token_cannot_be_taken_over(cm):
    link = cm.create_link("user-A", NOW)
    c = cm.claim(link.token, 111, 111, NOW + 1)
    assert cm.claim(link.token, 999, 999, NOW + 2) is None                 # attacker re-claims
    assert cm.confirm(c.claim_id, 999, 999, NOW + 2) is None               # attacker confirms
    assert cm.confirm(c.claim_id, 111, 222, NOW + 2) is None               # other chat
    assert cm.claim(link.token, 111, 111, NOW + 3) is not None             # same user may re-claim


def test_confirm_after_expiry_and_cancel(cm):
    link = cm.create_link("user-A", NOW, ttl_s=60)
    c = cm.claim(link.token, 111, 111, NOW + 1)
    assert cm.confirm(c.claim_id, 111, 111, NOW + 61) is None
    link = cm.create_link("user-A", NOW + 100)
    c = cm.claim(link.token, 111, 111, NOW + 101)
    assert cm.cancel_claim(c.claim_id, 111, NOW + 102)
    assert cm.confirm(c.claim_id, 111, 111, NOW + 103) is None and cm.get("user-A") is None


def test_concurrent_confirm_only_one_wins(tmp_path):
    path = str(tmp_path / "a.db")
    s = ApprovalStore(path)
    cm = ConnectionManager(s, "my_bot")
    link = cm.create_link("user-A", NOW)
    c = cm.claim(link.token, 111, 111, NOW + 1)
    results, barrier = [], threading.Barrier(2)

    def go():
        st = ApprovalStore(path)
        try:
            barrier.wait()
            results.append(ConnectionManager(st).confirm(c.claim_id, 111, 111, NOW + 2))
        finally:
            st.close()
    ts = [threading.Thread(target=go) for _ in range(2)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert sum(r is not None for r in results) == 1
    assert s._conn.execute("SELECT COUNT(*) FROM channel_connections").fetchone()[0] == 1
    s.close()


def test_reconnect_same_and_different_account(cm):
    first = connect(cm)
    again = connect(cm, now=NOW + 100, username="alice2")
    assert again.connection_id == first.connection_id and again.username == "alice2"
    other = connect(cm, tg=222, chat=222, now=NOW + 200)
    assert other.connection_id != first.connection_id and cm.get("user-A").telegram_user_id == 222
    assert cm.get_by_id(first.connection_id).status == "replaced"


def test_strict_one_to_one_telegram_account(cm):
    a = connect(cm, ext="user-A", tg=111)
    b = connect(cm, ext="user-B", tg=111, now=NOW + 100)     # same Telegram account links to B
    assert cm.get("user-A") is None and cm.get("user-B").connection_id == b.connection_id
    assert cm.get_by_id(a.connection_id).status == "replaced"
    with pytest.raises(sqlite3.IntegrityError):              # DB-level guard as well
        cm._conn.execute("INSERT INTO channel_connections VALUES ('x','user-C','telegram','111','{}',NULL,NULL,'active',1,1,NULL,NULL)")


def test_disconnect_block_unblock_and_profile(cm):
    connect(cm)
    assert cm.mark_blocked(111, NOW + 10) and cm.get("user-A").status == "blocked"
    assert cm.mark_unblocked(111, NOW + 11) and cm.get("user-A").status == "active"
    cm.refresh_profile(111, "renamed", "Alicia", NOW + 12)
    c = cm.get("user-A")
    assert (c.username, c.first_name, c.telegram_user_id) == ("renamed", "Alicia", 111)
    assert cm.disconnect("user-A", NOW + 13).status == "disconnected" and cm.get("user-A") is None
    assert not cm.mark_blocked(111, NOW + 14) and cm.disconnect("user-A", NOW + 15) is None


def test_connection_persists_across_reopen(tmp_path):
    path = str(tmp_path / "a.db")
    s = ApprovalStore(path)
    connect(ConnectionManager(s, "my_bot"))
    s.close()
    s2 = ApprovalStore(path)
    assert ConnectionManager(s2).get("user-A").chat_id == 111
    s2.close()
