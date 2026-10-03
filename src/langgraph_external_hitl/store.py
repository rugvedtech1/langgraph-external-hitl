"""SQLite storage (schema v4, Part 8.5): channel-neutral approvals, deliveries, connections.

approvals            what must be decided (no channel identifiers)
approval_deliveries  one row per (approval, channel): actor allowed to decide, address, external_ref
channel_connections  recipient_id -> (channel, actor_ref, address) bindings
connection_tokens    one-time connection links (SHA-256 only)
hitl_threads         application-owned LangGraph threads (recipient pinning, completion)

No Telegram or LangGraph code here. Telegram-shaped helpers (``create``, ``consume``,
``set_message_id``...) are kept as compatibility shims over the generic API.
"""
from __future__ import annotations

import json
import logging
import os
import re
import secrets
import sqlite3
import stat
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Literal

from .errors import ActionTooLongError, RecipientMismatchError, SchemaVersionError
from .options import (APPROVE_REJECT, MAX_MESSAGE_TOTAL, PRESET_JSON, ApprovalOption,
                      options_from_json, options_to_json, validate_request)

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 5
DEFAULT_DELIVERY_LEASE_S = 60
THREAD_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
MAX_ACTION_LENGTH = MAX_MESSAGE_TOTAL  # kept for 0.2 compatibility
LEGACY_CHANNEL = "telegram"           # channel of pre-v4 data and of the Telegram-shaped shims

Status = Literal["pending", "decided", "expired", "cancelled", "undeliverable"]
Outcome = Literal["won", "unknown", "not_authorized", "wrong_message", "already_decided",
                  "expired", "invalid_option", "disconnected"]

_APPROVALS_V2 = """
CREATE TABLE approvals (
    approval_id                 TEXT    PRIMARY KEY,
    connection_id               TEXT,
    recipient_external_user_id  TEXT,
    approver_user_id            INTEGER NOT NULL,
    chat_id                     INTEGER NOT NULL,
    message_id                  INTEGER,
    title                       TEXT    NOT NULL,
    message                     TEXT,
    options_json                TEXT    NOT NULL,
    selected_option_id          TEXT,
    status                      TEXT    NOT NULL DEFAULT 'pending'
                                CHECK (status IN ('pending','decided','expired','cancelled','undeliverable')),
    created_at                  INTEGER NOT NULL,
    expires_at                  INTEGER NOT NULL,
    decided_at                  INTEGER,
    thread_id                   TEXT,
    interrupt_id                TEXT,
    payload_digest              TEXT,
    payload_version             INTEGER NOT NULL DEFAULT 2,
    resumed_at                  INTEGER
)
"""

_CONNECTIONS_V2 = """
CREATE TABLE IF NOT EXISTS telegram_connections (
    connection_id     TEXT    PRIMARY KEY,
    external_user_id  TEXT    NOT NULL,
    telegram_user_id  INTEGER NOT NULL,
    chat_id           INTEGER NOT NULL,
    username          TEXT,
    first_name        TEXT,
    status            TEXT    NOT NULL CHECK (status IN ('active','blocked','disconnected','replaced')),
    connected_at      INTEGER NOT NULL,
    updated_at        INTEGER NOT NULL,
    blocked_at        INTEGER,
    ended_at          INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_conn_user_live ON telegram_connections(external_user_id)
    WHERE status IN ('active','blocked');
CREATE UNIQUE INDEX IF NOT EXISTS ux_conn_tg_live ON telegram_connections(telegram_user_id)
    WHERE status IN ('active','blocked');
CREATE TABLE IF NOT EXISTS connection_tokens (
    token_hash        TEXT    PRIMARY KEY,
    external_user_id  TEXT    NOT NULL,
    created_at        INTEGER NOT NULL,
    expires_at        INTEGER NOT NULL,
    claim_id          TEXT    UNIQUE,
    claimed_by_tg_id  INTEGER,
    claimed_chat_id   INTEGER,
    claimed_username  TEXT,
    claimed_first_name TEXT,
    claimed_at        INTEGER,
    consumed_at       INTEGER,
    revoked_at        INTEGER
);
CREATE INDEX IF NOT EXISTS ix_tokens_user ON connection_tokens(external_user_id);
"""

_V3_DELIVERY_COLUMNS = {
    "delivery_state": "TEXT CHECK (delivery_state IN ('reserved','sent','failed'))",
    "delivery_claimed_at": "INTEGER",
    "delivered_at": "INTEGER",
    "delivery_attempts": "INTEGER NOT NULL DEFAULT 0",
}

_V3_DDL = """
CREATE UNIQUE INDEX IF NOT EXISTS ux_approvals_thread_interrupt
    ON approvals(thread_id, interrupt_id) WHERE thread_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS hitl_threads (
    thread_id                   TEXT    PRIMARY KEY,
    recipient_external_user_id  TEXT    NOT NULL,
    status                      TEXT    NOT NULL DEFAULT 'active'
                                CHECK (status IN ('active','completed','cancelled')),
    created_at                  INTEGER NOT NULL,
    updated_at                  INTEGER NOT NULL,
    completed_at                INTEGER,
    completion_notified_at      INTEGER
);
CREATE INDEX IF NOT EXISTS ix_hitl_threads_status ON hitl_threads(status)
"""

_V1_PART3_COLUMNS = {"thread_id": "TEXT", "interrupt_id": "TEXT", "payload_digest": "TEXT",
                     "resumed_at": "INTEGER"}



_V4_DDL = """
CREATE TABLE approvals_v4 (
    approval_id         TEXT    PRIMARY KEY,
    recipient_id        TEXT,
    title               TEXT    NOT NULL,
    message             TEXT,
    options_json        TEXT    NOT NULL,
    selected_option_id  TEXT,
    status              TEXT    NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending','decided','expired','cancelled','undeliverable')),
    created_at          INTEGER NOT NULL,
    expires_at          INTEGER NOT NULL,
    decided_at          INTEGER,
    decided_delivery_id INTEGER,
    thread_id           TEXT,
    interrupt_id        TEXT,
    payload_digest      TEXT,
    payload_version     INTEGER NOT NULL DEFAULT 2,
    resumed_at          INTEGER
);
CREATE TABLE approval_deliveries (
    delivery_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    approval_id   TEXT    NOT NULL REFERENCES approvals(approval_id),
    channel       TEXT    NOT NULL,
    connection_id TEXT,
    actor_ref     TEXT    NOT NULL,
    address_json  TEXT    NOT NULL DEFAULT '{}',
    external_ref  TEXT,
    state         TEXT    NOT NULL DEFAULT 'reserved' CHECK (state IN ('reserved','sent','failed')),
    claimed_at    INTEGER,
    delivered_at  INTEGER,
    attempts      INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT,
    created_at    INTEGER NOT NULL,
    UNIQUE (approval_id, channel)
);
CREATE TABLE channel_connections (
    connection_id TEXT    PRIMARY KEY,
    recipient_id  TEXT    NOT NULL,
    channel       TEXT    NOT NULL,
    actor_ref     TEXT    NOT NULL,
    address_json  TEXT    NOT NULL DEFAULT '{}',
    handle        TEXT,
    label         TEXT,
    status        TEXT    NOT NULL CHECK (status IN ('active','blocked','disconnected','replaced')),
    connected_at  INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL,
    blocked_at    INTEGER,
    ended_at      INTEGER
);
CREATE TABLE connection_tokens_v4 (
    token_hash           TEXT    PRIMARY KEY,
    channel              TEXT    NOT NULL,
    recipient_id         TEXT    NOT NULL,
    created_at           INTEGER NOT NULL,
    expires_at           INTEGER NOT NULL,
    claim_id             TEXT    UNIQUE,
    claimed_actor_ref    TEXT,
    claimed_address_json TEXT,
    claimed_handle       TEXT,
    claimed_label        TEXT,
    claimed_at           INTEGER,
    consumed_at          INTEGER,
    revoked_at           INTEGER
)
"""

_V5_DDL = """
CREATE TABLE IF NOT EXISTS recipients (
    recipient_id  TEXT    PRIMARY KEY,
    name          TEXT    NOT NULL,
    name_key      TEXT    NOT NULL,
    display_name  TEXT,
    status        TEXT    NOT NULL DEFAULT 'active' CHECK (status IN ('active','removed')),
    renamed_from  TEXT,
    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_recipients_name_active ON recipients(name_key) WHERE status = 'active'
"""

_V4_INDEXES = """
CREATE UNIQUE INDEX ux_approvals_thread_interrupt ON approvals(thread_id, interrupt_id) WHERE thread_id IS NOT NULL;
CREATE INDEX ix_deliveries_approval ON approval_deliveries(approval_id);
CREATE UNIQUE INDEX ux_conn_recipient_live ON channel_connections(recipient_id, channel)
    WHERE status IN ('active','blocked');
CREATE UNIQUE INDEX ux_conn_actor_live ON channel_connections(channel, actor_ref)
    WHERE status IN ('active','blocked');
CREATE INDEX ix_tokens_recipient ON connection_tokens(recipient_id)
"""

def created_now() -> int:
    import time
    return int(time.time())


def validate_action(action: str) -> None:
    """0.2-compatible check for a legacy single-string action."""
    if not isinstance(action, str) or not action.strip():
        raise ActionTooLongError("action must be a non-empty string")
    if len(action) > MAX_ACTION_LENGTH:
        raise ActionTooLongError(f"action is {len(action)} characters; maximum is {MAX_ACTION_LENGTH}")


def _prepare_db_file(path: str) -> None:
    """Create a new database file as 0600; warn if an existing one is group/other accessible."""
    if path == ":memory:" or path.startswith("file:") or os.name != "posix":
        return
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(fd)
        return
    except FileExistsError:
        pass
    mode = stat.S_IMODE(os.stat(path).st_mode)
    if mode & 0o077:
        logger.warning("approvals database %s has permissions %s; recommended 0600 "
                       "(chmod 600 %s)", path, oct(mode), path)



def _json(d: dict[str, Any] | None) -> str:
    return json.dumps(d or {}, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class Approval:
    """An approval and (flattened) its primary delivery. Channel identifiers live on the delivery."""
    approval_id: str
    recipient_id: str | None
    title: str
    message: str | None
    options_json: str
    selected_option_id: str | None
    status: Status
    created_at: int
    expires_at: int
    decided_at: int | None
    decided_delivery_id: int | None
    thread_id: str | None
    interrupt_id: str | None
    payload_digest: str | None
    payload_version: int
    resumed_at: int | None
    recipient_name: str | None = None       # friendly registry name from the payload (v5)
    # primary delivery (None until a delivery exists)
    delivery_id: int | None = None
    channel: str | None = None
    connection_id: str | None = None
    actor_ref: str | None = None
    address_json: str | None = None
    external_ref: str | None = None
    delivery_state: str | None = None
    delivery_claimed_at: int | None = None
    delivered_at: int | None = None
    delivery_attempts: int = 0

    @property
    def options(self) -> tuple[ApprovalOption, ...]:
        return options_from_json(self.options_json)

    @property
    def selected_option(self) -> ApprovalOption | None:
        return next((o for o in self.options if o.id == self.selected_option_id), None)

    @property
    def address(self) -> dict[str, Any]:
        return json.loads(self.address_json or "{}")

    @property
    def decision(self) -> str | None:
        return self.selected_option_id if self.status == "decided" else None

    # ---- 0.4 compatibility (Telegram-shaped views of the primary delivery) ----
    @property
    def action(self) -> str:
        return self.title

    @property
    def recipient_external_user_id(self) -> str | None:
        return self.recipient_id

    @property
    def approver_user_id(self) -> int | None:
        a = self.actor_ref
        return int(a) if a is not None and a.lstrip("-").isdigit() else None

    @property
    def chat_id(self) -> int | None:
        return self.address.get("chat_id")

    @property
    def message_id(self) -> int | None:
        if not self.external_ref:
            return None
        tail = self.external_ref.rsplit(":", 1)[-1]
        return int(tail) if tail.lstrip("-").isdigit() else None


@dataclass(frozen=True)
class ConsumeResult:
    outcome: Outcome
    approval: Approval | None  # current row after the attempt (None if unknown/unauthorized)
    changed: bool               # True only if THIS call changed the row


@dataclass(frozen=True)
class ThreadRecord:
    thread_id: str
    recipient_external_user_id: str
    status: Literal["active", "completed", "cancelled"]
    created_at: int
    updated_at: int
    completed_at: int | None = None
    completion_notified_at: int | None = None


def validate_thread_id(thread_id: str) -> None:
    if not isinstance(thread_id, str) or not THREAD_ID_RE.match(thread_id):
        raise ValueError("thread_id must be 1-128 chars of A-Z a-z 0-9 _ . : -")


_SELECT = """
SELECT a.*, d.delivery_id, d.channel, d.connection_id, d.actor_ref, d.address_json, d.external_ref,
       d.state AS delivery_state, d.claimed_at AS delivery_claimed_at, d.delivered_at,
       COALESCE(d.attempts, 0) AS delivery_attempts
  FROM approvals a
  LEFT JOIN approval_deliveries d
    ON d.delivery_id = (SELECT MIN(delivery_id) FROM approval_deliveries x WHERE x.approval_id = a.approval_id)
"""


_SELECT_CHANNEL = _SELECT.replace(
    "ON d.delivery_id = (SELECT MIN(delivery_id) FROM approval_deliveries x WHERE x.approval_id = a.approval_id)",
    "ON d.approval_id = a.approval_id AND d.channel = ?")


class ApprovalStore:
    """Durable, channel-neutral approval store. The database path is required.

    New database files are created with permissions 0600. One connection per store.
    """

    def __init__(self, path: str, timeout: float = 5.0) -> None:
        _prepare_db_file(path)
        self._conn = sqlite3.connect(path, autocommit=True, timeout=timeout)
        self._conn.row_factory = sqlite3.Row
        try:
            self._migrate()
        except BaseException:
            self._conn.close()  # never leak the connection when opening fails
            raise

    # ---------- schema ----------

    def _table_exists(self, name: str) -> bool:
        return self._conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                                  (name,)).fetchone() is not None

    def _migrate(self) -> None:
        version = self._conn.execute("PRAGMA user_version").fetchone()[0]
        if version > SCHEMA_VERSION:
            raise SchemaVersionError(
                f"approvals database schema version {version} is newer than "
                f"supported version {SCHEMA_VERSION}; upgrade langgraph-external-hitl")
        if version == SCHEMA_VERSION:
            return
        self._conn.execute("BEGIN IMMEDIATE")  # whole migration chain is one transaction
        try:
            if version < 2:
                if self._table_exists("approvals"):
                    self._migrate_v1_to_v2()
                else:
                    self._conn.execute(_APPROVALS_V2)
                for stmt in _CONNECTIONS_V2.split(";"):
                    if stmt.strip():
                        self._conn.execute(stmt)
            if version < 3:
                self._migrate_v2_to_v3()
            if version < 4:
                self._migrate_v3_to_v4()
            self._migrate_v4_to_v5()
            self._conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            self._conn.execute("COMMIT")
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise

    def _migrate_v2_to_v3(self) -> None:
        """Delivery columns, (thread_id, interrupt_id) uniqueness, hitl_threads (Part 6.1)."""
        dupes = self._conn.execute(
            "SELECT thread_id, interrupt_id, COUNT(*) AS n FROM approvals WHERE thread_id IS NOT NULL"
            " GROUP BY thread_id, interrupt_id HAVING COUNT(*) > 1").fetchall()
        if dupes:
            raise SchemaVersionError(
                f"cannot migrate to schema v3: {len(dupes)} duplicate (thread_id, interrupt_id) "
                "approval rows; resolve them manually first")
        cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(approvals)")}
        for name, sql_type in _V3_DELIVERY_COLUMNS.items():
            if name not in cols:
                self._conn.execute(f"ALTER TABLE approvals ADD COLUMN {name} {sql_type}")
        for stmt in _V3_DDL.split(";"):
            if stmt.strip():
                self._conn.execute(stmt)
        # back-fill: rows with a recorded message were delivered
        self._conn.execute(
            "UPDATE approvals SET delivered_at = COALESCE(delivered_at, created_at),"
            " delivery_state = 'sent', delivery_attempts = MAX(delivery_attempts, 1)"
            " WHERE message_id IS NOT NULL AND delivered_at IS NULL")
        # back-fill tracked threads from connected (recipient-linked) approvals
        self._conn.execute(
            """
            INSERT OR IGNORE INTO hitl_threads (thread_id, recipient_external_user_id, status,
                created_at, updated_at, completed_at, completion_notified_at)
            SELECT thread_id, MIN(recipient_external_user_id),
                   CASE WHEN SUM(status = 'pending' OR (status = 'decided' AND resumed_at IS NULL)) > 0
                        THEN 'active' ELSE 'completed' END,
                   MIN(created_at), MAX(COALESCE(decided_at, created_at)),
                   CASE WHEN SUM(status = 'pending' OR (status = 'decided' AND resumed_at IS NULL)) > 0
                        THEN NULL ELSE MAX(COALESCE(resumed_at, decided_at, created_at)) END,
                   CASE WHEN SUM(status = 'pending' OR (status = 'decided' AND resumed_at IS NULL)) > 0
                        THEN NULL ELSE MAX(COALESCE(resumed_at, decided_at, created_at)) END
              FROM approvals
             WHERE thread_id IS NOT NULL AND recipient_external_user_id IS NOT NULL
             GROUP BY thread_id
            HAVING COUNT(DISTINCT recipient_external_user_id) = 1
            """)

    def _migrate_v1_to_v2(self) -> None:
        """Rebuild a Part 2/3/4/5 (v0/v1) approvals table into the v2 layout."""
        cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(approvals)")}
        if "options_json" in cols:  # already v2 layout (e.g. interrupted earlier run)
            return
        for name, sql_type in _V1_PART3_COLUMNS.items():
            if name not in cols:
                self._conn.execute(f"ALTER TABLE approvals ADD COLUMN {name} {sql_type}")
        self._conn.execute("ALTER TABLE approvals RENAME TO approvals_v1")
        self._conn.execute(_APPROVALS_V2)
        self._conn.execute(
            """
            INSERT INTO approvals (approval_id, connection_id, recipient_external_user_id,
                approver_user_id, chat_id, message_id, title, message, options_json,
                selected_option_id, status, created_at, expires_at, decided_at, thread_id,
                interrupt_id, payload_digest, payload_version, resumed_at)
            SELECT approval_id, NULL, NULL, approver_user_id, chat_id, message_id, action, NULL, ?,
                   CASE status WHEN 'approved' THEN 'approve' WHEN 'rejected' THEN 'reject' END,
                   CASE WHEN status IN ('approved','rejected') THEN 'decided' ELSE status END,
                   created_at, expires_at, decided_at, thread_id, interrupt_id, payload_digest,
                   1, resumed_at
              FROM approvals_v1
            """, (PRESET_JSON,))
        self._conn.execute("DROP TABLE approvals_v1")

    def _migrate_v4_to_v5(self) -> None:
        """Friendly recipient registry (Part 9): recipients table + approvals.recipient_name."""
        for stmt in _V5_DDL.split(";"):
            if stmt.strip():
                self._conn.execute(stmt)
        cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(approvals)")}
        if "recipient_name" not in cols:
            self._conn.execute("ALTER TABLE approvals ADD COLUMN recipient_name TEXT")

    def _migrate_v3_to_v4(self) -> None:
        """Split Telegram identifiers out of approvals into approval_deliveries; generalise
        telegram_connections -> channel_connections and connection_tokens (Part 8.5)."""
        for stmt in _V4_DDL.split(";"):
            if stmt.strip():
                self._conn.execute(stmt)
        self._conn.execute(
            """
            INSERT INTO approvals_v4 (approval_id, recipient_id, title, message, options_json,
                selected_option_id, status, created_at, expires_at, decided_at, thread_id, interrupt_id,
                payload_digest, payload_version, resumed_at)
            SELECT approval_id, recipient_external_user_id, title, message, options_json, selected_option_id,
                   status, created_at, expires_at, decided_at, thread_id, interrupt_id, payload_digest,
                   payload_version, resumed_at
              FROM approvals""")
        self._conn.execute(
            """
            INSERT INTO approval_deliveries (approval_id, channel, connection_id, actor_ref, address_json,
                external_ref, state, claimed_at, delivered_at, attempts, created_at)
            SELECT approval_id, 'telegram', connection_id, CAST(approver_user_id AS TEXT),
                   '{"chat_id":' || chat_id || '}',
                   CASE WHEN message_id IS NOT NULL THEN chat_id || ':' || message_id END,
                   CASE WHEN delivered_at IS NOT NULL OR message_id IS NOT NULL THEN 'sent'
                        ELSE COALESCE(delivery_state, 'reserved') END,
                   delivery_claimed_at, delivered_at, delivery_attempts, created_at
              FROM approvals""")
        self._conn.execute(
            "UPDATE approvals_v4 SET decided_delivery_id = (SELECT d.delivery_id FROM approval_deliveries d"
            " WHERE d.approval_id = approvals_v4.approval_id) WHERE status = 'decided'")
        self._conn.execute(
            """
            INSERT INTO channel_connections (connection_id, recipient_id, channel, actor_ref, address_json,
                handle, label, status, connected_at, updated_at, blocked_at, ended_at)
            SELECT connection_id, external_user_id, 'telegram', CAST(telegram_user_id AS TEXT),
                   '{"chat_id":' || chat_id || '}', username, first_name, status, connected_at, updated_at,
                   blocked_at, ended_at
              FROM telegram_connections""")
        self._conn.execute(
            """
            INSERT INTO connection_tokens_v4 (token_hash, channel, recipient_id, created_at, expires_at, claim_id,
                claimed_actor_ref, claimed_address_json, claimed_handle, claimed_label, claimed_at, consumed_at,
                revoked_at)
            SELECT token_hash, 'telegram', external_user_id, created_at, expires_at, claim_id,
                   CAST(claimed_by_tg_id AS TEXT),
                   CASE WHEN claimed_chat_id IS NOT NULL THEN '{"chat_id":' || claimed_chat_id || '}' END,
                   claimed_username, claimed_first_name, claimed_at, consumed_at, revoked_at
              FROM connection_tokens""")
        for t in ("approvals", "telegram_connections", "connection_tokens"):
            self._conn.execute(f"DROP TABLE {t}")
        self._conn.execute("ALTER TABLE approvals_v4 RENAME TO approvals")
        self._conn.execute("ALTER TABLE connection_tokens_v4 RENAME TO connection_tokens")
        for stmt in _V4_INDEXES.split(";"):
            if stmt.strip():
                self._conn.execute(stmt)

    @property
    def schema_version(self) -> int:
        return int(self._conn.execute("PRAGMA user_version").fetchone()[0])

    def close(self) -> None:
        self._conn.close()

    # ---------- generic approvals + deliveries ----------

    def _insert_approval(self, *, title: str, message: str | None, options: Sequence[ApprovalOption] | None,
                         now: int, ttl_s: int, recipient_id: str | None, thread_id: str | None,
                         interrupt_id: str | None, payload_digest: str | None, payload_version: int,
                         on_conflict_ignore: bool = False) -> str:
        opts = validate_request(title, message, APPROVE_REJECT if options is None else options)
        approval_id = secrets.token_urlsafe(16)  # 128 bits, 22 chars
        conflict = " ON CONFLICT(thread_id, interrupt_id) WHERE thread_id IS NOT NULL DO NOTHING" \
            if on_conflict_ignore else ""
        self._conn.execute(
            "INSERT INTO approvals (approval_id, recipient_id, title, message, options_json, status, created_at,"
            " expires_at, thread_id, interrupt_id, payload_digest, payload_version)"
            " VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?)" + conflict,
            (approval_id, recipient_id, title, None if payload_version == 1 else message, options_to_json(opts),
             now, now + ttl_s, thread_id, interrupt_id, payload_digest, payload_version))
        return approval_id

    def ensure_delivery(self, approval_id: str, *, channel: str, actor_ref: str,
                        address: dict[str, Any] | None = None, connection_id: str | None = None,
                        now: int | None = None) -> int:
        """Create the (approval, channel) delivery once (snapshot of the connection); return its id."""
        now = now if now is not None else created_now()
        self._conn.execute(
            "INSERT INTO approval_deliveries (approval_id, channel, connection_id, actor_ref, address_json, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(approval_id, channel) DO NOTHING",
            (approval_id, channel, connection_id, str(actor_ref), _json(address), now))
        return self._conn.execute("SELECT delivery_id FROM approval_deliveries WHERE approval_id = ? AND channel = ?",
                                  (approval_id, channel)).fetchone()[0]

    def reserve_approval(self, *, thread_id: str, interrupt_id: str, recipient_id: str | None, title: str,
                         message: str | None, options: Sequence[ApprovalOption] | None, payload_digest: str,
                         payload_version: int, now: int, ttl_s: int,
                         recipient_name: str | None = None) -> Approval:
        """Create the approval for (thread_id, interrupt_id) once; return the existing row otherwise."""
        self._insert_approval(title=title, message=message, options=options, now=now, ttl_s=ttl_s,
                              recipient_id=recipient_id, thread_id=thread_id, interrupt_id=interrupt_id,
                              payload_digest=payload_digest, payload_version=payload_version,
                              on_conflict_ignore=True)
        if recipient_name is not None:
            self._conn.execute("UPDATE approvals SET recipient_name = ? WHERE thread_id = ? AND interrupt_id = ?"
                               " AND recipient_name IS NULL", (recipient_name, thread_id, interrupt_id))
        row = self.get_by_interrupt(thread_id, interrupt_id)
        assert row is not None
        return row

    def get(self, approval_id: str, channel: str | None = None) -> Approval | None:
        """The approval with its primary delivery flattened in - or, with ``channel``, the delivery
        of that channel (what an adapter must see when sending/updating)."""
        if channel is None:
            row = self._conn.execute(_SELECT + " WHERE a.approval_id = ?", (approval_id,)).fetchone()
        else:
            row = self._conn.execute(_SELECT_CHANNEL + " WHERE a.approval_id = ?", (channel, approval_id)).fetchone()
        return Approval(**dict(row)) if row else None

    def get_by_interrupt(self, thread_id: str, interrupt_id: str) -> Approval | None:
        row = self._conn.execute(_SELECT + " WHERE a.thread_id = ? AND a.interrupt_id = ?",
                                 (thread_id, interrupt_id)).fetchone()
        return Approval(**dict(row)) if row else None

    def deliveries(self, approval_id: str) -> list[dict[str, Any]]:
        return [dict(r) for r in self._conn.execute(
            "SELECT * FROM approval_deliveries WHERE approval_id = ? ORDER BY delivery_id", (approval_id,))]

    def claim(self, delivery_id: int, now: int, lease_s: int = DEFAULT_DELIVERY_LEASE_S) -> bool:
        """Atomic delivery claim: only the winner may send. Stale claims (crashed sender) expire."""
        cur = self._conn.execute(
            "UPDATE approval_deliveries SET claimed_at = ?, attempts = attempts + 1"
            " WHERE delivery_id = ? AND delivered_at IS NULL"
            " AND (claimed_at IS NULL OR claimed_at <= ?)"
            " AND EXISTS (SELECT 1 FROM approvals a WHERE a.approval_id = approval_deliveries.approval_id"
            "             AND a.status = 'pending')",
            (now, delivery_id, now - lease_s))
        return cur.rowcount == 1

    def mark_sent(self, delivery_id: int, external_ref: str, now: int) -> bool:
        cur = self._conn.execute(
            "UPDATE approval_deliveries SET external_ref = ?, delivered_at = ?, state = 'sent', last_error = NULL"
            " WHERE delivery_id = ? AND delivered_at IS NULL", (str(external_ref), now, delivery_id))
        return cur.rowcount == 1

    def mark_failed(self, delivery_id: int, error: str | None = None) -> None:
        """Transient send failure: release the claim so a later call can retry."""
        self._conn.execute(
            "UPDATE approval_deliveries SET state = 'failed', claimed_at = NULL, last_error = ?"
            " WHERE delivery_id = ? AND delivered_at IS NULL", (error, delivery_id))

    def mark_undeliverable(self, approval_id: str, now: int) -> bool:
        cur = self._conn.execute(
            "UPDATE approvals SET status = 'undeliverable', decided_at = ?"
            " WHERE approval_id = ? AND status = 'pending'", (now, approval_id))
        return cur.rowcount == 1

    def mark_resumed(self, approval_id: str, now: int) -> bool:
        cur = self._conn.execute(
            "UPDATE approvals SET resumed_at = ? WHERE approval_id = ?"
            " AND resumed_at IS NULL AND status = 'decided'", (now, approval_id))
        return cur.rowcount == 1

    def find_unresumed(self) -> list[Approval]:
        rows = self._conn.execute(_SELECT + " WHERE a.status = 'decided' AND a.resumed_at IS NULL"
                                  " AND a.thread_id IS NOT NULL ORDER BY a.decided_at, a.approval_id").fetchall()
        return [Approval(**dict(r)) for r in rows]

    def find_overdue(self, now: int) -> list[Approval]:
        rows = self._conn.execute(_SELECT + " WHERE a.status = 'pending' AND a.expires_at <= ?"
                                  " ORDER BY a.expires_at, a.approval_id", (now,)).fetchall()
        return [Approval(**dict(r)) for r in rows]

    @staticmethod
    def _resolve_option(row: Approval, choice: int | str) -> str | None:
        opts = row.options
        if isinstance(choice, bool):
            return None
        if isinstance(choice, int):
            return opts[choice].id if 0 <= choice < len(opts) else None
        if isinstance(choice, str):
            return choice if any(o.id == choice for o in opts) else None
        return None

    def decide(self, approval_id: str, choice: int | str, *, channel: str, actor_ref: str,
               delivery_ref: str, now: int) -> ConsumeResult:
        """Atomically move pending -> decided. Exactly one caller can win, across all channels.

        Valid only for the delivery of ``channel`` whose actor is ``actor_ref`` and whose
        external_ref is ``delivery_ref``, while its connection (if any) is active, before expiry.
        """
        row = self.get(approval_id)
        if row is None:
            return ConsumeResult("unknown", None, False)
        d = self._conn.execute("SELECT * FROM approval_deliveries WHERE approval_id = ? AND channel = ?",
                               (approval_id, channel)).fetchone()
        option_id = self._resolve_option(row, choice)
        if d is None or d["actor_ref"] != str(actor_ref):
            return ConsumeResult("not_authorized", None, False)  # reveal nothing
        if option_id is None:
            return ConsumeResult("invalid_option", row, False)
        cur = self._conn.execute(
            """
            UPDATE approvals
               SET status = 'decided', selected_option_id = ?, decided_at = ?, decided_delivery_id = ?
             WHERE approval_id = ? AND status = 'pending' AND expires_at > ?
               AND EXISTS (SELECT 1 FROM approval_deliveries x
                            WHERE x.delivery_id = ? AND x.actor_ref = ? AND x.external_ref = ?
                              AND (x.connection_id IS NULL OR EXISTS (
                                    SELECT 1 FROM channel_connections c
                                     WHERE c.connection_id = x.connection_id AND c.status = 'active')))
            """,
            (option_id, now, d["delivery_id"], approval_id, now, d["delivery_id"], str(actor_ref),
             str(delivery_ref)))
        if cur.rowcount == 1:
            return ConsumeResult("won", self.get(approval_id), True)
        row = self.get(approval_id)
        assert row is not None
        if d["external_ref"] is None or d["external_ref"] != str(delivery_ref):
            return ConsumeResult("wrong_message", row, False)
        if row.status != "pending":
            return ConsumeResult("already_decided", row, False)
        if row.expires_at <= now:
            changed = self.expire_if_due(approval_id, now)
            return ConsumeResult("expired", self.get(approval_id), changed)
        if d["connection_id"] is not None:
            return ConsumeResult("disconnected", row, False)
        return ConsumeResult("already_decided", row, False)  # defensive fallback

    def expire_if_due(self, approval_id: str, now: int) -> bool:
        cur = self._conn.execute(
            "UPDATE approvals SET status = 'expired', decided_at = ?"
            " WHERE approval_id = ? AND status = 'pending' AND expires_at <= ?", (now, approval_id, now))
        return cur.rowcount == 1

    # ---------- tracked threads ----------

    def track_thread(self, thread_id: str, recipient: str, now: int) -> ThreadRecord:
        validate_thread_id(thread_id)
        if not isinstance(recipient, str) or not recipient.strip() or len(recipient) > 256:
            raise ValueError("recipient must be a non-empty external user id (max 256 chars)")
        self._conn.execute(
            "INSERT INTO hitl_threads (thread_id, recipient_external_user_id, status, created_at, updated_at)"
            " VALUES (?, ?, 'active', ?, ?) ON CONFLICT(thread_id) DO NOTHING", (thread_id, recipient, now, now))
        rec = self.get_thread(thread_id)
        assert rec is not None
        if rec.recipient_external_user_id != recipient:
            raise RecipientMismatchError(f"thread {thread_id!r} is tracked for a different recipient")
        self._conn.execute("UPDATE hitl_threads SET updated_at = ? WHERE thread_id = ?", (now, thread_id))
        return self.get_thread(thread_id)  # type: ignore[return-value]

    def get_thread(self, thread_id: str) -> ThreadRecord | None:
        row = self._conn.execute("SELECT * FROM hitl_threads WHERE thread_id = ?", (thread_id,)).fetchone()
        return ThreadRecord(**dict(row)) if row else None

    def active_threads(self) -> list[ThreadRecord]:
        rows = self._conn.execute("SELECT * FROM hitl_threads WHERE status = 'active'"
                                  " ORDER BY created_at, thread_id").fetchall()
        return [ThreadRecord(**dict(r)) for r in rows]

    def unnotified_completed_threads(self) -> list[ThreadRecord]:
        rows = self._conn.execute("SELECT * FROM hitl_threads WHERE status = 'completed'"
                                  " AND completion_notified_at IS NULL ORDER BY completed_at").fetchall()
        return [ThreadRecord(**dict(r)) for r in rows]

    def mark_thread_completed(self, thread_id: str, now: int) -> bool:
        cur = self._conn.execute(
            "UPDATE hitl_threads SET status = 'completed', completed_at = ?, updated_at = ?"
            " WHERE thread_id = ? AND status = 'active'", (now, now, thread_id))
        return cur.rowcount == 1

    def mark_completion_notified(self, thread_id: str, now: int) -> bool:
        cur = self._conn.execute(
            "UPDATE hitl_threads SET completion_notified_at = ?, updated_at = ?"
            " WHERE thread_id = ? AND status = 'completed' AND completion_notified_at IS NULL", (now, now, thread_id))
        return cur.rowcount == 1

    # ---------- 0.4 compatibility shims (Telegram-shaped; channel = "telegram") ----------

    def _primary_delivery_id(self, approval_id: str) -> int | None:
        r = self._conn.execute("SELECT MIN(delivery_id) FROM approval_deliveries WHERE approval_id = ?",
                               (approval_id,)).fetchone()
        return r[0] if r else None

    def create(self, approver_user_id: int, chat_id: int, action: str, now: int, ttl_s: int, *,
               message: str | None = None, options: Sequence[ApprovalOption] | None = None,
               thread_id: str | None = None, interrupt_id: str | None = None,
               payload_digest: str | None = None, payload_version: int | None = None,
               connection_id: str | None = None, recipient_external_user_id: str | None = None) -> Approval:
        legacy = options is None and message is None
        version = payload_version if payload_version is not None else (1 if legacy else 2)
        approval_id = self._insert_approval(title=action, message=message, options=options, now=now, ttl_s=ttl_s,
                                            recipient_id=recipient_external_user_id, thread_id=thread_id,
                                            interrupt_id=interrupt_id, payload_digest=payload_digest,
                                            payload_version=version)
        self.ensure_delivery(approval_id, channel=LEGACY_CHANNEL, actor_ref=str(approver_user_id),
                             address={"chat_id": chat_id}, connection_id=connection_id, now=now)
        approval = self.get(approval_id)
        assert approval is not None
        return approval

    def reserve(self, *, thread_id: str, interrupt_id: str, approver_user_id: int, chat_id: int, title: str,
                message: str | None, options: Sequence[ApprovalOption] | None, payload_digest: str,
                payload_version: int, connection_id: str | None, recipient_external_user_id: str | None,
                now: int, ttl_s: int) -> Approval:
        a = self.reserve_approval(thread_id=thread_id, interrupt_id=interrupt_id,
                                  recipient_id=recipient_external_user_id, title=title, message=message,
                                  options=options, payload_digest=payload_digest, payload_version=payload_version,
                                  now=now, ttl_s=ttl_s)
        self.ensure_delivery(a.approval_id, channel=LEGACY_CHANNEL, actor_ref=str(approver_user_id),
                             address={"chat_id": chat_id}, connection_id=connection_id, now=now)
        return self.get(a.approval_id)  # type: ignore[return-value]

    def set_message_id(self, approval_id: str, message_id: int, now: int | None = None) -> bool:
        a = self.get(approval_id)
        if a is None or a.delivery_id is None or a.external_ref is not None:
            return False
        if self.mark_sent(a.delivery_id, f"{a.chat_id}:{message_id}", now if now is not None else created_now()):
            self._conn.execute("UPDATE approval_deliveries SET attempts = MAX(attempts, 1) WHERE delivery_id = ?",
                               (a.delivery_id,))
            return True
        return False

    def claim_delivery(self, approval_id: str, now: int, lease_s: int = DEFAULT_DELIVERY_LEASE_S) -> bool:
        d = self._primary_delivery_id(approval_id)
        return d is not None and self.claim(d, now, lease_s)

    def mark_delivered(self, approval_id: str, message_id: int, now: int) -> bool:
        a = self.get(approval_id)
        return a is not None and a.delivery_id is not None and \
            self.mark_sent(a.delivery_id, f"{a.chat_id}:{message_id}", now)

    def mark_delivery_failed(self, approval_id: str) -> None:
        d = self._primary_delivery_id(approval_id)
        if d is not None:
            self.mark_failed(d)

    def consume(self, approval_id: str, choice: int | str, user_id: int, chat_id: int,
                message_id: int, now: int) -> ConsumeResult:
        return self.decide(approval_id, choice, channel=LEGACY_CHANNEL, actor_ref=str(user_id),
                           delivery_ref=f"{chat_id}:{message_id}", now=now)


__all__ = ["DEFAULT_DELIVERY_LEASE_S", "MAX_ACTION_LENGTH", "SCHEMA_VERSION", "Approval", "ApprovalStore",
           "ConsumeResult", "ThreadRecord", "validate_action", "validate_thread_id"]
