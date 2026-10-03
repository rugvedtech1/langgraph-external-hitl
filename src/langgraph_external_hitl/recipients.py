"""Friendly recipient registry (core, channel-neutral, stdlib only).

A recipient has a stable internal ``recipient_id`` and a friendly ``name`` such as "manager".
The name is ONLY a lookup key: authorization always uses the channel identity bound to the
recipient's connection (for Telegram, the numeric ``from.id``). Unknown names fail closed.
"""
from __future__ import annotations

import re
import secrets
import time
from dataclasses import dataclass

from ._audit import audit
from .errors import DuplicateRecipientError, InvalidRecipientNameError, UnknownRecipientError
from .store import ApprovalStore

NAME_RE = re.compile(r"^[a-z][a-z0-9_-]{0,31}$")
RESERVED_NAMES = frozenset({"all", "any", "none"})


def normalize_name(name: str) -> str:
    """Case-fold and validate a recipient name. Raises InvalidRecipientNameError."""
    if not isinstance(name, str):
        raise InvalidRecipientNameError("recipient name must be a string")
    key = name.strip().casefold()
    if not NAME_RE.match(key):
        raise InvalidRecipientNameError(
            f"invalid recipient name {name!r}: use 1-32 chars, start with a-z, then a-z 0-9 _ -")
    if key in RESERVED_NAMES:
        raise InvalidRecipientNameError(f"recipient name {name!r} is reserved")
    return key


@dataclass(frozen=True)
class Recipient:
    recipient_id: str
    name: str
    display_name: str | None
    status: str
    renamed_from: str | None
    created_at: int
    updated_at: int


class RecipientRegistry:
    """Maps friendly names to stable recipient ids. One registry per HITL database."""

    def __init__(self, store: ApprovalStore, clock=lambda: int(time.time())) -> None:
        self.store = store
        self._conn = store._conn
        self._clock = clock

    def _row(self, key: str) -> Recipient | None:
        row = self._conn.execute(
            "SELECT recipient_id, name, display_name, status, renamed_from, created_at, updated_at"
            " FROM recipients WHERE name_key = ? AND status = 'active'", (key,)).fetchone()
        return Recipient(**dict(row)) if row else None

    def create(self, name: str, *, display_name: str | None = None) -> Recipient:
        key = normalize_name(name)
        if self._row(key) is not None:
            raise DuplicateRecipientError(f"recipient {key!r} already exists")
        now = self._clock()
        rid = "rcp_" + secrets.token_urlsafe(12)
        self._conn.execute(
            "INSERT INTO recipients (recipient_id, name, name_key, display_name, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?)", (rid, key, key, display_name, now, now))
        audit("recipient.created", user_id=key, now=now)
        return self._row(key)  # type: ignore[return-value]

    def get(self, name: str) -> Recipient | None:
        return self._row(normalize_name(name))

    def resolve(self, name: str) -> Recipient:
        """Recipient for ``name`` or UnknownRecipientError (fail closed, no fallback)."""
        r = self.get(name)
        if r is None:
            names = [x.name for x in self.list()]
            known = f"registered approvers: {', '.join(names)}" if names else "no approvers are registered yet"
            raise UnknownRecipientError(f"approver {name!r} is not registered ({known}). Use a registered name "
                                        "in your code, or run `langgraph-hitl setup` to add/rename one")
        return r

    def by_id(self, recipient_id: str) -> Recipient | None:
        row = self._conn.execute(
            "SELECT recipient_id, name, display_name, status, renamed_from, created_at, updated_at"
            " FROM recipients WHERE recipient_id = ?", (recipient_id,)).fetchone()
        return Recipient(**dict(row)) if row else None

    def rename(self, name: str, new_name: str, *, force: bool = False) -> Recipient:
        r = self.resolve(name)
        new_key = normalize_name(new_name)
        if new_key == r.name:
            return r
        if self._row(new_key) is not None:
            raise DuplicateRecipientError(f"recipient {new_key!r} already exists")
        if not force and self._pending(r.recipient_id):
            raise ValueError(f"recipient {r.name!r} has pending approvals; finish them or use force=True")
        now = self._clock()
        self._conn.execute("UPDATE recipients SET name = ?, name_key = ?, renamed_from = ?, updated_at = ?"
                           " WHERE recipient_id = ?", (new_key, new_key, r.name, now, r.recipient_id))
        return self._row(new_key)  # type: ignore[return-value]

    def remove(self, name: str, *, force: bool = False) -> None:
        r = self.resolve(name)
        if not force and self._pending(r.recipient_id):
            raise ValueError(f"recipient {r.name!r} has pending approvals; finish them or use force=True")
        self._conn.execute("UPDATE recipients SET status = 'removed', updated_at = ? WHERE recipient_id = ?",
                           (self._clock(), r.recipient_id))

    def list(self) -> list[Recipient]:
        rows = self._conn.execute(
            "SELECT recipient_id, name, display_name, status, renamed_from, created_at, updated_at"
            " FROM recipients WHERE status = 'active' ORDER BY name").fetchall()
        return [Recipient(**dict(r)) for r in rows]

    def _pending(self, recipient_id: str) -> bool:
        return self._conn.execute("SELECT 1 FROM approvals WHERE recipient_id = ? AND status = 'pending' LIMIT 1",
                                  (recipient_id,)).fetchone() is not None


__all__ = ["NAME_RE", "Recipient", "RecipientRegistry", "normalize_name"]
