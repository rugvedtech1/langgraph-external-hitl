"""Channel connections (core, stdlib only, channel-neutral).

A recipient (the application's user id) connects a channel account once via a single-use
link token. The core stores the binding: ``recipient_id -> (channel, actor_ref, address)``.
``actor_ref`` = who may decide (e.g. Telegram user id); ``address`` = where to deliver
(e.g. ``{"chat_id": ...}``). Adapters define both; the core treats them as opaque.

Security: tokens have 256 bits of entropy, only their SHA-256 hash is stored, they are
single-use, short-lived, bound server-side to one recipient, and creating a new token revokes
older unused ones. ``ConnectionManager`` keeps the 0.4 Telegram-shaped API on top of this.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import secrets
from dataclasses import dataclass
from typing import Any, Callable, Literal

from ._audit import audit
from ._redact import register_secret
from .store import ApprovalStore

logger = logging.getLogger(__name__)

TOKEN_PREFIX = "c_"
TOKEN_RE = re.compile(r"^c_[A-Za-z0-9_-]{43}$")       # c_ + token_urlsafe(32) = 45 chars <= 64
DEFAULT_LINK_TTL_S = 600
MAX_LINK_TTL_S = 3600

ConnectionStatus = Literal["active", "blocked", "disconnected", "replaced"]


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("ascii", "replace")).hexdigest()


@dataclass(frozen=True)
class ConnectionLink:
    url: str
    expires_at: int
    token: str  # bearer secret: show only to the authenticated app user, never log

    def __repr__(self) -> str:
        return f"ConnectionLink(url=<hidden>, expires_at={self.expires_at})"


@dataclass(frozen=True)
class ChannelConnection:
    connection_id: str
    recipient_id: str
    channel: str
    actor_ref: str
    address_json: str
    handle: str | None
    label: str | None
    status: ConnectionStatus
    connected_at: int
    updated_at: int
    blocked_at: int | None = None
    ended_at: int | None = None

    @property
    def is_active(self) -> bool:
        return self.status == "active"

    @property
    def address(self) -> dict[str, Any]:
        return json.loads(self.address_json or "{}")

    # 0.4 compatibility (Telegram channel)
    @property
    def external_user_id(self) -> str:
        return self.recipient_id

    @property
    def telegram_user_id(self) -> int | None:
        return int(self.actor_ref) if self.actor_ref.lstrip("-").isdigit() else None

    @property
    def chat_id(self) -> int | None:
        return self.address.get("chat_id")

    @property
    def username(self) -> str | None:
        return self.handle

    @property
    def first_name(self) -> str | None:
        return self.label


TelegramConnection = ChannelConnection  # 0.4 name


@dataclass(frozen=True)
class Claim:
    claim_id: str
    external_user_id: str
    expires_at: int


def _j(d: dict[str, Any] | None) -> str:
    return json.dumps(d or {}, sort_keys=True, separators=(",", ":"))


class ChannelConnections:
    """Generic connection management for ONE channel (e.g. ``ChannelConnections(store, "telegram")``)."""

    def __init__(self, store: ApprovalStore, channel: str,
                 link_builder: Callable[[str], str] | None = None) -> None:
        self.store = store
        self.channel = channel
        self.link_builder = link_builder
        self._conn = store._conn

    def _row(self, sql: str, args: tuple) -> ChannelConnection | None:
        row = self._conn.execute(sql, args).fetchone()
        return ChannelConnection(**dict(row)) if row else None

    # ---------- application side ----------

    def create_token(self, recipient_id: str, now: int, *, ttl_s: int = DEFAULT_LINK_TTL_S) -> tuple[str, int]:
        """Single-use connection token for ``recipient_id`` (revokes older unused tokens). Returns (token, expires_at)."""
        if not isinstance(recipient_id, str) or not recipient_id.strip() or len(recipient_id) > 256:
            raise ValueError("external_user_id must be a non-empty string (max 256 chars)")
        if not 60 <= ttl_s <= MAX_LINK_TTL_S:
            raise ValueError(f"ttl_s must be between 60 and {MAX_LINK_TTL_S} seconds")
        token = TOKEN_PREFIX + secrets.token_urlsafe(32)
        register_secret(token)
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            self._conn.execute(
                "UPDATE connection_tokens SET revoked_at = ? WHERE recipient_id = ? AND channel = ?"
                " AND consumed_at IS NULL AND revoked_at IS NULL", (now, recipient_id, self.channel))
            self._conn.execute(
                "INSERT INTO connection_tokens (token_hash, channel, recipient_id, created_at, expires_at)"
                " VALUES (?, ?, ?, ?, ?)", (hash_token(token), self.channel, recipient_id, now, now + ttl_s))
            self._conn.execute("COMMIT")
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise
        audit("connection.link_created", user_id=recipient_id, now=now)
        return token, now + ttl_s

    def get(self, recipient_id: str) -> ChannelConnection | None:
        return self._row("SELECT * FROM channel_connections WHERE recipient_id = ? AND channel = ?"
                         " AND status IN ('active','blocked')", (recipient_id, self.channel))

    def get_by_actor(self, actor_ref: str) -> ChannelConnection | None:
        return self._row("SELECT * FROM channel_connections WHERE actor_ref = ? AND channel = ?"
                         " AND status IN ('active','blocked')", (str(actor_ref), self.channel))

    def get_by_id(self, connection_id: str) -> ChannelConnection | None:
        return self._row("SELECT * FROM channel_connections WHERE connection_id = ?", (connection_id,))

    def disconnect(self, recipient_id: str, now: int) -> ChannelConnection | None:
        conn = self.get(recipient_id)
        if conn is None:
            return None
        self._conn.execute(
            "UPDATE channel_connections SET status = 'disconnected', ended_at = ?, updated_at = ?"
            " WHERE connection_id = ? AND status IN ('active','blocked')", (now, now, conn.connection_id))
        audit("connection.disconnected", user_id=recipient_id, now=now)
        return self.get_by_id(conn.connection_id)

    def bind(self, recipient_id: str, actor_ref: str, address: dict[str, Any], now: int, *,
             handle: str | None = None, label: str | None = None) -> ChannelConnection:
        """Create/refresh a connection directly (migration from static config, tests)."""
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            conn = self._bind_locked(recipient_id, str(actor_ref), address, handle, label, now)
            self._conn.execute("COMMIT")
            return conn
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise

    # ---------- adapter side (onboarding) ----------

    def claim_token(self, token: str, actor_ref: str, address: dict[str, Any], now: int, *,
                    handle: str | None = None, label: str | None = None) -> Claim | None:
        """First step: bind the token to this channel account. Atomic. None for any invalid token."""
        if not isinstance(token, str) or not TOKEN_RE.match(token):
            return None
        claim_id = secrets.token_urlsafe(16)
        cur = self._conn.execute(
            "UPDATE connection_tokens SET claim_id = ?, claimed_actor_ref = ?, claimed_address_json = ?,"
            " claimed_handle = ?, claimed_label = ?, claimed_at = ?"
            " WHERE token_hash = ? AND channel = ? AND consumed_at IS NULL AND revoked_at IS NULL AND expires_at > ?"
            " AND (claimed_actor_ref IS NULL OR claimed_actor_ref = ?)",
            (claim_id, str(actor_ref), _j(address), handle, label, now, hash_token(token), self.channel, now,
             str(actor_ref)))
        if cur.rowcount != 1:
            audit("connection.claim", user_id=actor_ref, outcome="rejected", now=now)
            return None
        row = self._conn.execute("SELECT recipient_id, expires_at FROM connection_tokens WHERE claim_id = ?",
                                 (claim_id,)).fetchone()
        audit("connection.claim", user_id=actor_ref, outcome="claimed", now=now)
        return Claim(claim_id, row["recipient_id"], row["expires_at"])

    def confirm_claim(self, claim_id: str, actor_ref: str, address: dict[str, Any], now: int
                      ) -> ChannelConnection | None:
        """Second step: consume the claimed token and bind. Same actor and address required. Single-use."""
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            row = self._conn.execute(
                "SELECT * FROM connection_tokens WHERE claim_id = ? AND channel = ? AND claimed_actor_ref = ?"
                " AND claimed_address_json = ? AND consumed_at IS NULL AND revoked_at IS NULL AND expires_at > ?",
                (claim_id, self.channel, str(actor_ref), _j(address), now)).fetchone()
            if row is None:
                self._conn.execute("ROLLBACK")
                audit("connection.confirm", user_id=actor_ref, outcome="rejected", now=now)
                return None
            self._conn.execute("UPDATE connection_tokens SET consumed_at = ? WHERE token_hash = ?",
                               (now, row["token_hash"]))
            conn = self._bind_locked(row["recipient_id"], str(actor_ref), address, row["claimed_handle"],
                                     row["claimed_label"], now)
            self._conn.execute("COMMIT")
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise
        audit("connection.confirm", user_id=actor_ref, outcome="connected", now=now)
        return conn

    def cancel(self, claim_id: str, actor_ref: str, now: int) -> bool:
        cur = self._conn.execute(
            "UPDATE connection_tokens SET revoked_at = ? WHERE claim_id = ? AND claimed_actor_ref = ?"
            " AND consumed_at IS NULL AND revoked_at IS NULL", (now, claim_id, str(actor_ref)))
        return cur.rowcount == 1

    def disconnect_actor(self, actor_ref: str, now: int) -> ChannelConnection | None:
        conn = self.get_by_actor(actor_ref)
        return self.disconnect(conn.recipient_id, now) if conn else None

    def set_blocked(self, actor_ref: str, now: int) -> bool:
        cur = self._conn.execute(
            "UPDATE channel_connections SET status = 'blocked', blocked_at = ?, updated_at = ?"
            " WHERE channel = ? AND actor_ref = ? AND status = 'active'", (now, now, self.channel, str(actor_ref)))
        if cur.rowcount:
            audit("connection.blocked", user_id=actor_ref, now=now)
        return cur.rowcount == 1

    def set_unblocked(self, actor_ref: str, now: int) -> bool:
        cur = self._conn.execute(
            "UPDATE channel_connections SET status = 'active', blocked_at = NULL, updated_at = ?"
            " WHERE channel = ? AND actor_ref = ? AND status = 'blocked'", (now, self.channel, str(actor_ref)))
        if cur.rowcount:
            audit("connection.unblocked", user_id=actor_ref, now=now)
        return cur.rowcount == 1

    def refresh_display(self, actor_ref: str, handle: str | None, label: str | None, now: int) -> None:
        """Display-only fields; never used for identity or authorization."""
        self._conn.execute(
            "UPDATE channel_connections SET handle = ?, label = ?, updated_at = ?"
            " WHERE channel = ? AND actor_ref = ? AND status IN ('active','blocked')"
            " AND (handle IS NOT ? OR label IS NOT ?)", (handle, label, now, self.channel, str(actor_ref), handle, label))

    def _bind_locked(self, recipient_id: str, actor_ref: str, address: dict[str, Any], handle: str | None,
                     label: str | None, now: int) -> ChannelConnection:
        """Strict 1:1 per channel: replace other live connections of this recipient or this actor."""
        same = self._conn.execute(
            "SELECT connection_id FROM channel_connections WHERE recipient_id = ? AND channel = ? AND actor_ref = ?"
            " AND status IN ('active','blocked')", (recipient_id, self.channel, actor_ref)).fetchone()
        if same is not None:
            self._conn.execute(
                "UPDATE channel_connections SET address_json = ?, handle = ?, label = ?, status = 'active',"
                " blocked_at = NULL, updated_at = ? WHERE connection_id = ?",
                (_j(address), handle, label, now, same["connection_id"]))
            cid = same["connection_id"]
        else:
            self._conn.execute(
                "UPDATE channel_connections SET status = 'replaced', ended_at = ?, updated_at = ?"
                " WHERE channel = ? AND (recipient_id = ? OR actor_ref = ?) AND status IN ('active','blocked')",
                (now, now, self.channel, recipient_id, actor_ref))
            cid = secrets.token_urlsafe(16)
            self._conn.execute(
                "INSERT INTO channel_connections (connection_id, recipient_id, channel, actor_ref, address_json,"
                " handle, label, status, connected_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?, ?)",
                (cid, recipient_id, self.channel, actor_ref, _j(address), handle, label, now, now))
        return self.get_by_id(cid)  # type: ignore[return-value]


def _telegram_link(bot_username: str) -> Callable[[str], str]:
    return lambda token: f"https://t.me/{bot_username}?start={token}"


class ConnectionManager(ChannelConnections):
    """0.4-compatible, Telegram-shaped facade (channel ``"telegram"``): integer Telegram user id
    as actor, ``{"chat_id": ...}`` as address. New code can use ``ChannelConnections`` directly."""

    def __init__(self, store: ApprovalStore, bot_username: str | None = None, *, channel: str = "telegram") -> None:
        super().__init__(store, channel)
        self.bot_username = bot_username

    def create_link(self, external_user_id: str, now: int, *, ttl_s: int = DEFAULT_LINK_TTL_S,
                    bot_username: str | None = None) -> ConnectionLink:
        username = (bot_username or self.bot_username or "").lstrip("@")
        if not username:
            raise ValueError("bot_username is required to build the deep link")
        token, expires_at = self.create_token(external_user_id, now, ttl_s=ttl_s)
        return ConnectionLink(url=_telegram_link(username)(token), expires_at=expires_at, token=token)

    def get_by_telegram_user(self, telegram_user_id: int) -> ChannelConnection | None:
        return self.get_by_actor(str(telegram_user_id))

    def seed(self, external_user_id: str, telegram_user_id: int, chat_id: int, now: int, *,
             username: str | None = None, first_name: str | None = None) -> ChannelConnection:
        return self.bind(external_user_id, str(telegram_user_id), {"chat_id": chat_id}, now,
                         handle=username, label=first_name)

    def claim(self, token: str, telegram_user_id: int, chat_id: int, now: int, *,
              username: str | None = None, first_name: str | None = None) -> Claim | None:
        return self.claim_token(token, str(telegram_user_id), {"chat_id": chat_id}, now,
                                handle=username, label=first_name)

    def confirm(self, claim_id: str, telegram_user_id: int, chat_id: int, now: int) -> ChannelConnection | None:
        return self.confirm_claim(claim_id, str(telegram_user_id), {"chat_id": chat_id}, now)

    def cancel_claim(self, claim_id: str, telegram_user_id: int, now: int) -> bool:
        return self.cancel(claim_id, str(telegram_user_id), now)

    def disconnect_telegram_user(self, telegram_user_id: int, now: int) -> ChannelConnection | None:
        return self.disconnect_actor(str(telegram_user_id), now)

    def mark_blocked(self, telegram_user_id: int, now: int) -> bool:
        return self.set_blocked(str(telegram_user_id), now)

    def mark_unblocked(self, telegram_user_id: int, now: int) -> bool:
        return self.set_unblocked(str(telegram_user_id), now)

    def refresh_profile(self, telegram_user_id: int, username: str | None, first_name: str | None, now: int) -> None:
        self.refresh_display(str(telegram_user_id), username, first_name, now)


__all__ = ["DEFAULT_LINK_TTL_S", "ChannelConnection", "ChannelConnections", "Claim", "ConnectionLink",
           "ConnectionManager", "TelegramConnection", "hash_token"]
