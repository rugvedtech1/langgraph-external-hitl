"""Telegram channel adapter + the 0.4-compatible TelegramApprovalBot facade (Part 8.5).

TelegramAdapter: everything Telegram-specific - Bot API transport, message rendering, inline
keyboards, callback parsing/answering, edits, error classification. It implements the generic
``ApprovalChannel`` contract; it never decides validity (HitlService does).

TelegramApprovalBot: facade = HitlService (channel-neutral workflow) + TelegramAdapter, plus the
Telegram receive loop (long polling) and the /start deep-link connection flow.
"""
from __future__ import annotations

import asyncio
import html
import logging
import re
import sqlite3
import time
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol

import httpx

from .._redact import redact, register_secret
from ..channels.base import ChannelError, DecisionReport, DecisionRequest, DeliveryReceipt
from ..connections import ConnectionManager, TelegramConnection
from ..errors import HitlError, RecipientUnavailableError
from ..service import DeliveryResult, HitlService, RecoveryReport
from ..store import Approval
from ._api import BotApi, TelegramError
from .config import TelegramConfig

if TYPE_CHECKING:  # no runtime import of langgraph from the telegram extra
    from ..bridge import HitlBridge

logger = logging.getLogger("langgraph_external_hitl.telegram")

LEGACY_CODES: dict[str, str] = {"a": "approve", "r": "reject"}
_V2_RE = re.compile(r"^v2:([A-Za-z0-9_-]{1,43}):(\d{1,2})$")
_CONFIRM_RE = re.compile(r"^c2:([A-Za-z0-9_-]{1,43}):([yn])$")
ALLOWED_UPDATES = ["message", "callback_query", "my_chat_member"]

TOASTS: dict[str, str] = {
    "won": "Recorded: {label}.",
    "unknown": "Unknown or invalid request.",
    "not_authorized": "You are not authorized to decide this request.",
    "wrong_message": "This button does not belong to this request.",
    "already_decided": "Already decided: {label}.",
    "expired": "This request has expired.",
    "stale": "This request is no longer valid.",
    "busy": "Temporarily unavailable, please try again.",
    "invalid_option": "This choice is not valid.",
    "disconnected": "This Telegram account is no longer connected.",
}
LINK_INVALID = "This connection link is invalid or expired. Please create a new one in the app."
FAILED_START_LIMIT = 5
FAILED_START_WINDOW_S = 600
FAILED_START_BLOCK_S = 3600


class ApiLike(Protocol):
    async def call(self, method: str, **params: Any) -> Any: ...


StartHook = Callable[[int, int], Awaitable[Approval | None]]
ConnHook = Callable[[TelegramConnection], Awaitable[None] | None]


def now_s() -> int:
    return int(time.time())


def parse_callback_data(data: str) -> tuple[str, str] | None:
    """0.2 format: strictly parse '<a|r>:<approval_id>'. callback_data is untrusted."""
    code, sep, approval_id = data.partition(":")
    decision = LEGACY_CODES.get(code)
    if not sep or decision is None or not approval_id or len(approval_id) > 64:
        return None
    return decision, approval_id


def parse_option_callback(data: str) -> tuple[str, int | str] | None:
    """Parse an approval click: ``v2:<approval_id>:<index>`` or legacy ``a:``/``r:``.
    Returns (approval_id, option index or legacy option id)."""
    m = _V2_RE.match(data)
    if m:
        return m.group(1), int(m.group(2))
    legacy = parse_callback_data(data)
    if legacy:
        return legacy[1], legacy[0]
    return None


def render(a: Approval) -> str:
    """HTML message body. All developer/user text is escaped."""
    expires = datetime.fromtimestamp(a.expires_at).strftime("%H:%M:%S")
    parts = [f"<b>{html.escape(a.title)}</b>"]
    if a.message:
        parts.append(html.escape(a.message))
    described = [o for o in a.options if o.description]
    if described:
        parts.append("\n".join(f"• <b>{html.escape(o.label)}</b> — {html.escape(o.description or '')}"
                               for o in described))
    parts.append(f"Ref: <code>{a.approval_id[:8]}</code> · Expires at {expires}")
    return "\n\n".join(parts)


def keyboard(a: Approval) -> dict[str, Any]:
    """One button per row; up to 3 short labels share one row. callback_data is opaque."""
    buttons = []
    for i, o in enumerate(a.options):
        b: dict[str, Any] = {"text": o.label, "callback_data": f"v2:{a.approval_id}:{i}"}
        if o.style:
            b["style"] = o.style
        buttons.append(b)
    if len(buttons) <= 3 and all(len(o.label) <= 20 for o in a.options):
        return {"inline_keyboard": [buttons]}
    return {"inline_keyboard": [[b] for b in buttons]}



CHANNEL = "telegram"


def delivery_ref(chat_id: int, message_id: int) -> str:
    """Telegram external_ref: message ids are per chat, so the reference includes the chat."""
    return f"{chat_id}:{message_id}"


class TelegramAdapter:
    """``ApprovalChannel`` implementation for the Telegram Bot API."""

    name = CHANNEL

    def __init__(self, config: TelegramConfig, *, api: ApiLike | None = None) -> None:
        register_secret(config.token)
        self.config = config
        self._owns_api = api is None
        self.api: ApiLike = api if api is not None else BotApi(config.token, config.poll_timeout_s)

    async def safe_call(self, method: str, **params: Any) -> Any:
        """Best-effort UI call. Never raises (except cancellation). Returns result or None."""
        try:
            return await self.api.call(method, **params)
        except TelegramError as e:
            if method == "answerCallbackQuery" and e.error_code == 400:
                logger.info("answerCallbackQuery skipped (expired or invalid query): %s", redact(e.description))
            else:
                logger.warning("%s failed: %s", method, redact(e))
        except (httpx.HTTPError, ValueError) as e:
            logger.warning("%s failed: %s: %s", method, type(e).__name__, redact(e))
        except Exception as e:  # UI must never break the approval flow
            logger.warning("%s failed unexpectedly: %s: %s", method, type(e).__name__, redact(e))
        return None

    async def send(self, approval: Approval) -> DeliveryReceipt:
        chat_id = approval.address.get("chat_id")
        try:
            sent = await self.api.call("sendMessage", chat_id=chat_id, text=render(approval),
                                       parse_mode="HTML", reply_markup=keyboard(approval))
        except TelegramError as e:
            if e.error_code == 403:   # e.g. "Forbidden: bot was blocked by the user"
                raise ChannelError(redact(e), permanent=True, reason="blocked") from e
            if e.error_code in (401, 404):  # token revoked/reset or bot gone: channel-wide
                raise ChannelError(redact(e), permanent=True, reason="unauthorized") from e
            raise ChannelError(redact(e), permanent=False, reason="telegram_error") from e
        except (httpx.HTTPError, ValueError) as e:
            raise ChannelError(type(e).__name__, permanent=False, reason="network") from e
        return DeliveryReceipt(delivery_ref(chat_id, sent["message_id"]))

    async def update(self, approval: Approval, report: DecisionReport) -> None:
        a = approval
        if a is None or a.chat_id is None or a.message_id is None:
            return
        if report.outcome == "stale":
            text = f"{render(a)}\n\nStatus: NO LONGER VALID (graph not waiting)"
        elif report.outcome == "expired":
            text = f"{render(a)}\n\nStatus: EXPIRED"
        elif report.outcome == "won":
            if report.resume_status != "resumed":
                graph_line = f"Graph: NOT RESUMED ({report.resume_status})"
            elif report.next_state == "pending":
                graph_line = "Graph: waiting for the next approval"
            elif report.next_state == "completed" and report.graph_result is None:
                graph_line = "Graph: completed"
            else:
                graph_line = f"Graph: {html.escape(str(report.graph_result))}"
            sel = a.selected_option
            label = html.escape(sel.label if sel else str(a.selected_option_id))
            text = f"{render(a)}\n\n✅ Selected: <b>{label}</b> (user {a.actor_ref})\n{graph_line}"
        else:
            return
        await self.safe_call("editMessageText", chat_id=a.chat_id, message_id=a.message_id,
                             parse_mode="HTML", text=text)

    async def answer(self, callback_query_id: str, text: str) -> None:
        await self.safe_call("answerCallbackQuery", callback_query_id=callback_query_id, text=text)

    async def aclose(self) -> None:
        if self._owns_api and isinstance(self.api, BotApi):
            await self.api.aclose()


class TelegramApprovalBot:
    """0.4-compatible facade: HitlService (channel-neutral workflow) + TelegramAdapter, plus the
    Telegram receive loop and the /start deep-link connection flow."""

    def __init__(self, config: TelegramConfig, bridge: "HitlBridge", *,
                 on_start: StartHook | None = None, api: ApiLike | None = None,
                 clock: Callable[[], int] = now_s, connections: ConnectionManager | None = None,
                 app_name: str = "the application",
                 on_connected: ConnHook | None = None, on_disconnected: ConnHook | None = None,
                 on_blocked: ConnHook | None = None) -> None:
        self.config = config
        self.bridge = bridge
        self.on_start = on_start
        self.adapter = TelegramAdapter(config, api=api)
        self.api = self.adapter.api
        self._clock = clock
        self.connections = connections or ConnectionManager(bridge.store, config.bot_username)
        self.app_name = app_name
        self.on_connected = on_connected
        self.on_disconnected = on_disconnected
        self.service = HitlService(bridge, self.adapter, connections=self.connections, clock=clock,
                                   approval_ttl_s=config.approval_ttl_s, on_blocked=on_blocked)
        self._failed_starts: dict[int, list[int]] = {}
        self._blocked_until: dict[int, int] = {}

    # hooks kept as attributes for 0.4 compatibility
    @property
    def on_blocked(self) -> ConnHook | None:
        return self.service.on_blocked

    @on_blocked.setter
    def on_blocked(self, hook: ConnHook | None) -> None:
        self.service.on_blocked = hook

    async def _safe_call(self, method: str, **params: Any) -> Any:
        return await self.adapter.safe_call(method, **params)

    async def _fire(self, hook: ConnHook | None, conn: TelegramConnection | None) -> None:
        await self.service._fire(hook, conn)

    async def bot_username(self) -> str:
        if not self.connections.bot_username:
            me = await self.api.call("getMe")
            self.connections.bot_username = me.get("username")
        return self.connections.bot_username or ""

    # ---------- channel-neutral workflow (delegated to HitlService) ----------

    async def track(self, thread_id: str, recipient: str) -> None:
        """Register an application-owned thread for ``recipient``. Call BEFORE invoking the graph."""
        await self.service.track(thread_id, recipient)

    async def deliver_pending(self, thread_id: str, recipient: str | None = None, *,
                              ttl_s: int | None = None) -> DeliveryResult:
        """See HitlService.deliver_pending."""
        return await self.service.deliver_pending(thread_id, recipient, ttl_s=ttl_s)

    async def recover(self, now: int | None = None) -> RecoveryReport:
        """See HitlService.recover."""
        return await self.service.recover(now)

    async def request_approval(self, recipient: str, *, graph_input: dict[str, Any] | None = None,
                               action: str | None = None, ttl_s: int | None = None,
                               thread_id: str | None = None) -> Approval | None:
        """See HitlService.request_approval."""
        return await self.service.request_approval(recipient, graph_input=graph_input, action=action,
                                                   ttl_s=ttl_s, thread_id=thread_id)

    async def send_request(self, approval: Approval) -> int:
        """Legacy (0.2/0.3 start() path): send one approval and bind its delivery."""
        try:
            receipt = await self.adapter.send(approval)
        except ChannelError as e:
            if e.reason == "blocked":
                now = self._clock()
                self.bridge.store.mark_undeliverable(approval.approval_id, now)
                if self.connections.set_blocked(approval.actor_ref, now):
                    await self._fire(self.on_blocked, self.connections.get_by_actor(approval.actor_ref))
                raise RecipientUnavailableError("recipient has blocked the bot") from e
            raise (e.__cause__ or e)
        did = self.service._delivery_id(approval)
        self.bridge.store.mark_sent(did, receipt.external_ref, self._clock())
        message_id = int(receipt.external_ref.rsplit(":", 1)[1])
        logger.info("sent    approval_id=%s  message_id=%s", approval.approval_id, message_id)
        return message_id

    # ---------- incoming: connection flow ----------

    def _start_rate_limited(self, tg_id: int, now: int) -> bool:
        return self._blocked_until.get(tg_id, 0) > now

    def _record_failed_start(self, tg_id: int, now: int) -> None:
        hist = [t for t in self._failed_starts.get(tg_id, []) if t > now - FAILED_START_WINDOW_S]
        hist.append(now)
        self._failed_starts[tg_id] = hist
        if len(hist) >= FAILED_START_LIMIT:
            self._blocked_until[tg_id] = now + FAILED_START_BLOCK_S
            logger.warning("connection attempts rate-limited for telegram user %s", tg_id)

    async def handle_connect_start(self, msg: dict[str, Any], token: str) -> None:
        user = msg["from"]
        chat_id: int = msg["chat"]["id"]
        now = self._clock()
        if self._start_rate_limited(user["id"], now):
            return
        try:
            claim = self.connections.claim(token, user["id"], chat_id, now,
                                           username=user.get("username"),
                                           first_name=user.get("first_name"))
        except sqlite3.Error as e:
            logger.warning("connection claim failed: %s", type(e).__name__)
            await self._safe_call("sendMessage", chat_id=chat_id, text=TOASTS["busy"])
            return
        if claim is None:
            self._record_failed_start(user["id"], now)
            await self._safe_call("sendMessage", chat_id=chat_id, text=LINK_INVALID)
            return
        name = html.escape(user.get("first_name") or "this account")
        text = (f"Connect this Telegram account ({name}) to <b>{html.escape(self.app_name)}</b>?\n\n"
                "You will receive approval requests here. Only continue if you opened this link yourself.")
        await self._safe_call("sendMessage", chat_id=chat_id, text=text, parse_mode="HTML",
                              reply_markup={"inline_keyboard": [[
                                  {"text": "Connect", "callback_data": f"c2:{claim.claim_id}:y",
                                   "style": "success"},
                                  {"text": "Cancel", "callback_data": f"c2:{claim.claim_id}:n"}]]})

    async def handle_connect_confirm(self, cq: dict[str, Any], claim_id: str, yes: bool) -> None:
        user_id: int = cq["from"]["id"]
        msg = cq.get("message") or {}
        chat_id = msg.get("chat", {}).get("id")
        now = self._clock()
        conn = None
        try:
            if yes and chat_id is not None:
                conn = self.connections.confirm(claim_id, user_id, chat_id, now)
            elif not yes:
                self.connections.cancel_claim(claim_id, user_id, now)
        except sqlite3.Error as e:
            logger.warning("connection confirm failed: %s", type(e).__name__)
            await self._safe_call("answerCallbackQuery", callback_query_id=cq["id"], text=TOASTS["busy"])
            return
        if yes and conn is None:
            await self._safe_call("answerCallbackQuery", callback_query_id=cq["id"], text=LINK_INVALID)
            text = LINK_INVALID
        elif yes:
            await self._safe_call("answerCallbackQuery", callback_query_id=cq["id"], text="Connected.")
            text = f"✅ Connected to {self.app_name}. You will receive approval requests here."
            logger.info("connected  telegram_user_id=%s  connection_id=%s", user_id, conn.connection_id)
        else:
            await self._safe_call("answerCallbackQuery", callback_query_id=cq["id"], text="Cancelled.")
            text = "Connection cancelled."
        if chat_id is not None and msg.get("message_id") is not None:
            await self._safe_call("editMessageText", chat_id=chat_id, message_id=msg["message_id"],
                                  text=text)
        if conn is not None:
            await self._fire(self.on_connected, conn)

    async def handle_disconnect_command(self, msg: dict[str, Any]) -> None:
        conn = self.connections.disconnect_telegram_user(msg["from"]["id"], self._clock())
        text = ("Disconnected. You will no longer receive approval requests."
                if conn else "This Telegram account is not connected.")
        await self._safe_call("sendMessage", chat_id=msg["chat"]["id"], text=text)
        await self._fire(self.on_disconnected, conn)

    async def handle_my_chat_member(self, upd: dict[str, Any]) -> None:
        """Private chats: Telegram sends this only when the user blocks or unblocks the bot."""
        if upd.get("chat", {}).get("type") != "private":
            return
        tg_id = upd.get("from", {}).get("id")
        status = (upd.get("new_chat_member") or {}).get("status")
        now = self._clock()
        if status == "kicked" and self.connections.mark_blocked(tg_id, now):
            await self._fire(self.on_blocked, self.connections.get_by_telegram_user(tg_id))
        elif status == "member" and self.connections.mark_unblocked(tg_id, now):
            logger.info("unblocked  telegram_user_id=%s", tg_id)

    # ---------- incoming: messages ----------

    async def handle_start(self, msg: dict[str, Any]) -> None:
        user_id: int = msg["from"]["id"]
        chat_id: int = msg["chat"]["id"]
        logger.info("/start  user_id=%s  chat_id=%s  chat_type=%s", user_id, chat_id, msg["chat"]["type"])

        if msg["chat"]["type"] != "private":
            await self._safe_call("sendMessage", chat_id=chat_id,
                                  text="Please use a private chat with this bot.")
            return
        parts = (msg.get("text") or "").split(maxsplit=1)
        if len(parts) == 2:  # deep link /start <payload>
            await self.handle_connect_start(msg, parts[1].strip())
            return
        conn = self.connections.get_by_telegram_user(user_id)
        if self.on_start is not None and user_id in self.config.approver_user_ids:
            try:  # 0.2-style demo hook (static allowlist)
                approval = await self.on_start(user_id, chat_id)
            except (HitlError, sqlite3.Error) as e:
                logger.error("could not create approval request: %s: %s", type(e).__name__, redact(e))
                await self._safe_call("sendMessage", chat_id=chat_id,
                                      text="Could not create an approval request. Please try again later.")
                return
            if approval is not None:
                logger.info("graph   thread_id=%s  interrupt_id=%s", approval.thread_id, approval.interrupt_id)
                await self.send_request(approval)
            return
        if conn is not None:
            text = f"This Telegram account is connected to {self.app_name}. Approval requests will appear here."
        elif user_id in self.config.approver_user_ids:
            text = f"Your Telegram user ID is {user_id}. You are an authorized approver."
        else:
            text = (f"Your Telegram user ID is {user_id}. You are not an authorized approver. "
                    "To connect, open the connection link from the application.")
        await self._safe_call("sendMessage", chat_id=chat_id, text=text)

    # ---------- incoming: approval clicks ----------

    async def handle_callback(self, cq: dict[str, Any]) -> None:
        data = str(cq.get("data", ""))
        cm = _CONFIRM_RE.match(data)
        if cm:
            await self.handle_connect_confirm(cq, cm.group(1), cm.group(2) == "y")
            return
        user_id: int = cq["from"]["id"]
        msg = cq.get("message") or {}
        chat_id = msg.get("chat", {}).get("id")
        message_id = msg.get("message_id")
        parsed = parse_option_callback(data)
        if parsed is None or chat_id is None or message_id is None:
            logger.info("callback user_id=%s  callback_query_id=%s  outcome=invalid", user_id, cq["id"])
            await self.adapter.answer(cq["id"], TOASTS["unknown"])
            return
        approval_id, choice = parsed
        req = DecisionRequest(approval_id, choice, CHANNEL, str(user_id), delivery_ref(chat_id, message_id))
        try:
            result = await self.service.decide(req)
        except sqlite3.Error as e:  # e.g. database is locked: nothing was changed
            logger.warning("callback user_id=%s  approval_id(untrusted)=%s  outcome=busy  %s: %s",
                           user_id, approval_id, type(e).__name__, redact(e))
            await self.adapter.answer(cq["id"], TOASTS["busy"])
            return
        sel = result.approval.selected_option if result.approval else None
        toast = TOASTS[result.outcome].format(label=sel.label if sel else "")
        logger.info("callback user_id=%s  chat_id=%s  callback_query_id=%s  approval_id(untrusted)=%s  "
                    "outcome=%s  result=%r", user_id, chat_id, cq["id"], approval_id, result.outcome, toast)
        await self.adapter.answer(cq["id"], toast)   # 1) acknowledge fast (best effort; cannot raise)
        await self.service.finish(result)             # 2) resume / next approval / message update

    async def handle_update(self, upd: dict[str, Any]) -> None:
        if (msg := upd.get("message")) and "from" in msg:
            frm = msg["from"]
            if self.connections.get_by_telegram_user(frm["id"]) is not None:
                self.connections.refresh_profile(frm["id"], frm.get("username"), frm.get("first_name"),
                                                 self._clock())
            cmd = (msg.get("text") or "").split(maxsplit=1)
            head = cmd[0].split("@")[0] if cmd else ""
            if head == "/start":
                await self.handle_start(msg)
            elif head == "/disconnect" and msg.get("chat", {}).get("type") == "private":
                await self.handle_disconnect_command(msg)
        elif cq := upd.get("callback_query"):
            await self.handle_callback(cq)
        elif mcm := upd.get("my_chat_member"):
            await self.handle_my_chat_member(mcm)

    # ---------- main loop ----------

    async def run_polling(self) -> None:
        """Long-poll getUpdates forever (Ctrl+C to stop). One worker per bot token."""
        try:
            me = await self.api.call("getMe")
            if not self.connections.bot_username:
                self.connections.bot_username = me.get("username")
            logger.info("bot id=%s username=@%s  approvers=%s", me["id"], me.get("username"),
                        sorted(self.config.approver_user_ids) or "NONE")
            if (await self.api.call("getWebhookInfo"))["url"]:
                raise SystemExit("A webhook is set, so getUpdates will not work. Call deleteWebhook first.")

            offset: int | None = None
            while True:
                try:
                    updates = await self.api.call(
                        "getUpdates", offset=offset, timeout=self.config.poll_timeout_s,
                        allowed_updates=ALLOWED_UPDATES)
                except (httpx.HTTPError, TelegramError, ValueError) as e:
                    if isinstance(e, TelegramError) and e.error_code == 409:
                        logger.error("poll error: another process is polling this bot (409 Conflict); "
                                     "run only one worker per bot token")
                    logger.warning("poll error: %s: %s", type(e).__name__, redact(e))
                    await asyncio.sleep(3)
                    continue
                for upd in updates:
                    offset = upd["update_id"] + 1  # confirmed on the next getUpdates call
                    try:
                        await self.handle_update(upd)
                    except Exception as e:  # keep the bot alive
                        logger.error("handler error update_id=%s: %s: %s",
                                     upd["update_id"], type(e).__name__, redact(e))
        finally:
            await self.aclose()

    async def aclose(self) -> None:
        await self.adapter.aclose()
