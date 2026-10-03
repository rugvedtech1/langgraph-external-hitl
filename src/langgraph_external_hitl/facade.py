"""HITL: the simple, configured entry point (thin facade over the existing architecture).

    async with HITL.from_config() as hitl:            # finds ./.hitl (created by `langgraph-hitl setup`)
        graph = builder.compile(checkpointer=hitl.checkpointer)
        ...
        await hitl.start(graph, {"version": "1.4.2"}, thread_id="deploy-42", recipient="manager")
        await hitl.run(graph)                         # recover + receive Telegram clicks (one worker)

It composes ApprovalStore + RecipientRegistry + HitlBridge + TelegramApprovalBot; it does not
re-implement approval, delivery, polling or resume logic. LangGraph/Telegram are imported lazily.
"""
from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Callable

from .config import ConfigError, ensure_home, load_config
from .errors import UnknownRecipientError
from .recipients import RecipientRegistry
from .store import ApprovalStore


class HITL:
    def __init__(self, *, telegram_bot_token: str, db_path: str | os.PathLike, checkpoint_path: str | os.PathLike | None = None,
                 approval_ttl_s: int = 300, bot_username: str | None = None,
                 on_completed: Callable[[str, dict], Any] | None = None, app_name: str = "your application",
                 api: Any = None, clock: Callable[[], int] | None = None) -> None:
        if not telegram_bot_token:
            raise ConfigError("TELEGRAM_BOT_TOKEN is not set - run `langgraph-hitl setup`")
        self._token = telegram_bot_token
        self.store = ApprovalStore(str(db_path))
        self.recipients = RecipientRegistry(self.store)
        self.checkpoint_path = Path(checkpoint_path) if checkpoint_path else None
        self.checkpointer: Any = None
        self.approval_ttl_s = approval_ttl_s
        self.bot_username = bot_username
        self.on_completed = on_completed
        self.app_name = app_name
        self._api = api
        self._clock = clock or (lambda: int(time.time()))
        self._saver_cm: Any = None
        self._graph: Any = None
        self.bot: Any = None            # TelegramApprovalBot once a graph is bound

    def __repr__(self) -> str:
        return f"HITL(db={self.store!r}, bot=@{self.bot_username or '?'})"

    # ---------- construction ----------

    @classmethod
    def from_config(cls, path: str | os.PathLike | None = None, **kw: Any) -> "HITL":
        """Load ./.hitl (or ``path``/``$HITL_HOME``) created by `langgraph-hitl setup`."""
        cfg = load_config(Path(path) if path else None)
        if cfg.setup_state != "complete":
            raise ConfigError(f"HITL setup is not complete (state: {cfg.setup_state}) - run `langgraph-hitl setup`")
        return cls(telegram_bot_token=cfg.telegram_bot_token or "", db_path=cfg.db_path,
                   checkpoint_path=kw.pop("checkpoint_path", cfg.checkpoint_path), approval_ttl_s=cfg.approval_ttl_s,
                   bot_username=cfg.bot_username, **kw)

    @classmethod
    def from_env(cls, **kw: Any) -> "HITL":
        """Containers/CI: TELEGRAM_BOT_TOKEN, HITL_DB_PATH (default .hitl/hitl.db),
        HITL_CHECKPOINT_PATH (optional), APPROVAL_TTL_S, TELEGRAM_BOT_USERNAME (optional)."""
        db = Path(os.environ.get("HITL_DB_PATH", ".hitl/hitl.db"))
        if db.parent.name == ".hitl":
            ensure_home(db.parent)
        else:
            db.parent.mkdir(parents=True, exist_ok=True)
        return cls(telegram_bot_token=os.environ.get("TELEGRAM_BOT_TOKEN", ""), db_path=db,
                   checkpoint_path=kw.pop("checkpoint_path", os.environ.get("HITL_CHECKPOINT_PATH")),
                   approval_ttl_s=int(os.environ.get("APPROVAL_TTL_S", "300")),
                   bot_username=os.environ.get("TELEGRAM_BOT_USERNAME") or None, **kw)

    async def __aenter__(self) -> "HITL":
        if self.checkpoint_path is not None:
            from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

            from .store import _prepare_db_file
            _prepare_db_file(str(self.checkpoint_path))                 # created 0600, like hitl.db
            self._saver_cm = AsyncSqliteSaver.from_conn_string(str(self.checkpoint_path))
            self.checkpointer = await self._saver_cm.__aenter__()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self.bot is not None:
            await self.bot.aclose()
        if self._saver_cm is not None:
            await self._saver_cm.__aexit__(None, None, None)
            self._saver_cm = None
        self.store.close()

    # ---------- wiring ----------

    def bind(self, graph: Any) -> Any:
        """Attach the compiled graph (one per HITL) and build the existing Telegram stack."""
        if self.bot is not None:
            if graph is not None and graph is not self._graph:
                raise ValueError("this HITL instance is already bound to another graph")
            return self.bot
        if graph is None:
            raise ValueError("pass the compiled graph (hitl.start(graph, ...) or hitl.run(graph))")
        from .bridge import HitlBridge
        from .connections import ConnectionManager
        from .telegram import TelegramApprovalBot, TelegramConfig
        bridge = HitlBridge(graph, self.store, on_completed=self.on_completed)
        config = TelegramConfig(token=self._token, approval_ttl_s=self.approval_ttl_s, bot_username=self.bot_username)
        kw: dict[str, Any] = {"connections": ConnectionManager(self.store, self.bot_username),
                              "app_name": self.app_name, "clock": self._clock}
        if self._api is not None:
            kw["api"] = self._api
        self._graph, self.bot = graph, TelegramApprovalBot(config, bridge, **kw)
        return self.bot

    def recipient_id(self, name: str) -> str:
        """Registered name -> stable internal id (UnknownRecipientError otherwise; no fallback)."""
        return self.recipients.resolve(name).recipient_id

    # ---------- workflow ----------

    async def track(self, thread_id: str, recipient: str) -> None:
        """Pin ``thread_id`` to a registered recipient BEFORE invoking the graph (crash recovery)."""
        self.store.track_thread(thread_id, self.recipient_id(recipient), self._clock())

    async def start(self, graph: Any, graph_input: Any, *, thread_id: str, recipient: str | None = None) -> Any:
        """track (if ``recipient``) -> your graph run (durability="sync") -> deliver_pending.

        Returns the DeliveryResult (``state`` "pending" when an approval was sent, "completed" if the
        run needed none). Without ``recipient`` the approval node's ``request_approval(recipient=...)``
        literal decides (pinned on first delivery)."""
        bot = self.bind(graph)
        if recipient is not None:
            await self.track(thread_id, recipient)
        await graph.ainvoke(graph_input, {"configurable": {"thread_id": thread_id}}, durability="sync")
        return await bot.deliver_pending(thread_id)

    async def deliver_pending(self, thread_id: str) -> Any:
        if self.bot is None:
            raise ValueError("no graph bound yet - call hitl.start(graph, ...) or hitl.bind(graph) first")
        return await self.bot.deliver_pending(thread_id)

    async def run(self, graph: Any = None) -> None:
        """Validate the token, recover interrupted work, then receive Telegram clicks forever.
        Run in exactly ONE process per bot token."""
        from .telegram.onboarding import get_me
        bot = self.bind(graph if graph is not None else self._graph)
        me = await get_me(bot.api)                       # friendly InvalidBotTokenError on 401
        bot.connections.bot_username = self.bot_username = me.username
        await bot.recover()
        await bot.run_polling()


__all__ = ["HITL", "UnknownRecipientError"]
