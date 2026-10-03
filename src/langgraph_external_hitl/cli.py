"""`langgraph-hitl` command line (stdlib argparse).

    langgraph-hitl setup     one-time guided setup (Telegram), resumable and idempotent
    langgraph-hitl doctor    check configuration, permissions, database and bot

The bot token is read with hidden input (getpass) or from TELEGRAM_BOT_TOKEN with
--non-interactive; it is never accepted as a command-line argument and never printed.
"""
from __future__ import annotations

import argparse
import asyncio
import getpass
import os
import re
import shutil
import sys
import time
import warnings
from pathlib import Path
from typing import Any, Callable

from .config import (DB_FILE, HOME_DIRNAME, ConfigError, HitlConfig, load_config, permission_problems,
                     save_config, with_state)
from .errors import DuplicateRecipientError, HitlError, InvalidRecipientNameError, MissingDependencyError

TOKEN_RE = re.compile(r"^\d{5,}:[A-Za-z0-9_-]{30,}$")
LINK_TTL_S = 600
STATE_ORDER = ("new", "token_ok", "db_ok", "awaiting_connection", "complete")


class SetupAbort(Exception):
    """Stop setup with a user-facing message (state is kept)."""


def ok_glyph(stream: Any = None) -> str:
    """'✓' where the console can encode it, else '[ok]' (e.g. cp1252 Windows consoles)."""
    enc = getattr(stream or sys.stdout, "encoding", None) or "ascii"
    try:
        "✓".encode(enc)
        return "✓"
    except (UnicodeEncodeError, LookupError):
        return "[ok]"


class ConsoleIO:
    def __init__(self, input_fn: Callable[[str], str] = input, getpass_fn: Callable[[str], str] = getpass.getpass,
                 out: Any = None) -> None:
        self._input, self._getpass, self.out = input_fn, getpass_fn, out or sys.stdout
        self.ok = ok_glyph(self.out)

    def say(self, text: str = "") -> None:
        print(text, file=self.out, flush=True)

    def ask(self, prompt: str, default: str | None = None) -> str:
        suffix = f" [{default}]" if default is not None else ""
        value = self._input(f"{prompt}{suffix}: ").strip()
        return value or (default or "")

    def confirm(self, prompt: str, default: bool) -> bool:
        hint = "Y/n" if default else "y/N"
        value = self._input(f"{prompt} [{hint}]: ").strip().lower()
        return default if not value else value in ("y", "yes")

    def secret(self, prompt: str) -> str:
        with warnings.catch_warnings():
            warnings.simplefilter("error", getpass.GetPassWarning)
            try:
                return self._getpass(prompt).strip()
            except getpass.GetPassWarning:
                raise SetupAbort("This terminal cannot hide input. Set TELEGRAM_BOT_TOKEN in the environment and "
                                 "run `langgraph-hitl setup --non-interactive`.") from None


BOTFATHER_STEPS = """Create a bot with @BotFather (takes about a minute):
  1. In Telegram, open @BotFather
  2. Send /newbot
  3. Choose a display name (e.g. "Acme Approvals")
  4. Choose a username ending in "bot" (e.g. acme_approvals_bot)
  5. Copy the API token BotFather gives you (keep it secret)"""


def snippet(recipient: str) -> str:
    return f'''Use it in your LangGraph app:

    from langgraph_external_hitl import HITL, ApprovalOption, request_approval

    def approval_node(state):
        result = request_approval(            # first statement of the node
            recipient="{recipient}",
            title="Deploy application",
            message="Deploy version 1.4.2?",
            options=[ApprovalOption("approve", "Approve"), ApprovalOption("reject", "Reject")],
        )
        return {{"decision": result.option_id}}

    async with HITL.from_config() as hitl:    # reads ./{HOME_DIRNAME}
        graph = builder.compile(checkpointer=hitl.checkpointer)
        await hitl.start(graph, {{...}}, thread_id="job-1", recipient="{recipient}")
        await hitl.run(graph)                  # receives the Telegram decisions (one worker)'''


class _ConnectOnly:
    """Minimal bridge stand-in: setup only runs the Telegram connect flow (no graph)."""

    def __init__(self, store: Any) -> None:
        self.store = store


class SetupWizard:
    def __init__(self, project_dir: Path, io: ConsoleIO, *, api_factory: Callable[[str], Any] | None = None,
                 clock: Callable[[], float] = time.time, timeout_s: int = LINK_TTL_S, poll_s: int = 10,
                 non_interactive: bool = False, env: dict[str, str] | None = None) -> None:
        self.project_dir = Path(project_dir)
        self.home = self.project_dir / HOME_DIRNAME
        self.io = io
        self.api_factory = api_factory
        self.clock = clock
        self.timeout_s = timeout_s
        self.poll_s = poll_s
        self.non_interactive = non_interactive
        self.env = dict(os.environ) if env is None else env
        self._apis: list[Any] = []

    # ---------- helpers ----------

    def _api(self, token: str) -> Any:
        if self.api_factory is not None:
            api = self.api_factory(token)
        else:
            from .telegram._api import BotApi
            api = BotApi(token, self.poll_s)
        self._apis.append(api)
        return api

    async def _close(self) -> None:
        for api in self._apis:
            close = getattr(api, "aclose", None)
            if close is not None:
                try:
                    await close()
                except Exception:
                    pass

    def _load(self) -> HitlConfig | None:
        try:
            return load_config(self.home, env={})       # setup reads files only (env token used explicitly)
        except ConfigError:
            return None

    def _at_least(self, cfg: HitlConfig, state: str) -> bool:
        return STATE_ORDER.index(cfg.setup_state) >= STATE_ORDER.index(state)

    # ---------- main flow ----------

    async def run(self) -> int:
        try:
            return await self._run()
        finally:
            await self._close()

    async def _run(self) -> int:
        from .telegram.onboarding import TelegramSetupError  # noqa: F401  (ensures the extra is installed)
        io = self.io
        io.say("langgraph-external-hitl setup")
        io.say("Approval channel: Telegram (the only channel available today; Web and WhatsApp are planned).")
        io.say("Your own Telegram bot delivers approval requests; this library does not run a chat service.")
        io.say("")
        cfg = self._load()
        if cfg is not None and cfg.setup_state == "complete":
            action = await self._existing_menu(cfg)
            if action == "keep":
                io.say("Nothing changed.")
                return 0
            if action == "start_over":
                shutil.rmtree(self.home)
                cfg = None
            elif action == "reconnect":
                cfg = with_state(cfg, "db_ok")
            elif action == "replace_token":
                cfg = await self._token_step(cfg, force=True)
                cfg = with_state(cfg, "db_ok")
        elif cfg is not None and cfg.setup_state != "new":
            io.say(f"Resuming setup (saved progress: {cfg.setup_state}).")
        cfg = cfg or HitlConfig(home=self.home)
        if not self._at_least(cfg, "token_ok") or not cfg.telegram_bot_token:
            cfg = await self._token_step(cfg)
        else:
            cfg = await self._revalidate(cfg)
        await self._webhook_step(cfg)
        cfg = self._db_step(cfg)
        self._gitignore_step()
        cfg = self._recipient_step(cfg)
        return await self._connect_step(cfg)

    async def _existing_menu(self, cfg: HitlConfig) -> str:
        from .connections import ConnectionManager
        from .recipients import RecipientRegistry
        from .store import ApprovalStore
        from .telegram.onboarding import TelegramSetupError, get_me
        io = self.io
        status = "not connected"
        store = ApprovalStore(str(cfg.db_path))
        try:
            rec = RecipientRegistry(store).get(cfg.recipient) if cfg.recipient else None
            conn = ConnectionManager(store).get(rec.recipient_id) if rec else None
            status = conn.status if conn else "not connected"
        finally:
            store.close()
        try:
            me = await get_me(self._api(cfg.telegram_bot_token or ""))
            bot = f"@{me.username}"
        except TelegramSetupError as e:
            bot = f"@{cfg.bot_username} (problem: {e})"
        io.say("HITL is already configured.")
        io.say(f"  Telegram: {bot}")
        io.say(f"  Approver: {cfg.recipient}")
        io.say(f"  Status:   {status}")
        io.say("")
        io.say("  1. Keep")
        io.say("  2. Reconnect approver")
        io.say("  3. Replace bot token")
        io.say("  4. Start over (deletes .hitl/: approvals and connections)")
        if self.non_interactive:
            return "keep"
        choice = io.ask("Choose", "1")
        if choice == "4":
            if io.ask("Type 'yes' to delete all HITL data in .hitl/") != "yes":
                return "keep"
            return "start_over"
        return {"2": "reconnect", "3": "replace_token"}.get(choice, "keep")

    async def _token_step(self, cfg: HitlConfig, force: bool = False) -> HitlConfig:
        from .telegram.onboarding import InvalidBotTokenError, get_me
        io = self.io
        if self.non_interactive:
            token = self.env.get("TELEGRAM_BOT_TOKEN", "").strip()
            if not token:
                raise SetupAbort("--non-interactive needs TELEGRAM_BOT_TOKEN in the environment.")
            me = await get_me(self._api(token))
        else:
            io.say("Telegram setup")
            io.say("  1. Create a new Telegram bot")
            io.say("  2. I already have a Telegram bot")
            if io.ask("Choose", "2") == "1":
                io.say("")
                io.say(BOTFATHER_STEPS)
            io.say("")
            for attempt in range(3):
                token = io.secret("Paste your bot token (input is hidden): ")
                if not TOKEN_RE.match(token):
                    io.say("That does not look like a bot token (format 123456789:AA...). Try again.")
                    continue
                try:
                    me = await get_me(self._api(token))
                    break
                except InvalidBotTokenError as e:
                    io.say(str(e))
            else:
                raise SetupAbort("No valid bot token was entered.")
        if cfg.bot_id is not None and cfg.bot_id != me.id:
            io.say(f"Note: this is a different bot (@{me.username}); approvers must connect again.")
        io.say(f"{io.ok} Telegram bot validated: @{me.username}")
        state = cfg.setup_state if (force and self._at_least(cfg, "token_ok")) else "token_ok"
        return with_state(cfg, state, bot_id=me.id, bot_username=me.username, telegram_bot_token=token)

    async def _revalidate(self, cfg: HitlConfig) -> HitlConfig:
        from .telegram.onboarding import InvalidBotTokenError, get_me
        try:
            me = await get_me(self._api(cfg.telegram_bot_token or ""))
        except InvalidBotTokenError as e:
            self.io.say(str(e))
            return await self._token_step(with_state(cfg, "new"))
        self.io.say(f"{self.io.ok} Telegram bot validated: @{me.username}")
        return cfg

    async def _webhook_step(self, cfg: HitlConfig) -> None:
        from .telegram.onboarding import delete_webhook, webhook_url
        api = self._api(cfg.telegram_bot_token or "")
        url = await webhook_url(api)
        if not url:
            return
        host = re.sub(r"^https?://([^/]+).*$", r"\1", url)
        self.io.say(f"This bot has a webhook configured ({host}). This library receives decisions by polling,")
        self.io.say("which Telegram does not allow while a webhook is set.")
        if self.non_interactive or not self.io.confirm("Delete the webhook?", False):
            raise SetupAbort("Webhook kept; setup cannot continue. Use a dedicated bot or remove the webhook.")
        await delete_webhook(api)
        self.io.say(f"{self.io.ok} Webhook deleted")

    def _db_step(self, cfg: HitlConfig) -> HitlConfig:
        from .store import ApprovalStore
        ApprovalStore(str(self.home / DB_FILE)).close()      # create/migrate, 0600
        if not self._at_least(cfg, "db_ok"):
            cfg = with_state(cfg, "db_ok")
        return cfg

    def _gitignore_step(self) -> None:
        gi = self.project_dir / ".gitignore"
        if not gi.is_file() or self.non_interactive:
            return
        lines = [ln.strip() for ln in gi.read_text(encoding="utf-8").splitlines()]
        if any(ln in (".hitl", ".hitl/", "/.hitl", "/.hitl/") for ln in lines):
            return
        if self.io.confirm("Add .hitl/ to your project's .gitignore?", True):
            with gi.open("a", encoding="utf-8") as f:
                f.write(("" if gi.read_text(encoding="utf-8").endswith("\n") else "\n") + ".hitl/\n")

    def _recipient_step(self, cfg: HitlConfig) -> HitlConfig:
        from .recipients import RecipientRegistry, normalize_name
        from .store import ApprovalStore
        store = ApprovalStore(str(cfg.db_path))
        try:
            reg = RecipientRegistry(store)
            if cfg.recipient and reg.get(cfg.recipient):
                return cfg
            while True:
                name = "manager" if self.non_interactive else self.io.ask("Name this approver", "manager")
                try:
                    key = normalize_name(name)
                    if reg.get(key) is None:
                        reg.create(key)
                    elif not self.non_interactive and not self.io.confirm(
                            f"Approver '{key}' already exists. Connect it again?", True):
                        raise DuplicateRecipientError(f"approver '{key}' already exists")
                    if key != "manager":
                        self.io.say(f'Note: the README and examples use recipient="manager"; in your code use '
                                    f'recipient="{key}".')
                    return with_state(cfg, cfg.setup_state, recipient=key)
                except (InvalidRecipientNameError, DuplicateRecipientError) as e:
                    if self.non_interactive:
                        raise SetupAbort(str(e)) from None
                    self.io.say(f"{e}. Try again.")
        finally:
            store.close()

    async def _connect_step(self, cfg: HitlConfig) -> int:
        from .connections import ConnectionManager
        from .recipients import RecipientRegistry
        from .store import ApprovalStore
        from .telegram import TelegramApprovalBot, TelegramConfig
        from .telegram._handlers import ALLOWED_UPDATES
        from .telegram.onboarding import TelegramNetworkError, friendly
        io = self.io
        store = ApprovalStore(str(cfg.db_path))
        api = self._api(cfg.telegram_bot_token or "")
        try:
            rid = RecipientRegistry(store).resolve(cfg.recipient or "").recipient_id
            cm = ConnectionManager(store, cfg.bot_username)
            bot = TelegramApprovalBot(TelegramConfig(token=cfg.telegram_bot_token or "", bot_username=cfg.bot_username),
                                      _ConnectOnly(store), api=api, connections=cm, clock=lambda: int(self.clock()))
            while True:
                link = cm.create_link(rid, int(self.clock()), ttl_s=self.timeout_s)
                cfg = with_state(cfg, "awaiting_connection")
                io.say("")
                io.say(f"Connect your approver ({cfg.recipient}) in Telegram:")
                io.say("")
                io.say(f"    {link.url}")
                io.say("")
                io.say("Open the link, press Start, then press Connect.")
                io.say(f"Waiting for connection... (expires in {self.timeout_s // 60} min, Ctrl+C to pause)")
                deadline, offset, failures = self.clock() + self.timeout_s, None, 0
                while self.clock() < deadline:
                    try:
                        params: dict[str, Any] = {"timeout": self.poll_s, "allowed_updates": ALLOWED_UPDATES}
                        if offset is not None:
                            params["offset"] = offset
                        updates = await api.call("getUpdates", **params)
                        failures = 0
                    except Exception as e:  # noqa: BLE001
                        err = friendly(e, "getUpdates")
                        if isinstance(err, TelegramNetworkError) and failures < 5:
                            failures += 1
                            await asyncio.sleep(min(2 ** failures, 10))
                            continue
                        raise err from None
                    for u in updates:
                        offset = u["update_id"] + 1
                        await bot.handle_update(u)
                    conn = cm.get(rid)
                    if conn is not None and conn.is_active:
                        if offset is not None:     # acknowledge, so the app worker does not replay /start
                            try:
                                await api.call("getUpdates", offset=offset, timeout=0)
                            except Exception:
                                pass
                        with_state(cfg, "complete")
                        who = conn.label or (f"@{conn.handle}" if conn.handle else f"user {conn.actor_ref}")
                        io.say("")
                        io.say(f"{io.ok} Telegram connected successfully ({who})")
                        io.say(f"{io.ok} HITL setup complete")
                        io.say("")
                        io.say(snippet(cfg.recipient or "manager"))
                        return 0
                io.say("The link expired before the approver connected.")
                if self.non_interactive or not io.confirm("Create a new link?", True):
                    io.say("Run `langgraph-hitl setup` again when the approver is ready.")
                    return 1
        finally:
            store.close()


def doctor(project_dir: Path, io: ConsoleIO, api_factory: Callable[[str], Any] | None = None) -> int:
    async def run() -> int:
        from .connections import ConnectionManager
        from .recipients import RecipientRegistry
        from .store import ApprovalStore
        from .telegram.onboarding import TelegramSetupError, get_me, webhook_url
        problems = 0

        def check(good: bool, text: str) -> None:
            nonlocal problems
            problems += 0 if good else 1
            io.say(f"{io.ok if good else '[!!]'} {text}")
        try:
            cfg = load_config(project_dir / HOME_DIRNAME)
        except ConfigError as e:
            check(False, str(e))
            return 1
        check(cfg.setup_state == "complete", f"configuration: {cfg.home} (setup: {cfg.setup_state})")
        for p in permission_problems(cfg.home):
            check(False, p)
        store = ApprovalStore(str(cfg.db_path))
        try:
            check(True, f"database schema v{store.schema_version}")
            rec = RecipientRegistry(store).get(cfg.recipient) if cfg.recipient else None
            conn = ConnectionManager(store).get(rec.recipient_id) if rec else None
            check(bool(conn and conn.is_active), f"approver {cfg.recipient}: {conn.status if conn else 'not connected'}")
        finally:
            store.close()
        if api_factory is not None:
            api = api_factory(cfg.telegram_bot_token or "")
        else:
            from .telegram._api import BotApi
            api = BotApi(cfg.telegram_bot_token or "", 10)
        try:
            me = await get_me(api)
            check(True, f"Telegram bot @{me.username} (token accepted)")
            url = await webhook_url(api)
            check(not url, "no webhook set (polling works)" if not url else "a webhook is set - polling will fail")
        except TelegramSetupError as e:
            check(False, str(e))
        finally:
            close = getattr(api, "aclose", None)
            if close:
                await close()
        return 0 if problems == 0 else 1
    return asyncio.run(run())


def main(argv: list[str] | None = None, *, io: ConsoleIO | None = None,
         api_factory: Callable[[str], Any] | None = None) -> int:
    try:
        sys.stdout.reconfigure(errors="replace")  # type: ignore[union-attr]
    except (AttributeError, ValueError):
        pass
    from . import __version__
    parser = argparse.ArgumentParser(prog="langgraph-hitl", description="Human approvals for LangGraph via Telegram.")
    parser.add_argument("--version", action="version", version=f"langgraph-external-hitl {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)
    p_setup = sub.add_parser("setup", help="one-time guided setup (Telegram)")
    p_setup.add_argument("--project-dir", type=Path, default=Path.cwd(), help="project directory (default: current)")
    p_setup.add_argument("--non-interactive", action="store_true",
                         help="no prompts; read TELEGRAM_BOT_TOKEN from the environment")
    p_setup.add_argument("--timeout", type=int, default=LINK_TTL_S, help="seconds to wait for the approver (60-3600)")
    p_doc = sub.add_parser("doctor", help="check configuration, permissions, database and bot")
    p_doc.add_argument("--project-dir", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    io = io or ConsoleIO()
    try:
        if args.command == "setup":
            if not 60 <= args.timeout <= 3600:
                parser.error("--timeout must be between 60 and 3600 seconds")
            wizard = SetupWizard(args.project_dir, io, api_factory=api_factory, timeout_s=args.timeout,
                                 non_interactive=args.non_interactive)
            return asyncio.run(wizard.run())
        return doctor(args.project_dir, io, api_factory)
    except KeyboardInterrupt:
        io.say("")
        io.say("Setup paused. Your progress is saved - run `langgraph-hitl setup` again to continue.")
        return 130
    except EOFError:
        io.say("")
        io.say("Input ended unexpectedly. Run `langgraph-hitl setup` again in an interactive terminal.")
        return 1
    except MissingDependencyError as e:
        io.say(f"Error: {e}")
        return 2
    except (SetupAbort, ConfigError, HitlError) as e:
        io.say(f"Error: {e}")
        return 1


__all__ = ["ConsoleIO", "SetupWizard", "doctor", "main", "ok_glyph"]
