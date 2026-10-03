"""Phase 6: `langgraph-hitl setup` / `doctor` with scripted input and a simulated Telegram."""
import io
import os
import re
import stat
import subprocess
import sys

import pytest

from langgraph_external_hitl import ApprovalStore, ConnectionManager, RecipientRegistry
from langgraph_external_hitl.cli import ConsoleIO, main, ok_glyph
from langgraph_external_hitl.config import load_config
from langgraph_external_hitl.telegram import TelegramError

TOKEN = "123456789:AAH-cli-setup-secret-token-abcdefghijk"
TOKEN2 = "123456789:AAH-cli-setup-second-token-zyxwvutsrqp"
posix = pytest.mark.skipif(os.name != "posix", reason="POSIX permissions")


class FakeTelegram:
    """Simulates a user who opens the printed link, presses Start, then Connect."""

    def __init__(self, out, *, valid=(TOKEN, TOKEN2), webhook="", conflict=False, never_start=False,
                 interrupt_on_wait=False, network_down=False):
        self.out, self.valid, self.webhook = out, set(valid), webhook
        self.conflict, self.never_start, self.interrupt, self.network_down = conflict, never_start, interrupt_on_wait, network_down
        self.calls, self.confirm, self.mid, self.uid = [], None, 10, 0

    def factory(self, token):
        outer = self

        class Api:
            async def call(self, method, **p):
                outer.calls.append((method, p))
                if outer.network_down:
                    import httpx
                    raise httpx.ConnectError("down")
                if token not in outer.valid:
                    raise TelegramError(method, 401, "Unauthorized")
                return outer.handle(method, p)
        return Api()

    def handle(self, method, p):
        if method == "getMe":
            return {"id": 4242, "is_bot": True, "username": "my_bot", "first_name": "My Bot"}
        if method == "getWebhookInfo":
            return {"url": self.webhook}
        if method == "deleteWebhook":
            self.webhook = ""
            return True
        if method == "sendMessage":
            self.mid += 1
            kb = (p.get("reply_markup") or {}).get("inline_keyboard") or []
            data = [b["callback_data"] for row in kb for b in row]
            if data and data[0].startswith("c2:"):
                self.confirm = (data[0], self.mid)
            return {"message_id": self.mid}
        if method == "getUpdates":
            if p.get("timeout") == 0:
                return []
            if self.interrupt:
                raise KeyboardInterrupt
            if self.conflict:
                raise TelegramError("getUpdates", 409, "Conflict: terminated by other getUpdates request")
            self.uid += 1
            m = re.findall(r"start=(c_[A-Za-z0-9_-]{43})", self.out.getvalue())
            if self.never_start or not m:
                return []
            if self.confirm is None:
                return [{"update_id": self.uid, "message": {"from": {"id": 777, "first_name": "Rugved"},
                                                             "chat": {"id": 777, "type": "private"},
                                                             "text": f"/start {m[-1]}"}}]
            data, mid = self.confirm
            self.confirm = None
            return [{"update_id": self.uid, "callback_query": {"id": "cq", "from": {"id": 777}, "data": data,
                                                                "message": {"message_id": mid, "chat": {"id": 777}}}}]
        return True


def scripted(answers, secrets):
    out = io.StringIO()
    a, s = iter(answers), iter(secrets)
    con = ConsoleIO(input_fn=lambda prompt: (out.write(prompt), next(a))[1],
                    getpass_fn=lambda prompt: (out.write(prompt), next(s))[1], out=out)
    return con, out


def run_setup(tmp_path, answers, secrets, tg=None, extra=(), **tgkw):
    con, out = scripted(answers, secrets)
    tg = tg or FakeTelegram(out, **tgkw)
    tg.out = out
    code = main(["setup", "--project-dir", str(tmp_path), *extra], io=con, api_factory=tg.factory)
    return code, out.getvalue(), tg


def connection(tmp_path, name="manager"):
    s = ApprovalStore(str(tmp_path / ".hitl" / "hitl.db"))
    try:
        r = RecipientRegistry(s).get(name)
        return ConnectionManager(s).get(r.recipient_id) if r else None
    finally:
        s.close()


def test_fresh_setup_happy_path(tmp_path):
    code, out, tg = run_setup(tmp_path, ["2", ""], [TOKEN])
    assert code == 0, out
    assert "Telegram bot validated: @my_bot" in out and "press Start, then press Connect" in out
    assert "Telegram connected successfully (Rugved)" in out and "HITL setup complete" in out
    assert 'request_approval(' in out and 'recipient="manager"' in out             # developer snippet
    assert TOKEN not in out and TOKEN[12:] not in out                               # never printed
    cfg = load_config(tmp_path / ".hitl", env={})
    assert (cfg.setup_state, cfg.bot_username, cfg.bot_id, cfg.recipient, cfg.telegram_bot_token) == \
        ("complete", "my_bot", 4242, "manager", TOKEN)
    assert {p.name for p in (tmp_path / ".hitl").iterdir()} >= {".gitignore", "config.toml", "secrets.env", "hitl.db"}
    conn = connection(tmp_path)
    assert conn.is_active and conn.telegram_user_id == 777 and conn.chat_id == 777
    assert ("getUpdates", {"offset": tg.uid + 1, "timeout": 0}) in tg.calls        # updates acknowledged


@posix
def test_setup_permissions(tmp_path):
    run_setup(tmp_path, ["2", ""], [TOKEN])
    h = tmp_path / ".hitl"
    assert stat.S_IMODE(os.stat(h).st_mode) == 0o700
    assert stat.S_IMODE(os.stat(h / "secrets.env").st_mode) == 0o600
    assert stat.S_IMODE(os.stat(h / "hitl.db").st_mode) == 0o600


def test_create_new_bot_shows_botfather_steps(tmp_path):
    code, out, _ = run_setup(tmp_path, ["1", ""], [TOKEN])
    assert code == 0 and "@BotFather" in out and "/newbot" in out and 'ending in "bot"' in out


def test_invalid_then_valid_token_and_bad_format(tmp_path):
    code, out, _ = run_setup(tmp_path, ["2", ""], ["not-a-token", "987654321:AAH-wrong-token-xxxxxxxxxxxxxxxxx", TOKEN])
    assert code == 0
    assert "does not look like a bot token" in out and "rejected the bot token" in out
    assert "987654321:AAH-wrong" not in out


def test_three_invalid_tokens_abort_cleanly(tmp_path):
    bad = "987654321:AAH-wrong-token-xxxxxxxxxxxxxxxxx"
    code, out, _ = run_setup(tmp_path, ["2"], [bad, bad, bad])
    assert code == 1 and "No valid bot token" in out and "Traceback" not in out
    assert not (tmp_path / ".hitl" / "secrets.env").exists()


def test_network_failure_is_friendly(tmp_path):
    code, out, _ = run_setup(tmp_path, ["2"], [TOKEN], network_down=True)
    assert code == 1 and "Could not reach api.telegram.org" in out and "Traceback" not in out


def test_webhook_declined_keeps_webhook(tmp_path):
    code, out, tg = run_setup(tmp_path, ["2", "n"], [TOKEN], webhook="https://hooks.example.com/tg/secret-path")
    assert code == 1 and "hooks.example.com" in out and "secret-path" not in out
    assert not any(m == "deleteWebhook" for m, _ in tg.calls)
    assert load_config(tmp_path / ".hitl", env={}).setup_state == "token_ok"        # progress kept


def test_webhook_deleted_after_explicit_confirmation(tmp_path):
    code, out, tg = run_setup(tmp_path, ["2", "y", ""], [TOKEN], webhook="https://hooks.example.com/x")
    assert code == 0 and any(m == "deleteWebhook" for m, _ in tg.calls) and "Webhook deleted" in out


def test_poller_conflict_409(tmp_path):
    code, out, _ = run_setup(tmp_path, ["2", ""], [TOKEN], conflict=True)
    assert code == 1 and "409 Conflict" in out and "only one may poll" in out
    assert load_config(tmp_path / ".hitl", env={}).setup_state == "awaiting_connection"


def test_interrupt_then_resume(tmp_path):
    code, out, _ = run_setup(tmp_path, ["2", ""], [TOKEN], interrupt_on_wait=True)
    assert code == 130 and "Setup paused" in out and "Traceback" not in out
    assert load_config(tmp_path / ".hitl", env={}).setup_state == "awaiting_connection"
    code2, out2, _ = run_setup(tmp_path, [], [])                                    # no token asked again
    assert code2 == 0 and "Resuming setup (saved progress: awaiting_connection)" in out2
    assert "Paste your bot token" not in out2 and connection(tmp_path).is_active


def test_rerun_complete_keep_is_default_and_harmless(tmp_path):
    run_setup(tmp_path, ["2", ""], [TOKEN])
    before = (tmp_path / ".hitl" / "hitl.db").read_bytes()
    code, out, _ = run_setup(tmp_path, [""], [])
    assert code == 0 and "HITL is already configured." in out and "Telegram: @my_bot" in out
    assert "Approver: manager" in out and "Status:   active" in out and "Nothing changed." in out
    assert (tmp_path / ".hitl" / "hitl.db").read_bytes() == before


def test_rerun_reconnect_and_replace_token(tmp_path):
    run_setup(tmp_path, ["2", ""], [TOKEN])
    code, out, _ = run_setup(tmp_path, ["2"], [])                                   # 2 = reconnect
    assert code == 0 and out.count("start=c_") == 1 and connection(tmp_path).is_active
    code, out, _ = run_setup(tmp_path, ["3", "2"], [TOKEN2])                        # 3 = replace token
    assert code == 0 and load_config(tmp_path / ".hitl", env={}).telegram_bot_token == TOKEN2


def test_start_over_requires_typed_yes(tmp_path):
    run_setup(tmp_path, ["2", ""], [TOKEN])
    code, out, _ = run_setup(tmp_path, ["4", "no"], [])
    assert code == 0 and "Nothing changed." in out
    code, out, _ = run_setup(tmp_path, ["4", "yes", "2", "ops"], [TOKEN])
    assert code == 0 and load_config(tmp_path / ".hitl", env={}).recipient == "ops"


def test_invalid_and_duplicate_recipient_names(tmp_path):
    code, out, _ = run_setup(tmp_path, ["2", "Bad Name", "all", "lead"], [TOKEN])
    assert code == 0 and "invalid recipient name" in out and "is reserved" in out
    assert connection(tmp_path, "lead").is_active


def test_existing_recipient_name_reuse_prompt(tmp_path):
    (tmp_path / ".hitl").mkdir()
    s = ApprovalStore(str(tmp_path / ".hitl" / "hitl.db")); RecipientRegistry(s).create("manager"); s.close()
    code, out, _ = run_setup(tmp_path, ["2", "", "n", "backup"], [TOKEN])
    assert code == 0 and "already exists" in out and connection(tmp_path, "backup").is_active


def test_link_timeout_and_decline_new_link(tmp_path):
    t = {"now": 1_000_000.0}

    def clock():
        t["now"] += 100
        return t["now"]
    con, out = scripted(["2", "", "n"], [TOKEN])
    tg = FakeTelegram(out, never_start=True)
    from langgraph_external_hitl.cli import SetupWizard
    import asyncio
    code = asyncio.run(SetupWizard(tmp_path, con, api_factory=tg.factory, clock=clock, timeout_s=600).run())
    assert code == 1 and "link expired" in out.getvalue()
    assert load_config(tmp_path / ".hitl", env={}).setup_state == "awaiting_connection"


def test_non_interactive_uses_env_token(tmp_path, monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", TOKEN)
    code, out, _ = run_setup(tmp_path, [], [], extra=["--non-interactive"])
    assert code == 0 and load_config(tmp_path / ".hitl", env={}).recipient == "manager"
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN")
    code, out, _ = run_setup(tmp_path / "other", [], [], extra=["--non-interactive"])
    assert code == 1 and "TELEGRAM_BOT_TOKEN" in out


def test_project_gitignore_updated_only_after_consent(tmp_path):
    (tmp_path / ".gitignore").write_text("__pycache__/\n")
    run_setup(tmp_path, ["2", "y", ""], [TOKEN])
    assert ".hitl/" in (tmp_path / ".gitignore").read_text().splitlines()
    other = tmp_path / "p2"; other.mkdir(); (other / ".gitignore").write_text("x\n")
    run_setup(other, ["2", "n", ""], [TOKEN])
    assert (other / ".gitignore").read_text() == "x\n"


def test_no_token_cli_argument():
    with pytest.raises(SystemExit) as e:
        main(["setup", "--token", TOKEN])
    assert e.value.code == 2


def test_doctor(tmp_path):
    run_setup(tmp_path, ["2", ""], [TOKEN])
    con, out = scripted([], [])
    tg = FakeTelegram(out)
    assert main(["doctor", "--project-dir", str(tmp_path)], io=con, api_factory=tg.factory) == 0
    assert "database schema v5" in out.getvalue() and "approver manager: active" in out.getvalue()
    con2, out2 = scripted([], [])
    assert main(["doctor", "--project-dir", str(tmp_path / "nope")], io=con2, api_factory=tg.factory) == 1


def test_ok_glyph_fallback_for_legacy_consoles():
    class S:
        encoding = "cp1252"
    class U:
        encoding = "utf-8"
    assert ok_glyph(S()) == "[ok]" and ok_glyph(U()) == "✓"


def test_entry_points_and_cp1252_console(tmp_path):
    env = {**os.environ, "PYTHONIOENCODING": "cp1252"}
    r = subprocess.run([sys.executable, "-m", "langgraph_external_hitl", "doctor", "--project-dir", str(tmp_path)],
                       capture_output=True, text=True, env=env, timeout=60)
    assert r.returncode == 1 and "Traceback" not in r.stderr and "langgraph-hitl setup" in r.stdout
    r = subprocess.run([sys.executable, "-m", "langgraph_external_hitl", "--version"], capture_output=True, text=True)
    assert r.returncode == 0 and "langgraph-external-hitl" in r.stdout


def test_missing_telegram_extra_is_explained(tmp_path):
    code = ("import sys; sys.modules['httpx'] = None\n"
            "from langgraph_external_hitl.cli import main\n"
            f"raise SystemExit(main(['setup', '--project-dir', {str(tmp_path)!r}]))\n")
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, stdin=subprocess.DEVNULL, timeout=60)
    assert r.returncode == 2 and "langgraph-external-hitl[telegram]" in r.stdout and "Traceback" not in r.stderr


def test_non_default_name_warns_about_examples(tmp_path):
    code, out, _ = run_setup(tmp_path, ["2", "lead"], [TOKEN])
    assert code == 0 and 'in your code use recipient="lead"' in out
    code, out, _ = run_setup(tmp_path / "p2", ["2", ""], [TOKEN])
    assert "in your code use" not in out                                   # default name: no note
