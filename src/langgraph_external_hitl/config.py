"""Project-local HITL configuration (stdlib only).

Layout created by ``langgraph-hitl setup``::

    .hitl/
        .gitignore      "*"  (never commit anything in this directory)
        config.toml     non-secret settings + setup state
        secrets.env     TELEGRAM_BOT_TOKEN=...   (0600)
        hitl.db         approvals database        (0600)

Precedence: explicit arguments > environment (TELEGRAM_BOT_TOKEN, APPROVAL_TTL_S, HITL_HOME)
> files in .hitl/ (found by walking up from the current directory) > defaults.
"""
from __future__ import annotations

import os
import re
import tempfile
import tomllib
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

HOME_DIRNAME = ".hitl"
CONFIG_FILE = "config.toml"
SECRETS_FILE = "secrets.env"
DB_FILE = "hitl.db"
CHECKPOINT_FILE = "checkpoints.db"
SETUP_STATES = ("new", "token_ok", "db_ok", "awaiting_connection", "complete")
_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ConfigError(Exception):
    """Configuration missing or invalid (shown to users without a traceback)."""


@dataclass(frozen=True)
class HitlConfig:
    home: Path
    channel: str = "telegram"
    bot_id: int | None = None
    bot_username: str | None = None
    approval_ttl_s: int = 300
    recipient: str | None = None                 # the approver configured by setup (friendly name)
    setup_state: str = "new"
    telegram_bot_token: str | None = field(default=None, repr=False)

    @property
    def db_path(self) -> Path:
        return self.home / DB_FILE

    @property
    def checkpoint_path(self) -> Path:
        return self.home / CHECKPOINT_FILE


def find_home(start: Path | None = None) -> Path | None:
    """``$HITL_HOME`` or the nearest ``.hitl`` directory containing config.toml (walking up)."""
    env = os.environ.get("HITL_HOME")
    if env:
        return Path(env)
    here = (start or Path.cwd()).resolve()
    for d in (here, *here.parents):
        if (d / HOME_DIRNAME / CONFIG_FILE).is_file():
            return d / HOME_DIRNAME
    return None


def parse_env_file(path: Path) -> dict[str, str]:
    """Minimal KEY=VALUE parser: comments, blank lines, optional single/double quotes."""
    out: dict[str, str] = {}
    if not path.is_file():
        return out
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key, value = key.strip(), value.strip()
        if not _KEY_RE.match(key):
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        out[key] = value
    return out


def ensure_home(home: Path) -> Path:
    """Create .hitl/ (0700 on POSIX) with a '*' .gitignore. Idempotent."""
    home.mkdir(parents=True, exist_ok=True)
    if os.name == "posix":
        os.chmod(home, 0o700)
    gi = home / ".gitignore"
    if not gi.exists():
        _atomic_write(gi, "# created by langgraph-external-hitl: never commit secrets or databases\n*\n", 0o644)
    return home


def _atomic_write(path: Path, text: str, mode: int) -> None:
    """Write via a private temp file + os.replace, so readers never see a partial file and
    secrets never exist with permissive modes (mode set before content is written)."""
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    try:
        if os.name == "posix":
            os.fchmod(fd, mode)
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise


def _toml_value(v: Any) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    s = str(v).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{s}"'


def save_config(cfg: HitlConfig) -> None:
    """Write config.toml (non-secret) and, if present, secrets.env (0600)."""
    ensure_home(cfg.home)
    values = {"version": 1, "channel": cfg.channel, "setup_state": cfg.setup_state,
              "approval_ttl_s": cfg.approval_ttl_s}
    if cfg.recipient:
        values["recipient"] = cfg.recipient
    tg = {k: v for k, v in (("bot_id", cfg.bot_id), ("bot_username", cfg.bot_username)) if v is not None}
    lines = ["# langgraph-external-hitl configuration (no secrets here; see secrets.env)"]
    lines += [f"{k} = {_toml_value(v)}" for k, v in values.items()]
    if tg:
        lines += ["", "[telegram]"] + [f"{k} = {_toml_value(v)}" for k, v in tg.items()]
    _atomic_write(cfg.home / CONFIG_FILE, "\n".join(lines) + "\n", 0o644)
    if cfg.telegram_bot_token is not None:
        _atomic_write(cfg.home / SECRETS_FILE,
                      "# secret: do not commit or share\nTELEGRAM_BOT_TOKEN=" + cfg.telegram_bot_token + "\n", 0o600)


def load_config(home: Path | None = None, *, env: dict[str, str] | None = None) -> HitlConfig:
    """Load .hitl/ (discovered when ``home`` is None) with environment overrides."""
    env = dict(os.environ) if env is None else env
    home = home or find_home()
    if home is None:
        raise ConfigError("no .hitl/ configuration found - run `langgraph-hitl setup` in your project")
    home = Path(home)
    path = home / CONFIG_FILE
    if not path.is_file():
        raise ConfigError(f"{path} not found - run `langgraph-hitl setup`")
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as e:
        raise ConfigError(f"{path} is not valid TOML: {e}") from None
    tg = data.get("telegram", {})
    secrets = parse_env_file(home / SECRETS_FILE)
    token = env.get("TELEGRAM_BOT_TOKEN") or secrets.get("TELEGRAM_BOT_TOKEN")
    ttl = env.get("APPROVAL_TTL_S") or data.get("approval_ttl_s", 300)
    try:
        ttl = int(ttl)
    except (TypeError, ValueError):
        raise ConfigError("APPROVAL_TTL_S must be an integer") from None
    state = data.get("setup_state", "new")
    return HitlConfig(home=home, channel=data.get("channel", "telegram"), bot_id=tg.get("bot_id"),
                      bot_username=tg.get("bot_username"), approval_ttl_s=ttl, recipient=data.get("recipient"),
                      setup_state=state if state in SETUP_STATES else "new", telegram_bot_token=token)


def with_state(cfg: HitlConfig, state: str, **changes: Any) -> HitlConfig:
    """Persist a new setup state (and other changes). Returns the updated config."""
    if state not in SETUP_STATES:
        raise ValueError(f"unknown setup state {state!r}")
    new = replace(cfg, setup_state=state, **changes)
    save_config(new)
    return new


def permission_problems(home: Path) -> list[str]:
    """POSIX permission checks used by `doctor` (empty list = fine or not applicable)."""
    if os.name != "posix":
        return []
    out = []
    checks = [(home, 0o700)] + [(home / f, 0o600) for f in (SECRETS_FILE, DB_FILE) if (home / f).exists()]
    for p, want in checks:
        mode = os.stat(p).st_mode & 0o777
        if mode & 0o077:
            out.append(f"{p} has permissions {oct(mode)}; expected {oct(want)}")
    return out


__all__ = ["ConfigError", "HitlConfig", "SETUP_STATES", "ensure_home", "find_home", "load_config",
           "parse_env_file", "permission_problems", "save_config", "with_state"]
