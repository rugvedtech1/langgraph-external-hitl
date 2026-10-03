"""Phase 2: project-local configuration."""
import os
import stat

import pytest

from langgraph_external_hitl.config import (ConfigError, HitlConfig, ensure_home, find_home, load_config,
                                            parse_env_file, permission_problems, save_config, with_state)

posix = pytest.mark.skipif(os.name != "posix", reason="POSIX permissions")
TOKEN = "123456789:AAH-config-test-token-abcdefghijklmnopq"


def test_save_and_load_roundtrip(tmp_path):
    home = tmp_path / ".hitl"
    cfg = HitlConfig(home=home, bot_id=42, bot_username="my_bot", recipient="manager",
                     setup_state="complete", telegram_bot_token=TOKEN)
    save_config(cfg)
    got = load_config(home, env={})
    assert (got.bot_id, got.bot_username, got.recipient, got.setup_state, got.telegram_bot_token) == \
        (42, "my_bot", "manager", "complete", TOKEN)
    assert TOKEN not in (home / "config.toml").read_text()          # token only in secrets.env
    assert TOKEN not in repr(got)
    assert (home / ".gitignore").read_text().strip().endswith("*")


@posix
def test_permissions(tmp_path):
    home = tmp_path / ".hitl"
    save_config(HitlConfig(home=home, telegram_bot_token=TOKEN))
    assert stat.S_IMODE(os.stat(home).st_mode) == 0o700
    assert stat.S_IMODE(os.stat(home / "secrets.env").st_mode) == 0o600
    assert permission_problems(home) == []
    os.chmod(home / "secrets.env", 0o644)
    assert permission_problems(home) and "secrets.env" in permission_problems(home)[0]


def test_env_overrides_file(tmp_path):
    home = tmp_path / ".hitl"
    save_config(HitlConfig(home=home, telegram_bot_token=TOKEN, approval_ttl_s=300))
    got = load_config(home, env={"TELEGRAM_BOT_TOKEN": "999:override-token-xxxxxxxx", "APPROVAL_TTL_S": "60"})
    assert got.telegram_bot_token == "999:override-token-xxxxxxxx" and got.approval_ttl_s == 60
    with pytest.raises(ConfigError):
        load_config(home, env={"APPROVAL_TTL_S": "soon"})


def test_find_home_walks_up_and_env(tmp_path, monkeypatch):
    monkeypatch.delenv("HITL_HOME", raising=False)
    save_config(HitlConfig(home=tmp_path / ".hitl"))
    deep = tmp_path / "a" / "b"
    deep.mkdir(parents=True)
    assert find_home(deep) == tmp_path / ".hitl"
    assert find_home(tmp_path.parent / "elsewhere-does-not-exist") in (None, find_home(tmp_path.parent))
    monkeypatch.setenv("HITL_HOME", str(tmp_path / "custom"))
    assert find_home(deep) == tmp_path / "custom"


def test_missing_and_invalid_config(tmp_path, monkeypatch):
    monkeypatch.delenv("HITL_HOME", raising=False)
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ConfigError, match="langgraph-hitl setup"):
        load_config(env={})
    ensure_home(tmp_path / ".hitl")
    (tmp_path / ".hitl" / "config.toml").write_text("this is = = not toml")
    with pytest.raises(ConfigError, match="not valid TOML"):
        load_config(tmp_path / ".hitl", env={})


def test_state_persistence_and_unknown_state(tmp_path):
    cfg = HitlConfig(home=tmp_path / ".hitl")
    cfg = with_state(cfg, "token_ok", bot_username="b")
    assert load_config(cfg.home, env={}).setup_state == "token_ok"
    with pytest.raises(ValueError):
        with_state(cfg, "bogus")
    (cfg.home / "config.toml").write_text('setup_state = "weird"\n')
    assert load_config(cfg.home, env={}).setup_state == "new"


def test_env_parser(tmp_path):
    p = tmp_path / "x.env"
    p.write_text("# c\n\nA=1\nB='two words'\nC=\"q\"\nbad key=1\nNOEQ\nD=a=b\n")
    assert parse_env_file(p) == {"A": "1", "B": "two words", "C": "q", "D": "a=b"}
    assert parse_env_file(tmp_path / "missing.env") == {}


def test_ensure_home_idempotent(tmp_path):
    h = ensure_home(tmp_path / ".hitl")
    (h / ".gitignore").write_text("custom\n")
    ensure_home(h)
    assert (h / ".gitignore").read_text() == "custom\n"                 # not overwritten
