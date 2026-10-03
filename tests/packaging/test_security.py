"""Packaging-related secret handling."""
import pytest

from langgraph_external_hitl.telegram import TelegramConfig
from langgraph_external_hitl.telegram._api import BotApi

TOKEN = "123456:SECRET-TOKEN-VALUE"


def test_config_repr_hides_token():
    cfg = TelegramConfig(token=TOKEN, approver_user_ids=frozenset({1}))
    assert TOKEN not in repr(cfg) and TOKEN not in str(cfg)


def test_bot_api_repr_hides_token():
    assert TOKEN not in repr(BotApi(TOKEN))


def test_from_env_reads_part1_names():
    cfg = TelegramConfig.from_env({"TELEGRAM_BOT_TOKEN": TOKEN, "APPROVER_USER_IDS": "1, 2",
                                   "APPROVAL_TTL_S": "20"})
    assert cfg.approver_user_ids == frozenset({1, 2}) and cfg.approval_ttl_s == 20


def test_empty_token_rejected():
    with pytest.raises(ValueError):
        TelegramConfig(token="")
    with pytest.raises(ValueError):
        TelegramConfig.from_env({})
