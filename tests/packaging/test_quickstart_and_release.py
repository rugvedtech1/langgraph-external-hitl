"""Release checks: README quickstart == examples/quickstart.py, quickstart graph works,
deprecation of start(), no connection leak when opening fails."""
import asyncio
import importlib.util
import re
import sqlite3
import sys
import warnings
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
QS = ROOT / "examples" / "quickstart.py"
README = ROOT / "README.md"


def _quickstart_module():
    spec = importlib.util.spec_from_file_location("quickstart_example", QS)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.mark.skipif(not QS.is_file(), reason="examples not available")
def test_readme_quickstart_is_identical_to_example():
    m = re.search(r"<!-- quickstart:start -->\n```python\n(.*?)```\n<!-- quickstart:end -->", README.read_text(), re.S)
    assert m, "README quickstart markers missing"
    assert m.group(1) == QS.read_text(), "README quickstart drifted from examples/quickstart.py"


@pytest.mark.skipif(not QS.is_file(), reason="examples not available")
def test_quickstart_graph_pauses_and_resumes():
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.types import Command

    from langgraph_external_hitl.digest import payload_digest
    qs = _quickstart_module()
    g = qs.build_graph(InMemorySaver())
    cfg = {"configurable": {"thread_id": "job-q"}}

    async def t():
        out = await g.ainvoke({"task": "Deploy v1"}, cfg)
        intr = out["__interrupt__"][0]
        assert intr.value["title"] == "Approve production deployment?"
        assert [o["id"] for o in intr.value["options"]] == ["approve", "reject"]
        done = await g.ainvoke(Command(resume={intr.id: {"option_id": "reject",
                                                         "payload_digest": payload_digest(intr.value)}}), cfg)
        assert done["result"] == "cancelled: Deploy v1"
    asyncio.run(t())


def test_start_is_deprecated(tmp_path):
    from langgraph_external_hitl import ApprovalStore
    from langgraph_external_hitl.bridge import HitlBridge

    class G:
        async def ainvoke(self, *a, **k):
            return {}
    s = ApprovalStore(str(tmp_path / "a.db"))
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        with pytest.raises(Exception):
            asyncio.run(HitlBridge(G(), s).start(approver_user_id=1, chat_id=1, action="x", now=0, ttl_s=60))
    assert any(issubclass(x.category, DeprecationWarning) and "start() is deprecated" in str(x.message) for x in w)
    s.close()


def test_failed_open_does_not_leak_connection(tmp_path, monkeypatch):
    """Regression: a failed open (newer schema) must close its sqlite3 connection
    (otherwise Python 3.13+ reports ResourceWarning: unclosed database)."""
    import langgraph_external_hitl.store as store_mod
    from langgraph_external_hitl import ApprovalStore, SchemaVersionError
    p = str(tmp_path / "a.db")
    c = sqlite3.connect(p); c.execute("PRAGMA user_version = 99"); c.close()
    opened = []
    real = sqlite3.connect

    def spy(*a, **k):
        conn = real(*a, **k)
        opened.append(conn)
        return conn
    monkeypatch.setattr(store_mod.sqlite3, "connect", spy)
    with pytest.raises(SchemaVersionError):
        ApprovalStore(p)
    assert len(opened) == 1
    with pytest.raises(sqlite3.ProgrammingError):     # "Cannot operate on a closed database"
        opened[0].execute("SELECT 1")


def test_frozen_names_not_importable_from_package_root():
    import langgraph_external_hitl as h
    import langgraph_external_hitl.telegram as t
    for name in ("payload_digest", "validate_options", "ConsumeResult", "ThreadRecord", "SCHEMA_VERSION"):
        assert not hasattr(h, name), name
    for name in ("keyboard", "render", "parse_callback_data", "TOASTS", "ALLOWED_UPDATES"):
        assert not hasattr(t, name), name


APP = ROOT / "examples" / "app.py"


@pytest.mark.skipif(not APP.is_file(), reason="examples not available")
def test_readme_app_example_is_identical():
    m = re.search(r"<!-- app:start -->\n```python\n(.*?)```\n<!-- app:end -->", README.read_text(), re.S)
    assert m and m.group(1) == APP.read_text(), "README app example drifted from examples/app.py"


@pytest.mark.skipif(not APP.is_file(), reason="examples not available")
def test_app_example_graph_pauses_with_recipient():
    from langgraph.checkpoint.memory import InMemorySaver
    spec = importlib.util.spec_from_file_location("app_example", APP)
    mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
    g = mod.build(InMemorySaver())
    out = asyncio.run(g.ainvoke({"version": "1.4.2"}, {"configurable": {"thread_id": "t"}}))
    v = out["__interrupt__"][0].value
    assert v["v"] == 3 and v["recipient"] == "manager" and [o["id"] for o in v["options"]] == \
        ["production", "staging", "reject"]



@pytest.mark.skipif(not APP.is_file(), reason="examples not available")
@pytest.mark.parametrize("case", ["no_setup", "unknown_approver"])
def test_app_example_errors_are_friendly(tmp_path, case):
    import os
    import subprocess
    import sys as _sys
    env = {k: v for k, v in os.environ.items() if k not in ("HITL_HOME", "TELEGRAM_BOT_TOKEN")}
    if case == "unknown_approver":                    # configured, but the approver is named differently
        from langgraph_external_hitl import ApprovalStore, RecipientRegistry
        from langgraph_external_hitl.config import HitlConfig, save_config
        save_config(HitlConfig(home=tmp_path / ".hitl", bot_username="b", setup_state="complete",
                               recipient="testerwired", telegram_bot_token="1:T" + "x" * 30))
        s = ApprovalStore(str(tmp_path / ".hitl" / "hitl.db")); RecipientRegistry(s).create("testerwired"); s.close()
    r = subprocess.run([_sys.executable, str(APP)], cwd=tmp_path, env=env, capture_output=True, text=True, timeout=120)
    assert r.returncode == 1 and "Traceback" not in r.stderr, r.stderr
    expected = "langgraph-hitl setup" if case == "no_setup" else "registered approvers: testerwired"
    assert expected in r.stderr
