"""Real OS processes running examples/quickstart.py against a mocked Telegram server.

Process 1: connect once via deep link (/start <token> -> Connect); the approval is sent
           automatically right after connecting (no /start per approval, no Enter key).
Process 2: restart with the same JOB_ID -> no duplicate message -> user taps Approve ->
           the same LangGraph thread resumes and completes -> quickstart prints RESULT and exits.
A pre-existing Part 2 approvals database is migrated (schema v3) on first open.
"""
import json
import os
import sqlite3
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

TOKEN = "123456:E2E-SECRET-TOKEN"
EXAMPLES = Path(__file__).resolve().parents[2] / "examples"

RUNNER = textwrap.dedent('''
    import asyncio, json, os, sys
    import httpx
    phase, examples = sys.argv[1], sys.argv[2]
    sys.path.insert(0, examples)
    STATE = "state.json"
    def load():
        return json.load(open(STATE)) if os.path.exists(STATE) else {}
    def save(**kw):
        st = load(); st.update(kw); json.dump(st, open(STATE, "w"))

    import langgraph_external_hitl.connections as connmod
    _orig_link = connmod.ConnectionManager.create_link
    def _link(self, *a, **k):
        link = _orig_link(self, *a, **k); save(link_token=link.token); return link
    connmod.ConnectionManager.create_link = _link

    polls = {"n": 0}
    counter = {"mid": 100}
    def click(user, data, mid):
        return {"id": f"cq{mid}", "from": {"id": user}, "data": data, "message": {"message_id": mid, "chat": {"id": 42}}}
    def queue(poll):
        st = load()
        if phase == "1" and poll == 1:
            return [{"update_id": 1, "message": {"from": {"id": 42, "first_name": "R"}, "chat": {"id": 42, "type": "private"},
                                                 "text": "/start " + st["link_token"]}}]
        if phase == "1" and poll == 2:
            return [{"update_id": 2, "callback_query": click(42, st["confirm"], st["confirm_mid"])}]
        if phase == "2" and poll == 1:
            return [{"update_id": 3, "callback_query": click(999, st["approve"], st["mid"])},   # wrong user
                    {"update_id": 4, "callback_query": click(42, st["approve"], st["mid"])},
                    {"update_id": 5, "callback_query": click(42, st["approve"], st["mid"])}]   # duplicate
        return None
    def handler(req):
        method = req.url.path.rsplit("/", 1)[1]
        body = json.loads(req.content or b"{}")
        if method == "getMe":
            return httpx.Response(200, json={"ok": True, "result": {"id": 1, "username": "fake_bot"}})
        if method == "getWebhookInfo":
            return httpx.Response(200, json={"ok": True, "result": {"url": ""}})
        if method == "getUpdates":
            polls["n"] += 1
            q = queue(polls["n"])
            if q is None:
                raise SystemExit(0)
            return httpx.Response(200, json={"ok": True, "result": q})
        if method == "sendMessage":
            counter["mid"] += 1
            kb = (body.get("reply_markup") or {}).get("inline_keyboard") or []
            data = [b["callback_data"] for row in kb for b in row]
            if data and data[0].startswith("c2:"):
                save(confirm=data[0], confirm_mid=counter["mid"])
            elif data and data[0].startswith("v2:"):
                save(approve=data[0], mid=counter["mid"], labels=[b["text"] for row in kb for b in row], text=body["text"])
                print("  [telegram] approval message sent")
            return httpx.Response(200, json={"ok": True, "result": {"message_id": counter["mid"]}})
        if method == "answerCallbackQuery":
            print(f"  [telegram] answer: {body.get('text')!r}")
        return httpx.Response(200, json={"ok": True, "result": True})
    _orig = httpx.AsyncClient
    httpx.AsyncClient = lambda **kw: _orig(transport=httpx.MockTransport(handler), **kw)
    import quickstart
    try:
        asyncio.run(quickstart.main("Deploy web-app v2.4.1 to production", "job-1"))
    except SystemExit:
        pass
''')

PART2_DDL = """CREATE TABLE approvals (
    approval_id TEXT PRIMARY KEY, approver_user_id INTEGER NOT NULL, chat_id INTEGER NOT NULL,
    message_id INTEGER, action TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending','approved','rejected','expired')),
    created_at INTEGER NOT NULL, expires_at INTEGER NOT NULL, decided_at INTEGER)"""


@pytest.mark.skipif(not (EXAMPLES / "quickstart.py").is_file(), reason="examples not available")
def test_quickstart_connect_once_then_restart_and_approve(tmp_path):
    c = sqlite3.connect(tmp_path / "hitl.db")
    c.execute(PART2_DDL)
    c.execute("INSERT INTO approvals VALUES ('legacy', 42, 42, 8, 'old', 'approved', 1, 2, 2)")
    c.commit(); c.close()
    (tmp_path / "runner.py").write_text(RUNNER)
    env = {**os.environ, "TELEGRAM_BOT_TOKEN": TOKEN, "LANGGRAPH_STRICT_MSGPACK": "true"}

    def run(phase):
        r = subprocess.run([sys.executable, "runner.py", phase, str(EXAMPLES)], cwd=tmp_path, env=env,
                           stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=120)
        assert r.returncode == 0, r.stdout + r.stderr
        return r.stdout + r.stderr

    out1 = run("1")
    st = json.loads((tmp_path / "state.json").read_text())
    assert "Connect once: open https://t.me/fake_bot?start=c_" in out1
    assert out1.count("approval message sent") == 1 and "RESULT" not in out1
    assert st["labels"] == ["Approve", "Reject"] and st["text"].startswith("<b>Approve production deployment?</b>")

    out2 = run("2")                                     # restart: connection persists, no new link
    assert "Connect once" not in out2 and "approval message sent" not in out2      # no duplicate
    assert "You are not authorized to decide this request." in out2
    assert out2.count("RESULT job-1: done: Deploy web-app v2.4.1 to production") == 1
    assert "Already decided: Approve." in out2
    for out in (out1, out2):
        assert TOKEN not in out

    if os.name == "posix":                              # quickstart sets umask 077
        import stat
        # hitl.db was pre-created by this test (legacy fixture); checkpoints.db is created by LangGraph
        assert stat.S_IMODE(os.stat(tmp_path / "checkpoints.db").st_mode) == 0o600
    db = sqlite3.connect(tmp_path / "hitl.db")
    assert db.execute("PRAGMA user_version").fetchone()[0] == 5
    assert db.execute("SELECT status, selected_option_id FROM approvals WHERE thread_id='job-1'").fetchall() == \
        [("decided", "approve")]
    assert db.execute("SELECT status, selected_option_id FROM approvals WHERE approval_id='legacy'").fetchone() == \
        ("decided", "approve")
    assert db.execute("SELECT status, completion_notified_at IS NOT NULL FROM hitl_threads").fetchall() == \
        [("completed", 1)]
    db.close()
