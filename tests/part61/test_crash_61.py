"""Part 6.1 / T6, T7, T9 + crash between A's resume and B's delivery: real process kills."""
import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

HERE = Path(__file__).parent
WORKER = textwrap.dedent('''
    import asyncio, json, os, sys
    sys.path.insert(0, sys.argv[2])
    from graphs61 import build
    from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
    from langgraph_external_hitl import ApprovalStore, ConnectionManager
    from langgraph_external_hitl.bridge import HitlBridge
    from langgraph_external_hitl.telegram import TelegramApprovalBot, TelegramConfig

    phase = sys.argv[1]
    NOW = int(os.environ.get("NOW", "1000000"))

    def log(kind, **kw):
        with open("events.jsonl", "a") as f:
            f.write(json.dumps({"kind": kind, **kw}) + "\\n"); f.flush(); os.fsync(f.fileno())

    class Api:
        async def call(self, method, **p):
            if method == "sendMessage":
                n = len([l for l in open("events.jsonl")] if os.path.exists("events.jsonl") else []) + 600
                kb = p.get("reply_markup", {}).get("inline_keyboard", [])
                aid = kb[0][0]["callback_data"].split(":")[1] if kb else None
                log("send", message_id=n, approval_id=aid, text=p["text"][:40])
                return {"message_id": n}
            if method == "answerCallbackQuery":
                log("answer", text=p["text"])
            return True

    def on_completed(tid, values):
        log("completed", thread_id=tid, result=values.get("result"))
        if phase == "crash_in_completion":
            os._exit(9)          # side effect done, notification not recorded

    async def main():
        executed = []
        async with AsyncSqliteSaver.from_conn_string("cp.db") as saver:
            store = ApprovalStore("a.db")
            cm = ConnectionManager(store, "bot")
            if cm.get("user-A") is None:
                cm.seed("user-A", 111, 111, NOW)
            bridge = HitlBridge(build(saver, executed), store, on_completed=on_completed)
            bot = TelegramApprovalBot(TelegramConfig(token="1:T"), bridge, api=Api(), clock=lambda: NOW,
                                      connections=cm)
            if phase in ("pause_crash", "send_crash"):
                amount = int(os.environ.get("AMOUNT", "500"))
                await bot.track("job-1", "user-A")
                await bridge.graph.ainvoke({"amount": amount}, {"configurable": {"thread_id": "job-1"}},
                                           durability="sync")
                if phase == "pause_crash":
                    os._exit(9)                                   # paused, nothing delivered
                store.mark_sent = lambda *a, **k: os._exit(9)  # sent, not recorded
                await bot.deliver_pending("job-1")
            elif phase == "recover":
                rep = await bot.recover()
                print(json.dumps({"sent": [s for d in rep.deliveries for s in d.sent],
                                  "states": [d.state for d in rep.deliveries],
                                  "completed": rep.completions_notified}))
            elif phase in ("click", "crash_after_resume", "crash_in_completion"):
                if phase == "crash_after_resume":
                    async def boom(*a, **k):
                        os._exit(9)                               # A resumed, B not delivered
                    bot.service.deliver_pending = boom
                aid, idx, mid = sys.argv[3], int(sys.argv[4]), int(sys.argv[5])
                await bot.handle_update({"update_id": 1, "callback_query": {
                    "id": "q", "from": {"id": 111}, "data": f"v2:{aid}:{idx}",
                    "message": {"message_id": mid, "chat": {"id": 111}}}})
            for e in executed:
                log("executed", amount=e[0], manager=e[1], finance=e[2])
            store.close()
    asyncio.run(main())
''')


def write_worker(tmp_path):
    (tmp_path / "worker.py").write_text(WORKER)


def run(tmp_path, *args, env=None):
    return subprocess.run([sys.executable, "worker.py", args[0], str(HERE), *map(str, args[1:])],
                          cwd=tmp_path, env={**os.environ, **(env or {})}, capture_output=True, text=True,
                          timeout=120)


def events(tmp_path, kind=None):
    p = tmp_path / "events.jsonl"
    rows = [json.loads(x) for x in p.read_text().splitlines()] if p.exists() else []
    return [r for r in rows if kind is None or r["kind"] == kind]


def recover(tmp_path, now=1000000):
    r = run(tmp_path, "recover", env={"NOW": str(now)})
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout.strip().splitlines()[-1])


def test_t6_crash_after_pause_before_delivery(tmp_path):
    write_worker(tmp_path)
    assert run(tmp_path, "pause_crash").returncode == 9 and events(tmp_path, "send") == []
    rep = recover(tmp_path)
    assert len(rep["sent"]) == 1 and rep["states"] == ["pending"] and len(events(tmp_path, "send")) == 1
    rep2 = recover(tmp_path)                                         # restart again: no duplicate
    assert rep2["sent"] == [] and len(events(tmp_path, "send")) == 1


def test_t7_crash_after_send_before_record_resend_and_ghost_refused(tmp_path):
    write_worker(tmp_path)
    assert run(tmp_path, "send_crash").returncode == 9
    [ghost] = events(tmp_path, "send")
    assert recover(tmp_path)["sent"] == []                           # lease (60 s) still held
    rep = recover(tmp_path, now=1000000 + 61)                        # lease expired -> re-send
    assert len(rep["sent"]) == 1
    sends = events(tmp_path, "send")
    assert len(sends) == 2 and sends[0]["approval_id"] == sends[1]["approval_id"]
    aid = ghost["approval_id"]
    assert run(tmp_path, "click", aid, 0, ghost["message_id"]).returncode == 0
    assert events(tmp_path, "answer")[-1]["text"] == "This button does not belong to this request."
    assert events(tmp_path, "executed") == []
    assert run(tmp_path, "click", aid, 0, sends[1]["message_id"]).returncode == 0
    assert len(events(tmp_path, "executed")) == 1


def test_crash_between_a_resume_and_b_delivery_recover_sends_b(tmp_path):
    write_worker(tmp_path)
    run(tmp_path, "pause_crash", env={"AMOUNT": "5000"})
    recover(tmp_path)
    [a] = events(tmp_path, "send")
    assert run(tmp_path, "crash_after_resume", a["approval_id"], 0, a["message_id"]).returncode == 9
    assert len(events(tmp_path, "send")) == 1                        # B not delivered yet
    rep = recover(tmp_path)
    sends = events(tmp_path, "send")
    assert len(rep["sent"]) == 1 and len(sends) == 2 and sends[1]["text"].startswith("<b>Release 5000")
    b = sends[1]
    assert run(tmp_path, "click", b["approval_id"], 0, b["message_id"]).returncode == 0
    assert [e["finance"] for e in events(tmp_path, "executed")] == ["release"]
    assert [e["result"] for e in events(tmp_path, "completed")] == ["executed"]


def test_t9_crash_in_completion_callback_retried_by_recover(tmp_path):
    write_worker(tmp_path)
    run(tmp_path, "pause_crash")
    recover(tmp_path)
    [a] = events(tmp_path, "send")
    assert run(tmp_path, "crash_in_completion", a["approval_id"], 0, a["message_id"]).returncode == 9
    assert len(events(tmp_path, "completed")) == 1                   # callback ran, not recorded
    rep = recover(tmp_path)
    assert rep["completed"] == ["job-1"] and len(events(tmp_path, "completed")) == 2   # at least once
    assert recover(tmp_path)["completed"] == []                      # recorded now
