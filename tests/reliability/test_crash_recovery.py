"""Part 5 / T6, T8: real process kills (os._exit) at the dangerous points, then restart + reconcile.

A small worker script (installed package only, no test helpers) runs in a subprocess:
  phase "kill_before_resume": start + decide(won) ... os._exit before resume
  phase "kill_in_execute":    start + decide + resume; the execute node writes its side
                              effect and then the process is killed inside the node
  phase "recover":            new process: find_unresumed + reconcile, print a JSON report
The side-effect counter is a file, so it survives process death.
"""
import json
import subprocess
import sys
import textwrap

WORKER = textwrap.dedent('''
    import asyncio, json, os, sys
    from typing import TypedDict
    from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
    from langgraph.graph import END, START, StateGraph
    from langgraph_external_hitl import ApprovalStore
    from langgraph_external_hitl.bridge import HitlBridge, request_approval

    phase = sys.argv[1]
    EFFECTS = "effects.log"

    class S(TypedDict, total=False):
        action: str
        approved: bool
        approval_id: str
        result: str

    def approval(s):
        d = request_approval(s["action"])
        return {"approved": d.approved, "approval_id": d.approval_id}

    def execute(s):
        with open(EFFECTS, "a") as f:
            f.write(s["approval_id"] + "\\n"); f.flush(); os.fsync(f.fileno())
        if phase == "kill_in_execute":
            os._exit(9)                      # hard crash AFTER the side effect
        return {"result": "executed"}

    def build(saver):
        g = StateGraph(S)
        g.add_node("approval", approval); g.add_node("execute", execute)
        g.add_edge(START, "approval")
        g.add_conditional_edges("approval", lambda s: "execute" if s.get("approved") else END, ["execute", END])
        g.add_edge("execute", END)
        return g.compile(checkpointer=saver)

    async def main():
        async with AsyncSqliteSaver.from_conn_string("cp.db") as saver:
            store = ApprovalStore("a.db")
            b = HitlBridge(build(saver), store)
            if phase in ("kill_before_resume", "kill_in_execute"):
                a = await b.start(approver_user_id=7, chat_id=7, action="pay", now=1000, ttl_s=300)
                store.set_message_id(a.approval_id, 5)
                r = await b.decide(approval_id=a.approval_id, decision="approve", user_id=7,
                                   chat_id=7, message_id=5, now=1001)
                assert r.outcome == "won"
                json.dump({"aid": a.approval_id}, open("state.json", "w"))
                if phase == "kill_before_resume":
                    os._exit(9)              # hard crash between consume and resume
                await b.resume(r.approval, "approve", 1001)
                os._exit(3)                  # must not be reached in kill_in_execute
            else:
                before = [(u.approval.approval_id, u.state) for u in await b.find_unresumed()]
                rep = await b.reconcile(1002)
                print(json.dumps({"before": before,
                                  "resumed": [(a.approval_id, r.status, r.graph_result) for a, r in rep.resumed],
                                  "marked": [a.approval_id for a in rep.marked],
                                  "attention": [(u.approval.approval_id, u.state, list(u.graph_next))
                                                for u in rep.needs_attention]}))
    asyncio.run(main())
''')


def run(tmp, phase):
    return subprocess.run([sys.executable, "worker.py", phase], cwd=tmp, capture_output=True,
                          text=True, timeout=120)


def effects(tmp):
    p = tmp / "effects.log"
    return p.read_text().split() if p.exists() else []


def test_kill_between_consume_and_resume_then_reconcile(tmp_path):
    (tmp_path / "worker.py").write_text(WORKER)
    r = run(tmp_path, "kill_before_resume")
    assert r.returncode == 9, r.stderr
    aid = json.loads((tmp_path / "state.json").read_text())["aid"]
    assert effects(tmp_path) == []                       # action has not run

    rep = json.loads(run(tmp_path, "recover").stdout.strip().splitlines()[-1])
    assert rep["before"] == [[aid, "ready_to_resume"]]
    assert rep["resumed"] == [[aid, "resumed", "executed"]]
    assert effects(tmp_path) == [aid]                     # ran exactly once via reconcile

    rep2 = json.loads(run(tmp_path, "recover").stdout.strip().splitlines()[-1])
    assert rep2 == {"before": [], "resumed": [], "marked": [], "attention": []}
    assert effects(tmp_path) == [aid]


def test_kill_inside_side_effect_node_is_partial_and_never_retried(tmp_path):
    (tmp_path / "worker.py").write_text(WORKER)
    r = run(tmp_path, "kill_in_execute")
    assert r.returncode == 9, r.stderr
    aid = json.loads((tmp_path / "state.json").read_text())["aid"]
    assert effects(tmp_path) == [aid]                     # side effect happened before the crash

    for _ in range(2):                                    # restart twice: still only reported
        rep = json.loads(run(tmp_path, "recover").stdout.strip().splitlines()[-1])
        assert rep["before"] == [[aid, "partial"]]
        assert rep["resumed"] == [] and rep["marked"] == []
        assert rep["attention"] == [[aid, "partial", ["execute"]]]
        assert effects(tmp_path) == [aid]                 # NOT executed again
