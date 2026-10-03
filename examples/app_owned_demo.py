"""Larger example (domain: payouts, chosen only for illustration - the library is generic).

Shows the application owning graph invocation, an HTTP event as the trigger, two sequential
approvals and a completion callback. Replace the payout wording with your own domain.

An HTTP request (application event) creates a job; the app tracks it, invokes its own
LangGraph graph with its own thread_id and calls deliver_pending(). Approvals are sent to
the connected Telegram user automatically - including the SECOND approval after the first
is decided - and on_completed reports the final result. No Enter key, no manual trigger.

    python app_owned_demo.py
    curl -X POST "http://127.0.0.1:8080/jobs?amount=5000"   # two approvals (manager + finance)
    curl -X POST "http://127.0.0.1:8080/jobs?amount=40"     # no approval needed
    curl "http://127.0.0.1:8080/jobs/<job_id>"

Env: TELEGRAM_BOT_TOKEN, DEMO_USER_ID (default demo-user), APPROVAL_DB_PATH, CHECKPOINT_DB_PATH,
     LANGGRAPH_STRICT_MSGPACK=true. Standard library HTTP only (this is not the Part 7 app).
"""
from __future__ import annotations

import asyncio
import json
import logging
import operator
import os
import sys
import time
import uuid
from typing import Annotated, TypedDict
from urllib.parse import parse_qs, urlparse

from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from langgraph.graph import END, START, StateGraph

from langgraph_external_hitl import ApprovalOption, ApprovalStore, ConnectionManager, install_redaction
from langgraph_external_hitl.bridge import HitlBridge, request_approval
from langgraph_external_hitl.telegram import TelegramApprovalBot, TelegramConfig

MANAGER = [ApprovalOption("approve", "Approve", style="success"),
           ApprovalOption("reduce", "Approve 50%"), ApprovalOption("reject", "Reject", style="danger")]
FINANCE = [ApprovalOption("release", "Release funds", style="success"), ApprovalOption("hold", "Hold")]


class Job(TypedDict, total=False):
    amount: int
    manager: str | None
    finance: str | None
    approval_ids: Annotated[list, operator.add]
    result: str


def build_graph(saver):
    def assess(s: Job) -> Job:
        return {}

    def manager(s: Job) -> Job:
        r = request_approval(f"Payout of €{s['amount']}", "Manager approval required.", MANAGER)
        return {"manager": r.option_id, "approval_ids": [r.approval_id]}

    def finance(s: Job) -> Job:
        r = request_approval(f"Release €{s['amount']}", "Finance sign-off for large payouts.", FINANCE)
        return {"finance": r.option_id, "approval_ids": [r.approval_id]}

    def execute(s: Job) -> Job:
        # Real side effect goes here; make it idempotent (e.g. key by the job/approval ids).
        factor = 0.5 if s.get("manager") == "reduce" else 1.0
        print(f"EXECUTED payout €{s['amount'] * factor:.0f}")
        return {"result": f"paid:{s['amount'] * factor:.0f}"}

    def stop(s: Job) -> Job:
        return {"result": "rejected"}

    g = StateGraph(Job)
    for n, f in [("assess", assess), ("manager", manager), ("finance", finance), ("execute", execute),
                 ("stop", stop)]:
        g.add_node(n, f)
    g.add_edge(START, "assess")
    g.add_conditional_edges("assess", lambda s: "execute" if s["amount"] < 100 else "manager",
                            ["execute", "manager"])
    g.add_conditional_edges("manager", lambda s: ("stop" if s.get("manager") not in ("approve", "reduce")
                                                  else "finance" if s["amount"] >= 1000 else "execute"),
                            ["stop", "finance", "execute"])
    g.add_conditional_edges("finance", lambda s: "execute" if s.get("finance") == "release" else "stop",
                            ["execute", "stop"])
    g.add_edge("execute", END)
    g.add_edge("stop", END)
    return g.compile(checkpointer=saver)


async def main() -> None:
    os.umask(0o077)
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
    config = TelegramConfig.from_env()
    install_redaction(config.token)
    user = os.environ.get("DEMO_USER_ID", "demo-user")
    jobs: dict[str, str] = {}   # the APPLICATION's own job state

    def on_completed(thread_id: str, values: dict) -> None:  # at least once -> idempotent
        jobs[thread_id] = values.get("result", "done")
        print(f"COMPLETED job={thread_id} result={jobs[thread_id]}")

    store = ApprovalStore(os.environ.get("APPROVAL_DB_PATH", "approvals.db"))
    async with AsyncSqliteSaver.from_conn_string(os.environ.get("CHECKPOINT_DB_PATH", "checkpoints.db")) as saver:
        graph = build_graph(saver)
        bridge = HitlBridge(graph, store, on_completed=on_completed)
        bot = TelegramApprovalBot(config, bridge, connections=ConnectionManager(store), app_name="HITL payouts")
        if bot.connections.get(user) is None:
            link = bot.connections.create_link(user, int(time.time()), bot_username=await bot.bot_username())
            print(f"Connect {user} once: {link.url}")
        rep = await bot.recover()                       # pending deliveries, missed completions
        print(f"recover: deliveries={len(rep.deliveries)} completions={len(rep.completions_notified)}")

        async def submit_job(amount: int) -> dict:
            job_id = f"job-{uuid.uuid4().hex[:8]}"
            await bot.track(job_id, user)                                   # 1. BEFORE invoking
            jobs[job_id] = "running"
            await graph.ainvoke({"amount": amount}, {"configurable": {"thread_id": job_id}},
                                durability="sync")                          # 2. app-owned run
            d = await bot.deliver_pending(job_id)                           # 3. approvals out
            if d.state == "pending":
                jobs[job_id] = "waiting_for_approval"
            return {"job_id": job_id, "state": d.state, "status": jobs[job_id]}

        async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                line = (await reader.readline()).decode(errors="replace").split()
                while (await reader.readline()) not in (b"\r\n", b"\n", b""):
                    pass
                method, url = (line + ["", ""])[:2]
                path = urlparse(url)
                if method == "POST" and path.path == "/jobs":
                    amount = int(parse_qs(path.query).get("amount", ["0"])[0])
                    body, code = await submit_job(amount), 200
                elif method == "GET" and path.path.startswith("/jobs/"):
                    jid = path.path.rsplit("/", 1)[1]
                    body, code = ({"job_id": jid, "status": jobs[jid]}, 200) if jid in jobs else ({"error": "unknown"}, 404)
                else:
                    body, code = {"error": "not found"}, 404
            except Exception as e:
                body, code = {"error": type(e).__name__}, 400
            data = json.dumps(body).encode()
            writer.write(f"HTTP/1.1 {code} OK\r\nContent-Type: application/json\r\nContent-Length: {len(data)}\r\n"
                         f"Connection: close\r\n\r\n".encode() + data)
            await writer.drain()
            writer.close()

        server = await asyncio.start_server(handle, "127.0.0.1", int(os.environ.get("PORT", "8080")))
        print("listening on http://127.0.0.1:8080  (POST /jobs?amount=5000, GET /jobs/<id>)")
        async with server:
            await bot.run_polling()   # one process: HTTP app + Telegram worker
    store.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
