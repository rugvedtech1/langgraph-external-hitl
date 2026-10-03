"""Quickstart: pause a LangGraph workflow, ask a person on Telegram, resume with the answer.

    pip install "langgraph-external-hitl[all]"
    export TELEGRAM_BOT_TOKEN=123456:ABC...     # from @BotFather
    python quickstart.py "Deploy web-app v2.4.1 to production"

First run: open the printed link in Telegram, press Start, then Connect. The approval
is sent as soon as you are connected. Later runs send it immediately (connect once).
"""
import asyncio
import contextlib
import os
import sys
import time
import uuid
from typing import TypedDict

from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from langgraph.graph import END, START, StateGraph

from langgraph_external_hitl import APPROVE_REJECT, ApprovalStore, ConnectionManager
from langgraph_external_hitl.bridge import HitlBridge, request_approval
from langgraph_external_hitl.telegram import TelegramApprovalBot, TelegramConfig

APPROVER = os.environ.get("APPROVER_ID", "me")  # your application's id for the approver


class State(TypedDict, total=False):
    task: str
    decision: str
    result: str


def ask_human(state: State) -> State:
    # Must be the first statement of the node: the workflow pauses here until someone answers.
    answer = request_approval("Approve production deployment?", state["task"], APPROVE_REJECT)
    return {"decision": answer.option_id}


def act(state: State) -> State:
    # Your real action goes here (keep it idempotent). The library never runs business logic.
    verb = "done" if state["decision"] == "approve" else "cancelled"
    return {"result": f"{verb}: {state['task']}"}


def build_graph(checkpointer):
    g = StateGraph(State)
    g.add_node("ask_human", ask_human)
    g.add_node("act", act)
    g.add_edge(START, "ask_human")
    g.add_edge("ask_human", "act")
    g.add_edge("act", END)
    return g.compile(checkpointer=checkpointer)


async def main(task: str, job_id: str) -> None:
    os.umask(0o077)  # database files created below (incl. LangGraph's) are private to you
    finished = asyncio.Event()

    def on_completed(thread_id, values):  # may run more than once after a crash: keep idempotent
        print(f"RESULT {thread_id}: {values['result']}")
        finished.set()

    store = ApprovalStore("hitl.db")
    async with AsyncSqliteSaver.from_conn_string("checkpoints.db") as checkpointer:
        graph = build_graph(checkpointer)
        bridge = HitlBridge(graph, store, on_completed=on_completed)
        bot = TelegramApprovalBot(TelegramConfig.from_env(), bridge, connections=ConnectionManager(store))

        async def start_job():  # safe to call again for the same job_id
            await bot.track(job_id, APPROVER)  # 1. register who approves, BEFORE running
            config = {"configurable": {"thread_id": job_id}}
            if not (await graph.aget_state(config)).values:
                await graph.ainvoke({"task": task}, config, durability="sync")  # 2. your own run
            await bot.deliver_pending(job_id)  # 3. sends the Telegram message

        bot.on_connected = lambda connection: start_job()
        await bot.recover()  # resend/resume anything interrupted by a previous crash
        if bot.connections.get(APPROVER) is None:
            link = bot.connections.create_link(APPROVER, int(time.time()), bot_username=await bot.bot_username())
            print(f"Connect once: open {link.url} in Telegram, press Start, then Connect.")
        else:
            await start_job()
        polling = asyncio.create_task(bot.run_polling())  # receives the button clicks
        await finished.wait()
        await asyncio.sleep(3)  # let the worker finish updating the Telegram message
        polling.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await polling
    store.close()


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):  # Ctrl+C: stop quietly; run again to continue
        asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "Deploy web-app v2.4.1 to production",
                         os.environ.get("JOB_ID") or f"job-{uuid.uuid4().hex[:8]}"))
