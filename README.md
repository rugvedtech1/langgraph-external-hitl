# langgraph-external-hitl

**Pause a LangGraph workflow, ask a person on Telegram, and resume with their answer.**

Your workflow decides *when* a human must approve something. This library sends the question to
the right person on Telegram with buttons, checks who pressed which button, and resumes the same
LangGraph run with that choice.

> Community package. Not affiliated with or endorsed by LangChain, Inc. or Telegram.

## Quick start (Telegram)

```bash
pip install "langgraph-external-hitl[all]"      # [telegram] for the CLI only; [all] adds LangGraph
langgraph-hitl setup                             # or: python -m langgraph_external_hitl setup
```

The one-time wizard:

1. **Create or use a Telegram bot** - it shows the @BotFather steps if you need a new one.
   The bot is **yours**; this library does not run a chat service of its own.
2. **Paste the bot token** (hidden input; never a command-line argument, never printed).
   It is validated with Telegram `getMe`; the bot username is discovered automatically.
3. **Connect the approver** - name them (e.g. `manager`), open the printed one-time link in
   Telegram, press **Start**, then **Connect**. Connect once; every later approval arrives automatically.
4. Everything is stored in a project-local, git-ignored `.hitl/` directory
   (`config.toml`, `secrets.env` 0600, `hitl.db` 0600). Re-running `setup` is safe
   (Keep / Reconnect approver / Replace bot token / Start over); an interrupted setup resumes.
5. **Build your LangGraph app and call `request_approval()`**:

<!-- app:start -->
```python
"""Smallest app after `langgraph-hitl setup` (which created ./.hitl and connected "manager").

    pip install "langgraph-external-hitl[all]"
    langgraph-hitl setup
    python app.py 1.4.2
"""
import asyncio
import contextlib
import sys
import uuid
from typing import TypedDict

from langgraph.graph import END, START, StateGraph

from langgraph_external_hitl import HITL, ApprovalOption, ConfigError, HitlError, request_approval

APPROVER = "manager"   # the approver name you chose in `langgraph-hitl setup`


class State(TypedDict, total=False):
    version: str
    decision: str


def approve(state: State) -> State:
    result = request_approval(                      # FIRST statement of the node (it pauses here)
        recipient=APPROVER,                         # a registered approver - from your code, never LLM output
        title="Deploy application",
        message=f"Deploy version {state['version']}?",
        options=[
            ApprovalOption("production", "Deploy to Production"),
            ApprovalOption("staging", "Deploy to Staging"),
            ApprovalOption("reject", "Reject"),
        ],
    )
    return {"decision": result.option_id}


def act(state: State) -> State:
    print(f"Decision: {state['decision']}")         # your business action goes here (keep it idempotent)
    return {}


def build(checkpointer):
    g = StateGraph(State)
    g.add_node("approve", approve)
    g.add_node("act", act)
    g.add_edge(START, "approve")
    g.add_edge("approve", "act")
    g.add_edge("act", END)
    return g.compile(checkpointer=checkpointer)


async def main(version: str) -> None:
    async with HITL.from_config() as hitl:          # reads ./.hitl
        graph = build(hitl.checkpointer)
        await hitl.start(graph, {"version": version}, thread_id=f"deploy-{uuid.uuid4().hex[:8]}",
                         recipient=APPROVER)
        print("Approval sent to Telegram. Waiting for decisions (Ctrl+C to stop)...")
        await hitl.run(graph)                       # receives clicks, resumes the graph (one worker)


if __name__ == "__main__":
    try:
        with contextlib.suppress(KeyboardInterrupt):
            asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "1.4.2"))
    except (HitlError, ConfigError) as e:   # setup missing, unknown approver, rejected token, ...
        sys.exit(f"Error: {e}")
```
<!-- app:end -->

`langgraph-hitl doctor` checks the configuration, file permissions, database, bot token and webhook.
Telegram is the only channel implemented today (Web and WhatsApp are planned).

**Recipient names are not authorization.** `recipient="manager"` is only a lookup key into the
registry created by `setup`. Decisions are accepted only from the Telegram user id that connected
as `manager`. Unknown names fail closed (no fallback), and a thread already pinned to one approver
is never redirected to another. Pass recipient names as literals from your code - never from LLM
output or user input.

The lower-level API (`TelegramApprovalBot`, `HitlBridge`, `track` / `deliver_pending`) from 0.5
keeps working unchanged; see the next sections.

## When to use it

| Good fit | Not (yet) a fit |
|---|---|
| An agent/workflow must get a human "yes/no" (or one of up to 10 choices) before acting | Multi-host / horizontally scaled deployments (state is local SQLite) |
| Approvers are on their phones, not in your web UI | Several approvers voting, or different approvers per step of one run |
| A single service (or one app + one worker on the same machine) | Highly confidential approval text (Telegram bot chats are not end-to-end encrypted) |

The library is generic: deployments, refunds, data deletions, emails, purchases - you choose the
question, the options, the approver and what happens afterwards.

## Installation

```bash
pip install "langgraph-external-hitl[all]"     # Telegram + LangGraph support (what you normally want)
```

Python 3.12+. The bare `pip install langgraph-external-hitl` installs only the dependency-free core;
the extras are `telegram` (httpx) and `langgraph` (langgraph + SQLite checkpointer).

## Telegram setup (once)

1. In Telegram, talk to **@BotFather** → `/newbot` → copy the **bot token**.
2. Provide it as an environment variable (never commit it):

| Variable | Required | Meaning |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | yes | Token from @BotFather |
| `TELEGRAM_BOT_USERNAME` | no | Bot username; fetched from Telegram if not set |
| `APPROVAL_TTL_S` | no | Seconds an approval stays valid (default 300) |
| `LANGGRAPH_STRICT_MSGPACK` | recommended | Set to `true` for safer checkpoint deserialization |

## Connect an approver (once)

Each approver connects their Telegram account **once** using a one-time link. After that they
receive **any number of future approvals** - no link, no `/start` per approval.

```python
link = connections.create_link("alice", int(time.time()))   # "alice" = YOUR id for this person
print(link.url)   # https://t.me/<your_bot>?start=c_...  -> open, press Start, then Connect
```

* The link is **temporary** (10 minutes by default), **single-use**, and works only for the person
  who opens it first - treat it like a password-reset link and show it only to that person.
* Creating a new link cancels the previous unused one.
* The polling worker (below) must be running for the "Connect" button to work.

## Minimal LangGraph example

This is `examples/quickstart.py` (a test keeps the two identical):

<!-- quickstart:start -->
```python
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
```
<!-- quickstart:end -->

The three steps your application performs for each piece of work:

1. `bot.track(job_id, approver)` - say who must approve this run (call it **before** running the graph).
2. `graph.ainvoke(..., {"configurable": {"thread_id": job_id}}, durability="sync")` - **your** run, as usual.
3. `bot.deliver_pending(job_id)` - sends the Telegram message for any pending approval.

`bot.run_polling()` receives button clicks; the library then resumes **the same thread**, and
`on_completed(thread_id, values)` tells you the final result.

### The identifiers, simply

| Name | What it is | Who creates it |
|---|---|---|
| `job_id` | Your application's own id for one piece of work | You |
| `thread_id` | The LangGraph workflow id. Use the same one to start and to resume. Using `job_id` as `thread_id` is simplest | You |
| `approval_id` | The library's id for one approval request (handy in logs) | Library |
| `interrupt_id` | LangGraph's internal pause id - you never need it | LangGraph |
| approver id (e.g. `"alice"`) | Your id for the person who approves | You |
| Telegram `user_id` / `chat_id` | Captured when the person connects: `user_id` is checked on every click (authorization); `chat_id` is only where messages are sent | Telegram |

## Custom approval options

```python
from langgraph_external_hitl import ApprovalOption

OPTIONS = [
    ApprovalOption("now", "Deploy now", description="Roll out immediately", style="success"),
    ApprovalOption("tonight", "Deploy tonight"),
    ApprovalOption("cancel", "Cancel", style="danger"),
]
answer = request_approval("Deploy web-app v2.4.1?", "Changes: 14 commits, 2 migrations", OPTIONS)
if answer.option_id == "now": ...
```

1-10 options; `id` (returned to your graph) uses `A-Z a-z 0-9 _ . -`, labels are unique.
`APPROVE_REJECT` is the ready-made Approve/Reject pair; `answer.approved` only works with sets that
contain an option with id `"approve"` (otherwise it raises `ValueError` - use `answer.option_id`).

Rules for the approval node: `request_approval(...)` must be the **first statement** of its node
(LangGraph re-runs a node from the top when it resumes), and the real action belongs in a
**separate node** after it.

## Multiple approval steps

A graph may ask several times (e.g. "team lead" then "on-call"). After the first answer the library
resumes the run and, if it pauses again, **sends the next approval automatically** to the same
approver. Nothing to call in between.

## Recovery and restarts

Call `await bot.recover()` once at startup (before `run_polling()`). It re-sends approvals whose
delivery was interrupted, resumes decisions that were recorded but not yet applied, and re-runs
missed `on_completed` callbacks. The connection, pending approvals and paused runs all survive
restarts (they are stored in SQLite).

Guarantees you should design for:

* **Each approval is decided at most once** - double clicks and replays are refused.
* **Delivery is at least once**: in a rare crash window a message may be sent twice; only one of
  them can decide.
* **`on_completed` is at least once** - make it idempotent.
* **Business actions are yours**: LangGraph may run a node more than once; key your action by your
  job id (or `approval_id`) so a repeat does nothing. A crash *inside* the action is reported by
  `recover()` and never retried automatically.

## Data & privacy

Approval messages travel through **your** Telegram bot. Telegram bot chats are cloud chats and are
**not end-to-end encrypted**: do not put secrets, credentials or regulated personal data in approval
titles/messages - send a reference (e.g. "Deploy #1842") instead.


| Where | What is stored | Owner |
|---|---|---|
| **Your application database** | Customers, orders, your job records, business results | You |
| **HITL database** (`ApprovalStore("hitl.db")`) | Approval title/message/options and status, Telegram user id, chat id, username/first name of connected approvers, delivery/recovery state, *hashes* of connection links (never the raw link) | This library |
| **LangGraph checkpoint database** | Your graph state - including anything **you** put into the state | LangGraph |

The library never reads your business data. It stores and sends only what you pass to
`request_approval()` (title, message, options). **That text is saved in the HITL database and sent
to Telegram, which stores bot chats in its cloud; they are not end-to-end encrypted.**
Put only what the approver needs to decide; prefer ids/references over personal or regulated data
(health data, card numbers, passwords, secrets).

The HITL database file is created with owner-only permissions (`0600` on Linux/macOS). The
checkpoint database is created by LangGraph with your process umask, so set `os.umask(0o077)`
before opening it (as the quickstart does) and protect it like your application database.

## Security

The full model is in `SECURITY.md` (included in the source distribution). In short: only the
connected Telegram user can decide; each decision is single-use and bound to the exact message;
options are validated server-side; the bot token and connection links are redacted from logs.
Production checklist: keep the token in the environment or a secret manager, set
`LANGGRAPH_STRICT_MSGPACK=true`, run one worker per bot token, call `install_redaction(token)` after
configuring logging, protect the database files, and keep approval text minimal.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| "This connection link is invalid or expired" | Link older than its TTL, already used, or replaced by a newer link → create a new one |
| No approval message arrives | Approver not connected (`deliver_pending` returns `not_connected`), bot blocked (`blocked`), or the worker is not running |
| Clicking a button does nothing for a long time | The polling worker is stopped. Telegram keeps undelivered clicks for at most 24 hours |
| `409 Conflict ... other getUpdates request` | Two processes are polling with the same bot token - run exactly one worker |
| "This button does not belong to this request" | An older duplicate message (see delivery guarantees); use the newest message |
| Approval expired, workflow still paused | Expired approvals are refused but the run stays paused (see Limitations) |
| `RecipientMismatchError` | The same `thread_id` was tracked for a different approver |

## Limitations

* Single host: SQLite files and **one polling worker per bot token**; the worker resumes the graph,
  so it needs the same graph code and database files as your app.
* One approver per run (the same person receives every approval step of a run).
* An **expired approval leaves the run paused**; your application decides what to do (e.g. start a
  new run). There is no "resume as expired" API yet.
* A crash inside your side-effect node is reported, never retried automatically (operator decision).
* Telegram keeps undelivered updates for at most 24 hours; clicks made while the worker is down
  longer than that are lost (the approval stays pending).
* Delivery is at least once; exactly-once side effects are your application's responsibility.
* If an approver blocks the bot, the connection becomes `blocked`; unblocking restores it. A
  different Telegram account requires a new connection link (`/disconnect` ends a connection).

## Architecture (channels)

```
Your app ─► HitlService (channel-neutral: approvals, deliveries, single-use decisions, recovery)
                 │ ApprovalChannel interface (send / update)
                 ├─► TelegramAdapter   (today)
                 └─► other adapters    (later: web, WhatsApp, ...)
```

`TelegramApprovalBot` = `HitlService` + `TelegramAdapter` (+ Telegram polling and the connect-once
flow) and keeps working unchanged. Adapter authors implement `send()`/`update()`, turn user
interactions into a `DecisionRequest` and call `HitlService.decide()`/`finish()` (or `handle()`);
the core validates actor, delivery reference, option, expiry and single use. Run
`langgraph_external_hitl.testing.CONTRACT_CHECKS` against your adapter (a `FakeChannel` is included).

## Advanced

* `examples/app_owned_demo.py` - HTTP-triggered jobs, two sequential approvals and a completion
  callback (payout wording, for illustration only).
* `HitlBridge` (in `langgraph_external_hitl.bridge`) exposes the lower-level steps
  (`prepare_delivery`, `decide`, `resume`, `reconcile`) used by `TelegramApprovalBot`.
* `TelegramApprovalBot.handle_update(update)` feeds a raw Telegram update - useful in tests.
* Public API = names in `langgraph_external_hitl.__all__`, `langgraph_external_hitl.bridge.__all__`
  and `langgraph_external_hitl.telegram.__all__`. Everything else is internal and may change.
* `HitlBridge.start()` and `TelegramApprovalBot.request_approval()` are older convenience
  entry points; `start()` is deprecated.

## Status

Alpha (`0.x`). See `CHANGELOG.md` for changes.
