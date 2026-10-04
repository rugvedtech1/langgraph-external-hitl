![Uploading icorn.png…]()



# langgraph-external-hitl

**Pause a LangGraph workflow, ask a person on Telegram, and continue with their answer.**

Your graph decides *when* a human must approve something. This library sends the question to the
approver's Telegram with buttons, accepts the answer only from that person, and resumes the same
LangGraph run with their choice.

> Community package. Not affiliated with or endorsed by LangChain, Inc. or Telegram.
> Telegram is the only approval channel today; you use **your own** Telegram bot.

## Quick start

### 1. Install

```bash
pip install "langgraph-external-hitl[all]"
```

`[all]` adds the Telegram and LangGraph dependencies (Python 3.12+).

### 2. Run setup (once)

```bash
langgraph-hitl setup
```

- Create a Telegram bot with @BotFather (the wizard shows the steps) or use one you have.
- Paste the bot token when asked (hidden input).
- Name the approver (default `manager`), open the printed link in Telegram, press **Start**, then **Connect**.
- Setup stores everything in a local `.hitl/` folder in your project (git-ignored).

The approver connects **once**; every later approval arrives automatically.

### 3. Add approval to your LangGraph app

```python
from langgraph_external_hitl import HITL, ApprovalOption, request_approval

def approve(state):
    result = request_approval(               # must be the FIRST statement of the node
        recipient="manager",                 # the approver you connected in setup
        title="Deploy application",
        message=f"Deploy version {state['version']}?",
        options=[ApprovalOption("approve", "Approve"), ApprovalOption("reject", "Reject")],
    )
    return {"decision": result.option_id}  # "approve" or "reject"

async def main():
    async with HITL.from_config() as hitl:                       # reads ./.hitl from setup
        graph = builder.compile(checkpointer=hitl.checkpointer)  # your StateGraph with the approve node
        await hitl.start(graph, {"version": "1.4.2"}, thread_id="deploy-1", recipient="manager")
        await hitl.run(graph)                                    # receives the click, resumes the graph
```

`request_approval()` must be the first statement of its node, because LangGraph re-runs a node from
the top when it resumes. Put the real action (deploy, send, delete...) in a **separate node** after it.
`hitl.run()` must keep running to receive decisions; run it in exactly one process per bot.

### 4. Run the complete example

This is [`examples/app.py`](https://github.com/rugvedtech1/langgraph-external-hitl/blob/main/examples/app.py). Save it in the folder where you ran setup:

```bash
python app.py 1.4.2
```

Tap an option in Telegram; the terminal prints `Decision: ...`. Press Ctrl+C to stop.

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

## What happens

```
your LangGraph run
      │  request_approval()  -> the run pauses
      ▼
Telegram message with buttons  ->  approver taps one
      │
      ▼
decision recorded once  ->  the same LangGraph run resumes with result.option_id
```

## Custom approval options

```python
from langgraph_external_hitl import ApprovalOption

options = [
    ApprovalOption("now", "Deploy now", description="Roll out immediately", style="success"),
    ApprovalOption("tonight", "Deploy tonight"),
    ApprovalOption("cancel", "Cancel", style="danger"),
]
result = request_approval(recipient="manager", title="Deploy web-app v2.4.1?",
                          message="14 commits, 2 migrations", options=options)
if result.option_id == "now":
    ...
```

1-10 options per approval. `id` is what your graph receives (`A-Z a-z 0-9 _ . -`); labels must be
unique. Without `options`, the buttons are Approve / Reject (`APPROVE_REJECT`).

## When to use it

Good fit:
- An agent or workflow must get a human decision before acting (deploy, refund, delete, send).
- Approvers decide from their phone rather than a web UI.
- One service on one machine (local SQLite files).

Current limitations:
- Telegram is the only channel. Telegram bot chats are **not end-to-end encrypted**.
- One approver per run; no voting or multi-approver rules.
- Single host and **one worker per bot token** (no horizontal scaling).
- An approval that expires (default 5 minutes) leaves the run paused; your app decides what to do.

Full list: [Limitations](#limitations).

## Data & privacy

Approval title, message and options are sent through your Telegram bot and stored in `.hitl/hitl.db`.
Telegram bot chats are cloud chats and are **not end-to-end encrypted**. Do not put secrets,
credentials or regulated personal data in approval text; send a reference instead (e.g. "Deploy #1842").

## Troubleshooting

Run `langgraph-hitl doctor` first: it checks the configuration, file permissions, database, bot token
and webhook.

| Symptom | Fix |
|---|---|
| "Telegram rejected the bot token (401)" | The token is mistyped, revoked or reset. Copy the current one from @BotFather → `/mybots` → API Token, then run `langgraph-hitl setup` → Replace bot token |
| "This connection link is invalid or expired" | Links are single-use and expire after 10 minutes. Run `langgraph-hitl setup` → Reconnect approver |
| Approval message never arrives | Approver not connected or bot blocked (check `doctor`); or `recipient=` names an approver that is not registered |
| Clicking a button does nothing | `hitl.run()` (the worker) is not running. Telegram keeps undelivered clicks for at most 24 hours |
| "Another program is already receiving updates for this bot (409 Conflict)" | Two processes poll the same bot. Run exactly one worker per bot token |
| "This request has expired" | Approvals expire after 5 minutes by default; start a new run (or raise `approval_ttl_s`, see Advanced) |

More cases: [Advanced troubleshooting](#more-troubleshooting).

## Backward compatibility

The 0.5 low-level API (`TelegramApprovalBot`, `HitlBridge`, `ApprovalStore`, `ConnectionManager`,
`track` / `deliver_pending`) still works unchanged. New projects should use `langgraph-hitl setup`
+ `HITL.from_config()` as shown above. See [Lower-level API](#lower-level-api-05-compatible).

## Security

- Only the Telegram account that connected as the approver can decide; each decision is single-use.
- `recipient="manager"` is a **lookup name, not authorization**. Unknown names fail closed (no
  fallback) and a run already assigned to one approver is never redirected. Pass recipient names
  from your code, never from LLM output or user input.
- The bot token is never printed or logged; `.hitl/secrets.env` and `.hitl/hitl.db` are owner-only
  (`0600` on Linux/macOS). Never commit `.hitl/`.

Full model and production checklist: [SECURITY.md](https://github.com/rugvedtech1/langgraph-external-hitl/blob/main/SECURITY.md).

## Status

Alpha (`0.x`): the API may still change between minor versions. Telegram is the only implemented
channel. Changes: [CHANGELOG.md](https://github.com/rugvedtech1/langgraph-external-hitl/blob/main/CHANGELOG.md). Feedback and bug reports:
[issues](https://github.com/rugvedtech1/langgraph-external-hitl/issues).

---

## Advanced

**Most users do not need this section.**

### Configuration reference

`langgraph-hitl setup` creates:

| File | Contents |
|---|---|
| `.hitl/config.toml` | Bot username, approver name, `approval_ttl_s` (default 300), setup state - no secrets |
| `.hitl/secrets.env` | `TELEGRAM_BOT_TOKEN` (`0600`) |
| `.hitl/hitl.db` | Approvals, approver registry, Telegram connections (`0600`) |
| `.hitl/checkpoints.db` | LangGraph checkpoints when you use `hitl.checkpointer` (`0600`) |
| `.hitl/.gitignore` | `*` (nothing in `.hitl/` is ever committed) |

`HITL.from_config()` finds the nearest `.hitl/` walking up from the current directory (or
`$HITL_HOME`). Environment variables override the files: `TELEGRAM_BOT_TOKEN`, `APPROVAL_TTL_S`.
Re-running `langgraph-hitl setup` is safe (Keep / Reconnect approver / Replace bot token / Start
over); an interrupted setup resumes where it stopped. `HITL.from_config(on_completed=callback)`
receives `(thread_id, final_state)` when a run finishes.

**Containers / CI** (`HITL.from_env()`, no setup wizard): `TELEGRAM_BOT_TOKEN` (required),
`HITL_DB_PATH` (default `.hitl/hitl.db`), `HITL_CHECKPOINT_PATH` (optional), `APPROVAL_TTL_S`,
`TELEGRAM_BOT_USERNAME` (optional; discovered via Telegram otherwise). Approvers must already be
registered and connected in that database. Setting `LANGGRAPH_STRICT_MSGPACK=true` is recommended
for safer checkpoint deserialization.

### Identifiers

| Name | What it is | Who creates it |
|---|---|---|
| `thread_id` | The LangGraph run id; the same id is used to start and resume | You |
| `recipient` (e.g. `"manager"`) | Friendly approver name, mapped to a stable internal id | You (in setup) |
| `approval_id` | The library's id for one approval request (handy in logs) | Library |
| `interrupt_id` | LangGraph's internal pause id - you never need it | LangGraph |
| Telegram `user_id` / `chat_id` | Captured when the approver connects: `user_id` is checked on every click (authorization); `chat_id` is only where messages are sent | Telegram |

### Multiple approval steps, recovery and guarantees

A graph may ask several times (e.g. "team lead" then "on-call"). After the first answer the library
resumes the run and, if it pauses again, **sends the next approval automatically** to the same
approver.

`hitl.run()` first recovers interrupted work: it re-sends approvals whose delivery was interrupted,
resumes decisions that were recorded but not yet applied, and re-runs missed completion callbacks.
Connections, pending approvals and paused runs survive restarts (SQLite).

- **Each approval is decided at most once** - double clicks and replays are refused.
- **Delivery is at least once**: in a rare crash window a message may be sent twice; only one of
  them can decide.
- **Completion callbacks are at least once** - make them idempotent.
- **Business actions are yours**: LangGraph may run a node more than once; key your action by your
  run id (or `approval_id`) so a repeat does nothing. A crash *inside* the action is reported during
  recovery and never retried automatically.

### What is stored where

| Where | What is stored | Owner |
|---|---|---|
| Your application database | Customers, orders, job records, business results | You |
| HITL database (`.hitl/hitl.db`) | Approval title/message/options and status, approver names, Telegram user id, chat id, username/first name of connected approvers, delivery/recovery state, *hashes* of connection links (never the raw link) | This library |
| LangGraph checkpoint database | Your graph state - including anything **you** put into the state | LangGraph |

The library never reads your business data; it stores and sends only what you pass to
`request_approval()`. With the lower-level API you open the checkpoint database yourself: set
`os.umask(0o077)` first (as `examples/quickstart.py` does) so it is owner-only.

### Limitations

- Single host: SQLite files and **one polling worker per bot token**; the worker resumes the graph,
  so it needs the same graph code and database files as your app.
- One approver per run (the same person receives every approval step of a run).
- An **expired approval leaves the run paused**; there is no "resume as expired" API yet.
- A crash inside your side-effect node is reported, never retried automatically.
- Telegram keeps undelivered updates for at most 24 hours; clicks made while the worker is down
  longer than that are lost (the approval stays pending).
- Delivery is at least once; exactly-once side effects are your application's responsibility.
- If an approver blocks the bot, the connection becomes `blocked`; unblocking restores it. A different
  Telegram account needs a new connection (`langgraph-hitl setup` → Reconnect approver;
  `/disconnect` in the bot chat ends a connection).
- `hitl.start(..., recipient=None)` assigns the approver from the node's `recipient=` only at first
  delivery; pass `recipient=` to `start()` so a crash before delivery is recoverable.

### More troubleshooting

| Symptom | Cause / fix |
|---|---|
| "This button does not belong to this request" | An older duplicate message (see delivery guarantees); use the newest message |
| `UnknownRecipientError` / "approver ... is not registered" | The `recipient=` name differs from the one chosen in setup; the error lists the registered names |
| `RecipientMismatchError` | The same `thread_id` was assigned to a different approver |
| `ConfigError: no .hitl/ configuration found` | Run your app from the project folder where you ran setup, or set `HITL_HOME` |
| Setup: "This bot has a webhook configured" | Polling cannot work while a webhook is set; confirm deletion in setup or use a dedicated bot |

### Lower-level API (0.5-compatible)

New projects should use `HITL` (above). The lower-level API remains supported for existing code and
for full control over wiring:

1. `bot.track(job_id, approver)` - say who must approve this run (call it **before** running the graph).
2. `graph.ainvoke(..., {"configurable": {"thread_id": job_id}}, durability="sync")` - your run.
3. `bot.deliver_pending(job_id)` - sends the Telegram message for any pending approval.
4. `bot.recover()` at startup, then `bot.run_polling()` to receive clicks; `on_completed(thread_id, values)`
   reports the final result.

Configuration comes from environment variables (`TELEGRAM_BOT_TOKEN`; optional
`TELEGRAM_BOT_USERNAME`, `APPROVAL_TTL_S`). Approvers connect once through
`ConnectionManager.create_link(approver_id, now)`: a single-use link that expires after 10 minutes and
works only for the person who opens it first; creating a new link cancels the previous unused one.

This is [`examples/quickstart.py`](https://github.com/rugvedtech1/langgraph-external-hitl/blob/main/examples/quickstart.py):

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

### Architecture and custom channels

```
Your app ─► HitlService (channel-neutral: approvals, deliveries, single-use decisions, recovery)
                 │ ApprovalChannel interface (send / update)
                 └─► TelegramAdapter (the only adapter today)
```

`TelegramApprovalBot` = `HitlService` + `TelegramAdapter` (+ Telegram polling and the connect-once
flow). Adapter authors implement `send()`/`update()`, turn user interactions into a
`DecisionRequest` and call `HitlService.decide()`/`finish()` (or `handle()`); the core validates
actor, delivery reference, option, expiry and single use. Run
`langgraph_external_hitl.testing.CONTRACT_CHECKS` against your adapter (a `FakeChannel` is included).

### Other APIs and examples

- [`examples/app_owned_demo.py`](https://github.com/rugvedtech1/langgraph-external-hitl/blob/main/examples/app_owned_demo.py) - HTTP-triggered jobs, two
  sequential approvals and a completion callback (payout wording, for illustration only).
- `RecipientRegistry` - create / resolve / rename / remove approver names in code.
- `HitlBridge` (in `langgraph_external_hitl.bridge`) exposes the lower-level steps
  (`prepare_delivery`, `decide`, `resume`, `reconcile`) used by `TelegramApprovalBot`.
- `TelegramApprovalBot.handle_update(update)` feeds a raw Telegram update - useful in tests.
- `result.approved` works only when the options contain an option with id `"approve"` (otherwise it
  raises `ValueError`; use `result.option_id`).
- Public API = names in `langgraph_external_hitl.__all__`, `langgraph_external_hitl.bridge.__all__`
  and `langgraph_external_hitl.telegram.__all__`. Everything else is internal and may change.
- `HitlBridge.start()` and `TelegramApprovalBot.request_approval()` are older convenience entry
  points; `start()` is deprecated.
