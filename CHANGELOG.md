# Changelog

## 0.6.0 - guided setup, friendly recipients, HITL facade (Telegram)

Added (all additive; 0.5 APIs unchanged)
- `langgraph-hitl setup` (also `python -m langgraph_external_hitl setup`): stdlib wizard that validates
  the bot token with getMe (username discovered), checks/deletes webhooks only after confirmation,
  creates `.hitl/` (config.toml, secrets.env 0600, hitl.db 0600, .gitignore), registers a named
  approver and waits for the Telegram /start -> Connect handshake. Resumable (setup_state) and
  idempotent (Keep / Reconnect / Replace token / Start over). `langgraph-hitl doctor`.
- `RecipientRegistry` (create/get/resolve/rename/remove/list): friendly names
  (`^[a-z][a-z0-9_-]{0,31}$`, case-folded) -> stable `recipient_id`. Names are never authorization.
- `request_approval(..., recipient="manager")`: payload v3 binds the name into the digest; unknown
  names fail closed; a pinned thread is never redirected (pinned-or-literal policy).
- `HITL` facade: `from_config()`, `from_env()`, `start()`, `run()`, `track()`, `deliver_pending()`,
  `checkpointer` (async context manager) - wraps the existing store/bridge/Telegram bot.
- Telegram onboarding helpers: `get_me`, `webhook_url`, `delete_webhook`, friendly
  `InvalidBotTokenError` / `PollerConflictError` / `TelegramNetworkError`, `mask_token`.

Usability (from the first live run)
- Unknown approver errors list the registered names and how to fix it; `examples/app.py` uses one
  `APPROVER` constant and exits with a one-line error instead of a traceback.
- `setup` notes when the chosen approver name differs from the "manager" used in the docs/examples.

Changed
- Schema v5 (automatic, transactional): `recipients` table, `approvals.recipient_name`.
- Clearer log message when another process polls the same bot (409).

## 0.5.0 - Part 8.5: channel-neutral core + Telegram adapter (first planned public release)

Architecture
- New channel-neutral `HitlService` (track / deliver_pending / recover / request_approval /
  decide / finish / handle). `TelegramApprovalBot` is now a compatibility facade over
  `HitlService` + `TelegramAdapter`; its 0.4 methods are unchanged.
- New adapter contract (`langgraph_external_hitl.channels`): `ApprovalChannel` (send / update),
  `DeliveryReceipt`, `DecisionRequest`, `DecisionReport`, `ChannelError` (transient / permanent).
- `TelegramAdapter` owns all Telegram specifics (Bot API, rendering, keyboard, callbacks,
  answers, edits, error mapping: 403 -> blocked, 401/404 -> unauthorized).
- Generic `ChannelConnections` (recipient -> channel, actor_ref, address); `ConnectionManager`
  keeps the 0.4 Telegram-shaped API on top of it.
- `langgraph_external_hitl.testing`: `FakeChannel` + 12-check channel contract suite.

Schema v4 (automatic, transactional migration from v3; rollback on failure)
- `approvals` no longer contains Telegram identifiers; new `approval_deliveries`
  (approval, channel, connection, actor_ref, address, external_ref, delivery state) and
  `channel_connections` (replaces `telegram_connections`); `connection_tokens` gains `channel`.
- `Approval.approver_user_id / chat_id / message_id / recipient_external_user_id` remain as
  read-only views of the primary delivery (compatibility).

Fixes found while extracting
- An approval delivered on two channels is now always sent/updated with the delivery of the
  adapter's own channel.

## 0.4.0 — release preparation (Part 8, not yet published)

Changed (public API frozen for the first public release)
- Removed from the package root: `payload_digest`, `validate_action`, `validate_options`,
  `validate_request`, `ConsumeResult`, `ThreadRecord`, `SCHEMA_VERSION` (still available in their
  internal modules; not covered by compatibility promises).
- Removed from `langgraph_external_hitl.telegram`: `keyboard`, `render`, `parse_callback_data`,
  `parse_option_callback`, `TOASTS`, `ALLOWED_UPDATES` (internal).
- Removed `build_payload`, `is_approval_payload` from `langgraph_external_hitl.bridge.__all__`.
- `HitlBridge.start()` is deprecated (emits `DeprecationWarning`); use `track()` + your own graph
  run + `deliver_pending()`.

Fixed
- `ApprovalStore()` no longer leaks its SQLite connection when opening fails (newer schema,
  failed migration) - visible as `ResourceWarning: unclosed database` on Python 3.13+.

Docs / examples
- README rewritten for new users (connect once, identifiers, data & privacy, troubleshooting,
  limitations); SECURITY.md operational guidance (Telegram cloud chats are not end-to-end encrypted).
- New `examples/quickstart.py` (generic deployment approval), kept identical to the README by a test.
- Removed the Enter-key `telegram_demo.py` / `demo_graph.py` examples.
- Python 3.13 classifier.


## 0.4.0 (unreleased) — Part 6.1: application-owned invocation

Features
- `TelegramApprovalBot.track(thread_id, recipient)`, `deliver_pending(thread_id, recipient=None, ttl_s=None)`
  -> `DeliveryResult`, `recover(now=None)` -> `RecoveryReport`; `HitlBridge.track`,
  `HitlBridge.prepare_delivery`, `HitlBridge.on_completed`, `HitlBridge.on_resumed`.
- Subsequent approvals in the same thread are delivered automatically after a resume.
- Completion callback (at least once, persisted, retried by `recover()`).

Schema v3 (automatic, transactional migration; fails closed on duplicate (thread_id, interrupt_id))
- `approvals`: `delivery_state`, `delivery_claimed_at`, `delivered_at`, `delivery_attempts`,
  partial UNIQUE(thread_id, interrupt_id); new `hitl_threads` table.

Fixes
- `bot.request_approval()` returns `None` (instead of raising `StartError`) when the run
  finishes without needing approval; optional `thread_id=`.

Notes
- Delivery is at-least-once (Telegram has no idempotency key); decisions remain single-use.

## 0.3.0 (unreleased)

Features
- Developer-defined approval options: `ApprovalOption(id, label, description, style)`, 1–10 per
  request, `request_approval(title, message, options)` returning `ApprovalResult(option_id, ...)`.
- Rich HTML approval messages (title, message, option descriptions) with one button per option.
- One-time Telegram account connection with single-use deep-link tokens (`ConnectionManager`:
  `create_link`, `get`, `disconnect`, `seed`), Telegram-side confirmation, persistent
  `external_user_id -> (telegram_user_id, chat_id)` mapping; `TelegramApprovalBot.request_approval(recipient, ...)`
  sends proactively (no `/start` per approval).
- Multi-user support with strict 1:1 connections and cross-user isolation.
- Reconnect, `/disconnect`, block/unblock (`my_chat_member`, HTTP 403) handling; hooks
  `on_connected`, `on_disconnected`, `on_blocked`.

Breaking (0.x)
- Database schema v2 (automatic, transactional migration from v1). `Approval.status` is now
  `pending|decided|expired|cancelled|undeliverable` plus `selected_option_id`.
- Callback data is `v2:<approval_id>:<index>`; legacy `a:`/`r:` still accepted for preset options.
- Toasts show option labels (e.g. "Recorded: Approve.").

Security
- Token hashing, single use, TTL, revocation on new link, claim/confirm binding, failed-`/start`
  rate limiting, `connection_id` liveness check inside the atomic consume, HTML escaping.

## 0.2.0 (unreleased)

Reliability
- Fixed: a Telegram network/HTTP error (or non-JSON reply) on `answerCallbackQuery`
  after a winning click left the approval stuck as approved-but-never-resumed.
  Telegram UI calls are now best-effort and can never prevent the LangGraph resume.
- SQLite lock errors (`database is locked`) in handlers answer "Temporarily unavailable"
  and change nothing.
- New `HitlBridge.find_unresumed()` / `classify()` / `reconcile()`: recovers only
  `ready_to_resume` and `completed_unmarked`; `partial` threads are reported and never
  resumed or continued automatically.
- Resume (and start) use `durability="sync"` so a hard crash inside the action node is
  classified as `partial` (with the default async durability it could look like a
  still-pending interrupt and be re-run).
- In-process resume lock and a liveness re-check before every resume.
- Expired callback answers (HTTP 400) are treated as final and not retried.

Security
- Token redaction (`redact`, `register_secret`, `RedactingFilter`, `install_redaction`);
  `httpx`/`httpcore` records are masked automatically.
- New approval databases are created `0600`; warning for group/other-accessible files.
- In-graph payload binding: the resume value carries the approved payload digest and
  `request_approval()` rejects a missing or mismatched digest.
- Action length validation (`MAX_ACTION_LENGTH`, `ActionTooLongError`) before any graph run.
- Audit events on `langgraph_external_hitl.audit`.

Tooling
- Coverage gate (90%), Hypothesis property tests, crash-injection tests, GitHub Actions matrix.

## 0.1.0

- Package the verified Part 1–3 proof of concept as `langgraph-external-hitl`.
