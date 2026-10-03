# Security model — langgraph-external-hitl

## Invariants

1. **Authorization identity** is the numeric Telegram `from.id` only. Usernames are display-only.
2. **Send target** is the stored `chat_id` of the recipient's live connection, snapshotted into the
   approval row. With no active connection a request fails closed (`NotConnectedError` /
   `RecipientUnavailableError`); it never falls back to another chat.
3. **Connection tokens**: 256-bit (`c_` + `token_urlsafe(32)`, 45 chars), stored only as SHA-256,
   single-use, expire in 60–3600 s (default 600), bound server-side to one `external_user_id`,
   revoked when a newer link is created. Claiming binds the token to one Telegram account; a
   second Telegram account cannot take it over; confirmation requires the same account and chat.
4. **callback_data is untrusted**: only `v2:<approval_id>:<index>` (and legacy `a:`/`r:`) is
   accepted; the index is resolved against the stored, immutable option list.
5. **A decision is accepted only** through one atomic conditional UPDATE that checks status,
   expiry, Telegram user, chat, message, and that the recipient's connection is still `active`.
6. **Every callback query is answered**, including rejected ones.
7. **Resume** targets only the stored thread/interrupt; the graph accepts the resume value only
   if its payload digest matches and the option id belongs to the requested list.
8. **One-to-one connections**: one live connection per application user and per Telegram
   account (enforced by partial unique indexes).
9. **No exactly-once claim** for side effects; see README.
10. **Thread recipient pinning** (0.4): a tracked thread is bound to one external user; a different
    recipient fails closed. Recipients never come from graph state or interrupt payloads.
11. **Foreign interrupts** (payloads not produced by `request_approval`) are never delivered or resumed.
12. **Delivery is at-least-once**; a re-sent message after a crash supersedes the unrecorded one,
    which can never decide the approval (message binding).

## Threats and mitigations

| Threat | Mitigation |
|---|---|
| Connection link leaked/stolen | Show the link only to the authenticated app user; short TTL; single use; new link revokes old. The app should display "connected as <name> (@username)" with a disconnect action. |
| Link replay / brute force | Atomic claim/confirm; generic error replies; 5 failed `/start` attempts per 10 min block that Telegram user for 1 h; 256-bit tokens. |
| Someone else's Telegram account claiming a link | Whoever presses Start with a valid token is bound — protect the link like a password-reset link; Telegram-side confirmation names the application. |
| Forged / tampered callback data, option tampering | Strict parser; server-side option resolution; graph-side option + digest check. |
| Wrong user, other chat, other message | Rejected by the atomic consume. |
| Stale, duplicate or replayed callbacks | Single-row atomic consume: exactly one winner; others get "Already decided". |
| Disconnected / replaced / blocked recipient | Pending approvals cannot be decided (`disconnected`); new requests fail closed. |
| Leaked chat ids | Low risk: bots cannot message users who never started them. Treated as PII; not logged by default. |
| Leaked bot token | Critical: revoke it in @BotFather and rotate. The token is redacted from logs (`install_redaction`) and never shown in `repr`. |
| Local file access | Databases are created `0600`; warning for group/other-readable files. |
| Message injection | All developer and user text is HTML-escaped; length limits enforced. |

## Data stored

`approvals.db`: approval titles/messages/options, Telegram user and chat ids, external user ids,
username/first name (display only), SHA-256 hashes of connection tokens, timestamps.
Checkpoint data (`checkpoints.db`) contains your graph state; set `LANGGRAPH_STRICT_MSGPACK=true`.

## Reporting

Please report vulnerabilities privately to the maintainer (GitHub: rugvedtech1) instead of
opening a public issue.

## Operational guidance (release checklist)

| Topic | Guidance |
|---|---|
| Bot token | Keep it in the environment or a secret manager; never commit `.env`; if leaked, revoke it in @BotFather (`/revoke`) and redeploy. It is hidden from `repr` and redacted from logs (`install_redaction(token)` after configuring logging). |
| Connection links | A link is a **bearer credential until used**: whoever opens it first is connected. Deliver it only to the intended person over an authenticated channel; it expires (default 10 min) and is single-use. Only its SHA-256 hash is stored. |
| Identity | Authorization uses the numeric Telegram `user_id` of the clicking user. `chat_id` is only the delivery address and is never trusted for authorization; usernames are display data. |
| Replays / duplicates / wrong users | Each approval is decided once (atomic database update bound to user, chat, message, connection and expiry). Duplicate clicks, stale messages and other users are refused. |
| Expired approvals | Refused at click time; the run stays paused until your application decides. |
| Databases | The HITL DB is created `0600` on POSIX (no POSIX meaning on Windows - protect the directory). The LangGraph checkpoint DB is created by LangGraph with the process umask: run with `umask 077` / `os.umask(0o077)`. The HITL DB holds approval text and Telegram ids; the checkpoint DB holds your graph state. |
| Approval text | Stored in the HITL DB and sent to Telegram. Telegram stores bot (cloud) chats on its servers; they are **not end-to-end encrypted**. Never put secrets, card numbers, health or other regulated data in approval text. |
| Logs | The package logs ids, outcomes and a 12-character digest prefix - never tokens or approval text. Do not log approval text yourself at INFO. |
| Checkpoints | Set `LANGGRAPH_STRICT_MSGPACK=true` to restrict checkpoint deserialization. |
| Workers | Run exactly one polling worker per bot token. |
