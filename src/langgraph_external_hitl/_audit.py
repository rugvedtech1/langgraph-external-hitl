"""Structured audit events (internal).

Logged to ``langgraph_external_hitl.audit`` as ``event key=value ...``. Only
identifiers, outcomes, timestamps and a short digest prefix are logged - never
tokens, never the full action text or payload.
"""
from __future__ import annotations

import logging
from typing import Any

from ._redact import redact

audit_logger = logging.getLogger("langgraph_external_hitl.audit")

_ALLOWED = frozenset({
    "approval_id", "thread_id", "interrupt_id", "user_id", "approver_user_id", "chat_id",
    "message_id", "outcome", "decision", "status", "state", "digest", "action_len",
    "graph_result", "now", "reason", "update_id", "option_id",
})


def audit(event: str, **fields: Any) -> None:
    parts = [event]
    for key in sorted(fields):
        if key not in _ALLOWED:
            raise ValueError(f"audit field {key!r} is not allowed")
        value = fields[key]
        if key == "digest" and isinstance(value, str):
            value = value[:12]
        parts.append(f"{key}={redact(value)}")
    audit_logger.info(" ".join(parts))
