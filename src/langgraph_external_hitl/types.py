"""Shared type aliases (public)."""
from __future__ import annotations

from typing import Literal

Decision = Literal["approve", "reject"]  # the APPROVE_REJECT preset option ids
Status = Literal["pending", "approved", "rejected", "expired"]
Outcome = Literal["won", "unknown", "not_authorized", "wrong_message", "already_decided", "expired"]

__all__ = ["Decision", "Outcome", "Status"]
