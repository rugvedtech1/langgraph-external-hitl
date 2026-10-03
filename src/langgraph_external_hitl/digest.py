"""Canonical payload digest used to bind an approval to its interrupt payload."""
from __future__ import annotations

import hashlib
import json
from typing import Any


def payload_digest(value: Any) -> str:
    """sha256 of the canonical JSON form (sorted keys, no whitespace)."""
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


__all__ = ["payload_digest"]
