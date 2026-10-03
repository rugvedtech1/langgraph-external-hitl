"""Developer-defined approval options (core, stdlib only)."""
from __future__ import annotations

import json
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from .errors import ActionTooLongError, InvalidOptionsError

MIN_OPTIONS = 1
MAX_OPTIONS = 10
MAX_OPTION_LABEL = 64
MAX_OPTION_DESCRIPTION = 200
MAX_TITLE_LENGTH = 256
# Telegram text messages are limited to 4096 characters; keep headroom for
# the rendered header, ref, expiry and status lines.
MAX_MESSAGE_TOTAL = 3500
OPTION_ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")

Style = Literal["primary", "success", "danger"]
_STYLES = ("primary", "success", "danger")


@dataclass(frozen=True, slots=True)
class ApprovalOption:
    """One choice offered to the human. ``id`` is returned to the graph; ``label`` is shown."""
    id: str
    label: str
    description: str | None = None
    style: Style | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.id, "label": self.label, "description": self.description,
                "style": self.style}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ApprovalOption":
        return cls(id=d["id"], label=d["label"], description=d.get("description"),
                   style=d.get("style"))


APPROVE_REJECT: tuple[ApprovalOption, ...] = (
    ApprovalOption("approve", "Approve", style="success"),
    ApprovalOption("reject", "Reject", style="danger"),
)


def validate_options(options: Sequence[ApprovalOption]) -> tuple[ApprovalOption, ...]:
    """Validate and freeze an option list. Raises InvalidOptionsError."""
    opts = tuple(options)
    if not MIN_OPTIONS <= len(opts) <= MAX_OPTIONS:
        raise InvalidOptionsError(f"between {MIN_OPTIONS} and {MAX_OPTIONS} options are required, got {len(opts)}")
    ids: set[str] = set()
    labels: set[str] = set()
    for o in opts:
        if not isinstance(o, ApprovalOption):
            raise InvalidOptionsError(f"options must be ApprovalOption instances, got {type(o).__name__}")
        if not isinstance(o.id, str) or not OPTION_ID_RE.match(o.id):
            raise InvalidOptionsError(f"invalid option id {o.id!r}: use 1-64 of A-Z a-z 0-9 _ . -")
        if not isinstance(o.label, str) or not o.label.strip() or "\n" in o.label \
                or len(o.label.strip()) > MAX_OPTION_LABEL:
            raise InvalidOptionsError(f"option {o.id!r}: label must be 1-{MAX_OPTION_LABEL} chars, single line")
        if o.description is not None and (not isinstance(o.description, str)
                                          or len(o.description) > MAX_OPTION_DESCRIPTION):
            raise InvalidOptionsError(f"option {o.id!r}: description must be <= {MAX_OPTION_DESCRIPTION} chars")
        if o.style is not None and o.style not in _STYLES:
            raise InvalidOptionsError(f"option {o.id!r}: style must be one of {_STYLES}")
        if o.id in ids:
            raise InvalidOptionsError(f"duplicate option id {o.id!r}")
        if o.label.strip().casefold() in labels:
            raise InvalidOptionsError(f"duplicate option label {o.label!r}")
        ids.add(o.id)
        labels.add(o.label.strip().casefold())
    return opts


def validate_request(title: str, message: str | None, options: Sequence[ApprovalOption]) -> tuple[ApprovalOption, ...]:
    """Validate a full approval request (title, message, options, total size)."""
    if not isinstance(title, str) or not title.strip():
        raise ActionTooLongError("title/action must be a non-empty string")
    if message is not None and not isinstance(message, str):
        raise ActionTooLongError("message must be a string")
    opts = validate_options(options)
    # Labels are rendered on buttons; only described options repeat label + description in the body.
    total = len(title) + len(message or "") + sum(len(o.label) + len(o.description)
                                                  for o in opts if o.description)
    if total > MAX_MESSAGE_TOTAL:
        raise ActionTooLongError(f"approval text is {total} characters; maximum is {MAX_MESSAGE_TOTAL}")
    return opts


def options_to_json(options: Iterable[ApprovalOption]) -> str:
    return json.dumps([o.to_dict() for o in options], sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False)


def options_from_json(raw: str) -> tuple[ApprovalOption, ...]:
    return tuple(ApprovalOption.from_dict(d) for d in json.loads(raw))


PRESET_JSON = options_to_json(APPROVE_REJECT)

__all__ = ["APPROVE_REJECT", "MAX_MESSAGE_TOTAL", "MAX_OPTIONS", "MIN_OPTIONS", "ApprovalOption",
           "options_from_json", "options_to_json", "validate_options", "validate_request"]
