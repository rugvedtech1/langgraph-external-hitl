"""langgraph-external-hitl: secure external human-in-the-loop approvals for LangGraph.

Core API (standard library only). Optional features live in submodules:

* ``langgraph_external_hitl.bridge``   -> requires the ``[langgraph]`` extra
* ``langgraph_external_hitl.telegram`` -> requires the ``[telegram]`` extra

Community package; not affiliated with LangChain.
"""
from __future__ import annotations

import logging
from importlib.metadata import PackageNotFoundError, version

from ._redact import RedactingFilter, install_redaction, redact, register_secret
from .channels import ApprovalChannel, ChannelError, DecisionReport, DecisionRequest, DeliveryReceipt
from .connections import (ChannelConnection, ChannelConnections, ConnectionLink, ConnectionManager,
                          TelegramConnection)
from .service import DeliveryResult, HitlService, RecoveryReport
from .config import ConfigError
from .facade import HITL
from .recipients import Recipient, RecipientRegistry
from .errors import (DuplicateRecipientError, InvalidRecipientNameError, UnknownRecipientError,
                     ActionTooLongError, HitlError, InvalidOptionsError, MissingDependencyError,
                     NotConnectedError, NotTrackedError, RecipientMismatchError,
                     RecipientUnavailableError, SchemaVersionError, StartError)
from .options import APPROVE_REJECT, MAX_OPTIONS, MIN_OPTIONS, ApprovalOption
from .store import MAX_ACTION_LENGTH, Approval, ApprovalStore
from .store import Outcome, Status
from .types import Decision

try:
    __version__ = version("langgraph-external-hitl")
except PackageNotFoundError:  # pragma: no cover - running from an uninstalled tree
    __version__ = "0.0.0+unknown"

logging.getLogger(__name__).addHandler(logging.NullHandler())

__all__ = [
    "HITL",
    "ConfigError",
    "DuplicateRecipientError",
    "InvalidRecipientNameError",
    "Recipient",
    "RecipientRegistry",
    "UnknownRecipientError",
    "ApprovalChannel",
    "ChannelConnection",
    "ChannelConnections",
    "ChannelError",
    "DecisionReport",
    "DecisionRequest",
    "DeliveryReceipt",
    "DeliveryResult",
    "HitlService",
    "RecoveryReport",
    "APPROVE_REJECT",
    "MAX_ACTION_LENGTH",
    "MAX_OPTIONS",
    "MIN_OPTIONS",
    "ActionTooLongError",
    "ApprovalOption",
    "Approval",
    "ApprovalStore",
    "ConnectionLink",
    "ConnectionManager",
    "Decision",
    "HitlError",
    "InvalidOptionsError",
    "MissingDependencyError",
    "NotConnectedError",
    "NotTrackedError",
    "Outcome",
    "RecipientMismatchError",
    "RecipientUnavailableError",
    "RedactingFilter",
    "SchemaVersionError",
    "StartError",
    "Status",
    "TelegramConnection",
    "__version__",
    "install_redaction",
    "redact",
    "register_secret",
]


def __getattr__(name: str):
    """Lazy convenience import: ``from langgraph_external_hitl import request_approval``
    (needs the [langgraph] extra; not part of ``__all__`` so ``import *`` stays dependency-free)."""
    if name == "request_approval":
        from .bridge import request_approval
        return request_approval
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
