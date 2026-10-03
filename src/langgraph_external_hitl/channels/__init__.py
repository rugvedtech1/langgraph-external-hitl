"""Channel adapter interface (core, channel-neutral, stdlib only)."""
from .base import ApprovalChannel, ChannelError, DecisionReport, DecisionRequest, DeliveryReceipt

__all__ = ["ApprovalChannel", "ChannelError", "DecisionReport", "DecisionRequest", "DeliveryReceipt"]
