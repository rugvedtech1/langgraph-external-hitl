"""Public API snapshot: accidental additions/removals fail this test."""
import langgraph_external_hitl as core
from langgraph_external_hitl import bridge, telegram

CORE = {"HITL", "ConfigError", "DuplicateRecipientError", "InvalidRecipientNameError", "Recipient",
        "RecipientRegistry", "UnknownRecipientError", "ApprovalChannel", "ChannelConnection", "ChannelConnections", "ChannelError", "DecisionReport",
        "DecisionRequest", "DeliveryReceipt", "DeliveryResult", "HitlService", "RecoveryReport", "APPROVE_REJECT", "MAX_ACTION_LENGTH", "MAX_OPTIONS", "MIN_OPTIONS", "ActionTooLongError", "Approval",
        "ApprovalOption", "ApprovalStore", "ConnectionLink", "ConnectionManager", "Decision", "HitlError",
        "InvalidOptionsError", "MissingDependencyError", "NotConnectedError", "NotTrackedError", "Outcome",
        "RecipientMismatchError", "RecipientUnavailableError", "RedactingFilter", "SchemaVersionError",
        "StartError", "Status", "TelegramConnection", "__version__", "install_redaction", "redact",
        "register_secret"}
BRIDGE = {"ApprovalDecision", "ApprovalResult", "DecideResult", "DeliveryPlan", "HitlBridge", "ReconcileReport", "RecoveryState",
          "ResumeResult", "UnresumedApproval", "request_approval", "thread_config"}
TELEGRAM = {"BotIdentity", "InvalidBotTokenError", "PollerConflictError", "TelegramNetworkError",
            "TelegramSetupError", "mask_token", "DeliveryResult", "RecoveryReport", "TelegramAdapter", "TelegramApprovalBot", "TelegramConfig", "TelegramError"}


def test_core_all():
    assert set(core.__all__) == CORE
    for name in CORE:
        assert hasattr(core, name)


def test_bridge_all():
    assert set(bridge.__all__) == BRIDGE
    for name in BRIDGE:
        assert hasattr(bridge, name)


def test_telegram_all():
    assert set(telegram.__all__) == TELEGRAM
    for name in TELEGRAM:
        assert hasattr(telegram, name)


def test_py_typed_marker_installed():
    from importlib.resources import files
    assert files("langgraph_external_hitl").joinpath("py.typed").is_file()
