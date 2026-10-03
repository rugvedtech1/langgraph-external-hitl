"""Secret redaction for log output (internal; public helpers re-exported by the package).

Two layers:
* known secrets registered at runtime (e.g. the bot token) are replaced verbatim;
* anything shaped like a Telegram bot URL segment ``bot<digits>:<secret>`` is masked.
"""
from __future__ import annotations

import logging
import re
import threading

_PATTERN = re.compile(r"bot\d{3,}:[A-Za-z0-9_-]{8,}")
_BARE_TOKEN = re.compile(r"\b\d{6,}:[A-Za-z0-9_-]{30,}\b")
_secrets: set[str] = set()
_lock = threading.Lock()
MASK = "<redacted>"


def register_secret(secret: str) -> None:
    """Remember a secret so it is removed from every redacted string."""
    if secret and len(secret) >= 8:
        with _lock:
            _secrets.add(secret)


def redact(text: object) -> str:
    s = str(text)
    with _lock:
        known = sorted(_secrets, key=len, reverse=True)
    for secret in known:
        s = s.replace(secret, MASK)
    s = _PATTERN.sub("bot" + MASK, s)
    return _BARE_TOKEN.sub(MASK, s)


class RedactingFilter(logging.Filter):
    """Logging filter that redacts the fully formatted message (and exception text)."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = redact(record.getMessage())
        record.args = ()
        if record.exc_info:
            record.exc_text = redact(logging.Formatter().formatException(record.exc_info))
            record.exc_info = None
        if record.stack_info:
            record.stack_info = redact(record.stack_info)
        return True


def protect_logger(name: str) -> None:
    """Attach a RedactingFilter to one logger (applies to records CREATED by that logger).

    Used automatically for ``httpx``/``httpcore``, whose INFO request lines contain the
    bot-token URL, so the token is masked even if the application logs at INFO.
    """
    lg = logging.getLogger(name)
    if not any(isinstance(f, RedactingFilter) for f in lg.filters):
        lg.addFilter(RedactingFilter())


def install_redaction(*secrets: str, quiet_http_loggers: bool = True) -> RedactingFilter:
    """Register secrets and attach a RedactingFilter to every handler of the root logger.

    Filters on handlers apply to records from ALL loggers (including httpx), unlike
    logger-level filters. Also raises the httpx/httpcore loggers to WARNING, because
    at INFO they log request URLs, which contain the bot token.
    """
    for s in secrets:
        register_secret(s)
    flt = RedactingFilter()
    root = logging.getLogger()
    for handler in root.handlers:
        if not any(isinstance(f, RedactingFilter) for f in handler.filters):
            handler.addFilter(flt)
    if quiet_http_loggers:
        for name in ("httpx", "httpcore"):
            logging.getLogger(name).setLevel(logging.WARNING)
    return flt
