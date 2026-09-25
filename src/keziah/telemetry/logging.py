"""JSON logs. Authorization material is stripped if it ever reaches a message."""

from __future__ import annotations

import json
import logging
import re
import sys
from typing import Any

_SECRET = re.compile(
    r"(?i)(authorization\s*[:=]\s*bearer\s+)(\S+)|((?:api[_-]?key|token|secret)\s*[:=]\s*)(\S+)"
)
_CONTEXT = ("request_id", "job_id", "batch_id", "client_id", "model", "adapter", "attempt", "worker")


class RedactingFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        redacted = _SECRET.sub(lambda match: (match.group(1) or match.group(3) or "") + "[redacted]", message)
        if redacted != message:
            record.msg = redacted
            record.args = ()
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key in _CONTEXT:
            value = getattr(record, key, None)
            if value is not None:
                payload[key] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(level: str = "info", *, json_logs: bool = True) -> None:
    root = logging.getLogger("keziah")
    root.handlers.clear()
    root.setLevel(level.upper())
    handler = logging.StreamHandler(sys.stderr)
    handler.addFilter(RedactingFilter())
    if json_logs:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root.addHandler(handler)
    root.propagate = False


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(f"keziah.{name}")
