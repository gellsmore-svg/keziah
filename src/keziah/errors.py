"""Public errors. API and CLI map these to status codes and exit codes."""

from __future__ import annotations


class KeziahError(Exception):
    """Base class for expected Keziah failures."""

    code = "error"

    def __init__(self, message: str, *, code: str | None = None, details: dict | None = None) -> None:
        super().__init__(message)
        if code is not None:
            self.code = code
        self.details = details or {}


class ValidationError(KeziahError):
    code = "invalid_request"


class BackpressureError(KeziahError):
    code = "backpressure"


class NotFoundError(KeziahError):
    code = "not_found"


class ConflictError(KeziahError):
    code = "idempotency_conflict"


class ModelUnknownError(KeziahError):
    code = "unknown_model"


class ModelUnavailableError(KeziahError):
    code = "model_unavailable"


class ConfigError(KeziahError):
    code = "config"


class CancelledError(KeziahError):
    code = "cancelled"


class Timeout(KeziahError):
    """Wait exceeded the caller's timeout. Distinct from an execution timeout."""

    code = "timeout"


class InferenceError(KeziahError):
    """Adapter failure. ``retryable`` selects retry versus a terminal failure."""

    def __init__(
        self,
        message: str,
        *,
        retryable: bool,
        code: str = "inference",
        retry_after_s: float | None = None,
        details: dict | None = None,
    ) -> None:
        super().__init__(message, code=code, details=details)
        self.retryable = retryable
        self.retry_after_s = retry_after_s


class RetryableInferenceError(InferenceError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "transient",
        retry_after_s: float | None = None,
        details: dict | None = None,
    ) -> None:
        super().__init__(
            message,
            retryable=True,
            code=code,
            retry_after_s=retry_after_s,
            details=details,
        )


class PermanentInferenceError(InferenceError):
    def __init__(self, message: str, *, code: str = "permanent", details: dict | None = None) -> None:
        super().__init__(message, retryable=False, code=code, details=details)
