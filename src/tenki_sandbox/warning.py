from __future__ import annotations

import logging
import warnings
from dataclasses import dataclass
from typing import Callable

from ._pb import pb


logger = logging.getLogger("tenki_sandbox")


class SandboxWarningCode:
    UNSPECIFIED = "UNSPECIFIED"
    STICKY_OVERRIDES_MAX_DURATION = "STICKY_OVERRIDES_MAX_DURATION"
    STICKY_OVERRIDES_IDLE_TIMEOUT = "STICKY_OVERRIDES_IDLE_TIMEOUT"
    MAX_DURATION_CAPPED = "MAX_DURATION_CAPPED"


@dataclass(frozen=True)
class SandboxWarning:
    code: str
    message: str


class SandboxWarningNotification(UserWarning):
    def __init__(self, warning: SandboxWarning) -> None:
        self.warning = warning
        super().__init__(f"tenki sandbox warning [{warning.code}]: {warning.message}")


WarningHandler = Callable[[SandboxWarning], None]


def default_warning_handler(warning: SandboxWarning, *, stacklevel: int = 3) -> None:
    try:
        warnings.warn(SandboxWarningNotification(warning), stacklevel=stacklevel)
    except SandboxWarningNotification:
        pass


def emit_warnings(
    sandbox_warnings: tuple[SandboxWarning, ...],
    warning_handler: WarningHandler | None,
    *,
    stacklevel: int,
) -> None:
    if warning_handler is None:
        return
    for warning in sandbox_warnings:
        if warning_handler is default_warning_handler:
            default_warning_handler(warning, stacklevel=stacklevel)
        else:
            try:
                warning_handler(warning)
            except Exception:
                try:
                    logger.exception("sandbox warning handler failed")
                except Exception:
                    pass


def sandbox_warnings_from_proto(proto_warnings) -> tuple[SandboxWarning, ...]:
    return tuple(
        SandboxWarning(
            code=_sandbox_warning_code_from_proto(warning.code),
            message=warning.message,
        )
        for warning in proto_warnings
    )


def _sandbox_warning_code_from_proto(value: int) -> str:
    try:
        return pb.SandboxWarningCode.Name(value).removeprefix("SANDBOX_WARNING_CODE_")
    except ValueError:
        return SandboxWarningCode.UNSPECIFIED
