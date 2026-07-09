from __future__ import annotations

import builtins
import re
from dataclasses import dataclass
from typing import Any


class SandboxError(Exception):
    retryable = False


class MissingAuthTokenError(SandboxError):
    def __init__(self) -> None:
        super().__init__("missing auth token: set TENKI_AUTH_TOKEN or TENKI_API_KEY")


class UnauthorizedError(SandboxError):
    pass


class PermissionDeniedError(SandboxError):
    pass


class SessionNotFoundError(SandboxError):
    pass


class FileNotFoundError(SandboxError, builtins.FileNotFoundError):  # noqa: A001 - mirrors the TS/Go SDK error name
    """Raised when a filesystem path does not exist in the sandbox.

    Shadows the builtin name when imported directly; it also subclasses the
    builtin ``FileNotFoundError`` (and therefore ``OSError``), so handlers
    written for the builtin still catch it.
    """


class SessionTerminatedError(SandboxError):
    pass


class InvalidStateError(SandboxError):
    pass


class CommandTimeoutError(SandboxError):
    pass


class QuotaExceededError(SandboxError):
    pass


class PortLimitExceededError(SandboxError):
    pass


class InboundDisabledError(SandboxError):
    pass


class RateLimitedError(SandboxError):
    retryable = True


class VolumeNotFoundError(SandboxError):
    pass


class VolumeInUseError(SandboxError):
    pass


class VolumeSyncPendingError(SandboxError):
    pass


class SnapshotNotFoundError(SandboxError):
    pass


class SnapshotNotDurableError(SandboxError):
    pass


class TemplateNotFoundError(SandboxError):
    pass


class RegistryImageNotFoundError(SandboxError):
    pass


class TemplateExistsError(SandboxError):
    pass


class TemplateBuildFailedError(SandboxError):
    pass


class TemplateBuildInProgressError(SandboxError):
    pass


class StreamClosedError(SandboxError):
    pass


class InvalidResourceConfigError(SandboxError, ValueError):
    pass


class SSHUnavailableError(SandboxError):
    pass


class GitOperationFailedError(SandboxError):
    """Classifies an in-guest git failure surfaced by node-agent as
    FailedPrecondition.
    """

    def __init__(
        self,
        message: str,
        *,
        stderr: str = "",
        exit_code: int | None = None,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.stderr = stderr
        self.exit_code = exit_code
        self.retryable = retryable


@dataclass
class CommandFailedError(SandboxError):
    result: Any

    def __post_init__(self) -> None:
        super().__init__(
            f"command failed: exit_code={self.result.exit_code} "
            f"argv={' '.join(self.result.argv)!r}"
        )


class CapabilityUnavailableError(SandboxError):
    pass


class PrimitiveTimeoutError(CapabilityUnavailableError):
    pass


# Git failure classification — see the matching block in errors.ts / errors.go.
_GIT_FAILURE = re.compile(r"git .*failed \(exit_code=", re.IGNORECASE)
_GIT_EXIT_CODE = re.compile(r"\(exit_code=(\d+)\)")
_GIT_STDERR = re.compile(r"; check the repository, ref and access token: (.+)", re.DOTALL)
_TRANSIENT_GIT = re.compile(
    r"Recv failure|Connection reset by peer|Could not resolve host|Failed to connect"
    r"|Connection timed out|TLS|gnutls_handshake|SSL_ERROR|RPC failed|HTTP 5\d\d"
    r"|\b(429|503)\b|early EOF|remote end hung up unexpectedly|unexpected disconnect",
    re.IGNORECASE,
)
_TERMINAL_GIT = re.compile(
    r"Authentication failed|could not read Username|terminal prompts disabled"
    r"|Repository not found|fatal: repository .* not found|Permission denied|\b403\b"
    r"|invalid credentials|pathspec|did not match any",
    re.IGNORECASE,
)


def map_rpc_error(err: Exception) -> SandboxError:
    if isinstance(err, SandboxError):
        return err

    code_name = ""
    details = str(err)
    code_fn = getattr(err, "code", None)
    details_fn = getattr(err, "details", None)
    if callable(code_fn):
        code = code_fn()
        code_name = getattr(code, "name", str(code))
    if callable(details_fn):
        details = details_fn() or details

    msg = details.lower()
    if code_name == "UNAUTHENTICATED":
        return UnauthorizedError(details)
    if code_name == "PERMISSION_DENIED":
        return PermissionDeniedError(details)
    if code_name == "NOT_FOUND":
        if "template" in msg:
            return TemplateNotFoundError(details)
        if "snapshot" in msg:
            return SnapshotNotFoundError(details)
        if "volume" in msg:
            return VolumeNotFoundError(details)
        if "registry artifact" in msg:
            return RegistryImageNotFoundError(details)
        if "no such file or directory" in msg:
            return FileNotFoundError(details)
        return SessionNotFoundError(details)
    if code_name == "ALREADY_EXISTS":
        if "template" in msg:
            return TemplateExistsError(details)
        return SandboxError(details)
    if code_name == "DEADLINE_EXCEEDED":
        return CommandTimeoutError(details)
    if code_name == "RESOURCE_EXHAUSTED":
        if "maximum exposed ports" in msg:
            return PortLimitExceededError(details)
        if "rate limit" in msg:
            return RateLimitedError(details)
        return QuotaExceededError(details)
    if code_name == "FAILED_PRECONDITION":
        if "terminated" in msg:
            return SessionTerminatedError(details)
        if _GIT_FAILURE.search(details):
            retryable = bool(_TRANSIENT_GIT.search(details)) and not bool(_TERMINAL_GIT.search(details))
            exit_match = _GIT_EXIT_CODE.search(details)
            exit_code = int(exit_match.group(1)) if exit_match else None
            stderr_match = _GIT_STDERR.search(details)
            stderr = stderr_match.group(1) if stderr_match else ""
            return GitOperationFailedError(details, stderr=stderr, exit_code=exit_code, retryable=retryable)
        if "sync_pending" in msg or "sync pending" in msg:
            return VolumeSyncPendingError(details)
        if "volume is attached" in msg or "volume is in use" in msg:
            return VolumeInUseError(details)
        if "inbound access is disabled" in msg:
            return InboundDisabledError(details)
        if "template build already in progress" in msg or "build in progress" in msg:
            return TemplateBuildInProgressError(details)
        return InvalidStateError(details)
    if code_name == "UNAVAILABLE":
        mapped = SandboxError(details)
        mapped.retryable = True
        return mapped
    return SandboxError(details)
