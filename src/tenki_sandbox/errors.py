from __future__ import annotations

import builtins
import json
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
    def __init__(self, message: str, build=None) -> None:
        super().__init__(message)
        self.build = build


class TemplateBuildInProgressError(SandboxError):
    pass


class TemplateBuildWaitCancelledError(SandboxError):
    """Local build observation was cancelled; the remote build keeps running."""

    def __init__(self, message: str, build=None) -> None:
        super().__init__(message)
        self.build = build


class TemplateSpecValidationError(SandboxError, ValueError):
    """Aggregate local template spec validation failure.

    ``violations`` holds every field-addressable message found; server
    validation remains authoritative.
    """

    def __init__(self, violations) -> None:
        self.violations = tuple(violations)
        message = "invalid template spec"
        if self.violations:
            message += ": " + "; ".join(self.violations)
        super().__init__(message)


class TemplateRuntimeFailedError(SandboxError):
    def __init__(self, message: str, sandbox=None) -> None:
        super().__init__(message)
        self.sandbox = sandbox
        self.reason = message


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
_GIT_STDERR = re.compile(
    r"; check the repository, ref and access token: (.+)", re.DOTALL
)
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


def _grpc_status_message_class():
    from google.protobuf import any_pb2, descriptor_pb2, descriptor_pool, message_factory

    _ = any_pb2.Any
    pool = descriptor_pool.Default()
    name = "tenki.sdk.internal.Status"
    try:
        descriptor = pool.FindMessageTypeByName(name)
    except KeyError:
        file_proto = descriptor_pb2.FileDescriptorProto(
            name="tenki/sdk/internal/status.proto",
            package="tenki.sdk.internal",
            dependency=["google/protobuf/any.proto"],
        )
        status = file_proto.message_type.add(name="Status")
        status.field.add(name="code", number=1, label=1, type=5)
        status.field.add(name="message", number=2, label=1, type=9)
        status.field.add(
            name="details",
            number=3,
            label=3,
            type=11,
            type_name=".google.protobuf.Any",
        )
        descriptor = pool.Add(file_proto).message_types_by_name["Status"]
    return message_factory.GetMessageClass(descriptor)


def _grpc_status_details(err: Exception):
    trailing_metadata = getattr(err, "trailing_metadata", None)
    if not callable(trailing_metadata):
        return ()
    try:
        metadata = trailing_metadata() or ()
    except Exception:
        return ()
    for entry in metadata:
        key = getattr(entry, "key", None)
        value = getattr(entry, "value", None)
        if key is None and isinstance(entry, tuple) and len(entry) == 2:
            key, value = entry
        if key != "grpc-status-details-bin" or not isinstance(value, bytes):
            continue
        try:
            status = _grpc_status_message_class()()
            status.ParseFromString(value)
            return tuple(status.details)
        except Exception:
            return ()
    return ()


def _template_runtime_failure_detail(err: Exception):
    from ._pb import pb

    for detail in _grpc_status_details(err):
        if not detail.type_url.endswith(
            "/tenki.sandbox.v1.TemplateRuntimeFailure"
        ):
            continue
        failure = pb.TemplateRuntimeFailure()
        if detail.Unpack(failure):
            return failure
    return None


def _validation_field_path(path) -> str:
    value = ""
    for element in path.elements:
        if element.field_name:
            if value:
                value += "."
            value += element.field_name
        subscript = element.WhichOneof("subscript")
        if subscript == "string_key":
            value += f"[{json.dumps(element.string_key)}]"
        elif subscript:
            value += f"[{getattr(element, subscript)}]"
    return value


def _template_spec_violation_details(err: Exception) -> tuple[str, ...]:
    from ._internal.proto.buf.validate import validate_pb2

    violations = []
    for detail in _grpc_status_details(err):
        if not detail.type_url.endswith("/buf.validate.Violations"):
            continue
        proto = validate_pb2.Violations()
        if not detail.Unpack(proto):
            continue
        for violation in proto.violations:
            field = _validation_field_path(violation.field)
            violations.append(
                f"{field}: {violation.message}" if field else violation.message
            )
    return tuple(violations)


def map_template_spec_error(err: Exception) -> SandboxError:
    code_fn = getattr(err, "code", None)
    if callable(code_fn):
        code = code_fn()
        if getattr(code, "name", str(code)) == "INVALID_ARGUMENT":
            violations = _template_spec_violation_details(err)
            if violations:
                return TemplateSpecValidationError(violations)
    return map_rpc_error(err)


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
    if code_name == "FAILED_PRECONDITION":
        runtime_failure = _template_runtime_failure_detail(err)
        if runtime_failure is not None:
            from ._convert import sandbox_info

            return TemplateRuntimeFailedError(
                runtime_failure.reason or "template runtime failed",
                sandbox_info(runtime_failure.session),
            )
        if "template runtime failed" in msg:
            return TemplateRuntimeFailedError(details)
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
        if "registry image" in msg:
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
            retryable = bool(_TRANSIENT_GIT.search(details)) and not bool(
                _TERMINAL_GIT.search(details)
            )
            exit_match = _GIT_EXIT_CODE.search(details)
            exit_code = int(exit_match.group(1)) if exit_match else None
            stderr_match = _GIT_STDERR.search(details)
            stderr = stderr_match.group(1) if stderr_match else ""
            return GitOperationFailedError(
                details, stderr=stderr, exit_code=exit_code, retryable=retryable
            )
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
