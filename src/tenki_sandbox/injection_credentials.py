from __future__ import annotations

from dataclasses import InitVar, dataclass, field
from typing import Sequence

import grpc

from ._internal.proto.tenki.sandbox.v1 import session_injection_credentials_pb2 as credentials_pb
from .errors import CapabilityUnavailableError, FileTooLargeError, GitOperationFailedError, TemplateRuntimeFailedError, map_rpc_error
from .models import SandboxInfo, SessionEgressPolicy


@dataclass(frozen=True)
class InjectionRule:
    origin: str
    methods: Sequence[str]
    path_prefix: str
    header: str

    def __post_init__(self):
        object.__setattr__(self, "methods", tuple(self.methods))


class _CredentialValue:
    __slots__ = ("_value",)

    @property
    def value(self) -> bytes:
        return self._value


@dataclass(frozen=True)
class InjectionCredentialInput(_CredentialValue):
    name: str
    value: InitVar[bytes] = field()
    rules: Sequence[InjectionRule]

    def __post_init__(self, value):
        object.__setattr__(self, "_value", bytes(value))
        object.__setattr__(self, "rules", tuple(self.rules))

    def __getstate__(self):
        raise TypeError("InjectionCredentialInput cannot be pickled")


@dataclass(frozen=True)
class InjectionCredentialMetadata:
    credential_id: str
    name: str
    revision: int
    revoked: bool


def injection_input(value: InjectionCredentialInput):
    return credentials_pb.SessionInjectionCredentialInput(
        name=value.name,
        value=value._value,
        rules=[credentials_pb.SessionInjectionRule(
            origin=rule.origin, methods=rule.methods,
            path_prefix=rule.path_prefix, header=rule.header,
        ) for rule in value.rules],
    )


def injection_metadata(value) -> InjectionCredentialMetadata:
    return InjectionCredentialMetadata(value.credential_id, value.name, value.revision, value.revoked)


def injection_revision(value: int | None) -> int | None:
    if value is not None and (type(value) is not int or not 0 <= value <= 0xffffffff):
        raise ValueError("expected revision must be a uint32")
    return value


def _status_code(error: BaseException) -> grpc.StatusCode | None:
    # Transports raise map_rpc_error(err) from err, so the status may sit on the __cause__ chain.
    seen: set[int] = set()
    source: BaseException | None = error
    while source is not None and id(source) not in seen:
        seen.add(id(source))
        code_fn = getattr(source, "code", None)
        if callable(code_fn):
            return grpc.StatusCode.__members__.get(getattr(code_fn(), "name", ""))
        source = source.__cause__
    return None


def safe_injection_error(error: Exception):
    mapped = map_rpc_error(error)
    message = "session injection credential operation failed"
    if isinstance(mapped, TemplateRuntimeFailedError):
        sandbox = mapped.sandbox
        identity = SandboxInfo(
            id=sandbox.id, state=sandbox.state,
            egress=SessionEgressPolicy(public_egress=sandbox.egress.public_egress),
        ) if sandbox is not None else None
        safe = TemplateRuntimeFailedError("template runtime failed", identity)
    elif isinstance(mapped, FileTooLargeError):
        safe = FileTooLargeError(message, operation=mapped.operation, size_bytes=mapped.size_bytes, max_bytes=mapped.max_bytes)
    elif isinstance(mapped, GitOperationFailedError):
        safe = GitOperationFailedError(message, exit_code=mapped.exit_code, retryable=mapped.retryable)
    else:
        safe = type(mapped)(message, primitive=mapped.primitive) if isinstance(mapped, CapabilityUnavailableError) else type(mapped)(message)
        safe.retryable = mapped.retryable
    # Remote text is dropped; the status survives like WorkspaceSecretError.code.
    safe.code = _status_code(error)
    return safe
