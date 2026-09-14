from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal
from uuid import uuid4

import grpc

from ._internal.proto.tenki.cloud.workspace.v1beta1 import secrets_pb2 as pb
from ._internal.proto.tenki.cloud.workspace.v1beta1 import secrets_pb2_grpc


class WorkspaceSecretError(Exception):
    """Workspace RPC error with a stable gRPC status for conflict handling."""

    def __init__(self, message: str, code: grpc.StatusCode):
        super().__init__(message)
        self.code = code


def _workspace_secret_error(error):
    return WorkspaceSecretError(error.details(), error.code())


SecretDeliveryMode = Literal["guest_and_injection", "injection_only"]
SecretDestinationMode = Literal["unset", "allowlist", "allow_any"]


@dataclass(frozen=True)
class SecretPolicy:
    delivery_mode: SecretDeliveryMode
    destination_mode: SecretDestinationMode
    allowed_hosts: tuple[str, ...] = ()


@dataclass(frozen=True)
class WorkspaceSecret:
    id: str
    workspace_id: str
    name: str
    active_version: int
    revision: int
    policy: SecretPolicy
    created_at: datetime | None = None
    updated_at: datetime | None = None
    revoked_at: datetime | None = None
    deleted_at: datetime | None = None


@dataclass(frozen=True)
class WorkspaceSecretVersion:
    version: int
    created_at: datetime | None = None
    revoked_at: datetime | None = None


@dataclass(frozen=True)
class SecretPage:
    secrets: list[WorkspaceSecret]
    next_cursor: str


@dataclass(frozen=True)
class SecretVersionPage:
    versions: list[WorkspaceSecretVersion]
    next_cursor: str


_DELIVERY = {
    "guest_and_injection": pb.SECRET_DELIVERY_MODE_GUEST_AND_INJECTION,
    "injection_only": pb.SECRET_DELIVERY_MODE_INJECTION_ONLY,
}
_DESTINATION = {
    "unset": pb.SECRET_DESTINATION_MODE_UNSET,
    "allowlist": pb.SECRET_DESTINATION_MODE_ALLOWLIST,
    "allow_any": pb.SECRET_DESTINATION_MODE_ALLOW_ANY,
}


def _policy(policy: SecretPolicy):
    if (
        policy.delivery_mode not in _DELIVERY
        or policy.destination_mode not in _DESTINATION
    ):
        raise ValueError("Invalid secret policy mode")
    return pb.SecretPolicy(
        delivery_mode=_DELIVERY[policy.delivery_mode],
        destination_mode=_DESTINATION[policy.destination_mode],
        allowed_hosts=policy.allowed_hosts,
    )


def _time(message, field):
    return (
        getattr(message, field).ToDatetime(tzinfo=timezone.utc)
        if message.HasField(field)
        else None
    )


def _metadata(secret) -> WorkspaceSecret:
    delivery = {value: key for key, value in _DELIVERY.items()}.get(
        secret.policy.delivery_mode
    )
    destination = {value: key for key, value in _DESTINATION.items()}.get(
        secret.policy.destination_mode
    )
    if delivery is None or destination is None:
        raise ValueError("Unknown secret policy mode")
    return WorkspaceSecret(
        id=secret.id,
        workspace_id=secret.workspace_id,
        name=secret.name,
        active_version=secret.active_version,
        revision=secret.revision,
        policy=SecretPolicy(delivery, destination, tuple(secret.policy.allowed_hosts)),
        created_at=_time(secret, "created_at"),
        updated_at=_time(secret, "updated_at"),
        revoked_at=_time(secret, "revoked_at"),
        deleted_at=_time(secret, "deleted_at"),
    )


def _version(version) -> WorkspaceSecretVersion:
    return WorkspaceSecretVersion(
        version.version, _time(version, "created_at"), _time(version, "revoked_at")
    )


class _SecretsBase:
    def __init__(self, channel_factory, metadata, timeout: float, workspace_id: str):
        self._channel_factory = channel_factory
        self._cached_stub = None
        self._metadata = metadata
        self._timeout = timeout
        self._workspace_id = workspace_id

    @property
    def _stub(self):
        channel = self._channel_factory()
        if self._cached_stub is None:
            self._cached_stub = secrets_pb2_grpc.WorkspaceSecretsServiceStub(channel)
        return self._cached_stub


class _Secrets(_SecretsBase):
    """Workspace secret management. Reads return metadata only, never values.

    Mutations accept request_id for replay after an uncertain result. No SDK-level
    automatic retries are performed. The cloud endpoint is configured independently.
    """

    def create(
        self,
        name: str,
        value: bytes,
        policy: SecretPolicy,
        *,
        request_id: str | None = None,
    ) -> WorkspaceSecret:
        if not isinstance(value, bytes):
            raise TypeError("Secret value must be bytes")
        request = pb.CreateSecretRequest(
            workspace_id=self._workspace_id,
            name=name,
            value=value,
            policy=_policy(policy),
            request_id=request_id if request_id is not None else str(uuid4()),
        )
        try:
            response = self._stub.CreateSecret(
                request, metadata=self._metadata, timeout=self._timeout
            )
        except grpc.RpcError as err:
            raise _workspace_secret_error(err) from err
        return _metadata(response.secret)

    def update(
        self,
        secret_id: str,
        *,
        expected_revision: int,
        value: bytes | None = None,
        policy: SecretPolicy | None = None,
        active_version: int | None = None,
        request_id: str | None = None,
    ) -> WorkspaceSecret:
        """Omitted/None value retains it; b"" creates an empty value."""
        request = pb.UpdateSecretRequest(
            workspace_id=self._workspace_id,
            secret_id=secret_id,
            expected_revision=expected_revision,
            request_id=request_id if request_id is not None else str(uuid4()),
        )
        if value is not None:
            if not isinstance(value, bytes):
                raise TypeError("Secret value must be bytes")
            request.value = value
        if policy is not None:
            request.policy.CopyFrom(_policy(policy))
        if active_version is not None:
            request.active_version = active_version
        try:
            response = self._stub.UpdateSecret(
                request, metadata=self._metadata, timeout=self._timeout
            )
        except grpc.RpcError as err:
            raise _workspace_secret_error(err) from err
        return _metadata(response.secret)

    def get(self, secret_id: str) -> WorkspaceSecret:
        request = pb.GetSecretRequest(
            workspace_id=self._workspace_id, secret_id=secret_id
        )
        try:
            response = self._stub.GetSecret(
                request, metadata=self._metadata, timeout=self._timeout
            )
        except grpc.RpcError as err:
            raise _workspace_secret_error(err) from err
        return _metadata(response.secret)

    def list(self, *, page_size: int = 0, cursor: str = "") -> SecretPage:
        request = pb.ListSecretsRequest(
            workspace_id=self._workspace_id, page_size=page_size, cursor=cursor
        )
        try:
            response = self._stub.ListSecrets(
                request, metadata=self._metadata, timeout=self._timeout
            )
        except grpc.RpcError as err:
            raise _workspace_secret_error(err) from err
        return SecretPage(
            [_metadata(secret) for secret in response.secrets], response.next_cursor
        )

    def list_versions(
        self, secret_id: str, *, page_size: int = 0, cursor: str = ""
    ) -> SecretVersionPage:
        request = pb.ListSecretVersionsRequest(
            workspace_id=self._workspace_id,
            secret_id=secret_id,
            page_size=page_size,
            cursor=cursor,
        )
        try:
            response = self._stub.ListSecretVersions(
                request, metadata=self._metadata, timeout=self._timeout
            )
        except grpc.RpcError as err:
            raise _workspace_secret_error(err) from err
        return SecretVersionPage(
            [_version(version) for version in response.versions], response.next_cursor
        )

    def revoke(
        self,
        secret_id: str,
        *,
        expected_revision: int,
        version: int | None = None,
        request_id: str | None = None,
    ) -> WorkspaceSecret:
        request = pb.RevokeSecretRequest(
            workspace_id=self._workspace_id,
            secret_id=secret_id,
            expected_revision=expected_revision,
            request_id=request_id if request_id is not None else str(uuid4()),
        )
        if version is not None:
            request.version = version
        try:
            response = self._stub.RevokeSecret(
                request, metadata=self._metadata, timeout=self._timeout
            )
        except grpc.RpcError as err:
            raise _workspace_secret_error(err) from err
        return _metadata(response.secret)

    def delete(
        self,
        secret_id: str,
        *,
        expected_revision: int,
        request_id: str | None = None,
    ) -> WorkspaceSecret:
        request = pb.DeleteSecretRequest(
            workspace_id=self._workspace_id,
            secret_id=secret_id,
            expected_revision=expected_revision,
            request_id=request_id if request_id is not None else str(uuid4()),
        )
        try:
            response = self._stub.DeleteSecret(
                request, metadata=self._metadata, timeout=self._timeout
            )
        except grpc.RpcError as err:
            raise _workspace_secret_error(err) from err
        return _metadata(response.secret)


class _AsyncSecrets(_SecretsBase):
    """Workspace secret management. Reads return metadata only, never values.

    Mutations accept request_id for replay after an uncertain result. No SDK-level
    automatic retries are performed. The cloud endpoint is configured independently.
    """

    async def create(
        self,
        name: str,
        value: bytes,
        policy: SecretPolicy,
        *,
        request_id: str | None = None,
    ) -> WorkspaceSecret:
        if not isinstance(value, bytes):
            raise TypeError("Secret value must be bytes")
        request = pb.CreateSecretRequest(
            workspace_id=self._workspace_id,
            name=name,
            value=value,
            policy=_policy(policy),
            request_id=request_id if request_id is not None else str(uuid4()),
        )
        try:
            response = await self._stub.CreateSecret(
                request, metadata=self._metadata, timeout=self._timeout
            )
        except grpc.RpcError as err:
            raise _workspace_secret_error(err) from err
        return _metadata(response.secret)

    async def update(
        self,
        secret_id: str,
        *,
        expected_revision: int,
        value: bytes | None = None,
        policy: SecretPolicy | None = None,
        active_version: int | None = None,
        request_id: str | None = None,
    ) -> WorkspaceSecret:
        """Omitted/None value retains it; b"" creates an empty value."""
        request = pb.UpdateSecretRequest(
            workspace_id=self._workspace_id,
            secret_id=secret_id,
            expected_revision=expected_revision,
            request_id=request_id if request_id is not None else str(uuid4()),
        )
        if value is not None:
            if not isinstance(value, bytes):
                raise TypeError("Secret value must be bytes")
            request.value = value
        if policy is not None:
            request.policy.CopyFrom(_policy(policy))
        if active_version is not None:
            request.active_version = active_version
        try:
            response = await self._stub.UpdateSecret(
                request, metadata=self._metadata, timeout=self._timeout
            )
        except grpc.RpcError as err:
            raise _workspace_secret_error(err) from err
        return _metadata(response.secret)

    async def get(self, secret_id: str) -> WorkspaceSecret:
        request = pb.GetSecretRequest(
            workspace_id=self._workspace_id, secret_id=secret_id
        )
        try:
            response = await self._stub.GetSecret(
                request, metadata=self._metadata, timeout=self._timeout
            )
        except grpc.RpcError as err:
            raise _workspace_secret_error(err) from err
        return _metadata(response.secret)

    async def list(self, *, page_size: int = 0, cursor: str = "") -> SecretPage:
        request = pb.ListSecretsRequest(
            workspace_id=self._workspace_id, page_size=page_size, cursor=cursor
        )
        try:
            response = await self._stub.ListSecrets(
                request, metadata=self._metadata, timeout=self._timeout
            )
        except grpc.RpcError as err:
            raise _workspace_secret_error(err) from err
        return SecretPage(
            [_metadata(secret) for secret in response.secrets], response.next_cursor
        )

    async def list_versions(
        self, secret_id: str, *, page_size: int = 0, cursor: str = ""
    ) -> SecretVersionPage:
        request = pb.ListSecretVersionsRequest(
            workspace_id=self._workspace_id,
            secret_id=secret_id,
            page_size=page_size,
            cursor=cursor,
        )
        try:
            response = await self._stub.ListSecretVersions(
                request, metadata=self._metadata, timeout=self._timeout
            )
        except grpc.RpcError as err:
            raise _workspace_secret_error(err) from err
        return SecretVersionPage(
            [_version(version) for version in response.versions], response.next_cursor
        )

    async def revoke(
        self,
        secret_id: str,
        *,
        expected_revision: int,
        version: int | None = None,
        request_id: str | None = None,
    ) -> WorkspaceSecret:
        request = pb.RevokeSecretRequest(
            workspace_id=self._workspace_id,
            secret_id=secret_id,
            expected_revision=expected_revision,
            request_id=request_id if request_id is not None else str(uuid4()),
        )
        if version is not None:
            request.version = version
        try:
            response = await self._stub.RevokeSecret(
                request, metadata=self._metadata, timeout=self._timeout
            )
        except grpc.RpcError as err:
            raise _workspace_secret_error(err) from err
        return _metadata(response.secret)

    async def delete(
        self,
        secret_id: str,
        *,
        expected_revision: int,
        request_id: str | None = None,
    ) -> WorkspaceSecret:
        request = pb.DeleteSecretRequest(
            workspace_id=self._workspace_id,
            secret_id=secret_id,
            expected_revision=expected_revision,
            request_id=request_id if request_id is not None else str(uuid4()),
        )
        try:
            response = await self._stub.DeleteSecret(
                request, metadata=self._metadata, timeout=self._timeout
            )
        except grpc.RpcError as err:
            raise _workspace_secret_error(err) from err
        return _metadata(response.secret)
