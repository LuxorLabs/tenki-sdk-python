from __future__ import annotations

import re
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


# Where a secret applies: every CI job and sandbox in the workspace, or CI jobs in one repository.
SecretAvailability = Literal["workspace", "repository"]


@dataclass(frozen=True)
class WorkspaceSecret:
    id: str
    workspace_id: str
    name: str
    active_version: int
    revision: int
    created_at: datetime | None = None
    updated_at: datetime | None = None
    revoked_at: datetime | None = None
    deleted_at: datetime | None = None
    # Repository (owner/name) whose CI jobs get the secret; None for a workspace secret.
    repository: str | None = None
    # One-line note for the people who manage the secret; empty when there is none.
    description: str = ""


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


@dataclass(frozen=True)
class SecretRepositoryPage:
    # Full names (owner/name), ordered by name.
    repositories: list[str]
    next_cursor: str


def _repository_page(response) -> SecretRepositoryPage:
    return SecretRepositoryPage(
        [repository.full_name for repository in response.repositories],
        response.next_cursor,
    )


def _time(message, field):
    return (
        getattr(message, field).ToDatetime(tzinfo=timezone.utc)
        if message.HasField(field)
        else None
    )


def _metadata(secret) -> WorkspaceSecret:
    return WorkspaceSecret(
        id=secret.id,
        workspace_id=secret.workspace_id,
        name=secret.name,
        active_version=secret.active_version,
        revision=secret.revision,
        created_at=_time(secret, "created_at"),
        updated_at=_time(secret, "updated_at"),
        revoked_at=_time(secret, "revoked_at"),
        deleted_at=_time(secret, "deleted_at"),
        repository=secret.repository or None,
        description=secret.description,
    )


def _repository(repository: str) -> str:
    if not isinstance(repository, str) or not re.fullmatch(r"[^/]+/[^/]+", repository):
        raise ValueError(f"repository must be owner/name, got {repository!r}")
    return repository


def _availability(availability: SecretAvailability | None, repository: str | None) -> int:
    # As in the CLI: a repository alone means repository availability, and workspace refuses one.
    if repository:
        if availability == "workspace":
            raise ValueError("availability 'workspace' cannot be combined with a repository")
        _repository(repository)
    if availability is None:
        if repository:
            return pb.SECRET_AVAILABILITY_REPOSITORY
        return pb.SECRET_AVAILABILITY_UNSPECIFIED
    if availability not in ("workspace", "repository"):
        raise ValueError(f"availability must be 'workspace' or 'repository', got {availability!r}")
    return pb.SecretAvailability.Value(f"SECRET_AVAILABILITY_{availability.upper()}")


def _create_request(
    workspace_id: str,
    name: str,
    value: bytes,
    repository: str | None,
    description: str | None,
    request_id: str | None,
):
    if not isinstance(value, bytes):
        raise TypeError("Secret value must be bytes")
    return pb.CreateSecretRequest(
        workspace_id=workspace_id,
        name=name,
        value=value,
        request_id=request_id if request_id is not None else str(uuid4()),
        repository=_repository(repository) if repository else "",
        description=description or "",
    )


def _update_request(
    workspace_id: str,
    secret_id: str,
    expected_revision: int,
    value: bytes | None,
    active_version: int | None,
    description: str | None,
    availability: SecretAvailability | None,
    repository: str | None,
    request_id: str | None,
):
    request = pb.UpdateSecretRequest(
        workspace_id=workspace_id,
        secret_id=secret_id,
        expected_revision=expected_revision,
        request_id=request_id if request_id is not None else str(uuid4()),
        availability=_availability(availability, repository),
        repository=_repository(repository) if availability == "repository" else repository or "",
    )
    if value is not None:
        if not isinstance(value, bytes):
            raise TypeError("Secret value must be bytes")
        request.value = value
    if active_version is not None:
        request.active_version = active_version
    if description is not None:
        request.description = description
    return request


def _list_request(
    workspace_id: str,
    page_size: int,
    cursor: str,
    availability: SecretAvailability | None,
    repository: str | None,
):
    return pb.ListSecretsRequest(
        workspace_id=workspace_id,
        page_size=page_size,
        cursor=cursor,
        availability=_availability(availability, repository),
        repository=repository or "",
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

    @property
    def policies(self):
        from .secret_policies import _SecretPolicies
        return _SecretPolicies(self)

    def create(
        self,
        name: str,
        value: bytes,
        *,
        repository: str | None = None,
        description: str | None = None,
        request_id: str | None = None,
    ) -> WorkspaceSecret:
        """Create a secret. With ``repository`` (owner/name, connected to the workspace),
        only CI jobs there get it, in place of a workspace secret of the same name."""
        request = _create_request(
            self._workspace_id, name, value, repository, description, request_id
        )
        return self._create(request)

    def _create(self, request) -> WorkspaceSecret:
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
        active_version: int | None = None,
        description: str | None = None,
        availability: SecretAvailability | None = None,
        repository: str | None = None,
        request_id: str | None = None,
    ) -> WorkspaceSecret:
        """Change what is set. A value or active_version restores a revoked secret; b"" creates an
        empty value. description="" clears it; availability="repository", or repository alone,
        moves the secret to repository."""
        request = _update_request(
            self._workspace_id,
            secret_id,
            expected_revision,
            value,
            active_version,
            description,
            availability,
            repository,
            request_id,
        )
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

    def list(
        self,
        *,
        page_size: int = 0,
        cursor: str = "",
        availability: SecretAvailability | None = None,
        repository: str | None = None,
    ) -> SecretPage:
        """availability filters the list; repository, with or without availability="repository",
        narrows it to one repository."""
        request = _list_request(
            self._workspace_id, page_size, cursor, availability, repository
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

    def list_repositories(
        self, *, page_size: int = 0, cursor: str = "", search: str = ""
    ) -> SecretRepositoryPage:
        """Lists the connected repositories a secret can be limited to."""
        request = pb.ListSecretRepositoriesRequest(
            workspace_id=self._workspace_id,
            page_size=page_size,
            cursor=cursor,
            search=search,
        )
        try:
            response = self._stub.ListSecretRepositories(
                request, metadata=self._metadata, timeout=self._timeout
            )
        except grpc.RpcError as err:
            raise _workspace_secret_error(err) from err
        return _repository_page(response)

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

    @property
    def policies(self):
        from .secret_policies import _AsyncSecretPolicies
        return _AsyncSecretPolicies(self)

    async def create(
        self,
        name: str,
        value: bytes,
        *,
        repository: str | None = None,
        description: str | None = None,
        request_id: str | None = None,
    ) -> WorkspaceSecret:
        """Create a secret. With ``repository`` (owner/name, connected to the workspace),
        only CI jobs there get it, in place of a workspace secret of the same name."""
        request = _create_request(
            self._workspace_id, name, value, repository, description, request_id
        )
        return await self._create(request)

    async def _create(self, request) -> WorkspaceSecret:
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
        active_version: int | None = None,
        description: str | None = None,
        availability: SecretAvailability | None = None,
        repository: str | None = None,
        request_id: str | None = None,
    ) -> WorkspaceSecret:
        """Change what is set. A value or active_version restores a revoked secret; b"" creates an
        empty value. description="" clears it; availability="repository", or repository alone,
        moves the secret to repository."""
        request = _update_request(
            self._workspace_id,
            secret_id,
            expected_revision,
            value,
            active_version,
            description,
            availability,
            repository,
            request_id,
        )
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

    async def list(
        self,
        *,
        page_size: int = 0,
        cursor: str = "",
        availability: SecretAvailability | None = None,
        repository: str | None = None,
    ) -> SecretPage:
        """availability filters the list; repository, with or without availability="repository",
        narrows it to one repository."""
        request = _list_request(
            self._workspace_id, page_size, cursor, availability, repository
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

    async def list_repositories(
        self, *, page_size: int = 0, cursor: str = "", search: str = ""
    ) -> SecretRepositoryPage:
        """Lists the connected repositories a secret can be limited to."""
        request = pb.ListSecretRepositoriesRequest(
            workspace_id=self._workspace_id,
            page_size=page_size,
            cursor=cursor,
            search=search,
        )
        try:
            response = await self._stub.ListSecretRepositories(
                request, metadata=self._metadata, timeout=self._timeout
            )
        except grpc.RpcError as err:
            raise _workspace_secret_error(err) from err
        return _repository_page(response)

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
