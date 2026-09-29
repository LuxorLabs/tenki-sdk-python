from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import uuid4
import grpc

from ._internal.proto.tenki.cloud.workspace.v1beta1 import secrets_pb2 as pb
from .workspace_secrets import _time, _workspace_secret_error


@dataclass(frozen=True)
class SecretRequestRule:
    origin: str
    methods: list[str]
    path_prefix: str
    secrets: list[str]
    header: str = ""
    query_parameter: str = ""
    json_pointer: str = ""


def _rule(rule: SecretRequestRule):
    return pb.SecretRequestRule(origin=rule.origin, methods=rule.methods,
        path_prefix=rule.path_prefix, secrets=rule.secrets, header=rule.header,
        query_parameter=rule.query_parameter, json_pointer=rule.json_pointer)


@dataclass(frozen=True)
class WorkspaceSecretPolicy:
    id: str
    workspace_id: str
    name: str
    revision: int
    attached_session_count: int
    rules: list[SecretRequestRule]
    created_at: datetime | None = None
    updated_at: datetime | None = None
    deleted_at: datetime | None = None


@dataclass(frozen=True)
class SecretPolicyPage:
    policies: list[WorkspaceSecretPolicy]
    next_cursor: str


def _metadata(policy) -> WorkspaceSecretPolicy:
    if not policy.id:
        raise ValueError("Missing secret policy metadata")
    return WorkspaceSecretPolicy(
        id=policy.id, workspace_id=policy.workspace_id, name=policy.name,
        revision=policy.revision, attached_session_count=policy.attached_session_count,
        rules=[SecretRequestRule(origin=r.origin, methods=list(r.methods), path_prefix=r.path_prefix,
            secrets=list(r.secrets), header=r.header, query_parameter=r.query_parameter, json_pointer=r.json_pointer) for r in policy.rules],
        created_at=_time(policy, "created_at"), updated_at=_time(policy, "updated_at"), deleted_at=_time(policy, "deleted_at"))


class _SecretPolicies:
    def __init__(self, secrets):
        self._secrets = secrets

    def create(self, name: str, rules: list[SecretRequestRule], *, request_id: str | None = None) -> WorkspaceSecretPolicy:
        request = pb.CreateSecretPolicyRequest(workspace_id=self._secrets._workspace_id, name=name, rules=[_rule(r) for r in rules], request_id=request_id if request_id is not None else str(uuid4()))
        try:
            response = self._secrets._stub.CreateSecretPolicy(request, metadata=self._secrets._metadata, timeout=self._secrets._timeout)
        except grpc.RpcError as err:
            raise _workspace_secret_error(err) from err
        return _metadata(response.policy)

    def update(self, policy_id: str, rules: list[SecretRequestRule], *, expected_revision: int, request_id: str | None = None) -> WorkspaceSecretPolicy:
        request = pb.UpdateSecretPolicyRequest(workspace_id=self._secrets._workspace_id, policy_id=policy_id, rules=[_rule(r) for r in rules], expected_revision=expected_revision, request_id=request_id if request_id is not None else str(uuid4()))
        try:
            response = self._secrets._stub.UpdateSecretPolicy(request, metadata=self._secrets._metadata, timeout=self._secrets._timeout)
        except grpc.RpcError as err:
            raise _workspace_secret_error(err) from err
        return _metadata(response.policy)

    def delete(self, policy_id: str, *, expected_revision: int, request_id: str | None = None) -> WorkspaceSecretPolicy:
        request = pb.DeleteSecretPolicyRequest(workspace_id=self._secrets._workspace_id, policy_id=policy_id, expected_revision=expected_revision, request_id=request_id if request_id is not None else str(uuid4()))
        try:
            response = self._secrets._stub.DeleteSecretPolicy(request, metadata=self._secrets._metadata, timeout=self._secrets._timeout)
        except grpc.RpcError as err:
            raise _workspace_secret_error(err) from err
        return _metadata(response.policy)

    def get(self, name_or_id: str) -> WorkspaceSecretPolicy:
        request = pb.GetSecretPolicyRequest(workspace_id=self._secrets._workspace_id, policy_id=name_or_id)
        try:
            response = self._secrets._stub.GetSecretPolicy(request, metadata=self._secrets._metadata, timeout=self._secrets._timeout)
        except grpc.RpcError as err:
            raise _workspace_secret_error(err) from err
        return _metadata(response.policy)

    def list(self, *, page_size: int = 0, cursor: str = "") -> SecretPolicyPage:
        request = pb.ListSecretPoliciesRequest(workspace_id=self._secrets._workspace_id, page_size=page_size, cursor=cursor)
        try:
            response = self._secrets._stub.ListSecretPolicies(request, metadata=self._secrets._metadata, timeout=self._secrets._timeout)
        except grpc.RpcError as err:
            raise _workspace_secret_error(err) from err
        return SecretPolicyPage([_metadata(p) for p in response.policies], response.next_cursor)


class _AsyncSecretPolicies:
    def __init__(self, secrets):
        self._secrets = secrets

    async def create(self, name: str, rules: list[SecretRequestRule], *, request_id: str | None = None) -> WorkspaceSecretPolicy:
        request = pb.CreateSecretPolicyRequest(workspace_id=self._secrets._workspace_id, name=name, rules=[_rule(r) for r in rules], request_id=request_id if request_id is not None else str(uuid4()))
        try:
            response = await self._secrets._stub.CreateSecretPolicy(request, metadata=self._secrets._metadata, timeout=self._secrets._timeout)
        except grpc.RpcError as err:
            raise _workspace_secret_error(err) from err
        return _metadata(response.policy)

    async def update(self, policy_id: str, rules: list[SecretRequestRule], *, expected_revision: int, request_id: str | None = None) -> WorkspaceSecretPolicy:
        request = pb.UpdateSecretPolicyRequest(workspace_id=self._secrets._workspace_id, policy_id=policy_id, rules=[_rule(r) for r in rules], expected_revision=expected_revision, request_id=request_id if request_id is not None else str(uuid4()))
        try:
            response = await self._secrets._stub.UpdateSecretPolicy(request, metadata=self._secrets._metadata, timeout=self._secrets._timeout)
        except grpc.RpcError as err:
            raise _workspace_secret_error(err) from err
        return _metadata(response.policy)

    async def delete(self, policy_id: str, *, expected_revision: int, request_id: str | None = None) -> WorkspaceSecretPolicy:
        request = pb.DeleteSecretPolicyRequest(workspace_id=self._secrets._workspace_id, policy_id=policy_id, expected_revision=expected_revision, request_id=request_id if request_id is not None else str(uuid4()))
        try:
            response = await self._secrets._stub.DeleteSecretPolicy(request, metadata=self._secrets._metadata, timeout=self._secrets._timeout)
        except grpc.RpcError as err:
            raise _workspace_secret_error(err) from err
        return _metadata(response.policy)

    async def get(self, name_or_id: str) -> WorkspaceSecretPolicy:
        request = pb.GetSecretPolicyRequest(workspace_id=self._secrets._workspace_id, policy_id=name_or_id)
        try:
            response = await self._secrets._stub.GetSecretPolicy(request, metadata=self._secrets._metadata, timeout=self._secrets._timeout)
        except grpc.RpcError as err:
            raise _workspace_secret_error(err) from err
        return _metadata(response.policy)

    async def list(self, *, page_size: int = 0, cursor: str = "") -> SecretPolicyPage:
        request = pb.ListSecretPoliciesRequest(workspace_id=self._secrets._workspace_id, page_size=page_size, cursor=cursor)
        try:
            response = await self._secrets._stub.ListSecretPolicies(request, metadata=self._secrets._metadata, timeout=self._secrets._timeout)
        except grpc.RpcError as err:
            raise _workspace_secret_error(err) from err
        return SecretPolicyPage([_metadata(p) for p in response.policies], response.next_cursor)
