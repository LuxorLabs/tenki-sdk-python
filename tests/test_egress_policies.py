from __future__ import annotations

import inspect
from datetime import datetime, timezone

import pytest
from google.protobuf.timestamp_pb2 import Timestamp

from tenki_sandbox import (
    AsyncClient,
    Client,
    EgressHttpRule,
    EgressPolicies,
    EgressPolicy,
    EgressPolicyNotFoundError,
    SessionEgressPolicy,
    SessionNotFoundError,
)
from tenki_sandbox._pb import pb
from tenki_sandbox.errors import TemplateNotFoundError, VolumeNotFoundError, map_rpc_error
from tenki_sandbox.aio import AsyncEgressPolicies

CREATED_AT = datetime(2026, 10, 8, 9, 0, tzinfo=timezone.utc)


def _timestamp() -> Timestamp:
    ts = Timestamp()
    ts.FromDatetime(CREATED_AT)
    return ts


def _policy(name: str) -> pb.EgressPolicy:
    return pb.EgressPolicy(
        id=f"policy-{name}",
        workspace_id="ws-1",
        name=name,
        revision=3,
        active_session_count=2,
        egress=pb.SessionEgressPolicy(allow_domains=["github.com"], intercept_https=True, allowed_ports=[22, 8443]),
        created_at=_timestamp(),
        updated_at=_timestamp(),
    )


class NotFound(Exception):
    def __init__(self, message: str = "egress policy not found"):
        super().__init__(message)
        self._message = message

    def code(self):
        return type("Code", (), {"name": "NOT_FOUND"})()

    def details(self):
        return self._message


class EgressPolicyRPC:
    def __init__(self):
        self.requests = []

    def create_egress_policy(self, req):
        self.requests.append(req)
        return pb.CreateEgressPolicyResponse(policy=_policy(req.name))

    def update_egress_policy(self, req):
        self.requests.append(req)
        return pb.UpdateEgressPolicyResponse(policy=_policy("registries"))

    def get_egress_policy(self, req):
        self.requests.append(req)
        if req.policy_id == "missing":
            raise NotFound()
        return pb.GetEgressPolicyResponse(policy=_policy(req.policy_id))

    def list_egress_policies(self, req):
        self.requests.append(req)
        if not req.cursor:
            return pb.ListEgressPoliciesResponse(policies=[_policy("a")], next_cursor="a")
        return pb.ListEgressPoliciesResponse(policies=[_policy("b")])

    def delete_egress_policy(self, req):
        self.requests.append(req)
        deleted = _policy("registries")
        deleted.deleted_at.CopyFrom(_timestamp())
        return pb.DeleteEgressPolicyResponse(policy=deleted)


class AsyncEgressPolicyRPC(EgressPolicyRPC):
    async def create_egress_policy(self, req):
        return super().create_egress_policy(req)

    async def update_egress_policy(self, req):
        return super().update_egress_policy(req)

    async def get_egress_policy(self, req):
        return super().get_egress_policy(req)

    async def list_egress_policies(self, req):
        return super().list_egress_policies(req)

    async def delete_egress_policy(self, req):
        return super().delete_egress_policy(req)


RULES = [EgressHttpRule(domains=("github.com",), methods=("GET",), path_prefix="/repos/")]
WANT_BODY = pb.SessionEgressPolicy(
    allow_domains=["github.com"],
    allow_cidrs=["10.0.0.0/8"],
    intercept_https=True,
    allowed_ports=[22, 8443],
    http_rules=[pb.SessionEgressHttpRule(domains=["github.com"], methods=["GET"], prefix="/repos/")],
)
WANT_REQUESTS = [
    pb.CreateEgressPolicyRequest(workspace_id="ws-1", name="registries", egress=WANT_BODY),
    pb.UpdateEgressPolicyRequest(policy_id="registries", expected_revision=3, egress=WANT_BODY),
    pb.GetEgressPolicyRequest(policy_id="registries"),
    pb.ListEgressPoliciesRequest(workspace_id="ws-1", page_size=100),
    pb.ListEgressPoliciesRequest(workspace_id="ws-1", page_size=100, cursor="a"),
    pb.DeleteEgressPolicyRequest(policy_id="registries", expected_revision=4),
]
WANT_CREATED = EgressPolicy(
    id="policy-registries",
    workspace_id="ws-1",
    name="registries",
    revision=3,
    egress=SessionEgressPolicy(allow_domains=("github.com",), intercept_https=True, allowed_ports=(22, 8443)),
    active_session_count=2,
    created_at=CREATED_AT,
    updated_at=CREATED_AT,
)
BODY = dict(
    allow_domains=["github.com"],
    allow_cidrs=["10.0.0.0/8"],
    intercept_https=True,
    allowed_ports=[22, 8443],
    http_rules=RULES,
)


def test_egress_policy_crud_wire_contract():
    rpc = EgressPolicyRPC()
    policies = Client(rpc=rpc).egress_policies

    created = policies.create("registries", workspace_id=" ws-1 ", **BODY)
    policies.update("registries", expected_revision=3, **BODY)
    policies.get("registries")
    listed = policies.list(workspace_id="ws-1")
    deleted = policies.delete("registries", expected_revision=4)

    assert created == WANT_CREATED
    assert [policy.name for policy in listed] == ["a", "b"]
    assert deleted.deleted_at == CREATED_AT
    assert rpc.requests == WANT_REQUESTS
    assert not rpc.requests[1].HasField("workspace_id")


async def test_async_egress_policy_crud_wire_contract():
    rpc = AsyncEgressPolicyRPC()
    policies = AsyncClient(rpc=rpc).egress_policies

    created = await policies.create("registries", workspace_id="ws-1", **BODY)
    await policies.update("registries", expected_revision=3, **BODY)
    await policies.get("registries")
    listed = await policies.list(workspace_id="ws-1")
    await policies.delete("registries", expected_revision=4)

    assert created == WANT_CREATED
    assert [policy.name for policy in listed] == ["a", "b"]
    assert rpc.requests == WANT_REQUESTS


def test_missing_egress_policy_maps_to_its_own_error():
    with pytest.raises(EgressPolicyNotFoundError) as caught:
        Client(rpc=EgressPolicyRPC()).egress_policies.get("missing")
    assert not isinstance(caught.value, SessionNotFoundError)


def test_rule_with_two_paths_is_refused_before_the_request():
    rpc = EgressPolicyRPC()
    with pytest.raises(ValueError, match="only one of"):
        Client(rpc=rpc).egress_policies.create(
            "registries",
            allow_domains=["github.com"],
            intercept_https=True,
            http_rules=[EgressHttpRule(domains=("github.com",), path_exact="/a", path_regex="b")],
        )
    assert rpc.requests == []


def test_async_egress_policies_mirror_the_sync_surface():
    sync_methods = {name for name, _ in inspect.getmembers(EgressPolicies, inspect.isfunction) if not name.startswith("_")}
    async_methods = {name for name, fn in inspect.getmembers(AsyncEgressPolicies, inspect.iscoroutinefunction)}
    assert sync_methods == async_methods == {"create", "update", "get", "list", "delete"}


@pytest.mark.parametrize(
    "message",
    ["egress policies not found: ci-template", "egress policies not found: volume-sync, artifact-store"],
)
def test_missing_policy_names_never_map_to_another_not_found_error(message):
    err = map_rpc_error(NotFound(message))
    assert isinstance(err, EgressPolicyNotFoundError)
    assert not isinstance(err, (TemplateNotFoundError, VolumeNotFoundError, SessionNotFoundError))
