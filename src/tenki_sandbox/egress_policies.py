from __future__ import annotations

from ._convert import egress_http_rule_proto, session_egress_policy
from ._pagination import advance_page_token
from ._pb import pb
from ._time import datetime_from_proto
from .errors import map_rpc_error
from .models import EgressHttpRule, EgressPolicy


def _workspace(workspace_id: str | None) -> dict[str, str]:
    workspace_id = (workspace_id or "").strip()
    return {"workspace_id": workspace_id} if workspace_id else {}


def _body(
    allow_domains: list[str] | None,
    allow_cidrs: list[str] | None,
    intercept_https: bool,
    http_rules: list[EgressHttpRule] | None,
    allowed_ports: list[int] | None,
) -> "pb.SessionEgressPolicy":
    return pb.SessionEgressPolicy(
        allow_domains=list(allow_domains or []),
        allow_cidrs=list(allow_cidrs or []),
        intercept_https=intercept_https,
        allowed_ports=list(allowed_ports or []),
        http_rules=[egress_http_rule_proto(rule) for rule in http_rules or []],
    )


def egress_policy(proto) -> EgressPolicy:
    return EgressPolicy(
        id=proto.id,
        workspace_id=proto.workspace_id,
        name=proto.name,
        revision=proto.revision,
        egress=session_egress_policy(proto.egress),
        active_session_count=proto.active_session_count,
        created_at=datetime_from_proto(proto.created_at) if proto.HasField("created_at") else None,
        updated_at=datetime_from_proto(proto.updated_at) if proto.HasField("updated_at") else None,
        deleted_at=datetime_from_proto(proto.deleted_at) if proto.HasField("deleted_at") else None,
    )


def build_create_request(name, workspace_id, allow_domains, allow_cidrs, intercept_https, http_rules, allowed_ports):
    return pb.CreateEgressPolicyRequest(
        name=name,
        egress=_body(allow_domains, allow_cidrs, intercept_https, http_rules, allowed_ports),
        **_workspace(workspace_id),
    )


def build_update_request(
    name_or_id, expected_revision, workspace_id, allow_domains, allow_cidrs, intercept_https, http_rules, allowed_ports
):
    return pb.UpdateEgressPolicyRequest(
        policy_id=name_or_id,
        expected_revision=expected_revision,
        egress=_body(allow_domains, allow_cidrs, intercept_https, http_rules, allowed_ports),
        **_workspace(workspace_id),
    )


class EgressPolicies:
    """Named workspace egress policies. Sandboxes pin them with ``Sandbox.create(egress_policies=[...])``."""

    def __init__(self, rpc) -> None:
        self._rpc = rpc

    def create(
        self,
        name: str,
        *,
        allow_domains: list[str] | None = None,
        allow_cidrs: list[str] | None = None,
        intercept_https: bool = False,
        allowed_ports: list[int] | None = None,
        http_rules: list[EgressHttpRule] | None = None,
        workspace_id: str | None = None,
    ) -> EgressPolicy:
        """Save a policy under ``name``; the server checks it like an inline sandbox egress policy."""
        req = build_create_request(name, workspace_id, allow_domains, allow_cidrs, intercept_https, http_rules, allowed_ports)
        try:
            return egress_policy(self._rpc.create_egress_policy(req).policy)
        except Exception as err:
            raise map_rpc_error(err) from err

    def update(
        self,
        name_or_id: str,
        *,
        expected_revision: int,
        allow_domains: list[str] | None = None,
        allow_cidrs: list[str] | None = None,
        intercept_https: bool = False,
        allowed_ports: list[int] | None = None,
        http_rules: list[EgressHttpRule] | None = None,
        workspace_id: str | None = None,
    ) -> EgressPolicy:
        """Replace the policy body; existing sandboxes keep the policy they were created with."""
        req = build_update_request(
            name_or_id, expected_revision, workspace_id, allow_domains, allow_cidrs, intercept_https, http_rules, allowed_ports
        )
        try:
            return egress_policy(self._rpc.update_egress_policy(req).policy)
        except Exception as err:
            raise map_rpc_error(err) from err

    def get(self, name_or_id: str, *, workspace_id: str | None = None) -> EgressPolicy:
        req = pb.GetEgressPolicyRequest(policy_id=name_or_id, **_workspace(workspace_id))
        try:
            return egress_policy(self._rpc.get_egress_policy(req).policy)
        except Exception as err:
            raise map_rpc_error(err) from err

    def list(self, *, workspace_id: str | None = None) -> list[EgressPolicy]:
        """Return every live policy in the workspace, ordered by name."""
        try:
            policies: list[EgressPolicy] = []
            cursor = ""
            while True:
                req = pb.ListEgressPoliciesRequest(page_size=100, cursor=cursor, **_workspace(workspace_id))
                resp = self._rpc.list_egress_policies(req)
                policies.extend(egress_policy(item) for item in resp.policies)
                cursor = advance_page_token(cursor, resp.next_cursor)
                if not cursor:
                    return policies
        except Exception as err:
            raise map_rpc_error(err) from err

    def delete(self, name_or_id: str, *, expected_revision: int, workspace_id: str | None = None) -> EgressPolicy:
        """Soft-delete the policy; sandboxes that pinned it keep their merged policy."""
        req = pb.DeleteEgressPolicyRequest(
            policy_id=name_or_id, expected_revision=expected_revision, **_workspace(workspace_id)
        )
        try:
            return egress_policy(self._rpc.delete_egress_policy(req).policy)
        except Exception as err:
            raise map_rpc_error(err) from err


class AsyncEgressPolicies:
    """Async counterpart of ``EgressPolicies``."""

    def __init__(self, rpc) -> None:
        self._rpc = rpc

    async def create(
        self,
        name: str,
        *,
        allow_domains: list[str] | None = None,
        allow_cidrs: list[str] | None = None,
        intercept_https: bool = False,
        allowed_ports: list[int] | None = None,
        http_rules: list[EgressHttpRule] | None = None,
        workspace_id: str | None = None,
    ) -> EgressPolicy:
        req = build_create_request(name, workspace_id, allow_domains, allow_cidrs, intercept_https, http_rules, allowed_ports)
        try:
            return egress_policy((await self._rpc.create_egress_policy(req)).policy)
        except Exception as err:
            raise map_rpc_error(err) from err

    async def update(
        self,
        name_or_id: str,
        *,
        expected_revision: int,
        allow_domains: list[str] | None = None,
        allow_cidrs: list[str] | None = None,
        intercept_https: bool = False,
        allowed_ports: list[int] | None = None,
        http_rules: list[EgressHttpRule] | None = None,
        workspace_id: str | None = None,
    ) -> EgressPolicy:
        req = build_update_request(
            name_or_id, expected_revision, workspace_id, allow_domains, allow_cidrs, intercept_https, http_rules, allowed_ports
        )
        try:
            return egress_policy((await self._rpc.update_egress_policy(req)).policy)
        except Exception as err:
            raise map_rpc_error(err) from err

    async def get(self, name_or_id: str, *, workspace_id: str | None = None) -> EgressPolicy:
        req = pb.GetEgressPolicyRequest(policy_id=name_or_id, **_workspace(workspace_id))
        try:
            return egress_policy((await self._rpc.get_egress_policy(req)).policy)
        except Exception as err:
            raise map_rpc_error(err) from err

    async def list(self, *, workspace_id: str | None = None) -> list[EgressPolicy]:
        try:
            policies: list[EgressPolicy] = []
            cursor = ""
            while True:
                req = pb.ListEgressPoliciesRequest(page_size=100, cursor=cursor, **_workspace(workspace_id))
                resp = await self._rpc.list_egress_policies(req)
                policies.extend(egress_policy(item) for item in resp.policies)
                cursor = advance_page_token(cursor, resp.next_cursor)
                if not cursor:
                    return policies
        except Exception as err:
            raise map_rpc_error(err) from err

    async def delete(self, name_or_id: str, *, expected_revision: int, workspace_id: str | None = None) -> EgressPolicy:
        req = pb.DeleteEgressPolicyRequest(
            policy_id=name_or_id, expected_revision=expected_revision, **_workspace(workspace_id)
        )
        try:
            return egress_policy((await self._rpc.delete_egress_policy(req)).policy)
        except Exception as err:
            raise map_rpc_error(err) from err
