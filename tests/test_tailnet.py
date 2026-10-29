from __future__ import annotations

import hashlib
from typing import cast

import pytest

from tenki_sandbox import (
    AsyncClient,
    Client,
    TailnetAttachment,
    TailnetAuthKey,
    TailnetProvider,
    TailnetStatus,
)
from tenki_sandbox._convert import sandbox_info
from tenki_sandbox._pb import pb
from tenki_sandbox.client import build_create_session_request


def attachment() -> TailnetAttachment:
    return TailnetAttachment(
        auth_key=TailnetAuthKey("tskey-auth-secret"),
        hostname="sandbox-1",
        tags=("tag:sandbox",),
        ephemeral=True,
        control_url="https://control.example.com",
        exit_node="auto",
        accept_routes=True,
        wake_on_connect=True,
        expose_ports=(3000, 8080),
        wait_for_online=True,
        exit_policy="exit_node_managed",
        ephemeral_pause_policy="recreate_on_resume",
    )


def response() -> pb.CreateSessionResponse:
    return pb.CreateSessionResponse(
        session=pb.SandboxSession(
            id="session-1",
            state=pb.SESSION_STATE_RUNNING,
            tailnet_status=pb.TailnetStatus(
                provider="tailscale",
                state="online",
                node_id="node-1",
                fqdn="sandbox.example.ts.net",
                ips=["100.64.0.1", "fd7a:115c:a1e0::1"],
                tags=["tag:sandbox"],
                owner="tag:sandbox",
                device_retained=True,
            ),
        )
    )


def assert_tailnet_request(req: pb.CreateSessionRequest) -> None:
    assert req.HasField("tailnet")
    assert req.tailnet.provider == "tailscale"
    assert req.tailnet.hostname == "sandbox-1"
    assert list(req.tailnet.tags) == ["tag:sandbox"]
    assert req.tailnet.ephemeral is True
    assert req.tailnet.control_url == "https://control.example.com"
    assert req.tailnet.exit_node == "auto"
    assert req.tailnet.accept_routes is True
    assert req.tailnet.wake_on_connect is True
    assert list(req.tailnet.expose_ports) == [3000, 8080]
    assert req.tailnet.wait_for_online is True
    assert hashlib.sha256(req.tailnet.auth_key.encode()).digest() == hashlib.sha256(
        b"tskey-auth-secret"
    ).digest()
    assert req.tailnet.exit_policy == pb.TAILNET_EXIT_POLICY_EXIT_NODE_MANAGED
    assert (
        req.tailnet.ephemeral_pause_policy
        == pb.TAILNET_EPHEMERAL_PAUSE_POLICY_RECREATE_ON_RESUME
    )


def test_tailnet_request_and_status_conversion():
    assert_tailnet_request(build_create_session_request(tailnet=attachment()))

    status = sandbox_info(response().session).tailnet_status
    assert status is not None
    assert status.provider == "tailscale"
    assert status.state == "online"
    assert status.node_id == "node-1"
    assert status.fqdn == "sandbox.example.ts.net"
    assert status.ips == ("100.64.0.1", "fd7a:115c:a1e0::1")
    assert status.tags == ("tag:sandbox",)
    assert status.owner == "tag:sandbox"
    assert status.error == ""
    assert status.device_retained is True


def test_workspace_default_opt_out():
    request = build_create_session_request(disable_tailnet=True)
    assert request.disable_tailnet is True
    assert not request.HasField("tailnet")
    inherited = build_create_session_request()
    assert inherited.disable_tailnet is False
    assert not inherited.HasField("tailnet")
    with pytest.raises(ValueError, match="choose a Tailscale connection"):
        build_create_session_request(disable_tailnet=True, tailnet=attachment())


def test_tailnet_auth_key_repr_is_redacted():
    key = TailnetAuthKey("tskey-auth-secret")
    rendered = f"{key!s} {key!r} {TailnetAttachment(auth_key=key)!r}"
    assert "tskey-auth-secret" not in rendered
    assert "REDACTED" in rendered

    req = build_create_session_request(tailnet=TailnetAttachment(auth_key=key))
    assert req.tailnet.exit_policy == pb.TAILNET_EXIT_POLICY_UNSPECIFIED
    assert req.tailnet.ephemeral_pause_policy == pb.TAILNET_EPHEMERAL_PAUSE_POLICY_UNSPECIFIED


def test_tailnet_rejects_unsupported_provider_before_credential_read():
    with pytest.raises(ValueError, match="unsupported tailnet provider"):
        build_create_session_request(
            tailnet=TailnetAttachment(
                auth_key=TailnetAuthKey("sentinel"), provider=cast(TailnetProvider, "other")
            )
        )


def test_legacy_status_defaults_provider():
    status = sandbox_info(pb.SandboxSession(tailnet_status=pb.TailnetStatus(state="joining")))
    assert status.tailnet_status is not None
    assert status.tailnet_status.provider == "tailscale"


def test_future_status_provider_is_preserved():
    status = sandbox_info(
        pb.SandboxSession(tailnet_status=pb.TailnetStatus(provider="future", state="online"))
    )
    assert status.tailnet_status is not None
    assert status.tailnet_status.provider == "future"


def test_tailnet_positional_constructor_compatibility():
    key = TailnetAuthKey("sentinel")
    attachment = TailnetAttachment(key, "sandbox-1")
    status = TailnetStatus("online", "node-1")
    assert attachment.hostname == "sandbox-1"
    assert attachment.provider == "tailscale"
    assert status.node_id == "node-1"
    assert status.provider == "tailscale"


class SyncRPC:
    def __init__(self) -> None:
        self.request = None

    def create_session(self, req):
        self.request = req
        return response()


class AsyncRPC:
    def __init__(self) -> None:
        self.request = None

    async def create_session(self, req):
        self.request = req
        return response()


def test_sync_client_exposes_tailnet_status():
    rpc = SyncRPC()
    sandbox = Client(rpc=rpc).create(wait=False, tailnet=attachment())

    assert_tailnet_request(rpc.request)
    assert sandbox.tailnet_status == sandbox.info.tailnet_status


@pytest.mark.asyncio
async def test_async_client_uses_shared_tailnet_mapping():
    rpc = AsyncRPC()
    sandbox = await AsyncClient(rpc=rpc).create(wait=False, tailnet=attachment())

    assert_tailnet_request(rpc.request)
    assert sandbox.tailnet_status == sandbox.info.tailnet_status


def test_saved_profile_reference_and_credential_exclusivity():
    from tenki_sandbox.client import _tailnet_attachment_proto

    profile_id = "11111111-1111-4111-8111-111111111111"
    request = _tailnet_attachment_proto(TailnetAttachment(profile_id=profile_id, profile_revision=3))
    assert request.WhichOneof("credential") == "profile_id"
    assert request.profile_id == profile_id
    assert request.profile_revision == 3
    assert request.auth_key == ""
    with pytest.raises(ValueError, match="one tailnet credential"):
        _tailnet_attachment_proto(TailnetAttachment(auth_key=TailnetAuthKey("sentinel"), profile_id=profile_id))


@pytest.mark.asyncio
async def test_saved_profile_reference_reaches_async_client():
    rpc = AsyncRPC()
    profile_id = "11111111-1111-4111-8111-111111111111"
    await AsyncClient(rpc=rpc).create(
        wait=False,
        tailnet=TailnetAttachment(profile_id=profile_id, profile_revision=3),
    )
    assert rpc.request.tailnet.WhichOneof("credential") == "profile_id"
    assert rpc.request.tailnet.profile_id == profile_id
    assert rpc.request.tailnet.profile_revision == 3
    assert rpc.request.tailnet.auth_key == ""
