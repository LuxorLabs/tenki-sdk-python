from __future__ import annotations

import pytest

import tenki_sandbox
from tenki_sandbox import (
    Client,
    InvalidAuthTokenError,
    InvalidResourceConfigError,
    MissingAuthTokenError,
    Sandbox,
    TemplateRuntimeFailedError,
    UnauthorizedError,
    WaitReadyFailedError,
)
from tenki_sandbox._pb import pb, template_pb
from tenki_sandbox._transport import (
    GrpcConfig,
    GrpcSandboxRPC,
    auth_metadata,
    grpc_target,
    resolve_gateway_url,
)
from tenki_sandbox.client import _create_wait_timeout
from tenki_sandbox.models import VolumeMount


def test_client_requires_auth_without_injected_rpc(monkeypatch):
    monkeypatch.delenv("TENKI_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("TENKI_API_KEY", raising=False)

    with pytest.raises(MissingAuthTokenError):
        Client()


@pytest.mark.parametrize("auth_token", ["ory_st_test", "cookie:session-token"])
@pytest.mark.parametrize("rpc", [None, object()])
def test_client_rejects_invalid_auth_tokens(auth_token, rpc):
    with pytest.raises(InvalidAuthTokenError):
        Client(auth_token=auth_token, rpc=rpc)


def test_invalid_auth_token_is_unauthorized_error():
    assert issubclass(InvalidAuthTokenError, UnauthorizedError)


def test_auth_metadata_and_grpc_target_are_provider_friendly():
    client = (
        ("x-tenki-client-family", "python_sdk"),
        ("x-tenki-client-generation", "workspace_v1"),
    )
    assert auth_metadata(" tk_test ") == (("authorization", "Bearer tk_test"),) + client
    assert grpc_target("https://api.tenki.cloud") == ("api.tenki.cloud:443", True)
    assert grpc_target("http://localhost:8090") == ("localhost:8090", False)
    assert grpc_target("localhost:8090") == ("localhost:8090", True)


def test_sync_transport_starts_channel_connection(monkeypatch):
    import grpc

    from tenki_sandbox._internal.proto.tenki.sandbox.v1 import sandbox_pb2_grpc

    channel = object()

    class ReadyFuture:
        cancelled = False

        def cancel(self):
            self.cancelled = True

    ready_future = ReadyFuture()
    monkeypatch.setattr(grpc, "insecure_channel", lambda _target: channel)
    monkeypatch.setattr(grpc, "channel_ready_future", lambda candidate: ready_future)
    monkeypatch.setattr(sandbox_pb2_grpc, "SandboxServiceStub", lambda candidate: object())

    rpc = GrpcSandboxRPC(
        GrpcConfig(base_url="http://localhost:8090", auth_token="tk_test")
    )

    assert rpc._channel is channel
    assert rpc._ready_future is ready_future
    rpc.close()
    assert ready_future.cancelled is True


def test_gateway_url_derivation_preserves_port_and_overrides():
    assert (
        resolve_gateway_url(None, "https://api.tenki.cloud")
        == "wss://sandbox-gateway.tenki.cloud"
    )
    assert (
        resolve_gateway_url(None, "https://api.tenki.lab:4443")
        == "wss://sandbox-gateway.tenki.lab:4443"
    )
    assert (
        resolve_gateway_url(None, "http://localhost:8090")
        == "ws://sandbox-gateway.localhost:8090"
    )
    assert (
        resolve_gateway_url(None, "localhost:8090")
        == "wss://sandbox-gateway.localhost:8090"
    )
    assert (
        resolve_gateway_url("ws://gateway.example", "http://localhost:8090")
        == "ws://gateway.example"
    )


def test_create_maps_pythonic_options_without_wait():
    rpc = FakeRPC()
    client = Client(rpc=rpc)

    sandbox = client.create(
        wait=False,
        name="job",
        workspace_id="ws-1",
        cpu_cores=4,
        memory_mb=8192,
        disk_size_gb=20,
        env={"A": "B"},
        metadata={"purpose": "test"},
        tags=["Nightly", " gpu "],
        github_token="ghp_test",
        image="pub/polyglot:prod",
        wait_for_runtime=True,
        volumes=[
            {"volume_id": "vol-1", "mount_path": "/workspace/cache", "read_only": True}
        ],
    )

    assert isinstance(sandbox, Sandbox)
    req = rpc.requests["create_session"][-1]
    assert req.name == "job"
    assert req.workspace_id == "ws-1"
    assert req.cpu_cores == 4
    assert req.env["A"] == "B"
    assert req.env["GH_TOKEN"] == "ghp_test"
    assert list(req.tags) == ["nightly", "gpu"]
    assert req.registry_ref == "pub/polyglot:prod"
    assert req.wait_ready is False
    assert req.wait_for_runtime is True
    assert req.volumes[0].readonly is True


def test_create_infers_workspace_from_api_key():
    rpc = FakeRPC()
    client = Client(rpc=rpc)

    client.create(wait=False)

    assert rpc.requests["create_session"][-1].workspace_id == ""


def test_list_drains_every_page():
    rpc = FakeRPC()
    client = Client(rpc=rpc)

    sandboxes = client.list()

    assert [sandbox.id for sandbox in sandboxes] == ["session-1", "session-2"]
    assert [req.page_token for req in rpc.requests["list_workspace_sandboxes"]] == [
        "",
        "next",
    ]


def test_create_preserves_outbound_policy_presence():
    rpc = FakeRPC()
    client = Client(rpc=rpc)

    client.create(workspace_id="ws-1", wait=False, allow_outbound=False)
    disabled = rpc.requests["create_session"][-1]
    assert disabled.HasField("allow_outbound")
    assert disabled.allow_outbound is False
    assert pb.CreateSessionRequest(allow_outbound=False).SerializeToString() == b"\x20\x00"

    client.create(workspace_id="ws-1", wait=False)
    defaulted = rpc.requests["create_session"][-1]
    assert defaulted.HasField("allow_outbound")
    assert defaulted.allow_outbound is True

    assert not pb.CreateSessionRequest().HasField("allow_outbound")


def test_create_preserves_inbound_policy_presence():
    rpc = FakeRPC()
    client = Client(rpc=rpc)

    client.create(workspace_id="ws-1", wait=False, allow_inbound=False)
    disabled = rpc.requests["create_session"][-1]
    assert disabled.HasField("allow_inbound")
    assert disabled.allow_inbound is False
    assert pb.CreateSessionRequest(allow_inbound=False).SerializeToString() == b"\x18\x00"

    client.create(workspace_id="ws-1", wait=False)
    defaulted = rpc.requests["create_session"][-1]
    assert defaulted.HasField("allow_inbound")
    assert defaulted.allow_inbound is True

    assert not pb.CreateSessionRequest().HasField("allow_inbound")


def test_create_rejects_multiple_restore_sources():
    client = Client(rpc=FakeRPC())

    with pytest.raises(
        ValueError,
        match="only one of image, snapshot_id, or from_template_spec may be set",
    ):
        client.create(
            workspace_id="ws-1",
            wait=False,
            snapshot_id="snap-1",
            image="pub/polyglot:prod",
        )


def test_create_from_template_spec_maps_setup_inputs():
    rpc = FakeRPC()
    client = Client(rpc=rpc)

    client.create(
        workspace_id="ws-1",
        wait=False,
        from_template_spec="8d995c1c-8e24-4b3a-a8ab-a00316357385",
        setup_env={"MODE": "dev"},
        setup_secrets={"GH_TOKEN": "secret"},
    )

    req = rpc.requests["create_session"][-1]
    assert req.template_spec_id == "8d995c1c-8e24-4b3a-a8ab-a00316357385"
    assert dict(req.setup_env) == {"MODE": "dev"}
    assert dict(req.setup_secrets) == {"GH_TOKEN": "secret"}
    assert not req.HasField("cpu_cores")
    assert not req.HasField("memory_mb")
    assert not req.HasField("disk_size_gb")


def test_create_from_template_spec_uses_long_default_wait_timeout():
    template_id = "8d995c1c-8e24-4b3a-a8ab-a00316357385"
    assert _create_wait_timeout(None, template_id) == 7200
    assert _create_wait_timeout(None, None) == 180
    assert _create_wait_timeout(60, template_id) == 60


def test_create_rejects_invalid_resources():
    client = Client(rpc=FakeRPC())

    client.create(workspace_id="ws-1", wait=False, cpu_cores=2, memory_mb=4096)

    with pytest.raises(
        InvalidResourceConfigError, match="cpu_cores must be between 1 and 16"
    ):
        client.create(workspace_id="ws-1", wait=False, cpu_cores=17)

    with pytest.raises(
        InvalidResourceConfigError, match="memory_mb must be aligned to 2 MiB"
    ):
        client.create(workspace_id="ws-1", wait=False, memory_mb=8089)

    with pytest.raises(
        InvalidResourceConfigError, match="disk_size_gb must be between 5 and 100"
    ):
        client.create(workspace_id="ws-1", wait=False, disk_size_gb=3)


def test_sandbox_context_closes_unless_detached():
    rpc = FakeRPC()
    client = Client(rpc=rpc)

    with client.create(workspace_id="ws-1", wait=False) as sandbox:
        assert sandbox.id == "session-1"

    assert rpc.requests["terminate_session"][-1].session_id == "session-1"

    with client.create(workspace_id="ws-1", wait=False) as sandbox:
        sandbox.detach()

    assert len(rpc.requests["terminate_session"]) == 1


def test_pause_and_resume_reset_data_plane():
    rpc = FakeRPC()
    client = Client(rpc=rpc)
    sandbox = client.create(workspace_id="ws-1", wait=False)

    paused_credential, paused_rpc = _prime_data_plane(sandbox)
    sandbox.pause()
    assert sandbox.state == "PAUSED"
    assert paused_credential.stopped is True
    assert paused_rpc.closed is True
    assert sandbox._data_plane_credentials is None
    assert sandbox._data_plane_rpc is None
    assert sandbox._data_plane_verified is False

    resumed_credential, resumed_rpc = _prime_data_plane(sandbox)
    sandbox.resume()
    assert sandbox.state == "RUNNING"
    assert resumed_credential.stopped is True
    assert resumed_rpc.closed is True
    assert sandbox._data_plane_credentials is None
    assert sandbox._data_plane_rpc is None
    assert sandbox._data_plane_verified is False


def test_create_wait_failure_preserves_closable_sandbox():
    rpc = FakeRPC()
    rpc.wait_states = [pb.SESSION_STATE_CREATING]

    with pytest.raises(WaitReadyFailedError) as exc_info:
        Client(rpc=rpc).create(workspace_id="ws-1", wait=True, timeout=0)

    assert exc_info.value.sandbox.id == "session-1"
    exc_info.value.sandbox.close()
    assert rpc.requests["terminate_session"][-1].session_id == "session-1"


def test_create_terminal_wait_failure_is_not_wrapped():
    rpc = FakeRPC()
    rpc.wait_states = [pb.SESSION_STATE_TERMINATED]

    with pytest.raises(RuntimeError, match="terminal state") as exc_info:
        Client(rpc=rpc).create(workspace_id="ws-1", wait=True, timeout=0)

    assert not isinstance(exc_info.value, WaitReadyFailedError)


def test_wait_ready_uses_stream_until_running():
    rpc = FakeRPC()
    rpc.wait_states = [pb.SESSION_STATE_CREATING, pb.SESSION_STATE_RUNNING]
    client = Client(rpc=rpc)

    sandbox = client.create(workspace_id="ws-1", wait=True, timeout=2)

    assert sandbox.state == "RUNNING"
    assert rpc.requests["create_session"][-1].wait_ready is False
    assert len(rpc.requests["wait_session"]) == 1
    assert len(rpc.requests["get_session"]) == 0


def test_runtime_wait_keeps_server_held_create():
    rpc = FakeRPC()

    Client(rpc=rpc).create(workspace_id="ws-1", wait=True, wait_for_runtime=True, timeout=2)

    req = rpc.requests["create_session"][-1]
    assert req.wait_ready is True
    assert req.wait_for_runtime is True


def test_wait_ready_primes_data_plane_from_stream():
    rpc = FakeRPC()
    rpc.wait_data_plane_endpoint = "http://data-plane.test"
    rpc.wait_data_plane_credential = pb.SessionCredential(
        credential="wait-session-credential"
    )
    rpc.wait_route_status = pb.DATA_PLANE_ROUTE_STATUS_VERIFIED
    sandbox = Client(rpc=rpc).create(workspace_id="ws-1", wait=True, timeout=2)

    assert sandbox._data_plane_rpc is not None
    assert sandbox._data_plane_credentials.current() == "wait-session-credential"
    assert sandbox._data_plane_verified is True


def test_wait_ready_falls_back_to_poll_when_stream_unimplemented():
    rpc = FakeRPC()
    rpc.wait_unimplemented = True
    rpc.get_states = [pb.SESSION_STATE_RUNNING]
    sandbox = Client(rpc=rpc).create(workspace_id="ws-1", wait=False)

    sandbox.wait_ready(timeout=0)

    assert sandbox.state == "RUNNING"
    assert len(rpc.requests["wait_session"]) == 1
    assert len(rpc.requests["get_session"]) == 1


def test_wait_ready_stream_preserves_terminal_state_error_type():
    rpc = FakeRPC()
    rpc.wait_states = [pb.SESSION_STATE_TERMINATING]
    sandbox = Client(rpc=rpc).create(workspace_id="ws-1", wait=False)

    with pytest.raises(RuntimeError, match="terminal state"):
        sandbox.wait_ready(timeout=1)


def test_public_error_exports_are_importable():
    for name in [
        "VolumeNotFoundError",
        "VolumeInUseError",
        "VolumeSyncPendingError",
        "SnapshotNotFoundError",
        "TemplateNotFoundError",
        "TemplateExistsError",
        "TemplateBuildFailedError",
        "TemplateBuildInProgressError",
        "TemplateRuntimeFailedError",
        "WaitReadyFailedError",
        "InvalidStateError",
        "InvalidAuthTokenError",
        "StreamClosedError",
        "CapabilityUnavailableError",
        "PrimitiveTimeoutError",
    ]:
        assert getattr(tenki_sandbox, name)


def test_template_runtime_failure_preserves_running_sandbox():
    from google.protobuf.any_pb2 import Any

    from tenki_sandbox.errors import _grpc_status_message_class, map_rpc_error

    failure = pb.TemplateRuntimeFailure(
        session=pb.SandboxSession(
            id="session-1",
            state=pb.SESSION_STATE_RUNNING,
            runtime_state=template_pb.TEMPLATE_RUNTIME_STATE_FAILED,
        ),
        reason="runtime readiness deadline exceeded",
    )
    packed = Any()
    packed.Pack(failure)
    status = _grpc_status_message_class()(
        code=9,
        message="template runtime failed",
        details=[packed],
    )
    err = FakeRuntimeError(status.SerializeToString())

    mapped = map_rpc_error(err)
    assert isinstance(mapped, TemplateRuntimeFailedError)
    assert mapped.reason == "runtime readiness deadline exceeded"
    assert mapped.sandbox.id == "session-1"
    assert mapped.sandbox.state == "RUNNING"
    assert mapped.sandbox.runtime_state == "FAILED"


def test_volume_attach_detach_updates_frozen_info():
    rpc = FakeRPC()
    sandbox = Client(rpc=rpc).create(workspace_id="ws-1", wait=False)

    sandbox.attach_volume("vol-1", "/workspace/cache", read_only=True)
    assert sandbox.info.volume_mounts == (
        VolumeMount(volume_id="vol-1", mount_path="/workspace/cache", read_only=True),
    )

    sandbox.detach_volume("vol-1", wait=0)
    assert sandbox.info.volume_mounts == ()
    assert rpc.requests["attach_volume"][-1].volume.volume_id == "vol-1"
    assert rpc.requests["detach_volume"][-1].volume_id == "vol-1"


class FakeRPCError(Exception):
    def __init__(self, code_name: str):
        super().__init__(code_name)
        self._code_name = code_name

    def code(self):
        return type("Code", (), {"name": self._code_name})()


class FakeRuntimeError(Exception):
    def __init__(self, status: bytes):
        super().__init__("template runtime failed")
        self._status = status

    def code(self):
        return type("Code", (), {"name": "FAILED_PRECONDITION"})()

    def details(self):
        return "template runtime failed"

    def trailing_metadata(self):
        return (("grpc-status-details-bin", self._status),)


class FakeDataPlaneCredential:
    stopped = False

    def stop(self):
        self.stopped = True


class FakeDataPlaneRPC:
    closed = False

    def close(self):
        self.closed = True


def _prime_data_plane(sandbox):
    credential = FakeDataPlaneCredential()
    rpc = FakeDataPlaneRPC()
    sandbox._data_plane_credentials = credential
    sandbox._data_plane_rpc = rpc
    sandbox._data_plane_verified = True
    return credential, rpc


class FakeRPC:
    def __init__(self):
        self.requests = {
            "create_session": [],
            "get_session": [],
            "wait_session": [],
            "terminate_session": [],
            "pause_session": [],
            "resume_session": [],
            "attach_volume": [],
            "detach_volume": [],
            "list_workspace_sandboxes": [],
        }
        self.get_states = [pb.SESSION_STATE_RUNNING]
        self.wait_states = [pb.SESSION_STATE_RUNNING]
        self.wait_unimplemented = False
        self.wait_data_plane_endpoint = ""
        self.wait_data_plane_credential = None
        self.wait_route_status = pb.DATA_PLANE_ROUTE_STATUS_UNSPECIFIED

    def create_session(self, req):
        self.requests["create_session"].append(req)
        return pb.CreateSessionResponse(session=_session(pb.SESSION_STATE_CREATING))

    def get_session(self, req):
        self.requests["get_session"].append(req)
        state = self.get_states.pop(0) if self.get_states else pb.SESSION_STATE_RUNNING
        return pb.GetSessionResponse(session=_session(state))

    def list_workspace_sandboxes(self, req):
        self.requests["list_workspace_sandboxes"].append(req)
        if req.page_token == "next":
            return pb.ListWorkspaceSandboxesResponse(
                sessions=[_session(sid="session-2")]
            )
        return pb.ListWorkspaceSandboxesResponse(
            sessions=[_session()], next_page_token="next"
        )

    def wait_session(self, req, **kwargs):
        self.requests["wait_session"].append(req)
        if self.wait_unimplemented:
            raise FakeRPCError("UNIMPLEMENTED")
        for state in self.wait_states:
            yield pb.WaitSessionResponse(
                session=_session(state),
                data_plane_endpoint=(
                    self.wait_data_plane_endpoint
                    if state == pb.SESSION_STATE_RUNNING
                    else ""
                ),
                credential=(
                    self.wait_data_plane_credential
                    if state == pb.SESSION_STATE_RUNNING
                    else None
                ),
                route_status=(
                    self.wait_route_status
                    if state == pb.SESSION_STATE_RUNNING
                    else pb.DATA_PLANE_ROUTE_STATUS_UNSPECIFIED
                ),
            )

    def terminate_session(self, req):
        self.requests["terminate_session"].append(req)
        return pb.TerminateSessionResponse(
            session=_session(pb.SESSION_STATE_TERMINATED)
        )

    def pause_session(self, req):
        self.requests["pause_session"].append(req)
        return pb.PauseSessionResponse(session=_session(pb.SESSION_STATE_PAUSED))

    def resume_session(self, req):
        self.requests["resume_session"].append(req)
        return pb.ResumeSessionResponse(session=_session(pb.SESSION_STATE_RUNNING))

    def attach_volume(self, req):
        self.requests["attach_volume"].append(req)
        return pb.AttachVolumeResponse()

    def detach_volume(self, req):
        self.requests["detach_volume"].append(req)
        return pb.DetachVolumeResponse()


def _session(state=pb.SESSION_STATE_RUNNING, sid="session-1"):
    return pb.SandboxSession(
        id=sid,
        state=state,
        owner_type="SERVICE",
        owner_id="self",
        name="test",
        workspace_id="ws-1",
    )
