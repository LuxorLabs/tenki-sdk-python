from __future__ import annotations

import asyncio
import inspect

from tenki_sandbox._pb import pb, ssh_pb
from tenki_sandbox._transport import GrpcConfig
from tenki_sandbox.aio import _transport as aio_transport
from tenki_sandbox.aio._transport import AsyncGrpcSandboxRPC


class _FakeStub:
    def __init__(self):
        self.calls = []

    async def CreateSession(self, request, metadata=None, timeout=None):
        self.calls.append(("CreateSession", metadata, timeout))
        return pb.CreateSessionResponse()

    def WaitSession(self, request, metadata=None, timeout=None):
        self.calls.append(("WaitSession", metadata, timeout))

        async def gen():
            yield pb.WaitSessionResponse()

        return gen()


def _rpc():
    rpc = AsyncGrpcSandboxRPC(
        GrpcConfig(base_url="http://localhost:8090", auth_token="tk_test")
    )
    rpc._stub = _FakeStub()  # bypass real grpc.aio channel creation
    return rpc


async def test_async_transport_starts_channel_connection(monkeypatch):
    import grpc

    from tenki_sandbox._internal.proto.tenki.sandbox.v1 import sandbox_pb2_grpc

    class Channel:
        connecting = False
        closed = False

        def get_state(self, try_to_connect=False):
            self.connecting = try_to_connect

        async def close(self, _grace):
            self.closed = True

    channel = Channel()
    monkeypatch.setattr(grpc.aio, "insecure_channel", lambda _target: channel)
    monkeypatch.setattr(sandbox_pb2_grpc, "SandboxServiceStub", lambda candidate: object())

    rpc = AsyncGrpcSandboxRPC(
        GrpcConfig(base_url="http://localhost:8090", auth_token="tk_test")
    )

    assert rpc._channel is channel
    assert channel.connecting is True
    await rpc.close()
    assert channel.closed is True


async def test_unary_method_is_coroutine_and_passes_auth_metadata():
    rpc = _rpc()
    assert inspect.iscoroutinefunction(rpc.create_session)
    resp = await rpc.create_session(pb.CreateSessionRequest())
    assert isinstance(resp, pb.CreateSessionResponse)
    name, metadata, timeout = rpc._stub.calls[-1]
    assert ("authorization", "Bearer tk_test") in metadata


def test_streaming_method_returns_async_iterator_not_coroutine():
    rpc = _rpc()
    assert not inspect.iscoroutinefunction(rpc.wait_session)
    call = rpc.wait_session(pb.WaitSessionRequest())
    assert hasattr(call, "__aiter__")


async def test_streaming_method_iterates():
    rpc = _rpc()
    items = [item async for item in rpc.wait_session(pb.WaitSessionRequest())]
    assert len(items) == 1 and isinstance(items[0], pb.WaitSessionResponse)


async def test_streaming_rpc_does_not_inherit_client_timeout():
    # A client-wide timeout is a unary deadline; long-lived streams must not get
    # it or a persistent tunnel dies with DEADLINE_EXCEEDED after `timeout`.
    rpc = AsyncGrpcSandboxRPC(
        GrpcConfig(base_url="http://localhost:8090", auth_token="tk_test", timeout=30)
    )
    rpc._stub = _FakeStub()

    await rpc.create_session(pb.CreateSessionRequest())  # unary keeps the deadline
    assert rpc._stub.calls[-1][2] == 30

    rpc.wait_session(pb.WaitSessionRequest())  # stream: no default deadline
    assert rpc._stub.calls[-1][2] is None

    rpc.wait_session(pb.WaitSessionRequest(), timeout=5)  # explicit still honored
    assert rpc._stub.calls[-1][2] == 5


async def test_ssh_method_uses_connect_shim_via_executor(monkeypatch):
    seen = {}

    def fake_connect(http_base, metadata, service, rpc_name, request, *, timeout=None):
        seen["service"] = service
        seen["rpc_name"] = rpc_name
        seen["http_base"] = http_base
        return ssh_pb.IssueSandboxSSHCertResponse(ssh_cert="CERT")

    monkeypatch.setattr(aio_transport, "connect_unary_blocking", fake_connect)
    rpc = _rpc()
    assert inspect.iscoroutinefunction(rpc.issue_ssh_cert)
    resp = await rpc.issue_ssh_cert(ssh_pb.IssueSandboxSSHCertRequest())
    assert resp.ssh_cert == "CERT"
    assert seen["rpc_name"] == "IssueSandboxSSHCert"
    assert seen["service"] == AsyncGrpcSandboxRPC._SSH_SERVICE
    assert seen["http_base"] == "http://localhost:8090"


# --------------------------------------------------- credential renewal retries

import grpc  # noqa: E402
import pytest  # noqa: E402

from tenki_sandbox.aio.data_plane import (  # noqa: E402
    AsyncDataPlaneCredentialManager,
    AsyncGrpcDataPlaneRPC,
    _credential_error_is_retryable,
)
from tenki_sandbox.errors import PermissionDeniedError  # noqa: E402


class _FakeAio(grpc.aio.AioRpcError):
    def __init__(self, code, details=""):
        self._c = code
        self._details = details

    def code(self):
        return self._c

    def details(self):
        return self._details


def test_credential_error_retryable_classification():
    # FAILED_PRECONDITION (still provisioning) and non-gRPC transients retry;
    # permanent gRPC statuses do not.
    assert (
        _credential_error_is_retryable(_FakeAio(grpc.StatusCode.FAILED_PRECONDITION))
        is True
    )
    assert (
        _credential_error_is_retryable(
            _FakeAio(
                grpc.StatusCode.FAILED_PRECONDITION,
                "session not RUNNING (state=CREATING)",
            )
        )
        is True
    )
    assert (
        _credential_error_is_retryable(
            _FakeAio(
                grpc.StatusCode.FAILED_PRECONDITION,
                "session not RUNNING (state=RESUMING)",
            )
        )
        is True
    )
    assert (
        _credential_error_is_retryable(
            _FakeAio(
                grpc.StatusCode.FAILED_PRECONDITION,
                "session not RUNNING (state=PAUSED)",
            )
        )
        is False
    )
    assert (
        _credential_error_is_retryable(_FakeAio(grpc.StatusCode.PERMISSION_DENIED))
        is False
    )
    assert (
        _credential_error_is_retryable(_FakeAio(grpc.StatusCode.UNAUTHENTICATED))
        is False
    )
    assert _credential_error_is_retryable(ValueError("non-grpc")) is True
    mapped = PermissionDeniedError("revoked")
    mapped.__cause__ = _FakeAio(grpc.StatusCode.PERMISSION_DENIED)
    assert _credential_error_is_retryable(mapped) is False


async def test_async_data_plane_channels_are_shared_by_origin(monkeypatch):
    from tenki_sandbox._internal.proto.tenki.sandbox.v1 import sandbox_pb2_grpc

    class Channel:
        closed = False

        async def close(self, _grace):
            self.closed = True

    class Credentials:
        def current(self):
            return "credential"

    created = []

    def new_channel(target):
        channel = Channel()
        created.append((target, channel))
        return channel

    monkeypatch.setattr(grpc.aio, "insecure_channel", new_channel)
    monkeypatch.setattr(
        sandbox_pb2_grpc,
        "SandboxSessionDataPlaneServiceStub",
        lambda channel: object(),
    )

    first = AsyncGrpcDataPlaneRPC("http://shared-async.test:8080/a", Credentials())
    second = AsyncGrpcDataPlaneRPC("http://shared-async.test:8080/b", Credentials())
    first._ensure_stub()
    second._ensure_stub()

    assert first._channel is second._channel
    assert len(created) == 1
    await first.close()
    assert created[0][1].closed is False
    await second.close()
    assert created[0][1].closed is True


async def test_credential_renew_stops_on_permanent_error():
    calls = {"n": 0}

    class FakeRpc:
        async def create_session_credential(self, req):
            calls["n"] += 1
            raise PermissionDeniedError("revoked") from _FakeAio(
                grpc.StatusCode.PERMISSION_DENIED
            )

    class FakeClient:
        _rpc = FakeRpc()

    class FakeSandbox:
        id = "sess-1"
        _client = FakeClient()

    mgr = AsyncDataPlaneCredentialManager(FakeSandbox())
    with pytest.raises(PermissionDeniedError):
        await mgr._renew_loop(0)  # permanent error -> raise, do not loop
    assert calls["n"] == 1  # tried once, did not hammer


async def test_credential_renew_records_permanent_error_via_done_callback():
    # The renewal task is detached; a permanent-error raise must be retrieved and
    # recorded by _on_renew_done, not left as a dangling task exception.
    calls = {"n": 0}

    class FakeRpc:
        async def create_session_credential(self, req):
            calls["n"] += 1
            raise PermissionDeniedError("revoked") from _FakeAio(
                grpc.StatusCode.PERMISSION_DENIED
            )

    class FakeClient:
        _rpc = FakeRpc()

    class FakeSandbox:
        id = "sess-1"
        _client = FakeClient()

    mgr = AsyncDataPlaneCredentialManager(FakeSandbox())
    loop = asyncio.get_running_loop()
    task = loop.create_task(mgr._renew_loop(0))
    task.add_done_callback(mgr._on_renew_done)  # same wiring as _schedule
    await asyncio.sleep(0.05)  # let the detached task run + callback fire

    assert calls["n"] == 1
    assert isinstance(mgr._renew_error, PermissionDeniedError)  # recorded
    assert task.done() and not task.cancelled()
    assert isinstance(
        task.exception(), PermissionDeniedError
    )  # retrievable, not dangling


def test_schedule_skips_without_running_loop_but_keeps_expiry():
    # No running loop at set() (sync construction): renewal isn't scheduled, but
    # the expiry is retained so it can be recovered later.
    from datetime import datetime, timedelta, timezone

    cred = pb.SessionCredential(credential="c")
    cred.expires_at.FromDatetime(datetime.now(timezone.utc) + timedelta(seconds=100))

    class FakeSandbox:
        id = "sess-1"
        _client = None

    mgr = AsyncDataPlaneCredentialManager(FakeSandbox(), cred)
    assert mgr._task is None
    assert mgr._expires_at is not None


async def test_ensure_scheduled_recovers_renewal_in_loop():
    from datetime import datetime, timedelta, timezone

    cred = pb.SessionCredential(credential="c")
    cred.expires_at.FromDatetime(datetime.now(timezone.utc) + timedelta(seconds=100))

    class FakeSandbox:
        id = "sess-1"
        _client = None

    mgr = AsyncDataPlaneCredentialManager(FakeSandbox(), cred)
    # simulate the sync-construction state (no task scheduled)
    if mgr._task is not None:
        mgr._task.cancel()
    mgr._task = None

    mgr.ensure_scheduled()
    assert mgr._task is not None  # recovered
    mgr.stop()
