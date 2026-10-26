from __future__ import annotations

import pytest
from google.protobuf.any_pb2 import Any

from tenki_sandbox import (
    AsyncClient,
    AsyncSandbox,
    Client,
    ForkFailedError,
    QuotaExceededError,
    Sandbox,
    SessionNotFoundError,
)
from tenki_sandbox._pb import pb
from tenki_sandbox._transport import GrpcConfig, GrpcSandboxRPC
from tenki_sandbox.aio._transport import AsyncGrpcSandboxRPC
from tenki_sandbox.errors import SandboxError, _grpc_status_message_class, map_rpc_error


def _session(sid: str, name: str = "", state=pb.SESSION_STATE_RUNNING):
    return pb.SandboxSession(id=sid, name=name, state=state, owner_type="SERVICE", owner_id="self")


class _FakeRPCError(Exception):
    def __init__(self, code_name: str, message: str, details=()):
        super().__init__(message)
        self._code_name = code_name
        self._message = message
        status = _grpc_status_message_class()(message=message, details=list(details))
        self._status = status.SerializeToString()

    def code(self):
        return type("Code", (), {"name": self._code_name})()

    def details(self):
        return self._message

    def trailing_metadata(self):
        return (("grpc-status-details-bin", self._status),)


def _partial_failure() -> _FakeRPCError:
    packed = Any()
    packed.Pack(
        pb.ForkSessionResponse(
            snapshot_id="snap-9",
            sessions=[_session("child-1", "web-fork-1", pb.SESSION_STATE_CREATING)],
        )
    )
    return _FakeRPCError(
        "RESOURCE_EXHAUSTED", "workspace concurrent session quota exceeded", [packed]
    )


def _transport_wrapped(err: Exception) -> Exception:
    """Raise the way the transports do: the mapped error chained from the gRPC one."""
    try:
        raise map_rpc_error(err) from err
    except SandboxError as wrapped:
        return wrapped


class _ForkRPC:
    def __init__(self, error: Exception | None = None):
        self.request = None
        self.error = error

    def get_session(self, req):
        return pb.GetSessionResponse(session=_session(req.session_id, "web"))

    def fork_session(self, req):
        self.request = req
        if self.error is not None:
            raise self.error
        return pb.ForkSessionResponse(
            snapshot_id="snap-1",
            sessions=[
                _session("child-1", f"{req.name_prefix}-1", pb.SESSION_STATE_CREATING),
                _session("child-2", f"{req.name_prefix}-2", pb.SESSION_STATE_CREATING),
            ],
        )


class _AsyncForkRPC(_ForkRPC):
    async def get_session(self, req):
        return super().get_session(req)

    async def fork_session(self, req):
        return super().fork_session(req)


def test_fork_sends_options_and_returns_children():
    rpc = _ForkRPC()
    source = Client(rpc=rpc).get("source-1")

    children = source.fork(count=2, name_prefix="eval", wait=True)

    assert rpc.request.session_id == "source-1"
    assert rpc.request.count == 2
    assert rpc.request.name_prefix == "eval"
    assert rpc.request.wait is True
    assert [child.id for child in children] == ["child-1", "child-2"]
    assert all(isinstance(child, Sandbox) for child in children)
    assert children[1].name == "eval-2"
    assert children[0].state == "CREATING"


def test_fork_defaults_to_one_child_without_wait():
    rpc = _ForkRPC()

    Client(rpc=rpc).get("source-1").fork()

    assert rpc.request.count == 1
    assert rpc.request.name_prefix == ""
    assert rpc.request.wait is False


def test_fork_partial_failure_carries_created_children():
    source = Client(rpc=_ForkRPC(error=_partial_failure())).get("source-1")

    with pytest.raises(ForkFailedError) as raised:
        source.fork(count=3)

    assert raised.value.snapshot_id == "snap-9"
    assert [child.id for child in raised.value.sandboxes] == ["child-1"]
    assert isinstance(raised.value.sandboxes[0], Sandbox)
    assert isinstance(raised.value.cause, QuotaExceededError)


def test_fork_partial_failure_survives_transport_wrapping():
    source = Client(rpc=_ForkRPC(error=_transport_wrapped(_partial_failure()))).get("source-1")

    with pytest.raises(ForkFailedError) as raised:
        source.fork(count=3)

    assert raised.value.snapshot_id == "snap-9"
    assert [child.id for child in raised.value.sandboxes] == ["child-1"]
    assert isinstance(raised.value.cause, QuotaExceededError)


def test_fork_rejection_without_snapshot_maps_normally():
    source = Client(
        rpc=_ForkRPC(error=_FakeRPCError("NOT_FOUND", "session not found"))
    ).get("source-1")

    with pytest.raises(SessionNotFoundError):
        source.fork()


async def test_async_fork_returns_async_children():
    rpc = _AsyncForkRPC()
    source = await AsyncClient(rpc=rpc).get("source-1")

    children = await source.fork(count=2, name_prefix="eval")

    assert rpc.request.count == 2
    assert [child.name for child in children] == ["eval-1", "eval-2"]
    assert all(isinstance(child, AsyncSandbox) for child in children)


async def test_async_fork_partial_failure_carries_created_children():
    source = await AsyncClient(
        rpc=_AsyncForkRPC(error=_transport_wrapped(_partial_failure()))
    ).get("source-1")

    with pytest.raises(ForkFailedError) as raised:
        await source.fork(count=3)

    assert raised.value.snapshot_id == "snap-9"
    assert all(isinstance(child, AsyncSandbox) for child in raised.value.sandboxes)


class _ForkStub:
    """Stands in for the generated gRPC stub, so the call has to resolve through the transport's method map."""

    def __init__(self):
        self.calls = []

    def GetSession(self, request, metadata=None, timeout=None):
        return pb.GetSessionResponse(session=_session(request.session_id, "web"))

    def ForkSession(self, request, metadata=None, timeout=None):
        self.calls.append((request, metadata, timeout))
        return pb.ForkSessionResponse(
            snapshot_id="snap-1",
            sessions=[_session("child-1", f"{request.name_prefix}-1", pb.SESSION_STATE_CREATING)],
        )


class _AsyncForkStub(_ForkStub):
    async def GetSession(self, request, metadata=None, timeout=None):
        return super().GetSession(request, metadata, timeout)

    async def ForkSession(self, request, metadata=None, timeout=None):
        return super().ForkSession(request, metadata, timeout)


def test_fork_reaches_the_grpc_stub_through_the_sync_transport():
    rpc = GrpcSandboxRPC(GrpcConfig(base_url="http://localhost:8090", auth_token="tk_test"))
    stub = _ForkStub()
    rpc._stub = stub  # bypass the real channel; the dispatch under test is __getattr__
    try:
        children = Client(rpc=rpc).get("source-1").fork(count=2, name_prefix="eval")
    finally:
        rpc.close()

    request, metadata, timeout = stub.calls[0]
    assert request.session_id == "source-1"
    assert request.count == 2
    assert ("authorization", "Bearer tk_test") in metadata
    assert timeout == rpc._timeout
    assert [child.name for child in children] == ["eval-1"]


async def test_fork_reaches_the_grpc_stub_through_the_async_transport():
    rpc = AsyncGrpcSandboxRPC(GrpcConfig(base_url="http://localhost:8090", auth_token="tk_test"))
    stub = _AsyncForkStub()
    rpc._stub = stub
    try:
        source = await AsyncClient(rpc=rpc).get("source-1")
        children = await source.fork(count=2, name_prefix="eval")
    finally:
        await rpc.close()

    request, metadata, _ = stub.calls[0]
    assert request.count == 2
    assert ("authorization", "Bearer tk_test") in metadata
    assert [child.name for child in children] == ["eval-1"]
