from __future__ import annotations

import grpc

from tenki_sandbox.data_plane import GrpcDataPlaneRPC, _credential_error_is_retryable
from tenki_sandbox.errors import InvalidStateError


class _FakeRpc(grpc.RpcError):
    def __init__(self, code, details=""):
        super().__init__()
        self._code = code
        self._details = details

    def code(self):
        return self._code

    def details(self):
        return self._details


def test_sync_credential_error_retryable_classification():
    assert (
        _credential_error_is_retryable(_FakeRpc(grpc.StatusCode.FAILED_PRECONDITION))
        is True
    )
    assert (
        _credential_error_is_retryable(
            _FakeRpc(
                grpc.StatusCode.FAILED_PRECONDITION,
                "session not RUNNING (state=CREATING)",
            )
        )
        is True
    )
    assert (
        _credential_error_is_retryable(
            _FakeRpc(
                grpc.StatusCode.FAILED_PRECONDITION,
                "session not RUNNING (state=RESUMING)",
            )
        )
        is True
    )
    assert (
        _credential_error_is_retryable(
            _FakeRpc(
                grpc.StatusCode.FAILED_PRECONDITION,
                "session not RUNNING (state=PAUSED)",
            )
        )
        is False
    )
    mapped = InvalidStateError("session not RUNNING (state=PAUSED)")
    mapped.__cause__ = _FakeRpc(
        grpc.StatusCode.FAILED_PRECONDITION, "session not RUNNING (state=PAUSED)"
    )
    assert _credential_error_is_retryable(mapped) is False
    assert (
        _credential_error_is_retryable(_FakeRpc(grpc.StatusCode.PERMISSION_DENIED))
        is False
    )
    assert _credential_error_is_retryable(ValueError("transient")) is True


def test_sync_data_plane_channels_are_shared_by_origin(monkeypatch):
    from tenki_sandbox._internal.proto.tenki.sandbox.v1 import sandbox_pb2_grpc

    class Channel:
        closed = False

        def close(self):
            self.closed = True

    class Credentials:
        def current(self):
            return "credential"

    created = []

    def new_channel(target, options=None):
        assert dict(options or ())["grpc.keepalive_time_ms"] == 30_000
        channel = Channel()
        created.append((target, channel))
        return channel

    monkeypatch.setattr(grpc, "insecure_channel", new_channel)
    monkeypatch.setattr(
        sandbox_pb2_grpc,
        "SandboxSessionDataPlaneServiceStub",
        lambda channel: object(),
    )

    first = GrpcDataPlaneRPC("http://shared-sync.test:8080/a", Credentials())
    second = GrpcDataPlaneRPC("http://shared-sync.test:8080/b", Credentials())

    assert first._channel is second._channel
    assert len(created) == 1
    first.close()
    assert created[0][1].closed is False
    second.close()
    assert created[0][1].closed is True
