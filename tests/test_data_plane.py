from __future__ import annotations

import grpc

from tenki_sandbox.data_plane import _credential_error_is_retryable
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
