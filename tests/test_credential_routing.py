"""CLO-5016: a data-plane credential must never reach the wrong destination.

A credential authorizes one session on one node, so the SDK refuses to attach
it to a request for another session or a client aimed at another endpoint. A
sandbox that has been closed must not re-arm its renewal or its transport.
"""

from __future__ import annotations

import asyncio
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from tenki_sandbox._convert import sandbox_info
from tenki_sandbox._pb import pb
from tenki_sandbox.data_plane import (
    DataPlaneCredentialManager,
    DataPlaneCredentialMisroutedError,
    _authorize_data_plane_request,
)
from tenki_sandbox.sandbox import Sandbox

NODE_A = "https://node-a.sandbox.test"
NODE_B = "https://node-b.sandbox.test"


class _FakeSandbox:
    id = "sess-1"
    _client = None

    def _renew_data_plane(self) -> str:
        return ""


def _credential(value: str) -> pb.SessionCredential:
    credential = pb.SessionCredential(credential=value)
    credential.expires_at.FromDatetime(
        datetime.now(timezone.utc) + timedelta(seconds=3600)
    )
    return credential


def _manager(value: str, endpoint: str) -> DataPlaneCredentialManager:
    return DataPlaneCredentialManager(_FakeSandbox(), _credential(value), endpoint)


def test_credential_is_sent_to_the_endpoint_it_was_minted_for():
    credentials = _manager("cred-for-node-A", NODE_A)
    request = pb.StatRequest(session_id="sess-1", path="/home/tenki")

    assert (
        _authorize_data_plane_request(credentials, NODE_A, "sess-1", request)
        == "cred-for-node-A"
    )


def test_credential_minted_for_another_endpoint_is_refused():
    credentials = _manager("cred-for-node-B", NODE_B)
    request = pb.StatRequest(session_id="sess-1", path="/home/tenki")

    with pytest.raises(DataPlaneCredentialMisroutedError, match="another endpoint"):
        _authorize_data_plane_request(credentials, NODE_A, "sess-1", request)


def test_request_naming_another_session_is_refused():
    credentials = _manager("cred-for-node-A", NODE_A)
    request = pb.StatRequest(session_id="sess-2", path="/home/tenki")

    with pytest.raises(DataPlaneCredentialMisroutedError, match="targets session"):
        _authorize_data_plane_request(credentials, NODE_A, "sess-1", request)


def test_wrapped_request_session_id_is_checked():
    credentials = _manager("cred-for-node-A", NODE_A)
    request = pb.SandboxSessionDataPlaneServiceStatRequest(
        request=pb.StatRequest(session_id="sess-2", path="/home/tenki")
    )

    with pytest.raises(DataPlaneCredentialMisroutedError, match="targets session"):
        _authorize_data_plane_request(credentials, NODE_A, "sess-1", request)


def test_unbound_credential_is_adopted_once():
    credentials = _manager("cred-unbound", "")

    assert credentials.current_for(NODE_A) == ("", False)
    credentials.adopt_endpoint(NODE_A)
    assert credentials.current_for(NODE_A) == ("cred-unbound", True)

    # Adoption happens once; another node does not inherit it.
    credentials.adopt_endpoint(NODE_B)
    assert credentials.current_for(NODE_B) == ("", False)


class _TerminateRpc:
    def __init__(self, fail_first: bool = False) -> None:
        self.calls = 0
        self._fail_first = fail_first
        self._lock = threading.Lock()

    def terminate_session(self, req):
        with self._lock:
            self.calls += 1
            first = self.calls == 1
        if self._fail_first and first:
            raise RuntimeError("engine unavailable")
        return pb.TerminateSessionResponse(
            session=pb.SandboxSession(
                id="sess-1",
                state=pb.SESSION_STATE_TERMINATED,
                owner_type="SERVICE",
                owner_id="self",
            )
        )


class _FakeClient:
    def __init__(self, rpc) -> None:
        self._rpc = rpc


def _sandbox(rpc) -> Sandbox:
    info = sandbox_info(
        pb.SandboxSession(
            id="sess-1",
            state=pb.SESSION_STATE_RUNNING,
            owner_type="SERVICE",
            owner_id="self",
        )
    )
    return Sandbox(_FakeClient(rpc), info)


def test_concurrent_close_terminates_once():
    rpc = _TerminateRpc()
    sandbox = _sandbox(rpc)

    threads = [threading.Thread(target=sandbox.close) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert rpc.calls == 1


def test_close_is_a_no_op_once_terminated():
    rpc = _TerminateRpc()
    sandbox = _sandbox(rpc)

    sandbox.close()
    sandbox.close()

    assert rpc.calls == 1


def test_close_stays_retryable_when_terminate_fails():
    rpc = _TerminateRpc(fail_first=True)
    sandbox = _sandbox(rpc)

    with pytest.raises(Exception):
        sandbox.close()
    sandbox.close()

    assert rpc.calls == 2


def test_late_renewal_does_not_re_arm_a_closed_sandbox():
    rpc = _TerminateRpc()
    sandbox = _sandbox(rpc)
    sandbox.close()

    # A renewal in flight across close resolves afterwards and would otherwise
    # rebuild the credential manager and a pooled channel.
    sandbox._apply_data_plane_response(
        NODE_A, _credential("cred-late"), pb.DATA_PLANE_ROUTE_STATUS_VERIFIED
    )

    assert sandbox._data_plane_rpc is None
    assert sandbox._data_plane_credentials is None


_STREAM_CASES = [
    ("run", "Run", pb.RunRequest, "start"),
    ("dial", "Dial", pb.DialRequest, "open"),
    ("host_port_tunnel", "HostPortTunnel", pb.HostPortTunnelRequest, "open"),
    ("write_file_stream", "WriteFileStream", pb.WriteFileStreamRequest, "start"),
]


@pytest.mark.parametrize("method,rpc_name,frame_type,field", _STREAM_CASES)
@pytest.mark.parametrize("bad_index", [0, 1])
def test_sync_stream_refuses_other_session_frames(
    method, rpc_name, frame_type, field, bad_index
):
    from tenki_sandbox.data_plane import GrpcDataPlaneRPC

    credentials = _manager("stream-credential", NODE_A)
    rpc = GrpcDataPlaneRPC(NODE_A, credentials, "sess-1")
    seen = []

    def consume(frames, **kwargs):
        for envelope in frames:
            seen.append(envelope.frame)
        return (
            SimpleNamespace(response=None)
            if method == "write_file_stream"
            else iter(())
        )

    rpc._stub = SimpleNamespace(**{rpc_name: consume})
    frames = [frame_type(**{field: {"session_id": "sess-1"}})] * bad_index
    frames.append(frame_type(**{field: {"session_id": "other-session"}}))
    try:
        with pytest.raises(DataPlaneCredentialMisroutedError, match="other-session"):
            result = getattr(rpc, method)(iter(frames))
            if method != "write_file_stream":
                list(result)
        assert seen == frames[:bad_index]
    finally:
        credentials.stop()
        rpc.close()


@pytest.mark.parametrize("method,rpc_name,frame_type,field", _STREAM_CASES)
@pytest.mark.parametrize("bad_index", [0, 1])
@pytest.mark.asyncio
async def test_async_stream_refuses_other_session_frames(
    method, rpc_name, frame_type, field, bad_index
):
    from tenki_sandbox.aio.data_plane import AsyncGrpcDataPlaneRPC

    credentials = _manager("stream-credential", NODE_A)
    rpc = AsyncGrpcDataPlaneRPC(NODE_A, credentials, "sess-1")
    seen = []

    async def consume(frames, **kwargs):
        async for envelope in frames:
            seen.append(envelope.frame)
        return SimpleNamespace(response=None)

    async def stream(frames, **kwargs):
        await consume(frames, **kwargs)
        if False:
            yield

    rpc._stub = SimpleNamespace(
        **{rpc_name: consume if method == "write_file_stream" else stream}
    )
    frames = [frame_type(**{field: {"session_id": "sess-1"}})] * bad_index
    frames.append(frame_type(**{field: {"session_id": "other-session"}}))

    async def requests():
        for frame in frames:
            yield frame

    try:
        with pytest.raises(DataPlaneCredentialMisroutedError, match="other-session"):
            result = getattr(rpc, method)(requests())
            if method == "write_file_stream":
                await result
            else:
                async for _ in result:
                    pass
        assert seen == frames[:bad_index]
    finally:
        credentials.stop()
        await rpc.close()


@pytest.mark.parametrize("close_first", [False, True])
@pytest.mark.asyncio
async def test_async_close_cannot_be_revived_by_waiting_apply(close_first):
    from tenki_sandbox.aio.sandbox import AsyncSandbox

    async def terminate(req):
        return _TerminateRpc().terminate_session(req)

    sandbox = AsyncSandbox(
        _FakeClient(SimpleNamespace(terminate_session=terminate)), _sandbox(None)._info
    )
    async with sandbox._data_plane_lock:
        operations = [
            sandbox.close,
            lambda: sandbox._apply_data_plane_response(NODE_A, _credential("late")),
        ]
        if not close_first:
            operations.reverse()
        tasks = []
        for operation in operations:
            tasks.append(asyncio.create_task(operation()))
            await asyncio.sleep(0)
    await asyncio.gather(*tasks)
    assert sandbox._data_plane_rpc is None
    assert sandbox._data_plane_credentials is None
    assert sandbox._data_plane_closed


def test_missing_client_session_id_does_not_disable_authorization():
    credentials = _manager("credential", NODE_A)
    try:
        with pytest.raises(DataPlaneCredentialMisroutedError, match="targets session"):
            _authorize_data_plane_request(
                credentials, NODE_A, "", pb.StatRequest(session_id="sess-1")
            )
    finally:
        credentials.stop()
