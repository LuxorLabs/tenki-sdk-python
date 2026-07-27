from __future__ import annotations

import pytest

from tenki_sandbox import (
    AsyncClient,
    AsyncSandbox,
    MissingAuthTokenError,
    WaitReadyFailedError,
)
from tenki_sandbox._pb import pb

from _async_fakes import FakeControlRPC


def test_async_client_requires_auth_without_injected_rpc(monkeypatch):
    monkeypatch.delenv("TENKI_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("TENKI_API_KEY", raising=False)
    with pytest.raises(MissingAuthTokenError):
        AsyncClient()


async def test_async_create_without_wait_maps_options():
    rpc = FakeControlRPC()
    client = AsyncClient(rpc=rpc)
    sandbox = await client.create(
        workspace_id="ws-1",
        wait=False,
        name="job",
        cpu_cores=4,
        memory_mb=8192,
        env={"A": "B"},
        tags=["Nightly", " gpu "],
        image="pub/polyglot:prod",
    )
    assert isinstance(sandbox, AsyncSandbox)
    req = rpc.requests["create_session"][-1]
    assert req.name == "job"
    assert req.cpu_cores == 4
    assert req.registry_ref == "pub/polyglot:prod"
    assert list(req.tags) == ["nightly", "gpu"]


async def test_async_create_waits_via_stream():
    rpc = FakeControlRPC()
    rpc.wait_states = [pb.SESSION_STATE_CREATING, pb.SESSION_STATE_RUNNING]
    client = AsyncClient(rpc=rpc)
    sandbox = await client.create(workspace_id="ws-1", wait=True, timeout=5)
    assert sandbox.state == "RUNNING"
    assert rpc.requests["wait_session"]


async def test_async_create_wait_failure_preserves_closable_sandbox():
    rpc = FakeControlRPC()
    rpc.wait_states = [pb.SESSION_STATE_CREATING]

    with pytest.raises(WaitReadyFailedError) as exc_info:
        await AsyncClient(rpc=rpc).create(
            workspace_id="ws-1", wait=True, timeout=0
        )

    assert exc_info.value.sandbox.id == "session-1"
    await exc_info.value.sandbox.close()
    assert rpc.requests["terminate_session"][-1].session_id == "session-1"


async def test_async_create_terminal_wait_failure_is_not_wrapped():
    rpc = FakeControlRPC()
    rpc.wait_states = [pb.SESSION_STATE_TERMINATED]

    with pytest.raises(RuntimeError, match="terminal state") as exc_info:
        await AsyncClient(rpc=rpc).create(
            workspace_id="ws-1", wait=True, timeout=0
        )

    assert not isinstance(exc_info.value, WaitReadyFailedError)


async def test_async_create_skips_wait_stream_when_already_running():
    # Server-held create returns RUNNING directly; no redundant WaitSession.
    rpc = FakeControlRPC()
    rpc.create_state = pb.SESSION_STATE_RUNNING
    sandbox = await AsyncClient(rpc=rpc).create(workspace_id="ws-1", wait=True, timeout=5)
    assert sandbox.state == "RUNNING"
    assert "wait_session" not in rpc.requests


async def test_async_wait_falls_back_to_poll_on_unimplemented():
    rpc = FakeControlRPC()
    rpc.wait_unimplemented = True
    rpc.get_states = [pb.SESSION_STATE_RUNNING]
    client = AsyncClient(rpc=rpc)
    sandbox = await client.create(workspace_id="ws-1", wait=True, timeout=5)
    assert sandbox.state == "RUNNING"
    assert rpc.requests["get_session"]  # poll path used


async def test_async_create_skips_failed_data_plane_route():
    # A FAILED route carries a dead endpoint; must NOT be cached, so first use
    # re-bootstraps via CreateSessionCredential instead of routing to the corpse.
    rpc = FakeControlRPC(data_plane_endpoint="http://dead.test")
    rpc.create_route_status = pb.DATA_PLANE_ROUTE_STATUS_FAILED
    sandbox = await AsyncClient(rpc=rpc).create(workspace_id="ws-1", wait=False)
    assert sandbox._data_plane_rpc is None
    assert sandbox._data_plane_verified is False


async def test_async_create_binds_verified_data_plane_route():
    rpc = FakeControlRPC(data_plane_endpoint="http://data-plane.test")
    rpc.create_route_status = pb.DATA_PLANE_ROUTE_STATUS_VERIFIED
    sandbox = await AsyncClient(rpc=rpc).create(workspace_id="ws-1", wait=False)
    assert sandbox._data_plane_rpc is not None
    assert sandbox._data_plane_verified is True


async def test_async_wait_ready_primes_data_plane_from_stream():
    rpc = FakeControlRPC()
    rpc.wait_states = [pb.SESSION_STATE_CREATING, pb.SESSION_STATE_RUNNING]
    rpc.wait_data_plane_endpoint = "http://data-plane.test"
    rpc.wait_data_plane_credential = pb.SessionCredential(
        credential="wait-session-credential"
    )
    rpc.wait_route_status = pb.DATA_PLANE_ROUTE_STATUS_VERIFIED
    sandbox = await AsyncClient(rpc=rpc).create(workspace_id="ws-1", wait=True, timeout=5)
    assert sandbox._data_plane_rpc is not None
    assert sandbox._data_plane_credentials.current() == "wait-session-credential"
    assert sandbox._data_plane_verified is True


async def test_async_sandbox_update_normalizes_tags():
    rpc = FakeControlRPC()
    sb = await AsyncClient(rpc=rpc).create(workspace_id="ws-1", wait=False)

    await sb.update(tags=["  Nightly ", "GPU", "  "])
    req = rpc.requests["update_session"][-1]
    assert list(req.tags) == ["nightly", "gpu"]
    assert req.clear_tags is False

    await sb.update(tags=["   "])  # all-whitespace normalizes to empty -> clear
    req = rpc.requests["update_session"][-1]
    assert list(req.tags) == []
    assert req.clear_tags is True


async def test_async_context_manager_terminates():
    rpc = FakeControlRPC()
    client = AsyncClient(rpc=rpc)
    async with await client.create(workspace_id="ws-1", wait=False) as sandbox:
        assert sandbox.state == "CREATING"
    assert rpc.requests["terminate_session"]


async def test_async_close_resets_data_plane_fields():
    from tenki_sandbox.aio.data_plane import AsyncDataPlaneCredentialManager

    rpc = FakeControlRPC()
    sandbox = await AsyncClient(rpc=rpc).create(workspace_id="ws-1", wait=False)

    class _DP:
        def __init__(self):
            self.closed = False

        async def close(self):
            self.closed = True

    dp = _DP()
    sandbox._data_plane_rpc = dp
    sandbox._data_plane_credentials = AsyncDataPlaneCredentialManager(sandbox)
    sandbox._data_plane_verified = True

    await sandbox.close()

    # torn down and nulled (no closed channel left referenced), then terminated
    assert dp.closed is True
    assert sandbox._data_plane_rpc is None
    assert sandbox._data_plane_credentials is None
    assert sandbox._data_plane_verified is False
    assert rpc.requests["terminate_session"]


async def test_async_pause_and_resume_reset_data_plane_fields():
    from tenki_sandbox.aio.data_plane import AsyncDataPlaneCredentialManager

    rpc = FakeControlRPC()
    sandbox = await AsyncClient(rpc=rpc).create(workspace_id="ws-1", wait=False)

    class _DP:
        def __init__(self):
            self.closed = False

        async def close(self):
            self.closed = True

    paused_dp = _DP()
    sandbox._data_plane_rpc = paused_dp
    sandbox._data_plane_credentials = AsyncDataPlaneCredentialManager(sandbox)
    sandbox._data_plane_verified = True

    await sandbox.pause()

    assert sandbox.state == "PAUSED"
    assert paused_dp.closed is True
    assert sandbox._data_plane_rpc is None
    assert sandbox._data_plane_credentials is None
    assert sandbox._data_plane_verified is False

    resumed_dp = _DP()
    sandbox._data_plane_rpc = resumed_dp
    sandbox._data_plane_credentials = AsyncDataPlaneCredentialManager(sandbox)
    sandbox._data_plane_verified = True

    await sandbox.resume()

    assert sandbox.state == "RUNNING"
    assert resumed_dp.closed is True
    assert sandbox._data_plane_rpc is None
    assert sandbox._data_plane_credentials is None
    assert sandbox._data_plane_verified is False


async def test_async_who_am_i():
    rpc = FakeControlRPC()
    client = AsyncClient(rpc=rpc)
    ident = await client.who_am_i()
    assert ident.owner_id == "user-1"


async def test_async_get_usage():
    rpc = FakeControlRPC()
    limits = await AsyncClient(rpc=rpc).get_usage()
    req = rpc.requests["get_workspace_sandbox_usage"][-1]
    assert req.workspace_id == ""
    assert [(l.key, l.unit, l.current, l.max) for l in limits] == [
        ("active_sessions", "count", 2, 10),
        ("snapshot_storage", "bytes", 1024, None),  # unset max -> None (unlimited)
    ]


async def test_async_list_and_get():
    rpc = FakeControlRPC()
    client = AsyncClient(rpc=rpc)
    sandboxes = await client.list()
    assert [sandbox.id for sandbox in sandboxes] == ["session-1", "session-2"]
    assert all(isinstance(sandbox, AsyncSandbox) for sandbox in sandboxes)
    assert [req.page_token for req in rpc.requests["list_workspace_sandboxes"]] == [
        "",
        "next",
    ]
    assert rpc.requests["list_workspace_sandboxes"][-1].workspace_id == ""
    got = await client.get("session-1")
    assert got.id == "session-1"


async def test_async_list_accepts_explicit_service_token_workspace():
    rpc = FakeControlRPC()
    await AsyncClient(rpc=rpc).list(workspace_id="  workspace-1  ")
    assert rpc.requests["list_workspace_sandboxes"][-1].workspace_id == "workspace-1"


async def test_async_client_close_awaits_rpc_close():
    rpc = FakeControlRPC()
    client = AsyncClient(rpc=rpc)
    await client.close()
    assert rpc.closed
