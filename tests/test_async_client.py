from __future__ import annotations

import asyncio
import dataclasses
import warnings

import pytest

from tenki_sandbox import (
    AsyncClient,
    AsyncSandbox,
    InvalidAuthTokenError,
    MissingAuthTokenError,
    PauseFailedError,
    PauseWaitCancelledError,
    SandboxError,
    SandboxWarningCode,
    SandboxWarningNotification,
    WaitReadyFailedError,
)
from tenki_sandbox._pb import pb
from tenki_sandbox.aio.data_plane import _AsyncDataPlaneEndpointHints
from tenki_sandbox.models import VolumeMount

from _async_fakes import FakeControlRPC, FakeRPCError


def test_async_client_requires_auth_without_injected_rpc(monkeypatch):
    monkeypatch.delenv("TENKI_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("TENKI_API_KEY", raising=False)
    with pytest.raises(MissingAuthTokenError):
        AsyncClient()


async def test_async_data_plane_endpoint_hint_resolution_is_singleflight():
    hints = _AsyncDataPlaneEndpointHints()
    calls = 0

    async def resolve():
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.02)
        return "https://node.test"

    results = await asyncio.gather(
        *(hints.resolve(resolve) for _ in range(8))
    )

    assert calls == 1
    assert results == ["https://node.test"] * 8


async def test_async_data_plane_endpoint_hint_waiters_retry_resolver_failure():
    hints = _AsyncDataPlaneEndpointHints()
    calls = 0

    async def resolve():
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)
        if calls == 1:
            raise RuntimeError("transient resolver failure")
        return "https://node.test"

    results = await asyncio.gather(
        *(hints.resolve(resolve) for _ in range(8)), return_exceptions=True
    )

    assert calls == 2
    assert sum(isinstance(result, RuntimeError) for result in results) == 1
    assert results.count("https://node.test") == 7


async def test_async_data_plane_endpoint_hints_are_isolated_per_session():
    first = _AsyncDataPlaneEndpointHints()
    second = _AsyncDataPlaneEndpointHints()
    first.remember("https://first.test")
    second.remember("https://second.test")

    assert first.current() == "https://first.test"
    assert second.current() == "https://second.test"

    first.clear()
    assert first.current() == ""


async def test_async_volume_detach_waits_past_historical_detached_attachment():
    sandbox = await AsyncClient(rpc=FakeControlRPC()).create(wait=False)
    sandbox._info = dataclasses.replace(
        sandbox.info,
        volume_mounts=(
            VolumeMount(volume_id="vol-1", mount_path="/data", state="DETACHED"),
            VolumeMount(volume_id="vol-1", mount_path="/data", state="SYNC_PENDING"),
        ),
    )

    assert sandbox._is_volume_detached("vol-1") is False
    sandbox._info = dataclasses.replace(
        sandbox.info,
        volume_mounts=tuple(
            dataclasses.replace(mount, state="DETACHED")
            for mount in sandbox.info.volume_mounts
        ),
    )
    assert sandbox._is_volume_detached("vol-1") is True


async def test_async_client_sandboxes_isolate_data_plane_endpoint_hints():
    client = AsyncClient(rpc=FakeControlRPC())
    first = await client.create(wait=False)
    second = await client.create(wait=False)

    first._data_plane_endpoint_hints.remember("https://node.test")

    assert second._data_plane_endpoint_hints.current() == ""


async def test_async_renew_data_plane_rejects_failed_route_endpoint():
    rpc = FakeControlRPC()

    async def create_session_credential(_req):
        return pb.CreateSessionCredentialResponse(
            data_plane_endpoint="https://dead.test",
            route_status=pb.DATA_PLANE_ROUTE_STATUS_FAILED,
        )

    rpc.create_session_credential = create_session_credential
    sandbox = await AsyncClient(rpc=rpc).create(wait=False)

    assert await sandbox._renew_data_plane() == ""
    assert sandbox._data_plane_rpc is None


async def test_async_refresh_hinted_data_plane_keeps_hint_after_failed_renewal():
    rpc = FakeControlRPC()

    async def create_session_credential(_req):
        return pb.CreateSessionCredentialResponse(
            data_plane_endpoint="https://dead.test",
            route_status=pb.DATA_PLANE_ROUTE_STATUS_FAILED,
        )

    rpc.create_session_credential = create_session_credential
    sandbox = await AsyncClient(rpc=rpc).create(wait=False)
    data_plane = object()
    sandbox._data_plane_rpc = data_plane
    sandbox._data_plane_from_hint = True

    await sandbox._refresh_hinted_data_plane(data_plane)

    assert sandbox._data_plane_from_hint is True


async def test_async_verify_serving_discards_failed_rpc():
    class FailingDataPlaneRPC:
        closed = False

        async def ensure_serving(self, _session_id):
            raise FakeRPCError("UNAVAILABLE")

        async def close(self):
            self.closed = True

    sandbox = await AsyncClient(rpc=FakeControlRPC()).create(wait=False)
    data_plane = FailingDataPlaneRPC()
    class CredentialManager:
        stopped = False

        def stop(self):
            self.stopped = True

    credentials = CredentialManager()
    sandbox._data_plane_rpc = data_plane
    sandbox._data_plane_credentials = credentials

    with pytest.raises(FakeRPCError):
        await sandbox._verify_serving()

    assert data_plane.closed is True
    assert sandbox._data_plane_rpc is None
    assert credentials.stopped is True
    assert sandbox._data_plane_credentials is None


@pytest.mark.parametrize("auth_token", ["ory_st_test", "cookie:session-token"])
@pytest.mark.parametrize("rpc", [None, object()])
def test_async_client_rejects_invalid_auth_tokens(auth_token, rpc):
    with pytest.raises(InvalidAuthTokenError):
        AsyncClient(auth_token=auth_token, rpc=rpc)


@pytest.mark.parametrize("rpc", [None, object()])
def test_async_client_accepts_api_key_as_an_alias_for_auth_token(rpc, monkeypatch):
    monkeypatch.delenv("TENKI_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("TENKI_API_KEY", raising=False)

    assert AsyncClient(api_key="tk_alias", rpc=rpc).auth_token == "tk_alias"


def test_async_auth_token_wins_over_the_api_key_alias():
    assert AsyncClient(auth_token="tk_one", api_key="tk_two", rpc=object()).auth_token == "tk_one"


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
    assert req.wait_ready is False
    assert list(req.tags) == ["nightly", "gpu"]


async def test_async_get_session_metrics_maps_optional_values_and_window():
    rpc = FakeControlRPC()
    metrics = await AsyncClient(rpc=rpc).get_session_metrics("session-1", window=300)

    assert rpc.requests["get_session_metrics"][-1].window.seconds == 300
    assert metrics.cpu.average_cores is None
    assert metrics.cpu.sample_count == 3


async def test_async_sandbox_metrics_uses_its_session_id():
    rpc = FakeControlRPC()
    sandbox = await AsyncClient(rpc=rpc).get("session-1")
    metrics = await sandbox.metrics()

    assert metrics.session_id == "session-1"
    assert rpc.requests["get_session_metrics"][-1].session_id == "session-1"


async def test_async_create_retains_and_emits_warnings_once():
    rpc = _rpc_with_create_warning(FakeControlRPC())
    emitted = []

    sandbox = await AsyncClient(rpc=rpc, warning_handler=emitted.append).create(wait=False)

    assert sandbox.warnings[0].code == SandboxWarningCode.STICKY_OVERRIDES_MAX_DURATION
    assert emitted == list(sandbox.warnings)


async def test_async_create_default_warning_handler_is_safe_with_warnings_as_errors():
    rpc = _rpc_with_create_warning(FakeControlRPC())

    with warnings.catch_warnings():
        warnings.simplefilter("error", SandboxWarningNotification)
        sandbox = await AsyncClient(rpc=rpc).create(wait=False)

    assert sandbox.warnings[0].message == "sticky overrides max_duration"


async def test_async_create_default_warning_handler_uses_the_caller_module_for_filters():
    rpc = _rpc_with_create_warning(FakeControlRPC())

    with warnings.catch_warnings(record=True) as captured:
        warnings.filterwarnings(
            "ignore", category=SandboxWarningNotification, module=__name__
        )
        await AsyncClient(rpc=rpc).create(wait=False)

    assert not captured


async def test_async_sandbox_create_default_warning_handler_uses_the_caller_module_and_owns_client(
    monkeypatch,
):
    rpc = _rpc_with_create_warning(FakeControlRPC())
    client = AsyncClient(rpc=rpc)
    monkeypatch.setattr("tenki_sandbox.aio.client.AsyncClient", lambda **_kwargs: client)

    with warnings.catch_warnings(record=True) as captured:
        warnings.filterwarnings(
            "ignore", category=SandboxWarningNotification, module=__name__
        )
        async with await AsyncSandbox.create(wait=False) as sandbox:
            assert sandbox._client is client
            assert sandbox._owns_client
            assert sandbox.warnings[0].message == "sticky overrides max_duration"

    assert not captured
    assert rpc.closed


async def test_async_sandbox_create_ignores_custom_warning_handler_errors(
    monkeypatch, caplog
):
    rpc = _rpc_with_create_warning(FakeControlRPC())

    def warning_handler(_warning):
        raise RuntimeError("handler failed")

    client = AsyncClient(rpc=rpc, warning_handler=warning_handler)
    monkeypatch.setattr("tenki_sandbox.aio.client.AsyncClient", lambda **_kwargs: client)

    with caplog.at_level("ERROR", logger="tenki_sandbox"):
        async with await AsyncSandbox.create(wait=False) as sandbox:
            assert sandbox._client is client
            assert sandbox._owns_client
            assert sandbox.warnings[0].message == "sticky overrides max_duration"

    assert "sandbox warning handler failed" in caplog.messages
    assert rpc.closed


async def test_async_create_default_warning_handler_and_suppression():
    rpc = _rpc_with_create_warning(FakeControlRPC())
    with pytest.warns(SandboxWarningNotification, match="sticky overrides max_duration"):
        await AsyncClient(rpc=rpc).create(wait=False)

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        await AsyncClient(rpc=rpc, warning_handler=None).create(wait=False)
    assert not captured


def _rpc_with_create_warning(rpc):
    original_create = rpc.create_session

    async def create_session(req):
        response = await original_create(req)
        response.warnings.add(
            code=pb.SANDBOX_WARNING_CODE_STICKY_OVERRIDES_MAX_DURATION,
            message="sticky overrides max_duration",
        )
        return response

    rpc.create_session = create_session
    return rpc


async def test_async_create_waits_via_stream():
    rpc = FakeControlRPC()
    rpc.wait_states = [pb.SESSION_STATE_CREATING, pb.SESSION_STATE_RUNNING]
    client = AsyncClient(rpc=rpc)
    sandbox = await client.create(workspace_id="ws-1", wait=True, timeout=5)
    assert sandbox.state == "RUNNING"
    assert rpc.requests["create_session"][-1].wait_ready is False
    assert rpc.requests["wait_session"]


async def test_async_runtime_wait_keeps_server_held_create():
    rpc = FakeControlRPC()

    await AsyncClient(rpc=rpc).create(workspace_id="ws-1", wait=True, wait_for_runtime=True, timeout=5)

    req = rpc.requests["create_session"][-1]
    assert req.wait_ready is True
    assert req.wait_for_runtime is True


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


async def test_async_wait_paused_reaches_paused():
    rpc = FakeControlRPC()
    rpc.get_states = [pb.SESSION_STATE_PAUSING, pb.SESSION_STATE_PAUSED]
    sandbox = await AsyncClient(rpc=rpc).create(workspace_id="ws-1", wait=False)

    await sandbox.wait_paused(timeout=5)

    assert sandbox.state == "PAUSED"


async def test_async_wait_paused_retries_not_found(monkeypatch):
    rpc = FakeControlRPC()
    rpc.get_states = [pb.SESSION_STATE_PAUSED]
    get_session = rpc.get_session
    calls = 0

    async def get_session_after_replica_lag(req):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise FakeRPCError("NOT_FOUND")
        return await get_session(req)

    rpc.get_session = get_session_after_replica_lag
    sandbox = await AsyncClient(rpc=rpc).create(workspace_id="ws-1", wait=False)

    async def no_sleep(_duration):
        return None

    monkeypatch.setattr("tenki_sandbox.aio.sandbox.asyncio.sleep", no_sleep)

    await sandbox.wait_paused(timeout=5)

    assert sandbox.state == "PAUSED"
    assert calls == 2


@pytest.mark.parametrize("timeout", [0, -1])
async def test_async_wait_paused_nonpositive_timeout_waits_until_paused(monkeypatch, timeout):
    rpc = FakeControlRPC()
    rpc.get_states = [pb.SESSION_STATE_PAUSING, pb.SESSION_STATE_PAUSED]
    sandbox = await AsyncClient(rpc=rpc).create(workspace_id="ws-1", wait=False)

    async def no_sleep(_duration):
        return None

    monkeypatch.setattr("tenki_sandbox.aio.sandbox.asyncio.sleep", no_sleep)

    await sandbox.wait_paused(timeout=timeout)

    assert sandbox.state == "PAUSED"


async def test_async_wait_paused_unbounded_can_be_cancelled():
    rpc = FakeControlRPC()
    sandbox = await AsyncClient(rpc=rpc).create(workspace_id="ws-1", wait=False)
    cancel = asyncio.Event()
    cancel.set()

    with pytest.raises(PauseWaitCancelledError, match="cancelled locally"):
        await sandbox.wait_paused(timeout=0, cancel_event=cancel)

    assert "get_session" not in rpc.requests


async def test_async_wait_paused_preserves_task_cancellation():
    rpc = FakeControlRPC()
    rpc.get_states = [pb.SESSION_STATE_PAUSING]
    sandbox = await AsyncClient(rpc=rpc).create(workspace_id="ws-1", wait=False)
    task = asyncio.create_task(sandbox.wait_paused(timeout=0))
    await asyncio.sleep(0)

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task


async def test_async_wait_paused_raises_when_pause_rolls_back():
    rpc = FakeControlRPC()
    rpc.get_states = [pb.SESSION_STATE_PAUSING, pb.SESSION_STATE_RUNNING]
    sandbox = await AsyncClient(rpc=rpc).create(workspace_id="ws-1", wait=False)

    with pytest.raises(PauseFailedError):
        await sandbox.wait_paused(timeout=5)


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


async def test_async_sandbox_update_maps_max_duration_with_sticky_false():
    rpc = FakeControlRPC()
    sandbox = await AsyncClient(rpc=rpc).create(wait=False)

    await sandbox.update(sticky=False, max_duration=90.5)

    req = rpc.requests["update_session"][-1]
    assert req.HasField("sticky")
    assert req.sticky is False
    assert req.max_duration.seconds == 90
    assert req.max_duration.nanos == 500_000_000


@pytest.mark.parametrize("max_duration", [0, -1])
async def test_async_sandbox_update_rejects_nonpositive_max_duration_before_rpc(
    max_duration,
):
    rpc = FakeControlRPC()
    sandbox = await AsyncClient(rpc=rpc).create(wait=False)

    with pytest.raises(ValueError, match="max_duration must be greater than zero"):
        await sandbox.update(sticky=False, max_duration=max_duration)

    assert "update_session" not in rpc.requests


async def test_async_sandbox_update_requires_sticky_with_max_duration_before_rpc():
    rpc = FakeControlRPC()
    sandbox = await AsyncClient(rpc=rpc).create(wait=False)

    with pytest.raises(
        ValueError, match="sticky must be set when max_duration is provided"
    ):
        await sandbox.update(max_duration=90)

    assert "update_session" not in rpc.requests


async def test_async_sandbox_update_retains_warnings_and_is_safe_with_warnings_as_errors():
    rpc = FakeControlRPC()
    original_update = rpc.update_session

    async def update_session(req):
        response = await original_update(req)
        response.warnings.add(
            code=pb.SANDBOX_WARNING_CODE_STICKY_OVERRIDES_IDLE_TIMEOUT,
            message="sticky overrides idle timeout",
        )
        return response

    rpc.update_session = update_session
    sandbox = await AsyncClient(rpc=rpc).create(wait=False)

    with warnings.catch_warnings():
        warnings.simplefilter("error", SandboxWarningNotification)
        await sandbox.update(sticky=True)

    assert sandbox.warnings[-1].code == SandboxWarningCode.STICKY_OVERRIDES_IDLE_TIMEOUT
    assert sandbox.warnings[-1].message == "sticky overrides idle timeout"


async def test_async_sandbox_update_ignores_custom_warning_handler_errors(caplog):
    rpc = FakeControlRPC()
    original_update = rpc.update_session

    async def update_session(req):
        response = await original_update(req)
        response.warnings.add(
            code=pb.SANDBOX_WARNING_CODE_MAX_DURATION_CAPPED,
            message="max duration capped",
        )
        return response

    def warning_handler(_warning):
        raise RuntimeError("handler failed")

    rpc.update_session = update_session
    sandbox = await AsyncClient(rpc=rpc, warning_handler=warning_handler).create(
        wait=False
    )
    with caplog.at_level("ERROR", logger="tenki_sandbox"):
        await sandbox.update(sticky=False)

    assert sandbox.warnings[-1].code == SandboxWarningCode.MAX_DURATION_CAPPED
    assert "sandbox warning handler failed" in caplog.messages


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
    assert getattr(rpc.requests["pause_session"][-1], "async") is False
    assert paused_dp.closed is True
    assert sandbox._data_plane_rpc is None
    assert sandbox._data_plane_credentials is None
    assert sandbox._data_plane_verified is False

    await sandbox.pause_async()
    assert sandbox.state == "PAUSED"
    assert getattr(rpc.requests["pause_session"][-1], "async") is True

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
        ("max_concurrent_jobs", "count", 2, 10),
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


@pytest.mark.asyncio
async def test_async_sandbox_create_closes_owned_client_when_create_fails(monkeypatch):
    rpc = FakeControlRPC()

    async def boom(_req):
        raise FakeRPCError("UNAVAILABLE")

    rpc.create_session = boom
    client = AsyncClient(rpc=rpc)
    monkeypatch.setattr("tenki_sandbox.aio.client.AsyncClient", lambda **_kwargs: client)

    with pytest.raises(SandboxError):
        await AsyncSandbox.create(wait=False)

    assert rpc.closed, "owned client leaked when create failed"


@pytest.mark.asyncio
async def test_async_sandbox_create_keeps_owned_client_open_for_wait_failure(monkeypatch):
    # The sandbox exists server-side and rides out on the error, so the caller
    # still needs a live channel to terminate it through.
    rpc = FakeControlRPC()
    rpc.wait_states = [pb.SESSION_STATE_CREATING]
    client = AsyncClient(rpc=rpc)
    monkeypatch.setattr("tenki_sandbox.aio.client.AsyncClient", lambda **_kwargs: client)

    with pytest.raises(WaitReadyFailedError) as exc_info:
        await AsyncSandbox.create(wait=True, timeout=0)

    assert not rpc.closed
    async with exc_info.value.sandbox as sandbox:
        assert sandbox._owns_client
    assert rpc.requests["terminate_session"][-1].session_id == "session-1"
    assert rpc.closed
