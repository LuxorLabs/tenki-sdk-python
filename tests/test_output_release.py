from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from tenki_sandbox._convert import sandbox_info
from tenki_sandbox._pb import pb
from tenki_sandbox.aio.process import AsyncProcess
from tenki_sandbox.aio.sandbox import AsyncSandbox
from tenki_sandbox.errors import SandboxError
from tenki_sandbox.process import Process
from tenki_sandbox.sandbox import Sandbox


def frames():
    yield pb.RunResponse(started=pb.RunStarted(pid=42))
    yield pb.RunResponse(stdout=b"hello")
    yield pb.RunResponse(stderr=b"warn")
    yield pb.RunResponse(exit=pb.RunExit(exit_code=7, duration_ms=12, reason="exit"))


class RunRpc:
    def run(self, requests):
        return iter(frames())


class AsyncRunRpc:
    def run(self, requests):
        async def responses():
            for frame in frames():
                yield frame
                await asyncio.sleep(0)

        return responses()


@pytest.mark.parametrize("stream_only", [False, True])
def test_stream_output_retention(stream_only):
    proc = Process(RunRpc(), "test", ["test"], stream_only=stream_only)
    stdout, stderr = b"".join(proc.stdout), b"".join(proc.stderr)
    result = proc.wait()
    assert stdout == b"hello"
    assert stderr == b"warn"
    assert result.stdout == (b"" if stream_only else stdout)
    assert result.stderr == (b"" if stream_only else stderr)
    assert result.exit_code == 7
    assert result.duration_ms == 12


@pytest.mark.asyncio
@pytest.mark.parametrize("stream_only", [False, True])
async def test_async_stream_output_retention(stream_only):
    proc = await AsyncProcess.start(
        AsyncRunRpc(), "test", ["test"], stream_only=stream_only
    )

    async def read(stream):
        return b"".join([chunk async for chunk in stream])

    stdout, stderr, result = await asyncio.gather(
        read(proc.stdout), read(proc.stderr), proc.wait()
    )
    assert stdout == b"hello"
    assert stderr == b"warn"
    assert result.stdout == (b"" if stream_only else stdout)
    assert result.stderr == (b"" if stream_only else stderr)
    assert result.exit_code == 7
    assert result.duration_ms == 12


def info():
    return sandbox_info(pb.SandboxSession(id="test", state=pb.SESSION_STATE_RUNNING))


def test_release_stops_renewal_without_terminating():
    terminate = Mock(return_value=pb.TerminateSessionResponse())
    client = SimpleNamespace(_rpc=SimpleNamespace(terminate_session=terminate))
    sandbox = Sandbox(client, info())
    credentials, rpc = Mock(), Mock()
    sandbox._data_plane_credentials, sandbox._data_plane_rpc = credentials, rpc
    with sandbox:
        sandbox.release()
    sandbox.release()
    credentials.stop.assert_called_once()
    rpc.close.assert_called_once()
    terminate.assert_not_called()
    assert sandbox._detached
    with pytest.raises(RuntimeError, match="released"):
        sandbox._ensure_data_plane()
    sandbox.close()
    terminate.assert_called_once()


@pytest.mark.asyncio
async def test_async_release_stops_renewal_without_terminating():
    terminate = AsyncMock(return_value=pb.TerminateSessionResponse())
    client = SimpleNamespace(_rpc=SimpleNamespace(terminate_session=terminate))
    sandbox = AsyncSandbox(client, info())
    credentials, rpc = Mock(), SimpleNamespace(close=AsyncMock())
    sandbox._data_plane_credentials, sandbox._data_plane_rpc = credentials, rpc
    async with sandbox:
        await asyncio.gather(sandbox.release(), sandbox.release())
    credentials.stop.assert_called_once()
    rpc.close.assert_awaited_once()
    terminate.assert_not_called()
    assert sandbox._detached
    with pytest.raises(RuntimeError, match="released"):
        await sandbox._ensure_data_plane()
    await sandbox.close()
    terminate.assert_awaited_once()


@pytest.mark.parametrize("async_api", [False, True])
@pytest.mark.asyncio
async def test_release_rejects_late_credential_response(async_api):
    cls = AsyncSandbox if async_api else Sandbox
    sandbox = cls(SimpleNamespace(_rpc=SimpleNamespace()), info())
    credential = pb.SessionCredential(credential="late")
    if async_api:
        await sandbox.release()
        await sandbox._apply_data_plane_response("https://node.test", credential)
    else:
        sandbox.release()
        sandbox._apply_data_plane_response("https://node.test", credential)
    assert sandbox._data_plane_credentials is None
    assert sandbox._data_plane_rpc is None


@pytest.mark.parametrize("async_api", [False, True])
@pytest.mark.asyncio
async def test_failed_close_does_not_reopen_released_handle(async_api):
    mock = AsyncMock if async_api else Mock
    terminate = mock(side_effect=RuntimeError("unavailable"))
    cls = AsyncSandbox if async_api else Sandbox
    sandbox = cls(
        SimpleNamespace(_rpc=SimpleNamespace(terminate_session=terminate)), info()
    )
    if async_api:
        await sandbox.release()
        with pytest.raises(SandboxError, match="unavailable"):
            await sandbox.close()
        with pytest.raises(RuntimeError, match="released"):
            await sandbox._ensure_data_plane()
    else:
        sandbox.release()
        with pytest.raises(SandboxError, match="unavailable"):
            sandbox.close()
        with pytest.raises(RuntimeError, match="released"):
            sandbox._ensure_data_plane()


@pytest.mark.asyncio
async def test_release_blocks_credential_installation_waiting_for_lock(monkeypatch):
    import tenki_sandbox.aio.sandbox as module

    sandbox = AsyncSandbox(SimpleNamespace(_rpc=SimpleNamespace()), info())
    manager = Mock()
    monkeypatch.setattr(module, "AsyncDataPlaneCredentialManager", manager)
    async with sandbox._data_plane_lock:
        apply = asyncio.create_task(
            sandbox._apply_data_plane_response(
                "https://node.test", pb.SessionCredential(credential="late")
            )
        )
        await asyncio.sleep(0)
        release = asyncio.create_task(sandbox.release())
        await asyncio.sleep(0)
        assert sandbox._data_plane_closed
    await asyncio.gather(apply, release)
    manager.assert_not_called()
    assert sandbox._data_plane_credentials is None
    assert sandbox._data_plane_rpc is None


@pytest.mark.asyncio
async def test_release_blocks_credential_installation_during_transport_close(monkeypatch):
    import tenki_sandbox.aio.sandbox as module

    sandbox = AsyncSandbox(SimpleNamespace(_rpc=SimpleNamespace()), info())
    closing, proceed = asyncio.Event(), asyncio.Event()

    async def close():
        closing.set()
        await proceed.wait()

    sandbox._data_plane_rpc = SimpleNamespace(endpoint="https://old.test", close=close)
    sandbox._data_plane_credentials = Mock()
    manager = Mock()
    monkeypatch.setattr(module, "AsyncDataPlaneCredentialManager", manager)
    apply = asyncio.create_task(
        sandbox._apply_data_plane_response(
            "https://new.test", pb.SessionCredential(credential="late")
        )
    )
    await closing.wait()
    release = asyncio.create_task(sandbox.release())
    await asyncio.sleep(0)
    assert sandbox._data_plane_closed
    proceed.set()
    await asyncio.gather(apply, release)
    manager.assert_not_called()
    assert sandbox._data_plane_credentials is None
    assert sandbox._data_plane_rpc is None


class LargeRunRpc:
    def __init__(self):
        import threading
        self.full = threading.Event()

    def run(self, requests):
        yield pb.RunResponse(started=pb.RunStarted(pid=42))
        for index in range(32):
            if index == 2:
                self.full.set()
            yield pb.RunResponse(stdout=b"x" * 32768)
        yield pb.RunResponse(exit=pb.RunExit(exit_code=0))


def test_sync_stream_only_bounds_unread_queue():
    rpc = LargeRunRpc()
    proc = Process(rpc, "test", ["test"], stream_only=True)
    assert rpc.full.wait(timeout=5)
    assert proc.stdout._queue.qsize() == 16
    assert not proc._done.is_set()
    assert sum(len(chunk) for chunk in proc.stdout) == 32 * 32768
    assert list(proc.stderr) == []
    assert proc.wait().stdout == b""


@pytest.mark.asyncio
async def test_async_stream_only_bounds_unread_queue():
    full = asyncio.Event()

    class Rpc:
        def run(self, requests):
            async def responses():
                yield pb.RunResponse(started=pb.RunStarted(pid=42))
                for index in range(32):
                    if index == 2:
                        full.set()
                    yield pb.RunResponse(stdout=b"x" * 32768)
                yield pb.RunResponse(exit=pb.RunExit(exit_code=0))

            return responses()

    proc = await AsyncProcess.start(Rpc(), "test", ["test"], stream_only=True)
    await asyncio.wait_for(full.wait(), timeout=5)
    assert proc.stdout._queue.qsize() == 16
    assert not proc._done.is_set()
    total = 0
    async for chunk in proc.stdout:
        total += len(chunk)
    assert total == 32 * 32768
    assert [chunk async for chunk in proc.stderr] == []
    assert (await proc.wait()).stdout == b""


def test_sync_stream_only_timeout_releases_paused_output():
    rpc = LargeRunRpc()
    proc = Process(rpc, "test", ["test"], stream_only=True)
    assert rpc.full.wait(timeout=5)
    with pytest.raises(TimeoutError):
        proc.wait(timeout=0.01)
    result = proc.wait(timeout=5)
    assert result.stdout == b""
    proc._thread.join(timeout=1)
    assert not proc._thread.is_alive()


@pytest.mark.asyncio
async def test_async_stream_only_timeout_releases_paused_output():
    full, killed = asyncio.Event(), asyncio.Event()

    class Rpc:
        def run(self, requests):
            async def send():
                async for request in requests:
                    if request.WhichOneof("payload") == "signal":
                        assert request.signal.signal == pb.RunSignal.SIG_KILL
                        killed.set()
                        return

            async def responses():
                sender = asyncio.create_task(send())
                yield pb.RunResponse(started=pb.RunStarted(pid=42))
                for index in range(32):
                    if index == 2:
                        full.set()
                    yield pb.RunResponse(stdout=b"x" * 32768)
                await killed.wait()
                yield pb.RunResponse(exit=pb.RunExit(exit_code=137, signal="KILL"))
                await sender

            return responses()

    proc = await AsyncProcess.start(Rpc(), "test", ["test"], stream_only=True)
    await asyncio.wait_for(full.wait(), timeout=5)
    with pytest.raises(TimeoutError):
        await proc.wait(timeout=0.01)
    await asyncio.wait_for(killed.wait(), timeout=5)
    result = await proc.wait(timeout=5)
    assert result.exit_code == 137
    assert result.stdout == b""
    assert proc._pump_task.done()


def test_sync_stream_only_can_cancel_unused_stream():
    rpc = LargeRunRpc()
    proc = Process(rpc, "test", ["test"], stream_only=True)
    assert rpc.full.wait(timeout=5)
    proc.stdout.cancel()
    proc.stderr.cancel()
    assert proc.wait(timeout=5).stdout == b""


@pytest.mark.asyncio
async def test_async_stream_only_can_cancel_unused_stream():
    proc = await AsyncProcess.start(AsyncRunRpc(), "test", ["test"], stream_only=True)
    await proc.stdout.cancel()
    await proc.stderr.cancel()
    assert (await proc.wait(timeout=5)).stdout == b""
