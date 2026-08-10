"""Termination semantics for the sync and async process handles.

`kill()` / `signal()` enqueue a signal frame and return; `wait()` is the only
thing that resolves once the process has actually exited. These tests pin that
split so a future "make kill wait" change has to be an explicit decision.
"""

from __future__ import annotations

import asyncio
import threading

import pytest

from tenki_sandbox._pb import pb
from tenki_sandbox.aio.process import AsyncProcess
from tenki_sandbox.process import Process


class GatedKillRPC:
    """Guest whose process outlives the signal frame until release() is called."""

    def __init__(self) -> None:
        self.signals: list[int] = []
        self.signal_seen = threading.Event()
        self._release = threading.Event()

    def release(self) -> None:
        self._release.set()

    def run(self, requests):
        it = iter(requests)
        next(it)  # start frame
        yield pb.RunResponse(started=pb.RunStarted(pid=4242))
        for req in it:
            if req.WhichOneof("payload") == "signal":
                self.signals.append(req.signal.signal)
                self.signal_seen.set()
                break
        assert self._release.wait(timeout=5), "test never released the exit frame"
        # Mirror the real guest: a signal-killed process reports exit_code -1
        # (not the shell's 128+9) and Go's own name "killed", not "KILL".
        yield pb.RunResponse(exit=pb.RunExit(exit_code=-1, signal="killed", reason="signaled"))


class ImmediateExitRPC:
    """Guest whose process exits on its own, with no signal involved."""

    def run(self, requests):
        it = iter(requests)
        next(it)
        yield pb.RunResponse(started=pb.RunStarted(pid=4243))
        yield pb.RunResponse(exit=pb.RunExit(exit_code=0, reason="exit"))


class AsyncGatedKillRPC:
    def __init__(self) -> None:
        self.signals: list[int] = []
        self.signal_seen = asyncio.Event()
        self._release = asyncio.Event()

    def release(self) -> None:
        self._release.set()

    async def run(self, requests):
        it = requests.__aiter__()
        await it.__anext__()  # start frame
        yield pb.RunResponse(started=pb.RunStarted(pid=4242))
        async for req in it:
            if req.WhichOneof("payload") == "signal":
                self.signals.append(req.signal.signal)
                self.signal_seen.set()
                break
        await asyncio.wait_for(self._release.wait(), timeout=5)
        # Mirror the real guest: a signal-killed process reports exit_code -1
        # (not the shell's 128+9) and Go's own name "killed", not "KILL".
        yield pb.RunResponse(exit=pb.RunExit(exit_code=-1, signal="killed", reason="signaled"))


class AsyncImmediateExitRPC:
    async def run(self, requests):
        it = requests.__aiter__()
        await it.__anext__()
        yield pb.RunResponse(started=pb.RunStarted(pid=4243))
        yield pb.RunResponse(exit=pb.RunExit(exit_code=0, reason="exit"))


def test_kill_sends_sigkill_without_waiting_for_exit():
    rpc = GatedKillRPC()
    proc = Process(rpc, "session-1", ["sleep", "3600"])
    assert proc.pid == 4242

    proc.kill()

    assert rpc.signal_seen.wait(timeout=5), "guest never received the signal frame"
    assert rpc.signals == [pb.RunSignal.SIG_KILL]

    # kill() has returned while the exit frame is still gated, so the process is
    # provably still running. (wait() sends its own SIGKILL on timeout; the guest
    # has already stopped reading, so that extra frame goes nowhere.)
    with pytest.raises(TimeoutError):
        proc.wait(timeout=0.1)

    rpc.release()
    result = proc.wait(timeout=5)
    assert result.exit_code == -1
    assert result.signal == "killed"


def test_kill_after_exit_is_a_noop():
    proc = Process(ImmediateExitRPC(), "session-1", ["true"])
    assert proc.wait(timeout=5).exit_code == 0

    proc.kill()  # must not raise: the frame is simply never read


async def test_async_kill_sends_sigkill_without_waiting_for_exit():
    rpc = AsyncGatedKillRPC()
    proc = await AsyncProcess.start(rpc, "session-1", ["sleep", "3600"])
    assert await proc.pid() == 4242

    await proc.kill()

    await asyncio.wait_for(rpc.signal_seen.wait(), timeout=5)
    assert rpc.signals == [pb.RunSignal.SIG_KILL]

    with pytest.raises(TimeoutError):
        await proc.wait(timeout=0.1)

    rpc.release()
    result = await proc.wait(timeout=5)
    assert result.exit_code == -1
    assert result.signal == "killed"


async def test_async_kill_after_exit_is_a_noop():
    proc = await AsyncProcess.start(AsyncImmediateExitRPC(), "session-1", ["true"])
    result = await proc.wait(timeout=5)
    assert result.exit_code == 0

    await proc.kill()  # must not raise
