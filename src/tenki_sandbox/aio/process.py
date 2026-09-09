from __future__ import annotations

import asyncio
from typing import AsyncIterator, Sequence

from .._pb import pb
from .._time import duration_ms
from ..errors import (
    CapabilityUnavailableError,
    PrimitiveTimeoutError,
    SessionTerminatedError,
    is_session_terminated_reason,
    map_rpc_error,
    session_termination_cause,
)
from ..models import CommandResult
from ..process import (
    _disk_usage_from_exit,
    _is_provisioning_session_not_found,
    _is_retryable_run_establishment_error,
    normalize_argv,
)

# Seconds to wait for the guest to ack process start before giving up.
_START_TIMEOUT = 30


class _StaticAsyncProcessSandbox:
    def __init__(self, rpc) -> None:
        self._rpc = rpc

    async def _ensure_data_plane(self, *, verify_serving: bool = True):
        return self._rpc

    def _mark_data_plane_serving(self, rpc) -> None:
        return None

    def _data_plane_provisioning_pending(self, rpc) -> bool:
        return False

    async def _refresh_hinted_data_plane(self, rpc) -> None:
        return None


class AsyncProcessStream:
    """Async-iterable byte stream backed by an asyncio.Queue."""

    def __init__(self) -> None:
        self._queue: asyncio.Queue = asyncio.Queue()
        self._chunks: list[bytes] = []

    def push(self, data: bytes) -> None:
        if data:
            self._chunks.append(data)
            self._queue.put_nowait(data)

    def close(self) -> None:
        self._queue.put_nowait(None)

    def fail(self, err: Exception) -> None:
        self._queue.put_nowait(err)

    def __aiter__(self) -> "AsyncProcessStream":
        return self

    async def __anext__(self) -> bytes:
        item = await self._queue.get()
        if item is None:
            raise StopAsyncIteration
        if isinstance(item, Exception):
            raise item
        return item

    def read(self) -> bytes:
        return b"".join(self._chunks)

    def read_text(self, encoding: str = "utf-8") -> str:
        return self.read().decode(encoding, errors="replace")


class AsyncProcess:
    def __init__(
        self,
        session_id: str,
        argv: Sequence[object],
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout: float | int | object | None = None,
        privileged: bool = False,
    ) -> None:
        self.argv = normalize_argv(argv)
        self.stdout = AsyncProcessStream()
        self.stderr = AsyncProcessStream()
        self._outbound: asyncio.Queue = asyncio.Queue()
        self._started = asyncio.Event()
        self._done = asyncio.Event()
        self._pid: int | None = None
        self._result: CommandResult | None = None
        self._error: Exception | None = None
        self._pump_task: asyncio.Task | None = None
        self._start_replayable = True

        start = pb.RunRequest(
            start=pb.RunStart(
                session_id=session_id,
                cmd=self.argv,
                cwd=cwd or "",
                env=dict(env or {}),
                stream_stdin=True,
                timeout_ms=duration_ms(timeout),
                privileged=privileged,
            )
        )
        self._start_request = start

    @classmethod
    async def start(
        cls,
        sandbox,
        session_id: str,
        argv: Sequence[object],
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout: float | int | object | None = None,
        stdin: bytes | str | None = None,
        privileged: bool = False,
    ) -> "AsyncProcess":
        proc = cls(session_id, argv, cwd=cwd, env=env, timeout=timeout, privileged=privileged)
        proc._sandbox = (
            sandbox
            if hasattr(sandbox, "_ensure_data_plane")
            else _StaticAsyncProcessSandbox(sandbox)
        )
        proc._pump_task = asyncio.create_task(proc._pump())
        if stdin is not None:
            await proc.write_stdin(stdin)
            await proc.close_stdin()
        return proc

    async def _await_started(self) -> bool:
        """Wait up to _START_TIMEOUT for the start ack. True if set, False on timeout."""
        try:
            await asyncio.wait_for(self._started.wait(), timeout=_START_TIMEOUT)
            return True
        except asyncio.TimeoutError:
            return False

    async def pid(self) -> int | None:
        # Best-effort: return the pid once started, or None if it never acks /
        # errored — never raises (mirrors the sync client's pid()).
        await self._await_started()
        return self._pid

    async def write_stdin(self, data: bytes | str) -> None:
        await self._wait_started()
        payload = data.encode() if isinstance(data, str) else data
        await self._outbound.put(pb.RunRequest(stdin=payload))

    async def close_stdin(self) -> None:
        await self._wait_started()
        await self._outbound.put(pb.RunRequest(stdin_close=True))

    async def signal(self, signal: str) -> None:
        await self._wait_started()
        name = signal.upper().removeprefix("SIG")
        enum_name = f"SIG_{name}"
        enum_value = getattr(pb.RunSignal, enum_name, None)
        if enum_value is None:
            raise ValueError(f"unsupported signal: {signal}")
        await self._outbound.put(pb.RunRequest(signal=pb.RunSignal(signal=enum_value)))

    async def kill(self) -> None:
        enum_value = getattr(pb.RunSignal, "SIG_KILL")
        self._start_replayable = False
        await self._outbound.put(pb.RunRequest(signal=pb.RunSignal(signal=enum_value)))

    async def wait(self, timeout: float | None = None) -> CommandResult:
        """Block until the command finishes.

        ``timeout`` bounds how long this call waits, not the command itself: on
        expiry the process is killed and ``TimeoutError`` is raised. The command's
        own execution budget is ``AsyncSandbox.exec(timeout=...)``, which the
        guest-agent enforces and reports as ``CommandResult.timed_out``.
        """
        try:
            await asyncio.wait_for(self._done.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            await self.kill()
            raise TimeoutError("timed out waiting for command")
        if self._error:
            raise self._error
        assert self._result is not None
        return self._result

    async def _request_iter(self) -> AsyncIterator[pb.RunRequest]:
        yield self._start_request
        while True:
            item = await self._outbound.get()
            if item is None:
                return
            yield item

    async def _pump(self) -> None:
        try:
            loop = asyncio.get_running_loop()
            deadline = loop.time() + _START_TIMEOUT
            attempt = 0
            while True:
                rpc = await self._sandbox._ensure_data_plane(verify_serving=False)
                if rpc is None:
                    raise RuntimeError("sandbox data-plane endpoint unavailable")
                received_response = False
                try:
                    async for frame in rpc.run(self._request_iter()):
                        received_response = True
                        self._sandbox._mark_data_plane_serving(rpc)
                        self._start_replayable = False
                        kind = frame.WhichOneof("payload")
                        if kind == "started":
                            self._pid = int(frame.started.pid)
                            self._started.set()
                        elif kind == "stdout":
                            self.stdout.push(bytes(frame.stdout))
                        elif kind == "stderr":
                            self.stderr.push(bytes(frame.stderr))
                        elif kind == "exit":
                            exit = frame.exit
                            if exit.reason.startswith("first_frame_timeout"):
                                raise PrimitiveTimeoutError(exit.reason)
                            if exit.reason.startswith("capability_unavailable"):
                                raise CapabilityUnavailableError(exit.reason)
                            if is_session_terminated_reason(exit.reason):
                                raise SessionTerminatedError(
                                    exit.reason,
                                    termination_cause=session_termination_cause(exit.reason),
                                )
                            self._result = CommandResult(
                                argv=self.argv,
                                exit_code=exit.exit_code,
                                stdout=self.stdout.read(),
                                stderr=self.stderr.read(),
                                signal=exit.signal or None,
                                duration_ms=int(exit.duration_ms),
                                reason=exit.reason or None,
                                errno=exit.errno or None,
                                timed_out=bool(exit.timed_out),
                                disk=_disk_usage_from_exit(exit),
                            )
                            return
                    raise RuntimeError("sandbox run stream closed before exit")
                except Exception as err:
                    provisioning_pending = self._sandbox._data_plane_provisioning_pending(rpc)
                    if received_response or not _is_retryable_run_establishment_error(
                        err, provisioning_pending
                    ):
                        raise
                    if _is_provisioning_session_not_found(err, provisioning_pending):
                        await self._sandbox._refresh_hinted_data_plane(rpc)
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        raise
                    if attempt > 0:
                        await asyncio.sleep(min(0.05 * attempt, 0.75, remaining))
                    if not self._start_replayable:
                        raise
                    attempt += 1
        except Exception as err:
            self._error = (
                map_rpc_error(err)
                if not isinstance(
                    err,
                    (CapabilityUnavailableError, PrimitiveTimeoutError, SessionTerminatedError),
                )
                else err
            )
            self.stdout.fail(self._error)
            self.stderr.fail(self._error)
        finally:
            self._started.set()
            self._done.set()
            self.stdout.close()
            self.stderr.close()
            self._outbound.put_nowait(None)

    async def _wait_started(self) -> None:
        # Gate before sending stdin/signals: raise on timeout or start error.
        if not await self._await_started():
            raise TimeoutError("timed out waiting for command start")
        if self._error:
            raise self._error
