from __future__ import annotations

import asyncio
from typing import AsyncIterator, Sequence

from .._pb import pb
from .._time import duration_ms
from ..errors import CapabilityUnavailableError, PrimitiveTimeoutError, map_rpc_error
from ..models import CommandResult

# Seconds to wait for the guest to ack process start before giving up.
_START_TIMEOUT = 30


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
        argv: Sequence[str],
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout: float | int | object | None = None,
        privileged: bool = False,
    ) -> None:
        if not argv or not argv[0]:
            raise ValueError("empty command")
        self.argv = [str(part) for part in argv]
        self.stdout = AsyncProcessStream()
        self.stderr = AsyncProcessStream()
        self._outbound: asyncio.Queue = asyncio.Queue()
        self._started = asyncio.Event()
        self._done = asyncio.Event()
        self._pid: int | None = None
        self._result: CommandResult | None = None
        self._error: Exception | None = None
        self._pump_task: asyncio.Task | None = None

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
        self._outbound.put_nowait(start)

    @classmethod
    async def start(
        cls,
        rpc,
        session_id: str,
        argv: Sequence[str],
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout: float | int | object | None = None,
        stdin: bytes | str | None = None,
        privileged: bool = False,
    ) -> "AsyncProcess":
        proc = cls(session_id, argv, cwd=cwd, env=env, timeout=timeout, privileged=privileged)
        proc._pump_task = asyncio.create_task(proc._pump(rpc))
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
        await self._outbound.put(pb.RunRequest(signal=pb.RunSignal(signal=enum_value)))

    async def wait(self, timeout: float | None = None) -> CommandResult:
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
        while True:
            item = await self._outbound.get()
            if item is None:
                return
            yield item

    async def _pump(self, rpc) -> None:
        try:
            async for frame in rpc.run(self._request_iter()):
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
                    self._result = CommandResult(
                        argv=self.argv,
                        exit_code=exit.exit_code,
                        stdout=self.stdout.read(),
                        stderr=self.stderr.read(),
                        signal=exit.signal or None,
                        duration_ms=int(exit.duration_ms),
                        reason=exit.reason or None,
                        errno=exit.errno or None,
                    )
                    return
            raise RuntimeError("sandbox run stream closed before exit")
        except Exception as err:
            self._error = (
                map_rpc_error(err)
                if not isinstance(err, (CapabilityUnavailableError, PrimitiveTimeoutError))
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
