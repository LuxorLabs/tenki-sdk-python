from __future__ import annotations

import queue
import threading
from typing import Iterable, Iterator, Sequence

from ._pb import pb
from ._time import duration_ms
from .errors import CapabilityUnavailableError, PrimitiveTimeoutError, map_rpc_error
from .models import CommandResult


class ProcessStream(Iterable[bytes]):
    def __init__(self) -> None:
        self._queue: queue.Queue[bytes | Exception | None] = queue.Queue()
        self._chunks: list[bytes] = []

    def push(self, data: bytes) -> None:
        if data:
            self._chunks.append(data)
            self._queue.put(data)

    def close(self) -> None:
        self._queue.put(None)

    def fail(self, err: Exception) -> None:
        self._queue.put(err)

    def __iter__(self) -> Iterator[bytes]:
        while True:
            item = self._queue.get()
            if item is None:
                return
            if isinstance(item, Exception):
                raise item
            yield item

    def read(self) -> bytes:
        return b"".join(self._chunks)

    def read_text(self, encoding: str = "utf-8") -> str:
        return self.read().decode(encoding, errors="replace")


class Process:
    def __init__(
        self,
        rpc,
        session_id: str,
        argv: Sequence[str],
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout: float | int | object | None = None,
        stdin: bytes | str | None = None,
        privileged: bool = False,
    ) -> None:
        if not argv or not argv[0]:
            raise ValueError("empty command")
        self.argv = [str(part) for part in argv]
        self.stdout = ProcessStream()
        self.stderr = ProcessStream()
        self._outbound: queue.Queue[pb.RunRequest | None] = queue.Queue()
        self._started = threading.Event()
        self._done = threading.Event()
        self._pid: int | None = None
        self._result: CommandResult | None = None
        self._error: Exception | None = None
        self._stdin_closed = False
        self._stdin_lock = threading.Lock()

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
        self._outbound.put(start)

        self._thread = threading.Thread(target=self._pump, args=(rpc,), daemon=True)
        self._thread.start()
        if stdin is not None:
            self.write_stdin(stdin)
            self.close_stdin()

    @property
    def pid(self) -> int | None:
        self._started.wait(timeout=30)
        return self._pid

    def write_stdin(self, data: bytes | str) -> None:
        self._wait_started()
        payload = data.encode() if isinstance(data, str) else data
        self._outbound.put(pb.RunRequest(stdin=payload))

    def close_stdin(self) -> None:
        self._wait_started()
        self._close_stdin_now()

    def signal(self, signal: str) -> None:
        self._wait_started()
        name = signal.upper().removeprefix("SIG")
        enum_name = f"SIG_{name}"
        enum_value = getattr(pb.RunSignal, enum_name, None)
        if enum_value is None:
            raise ValueError(f"unsupported signal: {signal}")
        self._outbound.put(pb.RunRequest(signal=pb.RunSignal(signal=enum_value)))

    def kill(self) -> None:
        enum_value = getattr(pb.RunSignal, "SIG_KILL")
        self._outbound.put(pb.RunRequest(signal=pb.RunSignal(signal=enum_value)))

    def wait(self, timeout: float | None = None) -> CommandResult:
        if not self._done.wait(timeout=timeout):
            self.kill()
            raise TimeoutError("timed out waiting for command")
        if self._error:
            raise self._error
        assert self._result is not None
        return self._result

    def _request_iter(self) -> Iterator[pb.RunRequest]:
        while True:
            item = self._outbound.get()
            if item is None:
                return
            yield item

    def _pump(self, rpc) -> None:
        try:
            for frame in rpc.run(self._request_iter()):
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
            self._error = map_rpc_error(err) if not isinstance(err, (CapabilityUnavailableError, PrimitiveTimeoutError)) else err
            self.stdout.fail(self._error)
            self.stderr.fail(self._error)
        finally:
            self._started.set()
            self._done.set()
            self.stdout.close()
            self.stderr.close()
            self._outbound.put(None)

    def _wait_started(self) -> None:
        if not self._started.wait(timeout=30):
            raise TimeoutError("timed out waiting for command start")
        if self._error:
            raise self._error

    def _close_stdin_now(self) -> None:
        with self._stdin_lock:
            if self._stdin_closed:
                return
            self._stdin_closed = True
        self._outbound.put(pb.RunRequest(stdin_close=True))
