from __future__ import annotations

import queue
import threading
import time
from typing import Iterable, Iterator, Sequence

import grpc

from ._pb import pb
from ._time import duration_ms
from .errors import (
    CapabilityUnavailableError,
    PrimitiveTimeoutError,
    SessionTerminatedError,
    is_session_terminated_reason,
    map_rpc_error,
    session_termination_cause,
)
from .disk import DiskUsage
from .models import CommandResult

_RUN_ESTABLISHMENT_TIMEOUT = 30.0


def _stringifies_to_a_repr(value: object) -> bool:
    """Whether str(value) falls back to a repr instead of something meant as argv.

    A type owning its own __str__ renders to something the caller plausibly
    intended (``bytes``, ``UserString``, an int, a ``Path``), so it keeps the
    historical coercion. Every other iterable -- list, tuple, set, dict, deque,
    generator, map -- renders as a repr that can never name a binary, which is
    the silent corruption this guards against.
    """
    if isinstance(value, str):
        return False
    return isinstance(value, Iterable) and type(value).__str__ is object.__str__


def normalize_argv(argv: Sequence[object]) -> list[str]:
    """Resolve the varargs of exec/start into the argv the guest should run.

    A lone list or tuple is the argv itself, so `exec(["sh", "-c", cmd])` means
    the same as `exec("sh", "-c", cmd)`. Any other iterable is ambiguous -- a set
    has no argv order, an iterator cannot be replayed on reconnect -- and is
    rejected rather than stringified into a binary name that cannot resolve.
    """
    if len(argv) == 1 and isinstance(argv[0], (list, tuple)):
        argv = tuple(argv[0])
    if not argv or not argv[0]:
        raise ValueError("empty command")
    parts: list[str] = []
    for index, part in enumerate(argv):
        if _stringifies_to_a_repr(part):
            raise TypeError(
                f"argv[{index}] must be a string, got {type(part).__name__}: "
                f"{part!r}; pass argv as a list of strings"
            )
        parts.append(str(part))
    return parts



def _disk_usage_from_exit(exit: object) -> DiskUsage | None:
    """Maps the guest exit frame's disk fields, absent on older guests."""
    total = int(getattr(exit, "disk_total_bytes", 0) or 0)
    if total <= 0:
        return None
    return DiskUsage(
        total_bytes=total,
        available_bytes=int(getattr(exit, "disk_available_bytes", 0) or 0),
        min_available_bytes=int(getattr(exit, "disk_min_available_bytes", 0) or 0),
        free_bytes=int(getattr(exit, "disk_free_bytes", 0) or 0),
    )


class _StaticProcessSandbox:
    def __init__(self, rpc) -> None:
        self._rpc = rpc

    def _ensure_data_plane(self, *, verify_serving: bool = True):
        return self._rpc

    def _mark_data_plane_serving(self, rpc) -> None:
        return None

    def _data_plane_provisioning_pending(self, rpc) -> bool:
        return False

    def _refresh_hinted_data_plane(self, rpc) -> None:
        return None


def _rpc_error_message(err: Exception) -> str:
    details = getattr(err, "details", None)
    if callable(details):
        try:
            return f"{err} {details()}".lower()
        except Exception:
            pass
    return str(err).lower()


def _is_provisioning_session_not_found(err: Exception, provisioning_pending: bool) -> bool:
    if not provisioning_pending:
        return False
    get_code = getattr(err, "code", None)
    code = get_code() if callable(get_code) else None
    return code == grpc.StatusCode.NOT_FOUND and "session not found" in _rpc_error_message(err)


def _is_retryable_run_establishment_error(
    err: Exception, provisioning_pending: bool = False
) -> bool:
    if _is_provisioning_session_not_found(err, provisioning_pending):
        return True
    candidate: BaseException | None = err
    seen: set[int] = set()
    for _ in range(4):
        if candidate is None or id(candidate) in seen:
            break
        seen.add(id(candidate))
        message = _rpc_error_message(candidate)
        code = None
        get_code = getattr(candidate, "code", None)
        if callable(get_code):
            try:
                code = get_code()
            except Exception:
                pass
        code_name = getattr(code, "name", str(code))
        if code_name == "UNIMPLEMENTED" and "404" in message:
            return True
        if provisioning_pending and code == grpc.StatusCode.UNAVAILABLE:
            return True
        if any(token in message for token in ("refused_stream", "refused stream", "goaway")):
            return True
        if code in (grpc.StatusCode.ABORTED, grpc.StatusCode.UNAVAILABLE) and any(
            token in message
            for token in (
                "econnreset",
                "connection reset",
                "handshake read failed",
                "socket hang up",
                "unexpected eof",
                "stream closed before headers",
            )
        ):
            return True
        candidate = candidate.__cause__ or candidate.__context__
    return False


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
        sandbox,
        session_id: str,
        argv: Sequence[object],
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout: float | int | object | None = None,
        stdin: bytes | str | None = None,
        privileged: bool = False,
    ) -> None:
        self.argv = normalize_argv(argv)
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
        self._replay_lock = threading.Lock()
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

        self._sandbox = (
            sandbox
            if hasattr(sandbox, "_ensure_data_plane")
            else _StaticProcessSandbox(sandbox)
        )
        self._thread = threading.Thread(target=self._pump, daemon=True)
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
        with self._replay_lock:
            self._start_replayable = False
            self._outbound.put(pb.RunRequest(signal=pb.RunSignal(signal=enum_value)))

    def wait(self, timeout: float | None = None) -> CommandResult:
        """Block until the command finishes.

        ``timeout`` bounds how long this call blocks, not the command itself: on
        expiry the process is killed and ``TimeoutError`` is raised. The command's
        own execution budget is ``Sandbox.exec(timeout=...)``, which the
        guest-agent enforces and reports as ``CommandResult.timed_out``.
        """
        if not self._done.wait(timeout=timeout):
            self.kill()
            raise TimeoutError("timed out waiting for command")
        if self._error:
            raise self._error
        assert self._result is not None
        return self._result

    def _request_iter(self) -> Iterator[pb.RunRequest]:
        yield self._start_request
        while True:
            item = self._outbound.get()
            if item is None:
                return
            yield item

    def _pump(self) -> None:
        try:
            deadline = time.monotonic() + _RUN_ESTABLISHMENT_TIMEOUT
            attempt = 0
            while True:
                rpc = self._sandbox._ensure_data_plane(verify_serving=False)
                if rpc is None:
                    raise RuntimeError("sandbox data-plane endpoint unavailable")
                received_response = False
                try:
                    for frame in rpc.run(self._request_iter()):
                        received_response = True
                        self._sandbox._mark_data_plane_serving(rpc)
                        self._disable_start_replay()
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
                        self._sandbox._refresh_hinted_data_plane(rpc)
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise
                    if attempt > 0:
                        time.sleep(min(0.05 * attempt, 0.75, remaining))
                    if not self._can_replay_start():
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
            self._outbound.put(None)

    def _wait_started(self) -> None:
        if not self._started.wait(timeout=30):
            raise TimeoutError("timed out waiting for command start")
        if self._error:
            raise self._error

    def _disable_start_replay(self) -> None:
        with self._replay_lock:
            self._start_replayable = False

    def _can_replay_start(self) -> bool:
        with self._replay_lock:
            return self._start_replayable

    def _close_stdin_now(self) -> None:
        with self._stdin_lock:
            if self._stdin_closed:
                return
            self._stdin_closed = True
        self._outbound.put(pb.RunRequest(stdin_close=True))
