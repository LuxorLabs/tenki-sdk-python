from __future__ import annotations

import grpc
import pytest

from tenki_sandbox import AsyncClient, CommandFailedError
from tenki_sandbox import errors
from tenki_sandbox._pb import pb

from _async_fakes import FakeControlRPC


class FakeDataPlaneRPC(FakeControlRPC):
    """Serves both control-plane and data-plane RPCs (async transport contract)."""

    def __init__(self):
        super().__init__()
        self.files: dict[str, bytes] = {}
        self.run_starts = []
        self.next_exit_code = 0
        self.followups: list[str] = []
        self.stdin_data = b""

    def run(self, requests):
        # Yield output immediately without draining stdin (matches the sync base
        # FakeRPC); a stdin-close may never arrive for start()-without-input.
        async def gen():
            first = await requests.__anext__()
            self.run_starts.append(first.start)
            yield pb.RunResponse(started=pb.RunStarted(pid=42))
            yield pb.RunResponse(stdout=b"hello\n")
            yield pb.RunResponse(stderr=b"warn\n")
            yield pb.RunResponse(exit=pb.RunExit(exit_code=self.next_exit_code, duration_ms=12, reason="exit"))

        return gen()

    async def write_file(self, req):
        self.files[req.path] = bytes(req.content)
        return pb.WriteFileResponse(path=req.path, size_bytes=len(req.content))

    async def read_file(self, req):
        return pb.ReadFileResponse(path=req.path, content=self.files[req.path])

    def read_file_stream(self, req):
        data = self.files[req.path]

        async def gen():
            yield pb.ReadFileStreamResponse(data=data[:1])
            yield pb.ReadFileStreamResponse(data=data[1:], eof=True, total_size=len(data))

        return gen()

    async def write_file_stream(self, requests):
        path = None
        buf = b""
        async for frame in requests:
            kind = frame.WhichOneof("payload")
            if kind == "start":
                path = frame.start.path
            elif kind == "data":
                buf += bytes(frame.data)
        if path is not None:
            self.files[path] = buf
        return pb.WriteFileStreamResponse()

    async def stat(self, req):
        if req.path not in self.files:
            return pb.StatResponse(exists=False)
        return pb.StatResponse(exists=True, size=len(self.files[req.path]), mode=0o644, is_dir=False)

    async def mkdir(self, req):
        return pb.MkdirResponse()

    async def remove(self, req):
        return pb.RemoveResponse()

    async def list(self, req):
        return pb.ListResponse(entries=[pb.ListEntry(name="a.txt", size=3, mode=0o644)])


async def _sandbox(rpc):
    sandbox = await AsyncClient(rpc=rpc).create(wait=False)
    sandbox._data_plane_rpc = rpc  # bypass live data-plane bootstrap
    return sandbox


async def test_async_exec_collects_output_and_check():
    rpc = FakeDataPlaneRPC()
    sandbox = await _sandbox(rpc)

    result = await sandbox.exec("python3", "-c", "print(1)", check=True)
    assert result.ok
    assert result.stdout_text == "hello\n"
    assert result.stderr_text == "warn\n"
    assert list(rpc.run_starts[-1].cmd) == ["python3", "-c", "print(1)"]

    rpc.next_exit_code = 2
    with pytest.raises(CommandFailedError):
        await sandbox.exec("false", check=True)


async def test_async_start_rejects_relative_cwd():
    rpc = FakeDataPlaneRPC()
    sandbox = await _sandbox(rpc)
    with pytest.raises(ValueError, match="absolute path"):
        await sandbox.start("python3", cwd="www")
    assert not rpc.run_starts


async def test_async_pid_is_lenient_on_timeout(monkeypatch):
    # pid() must return None (never raise) when the start ack never arrives,
    # matching sync's best-effort pid(); _wait_started keeps raising.
    from tenki_sandbox.aio import process as proc_mod

    monkeypatch.setattr(proc_mod, "_START_TIMEOUT", 0.01)
    proc = proc_mod.AsyncProcess("sess-1", ["true"])  # constructed, never started
    assert await proc.pid() is None
    with pytest.raises(TimeoutError):
        await proc._wait_started()


async def test_async_start_streams_output_and_pid():
    rpc = FakeDataPlaneRPC()
    sandbox = await _sandbox(rpc)
    proc = await sandbox.start("echo", "x")
    assert await proc.pid() == 42
    chunks = [chunk async for chunk in proc.stdout]
    assert b"".join(chunks) == b"hello\n"
    assert (await proc.wait()).exit_code == 0


async def test_async_start_retries_transient_stream_establishment_reset():
    rpc = ResetOnFirstRunRPC()
    sandbox = await _sandbox(rpc)

    proc = await sandbox.start("true")

    assert (await proc.wait()).ok
    assert await proc.pid() == 42
    assert rpc.run_calls == 2


async def test_async_start_does_not_retry_after_first_response():
    rpc = ResetAfterStartedRPC()
    sandbox = await _sandbox(rpc)

    proc = await sandbox.start("true")

    with pytest.raises(errors.SandboxError, match="ECONNRESET"):
        await proc.wait()
    assert rpc.run_calls == 1


class TransientRunError(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.ABORTED

    def details(self):
        return "read ECONNRESET"

    def __str__(self):
        return self.details()


class ResetOnFirstRunRPC(FakeDataPlaneRPC):
    def __init__(self):
        super().__init__()
        self.run_calls = 0

    def run(self, requests):
        async def gen():
            first = await requests.__anext__()
            self.run_starts.append(first.start)
            self.run_calls += 1
            if self.run_calls == 1:
                raise TransientRunError()
            yield pb.RunResponse(started=pb.RunStarted(pid=42))
            yield pb.RunResponse(exit=pb.RunExit(exit_code=0, duration_ms=12, reason="exit"))

        return gen()


class ResetAfterStartedRPC(FakeDataPlaneRPC):
    def __init__(self):
        super().__init__()
        self.run_calls = 0

    def run(self, requests):
        async def gen():
            first = await requests.__anext__()
            self.run_starts.append(first.start)
            self.run_calls += 1
            yield pb.RunResponse(started=pb.RunStarted(pid=42))
            raise TransientRunError()

        return gen()


class StdinDrainRPC(FakeDataPlaneRPC):
    """Drains stdin frames (sent only after the started ack) before exiting."""

    def run(self, requests):
        async def gen():
            first = await requests.__anext__()
            self.run_starts.append(first.start)
            yield pb.RunResponse(started=pb.RunStarted(pid=42))
            async for frame in requests:
                kind = frame.WhichOneof("payload")
                self.followups.append(kind)
                if kind == "stdin":
                    self.stdin_data += bytes(frame.stdin)
                if kind == "stdin_close":
                    break
            yield pb.RunResponse(exit=pb.RunExit(exit_code=0, duration_ms=12, reason="exit"))

        return gen()


async def test_async_stdin_delivered_after_started():
    rpc = StdinDrainRPC()
    sandbox = await _sandbox(rpc)
    result = await sandbox.exec("cat", input="hello")
    assert result.ok
    assert rpc.stdin_data == b"hello"
    assert rpc.followups == ["stdin", "stdin_close"]


async def test_async_filesystem_helpers():
    rpc = FakeDataPlaneRPC()
    sandbox = await _sandbox(rpc)

    await sandbox.fs.write_text("/workspace/a.txt", "abc")
    assert rpc.files["/workspace/a.txt"] == b"abc"
    assert await sandbox.fs.read_text("/workspace/a.txt") == "abc"

    chunks = [c async for c in sandbox.fs.read_stream("/workspace/a.txt")]
    assert chunks == [b"a", b"bc"]

    info = await sandbox.fs.stat("/workspace/a.txt")
    assert info.size == 3
    entries = await sandbox.fs.list("/workspace")
    assert entries[0].path == "a.txt"


async def test_async_write_stream_reassembles():
    rpc = FakeDataPlaneRPC()
    sandbox = await _sandbox(rpc)
    await sandbox.fs.write_stream("/workspace/b.txt", [b"foo", b"bar"])
    assert rpc.files["/workspace/b.txt"] == b"foobar"


async def test_async_stat_missing_raises_file_not_found():
    rpc = FakeDataPlaneRPC()
    sandbox = await _sandbox(rpc)
    with pytest.raises(errors.FileNotFoundError):
        await sandbox.fs.stat("/workspace/missing.txt")
