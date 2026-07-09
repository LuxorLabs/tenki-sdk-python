from __future__ import annotations

import threading

import pytest

from tenki_sandbox import Client, CommandFailedError
from tenki_sandbox import errors
from tenki_sandbox._pb import pb


def test_exec_collects_stdout_stderr_and_check_raises():
    rpc = FakeRPC()
    sandbox = create_test_sandbox(rpc)

    result = sandbox.exec("python3", "-c", "print(1)", check=True)

    assert result.ok
    assert result.stdout_text == "hello\n"
    assert result.stderr_text == "warn\n"
    start = rpc.run_starts[-1]
    assert list(start.cmd) == ["python3", "-c", "print(1)"]
    assert start.stream_stdin is True

    rpc.next_exit_code = 2
    with pytest.raises(CommandFailedError):
        sandbox.exec("false", check=True)


def test_start_rejects_relative_cwd():
    rpc = FakeRPC()
    sandbox = create_test_sandbox(rpc)

    with pytest.raises(ValueError, match="absolute path"):
        sandbox.start("python3", "-m", "http.server", cwd="www")
    with pytest.raises(ValueError, match="absolute path"):
        sandbox.exec("ls", cwd="project/dir")
    assert not rpc.run_starts


def test_start_exposes_live_output_and_pid():
    rpc = FakeRPC()
    sandbox = create_test_sandbox(rpc)

    proc = sandbox.start("echo", "x")

    assert proc.pid == 42
    assert b"".join(proc.stdout) == b"hello\n"
    assert proc.wait().exit_code == 0


def test_stdin_frames_wait_for_started_ack():
    rpc = OrderingRPC()
    sandbox = create_test_sandbox(rpc)

    result = sandbox.exec("cat", input="hello")

    assert result.ok
    assert rpc.followup_before_started is False
    assert rpc.followups == ["stdin", "stdin_close"]


def test_exec_without_input_closes_stdin():
    rpc = CloseStdinRPC()
    sandbox = create_test_sandbox(rpc)

    result = sandbox.exec("cat")

    assert result.ok
    assert rpc.followups == ["stdin_close"]


def test_signal_after_close_stdin_still_sends_signal():
    rpc = SignalAfterCloseRPC()
    sandbox = create_test_sandbox(rpc)

    proc = sandbox.start("cat")
    proc.close_stdin()
    proc.signal("TERM")

    assert proc.wait().ok
    assert rpc.followups == ["stdin_close", "signal"]
    assert rpc.signal_value == pb.RunSignal.SIG_TERM
    assert rpc.request_closed_before_signal is False


def test_filesystem_helpers_map_to_rpc():
    rpc = FakeRPC()
    sandbox = create_test_sandbox(rpc)

    sandbox.fs.write_text("/workspace/a.txt", "abc")
    assert rpc.files["/workspace/a.txt"] == b"abc"
    assert sandbox.fs.read_text("/workspace/a.txt") == "abc"
    assert list(sandbox.fs.read_stream("/workspace/a.txt")) == [b"a", b"bc"]

    info = sandbox.fs.stat("/workspace/a.txt")
    assert info.size == 3
    assert sandbox.fs.list("/workspace")[0].path == "a.txt"


def test_stat_missing_path_raises_file_not_found():
    rpc = FakeRPC()
    client = Client(rpc=rpc)
    sandbox = client.create(wait=False)

    with pytest.raises(errors.FileNotFoundError):
        sandbox.fs.stat("/workspace/missing.txt")


def test_map_rpc_error_classifies_enoent_not_found():
    class FakeRpcError(Exception):
        def code(self):
            class C:
                name = "NOT_FOUND"

            return C()

        def details(self):
            return "open /home/tenki/missing: no such file or directory"

    mapped = errors.map_rpc_error(FakeRpcError())
    assert isinstance(mapped, errors.FileNotFoundError)
    # Handlers written for the builtin must catch the SDK error too.
    import builtins

    assert isinstance(mapped, builtins.FileNotFoundError)
    assert isinstance(mapped, OSError)


def test_preview_urls_and_expose_port_keep_optionals_absent_when_unspecified():
    rpc = FakeRPC()
    client = Client(rpc=rpc)
    sandbox = client.create(wait=False)

    preview = client.preview_urls.create("proj-1", "demo")
    assert preview.url == "https://demo.sandbox.tenki.test"
    create_req = rpc.preview_create_requests[-1]
    assert not create_req.HasField("session_id")
    assert not create_req.HasField("port")

    exposed = sandbox.expose_port(3000, slug="demo")
    assert exposed.url == "https://demo.sandbox.tenki.test"
    assert rpc.expose_requests[-1].slug == "demo"


class FakeRPC:
    def __init__(self):
        self.files = {}
        self.run_starts = []
        self.next_exit_code = 0
        self.preview_create_requests = []
        self.expose_requests = []

    def create_session(self, req):
        return pb.CreateSessionResponse(session=_session())

    def terminate_session(self, req):
        return pb.TerminateSessionResponse(session=_session(pb.SESSION_STATE_TERMINATED))

    def run(self, requests):
        first = next(requests)
        self.run_starts.append(first.start)
        yield pb.RunResponse(started=pb.RunStarted(pid=42))
        yield pb.RunResponse(stdout=b"hello\n")
        yield pb.RunResponse(stderr=b"warn\n")
        yield pb.RunResponse(exit=pb.RunExit(exit_code=self.next_exit_code, duration_ms=12, reason="exit"))

    def write_file(self, req):
        self.files[req.path] = bytes(req.content)
        return pb.WriteFileResponse(path=req.path, size_bytes=len(req.content))

    def read_file(self, req):
        return pb.ReadFileResponse(path=req.path, content=self.files[req.path])

    def read_file_stream(self, req):
        data = self.files[req.path]
        yield pb.ReadFileStreamResponse(data=data[:1])
        yield pb.ReadFileStreamResponse(data=data[1:], eof=True, total_size=len(data))

    def stat(self, req):
        if req.path not in self.files:
            return pb.StatResponse(exists=False)
        return pb.StatResponse(exists=True, size=len(self.files[req.path]), mode=0o644, is_dir=False)

    def list(self, req):
        return pb.ListResponse(entries=[pb.ListEntry(name="a.txt", size=3, mode=0o644)])

    def create_preview_url(self, req):
        self.preview_create_requests.append(req)
        return pb.CreatePreviewUrlResponse(
            preview_url=pb.PreviewUrl(
                id="prev-1",
                project_id=req.project_id,
                workspace_id="ws-1",
                owner_id="owner-1",
                slug=req.slug,
                token="tok",
                preview_url=f"https://{req.slug}.sandbox.tenki.test",
            )
        )

    def expose_port(self, req):
        self.expose_requests.append(req)
        return pb.ExposePortResponse(port=req.port, preview_url=f"https://{req.slug}.sandbox.tenki.test", slug=req.slug)


class OrderingRPC(FakeRPC):
    def __init__(self):
        super().__init__()
        self.followup_before_started = False
        self.followups = []

    def run(self, requests):
        first = next(requests)
        self.run_starts.append(first.start)

        pending = []
        read_next = threading.Thread(target=lambda: pending.append(next(requests)), daemon=True)
        read_next.start()
        read_next.join(0.05)
        self.followup_before_started = bool(pending)

        yield pb.RunResponse(started=pb.RunStarted(pid=42))

        read_next.join(1)
        if pending:
            self.followups.append(pending[0].WhichOneof("payload"))
        self.followups.append(next(requests).WhichOneof("payload"))
        yield pb.RunResponse(exit=pb.RunExit(exit_code=0, duration_ms=12, reason="exit"))


class CloseStdinRPC(FakeRPC):
    def __init__(self):
        super().__init__()
        self.followups = []

    def run(self, requests):
        first = next(requests)
        self.run_starts.append(first.start)

        yield pb.RunResponse(started=pb.RunStarted(pid=42))

        self.followups.append(next(requests).WhichOneof("payload"))
        yield pb.RunResponse(exit=pb.RunExit(exit_code=0, duration_ms=12, reason="exit"))


class SignalAfterCloseRPC(FakeRPC):
    def __init__(self):
        super().__init__()
        self.followups = []
        self.signal_value = None
        self.request_closed_before_signal = False

    def run(self, requests):
        first = next(requests)
        self.run_starts.append(first.start)

        yield pb.RunResponse(started=pb.RunStarted(pid=42))

        self.followups.append(next(requests).WhichOneof("payload"))
        try:
            signal_req = next(requests)
        except StopIteration:
            self.request_closed_before_signal = True
        else:
            self.followups.append(signal_req.WhichOneof("payload"))
            self.signal_value = signal_req.signal.signal

        yield pb.RunResponse(exit=pb.RunExit(exit_code=0, duration_ms=12, reason="exit"))


def _session(state=pb.SESSION_STATE_RUNNING):
    return pb.SandboxSession(
        id="session-1",
        state=state,
        owner_type="SERVICE",
        owner_id="self",
        name="test",
        project_id="proj-1",
        workspace_id="ws-1",
    )


def create_test_sandbox(rpc):
    sandbox = Client(rpc=rpc).create(wait=False)
    sandbox._data_plane_rpc = rpc
    sandbox.fs._rpc = rpc
    return sandbox
