from __future__ import annotations

import threading

import grpc
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


def test_start_passes_cwd_through():
    rpc = FakeRPC()
    sandbox = create_test_sandbox(rpc)

    proc = sandbox.start("python3", "-m", "http.server", cwd="www")
    assert proc.wait().ok
    result = sandbox.exec("ls", cwd="/home/tenki/project")
    assert result.ok

    assert rpc.run_starts[0].cwd == "www"
    assert rpc.run_starts[1].cwd == "/home/tenki/project"


def test_exec_missing_cwd_maps_file_not_found_and_passes_through():
    rpc = CwdErrorRPC(
        {
            "missing": RunRpcError(
                grpc.StatusCode.NOT_FOUND,
                "cwd /home/tenki/missing: no such file or directory",
            )
        }
    )
    sandbox = create_test_sandbox(rpc)

    with pytest.raises(errors.FileNotFoundError):
        sandbox.exec("pwd", cwd="missing")

    assert rpc.run_starts[0].cwd == "missing"


def test_exec_traversal_cwd_passes_through_and_surfaces_error():
    rpc = CwdErrorRPC(
        {
            "../outside": RunRpcError(
                grpc.StatusCode.INVALID_ARGUMENT,
                "cwd ../outside: invalid argument",
            )
        }
    )
    sandbox = create_test_sandbox(rpc)

    with pytest.raises(errors.SandboxError, match="invalid argument"):
        sandbox.exec("pwd", cwd="../outside")

    assert rpc.run_starts[0].cwd == "../outside"


def test_start_exposes_live_output_and_pid():
    rpc = FakeRPC()
    sandbox = create_test_sandbox(rpc)

    proc = sandbox.start("echo", "x")

    assert proc.pid == 42
    assert b"".join(proc.stdout) == b"hello\n"
    assert proc.wait().exit_code == 0


def test_start_retries_transient_stream_establishment_reset():
    rpc = ResetOnFirstRunRPC()
    sandbox = create_test_sandbox(rpc)

    proc = sandbox.start("true")

    assert proc.wait().ok
    assert proc.pid == 42
    assert rpc.run_calls == 2


def test_start_does_not_retry_after_first_response():
    rpc = ResetAfterStartedRPC()
    sandbox = create_test_sandbox(rpc)

    proc = sandbox.start("true")

    with pytest.raises(errors.SandboxError, match="ECONNRESET"):
        proc.wait()
    assert rpc.run_calls == 1


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
    sandbox = client.create(workspace_id="ws-1", wait=False)

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
    sandbox = client.create(workspace_id="ws-1", wait=False)

    preview = client.preview_urls.create("demo")
    assert preview.url == "https://demo.sandbox.tenki.test"
    create_req = rpc.preview_create_requests[-1]
    assert create_req.workspace_id == ""
    assert not create_req.HasField("session_id")
    assert not create_req.HasField("port")

    exposed = sandbox.expose_port(3000, slug="demo")
    assert exposed.url == "https://demo.sandbox.tenki.test"
    assert rpc.expose_requests[-1].slug == "demo"


def test_preview_urls_accept_explicit_service_token_workspace():
    rpc = FakeRPC()
    client = Client(rpc=rpc)

    client.preview_urls.create("demo", workspace_id="  workspace-1  ")

    assert rpc.preview_create_requests[-1].workspace_id == "workspace-1"


def test_preview_url_list_drains_every_page():
    class PreviewListRPC:
        def __init__(self):
            self.requests = []

        def list_preview_urls(self, req):
            self.requests.append(req)
            item = pb.PreviewUrl(id=f"preview-{len(self.requests)}")
            return pb.ListPreviewUrlsResponse(
                preview_urls=[item],
                next_page_token="next" if not req.page_token else "",
            )

    rpc = PreviewListRPC()
    preview_urls = Client(rpc=rpc).preview_urls.list()

    assert [item.id for item in preview_urls] == ["preview-1", "preview-2"]
    assert [req.page_token for req in rpc.requests] == ["", "next"]
    assert all(req.page_size == 100 for req in rpc.requests)


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
                workspace_id=req.workspace_id,
                owner_id="owner-1",
                slug=req.slug,
                token="tok",
                preview_url=f"https://{req.slug}.sandbox.tenki.test",
            )
        )

    def expose_port(self, req):
        self.expose_requests.append(req)
        return pb.ExposePortResponse(port=req.port, preview_url=f"https://{req.slug}.sandbox.tenki.test", slug=req.slug)


class RunRpcError(grpc.RpcError):
    def __init__(self, code, details):
        super().__init__()
        self._code = code
        self._details = details

    def code(self):
        return self._code

    def details(self):
        return self._details

    def __str__(self):
        return self._details


class CwdErrorRPC(FakeRPC):
    def __init__(self, errors_by_cwd):
        super().__init__()
        self.errors_by_cwd = errors_by_cwd

    def run(self, requests):
        first = next(requests)
        self.run_starts.append(first.start)
        err = self.errors_by_cwd.get(first.start.cwd)
        if err is not None:
            raise err
        yield from super().run(iter([first]))


class TransientRunError(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.ABORTED

    def details(self):
        return "read ECONNRESET"

    def __str__(self):
        return self.details()


class ResetOnFirstRunRPC(FakeRPC):
    def __init__(self):
        super().__init__()
        self.run_calls = 0

    def run(self, requests):
        first = next(requests)
        self.run_starts.append(first.start)
        self.run_calls += 1
        if self.run_calls == 1:
            raise TransientRunError()
        yield pb.RunResponse(started=pb.RunStarted(pid=42))
        yield pb.RunResponse(exit=pb.RunExit(exit_code=0, duration_ms=12, reason="exit"))


class ResetAfterStartedRPC(FakeRPC):
    def __init__(self):
        super().__init__()
        self.run_calls = 0

    def run(self, requests):
        first = next(requests)
        self.run_starts.append(first.start)
        self.run_calls += 1
        yield pb.RunResponse(started=pb.RunStarted(pid=42))
        raise TransientRunError()


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
        workspace_id="ws-1",
    )


def create_test_sandbox(rpc):
    sandbox = Client(rpc=rpc).create(workspace_id="ws-1", wait=False)
    sandbox._data_plane_rpc = rpc
    sandbox.fs._rpc = rpc
    return sandbox
