from __future__ import annotations

from tenki_sandbox import SecretsRefresh, SecretVersion
from tenki_sandbox._pb import pb

from test_async_process_fs import FakeDataPlaneRPC, _sandbox
from test_process_fs_preview import FakeRPC, create_test_sandbox


class WriteCapture:
    def __init__(self):
        self.starts = []

    def record(self, frames):
        for frame in frames:
            if frame.WhichOneof("payload") == "start":
                self.starts.append(frame.start)
        return pb.WriteFileStreamResponse()


class SyncSecretsRPC(FakeRPC):
    def __init__(self):
        super().__init__()
        self.writes = WriteCapture()
        self.refresh_requests = []

    def write_file_stream(self, requests):
        return self.writes.record(requests)

    def refresh_session_secrets(self, req):
        self.refresh_requests.append(req)
        return pb.RefreshSessionSecretsResponse(
            changed=True,
            workflow_id="sandbox-runtime-secret-refresh-sess",
            versions=[pb.RuntimeSecretVersionRef(secret_name="TOKEN", version=2, environment_key="API_TOKEN")],
        )


class AsyncSecretsRPC(FakeDataPlaneRPC):
    def __init__(self):
        super().__init__()
        self.write_starts = []
        self.refresh_requests = []

    async def write_file_stream(self, requests):
        async for frame in requests:
            if frame.WhichOneof("payload") == "start":
                self.write_starts.append(frame.start)
        return pb.WriteFileStreamResponse()

    async def refresh_session_secrets(self, req):
        self.refresh_requests.append(req)
        return pb.RefreshSessionSecretsResponse(changed=False, workflow_id="wf")


EXPECTED_REFRESH = SecretsRefresh(
    changed=True,
    versions=[SecretVersion(secret_name="TOKEN", version=2, environment_key="API_TOKEN")],
    workflow_id="sandbox-runtime-secret-refresh-sess",
)


def test_exec_with_secrets_sets_the_run_flag_only_when_asked():
    rpc = SyncSecretsRPC()
    sandbox = create_test_sandbox(rpc)
    sandbox.exec("env", with_secrets=True)
    sandbox.exec("env")
    assert sandbox.start("env", with_secrets=True).wait().ok
    assert [start.with_secrets for start in rpc.run_starts] == [True, False, True]


def test_write_stream_privileged_marks_only_requested_writes():
    rpc = SyncSecretsRPC()
    sandbox = create_test_sandbox(rpc)
    sandbox.fs.write_stream("/etc/app.conf", [b"config"], privileged=True)
    sandbox.fs.write_stream("notes.txt", [b"x"])
    assert [(s.path, s.privileged) for s in rpc.writes.starts] == [("/etc/app.conf", True), ("notes.txt", False)]


def test_refresh_secrets_returns_names_and_versions():
    rpc = SyncSecretsRPC()
    sandbox = create_test_sandbox(rpc)
    assert sandbox.refresh_secrets() == EXPECTED_REFRESH
    assert rpc.refresh_requests[0].session_id == sandbox.id


async def test_async_exec_with_secrets_and_privileged_write():
    rpc = AsyncSecretsRPC()
    sandbox = await _sandbox(rpc)
    await sandbox.exec("env", with_secrets=True)
    await sandbox.exec("env")
    proc = await sandbox.start("env", with_secrets=True)
    assert (await proc.wait()).ok
    assert [start.with_secrets for start in rpc.run_starts] == [True, False, True]

    await sandbox.fs.write_stream("/etc/app.conf", [b"config"], privileged=True)
    await sandbox.fs.write_stream("notes.txt", [b"x"])
    assert [(s.path, s.privileged) for s in rpc.write_starts] == [("/etc/app.conf", True), ("notes.txt", False)]


async def test_async_refresh_secrets():
    rpc = AsyncSecretsRPC()
    sandbox = await _sandbox(rpc)
    assert await sandbox.refresh_secrets() == SecretsRefresh(changed=False, versions=[], workflow_id="wf")
    assert rpc.refresh_requests[0].session_id == sandbox.id
