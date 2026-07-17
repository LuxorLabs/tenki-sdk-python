from __future__ import annotations

import pytest

from tenki_sandbox import (
    Client,
    GiB,
    InvalidResourceConfigError,
    PermissionDeniedError,
    QuotaExceededError,
    VolumeAttachment,
)
from tenki_sandbox._pb import pb, template_pb
from tenki_sandbox._convert import template_build
from tenki_sandbox.errors import (
    GitOperationFailedError,
    InvalidStateError,
    VolumeSyncPendingError,
    map_rpc_error,
)


def test_volume_snapshot_template_resource_namespaces():
    rpc = FakeRPC()
    client = Client(rpc=rpc)

    volume = client.volumes.create(workspace_id="ws-1", project_id="proj-1", name="cache", size_bytes=10 * GiB)
    assert volume.name == "cache"
    assert rpc.volume_create_request.size_bytes == 10 * GiB

    snapshot = client.snapshots.create("session-1", name="after-setup")
    assert snapshot.name == "after-setup"

    template = client.templates.create(workspace_id="ws-1", name="python", setup_script="uv sync", cpu_cores=4)
    assert template.name == "python"
    assert rpc.template_create_request.resources.cpu_cores == 4

    build_env = {"NODE_ENV": "production"}
    build_secrets = {"GIT_TOKEN": "token", "NPM_TOKEN": "npm"}
    build = client.templates.build(
        template.id, build_env=build_env, build_secrets=build_secrets
    )
    build_env["NODE_ENV"] = "mutated"
    build_secrets["GIT_TOKEN"] = "mutated"
    assert build.state == "READY"
    assert dict(rpc.template_build_request.build_env) == {"NODE_ENV": "production"}
    assert dict(rpc.template_build_request.build_secrets) == {
        "GIT_TOKEN": "token",
        "NPM_TOKEN": "npm",
    }


def test_template_build_ignores_unknown_events_and_preserves_unspecified_progress():
    converted = template_build(
        template_pb.TemplateBuild(
            events=[
                template_pb.TemplateBuildEvent(),
                template_pb.TemplateBuildEvent(
                    progress=template_pb.TemplateBuildProgressEvent()
                ),
            ]
        )
    )

    assert converted is not None
    assert len(converted.events) == 1
    assert converted.events[0].state == "unspecified"


def test_template_build_preserves_build_secret_key_provenance():
    converted = template_build(
        template_pb.TemplateBuild(
            provenance=template_pb.TemplateBuildProvenance(
                build_secret_keys=["GIT_TOKEN", "NPM_TOKEN"]
            )
        )
    )

    assert converted is not None
    assert converted.provenance is not None
    assert converted.provenance.build_secret_keys == ("GIT_TOKEN", "NPM_TOKEN")


def test_template_create_rejects_invalid_resources():
    client = Client(rpc=FakeRPC())

    client.templates.create(workspace_id="ws-1", name="python", cpu_cores=2, memory_mb=4096)

    with pytest.raises(InvalidResourceConfigError, match="cpu_cores must be between 1 and 16"):
        client.templates.create(workspace_id="ws-1", name="python", cpu_cores=17)

    with pytest.raises(InvalidResourceConfigError, match="cpu_cores must be between 1 and 16"):
        client.templates.create(workspace_id="ws-1", name="python", cpu_cores=False)

    with pytest.raises(InvalidResourceConfigError, match="memory_mb must be aligned to 2 MiB"):
        client.templates.create(workspace_id="ws-1", name="python", memory_mb=8089)

    with pytest.raises(InvalidResourceConfigError, match="memory_mb must be between 512 and 65536"):
        client.templates.create(workspace_id="ws-1", name="python", memory_mb=128)

    with pytest.raises(InvalidResourceConfigError, match="memory_mb must be between 512 and 65536"):
        client.templates.create(workspace_id="ws-1", name="python", memory_mb=False)

    with pytest.raises(InvalidResourceConfigError, match="disk_size_gb must be between 5 and 100"):
        client.templates.create(workspace_id="ws-1", name="python", disk_size_gb=4)

    with pytest.raises(InvalidResourceConfigError, match="disk_size_gb must be between 5 and 100"):
        client.templates.create(workspace_id="ws-1", name="python", disk_size_gb=False)


def test_rpc_error_mapping_uses_grpc_code_names():
    assert isinstance(map_rpc_error(FakeRpcError("PERMISSION_DENIED", "nope")), PermissionDeniedError)
    assert isinstance(map_rpc_error(FakeRpcError("RESOURCE_EXHAUSTED", "quota exceeded")), QuotaExceededError)


_PREFIX = "check the repository, ref and access token: "


@pytest.mark.parametrize(
    ("details", "want_retryable", "want_stderr"),
    [
        (
            f"git clone failed (exit_code=128); {_PREFIX}"
            "fatal: unable to access 'https://github.com/x/y': Recv failure: Connection reset by peer",
            True,
            "fatal: unable to access 'https://github.com/x/y': Recv failure: Connection reset by peer",
        ),
        (
            f"git fetch PR failed (exit_code=128); {_PREFIX}fatal: Authentication failed for 'https://github.com/x/y'",
            False,
            "fatal: Authentication failed for 'https://github.com/x/y'",
        ),
        (
            f"git clone failed (exit_code=128); {_PREFIX}fatal: repository 'https://github.com/x/y' not found",
            False,
            "fatal: repository 'https://github.com/x/y' not found",
        ),
        (
            f"git fetch PR failed (exit_code=128); {_PREFIX}fatal: fetch-pack: invalid index-pack output",
            False,
            "fatal: fetch-pack: invalid index-pack output",
        ),
        (f"git clone failed (exit_code=128); {_PREFIX}something unexpected", False, "something unexpected"),
    ],
)
def test_git_failure_classification(details, want_retryable, want_stderr):
    mapped = map_rpc_error(FakeRpcError("FAILED_PRECONDITION", details))
    assert isinstance(mapped, GitOperationFailedError)
    assert mapped.retryable is want_retryable
    assert mapped.exit_code == 128
    # stderr must be just the git output, not the engine wrapper prefix.
    assert mapped.stderr == want_stderr
    # Must no longer collapse into the un-actionable InvalidStateError.
    assert not isinstance(mapped, InvalidStateError)


def test_volume_get_populates_active_attachments():
    rpc = FakeRPC()
    rpc.get_volume_resp = _MockGetVolumeResp(
        volume=pb.Volume(id="vol-1", workspace_id="ws-1", name="data", size_bytes=10 * GiB, state=pb.VOLUME_STATE_AVAILABLE),
        active_attachments=[
            pb.VolumeAttachment(
                id="att-1",
                volume_id="vol-1",
                session_id="sess-1",
                mount_path="/data",
                readonly=False,
                state="ATTACHED",
            )
        ],
    )
    vol = Client(rpc=rpc).volumes.get("vol-1")

    assert len(vol.active_attachments) == 1
    att = vol.active_attachments[0]
    assert isinstance(att, VolumeAttachment)
    assert att.id == "att-1"
    assert att.volume_id == "vol-1"
    assert att.session_id == "sess-1"
    assert att.mount_path == "/data"
    assert att.read_only is False
    assert att.state == "ATTACHED"


def test_volume_is_deletable():
    rpc = FakeRPC()
    rpc.get_volume_resp = _MockGetVolumeResp(
        volume=pb.Volume(id="vol-1", workspace_id="ws-1", name="d", size_bytes=GiB, state=pb.VOLUME_STATE_AVAILABLE),
    )
    vol = Client(rpc=rpc).volumes.get("vol-1")
    assert vol.is_deletable() is True


def test_volume_not_deletable_when_attached():
    rpc = FakeRPC()
    rpc.get_volume_resp = _MockGetVolumeResp(
        volume=pb.Volume(id="vol-1", workspace_id="ws-1", name="d", size_bytes=GiB, state=pb.VOLUME_STATE_AVAILABLE),
        active_attachments=[pb.VolumeAttachment(id="att-1", volume_id="vol-1", session_id="s", mount_path="/d", state="ATTACHED")],
    )
    vol = Client(rpc=rpc).volumes.get("vol-1")
    assert vol.is_deletable() is False


def test_volume_delete_retries_on_sync_pending(monkeypatch):
    monkeypatch.setattr("tenki_sandbox.resources.time.sleep", lambda _: None)

    call_count = 0

    class RetryRPC(FakeRPC):
        def delete_volume(self, req):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise FakeRpcError("FAILED_PRECONDITION", "volume sync pending")

    client = Client(rpc=RetryRPC())
    client.volumes.delete("vol-1")  # should not raise
    assert call_count == 3


def test_volume_delete_raises_after_deadline(monkeypatch):
    import time as _time

    # Monotonic returns an increasing counter; make the second call already past deadline.
    calls = [0]
    start = _time.monotonic()

    def fake_monotonic():
        calls[0] += 1
        # First call (inside delete to set deadline): return start.
        # Subsequent calls (inside the loop to check deadline): return start + 120 (past 60s).
        return start if calls[0] == 1 else start + 120

    monkeypatch.setattr("tenki_sandbox.resources.time.monotonic", fake_monotonic)
    monkeypatch.setattr("tenki_sandbox.resources.time.sleep", lambda _: None)

    call_count = 0

    class AlwaysFailRPC(FakeRPC):
        def delete_volume(self, req):
            nonlocal call_count
            call_count += 1
            raise FakeRpcError("FAILED_PRECONDITION", "volume sync pending")

    client = Client(rpc=AlwaysFailRPC())
    with pytest.raises(VolumeSyncPendingError):
        client.volumes.delete("vol-1")
    assert call_count == 1


class _MockGetVolumeResp:
    def __init__(self, volume, active_attachments=None):
        self.volume = volume
        self.active_attachments = active_attachments or []


class FakeRPC:
    get_volume_resp = None

    def get_volume(self, req):
        if self.get_volume_resp is not None:
            return self.get_volume_resp
        return _MockGetVolumeResp(
            volume=pb.Volume(id=req.volume_id, workspace_id="ws-1", name="vol", size_bytes=GiB, state=pb.VOLUME_STATE_AVAILABLE)
        )

    def delete_volume(self, req):
        pass

    def create_volume(self, req):
        self.volume_create_request = req
        return pb.CreateVolumeResponse(
            volume=pb.Volume(
                id="vol-1",
                workspace_id=req.workspace_id,
                project_id=req.project_id,
                name=req.name,
                size_bytes=req.size_bytes,
                state=pb.VOLUME_STATE_AVAILABLE,
            )
        )

    def create_snapshot(self, req):
        return pb.CreateSnapshotResponse(
            snapshot=pb.Snapshot(
                id="snap-1",
                session_id=req.session_id,
                name=req.name,
                state=pb.SNAPSHOT_STATE_READY,
                project_id="proj-1",
            )
        )

    def create_template(self, req):
        self.template_create_request = req
        return template_pb.CreateTemplateResponse(
            template=template_pb.Template(
                id="tpl-1",
                workspace_id=req.workspace_id,
                owner_type="workspace",
                owner_id=req.workspace_id,
                name=req.name,
                setup_script=req.setup_script,
                resources=req.resources,
                project_id=req.project_id,
            )
        )

    def build_template(self, req):
        self.template_build_request = req
        return template_pb.BuildTemplateResponse(
            build=template_pb.TemplateBuild(
                id="build-1",
                template_id=req.template_id,
                state=template_pb.TEMPLATE_BUILD_STATE_READY,
            )
        )


class FakeCode:
    def __init__(self, name):
        self.name = name


class FakeRpcError(Exception):
    def __init__(self, code, details):
        super().__init__(details)
        self._code = FakeCode(code)
        self._details = details

    def code(self):
        return self._code

    def details(self):
        return self._details
