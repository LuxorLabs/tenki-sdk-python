from __future__ import annotations

import time
from datetime import datetime, timezone

import pytest
from google.protobuf.timestamp_pb2 import Timestamp

from tenki_sandbox import Client, SnapshotNotDurableError, SnapshotNotFoundError
from tenki_sandbox._pb import pb


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _snap(
    snapshot_id="snap-1",
    session_id="sess-1",
    state=pb.SNAPSHOT_STATE_READY,
    durability_state=pb.SNAPSHOT_DURABILITY_STATE_DURABLE,
    name=None,
    failure_reason="",
    raw_image_available=False,
) -> pb.Snapshot:
    s = pb.Snapshot(
        id=snapshot_id,
        session_id=session_id,
        state=state,
        failure_reason=failure_reason,
        durability_state=durability_state,
        raw_image_available=raw_image_available,
    )
    if name is not None:
        s.name = name
    return s


class FakeRPC:
    def __init__(self, snapshots=None):
        self._snapshots = snapshots or []
        self.list_session_req = None
        self.list_workspace_req = None
        self.list_dangling_req = None
        self.update_req = None
        self.download_url_req = None
        self._get_snap = None

    def list_session_snapshots(self, req):
        self.list_session_req = req
        return pb.ListSessionSnapshotsResponse(snapshots=self._snapshots)

    def list_workspace_snapshots(self, req):
        self.list_workspace_req = req
        return pb.ListWorkspaceSnapshotsResponse(snapshots=self._snapshots)

    def list_dangling_snapshots(self, req):
        self.list_dangling_req = req
        return pb.ListDanglingSnapshotsResponse(snapshots=self._snapshots)

    def update_snapshot(self, req):
        self.update_req = req
        return pb.UpdateSnapshotResponse(snapshot=_snap(snapshot_id=req.snapshot_id))

    def get_snapshot(self, req):
        return pb.GetSnapshotResponse(snapshot=self._get_snap or _snap())

    def get_snapshot_download_url(self, req):
        self.download_url_req = req
        expires_at = Timestamp()
        expires_at.FromDatetime(datetime(2030, 1, 1, tzinfo=timezone.utc))
        return pb.GetSnapshotDownloadURLResponse(
            url="https://example.test/raw-image", expires_at=expires_at
        )


# ---------------------------------------------------------------------------
# list_session
# ---------------------------------------------------------------------------


def test_list_session_request_fields():
    snaps = [_snap("s1"), _snap("s2")]
    rpc = FakeRPC(snapshots=snaps)
    client = Client(rpc=rpc)
    result = client.snapshots.list_session("  sess-42  ")
    assert rpc.list_session_req.session_id == "sess-42"
    assert rpc.list_session_req.page_size == 100
    assert len(result) == 2
    assert result[0].id == "s1"


def test_list_session_returns_snapshots():
    rpc = FakeRPC(snapshots=[_snap(name="boot")])
    result = Client(rpc=rpc).snapshots.list_session("sess-1")
    assert result[0].name == "boot"


# ---------------------------------------------------------------------------
# list_workspace
# ---------------------------------------------------------------------------


def test_list_workspace_request_fields():
    rpc = FakeRPC(snapshots=[_snap()])
    Client(rpc=rpc).snapshots.list_workspace("  ws-99  ")
    assert rpc.list_workspace_req.workspace_id == "ws-99"
    assert rpc.list_workspace_req.page_size == 100


# ---------------------------------------------------------------------------
# list_dangling
# ---------------------------------------------------------------------------


def test_list_dangling_request_fields():
    rpc = FakeRPC(snapshots=[_snap("d1"), _snap("d2")])
    result = Client(rpc=rpc).snapshots.list_dangling()
    assert rpc.list_dangling_req.page_size == 100
    assert len(result) == 2


# ---------------------------------------------------------------------------
# update
# ---------------------------------------------------------------------------


def test_update_name():
    rpc = FakeRPC()
    Client(rpc=rpc).snapshots.update("snap-1", name="renamed")
    assert rpc.update_req.snapshot_id == "snap-1"
    assert rpc.update_req.name == "renamed"


def test_update_expires_at():
    rpc = FakeRPC()
    ts = datetime(2030, 1, 1, tzinfo=timezone.utc)
    Client(rpc=rpc).snapshots.update("snap-1", expires_at=ts)
    assert rpc.update_req.HasField("expires_at")
    assert not rpc.update_req.clear_expires_at


def test_update_clear_expires_at():
    rpc = FakeRPC()
    Client(rpc=rpc).snapshots.update("snap-1", clear_expires_at=True)
    assert rpc.update_req.clear_expires_at is True
    assert not rpc.update_req.HasField("expires_at")


def test_update_expires_at_and_clear_raises():
    rpc = FakeRPC()
    ts = datetime(2030, 1, 1, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="mutually exclusive"):
        Client(rpc=rpc).snapshots.update("snap-1", expires_at=ts, clear_expires_at=True)


def test_update_tags_set():
    rpc = FakeRPC()
    Client(rpc=rpc).snapshots.update("snap-1", tags=["a", "b"])
    assert list(rpc.update_req.tags) == ["a", "b"]
    assert not rpc.update_req.clear_tags


def test_update_tags_empty_clears():
    rpc = FakeRPC()
    Client(rpc=rpc).snapshots.update("snap-1", tags=[])
    assert rpc.update_req.clear_tags is True


def test_update_tags_none_not_touched():
    rpc = FakeRPC()
    Client(rpc=rpc).snapshots.update("snap-1", name="x")
    assert not rpc.update_req.clear_tags
    assert list(rpc.update_req.tags) == []


def test_update_strips_snapshot_id():
    rpc = FakeRPC()
    Client(rpc=rpc).snapshots.update("  snap-1  ", name="x")
    assert rpc.update_req.snapshot_id == "snap-1"


# ---------------------------------------------------------------------------
# wait_durable
# ---------------------------------------------------------------------------


def test_wait_durable_returns_when_durable():
    rpc = FakeRPC()
    rpc._get_snap = _snap(durability_state=pb.SNAPSHOT_DURABILITY_STATE_DURABLE)
    result = Client(rpc=rpc).snapshots.wait_durable("snap-1", timeout=5)
    assert result.durability_state == "DURABLE"


def test_wait_durable_raises_on_propagation_failed():
    rpc = FakeRPC()
    rpc._get_snap = _snap(
        durability_state=pb.SNAPSHOT_DURABILITY_STATE_PROPAGATION_FAILED
    )
    with pytest.raises(SnapshotNotDurableError):
        Client(rpc=rpc).snapshots.wait_durable("snap-1", timeout=5)


def test_wait_durable_raises_on_unspecified_ready():
    rpc = FakeRPC()
    rpc._get_snap = _snap(
        state=pb.SNAPSHOT_STATE_READY,
        durability_state=pb.SNAPSHOT_DURABILITY_STATE_UNSPECIFIED,
    )
    with pytest.raises(SnapshotNotDurableError, match="unspecified"):
        Client(rpc=rpc).snapshots.wait_durable("snap-1", timeout=5)


def test_wait_durable_raises_on_failed_state():
    rpc = FakeRPC()
    rpc._get_snap = _snap(
        state=pb.SNAPSHOT_STATE_FAILED,
        durability_state=pb.SNAPSHOT_DURABILITY_STATE_LOCAL_READY,
        failure_reason="disk full",
    )
    with pytest.raises(RuntimeError, match="disk full"):
        Client(rpc=rpc).snapshots.wait_durable("snap-1", timeout=5)


def test_wait_durable_raises_on_deleted():
    rpc = FakeRPC()
    rpc._get_snap = _snap(
        state=pb.SNAPSHOT_STATE_DELETED,
        durability_state=pb.SNAPSHOT_DURABILITY_STATE_UNSPECIFIED,
    )
    with pytest.raises(SnapshotNotFoundError):
        Client(rpc=rpc).snapshots.wait_durable("snap-1", timeout=5)


def test_wait_durable_model_fields():
    rpc = FakeRPC()
    rpc._get_snap = _snap(
        durability_state=pb.SNAPSHOT_DURABILITY_STATE_DURABLE,
    )
    snap = Client(rpc=rpc).snapshots.wait_durable("snap-1")
    assert snap.durability_state == "DURABLE"
    assert snap.type == "UNSPECIFIED"


def test_snapshot_raw_image_available_field():
    rpc = FakeRPC()
    rpc._get_snap = _snap(raw_image_available=True)
    snap = Client(rpc=rpc).snapshots.get("snap-1")
    assert snap.raw_image_available is True


def test_download_url_request_and_response():
    rpc = FakeRPC()
    result = Client(rpc=rpc).snapshots.download_url("  snap-1  ")
    assert rpc.download_url_req.snapshot_id == "snap-1"
    assert result.url == "https://example.test/raw-image"
    assert result.expires_at == datetime(2030, 1, 1, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Snapshot model new fields
# ---------------------------------------------------------------------------


def test_snapshot_model_new_fields():
    rpc = FakeRPC(
        snapshots=[
            pb.Snapshot(
                id="s1",
                session_id="sess-1",
                state=pb.SNAPSHOT_STATE_READY,
                durability_state=pb.SNAPSHOT_DURABILITY_STATE_LOCAL_READY,
                type=pb.SNAPSHOT_TYPE_USER,
                cpu_cores=2,
                memory_mb=4096,
                disk_size_gb=20,
                memory_bytes=1024 * 1024,
            )
        ]
    )
    snaps = Client(rpc=rpc).snapshots.list_session("sess-1")
    s = snaps[0]
    assert s.durability_state == "LOCAL_READY"
    assert s.type == "USER"
    assert s.cpu_cores == 2
    assert s.memory_mb == 4096
    assert s.disk_size_gb == 20
    assert s.memory_bytes == 1024 * 1024
