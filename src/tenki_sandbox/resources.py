from __future__ import annotations

import time
from datetime import datetime

from ._convert import snapshot, snapshot_download_url, template, template_build, volume
from ._pb import pb
from ._resource_validation import validate_template_resources
from ._time import timestamp
from .errors import (
    SnapshotNotDurableError,
    SnapshotNotFoundError,
    TemplateBuildFailedError,
    VolumeNotFoundError,
    VolumeSyncPendingError,
    map_rpc_error,
)
from .models import Snapshot, SnapshotDownloadURL, Template, TemplateBuild, Volume


class Volumes:
    def __init__(self, rpc) -> None:
        self._rpc = rpc

    def create(
        self,
        *,
        workspace_id: str,
        name: str,
        size_bytes: int,
        project_id: str | None = None,
    ) -> Volume:
        req = pb.CreateVolumeRequest(
            workspace_id=workspace_id, name=name, size_bytes=size_bytes
        )
        if project_id:
            req.project_id = project_id
        try:
            return volume(self._rpc.create_volume(req).volume)
        except Exception as err:
            raise map_rpc_error(err) from err

    def get(self, volume_id: str) -> Volume:
        try:
            resp = self._rpc.get_volume(pb.GetVolumeRequest(volume_id=volume_id))
            return volume(resp.volume, getattr(resp, "active_attachments", []))
        except Exception as err:
            raise map_rpc_error(err) from err

    def list(self, workspace_id: str) -> list[Volume]:
        try:
            resp = self._rpc.list_volumes(
                pb.ListVolumesRequest(workspace_id=workspace_id, page_size=100)
            )
            return [volume(item) for item in resp.volumes]
        except Exception as err:
            raise map_rpc_error(err) from err

    def list_project(self, project_id: str) -> list[Volume]:
        try:
            resp = self._rpc.list_project_volumes(
                pb.ListProjectVolumesRequest(project_id=project_id, page_size=100)
            )
            return [volume(item) for item in resp.volumes]
        except Exception as err:
            raise map_rpc_error(err) from err

    def resize(self, volume_id: str, size_bytes: int) -> Volume:
        try:
            return volume(
                self._rpc.resize_volume(
                    pb.ResizeVolumeRequest(
                        volume_id=volume_id, new_size_bytes=size_bytes
                    )
                ).volume
            )
        except Exception as err:
            raise map_rpc_error(err) from err

    def update(
        self, volume_id: str, *, name: str | None = None, tags: list[str] | None = None
    ) -> Volume:
        req = pb.UpdateVolumeRequest(volume_id=volume_id)
        if name is not None:
            req.name = name
        if tags is not None:
            if tags:
                req.tags.extend(_normalize_tags(tags))
            else:
                req.clear_tags = True
        try:
            return volume(self._rpc.update_volume(req).volume)
        except Exception as err:
            raise map_rpc_error(err) from err

    def delete(self, volume_id: str) -> None:
        _delete_backoff = [1.0, 2.0, 4.0, 8.0, 10.0]
        deadline = time.monotonic() + 60.0
        backoff_idx = 0
        while True:
            try:
                self._rpc.delete_volume(pb.DeleteVolumeRequest(volume_id=volume_id))
                return
            except Exception as err:
                mapped = map_rpc_error(err)
                if (
                    not isinstance(mapped, VolumeSyncPendingError)
                    or time.monotonic() >= deadline
                ):
                    raise mapped from err
                time.sleep(_delete_backoff[min(backoff_idx, len(_delete_backoff) - 1)])
                backoff_idx += 1

    def wait_ready(self, volume_id: str, *, timeout: float = 60) -> Volume:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            vol = self.get(volume_id)
            if vol.state == "AVAILABLE":
                return vol
            if vol.state == "DELETED":
                raise VolumeNotFoundError(volume_id)
            if vol.state == "DELETING":
                raise RuntimeError(f"volume entered terminal state: {vol.state}")
            time.sleep(0.5)
        raise TimeoutError(f"timeout waiting for volume {volume_id}")


class Snapshots:
    def __init__(self, rpc) -> None:
        self._rpc = rpc

    def create(
        self,
        session_id: str,
        *,
        name: str | None = None,
        expires_at: datetime | None = None,
    ) -> Snapshot:
        req = pb.CreateSnapshotRequest(session_id=session_id)
        if name:
            req.name = name
        ts = timestamp(expires_at)
        if ts:
            req.expires_at.CopyFrom(ts)
        try:
            return snapshot(self._rpc.create_snapshot(req).snapshot)
        except Exception as err:
            raise map_rpc_error(err) from err

    def create_and_wait(
        self, session_id: str, *, timeout: float = 300, name: str | None = None
    ) -> Snapshot:
        created = self.create(session_id, name=name)
        return self.wait_ready(created.id, timeout=timeout)

    def get(self, snapshot_id: str) -> Snapshot:
        try:
            return snapshot(
                self._rpc.get_snapshot(
                    pb.GetSnapshotRequest(snapshot_id=snapshot_id)
                ).snapshot
            )
        except Exception as err:
            raise map_rpc_error(err) from err

    def download_url(self, snapshot_id: str) -> SnapshotDownloadURL:
        try:
            resp = self._rpc.get_snapshot_download_url(
                pb.GetSnapshotDownloadURLRequest(snapshot_id=snapshot_id.strip())
            )
            return snapshot_download_url(resp)
        except Exception as err:
            raise map_rpc_error(err) from err

    def list(self) -> list[Snapshot]:
        try:
            resp = self._rpc.list_snapshots(pb.ListSnapshotsRequest(page_size=100))
            return [snapshot(item) for item in resp.snapshots]
        except Exception as err:
            raise map_rpc_error(err) from err

    def list_project(self, project_id: str) -> list[Snapshot]:
        try:
            resp = self._rpc.list_project_snapshots(
                pb.ListProjectSnapshotsRequest(project_id=project_id, page_size=100)
            )
            return [snapshot(item) for item in resp.snapshots]
        except Exception as err:
            raise map_rpc_error(err) from err

    def list_session(self, session_id: str) -> list[Snapshot]:
        try:
            resp = self._rpc.list_session_snapshots(
                pb.ListSessionSnapshotsRequest(
                    session_id=session_id.strip(), page_size=100
                )
            )
            return [snapshot(item) for item in resp.snapshots]
        except Exception as err:
            raise map_rpc_error(err) from err

    def list_workspace(self, workspace_id: str) -> list[Snapshot]:
        try:
            resp = self._rpc.list_workspace_snapshots(
                pb.ListWorkspaceSnapshotsRequest(
                    workspace_id=workspace_id.strip(), page_size=100
                )
            )
            return [snapshot(item) for item in resp.snapshots]
        except Exception as err:
            raise map_rpc_error(err) from err

    def list_dangling(self) -> list[Snapshot]:
        try:
            resp = self._rpc.list_dangling_snapshots(
                pb.ListDanglingSnapshotsRequest(page_size=100)
            )
            return [snapshot(item) for item in resp.snapshots]
        except Exception as err:
            raise map_rpc_error(err) from err

    def update(
        self,
        snapshot_id: str,
        *,
        name: str | None = None,
        expires_at: datetime | None = None,
        clear_expires_at: bool = False,
        tags: list[str] | None = None,
    ) -> Snapshot:
        if expires_at is not None and clear_expires_at:
            raise ValueError("expires_at and clear_expires_at are mutually exclusive")
        req = pb.UpdateSnapshotRequest(snapshot_id=snapshot_id.strip())
        if name is not None:
            req.name = name
        if clear_expires_at:
            req.clear_expires_at = True
        elif expires_at is not None:
            ts = timestamp(expires_at)
            if ts:
                req.expires_at.CopyFrom(ts)
        if tags is not None:
            normalized = [t.strip().lower() for t in tags if t.strip()]
            if normalized:
                req.tags.extend(normalized)
            else:
                req.clear_tags = True
        try:
            return snapshot(self._rpc.update_snapshot(req).snapshot)
        except Exception as err:
            raise map_rpc_error(err) from err

    def wait_durable(self, snapshot_id: str, *, timeout: float = 300) -> Snapshot:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            snap = self.get(snapshot_id)
            if snap.durability_state == "DURABLE":
                return snap
            if snap.durability_state == "PROPAGATION_FAILED":
                raise SnapshotNotDurableError(
                    f"snapshot {snapshot_id} durability propagation failed"
                )
            if snap.durability_state == "UNSPECIFIED" and snap.state == "READY":
                raise SnapshotNotDurableError(
                    f"snapshot {snapshot_id} has unspecified durability state; may predate durability tracking"
                )
            if snap.state == "FAILED":
                msg = f"snapshot {snapshot_id} failed"
                if snap.failure_reason:
                    msg += f": {snap.failure_reason}"
                raise RuntimeError(msg)
            if snap.state in {"DELETED", "DELETING"}:
                raise SnapshotNotFoundError(snapshot_id)
            time.sleep(2)
        raise TimeoutError(
            f"timeout waiting for snapshot {snapshot_id} to become durable"
        )

    def delete(self, snapshot_id: str) -> Snapshot:
        try:
            return snapshot(
                self._rpc.delete_snapshot(
                    pb.DeleteSnapshotRequest(snapshot_id=snapshot_id)
                ).snapshot
            )
        except Exception as err:
            raise map_rpc_error(err) from err

    def wait_ready(self, snapshot_id: str, *, timeout: float = 300) -> Snapshot:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            snap = self.get(snapshot_id)
            if snap.state == "READY":
                return snap
            if snap.state in {"FAILED", "DELETING", "DELETED"}:
                raise RuntimeError(f"snapshot entered terminal state: {snap.state}")
            time.sleep(0.5)
        raise TimeoutError(f"timeout waiting for snapshot {snapshot_id}")


class Templates:
    def __init__(self, rpc) -> None:
        self._rpc = rpc

    def create(
        self,
        *,
        workspace_id: str,
        name: str,
        base_image_id: str = "",
        setup_script: str = "",
        start_cmd: str | None = None,
        env: dict[str, str] | None = None,
        tags: list[str] | None = None,
        project_id: str | None = None,
        cpu_cores: int | None = None,
        memory_mb: int | None = None,
        disk_size_gb: int | None = None,
    ) -> Template:
        validate_template_resources(cpu_cores, memory_mb, disk_size_gb)

        req = pb.CreateTemplateRequest(
            workspace_id=workspace_id,
            name=name,
            base_image_id=base_image_id,
            setup_script=setup_script,
            env_vars=dict(env or {}),
        )
        if start_cmd is not None:
            req.start_cmd = start_cmd
        if project_id:
            req.project_id = project_id
        if tags:
            req.tags.extend(_normalize_tags(tags))
        if cpu_cores or memory_mb or disk_size_gb:
            req.resources.CopyFrom(
                pb.TemplateResources(
                    cpu_cores=cpu_cores or 0,
                    memory_mb=memory_mb or 0,
                    disk_size_gb=disk_size_gb or 0,
                )
            )
        try:
            return template(self._rpc.create_template(req).template)
        except Exception as err:
            raise map_rpc_error(err) from err

    def get(self, template_id: str) -> Template:
        try:
            return template(
                self._rpc.get_template(
                    pb.GetTemplateRequest(template_id=template_id)
                ).template
            )
        except Exception as err:
            raise map_rpc_error(err) from err

    def list(
        self, workspace_id: str, *, tags: list[str] | None = None
    ) -> list[Template]:
        try:
            resp = self._rpc.list_templates(
                pb.ListTemplatesRequest(
                    workspace_id=workspace_id,
                    page_size=100,
                    tags=_normalize_tags(tags or []),
                )
            )
            return [template(item) for item in resp.templates]
        except Exception as err:
            raise map_rpc_error(err) from err

    def list_project(
        self, project_id: str, *, tags: list[str] | None = None
    ) -> list[Template]:
        try:
            resp = self._rpc.list_project_templates(
                pb.ListProjectTemplatesRequest(
                    project_id=project_id,
                    page_size=100,
                    tags=_normalize_tags(tags or []),
                )
            )
            return [template(item) for item in resp.templates]
        except Exception as err:
            raise map_rpc_error(err) from err

    def build(
        self, template_id: str, *, publish_raw_image: bool | None = None
    ) -> TemplateBuild:
        try:
            req = pb.BuildTemplateRequest(template_id=template_id)
            if publish_raw_image is not None:
                req.publish_raw_image = publish_raw_image
            return template_build(self._rpc.build_template(req).build)
        except Exception as err:
            raise map_rpc_error(err) from err

    def get_build(self, build_id: str) -> TemplateBuild:
        try:
            return template_build(
                self._rpc.get_template_build(
                    pb.GetTemplateBuildRequest(build_id=build_id)
                ).build
            )
        except Exception as err:
            raise map_rpc_error(err) from err

    def wait_build(self, build_id: str, *, timeout: float = 600) -> TemplateBuild:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            build = self.get_build(build_id)
            if build.state == "READY":
                return build
            if build.state == "FAILED":
                raise TemplateBuildFailedError(build.error or "template build failed")
            time.sleep(0.5)
        raise TimeoutError(f"timeout waiting for template build {build_id}")

    def delete(self, template_id: str) -> Template:
        try:
            return template(
                self._rpc.delete_template(
                    pb.DeleteTemplateRequest(template_id=template_id)
                ).template
            )
        except Exception as err:
            raise map_rpc_error(err) from err


def _normalize_tags(tags: list[str]) -> list[str]:
    return [tag.strip().lower() for tag in tags if tag.strip()]
