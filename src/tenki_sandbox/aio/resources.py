from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import Awaitable, Callable
from datetime import datetime

from .._convert import snapshot, snapshot_download_url, template, template_build, volume
from .._pb import pb, template_pb
from .._pagination import advance_page_token
from .._time import timestamp
from ..client import _normalize_tags  # shared with sync; async uses one canonical copy
from ..errors import (
    SnapshotNotDurableError,
    SnapshotNotFoundError,
    TemplateBuildFailedError,
    TemplateBuildWaitCancelledError,
    VolumeNotFoundError,
    VolumeSyncPendingError,
    map_rpc_error,
    map_template_spec_error,
)
from ..models import (
    Snapshot,
    SnapshotDownloadURL,
    Template,
    TemplateBuild,
    TemplateBuildEvent,
    Volume,
)
from ..resources import (  # shared request builders/id helpers; one canonical copy
    build_create_template_request,
    build_id_of,
    build_update_template_request,
    template_id_of,
)
from ..template_spec import TemplateSpec


class AsyncVolumes:
    def __init__(self, rpc) -> None:
        self._rpc = rpc

    async def create(
        self,
        *,
        workspace_id: str | None = None,
        name: str,
        size_bytes: int,
    ) -> Volume:
        req = pb.CreateVolumeRequest(name=name, size_bytes=size_bytes)
        if workspace_id and workspace_id.strip():
            req.workspace_id = workspace_id.strip()
        try:
            return volume((await self._rpc.create_volume(req)).volume)
        except Exception as err:
            raise map_rpc_error(err) from err

    async def get(self, volume_id: str) -> Volume:
        try:
            resp = await self._rpc.get_volume(pb.GetVolumeRequest(volume_id=volume_id))
            return volume(resp.volume, getattr(resp, "active_attachments", []))
        except Exception as err:
            raise map_rpc_error(err) from err

    async def list(self, *, workspace_id: str | None = None) -> list[Volume]:
        try:
            volumes: list[Volume] = []
            page_token = ""
            while True:
                req = pb.ListVolumesRequest(page_size=100, page_token=page_token)
                if workspace_id and workspace_id.strip():
                    req.workspace_id = workspace_id.strip()
                resp = await self._rpc.list_volumes(req)
                volumes.extend(volume(item) for item in resp.volumes)
                page_token = advance_page_token(page_token, resp.next_page_token)
                if not page_token:
                    return volumes
        except Exception as err:
            raise map_rpc_error(err) from err

    async def resize(self, volume_id: str, size_bytes: int) -> Volume:
        try:
            resp = await self._rpc.resize_volume(
                pb.ResizeVolumeRequest(volume_id=volume_id, new_size_bytes=size_bytes)
            )
            return volume(resp.volume)
        except Exception as err:
            raise map_rpc_error(err) from err

    async def update(
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
            return volume((await self._rpc.update_volume(req)).volume)
        except Exception as err:
            raise map_rpc_error(err) from err

    async def delete(self, volume_id: str) -> None:
        _delete_backoff = [1.0, 2.0, 4.0, 8.0, 10.0]
        deadline = time.monotonic() + 60.0
        backoff_idx = 0
        while True:
            try:
                await self._rpc.delete_volume(
                    pb.DeleteVolumeRequest(volume_id=volume_id)
                )
                return
            except Exception as err:
                mapped = map_rpc_error(err)
                if (
                    not isinstance(mapped, VolumeSyncPendingError)
                    or time.monotonic() >= deadline
                ):
                    raise mapped from err
                await asyncio.sleep(
                    _delete_backoff[min(backoff_idx, len(_delete_backoff) - 1)]
                )
                backoff_idx += 1

    async def wait_ready(self, volume_id: str, *, timeout: float = 60) -> Volume:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            vol = await self.get(volume_id)
            if vol.state == "AVAILABLE":
                return vol
            if vol.state == "DELETED":
                raise VolumeNotFoundError(volume_id)
            if vol.state == "DELETING":
                raise RuntimeError(f"volume entered terminal state: {vol.state}")
            await asyncio.sleep(0.5)
        raise TimeoutError(f"timeout waiting for volume {volume_id}")


class AsyncSnapshots:
    def __init__(self, rpc) -> None:
        self._rpc = rpc

    async def create(
        self,
        session_id: str,
        *,
        name: str | None = None,
        expires_at: datetime | None = None,
        async_: bool = False,
    ) -> Snapshot:
        req = pb.CreateSnapshotRequest(session_id=session_id)
        if name:
            req.name = name
        ts = timestamp(expires_at)
        if ts:
            req.expires_at.CopyFrom(ts)
        setattr(req, "async", async_)
        try:
            return snapshot((await self._rpc.create_snapshot(req)).snapshot)
        except Exception as err:
            raise map_rpc_error(err) from err

    async def create_async(
        self,
        session_id: str,
        *,
        name: str | None = None,
        expires_at: datetime | None = None,
    ) -> Snapshot:
        return await self.create(
            session_id, name=name, expires_at=expires_at, async_=True
        )

    async def create_and_wait(
        self, session_id: str, *, timeout: float = 300, name: str | None = None
    ) -> Snapshot:
        created = await self.create(session_id, name=name)
        return await self.wait_ready(created.id, timeout=timeout)

    async def get(self, snapshot_id: str) -> Snapshot:
        try:
            return snapshot(
                (
                    await self._rpc.get_snapshot(
                        pb.GetSnapshotRequest(snapshot_id=snapshot_id)
                    )
                ).snapshot
            )
        except Exception as err:
            raise map_rpc_error(err) from err

    async def download_url(self, snapshot_id: str) -> SnapshotDownloadURL:
        try:
            resp = await self._rpc.get_snapshot_download_url(
                pb.GetSnapshotDownloadURLRequest(snapshot_id=snapshot_id.strip())
            )
            return snapshot_download_url(resp)
        except Exception as err:
            raise map_rpc_error(err) from err

    async def list(self, *, workspace_id: str | None = None) -> list[Snapshot]:
        try:
            snapshots: list[Snapshot] = []
            page_token = ""
            while True:
                req = pb.ListWorkspaceSnapshotsRequest(
                    page_size=100, page_token=page_token
                )
                if workspace_id and workspace_id.strip():
                    req.workspace_id = workspace_id.strip()
                resp = await self._rpc.list_workspace_snapshots(req)
                snapshots.extend(snapshot(item) for item in resp.snapshots)
                page_token = advance_page_token(page_token, resp.next_page_token)
                if not page_token:
                    return snapshots
        except Exception as err:
            raise map_rpc_error(err) from err

    async def list_session(self, session_id: str) -> list[Snapshot]:
        try:
            snapshots: list[Snapshot] = []
            page_token = ""
            while True:
                resp = await self._rpc.list_session_snapshots(
                    pb.ListSessionSnapshotsRequest(
                        session_id=session_id.strip(),
                        page_size=100,
                        page_token=page_token,
                    )
                )
                snapshots.extend(snapshot(item) for item in resp.snapshots)
                page_token = advance_page_token(page_token, resp.next_page_token)
                if not page_token:
                    return snapshots
        except Exception as err:
            raise map_rpc_error(err) from err

    async def list_dangling(self) -> list[Snapshot]:
        try:
            snapshots: list[Snapshot] = []
            page_token = ""
            while True:
                resp = await self._rpc.list_dangling_snapshots(
                    pb.ListDanglingSnapshotsRequest(
                        page_size=100, page_token=page_token
                    )
                )
                snapshots.extend(snapshot(item) for item in resp.snapshots)
                page_token = advance_page_token(page_token, resp.next_page_token)
                if not page_token:
                    return snapshots
        except Exception as err:
            raise map_rpc_error(err) from err

    async def update(
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
            normalized = _normalize_tags(tags)
            if normalized:
                req.tags.extend(normalized)
            else:
                req.clear_tags = True
        try:
            return snapshot((await self._rpc.update_snapshot(req)).snapshot)
        except Exception as err:
            raise map_rpc_error(err) from err

    async def wait_durable(self, snapshot_id: str, *, timeout: float = 300) -> Snapshot:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            snap = await self.get(snapshot_id)
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
            await asyncio.sleep(2)
        raise TimeoutError(
            f"timeout waiting for snapshot {snapshot_id} to become durable"
        )

    async def delete(self, snapshot_id: str) -> Snapshot:
        try:
            return snapshot(
                (
                    await self._rpc.delete_snapshot(
                        pb.DeleteSnapshotRequest(snapshot_id=snapshot_id)
                    )
                ).snapshot
            )
        except Exception as err:
            raise map_rpc_error(err) from err

    async def wait_ready(self, snapshot_id: str, *, timeout: float = 300) -> Snapshot:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            snap = await self.get(snapshot_id)
            if snap.state == "READY":
                return snap
            if snap.state in {"FAILED", "DELETING", "DELETED"}:
                raise RuntimeError(f"snapshot entered terminal state: {snap.state}")
            await asyncio.sleep(0.5)
        raise TimeoutError(f"timeout waiting for snapshot {snapshot_id}")


class AsyncTemplates:
    def __init__(self, rpc) -> None:
        self._rpc = rpc

    async def create(
        self,
        *,
        workspace_id: str | None = None,
        name: str,
        spec: TemplateSpec | None = None,
        base_image_id: str = "",
        setup_script: str = "",
        start_cmd: str | None = None,
        env: dict[str, str] | None = None,
        tags: list[str] | None = None,
        cpu_cores: int | None = None,
        memory_mb: int | None = None,
        disk_size_gb: int | None = None,
    ) -> Template:
        req = build_create_template_request(
            name=name,
            spec=spec,
            base_image_id=base_image_id,
            setup_script=setup_script,
            start_cmd=start_cmd,
            env=env,
            tags=tags,
            cpu_cores=cpu_cores,
            memory_mb=memory_mb,
            disk_size_gb=disk_size_gb,
        )
        if workspace_id and workspace_id.strip():
            req.workspace_id = workspace_id.strip()
        try:
            return template((await self._rpc.create_template(req)).template)
        except Exception as err:
            raise map_template_spec_error(err) from err

    async def update(
        self,
        template_ref: Template | str,
        *,
        name: str | None = None,
        tags: list[str] | None = None,
        spec: TemplateSpec | None = None,
        base_image_id: str | None = None,
        setup_script: str | None = None,
        start_cmd: str | None = None,
        env: dict[str, str] | None = None,
        cpu_cores: int | None = None,
        memory_mb: int | None = None,
        disk_size_gb: int | None = None,
    ) -> Template:
        req = build_update_template_request(
            template_id=template_id_of(template_ref),
            name=name,
            tags=tags,
            spec=spec,
            base_image_id=base_image_id,
            setup_script=setup_script,
            start_cmd=start_cmd,
            env=env,
            cpu_cores=cpu_cores,
            memory_mb=memory_mb,
            disk_size_gb=disk_size_gb,
        )
        try:
            return template((await self._rpc.update_template(req)).template)
        except Exception as err:
            raise map_template_spec_error(err) from err

    async def get(self, template_ref: Template | str) -> Template:
        req = template_pb.GetTemplateRequest(template_id=template_id_of(template_ref))
        try:
            return template((await self._rpc.get_template(req)).template)
        except Exception as err:
            raise map_rpc_error(err) from err

    async def list(
        self, *, workspace_id: str | None = None, tags: list[str] | None = None
    ) -> list[Template]:
        try:
            templates: list[Template] = []
            page_token = ""
            while True:
                req = template_pb.ListTemplatesRequest(
                    page_size=100,
                    page_token=page_token,
                    tags=_normalize_tags(tags or []),
                )
                if workspace_id and workspace_id.strip():
                    req.workspace_id = workspace_id.strip()
                resp = await self._rpc.list_templates(req)
                templates.extend(template(item) for item in resp.templates)
                page_token = advance_page_token(page_token, resp.next_page_token)
                if not page_token:
                    return templates
        except Exception as err:
            raise map_rpc_error(err) from err

    async def build(
        self,
        template_ref: Template | str,
        *,
        build_env: dict[str, str] | None = None,
        build_secrets: dict[str, str] | None = None,
        publish_raw_image: bool | None = None,
        wait_for_completion: bool = False,
        timeout: float = 600,
        on_event: Callable[[TemplateBuildEvent], Awaitable[None] | None] | None = None,
        cancel_event=None,
    ) -> TemplateBuild:
        """Start a remote build; returns immediately unless waited.

        ``build_env`` values are frozen into this build without mutating the
        template. ``build_secrets`` are explicit request-time values (never
        persisted in the template or spec hash). ``cancel_event`` / task cancellation stop
        local observation only; use ``cancel_build`` to cancel remotely.
        Local timeout and event-handler errors are propagated unchanged.
        """
        req = template_pb.BuildTemplateRequest(
            template_id=template_id_of(template_ref)
        )
        req.build_env.update(dict(build_env or {}))
        req.build_secrets.update(dict(build_secrets or {}))
        if publish_raw_image is not None:
            req.publish_raw_image = publish_raw_image
        try:
            build = template_build((await self._rpc.build_template(req)).build)
        except Exception as err:
            raise map_rpc_error(err) from err
        if wait_for_completion:
            return await self.wait_build(
                build.id, timeout=timeout, on_event=on_event, cancel_event=cancel_event
            )
        return build

    async def get_build(self, build_ref: TemplateBuild | str) -> TemplateBuild:
        req = template_pb.GetTemplateBuildRequest(build_id=build_id_of(build_ref))
        try:
            return template_build((await self._rpc.get_template_build(req)).build)
        except Exception as err:
            raise map_rpc_error(err) from err

    async def list_active_builds(
        self, template_ref: Template | str
    ) -> list[TemplateBuild]:
        req = template_pb.ListActiveTemplateBuildsRequest(
            template_id=template_id_of(template_ref)
        )
        try:
            resp = await self._rpc.list_active_template_builds(req)
            return [template_build(item) for item in resp.builds]
        except Exception as err:
            raise map_rpc_error(err) from err

    async def wait_build(
        self,
        build_ref: TemplateBuild | str,
        *,
        timeout: float = 600,
        on_event: Callable[[TemplateBuildEvent], Awaitable[None] | None] | None = None,
        cancel_event=None,
    ) -> TemplateBuild:
        """Wait for (or reconnect to) a build, replaying ordered events.

        Sync or async ``on_event`` handlers are awaited sequentially. Local
        cancellation (``cancel_event`` set or the task cancelled) never
        cancels the remote build. A failed build raises
        ``TemplateBuildFailedError`` carrying the final redacted build. Local
        timeout and event-handler errors are propagated unchanged.
        """
        build_id = build_id_of(build_ref)
        deadline = time.monotonic() + timeout
        seen_events = 0
        build = None
        while time.monotonic() < deadline:
            if cancel_event is not None and cancel_event.is_set():
                raise TemplateBuildWaitCancelledError(
                    f"wait for template build {build_id} cancelled locally", build
                )
            build = await self.get_build(build_id)
            if on_event is not None:
                for event in build.events[seen_events:]:
                    result = on_event(event)
                    if inspect.isawaitable(result):
                        await result
                seen_events = len(build.events)
            if build.state == "READY":
                return build
            if build.state == "FAILED":
                raise TemplateBuildFailedError(
                    build.failure.message
                    if build.failure
                    else build.error or "template build failed",
                    build,
                )
            await asyncio.sleep(0.5)
        raise TimeoutError(f"timeout waiting for template build {build_id}")

    async def cancel_build(self, build_ref: TemplateBuild | str) -> TemplateBuild:
        """Explicitly cancel the remote build; returns the terminal build."""
        req = template_pb.CancelTemplateBuildRequest(build_id=build_id_of(build_ref))
        try:
            return template_build((await self._rpc.cancel_template_build(req)).build)
        except Exception as err:
            raise map_rpc_error(err) from err

    async def delete(self, template_ref: Template | str) -> Template:
        req = template_pb.DeleteTemplateRequest(
            template_id=template_id_of(template_ref)
        )
        try:
            return template((await self._rpc.delete_template(req)).template)
        except Exception as err:
            raise map_rpc_error(err) from err
