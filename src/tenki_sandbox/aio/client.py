from __future__ import annotations

import os

from .._convert import identity, sandbox_info, workspace_usage_limit
from .._pb import pb
from .._transport import (
    GrpcConfig,
    resolve_auth_token,
    resolve_base_url,
    resolve_gateway_url,
)
from ..client import _normalize_tags, build_create_session_request, build_issue_ssh_cert_request
from ..constants import ENV_GATEWAY_URL
from ..errors import map_rpc_error
from ..models import Identity, Image, WorkspaceUsageLimit
from ._transport import AsyncGrpcSandboxRPC
from .preview_url import AsyncPreviewUrls
from .registry import AsyncRegistry
from .resources import AsyncSnapshots, AsyncTemplates, AsyncVolumes
from .sandbox import AsyncSandbox


class AsyncClient:
    def __init__(
        self,
        *,
        auth_token: str | None = None,
        base_url: str | None = None,
        gateway_url: str | None = None,
        cookie_name: str = "tenki_session",
        timeout: float | None = None,
        rpc=None,
    ) -> None:
        self.base_url = resolve_base_url(base_url)
        self.gateway_url = resolve_gateway_url(gateway_url, self.base_url)
        self._gateway_explicit = bool((gateway_url or os.getenv(ENV_GATEWAY_URL) or "").strip())
        self.cookie_name = cookie_name
        self.auth_token = resolve_auth_token(auth_token) if rpc is None else (auth_token or "test")
        self._rpc = rpc or AsyncGrpcSandboxRPC(
            GrpcConfig(
                base_url=self.base_url,
                auth_token=self.auth_token,
                cookie_name=cookie_name,
                timeout=timeout,
            )
        )
        self.preview_urls = AsyncPreviewUrls(self._rpc)
        self.volumes = AsyncVolumes(self._rpc)
        self.snapshots = AsyncSnapshots(self._rpc)
        self.templates = AsyncTemplates(self._rpc)
        self.registry = AsyncRegistry(self._rpc)

    async def close(self) -> None:
        close = getattr(self._rpc, "close", None)
        if close is None:
            return
        result = close()
        if result is not None and hasattr(result, "__await__"):
            await result

    async def __aenter__(self) -> "AsyncClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def create(
        self,
        *,
        name: str | None = None,
        project_id: str | None = None,
        workspace_id: str | None = None,
        wait: bool = True,
        timeout: float = 180,
        allow_inbound: bool = True,
        allow_outbound: bool = True,
        max_duration: float | int | object | None = None,
        idle_timeout_minutes: int | None = None,
        pause_retention: float | int | object | None = None,
        cpu_cores: int | None = None,
        memory_mb: int | None = None,
        disk_size_gb: int | None = None,
        metadata: dict[str, str] | None = None,
        tags: list[str] | None = None,
        env: dict[str, str] | None = None,
        ssh_authorized_keys: list[str] | None = None,
        snapshot_id: str | None = None,
        image: str | Image | None = None,
        enable_opencode: bool = False,
        clone_repo_url: str | None = None,
        volumes: list[dict | tuple] | None = None,
        github_token: str | None = None,
        sticky: bool = False,
        wait_for_runtime: bool = False,
        _owns_client: bool = False,
    ) -> AsyncSandbox:
        req = build_create_session_request(
            name=name,
            project_id=project_id,
            workspace_id=workspace_id,
            allow_inbound=allow_inbound,
            allow_outbound=allow_outbound,
            max_duration=max_duration,
            idle_timeout_minutes=idle_timeout_minutes,
            pause_retention=pause_retention,
            cpu_cores=cpu_cores,
            memory_mb=memory_mb,
            disk_size_gb=disk_size_gb,
            metadata=metadata,
            tags=tags,
            env=env,
            ssh_authorized_keys=ssh_authorized_keys,
            snapshot_id=snapshot_id,
            image=image,
            enable_opencode=enable_opencode,
            clone_repo_url=clone_repo_url,
            volumes=volumes,
            github_token=github_token,
            sticky=sticky,
            # wait=True rides the server-held single-RPC create; the client-side
            # wait_ready() below only runs for older servers or waits past the
            # server hold. Mirrors the sync and TS SDKs.
            wait_ready=wait,
            wait_for_runtime=wait_for_runtime,
        )

        try:
            resp = await self._rpc.create_session(req)
        except Exception as err:
            raise map_rpc_error(err) from err

        sandbox = AsyncSandbox(
            self,
            sandbox_info(resp.session),
            owns_client=_owns_client,
            data_plane_endpoint=resp.data_plane_endpoint,
            data_plane_credential=resp.credential if resp.HasField("credential") else None,
            data_plane_route_status=resp.route_status,
        )
        # The server-held create (wait_ready) can return RUNNING directly; only
        # open a WaitSession stream if it hasn't settled yet (mirrors sync).
        if wait and sandbox.state != "RUNNING":
            await sandbox.wait_ready(timeout)
        return sandbox

    async def get(self, sandbox_id: str) -> AsyncSandbox:
        try:
            resp = await self._rpc.get_session(pb.GetSessionRequest(session_id=sandbox_id))
            return AsyncSandbox(self, sandbox_info(resp.session))
        except Exception as err:
            raise map_rpc_error(err) from err

    async def list(self, *, tags: list[str] | None = None, sticky: bool | None = None) -> list[AsyncSandbox]:
        req = pb.ListSessionsRequest(page_size=100, tags=_normalize_tags(tags or []))
        if sticky is not None:
            req.sticky = sticky
        try:
            resp = await self._rpc.list_sessions(req)
            return [AsyncSandbox(self, sandbox_info(item)) for item in resp.sessions]
        except Exception as err:
            raise map_rpc_error(err) from err

    async def list_project(self, project_id: str, *, tags: list[str] | None = None) -> list[AsyncSandbox]:
        try:
            resp = await self._rpc.list_project_sandboxes(
                pb.ListProjectSandboxesRequest(project_id=project_id, page_size=100, tags=_normalize_tags(tags or []))
            )
            return [AsyncSandbox(self, sandbox_info(item)) for item in resp.sessions]
        except Exception as err:
            raise map_rpc_error(err) from err

    async def who_am_i(self) -> Identity:
        try:
            return identity(await self._rpc.who_am_i(pb.WhoAmIRequest()))
        except Exception as err:
            raise map_rpc_error(err) from err

    async def get_workspace_sandbox_usage(
        self, workspace_id: str, *, project_id: str | None = None
    ) -> list[WorkspaceUsageLimit]:
        req = pb.GetWorkspaceSandboxUsageRequest(workspace_id=workspace_id.strip())
        if project_id and project_id.strip():
            req.project_id = project_id.strip()
        try:
            resp = await self._rpc.get_workspace_sandbox_usage(req)
            return [workspace_usage_limit(limit) for limit in resp.limits]
        except Exception as err:
            raise map_rpc_error(err) from err

    async def list_workspace(self, workspace_id: str, *, tags: list[str] | None = None) -> list[AsyncSandbox]:
        try:
            resp = await self._rpc.list_workspace_sandboxes(
                pb.ListWorkspaceSandboxesRequest(
                    workspace_id=workspace_id,
                    page_size=100,
                    tags=_normalize_tags(tags or []),
                )
            )
            return [AsyncSandbox(self, sandbox_info(item)) for item in resp.sessions]
        except Exception as err:
            raise map_rpc_error(err) from err

    async def ssh(self, session_id: str, *, gateway_url: str | None = None) -> "AsyncSSHConn":
        from .ssh import open_ssh_conn

        return await open_ssh_conn(
            self._rpc,
            session_id,
            self.gateway_url,
            self.auth_token,
            override_gateway_url=gateway_url,
            gateway_address_explicit=self._gateway_explicit or bool(gateway_url),
            cookie_name=self.cookie_name,
        )

    async def issue_ssh_cert(
        self,
        session_id: str,
        public_key: str,
        *,
        ttl: float | None = None,
    ) -> "SSHCert":
        from .._time import datetime_from_proto
        from ..models import SSHCert

        req = build_issue_ssh_cert_request(session_id, public_key, ttl=ttl)
        try:
            resp = await self._rpc.issue_ssh_cert(req)
            return SSHCert(
                ssh_cert=resp.ssh_cert,
                ca_pub=resp.ca_pub,
                expires_at=datetime_from_proto(resp.expires_at) if resp.HasField("expires_at") else None,
                cert_serial=resp.cert_serial,
            )
        except Exception as err:
            raise map_rpc_error(err) from err
