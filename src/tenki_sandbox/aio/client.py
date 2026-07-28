from __future__ import annotations

import os

from .._convert import identity, sandbox_info, workspace_usage_limit
from .._pb import pb
from .._pagination import advance_page_token
from .._transport import (
    GrpcConfig,
    resolve_auth_token,
    resolve_base_url,
    resolve_gateway_url,
)
from ..client import (
    _create_wait_timeout,
    _normalize_tags,
    build_create_session_request,
    build_issue_ssh_cert_request,
)
from ..constants import ENV_GATEWAY_URL
from ..errors import WaitReadyFailedError, map_rpc_error
from ..models import Identity, Image, Template, WorkspaceUsageLimit
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
        workspace_id: str | None = None,
        name: str | None = None,
        wait: bool = True,
        timeout: float | None = None,
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
        from_template_spec: str | Template | None = None,
        setup_env: dict[str, str] | None = None,
        setup_secrets: dict[str, str] | None = None,
        enable_opencode: bool = False,
        clone_repo_url: str | None = None,
        volumes: list[dict | tuple] | None = None,
        github_token: str | None = None,
        sticky: bool = False,
        wait_for_runtime: bool = False,
        _owns_client: bool = False,
    ) -> AsyncSandbox:
        req = build_create_session_request(
            workspace_id=workspace_id,
            name=name,
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
            from_template_spec=from_template_spec,
            setup_env=setup_env,
            setup_secrets=setup_secrets,
            enable_opencode=enable_opencode,
            clone_repo_url=clone_repo_url,
            volumes=volumes,
            github_token=github_token,
            sticky=sticky,
            # Normal readiness waits client-side so WaitSession can expose and
            # prewarm the data plane while provisioning continues.
            wait_ready=wait and wait_for_runtime,
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
        if wait and sandbox.state != "RUNNING":
            try:
                await sandbox.wait_ready(_create_wait_timeout(timeout, from_template_spec))
            except Exception as err:
                if sandbox.state in {"TERMINATING", "TERMINATED"}:
                    raise
                raise WaitReadyFailedError(
                    f"sandbox {sandbox.id} created but not ready within wait budget",
                    sandbox,
                ) from err
        return sandbox

    async def get(self, sandbox_id: str) -> AsyncSandbox:
        try:
            resp = await self._rpc.get_session(pb.GetSessionRequest(session_id=sandbox_id))
            return AsyncSandbox(self, sandbox_info(resp.session))
        except Exception as err:
            raise map_rpc_error(err) from err

    async def list(
        self,
        *,
        workspace_id: str | None = None,
        tags: list[str] | None = None,
        sticky: bool | None = None,
        include_terminated: bool = False,
    ) -> list[AsyncSandbox]:
        try:
            sandboxes: list[AsyncSandbox] = []
            page_token = ""
            while True:
                req = pb.ListWorkspaceSandboxesRequest(
                    page_size=100,
                    page_token=page_token,
                    tags=_normalize_tags(tags or []),
                    include_terminated=include_terminated,
                )
                if workspace_id and workspace_id.strip():
                    req.workspace_id = workspace_id.strip()
                if sticky is not None:
                    req.sticky = sticky
                resp = await self._rpc.list_workspace_sandboxes(req)
                sandboxes.extend(
                    AsyncSandbox(self, sandbox_info(item)) for item in resp.sessions
                )
                page_token = advance_page_token(page_token, resp.next_page_token)
                if not page_token:
                    return sandboxes
        except Exception as err:
            raise map_rpc_error(err) from err

    async def who_am_i(self) -> Identity:
        try:
            return identity(await self._rpc.who_am_i(pb.WhoAmIRequest()))
        except Exception as err:
            raise map_rpc_error(err) from err

    async def get_usage(self) -> list[WorkspaceUsageLimit]:
        req = pb.GetWorkspaceSandboxUsageRequest()
        try:
            resp = await self._rpc.get_workspace_sandbox_usage(req)
            return [workspace_usage_limit(limit) for limit in resp.limits]
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
