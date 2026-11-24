from __future__ import annotations

from typing import Sequence

from .._compatibility import cleanup_unacknowledged_session_async

from .._compatibility import cleanup_unsupported_public_egress_async

from .._convert import ci_runner_sizes, identity, sandbox_info, session_cost, session_costs, session_metrics, workspace_usage_limit, public_egress_acknowledged
from .._pagination import advance_page_token
from .._pb import pb
from .._time import duration
from .._transport import (
    CHANNEL_OPTIONS,
    GrpcConfig,
    resolve_auth_token,
    resolve_base_url,
    read_env,
    resolve_gateway_url,
)
from ..client import (
    _create_wait_timeout,
    _normalize_tags,
    build_create_session_request,
    build_issue_ssh_cert_request,
)
from ..constants import GATEWAY_URL_ENVS
from .data_plane import warm_data_plane_endpoints
from ..errors import TemplateRuntimeFailedError, WaitReadyFailedError, map_rpc_error
from ..models import (
    PublicEgressSpec,
    CIRunnerSizes,
    EgressHttpRule,
    Identity,
    Image,
    SessionCost,
    SessionCostBatch,
    SessionMetrics,
    TailnetAttachment,
    Template,
	WorkspaceUsageLimit,
    StorageMountSpec,
)
from ..secret_files import RuntimeSecretFile
from ..template_spec import TemplateSpec
from ..warning import (
    WarningHandler,
    default_warning_handler,
    emit_warnings,
    sandbox_warnings_from_proto,
)
from ._transport import AsyncGrpcSandboxRPC
from .preview_url import AsyncPreviewUrls
from .registry import AsyncRegistry
from ..egress_policies import AsyncEgressPolicies
from .resources import AsyncSnapshots, AsyncStorageConnections, AsyncTemplates, AsyncVolumes
from .sandbox import AsyncSandbox


class AsyncClient:
    def __init__(
        self,
        *,
        auth_token: str | None = None,
        api_key: str | None = None,
        base_url: str | None = None,
        gateway_url: str | None = None,
        timeout: float | None = None,
        warning_handler: WarningHandler | None = default_warning_handler,
        rpc=None,
    ) -> None:
        self.base_url = resolve_base_url(base_url)
        self.gateway_url = resolve_gateway_url(gateway_url, self.base_url)
        self._gateway_explicit = bool((gateway_url or read_env(GATEWAY_URL_ENVS)).strip())
        self.auth_token = (
            resolve_auth_token(auth_token, api_key)
            if rpc is None or auth_token is not None or api_key is not None
            else "tk_test"
        )
        self._rpc = rpc or AsyncGrpcSandboxRPC(
            GrpcConfig(
                base_url=self.base_url,
                auth_token=self.auth_token,
                timeout=timeout,
                # Same HTTP/2 keepalive as the Go and TypeScript clients.
                channel_options=CHANNEL_OPTIONS,
            )
        )
        self._warning_handler = warning_handler
        self.preview_urls = AsyncPreviewUrls(self._rpc)
        self.volumes = AsyncVolumes(self._rpc)
        self.egress_policies = AsyncEgressPolicies(self._rpc)
        self.storage_connections = AsyncStorageConnections(self._rpc)
        self.snapshots = AsyncSnapshots(self._rpc)
        self.templates = AsyncTemplates(self._rpc, warning_handler=warning_handler)
        self.registry = AsyncRegistry(self._rpc)

    def __repr__(self) -> str:
        return f"AsyncClient(base_url={self.base_url!r}, auth_token='[REDACTED]')"

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
        allow_domains: list[str] | None = None,
        allow_cidrs: list[str] | None = None,
        intercept_https: bool = False,
        allowed_ports: list[int] | None = None,
        public_egress: PublicEgressSpec | None = None,
        http_rules: list[EgressHttpRule] | None = None,
        egress_policies: list[str] | None = None,
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
        direct_runtime: TemplateSpec | None = None,
        secret_files: list[RuntimeSecretFile] | None = None,
        secret_policies: list[str] | None = None,
        secret_overrides: dict[str, str] | None = None,
        enable_opencode: bool = False,
        clone_repo_url: str | None = None,
        volumes: list[dict | tuple] | None = None,
        storage_mounts: list[StorageMountSpec | dict] | None = None,
        disable_storage_defaults: bool = False,
        excluded_default_mount_ids: list[str] | None = None,
        github_token: str | None = None,
        sticky: bool = False,
        wait_for_runtime: bool = False,
        tailnet: TailnetAttachment | None = None,
        disable_tailnet: bool = False,
        _owns_client: bool = False,
    ) -> AsyncSandbox:
        """Create a sandbox.

        ``cpu_cores`` is 1-128 and ``memory_mb`` is 128-524288 MiB and must be even;
        the workspace's size limit may be lower.
        ``disk_size_gb`` is 5-100 GiB: a create with no ``image``, ``snapshot_id`` or
        ``from_template_spec`` defaults to 5, while one with a source inherits
        that source's disk unless this is set.

        ``allow_domains``/``allow_cidrs`` restrict outbound access; both are
        ignored when ``allow_outbound`` is false. An ``allow_domains`` entry is
        an exact host, ``*.example.com`` (one subdomain label) or
        ``**.example.com`` (any depth); neither wildcard matches the bare
        parent. ``intercept_https`` (requires
        ``allow_domains``) terminates HTTPS at the egress proxy so each request's
        Host must match the TLS server name; ``http_rules`` (requires
        ``intercept_https``) refuses with 403 any HTTP request on ports 80 and
        443 that matches none of the rules. ``egress_policies`` pins named
        workspace egress policies that the server merges with these options.

        ``idle_timeout_minutes`` is deprecated and ignored: sandboxes do not
        auto-pause on idle.

        ``pause_retention`` is how long a paused session is kept resumable. It
        must be at most the workspace's pause retention limit, which is also
        the default when unset or not positive; a larger value is rejected.
        """
        req = build_create_session_request(
            workspace_id=workspace_id,
            name=name,
            allow_inbound=allow_inbound,
            allow_outbound=allow_outbound,
            allow_domains=allow_domains,
            allow_cidrs=allow_cidrs,
            intercept_https=intercept_https,
            allowed_ports=allowed_ports,
            public_egress=public_egress,
            http_rules=http_rules,
            egress_policies=egress_policies,
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
            direct_runtime=direct_runtime,
            secret_overrides=secret_overrides,
            secret_files=secret_files,
            secret_policies=secret_policies,
            enable_opencode=enable_opencode,
            clone_repo_url=clone_repo_url,
            volumes=volumes,
            storage_mounts=storage_mounts,
            disable_storage_defaults=disable_storage_defaults,
            excluded_default_mount_ids=excluded_default_mount_ids,
            github_token=github_token,
            sticky=sticky,
            # Normal readiness waits client-side so WaitSession can expose and
            # prewarm the data plane while provisioning continues.
            wait_ready=wait and wait_for_runtime,
            wait_for_runtime=wait_for_runtime,
            tailnet=tailnet,
            disable_tailnet=disable_tailnet,
        )

        try:
            if public_egress is not None:
                resp = (await self._rpc.create_session_with_public_egress(pb.CreateSessionWithPublicEgressRequest(request=req))).response
            else:
                resp = await self._rpc.create_session(req)
        except Exception as err:
            mapped = map_rpc_error(err)
            if isinstance(mapped, TemplateRuntimeFailedError) and mapped.sandbox is not None:
                session = mapped.sandbox
                if session.id and not public_egress_acknowledged(public_egress, session.egress.public_egress):
                    await cleanup_unsupported_public_egress_async(self._rpc, session.id)
            raise mapped from err

        if any(
            (port.port, port.protocol) not in {(accepted.port, accepted.protocol) for accepted in resp.session.tailnet_status.exposed_ports}
            for port in req.tailnet.exposed_ports
        ):
            try:
                await cleanup_unacknowledged_session_async(self._rpc, resp.session.id)
            except Exception as err:
                raise RuntimeError(f"sandbox {resp.session.id}: server did not acknowledge tailnet exposed ports; cleanup failed") from err
            raise RuntimeError("server does not support requested tailnet exposed ports; session terminated")

        if tailnet is not None and tailnet.accept_dns is not None:
            status = resp.session.tailnet_status
            if not status.HasField("accept_dns") or status.accept_dns != tailnet.accept_dns:
                error = RuntimeError("API did not acknowledge managed tailnet DNS configuration")
                try:
                    await cleanup_unacknowledged_session_async(self._rpc, resp.session.id)
                except Exception as cleanup_error:
                    raise error from cleanup_error
                raise error

        info = sandbox_info(resp.session)
        if not public_egress_acknowledged(public_egress, info.egress.public_egress):
            await cleanup_unsupported_public_egress_async(self._rpc, resp.session.id)

        warm_data_plane_endpoints(resp.data_plane_warm_endpoints)
        warnings = sandbox_warnings_from_proto(resp.warnings)
        sandbox = AsyncSandbox(
            self,
            info,
            warnings=warnings,
            owns_client=_owns_client,
            data_plane_endpoint=resp.data_plane_endpoint,
            data_plane_credential=resp.credential if resp.HasField("credential") else None,
            data_plane_route_status=resp.route_status,
        )
        emit_warnings(
            warnings, self._warning_handler, stacklevel=5 if _owns_client else 4
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
            return AsyncSandbox(
                self,
                sandbox_info(resp.session),
            )
        except Exception as err:
            raise map_rpc_error(err) from err

    async def get_session_metrics(
        self, sandbox_id: str, *, window: float | int | object | None = None
    ) -> SessionMetrics:
        req = pb.GetSessionMetricsRequest(session_id=sandbox_id)
        if window is not None:
            req.window.CopyFrom(duration(window))
        try:
            return session_metrics(await self._rpc.get_session_metrics(req))
        except Exception as err:
            raise map_rpc_error(err) from err

    async def get_session_cost(self, sandbox_id: str) -> SessionCost:
        try:
            resp = await self._rpc.get_session_cost(pb.GetSessionCostRequest(session_id=sandbox_id.strip()))
            return session_cost(resp.cost)
        except Exception as err:
            raise map_rpc_error(err) from err

    async def batch_get_session_costs(
        self, sandbox_ids: Sequence[str], *, workspace_id: str | None = None
    ) -> SessionCostBatch:
        """Costs of up to 100 sessions in one workspace, the caller's by default."""
        if isinstance(sandbox_ids, str):
            raise TypeError("sandbox_ids must be a sequence of IDs, not a single string")
        req = pb.BatchGetSessionCostsRequest(
            session_ids=[sandbox_id.strip() for sandbox_id in sandbox_ids]
        )
        if workspace_id and workspace_id.strip():
            req.workspace_id = workspace_id.strip()
        try:
            return session_costs(await self._rpc.batch_get_session_costs(req))
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
                    AsyncSandbox(
                        self,
                        sandbox_info(item),
                    )
                    for item in resp.sessions
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

    async def list_ci_runner_sizes(self) -> CIRunnerSizes:
        """Luxor CI runner sizes for the API key's workspace, within its job-size limits."""
        try:
            return ci_runner_sizes(await self._rpc.list_ci_runner_sizes(pb.ListCIRunnerSizesRequest()))
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
                pin_host_key=resp.pin_host_key,
                expires_at=datetime_from_proto(resp.expires_at) if resp.HasField("expires_at") else None,
                cert_serial=resp.cert_serial,
            )
        except Exception as err:
            raise map_rpc_error(err) from err
