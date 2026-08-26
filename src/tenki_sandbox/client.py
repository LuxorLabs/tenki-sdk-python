from __future__ import annotations

import os

from ._convert import identity, sandbox_info, workspace_usage_limit
from ._pb import pb
from ._pagination import advance_page_token
from ._resource_validation import validate_create_resources
from ._time import duration
from ._transport import (
    GrpcConfig,
    GrpcSandboxRPC,
    resolve_auth_token,
    resolve_base_url,
    resolve_gateway_url,
)
from .constants import ENV_GATEWAY_URL
from .errors import WaitReadyFailedError, map_rpc_error
from .models import Identity, Image, Template, WorkspaceUsageLimit
from .preview_url import PreviewUrls
from .registry import Registry
from .resources import Snapshots, Templates, Volumes
from .sandbox import Sandbox
from .warning import (
    WarningHandler,
    default_warning_handler,
    emit_warnings,
    sandbox_warnings_from_proto,
)


class Client:
    def __init__(
        self,
        *,
        auth_token: str | None = None,
        base_url: str | None = None,
        gateway_url: str | None = None,
        timeout: float | None = None,
        warning_handler: WarningHandler | None = default_warning_handler,
        rpc=None,
    ) -> None:
        self.base_url = resolve_base_url(base_url)
        self.gateway_url = resolve_gateway_url(gateway_url, self.base_url)
        self._gateway_explicit = bool((gateway_url or os.getenv(ENV_GATEWAY_URL) or "").strip())
        self.auth_token = resolve_auth_token(auth_token) if rpc is None or auth_token is not None else "tk_test"
        self._rpc = rpc or GrpcSandboxRPC(
            GrpcConfig(
                base_url=self.base_url,
                auth_token=self.auth_token,
                timeout=timeout,
            )
        )
        self._warning_handler = warning_handler
        self.preview_urls = PreviewUrls(self._rpc)
        self.volumes = Volumes(self._rpc)
        self.snapshots = Snapshots(self._rpc)
        self.templates = Templates(self._rpc)
        self.registry = Registry(self._rpc)

    def close(self) -> None:
        close = getattr(self._rpc, "close", None)
        if callable(close):
            close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def create(
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
    ) -> Sandbox:
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
            resp = self._rpc.create_session(req)
        except Exception as err:
            raise map_rpc_error(err) from err

        warnings = sandbox_warnings_from_proto(resp.warnings)
        sandbox = Sandbox(
            self,
            sandbox_info(resp.session),
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
                sandbox.wait_ready(_create_wait_timeout(timeout, from_template_spec))
            except Exception as err:
                if sandbox.state in {"TERMINATING", "TERMINATED"}:
                    raise
                raise WaitReadyFailedError(
                    f"sandbox {sandbox.id} created but not ready within wait budget",
                    sandbox,
                ) from err
        return sandbox

    def get(self, sandbox_id: str) -> Sandbox:
        try:
            resp = self._rpc.get_session(pb.GetSessionRequest(session_id=sandbox_id))
            return Sandbox(self, sandbox_info(resp.session))
        except Exception as err:
            raise map_rpc_error(err) from err

    def list(
        self,
        *,
        workspace_id: str | None = None,
        tags: list[str] | None = None,
        sticky: bool | None = None,
        include_terminated: bool = False,
    ) -> list[Sandbox]:
        try:
            sandboxes: list[Sandbox] = []
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
                resp = self._rpc.list_workspace_sandboxes(req)
                sandboxes.extend(
                    Sandbox(self, sandbox_info(item)) for item in resp.sessions
                )
                page_token = advance_page_token(page_token, resp.next_page_token)
                if not page_token:
                    return sandboxes
        except Exception as err:
            raise map_rpc_error(err) from err

    def who_am_i(self) -> Identity:
        try:
            return identity(self._rpc.who_am_i(pb.WhoAmIRequest()))
        except Exception as err:
            raise map_rpc_error(err) from err

    def get_usage(self) -> list[WorkspaceUsageLimit]:
        req = pb.GetWorkspaceSandboxUsageRequest()
        try:
            resp = self._rpc.get_workspace_sandbox_usage(req)
            return [workspace_usage_limit(limit) for limit in resp.limits]
        except Exception as err:
            raise map_rpc_error(err) from err

    def ssh(self, session_id: str, *, gateway_url: str | None = None) -> "SSHConn":
        from .ssh import SSHConn, open_ssh_conn  # noqa: F401
        return open_ssh_conn(
            self._rpc,
            session_id,
            self.gateway_url,
            self.auth_token,
            override_gateway_url=gateway_url,
            gateway_address_explicit=self._gateway_explicit or bool(gateway_url),
        )

    def issue_ssh_cert(
        self,
        session_id: str,
        public_key: str,
        *,
        ttl: float | None = None,
    ) -> "SSHCert":
        from ._time import datetime_from_proto
        from .models import SSHCert

        req = build_issue_ssh_cert_request(session_id, public_key, ttl=ttl)
        try:
            resp = self._rpc.issue_ssh_cert(req)
            return SSHCert(
                ssh_cert=resp.ssh_cert,
                ca_pub=resp.ca_pub,
                expires_at=datetime_from_proto(resp.expires_at) if resp.HasField("expires_at") else None,
                cert_serial=resp.cert_serial,
            )
        except Exception as err:
            raise map_rpc_error(err) from err


def _normalize_tags(tags: list[str]) -> list[str]:
    return [tag.strip().lower() for tag in tags if tag.strip()]


def _registry_ref(image: str | Image) -> str:
    """Resolve a launch ref; Image objects pin their immutable digest ref."""
    if isinstance(image, str):
        return image
    ref = image.digest_ref or image.ref
    if not ref:
        raise ValueError("image has no resolvable ref")
    return ref


def _template_id(template: str | Template) -> str:
    template_id = template if isinstance(template, str) else template.id
    template_id = template_id.strip()
    if not template_id:
        raise ValueError("from_template_spec requires a template ID or Template object")
    return template_id


def build_create_session_request(
    *,
    workspace_id: str | None = None,
    name: str | None = None,
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
    wait_ready: bool = False,
    wait_for_runtime: bool = False,
) -> "pb.CreateSessionRequest":
    """Build a CreateSessionRequest. Shared by the sync and async clients."""
    validate_create_resources(cpu_cores, memory_mb, disk_size_gb)

    req = pb.CreateSessionRequest(
        owner_type="SERVICE",
        owner_id="self",
        allow_inbound=allow_inbound,
        allow_outbound=allow_outbound,
        metadata=dict(metadata or {}),
        env=dict(env or {}),
        ssh_authorized_keys=list(ssh_authorized_keys or []),
        enable_opencode=enable_opencode,
        clone_repo_url=clone_repo_url or "",
        sticky=sticky,
        wait_ready=wait_ready,
        wait_for_runtime=wait_for_runtime,
        setup_env=dict(setup_env or {}),
        setup_secrets=dict(setup_secrets or {}),
    )
    if name:
        req.name = name
    if workspace_id and workspace_id.strip():
        req.workspace_id = workspace_id.strip()
    if tags:
        req.tags.extend(_normalize_tags(tags))
    if max_duration is not None:
        req.max_duration.CopyFrom(duration(max_duration))
    if idle_timeout_minutes is not None:
        req.idle_timeout_minutes = idle_timeout_minutes
    if pause_retention is not None:
        req.pause_retention.CopyFrom(duration(pause_retention))
    if cpu_cores is not None:
        req.cpu_cores = cpu_cores
    if memory_mb is not None:
        req.memory_mb = memory_mb
    if disk_size_gb is not None:
        req.disk_size_gb = disk_size_gb
    if sum(1 for source in (snapshot_id, image, from_template_spec) if source) > 1:
        raise ValueError(
            "only one of image, snapshot_id, or from_template_spec may be set"
        )
    if snapshot_id:
        req.snapshot_id = snapshot_id
    if image:
        req.registry_ref = _registry_ref(image)
    if from_template_spec:
        req.template_spec_id = _template_id(from_template_spec)
    if github_token:
        req.env["GH_TOKEN"] = github_token
        req.env["GIT_TOKEN"] = github_token
    for mount in volumes or []:
        if isinstance(mount, dict):
            req.volumes.append(
                pb.VolumeMount(
                    volume_id=mount["volume_id"],
                    mount_path=mount["mount_path"],
                    readonly=mount.get("read_only", False),
                )
            )
        else:
            volume_id, mount_path, *rest = mount
            req.volumes.append(
                pb.VolumeMount(volume_id=volume_id, mount_path=mount_path, readonly=bool(rest and rest[0]))
            )
    return req


def _create_wait_timeout(timeout: float | None, from_template_spec: str | Template | None) -> float:
    if timeout is not None:
        return timeout
    return 7200 if from_template_spec else 180


def build_issue_ssh_cert_request(session_id: str, public_key: str, *, ttl: float | None = None):
    """Build an IssueSandboxSSHCertRequest. Shared by the sync and async clients."""
    import datetime as _dt

    from ._pb import ssh_pb

    req = ssh_pb.IssueSandboxSSHCertRequest(
        session_id=session_id.strip(),
        public_key=public_key.strip(),
    )
    if ttl is not None:
        from google.protobuf.duration_pb2 import Duration

        d = Duration()
        d.FromTimedelta(_dt.timedelta(seconds=ttl))
        req.requested_ttl.CopyFrom(d)
    return req
