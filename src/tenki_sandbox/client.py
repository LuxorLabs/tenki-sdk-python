from __future__ import annotations

import os
import warnings

from ._convert import egress_http_rule_proto, identity, sandbox_info, session_metrics, workspace_usage_limit
from ._pagination import advance_page_token
from ._pb import pb
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
from .models import (
    EgressHttpRule,
    Identity,
    Image,
    SessionMetrics,
    TailnetAttachment,
    TailnetAuthKey,
    Template,
    WorkspaceUsageLimit,
)
from .preview_url import PreviewUrls
from .registry import Registry
from .egress_policies import EgressPolicies
from .resources import Snapshots, Templates, Volumes
from .sandbox import Sandbox
from .secret_files import RuntimeSecretFile, valid_secret_files
from .template_spec import TemplateSpec
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
        api_key: str | None = None,
        base_url: str | None = None,
        gateway_url: str | None = None,
        timeout: float | None = None,
        warning_handler: WarningHandler | None = default_warning_handler,
        rpc=None,
    ) -> None:
        self.base_url = resolve_base_url(base_url)
        self.gateway_url = resolve_gateway_url(gateway_url, self.base_url)
        self._gateway_explicit = bool((gateway_url or os.getenv(ENV_GATEWAY_URL) or "").strip())
        self.auth_token = (
            resolve_auth_token(auth_token, api_key)
            if rpc is None or auth_token is not None or api_key is not None
            else "tk_test"
        )
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
        self.egress_policies = EgressPolicies(self._rpc)
        self.snapshots = Snapshots(self._rpc)
        self.templates = Templates(self._rpc)
        self.registry = Registry(self._rpc)

    def __repr__(self) -> str:
        return f"Client(base_url={self.base_url!r}, auth_token='[REDACTED]')"

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
        allow_domains: list[str] | None = None,
        allow_cidrs: list[str] | None = None,
        intercept_https: bool = False,
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
        secret_overrides: dict[str, str] | None = None,
        secret_policies: list[str] | None = None,
        enable_opencode: bool = False,
        clone_repo_url: str | None = None,
        volumes: list[dict | tuple] | None = None,
        github_token: str | None = None,
        sticky: bool = False,
        wait_for_runtime: bool = False,
        tailnet: TailnetAttachment | None = None,
        _owns_client: bool = False,
    ) -> Sandbox:
        """Create a sandbox.

        ``cpu_cores`` is 1-128 and ``memory_mb`` is 128-524288 and must be even;
        the workspace's size limit may be lower.
        ``disk_size_gb`` is 5-100: a create with no ``image``, ``snapshot_id`` or
        ``from_template_spec`` defaults to 5, while one with a source inherits
        that source's disk unless this is set.

        ``allow_domains``/``allow_cidrs`` restrict outbound access; both are
        ignored when ``allow_outbound`` is false. ``intercept_https`` (requires
        ``allow_domains``) terminates HTTPS at the egress proxy so each request's
        Host must match the TLS server name; ``http_rules`` (requires
        ``intercept_https``) refuses with 403 any HTTP request on ports 80 and
        443 that matches none of the rules. ``egress_policies`` pins named
        workspace egress policies that the server merges with these options.

        ``idle_timeout_minutes`` is deprecated and ignored: sandboxes do not
        auto-pause on idle.
        """
        req = build_create_session_request(
            workspace_id=workspace_id,
            name=name,
            allow_inbound=allow_inbound,
            allow_outbound=allow_outbound,
            allow_domains=allow_domains,
            allow_cidrs=allow_cidrs,
            intercept_https=intercept_https,
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
            github_token=github_token,
            sticky=sticky,
            # Normal readiness waits client-side so WaitSession can expose and
            # prewarm the data plane while provisioning continues.
            wait_ready=wait and wait_for_runtime,
            wait_for_runtime=wait_for_runtime,
            tailnet=tailnet,
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
            return Sandbox(
                self,
                sandbox_info(resp.session),
            )
        except Exception as err:
            raise map_rpc_error(err) from err

    def get_session_metrics(
        self, sandbox_id: str, *, window: float | int | object | None = None
    ) -> SessionMetrics:
        req = pb.GetSessionMetricsRequest(session_id=sandbox_id)
        if window is not None:
            req.window.CopyFrom(duration(window))
        try:
            return session_metrics(self._rpc.get_session_metrics(req))
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
                    Sandbox(
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
                pin_host_key=resp.pin_host_key,
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
    allow_domains: list[str] | None = None,
    allow_cidrs: list[str] | None = None,
    intercept_https: bool = False,
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
    github_token: str | None = None,
    sticky: bool = False,
    wait_ready: bool = False,
    wait_for_runtime: bool = False,
    tailnet: TailnetAttachment | None = None,
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
        secret_overrides=dict(secret_overrides or {}),
        secret_files=list(secret_files or []),
        secret_policies=list(secret_policies or []),
        egress_policies=list(egress_policies or []),
    )
    if not valid_secret_files(req.secret_files, 0) or any(file.WhichOneof("format") == "source" for file in req.secret_files):
        raise ValueError("Invalid direct secret files; image source paths require a template build")
    if name:
        req.name = name
    if workspace_id and workspace_id.strip():
        req.workspace_id = workspace_id.strip()
    if tags:
        req.tags.extend(_normalize_tags(tags))
    if allow_domains or allow_cidrs or intercept_https or http_rules:
        req.egress.CopyFrom(
            pb.SessionEgressPolicy(
                allow_domains=list(allow_domains or []),
                allow_cidrs=list(allow_cidrs or []),
                intercept_https=intercept_https,
                http_rules=[egress_http_rule_proto(rule) for rule in http_rules or []],
            )
        )
    if max_duration is not None:
        req.max_duration.CopyFrom(duration(max_duration))
    if idle_timeout_minutes is not None:
        warnings.warn(
            "idle_timeout_minutes is deprecated and ignored; sandboxes do not "
            "auto-pause on idle. Use max_duration to bound a session's lifetime.",
            DeprecationWarning,
            stacklevel=3,
        )
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
    if direct_runtime is not None:
        if from_template_spec:
            raise ValueError("direct_runtime conflicts with from_template_spec")
        req.runtime.CopyFrom(direct_runtime._to_direct_runtime_proto())
        if any(target in req.env for target in req.runtime.secret_env):
            raise ValueError("secret target conflicts with session environment")
    if tailnet is not None:
        req.tailnet.CopyFrom(_tailnet_attachment_proto(tailnet))
    return req


def _tailnet_attachment_proto(attachment: TailnetAttachment):
    if attachment.provider != "tailscale":
        raise ValueError("unsupported tailnet provider")
    if not isinstance(attachment.auth_key, TailnetAuthKey):
        raise TypeError("tailnet auth_key must be a TailnetAuthKey")
    exit_policies = {
        None: pb.TAILNET_EXIT_POLICY_UNSPECIFIED,
        "tenki_allowlist": pb.TAILNET_EXIT_POLICY_TENKI_ALLOWLIST,
        "exit_node_managed": pb.TAILNET_EXIT_POLICY_EXIT_NODE_MANAGED,
    }
    pause_policies = {
        None: pb.TAILNET_EPHEMERAL_PAUSE_POLICY_UNSPECIFIED,
        "reject": pb.TAILNET_EPHEMERAL_PAUSE_POLICY_REJECT,
        "recreate_on_resume": pb.TAILNET_EPHEMERAL_PAUSE_POLICY_RECREATE_ON_RESUME,
    }
    try:
        exit_policy = exit_policies[attachment.exit_policy]
    except KeyError as err:
        raise ValueError(f"unsupported tailnet exit policy {attachment.exit_policy!r}") from err
    try:
        pause_policy = pause_policies[attachment.ephemeral_pause_policy]
    except KeyError as err:
        raise ValueError(
            f"unsupported tailnet ephemeral pause policy {attachment.ephemeral_pause_policy!r}"
        ) from err
    return pb.TailnetAttachment(
        provider=attachment.provider,
        hostname=attachment.hostname,
        tags=attachment.tags,
        ephemeral=attachment.ephemeral,
        control_url=attachment.control_url,
        exit_node=attachment.exit_node,
        accept_routes=attachment.accept_routes,
        wake_on_connect=attachment.wake_on_connect,
        expose_ports=attachment.expose_ports,
        wait_for_online=attachment.wait_for_online,
        auth_key=attachment.auth_key._wire_value(),
        exit_policy=exit_policy,
        ephemeral_pause_policy=pause_policy,
    )


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
