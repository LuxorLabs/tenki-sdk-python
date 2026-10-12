from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

from .constants import (
    DEFAULT_BASE_URL,
    ENV_API_ENDPOINT,
    ENV_API_KEY,
    ENV_API_URL,
    ENV_AUTH_TOKEN,
    ENV_GATEWAY_URL,
)
from .errors import InvalidAuthTokenError, MissingAuthTokenError, map_rpc_error


def _validate_auth_token(auth_token: str) -> str:
    token = auth_token.strip()
    if not token:
        raise MissingAuthTokenError()
    if not token.startswith("tk_"):
        raise InvalidAuthTokenError()
    return token


def resolve_auth_token(auth_token: str | None, api_key: str | None = None) -> str:
    # Strip first so a whitespace-only value counts as absent; auth_token wins over its alias.
    explicit = (auth_token or "").strip()
    alias = (api_key or "").strip()
    token = explicit or alias or os.getenv(ENV_AUTH_TOKEN) or os.getenv(ENV_API_KEY)
    return _validate_auth_token(token or "")


def resolve_base_url(base_url: str | None) -> str:
    return (
        base_url
        or os.getenv(ENV_API_ENDPOINT)
        or os.getenv(ENV_API_URL)
        or DEFAULT_BASE_URL
    ).strip()


def _parse_url(url: str):
    normalized = url.strip()
    if "://" not in normalized:
        normalized = "https://" + normalized
    return urlparse(normalized)


def resolve_gateway_url(gateway_url: str | None, base_url: str) -> str:
    explicit = (gateway_url or os.getenv(ENV_GATEWAY_URL) or "").strip()
    if explicit:
        return explicit
    parsed = _parse_url(base_url)
    host = parsed.hostname or parsed.netloc or parsed.path
    port = parsed.port
    if parsed.hostname is None:
        host = host.split("/", 1)[0]
        if ":" in host:
            maybe_host, maybe_port = host.rsplit(":", 1)
            if maybe_port.isdigit():
                host = maybe_host
                port = int(maybe_port)
    if host.startswith("api."):
        host = "sandbox-gateway." + host.removeprefix("api.")
    elif host.startswith("app."):
        host = "sandbox-gateway." + host.removeprefix("app.")
    else:
        host = "sandbox-gateway." + host
    if port is not None:
        host = f"{host}:{port}"
    scheme = "wss" if parsed.scheme != "http" else "ws"
    return f"{scheme}://{host}"


def grpc_target(base_url: str) -> tuple[str, bool]:
    parsed = _parse_url(base_url)
    secure = parsed.scheme != "http"
    target = parsed.hostname or parsed.netloc or parsed.path
    if target and parsed.port is not None:
        target = f"{target}:{parsed.port}"
    elif target and ":" not in target:
        target = f"{target}:{443 if secure else 80}"
    return target, secure


def http_base_url(base_url: str) -> str:
    parsed = _parse_url(base_url)
    scheme = "http" if parsed.scheme == "http" else "https"
    host = parsed.hostname or parsed.netloc or parsed.path
    if parsed.port is not None:
        host = f"{host}:{parsed.port}"
    return f"{scheme}://{host}"


class _RouteNotFoundError(Exception):
    """The HTTP path itself is not mounted (plain 404, non-Connect body)."""


class _ConnectError(Exception):
    """Connect-protocol error shaped like a grpc.RpcError for map_rpc_error."""

    class _Code:
        def __init__(self, name: str) -> None:
            self.name = name

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message or code)
        self._code = self._Code((code or "unknown").upper())
        self._message = message or code

    def code(self) -> "_ConnectError._Code":
        return self._code

    def details(self) -> str:
        return self._message


def auth_metadata(auth_token: str) -> tuple[tuple[str, str], ...]:
    token = _validate_auth_token(auth_token)
    return (("authorization", f"Bearer {token}"),)


@dataclass
class GrpcConfig:
    base_url: str
    auth_token: str = field(repr=False)
    timeout: float | None = None


# Server-/bidi-streaming RPCs (return an iterator sync / async iterator aio).
# They are open-ended — long-lived (run, dial, host_port_tunnel) or
# caller-bounded (wait_session passes its own timeout) — so the client-wide
# unary timeout must NOT be applied as their deadline, or a persistent stream
# dies with DEADLINE_EXCEEDED. Shared with the aio transport.
_STREAMING_OUT = frozenset({"wait_session", "run", "read_file_stream", "dial", "host_port_tunnel"})


class GrpcSandboxRPC:
    _methods = {
        "create_session": "CreateSession",
        "create_session_credential": "CreateSessionCredential",
        "get_session": "GetSession",
        "get_session_metrics": "GetSessionMetrics",
        "wait_session": "WaitSession",
        "list_workspace_sandboxes": "ListWorkspaceSandboxes",
        "update_session": "UpdateSession",
        "terminate_session": "TerminateSession",
        "extend_session": "ExtendSession",
        "pause_session": "PauseSession",
        "resume_session": "ResumeSession",
        "run": "Run",
        "write_file": "WriteFile",
        "read_file": "ReadFile",
        "read_file_stream": "ReadFileStream",
        "write_file_stream": "WriteFileStream",
        "stat": "Stat",
        "mkdir": "Mkdir",
        "remove": "Remove",
        "list": "List",
        "git_operation": "GitOperation",
        "expose_port": "ExposePort",
        "unexpose_port": "UnexposePort",
        "list_exposed_ports": "ListExposedPorts",
        "create_preview_url": "CreatePreviewUrl",
        "delete_preview_url": "DeletePreviewUrl",
        "bind_preview_url": "BindPreviewUrl",
        "unbind_preview_url": "UnbindPreviewUrl",
        "list_preview_urls": "ListPreviewUrls",
        "get_preview_url": "GetPreviewUrl",
        "create_volume": "CreateVolume",
        "get_volume": "GetVolume",
        "list_volumes": "ListVolumes",
        "update_volume": "UpdateVolume",
        "delete_volume": "DeleteVolume",
        "create_egress_policy": "CreateEgressPolicy",
        "update_egress_policy": "UpdateEgressPolicy",
        "get_egress_policy": "GetEgressPolicy",
        "list_egress_policies": "ListEgressPolicies",
        "delete_egress_policy": "DeleteEgressPolicy",
        "resize_volume": "ResizeVolume",
        "attach_volume": "AttachVolume",
        "detach_volume": "DetachVolume",
        "create_snapshot": "CreateSnapshot",
        "get_snapshot": "GetSnapshot",
        "get_snapshot_download_url": "GetSnapshotDownloadURL",
        "update_ssh_authorized_keys": "UpdateSSHAuthorizedKeys",
        "list_session_snapshots": "ListSessionSnapshots",
        "list_workspace_snapshots": "ListWorkspaceSnapshots",
        "list_dangling_snapshots": "ListDanglingSnapshots",
        "delete_snapshot": "DeleteSnapshot",
        "update_snapshot": "UpdateSnapshot",
        "create_template": "CreateTemplate",
        "get_template": "GetTemplate",
        "list_templates": "ListTemplates",
        "update_template": "UpdateTemplate",
        "delete_template": "DeleteTemplate",
        "build_template": "BuildTemplate",
        "get_template_build": "GetTemplateBuild",
        "list_active_template_builds": "ListActiveTemplateBuilds",
        "cancel_template_build": "CancelTemplateBuild",
        "who_am_i": "WhoAmI",
        "get_workspace_sandbox_usage": "GetWorkspaceSandboxUsage",
        "publish_registry_image": "PublishRegistryImage",
        "set_registry_image_visibility": "SetRegistryImageVisibility",
        "delete_registry_image": "DeleteRegistryImage",
        "delete_registry_image_version": "DeleteRegistryImageVersion",
        "list_registry_images": "ListRegistryImages",
        "get_registry_image": "GetRegistryImage",
        "resolve_registry_ref": "ResolveRegistryRef",
        "share_image": "ShareImage",
        "revoke_registry_share_grant": "RevokeRegistryShareGrant",
        "list_registry_share_grants": "ListRegistryShareGrants",
        "unshare_registry_image": "UnshareRegistryImage",
        "dial": "Dial",
        "host_port_tunnel": "HostPortTunnel",
    }

    _ssh_methods = {
        "issue_ssh_cert": "IssueSandboxSSHCert",
        "list_active_ssh_gateways": "ListActiveSSHGateways",
    }

    _SSH_SERVICE = "tenki.sandbox.v1.SSHGatewayClientService"
    # Engines predating the client-service split (#3866) serve the same two
    # RPCs (same request/response messages) on the legacy combined service.
    _SSH_LEGACY_SERVICE = "tenki.sandbox.v1.SSHGatewayService"

    def __init__(self, config: GrpcConfig) -> None:
        import grpc

        from tenki_sandbox._internal.proto.tenki.sandbox.v1 import sandbox_pb2_grpc

        target, secure = grpc_target(config.base_url)
        if not target:
            target = config.base_url
        if not secure:
            self._channel = grpc.insecure_channel(target)
        else:
            self._channel = grpc.secure_channel(target, grpc.ssl_channel_credentials())
        self._stub = sandbox_pb2_grpc.SandboxServiceStub(self._channel)
        self._http_base = http_base_url(config.base_url)
        self._metadata = auth_metadata(config.auth_token)
        self._timeout = config.timeout

    def close(self) -> None:
        close = getattr(self._channel, "close", None)
        if callable(close):
            close()

    def __getattr__(self, name: str) -> Any:
        rpc_name = self._methods.get(name)
        if rpc_name is not None:
            method = getattr(object.__getattribute__(self, "_stub"), rpc_name)

            default_timeout = None if name in _STREAMING_OUT else self._timeout

            def call(request, *args, **kwargs):
                timeout = kwargs.pop("timeout", default_timeout)
                try:
                    return method(
                        request,
                        *args,
                        metadata=self._metadata,
                        timeout=timeout,
                        **kwargs,
                    )
                except Exception as err:
                    raise map_rpc_error(err) from err

            return call

        rpc_name = self._ssh_methods.get(name)
        if rpc_name is None:
            raise AttributeError(name)

        def ssh_call(request, *args, **kwargs):
            timeout = kwargs.pop("timeout", self._timeout)
            try:
                return self._connect_unary(
                    self._SSH_SERVICE, rpc_name, request, timeout=timeout
                )
            except _RouteNotFoundError:
                pass
            try:
                return self._connect_unary(
                    self._SSH_LEGACY_SERVICE, rpc_name, request, timeout=timeout
                )
            except _RouteNotFoundError as err:
                raise map_rpc_error(
                    _ConnectError(
                        "unimplemented", f"{rpc_name}: not served by this engine"
                    )
                ) from err

        return ssh_call

    def _connect_unary(
        self, service: str, rpc_name: str, request, *, timeout: float | None = None
    ):
        return connect_unary_blocking(
            self._http_base, self._metadata, service, rpc_name, request, timeout=timeout
        )


def connect_unary_blocking(
    http_base: str,
    metadata: tuple[tuple[str, str], ...],
    service: str,
    rpc_name: str,
    request,
    *,
    timeout: float | None = None,
):
    # SSHGatewayClientService is called via the Connect protocol (a plain
    # HTTPS POST with a binary proto body), matching the Go SDK. Edge
    # ingress does not route the raw-gRPC protocol for this service.
    #
    # Blocking (urllib) by design: the async client reuses this via
    # loop.run_in_executor since these two RPCs are rare and not hot-path.
    import json
    import urllib.error
    import urllib.request

    from tenki_sandbox._internal.proto.tenki.sandbox.v1 import (
        ssh_gateway_client_pb2,
    )

    response_types = {
        "IssueSandboxSSHCert": ssh_gateway_client_pb2.IssueSandboxSSHCertResponse,
        "ListActiveSSHGateways": ssh_gateway_client_pb2.ListActiveSSHGatewaysResponse,
    }
    response_cls = response_types[rpc_name]

    req = urllib.request.Request(
        f"{http_base}/{service}/{rpc_name}",
        data=request.SerializeToString(),
        method="POST",
    )
    req.add_header("Content-Type", "application/proto")
    req.add_header("Connect-Protocol-Version", "1")
    for key, value in metadata:
        req.add_header(key, value)
    try:
        with urllib.request.urlopen(req, timeout=timeout or 30) as resp:
            body = resp.read()
    except urllib.error.HTTPError as err:
        code, message = "", ""
        try:
            payload = json.loads(err.read().decode("utf-8", errors="replace"))
            code = payload.get("code", "")
            message = payload.get("message", "")
        except Exception:
            pass
        if not code:
            if err.code == 404:
                raise _RouteNotFoundError(f"{service}/{rpc_name}") from err
            code = "unknown"
        if not message:
            message = f"HTTP {err.code}"
        raise map_rpc_error(_ConnectError(code, message)) from err
    except Exception as err:
        raise map_rpc_error(err) from err
    response = response_cls()
    response.ParseFromString(body)
    return response
