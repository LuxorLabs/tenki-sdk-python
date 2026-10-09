from __future__ import annotations

import dataclasses
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Sequence

from ._convert import exposed_port, sandbox_info
from ._pb import pb
from ._time import duration, seconds
from .data_plane import (
    DataPlaneCredentialManager,
    GrpcDataPlaneRPC,
    _DataPlaneEndpointHints,
)
from .errors import (
    PauseFailedError,
    PauseWaitCancelledError,
    SessionNotFoundError,
    WaitReadyFailedError,
    map_rpc_error,
)
from .fs import SandboxFS
from .git import SandboxGit
from .models import (
    CommandResult,
    EgressHttpRule,
    ExposedPort,
    Image,
    SandboxInfo,
    SessionEgressPolicy,
    SessionMetrics,
    TailnetAttachment,
    TailnetStatus,
    Template,
    VolumeMount,
)
from .process import Process
from .secret_files import RuntimeSecretFile
from .template_spec import TemplateSpec
from .warning import (
    SandboxWarning,
    WarningHandler,
    default_warning_handler,
    emit_warnings,
    sandbox_warnings_from_proto,
)


class _SessionRPC:
    """Defers RPC resolution so fs picks up a data plane bootstrapped after init."""

    def __init__(self, sandbox: "Sandbox") -> None:
        self._sandbox = sandbox

    def __getattr__(self, name: str):
        return getattr(self._sandbox._session_rpc, name)


class Sandbox:
    def __init__(
        self,
        client,
        info: SandboxInfo,
        *,
        warnings: tuple[SandboxWarning, ...] = (),
        owns_client: bool = False,
        data_plane_endpoint: str = "",
        data_plane_credential=None,
        data_plane_route_status=None,
    ) -> None:
        self._client = client
        self._info = info
        self.warnings = warnings
        self._owns_client = owns_client
        self._detached = False
        self._data_plane_endpoint_hints = _DataPlaneEndpointHints()
        self._data_plane_lock = threading.RLock()
        self._data_plane_credentials = None
        self._data_plane_rpc = None
        self._data_plane_verified = False
        self._data_plane_from_hint = False
        self._data_plane_warm_started = False
        self._apply_data_plane_response(
            data_plane_endpoint, data_plane_credential, data_plane_route_status
        )
        self.fs = SandboxFS(_SessionRPC(self), info.id)
        self.git = SandboxGit(self)

    @property
    def _session_rpc(self):
        return self._ensure_data_plane() or self._client._rpc

    def _apply_data_plane_response(
        self,
        endpoint: str = "",
        credential=None,
        route_status=None,
        *,
        from_hint: bool = False,
    ) -> None:
        if route_status == pb.DATA_PLANE_ROUTE_STATUS_FAILED:
            return
        endpoint = (endpoint or "").strip()
        with self._data_plane_lock:
            if credential is not None and self._data_plane_credentials is None:
                self._data_plane_credentials = DataPlaneCredentialManager(
                    self, credential
                )
            if not endpoint:
                hint = self._data_plane_endpoint_hints.current()
                if hint and self._data_plane_credentials is not None:
                    endpoint = hint
                    from_hint = True
                else:
                    return
            if not from_hint:
                self._data_plane_endpoint_hints.remember(endpoint)
            if (
                self._data_plane_rpc is not None
                and getattr(self._data_plane_rpc, "endpoint", "") == endpoint
            ):
                if credential is not None and self._data_plane_credentials is not None:
                    self._data_plane_credentials.set(credential)
                self._data_plane_from_hint = from_hint
                if route_status == pb.DATA_PLANE_ROUTE_STATUS_VERIFIED:
                    self._data_plane_verified = True
                return
            if self._data_plane_rpc is not None:
                self._data_plane_rpc.close()
            if credential is not None and self._data_plane_credentials is not None:
                self._data_plane_credentials.stop()
            if credential is not None or self._data_plane_credentials is None:
                self._data_plane_credentials = DataPlaneCredentialManager(
                    self, credential
                )
            self._data_plane_rpc = GrpcDataPlaneRPC(
                endpoint, self._data_plane_credentials
            )
            self._data_plane_from_hint = from_hint
            self._data_plane_verified = (
                route_status == pb.DATA_PLANE_ROUTE_STATUS_VERIFIED
            )
            warm_target = None if self._data_plane_verified else self._data_plane_rpc
        if warm_target is not None:
            self._start_data_plane_warm(warm_target)

    def _start_data_plane_warm(self, rpc) -> None:
        # Open the connection so the first real op skips the handshake; the probe may be rejected for want of a credential, so this must not set _data_plane_verified.
        if self._data_plane_warm_started:
            return
        self._data_plane_warm_started = True

        def _warm() -> None:
            try:
                rpc.stat(
                    pb.StatRequest(session_id=self.id, path="/home/tenki"),
                    metadata=(("x-tenki-dataplane-probe", "warmup"),),
                )
            except Exception:
                pass

        threading.Thread(target=_warm, name="tenki-dp-warm", daemon=True).start()

    def _ensure_data_plane(self, *, verify_serving: bool = True):
        # CreateSessionResponse returns the endpoint empty pre-provision; bootstrap
        # via CreateSessionCredential on first data-plane use (mirrors the Go SDK).
        with self._data_plane_lock:
            if self._data_plane_rpc is None:
                if (
                    self._data_plane_credentials is not None
                    and self._data_plane_credentials.current()
                ):
                    endpoint = self._data_plane_endpoint_hints.resolve(
                        self._renew_data_plane
                    )
                else:
                    endpoint = self._renew_data_plane()
                if not endpoint:
                    return None
                if self._data_plane_rpc is None:
                    self._apply_data_plane_response(endpoint, from_hint=True)
            if verify_serving and not self._data_plane_verified:
                try:
                    ensure_serving = getattr(
                        self._data_plane_rpc, "ensure_serving", None
                    )
                    if callable(ensure_serving):
                        ensure_serving(self.id)
                except Exception as err:
                    if self._data_plane_credentials is not None:
                        self._data_plane_credentials.stop()
                        self._data_plane_credentials = None
                    self._data_plane_rpc.close()
                    self._data_plane_rpc = None
                    self._data_plane_verified = False
                    self._data_plane_from_hint = False
                    raise map_rpc_error(err) from err
                self._data_plane_verified = True
            return self._data_plane_rpc

    def _renew_data_plane(self) -> str:
        try:
            resp = self._client._rpc.create_session_credential(
                pb.CreateSessionCredentialRequest(session_id=self.id)
            )
        except AttributeError:
            return ""
        except Exception as err:
            raise map_rpc_error(err) from err
        endpoint = resp.data_plane_endpoint.strip()
        self._apply_data_plane_response(
            endpoint,
            resp.credential if resp.HasField("credential") else None,
            resp.route_status,
        )
        if resp.route_status == pb.DATA_PLANE_ROUTE_STATUS_FAILED:
            return ""
        if endpoint:
            return endpoint
        if self._data_plane_rpc is not None:
            return self._data_plane_endpoint_hints.current()
        return ""

    def _data_plane_provisioning_pending(self, rpc) -> bool:
        with self._data_plane_lock:
            return self._data_plane_rpc is rpc and not self._data_plane_verified

    def _mark_data_plane_serving(self, rpc) -> None:
        with self._data_plane_lock:
            if self._data_plane_rpc is rpc:
                self._data_plane_verified = True

    def _refresh_hinted_data_plane(self, rpc) -> None:
        with self._data_plane_lock:
            if self._data_plane_rpc is not rpc or not self._data_plane_from_hint:
                return
            self._renew_data_plane()

    def _reset_data_plane(self) -> None:
        with self._data_plane_lock:
            if self._data_plane_credentials is not None:
                self._data_plane_credentials.stop()
                self._data_plane_credentials = None
            if self._data_plane_rpc is not None:
                self._data_plane_rpc.close()
                self._data_plane_rpc = None
            self._data_plane_endpoint_hints.clear()
            self._data_plane_verified = False
            self._data_plane_from_hint = False

    @classmethod
    def create(
        cls,
        *,
        auth_token: str | None = None,
        api_key: str | None = None,
        base_url: str | None = None,
        gateway_url: str | None = None,
        timeout: float | None = None,
        warning_handler: WarningHandler | None = default_warning_handler,
        workspace_id: str | None = None,
        name: str | None = None,
        wait: bool = True,
        allow_inbound: bool = True,
        allow_outbound: bool = True,
        allow_domains: list[str] | None = None,
        allow_cidrs: list[str] | None = None,
        intercept_https: bool = False,
        http_rules: list[EgressHttpRule] | None = None,
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
        direct_runtime: TemplateSpec | None = None,
        secret_files: list[RuntimeSecretFile] | None = None,
        secret_policies: list[str] | None = None,
        secret_overrides: dict[str, str] | None = None,
        setup_env: dict[str, str] | None = None,
        setup_secrets: dict[str, str] | None = None,
        enable_opencode: bool = False,
        clone_repo_url: str | None = None,
        volumes: list[dict | tuple] | None = None,
        github_token: str | None = None,
        sticky: bool = False,
        wait_for_runtime: bool = False,
        tailnet: TailnetAttachment | None = None,
    ) -> "Sandbox":
        """Create a sandbox on a client this sandbox owns and closes.

        Accepts every ``Client`` connection argument and every ``Client.create``
        argument; see ``Client.create`` for resource bounds and egress rules.
        ``timeout`` is the client transport timeout, not the readiness budget;
        construct a client and call ``Client.create(timeout=...)`` to set that.
        """
        from .client import Client

        # timeout is the transport timeout here; Client.create's is the wait budget.
        client = Client(
            auth_token=auth_token,
            api_key=api_key,
            base_url=base_url,
            gateway_url=gateway_url,
            timeout=timeout,
            warning_handler=warning_handler,
        )
        try:
            return client.create(
                workspace_id=workspace_id,
                name=name,
                wait=wait,
                allow_inbound=allow_inbound,
                allow_outbound=allow_outbound,
                allow_domains=allow_domains,
                allow_cidrs=allow_cidrs,
                intercept_https=intercept_https,
                http_rules=http_rules,
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
                direct_runtime=direct_runtime,
                secret_overrides=secret_overrides,
                secret_files=secret_files,
                secret_policies=secret_policies,
                setup_env=setup_env,
                setup_secrets=setup_secrets,
                enable_opencode=enable_opencode,
                clone_repo_url=clone_repo_url,
                volumes=volumes,
                github_token=github_token,
                sticky=sticky,
                wait_for_runtime=wait_for_runtime,
                tailnet=tailnet,
                _owns_client=True,
            )
        except WaitReadyFailedError:
            # The sandbox rides out on the error and owns this client; closing it
            # here would strip the caller of the handle they terminate it with.
            raise
        except BaseException:
            client.close()
            raise

    @property
    def id(self) -> str:
        return self._info.id

    @property
    def info(self) -> SandboxInfo:
        return self._info

    @property
    def state(self) -> str:
        return self._info.state

    @property
    def name(self) -> str:
        return self._info.name

    @property
    def has_runtime_secrets(self) -> bool:
        return self._info.has_runtime_secrets

    @property
    def egress(self) -> SessionEgressPolicy:
        return self._info.egress

    @property
    def tailnet_status(self) -> TailnetStatus | None:
        return self._info.tailnet_status

    def __enter__(self) -> "Sandbox":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if not self._detached:
            self.close()
        if self._owns_client:
            self._client.close()

    def detach(self) -> None:
        self._detached = True

    def refresh(self) -> SandboxInfo:
        self._info = self._client.get(self.id).info
        return self._info

    def metrics(self, *, window: float | int | object | None = None) -> SessionMetrics:
        return self._client.get_session_metrics(self.id, window=window)

    def wait_ready(self, timeout: float = 180) -> None:
        try:
            self._wait_ready_stream(timeout)
            return
        except Exception as err:
            if not _is_unimplemented(err):
                if hasattr(err, "code"):
                    raise map_rpc_error(err) from err
                raise
        self._wait_ready_poll(timeout)

    def _wait_ready_stream(self, timeout: float) -> None:
        for resp in self._client._rpc.wait_session(
            pb.WaitSessionRequest(session_id=self.id), timeout=timeout
        ):
            self._info = sandbox_info(resp.session)
            self._apply_data_plane_response(
                resp.data_plane_endpoint,
                resp.credential if resp.HasField("credential") else None,
                resp.route_status,
            )
            if self.state == "RUNNING":
                return
            if self.state in {"TERMINATING", "TERMINATED"}:
                raise RuntimeError(f"sandbox entered terminal state: {self.state}")
        if self.state == "RUNNING":
            return
        if self.state in {"TERMINATING", "TERMINATED"}:
            raise RuntimeError(f"sandbox entered terminal state: {self.state}")
        raise TimeoutError(f"timeout waiting for sandbox {self.id}")

    def _wait_ready_poll(self, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while True:
            self.refresh()
            if self.state == "RUNNING":
                return
            if self.state in {"TERMINATING", "TERMINATED"}:
                raise RuntimeError(f"sandbox entered terminal state: {self.state}")
            if time.monotonic() >= deadline:
                break
            time.sleep(0.5)
        raise TimeoutError(f"timeout waiting for sandbox {self.id}")

    def wait_paused(
        self,
        timeout: float = 180,
        *,
        cancel_event: threading.Event | None = None,
    ) -> None:
        """Wait locally for a durable pause without cancelling the remote pause."""
        deadline = time.monotonic() + timeout if timeout > 0 else None
        started = time.monotonic()
        saw_pausing = False
        while deadline is None or time.monotonic() < deadline:
            if cancel_event is not None and cancel_event.is_set():
                raise PauseWaitCancelledError(
                    f"wait for sandbox {self.id} to pause cancelled locally"
                )
            try:
                self.refresh()
            except SessionNotFoundError:
                pass
            else:
                if self.state == "PAUSED":
                    return
                if self.state in {"TERMINATING", "TERMINATED"}:
                    raise RuntimeError(f"sandbox entered terminal state: {self.state}")
                if self.state == "PAUSING":
                    saw_pausing = True
                elif self.state == "RUNNING" and (
                    saw_pausing or time.monotonic() - started > 3
                ):
                    raise PauseFailedError(
                        f"pause failed: sandbox {self.id} reverted to {self.state}"
                    )
            if cancel_event is not None:
                if cancel_event.wait(0.5):
                    raise PauseWaitCancelledError(
                        f"wait for sandbox {self.id} to pause cancelled locally"
                    )
            else:
                time.sleep(0.5)
        raise TimeoutError(f"timeout waiting for sandbox {self.id} to pause")

    def start(
        self,
        *argv: str | Sequence[str],
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout: float | int | object | None = None,
        stdin: bytes | str | None = None,
        privileged: bool = False,
    ) -> Process:
        rpc = self._ensure_data_plane(verify_serving=False)
        if rpc is None:
            raise RuntimeError("sandbox data-plane endpoint unavailable")
        return Process(
            self,
            self.id,
            argv,
            cwd=cwd,
            env=env,
            timeout=timeout,
            stdin=stdin,
            privileged=privileged,
        )

    def exec(
        self,
        *argv: str | Sequence[str],
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout: float | int | object | None = None,
        input: bytes | str | None = None,
        check: bool = False,
        privileged: bool = False,
    ) -> CommandResult:
        proc = self.start(
            *argv, cwd=cwd, env=env, timeout=timeout, stdin=input, privileged=privileged
        )
        if input is None:
            proc.close_stdin()
        result = proc.wait()
        if check:
            result.check()
        return result

    def shell(self, command: str, **kwargs) -> CommandResult:
        """Run ``command`` through a login shell.

        A login shell sources the guest's startup files, so a ``cd`` in
        ``~/.bashrc`` or ``~/.profile`` runs before ``command`` and wins over
        ``cwd``. Call ``exec("bash", "-c", command)`` when the directory must hold.
        """
        return self.exec("bash", "-lc", command, **kwargs)

    def close(self) -> None:
        self._reset_data_plane()
        try:
            resp = self._client._rpc.terminate_session(
                pb.TerminateSessionRequest(session_id=self.id)
            )
            if resp.session:
                self._info = sandbox_info(resp.session)
        except Exception as err:
            raise map_rpc_error(err) from err

    terminate = close

    def close_if_open(self) -> None:
        if self.state not in {"TERMINATING", "TERMINATED"}:
            self.close()

    def update(
        self,
        *,
        name: str | None = None,
        tags: list[str] | None = None,
        sticky: bool | None = None,
        max_duration: float | int | object | None = None,
    ) -> None:
        req = pb.UpdateSessionRequest(session_id=self.id)
        if name is not None:
            req.name = name
        if tags is not None:
            normalized = [t.strip().lower() for t in tags if t.strip()]
            if normalized:
                req.tags.extend(normalized)
            else:
                req.clear_tags = True
        if sticky is not None:
            req.sticky = sticky
        if max_duration is not None:
            max_duration_proto = duration(max_duration)
            if (
                max_duration_proto.seconds + max_duration_proto.nanos / 1_000_000_000
                <= 0
            ):
                raise ValueError("max_duration must be greater than zero")
            if sticky is None:
                raise ValueError("sticky must be set when max_duration is provided")
            req.max_duration.CopyFrom(max_duration_proto)
        try:
            resp = self._client._rpc.update_session(req)
            if resp.session:
                self._info = sandbox_info(resp.session)
            warnings = sandbox_warnings_from_proto(resp.warnings)
            self.warnings = warnings
            emit_warnings(warnings, self._client._warning_handler, stacklevel=4)
        except Exception as err:
            raise map_rpc_error(err) from err

    def update_tags(self, *tags: str) -> None:
        self.update(tags=list(tags))

    def update_ssh_authorized_keys(self, keys: list[str]) -> None:
        req = pb.UpdateSSHAuthorizedKeysRequest(session_id=self.id)
        req.ssh_authorized_keys.extend(keys)
        try:
            self._client._rpc.update_ssh_authorized_keys(req)
        except Exception as err:
            raise map_rpc_error(err) from err

    def pause(self) -> None:
        self._pause(async_=False)

    def pause_async(self) -> None:
        self._pause(async_=True)

    def _pause(self, *, async_: bool) -> None:
        req = pb.PauseSessionRequest(session_id=self.id)
        setattr(req, "async", async_)
        try:
            resp = self._client._rpc.pause_session(req)
            self._info = sandbox_info(resp.session)
        except Exception as err:
            raise map_rpc_error(err) from err
        self._reset_data_plane()

    def resume(self) -> None:
        try:
            resp = self._client._rpc.resume_session(
                pb.ResumeSessionRequest(session_id=self.id)
            )
            self._info = sandbox_info(resp.session)
        except Exception as err:
            raise map_rpc_error(err) from err
        # The session may resume on a different host; re-bootstrap on next use.
        self._reset_data_plane()

    def extend(self, additional: float | int | object) -> None:
        dur = duration(additional)
        try:
            resp = self._client._rpc.extend_session(
                pb.ExtendSessionRequest(session_id=self.id, additional_duration=dur)
            )
            self._info = sandbox_info(resp.session)
        except Exception as err:
            raise map_rpc_error(err) from err

    def expose_port(
        self,
        port: int,
        *,
        ttl: float | int | object | None = None,
        slug: str | None = None,
    ) -> ExposedPort:
        req = pb.ExposePortRequest(session_id=self.id, port=port)
        ttl_seconds = seconds(ttl)
        if ttl_seconds and ttl_seconds > 0:
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
            req.expires_at.FromDatetime(expires_at)
        if slug:
            req.slug = slug
        try:
            return exposed_port(self._client._rpc.expose_port(req))
        except Exception as err:
            raise map_rpc_error(err) from err

    def unexpose_port(self, port: int) -> None:
        try:
            self._client._rpc.unexpose_port(
                pb.UnexposePortRequest(session_id=self.id, port=port)
            )
        except Exception as err:
            raise map_rpc_error(err) from err

    def list_exposed_ports(self) -> list[ExposedPort]:
        try:
            resp = self._client._rpc.list_exposed_ports(
                pb.ListExposedPortsRequest(session_id=self.id)
            )
            # The shared converter also carries the wildcard fields; rebuilding
            # ExposedPort by hand here silently dropped them, so listing an
            # exposure reported wildcard=False/UNSPECIFIED for the same port
            # expose_port had just described correctly.
            return [exposed_port(item) for item in resp.ports]
        except Exception as err:
            raise map_rpc_error(err) from err

    def attach_volume(
        self, volume_id: str, mount_path: str, *, read_only: bool = False
    ) -> None:
        try:
            self._client._rpc.attach_volume(
                pb.AttachVolumeRequest(
                    session_id=self.id,
                    volume=pb.VolumeMount(
                        volume_id=volume_id, mount_path=mount_path, readonly=read_only
                    ),
                )
            )
            mounts = list(self._info.volume_mounts)
            mounts.append(
                VolumeMount(
                    volume_id=volume_id, mount_path=mount_path, read_only=read_only
                )
            )
            self._info = dataclasses.replace(self._info, volume_mounts=tuple(mounts))
        except Exception as err:
            raise map_rpc_error(err) from err

    def detach_volume(
        self, volume_id: str, *, force: bool = False, wait: float = 120
    ) -> None:
        try:
            self._client._rpc.detach_volume(
                pb.DetachVolumeRequest(
                    session_id=self.id, volume_id=volume_id, force_detach=force
                )
            )
        except Exception as err:
            raise map_rpc_error(err) from err

        deadline = time.monotonic() + wait
        while wait > 0 and time.monotonic() < deadline:
            self.refresh()
            if self._is_volume_detached(volume_id):
                return
            time.sleep(0.5)
        mounts = tuple(
            mount for mount in self._info.volume_mounts if mount.volume_id != volume_id
        )
        self._info = dataclasses.replace(self._info, volume_mounts=mounts)

    def _is_volume_detached(self, volume_id: str) -> bool:
        matches = [
            mount for mount in self._info.volume_mounts if mount.volume_id == volume_id
        ]
        return not matches or all(mount.state == "DETACHED" for mount in matches)

    def snapshot(self, *, name: str | None = None, wait: bool = True):
        if wait:
            return self._client.snapshots.create_and_wait(self.id, name=name)
        return self._client.snapshots.create(self.id, name=name)

    def _git_operation(self, operation: str, args: dict[str, str]) -> str:
        try:
            resp = self._client._rpc.git_operation(
                pb.GitOperationRequest(
                    session_id=self.id, operation=operation, args=args
                )
            )
            return resp.output
        except Exception as err:
            raise map_rpc_error(err) from err

    def ssh(self, *, gateway_url: str | None = None) -> "SSHConn":
        from .ssh import SSHConn  # noqa: F401

        return self._client.ssh(self.id, gateway_url=gateway_url)

    def issue_ssh_cert(
        self,
        public_key: str,
        *,
        ttl: float | None = None,
    ) -> "SSHCert":
        from .models import SSHCert  # noqa: F401

        return self._client.issue_ssh_cert(self.id, public_key, ttl=ttl)

    def dial(
        self, unix_socket_path: str, *, connect_timeout: float | None = None
    ) -> "DialConn":
        from .dial import open_dial_conn

        timeout_ms = int(connect_timeout * 1000) if connect_timeout is not None else 0
        return open_dial_conn(
            self._session_rpc, self.id, unix_socket_path, connect_timeout_ms=timeout_ms
        )

    def expose_host_port(
        self,
        host_addr: str,
        *,
        sandbox_bind_address: str = "",
        sandbox_port: int = 0,
        host_dial_target_label: str = "",
    ) -> "HostPortTunnel":
        from .tunnel import open_host_port_tunnel

        return open_host_port_tunnel(
            self._session_rpc,
            self.id,
            host_addr,
            sandbox_bind_address=sandbox_bind_address,
            sandbox_port=sandbox_port,
            host_dial_target_label=host_dial_target_label,
        )

    def expose_host_port_resilient(
        self,
        host_addr: str,
        *,
        sandbox_bind_address: str = "",
        sandbox_port: int = 0,
        host_dial_target_label: str = "",
        initial_backoff: float = 0.25,
        max_backoff: float = 10.0,
    ) -> "ResilientHostPortTunnel":
        from .tunnel import open_resilient_host_port_tunnel

        return open_resilient_host_port_tunnel(
            self._session_rpc,
            self.id,
            host_addr,
            sandbox_bind_address=sandbox_bind_address,
            sandbox_port=sandbox_port,
            host_dial_target_label=host_dial_target_label,
            initial_backoff=initial_backoff,
            max_backoff=max_backoff,
        )


def _is_unimplemented(err: Exception) -> bool:
    code_fn = getattr(err, "code", None)
    if not callable(code_fn):
        return False
    code = code_fn()
    return getattr(code, "name", str(code)) == "UNIMPLEMENTED"
