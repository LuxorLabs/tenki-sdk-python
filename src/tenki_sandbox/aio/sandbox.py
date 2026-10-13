from __future__ import annotations

import asyncio
import dataclasses
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Sequence

from .._convert import exposed_port, sandbox_info
from .._pb import pb
from .._time import duration, seconds
from ..client import _normalize_tags
from ..errors import (
    PauseFailedError,
    PauseWaitCancelledError,
    SessionNotFoundError,
    WaitReadyFailedError,
    map_rpc_error,
)
from ..models import (
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
from ..sandbox import _is_unimplemented
from ..secret_files import RuntimeSecretFile
from ..template_spec import TemplateSpec
from ..warning import (
    SandboxWarning,
    WarningHandler,
    default_warning_handler,
    emit_warnings,
    sandbox_warnings_from_proto,
)
from .data_plane import (
    AsyncDataPlaneCredentialManager,
    AsyncGrpcDataPlaneRPC,
    _AsyncDataPlaneEndpointHints,
)
from .fs import AsyncSandboxFS
from .git import AsyncSandboxGit
from .process import AsyncProcess


class AsyncSandbox:
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
        self._data_plane_endpoint_hints = _AsyncDataPlaneEndpointHints()
        self._data_plane_lock = asyncio.Lock()
        self._data_plane_credentials: AsyncDataPlaneCredentialManager | None = None
        self._data_plane_rpc: AsyncGrpcDataPlaneRPC | None = None
        self._data_plane_verified = False
        self._data_plane_from_hint = False
        # Set by close(); keeps a late response from re-arming a closed sandbox.
        self._data_plane_closed = False
        # Serializes close() so concurrent callers issue one TerminateSession.
        self._close_lock = asyncio.Lock()
        self._close_done = False
        self._released = False
        # A FAILED route carries a stale/dead endpoint; leave _data_plane_rpc
        # unset so first use re-bootstraps via CreateSessionCredential (mirrors
        # the sync client). Binding it would route every fs/exec/dial to a dead
        # endpoint with no recovery.
        self._bind_data_plane(
            data_plane_endpoint, data_plane_credential, data_plane_route_status
        )
        self.fs = AsyncSandboxFS(self._session_rpc, info.id)
        self.git = AsyncSandboxGit(self)

    def _bind_data_plane(
        self,
        endpoint: str = "",
        credential=None,
        route_status=None,
        *,
        from_hint: bool = False,
    ) -> None:
        # Synchronous bind for construction: no existing rpc to close and no loop
        # yet. `_apply_data_plane_response` is the async equivalent used later.
        if route_status == pb.DATA_PLANE_ROUTE_STATUS_FAILED:
            return
        endpoint = (endpoint or "").strip()
        if credential is not None and self._data_plane_credentials is None:
            self._data_plane_credentials = AsyncDataPlaneCredentialManager(
                self, credential, endpoint
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
        if credential is not None or self._data_plane_credentials is None:
            self._data_plane_credentials = AsyncDataPlaneCredentialManager(
                self, credential, endpoint
            )
        self._data_plane_credentials.adopt_endpoint(endpoint)
        self._data_plane_rpc = AsyncGrpcDataPlaneRPC(
            endpoint, self._data_plane_credentials, self.id
        )
        self._data_plane_from_hint = from_hint
        self._data_plane_verified = route_status == pb.DATA_PLANE_ROUTE_STATUS_VERIFIED

    async def _apply_data_plane_response(
        self,
        endpoint: str = "",
        credential=None,
        route_status=None,
        *,
        from_hint: bool = False,
    ) -> None:
        # Re-apply an endpoint/route observed while waiting (WaitSession frames).
        # Skips FAILED and may replace an existing rpc, so it awaits close().
        if route_status == pb.DATA_PLANE_ROUTE_STATUS_FAILED:
            return
        endpoint = (endpoint or "").strip()
        async with self._data_plane_lock:
            if self._data_plane_closed:
                return
            if credential is not None and self._data_plane_credentials is None:
                self._data_plane_credentials = AsyncDataPlaneCredentialManager(
                    self, credential, endpoint
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
                    self._data_plane_credentials.set(credential, endpoint)
                self._data_plane_from_hint = from_hint
                if route_status == pb.DATA_PLANE_ROUTE_STATUS_VERIFIED:
                    self._data_plane_verified = True
                return
            if self._data_plane_rpc is not None:
                await self._data_plane_rpc.close()
                if self._data_plane_closed:
                    return
            if credential is not None and self._data_plane_credentials is not None:
                self._data_plane_credentials.stop()
            if credential is not None or self._data_plane_credentials is None:
                self._data_plane_credentials = AsyncDataPlaneCredentialManager(
                    self, credential, endpoint
                )
            self._data_plane_credentials.adopt_endpoint(endpoint)
            self._data_plane_rpc = AsyncGrpcDataPlaneRPC(
                endpoint, self._data_plane_credentials, self.id
            )
            self._data_plane_from_hint = from_hint
            self._data_plane_verified = (
                route_status == pb.DATA_PLANE_ROUTE_STATUS_VERIFIED
            )

    async def _session_rpc(self):
        return (await self._ensure_data_plane()) or self._client._rpc

    async def _verify_serving(self) -> None:
        # An eagerly-bound but unverified endpoint may not be routable yet; probe
        # once (retries edge-not-ready) before first use, like the sync client.
        async with self._data_plane_lock:
            if self._data_plane_verified or self._data_plane_rpc is None:
                return
            ensure_serving = getattr(self._data_plane_rpc, "ensure_serving", None)
            if ensure_serving is not None:
                rpc = self._data_plane_rpc
                try:
                    await ensure_serving(self.id)
                except Exception:
                    if self._data_plane_credentials is not None:
                        self._data_plane_credentials.stop()
                        self._data_plane_credentials = None
                    await rpc.close()
                    self._data_plane_rpc = None
                    self._data_plane_verified = False
                    self._data_plane_from_hint = False
                    raise
            self._data_plane_verified = True

    async def _ensure_data_plane(self, *, verify_serving: bool = True):
        # CreateSessionResponse returns the endpoint empty pre-provision; bootstrap
        # via CreateSessionCredential on first data-plane use (mirrors the Go SDK).
        if self._data_plane_closed:
            raise RuntimeError("sandbox session handle closed or released")
        if self._data_plane_rpc is not None:
            if verify_serving:
                await self._verify_serving()
            if self._data_plane_credentials is not None:
                # Recover renewal if the manager was constructed without a loop.
                self._data_plane_credentials.ensure_scheduled()
            return self._data_plane_rpc
        if (
            self._data_plane_credentials is not None
            and self._data_plane_credentials.current()
        ):
            endpoint = await self._data_plane_endpoint_hints.resolve(
                self._renew_data_plane
            )
        else:
            endpoint = await self._renew_data_plane()
        if not endpoint:
            return None
        if self._data_plane_rpc is None:
            await self._apply_data_plane_response(endpoint, from_hint=True)
        if verify_serving:
            await self._verify_serving()
        if self._data_plane_credentials is not None:
            self._data_plane_credentials.ensure_scheduled()
        return self._data_plane_rpc

    async def _renew_data_plane(self) -> str:
        try:
            resp = await self._client._rpc.create_session_credential(
                pb.CreateSessionCredentialRequest(session_id=self.id)
            )
        except AttributeError:
            return ""
        except Exception as err:
            raise map_rpc_error(err) from err
        endpoint = resp.data_plane_endpoint.strip()
        await self._apply_data_plane_response(
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
        return self._data_plane_rpc is rpc and not self._data_plane_verified

    def _mark_data_plane_serving(self, rpc) -> None:
        if self._data_plane_rpc is rpc:
            self._data_plane_verified = True

    async def _refresh_hinted_data_plane(self, rpc) -> None:
        if self._data_plane_rpc is not rpc or not self._data_plane_from_hint:
            return
        await self._renew_data_plane()

    async def _reset_data_plane(self) -> None:
        async with self._data_plane_lock:
            if self._data_plane_credentials is not None:
                self._data_plane_credentials.stop()
                self._data_plane_credentials = None
            if self._data_plane_rpc is not None:
                await self._data_plane_rpc.close()
                self._data_plane_rpc = None
            self._data_plane_endpoint_hints.clear()
            self._data_plane_verified = False
            self._data_plane_from_hint = False

    @classmethod
    async def create(
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
        allowed_ports: list[int] | None = None,
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
    ) -> "AsyncSandbox":
        """Create a sandbox on a client this sandbox owns and closes.

        Accepts every ``AsyncClient`` connection argument and every
        ``AsyncClient.create`` argument; see ``Client.create`` for resource
        bounds and egress rules. ``timeout`` is the client transport timeout,
        not the readiness budget; construct a client and call
        ``AsyncClient.create(timeout=...)`` to set that.
        """
        from .client import AsyncClient

        # timeout is the transport timeout here; Client.create's is the wait budget.
        client = AsyncClient(
            auth_token=auth_token,
            api_key=api_key,
            base_url=base_url,
            gateway_url=gateway_url,
            timeout=timeout,
            warning_handler=warning_handler,
        )
        try:
            return await client.create(
                workspace_id=workspace_id,
                name=name,
                wait=wait,
                allow_inbound=allow_inbound,
                allow_outbound=allow_outbound,
                allow_domains=allow_domains,
                allow_cidrs=allow_cidrs,
                intercept_https=intercept_https,
                allowed_ports=allowed_ports,
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
            await client.close()
            raise

    def __repr__(self) -> str:
        return f"AsyncSandbox(id={self.id!r}, name={self.name!r}, state={self.state!r})"

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

    async def __aenter__(self) -> "AsyncSandbox":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if not self._detached:
            await self.close()
        if self._owns_client:
            await self._client.close()

    def detach(self) -> None:
        self._detached = True

    async def refresh(self) -> SandboxInfo:
        self._info = (await self._client.get(self.id)).info
        return self._info

    async def metrics(
        self, *, window: float | int | object | None = None
    ) -> SessionMetrics:
        return await self._client.get_session_metrics(self.id, window=window)

    async def wait_ready(self, timeout: float = 180) -> None:
        try:
            await self._wait_ready_stream(timeout)
            return
        except Exception as err:
            if not _is_unimplemented(err):
                if hasattr(err, "code"):
                    raise map_rpc_error(err) from err
                raise
        await self._wait_ready_poll(timeout)

    async def _wait_ready_stream(self, timeout: float) -> None:
        async for resp in self._client._rpc.wait_session(
            pb.WaitSessionRequest(session_id=self.id), timeout=timeout
        ):
            self._info = sandbox_info(resp.session)
            await self._apply_data_plane_response(
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

    async def _wait_ready_poll(self, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while True:
            await self.refresh()
            if self.state == "RUNNING":
                return
            if self.state in {"TERMINATING", "TERMINATED"}:
                raise RuntimeError(f"sandbox entered terminal state: {self.state}")
            if time.monotonic() >= deadline:
                break
            await asyncio.sleep(0.5)
        raise TimeoutError(f"timeout waiting for sandbox {self.id}")

    async def wait_paused(
        self,
        timeout: float = 180,
        *,
        cancel_event: asyncio.Event | threading.Event | None = None,
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
                await self.refresh()
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
            await asyncio.sleep(0.5)
        raise TimeoutError(f"timeout waiting for sandbox {self.id} to pause")

    async def start(
        self,
        *argv: str | Sequence[str],
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout: float | int | object | None = None,
        stdin: bytes | str | None = None,
        privileged: bool = False,
        stream_only: bool = False,
    ) -> AsyncProcess:
        rpc = await self._ensure_data_plane(verify_serving=False)
        if rpc is None:
            raise RuntimeError("sandbox data-plane endpoint unavailable")
        return await AsyncProcess.start(
            self,
            self.id,
            argv,
            cwd=cwd,
            env=env,
            timeout=timeout,
            stdin=stdin,
            privileged=privileged,
            stream_only=stream_only,
        )

    async def exec(
        self,
        *argv: str | Sequence[str],
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout: float | int | object | None = None,
        input: bytes | str | None = None,
        check: bool = False,
        privileged: bool = False,
    ) -> CommandResult:
        proc = await self.start(
            *argv, cwd=cwd, env=env, timeout=timeout, stdin=input, privileged=privileged
        )
        if input is None:
            await proc.close_stdin()
        result = await proc.wait()
        if check:
            result.check()
        return result

    async def shell(self, command: str, **kwargs) -> CommandResult:
        """Run ``command`` through a login shell.

        A login shell sources the guest's startup files, so a ``cd`` in
        ``~/.bashrc`` or ``~/.profile`` runs before ``command`` and wins over
        ``cwd``. Call ``exec("bash", "-c", command)`` when the directory must hold.
        """
        return await self.exec("bash", "-lc", command, **kwargs)

    async def release(self) -> None:
        # Permanently free the local data plane without terminating the sandbox.
        async with self._close_lock:
            self._released = True
            self._detached = True
            self._data_plane_closed = True
            await self._reset_data_plane()

    async def close(self) -> None:
        """Terminate this sandbox.

        Idempotent and safe to call concurrently: the first successful call
        terminates and every later one is a no-op. A failed call stays
        retryable.
        """
        async with self._close_lock:
            if self._close_done:
                return
            # Latch the data plane shut before terminating, so a renewal in
            # flight cannot revive the renewal task or the channel. Tear down
            # under the lock and null the fields (like resume()), so a closed
            # channel is never left referenced for a later data-plane call.
            self._data_plane_closed = True
            await self._reset_data_plane()
            try:
                resp = await self._client._rpc.terminate_session(
                    pb.TerminateSessionRequest(session_id=self.id)
                )
            except Exception as err:
                self._data_plane_closed = self._released
                raise map_rpc_error(err) from err
            self._close_done = True
            if resp.session:
                self._info = sandbox_info(resp.session)

    terminate = close

    async def close_if_open(self) -> None:
        if self.state not in {"TERMINATING", "TERMINATED"}:
            await self.close()

    async def update(
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
            normalized = _normalize_tags(tags)
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
            resp = await self._client._rpc.update_session(req)
            if resp.session:
                self._info = sandbox_info(resp.session)
            warnings = sandbox_warnings_from_proto(resp.warnings)
            self.warnings = warnings
            emit_warnings(warnings, self._client._warning_handler, stacklevel=4)
        except Exception as err:
            raise map_rpc_error(err) from err

    async def update_tags(self, *tags: str) -> None:
        await self.update(tags=list(tags))

    async def update_ssh_authorized_keys(self, keys: list[str]) -> None:
        req = pb.UpdateSSHAuthorizedKeysRequest(session_id=self.id)
        req.ssh_authorized_keys.extend(keys)
        try:
            await self._client._rpc.update_ssh_authorized_keys(req)
        except Exception as err:
            raise map_rpc_error(err) from err

    async def pause(self) -> None:
        await self._pause(async_=False)

    async def pause_async(self) -> None:
        await self._pause(async_=True)

    async def _pause(self, *, async_: bool) -> None:
        req = pb.PauseSessionRequest(session_id=self.id)
        setattr(req, "async", async_)
        try:
            resp = await self._client._rpc.pause_session(req)
            self._info = sandbox_info(resp.session)
        except Exception as err:
            raise map_rpc_error(err) from err
        await self._reset_data_plane()

    async def resume(self) -> None:
        try:
            resp = await self._client._rpc.resume_session(
                pb.ResumeSessionRequest(session_id=self.id)
            )
            self._info = sandbox_info(resp.session)
        except Exception as err:
            raise map_rpc_error(err) from err
        # The session may resume on a different host; re-bootstrap on next use.
        await self._reset_data_plane()

    async def extend(self, additional: float | int | object) -> None:
        dur = duration(additional)
        try:
            resp = await self._client._rpc.extend_session(
                pb.ExtendSessionRequest(session_id=self.id, additional_duration=dur)
            )
            self._info = sandbox_info(resp.session)
        except Exception as err:
            raise map_rpc_error(err) from err

    async def expose_port(
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
            return exposed_port(await self._client._rpc.expose_port(req))
        except Exception as err:
            raise map_rpc_error(err) from err

    async def unexpose_port(self, port: int) -> None:
        try:
            await self._client._rpc.unexpose_port(
                pb.UnexposePortRequest(session_id=self.id, port=port)
            )
        except Exception as err:
            raise map_rpc_error(err) from err

    async def list_exposed_ports(self) -> list[ExposedPort]:
        try:
            resp = await self._client._rpc.list_exposed_ports(
                pb.ListExposedPortsRequest(session_id=self.id)
            )
            # The shared converter also carries the wildcard fields; rebuilding
            # ExposedPort by hand here silently dropped them, so listing an
            # exposure reported wildcard=False/UNSPECIFIED for the same port
            # expose_port had just described correctly.
            return [exposed_port(item) for item in resp.ports]
        except Exception as err:
            raise map_rpc_error(err) from err

    async def attach_volume(
        self, volume_id: str, mount_path: str, *, read_only: bool = False
    ) -> None:
        try:
            await self._client._rpc.attach_volume(
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

    async def detach_volume(
        self, volume_id: str, *, force: bool = False, wait: float = 120
    ) -> None:
        """``force`` releases a ``SYNC_PENDING`` attachment at once, or unplugs an attached
        volume the guest cannot unmount (writes the guest has not flushed are lost)."""
        try:
            await self._client._rpc.detach_volume(
                pb.DetachVolumeRequest(
                    session_id=self.id, volume_id=volume_id, force_detach=force
                )
            )
        except Exception as err:
            raise map_rpc_error(err) from err

        deadline = time.monotonic() + wait
        while wait > 0 and time.monotonic() < deadline:
            await self.refresh()
            if self._is_volume_detached(volume_id):
                return
            await asyncio.sleep(0.5)
        mounts = tuple(
            mount for mount in self._info.volume_mounts if mount.volume_id != volume_id
        )
        self._info = dataclasses.replace(self._info, volume_mounts=mounts)

    def _is_volume_detached(self, volume_id: str) -> bool:
        matches = [
            mount for mount in self._info.volume_mounts if mount.volume_id == volume_id
        ]
        return not matches or all(mount.state == "DETACHED" for mount in matches)

    async def snapshot(self, *, name: str | None = None, wait: bool = True):
        if wait:
            return await self._client.snapshots.create_and_wait(self.id, name=name)
        return await self._client.snapshots.create(self.id, name=name)

    async def _git_operation(self, operation: str, args: dict[str, str]) -> str:
        try:
            resp = await self._client._rpc.git_operation(
                pb.GitOperationRequest(
                    session_id=self.id, operation=operation, args=args
                )
            )
            return resp.output
        except Exception as err:
            raise map_rpc_error(err) from err

    async def issue_ssh_cert(
        self, public_key: str, *, ttl: float | None = None
    ) -> "SSHCert":
        from ..models import SSHCert  # noqa: F401

        return await self._client.issue_ssh_cert(self.id, public_key, ttl=ttl)

    async def ssh(self, *, gateway_url: str | None = None) -> "AsyncSSHConn":
        return await self._client.ssh(self.id, gateway_url=gateway_url)

    async def dial(
        self, unix_socket_path: str, *, connect_timeout: float | None = None
    ) -> "AsyncDialConn":
        from .dial import open_dial_conn

        rpc = await self._session_rpc()
        timeout_ms = int(connect_timeout * 1000) if connect_timeout is not None else 0
        return await open_dial_conn(
            rpc, self.id, unix_socket_path, connect_timeout_ms=timeout_ms
        )

    async def expose_host_port(
        self,
        host_addr: str,
        *,
        sandbox_bind_address: str = "",
        sandbox_port: int = 0,
        host_dial_target_label: str = "",
    ) -> "AsyncHostPortTunnel":
        from .tunnel import open_host_port_tunnel

        rpc = await self._session_rpc()
        return await open_host_port_tunnel(
            rpc,
            self.id,
            host_addr,
            sandbox_bind_address=sandbox_bind_address,
            sandbox_port=sandbox_port,
            host_dial_target_label=host_dial_target_label,
        )

    async def expose_host_port_resilient(
        self,
        host_addr: str,
        *,
        sandbox_bind_address: str = "",
        sandbox_port: int = 0,
        host_dial_target_label: str = "",
        initial_backoff: float = 0.25,
        max_backoff: float = 10.0,
    ) -> "AsyncResilientHostPortTunnel":
        from .tunnel import open_resilient_host_port_tunnel

        rpc = await self._session_rpc()
        return await open_resilient_host_port_tunnel(
            rpc,
            self.id,
            host_addr,
            sandbox_bind_address=sandbox_bind_address,
            sandbox_port=sandbox_port,
            host_dial_target_label=host_dial_target_label,
            initial_backoff=initial_backoff,
            max_backoff=max_backoff,
        )
