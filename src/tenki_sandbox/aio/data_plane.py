from __future__ import annotations

import asyncio
import logging
import re
import threading
from datetime import datetime, timezone

from .._pb import pb
from .._transport import grpc_target
from ..data_plane import (
    DATA_PLANE_CREDENTIAL_HEADER,
    _is_edge_not_ready,
    _unwrap_field,
    _wrap_frame,
    _wrap_unary_request,
)

logger = logging.getLogger("tenki_sandbox")

# Data-plane RPCs whose response is a stream (consumed via `async for`).
_DP_STREAM_OUT = frozenset({"run", "dial", "host_port_tunnel", "read_file_stream"})
# Data-plane RPCs whose request is an async iterator of frames.
_DP_STREAM_IN = frozenset({"run", "dial", "host_port_tunnel", "write_file_stream"})

_shared_channels_lock = threading.Lock()
_shared_channels: dict[tuple[object, str, bool], list] = {}


class _AsyncDataPlaneEndpointHints:
    def __init__(self) -> None:
        self._endpoint = ""
        self._lock = asyncio.Lock()
        self._task: asyncio.Task | None = None

    def remember(self, endpoint: str) -> None:
        endpoint = endpoint.strip()
        if endpoint:
            self._endpoint = endpoint

    def current(self) -> str:
        return self._endpoint

    def clear(self) -> None:
        self._endpoint = ""

    async def resolve(self, resolve) -> str:
        while True:
            if self._endpoint:
                return self._endpoint
            async with self._lock:
                if self._endpoint:
                    return self._endpoint
                owner = self._task is None
                if owner:
                    self._task = asyncio.create_task(resolve())
                task = self._task
            try:
                endpoint = (await task).strip()
            except Exception:
                if owner:
                    raise
                async with self._lock:
                    if self._task is task:
                        self._task = None
                continue
            finally:
                if owner:
                    async with self._lock:
                        if self._task is task:
                            self._task = None
            self.remember(endpoint)
            return endpoint


def _acquire_shared_channel(target: str, secure: bool):
    import grpc

    loop = asyncio.get_running_loop()
    key = (loop, target, secure)
    with _shared_channels_lock:
        shared = _shared_channels.get(key)
        if shared is None:
            channel = (
                grpc.aio.secure_channel(target, grpc.ssl_channel_credentials())
                if secure
                else grpc.aio.insecure_channel(target)
            )
            shared = [channel, 0]
            _shared_channels[key] = shared
        shared[1] += 1

    async def release() -> None:
        channel = None
        with _shared_channels_lock:
            if shared[1] <= 0:
                return
            shared[1] -= 1
            if shared[1] == 0 and _shared_channels.get(key) is shared:
                del _shared_channels[key]
                channel = shared[0]
        if channel is not None:
            await channel.close(None)

    return shared[0], release


def _rpc_error_message(err) -> str:
    details = getattr(err, "details", None)
    if callable(details):
        try:
            return str(details())
        except Exception:
            pass
    return str(err)


def _credential_error_state(err) -> str:
    match = re.search(
        r"\bstate\s*=\s*([A-Z_]+)\b", _rpc_error_message(err), re.IGNORECASE
    )
    return match.group(1).upper() if match else ""


def _credential_error_is_retryable(err: Exception) -> bool:
    """Whether a CreateSessionCredential failure is worth retrying.

    Mirrors the sync manager: retry only FAILED_PRECONDITION (the session is
    still provisioning / edge not ready) and non-gRPC transients; treat every
    other gRPC status (PermissionDenied, Unauthenticated, NotFound, ...) as
    permanent so we don't hammer the endpoint forever. The async transport maps
    errors, so the original AioRpcError is the mapped exception's __cause__.
    """
    import grpc

    for candidate in (err, getattr(err, "__cause__", None)):
        if isinstance(candidate, grpc.aio.AioRpcError):
            if candidate.code() != grpc.StatusCode.FAILED_PRECONDITION:
                return False
            return _credential_error_state(candidate) != "PAUSED"
    return True  # non-gRPC / unmapped — retry, matching sync's `except Exception: pass`


class AsyncDataPlaneCredentialManager:
    """Async twin of DataPlaneCredentialManager — renews via an asyncio task."""

    def __init__(self, sandbox, credential=None) -> None:
        self._sandbox = sandbox
        self._credential = ""
        self._expires_at: datetime | None = None
        self._stopped = False
        self._task: asyncio.Task | None = None
        self._renew_error: Exception | None = None
        if credential is not None:
            self.set(credential)

    def current(self) -> str:
        return self._credential

    def set(self, credential) -> None:
        if credential is None or not getattr(credential, "credential", ""):
            return
        self._credential = credential.credential
        self._expires_at = (
            credential.expires_at.ToDatetime(tzinfo=timezone.utc)
            if credential.HasField("expires_at")
            else None
        )
        self._schedule()

    def stop(self) -> None:
        self._stopped = True
        if self._task is not None:
            self._task.cancel()
            self._task = None

    def ensure_scheduled(self) -> None:
        # Recover renewal when set() ran without a running loop (e.g. an
        # AsyncSandbox constructed with an eager credential in sync code, where
        # get_running_loop() raised). Safe from any coroutine; no-op once a
        # renewal task exists or we're stopped / have no expiry. Called on first
        # data-plane use, which is always inside a running loop.
        if self._task is not None or self._expires_at is None or self._stopped:
            return
        self._schedule()

    def _schedule(self) -> None:
        if self._expires_at is None or self._stopped:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        remaining = max(
            1.0, (self._expires_at - datetime.now(timezone.utc)).total_seconds()
        )
        if self._task is not None:
            self._task.cancel()
        self._task = loop.create_task(self._renew_loop(remaining / 2))
        self._task.add_done_callback(self._on_renew_done)

    def _on_renew_done(self, task: asyncio.Task) -> None:
        # The renewal runs detached, so a permanent-failure raise from
        # _renew_loop would otherwise be a dangling "Task exception was never
        # retrieved". Retrieve it here, record it, and log it so the real cause
        # is visible instead of only a later opaque expired-credential error.
        if task.cancelled():
            return
        err = task.exception()
        if err is not None:
            self._renew_error = err
            logger.warning("sandbox data-plane credential renewal stopped: %r", err)

    async def _renew_loop(self, delay: float) -> None:
        try:
            await asyncio.sleep(delay)
            backoff = 1.0
            while not self._stopped:
                try:
                    resp = await self._sandbox._client._rpc.create_session_credential(
                        pb.CreateSessionCredentialRequest(session_id=self._sandbox.id)
                    )
                    self.set(resp.credential)
                    return
                except Exception as err:
                    # Retry transient failures (still provisioning); stop on
                    # permanent ones instead of hammering forever. The raise is
                    # captured by _on_renew_done (this task is detached), which
                    # records/logs the real cause.
                    if not _credential_error_is_retryable(err):
                        raise
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
        except asyncio.CancelledError:
            return


class AsyncGrpcDataPlaneRPC:
    _methods = {
        "run": "Run",
        "write_file": "WriteFile",
        "read_file": "ReadFile",
        "read_file_stream": "ReadFileStream",
        "write_file_stream": "WriteFileStream",
        "stat": "Stat",
        "mkdir": "Mkdir",
        "remove": "Remove",
        "list": "List",
        "dial": "Dial",
        "host_port_tunnel": "HostPortTunnel",
    }

    def __init__(
        self, endpoint: str, credentials: AsyncDataPlaneCredentialManager
    ) -> None:
        target, secure = grpc_target(endpoint)
        self.endpoint = endpoint
        self._target = target
        self._secure = secure
        self._credentials = credentials
        self._channel = None
        self._stub = None
        self._release_channel = None

    def _ensure_stub(self):
        if self._stub is None:
            import grpc

            from tenki_sandbox._internal.proto.tenki.sandbox.v1 import sandbox_pb2_grpc

            self._channel, self._release_channel = _acquire_shared_channel(
                self._target, self._secure
            )
            self._stub = sandbox_pb2_grpc.SandboxSessionDataPlaneServiceStub(
                self._channel
            )
        return self._stub

    async def close(self) -> None:
        if self._release_channel is not None:
            release = self._release_channel
            self._release_channel = None
            await release()

    async def ensure_serving(self, session_id: str, *, attempts: int = 24) -> None:
        # Mirror the sync probe: the edge publishes the per-session route
        # asynchronously, so retry a cheap Stat until any non-404 proves serving.
        import grpc

        last = None
        for attempt in range(attempts):
            try:
                await self.stat(
                    pb.StatRequest(session_id=session_id, path="/home/tenki")
                )
                return
            except grpc.aio.AioRpcError as err:
                if not _is_edge_not_ready(err):
                    return
                last = err
            await asyncio.sleep(min(0.05 * (attempt + 1), 0.75))
        if last is not None:
            raise last

    def _auth_metadata(self, kwargs: dict) -> tuple:
        metadata = tuple(kwargs.pop("metadata", ()))
        credential = self._credentials.current().strip()
        if credential:
            metadata += ((DATA_PLANE_CREDENTIAL_HEADER, credential),)
        return metadata

    async def _wrap_stream(self, name: str, request):
        async for frame in request:
            yield _wrap_frame(name, frame)

    def __getattr__(self, name: str):
        rpc_name = self._methods.get(name)
        if rpc_name is None:
            raise AttributeError(name)

        if name in _DP_STREAM_OUT:

            def stream_call(request, *args, **kwargs):
                return self._call_stream(name, rpc_name, request, kwargs)

            return stream_call

        async def unary_call(request, *args, **kwargs):
            return await self._call_unary(name, rpc_name, request, kwargs)

        return unary_call

    def _wrap(self, name: str, request):
        return (
            self._wrap_stream(name, request)
            if name in _DP_STREAM_IN
            else _wrap_unary_request(name, request)
        )

    async def _call_unary(self, name: str, rpc_name: str, request, kwargs: dict):
        metadata = self._auth_metadata(kwargs)
        method = getattr(self._ensure_stub(), rpc_name)
        resp = await method(self._wrap(name, request), metadata=metadata, **kwargs)
        return resp.response

    async def _call_stream(self, name: str, rpc_name: str, request, kwargs: dict):
        metadata = self._auth_metadata(kwargs)
        method = getattr(self._ensure_stub(), rpc_name)
        field = _unwrap_field(name)
        call = method(self._wrap(name, request), metadata=metadata, **kwargs)
        async for item in call:
            yield getattr(item, field)
